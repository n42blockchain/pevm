// SPDX-License-Identifier: MIT OR Apache-2.0

//! Database wrappers for logging and caching EVM state access.
//!
//! This module contains various database wrapper implementations used for:
//! - Logging state reads during block execution
//! - Caching bytecode and account data
//! - Batch execution with state tracking

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::fs::File;
use std::io::Write;
use alloy_primitives::{Address, B256, U256};
use reth_codecs::Compact;
use reth_primitives::Account;
use reth_provider::StateProvider;
use reth_revm::database::StateProviderDatabase;
use tracing::info;

use crate::revm::Database as RevmDatabase;
use crate::revm::state::{AccountInfo, Bytecode};

use super::types::{ReadLogEntry, RawLogEntry};
use super::logged_db::BytecodeCache;
use super::EMPTY_CODE_HASH_BYTES;

/// 包装的 Database，用于拦截读取操作并记录日志
pub(crate) struct LoggingDatabase<DB> {
    pub(crate) inner: DB,
    pub(crate) read_logs: Arc<Mutex<Vec<ReadLogEntry>>>,
}

impl<DB> std::fmt::Debug for LoggingDatabase<DB>
where
    DB: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoggingDatabase")
            .field("inner", &self.inner)
            .field("read_logs", &self.read_logs)
            .finish_non_exhaustive()
    }
}

impl<DB: RevmDatabase> RevmDatabase for LoggingDatabase<DB> {
    type Error = <DB as RevmDatabase>::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, <DB as RevmDatabase>::Error> {
        // 先调用 inner，减少锁持有时间
        let result = self.inner.basic(address)?;

        // 记录账户读取：使用 Reth 的 Compact<Account> 编码替代 RLP（性能优化）
        // 即使账户不存在，也要记录一个条目以保持顺序一致
        // 优化：先准备数据，再快速加锁写入
        let log_entry = if let Some(account_info) = &result {
            // 将 revm::AccountInfo 转换为 reth::Account
            // reth::Account 结构：nonce, balance, bytecode_hash (Option<B256>)
            // 性能优化：跳过默认的空 codehash，使用 None 让 Compact 编码省略它
            let code_hash = account_info.code_hash();

            // 性能优化：直接比较字节数组，避免创建 B256 对象
            let bytecode_hash = if code_hash.as_slice() == EMPTY_CODE_HASH_BYTES {
                None
            } else {
                Some(code_hash)
            };

            let account = Account {
                nonce: account_info.nonce,
                balance: account_info.balance,
                bytecode_hash,
            };

            // 性能优化：预分配 Vec 容量（Account 通常编码后 < 100 字节）
            let mut buf = Vec::with_capacity(100);
            account.to_compact(&mut buf);

            ReadLogEntry::Account {
                address,
                data: buf,
            }
        } else {
            // 账户不存在：使用空的 Compact 编码标记
            // Compact 编码：对于 Option<Account>，None 可以用 0x00 或空字节表示
            ReadLogEntry::Account {
                address,
                data: vec![0x00], // 使用 0x00 表示账户不存在
            }
        };

        // 性能优化：快速加锁写入（减少锁持有时间）
        // 使用作用域自动释放锁，避免显式 drop
        {
            let mut logs = self.read_logs.lock().unwrap();
            logs.push(log_entry);
        } // 锁在这里自动释放

        Ok(result)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, <DB as RevmDatabase>::Error> {
        // 代码读取忽略（按用户要求），不记录到日志
        self.inner.code_by_hash(code_hash)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, <DB as RevmDatabase>::Error> {
        // 先调用 inner，减少锁持有时间
        let result = self.inner.storage(address, index)?;

        // 记录存储读取：使用 Reth 的 CompactU256 编码替代 RLP（性能优化）
        // 注意：index 是 U256，需要转换为 32 字节的 key

        // 将存储值使用 Compact 编码（在锁外完成）
        // CompactU256 使用紧凑编码，比 RLP 更高效
        let key_bytes = index.to_be_bytes::<32>();
        let key = B256::from_slice(&key_bytes);
        // 性能优化：预分配 Vec 容量（U256 Compact 编码通常 < 40 字节）
        let mut buf = Vec::with_capacity(40);
        // 使用 Compact 编码 U256（reth-primitives 中的 CompactU256）
        result.to_compact(&mut buf);

        // 性能优化：快速加锁写入（减少锁持有时间）
        {
            let mut logs = self.read_logs.lock().unwrap();
            logs.push(ReadLogEntry::Storage {
                address,
                key,
                data: buf,
            });
        } // 锁在这里自动释放

        Ok(result)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, <DB as RevmDatabase>::Error> {
        self.inner.block_hash(number)
    }
}

/// 批次日志收集的共享状态（用于批量执行模式，避免每块创建 StateProvider）
/// 使用 RefCell 而非 Mutex - 单线程执行，无需锁开销
/// 性能优化：使用 RawLogEntry 延迟序列化，将 to_compact 移出热路径
pub(crate) struct BatchLoggingState {
    current_block: std::cell::Cell<u64>,
    // 按块号分组的原始日志（未序列化）
    raw_logs: std::cell::RefCell<HashMap<u64, Vec<RawLogEntry>>>,
    // 账户状态缓存：address -> AccountInfo (执行后更新)
    account_cache: std::cell::RefCell<HashMap<Address, Option<AccountInfo>>>,
    // 存储状态缓存：(address, slot) -> value (执行后更新)
    storage_cache: std::cell::RefCell<HashMap<(Address, U256), U256>>,
    // 性能测试：跳过日志记录（仍然执行缓存操作）
    skip_logging: bool,
}

impl BatchLoggingState {
    pub fn new() -> Self {
        Self {
            current_block: std::cell::Cell::new(0),
            raw_logs: std::cell::RefCell::new(HashMap::new()),
            account_cache: std::cell::RefCell::new(HashMap::new()),
            storage_cache: std::cell::RefCell::new(HashMap::new()),
            skip_logging: false,
        }
    }

    #[allow(dead_code)]
    pub fn new_skip_logging() -> Self {
        Self {
            current_block: std::cell::Cell::new(0),
            raw_logs: std::cell::RefCell::new(HashMap::new()),
            account_cache: std::cell::RefCell::new(HashMap::new()),
            storage_cache: std::cell::RefCell::new(HashMap::new()),
            skip_logging: true,
        }
    }

    #[inline]
    pub fn set_current_block(&self, block_number: u64) {
        self.current_block.set(block_number);
    }

    #[inline]
    #[allow(dead_code)]
    pub fn get_current_block(&self) -> u64 {
        self.current_block.get()
    }

    /// 推送原始日志条目（延迟序列化）
    /// 注意：块号为 0 时跳过日志（用于已存在的块，需要执行但不记录）
    #[inline]
    pub fn push_raw_log(&self, entry: RawLogEntry) {
        let block_num = self.current_block.get();
        // 跳过日志：skip_logging 标志 或 块号为 0（已存在的块）
        if self.skip_logging || block_num == 0 {
            return;
        }
        self.raw_logs.borrow_mut().entry(block_num).or_default().push(entry);
    }

    /// 获取缓存的账户状态
    #[inline]
    #[allow(dead_code)]
    pub fn get_cached_account(&self, address: &Address) -> Option<Option<AccountInfo>> {
        self.account_cache.borrow().get(address).cloned()
    }

    /// 更新账户缓存
    #[inline]
    #[allow(dead_code)]
    pub fn update_account_cache(&self, address: Address, account: Option<AccountInfo>) {
        self.account_cache.borrow_mut().insert(address, account);
    }

    /// 获取缓存的存储值
    #[inline]
    #[allow(dead_code)]
    pub fn get_cached_storage(&self, address: &Address, slot: &U256) -> Option<U256> {
        self.storage_cache.borrow().get(&(*address, *slot)).cloned()
    }

    /// 更新存储缓存
    #[inline]
    #[allow(dead_code)]
    pub fn update_storage_cache(&self, address: Address, slot: U256, value: U256) {
        self.storage_cache.borrow_mut().insert((address, slot), value);
    }

    /// 从执行输出更新缓存（块执行完成后调用）
    #[allow(dead_code)]
    pub fn apply_state_changes(&self, state_changes: &reth::revm::db::BundleState) {
        let mut account_cache = self.account_cache.borrow_mut();
        let mut storage_cache = self.storage_cache.borrow_mut();

        // 更新账户和存储缓存
        for (address, account) in state_changes.state() {
            if let Some(info) = &account.info {
                account_cache.insert(*address, Some(AccountInfo {
                    balance: info.balance,
                    nonce: info.nonce,
                    code_hash: info.code_hash,
                    code: None,
                }));
            }
            for (slot, value) in account.storage.iter() {
                storage_cache.insert((*address, *slot), value.present_value);
            }
        }
    }

    /// 提取并清空所有日志，返回按块号分组的日志
    /// 性能优化：在这里统一执行序列化（延迟序列化）
    #[allow(dead_code)]
    pub fn take_logs(&self) -> Vec<(u64, Vec<ReadLogEntry>)> {
        let mut raw = self.raw_logs.borrow_mut();
        let mut result: Vec<(u64, Vec<ReadLogEntry>)> = raw.drain()
            .map(|(block_num, entries)| {
                // 将 RawLogEntry 转换为 ReadLogEntry（执行序列化）
                let serialized: Vec<ReadLogEntry> = entries.into_iter()
                    .map(|e| e.into_serialized())
                    .collect();
                (block_num, serialized)
            })
            .collect();
        result.sort_by_key(|(bn, _)| *bn);
        result
    }
}

/// 超高性能日志收集器（使用 UnsafeCell 消除借用检查开销）
///
/// 安全性：仅用于单线程执行模式，不会有并发访问
///
/// 性能优化：
/// - 使用 UnsafeCell 替代 RefCell，消除运行时借用检查
/// - 使用 Cell<u64> 追踪当前块号（Copy 类型，零开销）
/// - 提前检查块号为 0 的情况，避免不必要的操作
/// - 当 skip_blocks 为空时，跳过 HashSet 查找
pub(crate) struct SimpleLogCollector {
    /// 当前块号
    current_block: std::cell::Cell<u64>,
    /// 所有日志：(块号, 条目) - 使用 UnsafeCell 消除借用检查开销
    logs: std::cell::UnsafeCell<Vec<(u64, RawLogEntry)>>,
    /// 需要跳过的块号集合
    skip_blocks: std::collections::HashSet<u64>,
    /// 优化标志：skip_blocks 是否为空
    skip_blocks_empty: bool,
    /// 已执行的块号列表（确保每个块都有条目，即使没有日志）
    executed_blocks: std::cell::UnsafeCell<Vec<u64>>,
}

impl SimpleLogCollector {
    pub fn new() -> Self {
        Self {
            current_block: std::cell::Cell::new(0),
            logs: std::cell::UnsafeCell::new(Vec::with_capacity(100000)),
            skip_blocks: std::collections::HashSet::new(),
            skip_blocks_empty: true,
            executed_blocks: std::cell::UnsafeCell::new(Vec::with_capacity(1000)),
        }
    }

    #[allow(dead_code)]
    pub fn with_skip_blocks(skip_blocks: std::collections::HashSet<u64>) -> Self {
        let skip_blocks_empty = skip_blocks.is_empty();
        Self {
            current_block: std::cell::Cell::new(0),
            logs: std::cell::UnsafeCell::new(Vec::with_capacity(100000)),
            skip_blocks,
            skip_blocks_empty,
            executed_blocks: std::cell::UnsafeCell::new(Vec::with_capacity(1000)),
        }
    }

    #[inline]
    pub fn set_current_block(&self, block_number: u64) {
        self.current_block.set(block_number);
        // 记录已执行的块（非跳过的块）
        if block_number != 0 && (self.skip_blocks_empty || !self.skip_blocks.contains(&block_number)) {
            // SAFETY: 单线程执行，不存在并发访问
            unsafe {
                (*self.executed_blocks.get()).push(block_number);
            }
        }
    }

    /// 超高性能日志推送：使用 UnsafeCell 消除借用检查
    ///
    /// 安全性：此函数仅在单线程环境下调用，不存在数据竞争
    #[inline]
    pub fn push(&self, entry: RawLogEntry) {
        let block_num = self.current_block.get();
        // 快速路径：块号为 0，直接返回（最常见的跳过情况）
        if block_num == 0 {
            return;
        }
        // 检查 skip_blocks（只在非空时查找，避免 HashSet 开销）
        if !self.skip_blocks_empty && self.skip_blocks.contains(&block_num) {
            return;
        }
        // SAFETY: 单线程执行，不存在并发访问
        unsafe {
            (*self.logs.get()).push((block_num, entry));
        }
    }

    /// 提取并分组日志
    /// 确保每个已执行的块都有条目（即使没有日志，也写入空条目）
    pub fn take_logs(&self) -> Vec<(u64, Vec<ReadLogEntry>)> {
        // SAFETY: 单线程执行，不存在并发访问
        let logs = unsafe { &mut *self.logs.get() };
        let executed_blocks = unsafe { &mut *self.executed_blocks.get() };

        // 按块号分组
        let mut grouped: HashMap<u64, Vec<RawLogEntry>> = HashMap::new();
        for (block_num, entry) in logs.drain(..) {
            grouped.entry(block_num).or_default().push(entry);
        }

        // 确保每个已执行的块都有条目（即使是空的）
        for &block_num in executed_blocks.iter() {
            grouped.entry(block_num).or_default();
        }
        executed_blocks.clear();

        // 转换为序列化格式
        let mut result: Vec<(u64, Vec<ReadLogEntry>)> = grouped
            .into_iter()
            .map(|(block_num, entries)| {
                let serialized: Vec<ReadLogEntry> = entries.into_iter()
                    .map(|e| e.into_serialized())
                    .collect();
                (block_num, serialized)
            })
            .collect();
        result.sort_by_key(|(bn, _)| *bn);
        result
    }
}

/// 透明包装器 - 用于性能测试
/// 仅转发所有调用，不做任何额外操作
/// 目的：测试类型包装对性能的影响
pub(crate) struct PassthroughDatabase<DB> {
    inner: DB,
}

impl<DB> PassthroughDatabase<DB> {
    pub fn new(inner: DB) -> Self {
        Self { inner }
    }
}

impl<DB: std::fmt::Debug> std::fmt::Debug for PassthroughDatabase<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PassthroughDatabase")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<DB: RevmDatabase> RevmDatabase for PassthroughDatabase<DB> {
    type Error = DB::Error;

    #[inline]
    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.inner.basic(address)
    }

    #[inline]
    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.inner.code_by_hash(code_hash)
    }

    #[inline]
    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        self.inner.storage(address, index)
    }

    #[inline]
    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.inner.block_hash(number)
    }
}

/// 批次级别的 LoggingDatabase（用于批量执行模式）
/// 性能优化：使用简单 Vec 收集日志，避免 HashMap 开销
pub(crate) struct BatchLoggingDatabase<DB> {
    inner: DB,
    collector: std::rc::Rc<SimpleLogCollector>,
}

impl<DB> BatchLoggingDatabase<DB> {
    pub fn new(inner: DB, collector: std::rc::Rc<SimpleLogCollector>) -> Self {
        Self { inner, collector }
    }
}

impl<DB> std::fmt::Debug for BatchLoggingDatabase<DB>
where
    DB: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchLoggingDatabase")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<DB: RevmDatabase> RevmDatabase for BatchLoggingDatabase<DB> {
    type Error = <DB as RevmDatabase>::Error;

    #[inline]
    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, <DB as RevmDatabase>::Error> {
        let result = self.inner.basic(address)?;
        // 高性能日志记录：使用 from_account 避免克隆整个 AccountInfo（含 Bytecode）
        self.collector.push(RawLogEntry::from_account(address, result.as_ref()));
        Ok(result)
    }

    #[inline]
    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, <DB as RevmDatabase>::Error> {
        self.inner.code_by_hash(code_hash)
    }

    #[inline]
    fn storage(&mut self, address: Address, index: U256) -> Result<U256, <DB as RevmDatabase>::Error> {
        let result = self.inner.storage(address, index)?;
        // 高性能日志记录：U256 是 Copy 类型，无需克隆
        self.collector.push(RawLogEntry::Storage { address, slot: index, value: result });
        Ok(result)
    }

    #[inline]
    fn block_hash(&mut self, number: u64) -> Result<B256, <DB as RevmDatabase>::Error> {
        self.inner.block_hash(number)
    }
}

/// 高性能日志记录数据库（用于 --log-block on 模式）
///
/// 关键优化：一个数据库实例可复用于整个批次的所有块
/// - 直接访问 StateProvider，无需中间包装器
/// - 集成缓存（避免创建多个数据库实例的开销）
/// - 使用 Cell/RefCell 实现零开销的块号追踪和日志收集
///
/// 对比原有 BatchLoggingDatabase：
/// - 原有：每块创建新的 StateProviderDatabase + BatchLoggingDatabase
/// - 新版：整个批次复用同一个 FastLoggingDatabase 实例
pub(crate) struct FastLoggingDatabase<'a> {
    state_provider: &'a dyn StateProvider,
    bytecode_cache: Option<Arc<BytecodeCache>>,
    // 本地缓存（执行后更新，供后续块读取）
    account_cache: std::cell::RefCell<HashMap<Address, Option<AccountInfo>>>,
    storage_cache: std::cell::RefCell<HashMap<(Address, U256), U256>>,
    block_hash_cache: std::cell::RefCell<HashMap<u64, B256>>,
    // 当前执行的块号
    current_block: std::cell::Cell<u64>,
    // 按块号分组的日志
    read_logs: std::cell::RefCell<HashMap<u64, Vec<ReadLogEntry>>>,
}

impl<'a> FastLoggingDatabase<'a> {
    pub fn new(
        state_provider: &'a dyn StateProvider,
        bytecode_cache: Option<Arc<BytecodeCache>>,
    ) -> Self {
        Self {
            state_provider,
            bytecode_cache,
            account_cache: std::cell::RefCell::new(HashMap::new()),
            storage_cache: std::cell::RefCell::new(HashMap::new()),
            block_hash_cache: std::cell::RefCell::new(HashMap::new()),
            current_block: std::cell::Cell::new(0),
            read_logs: std::cell::RefCell::new(HashMap::new()),
        }
    }

    /// 设置当前执行的块号（每块执行前调用）
    #[inline]
    #[allow(dead_code)]
    pub fn set_current_block(&self, block_number: u64) {
        self.current_block.set(block_number);
    }

    /// 记录日志条目
    #[inline]
    fn push_log(&self, entry: ReadLogEntry) {
        let block_num = self.current_block.get();
        self.read_logs.borrow_mut().entry(block_num).or_default().push(entry);
    }

    /// 应用块执行后的状态变更到缓存
    /// 这样后续块可以读取到最新状态
    #[allow(dead_code)]
    pub fn apply_state_changes(&self, state: &reth::revm::db::BundleState) {
        let mut account_cache = self.account_cache.borrow_mut();
        let mut storage_cache = self.storage_cache.borrow_mut();

        for (address, account) in state.state() {
            // 更新账户缓存
            if account.was_destroyed() {
                account_cache.insert(*address, None);
            } else if let Some(info) = &account.info {
                let code_hash = info.code_hash;
                let bytecode = if code_hash == super::get_empty_code_hash() {
                    Bytecode::default()
                } else if let Some(code) = &info.code {
                    code.clone()
                } else {
                    Bytecode::default()
                };
                account_cache.insert(*address, Some(AccountInfo {
                    balance: info.balance,
                    nonce: info.nonce,
                    code_hash,
                    code: Some(bytecode),
                }));
            }

            // 更新存储缓存
            for (slot, value) in account.storage.iter() {
                storage_cache.insert((*address, *slot), value.present_value);
            }
        }
    }

    /// 提取并清空所有日志，返回按块号排序的日志
    #[allow(dead_code)]
    pub fn take_logs(&self) -> Vec<(u64, Vec<ReadLogEntry>)> {
        let mut logs = self.read_logs.borrow_mut();
        let mut result: Vec<_> = logs.drain().collect();
        result.sort_by_key(|(bn, _)| *bn);
        result
    }
}

impl<'a> std::fmt::Debug for FastLoggingDatabase<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FastLoggingDatabase")
            .field("bytecode_cache", &self.bytecode_cache.as_ref().map(|c| c.len()))
            .field("account_cache_size", &self.account_cache.borrow().len())
            .field("storage_cache_size", &self.storage_cache.borrow().len())
            .field("current_block", &self.current_block.get())
            .finish()
    }
}

impl<'a> RevmDatabase for FastLoggingDatabase<'a> {
    type Error = <StateProviderDatabase<Box<dyn StateProvider>> as RevmDatabase>::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        // 优先从缓存读取（包括：上一块的状态变更 + 本块的读取结果）
        // 关键优化：缓存所有读取结果，避免重复数据库查询
        let result = {
            let cache = self.account_cache.borrow();
            if let Some(cached) = cache.get(&address) {
                cached.clone()
            } else {
                drop(cache); // 释放借用再查询数据库
                // 缓存未命中，查询数据库
                let mut inner_db = StateProviderDatabase::new(self.state_provider);
                let db_result = inner_db.basic(address)?;
                // 写入缓存（缓存所有读取，不仅仅是状态变更）
                self.account_cache.borrow_mut().insert(address, db_result.clone());
                db_result
            }
        };

        // 记录账户读取
        let log_entry = if let Some(ref account_info) = result {
            let code_hash = account_info.code_hash();
            let bytecode_hash = if code_hash.as_slice() == EMPTY_CODE_HASH_BYTES {
                None
            } else {
                Some(code_hash)
            };

            let account = Account {
                nonce: account_info.nonce,
                balance: account_info.balance,
                bytecode_hash,
            };

            let mut buf = Vec::with_capacity(100);
            account.to_compact(&mut buf);
            ReadLogEntry::Account { address, data: buf }
        } else {
            ReadLogEntry::Account { address, data: vec![0x00] }
        };

        self.push_log(log_entry);
        Ok(result)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        // 先查全局缓存
        if let Some(ref cache) = self.bytecode_cache {
            if let Some(bytecode) = cache.get(&code_hash) {
                return Ok(bytecode);
            }
        }

        // 缓存未命中，查询数据库
        let mut inner_db = StateProviderDatabase::new(self.state_provider);
        let bytecode = inner_db.code_by_hash(code_hash)?;

        // 写入全局缓存
        if let Some(ref cache) = self.bytecode_cache {
            cache.insert(code_hash, bytecode.clone());
        }

        Ok(bytecode)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        // 优先从缓存读取（包括：上一块的状态变更 + 本块的读取结果）
        let key = (address, index);
        let result = {
            let cache = self.storage_cache.borrow();
            if let Some(&cached) = cache.get(&key) {
                cached
            } else {
                drop(cache); // 释放借用再查询数据库
                // 缓存未命中，查询数据库
                let mut inner_db = StateProviderDatabase::new(self.state_provider);
                let db_result = inner_db.storage(address, index)?;
                // 写入缓存（缓存所有读取，不仅仅是状态变更）
                self.storage_cache.borrow_mut().insert(key, db_result);
                db_result
            }
        };

        // 记录存储读取
        let key_bytes = index.to_be_bytes::<32>();
        let key_b256 = B256::from_slice(&key_bytes);
        let mut buf = Vec::with_capacity(40);
        result.to_compact(&mut buf);
        self.push_log(ReadLogEntry::Storage { address, key: key_b256, data: buf });

        Ok(result)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        // 先查缓存
        if let Some(&cached) = self.block_hash_cache.borrow().get(&number) {
            return Ok(cached);
        }

        // 缓存未命中，查询数据库
        let mut inner_db = StateProviderDatabase::new(self.state_provider);
        let hash = inner_db.block_hash(number)?;

        // 写入缓存
        self.block_hash_cache.borrow_mut().insert(number, hash);
        Ok(hash)
    }
}

/// 写入读取日志到文件（文本格式，用于调试）
pub(crate) fn write_read_logs(block_number: u64, read_logs: &[ReadLogEntry]) -> eyre::Result<()> {
    let log_file_path = format!("block_{}_reads.log", block_number);
    let mut log_file = File::create(&log_file_path)?;

    // 按照访问顺序写入日志
    for entry in read_logs {
        match entry {
            ReadLogEntry::Account { address, data } => {
                // 格式: address, 0x{账户数据不解码}（十六进制）
                writeln!(log_file, "{}, 0x{}", address, hex::encode(data))?;
            }
            ReadLogEntry::Storage { address, key, data } => {
                // 格式: address 0x{key}, 0x{存储数据}（十六进制）
                writeln!(log_file, "{} 0x{:x}, 0x{}", address, key, hex::encode(data))?;
            }
        }
    }

    info!("Read log written to: {} ({} entries)", log_file_path, read_logs.len());
    Ok(())
}
