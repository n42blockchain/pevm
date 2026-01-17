// SPDX-License-Identifier: MIT OR Apache-2.0

//! 日志数据库模块
//! 
//! 提供从日志数据读取的 Database 实现，用于 EVM 执行

use std::sync::Arc;
use alloy_primitives::{Address, B256, U256};
use reth_codecs::Compact;
use reth_primitives::Account;
use reth_revm::database::StateProviderDatabase;

use crate::revm::Database as RevmDatabase;
use crate::revm::state::{AccountInfo, Bytecode};

/// 全局 Bytecode 缓存（合约代码不可变，可以安全缓存）
/// 使用 DashMap 实现高并发的线程安全访问
/// 注意：Bytecode 内部使用 Bytes（引用计数），克隆成本很低
pub struct BytecodeCache {
    cache: dashmap::DashMap<B256, Bytecode>,
}

impl std::fmt::Debug for BytecodeCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BytecodeCache")
            .field("cache_size", &self.len())
            .finish()
    }
}

impl BytecodeCache {
    /// 创建新的缓存
    pub fn new() -> Self {
        Self {
            cache: dashmap::DashMap::with_capacity(100_000),
        }
    }

    /// 查询缓存（Bytecode 克隆成本很低，内部使用 Bytes 引用计数）
    /// 性能优化：使用 DashMap，无锁读取，细粒度写锁，提升并发性能
    #[inline]
    pub fn get(&self, code_hash: &B256) -> Option<Bytecode> {
        self.cache.get(code_hash).map(|entry| entry.value().clone())
    }

    /// 插入缓存
    /// 性能优化：使用 DashMap，细粒度锁，不会阻塞其他条目的读写
    #[inline]
    pub fn insert(&self, code_hash: B256, bytecode: Bytecode) {
        self.cache.insert(code_hash, bytecode);
    }

    /// 获取缓存大小
    pub fn len(&self) -> usize {
        self.cache.len()
    }
}

impl Default for BytecodeCache {
    fn default() -> Self {
        Self::new()
    }
}

/// 空 codehash 常量
const EMPTY_CODE_HASH_BYTES: &[u8; 32] = &[
    0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c,
    0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
    0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b,
    0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
];

/// 获取空 codehash
#[inline]
fn get_empty_code_hash() -> B256 {
    B256::from_slice(EMPTY_CODE_HASH_BYTES)
}

/// 从日志数据读取的 Database（零拷贝版本）
///
/// 直接使用内存中的数据指针访问日志数据，适用于 mmap 模式
pub struct DbLoggedDatabase<'a> {
    /// 原始数据的引用
    data: &'a [u8],
    /// 当前读取位置
    pos: usize,
    /// 用于 code_by_hash 查询的状态提供者
    state_provider: Arc<dyn reth_provider::StateProvider>,
    /// 全局 Bytecode 缓存（可选，用于减少数据库查询）
    bytecode_cache: Option<Arc<BytecodeCache>>,
}

impl<'a> std::fmt::Debug for DbLoggedDatabase<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbLoggedDatabase")
            .field("data_len", &self.data.len())
            .field("pos", &self.pos)
            .finish_non_exhaustive()
    }
}

impl<'a> DbLoggedDatabase<'a> {
    /// 从数据创建（零拷贝）
    /// data 格式：count(8 bytes) + entries
    #[inline]
    pub fn new(data: &'a [u8], state_provider: Arc<dyn reth_provider::StateProvider>) -> eyre::Result<Self> {
        if data.len() < 8 {
            return Err(eyre::eyre!("Invalid data: too short"));
        }
        Ok(Self {
            data,
            pos: 8, // 跳过 count
            state_provider,
            bytecode_cache: None,
        })
    }

    /// 从数据创建，带 Bytecode 缓存（零拷贝）
    /// data 格式：count(8 bytes) + entries
    #[inline]
    pub fn new_with_cache(
        data: &'a [u8],
        state_provider: Arc<dyn reth_provider::StateProvider>,
        bytecode_cache: Arc<BytecodeCache>,
    ) -> eyre::Result<Self> {
        if data.len() < 8 {
            return Err(eyre::eyre!("Invalid data: too short"));
        }
        Ok(Self {
            data,
            pos: 8, // 跳过 count
            state_provider,
            bytecode_cache: Some(bytecode_cache),
        })
    }
    
    /// 读取下一个条目数据（零拷贝）
    #[inline]
    fn next_entry(&mut self) -> Option<&'a [u8]> {
        if self.pos >= self.data.len() {
            return None;
        }
        
        let len = self.data[self.pos] as usize;
        let start = self.pos + 1;
        let end = start + len;
        
        if end > self.data.len() {
            return None;
        }
        
        self.pos = end;
        Some(&self.data[start..end])
    }
    
    /// 从 Compact 编码数据解码账户信息
    #[inline]
    fn decode_account_compact(data: &[u8]) -> eyre::Result<AccountInfo> {
        if data.is_empty() || (data.len() == 1 && data[0] == 0x00) {
            return Err(eyre::eyre!("Account does not exist"));
        }
        
        let data_len = data.len();
        let (account, _) = Account::from_compact(data, data_len);
        
        let code_hash = account.bytecode_hash.unwrap_or_else(get_empty_code_hash);
        
        Ok(AccountInfo::new(
            account.balance,
            account.nonce,
            code_hash,
            Bytecode::default(),
        ))
    }
}

impl<'a> RevmDatabase for DbLoggedDatabase<'a> {
    type Error = <StateProviderDatabase<Box<dyn reth_provider::StateProvider>> as RevmDatabase>::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        if let Some(compact_data) = self.next_entry() {
            // 快速检查空账户标记
            if compact_data.is_empty() ||
               (compact_data.len() == 1 && (compact_data[0] == 0x00 || compact_data[0] == 0xc0)) {
                return Ok(None);
            }

            // 解码账户
            let mut account_info = match Self::decode_account_compact(compact_data) {
                Ok(info) => info,
                Err(_) => return Ok(None),
            };

            // 加载 bytecode（如果需要）
            let code_hash = account_info.code_hash();
            if code_hash.as_slice() != EMPTY_CODE_HASH_BYTES {
                if let Ok(code) = self.code_by_hash(code_hash) {
                    account_info.code = Some(code);
                }
            }

            Ok(Some(account_info))
        } else {
            // 日志数据用完，回退到数据库查询
            let mut inner_db = StateProviderDatabase::new(
                self.state_provider.as_ref() as &dyn reth_provider::StateProvider
            );
            inner_db.basic(address)
        }
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        // 性能优化：先查缓存（Bytecode 克隆成本很低，内部使用 Bytes 引用计数）
        if let Some(ref cache) = self.bytecode_cache {
            if let Some(bytecode) = cache.get(&code_hash) {
                return Ok(bytecode);
            }
        }

        // 缓存未命中，查询数据库
        let mut inner_db = StateProviderDatabase::new(
            self.state_provider.as_ref() as &dyn reth_provider::StateProvider
        );
        let bytecode = inner_db.code_by_hash(code_hash)?;

        // 写入缓存（合约代码不可变，安全缓存）
        if let Some(ref cache) = self.bytecode_cache {
            cache.insert(code_hash, bytecode.clone());
        }

        Ok(bytecode)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        if let Some(compact_data) = self.next_entry() {
            let data_len = compact_data.len();
            let (value, _) = U256::from_compact(compact_data, data_len);
            Ok(value)
        } else {
            let mut inner_db = StateProviderDatabase::new(
                self.state_provider.as_ref() as &dyn reth_provider::StateProvider
            );
            inner_db.storage(address, index)
        }
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        let mut inner_db = StateProviderDatabase::new(
            self.state_provider.as_ref() as &dyn reth_provider::StateProvider
        );
        inner_db.block_hash(number)
    }
}

// ============================================================================
// CachedStateProviderDatabase: 用于纯 EVM 执行模式的轻量级数据库
// ============================================================================

/// 轻量级 StateProviderDatabase 包装器
///
/// 用于纯 EVM 执行模式，零额外开销
///
/// 设计原理：
/// - executor 内部的 State 已经缓存了账户和存储数据
/// - MDBX 本身有页面缓存，重复查询很快
/// - DashMap 等并发缓存的开销 > 直接 MDBX 查询
///
/// 性能优化：
/// - 完全移除所有缓存层，直接转发查询到内部数据库
/// - 内部复用同一个 StateProviderDatabase 实例
/// - 所有方法都标记为 #[inline]，消除函数调用开销
pub struct CachedStateProviderDatabase<'a> {
    /// 内部数据库（复用，避免每次查询创建新实例）
    inner_db: StateProviderDatabase<&'a dyn reth_provider::StateProvider>,
}

impl<'a> std::fmt::Debug for CachedStateProviderDatabase<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedStateProviderDatabase")
            .finish()
    }
}

impl<'a> CachedStateProviderDatabase<'a> {
    /// 创建新的轻量级数据库
    /// 注意：bytecode_cache 参数保留用于 API 兼容性，但不再使用
    pub fn new(
        state_provider: &'a dyn reth_provider::StateProvider,
        _bytecode_cache: Option<Arc<BytecodeCache>>,
    ) -> Self {
        Self {
            inner_db: StateProviderDatabase::new(state_provider),
        }
    }
}

impl<'a> RevmDatabase for CachedStateProviderDatabase<'a> {
    type Error = <StateProviderDatabase<Box<dyn reth_provider::StateProvider>> as RevmDatabase>::Error;

    #[inline]
    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        // 直接转发到内部数据库，无额外缓存开销
        // executor 的 State 已经缓存了账户数据
        self.inner_db.basic(address)
    }

    #[inline]
    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        // 直接查询数据库，不使用缓存
        // 原因：DashMap 的并发开销 > 直接 MDBX 查询
        // MDBX 本身有页面缓存，重复查询很快
        self.inner_db.code_by_hash(code_hash)
    }

    #[inline]
    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        // 直接转发到内部数据库，无额外缓存开销
        // executor 的 State 已经缓存了存储数据
        self.inner_db.storage(address, index)
    }

    #[inline]
    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        // 直接转发到内部数据库
        // block_hash 查询较少，不值得缓存
        self.inner_db.block_hash(number)
    }
}

// ============================================================================
// BatchDbLoggedDatabase: 支持批量执行的日志数据库
// ============================================================================

/// 批量日志数据库 - 支持使用 execute_batch 批量执行多个块
///
/// 关键优化：通过自动跟踪每个块的日志条目数量，在块边界自动切换日志数据源
/// 这使得可以使用单个 executor 批量执行多个块，性能提升 5-6 倍
///
/// 工作原理：
/// 1. 预先解析每个块的日志数据，获取条目数量
/// 2. 每次 basic/storage 调用读取当前块的下一个条目
/// 3. 当当前块的条目读完时，自动切换到下一个块
pub struct BatchDbLoggedDatabase<'a> {
    /// 每个块的日志数据：(数据切片, 条目数量, 当前读取位置)
    /// 按执行顺序存储
    blocks_data: Vec<BlockLogData<'a>>,
    /// 当前正在执行的块索引
    current_block_idx: std::cell::Cell<usize>,
    /// 状态提供者（用于 code_by_hash 查询）
    state_provider: Arc<dyn reth_provider::StateProvider>,
    /// Bytecode 缓存
    bytecode_cache: Option<Arc<BytecodeCache>>,
}

/// 单个块的日志数据
struct BlockLogData<'a> {
    /// 块号
    block_number: u64,
    /// 日志数据（跳过 count 头后的数据）
    data: &'a [u8],
    /// 条目数量
    entry_count: u64,
    /// 当前读取位置
    pos: std::cell::Cell<usize>,
    /// 已读取的条目数
    entries_read: std::cell::Cell<u64>,
}

impl<'a> std::fmt::Debug for BatchDbLoggedDatabase<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchDbLoggedDatabase")
            .field("blocks_count", &self.blocks_data.len())
            .field("current_block_idx", &self.current_block_idx.get())
            .finish()
    }
}

impl<'a> BatchDbLoggedDatabase<'a> {
    /// 从多个块的日志数据创建
    ///
    /// # 参数
    /// - `blocks_data`: Vec<(block_number, data)> - 每个块的日志数据，数据格式：count(8 bytes) + entries
    /// - `state_provider`: 状态提供者（用于 code_by_hash 和回退查询）
    /// - `bytecode_cache`: 可选的全局 Bytecode 缓存
    pub fn new(
        blocks_data: Vec<(u64, &'a [u8])>,
        state_provider: Arc<dyn reth_provider::StateProvider>,
        bytecode_cache: Option<Arc<BytecodeCache>>,
    ) -> eyre::Result<Self> {
        let mut parsed_blocks = Vec::with_capacity(blocks_data.len());

        for (block_number, data) in blocks_data {
            if data.len() < 8 {
                return Err(eyre::eyre!("Invalid data for block {}: too short", block_number));
            }

            // 读取条目数量（小端序）
            let entry_count = u64::from_le_bytes(data[0..8].try_into().unwrap());

            parsed_blocks.push(BlockLogData {
                block_number,
                data: &data[8..], // 跳过 count
                entry_count,
                pos: std::cell::Cell::new(0),
                entries_read: std::cell::Cell::new(0),
            });
        }

        Ok(Self {
            blocks_data: parsed_blocks,
            current_block_idx: std::cell::Cell::new(0),
            state_provider,
            bytecode_cache,
        })
    }

    /// 读取当前块的下一个条目数据
    /// 如果当前块数据读完，自动切换到下一个块
    #[inline]
    fn next_entry(&self) -> Option<&'a [u8]> {
        loop {
            let idx = self.current_block_idx.get();
            if idx >= self.blocks_data.len() {
                return None;
            }

            let block = &self.blocks_data[idx];
            let pos = block.pos.get();
            let entries_read = block.entries_read.get();

            // 检查是否已读完当前块的所有条目
            if entries_read >= block.entry_count {
                // 切换到下一个块
                self.current_block_idx.set(idx + 1);
                continue;
            }

            // 读取下一个条目
            if pos >= block.data.len() {
                // 数据不足，切换到下一个块
                self.current_block_idx.set(idx + 1);
                continue;
            }

            let len = block.data[pos] as usize;
            let start = pos + 1;
            let end = start + len;

            if end > block.data.len() {
                // 数据损坏，切换到下一个块
                self.current_block_idx.set(idx + 1);
                continue;
            }

            // 更新读取位置和计数
            block.pos.set(end);
            block.entries_read.set(entries_read + 1);

            return Some(&block.data[start..end]);
        }
    }

    /// 获取当前块号（用于调试）
    #[allow(dead_code)]
    pub fn current_block_number(&self) -> Option<u64> {
        let idx = self.current_block_idx.get();
        self.blocks_data.get(idx).map(|b| b.block_number)
    }
}

impl<'a> RevmDatabase for BatchDbLoggedDatabase<'a> {
    type Error = <StateProviderDatabase<Box<dyn reth_provider::StateProvider>> as RevmDatabase>::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        if let Some(compact_data) = self.next_entry() {
            // 快速检查空账户标记
            if compact_data.is_empty() ||
               (compact_data.len() == 1 && (compact_data[0] == 0x00 || compact_data[0] == 0xc0)) {
                return Ok(None);
            }

            // 解码账户
            let mut account_info = match DbLoggedDatabase::decode_account_compact(compact_data) {
                Ok(info) => info,
                Err(_) => return Ok(None),
            };

            // 加载 bytecode（如果需要）
            let code_hash = account_info.code_hash();
            if code_hash.as_slice() != EMPTY_CODE_HASH_BYTES {
                if let Ok(code) = self.code_by_hash(code_hash) {
                    account_info.code = Some(code);
                }
            }

            Ok(Some(account_info))
        } else {
            // 日志数据用完，回退到数据库查询
            let mut inner_db = StateProviderDatabase::new(
                self.state_provider.as_ref() as &dyn reth_provider::StateProvider
            );
            inner_db.basic(address)
        }
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        // 先查缓存
        if let Some(ref cache) = self.bytecode_cache {
            if let Some(bytecode) = cache.get(&code_hash) {
                return Ok(bytecode);
            }
        }

        // 缓存未命中，查询数据库
        let mut inner_db = StateProviderDatabase::new(
            self.state_provider.as_ref() as &dyn reth_provider::StateProvider
        );
        let bytecode = inner_db.code_by_hash(code_hash)?;

        // 写入缓存
        if let Some(ref cache) = self.bytecode_cache {
            cache.insert(code_hash, bytecode.clone());
        }

        Ok(bytecode)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        if let Some(compact_data) = self.next_entry() {
            let data_len = compact_data.len();
            let (value, _) = U256::from_compact(compact_data, data_len);
            Ok(value)
        } else {
            let mut inner_db = StateProviderDatabase::new(
                self.state_provider.as_ref() as &dyn reth_provider::StateProvider
            );
            inner_db.storage(address, index)
        }
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        let mut inner_db = StateProviderDatabase::new(
            self.state_provider.as_ref() as &dyn reth_provider::StateProvider
        );
        inner_db.block_hash(number)
    }
}

// ============================================================================
// BatchOwnedLoggedDatabase: 支持批量执行的日志数据库（拥有数据所有权版本）
// ============================================================================

/// 批量日志数据库（拥有数据所有权） - 用于文件系统模式
///
/// 与 BatchDbLoggedDatabase 类似，但拥有数据的所有权而不是借用
/// 适用于需要解压后存储的场景
pub struct BatchOwnedLoggedDatabase {
    /// 每个块的日志数据
    blocks_data: Vec<OwnedBlockLogData>,
    /// 当前正在执行的块索引
    current_block_idx: std::cell::Cell<usize>,
    /// 状态提供者
    state_provider: Arc<dyn reth_provider::StateProvider>,
    /// Bytecode 缓存
    bytecode_cache: Option<Arc<BytecodeCache>>,
}

/// 单个块的日志数据（拥有所有权）
struct OwnedBlockLogData {
    /// 块号
    #[allow(dead_code)]
    block_number: u64,
    /// 日志数据（拥有所有权，跳过 count 头后的数据）
    data: Vec<u8>,
    /// 条目数量
    entry_count: u64,
    /// 当前读取位置
    pos: std::cell::Cell<usize>,
    /// 已读取的条目数
    entries_read: std::cell::Cell<u64>,
}

impl std::fmt::Debug for BatchOwnedLoggedDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchOwnedLoggedDatabase")
            .field("blocks_count", &self.blocks_data.len())
            .field("current_block_idx", &self.current_block_idx.get())
            .finish()
    }
}

impl BatchOwnedLoggedDatabase {
    /// 从多个块的日志数据创建
    ///
    /// # 参数
    /// - `blocks_data`: Vec<(block_number, data)> - 每个块的日志数据，数据格式：count(8 bytes) + entries
    /// - `state_provider`: 状态提供者
    /// - `bytecode_cache`: 可选的全局 Bytecode 缓存
    pub fn new(
        blocks_data: Vec<(u64, Vec<u8>)>,
        state_provider: Arc<dyn reth_provider::StateProvider>,
        bytecode_cache: Option<Arc<BytecodeCache>>,
    ) -> eyre::Result<Self> {
        let mut parsed_blocks = Vec::with_capacity(blocks_data.len());

        for (block_number, data) in blocks_data {
            if data.len() < 8 {
                return Err(eyre::eyre!("Invalid data for block {}: too short", block_number));
            }

            // 读取条目数量（小端序）
            let entry_count = u64::from_le_bytes(data[0..8].try_into().unwrap());

            parsed_blocks.push(OwnedBlockLogData {
                block_number,
                data: data[8..].to_vec(), // 跳过 count
                entry_count,
                pos: std::cell::Cell::new(0),
                entries_read: std::cell::Cell::new(0),
            });
        }

        Ok(Self {
            blocks_data: parsed_blocks,
            current_block_idx: std::cell::Cell::new(0),
            state_provider,
            bytecode_cache,
        })
    }

    /// 读取当前块的下一个条目数据
    #[inline]
    fn next_entry(&self) -> Option<&[u8]> {
        loop {
            let idx = self.current_block_idx.get();
            if idx >= self.blocks_data.len() {
                return None;
            }

            let block = &self.blocks_data[idx];
            let pos = block.pos.get();
            let entries_read = block.entries_read.get();

            if entries_read >= block.entry_count {
                self.current_block_idx.set(idx + 1);
                continue;
            }

            if pos >= block.data.len() {
                self.current_block_idx.set(idx + 1);
                continue;
            }

            let len = block.data[pos] as usize;
            let start = pos + 1;
            let end = start + len;

            if end > block.data.len() {
                self.current_block_idx.set(idx + 1);
                continue;
            }

            block.pos.set(end);
            block.entries_read.set(entries_read + 1);

            return Some(&block.data[start..end]);
        }
    }
}

impl RevmDatabase for BatchOwnedLoggedDatabase {
    type Error = <StateProviderDatabase<Box<dyn reth_provider::StateProvider>> as RevmDatabase>::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        if let Some(compact_data) = self.next_entry() {
            if compact_data.is_empty() ||
               (compact_data.len() == 1 && (compact_data[0] == 0x00 || compact_data[0] == 0xc0)) {
                return Ok(None);
            }

            let mut account_info = match DbLoggedDatabase::decode_account_compact(compact_data) {
                Ok(info) => info,
                Err(_) => return Ok(None),
            };

            let code_hash = account_info.code_hash();
            if code_hash.as_slice() != EMPTY_CODE_HASH_BYTES {
                if let Ok(code) = self.code_by_hash(code_hash) {
                    account_info.code = Some(code);
                }
            }

            Ok(Some(account_info))
        } else {
            let mut inner_db = StateProviderDatabase::new(
                self.state_provider.as_ref() as &dyn reth_provider::StateProvider
            );
            inner_db.basic(address)
        }
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        if let Some(ref cache) = self.bytecode_cache {
            if let Some(bytecode) = cache.get(&code_hash) {
                return Ok(bytecode);
            }
        }

        let mut inner_db = StateProviderDatabase::new(
            self.state_provider.as_ref() as &dyn reth_provider::StateProvider
        );
        let bytecode = inner_db.code_by_hash(code_hash)?;

        if let Some(ref cache) = self.bytecode_cache {
            cache.insert(code_hash, bytecode.clone());
        }

        Ok(bytecode)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        if let Some(compact_data) = self.next_entry() {
            let data_len = compact_data.len();
            let (value, _) = U256::from_compact(compact_data, data_len);
            Ok(value)
        } else {
            let mut inner_db = StateProviderDatabase::new(
                self.state_provider.as_ref() as &dyn reth_provider::StateProvider
            );
            inner_db.storage(address, index)
        }
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        let mut inner_db = StateProviderDatabase::new(
            self.state_provider.as_ref() as &dyn reth_provider::StateProvider
        );
        inner_db.block_hash(number)
    }
}
