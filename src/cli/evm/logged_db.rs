//! 日志数据库模块
//! 
//! 提供从日志数据读取的 Database 实现，用于 EVM 执行

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use alloy_primitives::{Address, B256, U256};
use reth_codecs::Compact;
use reth_primitives::Account;
use reth_revm::database::StateProviderDatabase;

use crate::revm::Database as RevmDatabase;
use crate::revm::state::{AccountInfo, Bytecode};

/// 全局 Bytecode 缓存（合约代码不可变，可以安全缓存）
/// 使用 RwLock 实现线程安全的读写访问
/// 注意：Bytecode 内部使用 Bytes（引用计数），克隆成本很低
pub struct BytecodeCache {
    cache: RwLock<HashMap<B256, Bytecode>>,
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
            cache: RwLock::new(HashMap::with_capacity(100_000)),
        }
    }
    
    /// 查询缓存（Bytecode 克隆成本很低，内部使用 Bytes 引用计数）
    #[inline]
    pub fn get(&self, code_hash: &B256) -> Option<Bytecode> {
        self.cache.read().ok()?.get(code_hash).cloned()
    }
    
    /// 插入缓存
    #[inline]
    pub fn insert(&self, code_hash: B256, bytecode: Bytecode) {
        if let Ok(mut cache) = self.cache.write() {
            cache.insert(code_hash, bytecode);
        }
    }
    
    /// 获取缓存大小
    pub fn len(&self) -> usize {
        self.cache.read().map(|c| c.len()).unwrap_or(0)
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

