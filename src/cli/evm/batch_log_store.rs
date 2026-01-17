// SPDX-License-Identifier: MIT OR Apache-2.0

//! 批次级日志存储模块
//!
//! 设计目标：
//! - 按批次（默认100块）存储日志数据
//! - 使用批次第一个块号作为索引，大幅减少索引数量
//! - 整批数据使用 zstd 压缩
//! - 生成和回放都使用批量执行，State 缓存行为一致，避免错位
//!
//! 文件格式 V2：
//! - blocks.batchlog.dat - 纯数据文件（紧凑压缩数据，无填充）
//! - blocks.batchlog.idx - 索引文件（头部 + 索引条目）

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write as IoWrite, BufWriter, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::cell::RefCell;

use alloy_primitives::{Address, B256, U256};
use dashmap::DashMap;
use reth_codecs::Compact;
use reth_primitives::Account;
use reth_revm::database::StateProviderDatabase;
use tracing::{info, debug};

use crate::revm::Database as RevmDatabase;
use crate::revm::state::{AccountInfo, Bytecode};

use super::logged_db::BytecodeCache;

// ============================================================================
// 文件格式常量
// ============================================================================

/// 索引文件魔数
const MAGIC: &[u8; 8] = b"BATCHLG2";

/// 版本号
const VERSION: u32 = 2;

/// 索引文件头部大小（64字节，紧凑）
const HEADER_SIZE: usize = 64;

// ============================================================================
// 文件格式结构
// ============================================================================

/// 索引文件头（固定 64 字节）
///
/// 布局：
/// - [0..8]: magic "BATCHLG2"
/// - [8..12]: version (u32 LE)
/// - [12..20]: batch_count (u64 LE) - 批次数量
/// - [20..28]: first_block_number (u64 LE)
/// - [28..36]: last_block_number (u64 LE)
/// - [36..44]: batch_size (u64 LE) - 每批块数（如100）
/// - [44..52]: total_data_size (u64 LE) - 数据文件总大小
/// - [52..64]: reserved
#[derive(Debug, Clone)]
pub struct BatchFileHeader {
    pub batch_count: u64,
    pub first_block_number: u64,
    pub last_block_number: u64,
    pub batch_size: u64,
    pub total_data_size: u64,
}

impl BatchFileHeader {
    pub fn from_bytes(data: &[u8]) -> eyre::Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(eyre::eyre!("Header too short"));
        }

        if &data[0..8] != MAGIC {
            return Err(eyre::eyre!("Invalid magic number"));
        }

        let version = u32::from_le_bytes(data[8..12].try_into().unwrap());
        if version != VERSION {
            return Err(eyre::eyre!("Unsupported version: {}", version));
        }

        Ok(Self {
            batch_count: u64::from_le_bytes(data[12..20].try_into().unwrap()),
            first_block_number: u64::from_le_bytes(data[20..28].try_into().unwrap()),
            last_block_number: u64::from_le_bytes(data[28..36].try_into().unwrap()),
            batch_size: u64::from_le_bytes(data[36..44].try_into().unwrap()),
            total_data_size: u64::from_le_bytes(data[44..52].try_into().unwrap()),
        })
    }

    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..8].copy_from_slice(MAGIC);
        buf[8..12].copy_from_slice(&VERSION.to_le_bytes());
        buf[12..20].copy_from_slice(&self.batch_count.to_le_bytes());
        buf[20..28].copy_from_slice(&self.first_block_number.to_le_bytes());
        buf[28..36].copy_from_slice(&self.last_block_number.to_le_bytes());
        buf[36..44].copy_from_slice(&self.batch_size.to_le_bytes());
        buf[44..52].copy_from_slice(&self.total_data_size.to_le_bytes());
        buf
    }
}

/// 批次索引条目（固定 32 字节）
///
/// 布局：
/// - [0..8]: batch_start_block (u64 LE) - 批次起始块号
/// - [8..16]: data_offset (u64 LE) - 数据在 .dat 文件中的偏移
/// - [16..24]: compressed_size (u64 LE) - 压缩后大小
/// - [24..32]: entry_count (u64 LE) - 日志条目数量（整批）
#[derive(Debug, Clone, Copy)]
pub struct BatchIndexEntry {
    pub batch_start_block: u64,
    pub data_offset: u64,
    pub compressed_size: u64,
    pub entry_count: u64,
}

impl BatchIndexEntry {
    pub const SIZE: usize = 32;

    pub fn from_bytes(data: &[u8]) -> eyre::Result<Self> {
        if data.len() < Self::SIZE {
            return Err(eyre::eyre!("Index entry too short"));
        }

        Ok(Self {
            batch_start_block: u64::from_le_bytes(data[0..8].try_into().unwrap()),
            data_offset: u64::from_le_bytes(data[8..16].try_into().unwrap()),
            compressed_size: u64::from_le_bytes(data[16..24].try_into().unwrap()),
            entry_count: u64::from_le_bytes(data[24..32].try_into().unwrap()),
        })
    }

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..8].copy_from_slice(&self.batch_start_block.to_le_bytes());
        buf[8..16].copy_from_slice(&self.data_offset.to_le_bytes());
        buf[16..24].copy_from_slice(&self.compressed_size.to_le_bytes());
        buf[24..32].copy_from_slice(&self.entry_count.to_le_bytes());
        buf
    }
}

// ============================================================================
// BatchLogStore - 批次日志存储
// ============================================================================

/// 批次数据（已加载到内存并解压）
pub struct BatchData {
    /// 解压后的日志数据
    pub data: Vec<u8>,
    /// 日志条目数量
    pub entry_count: u64,
    /// 批次起始块号
    pub start_block: u64,
}

/// 批次日志存储
pub struct BatchLogStore {
    header: BatchFileHeader,
    /// 批次索引：batch_start_block -> BatchIndexEntry
    index: HashMap<u64, BatchIndexEntry>,
    /// 已加载的批次：batch_start_block -> Arc<BatchData>
    loaded_batches: DashMap<u64, Arc<BatchData>>,
    /// 内存映射的数据文件（避免文件 I/O 和锁竞争）
    dat_mmap: memmap2::Mmap,
}

impl std::fmt::Debug for BatchLogStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchLogStore")
            .field("batch_count", &self.header.batch_count)
            .field("batch_size", &self.header.batch_size)
            .field("loaded_batches", &self.loaded_batches.len())
            .finish()
    }
}

impl BatchLogStore {
    /// 从文件打开存储
    /// path 可以是 .batchlog 或 .batchlog.idx
    pub fn open<P: AsRef<Path>>(path: P) -> eyre::Result<Self> {
        let path = path.as_ref();

        // 确定索引文件和数据文件路径
        let (idx_path, dat_path) = Self::resolve_paths(path)?;

        // 读取索引文件
        let mut idx_file = File::open(&idx_path)?;

        // 读取头部
        let mut header_buf = [0u8; HEADER_SIZE];
        idx_file.read_exact(&mut header_buf)?;
        let header = BatchFileHeader::from_bytes(&header_buf)?;

        // 读取索引表
        let index_size = header.batch_count as usize * BatchIndexEntry::SIZE;
        let mut index_buf = vec![0u8; index_size];
        idx_file.read_exact(&mut index_buf)?;

        let mut index = HashMap::with_capacity(header.batch_count as usize);
        for i in 0..header.batch_count as usize {
            let entry_data = &index_buf[i * BatchIndexEntry::SIZE..(i + 1) * BatchIndexEntry::SIZE];
            let entry = BatchIndexEntry::from_bytes(entry_data)?;
            index.insert(entry.batch_start_block, entry);
        }

        // 使用内存映射打开数据文件（避免文件 I/O 和锁竞争）
        let dat_file = File::open(&dat_path)?;
        let dat_mmap = unsafe { memmap2::Mmap::map(&dat_file)? };

        info!(
            "Opened batch log store: {} batches, batch_size={}, range={}-{}, data_size={}",
            header.batch_count, header.batch_size,
            header.first_block_number, header.last_block_number,
            dat_mmap.len()
        );

        Ok(Self {
            header,
            index,
            loaded_batches: DashMap::new(),
            dat_mmap,
        })
    }

    /// 解析文件路径，返回 (idx_path, dat_path)
    fn resolve_paths(path: &Path) -> eyre::Result<(PathBuf, PathBuf)> {
        let path_str = path.to_string_lossy();

        if path_str.ends_with(".batchlog.idx") {
            let base = path_str.trim_end_matches(".idx");
            Ok((path.to_path_buf(), PathBuf::from(format!("{}.dat", base))))
        } else if path_str.ends_with(".batchlog.dat") {
            let base = path_str.trim_end_matches(".dat");
            Ok((PathBuf::from(format!("{}.idx", base)), path.to_path_buf()))
        } else if path_str.ends_with(".batchlog") {
            Ok((
                PathBuf::from(format!("{}.idx", path_str)),
                PathBuf::from(format!("{}.dat", path_str)),
            ))
        } else {
            // 假设是基础路径
            Ok((
                path.with_extension("batchlog.idx"),
                path.with_extension("batchlog.dat"),
            ))
        }
    }

    /// 获取文件头
    pub fn header(&self) -> &BatchFileHeader {
        &self.header
    }

    /// 获取批次大小
    pub fn batch_size(&self) -> u64 {
        self.header.batch_size
    }

    /// 根据块号计算批次起始块号
    pub fn batch_start_for_block(&self, block_number: u64) -> u64 {
        let batch_size = self.header.batch_size;
        let first = self.header.first_block_number;
        let offset = block_number.saturating_sub(first);
        let batch_index = offset / batch_size;
        first + batch_index * batch_size
    }

    /// 检查块是否在日志范围内
    pub fn contains_block(&self, block_number: u64) -> bool {
        block_number >= self.header.first_block_number &&
        block_number <= self.header.last_block_number
    }

    /// 加载批次数据
    pub fn load_batch(&self, batch_start: u64) -> eyre::Result<Arc<BatchData>> {
        // 检查是否已加载
        if let Some(data) = self.loaded_batches.get(&batch_start) {
            return Ok(data.clone());
        }

        // 获取索引
        let entry = self.index.get(&batch_start)
            .ok_or_else(|| eyre::eyre!("Batch {} not found in index", batch_start))?;

        // 从内存映射读取压缩数据（零拷贝，无锁）
        let offset = entry.data_offset as usize;
        let end = offset + entry.compressed_size as usize;
        if end > self.dat_mmap.len() {
            return Err(eyre::eyre!(
                "Batch {} data out of bounds: offset={}, size={}, mmap_len={}",
                batch_start, offset, entry.compressed_size, self.dat_mmap.len()
            ));
        }
        let compressed = &self.dat_mmap[offset..end];

        // 解压
        let data = zstd::decode_all(compressed)?;

        let batch_data = Arc::new(BatchData {
            data,
            entry_count: entry.entry_count,
            start_block: batch_start,
        });

        // 存入缓存
        self.loaded_batches.entry(batch_start)
            .or_insert(batch_data.clone());

        Ok(batch_data)
    }

    /// 为指定块范围加载批次
    pub fn load_batch_for_range(&self, start_block: u64, end_block: u64) -> eyre::Result<Arc<BatchData>> {
        let batch_start = self.batch_start_for_block(start_block);

        // 验证整个范围在同一批次内
        let batch_end = batch_start + self.header.batch_size - 1;
        if end_block > batch_end {
            return Err(eyre::eyre!(
                "Block range {}-{} spans multiple batches (batch {}-{})",
                start_block, end_block, batch_start, batch_end
            ));
        }

        self.load_batch(batch_start)
    }

    /// 释放批次
    pub fn release_batch(&self, batch_start: u64) {
        self.loaded_batches.remove(&batch_start);
    }

    /// 释放所有批次
    pub fn release_all(&self) {
        self.loaded_batches.clear();
    }
}

// ============================================================================
// BatchLogStoreWriter - 写入器
// ============================================================================

/// 批次日志写入器
pub struct BatchLogStoreWriter {
    dat_writer: BufWriter<File>,
    idx_path: PathBuf,
    compression_level: i32,
    current_offset: u64,
    entries: Vec<BatchIndexEntry>,
    first_block_number: Option<u64>,
    last_block_number: Option<u64>,
    batch_size: u64,
}

impl BatchLogStoreWriter {
    /// 创建新的写入器
    /// path 应该是基础路径（如 dir/blocks.batchlog），会自动创建 .dat 和 .idx 文件
    pub fn new<P: AsRef<Path>>(path: P, batch_size: u64) -> eyre::Result<Self> {
        let path = path.as_ref();
        let path_str = path.to_string_lossy();

        // 确定数据文件和索引文件路径
        let (dat_path, idx_path) = if path_str.ends_with(".batchlog") {
            (
                PathBuf::from(format!("{}.dat", path_str)),
                PathBuf::from(format!("{}.idx", path_str)),
            )
        } else {
            (
                path.with_extension("batchlog.dat"),
                path.with_extension("batchlog.idx"),
            )
        };

        // 创建数据文件
        let dat_file = File::create(&dat_path)?;
        let dat_writer = BufWriter::with_capacity(4 * 1024 * 1024, dat_file);

        info!("Creating batch log files: {} and {}", dat_path.display(), idx_path.display());

        Ok(Self {
            dat_writer,
            idx_path,
            compression_level: 3,
            current_offset: 0,
            entries: Vec::new(),
            first_block_number: None,
            last_block_number: None,
            batch_size,
        })
    }

    /// 设置压缩级别
    pub fn set_compression_level(&mut self, level: i32) {
        self.compression_level = level.clamp(1, 22);
    }

    /// 写入一个批次的日志数据
    pub fn write_batch(
        &mut self,
        batch_start_block: u64,
        batch_end_block: u64,
        data: &[u8],
        entry_count: u64,
    ) -> eyre::Result<()> {
        // 更新块号范围
        if self.first_block_number.is_none() {
            self.first_block_number = Some(batch_start_block);
        }
        self.last_block_number = Some(batch_end_block);

        let data_offset = self.current_offset;

        // 压缩数据
        let compressed = zstd::encode_all(data, self.compression_level)?;
        let compressed_size = compressed.len() as u64;

        // 写入数据文件（紧凑，无填充）
        self.dat_writer.write_all(&compressed)?;
        self.current_offset += compressed_size;

        // 记录索引
        self.entries.push(BatchIndexEntry {
            batch_start_block,
            data_offset,
            compressed_size,
            entry_count,
        });

        debug!(
            "Batch {}-{}: {} entries, {} -> {} bytes ({:.1}%)",
            batch_start_block, batch_end_block, entry_count,
            data.len(), compressed_size,
            (compressed_size as f64 / data.len() as f64) * 100.0
        );

        Ok(())
    }

    /// 完成写入（消耗 self）
    pub fn finish(mut self) -> eyre::Result<()> {
        self.finish_inner()
    }

    /// 完成写入（通过可变引用，用于无法获取所有权的场景）
    pub fn finish_ref(&mut self) -> eyre::Result<()> {
        self.finish_inner()
    }

    /// 内部完成写入逻辑
    fn finish_inner(&mut self) -> eyre::Result<()> {
        // 刷新数据文件
        self.dat_writer.flush()?;
        let total_data_size = self.current_offset;

        // 写入索引文件
        let mut idx_file = File::create(&self.idx_path)?;

        // 构建并写入头部
        let header = BatchFileHeader {
            batch_count: self.entries.len() as u64,
            first_block_number: self.first_block_number.unwrap_or(0),
            last_block_number: self.last_block_number.unwrap_or(0),
            batch_size: self.batch_size,
            total_data_size,
        };
        idx_file.write_all(&header.to_bytes())?;

        // 写入索引条目
        for entry in &self.entries {
            idx_file.write_all(&entry.to_bytes())?;
        }
        idx_file.flush()?;

        info!(
            "Batch log store written: {} batches, range {}-{}, data size: {} bytes",
            header.batch_count, header.first_block_number, header.last_block_number,
            total_data_size
        );

        Ok(())
    }
}

// ============================================================================
// BatchDatabase - 批次执行数据库
// ============================================================================

/// 空 codehash 常量
const EMPTY_CODE_HASH_BYTES: &[u8; 32] = &[
    0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c,
    0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
    0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b,
    0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
];

#[inline]
fn get_empty_code_hash() -> B256 {
    B256::from_slice(EMPTY_CODE_HASH_BYTES)
}

/// 批次执行数据库
///
/// 包含账户、存储、块哈希缓存，与 CachedStateProviderDatabase 行为一致
/// 缓存策略：
/// - 先从日志读取（主要数据源）
/// - 日志未命中时从数据库读取并缓存
/// - 后续相同查询直接返回缓存
pub struct BatchDatabase {
    batch_data: Arc<BatchData>,
    pos: std::cell::Cell<usize>,
    entries_read: std::cell::Cell<u64>,
    state_provider: Arc<dyn reth_provider::StateProvider>,
    bytecode_cache: Option<Arc<BytecodeCache>>,
    /// 账户缓存：减少数据库回退查询
    account_cache: RefCell<HashMap<Address, Option<AccountInfo>>>,
    /// 存储缓存：减少数据库回退查询
    storage_cache: RefCell<HashMap<(Address, U256), U256>>,
    /// 块哈希缓存：减少数据库查询
    block_hash_cache: RefCell<HashMap<u64, B256>>,
}

impl std::fmt::Debug for BatchDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchDatabase")
            .field("start_block", &self.batch_data.start_block)
            .field("entry_count", &self.batch_data.entry_count)
            .field("entries_read", &self.entries_read.get())
            .field("account_cache_size", &self.account_cache.borrow().len())
            .field("storage_cache_size", &self.storage_cache.borrow().len())
            .field("block_hash_cache_size", &self.block_hash_cache.borrow().len())
            .finish()
    }
}

impl BatchDatabase {
    pub fn from_store(
        store: &BatchLogStore,
        batch_start: u64,
        state_provider: Arc<dyn reth_provider::StateProvider>,
        bytecode_cache: Option<Arc<BytecodeCache>>,
    ) -> eyre::Result<Self> {
        let batch_data = store.load_batch(batch_start)?;
        Ok(Self::new(batch_data, state_provider, bytecode_cache))
    }

    pub fn new(
        batch_data: Arc<BatchData>,
        state_provider: Arc<dyn reth_provider::StateProvider>,
        bytecode_cache: Option<Arc<BytecodeCache>>,
    ) -> Self {
        Self {
            batch_data,
            pos: std::cell::Cell::new(0),
            entries_read: std::cell::Cell::new(0),
            state_provider,
            bytecode_cache,
            account_cache: RefCell::new(HashMap::new()),
            storage_cache: RefCell::new(HashMap::new()),
            block_hash_cache: RefCell::new(HashMap::new()),
        }
    }

    #[inline]
    fn next_entry(&self) -> Option<&[u8]> {
        let pos = self.pos.get();
        let entries_read = self.entries_read.get();

        if entries_read >= self.batch_data.entry_count {
            return None;
        }

        if pos >= self.batch_data.data.len() {
            return None;
        }

        let len = self.batch_data.data[pos] as usize;
        let start = pos + 1;
        let end = start + len;

        if end > self.batch_data.data.len() {
            return None;
        }

        self.pos.set(end);
        self.entries_read.set(entries_read + 1);

        Some(&self.batch_data.data[start..end])
    }

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

impl RevmDatabase for BatchDatabase {
    type Error = <StateProviderDatabase<Box<dyn reth_provider::StateProvider>> as RevmDatabase>::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        // 优先从日志读取（主要数据源，零开销）
        if let Some(compact_data) = self.next_entry() {
            if compact_data.is_empty() ||
               (compact_data.len() == 1 && (compact_data[0] == 0x00 || compact_data[0] == 0xc0)) {
                return Ok(None);
            }

            let account_info = match Self::decode_account_compact(compact_data) {
                Ok(info) => info,
                Err(_) => return Ok(None),
            };

            return Ok(Some(account_info));
        }

        // 日志用完，先查缓存（仅在回退到数据库时使用）
        if let Some(cached) = self.account_cache.borrow().get(&address) {
            return Ok(cached.clone());
        }

        // 缓存未命中，从数据库查询
        let mut inner_db = StateProviderDatabase::new(
            self.state_provider.as_ref() as &dyn reth_provider::StateProvider
        );
        let result = inner_db.basic(address)?;

        // 写入缓存（仅缓存数据库查询结果）
        self.account_cache.borrow_mut().insert(address, result.clone());
        Ok(result)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        // 先查全局缓存（bytecode 不可变，可跨批次共享）
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

        // 写入全局缓存
        if let Some(ref cache) = self.bytecode_cache {
            cache.insert(code_hash, bytecode.clone());
        }

        Ok(bytecode)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        // 优先从日志读取（零开销）
        if let Some(compact_data) = self.next_entry() {
            let data_len = compact_data.len();
            let (value, _) = U256::from_compact(compact_data, data_len);
            return Ok(value);
        }

        // 日志用完，先查缓存
        let key = (address, index);
        if let Some(&cached) = self.storage_cache.borrow().get(&key) {
            return Ok(cached);
        }

        // 缓存未命中，从数据库查询
        let mut inner_db = StateProviderDatabase::new(
            self.state_provider.as_ref() as &dyn reth_provider::StateProvider
        );
        let value = inner_db.storage(address, index)?;

        // 写入缓存
        self.storage_cache.borrow_mut().insert(key, value);
        Ok(value)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        // 先查缓存
        if let Some(&cached) = self.block_hash_cache.borrow().get(&number) {
            return Ok(cached);
        }

        // 缓存未命中，查询数据库
        let mut inner_db = StateProviderDatabase::new(
            self.state_provider.as_ref() as &dyn reth_provider::StateProvider
        );
        let hash = inner_db.block_hash(number)?;

        // 写入缓存
        self.block_hash_cache.borrow_mut().insert(number, hash);
        Ok(hash)
    }
}

// ============================================================================
// BatchLoggingDatabase - 批次日志记录数据库
// ============================================================================

/// 批次日志记录数据库
pub struct BatchLoggingDatabase<DB> {
    inner: DB,
    logs: std::cell::RefCell<Vec<u8>>,
    entry_count: std::cell::Cell<u64>,
}

impl<DB> BatchLoggingDatabase<DB> {
    pub fn new(inner: DB) -> Self {
        Self {
            inner,
            logs: std::cell::RefCell::new(Vec::with_capacity(1024 * 1024)),
            entry_count: std::cell::Cell::new(0),
        }
    }

    pub fn take_logs(&self) -> (Vec<u8>, u64) {
        let data = std::mem::take(&mut *self.logs.borrow_mut());
        let count = self.entry_count.get();
        self.entry_count.set(0);
        (data, count)
    }

    #[inline]
    fn push_entry(&self, data: &[u8]) {
        let len = data.len().min(255) as u8;
        let mut logs = self.logs.borrow_mut();
        logs.push(len);
        logs.extend_from_slice(&data[..len as usize]);
        self.entry_count.set(self.entry_count.get() + 1);
    }
}

impl<DB: std::fmt::Debug> std::fmt::Debug for BatchLoggingDatabase<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchLoggingDatabase")
            .field("inner", &self.inner)
            .field("entry_count", &self.entry_count.get())
            .finish()
    }
}

impl<DB: RevmDatabase> RevmDatabase for BatchLoggingDatabase<DB> {
    type Error = DB::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let result = self.inner.basic(address)?;

        let compact_data = match &result {
            None => vec![0x00],
            Some(info) => {
                let account = Account {
                    nonce: info.nonce,
                    balance: info.balance,
                    bytecode_hash: Some(info.code_hash()),
                };
                let mut buf = Vec::with_capacity(64);
                account.to_compact(&mut buf);
                buf
            }
        };
        self.push_entry(&compact_data);

        Ok(result)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.inner.code_by_hash(code_hash)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let result = self.inner.storage(address, index)?;

        let mut compact_data = Vec::with_capacity(32);
        result.to_compact(&mut compact_data);
        self.push_entry(&compact_data);

        Ok(result)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.inner.block_hash(number)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_batch_log_format_v2() -> eyre::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.batchlog");

        // 写入测试数据
        {
            let mut writer = BatchLogStoreWriter::new(&path, 100)?;

            let data1 = vec![
                3, 0x01, 0x02, 0x03,
                2, 0x04, 0x05,
            ];
            writer.write_batch(1, 100, &data1, 2)?;

            let data2 = vec![
                4, 0x10, 0x20, 0x30, 0x40,
            ];
            writer.write_batch(101, 200, &data2, 1)?;

            writer.finish()?;
        }

        // 验证文件存在
        assert!(dir.path().join("test.batchlog.dat").exists());
        assert!(dir.path().join("test.batchlog.idx").exists());

        // 读取验证
        let store = BatchLogStore::open(&path)?;
        assert_eq!(store.header().batch_count, 2);
        assert_eq!(store.header().batch_size, 100);
        assert_eq!(store.header().first_block_number, 1);
        assert_eq!(store.header().last_block_number, 200);

        Ok(())
    }
}
