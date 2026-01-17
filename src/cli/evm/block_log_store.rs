// SPDX-License-Identifier: MIT OR Apache-2.0

//! 块级日志存储模块
//!
//! 提供高性能的多块日志数据管理，支持：
//! - 按块号读取日志数据
//! - 4096字节对齐的文件格式（利用扇区缓存）
//! - 按需加载、及时释放内存
//! - 多线程只读无锁访问

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write as IoWrite, BufWriter};
use std::path::Path;
use std::sync::Arc;
use std::cell::Cell;

use alloy_primitives::{Address, B256, U256};
use dashmap::DashMap;
use reth_codecs::Compact;
use reth_primitives::Account;
use reth_revm::database::StateProviderDatabase;
use tracing::error;

use crate::revm::Database as RevmDatabase;
use crate::revm::state::{AccountInfo, Bytecode};

use super::logged_db::BytecodeCache;

// ============================================================================
// 文件格式常量
// ============================================================================

/// 文件魔数
const MAGIC: &[u8; 8] = b"BLKLOG02";

/// 版本号
const VERSION: u32 = 2;

/// 扇区大小（4096字节对齐）
const SECTOR_SIZE: u64 = 4096;

/// 头部大小（一个扇区）
const HEADER_SIZE: u64 = SECTOR_SIZE;

/// 文件头标志位
mod flags {
    /// 数据使用 zstd 压缩
    pub const COMPRESSED_ZSTD: u32 = 1 << 0;
}

// ============================================================================
// 文件格式结构
// ============================================================================

/// 文件头（固定 4096 字节）
///
/// 布局：
/// - [0..8]: magic "BLKLOG02"
/// - [8..12]: version (u32 LE)
/// - [12..20]: block_count (u64 LE)
/// - [20..28]: index_offset (u64 LE) - 索引表在文件中的偏移
/// - [28..32]: flags (u32 LE)
/// - [32..40]: first_block_number (u64 LE)
/// - [40..48]: last_block_number (u64 LE)
/// - [48..4096]: reserved
#[derive(Debug, Clone)]
pub struct FileHeader {
    pub block_count: u64,
    pub index_offset: u64,
    pub flags: u32,
    pub first_block_number: u64,
    pub last_block_number: u64,
}

impl FileHeader {
    /// 从字节解析
    pub fn from_bytes(data: &[u8]) -> eyre::Result<Self> {
        if data.len() < HEADER_SIZE as usize {
            return Err(eyre::eyre!("Header too short"));
        }

        // 验证魔数
        if &data[0..8] != MAGIC {
            return Err(eyre::eyre!("Invalid magic number"));
        }

        // 验证版本
        let version = u32::from_le_bytes(data[8..12].try_into().unwrap());
        if version != VERSION {
            return Err(eyre::eyre!("Unsupported version: {}", version));
        }

        Ok(Self {
            block_count: u64::from_le_bytes(data[12..20].try_into().unwrap()),
            index_offset: u64::from_le_bytes(data[20..28].try_into().unwrap()),
            flags: u32::from_le_bytes(data[28..32].try_into().unwrap()),
            first_block_number: u64::from_le_bytes(data[32..40].try_into().unwrap()),
            last_block_number: u64::from_le_bytes(data[40..48].try_into().unwrap()),
        })
    }

    /// 序列化为字节
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8; HEADER_SIZE as usize];
        buf[0..8].copy_from_slice(MAGIC);
        buf[8..12].copy_from_slice(&VERSION.to_le_bytes());
        buf[12..20].copy_from_slice(&self.block_count.to_le_bytes());
        buf[20..28].copy_from_slice(&self.index_offset.to_le_bytes());
        buf[28..32].copy_from_slice(&self.flags.to_le_bytes());
        buf[32..40].copy_from_slice(&self.first_block_number.to_le_bytes());
        buf[40..48].copy_from_slice(&self.last_block_number.to_le_bytes());
        buf
    }

    /// 是否启用压缩
    pub fn is_compressed(&self) -> bool {
        self.flags & flags::COMPRESSED_ZSTD != 0
    }
}

/// 块索引条目（固定 48 字节）
///
/// 布局：
/// - [0..8]: block_number (u64 LE)
/// - [8..16]: data_offset (u64 LE) - 数据在文件中的偏移
/// - [16..24]: data_size (u64 LE) - 解压后大小
/// - [24..32]: compressed_size (u64 LE) - 压缩后大小（0表示未压缩）
/// - [32..40]: entry_count (u64 LE) - 日志条目数量
/// - [40..44]: checksum (u32 LE) - CRC32 校验和
/// - [44..48]: reserved
#[derive(Debug, Clone, Copy)]
pub struct BlockIndexEntry {
    pub block_number: u64,
    pub data_offset: u64,
    pub data_size: u64,
    pub compressed_size: u64,
    pub entry_count: u64,
    pub checksum: u32,
}

impl BlockIndexEntry {
    /// 条目大小
    pub const SIZE: usize = 48;

    /// 从字节解析
    pub fn from_bytes(data: &[u8]) -> eyre::Result<Self> {
        if data.len() < Self::SIZE {
            return Err(eyre::eyre!("Index entry too short"));
        }

        Ok(Self {
            block_number: u64::from_le_bytes(data[0..8].try_into().unwrap()),
            data_offset: u64::from_le_bytes(data[8..16].try_into().unwrap()),
            data_size: u64::from_le_bytes(data[16..24].try_into().unwrap()),
            compressed_size: u64::from_le_bytes(data[24..32].try_into().unwrap()),
            entry_count: u64::from_le_bytes(data[32..40].try_into().unwrap()),
            checksum: u32::from_le_bytes(data[40..44].try_into().unwrap()),
        })
    }

    /// 序列化为字节
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..8].copy_from_slice(&self.block_number.to_le_bytes());
        buf[8..16].copy_from_slice(&self.data_offset.to_le_bytes());
        buf[16..24].copy_from_slice(&self.data_size.to_le_bytes());
        buf[24..32].copy_from_slice(&self.compressed_size.to_le_bytes());
        buf[32..40].copy_from_slice(&self.entry_count.to_le_bytes());
        buf[40..44].copy_from_slice(&self.checksum.to_le_bytes());
        buf
    }

    /// 实际存储大小（考虑压缩）
    pub fn stored_size(&self) -> u64 {
        if self.compressed_size > 0 {
            self.compressed_size
        } else {
            self.data_size
        }
    }
}

// ============================================================================
// BlockLogStore - 中央日志存储
// ============================================================================

/// 块数据（已加载到内存）
pub struct BlockData {
    /// 日志数据（已解压）
    pub data: Vec<u8>,
    /// 条目数量
    pub entry_count: u64,
}

impl std::fmt::Debug for BlockData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockData")
            .field("data_len", &self.data.len())
            .field("entry_count", &self.entry_count)
            .finish()
    }
}

/// 块日志存储
///
/// 管理多个块的日志数据，支持：
/// - 按块号随机访问
/// - 按需加载（懒加载）
/// - 内存释放
/// - 多线程安全（只读访问无锁）
pub struct BlockLogStore {
    /// 文件路径
    path: std::path::PathBuf,
    /// 文件头信息
    header: FileHeader,
    /// 块索引：block_number -> BlockIndexEntry
    index: HashMap<u64, BlockIndexEntry>,
    /// 已加载的块数据：block_number -> Arc<BlockData>
    /// 使用 DashMap 实现无锁并发读取
    loaded_blocks: DashMap<u64, Arc<BlockData>>,
}

impl std::fmt::Debug for BlockLogStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockLogStore")
            .field("path", &self.path)
            .field("block_count", &self.header.block_count)
            .field("loaded_blocks", &self.loaded_blocks.len())
            .finish()
    }
}

impl BlockLogStore {
    /// 从文件打开存储
    pub fn open<P: AsRef<Path>>(path: P) -> eyre::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;

        // 读取文件头
        let mut header_buf = vec![0u8; HEADER_SIZE as usize];
        file.read_exact(&mut header_buf)?;
        let header = FileHeader::from_bytes(&header_buf)?;

        // 读取索引表
        file.seek(SeekFrom::Start(header.index_offset))?;
        let index_size = header.block_count as usize * BlockIndexEntry::SIZE;
        let mut index_buf = vec![0u8; index_size];
        file.read_exact(&mut index_buf)?;

        let mut index = HashMap::with_capacity(header.block_count as usize);
        for i in 0..header.block_count as usize {
            let entry_data = &index_buf[i * BlockIndexEntry::SIZE..(i + 1) * BlockIndexEntry::SIZE];
            let entry = BlockIndexEntry::from_bytes(entry_data)?;
            index.insert(entry.block_number, entry);
        }

        Ok(Self {
            path,
            header,
            index,
            loaded_blocks: DashMap::new(),
        })
    }

    /// 获取文件头
    pub fn header(&self) -> &FileHeader {
        &self.header
    }

    /// 获取块索引
    pub fn get_index(&self, block_number: u64) -> Option<&BlockIndexEntry> {
        self.index.get(&block_number)
    }

    /// 检查块是否存在
    pub fn contains_block(&self, block_number: u64) -> bool {
        self.index.contains_key(&block_number)
    }

    /// 获取所有块号（按顺序）
    pub fn block_numbers(&self) -> Vec<u64> {
        let mut numbers: Vec<u64> = self.index.keys().copied().collect();
        numbers.sort();
        numbers
    }

    /// 加载块数据到内存
    ///
    /// 如果已加载，直接返回缓存的数据
    /// 支持并发调用，同一个块只会加载一次
    pub fn load_block(&self, block_number: u64) -> eyre::Result<Arc<BlockData>> {
        // 先检查是否已加载
        if let Some(data) = self.loaded_blocks.get(&block_number) {
            return Ok(data.clone());
        }

        // 获取索引
        let entry = self.index.get(&block_number)
            .ok_or_else(|| eyre::eyre!("Block {} not found in index", block_number))?;

        // 从文件读取
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(entry.data_offset))?;

        let stored_size = entry.stored_size() as usize;
        let mut buf = vec![0u8; stored_size];
        file.read_exact(&mut buf)?;

        // 解压（如果需要）
        let data = if entry.compressed_size > 0 {
            zstd::decode_all(&buf[..])?
        } else {
            buf
        };

        let block_data = Arc::new(BlockData {
            data,
            entry_count: entry.entry_count,
        });

        // 存入缓存（如果其他线程已经插入，使用已有的）
        self.loaded_blocks.entry(block_number)
            .or_insert(block_data.clone());

        Ok(block_data)
    }

    /// 预加载指定范围的块
    pub fn preload_range(&self, start: u64, end: u64) -> eyre::Result<()> {
        for block_number in start..=end {
            if self.contains_block(block_number) {
                self.load_block(block_number)?;
            }
        }
        Ok(())
    }

    /// 释放块数据
    pub fn release_block(&self, block_number: u64) {
        self.loaded_blocks.remove(&block_number);
    }

    /// 释放指定范围内的块
    pub fn release_range(&self, start: u64, end: u64) {
        for block_number in start..=end {
            self.release_block(block_number);
        }
    }

    /// 释放所有已加载的块
    pub fn release_all(&self) {
        self.loaded_blocks.clear();
    }

    /// 获取已加载的块数量
    pub fn loaded_count(&self) -> usize {
        self.loaded_blocks.len()
    }

    /// 获取总内存使用量（已加载的块）
    pub fn memory_usage(&self) -> usize {
        self.loaded_blocks.iter()
            .map(|entry| entry.value().data.len())
            .sum()
    }
}

// ============================================================================
// BlockLogStoreWriter - 写入器
// ============================================================================

/// 块日志存储写入器
///
/// 用于创建新的块日志存储文件
pub struct BlockLogStoreWriter {
    writer: BufWriter<File>,
    /// 是否启用压缩
    compress: bool,
    /// 压缩级别
    compression_level: i32,
    /// 当前写入偏移
    current_offset: u64,
    /// 索引条目
    entries: Vec<BlockIndexEntry>,
    /// 第一个块号
    first_block_number: Option<u64>,
    /// 最后一个块号
    last_block_number: Option<u64>,
}

impl BlockLogStoreWriter {
    /// 创建新的写入器
    pub fn new<P: AsRef<Path>>(path: P, compress: bool) -> eyre::Result<Self> {
        let file = File::create(path)?;
        let mut writer = BufWriter::with_capacity(1024 * 1024, file); // 1MB buffer

        // 写入占位头（稍后更新）
        let placeholder = vec![0u8; HEADER_SIZE as usize];
        writer.write_all(&placeholder)?;

        Ok(Self {
            writer,
            compress,
            compression_level: 3, // 默认压缩级别
            current_offset: HEADER_SIZE,
            entries: Vec::new(),
            first_block_number: None,
            last_block_number: None,
        })
    }

    /// 设置压缩级别（1-22，默认3）
    pub fn set_compression_level(&mut self, level: i32) {
        self.compression_level = level.clamp(1, 22);
    }

    /// 写入一个块的日志数据
    ///
    /// 数据格式：原始日志数据（不含 count 头）
    ///
    /// # 注意
    /// 块号必须连续递增，避免批量执行时数据错位
    pub fn write_block(&mut self, block_number: u64, data: &[u8], entry_count: u64) -> eyre::Result<()> {
        // 验证块号连续性
        if let Some(last_bn) = self.last_block_number {
            if block_number != last_bn + 1 {
                return Err(eyre::eyre!(
                    "Block numbers must be consecutive: last={}, current={} (expected {})",
                    last_bn, block_number, last_bn + 1
                ));
            }
        }

        // 更新块号范围
        if self.first_block_number.is_none() {
            self.first_block_number = Some(block_number);
        }
        self.last_block_number = Some(block_number);

        // 对齐到扇区边界
        let padding = align_padding(self.current_offset, SECTOR_SIZE);
        if padding > 0 {
            let pad_bytes = vec![0u8; padding as usize];
            self.writer.write_all(&pad_bytes)?;
            self.current_offset += padding;
        }

        let data_offset = self.current_offset;
        let data_size = data.len() as u64;

        // 压缩数据（如果启用）
        let (write_data, compressed_size) = if self.compress {
            let compressed = zstd::encode_all(data, self.compression_level)?;
            let csize = compressed.len() as u64;
            (compressed, csize)
        } else {
            (data.to_vec(), 0)
        };

        // 计算校验和（简化版：使用数据长度作为简单校验）
        let checksum = write_data.len() as u32;

        // 写入数据
        self.writer.write_all(&write_data)?;
        self.current_offset += write_data.len() as u64;

        // 记录索引
        self.entries.push(BlockIndexEntry {
            block_number,
            data_offset,
            data_size,
            compressed_size,
            entry_count,
            checksum,
        });

        Ok(())
    }

    /// 完成写入并关闭文件
    pub fn finish(mut self) -> eyre::Result<()> {
        // 对齐到扇区边界
        let padding = align_padding(self.current_offset, SECTOR_SIZE);
        if padding > 0 {
            let pad_bytes = vec![0u8; padding as usize];
            self.writer.write_all(&pad_bytes)?;
            self.current_offset += padding;
        }

        let index_offset = self.current_offset;

        // 写入索引表
        for entry in &self.entries {
            self.writer.write_all(&entry.to_bytes())?;
        }

        // 构建头部
        let flags = if self.compress { flags::COMPRESSED_ZSTD } else { 0 };
        let header = FileHeader {
            block_count: self.entries.len() as u64,
            index_offset,
            flags,
            first_block_number: self.first_block_number.unwrap_or(0),
            last_block_number: self.last_block_number.unwrap_or(0),
        };

        // 写入头部（回到文件开头）
        self.writer.flush()?;
        let mut file = self.writer.into_inner()?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&header.to_bytes())?;
        file.flush()?;

        Ok(())
    }
}

/// 计算对齐填充
#[inline]
fn align_padding(offset: u64, alignment: u64) -> u64 {
    let remainder = offset % alignment;
    if remainder == 0 {
        0
    } else {
        alignment - remainder
    }
}

// ============================================================================
// BlockReader - 块数据读取器
// ============================================================================

/// 块数据读取器
///
/// 用于顺序读取一个块的日志条目
/// 每个线程/任务应该有自己的读取器实例
pub struct BlockReader {
    /// 块数据引用
    data: Arc<BlockData>,
    /// 当前读取位置
    pos: Cell<usize>,
    /// 已读取的条目数
    entries_read: Cell<u64>,
}

impl BlockReader {
    /// 创建新的读取器
    pub fn new(data: Arc<BlockData>) -> Self {
        Self {
            data,
            pos: Cell::new(0),
            entries_read: Cell::new(0),
        }
    }

    /// 读取下一个条目
    #[inline]
    pub fn next_entry(&self) -> Option<&[u8]> {
        let pos = self.pos.get();
        let entries_read = self.entries_read.get();

        // 检查是否已读完所有条目
        if entries_read >= self.data.entry_count {
            return None;
        }

        // 检查数据边界
        if pos >= self.data.data.len() {
            return None;
        }

        // 读取长度
        let len = self.data.data[pos] as usize;
        let start = pos + 1;
        let end = start + len;

        if end > self.data.data.len() {
            return None;
        }

        // 更新位置
        self.pos.set(end);
        self.entries_read.set(entries_read + 1);

        Some(&self.data.data[start..end])
    }

    /// 重置读取位置
    pub fn reset(&self) {
        self.pos.set(0);
        self.entries_read.set(0);
    }

    /// 获取已读取的条目数
    pub fn entries_read(&self) -> u64 {
        self.entries_read.get()
    }

    /// 获取总条目数
    pub fn entry_count(&self) -> u64 {
        self.data.entry_count
    }
}

// ============================================================================
// SingleBlockDatabase - 单块日志数据库
// ============================================================================

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

/// 单块日志数据库
///
/// 针对单个块的数据库实例，每个块执行时创建一个新实例。
///
/// 使用方式：
/// 1. 从 BlockLogStore 加载块数据：`store.load_block(block_number)`
/// 2. 创建数据库实例：`SingleBlockDatabase::new(block_data, ...)`
/// 3. 执行块
/// 4. 实例自动释放，或调用 `store.release_block()` 释放缓存
///
/// 多线程模型：
/// - BlockLogStore 是共享的，多线程无锁读取（使用 DashMap）
/// - 每个线程为每个块创建独立的 SingleBlockDatabase 实例
/// - 不需要状态切换，请求时直接带块号
pub struct SingleBlockDatabase {
    /// 块数据（从 BlockLogStore 获取）
    block_data: Arc<BlockData>,
    /// 读取器（顺序读取日志条目）
    reader: BlockReader,
    /// 块号
    block_number: u64,
    /// 状态提供者（用于 code_by_hash 和回退查询）
    state_provider: Arc<dyn reth_provider::StateProvider>,
    /// Bytecode 缓存（全局共享）
    bytecode_cache: Option<Arc<BytecodeCache>>,
}

impl std::fmt::Debug for SingleBlockDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SingleBlockDatabase")
            .field("block_number", &self.block_number)
            .field("entry_count", &self.block_data.entry_count)
            .finish()
    }
}

impl SingleBlockDatabase {
    /// 从 BlockLogStore 创建单块数据库
    ///
    /// # 参数
    /// - `store`: 共享的块日志存储
    /// - `block_number`: 块号
    /// - `state_provider`: 状态提供者（用于回退查询）
    /// - `bytecode_cache`: 可选的 Bytecode 缓存
    ///
    /// # 返回
    /// 成功返回数据库实例，失败返回错误（块不存在等）
    pub fn from_store(
        store: &BlockLogStore,
        block_number: u64,
        state_provider: Arc<dyn reth_provider::StateProvider>,
        bytecode_cache: Option<Arc<BytecodeCache>>,
    ) -> eyre::Result<Self> {
        let block_data = store.load_block(block_number)?;
        Ok(Self::new(block_data, block_number, state_provider, bytecode_cache))
    }

    /// 直接从块数据创建
    pub fn new(
        block_data: Arc<BlockData>,
        block_number: u64,
        state_provider: Arc<dyn reth_provider::StateProvider>,
        bytecode_cache: Option<Arc<BytecodeCache>>,
    ) -> Self {
        let reader = BlockReader::new(Arc::clone(&block_data));
        Self {
            block_data,
            reader,
            block_number,
            state_provider,
            bytecode_cache,
        }
    }

    /// 获取块号
    pub fn block_number(&self) -> u64 {
        self.block_number
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

// 保留旧的 BlockAwareDatabase 作为别名，保持向后兼容
#[deprecated(note = "Use SingleBlockDatabase instead")]
pub type BlockAwareDatabase = SingleBlockDatabase;

impl RevmDatabase for SingleBlockDatabase {
    type Error = <StateProviderDatabase<Box<dyn reth_provider::StateProvider>> as RevmDatabase>::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        // 从日志读取
        if let Some(compact_data) = self.reader.next_entry() {
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

            return Ok(Some(account_info));
        }

        // 日志数据不足，回退到数据库查询（这是错误情况，说明日志缺少数据）
        error!(
            "Log missing: block {} basic({:?}) - falling back to database query",
            self.block_number, address
        );
        let mut inner_db = StateProviderDatabase::new(
            self.state_provider.as_ref() as &dyn reth_provider::StateProvider
        );
        inner_db.basic(address)
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
        // 从日志读取
        if let Some(compact_data) = self.reader.next_entry() {
            let data_len = compact_data.len();
            let (value, _) = U256::from_compact(compact_data, data_len);
            return Ok(value);
        }

        // 日志数据不足，回退到数据库查询（这是错误情况，说明日志缺少数据）
        error!(
            "Log missing: block {} storage({:?}, {:?}) - falling back to database query",
            self.block_number, address, index
        );
        let mut inner_db = StateProviderDatabase::new(
            self.state_provider.as_ref() as &dyn reth_provider::StateProvider
        );
        inner_db.storage(address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        let mut inner_db = StateProviderDatabase::new(
            self.state_provider.as_ref() as &dyn reth_provider::StateProvider
        );
        inner_db.block_hash(number)
    }
}

// ============================================================================
// BatchBlockDatabase - 批量块日志数据库
// ============================================================================

/// 批量块日志数据库
///
/// 支持批量执行多个块，自动在块之间切换。
/// 使用方式：
/// 1. 从 BlockLogStore 预加载所有块：`BatchBlockDatabase::from_store(store, block_numbers, ...)`
/// 2. 创建执行器：`evm_config.batch_executor(&mut batch_db)`
/// 3. 批量执行：`executor.execute_batch(blocks.iter())`
///
/// 工作原理：
/// - 顺序读取日志条目
/// - 当一个块的 entry_count 条目读完后，自动切换到下一个块
/// - 支持原有的批量执行流程，无需修改执行逻辑
pub struct BatchBlockDatabase {
    /// 所有块的数据（按执行顺序）
    blocks: Vec<(u64, Arc<BlockData>)>,
    /// 当前块索引
    current_block_idx: std::cell::Cell<usize>,
    /// 当前块已读取的条目数
    current_entries_read: std::cell::Cell<u64>,
    /// 当前块数据的读取位置
    current_pos: std::cell::Cell<usize>,
    /// 状态提供者（用于 code_by_hash 和回退查询）
    state_provider: Arc<dyn reth_provider::StateProvider>,
    /// Bytecode 缓存（全局共享）
    bytecode_cache: Option<Arc<BytecodeCache>>,
}

impl std::fmt::Debug for BatchBlockDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchBlockDatabase")
            .field("block_count", &self.blocks.len())
            .field("current_block_idx", &self.current_block_idx.get())
            .finish()
    }
}

impl BatchBlockDatabase {
    /// 从 BlockLogStore 创建批量块数据库
    ///
    /// # 参数
    /// - `store`: 共享的块日志存储
    /// - `block_numbers`: 要执行的块号列表（必须连续且按顺序）
    /// - `state_provider`: 状态提供者（用于回退查询）
    /// - `bytecode_cache`: 可选的 Bytecode 缓存
    ///
    /// # 错误
    /// - 如果块号不连续，返回错误（避免数据错位）
    /// - 如果某个块在 store 中不存在，返回错误
    pub fn from_store(
        store: &BlockLogStore,
        block_numbers: &[u64],
        state_provider: Arc<dyn reth_provider::StateProvider>,
        bytecode_cache: Option<Arc<BytecodeCache>>,
    ) -> eyre::Result<Self> {
        if block_numbers.is_empty() {
            return Err(eyre::eyre!("Block numbers list is empty"));
        }

        // 验证块号连续性，避免数据错位
        for i in 1..block_numbers.len() {
            if block_numbers[i] != block_numbers[i - 1] + 1 {
                return Err(eyre::eyre!(
                    "Block numbers must be consecutive: {} -> {} (gap detected)",
                    block_numbers[i - 1], block_numbers[i]
                ));
            }
        }

        let mut blocks = Vec::with_capacity(block_numbers.len());

        for &bn in block_numbers {
            let block_data = store.load_block(bn)?;
            blocks.push((bn, block_data));
        }

        Ok(Self {
            blocks,
            current_block_idx: std::cell::Cell::new(0),
            current_entries_read: std::cell::Cell::new(0),
            current_pos: std::cell::Cell::new(0),
            state_provider,
            bytecode_cache,
        })
    }

    /// 获取当前块号
    fn current_block_number(&self) -> Option<u64> {
        self.blocks.get(self.current_block_idx.get()).map(|(bn, _)| *bn)
    }

    /// 读取下一个日志条目，自动在块之间切换
    #[inline]
    fn next_entry(&self) -> Option<&[u8]> {
        loop {
            let idx = self.current_block_idx.get();
            if idx >= self.blocks.len() {
                return None;
            }

            let (_, block_data) = &self.blocks[idx];
            let entries_read = self.current_entries_read.get();

            // 检查是否需要切换到下一个块
            if entries_read >= block_data.entry_count {
                // 切换到下一个块
                self.current_block_idx.set(idx + 1);
                self.current_entries_read.set(0);
                self.current_pos.set(0);
                continue;
            }

            let pos = self.current_pos.get();
            if pos >= block_data.data.len() {
                // 数据已读完但条目数未达到，切换到下一个块
                self.current_block_idx.set(idx + 1);
                self.current_entries_read.set(0);
                self.current_pos.set(0);
                continue;
            }

            // 读取长度
            let len = block_data.data[pos] as usize;
            let start = pos + 1;
            let end = start + len;

            if end > block_data.data.len() {
                // 数据不完整，切换到下一个块
                self.current_block_idx.set(idx + 1);
                self.current_entries_read.set(0);
                self.current_pos.set(0);
                continue;
            }

            // 更新位置
            self.current_pos.set(end);
            self.current_entries_read.set(entries_read + 1);

            return Some(&block_data.data[start..end]);
        }
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

impl RevmDatabase for BatchBlockDatabase {
    type Error = <StateProviderDatabase<Box<dyn reth_provider::StateProvider>> as RevmDatabase>::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        // 从日志读取
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

            return Ok(Some(account_info));
        }

        // 日志数据不足，回退到数据库查询
        let block_number = self.current_block_number().unwrap_or(0);
        error!(
            "Log missing: block {} basic({:?}) - falling back to database query",
            block_number, address
        );
        let mut inner_db = StateProviderDatabase::new(
            self.state_provider.as_ref() as &dyn reth_provider::StateProvider
        );
        inner_db.basic(address)
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
        // 从日志读取
        if let Some(compact_data) = self.next_entry() {
            let data_len = compact_data.len();
            let (value, _) = U256::from_compact(compact_data, data_len);
            return Ok(value);
        }

        // 日志数据不足，回退到数据库查询
        let block_number = self.current_block_number().unwrap_or(0);
        error!(
            "Log missing: block {} storage({:?}, {:?}) - falling back to database query",
            block_number, address, index
        );
        let mut inner_db = StateProviderDatabase::new(
            self.state_provider.as_ref() as &dyn reth_provider::StateProvider
        );
        inner_db.storage(address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        let mut inner_db = StateProviderDatabase::new(
            self.state_provider.as_ref() as &dyn reth_provider::StateProvider
        );
        inner_db.block_hash(number)
    }
}

// ============================================================================
// 工具函数
// ============================================================================

/// 从旧格式日志文件转换为新格式
///
/// 旧格式：每个 .bin 文件包含一个块的日志数据
/// 新格式：单个 .blklog 文件包含多个块的数据
pub fn convert_from_old_format<P1: AsRef<Path>, P2: AsRef<Path>>(
    log_dir: P1,
    output_path: P2,
    compress: bool,
) -> eyre::Result<u64> {
    use std::fs;

    let log_dir = log_dir.as_ref();

    // 收集所有 .bin 文件
    let mut bin_files: Vec<(u64, std::path::PathBuf)> = Vec::new();

    for entry in fs::read_dir(log_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map_or(false, |ext| ext == "bin") {
            if let Some(name) = path.file_stem() {
                if let Ok(block_number) = name.to_string_lossy().parse::<u64>() {
                    bin_files.push((block_number, path));
                }
            }
        }
    }

    // 按块号排序
    bin_files.sort_by_key(|(n, _)| *n);

    if bin_files.is_empty() {
        return Err(eyre::eyre!("No .bin files found in {}", log_dir.display()));
    }

    // 验证块号连续性
    for i in 1..bin_files.len() {
        if bin_files[i].0 != bin_files[i - 1].0 + 1 {
            return Err(eyre::eyre!(
                "Block numbers in .bin files are not consecutive: {} -> {} (missing blocks)",
                bin_files[i - 1].0, bin_files[i].0
            ));
        }
    }

    // 创建写入器
    let mut writer = BlockLogStoreWriter::new(output_path, compress)?;

    let mut converted = 0u64;
    for (block_number, path) in bin_files {
        // 读取旧格式文件
        let data = fs::read(&path)?;
        if data.len() < 8 {
            return Err(eyre::eyre!(
                "Invalid .bin file for block {}: file too small ({} bytes)",
                block_number, data.len()
            ));
        }

        // 解析条目数量
        let entry_count = u64::from_le_bytes(data[0..8].try_into().unwrap());

        // 写入新格式（跳过 count 头）
        writer.write_block(block_number, &data[8..], entry_count)?;
        converted += 1;
    }

    writer.finish()?;

    Ok(converted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_file_format() -> eyre::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.blklog");

        // 写入测试数据
        {
            let mut writer = BlockLogStoreWriter::new(&path, false)?;

            // 块 100：2 个条目
            let data1 = vec![
                3, 0x01, 0x02, 0x03,  // entry 1: len=3, data=[1,2,3]
                2, 0x04, 0x05,        // entry 2: len=2, data=[4,5]
            ];
            writer.write_block(100, &data1, 2)?;

            // 块 101：1 个条目
            let data2 = vec![
                4, 0x10, 0x20, 0x30, 0x40,  // entry 1: len=4
            ];
            writer.write_block(101, &data2, 1)?;

            writer.finish()?;
        }

        // 读取并验证
        let store = BlockLogStore::open(&path)?;
        assert_eq!(store.header().block_count, 2);
        assert_eq!(store.header().first_block_number, 100);
        assert_eq!(store.header().last_block_number, 101);

        // 验证块 100
        let block100 = store.load_block(100)?;
        assert_eq!(block100.entry_count, 2);

        let reader = BlockReader::new(block100);
        assert_eq!(reader.next_entry(), Some([0x01, 0x02, 0x03].as_slice()));
        assert_eq!(reader.next_entry(), Some([0x04, 0x05].as_slice()));
        assert_eq!(reader.next_entry(), None);

        // 验证块 101
        let block101 = store.load_block(101)?;
        assert_eq!(block101.entry_count, 1);

        let reader = BlockReader::new(block101);
        assert_eq!(reader.next_entry(), Some([0x10, 0x20, 0x30, 0x40].as_slice()));
        assert_eq!(reader.next_entry(), None);

        Ok(())
    }

    #[test]
    fn test_compression() -> eyre::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test_compressed.blklog");

        // 写入压缩数据
        {
            let mut writer = BlockLogStoreWriter::new(&path, true)?;

            // 创建可压缩的数据
            let data = vec![0u8; 1000];
            writer.write_block(1, &data, 1)?;

            writer.finish()?;
        }

        // 读取并验证
        let store = BlockLogStore::open(&path)?;
        assert!(store.header().is_compressed());

        let block = store.load_block(1)?;
        assert_eq!(block.data.len(), 1000);

        Ok(())
    }
}
