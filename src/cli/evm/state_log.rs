//! 状态日志存储模块
//! 
//! 使用内存映射（mmap）存储状态日志，支持大数据集（400GB+）
//! 
//! 文件格式（v2 - 分离索引）：
//! ```text
//! state_logs_data.bin - 数据文件（追加写入）
//! +----------------+  0
//! | Header (32B)   |  magic(8) + version(4) + reserved(20)
//! +----------------+  32
//! | Data Section   |  连续存储所有块的日志数据（未压缩）
//! | ...            |
//! +----------------+
//! 
//! state_logs_index.bin - 索引文件（追加写入）
//! +----------------+  0
//! | Header (32B)   |  magic(8) + version(4) + block_count(8) + reserved(12)
//! +----------------+  32
//! | Index Entries  |  每条 20 bytes: block_number(8) + offset(8) + length(4)
//! | ...            |
//! +----------------+
//! ```

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use tracing::{info, warn, error};

use super::ReadLogEntry;

/// 内存映射状态日志数据库（v2 - 分离索引，Windows 兼容）
/// 
/// 特点：
/// - 数据文件追加写入，无需重写
/// - 索引文件独立存储，追加写入
/// - 只读时使用 mmap，写入时使用普通文件 I/O
/// - 避免 Windows mmap 扩展文件的问题
/// - 写入模式下不重新映射，避免 Windows STATUS_IN_PAGE_ERROR
pub struct MmapStateLogDatabase {
    /// 数据文件的内存映射（只读，写入模式下可能为 None）
    mmap_data: Option<memmap2::Mmap>,
    /// 索引：block_number -> (offset, length)
    index: HashMap<u64, (u64, u32)>,
    /// 数据文件路径
    data_file_path: PathBuf,
    /// 索引文件路径
    index_file_path: PathBuf,
    /// 当前数据段末尾位置（用于追加）
    data_end: u64,
    /// 是否有未保存的修改
    dirty: bool,
    /// 待写入的新数据（批量写入优化）
    pending_writes: Vec<(u64, Vec<u8>)>,
    /// 是否为只读模式
    read_only: bool,
    /// 是否为写入模式（写入模式下 flush 后不重新映射）
    write_mode: bool,
}

impl std::fmt::Debug for MmapStateLogDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MmapStateLogDatabase")
            .field("data_file_path", &self.data_file_path)
            .field("index_count", &self.index.len())
            .field("data_end", &self.data_end)
            .field("dirty", &self.dirty)
            .field("pending_writes", &self.pending_writes.len())
            .field("read_only", &self.read_only)
            .finish_non_exhaustive()
    }
}

impl MmapStateLogDatabase {
    /// 文件魔数（数据文件）
    const MAGIC_DATA: &'static [u8; 8] = b"STLOGDT2";
    /// 文件魔数（索引文件）
    const MAGIC_INDEX: &'static [u8; 8] = b"STLOGIX2";
    /// 版本号
    const VERSION: u32 = 2;
    /// 头部大小
    const HEADER_SIZE: u64 = 32;
    /// 索引条目大小
    const INDEX_ENTRY_SIZE: u64 = 20;
    
    /// 打开或创建内存映射状态日志数据库（读取模式，会创建 mmap）
    pub fn open(log_dir: &Path) -> eyre::Result<Self> {
        Self::open_internal(log_dir, false)
    }
    
    /// 打开或创建内存映射状态日志数据库（写入模式，不创建 mmap，避免 Windows 大文件 mmap 问题）
    pub fn open_for_write(log_dir: &Path) -> eyre::Result<Self> {
        Self::open_internal(log_dir, true)
    }
    
    /// 内部打开方法
    fn open_internal(log_dir: &Path, write_mode: bool) -> eyre::Result<Self> {
        let data_file_path = log_dir.join("state_logs_data.bin");
        let index_file_path = log_dir.join("state_logs_index.bin");
        std::fs::create_dir_all(log_dir)?;
        
        // 检查是否存在 v1 格式的文件
        let old_file_path = log_dir.join("state_logs_mmap.bin");
        if old_file_path.exists() && !data_file_path.exists() {
            info!("Found v1 format file, migrating to v2...");
            Self::migrate_v1_to_v2(&old_file_path, &data_file_path, &index_file_path)?;
        }
        
        if data_file_path.exists() && index_file_path.exists() {
            Self::open_existing(&data_file_path, &index_file_path, write_mode)
        } else {
            Self::create_new(&data_file_path, &index_file_path, write_mode)
        }
    }
    
    /// 迁移 v1 格式到 v2 格式
    fn migrate_v1_to_v2(old_path: &Path, data_path: &Path, index_path: &Path) -> eyre::Result<()> {
        info!("Migrating state_logs_mmap.bin to v2 format...");
        
        let old_file = File::open(old_path)?;
        let old_mmap = unsafe { memmap2::Mmap::map(&old_file)? };
        
        if old_mmap.len() < 32 {
            return Err(eyre::eyre!("Old file too small"));
        }
        
        // 读取旧格式头部
        let magic = &old_mmap[0..8];
        if magic != b"STLOGMM1" {
            return Err(eyre::eyre!("Invalid old file format"));
        }
        
        let block_count = u64::from_le_bytes(old_mmap[12..20].try_into().unwrap());
        let index_offset = u64::from_le_bytes(old_mmap[20..28].try_into().unwrap());
        
        info!("Old file: {} blocks, index_offset={}", block_count, index_offset);
        
        // 创建新的数据文件
        let mut data_file = BufWriter::new(File::create(data_path)?);
        
        // 写入数据文件头部
        let mut header = [0u8; 32];
        header[0..8].copy_from_slice(Self::MAGIC_DATA);
        header[8..12].copy_from_slice(&Self::VERSION.to_le_bytes());
        data_file.write_all(&header)?;
        
        // 复制数据部分（从旧文件的第 32 字节到 index_offset）
        let data_section = &old_mmap[32..index_offset as usize];
        data_file.write_all(data_section)?;
        data_file.flush()?;
        drop(data_file);
        
        // 创建新的索引文件
        let mut index_file = BufWriter::new(File::create(index_path)?);
        
        // 写入索引文件头部
        let mut index_header = [0u8; 32];
        index_header[0..8].copy_from_slice(Self::MAGIC_INDEX);
        index_header[8..12].copy_from_slice(&Self::VERSION.to_le_bytes());
        index_header[12..20].copy_from_slice(&block_count.to_le_bytes());
        index_file.write_all(&index_header)?;
        
        // 复制索引部分
        let index_section = &old_mmap[index_offset as usize..];
        index_file.write_all(index_section)?;
        index_file.flush()?;
        drop(index_file);
        
        info!("Migration complete: {} bytes data, {} bytes index", 
            data_section.len(), index_section.len());
        
        // 重命名旧文件
        let backup_path = old_path.with_extension("bin.v1.bak");
        std::fs::rename(old_path, &backup_path)?;
        info!("Old file renamed to {:?}", backup_path);
        
        Ok(())
    }
    
    /// 打开已存在的文件
    fn open_existing(data_path: &Path, index_path: &Path, write_mode: bool) -> eyre::Result<Self> {
        // 获取数据文件大小（不需要 mmap）
        let data_file_metadata = std::fs::metadata(data_path)?;
        let data_end = data_file_metadata.len();
        
        // 验证数据文件头部（读取前 32 字节即可）
        let mut data_file = File::open(data_path)?;
        let mut header_buf = [0u8; 32];
        data_file.read_exact(&mut header_buf)?;
        
        let magic = &header_buf[0..8];
        if magic != Self::MAGIC_DATA {
            return Err(eyre::eyre!("Invalid data file format (magic mismatch)"));
        }
        
        let version = u32::from_le_bytes(header_buf[8..12].try_into().unwrap());
        if version != Self::VERSION {
            return Err(eyre::eyre!("Unsupported version: {}", version));
        }
        drop(data_file); // 关闭文件
        
        // 读取索引文件
        let mut index_file = File::open(index_path)?;
        let mut index_header = [0u8; 32];
        index_file.read_exact(&mut index_header)?;
        
        let index_magic = &index_header[0..8];
        if index_magic != Self::MAGIC_INDEX {
            return Err(eyre::eyre!("Invalid index file format (magic mismatch)"));
        }
        
        let index_version = u32::from_le_bytes(index_header[8..12].try_into().unwrap());
        if index_version != Self::VERSION {
            return Err(eyre::eyre!("Unsupported index version: {}", index_version));
        }
        
        let header_block_count = u64::from_le_bytes(index_header[12..20].try_into().unwrap());
        
        // 计算索引文件实际大小，读取所有有效条目（不仅仅依赖头部的 block_count）
        // 这样即使程序崩溃时索引条目已追加但头部未更新，也能恢复所有条目
        let index_file_size = index_file.metadata()?.len();
        let actual_entry_count = (index_file_size.saturating_sub(Self::HEADER_SIZE)) / Self::INDEX_ENTRY_SIZE;
        
        // 使用实际条目数（可能比头部记录的多）
        let entry_count = std::cmp::max(header_block_count, actual_entry_count);
        
        // 读取所有索引条目
        let mut index = HashMap::with_capacity(entry_count as usize);
        let mut entry_buf = [0u8; Self::INDEX_ENTRY_SIZE as usize];
        let mut skipped_invalid = 0u64;
        
        // 读取到文件末尾，忽略不完整或无效的条目
        loop {
            match index_file.read_exact(&mut entry_buf) {
                Ok(_) => {
                    let block_number = u64::from_le_bytes(entry_buf[0..8].try_into().unwrap());
                    let offset = u64::from_le_bytes(entry_buf[8..16].try_into().unwrap());
                    let length = u32::from_le_bytes(entry_buf[16..20].try_into().unwrap());
                    
                    // 严格验证条目有效性：
                    // 1. block_number 必须在合理范围内（< 1亿，足够覆盖以太坊历史）
                    // 2. offset 必须 >= HEADER_SIZE 且 <= data_end
                    // 3. length 必须 > 0 且 < 10MB（单个块的日志数据不应该超过这个大小）
                    let is_valid = block_number < 100_000_000 
                        && offset >= Self::HEADER_SIZE 
                        && offset <= data_end
                        && length > 0 
                        && length < 10_000_000;
                    
                    if is_valid {
                        index.insert(block_number, (offset, length));
                    } else if block_number != 0 || offset != 0 || length != 0 {
                        // 跳过明显无效的条目（但不是全零条目）
                        skipped_invalid += 1;
                    }
                }
                Err(_) => break, // 到达文件末尾或读取不完整
            }
        }
        
        if skipped_invalid > 0 {
            warn!("Skipped {} invalid index entries (corrupted data)", skipped_invalid);
        }
        
        // 如果实际读取的条目数与头部不一致，记录日志
        if index.len() as u64 != header_block_count {
            info!("Index recovery: found {} entries (header said {}), will update header on next flush", 
                index.len(), header_block_count);
        }
        
        // 只有在读取模式下才创建 mmap（避免 Windows 大文件 mmap 问题）
        let mmap_data = if write_mode {
            info!("MmapStateLogDatabase opened in WRITE mode (v2): {} blocks, data_end={} (no mmap)", 
                index.len(), data_end);
            None
        } else {
            let data_file = File::open(data_path)?;
            let mmap = unsafe { memmap2::Mmap::map(&data_file)? };
            info!("MmapStateLogDatabase opened in READ mode (v2): {} blocks, data_end={}", 
                index.len(), data_end);
            Some(mmap)
        };
        
        Ok(Self {
            mmap_data,
            index,
            data_file_path: data_path.to_path_buf(),
            index_file_path: index_path.to_path_buf(),
            data_end,
            dirty: false,
            pending_writes: Vec::new(),
            read_only: false,
            write_mode,
        })
    }
    
    /// 创建新文件
    fn create_new(data_path: &Path, index_path: &Path, write_mode: bool) -> eyre::Result<Self> {
        // 创建数据文件
        let mut data_file = File::create(data_path)?;
        
        // 写入数据文件头部
        let mut header = [0u8; 32];
        header[0..8].copy_from_slice(Self::MAGIC_DATA);
        header[8..12].copy_from_slice(&Self::VERSION.to_le_bytes());
        data_file.write_all(&header)?;
        data_file.flush()?;
        drop(data_file);
        
        // 创建索引文件
        let mut index_file = File::create(index_path)?;
        
        // 写入索引文件头部
        let mut index_header = [0u8; 32];
        index_header[0..8].copy_from_slice(Self::MAGIC_INDEX);
        index_header[8..12].copy_from_slice(&Self::VERSION.to_le_bytes());
        // block_count 初始为 0
        index_file.write_all(&index_header)?;
        index_file.flush()?;
        drop(index_file);
        
        // 只有在读取模式下才创建 mmap
        let mmap_data = if write_mode {
            info!("MmapStateLogDatabase created in WRITE mode (v2, no mmap)");
            None
        } else {
            let data_file = File::open(data_path)?;
            let mmap = unsafe { memmap2::Mmap::map(&data_file)? };
            info!("MmapStateLogDatabase created in READ mode (v2)");
            Some(mmap)
        };
        
        Ok(Self {
            mmap_data,
            index: HashMap::new(),
            data_file_path: data_path.to_path_buf(),
            index_file_path: index_path.to_path_buf(),
            data_end: Self::HEADER_SIZE,
            dirty: false,
            pending_writes: Vec::new(),
            read_only: false,
            write_mode,
        })
    }
    
    /// 读取块的日志数据（零拷贝）
    pub fn read_block_log(&self, block_number: u64) -> Option<&[u8]> {
        let (offset, length) = self.index.get(&block_number)?;
        
        if let Some(ref mmap) = self.mmap_data {
            let start = *offset as usize;
            let end = start + *length as usize;
            if end <= mmap.len() {
                return Some(&mmap[start..end]);
            }
        }
        
        None
    }
    
    /// 批量读取多个块的日志数据（零拷贝）
    pub fn read_block_logs_batch(&self, block_numbers: &[u64]) -> Vec<(u64, &[u8])> {
        block_numbers.iter()
            .filter_map(|&bn| self.read_block_log(bn).map(|data| (bn, data)))
            .collect()
    }
    
    /// 写入块的日志数据（先缓存，超过阈值自动 flush）
    /// 自动跳过已存在且有效的块（避免重复写入）
    /// 对于损坏的块，会覆盖重新生成
    #[allow(dead_code)]
    pub(crate) fn write_block_log(&mut self, block_number: u64, entries: &[ReadLogEntry]) -> eyre::Result<()> {
        // 检查块是否已存在且有效
        if let Some((offset, length)) = self.index.get(&block_number) {
            let is_valid = *offset >= Self::HEADER_SIZE 
                && *length > 0 
                && *length < 10_000_000
                && (*offset + *length as u64) <= self.data_end;
            
            if is_valid {
                return Ok(()); // 已存在且有效，跳过
            } else {
                // 数据损坏，移除旧索引
                warn!("Block {} data corrupted, will regenerate", block_number);
                self.index.remove(&block_number);
            }
        }
        
        // 检查是否在 pending_writes 中
        if self.pending_writes.iter().any(|(bn, _)| *bn == block_number) {
            return Ok(()); // 在 pending_writes 中，跳过
        }
        
        let data = Self::serialize_entries(entries)?;
        self.pending_writes.push((block_number, data));
        self.dirty = true;
        
        // 内存保护：当 pending_writes 累积超过 10000 个块时自动 flush
        // 降低阈值可减少内存压力，避免长时间运行后性能下降
        if self.pending_writes.len() >= 10000 {
            info!("Auto-flushing {} pending blocks to prevent memory growth", self.pending_writes.len());
            self.flush()?;
        }
        
        Ok(())
    }
    
    /// 批量写入（先缓存，超过阈值自动 flush）
    /// 自动跳过已存在且有效的块（避免重复写入）
    /// 对于损坏的块，会覆盖重新生成
    pub(crate) fn write_block_logs_batch(&mut self, batch: &[(u64, Vec<ReadLogEntry>)]) -> eyre::Result<()> {
        let mut skipped = 0usize;
        let mut regenerated = 0usize;
        
        for (block_number, entries) in batch {
            // 检查块是否已存在于 index 中（已 flush 的块）
            if let Some((offset, length)) = self.index.get(block_number) {
                // 验证数据是否有效
                let is_valid = *offset >= Self::HEADER_SIZE 
                    && *length > 0 
                    && *length < 10_000_000
                    && (*offset + *length as u64) <= self.data_end;
                
                if is_valid {
                    skipped += 1;
                    continue;
                } else {
                    // 数据损坏，移除旧索引，允许重新生成
                    warn!("Block {} data corrupted (offset={}, length={}, data_end={}), will regenerate", 
                        block_number, offset, length, self.data_end);
                    self.index.remove(block_number);
                    regenerated += 1;
                }
            }
            
            // 检查块是否已存在于 pending_writes 中（未 flush 的块）
            if self.pending_writes.iter().any(|(bn, _)| bn == block_number) {
                skipped += 1;
                continue;
            }
            
            let data = Self::serialize_entries(entries)?;
            self.pending_writes.push((*block_number, data));
        }
        
        if skipped > 0 && skipped >= batch.len() / 2 {
            // 只在跳过大量块时输出日志（避免日志泛滥）
            warn!("Skipped {} existing blocks out of {} in batch", skipped, batch.len());
        }
        
        if regenerated > 0 {
            info!("Regenerated {} corrupted blocks", regenerated);
        }
        
        if !self.pending_writes.is_empty() {
            self.dirty = true;
        }
        
        // 内存保护：当 pending_writes 累积超过 10000 个块时自动 flush
        // 降低阈值可减少内存压力，避免长时间运行后性能下降
        if self.pending_writes.len() >= 10000 {
            info!("Auto-flushing {} pending blocks to prevent memory growth", self.pending_writes.len());
            self.flush()?;
        }
        
        Ok(())
    }
    
    /// 刷新待写入的数据到文件（追加写入，不重映射）
    pub fn flush(&mut self) -> eyre::Result<()> {
        if self.pending_writes.is_empty() {
            return Ok(());
        }
        
        if self.read_only {
            return Err(eyre::eyre!("Database is in read-only mode"));
        }
        
        let pending_count = self.pending_writes.len();
        
        // 计算新数据大小
        let _new_data_size: u64 = self.pending_writes.iter()
            .map(|(_, data)| data.len() as u64)
            .sum();
        
        // 打开数据文件进行追加写入（使用 BufWriter 提高性能）
        let data_file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&self.data_file_path)?;
        let mut data_writer = BufWriter::with_capacity(1024 * 1024, data_file); // 1MB buffer
        
        // 准备索引条目
        let mut new_index_entries: Vec<(u64, u64, u32)> = Vec::with_capacity(pending_count);
        let mut current_offset = self.data_end;
        
        // 写入新数据
        for (block_number, data) in self.pending_writes.drain(..) {
            let data_len = data.len() as u32;
            
            // 写入数据
            data_writer.write_all(&data)?;
            
            // 记录索引
            new_index_entries.push((block_number, current_offset, data_len));
            self.index.insert(block_number, (current_offset, data_len));
            
            current_offset += data.len() as u64;
        }
        
        data_writer.flush()?;
        drop(data_writer);
        
        // 打开索引文件进行追加写入
        let mut index_file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&self.index_file_path)?;
        
        // 追加新索引条目
        let mut index_buf = Vec::with_capacity(pending_count * Self::INDEX_ENTRY_SIZE as usize);
        for (block_number, offset, length) in new_index_entries {
            index_buf.extend_from_slice(&block_number.to_le_bytes());
            index_buf.extend_from_slice(&offset.to_le_bytes());
            index_buf.extend_from_slice(&length.to_le_bytes());
        }
        index_file.write_all(&index_buf)?;
        
        // 更新索引文件头部的 block_count
        index_file.seek(SeekFrom::Start(12))?;
        index_file.write_all(&(self.index.len() as u64).to_le_bytes())?;
        index_file.flush()?;
        drop(index_file);
        
        // 更新状态
        self.data_end = current_offset;
        self.dirty = false;
        
        // 写入模式下不重新映射（避免 Windows STATUS_IN_PAGE_ERROR）
        // 只有在需要读取数据时才重新映射
        if !self.write_mode {
            // 重新打开 mmap（只读）
            // 注意：这里我们先 drop 旧的 mmap，再创建新的，避免文件锁冲突
            self.mmap_data = None;
            let data_file = File::open(&self.data_file_path)?;
            self.mmap_data = Some(unsafe { memmap2::Mmap::map(&data_file)? });
        }
        
        info!("MmapStateLogDatabase flushed (v2): {} new blocks, {} total blocks, data_end={}", 
            pending_count, self.index.len(), self.data_end);
        
        Ok(())
    }
    
    /// 刷新并重新映射（用于读取时确保最新数据可见）
    pub fn refresh_mmap(&mut self) -> eyre::Result<()> {
        // 先 drop 旧的 mmap
        self.mmap_data = None;
        
        // 重新打开文件并映射
        let data_file = File::open(&self.data_file_path)?;
        self.mmap_data = Some(unsafe { memmap2::Mmap::map(&data_file)? });
        self.data_end = self.mmap_data.as_ref().map(|m| m.len() as u64).unwrap_or(Self::HEADER_SIZE);
        
        Ok(())
    }
    
    /// 检查块是否存在且数据有效（用于断点续传的精确检查）
    /// 如果块存在但数据损坏，返回 false（允许重新生成）
    pub fn block_exists(&self, block_number: u64) -> bool {
        if let Some((offset, length)) = self.index.get(&block_number) {
            // 验证数据范围是否有效
            let is_valid = *offset >= Self::HEADER_SIZE 
                && *length > 0 
                && *length < 10_000_000  // 单块数据不应超过 10MB
                && (*offset + *length as u64) <= self.data_end;
            
            if !is_valid {
                // 数据损坏，返回 false 允许重新生成
                return false;
            }
            true
        } else {
            false
        }
    }
    
    /// 检查块数据是否损坏
    /// 返回 true 表示损坏（需要重新生成）
    pub fn is_block_corrupted(&self, block_number: u64) -> bool {
        if let Some((offset, length)) = self.index.get(&block_number) {
            // 检查基本有效性
            if *offset < Self::HEADER_SIZE 
                || *length == 0 
                || *length >= 10_000_000
                || (*offset + *length as u64) > self.data_end {
                return true; // 索引信息损坏
            }
            
            // 如果有 mmap，可以进一步验证数据内容
            if let Some(ref mmap) = self.mmap_data {
                let start = *offset as usize;
                let end = start + *length as usize;
                
                if end > mmap.len() {
                    return true; // 数据超出文件范围
                }
                
                // 验证数据格式：前 8 字节是条目数量
                if *length >= 8 {
                    let count = u64::from_le_bytes(mmap[start..start+8].try_into().unwrap_or([0u8; 8]));
                    // 条目数量应该合理（不超过 100 万）
                    if count > 1_000_000 {
                        return true; // 数据格式损坏
                    }
                }
            }
            
            false // 数据有效
        } else {
            false // 块不存在，不算损坏
        }
    }
    
    /// 标记块为损坏（从索引中移除，允许重新生成）
    pub fn mark_block_corrupted(&mut self, block_number: u64) {
        if self.index.remove(&block_number).is_some() {
            warn!("Marked block {} as corrupted, will be regenerated", block_number);
            self.dirty = true;
        }
    }
    
    /// 获取块数量
    pub fn block_count(&self) -> usize {
        self.index.len()
    }
    
    /// 获取已存在的块号范围
    pub fn get_block_range(&self) -> Option<(u64, u64)> {
        if self.index.is_empty() {
            return None;
        }
        let min = *self.index.keys().min().unwrap();
        let max = *self.index.keys().max().unwrap();
        Some((min, max))
    }
    
    /// 获取所有已存在的块号（用于断点续传）
    /// 警告：对于大数据集（>100万块），此方法会占用大量内存
    /// 考虑使用 get_block_range() + block_exists() 代替
    pub fn get_existing_blocks(&self) -> std::collections::HashSet<u64> {
        self.index.keys().copied().collect()
    }
    
    /// 获取已存在的最大块号（用于快速断点续传检查）
    pub fn get_max_existing_block(&self) -> Option<u64> {
        self.index.keys().max().copied()
    }
    
    /// 检查块号是否在已存在的范围内
    /// 这是一个快速的 O(1) 检查，假设块号是连续的
    pub fn is_block_in_range(&self, block_number: u64) -> bool {
        if let Some((min, max)) = self.get_block_range() {
            block_number >= min && block_number <= max
        } else {
            false
        }
    }
    
    /// 序列化 entries 为未压缩的二进制格式
    /// 格式：count(8 bytes, little-endian) + entries
    /// 每个 entry: length(1 byte) + data
    pub(crate) fn serialize_entries(entries: &[ReadLogEntry]) -> eyre::Result<Vec<u8>> {
        let estimated_size = 8 + entries.len() * 50;
        let mut data = Vec::with_capacity(estimated_size);
        
        // 写入条目数量
        data.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        
        // 写入每个条目
        for entry in entries {
            match entry {
                ReadLogEntry::Account { data: entry_data, .. } |
                ReadLogEntry::Storage { data: entry_data, .. } => {
                    if entry_data.len() > 255 {
                        return Err(eyre::eyre!("Entry data too long: {} bytes", entry_data.len()));
                    }
                    data.push(entry_data.len() as u8);
                    data.extend_from_slice(entry_data);
                }
            }
        }
        
        Ok(data)
    }
}

impl Drop for MmapStateLogDatabase {
    fn drop(&mut self) {
        if self.dirty && !self.pending_writes.is_empty() {
            if let Err(e) = self.flush() {
                error!("Failed to flush MmapStateLogDatabase on drop: {}", e);
            }
        }
    }
}

/// 日志修复结果
#[derive(Debug)]
pub struct RepairResult {
    pub total_entries: u64,
    pub valid_entries: u64,
    pub invalid_entries: u64,
    pub duplicate_entries: u64,
    pub truncated_entries: u64,
    pub min_block: Option<u64>,
    pub max_block: Option<u64>,
    pub missing_blocks: Vec<u64>,
    pub repaired: bool,
    pub error_message: Option<String>,
}

/// 分析损坏的索引条目，尝试找出规律
fn analyze_corrupted_entries(
    index_file_path: &Path,
    data_end: u64,
) -> eyre::Result<()> {
    use std::io::{BufReader, Read, Seek, SeekFrom};
    
    let mut file = BufReader::new(File::open(index_file_path)?);
    file.seek(SeekFrom::Start(32))?; // 跳过头部
    
    let mut entry_buf = [0u8; 20];
    let mut entry_num = 0u64;
    let mut first_invalid: Option<u64> = None;
    let mut last_valid_offset: u64 = 32; // HEADER_SIZE
    let mut consecutive_invalid = 0u64;
    
    info!("=== Analyzing index corruption pattern ===");
    
    loop {
        match file.read_exact(&mut entry_buf) {
            Ok(_) => {
                entry_num += 1;
                let block_number = u64::from_le_bytes(entry_buf[0..8].try_into().unwrap());
                let offset = u64::from_le_bytes(entry_buf[8..16].try_into().unwrap());
                let length = u32::from_le_bytes(entry_buf[16..20].try_into().unwrap());
                
                let is_valid = block_number < 100_000_000 
                    && offset >= 32 
                    && offset <= data_end
                    && length > 0 
                    && length < 10_000_000
                    && (offset + length as u64) <= data_end;
                
                if is_valid {
                    last_valid_offset = offset + length as u64;
                    consecutive_invalid = 0;
                } else {
                    if first_invalid.is_none() {
                        first_invalid = Some(entry_num);
                        info!("First invalid entry at #{}: block={}, offset={}, length={}", 
                            entry_num, block_number, offset, length);
                        info!("Last valid data ended at offset: {}", last_valid_offset);
                    }
                    consecutive_invalid += 1;
                    
                    // 检查是否是连续的块号模式（可能是错误写入）
                    if consecutive_invalid <= 5 {
                        info!("Invalid entry #{}: block={}, offset={}, length={}", 
                            entry_num, block_number, offset, length);
                    }
                }
            }
            Err(_) => break,
        }
    }
    
    if let Some(first) = first_invalid {
        info!("=== Corruption Analysis ===");
        info!("First invalid entry: #{}", first);
        info!("Total entries: {}", entry_num);
        info!("Valid entries before corruption: {}", first - 1);
        info!("Last valid data offset: {} bytes ({:.2} GB)", 
            last_valid_offset, last_valid_offset as f64 / 1024.0 / 1024.0 / 1024.0);
        info!("Data file size: {} bytes ({:.2} GB)", 
            data_end, data_end as f64 / 1024.0 / 1024.0 / 1024.0);
        
        if last_valid_offset < data_end {
            let orphaned_data = data_end - last_valid_offset;
            info!("Orphaned data (no index): {} bytes ({:.2} GB)", 
                orphaned_data, orphaned_data as f64 / 1024.0 / 1024.0 / 1024.0);
            warn!("There may be additional blocks in the data file without valid index entries!");
            warn!("These blocks need to be regenerated.");
        }
    } else {
        info!("No corruption found in index file.");
    }
    
    Ok(())
}

impl MmapStateLogDatabase {
    /// 修复和整理日志文件
    /// 
    /// 功能：
    /// 1. 验证所有索引条目的有效性
    /// 2. 按块号排序并紧密排列数据
    /// 3. 移除重复条目
    /// 4. 检查块号连续性
    /// 5. 发现错误或缺失时存盘并终止
    pub fn repair_log(log_dir: &Path, expected_start: u64, expected_end: u64) -> eyre::Result<RepairResult> {
        let data_file_path = log_dir.join("state_logs_data.bin");
        let index_file_path = log_dir.join("state_logs_index.bin");
        
        if !data_file_path.exists() || !index_file_path.exists() {
            return Err(eyre::eyre!("Log files not found in {:?}", log_dir));
        }
        
        info!("=== Starting log repair for {:?} ===", log_dir);
        info!("Expected block range: {} - {}", expected_start, expected_end);
        
        // 获取数据文件大小
        let data_file_metadata = std::fs::metadata(&data_file_path)?;
        let data_end = data_file_metadata.len();
        
        info!("Data file size: {} bytes ({:.2} GB)", data_end, data_end as f64 / 1024.0 / 1024.0 / 1024.0);
        
        // 先分析损坏模式
        let _ = analyze_corrupted_entries(&index_file_path, data_end);
        
        // 读取所有索引条目
        let mut index_file = File::open(&index_file_path)?;
        let mut header = [0u8; 32];
        index_file.read_exact(&mut header)?;
        
        // 验证头部
        let magic = &header[0..8];
        if magic != Self::MAGIC_INDEX {
            return Err(eyre::eyre!("Invalid index file format"));
        }
        
        let header_block_count = u64::from_le_bytes(header[12..20].try_into().unwrap());
        info!("Index header says {} blocks", header_block_count);
        
        // 读取所有条目
        let mut all_entries: Vec<(u64, u64, u32)> = Vec::new(); // (block_number, offset, length)
        let mut entry_buf = [0u8; Self::INDEX_ENTRY_SIZE as usize];
        let mut total_entries = 0u64;
        let mut invalid_entries = 0u64;
        
        loop {
            match index_file.read_exact(&mut entry_buf) {
                Ok(_) => {
                    total_entries += 1;
                    let block_number = u64::from_le_bytes(entry_buf[0..8].try_into().unwrap());
                    let offset = u64::from_le_bytes(entry_buf[8..16].try_into().unwrap());
                    let length = u32::from_le_bytes(entry_buf[16..20].try_into().unwrap());
                    
                    // 验证条目
                    let is_valid = block_number < 100_000_000 
                        && offset >= Self::HEADER_SIZE 
                        && offset <= data_end
                        && length > 0 
                        && length < 10_000_000
                        && (offset + length as u64) <= data_end;
                    
                    if is_valid {
                        all_entries.push((block_number, offset, length));
                    } else {
                        invalid_entries += 1;
                        if invalid_entries <= 10 {
                            warn!("Invalid entry #{}: block={}, offset={}, length={}", 
                                total_entries, block_number, offset, length);
                        }
                    }
                }
                Err(_) => break,
            }
            
            // 进度报告
            if total_entries % 1_000_000 == 0 {
                info!("Read {} entries...", total_entries);
            }
        }
        
        info!("Total entries read: {}, valid: {}, invalid: {}", 
            total_entries, all_entries.len(), invalid_entries);
        
        if all_entries.is_empty() {
            return Ok(RepairResult {
                total_entries,
                valid_entries: 0,
                invalid_entries,
                duplicate_entries: 0,
                truncated_entries: 0,
                min_block: None,
                max_block: None,
                missing_blocks: Vec::new(),
                repaired: false,
                error_message: Some("No valid entries found".to_string()),
            });
        }
        
        // 按块号排序
        info!("Sorting {} entries by block number...", all_entries.len());
        all_entries.sort_by_key(|(bn, _, _)| *bn);
        
        // 检查重复并构建去重后的索引
        info!("Checking for duplicates and filtering by range...");
        let mut unique_entries: Vec<(u64, u64, u32)> = Vec::with_capacity(all_entries.len());
        let mut duplicate_entries = 0u64;
        let mut truncated_entries = 0u64;
        let mut last_block: Option<u64> = None;
        
        for (block_number, offset, length) in all_entries.iter() {
            // 截断：忽略大于 expected_end 的块
            if *block_number > expected_end {
                truncated_entries += 1;
                continue;
            }
            
            if last_block == Some(*block_number) {
                duplicate_entries += 1;
                // 保留后一个（可能是更新的数据）
                unique_entries.pop();
            }
            unique_entries.push((*block_number, *offset, *length));
            last_block = Some(*block_number);
        }
        
        if truncated_entries > 0 {
            info!("Truncated {} entries with block number > {}", truncated_entries, expected_end);
        }
        
        if duplicate_entries > 0 {
            warn!("Found {} duplicate entries", duplicate_entries);
        }
        
        let min_block = unique_entries.first().map(|(bn, _, _)| *bn);
        let max_block = unique_entries.last().map(|(bn, _, _)| *bn);
        
        info!("Unique entries: {}, block range: {:?} - {:?}", 
            unique_entries.len(), min_block, max_block);
        
        // 检查块号连续性（在期望范围内）
        info!("Checking block continuity in range {} - {}...", expected_start, expected_end);
        let mut missing_blocks: Vec<u64> = Vec::new();
        let block_set: std::collections::HashSet<u64> = unique_entries.iter()
            .map(|(bn, _, _)| *bn)
            .collect();
        
        // 只检查期望范围内的缺失块
        let check_start = std::cmp::max(expected_start, min_block.unwrap_or(0));
        let check_end = std::cmp::min(expected_end, max_block.unwrap_or(u64::MAX));
        
        for block_num in check_start..=check_end {
            if !block_set.contains(&block_num) {
                missing_blocks.push(block_num);
                if missing_blocks.len() <= 100 {
                    warn!("Missing block: {}", block_num);
                }
                if missing_blocks.len() >= 1000 {
                    warn!("Found {} missing blocks, stopping check...", missing_blocks.len());
                    break;
                }
            }
        }
        
        let has_missing_blocks = !missing_blocks.is_empty();
        if has_missing_blocks {
            let missing_count = missing_blocks.len();
            warn!("Found {} missing blocks in range {} - {}", 
                missing_count, check_start, check_end);
            warn!("Will continue repair with existing data. Missing blocks can be regenerated with --log-block on");
        }
        
        // 开始整理：按块号顺序紧密排列数据
        info!("=== Starting data compaction ===");
        info!("This will create new compacted files...");
        
        // 打开原始数据文件用于读取
        let data_file = File::open(&data_file_path)?;
        let data_mmap = unsafe { memmap2::Mmap::map(&data_file)? };
        
        // 创建新的数据文件
        let new_data_path = log_dir.join("state_logs_data.bin.new");
        let new_index_path = log_dir.join("state_logs_index.bin.new");
        
        let mut new_data_file = BufWriter::with_capacity(4 * 1024 * 1024, File::create(&new_data_path)?);
        let mut new_index_file = BufWriter::with_capacity(1024 * 1024, File::create(&new_index_path)?);
        
        // 写入数据文件头部
        let mut data_header = [0u8; 32];
        data_header[0..8].copy_from_slice(Self::MAGIC_DATA);
        data_header[8..12].copy_from_slice(&Self::VERSION.to_le_bytes());
        new_data_file.write_all(&data_header)?;
        
        // 写入索引文件头部
        let mut index_header = [0u8; 32];
        index_header[0..8].copy_from_slice(Self::MAGIC_INDEX);
        index_header[8..12].copy_from_slice(&Self::VERSION.to_le_bytes());
        index_header[12..20].copy_from_slice(&(unique_entries.len() as u64).to_le_bytes());
        new_index_file.write_all(&index_header)?;
        
        // 按块号顺序复制数据并生成新索引
        let mut new_offset = Self::HEADER_SIZE;
        let mut processed = 0u64;
        
        for (block_number, old_offset, length) in unique_entries.iter() {
            // 从原文件读取数据
            let start = *old_offset as usize;
            let end = start + *length as usize;
            
            if end > data_mmap.len() {
                error!("Data corruption: block {} points beyond file end", block_number);
                return Ok(RepairResult {
                    total_entries,
                    valid_entries: processed,
                    invalid_entries,
                    duplicate_entries,
                    truncated_entries,
                    min_block,
                    max_block,
                    missing_blocks: vec![*block_number],
                    repaired: false,
                    error_message: Some(format!("Block {} data extends beyond file end", block_number)),
                });
            }
            
            let data = &data_mmap[start..end];
            
            // 写入新数据文件
            new_data_file.write_all(data)?;
            
            // 写入新索引条目
            let mut entry_buf = [0u8; 20];
            entry_buf[0..8].copy_from_slice(&block_number.to_le_bytes());
            entry_buf[8..16].copy_from_slice(&new_offset.to_le_bytes());
            entry_buf[16..20].copy_from_slice(&length.to_le_bytes());
            new_index_file.write_all(&entry_buf)?;
            
            new_offset += *length as u64;
            processed += 1;
            
            // 进度报告
            if processed % 1_000_000 == 0 {
                info!("Compacted {} / {} blocks ({:.1}%)...", 
                    processed, unique_entries.len(), 
                    processed as f64 / unique_entries.len() as f64 * 100.0);
            }
        }
        
        new_data_file.flush()?;
        new_index_file.flush()?;
        drop(new_data_file);
        drop(new_index_file);
        drop(data_mmap);
        drop(data_file);
        
        // 备份原文件并替换
        info!("Backing up original files...");
        let backup_data_path = data_file_path.with_extension("bin.backup");
        let backup_index_path = index_file_path.with_extension("bin.backup");
        
        std::fs::rename(&data_file_path, &backup_data_path)?;
        std::fs::rename(&index_file_path, &backup_index_path)?;
        
        info!("Replacing with compacted files...");
        std::fs::rename(&new_data_path, &data_file_path)?;
        std::fs::rename(&new_index_path, &index_file_path)?;
        
        let new_data_size = std::fs::metadata(&data_file_path)?.len();
        let saved_bytes = data_end.saturating_sub(new_data_size);
        
        info!("=== Repair completed successfully! ===");
        info!("  - Total entries processed: {}", total_entries);
        info!("  - Valid entries: {}", unique_entries.len());
        info!("  - Invalid entries removed: {}", invalid_entries);
        info!("  - Duplicate entries removed: {}", duplicate_entries);
        info!("  - Truncated entries (> {}): {}", expected_end, truncated_entries);
        info!("  - Block range: {} - {}", min_block.unwrap_or(0), max_block.unwrap_or(0));
        info!("  - Original size: {:.2} GB", data_end as f64 / 1024.0 / 1024.0 / 1024.0);
        info!("  - Compacted size: {:.2} GB", new_data_size as f64 / 1024.0 / 1024.0 / 1024.0);
        info!("  - Space saved: {:.2} GB ({:.1}%)", 
            saved_bytes as f64 / 1024.0 / 1024.0 / 1024.0,
            saved_bytes as f64 / data_end as f64 * 100.0);
        info!("  - Backup files: {:?}, {:?}", backup_data_path, backup_index_path);
        
        let missing_count = missing_blocks.len();
        if has_missing_blocks {
            warn!("  - Missing blocks: {} (run with --log-block on to regenerate)", missing_count);
        }
        
        Ok(RepairResult {
            total_entries,
            valid_entries: unique_entries.len() as u64,
            invalid_entries,
            duplicate_entries,
            truncated_entries,
            min_block,
            max_block,
            missing_blocks,
            repaired: true,
            error_message: if has_missing_blocks { 
                Some(format!("{} missing blocks need to be regenerated", missing_count))
            } else { 
                None 
            },
        })
    }
}
