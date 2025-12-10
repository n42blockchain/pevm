//! 状态日志存储模块
//! 
//! 使用内存映射（mmap）存储状态日志，支持大数据集（400GB+）
//! 
//! 文件格式：
//! ```text
//! +----------------+  0
//! | Header (32B)   |  magic(8) + version(4) + block_count(8) + index_offset(8) + reserved(4)
//! +----------------+  32
//! | Data Section   |  连续存储所有块的日志数据（未压缩）
//! | ...            |
//! +----------------+  index_offset
//! | Index Section  |  每条 20 bytes: block_number(8) + offset(8) + length(4)
//! | ...            |
//! +----------------+
//! ```

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use tracing::{info, error};

use super::ReadLogEntry;

/// 内存映射状态日志数据库
/// 
/// 特点：
/// - 只将索引加载到内存（约 480MB for 24M blocks）
/// - 数据通过 mmap 零拷贝访问
/// - 操作系统智能缓存热点数据
pub struct MmapStateLogDatabase {
    /// 内存映射（只读）
    mmap: Option<memmap2::Mmap>,
    /// 索引：block_number -> (offset, length)
    index: HashMap<u64, (u64, u32)>,
    /// 数据文件路径
    file_path: PathBuf,
    /// 当前数据段末尾位置（用于追加）
    data_end: u64,
    /// 是否有未保存的修改
    dirty: bool,
    /// 待写入的新数据（批量写入优化）
    pending_writes: Vec<(u64, Vec<u8>)>,
}

impl std::fmt::Debug for MmapStateLogDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MmapStateLogDatabase")
            .field("file_path", &self.file_path)
            .field("index_count", &self.index.len())
            .field("data_end", &self.data_end)
            .field("dirty", &self.dirty)
            .field("pending_writes", &self.pending_writes.len())
            .finish_non_exhaustive()
    }
}

impl MmapStateLogDatabase {
    /// 文件魔数
    const MAGIC: &'static [u8; 8] = b"STLOGMM1";
    /// 版本号
    const VERSION: u32 = 1;
    /// 头部大小
    const HEADER_SIZE: u64 = 32;
    /// 索引条目大小
    const INDEX_ENTRY_SIZE: u64 = 20;
    
    /// 打开或创建内存映射状态日志数据库
    pub fn open(log_dir: &Path) -> eyre::Result<Self> {
        let file_path = log_dir.join("state_logs_mmap.bin");
        std::fs::create_dir_all(log_dir)?;
        
        if file_path.exists() {
            Self::open_existing(&file_path)
        } else {
            Self::create_new(&file_path)
        }
    }
    
    /// 打开已存在的文件
    fn open_existing(file_path: &Path) -> eyre::Result<Self> {
        let file = File::open(file_path)?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        
        // 验证头部
        if mmap.len() < Self::HEADER_SIZE as usize {
            return Err(eyre::eyre!("File too small"));
        }
        
        let magic = &mmap[0..8];
        if magic != Self::MAGIC {
            return Err(eyre::eyre!("Invalid file format (magic mismatch)"));
        }
        
        let version = u32::from_le_bytes(mmap[8..12].try_into().unwrap());
        if version != Self::VERSION {
            return Err(eyre::eyre!("Unsupported version: {}", version));
        }
        
        let block_count = u64::from_le_bytes(mmap[12..20].try_into().unwrap());
        let index_offset = u64::from_le_bytes(mmap[20..28].try_into().unwrap());
        
        // 加载索引
        let mut index = HashMap::with_capacity(block_count as usize);
        let index_start = index_offset as usize;
        
        for i in 0..block_count {
            let entry_start = index_start + (i as usize * Self::INDEX_ENTRY_SIZE as usize);
            let entry_end = entry_start + Self::INDEX_ENTRY_SIZE as usize;
            
            if entry_end > mmap.len() {
                return Err(eyre::eyre!("Index entry out of bounds"));
            }
            
            let block_number = u64::from_le_bytes(mmap[entry_start..entry_start+8].try_into().unwrap());
            let offset = u64::from_le_bytes(mmap[entry_start+8..entry_start+16].try_into().unwrap());
            let length = u32::from_le_bytes(mmap[entry_start+16..entry_start+20].try_into().unwrap());
            
            index.insert(block_number, (offset, length));
        }
        
        info!("MmapStateLogDatabase opened: {} blocks from {:?}", index.len(), file_path);
        
        Ok(Self {
            mmap: Some(mmap),
            index,
            file_path: file_path.to_path_buf(),
            data_end: index_offset,
            dirty: false,
            pending_writes: Vec::new(),
        })
    }
    
    /// 创建新文件
    fn create_new(file_path: &Path) -> eyre::Result<Self> {
        let mut file = File::create(file_path)?;
        
        // 写入头部
        file.write_all(Self::MAGIC)?;
        file.write_all(&Self::VERSION.to_le_bytes())?;
        file.write_all(&0u64.to_le_bytes())?; // block_count = 0
        file.write_all(&Self::HEADER_SIZE.to_le_bytes())?; // index_offset
        file.write_all(&[0u8; 4])?; // reserved
        file.flush()?;
        
        info!("MmapStateLogDatabase created: {:?}", file_path);
        
        Ok(Self {
            mmap: None,
            index: HashMap::new(),
            file_path: file_path.to_path_buf(),
            data_end: Self::HEADER_SIZE,
            dirty: false,
            pending_writes: Vec::new(),
        })
    }
    
    /// 读取块的日志数据（零拷贝）
    pub fn read_block_log(&self, block_number: u64) -> Option<&[u8]> {
        let (offset, length) = self.index.get(&block_number)?;
        
        if let Some(ref mmap) = self.mmap {
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
    #[allow(dead_code)]
    pub(crate) fn write_block_log(&mut self, block_number: u64, entries: &[ReadLogEntry]) -> eyre::Result<()> {
        let data = Self::serialize_entries(entries)?;
        self.pending_writes.push((block_number, data));
        self.dirty = true;
        
        // 内存保护：当 pending_writes 累积超过 100000 个块时自动 flush
        if self.pending_writes.len() >= 100000 {
            info!("Auto-flushing {} pending blocks to prevent memory growth", self.pending_writes.len());
            self.flush()?;
        }
        
        Ok(())
    }
    
    /// 批量写入（先缓存，超过阈值自动 flush）
    pub(crate) fn write_block_logs_batch(&mut self, batch: &[(u64, Vec<ReadLogEntry>)]) -> eyre::Result<()> {
        for (block_number, entries) in batch {
            let data = Self::serialize_entries(entries)?;
            self.pending_writes.push((*block_number, data));
        }
        self.dirty = true;
        
        // 内存保护：当 pending_writes 累积超过 100000 个块时自动 flush
        // 避免长时间运行时内存持续增长导致 OOM
        if self.pending_writes.len() >= 100000 {
            info!("Auto-flushing {} pending blocks to prevent memory growth", self.pending_writes.len());
            self.flush()?;
        }
        
        Ok(())
    }
    
    /// 刷新待写入的数据到文件
    pub fn flush(&mut self) -> eyre::Result<()> {
        if self.pending_writes.is_empty() {
            return Ok(());
        }
        
        // 计算新数据大小
        let new_data_size: u64 = self.pending_writes.iter()
            .map(|(_, data)| data.len() as u64)
            .sum();
        
        // 计算新索引大小
        let new_index_entries = self.pending_writes.len();
        let total_index_entries = self.index.len() + new_index_entries;
        let index_size = total_index_entries as u64 * Self::INDEX_ENTRY_SIZE;
        
        // 计算新文件大小
        let new_data_end = self.data_end + new_data_size;
        let new_file_size = new_data_end + index_size;
        
        // 打开文件进行写入
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.file_path)?;
        
        // 扩展文件
        file.set_len(new_file_size)?;
        
        // 创建可写的 mmap
        let mut mmap_mut = unsafe { memmap2::MmapMut::map_mut(&file)? };
        
        // 写入新数据
        let mut current_offset = self.data_end;
        
        for (block_number, data) in self.pending_writes.drain(..) {
            let data_len = data.len() as u32;
            
            // 写入数据
            let start = current_offset as usize;
            let end = start + data.len();
            mmap_mut[start..end].copy_from_slice(&data);
            
            // 记录索引
            self.index.insert(block_number, (current_offset, data_len));
            
            current_offset += data.len() as u64;
        }
        
        // 写入所有索引
        let index_start = new_data_end as usize;
        let mut index_offset = index_start;
        
        for (&block_number, &(offset, length)) in self.index.iter() {
            mmap_mut[index_offset..index_offset+8].copy_from_slice(&block_number.to_le_bytes());
            mmap_mut[index_offset+8..index_offset+16].copy_from_slice(&offset.to_le_bytes());
            mmap_mut[index_offset+16..index_offset+20].copy_from_slice(&length.to_le_bytes());
            index_offset += Self::INDEX_ENTRY_SIZE as usize;
        }
        
        // 更新头部
        mmap_mut[12..20].copy_from_slice(&(self.index.len() as u64).to_le_bytes());
        mmap_mut[20..28].copy_from_slice(&new_data_end.to_le_bytes());
        
        // 刷新到磁盘
        mmap_mut.flush()?;
        drop(mmap_mut);
        
        // 更新状态
        self.data_end = new_data_end;
        self.dirty = false;
        
        // 重新打开只读 mmap
        let file = File::open(&self.file_path)?;
        self.mmap = Some(unsafe { memmap2::Mmap::map(&file)? });
        
        info!("MmapStateLogDatabase flushed: {} total blocks, data_end={}", self.index.len(), self.data_end);
        
        Ok(())
    }
    
    /// 检查块是否存在
    #[allow(dead_code)]
    pub fn block_exists(&self, block_number: u64) -> bool {
        self.index.contains_key(&block_number)
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
        if self.dirty {
            if let Err(e) = self.flush() {
                error!("Failed to flush MmapStateLogDatabase on drop: {}", e);
            }
        }
    }
}

