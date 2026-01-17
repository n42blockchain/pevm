// SPDX-License-Identifier: MIT OR Apache-2.0

//! 文件 I/O 工具模块
//!
//! 包含：
//! - BufferedLogWriter: 缓冲写入器
//! - GlobalLogFileHandle: 全局文件句柄
//! - 日志文件读写函数

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tracing::{debug, info};

use super::compression::{CompressionAlgorithm, decompress_data, choose_compression_algorithm};
use super::types::{IndexEntry, ReadLogEntry};

#[cfg(unix)]
use libc;

/// 缓冲写入器管理器（用于优化写入性能）
pub(crate) struct BufferedLogWriter {
    idx_writer: BufWriter<File>,
    bin_writer: BufWriter<File>,
    #[allow(dead_code)]
    bin_file_path: PathBuf,
    bin_position: u64,
    blocks_written: u64,
}

impl BufferedLogWriter {
    /// 创建新的缓冲写入器，缓冲区大小为 1MB
    pub fn new(log_dir: &Path) -> eyre::Result<Self> {
        std::fs::create_dir_all(log_dir)?;

        let idx_path = log_dir.join("blocks_log.idx");
        let bin_path = log_dir.join("blocks_log.bin");

        let idx_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(&idx_path)?;

        let bin_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(&bin_path)?;

        let bin_position = bin_file.metadata()?.len();

        const BUFFER_SIZE: usize = 1024 * 1024;
        Ok(Self {
            idx_writer: BufWriter::with_capacity(BUFFER_SIZE, idx_file),
            bin_writer: BufWriter::with_capacity(BUFFER_SIZE, bin_file),
            bin_file_path: bin_path,
            bin_position,
            blocks_written: 0,
        })
    }

    #[inline]
    pub fn get_bin_file_position(&self) -> eyre::Result<u64> {
        Ok(self.bin_position)
    }

    #[inline]
    pub fn write_bin_data(&mut self, data: &[u8]) -> eyre::Result<()> {
        self.bin_writer.write_all(data)
            .map_err(|e| eyre::eyre!("Failed to write bin data: {}", e))?;
        self.bin_position += data.len() as u64;
        Ok(())
    }

    pub fn write_index_entry(&mut self, block_number: u64, offset: u64, length: u64) -> eyre::Result<()> {
        self.idx_writer.write_all(&block_number.to_le_bytes())?;
        self.idx_writer.write_all(&offset.to_le_bytes())?;
        self.idx_writer.write_all(&length.to_le_bytes())?;
        self.blocks_written += 1;
        Ok(())
    }

    #[inline]
    pub fn write_compressed_block(&mut self, block_number: u64, compressed_data: &[u8]) -> eyre::Result<()> {
        let offset = self.bin_position;
        let length = compressed_data.len() as u64;

        self.bin_writer.write_all(compressed_data)
            .map_err(|e| eyre::eyre!("Failed to write bin data: {}", e))?;
        self.bin_position += length;

        self.idx_writer.write_all(&block_number.to_le_bytes())?;
        self.idx_writer.write_all(&offset.to_le_bytes())?;
        self.idx_writer.write_all(&length.to_le_bytes())?;
        self.blocks_written += 1;

        Ok(())
    }

    pub fn flush_all(&mut self) -> eyre::Result<()> {
        self.idx_writer.flush()?;
        self.bin_writer.flush()?;
        Ok(())
    }

    pub fn maybe_flush(&mut self) -> eyre::Result<()> {
        const FLUSH_INTERVAL: u64 = 10000;
        if self.blocks_written % FLUSH_INTERVAL == 0 {
            self.flush_all()?;
        }
        Ok(())
    }

    pub fn auto_flush_if_needed(&mut self) -> eyre::Result<()> {
        Ok(())
    }
}

impl Drop for BufferedLogWriter {
    fn drop(&mut self) {
        let _ = self.idx_writer.flush();
        let _ = self.bin_writer.flush();
    }
}

/// 打开日志文件（索引文件和数据文件）
pub(crate) fn open_log_files(log_dir: &Path) -> eyre::Result<(File, File)> {
    std::fs::create_dir_all(log_dir)?;

    let idx_path = log_dir.join("blocks_log.idx");
    let bin_path = log_dir.join("blocks_log.bin");

    let idx_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .append(true)
        .open(&idx_path)?;

    let bin_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .append(true)
        .open(&bin_path)?;

    Ok((idx_file, bin_file))
}

/// 读取索引文件
pub(crate) fn read_index_file(idx_file: &mut File) -> eyre::Result<Vec<IndexEntry>> {
    idx_file.seek(SeekFrom::Start(0))?;

    const BUFFER_SIZE: usize = 64 * 1024;
    let mut reader = BufReader::with_capacity(BUFFER_SIZE, idx_file);

    let mut entries = Vec::new();
    let mut buffer = [0u8; IndexEntry::SIZE];

    loop {
        match reader.read_exact(&mut buffer) {
            Ok(_) => {
                if let Some(entry) = IndexEntry::from_bytes(&buffer) {
                    entries.push(entry);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                break;
            }
            Err(e) => return Err(e.into()),
        }
    }

    Ok(entries)
}

/// 查找索引条目
pub(crate) fn find_index_entry(entries: &[IndexEntry], block_number: u64) -> Option<&IndexEntry> {
    entries.iter().find(|e| e.block_number == block_number)
}

/// 压缩块日志数据
pub(crate) fn compress_block_logs(
    _block_number: u64,
    read_logs: &[ReadLogEntry],
    compression_algorithm: &str,
) -> eyre::Result<Vec<u8>> {
    let estimated_size = read_logs.len() * 50;
    let mut uncompressed_data = Vec::with_capacity(estimated_size + 8);

    uncompressed_data.write_all(&(read_logs.len() as u64).to_le_bytes())?;

    for entry in read_logs {
        let data = match entry {
            ReadLogEntry::Account { data, .. } => data,
            ReadLogEntry::Storage { data, .. } => data,
        };

        if data.len() > 255 {
            return Err(eyre::eyre!("Data too long: {} bytes (max 255)", data.len()));
        }
        uncompressed_data.push(data.len() as u8);
        uncompressed_data.extend_from_slice(data);
    }

    let env_var = std::env::var("PEVM_IN_MEMORY_MODE");
    let enable_in_memory_mode = env_var
        .as_ref()
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    if enable_in_memory_mode {
        Ok(uncompressed_data)
    } else {
        let (algorithm, compressed_data, _stats) = choose_compression_algorithm(&uncompressed_data, compression_algorithm)?;
        let mut data = Vec::with_capacity(1 + compressed_data.len());
        data.push(algorithm.as_u8());
        data.extend_from_slice(&compressed_data);
        Ok(data)
    }
}

/// 全局文件句柄包装器
pub(crate) struct GlobalLogFileHandle {
    #[allow(dead_code)]
    in_memory_data: Option<Arc<Vec<u8>>>,

    #[cfg(unix)]
    fd: i32,
    #[cfg(not(unix))]
    file: Arc<Mutex<File>>,
    #[allow(dead_code)]
    file_path: PathBuf,
}

impl GlobalLogFileHandle {
    pub(crate) fn new(file_path: &Path, enable_in_memory: bool) -> eyre::Result<Self> {
        if enable_in_memory {
            info!("测试模式：将 bin 文件完全读入内存: {}", file_path.display());
            let file_size = std::fs::metadata(file_path)?.len();
            info!("文件大小: {} bytes ({:.2} GB)", file_size, file_size as f64 / (1024.0 * 1024.0 * 1024.0));

            let mut file = std::fs::File::open(file_path)?;
            let mut buffer = Vec::with_capacity(file_size as usize);
            file.read_to_end(&mut buffer)?;

            info!("文件已读入内存: {} bytes", buffer.len());

            Ok(Self {
                in_memory_data: Some(Arc::new(buffer)),
                #[cfg(unix)]
                fd: 0,
                #[cfg(not(unix))]
                file: Arc::new(Mutex::new(std::fs::File::open(file_path)?)),
                file_path: file_path.to_path_buf(),
            })
        } else {
            #[cfg(unix)]
            {
                use std::os::unix::io::AsRawFd;
                let file = std::fs::File::open(file_path)?;
                let fd = file.as_raw_fd();
                let fd_clone = unsafe { libc::dup(fd) };
                if fd_clone < 0 {
                    return Err(eyre::eyre!("Failed to dup file descriptor"));
                }
                Ok(Self {
                    in_memory_data: None,
                    fd: fd_clone,
                    file_path: file_path.to_path_buf(),
                })
            }
            #[cfg(not(unix))]
            {
                let file = std::fs::File::open(file_path)?;
                Ok(Self {
                    in_memory_data: None,
                    file: Arc::new(Mutex::new(file)),
                    file_path: file_path.to_path_buf(),
                })
            }
        }
    }

    pub(crate) fn read_range(&self, offset: u64, length: u64) -> eyre::Result<Vec<u8>> {
        if let Some(ref in_memory) = self.in_memory_data {
            let start = offset as usize;
            let end = start + length as usize;
            if end > in_memory.len() {
                return Err(eyre::eyre!("Read range out of bounds"));
            }
            Ok(in_memory[start..end].to_vec())
        } else {
            #[cfg(unix)]
            {
                use std::os::unix::io::RawFd;
                let mut buffer = vec![0u8; length as usize];
                let bytes_read = unsafe {
                    libc::pread(
                        self.fd as RawFd,
                        buffer.as_mut_ptr() as *mut libc::c_void,
                        length as libc::size_t,
                        offset as libc::off_t,
                    )
                };
                if bytes_read < 0 {
                    return Err(eyre::eyre!("pread failed"));
                }
                if bytes_read as u64 != length {
                    return Err(eyre::eyre!("pread incomplete"));
                }
                Ok(buffer)
            }
            #[cfg(not(unix))]
            {
                let mut file = self.file.lock().unwrap();
                file.seek(SeekFrom::Start(offset))?;
                let mut buffer = vec![0u8; length as usize];
                file.read_exact(&mut buffer)?;
                Ok(buffer)
            }
        }
    }

    pub(crate) fn read_ranges_batch(&self, ranges: &[(usize, u64, u64)]) -> eyre::Result<Vec<(usize, Vec<u8>)>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        if let Some(ref in_memory) = self.in_memory_data {
            let mut results = Vec::with_capacity(ranges.len());
            for &(original_idx, offset, length) in ranges {
                let start = offset as usize;
                let end = start + length as usize;
                if end > in_memory.len() {
                    return Err(eyre::eyre!("Read range out of bounds"));
                }
                results.push((original_idx, in_memory[start..end].to_vec()));
            }
            results.sort_by_key(|(idx, _)| *idx);
            Ok(results)
        } else {
            #[cfg(unix)]
            {
                use std::os::unix::io::RawFd;
                let mut results = Vec::with_capacity(ranges.len());
                for &(original_idx, offset, length) in ranges {
                    let mut buffer = vec![0u8; length as usize];
                    let bytes_read = unsafe {
                        libc::pread(
                            self.fd as RawFd,
                            buffer.as_mut_ptr() as *mut libc::c_void,
                            length as libc::size_t,
                            offset as libc::off_t,
                        )
                    };
                    if bytes_read < 0 {
                        return Err(eyre::eyre!("pread failed"));
                    }
                    results.push((original_idx, buffer));
                }
                results.sort_by_key(|(idx, _)| *idx);
                Ok(results)
            }
            #[cfg(not(unix))]
            {
                let mut results = Vec::with_capacity(ranges.len());
                let mut file = self.file.lock().unwrap();
                for &(original_idx, offset, length) in ranges {
                    file.seek(SeekFrom::Start(offset))?;
                    let mut buffer = vec![0u8; length as usize];
                    file.read_exact(&mut buffer)?;
                    results.push((original_idx, buffer));
                }
                Ok(results)
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn file_path(&self) -> &Path {
        &self.file_path
    }

    /// 检查是否有内存数据
    pub(crate) fn has_in_memory_data(&self) -> bool {
        self.in_memory_data.is_some()
    }

    /// 获取内存数据的引用
    pub(crate) fn get_in_memory_data(&self) -> Option<&Arc<Vec<u8>>> {
        self.in_memory_data.as_ref()
    }
}

/// 读取日志压缩数据
pub(crate) fn read_log_compressed_data(
    log_path_or_block: &str,
    log_dir: Option<&Path>,
    cached_index_entries: Option<&[IndexEntry]>,
    global_file_handle: Option<&GlobalLogFileHandle>,
    is_in_memory_mode: bool,
) -> eyre::Result<Vec<u8>> {
    if let Some(log_dir) = log_dir {
        let block_number: u64 = log_path_or_block.parse()
            .map_err(|_| eyre::eyre!("Invalid block number: {}", log_path_or_block))?;

        let entry = if let Some(cached) = cached_index_entries {
            *find_index_entry(cached, block_number)
                .ok_or_else(|| eyre::eyre!("Block {} not found in log index", block_number))?
        } else {
            let (mut idx_file, _) = open_log_files(log_dir)?;
            let index_entries = read_index_file(&mut idx_file)?;
            *find_index_entry(&index_entries, block_number)
                .ok_or_else(|| eyre::eyre!("Block {} not found in log index", block_number))?
        };

        let data = if let Some(file_handle) = global_file_handle {
            file_handle.read_range(entry.offset, entry.length)?
        } else {
            let bin_path = log_dir.join("blocks_log.bin");
            let mut file = std::fs::File::open(&bin_path)?;
            file.seek(SeekFrom::Start(entry.offset))?;
            let mut buffer = vec![0u8; entry.length as usize];
            file.read_exact(&mut buffer)?;
            buffer
        };

        if is_in_memory_mode && !data.is_empty() && data[0] <= 4 {
            if let Ok(algorithm) = CompressionAlgorithm::from_u8(data[0]) {
                let compressed_data = &data[1..];
                if let Ok(uncompressed_data) = decompress_data(compressed_data, algorithm) {
                    return Ok(uncompressed_data);
                }
            }
        }

        Ok(data)
    } else {
        let mut file = std::fs::File::open(log_path_or_block)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        if is_in_memory_mode && !buffer.is_empty() && buffer[0] <= 4 {
            if let Ok(algorithm) = CompressionAlgorithm::from_u8(buffer[0]) {
                let compressed_data = &buffer[1..];
                if let Ok(uncompressed_data) = decompress_data(compressed_data, algorithm) {
                    return Ok(uncompressed_data);
                }
            }
        }

        Ok(buffer)
    }
}

/// 写入日志到累加文件系统
pub(crate) fn write_read_logs_binary(
    block_number: u64,
    read_logs: &[ReadLogEntry],
    compression_algorithm: &str,
    log_dir: Option<&Path>,
    _existing_blocks: &std::collections::HashSet<u64>,
    _single_thread: bool,
    buffered_writer: Option<&mut BufferedLogWriter>,
) -> eyre::Result<()> {
    let estimated_size = read_logs.len() * 50;
    let mut uncompressed_data = Vec::with_capacity(estimated_size + 8);

    uncompressed_data.write_all(&(read_logs.len() as u64).to_le_bytes())?;

    for entry in read_logs {
        let data = match entry {
            ReadLogEntry::Account { data, .. } => data,
            ReadLogEntry::Storage { data, .. } => data,
        };

        if data.len() > 255 {
            return Err(eyre::eyre!("Data too long: {} bytes (max 255)", data.len()));
        }
        uncompressed_data.push(data.len() as u8);
        uncompressed_data.extend_from_slice(data);
    }

    let env_var = std::env::var("PEVM_IN_MEMORY_MODE");
    let enable_in_memory_mode = env_var
        .as_ref()
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    let uncompressed_size = uncompressed_data.len();

    let (final_data, algorithm_opt, compressed_size) = if enable_in_memory_mode {
        (uncompressed_data, None, 0)
    } else {
        let (algorithm, compressed_data, _stats) = choose_compression_algorithm(&uncompressed_data, compression_algorithm)?;
        let compressed_size = compressed_data.len();

        let mut data = Vec::new();
        data.push(algorithm.as_u8());
        data.extend_from_slice(&compressed_data);
        (data, Some(algorithm), compressed_size)
    };

    if let Some(log_dir) = log_dir {
        if let Some(writer) = buffered_writer {
            let offset = writer.get_bin_file_position()?;
            writer.write_bin_data(&final_data)?;
            let length = final_data.len() as u64;
            writer.write_index_entry(block_number, offset, length)?;
            writer.auto_flush_if_needed()?;

            if enable_in_memory_mode {
                debug!("Binary log written (buffered, no compression): block {} ({} entries, {} bytes)",
                    block_number, read_logs.len(), uncompressed_size);
            } else if let Some(alg) = algorithm_opt {
                debug!("Binary log written (buffered): block {} ({} entries, {:?}, {} -> {} bytes)",
                    block_number, read_logs.len(), alg, uncompressed_size, compressed_size);
            }
        } else {
            let (mut idx_file, mut bin_file) = open_log_files(log_dir)?;

            let index_entries = read_index_file(&mut idx_file)?;
            if find_index_entry(&index_entries, block_number).is_some() {
                debug!("Block {} already exists, skipping", block_number);
                return Ok(());
            }

            let offset = bin_file.seek(SeekFrom::End(0))?;
            bin_file.write_all(&final_data)?;
            bin_file.flush()?;

            let length = final_data.len() as u64;

            let mut index_buf = Vec::with_capacity(24);
            index_buf.write_all(&block_number.to_le_bytes())?;
            index_buf.write_all(&offset.to_le_bytes())?;
            index_buf.write_all(&length.to_le_bytes())?;
            idx_file.write_all(&index_buf)?;
            idx_file.flush()?;

            if enable_in_memory_mode {
                debug!("Binary log written: block {} ({} entries, {} bytes)",
                    block_number, read_logs.len(), uncompressed_size);
            } else if let Some(alg) = algorithm_opt {
                debug!("Binary log written: block {} ({} entries, {:?}, {} -> {} bytes)",
                    block_number, read_logs.len(), alg, uncompressed_size, compressed_size);
            }
        }
    } else {
        let log_file_path = format!("block_{}_reads.bin", block_number);
        let mut log_file = BufWriter::new(File::create(&log_file_path)?);
        log_file.write_all(&final_data)?;
        log_file.flush()?;

        if enable_in_memory_mode {
            debug!("Binary log written to: {} ({} entries, no compression, {} bytes)",
                log_file_path, read_logs.len(), uncompressed_size);
        } else if let Some(alg) = algorithm_opt {
            debug!("Binary log written to: {} ({} entries, {:?}, {} -> {} bytes)",
                log_file_path, read_logs.len(), alg, uncompressed_size, compressed_size);
        }
    }

    Ok(())
}
