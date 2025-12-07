//! `reth evm` command.

#[cfg(any(unix, windows))]
mod profiling;

use clap::Parser;
use reth_chainspec::ChainSpec;
use reth_cli::chainspec::ChainSpecParser;
use reth_cli_commands::common::CliNodeTypes;
use reth_cli_runner::CliContext;
use reth_ethereum_primitives::EthPrimitives;
use reth_node_ethereum::EthEngineTypes;

use tracing::{info, debug, error, warn};
use std::time::{Duration, Instant};
use reth_cli_commands::common::{AccessRights, Environment, EnvironmentArgs};

use reth_consensus::FullConsensus;
use reth_provider::{
    BlockNumReader, HeaderProvider, ProviderError, 
    providers::BlockchainProvider, BlockReader, ChainSpecProvider, StateProviderFactory,
};
use reth_errors::ConsensusError;
use reth_node_ethereum::{consensus::EthBeaconConsensus, EthEvmConfig};
use reth_evm::{execute::Executor, ConfigureEvm};
use reth_revm::database::StateProviderDatabase;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}};
use std::thread;
use std::thread::JoinHandle;
use eyre::{Report, Result};
use std::panic::AssertUnwindSafe;
use reth_primitives_traits::{BlockBody, format_gas_throughput};
use tokio::signal;
use std::fs::File;
use std::io::{Write, Read, BufWriter, BufReader};
use std::path::{Path, PathBuf};
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom};
use alloy_primitives::{Address, B256, U256};
use crate::revm::Database as RevmDatabase;
use crate::revm::state::{AccountInfo, Bytecode};
// reth_db 相关导入暂时未使用（已改为文件系统实现）
// use reth_db::DatabaseEnv;
// use reth_db::Database as RethDatabase;
// use reth_db::transaction::DbTx;

/// EVM commands
#[derive(Debug, Parser)]
pub struct EvmCommand<C: ChainSpecParser> {
    #[command(flatten)]
    env: EnvironmentArgs<C>,
    /// begin block number
    #[arg(long, alias = "begin", short = 'b')]
    begin_number: u64,
    /// end block number
    #[arg(long, alias = "end", short = 'e')]
    end_number: u64,
    /// step size for loop
    #[arg(long, alias = "step", short = 's', default_value = "100")]
    step_size: usize,
    /// Enable logging for blocks (use "on" to log all blocks in range, or a specific block number)
    #[arg(long, alias = "log-block")]
    log_block: Option<String>,
    /// Use log file for execution: "on" to use logs for all blocks in range (requires --log-dir), or a specific block number/file path
    #[arg(long, alias = "use-log")]
    use_log: Option<String>,
    /// Compression algorithm to use: none, zstd, brotli, lzma, lz4, or auto (default: zstd)
    #[arg(long, alias = "compression", default_value = "zstd")]
    compression_algorithm: String,
    /// Path to log directory for storing accumulated log bin files (default: current directory)
    /// Uses accumulated format: blocks_log.bin (data) and blocks_log.idx (index with offsets and lengths)
    #[arg(long, alias = "log-dir")]
    log_dir: Option<PathBuf>,
    /// Use single thread for execution (useful for log generation to avoid file locking)
    #[arg(long, alias = "single-thread")]
    single_thread: bool,
    /// Enable CPU profiling and generate flamegraph (output: flamegraph.svg)
    #[arg(long, alias = "profile")]
    enable_profiling: bool,
}

struct Task {
    start: u64,
    end: u64,
}

/// Read log entry types - 按访问顺序记录
#[derive(Debug, Clone)]
pub(crate) enum ReadLogEntry {
    Account { address: Address, data: Vec<u8> },
    Storage { address: Address, key: B256, data: Vec<u8> },
}

/// 包装的 Database，用于拦截读取操作并记录日志
struct LoggingDatabase<DB> {
    inner: DB,
    read_logs: Arc<Mutex<Vec<ReadLogEntry>>>,
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
        
        // 记录账户读取：使用拦截到的 AccountInfo 数据，编码为与 PlainState 一致的 RLP 格式
        // 即使账户不存在，也要记录一个条目以保持顺序一致
        // 优化：先准备数据，再快速加锁写入
        let log_entry = if let Some(account_info) = &result {
            // PlainState 中账户的 RLP 编码格式: [nonce, balance, storage_root?, code_hash?]
            // 如果 storage_root 为 ZERO 或 code_hash 为空 code 的 hash，则省略
            use alloy_rlp::Encodable;
            
            // 空 code 的 hash: KECCAK256("") = 0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470
            let empty_code_hash = B256::from_slice(&[
                0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c,
                0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
                0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b,
                0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
            ]);
            
            // 尝试从 AccountInfo 获取 storage_root
            // 注意：在 revm 中，AccountInfo 可能不直接包含 storage_root
            // 但我们可以通过检查账户是否有存储来确定 storage_root
            // 如果账户有存储，storage_root 可能不为 ZERO
            // 目前，我们使用 B256::ZERO 作为默认值，但如果需要，可以从 StateProvider 获取
            // 为了简化，我们先使用 ZERO，如果将来需要支持非 ZERO 的 storage_root，需要从 StateProvider 获取
            let storage_root = B256::ZERO; // TODO: 如果账户有存储，应该从 StateProvider 获取实际的 storage_root
            let code_hash = account_info.code_hash();
            
            // 决定哪些字段需要编码
            // 如果 storage_root 不为 ZERO，也要编码
            let include_storage_root = storage_root != B256::ZERO;
            let include_code_hash = code_hash != empty_code_hash;
            
            // 计算 payload 长度
            let mut payload_len = account_info.nonce.length() + account_info.balance.length();
            if include_storage_root {
                payload_len += storage_root.length();
            }
            if include_code_hash {
                payload_len += code_hash.length();
            }
            
            let mut buf = Vec::new();
            let header = alloy_rlp::Header {
                list: true,
                payload_length: payload_len,
            };
            header.encode(&mut buf);
            account_info.nonce.encode(&mut buf);
            account_info.balance.encode(&mut buf);
            if include_storage_root {
                storage_root.encode(&mut buf);
            }
            if include_code_hash {
                code_hash.encode(&mut buf);
            }
            
            ReadLogEntry::Account {
                address,
                data: buf,
            }
        } else {
            // 账户不存在，记录一个空的 RLP 列表（表示账户不存在）
            // RLP 空列表: 0xc0
            ReadLogEntry::Account {
                address,
                data: vec![0xc0],
            }
        };
        
        // 快速加锁写入（减少锁持有时间）
        let mut logs = self.read_logs.lock().unwrap();
        logs.push(log_entry);
        drop(logs); // 显式释放锁
        
        Ok(result)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, <DB as RevmDatabase>::Error> {
        // 代码读取忽略（按用户要求），不记录到日志
        self.inner.code_by_hash(code_hash)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, <DB as RevmDatabase>::Error> {
        // 先调用 inner，减少锁持有时间
        let result = self.inner.storage(address, index)?;
        
        // 记录存储读取：使用拦截到的存储值，编码为 RLP
        // 注意：index 是 U256，需要转换为 32 字节的 key
        
        // 将存储值编码为 RLP（在锁外完成）
        use alloy_rlp::Encodable;
        let key_bytes = index.to_be_bytes::<32>();
        let key = B256::from_slice(&key_bytes);
        let mut buf = Vec::new();
        result.encode(&mut buf);
        
        // 快速加锁写入（减少锁持有时间）
        let mut logs = self.read_logs.lock().unwrap();
        logs.push(ReadLogEntry::Storage {
            address,
            key,
            data: buf,
        });
        drop(logs); // 显式释放锁
        
        Ok(result)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, <DB as RevmDatabase>::Error> {
        self.inner.block_hash(number)
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

/// 压缩算法类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompressionAlgorithm {
    None,
    Zstd,
    Brotli,
    Lzma,
    Lz4,
}

impl CompressionAlgorithm {
    fn as_u8(self) -> u8 {
        match self {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::Zstd => 1,
            CompressionAlgorithm::Brotli => 2,
            CompressionAlgorithm::Lzma => 3,
            CompressionAlgorithm::Lz4 => 4,
        }
    }
    
    fn from_u8(v: u8) -> eyre::Result<Self> {
        match v {
            0 => Ok(CompressionAlgorithm::None),
            1 => Ok(CompressionAlgorithm::Zstd),
            2 => Ok(CompressionAlgorithm::Brotli),
            3 => Ok(CompressionAlgorithm::Lzma),
            4 => Ok(CompressionAlgorithm::Lz4),
            _ => Err(eyre::eyre!("Unknown compression algorithm: {}", v)),
        }
    }
}

/// 压缩数据
fn compress_data(data: &[u8], algorithm: CompressionAlgorithm) -> eyre::Result<Vec<u8>> {
    match algorithm {
        CompressionAlgorithm::None => Ok(data.to_vec()),
        CompressionAlgorithm::Zstd => {
            zstd::encode_all(data, 5).map_err(|e| eyre::eyre!("Zstd compression failed: {}", e))
        }
        CompressionAlgorithm::Brotli => {
            let mut compressed = Vec::new();
            {
                let mut writer = brotli::CompressorWriter::new(&mut compressed, 4096, 11, 22);
                writer.write_all(data).map_err(|e| eyre::eyre!("Brotli compression failed: {}", e))?;
            }
            Ok(compressed)
        }
        CompressionAlgorithm::Lzma => {
            let mut compressed = Vec::new();
            let mut encoder = xz2::write::XzEncoder::new(compressed, 6);
            encoder.write_all(data).map_err(|e| eyre::eyre!("LZMA compression failed: {}", e))?;
            compressed = encoder.finish().map_err(|e| eyre::eyre!("LZMA compression failed: {}", e))?;
            Ok(compressed)
        }
        CompressionAlgorithm::Lz4 => {
            // lz4_flex 需要预先知道解压后的大小，所以我们先写入大小
            let compressed = lz4_flex::compress(data);
            let mut result = Vec::new();
            result.write_all(&(data.len() as u32).to_le_bytes())
                .map_err(|e| eyre::eyre!("LZ4 compression failed: {}", e))?;
            result.write_all(&compressed)
                .map_err(|e| eyre::eyre!("LZ4 compression failed: {}", e))?;
            Ok(result)
        }
    }
}

/// 解压数据
fn decompress_data(data: &[u8], algorithm: CompressionAlgorithm) -> eyre::Result<Vec<u8>> {
    match algorithm {
        CompressionAlgorithm::None => Ok(data.to_vec()),
        CompressionAlgorithm::Zstd => {
            zstd::decode_all(data).map_err(|e| eyre::eyre!("Zstd decompression failed: {}", e))
        }
        CompressionAlgorithm::Brotli => {
            let mut decompressed = Vec::new();
            let mut reader = brotli::Decompressor::new(data, 4096);
            reader.read_to_end(&mut decompressed)
                .map_err(|e| eyre::eyre!("Brotli decompression failed: {}", e))?;
            Ok(decompressed)
        }
        CompressionAlgorithm::Lzma => {
            let mut decompressed = Vec::new();
            xz2::read::XzDecoder::new(data)
                .read_to_end(&mut decompressed)
                .map_err(|e| eyre::eyre!("LZMA decompression failed: {}", e))?;
            Ok(decompressed)
        }
        CompressionAlgorithm::Lz4 => {
            // 读取大小前缀
            if data.len() < 4 {
                return Err(eyre::eyre!("LZ4 decompression failed: data too short"));
            }
            let size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
            let compressed_data = &data[4..];
            lz4_flex::decompress(compressed_data, size)
                .map_err(|e| eyre::eyre!("LZ4 decompression failed: {}", e))
        }
    }
}

/// 压缩统计信息
#[derive(Debug)]
struct CompressionStats {
    algorithm: CompressionAlgorithm,
    compressed_size: usize,
    compression_time: Duration,
    decompression_time: Duration,
    ratio: f64,
}

/// 测试所有压缩算法并记录统计信息
fn compare_all_compressions(data: &[u8]) -> Vec<CompressionStats> {
    let algorithms = [
        CompressionAlgorithm::Zstd,
        CompressionAlgorithm::Brotli,
        CompressionAlgorithm::Lzma,
        CompressionAlgorithm::Lz4,
    ];
    
    let mut stats = Vec::new();
    
    for &alg in &algorithms {
        // 压缩时间
        let compress_start = Instant::now();
        if let Ok(compressed) = compress_data(data, alg) {
            let compress_time = compress_start.elapsed();
            
            // 解压时间
            let decompress_start = Instant::now();
            if decompress_data(&compressed, alg).is_ok() {
                let decompress_time = decompress_start.elapsed();
                
                let ratio = (compressed.len() as f64 / data.len() as f64) * 100.0;
                stats.push(CompressionStats {
                    algorithm: alg,
                    compressed_size: compressed.len(),
                    compression_time: compress_time,
                    decompression_time: decompress_time,
                    ratio,
                });
            }
        }
    }
    
    stats
}

/// 选择压缩算法（根据命令行参数）
/// 如果压缩后的长度大于原数据，自动使用不压缩（None）
fn choose_compression_algorithm(
    data: &[u8], 
    algorithm_str: &str
) -> eyre::Result<(CompressionAlgorithm, Vec<u8>, Vec<CompressionStats>)> {
    // 根据命令行参数选择算法
    let algorithm = match algorithm_str.to_lowercase().as_str() {
        "none" => CompressionAlgorithm::None,
        "zstd" => CompressionAlgorithm::Zstd,
        "brotli" => CompressionAlgorithm::Brotli,
        "lzma" => CompressionAlgorithm::Lzma,
        "lz4" => CompressionAlgorithm::Lz4,
        "auto" => {
            // 自动模式：测试所有算法并选择最好的
            let stats = compare_all_compressions(data);
            
            // 打印对比信息（debug 级别，避免刷屏）
            debug!("=== Compression Comparison ===");
            debug!("Original size: {} bytes", data.len());
            for stat in &stats {
                debug!("  {:?}: {} bytes ({:.2}%), compress={:.2}ms, decompress={:.2}ms", 
                    stat.algorithm, stat.compressed_size, stat.ratio,
                    stat.compression_time.as_secs_f64() * 1000.0,
                    stat.decompression_time.as_secs_f64() * 1000.0);
            }
            
            // 自动选择压缩率最好的（但必须小于原数据大小）
            let best = stats.iter()
                .filter(|s| s.compressed_size < data.len()) // 只考虑压缩后更小的
                .min_by(|a, b| a.compressed_size.cmp(&b.compressed_size));
            
            if let Some(best_stat) = best {
                best_stat.algorithm
            } else {
                // 如果所有压缩算法都比原数据大，使用不压缩
                CompressionAlgorithm::None
            }
        }
        _ => return Err(eyre::eyre!("Unknown compression algorithm: {}. Use: none, zstd, brotli, lzma, lz4, or auto", algorithm_str)),
    };
    
    // 压缩数据
    let compressed = if algorithm == CompressionAlgorithm::None {
        data.to_vec()
    } else {
        compress_data(data, algorithm)?
    };
    
    // 检查压缩后的大小：如果压缩后的长度大于或等于原数据，使用不压缩
    let (final_algorithm, final_data) = if compressed.len() >= data.len() {
        (CompressionAlgorithm::None, data.to_vec())
    } else {
        (algorithm, compressed)
    };
    
    // 返回空的统计信息（不再比较所有算法）
    Ok((final_algorithm, final_data, Vec::new()))
}

/// 索引条目：块号 -> (偏移量, 长度)
#[derive(Debug, Clone, Copy)]
pub(crate) struct IndexEntry {
    block_number: u64,
    offset: u64,
    length: u64,
}

/// 缓冲写入器管理器（用于优化写入性能）
pub(crate) struct BufferedLogWriter {
    idx_writer: BufWriter<File>,
    bin_writer: BufWriter<File>,
    #[allow(dead_code)]
    bin_file_path: std::path::PathBuf, // 保留用于将来可能的用途
}

impl BufferedLogWriter {
    /// 创建新的缓冲写入器，缓冲区大小为 1MB（IO 密集型软件的典型大小）
    fn new(log_dir: &Path) -> eyre::Result<Self> {
        // 确保目录存在
        std::fs::create_dir_all(log_dir)?;
        
        let idx_path = log_dir.join("blocks_log.idx");
        let bin_path = log_dir.join("blocks_log.bin");
        
        // 打开索引文件（追加模式）
        let idx_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(&idx_path)?;
        
        // 打开数据文件（追加模式）
        let bin_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(&bin_path)?;
        
        // 使用 1MB 缓冲区（1048576 字节）
        const BUFFER_SIZE: usize = 1024 * 1024;
        Ok(Self {
            idx_writer: BufWriter::with_capacity(BUFFER_SIZE, idx_file),
            bin_writer: BufWriter::with_capacity(BUFFER_SIZE, bin_file),
            bin_file_path: bin_path,
        })
    }
    
    /// 获取数据文件的当前位置（用于计算偏移量）
    fn get_bin_file_position(&mut self) -> eyre::Result<u64> {
        // 先刷新缓冲区，确保位置准确
        self.bin_writer.flush()?;
        // 获取底层文件的位置
        use std::io::Seek;
        self.bin_writer.get_mut().seek(SeekFrom::End(0))
            .map_err(|e| eyre::eyre!("Failed to get file position: {}", e))
    }
    
    /// 写入数据到数据文件
    fn write_bin_data(&mut self, data: &[u8]) -> eyre::Result<()> {
        self.bin_writer.write_all(data)
            .map_err(|e| eyre::eyre!("Failed to write bin data: {}", e))
    }
    
    /// 写入索引条目
    fn write_index_entry(&mut self, block_number: u64, offset: u64, length: u64) -> eyre::Result<()> {
        self.idx_writer.write_all(&block_number.to_le_bytes())?;
        self.idx_writer.write_all(&offset.to_le_bytes())?;
        self.idx_writer.write_all(&length.to_le_bytes())?;
        Ok(())
    }
    
    /// 刷新所有缓冲区（在程序结束或 Ctrl+C 时调用）
    fn flush_all(&mut self) -> eyre::Result<()> {
        self.idx_writer.flush()?;
        self.bin_writer.flush()?;
        Ok(())
    }
    
    /// 检查并自动刷新（如果缓冲区接近满）
    /// 注意：由于 BufWriter 不直接暴露缓冲区状态，我们使用定期刷新策略
    fn auto_flush_if_needed(&mut self) -> eyre::Result<()> {
        // 对于 BufWriter，我们无法直接检查缓冲区使用情况
        // 因此采用定期刷新策略（在调用方控制）
        // 这里保留接口以便将来扩展
        Ok(())
    }
}

/// 步骤1: 创建/打开索引文件和数据文件（使用文件系统，累加方式）
/// 索引文件格式：每个条目 24 字节 [block_number(8) + offset(8) + length(8)]
/// 数据文件：累加写入所有块的bin数据
/// 注意：此函数保留用于读取操作，写入操作使用 BufferedLogWriter
fn open_log_files(log_dir: &Path) -> eyre::Result<(File, File)> {
    // 确保目录存在
    std::fs::create_dir_all(log_dir)?;
    
    let idx_path = log_dir.join("blocks_log.idx");
    let bin_path = log_dir.join("blocks_log.bin");
    
    // 打开索引文件（追加模式）
    let idx_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .append(true)
        .open(&idx_path)?;
    
    // 打开数据文件（追加模式）
    let bin_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .append(true)
        .open(&bin_path)?;
    
    Ok((idx_file, bin_file))
}

/// 步骤2: 读取索引文件，获取所有块的索引条目（使用BufReader优化）
fn read_index_file(idx_file: &mut File) -> eyre::Result<Vec<IndexEntry>> {
    use std::io::Seek;
    idx_file.seek(SeekFrom::Start(0))?;
    
    // 使用BufReader包装文件（缓冲区大小64KB，适合索引文件读取）
    const BUFFER_SIZE: usize = 64 * 1024;
    let mut reader = BufReader::with_capacity(BUFFER_SIZE, idx_file);
    
    let mut entries = Vec::new();
    let mut buffer = [0u8; 24]; // 每个条目24字节
    
    loop {
        match reader.read_exact(&mut buffer) {
            Ok(_) => {
                let block_number = u64::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7]]);
                let offset = u64::from_le_bytes([buffer[8], buffer[9], buffer[10], buffer[11], buffer[12], buffer[13], buffer[14], buffer[15]]);
                let length = u64::from_le_bytes([buffer[16], buffer[17], buffer[18], buffer[19], buffer[20], buffer[21], buffer[22], buffer[23]]);
                entries.push(IndexEntry { block_number, offset, length });
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                break; // 文件结束
            }
            Err(e) => return Err(e.into()),
        }
    }
    
    Ok(entries)
}

/// 步骤2: 查找索引条目
fn find_index_entry(entries: &[IndexEntry], block_number: u64) -> Option<&IndexEntry> {
    entries.iter().find(|e| e.block_number == block_number)
}

/// 步骤3: 将log的bin文件累加写入数据文件，在索引中记录偏移量和长度
/// 写入日志到累加文件系统（版本5：按顺序，无address和key，无type，格式：data_len(1) + data，支持压缩）
/// 如果提供了 buffered_writer，使用缓冲写入；否则使用直接写入（向后兼容）
pub(crate) fn write_read_logs_binary(
    block_number: u64, 
    read_logs: &[ReadLogEntry],
    compression_algorithm: &str,
    log_dir: Option<&Path>,
    _existing_blocks: &std::collections::HashSet<u64>,
    _single_thread: bool, // 如果为 true，不需要文件锁
    buffered_writer: Option<&mut BufferedLogWriter>, // 可选的缓冲写入器
) -> eyre::Result<()> {
    // 先构建未压缩的数据（去掉前缀）
    // 性能优化：预分配容量，减少内存重新分配
    let estimated_size = read_logs.len() * 50; // 估算每个条目平均50字节
    let mut uncompressed_data = Vec::with_capacity(estimated_size + 8); // +8 for count
    
    // 写入条目数量
    uncompressed_data.write_all(&(read_logs.len() as u64).to_le_bytes())?;
    
    // 按照访问顺序写入日志（格式：data_len(1) + data）
    for entry in read_logs {
        let data = match entry {
            ReadLogEntry::Account { data, .. } => data,
            ReadLogEntry::Storage { data, .. } => data,
        };
        
        // 数据长度（1字节，最大255）
        if data.len() > 255 {
            return Err(eyre::eyre!("Data too long: {} bytes (max 255)", data.len()));
        }
        uncompressed_data.push(data.len() as u8); // 使用push而不是write_all，减少系统调用
        uncompressed_data.extend_from_slice(data); // 使用extend_from_slice，比write_all更高效
    }
    
    // 对比所有压缩算法并选择
    let (algorithm, compressed_data, _stats) = choose_compression_algorithm(&uncompressed_data, compression_algorithm)?;
    
    // 构建最终数据：压缩算法标识 + 压缩后的数据
    let mut final_data = Vec::new();
    final_data.write_all(&[algorithm.as_u8()])?; // 压缩算法标识
    final_data.write_all(&compressed_data)?; // 压缩后的数据
    
    if let Some(log_dir) = log_dir {
        if let Some(writer) = buffered_writer {
            // 使用缓冲写入器（性能优化）
            // 获取当前数据文件位置（即新的偏移量）
            let offset = writer.get_bin_file_position()?;
            
            // 写入数据文件（缓冲写入，不立即刷新）
            writer.write_bin_data(&final_data)?;
            
            // 计算数据长度
            let length = final_data.len() as u64;
            
            // 写入索引条目（缓冲写入，不立即刷新）
            writer.write_index_entry(block_number, offset, length)?;
            
            // 定期刷新（每 100 个块或缓冲区接近满时）
            // 这里简化处理，由调用方控制刷新频率
            writer.auto_flush_if_needed()?;
            
            debug!("Binary log written to accumulated files (buffered): block {} ({} entries, compressed with {:?}, {} -> {} bytes, {:.2}% ratio, offset: {}, length: {})", 
                block_number, read_logs.len(), algorithm, uncompressed_data.len(), compressed_data.len(),
                (compressed_data.len() as f64 / uncompressed_data.len() as f64) * 100.0, offset, length);
        } else {
            // 直接写入（向后兼容，不使用缓冲）
            let (mut idx_file, mut bin_file) = open_log_files(log_dir)?;
            
            // 读取现有索引，检查块是否已存在
            let index_entries = read_index_file(&mut idx_file)?;
            if find_index_entry(&index_entries, block_number).is_some() {
                // 块已存在，跳过写入（累加模式：已存在的块不再写入）
                debug!("Block {} already exists in log files, skipping", block_number);
                return Ok(());
            }
            
            // 获取当前数据文件位置（即新的偏移量）
            let offset = bin_file.seek(SeekFrom::End(0))?;
            
            // 写入数据文件
            bin_file.write_all(&final_data)?;
            bin_file.flush()?;
            
            // 计算数据长度
            let length = final_data.len() as u64;
            
            // 写入索引文件（追加模式）
            let mut index_buf = Vec::with_capacity(24);
            index_buf.write_all(&block_number.to_le_bytes())?;
            index_buf.write_all(&offset.to_le_bytes())?;
            index_buf.write_all(&length.to_le_bytes())?;
            idx_file.write_all(&index_buf)?;
            idx_file.flush()?;
            
            debug!("Binary log written to accumulated files: block {} ({} entries, compressed with {:?}, {} -> {} bytes, {:.2}% ratio, offset: {}, length: {})", 
                block_number, read_logs.len(), algorithm, uncompressed_data.len(), compressed_data.len(),
                (compressed_data.len() as f64 / uncompressed_data.len() as f64) * 100.0, offset, length);
        }
    } else {
        // 写入单个文件（向后兼容）
        let log_file_path = format!("block_{}_reads.bin", block_number);
        let mut log_file = BufWriter::new(File::create(&log_file_path)?);
        log_file.write_all(&final_data)?;
        log_file.flush()?;
        
        debug!("Binary log written to: {} ({} entries, compressed with {:?}, {} -> {} bytes, {:.2}% ratio)", 
            log_file_path, read_logs.len(), algorithm, uncompressed_data.len(), compressed_data.len(),
            (compressed_data.len() as f64 / uncompressed_data.len() as f64) * 100.0);
    }
    
    Ok(())
}

/// 从已打开的文件读取指定块的日志数据（性能优化版本，复用文件句柄）
fn read_log_entry_from_file(
    bin_reader: &mut BufReader<File>,
    entry: &IndexEntry,
    block_number: u64,
) -> eyre::Result<Vec<u8>> {
    // 定位到正确的偏移量
    let actual_offset = bin_reader.seek(SeekFrom::Start(entry.offset))?;
    if actual_offset != entry.offset {
        return Err(eyre::eyre!("Failed to seek to offset {} for block {} (actual: {})", entry.offset, block_number, actual_offset));
    }
    
    // 使用 read_exact 确保读取完整的数据（算法标识 + 压缩数据）
    let mut buffer = vec![0u8; entry.length as usize];
    bin_reader.read_exact(&mut buffer)
        .map_err(|e| eyre::eyre!("Failed to read log entry for block {}: offset={}, length={}, error: {}. This may indicate the log file is corrupted or the index is out of sync.", 
            block_number, entry.offset, entry.length, e))?;
    
    Ok(buffer)
}

/// 解析日志缓冲区（解压缩和解析，提取为ReadLogEntry列表）
fn parse_log_buffer(buffer: &[u8]) -> eyre::Result<Vec<ReadLogEntry>> {
    let mut cursor = std::io::Cursor::new(buffer);
    
    // 读取压缩算法标识
    let mut algorithm_byte = [0u8; 1];
    cursor.read_exact(&mut algorithm_byte)?;
    let algorithm = CompressionAlgorithm::from_u8(algorithm_byte[0])?;
    
    // 读取压缩后的数据
    let compressed_data = buffer[cursor.position() as usize..].to_vec();
    
    // 解压数据
    let uncompressed_data = decompress_data(&compressed_data, algorithm)?;
    let mut cursor = std::io::Cursor::new(&uncompressed_data);
    
    // 读取条目数量
    let mut count_bytes = [0u8; 8];
    cursor.read_exact(&mut count_bytes)?;
    let count = u64::from_le_bytes(count_bytes);
    
    let mut entries = Vec::new();
    
    // 版本5格式：data_len(1) + data（按顺序读取，类型通过数据内容推断）
    for _entry_index in 0..count {
        // 读取数据长度（1字节）
        let mut data_len_bytes = [0u8; 1];
        cursor.read_exact(&mut data_len_bytes)?;
        let data_len = data_len_bytes[0] as usize;
        
        // 读取数据
        let mut data = vec![0u8; data_len];
        cursor.read_exact(&mut data)?;
        
        // 通过数据内容推断类型
        use alloy_rlp::Decodable;
        let data_copy = data.clone();
        let is_storage = if let Ok(_) = U256::decode(&mut data_copy.as_slice()) {
            data_len <= 33
        } else {
            false
        };
        
        if is_storage {
            entries.push(ReadLogEntry::Storage { 
                address: Address::ZERO,
                key: B256::ZERO,
                data 
            });
        } else {
            entries.push(ReadLogEntry::Account { 
                address: Address::ZERO,
                data 
            });
        }
    }
    
    Ok(entries)
}

/// 步骤2和3: 从累加文件系统或单个文件读取日志（版本5：按顺序，无address和key，无type，支持压缩）
/// 如果提供了缓存的索引条目，使用缓存；否则读取索引文件
pub(crate) fn read_read_logs_binary(
    log_path_or_block: &str,
    log_dir: Option<&Path>,
    cached_index_entries: Option<&[IndexEntry]>,
) -> eyre::Result<Vec<ReadLogEntry>> {
    let file_contents = if let Some(log_dir) = log_dir {
        // 步骤2和3: 从累加文件系统读取（使用索引查找偏移量和长度）
        // 尝试解析块号
        let block_number: u64 = log_path_or_block.parse()
            .map_err(|_| eyre::eyre!("Invalid block number: {}", log_path_or_block))?;
        
        // 使用缓存的索引条目或读取索引文件
        let entry = if let Some(cached) = cached_index_entries {
            // 使用缓存的索引条目（性能优化：避免每次读取都打开索引文件）
            *find_index_entry(cached, block_number)
                .ok_or_else(|| eyre::eyre!("Block {} not found in log index", block_number))?
        } else {
            // 读取索引文件（向后兼容，但性能较差）
            let (mut idx_file, _) = open_log_files(log_dir)?;
            let index_entries = read_index_file(&mut idx_file)?;
            *find_index_entry(&index_entries, block_number)
                .ok_or_else(|| eyre::eyre!("Block {} not found in log index", block_number))?
        };
        
        // 打开数据文件（只读，使用BufReader优化读取性能）
        let bin_path = log_dir.join("blocks_log.bin");
        let bin_file = std::fs::File::open(&bin_path)?;
        
        // 从数据文件读取指定范围的数据
        // 先检查文件大小
        let file_end = bin_file.metadata()?.len();
        if entry.offset + entry.length > file_end {
            return Err(eyre::eyre!("Block {} log entry extends beyond file end: offset={}, length={}, file_size={}. This may indicate the log file is corrupted or incomplete.", 
                block_number, entry.offset, entry.length, file_end));
        }
        
        // 使用BufReader包装文件（缓冲区大小1MB，与写入时一致）
        const BUFFER_SIZE: usize = 1024 * 1024;
        let mut bin_reader = BufReader::with_capacity(BUFFER_SIZE, bin_file);
        
        // 使用辅助函数读取数据
        read_log_entry_from_file(&mut bin_reader, &entry, block_number)?
    } else {
        // 从单个文件读取（向后兼容，使用BufReader优化）
        let file = std::fs::File::open(log_path_or_block)?;
        const BUFFER_SIZE: usize = 1024 * 1024;
        let mut reader = BufReader::with_capacity(BUFFER_SIZE, file);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;
        buffer
    };
    
    let mut cursor = std::io::Cursor::new(&file_contents);
    
    // 读取压缩算法标识（去掉前缀）
    let mut algorithm_byte = [0u8; 1];
    cursor.read_exact(&mut algorithm_byte)?;
    let algorithm = CompressionAlgorithm::from_u8(algorithm_byte[0])?;
    
    // 读取压缩后的数据
    let compressed_data = file_contents[cursor.position() as usize..].to_vec();
    
    // 解压数据
    let uncompressed_data = decompress_data(&compressed_data, algorithm)?;
    let mut cursor = std::io::Cursor::new(&uncompressed_data);
    
    // 读取条目数量
    let mut count_bytes = [0u8; 8];
    cursor.read_exact(&mut count_bytes)?;
    let count = u64::from_le_bytes(count_bytes);
    
    let mut entries = Vec::new();
    
    // 版本5格式：data_len(1) + data（按顺序读取，类型通过数据内容推断）
    for _entry_index in 0..count {
        // 读取数据长度（1字节）
        let mut data_len_bytes = [0u8; 1];
        cursor.read_exact(&mut data_len_bytes)?;
        let data_len = data_len_bytes[0] as usize;
        
        // 读取数据
        let mut data = vec![0u8; data_len];
        cursor.read_exact(&mut data)?;
        
        // 通过数据内容推断类型：
        // Account 的 RLP 编码通常包含多个字段（nonce, balance, storage_root?, code_hash?），长度较长
        // Storage 的 RLP 编码只有一个 U256 值，长度较短（通常 <= 33 字节）
        // 如果数据长度 <= 33 字节，可能是 Storage；否则是 Account
        // 但更准确的方法是尝试解码：如果能解码为 U256 且长度匹配，则是 Storage；否则是 Account
        use alloy_rlp::Decodable;
        let data_copy = data.clone();
        let is_storage = if let Ok(_) = U256::decode(&mut data_copy.as_slice()) {
            // 能解码为 U256，且解码后数据应该被消耗完（或接近）
            data_len <= 33
        } else {
            false
        };
        
        if is_storage {
            entries.push(ReadLogEntry::Storage { 
                address: Address::ZERO, // 占位符，不使用
                key: B256::ZERO, // 占位符，不使用
                data 
            });
        } else {
            entries.push(ReadLogEntry::Account { 
                address: Address::ZERO, // 占位符，不使用
                data 
            });
        }
    }
    
    Ok(entries)
}

/// 从日志文件读取数据的 Database（按顺序访问）
pub(crate) struct LoggedDatabase {
    log_entries: Vec<ReadLogEntry>, // 日志条目（按顺序）
    current_index: usize, // 当前访问索引
    state_provider: Box<dyn reth_provider::StateProvider>, // 用于 code_by_hash（代码不在日志文件中）
    code_cache: std::collections::HashMap<B256, Bytecode>, // 代码缓存，避免重复加载
}

impl std::fmt::Debug for LoggedDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoggedDatabase")
            .field("log_entries_count", &self.log_entries.len())
            .field("current_index", &self.current_index)
            .field("code_cache_size", &self.code_cache.len())
            .finish_non_exhaustive()
    }
}

impl LoggedDatabase {
    fn new(log_entries: Vec<ReadLogEntry>, state_provider: Box<dyn reth_provider::StateProvider>) -> Self {
        Self {
            log_entries,
            current_index: 0,
            state_provider,
            code_cache: std::collections::HashMap::new(),
        }
    }
    
    /// 获取下一个 Account 数据（按顺序）
    /// 性能优化：避免不必要的克隆，直接返回引用（但需要克隆，因为调用方需要拥有数据）
    fn next_account_data(&mut self) -> Option<Vec<u8>> {
        while self.current_index < self.log_entries.len() {
            match &self.log_entries[self.current_index] {
                ReadLogEntry::Account { data, .. } => {
                    // 必须克隆，因为调用方需要拥有数据
                    let data = data.clone();
                    self.current_index += 1;
                    return Some(data);
                }
                ReadLogEntry::Storage { .. } => {
                    // 跳过 Storage 条目
                    self.current_index += 1;
                }
            }
        }
        None
    }
    
    /// 获取下一个 Storage 数据（按顺序）
    /// 性能优化：避免不必要的克隆，直接返回引用（但需要克隆，因为调用方需要拥有数据）
    fn next_storage_data(&mut self) -> Option<Vec<u8>> {
        while self.current_index < self.log_entries.len() {
            match &self.log_entries[self.current_index] {
                ReadLogEntry::Storage { data, .. } => {
                    // 必须克隆，因为调用方需要拥有数据
                    let data = data.clone();
                    self.current_index += 1;
                    return Some(data);
                }
                ReadLogEntry::Account { .. } => {
                    // 跳过 Account 条目
                    self.current_index += 1;
                }
            }
        }
        None
    }
    
    /// 从 RLP 数据解码账户信息，处理可选的 storage_root 和 code_hash
    pub(crate) fn decode_account_rlp(data: &[u8]) -> eyre::Result<AccountInfo> {
        use alloy_rlp::Decodable;
        
        // 空 code 的 hash
        let empty_code_hash = B256::from_slice(&[
            0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c,
            0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
            0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b,
            0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
        ]);
        
        let mut data = data;
        let header = alloy_rlp::Header::decode(&mut data)?;
        if !header.list {
            return Err(eyre::eyre!("Account RLP must be a list"));
        }
        
        // 解码 nonce
        let nonce = u64::decode(&mut data)?;
        
        // 解码 balance
        let balance = U256::decode(&mut data)?;
        
        // 根据编码逻辑：如果 storage_root 是 ZERO，它不会被编码
        // 如果 storage_root 不为 ZERO，它会被编码在 balance 之后
        // 如果 code_hash 不是空 code 的 hash，它会被编码在 storage_root 之后（如果 storage_root 存在）或 balance 之后（如果 storage_root 不存在）
        
        // 尝试解码 storage_root（如果存在）
        // 如果还有数据，可能是 storage_root（如果 storage_root 不为 ZERO）或 code_hash（如果 storage_root 为 ZERO 且 code_hash 不为空）
        // 我们需要判断：如果下一个 B256 是空 code hash，那么它可能是 storage_root（如果 storage_root 不为 ZERO）
        // 否则，如果下一个 B256 不是空 code hash，它可能是 storage_root 或 code_hash
        
        // 解码 storage_root 和 code_hash
        // 根据编码逻辑：如果 storage_root 不为 ZERO，它会被编码在 balance 之后
        // 如果 code_hash 不为空 code hash，它会被编码在 storage_root 之后（如果 storage_root 存在）或 balance 之后（如果 storage_root 不存在）
        let (storage_root, code_hash) = if data.is_empty() {
            // 没有更多数据，使用默认值
            (B256::ZERO, empty_code_hash)
        } else {
            // 还有数据，尝试解码
            // 先尝试解码一个 B256
            let first_b256 = B256::decode(&mut data)?;
            
            if data.is_empty() {
                // 只有一个 B256，可能是 storage_root（如果 storage_root 不为 ZERO）或 code_hash（如果 storage_root 为 ZERO）
                // 如果它是空 code hash，那么它可能是 storage_root（如果 storage_root 不为 ZERO）
                // 否则，它可能是 code_hash
                if first_b256 == empty_code_hash {
                    // 这可能是 storage_root（如果 storage_root 不为 ZERO），但通常 storage_root 不会是空 code hash
                    // 更可能的情况是：storage_root 为 ZERO（被省略），code_hash 为空 code hash（也被省略）
                    // 但这里只有一个 B256，所以可能是 storage_root
                    (first_b256, empty_code_hash)
                } else {
                    // 这可能是 code_hash（如果 storage_root 为 ZERO 被省略）
                    (B256::ZERO, first_b256)
                }
            } else {
                // 还有更多数据，说明 storage_root 和 code_hash 都存在
                // 第一个是 storage_root，第二个是 code_hash
                let code_hash = B256::decode(&mut data)?;
                (first_b256, code_hash)
            }
        };
        
        // 注意：storage_root 被解码了，但 AccountInfo 可能不包含它
        // 我们在这里解码 storage_root 是为了确保格式正确，即使 AccountInfo 不使用它
        let _ = storage_root; // 标记为已使用，避免警告
        
        // 创建空的 Bytecode（因为 code 不在这里存储）
        let empty_bytecode = Bytecode::default();
        Ok(AccountInfo::new(
            balance,
            nonce,
            code_hash,
            empty_bytecode,
        ))
    }
}

impl crate::revm::Database for LoggedDatabase {
    type Error = <StateProviderDatabase<Box<dyn reth_provider::StateProvider>> as crate::revm::Database>::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        // 按顺序从日志文件读取数据（不读数据库，不对比）
        if let Some(rlp_data) = self.next_account_data() {
            // 检查是否是空账户标记（RLP 空列表 0xc0）
            if rlp_data.as_slice() == [0xc0] {
                // 日志文件说账户不存在
                return Ok(None);
            } else {
                // 从日志文件读取的数据解码
                match LoggedDatabase::decode_account_rlp(&rlp_data) {
                    Ok(mut logged_account_info) => {
                        // 如果 code_hash 不是 empty_code_hash，加载 bytecode
                        let empty_code_hash = B256::from_slice(&[
                            0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c,
                            0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
                            0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b,
                            0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
                        ]);
                        
                        let logged_code_hash = logged_account_info.code_hash();
                        if logged_code_hash != empty_code_hash {
                            match self.code_by_hash(logged_code_hash) {
                                Ok(code) => {
                                    logged_account_info.code = Some(code.clone());
                                }
                                Err(_err) => {
                                    // 加载失败，继续使用空的 code
                                }
                            }
                        }
                        
                        // 返回日志文件的数据
                        Ok(Some(logged_account_info))
                    }
                    Err(e) => {
                        warn!("LoggedDatabase::basic({}) failed to decode account RLP from log file: {}", address, e);
                        Ok(None)
                    }
                }
            }
        } else {
            warn!("LoggedDatabase::basic({}) NOT IN LOG FILE (no more account data)", address);
            Ok(None)
        }
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        // 先检查缓存
        if let Some(cached_code) = self.code_cache.get(&code_hash) {
            return Ok(cached_code.clone());
        }
        
        // 从数据库读取代码（代码不在日志文件中）
        let mut inner_db = StateProviderDatabase::new(self.state_provider.as_ref());
        let code = inner_db.code_by_hash(code_hash)?;
        
        // 缓存代码（限制缓存大小，避免内存爆炸）
        if self.code_cache.len() < 1000 {
            self.code_cache.insert(code_hash, code.clone());
        }
        
        Ok(code)
    }

    fn storage(&mut self, _address: Address, _index: U256) -> Result<U256, Self::Error> {
        // 按顺序从日志文件读取数据（不读数据库，不对比）
        if let Some(rlp_data) = self.next_storage_data() {
            // 从日志文件读取的数据解码
            use alloy_rlp::Decodable;
            let mut data = rlp_data.as_slice();
            match U256::decode(&mut data) {
                Ok(logged_value) => {
                    // 返回日志文件的数据
                    Ok(logged_value)
                }
                Err(_e) => {
                    // 解码失败，返回零值
                    Ok(U256::ZERO)
                }
            }
        } else {
            // 日志文件中没有更多存储数据
            Ok(U256::ZERO)
        }
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        // 创建一个临时的 StateProviderDatabase 来获取 block_hash
        let mut temp_db = StateProviderDatabase::new(self.state_provider.as_ref());
        temp_db.block_hash(number)
    }
}

/// Represents one Kilogas, or `1_000` gas.
pub const KILOGAS: u64 = 1_000;

/// Represents one Megagas, or `1_000_000` gas.
pub const MEGAGAS: u64 = KILOGAS * 1_000;

/// Represents one Gigagas, or `1_000_000_000` gas.
pub const GIGAGAS: u64 = MEGAGAS * 1_000;

/// Formats gas throughput as Gigagas per second.
///
/// # Arguments
///
/// * `gas` - Total gas consumed
/// * `execution_duration` - Duration of execution
///
/// # Returns
///
/// A formatted string representing the gas throughput in Ggas/s
pub fn format_gas_throughput_as_ggas(gas: u64, execution_duration: Duration) -> String {
    let gas_per_second = gas as f64 / execution_duration.as_secs_f64();
    format!("{:.2}", gas_per_second / GIGAGAS as f64)
}

impl<C: ChainSpecParser<ChainSpec = ChainSpec>> EvmCommand<C> {
    /// Execute `evm` command
    pub async fn execute<
        N: CliNodeTypes<
            Payload = EthEngineTypes,
            Primitives = EthPrimitives,
            ChainSpec = C::ChainSpec,
        >,
    >(
        self,
        _ctx: CliContext,
    ) -> eyre::Result<()> {
        info!("Executing EVM command...");

        // 如果启用了性能分析，启动 CPU profiling（跨平台支持）
        #[cfg(any(unix, windows))]
        let mut _profiler_guard = if self.enable_profiling {
            info!("CPU profiling enabled, will generate flamegraph.svg after execution");
            // 采样频率 100Hz（每秒采样 100 次）
            match profiling::ProfilerGuardWrapper::new(100) {
                Ok(guard) => {
                    #[cfg(unix)]
                    info!("CPU profiler started (100Hz sampling rate, using pprof-rs)");
                    #[cfg(windows)]
                    info!("CPU profiler started (100Hz sampling rate, using thread-based sampling)");
                    Some(guard)
                }
                Err(e) => {
                    warn!("Failed to start CPU profiler: {}. Continuing without profiling.", e);
                    None
                }
            }
        } else {
            None
        };
        
        #[cfg(not(any(unix, windows)))]
        let _profiler_guard: Option<()> = if self.enable_profiling {
            warn!("CPU profiling is not supported on this platform.");
            None
        } else {
            None
        };

        // 步骤1: 获取日志目录（如果指定了 log_dir，使用累加文件系统；否则使用单个文件）
        let Environment { provider_factory, .. } = self.env.init::<N>(AccessRights::RO)?;
        let log_dir = self.log_dir.as_deref();
        
        // 如果使用累加文件系统，检查已存在的块（步骤4：累加模式，支持断点续传）
        // 同时缓存索引条目，避免每次读取都重新读取索引文件
        let (existing_blocks, cached_index_entries): (Arc<std::collections::HashSet<u64>>, Arc<Vec<IndexEntry>>) = if let Some(log_dir) = log_dir {
            let (mut idx_file, _) = open_log_files(log_dir)?;
            let index_entries = read_index_file(&mut idx_file)?;
            let blocks: std::collections::HashSet<u64> = index_entries.iter().map(|e| e.block_number).collect();
            if !blocks.is_empty() {
                let min_block = *blocks.iter().min().unwrap();
                let max_block = *blocks.iter().max().unwrap();
                info!("Using accumulated log files in directory: {} ({} blocks already indexed, range: {} - {})", 
                    log_dir.display(), blocks.len(), min_block, max_block);
            } else {
                info!("Using accumulated log files in directory: {} (no blocks indexed yet)", 
                    log_dir.display());
            }
            (Arc::new(blocks), Arc::new(index_entries))
        } else {
            (Arc::new(std::collections::HashSet::new()), Arc::new(Vec::new()))
        };

        // 检查是否启用日志记录模式（用于决定是否跳过已存在的块）
        let log_block_enabled = self.log_block.as_ref().map(|s| s == "on").unwrap_or(false);
        
        // 创建全局缓冲写入器（如果使用累加文件系统且启用日志记录）
        let buffered_writer: Option<Arc<Mutex<BufferedLogWriter>>> = if let Some(log_dir) = log_dir {
            if log_block_enabled {
                // 创建缓冲写入器，缓冲区大小为 1MB
                Some(Arc::new(Mutex::new(BufferedLogWriter::new(log_dir)?)))
            } else {
                None
            }
        } else {
            None
        };
        // 提前获取 chain_spec，避免在线程中重复调用（可能有锁）
        let chain_spec = provider_factory.chain_spec();
        let _consensus: Arc<dyn FullConsensus<EthPrimitives, Error = ConsensusError>> =
            Arc::new(EthBeaconConsensus::new(chain_spec.clone()));

        // 在 v1.8.4 中，共享 blockchain_db 也能正常工作
        let blockchain_db = BlockchainProvider::new(provider_factory.clone())?;
        let provider = provider_factory.provider()?;

        let last = provider.last_block_number()?;

        if self.begin_number > self.end_number {
            eyre::bail!("the begin block number is higher than the end block number")
        }
        if self.end_number > last {
            eyre::bail!("The end block number is higher than the latest block number")
        }

        // 检查是否启用日志记录模式（用于决定是否跳过已存在的块）
        let log_block_enabled = self.log_block.as_ref().map(|s| s == "on").unwrap_or(false);
        
        // 步骤4: 创建任务池
        // 只有在 --log-block on 模式下才跳过已存在的块（断点续传）
        // --use-log on 模式下不跳过，因为要执行所有块
        let mut tasks = VecDeque::new();
        let mut current_start = self.begin_number;
        while current_start <= self.end_number {
            // 如果使用累加模式且启用了日志记录（--log-block on），跳过已存在的块（断点续传）
            if let Some(_log_dir) = log_dir {
                if log_block_enabled {
                    // 找到下一个不存在的块
                    while current_start <= self.end_number && existing_blocks.contains(&current_start) {
                        current_start += 1;
                    }
                    if current_start > self.end_number {
                        break; // 所有块都已存在
                    }
                }
            }
            
            let mut current_end = std::cmp::min(current_start + self.step_size as u64 - 1, self.end_number);
            if current_end == self.end_number - 1 {
                current_end += 1;
            }
            
            // 如果使用累加模式且启用了日志记录（--log-block on），确保任务范围不包含已存在的块
            if let Some(_log_dir) = log_dir {
                if log_block_enabled {
                    // 调整 end，确保不包含已存在的块
                    while current_end >= current_start && existing_blocks.contains(&current_end) {
                        current_end -= 1;
                    }
                    if current_end < current_start {
                        current_start = current_end + 1;
                        continue;
                    }
                }
            }
            
            tasks.push_back(Task {
                start: current_start,
                end: current_end,
            });
            current_start = current_end + 1;
        }
        
        // 只有在 --log-block on 模式下，如果所有块都已存在才返回
        if tasks.is_empty() && log_dir.is_some() && log_block_enabled {
            info!("All blocks from {} to {} already exist in log files, nothing to do", 
                self.begin_number, self.end_number);
            return Ok(());
        }

        // 获取线程数：如果启用单线程模式，使用1个线程；否则使用多线程
        let thread_count = if self.single_thread {
            1
        } else {
            self.get_cpu_count() * 2 - 1
        };
        let mut threads: Vec<JoinHandle<Result<()>>> = Vec::with_capacity(thread_count);

        // 创建共享 gas 计数器和停止标志
        let task_queue = Arc::new(Mutex::new(tasks));
        let cumulative_gas = Arc::new(Mutex::new(0));
        let block_counter = Arc::new(Mutex::new(self.begin_number - 1));
        let txs_counter = Arc::new(Mutex::new(0));
        let should_stop = Arc::new(AtomicBool::new(false));

        // 设置 Ctrl+C 信号处理
        let should_stop_clone = Arc::clone(&should_stop);
        tokio::spawn(async move {
            if signal::ctrl_c().await.is_ok() {
                info!("Received Ctrl+C, shutting down gracefully...");
                should_stop_clone.store(true, Ordering::Relaxed);
            }
        });

        // 创建状态输出线程
        {
            let cumulative_gas = Arc::clone(&cumulative_gas);
            let block_counter = Arc::clone(&block_counter);
            let txs_counter = Arc::clone(&txs_counter);
            let should_stop_status = Arc::clone(&should_stop);
            let start = Instant::now();
            let begin_number = self.begin_number;
            let end_number = self.end_number;

            thread::spawn(move || {
                let mut previous_cumulative_gas: u64 = 0;
                let mut previous_block_counter: u64 = begin_number - 1;
                let mut previous_txs_counter: u64 = 0;
                loop {
                    // 检查停止标志
                    if should_stop_status.load(Ordering::Relaxed) {
                        info!("Status thread stopping due to Ctrl+C");
                        break;
                    }

                    thread::sleep(Duration::from_secs(1));

                    let current_cumulative_gas = cumulative_gas.lock().unwrap();
                    let diff_gas = *current_cumulative_gas - previous_cumulative_gas;
                    previous_cumulative_gas = *current_cumulative_gas;

                    let current_block_counter = block_counter.lock().unwrap();
                    let diff_block = *current_block_counter - previous_block_counter;
                    previous_block_counter = *current_block_counter;

                    let current_txs_counter = txs_counter.lock().unwrap();
                    let diff_txs = *current_txs_counter - previous_txs_counter;
                    previous_txs_counter = *current_txs_counter;

                    if diff_block > 0 {
                        let duration = start.elapsed();
                        let seconds = duration.as_secs();
                        let millis = duration.subsec_millis(); // Milliseconds part of the duration
                        let total_seconds = seconds as f64 + millis as f64 / 1000.0;
                        let gas_throughput_str = format_gas_throughput_as_ggas(diff_gas, Duration::from_secs(1));
                        //let gas_throughput_final : String =gas_throughput_str.chars().take(gas_throughput_str.len()-" Ggas/second".len()).collect();
                        let tps_colored = format!("\x1b[32m{}\x1b[0m", diff_txs);
                        let ggas_colored = format!("\x1b[32m{}\x1b[0m", gas_throughput_str);
                        info!(
                            bn = %current_block_counter,
                            txs = %current_txs_counter,
                            b_per_s = %diff_block,
                            TPS = %tps_colored,
                            Ggas_per_s = %ggas_colored,
                            time = %format!("{:.1}", total_seconds),
                            totalgas = %current_cumulative_gas,
                            "Execution"
                        );
                    }

                    if *current_block_counter >= (end_number - begin_number + 1) {
                        break;
                    }
                }
            });
        }

        for _ in 0..thread_count {
            let task_queue = Arc::clone(&task_queue);
            let cumulative_gas = Arc::clone(&cumulative_gas);
            let block_counter = Arc::clone(&block_counter);
            let txs_counter = Arc::clone(&txs_counter);
            let should_stop_worker = Arc::clone(&should_stop);

            let chain_spec = chain_spec.clone();
            let blockchain_db = blockchain_db.clone();
            let log_dir_clone = log_dir.map(|p| p.to_path_buf());
            let compression_algorithm = self.compression_algorithm.clone();
            let log_block_enabled = self.log_block.as_ref().map(|s| s == "on").unwrap_or(false);
            let use_log_enabled = self.use_log.as_ref().map(|s| s == "on").unwrap_or(false);
            let single_thread = self.single_thread;
            let existing_blocks_clone = Arc::clone(&existing_blocks);
            let cached_index_entries_clone = Arc::clone(&cached_index_entries);
            let buffered_writer_clone = buffered_writer.clone();

            // 在 v1.8.4 中，共享 blockchain_db 也能正常工作
            threads.push(thread::spawn(move || {
                let thread_id = thread::current().id();
                
                // 预先创建 EVM 配置，避免在循环中重复创建（这是线程级别的，可以复用）
                let evm_config = EthEvmConfig::ethereum(chain_spec.clone());
                
                loop {
                    // 检查停止标志
                    if should_stop_worker.load(Ordering::Relaxed) {
                        debug!(target: "exex::evm", thread_id = ?thread_id, "Worker thread stopping due to Ctrl+C");
                        break;
                    }

                    let task = {
                        let mut queue = task_queue.lock().unwrap();
                        if queue.is_empty() {
                            break;
                        }
                        queue.pop_front()
                    };

                    if let Some(task) = task {
                        // 再次检查停止标志，避免开始执行新任务
                        if should_stop_worker.load(Ordering::Relaxed) {
                            debug!(target: "exex::evm", thread_id = ?thread_id, "Worker thread stopping before task execution");
                            break;
                        }

                        debug!(
                            target: "exex::evm",
                            task_start = task.start,
                            task_end = task.end,
                            thread_id = ?thread_id,
                            "start loop",
                        );
                        let result = std::panic::catch_unwind(AssertUnwindSafe(|| -> Result<(), Report> {
                            let mut td = blockchain_db.header_td_by_number(task.start - 1)?
                                .ok_or_else(|| ProviderError::HeaderNotFound(task.start.into()))?;

                            // 使用共享的 blockchain_db（如 v1.8.4 的方式）
                            // 预先创建的 evm_config 避免重复创建
                            let blocks = blockchain_db.block_with_senders_range(task.start..=task.end).unwrap();

                            // 步骤4: 如果使用累加模式且启用了日志记录（--log-block on），过滤掉已存在的块（断点续传）
                            // 先收集需要处理的块号，用于统计
                            let blocks_to_process_numbers: Vec<u64> = if let Some(_log_dir) = &log_dir_clone {
                                if log_block_enabled {
                                    // 只在 --log-block on 模式下过滤已存在的块
                                    blocks.iter()
                                        .filter(|block| {
                                            let block_num = block.sealed_block().header().number;
                                            !existing_blocks_clone.contains(&block_num)
                                        })
                                        .map(|block| block.sealed_block().header().number)
                                        .collect()
                                } else {
                                    // --use-log on 或其他模式，不过滤，处理所有块
                                    blocks.iter().map(|block| block.sealed_block().header().number).collect()
                                }
                            } else {
                                blocks.iter().map(|block| block.sealed_block().header().number).collect()
                            };
                            
                            // 如果使用累加模式且启用了日志记录（--log-block on）且所有块都已存在，跳过执行
                            if let Some(_log_dir) = &log_dir_clone {
                                if log_block_enabled && blocks_to_process_numbers.is_empty() {
                                    debug!(target: "exex::evm", task_start = task.start, task_end = task.end, "All blocks in task already exist, skipping execution");
                                    return Ok(());
                                }
                            }

                            // 如果启用了日志记录（--log-block on），为每个块单独执行并记录日志
                            if log_block_enabled {
                                // 性能优化：批量收集日志数据，减少写入次数和锁竞争
                                // 先执行所有块并收集日志，然后批量写入
                                let mut block_logs: Vec<(u64, Vec<ReadLogEntry>)> = Vec::with_capacity(blocks.len());
                                
                                // 为每个块单独执行并记录日志
                                for block in blocks.iter() {
                                    // 在处理每个块之前检查停止标志
                                    if should_stop_worker.load(Ordering::Relaxed) {
                                        debug!(target: "exex::evm", thread_id = ?thread_id, "Worker thread stopping during block processing");
                                        return Ok(());
                                    }
                                    
                                    let block_number = block.sealed_block().header().number;
                                    
                                    // 如果使用累加模式且块已存在，跳过
                                    if let Some(_log_dir) = &log_dir_clone {
                                        if existing_blocks_clone.contains(&block_number) {
                                            continue;
                                        }
                                    }
                                    
                                    // 为每个块创建独立的 state provider 和日志记录器
                                    // 优化：使用任务范围的 state provider（如果块号接近）
                                    let state_provider = blockchain_db.history_by_block_number(
                                        std::cmp::max(task.start.checked_sub(1).unwrap_or(0), block_number.checked_sub(1).unwrap_or(0))
                                    )?;
                                    let inner_db = StateProviderDatabase::new(&state_provider);
                                    
                                    // 创建日志收集器
                                    let read_logs = Arc::new(Mutex::new(Vec::<ReadLogEntry>::new()));
                                    let logging_db = LoggingDatabase {
                                        inner: inner_db,
                                        read_logs: Arc::clone(&read_logs),
                                    };
                                    
                                    let executor = evm_config.batch_executor(logging_db);
                                    
                                    // 执行单个块
                                    let _output = executor.execute(block)?;
                                    
                                    // 再次检查停止标志（执行可能耗时较长）
                                    if should_stop_worker.load(Ordering::Relaxed) {
                                        debug!(target: "exex::evm", thread_id = ?thread_id, "Worker thread stopping after block execution");
                                        return Ok(());
                                    }
                                    
                                    // 获取记录的日志（快速获取，不持有锁太久）
                                    let logs = {
                                        let mut logs_guard = read_logs.lock().unwrap();
                                        logs_guard.drain(..).collect::<Vec<_>>()
                                    };
                                    
                                    if !logs.is_empty() {
                                        block_logs.push((block_number, logs));
                                    }
                                }
                                
                                // 批量写入日志文件（减少锁竞争和文件I/O）
                                if let Some(ref writer) = buffered_writer_clone {
                                    let mut writer_guard = writer.lock().unwrap();
                                    for (block_number, logs) in block_logs {
                                        write_read_logs_binary(block_number, &logs, &compression_algorithm, log_dir_clone.as_deref(), &existing_blocks_clone, single_thread, Some(&mut *writer_guard))?;
                                    }
                                    // 批量写入完成后刷新一次
                                    writer_guard.flush_all()?;
                                } else {
                                    for (block_number, logs) in block_logs {
                                        write_read_logs_binary(block_number, &logs, &compression_algorithm, log_dir_clone.as_deref(), &existing_blocks_clone, single_thread, None)?;
                                    }
                                }
                            } else if use_log_enabled {
                                // 如果启用了从日志执行（--use-log on），为每个块从累计日志中读取并执行
                                // 性能优化：批量预加载所有块的日志数据，减少文件I/O和创建开销
                                if let Some(log_dir) = &log_dir_clone {
                                    // 注意：StateProvider 不能跨块复用，因为每个块的状态不同
                                    // 但我们可以批量预加载日志数据，减少文件I/O
                                    
                                    // 打开文件句柄（任务范围内复用）
                                    let bin_path = log_dir.join("blocks_log.bin");
                                    let bin_file = std::fs::File::open(&bin_path)
                                        .map_err(|e| eyre::eyre!("Failed to open log file {}: {}", bin_path.display(), e))?;
                                    const BUFFER_SIZE: usize = 1024 * 1024;
                                    let mut bin_reader = BufReader::with_capacity(BUFFER_SIZE, bin_file);
                                    
                                    // 批量预加载所有块的日志数据
                                    // 性能优化：使用HashMap快速查找，避免每次都用find
                                    use std::collections::HashMap;
                                    let mut block_log_entries: HashMap<u64, Vec<ReadLogEntry>> = HashMap::with_capacity(blocks.len());
                                    for block in blocks.iter() {
                                        let block_number = block.sealed_block().header().number;
                                        
                                        // 使用缓存的索引条目查找块
                                        let entry = match find_index_entry(&cached_index_entries_clone, block_number) {
                                            Some(e) => e,
                                            None => {
                                                warn!("Block {} not found in log index, skipping", block_number);
                                                continue;
                                            }
                                        };
                                        
                                        // 从已打开的文件读取（性能优化：复用文件句柄）
                                        let buffer = match read_log_entry_from_file(&mut bin_reader, entry, block_number) {
                                            Ok(b) => b,
                                            Err(e) => {
                                                error!("Failed to read log for block {}: {}", block_number, e);
                                                continue;
                                            }
                                        };
                                        
                                        // 解析和解压缩数据（一次性完成）
                                        let entries = match parse_log_buffer(&buffer) {
                                            Ok(e) => {
                                                if e.is_empty() {
                                                    warn!("Block {} log file is empty, skipping", block_number);
                                                    continue;
                                                }
                                                e
                                            },
                                            Err(e) => {
                                                error!("Failed to parse log for block {}: {}", block_number, e);
                                                continue;
                                            }
                                        };
                                        
                                        block_log_entries.insert(block_number, entries);
                                    }
                                    
                                    // 性能优化：预先创建所有 LoggedDatabase 实例，减少查找开销
                                    // 然后按顺序执行，避免在循环中重复查找
                                    let mut block_db_pairs: Vec<(u64, LoggedDatabase)> = Vec::with_capacity(blocks.len());
                                    
                                    // 第一步：为所有块创建 LoggedDatabase（按块号顺序）
                                    for block in blocks.iter() {
                                        let block_number = block.sealed_block().header().number;
                                        
                                        // 快速查找对应的日志数据
                                        let log_entries = match block_log_entries.remove(&block_number) {
                                            Some(entries) => entries,
                                            None => {
                                                warn!("Block {} log data not found, skipping", block_number);
                                                continue;
                                            }
                                        };
                                        
                                        // 创建 state provider（用于 code_by_hash，代码不在日志文件中）
                                        // 注意：每个块的状态都不同，必须为每个块创建新的state_provider
                                        let state_provider = blockchain_db.history_by_block_number(block_number.checked_sub(1).unwrap_or(0))?;
                                        
                                        // 创建从日志文件读取的 Database
                                        let logged_db = LoggedDatabase::new(log_entries, state_provider);
                                        block_db_pairs.push((block_number, logged_db));
                                    }
                                    
                                    // 第二步：按顺序执行所有块（减少 executor 创建开销）
                                    // 性能优化：虽然仍需要为每个块创建 executor，但至少减少了查找和创建的开销
                                    for (block_number, logged_db) in block_db_pairs {
                                        // 在处理每个块之前检查停止标志
                                        if should_stop_worker.load(Ordering::Relaxed) {
                                            debug!(target: "exex::evm", thread_id = ?thread_id, "Worker thread stopping during block processing");
                                            return Ok(());
                                        }
                                        
                                        // 找到对应的块（使用 find，因为块号可能不连续）
                                        let block = blocks.iter().find(|b| b.sealed_block().header().number == block_number);
                                        if let Some(block) = block {
                                            // 创建 executor（这是必要的，因为每个块的状态不同）
                                            // 性能优化：executor 的创建开销相对较小，主要是执行开销
                                            let executor = evm_config.batch_executor(logged_db);
                                            
                                            // 执行单个块
                                            let _output = executor.execute(block)?;
                                            
                                            // 再次检查停止标志（执行可能耗时较长）
                                            if should_stop_worker.load(Ordering::Relaxed) {
                                                debug!(target: "exex::evm", thread_id = ?thread_id, "Worker thread stopping after block execution");
                                                return Ok(());
                                            }
                                        }
                                    }
                                } else {
                                    error!("--use-log on requires --log-dir to be specified");
                                    return Err(eyre::eyre!("--use-log on requires --log-dir"));
                                }
                            } else {
                                // 正常批量执行（不记录日志）
                                let db = StateProviderDatabase::new(blockchain_db.history_by_block_number(task.start - 1)?);
                                let executor = evm_config.batch_executor(db);
                                let _execute_result = executor.execute_batch(blocks.iter())?;
                            }

                            let start = Instant::now();
                            let mut step_cumulative_gas: u64 = 0;
                            let mut step_txs_counter: usize = 0;

                            // 只统计实际处理的块（不包括已存在的块，用于断点续传）
                            // 只在 --log-block on 模式下过滤已存在的块
                            blocks.iter()
                                .filter(|block| {
                                    let block_num = block.sealed_block().header().number;
                                    if let Some(_log_dir) = &log_dir_clone {
                                        if log_block_enabled {
                                            !existing_blocks_clone.contains(&block_num)
                                        } else {
                                            true // --use-log on 或其他模式，统计所有块
                                        }
                                    } else {
                                        true
                                    }
                                })
                                .for_each(|block| {
                                    td += block.sealed_block().header().difficulty;
                                    step_cumulative_gas += block.sealed_block().header().gas_used;
                                    step_txs_counter += block.sealed_block().body().transaction_count();
                                    debug!(target: "exex::evm", block_number = block.sealed_block().header().number, txs_count = block.sealed_block().body().transaction_count(), thread_id = ?thread_id, "Adding transactions count");
                                });

                            // Ensure the locks are correctly used without deadlock
                            {
                                *cumulative_gas.lock().unwrap() += step_cumulative_gas;
                            }
                            {
                                // 只统计实际处理的块数（不包括已存在的块，用于断点续传）
                                let processed_count = if let Some(_log_dir) = &log_dir_clone {
                                    blocks_to_process_numbers.len() as u64
                                } else {
                                    blocks.len() as u64
                                };
                                *block_counter.lock().unwrap() += processed_count;
                            }
                            {
                                *txs_counter.lock().unwrap() += step_txs_counter as u64;
                            }

                            debug!(
                                target: "exex::evm",
                                task_start = task.start,
                                task_end = task.end,
                                txs = step_txs_counter,
                                blocks = blocks_to_process_numbers.len(),
                                total_blocks_in_range = blocks.len(),
                                skipped_blocks = blocks.len() - blocks_to_process_numbers.len(),
                                throughput = format_gas_throughput(step_cumulative_gas, start.elapsed()),
                                time = ?start.elapsed(),
                                thread_id = ?thread_id,
                                total_difficulty = ?td,
                                "loop"
                            );

                            Ok(())
                        }));

                        match result {
                            Ok(res) => {
                                if let Err(e) = res {
                                    error!("Thread {:?} execution error: {:?}", thread_id, e);
                                }
                            }
                            Err(e) => {
                                error!("Thread {:?} execution error: {:?}", thread_id, e);
                            }
                        };
                    }
                }
                Ok(())
            }));
        }

        // 等待所有线程完成，或检查停止标志
        // 如果收到 Ctrl+C，刷新缓冲写入器
        if should_stop.load(Ordering::Relaxed) {
            if let Some(ref writer) = buffered_writer {
                if let Ok(mut writer_guard) = writer.lock() {
                    if let Err(e) = writer_guard.flush_all() {
                        warn!("Failed to flush buffered log writer on Ctrl+C: {}", e);
                    } else {
                        info!("Buffered log writer flushed on Ctrl+C");
                    }
                }
            }
            info!("Ctrl+C received, waiting for threads to finish current tasks...");
            // 给线程一些时间完成当前任务，但不要无限等待
            let timeout = Duration::from_secs(5);
            let start = Instant::now();
            
            for thread in threads {
                if start.elapsed() > timeout {
                    info!("Timeout waiting for threads, forcing exit...");
                    break;
                }
                // 使用 try_join 或设置超时
                // 由于 std::thread::JoinHandle 不支持超时，我们只能等待
                match thread.join() {
                    Ok(res) => {
                        if let Err(e) = res {
                            error!("Thread execution error: {:?}", e);
                        }
                    }
                    Err(e) => error!("Thread execution error: {:?}", e),
                };
            }
            info!("Exiting due to Ctrl+C");
            return Ok(());
        } else {
            // 正常等待所有线程完成
            for thread in threads {
                match thread.join() {
                    Ok(res) => {
                        if let Err(e) = res {
                            error!("Thread execution error: {:?}", e);
                        }
                    }
                    Err(e) => error!("Thread execution error: {:?}", e),
                };
            }
        }

        // 如果指定了 log_block 且不是 "on"，按正常访问顺序生成该块的 account/storage 日志（单个块模式）
        if let Some(log_block_str) = &self.log_block {
            if log_block_str != "on" {
                // 尝试解析为块号
                if let Ok(log_block) = log_block_str.parse::<u64>() {
                    // 如果使用累加模式，检查块是否已存在（断点续传）
                    if let Some(_log_dir) = log_dir {
                        if existing_blocks.contains(&log_block) {
                            info!("Block {} already exists in log files, skipping log recording", log_block);
                            return Ok(());
                        } else {
                            info!("Starting log recording for block {} (will be added to accumulated log files)", log_block);
                        }
                    } else {
                        info!("Starting log recording for block {}", log_block);
                    }
                    
                    // 为避免影响上面的并行执行，这里单独重新构建一次 provider 和 executor
                    let blockchain_db = BlockchainProvider::new(provider_factory.clone())?;
                    let state_provider = blockchain_db.history_by_block_number(
                        log_block.checked_sub(1).unwrap_or(0)
                    )?;
                    let inner_db = StateProviderDatabase::new(&state_provider);
                    
                    // 创建日志收集器
                    let read_logs = Arc::new(Mutex::new(Vec::<ReadLogEntry>::new()));
                    let logging_db = LoggingDatabase {
                        inner: inner_db,
                        read_logs: Arc::clone(&read_logs),
                    };
                    
                    let evm_config = EthEvmConfig::ethereum(provider_factory.chain_spec());
                    let executor = evm_config.batch_executor(logging_db);

                    let blocks = blockchain_db
                        .block_with_senders_range(log_block..=log_block)
                        .map_err(|e| eyre::eyre!("failed to load block {}: {}", log_block, e))?;

                    if let Some(block) = blocks.first() {
                        warn!("LoggingDatabase: executing block {} to record access order (txs: {})", 
                            log_block, block.body().transaction_count());
                        // 执行该块，LoggingDatabase 会在读取时自动记录日志
                        // 使用 execute 而不是 execute_batch，与其他代码保持一致
                        let _output = executor.execute(block)?;
                        
                        // 获取记录的日志（按正常访问顺序）
                        let logs = read_logs.lock().unwrap();
                        info!("Recorded {} log entries during execution", logs.len());
                        write_read_logs(log_block, &logs)?;
                        write_read_logs_binary(log_block, &logs, &self.compression_algorithm, log_dir, &existing_blocks, self.single_thread, None)?;
                    }
                } else {
                    return Err(eyre::eyre!("Invalid --log-block value: {}. Use 'on' to enable logging for all blocks in range, or a specific block number", log_block_str));
                }
            }
            // 如果 log_block == "on"，日志记录已经在并行执行线程中完成，这里不需要额外处理
        }

        // 如果指定了 use_log 且不是 "on"，使用日志文件中的数据执行单个块
        if let Some(log_path_or_block) = &self.use_log {
            if log_path_or_block != "on" {
                // 确定日志文件路径和块号
                let (log_path, block_number) = if let Ok(block_num) = log_path_or_block.parse::<u64>() {
                    // 如果传递的是数字，自动构建文件路径
                    let path = format!("block_{}_reads.bin", block_num);
                    (path, block_num)
                } else {
                    // 如果传递的是路径，尝试从文件名提取块号
                    let path = log_path_or_block.clone();
                    let block_num = self.log_block.as_ref().and_then(|s| s.parse::<u64>().ok()).unwrap_or_else(|| {
                        // 尝试从文件名提取块号
                        if let Some(name) = std::path::Path::new(&path).file_stem() {
                            if let Some(name_str) = name.to_str() {
                                if let Some(block_str) = name_str.strip_prefix("block_") {
                                    if let Some(block_num_str) = block_str.strip_suffix("_reads") {
                                        return block_num_str.parse().unwrap_or(0);
                                    }
                                }
                            }
                        }
                        0
                    });
                    if block_num == 0 {
                        return Err(eyre::eyre!("Cannot determine block number from log file path '{}' or --log-block. Please specify --log-block or use format 'block_XXXXX_reads.bin'", path));
                    }
                    (path, block_num)
                };
                
                // 读取日志文件（累加文件系统或单个文件）
                let log_entries = if let Some(log_dir) = log_dir {
                    // 从累加文件系统读取，使用块号作为 key（使用缓存的索引条目）
                    read_read_logs_binary(&block_number.to_string(), Some(log_dir), Some(&cached_index_entries))?
                } else {
                    // 从单个文件读取（不使用索引）
                    read_read_logs_binary(&log_path, None, None)?
                };
                info!("Loaded {} log entries from {}", log_entries.len(), &log_path);
                
                // 创建 state provider
                let blockchain_db = BlockchainProvider::new(provider_factory.clone())?;
                let state_provider = blockchain_db.history_by_block_number(
                    block_number.checked_sub(1).unwrap_or(0)
                )?;
                
                // 创建从日志文件读取的 Database
                let logged_db = LoggedDatabase::new(log_entries, state_provider);
                
                let evm_config = EthEvmConfig::ethereum(provider_factory.chain_spec());
                
                let blocks = blockchain_db
                    .block_with_senders_range(block_number..=block_number)
                    .map_err(|e| eyre::eyre!("failed to load block {}: {}", block_number, e))?;

                if let Some(block) = blocks.first() {
                    warn!("LoggedDatabase: executing block {} using log file {} (txs: {})", 
                        block_number, log_path, block.body().transaction_count());
                    // 执行该块，LoggedDatabase 会从日志文件读取数据
                    // 为每个块创建新的 executor，避免状态复用或缓存问题
                    let executor = evm_config.batch_executor(logged_db);
                    // 使用 execute 而不是 execute_batch，与记录日志时保持一致
                    let _output = executor.execute(block)?;
                    // 注意：check_unaccessed_storages 会在 LoggedDatabase 析构时自动调用
                    info!("Block {} executed successfully using log file", block_number);
                }
            }
            // 如果 use_log == "on"，日志执行已经在并行执行线程中完成，这里不需要额外处理
        }

        if should_stop.load(Ordering::Relaxed) {
            info!("EVM command stopped by user");
        } else {
            thread::sleep(Duration::from_secs(1));
            info!("EVM command completed successfully");
        }

        // 如果启用了性能分析，生成火焰图（跨平台支持）
        #[cfg(any(unix, windows))]
        if let Some(ref mut guard) = _profiler_guard {
            info!("Generating flamegraph...");
            let output_file = "flamegraph.svg";
            match guard.generate_flamegraph(output_file) {
                Ok(()) => {
                    info!("🔥 火焰图已生成: {}", output_file);
                    #[cfg(windows)]
                    info!("注意: Windows 版本使用简化的线程采样，可能不如 Unix 版本精确。建议使用 WSL 或 Windows Performance Toolkit 进行更详细的分析。");
                }
                Err(e) => {
                    warn!("Failed to generate flamegraph: {}", e);
                }
            }
        }

        Ok(())
    }

    // 获取系统 CPU 核心数
    fn get_cpu_count(&self) -> usize {
        num_cpus::get()
    }
}

impl<C: ChainSpecParser> EvmCommand<C> {
    /// Returns the underlying chain being used to run this command
    pub const fn chain_spec(&self) -> Option<&Arc<C::ChainSpec>> {
        Some(&self.env.chain)
    }
}

