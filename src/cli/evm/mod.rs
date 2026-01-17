// SPDX-License-Identifier: MIT OR Apache-2.0

//! `reth evm` command.

#[cfg(any(unix, windows))]
mod profiling;
mod state_log;
mod logged_db;
mod block_log_store;
mod batch_log_store;
mod compression;
mod types;
mod file_io;
mod database_wrappers;
mod log_reader;

use compression::{CompressionAlgorithm, decompress_data};
use types::{Task, ReadLogEntry, IndexEntry};
use file_io::{BufferedLogWriter, GlobalLogFileHandle, open_log_files, read_index_file, find_index_entry, compress_block_logs, write_read_logs_binary, read_log_compressed_data};
use database_wrappers::{LoggingDatabase, SimpleLogCollector, PassthroughDatabase, BatchLoggingDatabase, write_read_logs};
use log_reader::LoggedDatabase;

pub use state_log::{MmapStateLogDatabase, MmapStateLogReader};
pub use logged_db::{DbLoggedDatabase, BytecodeCache, CachedStateProviderDatabase, BatchDbLoggedDatabase, BatchOwnedLoggedDatabase};
pub use block_log_store::{BlockLogStore, BlockLogStoreWriter, SingleBlockDatabase, BatchBlockDatabase, convert_from_old_format};
pub use batch_log_store::{BatchLogStore, BatchLogStoreWriter, BatchDatabase, BatchLoggingDatabase as BatchLoggingDb};

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
    BlockNumReader,
    providers::BlockchainProvider, BlockReader, ChainSpecProvider, StateProviderFactory,
};
use reth_node_ethereum::{consensus::EthBeaconConsensus, EthEvmConfig};
use reth_evm::{execute::Executor, ConfigureEvm};
use reth_revm::database::StateProviderDatabase;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, atomic::{AtomicBool, AtomicU64, Ordering}};
use std::thread;
use std::thread::JoinHandle;
use eyre::{Report, Result};
// 已移除 AssertUnwindSafe，不再使用 catch_unwind（性能优化）
use reth_primitives_traits::BlockBody;
use tokio::signal;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::io::{Seek, SeekFrom};
#[cfg(unix)]
use libc;
use alloy_primitives::B256;

// 性能优化：预定义空 codehash 常量，避免重复创建
const EMPTY_CODE_HASH_BYTES: &[u8; 32] = &[
    0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c,
    0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
    0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b,
    0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
];

// 性能优化：预计算空 codehash
#[inline]
fn get_empty_code_hash() -> B256 {
    B256::from_slice(EMPTY_CODE_HASH_BYTES)
}

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
    /// Number of worker threads (default: CPU_COUNT * 2 - 1, use lower value on Windows to avoid mmap issues)
    #[arg(long, alias = "threads", short = 't')]
    thread_count: Option<usize>,
    /// Enable CPU profiling and generate flamegraph (output: flamegraph.svg)
    #[arg(long, alias = "profile")]
    enable_profiling: bool,
    /// Use memory-mapped file for state logs (recommended for large datasets 400GB+)
    /// When enabled, logs are stored in state_logs_mmap.bin with mmap access
    #[arg(long, alias = "mmap-log")]
    use_mmap_log: bool,
    /// Repair and compact log files: validate, sort by block number, remove duplicates
    /// and compact data. Stops if missing blocks are found.
    #[arg(long, alias = "repair-log")]
    repair_log: bool,
    /// Rebuild index from data file: scan data file, validate entries,
    /// rebuild clean index and report missing/corrupted blocks.
    #[arg(long, alias = "rebuild-idx")]
    rebuild_idx: bool,
    /// Convert mmap log format to new block-aware format (.blklog)
    /// The new format supports: 4096-byte alignment, on-demand loading, memory release
    #[arg(long, alias = "convert-log")]
    convert_log: bool,
    /// Use the new block-aware log format for execution (requires .blklog file)
    #[arg(long, alias = "use-blklog")]
    use_blklog: bool,
    /// Use batch-level log format for execution (requires .batchlog file)
    /// Both generation and replay use batch execution, ensuring State cache consistency
    /// Index by batch start block, greatly reducing index count (e.g., 2M blocks -> 20K batches)
    #[arg(long, alias = "use-batchlog")]
    use_batchlog: bool,
    /// Generate batch-level logs during execution (requires --log-dir)
    /// Uses batch execution with zstd compression, compatible with --use-batchlog replay
    #[arg(long, alias = "gen-batchlog")]
    gen_batchlog: bool,
    /// Disable database caching for direct execution (for performance testing)
    /// Uses plain StateProviderDatabase instead of CachedStateProviderDatabase
    #[arg(long)]
    no_db_cache: bool,
}

// NOTE: LoggingDatabase, BatchLoggingState, SimpleLogCollector, PassthroughDatabase,
// BatchLoggingDatabase, FastLoggingDatabase moved to database_wrappers.rs
// ThreadLocalLogCache, LoggedDatabase, read_read_logs_binary moved to log_reader.rs

// IndexEntry, BufferedLogWriter 已移至 file_io.rs 模块

// open_log_files, read_index_file, find_index_entry, compress_block_logs 已移至 file_io.rs 模块

// write_read_logs_binary 已移至 file_io.rs 模块


// GlobalLogFileHandle 已移至 file_io.rs 模块


// read_log_compressed_data 已移至 file_io.rs 模块

// read_read_logs_binary, ThreadLocalLogCache, LoggedDatabase 已移至 log_reader.rs 模块

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
                    #[cfg(all(unix, target_os = "linux"))]
                    info!("CPU profiler started (100Hz sampling rate, using pprof-rs)");
                    #[cfg(target_os = "macos")]
                    info!("CPU profiler started (100Hz sampling rate, using thread-based sampling)");
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
        
        // 如果启用了 --repair-log，执行修复并退出
        if self.repair_log {
            if let Some(log_dir) = log_dir {
                info!("Starting log repair mode...");
                match MmapStateLogDatabase::repair_log(log_dir, self.begin_number, self.end_number) {
                    Ok(result) => {
                        if result.repaired {
                            info!("Log repair completed successfully!");
                            return Ok(());
                        } else {
                            if let Some(msg) = &result.error_message {
                                error!("Log repair failed: {}", msg);
                            }
                            if !result.missing_blocks.is_empty() {
                                let first_missing = result.missing_blocks.first().unwrap();
                                let last_missing = result.missing_blocks.last().unwrap();
                                error!("Missing blocks: first={}, last={}, count={}", 
                                    first_missing, last_missing, result.missing_blocks.len());
                                error!("Please re-run with --begin {} to fill in missing blocks", first_missing);
                            }
                            return Err(eyre::eyre!("Log repair failed"));
                        }
                    }
                    Err(e) => {
                        error!("Log repair error: {}", e);
                        return Err(e);
                    }
                }
            } else {
                return Err(eyre::eyre!("--repair-log requires --log-dir to be set"));
            }
        }
        
        // 如果启用了 --rebuild-idx，从数据文件重建索引并退出
        if self.rebuild_idx {
            if let Some(log_dir) = log_dir {
                info!("Starting index rebuild mode...");
                match MmapStateLogDatabase::rebuild_index(log_dir, self.begin_number, self.end_number) {
                    Ok(result) => {
                        info!("Index rebuild completed!");
                        if !result.missing_blocks.is_empty() {
                            let first_missing = result.missing_blocks.first().unwrap();
                            let last_missing = result.missing_blocks.last().unwrap();
                            warn!("Missing/corrupted blocks: first={}, last={}, count={}", 
                                first_missing, last_missing, result.missing_blocks.len());
                            warn!("To regenerate, run: --log-block on --begin {} --end {}", 
                                first_missing, self.end_number);
                        }
                        return Ok(());
                    }
                    Err(e) => {
                        error!("Index rebuild error: {}", e);
                        return Err(e);
                    }
                }
            } else {
                return Err(eyre::eyre!("--rebuild-idx requires --log-dir to be set"));
            }
        }

        // 如果启用了 --convert-log，将日志转换为新的块感知格式并退出
        // 支持两种源格式：
        // 1. mmap 格式：state_logs_mmap.bin + state_logs_mmap.idx
        // 2. 文件系统格式：blocks_log.bin + blocks_log.idx
        if self.convert_log {
            if let Some(log_dir) = log_dir {
                info!("Starting log conversion to block-aware format...");

                // 检查源文件格式
                let mmap_data_path = log_dir.join("state_logs_mmap.bin");
                let fs_data_path = log_dir.join("blocks_log.bin");
                let fs_idx_path = log_dir.join("blocks_log.idx");

                // 创建输出文件
                let output_path = log_dir.join("blocks.blklog");

                // 使用压缩
                let compress = self.compression_algorithm != "none";

                if mmap_data_path.exists() {
                    // mmap 格式
                    info!("Detected mmap format logs");

                    let reader = MmapStateLogReader::open(log_dir)?;
                    if let Some((min, max)) = reader.get_block_range() {
                        info!("Source log contains {} blocks (range: {} - {})", reader.block_count(), min, max);
                    } else {
                        return Err(eyre::eyre!("Source log is empty"));
                    }

                    let start_block = if self.begin_number > 0 { self.begin_number } else { reader.get_block_range().unwrap().0 };
                    let end_block = if self.end_number > 0 { self.end_number } else { reader.get_block_range().unwrap().1 };

                    info!("Converting blocks {} - {} to {}", start_block, end_block, output_path.display());

                    let mut writer = block_log_store::BlockLogStoreWriter::new(&output_path, compress)?;
                    if compress {
                        info!("Compression enabled (zstd)");
                    }

                    let mut converted = 0u64;
                    let mut skipped = 0u64;
                    let start_time = Instant::now();

                    for block_number in start_block..=end_block {
                        if let Some(data) = reader.read_block_log(block_number) {
                            if data.len() < 8 {
                                warn!("Block {} has invalid data (too short)", block_number);
                                skipped += 1;
                                continue;
                            }
                            let entry_count = u64::from_le_bytes(data[0..8].try_into().unwrap());
                            writer.write_block(block_number, &data[8..], entry_count)?;
                            converted += 1;

                            if converted % 10000 == 0 {
                                info!("Converted {} blocks...", converted);
                            }
                        } else {
                            skipped += 1;
                        }
                    }

                    writer.finish()?;

                    let elapsed = start_time.elapsed();
                    info!("Conversion completed: {} blocks converted, {} skipped, time: {:.2}s",
                        converted, skipped, elapsed.as_secs_f64());

                } else if fs_data_path.exists() && fs_idx_path.exists() {
                    // 文件系统格式
                    info!("Detected file system format logs");

                    // 读取索引
                    let (mut idx_file, _) = open_log_files(log_dir)?;
                    let index_entries = read_index_file(&mut idx_file)?;

                    if index_entries.is_empty() {
                        return Err(eyre::eyre!("Source log index is empty"));
                    }

                    let min_block = index_entries.iter().map(|e| e.block_number).min().unwrap();
                    let max_block = index_entries.iter().map(|e| e.block_number).max().unwrap();
                    info!("Source log contains {} blocks (range: {} - {})", index_entries.len(), min_block, max_block);

                    let start_block = if self.begin_number > 0 { self.begin_number } else { min_block };
                    let end_block = if self.end_number > 0 { self.end_number } else { max_block };

                    info!("Converting blocks {} - {} to {}", start_block, end_block, output_path.display());

                    // 打开数据文件
                    let mut data_file = File::open(&fs_data_path)?;

                    let mut writer = block_log_store::BlockLogStoreWriter::new(&output_path, compress)?;
                    if compress {
                        info!("Compression enabled (zstd)");
                    }

                    let mut converted = 0u64;
                    let mut skipped = 0u64;
                    let start_time = Instant::now();

                    // 按块号过滤和排序索引条目
                    let mut filtered_entries: Vec<&IndexEntry> = index_entries.iter()
                        .filter(|e| e.block_number >= start_block && e.block_number <= end_block)
                        .collect();
                    filtered_entries.sort_by_key(|e| e.block_number);

                    for entry in filtered_entries {
                        // 读取压缩数据
                        data_file.seek(SeekFrom::Start(entry.offset))?;
                        let mut compressed_data = vec![0u8; entry.length as usize];
                        data_file.read_exact(&mut compressed_data)?;

                        // 检测压缩算法并解压
                        // 文件系统格式在数据开头存储压缩算法标识字节
                        if compressed_data.is_empty() {
                            warn!("Block {} has empty data", entry.block_number);
                            skipped += 1;
                            continue;
                        }
                        let algorithm = match CompressionAlgorithm::from_u8(compressed_data[0]) {
                            Ok(alg) => alg,
                            Err(_) => {
                                warn!("Block {} has invalid compression algorithm: {}", entry.block_number, compressed_data[0]);
                                skipped += 1;
                                continue;
                            }
                        };
                        let decompressed = decompress_data(&compressed_data[1..], algorithm)?;

                        // 解析条目数量
                        if decompressed.len() < 8 {
                            warn!("Block {} has invalid data (too short)", entry.block_number);
                            skipped += 1;
                            continue;
                        }
                        let entry_count = u64::from_le_bytes(decompressed[0..8].try_into().unwrap());

                        // 写入（跳过 count 头）
                        writer.write_block(entry.block_number, &decompressed[8..], entry_count)?;
                        converted += 1;

                        if converted % 10000 == 0 {
                            info!("Converted {} blocks...", converted);
                        }
                    }

                    writer.finish()?;

                    let elapsed = start_time.elapsed();
                    info!("Conversion completed: {} blocks converted, {} skipped, time: {:.2}s",
                        converted, skipped, elapsed.as_secs_f64());

                } else {
                    return Err(eyre::eyre!(
                        "No valid source logs found. Expected either:\n  - mmap format: {}\n  - file system format: {} and {}",
                        mmap_data_path.display(),
                        fs_data_path.display(),
                        fs_idx_path.display()
                    ));
                }

                info!("Output file: {}", output_path.display());

                // 验证输出文件
                let store = block_log_store::BlockLogStore::open(&output_path)?;
                info!("Verification: {} blocks in output file, range: {} - {}",
                    store.header().block_count,
                    store.header().first_block_number,
                    store.header().last_block_number);

                return Ok(());
            } else {
                return Err(eyre::eyre!("--convert-log requires --log-dir to be set"));
            }
        }

        // 测试模式：启用内存模式（将 bin 文件完全读入内存，无压缩）
        // 可以通过环境变量 PEVM_IN_MEMORY_MODE=true 启用
        let enable_in_memory_mode = std::env::var("PEVM_IN_MEMORY_MODE")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(true); // 默认启用测试模式
        
        // 检查是否启用日志记录模式（用于决定是否跳过已存在的块）
        let log_block_enabled = self.log_block.as_ref().map(|s| s == "on").unwrap_or(false);
        
        // 检查是否启用从日志执行模式
        let use_log_enabled = self.use_log.as_ref().map(|s| s == "on").unwrap_or(false);
        
        // 如果启用了 mmap 日志模式（--mmap-log），初始化数据库
        // - 写入模式（--log-block on）：使用 MmapStateLogDatabase + RwLock
        // - 读取模式（--use-log on）：使用 MmapStateLogReader（无锁，性能更好）
        let mmap_log_db: Option<Arc<std::sync::RwLock<MmapStateLogDatabase>>> = if self.use_mmap_log && log_block_enabled {
            if let Some(log_dir) = log_dir {
                match MmapStateLogDatabase::open_for_write(log_dir) {
                    Ok(db) => {
                        if let Some((min, max)) = db.get_block_range() {
                            info!("Using mmap-based state log storage (WRITE mode): {} blocks, range {} - {}", 
                                db.block_count(), min, max);
                        } else {
                            info!("Using mmap-based state log storage (WRITE mode): empty database");
                        }
                        Some(Arc::new(std::sync::RwLock::new(db)))
                    }
                    Err(e) => {
                        warn!("Failed to open MmapStateLogDatabase: {}. Falling back to other modes.", e);
                        None
                    }
                }
            } else {
                warn!("--mmap-log requires --log-dir to be set.");
                None
            }
        } else {
            None
        };
        
        // 无锁只读访问器（用于 --use-log on 模式，性能更好）
        let mmap_log_reader: Option<Arc<MmapStateLogReader>> = if self.use_mmap_log && use_log_enabled && !log_block_enabled {
            if let Some(log_dir) = log_dir {
                match MmapStateLogReader::open(log_dir) {
                    Ok(reader) => {
                        if let Some((min, max)) = reader.get_block_range() {
                            info!("Using mmap-based state log storage (READ mode, lock-free): {} blocks, range {} - {}", 
                                reader.block_count(), min, max);
                        } else {
                            info!("Using mmap-based state log storage (READ mode, lock-free): empty database");
                        }
                        Some(Arc::new(reader))
                    }
                    Err(e) => {
                        warn!("Failed to open MmapStateLogReader: {}. Falling back to locked mode.", e);
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        // 新架构：块感知日志存储（用于 --use-blklog 模式）
        // 优势：4096字节对齐、按需加载、内存释放、多线程无锁读取
        let blklog_store: Option<Arc<BlockLogStore>> = if self.use_blklog {
            if let Some(log_dir) = log_dir {
                let blklog_path = log_dir.join("blocks.blklog");
                if blklog_path.exists() {
                    match BlockLogStore::open(&blklog_path) {
                        Ok(store) => {
                            info!("Using block-aware log storage: {} blocks (range: {} - {})",
                                store.header().block_count,
                                store.header().first_block_number,
                                store.header().last_block_number);
                            if store.header().is_compressed() {
                                info!("  Compression: zstd enabled");
                            }
                            Some(Arc::new(store))
                        }
                        Err(e) => {
                            error!("Failed to open block-aware log store: {}", e);
                            None
                        }
                    }
                } else {
                    error!("Block-aware log file not found: {}. Run --convert-log first.", blklog_path.display());
                    None
                }
            } else {
                error!("--use-blklog requires --log-dir to be set");
                None
            }
        } else {
            None
        };

        // 批次级日志存储（用于 --use-batchlog 模式）
        // 优势：批次级索引（大幅减少索引数）、zstd压缩、生成和回放都使用批量执行（State缓存一致）
        let batchlog_store: Option<Arc<batch_log_store::BatchLogStore>> = if self.use_batchlog {
            if let Some(log_dir) = log_dir {
                let batchlog_path = log_dir.join("blocks.batchlog");
                // 检查 .idx 文件是否存在（实际文件是 .dat 和 .idx）
                let batchlog_idx_path = log_dir.join("blocks.batchlog.idx");
                if batchlog_idx_path.exists() {
                    match batch_log_store::BatchLogStore::open(&batchlog_path) {
                        Ok(store) => {
                            info!("Using batch-level log storage: {} batches (batch_size={}, range: {} - {})",
                                store.header().batch_count,
                                store.header().batch_size,
                                store.header().first_block_number,
                                store.header().last_block_number);
                            Some(Arc::new(store))
                        }
                        Err(e) => {
                            error!("Failed to open batch-level log store: {}", e);
                            None
                        }
                    }
                } else {
                    error!("Batch-level log file not found: {}. Run --gen-batchlog first.", batchlog_idx_path.display());
                    None
                }
            } else {
                error!("--use-batchlog requires --log-dir to be set");
                None
            }
        } else {
            None
        };

        // 批次日志写入器（用于 --gen-batchlog 模式）
        let batchlog_writer: Option<Arc<Mutex<batch_log_store::BatchLogStoreWriter>>> = if self.gen_batchlog {
            if let Some(log_dir) = log_dir {
                let batchlog_path = log_dir.join("blocks.batchlog");
                match batch_log_store::BatchLogStoreWriter::new(&batchlog_path, self.step_size as u64) {
                    Ok(writer) => {
                        info!("Creating batch-level log file: {} (batch_size={})", batchlog_path.display(), self.step_size);
                        Some(Arc::new(Mutex::new(writer)))
                    }
                    Err(e) => {
                        error!("Failed to create batch-level log writer: {}", e);
                        None
                    }
                }
            } else {
                error!("--gen-batchlog requires --log-dir to be set");
                None
            }
        } else {
            None
        };

        // 检查已存在的块（用于补齐模式：只生成不存在或损坏的块）
        // mmap 模式：使用 block_exists() 精确检查（包括损坏检测）
        // 文件系统模式：使用 HashSet 检查
        let (existing_blocks, cached_index_entries): (Arc<std::collections::HashSet<u64>>, Arc<Vec<IndexEntry>>) = 
            if let Some(ref mmap_db) = mmap_log_db {
                // mmap 模式：显示范围信息（实际检查使用 block_exists()）
                let db_guard = mmap_db.read().unwrap();
                let range = db_guard.get_block_range();
                let block_count = db_guard.block_count();
                drop(db_guard);
                
                if let Some((min_block, max_block)) = range {
                    info!("Found mmap log: {} blocks indexed (range: {} - {}), will fill in missing/corrupted blocks", 
                        block_count, min_block, max_block);
                } else {
                    info!("Mmap log is empty, will generate all blocks from --begin");
                }
                // mmap 模式不需要 HashSet，使用 block_exists() 精确检查
                (Arc::new(std::collections::HashSet::new()), Arc::new(Vec::new()))
            } else if let Some(log_dir) = log_dir {
                // 文件系统模式：从索引文件获取已存在的块
                let (mut idx_file, _) = open_log_files(log_dir)?;
                let index_entries = read_index_file(&mut idx_file)?;
                // 性能优化：预分配 HashSet 容量，减少重新分配
                let blocks: std::collections::HashSet<u64> = index_entries.iter()
                    .map(|e| e.block_number)
                    .collect();
                if !blocks.is_empty() {
                    let min_block = *blocks.iter().min().unwrap();
                    let max_block = *blocks.iter().max().unwrap();
                    info!("Found log files: {} ({} blocks indexed, range: {} - {}), will fill in missing blocks", 
                        log_dir.display(), blocks.len(), min_block, max_block);
                } else {
                    info!("Log directory is empty: {}, will generate all blocks from --begin", 
                        log_dir.display());
                }
                // 文件系统模式使用完整的 HashSet（块可能不连续）
                (Arc::new(blocks), Arc::new(index_entries))
            } else {
                (Arc::new(std::collections::HashSet::new()), Arc::new(Vec::new()))
            };
        
        // 创建全局缓冲写入器（仅在文件系统模式下使用，mmap 模式不需要）
        let buffered_writer: Option<Arc<Mutex<BufferedLogWriter>>> = if mmap_log_db.is_none() {
            if let Some(log_dir) = log_dir {
                if log_block_enabled {
                    // 创建缓冲写入器，缓冲区大小为 1MB
                    Some(Arc::new(Mutex::new(BufferedLogWriter::new(log_dir)?)))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None // mmap 模式不使用 buffered_writer
        };
        
        // 提前获取 chain_spec，避免在线程中重复调用（可能有锁）
        let chain_spec = provider_factory.chain_spec();
        let _consensus: Arc<dyn FullConsensus<EthPrimitives>> =
            Arc::new(EthBeaconConsensus::new(chain_spec.clone()));

        // 在 v1.8.4 中，共享 blockchain_db 也能正常工作
        let blockchain_db = BlockchainProvider::new(provider_factory.clone())?;
        
        // 性能修复：限制 provider 的生命周期，避免长时间持有读事务
        // 只用来获取 last_block_number，获取后立即释放
        let last = {
            let provider = provider_factory.provider()?;
            provider.last_block_number()?
        }; // provider 在这里被 drop，释放读事务

        if self.begin_number > self.end_number {
            eyre::bail!("the begin block number is higher than the end block number")
        }
        if self.end_number > last {
            eyre::bail!("The end block number is higher than the latest block number")
        }

        // 步骤4: 创建任务池
        // 取消续传逻辑：从 --begin 开始创建所有任务
        // 块级别的过滤在 worker 线程中进行（只处理不存在的块，补齐模式）
        
        let mut tasks = VecDeque::new();
        let mut current_start = self.begin_number;
        while current_start <= self.end_number {
            // 取消续传逻辑：从 --begin 开始创建所有任务，不跳过已存在的块
            // 实际的块级别过滤在 worker 线程中进行（只处理不存在的块，补齐模式）
            
            let mut current_end = std::cmp::min(current_start + self.step_size as u64 - 1, self.end_number);
            if current_end == self.end_number - 1 {
                current_end += 1;
            }
            
            tasks.push_back(Task {
                start: current_start,
                end: current_end,
            });
            current_start = current_end + 1;
        }

        // 性能优化：在程序开始时打开日志文件句柄，所有线程共享，程序结束时关闭
        // 如果启用了 --use-log on 且使用了日志目录，创建全局文件句柄
        let use_log_enabled = self.use_log.as_ref().map(|s| s == "on").unwrap_or(false);
        let global_log_file_handle: Option<Arc<GlobalLogFileHandle>> = if use_log_enabled {
            if let Some(log_dir) = log_dir {
                let bin_path = log_dir.join("blocks_log.bin");
                match GlobalLogFileHandle::new(&bin_path, enable_in_memory_mode) {
                    Ok(handle) => {
                        if enable_in_memory_mode {
                            info!("Successfully loaded log file into memory: {} (will be shared across all threads, no compression)", bin_path.display());
                        } else {
                            info!("Successfully opened global log file handle: {} (will be shared across all threads)", bin_path.display());
                        }
                        Some(Arc::new(handle))
                    }
                    Err(e) => {
                        warn!("Failed to open global log file handle ({}), will open file on each read: {}", bin_path.display(), e);
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        // 获取线程数：
        // 1. 如果启用单线程模式，使用 1 个线程
        // 2. 如果指定了 --threads，使用指定的值
        // 3. 否则使用默认值：
        //    - Windows: CPU_COUNT（减少 mmap 压力，避免 STATUS_IN_PAGE_ERROR）
        //    - 其他系统: CPU_COUNT * 2 - 1
        let thread_count = if self.single_thread {
            1
        } else if let Some(tc) = self.thread_count {
            std::cmp::max(1, tc) // 至少 1 个线程
        } else {
            #[cfg(windows)]
            {
                // Windows 修复：使用较少的线程以减少 MDBX mmap 压力
                // 过多线程会导致 STATUS_IN_PAGE_ERROR
                let cpu_count = self.get_cpu_count();
                std::cmp::max(1, cpu_count)
            }
            #[cfg(not(windows))]
            {
                self.get_cpu_count() * 2 - 1
            }
        };
        #[cfg(windows)]
        info!("Using {} worker threads (Windows: reduced to avoid mmap issues)", thread_count);
        #[cfg(not(windows))]
        info!("Using {} worker threads", thread_count);
        let mut threads: Vec<JoinHandle<Result<()>>> = Vec::with_capacity(thread_count);

        // 创建共享计数器（使用原子操作避免锁竞争，提高多线程性能）
        let task_queue = Arc::new(Mutex::new(tasks));
        let cumulative_gas = Arc::new(AtomicU64::new(0));
        let block_counter = Arc::new(AtomicU64::new(self.begin_number - 1));
        let txs_counter = Arc::new(AtomicU64::new(0));
        let should_stop = Arc::new(AtomicBool::new(false));
        
        // 创建全局 Bytecode 缓存（合约代码不可变，可安全跨线程共享）
        // 这是最重要的性能优化之一，避免重复查询数据库获取相同的合约代码
        let bytecode_cache = Arc::new(BytecodeCache::new());

        // 设置 Ctrl+C 信号处理
        let should_stop_clone = Arc::clone(&should_stop);
        let ctrl_c_handle = tokio::spawn(async move {
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

                    // 使用原子操作读取计数器（无锁，高性能）
                    let current_cumulative_gas = cumulative_gas.load(Ordering::Relaxed);
                    let diff_gas = current_cumulative_gas - previous_cumulative_gas;
                    previous_cumulative_gas = current_cumulative_gas;

                    let current_block_counter = block_counter.load(Ordering::Relaxed);
                    let diff_block = current_block_counter - previous_block_counter;
                    previous_block_counter = current_block_counter;

                    let current_txs_counter = txs_counter.load(Ordering::Relaxed);
                    let diff_txs = current_txs_counter - previous_txs_counter;
                    previous_txs_counter = current_txs_counter;

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

                    // 检查是否已经处理完所有块
                    // current_block_counter 存储的是实际块号，所以要与 end_number 比较
                    if current_block_counter >= end_number {
                        info!("All blocks processed, status thread exiting");
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
            // 传递已存在块的 HashSet（用于文件系统模式的块存在检查）
            let existing_blocks_clone = Arc::clone(&existing_blocks);
            let cached_index_entries_clone = Arc::clone(&cached_index_entries);
            let buffered_writer_clone = buffered_writer.clone();
            let global_log_file_handle_clone = global_log_file_handle.clone();
            // 将内存模式标志传递给 worker 线程
            let enable_in_memory_mode_clone = enable_in_memory_mode;
            // mmap 日志模式（写入用）
            let mmap_log_db_clone = mmap_log_db.clone();
            // mmap 无锁只读访问器（读取用，性能更好）
            let mmap_log_reader_clone = mmap_log_reader.clone();
            // Bytecode 缓存（合约代码不可变，跨线程共享）
            let bytecode_cache_clone = bytecode_cache.clone();
            // 新架构：块感知日志存储
            let blklog_store_clone = blklog_store.clone();
            // 批次级日志存储（回放用）
            let batchlog_store_clone = batchlog_store.clone();
            // 批次级日志写入器（生成用）
            let batchlog_writer_clone = batchlog_writer.clone();
            let gen_batchlog = self.gen_batchlog;
            let no_db_cache = self.no_db_cache;
            let step_size = self.step_size;

            // 在 v1.8.4 中，共享 blockchain_db 也能正常工作
            threads.push(thread::spawn(move || {
                let thread_id = thread::current().id();
                
                // 预先创建 EVM 配置，避免在循环中重复创建（这是线程级别的，可以复用）
                let evm_config = EthEvmConfig::ethereum(chain_spec.clone());
                
                // 性能关键优化：在 worker 线程开始时只调用一次 history_by_block_number
                // 而不是在每个 task 中调用（避免大量数据库查询）
                // code_by_hash 查询的 bytecode 是不可变的，不依赖于具体块号
                // 所以使用 begin_number - 1 作为基准块号是安全的
                // 注意：需要定期刷新以避免 MDBX 读事务超时（300秒）
                let mut thread_level_state_provider: Option<Arc<dyn reth_provider::StateProvider>> = 
                    if use_log_enabled {
                        match blockchain_db.history_by_block_number(0) {
                            Ok(sp) => Some(Arc::new(sp)),
                            Err(e) => {
                                error!("Failed to create thread-level state provider: {}", e);
                                None
                            }
                        }
                    } else {
                        None
                    };
                
                // 用于跟踪 state_provider 的创建时间，避免 MDBX 读事务超时
                let mut state_provider_created_at = std::time::Instant::now();
                // Windows 修复：使用更短的刷新间隔来避免 STATUS_IN_PAGE_ERROR
                // MDBX 的 mmap 在 Windows 上长时间运行容易出问题
                #[cfg(windows)]
                const STATE_PROVIDER_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
                #[cfg(not(windows))]
                const STATE_PROVIDER_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(240);
                
                // Windows 修复：每处理 N 个 task 后强制刷新，避免 mmap 积累
                #[cfg(windows)]
                let mut task_counter: u32 = 0;
                #[cfg(windows)]
                const TASKS_BEFORE_FORCE_REFRESH: u32 = 50; // Windows 上每 50 个 task 强制刷新
                
                loop {
                    // 检查停止标志
                    if should_stop_worker.load(Ordering::Relaxed) {
                        debug!(target: "exex::evm", thread_id = ?thread_id, "Worker thread stopping due to Ctrl+C");
                        break;
                    }

                    // 性能优化：当 step_size 很小时（如 step=1），内部合并连续任务
                    // 这避免了每块都调用 history_by_block_number 的开销
                    // 内部批量大小：至少 100 块，以获得批量执行的性能优势
                    const INTERNAL_BATCH_SIZE: usize = 100;

                    let merged_task = {
                        let mut queue = task_queue.lock().unwrap();
                        if queue.is_empty() {
                            break;
                        }

                        // 获取第一个任务
                        let first_task = queue.pop_front().unwrap();

                        // 如果 step_size >= INTERNAL_BATCH_SIZE，不需要合并
                        if step_size >= INTERNAL_BATCH_SIZE {
                            first_task
                        } else {
                            // 尝试合并连续的任务，直到达到 INTERNAL_BATCH_SIZE 块
                            let mut merged_end = first_task.end;
                            let target_blocks = INTERNAL_BATCH_SIZE;
                            let current_blocks = (first_task.end - first_task.start + 1) as usize;
                            let mut blocks_collected = current_blocks;

                            while blocks_collected < target_blocks {
                                if let Some(next_task) = queue.front() {
                                    // 检查是否连续（下一个任务的 start 应该等于当前 merged_end + 1）
                                    if next_task.start == merged_end + 1 {
                                        let next_task = queue.pop_front().unwrap();
                                        merged_end = next_task.end;
                                        blocks_collected += (next_task.end - next_task.start + 1) as usize;
                                    } else {
                                        // 不连续，停止合并
                                        break;
                                    }
                                } else {
                                    // 队列空了
                                    break;
                                }
                            }

                            Task {
                                start: first_task.start,
                                end: merged_end,
                            }
                        }
                    };

                    let task = merged_task;
                    {
                        // 再次检查停止标志，避免开始执行新任务
                        if should_stop_worker.load(Ordering::Relaxed) {
                            debug!(target: "exex::evm", thread_id = ?thread_id, "Worker thread stopping before task execution");
                            break;
                        }

                        // 性能优化：移除热路径上的 debug 日志，避免 tracing 开销
                        // 性能优化：移除 catch_unwind，直接执行（如果发生 panic，让线程崩溃而不是捕获）
                        // catch_unwind 的开销很大，特别是在没有 panic 的情况下也会产生 unwinding 开销
                        // 如果确实需要错误恢复，可以在更高层处理
                        let result: Result<(), Report> = (|| -> Result<(), Report> {
                            // Note: header_td_by_number removed in v1.10.0 (total difficulty not needed in PoS)
                            // let mut td = blockchain_db.header_td_by_number(task.start - 1)?
                            //     .ok_or_else(|| ProviderError::HeaderNotFound(task.start.into()))?;

                            // 使用共享的 blockchain_db（如 v1.8.4 的方式）
                            // 预先创建的 evm_config 避免重复创建
                            let blocks = blockchain_db.block_with_senders_range(task.start..=task.end).unwrap();

                            // 步骤4: 如果使用累加模式且启用了日志记录（--log-block on），过滤掉已存在的块（断点续传）
                            // 精确检查：使用 mmap_log_db 的 block_exists() 方法
                            // 这样可以正确处理不连续的块（有缺失块的情况）
                            // 块存在检查函数（用于补齐模式：跳过已存在且有效的块）
                            // mmap 模式：使用 block_exists() 精确检查（包括损坏检测）
                            // 文件系统模式：使用 HashSet 检查
                            let worker_block_exists = |block_num: u64| -> bool {
                                if let Some(ref db) = mmap_log_db_clone {
                                    // mmap 模式：精确检查块是否存在且数据有效（包括损坏检测）
                                    let db_guard = db.read().unwrap();
                                    db_guard.block_exists(block_num)
                                } else {
                                    // 文件系统模式：使用 HashSet 检查
                                    existing_blocks_clone.contains(&block_num)
                                }
                            };
                            
                            // 先收集需要处理的块号，用于统计
                            let blocks_to_process_numbers: Vec<u64> = if let Some(_log_dir) = &log_dir_clone {
                                if log_block_enabled {
                                    // 只在 --log-block on 模式下过滤已存在的块
                                    blocks.iter()
                                        .filter(|block| {
                                            let block_num = block.sealed_block().header().number;
                                            !worker_block_exists(block_num)
                                        })
                                        .map(|block| block.sealed_block().header().number)
                                        .collect::<Vec<u64>>()
                                } else {
                                    // --use-log on 或其他模式，不过滤，处理所有块
                                    // 性能优化：预分配 Vec 容量
                                    blocks.iter()
                                        .map(|block| block.sealed_block().header().number)
                                        .collect::<Vec<u64>>()
                                }
                            } else {
                                // 性能优化：预分配 Vec 容量
                                blocks.iter()
                                    .map(|block| block.sealed_block().header().number)
                                    .collect::<Vec<u64>>()
                            };
                            
                            // 如果使用累加模式且启用了日志记录（--log-block on）且所有块都已存在，跳过执行
                            if let Some(_log_dir) = &log_dir_clone {
                                if log_block_enabled && blocks_to_process_numbers.is_empty() {
                                    return Ok(());
                                }
                            }

                            // 批次级日志生成模式（--gen-batchlog）
                            // 优势：批量执行（State缓存一致）、批次级压缩、索引数大幅减少
                            if gen_batchlog {
                                if let Some(ref writer) = batchlog_writer_clone {
                                    // 日志数据（在数据库资源释放后使用）
                                    let log_data: Vec<u8>;
                                    let entry_count: u64;
                                    let batch_start_block: u64;
                                    let batch_end_block: u64;

                                    // 作用域：执行和收集日志（完成后立即释放数据库资源）
                                    {
                                        // 创建批次级别的 state provider
                                        let batch_state_provider = blockchain_db.history_by_block_number(
                                            task.start.checked_sub(1).unwrap_or(0)
                                        )?;

                                        // 创建 BatchLoggingDatabase（记录整个批次的日志）
                                        let inner_db = StateProviderDatabase::new(&batch_state_provider);
                                        let mut logging_db = batch_log_store::BatchLoggingDatabase::new(inner_db);

                                        // 批量执行整个批次（使用同一个 executor，State 缓存生效）
                                        // 这与回放时的行为一致，确保日志数据正确对齐
                                        {
                                            let executor = evm_config.batch_executor(&mut logging_db);
                                            if let Err(e) = executor.execute_batch(blocks.iter()) {
                                                error!("Batch execution error for blocks {}-{}: {}", task.start, task.end, e);
                                            }
                                        } // executor 在这里立即释放，避免执行结束后仍持有数据库引用

                                        // 提取日志数据
                                        (log_data, entry_count) = logging_db.take_logs();

                                        // 确定批次范围
                                        batch_start_block = blocks.first()
                                            .map(|b| b.sealed_block().header().number)
                                            .unwrap_or(task.start);
                                        batch_end_block = blocks.last()
                                            .map(|b| b.sealed_block().header().number)
                                            .unwrap_or(task.end);
                                    } // batch_state_provider 和 logging_db 在这里被释放，减少 MDBX 读操作

                                    // 写入批次日志（数据库资源已释放）
                                    {
                                        let mut writer_guard = writer.lock().unwrap();
                                        if let Err(e) = writer_guard.write_batch(
                                            batch_start_block,
                                            batch_end_block,
                                            &log_data,
                                            entry_count,
                                        ) {
                                            error!("Failed to write batch log for blocks {}-{}: {}", batch_start_block, batch_end_block, e);
                                        }
                                    }

                                    // 统计
                                    for block in blocks.iter() {
                                        let txs = block.sealed_block().body().transaction_count();
                                        cumulative_gas.fetch_add(block.sealed_block().header().gas_used, Ordering::Relaxed);
                                        txs_counter.fetch_add(txs as u64, Ordering::Relaxed);
                                    }

                                    // 更新 block_counter
                                    block_counter.fetch_add(blocks.len() as u64, Ordering::Relaxed);
                                } else {
                                    error!("--gen-batchlog enabled but writer not available");
                                }
                            }
                            // 如果启用了日志记录（--log-block on），为每个块单独执行并记录日志
                            else if log_block_enabled {
                                {
                                    // 正常日志记录模式
                                    // Windows性能优化：复用同一个 executor，避免每块创建 executor 的开销

                                    // 创建批次级别的 state provider（只调用一次 history_by_block_number）
                                    let batch_state_provider = blockchain_db.history_by_block_number(
                                        task.start.checked_sub(1).unwrap_or(0)
                                    )?;

                                    // 日志记录模式：每个块独立执行，确保日志完整
                                    // 注意：不能使用 batch executor，因为它的缓存会导致后续块的数据库读取不被记录
                                    //
                                    // 原理：batch executor 内部维护状态缓存，当块 A 修改了账户 X 的 nonce，
                                    // 块 B 读取账户 X 时会命中缓存，不会调用我们的 BatchLoggingDatabase，
                                    // 导致块 B 的日志缺少账户 X 的数据。重放时就会出现 nonce 错误。

                                    let mut block_logs: Vec<(u64, Vec<ReadLogEntry>)> = Vec::with_capacity(blocks.len());

                                    for block in blocks.iter() {
                                        if should_stop_worker.load(Ordering::Relaxed) {
                                            break;
                                        }

                                        let block_number = block.sealed_block().header().number;

                                        // 检查块是否已存在
                                        if worker_block_exists(block_number) {
                                            continue;
                                        }

                                        // 为每个块创建独立的 state provider 和 executor
                                        // 这确保每个块的所有数据库读取都被记录
                                        let block_state_provider = blockchain_db.history_by_block_number(
                                            block_number.saturating_sub(1)
                                        )?;

                                        // 创建日志收集器（每个块独立）
                                        let collector = std::rc::Rc::new(SimpleLogCollector::new());
                                        collector.set_current_block(block_number);

                                        // 创建带日志的数据库
                                        let inner_db = StateProviderDatabase::new(&block_state_provider);
                                        let logging_db = BatchLoggingDatabase::new(inner_db, std::rc::Rc::clone(&collector));

                                        // 执行块
                                        {
                                            let executor = evm_config.batch_executor(logging_db);
                                            let _output = executor.execute(block)?;
                                        } // executor 立即释放

                                        // 提取日志
                                        let logs = collector.take_logs();
                                        for (bn, entries) in logs {
                                            block_logs.push((bn, entries));
                                        }

                                        // 释放 state_provider
                                        drop(block_state_provider);
                                    }

                                    // 释放批次级别的 state_provider（不再使用）
                                    drop(batch_state_provider);

                                    // 批量写入日志（减少锁竞争和文件I/O）
                                    // 优先使用 mmap 模式，其次使用文件系统模式
                                    if let Some(ref db) = mmap_log_db_clone {
                                        // mmap 日志模式：批量写入（零拷贝，适合大数据集）
                                        let mut db_guard = db.write().unwrap();
                                        // 直接传递引用，避免不必要的 clone
                                        db_guard.write_block_logs_batch(&block_logs)?;
                                    } else if let Some(ref writer) = buffered_writer_clone {
                                        // 文件系统模式：使用缓冲写入
                                        // Windows优化：先在锁外压缩所有数据，然后只持锁做快速内存拷贝

                                        // 步骤1：锁外压缩（CPU密集，不需要锁）
                                        let mut compressed_blocks: Vec<(u64, Vec<u8>)> = Vec::with_capacity(block_logs.len());
                                        for (block_number, logs) in block_logs {
                                            let compressed = compress_block_logs(block_number, &logs, &compression_algorithm)?;
                                            compressed_blocks.push((block_number, compressed));
                                        }

                                        // 步骤2：持锁写入（只做快速内存拷贝）
                                        {
                                            let mut writer_guard = writer.lock().unwrap();
                                            for (block_number, compressed_data) in compressed_blocks {
                                                writer_guard.write_compressed_block(block_number, &compressed_data)?;
                                            }
                                            // 每10000个块flush一次，而不是每批次都flush
                                            writer_guard.maybe_flush()?;
                                        }
                                    } else {
                                        for (block_number, logs) in block_logs {
                                            write_read_logs_binary(block_number, &logs, &compression_algorithm, log_dir_clone.as_deref(), &existing_blocks_clone, single_thread, None)?;
                                        }
                                    }
                                }
                            } else if use_log_enabled {
                                // 如果启用了从日志执行（--use-log on），为每个块从累计日志中读取并执行

                                // 最优先使用批次级日志存储（--use-batchlog）
                                // 优势：批次级索引（大幅减少索引数）、zstd压缩、批量执行State缓存一致
                                if let Some(ref store) = batchlog_store_clone {
                                    // 创建共享的 StateProvider（用于 code_by_hash 回退查询）
                                    let shared_state_provider: Arc<dyn reth_provider::StateProvider> =
                                        Arc::new(blockchain_db.history_by_block_number(task.start.checked_sub(1).unwrap_or(0))?);

                                    // 计算批次边界
                                    let batch_size = store.batch_size() as usize;
                                    let first_block = blocks.first().map(|b| b.sealed_block().header().number).unwrap_or(task.start);
                                    let last_block = blocks.last().map(|b| b.sealed_block().header().number).unwrap_or(task.end);

                                    // 按批次处理（一个 task 可能跨多个批次）
                                    let batch_start = store.batch_start_for_block(first_block);
                                    let mut current_batch_start = batch_start;

                                    while current_batch_start <= last_block {
                                        // 获取当前批次的块
                                        let batch_end = current_batch_start + batch_size as u64 - 1;
                                        let batch_blocks: Vec<_> = blocks.iter()
                                            .filter(|b| {
                                                let bn = b.sealed_block().header().number;
                                                bn >= current_batch_start && bn <= batch_end
                                            })
                                            .collect();

                                        if batch_blocks.is_empty() {
                                            current_batch_start += batch_size as u64;
                                            continue;
                                        }

                                        // 从存储加载批次数据并创建 BatchDatabase
                                        match batch_log_store::BatchDatabase::from_store(
                                            store,
                                            current_batch_start,
                                            shared_state_provider.clone(),
                                            Some(bytecode_cache_clone.clone()),
                                        ) {
                                            Ok(mut batch_db) => {
                                                // 批量执行整个批次（使用同一个 executor，State 缓存行为与生成时一致）
                                                {
                                                    let executor = evm_config.batch_executor(&mut batch_db);
                                                    if let Err(e) = executor.execute_batch(batch_blocks.iter().copied()) {
                                                        error!("Batch execution error for blocks {}-{}: {}",
                                                            current_batch_start, batch_end.min(last_block), e);
                                                    }
                                                } // executor 立即释放，避免执行结束后仍持有数据库引用

                                                // 统计
                                                for block in batch_blocks.iter() {
                                                    let txs = block.sealed_block().body().transaction_count();
                                                    cumulative_gas.fetch_add(block.sealed_block().header().gas_used, Ordering::Relaxed);
                                                    txs_counter.fetch_add(txs as u64, Ordering::Relaxed);
                                                }
                                            }
                                            Err(e) => {
                                                // 批次在日志中不存在，回退到数据库逐块执行
                                                debug!("Batch {} not found in batchlog, falling back to database: {}", current_batch_start, e);
                                                for block in batch_blocks.iter() {
                                                    let block_number = block.sealed_block().header().number;
                                                    let db = StateProviderDatabase::new(
                                                        blockchain_db.history_by_block_number(block_number.saturating_sub(1))?
                                                    );
                                                    let executor = evm_config.batch_executor(db);
                                                    if let Err(e) = executor.execute_batch(std::iter::once(*block)) {
                                                        error!("Execution error for block {} using StateProviderDatabase: {}", block_number, e);
                                                    }

                                                    let txs = block.sealed_block().body().transaction_count();
                                                    cumulative_gas.fetch_add(block.sealed_block().header().gas_used, Ordering::Relaxed);
                                                    txs_counter.fetch_add(txs as u64, Ordering::Relaxed);
                                                }
                                            }
                                        }

                                        // 释放批次数据
                                        store.release_batch(current_batch_start);
                                        current_batch_start += batch_size as u64;
                                    }

                                    // 更新 block_counter
                                    block_counter.fetch_add(blocks.len() as u64, Ordering::Relaxed);

                                // 其次使用块感知日志存储（--use-blklog）
                                // 优势：4096字节对齐、按需加载、内存释放、多线程无锁读取
                                } else if let Some(ref store) = blklog_store_clone {
                                    // 创建共享的 StateProvider（用于 code_by_hash 回退查询）
                                    let shared_state_provider: Arc<dyn reth_provider::StateProvider> =
                                        Arc::new(blockchain_db.history_by_block_number(task.start.checked_sub(1).unwrap_or(0))?);

                                    // 逐块执行：每个块创建新的 executor（State 缓存为空）
                                    // 原因：日志是按逐块独立执行方式记录的，State 在每块开始时缓存为空
                                    //       如果批量执行，State 会跨块缓存，导致某些 basic/storage 调用被跳过，日志读取错位
                                    // 优化：store 共享，块数据可缓存复用，避免重复 IO
                                    for block in blocks.iter() {
                                        let block_number = block.sealed_block().header().number;

                                        // 为每个块创建独立的数据库实例
                                        match block_log_store::SingleBlockDatabase::from_store(
                                            store,
                                            block_number,
                                            shared_state_provider.clone(),
                                            Some(bytecode_cache_clone.clone()),
                                        ) {
                                            Ok(mut single_block_db) => {
                                                // 每个块创建新的 executor（State 缓存为空，所有读取都会调用 database）
                                                {
                                                    let executor = evm_config.batch_executor(&mut single_block_db);
                                                    if let Err(e) = executor.execute_batch(std::iter::once(block)) {
                                                        error!("Execution error for block {} using SingleBlockDatabase: {}", block_number, e);
                                                    }
                                                } // executor 立即释放

                                                let txs = block.sealed_block().body().transaction_count();
                                                cumulative_gas.fetch_add(block.sealed_block().header().gas_used, Ordering::Relaxed);
                                                txs_counter.fetch_add(txs as u64, Ordering::Relaxed);
                                            }
                                            Err(e) => {
                                                // 块在日志中不存在，回退到数据库执行
                                                debug!("Block {} not found in blklog, falling back to database: {}", block_number, e);
                                                let db = StateProviderDatabase::new(
                                                    blockchain_db.history_by_block_number(block_number.saturating_sub(1))?
                                                );
                                                {
                                                    let executor = evm_config.batch_executor(db);
                                                    if let Err(e) = executor.execute_batch(std::iter::once(block)) {
                                                        error!("Execution error for block {} using StateProviderDatabase: {}", block_number, e);
                                                    }
                                                } // executor 立即释放

                                                let txs = block.sealed_block().body().transaction_count();
                                                cumulative_gas.fetch_add(block.sealed_block().header().gas_used, Ordering::Relaxed);
                                                txs_counter.fetch_add(txs as u64, Ordering::Relaxed);
                                            }
                                        }

                                        // 可选：执行完一个块后释放其数据（减少内存）
                                        // store.release_block(block_number);
                                    }

                                    // 更新 block_counter
                                    block_counter.fetch_add(blocks.len() as u64, Ordering::Relaxed);

                                // 其次使用无锁 mmap reader（零拷贝，无锁，性能较好）
                                } else if let Some(ref reader) = mmap_log_reader_clone {
                                    let blocks_len = blocks.len();

                                    // 获取所有需要的块号
                                    let block_numbers: Vec<u64> = blocks.iter()
                                        .map(|b| b.sealed_block().header().number)
                                        .collect();

                                    // 从 mmap 读取（零拷贝，无锁！）
                                    let batch_data = reader.read_block_logs_batch(&block_numbers);

                                    // 创建共享的 StateProvider
                                    let shared_state_provider: Arc<dyn reth_provider::StateProvider> =
                                        Arc::new(blockchain_db.history_by_block_number(task.start.checked_sub(1).unwrap_or(0))?);

                                    // 为每个块创建 DbLoggedDatabase 并逐块执行
                                    // 注意：日志数据是按逐块独立执行方式记录的，所以回放时也必须逐块执行
                                    let mut block_db_pairs: Vec<(u64, Option<DbLoggedDatabase<'_>>)> = Vec::with_capacity(blocks_len);

                                    for (bn, data_opt) in batch_data.iter() {
                                        if let Some(data) = data_opt {
                                            match DbLoggedDatabase::new_with_cache(
                                                data,
                                                shared_state_provider.clone(),
                                                bytecode_cache_clone.clone(),
                                            ) {
                                                Ok(db) => block_db_pairs.push((*bn, Some(db))),
                                                Err(e) => {
                                                    warn!("Failed to create DbLoggedDatabase for block {}: {}", bn, e);
                                                    block_db_pairs.push((*bn, None));
                                                }
                                            }
                                        } else {
                                            block_db_pairs.push((*bn, None));
                                        }
                                    }

                                    // 逐块执行
                                    let mut db_iter = block_db_pairs.into_iter();
                                    for block in blocks.iter() {
                                        let block_number = block.sealed_block().header().number;

                                        if let Some((db_block_number, db_opt)) = db_iter.next() {
                                            if db_block_number != block_number {
                                                error!("Block number mismatch: expected {}, got {}", block_number, db_block_number);
                                                continue;
                                            }

                                            if let Some(mut logged_db) = db_opt {
                                                {
                                                    let executor = evm_config.batch_executor(&mut logged_db);
                                                    if let Err(e) = executor.execute_batch(std::iter::once(block)) {
                                                        error!("Execution error for block {} using mmap DbLoggedDatabase: {}", block_number, e);
                                                    }
                                                } // executor 立即释放
                                            } else {
                                                let db = StateProviderDatabase::new(
                                                    blockchain_db.history_by_block_number(block_number.saturating_sub(1))?
                                                );
                                                {
                                                    let executor = evm_config.batch_executor(db);
                                                    if let Err(e) = executor.execute_batch(std::iter::once(block)) {
                                                        error!("Execution error for block {} using StateProviderDatabase: {}", block_number, e);
                                                    }
                                                } // executor 立即释放
                                            }

                                            let txs = block.sealed_block().body().transaction_count();
                                            cumulative_gas.fetch_add(block.sealed_block().header().gas_used, Ordering::Relaxed);
                                            txs_counter.fetch_add(txs as u64, Ordering::Relaxed);
                                        }
                                    }

                                    // 更新 block_counter（使用原子操作，无锁）
                                    block_counter.fetch_add(blocks.len() as u64, Ordering::Relaxed);

                                } else if let Some(log_dir) = &log_dir_clone {
                                    // 文件系统模式：从文件读取日志数据
                                    // 注意：StateProvider 不能跨块复用，因为每个块的状态不同
                                    // 但我们可以批量预加载日志数据，减少文件I/O
                                    
                                    // 性能优化：批量预加载所有块的压缩数据到内存（线程局部）
                                    // 每个块的数据从文件读取，然后在线程内部解压并缓存
                                    use std::collections::HashMap;
                                    // 性能优化：预分配 HashMap 容量，避免重新分配
                                    let blocks_len = blocks.len();
                                    let mut block_compressed_data: HashMap<u64, Vec<u8>> = HashMap::with_capacity(blocks_len);
                                    
                                    // 性能优化：预分配 Vec 容量，减少重新分配
                                    let mut read_requests: Vec<(&IndexEntry, u64)> = Vec::with_capacity(blocks_len);
                                    for block in blocks.iter() {
                                        let block_number = block.sealed_block().header().number;
                                        
                                        // 使用缓存的索引条目查找块
                                        if let Some(entry) = find_index_entry(&cached_index_entries_clone, block_number) {
                                            read_requests.push((entry, block_number));
                                        } else {
                                            warn!("Block {} not found in log index, skipping", block_number);
                                        }
                                    }
                                    
                                    // 按偏移量排序，优化文件访问模式（顺序读取）
                                    read_requests.sort_by_key(|(entry, _)| entry.offset);
                                    
                                    // 性能优化：使用批量读取减少 seek 和 read 系统调用
                                    // 如果使用全局文件句柄，使用批量读取；否则逐个读取
                                    if let Some(file_handle) = global_log_file_handle_clone.as_deref() {
                                        // 性能优化：在 in_memory_mode 下，直接使用共享内存，避免复制
                                        if enable_in_memory_mode_clone && file_handle.has_in_memory_data() {
                                            // 直接从共享内存读取，避免复制
                                            let shared_data = file_handle.get_in_memory_data().unwrap();
                                            for (entry, block_number) in read_requests {
                                                let start = entry.offset as usize;
                                                let end = start + entry.length as usize;
                                                if end > shared_data.len() {
                                                    error!("Block {} read range out of bounds: offset={}, length={}, file_size={}", block_number, entry.offset, entry.length, shared_data.len());
                                                    continue;
                                                }
                                                // 性能优化：直接使用共享内存的 Arc，避免复制
                                                let data = shared_data[start..end].to_vec();
                                                block_compressed_data.insert(block_number, data);
                                            }
                                        } else {
                                            // 正常模式：使用批量读取
                                            // 性能优化：预分配批量读取请求 Vec 容量
                                            let batch_ranges: Vec<(usize, u64, u64)> = read_requests.iter()
                                                .enumerate()
                                                .map(|(idx, (entry, _))| (idx, entry.offset, entry.length))
                                                .collect();
                                            
                                            // 批量读取所有数据
                                            match file_handle.read_ranges_batch(&batch_ranges) {
                                                Ok(batch_data) => {
                                                    // 将批量读取的数据插入到 HashMap 中
                                                    for (idx, data) in batch_data {
                                                        let (_, block_number) = read_requests[idx];
                                                        block_compressed_data.insert(block_number, data);
                                                    }
                                                }
                                                Err(e) => {
                                                    error!("Failed to batch read log data: {}. Falling back to individual reads.", e);
                                                    // 回退到逐个读取
                                                    for (_entry, block_number) in read_requests {
                                                        match read_log_compressed_data(
                                                            &block_number.to_string(), 
                                                            Some(log_dir), 
                                                            Some(&cached_index_entries_clone),
                                                            Some(file_handle),
                                                            enable_in_memory_mode_clone
                                                        ) {
                                                            Ok(b) => {
                                                                block_compressed_data.insert(block_number, b);
                                                            }
                                                            Err(err) => {
                                                                error!("Failed to read log for block {}: {}", block_number, err);
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        // 没有全局文件句柄，逐个读取
                                        for (_entry, block_number) in read_requests {
                                            match read_log_compressed_data(
                                                &block_number.to_string(), 
                                                Some(log_dir), 
                                                Some(&cached_index_entries_clone),
                                                None,
                                                enable_in_memory_mode_clone
                                            ) {
                                                Ok(b) => {
                                                    block_compressed_data.insert(block_number, b);
                                                }
                                                Err(e) => {
                                                    error!("Failed to read log for block {}: {}", block_number, e);
                                                }
                                            }
                                        }
                                    }
                                    
                                    // 性能关键优化：使用线程级别的 state provider（在 worker 线程开始时只创建一次）
                                    // 而不是每个 task 都调用 history_by_block_number（避免大量数据库查询）
                                    // code_by_hash 查询的 bytecode 是不可变的，不依赖于具体块号
                                    let shared_state_provider = match &thread_level_state_provider {
                                        Some(sp) => sp.clone(),
                                        None => {
                                            // 回退：如果线程级别 state provider 创建失败，尝试为当前 task 创建
                                            Arc::new(blockchain_db.history_by_block_number(task.start.checked_sub(1).unwrap_or(0))?)
                                        }
                                    };

                                    // 注意：日志数据是按逐块独立执行方式记录的，所以回放时也必须逐块执行
                                    // 批量执行会导致 executor 内部状态累积，与日志数据不兼容

                                    // 准备处理的块数据
                                    let is_uncompressed = enable_in_memory_mode_clone;
                                    let mut block_db_pairs: Vec<(u64, LoggedDatabase)> = Vec::with_capacity(blocks_len);

                                    let mut block_decompress_tasks: Vec<(u64, Vec<u8>, usize)> = blocks.iter()
                                        .enumerate()
                                        .filter_map(|(block_idx, block)| {
                                            let block_number = block.sealed_block().header().number;
                                            block_compressed_data.remove(&block_number).map(|data| (block_number, data, block_idx))
                                        })
                                        .collect();

                                    // 按块号排序以保持顺序
                                    block_decompress_tasks.sort_by_key(|(_, _, idx)| *idx);

                                    // 串行创建所有 LoggedDatabase
                                    for (block_number, compressed_data, _) in block_decompress_tasks {
                                        match LoggedDatabase::new_from_compressed_buffer_owned(
                                            compressed_data.clone(),
                                            shared_state_provider.clone(),
                                            is_uncompressed
                                        ) {
                                            Ok(db) => block_db_pairs.push((block_number, db)),
                                            Err(e) => {
                                                // 尝试用相反的格式标志重试
                                                debug!("Failed with is_uncompressed={}, retrying with opposite flag for block {}: {}", is_uncompressed, block_number, e);
                                                match LoggedDatabase::new_from_compressed_buffer_owned(
                                                    compressed_data,
                                                    shared_state_provider.clone(),
                                                    !is_uncompressed
                                                ) {
                                                    Ok(db) => block_db_pairs.push((block_number, db)),
                                                    Err(e2) => {
                                                        error!("Failed to create LoggedDatabase for block {} with both format flags: {} / {}", block_number, e, e2);
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    // 逐块执行（每块使用独立的 executor）
                                    let mut block_db_map: std::collections::HashMap<u64, LoggedDatabase> =
                                        block_db_pairs.into_iter().collect();

                                    for block in blocks.iter() {
                                        if should_stop_worker.load(Ordering::Relaxed) {
                                            return Ok(());
                                        }

                                        let block_number = block.sealed_block().header().number;
                                        let mut executed_successfully = false;

                                        if let Some(logged_db) = block_db_map.remove(&block_number) {
                                            let result = {
                                                let executor = evm_config.batch_executor(logged_db);
                                                executor.execute(block)
                                            }; // executor 立即释放
                                            match result {
                                                Ok(_output) => {
                                                    executed_successfully = true;
                                                }
                                                Err(e) => {
                                                    warn!("LoggedDatabase execution failed for block {}, falling back to database: {}", block_number, e);
                                                }
                                            }
                                        }

                                        if !executed_successfully {
                                            match (|| -> eyre::Result<()> {
                                                let state_provider = blockchain_db.history_by_block_number(block_number.saturating_sub(1))?;
                                                let db = CachedStateProviderDatabase::new(
                                                    &state_provider,
                                                    Some(bytecode_cache_clone.clone()),
                                                );
                                                let result = {
                                                    let executor = evm_config.batch_executor(db);
                                                    executor.execute(block)
                                                }; // executor 立即释放
                                                drop(state_provider);
                                                result?;
                                                Ok(())
                                            })() {
                                                Ok(_) => {},
                                                Err(e) => {
                                                    error!("Failed to execute block {} with database fallback: {}. Skipping this block.", block_number, e);
                                                }
                                            }
                                        }
                                    }
                                    // 文件系统模式：统计更新
                                    // 更新 block_counter 和 txs_counter
                                    {
                                        let mut step_gas: u64 = 0;
                                        let mut step_txs: u64 = 0;
                                        for block in blocks.iter() {
                                            step_gas += block.sealed_block().header().gas_used;
                                            step_txs += block.sealed_block().body().transaction_count() as u64;
                                        }
                                        // 使用原子操作更新统计（无锁，高性能）
                                        cumulative_gas.fetch_add(step_gas, Ordering::Relaxed);
                                        txs_counter.fetch_add(step_txs, Ordering::Relaxed);
                                        block_counter.fetch_add(blocks.len() as u64, Ordering::Relaxed);
                                    }
                                } else {
                                    error!("--use-log on requires --log-dir to be specified");
                                    return Err(eyre::eyre!("--use-log on requires --log-dir"));
                                }
                            } else {
                                // 正常批量执行（不记录日志）
                                let state_provider = blockchain_db.history_by_block_number(task.start - 1)?;

                                if no_db_cache {
                                    // 性能测试模式：使用 PassthroughDatabase 包装器
                                    // 目的：测试类型包装对性能的影响
                                    // - gen-batchlog 使用 BatchLoggingDatabase 包装
                                    // - direct 使用 PassthroughDatabase 包装（相同结构，无日志开销）
                                    let inner_db = StateProviderDatabase::new(&state_provider);
                                    let mut db = PassthroughDatabase::new(inner_db);
                                    {
                                        let executor = evm_config.batch_executor(&mut db);
                                        let _execute_result = executor.execute_batch(blocks.iter())?;
                                    }
                                } else {
                                    // 默认使用 CachedStateProviderDatabase
                                    // 修复：与 gen-batchlog 一致的调用模式（使用 &mut）
                                    let mut db = CachedStateProviderDatabase::new(
                                        &state_provider,
                                        Some(bytecode_cache_clone.clone()),
                                    );
                                    {
                                        let executor = evm_config.batch_executor(&mut db);
                                        let _execute_result = executor.execute_batch(blocks.iter())?;
                                    }
                                }
                                // 显式释放 state_provider，减少 MDBX mmap 压力
                                drop(state_provider);
                            }

                            // 通用统计代码（仅用于非 use_log 且非 gen_batchlog 模式）
                            // use_log 和 gen_batchlog 模式的统计已在各自分支中完成
                            if !use_log_enabled && !gen_batchlog {
                                let _start = Instant::now(); // 保留用于将来的性能统计
                                let mut step_cumulative_gas: u64 = 0;
                                let mut step_txs_counter: usize = 0;

                                // 只统计实际处理的块（不包括已存在的块，用于补齐模式）
                                // 使用 blocks_to_process_numbers 作为过滤依据，保持一致性
                                // （避免因为其他线程 flush 导致 worker_block_exists() 返回不同结果）
                                if let Some(_log_dir) = &log_dir_clone {
                                    if log_block_enabled {
                                        // --log-block on 模式：只统计需要处理的块
                                        let blocks_to_process_set: std::collections::HashSet<u64> = 
                                            blocks_to_process_numbers.iter().cloned().collect();
                                        blocks.iter()
                                            .filter(|block| {
                                                let block_num = block.sealed_block().header().number;
                                                blocks_to_process_set.contains(&block_num)
                                            })
                                            .for_each(|block| {
                                                // td += block.sealed_block().header().difficulty; // PoS: total difficulty not used
                                                step_cumulative_gas += block.sealed_block().header().gas_used;
                                                step_txs_counter += block.sealed_block().body().transaction_count();
                                            });
                                    } else {
                                        // 其他模式，统计所有块
                                        blocks.iter().for_each(|block| {
                                            // td += block.sealed_block().header().difficulty; // PoS: total difficulty not used
                                            step_cumulative_gas += block.sealed_block().header().gas_used;
                                            step_txs_counter += block.sealed_block().body().transaction_count();
                                        });
                                    }
                                } else {
                                    // 没有 log_dir，统计所有块
                                    blocks.iter().for_each(|block| {
                                        // td += block.sealed_block().header().difficulty; // PoS: total difficulty not used
                                        step_cumulative_gas += block.sealed_block().header().gas_used;
                                        step_txs_counter += block.sealed_block().body().transaction_count();
                                    });
                                }

                                // 使用原子操作更新统计（无锁，高性能）
                                cumulative_gas.fetch_add(step_cumulative_gas, Ordering::Relaxed);
                                
                                // 只统计实际处理的块数（不包括已存在的块，用于补齐模式）
                                let processed_count = if let Some(_log_dir) = &log_dir_clone {
                                    blocks_to_process_numbers.len() as u64
                                } else {
                                    blocks.len() as u64
                                };
                                block_counter.fetch_add(processed_count, Ordering::Relaxed);
                                
                                txs_counter.fetch_add(step_txs_counter as u64, Ordering::Relaxed);
                            }

                            Ok(())
                        })();

                        // 直接处理结果（不再需要处理 panic）
                        if let Err(e) = result {
                            error!("Thread {:?} execution error: {:?}", thread_id, e);
                        }
                        
                        // 定期刷新 state_provider，避免 MDBX 读事务超时和 mmap 资源积累
                        // Windows 上长时间运行的 mmap 映射可能会导致 STATUS_IN_PAGE_ERROR
                        // 所有模式都需要定期刷新，不仅仅是 use_log 模式
                        
                        // Windows 修复：基于 task 计数的强制刷新
                        #[cfg(windows)]
                        {
                            task_counter += 1;
                            if task_counter >= TASKS_BEFORE_FORCE_REFRESH {
                                task_counter = 0;
                                state_provider_created_at = std::time::Instant::now() - STATE_PROVIDER_REFRESH_INTERVAL;
                            }
                        }
                        
                        if state_provider_created_at.elapsed() > STATE_PROVIDER_REFRESH_INTERVAL {
                            if use_log_enabled {
                                // use_log 模式：刷新线程级别的 state_provider
                                thread_level_state_provider = match blockchain_db.history_by_block_number(0) {
                                    Ok(sp) => Some(Arc::new(sp)),
                                    Err(e) => {
                                        error!("Failed to refresh thread-level state provider: {}", e);
                                        None
                                    }
                                };
                            }
                            // 重置计时器（所有模式）
                            state_provider_created_at = std::time::Instant::now();
                            
                            // Windows 修复：强制释放内存，帮助系统回收 mmap 资源
                            #[cfg(windows)]
                            {
                                // 提示 Rust 运行时进行垃圾收集
                                // 虽然 Rust 没有 GC，但这会帮助释放已 drop 的资源
                                std::hint::spin_loop();
                            }
                        }
                    }
                }

                // 显式释放资源，避免线程退出时产生大量 MDBX cleanup reads
                // 先释放 state_provider（依赖 blockchain_db）
                drop(thread_level_state_provider);
                // 再释放 blockchain_db 的引用
                drop(blockchain_db);
                debug!(target: "exex::evm", thread_id = ?thread_id, "Worker thread resources released");

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
            // 刷新 mmap 日志数据库
            if let Some(ref mmap_db) = mmap_log_db {
                if let Ok(mut db_guard) = mmap_db.write() {
                    if let Err(e) = db_guard.flush() {
                        warn!("Failed to flush mmap log database on Ctrl+C: {}", e);
                    } else {
                        info!("Mmap log database flushed on Ctrl+C ({} blocks pending)", 0);
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

            // Ctrl+C 时也要完成批次日志写入（写入索引和头部）
            if let Some(writer) = batchlog_writer {
                match Arc::try_unwrap(writer) {
                    Ok(mutex) => {
                        match mutex.into_inner() {
                            Ok(w) => {
                                if let Err(e) = w.finish() {
                                    error!("Failed to finish batch log writer on Ctrl+C: {}", e);
                                } else {
                                    info!("Batch log writer finished successfully on Ctrl+C");
                                }
                            }
                            Err(e) => {
                                error!("Failed to get batch log writer from mutex on Ctrl+C: {:?}", e);
                            }
                        }
                    }
                    Err(arc) => {
                        if let Ok(mut writer_guard) = arc.lock() {
                            if let Err(e) = writer_guard.finish_ref() {
                                error!("Failed to finish batch log writer via lock on Ctrl+C: {}", e);
                            } else {
                                info!("Batch log writer finished successfully on Ctrl+C (via lock)");
                            }
                        }
                    }
                }
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
            
            // 正常结束时刷新缓冲写入器（重要！否则最后的索引条目不会写入磁盘）
            if let Some(ref writer) = buffered_writer {
                if let Ok(mut writer_guard) = writer.lock() {
                    if let Err(e) = writer_guard.flush_all() {
                        error!("Failed to flush buffered log writer: {}", e);
                    } else {
                        info!("Buffered log writer flushed successfully");
                    }
                }
            }

            // 正常结束时完成批次日志写入（写入索引和头部）
            if let Some(writer) = batchlog_writer {
                // 尝试获取所有权，如果失败则通过锁完成写入
                match Arc::try_unwrap(writer) {
                    Ok(mutex) => {
                        match mutex.into_inner() {
                            Ok(w) => {
                                if let Err(e) = w.finish() {
                                    error!("Failed to finish batch log writer: {}", e);
                                } else {
                                    info!("Batch log writer finished successfully");
                                }
                            }
                            Err(e) => {
                                error!("Failed to get batch log writer from mutex: {:?}", e);
                            }
                        }
                    }
                    Err(arc) => {
                        // Arc 仍有其他引用，通过锁完成写入
                        warn!("Arc has {} references, using lock to finish", Arc::strong_count(&arc));
                        if let Ok(mut writer_guard) = arc.lock() {
                            if let Err(e) = writer_guard.finish_ref() {
                                error!("Failed to finish batch log writer via lock: {}", e);
                            } else {
                                info!("Batch log writer finished successfully (via lock)");
                            }
                        } else {
                            error!("Failed to lock batch log writer for finishing");
                        }
                    }
                }
            }

            // 正常结束时刷新 mmap 日志数据库
            if let Some(ref mmap_db) = mmap_log_db {
                if let Ok(mut db_guard) = mmap_db.write() {
                    let pending_count = db_guard.pending_count();
                    if pending_count > 0 {
                        info!("Flushing {} pending blocks to mmap log database...", pending_count);
                        if let Err(e) = db_guard.flush() {
                            error!("Failed to flush mmap log database: {}", e);
                        } else {
                            info!("Mmap log database flushed successfully");
                        }
                    }
                }
            }
        }

        // 如果指定了 log_block 且不是 "on"，按正常访问顺序生成该块的 account/storage 日志（单个块模式）
        if let Some(log_block_str) = &self.log_block {
            if log_block_str != "on" {
                // 尝试解析为块号
                if let Ok(log_block) = log_block_str.parse::<u64>() {
                    // 检查块是否已存在且数据有效（用于补齐模式）
                    // mmap 模式：使用 block_exists() 精确检查（包括损坏检测）
                    // 文件系统模式：使用 HashSet 检查
                    let single_block_exists = if let Some(ref mmap_db) = mmap_log_db {
                        let db_guard = mmap_db.read().unwrap();
                        db_guard.block_exists(log_block)
                    } else {
                        existing_blocks.contains(&log_block)
                    };
                    
                    if let Some(_log_dir) = log_dir {
                        if single_block_exists {
                            info!("Block {} already exists in log files (valid data), skipping log recording", log_block);
                            return Ok(());
                        } else {
                            info!("Starting log recording for block {} (will fill in missing/corrupted block)", log_block);
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

                    let blocks = blockchain_db
                        .block_with_senders_range(log_block..=log_block)
                        .map_err(|e| eyre::eyre!("failed to load block {}: {}", log_block, e))?;

                    if let Some(block) = blocks.first() {
                        warn!("LoggingDatabase: executing block {} to record access order (txs: {})",
                            log_block, block.body().transaction_count());
                        // 执行该块，LoggingDatabase 会在读取时自动记录日志
                        // 使用 execute 而不是 execute_batch，与其他代码保持一致
                        {
                            let executor = evm_config.batch_executor(logging_db);
                            let _output = executor.execute(block)?;
                        } // executor 立即释放

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
                
                // 性能优化：读取压缩数据，在线程内解压到线程局部缓存
                // 注意：单个块执行不在线程中，不使用全局文件句柄
                // 测试模式标志（单块执行也支持）
                let enable_in_memory_mode = std::env::var("PEVM_IN_MEMORY_MODE")
                    .map(|v| v == "true" || v == "1")
                    .unwrap_or(true); // 默认启用测试模式
                
                let compressed_data = if let Some(log_dir_ref) = log_dir {
                    // 从累加文件系统读取压缩数据（每次打开文件）
                    read_log_compressed_data(&block_number.to_string(), Some(log_dir_ref), Some(&cached_index_entries), None, enable_in_memory_mode)?
                } else {
                    // 从单个文件读取压缩数据（不使用索引）
                    read_log_compressed_data(&log_path, None, None, None, enable_in_memory_mode)?
                };
                
                // 创建 state provider
                let blockchain_db = BlockchainProvider::new(provider_factory.clone())?;
                let state_provider = blockchain_db.history_by_block_number(
                    block_number.checked_sub(1).unwrap_or(0)
                )?;

                // 从压缩数据创建 LoggedDatabase（性能优化：零复制版本）
                // 直接传递 Vec<u8> 所有权，避免不必要的内存复制
                // v1.10.0修复：自适应数据格式处理（先尝试正确格式以保持性能，失败时回退）
                let state_provider_arc = Arc::new(state_provider);
                let logged_db = match LoggedDatabase::new_from_compressed_buffer_owned(compressed_data.clone(), state_provider_arc.clone(), enable_in_memory_mode) {
                    Ok(db) => db,
                    Err(e) => {
                        debug!("Failed with is_uncompressed={}, retrying with opposite flag: {}", enable_in_memory_mode, e);
                        LoggedDatabase::new_from_compressed_buffer_owned(compressed_data, state_provider_arc, !enable_in_memory_mode)
                            .map_err(|e2| eyre::eyre!("Failed to create LoggedDatabase with both format flags: {} / {}", e, e2))?
                    }
                };
                
                info!("Loaded log data from {} (using thread-local cache)", &log_path);
                
                let evm_config = EthEvmConfig::ethereum(provider_factory.chain_spec());
                
                let blocks = blockchain_db
                    .block_with_senders_range(block_number..=block_number)
                    .map_err(|e| eyre::eyre!("failed to load block {}: {}", block_number, e))?;

                if let Some(block) = blocks.first() {
                    warn!("LoggedDatabase: executing block {} using log file {} (txs: {})",
                        block_number, log_path, block.body().transaction_count());
                    // 执行该块，LoggedDatabase 会从日志文件读取数据
                    // 为每个块创建新的 executor，避免状态复用或缓存问题
                    {
                        let executor = evm_config.batch_executor(logged_db);
                        // 使用 execute 而不是 execute_batch，与记录日志时保持一致
                        let _output = executor.execute(block)?;
                    } // executor 立即释放
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
                    // 获取完整的绝对路径以便用户知道文件位置
                    let abs_path = std::env::current_dir()
                        .ok()
                        .map(|dir| dir.join(output_file))
                        .and_then(|p| p.canonicalize().ok())
                        .unwrap_or_else(|| {
                            std::env::current_dir()
                                .ok()
                                .map(|dir| dir.join(output_file))
                                .unwrap_or_else(|| PathBuf::from(output_file))
                        });
                    info!("🔥 火焰图已生成: {}", abs_path.display());
                    #[cfg(windows)]
                    info!("注意: Windows 版本使用简化的线程采样，可能不如 Unix 版本精确。建议使用 WSL 或 Windows Performance Toolkit 进行更详细的分析。");
                }
                Err(e) => {
                    warn!("Failed to generate flamegraph: {}", e);
                }
            }
        }

        // 中止 Ctrl+C 信号处理任务，确保程序能正常退出
        ctrl_c_handle.abort();

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

