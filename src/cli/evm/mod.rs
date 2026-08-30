// SPDX-License-Identifier: MIT OR Apache-2.0

//! `reth evm` command.
//!
//! Note: We use `Arc<Box<dyn StateProvider + Send>>` which triggers clippy::arc_with_non_send_sync
//! because the trait object lacks `Sync`. However, these Arc instances are actually used within
//! single-threaded contexts (each worker thread has its own copy), so this is safe.
#![allow(clippy::arc_with_non_send_sync)]

mod logged_db;
#[cfg(any(unix, windows))]
mod profiling;
mod codes_freezer;
mod plain_state;
mod recsplit;
mod geth_freezer;
mod heat;
#[cfg(feature = "jit")]
mod aot;
mod state_log;
mod state_override;
mod trace;
mod witness;

pub use logged_db::{
    BytecodeCache, CachedStateProviderDatabase, DbLoggedDatabase, LocalStatsAccumulator,
    LogExecutionStats,
};
pub use state_log::{MmapStateLogDatabase, MmapStateLogReader};
use codes_freezer::{AddressCodes, CodeMdbx, CodeResolver, CodesFreezer, ExternalSourceDatabase};
use plain_state::PlainStateStore;

/// Blocks per state commit while recording forward.
const STATE_COMMIT_INTERVAL: u64 = 512;
use eyre::Context as _;
use reth_primitives_traits::RecoveredBlock;
use geth_freezer::GethBlockSource;
use witness::{
    WitnessBatchCache, WitnessDatabase, WitnessFreezerReader, WitnessFreezerWriter,
    WitnessReplayDatabase, WitnessSink,
};

use clap::Parser;
use reth_chainspec::{ChainSpec, EthereumHardforks};
use reth_cli::chainspec::ChainSpecParser;
use reth_cli_commands::common::CliNodeTypes;
use reth_cli_runner::CliContext;
use reth_ethereum_primitives::EthPrimitives;
use reth_node_ethereum::EthEngineTypes;

use reth_cli_commands::common::{AccessRights, Environment, EnvironmentArgs};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, trace, warn};

use eyre::{Report, Result};
use reth_consensus::FullConsensus;
use reth_evm::{block::BlockExecutor, execute::Executor, ConfigureEvm};
use reth_node_ethereum::{consensus::EthBeaconConsensus, EthEvmConfig};
use reth_provider::{
    providers::BlockchainProvider, BlockNumReader, BlockReader, ChainSpecProvider,
    StateProviderFactory,
};
use reth_revm::database::StateProviderDatabase;
use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::thread::JoinHandle;
// 已移除 AssertUnwindSafe，不再使用 catch_unwind（性能优化）
use crate::revm::state::{AccountInfo, Bytecode};
use crate::revm::Database as RevmDatabase;
use alloy_primitives::{Address, B256, U256};
#[cfg(unix)]
use libc;
use reth_primitives_traits::BlockBody;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Read, Write};
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use tokio::signal;
// 使用 Reth 的 Compact 编码替代 RLP（性能优化）
use reth_codecs::Compact;
use reth_primitives_traits::Account;

// 性能优化：预定义空 codehash 常量，避免重复创建
const EMPTY_CODE_HASH_BYTES: &[u8; 32] = &[
    0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c, 0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
    0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b, 0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
];

// 性能优化：预计算空 codehash
#[inline]
fn get_empty_code_hash() -> B256 {
    B256::from_slice(EMPTY_CODE_HASH_BYTES)
}

/// Checks an execution against the block header.
///
/// Gas alone is a weak check: it says the block consumed what it should, not
/// that each transaction produced the right result. The receipts root covers
/// per-transaction status, cumulative gas and logs, so a witness that fed back
/// a wrong value shows up here even when the totals happen to line up.
/// The EVM configuration every replay worker shares.
///
/// With `--jit` the revmc backend compiles in this process: reth's node
/// configuration compiles in helper processes for the node's isolation, which
/// a batch replay does not need, and the replay's 256 workers observe
/// contract calls far faster than the node's default event channel drains,
/// so the channel and the drain are sized to keep the hotness counts honest.
/// Nothing is evicted during the run; the resident budget is the only cap.
#[cfg(feature = "jit")]
fn store_path_display(path: Option<&std::path::Path>) -> String {
    path.map(|p| p.display().to_string()).unwrap_or_default()
}

#[cfg(feature = "jit")]
fn replay_evm_config(
    chain_spec: Arc<ChainSpec>,
    jit: &reth_node_core::args::JitArgs,
    store: Option<Arc<dyn revmc::runtime::ArtifactStore>>,
) -> eyre::Result<EthEvmConfig<ChainSpec, reth_evm_ethereum::factory::RethEvmFactory>> {
    use reth_evm_ethereum::factory::{JitBackend, JitMode, RethEvmFactory, RuntimeConfig, RuntimeTuning};
    if !jit.enabled {
        return Ok(EthEvmConfig::new_with_evm_factory(chain_spec, RethEvmFactory::disabled()));
    }
    let defaults = RuntimeTuning::default();
    let tuning = RuntimeTuning {
        channel_capacity: jit.channel_capacity.max(1 << 20),
        max_events_per_drain: 1 << 20,
        event_drain_interval: Duration::from_millis(10),
        jit_hot_threshold: jit.hot_threshold,
        jit_max_bytecode_len: jit.max_bytecode_len,
        jit_max_pending_jobs: jit.max_pending_jobs,
        jit_worker_count: jit.worker_count.unwrap_or(32),
        resident_code_cache_bytes: jit.code_cache_bytes,
        idle_evict_duration: None,
        // The machine code is the point; revmc's default is LLVM -O2.
        // Everything that will ever be compiled is compiled up front; the
        // lookup events only feed hotness counts nobody reads.
        observe_lookups: jit.hot_threshold < 1_000_000_000,
        jit_opt_level: revmc::OptimizationLevel::Aggressive,
        aot_opt_level: revmc::OptimizationLevel::Aggressive,
        ..defaults
    };
    let config = RuntimeConfig {
        enabled: true,
        tuning,
        store,
        debug_assertions: jit.debug,
        blocking: jit.blocking,
        jit_mode: JitMode::InProcess,
        ..RuntimeConfig::default()
    };
    let backend = JitBackend::new(config)?;
    info!(
        hot_threshold = jit.hot_threshold,
        workers = jit.worker_count.unwrap_or(32),
        "revmc JIT compiles hot contracts in process"
    );
    Ok(EthEvmConfig::new_with_evm_factory(chain_spec, RethEvmFactory::new(backend)).with_jit_support())
}

#[cfg(not(feature = "jit"))]
fn replay_evm_config(
    chain_spec: Arc<ChainSpec>,
    jit: &reth_node_core::args::JitArgs,
) -> eyre::Result<EthEvmConfig<ChainSpec, reth_evm_ethereum::factory::RethEvmFactory>> {
    let (config, _) = reth_node_ethereum::node::build_evm_config(chain_spec, jit, None)?;
    Ok(config)
}

fn verify_against_header(
    chain_spec: &ChainSpec,
    block: &RecoveredBlock<reth_ethereum_primitives::Block>,
    result: &reth_evm::block::BlockExecutionResult<reth_ethereum_primitives::Receipt>,
    what: &str,
) -> eyre::Result<()> {
    let header = block.sealed_block().header();

    if result.gas_used != header.gas_used {
        // Per-transaction cumulative gas pins the divergence to a transaction
        // index; the block total alone only says that one exists.
        let cumulative: Vec<u64> = result
            .receipts
            .iter()
            .map(alloy_consensus::TxReceipt::cumulative_gas_used)
            .collect();
        eyre::bail!(
            "gas mismatch: {} {}, header says {}; per-tx cumulative: {:?}",
            what,
            result.gas_used,
            header.gas_used,
            cumulative
        )
    }

    // Before Byzantium a receipt carried the post-transaction state root rather
    // than a status flag (EIP-658), and execution does not produce a state root
    // per transaction - so the root simply cannot be rebuilt from these
    // receipts. reth's own consensus check is gated the same way.
    if chain_spec.is_byzantium_active_at_block(header.number) {
        let receipts_root =
            reth_ethereum_primitives::calculate_receipt_root_no_memo(&result.receipts);
        if receipts_root != header.receipts_root {
            eyre::bail!(
                "receipts root mismatch: {} {:?}, header says {:?}",
                what,
                receipts_root,
                header.receipts_root
            )
        }

        let logs_bloom = result
            .receipts
            .iter()
            .fold(alloy_primitives::Bloom::ZERO, |bloom, receipt| {
                bloom | alloy_consensus::TxReceipt::bloom(receipt)
            });
        if logs_bloom != header.logs_bloom {
            eyre::bail!("logs bloom mismatch: {}", what)
        }
    }

    Ok(())
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
    /// step size for loop (smaller value = better load balancing for mmap mode)
    #[arg(long, alias = "step", short = 's', default_value = "3")]
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
    /// Generate a keyless N42 witness freezer (`witness.cidx` + segmented `.cdat`).
    /// Uses the N42 batch-64 index and compression format.
    #[arg(long, alias = "witness-dir")]
    witness_dir: Option<PathBuf>,
    /// Execute from a previously generated witness freezer instead of the
    /// database ("on" to replay the whole range). Requires --witness-dir.
    #[arg(long, alias = "use-witness")]
    use_witness: Option<String>,
    /// Read blocks from a go-ethereum ancient store instead of the reth
    /// database (the directory holding `headers.cidx` and `bodies.cidx`).
    ///
    /// Replay needs headers, bodies and code but never account or storage
    /// state, so an ancient store plus --codes-dir covers it without a reth
    /// archive.
    #[arg(long, alias = "geth-ancient-dir")]
    geth_ancient_dir: Option<PathBuf>,
    /// Count the transactions and gas the ancient store holds, then exit.
    ///
    /// Reads headers and bodies only, so the totals come from the block data
    /// rather than from an execution run - a run reports what it processed,
    /// which is the same number only if it processed every block exactly once.
    #[arg(long, alias = "geth-census", requires = "geth_ancient_dir")]
    geth_census: bool,
    /// Read contract bytecode from an N42 codes freezer instead of the reth
    /// `Bytecodes` table (the directory holding `codes.hoff`).
    #[arg(long, alias = "codes-dir")]
    codes_dir: Option<PathBuf>,

    /// gov5's code MDBX (its `Code` table, code hash → bytecode): the fallback
    /// for code an address-indexed codes freezer does not resolve, such as a
    /// contract redeployed with different code.
    #[arg(long)]
    code_mdbx: Option<PathBuf>,

    /// gov5's `senders` freezer (the directory holding `senders.cidx`):
    /// transaction senders in block order, so none is recovered from its
    /// signature.
    #[arg(long)]
    senders_dir: Option<PathBuf>,

    /// Write contract heat — calls, inclusive gas and the block span of every
    /// contract whose code ran, by bytecode address, hottest first — to this
    /// CSV at the end of a witness replay.
    #[arg(long)]
    contract_heat: Option<PathBuf>,

    /// Directory of revmc AOT artifacts (`<code hash>_<spec>.so` + manifest).
    /// With --jit, everything in it is loaded into the resident map before
    /// the replay starts; with --aot-from-heat it is where compilation
    /// writes. Needs the `jit` feature.
    #[arg(long)]
    aot_store: Option<PathBuf>,

    /// Compile the hottest contracts of this contract-heat CSV (see
    /// --contract-heat) into --aot-store, for every spec their block span
    /// touches, then exit.
    #[arg(long)]
    aot_from_heat: Option<PathBuf>,

    /// How many rows of the heat table to compile.
    #[arg(long, default_value_t = 10_000)]
    aot_top: usize,

    /// revmc JIT: `--jit` compiles hot contracts to machine code on
    /// background workers and runs the compiled code from then on. Only
    /// available in a binary built with the `jit` feature.
    #[command(flatten)]
    jit: reth_node_core::args::JitArgs,
    /// Record the witness by executing forward from genesis, keeping the plain
    /// state in this directory, instead of reading historical state from reth.
    ///
    /// Block order means the parent state of each block is just what the
    /// previous one left behind, so no historical lookup is needed - but the
    /// blocks cannot be spread across workers. Pair with --geth-ancient-dir to
    /// leave the reth database out entirely.
    #[arg(long, alias = "state-dir")]
    state_dir: Option<PathBuf>,
    /// Check every block against its header (gas, and receipts root plus logs
    /// bloom from Byzantium on) during plain execution.
    ///
    /// Plain execution runs a whole task as one batch, which is faster but only
    /// reports totals. This runs block by block so a divergence is attributed
    /// to the block that caused it - the point of the mode is finding one.
    #[arg(long, alias = "verify")]
    verify: bool,
    /// Trace one block's calls, creates and self-destructs, then exit.
    ///
    /// A witness only records reads, so a divergence that lives in writes is
    /// invisible in it. This records what moves value instead.
    #[arg(long, alias = "trace-block")]
    trace_block: Option<u64>,
    /// Print what the historical provider reports for this account as of --begin.
    #[arg(long, alias = "query-account")]
    query_account: Option<Address>,
    /// Substitute account values the database reports incorrectly.
    ///
    /// Lines of `block,address,balance,nonce`. Use only where the stored value
    /// is known to be wrong and the correct one is known - an incomplete
    /// history index, for instance, resolves to the current value instead of
    /// the historical one, which execution cannot tell apart from a real read.
    #[arg(long, alias = "state-overrides")]
    state_overrides: Option<PathBuf>,
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
}

struct Task {
    start: u64,
    end: u64,
}

/// Read log entry types - 按访问顺序记录
#[derive(Debug, Clone)]
pub(crate) enum ReadLogEntry {
    Account {
        address: Address,
        data: Vec<u8>,
    },
    Storage {
        address: Address,
        key: B256,
        data: Vec<u8>,
    },
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

    fn basic(
        &mut self,
        address: Address,
    ) -> Result<Option<AccountInfo>, <DB as RevmDatabase>::Error> {
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

            ReadLogEntry::Account { address, data: buf }
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

    fn storage(
        &mut self,
        address: Address,
        index: U256,
    ) -> Result<U256, <DB as RevmDatabase>::Error> {
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

    info!(
        "Read log written to: {} ({} entries)",
        log_file_path,
        read_logs.len()
    );
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
                writer
                    .write_all(data)
                    .map_err(|e| eyre::eyre!("Brotli compression failed: {}", e))?;
            }
            Ok(compressed)
        }
        CompressionAlgorithm::Lzma => {
            let mut compressed = Vec::new();
            let mut encoder = xz2::write::XzEncoder::new(compressed, 6);
            encoder
                .write_all(data)
                .map_err(|e| eyre::eyre!("LZMA compression failed: {}", e))?;
            compressed = encoder
                .finish()
                .map_err(|e| eyre::eyre!("LZMA compression failed: {}", e))?;
            Ok(compressed)
        }
        CompressionAlgorithm::Lz4 => {
            // lz4_flex 需要预先知道解压后的大小，所以我们先写入大小
            let compressed = lz4_flex::compress(data);
            let mut result = Vec::new();
            result
                .write_all(&(data.len() as u32).to_le_bytes())
                .map_err(|e| eyre::eyre!("LZ4 compression failed: {}", e))?;
            result
                .write_all(&compressed)
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
            reader
                .read_to_end(&mut decompressed)
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
            // 性能优化：预分配正确大小的缓冲区，避免重新分配
            let mut decompressed = vec![0u8; size];
            // 使用 decompress_into 而不是 decompress，避免额外的内存分配
            lz4_flex::decompress_into(compressed_data, &mut decompressed)
                .map_err(|e| eyre::eyre!("LZ4 decompression failed: {}", e))?;
            Ok(decompressed)
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
    algorithm_str: &str,
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
                debug!(
                    "  {:?}: {} bytes ({:.2}%), compress={:.2}ms, decompress={:.2}ms",
                    stat.algorithm,
                    stat.compressed_size,
                    stat.ratio,
                    stat.compression_time.as_secs_f64() * 1000.0,
                    stat.decompression_time.as_secs_f64() * 1000.0
                );
            }

            // 自动选择压缩率最好的（但必须小于原数据大小）
            let best = stats
                .iter()
                .filter(|s| s.compressed_size < data.len()) // 只考虑压缩后更小的
                .min_by(|a, b| a.compressed_size.cmp(&b.compressed_size));

            if let Some(best_stat) = best {
                best_stat.algorithm
            } else {
                // 如果所有压缩算法都比原数据大，使用不压缩
                CompressionAlgorithm::None
            }
        }
        _ => {
            return Err(eyre::eyre!(
                "Unknown compression algorithm: {}. Use: none, zstd, brotli, lzma, lz4, or auto",
                algorithm_str
            ))
        }
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
            .append(true)
            .open(&idx_path)?;

        // 打开数据文件（追加模式）
        let bin_file = OpenOptions::new()
            .create(true)
            .read(true)
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
        self.bin_writer
            .get_mut()
            .seek(SeekFrom::End(0))
            .map_err(|e| eyre::eyre!("Failed to get file position: {}", e))
    }

    /// 写入数据到数据文件
    fn write_bin_data(&mut self, data: &[u8]) -> eyre::Result<()> {
        self.bin_writer
            .write_all(data)
            .map_err(|e| eyre::eyre!("Failed to write bin data: {}", e))
    }

    /// 写入索引条目
    fn write_index_entry(
        &mut self,
        block_number: u64,
        offset: u64,
        length: u64,
    ) -> eyre::Result<()> {
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
        .append(true)
        .open(&idx_path)?;

    // 打开数据文件（追加模式）
    let bin_file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(&bin_path)?;

    Ok((idx_file, bin_file))
}

/// 步骤2: 读取索引文件，获取所有块的索引条目（性能优化：批量读取+预分配）
fn read_index_file(idx_file: &mut File) -> eyre::Result<Vec<IndexEntry>> {
    use std::io::Seek;
    idx_file.seek(SeekFrom::Start(0))?;

    // 性能优化：获取文件大小，预分配Vec容量
    // 避免2000万次插入时的频繁重新分配（每次容量翻倍时需要复制所有数据）
    let file_size = idx_file.metadata()?.len() as usize;
    let entry_count = file_size / 24; // 每个条目24字节

    // 大索引文件（>100MB）显示加载进度
    let show_progress = file_size > 100 * 1024 * 1024;
    if show_progress {
        info!(
            "Loading index file: {:.1} MB ({} blocks)...",
            file_size as f64 / 1024.0 / 1024.0,
            entry_count
        );
    }

    // 性能优化：使用大缓冲区批量读取（2MB，适合大索引文件）
    // 对于2000万块（480MB），减少系统调用次数从2000万次到240次
    const BUFFER_SIZE: usize = 2 * 1024 * 1024; // 2MB缓冲区
    let mut reader = BufReader::with_capacity(BUFFER_SIZE, idx_file);

    // 性能优化：预分配Vec容量，避免频繁重新分配
    // 2000万条记录时，可节省约25次重新分配（log2(20000000) ≈ 24.25）
    let mut entries = Vec::with_capacity(entry_count);

    // 进度跟踪
    let mut bytes_read = 0usize;
    let mut last_progress = 0usize;

    // 性能优化：批量读取缓冲区（8192条 * 24字节 = 196KB）
    // 减少逐条解析的开销，使用Vec处理更快
    const BATCH_SIZE: usize = 8192;
    let mut batch_buffer = vec![0u8; BATCH_SIZE * 24];

    loop {
        match reader.read(&mut batch_buffer) {
            Ok(0) => break, // 文件结束
            Ok(n) => {
                bytes_read += n;

                // 显示进度（每10%显示一次，避免过多日志）
                if show_progress {
                    let progress = (bytes_read * 100) / file_size;
                    if progress >= last_progress + 10 {
                        info!(
                            "  Loading index: {}% ({:.1} MB / {:.1} MB)",
                            progress,
                            bytes_read as f64 / 1024.0 / 1024.0,
                            file_size as f64 / 1024.0 / 1024.0
                        );
                        last_progress = progress;
                    }
                }

                // 批量解析读取的数据
                let entry_count_in_batch = n / 24;
                for i in 0..entry_count_in_batch {
                    let offset = i * 24;
                    let block_number = u64::from_le_bytes([
                        batch_buffer[offset],
                        batch_buffer[offset + 1],
                        batch_buffer[offset + 2],
                        batch_buffer[offset + 3],
                        batch_buffer[offset + 4],
                        batch_buffer[offset + 5],
                        batch_buffer[offset + 6],
                        batch_buffer[offset + 7],
                    ]);
                    let entry_offset = u64::from_le_bytes([
                        batch_buffer[offset + 8],
                        batch_buffer[offset + 9],
                        batch_buffer[offset + 10],
                        batch_buffer[offset + 11],
                        batch_buffer[offset + 12],
                        batch_buffer[offset + 13],
                        batch_buffer[offset + 14],
                        batch_buffer[offset + 15],
                    ]);
                    let length = u64::from_le_bytes([
                        batch_buffer[offset + 16],
                        batch_buffer[offset + 17],
                        batch_buffer[offset + 18],
                        batch_buffer[offset + 19],
                        batch_buffer[offset + 20],
                        batch_buffer[offset + 21],
                        batch_buffer[offset + 22],
                        batch_buffer[offset + 23],
                    ]);
                    entries.push(IndexEntry {
                        block_number,
                        offset: entry_offset,
                        length,
                    });
                }
            }
            Err(e) => return Err(e.into()),
        }
    }

    if show_progress {
        info!("Index loaded: {} blocks", entries.len());
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
    compression_algorithm: &str, // 测试模式下会被忽略（不压缩）
    log_dir: Option<&Path>,
    _existing_blocks: &std::collections::HashSet<u64>,
    _single_thread: bool,                            // 如果为 true，不需要文件锁
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

    // 测试模式：如果启用内存模式，不压缩直接写入原始数据
    // 注意：这里需要检查环境变量，如果未设置，默认不启用（因为这是测试模式）
    let env_var = std::env::var("PEVM_IN_MEMORY_MODE");
    let enable_in_memory_mode = env_var
        .as_ref()
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false); // 默认不启用测试模式（需要显式设置环境变量）

    let uncompressed_size = uncompressed_data.len(); // 保存原始大小，供日志使用

    let (final_data, algorithm_opt, compressed_size) = if enable_in_memory_mode {
        // 测试模式：不压缩，直接写入原始数据（格式：count(8) + entries）
        trace!("测试模式：写入未压缩日志数据（块 {}，{} 条目，原始大小: {} bytes）", block_number, read_logs.len(), uncompressed_size);
        (uncompressed_data, None, 0)
    } else {
        // 正常模式：压缩数据
        let (algorithm, compressed_data, _stats) =
            choose_compression_algorithm(&uncompressed_data, compression_algorithm)?;
        let compressed_size = compressed_data.len();

        trace!("正常模式：写入压缩日志数据（块 {}，环境变量 PEVM_IN_MEMORY_MODE={:?}，压缩算法: {:?}，原始大小: {} bytes，压缩后: {} bytes）",
            block_number, env_var, algorithm, uncompressed_size, compressed_size);

        // 构建最终数据：压缩算法标识 + 压缩后的数据
        let mut data = Vec::new();
        data.write_all(&[algorithm.as_u8()])?; // 压缩算法标识
        data.write_all(&compressed_data)?; // 压缩后的数据
        (data, Some(algorithm), compressed_size)
    };

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

            if enable_in_memory_mode {
                debug!("Binary log written to accumulated files (buffered, no compression): block {} ({} entries, {} bytes, offset: {}, length: {})",
                    block_number, read_logs.len(), uncompressed_size, offset, length);
            } else if let Some(alg) = algorithm_opt {
                debug!("Binary log written to accumulated files (buffered): block {} ({} entries, compressed with {:?}, {} -> {} bytes, {:.2}% ratio, offset: {}, length: {})",
                    block_number, read_logs.len(), alg, uncompressed_size, compressed_size,
                    (compressed_size as f64 / uncompressed_size as f64) * 100.0, offset, length);
            }
        } else {
            // 直接写入（向后兼容，不使用缓冲）
            let (mut idx_file, mut bin_file) = open_log_files(log_dir)?;

            // 读取现有索引，检查块是否已存在
            let index_entries = read_index_file(&mut idx_file)?;
            if find_index_entry(&index_entries, block_number).is_some() {
                // 块已存在，跳过写入（累加模式：已存在的块不再写入）
                debug!(
                    "Block {} already exists in log files, skipping",
                    block_number
                );
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

            if enable_in_memory_mode {
                debug!("Binary log written to accumulated files (no compression): block {} ({} entries, {} bytes, offset: {}, length: {})",
                    block_number, read_logs.len(), uncompressed_size, offset, length);
            } else if let Some(alg) = algorithm_opt {
                debug!("Binary log written to accumulated files: block {} ({} entries, compressed with {:?}, {} -> {} bytes, {:.2}% ratio, offset: {}, length: {})",
                    block_number, read_logs.len(), alg, uncompressed_size, compressed_size,
                    (compressed_size as f64 / uncompressed_size as f64) * 100.0, offset, length);
            }
        }
    } else {
        // 写入单个文件（向后兼容）
        let log_file_path = format!("block_{}_reads.bin", block_number);
        let mut log_file = BufWriter::new(File::create(&log_file_path)?);
        log_file.write_all(&final_data)?;
        log_file.flush()?;

        if enable_in_memory_mode {
            debug!(
                "Binary log written to: {} ({} entries, no compression, {} bytes)",
                log_file_path,
                read_logs.len(),
                uncompressed_size
            );
        } else if let Some(alg) = algorithm_opt {
            debug!("Binary log written to: {} ({} entries, compressed with {:?}, {} -> {} bytes, {:.2}% ratio)",
                log_file_path, read_logs.len(), alg, uncompressed_size, compressed_size,
                (compressed_size as f64 / uncompressed_size as f64) * 100.0);
        }
    }

    Ok(())
}

/// 全局文件句柄包装器（用于复用文件句柄，避免重复打开文件）
/// 性能优化：在程序开始时打开一次，所有线程共享，程序结束时关闭
///
/// 测试模式：如果使用内存模式，将整个 bin 文件读入内存
pub(crate) struct GlobalLogFileHandle {
    // 测试模式：整个文件内容在内存中（无压缩）
    #[allow(dead_code)]
    in_memory_data: Option<Arc<Vec<u8>>>,

    #[cfg(unix)]
    fd: i32, // Unix 文件描述符，用于 pread（无锁读取）
    #[cfg(not(unix))]
    file: Arc<Mutex<File>>, // Windows 系统仍然需要 Mutex
    #[allow(dead_code)] // 保留用于调试和日志
    file_path: PathBuf,
}

impl GlobalLogFileHandle {
    /// 创建新的全局文件句柄
    /// 测试模式：如果 enable_in_memory 为 true，将整个文件读入内存（无压缩）
    pub(crate) fn new(file_path: &Path, enable_in_memory: bool) -> eyre::Result<Self> {
        if enable_in_memory {
            // 测试模式：将整个文件读入内存
            info!(
                "测试模式：将 bin 文件完全读入内存（无压缩）: {}",
                file_path.display()
            );
            let file_size = std::fs::metadata(file_path)?.len();
            info!(
                "文件大小: {} bytes ({:.2} GB)",
                file_size,
                file_size as f64 / (1024.0 * 1024.0 * 1024.0)
            );

            let mut file = std::fs::File::open(file_path).map_err(|e| {
                eyre::eyre!("Failed to open log file {}: {}", file_path.display(), e)
            })?;

            let mut buffer = Vec::with_capacity(file_size as usize);
            std::io::Read::read_to_end(&mut file, &mut buffer)?;

            info!("文件已读入内存: {} bytes", buffer.len());

            Ok(Self {
                in_memory_data: Some(Arc::new(buffer)),
                #[cfg(unix)]
                fd: 0,
                #[cfg(not(unix))]
                file: Arc::new(Mutex::new(std::fs::File::create("/dev/null")?)), // 占位符
                file_path: file_path.to_path_buf(),
            })
        } else {
            // 正常模式：使用文件描述符
            #[cfg(unix)]
            {
                use std::os::unix::io::AsRawFd;
                let file = std::fs::File::open(file_path).map_err(|e| {
                    eyre::eyre!("Failed to open log file {}: {}", file_path.display(), e)
                })?;
                let fd = file.as_raw_fd();
                let fd_clone = unsafe { libc::dup(fd) };
                if fd_clone < 0 {
                    return Err(eyre::eyre!(
                        "Failed to dup file descriptor: {}",
                        std::io::Error::last_os_error()
                    ));
                }
                Ok(Self {
                    in_memory_data: None,
                    fd: fd_clone,
                    file_path: file_path.to_path_buf(),
                })
            }
            #[cfg(not(unix))]
            {
                let file = std::fs::File::open(file_path).map_err(|e| {
                    eyre::eyre!("Failed to open log file {}: {}", file_path.display(), e)
                })?;
                Ok(Self {
                    in_memory_data: None,
                    file: Arc::new(Mutex::new(file)),
                    file_path: file_path.to_path_buf(),
                })
            }
        }
    }

    /// 从全局文件句柄读取数据
    /// 优化：在 Unix 系统上使用 pread（无锁、无 seek 的系统调用）
    /// 测试模式：如果使用内存模式，直接从内存读取（无压缩）
    pub(crate) fn read_range(&self, offset: u64, length: u64) -> eyre::Result<Vec<u8>> {
        // 测试模式：从内存读取
        if let Some(ref in_memory) = self.in_memory_data {
            let start = offset as usize;
            let end = start + length as usize;
            if end > in_memory.len() {
                return Err(eyre::eyre!(
                    "Read range out of bounds: offset={}, length={}, file_size={}",
                    offset,
                    length,
                    in_memory.len()
                ));
            }
            // 直接从内存复制（无压缩，数据已经是未压缩的）
            Ok(in_memory[start..end].to_vec())
        } else {
            // 正常模式：从文件读取
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
                    return Err(eyre::eyre!(
                        "pread failed: {}",
                        std::io::Error::last_os_error()
                    ));
                }
                if bytes_read as u64 != length {
                    return Err(eyre::eyre!(
                        "pread incomplete: expected {} bytes, got {}",
                        length,
                        bytes_read
                    ));
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

    /// 批量读取多个数据段（性能优化：使用 pread 无锁并发读取）
    /// 输入：按偏移量排序的 (原始索引, offset, length) 列表
    /// 返回：Vec<(原始索引, 数据)>，按原始索引排序
    ///
    /// 优化策略：
    /// 1. 测试模式：如果使用内存模式，直接从内存读取
    /// 2. 正常模式：在 Unix 系统上使用 pread（无锁、无 seek 的系统调用），支持并发读取
    /// 3. 对于连续的数据段，仍然合并读取以减少系统调用
    pub(crate) fn read_ranges_batch(
        &self,
        ranges: &[(usize, u64, u64)],
    ) -> eyre::Result<Vec<(usize, Vec<u8>)>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        // 测试模式：从内存读取
        if let Some(ref in_memory) = self.in_memory_data {
            let mut results = Vec::with_capacity(ranges.len());

            // 性能优化：检测连续的数据段，合并读取以减少内存复制
            let mut i = 0;
            while i < ranges.len() {
                let (original_idx, start_offset, start_length) = ranges[i];

                // 检查后续是否有连续的数据段
                let mut end_offset = start_offset + start_length;
                let mut merged_count = 1;

                while i + merged_count < ranges.len() {
                    let (_, next_offset, next_length) = ranges[i + merged_count];
                    if next_offset == end_offset {
                        end_offset = next_offset + next_length;
                        merged_count += 1;
                    } else {
                        break;
                    }
                }

                if merged_count > 1 {
                    // 合并读取连续的数据段
                    let total_length = (end_offset - start_offset) as usize;
                    let start = start_offset as usize;
                    let end = start + total_length;
                    if end > in_memory.len() {
                        return Err(eyre::eyre!(
                            "Read range out of bounds: offset={}, length={}, file_size={}",
                            start_offset,
                            total_length,
                            in_memory.len()
                        ));
                    }
                    // 性能优化：避免创建中间 Vec，直接使用切片索引
                    let merged_buffer = &in_memory[start..end];

                    // 分割合并的数据
                    let mut current_offset = 0;
                    for j in 0..merged_count {
                        let (orig_idx, _, length) = ranges[i + j];
                        let data = merged_buffer[current_offset..current_offset + length as usize]
                            .to_vec();
                        results.push((orig_idx, data));
                        current_offset += length as usize;
                    }

                    i += merged_count;
                } else {
                    // 单独读取：直接从内存读取
                    let start = start_offset as usize;
                    let end = start + start_length as usize;
                    if end > in_memory.len() {
                        return Err(eyre::eyre!(
                            "Read range out of bounds: offset={}, length={}, file_size={}",
                            start_offset,
                            start_length,
                            in_memory.len()
                        ));
                    }
                    // 性能优化：需要 Vec<u8> 类型，必须 clone
                    let buffer = in_memory[start..end].to_vec();
                    results.push((original_idx, buffer));
                    i += 1;
                }
            }

            // 按原始索引排序，保持调用顺序
            results.sort_by_key(|(idx, _)| *idx);
            Ok(results)
        } else {
            // 正常模式：从文件读取
            #[cfg(unix)]
            {
                use std::os::unix::io::RawFd;
                let mut results = Vec::with_capacity(ranges.len());

                // 性能优化：检测连续的数据段，合并读取以减少系统调用
                let mut i = 0;
                while i < ranges.len() {
                    let (original_idx, start_offset, start_length) = ranges[i];

                    // 检查后续是否有连续的数据段（当前段的结束位置 = 下一段的开始位置）
                    let mut end_offset = start_offset + start_length;
                    let mut merged_count = 1;

                    // 查找连续的数据段
                    while i + merged_count < ranges.len() {
                        let (_, next_offset, next_length) = ranges[i + merged_count];
                        if next_offset == end_offset {
                            // 连续的数据段，合并读取
                            end_offset = next_offset + next_length;
                            merged_count += 1;
                        } else {
                            break;
                        }
                    }

                    if merged_count > 1 {
                        // 合并读取连续的数据段
                        let total_length = (end_offset - start_offset) as usize;
                        let mut merged_buffer = vec![0u8; total_length];
                        let bytes_read = unsafe {
                            libc::pread(
                                self.fd as RawFd,
                                merged_buffer.as_mut_ptr() as *mut libc::c_void,
                                total_length as libc::size_t,
                                start_offset as libc::off_t,
                            )
                        };
                        if bytes_read < 0 {
                            return Err(eyre::eyre!(
                                "pread failed: {}",
                                std::io::Error::last_os_error()
                            ));
                        }
                        if bytes_read as usize != total_length {
                            return Err(eyre::eyre!(
                                "pread incomplete: expected {} bytes, got {}",
                                total_length,
                                bytes_read
                            ));
                        }

                        // 分割合并的数据
                        let mut current_offset = 0;
                        for j in 0..merged_count {
                            let (orig_idx, _, length) = ranges[i + j];
                            let data = merged_buffer
                                [current_offset..current_offset + length as usize]
                                .to_vec();
                            results.push((orig_idx, data));
                            current_offset += length as usize;
                        }

                        i += merged_count;
                    } else {
                        // 单独读取：使用 pread（无锁、无 seek）
                        let mut buffer = vec![0u8; start_length as usize];
                        let bytes_read = unsafe {
                            libc::pread(
                                self.fd as RawFd,
                                buffer.as_mut_ptr() as *mut libc::c_void,
                                start_length as libc::size_t,
                                start_offset as libc::off_t,
                            )
                        };
                        if bytes_read < 0 {
                            return Err(eyre::eyre!(
                                "pread failed: {}",
                                std::io::Error::last_os_error()
                            ));
                        }
                        if bytes_read as u64 != start_length {
                            return Err(eyre::eyre!(
                                "pread incomplete: expected {} bytes, got {}",
                                start_length,
                                bytes_read
                            ));
                        }

                        results.push((original_idx, buffer));
                        i += 1;
                    }
                }

                // 按原始索引排序，保持调用顺序
                results.sort_by_key(|(idx, _)| *idx);
                Ok(results)
            }
            #[cfg(not(unix))]
            {
                // Windows 系统：仍然使用带锁的方式
                let mut results = Vec::with_capacity(ranges.len());
                let mut file = self.file.lock().unwrap();

                for (original_idx, offset, length) in ranges {
                    file.seek(SeekFrom::Start(*offset))?;
                    let mut buffer = vec![0u8; *length as usize];
                    file.read_exact(&mut buffer)?;
                    results.push((*original_idx, buffer));
                }

                Ok(results)
            }
        }
    }

    /// 获取文件路径
    #[allow(dead_code)] // 保留用于调试和日志
    pub(crate) fn file_path(&self) -> &Path {
        &self.file_path
    }
}

/// 从累加文件系统或单个文件读取日志数据
/// 测试模式：如果使用内存模式且无压缩，直接返回未压缩数据；否则返回压缩数据
/// 性能优化：返回压缩数据，在线程内部解压到线程局部缓存
/// 如果提供了全局文件句柄，使用它；否则每次打开文件（向后兼容）
pub(crate) fn read_log_compressed_data(
    log_path_or_block: &str,
    log_dir: Option<&Path>,
    cached_index_entries: Option<&[IndexEntry]>,
    global_file_handle: Option<&GlobalLogFileHandle>,
    is_in_memory_mode: bool, // 测试模式：是否使用内存模式（无压缩）
) -> eyre::Result<Vec<u8>> {
    if let Some(log_dir) = log_dir {
        // 从累加文件系统读取（使用索引查找偏移量和长度）
        let block_number: u64 = log_path_or_block
            .parse()
            .map_err(|_| eyre::eyre!("Invalid block number: {}", log_path_or_block))?;

        // 使用缓存的索引条目或读取索引文件
        let entry = if let Some(cached) = cached_index_entries {
            *find_index_entry(cached, block_number)
                .ok_or_else(|| eyre::eyre!("Block {} not found in log index", block_number))?
        } else {
            let (mut idx_file, _) = open_log_files(log_dir)?;
            let index_entries = read_index_file(&mut idx_file)?;
            *find_index_entry(&index_entries, block_number)
                .ok_or_else(|| eyre::eyre!("Block {} not found in log index", block_number))?
        };

        // 使用全局文件句柄（如果提供）或每次打开文件
        let data = if let Some(file_handle) = global_file_handle {
            // 使用全局文件句柄，避免重复打开文件
            file_handle.read_range(entry.offset, entry.length)?
        } else {
            // 向后兼容：每次打开文件，但使用 pread（如果可用）而不是 BufReader
            let bin_path = log_dir.join("blocks_log.bin");
            #[cfg(unix)]
            {
                use std::os::unix::io::{AsRawFd, RawFd};
                let file = std::fs::File::open(&bin_path)?;
                let fd = file.as_raw_fd();
                let mut buffer = vec![0u8; entry.length as usize];
                let bytes_read = unsafe {
                    libc::pread(
                        fd as RawFd,
                        buffer.as_mut_ptr() as *mut libc::c_void,
                        entry.length as libc::size_t,
                        entry.offset as libc::off_t,
                    )
                };
                if bytes_read < 0 {
                    return Err(eyre::eyre!(
                        "pread failed: {}",
                        std::io::Error::last_os_error()
                    ));
                }
                if bytes_read as u64 != entry.length {
                    return Err(eyre::eyre!(
                        "pread incomplete: expected {} bytes, got {}",
                        entry.length,
                        bytes_read
                    ));
                }
                buffer
            }
            #[cfg(not(unix))]
            {
                let file = std::fs::File::open(&bin_path)?;
                const BUFFER_SIZE: usize = 1024 * 1024;
                let mut reader = BufReader::with_capacity(BUFFER_SIZE, file);
                reader.seek(SeekFrom::Start(entry.offset))?;
                let mut buffer = vec![0u8; entry.length as usize];
                reader.read_exact(&mut buffer)?;
                buffer
            }
        };

        // 检测数据格式：如果第一个字节是压缩算法标识（0-4），说明是压缩格式
        // 测试模式：如果使用内存模式，尝试检测数据格式并自动解压
        // 注意：更精确的检测方法是检查第一个字节是否是有效的压缩算法标识
        // 但为了简单，我们检查第一个字节是否 <= 4（压缩算法标识范围）
        // 未压缩数据的第一个字节通常是 count 的低位，对于小数据量可能是 0-4
        // 但 count 是 u64，如果 count 很大，第一个字节不会是 0-4
        // 更安全的检测：尝试解析第一个字节作为压缩算法，如果成功且后续数据能解压，则是压缩格式
        if is_in_memory_mode {
            // 检查第一个字节是否是压缩算法标识（0=None, 1=Zstd, 2=Brotli, 3=Lzma, 4=Lz4）
            if !data.is_empty() && data[0] <= 4 {
                // 尝试解析为压缩格式
                if let Ok(algorithm) = CompressionAlgorithm::from_u8(data[0]) {
                    // 尝试解压（如果失败，说明不是压缩格式，回退到未压缩格式）
                    let mut cursor = std::io::Cursor::new(&data);
                    let mut algorithm_byte = [0u8; 1];
                    if cursor.read_exact(&mut algorithm_byte).is_ok() {
                        let compressed_data = data[cursor.position() as usize..].to_vec();
                        if let Ok(uncompressed_data) = decompress_data(&compressed_data, algorithm)
                        {
                            // 解压成功，说明是压缩格式
                            return Ok(uncompressed_data);
                        }
                    }
                }
            }
            // 数据是未压缩格式，或解压失败（回退），直接返回
            return Ok(data);
        }

        // 正常模式：数据包含压缩算法标识，需要解压（但这里只返回压缩数据，解压在线程内部进行）
        Ok(data)
    } else {
        // 从单个文件读取（向后兼容）
        let file = std::fs::File::open(log_path_or_block)?;
        const BUFFER_SIZE: usize = 1024 * 1024;
        let mut reader = BufReader::with_capacity(BUFFER_SIZE, file);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;

        // 测试模式：如果使用内存模式，检测数据格式并自动解压
        if is_in_memory_mode {
            // 检查第一个字节是否是压缩算法标识（0=None, 1=Zstd, 2=Brotli, 3=Lzma, 4=Lz4）
            if !buffer.is_empty() && buffer[0] <= 4 {
                // 尝试解析为压缩格式
                if let Ok(algorithm) = CompressionAlgorithm::from_u8(buffer[0]) {
                    // 尝试解压（如果失败，说明不是压缩格式，回退到未压缩格式）
                    let mut cursor = std::io::Cursor::new(&buffer);
                    let mut algorithm_byte = [0u8; 1];
                    if cursor.read_exact(&mut algorithm_byte).is_ok() {
                        let compressed_data = buffer[cursor.position() as usize..].to_vec();
                        if let Ok(uncompressed_data) = decompress_data(&compressed_data, algorithm)
                        {
                            // 解压成功，说明是压缩格式
                            return Ok(uncompressed_data);
                        }
                    }
                }
            }
            // 数据是未压缩格式，或解压失败（回退），直接返回
            return Ok(buffer);
        }

        // 正常模式：返回压缩数据
        Ok(buffer)
    }
}

/// 步骤2和3: 从累加文件系统或单个文件读取日志（版本5：按顺序，无address和key，无type，支持压缩）
/// 如果提供了缓存的索引条目，使用缓存；否则读取索引文件
/// 注意：此函数保留用于向后兼容，建议使用 read_log_compressed_data + new_from_compressed_buffer 以获得更好性能
#[allow(dead_code)] // 在测试中使用
pub(crate) fn read_read_logs_binary(
    log_path_or_block: &str,
    log_dir: Option<&Path>,
    cached_index_entries: Option<&[IndexEntry]>,
) -> eyre::Result<Vec<ReadLogEntry>> {
    let file_contents = if let Some(log_dir) = log_dir {
        // 步骤2和3: 从累加文件系统读取（使用索引查找偏移量和长度）
        // 尝试解析块号
        let block_number: u64 = log_path_or_block
            .parse()
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

        // 直接从文件读取（使用 BufReader 优化）
        let bin_path = log_dir.join("blocks_log.bin");
        let file = std::fs::File::open(&bin_path)?;
        const BUFFER_SIZE: usize = 1024 * 1024;
        let mut reader = BufReader::with_capacity(BUFFER_SIZE, file);

        // Seek 到指定位置
        reader.seek(SeekFrom::Start(entry.offset))?;

        // 读取指定长度的数据
        let mut buffer = vec![0u8; entry.length as usize];
        reader.read_exact(&mut buffer)?;
        buffer
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
        // 能解码为 U256，且解码后数据应该被消耗完（或接近）
        let is_storage =
            U256::decode(&mut data_copy.as_slice()).is_ok() && data_len <= 33;

        if is_storage {
            entries.push(ReadLogEntry::Storage {
                address: Address::ZERO, // 占位符，不使用
                key: B256::ZERO,        // 占位符，不使用
                data,
            });
        } else {
            entries.push(ReadLogEntry::Account {
                address: Address::ZERO, // 占位符，不使用
                data,
            });
        }
    }

    Ok(entries)
}

/// 线程局部缓存：解压后的原始字节数据（线程本地，无需锁）
/// 性能优化：每个块只解压一次，后续所有访问都是内存访问，只移动指针
/// 使用单一位置指针，按顺序读取：第一字节是长度，然后是内容，指针+长度+1指向下一个长度
/// 性能优化：支持共享内存数据（Arc<Vec<u8>>），避免在 in_memory_mode 下复制数据
struct ThreadLocalLogCache {
    // 性能优化：使用枚举支持两种模式：
    // - Owned: 拥有数据（解压后的数据，需要复制）
    // - Shared: 共享数据（in_memory_mode 下，直接使用 Arc，避免复制）
    uncompressed_data: std::sync::Arc<Vec<u8>>, // 统一使用 Arc，支持共享和拥有
    current_pos: usize,                         // 当前位置指针（指向下一个条目的长度字节）
}

impl ThreadLocalLogCache {
    /// 从压缩的缓冲区创建线程局部缓存
    /// 测试模式：如果 is_uncompressed 为 true，直接使用 buffer（无压缩）
    /// 一次性解压所有数据，避免后续的文件读取和解压
    /// 性能优化：不预先解析所有条目，只记录数据开始位置
    /// 从压缩的缓冲区创建线程局部缓存
    /// 性能优化：接受 Vec<u8> 所有权，避免不必要的内存复制
    #[inline]
    fn from_compressed_buffer_owned(buffer: Vec<u8>, is_uncompressed: bool) -> eyre::Result<Self> {
        let uncompressed_data = if is_uncompressed {
            // 测试模式：数据已经是未压缩的格式（count(8) + entries）
            // 性能优化：直接使用传入的 Vec，零复制
            std::sync::Arc::new(buffer)
        } else {
            // 正常模式：需要解压
            if buffer.is_empty() {
                return Err(eyre::eyre!("Empty buffer"));
            }

            let algorithm = CompressionAlgorithm::from_u8(buffer[0])?;

            // 读取压缩后的数据（跳过算法标识字节）
            let compressed_data = &buffer[1..];

            // 解压数据（一次性解压，缓存到线程本地）
            // 注意：原始 buffer 会被 drop，解压后的数据是新分配的
            std::sync::Arc::new(decompress_data(compressed_data, algorithm)?)
        };

        // 性能优化：快速检查：读取条目数量（8字节），当前位置指向第一个条目的长度字节
        if uncompressed_data.len() < 8 {
            return Err(eyre::eyre!(
                "Invalid uncompressed data: too short ({} bytes)",
                uncompressed_data.len()
            ));
        }

        Ok(Self {
            uncompressed_data,
            current_pos: 8, // 跳过8字节的count，指向第一个条目的长度字节
        })
    }

    /// 兼容性方法：从引用创建（会复制数据）
    #[inline]
    #[allow(dead_code)]
    fn from_compressed_buffer(buffer: &[u8], is_uncompressed: bool) -> eyre::Result<Self> {
        Self::from_compressed_buffer_owned(buffer.to_vec(), is_uncompressed)
    }

    /// 从共享内存数据创建线程局部缓存（性能优化：避免复制）
    /// 用于 in_memory_mode 下，直接使用 GlobalLogFileHandle 的 in_memory_data
    #[inline]
    #[allow(dead_code)] // 预留给后续优化使用
    fn from_shared_data(shared_data: std::sync::Arc<Vec<u8>>) -> eyre::Result<Self> {
        if shared_data.len() < 8 {
            return Err(eyre::eyre!(
                "Invalid uncompressed data: too short ({} bytes)",
                shared_data.len()
            ));
        }

        Ok(Self {
            uncompressed_data: shared_data,
            current_pos: 8, // 跳过8字节的count，指向第一个条目的长度字节
        })
    }

    /// 读取下一个条目的位置信息（按顺序读取）
    /// 返回：Some((数据起始位置, 数据长度)) 或 None（已到末尾）
    /// 性能优化：只移动指针，不复制数据，不判断类型
    #[inline] // 性能优化：内联函数，减少函数调用开销
    fn next_entry_pos(&mut self) -> Option<(usize, usize)> {
        // 性能优化：缓存数据长度和当前位置，避免重复访问
        let data_len = self.uncompressed_data.len();
        let current = self.current_pos;

        // 性能优化：合并边界检查和长度读取
        if current >= data_len {
            return None;
        }

        // 读取长度字节（1字节）
        let entry_len = self.uncompressed_data[current] as usize;
        let data_start = current + 1; // 数据开始位置（跳过长度字节）

        // 性能优化：提前计算下一个位置，减少重复计算
        let next_pos = data_start + entry_len;

        // 检查边界（使用提前计算的位置）
        if next_pos > data_len {
            return None; // 数据损坏
        }

        // 移动指针到下一个条目的长度字节
        self.current_pos = next_pos;

        Some((data_start, entry_len))
    }

    /// 获取指定位置的数据切片（零拷贝，直接返回引用）
    /// 性能优化：内联函数，减少函数调用开销
    #[inline]
    fn get_data_at(&self, offset: usize, length: usize) -> Option<&[u8]> {
        // 性能优化：使用 get 方法，避免边界检查的额外开销（在 release 模式下）
        self.uncompressed_data.get(offset..offset + length)
    }
}

/// 从日志文件读取数据的 Database（按顺序访问，使用线程局部缓存）
pub(crate) struct LoggedDatabase {
    cache: ThreadLocalLogCache, // 线程局部缓存（解压后的原始数据）
    state_provider: Arc<dyn reth_provider::StateProvider>, // 用于 code_by_hash（代码不在日志文件中），使用 Arc 以便共享
                                                           // 性能关键修复：移除 code_cache，直接运行模式没有缓存，使用日志模式也不应该有
                                                           // 避免额外的内存分配和 HashMap 查找开销
}

impl std::fmt::Debug for LoggedDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoggedDatabase").finish_non_exhaustive()
    }
}

impl LoggedDatabase {
    /// 从压缩的缓冲区创建新的 LoggedDatabase（性能优化：零复制版本）
    /// 接受 Vec<u8> 所有权，避免不必要的内存复制
    fn new_from_compressed_buffer_owned(
        buffer: Vec<u8>,
        state_provider: Arc<dyn reth_provider::StateProvider>,
        is_uncompressed: bool, // 测试模式：是否未压缩
    ) -> eyre::Result<Self> {
        let cache = ThreadLocalLogCache::from_compressed_buffer_owned(buffer, is_uncompressed)?;
        Ok(Self {
            cache,
            state_provider,
        })
    }

    /// 兼容性方法：从引用创建（会复制数据）
    #[allow(dead_code)]
    fn new_from_compressed_buffer(
        buffer: &[u8],
        state_provider: Arc<dyn reth_provider::StateProvider>,
        is_uncompressed: bool, // 测试模式：是否未压缩
    ) -> eyre::Result<Self> {
        Self::new_from_compressed_buffer_owned(buffer.to_vec(), state_provider, is_uncompressed)
    }

    /// 从共享内存数据创建新的 LoggedDatabase（性能优化：避免复制）
    /// 用于 in_memory_mode 下，直接使用 GlobalLogFileHandle 的 in_memory_data
    #[allow(dead_code)] // 预留给后续优化使用
    fn new_from_shared_data(
        shared_data: std::sync::Arc<Vec<u8>>,
        state_provider: Arc<dyn reth_provider::StateProvider>,
    ) -> eyre::Result<Self> {
        let cache = ThreadLocalLogCache::from_shared_data(shared_data)?;
        Ok(Self {
            cache,
            state_provider,
        })
    }

    /// 获取下一个条目数据（按顺序，使用指针访问，零拷贝）
    /// 性能优化：只移动指针，不复制数据，直接返回引用
    /// 不判断类型，只是按顺序返回下一个条目
    /// 内联函数，减少调用开销
    #[inline]
    fn next_entry_data(&mut self) -> Option<&[u8]> {
        let (offset, length) = self.cache.next_entry_pos()?;
        self.cache.get_data_at(offset, length)
    }

    /// 获取下一个 Account 数据（按顺序，使用指针访问，零拷贝）
    /// 性能优化：只移动指针，不复制数据，直接返回引用
    /// 按顺序返回下一个条目，不判断类型
    /// 内联函数，直接调用 next_entry_data
    #[inline]
    fn next_account_data(&mut self) -> Option<&[u8]> {
        self.next_entry_data()
    }

    /// 获取下一个 Storage 数据（按顺序，使用指针访问，零拷贝）
    /// 性能优化：只移动指针，不复制数据，直接返回引用
    /// 按顺序返回下一个条目，不判断类型
    /// 内联函数，直接调用 next_entry_data
    #[inline]
    fn next_storage_data(&mut self) -> Option<&[u8]> {
        self.next_entry_data()
    }

    /// 从 Compact 编码数据解码账户信息（替代 RLP，性能优化）
    /// 性能优化：内联函数，减少函数调用开销
    #[inline]
    pub(crate) fn decode_account_compact(data: &[u8]) -> eyre::Result<AccountInfo> {
        // 性能优化：快速检查：空数据表示账户不存在
        // 合并检查条件，减少分支预测失败
        if data.is_empty() || (data.len() == 1 && data[0] == 0x00) {
            return Err(eyre::eyre!("Account does not exist (empty data)"));
        }

        // 使用 Compact 解码 Account
        // 性能优化：直接使用数据长度，避免重复计算
        let data_len = data.len();
        let (account, _) = Account::from_compact(data, data_len);

        // 性能优化：直接使用 unwrap_or_else，但使用内联闭包减少开销
        // 使用模块级常量，避免重复创建 B256
        let code_hash = account.bytecode_hash.unwrap_or_else(get_empty_code_hash);

        // 性能优化：直接使用 default()，编译器会优化
        Ok(AccountInfo::new(
            account.balance,
            account.nonce,
            code_hash,
            Bytecode::default(),
        ))
    }

    /// 从 RLP 数据解码账户信息，处理可选的 storage_root 和 code_hash（向后兼容）
    pub(crate) fn decode_account_rlp(data: &[u8]) -> eyre::Result<AccountInfo> {
        use alloy_rlp::Decodable;

        // 空 code 的 hash
        let empty_code_hash = B256::from_slice(&[
            0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c, 0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7,
            0x03, 0xc0, 0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b, 0x7b, 0xfa, 0xd8, 0x04,
            0x5d, 0x85, 0xa4, 0x70,
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
        Ok(AccountInfo::new(balance, nonce, code_hash, empty_bytecode))
    }
}

impl crate::revm::Database for LoggedDatabase {
    type Error = <StateProviderDatabase<Box<dyn reth_provider::StateProvider>> as crate::revm::Database>::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        // 按顺序从日志文件读取数据（不读数据库，不对比）
        // 性能优化：直接使用引用，零拷贝
        if let Some(compact_data) = self.next_account_data() {
            // 性能优化：快速检查空账户标记（Compact 编码：0x00 或 RLP 旧格式：0xc0）
            // 合并条件检查，减少分支预测失败
            if compact_data.is_empty()
                || (compact_data.len() == 1 && (compact_data[0] == 0x00 || compact_data[0] == 0xc0))
            {
                // 日志文件说账户不存在
                return Ok(None);
            }

            // 性能优化：直接使用 Compact 解码（新格式），只在必要时回退到 RLP
            // 使用 if let 替代 match，减少分支开销
            let mut logged_account_info =
                if let Ok(info) = Self::decode_account_compact(compact_data) {
                    info
                } else {
                    // 如果 Compact 解码失败，尝试 RLP 解码（向后兼容）
                    match Self::decode_account_rlp(compact_data) {
                        Ok(info) => info,
                        Err(e) => {
                            debug!(
                            "LoggedDatabase::basic({}) failed to decode account from log file: {}",
                            address, e
                        );
                            return Ok(None);
                        }
                    }
                };

            // 如果 code_hash 不是 empty_code_hash，加载 bytecode
            // 性能优化：直接使用 code_hash() 方法，避免中间变量
            // 性能优化：使用模块级常量进行快速比较（直接比较字节），避免不必要的数据库查询
            let code_hash = logged_account_info.code_hash();
            if code_hash.as_slice() != EMPTY_CODE_HASH_BYTES {
                // 只在必要时查询数据库
                // 性能优化：使用 if let 替代 match，减少分支
                if let Ok(code) = self.code_by_hash(code_hash) {
                    // 性能优化：避免不必要的 clone，直接设置
                    logged_account_info.code = Some(code);
                }
                // 加载失败，继续使用空的 code（不打印警告，避免日志噪音）
            }

            // 返回日志文件的数据
            Ok(Some(logged_account_info))
        } else {
            // 性能关键：不打印日志，避免热路径上的 tracing 开销
            // 返回 None 表示日志文件中没有更多账户数据
            Ok(None)
        }
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        // 性能关键修复：不使用缓存，直接查询数据库
        // 原因：直接运行模式没有缓存，使用日志模式也不应该有缓存，避免额外的内存分配和 HashMap 查找开销
        // 从数据库读取代码（代码不在日志文件中）
        // 注意：StateProviderDatabase 需要 &mut，所以每次都需要创建
        let mut inner_db = StateProviderDatabase::new(
            self.state_provider.as_ref() as &dyn reth_provider::StateProvider
        );
        inner_db.code_by_hash(code_hash)
    }

    fn storage(&mut self, _address: Address, _index: U256) -> Result<U256, Self::Error> {
        // 性能优化：按顺序从日志文件读取数据（不读数据库，不对比）
        // 直接使用引用，零拷贝
        if let Some(compact_data) = self.next_storage_data() {
            // 性能优化：使用 Compact 解码 U256（新格式）
            // Compact 解码 U256 返回 (value, remaining_data)
            // 直接使用数据长度，避免重复计算
            let data_len = compact_data.len();
            let (logged_value, _remaining) = U256::from_compact(compact_data, data_len);
            Ok(logged_value)
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
    /// Records a witness by executing forward from genesis.
    ///
    /// The state is carried in `state_dir` rather than looked up per block, so
    /// this needs no historical index - but it also cannot be spread across
    /// workers, since block N's input is block N-1's output.
    async fn record_sequentially(
        &self,
        load_block: impl Fn(u64) -> eyre::Result<RecoveredBlock<reth_ethereum_primitives::Block>>,
        chain_spec: Arc<ChainSpec>,
        state_dir: &Path,
        witness_dir: &Path,
        blocks: Option<Arc<GethBlockSource>>,
    ) -> eyre::Result<()> {
        let store = PlainStateStore::open(state_dir)?;
        let applied = store.last_block()?;

        // The freezer and the state have to agree on where to resume; both are
        // append-only and neither can be rewound.
        let next_block = applied.map_or(0, |block| block + 1);
        if self.begin_number != next_block {
            eyre::bail!(
                "state at {} holds blocks 0..{}, so --begin must be {}, got {}",
                state_dir.display(),
                next_block.saturating_sub(1),
                next_block,
                self.begin_number
            )
        }

        if applied.is_none() {
            let seeded = store.init_genesis(chain_spec.genesis())?;
            info!(accounts = seeded, "Seeded the genesis allocation");
        }

        let should_stop = Arc::new(AtomicBool::new(false));
        let should_stop_signal = Arc::clone(&should_stop);
        tokio::spawn(async move {
            if signal::ctrl_c().await.is_ok() {
                info!("Received Ctrl+C, finishing the current block...");
                should_stop_signal.store(true, Ordering::Relaxed);
            }
        });

        let writer = WitnessFreezerWriter::open(witness_dir, self.begin_number)?;
        let sink = WitnessSink::new(writer, self.begin_number, Arc::clone(&should_stop));
        let evm_config = EthEvmConfig::ethereum(chain_spec.clone());

        info!(
            state = %state_dir.display(),
            witness = %witness_dir.display(),
            from = self.begin_number,
            to = self.end_number,
            blocks = if blocks.is_some() { "geth ancient store" } else { "reth database" },
            "Recording the witness in block order"
        );

        let mut batch = store.batch()?;
        let start = Instant::now();
        let mut gas_total: u64 = 0;
        let mut transactions: u64 = 0;
        let mut reported = Instant::now();

        for block_number in self.begin_number..=self.end_number {
            if should_stop.load(Ordering::Relaxed) {
                break
            }

            let block = load_block(block_number)?;

            let (output, witness) = {
                let state_db = batch.database(blocks.clone());
                let (witness_db, witness_stream) = WitnessDatabase::new(state_db);
                let executor = evm_config.batch_executor(witness_db);
                let output = executor.execute(&block)?;
                let witness = witness_stream.take();
                (output, witness)
            };

            verify_against_header(&chain_spec, &block, &output.result, "executed")
                .wrap_err_with(|| format!("block {block_number}"))?;

            // The genesis block is not executed by a node: its state comes
            // from the chain spec's allocation. Recording its witness keeps the
            // freezer aligned, but folding its rewards into the state would
            // credit a block reward nobody ever received - visible later as
            // every zero-address beneficiary reading one reward too many.
            if block_number == 0 {
                batch.mark_block(0)?;
            } else {
                batch.apply(block_number, &output.state)?;
            }
            sink.submit(block_number, witness)?;

            // Commit periodically: MDBX holds every dirty page until commit, so
            // one transaction cannot span the whole range.
            if block_number.wrapping_sub(self.begin_number) % STATE_COMMIT_INTERVAL
                == STATE_COMMIT_INTERVAL - 1
            {
                batch.commit()?;
                batch = store.batch()?;
            }

            gas_total += output.gas_used;
            transactions += block.sealed_block().body().transaction_count() as u64;

            if reported.elapsed() >= Duration::from_secs(1) {
                let elapsed = start.elapsed().as_secs_f64().max(0.001);
                let done = block_number - self.begin_number + 1;
                info!(
                    bn = block_number,
                    txs = transactions,
                    b_per_s = format!("{:.0}", done as f64 / elapsed),
                    Ggas_per_s = format!("{:.2}", gas_total as f64 / elapsed / 1e9),
                    "Recording"
                );
                reported = Instant::now();
            }
        }

        batch.commit()?;
        let items = sink.finish()?;
        info!(
            items,
            state_at = store.last_block()?.unwrap_or_default(),
            "Wrote N42 witness freezer"
        );
        Ok(())
    }

    /// Totals the ancient store holds: transactions, gas, and where it ends.
    ///
    /// The cost is reading headers and bodies once, so the walk is spread
    /// across threads and nothing else is done per block.
    fn run_census(&self, directory: &Path) -> eyre::Result<()> {
        let source = geth_freezer::GethBlockSource::open(directory)?;
        let end = self.end_number.min(source.last_block());
        let threads = self.thread_count.unwrap_or_else(num_cpus::get).max(1);
        let census = source.census(self.begin_number..=end, threads)?;
        println!("transactions {}", census.transactions);
        println!("gas_used     {}", census.gas_used);
        println!("last_block   {}", end);
        Ok(())
    }

    pub async fn execute<
        N: CliNodeTypes<
            Payload = EthEngineTypes,
            Primitives = EthPrimitives,
            ChainSpec = C::ChainSpec,
        >,
    >(
        self,
        _ctx: CliContext,
        runtime: reth_tasks::Runtime,
    ) -> eyre::Result<()> {
        info!("Executing EVM command...");

        // A census reads headers and bodies and nothing else, so it runs before
        // the reth environment is opened and works on a machine that has only
        // the ancient store.
        if self.geth_census {
            let directory = self
                .geth_ancient_dir
                .as_deref()
                .expect("clap requires --geth-ancient-dir");
            return self.run_census(directory)
        }

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
                    info!(
                        "CPU profiler started (100Hz sampling rate, using thread-based sampling)"
                    );
                    #[cfg(windows)]
                    info!(
                        "CPU profiler started (100Hz sampling rate, using thread-based sampling)"
                    );
                    Some(guard)
                }
                Err(e) => {
                    warn!(
                        "Failed to start CPU profiler: {}. Continuing without profiling.",
                        e
                    );
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
        let Environment {
            provider_factory, ..
            // reth v2.5 moved runtime creation out of `init`; storage I/O runs on it.
        } = self.env.init::<N>(AccessRights::RO, runtime)?;

        let log_dir = self.log_dir.as_deref();

        // 如果启用了 --repair-log，执行修复并退出
        if self.repair_log {
            if let Some(log_dir) = log_dir {
                info!("Starting log repair mode...");
                match MmapStateLogDatabase::repair_log(log_dir, self.begin_number, self.end_number)
                {
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
                                error!(
                                    "Missing blocks: first={}, last={}, count={}",
                                    first_missing,
                                    last_missing,
                                    result.missing_blocks.len()
                                );
                                error!(
                                    "Please re-run with --begin {} to fill in missing blocks",
                                    first_missing
                                );
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
                match MmapStateLogDatabase::rebuild_index(
                    log_dir,
                    self.begin_number,
                    self.end_number,
                ) {
                    Ok(result) => {
                        info!("Index rebuild completed!");
                        if !result.missing_blocks.is_empty() {
                            let first_missing = result.missing_blocks.first().unwrap();
                            let last_missing = result.missing_blocks.last().unwrap();
                            warn!(
                                "Missing/corrupted blocks: first={}, last={}, count={}",
                                first_missing,
                                last_missing,
                                result.missing_blocks.len()
                            );
                            warn!(
                                "To regenerate, run: --log-block on --begin {} --end {}",
                                first_missing, self.end_number
                            );
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

        // 测试模式：启用内存模式（将 bin 文件完全读入内存，无压缩）
        // 可以通过环境变量 PEVM_IN_MEMORY_MODE=true 启用
        let enable_in_memory_mode = std::env::var("PEVM_IN_MEMORY_MODE")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(true); // 默认启用测试模式

        // 检查是否启用日志记录模式（用于决定是否跳过已存在的块）
        let log_block_enabled = self.log_block.as_ref().map(|s| s == "on").unwrap_or(false);

        // 检查是否启用从日志执行模式
        let use_log_enabled = self.use_log.as_ref().map(|s| s == "on").unwrap_or(false);

        // 创建日志执行统计收集器（仅在use_log模式下使用）
        let log_stats = Arc::new(LogExecutionStats::new());

        // 如果启用了 mmap 日志模式（--mmap-log），初始化数据库
        // - 写入模式（--log-block on）：使用 MmapStateLogDatabase + RwLock
        // - 读取模式（--use-log on）：使用 MmapStateLogReader（无锁，性能更好）
        let mmap_log_db: Option<Arc<std::sync::RwLock<MmapStateLogDatabase>>> = if self.use_mmap_log
            && log_block_enabled
        {
            if let Some(log_dir) = log_dir {
                match MmapStateLogDatabase::open_for_write(log_dir) {
                    Ok(db) => {
                        if let Some((min, max)) = db.get_block_range() {
                            info!("Using mmap-based state log storage (WRITE mode): {} blocks, range {} - {}",
                                db.block_count(), min, max);
                        } else {
                            info!(
                                "Using mmap-based state log storage (WRITE mode): empty database"
                            );
                        }
                        Some(Arc::new(std::sync::RwLock::new(db)))
                    }
                    Err(e) => {
                        warn!(
                            "Failed to open MmapStateLogDatabase: {}. Falling back to other modes.",
                            e
                        );
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
        let mmap_log_reader: Option<Arc<MmapStateLogReader>> = if self.use_mmap_log
            && use_log_enabled
            && !log_block_enabled
        {
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
                        warn!(
                            "Failed to open MmapStateLogReader: {}. Falling back to locked mode.",
                            e
                        );
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        // 检查已存在的块（用于补齐模式：只生成不存在或损坏的块）
        // mmap 模式：使用 block_exists() 精确检查（包括损坏检测）
        // 文件系统模式：使用 HashSet 检查
        let (existing_blocks, cached_index_entries): (
            Arc<std::collections::HashSet<u64>>,
            Arc<Vec<IndexEntry>>,
        ) = if let Some(ref mmap_db) = mmap_log_db {
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
            (
                Arc::new(std::collections::HashSet::new()),
                Arc::new(Vec::new()),
            )
        } else if let Some(log_dir) = log_dir {
            // 文件系统模式：从索引文件获取已存在的块
            let (mut idx_file, _) = open_log_files(log_dir)?;
            let index_entries = read_index_file(&mut idx_file)?;
            // 性能优化：预分配 HashSet 容量，减少重新分配
            let blocks: std::collections::HashSet<u64> =
                index_entries.iter().map(|e| e.block_number).collect();
            if !blocks.is_empty() {
                let min_block = *blocks.iter().min().unwrap();
                let max_block = *blocks.iter().max().unwrap();
                info!("Found log files: {} ({} blocks indexed, range: {} - {}), will fill in missing blocks",
                        log_dir.display(), blocks.len(), min_block, max_block);
            } else {
                info!(
                    "Log directory is empty: {}, will generate all blocks from --begin",
                    log_dir.display()
                );
            }
            // 文件系统模式使用完整的 HashSet（块可能不连续）
            (Arc::new(blocks), Arc::new(index_entries))
        } else {
            (
                Arc::new(std::collections::HashSet::new()),
                Arc::new(Vec::new()),
            )
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
        // One EVM configuration for every worker: with `--jit` it owns the
        // revmc backend and its resident map of compiled contracts, which the
        // workers share; without it, a thin wrapper over the plain factory.
        let contract_heat: Option<Arc<Mutex<heat::HeatMap>>> = self
            .contract_heat
            .as_ref()
            .map(|_| Arc::new(Mutex::new(heat::HeatMap::new())));
        let _consensus: Arc<dyn FullConsensus<EthPrimitives>> =
            Arc::new(EthBeaconConsensus::new(chain_spec.clone()));

        // 在 v1.8.4 中，共享 blockchain_db 也能正常工作
        let blockchain_db = BlockchainProvider::new(provider_factory.clone())?;

        // 性能修复：限制 provider 的生命周期，避免长时间持有读事务
        // 只用来获取 last_block_number，获取后立即释放
        // With an ancient store the blocks come from there and the database
        // may be an empty one made by `init`; the store's own range is checked
        // when it is opened.
        let last = if self.geth_ancient_dir.is_some() {
            u64::MAX
        } else {
            let provider = provider_factory.provider()?;
            provider.last_block_number()?
        }; // provider 在这里被 drop，释放读事务

        if self.begin_number > self.end_number {
            eyre::bail!("the begin block number is higher than the end block number")
        }
        if self.end_number > last {
            eyre::bail!("The end block number is higher than the latest block number")
        }

        // Witness recording and witness replay use a separate wire format from
        // the keyed state-log machinery; mixing them produces files that
        // neither reader understands.
        let witness_replay = match self.use_witness.as_deref() {
            None => false,
            Some("on") => true,
            Some(other) => {
                eyre::bail!("--use-witness only accepts \"on\", got \"{}\"", other)
            }
        };
        if (self.witness_dir.is_some() || witness_replay)
            && (self.log_block.is_some()
                || self.use_log.is_some()
                || self.use_mmap_log
                || self.repair_log
                || self.rebuild_idx)
        {
            eyre::bail!(
                "--witness-dir/--use-witness cannot be combined with state-log recording, replay, or repair options"
            );
        }
        if witness_replay && self.witness_dir.is_none() {
            eyre::bail!("--use-witness on requires --witness-dir")
        }

        // Diagnostic: what does the historical provider say an account held at
        // the end of --begin? Answers whether a divergence starts in the state
        // handed to execution or inside execution itself.
        if let Some(address) = self.query_account {
            let at = self.begin_number;
            let provider = blockchain_db.history_by_block_number(at)?;
            let account = reth_provider::AccountReader::basic_account(&provider, &address)?;
            match account {
                Some(account) => println!(
                    "account {address:?} after block {at}: nonce {} balance {} ({:.6} ETH) code {:?}",
                    account.nonce,
                    account.balance,
                    f64::from(account.balance) / 1e18,
                    account.bytecode_hash
                ),
                None => println!("account {address:?} does not exist after block {at}"),
            }
            return Ok(())
        }

        if let Some(number) = self.trace_block {
            let blocks = blockchain_db.block_with_senders_range(number..=number)?;
            let block = blocks
                .first()
                .ok_or_else(|| eyre::eyre!("provider has no block {}", number))?;
            let state_provider = blockchain_db.history_by_block_number(number.saturating_sub(1))?;
            let evm_config = EthEvmConfig::ethereum(chain_spec.clone());

            let tracer = trace::CallTracer::default();
            let mut state = crate::revm::State::builder()
                .with_database(StateProviderDatabase::new(&state_provider))
                .with_bundle_update()
                .build();
            let evm = evm_config.evm_with_env_and_inspector(
                &mut state,
                evm_config.evm_env(block.sealed_block().header())?,
                tracer.clone(),
            );
            let ctx = evm_config.context_for_block(block.sealed_block())?;
            let executor = evm_config.create_executor(evm, ctx);
            let result = executor.execute_block(block.transactions_recovered());

            for event in tracer.events() {
                println!("{event}");
            }
            match result {
                Ok(output) => {
                    println!(
                        "gas_used {} (header says {})",
                        output.gas_used,
                        block.sealed_block().header().gas_used
                    );
                }
                Err(error) => println!("execution failed: {error}"),
            }
            return Ok(())
        }

        // Forward recording is its own execution mode: one block after another,
        // carrying the state along, so it does not go through the worker pool.
        if let Some(state_dir) = self.state_dir.as_deref() {
            let Some(witness_dir) = self.witness_dir.as_deref() else {
                eyre::bail!("--state-dir is for recording a witness; pass --witness-dir too")
            };
            if witness_replay {
                eyre::bail!("--state-dir records a witness; it cannot be combined with --use-witness")
            }
            // Forward recording derives state by executing, so it never consults
            // the history index an override exists to correct. Applying one here
            // would replace a value this mode computed correctly with an
            // externally asserted one.
            if self.state_overrides.is_some() {
                eyre::bail!(
                    "--state-overrides corrects historical lookups; --state-dir builds state by                      executing forward and does not perform any"
                )
            }
            let blocks = match self.geth_ancient_dir.as_deref() {
                Some(directory) => Some(Arc::new(GethBlockSource::open(directory)?)),
                None => None,
            };
            let source = blocks.clone();
            let provider = blockchain_db.clone();
            let load_block = move |number: u64| match source.as_ref() {
                Some(ancient) => ancient
                    .blocks(number..=number)?
                    .pop()
                    .ok_or_else(|| eyre::eyre!("ancient store has no block {}", number)),
                None => provider
                    .block_with_senders_range(number..=number)?
                    .pop()
                    .ok_or_else(|| eyre::eyre!("provider has no block {}", number)),
            };
            return self
                .record_sequentially(
                    load_block,
                    chain_spec.clone(),
                    state_dir,
                    witness_dir,
                    blocks,
                )
                .await
        }

        let state_overrides = match self.state_overrides.as_deref() {
            Some(path) => Some(Arc::new(state_override::StateOverrides::load(path)?)),
            None => None,
        };

        // Alternative block and code sources. Both are read-only and shared
        // across workers.
        let geth_blocks = match self.geth_ancient_dir.as_deref() {
            Some(directory) => {
                let mut source = GethBlockSource::open(directory)?;
                if let Some(senders) = self.senders_dir.as_deref() {
                    source = source.with_senders(senders)?;
                }
                if self.end_number > source.last_block() {
                    eyre::bail!(
                        "ancient store holds blocks 0..{}, but --end {} was requested",
                        source.last_block(),
                        self.end_number
                    )
                }
                info!(
                    last_block = source.last_block(),
                    path = %directory.display(),
                    "Reading blocks from the geth ancient store"
                );
                Some(Arc::new(source))
            }
            None => None,
        };

        // An address-indexed codes freezer resolves code per account as the
        // witness is read; the content-addressed one answers code_by_hash.
        let code_resolver = match self.codes_dir.as_deref() {
            Some(directory) if AddressCodes::is_present(directory) => {
                let by_address = AddressCodes::open(directory)?;
                info!(
                    contracts = by_address.entries(),
                    path = %directory.display(),
                    "Reading contract code from the address-indexed codes freezer"
                );
                let mdbx = match self.code_mdbx.as_deref() {
                    Some(directory) => {
                        info!(path = %directory.display(), "Falling back to the code MDBX for redeployed contracts");
                        Some(Arc::new(CodeMdbx::open(directory)?))
                    }
                    None => None,
                };
                Some(Arc::new(CodeResolver::new(Some(Arc::new(by_address)), mdbx)))
            }
            _ => match self.code_mdbx.as_deref() {
                Some(directory) => Some(Arc::new(CodeResolver::new(None, Some(Arc::new(CodeMdbx::open(directory)?))))),
                None => None,
            },
        };
        let codes = match self.codes_dir.as_deref() {
            Some(directory) if code_resolver.is_some() && AddressCodes::is_present(directory) => None,
            Some(directory) => {
                let freezer = CodesFreezer::open(directory)?;
                info!(
                    contracts = freezer.entries(),
                    path = %directory.display(),
                    "Reading contract code from the codes freezer"
                );
                Some(Arc::new(freezer))
            }
            None => None,
        };

        // Replay maps the freezer read-only, so every worker can slice it
        // without copying or locking.
        let witness_reader = if witness_replay {
            let directory = self.witness_dir.as_deref().expect("checked above");
            let reader = WitnessFreezerReader::open(directory)?;
            if self.end_number >= reader.items() {
                eyre::bail!(
                    "witness freezer holds blocks 0..{}, but --end {} was requested",
                    reader.items().saturating_sub(1),
                    self.end_number
                )
            }
            info!(
                items = reader.items(),
                path = %directory.display(),
                "Replaying blocks from N42 witness freezer"
            );
            Some(Arc::new(reader))
        } else {
            None
        };

        #[cfg(feature = "jit")]
        let aot_store = match self.aot_store.as_deref() {
            Some(dir) => Some(Arc::new(aot::DirArtifactStore::open(dir)?)),
            None => None,
        };
        #[cfg(feature = "jit")]
        let shared_evm_config = Arc::new(replay_evm_config(
            chain_spec.clone(),
            &self.jit,
            aot_store.clone().map(|s| s as Arc<dyn revmc::runtime::ArtifactStore>),
        )?);
        #[cfg(not(feature = "jit"))]
        let shared_evm_config = Arc::new(replay_evm_config(chain_spec.clone(), &self.jit)?);
        #[cfg(feature = "jit")]
        if let Some(store) = aot_store.as_ref() {
            if !self.jit.enabled {
                eyre::bail!("--aot-store needs --jit");
            }
            let Some(resolver) = code_resolver.as_ref() else {
                eyre::bail!("--aot-store needs --codes-dir / --code-mdbx to read the bytecodes")
            };
            let backend = shared_evm_config.executor_factory.evm_factory().backend();
            let code_for = |hash: B256| -> Option<alloy_primitives::Bytes> {
                resolver.by_hash(hash).ok().flatten().map(|code| code.original_bytes())
            };
            if let Some(heat) = self.aot_from_heat.as_deref() {
                let Some(blocks) = geth_blocks.as_ref() else {
                    eyre::bail!("--aot-from-heat needs --geth-ancient-dir for the block timestamps")
                };
                let timeline = aot::SpecTimeline::build(&chain_spec, blocks, self.end_number)?;
                let contracts = aot::read_heat(heat, self.aot_top)?;
                let (requests, missing) = aot::requests_for(&contracts, &timeline, code_for);
                info!(contracts = contracts.len(), requests = requests.len(), missing, "AOT build");
                aot::drive(backend, requests, "compile")?;
                let (count, bytes) = store.size();
                info!(count, mib = bytes / (1024 * 1024), path = %store_path_display(self.aot_store.as_deref()), "AOT artifacts");
                return Ok(());
            }
            let requests = aot::requests_from_store(store, code_for);
            info!(artifacts = requests.len(), "loading AOT artifacts into the resident map");
            aot::drive(backend, requests, "load")?;
        }
        // 步骤4: 创建任务池
        // 取消续传逻辑：从 --begin 开始创建所有任务
        // 块级别的过滤在 worker 线程中进行（只处理不存在的块，补齐模式）

        let mut tasks = VecDeque::new();
        let mut current_start = self.begin_number;
        while current_start <= self.end_number {
            // 取消续传逻辑：从 --begin 开始创建所有任务，不跳过已存在的块
            // 实际的块级别过滤在 worker 线程中进行（只处理不存在的块，补齐模式）

            let mut current_end =
                std::cmp::min(current_start + self.step_size as u64 - 1, self.end_number);
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
        // 重要修复：如果使用了 --mmap-log 且 mmap_log_reader 存在，跳过旧格式文件（blocks_log.bin）
        let use_log_enabled = self.use_log.as_ref().map(|s| s == "on").unwrap_or(false);
        let global_log_file_handle: Option<Arc<GlobalLogFileHandle>> = if use_log_enabled
            && mmap_log_reader.is_none()
        {
            // 只有在未使用 mmap_log_reader 时才使用旧格式的 blocks_log.bin
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
        info!(
            "Using {} worker threads (Windows: reduced to avoid mmap issues)",
            thread_count
        );
        #[cfg(not(windows))]
        info!("Using {} worker threads", thread_count);
        let mut threads: Vec<JoinHandle<Result<()>>> = Vec::with_capacity(thread_count);

        // 性能优化：预分配任务给每个线程，避免 Mutex 竞争
        // 在日志回放模式下，每个块数据独立，不需要同步
        let use_log_enabled_check = self.use_log.as_ref().map(|s| s == "on").unwrap_or(false);
        let use_lockfree_mode = use_log_enabled_check && self.use_mmap_log;
        let tasks_vec: Vec<Task> = tasks.into_iter().collect();

        // 根据模式决定任务分配方式（避免 tasks_vec 被使用两次）
        let (per_thread_tasks, task_queue_vec): (Vec<Vec<Task>>, Vec<Task>) = if use_lockfree_mode {
            // 无锁模式：预分配任务给每个线程
            let mut distributed: Vec<Vec<Task>> = (0..thread_count).map(|_| Vec::new()).collect();
            for (i, task) in tasks_vec.into_iter().enumerate() {
                distributed[i % thread_count].push(task);
            }
            (distributed, Vec::new())
        } else {
            // 兼容模式：使用原来的共享队列
            (Vec::new(), tasks_vec)
        };

        // 共享任务队列（仅在非无锁模式下使用）
        let task_queue = Arc::new(Mutex::new(VecDeque::from(task_queue_vec)));

        // 创建共享计数器（使用原子操作避免锁竞争，提高多线程性能）
        let cumulative_gas = Arc::new(AtomicU64::new(0));
        // The counter tracks the last finished block, so it starts one below
        // the range. A witness freezer always starts at block 0, where there is
        // no lower block to name - saturating keeps that from wrapping to
        // u64::MAX and making the monitor think the run is already done.
        let block_counter = Arc::new(AtomicU64::new(self.begin_number.saturating_sub(1)));
        let txs_counter = Arc::new(AtomicU64::new(0));
        let should_stop = Arc::new(AtomicBool::new(false));

        // The freezer index is positional - item N is block N - so witnesses
        // must reach it in block order even though workers finish out of order.
        // The sink buffers and reorders; opening the writer here also enforces
        // that --begin continues exactly where the freezer left off.
        let witness_sink = match (self.witness_dir.as_deref(), witness_replay) {
            (Some(directory), false) => {
                let writer = WitnessFreezerWriter::open(directory, self.begin_number)?;
                info!(
                    path = %directory.display(),
                    first_block = self.begin_number,
                    "Recording N42 witness freezer"
                );
                Some(Arc::new(WitnessSink::new(
                    writer,
                    self.begin_number,
                    Arc::clone(&should_stop),
                )))
            }
            _ => None,
        };

        // 创建全局 Bytecode 缓存（合约代码不可变，可安全跨线程共享）
        // 这是最重要的性能优化之一，避免重复查询数据库获取相同的合约代码
        let bytecode_cache = Arc::new(BytecodeCache::new());

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
                let mut previous_block_counter: u64 = begin_number.saturating_sub(1);
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

                    // With --begin 0 there is no block below the range for the
                    // counter to start from, so it starts at 0 and ends up one
                    // ahead of the block it actually finished. Correct that here
                    // rather than distorting the counter every worker updates.
                    let current_block_counter = block_counter
                        .load(Ordering::Relaxed)
                        .saturating_sub(u64::from(begin_number == 0));
                    let diff_block = current_block_counter.saturating_sub(previous_block_counter);
                    previous_block_counter = current_block_counter;

                    let current_txs_counter = txs_counter.load(Ordering::Relaxed);
                    let diff_txs = current_txs_counter - previous_txs_counter;
                    previous_txs_counter = current_txs_counter;

                    if diff_block > 0 {
                        let duration = start.elapsed();
                        let seconds = duration.as_secs();
                        let millis = duration.subsec_millis(); // Milliseconds part of the duration
                        let total_seconds = seconds as f64 + millis as f64 / 1000.0;
                        let gas_throughput_str =
                            format_gas_throughput_as_ggas(diff_gas, Duration::from_secs(1));
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

        // 预分配任务迭代器（无锁模式）
        let mut per_thread_tasks_iter = per_thread_tasks.into_iter();

        for _thread_idx in 0..thread_count {
            let task_queue = Arc::clone(&task_queue);
            let cumulative_gas = Arc::clone(&cumulative_gas);
            let block_counter = Arc::clone(&block_counter);
            let txs_counter = Arc::clone(&txs_counter);
            let should_stop_worker = Arc::clone(&should_stop);

            // 无锁模式：获取该线程的预分配任务
            let thread_tasks: Vec<Task> = if use_lockfree_mode {
                per_thread_tasks_iter.next().unwrap_or_default()
            } else {
                Vec::new()
            };
            let use_lockfree_mode_thread = use_lockfree_mode;

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
            // 日志执行统计收集器（线程安全，用于聚合所有线程的统计）
            let log_stats_clone = log_stats.clone();
            // Witness 记录汇聚点（按块号排序后顺序写入 freezer）
            let witness_sink_clone = witness_sink.clone();
            // Witness 只读映射（回放模式，无锁共享）
            let witness_reader_clone = witness_reader.clone();
            let witness_replay_thread = witness_replay;
            let verify_blocks = self.verify;
            let geth_blocks_clone = geth_blocks.clone();
            let code_resolver_clone = code_resolver.clone();
            let state_overrides_clone = state_overrides.clone();
            let codes_clone = codes.clone();

            // 在 v1.8.4 中，共享 blockchain_db 也能正常工作
            let shared_evm_config = shared_evm_config.clone();
            let contract_heat_clone = contract_heat.clone();
            // Witness replay reads one block at a time from the ancient store:
            // decoding a whole task's blocks up front holds every body of a
            // 64-block task in memory at once, per thread, and the point of a
            // 64-block task is that its witness batch is decoded once.
            let lazy_blocks = witness_replay && geth_blocks.is_some();
            threads.push(thread::spawn(move || {
                let thread_id = thread::current().id();

                // The shared configuration; `ConfigureEvm` is implemented for
                // the `Arc`, so nothing downstream changes.
                let evm_config = shared_evm_config;

                // 性能关键优化：在 worker 线程开始时只调用一次 history_by_block_number
                // 而不是在每个 task 中调用（避免大量数据库查询）
                // code_by_hash 查询的 bytecode 是不可变的，不依赖于具体块号
                // 所以使用 begin_number - 1 作为基准块号是安全的
                // 注意：需要定期刷新以避免 MDBX 读事务超时（300秒）
                // Replay, like log replay, only needs the provider for bytecode
                // and block hashes - both keyed independently of block number -
                // so one provider per thread is enough.
                let mut thread_level_state_provider: Option<Arc<dyn reth_provider::StateProvider>> =
                    if use_log_enabled || witness_replay_thread {
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

                // Witness 回放：线程本地的批次缓存与失败计数（跨 task 复用）
                let mut witness_batch_cache = WitnessBatchCache::default();
                // Contract heat, when asked for: this worker's share, merged
                // into the shared map once its tasks are done.
                let mut heat_local = heat::HeatMap::new();
                let heat_wanted = contract_heat_clone.is_some();
                let mut reusable_cache = crate::revm::db::CacheState {
                    accounts: crate::revm::primitives::HashMap::with_capacity_and_hasher(512, Default::default()),
                    contracts: crate::revm::primitives::HashMap::with_capacity_and_hasher(64, Default::default()),
                };
                let witness_failures = Arc::new(AtomicUsize::new(0));

                // 无锁模式：使用预分配的任务迭代器
                let mut thread_tasks_iter = thread_tasks.into_iter();

                loop {
                    // 检查停止标志
                    if should_stop_worker.load(Ordering::Relaxed) {
                        debug!(target: "exex::evm", thread_id = ?thread_id, "Worker thread stopping due to Ctrl+C");
                        break;
                    }

                    // 获取下一个任务：无锁模式从本地列表获取，否则从共享队列获取
                    let task = if use_lockfree_mode_thread {
                        // 无锁模式：直接从本地列表获取，无需锁
                        match thread_tasks_iter.next() {
                            Some(t) => Some(t),
                            None => break, // 本地任务用完，退出循环
                        }
                    } else {
                        // 兼容模式：从共享队列获取（需要锁）
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
                            let blocks = match geth_blocks_clone.as_ref() {
                                Some(_) if lazy_blocks => Vec::new(),
                                Some(source) => source.blocks(task.start..=task.end)?,
                                None => blockchain_db
                                    .block_with_senders_range(task.start..=task.end)?,
                            };

                            // 步骤4: 如果使用累加模式且启用了日志记录（--log-block on），过滤掉已存在的块（断点续传）
                            // 性能优化：批量检查块存在状态，只获取一次 RwLock 读锁（减少锁竞争）
                            // 先收集所有块号
                            let all_block_numbers: Vec<u64> = blocks.iter()
                                .map(|block| block.sealed_block().header().number)
                                .collect();

                            // 批量检查块是否存在（一次锁获取）
                            let existing_in_mmap: std::collections::HashSet<u64> = if log_block_enabled {
                                if let Some(ref db) = mmap_log_db_clone {
                                    // mmap 模式：批量检查块是否存在（只获取一次锁）
                                    let db_guard = db.read().unwrap();
                                    db_guard.blocks_exist_batch(&all_block_numbers)
                                } else {
                                    // 文件系统模式：使用已有的 HashSet
                                    all_block_numbers.iter()
                                        .filter(|bn| existing_blocks_clone.contains(bn))
                                        .copied()
                                        .collect()
                                }
                            } else {
                                std::collections::HashSet::new()
                            };

                            // 本地检查函数（无锁，使用预计算的结果）
                            let worker_block_exists = |block_num: u64| -> bool {
                                existing_in_mmap.contains(&block_num)
                            };

                            // 先收集需要处理的块号，用于统计
                            let blocks_to_process_numbers: Vec<u64> = if let Some(_log_dir) = &log_dir_clone {
                                if log_block_enabled {
                                    // 只在 --log-block on 模式下过滤已存在的块
                                    all_block_numbers.iter()
                                        .filter(|bn| !worker_block_exists(**bn))
                                        .copied()
                                        .collect()
                                } else {
                                    // --use-log on 或其他模式，不过滤，处理所有块
                                    all_block_numbers
                                }
                            } else {
                                all_block_numbers
                            };

                            // 如果使用累加模式且启用了日志记录（--log-block on）且所有块都已存在，跳过执行
                            if let Some(_log_dir) = &log_dir_clone {
                                if log_block_enabled && blocks_to_process_numbers.is_empty() {
                                    return Ok(());
                                }
                            }

                            // 修复：记录实际处理的块号（用于准确统计，避免时序竞态导致的虚假统计）
                            // 在 --log-block on 模式下，blocks_to_process_numbers 是预先计算的，
                            // 但在执行时可能被其他 worker 跳过，导致统计不准确
                            let mut actually_processed_blocks: Vec<u64> = Vec::new();

                            // Witness 记录：每个块都要用其父块状态执行，因此和
                            // state-log 记录一样逐块创建 provider。产出交给 sink
                            // 按块号重排后顺序落盘。
                            if let Some(ref sink) = witness_sink_clone {
                                // The freezer cannot tolerate a hole: a block
                                // that never arrives stalls every worker queued
                                // behind it until shutdown.
                                let expected = task.end - task.start + 1;
                                if blocks.len() as u64 != expected {
                                    let message = format!(
                                        "provider returned {} of {} blocks for {}..={}",
                                        blocks.len(),
                                        expected,
                                        task.start,
                                        task.end
                                    );
                                    sink.abort(message.clone());
                                    should_stop_worker.store(true, Ordering::Relaxed);
                                    return Err(eyre::eyre!(message));
                                }

                                for block in blocks.iter() {
                                    if should_stop_worker.load(Ordering::Relaxed) {
                                        return Ok(());
                                    }
                                    let block_number = block.sealed_block().header().number;

                                    let recorded = (|| -> eyre::Result<Vec<u8>> {
                                        let state_provider = blockchain_db
                                            .history_by_block_number(block_number.saturating_sub(1))?;
                                        let inner_db = StateProviderDatabase::new(&state_provider);
                                        // Declared substitutions sit under the
                                        // recorder, so what gets written is the
                                        // corrected value, not the stored one.
                                        let inner_db = state_override::OverrideDatabase::new(
                                            inner_db,
                                            state_overrides_clone
                                                .as_ref()
                                                .and_then(|o| o.for_block(block_number))
                                                .unwrap_or_default(),
                                        );
                                        let (witness_db, witness_stream) =
                                            WitnessDatabase::new(inner_db);
                                        let executor = evm_config.batch_executor(witness_db);
                                        let output = executor.execute(block)?;
                                        // A witness is only worth keeping if the
                                        // execution that produced it reproduced
                                        // the block.
                                        verify_against_header(&chain_spec, block, &output.result, "executed")?;
                                        Ok(witness_stream.take())
                                    })();

                                    match recorded {
                                        Ok(witness) => sink.submit(block_number, witness)?,
                                        Err(error) => {
                                            // A missing block would stall every
                                            // worker queued behind it, so fail the
                                            // whole run instead of leaving a gap.
                                            sink.abort(format!(
                                                "block {} failed to execute: {}",
                                                block_number, error
                                            ));
                                            should_stop_worker.store(true, Ordering::Relaxed);
                                            return Err(error);
                                        }
                                    }
                                }
                            } else if let Some(ref reader) = witness_reader_clone {
                                // Witness 回放：bytecode 与 block hash 从未被记录，
                                // 仍需 provider 提供；账户/存储读取全部来自 witness。
                                let state_provider: Arc<dyn reth_provider::StateProvider> =
                                    if let Some(ref provider) = thread_level_state_provider {
                                        provider.clone()
                                    } else {
                                        Arc::new(blockchain_db.history_by_block_number(
                                            task.start.saturating_sub(1),
                                        )?)
                                    };

                                let mut senders_batch = None;
                                let mut lazy_block;
                                for block_number in task.start..=task.end {
                                    if should_stop_worker.load(Ordering::Relaxed) {
                                        return Ok(());
                                    }
                                    let block = if lazy_blocks {
                                        lazy_block = geth_blocks_clone
                                            .as_ref()
                                            .expect("lazy blocks need the ancient store")
                                            .block(block_number, &mut senders_batch)?;
                                        &lazy_block
                                    } else {
                                        match blocks.iter().find(|b| b.sealed_block().header().number == block_number) {
                                            Some(block) => block,
                                            None => continue,
                                        }
                                    };

                                    // A block whose witness does not replay is
                                    // reported and skipped: one bad witness must
                                    // not hide every block queued behind it.
                                    let replayed = (|| -> eyre::Result<()> {
                                        let witness = witness_batch_cache
                                            .witness_for(reader, block_number)?;
                                        let inner_db = CachedStateProviderDatabase::new(
                                            state_provider.as_ref(),
                                            Some(bytecode_cache_clone.clone()),
                                        );
                                        // Code and block hashes are not in the
                                        // witness; serve them from the freezers
                                        // when they were given.
                                        let sourced = ExternalSourceDatabase::new(
                                            inner_db,
                                            codes_clone.clone(),
                                            geth_blocks_clone.clone(),
                                        );
                                        let (replay_db, cursor) =
                                            WitnessReplayDatabase::new(sourced, witness);
                                        let replay_db = replay_db.with_codes(code_resolver_clone.clone());
                                        // Replay wants the receipts, not the
                                        // post-state: a State without bundle
                                        // updates skips the transition
                                        // bookkeeping the batch executor keeps
                                        // for a bundle nobody takes.
                                        // Pre-sized cache: a block touches a
                                        // few hundred accounts, and a table
                                        // that doubles its way there rehashes
                                        // several times per block.
                                        // This worker's cache, emptied and
                                        // handed back after every block: the
                                        // tables keep their capacity and stay
                                        // warm instead of being allocated and
                                        // freed a few hundred entries at a time.
                                        let prestate = std::mem::take(&mut reusable_cache);
                                        let mut state = crate::revm::State::builder()
                                            .with_database(replay_db)
                                            .with_cached_prestate(prestate)
                                            .build();
                                        let evm_env = evm_config.evm_env(block.sealed_block().header())?;
                                        let ctx = evm_config.context_for_block(block.sealed_block())?;
                                        let result = if heat_wanted {
                                            let inspector = heat::HeatInspector::new(&mut heat_local, block_number);
                                            let evm = evm_config.evm_with_env_and_inspector(&mut state, evm_env, inspector);
                                            let executor = evm_config.create_executor(evm, ctx);
                                            executor.execute_block(block.transactions_recovered())?
                                        } else {
                                            let evm = evm_config.evm_with_env(&mut state, evm_env);
                                            let executor = evm_config.create_executor(evm, ctx);
                                            executor.execute_block(block.transactions_recovered())?
                                        };
                                        verify_against_header(&chain_spec, block, &result, "replayed")?;
                                        let mut cache = std::mem::take(&mut state.cache);
                                        cache.clear();
                                        reusable_cache = cache;
                                        // Running out of witness is caught by the
                                        // database; bytes left over are the same
                                        // mismatch seen from the other side, and
                                        // nothing else would notice them.
                                        let consumed = cursor.consumed();
                                        if consumed != witness.len() {
                                            eyre::bail!(
                                                "witness not fully consumed: read {} of {} bytes",
                                                consumed,
                                                witness.len()
                                            )
                                        }
                                        Ok(())
                                    })();

                                    if let Err(error) = replayed {
                                        witness_failures.fetch_add(1, Ordering::Relaxed);
                                        error!(
                                            "Witness replay failed for block {}: {}",
                                            block_number, error
                                        );
                                    }
                                }
                            } else if log_block_enabled {
                                // 性能优化：批量收集日志数据，减少写入次数和锁竞争
                                // 先执行所有块并收集日志，然后批量写入
                                let mut block_logs: Vec<(u64, Vec<ReadLogEntry>)> = Vec::with_capacity(blocks.len());

                                // 为每个块单独执行并记录日志
                                for block in blocks.iter() {
                                    // 性能优化：移除热路径上的日志检查
                                    if should_stop_worker.load(Ordering::Relaxed) {
                                        return Ok(());
                                    }

                                    let block_number = block.sealed_block().header().number;

                                    // 如果使用累加模式且块已存在，跳过
                                    if let Some(_log_dir) = &log_dir_clone {
                                        if worker_block_exists(block_number) {
                                            continue;
                                        }
                                    }

                                    // 为每个块创建独立的 state provider 和日志记录器
                                    // 注意：必须使用 block_number - 1 的状态，否则执行结果不正确
                                    let state_provider = blockchain_db.history_by_block_number(
                                        block_number.saturating_sub(1)
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

                                    // 立即释放 state_provider，减少 MDBX mmap 压力
                                    drop(state_provider);

                                    // 性能优化：移除热路径上的日志检查
                                    if should_stop_worker.load(Ordering::Relaxed) {
                                        return Ok(());
                                    }

                                    // 获取记录的日志（快速获取，不持有锁太久）
                                    let logs = {
                                        let mut logs_guard = read_logs.lock().unwrap();
                                        logs_guard.drain(..).collect::<Vec<_>>()
                                    };

                                    // 总是添加到 block_logs，即使日志为空
                                    // 这样索引会被更新，表示这个块已经被处理过了
                                    block_logs.push((block_number, logs));

                                    // 修复：记录实际处理的块号（用于准确统计）
                                    actually_processed_blocks.push(block_number);
                                }

                                // 批量写入日志（减少锁竞争和文件I/O）
                                // 优先使用 mmap 模式，其次使用文件系统模式
                                if let Some(ref db) = mmap_log_db_clone {
                                    // mmap 日志模式：批量写入（零拷贝，适合大数据集）
                                    let mut db_guard = db.write().unwrap();
                                    // 直接传递引用，避免不必要的 clone
                                    db_guard.write_block_logs_batch(&block_logs)?;
                                } else if let Some(ref writer) = buffered_writer_clone {
                                    // 文件系统模式：使用缓冲写入
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

                                // 优先使用无锁 mmap reader（零拷贝，无锁，性能最好）
                                if let Some(ref reader) = mmap_log_reader_clone {
                                    let blocks_len = blocks.len();

                                    // 创建线程本地统计累加器（性能优化：避免频繁原子操作）
                                    let mut local_stats = LocalStatsAccumulator::new();

                                    // 获取所有需要的块号
                                    let block_numbers: Vec<u64> = blocks.iter()
                                        .map(|b| b.sealed_block().header().number)
                                        .collect();

                                    // 从 mmap 读取（零拷贝，无锁！）
                                    // 返回格式: Vec<(block_number, Option<data>)>，缺失的块返回 None
                                    let batch_data = reader.read_block_logs_batch(&block_numbers);

                                    // 性能优化：复用线程级别的 StateProvider（避免每个 task 都调用 history_by_block_number）
                                    // 在日志重放模式下，StateProvider 只用于 code_by_hash 查询
                                    // Bytecode 是不可变的，所以使用任何块号的 state provider 都可以
                                    let shared_state_provider: Arc<dyn reth_provider::StateProvider> =
                                        if let Some(ref sp) = thread_level_state_provider {
                                            sp.clone()
                                        } else {
                                            Arc::new(blockchain_db.history_by_block_number(task.start.saturating_sub(1))?)
                                        };

                                    // 为每个块创建 DbLoggedDatabase 并执行（零拷贝版本）
                                    // 对于缺失的块，block_db_pairs 中的值为 None
                                    let mut block_db_pairs: Vec<(u64, Option<DbLoggedDatabase<'_>>)> = Vec::with_capacity(blocks_len);

                                    for (bn, data_opt) in batch_data.iter() {
                                        if let Some(data) = data_opt {
                                            // 使用带缓存的版本，避免重复查询合约代码
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
                                            // 日志中缺失的块
                                            block_db_pairs.push((*bn, None));
                                        }
                                    }

                                    // 执行每个块
                                    // block_db_pairs 与 blocks 一一对应（顺序相同）
                                    // 性能优化：使用本地计数器，减少原子操作竞争（特别是在多 CCX CPU 上）
                                    let mut local_gas: u64 = 0;
                                    let mut local_txs: u64 = 0;

                                    let mut db_iter = block_db_pairs.into_iter();
                                    for block in blocks.iter() {
                                        let block_number = block.sealed_block().header().number;

                                        if let Some((db_block_number, db_opt)) = db_iter.next() {
                                            // 验证块号匹配（应该总是匹配，因为我们保持了顺序）
                                            if db_block_number != block_number {
                                                error!("Block number mismatch: expected {}, got {} (this should not happen)", block_number, db_block_number);
                                                continue;
                                            }

                                            if let Some(mut logged_db) = db_opt {
                                                // 使用日志数据执行块
                                                let executor = evm_config.batch_executor(&mut logged_db);
                                                if let Err(e) = executor.execute_batch(std::iter::once(block)) {
                                                    error!("Execution error for block {} using mmap DbLoggedDatabase: {}", block_number, e);
                                                }
                                                // 收集统计信息到本地累加器（零开销，无原子操作）
                                                local_stats.accumulate(&logged_db);
                                            } else {
                                                // 日志中缺失的块，使用数据库直接执行
                                                let db = StateProviderDatabase::new(
                                                    blockchain_db.history_by_block_number(block_number.saturating_sub(1))?
                                                );
                                                let executor = evm_config.batch_executor(db);
                                                if let Err(e) = executor.execute_batch(std::iter::once(block)) {
                                                    error!("Execution error for block {} using StateProviderDatabase: {}", block_number, e);
                                                }
                                            }

                                            // 累积到本地计数器（无原子操作）
                                            local_gas += block.sealed_block().header().gas_used;
                                            local_txs += block.sealed_block().body().transaction_count() as u64;
                                        }
                                    }

                                    // 批量更新共享计数器（每个 task 只更新一次，减少原子竞争）
                                    cumulative_gas.fetch_add(local_gas, Ordering::Relaxed);
                                    txs_counter.fetch_add(local_txs, Ordering::Relaxed);
                                    block_counter.fetch_add(blocks.len() as u64, Ordering::Relaxed);

                                    // 批量提交本地统计到全局（性能优化：只需3次原子操作，而非 N*3 次）
                                    if !local_stats.is_empty() {
                                        log_stats_clone.accumulate_batch(&local_stats);
                                    }

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
                                        if let Some(shared_data) = enable_in_memory_mode_clone
                                            .then_some(file_handle.in_memory_data.as_ref())
                                            .flatten()
                                        {
                                            // 直接从共享内存读取，避免复制
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
                                            Arc::new(blockchain_db.history_by_block_number(task.start.saturating_sub(1))?)
                                        }
                                    };

                                    // 性能优化：预先创建所有 LoggedDatabase 实例
                                    // 单线程模式：顺序处理，避免线程池开销
                                    // 多线程模式：使用 rayon 并行解压以充分利用 CPU

                                    // 性能优化：预分配 block_db_pairs 容量，减少重新分配
                                    let mut block_db_pairs: Vec<(u64, LoggedDatabase)> = Vec::with_capacity(blocks_len);

                                    // 性能优化：直接处理，避免创建中间 Vec
                                    // 按块顺序处理，保持执行顺序
                                    if single_thread {
                                        // 单线程模式：顺序处理，避免线程池和同步开销
                                        // 正常模式：从 block_compressed_data 读取
                                        for block in blocks.iter() {
                                            let block_number = block.sealed_block().header().number;

                                            // 获取压缩数据
                                            let compressed_data = match block_compressed_data.remove(&block_number) {
                                                Some(data) => data,
                                                None => {
                                                    error!("Block {} data not found in compressed data map", block_number);
                                                    continue;
                                                }
                                            };

                                            // 从压缩数据创建 LoggedDatabase（性能优化：零复制版本）
                                            // 直接传递 Vec<u8> 所有权，避免不必要的内存复制
                                            // v1.10.0修复：自适应数据格式处理（先尝试正确格式以保持性能，失败时回退）
                                            let is_uncompressed = enable_in_memory_mode_clone;
                                            match LoggedDatabase::new_from_compressed_buffer_owned(compressed_data.clone(), shared_state_provider.clone(), is_uncompressed) {
                                                Ok(db) => {
                                                    block_db_pairs.push((block_number, db));
                                                }
                                                Err(e) => {
                                                    // 如果创建失败，尝试用相反的格式标志重试
                                                    debug!("Failed with is_uncompressed={}, retrying with opposite flag for block {}: {}", is_uncompressed, block_number, e);
                                                    match LoggedDatabase::new_from_compressed_buffer_owned(compressed_data, shared_state_provider.clone(), !is_uncompressed) {
                                                        Ok(db) => {
                                                            block_db_pairs.push((block_number, db));
                                                        }
                                                        Err(e2) => {
                                                            error!("Failed to create LoggedDatabase for block {} with both format flags: {} / {}", block_number, e, e2);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        // v1.10.0修复：串行模式（StateProvider线程安全问题）

                                        // 准备处理的块数据：Vec<(block_number, compressed_data, block_index)>
                                        // 性能优化：预分配容量
                                        let mut block_decompress_tasks: Vec<(u64, Vec<u8>, usize)> = blocks.iter()
                                            .enumerate()
                                            .filter_map(|(block_idx, block)| {
                                                let block_number = block.sealed_block().header().number;
                                                block_compressed_data.remove(&block_number).map(|data| (block_number, data, block_idx))
                                            })
                                            .collect();

                                        // v1.10.0修复：串行创建LoggedDatabase（避免StateProvider线程安全问题）
                                        // StateProvider不再是Sync，无法在rayon线程中使用
                                        // 解压操作交给ThreadLocalLogCache内部处理，保证数据格式正确

                                        // 按块号排序以保持顺序
                                        block_decompress_tasks.sort_by_key(|(_, _, idx)| *idx);

                                        // 串行创建所有LoggedDatabase
                                        let is_uncompressed = enable_in_memory_mode_clone;
                                        for (block_number, compressed_data, _) in block_decompress_tasks {
                                            // v1.10.0修复：自适应数据格式处理（先尝试正确格式以保持性能，失败时回退）
                                            match LoggedDatabase::new_from_compressed_buffer_owned(
                                                compressed_data.clone(),
                                                shared_state_provider.clone(),
                                                is_uncompressed
                                            ) {
                                                Ok(db) => block_db_pairs.push((block_number, db)),
                                                Err(e) => {
                                                    // 如果创建失败，尝试用相反的格式标志重试
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
                                    }

                                    // 第二步：按顺序执行所有块
                                    // block_db_pairs 可能比 blocks 少（缺失的块不在日志中）
                                    // 使用 HashMap 来快速查找，对于缺失的块使用数据库直接执行
                                    let mut block_db_map: std::collections::HashMap<u64, LoggedDatabase> =
                                        block_db_pairs.into_iter().collect();

                                    for block in blocks.iter() {
                                        // 性能优化：移除热路径上的日志检查，直接判断停止标志
                                        if should_stop_worker.load(Ordering::Relaxed) {
                                            return Ok(());
                                        }

                                        let block_number = block.sealed_block().header().number;

                                        // 查找日志数据库，如果存在则使用日志执行，否则使用数据库直接执行
                                        // 使用 remove 取出所有权
                                        let mut executed_successfully = false;
                                        if let Some(logged_db) = block_db_map.remove(&block_number) {
                                            // 使用日志数据执行块
                                            // v1.10.0修复：捕获LoggedDatabase执行错误，自动回退到数据库执行
                                            let executor = evm_config.batch_executor(logged_db);
                                            match executor.execute(block) {
                                                Ok(_output) => {
                                                    executed_successfully = true;
                                                }
                                                Err(e) => {
                                                    warn!("LoggedDatabase execution failed for block {}, falling back to database: {}", block_number, e);
                                                }
                                            }
                                        }

                                        if !executed_successfully {
                                            // 日志中缺失的块或日志执行失败，使用数据库直接执行
                                            // v1.10.0修复：捕获数据库执行错误，避免StateProvider多线程问题导致panic
                                            match (|| -> eyre::Result<()> {
                                                let state_provider = blockchain_db.history_by_block_number(block_number.saturating_sub(1))?;
                                                let db = CachedStateProviderDatabase::new(
                                                    &state_provider,
                                                    Some(bytecode_cache_clone.clone()),
                                                );
                                                let executor = evm_config.batch_executor(db);
                                                let _output = executor.execute(block)?;
                                                drop(state_provider);
                                                Ok(())
                                            })() {
                                                Ok(_) => {},
                                                Err(e) => {
                                                    error!("Failed to execute block {} with database fallback: {}. Skipping this block.", block_number, e);
                                                }
                                            }
                                        }

                                        // 性能优化：移除热路径上的日志检查
                                        if should_stop_worker.load(Ordering::Relaxed) {
                                            return Ok(());
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
                                // Windows 修复：使用 CachedStateProviderDatabase 减少 MDBX 访问
                                // 通过缓存 bytecode/account/storage 显著减少数据库查询
                                // 这是 --use-log 模式能稳定运行的关键原因之一
                                if verify_blocks {
                                    // One block at a time, each against the state
                                    // its parent left - the same view the witness
                                    // recorder uses - so a mismatch names a block.
                                    for block in blocks.iter() {
                                        let number = block.sealed_block().header().number;
                                        let state_provider = blockchain_db
                                            .history_by_block_number(number.saturating_sub(1))?;
                                        let db = CachedStateProviderDatabase::new(
                                            &state_provider,
                                            Some(bytecode_cache_clone.clone()),
                                        );
                                        // Wrapped so a failure can report the
                                        // values the execution actually read,
                                        // which is what a divergence is about.
                                        let (witness_db, witness_stream) =
                                            WitnessDatabase::new(db);
                                        let executor = evm_config.batch_executor(witness_db);
                                        let output = executor.execute(block)?;
                                        let recorded = witness_stream.take();
                                        if let Err(error) = verify_against_header(
                                            &chain_spec,
                                            block,
                                            &output.result,
                                            "executed",
                                        ) {
                                            let path = std::path::Path::new(".")
                                                .join(format!("witness-{number}.bin"));
                                            std::fs::write(&path, &recorded).ok();
                                            error!(
                                                "block {} read {} witness bytes, written to {}",
                                                number,
                                                recorded.len(),
                                                path.display()
                                            );
                                            return Err(error.wrap_err(format!("block {number}")))
                                        }
                                    }
                                } else {
                                    let state_provider =
                                        blockchain_db.history_by_block_number(task.start - 1)?;
                                    let db = CachedStateProviderDatabase::new(
                                        &state_provider,
                                        Some(bytecode_cache_clone.clone()),
                                    );
                                    let executor = evm_config.batch_executor(db);
                                    let _execute_result = executor.execute_batch(blocks.iter())?;
                                    // 显式释放 state_provider，减少 MDBX mmap 压力
                                    drop(state_provider);
                                }
                            }

                            // 通用统计代码（仅用于非 use_log 模式）
                            // use_log 模式的统计已在各自分支中完成
                            if !use_log_enabled {
                                let _start = Instant::now(); // 保留用于将来的性能统计
                                let mut step_cumulative_gas: u64 = 0;
                                let mut step_txs_counter: usize = 0;

                                // 只统计实际处理的块（不包括已存在的块，用于补齐模式）
                                // 修复：使用 actually_processed_blocks 而不是 blocks_to_process_numbers
                                // 避免时序竞态导致的虚假统计（预先计算vs实际执行的差异）
                                if let Some(_log_dir) = &log_dir_clone {
                                    if log_block_enabled {
                                        // --log-block on 模式：只统计实际处理的块
                                        let actually_processed_set: std::collections::HashSet<u64> =
                                            actually_processed_blocks.iter().cloned().collect();
                                        blocks.iter()
                                            .filter(|block| {
                                                let block_num = block.sealed_block().header().number;
                                                actually_processed_set.contains(&block_num)
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
                                // 修复：使用 actually_processed_blocks.len() 而不是 blocks_to_process_numbers.len()
                                let processed_count = if let Some(_log_dir) = &log_dir_clone {
                                    if log_block_enabled {
                                        actually_processed_blocks.len() as u64
                                    } else {
                                        blocks_to_process_numbers.len() as u64
                                    }
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
                            if use_log_enabled || witness_replay_thread {
                                // use_log / witness 回放模式：刷新线程级 state_provider
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

                if let Some(shared) = contract_heat_clone.as_ref() {
                    let mut total = shared.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    heat::merge(&mut total, &heat_local);
                }

                // Replay never reads state from the database, so a failure here
                // means the witness no longer matches the execution it was
                // recorded from. Those blocks were skipped, not quietly patched.
                let failures = witness_failures.load(Ordering::Relaxed);
                if failures > 0 {
                    error!(
                        thread_id = ?thread_id,
                        failures,
                        "Blocks skipped because their witness did not match"
                    );
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
            // Flush whatever contiguous run made it through; the trailing
            // partial batch is rewritten when the next run resumes.
            if let Some(ref sink) = witness_sink {
                match sink.finish() {
                    Ok(items) => info!(items, "Witness freezer flushed on Ctrl+C"),
                    Err(error) => {
                        error!("Failed to flush witness freezer on Ctrl+C: {}", error)
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

            // 正常结束时刷新 mmap 日志数据库
            if let Some(ref mmap_db) = mmap_log_db {
                if let Ok(mut db_guard) = mmap_db.write() {
                    let pending_count = db_guard.pending_count();
                    if pending_count > 0 {
                        info!(
                            "Flushing {} pending blocks to mmap log database...",
                            pending_count
                        );
                        if let Err(e) = db_guard.flush() {
                            error!("Failed to flush mmap log database: {}", e);
                        } else {
                            info!("Mmap log database flushed successfully");
                        }
                    }
                }
            }

            if let Some(ref overrides) = state_overrides {
                overrides.report();
            }

            // Write the trailing partial batch and report what is on disk.
            if let Some(ref sink) = witness_sink {
                let items = sink.finish()?;
                info!(items, next_block = sink.next_block(), "Wrote N42 witness freezer");
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
                    let state_provider = blockchain_db
                        .history_by_block_number(log_block.saturating_sub(1))?;
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
                        warn!(
                            "LoggingDatabase: executing block {} to record access order (txs: {})",
                            log_block,
                            block.body().transaction_count()
                        );
                        // 执行该块，LoggingDatabase 会在读取时自动记录日志
                        // 使用 execute 而不是 execute_batch，与其他代码保持一致
                        let _output = executor.execute(block)?;

                        // 获取记录的日志（按正常访问顺序）
                        let logs = read_logs.lock().unwrap();
                        info!("Recorded {} log entries during execution", logs.len());
                        write_read_logs(log_block, &logs)?;
                        write_read_logs_binary(
                            log_block,
                            &logs,
                            &self.compression_algorithm,
                            log_dir,
                            &existing_blocks,
                            self.single_thread,
                            None,
                        )?;
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
                let (log_path, block_number) = if let Ok(block_num) =
                    log_path_or_block.parse::<u64>()
                {
                    // 如果传递的是数字，自动构建文件路径
                    let path = format!("block_{}_reads.bin", block_num);
                    (path, block_num)
                } else {
                    // 如果传递的是路径，尝试从文件名提取块号
                    let path = log_path_or_block.clone();
                    let block_num = self
                        .log_block
                        .as_ref()
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or_else(|| {
                            // 尝试从文件名提取块号
                            if let Some(name) = std::path::Path::new(&path).file_stem() {
                                if let Some(name_str) = name.to_str() {
                                    if let Some(block_str) = name_str.strip_prefix("block_") {
                                        if let Some(block_num_str) =
                                            block_str.strip_suffix("_reads")
                                        {
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
                    read_log_compressed_data(
                        &block_number.to_string(),
                        Some(log_dir_ref),
                        Some(&cached_index_entries),
                        None,
                        enable_in_memory_mode,
                    )?
                } else {
                    // 从单个文件读取压缩数据（不使用索引）
                    read_log_compressed_data(&log_path, None, None, None, enable_in_memory_mode)?
                };

                // 创建 state provider
                let blockchain_db = BlockchainProvider::new(provider_factory.clone())?;
                let state_provider = blockchain_db
                    .history_by_block_number(block_number.saturating_sub(1))?;

                // 从压缩数据创建 LoggedDatabase（性能优化：零复制版本）
                // 直接传递 Vec<u8> 所有权，避免不必要的内存复制
                // v1.10.0修复：自适应数据格式处理（先尝试正确格式以保持性能，失败时回退）
                let state_provider_arc = Arc::new(state_provider);
                let logged_db = match LoggedDatabase::new_from_compressed_buffer_owned(
                    compressed_data.clone(),
                    state_provider_arc.clone(),
                    enable_in_memory_mode,
                ) {
                    Ok(db) => db,
                    Err(e) => {
                        debug!(
                            "Failed with is_uncompressed={}, retrying with opposite flag: {}",
                            enable_in_memory_mode, e
                        );
                        LoggedDatabase::new_from_compressed_buffer_owned(
                            compressed_data,
                            state_provider_arc,
                            !enable_in_memory_mode,
                        )
                        .map_err(|e2| {
                            eyre::eyre!(
                                "Failed to create LoggedDatabase with both format flags: {} / {}",
                                e,
                                e2
                            )
                        })?
                    }
                };

                info!(
                    "Loaded log data from {} (using thread-local cache)",
                    &log_path
                );

                let evm_config = EthEvmConfig::ethereum(provider_factory.chain_spec());

                let blocks = blockchain_db
                    .block_with_senders_range(block_number..=block_number)
                    .map_err(|e| eyre::eyre!("failed to load block {}: {}", block_number, e))?;

                if let Some(block) = blocks.first() {
                    warn!(
                        "LoggedDatabase: executing block {} using log file {} (txs: {})",
                        block_number,
                        log_path,
                        block.body().transaction_count()
                    );
                    // 执行该块，LoggedDatabase 会从日志文件读取数据
                    // 为每个块创建新的 executor，避免状态复用或缓存问题
                    let executor = evm_config.batch_executor(logged_db);
                    // 使用 execute 而不是 execute_batch，与记录日志时保持一致
                    let _output = executor.execute(block)?;
                    // 注意：check_unaccessed_storages 会在 LoggedDatabase 析构时自动调用
                    info!(
                        "Block {} executed successfully using log file",
                        block_number
                    );
                }
            }
            // 如果 use_log == "on"，日志执行已经在并行执行线程中完成，这里不需要额外处理
        }

        if should_stop.load(Ordering::Relaxed) {
            info!("EVM command stopped by user");
        } else {
            thread::sleep(Duration::from_secs(1));
            if let Some(resolver) = code_resolver.as_ref() {
                info!(?resolver, "Code resolution");
            }
            if let (Some(path), Some(shared)) = (self.contract_heat.as_deref(), contract_heat.as_ref()) {
                let total = shared.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                let calls: u64 = total.values().map(|heat| heat.calls).sum();
                heat::write_csv(path, &total)?;
                info!(contracts = total.len(), calls, path = %path.display(), "Contract heat written");
            }
            #[cfg(feature = "jit")]
            if self.jit.enabled {
                let stats = shared_evm_config.executor_factory.evm_factory().backend().stats();
                info!(
                    hits = stats.lookup_hits,
                    misses = stats.lookup_misses,
                    resident = stats.resident_entries,
                    compiled = stats.compilations_succeeded,
                    failed = stats.compilations_failed,
                    code_mib = stats.jit_code_bytes / (1024 * 1024),
                    dropped_events = stats.events_dropped,
                    "JIT"
                );
            }
            info!("EVM command completed successfully");
        }

        // 显示日志执行统计（仅在use_log模式下）
        if use_log_enabled {
            log_stats.print();
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
