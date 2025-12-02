//! `reth evm` command.

use clap::Parser;
use reth_chainspec::ChainSpec;
use reth_cli::chainspec::ChainSpecParser;
use reth_cli_commands::common::CliNodeTypes;
use reth_cli_runner::CliContext;
use reth_ethereum_primitives::EthPrimitives;
use reth_node_ethereum::EthEngineTypes;

use tracing::{info, debug, error, trace};
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
use std::io::Write;
use alloy_primitives::{Address, B256, U256};
use crate::revm::Database;
use crate::revm::state::{AccountInfo, Bytecode};

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
    /// Block number to log reads for (generates a log file with account and storage reads)
    #[arg(long, alias = "log-block")]
    log_block: Option<u64>,
}

struct Task {
    start: u64,
    end: u64,
}

/// Read log entry types - 按访问顺序记录
#[derive(Debug, Clone)]
enum ReadLogEntry {
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

impl<DB: crate::revm::Database> crate::revm::Database for LoggingDatabase<DB> {
    type Error = <DB as Database>::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, <DB as Database>::Error> {
        let result = self.inner.basic(address)?;
        
        // 记录账户读取：使用拦截到的 AccountInfo 数据，编码为与 PlainState 一致的 RLP 格式
        if let Some(account_info) = &result {
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
            
            let storage_root = B256::ZERO;
            let code_hash = account_info.code_hash();
            
            // 决定哪些字段需要编码
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
            
            let mut logs = self.read_logs.lock().unwrap();
            logs.push(ReadLogEntry::Account {
                address,
                data: buf,
            });
        }
        
        Ok(result)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, <DB as Database>::Error> {
        // 代码读取忽略（按用户要求）
        self.inner.code_by_hash(code_hash)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, <DB as Database>::Error> {
        let result = self.inner.storage(address, index)?;
        
        // 记录存储读取：使用拦截到的存储值，编码为 RLP
        let key_bytes = index.to_be_bytes::<32>();
        let key = B256::from_slice(&key_bytes);
        
        // 将存储值编码为 RLP
        use alloy_rlp::Encodable;
        let mut buf = Vec::new();
        result.encode(&mut buf);
        
        let mut logs = self.read_logs.lock().unwrap();
        logs.push(ReadLogEntry::Storage {
            address,
            key,
            data: buf,
        });
        
        Ok(result)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, <DB as Database>::Error> {
        self.inner.block_hash(number)
    }
}

/// 写入读取日志到文件
fn write_read_logs(block_number: u64, read_logs: &[ReadLogEntry]) -> eyre::Result<()> {
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

        let Environment { provider_factory, .. } = self.env.init::<N>(AccessRights::RO)?;

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

        // 创建任务池
        let mut tasks = VecDeque::new();
        let mut current_start = self.begin_number;
        while current_start <= self.end_number {
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

        // 获取 CPU 核心数，减一作为线程数
        let thread_count = self.get_cpu_count() * 2 - 1;
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

            // 在 v1.8.4 中，共享 blockchain_db 也能正常工作
            threads.push(thread::spawn(move || {
                let thread_id = thread::current().id();
                
                // 预先创建 EVM 配置，避免在循环中重复创建
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
                            let db = StateProviderDatabase::new(blockchain_db.history_by_block_number(task.start - 1)?);
                            let executor = evm_config.batch_executor(db);
                            let blocks = blockchain_db.block_with_senders_range(task.start..=task.end).unwrap();

                            let execute_result = executor.execute_batch(blocks.iter())?;

                            let start = Instant::now();
                            let mut step_cumulative_gas: u64 = 0;
                            let mut step_txs_counter: usize = 0;

                            trace!(target: "exex::evm", ?execute_result);
                            blocks.iter().for_each(|block| {
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
                                *block_counter.lock().unwrap() += blocks.len() as u64;
                            }
                            {
                                *txs_counter.lock().unwrap() += step_txs_counter as u64;
                            }

                            debug!(
                                target: "exex::evm",
                                task_start = task.start,
                                task_end = task.end,
                                txs = step_txs_counter,
                                blocks = blocks.len(),
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

        // 如果指定了 log_block，按正常访问顺序生成该块的 account/storage 日志
        if let Some(log_block) = self.log_block {
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
                // 执行该块，LoggingDatabase 会在读取时自动记录日志
                executor.execute(block)?;
                
                // 获取记录的日志（按正常访问顺序）
                let logs = read_logs.lock().unwrap();
                write_read_logs(log_block, &logs)?;
            }
        }

        if should_stop.load(Ordering::Relaxed) {
            info!("EVM command stopped by user");
        } else {
            thread::sleep(Duration::from_secs(1));
            info!("EVM command completed successfully");
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
