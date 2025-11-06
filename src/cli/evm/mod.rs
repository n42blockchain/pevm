//! `reth evm` command.

use clap::Parser;
use reth_chainspec::ChainSpec;
use reth_cli::chainspec::ChainSpecParser;
use reth_cli_commands::common::CliNodeTypes;
use reth_cli_runner::CliContext;
use reth_ethereum_primitives::EthPrimitives;
use reth_node_ethereum::EthEngineTypes;

use tracing::{info, debug, error, trace};
use inline_colorization::*;
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
}

struct Task {
    start: u64,
    end: u64,
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

        let _consensus: Arc<dyn FullConsensus<EthPrimitives, Error = ConsensusError>> =
            Arc::new(EthBeaconConsensus::new(provider_factory.chain_spec()));

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
                        info!(
                            "Execution bn={} txs={} b/s={} TPS={color_green}{}{color_reset} Ggas/s={color_green}{}{color_reset} time={:.1} totalgas={}", current_block_counter,
                            current_txs_counter,
                            diff_block,
                            diff_txs,
                            gas_throughput_str,
                            total_seconds,
                            current_cumulative_gas,
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

            let provider_factory = provider_factory.clone();
            let blockchain_db = blockchain_db.clone();

            threads.push(thread::spawn(move || {
                let thread_id = thread::current().id();
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
                            // Get parent header for total difficulty calculation
                            let parent_header = blockchain_db.header_by_number(task.start - 1)?
                                .ok_or_else(|| ProviderError::HeaderNotFound(task.start.into()))?;
                            // In reth 1.9.0, we need to calculate total difficulty from the difficulty field
                            // Start with parent's difficulty (which is the block's difficulty)
                            let mut td = parent_header.difficulty;

                            let db = StateProviderDatabase::new(blockchain_db.history_by_block_number(task.start - 1)?);
                            let evm_config = EthEvmConfig::ethereum(provider_factory.chain_spec());
                            let executor = evm_config.batch_executor(db);
                            let blocks = blockchain_db.block_with_senders_range(task.start..=task.end).unwrap();

                            let start = Instant::now();
                            let mut step_cumulative_gas: u64 = 0;
                            let mut step_txs_counter: usize = 0;

                            let execute_result = executor.execute_batch(blocks.iter())?;
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
