//! `reth evm` command.

use clap::Parser;
use reth_chainspec::ChainSpec;
use reth_cli::chainspec::ChainSpecParser;
use reth_cli_commands::common::CliNodeTypes;
use reth_cli_runner::CliContext;
use reth_ethereum_primitives::EthPrimitives;
use reth_node_ethereum::EthEngineTypes;

use tracing::{info, debug, error, trace, warn};
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
use std::io::{Write, Read, BufWriter};
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
    /// Path to binary log file to use for reading account and storage data (instead of database)
    #[arg(long, alias = "use-log")]
    use_log: Option<String>,
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

impl<DB: crate::revm::Database> crate::revm::Database for LoggingDatabase<DB> {
    type Error = <DB as Database>::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, <DB as Database>::Error> {
        // 先记录访问顺序（在调用 inner 之前），确保顺序一致
        let mut logs = self.read_logs.lock().unwrap();
        let log_index = logs.len();
        
        let result = self.inner.basic(address)?;
        
        // 记录账户读取：使用拦截到的 AccountInfo 数据，编码为与 PlainState 一致的 RLP 格式
        // 即使账户不存在，也要记录一个条目以保持顺序一致
        if let Some(account_info) = &result {
            debug!("LoggingDatabase::basic({}) -> Some(nonce={}, balance={}) at log index {}", 
                address, account_info.nonce, account_info.balance, log_index);
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
            
            logs.push(ReadLogEntry::Account {
                address,
                data: buf,
            });
        } else {
            // 账户不存在，记录一个空的 RLP 列表（表示账户不存在）
            // RLP 空列表: 0xc0
            debug!("LoggingDatabase::basic({}) -> None (empty account) at log index {}", address, log_index);
            logs.push(ReadLogEntry::Account {
                address,
                data: vec![0xc0],
            });
        }
        
        Ok(result)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, <DB as Database>::Error> {
        // 代码读取忽略（按用户要求），但我们需要记录它被调用了
        warn!("LoggingDatabase::code_by_hash({:x}) CALLED (RECORDING - code reads are ignored per user request)", code_hash);
        let result = self.inner.code_by_hash(code_hash)?;
        warn!("LoggingDatabase::code_by_hash({:x}) -> loaded {} bytes (RECORDED but not logged)", code_hash, result.len());
        Ok(result)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, <DB as Database>::Error> {
        // 先记录访问顺序（在调用 inner 之前），确保顺序一致
        let key_bytes = index.to_be_bytes::<32>();
        let key = B256::from_slice(&key_bytes);
        
        let result = self.inner.storage(address, index)?;
        
        // 记录存储读取：使用拦截到的存储值，编码为 RLP
        // 注意：index 是 U256，需要转换为 32 字节的 key
        
        // 将存储值编码为 RLP
        use alloy_rlp::Encodable;
        let mut buf = Vec::new();
        result.encode(&mut buf);
        
        let mut logs = self.read_logs.lock().unwrap();
        let log_index = logs.len();
        info!("LoggingDatabase::storage({}, index={}, key={:x}) -> {} at log index {} (RECORDED)", 
            address, index, key, result, log_index);
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

/// 写入读取日志到二进制文件（版本4：按顺序，无类型标记，格式：address + key_len + key + data_len + data）
pub(crate) fn write_read_logs_binary(block_number: u64, read_logs: &[ReadLogEntry]) -> eyre::Result<()> {
    let log_file_path = format!("block_{}_reads.bin", block_number);
    let mut log_file = BufWriter::new(File::create(&log_file_path)?);
    
    // 写入魔数和版本号
    log_file.write_all(b"PEVMLOG")?; // 魔数
    log_file.write_all(&4u32.to_le_bytes())?; // 版本号 4 = 按顺序，无类型标记
    
    // 写入条目数量
    log_file.write_all(&(read_logs.len() as u64).to_le_bytes())?;
    
    // 按照访问顺序写入日志（格式：address(20) + key_len(1) + key(0或32) + data_len(1) + data）
    // Account: key_len=0, Storage: key_len=32
    for entry in read_logs {
        match entry {
            ReadLogEntry::Account { address, data } => {
                // address (20 bytes)
                log_file.write_all(address.as_slice())?;
                // key_len = 0 (表示Account)
                log_file.write_all(&[0u8])?;
                // 数据长度（1字节，最大255）
                if data.len() > 255 {
                    return Err(eyre::eyre!("Account data too long: {} bytes (max 255)", data.len()));
                }
                log_file.write_all(&[data.len() as u8])?;
                // 数据
                log_file.write_all(data)?;
            }
            ReadLogEntry::Storage { address, key, data } => {
                // address (20 bytes)
                log_file.write_all(address.as_slice())?;
                // key_len = 32 (表示Storage)
                log_file.write_all(&[32u8])?;
                // key (32 bytes)
                log_file.write_all(key.as_slice())?;
                // 数据长度（1字节，最大255）
                if data.len() > 255 {
                    return Err(eyre::eyre!("Storage data too long: {} bytes (max 255)", data.len()));
                }
                log_file.write_all(&[data.len() as u8])?;
                // 数据
                log_file.write_all(data)?;
            }
        }
    }
    
    log_file.flush()?;
    info!("Binary log written to: {} ({} entries)", log_file_path, read_logs.len());
    Ok(())
}

/// 从二进制文件读取日志（版本4：按顺序，无类型标记）
pub(crate) fn read_read_logs_binary(log_path: &str) -> eyre::Result<Vec<ReadLogEntry>> {
    // 一次性读取整个文件
    let file_contents = std::fs::read(log_path)?;
    let mut cursor = std::io::Cursor::new(file_contents);
    
    // 读取魔数和版本号
    let mut magic = [0u8; 7];
    cursor.read_exact(&mut magic)?;
    if &magic != b"PEVMLOG" {
        return Err(eyre::eyre!("Invalid log file magic"));
    }
    
    let mut version = [0u8; 4];
    cursor.read_exact(&mut version)?;
    let version = u32::from_le_bytes(version);
    
    if version != 4 {
        return Err(eyre::eyre!("Unsupported log file version: {} (only version 4 is supported)", version));
    }
    
    // 读取条目数量
    let mut count_bytes = [0u8; 8];
    cursor.read_exact(&mut count_bytes)?;
    let count = u64::from_le_bytes(count_bytes);
    
    let mut entries = Vec::new();
    
    // 版本4格式：address(20) + key_len(1) + key(0或32) + data_len(1) + data
    info!("read_read_logs_binary: reading {} entries from binary log file", count);
    let mut account_count = 0;
    let mut storage_count = 0;
    
    for entry_index in 0..count {
        // 读取 address (20 bytes)
        let mut address_bytes = [0u8; 20];
        cursor.read_exact(&mut address_bytes)?;
        let address = Address::from(address_bytes);
        
        // 读取 key_len (1 byte)
        let mut key_len_bytes = [0u8; 1];
        cursor.read_exact(&mut key_len_bytes)?;
        let key_len = key_len_bytes[0] as usize;
        
        // 读取 key (如果 key_len > 0)
        let key = if key_len == 32 {
            let mut key_bytes = [0u8; 32];
            cursor.read_exact(&mut key_bytes)?;
            B256::from_slice(&key_bytes)
        } else if key_len == 0 {
            B256::ZERO // Account 条目，key 不使用
        } else {
            return Err(eyre::eyre!("Invalid key_len: {} (expected 0 or 32)", key_len));
        };
        
        // 读取数据长度（1字节）
        let mut data_len_bytes = [0u8; 1];
        cursor.read_exact(&mut data_len_bytes)?;
        let data_len = data_len_bytes[0] as usize;
        
        // 读取数据
        let mut data = vec![0u8; data_len];
        cursor.read_exact(&mut data)?;
        
        // 根据 key_len 判断类型：0 = Account, 32 = Storage
        if key_len == 0 {
            entries.push(ReadLogEntry::Account { address, data });
            account_count += 1;
            if entry_index < 10 || entry_index % 100 == 0 {
                debug!("read_read_logs_binary: entry {} = Account({})", entry_index, address);
            }
        } else {
            entries.push(ReadLogEntry::Storage { address, key, data });
            storage_count += 1;
            if entry_index < 10 || entry_index % 100 == 0 {
                debug!("read_read_logs_binary: entry {} = Storage({}, key={:x})", entry_index, address, key);
            }
        }
    }
    
    info!("read_read_logs_binary: loaded {} accounts, {} storages", account_count, storage_count);
    
    Ok(entries)
}

/// 从日志文件读取数据的 Database（使用 HashMap 进行随机访问）
/// 现在仅用于记录日志，数据仍从原数据库读取
pub(crate) struct LoggedDatabase {
    accounts: std::collections::HashMap<Address, Vec<u8>>, // 账户数据（RLP编码）- 仅用于记录日志
    storages: std::collections::HashMap<(Address, B256), Vec<u8>>, // 存储数据（RLP编码）- 仅用于记录日志
    state_provider: Box<dyn reth_provider::StateProvider>, // 用于实际数据读取
    accessed_storages: std::collections::HashSet<(Address, B256)>, // 记录哪些 storage 被访问了
}

impl std::fmt::Debug for LoggedDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoggedDatabase")
            .field("accounts_count", &self.accounts.len())
            .field("storages_count", &self.storages.len())
            .field("accessed_storages_count", &self.accessed_storages.len())
            .finish_non_exhaustive()
    }
}

impl Drop for LoggedDatabase {
    fn drop(&mut self) {
        // 在析构时检查哪些 storage 没有被访问
        self.check_unaccessed_storages();
    }
}

impl LoggedDatabase {
    fn new(log_entries: Vec<ReadLogEntry>, state_provider: Box<dyn reth_provider::StateProvider>) -> Self {
        let mut accounts = std::collections::HashMap::new();
        let mut storages = std::collections::HashMap::new();
        
        info!("LoggedDatabase::new: processing {} log entries (for logging only, data will be read from database)", log_entries.len());
        let mut account_count = 0;
        let mut storage_count = 0;
        
        for (index, entry) in log_entries.iter().enumerate() {
            match entry {
                ReadLogEntry::Account { address, data } => {
                    accounts.insert(*address, data.clone());
                    account_count += 1;
                    if index < 10 || (index % 100 == 0 && index < 1000) {
                        info!("LoggedDatabase::new: entry {} = Account({})", index, address);
                    }
                }
                ReadLogEntry::Storage { address, key, data } => {
                    storages.insert((*address, *key), data.clone());
                    storage_count += 1;
                    if index < 10 || (index % 100 == 0 && index < 1000) {
                        info!("LoggedDatabase::new: entry {} = Storage({}, key={:x})", index, address, key);
                    }
                }
            }
        }
        
        info!("LoggedDatabase::new: loaded {} accounts, {} storages (for logging only)", account_count, storage_count);
        
        Self {
            accounts,
            storages,
            state_provider,
            accessed_storages: std::collections::HashSet::new(),
        }
    }
    
    /// 检查哪些 storage 没有被访问（用于调试）
    fn check_unaccessed_storages(&self) {
        let unaccessed: Vec<_> = self.storages.keys()
            .filter(|k| !self.accessed_storages.contains(k))
            .collect();
        if !unaccessed.is_empty() {
            warn!("LoggedDatabase: {} storages were NOT accessed during execution:", unaccessed.len());
            warn!("Total storages in log: {}, Total accessed: {}", 
                self.storages.len(), self.accessed_storages.len());
            
            // 按地址分组显示
            use std::collections::HashMap;
            let mut by_address: HashMap<Address, Vec<&B256>> = HashMap::new();
            for (addr, key) in unaccessed.iter() {
                by_address.entry(*addr).or_insert_with(Vec::new).push(key);
            }
            
            for (addr, keys) in by_address.iter() {
                warn!("  Address {} has {} unaccessed storages:", addr, keys.len());
                
                // 检查这个地址有多少个 storage 被访问了
                let accessed_for_address: Vec<_> = self.accessed_storages.iter()
                    .filter(|(a, _)| *a == *addr)
                    .collect();
                warn!("    (This address has {} accessed storages out of {} total)", 
                    accessed_for_address.len(), 
                    self.storages.keys().filter(|(a, _)| *a == *addr).count());
                
                for key in keys.iter().take(5) {
                    // 尝试将 key 转换为 U256 index 以便调试
                    let index = U256::from_be_slice(key.as_slice());
                    warn!("    - key={:x} (index={})", key, index);
                }
                if keys.len() > 5 {
                    warn!("    ... and {} more", keys.len() - 5);
                }
                
                // 检查这个地址的账户信息
                if let Some(account_rlp) = self.accounts.get(addr) {
                    if account_rlp.as_slice() != [0xc0] {
                        if let Ok(account_info) = LoggedDatabase::decode_account_rlp(account_rlp) {
                            warn!("    Account info: nonce={}, balance={}, code_hash={:x}", 
                                account_info.nonce, account_info.balance, account_info.code_hash());
                            
                            // 检查 code_hash 是否是空代码
                            let empty_code_hash = B256::from_slice(&[
                                0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c,
                                0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
                                0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b,
                                0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
                            ]);
                            if account_info.code_hash() == empty_code_hash {
                                warn!("    WARNING: code_hash is empty code hash - this account has no code!");
                            } else {
                                warn!("    INFO: code_hash is NOT empty - code should be loaded and executed");
                            }
                        }
                    } else {
                        warn!("    Account is empty (None)");
                    }
                } else {
                    warn!("    Account not found in log");
                }
            }
        } else {
            info!("LoggedDatabase: All {} storages were accessed", self.storages.len());
        }
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
        // 首先尝试从日志文件读取数据（使用保存的文件数据执行）
        if let Some(rlp_data) = self.accounts.get(&address) {
            // 检查是否是空账户标记（RLP 空列表 0xc0）
            if rlp_data.as_slice() == [0xc0] {
                // 日志文件说账户不存在，使用日志文件的数据（返回 None）
                // 对比数据库中的数据
                let mut inner_db = StateProviderDatabase::new(self.state_provider.as_ref());
                let db_result = inner_db.basic(address)?;
                if db_result.is_some() {
                    warn!("LoggedDatabase::basic({}) MISMATCH (using None from log file): log file says None, but database has Some(nonce={}, balance={}, code_hash={:x})", 
                        address, db_result.as_ref().unwrap().nonce, db_result.as_ref().unwrap().balance, db_result.as_ref().unwrap().code_hash());
                } else {
                    info!("LoggedDatabase::basic({}) MATCH: both log file and database return None", address);
                }
                return Ok(None);
            } else {
                // 从日志文件读取的数据解码
                match LoggedDatabase::decode_account_rlp(rlp_data) {
                    Ok(mut logged_account_info) => {
                        // 使用日志文件的数据（这样 EVM 会使用日志文件中的 code_hash，从而触发 code_by_hash）
                        // 对比数据库中的数据
                        let mut inner_db = StateProviderDatabase::new(self.state_provider.as_ref());
                        let db_result = inner_db.basic(address)?;
                        
                        if let Some(db_account_info) = &db_result {
                            // 对比 nonce, balance, code_hash
                            let nonce_match = logged_account_info.nonce == db_account_info.nonce;
                            let balance_match = logged_account_info.balance == db_account_info.balance;
                            let code_hash_match = logged_account_info.code_hash() == db_account_info.code_hash();
                            
                            // 对比 bytecode（通过 code_hash 获取）
                            // 只要不是 empty_code_hash，就需要对比 bytecode 前 8 个字节
                            let empty_code_hash = B256::from_slice(&[
                                0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c,
                                0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
                                0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b,
                                0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
                            ]);
                            
                            let logged_code_hash = logged_account_info.code_hash();
                            let db_code_hash = db_account_info.code_hash();
                            
                            // 确保 logged_account_info 包含实际的 bytecode（这样 revm 会调用 code_by_hash / 或已有代码可执行）
                            if logged_code_hash != empty_code_hash {
                                match self.code_by_hash(logged_code_hash) {
                                    Ok(code) => {
                                        logged_account_info.code = Some(code.clone());
                                    }
                                    Err(err) => {
                                        warn!(
                                            "LoggedDatabase::basic({}) failed to load bytecode for log code_hash {:x}: {}",
                                            address, logged_code_hash, err
                                        );
                                    }
                                }
                            }
                            
                            let bytecode_match = if logged_code_hash == empty_code_hash && db_code_hash == empty_code_hash {
                                // 两者都是空 code_hash，认为匹配（都没有 bytecode）
                                true
                            } else {
                                // 只要有一个不是 empty_code_hash，就需要对比 bytecode 前 8 个字节
                                let mut inner_db = StateProviderDatabase::new(self.state_provider.as_ref());
                                let logged_bytecode = if logged_code_hash != empty_code_hash {
                                    inner_db.code_by_hash(logged_code_hash).ok()
                                } else {
                                    None
                                };
                                let db_bytecode = if db_code_hash != empty_code_hash {
                                    inner_db.code_by_hash(db_code_hash).ok()
                                } else {
                                    None
                                };
                                
                                match (logged_bytecode, db_bytecode) {
                                    (Some(logged), Some(db)) => {
                                        // 对比 bytecode 的前 8 个字节
                                        let logged_bytes = logged.bytes();
                                        let db_bytes = db.bytes();
                                        let logged_prefix = logged_bytes.get(0..8).unwrap_or(&[]);
                                        let db_prefix = db_bytes.get(0..8).unwrap_or(&[]);
                                        logged_prefix == db_prefix
                                    }
                                    (None, None) => {
                                        // 两者都没有 bytecode（但 code_hash 不是 empty_code_hash，可能是加载失败）
                                        // 如果 code_hash 相同，认为匹配；否则不匹配
                                        logged_code_hash == db_code_hash
                                    }
                                    _ => false, // 一个有，一个没有
                                }
                            };
                            
                            // 如果 code_hash 不是 empty_code_hash，显示两个 code_hash 和对应的 bytecode 前 8 个字节
                            let logged_code_hash = logged_account_info.code_hash();
                            let db_code_hash = db_account_info.code_hash();
                            let empty_code_hash = B256::from_slice(&[
                                0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c,
                                0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
                                0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b,
                                0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
                            ]);
                            
                            if logged_code_hash != empty_code_hash || db_code_hash != empty_code_hash {
                                // 获取 bytecode 前 8 个字节
                                let mut inner_db = StateProviderDatabase::new(self.state_provider.as_ref());
                                let logged_prefix = if logged_code_hash != empty_code_hash {
                                    inner_db.code_by_hash(logged_code_hash).ok()
                                        .and_then(|b| {
                                            let bytes = b.bytes();
                                            bytes.get(0..8).map(|s| hex::encode(s))
                                        })
                                        .unwrap_or_else(|| "N/A (failed to load)".to_string())
                                } else {
                                    "empty".to_string()
                                };
                                
                                let db_prefix = if db_code_hash != empty_code_hash {
                                    inner_db.code_by_hash(db_code_hash).ok()
                                        .and_then(|b| {
                                            let bytes = b.bytes();
                                            bytes.get(0..8).map(|s| hex::encode(s))
                                        })
                                        .unwrap_or_else(|| "N/A (failed to load)".to_string())
                                } else {
                                    "empty".to_string()
                                };
                                
                                if nonce_match && balance_match && code_hash_match && bytecode_match {
                                    info!("LoggedDatabase::basic({}) MATCH (using log file data): nonce={}, balance={}, code_hash={:x}, bytecode match", 
                                        address, logged_account_info.nonce, logged_account_info.balance, logged_account_info.code_hash());
                                    info!("  bytecode (first 8 bytes): log code_hash={:x} (prefix=0x{}), db code_hash={:x} (prefix=0x{})", 
                                        logged_code_hash, logged_prefix,
                                        db_code_hash, db_prefix);
                                } else {
                                    warn!("LoggedDatabase::basic({}) MISMATCH (using log file data):", address);
                                    if !nonce_match {
                                        warn!("  nonce: log={}, db={}", logged_account_info.nonce, db_account_info.nonce);
                                    }
                                    if !balance_match {
                                        warn!("  balance: log={}, db={}", logged_account_info.balance, db_account_info.balance);
                                    }
                                    if !code_hash_match {
                                        warn!("  code_hash: log={:x}, db={:x}", logged_code_hash, db_code_hash);
                                    }
                                    if !bytecode_match {
                                        warn!("  bytecode (first 8 bytes): log code_hash={:x} (prefix=0x{}), db code_hash={:x} (prefix=0x{})", 
                                            logged_code_hash, logged_prefix,
                                            db_code_hash, db_prefix);
                                    } else {
                                        // bytecode 匹配，但也显示信息
                                        info!("  bytecode (first 8 bytes): log code_hash={:x} (prefix=0x{}), db code_hash={:x} (prefix=0x{})", 
                                            logged_code_hash, logged_prefix,
                                            db_code_hash, db_prefix);
                                    }
                                }
                            } else {
                                // 两者都是 empty_code_hash，不需要显示 bytecode 信息
                                if nonce_match && balance_match && code_hash_match && bytecode_match {
                                    info!("LoggedDatabase::basic({}) MATCH (using log file data): nonce={}, balance={}, code_hash={:x} (empty)", 
                                        address, logged_account_info.nonce, logged_account_info.balance, logged_account_info.code_hash());
                                } else {
                                    warn!("LoggedDatabase::basic({}) MISMATCH (using log file data):", address);
                                    if !nonce_match {
                                        warn!("  nonce: log={}, db={}", logged_account_info.nonce, db_account_info.nonce);
                                    }
                                    if !balance_match {
                                        warn!("  balance: log={}, db={}", logged_account_info.balance, db_account_info.balance);
                                    }
                                    if !code_hash_match {
                                        warn!("  code_hash: log={:x}, db={:x}", logged_code_hash, db_code_hash);
                                    }
                                }
                            }
                        } else {
                            warn!("LoggedDatabase::basic({}) MISMATCH (using log file data): log file says Some(nonce={}, balance={}, code_hash={:x}), but database returns None", 
                                address, logged_account_info.nonce, logged_account_info.balance, logged_account_info.code_hash());
                        }
                        
                        // 返回日志文件的数据，这样 EVM 会使用日志文件中的 code_hash，从而触发 code_by_hash
                        Ok(Some(logged_account_info))
                    }
                    Err(e) => {
                        warn!("LoggedDatabase::basic({}) failed to decode account RLP from log file: {}, falling back to database", address, e);
                        // 解码失败，回退到数据库
                        let mut inner_db = StateProviderDatabase::new(self.state_provider.as_ref());
                        inner_db.basic(address)
                    }
                }
            }
        } else {
            // 日志文件中没有该账户数据，从数据库读取并标注
            warn!("LoggedDatabase::basic({}) NOT IN LOG FILE (reading from database)", address);
            let mut inner_db = StateProviderDatabase::new(self.state_provider.as_ref());
            let db_result = inner_db.basic(address)?;
            if let Some(db_account_info) = &db_result {
                warn!("LoggedDatabase::basic({}) NOT IN LOG FILE: database has Some(nonce={}, balance={}, code_hash={:x})", 
                    address, db_account_info.nonce, db_account_info.balance, db_account_info.code_hash());
            } else {
                warn!("LoggedDatabase::basic({}) NOT IN LOG FILE: database also returns None", address);
            }
            Ok(db_result)
        }
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        // 始终从数据库读取代码（代码不在日志文件中，按用户要求忽略代码读取）
        // 注意：这里不检查日志文件，因为代码数据不保存在日志文件中
        let mut inner_db = StateProviderDatabase::new(self.state_provider.as_ref());
        let result = inner_db.code_by_hash(code_hash)?;
        info!("LoggedDatabase::code_by_hash({:x}) -> loaded {} bytes from database (code always read from database, not from log file)", 
            code_hash, result.len());
        Ok(result)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        // 注意：index 是 U256，需要转换为 32 字节的 key（与 LoggingDatabase 中的转换方式一致）
        let key_bytes = index.to_be_bytes::<32>();
        let key = B256::from_slice(&key_bytes);
        
        // 记录这个 storage 被访问了（在查找之前就记录，确保所有调用都被追踪）
        let was_already_accessed = self.accessed_storages.contains(&(address, key));
        if was_already_accessed {
            warn!("LoggedDatabase::storage({}, key={:x}) called MULTIPLE times!", address, key);
        }
        self.accessed_storages.insert((address, key));
        
        // 首先尝试从日志文件读取数据（使用保存的文件数据执行）
        if let Some(rlp_data) = self.storages.get(&(address, key)) {
            // 从日志文件读取的数据解码
            use alloy_rlp::Decodable;
            let mut data = rlp_data.as_slice();
            match U256::decode(&mut data) {
                Ok(logged_value) => {
                    // 使用日志文件的数据
                    // 对比数据库中的数据
                    let mut inner_db = StateProviderDatabase::new(self.state_provider.as_ref());
                    let db_value = inner_db.storage(address, index)?;
                    
                    if logged_value == db_value {
                        info!("LoggedDatabase::storage({}, key={:x}) MATCH (using log file data): value={}", 
                            address, key, logged_value);
                    } else {
                        warn!("LoggedDatabase::storage({}, key={:x}) MISMATCH (using log file data): log={}, db={}", 
                            address, key, logged_value, db_value);
                    }
                    
                    Ok(logged_value)
                }
                Err(e) => {
                    warn!("LoggedDatabase::storage({}, key={:x}) failed to decode storage RLP from log file: {}, falling back to database", 
                        address, key, e);
                    // 解码失败，回退到数据库
                    let mut inner_db = StateProviderDatabase::new(self.state_provider.as_ref());
                    inner_db.storage(address, index)
                }
            }
        } else {
            // 日志文件中没有该存储数据，从数据库读取并标注
            warn!("LoggedDatabase::storage({}, key={:x}) NOT IN LOG FILE (reading from database, total storages: {})", 
                address, key, self.storages.len());
            let mut inner_db = StateProviderDatabase::new(self.state_provider.as_ref());
            let db_value = inner_db.storage(address, index)?;
            warn!("LoggedDatabase::storage({}, key={:x}) NOT IN LOG FILE: database has value={}", 
                address, key, db_value);
            Ok(db_value)
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
            info!("Starting log recording for block {}", log_block);
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
                write_read_logs_binary(log_block, &logs)?;
            }
        }

        // 如果指定了 use_log，使用日志文件中的数据执行块
        if let Some(log_path_or_block) = &self.use_log {
            // 确定日志文件路径和块号
            let (log_path, block_number) = if let Ok(block_num) = log_path_or_block.parse::<u64>() {
                // 如果传递的是数字，自动构建文件路径
                let path = format!("block_{}_reads.bin", block_num);
                (path, block_num)
            } else {
                // 如果传递的是路径，尝试从文件名提取块号
                let path = log_path_or_block.clone();
                let block_num = self.log_block.unwrap_or_else(|| {
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
            
            // 读取日志文件
            let log_entries = read_read_logs_binary(&log_path)?;
            info!("Loaded {} log entries from {}", log_entries.len(), log_path);
            
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
