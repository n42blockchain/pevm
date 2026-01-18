// SPDX-License-Identifier: MIT OR Apache-2.0

//! 批次级日志存储模块

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write as IoWrite, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::cell::RefCell;

use alloy_primitives::{Address, B256, U256};
use dashmap::DashMap;
use reth_codecs::Compact;
use reth_primitives::Account;
use reth_revm::database::StateProviderDatabase;
use tracing::{info, debug};

use crate::revm::Database as RevmDatabase;
use crate::revm::state::{AccountInfo, Bytecode};

use super::logged_db::BytecodeCache;

const MAGIC: &[u8; 8] = b"BATCHLG3";
const VERSION: u32 = 3;
const HEADER_SIZE: usize = 64;
const ENTRY_TYPE_BASIC: u8 = 0x00;
const ENTRY_TYPE_STORAGE: u8 = 0x01;

#[derive(Debug, Clone)]
pub struct BatchFileHeader {
    pub batch_count: u64,
    pub first_block_number: u64,
    pub last_block_number: u64,
    pub batch_size: u64,
    pub total_data_size: u64,
}

impl BatchFileHeader {
    pub fn from_bytes(data: &[u8]) -> eyre::Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(eyre::eyre!("Header too short"));
        }
        if &data[0..8] != MAGIC {
            return Err(eyre::eyre!("Invalid magic number"));
        }
        let version = u32::from_le_bytes(data[8..12].try_into().unwrap());
        if version != VERSION {
            return Err(eyre::eyre!("Unsupported version: {}", version));
        }
        Ok(Self {
            batch_count: u64::from_le_bytes(data[12..20].try_into().unwrap()),
            first_block_number: u64::from_le_bytes(data[20..28].try_into().unwrap()),
            last_block_number: u64::from_le_bytes(data[28..36].try_into().unwrap()),
            batch_size: u64::from_le_bytes(data[36..44].try_into().unwrap()),
            total_data_size: u64::from_le_bytes(data[44..52].try_into().unwrap()),
        })
    }

    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..8].copy_from_slice(MAGIC);
        buf[8..12].copy_from_slice(&VERSION.to_le_bytes());
        buf[12..20].copy_from_slice(&self.batch_count.to_le_bytes());
        buf[20..28].copy_from_slice(&self.first_block_number.to_le_bytes());
        buf[28..36].copy_from_slice(&self.last_block_number.to_le_bytes());
        buf[36..44].copy_from_slice(&self.batch_size.to_le_bytes());
        buf[44..52].copy_from_slice(&self.total_data_size.to_le_bytes());
        buf
    }
}

#[derive(Debug, Clone, Copy)]
struct BatchIndexEntry {
    batch_start_block: u64,
    data_offset: u64,
    compressed_size: u64,
    entry_count: u64,
}

impl BatchIndexEntry {
    const SIZE: usize = 32;

    fn from_bytes(data: &[u8]) -> eyre::Result<Self> {
        if data.len() < Self::SIZE {
            return Err(eyre::eyre!("Index entry too short"));
        }
        Ok(Self {
            batch_start_block: u64::from_le_bytes(data[0..8].try_into().unwrap()),
            data_offset: u64::from_le_bytes(data[8..16].try_into().unwrap()),
            compressed_size: u64::from_le_bytes(data[16..24].try_into().unwrap()),
            entry_count: u64::from_le_bytes(data[24..32].try_into().unwrap()),
        })
    }

    fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..8].copy_from_slice(&self.batch_start_block.to_le_bytes());
        buf[8..16].copy_from_slice(&self.data_offset.to_le_bytes());
        buf[16..24].copy_from_slice(&self.compressed_size.to_le_bytes());
        buf[24..32].copy_from_slice(&self.entry_count.to_le_bytes());
        buf
    }
}

pub struct BatchData {
    pub data: Vec<u8>,
    pub entry_count: u64,
    pub start_block: u64,
}

pub struct BatchLogStore {
    header: BatchFileHeader,
    index: HashMap<u64, BatchIndexEntry>,
    loaded_batches: DashMap<u64, Arc<BatchData>>,
    dat_mmap: memmap2::Mmap,
}

impl std::fmt::Debug for BatchLogStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchLogStore")
            .field("batch_count", &self.header.batch_count)
            .field("batch_size", &self.header.batch_size)
            .finish()
    }
}

impl BatchLogStore {
    pub fn open<P: AsRef<Path>>(path: P) -> eyre::Result<Self> {
        let path = path.as_ref();
        let (idx_path, dat_path) = Self::resolve_paths(path)?;

        let mut idx_file = File::open(&idx_path)?;
        let mut header_buf = [0u8; HEADER_SIZE];
        idx_file.read_exact(&mut header_buf)?;
        let header = BatchFileHeader::from_bytes(&header_buf)?;

        let mut index = HashMap::with_capacity(header.batch_count as usize);
        let mut entry_buf = [0u8; BatchIndexEntry::SIZE];
        for _ in 0..header.batch_count {
            idx_file.read_exact(&mut entry_buf)?;
            let entry = BatchIndexEntry::from_bytes(&entry_buf)?;
            index.insert(entry.batch_start_block, entry);
        }

        let dat_file = File::open(&dat_path)?;
        let dat_mmap = unsafe { memmap2::Mmap::map(&dat_file)? };

        info!("Opened batch log store: {} batches, batch_size={}, range={}-{}, data_size={}",
            header.batch_count, header.batch_size, header.first_block_number,
            header.last_block_number, header.total_data_size);

        Ok(Self { header, index, loaded_batches: DashMap::new(), dat_mmap })
    }

    fn resolve_paths(path: &Path) -> eyre::Result<(PathBuf, PathBuf)> {
        let path_str = path.to_string_lossy();
        if path_str.ends_with(".batchlog.idx") {
            let base = path_str.strip_suffix(".idx").unwrap();
            Ok((path.to_path_buf(), PathBuf::from(format!("{}.dat", base.strip_suffix(".batchlog").unwrap_or(base)))))
        } else if path_str.ends_with(".batchlog.dat") {
            let base = path_str.strip_suffix(".dat").unwrap();
            Ok((PathBuf::from(format!("{}.idx", base.strip_suffix(".batchlog").unwrap_or(base))), path.to_path_buf()))
        } else if path_str.ends_with(".batchlog") {
            Ok((PathBuf::from(format!("{}.idx", path_str)), PathBuf::from(format!("{}.dat", path_str))))
        } else {
            let idx_path = path.join("blocks.batchlog.idx");
            let dat_path = path.join("blocks.batchlog.dat");
            if idx_path.exists() { Ok((idx_path, dat_path)) }
            else { Err(eyre::eyre!("Cannot determine batch log file paths from: {}", path.display())) }
        }
    }

    pub fn header(&self) -> &BatchFileHeader { &self.header }
    pub fn batch_size(&self) -> u64 { self.header.batch_size }

    pub fn load_batch(&self, batch_start: u64) -> eyre::Result<Arc<BatchData>> {
        if let Some(cached) = self.loaded_batches.get(&batch_start) { return Ok(cached.clone()); }
        let entry = self.index.get(&batch_start).ok_or_else(|| eyre::eyre!("Batch {} not found in index", batch_start))?;
        let start = entry.data_offset as usize;
        let end = start + entry.compressed_size as usize;
        if end > self.dat_mmap.len() { return Err(eyre::eyre!("Batch {} data out of bounds", batch_start)); }
        let compressed = &self.dat_mmap[start..end];
        let data = zstd::decode_all(compressed)?;
        let batch_data = Arc::new(BatchData { data, entry_count: entry.entry_count, start_block: batch_start });
        self.loaded_batches.insert(batch_start, batch_data.clone());
        Ok(batch_data)
    }

    pub fn release_batch(&self, batch_start: u64) { self.loaded_batches.remove(&batch_start); }
}

pub struct BatchLogStoreWriter {
    dat_writer: BufWriter<File>,
    idx_path: PathBuf,
    compression_level: i32,
    entries: Vec<BatchIndexEntry>,
    current_offset: u64,
    first_block: Option<u64>,
    last_block: Option<u64>,
    batch_size: u64,
}

impl BatchLogStoreWriter {
    pub fn new<P: AsRef<Path>>(path: P, batch_size: u64) -> eyre::Result<Self> {
        let path = path.as_ref();
        let (idx_path, dat_path) = if path.extension().map_or(false, |e| e == "batchlog") {
            (PathBuf::from(format!("{}.idx", path.display())), PathBuf::from(format!("{}.dat", path.display())))
        } else {
            std::fs::create_dir_all(path)?;
            (path.join("blocks.batchlog.idx"), path.join("blocks.batchlog.dat"))
        };
        info!("Creating batch log files: {} and {}", dat_path.display(), idx_path.display());
        let dat_file = File::create(&dat_path)?;
        let dat_writer = BufWriter::with_capacity(1024 * 1024, dat_file);
        Ok(Self { dat_writer, idx_path, compression_level: 3, entries: Vec::new(), current_offset: 0, first_block: None, last_block: None, batch_size })
    }

    pub fn write_batch(&mut self, start_block: u64, end_block: u64, data: &[u8], entry_count: u64) -> eyre::Result<()> {
        let compressed = zstd::encode_all(data, self.compression_level)?;
        self.dat_writer.write_all(&compressed)?;
        let entry = BatchIndexEntry { batch_start_block: start_block, data_offset: self.current_offset, compressed_size: compressed.len() as u64, entry_count };
        self.entries.push(entry);
        self.current_offset += compressed.len() as u64;
        if self.first_block.is_none() { self.first_block = Some(start_block); }
        self.last_block = Some(end_block);
        Ok(())
    }

    pub fn finish(mut self) -> eyre::Result<()> {
        self.dat_writer.flush()?;
        let mut idx_file = File::create(&self.idx_path)?;
        let header = BatchFileHeader { batch_count: self.entries.len() as u64, first_block_number: self.first_block.unwrap_or(0), last_block_number: self.last_block.unwrap_or(0), batch_size: self.batch_size, total_data_size: self.current_offset };
        idx_file.write_all(&header.to_bytes())?;
        for entry in &self.entries { idx_file.write_all(&entry.to_bytes())?; }
        info!("Batch log store written: {} batches, range {}-{}, data size: {} bytes", self.entries.len(), self.first_block.unwrap_or(0), self.last_block.unwrap_or(0), self.current_offset);
        Ok(())
    }
}

pub struct BatchDatabase {
    batch_data: Arc<BatchData>,
    basic_entries: Vec<(usize, usize)>,
    storage_entries: Vec<(usize, usize)>,
    basic_pos: std::cell::Cell<usize>,
    storage_pos: std::cell::Cell<usize>,
    state_provider: Arc<dyn reth_provider::StateProvider>,
    bytecode_cache: Option<Arc<BytecodeCache>>,
    account_cache: RefCell<HashMap<Address, Option<AccountInfo>>>,
    storage_cache: RefCell<HashMap<(Address, U256), U256>>,
    block_hash_cache: RefCell<HashMap<u64, B256>>,
}

impl std::fmt::Debug for BatchDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchDatabase").field("start_block", &self.batch_data.start_block).finish()
    }
}

impl BatchDatabase {
    pub fn from_store(store: &BatchLogStore, batch_start: u64, state_provider: Arc<dyn reth_provider::StateProvider>, bytecode_cache: Option<Arc<BytecodeCache>>) -> eyre::Result<Self> {
        let batch_data = store.load_batch(batch_start)?;
        Ok(Self::new(batch_data, state_provider, bytecode_cache))
    }

    pub fn new(batch_data: Arc<BatchData>, state_provider: Arc<dyn reth_provider::StateProvider>, bytecode_cache: Option<Arc<BytecodeCache>>) -> Self {
        let (basic_entries, storage_entries) = Self::parse_entries(&batch_data.data);
        Self { batch_data, basic_entries, storage_entries, basic_pos: std::cell::Cell::new(0), storage_pos: std::cell::Cell::new(0), state_provider, bytecode_cache, account_cache: RefCell::new(HashMap::new()), storage_cache: RefCell::new(HashMap::new()), block_hash_cache: RefCell::new(HashMap::new()) }
    }

    fn parse_entries(data: &[u8]) -> (Vec<(usize, usize)>, Vec<(usize, usize)>) {
        let mut basic_entries = Vec::new();
        let mut storage_entries = Vec::new();
        let mut pos = 0;
        while pos < data.len() {
            let first_byte = data[pos];
            if first_byte == ENTRY_TYPE_BASIC || first_byte == ENTRY_TYPE_STORAGE {
                if pos + 2 > data.len() { break; }
                let entry_type = first_byte;
                let len = data[pos + 1] as usize;
                let start = pos + 2;
                let end = start + len;
                if end > data.len() { break; }
                if entry_type == ENTRY_TYPE_BASIC { basic_entries.push((start, end)); }
                else { storage_entries.push((start, end)); }
                pos = end;
            } else {
                let len = first_byte as usize;
                let start = pos + 1;
                let end = start + len;
                if end > data.len() { break; }
                basic_entries.push((start, end));
                pos = end;
            }
        }
        (basic_entries, storage_entries)
    }

    fn next_basic_entry(&self) -> Option<&[u8]> {
        let pos = self.basic_pos.get();
        if pos >= self.basic_entries.len() { return None; }
        let (start, end) = self.basic_entries[pos];
        self.basic_pos.set(pos + 1);
        Some(&self.batch_data.data[start..end])
    }

    fn next_storage_entry(&self) -> Option<&[u8]> {
        let pos = self.storage_pos.get();
        if pos >= self.storage_entries.len() { return None; }
        let (start, end) = self.storage_entries[pos];
        self.storage_pos.set(pos + 1);
        Some(&self.batch_data.data[start..end])
    }

    pub fn get_stats(&self) -> (u64, u64, u64, u64, u64, u64) { (0, 0, 0, 0, 0, self.batch_data.entry_count) }

    fn decode_account_compact(data: &[u8]) -> eyre::Result<AccountInfo> {
        if data.is_empty() || (data.len() == 1 && data[0] == 0x00) { return Err(eyre::eyre!("Account does not exist")); }
        let data_len = data.len();
        let (account, _) = Account::from_compact(data, data_len);
        Ok(account.into())
    }
}

impl RevmDatabase for BatchDatabase {
    type Error = <StateProviderDatabase<Box<dyn reth_provider::StateProvider>> as RevmDatabase>::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        if let Some(cached) = self.account_cache.borrow().get(&address) { return Ok(cached.clone()); }
        let entry_idx = self.basic_pos.get();
        if let Some(entry_data) = self.next_basic_entry() {
            // Entry format: [addr(20)][account_data(remaining)]
            if entry_data.len() >= 20 {
                let logged_addr = Address::from_slice(&entry_data[0..20]);
                let compact_data = &entry_data[20..];

                if logged_addr == address {
                    let result = if compact_data.is_empty() || (compact_data.len() == 1 && (compact_data[0] == 0x00 || compact_data[0] == 0xc0)) { None }
                    else { Self::decode_account_compact(compact_data).ok() };
                    let nonce = result.as_ref().map(|i| i.nonce).unwrap_or(0);
                    debug!(target: "batch_replay", "REPLAY basic #{}: addr={}, nonce={}", entry_idx, address, nonce);
                    self.account_cache.borrow_mut().insert(address, result.clone());
                    return Ok(result);
                } else {
                    // Address mismatch - cache the logged entry for later use, fall back to DB
                    let logged_result = if compact_data.is_empty() || (compact_data.len() == 1 && (compact_data[0] == 0x00 || compact_data[0] == 0xc0)) { None }
                    else { Self::decode_account_compact(compact_data).ok() };
                    self.account_cache.borrow_mut().insert(logged_addr, logged_result);
                    debug!(target: "batch_replay", "REPLAY basic #{}: addr MISMATCH! wanted={}, got={}, falling back to DB", entry_idx, address, logged_addr);
                }
            }
        }
        debug!(target: "batch_replay", "REPLAY basic #{}: addr={} FALLBACK to DB", entry_idx, address);
        let mut inner_db = StateProviderDatabase::new(self.state_provider.as_ref() as &dyn reth_provider::StateProvider);
        let result = inner_db.basic(address)?;
        self.account_cache.borrow_mut().insert(address, result.clone());
        Ok(result)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        if let Some(ref cache) = self.bytecode_cache { if let Some(bytecode) = cache.get(&code_hash) { return Ok(bytecode); } }
        let mut inner_db = StateProviderDatabase::new(self.state_provider.as_ref() as &dyn reth_provider::StateProvider);
        let bytecode = inner_db.code_by_hash(code_hash)?;
        if let Some(ref cache) = self.bytecode_cache { cache.insert(code_hash, bytecode.clone()); }
        Ok(bytecode)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let key = (address, index);
        if let Some(&cached) = self.storage_cache.borrow().get(&key) { return Ok(cached); }
        if let Some(entry_data) = self.next_storage_entry() {
            // Entry format: [addr(20)][index(32)][value_data(remaining)]
            if entry_data.len() >= 52 {
                let logged_addr = Address::from_slice(&entry_data[0..20]);
                let logged_index = U256::from_be_slice(&entry_data[20..52]);
                let compact_data = &entry_data[52..];

                if logged_addr == address && logged_index == index {
                    let data_len = compact_data.len();
                    let (value, _) = U256::from_compact(compact_data, data_len);
                    self.storage_cache.borrow_mut().insert(key, value);
                    return Ok(value);
                } else {
                    // Key mismatch - cache the logged entry for later use
                    let data_len = compact_data.len();
                    let (logged_value, _) = U256::from_compact(compact_data, data_len);
                    self.storage_cache.borrow_mut().insert((logged_addr, logged_index), logged_value);
                    debug!(target: "batch_replay", "REPLAY storage: key MISMATCH! wanted=({},{}), got=({},{}), falling back to DB", address, index, logged_addr, logged_index);
                }
            }
        }
        let mut inner_db = StateProviderDatabase::new(self.state_provider.as_ref() as &dyn reth_provider::StateProvider);
        let value = inner_db.storage(address, index)?;
        self.storage_cache.borrow_mut().insert(key, value);
        Ok(value)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        if let Some(&cached) = self.block_hash_cache.borrow().get(&number) { return Ok(cached); }
        let mut inner_db = StateProviderDatabase::new(self.state_provider.as_ref() as &dyn reth_provider::StateProvider);
        let hash = inner_db.block_hash(number)?;
        self.block_hash_cache.borrow_mut().insert(number, hash);
        Ok(hash)
    }
}

pub struct BatchLoggingDatabase<DB> {
    inner: DB,
    logs: std::cell::RefCell<Vec<u8>>,
    entry_count: std::cell::Cell<u64>,
    account_cache: RefCell<HashMap<Address, Option<AccountInfo>>>,
    storage_cache: RefCell<HashMap<(Address, U256), U256>>,
}

impl<DB> BatchLoggingDatabase<DB> {
    pub fn new(inner: DB) -> Self {
        Self { inner, logs: std::cell::RefCell::new(Vec::with_capacity(1024 * 1024)), entry_count: std::cell::Cell::new(0), account_cache: RefCell::new(HashMap::new()), storage_cache: RefCell::new(HashMap::new()) }
    }

    pub fn take_logs(&self) -> (Vec<u8>, u64) {
        let data = std::mem::take(&mut *self.logs.borrow_mut());
        let count = self.entry_count.get();
        self.entry_count.set(0);
        self.account_cache.borrow_mut().clear();
        self.storage_cache.borrow_mut().clear();
        (data, count)
    }

    fn push_basic_entry(&self, address: Address, data: &[u8]) {
        // Format: [type(1)][len(1)][addr(20)][account_data(len-20)]
        let total_len = (20 + data.len()).min(255) as u8;
        let mut logs = self.logs.borrow_mut();
        logs.push(ENTRY_TYPE_BASIC);
        logs.push(total_len);
        logs.extend_from_slice(address.as_slice());  // 20 bytes
        let account_len = (total_len as usize).saturating_sub(20);
        logs.extend_from_slice(&data[..account_len.min(data.len())]);
        self.entry_count.set(self.entry_count.get() + 1);
    }

    fn push_storage_entry(&self, address: Address, index: U256, data: &[u8]) {
        // Format: [type(1)][len(1)][addr(20)][index(32)][value_data(remaining)]
        let total_len = (20 + 32 + data.len()).min(255) as u8;
        let mut logs = self.logs.borrow_mut();
        logs.push(ENTRY_TYPE_STORAGE);
        logs.push(total_len);
        logs.extend_from_slice(address.as_slice());  // 20 bytes
        logs.extend_from_slice(&index.to_be_bytes::<32>());  // 32 bytes
        let value_len = (total_len as usize).saturating_sub(52);
        logs.extend_from_slice(&data[..value_len.min(data.len())]);
        self.entry_count.set(self.entry_count.get() + 1);
    }
}

impl<DB: std::fmt::Debug> std::fmt::Debug for BatchLoggingDatabase<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchLoggingDatabase").field("inner", &self.inner).field("entry_count", &self.entry_count.get()).finish()
    }
}

impl<DB: RevmDatabase> RevmDatabase for BatchLoggingDatabase<DB> {
    type Error = DB::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        if let Some(cached) = self.account_cache.borrow().get(&address) { return Ok(cached.clone()); }
        let result = self.inner.basic(address)?;
        let entry_idx = self.entry_count.get();
        let nonce = result.as_ref().map(|i| i.nonce).unwrap_or(0);
        debug!(target: "batch_gen", "GEN basic #{}: addr={}, nonce={}", entry_idx, address, nonce);
        let compact_data = match &result {
            None => vec![0x00],
            Some(info) => {
                let account = Account { nonce: info.nonce, balance: info.balance, bytecode_hash: Some(info.code_hash()) };
                let mut buf = Vec::with_capacity(64);
                account.to_compact(&mut buf);
                buf
            }
        };
        self.push_basic_entry(address, &compact_data);
        self.account_cache.borrow_mut().insert(address, result.clone());
        Ok(result)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> { self.inner.code_by_hash(code_hash) }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let key = (address, index);
        if let Some(&cached) = self.storage_cache.borrow().get(&key) { return Ok(cached); }
        let result = self.inner.storage(address, index)?;
        let mut compact_data = Vec::with_capacity(32);
        result.to_compact(&mut compact_data);
        self.push_storage_entry(address, index, &compact_data);
        self.storage_cache.borrow_mut().insert(key, result);
        Ok(result)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> { self.inner.block_hash(number) }
}
