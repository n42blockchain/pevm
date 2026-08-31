//! Reading contract bytecode out of an N42 codes freezer.
//!
//! Witness replay needs code, and the witness itself never carries it. Pointing
//! at a codes freezer means replay does not need reth's `Bytecodes` table, which
//! is the last piece of state it would otherwise depend on.
//!
//! Layout is `codes.hidx` (a RecSplit minimal perfect hash over the code
//! hashes), `codes.hoff` (a slot-ordered offset table) and `codes.NNNN.cdat`
//! (one zstd frame per contract) - the same three files gov5 reads.
//!
//! The MPHF stores no keys, so it maps an unknown hash onto some slot rather
//! than reporting a miss. `keccak(code) == code_hash` is what separates a real
//! hit from that, exactly as on the Go side; it is never a membership test on
//! its own.

use super::recsplit::RecSplitIndex;
use crate::revm::{
    state::{AccountInfo, Bytecode},
    Database as RevmDatabase,
};
use alloy_primitives::{keccak256, Address, Bytes, B256, U256};
use crate::revm::bytecode::JumpTable;
use eyre::{Context, Result};
use reth_storage_errors::provider::ProviderError;
use std::{fs, path::Path, sync::Arc};

/// `[file_number: u16 LE][offset: u32 LE][length: u32 LE]`
const HOFF_ENTRY_SIZE: usize = 10;

/// Content-addressed view over a codes freezer.
pub(super) struct CodesFreezer {
    index: RecSplitIndex,
    hoff: memmap2::Mmap,
    data: Vec<memmap2::Mmap>,
    slots: usize,
}

impl std::fmt::Debug for CodesFreezer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CodesFreezer")
            .field("slots", &self.slots)
            .field("keys", &self.index.key_count())
            .field("data_files", &self.data.len())
            .finish()
    }
}

impl CodesFreezer {
    pub(super) fn open(directory: &Path) -> Result<Self> {
        let index = RecSplitIndex::open(&directory.join("codes.hidx"))?;

        let hoff_path = directory.join("codes.hoff");
        let hoff_file = fs::File::open(&hoff_path)
            .wrap_err_with(|| format!("failed to open {}", hoff_path.display()))?;
        // SAFETY: the freezer is read-only here and is not written while a run
        // holds it mapped.
        let hoff = unsafe { memmap2::Mmap::map(&hoff_file) }
            .wrap_err_with(|| format!("failed to map {}", hoff_path.display()))?;
        if hoff.len() % HOFF_ENTRY_SIZE != 0 {
            eyre::bail!("{} ends with a partial entry", hoff_path.display())
        }
        let slots = hoff.len() / HOFF_ENTRY_SIZE;

        let data = map_data_files(directory)?;

        Ok(Self {
            index,
            hoff,
            data,
            slots,
        })
    }

    /// Number of contracts the index was built over.
    pub(super) fn entries(&self) -> usize {
        self.index.key_count() as usize
    }

    /// Returns the code with this hash, or `None` if the freezer does not hold it.
    ///
    /// Three things can send an unknown hash here to `None`: a slot outside the
    /// table, bytes that are not a zstd frame, or code that hashes to something
    /// else. All three are expected for a hash outside the build set - the MPHF
    /// answers anyway - so none of them is treated as corruption.
    pub(super) fn code_by_hash(&self, code_hash: B256) -> Result<Option<Vec<u8>>> {
        let Some(slot) = self.index.lookup(code_hash.as_slice()) else {
            return Ok(None)
        };
        let slot = slot as usize;
        if slot >= self.slots {
            return Ok(None)
        }

        let position = slot * HOFF_ENTRY_SIZE;
        let file_number = u16::from_le_bytes(self.hoff[position..position + 2].try_into().unwrap());
        let offset = u32::from_le_bytes(self.hoff[position + 2..position + 6].try_into().unwrap());
        let length = u32::from_le_bytes(self.hoff[position + 6..position + 10].try_into().unwrap());

        let Some(mapped) = self.data.get(file_number as usize) else {
            return Ok(None)
        };
        let start = offset as usize;
        let end = start + length as usize;
        if end > mapped.len() {
            return Ok(None)
        }

        let Ok(code) = zstd::stream::decode_all(&mapped[start..end]) else {
            return Ok(None)
        };
        if keccak256(&code) != code_hash {
            return Ok(None)
        }
        Ok(Some(code))
    }
}

fn map_data_files(directory: &Path) -> Result<Vec<memmap2::Mmap>> {
    let mut data = Vec::new();
    for file_number in 0u16.. {
        let path = directory.join(format!("codes.{file_number:04}.cdat"));
        if !path.exists() {
            break
        }
        let file = fs::File::open(&path)
            .wrap_err_with(|| format!("failed to open {}", path.display()))?;
        // SAFETY: read-only, not written while mapped.
        let mapped = unsafe { memmap2::Mmap::map(&file) }
            .wrap_err_with(|| format!("failed to map {}", path.display()))?;
        data.push(mapped);
    }
    if data.is_empty() {
        eyre::bail!("no codes.NNNN.cdat files in {}", directory.display())
    }
    Ok(data)
}

/// Serves code and block hashes from the freezers instead of the database.
///
/// A witness carries neither, so replay would otherwise still need a reth
/// archive for them. With both external sources present, `inner` is only
/// reached when a lookup misses - which for a store that covers the range
/// being replayed should not happen.

/// Codes freezer with gov5's 20-byte-key index: `codes.cidx` carries the
/// `NCIX` header with the "address index" flag and 26-byte entries
/// `key ‖ file (u16 BE) ‖ offset (u32 BE)`, sorted by key; each item is one
/// zstd frame in `codes.NNNN.cdat`. The flag's name is historical: exported
/// from reth's `Bytecodes` table, as the production input set on this host
/// was, `code-import2fz` writes the first 20 bytes of the *code hash* as the
/// key (`copy(a[:], k[:20])`), so the lookup is by hash prefix and
/// `keccak(code) == code_hash` settles a prefix collision or a stale entry;
/// a miss falls through to the code MDBX.
pub(super) struct AddressCodes {
    index: memmap2::Mmap,
    entries: usize,
    data: Vec<memmap2::Mmap>,
}

const NCIX_HEADER: usize = 16;
const NCIX_FLAG_ADDR_INDEX: u8 = 0x08;
const ADDR_ENTRY_SIZE: usize = 26;

impl std::fmt::Debug for AddressCodes {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AddressCodes")
            .field("entries", &self.entries)
            .field("data_files", &self.data.len())
            .finish()
    }
}

impl AddressCodes {
    /// Whether `directory` holds an address-indexed `codes.cidx`.
    pub(super) fn is_present(directory: &Path) -> bool {
        let path = directory.join("codes.cidx");
        let Ok(mut file) = fs::File::open(path) else { return false };
        let mut header = [0u8; NCIX_HEADER];
        std::io::Read::read_exact(&mut file, &mut header).is_ok()
            && &header[..4] == b"NCIX"
            && header[5] & NCIX_FLAG_ADDR_INDEX != 0
    }

    pub(super) fn open(directory: &Path) -> Result<Self> {
        let path = directory.join("codes.cidx");
        let file = fs::File::open(&path).wrap_err_with(|| format!("failed to open {}", path.display()))?;
        let index = unsafe { memmap2::Mmap::map(&file) }
            .wrap_err_with(|| format!("failed to map {}", path.display()))?;
        if index.len() < NCIX_HEADER || &index[..4] != b"NCIX" {
            eyre::bail!("{} is not an NCIX index", path.display())
        }
        let (version, flags, entry_size) = (index[4], index[5], index[7] as usize);
        if version != 1 || flags & NCIX_FLAG_ADDR_INDEX == 0 || entry_size != ADDR_ENTRY_SIZE {
            eyre::bail!(
                "{} is not an address index (version {version}, flags {flags:#x}, entry size {entry_size})",
                path.display()
            )
        }
        let body = index.len() - NCIX_HEADER;
        if body % ADDR_ENTRY_SIZE != 0 {
            eyre::bail!("{} ends with a partial entry", path.display())
        }
        let entries = body / ADDR_ENTRY_SIZE;
        let data = map_data_files(directory)?;
        Ok(Self { index, entries, data })
    }

    pub(super) const fn entries(&self) -> usize {
        self.entries
    }

    fn entry(&self, position: usize) -> &[u8] {
        let start = NCIX_HEADER + position * ADDR_ENTRY_SIZE;
        &self.index[start..start + ADDR_ENTRY_SIZE]
    }

    /// The code stored under the first 20 bytes of `code_hash`, unverified;
    /// `None` when there is no entry.
    pub(super) fn code_by_hash_prefix(&self, code_hash: B256) -> Result<Option<Vec<u8>>> {
        let key = &code_hash.as_slice()[..20];
        let (mut low, mut high) = (0usize, self.entries);
        while low < high {
            let middle = low + (high - low) / 2;
            match self.entry(middle)[..20].cmp(key) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => {
                    let entry = self.entry(middle);
                    let file_number = u16::from_be_bytes(entry[20..22].try_into().unwrap());
                    let offset = u32::from_be_bytes(entry[22..26].try_into().unwrap()) as usize;
                    let Some(mapped) = self.data.get(file_number as usize) else {
                        eyre::bail!("codes data file {file_number} is missing")
                    };
                    if offset >= mapped.len() {
                        eyre::bail!("codes entry for {code_hash:?} points past file {file_number}")
                    }
                    // One zstd frame per item and no stored length: the
                    // decoder stops at the frame's end.
                    let decoder = zstd::stream::read::Decoder::with_buffer(&mapped[offset..])
                        .wrap_err("bad zstd frame in the codes freezer")?;
                    let mut decoder = decoder.single_frame();
                    let mut code = Vec::new();
                    std::io::Read::read_to_end(&mut decoder, &mut code)
                        .wrap_err_with(|| format!("bad zstd frame for {code_hash:?}"))?;
                    return Ok(Some(code))
                }
            }
        }
        Ok(None)
    }
}

/// gov5's `Code` table (code hash → bytecode) in its own MDBX, the fallback
/// for a code the address index does not resolve.
pub(super) struct CodeMdbx {
    /// One read transaction for the process, taken under a lock: the table
    /// never changes, lookups are rare (only redeployed contracts get here),
    /// and a transaction per lookup from every worker exhausts the reader
    /// slots the environment was created with.
    txn: std::sync::Mutex<reth_libmdbx::Transaction<reth_libmdbx::RO>>,
    dbi: reth_libmdbx::ffi::MDBX_dbi,
    _env: reth_libmdbx::Environment,
}

impl std::fmt::Debug for CodeMdbx {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("CodeMdbx").finish()
    }
}

impl CodeMdbx {
    pub(super) fn open(directory: &Path) -> Result<Self> {
        let env = reth_libmdbx::Environment::builder()
            .set_flags(reth_libmdbx::EnvironmentFlags {
                mode: reth_libmdbx::Mode::ReadOnly,
                accede: true,
                no_rdahead: true,
                ..Default::default()
            })
            .set_max_dbs(256)
            // The one shared transaction lives for the whole run; reth's
            // default is to time a read transaction out after five minutes,
            // which turned every code lookup past that into an error.
            .set_max_read_transaction_duration(reth_libmdbx::MaxReadTransactionDuration::Unbounded)
            .open(directory)
            .wrap_err_with(|| format!("failed to open the code MDBX at {}", directory.display()))?;
        let txn = env.begin_ro_txn()?;
        let dbi = txn
            .open_db(Some("Code"))
            .wrap_err("the code MDBX has no `Code` table")?
            .dbi();
        Ok(Self {
            txn: std::sync::Mutex::new(txn),
            dbi,
            _env: env,
        })
    }

    pub(super) fn code_by_hash(&self, code_hash: B256) -> Result<Option<Vec<u8>>> {
        let txn = self.txn.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(txn.get::<Vec<u8>>(self.dbi, code_hash.as_slice())?)
    }
}

/// Resolves the code of an account whose witness record names only the code
/// hash: the cache first, then the address index (checked against the hash),
/// then the code MDBX. Shared by every worker.
pub(super) struct CodeResolver {
    by_address: Option<Arc<AddressCodes>>,
    /// The content-addressed freezer (codes.hidx), when the codes dir ships
    /// one instead of the address index.
    freezer: Option<Arc<CodesFreezer>>,
    mdbx: Option<Arc<CodeMdbx>>,
    /// Keyed by a hash already, so the map hashes with alloy's fast hasher
    /// rather than SipHash: this is read on every contract account.
    cache: dashmap::DashMap<B256, Bytecode, alloy_primitives::map::DefaultHashBuilder>,
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
    mdbx_hits: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for CodeResolver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CodeResolver")
            .field("cached", &self.cache.len())
            .field("hits", &self.hits.load(std::sync::atomic::Ordering::Relaxed))
            .field("misses", &self.misses.load(std::sync::atomic::Ordering::Relaxed))
            .field("mdbx_hits", &self.mdbx_hits.load(std::sync::atomic::Ordering::Relaxed))
            .finish()
    }
}

impl CodeResolver {
    pub(super) fn new(
        by_address: Option<Arc<AddressCodes>>,
        freezer: Option<Arc<CodesFreezer>>,
        mdbx: Option<Arc<CodeMdbx>>,
    ) -> Self {
        Self {
            by_address,
            freezer,
            mdbx,
            cache: dashmap::DashMap::with_hasher(Default::default()),
            hits: Default::default(),
            misses: Default::default(),
            mdbx_hits: Default::default(),
        }
    }

    /// Code for the account at `address` whose hash is `code_hash`; `None`
    /// when no source has it. The address is not needed by the sources this
    /// host has, but a true address index would be.
    pub(super) fn code_for(&self, address: Address, code_hash: B256) -> Result<Option<Bytecode>> {
        let _ = address;
        self.by_hash(code_hash)
    }

    /// Code by hash: this thread's own cache, the shared one, the freezer,
    /// then the code MDBX.
    pub(super) fn by_hash(&self, code_hash: B256) -> Result<Option<Bytecode>> {
        use std::sync::atomic::Ordering::Relaxed;
        // A contract account is read a few hundred times per block and the
        // same contracts recur, so most lookups end here without touching
        // the shared map's locks. Bytecode is a reference-counted buffer, so
        // the clone is a pointer.
        if let Some(code) = LOCAL_CODES.with(|local| local.borrow().get(&code_hash).cloned()) {
            return Ok(Some(code))
        }
        if let Some(code) = self.cache.get(&code_hash) {
            self.hits.fetch_add(1, Relaxed);
            let code = code.clone();
            Self::remember_locally(code_hash, &code);
            return Ok(Some(code))
        }
        self.misses.fetch_add(1, Relaxed);
        if let Some(codes) = self.by_address.as_ref() {
            if let Some(code) = codes.code_by_hash_prefix(code_hash)? {
                if keccak256(&code) == code_hash {
                    return Ok(Some(self.remember(code_hash, code)))
                }
            }
        }
        if let Some(freezer) = self.freezer.as_ref() {
            // The freezer verifies keccak itself.
            if let Some(code) = freezer.code_by_hash(code_hash)? {
                return Ok(Some(self.remember(code_hash, code)))
            }
        }
        if let Some(mdbx) = self.mdbx.as_ref() {
            if let Some(code) = mdbx.code_by_hash(code_hash)? {
                // Verified like every other source: a wrong value under this
                // key would otherwise diverge the replay unnoticed.
                if keccak256(&code) == code_hash {
                    self.mdbx_hits.fetch_add(1, Relaxed);
                    return Ok(Some(self.remember(code_hash, code)))
                }
            }
        }
        Ok(None)
    }

    fn remember(&self, code_hash: B256, code: Vec<u8>) -> Bytecode {
        let bytecode = Self::shared(Bytecode::new_raw(code.into()));
        self.cache.insert(code_hash, bytecode.clone());
        Self::remember_locally(code_hash, &bytecode);
        bytecode
    }

    /// Rebuilds legacy code over leaked, static copies of its analysed bytes
    /// and jump table. `Bytes` over static memory clones without touching a
    /// reference count, so every thread can hold its own `Bytecode` (its own
    /// `Arc`) over the one copy of the code: the cores of a CCD then share
    /// that copy in L3 instead of each pulling a private one from DRAM, and
    /// nothing shared is written when a frame clones it. The leak is bounded
    /// by the code cache, which keeps every contract for the life of the run
    /// anyway. Delegation designators keep their ordinary form.
    fn shared(bytecode: Bytecode) -> Bytecode {
        if !bytecode.is_legacy() {
            return bytecode;
        }
        let Some(jump_table) = bytecode.legacy_jump_table() else {
            return bytecode;
        };
        let original_len = bytecode.original_byte_slice().len();
        let bytes: &'static [u8] = Box::leak(bytecode.bytes_slice().to_vec().into_boxed_slice());
        let table: &'static [u8] = Box::leak(jump_table.as_slice().to_vec().into_boxed_slice());
        let jump_table = JumpTable::from_static_slice(table, jump_table.len());
        // SAFETY: the bytes and the jump table are the ones the analysis of
        // this code produced, copied verbatim.
        unsafe { Bytecode::new_analyzed(Bytes::from_static(bytes), original_len, jump_table) }
    }

    fn remember_locally(code_hash: B256, code: &Bytecode) {
        LOCAL_CODES.with(|local| {
            let mut local = local.borrow_mut();
            if local.len() >= LOCAL_CODES_CAPACITY {
                local.clear();
            }
            local.insert(code_hash, Self::private(code));
        });
    }

    /// This thread's own `Bytecode` over the shared code: a clone of the
    /// shared one would share its reference count with every other thread
    /// running the same contract, and a hot contract's count then bounces
    /// between all their caches on every account load. Over static bytes the
    /// private one costs one small allocation; anything else gets a private
    /// copy of the bytes.
    fn private(code: &Bytecode) -> Bytecode {
        if code.is_legacy() {
            if let Some(jump_table) = code.legacy_jump_table() {
                let original_len = code.original_byte_slice().len();
                // SAFETY: the same bytes and jump table the shared code holds.
                return unsafe {
                    Bytecode::new_analyzed(code.bytes_ref().clone(), original_len, jump_table.clone())
                };
            }
        }
        Bytecode::new_raw(Bytes::copy_from_slice(code.original_byte_slice()))
    }
}

/// Per-thread front of the code cache; cleared wholesale when full rather
/// than evicted, since a worker's working set turns over with the blocks it
/// is handed.
const LOCAL_CODES_CAPACITY: usize = 8192;

thread_local! {
    static LOCAL_CODES: std::cell::RefCell<alloy_primitives::map::B256Map<Bytecode>> =
        std::cell::RefCell::new(alloy_primitives::map::B256Map::with_capacity_and_hasher(
            LOCAL_CODES_CAPACITY,
            Default::default(),
        ));
}

pub(super) struct ExternalSourceDatabase<DB> {
    inner: DB,
    codes: Option<Arc<CodesFreezer>>,
    blocks: Option<Arc<crate::cli::evm::geth_freezer::GethBlockSource>>,
}

impl<DB: std::fmt::Debug> std::fmt::Debug for ExternalSourceDatabase<DB> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExternalSourceDatabase")
            .field("inner", &self.inner)
            .field("has_codes", &self.codes.is_some())
            .field("has_blocks", &self.blocks.is_some())
            .finish()
    }
}

impl<DB> ExternalSourceDatabase<DB> {
    pub(super) const fn new(
        inner: DB,
        codes: Option<Arc<CodesFreezer>>,
        blocks: Option<Arc<crate::cli::evm::geth_freezer::GethBlockSource>>,
    ) -> Self {
        Self {
            inner,
            codes,
            blocks,
        }
    }
}

impl<DB: RevmDatabase<Error = ProviderError>> RevmDatabase for ExternalSourceDatabase<DB> {
    type Error = ProviderError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.inner.basic(address)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        if let Some(codes) = self.codes.as_ref() {
            match codes.code_by_hash(code_hash) {
                Ok(Some(code)) => return Ok(Bytecode::new_raw(code.into())),
                // A miss falls through to the database rather than failing: the
                // freezer may simply not cover this contract.
                Ok(None) => {}
                Err(error) => return Err(ProviderError::other(CodeLookupFailed(error.to_string()))),
            }
        }
        self.inner.code_by_hash(code_hash)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        self.inner.storage(address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        if let Some(blocks) = self.blocks.as_ref() {
            match blocks.block_hash(number) {
                Ok(hash) => return Ok(hash),
                Err(error) => {
                    return Err(ProviderError::other(CodeLookupFailed(error.to_string())))
                }
            }
        }
        self.inner.block_hash(number)
    }
}

/// Wraps a freezer error so it can travel as a `ProviderError`.
#[derive(Debug)]
struct CodeLookupFailed(String);

impl std::fmt::Display for CodeLookupFailed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for CodeLookupFailed {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn codes_dir() -> Option<PathBuf> {
        let path = PathBuf::from(r"D:/n42-codes-25765565");
        path.join("codes.hidx").exists().then_some(path)
    }

    #[test]
    fn looks_up_code_through_the_gov5_index() {
        let Some(directory) = codes_dir() else { return };
        let freezer = CodesFreezer::open(&directory).unwrap();
        assert!(freezer.entries() > 1_000_000, "index looks truncated");

        // Take hashes from the verified sidecar when it is around, so this
        // checks real lookups rather than only the miss path.
        let Ok(verified) = fs::read(directory.join("codes.rhidx")) else {
            return
        };
        let entries = (verified.len() - 8) / 42;
        for entry in [0usize, 1, entries / 2, entries - 1] {
            let base = 8 + entry * 42;
            let hash = B256::from_slice(&verified[base..base + 32]);
            let code = freezer
                .code_by_hash(hash)
                .unwrap()
                .expect("a known hash resolves");
            assert_eq!(keccak256(&code), hash);
        }
    }

    #[test]
    fn reports_a_miss_for_an_unknown_hash() {
        let Some(directory) = codes_dir() else { return };
        let freezer = CodesFreezer::open(&directory).unwrap();
        // The MPHF still maps this onto some slot; the keccak check is what
        // turns it into a miss.
        assert!(freezer.code_by_hash(B256::repeat_byte(0xab)).unwrap().is_none());
    }
}
