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
use alloy_primitives::{keccak256, Address, B256, U256};
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
