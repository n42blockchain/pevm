// SPDX-License-Identifier: MIT OR Apache-2.0

//! Reading contract bytecode out of an N42 codes freezer.
//!
//! Witness replay needs code, and the witness itself never carries it. Pointing
//! at a codes freezer means replay does not need reth's `Bytecodes` table, which
//! is the last piece of state it would otherwise depend on.
//!
//! Layout is `codes.hoff` (a slot-ordered offset table) plus `codes.NNNN.cdat`
//! (one zstd frame per contract). gov5 finds a slot through a RecSplit minimal
//! perfect hash in `codes.hidx`, which stores no keys and answers arbitrarily
//! for anything outside its build set - correctness there rests on the reader
//! checking `keccak(code) == code_hash` afterwards.
//!
//! Rather than reimplement that MPHF bit-for-bit, this reader derives its own
//! index the same way the check does: decompress every slot once, hash it, and
//! record where it lives. The result is written next to the freezer as
//! `codes.rhidx` so later runs just map it.

use crate::revm::{
    state::{AccountInfo, Bytecode},
    Database as RevmDatabase,
};
use alloy_primitives::{keccak256, Address, B256, U256};
use reth_storage_errors::provider::ProviderError;
use std::sync::Arc;
use eyre::{Context, Result};
use rayon::prelude::*;
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};
use tracing::info;

/// `[file_number: u16 LE][offset: u32 LE][length: u32 LE]`
const HOFF_ENTRY_SIZE: usize = 10;

/// `RHIX` + version, then `[code_hash: 32][file_number: u16 LE][offset: u32 LE][length: u32 LE]`
/// per entry, sorted by hash.
const RHIDX_MAGIC: &[u8; 4] = b"RHIX";
const RHIDX_HEADER_SIZE: usize = 8;
const RHIDX_ENTRY_SIZE: usize = 42;

/// Content-addressed view over a codes freezer.
pub(super) struct CodesFreezer {
    /// Sorted by hash, so a lookup is a binary search over a mapping.
    index: memmap2::Mmap,
    data: Vec<memmap2::Mmap>,
    entries: usize,
}

impl std::fmt::Debug for CodesFreezer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CodesFreezer")
            .field("entries", &self.entries)
            .field("data_files", &self.data.len())
            .finish()
    }
}

impl CodesFreezer {
    /// Opens the freezer, building `codes.rhidx` first if it is not there yet.
    pub(super) fn open(directory: &Path) -> Result<Self> {
        let data = map_data_files(directory)?;
        let index_path = directory.join("codes.rhidx");
        if !index_path.exists() {
            build_hash_index(directory, &data, &index_path)?;
        }

        let index_file = fs::File::open(&index_path)
            .wrap_err_with(|| format!("failed to open {}", index_path.display()))?;
        // SAFETY: the freezer is read-only here and is not written while a run
        // holds it mapped.
        let index = unsafe { memmap2::Mmap::map(&index_file) }
            .wrap_err_with(|| format!("failed to map {}", index_path.display()))?;

        if index.len() < RHIDX_HEADER_SIZE || &index[0..4] != RHIDX_MAGIC {
            eyre::bail!("{} is not an RHIX index", index_path.display())
        }
        if (index.len() - RHIDX_HEADER_SIZE) % RHIDX_ENTRY_SIZE != 0 {
            eyre::bail!("{} ends with a partial entry", index_path.display())
        }
        let entries = (index.len() - RHIDX_HEADER_SIZE) / RHIDX_ENTRY_SIZE;

        Ok(Self {
            index,
            data,
            entries,
        })
    }

    /// Number of distinct contracts in the freezer.
    pub(super) const fn entries(&self) -> usize {
        self.entries
    }

    /// Returns the code with this hash, or `None` if the freezer does not hold it.
    pub(super) fn code_by_hash(&self, code_hash: B256) -> Result<Option<Vec<u8>>> {
        let Some(slot) = self.find(code_hash) else {
            return Ok(None)
        };
        let (file_number, offset, length) = self.locate(slot);
        let blob = self.blob(file_number, offset, length)?;
        let code = zstd::stream::decode_all(blob)
            .wrap_err_with(|| format!("failed to decompress code {code_hash:?}"))?;
        Ok(Some(code))
    }

    /// Binary search over the hash-sorted index.
    fn find(&self, code_hash: B256) -> Option<usize> {
        let (mut low, mut high) = (0usize, self.entries);
        while low < high {
            let middle = (low + high) / 2;
            match self.hash_at(middle).cmp(code_hash.as_slice()) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Some(middle),
            }
        }
        None
    }

    fn hash_at(&self, entry: usize) -> &[u8] {
        let position = RHIDX_HEADER_SIZE + entry * RHIDX_ENTRY_SIZE;
        &self.index[position..position + 32]
    }

    fn locate(&self, entry: usize) -> (u16, u32, u32) {
        let position = RHIDX_HEADER_SIZE + entry * RHIDX_ENTRY_SIZE + 32;
        (
            u16::from_le_bytes(self.index[position..position + 2].try_into().unwrap()),
            u32::from_le_bytes(self.index[position + 2..position + 6].try_into().unwrap()),
            u32::from_le_bytes(self.index[position + 6..position + 10].try_into().unwrap()),
        )
    }

    fn blob(&self, file_number: u16, offset: u32, length: u32) -> Result<&[u8]> {
        let mapped = self
            .data
            .get(file_number as usize)
            .ok_or_else(|| eyre::eyre!("codes data file {file_number} is missing"))?;
        let start = offset as usize;
        let end = start + length as usize;
        if end > mapped.len() {
            eyre::bail!(
                "codes entry spans {}..{} which is outside file {} ({} bytes)",
                start,
                end,
                file_number,
                mapped.len()
            )
        }
        Ok(&mapped[start..end])
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

/// Decompresses every slot once to learn its hash, then writes a sorted index.
///
/// This is the expensive path - a few gigabytes of zstd and one keccak per
/// contract - which is why the result is kept on disk.
fn build_hash_index(directory: &Path, data: &[memmap2::Mmap], index_path: &Path) -> Result<()> {
    let hoff_path = directory.join("codes.hoff");
    let hoff = fs::read(&hoff_path)
        .wrap_err_with(|| format!("failed to read {}", hoff_path.display()))?;
    if hoff.len() % HOFF_ENTRY_SIZE != 0 {
        eyre::bail!("{} ends with a partial entry", hoff_path.display())
    }
    let slots = hoff.len() / HOFF_ENTRY_SIZE;
    info!(
        slots,
        path = %index_path.display(),
        "Building the codes hash index (one-time, decompresses every contract)"
    );

    let mut entries: Vec<([u8; 32], u16, u32, u32)> = (0..slots)
        .into_par_iter()
        .map(|slot| -> Result<Option<([u8; 32], u16, u32, u32)>> {
            let position = slot * HOFF_ENTRY_SIZE;
            let file_number = u16::from_le_bytes(hoff[position..position + 2].try_into().unwrap());
            let offset = u32::from_le_bytes(hoff[position + 2..position + 6].try_into().unwrap());
            let length = u32::from_le_bytes(hoff[position + 6..position + 10].try_into().unwrap());

            let Some(mapped) = data.get(file_number as usize) else {
                return Ok(None)
            };
            let start = offset as usize;
            let end = start + length as usize;
            if end > mapped.len() {
                return Ok(None)
            }
            // A slot that does not decompress is not a corrupt freezer: the
            // offset table is written per slot and unused slots can hold
            // anything. Skip it rather than failing the build.
            let Ok(code) = zstd::stream::decode_all(&mapped[start..end]) else {
                return Ok(None)
            };
            Ok(Some((keccak256(&code).0, file_number, offset, length)))
        })
        .filter_map(|result| result.transpose())
        .collect::<Result<Vec<_>>>()?;

    entries.par_sort_unstable_by(|left, right| left.0.cmp(&right.0));
    entries.dedup_by(|left, right| left.0 == right.0);

    let temporary = index_path.with_extension("rhidx.tmp");
    {
        let file = fs::File::create(&temporary)
            .wrap_err_with(|| format!("failed to create {}", temporary.display()))?;
        let mut writer = std::io::BufWriter::new(file);
        writer.write_all(RHIDX_MAGIC)?;
        writer.write_all(&[1, 0, 0, 0])?;
        for (hash, file_number, offset, length) in &entries {
            writer.write_all(hash)?;
            writer.write_all(&file_number.to_le_bytes())?;
            writer.write_all(&offset.to_le_bytes())?;
            writer.write_all(&length.to_le_bytes())?;
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
    }
    fs::rename(&temporary, index_path)
        .wrap_err_with(|| format!("failed to install {}", index_path.display()))?;

    info!(
        contracts = entries.len(),
        path = %index_path.display(),
        "Codes hash index built"
    );
    Ok(())
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

    fn codes_dir() -> Option<PathBuf> {
        let path = PathBuf::from(r"D:/n42-codes-25765565");
        path.join("codes.hoff").exists().then_some(path)
    }

    #[test]
    fn looks_up_code_by_its_hash() {
        let Some(directory) = codes_dir() else { return };
        let freezer = CodesFreezer::open(&directory).unwrap();
        assert!(freezer.entries() > 1_000_000, "index looks truncated");

        // Walk the index itself and re-verify: whatever it hands back must hash
        // to the key it was found under.
        for entry in [0usize, 1, freezer.entries() / 2, freezer.entries() - 1] {
            let hash = B256::from_slice(freezer.hash_at(entry));
            let code = freezer
                .code_by_hash(hash)
                .unwrap()
                .expect("index entry resolves");
            assert_eq!(keccak256(&code), hash);
        }
    }

    #[test]
    fn reports_a_miss_for_an_unknown_hash() {
        let Some(directory) = codes_dir() else { return };
        let freezer = CodesFreezer::open(&directory).unwrap();
        assert!(freezer.code_by_hash(B256::repeat_byte(0xab)).unwrap().is_none());
    }
}
