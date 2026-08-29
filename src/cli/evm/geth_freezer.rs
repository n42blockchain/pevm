// SPDX-License-Identifier: MIT OR Apache-2.0

//! Reading blocks straight out of a go-ethereum ancient store.
//!
//! Witness replay needs headers, bodies and code - never account or storage
//! state. A geth freezer already holds the first two, so pointing at one avoids
//! standing up a multi-terabyte reth archive just to feed the executor.
//!
//! The layout is geth's freezer table: `<name>.cidx` holding 6-byte entries and
//! `<name>.NNNN.cdat` holding the payloads. N42's own freezer is the same shape
//! with a 16-byte header added, which is why the file names look familiar.

use alloy_consensus::{BlockBody, Header};
use alloy_primitives::{Address, B256};
use alloy_eips::eip4895::Withdrawals;
use alloy_rlp::Decodable;
use eyre::{Context, Result};
use reth_ethereum_primitives::{Block, TransactionSigned};
use reth_primitives_traits::RecoveredBlock;
use std::{
    fs,
    path::{Path, PathBuf},
};

/// `[file_number: u16 BE][offset: u32 BE]`
const INDEX_ENTRY_SIZE: usize = 6;

/// One table of a geth ancient store.
///
/// Index entry `i` is where item `i` starts, so an item is bounded by entries
/// `i` and `i + 1` - which is why a table with N items has N + 1 entries. When
/// those two entries name different files the item begins at offset 0 of the
/// later one, because the writer never splits an item across files.
pub(super) struct GethFreezerTable {
    name: String,
    index: memmap2::Mmap,
    data: Vec<memmap2::Mmap>,
    items: u64,
    /// `.cidx`/`.cdat` payloads are snappy-compressed; `.ridx`/`.rdat` are not.
    /// geth stores hashes raw, since 32 random bytes do not compress.
    compressed: bool,
}

impl std::fmt::Debug for GethFreezerTable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GethFreezerTable")
            .field("name", &self.name)
            .field("items", &self.items)
            .field("data_files", &self.data.len())
            .finish()
    }
}

impl GethFreezerTable {
    pub(super) fn open(directory: &Path, name: &str) -> Result<Self> {
        // A table is written either compressed or raw, never both.
        let compressed_index = directory.join(format!("{name}.cidx"));
        let (index_path, compressed) = if compressed_index.exists() {
            (compressed_index, true)
        } else {
            (directory.join(format!("{name}.ridx")), false)
        };
        let index_file = fs::File::open(&index_path)
            .wrap_err_with(|| format!("failed to open {}", index_path.display()))?;
        // SAFETY: the ancient store is read-only here and is not written while
        // a run holds it mapped.
        let index = unsafe { memmap2::Mmap::map(&index_file) }
            .wrap_err_with(|| format!("failed to map {}", index_path.display()))?;

        if index.len() < INDEX_ENTRY_SIZE * 2 || index.len() % INDEX_ENTRY_SIZE != 0 {
            eyre::bail!(
                "{} is {} bytes, not a whole number of 6-byte entries",
                index_path.display(),
                index.len()
            )
        }
        let items = (index.len() / INDEX_ENTRY_SIZE) as u64 - 1;

        // The highest file the index names is the last one that exists.
        let (last_file, _) = read_index_entry(&index, items);
        let mut data = Vec::with_capacity(last_file as usize + 1);
        for file_number in 0..=last_file {
            let path = data_path(directory, name, file_number, compressed);
            let file = fs::File::open(&path)
                .wrap_err_with(|| format!("failed to open {}", path.display()))?;
            // SAFETY: same as the index above.
            let mapped = unsafe { memmap2::Mmap::map(&file) }
                .wrap_err_with(|| format!("failed to map {}", path.display()))?;
            data.push(mapped);
        }

        Ok(Self {
            name: name.to_string(),
            index,
            data,
            items,
            compressed,
        })
    }

    /// Number of items in the table; item N is block N for the chain tables.
    pub(super) const fn items(&self) -> u64 {
        self.items
    }

    /// Returns item `item`, decompressed.
    pub(super) fn get(&self, item: u64) -> Result<Vec<u8>> {
        if item >= self.items {
            eyre::bail!(
                "{} holds items 0..{}, asked for {}",
                self.name,
                self.items.saturating_sub(1),
                item
            )
        }

        let (start_file, start_offset) = read_index_entry(&self.index, item);
        let (end_file, end_offset) = read_index_entry(&self.index, item + 1);

        // An item never straddles two files: when the boundaries disagree the
        // item is at the start of the later file.
        let (file_number, start, end) = if start_file == end_file {
            (start_file, start_offset as usize, end_offset as usize)
        } else {
            (end_file, 0usize, end_offset as usize)
        };

        let mapped = self.data.get(file_number as usize).ok_or_else(|| {
            eyre::eyre!("{}: data file {} is missing", self.name, file_number)
        })?;
        if start > end || end > mapped.len() {
            eyre::bail!(
                "{}: item {} spans {}..{} which is outside file {} ({} bytes)",
                self.name,
                item,
                start,
                end,
                file_number,
                mapped.len()
            )
        }

        let blob = &mapped[start..end];
        if !self.compressed {
            return Ok(blob.to_vec())
        }
        snap::raw::Decoder::new()
            .decompress_vec(blob)
            .wrap_err_with(|| format!("{}: failed to decompress item {}", self.name, item))
    }
}

fn read_index_entry(index: &[u8], entry: u64) -> (u16, u32) {
    let position = entry as usize * INDEX_ENTRY_SIZE;
    (
        u16::from_be_bytes(index[position..position + 2].try_into().unwrap()),
        u32::from_be_bytes(index[position + 2..position + 6].try_into().unwrap()),
    )
}

fn data_path(directory: &Path, name: &str, file_number: u16, compressed: bool) -> PathBuf {
    let extension = if compressed { "cdat" } else { "rdat" };
    directory.join(format!("{name}.{file_number:04}.{extension}"))
}

/// Blocks read from a geth ancient store, ready for the executor.
#[derive(Debug)]
pub(super) struct GethBlockSource {
    headers: GethFreezerTable,
    bodies: GethFreezerTable,
    /// Canonical hashes, so BLOCKHASH does not need a state provider either.
    /// A store copied without its `hashes` table (headers and bodies are all
    /// a replay needs) has them computed from the headers instead.
    hashes: Option<GethFreezerTable>,
    /// gov5's `senders` table: one item per block, the transactions' senders
    /// as `20 × n` bytes in transaction order. Recovering a sender from its
    /// signature costs tens of microseconds; with nine million transactions
    /// in fifty thousand blocks that is a quarter of the replay's CPU, and
    /// reth's recovery also fans out onto rayon from every worker at once.
    senders: Option<super::witness::WitnessFreezerReader>,
}

impl GethBlockSource {
    pub(super) fn open(directory: &Path) -> Result<Self> {
        let headers = GethFreezerTable::open(directory, "headers")?;
        let bodies = GethFreezerTable::open(directory, "bodies")?;
        let hashes = match GethFreezerTable::open(directory, "hashes") {
            Ok(table) => Some(table),
            Err(error) => {
                tracing::info!(
                    %error,
                    "no hashes table in the ancient store; block hashes are computed from the headers"
                );
                None
            }
        };
        if headers.items() != bodies.items() {
            eyre::bail!(
                "ancient store is inconsistent: {} headers but {} bodies",
                headers.items(),
                bodies.items()
            )
        }
        Ok(Self {
            headers,
            bodies,
            hashes,
            senders: None,
        })
    }

    /// Reads senders from gov5's `senders` table in `directory` instead of
    /// recovering them from the signatures.
    pub(super) fn with_senders(mut self, directory: &Path) -> Result<Self> {
        let senders = super::witness::WitnessFreezerReader::open_table(directory, "senders", true)?;
        tracing::info!(
            items = senders.items(),
            path = %directory.display(),
            "Reading transaction senders from the senders table"
        );
        self.senders = Some(senders);
        Ok(self)
    }

    /// Canonical hash of `number`, for the BLOCKHASH opcode.
    pub(super) fn block_hash(&self, number: u64) -> Result<B256> {
        let Some(hashes) = self.hashes.as_ref() else {
            // The hash of a block is the hash of its header's RLP, which the
            // headers table holds verbatim.
            return Ok(alloy_primitives::keccak256(self.headers.get(number)?));
        };
        let raw = hashes.get(number)?;
        if raw.len() != 32 {
            eyre::bail!("hash for block {} is {} bytes, not 32", number, raw.len())
        }
        Ok(B256::from_slice(&raw))
    }

    /// Highest block the store holds.
    pub(super) const fn last_block(&self) -> u64 {
        self.headers.items().saturating_sub(1)
    }

    /// Reads `range` and recovers each block's senders.
    ///
    /// The ancient store keeps no senders, so they are recovered from the
    /// signatures - the same fallback reth uses when its sender table has no
    /// entry for a transaction.
    pub(super) fn blocks(
        &self,
        range: std::ops::RangeInclusive<u64>,
    ) -> Result<Vec<RecoveredBlock<Block>>> {
        let mut out = Vec::with_capacity((range.end() - range.start() + 1) as usize);
        // The senders batch covering the current block, decoded once per
        // batch: tasks are short and consecutive, so this is nearly once.
        let mut senders_batch: Option<(u64, Vec<Vec<u8>>)> = None;
        for number in range {
            out.push(self.block(number, &mut senders_batch)?);
        }
        Ok(out)
    }

    /// Sums transactions and gas over `range`.
    ///
    /// Headers and bodies only - no state, no execution - so the totals come
    /// from the block data rather than from a run that could have skipped or
    /// double-counted a block. Reading them once is the whole cost, which is
    /// why the range is split across threads.
    pub(super) fn census(
        &self,
        range: std::ops::RangeInclusive<u64>,
        threads: usize,
    ) -> Result<Census> {
        let (first, last) = (*range.start(), *range.end());
        if last >= self.headers.items() {
            eyre::bail!(
                "ancient store holds blocks 0..{}, but {} was requested",
                self.headers.items().saturating_sub(1),
                last
            )
        }
        let threads = threads.max(1);
        let total = last - first + 1;
        let per_thread = total.div_ceil(threads as u64);

        let results: Vec<Result<Census>> = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(threads);
            for index in 0..threads as u64 {
                let start = first + index * per_thread;
                if start > last {
                    break
                }
                let end = (start + per_thread - 1).min(last);
                handles.push(scope.spawn(move || {
                    let mut census = Census::default();
                    for number in start..=end {
                        let header_rlp = self.headers.get(number)?;
                        let header = Header::decode(&mut header_rlp.as_slice())
                            .wrap_err_with(|| format!("failed to decode header {number}"))?;
                        let body_rlp = self.bodies.get(number)?;
                        let transactions = body_transaction_count(&body_rlp)
                            .wrap_err_with(|| format!("failed to walk body {number}"))?;

                        census.blocks += 1;
                        census.transactions += transactions;
                        census.gas_used += u128::from(header.gas_used);
                    }
                    Ok(census)
                }));
            }
            handles.into_iter().map(|handle| handle.join().unwrap()).collect()
        });

        let mut total_census = Census::default();
        for result in results {
            total_census.absorb(result?);
        }
        Ok(total_census)
    }

    fn block(
        &self,
        number: u64,
        senders_batch: &mut Option<(u64, Vec<Vec<u8>>)>,
    ) -> Result<RecoveredBlock<Block>> {
        let header_rlp = self.headers.get(number)?;
        let header = Header::decode(&mut header_rlp.as_slice())
            .wrap_err_with(|| format!("failed to decode header {number}"))?;

        let body_rlp = self.bodies.get(number)?;
        let body = decode_body(&body_rlp)
            .wrap_err_with(|| format!("failed to decode body {number}"))?;

        if let Some(table) = self.senders.as_ref() {
            if number < table.items() {
                use super::witness::WitnessFreezerReader as Reader;
                let batch = Reader::batch_of(number);
                if !matches!(senders_batch, Some((cached, _)) if *cached == batch) {
                    *senders_batch = Some((batch, table.read_batch(batch)?));
                }
                let raw = senders_batch
                    .as_ref()
                    .and_then(|(_, entries)| entries.get((number - Reader::batch_start(batch)) as usize))
                    .ok_or_else(|| eyre::eyre!("senders batch {batch} is missing block {number}"))?;
                if raw.len() != body.transactions.len() * 20 {
                    eyre::bail!(
                        "senders item {number} holds {} bytes for {} transactions",
                        raw.len(),
                        body.transactions.len()
                    )
                }
                let senders = raw.chunks_exact(20).map(Address::from_slice).collect();
                return Ok(RecoveredBlock::new_unhashed(Block::new(header, body), senders))
            }
        }
        RecoveredBlock::try_recover_unchecked(Block::new(header, body))
            .map_err(|error| eyre::eyre!("failed to recover senders for block {number}: {error}"))
    }
}

/// What a range of the store holds, counted without executing anything.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct Census {
    pub(super) blocks: u64,
    pub(super) transactions: u64,
    /// `u128` because the chain's cumulative gas passes `u64` territory only
    /// far in the future, but a sum that silently wraps would be worse than
    /// one that is obviously too wide.
    pub(super) gas_used: u128,
}

impl Census {
    fn absorb(&mut self, other: Self) {
        self.blocks += other.blocks;
        self.transactions += other.transactions;
        self.gas_used += other.gas_used;
    }
}

/// Number of transactions in a body, without decoding them.
///
/// Only the count is wanted, so the transaction list is walked by its RLP
/// headers: every item is either a list (a legacy transaction) or a byte string
/// (a typed envelope), and both give the payload length needed to step to the
/// next. Decoding the transactions would cost far more and answer the same
/// question.
fn body_transaction_count(raw: &[u8]) -> Result<u64> {
    let mut buf = raw;
    let outer = alloy_rlp::Header::decode(&mut buf)?;
    if !outer.list {
        eyre::bail!("body is not an RLP list")
    }
    let list = alloy_rlp::Header::decode(&mut buf)?;
    if !list.list {
        eyre::bail!("body does not start with a transaction list")
    }

    let mut remaining = list.payload_length;
    let mut count = 0u64;
    while remaining > 0 {
        let before = buf.len();
        let item = alloy_rlp::Header::decode(&mut buf)?;
        if buf.len() < item.payload_length {
            eyre::bail!("transaction payload runs past the end of the body")
        }
        buf = &buf[item.payload_length..];
        let consumed = before - buf.len();
        remaining = remaining
            .checked_sub(consumed)
            .ok_or_else(|| eyre::eyre!("transaction list overruns its own length"))?;
        count += 1;
    }
    Ok(count)
}

/// Decodes geth's body encoding: `[transactions, uncles, withdrawals?]`.
///
/// `BlockBody` has no `Decodable` of its own, and the withdrawals field is
/// optional rather than defaulted, so the list is walked by hand and the tail
/// decides whether a withdrawals list is present.
fn decode_body(raw: &[u8]) -> Result<BlockBody<TransactionSigned, Header>> {
    let mut buf = raw;
    let list = alloy_rlp::Header::decode(&mut buf)?;
    if !list.list {
        eyre::bail!("body is not an RLP list")
    }
    let remaining_after_body = buf
        .len()
        .checked_sub(list.payload_length)
        .ok_or_else(|| eyre::eyre!("body payload runs past the end of the input"))?;

    let transactions = Vec::<TransactionSigned>::decode(&mut buf)?;
    let ommers = Vec::<Header>::decode(&mut buf)?;
    let withdrawals = if buf.len() > remaining_after_body {
        Some(Withdrawals::decode(&mut buf)?)
    } else {
        None
    };

    if buf.len() != remaining_after_body {
        eyre::bail!("body has {} trailing bytes", buf.len() - remaining_after_body)
    }

    Ok(BlockBody {
        transactions,
        ommers,
        withdrawals,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The store used here is the operator's own mainnet ancient directory; the
    /// test is skipped when it is not present.
    fn ancient_dir() -> Option<PathBuf> {
        let path = PathBuf::from(r"D:/geth/geth/chaindata/ancient/chain");
        path.join("headers.cidx").exists().then_some(path)
    }

    /// Diagnostic: who mined a block, and how often that address mined before it.
    #[test]
    fn beneficiary_history_around_block_5305() {
        let Some(directory) = ancient_dir() else { return };
        let source = GethBlockSource::open(&directory).unwrap();

        for number in [5305u64, 5662, 5748] {
            let block = source.blocks(number..=number).unwrap().pop().unwrap();
            println!(
                "block {} beneficiary {:?} ommers {}",
                number,
                block.sealed_block().header().beneficiary,
                block.body().ommers.len()
            );
        }

        let target = source.blocks(5305..=5305).unwrap().pop().unwrap();
        let miner = target.sealed_block().header().beneficiary;
        let mut produced = 0usize;
        let mut as_ommer = 0usize;
        for number in 0..5305u64 {
            let block = source.blocks(number..=number).unwrap().pop().unwrap();
            if block.sealed_block().header().beneficiary == miner {
                produced += 1;
            }
            for ommer in &block.body().ommers {
                if ommer.beneficiary == miner {
                    as_ommer += 1;
                }
            }
        }
        println!("before 5305: mined {produced} blocks, was ommer beneficiary {as_ommer} times");
    }

    #[test]
    fn reads_the_genesis_header_and_body() {
        let Some(directory) = ancient_dir() else { return };
        let source = GethBlockSource::open(&directory).unwrap();

        let block = source.blocks(0..=0).unwrap().pop().unwrap();
        // Mainnet genesis, the one hash worth hard-coding.
        assert_eq!(
            format!("{:?}", block.hash()),
            "0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"
        );
        assert!(block.body().transactions.is_empty());
        assert!(block.body().ommers.is_empty());
    }

    #[test]
    fn recovers_senders_for_a_block_with_transactions() {
        let Some(directory) = ancient_dir() else { return };
        let source = GethBlockSource::open(&directory).unwrap();

        // 46147 carries mainnet's first transaction.
        let block = source.blocks(46147..=46147).unwrap().pop().unwrap();
        assert_eq!(block.body().transactions.len(), 1);
        assert_eq!(block.senders().len(), 1);
    }

    #[test]
    fn reports_the_stored_range() {
        let Some(directory) = ancient_dir() else { return };
        let source = GethBlockSource::open(&directory).unwrap();
        assert!(source.last_block() > 1_000_000, "store looks truncated");
    }
}
