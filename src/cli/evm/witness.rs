// SPDX-License-Identifier: MIT OR Apache-2.0

//! Keyless, ordered block-witness recording compatible with N42-gov5.

use crate::revm::{
    state::{AccountInfo, Bytecode},
    Database as RevmDatabase,
};
use alloy_primitives::{Address, B256, U256};
use eyre::{Context, Result};
use reth_storage_errors::provider::ProviderError;
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    fmt,
    fs::{self, OpenOptions},
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};
use tracing::warn;

const FREEZER_BATCH_SIZE: usize = 64;
const FREEZER_MAX_FILE_SIZE: u64 = 2_000_000_000;
const CIDX_HEADER_SIZE: u64 = 16;
const CIDX_ENTRY_SIZE: u64 = 6;
const CIDX_FLAGS_COMPRESSED_BATCH: u8 = 0x01 | 0x02;
const ZSTD_LEVEL_BETTER_COMPRESSION: i32 = 7;

const EMPTY_CODE_HASH: [u8; 32] = [
    0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c, 0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
    0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b, 0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
];

/// Handle to the raw witness bytes produced by [`WitnessDatabase`].
///
/// A block is recorded start to finish on one thread, and the executor consumes
/// the database, so the handle only has to outlive it - not cross threads. That
/// makes a plain `Rc<RefCell<_>>` enough; no lock is taken per recorded value.
#[derive(Clone, Debug)]
pub(super) struct WitnessStream(Rc<RefCell<Vec<u8>>>);

impl WitnessStream {
    /// Removes and returns all bytes currently in the stream.
    pub(super) fn take(&self) -> Vec<u8> {
        std::mem::take(&mut *self.0.borrow_mut())
    }
}

/// Database wrapper that records account and storage values in access order.
///
/// Each read appends `[len:u8][value:len]`. Addresses, storage keys, entry
/// types, and entry counts are deliberately not part of the stream. Code and
/// block-hash reads are delegated without recording.
pub(super) struct WitnessDatabase<DB> {
    inner: DB,
    stream: WitnessStream,
}

impl<DB: std::fmt::Debug> std::fmt::Debug for WitnessDatabase<DB> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WitnessDatabase")
            .field("inner", &self.inner)
            .field("stream", &self.stream)
            .finish()
    }
}

impl<DB> WitnessDatabase<DB> {
    pub(super) fn new(inner: DB) -> (Self, WitnessStream) {
        let stream = WitnessStream(Rc::new(RefCell::new(Vec::with_capacity(4096))));
        (
            Self {
                inner,
                stream: stream.clone(),
            },
            stream,
        )
    }

    fn append(&self, value: &[u8]) {
        // N42 account V2 is at most 76 bytes and an EVM storage value is at
        // most 32 bytes. Keep this guard fail-loud if either encoding changes.
        assert!(
            value.len() <= u8::MAX as usize,
            "witness value exceeds one-byte length prefix"
        );
        let mut stream = self.stream.0.borrow_mut();
        stream.push(value.len() as u8);
        stream.extend_from_slice(value);
    }
}

impl<DB: RevmDatabase> RevmDatabase for WitnessDatabase<DB> {
    type Error = DB::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let account = self.inner.basic(address)?;
        match account.as_ref() {
            Some(account) => self.append(&encode_account_v2(account)),
            None => self.append(&[]),
        }
        Ok(account)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.inner.code_by_hash(code_hash)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let value = self.inner.storage(address, index)?;
        let bytes = value.to_be_bytes::<32>();
        let first_nonzero = bytes
            .iter()
            .position(|byte| *byte != 0)
            .unwrap_or(bytes.len());
        self.append(&bytes[first_nonzero..]);
        Ok(value)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.inner.block_hash(number)
    }
}

/// N42-gov5 `StateAccount.MarshalV2` encoding.
fn encode_account_v2(account: &AccountInfo) -> Vec<u8> {
    const NONCE_BIT: u8 = 1;
    const BALANCE_BIT: u8 = 2;
    const CODE_HASH_BIT: u8 = 8;

    let mut encoded = Vec::with_capacity(76);
    encoded.push(0); // field bits, filled after the optional fields
    let mut field_bits = 0;

    if account.nonce != 0 {
        field_bits |= NONCE_BIT;
        put_uvarint(&mut encoded, account.nonce);
    }

    if account.balance != U256::ZERO {
        field_bits |= BALANCE_BIT;
        let bytes = account.balance.to_be_bytes::<32>();
        let first_nonzero = bytes
            .iter()
            .position(|byte| *byte != 0)
            .expect("non-zero balance has a non-zero byte");
        let trimmed = &bytes[first_nonzero..];
        encoded.push(trimmed.len() as u8);
        encoded.extend_from_slice(trimmed);
    }

    let code_hash = account.code_hash();
    if code_hash != B256::ZERO && code_hash.as_slice() != EMPTY_CODE_HASH {
        field_bits |= CODE_HASH_BIT;
        encoded.extend_from_slice(code_hash.as_slice());
    }

    encoded[0] = field_bits;
    encoded
}

/// Go `encoding/binary.PutUvarint` compatible unsigned varint.
fn put_uvarint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push(value as u8 | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

/// N42 `FreezerTable` writer for `witness.cidx` + `witness.NNNN.cdat`.
///
/// It reproduces the Go output batcher's batch-64 layout. A partial final
/// batch is recovered and rewritten on resume, as `alignOnResume` does in Go.
pub(super) struct WitnessFreezerWriter {
    output_dir: PathBuf,
    index: BufWriter<fs::File>,
    data: BufWriter<fs::File>,
    head_file: u16,
    head_size: u64,
    disk_items: u64,
    next_item: u64,
    pending: Vec<Vec<u8>>,
}

impl WitnessFreezerWriter {
    pub(super) fn open(output_dir: &Path, first_block: u64) -> Result<Self> {
        fs::create_dir_all(output_dir).wrap_err_with(|| {
            format!(
                "failed to create witness directory {}",
                output_dir.display()
            )
        })?;
        let index_path = output_dir.join("witness.cidx");

        if !index_path.exists() {
            if first_block != 0 {
                eyre::bail!(
                    "new witness freezer must start at block 0; requested block {}",
                    first_block
                )
            }
            return Self::create(output_dir, index_path);
        }

        Self::resume(output_dir, index_path, first_block)
    }

    fn create(output_dir: &Path, index_path: PathBuf) -> Result<Self> {
        let mut index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&index_path)
            .wrap_err_with(|| format!("failed to create {}", index_path.display()))?;
        index_file
            .write_all(&cidx_header())
            .wrap_err_with(|| format!("failed to write {}", index_path.display()))?;
        index_file.seek(SeekFrom::End(0))?;

        let data_path = data_path(output_dir, 0);
        let data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&data_path)
            .wrap_err_with(|| format!("failed to create {}", data_path.display()))?;

        Ok(Self {
            output_dir: output_dir.to_path_buf(),
            index: BufWriter::new(index_file),
            data: BufWriter::new(data_file),
            head_file: 0,
            head_size: 0,
            disk_items: 0,
            next_item: 0,
            pending: Vec::with_capacity(FREEZER_BATCH_SIZE),
        })
    }

    fn resume(output_dir: &Path, index_path: PathBuf, first_block: u64) -> Result<Self> {
        let mut index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&index_path)
            .wrap_err_with(|| format!("failed to open {}", index_path.display()))?;
        let mut index_bytes = Vec::new();
        index_file.read_to_end(&mut index_bytes)?;
        validate_cidx(&index_bytes, &index_path)?;

        let items = (index_bytes.len() as u64 - CIDX_HEADER_SIZE) / CIDX_ENTRY_SIZE;
        if first_block != items {
            eyre::bail!(
                "witness freezer contains blocks 0..{}; resume must use --begin {}, got {}",
                items.saturating_sub(1),
                items,
                first_block
            )
        }

        let (head_file, disk_items, pending) = if items == 0 {
            let path = data_path(output_dir, 0);
            let size = fs::metadata(&path)
                .wrap_err_with(|| format!("failed to stat {}", path.display()))?
                .len();
            if size != 0 {
                eyre::bail!(
                    "empty witness index has non-empty data file {}",
                    path.display()
                )
            }
            (0, 0, Vec::with_capacity(FREEZER_BATCH_SIZE))
        } else {
            let last = read_index_entry(&index_bytes, items - 1);
            let path = data_path(output_dir, last.0);
            let size = fs::metadata(&path)
                .wrap_err_with(|| format!("failed to stat {}", path.display()))?
                .len();
            if u64::from(last.1) > size {
                eyre::bail!("witness index offset is past end of {}", path.display())
            }

            let tail = (items % FREEZER_BATCH_SIZE as u64) as usize;
            if tail == 0 {
                (last.0, items, Vec::with_capacity(FREEZER_BATCH_SIZE))
            } else {
                let batch_start = items - tail as u64;
                let batch_entry = read_index_entry(&index_bytes, batch_start);
                if batch_entry.0 != last.0 || batch_entry.1 != last.1 {
                    eyre::bail!("partial witness batch does not share one cidx offset")
                }
                let recovered = read_batch(&path, u64::from(batch_entry.1), size, tail)?;
                let data_file = OpenOptions::new().write(true).open(&path)?;
                data_file.set_len(u64::from(batch_entry.1))?;
                index_file.set_len(CIDX_HEADER_SIZE + batch_start * CIDX_ENTRY_SIZE)?;
                (batch_entry.0, batch_start, recovered)
            }
        };

        let data_path = data_path(output_dir, head_file);
        let mut data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&data_path)
            .wrap_err_with(|| format!("failed to open {}", data_path.display()))?;
        data_file.seek(SeekFrom::End(0))?;
        let head_size = data_file.metadata()?.len();
        index_file.seek(SeekFrom::End(0))?;

        Ok(Self {
            output_dir: output_dir.to_path_buf(),
            index: BufWriter::new(index_file),
            data: BufWriter::new(data_file),
            head_file,
            head_size,
            disk_items,
            next_item: items,
            pending,
        })
    }

    pub(super) fn push(&mut self, block_number: u64, witness: Vec<u8>) -> Result<()> {
        if block_number != self.next_item {
            eyre::bail!(
                "witness append out of order: expected block {}, got {}",
                self.next_item,
                block_number
            )
        }
        self.pending.push(witness);
        self.next_item += 1;
        if self.pending.len() == FREEZER_BATCH_SIZE {
            self.flush_pending()?;
        }
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<u64> {
        self.flush_pending()?;
        self.index.flush()?;
        self.data.flush()?;
        self.index.get_ref().sync_all()?;
        self.data.get_ref().sync_all()?;
        Ok(self.disk_items)
    }

    fn flush_pending(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        let raw = encode_batch(&self.pending)?;
        let compressed = compress_batch(&raw)?;
        let blob = if compressed.len() < raw.len() {
            compressed.as_slice()
        } else {
            raw.as_slice()
        };

        if self.head_size + blob.len() as u64 > FREEZER_MAX_FILE_SIZE {
            self.data.flush()?;
            self.data.get_ref().sync_all()?;
            self.head_file = self
                .head_file
                .checked_add(1)
                .ok_or_else(|| eyre::eyre!("witness freezer data file number overflow"))?;
            let path = data_path(&self.output_dir, self.head_file);
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
                .wrap_err_with(|| format!("failed to create {}", path.display()))?;
            self.data = BufWriter::new(file);
            self.head_size = 0;
        }

        let offset = u32::try_from(self.head_size)
            .wrap_err("witness freezer offset exceeds cidx u32 field")?;
        self.data.write_all(blob)?;
        let entry = cidx_entry(self.head_file, offset);
        for _ in 0..self.pending.len() {
            self.index.write_all(&entry)?;
        }
        self.head_size += blob.len() as u64;
        self.disk_items += self.pending.len() as u64;
        self.pending.clear();

        // Match the append-only streaming use case: completed batches become
        // visible without retaining all blocks in memory.
        self.data.flush()?;
        self.index.flush()?;
        Ok(())
    }
}

fn cidx_header() -> [u8; CIDX_HEADER_SIZE as usize] {
    let mut header = [0u8; CIDX_HEADER_SIZE as usize];
    header[0..4].copy_from_slice(b"NCIX");
    header[4] = 1;
    header[5] = CIDX_FLAGS_COMPRESSED_BATCH;
    header[6] = FREEZER_BATCH_SIZE as u8;
    header[7] = CIDX_ENTRY_SIZE as u8;
    // header[8..16] is the big-endian start item, zero for an untrimmed table.
    header
}

fn cidx_entry(file_number: u16, offset: u32) -> [u8; CIDX_ENTRY_SIZE as usize] {
    let mut entry = [0u8; CIDX_ENTRY_SIZE as usize];
    entry[0..2].copy_from_slice(&file_number.to_be_bytes());
    entry[2..6].copy_from_slice(&offset.to_be_bytes());
    entry
}

fn validate_cidx(bytes: &[u8], path: &Path) -> Result<()> {
    if bytes.len() < CIDX_HEADER_SIZE as usize || &bytes[0..4] != b"NCIX" {
        eyre::bail!("{} is not an N42 NCIX witness index", path.display())
    }
    if bytes[4] != 1
        || bytes[5] != CIDX_FLAGS_COMPRESSED_BATCH
        || bytes[6] != FREEZER_BATCH_SIZE as u8
        || bytes[7] != CIDX_ENTRY_SIZE as u8
        || bytes[8..16] != [0; 8]
    {
        eyre::bail!("{} has unsupported witness cidx metadata", path.display())
    }
    if (bytes.len() as u64 - CIDX_HEADER_SIZE) % CIDX_ENTRY_SIZE != 0 {
        eyre::bail!("{} ends with a partial cidx entry", path.display())
    }
    Ok(())
}

fn read_index_entry(bytes: &[u8], item: u64) -> (u16, u32) {
    read_index_entry_at(bytes, CIDX_HEADER_SIZE, item)
}

fn read_index_entry_at(bytes: &[u8], header: u64, item: u64) -> (u16, u32) {
    let position = (header + item * CIDX_ENTRY_SIZE) as usize;
    (
        u16::from_be_bytes(bytes[position..position + 2].try_into().unwrap()),
        u32::from_be_bytes(bytes[position + 2..position + 6].try_into().unwrap()),
    )
}

fn data_path(output_dir: &Path, file_number: u16) -> PathBuf {
    table_data_path(output_dir, "witness", file_number)
}

fn table_data_path(directory: &Path, name: &str, file_number: u16) -> PathBuf {
    directory.join(format!("{name}.{file_number:04}.cdat"))
}

fn encode_batch(entries: &[Vec<u8>]) -> Result<Vec<u8>> {
    let capacity = entries.iter().try_fold(0usize, |total, entry| {
        total
            .checked_add(4 + entry.len())
            .ok_or_else(|| eyre::eyre!("witness batch size overflow"))
    })?;
    let mut raw = Vec::with_capacity(capacity);
    for entry in entries {
        let length = u32::try_from(entry.len())
            .wrap_err("individual block witness exceeds u32 batch length")?;
        raw.extend_from_slice(&length.to_le_bytes());
        raw.extend_from_slice(entry);
    }
    Ok(raw)
}

fn compress_batch(raw: &[u8]) -> Result<Vec<u8>> {
    // klauspost/zstd (used by gov5) enables the frame checksum by default and
    // its SpeedBetterCompression preset is approximately zstd level 7-8.
    let mut encoder = zstd::stream::Encoder::new(Vec::new(), ZSTD_LEVEL_BETTER_COMPRESSION)
        .wrap_err("failed to create witness zstd encoder")?;
    encoder.include_checksum(true)?;
    encoder.set_pledged_src_size(Some(raw.len() as u64))?;
    encoder.write_all(raw)?;
    encoder
        .finish()
        .wrap_err("failed to compress witness batch")
}

fn read_batch(path: &Path, start: u64, end: u64, expected: usize) -> Result<Vec<Vec<u8>>> {
    let mut file = fs::File::open(path)?;
    file.seek(SeekFrom::Start(start))?;
    let mut blob = vec![0u8; (end - start) as usize];
    file.read_exact(&mut blob)?;
    Ok(decode_batch(&blob, expected, &path.display().to_string())?.into_entries())
}

/// Splits one freezer batch blob into the block witnesses it holds.
///
/// A batch is only stored compressed when that was smaller than the raw
/// encoding, so the zstd frame magic decides which of the two is on disk.
/// A decoded batch: the decompressed bytes once, and where each item lies in
/// them. Every worker decodes the batch its task falls in, so the copy per
/// item that a `Vec<Vec<u8>>` costs was paid a few thousand times per batch
/// across the run.
#[derive(Debug)]
pub(super) struct DecodedBatch {
    raw: Vec<u8>,
    bounds: Vec<(usize, usize)>,
}

impl DecodedBatch {
    pub(super) fn get(&self, index: usize) -> Option<&[u8]> {
        self.bounds.get(index).map(|&(start, end)| &self.raw[start..end])
    }

    pub(super) fn len(&self) -> usize {
        self.bounds.len()
    }

    pub(super) fn into_entries(self) -> Vec<Vec<u8>> {
        self.bounds.iter().map(|&(start, end)| self.raw[start..end].to_vec()).collect()
    }
}

fn decode_batch(blob: &[u8], expected: usize, source: &str) -> Result<DecodedBatch> {
    let raw: Vec<u8> = if blob.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        zstd::stream::decode_all(blob).wrap_err("failed to decode witness batch")?
    } else {
        blob.to_vec()
    };

    let mut bounds = Vec::with_capacity(expected);
    let mut position = 0;
    for _ in 0..expected {
        if position + 4 > raw.len() {
            eyre::bail!("truncated witness batch length in {}", source)
        }
        let length = u32::from_le_bytes(raw[position..position + 4].try_into().unwrap()) as usize;
        position += 4;
        if position + length > raw.len() {
            eyre::bail!("truncated witness batch payload in {}", source)
        }
        bounds.push((position, position + length));
        position += length;
    }
    if position != raw.len() {
        eyre::bail!("witness batch in {} has trailing bytes", source)
    }
    Ok(DecodedBatch { raw, bounds })
}

// ---------------------------------------------------------------------------
// Ordered sink: parallel producers -> sequential freezer
// ---------------------------------------------------------------------------

/// Out-of-order block witnesses held in memory before a submitter has to wait.
const SINK_MAX_PENDING_ENTRIES: usize = 8192;
/// Out-of-order witness bytes held in memory before a submitter has to wait.
const SINK_MAX_PENDING_BYTES: usize = 2 * 1024 * 1024 * 1024;
/// Wake-up interval used to re-check the shutdown flag while blocked.
const SINK_WAIT_TICK: Duration = Duration::from_millis(100);

/// Order-restoring bridge between parallel workers and [`WitnessFreezerWriter`].
///
/// The freezer index is positional - item N *is* block N - so appends must be
/// strictly sequential, while workers finish blocks in whatever order the
/// scheduler hands them out. Each submission is buffered until every lower
/// block has arrived; the submitting thread then drains the longest contiguous
/// run into the writer. Buffering is bounded in both entries and bytes so one
/// slow block cannot let the rest of the fleet grow memory without limit.
pub(super) struct WitnessSink {
    state: Mutex<SinkState>,
    drained: Condvar,
    should_stop: Arc<AtomicBool>,
}

impl std::fmt::Debug for WitnessSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WitnessSink")
            .finish_non_exhaustive()
    }
}

struct SinkState {
    writer: Option<WitnessFreezerWriter>,
    pending: BTreeMap<u64, Vec<u8>>,
    pending_bytes: usize,
    next_block: u64,
    /// First write error, latched so every later submission fails the same way.
    failure: Option<String>,
}

impl WitnessSink {
    pub(super) fn new(
        writer: WitnessFreezerWriter,
        first_block: u64,
        should_stop: Arc<AtomicBool>,
    ) -> Self {
        Self {
            state: Mutex::new(SinkState {
                writer: Some(writer),
                pending: BTreeMap::new(),
                pending_bytes: 0,
                next_block: first_block,
                failure: None,
            }),
            drained: Condvar::new(),
            should_stop,
        }
    }

    /// Hands one finished block witness to the sink.
    ///
    /// Blocks while the reorder buffer is full, except for the block the writer
    /// is actually waiting on - accepting that one is what lets a full buffer
    /// drain, so it can never be made to wait.
    pub(super) fn submit(&self, block_number: u64, witness: Vec<u8>) -> Result<()> {
        let mut state = self.state.lock().expect("witness sink mutex poisoned");
        loop {
            if let Some(failure) = state.failure.as_deref() {
                eyre::bail!("witness freezer already failed: {}", failure)
            }
            if block_number <= state.next_block {
                break
            }
            if state.pending.len() < SINK_MAX_PENDING_ENTRIES
                && state.pending_bytes < SINK_MAX_PENDING_BYTES
            {
                break
            }
            if self.should_stop.load(Ordering::Relaxed) {
                return Ok(())
            }
            let (guard, _) = self
                .drained
                .wait_timeout(state, SINK_WAIT_TICK)
                .expect("witness sink mutex poisoned");
            state = guard;
        }

        if block_number < state.next_block {
            eyre::bail!(
                "block {} was already written to the witness freezer",
                block_number
            )
        }
        state.pending_bytes += witness.len();
        if state.pending.insert(block_number, witness).is_some() {
            eyre::bail!(
                "block {} was submitted to the witness sink twice",
                block_number
            )
        }

        let result = state.drain_contiguous();
        self.drained.notify_all();
        result
    }

    /// Marks the run as failed and releases everyone blocked on the buffer.
    ///
    /// A block that never arrives would stall every worker waiting behind it,
    /// so an execution error has to poison the sink rather than leave a gap.
    pub(super) fn abort(&self, reason: String) {
        let mut state = self.state.lock().expect("witness sink mutex poisoned");
        if state.failure.is_none() {
            state.failure = Some(reason);
        }
        drop(state);
        self.drained.notify_all();
    }

    /// The block the freezer is waiting for; everything below it is on disk.
    pub(super) fn next_block(&self) -> u64 {
        self.state
            .lock()
            .expect("witness sink mutex poisoned")
            .next_block
    }

    /// Flushes the freezer and reports how many items it holds.
    ///
    /// Witnesses still buffered above a gap are dropped: they were never
    /// indexed, so a later run simply re-executes those blocks.
    pub(super) fn finish(&self) -> Result<u64> {
        let mut state = self.state.lock().expect("witness sink mutex poisoned");
        state.drain_contiguous()?;

        if !state.pending.is_empty() {
            warn!(
                dropped = state.pending.len(),
                waiting_for = state.next_block,
                "Discarding witnesses buffered above an unfinished block"
            );
            state.pending.clear();
            state.pending_bytes = 0;
        }

        let writer = state
            .writer
            .take()
            .ok_or_else(|| eyre::eyre!("witness freezer was already closed"))?;
        writer.finish()
    }
}

impl SinkState {
    /// Appends the longest run of blocks that starts at `next_block`.
    fn drain_contiguous(&mut self) -> Result<()> {
        while let Some(witness) = self.pending.remove(&self.next_block) {
            self.pending_bytes -= witness.len();
            let block_number = self.next_block;

            let writer = match self.writer.as_mut() {
                Some(writer) => writer,
                None => {
                    let message = "witness freezer was already closed".to_string();
                    self.failure = Some(message.clone());
                    eyre::bail!(message)
                }
            };

            if let Err(error) = writer.push(block_number, witness) {
                self.failure = Some(error.to_string());
                return Err(error)
            }
            self.next_block += 1;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Reader: witness freezer -> parallel replay
// ---------------------------------------------------------------------------

/// Read-only view over a freezer written by [`WitnessFreezerWriter`].
///
/// The `.cidx` repeats one entry per item but its value only changes once per
/// batch, so only the per-batch entry is kept. Data files are mapped read-only,
/// which makes the reader `Sync` and lets every worker slice batches without
/// copying.
pub(super) struct WitnessFreezerReader {
    /// One `(file_number, offset)` per batch, in batch order.
    batches: Vec<(u16, u32)>,
    /// Mapped `.cdat` files, indexed by file number.
    data: Vec<memmap2::Mmap>,
    items: u64,
}

impl std::fmt::Debug for WitnessFreezerReader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WitnessFreezerReader")
            .field("items", &self.items)
            .field("batches", &self.batches.len())
            .field("data_files", &self.data.len())
            .finish()
    }
}

impl WitnessFreezerReader {
    pub(super) fn open(directory: &Path) -> Result<Self> {
        Self::open_table(directory, "witness", false)
    }

    /// Opens any of gov5's 64-item batched zstd tables by name. `headerless`
    /// is the legacy layout gov5 still writes for `senders`: no `NCIX`
    /// header, batch mode implied, and a trailing "next write" sentinel entry
    /// after the last item, as in geth's own index.
    pub(super) fn open_table(directory: &Path, name: &str, headerless: bool) -> Result<Self> {
        let index_path = directory.join(format!("{name}.cidx"));
        let mut index_bytes = Vec::new();
        fs::File::open(&index_path)
            .wrap_err_with(|| format!("failed to open {}", index_path.display()))?
            .read_to_end(&mut index_bytes)
            .wrap_err_with(|| format!("failed to read {}", index_path.display()))?;
        let header = if headerless {
            if index_bytes.len() as u64 % CIDX_ENTRY_SIZE != 0 {
                eyre::bail!("{} ends with a partial cidx entry", index_path.display())
            }
            0
        } else {
            validate_cidx(&index_bytes, &index_path)?;
            CIDX_HEADER_SIZE
        };

        let entries = (index_bytes.len() as u64 - header) / CIDX_ENTRY_SIZE;
        let items = if headerless { entries.saturating_sub(1) } else { entries };
        let batch_count = items.div_ceil(FREEZER_BATCH_SIZE as u64);
        let batches = (0..batch_count)
            .map(|batch| {
                read_index_entry_at(&index_bytes, header, batch * FREEZER_BATCH_SIZE as u64)
            })
            .collect::<Vec<_>>();

        let file_count = batches
            .last()
            .map(|(file_number, _)| *file_number as usize + 1)
            .unwrap_or(1);
        let mut data = Vec::with_capacity(file_count);
        for file_number in 0..file_count {
            let path = table_data_path(directory, name, file_number as u16);
            let file = fs::File::open(&path)
                .wrap_err_with(|| format!("failed to open {}", path.display()))?;
            // SAFETY: replay opens the freezer read-only and nothing writes to
            // these files while a replay run holds them mapped.
            let mapped = unsafe { memmap2::Mmap::map(&file) }
                .wrap_err_with(|| format!("failed to map {}", path.display()))?;
            data.push(mapped);
        }

        Ok(Self {
            batches,
            data,
            items,
        })
    }

    /// Number of blocks in the freezer; item N is block N.
    pub(super) const fn items(&self) -> u64 {
        self.items
    }

    /// Batch that holds `block_number`.
    pub(super) const fn batch_of(block_number: u64) -> u64 {
        block_number / FREEZER_BATCH_SIZE as u64
    }

    /// First block stored in `batch`.
    pub(super) const fn batch_start(batch: u64) -> u64 {
        batch * FREEZER_BATCH_SIZE as u64
    }

    /// Decodes one batch into its individual block witnesses.
    pub(super) fn read_batch(&self, batch: u64) -> Result<DecodedBatch> {
        let (file_number, offset) = *self
            .batches
            .get(batch as usize)
            .ok_or_else(|| eyre::eyre!("witness batch {} is past the end of the freezer", batch))?;
        let mapped = self
            .data
            .get(file_number as usize)
            .ok_or_else(|| eyre::eyre!("witness data file {} is missing", file_number))?;

        // A batch runs to the next batch in the same file, or to end of file
        // when the next batch started a new one.
        let end = match self.batches.get(batch as usize + 1) {
            Some((next_file, next_offset)) if *next_file == file_number => *next_offset as usize,
            _ => mapped.len(),
        };
        let start = offset as usize;
        if start > end || end > mapped.len() {
            eyre::bail!("witness batch {} has an out-of-range offset", batch)
        }

        let expected = std::cmp::min(
            FREEZER_BATCH_SIZE as u64,
            self.items - Self::batch_start(batch),
        ) as usize;
        decode_batch(
            &mapped[start..end],
            expected,
            &format!("witness batch {}", batch),
        )
    }
}

/// Caches the batch a worker is currently replaying from.
///
/// Blocks are handed out in small consecutive tasks, so a single slot absorbs
/// almost every repeat lookup without any cross-thread coordination.
#[derive(Debug, Default)]
pub(super) struct WitnessBatchCache {
    loaded: Option<(u64, DecodedBatch)>,
}

impl WitnessBatchCache {
    /// Returns the recorded witness for `block_number`.
    pub(super) fn witness_for(
        &mut self,
        reader: &WitnessFreezerReader,
        block_number: u64,
    ) -> Result<&[u8]> {
        if block_number >= reader.items() {
            eyre::bail!(
                "block {} is not in the witness freezer, which holds blocks 0..{}",
                block_number,
                reader.items().saturating_sub(1)
            )
        }

        let batch = WitnessFreezerReader::batch_of(block_number);
        let reload = !matches!(&self.loaded, Some((cached, _)) if *cached == batch);
        if reload {
            self.loaded = Some((batch, reader.read_batch(batch)?));
        }

        let (_, entries) = self.loaded.as_ref().expect("batch was just loaded");
        let position = (block_number - WitnessFreezerReader::batch_start(batch)) as usize;
        entries
            .get(position)
            .ok_or_else(|| eyre::eyre!("witness batch {} is missing block {}", batch, block_number))
    }
}

/// A read the witness could not answer.
///
/// Replay is witness-only on purpose. Silently reaching for the database would
/// paper over a witness that no longer matches the execution it was recorded
/// from and report a clean run, so the read fails instead and the caller
/// decides what to do with the block.
#[derive(Debug)]
struct CodeResolveFailed(String);

impl fmt::Display for CodeResolveFailed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for CodeResolveFailed {}

#[derive(Debug)]
struct WitnessUnavailable {
    reason: &'static str,
    position: usize,
    length: usize,
}

impl fmt::Display for WitnessUnavailable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} at byte {} of a {}-byte witness",
            self.reason, self.position, self.length
        )
    }
}

impl std::error::Error for WitnessUnavailable {}

/// Serves one block's state reads from its recorded witness.
///
/// The witness is keyless and ordered, so replay is only correct while the
/// re-execution issues the same sequence of `basic`/`storage` calls that
/// produced it. Code and block hashes were never recorded and are the only
/// reads served by `inner`; account and storage reads must come from the
/// witness or fail.
pub(super) struct WitnessReplayDatabase<'a, DB> {
    inner: DB,
    stream: &'a [u8],
    /// Attaches an account's code as the account is read, by address, so the
    /// executor never has to look code up by hash alone.
    codes: Option<Arc<super::codes_freezer::CodeResolver>>,
    /// Shared with the caller: the executor consumes the database, so how far
    /// the witness was read has to outlive it.
    position: WitnessCursor,
}

/// How many witness bytes the replay has consumed so far.
///
/// A block that leaves bytes behind read fewer values than were recorded, which
/// means the witness does not describe this execution either - the same problem
/// as running out early, just from the other side.
#[derive(Clone, Debug, Default)]
pub(super) struct WitnessCursor(Rc<Cell<usize>>);

impl WitnessCursor {
    pub(super) fn consumed(&self) -> usize {
        self.0.get()
    }
}

impl<DB: fmt::Debug> fmt::Debug for WitnessReplayDatabase<'_, DB> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WitnessReplayDatabase")
            .field("inner", &self.inner)
            .field("consumed", &self.position.consumed())
            .field("stream_len", &self.stream.len())
            .finish()
    }
}

impl<'a, DB> WitnessReplayDatabase<'a, DB> {
    /// Returns the database and a cursor the caller keeps, so it can check
    /// afterwards that the whole witness was consumed.
    pub(super) fn new(inner: DB, stream: &'a [u8]) -> (Self, WitnessCursor) {
        let position = WitnessCursor::default();
        (
            Self {
                inner,
                stream,
                codes: None,
                position: position.clone(),
            },
            position,
        )
    }

    pub(super) fn with_codes(
        mut self,
        codes: Option<Arc<super::codes_freezer::CodeResolver>>,
    ) -> Self {
        self.codes = codes;
        self
    }

    /// Reads the next `[len][value]` record, or `None` once the witness ends.
    fn next_value(&mut self) -> Option<&'a [u8]> {
        let position = self.position.consumed();
        let length = *self.stream.get(position)? as usize;
        let start = position + 1;
        let end = start + length;
        let value = self.stream.get(start..end)?;
        self.position.0.set(end);
        Some(value)
    }

    fn unavailable(&self, reason: &'static str) -> ProviderError {
        ProviderError::other(WitnessUnavailable {
            reason,
            position: self.position.consumed(),
            length: self.stream.len(),
        })
    }
}

impl<DB: RevmDatabase<Error = ProviderError>> RevmDatabase for WitnessReplayDatabase<'_, DB> {
    type Error = ProviderError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        match self.next_value() {
            // An absent account was recorded as a zero-length value; an account
            // that exists always carries at least its field-bits byte.
            Some([]) => Ok(None),
            Some(value) => match decode_account_v2(value) {
                Some(mut account) => {
                    // The record names the code hash; the code itself comes
                    // by address, which only this read knows.
                    if account.code_hash != alloy_primitives::KECCAK256_EMPTY {
                        if let Some(codes) = self.codes.as_ref() {
                            account.code =
                                codes.code_for(address, account.code_hash).map_err(|error| {
                                    ProviderError::other(CodeResolveFailed(error.to_string()))
                                })?;
                        }
                    }
                    Ok(Some(account))
                }
                None => Err(self.unavailable("malformed account record")),
            },
            None => Err(self.unavailable("witness ran out on an account read")),
        }
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        if let Some(codes) = self.codes.as_ref() {
            // With code sources configured, a code none of them has is an
            // error: the database behind `inner` would answer with empty
            // bytecode and the block would run wrong rather than fail.
            return match codes.by_hash(code_hash) {
                Ok(Some(code)) => Ok(code),
                Ok(None) => Err(ProviderError::other(CodeResolveFailed(format!(
                    "code {code_hash:?} is in none of the code sources"
                )))),
                Err(error) => Err(ProviderError::other(CodeResolveFailed(error.to_string()))),
            };
        }
        self.inner.code_by_hash(code_hash)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let _ = (address, index);
        match self.next_value() {
            Some(value) => Ok(U256::from_be_slice(value)),
            None => Err(self.unavailable("witness ran out on a storage read")),
        }
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.inner.block_hash(number)
    }
}

/// Inverse of [`encode_account_v2`]; `None` means the record was malformed.
///
/// Omitted fields carry their zero value and an omitted code hash means the
/// empty-code hash. The encoding also maps a zero code hash onto the empty-code
/// hash, which revm never produces for a loaded account.
fn decode_account_v2(encoded: &[u8]) -> Option<AccountInfo> {
    const NONCE_BIT: u8 = 1;
    const BALANCE_BIT: u8 = 2;
    const CODE_HASH_BIT: u8 = 8;

    let field_bits = *encoded.first()?;
    let mut cursor = 1;

    let nonce = if field_bits & NONCE_BIT != 0 {
        read_uvarint(encoded, &mut cursor)?
    } else {
        0
    };

    let balance = if field_bits & BALANCE_BIT != 0 {
        let length = *encoded.get(cursor)? as usize;
        cursor += 1;
        let bytes = encoded.get(cursor..cursor + length)?;
        cursor += length;
        U256::from_be_slice(bytes)
    } else {
        U256::ZERO
    };

    let code_hash = if field_bits & CODE_HASH_BIT != 0 {
        let bytes = encoded.get(cursor..cursor + 32)?;
        cursor += 32;
        B256::from_slice(bytes)
    } else {
        B256::from_slice(&EMPTY_CODE_HASH)
    };

    // Trailing bytes mean the record is not the V2 encoding it claims to be.
    if cursor != encoded.len() {
        return None
    }

    // `code` must stay `None`, the way reth's own `basic_ref` returns it. A
    // `Some(empty)` here would tell revm the account has no code, so every
    // contract would execute as an EOA and diverge from what was recorded.
    let mut account = AccountInfo::new(balance, nonce, code_hash, Bytecode::default());
    account.code = None;
    Some(account)
}

/// Go `encoding/binary.Uvarint` compatible decoder.
fn read_uvarint(input: &[u8], cursor: &mut usize) -> Option<u64> {
    let mut value: u64 = 0;
    let mut shift = 0;
    loop {
        let byte = *input.get(*cursor)?;
        *cursor += 1;
        if byte < 0x80 {
            if shift > 63 || (shift == 63 && byte > 1) {
                return None
            }
            return Some(value | (u64::from(byte) << shift))
        }
        value |= u64::from(byte & 0x7f) << shift;
        shift += 7;
        if shift > 63 {
            return None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct MockDatabase {
        account: Option<AccountInfo>,
        storage: U256,
    }

    impl RevmDatabase for MockDatabase {
        type Error = ProviderError;

        fn basic(&mut self, _address: Address) -> Result<Option<AccountInfo>, Self::Error> {
            Ok(self.account.clone())
        }

        fn code_by_hash(&mut self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
            Ok(Bytecode::default())
        }

        fn storage(&mut self, _address: Address, _index: U256) -> Result<U256, Self::Error> {
            Ok(self.storage)
        }

        fn block_hash(&mut self, _number: u64) -> Result<B256, Self::Error> {
            Ok(B256::ZERO)
        }
    }

    #[test]
    fn matches_gov5_account_v2_and_storage_stream() {
        let account = AccountInfo {
            nonce: 300,
            balance: U256::from(1000),
            ..AccountInfo::default()
        };
        let (mut database, stream) = WitnessDatabase::new(MockDatabase {
            account: Some(account),
            storage: U256::from(0x42abu64),
        });

        database.basic(Address::repeat_byte(0x11)).unwrap();
        database
            .storage(Address::repeat_byte(0x22), U256::from(7))
            .unwrap();

        // account: [bits=3][nonce=300 uvarint][balance length=2][0x03e8]
        // storage: minimal big-endian bytes. No address, key, type, or count.
        assert_eq!(
            stream.take(),
            vec![6, 3, 0xac, 0x02, 2, 0x03, 0xe8, 2, 0x42, 0xab]
        );
    }

    #[test]
    fn distinguishes_absent_from_present_empty_account() {
        let (mut absent, absent_stream) = WitnessDatabase::new(MockDatabase::default());
        absent.basic(Address::ZERO).unwrap();
        assert_eq!(absent_stream.take(), vec![0]);

        let (mut present, present_stream) = WitnessDatabase::new(MockDatabase {
            account: Some(AccountInfo::default()),
            ..MockDatabase::default()
        });
        present.basic(Address::ZERO).unwrap();
        assert_eq!(present_stream.take(), vec![1, 0]);
    }

    #[test]
    fn records_code_and_block_hash_as_no_entries() {
        let (mut database, stream) = WitnessDatabase::new(MockDatabase::default());
        database.code_by_hash(B256::repeat_byte(1)).unwrap();
        database.block_hash(123).unwrap();
        assert!(stream.take().is_empty());
    }

    #[test]
    fn includes_non_empty_code_hash() {
        let code_hash = B256::repeat_byte(0x77);
        let account = AccountInfo {
            code_hash,
            ..AccountInfo::default()
        };
        let encoded = encode_account_v2(&account);
        assert_eq!(encoded[0], 8);
        assert_eq!(&encoded[1..], code_hash.as_slice());
    }

    #[test]
    fn writes_gov5_batch_freezer_layout() {
        let directory = tempfile::tempdir().unwrap();
        let mut writer = WitnessFreezerWriter::open(directory.path(), 0).unwrap();
        for block in 0..65 {
            writer
                .push(block, vec![block as u8; block as usize % 9])
                .unwrap();
        }
        assert_eq!(writer.finish().unwrap(), 65);

        let index = fs::read(directory.path().join("witness.cidx")).unwrap();
        assert_eq!(&index[..16], &cidx_header());
        assert_eq!(index.len(), 16 + 65 * 6);

        let first = read_index_entry(&index, 0);
        for item in 1..64 {
            assert_eq!(read_index_entry(&index, item), first);
        }
        let second = read_index_entry(&index, 64);
        assert_eq!(first, (0, 0));
        assert_eq!(second.0, 0);
        assert!(second.1 > first.1);

        let path = directory.path().join("witness.0000.cdat");
        let data_len = fs::metadata(&path).unwrap().len();
        let first_batch = read_batch(&path, 0, u64::from(second.1), 64).unwrap();
        let second_batch = read_batch(&path, u64::from(second.1), data_len, 1).unwrap();
        for (block, witness) in first_batch.iter().chain(&second_batch).enumerate() {
            assert_eq!(witness, &vec![block as u8; block % 9]);
        }
    }

    #[test]
    fn resumes_by_rewriting_partial_batch_like_gov5() {
        let directory = tempfile::tempdir().unwrap();
        let mut writer = WitnessFreezerWriter::open(directory.path(), 0).unwrap();
        for block in 0..65 {
            writer.push(block, block.to_le_bytes().to_vec()).unwrap();
        }
        writer.finish().unwrap();

        let mut resumed = WitnessFreezerWriter::open(directory.path(), 65).unwrap();
        for block in 65..128 {
            resumed.push(block, block.to_le_bytes().to_vec()).unwrap();
        }
        assert_eq!(resumed.finish().unwrap(), 128);

        let index = fs::read(directory.path().join("witness.cidx")).unwrap();
        assert_eq!(index.len(), 16 + 128 * 6);
        let second = read_index_entry(&index, 64);
        for item in 65..128 {
            assert_eq!(read_index_entry(&index, item), second);
        }

        let path = directory.path().join("witness.0000.cdat");
        let data_len = fs::metadata(&path).unwrap().len();
        let batch = read_batch(&path, u64::from(second.1), data_len, 64).unwrap();
        for (offset, witness) in batch.iter().enumerate() {
            assert_eq!(witness, &(64 + offset as u64).to_le_bytes());
        }
    }

    #[test]
    fn sink_writes_blocks_in_order_regardless_of_submission_order() {
        let directory = tempfile::tempdir().unwrap();
        let writer = WitnessFreezerWriter::open(directory.path(), 0).unwrap();
        let sink = Arc::new(WitnessSink::new(
            writer,
            0,
            Arc::new(AtomicBool::new(false)),
        ));

        // The low blocks arrive shuffled and the last three arrive before the
        // long run that has to precede them.
        for block in [5u64, 3, 0, 6, 1, 2, 4, 130, 129, 128] {
            sink.submit(block, vec![block as u8; 4]).unwrap();
        }
        // Only 0..=6 could be written so far; 128..=130 are still buffered.
        assert_eq!(sink.next_block(), 7);

        for block in 7..128u64 {
            sink.submit(block, vec![block as u8; 4]).unwrap();
        }
        // Closing the gap releases the buffered tail in one drain.
        assert_eq!(sink.next_block(), 131);
        assert_eq!(sink.finish().unwrap(), 131);

        // Two full batches plus a partial one, all in block order.
        let reader = WitnessFreezerReader::open(directory.path()).unwrap();
        assert_eq!(reader.items(), 131);
        let mut cache = WitnessBatchCache::default();
        for block in 0..131u64 {
            let witness = cache.witness_for(&reader, block).unwrap();
            assert_eq!(witness, vec![block as u8; 4]);
        }
        let error = cache.witness_for(&reader, 131).unwrap_err().to_string();
        assert!(error.contains("not in the witness freezer"), "{error}");
    }

    #[test]
    fn sink_rejects_a_block_that_was_already_written() {
        let directory = tempfile::tempdir().unwrap();
        let writer = WitnessFreezerWriter::open(directory.path(), 0).unwrap();
        let sink = WitnessSink::new(writer, 0, Arc::new(AtomicBool::new(false)));

        sink.submit(0, vec![1]).unwrap();
        let error = sink.submit(0, vec![1]).unwrap_err().to_string();
        assert!(error.contains("already written"), "{error}");
    }

    #[test]
    fn sink_stops_accepting_after_an_abort() {
        let directory = tempfile::tempdir().unwrap();
        let writer = WitnessFreezerWriter::open(directory.path(), 0).unwrap();
        let sink = WitnessSink::new(writer, 0, Arc::new(AtomicBool::new(false)));

        sink.abort("block 7 failed to execute".to_string());
        let error = sink.submit(0, vec![1]).unwrap_err().to_string();
        assert!(error.contains("block 7 failed to execute"), "{error}");
    }

    #[test]
    fn account_v2_survives_an_encode_decode_round_trip() {
        let cases = [
            AccountInfo::new(U256::ZERO, 0, B256::from_slice(&EMPTY_CODE_HASH), Bytecode::default()),
            AccountInfo::new(U256::from(1u64), 1, B256::from_slice(&EMPTY_CODE_HASH), Bytecode::default()),
            AccountInfo::new(
                U256::from(12_345_678_901_234_567_890u128),
                u64::MAX,
                B256::repeat_byte(0xab),
                Bytecode::default(),
            ),
            AccountInfo::new(U256::MAX, 300, B256::repeat_byte(0x11), Bytecode::default()),
        ];

        for account in cases {
            let encoded = encode_account_v2(&account);
            let decoded = decode_account_v2(&encoded).expect("round trip decodes");
            assert_eq!(decoded.balance, account.balance);
            assert_eq!(decoded.nonce, account.nonce);
            assert_eq!(decoded.code_hash, account.code_hash());
        }
    }

    #[test]
    fn replay_serves_the_recorded_values_in_order() {
        let account = AccountInfo::new(
            U256::from(7u64),
            3,
            B256::from_slice(&EMPTY_CODE_HASH),
            Bytecode::default(),
        );
        let mock = MockDatabase {
            account: Some(account.clone()),
            storage: U256::from(0x1234u64),
        };

        // Record one account read followed by one storage read.
        let (mut recorder, stream) = WitnessDatabase::new(mock);
        recorder.basic(Address::ZERO).unwrap();
        recorder.storage(Address::ZERO, U256::ZERO).unwrap();
        let recorded = stream.take();

        // Replay against a database that would answer differently, so any
        // value that matches must have come from the witness.
        let divergent = MockDatabase {
            account: None,
            storage: U256::from(0xffffu64),
        };
        let (mut replay, _cursor) = WitnessReplayDatabase::new(divergent, &recorded);

        let replayed = replay.basic(Address::ZERO).unwrap().expect("account present");
        assert_eq!(replayed.balance, account.balance);
        assert_eq!(replayed.nonce, account.nonce);
        assert_eq!(
            replay.storage(Address::ZERO, U256::ZERO).unwrap(),
            U256::from(0x1234u64)
        );

        // Reading past the end fails rather than quietly using the database,
        // which would have answered 0xffff here.
        let error = replay
            .storage(Address::ZERO, U256::ZERO)
            .unwrap_err()
            .to_string();
        assert!(error.contains("witness ran out on a storage read"), "{error}");
    }

    #[test]
    fn replay_refuses_to_read_state_from_the_database() {
        // An empty witness cannot answer anything.
        let (mut replay, _cursor) = WitnessReplayDatabase::new(            MockDatabase {
                account: Some(AccountInfo::default()),
                storage: U256::from(9u64),
            },
            &[],
        );

        let error = replay.basic(Address::ZERO).unwrap_err().to_string();
        assert!(error.contains("witness ran out on an account read"), "{error}");

        // Code and block hashes were never recorded, so those still resolve.
        assert!(replay.code_by_hash(B256::ZERO).is_ok());
        assert!(replay.block_hash(0).is_ok());
    }

    #[test]
    fn replay_rejects_a_malformed_account_record() {
        // A complete 2-byte record whose field bits claim a 32-byte code hash
        // that is not there.
        let (mut replay, _cursor) = WitnessReplayDatabase::new(MockDatabase::default(), &[2, 8, 1]);
        let error = replay.basic(Address::ZERO).unwrap_err().to_string();
        assert!(error.contains("malformed account record"), "{error}");
    }

    #[test]
    fn replay_distinguishes_an_absent_account_from_an_empty_one() {
        let absent = MockDatabase {
            account: None,
            storage: U256::ZERO,
        };
        let (mut recorder, stream) = WitnessDatabase::new(absent);
        recorder.basic(Address::ZERO).unwrap();
        let recorded_absent = stream.take();

        let empty = MockDatabase {
            account: Some(AccountInfo::new(
                U256::ZERO,
                0,
                B256::from_slice(&EMPTY_CODE_HASH),
                Bytecode::default(),
            )),
            storage: U256::ZERO,
        };
        let (mut recorder, stream) = WitnessDatabase::new(empty);
        recorder.basic(Address::ZERO).unwrap();
        let recorded_empty = stream.take();

        // Absent is a zero-length value; empty still carries its field bits.
        assert_eq!(recorded_absent, vec![0]);
        assert_eq!(recorded_empty, vec![1, 0]);

        let (mut replay, _cursor) =
            WitnessReplayDatabase::new(MockDatabase::default(), &recorded_absent);
        assert!(replay.basic(Address::ZERO).unwrap().is_none());

        let (mut replay, _cursor) =
            WitnessReplayDatabase::new(MockDatabase::default(), &recorded_empty);
        assert!(replay.basic(Address::ZERO).unwrap().is_some());
    }
}
