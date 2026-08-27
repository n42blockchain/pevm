// SPDX-License-Identifier: MIT OR Apache-2.0

//! Keyless, ordered block-witness recording compatible with N42-gov5.

use crate::revm::{
    state::{AccountInfo, Bytecode},
    Database as RevmDatabase,
};
use alloy_primitives::{Address, B256, U256};
use eyre::{Context, Result};
use std::{
    fs::{self, OpenOptions},
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

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

/// Shared handle to the raw witness bytes produced by [`WitnessDatabase`].
#[derive(Clone, Debug)]
pub(super) struct WitnessStream(Arc<Mutex<Vec<u8>>>);

impl WitnessStream {
    /// Removes and returns all bytes currently in the stream.
    pub(super) fn take(&self) -> Vec<u8> {
        std::mem::take(&mut *self.0.lock().expect("witness stream mutex poisoned"))
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
        let stream = WitnessStream(Arc::new(Mutex::new(Vec::with_capacity(4096))));
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
        let mut stream = self.stream.0.lock().expect("witness stream mutex poisoned");
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
    let position = (CIDX_HEADER_SIZE + item * CIDX_ENTRY_SIZE) as usize;
    (
        u16::from_be_bytes(bytes[position..position + 2].try_into().unwrap()),
        u32::from_be_bytes(bytes[position + 2..position + 6].try_into().unwrap()),
    )
}

fn data_path(output_dir: &Path, file_number: u16) -> PathBuf {
    output_dir.join(format!("witness.{file_number:04}.cdat"))
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
    let raw = if blob.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        zstd::stream::decode_all(blob.as_slice()).wrap_err("failed to decode witness batch")?
    } else {
        blob
    };

    let mut entries = Vec::with_capacity(expected);
    let mut position = 0;
    for _ in 0..expected {
        if position + 4 > raw.len() {
            eyre::bail!("truncated witness batch length in {}", path.display())
        }
        let length = u32::from_le_bytes(raw[position..position + 4].try_into().unwrap()) as usize;
        position += 4;
        if position + length > raw.len() {
            eyre::bail!("truncated witness batch payload in {}", path.display())
        }
        entries.push(raw[position..position + length].to_vec());
        position += length;
    }
    if position != raw.len() {
        eyre::bail!("witness batch in {} has trailing bytes", path.display())
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;

    #[derive(Default)]
    struct MockDatabase {
        account: Option<AccountInfo>,
        storage: U256,
    }

    impl RevmDatabase for MockDatabase {
        type Error = Infallible;

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
}
