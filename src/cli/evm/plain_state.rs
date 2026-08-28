// SPDX-License-Identifier: MIT OR Apache-2.0

//! Plain state built by executing forward from genesis.
//!
//! Recording a witness in block order does not need historical state at all:
//! the parent state of block N is just what block N-1 left behind. Only
//! out-of-order execution has to walk changesets backwards, which is what ties
//! the parallel recorder to a reth archive.
//!
//! So this keeps one current-state table pair - accounts and storage - and
//! advances it a block at a time. Reads during execution are what the witness
//! records; writes are applied afterwards from the executor's bundle.
//!
//! Contract code is not read from anywhere: every contract is deployed by some
//! block, so executing from genesis produces it. Codes seen so far are kept
//! alongside the state.

use crate::revm::{
    state::{AccountInfo, Bytecode},
    Database as RevmDatabase,
};
use alloy_primitives::{keccak256, Address, B256, U256};
use alloy_primitives::map::foldhash as _;
use eyre::{Context, Result};
use reth_libmdbx::{
    DatabaseFlags, Environment, EnvironmentFlags, Geometry, Mode, PageSize, SyncMode, WriteFlags, RW,
};
use reth_storage_errors::provider::ProviderError;
use crate::revm::db::BundleState;
use std::{ops::Range, path::Path, sync::Arc};

const ACCOUNTS_TABLE: &str = "accounts";
const STORAGE_TABLE: &str = "storage";
const CODES_TABLE: &str = "codes";
const METADATA_TABLE: &str = "metadata";

/// Key under which the last fully applied block number is stored.
const LAST_BLOCK_KEY: &[u8] = b"last_block";

/// keccak256 of the empty byte string.
const EMPTY_CODE_HASH: [u8; 32] = [
    0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c, 0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
    0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b, 0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
];

/// Growth headroom for the state database. Mainnet plain state is on the order
/// of 160 GB, so this is deliberately generous - MDBX only commits pages it
/// actually writes.
const MAX_SIZE: usize = 3 * 1024 * 1024 * 1024 * 1024;
const GROWTH_STEP: isize = 4 * 1024 * 1024 * 1024;

/// Current-state store for forward execution.
pub(super) struct PlainStateStore {
    environment: Environment,
}

impl std::fmt::Debug for PlainStateStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("PlainStateStore").finish_non_exhaustive()
    }
}

impl PlainStateStore {
    pub(super) fn open(directory: &Path) -> Result<Self> {
        std::fs::create_dir_all(directory)
            .wrap_err_with(|| format!("failed to create {}", directory.display()))?;

        let environment = Environment::builder()
            .set_max_dbs(8)
            .set_geometry(Geometry::<Range<usize>> {
                size: Some(0..MAX_SIZE),
                growth_step: Some(GROWTH_STEP),
                shrink_threshold: None,
                page_size: Some(PageSize::Set(16 * 1024)),
            })
            // Durability is not worth its cost here: the state is derived, and
            // a torn run is recovered by re-executing from the last committed
            // block rather than by fsync.
            .set_flags(EnvironmentFlags {
                mode: Mode::ReadWrite {
                    sync_mode: SyncMode::UtterlyNoSync,
                },
                no_rdahead: true,
                coalesce: true,
                ..Default::default()
            })
            .open(directory)
            .wrap_err_with(|| format!("failed to open state at {}", directory.display()))?;

        // Create the tables up front so later transactions can just open them.
        let transaction = environment.begin_rw_txn()?;
        for table in [ACCOUNTS_TABLE, STORAGE_TABLE, CODES_TABLE, METADATA_TABLE] {
            transaction.create_db(Some(table), DatabaseFlags::default())?;
        }
        transaction.commit()?;

        Ok(Self { environment })
    }

    /// Block number the store is currently at, or `None` when it is empty.
    pub(super) fn last_block(&self) -> Result<Option<u64>> {
        let transaction = self.environment.begin_ro_txn()?;
        let table = transaction.open_db(Some(METADATA_TABLE))?;
        let raw: Option<Vec<u8>> = transaction.get(table.dbi(), LAST_BLOCK_KEY)?;
        Ok(raw.map(|bytes| u64::from_be_bytes(bytes[..8].try_into().unwrap())))
    }

    /// Writes the genesis allocation.
    ///
    /// Genesis balances, code and storage are declared by the chain spec, not
    /// produced by executing block 0, so forward execution has to seed them
    /// before the first transaction can pay for anything.
    pub(super) fn init_genesis(&self, genesis: &alloy_genesis::Genesis) -> Result<usize> {
        let batch = self.batch()?;
        let mut written = 0usize;
        for (address, account) in &genesis.alloc {
            let code_hash = account
                .code
                .as_ref()
                .filter(|code| !code.is_empty())
                .map_or_else(|| B256::from_slice(&EMPTY_CODE_HASH), |code| keccak256(code));

            let info = AccountInfo::new(
                account.balance,
                account.nonce.unwrap_or_default(),
                code_hash,
                Bytecode::default(),
            );
            batch.transaction.put(
                batch.accounts,
                address.as_slice(),
                encode_account(&info),
                WriteFlags::UPSERT,
            )?;

            if let Some(code) = account.code.as_ref().filter(|code| !code.is_empty()) {
                batch
                    .transaction
                    .put(batch.codes, code_hash.as_slice(), code.to_vec(), WriteFlags::UPSERT)?;
            }

            if let Some(storage) = account.storage.as_ref() {
                for (slot, value) in storage {
                    if value.is_zero() {
                        continue
                    }
                    let key = storage_key(address, &U256::from_be_bytes(slot.0));
                    batch.transaction.put(
                        batch.storage,
                        key.as_slice(),
                        value.0,
                        WriteFlags::UPSERT,
                    )?;
                }
            }
            written += 1;
        }
        batch.commit()?;
        Ok(written)
    }

    /// Starts a batch that spans several blocks.
    ///
    /// One write transaction per block does not survive a long run: MDBX holds
    /// every dirty page until commit, and the per-transaction overhead dominates
    /// besides. A batch also removes the need for a separate write buffer, since
    /// reads inside a write transaction already see that transaction's writes.
    pub(super) fn batch(&self) -> Result<PlainStateBatch<'_>> {
        let transaction = self.environment.begin_rw_txn()?;
        let accounts = transaction.open_db(Some(ACCOUNTS_TABLE))?.dbi();
        let storage = transaction.open_db(Some(STORAGE_TABLE))?.dbi();
        let codes = transaction.open_db(Some(CODES_TABLE))?.dbi();
        let metadata = transaction.open_db(Some(METADATA_TABLE))?.dbi();
        Ok(PlainStateBatch {
            transaction,
            accounts,
            storage,
            codes,
            metadata,
            _marker: std::marker::PhantomData,
        })
    }
}

/// A write transaction covering a run of blocks.
pub(super) struct PlainStateBatch<'env> {
    transaction: reth_libmdbx::Transaction<RW>,
    accounts: reth_libmdbx::ffi::MDBX_dbi,
    storage: reth_libmdbx::ffi::MDBX_dbi,
    codes: reth_libmdbx::ffi::MDBX_dbi,
    metadata: reth_libmdbx::ffi::MDBX_dbi,
    _marker: std::marker::PhantomData<&'env ()>,
}

impl std::fmt::Debug for PlainStateBatch<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("PlainStateBatch").finish_non_exhaustive()
    }
}

impl<'env> PlainStateBatch<'env> {
    /// A database view over this batch, including its uncommitted writes.
    pub(super) fn database(
        &'env self,
        blocks: Option<Arc<super::geth_freezer::GethBlockSource>>,
    ) -> PlainStateDatabase<'env> {
        PlainStateDatabase {
            batch: self,
            blocks,
        }
    }

    /// Records that the state now covers everything up to `block`.
    pub(super) fn mark_block(&self, block: u64) -> Result<()> {
        self.transaction.put(
            self.metadata,
            LAST_BLOCK_KEY,
            block.to_be_bytes(),
            WriteFlags::UPSERT,
        )?;
        Ok(())
    }

    /// Applies one block's changes and marks the state as covering it.
    pub(super) fn apply(&self, block: u64, bundle: &BundleState) -> Result<()> {
        for (address, account) in &bundle.state {
            match account.info.as_ref() {
                Some(info) => {
                    self.transaction.put(
                        self.accounts,
                        address.as_slice(),
                        encode_account(info),
                        WriteFlags::UPSERT,
                    )?;
                }
                None => {
                    // A missing info means the account is gone; its storage went
                    // with it, and the bundle lists those slots as zeroed.
                    let _ = self.transaction.del(self.accounts, address.as_slice(), None);
                }
            }

            for (slot, value) in &account.storage {
                let key = storage_key(address, slot);
                if value.present_value.is_zero() {
                    let _ = self.transaction.del(self.storage, key.as_slice(), None);
                } else {
                    self.transaction.put(
                        self.storage,
                        key.as_slice(),
                        value.present_value.to_be_bytes::<32>(),
                        WriteFlags::UPSERT,
                    )?;
                }
            }
        }

        for (hash, bytecode) in &bundle.contracts {
            self.transaction
                .put(
                    self.codes,
                    hash.as_slice(),
                    bytecode.original_byte_slice().to_vec(),
                    WriteFlags::NO_OVERWRITE,
                )
                .or_else(|error| match error {
                    // The same code redeployed elsewhere is not a problem.
                    reth_libmdbx::Error::KeyExist => Ok(()),
                    other => Err(other),
                })?;
        }

        self.mark_block(block)
    }

    /// Commits everything applied so far.
    pub(super) fn commit(self) -> Result<()> {
        self.transaction.commit()?;
        Ok(())
    }

    fn account(&self, address: Address) -> Result<Option<AccountInfo>> {
        let raw: Option<Vec<u8>> = self.transaction.get(self.accounts, address.as_slice())?;
        Ok(raw.as_deref().and_then(decode_account))
    }

    fn slot(&self, address: Address, index: U256) -> Result<U256> {
        let raw: Option<Vec<u8>> = self
            .transaction
            .get(self.storage, &storage_key(&address, &index))?;
        Ok(raw.map_or(U256::ZERO, |bytes| U256::from_be_slice(&bytes)))
    }

    fn code(&self, code_hash: B256) -> Result<Option<Bytecode>> {
        let raw: Option<Vec<u8>> = self.transaction.get(self.codes, code_hash.as_slice())?;
        Ok(raw.map(|bytes| Bytecode::new_raw(bytes.into())))
    }
}

/// Database view for one block's execution.
///
/// Block hashes come from the block source rather than the state, which keeps
/// no headers.
pub(super) struct PlainStateDatabase<'env> {
    batch: &'env PlainStateBatch<'env>,
    blocks: Option<Arc<super::geth_freezer::GethBlockSource>>,
}

impl std::fmt::Debug for PlainStateDatabase<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PlainStateDatabase")
            .field("has_blocks", &self.blocks.is_some())
            .finish()
    }
}

impl RevmDatabase for PlainStateDatabase<'_> {
    type Error = ProviderError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.batch.account(address).map_err(provider_error)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        match self.batch.code(code_hash).map_err(provider_error)? {
            Some(code) => Ok(code),
            // Executing from genesis means every contract was deployed by some
            // earlier block, so a miss here is a real inconsistency.
            None => Err(provider_error(format!(
                "no code for {code_hash:?} in the state built so far"
            ))),
        }
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        self.batch.slot(address, index).map_err(provider_error)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        match self.blocks.as_ref() {
            Some(blocks) => blocks.block_hash(number).map_err(provider_error),
            None => Err(provider_error(
                "BLOCKHASH needs a block source; pass --geth-ancient-dir",
            )),
        }
    }
}

fn provider_error(error: impl std::fmt::Display) -> ProviderError {
    ProviderError::other(StateReadFailed(error.to_string()))
}

#[derive(Debug)]
struct StateReadFailed(String);

impl std::fmt::Display for StateReadFailed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for StateReadFailed {}

fn storage_key(address: &Address, index: &U256) -> [u8; 52] {
    let mut key = [0u8; 52];
    key[..20].copy_from_slice(address.as_slice());
    key[20..].copy_from_slice(&index.to_be_bytes::<32>());
    key
}

/// `[nonce: u64 BE][balance: 32 BE][code_hash: 32]`
///
/// Fixed width keeps decoding branch-free; the table is dominated by storage
/// anyway, and MDBX pages compress poorly either way.
fn encode_account(info: &AccountInfo) -> [u8; 72] {
    let mut encoded = [0u8; 72];
    encoded[..8].copy_from_slice(&info.nonce.to_be_bytes());
    encoded[8..40].copy_from_slice(&info.balance.to_be_bytes::<32>());
    encoded[40..].copy_from_slice(info.code_hash().as_slice());
    encoded
}

fn decode_account(raw: &[u8]) -> Option<AccountInfo> {
    if raw.len() != 72 {
        return None
    }
    let nonce = u64::from_be_bytes(raw[..8].try_into().unwrap());
    let balance = U256::from_be_slice(&raw[8..40]);
    let code_hash = B256::from_slice(&raw[40..]);
    // `code` stays None so revm resolves it through `code_by_hash`, the way
    // reth's own provider returns accounts.
    let mut account = AccountInfo::new(balance, nonce, code_hash, Bytecode::default());
    account.code = None;
    Some(account)
}
