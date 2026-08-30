//! A per-worker account cache for witness replay: revm's `State` without the
//! transition bookkeeping.
//!
//! Replay wants the receipts and nothing after the block, but `State::commit`
//! still builds a `TransitionAccount` — a clone of the previous info and a
//! fresh storage map — for every touched account of every transaction and
//! drops it at once. With a hundred-odd workers that churn is the largest
//! source of memory stalls: the freed lines come back through the allocator
//! still owned by another core. This cache keeps exactly what `State` shows
//! the outside — which reads reach the database and what they return — by
//! running revm's own `AccountStatus` machine, so a positional witness
//! recorded through `State` replays through it unchanged.

use crate::revm::{
    db::states::AccountStatus,
    state::{Account, AccountInfo, Bytecode},
    Database, DatabaseCommit,
};
use alloy_primitives::{
    map::{AddressMap, HashMap},
    Address, B256, U256,
};

/// One account as the block has seen it so far.
#[derive(Debug, Default)]
struct CachedAccount {
    /// `None` once the account is known not to exist: never loaded, destroyed
    /// or cleared as empty. Reads then answer without the database.
    info: Option<AccountInfo>,
    /// Slots read or written so far.
    storage: HashMap<U256, U256>,
    status: AccountStatus,
}

/// The per-worker tables, emptied and handed back after every block so they
/// keep their capacity.
#[derive(Debug, Default)]
pub(super) struct ReplayCacheStore {
    accounts: AddressMap<CachedAccount>,
}

impl ReplayCacheStore {
    pub(super) fn with_capacity(accounts: usize) -> Self {
        Self { accounts: AddressMap::with_capacity_and_hasher(accounts, Default::default()) }
    }

    pub(super) fn clear(&mut self) {
        self.accounts.clear();
    }
}

/// The cache in front of a witness database for one block.
pub(super) struct ReplayCache<DB> {
    database: DB,
    cache: ReplayCacheStore,
}

impl<DB> std::fmt::Debug for ReplayCache<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayCache").field("cache", &self.cache).finish_non_exhaustive()
    }
}

impl<DB: Database> ReplayCache<DB> {
    pub(super) fn new(database: DB, cache: ReplayCacheStore) -> Self {
        Self { database, cache }
    }

    pub(super) fn into_parts(self) -> (DB, ReplayCacheStore) {
        (self.database, self.cache)
    }
}

/// The account's entry, loaded from the database on first sight the way
/// `State::load_cache_account` loads it.
fn load<'a, DB: Database>(
    cache: &'a mut ReplayCacheStore,
    database: &mut DB,
    address: Address,
) -> Result<&'a mut CachedAccount, DB::Error> {
    use alloy_primitives::map::Entry;
    match cache.accounts.entry(address) {
        Entry::Occupied(entry) => Ok(entry.into_mut()),
        Entry::Vacant(entry) => {
            let account = match database.basic(address)? {
                None => CachedAccount { status: AccountStatus::LoadedNotExisting, ..Default::default() },
                Some(info) if info.is_empty() => CachedAccount {
                    info: Some(info),
                    status: AccountStatus::LoadedEmptyEIP161,
                    ..Default::default()
                },
                Some(info) => {
                    CachedAccount { info: Some(info), status: AccountStatus::Loaded, ..Default::default() }
                }
            };
            Ok(entry.insert(account))
        }
    }
}

impl<DB: Database> Database for ReplayCache<DB> {
    type Error = DB::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        Ok(load(&mut self.cache, &mut self.database, address)?.info.clone())
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        // Code never comes from the witness stream, so caching it here would
        // change nothing the witness can see; the resolver behind the
        // database keeps its own per-thread table.
        self.database.code_by_hash(code_hash)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let account = load(&mut self.cache, &mut self.database, address)?;
        if account.info.is_none() {
            // `State` answers zero for an account it holds as absent without
            // asking the database.
            return Ok(U256::ZERO);
        }
        let storage_known = account.status.is_storage_known();
        use alloy_primitives::map::Entry;
        match account.storage.entry(index) {
            Entry::Occupied(entry) => Ok(*entry.get()),
            Entry::Vacant(entry) => {
                let value =
                    if storage_known { U256::ZERO } else { self.database.storage(address, index)? };
                entry.insert(value);
                Ok(value)
            }
        }
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.database.block_hash(number)
    }
}

impl<DB: Database> DatabaseCommit for ReplayCache<DB> {
    /// `CacheState::apply_evm_state`, minus the transitions it returns.
    fn commit(&mut self, changes: AddressMap<Account>) {
        use alloy_primitives::map::Entry;
        for (address, account) in changes {
            if !account.is_touched() {
                continue;
            }
            let entry = match self.cache.accounts.entry(address) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => entry.insert(if account.is_loaded_as_not_existing() {
                    CachedAccount { status: AccountStatus::LoadedNotExisting, ..Default::default() }
                } else {
                    let original = account.original_info();
                    let status = if original.is_empty() {
                        AccountStatus::LoadedEmptyEIP161
                    } else {
                        AccountStatus::Loaded
                    };
                    CachedAccount { info: Some(original), status, ..Default::default() }
                }),
            };

            if account.is_selfdestructed() {
                entry.info = None;
                entry.storage.clear();
                entry.status = entry.status.on_selfdestructed();
                continue;
            }
            if account.is_created() {
                entry.status = entry.status.on_created();
                entry.storage.clear();
                entry.storage.extend(changed_slots(&account));
                entry.info = Some(account.info);
                continue;
            }
            if account.is_empty() {
                entry.info = None;
                entry.storage.clear();
                entry.status = entry.status.on_touched_empty_post_eip161();
                continue;
            }
            let had_no_nonce_and_code =
                entry.info.as_ref().map(AccountInfo::has_no_code_and_nonce).unwrap_or_default();
            entry.storage.extend(changed_slots(&account));
            entry.status = entry.status.on_changed(had_no_nonce_and_code);
            entry.info = Some(account.info);
        }
    }
}

fn changed_slots(account: &Account) -> impl Iterator<Item = (U256, U256)> + '_ {
    account.storage.iter().filter_map(|(key, slot)| slot.is_changed().then_some((*key, slot.present_value)))
}

/// Block access lists are an artefact of the post-state, which replay throws
/// away; the index is accepted and ignored.
impl<DB: Database> reth_evm::block::BalIndexedDatabase for ReplayCache<DB> {
    fn set_bal_index(&mut self, _index: u64) {}

    fn bump_bal_index(&mut self) {}
}
