// SPDX-License-Identifier: MIT OR Apache-2.0

//! Substituting account values that the database reports incorrectly.
//!
//! A history index can be incomplete for a single account while the rest of the
//! database is sound: the lookup then falls through to the current value, and
//! execution silently runs on state from the wrong height. That is not a
//! failure the executor can detect - the value it gets back is well-formed, just
//! wrong - so the only way past it, short of rebuilding the index, is to say
//! explicitly what the value should have been.
//!
//! Every substitution is declared in a file and logged when it is applied.
//! Nothing here is inferred, and nothing is silent: an override that is never
//! used is reported too, because that usually means the assumption behind it no
//! longer holds.

use crate::revm::{
    state::{AccountInfo, Bytecode},
    Database as RevmDatabase,
};
use alloy_primitives::{Address, B256, U256};
use eyre::{Context, Result};
use std::{
    collections::HashMap,
    fs,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tracing::{info, warn};

/// What an account should have held, as of a block.
#[derive(Debug, Clone, Copy)]
pub(super) struct AccountValue {
    pub(super) balance: U256,
    pub(super) nonce: u64,
}

/// Declared substitutions, keyed by the block they apply from.
#[derive(Debug, Default)]
pub(super) struct StateOverrides {
    /// `(from_block, to_block, address) -> value`, applying to every block in
    /// the inclusive range.
    ///
    /// The upper bound is required rather than optional. A balance moves, so a
    /// correction that is right at one height is wrong at a later one, and an
    /// unbounded entry goes on being applied long after it stopped being true -
    /// silently, because a stale correction looks exactly like a live one.
    entries: Vec<(u64, u64, Address, AccountValue)>,
    applied: Arc<AtomicUsize>,
}

impl StateOverrides {
    /// Reads a file of `from_block,to_block,address,balance,nonce` lines.
    ///
    /// Blank lines and `#` comments are ignored. Balances are decimal wei, so a
    /// value copied out of a witness or an explorer goes in unchanged.
    pub(super) fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)
            .wrap_err_with(|| format!("failed to read {}", path.display()))?;

        let mut entries = Vec::new();
        for (index, line) in text.lines().enumerate() {
            let line = line.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue
            }
            let fields: Vec<&str> = line.split(',').map(str::trim).collect();
            if fields.len() != 5 {
                eyre::bail!(
                    "{}:{}: expected `from_block,to_block,address,balance,nonce`, got {} fields",
                    path.display(),
                    index + 1,
                    fields.len()
                )
            }
            let from_block: u64 = fields[0]
                .parse()
                .wrap_err_with(|| format!("{}:{}: bad from_block", path.display(), index + 1))?;
            let to_block: u64 = fields[1]
                .parse()
                .wrap_err_with(|| format!("{}:{}: bad to_block", path.display(), index + 1))?;
            if to_block < from_block {
                eyre::bail!(
                    "{}:{}: to_block {} is below from_block {}",
                    path.display(),
                    index + 1,
                    to_block,
                    from_block
                )
            }
            let address: Address = fields[2]
                .parse()
                .wrap_err_with(|| format!("{}:{}: bad address", path.display(), index + 1))?;
            let balance = U256::from_str_radix(fields[3], 10)
                .wrap_err_with(|| format!("{}:{}: bad balance", path.display(), index + 1))?;
            let nonce: u64 = fields[4]
                .parse()
                .wrap_err_with(|| format!("{}:{}: bad nonce", path.display(), index + 1))?;
            entries.push((from_block, to_block, address, AccountValue { balance, nonce }));
        }

        entries.sort_by_key(|(from_block, _, address, _)| (*address, *from_block));
        info!(
            count = entries.len(),
            path = %path.display(),
            "Loaded declared state overrides"
        );
        Ok(Self {
            entries,
            applied: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// The substitutions that apply when executing `block`, if any.
    pub(super) fn for_block(&self, block: u64) -> Option<BlockOverrides> {
        let mut map: HashMap<Address, AccountValue> = HashMap::new();
        for (from_block, to_block, address, value) in &self.entries {
            if (*from_block..=*to_block).contains(&block) {
                // Entries are sorted by (address, from_block), so a later match
                // for the same address supersedes an earlier one.
                map.insert(*address, *value);
            }
        }
        (!map.is_empty()).then(|| BlockOverrides {
            values: map,
            applied: Arc::clone(&self.applied),
        })
    }

    /// Warns when a declared override never took effect.
    pub(super) fn report(&self) {
        let applied = self.applied.load(Ordering::Relaxed);
        if !self.entries.is_empty() && applied == 0 {
            warn!(
                declared = self.entries.len(),
                "No state override was applied - the accounts they name were never read"
            );
        } else if applied > 0 {
            info!(applied, "State overrides applied");
        }
    }
}

/// The substitutions in effect for one block.
#[derive(Debug, Clone, Default)]
pub(super) struct BlockOverrides {
    values: HashMap<Address, AccountValue>,
    applied: Arc<AtomicUsize>,
}

/// Serves declared values for the accounts they name, and defers otherwise.
///
/// Only `basic` is intercepted: an override states what an account held, which
/// says nothing about its storage or code, and guessing at those would trade a
/// known-wrong value for an unknown one.
pub(super) struct OverrideDatabase<DB> {
    inner: DB,
    overrides: BlockOverrides,
}

impl<DB: std::fmt::Debug> std::fmt::Debug for OverrideDatabase<DB> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OverrideDatabase")
            .field("inner", &self.inner)
            .field("overrides", &self.overrides.values.len())
            .finish()
    }
}

impl<DB> OverrideDatabase<DB> {
    pub(super) const fn new(inner: DB, overrides: BlockOverrides) -> Self {
        Self { inner, overrides }
    }
}

impl<DB: RevmDatabase> RevmDatabase for OverrideDatabase<DB> {
    type Error = DB::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let account = self.inner.basic(address)?;
        let Some(value) = self.overrides.values.get(&address) else {
            return Ok(account)
        };

        self.overrides.applied.fetch_add(1, Ordering::Relaxed);
        // Keep whatever code the account has; an override speaks only to the
        // balance and nonce.
        let mut account = account.unwrap_or_else(|| {
            AccountInfo::new(U256::ZERO, 0, B256::ZERO, Bytecode::default())
        });
        account.balance = value.balance;
        account.nonce = value.nonce;
        Ok(Some(account))
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.inner.code_by_hash(code_hash)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        self.inner.storage(address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.inner.block_hash(number)
    }
}
