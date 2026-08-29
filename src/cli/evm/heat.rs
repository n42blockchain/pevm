// SPDX-License-Identifier: MIT OR Apache-2.0
//! Contract heat: how often each contract's code runs over a replay, and how
//! much gas passes through it.
//!
//! Keyed by the address whose *code* runs (`bytecode_address`), so a library
//! reached through DELEGATECALL is credited, not the proxy in front of it.
//! Gas is inclusive: what the frame spent including every frame it called,
//! which is the figure a JIT or a cache would want to weigh a contract by.
//! Each worker keeps its own map and merges it at the end; the per-address
//! code hash is read from the journal once, on the address's first call on
//! that worker, where the account is already loaded.

use crate::revm::{
    context::ContextTr,
    context_interface::JournalTr,
    inspector::Inspector,
    interpreter::{interpreter::EthInterpreter, CallInputs, CallOutcome},
};
use alloy_primitives::{Address, B256};
use std::collections::HashMap;

/// What one worker saw of one contract.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Heat {
    pub(super) calls: u64,
    pub(super) gas: u64,
    pub(super) first_block: u64,
    pub(super) last_block: u64,
    pub(super) code_hash: Option<B256>,
}

impl Heat {
    fn absorb(&mut self, other: &Self) {
        if self.calls == 0 {
            *self = *other;
            return;
        }
        self.calls += other.calls;
        self.gas += other.gas;
        self.first_block = self.first_block.min(other.first_block);
        self.last_block = self.last_block.max(other.last_block);
        if self.code_hash.is_none() {
            self.code_hash = other.code_hash;
        }
    }
}

/// A worker's accumulated heat, by contract.
pub(super) type HeatMap = HashMap<Address, Heat>;

/// Folds `part` into `total`.
pub(super) fn merge(total: &mut HeatMap, part: &HeatMap) {
    for (address, heat) in part {
        total.entry(*address).or_default().absorb(heat);
    }
}

/// Counts the calls of one block into the worker's map.
pub(super) struct HeatInspector<'a> {
    map: &'a mut HeatMap,
    block: u64,
}

impl<'a> HeatInspector<'a> {
    pub(super) fn new(map: &'a mut HeatMap, block: u64) -> Self {
        Self { map, block }
    }
}

impl<CTX: ContextTr> Inspector<CTX, EthInterpreter> for HeatInspector<'_> {
    fn call(&mut self, context: &mut CTX, inputs: &mut CallInputs) -> Option<CallOutcome> {
        let address = inputs.bytecode_address;
        let block = self.block;
        if let Some(entry) = self.map.get_mut(&address) {
            entry.calls += 1;
            entry.last_block = block;
            return None;
        }
        // The callee is loaded by the time its frame starts, so this is a
        // cache read, and it happens once per address per worker. Frames on
        // addresses without code — plain transfers to accounts, precompiles —
        // are not contracts and would swamp the map with every recipient the
        // chain ever had.
        let code_hash = match context.journal_mut().load_account(address) {
            Ok(account) => account.data.info.code_hash,
            Err(_) => return None,
        };
        if code_hash == alloy_primitives::KECCAK256_EMPTY {
            return None;
        }
        self.map.insert(
            address,
            Heat {
                calls: 1,
                gas: 0,
                first_block: block,
                last_block: block,
                code_hash: Some(code_hash),
            },
        );
        None
    }

    fn call_end(&mut self, _context: &mut CTX, inputs: &CallInputs, outcome: &mut CallOutcome) {
        if let Some(entry) = self.map.get_mut(&inputs.bytecode_address) {
            entry.gas += outcome.result.gas.total_gas_spent();
        }
    }
}

/// Writes the merged map as CSV, hottest first.
pub(super) fn write_csv(path: &std::path::Path, total: &HeatMap) -> eyre::Result<()> {
    use std::io::Write;
    let mut rows: Vec<(&Address, &Heat)> = total.iter().collect();
    rows.sort_by(|a, b| b.1.calls.cmp(&a.1.calls).then(b.1.gas.cmp(&a.1.gas)));
    let mut out = std::io::BufWriter::new(std::fs::File::create(path)?);
    writeln!(out, "rank,address,code_hash,calls,gas_inclusive,first_block,last_block")?;
    for (rank, (address, heat)) in rows.iter().enumerate() {
        writeln!(
            out,
            "{},{:?},{},{},{},{},{}",
            rank + 1,
            address,
            heat.code_hash.map(|h| format!("{h:?}")).unwrap_or_default(),
            heat.calls,
            heat.gas,
            heat.first_block,
            heat.last_block
        )?;
    }
    out.flush()?;
    Ok(())
}
