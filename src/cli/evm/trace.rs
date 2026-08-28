// SPDX-License-Identifier: MIT OR Apache-2.0

//! Call-level tracing for diagnosing an execution that diverges from the chain.
//!
//! A witness records reads, so a divergence that lives in *writes* stays
//! invisible in it - two runs can read the same values for a thousand accesses
//! and still be building different state. This records the events that move
//! value instead: calls, creates and self-destructs, with the gas each frame
//! was given and what it returned.

use alloy_primitives::{Address, U256};
use crate::revm::{
    context::ContextTr,
    inspector::Inspector,
    interpreter::{
        interpreter::EthInterpreter, CallInputs, CallOutcome, CreateInputs, CreateOutcome,
    },
};
use std::cell::RefCell;
use std::rc::Rc;

/// One recorded event, flattened to a line so two runs can be diffed.
#[derive(Debug, Clone)]
pub(super) enum TraceEvent {
    Call {
        depth: usize,
        from: Address,
        to: Address,
        value: U256,
        gas_limit: u64,
    },
    CallEnd {
        depth: usize,
        to: Address,
        success: bool,
        gas_used: u64,
        output_len: usize,
    },
    Create {
        depth: usize,
        from: Address,
        value: U256,
        gas_limit: u64,
    },
    CreateEnd {
        depth: usize,
        address: Option<Address>,
        success: bool,
        gas_used: u64,
    },
    SelfDestruct {
        contract: Address,
        target: Address,
        value: U256,
    },
}

impl std::fmt::Display for TraceEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Call {
                depth,
                from,
                to,
                value,
                gas_limit,
            } => write!(
                f,
                "{:indent$}CALL     {to:?} from {from:?} value {value} gas {gas_limit}",
                "",
                indent = depth * 2
            ),
            Self::CallEnd {
                depth,
                to,
                success,
                gas_used,
                output_len,
            } => write!(
                f,
                "{:indent$}CALL_END {to:?} {} gas_used {gas_used} out {output_len}",
                "",
                if *success { "ok" } else { "FAIL" },
                indent = depth * 2
            ),
            Self::Create {
                depth,
                from,
                value,
                gas_limit,
            } => write!(
                f,
                "{:indent$}CREATE   from {from:?} value {value} gas {gas_limit}",
                "",
                indent = depth * 2
            ),
            Self::CreateEnd {
                depth,
                address,
                success,
                gas_used,
            } => write!(
                f,
                "{:indent$}CREATE_END {address:?} {} gas_used {gas_used}",
                "",
                if *success { "ok" } else { "FAIL" },
                indent = depth * 2
            ),
            Self::SelfDestruct {
                contract,
                target,
                value,
            } => write!(f, "SELFDESTRUCT {contract:?} -> {target:?} value {value}"),
        }
    }
}

/// Records call-level events; shared so it outlives the EVM that consumes it.
#[derive(Debug, Clone, Default)]
pub(super) struct CallTracer {
    events: Rc<RefCell<Vec<TraceEvent>>>,
    depth: Rc<RefCell<usize>>,
}

impl CallTracer {
    pub(super) fn events(&self) -> Vec<TraceEvent> {
        self.events.borrow().clone()
    }
}

impl<CTX: ContextTr> Inspector<CTX, EthInterpreter> for CallTracer {
    fn call(&mut self, _context: &mut CTX, inputs: &mut CallInputs) -> Option<CallOutcome> {
        let depth = *self.depth.borrow();
        self.events.borrow_mut().push(TraceEvent::Call {
            depth,
            from: inputs.caller,
            to: inputs.target_address,
            value: inputs.value.get(),
            gas_limit: inputs.gas_limit,
        });
        *self.depth.borrow_mut() += 1;
        None
    }

    fn call_end(&mut self, _context: &mut CTX, inputs: &CallInputs, outcome: &mut CallOutcome) {
        let depth = self.depth.borrow().saturating_sub(1);
        *self.depth.borrow_mut() = depth;
        self.events.borrow_mut().push(TraceEvent::CallEnd {
            depth,
            to: inputs.target_address,
            success: outcome.result.result.is_ok(),
            gas_used: outcome.result.gas.total_gas_spent(),
            output_len: outcome.result.output.len(),
        });
    }

    fn create(&mut self, _context: &mut CTX, inputs: &mut CreateInputs) -> Option<CreateOutcome> {
        let depth = *self.depth.borrow();
        self.events.borrow_mut().push(TraceEvent::Create {
            depth,
            from: inputs.caller(),
            value: inputs.value(),
            gas_limit: inputs.gas_limit(),
        });
        *self.depth.borrow_mut() += 1;
        None
    }

    fn create_end(
        &mut self,
        _context: &mut CTX,
        _inputs: &CreateInputs,
        outcome: &mut CreateOutcome,
    ) {
        let depth = self.depth.borrow().saturating_sub(1);
        *self.depth.borrow_mut() = depth;
        self.events.borrow_mut().push(TraceEvent::CreateEnd {
            depth,
            address: outcome.address,
            success: outcome.result.result.is_ok(),
            gas_used: outcome.result.gas.total_gas_spent(),
        });
    }

    fn selfdestruct(&mut self, contract: Address, target: Address, value: U256) {
        self.events.borrow_mut().push(TraceEvent::SelfDestruct {
            contract,
            target,
            value,
        });
    }
}
