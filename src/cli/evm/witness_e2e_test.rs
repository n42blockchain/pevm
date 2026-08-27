//! A chain recorded block by block replays block by block from its witnesses
//! alone, and every replay reproduces its header. No database is needed: the
//! "archive" is an in-memory `CacheDB` advanced by each block's own output.
//!
//! The blocks are built to hit the cases a positional witness has to get
//! right: an account and slots read in an earlier block, a contract created
//! in an earlier block, an account first absent then created, reads inside a
//! call that reverts, a transaction that itself reverts, and withdrawals
//! touching accounts nothing else read.

use super::witness::{replay_block_verified, verify_execution, WitnessDatabase};
use crate::revm::{
    db::{BundleState, CacheDB, EmptyDB, OriginalValuesKnown},
    revm::database_interface::DBErrorMarker,
    state::{AccountInfo, Bytecode},
    Database,
};
use alloy_consensus::{
    constants::EMPTY_ROOT_HASH, proofs::calculate_receipt_root, Header, SignableTransaction,
    TxLegacy, TxReceipt,
};
use alloy_eips::eip4895::{Withdrawal, Withdrawals};
use alloy_primitives::{address, keccak256, Address, Bytes, Signature, TxKind, B256, U256};
use reth_chainspec::{ChainSpec, ChainSpecBuilder};
use reth_ethereum_primitives::{Block, BlockBody, Receipt, TransactionSigned};
use reth_evm::{execute::Executor, ConfigureEvm};
use reth_evm_ethereum::EthEvmConfig;
use reth_execution_types::BlockExecutionResult;
use reth_primitives_traits::{proofs, RecoveredBlock};
use std::sync::Arc;

const ALICE: Address = address!("0x000000000000000000000000000000000000a11c");
const BOB: Address = address!("0x0000000000000000000000000000000000000b0b");
const DAVE: Address = address!("0x000000000000000000000000000000000000da7e");
const COUNTER: Address = address!("0x00000000000000000000000000000000c0047e12");
const REVERTER: Address = address!("0x000000000000000000000000000000000e7e12e1");
const CALLER: Address = address!("0x00000000000000000000000000000000ca11e100");
const WITHDRAWN_TO: Address = address!("0x00000000000000000000000000000000d1d1d1d1");
const COINBASE: Address = address!("0x00000000000000000000000000000000c01b6a5e");

/// PUSH1 0 SLOAD PUSH1 1 ADD PUSH1 0 SSTORE PUSH1 1 SLOAD POP STOP.
const COUNTER_CODE: &[u8] = &[0x60, 0, 0x54, 0x60, 1, 0x01, 0x60, 0, 0x55, 0x60, 1, 0x54, 0x50, 0x00];
/// PUSH1 0 SLOAD POP PUSH1 0 PUSH1 0 REVERT.
const REVERTER_CODE: &[u8] = &[0x60, 0, 0x54, 0x50, 0x60, 0, 0x60, 0, 0xfd];

/// CALL(gas, REVERTER, 0, 0, 0, 0, 0) POP; BALANCE(DAVE) POP; SLOAD 0 POP; STOP.
fn caller_code() -> Vec<u8> {
    let mut code = vec![0x60, 0, 0x60, 0, 0x60, 0, 0x60, 0, 0x60, 0, 0x73];
    code.extend_from_slice(REVERTER.as_slice());
    code.extend_from_slice(&[0x5a, 0xf1, 0x50, 0x73]);
    code.extend_from_slice(DAVE.as_slice());
    code.extend_from_slice(&[0x31, 0x50, 0x60, 0, 0x54, 0x50, 0x00]);
    code
}

/// Init code: SLOAD 0 POP; SSTORE 0 = 1; return `COUNTER_CODE` as runtime.
fn creator_init_code() -> Vec<u8> {
    let prefix = [0x60, 0, 0x54, 0x50, 0x60, 1, 0x60, 0, 0x55];
    let len = COUNTER_CODE.len() as u8;
    let mut code = prefix.to_vec();
    let offset = (prefix.len() + 11) as u8;
    code.extend_from_slice(&[0x60, len, 0x60, offset, 0x60, 0, 0x39, 0x60, len, 0x60, 0, 0xf3]);
    code.extend_from_slice(COUNTER_CODE);
    code
}

fn chain_spec() -> Arc<ChainSpec> {
    Arc::new(ChainSpecBuilder::mainnet().shanghai_activated().build())
}

fn contract(code: &[u8]) -> AccountInfo {
    AccountInfo::new(
        U256::ZERO,
        1,
        keccak256(code),
        Bytecode::new_raw(Bytes::copy_from_slice(code)),
    )
}

fn eoa(balance: u128) -> AccountInfo {
    let mut info = AccountInfo::new(U256::from(balance), 0, alloy_primitives::KECCAK256_EMPTY, Bytecode::default());
    info.code = None;
    info
}

fn genesis() -> CacheDB<EmptyDB> {
    let mut db = CacheDB::new(EmptyDB::default());
    db.insert_account_info(ALICE, eoa(1_000_000_000_000_000_000));
    db.insert_account_info(BOB, eoa(1_000_000_000_000_000_000));
    db.insert_account_info(COUNTER, contract(COUNTER_CODE));
    db.insert_account_storage(COUNTER, U256::from(0), U256::from(5)).unwrap();
    db.insert_account_storage(COUNTER, U256::from(1), U256::from(7)).unwrap();
    db.insert_account_info(REVERTER, contract(REVERTER_CODE));
    db.insert_account_storage(REVERTER, U256::from(0), U256::from(9)).unwrap();
    db.insert_account_info(CALLER, contract(&caller_code()));
    db.insert_account_storage(CALLER, U256::from(0), U256::from(11)).unwrap();
    db
}

fn tx(nonce: u64, to: TxKind, value: u64, input: Vec<u8>) -> TransactionSigned {
    TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 10,
        gas_limit: 300_000,
        to,
        value: U256::from(value),
        input: input.into(),
    }
    .into_signed(Signature::test_signature())
    .into()
}

fn header(number: u64, parent: B256) -> Header {
    Header {
        parent_hash: parent,
        number,
        timestamp: 1_700_000_000 + number * 12,
        gas_limit: 30_000_000,
        base_fee_per_gas: Some(7),
        beneficiary: COINBASE,
        withdrawals_root: Some(EMPTY_ROOT_HASH),
        ..Default::default()
    }
}

const fn withdrawal(index: u64, to: Address, gwei: u64) -> Withdrawal {
    Withdrawal { index, validator_index: index, address: to, amount: gwei }
}

/// Three blocks whose headers still lack what execution decides.
fn blocks() -> Vec<RecoveredBlock<Block>> {
    let created = ALICE.create(2);
    let b1 = (
        vec![
            (ALICE, tx(0, TxKind::Call(COUNTER), 0, vec![])),
            (ALICE, tx(1, TxKind::Call(DAVE), 1_000, vec![])),
            (ALICE, tx(2, TxKind::Create, 0, creator_init_code())),
            (BOB, tx(0, TxKind::Call(CALLER), 0, vec![])),
            (BOB, tx(1, TxKind::Call(REVERTER), 0, vec![])),
        ],
        vec![withdrawal(0, WITHDRAWN_TO, 5), withdrawal(1, ALICE, 5)],
    );
    let b2 = (
        vec![
            (BOB, tx(2, TxKind::Call(COUNTER), 0, vec![])),
            (ALICE, tx(3, TxKind::Call(created), 0, vec![])),
            (ALICE, tx(4, TxKind::Call(WITHDRAWN_TO), 1, vec![])),
        ],
        vec![],
    );
    let b3 = (
        vec![
            (ALICE, tx(5, TxKind::Call(DAVE), 1, vec![])),
            (BOB, tx(3, TxKind::Call(CALLER), 0, vec![])),
            (ALICE, tx(6, TxKind::Call(created), 0, vec![])),
        ],
        vec![withdrawal(2, DAVE, 1)],
    );
    let mut parent = B256::ZERO;
    [b1, b2, b3]
        .into_iter()
        .enumerate()
        .map(|(i, (txs, withdrawals))| {
            let (senders, transactions): (Vec<_>, Vec<_>) = txs.into_iter().unzip();
            let mut header = header(i as u64 + 1, parent);
            header.transactions_root = proofs::calculate_transaction_root(&transactions);
            let body = BlockBody { transactions, ommers: vec![], withdrawals: Some(Withdrawals(withdrawals)) };
            let block = RecoveredBlock::new_unhashed(Block { header, body }, senders);
            parent = block.hash();
            block
        })
        .collect()
}

/// Puts what execution decided into the headers.
fn seal_headers(
    blocks: Vec<RecoveredBlock<Block>>,
    results: &[BlockExecutionResult<Receipt>],
) -> Vec<RecoveredBlock<Block>> {
    let mut parent = B256::ZERO;
    blocks
        .into_iter()
        .zip(results)
        .map(|(block, result)| {
            let senders = block.senders().to_vec();
            let mut block = block.into_block();
            block.header.parent_hash = parent;
            block.header.gas_used = result.gas_used;
            block.header.receipts_root = calculate_receipt_root(
                &result.receipts.iter().map(|r| r.with_bloom_ref()).collect::<Vec<_>>(),
            );
            block.header.logs_bloom =
                result.receipts.iter().fold(Default::default(), |bloom, r: &Receipt| bloom | r.bloom());
            let block = RecoveredBlock::new_unhashed(block, senders);
            parent = block.hash();
            block
        })
        .collect()
}

/// Advances the in-memory archive by a block's output.
fn apply(archive: &mut CacheDB<EmptyDB>, output: BundleState) {
    let changes = output.to_plain_state(OriginalValuesKnown::Yes);
    for (address, info) in changes.accounts {
        match info {
            Some(info) => archive.insert_account_info(address, info),
            None => {
                archive.cache.accounts.remove(&address);
            }
        }
    }
    for change in changes.storage {
        if change.wipe_storage {
            if let Some(account) = archive.cache.accounts.get_mut(&change.address) {
                account.storage.clear();
            }
        }
        for (slot, value) in change.storage {
            archive.insert_account_storage(change.address, slot, value).unwrap();
        }
    }
    for (hash, code) in changes.contracts {
        archive.cache.contracts.insert(hash, code);
    }
}

/// Executes the chain the way pevm records it — every block against its
/// parent's state with a fresh state — and returns each block's result,
/// witness and parent archive.
fn record(
    evm_config: &EthEvmConfig,
    blocks: &[RecoveredBlock<Block>],
) -> Vec<(BlockExecutionResult<Receipt>, Vec<u8>, CacheDB<EmptyDB>)> {
    let mut archive = genesis();
    let mut out = Vec::new();
    for block in blocks {
        let parent = archive.clone();
        let (witness_db, stream) = WitnessDatabase::new(&mut archive);
        let executor = evm_config.batch_executor(witness_db);
        let output = executor.execute(block).expect("block executes");
        apply(&mut archive, output.state);
        out.push((output.result, stream.take(), parent));
    }
    out
}

/// A replay source that serves code and block hashes and refuses state, so
/// a replay that passes provably took every account and slot from the witness.
#[derive(Debug)]
struct CodeOnly(CacheDB<EmptyDB>);

#[derive(Debug)]
struct StateReadRefused(&'static str);

impl std::fmt::Display for StateReadRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "replay asked the database for {} instead of the witness", self.0)
    }
}
impl std::error::Error for StateReadRefused {}
impl DBErrorMarker for StateReadRefused {}

impl Database for CodeOnly {
    type Error = StateReadRefused;

    fn basic(&mut self, _address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        Err(StateReadRefused("an account"))
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        Ok(self.0.code_by_hash(code_hash).unwrap_or_else(|never| match never {}))
    }

    fn storage(&mut self, _address: Address, _index: U256) -> Result<U256, Self::Error> {
        Err(StateReadRefused("a storage slot"))
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        Ok(self.0.block_hash(number).unwrap_or_else(|never| match never {}))
    }
}

#[test]
fn a_recorded_chain_replays_block_by_block_from_its_witnesses_alone() {
    let chain_spec = chain_spec();
    let evm_config = EthEvmConfig::ethereum(chain_spec.clone());

    // A first pass decides what the headers must say.
    let dry: Vec<_> = record(&evm_config, &blocks()).into_iter().map(|(result, _, _)| result).collect();
    let blocks = seal_headers(blocks(), &dry);

    let recorded = record(&evm_config, &blocks);
    assert!(recorded[0].0.receipts.iter().any(|r| !r.success), "block 1 carries a failed transaction");
    for (block, (result, witness, parent)) in blocks.iter().zip(&recorded) {
        verify_execution(&chain_spec, block, result).expect("the recording run reproduces its header");
        assert!(!witness.is_empty());
        replay_block_verified(&evm_config, &chain_spec, block, witness, CodeOnly(parent.clone()))
            .unwrap_or_else(|err| panic!("block {} replays: {err:?}", block.sealed_block().header().number));
    }
}

#[test]
fn a_wrong_or_tampered_witness_fails_the_block() {
    let chain_spec = chain_spec();
    let evm_config = EthEvmConfig::ethereum(chain_spec.clone());
    let dry: Vec<_> = record(&evm_config, &blocks()).into_iter().map(|(result, _, _)| result).collect();
    let blocks = seal_headers(blocks(), &dry);
    let recorded = record(&evm_config, &blocks);

    // Block 2 against block 1's witness.
    let (_, witness_1, _) = &recorded[0];
    let (_, _, parent_2) = &recorded[1];
    assert!(
        replay_block_verified(&evm_config, &chain_spec, &blocks[1], witness_1, CodeOnly(parent_2.clone())).is_err(),
        "a foreign witness passed"
    );

    // A flipped byte in a value execution depends on: the first record is
    // the first sender's account, byte 2 its nonce. (Header verification is
    // gas + receipts; a value nothing observable depends on, such as the
    // coinbase balance, can only be caught by a state root.)
    let (_, witness_2, _) = &recorded[1];
    let mut tampered = witness_2.clone();
    tampered[2] ^= 0x01;
    assert!(
        replay_block_verified(&evm_config, &chain_spec, &blocks[1], &tampered, CodeOnly(parent_2.clone())).is_err(),
        "tampering went unnoticed"
    );

    // A witness cut short.
    let cut = &witness_2[..witness_2.len() / 2];
    assert!(
        replay_block_verified(&evm_config, &chain_spec, &blocks[1], cut, CodeOnly(parent_2.clone())).is_err(),
        "a short witness passed"
    );
}
