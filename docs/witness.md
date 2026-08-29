# Witness freezer

Records the state a block reads, so the block can later be re-executed without a
database. The format is N42's freezer (`witness.cidx` + `witness.NNNN.cdat`),
matching what gov5 writes.

## Recording

Two modes produce the same bytes; they differ in what they need to run.

**Parallel** (`--witness-dir`) spreads blocks across workers, each reading the
state as of its block's parent. That needs a reth archive, because an
out-of-order block can only get its parent state by walking changesets
backwards.

```bash
pevm evm --chain mainnet --datadir <reth> -b 0 -e <end> --witness-dir <out>
```

**Forward** (`--state-dir`) executes in block order and carries the state along,
so the parent state of block N is just what N-1 left behind - no historical
lookup at all. Combined with `--geth-ancient-dir` it does not touch a reth
database. The cost is that blocks cannot be spread across workers.

```bash
pevm evm --chain mainnet -b 0 -e <end> --witness-dir <out> \
  --state-dir <state> --geth-ancient-dir <geth>/chaindata/ancient/chain
```

Recording 0..300000 both ways produces byte-identical freezers.

Only the parallel mode reads history, so only it can be affected by a bad
history index - and only it accepts `--state-overrides`. Forward recording
computes the value itself, so an override there would overwrite a correct
value with an asserted one; the combination is refused rather than ignored.

### The freezer is positional

Item N *is* block N, and appends are strictly sequential. `--begin` must equal
the number of index entries already written; a run that stops mid-way resumes
from exactly there. A block that fails to execute aborts the whole run instead
of leaving a hole - a gap could not be filled later without rewriting the index.

## Replaying

```bash
pevm evm --chain mainnet --datadir <reth> -b 0 -e <end> \
  --witness-dir <in> --use-witness on -t 16
```

Replay is witness-only: account and storage reads must come from the witness or
the block fails. It never falls back to the database, because a fallback would
paper over a witness that no longer matches the execution it was recorded from
and still report a clean run.

Only code and block hashes come from outside, since neither is recorded.
`--geth-ancient-dir` and `--codes-dir` supply them without a reth archive.

Each block is checked against its header: gas, and from Byzantium on the
receipts root and logs bloom. Gas alone is weak - it says the block consumed
what it should, not that each transaction produced the right result. A witness
that is fully consumed is also required, since leftover bytes mean the replay
read fewer values than were recorded.

## What the checks do and do not catch

Reordered block rewards were invisible to every check for a while: the gas was
right, the number of reads was right, only the values were paired with the wrong
accounts. What surfaced it was comparing two recordings of the same range
byte-for-byte. When something looks wrong and the checks are quiet, that
comparison is the tool to reach for.

A witness is only meaningful if the execution that produced it is reproducible.
`post_block_balance_increments` computes beneficiaries in a fixed order but
collected them into a hash map, so the same block recorded differently on every
run - 2.5% of blocks, always a pure reordering. `vendor/alloy-evm` keeps that
order; see `patches/README.md`.

## Cross-engine witnesses are not interchangeable

pevm and gov5 read state in different orders. Sampling 2000 blocks, 39.9%
differ - the same records, different sequence. Since the format is keyless, a
witness from one engine cannot be replayed by the other. Each is self-consistent
with itself.

## When a block will not execute

`--verify` runs plain execution block by block with the same header checks, so a
divergence names the block that caused it, and a gas mismatch reports
per-transaction cumulative gas to narrow it to a transaction.

`--trace-block` prints the call tree - calls, creates, self-destructs, with the
gas each frame was given. A witness records reads, so an execution that diverges
in its *writes* leaves nothing in it to compare; the trace shows what moved
value.

`--query-account` prints what the historical provider reports for an account as
of `--begin`. This separates "the state handed to execution was already wrong"
from "execution went wrong", which otherwise look identical from the outside.

### An account the database reports wrongly

An account history index can be incomplete for one account while the rest of the
database is sound. The lookup then resolves to the *current* value instead of
the historical one - well-formed, just from the wrong height, and nothing in the
executor can tell the difference.

In `D:\reth2k` the zero address is such an account: its index stops around block
17456000, and every lookup past that returns 13430.037064 ETH. Other
high-traffic accounts in the same database are fine, and reth's lookup code is
byte-identical between 1.10.2 and 2.5.1, so this is one gap rather than a broken
index or a code regression.

Block 18116189 read that account 1680.121 ETH richer than it was, took a
different branch, and burned 440639 gas more than the header allows.

`--state-overrides <file>` declares the correct values:

```
# from_block,to_block,address,balance,nonce
18116189,18116189,0x0000000000000000000000000000000000000000,11749916056243464802008,0
```

The upper bound is required, and the reason is the failure it prevents. The
first version of this entry had no end and applied from 17456000 onward. It
unblocked 18116189 and then broke 20846649, 2.7M blocks later: by then the
balance had risen to exactly the value the index was wrongly returning, so the
stored value was right and the correction was the stale one. A correction is
true at a height, not forever.

Only `basic` is intercepted - an override says what an account held, which says
nothing about its storage or code. Every substitution is logged, and one that
never fires is reported too, since that usually means the assumption behind it
no longer holds.

Use this only where the stored value is known to be wrong *and* the correct one
is known. Rebuilding the index is the real fix.
