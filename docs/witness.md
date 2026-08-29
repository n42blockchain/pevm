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

## Replaying the production set on Linux

The Linux host holds the full input set without a reth archive: the Rust
witness (`/data/witness-rust`, 25,765,567 items, 170 GB, recorded on the
Windows host), a geth-style ancient store with only `headers` and `bodies`
(`/data/blockchain/witness-geth`), and gov5's column set
(`/data/blockchain/witness`) for `senders` and `codes`, with gov5's Code
MDBX (`/data/blockchain/code-mdbx`) as the fallback for contracts the codes
freezer predates. The database is an empty datadir made by `init`.

```bash
pevm init --chain mainnet --datadir /data/pevm-db
pevm evm --chain mainnet --datadir /data/pevm-db -b 0 -e 25765564 \
  --witness-dir /data/witness-rust --use-witness on \
  --geth-ancient-dir /data/blockchain/witness-geth \
  --codes-dir /data/blockchain/witness --code-mdbx /data/blockchain/code-mdbx \
  --senders-dir /data/blockchain/witness -t 256
```

Three things about that set differ from the Windows one. Block hashes for
BLOCKHASH are computed from the header RLP because the `hashes` table was
not copied. The codes freezer is the 20-byte-key kind `code-import2fz`
writes from reth's `Bytecodes` table (NCIX header, 26-byte entries sorted by
the first 20 bytes of the code hash, one zstd frame per contract); it is
read by hash prefix, `keccak(code)` settles the hit, and the Code MDBX takes
the rest through one unbounded read transaction — reth's default
five-minute read-transaction timeout turned every lookup past that into an
error on the first full run. And `senders` is gov5's legacy headerless
batched table, read through the witness reader's batch decoder.

Measured on 128 cores / 256 threads (EPYC 9B45), 25,765,565 blocks,
3,678,099,879 transactions and 312,450 Ggas (`--geth-census`), every block checked against
its header:

| build | wall | CPU | notes |
|---|---|---|---|
| first run | 27.0 min | 396,850 s | 83,369 blocks failed: code MDBX transaction timed out |
| senders table, no bundle State, batch without copies, thread-local code cache | **23.9 min** | 351,575 s | 0 failures |
| same, `maxperf` + `target-cpu=native` | 23.8 min | 352,160 s | no gain at full scale |

gov5's own replay of the same range takes 41 minutes (49m48s in its
measured notes). Sender recovery was the first bottleneck: reth recovers
signatures on rayon from every worker at once, and a profile of a dense
range put 56% of CPU in crossbeam and 22% in secp256k1 before the senders
table removed both. After it the profile is the EVM itself — interpreter
dispatch, keccak, revm's State cache, bn254 pairings — at roughly
1.5 Ggas/s per physical core, which is the interpreter's natural pace;
substrate-bn in place of arkworks was 10% slower.

### revmc JIT does not fit a positional witness

The `jit` feature (LLVM 22.1, `LLVM_SYS_221_PREFIX`) builds revmc in and
`--jit` compiles hot contracts in process. On 200,000 dense blocks it
compiled 1,439 contracts and ran 16 million frames from machine code, took
3.6× the wall time and 3.8× the CPU of the interpreter, and 84 blocks
failed to replay: compiled code does not issue the same sequence of state
reads as the interpreter, and a keyless witness has no way to absorb that.
A JIT could only replay a witness that was recorded through the same JIT.
The feature stays for that experiment; the replay path does not use it.

A full run with `--jit`, failures skipped, did not finish either: after
2824 s it was at block 21.93M (the interpreter passes 20M at 862 s; the
JIT run at 2321 s) when glibc aborted the process with "corrupted
double-linked list" — heap corruption on the LLVM side, which reth itself
warns the backend may cause. Up to there 11,991 blocks had failed, all
between 3,130,512 and 7,278,436 (7,632 malformed account records, 368
witnesses not fully consumed, the rest transactions rejected on nonce or
funds — the reads that drifted). An earlier attempt aborted at block 4.03M
on a panic in a host call from a frame that cannot unwind; a storage value
longer than a word is an error now, not a panic.
