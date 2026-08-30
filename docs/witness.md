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
3,678,099,879 transactions and 312,450 Ggas (`--geth-census`), every block
checked against its header:

| build | wall | CPU | notes |
|---|---|---|---|
| first run | 27.0 min | 396,850 s | 83,369 blocks failed: code MDBX transaction timed out; tail skipped (below) |
| senders table, no bundle State, batch without copies, thread-local code cache | 23.9 min | 351,575 s | tail skipped: the last 889,000 blocks never ran |
| + reader follows the index, one block at a time, reused CacheState, private code copies, `-s 64` | 23.3 min | 350,905 s | every block, 0 failures, 0 tasks aborted |
| + one static copy of each contract's code, per-block `ReplayCache` in place of revm's `State` | **20.5 min** | 309,749 s | **every block, 0 failures, 0 tasks aborted**; peak 106 GB |

The "tail skipped" runs looked clean and were not: gov5's senders table
is appended in batches of 64 from wherever a resumed writer stood, so from
block 24,792,851 its blobs start at 19 modulo 64, and a reader that
assumed blobs at multiples of 64 handed every later block another block's
senders. The length check refused the block, the error took the whole
3-block task with it, and that was logged as a thread execution error —
not a failed block, not in the failure count. 296,277 tasks were lost that
way in each of those runs. The reader now finds a blob by walking the
index entries, a block that cannot be read is one failed block, and tasks
that abort are counted and printed at the end. The tail is the heaviest
part of the chain, which is why the honest number is only slightly better
than the flawed one despite the work below.

gov5's own replay of the same range takes 41 minutes (49m48s in its
measured notes). Sender recovery was the first bottleneck: reth recovers
signatures on rayon from every worker at once, and a profile of a dense
range put 56% of CPU in crossbeam and 22% in secp256k1 before the senders
table removed both. After it the profile is the EVM itself — interpreter
dispatch, keccak, revm's State cache, precompiles — at roughly
1.5 Ggas/s per physical core, which is the interpreter's natural pace;
substrate-bn in place of arkworks was 10% slower.

### Where a single thread spends its time

`perf` on one worker, 1,000 blocks per range, self time by category:

| | 12.0M | 16.0M | 20.0M |
|---|---|---|---|
| interpreter proper (opcode implementations, dispatch, gas, jump analysis, U256) | 48.7% | 47.4% | 40.8% |
| precompiles (ecrecover; bn254; BLS12-381 / KZG after Cancun) | 8.2% | 13.6% | 32.5% |
| keccak | 13.6% | 14.3% | 8.9% |
| revm State / journal | 8.3% | 5.8% | 4.5% |
| kernel, decode, allocation, frames, other | 21% | 19% | 13% |

A bytecode compiler touches the first row only. Even at infinite speed
that caps a block at 1.95× / 1.90× / 1.69×; revmc's own 1.85–2.77× on
WETH-like code puts the realistic figure at 1.3–1.45×, and that is what
the top-10k AOT measures on one thread: 1.44× at 12M, 1.36× at 16M, 1.29×
at 20M (3,000 blocks each). The 19× (revmc, Fibonacci), 6.9× (BNB, fib_255)
and 15× (Nethermind, a spin loop) in the literature are for code that is
all first row. Witness replay removes the disk, not the host operations.

### Many threads

One worker replays the 12.0M band at 2.8 Ggas/s; 16 workers at 2.79 each;
32 at 2.30; 64 at 1.92; 128 at 1.20; 256 at 0.80 (SMT). Between 8 and 128
threads the instructions per block stay at 35M while the IPC falls from
2.05 to 1.17, the clock only from 4.12 to 3.96 GHz, and nothing the fill
counters see changes: demand DRAM fills 3k per block, L3 fills 22k, TLB
misses, page faults and instruction-cache misses all flat, hardware
prefetch traffic 0.5 MB per block, cross-CCD cache transfers zero, kernel
time 0.2%. Pure compute (openssl sha256) scales to 128 cores at −1% per
thread, and a pointer chase over 512 MB shows DRAM latency rising only from
122 to 167 ns. Two independent 64-thread processes on the two halves of the
chip finish 22% sooner than one 128-thread process on all of it, so the
loss is inside the process, not in the silicon.

IBS sampling (no skid, with data source and latency) named it: at 128
threads the latency-weighted memory stalls per block are ten times those
at 8, and almost all of them are *HitM* — the line was found modified in
another core's cache — at 1,355 cycles inside a CCX and 2,964 across CCDs
against 67 and 436 on a quiet chip. The stalled instructions are the
allocation and release paths of revm's `State`: `TransitionAccount` drops,
`RawTable::drop_elements`, `load_cache_account_with`, `CacheAccount::change`.
`State::commit` builds a `TransitionAccount` — a clone of the previous
info and a fresh storage map — for every touched account of every
transaction and drops it at once; the freed lines go back through the
allocator and come to the next thread still owned by another core.

Fixed by not doing it: `ReplayCache` (`src/cli/evm/replay_cache.rs`) is a
per-block account cache that runs revm's own `AccountStatus` machine so the
reads reaching the witness are the ones `State` would make, and commits
without transitions; the executor is alloy-evm's `EthBlockExecutor`
directly, whose `StateDB` bound any `Database + DatabaseCommit` satisfies.
12.0M–12.2M at 256 threads: CPU 2394–2426 s to 1992–1999 s, wall 11.0 s to
9.4 s; the full chain 23.3 to 20.5 minutes. Before it, sharing one static
copy of each contract's code across threads (`Bytes::from_static` clones
without a reference count, so a thread's private `Bytecode` can sit over
the shared bytes) took the wall from 12.2–12.5 s to 11.0 s and 6 GB off the
peak.

Tried and measured as no gain, each against the same 200,000 blocks:
`taskset` onto physical cores, pinning each worker to a CPU (migrations
are 1.4 per thread per second), a thread-local keccak cache in place of
alloy's global one, dropping that cache (16% slower on one thread, 5% at
256), mimalloc for snmalloc (+3 GB, same time), transparent huge pages set
to `always` (684 MB of 18 GB anonymous memory ended up in huge pages),
`RAYON_NUM_THREADS` (rayon is not on the per-block path), and task sizes
of 16, 256 or 1024 blocks against 64. The fixed cost of a block — the
wrappers, the EVM, the executor, the header check — is 0.096 ms of CPU on
near-empty blocks, under 1% of the run, so nothing is left to save by
reusing them across a task.

After the per-block cache the 256-thread profile is interpreter 37%,
state and journal 28%, keccak 12.5%, precompiles 9%, allocation 6%. Levers
measured on the same 200,000 blocks since: a profile-guided build
(`scripts/pgo-build.sh`) takes 2.7–3.7% off the CPU time; a 4M-entry
keccak cache (`PEVM_KECCAK_CACHE_ENTRIES=4194304`, +0.5 GB) 1%; carrying
the journal's containers and the interpreter frames from block to block
and handing the journal its emptied account table back after every
transaction nothing (reverted); `-C target-cpu=native` nothing (sha3-asm
already picks the scalar Keccak that is fastest on Zen 5). GMP for modexp
(`--features gmp`): modexp is 0.03–0.6% of the CPU in most eras and 2.4%
at 25.0M, and GMP takes 0.1–1.4% off there and 0.7% at 18.5M.

What remains at 256 threads is the interpreter itself and the SMT pair
sharing a core: 128 threads on 64 cores replay as fast as 128 threads on
128 cores, because a single thread of this workload leaves half the core
idle on memory stalls and the sibling fills it.

### evmone instead of revm?

evmone (C++, EVMC) runs 4.9× faster than geth's interpreter on synthetic
loops but sits in revm's tier — guillotine reports "on par with evmone,
ahead of revm". Interpreter differences of 1.2–1.5× apply to the first
row above, so the block-level expectation is under 1.15×, for an EVMC host
bridge crossed on every SLOAD, SSTORE and CALL, a re-plumbed executor, and
a witness that would have to be recorded again through evmone. Not worth
it for this workload.

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

## Contract heat

`--contract-heat <FILE>` writes, after a replay, one CSV row per contract
whose code ran — the address whose code executed (a DELEGATECALL credits the
library, not the proxy), its code hash, call frames, gas spent by the frame
including everything it called, and the first and last block it ran in —
hottest first. Addresses without code are left out. Over blocks
0..25,765,564: 83,386,919 contracts and 13.9 billion call frames;
51.5 million contracts were called exactly once and 93.2% at most eight
times, while the 10,000 hottest take 79.1% of the frames and 71.2% of the
inclusive gas (WETH9 alone 1.53 billion frames). The tables and a summary
sit next to the witness: `/data/witness-rust/contract-heat-20260829*`.

## AOT of the hottest contracts: expected against measured

The literature sets the expectation. Paradigm's revmc announcement
([paradigm.xyz/2024/06/revmc](https://www.paradigm.xyz/2024/06/revmc))
measured 19× on Fibonacci, 1.85–2.77× on WETH and a counter, and
"O(1–10%) depending on the block range" on an Ethereum L1 historical sync,
because most of L1's work is host operations a bytecode compiler cannot
touch; BNB Chain's deep-dive
([bnbchain.org](https://www.bnbchain.org/en/blog/a-technical-deep-dive-on-the-jit-aot-compiler-for-revm-of-bnb-chain))
put it at 6.9× for fibonacci_255, 1.6× for a merkle hash, 1.13× for WBNB
and 1.01× for a PancakeSwap pair read, with the speedup "inversely
proportional to the number of stateful opcodes". Nethermind's IL-EVM
([PR #3888](https://github.com/NethermindEth/nethermind/pull/3888),
[PR #6985](https://github.com/NethermindEth/nethermind/pull/6985)) saw 15×
on a spin loop and 1.0–1.7× on mixed bytecode, and neither was merged;
megaeth's evmone-compiler
([github](https://github.com/megaeth-labs/evmone-compiler)) is archived
without numbers; Ipsilon's report
([Geth vs evmone](https://notes.ethereum.org/@ipsilon/evm-performance-report-geth-vs-evmone))
shows the spread among *interpreters* (evmone 784 Mgas/s against geth's
161 on synthetic loops) is larger than what compilation adds on real blocks.
Replay of Ethereum history is the state-heavy case, so the expectation for
whole blocks is a few percent.

Measured here, with the 10,000 hottest contracts of the heat table (79.1%
of all call frames) compiled ahead of time — 8,123 distinct codes, 39,783
(code, spec) artifacts, 3.79 GB of machine code at LLVM -O3, 10.4× the
bytecode, 334 s to build on 128 workers — and loaded before the replay:

| 20.0M–20.2M, 200,000 blocks, 3,027 Ggas | replay phase | failed blocks |
|---|---|---|
| interpreter | 19.0 s | 0 |
| top-10k AOT, 74% of frames compiled | 19.0 s | 165 |

The same wall time, after two fixes to the revmc runtime that the first
attempts needed (`patches/revmc-cf68a87-runtime.patch`): every lookup
pushed an event to the backend's bounded queue, which was 53% of CPU across
256 threads, and each artifact was `dlopen`ed lazily, so its builtins were
bound under glibc's global load lock. Loading 39,783 artifacts takes 51 s
and unloading them at exit tens of seconds more, both fixed costs. The 165
failures are the same read-order divergence as the JIT: compiled code does
not issue the interpreter's sequence of state reads, which a positional
witness cannot absorb. Contract heat is not the lever for this workload; the
replay stays on the interpreter.
