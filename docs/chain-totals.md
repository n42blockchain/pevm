# Mainnet totals

Counted from the block data itself, not from an execution run. A run reports
what it processed, which equals what the chain holds only if it processed every
block exactly once; segment logs from separate runs overlap and leave gaps, so
adding them up gives an estimate rather than a total.

| | |
|---|---|
| Blocks | 0 .. 25765565 (25765566 blocks) |
| Transactions | 3678100106 |
| Gas used | 312450256843510 |

Source: `D:/geth/geth/chaindata/ancient/chain`, read 2026-08-29. Reading headers
and bodies once took 397s across 16 threads.

```bash
pevm evm --chain mainnet --geth-ancient-dir <ancient>/chain --geth-census \
  -b 0 -e <end> -t 16
```

The cost is one pass over the freezer, so the walk is split across threads and
nothing else happens per block: headers give `gas_used`, and bodies are stepped
through by RLP header to count transactions without decoding them. `--geth-census`
runs before the reth environment is opened and needs no database.

Two checks that the counter is reading what it claims: blocks 0..2000 count zero
transactions and zero gas, and block 46147 - the first transaction on mainnet -
counts one transaction and 21000 gas.

## What these totals say about throughput claims

A published figure of "40 minutes for the full chain" implies 312 Tgas / 2400s =
130 Ggas/s, which matches the gas total above almost exactly; its 3.629 billion
transactions is 1.3% under the count here, consistent with an end block slightly
below 25765565. The totals are therefore no argument against that claim.

They say nothing about how long any particular machine takes. Replaying blocks
19089319..25765565 from the witness freezer on the machine used here took 10273s
on 8 threads - that is one quarter of the chain in nearly three hours.

With all 256 threads and the tuned replay path, the same machine now does the
whole extended chain (25,864,982 blocks, 315,472 Ggas) in 1184 s — 19.7
minutes, 266.4 Ggas/s, 3.13M transactions per second (`docs/witness.md`).
