# n42-eth-snapshot — the eth-el distribution client, in Rust

A port of gov5's `cmd/n42-eth-snapshot` + `cmd/n42-eth-manifest`
(spec: N42-gov5 `docs/ethel/n42-eth-client-distribution.md`): the
three-tier minimal / full / archive contract, byte-compatible.

- **Formats**: `manifest-<mode>.json` (blake2b-256 per file; sha256
  `manifest_id` over the sorted `path\tsize\thash` lines), the
  publisher's `releases.json` (latest + releases + deltas), delta trees
  at `deltas/<from>-<to>/<mode>/` with `delta-manifest-<mode>.json`,
  and the mode selectors (sections, patterns, the optional caplin
  seeds, the one-year bodies window for `full`) — all mirrored from the
  Go side verbatim.
- **Cross-client proof**: on the same datadir, `n42-eth-manifest` (Go)
  and `n42-eth-snapshot manifest` (Rust) produce identical
  `manifest_id`s for `minimal` and `archive`, and each side's `verify`
  accepts the other's manifest.
- **Tests**: `cargo test --lib snapshot` carries the Go suite's
  scenarios — mode detection, fetch/verify round trip, corruption,
  downgrade dry-run, status against a mirror, delta chains with
  max-iterations, and the follow loop picking up a release published
  mid-run.

## Chasing the live tip

```bash
# Bootstrap a tier
n42-eth-snapshot fetch --source https://mirror/mainnet --datadir /var/lib/n42 --mode archive

# One-shot: apply every available delta
n42-eth-snapshot catch-up --source https://mirror/mainnet --datadir /var/lib/n42 --mode archive

# Autopilot: poll every 30 s, apply new deltas as they publish
n42-eth-snapshot follow --source https://mirror/mainnet --datadir /var/lib/n42 \
    --mode archive --interval-secs 30
```

Every file a delta brings in is blake2b-verified before it is renamed
into place, and the installed manifest pins the tree at the new height
— that is the minute-scale verification a delta needs. For the deeper
check on `archive` data, `follow --verify-cmd` runs an operator
command after each applied delta with `{datadir}`, `{from}`, `{to}`
substituted — e.g. a witness replay of exactly the appended range:

```bash
n42-eth-snapshot follow ... --verify-cmd \
  'pevm evm --chain mainnet --datadir /data/pevm-db -b {from} -e {to} \
     --witness-dir {datadir}/chain/freezer --use-witness on ...'
```

## Tested against the real archive

Three tests over `/data/blockchain/witness` (gov5's real 858 GiB
archive input set) and `/data/witness-rust` (the pevm-recorded
witness), 2026-08-30:

1. **Manifest + verify on real files** — an archive-shaped datadir
   with 19 GB of real freezer files: Go and Rust `manifest` produce the
   same `manifest_id`, each side's `verify` accepts the other's
   manifest (Rust 2.2 s, Go 7.6 s), and a single flipped byte in a
   2 GB segment is reported by both, naming the file.
2. **Delta catch-up over HTTP** — a mirror publishing release A
   (witness through segment 0) and release B (+ segment 1): `fetch` of
   A in 2.3 s, `status` reports 788k blocks behind, `catch-up` applies
   the 2.0 GB delta in ~2 s and lands on B's manifest_id; the Go
   client run against the same datadir agrees.
3. **Follow + replay verification** — `follow` polling every 5 s picks
   up a release published mid-run, applies the delta, and
   `--verify-cmd` replays the 787,649 appended blocks from the fetched
   witness in **17.7 s** with zero failures and no aborted tasks —
   catch-up plus full re-execution of the new range, well inside a
   minute.

One finding worth the fixture rebuild it caused: a positional witness
replays only through the engine that recorded it. gov5's shipped
`witness.*` is its own executor's read stream and fails immediately
under pevm (nonce/balance mismatches from the first blocks); the
pevm-recorded witness in the same NCIX container replays cleanly. A
distribution whose archive tier is meant to be verified by a given
client must ship that client's witness stream — the container format
is shared, the stream is per-recorder.

## Not ported yet

`pevm evm` reads the standard freezer tables (witness, senders, codes)
and geth ancient bodies; gov5's archive tier ships headers and bodies
as N42 *columnar* files (`headerc`/`bodyc`, 8192-block segments,
per-field zstd). Until those readers exist on the Rust side, the
built-in replay verification of a gov5-published archive needs bodies
in a format pevm reads; `--verify-cmd` keeps the hook format-agnostic.
The publisher tools (`n42-eth-delta-build`, `n42-eth-publish`,
torrent metadata) also stay Go-side for now.
