# reth patches

## reth-v2.5.1-windows.patch

reth v2.5.1 uses unix-only positioned file I/O in two places without a `cfg`
gate, so `reth-provider` and `reth-cli-commands` do not compile on Windows at
all:

| file | API | why it breaks |
|---|---|---|
| `crates/static-file/types/src/changeset_offsets.rs` | `FileExt::read_exact_at` | the module is gated `#[cfg(all(feature = "std", unix))]`, but `reth-provider` imports `ChangesetOffsetReader`/`ChangesetOffsetWriter` unconditionally |
| `crates/cli/commands/src/download/fetch.rs` | `FileExt::write_all_at` | `pub mod download` is not feature-gated, so it always compiles |

v2.4.1 has the same problem. The other four `os::unix` uses in the tree are
correctly gated.

The patch replaces both with shims that keep `FileExt` on unix and drive
Windows' `seek_read` / `seek_write` to completion (those may transfer less than
asked, unlike the unix `_exact` / `_all` variants).

### Applying it

The build expects a patched reth checkout at the path named in the `[patch]`
section at the bottom of `Cargo.toml`:

```bash
git clone --depth 1 --branch v2.5.1 https://github.com/paradigmxyz/reth.git ../reth-patched
cd ../reth-patched && git apply ../pevm/patches/reth-v2.5.1-windows.patch
```

Every reth crate is redirected to that one tree, not just the two broken ones:
a patched crate pulls its siblings in by path, and those would collide with the
same crates coming from the upstream git source.

### Removing it

Once these land upstream, delete the `[patch]` section from `Cargo.toml`, delete
this directory, and the git dependencies take over again.

## alloy-evm-0.38.0-deterministic-order.patch

`post_block_balance_increments` computes the post-block beneficiaries in a
fixed order - ommers in header order, then the block beneficiary, then
withdrawals - but collects them into an `AddressMap`, which is a `HashMap`
with a randomly seeded hasher. `increment_balances` then iterates that map, so
the beneficiary accounts are read in a different order on every run.

Ordinary execution does not care. A keyless witness does: it records state
reads as a bare ordered stream with no addresses, so the same block produced a
different witness on every run - measured at 2.5% of blocks over 0..300000,
every one of them a pure reordering of the same records. A witness recorded in
one order and replayed in another silently pairs values with the wrong
accounts, and neither the gas check nor the fully-consumed check notices,
because the gas and the number of reads are identical either way.

The patch adds `post_block_balance_increments_ordered`, which returns a
`Vec<(Address, u128)>` keeping the order the beneficiaries were computed in,
and switches the Ethereum block executor to it. The original map-returning
function is left in place. Repeated addresses merge into their first position,
so the number of account reads is unchanged.

That order matches N42-gov5 (`internal/ethel/consensus.go` `Finalize`:
`AddBalance` per uncle in header order, then the miner;
`internal/api/engine_payload_stateful.go`: withdrawals in array order, skipping
zero amounts), which is what this witness format has to interoperate with.

### Applying it

```bash
cp -r ~/.cargo/registry/src/*/alloy-evm-0.38.0 vendor/alloy-evm
cd vendor/alloy-evm && git apply ../../patches/alloy-evm-0.38.0-deterministic-order.patch
```

`vendor/alloy-evm` is wired in through `[patch.crates-io]` in `Cargo.toml`.

### Known remaining difference

The DAO fork refund (block 1920000) is applied by alloy-evm during
post-execution, while gov5 applies it in `internal/consensus/misc/dao.go`.
Only that one block is affected; it has not been compared byte for byte.
