# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PEVM is a high-performance parallel EVM (Ethereum Virtual Machine) block executor built on top of Reth. It processes Ethereum blocks in parallel across multiple threads, with a sophisticated log system that can record state reads during execution and replay them later without database access (10-50x faster).

## Build & Run Commands

```bash
# Build (release)
cargo build --release

# Build (with debug symbols for profiling)
cargo build --profile profiling

# Run EVM execution against Reth database
./target/release/pevm evm -b <begin_block> -e <end_block> --datadir <reth_db_path>

# Run with state log generation (mmap mode, recommended)
./target/release/pevm evm -b 3000000 -e 3500000 --log-block on --datadir <reth_db_path> --log-dir <output_dir> --mmap-log

# Run from pre-recorded logs (mmap mode, lock-free)
./target/release/pevm evm -b 3000000 -e 3500000 --use-log on --log-dir <log_dir> --datadir <reth_db_path> --mmap-log

# Run tests
cargo test

# Run a single test
cargo test <test_name>

# Run clippy lint checks
cargo clippy

# Generate flamegraph (requires cargo-flamegraph)
cargo flamegraph --profile profiling -- evm -b <begin> -e <end> --datadir <path> --use-log on --log-dir <log_dir>
```

### Key CLI Parameters
- `-b` / `--begin`: Start block number
- `-e` / `--end`: End block number
- `-s` / `--step`: Task batch size (default 3, controls load balancing granularity)
- `-t` / `--threads`: Worker thread count (default: `CPU_COUNT * 2 - 1` on Unix, `CPU_COUNT` on Windows)
- `--log-block on`: Enable state log recording during execution
- `--use-log on`: Execute from pre-recorded state logs
- `--mmap-log`: Use memory-mapped file storage (better performance, enables lock-free mode)
- `--compression <algo>`: Compression for logs (`none|zstd|brotli|lzma|lz4|auto`)
- `--enable-profiling`: Enable built-in CPU profiling (generates `flamegraph.svg`)
- `--single-thread`: Force single-thread mode
- `--repair-log`: Repair corrupted log files
- `--rebuild-idx`: Rebuild log index files

## Architecture

### Source Structure
```
src/
├── main.rs                    # Entry point, allocator setup, Reth CLI bootstrap
├── lib.rs                     # Library root, re-exports Reth components
├── ress.rs                    # RESS sub-protocol installation
└── cli/
    ├── mod.rs                 # CLI module root
    ├── interface.rs           # Main CLI interface (Cli<C, Ext>, Commands enum)
    ├── chainspec.rs           # Chain spec parser
    ├── evm/
    │   ├── mod.rs             # EvmCommand - core execution logic (~3600 lines)
    │   ├── logged_db.rs       # LoggingDatabase / DbLoggedDatabase wrappers
    │   ├── state_log.rs       # MmapStateLogDatabase / MmapStateLogReader
    │   ├── profiling.rs       # Cross-platform CPU profiler (Linux/macOS/Windows)
    │   └── log_test.rs        # Log I/O tests
    └── debug_cmd/
        ├── mod.rs             # Debug subcommands
        ├── execution.rs       # Block execution verification
        ├── merkle.rs          # Merkle trie debugging
        ├── in_memory_merkle.rs # In-memory state root computation
        └── build_block.rs     # Block building from txpool
```

### Core Execution Flow (`cli/evm/mod.rs`)

1. **Task Generation**: Block range is split into `Task{start, end}` batches by `step_size`
2. **Task Distribution**: Two modes:
   - **Lock-free mode** (when `--use-log on --mmap-log`): Tasks pre-assigned to threads, each thread iterates its own local list — zero Mutex contention
   - **Shared queue mode** (default): `Arc<Mutex<VecDeque<Task>>>` with work-stealing
3. **Worker Threads**: Each thread executes blocks using Reth's EVM with either database-backed or log-backed state
4. **Monitoring Thread**: Reports blocks/s, TPS, Ggas/s every second via atomic counters (`Relaxed` ordering)
5. **Graceful Shutdown**: Ctrl+C sets `AtomicBool` flag, all threads check and exit

### Log System (Two Modes)

**Recording mode** (`--log-block on`):
- `LoggingDatabase<DB>` wraps the real database, intercepts `basic()` and `storage()` calls
- Captures `ReadLogEntry::Account` / `ReadLogEntry::Storage` with Compact-encoded data
- Writes to log files (cumulative binary format or mmap format)

**Replay mode** (`--use-log on`):
- `DbLoggedDatabase` reads from pre-recorded logs (zero-copy from mmap)
- Serves state reads from memory without database access
- Falls back to real DB only for bytecode lookups (cached in `DashMap`)

### Storage Formats

**Mmap format** (`--mmap-log`, recommended):
- `state_logs_data.bin`: Header(32B) + continuous block log data
- `state_logs_index.bin`: `block_number(8) + offset(8) + length(4)` per entry
- `MmapStateLogReader` is `Sync` (read-only mmap, supports hundreds of concurrent threads)

**Cumulative format** (default):
- `blocks_log.bin`: Appended block data with compression
- `blocks_log.idx`: `block_number(8) + offset(8) + length(8)` per entry
- Supports resume after interruption

### Key Performance Patterns

- **Bytecode Cache**: Global `DashMap<B256, Bytecode>` — contract code is immutable, safe to share
- **Thread-local caches**: Each worker has local `HashMap` for bytecode and block hashes
- **StateProvider reuse**: Created once per thread, refreshed periodically (60s Windows / 240s Unix) to avoid MDBX read transaction timeouts
- **Atomic counters**: `AtomicU64` with `Relaxed` ordering for gas/block/tx stats — no synchronization overhead
- **Platform-specific tuning**: Windows uses fewer threads and more frequent StateProvider refreshes due to mmap pressure

### Profiling (Cross-Platform)

- **Linux**: `pprof-rs` (signal-based sampling at 100Hz)
- **macOS**: Thread-based sampling with `backtrace` + `inferno` (avoids signal compatibility issues)
- **Windows**: Simplified thread-based sampling with `backtrace` + `inferno`

## Dependencies

All `reth-*` crates are pinned to a specific git revision (`ad476e2b5cb6cbc63fcb707d18a6bc6f3b0d6e38`). When updating Reth, all crates must be updated to the same revision simultaneously. The `revm` version is managed by Reth (not specified directly).

The project uses `snmalloc-rs` as the global allocator (via `reth_cli_util::allocator`).

## Cargo Profiles

- `dev`: opt-level 1, performance-critical deps at opt-level 3
- `release`: LTO thin, opt-level 3, stripped symbols
- `profiling`: Inherits release, keeps debug symbols (`debug = 2, strip = false`)
- `bench`: Inherits profiling
- `maxperf`: Fat LTO, single codegen unit (maximum performance)
