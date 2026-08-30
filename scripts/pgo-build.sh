#!/bin/bash
# Profile-guided build of pevm for witness replay.
#
# Builds an instrumented binary, replays short ranges from four eras of the
# chain to collect profiles, merges them and rebuilds with them. Measured on
# 12.0M–12.2M at 256 threads: 2.7–3.7% less CPU than the plain release
# build; 1% on one thread. rustc's LLVM and llvm-profdata must be the same
# major version (`rustc -vV`; LLVM_PROFDATA points at the tool).
#
#   scripts/pgo-build.sh --witness-dir ... --geth-ancient-dir ... (the replay
#   inputs, passed through to `pevm evm`)
#
# CARGO_FEATURES adds cargo features to both builds (e.g. CARGO_FEATURES=gmp).
# The result is target-pgo/release/pevm.
set -euo pipefail
cd "$(dirname "$0")/.."
PROFDATA=${LLVM_PROFDATA:-llvm-profdata}
FEATURES=(); [ -n "${CARGO_FEATURES:-}" ] && FEATURES=(--features "$CARGO_FEATURES")
DATA=${PGO_DATA:-target-pgo/pgo-data}
INPUTS=("$@")
if [ ${#INPUTS[@]} -eq 0 ]; then
    echo "usage: $0 <pevm evm input flags>" >&2
    exit 2
fi

rm -rf "$DATA"; mkdir -p "$DATA"
echo "== instrumented build"
RUSTFLAGS="-Cprofile-generate=$(realpath "$DATA")" cargo build --release --target-dir target-pgo "${FEATURES[@]}"
echo "== training"
for range in "4300000 4310000" "12000000 12010000" "16000000 16008000" "20000000 20006000" "25000000 25004000"; do
    set -- $range
    ./target-pgo/release/pevm evm --chain mainnet "${INPUTS[@]}" -b "$1" -e "$2" -t 64 -s 64 >/dev/null
done
echo "== merge"
"$PROFDATA" merge -o target-pgo/merged.profdata "$DATA"
echo "== optimized build"
RUSTFLAGS="-Cprofile-use=$(realpath target-pgo/merged.profdata)" cargo build --release --target-dir target-pgo "${FEATURES[@]}"
echo "built target-pgo/release/pevm"
