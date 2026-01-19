#!/bin/bash

# Performance test for --use-log mode with optimizations
# Test range: 9,000,000 - 9,100,000 (100,000 blocks)
# Step size: 3 (optimized for Windows)

echo "=== Performance Test for --use-log Mode ==="
echo "Block range: 9,000,000 - 9,100,000 (100,000 blocks)"
echo "Step size: 3 blocks per batch"
echo "Date: $(date)"
echo ""

# Test parameters
START_BLOCK=9000000
END_BLOCK=9100000
STEP=3
LOG_DIR="./test_bench_logs"

# Ensure log directory exists
if [ ! -d "$LOG_DIR" ]; then
    echo "Error: Log directory $LOG_DIR not found"
    echo "Please ensure logs are generated first with --log-block on"
    exit 1
fi

# Check if mmap log exists
if [ -f "$LOG_DIR/state_logs_mmap.bin" ]; then
    echo "Found mmap log file: state_logs_mmap.bin"
    MMAP_FLAG="--mmap-log"
else
    echo "Using file-based log mode (no mmap)"
    MMAP_FLAG=""
fi

echo ""
echo "Starting test..."
echo "Command: pevm evm --begin $START_BLOCK --end $END_BLOCK --step $STEP --use-log on --log-dir $LOG_DIR $MMAP_FLAG"
echo ""

# Run the test
time ./target/release/pevm evm \
    --begin $START_BLOCK \
    --end $END_BLOCK \
    --step $STEP \
    --use-log on \
    --log-dir "$LOG_DIR" \
    $MMAP_FLAG

echo ""
echo "=== Test Completed ==="
