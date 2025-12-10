#!/bin/bash
# PEVM 性能基准测试脚本 (macOS/Linux 版本)
# 对比使用日志 bin 和不使用日志的执行性能
#
# 使用方法：
#   ./scripts/benchmark_performance.sh -b 1000 -e 2000 -d "/path/to/reth2k" -l "./bench_logs"
#
# 或者使用环境变量：
#   export PEVM_DATADIR="/path/to/reth2k"
#   export PEVM_LOG_DIR="./bench_logs"
#   ./scripts/benchmark_performance.sh -b 1000 -e 2000

set -euo pipefail

# 默认参数
BEGIN_BLOCK=""
END_BLOCK=""
DATA_DIR="${PEVM_DATADIR:-}"
LOG_DIR="${PEVM_LOG_DIR:-}"
PEVM_EXE="${PEVM_EXE:-./target/release/pevm}"
WARMUP_RUNS="${WARMUP_RUNS:-1}"
BENCHMARK_RUNS="${BENCHMARK_RUNS:-3}"

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        -b|--begin)
            BEGIN_BLOCK="$2"
            shift 2
            ;;
        -e|--end)
            END_BLOCK="$2"
            shift 2
            ;;
        -d|--datadir)
            DATA_DIR="$2"
            shift 2
            ;;
        -l|--logdir)
            LOG_DIR="$2"
            shift 2
            ;;
        --pevm-exe)
            PEVM_EXE="$2"
            shift 2
            ;;
        --warmup-runs)
            WARMUP_RUNS="$2"
            shift 2
            ;;
        --benchmark-runs)
            BENCHMARK_RUNS="$2"
            shift 2
            ;;
        -h|--help)
            echo "用法: $0 [选项]"
            echo "选项:"
            echo "  -b, --begin BLOCK          开始块号 (必需)"
            echo "  -e, --end BLOCK            结束块号 (必需)"
            echo "  -d, --datadir DIR          数据目录 (或设置 PEVM_DATADIR 环境变量)"
            echo "  -l, --logdir DIR           日志目录 (或设置 PEVM_LOG_DIR 环境变量，默认: ./bench_logs)"
            echo "  --pevm-exe PATH            PEVM 可执行文件路径 (默认: ./target/release/pevm)"
            echo "  --warmup-runs N            预热运行次数 (默认: 1)"
            echo "  --benchmark-runs N         基准测试运行次数 (默认: 3)"
            echo "  -h, --help                 显示此帮助信息"
            exit 0
            ;;
        *)
            echo "错误: 未知参数: $1" >&2
            echo "使用 -h 或 --help 查看帮助信息"
            exit 1
            ;;
    esac
done

# 验证必需参数
if [[ -z "$BEGIN_BLOCK" ]] || [[ -z "$END_BLOCK" ]]; then
    echo "错误: 必须指定开始块号 (-b) 和结束块号 (-e)" >&2
    exit 1
fi

if [[ -z "$DATA_DIR" ]]; then
    echo "错误: 必须指定数据目录。设置 PEVM_DATADIR 环境变量或使用 -d 参数" >&2
    exit 1
fi

# 设置默认日志目录
if [[ -z "$LOG_DIR" ]]; then
    LOG_DIR="./bench_logs"
    echo "使用默认日志目录: $LOG_DIR"
fi

# 转换为绝对路径（如果还不是绝对路径）
if [[ "$LOG_DIR" != /* ]]; then
    LOG_DIR="$(cd "$(dirname "$LOG_DIR")" 2>/dev/null && pwd)/$(basename "$LOG_DIR")" || LOG_DIR="$PWD/$LOG_DIR"
fi
if [[ "$DATA_DIR" != /* ]]; then
    DATA_DIR="$(cd "$(dirname "$DATA_DIR")" 2>/dev/null && pwd)/$(basename "$DATA_DIR")" || DATA_DIR="$PWD/$DATA_DIR"
fi

# 检查可执行文件是否存在
if [[ ! -f "$PEVM_EXE" ]]; then
    # 尝试相对路径
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
    if [[ -f "$SCRIPT_DIR/$PEVM_EXE" ]]; then
        PEVM_EXE="$SCRIPT_DIR/$PEVM_EXE"
    else
        echo "错误: PEVM 可执行文件不存在: $PEVM_EXE" >&2
        echo "请先构建: cargo build --release" >&2
        exit 1
    fi
fi

# 确保可执行文件有执行权限
chmod +x "$PEVM_EXE" 2>/dev/null || true

# 检查 bc 命令是否可用
if ! command -v bc &> /dev/null; then
    echo "错误: 需要安装 bc 计算器" >&2
    echo "在 macOS 上可以使用: brew install bc" >&2
    exit 1
fi

echo "========================================="
echo "PEVM 性能基准测试"
echo "========================================="
echo "测试范围: 块 $BEGIN_BLOCK 到 $END_BLOCK"
echo "数据目录: $DATA_DIR"
echo "日志目录: $LOG_DIR"
echo "可执行文件: $PEVM_EXE"
echo "预热运行: $WARMUP_RUNS 次"
echo "基准测试: $BENCHMARK_RUNS 次"
echo "========================================="
echo ""

# 确保日志目录存在
mkdir -p "$LOG_DIR"

# 步骤 1: 生成日志文件（如果不存在）
echo "[1/3] 检查并生成日志文件..."
LOG_INDEX_FILE="$LOG_DIR/blocks_log.idx"
if [[ ! -f "$LOG_INDEX_FILE" ]]; then
    echo "  生成日志文件..."
    "$PEVM_EXE" evm -b "$BEGIN_BLOCK" -e "$END_BLOCK" --log-block on --log-dir "$LOG_DIR" --compression zstd --datadir "$DATA_DIR"
    if [[ $? -ne 0 ]]; then
        echo "错误: 生成日志文件失败" >&2
        exit 1
    fi
    echo "  日志文件生成完成"
else
    echo "  日志文件已存在，跳过生成"
fi
echo ""

# 步骤 2: 预热运行
echo "[2/3] 预热运行..."
for ((i=1; i<=WARMUP_RUNS; i++)); do
    echo "  预热运行 $i/$WARMUP_RUNS..."
    "$PEVM_EXE" evm -b "$BEGIN_BLOCK" -e "$END_BLOCK" --datadir "$DATA_DIR" >/dev/null 2>&1
    "$PEVM_EXE" evm -b "$BEGIN_BLOCK" -e "$END_BLOCK" --use-log on --log-dir "$LOG_DIR" --datadir "$DATA_DIR" >/dev/null 2>&1
done
echo "  预热完成"
echo ""

# 步骤 3: 性能基准测试
echo "[3/3] 性能基准测试..."
echo ""

# 存储结果的数组
declare -a WITHOUT_LOG_RESULTS
declare -a WITH_LOG_BIN_RESULTS

# 测试不使用日志
echo "测试 1: 不使用日志执行"
for ((i=1; i<=BENCHMARK_RUNS; i++)); do
    echo "  运行 $i/$BENCHMARK_RUNS..."
    START_TIME=$(date +%s.%N)
    OUTPUT=$("$PEVM_EXE" evm -b "$BEGIN_BLOCK" -e "$END_BLOCK" --datadir "$DATA_DIR" 2>&1)
    EXIT_CODE=$?
    END_TIME=$(date +%s.%N)
    DURATION=$(echo "$END_TIME - $START_TIME" | bc)
    
    if [[ $EXIT_CODE -ne 0 ]]; then
        echo "错误: 执行失败 (退出码: $EXIT_CODE)" >&2
        echo "$OUTPUT" >&2
        exit 1
    fi
    
    WITHOUT_LOG_RESULTS+=("$DURATION")
    
    # 提取性能指标
    GAS_LINE=$(echo "$OUTPUT" | grep -i "Ggas_per_s" || true)
    if [[ -n "$GAS_LINE" ]]; then
        printf "    耗时: %.2fs, %s\n" "$DURATION" "$GAS_LINE"
    else
        printf "    耗时: %.2fs\n" "$DURATION"
    fi
done
echo ""

# 测试使用日志 bin
echo "测试 2: 使用日志 bin 执行"
for ((i=1; i<=BENCHMARK_RUNS; i++)); do
    echo "  运行 $i/$BENCHMARK_RUNS..."
    START_TIME=$(date +%s.%N)
    OUTPUT=$("$PEVM_EXE" evm -b "$BEGIN_BLOCK" -e "$END_BLOCK" --use-log on --log-dir "$LOG_DIR" --datadir "$DATA_DIR" 2>&1)
    EXIT_CODE=$?
    END_TIME=$(date +%s.%N)
    DURATION=$(echo "$END_TIME - $START_TIME" | bc)
    
    if [[ $EXIT_CODE -ne 0 ]]; then
        echo "错误: 执行失败 (退出码: $EXIT_CODE)" >&2
        echo "$OUTPUT" >&2
        exit 1
    fi
    
    WITH_LOG_BIN_RESULTS+=("$DURATION")
    
    # 提取性能指标
    GAS_LINE=$(echo "$OUTPUT" | grep -i "Ggas_per_s" || true)
    if [[ -n "$GAS_LINE" ]]; then
        printf "    耗时: %.2fs, %s\n" "$DURATION" "$GAS_LINE"
    else
        printf "    耗时: %.2fs\n" "$DURATION"
    fi
done
echo ""

# 计算统计信息的函数
calculate_stats() {
    local values=("$@")
    local count=${#values[@]}
    
    if [[ $count -eq 0 ]]; then
        echo "0 0 0 0"
        return
    fi
    
    # 计算平均值
    local sum=0
    for val in "${values[@]}"; do
        sum=$(echo "$sum + $val" | bc)
    done
    local avg=$(echo "scale=10; $sum / $count" | bc)
    
    # 计算最小值和最大值
    local min=${values[0]}
    local max=${values[0]}
    for val in "${values[@]}"; do
        min=$(echo "if ($val < $min) $val else $min" | bc)
        max=$(echo "if ($val > $max) $val else $max" | bc)
    done
    
    # 计算标准差
    local variance_sum=0
    for val in "${values[@]}"; do
        local diff=$(echo "$val - $avg" | bc)
        local squared=$(echo "$diff * $diff" | bc)
        variance_sum=$(echo "$variance_sum + $squared" | bc)
    done
    local variance=$(echo "scale=10; $variance_sum / $count" | bc)
    local stddev=$(echo "scale=10; sqrt($variance)" | bc)
    
    printf "%.10f %.10f %.10f %.10f" "$avg" "$min" "$max" "$stddev"
}

# 计算统计信息
WITHOUT_LOG_STATS=($(calculate_stats "${WITHOUT_LOG_RESULTS[@]}"))
WITH_LOG_BIN_STATS=($(calculate_stats "${WITH_LOG_BIN_RESULTS[@]}"))

WITHOUT_LOG_AVG=${WITHOUT_LOG_STATS[0]}
WITHOUT_LOG_MIN=${WITHOUT_LOG_STATS[1]}
WITHOUT_LOG_MAX=${WITHOUT_LOG_STATS[2]}
WITHOUT_LOG_STDDEV=${WITHOUT_LOG_STATS[3]}

WITH_LOG_BIN_AVG=${WITH_LOG_BIN_STATS[0]}
WITH_LOG_BIN_MIN=${WITH_LOG_BIN_STATS[1]}
WITH_LOG_BIN_MAX=${WITH_LOG_BIN_STATS[2]}
WITH_LOG_BIN_STDDEV=${WITH_LOG_BIN_STATS[3]}

# 输出结果
echo "========================================="
echo "性能测试结果"
echo "========================================="
echo ""
echo "不使用日志:"
printf "  平均耗时: %.2fs\n" "$WITHOUT_LOG_AVG"
printf "  最小耗时: %.2fs\n" "$WITHOUT_LOG_MIN"
printf "  最大耗时: %.2fs\n" "$WITHOUT_LOG_MAX"
printf "  标准差:   %.2fs\n" "$WITHOUT_LOG_STDDEV"
echo ""
echo "使用日志 bin:"
printf "  平均耗时: %.2fs\n" "$WITH_LOG_BIN_AVG"
printf "  最小耗时: %.2fs\n" "$WITH_LOG_BIN_MIN"
printf "  最大耗时: %.2fs\n" "$WITH_LOG_BIN_MAX"
printf "  标准差:   %.2fs\n" "$WITH_LOG_BIN_STDDEV"
echo ""

# 计算性能差异
SPEEDUP=$(echo "scale=10; $WITHOUT_LOG_AVG / $WITH_LOG_BIN_AVG" | bc)
SLOWDOWN=$(echo "scale=10; $WITH_LOG_BIN_AVG / $WITHOUT_LOG_AVG" | bc)

echo "性能对比:"
SPEEDUP_ROUNDED=$(printf "%.2f" "$SPEEDUP")
if (( $(echo "$SPEEDUP > 1" | bc -l) )); then
    PERCENTAGE=$(echo "scale=1; ($SPEEDUP - 1) * 100" | bc)
    printf "  使用日志 bin 快 %sx (提升 %s%%)\n" "$SPEEDUP_ROUNDED" "$PERCENTAGE"
else
    SLOWDOWN_ROUNDED=$(printf "%.2f" "$SLOWDOWN")
    PERCENTAGE=$(echo "scale=1; ($SLOWDOWN - 1) * 100" | bc)
    printf "  使用日志 bin 慢 %sx (下降 %s%%)\n" "$SLOWDOWN_ROUNDED" "$PERCENTAGE"
fi
echo ""

# 计算吞吐量（块/秒）
BLOCK_COUNT=$((END_BLOCK - BEGIN_BLOCK + 1))
WITHOUT_LOG_THROUGHPUT=$(echo "scale=2; $BLOCK_COUNT / $WITHOUT_LOG_AVG" | bc)
WITH_LOG_BIN_THROUGHPUT=$(echo "scale=2; $BLOCK_COUNT / $WITH_LOG_BIN_AVG" | bc)

echo "吞吐量对比:"
printf "  不使用日志: %.2f 块/秒\n" "$WITHOUT_LOG_THROUGHPUT"
printf "  使用日志 bin: %.2f 块/秒\n" "$WITH_LOG_BIN_THROUGHPUT"
echo ""

echo "========================================="
echo "测试完成"
echo "========================================="

