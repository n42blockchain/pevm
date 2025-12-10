#!/bin/bash
# 使用火焰图进行性能分析 (macOS/Linux 版本)
# 
# 安装方式：
#   cargo install flamegraph
#
# 使用方法：
#   ./scripts/profile_with_flamegraph.sh -b 1000 -e 2000 -d "/path/to/reth2k" -u false
#   ./scripts/profile_with_flamegraph.sh -b 1000 -e 2000 -d "/path/to/reth2k" -u true -l "./bench_logs"
#
# 或者使用环境变量：
#   export PEVM_DATADIR="/path/to/reth2k"
#   export PEVM_LOG_DIR="./bench_logs"
#   ./scripts/profile_with_flamegraph.sh -b 1000 -e 2000 -u true

set -euo pipefail

# 默认参数
BEGIN_BLOCK=""
END_BLOCK=""
DATA_DIR="${PEVM_DATADIR:-}"
LOG_DIR="${PEVM_LOG_DIR:-}"
USE_LOG="${USE_LOG:-false}"
OUTPUT_FILE="${OUTPUT_FILE:-flamegraph.svg}"
PEVM_EXE="${PEVM_EXE:-./target/profiling/pevm}"
SINGLE_THREAD="${SINGLE_THREAD:-false}"

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
        -u|--use-log)
            USE_LOG="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        --single-thread|--single)
            SINGLE_THREAD="true"
            shift
            ;;
        --pevm-exe)
            PEVM_EXE="$2"
            shift 2
            ;;
        -h|--help)
            echo "用法: $0 [选项]"
            echo "选项:"
            echo "  -b, --begin BLOCK      开始块号 (必需)"
            echo "  -e, --end BLOCK        结束块号 (必需)"
            echo "  -d, --datadir DIR      数据目录 (或设置 PEVM_DATADIR 环境变量)"
            echo "  -l, --logdir DIR       日志目录 (或设置 PEVM_LOG_DIR 环境变量)"
            echo "  -u, --use-log BOOL     是否使用日志 (true/false，默认: false)"
            echo "  -o, --output FILE      输出文件路径 (默认: flamegraph.svg)"
            echo "  --single-thread        使用单线程模式（便于分析单线程瓶颈）"
            echo "  --pevm-exe PATH        PEVM 可执行文件路径 (默认: ./target/profiling/pevm)"
            echo "  -h, --help             显示此帮助信息"
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
if [[ "$USE_LOG" == "true" ]] && [[ -z "$LOG_DIR" ]]; then
    LOG_DIR="./bench_logs"
    echo "使用默认日志目录: $LOG_DIR"
fi

# 转换为绝对路径
if [[ "$LOG_DIR" != /* ]] && [[ -n "$LOG_DIR" ]]; then
    LOG_DIR="$(cd "$(dirname "$LOG_DIR")" 2>/dev/null && pwd)/$(basename "$LOG_DIR")" || LOG_DIR="$PWD/$LOG_DIR"
fi
if [[ "$DATA_DIR" != /* ]]; then
    DATA_DIR="$(cd "$(dirname "$DATA_DIR")" 2>/dev/null && pwd)/$(basename "$DATA_DIR")" || DATA_DIR="$PWD/$DATA_DIR"
fi

echo "========================================="
echo "PEVM 火焰图性能分析"
echo "========================================="
echo "测试范围: 块 $BEGIN_BLOCK 到 $END_BLOCK"
echo "数据目录: $DATA_DIR"
echo "使用日志: $USE_LOG"
if [[ "$USE_LOG" == "true" ]]; then
    echo "日志目录: $LOG_DIR"
fi
echo "输出文件: $OUTPUT_FILE"
echo "========================================="
echo ""

# 检查 flamegraph 是否安装
if ! command -v flamegraph &> /dev/null && ! cargo flamegraph --help &> /dev/null 2>&1; then
    echo "flamegraph 未安装，正在安装..."
    echo ""
    
    if ! cargo install flamegraph 2>&1; then
        echo ""
        echo "错误: 安装 flamegraph 失败" >&2
        echo ""
        echo "请手动安装："
        echo "  cargo install flamegraph"
        echo ""
        echo "如果安装过程中遇到问题，请检查："
        echo "  1. Rust 工具链是否已安装: rustc --version"
        echo "  2. Cargo 是否可用: cargo --version"
        exit 1
    fi
    echo ""
fi

# 检查操作系统
OS="$(uname -s)"
case "$OS" in
    Darwin)
        echo "检测到 macOS，将使用 dtrace 进行性能分析"
        ;;
    Linux)
        echo "检测到 Linux，将使用 perf 进行性能分析"
        ;;
    *)
        echo "警告: 未识别的操作系统: $OS"
        echo "火焰图可能无法正常工作"
        ;;
esac
echo ""

# 构建 profiling 版本（带调试符号，用于性能分析）
echo "构建 profiling 版本（带调试符号）..."
if ! cargo build --profile profiling; then
    echo "错误: 构建失败" >&2
    exit 1
fi

# 检查可执行文件是否存在
if [[ ! -f "$PEVM_EXE" ]]; then
    # 尝试相对路径
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
    if [[ -f "$SCRIPT_DIR/$PEVM_EXE" ]]; then
        PEVM_EXE="$SCRIPT_DIR/$PEVM_EXE"
    else
        echo "错误: PEVM 可执行文件不存在: $PEVM_EXE" >&2
        echo "请先构建: cargo build --profile profiling" >&2
        exit 1
    fi
fi

# 确保可执行文件有执行权限
chmod +x "$PEVM_EXE" 2>/dev/null || true

# 准备命令参数
CMD_ARGS=(
    "evm"
    "-b" "$BEGIN_BLOCK"
    "-e" "$END_BLOCK"
    "--datadir" "$DATA_DIR"
)

if [[ "$USE_LOG" == "true" ]]; then
    if [[ -z "$LOG_DIR" ]]; then
        echo "错误: 使用日志时需要指定 LogDir" >&2
        exit 1
    fi
    CMD_ARGS+=("--use-log" "on")
    CMD_ARGS+=("--log-dir" "$LOG_DIR")
fi

# 添加单线程模式（如果启用）
if [[ "$SINGLE_THREAD" == "true" ]]; then
    CMD_ARGS+=("--single-thread")
    echo "使用单线程模式（便于分析单线程性能瓶颈）"
fi

# 生成火焰图
echo "生成火焰图..."
echo "命令: cargo flamegraph --bin pevm --profile profiling -- ${CMD_ARGS[*]} -o $OUTPUT_FILE"
echo ""

# 使用绝对路径的输出文件
if [[ "$OUTPUT_FILE" != /* ]]; then
    OUTPUT_FILE="$(pwd)/$OUTPUT_FILE"
fi

# 运行 flamegraph
# 暂时禁用严格模式以便捕获退出码
set +e  # 禁用 exit on error
set +u  # 禁用未定义变量检查

cargo flamegraph --bin pevm --profile profiling -- "${CMD_ARGS[@]}" -o "$OUTPUT_FILE" 2>&1
FLAMEGRAPH_EXIT_CODE=$?

# 确保变量有值（在重新启用严格模式之前）
FLAMEGRAPH_EXIT_CODE=${FLAMEGRAPH_EXIT_CODE:-1}

# 重新启用严格模式
set -e  # 重新启用 exit on error  
set -u  # 重新启用未定义变量检查

if [[ $FLAMEGRAPH_EXIT_CODE -eq 0 ]]; then
    echo ""
    echo "========================================="
    echo "火焰图生成成功: $OUTPUT_FILE"
    echo "========================================="
    echo ""
    
    # 尝试在浏览器中打开（macOS）
    if command -v open &> /dev/null; then
        echo "在浏览器中打开火焰图..."
        open "$OUTPUT_FILE"
    elif command -v xdg-open &> /dev/null; then
        echo "在浏览器中打开火焰图..."
        xdg-open "$OUTPUT_FILE"
    else
        echo "请手动在浏览器中打开火焰图文件："
        echo "  $OUTPUT_FILE"
    fi
    echo ""
else
    echo ""
    echo "错误: 生成火焰图失败（退出码: ${FLAMEGRAPH_EXIT_CODE:-1}）" >&2
    echo ""
    
    # 检查常见的错误原因并提供解决方案
    case "$OS" in
        Darwin)
            echo "在 macOS 上，检测到 dtrace 权限问题。"
            echo ""
            echo "========================================="
            echo "推荐解决方案：使用项目内置的性能分析功能"
            echo "========================================="
            echo ""
            echo "项目内置的性能分析功能使用 pprof-rs，不需要 dtrace 权限。"
            echo "运行以下命令："
            echo ""
            if [[ "$USE_LOG" == "true" ]] && [[ -n "$LOG_DIR" ]]; then
                echo "  ./target/profiling/pevm evm -b $BEGIN_BLOCK -e $END_BLOCK \\"
                echo "    --datadir \"$DATA_DIR\" \\"
                echo "    --use-log on --log-dir \"$LOG_DIR\" \\"
                echo "    --enable-profiling"
                echo ""
                echo "或者（如果使用 release 版本）："
                echo "  ./target/release/pevm evm -b $BEGIN_BLOCK -e $END_BLOCK \\"
                echo "    --datadir \"$DATA_DIR\" \\"
                echo "    --use-log on --log-dir \"$LOG_DIR\" \\"
                echo "    --enable-profiling"
            else
                echo "  ./target/profiling/pevm evm -b $BEGIN_BLOCK -e $END_BLOCK \\"
                echo "    --datadir \"$DATA_DIR\" \\"
                echo "    --enable-profiling"
                echo ""
                echo "或者（如果使用 release 版本）："
                echo "  ./target/release/pevm evm -b $BEGIN_BLOCK -e $END_BLOCK \\"
                echo "    --datadir \"$DATA_DIR\" \\"
                echo "    --enable-profiling"
            fi
            echo ""
            echo "执行完成后，会在当前目录生成 flamegraph.svg 文件。"
            echo ""
            echo "========================================="
            echo "其他选项（不推荐）"
            echo "========================================="
            echo ""
            echo "选项 1: 使用 sudo 运行（需要管理员权限）"
            echo "   sudo $0 -b $BEGIN_BLOCK -e $END_BLOCK -d \"$DATA_DIR\" -u $USE_LOG ${LOG_DIR:+-l \"$LOG_DIR\"} -o \"$OUTPUT_FILE\""
            echo ""
            echo "选项 2: 禁用系统完整性保护 (SIP) - 不推荐"
            echo "   这需要重启并修改系统设置，可能影响系统安全性"
            echo ""
            echo "选项 3: 在 Linux 系统或 Docker 容器中运行"
            ;;
        Linux)
            echo "在 Linux 上，可能的原因和解决方案："
            echo ""
            echo "1. perf 工具未安装："
            echo "   Ubuntu/Debian: sudo apt-get install linux-perf"
            echo "   CentOS/RHEL: sudo yum install perf"
            echo ""
            echo "2. perf 权限问题："
            echo "   sudo sysctl -w kernel.perf_event_paranoid=-1"
            echo "   或者运行: sudo $0 -b $BEGIN_BLOCK -e $END_BLOCK -d \"$DATA_DIR\" -u $USE_LOG ${LOG_DIR:+-l \"$LOG_DIR\"}"
            echo ""
            echo "3. 查看详细错误信息："
            echo "   RUST_BACKTRACE=1 cargo flamegraph --bin pevm --profile profiling -- ${CMD_ARGS[*]} -o $OUTPUT_FILE"
            ;;
        *)
            echo "查看详细错误信息："
            echo "   RUST_BACKTRACE=1 cargo flamegraph --bin pevm --profile profiling -- ${CMD_ARGS[*]} -o $OUTPUT_FILE"
            ;;
    esac
    echo ""
    EXIT_CODE=${FLAMEGRAPH_EXIT_CODE:-1}
    exit $EXIT_CODE
fi

