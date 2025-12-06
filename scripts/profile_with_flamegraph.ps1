# 使用火焰图进行性能分析
# 注意：在 Windows 上，火焰图工具可能不可用或需要额外配置
# 
# 安装方式（选择其一）：
#   1. cargo install flamegraph (推荐，但 Windows 上可能有限制)
#   2. 使用 Windows Performance Toolkit (WPT) 替代
#   3. 使用 WSL (Windows Subsystem for Linux)
#
# 使用方法：
#   .\scripts\profile_with_flamegraph.ps1 -BeginBlock 1000 -EndBlock 2000 -DataDir "d:\reth2k" -UseLog $false
#   .\scripts\profile_with_flamegraph.ps1 -BeginBlock 1000 -EndBlock 2000 -DataDir "d:\reth2k" -UseLog $true -LogDir ".\bench_logs"

param(
    [Parameter(Mandatory=$true)]
    [int]$BeginBlock,
    
    [Parameter(Mandatory=$true)]
    [int]$EndBlock,
    
    [string]$DataDir = $env:PEVM_DATADIR,
    [string]$LogDir = $env:PEVM_LOG_DIR,
    [bool]$UseLog = $false,
    [string]$OutputFile = "flamegraph.svg"
)

if (-not $DataDir) {
    Write-Error "DataDir is required. Set PEVM_DATADIR environment variable or use -DataDir parameter"
    exit 1
}

Write-Host "========================================="
Write-Host "PEVM 火焰图性能分析"
Write-Host "========================================="
Write-Host "测试范围: 块 $BeginBlock 到 $EndBlock"
Write-Host "数据目录: $DataDir"
Write-Host "使用日志: $UseLog"
if ($UseLog) {
    if (-not $LogDir) {
        $LogDir = ".\bench_logs"
    }
    Write-Host "日志目录: $LogDir"
}
Write-Host "输出文件: $OutputFile"
Write-Host "========================================="
Write-Host ""

# 检查 flamegraph 是否安装
$flamegraphInstalled = cargo flamegraph --help 2>&1 | Select-String -Pattern "flamegraph|Usage"
if (-not $flamegraphInstalled) {
    Write-Host "安装 flamegraph..."
    Write-Host "注意：在 Windows 上，flamegraph 可能需要额外的依赖（如 perf 工具）"
    Write-Host "如果安装失败，请考虑使用 WSL 或 Windows Performance Toolkit"
    Write-Host ""
    
    cargo install flamegraph
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "安装 flamegraph 失败"
        Write-Warning "在 Windows 上，建议使用以下替代方案："
        Write-Warning "1. 使用 WSL (Windows Subsystem for Linux)"
        Write-Warning "2. 使用 Windows Performance Toolkit (WPT)"
        Write-Warning "3. 使用 PowerShell 基准测试脚本进行性能对比"
        Write-Host ""
        Write-Host "继续尝试运行，但可能会失败..."
    }
}

# 构建 release 版本（带调试符号）
Write-Host "构建 release 版本（带调试符号）..."
cargo build --release --profile profiling
if ($LASTEXITCODE -ne 0) {
    Write-Error "构建失败"
    exit 1
}

# 准备命令参数
$args = @(
    "evm",
    "-b", $BeginBlock,
    "-e", $EndBlock,
    "--datadir", $DataDir
)

if ($UseLog) {
    if (-not $LogDir) {
        Write-Error "使用日志时需要指定 LogDir"
        exit 1
    }
    $args += "--use-log", "on"
    $args += "--log-dir", $LogDir
}

# 生成火焰图
Write-Host "生成火焰图..."
Write-Host "命令: cargo flamegraph --bin pevm -- $($args -join ' ')"
cargo flamegraph --bin pevm -- $args -o $OutputFile

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "========================================="
    Write-Host "火焰图生成成功: $OutputFile"
    Write-Host "========================================="
    Write-Host ""
    Write-Host "在浏览器中打开火焰图查看性能热点："
    Write-Host "  start $OutputFile"
} else {
    Write-Error "生成火焰图失败"
    exit 1
}

