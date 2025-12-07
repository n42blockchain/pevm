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

# 检查是否在 Windows 上
$isWindows = $PSVersionTable.Platform -eq $null -or $PSVersionTable.Platform -eq "Win32NT"

if ($isWindows) {
    Write-Warning "========================================="
    Write-Warning "Windows 平台限制"
    Write-Warning "========================================="
    Write-Warning "cargo flamegraph 在 Windows 上无法正常工作"
    Write-Warning "因为它需要 Linux 的 perf 工具或 macOS 的 dtrace"
    Write-Warning ""
    Write-Warning "推荐使用以下替代方案："
    Write-Warning "1. Windows Performance Toolkit (WPT) - 推荐"
    Write-Warning "   运行: .\scripts\profile_with_wpt.ps1 -BeginBlock $BeginBlock -EndBlock $EndBlock -UseLog `$$UseLog"
    Write-Warning ""
    Write-Warning "2. PowerShell 基准测试脚本 - 最简单"
    Write-Warning "   运行: .\scripts\benchmark_performance.ps1 -BeginBlock $BeginBlock -EndBlock $EndBlock"
    Write-Warning ""
    Write-Warning "3. 使用 WSL (Windows Subsystem for Linux)"
    Write-Warning "   在 WSL 中安装并运行 flamegraph"
    Write-Warning "========================================="
    Write-Host ""
    
    $continue = Read-Host "是否继续尝试运行 flamegraph？(可能会失败) [y/N]"
    if ($continue -ne "y" -and $continue -ne "Y") {
        Write-Host "已取消。请使用上述替代方案。"
        exit 0
    }
    Write-Host ""
}

# 检查 flamegraph 是否安装
$flamegraphInstalled = cargo flamegraph --help 2>&1 | Select-String -Pattern "flamegraph|Usage"
if (-not $flamegraphInstalled) {
    Write-Host "安装 flamegraph..."
    Write-Host "注意：在 Windows 上，flamegraph 可能需要额外的依赖（如 perf 工具）"
    Write-Host "如果安装失败，请考虑使用 WSL 或 Windows Performance Toolkit"
    Write-Host ""
    
    cargo install flamegraph
    if ($LASTEXITCODE -ne 0) {
        Write-Error "安装 flamegraph 失败"
        Write-Host ""
        Write-Host "在 Windows 上，建议使用以下替代方案："
        Write-Host "1. 使用 WSL (Windows Subsystem for Linux)"
        Write-Host "2. 使用 Windows Performance Toolkit (WPT)"
        Write-Host "   运行: .\scripts\profile_with_wpt.ps1 -BeginBlock $BeginBlock -EndBlock $EndBlock -UseLog `$$UseLog"
        Write-Host "3. 使用 PowerShell 基准测试脚本进行性能对比"
        Write-Host "   运行: .\scripts\benchmark_performance.ps1 -BeginBlock $BeginBlock -EndBlock $EndBlock"
        exit 1
    }
}

# 构建 profiling 版本（带调试符号，用于性能分析）
Write-Host "构建 profiling 版本（带调试符号）..."
cargo build --profile profiling
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
# 注意：profiling profile 构建的可执行文件在 target/profiling/ 目录
Write-Host "生成火焰图..."
Write-Host "命令: cargo flamegraph --bin pevm --profile profiling -- $($args -join ' ')"
Write-Host "注意：在 Windows 上，这可能会失败（需要 Linux perf 或 macOS dtrace）"
Write-Host ""

# 尝试使用 profiling profile
$flamegraphOutput = cargo flamegraph --bin pevm --profile profiling -- $args -o $OutputFile 2>&1
$flamegraphExitCode = $LASTEXITCODE

# 检查输出中是否有 Windows 相关的错误
if ($flamegraphOutput -match "could not find dtrace|could not profile|NotAnAdmin|perf") {
    Write-Error "生成火焰图失败：flamegraph 在 Windows 上不可用"
    Write-Host ""
    Write-Host "错误信息："
    Write-Host $flamegraphOutput
    Write-Host ""
    Write-Host "========================================="
    Write-Host "推荐使用替代方案"
    Write-Host "========================================="
    Write-Host ""
    Write-Host "1. Windows Performance Toolkit (WPT) - 推荐"
    Write-Host "   .\scripts\profile_with_wpt.ps1 -BeginBlock $BeginBlock -EndBlock $EndBlock -UseLog `$$UseLog"
    Write-Host ""
    Write-Host "2. PowerShell 基准测试脚本 - 最简单"
    Write-Host "   .\scripts\benchmark_performance.ps1 -BeginBlock $BeginBlock -EndBlock $EndBlock"
    Write-Host ""
    Write-Host "3. 使用 WSL (Windows Subsystem for Linux)"
    Write-Host "   在 WSL 中运行："
    Write-Host "   cargo install flamegraph"
    Write-Host "   cargo flamegraph --bin pevm -- evm -b $BeginBlock -e $EndBlock --datadir /mnt/d/reth2k"
    Write-Host ""
    exit 1
}

if ($flamegraphExitCode -eq 0) {
    Write-Host ""
    Write-Host "========================================="
    Write-Host "火焰图生成成功: $OutputFile"
    Write-Host "========================================="
    Write-Host ""
    Write-Host "在浏览器中打开火焰图查看性能热点："
    Write-Host "  start $OutputFile"
} else {
    Write-Error "生成火焰图失败（退出码: $flamegraphExitCode）"
    Write-Host ""
    Write-Host "输出："
    Write-Host $flamegraphOutput
    Write-Host ""
    Write-Host "建议使用 Windows Performance Toolkit 或 PowerShell 基准测试脚本"
    exit 1
}

