# 使用 Windows Performance Toolkit (WPT) 进行性能分析
# WPT 是 Windows 自带的性能分析工具，不需要额外安装
#
# 使用方法：
#   .\scripts\profile_with_wpt.ps1 -BeginBlock 1000 -EndBlock 2000 -DataDir "d:\reth2k" -UseLog $false
#   .\scripts\profile_with_wpt.ps1 -BeginBlock 1000 -EndBlock 2000 -DataDir "d:\reth2k" -UseLog $true -LogDir ".\bench_logs"

param(
    [Parameter(Mandatory=$true)]
    [int]$BeginBlock,
    
    [Parameter(Mandatory=$true)]
    [int]$EndBlock,
    
    [string]$DataDir = $env:PEVM_DATADIR,
    [string]$LogDir = $env:PEVM_LOG_DIR,
    [bool]$UseLog = $false,
    [string]$OutputFile = "pevm_trace.etl"
)

if (-not $DataDir) {
    Write-Error "DataDir is required. Set PEVM_DATADIR environment variable or use -DataDir parameter"
    exit 1
}

Write-Host "========================================="
Write-Host "PEVM Windows Performance Toolkit 分析"
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

# 检查 wpr 是否可用
$wprAvailable = Get-Command wpr -ErrorAction SilentlyContinue
if (-not $wprAvailable) {
    Write-Error "Windows Performance Recorder (wpr) 不可用"
    Write-Error "请确保已安装 Windows Performance Toolkit (WPT)"
    Write-Error "下载地址: https://www.microsoft.com/en-us/download/details.aspx?id=39982"
    exit 1
}

# 准备命令参数
$pevmArgs = @(
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
    $pevmArgs += "--use-log", "on"
    $pevmArgs += "--log-dir", $LogDir
}

$pevmExe = ".\target\release\pevm.exe"
if (-not (Test-Path $pevmExe)) {
    Write-Error "PEVM 可执行文件不存在: $pevmExe"
    Write-Error "请先构建: cargo build --release"
    exit 1
}

Write-Host "开始性能记录..."
Write-Host "命令: $pevmExe $($pevmArgs -join ' ')"
Write-Host ""

# 启动性能记录
Write-Host "[1/3] 启动性能记录器..."
wpr -start GeneralProfile -filemode
if ($LASTEXITCODE -ne 0) {
    Write-Error "启动性能记录失败（可能需要管理员权限）"
    exit 1
}

# 运行测试
Write-Host "[2/3] 运行测试..."
& $pevmExe $pevmArgs
$testExitCode = $LASTEXITCODE

# 停止记录
Write-Host "[3/3] 停止性能记录并保存..."
wpr -stop $OutputFile
if ($LASTEXITCODE -ne 0) {
    Write-Error "停止性能记录失败"
    exit 1
}

if ($testExitCode -ne 0) {
    Write-Warning "测试执行失败（退出码: $testExitCode），但性能记录已保存"
}

if (Test-Path $OutputFile) {
    Write-Host ""
    Write-Host "========================================="
    Write-Host "性能记录已保存: $OutputFile"
    Write-Host "========================================="
    Write-Host ""
    Write-Host "使用 Windows Performance Analyzer (WPA) 打开文件进行分析："
    Write-Host "  1. 打开 Windows Performance Analyzer (wpa.exe)"
    Write-Host "  2. File -> Open -> 选择 $OutputFile"
    Write-Host ""
    Write-Host "或者使用命令行打开："
    Write-Host "  wpa.exe $OutputFile"
} else {
    Write-Error "性能记录文件未生成"
    exit 1
}

