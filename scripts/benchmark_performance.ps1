# PEVM 性能基准测试脚本
# 对比使用日志 bin 和不使用日志的执行性能
#
# 使用方法：
#   .\scripts\benchmark_performance.ps1 -BeginBlock 1000 -EndBlock 2000 -DataDir "d:\reth2k" -LogDir ".\bench_logs"
#
# 或者使用环境变量：
#   $env:PEVM_DATADIR="d:\reth2k"
#   $env:PEVM_LOG_DIR=".\bench_logs"
#   .\scripts\benchmark_performance.ps1 -BeginBlock 1000 -EndBlock 2000

param(
    [Parameter(Mandatory=$true)]
    [int]$BeginBlock,
    
    [Parameter(Mandatory=$true)]
    [int]$EndBlock,
    
    [string]$DataDir = $env:PEVM_DATADIR,
    [string]$LogDir = $env:PEVM_LOG_DIR,
    [string]$PevmExe = ".\target\release\pevm.exe",
    [int]$WarmupRuns = 1,
    [int]$BenchmarkRuns = 3
)

if (-not $DataDir) {
    Write-Error "DataDir is required. Set PEVM_DATADIR environment variable or use -DataDir parameter"
    exit 1
}

if (-not $LogDir) {
    $LogDir = ".\bench_logs"
    Write-Host "Using default log directory: $LogDir"
}

$LogDir = [System.IO.Path]::GetFullPath($LogDir)

Write-Host "========================================="
Write-Host "PEVM 性能基准测试"
Write-Host "========================================="
Write-Host "测试范围: 块 $BeginBlock 到 $EndBlock"
Write-Host "数据目录: $DataDir"
Write-Host "日志目录: $LogDir"
Write-Host "预热运行: $WarmupRuns 次"
Write-Host "基准测试: $BenchmarkRuns 次"
Write-Host "========================================="
Write-Host ""

# 确保日志目录存在
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

# 步骤 1: 生成日志文件（如果不存在）
Write-Host "[1/3] 检查并生成日志文件..."
$logIndexFile = Join-Path $LogDir "blocks_log.idx"
if (-not (Test-Path $logIndexFile)) {
    Write-Host "  生成日志文件..."
    & $PevmExe evm -b $BeginBlock -e $EndBlock --log-block on --log-dir $LogDir --compression zstd --datadir $DataDir
    if ($LASTEXITCODE -ne 0) {
        Write-Error "生成日志文件失败"
        exit 1
    }
    Write-Host "  日志文件生成完成"
} else {
    Write-Host "  日志文件已存在，跳过生成"
}
Write-Host ""

# 步骤 2: 预热运行
Write-Host "[2/3] 预热运行..."
for ($i = 1; $i -le $WarmupRuns; $i++) {
    Write-Host "  预热运行 $i/$WarmupRuns..."
    & $PevmExe evm -b $BeginBlock -e $EndBlock --datadir $DataDir 2>&1 | Out-Null
    & $PevmExe evm -b $BeginBlock -e $EndBlock --use-log on --log-dir $LogDir --datadir $DataDir 2>&1 | Out-Null
}
Write-Host "  预热完成"
Write-Host ""

# 步骤 3: 性能基准测试
Write-Host "[3/3] 性能基准测试..."
Write-Host ""

$results = @{
    WithoutLog = @()
    WithLogBin = @()
}

# 测试不使用日志
Write-Host "测试 1: 不使用日志执行"
for ($i = 1; $i -le $BenchmarkRuns; $i++) {
    Write-Host "  运行 $i/$BenchmarkRuns..."
    $startTime = Get-Date
    $output = & $PevmExe evm -b $BeginBlock -e $EndBlock --datadir $DataDir 2>&1
    $endTime = Get-Date
    $duration = ($endTime - $startTime).TotalSeconds
    $results.WithoutLog += $duration
    
    # 提取性能指标
    $gasLine = $output | Select-String -Pattern "Ggas_per_s"
    if ($gasLine) {
        Write-Host "    耗时: $([math]::Round($duration, 2))s, $gasLine"
    } else {
        Write-Host "    耗时: $([math]::Round($duration, 2))s"
    }
}
Write-Host ""

# 测试使用日志 bin
Write-Host "测试 2: 使用日志 bin 执行"
for ($i = 1; $i -le $BenchmarkRuns; $i++) {
    Write-Host "  运行 $i/$BenchmarkRuns..."
    $startTime = Get-Date
    $output = & $PevmExe evm -b $BeginBlock -e $EndBlock --use-log on --log-dir $LogDir --datadir $DataDir 2>&1
    $endTime = Get-Date
    $duration = ($endTime - $startTime).TotalSeconds
    $results.WithLogBin += $duration
    
    # 提取性能指标
    $gasLine = $output | Select-String -Pattern "Ggas_per_s"
    if ($gasLine) {
        Write-Host "    耗时: $([math]::Round($duration, 2))s, $gasLine"
    } else {
        Write-Host "    耗时: $([math]::Round($duration, 2))s"
    }
}
Write-Host ""

# 计算统计信息
function Get-Stats($values) {
    $avg = ($values | Measure-Object -Average).Average
    $min = ($values | Measure-Object -Minimum).Minimum
    $max = ($values | Measure-Object -Maximum).Maximum
    $stdDev = [math]::Sqrt((($values | ForEach-Object { [math]::Pow($_ - $avg, 2) }) | Measure-Object -Average).Average)
    return @{
        Average = $avg
        Min = $min
        Max = $max
        StdDev = $stdDev
    }
}

$withoutLogStats = Get-Stats $results.WithoutLog
$withLogBinStats = Get-Stats $results.WithLogBin

# 输出结果
Write-Host "========================================="
Write-Host "性能测试结果"
Write-Host "========================================="
Write-Host ""
Write-Host "不使用日志:"
Write-Host "  平均耗时: $([math]::Round($withoutLogStats.Average, 2))s"
Write-Host "  最小耗时: $([math]::Round($withoutLogStats.Min, 2))s"
Write-Host "  最大耗时: $([math]::Round($withoutLogStats.Max, 2))s"
Write-Host "  标准差:   $([math]::Round($withoutLogStats.StdDev, 2))s"
Write-Host ""
Write-Host "使用日志 bin:"
Write-Host "  平均耗时: $([math]::Round($withLogBinStats.Average, 2))s"
Write-Host "  最小耗时: $([math]::Round($withLogBinStats.Min, 2))s"
Write-Host "  最大耗时: $([math]::Round($withLogBinStats.Max, 2))s"
Write-Host "  标准差:   $([math]::Round($withLogBinStats.StdDev, 2))s"
Write-Host ""

# 计算性能差异
$speedup = $withoutLogStats.Average / $withLogBinStats.Average
$slowdown = $withLogBinStats.Average / $withoutLogStats.Average

Write-Host "性能对比:"
if ($speedup -gt 1) {
    Write-Host "  使用日志 bin 快 $([math]::Round($speedup, 2))x (提升 $([math]::Round(($speedup - 1) * 100, 1))%)"
} else {
    Write-Host "  使用日志 bin 慢 $([math]::Round($slowdown, 2))x (下降 $([math]::Round(($slowdown - 1) * 100, 1))%)"
}
Write-Host ""

# 计算吞吐量（块/秒）
$blockCount = $EndBlock - $BeginBlock + 1
$withoutLogThroughput = $blockCount / $withoutLogStats.Average
$withLogBinThroughput = $blockCount / $withLogBinStats.Average

Write-Host "吞吐量对比:"
Write-Host "  不使用日志: $([math]::Round($withoutLogThroughput, 2)) 块/秒"
Write-Host "  使用日志 bin: $([math]::Round($withLogBinThroughput, 2)) 块/秒"
Write-Host ""

Write-Host "========================================="
Write-Host "测试完成"
Write-Host "========================================="

