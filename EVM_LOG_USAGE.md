# EVM 日志功能使用说明

## 概述

本工具支持两种日志存储模式：
1. **单个文件模式**：每个块生成一个独立的日志文件（`block_XXXXX_reads.bin`）
2. **累加文件模式**：所有块的日志累加写入单个数据文件，使用索引文件记录每个块的位置（`blocks_log.bin` + `blocks_log.idx`）

累加模式支持**断点续传**：中断后可以继续运行，自动跳过已存在的块。

## 命令行参数

### 基本参数
- `--begin, -b <number>`: 起始块号（必需）
- `--end, -e <number>`: 结束块号（必需）
- `--step, -s <size>`: 任务步长，默认 100
- `--datadir <path>`: 数据库目录路径（从 EnvironmentArgs 继承）

### 日志生成参数
- `--log-block <number>`: 指定要记录日志的块号（单个块）
- `--log-dir <path>`: 日志目录路径（累加模式）
  - 如果指定，使用累加文件模式：`blocks_log.bin`（数据）+ `blocks_log.idx`（索引）
  - 如果不指定，使用单个文件模式：`block_XXXXX_reads.bin`
- `--compression <algorithm>`: 压缩算法
  - 可选值：`none`, `zstd`, `brotli`, `lzma`, `lz4`, `auto`（默认）
  - `auto` 模式会自动选择压缩率最好的算法
  - **自动优化**：如果压缩后大小 >= 原数据大小，自动使用不压缩

### 日志使用参数
- `--use-log <path_or_block>`: 使用日志文件执行块
  - 可以是文件路径（单个文件模式）
  - 可以是块号（累加模式，需要配合 `--log-dir`）

## 使用场景

### 场景1: 批量生成日志（累加模式，支持断点续传）

**重要说明**：当前 `--log-block` 参数只能记录单个块的日志。批量生成需要循环调用，或使用脚本。

**方法1：使用脚本批量生成（推荐）**

创建批处理脚本 `generate_logs.bat`（Windows）：
```batch
@echo off
set BEGIN=1000
set END=2000
set LOG_DIR=./logs
set DATADIR=d:\reth2k

for /L %%i in (%BEGIN%,1,%END%) do (
    echo Generating log for block %%i...
    pevm.exe evm --begin %%i --end %%i --log-block %%i --log-dir %LOG_DIR% --compression auto --datadir %DATADIR%
    if errorlevel 1 (
        echo Error generating log for block %%i
        pause
        exit /b 1
    )
)
echo All logs generated successfully!
```

或 PowerShell 脚本 `generate_logs.ps1`：
```powershell
$begin = 1000
$end = 2000
$logDir = "./logs"
$datadir = "d:\reth2k"

for ($block = $begin; $block -le $end; $block++) {
    Write-Host "Generating log for block $block..."
    & .\pevm.exe evm --begin $block --end $block --log-block $block --log-dir $logDir --compression auto --datadir $datadir
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error generating log for block $block"
        exit 1
    }
}
Write-Host "All logs generated successfully!"
```

**方法2：单次运行（记录单个块）**

```bash
# 生成块 1000 的日志
./pevm.exe evm \
  --begin 1000 \
  --end 1000 \
  --log-block 1000 \
  --log-dir ./logs \
  --compression auto \
  --datadir d:\reth2k
```

**断点续传**：如果中断后继续运行，系统会自动跳过已存在的块
```bash
# 重新运行脚本，系统会自动检测已存在的块并跳过
# 例如：块 1000-1500 已存在，会从 1501 继续
```

**说明**：
- `--log-dir` 指定日志目录，使用累加模式
- 系统会自动检查 `./logs/blocks_log.idx`，跳过已存在的块
- 新块的日志会追加到 `./logs/blocks_log.bin`
- 索引会更新到 `./logs/blocks_log.idx`
- **支持断点续传**：中断后重新运行，自动从断点继续

### 场景2: 批量生成日志（单个文件模式）

为每个块生成独立的日志文件：
```bash
# 生成块 1000 的日志
./pevm.exe evm \
  --begin 1000 \
  --end 1000 \
  --log-block 1000 \
  --compression auto \
  --datadir d:\reth2k

# 生成块 1001 的日志
./pevm.exe evm \
  --begin 1001 \
  --end 1001 \
  --log-block 1001 \
  --compression auto \
  --datadir d:\reth2k
```

**说明**：
- 不指定 `--log-dir`，使用单个文件模式
- 每个块生成一个文件：`block_1000_reads.bin`, `block_1001_reads.bin` 等

### 场景3: 使用日志执行（累加模式）

从累加日志文件中读取并执行块：
```bash
./pevm.exe evm \
  --begin 1000 \
  --end 1000 \
  --use-log 1000 \
  --log-dir ./logs \
  --datadir d:\reth2k
```

**说明**：
- `--use-log 1000` 指定块号
- `--log-dir ./logs` 指定日志目录（累加模式）
- 系统会从 `./logs/blocks_log.idx` 查找块 1000 的偏移量和长度
- 然后从 `./logs/blocks_log.bin` 读取对应的数据

### 场景4: 使用日志执行（单个文件模式）

从单个日志文件读取并执行：
```bash
./pevm.exe evm \
  --begin 1000 \
  --end 1000 \
  --use-log block_1000_reads.bin \
  --datadir d:\reth2k
```

**说明**：
- `--use-log` 指定文件路径
- 不指定 `--log-dir`，使用单个文件模式

### 场景5: 大范围批量生成（支持断点续传）

**使用脚本批量生成**：从块 1 到 1000000

PowerShell 脚本 `generate_large_range.ps1`：
```powershell
$begin = 1
$end = 1000000
$logDir = "./logs"
$datadir = "d:\reth2k"

# 读取已存在的块（从索引文件）
$existingBlocks = @{}
if (Test-Path "$logDir\blocks_log.idx") {
    # 这里可以添加读取索引文件的逻辑，但脚本中简化处理
    # 实际运行时，程序会自动跳过已存在的块
}

for ($block = $begin; $block -le $end; $block++) {
    Write-Host "[$block/$end] Generating log for block $block..."
    & .\pevm.exe evm --begin $block --end $block --log-block $block --log-dir $logDir --compression auto --datadir $datadir
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error at block $block, stopping..."
        exit 1
    }
    
    # 每 1000 个块输出一次进度
    if ($block % 1000 -eq 0) {
        Write-Host "Progress: $block/$end blocks completed"
    }
}
Write-Host "All logs generated successfully!"
```

**中断后继续**：如果中断在块 500000，重新运行相同脚本
```powershell
# 系统会自动检测已存在的块，从 500001 继续
# 重新运行上面的脚本即可
```

**说明**：
- 系统会读取 `./logs/blocks_log.idx`，找出已存在的块
- 自动跳过已存在的块，只处理不存在的块
- 支持从任意块号开始继续处理
- **建议**：对于大范围（如 2000 万块），可以分批处理，每批 10000-100000 块

## 文件格式

### 累加模式文件结构

**索引文件** (`blocks_log.idx`)：
- 每个条目 24 字节
- 格式：`[block_number(8) + offset(8) + length(8)]`
- 按块号顺序存储（但不要求连续）

**数据文件** (`blocks_log.bin`)：
- 所有块的压缩日志数据累加存储
- 每个块的数据格式：`[compression_algorithm_id(1)] + [compressed_data]`
- 压缩算法 ID：
  - `0` = None（不压缩）
  - `1` = Zstd
  - `2` = Brotli
  - `3` = LZMA
  - `4` = LZ4

### 单个文件模式

**日志文件** (`block_XXXXX_reads.bin`)：
- 格式：`[compression_algorithm_id(1)] + [compressed_data]`
- 压缩后的数据包含：
  - 条目数量（8 字节，小端）
  - 每个条目的数据长度（1 字节）+ 数据内容

## 压缩优化

- **自动选择**：`--compression auto` 会自动测试所有压缩算法，选择压缩率最好的
- **智能回退**：如果压缩后大小 >= 原数据大小，自动使用不压缩（None）
- **性能对比**：会打印所有压缩算法的对比信息（压缩率、压缩时间、解压时间）

## 注意事项

1. **断点续传**：累加模式支持断点续传，中断后可以继续运行
2. **块号顺序**：索引文件中的块号不需要连续，可以跳跃
3. **压缩优化**：系统会自动选择最优压缩方案，避免压缩后文件变大
4. **并发安全**：多个进程同时写入同一个日志目录可能导致数据损坏，建议单进程运行
5. **大范围处理**：对于超过2000万块的大范围，建议使用累加模式，支持中断后继续

## 示例命令总结

### 批量生成（累加模式，推荐）

**单次生成一个块**：
```bash
# 生成块 1000 的日志
./pevm.exe evm -b 1000 -e 1000 --log-block 1000 --log-dir ./logs --compression auto --datadir d:\reth2k
```

**批量生成多个块（使用脚本）**：
```powershell
# PowerShell 脚本示例
for ($i = 1000; $i -le 2000; $i++) {
    .\pevm.exe evm -b $i -e $i --log-block $i --log-dir ./logs --compression auto --datadir d:\reth2k
}
```

### 批量使用日志执行（累加模式）

**单次执行一个块**：
```bash
# 使用累加日志执行块 1000
./pevm.exe evm -b 1000 -e 1000 --use-log 1000 --log-dir ./logs --datadir d:\reth2k
```

**批量执行多个块（使用脚本）**：
```powershell
# PowerShell 脚本示例
for ($i = 1000; $i -le 2000; $i++) {
    .\pevm.exe evm -b $i -e $i --use-log $i --log-dir ./logs --datadir d:\reth2k
}
```

### 批量生成（单个文件模式）

**单次生成一个块**：
```bash
# 生成块 1000 的日志（单个文件）
./pevm.exe evm -b 1000 -e 1000 --log-block 1000 --compression auto --datadir d:\reth2k
```

**批量生成（使用脚本）**：
```powershell
for ($i = 1000; $i -le 2000; $i++) {
    .\pevm.exe evm -b $i -e $i --log-block $i --compression auto --datadir d:\reth2k
}
```

### 批量使用日志执行（单个文件模式）

**单次执行一个块**：
```bash
# 使用单个日志文件执行块 1000
./pevm.exe evm -b 1000 -e 1000 --use-log block_1000_reads.bin --datadir d:\reth2k
```

**批量执行（使用脚本）**：
```powershell
for ($i = 1000; $i -le 2000; $i++) {
    .\pevm.exe evm -b $i -e $i --use-log "block_${i}_reads.bin" --datadir d:\reth2k
}
```

