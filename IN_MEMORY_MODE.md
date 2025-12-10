# 内存模式测试版本使用说明

## 概述

这是一个**临时测试版本**，用于性能测试。主要改动：
1. **将整个 bin 文件读入内存**：所有文件操作改为内存访问
2. **取消压缩**：数据直接存储在内存中，读取和写入都不进行压缩/解压

## 启用方式

通过环境变量 `PEVM_IN_MEMORY_MODE` 控制（默认已启用）：

```bash
# 启用内存模式（默认）
export PEVM_IN_MEMORY_MODE=true
# 或者
export PEVM_IN_MEMORY_MODE=1

# 禁用内存模式（使用正常模式）
export PEVM_IN_MEMORY_MODE=false
# 或者
export PEVM_IN_MEMORY_MODE=0
```

## 使用示例

### 1. 生成日志（无压缩）

```bash
# 启用内存模式
export PEVM_IN_MEMORY_MODE=true

# 生成日志（会自动以无压缩方式写入）
cargo run --profile profiling -- evm \
    --begin 1 \
    --end 500000 \
    --datadir "/path/to/reth2k" \
    --log-block on \
    --log-dir "./bench_logs"
```

**注意**：生成的文件格式为无压缩格式（直接是 `count(8) + entries`），不是压缩格式。

### 2. 使用日志执行（从内存读取，无解压）

```bash
# 启用内存模式（必须）
export PEVM_IN_MEMORY_MODE=true

# 执行时会：
# 1. 启动时将整个 blocks_log.bin 读入内存
# 2. 所有读取操作直接从内存访问
# 3. 不需要解压操作
cargo run --profile profiling -- evm \
    --begin 1 \
    --end 500000 \
    --datadir "/path/to/reth2k" \
    --use-log on \
    --log-dir "./bench_logs"
```

## 性能影响

### 优势
- **消除文件 I/O 开销**：所有读取操作都是内存访问
- **消除解压开销**：不需要 LZ4/zstd 等解压操作
- **可以评估**：文件 I/O 和解压操作对性能的真实影响

### 限制
- **内存占用**：整个 bin 文件必须在内存中（最大 8GB）
- **数据格式不兼容**：生成的日志文件是未压缩格式，不能与正常模式互用
- **仅用于测试**：这是临时测试版本，不适合生产使用

## 文件格式差异

### 正常模式（压缩）
```
[压缩算法标识(1字节)][压缩后的数据]
```

### 内存模式（无压缩）
```
[count(8字节)][entry1: data_len(1) + data][entry2: data_len(1) + data]...
```

**注意**：内存模式下，数据直接是未压缩格式，没有压缩算法标识前缀。

## 注意事项

1. **首次运行需要生成无压缩格式的日志**：确保使用内存模式生成日志
2. **内存要求**：确保有足够内存容纳整个 bin 文件
3. **格式不兼容**：内存模式生成的日志不能用于正常模式，反之亦然
4. **仅用于测试**：这个模式是为了测试性能瓶颈，不适合日常使用

## 预期性能提升

使用内存模式后，应该能观察到：
- 文件 I/O 时间显著减少（接近 0）
- 解压时间完全消失
- 如果性能仍然慢，说明瓶颈在其他地方（如 EVM 执行本身）

## 恢复到正常模式

```bash
# 禁用内存模式
export PEVM_IN_MEMORY_MODE=false

# 重新生成正常格式的日志
cargo run --profile profiling -- evm \
    --begin 1 \
    --end 500000 \
    --datadir "/path/to/reth2k" \
    --log-block on \
    --log-dir "./bench_logs_new"
```

