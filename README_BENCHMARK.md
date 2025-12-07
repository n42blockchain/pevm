# PEVM 性能基准测试指南

本文档介绍如何使用 Rust 性能分析工具来测试和对比 PEVM 使用日志 bin 和不使用日志的执行性能。

## 快速开始

### 1. 使用 PowerShell 脚本进行基准测试

最简单的性能测试方法是使用提供的 PowerShell 脚本：

```powershell
# 设置环境变量
$env:PEVM_DATADIR = "d:\reth2k"
$env:PEVM_LOG_DIR = ".\bench_logs"

# 运行基准测试（测试块 1000-2000）
.\scripts\benchmark_performance.ps1 -BeginBlock 1000 -EndBlock 2000
```

脚本会自动：
1. 检查并生成日志文件（如果不存在）
2. 进行预热运行
3. 分别测试不使用日志和使用日志 bin 的执行性能
4. 输出详细的性能对比报告

### 2. 使用性能分析工具

#### 选项 A: Windows Performance Toolkit (WPT) - Windows 推荐

Windows 上推荐使用 WPT 进行性能分析：

```powershell
# 分析不使用日志的执行
.\scripts\profile_with_wpt.ps1 -BeginBlock 1000 -EndBlock 2000 -UseLog $false -OutputFile "pevm_without_log.etl"

# 分析使用日志 bin 的执行
.\scripts\profile_with_wpt.ps1 -BeginBlock 1000 -EndBlock 2000 -UseLog $true -OutputFile "pevm_with_log.etl"
```

然后使用 Windows Performance Analyzer (WPA) 打开 `.etl` 文件进行分析。

#### 选项 B: 火焰图（仅限 Linux/macOS 或 WSL）

**注意**：`cargo flamegraph` 在 Windows 上**无法正常工作**，因为它需要 Linux 的 `perf` 工具或 macOS 的 `dtrace`。

如果需要在 Windows 上使用火焰图，可以：

1. **使用 WSL (Windows Subsystem for Linux)**：
```bash
# 在 WSL 中安装
cargo install flamegraph

# 在 WSL 中运行（注意路径转换）
cargo flamegraph --bin pevm -- evm -b 1000 -e 2000 --datadir /mnt/d/reth2k
```

2. **在 Linux 或 macOS 上运行**：
```powershell
# 分析不使用日志的执行
.\scripts\profile_with_flamegraph.ps1 -BeginBlock 1000 -EndBlock 2000 -UseLog $false -OutputFile "flamegraph_without_log.svg"

# 分析使用日志 bin 的执行
.\scripts\profile_with_flamegraph.ps1 -BeginBlock 1000 -EndBlock 2000 -UseLog $true -OutputFile "flamegraph_with_log.svg"
```

**在 Windows 上安装火焰图工具（不推荐，会失败）**：

```bash
# 方法 1: 安装 flamegraph（推荐，但 Windows 上可能有限制）
cargo install flamegraph

# 方法 2: 如果方法 1 失败，使用 WSL
# 在 WSL 中运行：
# cargo install flamegraph
# cargo flamegraph --bin pevm -- evm -b 1000 -e 2000 --datadir /mnt/d/reth2k

# 方法 3: 使用 Windows Performance Toolkit (WPT) 替代
# 见下面的"使用 Windows Performance Toolkit"部分
```

**Windows 限制**：`flamegraph` 在 Windows 上可能无法正常工作，因为它依赖于 Linux 的 `perf` 工具。建议：
- 使用 WSL (Windows Subsystem for Linux)
- 或使用 Windows Performance Toolkit
- 或使用 PowerShell 基准测试脚本进行性能对比

## 手动性能测试

### 方法 1: 使用命令行工具直接测试

```powershell
# 测试不使用日志
Measure-Command { .\target\release\pevm.exe evm -b 1000 -e 2000 --datadir d:\reth2k }

# 测试使用日志 bin
Measure-Command { .\target\release\pevm.exe evm -b 1000 -e 2000 --use-log on --log-dir .\bench_logs --datadir d:\reth2k }
```

### 方法 2: 使用 Criterion 基准测试框架

项目已经配置了 `criterion` 基准测试框架。运行基准测试：

```bash
cargo bench --bench evm_performance
```

基准测试结果会保存在 `target/criterion/` 目录下，包含详细的 HTML 报告。

### 方法 3: 使用 Windows Performance Toolkit (WPT)

Windows 上可以使用 WPT 进行更深入的性能分析（推荐用于 Windows）：

```powershell
# 使用提供的脚本（推荐）
.\scripts\profile_with_wpt.ps1 -BeginBlock 1000 -EndBlock 2000 -UseLog $false -OutputFile "pevm_without_log.etl"
.\scripts\profile_with_wpt.ps1 -BeginBlock 1000 -EndBlock 2000 -UseLog $true -OutputFile "pevm_with_log.etl"
```

或者手动使用：

```powershell
# 启动性能记录
wpr -start GeneralProfile -filemode

# 运行测试
.\target\release\pevm.exe evm -b 1000 -e 2000 --datadir d:\reth2k

# 停止记录并生成报告
wpr -stop pevm_trace.etl
```

然后使用 Windows Performance Analyzer (WPA) 打开 `.etl` 文件进行分析。

**注意**：WPT 需要管理员权限。如果遇到权限问题，请以管理员身份运行 PowerShell。

## 性能指标说明

### 主要指标

1. **执行时间**：完成指定块范围执行的总时间
2. **吞吐量**：块/秒 (blocks/second)
3. **Gas 吞吐量**：Ggas/秒 (Ggas/second)
4. **内存使用**：峰值内存占用

### 性能对比

性能测试脚本会自动计算：
- **加速比**：使用日志 bin 相对于不使用日志的速度提升
- **吞吐量对比**：两种模式的块处理速度对比
- **统计信息**：平均值、最小值、最大值、标准差

## 性能优化建议

### 如果使用日志 bin 较慢

可能的原因和优化方向：

1. **日志文件 I/O 开销**
   - 检查日志文件是否在 SSD 上
   - 考虑使用更快的压缩算法（如 `lz4`）
   - 增加缓冲区大小

2. **日志解析开销**
   - 使用火焰图识别热点
   - 优化 RLP 解码逻辑
   - 减少不必要的内存分配

3. **代码缓存未命中**
   - 检查 `code_by_hash` 的调用频率
   - 增加代码缓存大小
   - 优化缓存策略

### 如果使用日志 bin 较快

说明优化成功！可能的原因：

1. **减少了数据库查询**
   - 日志文件提供了预加载的数据
   - 避免了数据库索引查找

2. **更好的缓存局部性**
   - 日志数据按访问顺序存储
   - 减少了随机访问

## 性能测试最佳实践

1. **预热运行**：在正式测试前进行几次预热运行，确保缓存已预热
2. **多次运行**：进行多次测试并取平均值，减少随机误差
3. **固定环境**：在相同的硬件和系统负载下进行测试
4. **大范围测试**：测试不同大小的块范围，观察性能变化
5. **对比分析**：同时测试两种模式，确保公平对比

## 示例输出

```
=========================================
PEVM 性能基准测试
=========================================
测试范围: 块 1000 到 2000
数据目录: d:\reth2k
日志目录: .\bench_logs
预热运行: 1 次
基准测试: 3 次
=========================================

[1/3] 检查并生成日志文件...
  日志文件已存在，跳过生成

[2/3] 预热运行...
  预热运行 1/1...
  预热完成

[3/3] 性能基准测试...

测试 1: 不使用日志执行
  运行 1/3...
    耗时: 12.34s, Ggas_per_s=0.15
  运行 2/3...
    耗时: 12.28s, Ggas_per_s=0.15
  运行 3/3...
    耗时: 12.31s, Ggas_per_s=0.15

测试 2: 使用日志 bin 执行
  运行 1/3...
    耗时: 8.45s, Ggas_per_s=0.22
  运行 2/3...
    耗时: 8.52s, Ggas_per_s=0.22
  运行 3/3...
    耗时: 8.48s, Ggas_per_s=0.22

=========================================
性能测试结果
=========================================

不使用日志:
  平均耗时: 12.31s
  最小耗时: 12.28s
  最大耗时: 12.34s
  标准差:   0.03s

使用日志 bin:
  平均耗时: 8.48s
  最小耗时: 8.45s
  最大耗时: 8.52s
  标准差:   0.04s

性能对比:
  使用日志 bin 快 1.45x (提升 45.2%)

吞吐量对比:
  不使用日志: 81.24 块/秒
  使用日志 bin: 118.16 块/秒

=========================================
测试完成
=========================================
```

## 故障排除

### 问题：flamegraph 无法在 Windows 上运行

**解决方案**：`flamegraph` 在 Windows 上需要额外的设置，因为它依赖于 Linux 的 `perf` 工具。可以：

1. **使用 WSL (Windows Subsystem for Linux)**（推荐）：
   ```bash
   # 在 WSL 中安装
   cargo install flamegraph
   
   # 在 WSL 中运行（注意路径转换）
   cargo flamegraph --bin pevm -- evm -b 1000 -e 2000 --datadir /mnt/d/reth2k
   ```

2. **使用 Windows Performance Toolkit (WPT)**：
   ```powershell
   # 启动性能记录
   wpr -start GeneralProfile -filemode
   
   # 运行测试
   .\target\release\pevm.exe evm -b 1000 -e 2000 --datadir d:\reth2k
   
   # 停止记录
   wpr -stop pevm_trace.etl
   ```
   然后使用 Windows Performance Analyzer (WPA) 打开 `.etl` 文件。

3. **使用 PowerShell 基准测试脚本**：
   这是最简单的方法，不需要额外工具：
   ```powershell
   .\scripts\benchmark_performance.ps1 -BeginBlock 1000 -EndBlock 2000
   ```

### 问题：基准测试结果不稳定

**解决方案**：
1. 增加预热运行次数
2. 关闭其他应用程序，减少系统负载
3. 使用固定 CPU 频率（禁用 CPU 频率调节）
4. 增加测试运行次数，取平均值

### 问题：日志文件不存在

**解决方案**：
- 脚本会自动生成日志文件
- 或手动运行：`.\pevm.exe evm -b 1000 -e 2000 --log-block on --log-dir .\bench_logs --datadir d:\reth2k`

## 更多资源

- [Criterion.rs 文档](https://docs.rs/criterion/)
- [cargo-flamegraph 文档](https://github.com/flamegraph-rs/flamegraph)
- [Windows Performance Toolkit 文档](https://docs.microsoft.com/en-us/windows-hardware/test/wpt/)

