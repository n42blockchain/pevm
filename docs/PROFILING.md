# PEVM 性能分析指南

本文档介绍如何使用 pprof-rs 进行 CPU 性能分析和生成火焰图。

## 功能说明

PEVM 集成了 `pprof-rs` 库，可以在执行 EVM 命令时进行 CPU 性能分析，并自动生成火焰图。

## 使用方法

### 基本用法

```bash
# 启用性能分析并生成火焰图
cargo run --release -- evm -b 1000 -e 2000 --profile --datadir d:\reth2k
```

执行完成后，会在当前目录生成 `flamegraph.svg` 文件。

### 平台支持

- ✅ **Linux/macOS**: 完全支持，使用 `pprof-rs` 进行 CPU 采样（基于 POSIX 信号）
- ✅ **Windows**: 支持简化版本，使用基于线程的采样（精度略低于 Unix 版本）

**注意**：Windows 版本使用简化的线程采样方法，精度不如 Unix 版本的信号驱动采样。对于更详细的 Windows 性能分析，建议使用 Windows Performance Toolkit。

### Windows 上的替代方案

在 Windows 上，请使用以下替代方案：

1. **Windows Performance Toolkit (WPT)**（推荐）：
   ```powershell
   .\scripts\profile_with_wpt.ps1 -BeginBlock 1000 -EndBlock 2000 -UseLog $false
   ```

2. **PowerShell 基准测试脚本**：
   ```powershell
   .\scripts\benchmark_performance.ps1 -BeginBlock 1000 -EndBlock 2000
   ```

3. **使用 WSL (Windows Subsystem for Linux)**：
   在 WSL 中运行，可以使用完整的 pprof 功能。

## 技术细节

### 采样频率

默认采样频率为 **100Hz**（每秒采样 100 次），这是一个在性能和精度之间的良好平衡。

### 火焰图输出

- **文件名**: `flamegraph.svg`
- **格式**: SVG（可在浏览器中打开）
- **内容**: CPU 时间分布，显示函数调用栈和耗时

### 性能影响

启用性能分析会有轻微的性能开销（通常 < 5%），但可以提供详细的性能分析数据。

## 示例

### 分析不使用日志的执行

```bash
cargo run --release -- evm -b 1000 -e 2000 --profile --datadir d:\reth2k
```

### 分析使用日志 bin 的执行

```bash
cargo run --release -- evm -b 1000 -e 2000 --profile --use-log on --log-dir ./logs --datadir d:\reth2k
```

### 查看火焰图

生成 `flamegraph.svg` 后，在浏览器中打开即可查看：

```bash
# Linux/macOS
open flamegraph.svg

# Windows (PowerShell)
start flamegraph.svg
```

## 火焰图解读

火焰图的特点：
- **X 轴**: 时间顺序（从左到右）
- **Y 轴**: 调用栈深度（从上到下）
- **宽度**: 函数执行时间（越宽表示耗时越长）
- **颜色**: 随机分配，用于区分不同函数

### 如何识别性能瓶颈

1. **寻找最宽的条**: 表示最耗时的函数
2. **查看调用栈**: 从下往上阅读，了解函数调用关系
3. **关注热点**: 频繁出现的函数可能是优化重点

## 故障排除

### 问题：在 Windows 上编译失败

**原因**: `pprof-rs` 依赖 `nix` crate，在 Windows 上无法编译。

**解决方案**: 
- 使用 Windows Performance Toolkit
- 或在 WSL 中运行

### 问题：火焰图为空或很小

**可能原因**:
1. 执行时间太短（< 1 秒）
2. 采样频率太低
3. 代码执行路径没有被采样到

**解决方案**:
- 增加测试范围（更多块）
- 确保代码实际执行了

### 问题：性能分析失败

如果看到警告信息 "Failed to start CPU profiler"，可能是：
- 权限不足（某些系统需要特殊权限）
- 系统不支持性能分析

## 更多资源

- [pprof-rs 文档](https://docs.rs/pprof/)
- [火焰图解读指南](http://www.brendangregg.com/flamegraphs.html)
- [性能分析最佳实践](https://nnethercote.github.io/perf-book/profiling.html)

