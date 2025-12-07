# Windows 上的性能分析支持

根据 [pprof-rs GitHub 仓库](https://github.com/tikv/pprof-rs) 的研究，`pprof-rs` 主要依赖 POSIX 系统调用（`setitimer`、`SIGPROF`），这些在 Windows 上不可用。

## 实现方案

我们实现了一个**跨平台的性能分析方案**：

### Unix 系统（Linux/macOS）

- 使用 **pprof-rs**：基于 POSIX 信号的精确 CPU 采样
- 采样频率：100Hz（每秒 100 次）
- 精度：高（系统级信号驱动）

### Windows 系统

- 使用 **基于线程的采样**：简化的 CPU 性能分析
- 采样频率：100Hz（每秒 100 次）
- 精度：中等（线程级采样，不如信号驱动精确）

## 使用方法

### 启用性能分析

```powershell
# Windows 上使用 --profile 参数
.\target\release\pevm.exe evm -b 1000 -e 2000 --profile --datadir d:\reth2k
```

执行完成后，会生成 `flamegraph.svg` 文件。

### Windows 版本的限制

1. **采样精度**：Windows 版本使用线程采样，只能捕获采样线程本身的堆栈，无法像 Unix 版本那样捕获所有线程的堆栈
2. **多线程支持**：当前实现主要采样主执行线程，对于多线程程序的性能分析可能不够全面

## 推荐的 Windows 性能分析方案

### 方案 1: Windows Performance Toolkit（最推荐）

```powershell
.\scripts\profile_with_wpt.ps1 -BeginBlock 1000 -EndBlock 2000 -UseLog $false
```

**优点**：
- Windows 原生支持
- 完整的系统级性能分析
- 可以分析所有线程和进程

### 方案 2: 使用 WSL（如果需要完整的 pprof-rs 功能）

在 WSL 中运行：
```bash
cargo run --release -- evm -b 1000 -e 2000 --profile --datadir /mnt/d/reth2k
```

**优点**：
- 完整的 pprof-rs 功能
- 精确的 CPU 采样
- 支持多线程分析

### 方案 3: PowerShell 基准测试脚本（最简单）

```powershell
.\scripts\benchmark_performance.ps1 -BeginBlock 1000 -EndBlock 2000
```

**优点**：
- 不需要额外工具
- 快速性能对比
- 详细的统计信息

## 技术实现细节

### Unix 版本（pprof-rs）

- 使用 `setitimer` 设置定时器
- 通过 `SIGPROF` 信号触发采样
- 使用 `backtrace-rs` 捕获堆栈跟踪
- 使用 `inferno` 生成火焰图

### Windows 版本（简化实现）

- 使用独立线程进行定时采样
- 使用 `backtrace` crate 捕获堆栈跟踪
- 使用 `inferno` 生成火焰图
- **限制**：只能采样采样线程本身，无法跨线程采样

## 未来改进方向

如果要改进 Windows 版本的性能分析，可以考虑：

1. **使用 Windows Performance API**：
   - `QueryThreadCycleTime`：获取线程 CPU 周期
   - `GetThreadTimes`：获取线程时间信息
   - `SuspendThread` + `CaptureStackBackTrace`：捕获其他线程的堆栈

2. **集成 Windows Performance Toolkit**：
   - 直接调用 WPT API
   - 生成 ETL 文件
   - 使用 WPA 进行分析

3. **使用第三方工具**：
   - Intel VTune
   - Visual Studio Profiler
   - PerfView

## 参考资源

- [pprof-rs GitHub](https://github.com/tikv/pprof-rs)
- [Windows Performance Toolkit 文档](https://docs.microsoft.com/en-us/windows-hardware/test/wpt/)
- [inferno 文档](https://docs.rs/inferno/)

