# 性能分析火焰图使用指南

## 概述

本项目支持生成性能分析火焰图，用于识别性能瓶颈。火焰图可以显示程序中各个函数占用的 CPU 时间。

## 编译带性能分析的版本

### 方法 1: 使用 `profiling` profile（推荐）

```bash
# 编译 profiling 版本（保留调试符号，优化级别 3）
cargo build --profile profiling
```

编译后的可执行文件位于：`./target/profiling/pevm`

**特点：**
- 启用优化（`opt-level = 3`）
- 保留调试符号（`debug = 2`）
- 不剥离符号（`strip = false`）
- 适合性能分析，符号信息完整

### 方法 2: 使用 `release` profile

```bash
# 编译 release 版本
cargo build --release
```

编译后的可执行文件位于：`./target/release/pevm`

**注意：** Release 版本会剥离符号，火焰图可能无法显示函数名，建议使用 `profiling` profile。

## 生成火焰图

### 方法 1: 使用自动脚本（推荐）

```bash
# 基本用法
./scripts/profile_with_flamegraph.sh -b 1 -e 500000 -d "/path/to/reth2k" -u true -l "./bench_logs"

# 参数说明：
# -b, --begin BLOCK      开始块号
# -e, --end BLOCK        结束块号
# -d, --datadir DIR      数据目录
# -l, --logdir DIR       日志目录（使用 --use-log 时需要）
# -u, --use-log BOOL     是否使用日志 (true/false)
# -o, --output FILE      输出文件路径 (默认: flamegraph.svg)

# 使用环境变量
export PEVM_DATADIR="/path/to/reth2k"
export PEVM_LOG_DIR="./bench_logs"
./scripts/profile_with_flamegraph.sh -b 1 -e 500000 -u true

# 指定可执行文件路径（如果使用自定义编译）
./scripts/profile_with_flamegraph.sh -b 1 -e 500000 -d "/path/to/reth2k" --pevm-exe "./target/profiling/pevm"
```

脚本会自动：
1. 检查并安装 `cargo flamegraph`（如果未安装）
2. 使用 `cargo flamegraph` 运行程序
3. 生成火焰图 SVG 文件

### 方法 2: 手动使用 cargo flamegraph

```bash
# 确保已安装 cargo flamegraph
cargo install flamegraph

# 直接运行并生成火焰图
cargo flamegraph --profile profiling -- \
    evm --begin 1 --end 500000 \
    --datadir "/path/to/reth2k" \
    --use-log on \
    --log-dir "./bench_logs"
```

### 方法 3: 使用项目内置的性能分析功能

项目还支持内置的性能分析功能（使用 `pprof-rs`）：

```bash
# 编译 profiling 版本
cargo build --profile profiling

# 运行并启用内置性能分析
./target/profiling/pevm evm \
    --begin 1 --end 500000 \
    --datadir "/path/to/reth2k" \
    --use-log on \
    --log-dir "./bench_logs" \
    --enable-profiling

# 程序结束后会自动生成 flamegraph.svg
```

## Profile 配置说明

项目中的 `Cargo.toml` 定义了以下 profiles：

```toml
[profile.profiling]
debug = 2          # 保留完整调试信息
inherits = "release"  # 继承 release 的优化设置
strip = false      # 不剥离符号
```

这确保了：
- 程序以最优性能运行（`opt-level = 3`）
- 保留完整的调试符号（用于性能分析）
- 函数名在火焰图中可见

## 查看火焰图

生成的火焰图是 SVG 格式，可以用以下方式查看：

1. **浏览器**：直接在浏览器中打开 `flamegraph.svg`
2. **VS Code**：安装 SVG 查看器扩展
3. **在线工具**：上传到任何支持 SVG 的在线查看器

### 火焰图阅读技巧

- **宽度**：表示函数占用 CPU 时间的比例
- **高度**：表示调用栈的深度
- **颜色**：主要用于区分不同的函数（无特殊含义）
- **点击**：可以放大查看特定函数
- **搜索**：使用浏览器的搜索功能（Ctrl+F）查找特定函数名

## 常见问题

### 1. macOS 上的 dtrace 权限问题

如果遇到 `dtrace: failed to initialize dtrace: DTrace requires additional privileges`，可以：

- 使用项目内置的性能分析（`--enable-profiling`）
- 或者在 Linux 系统上运行

### 2. 火焰图没有函数名

确保使用 `--profile profiling` 编译，不要使用 `--release`（会剥离符号）。

### 3. 性能分析结果不准确

- 确保编译时使用了优化（`--profile profiling` 或 `--release`）
- 测试数据量要足够大，以获取有代表性的结果
- 多次运行取平均值

## 示例命令

```bash
# 完整示例：分析使用日志的执行性能
cargo build --profile profiling
./scripts/profile_with_flamegraph.sh \
    -b 1000 \
    -e 2000 \
    -d "/Users/jieliu/Documents/reth2k" \
    -u true \
    -l "./bench_logs" \
    -o "my_flamegraph.svg"

# 查看结果
open flamegraph.svg  # macOS
# 或
xdg-open flamegraph.svg  # Linux
```

## 性能优化建议

根据火焰图识别瓶颈后：

1. **文件 I/O 瓶颈**：考虑使用 `pread`（已实现）、内存映射等
2. **解压瓶颈**：考虑更换压缩算法或优化解压逻辑
3. **锁竞争**：减少锁的粒度或使用无锁数据结构
4. **CPU 密集操作**：考虑并行化或算法优化

