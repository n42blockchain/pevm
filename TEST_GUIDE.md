# --use-log 模式性能测试指南

## 测试配置

### 测试参数
- **区块范围**: 9,000,000 - 9,100,000 (100,000 块)
- **Step 大小**: 3 (Windows优化值)
- **日志目录**: `test_bench_logs` 或你的日志目录
- **模式**: `--use-log on` (使用日志执行)
- **可选**: `--mmap-log` (如果有mmap日志文件)

### 为什么选择 step=3？
- Windows 系统在 step=3 时性能最优
- 减少线程创建开销
- 批量处理效率高
- Linux/macOS 可以使用更大的 step (如 100)

## 快速开始

### Windows 系统
```batch
# 方式1：使用批处理脚本（推荐）
test_log_execution.bat

# 方式2：直接命令行
target\release\pevm.exe evm ^
    --begin 9000000 ^
    --end 9100000 ^
    --step 3 ^
    --use-log on ^
    --log-dir test_bench_logs ^
    --mmap-log
```

### Linux/macOS 系统
```bash
# 方式1：使用Shell脚本（推荐）
./test_log_execution.sh

# 方式2：直接命令行
./target/release/pevm evm \
    --begin 9000000 \
    --end 9100000 \
    --step 3 \
    --use-log on \
    --log-dir test_bench_logs \
    --mmap-log
```

## 测试前准备

### 1. 确保已编译 release 版本
```bash
cargo build --release
```

### 2. 确保日志文件存在
检查日志目录中是否有以下文件之一：
- `state_logs_mmap.bin` + `state_logs_mmap.idx` (mmap模式)
- `blocks_log.bin` + `blocks_log.idx` (文件模式)

### 3. 如果需要生成日志
```bash
# 生成 mmap 日志（推荐，性能更好）
pevm evm \
    --begin 9000000 \
    --end 9100000 \
    --step 100 \
    --log-block on \
    --log-dir test_bench_logs \
    --mmap-log

# 或生成普通文件日志
pevm evm \
    --begin 9000000 \
    --end 9100000 \
    --step 100 \
    --log-block on \
    --log-dir test_bench_logs
```

## 预期输出

### 执行过程
```
=== Performance Test for --use-log Mode ===
Block range: 9,000,000 - 9,100,000 (100,000 blocks)
Step size: 3 blocks per batch
...

Using mmap-based state log storage (READ mode, lock-free): 10000000 blocks, range 1 - 10000000
...

[执行日志]
Block 9000000-9000099 processed
Block 9000100-9000199 processed
...
```

### 统计信息（优化后新增）
```
=== Log Execution Statistics ===
  Account reads:          12,345,678
  Storage reads:          23,456,789
  Total reads:            35,802,467
  DB fallbacks:                1,234
  Fallback rate:              0.00%
================================
```

### 性能指标
- **执行时间**: 记录总耗时
- **吞吐量**: 块数/秒
- **原子操作**: 通过统计可以计算减少的次数
- **退化率**: DB fallbacks / Total reads (越低越好)

## 性能对比测试

### 测试不同 step 大小（Windows）
```batch
# Step = 3 (推荐)
pevm evm --begin 9000000 --end 9010000 --step 3 --use-log on --log-dir test_bench_logs --mmap-log

# Step = 10
pevm evm --begin 9000000 --end 9010000 --step 10 --use-log on --log-dir test_bench_logs --mmap-log

# Step = 100
pevm evm --begin 9000000 --end 9010000 --step 100 --use-log on --log-dir test_bench_logs --mmap-log
```

### 对比优化前后性能
如果你有优化前的版本：
```bash
# 1. 测试优化前版本
git checkout b1be820  # 优化前的提交
cargo build --release
./test_log_execution.sh > results_before.txt

# 2. 测试优化后版本
git checkout main
cargo build --release
./test_log_execution.sh > results_after.txt

# 3. 对比结果
diff results_before.txt results_after.txt
```

## 监控要点

### 关键指标
1. **总执行时间** - 主要性能指标
2. **原子操作次数** - 应该减少约75倍
3. **CPU使用率** - 应该更均匀，减少尖峰
4. **内存使用** - 应该保持稳定
5. **退化率** - 日志缺失率（应接近0%）

### Windows 性能监控
```powershell
# 使用 Performance Monitor
perfmon

# 或使用任务管理器
# Ctrl+Shift+Esc -> Performance tab
```

### Linux 性能监控
```bash
# CPU 使用率
htop

# 详细性能分析
perf stat ./test_log_execution.sh

# 火焰图（如果启用了 --profile）
# 会生成 flamegraph.svg
```

## 故障排查

### 问题1：找不到日志文件
```
Error: Log directory test_bench_logs not found
```
**解决**: 先运行 `--log-block on` 生成日志

### 问题2：mmap 文件太大导致启动慢
```
# 检查文件大小
ls -lh test_bench_logs/state_logs_mmap.bin

# 如果 >1GB，优化已自动跳过 MADV_WILLNEED
# 如果仍然慢，可以不使用 --mmap-log
```

### 问题3：性能没有提升
**检查点**:
1. 是否使用了 `--release` 构建？
2. Step 是否太小（建议Windows=3, Linux=100）？
3. 是否使用了 `--mmap-log`（性能更好）？
4. 日志文件是否在快速磁盘上（SSD）？

## 预期性能提升

基于优化（commit bcee16e）:

### 原子操作减少
- **优化前**: 100,000块 × 6次 = 600,000 次原子操作
- **优化后**: ~33,333批次 × 8次 ≈ 266,664 次原子操作
- **提升**: 约 2.25x 减少

### Windows 系统特别受益
- 原子操作开销比 Linux 高 2-3x
- 预期性能提升: **10-30%**
- 更平滑的多线程执行

### 统计输出新功能
- 详细的读取统计
- 退化到数据库的次数
- 帮助分析日志质量

## 高级测试

### 测试不同区块范围
```bash
# 小批量测试 (1万块)
pevm evm --begin 9000000 --end 9010000 --step 3 --use-log on --log-dir test_bench_logs --mmap-log

# 中批量测试 (10万块) - 推荐
pevm evm --begin 9000000 --end 9100000 --step 3 --use-log on --log-dir test_bench_logs --mmap-log

# 大批量测试 (100万块)
pevm evm --begin 9000000 --end 10000000 --step 3 --use-log on --log-dir test_bench_logs --mmap-log
```

### 测试线程数影响
```bash
# 默认线程数 (CPU核心数 * 2 - 1)
pevm evm --begin 9000000 --end 9100000 --step 3 --use-log on --log-dir test_bench_logs --mmap-log

# 指定线程数 (Windows 建议较少线程)
pevm evm --begin 9000000 --end 9100000 --step 3 --threads 4 --use-log on --log-dir test_bench_logs --mmap-log
```

## 报告性能结果

如果要报告性能测试结果，请包含：
1. **系统信息**: OS版本、CPU型号、核心数、内存
2. **测试参数**: 区块范围、step大小、线程数
3. **执行时间**: 总耗时、吞吐量（块/秒）
4. **统计输出**: 日志统计信息截图
5. **对比数据**: 优化前后对比（如果有）

祝测试顺利！如有问题请查看项目 README 或提交 issue。
