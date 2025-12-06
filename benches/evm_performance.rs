use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use std::process::Command;
use std::time::Instant;
use std::path::PathBuf;

/// 性能基准测试：对比使用日志 bin 和不使用日志的执行性能
/// 
/// 注意：这个基准测试通过调用命令行工具来测试性能，而不是直接调用内部函数。
/// 这是因为 EvmCommand::execute 是 async 的，且需要完整的 EVM 环境。
/// 
/// 运行方式：
/// ```bash
/// # 设置环境变量
/// export PEVM_DATADIR="d:\\reth2k"
/// export PEVM_LOG_DIR=".\\bench_logs"
/// 
/// # 运行基准测试
/// cargo bench --bench evm_performance
/// ```
/// 
/// 或者使用火焰图分析：
/// ```bash
/// cargo install cargo-flamegraph
/// cargo flamegraph --bench evm_performance -- --bench
/// ```

fn bench_evm_execution(c: &mut Criterion) {
    let mut group = c.benchmark_group("evm_execution");
    
    // 测试参数
    let test_blocks = vec![
        (1000, 1000),      // 单个块
        (1000, 2000),      // 1000 个块
    ];
    
    // 从环境变量获取配置
    let datadir = std::env::var("PEVM_DATADIR")
        .unwrap_or_else(|_| "d:\\reth2k".to_string());
    let log_dir = std::env::var("PEVM_LOG_DIR")
        .unwrap_or_else(|_| "./bench_logs".to_string());
    let pevm_exe = std::env::var("PEVM_EXE")
        .unwrap_or_else(|_| "./target/release/pevm.exe".to_string());
    
    // 检查可执行文件是否存在
    if !PathBuf::from(&pevm_exe).exists() {
        eprintln!("警告: PEVM 可执行文件不存在: {}", pevm_exe);
        eprintln!("请先构建: cargo build --release");
        eprintln!("或设置 PEVM_EXE 环境变量指向正确的路径");
        return;
    }
    
    for (begin, end) in test_blocks {
        let block_count = end - begin + 1;
        
        // 基准测试：不使用日志（正常执行）
        group.bench_with_input(
            BenchmarkId::new("without_log", format!("blocks_{}", block_count)),
            &(begin, end, false, datadir.clone(), log_dir.clone(), pevm_exe.clone()),
            |b, (begin, end, _use_log, datadir, log_dir, pevm_exe)| {
                b.iter(|| {
                    black_box(execute_blocks_via_cli(
                        *begin, *end, false, datadir, log_dir, pevm_exe
                    ));
                });
            },
        );
        
        // 基准测试：使用日志 bin
        group.bench_with_input(
            BenchmarkId::new("with_log_bin", format!("blocks_{}", block_count)),
            &(begin, end, true, datadir.clone(), log_dir.clone(), pevm_exe.clone()),
            |b, (begin, end, use_log, datadir, log_dir, pevm_exe)| {
                // 首先确保日志文件存在
                ensure_logs_exist(*begin, *end, datadir, log_dir, pevm_exe);
                
                b.iter(|| {
                    black_box(execute_blocks_via_cli(
                        *begin, *end, *use_log, datadir, log_dir, pevm_exe
                    ));
                });
            },
        );
    }
    
    group.finish();
}

/// 通过命令行工具执行块
fn execute_blocks_via_cli(
    begin: u64,
    end: u64,
    use_log: bool,
    datadir: &str,
    log_dir: &str,
    pevm_exe: &str,
) -> bool {
    let mut cmd = Command::new(pevm_exe);
    cmd.arg("evm")
        .arg("-b")
        .arg(begin.to_string())
        .arg("-e")
        .arg(end.to_string())
        .arg("--datadir")
        .arg(datadir);
    
    if use_log {
        cmd.arg("--use-log")
            .arg("on")
            .arg("--log-dir")
            .arg(log_dir);
    }
    
    // 静默输出（只捕获错误）
    cmd.stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    
    let status = cmd.status();
    status.map(|s| s.success()).unwrap_or(false)
}

/// 确保日志文件存在（如果不存在则生成）
fn ensure_logs_exist(begin: u64, end: u64, datadir: &str, log_dir: &str, pevm_exe: &str) {
    let log_path = PathBuf::from(log_dir).join("blocks_log.idx");
    if !log_path.exists() {
        // 如果日志文件不存在，生成它们
        println!("Generating logs for blocks {}..={}", begin, end);
        
        let mut cmd = Command::new(pevm_exe);
        cmd.arg("evm")
            .arg("-b")
            .arg(begin.to_string())
            .arg("-e")
            .arg(end.to_string())
            .arg("--log-block")
            .arg("on")
            .arg("--log-dir")
            .arg(log_dir)
            .arg("--compression")
            .arg("zstd")
            .arg("--datadir")
            .arg(datadir);
        
        let _ = cmd.status();
    }
}

criterion_group!(benches, bench_evm_execution);
criterion_main!(benches);

