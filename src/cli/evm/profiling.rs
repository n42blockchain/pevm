//! 性能分析模块 - 跨平台 CPU 性能分析支持
//! 
//! - Linux: 使用 pprof-rs（信号采样，最准确）
//! - macOS/Windows: 使用基于线程的采样（避免信号兼容性问题）

// Linux 上使用 pprof-rs（信号采样）
#[cfg(all(unix, target_os = "linux"))]
mod linux_profiler {
    use pprof::ProfilerGuard;
    
    pub(crate) struct ProfilerGuardWrapper<'a> {
        guard: Option<ProfilerGuard<'a>>,
    }
    
    impl<'a> ProfilerGuardWrapper<'a> {
        pub(crate) fn new(frequency: i32) -> Result<Self, String> {
            match ProfilerGuard::new(frequency) {
                Ok(guard) => Ok(Self { guard: Some(guard) }),
                Err(e) => Err(format!("Failed to start profiler: {}", e)),
            }
        }
        
        pub(crate) fn generate_flamegraph(&mut self, output_file: &str) -> Result<(), String> {
            if let Some(ref guard) = self.guard {
                match guard.report().build() {
                    Ok(report) => {
                        match std::fs::File::create(output_file) {
                            Ok(file) => {
                                report.flamegraph(file)
                                    .map_err(|e| format!("Failed to generate flamegraph: {}", e))?;
                                Ok(())
                            }
                            Err(e) => Err(format!("Failed to create file {}: {}", output_file, e)),
                        }
                    }
                    Err(e) => Err(format!("Failed to build report: {}", e)),
                }
            } else {
                Err("Profiler guard not initialized".to_string())
            }
        }
    }
}

// macOS 上使用基于线程的采样（避免信号兼容性问题导致的 trace trap）
#[cfg(target_os = "macos")]
mod macos_profiler {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};
    use backtrace::Backtrace;
    
    /// macOS 上的 CPU profiler（基于线程采样，避免信号问题）
    pub(crate) struct ProfilerGuardWrapper {
        samples: Arc<Mutex<Vec<Backtrace>>>,
        sampling_thread: Option<thread::JoinHandle<()>>,
        should_stop: Arc<std::sync::atomic::AtomicBool>,
    }
    
    impl ProfilerGuardWrapper {
        pub(crate) fn new(frequency: i32) -> Result<Self, String> {
            let samples = Arc::new(Mutex::new(Vec::new()));
            let should_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let samples_clone = Arc::clone(&samples);
            let should_stop_clone = Arc::clone(&should_stop);
            
            // 计算采样间隔（微秒）
            let interval_us = 1_000_000 / frequency as u64;
            
            let sampling_thread = thread::spawn(move || {
                let interval = Duration::from_micros(interval_us);
                while !should_stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
                    let start = Instant::now();
                    
                    // 捕获当前堆栈跟踪
                    let bt = Backtrace::new();
                    if let Ok(mut samples) = samples_clone.lock() {
                        samples.push(bt);
                    }
                    
                    // 等待到下一个采样点
                    let elapsed = start.elapsed();
                    if elapsed < interval {
                        thread::sleep(interval - elapsed);
                    }
                }
            });
            
            Ok(Self {
                samples,
                sampling_thread: Some(sampling_thread),
                should_stop,
            })
        }
        
        pub(crate) fn generate_flamegraph(&mut self, output_file: &str) -> Result<(), String> {
            // 停止采样线程
            self.should_stop.store(true, std::sync::atomic::Ordering::Relaxed);
            if let Some(handle) = self.sampling_thread.take() {
                let _ = handle.join();
            }
            
            // 获取所有样本
            let samples = self.samples.lock().map_err(|e| format!("Lock error: {}", e))?;
            
            if samples.is_empty() {
                return Err("No samples collected".to_string());
            }
            
            // 统计堆栈跟踪
            let mut stack_counts: HashMap<String, usize> = HashMap::new();
            for bt in samples.iter() {
                let mut stack_frames = Vec::new();
                for frame in bt.frames() {
                    for symbol in frame.symbols() {
                        if let Some(name) = symbol.name() {
                            let name_str = name.to_string();
                            // 保留更多函数路径信息
                            let simplified = name_str
                                .split('<')
                                .next()
                                .unwrap_or(&name_str)
                                .to_string();
                            stack_frames.push(simplified);
                            break;
                        }
                    }
                    if stack_frames.len() >= 20 {
                        break;
                    }
                }
                if !stack_frames.is_empty() {
                    // 反转堆栈顺序（从调用者到被调用者）
                    stack_frames.reverse();
                    let stack_key = stack_frames.join(";");
                    *stack_counts.entry(stack_key).or_insert(0) += 1;
                }
            }
            
            // 生成火焰图
            use std::io::Write;
            
            let mut stack_data = String::new();
            for (stack, count) in stack_counts.iter() {
                stack_data.push_str(&format!("{} {}\n", stack, count));
            }
            
            if stack_data.is_empty() {
                return Err("No valid stack frames collected".to_string());
            }
            
            let mut opts = inferno::flamegraph::Options::default();
            let writer = std::fs::File::create(output_file)
                .map_err(|e| format!("Failed to create file {}: {}", output_file, e))?;
            let mut writer = std::io::BufWriter::new(writer);
            
            inferno::flamegraph::from_reader(
                &mut opts,
                std::io::Cursor::new(stack_data.as_bytes()),
                &mut writer,
            )
            .map_err(|e| format!("Failed to generate flamegraph: {}", e))?;
            
            writer.flush()
                .map_err(|e| format!("Failed to flush writer: {}", e))?;
            
            Ok(())
        }
    }
    
    impl Drop for ProfilerGuardWrapper {
        fn drop(&mut self) {
            self.should_stop.store(true, std::sync::atomic::Ordering::Relaxed);
            if let Some(handle) = self.sampling_thread.take() {
                let _ = handle.join();
            }
        }
    }
}

#[cfg(windows)]
mod windows_profiler {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};
    use backtrace::Backtrace;
    
    /// Windows 上的简化 CPU profiler（基于线程采样）
    pub(crate) struct ProfilerGuardWrapper {
        samples: Arc<Mutex<Vec<Backtrace>>>,
        sampling_thread: Option<thread::JoinHandle<()>>,
        should_stop: Arc<std::sync::atomic::AtomicBool>,
    }
    
    impl ProfilerGuardWrapper {
        pub(crate) fn new(frequency: i32) -> Result<Self, String> {
            let samples = Arc::new(Mutex::new(Vec::new()));
            let should_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let samples_clone = Arc::clone(&samples);
            let should_stop_clone = Arc::clone(&should_stop);
            
            // 计算采样间隔（毫秒）
            let interval_ms = 1000 / frequency as u64;
            
            let sampling_thread = thread::spawn(move || {
                let interval = Duration::from_millis(interval_ms);
                while !should_stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
                    let start = Instant::now();
                    
                    // 捕获当前线程的堆栈跟踪
                    let bt = Backtrace::new();
                    samples_clone.lock().unwrap().push(bt);
                    
                    // 等待到下一个采样点
                    let elapsed = start.elapsed();
                    if elapsed < interval {
                        thread::sleep(interval - elapsed);
                    }
                }
            });
            
            Ok(Self {
                samples,
                sampling_thread: Some(sampling_thread),
                should_stop,
            })
        }
        
        pub(crate) fn generate_flamegraph(&mut self, output_file: &str) -> Result<(), String> {
            // 停止采样线程
            self.should_stop.store(true, std::sync::atomic::Ordering::Relaxed);
            if let Some(handle) = self.sampling_thread.take() {
                let _ = handle.join();
            }
            
            // 获取所有样本
            let samples = self.samples.lock().unwrap();
            
            if samples.is_empty() {
                return Err("No samples collected".to_string());
            }
            
            // 统计堆栈跟踪（提取函数名）
            let mut stack_counts: HashMap<String, usize> = HashMap::new();
            for bt in samples.iter() {
                // 从 Backtrace 中提取函数名
                // 格式：func1;func2;func3
                let mut stack_frames = Vec::new();
                for frame in bt.frames() {
                    for symbol in frame.symbols() {
                        if let Some(name) = symbol.name() {
                            // 清理函数名，移除 Rust 的命名空间装饰
                            let name_str = name.to_string();
                            // 简化：只取函数名部分（去掉路径和类型参数）
                            let simplified = name_str
                                .split("::")
                                .last()
                                .unwrap_or(&name_str)
                                .split('<')
                                .next()
                                .unwrap_or(&name_str)
                                .to_string();
                            stack_frames.push(simplified);
                            break; // 每个 frame 只取第一个 symbol
                        }
                    }
                    if stack_frames.len() >= 10 {
                        break; // 最多取10层
                    }
                }
                if !stack_frames.is_empty() {
                    let stack_key = stack_frames.join(";");
                    *stack_counts.entry(stack_key).or_insert(0) += 1;
                }
            }
            
            // 使用 inferno 生成火焰图
            use std::io::Write;
            
            // 创建堆栈数据（inferno 格式：func1;func2;func3 count）
            let mut stack_data = String::new();
            for (stack, count) in stack_counts.iter() {
                stack_data.push_str(&format!("{} {}\n", stack, count));
            }
            
            if stack_data.is_empty() {
                return Err("No valid stack frames collected".to_string());
            }
            
            // 生成火焰图
            let mut opts = inferno::flamegraph::Options::default();
            let writer = std::fs::File::create(output_file)
                .map_err(|e| format!("Failed to create file {}: {}", output_file, e))?;
            let mut writer = std::io::BufWriter::new(writer);
            
            // 使用 inferno 生成火焰图
            inferno::flamegraph::from_reader(
                &mut opts,
                std::io::Cursor::new(stack_data.as_bytes()),
                &mut writer,
            )
            .map_err(|e| format!("Failed to generate flamegraph: {}", e))?;
            
            writer.flush()
                .map_err(|e| format!("Failed to flush writer: {}", e))?;
            
            Ok(())
        }
    }
    
    impl Drop for ProfilerGuardWrapper {
        fn drop(&mut self) {
            self.should_stop.store(true, std::sync::atomic::Ordering::Relaxed);
            if let Some(handle) = self.sampling_thread.take() {
                let _ = handle.join();
            }
        }
    }
}

// 导出统一的接口
#[cfg(all(unix, target_os = "linux"))]
pub(crate) use linux_profiler::ProfilerGuardWrapper;

#[cfg(target_os = "macos")]
pub(crate) use macos_profiler::ProfilerGuardWrapper;

#[cfg(windows)]
pub(crate) use windows_profiler::ProfilerGuardWrapper;

