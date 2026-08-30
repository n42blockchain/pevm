//! The autopilot: poll the publisher, apply what is new, verify what
//! arrives, repeat — which is how a node stays one delta behind the
//! live tip.

use super::catchup::{catch_up, CatchUpReport};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// The autopilot loop's configuration.
pub struct FollowConfig {
    pub datadir: PathBuf,
    /// file:// or http(s):// per-network root.
    pub source: String,
    /// minimal | full | archive.
    pub mode: String,
    /// Sleep between cycles; zero means 30 s.
    pub poll_interval: Duration,
    /// 0 = unlimited (stop only via the stop flag).
    pub max_cycles: usize,
    /// Per-cycle catch-up iteration bound; 0 = unlimited.
    pub max_iter: usize,
    /// Cooperative cancellation; set to stop after the current cycle.
    pub stop: Arc<AtomicBool>,
    /// Invoked at the end of every cycle.
    #[allow(clippy::type_complexity)]
    pub on_cycle: Option<Box<dyn FnMut(usize, &std::io::Result<CatchUpReport>)>>,
}

/// What an entire `follow` run did.
#[derive(Debug, Default)]
pub struct FollowReport {
    pub cycles: usize,
    pub applied_deltas: usize,
    pub final_height: u64,
    pub last_error: String,
    /// The stop flag (not an error) ended the loop.
    pub cancelled_clean: bool,
}

/// Every `poll_interval`, run a catch-up cycle and apply any new
/// deltas. Errors during one cycle are recorded and the loop carries
/// on — autopilot survives transient mirror outages.
pub fn follow(mut cfg: FollowConfig) -> std::io::Result<FollowReport> {
    if cfg.source.is_empty() || cfg.datadir.as_os_str().is_empty() {
        return Err(std::io::Error::other("follow: source and datadir required"));
    }
    if cfg.poll_interval.is_zero() {
        cfg.poll_interval = Duration::from_secs(30);
    }
    let mut rep = FollowReport::default();
    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            rep.cancelled_clean = true;
            return Ok(rep);
        }
        if cfg.max_cycles > 0 && rep.cycles >= cfg.max_cycles {
            return Ok(rep);
        }
        rep.cycles += 1;
        let cur = catch_up(&cfg.datadir, &cfg.source, &cfg.mode, cfg.max_iter);
        match &cur {
            Ok(cur) => {
                rep.applied_deltas += cur.iterations;
                rep.final_height = rep.final_height.max(cur.final_height);
                rep.last_error.clear();
            }
            Err(e) => rep.last_error = e.to_string(),
        }
        if let Some(on_cycle) = cfg.on_cycle.as_mut() {
            on_cycle(rep.cycles, &cur);
        }
        // Interruptible sleep.
        let deadline = std::time::Instant::now() + cfg.poll_interval;
        while std::time::Instant::now() < deadline {
            if cfg.stop.load(Ordering::Relaxed) {
                rep.cancelled_clean = true;
                return Ok(rep);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
