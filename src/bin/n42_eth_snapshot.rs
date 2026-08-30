// SPDX-License-Identifier: MIT OR Apache-2.0

//! n42-eth-snapshot — the client CLI for gov5's eth-el snapshot
//! distribution, byte-compatible with the Go tool of the same name.
//!
//! Subcommands: verify, mode, status, catch-up, follow, fetch,
//! upgrade, downgrade, delta plan|apply, manifest.

#![allow(missing_docs)]

use pevm::snapshot::*;
use std::path::{Path, PathBuf};
use std::process::exit;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(cmd) = args.first() else {
        usage();
        exit(2);
    };
    let rest = &args[1..];
    let code = match cmd.as_str() {
        "verify" => run_verify(rest),
        "mode" => run_mode(rest),
        "status" => run_status(rest),
        "catch-up" | "catchup" => run_catch_up(rest),
        "follow" => run_follow(rest),
        "fetch" => run_fetch(rest),
        "upgrade" => run_upgrade(rest),
        "downgrade" => run_downgrade(rest),
        "delta" => run_delta(rest),
        "manifest" => run_manifest(rest),
        "-h" | "--help" | "help" => {
            usage();
            0
        }
        other => {
            eprintln!("unknown subcommand: {other}\n");
            usage();
            2
        }
    };
    exit(code);
}

fn usage() {
    eprintln!(
        r#"n42-eth-snapshot — client snapshot tool

USAGE
    n42-eth-snapshot <subcommand> [flags]

SUBCOMMANDS
    verify         hash every file in a datadir against its manifest
    mode           detect maximal mode (minimal/full/archive) in a datadir
    status         compare local height to publisher's latest (am I behind?)
    catch-up       loop delta-apply until at publisher's latest height
    follow         background autopilot: poll publisher + apply new deltas
    fetch          copy missing files from --source into --datadir for --mode
    upgrade        fetch the delta needed to move from current to --to mode
    downgrade      remove files not in --to mode's manifest
    delta plan     show what files a delta from --source would fetch
    delta apply    apply an incremental delta from --source
    manifest       write manifest-<mode>.json for a built datadir
    help           this message"#
    );
}

/// The Go tool's flag surface, without a dependency: `--flag value` or
/// `--flag=value`, booleans bare.
struct Flags {
    values: std::collections::HashMap<String, String>,
}

impl Flags {
    fn parse(args: &[String], bools: &[&str]) -> Self {
        let mut values = std::collections::HashMap::new();
        let mut i = 0;
        while i < args.len() {
            let arg = args[i].trim_start_matches('-');
            if let Some((k, v)) = arg.split_once('=') {
                values.insert(k.to_string(), v.to_string());
            } else if bools.contains(&arg) {
                values.insert(arg.to_string(), "true".into());
            } else if i + 1 < args.len() {
                values.insert(arg.to_string(), args[i + 1].clone());
                i += 1;
            } else {
                eprintln!("flag --{arg} needs a value");
                exit(2);
            }
            i += 1;
        }
        Self { values }
    }
    fn get(&self, key: &str, default: &str) -> String {
        self.values.get(key).cloned().unwrap_or_else(|| default.into())
    }
    fn required(&self, key: &str) -> String {
        self.values.get(key).cloned().unwrap_or_else(|| {
            eprintln!("--{key} is required");
            exit(2);
        })
    }
    fn get_bool(&self, key: &str) -> bool {
        self.values.get(key).is_some_and(|v| v != "false")
    }
    fn get_usize(&self, key: &str, default: usize) -> usize {
        self.values.get(key).and_then(|v| v.parse().ok()).unwrap_or(default)
    }
}

fn gb(n: u64) -> f64 {
    n as f64 / 1024.0 / 1024.0 / 1024.0
}

fn run_verify(args: &[String]) -> i32 {
    let f = Flags::parse(args, &[]);
    let datadir = PathBuf::from(f.get("datadir", "."));
    let manifest = f.get("manifest", "");
    let workers = f.get_usize("parallel", 0);
    match verify(&datadir, &manifest, workers) {
        Ok(rep) => {
            println!("manifest : {}", rep.manifest_path);
            println!("mode     : {}", rep.mode);
            println!("height   : {}", rep.height);
            println!("files    : {}", rep.files_checked);
            for (label, list) in [
                ("MISSING", &rep.missing_files),
                ("WRONG SIZE", &rep.wrong_size),
                ("HASH MISMATCH", &rep.mismatches),
            ] {
                if !list.is_empty() {
                    println!("\n{label} ({}):", list.len());
                    for p in list {
                        println!("  {p}");
                    }
                }
            }
            println!("\nresult   : {}", if rep.ok { "OK" } else { "FAIL" });
            if rep.ok { 0 } else { 2 }
        }
        Err(e) => {
            eprintln!("verify: {e}");
            1
        }
    }
}

fn run_mode(args: &[String]) -> i32 {
    let f = Flags::parse(args, &[]);
    let datadir = PathBuf::from(f.get("datadir", "."));
    match detect_mode(&datadir) {
        Ok(det) => {
            println!("mode={}  height={}  intact={}", det.mode, det.height, det.intact);
            if !det.missing_sections.is_empty() {
                println!("  missing sections for next tier: {:?}", det.missing_sections);
            }
            0
        }
        Err(e) => {
            eprintln!("mode: {e}");
            1
        }
    }
}

fn run_status(args: &[String]) -> i32 {
    let f = Flags::parse(args, &[]);
    let datadir = PathBuf::from(f.get("datadir", "."));
    let source = f.required("source");
    let mode = f.get("mode", "archive");
    match status(&datadir, &source, &mode) {
        Ok(rep) => {
            println!("network        : {}", rep.network);
            println!("mode           : {}", rep.mode);
            println!("local height   : {}", rep.local_height);
            println!("remote height  : {}", rep.remote_height);
            println!("behind blocks  : {}", rep.behind_blocks);
            println!("up to date     : {}", rep.up_to_date);
            if !rep.note.is_empty() {
                println!("note           : {}", rep.note);
            }
            if rep.up_to_date { 0 } else { 3 }
        }
        Err(e) => {
            eprintln!("status: {e}");
            1
        }
    }
}

fn print_catch_up(rep: &CatchUpReport) {
    println!("catch-up: mode={} network={}", rep.mode, rep.network);
    println!("  start height  : {}", rep.start_height);
    println!("  final height  : {}", rep.final_height);
    println!("  remote height : {}", rep.remote_height);
    println!("  iterations    : {}", rep.iterations);
    println!("  bytes xferred : {:.2} MB", rep.total_bytes_xfer as f64 / 1024.0 / 1024.0);
    println!("  up to date    : {}", rep.up_to_date);
    for e in &rep.errors {
        println!("    {e}");
    }
}

fn run_catch_up(args: &[String]) -> i32 {
    let f = Flags::parse(args, &[]);
    let datadir = PathBuf::from(f.get("datadir", "."));
    let source = f.required("source");
    let mode = f.get("mode", "archive");
    let max_iter = f.get_usize("max-iterations", 0);
    match catch_up(&datadir, &source, &mode, max_iter) {
        Ok(rep) => {
            print_catch_up(&rep);
            if rep.up_to_date { 0 } else { 3 }
        }
        Err(e) => {
            eprintln!("catch-up: {e}");
            1
        }
    }
}

fn run_follow(args: &[String]) -> i32 {
    let f = Flags::parse(args, &[]);
    let datadir = PathBuf::from(f.get("datadir", "."));
    let source = f.required("source");
    let mode = f.get("mode", "archive");
    let interval = f.get_usize("interval-secs", 30);
    let max_cycles = f.get_usize("max-cycles", 0);
    let max_iter = f.get_usize("max-iterations", 0);
    let verify_cmd = f.get("verify-cmd", "");

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    for sig in [libc::SIGINT, libc::SIGTERM] {
        let stop = stop.clone();
        // SAFETY: the handler only stores to an atomic.
        unsafe {
            signal_hook(sig, stop);
        }
    }
    let cfg = FollowConfig {
        datadir: datadir.clone(),
        source,
        mode,
        poll_interval: std::time::Duration::from_secs(interval as u64),
        max_cycles,
        max_iter,
        stop,
        on_cycle: Some(Box::new(move |cycle, result| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or_default();
            match result {
                Ok(rep) => {
                    println!(
                        "t={now} cycle={cycle} height={} deltas_applied={}",
                        rep.final_height, rep.iterations
                    );
                    // The applied range's verification hook: every file of
                    // the delta was blake2b-verified on the way in; this is
                    // for the deeper check (e.g. a witness replay of the
                    // appended blocks) supplied by the operator.
                    if rep.iterations > 0 && !verify_cmd.is_empty() {
                        run_verify_cmd(&verify_cmd, rep, &datadir);
                    }
                }
                Err(e) => println!("t={now} cycle={cycle} ERR: {e}"),
            }
        })),
    };
    match follow(cfg) {
        Ok(rep) => {
            println!("follow:");
            println!("  cycles run     : {}", rep.cycles);
            println!("  deltas applied : {}", rep.applied_deltas);
            println!("  final height   : {}", rep.final_height);
            if !rep.last_error.is_empty() {
                println!("  last error     : {}", rep.last_error);
            }
            if rep.cancelled_clean {
                println!("  stopped via    : signal (clean)");
            }
            0
        }
        Err(e) => {
            eprintln!("follow: {e}");
            1
        }
    }
}

/// Registers a stop flag on a signal via libc, keeping the binary free
/// of an extra dependency.
unsafe fn signal_hook(sig: i32, stop: std::sync::Arc<std::sync::atomic::AtomicBool>) {
    static STOPS: std::sync::Mutex<Vec<std::sync::Arc<std::sync::atomic::AtomicBool>>> =
        std::sync::Mutex::new(Vec::new());
    extern "C" fn handler(_: i32) {
        if let Ok(stops) = STOPS.try_lock() {
            for stop in stops.iter() {
                stop.store(true, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
    STOPS.lock().unwrap().push(stop);
    // SAFETY: handler is async-signal-minimal (atomic stores behind try_lock).
    unsafe {
        libc::signal(sig, handler as usize);
    }
}

/// Runs the operator's deep-verify command with {datadir}, {from}, {to}
/// substituted; a non-zero exit is reported, not fatal.
fn run_verify_cmd(template: &str, rep: &CatchUpReport, datadir: &Path) {
    let cmd = template
        .replace("{datadir}", &datadir.display().to_string())
        .replace("{from}", &rep.start_height.to_string())
        .replace("{to}", &rep.final_height.to_string());
    println!("verify-cmd: {cmd}");
    match std::process::Command::new("sh").arg("-c").arg(&cmd).status() {
        Ok(status) if status.success() => println!("verify-cmd: OK"),
        Ok(status) => println!("verify-cmd: FAILED ({status})"),
        Err(e) => println!("verify-cmd: could not run: {e}"),
    }
}

fn print_fetch(rep: &FetchReport) {
    let prefix = if rep.dry_run { "(dry-run) " } else { "" };
    println!(
        "{prefix}mode={}  files={}  already-ok={}  downloaded={}  failed={}",
        rep.mode, rep.total_files, rep.already_ok, rep.downloaded, rep.failed
    );
    println!("  bytes xferred : {:.2} GB", gb(rep.bytes_xfer));
    println!("  bytes skipped : {:.2} GB", gb(rep.bytes_skipped));
    for e in &rep.errors {
        println!("    {e}");
    }
    println!("  result        : {}", if rep.ok { "OK" } else { "FAIL" });
}

fn run_fetch(args: &[String]) -> i32 {
    let f = Flags::parse(args, &["dry-run"]);
    let source = f.required("source");
    let datadir = PathBuf::from(f.required("datadir"));
    let mode = f.get("mode", "archive");
    match fetch(&source, &datadir, &mode, f.get_bool("dry-run"), f.get_usize("parallel", 4)) {
        Ok(rep) => {
            print_fetch(&rep);
            if rep.ok { 0 } else { 2 }
        }
        Err(e) => {
            eprintln!("fetch: {e}");
            1
        }
    }
}

fn run_upgrade(args: &[String]) -> i32 {
    let f = Flags::parse(args, &[]);
    let source = f.required("source");
    let datadir = PathBuf::from(f.required("datadir"));
    let to = f.get("to", "archive");
    match upgrade(&source, &datadir, &to, f.get_usize("parallel", 4)) {
        Ok(rep) => {
            print_fetch(&rep);
            if rep.ok { 0 } else { 2 }
        }
        Err(e) => {
            eprintln!("upgrade: {e}");
            1
        }
    }
}

fn run_downgrade(args: &[String]) -> i32 {
    let f = Flags::parse(args, &["delete-extra"]);
    let datadir = PathBuf::from(f.required("datadir"));
    let to = f.get("to", "minimal");
    match downgrade(&datadir, &to, f.get_bool("delete-extra")) {
        Ok(rep) => {
            let prefix = if rep.dry_run { "(dry-run) " } else { "" };
            println!(
                "{prefix}downgrade target={} — {} files to remove, {:.2} GB to free",
                rep.mode,
                rep.removed.len(),
                gb(rep.bytes_freed)
            );
            for p in &rep.removed {
                println!("  {p}");
            }
            0
        }
        Err(e) => {
            eprintln!("downgrade: {e}");
            1
        }
    }
}

fn run_delta(args: &[String]) -> i32 {
    let Some(sub) = args.first() else {
        eprintln!("delta requires a sub-subcommand (plan|apply)");
        return 2;
    };
    let f = Flags::parse(&args[1..], &[]);
    let source = f.required("source");
    let datadir = PathBuf::from(f.required("datadir"));
    let mode = f.get("mode", "archive");
    match sub.as_str() {
        "plan" => match plan_delta(&source, &datadir, &mode) {
            Ok((plan, _)) => {
                println!(
                    "delta plan: mode={} from={} → to={}",
                    plan.mode, plan.from_height, plan.to_height
                );
                println!("  local manifest_id    : {}", plan.local_manifest_id);
                println!("  baseline manifest_id : {}", plan.baseline_manifest_id);
                println!("  applicable           : {}", plan.applicable);
                if !plan.applicable {
                    println!("  reason               : {}", plan.reason);
                }
                println!("  files to fetch       : {}", plan.files_to_fetch);
                println!("  bytes to fetch       : {:.2} GB", gb(plan.bytes_to_fetch));
                0
            }
            Err(e) => {
                eprintln!("plan: {e}");
                1
            }
        },
        "apply" => match apply_delta(&source, &datadir, &mode, f.get_usize("parallel", 4)) {
            Ok(rep) => {
                println!("delta apply: mode={mode} from={} → to={}", rep.from_height, rep.to_height);
                println!("  files skipped     : {}", rep.skipped);
                println!("  files downloaded  : {}", rep.downloaded);
                println!("  bytes xferred     : {:.2} GB", gb(rep.bytes_xfer));
                println!("  result            : OK (manifest installed)");
                0
            }
            Err(e) => {
                eprintln!("apply: {e}");
                1
            }
        },
        other => {
            eprintln!("unknown delta sub-subcommand: {other}");
            2
        }
    }
}

fn run_manifest(args: &[String]) -> i32 {
    let f = Flags::parse(args, &["include-senders"]);
    let datadir = PathBuf::from(f.get("datadir", "."));
    let mode = f.get("mode", "archive");
    let network = f.get("network", "mainnet");
    let height: u64 = f.get("height", "0").parse().unwrap_or(0);
    let sel = match pevm::snapshot::selector_for(&mode) {
        Ok(sel) => sel,
        Err(e) => {
            eprintln!("{e}");
            return 1;
        }
    };
    let mut files = match walk_files(&datadir, &sel) {
        Ok(files) if !files.is_empty() => files,
        Ok(_) => {
            eprintln!("no files matched for mode={mode} under {}", datadir.display());
            return 1;
        }
        Err(e) => {
            eprintln!("walk files: {e}");
            return 1;
        }
    };
    if let Err(e) = pevm::snapshot::hash_all(&datadir, &mut files, f.get_usize("parallel", 0)) {
        eprintln!("hash: {e}");
        return 1;
    }
    let mut man = Manifest {
        network,
        height,
        mode: mode.clone(),
        created_at: String::new(),
        manifest_id: String::new(),
        files,
    };
    man.manifest_id = compute_manifest_id(&mut man);
    let out = PathBuf::from(f.get("out", ""));
    let out = if out.as_os_str().is_empty() {
        datadir.join(format!("manifest-{mode}.json"))
    } else {
        out
    };
    if let Err(e) = write_manifest(&out, &man) {
        eprintln!("write {}: {e}", out.display());
        return 1;
    }
    println!("wrote {}", out.display());
    println!("  files     : {}", man.files.len());
    println!("  manifestID: {}", man.manifest_id);
    0
}
