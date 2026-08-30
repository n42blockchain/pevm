//! Fetch a whole tier, upgrade to a higher one, downgrade below one.

use super::manifest::{hash_file, manifest_for, FileEntry};
use super::source::{open_source, Source};
use std::io::Write;
use std::path::Path;

/// What `fetch`/`upgrade` did.
#[derive(Debug, Default)]
pub struct FetchReport {
    pub mode: String,
    pub total_files: usize,
    pub already_ok: usize,
    pub downloaded: usize,
    pub failed: usize,
    pub bytes_xfer: u64,
    pub bytes_skipped: u64,
    pub errors: Vec<String>,
    pub dry_run: bool,
    pub ok: bool,
}

fn local_file_ok(full: &Path, want: &FileEntry) -> bool {
    matches!(std::fs::metadata(full), Ok(meta) if meta.len() == want.size)
        && matches!(hash_file(full), Ok(got) if got == want.blake2b256)
}

/// Copies every file in the target manifest from `source` into
/// `datadir`. A file already present with the right size and blake2b is
/// skipped; each download is hash-verified in a `.tmp` before rename.
pub fn fetch(
    source: &str,
    datadir: &Path,
    mode: &str,
    dry_run: bool,
    parallel: usize,
) -> std::io::Result<FetchReport> {
    let src = open_source(source)?;
    let man_rel = format!("manifest-{mode}.json");
    std::fs::create_dir_all(datadir)?;
    // Always (re-)fetch the manifest itself first, unverified — it is
    // what the verification of everything else comes from.
    fetch_one_file(&src, &man_rel, &datadir.join(&man_rel))?;
    let m = manifest_for(datadir, mode)?;

    let mut rep = FetchReport {
        mode: mode.into(),
        total_files: m.files.len(),
        dry_run,
        ok: true,
        ..Default::default()
    };

    let mut to_fetch = Vec::new();
    for f in &m.files {
        if local_file_ok(&datadir.join(&f.path), f) {
            rep.already_ok += 1;
            rep.bytes_skipped += f.size;
        } else {
            to_fetch.push(f);
        }
    }
    if dry_run {
        return Ok(rep);
    }

    let (done, bytes, errors) = fetch_parallel(&src, datadir, &to_fetch, parallel);
    rep.downloaded = done;
    rep.bytes_xfer = bytes;
    rep.failed = errors.len();
    rep.errors = errors;
    rep.ok = rep.failed == 0;
    Ok(rep)
}

pub(super) fn fetch_parallel(
    src: &Source,
    datadir: &Path,
    files: &[&FileEntry],
    parallel: usize,
) -> (usize, u64, Vec<String>) {
    let parallel = if parallel == 0 { 4 } else { parallel };
    let next = std::sync::atomic::AtomicUsize::new(0);
    let state = std::sync::Mutex::new((0usize, 0u64, Vec::new()));
    std::thread::scope(|scope| {
        for _ in 0..parallel {
            scope.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(entry) = files.get(i) else { return };
                let dst = datadir.join(&entry.path);
                match fetch_and_verify(src, &entry.path, &dst, &entry.blake2b256, entry.size) {
                    Ok(n) => {
                        let mut s = state.lock().unwrap();
                        s.0 += 1;
                        s.1 += n;
                    }
                    Err(e) => {
                        state.lock().unwrap().2.push(format!("{}: {e}", entry.path));
                    }
                }
            });
        }
    });
    state.into_inner().unwrap()
}

/// Copies source→dst without verification; only for the manifest itself.
fn fetch_one_file(src: &Source, rel: &str, dst: &Path) -> std::io::Result<()> {
    let mut rc = src.open(rel)?;
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = tmp_path(dst);
    let mut f = std::fs::File::create(&tmp)?;
    if let Err(e) = std::io::copy(&mut rc, &mut f).and_then(|_| f.flush()) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    drop(f);
    std::fs::rename(&tmp, dst)
}

fn tmp_path(dst: &Path) -> std::path::PathBuf {
    let mut s = dst.as_os_str().to_owned();
    s.push(".tmp");
    std::path::PathBuf::from(s)
}

/// Downloads, hashes, and only renames into place when the hash and
/// size match. Returns bytes transferred.
pub(super) fn fetch_and_verify(
    src: &Source,
    rel: &str,
    dst: &Path,
    want_hash: &str,
    want_size: u64,
) -> std::io::Result<u64> {
    let mut rc = src.open(rel)?;
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = tmp_path(dst);
    let n = {
        let mut f = std::fs::File::create(&tmp)?;
        std::io::copy(&mut rc, &mut f).inspect_err(|_| {
            let _ = std::fs::remove_file(&tmp);
        })?
    };
    let fail = |msg: String| {
        let _ = std::fs::remove_file(&tmp);
        Err(std::io::Error::other(msg))
    };
    if want_size > 0 && n != want_size {
        return fail(format!("size mismatch: got {n}, want {want_size}"));
    }
    match hash_file(&tmp) {
        Ok(got) if got == want_hash => {}
        Ok(_) => return fail("blake2b mismatch after download".into()),
        Err(e) => return fail(e.to_string()),
    }
    std::fs::rename(&tmp, dst)?;
    Ok(n)
}

/// Fetches whatever is missing for the target mode, leaving existing
/// higher-tier files in place.
pub fn upgrade(source: &str, datadir: &Path, to: &str, parallel: usize) -> std::io::Result<FetchReport> {
    fetch(source, datadir, to, false, parallel)
}

/// Files removed (or to be removed, in dry-run).
#[derive(Debug, Default)]
pub struct DowngradeReport {
    pub mode: String,
    pub removed: Vec<String>,
    pub bytes_freed: u64,
    pub dry_run: bool,
}

/// Removes files under `chain/freezer/` and `snapshot/` that the target
/// mode's manifest does not reference; never touches anything else.
pub fn downgrade(datadir: &Path, to: &str, do_delete: bool) -> std::io::Result<DowngradeReport> {
    let want = manifest_for(datadir, to)?;
    let keep: std::collections::HashSet<&str> =
        want.files.iter().map(|f| f.path.as_str()).collect();
    let mut rep = DowngradeReport { mode: to.into(), dry_run: !do_delete, ..Default::default() };
    for sub in ["chain/freezer", "snapshot"] {
        walk_removals(datadir, &datadir.join(sub), &keep, do_delete, &mut rep)?;
    }
    Ok(rep)
}

fn walk_removals(
    datadir: &Path,
    dir: &Path,
    keep: &std::collections::HashSet<&str>,
    do_delete: bool,
    rep: &mut DowngradeReport,
) -> std::io::Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return Ok(()),
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            walk_removals(datadir, &path, keep, do_delete, rep)?;
            continue;
        }
        let rel = path
            .strip_prefix(datadir)
            .map_err(std::io::Error::other)?
            .to_string_lossy()
            .replace('\\', "/");
        if keep.contains(rel.as_str()) {
            continue;
        }
        if let Ok(meta) = entry.metadata() {
            rep.bytes_freed += meta.len();
        }
        rep.removed.push(rel);
        if do_delete {
            let _ = std::fs::remove_file(&path);
        }
    }
    Ok(())
}
