//! Re-hash every file a manifest lists and report what disagrees.

use super::manifest::{hash_file, manifest_for, read_manifest, Manifest};
use super::selector::detect_mode;
use std::path::Path;

/// The outcome of `verify`.
#[derive(Debug, Default)]
pub struct VerifyReport {
    pub manifest_path: String,
    pub mode: String,
    pub height: u64,
    pub files_checked: usize,
    pub mismatches: Vec<String>,
    pub missing_files: Vec<String>,
    pub wrong_size: Vec<String>,
    pub ok: bool,
}

/// Walks the manifest's files, re-hashes each, and reports mismatches,
/// missing files and size discrepancies. An empty `manifest_path`
/// auto-detects the maximal mode under the datadir.
pub fn verify(datadir: &Path, manifest_path: &str, workers: usize) -> std::io::Result<VerifyReport> {
    let (path, m): (String, Manifest) = if manifest_path.is_empty() {
        let det = detect_mode(datadir)?;
        if det.mode.is_empty() {
            return Err(std::io::Error::other(
                "verify: no manifest found and no mode detected in datadir",
            ));
        }
        let p = datadir.join(format!("manifest-{}.json", det.mode));
        (p.display().to_string(), manifest_for(datadir, &det.mode)?)
    } else {
        (manifest_path.to_string(), read_manifest(Path::new(manifest_path))?)
    };

    let mut rep = VerifyReport {
        manifest_path: path,
        mode: m.mode.clone(),
        height: m.height,
        files_checked: m.files.len(),
        ok: true,
        ..Default::default()
    };

    // Stat pass first, then the hash pass over what remains.
    let mut to_hash = Vec::new();
    for f in &m.files {
        let full = datadir.join(&f.path);
        match std::fs::metadata(&full) {
            Err(_) => {
                rep.missing_files.push(f.path.clone());
                rep.ok = false;
            }
            Ok(meta) if meta.len() != f.size => {
                rep.wrong_size
                    .push(format!("{} (got {}, want {})", f.path, meta.len(), f.size));
                rep.ok = false;
            }
            Ok(_) => to_hash.push(f),
        }
    }

    let workers = if workers == 0 { 4 } else { workers };
    let next = std::sync::atomic::AtomicUsize::new(0);
    let results: Vec<std::sync::Mutex<Option<std::io::Result<String>>>> =
        to_hash.iter().map(|_| std::sync::Mutex::new(None)).collect();
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(entry) = to_hash.get(i) else { return };
                *results[i].lock().unwrap() = Some(hash_file(&datadir.join(&entry.path)));
            });
        }
    });
    for (entry, result) in to_hash.iter().zip(results) {
        match result.into_inner().unwrap().expect("every job ran") {
            Ok(got) if got == entry.blake2b256 => {}
            Ok(_) => {
                rep.mismatches.push(format!("{} — blake2b mismatch", entry.path));
                rep.ok = false;
            }
            Err(e) => {
                rep.mismatches.push(format!("{} — hash error: {e}", entry.path));
                rep.ok = false;
            }
        }
    }
    Ok(rep)
}
