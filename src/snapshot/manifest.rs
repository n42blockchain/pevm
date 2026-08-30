//! The manifest: blake2b-256 per file, sha256 `manifest_id` over the
//! sorted `path\tsize\thash` lines. Byte-compatible with
//! `cmd/n42-eth-manifest`.

use blake2::{digest::consts::U32, Blake2b, Digest};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::io::Read;
use std::path::Path;

/// One row in `Manifest::files`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    /// Relative to the datadir, forward slashes.
    pub path: String,
    /// Logical grouping (the selector section).
    #[serde(default)]
    pub section: String,
    pub size: u64,
    pub blake2b256: String,
}

/// The on-disk `manifest-<mode>.json` shape.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Manifest {
    pub network: String,
    pub height: u64,
    pub mode: String,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub manifest_id: String,
    pub files: Vec<FileEntry>,
}

/// Reads `manifest-<mode>.json` under a datadir.
pub fn manifest_for(datadir: &Path, mode: &str) -> std::io::Result<Manifest> {
    read_manifest(&datadir.join(format!("manifest-{mode}.json")))
}

/// Parses any manifest JSON file from disk.
pub fn read_manifest(path: &Path) -> std::io::Result<Manifest> {
    let data = std::fs::read(path)?;
    serde_json::from_slice(&data)
        .map_err(|e| std::io::Error::other(format!("decode {}: {e}", path.display())))
}

/// Writes a manifest as pretty JSON via tmp + rename.
pub fn write_manifest(path: &Path, m: &Manifest) -> std::io::Result<()> {
    let tmp = path.with_extension("json.tmp");
    let mut data = serde_json::to_vec_pretty(m).map_err(std::io::Error::other)?;
    data.push(b'\n');
    std::fs::write(&tmp, data)?;
    std::fs::rename(&tmp, path)
}

/// blake2b-256 of a file, streamed.
pub fn hash_file(path: &Path) -> std::io::Result<String> {
    let mut f = std::fs::File::open(path)?;
    let mut hasher = Blake2b::<U32>::new();
    let mut buf = vec![0u8; 4 << 20];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Sorts the files by path and hashes `path\tsize\thash\n` per entry
/// with sha256 — the id the publisher's index and every delta baseline
/// pointer use.
pub fn compute_manifest_id(m: &mut Manifest) -> String {
    m.files.sort_by(|a, b| a.path.cmp(&b.path));
    let mut hasher = Sha256::new();
    for f in &m.files {
        hasher.update(format!("{}\t{}\t{}\n", f.path, f.size, f.blake2b256));
    }
    hex::encode(hasher.finalize())
}

/// Hashes every entry in place, in parallel.
pub fn hash_all(root: &Path, files: &mut [FileEntry], workers: usize) -> std::io::Result<()> {
    let workers = if workers == 0 {
        (std::thread::available_parallelism().map_or(1, |n| n.get()) / 2).max(1)
    } else {
        workers
    };
    let next = std::sync::atomic::AtomicUsize::new(0);
    let error = std::sync::Mutex::new(None);
    let hashes: Vec<std::sync::Mutex<String>> =
        files.iter().map(|_| std::sync::Mutex::new(String::new())).collect();
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(entry) = files.get(i) else { return };
                match hash_file(&root.join(&entry.path)) {
                    Ok(h) => *hashes[i].lock().unwrap() = h,
                    Err(e) => {
                        error.lock().unwrap().get_or_insert(e);
                        return;
                    }
                }
            });
        }
    });
    if let Some(e) = error.into_inner().unwrap() {
        return Err(e);
    }
    for (entry, hash) in files.iter_mut().zip(hashes) {
        entry.blake2b256 = hash.into_inner().unwrap();
    }
    Ok(())
}
