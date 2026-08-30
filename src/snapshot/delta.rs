//! Incremental deltas: `delta-manifest-<mode>.json` plus the changed
//! files, applied only onto the exact baseline they were built from.

use super::fetch::{fetch_and_verify, fetch_parallel};
use super::manifest::{hash_file, manifest_for, FileEntry};
use super::source::open_source;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// The schema of `delta-manifest-<mode>.json` (the publisher's
/// delta-build output).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeltaManifest {
    pub network: String,
    pub from_height: u64,
    pub to_height: u64,
    pub mode: String,
    pub based_on_manifest_id: String,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub manifest_id: String,
    pub files: Vec<FileEntry>,
}

/// What a `delta apply` would do.
#[derive(Debug, Default)]
pub struct DeltaPlan {
    pub source: String,
    pub datadir: String,
    pub mode: String,
    pub local_manifest_id: String,
    pub baseline_manifest_id: String,
    pub from_height: u64,
    pub to_height: u64,
    pub files_to_fetch: usize,
    pub bytes_to_fetch: u64,
    /// The local datadir matches the delta's baseline.
    pub applicable: bool,
    /// Why not, when it is not.
    pub reason: String,
}

/// Fetches the delta manifest and reports whether it applies to the
/// local datadir, plus the size of the work.
pub fn plan_delta(source: &str, datadir: &Path, mode: &str) -> std::io::Result<(DeltaPlan, DeltaManifest)> {
    let src = open_source(source)?;
    let delta_rel = format!("delta-manifest-{mode}.json");
    let mut rc = src.open(&delta_rel)?;
    let mut data = Vec::new();
    std::io::Read::read_to_end(&mut rc, &mut data)?;
    let dm: DeltaManifest = serde_json::from_slice(&data)
        .map_err(|e| std::io::Error::other(format!("decode {delta_rel}: {e}")))?;
    if dm.mode != mode {
        return Err(std::io::Error::other(format!(
            "delta manifest mode={}, expected {mode}",
            dm.mode
        )));
    }

    let mut plan = DeltaPlan {
        source: source.into(),
        datadir: datadir.display().to_string(),
        mode: mode.into(),
        baseline_manifest_id: dm.based_on_manifest_id.clone(),
        from_height: dm.from_height,
        to_height: dm.to_height,
        ..Default::default()
    };
    let local = match manifest_for(datadir, mode) {
        Ok(local) => local,
        Err(e) => {
            plan.reason = format!("no local manifest-{mode}.json: {e}");
            return Ok((plan, dm));
        }
    };
    plan.local_manifest_id = local.manifest_id.clone();
    if local.manifest_id != dm.based_on_manifest_id {
        plan.reason = format!(
            "local manifest_id {} ≠ delta baseline {}",
            local.manifest_id, dm.based_on_manifest_id
        );
        return Ok((plan, dm));
    }
    plan.applicable = true;
    for f in &dm.files {
        let full = datadir.join(&f.path);
        let already = matches!(std::fs::metadata(&full), Ok(meta) if meta.len() == f.size)
            && matches!(hash_file(&full), Ok(got) if got == f.blake2b256);
        if !already {
            plan.files_to_fetch += 1;
            plan.bytes_to_fetch += f.size;
        }
    }
    Ok((plan, dm))
}

/// What `apply_delta` did.
#[derive(Debug, Default)]
pub struct DeltaApplyReport {
    pub plan: DeltaPlan,
    pub from_height: u64,
    pub to_height: u64,
    pub skipped: usize,
    pub downloaded: usize,
    pub failed: usize,
    pub bytes_xfer: u64,
    pub errors: Vec<String>,
    pub ok: bool,
}

/// Downloads each delta file, verifies its blake2b, and installs the
/// publisher's full target manifest at the end — refusing outright when
/// the local baseline does not match.
pub fn apply_delta(
    source: &str,
    datadir: &Path,
    mode: &str,
    parallel: usize,
) -> std::io::Result<DeltaApplyReport> {
    let (plan, dm) = plan_delta(source, datadir, mode)?;
    let mut rep = DeltaApplyReport {
        from_height: dm.from_height,
        to_height: dm.to_height,
        ..Default::default()
    };
    if !plan.applicable {
        let reason = plan.reason.clone();
        rep.plan = plan;
        return Err(std::io::Error::other(format!("delta not applicable: {reason}")));
    }
    rep.plan = plan;

    let src = open_source(source)?;
    let mut to_fetch = Vec::new();
    for f in &dm.files {
        let full = datadir.join(&f.path);
        let already = matches!(std::fs::metadata(&full), Ok(meta) if meta.len() == f.size)
            && matches!(hash_file(&full), Ok(got) if got == f.blake2b256);
        if already {
            rep.skipped += 1;
        } else {
            to_fetch.push(f);
        }
    }
    let (done, bytes, errors) = fetch_parallel(&src, datadir, &to_fetch, parallel);
    rep.downloaded = done;
    rep.bytes_xfer = bytes;
    rep.failed = errors.len();
    rep.errors = errors;
    if rep.failed > 0 {
        return Err(std::io::Error::other(format!("{} files failed", rep.failed)));
    }

    // Install the new full manifest, fetched verbatim from the same
    // delta directory (the publisher stages it there).
    let man_rel = format!("manifest-{mode}.json");
    let dst = datadir.join(&man_rel);
    fetch_and_verify(&src, &man_rel, &dst, "", 0).or_else(|_| {
        // The manifest has no hash of itself to check against; copy raw.
        let mut rc = src.open(&man_rel)?;
        let mut data = Vec::new();
        std::io::Read::read_to_end(&mut rc, &mut data)?;
        std::fs::write(&dst, data).map(|()| 0)
    })?;
    rep.ok = true;
    Ok(rep)
}
