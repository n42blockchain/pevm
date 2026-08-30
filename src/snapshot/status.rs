//! "Am I behind, and by how much?" against the publisher's
//! `releases.json`.

use super::manifest::manifest_for;
use super::source::open_source;
use serde::Deserialize;
use std::path::Path;

/// One release reference in `releases.json` — the shape
/// `n42-eth-publish` writes.
#[derive(Debug, Clone, Default, Deserialize)]
pub(super) struct RemoteReleaseRef {
    pub height: u64,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub manifests: std::collections::HashMap<String, String>,
}

/// One delta reference in `releases.json`.
#[derive(Debug, Clone, Default, Deserialize)]
pub(super) struct RemoteDeltaRef {
    pub from_height: u64,
    pub to_height: u64,
    pub mode: String,
    #[serde(default)]
    pub manifest_id: String,
    #[serde(default)]
    pub created_at: String,
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct RemoteIndex {
    #[serde(default)]
    pub network: String,
    #[serde(default)]
    pub latest: std::collections::HashMap<String, RemoteReleaseRef>,
    #[serde(default)]
    pub deltas: Vec<RemoteDeltaRef>,
}

pub(super) fn fetch_index(source: &str) -> std::io::Result<RemoteIndex> {
    let src = open_source(source)?;
    let mut rc = src
        .open("releases.json")
        .map_err(|e| std::io::Error::other(format!("fetch releases.json: {e}")))?;
    let mut data = Vec::new();
    std::io::Read::read_to_end(&mut rc, &mut data)?;
    serde_json::from_slice(&data)
        .map_err(|e| std::io::Error::other(format!("decode releases.json: {e}")))
}

/// The comparison of local height to the publisher's latest.
#[derive(Debug, Default)]
pub struct StatusReport {
    pub network: String,
    pub mode: String,
    pub local_height: u64,
    pub remote_height: u64,
    pub behind_blocks: u64,
    pub up_to_date: bool,
    pub local_manifest_id: String,
    pub remote_manifest_id: String,
    /// One-line operator hint for the edge cases.
    pub note: String,
}

/// Fetches the publisher's index from `source` (the per-network root
/// where `releases.json` lives) and compares its latest for `mode`
/// against the local manifest. A missing local manifest is a soft
/// condition; an unreachable or modeless publisher is an error.
pub fn status(datadir: &Path, source: &str, mode: &str) -> std::io::Result<StatusReport> {
    let idx = fetch_index(source)?;
    let latest = idx
        .latest
        .get(mode)
        .ok_or_else(|| std::io::Error::other(format!("publisher has no latest entry for mode {mode:?}")))?;
    let mut rep = StatusReport {
        network: idx.network.clone(),
        mode: mode.into(),
        remote_height: latest.height,
        remote_manifest_id: latest.manifests.get(mode).cloned().unwrap_or_default(),
        ..Default::default()
    };
    let local = match manifest_for(datadir, mode) {
        Ok(local) => local,
        Err(_) => {
            rep.note = format!(
                "no local manifest-{mode}.json in {} — bootstrap needed",
                datadir.display()
            );
            rep.behind_blocks = rep.remote_height;
            return Ok(rep);
        }
    };
    rep.local_height = local.height;
    rep.local_manifest_id = local.manifest_id;
    if rep.local_height > rep.remote_height {
        rep.note = format!(
            "local is AHEAD of publisher by {} blocks (publisher may be behind)",
            rep.local_height - rep.remote_height
        );
    } else if rep.local_height == rep.remote_height {
        if rep.local_manifest_id != rep.remote_manifest_id {
            rep.note =
                "heights match but manifest_ids differ — file-level corruption suspected; run verify"
                    .into();
        } else {
            rep.up_to_date = true;
        }
    } else {
        rep.behind_blocks = rep.remote_height - rep.local_height;
    }
    Ok(rep)
}
