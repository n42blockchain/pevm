// SPDX-License-Identifier: MIT OR Apache-2.0
//! Ahead-of-time compilation of the hottest contracts with revmc, and the
//! directory-backed artifact store that carries the machine code from the
//! build to the replay.
//!
//! revmc's own store lives in a temporary directory, so nothing survives the
//! process. This one keeps every artifact as `<code hash>_<spec>.so` next to
//! a `.json` manifest, and the backend loads them through the same
//! `prepare_aot` path it compiles on: with the artifact present the job is a
//! `dlopen`, not a compilation.

use super::geth_freezer::GethBlockSource;
use alloy_primitives::{keccak256, B256};
use eyre::{Context, Result};
use reth_chainspec::ChainSpec;
use revmc::runtime::{
    AotRequest, ArtifactKey, ArtifactManifest, ArtifactStore, BackendSelection, JitBackend,
    RuntimeCacheKey, StoredArtifact,
};
use revmc::OptimizationLevel;
use std::{
    collections::{BTreeSet, HashMap},
    fs,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tracing::info;

/// Artifacts on disk, one `.so` and one `.json` per (code hash, spec).
pub(super) struct DirArtifactStore {
    dir: PathBuf,
    index: Mutex<HashMap<ArtifactKey, StoredArtifact>>,
}

impl std::fmt::Debug for DirArtifactStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("DirArtifactStore").field("dir", &self.dir).finish()
    }
}

fn spec_name(spec: revmc::primitives::hardfork::SpecId) -> String {
    format!("{spec:?}")
}

impl DirArtifactStore {
    pub(super) fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir).wrap_err_with(|| format!("failed to create {}", dir.display()))?;
        let mut index = HashMap::new();
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let text = fs::read_to_string(&path)?;
            let value: serde_json::Value = serde_json::from_str(&text)
                .wrap_err_with(|| format!("bad manifest {}", path.display()))?;
            let get = |k: &str| value.get(k).cloned().unwrap_or(serde_json::Value::Null);
            let code_hash = B256::from_str(get("code_hash").as_str().unwrap_or_default())?;
            let spec_id = revmc::primitives::hardfork::SpecId::from_str(
                get("spec_id").as_str().unwrap_or_default(),
            )
            .map_err(|_| eyre::eyre!("unknown spec in {}", path.display()))?;
            let key = ArtifactKey {
                runtime: RuntimeCacheKey { code_hash, spec_id },
                backend: BackendSelection::Llvm,
                opt_level: OptimizationLevel::Default,
            };
            let dylib_path = path.with_extension("so");
            if !dylib_path.exists() {
                continue;
            }
            let mut content_hash = [0u8; 32];
            if let Some(hex) = get("content_hash").as_str() {
                content_hash.copy_from_slice(B256::from_str(hex)?.as_slice());
            }
            let manifest = ArtifactManifest {
                artifact_key: key.clone(),
                symbol_name: get("symbol_name").as_str().unwrap_or("main").to_string(),
                bytecode_len: get("bytecode_len").as_u64().unwrap_or(0) as usize,
                artifact_len: get("artifact_len").as_u64().unwrap_or(0) as usize,
                created_at_unix_secs: get("created_at_unix_secs").as_u64().unwrap_or(0),
                content_hash,
            };
            index.insert(key, StoredArtifact { manifest, dylib_path });
        }
        Ok(Self { dir: dir.to_path_buf(), index: Mutex::new(index) })
    }

    fn stem(&self, key: &ArtifactKey) -> PathBuf {
        self.dir.join(format!("{:x}_{}", key.runtime.code_hash, spec_name(key.runtime.spec_id)))
    }

    /// Every key the store holds.
    pub(super) fn keys(&self) -> Vec<RuntimeCacheKey> {
        self.index.lock().unwrap().keys().map(|k| k.runtime).collect()
    }

    /// Count and total bytes of the artifacts.
    pub(super) fn size(&self) -> (usize, u64) {
        let index = self.index.lock().unwrap();
        (index.len(), index.values().map(|a| a.manifest.artifact_len as u64).sum())
    }
}

impl ArtifactStore for DirArtifactStore {
    fn load_all(&self) -> eyre::Result<Vec<(ArtifactKey, StoredArtifact)>> {
        Ok(self.index.lock().unwrap().iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    fn load(&self, key: &ArtifactKey) -> eyre::Result<Option<StoredArtifact>> {
        Ok(self.index.lock().unwrap().get(key).cloned())
    }

    fn store(
        &self,
        key: &ArtifactKey,
        manifest: &ArtifactManifest,
        dylib_bytes: &[u8],
    ) -> eyre::Result<()> {
        let stem = self.stem(key);
        let dylib_path = stem.with_extension("so");
        fs::write(&dylib_path, dylib_bytes)?;
        let json = serde_json::json!({
            "code_hash": format!("{:?}", key.runtime.code_hash),
            "spec_id": spec_name(key.runtime.spec_id),
            "symbol_name": manifest.symbol_name,
            "bytecode_len": manifest.bytecode_len,
            "artifact_len": dylib_bytes.len(),
            "created_at_unix_secs": manifest.created_at_unix_secs,
            "content_hash": format!("{:?}", B256::from(keccak256(dylib_bytes).0)),
        });
        fs::write(stem.with_extension("json"), serde_json::to_string_pretty(&json)?)?;
        let mut manifest = manifest.clone();
        manifest.artifact_len = dylib_bytes.len();
        self.index.lock().unwrap().insert(key.clone(), StoredArtifact { manifest, dylib_path });
        Ok(())
    }

    fn delete(&self, key: &ArtifactKey) -> eyre::Result<()> {
        if let Some(artifact) = self.index.lock().unwrap().remove(key) {
            let _ = fs::remove_file(&artifact.dylib_path);
            let _ = fs::remove_file(artifact.dylib_path.with_extension("json"));
        }
        Ok(())
    }

    fn clear(&self) -> eyre::Result<()> {
        let keys: Vec<ArtifactKey> = self.index.lock().unwrap().keys().cloned().collect();
        for key in keys {
            self.delete(&key)?;
        }
        Ok(())
    }
}

/// The spec in force at each block, from the chain's forks and the block
/// timestamps in the ancient store; only the boundaries are kept.
pub(super) struct SpecTimeline {
    /// `(first block, spec)`, ascending.
    boundaries: Vec<(u64, revmc::primitives::hardfork::SpecId)>,
}

impl SpecTimeline {
    pub(super) fn build(chain_spec: &ChainSpec, blocks: &GethBlockSource, last: u64) -> Result<Self> {
        let spec_of = |number: u64| -> Result<revmc::primitives::hardfork::SpecId> {
            let timestamp = blocks.timestamp(number)?;
            Ok(reth_evm_ethereum::revm_spec_by_timestamp_and_block_number(chain_spec, timestamp, number))
        };
        let mut boundaries = vec![(0u64, spec_of(0)?)];
        loop {
            let (start, current) = *boundaries.last().unwrap();
            if spec_of(last)? == current {
                break;
            }
            // First block after `start` whose spec differs: specs only go up.
            let (mut low, mut high) = (start + 1, last);
            while low < high {
                let middle = low + (high - low) / 2;
                if spec_of(middle)? == current {
                    low = middle + 1;
                } else {
                    high = middle;
                }
            }
            boundaries.push((low, spec_of(low)?));
        }
        info!(?boundaries, "spec timeline");
        Ok(Self { boundaries })
    }

    /// Specs in force anywhere in `first..=last`.
    pub(super) fn specs_over(&self, first: u64, last: u64) -> BTreeSet<revmc::primitives::hardfork::SpecId> {
        let mut out = BTreeSet::new();
        for (i, &(start, spec)) in self.boundaries.iter().enumerate() {
            let end = self.boundaries.get(i + 1).map(|b| b.0 - 1).unwrap_or(u64::MAX);
            if start <= last && end >= first {
                out.insert(spec);
            }
        }
        out
    }
}

/// One contract from the heat table.
pub(super) struct HotContract {
    pub(super) code_hash: B256,
    pub(super) first_block: u64,
    pub(super) last_block: u64,
}

/// Reads the first `top` rows of a contract-heat CSV, merged by code hash.
pub(super) fn read_heat(path: &Path, top: usize) -> Result<Vec<HotContract>> {
    let text = std::io::BufReader::new(fs::File::open(path)?);
    let mut by_hash: HashMap<B256, HotContract> = HashMap::new();
    let mut order = Vec::new();
    use std::io::BufRead;
    for (i, line) in text.lines().enumerate() {
        if i == 0 {
            continue;
        }
        if i > top {
            break;
        }
        let line = line?;
        let fields: Vec<&str> = line.split(',').collect();
        if fields.len() < 7 || fields[2].is_empty() {
            continue;
        }
        let code_hash = B256::from_str(fields[2])?;
        let first: u64 = fields[5].parse()?;
        let last: u64 = fields[6].parse()?;
        match by_hash.get_mut(&code_hash) {
            Some(entry) => {
                entry.first_block = entry.first_block.min(first);
                entry.last_block = entry.last_block.max(last);
            }
            None => {
                by_hash.insert(code_hash, HotContract { code_hash, first_block: first, last_block: last });
                order.push(code_hash);
            }
        }
    }
    Ok(order.into_iter().filter_map(|h| by_hash.remove(&h)).collect())
}

/// Hands `requests` to the backend and waits until every one has been
/// compiled, loaded from the store, or failed.
pub(super) fn drive(backend: &JitBackend, requests: Vec<AotRequest>, what: &str) -> Result<()> {
    let total = requests.len() as u64;
    let before = backend.stats();
    let baseline = before.compilations_succeeded + before.compilations_failed;
    let started = Instant::now();
    for chunk in requests.chunks(256) {
        backend.prepare_aot_batch(chunk.to_vec());
    }
    let mut last_report = Instant::now();
    loop {
        let stats = backend.stats();
        let done = stats.compilations_succeeded + stats.compilations_failed - baseline;
        if done >= total && stats.pending_jobs == 0 {
            info!(
                what,
                total,
                succeeded = stats.compilations_succeeded - before.compilations_succeeded,
                failed = stats.compilations_failed - before.compilations_failed,
                resident = stats.resident_entries,
                code_mib = stats.jit_code_bytes / (1024 * 1024),
                elapsed = ?started.elapsed(),
                "AOT done"
            );
            return Ok(());
        }
        if last_report.elapsed() > Duration::from_secs(10) {
            info!(what, done, total, pending = stats.pending_jobs, resident = stats.resident_entries, "AOT progress");
            last_report = Instant::now();
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Requests for every (contract, spec) pair the timeline says is needed.
pub(super) fn requests_for(
    contracts: &[HotContract],
    timeline: &SpecTimeline,
    code_for: impl Fn(B256) -> Option<alloy_primitives::Bytes>,
) -> (Vec<AotRequest>, usize) {
    let mut requests = Vec::new();
    let mut missing = 0usize;
    for contract in contracts {
        let Some(code) = code_for(contract.code_hash) else {
            missing += 1;
            continue;
        };
        for spec in timeline.specs_over(contract.first_block, contract.last_block) {
            requests.push(AotRequest { code_hash: contract.code_hash, code: code.clone(), spec_id: spec });
        }
    }
    (requests, missing)
}

/// Requests that only load what the store already holds (no compilation).
pub(super) fn requests_from_store(
    store: &DirArtifactStore,
    code_for: impl Fn(B256) -> Option<alloy_primitives::Bytes>,
) -> Vec<AotRequest> {
    store
        .keys()
        .into_iter()
        .filter_map(|key| {
            code_for(key.code_hash).map(|code| AotRequest { code_hash: key.code_hash, code, spec_id: key.spec_id })
        })
        .collect()
}

#[allow(dead_code)]
pub(super) fn shared(store: DirArtifactStore) -> Arc<DirArtifactStore> {
    Arc::new(store)
}
