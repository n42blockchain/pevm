//! The gov5 client's test suite, ported: the fixtures are the mirror
//! layout `n42-eth-publish` writes, and the assertions are the Go
//! side's.

use super::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Self {
        static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pevm-snapshot-test-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn touch_fake_archive(extras: &[&str]) -> TempDir {
    let dir = TempDir::new();
    let base = [
        "chain/freezer/headerc.cidx",
        "chain/freezer/headerc.0000.cdat",
        "chain/freezer/codes.cidx",
        "chain/freezer/codes.0000.cdat",
        "snapshot/accounts.0-25099999.idx",
        "snapshot/accounts.0-25099999.ef",
        "snapshot/accounts.0-25099999.val.zst",
        "snapshot/storage.0-25099999.idx",
        "snapshot/storage.0-25099999.ef",
        "snapshot/storage.0-25099999.val.zst",
    ];
    for p in base.iter().chain(extras) {
        let full = dir.path().join(p);
        std::fs::create_dir_all(full.parent().unwrap()).unwrap();
        std::fs::write(&full, format!("stub-{p}")).unwrap();
    }
    dir
}

fn write_fake_manifest(dir: &Path, mode: &str, height: u64) {
    let sel = selector_for(mode).unwrap();
    let mut files = walk_files(dir, &sel).unwrap();
    for f in &mut files {
        f.blake2b256 = hash_file(&dir.join(&f.path)).unwrap();
    }
    let mut man = Manifest {
        network: "test".into(),
        height,
        mode: mode.into(),
        files,
        ..Default::default()
    };
    man.manifest_id = compute_manifest_id(&mut man);
    write_manifest(&dir.join(format!("manifest-{mode}.json")), &man).unwrap();
}

/// The mirror layout n42-eth-publish produces:
/// `<mirror>/<network>/<height>/<mode>/<files>` + `releases.json`.
fn publish_fake_mirror(src: &Path, mirror: &Path, network: &str, mode: &str) {
    let man = manifest_for(src, mode).unwrap();
    let dst_dir = mirror.join(network).join(man.height.to_string()).join(mode);
    for f in &man.files {
        let dst = dst_dir.join(&f.path);
        std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
        std::fs::copy(src.join(&f.path), dst).unwrap();
    }
    std::fs::copy(
        src.join(format!("manifest-{mode}.json")),
        dst_dir.join(format!("manifest-{mode}.json")),
    )
    .unwrap();

    let idx_path = mirror.join(network).join("releases.json");
    let mut idx: serde_json::Value = std::fs::read(&idx_path)
        .ok()
        .and_then(|d| serde_json::from_slice(&d).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    idx["network"] = network.into();
    let entry = serde_json::json!({
        "height": man.height,
        "manifests": { mode: man.manifest_id },
        "created_at": man.created_at,
    });
    let latest = idx["latest"]
        .as_object()
        .and_then(|l| l.get(mode))
        .and_then(|r| r["height"].as_u64())
        .unwrap_or(0);
    if latest < man.height {
        if !idx["latest"].is_object() {
            idx["latest"] = serde_json::json!({});
        }
        idx["latest"][mode] = entry.clone();
    }
    if !idx["releases"].is_array() {
        idx["releases"] = serde_json::json!([]);
    }
    idx["releases"].as_array_mut().unwrap().push(entry);
    std::fs::create_dir_all(idx_path.parent().unwrap()).unwrap();
    std::fs::write(&idx_path, serde_json::to_vec_pretty(&idx).unwrap()).unwrap();
}

fn append_delta_to_mirror(mirror: &Path, network: &str, mode: &str, from: u64, to: u64, mid: &str) {
    let idx_path = mirror.join(network).join("releases.json");
    let mut idx: serde_json::Value = std::fs::read(&idx_path)
        .ok()
        .and_then(|d| serde_json::from_slice(&d).ok())
        .unwrap_or_else(|| serde_json::json!({"network": network}));
    if !idx["deltas"].is_array() {
        idx["deltas"] = serde_json::json!([]);
    }
    idx["deltas"].as_array_mut().unwrap().push(serde_json::json!({
        "from_height": from, "to_height": to, "mode": mode, "manifest_id": mid,
    }));
    std::fs::write(&idx_path, serde_json::to_vec_pretty(&idx).unwrap()).unwrap();
}

/// A thin in-process port of the delta builder, as in the Go tests.
fn build_delta_tree(from_arch: &Path, to_arch: &Path, mode: &str, out: &Path) {
    let from_man = manifest_for(from_arch, mode).unwrap();
    let to_man = manifest_for(to_arch, mode).unwrap();
    let from_idx: std::collections::HashMap<_, _> =
        from_man.files.iter().map(|f| (f.path.clone(), f.blake2b256.clone())).collect();
    let mut delta_files = Vec::new();
    for f in &to_man.files {
        if from_idx.get(&f.path) == Some(&f.blake2b256) {
            continue;
        }
        delta_files.push(f.clone());
        let dst = out.join(&f.path);
        std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
        std::fs::copy(to_arch.join(&f.path), dst).unwrap();
    }
    delta_files.sort_by(|a, b| a.path.cmp(&b.path));
    let dm = DeltaManifest {
        network: to_man.network.clone(),
        from_height: from_man.height,
        to_height: to_man.height,
        mode: mode.into(),
        based_on_manifest_id: from_man.manifest_id.clone(),
        manifest_id: to_man.manifest_id.clone(),
        files: delta_files,
        ..Default::default()
    };
    std::fs::write(
        out.join(format!("delta-manifest-{mode}.json")),
        serde_json::to_vec_pretty(&dm).unwrap(),
    )
    .unwrap();
    // Stage the target's full manifest next to the delta, as the
    // publisher does (apply installs it at the end).
    std::fs::copy(
        to_arch.join(format!("manifest-{mode}.json")),
        out.join(format!("manifest-{mode}.json")),
    )
    .unwrap();
}

fn file_src(p: &Path) -> String {
    format!("file://{}", p.display())
}

#[test]
fn detects_minimal_and_archive_and_partial() {
    let dir = touch_fake_archive(&[]);
    let d = detect_mode(dir.path()).unwrap();
    assert_eq!(d.mode, "minimal");
    assert!(d.intact);

    let dir = touch_fake_archive(&[
        "chain/freezer/bodyc.cidx",
        "chain/freezer/bodyc.0000.cdat",
        "chain/freezer/txindex.cidx",
        "chain/freezer/witness.cidx",
        "chain/freezer/witness.0000.cdat",
        "chain/freezer/anchorc.cidx",
        "chain/freezer/anchorc.0000.cdat",
        "chain/freezer/anchorc.blocks",
    ]);
    assert_eq!(detect_mode(dir.path()).unwrap().mode, "archive");

    let dir = TempDir::new();
    std::fs::create_dir_all(dir.path().join("chain/freezer")).unwrap();
    std::fs::write(dir.path().join("chain/freezer/headerc.cidx"), "x").unwrap();
    let d = detect_mode(dir.path()).unwrap();
    assert_eq!(d.mode, "");
    assert!(!d.intact);
    assert!(!d.missing_sections.is_empty());
}

#[test]
fn fetch_and_verify_round_trip_then_skip_then_corruption() {
    let src = touch_fake_archive(&[]);
    write_fake_manifest(src.path(), "minimal", 1);
    let dst = TempDir::new();
    let rep = fetch(&file_src(src.path()), dst.path(), "minimal", false, 2).unwrap();
    assert!(rep.ok, "{rep:?}");
    assert_eq!(rep.downloaded, rep.total_files);
    let vrep = verify(dst.path(), "", 2).unwrap();
    assert!(vrep.ok, "{:?} {:?}", vrep.missing_files, vrep.mismatches);
    assert_eq!(vrep.mode, "minimal");

    let rep = fetch(&file_src(src.path()), dst.path(), "minimal", false, 2).unwrap();
    assert_eq!(rep.downloaded, 0);
    assert_eq!(rep.already_ok, rep.total_files);

    std::fs::write(dst.path().join("snapshot/accounts.0-25099999.idx"), "corrupted").unwrap();
    let vrep = verify(dst.path(), "", 2).unwrap();
    assert!(!vrep.ok);
    assert!(!vrep.mismatches.is_empty() || !vrep.wrong_size.is_empty());
}

#[test]
fn downgrade_identifies_redundant_files_in_dry_run() {
    let src = touch_fake_archive(&[
        "chain/freezer/bodyc.cidx",
        "chain/freezer/witness.cidx",
        "chain/freezer/witness.0000.cdat",
    ]);
    write_fake_manifest(src.path(), "minimal", 1);
    let rep = downgrade(src.path(), "minimal", false).unwrap();
    assert!(!rep.removed.is_empty());
    for p in &rep.removed {
        assert!(src.path().join(p).exists(), "dry-run removed {p}");
    }
}

#[test]
fn status_reports_current_behind_empty_and_missing_index() {
    let src = touch_fake_archive(&[]);
    write_fake_manifest(src.path(), "minimal", 25000);
    let mirror = TempDir::new();
    publish_fake_mirror(src.path(), mirror.path(), "simnet", "minimal");
    let client = TempDir::new();
    let net_src = file_src(&mirror.path().join("simnet"));
    fetch(
        &file_src(&mirror.path().join("simnet/25000/minimal")),
        client.path(),
        "minimal",
        false,
        2,
    )
    .unwrap();
    let st = status(client.path(), &net_src, "minimal").unwrap();
    assert_eq!((st.local_height, st.remote_height, st.behind_blocks), (25000, 25000, 0));
    assert!(st.up_to_date);

    // A newer release with a changed file: behind by 1000.
    let src2 = touch_fake_archive(&[]);
    std::fs::write(src2.path().join("chain/freezer/headerc.cidx"), "v2").unwrap();
    write_fake_manifest(src2.path(), "minimal", 26000);
    publish_fake_mirror(src2.path(), mirror.path(), "simnet", "minimal");
    let st = status(client.path(), &net_src, "minimal").unwrap();
    assert_eq!((st.local_height, st.remote_height, st.behind_blocks), (25000, 26000, 1000));
    assert!(!st.up_to_date);

    // An empty client: behind by the whole height, with the hint.
    let empty = TempDir::new();
    let st = status(empty.path(), &net_src, "minimal").unwrap();
    assert_eq!((st.local_height, st.behind_blocks), (0, 26000));
    assert!(st.note.contains("no local manifest"));

    // No releases.json at all: a hard error.
    let no_index = TempDir::new();
    assert!(status(client.path(), &file_src(no_index.path()), "minimal").is_err());
}

/// Builds a mirror with a chain of releases and deltas between them.
fn mirror_with_delta_chain(heights: &[u64]) -> (TempDir, String) {
    let mirror = TempDir::new();
    let mut prev: Option<TempDir> = None;
    for (i, &h) in heights.iter().enumerate() {
        let src = touch_fake_archive(&[]);
        std::fs::write(src.path().join("chain/freezer/headerc.cidx"), format!("v{i}")).unwrap();
        write_fake_manifest(src.path(), "minimal", h);
        publish_fake_mirror(src.path(), mirror.path(), "simnet", "minimal");
        if let Some(prev) = &prev {
            let from = heights[i - 1];
            let dst = mirror
                .path()
                .join("simnet/deltas")
                .join(format!("{from}-{h}"))
                .join("minimal");
            std::fs::create_dir_all(&dst).unwrap();
            build_delta_tree(prev.path(), src.path(), "minimal", &dst);
            let mid = manifest_for(src.path(), "minimal").unwrap().manifest_id;
            append_delta_to_mirror(mirror.path(), "simnet", "minimal", from, h, &mid);
        }
        prev = Some(src);
    }
    let net_src = file_src(&mirror.path().join("simnet"));
    (mirror, net_src)
}

#[test]
fn catch_up_noop_single_delta_chain_and_max_iterations() {
    // No-op when current.
    let (mirror, net_src) = mirror_with_delta_chain(&[25000]);
    let client = TempDir::new();
    fetch(
        &file_src(&mirror.path().join("simnet/25000/minimal")),
        client.path(),
        "minimal",
        false,
        2,
    )
    .unwrap();
    let rep = catch_up(client.path(), &net_src, "minimal", 0).unwrap();
    assert_eq!((rep.iterations, rep.final_height, rep.up_to_date), (0, 25000, true));

    // A three-release chain: two deltas applied.
    let (mirror, net_src) = mirror_with_delta_chain(&[23000, 24000, 25000]);
    let client = TempDir::new();
    fetch(
        &file_src(&mirror.path().join("simnet/23000/minimal")),
        client.path(),
        "minimal",
        false,
        2,
    )
    .unwrap();
    let rep = catch_up(client.path(), &net_src, "minimal", 5).unwrap();
    assert_eq!((rep.iterations, rep.final_height, rep.up_to_date), (2, 25000, true));
    let m = manifest_for(client.path(), "minimal").unwrap();
    assert_eq!(m.height, 25000);

    // The same chain bounded to one iteration stops early, cleanly.
    let client = TempDir::new();
    fetch(
        &file_src(&mirror.path().join("simnet/23000/minimal")),
        client.path(),
        "minimal",
        false,
        2,
    )
    .unwrap();
    let rep = catch_up(client.path(), &net_src, "minimal", 1).unwrap();
    assert_eq!((rep.iterations, rep.final_height, rep.up_to_date), (1, 24000, false));
}

#[test]
fn follow_stays_current_applies_new_release_and_cancels() {
    // Quiet mirror: three cycles, nothing applied.
    let (mirror, net_src) = mirror_with_delta_chain(&[25000]);
    let client = TempDir::new();
    fetch(
        &file_src(&mirror.path().join("simnet/25000/minimal")),
        client.path(),
        "minimal",
        false,
        2,
    )
    .unwrap();
    let rep = follow(FollowConfig {
        datadir: client.path().to_path_buf(),
        source: net_src.clone(),
        mode: "minimal".into(),
        poll_interval: Duration::from_millis(50),
        max_cycles: 3,
        max_iter: 0,
        stop: Arc::new(AtomicBool::new(false)),
        on_cycle: None,
    })
    .unwrap();
    assert_eq!((rep.cycles, rep.applied_deltas, rep.final_height), (3, 0, 25000));

    // A release published mid-loop is picked up on a later poll.
    let (mirror, net_src) = mirror_with_delta_chain(&[1000]);
    let client = TempDir::new();
    fetch(
        &file_src(&mirror.path().join("simnet/1000/minimal")),
        client.path(),
        "minimal",
        false,
        2,
    )
    .unwrap();
    let first = manifest_for(&mirror.path().join("simnet/1000/minimal"), "minimal").unwrap();
    let mirror_path = mirror.path().to_path_buf();
    let publisher = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(80));
        let src_b = touch_fake_archive(&[]);
        std::fs::write(src_b.path().join("chain/freezer/headerc.cidx"), "v-2000").unwrap();
        write_fake_manifest(src_b.path(), "minimal", 2000);
        publish_fake_mirror(src_b.path(), &mirror_path, "simnet", "minimal");
        let dst = mirror_path.join("simnet/deltas/1000-2000/minimal");
        std::fs::create_dir_all(&dst).unwrap();
        // The prior release stays on disk under 1000/; rebuild it for
        // the delta baseline.
        let src_a = touch_fake_archive(&[]);
        std::fs::write(src_a.path().join("chain/freezer/headerc.cidx"), "v0").unwrap();
        write_fake_manifest(src_a.path(), "minimal", 1000);
        assert_eq!(
            manifest_for(src_a.path(), "minimal").unwrap().manifest_id,
            first.manifest_id,
            "rebuilt baseline must match the published one"
        );
        build_delta_tree(src_a.path(), src_b.path(), "minimal", &dst);
        let mid = manifest_for(src_b.path(), "minimal").unwrap().manifest_id;
        append_delta_to_mirror(&mirror_path, "simnet", "minimal", 1000, 2000, &mid);
    });
    let rep = follow(FollowConfig {
        datadir: client.path().to_path_buf(),
        source: net_src,
        mode: "minimal".into(),
        poll_interval: Duration::from_millis(50),
        max_cycles: 10,
        max_iter: 0,
        stop: Arc::new(AtomicBool::new(false)),
        on_cycle: None,
    })
    .unwrap();
    publisher.join().unwrap();
    assert!(rep.applied_deltas >= 1, "{rep:?}");
    assert_eq!(rep.final_height, 2000);

    // The stop flag ends an unlimited loop cleanly.
    let (mirror, net_src) = mirror_with_delta_chain(&[100]);
    let client = TempDir::new();
    fetch(
        &file_src(&mirror.path().join("simnet/100/minimal")),
        client.path(),
        "minimal",
        false,
        2,
    )
    .unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();
    let datadir = client.path().to_path_buf();
    let handle = std::thread::spawn(move || {
        follow(FollowConfig {
            datadir,
            source: net_src,
            mode: "minimal".into(),
            poll_interval: Duration::from_millis(200),
            max_cycles: 0,
            max_iter: 0,
            stop: stop_clone,
            on_cycle: None,
        })
    });
    std::thread::sleep(Duration::from_millis(150));
    stop.store(true, Ordering::Relaxed);
    let rep = handle.join().unwrap().unwrap();
    assert!(rep.cancelled_clean);
}
