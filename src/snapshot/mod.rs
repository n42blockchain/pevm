//! Client for gov5's eth-el snapshot distribution: the three-tier
//! minimal / full / archive contract, byte-compatible with
//! `cmd/n42-eth-snapshot` and `cmd/n42-eth-manifest` in the Go client
//! (spec: N42-gov5 `docs/ethel/n42-eth-client-distribution.md`).
//!
//! A datadir is described by `manifest-<mode>.json` — blake2b-256 per
//! file plus a sha256 `manifest_id` over the sorted file list. A
//! publisher mirror serves `releases.json`, per-release file trees and
//! `deltas/<from>-<to>/<mode>/` trees; `catch_up` walks the delta chain
//! and `follow` polls it, which is what keeps a node a delta behind the
//! publisher's tip with every applied file hash-verified on the way in.

mod catchup;
mod delta;
mod fetch;
mod manifest;
mod selector;
mod source;
mod status;
mod verify;

pub use catchup::{catch_up, CatchUpReport};
pub use delta::{apply_delta, plan_delta, DeltaApplyReport, DeltaManifest, DeltaPlan};
pub use fetch::{downgrade, fetch, upgrade, DowngradeReport, FetchReport};
pub use manifest::{
    compute_manifest_id, hash_all, hash_file, manifest_for, read_manifest, write_manifest,
    FileEntry,
    Manifest,
};
pub use selector::{detect_mode, selector_for, walk_files, DetectResult, Section, Selector};
pub use source::{open_source, Source};
pub use status::{status, StatusReport};
pub use verify::{verify, VerifyReport};

mod follow;
pub use follow::{follow, FollowConfig, FollowReport};

#[cfg(test)]
mod tests;
