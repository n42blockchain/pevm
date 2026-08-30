//! The file-selector contract mapping each distribution mode to the
//! paths it ships. Mirrors `cmd/n42-eth-manifest/manifest/selector.go`:
//! the pattern lists, the section names, their order, the optional
//! flag and the windowed-bodies rule are the published contract.

use super::manifest::FileEntry;
use std::collections::HashSet;
use std::path::Path;

/// A named group of file patterns (e.g. "headers", "witness").
#[derive(Debug, Clone)]
pub struct Section {
    pub name: &'static str,
    pub patterns: &'static [&'static str],
    /// Files that belong to the eventual tier shape but are not required
    /// to identify a usable tier.
    pub optional: bool,
    /// When > 0, publish only the newest N data (`.cdat`) segments of this
    /// section, keeping every index file — what makes `full` a
    /// history-windowed product (EIP-4444 shape).
    pub window_segments: usize,
}

const fn section(name: &'static str, patterns: &'static [&'static str]) -> Section {
    Section { name, patterns, optional: false, window_segments: 0 }
}

/// The ordered sections of one distribution mode.
#[derive(Debug, Clone)]
pub struct Selector {
    pub mode: String,
    pub sections: Vec<Section>,
}

const STATE_ACCOUNTS: Section = section(
    "state-accounts",
    &[
        "snapshot/accounts.*.idx",
        "snapshot/accounts.*.ef",
        "snapshot/accounts.*.val.zst",
        "snapshot/accounts.*.codedict",
    ],
);
const STATE_STORAGE: Section = section(
    "state-storage",
    &["snapshot/storage.*.idx", "snapshot/storage.*.ef", "snapshot/storage.*.val.zst"],
);
const BEACON_CHECKPOINT: Section = Section {
    name: "beacon-checkpoint",
    patterns: &["caplin/checkpoint/state.*.ssz.zst"],
    optional: true,
    window_segments: 0,
};
const BEACON_ARCHIVE: Section = Section {
    name: "beacon-archive",
    patterns: &["caplin/beacon-archive.*.zst", "caplin/beacon-archive.*.idx"],
    optional: true,
    window_segments: 0,
};

/// How many bodyc data segments the `full` product ships — roughly one
/// year of bodies. The Go side's `DefaultFullBodiesWindow`.
pub const DEFAULT_FULL_BODIES_WINDOW: usize = 56;

/// The Selector for a named mode. Unknown modes error rather than
/// silently fall back; `mobile` has no file bundle at all.
pub fn selector_for(mode: &str) -> Result<Selector, String> {
    selector_for_with_window(mode, 0)
}

/// `selector_for` with an explicit bodies window for `full` (0 = the
/// built-in one-year default). Other modes ignore it.
pub fn selector_for_with_window(mode: &str, window: usize) -> Result<Selector, String> {
    match mode {
        "mobile" => {
            Err("mobile has no file bundle (app + checkpoint config, streams from IDC)".into())
        }
        "minimal" => Ok(Selector {
            mode: "minimal".into(),
            sections: vec![STATE_ACCOUNTS, STATE_STORAGE, BEACON_CHECKPOINT],
        }),
        "full" => {
            let window = if window > 0 { window } else { DEFAULT_FULL_BODIES_WINDOW };
            Ok(Selector {
                mode: "full".into(),
                sections: vec![
                    section(
                        "headers",
                        &["chain/freezer/headerc.cidx", "chain/freezer/headerc.*.cdat"],
                    ),
                    section("code", &["chain/freezer/codes.cidx", "chain/freezer/codes.*.cdat"]),
                    STATE_ACCOUNTS,
                    STATE_STORAGE,
                    Section {
                        name: "bodies",
                        patterns: &["chain/freezer/bodyc.cidx", "chain/freezer/bodyc.*.cdat"],
                        optional: false,
                        window_segments: window,
                    },
                    section(
                        "tx-index",
                        &["chain/freezer/txindex.cidx", "chain/freezer/txindex.*.cdat"],
                    ),
                    BEACON_CHECKPOINT,
                ],
            })
        }
        "archive" => Ok(Selector {
            mode: "archive".into(),
            sections: vec![
                section("headers", &["chain/freezer/headerc.cidx", "chain/freezer/headerc.*.cdat"]),
                section("bodies", &["chain/freezer/bodyc.cidx", "chain/freezer/bodyc.*.cdat"]),
                section("code", &["chain/freezer/codes.cidx", "chain/freezer/codes.*.cdat"]),
                section("witness", &["chain/freezer/witness.cidx", "chain/freezer/witness.*.cdat"]),
                section(
                    "tx-index",
                    &["chain/freezer/txindex.cidx", "chain/freezer/txindex.*.cdat"],
                ),
                section(
                    "anchors",
                    &[
                        "chain/freezer/anchorc.cidx",
                        "chain/freezer/anchorc.*.cdat",
                        "chain/freezer/anchorc.blocks",
                    ],
                ),
                BEACON_ARCHIVE,
            ],
        }),
        other => Err(format!("unknown mode {other:?} (want minimal|full|archive)")),
    }
}

/// Adds the opt-in senders pack. Idempotent.
pub fn with_senders(mut s: Selector) -> Selector {
    if s.sections.iter().any(|sec| sec.name == "senders") {
        return s;
    }
    s.mode.push_str("+senders");
    s.sections.push(section(
        "senders",
        &["chain/freezer/senders.cidx", "chain/freezer/senders.*.cdat"],
    ));
    s
}

fn match_options() -> glob::MatchOptions {
    // Go's filepath.Match: `*` never crosses a path separator.
    glob::MatchOptions {
        case_sensitive: true,
        require_literal_separator: true,
        require_literal_leading_dot: false,
    }
}

/// Resolves every section's patterns against the datadir: the
/// deduplicated file list in deterministic order (section order, then
/// alphabetical within the section), sizes from stat, hashes left empty.
/// A windowed section keeps every index file and only the newest N
/// `.cdat` segments.
pub fn walk_files(root: &Path, sel: &Selector) -> std::io::Result<Vec<FileEntry>> {
    let mut out = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for sec in &sel.sections {
        let mut matched: Vec<String> = Vec::new();
        for pat in sec.patterns {
            let pattern = glob::Pattern::new(pat)
                .map_err(|e| std::io::Error::other(format!("pattern {pat}: {e}")))?;
            collect_matches(root, &pattern, &mut matched)?;
        }
        matched.sort();
        if sec.window_segments > 0 {
            let (mut idx, mut dat): (Vec<_>, Vec<_>) =
                matched.into_iter().partition(|m| !m.ends_with(".cdat"));
            if dat.len() > sec.window_segments {
                dat = dat.split_off(dat.len() - sec.window_segments);
            }
            idx.extend(dat);
            idx.sort();
            matched = idx;
        }
        for rel in matched {
            if !seen.insert(rel.clone()) {
                continue;
            }
            let meta = std::fs::metadata(root.join(&rel))?;
            if meta.is_dir() {
                continue;
            }
            out.push(FileEntry {
                path: rel,
                section: sec.name.to_string(),
                size: meta.len(),
                blake2b256: String::new(),
            });
        }
    }
    Ok(out)
}

/// Walks only as deep as the pattern's fixed directory prefix, then
/// matches the remainder with Go-`filepath.Match` semantics.
fn collect_matches(root: &Path, pattern: &glob::Pattern, out: &mut Vec<String>) -> std::io::Result<()> {
    // Patterns here have a literal directory part and a wild last
    // component (or are fully literal); listing the parent directory is
    // both simpler and cheaper than a full walk.
    let pat = pattern.as_str();
    let (dir, _last) = match pat.rfind('/') {
        Some(i) => (&pat[..i], &pat[i + 1..]),
        None => ("", pat),
    };
    if dir.contains(['*', '?', '[']) {
        return Err(std::io::Error::other(format!(
            "pattern {pat}: wildcards in directories are not part of the contract"
        )));
    }
    let full_dir = root.join(dir);
    let entries = match std::fs::read_dir(&full_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    for entry in entries {
        let entry = entry?;
        let rel = if dir.is_empty() {
            entry.file_name().to_string_lossy().into_owned()
        } else {
            format!("{dir}/{}", entry.file_name().to_string_lossy())
        };
        if pattern.matches_with(&rel, match_options()) {
            out.push(rel);
        }
    }
    Ok(())
}

fn present_sections(root: &Path, sel: &Selector) -> std::io::Result<HashSet<&'static str>> {
    let files = walk_files(root, sel)?;
    Ok(files.iter().filter_map(|f| {
        sel.sections.iter().find(|s| s.name == f.section).map(|s| s.name)
    }).collect())
}

/// Required sections with no matching files; optional sections never
/// make a tier undetectable.
pub fn required_missing_sections(root: &Path, sel: &Selector) -> std::io::Result<Vec<String>> {
    let present = present_sections(root, sel)?;
    Ok(sel
        .sections
        .iter()
        .filter(|s| !s.optional && !present.contains(s.name))
        .map(|s| s.name.to_string())
        .collect())
}

/// What mode a datadir currently contains.
#[derive(Debug, Clone, Default)]
pub struct DetectResult {
    /// "minimal" | "full" | "archive" | "".
    pub mode: String,
    /// 0 if no manifest is available.
    pub height: u64,
    /// All required sections of the detected mode are present.
    pub intact: bool,
    /// Sections missing from the next tier up (or, with no mode, from
    /// `minimal`).
    pub missing_sections: Vec<String>,
}

/// Classifies the datadir as the highest mode whose required sections
/// are all populated, reading the height from its manifest when there
/// is one.
pub fn detect_mode(datadir: &Path) -> std::io::Result<DetectResult> {
    for mode in ["archive", "full", "minimal"] {
        let sel = selector_for(mode).expect("known mode");
        if !required_missing_sections(datadir, &sel)?.is_empty() {
            continue;
        }
        let mut res = DetectResult { mode: mode.into(), intact: true, ..Default::default() };
        if let Ok(m) = super::manifest::manifest_for(datadir, mode) {
            res.height = m.height;
        }
        let next = match mode {
            "minimal" => Some("full"),
            "full" => Some("archive"),
            _ => None,
        };
        if let Some(next) = next {
            let next_sel = selector_for(next).expect("known mode");
            res.missing_sections = required_missing_sections(datadir, &next_sel)?;
        }
        return Ok(res);
    }
    let sel = selector_for("minimal").expect("known mode");
    Ok(DetectResult {
        missing_sections: required_missing_sections(datadir, &sel)?,
        ..Default::default()
    })
}
