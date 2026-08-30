//! Walk the publisher's delta chain from the local height to its
//! latest.

use super::delta::apply_delta;
use super::manifest::manifest_for;
use super::source::open_source;
use super::status::{fetch_index, status, RemoteDeltaRef};
use std::path::Path;

/// What a `catch_up` run did.
#[derive(Debug, Default)]
pub struct CatchUpReport {
    pub network: String,
    pub mode: String,
    pub start_height: u64,
    pub final_height: u64,
    pub remote_height: u64,
    pub iterations: usize,
    pub up_to_date: bool,
    pub total_bytes_xfer: u64,
    pub errors: Vec<String>,
}

/// Applies deltas until at the publisher's latest, out of deltas, or at
/// `max_iterations` (0 = unlimited). Running out of deltas or
/// iterations is reported, not an error; a failed apply is an error.
pub fn catch_up(
    datadir: &Path,
    source: &str,
    mode: &str,
    max_iterations: usize,
) -> std::io::Result<CatchUpReport> {
    let mut rep = CatchUpReport { mode: mode.into(), ..Default::default() };
    let st = status(datadir, source, mode)?;
    rep.network = st.network;
    rep.start_height = st.local_height;
    rep.final_height = st.local_height;
    rep.remote_height = st.remote_height;
    if st.up_to_date {
        rep.up_to_date = true;
        return Ok(rep);
    }
    // One index fetch per run; a follower re-polls every cycle anyway.
    let idx = fetch_index(source)?;
    let base = open_source(source)?;
    loop {
        let local = manifest_for(datadir, mode)?;
        if local.height >= rep.remote_height {
            rep.up_to_date = true;
            rep.final_height = local.height;
            break;
        }
        if max_iterations > 0 && rep.iterations >= max_iterations {
            break;
        }
        let Some(next) = find_next_delta(&idx.deltas, mode, local.height) else {
            rep.errors.push(format!(
                "no delta available from height={} manifest_id={}",
                local.height, local.manifest_id
            ));
            break;
        };
        let range = format!("{}-{}", next.from_height, next.to_height);
        let delta_source = match &base {
            super::source::Source::Local(root) => {
                format!("{}", root.join("deltas").join(&range).join(mode).display())
            }
            super::source::Source::Http(url) => format!("{url}/deltas/{range}/{mode}"),
        };
        rep.iterations += 1;
        match apply_delta(&delta_source, datadir, mode, 4) {
            Ok(ar) => {
                rep.total_bytes_xfer += ar.bytes_xfer;
                rep.final_height = next.to_height;
            }
            Err(e) => {
                rep.errors
                    .push(format!("delta {}→{}: {e}", next.from_height, next.to_height));
                return Err(std::io::Error::other(format!(
                    "delta {}→{}: {e}",
                    next.from_height, next.to_height
                )));
            }
        }
    }
    Ok(rep)
}

/// The best delta from the current height: `from_height` equal to it,
/// longest leap forward among the candidates.
fn find_next_delta<'a>(
    deltas: &'a [RemoteDeltaRef],
    mode: &str,
    local_height: u64,
) -> Option<&'a RemoteDeltaRef> {
    deltas
        .iter()
        .filter(|d| d.mode == mode && d.from_height == local_height)
        .max_by_key(|d| d.to_height)
}
