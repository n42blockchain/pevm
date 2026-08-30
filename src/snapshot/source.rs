//! A snapshot mirror — local directory or HTTP server.

use std::io::Read;
use std::path::PathBuf;

/// Where files come from.
#[derive(Debug, Clone)]
pub enum Source {
    Local(PathBuf),
    Http(String),
}

/// Resolves a user-supplied source string: `file:///path`, a bare
/// path, or `http(s)://host/...`.
pub fn open_source(s: &str) -> std::io::Result<Source> {
    if s.starts_with("http://") || s.starts_with("https://") {
        return Ok(Source::Http(s.trim_end_matches('/').to_string()));
    }
    let dir = PathBuf::from(s.strip_prefix("file://").unwrap_or(s));
    let meta = std::fs::metadata(&dir)
        .map_err(|e| std::io::Error::other(format!("open source {}: {e}", dir.display())))?;
    if !meta.is_dir() {
        return Err(std::io::Error::other(format!("source {} is not a directory", dir.display())));
    }
    Ok(Source::Local(dir))
}

impl Source {
    /// A readable handle for the file at the given relative path.
    pub fn open(&self, rel_path: &str) -> std::io::Result<Box<dyn Read>> {
        match self {
            Self::Local(root) => Ok(Box::new(std::fs::File::open(root.join(rel_path))?)),
            Self::Http(base) => {
                let url = format!("{base}/{rel_path}");
                let resp = reqwest::blocking::get(&url)
                    .map_err(|e| std::io::Error::other(format!("GET {url}: {e}")))?;
                if !resp.status().is_success() {
                    return Err(std::io::Error::other(format!("GET {url}: {}", resp.status())));
                }
                Ok(Box::new(resp))
            }
        }
    }

    /// A child source rooted below this one (`deltas/<range>/<mode>`).
    pub fn join(&self, parts: &[&str]) -> Source {
        match self {
            Self::Local(root) => {
                let mut p = root.clone();
                for part in parts {
                    p.push(part);
                }
                Self::Local(p)
            }
            Self::Http(base) => {
                let mut url = base.clone();
                for part in parts {
                    url.push('/');
                    url.push_str(part);
                }
                Self::Http(url)
            }
        }
    }
}
