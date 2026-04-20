//! Shared I/O helpers for the CLI subcommands.
//!
//! Every subcommand (`export`, `validate`, `schema`) goes through the
//! same file-open + RDB-reader + Parquet shard-discovery plumbing. Each
//! used to reimplement it, and the implementations drifted — in one spot
//! a bare `io::Error` lost the filename, in another the shard-glob
//! filter silently dropped the base file. This module is the single
//! source of truth for those paths.

use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use rdb_parser::RdbReader;
use rdb_to_arrow::TypeTag;

/// Error with the filename that the I/O operation was attempting.
///
/// Bare `io::Error` (e.g. "No such file or directory") loses the
/// filename context entirely, which turns every multi-file operation
/// into a scavenger hunt. Callers convert the wrapped error into
/// `Box<dyn Error>` for display.
#[derive(Debug)]
pub struct IoContext {
    path: String,
    source: io::Error,
}

impl IoContext {
    pub fn new(path: impl Into<String>, source: io::Error) -> Self {
        Self {
            path: path.into(),
            source,
        }
    }
}

impl std::fmt::Display for IoContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.path, self.source)
    }
}

impl std::error::Error for IoContext {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Open a file with its path attached to any I/O error.
pub fn open_file(path: &str) -> Result<File, IoContext> {
    File::open(path).map_err(|e| IoContext::new(path, e))
}

/// Open an RDB input source — either stdin for `"-"` or a regular file.
///
/// Returns a boxed `Read` so the caller doesn't have to fan out for the
/// two cases. The `RdbReader` constructor wraps this in an internal
/// buffered reader.
pub fn open_rdb_input(path: &str) -> Result<Box<dyn Read>, IoContext> {
    if path == "-" {
        Ok(Box::new(io::stdin()))
    } else {
        Ok(Box::new(open_file(path)?))
    }
}

/// Error class returned by [`build_rdb_reader`] — either the file open
/// failed (wrapped with filename) or the RDB header parse failed.
#[derive(Debug)]
pub enum RdbOpenError {
    Io(IoContext),
    Rdb(rdb_parser::RdbError),
}

impl std::fmt::Display for RdbOpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "{e}"),
            Self::Rdb(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for RdbOpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Rdb(e) => Some(e),
        }
    }
}

/// Open an RDB file and construct an [`RdbReader`]. Preserves filename
/// context on I/O errors and distinguishes header-parse errors as a
/// separate variant so callers can map them to distinct exit codes.
pub fn build_rdb_reader(path: &str) -> Result<RdbReader<Box<dyn Read>>, RdbOpenError> {
    let input = open_rdb_input(path).map_err(RdbOpenError::Io)?;
    RdbReader::new(input).map_err(RdbOpenError::Rdb)
}

/// Build the shard suffix appended to output filenames: either `".<id>"`
/// when a shard ID is set, or the empty string.
///
/// `validate` reads files produced with this suffix; the two callers
/// must agree on the format, so they both go through here.
pub fn shard_suffix(shard_id: Option<&str>) -> String {
    match shard_id {
        Some(s) => format!(".{s}"),
        None => String::new(),
    }
}

/// Find every Parquet file for a given type tag in `dir`: the base
/// `{tag}.parquet` plus any shard files matching `{tag}.*.parquet`.
///
/// A previous implementation used a glob crate here and had to carve
/// out the base file explicitly; the simple prefix/suffix check is
/// equivalent and one fewer dependency to audit.
pub fn find_parquet_files(dir: &Path, tag: TypeTag) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let base = dir.join(format!("{}.parquet", tag.as_str()));
    if base.exists() {
        files.push(base);
    }
    let prefix = format!("{}.", tag.as_str());
    let suffix = ".parquet";
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            // Shard files look like `{tag}.{id}.parquet` — skip the
            // base file (which has no shard ID between the two dots).
            let is_shard = name_str.starts_with(&prefix)
                && name_str.ends_with(suffix)
                && name_str.as_ref() != format!("{}{}", tag.as_str(), suffix);
            if is_shard {
                files.push(entry.path());
            }
        }
    }
    files
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_suffix_empty_without_id() {
        assert_eq!(shard_suffix(None), "");
    }

    #[test]
    fn shard_suffix_dot_prefixed_with_id() {
        assert_eq!(shard_suffix(Some("n1")), ".n1");
    }

    #[test]
    fn open_file_attaches_path_to_error() {
        let err = open_file("/this/definitely/does/not/exist.rdb").unwrap_err();
        let display = err.to_string();
        assert!(
            display.contains("/this/definitely/does/not/exist.rdb"),
            "error display should include the path: {display}"
        );
    }

    #[test]
    fn find_parquet_files_returns_base_and_shards() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        std::fs::write(dir.join("string.parquet"), b"").unwrap();
        std::fs::write(dir.join("string.shard-a.parquet"), b"").unwrap();
        std::fs::write(dir.join("string.shard-b.parquet"), b"").unwrap();
        // Unrelated files must be ignored.
        std::fs::write(dir.join("list.parquet"), b"").unwrap();
        std::fs::write(dir.join("notes.txt"), b"").unwrap();

        let mut got: Vec<_> = find_parquet_files(dir, TypeTag::String)
            .into_iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                "string.parquet",
                "string.shard-a.parquet",
                "string.shard-b.parquet",
            ]
        );
    }
}
