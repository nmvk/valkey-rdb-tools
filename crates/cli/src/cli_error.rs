//! Classified error type returned by every CLI subcommand.
//!
//! Exit codes were previously a uniform `exit(1)`, which is poor for
//! agent-driven or CI use: a caller can't distinguish "file not found"
//! from "RDB is corrupt" from "validation showed a row mismatch"
//! without parsing the error message. Each variant here maps to a
//! dedicated exit code so callers can branch on them.

use std::fmt;

use crate::io::{IoContext, RdbOpenError};

/// A classified CLI error. `exit_code` is the `std::process::exit` value
/// `main` uses when this variant surfaces.
#[derive(Debug)]
pub enum CliError {
    /// Command-line usage error — bad flag value, unknown type name,
    /// conflicting options. `2` matches the BSD `sysexits` `EX_USAGE`.
    Usage(String),
    /// I/O failure — file not found, permission denied, directory not
    /// writable. Wraps the original error with filename context where
    /// available.
    Io(Box<dyn std::error::Error + Send + Sync>),
    /// RDB parser rejected the input as corrupt or unsupported. Distinct
    /// from I/O so callers can tell a broken file from a missing one.
    Corrupt(rdb_parser::RdbError),
    /// `validate` command ran to completion but found at least one
    /// type-count mismatch between the RDB and the exported Parquet.
    ValidationFailed(String),
    /// Anything else — Arrow / Parquet / serialization errors that
    /// don't fit the above. Caller should treat this as a general
    /// failure and read the error message.
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl CliError {
    /// Exit code to pass to `std::process::exit`.
    ///
    /// | Variant             | Code | Meaning                               |
    /// |---------------------|------|---------------------------------------|
    /// | `Usage`             | `2`  | Invalid arguments.                    |
    /// | `Io`                | `3`  | File/FS error.                        |
    /// | `Corrupt`           | `4`  | RDB parse rejected the file.          |
    /// | `ValidationFailed`  | `5`  | Validate found a row-count mismatch.  |
    /// | `Other`             | `1`  | Generic failure.                      |
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Usage(_) => 2,
            Self::Io(_) => 3,
            Self::Corrupt(_) => 4,
            Self::ValidationFailed(_) => 5,
            Self::Other(_) => 1,
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage(m) => write!(f, "{m}"),
            Self::Io(e) => write!(f, "{e}"),
            Self::Corrupt(e) => write!(f, "{e}"),
            Self::ValidationFailed(m) => write!(f, "{m}"),
            Self::Other(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for CliError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Usage(_) | Self::ValidationFailed(_) => None,
            Self::Io(e) => Some(e.as_ref()),
            Self::Corrupt(e) => Some(e),
            Self::Other(e) => Some(e.as_ref()),
        }
    }
}

impl From<IoContext> for CliError {
    fn from(e: IoContext) -> Self {
        Self::Io(Box::new(e))
    }
}

impl From<std::io::Error> for CliError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(Box::new(e))
    }
}

impl From<RdbOpenError> for CliError {
    fn from(e: RdbOpenError) -> Self {
        match e {
            RdbOpenError::Io(io) => Self::Io(Box::new(io)),
            RdbOpenError::Rdb(rdb) => Self::Corrupt(rdb),
        }
    }
}

impl From<rdb_parser::RdbError> for CliError {
    fn from(e: rdb_parser::RdbError) -> Self {
        Self::Corrupt(e)
    }
}

impl From<rdb_to_arrow::ArrowConvertError> for CliError {
    fn from(e: rdb_to_arrow::ArrowConvertError) -> Self {
        // Parser errors via the Arrow path are still corruption; other
        // Arrow/Parquet failures are bucketed as `Other`.
        match e {
            rdb_to_arrow::ArrowConvertError::Parser(rdb) => Self::Corrupt(rdb),
            rdb_to_arrow::ArrowConvertError::Io(io) => Self::Io(Box::new(io)),
            other => Self::Other(Box::new(other)),
        }
    }
}

impl From<parquet::errors::ParquetError> for CliError {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Self::Other(Box::new(e))
    }
}

impl From<String> for CliError {
    fn from(s: String) -> Self {
        // String errors come from user-facing parse helpers like
        // `Heuristic::parse_set`, which always signal a usage mistake.
        Self::Usage(s)
    }
}

impl From<&str> for CliError {
    fn from(s: &str) -> Self {
        Self::Usage(s.to_string())
    }
}

impl From<serde_json::Error> for CliError {
    fn from(e: serde_json::Error) -> Self {
        Self::Other(Box::new(e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes_are_distinct_per_variant() {
        let codes = [
            CliError::Usage(String::new()).exit_code(),
            CliError::Io(Box::new(std::io::Error::other("x"))).exit_code(),
            CliError::Corrupt(rdb_parser::RdbError::CorruptData("x".into())).exit_code(),
            CliError::ValidationFailed(String::new()).exit_code(),
            CliError::Other(Box::new(std::io::Error::other("x"))).exit_code(),
        ];
        // Every code should be unique so an exit-status consumer can
        // fan out on them unambiguously.
        let mut sorted: Vec<i32> = codes.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), codes.len(), "exit codes collide: {codes:?}");
        // None should collide with success (0).
        assert!(!codes.contains(&0));
    }

    #[test]
    fn rdb_open_io_maps_to_io_variant() {
        let io = IoContext::new("/x", std::io::Error::other("boom"));
        let err: CliError = RdbOpenError::Io(io).into();
        assert!(matches!(err, CliError::Io(_)));
        assert_eq!(err.exit_code(), 3);
    }

    #[test]
    fn rdb_open_rdb_maps_to_corrupt_variant() {
        let rdb = rdb_parser::RdbError::CorruptData("bad magic".into());
        let err: CliError = RdbOpenError::Rdb(rdb).into();
        assert!(matches!(err, CliError::Corrupt(_)));
        assert_eq!(err.exit_code(), 4);
    }
}
