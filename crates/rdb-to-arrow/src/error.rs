use std::fmt;

/// Errors that can occur during RDB-to-Arrow conversion.
#[derive(Debug)]
pub enum ArrowConvertError {
    /// Error from the RDB parser.
    Parser(rdb_parser::RdbError),
    /// Error from Arrow operations.
    Arrow(arrow::error::ArrowError),
    /// Error from Parquet operations.
    #[cfg(feature = "parquet")]
    Parquet(parquet::errors::ParquetError),
    /// I/O error.
    Io(std::io::Error),
}

impl fmt::Display for ArrowConvertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parser(e) => write!(f, "RDB parser error: {e}"),
            Self::Arrow(e) => write!(f, "Arrow error: {e}"),
            #[cfg(feature = "parquet")]
            Self::Parquet(e) => write!(f, "Parquet error: {e}"),
            Self::Io(e) => write!(f, "I/O error: {e}"),
        }
    }
}

impl std::error::Error for ArrowConvertError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Parser(e) => Some(e),
            Self::Arrow(e) => Some(e),
            #[cfg(feature = "parquet")]
            Self::Parquet(e) => Some(e),
            Self::Io(e) => Some(e),
        }
    }
}

impl From<rdb_parser::RdbError> for ArrowConvertError {
    fn from(e: rdb_parser::RdbError) -> Self {
        Self::Parser(e)
    }
}

impl From<arrow::error::ArrowError> for ArrowConvertError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Self::Arrow(e)
    }
}

#[cfg(feature = "parquet")]
impl From<parquet::errors::ParquetError> for ArrowConvertError {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Self::Parquet(e)
    }
}

impl From<std::io::Error> for ArrowConvertError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
