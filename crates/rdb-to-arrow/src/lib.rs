//! Convert parsed RDB entries into Apache Arrow `RecordBatch`es and
//! optionally write them to Parquet, Arrow IPC, CSV, or JSON.
//!
//! The entry point is [`ArrowBatcher`], which consumes
//! [`rdb_parser::RdbEntry`] values and yields typed [`TypedBatch`]es.
//! Per-type schemas and virtual-type heuristics (HyperLogLog, Geo) live
//! in the [`schema`] module; format-specific writers live in [`writer`].

#![warn(missing_docs)]

/// Error types for the conversion and writer layers.
pub mod error;
pub(crate) mod detect;
pub(crate) mod schema;
pub(crate) mod builders;
/// Batch accumulator that groups RDB entries by logical type.
pub mod batcher;
/// Single-pass RDB statistics used by CLI `validate` and Python `inspect`.
pub mod summarize;
/// Format-specific writers (Parquet, Arrow IPC, CSV, JSON).
pub mod writer;
#[cfg(test)]
pub(crate) mod test_helpers;

pub use batcher::{ArrowBatcher, BatchIterator, BatcherConfig, TypedBatch};
pub use error::ArrowConvertError;
pub use schema::{expected_row_count, schema_for, type_tag_for, is_geo_entry, Heuristic, TypeTag};
pub use summarize::{summarize_entries, EntrySummary, TypeCount};
pub use writer::write_arrow_ipc;
#[cfg(feature = "parquet")]
pub use writer::{
    metadata_from_rdb, parse_compression, write_parquet, ParquetConfig, RDB_EXPORTED_BY_KEY,
    RDB_EXPORTED_BY_VALUE, RDB_HEURISTICS_KEY,
};
#[cfg(feature = "csv")]
pub use writer::write_csv;
#[cfg(feature = "json")]
pub use writer::write_json;
