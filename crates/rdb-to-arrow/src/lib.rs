pub mod error;
pub(crate) mod detect;
pub(crate) mod schema;
pub(crate) mod builders;
pub mod batcher;
pub mod writer;
#[cfg(test)]
pub(crate) mod test_helpers;

pub use batcher::{ArrowBatcher, BatchIterator, BatcherConfig, TypedBatch};
pub use error::ArrowConvertError;
pub use schema::{schema_for, type_tag_for, TypeTag};
pub use writer::write_arrow_ipc;
#[cfg(feature = "parquet")]
pub use writer::{metadata_from_rdb, write_parquet, ParquetConfig};
#[cfg(feature = "csv")]
pub use writer::write_csv;
#[cfg(feature = "json")]
pub use writer::write_json;
