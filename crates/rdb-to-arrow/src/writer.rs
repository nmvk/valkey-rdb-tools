use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::Write;

use arrow::ipc::writer::FileWriter as IpcFileWriter;

use crate::batcher::TypedBatch;
use crate::error::ArrowConvertError;
use crate::schema::{schema_for, TypeTag};

/// Configuration for Parquet output.
#[cfg(feature = "parquet")]
#[derive(Debug, Clone)]
pub struct ParquetConfig {
    pub compression: parquet::basic::Compression,
    pub max_row_group_size: usize,
    /// Key-value metadata written into every Parquet file's Arrow schema.
    /// Useful for embedding AUX fields (server version, ctime, etc.) and
    /// export provenance.
    pub file_metadata: HashMap<String, String>,
}

#[cfg(feature = "parquet")]
impl Default for ParquetConfig {
    fn default() -> Self {
        Self {
            compression: parquet::basic::Compression::ZSTD(Default::default()),
            max_row_group_size: 1_048_576,
            file_metadata: HashMap::new(),
        }
    }
}

/// Write TypedBatches as Parquet files.
///
/// `writer_factory` is called once per TypeTag to create the output writer.
/// This keeps the crate filesystem-agnostic — callers provide the factory
/// (e.g. to create files, or write to `Vec<u8>` in tests).
#[cfg(feature = "parquet")]
pub fn write_parquet<I, W, F>(
    batches: I,
    config: &ParquetConfig,
    mut writer_factory: F,
) -> Result<(), ArrowConvertError>
where
    I: Iterator<Item = Result<TypedBatch, ArrowConvertError>>,
    W: Write + Send,
    F: FnMut(TypeTag) -> Result<W, ArrowConvertError>,
{
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let props = WriterProperties::builder()
        .set_compression(config.compression)
        .set_max_row_group_size(config.max_row_group_size)
        .build();

    let mut writers: HashMap<TypeTag, ArrowWriter<W>> = HashMap::new();

    for batch_result in batches {
        let typed_batch = batch_result?;
        let writer = match writers.entry(typed_batch.tag) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let w = writer_factory(typed_batch.tag)?;
                let schema = if config.file_metadata.is_empty() {
                    schema_for(typed_batch.tag)
                } else {
                    schema_for(typed_batch.tag).with_metadata(config.file_metadata.clone())
                };
                let arrow_writer =
                    ArrowWriter::try_new(w, std::sync::Arc::new(schema), Some(props.clone()))?;
                e.insert(arrow_writer)
            }
        };
        writer.write(&typed_batch.batch)?;
    }

    for (_, writer) in writers {
        writer.close()?;
    }

    Ok(())
}

/// Build Parquet file metadata from RDB metadata (AUX fields).
///
/// Copies all AUX key-value pairs prefixed with `rdb.` and adds an
/// `rdb.exported_by` provenance tag.
#[cfg(feature = "parquet")]
pub fn metadata_from_rdb(meta: &rdb_parser::RdbMetadata) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for (k, v) in &meta.aux {
        map.insert(format!("rdb.{k}"), v.clone());
    }
    map.insert(
        "rdb.exported_by".to_string(),
        "valkey-rdb-tools".to_string(),
    );
    map
}

/// Write TypedBatches as Arrow IPC files.
pub fn write_arrow_ipc<I, W, F>(
    batches: I,
    mut writer_factory: F,
) -> Result<(), ArrowConvertError>
where
    I: Iterator<Item = Result<TypedBatch, ArrowConvertError>>,
    W: Write,
    F: FnMut(TypeTag) -> Result<W, ArrowConvertError>,
{
    let mut writers: HashMap<TypeTag, IpcFileWriter<W>> = HashMap::new();

    for batch_result in batches {
        let typed_batch = batch_result?;
        let writer = match writers.entry(typed_batch.tag) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let w = writer_factory(typed_batch.tag)?;
                let schema = schema_for(typed_batch.tag);
                let ipc_writer = IpcFileWriter::try_new(w, &schema)?;
                e.insert(ipc_writer)
            }
        };
        writer.write(&typed_batch.batch)?;
    }

    for (_, mut writer) in writers {
        writer.finish()?;
    }

    Ok(())
}

/// Write TypedBatches as CSV.
#[cfg(feature = "csv")]
pub fn write_csv<I, W, F>(
    batches: I,
    mut writer_factory: F,
) -> Result<(), ArrowConvertError>
where
    I: Iterator<Item = Result<TypedBatch, ArrowConvertError>>,
    W: Write,
    F: FnMut(TypeTag) -> Result<W, ArrowConvertError>,
{
    let mut writers: HashMap<TypeTag, arrow_csv::writer::Writer<W>> = HashMap::new();

    for batch_result in batches {
        let typed_batch = batch_result?;
        let writer = match writers.entry(typed_batch.tag) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let w = writer_factory(typed_batch.tag)?;
                let csv_writer = arrow_csv::writer::Writer::new(w);
                e.insert(csv_writer)
            }
        };
        writer.write(&typed_batch.batch)?;
    }

    for (_, writer) in writers {
        writer.into_inner().flush()?;
    }

    Ok(())
}

/// Write TypedBatches as line-delimited JSON.
#[cfg(feature = "json")]
pub fn write_json<I, W, F>(
    batches: I,
    mut writer_factory: F,
) -> Result<(), ArrowConvertError>
where
    I: Iterator<Item = Result<TypedBatch, ArrowConvertError>>,
    W: Write,
    F: FnMut(TypeTag) -> Result<W, ArrowConvertError>,
{
    let mut writers: HashMap<TypeTag, arrow_json::writer::LineDelimitedWriter<W>> = HashMap::new();

    for batch_result in batches {
        let typed_batch = batch_result?;
        let writer = match writers.entry(typed_batch.tag) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let w = writer_factory(typed_batch.tag)?;
                let json_writer = arrow_json::writer::LineDelimitedWriter::new(w);
                e.insert(json_writer)
            }
        };
        writer.write(&typed_batch.batch)?;
    }

    for (_, mut writer) in writers {
        writer.finish()?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batcher::TypedBatch;
    use crate::builders::StringBatchBuilder;
    use crate::schema::TypeTag;
    use crate::test_helpers::test_entry;
    use rdb_parser::RdbValue;

    fn make_string_batch(count: usize) -> TypedBatch {
        let mut builder = StringBatchBuilder::new();
        for i in 0..count {
            let key = format!("key{i}");
            builder.push(&test_entry(
                key.as_bytes(),
                RdbValue::String(format!("val{i}").into_bytes()),
            ));
        }
        TypedBatch {
            tag: TypeTag::String,
            batch: builder.finish().unwrap(),
        }
    }

    #[test]
    #[cfg(feature = "parquet")]
    fn parquet_roundtrip() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::arrow::ArrowWriter;
        use parquet::basic::Compression;
        use parquet::file::properties::WriterProperties;

        let batch = make_string_batch(5);
        let mut buf = Vec::new();
        {
            let props = WriterProperties::builder()
                .set_compression(Compression::UNCOMPRESSED)
                .build();
            let mut writer = ArrowWriter::try_new(
                &mut buf,
                std::sync::Arc::new(schema_for(TypeTag::String)),
                Some(props),
            )
            .unwrap();
            writer.write(&batch.batch).unwrap();
            writer.close().unwrap();
        }

        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(buf))
            .unwrap()
            .build()
            .unwrap();
        let read_batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(read_batches.len(), 1);
        assert_eq!(read_batches[0].num_rows(), 5);
        assert_eq!(read_batches[0].num_columns(), 9);
    }

    #[test]
    fn ipc_roundtrip() {
        use arrow::ipc::reader::FileReader as IpcFileReader;

        let batch = make_string_batch(3);
        let mut buf = Vec::new();
        {
            let schema = schema_for(TypeTag::String);
            let mut writer = IpcFileWriter::try_new(&mut buf, &schema).unwrap();
            writer.write(&batch.batch).unwrap();
            writer.finish().unwrap();
        }

        let cursor = std::io::Cursor::new(buf);
        let reader = IpcFileReader::try_new(cursor, None).unwrap();
        let read_batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(read_batches.len(), 1);
        assert_eq!(read_batches[0].num_rows(), 3);
    }

    #[test]
    #[cfg(feature = "csv")]
    fn csv_roundtrip() {
        let batch = make_string_batch(2);
        let mut buf = Vec::new();
        {
            let mut csv_writer = arrow_csv::writer::Writer::new(&mut buf);
            csv_writer.write(&batch.batch).unwrap();
        }

        let output = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = output.lines().collect();
        // Header + 2 data rows
        assert_eq!(lines.len(), 3, "expected header + 2 rows, got: {output}");
        assert!(lines[0].contains("db"), "header should contain column names");
    }

    #[test]
    #[cfg(feature = "parquet")]
    fn write_parquet_via_factory() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::arrow::ArrowWriter;
        use parquet::basic::Compression;
        use parquet::file::properties::WriterProperties;

        let batch = make_string_batch(4);

        let mut buf = Vec::new();
        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .build();
        let schema = std::sync::Arc::new(schema_for(TypeTag::String));
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        writer.write(&batch.batch).unwrap();
        writer.close().unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(buf))
            .unwrap()
            .build()
            .unwrap();
        let read: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(read[0].num_rows(), 4);
    }
}
