use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
#[cfg(feature = "parquet")]
use std::collections::HashMap;
use std::io::Write;

use arrow::ipc::writer::FileWriter as IpcFileWriter;

use crate::batcher::TypedBatch;
use crate::error::ArrowConvertError;
use crate::schema::{schema_for, TypeTag};

/// Close/finish all writers in a map, returning the first error encountered
/// while ensuring every writer is finalized.
///
/// Takes a `BTreeMap` so close order is deterministic (sorted by
/// [`TypeTag`] variant order) — important for reproducible test output
/// and stable error-report sequencing.
fn close_all<V, E>(
    writers: BTreeMap<TypeTag, V>,
    mut close_fn: impl FnMut(V) -> Result<(), E>,
) -> Result<(), ArrowConvertError>
where
    E: Into<ArrowConvertError>,
{
    let mut first_err: Option<ArrowConvertError> = None;
    for (_, writer) in writers {
        if let Err(e) = close_fn(writer) {
            if first_err.is_none() {
                first_err = Some(e.into());
            }
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Shared driver for every format-specific writer.
///
/// Every `write_*` function follows the same pattern:
///   1. Open a fresh backend writer the first time a `TypeTag` appears
///      (via `writer_factory` + `open`).
///   2. Route each batch to the matching writer via `write`.
///   3. On upstream error or exhaustion, call `close` on every opened
///      writer so partial files aren't left unfinalized on disk.
///
/// Wrapping the populate loop in an IIFE lets early `?` errors fall
/// through to `close_all` (which always runs) rather than leaking
/// writers; upstream/write errors take precedence over close errors.
fn run_writer_pipeline<I, W, Wr, Factory, Open, WriteFn, CloseFn, CloseErr>(
    batches: I,
    mut writer_factory: Factory,
    mut open: Open,
    mut write: WriteFn,
    close: CloseFn,
) -> Result<(), ArrowConvertError>
where
    I: Iterator<Item = Result<TypedBatch, ArrowConvertError>>,
    Factory: FnMut(TypeTag) -> Result<W, ArrowConvertError>,
    Open: FnMut(TypeTag, W) -> Result<Wr, ArrowConvertError>,
    WriteFn: FnMut(&mut Wr, &arrow::array::RecordBatch) -> Result<(), ArrowConvertError>,
    CloseFn: FnMut(Wr) -> Result<(), CloseErr>,
    CloseErr: Into<ArrowConvertError>,
{
    let mut writers: BTreeMap<TypeTag, Wr> = BTreeMap::new();

    let populate_result: Result<(), ArrowConvertError> = (|| {
        for batch_result in batches {
            let typed_batch = batch_result?;
            let writer = match writers.entry(typed_batch.tag) {
                Entry::Occupied(e) => e.into_mut(),
                Entry::Vacant(e) => {
                    let raw = writer_factory(typed_batch.tag)?;
                    let wr = open(typed_batch.tag, raw)?;
                    e.insert(wr)
                }
            };
            write(writer, &typed_batch.batch)?;
        }
        Ok(())
    })();

    let close_result = close_all(writers, close);
    populate_result.and(close_result)
}

/// Configuration for Parquet output.
#[cfg(feature = "parquet")]
#[derive(Debug, Clone)]
pub struct ParquetConfig {
    /// Compression codec applied to each column chunk.
    pub compression: parquet::basic::Compression,
    /// Target rows per Parquet row group. Smaller values reduce per-row
    /// decode memory; larger values improve scan throughput.
    pub max_row_group_size: usize,
    /// Key-value metadata written into every Parquet file's Arrow schema.
    /// Useful for embedding AUX fields (server version, ctime, etc.) and
    /// export provenance.
    pub file_metadata: HashMap<String, String>,
}

/// Parse a CLI/config-friendly compression name into a Parquet compression
/// codec. Accepted names (case-insensitive): `zstd`, `snappy`, `lz4`,
/// `lz4-raw` / `lz4_raw`, `gzip`, `none`.
///
/// Shared between the CLI (via its `CompressionArg` `ValueEnum`) and the
/// Python binding, where the arg arrives as a plain string.
#[cfg(feature = "parquet")]
pub fn parse_compression(name: &str) -> Result<parquet::basic::Compression, String> {
    use parquet::basic::Compression;
    match name.to_ascii_lowercase().as_str() {
        "zstd" => Ok(Compression::ZSTD(Default::default())),
        "snappy" => Ok(Compression::SNAPPY),
        "lz4" => Ok(Compression::LZ4),
        "lz4-raw" | "lz4_raw" => Ok(Compression::LZ4_RAW),
        "gzip" => Ok(Compression::GZIP(Default::default())),
        "none" => Ok(Compression::UNCOMPRESSED),
        other => Err(format!(
            "unknown compression: '{other}'. Valid: zstd, snappy, lz4, lz4-raw, gzip, none"
        )),
    }
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
    writer_factory: F,
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
    let file_metadata = &config.file_metadata;

    run_writer_pipeline(
        batches,
        writer_factory,
        |tag, w| {
            let schema = if file_metadata.is_empty() {
                schema_for(tag)
            } else {
                schema_for(tag).with_metadata(file_metadata.clone())
            };
            Ok(ArrowWriter::try_new(
                w,
                std::sync::Arc::new(schema),
                Some(props.clone()),
            )?)
        },
        |wr, batch| wr.write(batch).map(|_| ()).map_err(Into::into),
        |w| w.close().map(|_| ()),
    )
}

/// Parquet file-metadata key recording the CLI/exporter identity. Used
/// as a sanity signal in `validate` — a missing value surfaces as a
/// warning because the file wasn't produced by this pipeline.
///
/// Gated on the `parquet` feature alongside [`metadata_from_rdb`] — the
/// key is only meaningful when producing or inspecting Parquet output.
#[cfg(feature = "parquet")]
pub const RDB_EXPORTED_BY_KEY: &str = "rdb.exported_by";

/// Parquet file-metadata key recording the set of heuristics active when
/// the file was produced. `validate` parses this back into a
/// `HashSet<Heuristic>` so the replay uses the same heuristic policy as
/// the original export.
#[cfg(feature = "parquet")]
pub const RDB_HEURISTICS_KEY: &str = "rdb.heuristics";

/// Provenance value written alongside [`RDB_EXPORTED_BY_KEY`].
#[cfg(feature = "parquet")]
pub const RDB_EXPORTED_BY_VALUE: &str = "valkey-rdb-tools";

/// Build Parquet file metadata from RDB metadata (AUX fields) and the
/// heuristic set that was active during export.
///
/// Output contains:
/// - Every AUX key/value from the RDB, prefixed with `rdb.`.
/// - [`RDB_EXPORTED_BY_KEY`] provenance tag (`valkey-rdb-tools`).
/// - [`RDB_HEURISTICS_KEY`] with the canonical string form of
///   `heuristics` (see [`crate::Heuristic::format_set`]).
///
/// Centralizing this lets `validate` reparse the metadata with
/// [`crate::Heuristic::parse_set`] and get the exact heuristic set the
/// exporter used — preventing drift between the CLI and Python
/// exporters that previously built this map independently.
#[cfg(feature = "parquet")]
pub fn metadata_from_rdb(
    meta: &rdb_parser::RdbMetadata,
    heuristics: &std::collections::HashSet<crate::Heuristic>,
) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for (k, v) in &meta.aux {
        map.insert(format!("rdb.{k}"), v.clone());
    }
    map.insert(
        RDB_EXPORTED_BY_KEY.to_string(),
        RDB_EXPORTED_BY_VALUE.to_string(),
    );
    map.insert(
        RDB_HEURISTICS_KEY.to_string(),
        crate::Heuristic::format_set(heuristics),
    );
    map
}

/// Write TypedBatches as Arrow IPC files.
pub fn write_arrow_ipc<I, W, F>(
    batches: I,
    writer_factory: F,
) -> Result<(), ArrowConvertError>
where
    I: Iterator<Item = Result<TypedBatch, ArrowConvertError>>,
    W: Write,
    F: FnMut(TypeTag) -> Result<W, ArrowConvertError>,
{
    run_writer_pipeline(
        batches,
        writer_factory,
        |tag, w| Ok(IpcFileWriter::try_new(w, &schema_for(tag))?),
        |wr, batch| wr.write(batch).map_err(Into::into),
        |mut w| w.finish(),
    )
}

/// Write TypedBatches as CSV.
#[cfg(feature = "csv")]
pub fn write_csv<I, W, F>(
    batches: I,
    writer_factory: F,
) -> Result<(), ArrowConvertError>
where
    I: Iterator<Item = Result<TypedBatch, ArrowConvertError>>,
    W: Write,
    F: FnMut(TypeTag) -> Result<W, ArrowConvertError>,
{
    run_writer_pipeline(
        batches,
        writer_factory,
        |_tag, w| Ok(arrow_csv::writer::Writer::new(w)),
        |wr, batch| wr.write(batch).map_err(Into::into),
        |w| w.into_inner().flush().map_err(ArrowConvertError::Io),
    )
}

/// Write TypedBatches as line-delimited JSON.
#[cfg(feature = "json")]
pub fn write_json<I, W, F>(
    batches: I,
    writer_factory: F,
) -> Result<(), ArrowConvertError>
where
    I: Iterator<Item = Result<TypedBatch, ArrowConvertError>>,
    W: Write,
    F: FnMut(TypeTag) -> Result<W, ArrowConvertError>,
{
    run_writer_pipeline(
        batches,
        writer_factory,
        |_tag, w| Ok(arrow_json::writer::LineDelimitedWriter::new(w)),
        |wr, batch| wr.write(batch).map_err(Into::into),
        |mut w| w.finish(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batcher::TypedBatch;
    use crate::builders::{BatchBuilder, StringBatchBuilder};
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

    /// Writer wrapper that drops finished bytes into a shared sink keyed
    /// by [`TypeTag`]. `write_parquet` requires `W: Send`, so the sink
    /// uses a `Mutex` (not `RefCell`) to stay `Sync`.
    #[cfg(feature = "parquet")]
    struct SinkWriter<'a> {
        tag: TypeTag,
        sink: &'a std::sync::Mutex<BTreeMap<TypeTag, Vec<u8>>>,
        buf: Vec<u8>,
    }

    #[cfg(feature = "parquet")]
    impl Write for SinkWriter<'_> {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.buf.write(b)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[cfg(feature = "parquet")]
    impl Drop for SinkWriter<'_> {
        fn drop(&mut self) {
            // If another thread poisoned the mutex by panicking mid-write,
            // recover the underlying map rather than double-panicking here
            // — the second panic in a Drop while unwinding would abort.
            let mut sink = self
                .sink
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            sink.insert(self.tag, std::mem::take(&mut self.buf));
        }
    }

    #[test]
    #[cfg(feature = "parquet")]
    fn write_parquet_batches_per_tag() {
        // Exercise the real `write_parquet` entry point end-to-end: feed a
        // mix of String and List batches through it, confirm the factory
        // is invoked once per tag with deterministic ordering, and the
        // written bytes round-trip via `ParquetRecordBatchReaderBuilder`.
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::basic::Compression;
        use rdb_parser::RdbEntry;
        use std::sync::Mutex;

        fn list_batch(count: usize) -> TypedBatch {
            use crate::builders::{BatchBuilder, ListBatchBuilder};
            let mut builder = ListBatchBuilder::new();
            for i in 0..count {
                let key = format!("lk{i}");
                let entry = RdbEntry::new(
                    key.into_bytes(),
                    RdbValue::List(vec![b"a".to_vec(), b"b".to_vec()]),
                    1, // RDB_TYPE_LIST
                );
                builder.push(&entry);
            }
            TypedBatch {
                tag: TypeTag::List,
                batch: builder.finish().unwrap(),
            }
        }

        let config = ParquetConfig {
            compression: Compression::UNCOMPRESSED,
            max_row_group_size: 1024,
            ..Default::default()
        };

        let sink: Mutex<BTreeMap<TypeTag, Vec<u8>>> = Mutex::new(BTreeMap::new());
        let batches: Vec<Result<TypedBatch, ArrowConvertError>> =
            vec![Ok(make_string_batch(5)), Ok(list_batch(3))];

        write_parquet(batches.into_iter(), &config, |tag| {
            Ok(SinkWriter {
                tag,
                sink: &sink,
                buf: Vec::new(),
            })
        })
        .unwrap();

        let out = sink.into_inner().unwrap();
        assert_eq!(
            out.keys().copied().collect::<Vec<_>>(),
            vec![TypeTag::String, TypeTag::List],
            "factory invoked once per tag and closed in TypeTag order"
        );

        let string_read: Vec<_> = ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(out[&TypeTag::String].clone()),
        )
        .unwrap()
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        assert_eq!(string_read[0].num_rows(), 5);

        let list_read: Vec<_> = ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(out[&TypeTag::List].clone()),
        )
        .unwrap()
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        // 3 list entries × 2 elements each = 6 exploded rows
        assert_eq!(list_read[0].num_rows(), 6);
    }

    /// Pin the always-finalize invariant: when the batch iterator yields
    /// an error partway through, writers that have already been opened
    /// must still be closed. Otherwise partial IPC files would be left
    /// on disk without their footer (and Parquet files without the
    /// row-group index), and `close_all` close errors would never
    /// surface.
    #[test]
    fn ipc_finalizes_writers_even_on_upstream_error() {
        use arrow::ipc::reader::FileReader as IpcFileReader;
        use std::sync::Mutex;

        let sink: Mutex<BTreeMap<TypeTag, Vec<u8>>> = Mutex::new(BTreeMap::new());

        /// Writer that captures its buffer into the shared sink on drop.
        /// Detects whether the writer was closed (via an `io::Error`
        /// deliberately triggered in `write`, which doesn't happen
        /// here) — we only need the drop to fire to know finalization
        /// reached this writer.
        struct IpcSinkWriter<'a> {
            tag: TypeTag,
            sink: &'a Mutex<BTreeMap<TypeTag, Vec<u8>>>,
            buf: Vec<u8>,
        }
        impl Write for IpcSinkWriter<'_> {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.buf.write(b)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl Drop for IpcSinkWriter<'_> {
            fn drop(&mut self) {
                self.sink
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(self.tag, std::mem::take(&mut self.buf));
            }
        }

        // Yield one valid batch, then an error. Without always-finalize
        // the writer opened for the first batch would drop without its
        // IPC footer written, leaving an unreadable file.
        let batches: Vec<Result<TypedBatch, ArrowConvertError>> = vec![
            Ok(make_string_batch(2)),
            Err(ArrowConvertError::Parser(rdb_parser::RdbError::CorruptData(
                "synthetic upstream error".into(),
            ))),
        ];

        let result = write_arrow_ipc(batches.into_iter(), |tag| {
            Ok(IpcSinkWriter {
                tag,
                sink: &sink,
                buf: Vec::new(),
            })
        });
        assert!(result.is_err(), "upstream error must still propagate");

        let out = sink.into_inner().unwrap();
        assert!(
            out.contains_key(&TypeTag::String),
            "writer must have been opened and its buffer flushed to sink"
        );

        // The finalized IPC file must be readable end-to-end. If finish()
        // wasn't called, IpcFileReader would fail to locate the footer.
        let reader = IpcFileReader::try_new(
            std::io::Cursor::new(out[&TypeTag::String].clone()),
            None,
        )
        .expect("IPC file should be properly finalized even though the stream errored");
        let read_batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(read_batches[0].num_rows(), 2);
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

    /// Writer wrapper that captures finished bytes into a shared sink
    /// keyed by `TypeTag`. Used by the CSV/JSON roundtrip tests in the
    /// same way `SinkWriter` is used for Parquet. These writers do not
    /// require `Send`, so a `RefCell` is enough.
    struct CellSink<'a> {
        tag: TypeTag,
        sink: &'a std::cell::RefCell<BTreeMap<TypeTag, Vec<u8>>>,
        buf: Vec<u8>,
    }

    impl Write for CellSink<'_> {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.buf.write(b)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Drop for CellSink<'_> {
        fn drop(&mut self) {
            self.sink
                .borrow_mut()
                .insert(self.tag, std::mem::take(&mut self.buf));
        }
    }

    /// Drive `write_csv` end-to-end with two tags and verify both files
    /// have the expected headers + row counts. Previously `csv_roundtrip`
    /// instantiated `arrow_csv::writer::Writer` directly and never
    /// exercised the real entry point.
    #[test]
    #[cfg(feature = "csv")]
    fn write_csv_roundtrip() {
        use std::cell::RefCell;

        let sink: RefCell<BTreeMap<TypeTag, Vec<u8>>> = RefCell::new(BTreeMap::new());
        let batches: Vec<Result<TypedBatch, ArrowConvertError>> = vec![
            Ok(make_string_batch(3)),
            Ok(make_string_batch(2)), // two string batches collapse into one writer
        ];

        write_csv(batches.into_iter(), |tag| {
            Ok(CellSink {
                tag,
                sink: &sink,
                buf: Vec::new(),
            })
        })
        .unwrap();

        let out = sink.into_inner();
        let bytes = out
            .get(&TypeTag::String)
            .expect("write_csv must emit a String file");
        let text = std::str::from_utf8(bytes).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        // Header + 5 data rows (3 + 2, single writer reused across batches).
        assert_eq!(
            lines.len(),
            1 + 5,
            "expected header + 5 rows, got {} lines: {text}",
            lines.len()
        );
        assert!(
            lines[0].contains("db") && lines[0].contains("key"),
            "header should contain expected column names"
        );
    }

    /// Drive `write_json` end-to-end with two tags and verify the output
    /// is line-delimited JSON that parses back and yields the expected
    /// row counts per tag.
    #[test]
    #[cfg(feature = "json")]
    fn write_json_roundtrip() {
        use std::cell::RefCell;

        let sink: RefCell<BTreeMap<TypeTag, Vec<u8>>> = RefCell::new(BTreeMap::new());

        // Second batch is for the Hash tag so we can verify per-tag
        // routing (two different files land in the sink).
        fn hash_batch() -> TypedBatch {
            use crate::builders::{BatchBuilder, HashBatchBuilder};
            use rdb_parser::{HashField, RdbEntry};
            let mut b = HashBatchBuilder::new();
            let entry = RdbEntry::new(
                b"h".to_vec(),
                RdbValue::Hash(vec![HashField {
                    field: b"f".to_vec(),
                    value: b"v".to_vec(),
                    expiry_ms: None,
                }]),
                4, // RDB_TYPE_HASH
            );
            b.push(&entry);
            TypedBatch {
                tag: TypeTag::Hash,
                batch: b.finish().unwrap(),
            }
        }

        let batches: Vec<Result<TypedBatch, ArrowConvertError>> =
            vec![Ok(make_string_batch(2)), Ok(hash_batch())];

        write_json(batches.into_iter(), |tag| {
            Ok(CellSink {
                tag,
                sink: &sink,
                buf: Vec::new(),
            })
        })
        .unwrap();

        let out = sink.into_inner();

        let string_bytes = out
            .get(&TypeTag::String)
            .expect("write_json must emit a String file");
        let string_text = std::str::from_utf8(string_bytes).unwrap();
        let string_lines: Vec<&str> = string_text.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(string_lines.len(), 2, "expected 2 JSON rows: {string_text}");
        for line in &string_lines {
            let v: serde_json::Value = serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("each line should parse as JSON ({e}): {line}"));
            assert!(v.get("db").is_some(), "db column expected in: {line}");
        }

        let hash_bytes = out
            .get(&TypeTag::Hash)
            .expect("write_json must emit a Hash file");
        let hash_text = std::str::from_utf8(hash_bytes).unwrap();
        let hash_lines: Vec<&str> = hash_text.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(hash_lines.len(), 1, "hash payload has 1 field → 1 row");
    }
}
