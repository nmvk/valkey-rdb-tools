use std::collections::BTreeMap;
use std::fs::File;

use arrow::pyarrow::ToPyArrow;
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use std::collections::HashSet;

use rdb_parser::RdbReader;
use rdb_to_arrow::{
    metadata_from_rdb, parse_compression, summarize_entries, write_parquet, ArrowBatcher,
    ArrowConvertError, BatcherConfig, Heuristic, ParquetConfig, TypeTag,
};

fn parse_heuristics(s: &str) -> PyResult<HashSet<Heuristic>> {
    Heuristic::parse_set(s).map_err(pyo3::exceptions::PyValueError::new_err)
}

fn make_batcher_config(
    batch_size: usize,
    batch_bytes: Option<usize>,
    max_entry_bytes: Option<usize>,
    heuristics: &str,
) -> PyResult<BatcherConfig> {
    if batch_size == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err("batch_size must be > 0"));
    }
    let heuristics = parse_heuristics(heuristics)?;
    #[allow(clippy::field_reassign_with_default)] // conditional batch_bytes override
    let config = {
        let mut c = BatcherConfig::default();
        c.batch_size = batch_size;
        if let Some(bb) = batch_bytes {
            c.batch_bytes = Some(bb);
        }
        c.max_entry_bytes = max_entry_bytes;
        c.heuristics = heuristics;
        c
    };
    Ok(config)
}

/// Read an RDB file and return a dict of {type_name: pyarrow.Table}.
///
/// Collects all entries into memory. For large files, use read_batches() instead
/// to process one batch at a time.
#[pyfunction]
#[pyo3(signature = (path, /, batch_size=65536, no_chunking=false, batch_bytes=None, max_entry_bytes=None, heuristic="all"))]
fn read<'py>(
    py: Python<'py>,
    // Take `String` directly — the closure below needs to move an owned
    // path into the `allow_threads` block. Previously we took `&str`
    // and cloned it; letting pyo3 extract `String` up front skips the
    // redundant allocation.
    path: String,
    batch_size: usize,
    no_chunking: bool,
    batch_bytes: Option<usize>,
    max_entry_bytes: Option<usize>,
    heuristic: &str,
) -> PyResult<Bound<'py, PyDict>> {
    let config = make_batcher_config(batch_size, batch_bytes, max_entry_bytes, heuristic)?;

    // Release GIL during RDB parsing + Arrow batch construction
    let per_type: BTreeMap<TypeTag, Vec<RecordBatch>> = py.allow_threads(move || {
        let file = File::open(&path).map_err(ThreadError::Io)?;
        let reader = RdbReader::new(file).map_err(ThreadError::Rdb)?;
        let reader = if no_chunking {
            reader.without_chunking()
        } else {
            reader
        };

        let batcher = ArrowBatcher::new(config);
        let batch_iter = batcher.process(reader);

        let mut per_type: BTreeMap<TypeTag, Vec<RecordBatch>> = BTreeMap::new();
        for result in batch_iter {
            let typed_batch = result.map_err(ThreadError::Arrow)?;
            per_type
                .entry(typed_batch.tag)
                .or_default()
                .push(typed_batch.batch);
        }
        Ok::<_, ThreadError>(per_type)
    })?;

    // Convert to Python dict (needs GIL for PyArrow)
    let pa = py.import("pyarrow")?;
    let table_cls = pa.getattr("Table")?;

    let dict = PyDict::new(py);
    for (tag, batches) in per_type {
        let py_batches = PyList::empty(py);
        for batch in &batches {
            let py_batch = batch.to_pyarrow(py)?;
            py_batches.append(py_batch)?;
        }

        let py_table = table_cls.call_method1("from_batches", (py_batches,))?;
        dict.set_item(tag.as_str(), py_table)?;
    }

    Ok(dict)
}

/// Return a lazy iterator that yields (type_name, pyarrow.RecordBatch) tuples.
#[pyfunction]
#[pyo3(signature = (path, /, batch_size=65536, no_chunking=false, batch_bytes=None, max_entry_bytes=None, heuristic="all"))]
fn read_batches(
    py: Python<'_>,
    path: String,
    batch_size: usize,
    no_chunking: bool,
    batch_bytes: Option<usize>,
    max_entry_bytes: Option<usize>,
    heuristic: &str,
) -> PyResult<BatchReader> {
    let config = make_batcher_config(batch_size, batch_bytes, max_entry_bytes, heuristic)?;

    // Release GIL during file open + RDB header parsing
    let batch_iter = py.allow_threads(move || -> Result<_, ThreadError> {
        let file = File::open(&path).map_err(ThreadError::Io)?;
        let reader = RdbReader::new(file).map_err(ThreadError::Rdb)?;
        let reader = if no_chunking {
            reader.without_chunking()
        } else {
            reader
        };
        let batcher = ArrowBatcher::new(config);
        Ok(batcher.process(reader))
    })?;

    Ok(BatchReader {
        inner: Box::new(batch_iter),
    })
}

/// Python-visible iterator over `(type_name, pyarrow.RecordBatch)` pairs.
///
/// `unsendable` at the pyo3 layer forbids Python code from moving this
/// object between Python threads — each `BatchReader` is pinned to the
/// thread that constructed it. The inner iterator still needs `Send`
/// because [`Python::allow_threads`] releases the GIL before calling
/// `.next()`, and the closure it runs must be `Send`. The two bounds
/// apply to different layers (Python-side sharing vs. Rust-side GIL
/// release) and are intentionally both present.
#[pyclass(unsendable)]
struct BatchReader {
    inner: Box<dyn Iterator<Item = Result<rdb_to_arrow::TypedBatch, ArrowConvertError>> + Send>,
}

#[pymethods]
impl BatchReader {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<(String, PyObject)>> {
        // Release GIL during RDB parsing + Arrow batch construction
        let result = py.allow_threads(|| self.inner.next());
        match result {
            Some(Ok(typed_batch)) => {
                let name = typed_batch.tag.as_str().to_string();
                let py_batch = typed_batch.batch.to_pyarrow(py)?;
                Ok(Some((name, py_batch)))
            }
            Some(Err(e)) => Err(arrow_to_py_err(e)),
            None => Ok(None),
        }
    }
}

/// Error type used inside allow_threads closures (must be Send, unlike PyErr).
enum ThreadError {
    Io(std::io::Error),
    Rdb(rdb_parser::RdbError),
    Arrow(ArrowConvertError),
}

impl From<ThreadError> for pyo3::PyErr {
    fn from(e: ThreadError) -> Self {
        match e {
            ThreadError::Io(e) => pyo3::exceptions::PyOSError::new_err(e.to_string()),
            ThreadError::Rdb(e) => rdb_to_py_err(e),
            ThreadError::Arrow(e) => arrow_to_py_err(e),
        }
    }
}

/// Export an RDB file directly to Parquet files.
#[pyfunction]
#[pyo3(signature = (path, output_dir, /, compression="zstd", batch_size=65536, row_group_size=1048576, no_chunking=false, batch_bytes=None, max_entry_bytes=None, heuristic="all"))]
#[allow(clippy::too_many_arguments)]
fn to_parquet(
    py: Python<'_>,
    // Owned `String` inputs move directly into the `allow_threads`
    // closure, sparing the prior `path.to_string()` / `output_dir.to_string()`
    // dance at the entry point.
    path: String,
    output_dir: String,
    compression: &str,
    batch_size: usize,
    row_group_size: usize,
    no_chunking: bool,
    batch_bytes: Option<usize>,
    max_entry_bytes: Option<usize>,
    heuristic: &str,
) -> PyResult<()> {
    let batcher_config = make_batcher_config(batch_size, batch_bytes, max_entry_bytes, heuristic)?;
    if row_group_size == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "row_group_size must be > 0",
        ));
    }

    let compression =
        parse_compression(compression).map_err(pyo3::exceptions::PyValueError::new_err)?;

    // Snapshot the active heuristics before the batcher takes ownership
    // of the config — `metadata_from_rdb` below bakes them into the
    // `rdb.heuristics` Parquet tag so `validate` can reparse them.
    let heuristics_snapshot = batcher_config.heuristics.clone();

    py.allow_threads(move || -> Result<(), ThreadError> {
        let file = File::open(&path).map_err(ThreadError::Io)?;
        let reader = RdbReader::new(file).map_err(ThreadError::Rdb)?;
        let reader = if no_chunking {
            reader.without_chunking()
        } else {
            reader
        };
        let metadata = metadata_from_rdb(reader.metadata(), &heuristics_snapshot);

        let config = ParquetConfig {
            compression,
            max_row_group_size: row_group_size,
            file_metadata: metadata,
        };

        let output_path = std::path::Path::new(&output_dir);
        std::fs::create_dir_all(output_path).map_err(ThreadError::Io)?;

        let batcher = ArrowBatcher::new(batcher_config);
        let batches = batcher.process(reader);

        let writer_factory = |tag: TypeTag| -> Result<File, ArrowConvertError> {
            let filename = format!("{}.parquet", tag.as_str());
            let path = output_path.join(filename);
            File::create(&path).map_err(ArrowConvertError::Io)
        };

        write_parquet(batches, &config, writer_factory).map_err(ThreadError::Arrow)?;
        Ok(())
    })?;
    Ok(())
}

/// Inspect an RDB file and return summary statistics as a dict.
#[pyfunction]
#[pyo3(signature = (path, /, heuristic="all"))]
fn inspect(py: Python<'_>, path: String, heuristic: &str) -> PyResult<PyObject> {
    let heuristics = parse_heuristics(heuristic)?;

    let summary = py.allow_threads(move || -> Result<_, ThreadError> {
        let file = File::open(&path).map_err(ThreadError::Io)?;
        let mut reader = RdbReader::new(file).map_err(ThreadError::Rdb)?;
        summarize_entries(&mut reader, &heuristics).map_err(ThreadError::Rdb)
    })?;

    let result = PyDict::new(py);
    result.set_item("magic", format!("{}", summary.header.magic))?;
    result.set_item("rdb_version", summary.header.version)?;
    result.set_item(
        "server_version",
        summary.metadata.server_version().map(|s| s.to_string()),
    )?;
    result.set_item("total_keys", summary.total_keys)?;

    let dbs = PyList::empty(py);
    for (db, types) in &summary.per_db {
        let db_dict = PyDict::new(py);
        db_dict.set_item("db", db)?;
        db_dict.set_item(
            "keys",
            summary.per_db_keys.get(db).copied().unwrap_or(0),
        )?;
        let types_dict = PyDict::new(py);
        for (tag, count) in types {
            types_dict.set_item(tag.as_str(), count)?;
        }
        db_dict.set_item("types", types_dict)?;
        dbs.append(db_dict)?;
    }
    result.set_item("dbs", dbs)?;

    Ok(result.into())
}

fn rdb_to_py_err(e: rdb_parser::RdbError) -> pyo3::PyErr {
    match e {
        rdb_parser::RdbError::Io(io_err) => {
            pyo3::exceptions::PyOSError::new_err(io_err.to_string())
        }
        other => pyo3::exceptions::PyValueError::new_err(other.to_string()),
    }
}

fn arrow_to_py_err(e: ArrowConvertError) -> pyo3::PyErr {
    match e {
        ArrowConvertError::Io(io_err) => {
            pyo3::exceptions::PyOSError::new_err(io_err.to_string())
        }
        ArrowConvertError::Parser(rdb_err) => rdb_to_py_err(rdb_err),
        other => pyo3::exceptions::PyRuntimeError::new_err(other.to_string()),
    }
}

#[pymodule]
fn valkey_rdb(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read, m)?)?;
    m.add_function(wrap_pyfunction!(read_batches, m)?)?;
    m.add_function(wrap_pyfunction!(to_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(inspect, m)?)?;
    Ok(())
}
