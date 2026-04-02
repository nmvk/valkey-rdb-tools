use std::collections::BTreeMap;
use std::fs::File;

use arrow::pyarrow::ToPyArrow;
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use rdb_parser::RdbReader;
use rdb_to_arrow::{
    metadata_from_rdb, type_tag_for, write_parquet, ArrowBatcher, ArrowConvertError,
    BatcherConfig, ParquetConfig, TypeTag,
};

/// Read an RDB file and return a dict of {type_name: pyarrow.Table}.
///
/// Collects all entries into memory. For large files, use read_batches() instead
/// to process one batch at a time.
#[pyfunction]
#[pyo3(signature = (path, /, batch_size=65536))]
fn read<'py>(py: Python<'py>, path: &str, batch_size: usize) -> PyResult<Bound<'py, PyDict>> {
    if batch_size == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "batch_size must be > 0",
        ));
    }

    let file = File::open(path).map_err(io_to_py_err)?;
    let reader = RdbReader::new(file).map_err(rdb_to_py_err)?;

    let batcher = ArrowBatcher::new(BatcherConfig { batch_size });
    let batch_iter = batcher.process(reader);

    // Collect batches per type
    let mut per_type: BTreeMap<TypeTag, Vec<RecordBatch>> = BTreeMap::new();

    for result in batch_iter {
        let typed_batch = result.map_err(arrow_to_py_err)?;
        per_type
            .entry(typed_batch.tag)
            .or_default()
            .push(typed_batch.batch);
    }

    // Convert to Python dict of {type_name: pyarrow.Table}
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
#[pyo3(signature = (path, /, batch_size=65536))]
fn read_batches(path: &str, batch_size: usize) -> PyResult<BatchReader> {
    if batch_size == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "batch_size must be > 0",
        ));
    }

    let file = File::open(path).map_err(io_to_py_err)?;
    let reader = RdbReader::new(file).map_err(rdb_to_py_err)?;

    let batcher = ArrowBatcher::new(BatcherConfig { batch_size });
    let batch_iter = batcher.process(reader);

    Ok(BatchReader {
        inner: Box::new(batch_iter),
    })
}

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
        match self.inner.next() {
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
#[pyo3(signature = (path, output_dir, /, compression="zstd", batch_size=65536, row_group_size=1048576))]
fn to_parquet(
    py: Python<'_>,
    path: &str,
    output_dir: &str,
    compression: &str,
    batch_size: usize,
    row_group_size: usize,
) -> PyResult<()> {
    if batch_size == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "batch_size must be > 0",
        ));
    }
    if row_group_size == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "row_group_size must be > 0",
        ));
    }

    let compression = match compression.to_ascii_lowercase().as_str() {
        "zstd" => parquet::basic::Compression::ZSTD(Default::default()),
        "snappy" => parquet::basic::Compression::SNAPPY,
        "lz4" => parquet::basic::Compression::LZ4,
        "gzip" => parquet::basic::Compression::GZIP(Default::default()),
        "none" => parquet::basic::Compression::UNCOMPRESSED,
        _ => {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "unknown compression: '{compression}'. Valid: zstd, snappy, lz4, gzip, none"
            )))
        }
    };

    let path = path.to_string();
    let output_dir = output_dir.to_string();

    py.allow_threads(move || -> Result<(), ThreadError> {
        let file = File::open(&path).map_err(ThreadError::Io)?;
        let reader = RdbReader::new(file).map_err(ThreadError::Rdb)?;
        let metadata = metadata_from_rdb(reader.metadata());

        let config = ParquetConfig {
            compression,
            max_row_group_size: row_group_size,
            file_metadata: metadata,
        };

        let output_path = std::path::Path::new(&output_dir);
        std::fs::create_dir_all(output_path).map_err(ThreadError::Io)?;

        let batcher = ArrowBatcher::new(BatcherConfig { batch_size });
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
fn inspect(py: Python<'_>, path: &str) -> PyResult<PyObject> {
    let path = path.to_string();

    // Do all the I/O without the GIL
    let (header, metadata, total_keys, db_type_counts) = py.allow_threads(
        move || -> Result<_, ThreadError> {
            let file = File::open(&path).map_err(ThreadError::Io)?;
            let reader = RdbReader::new(file).map_err(ThreadError::Rdb)?;

            let header = reader.header().clone();
            let metadata = reader.metadata().clone();

            let mut db_type_counts: BTreeMap<u32, BTreeMap<String, u64>> = BTreeMap::new();
            let mut total_keys: u64 = 0;

            for entry_result in reader {
                let entry = entry_result.map_err(ThreadError::Rdb)?;
                total_keys += 1;
                let type_name = match type_tag_for(&entry) {
                    Some(tag) => tag.as_str().to_string(),
                    None => entry.type_name().to_string(),
                };
                *db_type_counts
                    .entry(entry.db)
                    .or_default()
                    .entry(type_name)
                    .or_insert(0) += 1;
            }

            Ok((header, metadata, total_keys, db_type_counts))
        },
    )?;

    // Build the Python dict (needs the GIL)
    let result = PyDict::new(py);
    result.set_item("magic", format!("{}", header.magic))?;
    result.set_item("rdb_version", header.version)?;
    result.set_item(
        "server_version",
        metadata.server_version().map(|s| s.to_string()),
    )?;
    result.set_item("total_keys", total_keys)?;

    let dbs = PyList::empty(py);
    for (db, types) in &db_type_counts {
        let db_dict = PyDict::new(py);
        db_dict.set_item("db", db)?;
        db_dict.set_item("keys", types.values().sum::<u64>())?;
        let types_dict = PyDict::new(py);
        for (type_name, count) in types {
            types_dict.set_item(type_name.as_str(), count)?;
        }
        db_dict.set_item("types", types_dict)?;
        dbs.append(db_dict)?;
    }
    result.set_item("dbs", dbs)?;

    Ok(result.into())
}

fn io_to_py_err(e: std::io::Error) -> pyo3::PyErr {
    pyo3::exceptions::PyOSError::new_err(e.to_string())
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
