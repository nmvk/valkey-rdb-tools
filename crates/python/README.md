# valkey-rdb (Python)

Python bindings for valkey-rdb-tools. Reads Valkey/Redis RDB files into PyArrow tables with zero-copy Arrow transfer.

## Install

Requires Python 3.9+ and a Rust toolchain.

```bash
pip install maturin
maturin develop
```

## API

### `read(path, /, batch_size=65536) -> dict[str, pyarrow.Table]`

Parse an RDB file and return all data as PyArrow tables, keyed by type name.

```python
import valkey_rdb

tables = valkey_rdb.read("dump.rdb")
tables["string"].to_pandas()
tables["hash"].schema
```

### `read_batches(path, /, batch_size=65536) -> Iterator[(str, pyarrow.RecordBatch)]`

Lazy iterator that yields `(type_name, RecordBatch)` tuples. Useful for large files where you don't want everything in memory at once.

```python
for type_name, batch in valkey_rdb.read_batches("dump.rdb"):
    print(type_name, batch.num_rows)
```

### `to_parquet(path, output_dir, /, compression="zstd", batch_size=65536, row_group_size=1048576)`

Export an RDB file directly to Parquet files. One file per type.

```python
valkey_rdb.to_parquet("dump.rdb", "output/")
```

### `inspect(path) -> dict`

Returns a summary of the RDB file: magic, version, key counts by database and type.

```python
info = valkey_rdb.inspect("dump.rdb")
# {'magic': 'VALKEY', 'rdb_version': 80, 'server_version': '9.0.1' or None,
#  'total_keys': 1000, 'dbs': [{'db': 0, 'keys': 1000, 'types': {'string': 500, ...}}]}
```

## License

BSD-3-Clause
