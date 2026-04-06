# valkey-rdb-tools

Export Valkey/Redis RDB snapshots to Parquet, Arrow IPC, CSV, or JSON.

```
valkey-rdb export dump.rdb
```

This creates one Parquet file per data type (`string.parquet`, `hash.parquet`, etc.) in a directory named after the input file. Query them with DuckDB, Spark, Pandas, Polars — anything that reads Parquet.

```sql
SELECT key, value FROM 'dump/string.parquet' LIMIT 10;
```

## Install

Requires Rust 1.75+.

```bash
cargo install --path crates/cli
```

Or build from source:

```bash
cargo build --release -p valkey-rdb-cli
# Binary at target/release/valkey-rdb
```

## CLI Usage

### export

```bash
# Parquet (default, zstd compressed)
valkey-rdb export dump.rdb -o output/

# Arrow IPC
valkey-rdb export dump.rdb -f arrow-ipc

# CSV
valkey-rdb export dump.rdb -f csv

# Filter by database, type, or key pattern
valkey-rdb export dump.rdb --db 0 --type hash --key-pattern "user:*"

# Filter multiple types (comma-separated)
valkey-rdb export dump.rdb --type hash,geo

# Read from stdin
cat dump.rdb | valkey-rdb export - -o output/

# Custom compression
valkey-rdb export dump.rdb --compression snappy

# Bound memory: flush builders at ~50 MB, skip entries over 10 MB
valkey-rdb export dump.rdb --batch-bytes 50mb --max-entry-bytes 10mb
```

### validate

Verify exported Parquet files match the source RDB (row counts per type, CRC-64, metadata):

```bash
valkey-rdb validate dump.rdb output/
```

### schema

Print the Arrow schema for each type, or a specific one:

```bash
valkey-rdb schema
valkey-rdb schema --type geo --output json
```

## Python

Python bindings via PyO3 with zero-copy Arrow transfer.

```bash
cd crates/python
pip install maturin
maturin develop
```

```python
import valkey_rdb

# Get pyarrow Tables keyed by type
tables = valkey_rdb.read("dump.rdb")
tables["string"].to_pandas()

# Lazy batch iteration
for type_name, batch in valkey_rdb.read_batches("dump.rdb"):
    print(type_name, batch.num_rows)

# Direct RDB-to-Parquet
valkey_rdb.to_parquet("dump.rdb", "output/")

# File summary
valkey_rdb.inspect("dump.rdb")
```

## Architecture

Four crates in a streaming pipeline:

```
rdb-parser  -->  rdb-to-arrow  -->  cli / python
(Iterator)       (Batcher)          (Commands)
```

- **[rdb-parser](crates/rdb-parser/)** — Zero-dependency RDB parser. Reads the binary format and yields `RdbEntry` items via `Iterator`.
- **[rdb-to-arrow](crates/rdb-to-arrow/)** — Converts entries into Arrow RecordBatches. Handles virtual type detection (HyperLogLog exclusive from strings; Geo additive alongside sorted sets), batching, and writing to all output formats.
- **[cli](crates/cli/)** — The `valkey-rdb` binary. Commands: `export`, `schema`, and `validate`.
- **[python](crates/python/)** — PyO3 bindings exposing `read()`, `read_batches()`, `to_parquet()`, and `inspect()`.

### Type support

| Type | RDB Encodings | Arrow Schema |
|------|--------------|--------------|
| String | raw, INT8/16/32, LZF | 9 columns (key + value) |
| List | linkedlist, ziplist, quicklist v1/v2, listpack | 10 columns (one row per element) |
| Set | hashtable, intset, listpack | 9 columns (one row per member) |
| Sorted Set | v1, v2, ziplist, listpack | 10 columns (member + score) |
| Hash | hashtable, ziplist, listpack, HASH_2 | 11 columns (field + value + per-field TTL) |
| Geo (virtual, always-on) | Additive: sorted sets with geohash scores appear in both zset and geo output | 12 columns (member + lon/lat) |
| HyperLogLog (virtual, exclusive) | Detected from HYLL magic header; replaces string output | 11 columns (encoding + cardinality) |

Streams and modules are skipped during parsing.

### Design choices

- **Streaming** — Never loads an entire RDB into memory. Large plain-encoded collections are chunked automatically (default: 50K elements) to bound peak memory. Use `--batch-bytes` to cap builder memory and `--max-entry-bytes` to skip oversized keys.
- **Export only** — No RDB writing. RDB is Valkey's internal format; writing it externally is fragile.
- **Lean** — Zero cloud dependencies. No runtime config files. Bring your own upload logic.

## Build & Test

```bash
cargo build
cargo test
```

Run tests for a specific crate:

```bash
cargo test -p rdb-parser
cargo test -p rdb-to-arrow
```

Python tests require `maturin develop` first:

```bash
cd crates/python && maturin develop && pytest tests/
```

## References

- [Valkey source: rdb.c](https://github.com/valkey-io/valkey/blob/unstable/src/rdb.c) — RDB format reference
- [Valkey source: rdb.h](https://github.com/valkey-io/valkey/blob/unstable/src/rdb.h) — Opcodes and type constants
- [Apache Arrow Rust](https://docs.rs/arrow/latest/arrow/) — Arrow array builders and Parquet writer

## License

BSD-3-Clause
