# valkey-rdb-tools

High-performance RDB-to-Parquet/Arrow export tool for Valkey and Redis. Converts RDB snapshots into formats used by the modern data ecosystem — Parquet, Arrow IPC, CSV, JSON.

```
valkey-rdb export dump.rdb -o output.parquet
```

Then query with DuckDB, Spark, Snowflake, Pandas, Polars — any tool that reads Parquet.

```sql
SELECT key, type, size_bytes FROM 'output.parquet' ORDER BY size_bytes DESC LIMIT 10;
```

## Architecture

Three Rust crates in a pipeline:

```
┌──────────────┐     ┌───────────────┐     ┌─────┐
│  rdb-parser  │ --> │ rdb-to-arrow  │ --> │ cli │
│  (Iterator)  │     │  (Batcher)    │     │     │
└──────────────┘     └───────────────┘     └─────┘
```

- **`rdb-parser`** — Reads RDB binary format, yields `RdbEntry` items via `Iterator` trait
- **`rdb-to-arrow`** — Converts entries into Arrow RecordBatches, writes Parquet/Arrow IPC
- **`cli`** — Three commands: `export`, `inspect`, `schema`

### Design principles

- **Streaming** — Never loads an entire RDB file into memory. A 100GB file uses the same ~30MB as a 1MB file.
- **Lean SDK** — Zero cloud dependencies. Users bring their own upload logic.
- **Export only** — No RDB import. RDB is Valkey's internal format; writing it externally is fragile.
- **One file at a time** — Users orchestrate parallelism with their own tools (GNU parallel, Airflow, K8s jobs).

## Current status

The **rdb-parser** foundation is implemented and tested (29 tests passing):

| Component | Status |
|-----------|--------|
| Header parsing (REDIS + VALKEY magic) | Done |
| Length encoding (6/14/32/64-bit) | Done |
| String decoding (raw, INT8/16/32, LZF) | Done |
| Opcode state machine (AUX, SELECTDB, RESIZEDB, EXPIRETIME, IDLE, FREQ, EOF, FUNCTION2, SLOT_INFO/IMPORT) | Done |
| Iterator trait for streaming | Done |
| STRING type (type 0) parsing | Done |
| Skip logic for all other types (stream stays aligned) | Done |
| Test fixtures (8 RDB files) | Done |

## Build and test

```bash
cargo build
cargo test -p rdb-parser
```

Run a single test:
```bash
cargo test -p rdb-parser test_fixture_expiry
```

## Pending work — non-blocking tasks

These tasks can be worked on independently. No task below blocks another unless noted.

### Compact encoding decoders (`crates/rdb-parser/src/`)

These decoders are needed before the data type parsers can produce actual values. Each takes a `&[u8]` blob and returns decoded elements.

| Task | Input | Output | Reference |
|------|-------|--------|-----------|
| **Listpack decoder** | raw listpack bytes | `Vec<Vec<u8>>` of entries | `valkey/src/listpack.c` — entries are: `[encoding_byte, data, backlen]` |
| **Ziplist decoder** | raw ziplist bytes | `Vec<Vec<u8>>` of entries | `valkey/src/ziplist.c` — header: `[u32 total_bytes, u32 tail_offset, u16 num_entries]`, entries: `[prevlen, encoding, data]` |
| **Intset decoder** | raw intset bytes | `Vec<i64>` | `valkey/src/intset.h` — header: `[u32 encoding (2/4/8), u32 length]`, then N ints in the declared width |
| **Quicklist v1/v2** | type code + stream | `Vec<Vec<u8>>` list elements | v1: N ziplist nodes. v2: N `(container_type, listpack_node)` pairs. Container type 1 = plain, 2 = listpack |

### Data type parsers (`crates/rdb-parser/src/reader.rs` — `read_value()`)

Each type has a skip stub in `read_value()` already. Replace the stub with actual decoding. The return types are defined in `types.rs`.

| Task | RDB type codes | Returns | Notes |
|------|---------------|---------|-------|
| **List** | 1 (linkedlist), 10 (ziplist), 14 (quicklist), 18 (quicklist2) | `RdbValue::List(Vec<Vec<u8>>)` | Needs: listpack, ziplist, quicklist decoders |
| **Set** | 2 (hashtable), 11 (intset), 20 (listpack) | `RdbValue::Set(Vec<Vec<u8>>)` | Needs: intset, listpack decoders |
| **Hash** | 4 (hashtable), 9 (zipmap), 13 (ziplist), 16 (listpack) | `RdbValue::Hash(Vec<HashField>)` | Needs: listpack, ziplist, zipmap decoders. Zipmap is rare (old Redis). |
| **Sorted Set** | 3 (v1), 5 (v2), 12 (ziplist), 17 (listpack) | `RdbValue::SortedSet(Vec<(Vec<u8>, f64)>)` | v1 stores score as string, v2 as 8-byte IEEE 754 double |
| **HASH_2** | 22 | `RdbValue::Hash(Vec<HashField>)` with `expiry_ms` populated | Valkey 9.0 only. Per-field TTL. Skip stub already reads the data, just needs to build HashField structs |
| **Stream** | 15, 19, 21 | New `RdbValue::Stream(...)` type needed | Radix tree + listpack entries + consumer groups. See `rdbLoadObject` in `rdb.c`. Skip logic already implemented in `skip_stream()` |

### Arrow and Parquet layer (`crates/rdb-to-arrow/`)

| Task | File | What it does |
|------|------|-------------|
| **Per-type Arrow schemas** | `schema.rs` | Define Arrow schemas for each RdbValue variant. Default schema: `(key: Utf8, value: Utf8/Binary, db: UInt32, type: Utf8, expiry_ms: Int64, ...)` |
| **RecordBatch builders** | `converter.rs` | Take `Vec<RdbEntry>`, build Arrow arrays using `StringBuilder`, `UInt32Builder`, etc. One builder per schema column |
| **ArrowBatcher** | new file | Accumulates entries up to a configurable batch size (default 10K rows), flushes to Arrow RecordBatch, drops the batch. This is what keeps memory constant |
| **Parquet writer** | new file | Uses `arrow::parquet::ArrowWriter` to write RecordBatches to any `impl Write`. Configurable compression (zstd, snappy, none). Writes custom file metadata |
| **Arrow IPC writer** | new file | Uses `arrow::ipc::writer::FileWriter` for Arrow IPC format output |

### CLI (`crates/cli/`)

| Task | Command | What it does |
|------|---------|-------------|
| **`export`** | `valkey-rdb export dump.rdb -o output.parquet` | Reads RDB, pipes through ArrowBatcher, writes Parquet/Arrow/CSV/JSON. Format auto-detected from extension |
| **`inspect`** | `valkey-rdb inspect dump.rdb` | Quick mode: show header + metadata. Deep mode (`--top-keys N`): stream through file, track top-N by size, show type distribution and TTL stats |
| **`schema`** | `valkey-rdb schema [--type hash]` | Print the Arrow schema for a given type. Useful for data engineers setting up downstream tables |
| **Partitioned output** | `--partition-by db,type` | Hive-style directory partitioning: `output/db=0/type=string/part.parquet` |
| **Filtering** | `--db 0 --type hash --key-pattern "user:*"` | Skip entries at parse time that don't match filters |

### Testing

| Task | What |
|------|------|
| **Encoding-specific fixtures** | Generate RDB files that force specific encodings (ziplist vs listpack, intset, quicklist v1 vs v2) by tweaking Valkey config thresholds |
| **Stream fixtures** | Generate RDB files with streams, consumer groups, and PEL entries |
| **E2E test** | RDB file → export to Parquet → query with DuckDB → assert results match expected values |
| **CRC64 validation** | Implement CRC64 checksum verification on the EOF trailer. Reference: `valkey/src/crc64.c` |

### Future

| Task | What |
|------|------|
| **Python bindings** | PyO3 + maturin. Expose `read(path) -> pyarrow.Table`. Zero-copy via Arrow PyCapsule interface |
| **Progress reporting** | Byte-position progress bar for large files (indicatif crate) |
| **Error recovery** | Skip corrupt entries instead of aborting. Report warnings for skipped entries |

## Test fixtures

Located in `tests/fixtures/`. Generated with `tests/fixtures/generate_fixtures.sh` using a local Valkey build at `/Users/raghav/valkeys/valkey/src/`.

| File | Magic | Size | Contents |
|------|-------|------|----------|
| `basic.rdb` | VALKEY080 | 310B | One of each type (string, hash, list, set, zset) |
| `redis_compat.rdb` | REDIS0009 | 329B | Same data with older encodings (ziplist, quicklist v1) |
| `empty.rdb` | VALKEY080 | 95B | No keys |
| `multi_db.rdb` | VALKEY080 | 149B | Keys in db 0 and db 1 |
| `encodings.rdb` | VALKEY080 | ~10KB | Various encodings forced via config thresholds |
| `expiry.rdb` | VALKEY080 | 235B | Keys with/without/past TTL |
| `hash_field_ttl.rdb` | VALKEY080 | 248B | HASH_2 with per-field TTL (Valkey 9.0) |
| `streams.rdb` | VALKEY080 | 561B | Streams with consumer groups |

## References

- [Valkey source: rdb.c](https://github.com/valkey-io/valkey/blob/unstable/src/rdb.c) — the authoritative reference for RDB format
- [Valkey source: rdb.h](https://github.com/valkey-io/valkey/blob/unstable/src/rdb.h) — opcodes and type constants
- [Valkey source: listpack.c](https://github.com/valkey-io/valkey/blob/unstable/src/listpack.c) — listpack encoding
- [Apache Arrow Rust](https://docs.rs/arrow/latest/arrow/) — Arrow array builders and Parquet writer
