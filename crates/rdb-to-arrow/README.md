# rdb-to-arrow

Converts parsed RDB entries into Arrow RecordBatches and writes them to Parquet, Arrow IPC, CSV, or JSON.

## Usage

```rust
use std::fs::File;
use std::io::BufReader;
use rdb_parser::RdbReader;
use rdb_to_arrow::*;

let reader = RdbReader::new(BufReader::new(File::open("dump.rdb").unwrap())).unwrap();
let heuristics: std::collections::HashSet<Heuristic> = Heuristic::ALL.iter().copied().collect();
let metadata = metadata_from_rdb(reader.metadata(), &heuristics);

let batcher = ArrowBatcher::new(BatcherConfig::default());
let batches = batcher.process(reader);

let config = ParquetConfig {
    file_metadata: metadata,
    ..Default::default()
};

write_parquet(batches, &config, |tag| {
    File::create(format!("{}.parquet", tag.as_str()))
        .map_err(ArrowConvertError::Io)
}).unwrap();
```

## Schemas

Each RDB type gets its own Arrow schema. All schemas share 8 common prefix columns (`db`, `key`, `type`, `expiry_ms`, `lru_idle_secs`, `lfu_frequency`, `encoding`, `num_elements`) plus type-specific columns.

Collections (lists, sets, hashes, sorted sets) are expanded to one row per element. Empty collections get a single row with null element columns.

## Virtual type detection

- **Geo** — Sorted sets where all scores are valid 52-bit geohashes are decoded into `(longitude, latitude)` columns.
- **HyperLogLog** — Strings with the `HYLL` magic header get `hll_encoding` and `cached_cardinality` columns.

## Output formats

- **Parquet** — Configurable compression (zstd, snappy, lz4, gzip, none) and row group size. RDB metadata is embedded as Parquet file metadata.
- **Arrow IPC** — Columnar binary format.
- **CSV** / **JSON** — For quick inspection or piping to other tools.

All writers use a factory pattern (`FnMut(TypeTag) -> Result<W>`) so callers control where output goes — files, buffers, network sinks.

## License

BSD-3-Clause
