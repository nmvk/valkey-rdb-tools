# valkey-rdb CLI

The `valkey-rdb` command-line tool for exporting RDB files.

## Install

```bash
cargo install --path .
```

## Commands

### export

```bash
valkey-rdb export dump.rdb                          # Parquet to ./dump/
valkey-rdb export dump.rdb -o out/ -f csv           # CSV to out/
valkey-rdb export dump.rdb -f arrow-ipc             # Arrow IPC
valkey-rdb export dump.rdb -f json                  # Line-delimited JSON
valkey-rdb export - -o out/                         # Read from stdin
```

Options:

| Flag | Default | Description |
|------|---------|-------------|
| `-o, --output` | input stem | Output directory |
| `-f, --format` | `parquet` | `parquet`, `arrow-ipc`, `csv`, `json` |
| `--compression` | `zstd` | `zstd`, `snappy`, `lz4`, `gzip`, `none` |
| `--db` | all | Filter by database number |
| `--type` | all | Filter by type (`string`, `list`, `set`, `zset`, `hash`, `geo`, `hll`) |
| `--key-pattern` | all | Filter keys by glob |
| `--batch-size` | 65536 | Rows per Arrow RecordBatch |
| `--row-group-size` | 1048576 | Rows per Parquet row group |
| `--shard-id` | none | Suffix for conflict-free parallel writes |

Output files are named `{type}.{ext}` (e.g., `string.parquet`, `hash.parquet`).

### schema

```bash
valkey-rdb schema                        # All types, text
valkey-rdb schema --type hash            # Single type
valkey-rdb schema --output json          # JSON format
```

## License

BSD-3-Clause
