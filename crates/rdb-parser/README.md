# rdb-parser

Zero-dependency Rust library for parsing Valkey and Redis RDB files. Reads the binary format and yields typed entries via the standard `Iterator` trait.

## Usage

```rust
use std::fs::File;
use std::io::BufReader;
use rdb_parser::RdbReader;

let file = File::open("dump.rdb").unwrap();
let reader = RdbReader::new(BufReader::new(file)).unwrap();

// Header and AUX metadata are available immediately
println!("RDB version: {}", reader.header().version);
println!("Server: {:?}", reader.metadata().server_version());

// Iterate over key-value entries
for entry in reader {
    let entry = entry.unwrap();
    println!("db={} key={:?} type={}", entry.db, entry.key, entry.type_name());
}
```

## What it parses

- **Header**: REDIS and VALKEY magic strings, RDB versions up to 80 (Valkey 9.0)
- **Metadata**: All AUX fields (server version, ctime, used-mem, repl-id, etc.)
- **Types**: String, List, Set, Sorted Set, Hash (including HASH_2 with per-field TTL)
- **Encodings**: Raw, LZF compressed, ziplist, listpack, intset, quicklist v1/v2
- **Key metadata**: Expiry (ms), LRU idle time, LFU frequency
- **Integrity**: CRC-64 checksum validation

Streams and modules are skipped (the parser stays aligned but does not decode their contents).

## Design

- Zero external dependencies — embeddable anywhere
- Streaming — reads entries one at a time, never buffers the whole file
- 512MB allocation cap protects against crafted inputs

## License

BSD-3-Clause
