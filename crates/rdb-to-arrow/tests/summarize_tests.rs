//! Integration tests for `summarize_entries`, covering the paths that
//! the in-crate unit test (`empty_rdb_summary_is_zero`) doesn't reach:
//! per-database breakdown, chunked collections (first-chunk-only key
//! counts), and additive geo counting.

use std::collections::HashSet;

use rdb_parser::RdbReader;
use rdb_to_arrow::{summarize_entries, Heuristic, TypeTag};

fn fixture_path(name: &str) -> String {
    format!(
        "{}/../../tests/fixtures/{name}",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn all_heuristics() -> HashSet<Heuristic> {
    Heuristic::ALL.iter().copied().collect()
}

#[test]
fn summarize_basic_rdb_matches_expected_counts() {
    // basic.rdb has exactly 5 keys in db 0: 1 string (+1 expiring string),
    // 1 list[3], 1 set[3], 1 zset[3], 1 hash[3]. Pin those totals here so
    // regressions in first-chunk detection or type routing fail loudly.
    let mut reader = RdbReader::new(std::fs::File::open(fixture_path("basic.rdb")).unwrap()).unwrap();
    let summary = summarize_entries(&mut reader, &all_heuristics()).unwrap();

    // 2 strings + 1 list + 1 set + 1 zset + 1 hash = 6 distinct keys.
    assert_eq!(summary.total_keys, 6);

    let db0_keys = summary.per_db_keys.get(&0).copied().unwrap_or(0);
    assert_eq!(db0_keys, 6);

    // Per-tag key counts.
    assert_eq!(summary.totals.get(&TypeTag::String).map(|t| t.keys), Some(2));
    assert_eq!(summary.totals.get(&TypeTag::List).map(|t| t.keys), Some(1));
    assert_eq!(summary.totals.get(&TypeTag::Set).map(|t| t.keys), Some(1));
    assert_eq!(summary.totals.get(&TypeTag::SortedSet).map(|t| t.keys), Some(1));
    assert_eq!(summary.totals.get(&TypeTag::Hash).map(|t| t.keys), Some(1));

    // Row counts reflect the flat Arrow output (collections exploded).
    assert_eq!(summary.totals.get(&TypeTag::List).map(|t| t.rows), Some(3));
    assert_eq!(summary.totals.get(&TypeTag::Set).map(|t| t.rows), Some(3));
    assert_eq!(summary.totals.get(&TypeTag::SortedSet).map(|t| t.rows), Some(3));
    assert_eq!(summary.totals.get(&TypeTag::Hash).map(|t| t.rows), Some(3));

    // basic.rdb contains no geohash-like scores, so the additive Geo
    // bucket must stay empty even with the heuristic on.
    assert!(!summary.totals.contains_key(&TypeTag::Geo));
}

#[test]
fn summarize_multi_db_breaks_down_per_database() {
    // multi_db.rdb has entries in multiple DBs; pin that per_db is
    // populated with more than one entry and per_db_keys sums to
    // total_keys.
    let mut reader = RdbReader::new(std::fs::File::open(fixture_path("multi_db.rdb")).unwrap()).unwrap();
    let summary = summarize_entries(&mut reader, &all_heuristics()).unwrap();

    assert!(
        summary.per_db.len() >= 2,
        "multi_db.rdb should populate >= 2 databases, got {}",
        summary.per_db.len()
    );
    let per_db_total: u64 = summary.per_db_keys.values().sum();
    assert_eq!(
        per_db_total, summary.total_keys,
        "per_db_keys should sum to total_keys"
    );
    // Every db entry in `per_db` must also have a matching `per_db_keys`
    // entry — otherwise consumers building a "db X has N keys" view
    // would silently report zero.
    for db in summary.per_db.keys() {
        assert!(
            summary.per_db_keys.contains_key(db),
            "per_db contains db {db} with no per_db_keys entry"
        );
    }
}

#[test]
fn summarize_chunked_list_counts_each_key_once() {
    // encodings.rdb contains multi-element collections. Forcing a small
    // max_key_elements makes the reader emit multiple chunks per key —
    // summarize must still only count first chunks when tallying keys.
    let reader = RdbReader::new(std::fs::File::open(fixture_path("encodings.rdb")).unwrap())
        .unwrap()
        .with_max_key_elements(2);

    // Baseline: count at the same chunking level but ignore chunking.
    let mut reader_baseline =
        RdbReader::new(std::fs::File::open(fixture_path("encodings.rdb")).unwrap())
            .unwrap()
            .without_chunking();
    let baseline = summarize_entries(&mut reader_baseline, &all_heuristics()).unwrap();

    let mut chunked = reader;
    let chunked_summary = summarize_entries(&mut chunked, &all_heuristics()).unwrap();

    assert_eq!(
        chunked_summary.total_keys, baseline.total_keys,
        "chunked iteration must not inflate distinct-key count"
    );
    // Row counts should also match because chunking just splits a key
    // across multiple entries — the sum of expanded rows is invariant.
    for (tag, bl_count) in &baseline.totals {
        let ch_count = chunked_summary.totals.get(tag).copied().unwrap_or_default();
        assert_eq!(
            ch_count.rows, bl_count.rows,
            "type {tag:?}: chunked row count {} != baseline {}",
            ch_count.rows, bl_count.rows
        );
        assert_eq!(
            ch_count.keys, bl_count.keys,
            "type {tag:?}: chunked key count {} != baseline {}",
            ch_count.keys, bl_count.keys
        );
    }
}

/// Build a synthetic RDB payload whose single entry is a sorted set with
/// valid 52-bit geohash scores. Verifies the additive geo branch of
/// `summarize_entries`: when the Geo heuristic is enabled, the same key
/// must appear under both `SortedSet` and `Geo`.
#[test]
fn summarize_geo_additive_under_heuristic() {
    use std::io::Cursor;

    // Pull opcode/type bytes from rdb-parser's public `opcodes` module
    // so an upstream renumbering fails the build rather than silently
    // producing a malformed synthetic RDB.
    use rdb_parser::opcodes::{RDB_OPCODE_EOF, RDB_OPCODE_SELECTDB, RDB_TYPE_ZSET_2};

    fn rdb_len(n: u64) -> Vec<u8> {
        if n < 64 {
            vec![n as u8]
        } else {
            // 14-bit encoding: top 2 bits = 01, next 14 bits = length.
            vec![0x40 | ((n >> 8) as u8 & 0x3F), (n & 0xFF) as u8]
        }
    }
    fn rdb_string(s: &[u8]) -> Vec<u8> {
        let mut out = rdb_len(s.len() as u64);
        out.extend_from_slice(s);
        out
    }

    // Build a ZSET_2 payload: member count + (member, f64 score) pairs.
    // Two members with valid 52-bit geohash scores taken from the
    // `detect` module's test fixtures elsewhere in the crate.
    let mut payload = rdb_len(2);
    payload.extend_from_slice(&rdb_string(b"A"));
    payload.extend_from_slice(&3_479_099_956_230_698.0f64.to_le_bytes());
    payload.extend_from_slice(&rdb_string(b"B"));
    payload.extend_from_slice(&3_663_941_556_696_959.0f64.to_le_bytes());

    let mut data = Vec::new();
    data.extend_from_slice(b"VALKEY080");
    data.push(RDB_OPCODE_SELECTDB);
    data.push(0);
    data.push(RDB_TYPE_ZSET_2);
    data.extend_from_slice(&rdb_string(b"places"));
    data.extend_from_slice(&payload);
    data.push(RDB_OPCODE_EOF);
    data.extend_from_slice(&[0u8; 8]); // CRC placeholder

    // With Geo heuristic: zset counts toward both SortedSet and Geo.
    let mut reader_with = RdbReader::new(Cursor::new(data.clone())).unwrap();
    let s_with = summarize_entries(&mut reader_with, &all_heuristics()).unwrap();
    assert_eq!(s_with.totals.get(&TypeTag::SortedSet).map(|t| t.keys), Some(1));
    assert_eq!(s_with.totals.get(&TypeTag::Geo).map(|t| t.keys), Some(1));
    assert_eq!(s_with.total_keys, 1, "distinct key count excludes the additive geo bucket");

    // Without any heuristic: Geo bucket must be absent.
    let mut reader_without = RdbReader::new(Cursor::new(data)).unwrap();
    let s_without = summarize_entries(&mut reader_without, &HashSet::new()).unwrap();
    assert_eq!(
        s_without.totals.get(&TypeTag::SortedSet).map(|t| t.keys),
        Some(1)
    );
    assert!(!s_without.totals.contains_key(&TypeTag::Geo));
}
