//! Single-pass aggregation over an RDB reader.
//!
//! Both the CLI `validate` command and the Python `inspect` API need the
//! same information — per-type key counts, per-db breakdowns, and
//! expected row counts — but historically reimplemented it separately and
//! drifted apart (one counted flat rows, the other counted keys, only
//! one treated geo as additive). [`summarize_entries`] centralizes the
//! traversal so both consumers project the same underlying data.

use std::collections::{BTreeMap, HashSet};
use std::io::Read;

use rdb_parser::{RdbError, RdbHeader, RdbMetadata, RdbReader};

use crate::schema::{expected_row_count, should_emit_geo, type_tag_for, Heuristic, TypeTag};

/// Aggregate counts for one logical type.
#[derive(Debug, Clone, Copy, Default)]
pub struct TypeCount {
    /// Number of distinct keys (first-chunk only — chunked collections
    /// count once no matter how many chunks they produce).
    pub keys: u64,
    /// Flat row count once the keys are expanded into Arrow rows
    /// (collection elements become individual rows, streams get one row
    /// per field/value pair).
    pub rows: u64,
}

/// Output of [`summarize_entries`].
///
/// `totals` and `per_db` both apply the same additive rule for geo: when
/// the Geo heuristic is enabled, a sorted set that looks like geo data is
/// counted under both `SortedSet` and `Geo`. Callers that want just the
/// native-type counts should ignore `TypeTag::Geo`. `per_db_keys` is the
/// distinct-key count per database (no double-counting for geo) so
/// consumers can display a coherent "keys in db" total.
#[derive(Debug, Clone)]
pub struct EntrySummary {
    /// Header from the RDB file (magic + version).
    pub header: RdbHeader,
    /// AUX metadata accumulated during iteration.
    pub metadata: RdbMetadata,
    /// Whether the trailing CRC was verified against recomputed bytes.
    pub crc_verified: bool,
    /// Total distinct keys across all types and databases.
    pub total_keys: u64,
    /// Aggregate per-type counts.
    pub totals: BTreeMap<TypeTag, TypeCount>,
    /// Per-database, per-type key counts. Geo is additive (a zset that
    /// looks like geo appears under both `SortedSet` and `Geo`), so
    /// `per_db[db].values().sum()` over-counts distinct keys; use
    /// `per_db_keys[db]` for an accurate per-db key total.
    pub per_db: BTreeMap<u32, BTreeMap<TypeTag, u64>>,
    /// Per-database distinct-key counts — each key counted exactly once
    /// no matter how many virtual types it matches.
    pub per_db_keys: BTreeMap<u32, u64>,
}

/// Walk the reader once and return aggregate statistics.
///
/// `reader` is borrowed mutably so the caller can inspect CRC state and
/// AUX metadata after the call returns. Entries with unsupported types
/// (`RdbError::UnknownType`) are skipped silently; any other error
/// terminates the scan and is returned.
pub fn summarize_entries<R: Read>(
    reader: &mut RdbReader<R>,
    heuristics: &HashSet<Heuristic>,
) -> Result<EntrySummary, RdbError> {
    let header = reader.header().clone();

    let mut totals: BTreeMap<TypeTag, TypeCount> = BTreeMap::new();
    let mut per_db: BTreeMap<u32, BTreeMap<TypeTag, u64>> = BTreeMap::new();
    let mut per_db_keys: BTreeMap<u32, u64> = BTreeMap::new();
    let mut total_keys: u64 = 0;

    for entry_result in reader.by_ref() {
        let entry = match entry_result {
            Ok(e) => e,
            Err(RdbError::UnknownType(_)) => continue,
            Err(e) => return Err(e),
        };
        let tag = match type_tag_for(&entry) {
            Some(t) => t,
            None => continue,
        };

        let is_first = entry.is_first_chunk();
        let rows = expected_row_count(&entry);

        let tc = totals.entry(tag).or_default();
        if is_first {
            tc.keys += 1;
        }
        tc.rows += rows;

        if is_first {
            total_keys += 1;
            *per_db_keys.entry(entry.db).or_insert(0) += 1;
            *per_db.entry(entry.db).or_default().entry(tag).or_insert(0) += 1;
        }

        // Geo is additive: a sorted set with all 52-bit geohash scores is
        // counted under both `SortedSet` and `Geo` so downstream exports
        // can produce a separate `geo.parquet` without losing the zset.
        // Does NOT bump `per_db_keys` / `total_keys` — those track
        // distinct keys.
        if should_emit_geo(heuristics, tag, &entry) {
            let gc = totals.entry(TypeTag::Geo).or_default();
            if is_first {
                gc.keys += 1;
            }
            gc.rows += rows;
            if is_first {
                *per_db
                    .entry(entry.db)
                    .or_default()
                    .entry(TypeTag::Geo)
                    .or_insert(0) += 1;
            }
        }
    }

    Ok(EntrySummary {
        header,
        metadata: reader.metadata().clone(),
        crc_verified: reader.crc_checked(),
        total_keys,
        totals,
        per_db,
        per_db_keys,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdb_parser::RdbReader;
    use std::io::Cursor;

    /// Minimal Valkey-header-only RDB: empty database, no entries.
    /// Magic is 6 bytes ("VALKEY") + 3 ASCII digits for the version ("080").
    fn empty_rdb_bytes() -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(b"VALKEY080");
        data.push(0xFF); // EOF
        data.extend_from_slice(&[0u8; 8]); // CRC placeholder (unverified)
        data
    }

    #[test]
    fn empty_rdb_summary_is_zero() {
        let bytes = empty_rdb_bytes();
        let mut reader = RdbReader::new(Cursor::new(bytes)).unwrap();
        let summary = summarize_entries(&mut reader, &HashSet::new()).unwrap();
        assert_eq!(summary.total_keys, 0);
        assert!(summary.totals.is_empty());
        assert!(summary.per_db.is_empty());
        assert_eq!(summary.header.version, 80);
    }
}
