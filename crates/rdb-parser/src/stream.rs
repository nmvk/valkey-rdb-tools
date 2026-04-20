//! Stream listpack decoding.
//!
//! Stream listpacks have a special internal structure layered on top of the
//! generic listpack encoding:
//! - Primary entry header: `[count][deleted][num_fields][field1]...[fieldN][0]`
//! - For each entry: `[flags][ms_delta][seq_delta][field-value pairs][lp_count]`
//!
//! IDs are delta-encoded relative to the primary ID (the radix tree key).
//!
//! All integer fields read from untrusted listpack data are validated for
//! range before being used as counts or sizes — see `lp_u64`, `lp_usize`,
//! and `lp_u32` for the conversion helpers.
//!
//! Reference: valkey/src/t_stream.c (`streamAppendItem`, `streamIteratorGetID`).

use crate::compact::CompactEntry;
use crate::listpack;
use crate::types::{RdbError, StreamEntry};
use crate::CAPACITY_HINT_MAX;

/// Maximum stream entries to accumulate before returning an error.
///
/// Prevents OOM on huge streams. A 10M-entry stream with 2 fields each at
/// ~100 bytes per field would be ~2GB. This cap limits to ~500MB.
pub(crate) const MAX_STREAM_ENTRIES: usize = 5_000_000;

/// Extract a non-negative `u64` from a listpack integer, or return `CorruptData`.
fn lp_u64(entry: &CompactEntry, ctx: &str) -> Result<u64, RdbError> {
    match entry {
        CompactEntry::Int(n) if *n >= 0 => Ok(*n as u64),
        CompactEntry::Int(n) => Err(RdbError::CorruptData(format!(
            "stream {ctx}: expected non-negative integer, got {n}"
        ))),
        _ => Err(RdbError::CorruptData(format!(
            "stream {ctx}: expected integer"
        ))),
    }
}

/// Extract a non-negative `usize` from a listpack integer, with checked conversion.
fn lp_usize(entry: &CompactEntry, ctx: &str) -> Result<usize, RdbError> {
    let v = lp_u64(entry, ctx)?;
    usize::try_from(v)
        .map_err(|_| RdbError::CorruptData(format!("stream {ctx}: value {v} overflows usize")))
}

/// Extract a `u32` from a listpack integer, with checked conversion.
fn lp_u32(entry: &CompactEntry, ctx: &str) -> Result<u32, RdbError> {
    let v = lp_u64(entry, ctx)?;
    u32::try_from(v)
        .map_err(|_| RdbError::CorruptData(format!("stream {ctx}: value {v} overflows u32")))
}

/// Decode one stream listpack blob into the sequence of `StreamEntry` rows it contains.
///
/// `master_ms`/`master_seq` come from the radix tree key (the master ID). IDs
/// inside the listpack are deltas from that pair. `remaining_budget` is the
/// number of additional entries the caller can still accept before hitting
/// [`MAX_STREAM_ENTRIES`]; exceeding it is reported as `CorruptData`.
pub(crate) fn decode_stream_listpack(
    master_ms: u64,
    master_seq: u64,
    lp_data: &[u8],
    remaining_budget: usize,
) -> Result<Vec<StreamEntry>, RdbError> {
    let lp_entries = listpack::decode(lp_data)?;
    if lp_entries.len() < 3 {
        return Err(RdbError::CorruptData(
            "stream listpack too short for header".into(),
        ));
    }

    let mut pos = 0;

    let count = lp_u64(&lp_entries[pos], "count")?;
    pos += 1;

    let deleted = lp_u64(&lp_entries[pos], "deleted")?;
    pos += 1;

    let num_master_fields = lp_usize(&lp_entries[pos], "num_fields")?;
    pos += 1;

    let mut master_fields: Vec<Vec<u8>> =
        Vec::with_capacity(num_master_fields.min(CAPACITY_HINT_MAX));
    for _ in 0..num_master_fields {
        if pos >= lp_entries.len() {
            return Err(RdbError::CorruptData(
                "stream: truncated master fields".into(),
            ));
        }
        master_fields.push(lp_entries[pos].clone().into_bytes());
        pos += 1;
    }

    if pos >= lp_entries.len() {
        return Err(RdbError::CorruptData(
            "stream: missing terminator after master fields".into(),
        ));
    }
    match &lp_entries[pos] {
        CompactEntry::Int(0) => {}
        _ => {
            return Err(RdbError::CorruptData(
                "stream: expected zero terminator after master fields".into(),
            ));
        }
    }
    pos += 1;

    let total = count.saturating_add(deleted);
    let cap = usize::try_from(count)
        .unwrap_or(usize::MAX)
        .min(CAPACITY_HINT_MAX)
        .min(remaining_budget);
    let mut entries = Vec::with_capacity(cap);

    for _ in 0..total {
        if pos + 2 >= lp_entries.len() {
            return Err(RdbError::CorruptData(
                "stream listpack truncated mid-entry".into(),
            ));
        }

        let flags = lp_u32(&lp_entries[pos], "flags")?;
        pos += 1;

        let ms_delta = lp_u64(&lp_entries[pos], "ms_delta")?;
        pos += 1;

        let seq_delta = lp_u64(&lp_entries[pos], "seq_delta")?;
        pos += 1;

        let is_deleted = flags & 1 != 0; // STREAM_ITEM_FLAG_DELETED
        let same_fields = flags & 2 != 0; // STREAM_ITEM_FLAG_SAMEFIELDS

        let entry_ms = master_ms
            .checked_add(ms_delta)
            .ok_or_else(|| RdbError::CorruptData("stream: ms overflow".into()))?;
        let entry_seq = master_seq
            .checked_add(seq_delta)
            .ok_or_else(|| RdbError::CorruptData("stream: seq overflow".into()))?;
        let entry_id = format!("{entry_ms}-{entry_seq}");

        let field_values = if same_fields {
            let mut fv = Vec::with_capacity(num_master_fields.min(CAPACITY_HINT_MAX));
            for mf in master_fields.iter().take(num_master_fields) {
                if pos >= lp_entries.len() {
                    return Err(RdbError::CorruptData(
                        "stream: truncated SAMEFIELDS entry".into(),
                    ));
                }
                let val = lp_entries[pos].clone().into_bytes();
                fv.push((mf.clone(), val));
                pos += 1;
            }
            fv
        } else {
            if pos >= lp_entries.len() {
                return Err(RdbError::CorruptData(
                    "stream: truncated entry num_fields".into(),
                ));
            }
            let nf = lp_usize(&lp_entries[pos], "entry num_fields")?;
            pos += 1;

            let mut fv = Vec::with_capacity(nf.min(CAPACITY_HINT_MAX));
            for _ in 0..nf {
                if pos + 1 >= lp_entries.len() {
                    return Err(RdbError::CorruptData(
                        "stream: truncated field-value pair".into(),
                    ));
                }
                let field = lp_entries[pos].clone().into_bytes();
                pos += 1;
                let val = lp_entries[pos].clone().into_bytes();
                pos += 1;
                fv.push((field, val));
            }
            fv
        };

        if pos >= lp_entries.len() {
            return Err(RdbError::CorruptData(
                "stream: missing lp_count after entry".into(),
            ));
        }
        let _lp_count = lp_u64(&lp_entries[pos], "lp_count")?;
        pos += 1;

        if !is_deleted {
            if entries.len() >= remaining_budget {
                return Err(RdbError::CorruptData(format!(
                    "stream exceeds {MAX_STREAM_ENTRIES} entry limit"
                )));
            }
            entries.push(StreamEntry {
                id: entry_id,
                fields: field_values,
            });
        }
    }

    // The header promised `count + deleted` records; anything past that is
    // a corrupt blob that could silently drop real entries from the output.
    if pos != lp_entries.len() {
        return Err(RdbError::CorruptData(format!(
            "stream listpack has {} trailing entries beyond count+deleted header",
            lp_entries.len() - pos
        )));
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compact::CompactEntry;

    #[test]
    fn lp_u64_accepts_non_negative() {
        assert_eq!(lp_u64(&CompactEntry::Int(0), "x").unwrap(), 0);
        assert_eq!(lp_u64(&CompactEntry::Int(42), "x").unwrap(), 42);
        assert_eq!(lp_u64(&CompactEntry::Int(i64::MAX), "x").unwrap(), i64::MAX as u64);
    }

    #[test]
    fn lp_u64_rejects_negative() {
        assert!(matches!(
            lp_u64(&CompactEntry::Int(-1), "x"),
            Err(RdbError::CorruptData(_))
        ));
    }

    #[test]
    fn lp_u64_rejects_non_integer() {
        assert!(matches!(
            lp_u64(&CompactEntry::Str(b"nope".to_vec()), "x"),
            Err(RdbError::CorruptData(_))
        ));
    }

    #[test]
    fn lp_u32_rejects_overflow() {
        // 2^32 does not fit in u32
        let big = CompactEntry::Int((u32::MAX as i64) + 1);
        assert!(matches!(
            lp_u32(&big, "flags"),
            Err(RdbError::CorruptData(_))
        ));
    }

    #[test]
    fn lp_usize_accepts_small_values() {
        assert_eq!(lp_usize(&CompactEntry::Int(100), "n").unwrap(), 100);
    }

    #[test]
    fn decode_rejects_truncated_listpack_blob() {
        // Fewer than 4 bytes is not a valid listpack — decode() fails first.
        let err = decode_stream_listpack(0, 0, &[0u8; 3], MAX_STREAM_ENTRIES).unwrap_err();
        assert!(matches!(err, RdbError::CorruptData(_)));
    }

    /// Build a listpack of small non-negative integer entries.
    /// Header = `[u32 total_bytes][u16 num_entries]`, each entry = `[value][backlen=1]`,
    /// trailing `0xFF` EOF byte. Values must be in `0..=127`.
    fn build_small_int_listpack(values: &[u8]) -> Vec<u8> {
        let header = 6;
        let entries_len = values.len() * 2;
        let total = header + entries_len + 1;
        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(&(total as u32).to_le_bytes());
        out.extend_from_slice(&(values.len() as u16).to_le_bytes());
        for &v in values {
            assert!(v <= 127, "test helper only handles small non-negative ints");
            out.push(v);
            out.push(1);
        }
        out.push(0xFF);
        out
    }

    #[test]
    fn decode_accepts_empty_stream_listpack() {
        // Minimal valid empty stream listpack:
        // [count=0, deleted=0, num_fields=0, terminator=0]
        let lp = build_small_int_listpack(&[0, 0, 0, 0]);
        let entries = decode_stream_listpack(0, 0, &lp, MAX_STREAM_ENTRIES).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn decode_rejects_trailing_listpack_entries() {
        // Empty-stream header promises 0 records after the terminator; any
        // extra entry past that is a corruption signal the decoder must flag.
        // Without this check a crafted listpack could quietly drop real rows.
        let lp = build_small_int_listpack(&[0, 0, 0, 0, 42]);
        let err = decode_stream_listpack(0, 0, &lp, MAX_STREAM_ENTRIES).unwrap_err();
        let RdbError::CorruptData(msg) = err else {
            panic!("expected CorruptData, got {err:?}");
        };
        assert!(
            msg.contains("trailing entries"),
            "error message should mention trailing entries: {msg}"
        );
    }
}
