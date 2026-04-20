// Shared entry type for compact encodings (listpack, ziplist).
//
// Both encodings produce the same logical output: a sequence of entries that
// are either integers or byte strings. This module avoids duplicating the
// enum, conversion methods, and sign_extend helper.

use crate::types::RdbError;

/// A single decoded entry from a compact encoding (listpack or ziplist).
#[derive(Debug, Clone, PartialEq)]
pub enum CompactEntry {
    /// Integer value.
    Int(i64),
    /// String (raw bytes).
    Str(Vec<u8>),
}

impl CompactEntry {
    /// Convert entry to bytes: strings pass through, integers become decimal ASCII.
    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            CompactEntry::Str(s) => s,
            CompactEntry::Int(n) => n.to_string().into_bytes(),
        }
    }

    /// Try to interpret entry as f64. Integer entries convert directly,
    /// string entries are parsed as decimal.
    pub fn to_f64(&self) -> Result<f64, RdbError> {
        match self {
            CompactEntry::Int(n) => Ok(*n as f64),
            CompactEntry::Str(s) => {
                let s = std::str::from_utf8(s).map_err(|_| {
                    RdbError::CorruptData("compact entry score is not valid UTF-8".into())
                })?;
                s.parse::<f64>().map_err(|_| {
                    RdbError::CorruptData(format!(
                        "compact entry score is not a valid float: {:?}",
                        s
                    ))
                })
            }
        }
    }
}

/// Sign-extend an unsigned value of `bits` width to i64.
pub(crate) fn sign_extend(raw: u64, bits: u32) -> i64 {
    let shift = 64 - bits;
    ((raw << shift) as i64) >> shift
}

/// Read a 24-bit little-endian signed integer from `buf` at `offset`,
/// returning it sign-extended to `i64`. Caller must have already verified
/// that `buf[offset..offset + 3]` is in bounds.
///
/// Duplicated between listpack and ziplist before this was extracted: both
/// encodings use a 3-byte LE wire representation for 24-bit ints.
pub(crate) fn read_i24_le(buf: &[u8], offset: usize) -> i64 {
    let raw = (buf[offset] as u32)
        | ((buf[offset + 1] as u32) << 8)
        | ((buf[offset + 2] as u32) << 16);
    sign_extend(raw as u64, 24)
}

/// Pre-allocation hint from a compact-encoding header count field.
///
/// Both listpack and ziplist store a 16-bit element count in their header
/// with `0xFFFF` as a sentinel for "unknown, must scan". Returns a sane
/// initial `Vec::with_capacity` value: the default when the count is the
/// sentinel, otherwise `count.min(blob_len)` so a crafted header cannot
/// force a multi-GB allocation (since every entry is at least 1 byte).
pub(crate) fn capacity_hint(count_field: u16, blob_len: usize) -> usize {
    const UNKNOWN_COUNT_DEFAULT: usize = 256;
    const SENTINEL: u16 = 0xFFFF;
    if count_field == SENTINEL {
        UNKNOWN_COUNT_DEFAULT
    } else {
        (count_field as usize).min(blob_len)
    }
}

/// Checked addition with a context message for corrupt data errors.
pub(crate) fn checked_add(a: usize, b: usize, format: &str, ctx: &str) -> Result<usize, RdbError> {
    a.checked_add(b).ok_or_else(|| {
        RdbError::CorruptData(format!("{format} {ctx}: length overflow"))
    })
}

/// Ensure a buffer has at least `need` bytes, or return a corrupt data error.
pub(crate) fn ensure_len(buf: &[u8], need: usize, format: &str, ctx: &str) -> Result<(), RdbError> {
    if buf.len() < need {
        return Err(RdbError::CorruptData(format!(
            "{format} {ctx}: need {need} bytes, have {}",
            buf.len()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- sign_extend boundary tests ---

    #[test]
    fn test_sign_extend_13bit() {
        // Positive: 4095 (0x0FFF) stays positive
        assert_eq!(sign_extend(0x0FFF, 13), 4095);
        // Negative: 0x1FFF (-1 in 13-bit) → -1
        assert_eq!(sign_extend(0x1FFF, 13), -1);
        // Negative: 0x1000 (-4096 in 13-bit) → -4096
        assert_eq!(sign_extend(0x1000, 13), -4096);
        // Zero
        assert_eq!(sign_extend(0, 13), 0);
    }

    #[test]
    fn test_sign_extend_24bit() {
        // Max positive: 2^23 - 1 = 8388607
        assert_eq!(sign_extend(0x7FFFFF, 24), 8_388_607);
        // Min negative: 2^23 = -8388608
        assert_eq!(sign_extend(0x800000, 24), -8_388_608);
        // -1
        assert_eq!(sign_extend(0xFFFFFF, 24), -1);
    }

    #[test]
    fn test_sign_extend_8bit() {
        assert_eq!(sign_extend(127, 8), 127);
        assert_eq!(sign_extend(128, 8), -128);
        assert_eq!(sign_extend(255, 8), -1);
    }

    #[test]
    fn test_sign_extend_64bit() {
        // Full width — no sign extension needed
        assert_eq!(sign_extend(u64::MAX, 64), -1i64);
        assert_eq!(sign_extend(0, 64), 0);
        assert_eq!(sign_extend(i64::MAX as u64, 64), i64::MAX);
    }

    // --- CompactEntry conversion tests ---

    #[test]
    fn test_into_bytes() {
        assert_eq!(CompactEntry::Int(42).into_bytes(), b"42");
        assert_eq!(CompactEntry::Int(-7).into_bytes(), b"-7");
        assert_eq!(CompactEntry::Int(0).into_bytes(), b"0");
        assert_eq!(CompactEntry::Str(b"hi".to_vec()).into_bytes(), b"hi");
        assert_eq!(CompactEntry::Str(vec![]).into_bytes(), b"");
    }

    #[test]
    fn test_to_f64() {
        assert_eq!(CompactEntry::Int(42).to_f64().unwrap(), 42.0);
        assert_eq!(CompactEntry::Int(-1).to_f64().unwrap(), -1.0);
        assert_eq!(
            CompactEntry::Str(b"1.5".to_vec()).to_f64().unwrap(),
            1.5
        );
        assert_eq!(
            CompactEntry::Str(b"-2.75".to_vec()).to_f64().unwrap(),
            -2.75
        );
        // Error cases
        assert!(CompactEntry::Str(b"notanumber".to_vec()).to_f64().is_err());
        assert!(CompactEntry::Str(vec![0xFF, 0xFE]).to_f64().is_err()); // invalid UTF-8
    }
}
