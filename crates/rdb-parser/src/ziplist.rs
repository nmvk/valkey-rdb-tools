// Ziplist decoder — parses the legacy compact encoding used by older Redis/Valkey
// versions for small lists, hashes, and sorted sets.
//
// Reference: valkey/src/ziplist.c
//
// Binary layout:
//   [4-byte LE zlbytes] [4-byte LE zltail] [2-byte LE zllen] [entries...] [0xFF]
//
// Each entry:
//   [prevlen (1 or 5 bytes)] [encoding byte(s)] [data bytes...]
//
// String lengths are BIG endian. Integer data is LITTLE endian.

use crate::compact::{self, CompactEntry};
use crate::types::RdbError;

fn checked_add(a: usize, b: usize, ctx: &str) -> Result<usize, RdbError> {
    compact::checked_add(a, b, "ziplist", ctx)
}

fn ensure_len(buf: &[u8], need: usize, ctx: &str) -> Result<(), RdbError> {
    compact::ensure_len(buf, need, "ziplist", ctx)
}

const ZL_HDR_SIZE: usize = 10;
const ZL_END: u8 = 0xFF;

/// Decode all entries from a ziplist blob.
pub fn decode(data: &[u8]) -> Result<Vec<CompactEntry>, RdbError> {
    if data.len() < ZL_HDR_SIZE + 1 {
        return Err(RdbError::CorruptData(
            "ziplist too short for header + end byte".into(),
        ));
    }

    // Cross-check zlbytes against actual blob size (mirrors ziplistValidateIntegrity).
    let zlbytes = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if zlbytes != data.len() {
        return Err(RdbError::CorruptData(format!(
            "ziplist zlbytes ({}) != blob size ({})",
            zlbytes,
            data.len()
        )));
    }

    // Pre-allocation hint from the `zllen` header field. `0xFFFF` means
    // "count > 65534, must scan to determine"; see `compact::capacity_hint`.
    let zllen = u16::from_le_bytes([data[8], data[9]]);
    let capacity = compact::capacity_hint(zllen, data.len());

    let mut pos = ZL_HDR_SIZE;
    let mut entries = Vec::with_capacity(capacity);

    loop {
        if pos >= data.len() {
            return Err(RdbError::CorruptData("ziplist missing end byte".into()));
        }

        if data[pos] == ZL_END {
            break;
        }

        let (entry, consumed) = decode_entry(&data[pos..])?;
        entries.push(entry);

        pos = pos
            .checked_add(consumed)
            .ok_or_else(|| RdbError::CorruptData("ziplist position overflow".into()))?;
    }

    Ok(entries)
}

/// Decode a single ziplist entry starting at `buf`.
/// Returns (entry, total_bytes_consumed) including prevlen + encoding + data.
fn decode_entry(buf: &[u8]) -> Result<(CompactEntry, usize), RdbError> {
    if buf.is_empty() {
        return Err(RdbError::CorruptData("ziplist entry: empty buffer".into()));
    }

    // 1. Parse prevlen (1 or 5 bytes)
    let (prevlen_size, _prevlen) = decode_prevlen(buf)?;
    let rest = &buf[prevlen_size..];

    if rest.is_empty() {
        return Err(RdbError::CorruptData(
            "ziplist entry: truncated after prevlen".into(),
        ));
    }

    // 2. Parse encoding + data
    let enc = rest[0];
    let top2 = enc >> 6;

    let (entry, enc_data_size) = match top2 {
        // 00xxxxxx — 6-bit length string
        0b00 => {
            let len = (enc & 0x3F) as usize;
            let total = checked_add(1, len, "6-bit string")?;
            ensure_len(rest, total, "6-bit string")?;
            let s = rest[1..total].to_vec();
            (CompactEntry::Str(s), total)
        }

        // 01xxxxxx xxxxxxxx — 14-bit length string (BIG endian)
        0b01 => {
            ensure_len(rest, 2, "14-bit string header")?;
            let len = (((enc & 0x3F) as usize) << 8) | (rest[1] as usize);
            let total = checked_add(2, len, "14-bit string")?;
            ensure_len(rest, total, "14-bit string")?;
            let s = rest[2..total].to_vec();
            (CompactEntry::Str(s), total)
        }

        // 10000000 + 4 bytes BIG endian length — 32-bit length string
        // Valkey only writes 0x80 (ZIP_STR_32B). We match on top 2 bits like
        // Valkey's ziplistGet, but reject non-0x80 values as corrupt since the
        // low 6 bits are unused and should be zero.
        0b10 => {
            if enc != 0x80 {
                return Err(RdbError::CorruptData(format!(
                    "ziplist 32-bit string encoding has non-zero low bits: 0x{:02x}",
                    enc
                )));
            }
            ensure_len(rest, 5, "32-bit string header")?;
            let len = u32::from_be_bytes([rest[1], rest[2], rest[3], rest[4]]) as usize;
            let total = checked_add(5, len, "32-bit string")?;
            ensure_len(rest, total, "32-bit string")?;
            let s = rest[5..total].to_vec();
            (CompactEntry::Str(s), total)
        }

        // 11xxxxxx — integer encodings
        0b11 => decode_int_entry(rest)?,

        _ => unreachable!(),
    };

    let total_consumed = checked_add(prevlen_size, enc_data_size, "entry total")?;
    Ok((entry, total_consumed))
}

/// Parse the prevlen field. Returns (bytes_consumed, prevlen_value).
fn decode_prevlen(buf: &[u8]) -> Result<(usize, u32), RdbError> {
    if buf.is_empty() {
        return Err(RdbError::CorruptData("ziplist prevlen: empty".into()));
    }

    if buf[0] == ZL_END {
        // 0xFF is the ziplist terminator, not a valid prevlen byte.
        // The caller should check for ZL_END before calling decode_entry,
        // but guard here to make this function self-contained.
        return Err(RdbError::CorruptData(
            "ziplist prevlen byte is 0xFF (end marker)".into(),
        ));
    }

    if buf[0] < 254 {
        Ok((1, buf[0] as u32))
    } else {
        // 0xFE prefix + 4-byte LE length
        ensure_len(buf, 5, "prevlen 5-byte")?;
        let len = u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]);
        Ok((5, len))
    }
}

/// Decode an integer entry from the encoding byte onward.
/// Returns (entry, bytes_consumed_for_encoding_and_data).
fn decode_int_entry(buf: &[u8]) -> Result<(CompactEntry, usize), RdbError> {
    let enc = buf[0];

    match enc {
        // 0xC0 — 16-bit signed int LE
        0xC0 => {
            ensure_len(buf, 3, "16-bit int")?;
            let val = i16::from_le_bytes([buf[1], buf[2]]) as i64;
            Ok((CompactEntry::Int(val), 3))
        }

        // 0xD0 — 32-bit signed int LE
        0xD0 => {
            ensure_len(buf, 5, "32-bit int")?;
            let val = i32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]) as i64;
            Ok((CompactEntry::Int(val), 5))
        }

        // 0xE0 — 64-bit signed int LE
        0xE0 => {
            ensure_len(buf, 9, "64-bit int")?;
            let val = i64::from_le_bytes([
                buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8],
            ]);
            Ok((CompactEntry::Int(val), 9))
        }

        // 0xF0 — 24-bit signed int LE
        0xF0 => {
            ensure_len(buf, 4, "24-bit int")?;
            Ok((CompactEntry::Int(compact::read_i24_le(buf, 1)), 4))
        }

        // 0xFE — 8-bit signed int
        0xFE => {
            ensure_len(buf, 2, "8-bit int")?;
            let val = buf[1] as i8 as i64;
            Ok((CompactEntry::Int(val), 2))
        }

        // 0xF1-0xFD — 4-bit immediate integer (value = low_nibble - 1, so 0-12)
        b if (0xF1..=0xFD).contains(&b) => {
            let val = ((b & 0x0F) - 1) as i64;
            Ok((CompactEntry::Int(val), 1))
        }

        _ => Err(RdbError::CorruptData(format!(
            "ziplist unknown integer encoding 0x{:02x}",
            enc
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal ziplist blob: header + entries + 0xFF.
    fn make_ziplist(entries_bytes: &[u8], num_entries: u16) -> Vec<u8> {
        let total = (ZL_HDR_SIZE + entries_bytes.len() + 1) as u32; // +1 for ZL_END
        let zltail = 0u32; // not validated by our decoder
        let mut buf = Vec::new();
        buf.extend_from_slice(&total.to_le_bytes()); // zlbytes
        buf.extend_from_slice(&zltail.to_le_bytes()); // zltail
        buf.extend_from_slice(&num_entries.to_le_bytes()); // zllen
        buf.extend_from_slice(entries_bytes);
        buf.push(ZL_END);
        buf
    }

    /// Encode an entry with 1-byte prevlen=0 and a 6-bit string.
    fn entry_str6(prevlen: u8, s: &[u8]) -> Vec<u8> {
        assert!(s.len() < 64);
        assert!(prevlen < 254);
        let mut buf = vec![prevlen]; // prevlen
        buf.push(s.len() as u8); // encoding: 00xxxxxx
        buf.extend_from_slice(s);
        buf
    }

    /// Encode an entry with 1-byte prevlen and a 4-bit immediate int (0-12).
    fn entry_imm(prevlen: u8, val: u8) -> Vec<u8> {
        assert!(val <= 12);
        assert!(prevlen < 254);
        vec![prevlen, 0xF1 + val] // 0xF1 + val → stored nibble = val + 1
    }

    /// Encode an entry with 1-byte prevlen and a 16-bit int.
    fn entry_i16(prevlen: u8, val: i16) -> Vec<u8> {
        assert!(prevlen < 254);
        let bytes = val.to_le_bytes();
        vec![prevlen, 0xC0, bytes[0], bytes[1]]
    }

    /// Encode an entry with 1-byte prevlen and a 32-bit int.
    fn entry_i32(prevlen: u8, val: i32) -> Vec<u8> {
        assert!(prevlen < 254);
        let bytes = val.to_le_bytes();
        vec![prevlen, 0xD0, bytes[0], bytes[1], bytes[2], bytes[3]]
    }

    /// Encode an entry with 1-byte prevlen and a 64-bit int.
    fn entry_i64(prevlen: u8, val: i64) -> Vec<u8> {
        assert!(prevlen < 254);
        let bytes = val.to_le_bytes();
        let mut buf = vec![prevlen, 0xE0];
        buf.extend_from_slice(&bytes);
        buf
    }

    /// Encode an entry with 1-byte prevlen and an 8-bit int.
    fn entry_i8(prevlen: u8, val: i8) -> Vec<u8> {
        assert!(prevlen < 254);
        vec![prevlen, 0xFE, val as u8]
    }

    /// Encode an entry with 1-byte prevlen and a 24-bit int.
    fn entry_i24(prevlen: u8, val: i32) -> Vec<u8> {
        assert!(prevlen < 254);
        let uval = if val < 0 {
            ((1i64 << 24) + val as i64) as u32
        } else {
            val as u32
        };
        vec![
            prevlen,
            0xF0,
            (uval & 0xFF) as u8,
            ((uval >> 8) & 0xFF) as u8,
            ((uval >> 16) & 0xFF) as u8,
        ]
    }

    #[test]
    fn test_empty_ziplist() {
        let data = make_ziplist(&[], 0);
        let entries = decode(&data).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_6bit_strings() {
        let e1 = entry_str6(0, b"hello");
        let e1_len = e1.len();
        let e2 = entry_str6(e1_len as u8, b"world");
        let mut body = Vec::new();
        body.extend_from_slice(&e1);
        body.extend_from_slice(&e2);
        let data = make_ziplist(&body, 2);
        let entries = decode(&data).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], CompactEntry::Str(b"hello".to_vec()));
        assert_eq!(entries[1], CompactEntry::Str(b"world".to_vec()));
    }

    #[test]
    fn test_14bit_string() {
        // Build a 14-bit string entry (length > 63)
        let s = vec![b'x'; 100];
        let mut entry = vec![0u8]; // prevlen=0
        entry.push(0x40 | ((100 >> 8) as u8)); // 01xxxxxx (big endian high)
        entry.push(100u8); // low byte
        entry.extend_from_slice(&s);
        let data = make_ziplist(&entry, 1);
        let entries = decode(&data).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], CompactEntry::Str(s));
    }

    #[test]
    fn test_immediate_ints() {
        let mut body = Vec::new();
        let e0 = entry_imm(0, 0);
        let e0_len = e0.len();
        body.extend_from_slice(&e0);
        let e5 = entry_imm(e0_len as u8, 5);
        let e5_len = e5.len();
        body.extend_from_slice(&e5);
        body.extend_from_slice(&entry_imm(e5_len as u8, 12));
        let data = make_ziplist(&body, 3);
        let entries = decode(&data).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], CompactEntry::Int(0));
        assert_eq!(entries[1], CompactEntry::Int(5));
        assert_eq!(entries[2], CompactEntry::Int(12));
    }

    #[test]
    fn test_8bit_int() {
        let body = entry_i8(0, -42);
        let data = make_ziplist(&body, 1);
        let entries = decode(&data).unwrap();
        assert_eq!(entries[0], CompactEntry::Int(-42));
    }

    #[test]
    fn test_16bit_int() {
        let body = entry_i16(0, -1000);
        let data = make_ziplist(&body, 1);
        let entries = decode(&data).unwrap();
        assert_eq!(entries[0], CompactEntry::Int(-1000));
    }

    #[test]
    fn test_24bit_int() {
        let mut body = Vec::new();
        let e1 = entry_i24(0, 100000);
        let e1_len = e1.len();
        body.extend_from_slice(&e1);
        body.extend_from_slice(&entry_i24(e1_len as u8, -100000));
        let data = make_ziplist(&body, 2);
        let entries = decode(&data).unwrap();
        assert_eq!(entries[0], CompactEntry::Int(100000));
        assert_eq!(entries[1], CompactEntry::Int(-100000));
    }

    #[test]
    fn test_32bit_int() {
        let body = entry_i32(0, i32::MIN);
        let data = make_ziplist(&body, 1);
        let entries = decode(&data).unwrap();
        assert_eq!(entries[0], CompactEntry::Int(i32::MIN as i64));
    }

    #[test]
    fn test_64bit_int() {
        let body = entry_i64(0, i64::MAX);
        let data = make_ziplist(&body, 1);
        let entries = decode(&data).unwrap();
        assert_eq!(entries[0], CompactEntry::Int(i64::MAX));
    }

    #[test]
    fn test_5byte_prevlen() {
        // Simulate an entry whose prevlen >= 254 (5-byte encoding)
        let s = b"after-big";
        let mut entry = vec![0xFE]; // prevlen marker
        entry.extend_from_slice(&300u32.to_le_bytes()); // 4-byte LE prevlen
        entry.push(s.len() as u8); // 6-bit string encoding
        entry.extend_from_slice(s);
        let data = make_ziplist(&entry, 1);
        let entries = decode(&data).unwrap();
        assert_eq!(entries[0], CompactEntry::Str(s.to_vec()));
    }

    #[test]
    fn test_mixed_entries() {
        let mut body = Vec::new();
        let e1 = entry_str6(0, b"key");
        let e1_len = e1.len();
        body.extend_from_slice(&e1);
        let e2 = entry_i16(e1_len as u8, 42);
        let e2_len = e2.len();
        body.extend_from_slice(&e2);
        body.extend_from_slice(&entry_imm(e2_len as u8, 7));
        let data = make_ziplist(&body, 3);
        let entries = decode(&data).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], CompactEntry::Str(b"key".to_vec()));
        assert_eq!(entries[1], CompactEntry::Int(42));
        assert_eq!(entries[2], CompactEntry::Int(7));
    }

    #[test]
    fn test_32bit_string() {
        // 0x80 + 4 bytes BE length
        let s = vec![b'A'; 200];
        let mut entry = vec![0u8]; // prevlen=0
        entry.push(0x80); // ZIP_STR_32B
        entry.extend_from_slice(&(200u32).to_be_bytes()); // big endian length
        entry.extend_from_slice(&s);
        let data = make_ziplist(&entry, 1);
        let entries = decode(&data).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], CompactEntry::Str(s));
    }

    #[test]
    fn test_empty_string() {
        let body = entry_str6(0, b"");
        let data = make_ziplist(&body, 1);
        let entries = decode(&data).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], CompactEntry::Str(b"".to_vec()));
    }

    #[test]
    fn test_truncated_entry_data() {
        // 14-bit string header claiming 1000 bytes, but only 5 available
        let mut entry = vec![0u8]; // prevlen=0
        entry.push(0x40 | ((1000 >> 8) as u8)); // 01xxxxxx
        entry.push((1000 & 0xFF) as u8);
        entry.extend_from_slice(&[0; 5]); // only 5 data bytes, not 1000
        let data = make_ziplist(&entry, 1);
        assert!(decode(&data).is_err());
    }

    #[test]
    fn test_64bit_int_min() {
        let body = entry_i64(0, i64::MIN);
        let data = make_ziplist(&body, 1);
        let entries = decode(&data).unwrap();
        assert_eq!(entries[0], CompactEntry::Int(i64::MIN));
    }

    #[test]
    fn test_24bit_boundary_values() {
        let mut body = Vec::new();
        let e1 = entry_i24(0, 8388607); // 2^23 - 1 (max positive)
        let e1_len = e1.len();
        body.extend_from_slice(&e1);
        body.extend_from_slice(&entry_i24(e1_len as u8, -8388608)); // -2^23 (min negative)
        let data = make_ziplist(&body, 2);
        let entries = decode(&data).unwrap();
        assert_eq!(entries[0], CompactEntry::Int(8388607));
        assert_eq!(entries[1], CompactEntry::Int(-8388608));
    }

    #[test]
    fn test_32bit_string_corrupt_low_bits() {
        // 0x81 instead of 0x80 — non-zero low bits should be rejected
        let mut entry = vec![0u8]; // prevlen=0
        entry.push(0x81); // corrupt: should be 0x80
        entry.extend_from_slice(&10u32.to_be_bytes());
        entry.extend_from_slice(&[0; 10]);
        let data = make_ziplist(&entry, 1);
        assert!(decode(&data).is_err());
    }

    #[test]
    fn test_truncated_ziplist() {
        // Too short for header
        assert!(decode(&[0; 5]).is_err());
    }

    #[test]
    fn test_missing_end_byte() {
        // Valid header but no end byte
        let mut data = vec![0; ZL_HDR_SIZE];
        let total = ZL_HDR_SIZE as u32;
        data[0..4].copy_from_slice(&total.to_le_bytes());
        assert!(decode(&data).is_err());
    }
}
