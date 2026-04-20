// Listpack decoder — parses the compact encoding used by Valkey/Redis for
// small hashes, sorted sets, and sets.
//
// Reference: valkey/src/listpack.c
//
// Binary layout:
//   [4-byte LE total_bytes] [2-byte LE num_elements] [entries...] [0xFF]
//
// Each entry:
//   [encoding byte(s)] [data bytes...] [backlen (1-5 bytes)]

use crate::compact::{self, sign_extend, CompactEntry};
use crate::types::RdbError;

fn checked_add(a: usize, b: usize, ctx: &str) -> Result<usize, RdbError> {
    compact::checked_add(a, b, "listpack", ctx)
}

fn ensure_len(buf: &[u8], need: usize, ctx: &str) -> Result<(), RdbError> {
    compact::ensure_len(buf, need, "listpack", ctx)
}

const LP_HDR_SIZE: usize = 6;
const LP_EOF: u8 = 0xFF;

/// Decode all entries from a listpack blob.
///
/// Validates three structural invariants that a crafted blob could
/// otherwise exploit to smuggle trailing data past the decoder:
///
/// 1. The `total_bytes` header field (first 4 bytes, LE) equals
///    `data.len()`. A mismatch means either the caller passed the wrong
///    slice length or the blob has been tampered.
/// 2. `LP_EOF` appears at the end of the blob, not earlier — an early
///    EOF followed by extra bytes would silently drop records. For
///    stream listpacks this would also bypass the trailing-entry check
///    in `stream::decode_stream_listpack`, because the hidden bytes
///    never become `CompactEntry` values.
/// 3. EOF is the *final* byte (`pos == data.len() - 1`).
pub fn decode(data: &[u8]) -> Result<Vec<CompactEntry>, RdbError> {
    if data.len() < LP_HDR_SIZE + 1 {
        return Err(RdbError::CorruptData(
            "listpack too short for header + EOF".into(),
        ));
    }

    // Total-bytes header: the first four LE bytes must equal the full
    // blob length. Valkey's lpNew/lpInsert keep this invariant when
    // writing, so any mismatch is a tamper signal.
    let total_bytes = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if total_bytes != data.len() {
        return Err(RdbError::CorruptData(format!(
            "listpack total_bytes header ({total_bytes}) does not match blob length ({})",
            data.len()
        )));
    }

    // Pre-allocation hint from the `num_elements` header field.
    // `0xFFFF` means "unknown, must scan"; see `compact::capacity_hint`.
    let num_elements = u16::from_le_bytes([data[4], data[5]]);
    let capacity = compact::capacity_hint(num_elements, data.len());

    let mut pos = LP_HDR_SIZE;
    let mut entries = Vec::with_capacity(capacity);

    loop {
        if pos >= data.len() {
            return Err(RdbError::CorruptData("listpack missing EOF byte".into()));
        }

        let enc = data[pos];
        if enc == LP_EOF {
            // EOF must be the very last byte. A crafted blob that drops
            // EOF in the middle with extra trailing records would
            // otherwise be accepted, truncating the decoded entry list
            // without any corruption signal.
            if pos != data.len() - 1 {
                return Err(RdbError::CorruptData(format!(
                    "listpack EOF at byte {pos} but blob length is {}, expected EOF at {}",
                    data.len(),
                    data.len() - 1
                )));
            }
            break;
        }

        let (entry, entry_len) = decode_entry(&data[pos..])?;
        entries.push(entry);

        // Advance past entry data + backlen using checked arithmetic
        let bl_size = backlen_size(entry_len);
        pos = pos
            .checked_add(entry_len)
            .and_then(|p| p.checked_add(bl_size))
            .ok_or_else(|| RdbError::CorruptData("listpack position overflow".into()))?;

        // Validate the backlen bytes match the entry length
        if pos > data.len() {
            return Err(RdbError::CorruptData(
                "listpack entry overflows blob".into(),
            ));
        }
        let backlen_start = pos - bl_size;
        let decoded_bl = decode_backlen(&data[backlen_start..pos])?;
        if decoded_bl != entry_len {
            return Err(RdbError::CorruptData(format!(
                "listpack backlen mismatch: expected {}, decoded {}",
                entry_len, decoded_bl
            )));
        }
    }

    Ok(entries)
}

/// Decode a single entry starting at `buf`. Returns (entry, encoding_bytes_consumed)
/// where encoding_bytes_consumed does NOT include the trailing backlen.
fn decode_entry(buf: &[u8]) -> Result<(CompactEntry, usize), RdbError> {
    if buf.is_empty() {
        return Err(RdbError::CorruptData("listpack entry: empty buffer".into()));
    }

    let b0 = buf[0];

    // 7-bit unsigned integer: 0xxxxxxx
    if b0 & 0x80 == 0 {
        let val = (b0 & 0x7F) as i64;
        return Ok((CompactEntry::Int(val), 1));
    }

    // 6-bit string: 10xxxxxx
    if b0 & 0xC0 == 0x80 {
        let len = (b0 & 0x3F) as usize;
        let total = checked_add(1, len, "6-bit string")?;
        ensure_len(buf, total, "6-bit string")?;
        let s = buf[1..total].to_vec();
        return Ok((CompactEntry::Str(s), total));
    }

    // 13-bit signed integer: 110xxxxx + 1 byte
    if b0 & 0xE0 == 0xC0 {
        ensure_len(buf, 2, "13-bit int")?;
        let raw = (((b0 & 0x1F) as u16) << 8) | (buf[1] as u16);
        let val = sign_extend(raw as u64, 13);
        return Ok((CompactEntry::Int(val), 2));
    }

    // 12-bit string: 1110xxxx + 1 byte len + N bytes
    if b0 & 0xF0 == 0xE0 {
        ensure_len(buf, 2, "12-bit string header")?;
        let len = (((b0 & 0x0F) as usize) << 8) | (buf[1] as usize);
        let total = checked_add(2, len, "12-bit string")?;
        ensure_len(buf, total, "12-bit string")?;
        let s = buf[2..total].to_vec();
        return Ok((CompactEntry::Str(s), total));
    }

    // 0xF0-0xF4: fixed-width encodings
    match b0 {
        // 32-bit string: 0xF0 + 4 bytes LE len + N bytes
        0xF0 => {
            ensure_len(buf, 5, "32-bit string header")?;
            let len = u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
            let total = checked_add(5, len, "32-bit string")?;
            ensure_len(buf, total, "32-bit string")?;
            let s = buf[5..total].to_vec();
            Ok((CompactEntry::Str(s), total))
        }

        // 16-bit signed integer: 0xF1 + 2 bytes LE
        0xF1 => {
            ensure_len(buf, 3, "16-bit int")?;
            let val = i16::from_le_bytes([buf[1], buf[2]]) as i64;
            Ok((CompactEntry::Int(val), 3))
        }

        // 24-bit signed integer: 0xF2 + 3 bytes LE
        0xF2 => {
            ensure_len(buf, 4, "24-bit int")?;
            Ok((CompactEntry::Int(compact::read_i24_le(buf, 1)), 4))
        }

        // 32-bit signed integer: 0xF3 + 4 bytes LE
        0xF3 => {
            ensure_len(buf, 5, "32-bit int")?;
            let val = i32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]) as i64;
            Ok((CompactEntry::Int(val), 5))
        }

        // 64-bit signed integer: 0xF4 + 8 bytes LE
        0xF4 => {
            ensure_len(buf, 9, "64-bit int")?;
            let val = i64::from_le_bytes([
                buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8],
            ]);
            Ok((CompactEntry::Int(val), 9))
        }

        _ => Err(RdbError::CorruptData(format!(
            "listpack unknown encoding byte 0x{:02x}",
            b0
        ))),
    }
}

/// How many bytes does the backlen occupy for an entry of `entry_len` bytes?
/// Compute the number of backlen bytes for a given entry length.
///
/// Bug-for-bug compatible with Valkey's `lpEncodeBacklen`: the boundary
/// at 16383 uses `<` not `<=`, so entry_len == 16383 gets 3 bytes instead
/// of the expected 2. This matches the server's encoding.
fn backlen_size(entry_len: usize) -> usize {
    if entry_len <= 127 {
        1
    } else if entry_len < 16383 {
        2
    } else if entry_len < 2097151 {
        3
    } else if entry_len < 268435455 {
        4
    } else {
        5
    }
}

/// Decode backlen from its bytes. The backlen is stored with the LAST byte
/// holding the LSB (high bit set = continuation), and the FIRST byte holding
/// the MSB (high bit clear = terminator). We iterate in reverse to match
/// lpDecodeBacklen in valkey/src/listpack.c, which reads backward from p.
fn decode_backlen(buf: &[u8]) -> Result<usize, RdbError> {
    if buf.is_empty() {
        return Err(RdbError::CorruptData("listpack backlen: empty".into()));
    }
    // First byte (forward) must have bit 7 clear (terminator)
    if buf[0] & 0x80 != 0 {
        return Err(RdbError::CorruptData("listpack backlen: first byte missing terminator bit".into()));
    }
    let mut val: u64 = 0;
    let mut shift: u32 = 0;
    for &b in buf.iter().rev() {
        if shift > 28 {
            return Err(RdbError::CorruptData("listpack backlen exceeds 5 bytes".into()));
        }
        val |= ((b & 0x7F) as u64) << shift;
        shift += 7;
    }
    usize::try_from(val).map_err(|_| {
        RdbError::CorruptData(format!("listpack backlen {} exceeds usize", val))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal listpack blob: header + entries + 0xFF.
    fn make_listpack(entries_bytes: &[u8], num_elements: u16) -> Vec<u8> {
        let total = (LP_HDR_SIZE + entries_bytes.len() + 1) as u32; // +1 for EOF
        let mut buf = Vec::new();
        buf.extend_from_slice(&total.to_le_bytes());
        buf.extend_from_slice(&num_elements.to_le_bytes());
        buf.extend_from_slice(entries_bytes);
        buf.push(LP_EOF);
        buf
    }

    /// Encode a 7-bit uint entry with backlen.
    fn entry_7bit(val: u8) -> Vec<u8> {
        assert!(val < 128);
        vec![val, 1] // encoding=1 byte, backlen=1
    }

    /// Encode a 6-bit string entry with backlen.
    fn entry_str6(s: &[u8]) -> Vec<u8> {
        assert!(s.len() < 64);
        let mut buf = vec![0x80 | s.len() as u8];
        buf.extend_from_slice(s);
        let entry_len = 1 + s.len();
        buf.push(entry_len as u8); // backlen (fits in 1 byte for small strings)
        buf
    }

    /// Encode a 13-bit signed int entry with backlen.
    fn entry_13bit(val: i16) -> Vec<u8> {
        assert!((-4096..=4095).contains(&val));
        let uval = if val < 0 {
            ((1i32 << 13) + val as i32) as u16
        } else {
            val as u16
        };
        let b0 = ((uval >> 8) as u8) | 0xC0;
        let b1 = (uval & 0xFF) as u8;
        vec![b0, b1, 2] // encoding=2 bytes, backlen=1
    }

    /// Encode a 16-bit int entry with backlen.
    fn entry_16bit(val: i16) -> Vec<u8> {
        let bytes = val.to_le_bytes();
        vec![0xF1, bytes[0], bytes[1], 3] // encoding=3 bytes, backlen=1
    }

    /// Encode a 24-bit int entry with backlen.
    fn entry_24bit(val: i32) -> Vec<u8> {
        let uval = if val < 0 {
            ((1i64 << 24) + val as i64) as u32
        } else {
            val as u32
        };
        vec![
            0xF2,
            (uval & 0xFF) as u8,
            ((uval >> 8) & 0xFF) as u8,
            ((uval >> 16) & 0xFF) as u8,
            4, // backlen
        ]
    }

    /// Encode a 32-bit int entry with backlen.
    fn entry_32bit(val: i32) -> Vec<u8> {
        let bytes = val.to_le_bytes();
        vec![0xF3, bytes[0], bytes[1], bytes[2], bytes[3], 5] // backlen=1
    }

    /// Encode a 64-bit int entry with backlen.
    fn entry_64bit(val: i64) -> Vec<u8> {
        let bytes = val.to_le_bytes();
        let mut buf = vec![0xF4];
        buf.extend_from_slice(&bytes);
        buf.push(9); // backlen
        buf
    }

    #[test]
    fn test_empty_listpack() {
        let data = make_listpack(&[], 0);
        let entries = decode(&data).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_7bit_uint() {
        let mut body = Vec::new();
        body.extend_from_slice(&entry_7bit(0));
        body.extend_from_slice(&entry_7bit(42));
        body.extend_from_slice(&entry_7bit(127));
        let data = make_listpack(&body, 3);
        let entries = decode(&data).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], CompactEntry::Int(0));
        assert_eq!(entries[1], CompactEntry::Int(42));
        assert_eq!(entries[2], CompactEntry::Int(127));
    }

    #[test]
    fn test_6bit_string() {
        let mut body = Vec::new();
        body.extend_from_slice(&entry_str6(b"hello"));
        body.extend_from_slice(&entry_str6(b""));
        let data = make_listpack(&body, 2);
        let entries = decode(&data).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], CompactEntry::Str(b"hello".to_vec()));
        assert_eq!(entries[1], CompactEntry::Str(b"".to_vec()));
    }

    #[test]
    fn test_13bit_signed_int() {
        let mut body = Vec::new();
        body.extend_from_slice(&entry_13bit(200));
        body.extend_from_slice(&entry_13bit(-1));
        body.extend_from_slice(&entry_13bit(-4096));
        body.extend_from_slice(&entry_13bit(4095));
        let data = make_listpack(&body, 4);
        let entries = decode(&data).unwrap();
        assert_eq!(entries[0], CompactEntry::Int(200));
        assert_eq!(entries[1], CompactEntry::Int(-1));
        assert_eq!(entries[2], CompactEntry::Int(-4096));
        assert_eq!(entries[3], CompactEntry::Int(4095));
    }

    #[test]
    fn test_16bit_int() {
        let mut body = Vec::new();
        body.extend_from_slice(&entry_16bit(-1000));
        body.extend_from_slice(&entry_16bit(32767));
        let data = make_listpack(&body, 2);
        let entries = decode(&data).unwrap();
        assert_eq!(entries[0], CompactEntry::Int(-1000));
        assert_eq!(entries[1], CompactEntry::Int(32767));
    }

    #[test]
    fn test_24bit_int() {
        let mut body = Vec::new();
        body.extend_from_slice(&entry_24bit(100000));
        body.extend_from_slice(&entry_24bit(-100000));
        let data = make_listpack(&body, 2);
        let entries = decode(&data).unwrap();
        assert_eq!(entries[0], CompactEntry::Int(100000));
        assert_eq!(entries[1], CompactEntry::Int(-100000));
    }

    #[test]
    fn test_32bit_int() {
        let mut body = Vec::new();
        body.extend_from_slice(&entry_32bit(i32::MAX));
        body.extend_from_slice(&entry_32bit(i32::MIN));
        let data = make_listpack(&body, 2);
        let entries = decode(&data).unwrap();
        assert_eq!(entries[0], CompactEntry::Int(i32::MAX as i64));
        assert_eq!(entries[1], CompactEntry::Int(i32::MIN as i64));
    }

    #[test]
    fn test_64bit_int() {
        let mut body = Vec::new();
        body.extend_from_slice(&entry_64bit(i64::MAX));
        body.extend_from_slice(&entry_64bit(i64::MIN));
        let data = make_listpack(&body, 2);
        let entries = decode(&data).unwrap();
        assert_eq!(entries[0], CompactEntry::Int(i64::MAX));
        assert_eq!(entries[1], CompactEntry::Int(i64::MIN));
    }

    #[test]
    fn test_12bit_string() {
        // Build a 12-bit string entry (length > 63)
        let s = vec![b'x'; 100];
        let mut entry = vec![0xE0 | ((100 >> 8) as u8), (100 & 0xFF) as u8];
        entry.extend_from_slice(&s);
        let entry_len = 2 + 100;
        entry.push(entry_len as u8); // backlen (102 < 128, fits in 1 byte)
        let data = make_listpack(&entry, 1);
        let entries = decode(&data).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], CompactEntry::Str(s));
    }

    #[test]
    fn test_mixed_entries() {
        let mut body = Vec::new();
        body.extend_from_slice(&entry_7bit(5));
        body.extend_from_slice(&entry_str6(b"field"));
        body.extend_from_slice(&entry_13bit(-42));
        body.extend_from_slice(&entry_16bit(10000));
        let data = make_listpack(&body, 4);
        let entries = decode(&data).unwrap();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0], CompactEntry::Int(5));
        assert_eq!(entries[1], CompactEntry::Str(b"field".to_vec()));
        assert_eq!(entries[2], CompactEntry::Int(-42));
        assert_eq!(entries[3], CompactEntry::Int(10000));
    }

    #[test]
    fn test_32bit_string() {
        // 0xF0 + 4-byte LE length + string data
        let s = b"hello world!";
        let mut entry = vec![0xF0];
        entry.extend_from_slice(&(s.len() as u32).to_le_bytes());
        entry.extend_from_slice(s);
        let entry_len = 5 + s.len(); // 1 encoding byte + 4 len bytes + data
        entry.push(entry_len as u8); // backlen
        let data = make_listpack(&entry, 1);
        let entries = decode(&data).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], CompactEntry::Str(s.to_vec()));
    }

    #[test]
    fn test_unknown_encoding_byte() {
        // 0xF5 is not a valid listpack encoding
        let entry = vec![0xF5, 1]; // encoding + dummy backlen
        let data = make_listpack(&entry, 1);
        assert!(decode(&data).is_err());
    }

    #[test]
    fn test_truncated_listpack() {
        // Too short for header
        assert!(decode(&[0; 3]).is_err());
    }

    #[test]
    fn test_multibyte_backlen() {
        // Build a 12-bit string entry with length 200 (> 127, so 2-byte backlen).
        // This exercises the decode_backlen reverse-iteration path.
        let s = vec![b'A'; 200];
        let mut entry = vec![0xE0 | ((200 >> 8) as u8), (200 & 0xFF) as u8];
        entry.extend_from_slice(&s);
        let entry_len = 2 + 200; // encoding header + string = 202
        // Encode backlen matching Valkey's lpEncodeBacklen for 202:
        // buf[0] = 202 >> 7 = 1, buf[1] = (202 & 127) | 128 = 75 | 128 = 203
        let bl0 = (entry_len >> 7) as u8;
        let bl1 = ((entry_len & 127) | 128) as u8;
        entry.push(bl0);
        entry.push(bl1);

        let data = make_listpack(&entry, 1);
        let entries = decode(&data).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], CompactEntry::Str(s));
    }

    #[test]
    fn test_missing_eof() {
        // Valid header but no EOF
        let mut data = vec![0; LP_HDR_SIZE];
        // Set total_bytes to just the header (no EOF)
        let total = LP_HDR_SIZE as u32;
        data[0..4].copy_from_slice(&total.to_le_bytes());
        assert!(decode(&data).is_err());
    }

    #[test]
    fn backlen_size_boundaries() {
        // Pin the width boundaries of `lpEncodeBacklen` per Valkey's wire
        // format. Any change here breaks compatibility with every existing
        // RDB file — the comparisons must stay `<` at 16383 and 2097151
        // to match the server's bug-for-bug encoding.
        let cases: &[(usize, usize)] = &[
            (0, 1),
            (1, 1),
            (127, 1),           // upper bound of 1-byte backlen
            (128, 2),           // first value requiring 2 bytes
            (16382, 2),         // still within 2-byte range
            (16383, 3),         // bug-for-bug: `<` not `<=` so this is 3, not 2
            (16384, 3),
            (2097150, 3),       // upper bound of 3-byte backlen
            (2097151, 4),       // first value requiring 4 bytes
            (2097152, 4),
            (268435454, 4),     // upper bound of 4-byte backlen
            (268435455, 5),     // first value requiring 5 bytes
            (usize::MAX, 5),
        ];
        for &(entry_len, expected) in cases {
            assert_eq!(
                backlen_size(entry_len),
                expected,
                "backlen_size({entry_len}) should be {expected}"
            );
        }
    }

    #[test]
    fn test_rejects_total_bytes_mismatch() {
        // Build a valid single-int listpack and then corrupt total_bytes
        // so it disagrees with the actual blob length. The decoder must
        // reject it rather than silently parsing the first N entries.
        let mut data = make_listpack(&entry_7bit(42), 1);
        data[0] = data[0].wrapping_add(1); // bump total_bytes by 1
        let err = decode(&data).unwrap_err();
        assert!(matches!(err, RdbError::CorruptData(ref m) if m.contains("total_bytes")));
    }

    #[test]
    fn test_rejects_early_eof_with_trailing_bytes() {
        // Craft a blob whose EOF appears before the stated blob end —
        // hidden payload past EOF would otherwise be silently dropped
        // by the decoder and, for stream listpacks, bypass the
        // trailing-entry check that lives downstream.
        let entries = entry_7bit(1);
        let mut data = Vec::new();
        // total_bytes covers header + entries + EOF + one trailing byte.
        let total = (LP_HDR_SIZE + entries.len() + 1 + 1) as u32;
        data.extend_from_slice(&total.to_le_bytes());
        data.extend_from_slice(&1u16.to_le_bytes()); // num_elements
        data.extend_from_slice(&entries);
        data.push(LP_EOF); // early EOF — not at the final byte
        data.push(0x00); // trailing byte past EOF
        assert_eq!(data.len(), total as usize);
        let err = decode(&data).unwrap_err();
        assert!(matches!(err, RdbError::CorruptData(ref m) if m.contains("EOF")));
    }

    #[test]
    fn decode_backlen_known_bit_patterns() {
        // decode_backlen reads `buf.iter().rev()` and OR-accumulates the low
        // 7 bits of each byte. These cases pin the wire format at the
        // boundaries the encoder produces.
        // 127 → single byte 0x7F.
        assert_eq!(decode_backlen(&[0x7F]).unwrap(), 127);
        // 128 → two bytes. High-order byte has bit 7 set (continuation),
        // first byte (forward / MSB) has bit 7 clear (terminator).
        // value = 128 = 0b1000_0000: low 7 bits = 0, next 1 bit = 1.
        assert_eq!(decode_backlen(&[0x01, 0x80]).unwrap(), 128);
        // 16384 → three bytes. 16384 = 0b100_0000000000000.
        // low 7 bits = 0, next 7 bits = 0, top bit = 1.
        assert_eq!(decode_backlen(&[0x01, 0x80, 0x80]).unwrap(), 16384);
    }
}
