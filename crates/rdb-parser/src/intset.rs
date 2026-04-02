// Intset decoder — parses the compact sorted-integer encoding used by
// Valkey/Redis for small sets of integers.
//
// Reference: valkey/src/intset.c
//
// Binary layout:
//   [4-byte LE encoding] [4-byte LE length] [elements...]
//
// Encoding determines element width:
//   2 = int16, 4 = int32, 8 = int64
// All integers are little-endian and signed.

use crate::types::RdbError;

const INTSET_HDR_SIZE: usize = 8;
const INTSET_ENC_INT16: u32 = 2;
const INTSET_ENC_INT32: u32 = 4;
const INTSET_ENC_INT64: u32 = 8;

/// Maximum number of intset elements we'll decode.
/// Each 2-byte int16 becomes a Vec<u8> (~24-byte struct + string data),
/// so 10M elements ≈ 300MB output — well within reason while preventing
/// the 23x amplification attack at 268M elements.
const MAX_INTSET_ELEMENTS: usize = 10_000_000;

/// Decode all members from an intset blob, returning them as decimal ASCII bytes.
pub fn decode(data: &[u8]) -> Result<Vec<Vec<u8>>, RdbError> {
    if data.len() < INTSET_HDR_SIZE {
        return Err(RdbError::CorruptData(
            "intset too short for header".into(),
        ));
    }

    let encoding = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    let length = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;

    if length > MAX_INTSET_ELEMENTS {
        return Err(RdbError::CorruptData(format!(
            "intset element count {} exceeds limit {}",
            length, MAX_INTSET_ELEMENTS
        )));
    }

    let elem_size = match encoding {
        INTSET_ENC_INT16 | INTSET_ENC_INT32 | INTSET_ENC_INT64 => encoding as usize,
        _ => {
            return Err(RdbError::CorruptData(format!(
                "intset unknown encoding: {}",
                encoding
            )))
        }
    };

    let expected_size = INTSET_HDR_SIZE
        .checked_add(length.checked_mul(elem_size).ok_or_else(|| {
            RdbError::CorruptData("intset data size overflow".into())
        })?)
        .ok_or_else(|| RdbError::CorruptData("intset data size overflow".into()))?;

    if data.len() != expected_size {
        return Err(RdbError::CorruptData(format!(
            "intset size mismatch: expected {} bytes, have {}",
            expected_size,
            data.len()
        )));
    }

    let mut members = Vec::with_capacity(length);
    let mut pos = INTSET_HDR_SIZE;

    for _ in 0..length {
        let val: i64 = match encoding {
            INTSET_ENC_INT16 => {
                i16::from_le_bytes([data[pos], data[pos + 1]]) as i64
            }
            INTSET_ENC_INT32 => {
                i32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                    as i64
            }
            INTSET_ENC_INT64 => i64::from_le_bytes([
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
                data[pos + 4],
                data[pos + 5],
                data[pos + 6],
                data[pos + 7],
            ]),
            _ => unreachable!(), // encoding validated above
        };
        members.push(val.to_string().into_bytes());
        pos = pos.checked_add(elem_size).ok_or_else(|| {
            RdbError::CorruptData("intset position overflow".into())
        })?;
    }

    Ok(members)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_intset_i16(values: &[i16]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&INTSET_ENC_INT16.to_le_bytes());
        buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
        for &v in values {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        buf
    }

    fn make_intset_i32(values: &[i32]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&INTSET_ENC_INT32.to_le_bytes());
        buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
        for &v in values {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        buf
    }

    fn make_intset_i64(values: &[i64]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&INTSET_ENC_INT64.to_le_bytes());
        buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
        for &v in values {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        buf
    }

    #[test]
    fn test_empty_intset() {
        let data = make_intset_i16(&[]);
        let members = decode(&data).unwrap();
        assert!(members.is_empty());
    }

    #[test]
    fn test_int16() {
        let data = make_intset_i16(&[-100, 0, 42, 32767]);
        let members = decode(&data).unwrap();
        assert_eq!(
            members,
            vec![
                b"-100".to_vec(),
                b"0".to_vec(),
                b"42".to_vec(),
                b"32767".to_vec(),
            ]
        );
    }

    #[test]
    fn test_int32() {
        let data = make_intset_i32(&[i32::MIN, -1, 0, 1, i32::MAX]);
        let members = decode(&data).unwrap();
        assert_eq!(
            members,
            vec![
                i32::MIN.to_string().into_bytes(),
                b"-1".to_vec(),
                b"0".to_vec(),
                b"1".to_vec(),
                i32::MAX.to_string().into_bytes(),
            ]
        );
    }

    #[test]
    fn test_int64() {
        let data = make_intset_i64(&[i64::MIN, 0, i64::MAX]);
        let members = decode(&data).unwrap();
        assert_eq!(
            members,
            vec![
                i64::MIN.to_string().into_bytes(),
                b"0".to_vec(),
                i64::MAX.to_string().into_bytes(),
            ]
        );
    }

    #[test]
    fn test_unknown_encoding() {
        let mut data = make_intset_i16(&[1]);
        data[0] = 3; // corrupt encoding
        assert!(decode(&data).is_err());
    }

    #[test]
    fn test_truncated_header() {
        assert!(decode(&[0; 4]).is_err());
    }

    #[test]
    fn test_truncated_data() {
        let mut data = make_intset_i32(&[1, 2, 3]);
        data.truncate(INTSET_HDR_SIZE + 4); // only 1 element instead of 3
        assert!(decode(&data).is_err());
    }
}
