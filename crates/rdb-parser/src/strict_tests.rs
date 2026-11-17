use crate::{crc64, Limits, RdbError, RdbReader, RdbValue};
use std::io::Cursor;

fn limits() -> Limits {
    Limits {
        input_bytes: 4096,
        allocated_bytes: 65536,
        metadata_bytes: 1024,
        string_bytes: 4096,
        elements: 1024,
        work: 65536,
    }
}
fn finish(mut bytes: Vec<u8>) -> Vec<u8> {
    bytes.push(255);
    let crc = crc64::crc64(0, &bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    bytes
}
fn snapshot() -> Vec<u8> {
    let mut bytes = b"VALKEY080".to_vec();
    // Duplicate raw binary metadata is intentionally retained without rendering.
    bytes.extend_from_slice(&[250, 1, b'x', 2, 0, 255, 250, 1, b'x', 1, b'y']);
    bytes.extend_from_slice(&[254, 0, 252]);
    bytes.extend_from_slice(&1234i64.to_le_bytes());
    bytes.extend_from_slice(&[0, 2, 0, 255, 3, b'a', 0, 255]);
    finish(bytes)
}
#[test]
fn strict_snapshot_binary_expiry_metadata_and_crc() {
    let mut reader = RdbReader::new_limited(Cursor::new(snapshot()), limits()).unwrap();
    assert_eq!(
        reader.metadata().raw_aux,
        vec![
            (b"x".to_vec(), vec![0, 255]),
            (b"x".to_vec(), b"y".to_vec())
        ]
    );
    let entry = reader.next().unwrap().unwrap();
    assert_eq!(entry.key, [0, 255]);
    assert_eq!(entry.expiry_ms, Some(1234));
    assert!(matches!(&entry.value, RdbValue::String(v) if v == &[b'a',0,255]));
    assert!(reader.next().is_none());
    assert!(reader.crc_checked());
}
#[test]
fn strict_snapshot_refuses_trailing_corrupt_and_truncated_input() {
    for variant in 0..3 {
        let mut bytes = snapshot();
        match variant {
            0 => bytes.push(1),
            1 => {
                let n = bytes.len();
                bytes[n - 1] ^= 1;
            }
            _ => {
                bytes.pop();
            }
        }
        let mut reader = RdbReader::new_limited(Cursor::new(bytes), limits()).unwrap();
        assert!(reader.next().unwrap().is_ok());
        assert!(reader.next().unwrap().is_err());
        assert!(reader.next().is_none());
    }
}
#[test]
fn strict_snapshot_bounds_apply_before_aux_and_expansion() {
    let mut bound = limits();
    bound.metadata_bytes = 1;
    assert!(RdbReader::new_limited(Cursor::new(snapshot()), bound).is_err());
    let mut bound = limits();
    bound.string_bytes = 1;
    assert!(RdbReader::new_limited(Cursor::new(snapshot()), bound).is_err());
    let mut bytes = b"VALKEY080".to_vec();
    // LZF with a claimed 4096-byte expansion; refuse before allocating expansion.
    bytes.extend_from_slice(&[0, 1, b'k', 0xc3, 2, 0x50, 0, 0, b'a']);
    let mut bound = limits();
    bound.allocated_bytes = 64;
    let mut reader = RdbReader::new_limited(Cursor::new(finish(bytes)), bound).unwrap();
    assert!(reader.next().unwrap().is_err());
}
#[test]
fn strict_snapshot_never_skips_unsupported_families() {
    for opcode in [6, 7, 9, 15, 19, 21, 22, 243, 244, 245, 246, 247] {
        let mut bytes = b"VALKEY080".to_vec();
        bytes.push(opcode);
        let mut reader = RdbReader::new_limited(Cursor::new(finish(bytes)), limits()).unwrap();
        assert!(reader.next().unwrap().is_err());
        assert!(reader.next().is_none());
    }
}

#[test]
fn strict_snapshot_total_stream_can_exceed_live_allocation_budget() {
    let mut bytes = b"VALKEY080".to_vec();
    for key in 0u16..10000 {
        bytes.extend_from_slice(&[0, 2]);
        bytes.extend_from_slice(&key.to_le_bytes());
        bytes.extend_from_slice(&[1, 0]);
    }
    let bytes = finish(bytes);
    let mut bound = limits();
    bound.input_bytes = bytes.len() as u64;
    bound.allocated_bytes = 32;
    bound.string_bytes = 2;
    bound.work = 1_000_000;
    let mut reader = RdbReader::new_limited(Cursor::new(bytes), bound).unwrap();
    let mut count = 0;
    while let Some(entry) = reader.next() {
        drop(entry.unwrap());
        count += 1;
    }
    assert_eq!(count, 10000);
    assert!(reader.crc_checked());
}

#[test]
fn limits_unlimited_and_setters_keep_bounds_explicit() {
    let unlimited = Limits::unlimited();
    assert_eq!(unlimited.input_bytes, u64::MAX);
    assert_eq!(unlimited.work, u64::MAX);
    let bound = Limits::unlimited().with_elements(7).with_string_bytes(9);
    assert_eq!((bound.elements, bound.string_bytes), (7, 9));
    assert_eq!(bound.allocated_bytes, u64::MAX);
    // An unlimited profile still parses a well-formed snapshot.
    let mut reader = RdbReader::new_limited(Cursor::new(snapshot()), unlimited).unwrap();
    assert!(reader.next().unwrap().is_ok());
    assert!(reader.next().is_none());
}

#[cfg(feature = "zeroize")]
#[test]
fn next_protected_yields_entries() {
    let mut reader = RdbReader::new_limited(Cursor::new(snapshot()), limits()).unwrap();
    let entry = reader.next_protected().unwrap().unwrap();
    assert_eq!(entry.key, [0, 255]);
    assert!(reader.next_protected().is_none());
}

#[test]
fn strict_errors_are_classified() {
    // Zero-valued limits are an invalid argument, not an exhausted budget.
    assert!(matches!(
        RdbReader::new_limited(Cursor::new(snapshot()), limits().with_work(0)),
        Err(RdbError::Io(_))
    ));
    // Exhausting the input budget is a resource limit, not an I/O error.
    assert!(matches!(
        RdbReader::new_limited(Cursor::new(snapshot()), limits().with_input_bytes(5)),
        Err(RdbError::ResourceLimit)
    ));
    // Trailing bytes after the checksum are corrupt data.
    let mut bytes = snapshot();
    bytes.push(1);
    let mut reader = RdbReader::new_limited(Cursor::new(bytes), limits()).unwrap();
    assert!(reader.next().unwrap().is_ok());
    assert!(matches!(
        reader.next().unwrap(),
        Err(RdbError::CorruptData(_))
    ));
}
