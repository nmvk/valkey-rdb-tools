use super::*;
use std::io::Cursor;

// --- Header tests ---

#[test]
fn test_redis_magic() {
    let data = b"REDIS0009xxxxxxxxx";
    let header = read_header(&mut Cursor::new(&data[..])).unwrap();
    assert_eq!(header.magic, RdbMagic::Redis);
    assert_eq!(header.version, 9);
}

#[test]
fn test_valkey_magic() {
    let data = b"VALKEY080xxxxxxxxx";
    let header = read_header(&mut Cursor::new(&data[..])).unwrap();
    assert_eq!(header.magic, RdbMagic::Valkey);
    assert_eq!(header.version, 80);
}

#[test]
fn test_redis_version_11() {
    let data = b"REDIS0011xxxxxxxxx";
    let header = read_header(&mut Cursor::new(&data[..])).unwrap();
    assert_eq!(header.magic, RdbMagic::Redis);
    assert_eq!(header.version, 11);
}

#[test]
fn test_invalid_magic() {
    let data = b"GARBAGE00xxxxxxxxx";
    assert!(read_header(&mut Cursor::new(&data[..])).is_err());
}

#[test]
fn test_too_short() {
    let data = b"REDIS";
    assert!(read_header(&mut Cursor::new(&data[..])).is_err());
}

// --- Length encoding tests ---

#[test]
fn test_length_6bit() {
    // 0b00_001010 = 10
    let data = [0b00_001010u8];
    let mut rdr = make_test_reader(&data);
    match rdr.read_length().unwrap() {
        LenResult::Len(v) => assert_eq!(v, 10),
        _ => panic!("expected Len"),
    }
}

#[test]
fn test_length_6bit_zero() {
    let data = [0b00_000000u8];
    let mut rdr = make_test_reader(&data);
    match rdr.read_length().unwrap() {
        LenResult::Len(v) => assert_eq!(v, 0),
        _ => panic!("expected Len"),
    }
}

#[test]
fn test_length_6bit_max() {
    // 0b00_111111 = 63
    let data = [0b00_111111u8];
    let mut rdr = make_test_reader(&data);
    match rdr.read_length().unwrap() {
        LenResult::Len(v) => assert_eq!(v, 63),
        _ => panic!("expected Len"),
    }
}

#[test]
fn test_length_14bit() {
    // 0b01_000001, 0x00 → (1 << 8) | 0 = 256
    let data = [0b01_000001u8, 0x00];
    let mut rdr = make_test_reader(&data);
    match rdr.read_length().unwrap() {
        LenResult::Len(v) => assert_eq!(v, 256),
        _ => panic!("expected Len"),
    }
}

#[test]
fn test_length_14bit_max() {
    // 0b01_111111, 0xFF → (63 << 8) | 255 = 16383
    let data = [0b01_111111u8, 0xFF];
    let mut rdr = make_test_reader(&data);
    match rdr.read_length().unwrap() {
        LenResult::Len(v) => assert_eq!(v, 16383),
        _ => panic!("expected Len"),
    }
}

#[test]
fn test_length_32bit() {
    // 0x80 followed by 4 bytes big-endian: 0x00, 0x01, 0x00, 0x00 = 65536
    let data = [0x80u8, 0x00, 0x01, 0x00, 0x00];
    let mut rdr = make_test_reader(&data);
    match rdr.read_length().unwrap() {
        LenResult::Len(v) => assert_eq!(v, 65536),
        _ => panic!("expected Len"),
    }
}

#[test]
fn test_length_64bit() {
    // 0x81 followed by 8 bytes big-endian: 1
    let data = [0x81u8, 0, 0, 0, 0, 0, 0, 0, 1];
    let mut rdr = make_test_reader(&data);
    match rdr.read_length().unwrap() {
        LenResult::Len(v) => assert_eq!(v, 1),
        _ => panic!("expected Len"),
    }
}

#[test]
fn test_length_special_encoding() {
    // 0b11_000010 → special encoding, sub-type 2 (INT32)
    let data = [0b11_000010u8];
    let mut rdr = make_test_reader(&data);
    match rdr.read_length().unwrap() {
        LenResult::Special(enc) => assert_eq!(enc, RDB_ENC_INT32),
        _ => panic!("expected Special"),
    }
}

// --- String encoding tests ---

#[test]
fn test_string_raw() {
    // Length 5, then "hello"
    let data = [5u8, b'h', b'e', b'l', b'l', b'o'];
    let mut rdr = make_test_reader(&data);
    assert_eq!(rdr.read_string().unwrap(), b"hello");
}

#[test]
fn test_string_empty() {
    let data = [0u8];
    let mut rdr = make_test_reader(&data);
    assert_eq!(rdr.read_string().unwrap(), b"");
}

#[test]
fn test_string_int8() {
    // 0b11_000000 (INT8), then 42
    let data = [0xC0u8, 42];
    let mut rdr = make_test_reader(&data);
    assert_eq!(rdr.read_string().unwrap(), b"42");
}

#[test]
fn test_string_int8_negative() {
    // 0xC0 (INT8), then 0xFE = -2 as signed
    let data = [0xC0u8, 0xFE];
    let mut rdr = make_test_reader(&data);
    assert_eq!(rdr.read_string().unwrap(), b"-2");
}

#[test]
fn test_string_int16() {
    // 0xC1 (INT16), then 0xE8, 0x03 = 1000 LE
    let data = [0xC1u8, 0xE8, 0x03];
    let mut rdr = make_test_reader(&data);
    assert_eq!(rdr.read_string().unwrap(), b"1000");
}

#[test]
fn test_string_int32() {
    // 0xC2 (INT32), then 0xD2, 0x02, 0x96, 0x49 = 1234567890 LE
    let data = [0xC2u8, 0xD2, 0x02, 0x96, 0x49];
    let mut rdr = make_test_reader(&data);
    assert_eq!(rdr.read_string().unwrap(), b"1234567890");
}


// --- Iterator becomes terminal after hard I/O error ---

#[test]
fn test_iterator_terminal_on_truncated_input() {
    // Minimal RDB: header + SELECTDB 0 + RESIZEDB + STRING key, then truncated
    let mut data = Vec::new();
    data.extend_from_slice(b"VALKEY080");
    data.push(RDB_OPCODE_SELECTDB);
    data.push(0); // db 0
    data.push(RDB_OPCODE_RESIZEDB);
    data.push(1); // 1 key
    data.push(0); // 0 expires
    data.push(RDB_TYPE_STRING);
    data.push(3); // key length 3
    data.extend_from_slice(b"foo");
    // value truncated -- no string follows

    let reader = RdbReader::new(Cursor::new(data)).unwrap();
    let results: Vec<_> = reader.collect();
    // Should get exactly one error, not an infinite stream of errors
    assert_eq!(results.len(), 1);
    assert!(results[0].is_err());
}

// --- Pending state does not leak across entries ---

#[test]
fn test_no_state_leak_after_unsupported_type() {
    // Build: header + SELECTDB 0 + RESIZEDB +
    //   EXPIRETIME_MS(999) + unsupported_type(listpack hash) + STRING("ok")
    // The STRING entry should NOT inherit the TTL from the hash.
    let mut data = Vec::new();
    data.extend_from_slice(b"VALKEY080");
    data.push(RDB_OPCODE_SELECTDB);
    data.push(0);
    data.push(RDB_OPCODE_RESIZEDB);
    data.push(2);
    data.push(0);

    // EXPIRETIME_MS followed by a HASH_ZIPMAP (still unsupported single-blob type)
    data.push(RDB_OPCODE_EXPIRETIME_MS);
    data.extend_from_slice(&999i64.to_le_bytes());
    data.push(RDB_TYPE_HASH_ZIPMAP);
    data.push(7); // key length
    data.extend_from_slice(b"thehash");
    // Zipmap blob: dummy bytes (just needs to be consumable as a string)
    data.push(4); // blob length 4
    data.extend_from_slice(&[0, 0, 0, 0]); // dummy blob

    // Second entry: STRING "ok" = "val", no TTL set
    data.push(RDB_TYPE_STRING);
    data.push(2);
    data.extend_from_slice(b"ok");
    data.push(3);
    data.extend_from_slice(b"val");

    data.push(RDB_OPCODE_EOF);
    data.extend_from_slice(&[0u8; 8]); // dummy CRC

    let reader = RdbReader::new(Cursor::new(data)).unwrap();
    let mut ok_expiry = None;
    for entry in reader {
        if let Ok(e) = &entry {
            if e.key == b"ok" {
                ok_expiry = Some(e.expiry_ms);
            }
        }
    }
    assert_eq!(ok_expiry, Some(None), "TTL should not leak to next entry");
}

// --- Version validation ---

#[test]
fn test_unsupported_redis_version() {
    let data = b"REDIS0099xxxxxxxxx";
    assert!(matches!(
        read_header(&mut Cursor::new(&data[..])),
        Err(RdbError::UnsupportedVersion(99))
    ));
}

#[test]
fn test_unsupported_valkey_version() {
    let data = b"VALKEY999xxxxxxxxx";
    assert!(matches!(
        read_header(&mut Cursor::new(&data[..])),
        Err(RdbError::UnsupportedVersion(999))
    ));
}


// --- Unknown top-level byte is terminal ---

#[test]
fn test_unknown_opcode_is_terminal() {
    let mut data = Vec::new();
    data.extend_from_slice(b"VALKEY080");
    data.push(RDB_OPCODE_SELECTDB);
    data.push(0);
    data.push(RDB_OPCODE_RESIZEDB);
    data.push(1);
    data.push(0);
    // Byte 200 is not a valid opcode or type code
    data.push(200);
    // Followed by what looks like a valid STRING entry
    data.push(RDB_TYPE_STRING);
    data.push(1);
    data.push(b'k');
    data.push(1);
    data.push(b'v');
    data.push(RDB_OPCODE_EOF);
    data.extend_from_slice(&[0u8; 8]);

    let reader = RdbReader::new(Cursor::new(data)).unwrap();
    let results: Vec<_> = reader.collect();
    // Should get exactly one terminal error, not a fabricated entry after it
    assert_eq!(results.len(), 1);
    assert!(matches!(&results[0], Err(RdbError::CorruptData(_))));
}

// --- Invalid version/magic pairing ---

#[test]
fn test_redis_version_12_rejected() {
    // REDIS version 12 is a foreign version (12-79), rejected in strict mode
    let data = b"REDIS0012xxxxxxxxx";
    assert!(matches!(
        read_header(&mut Cursor::new(&data[..])),
        Err(RdbError::UnsupportedVersion(12))
    ));
}

#[test]
fn test_redis_version_0_rejected() {
    let data = b"REDIS0000xxxxxxxxx";
    assert!(matches!(
        read_header(&mut Cursor::new(&data[..])),
        Err(RdbError::UnsupportedVersion(0))
    ));
}

#[test]
fn test_valkey_version_012_rejected() {
    // VALKEY012 is not a valid Valkey version (only 080 is)
    let data = b"VALKEY012xxxxxxxxx";
    assert!(matches!(
        read_header(&mut Cursor::new(&data[..])),
        Err(RdbError::UnsupportedVersion(12))
    ));
}

// --- Double value format (ZSET v1) ---

#[test]
#[allow(clippy::approx_constant)]
fn test_read_double_value_normal() {
    // len=4, then "3.14"
    let data = [4u8, b'3', b'.', b'1', b'4'];
    let mut rdr = make_test_reader(&data);
    let v = rdr.read_double_value().unwrap();
    assert!((v - 3.14).abs() < 1e-10);
}

#[test]
fn test_read_double_value_special() {
    // 255 = -inf, 254 = +inf, 253 = NaN
    let mut rdr = make_test_reader(&[255u8]);
    assert!(rdr.read_double_value().unwrap().is_infinite());

    let mut rdr = make_test_reader(&[254u8]);
    assert!(rdr.read_double_value().unwrap().is_infinite());

    let mut rdr = make_test_reader(&[253u8]);
    assert!(rdr.read_double_value().unwrap().is_nan());
}

// --- Test helper ---

// --- Quicklist edge-case tests ---

#[test]
fn test_quicklist2_invalid_container() {
    // Quicklist v2 with container=3 should fail
    let mut data = Vec::new();
    data.extend_from_slice(b"VALKEY080");
    data.push(RDB_OPCODE_SELECTDB);
    data.push(0);
    data.push(RDB_OPCODE_RESIZEDB);
    data.push(1);
    data.push(0);
    data.push(RDB_TYPE_LIST_QUICKLIST_2);
    data.push(3); // key "ql2"
    data.extend_from_slice(b"ql2");
    data.push(1); // 1 node
    data.push(3); // container=3 (INVALID)
    data.push(3); // blob len
    data.extend_from_slice(b"abc");
    data.push(RDB_OPCODE_EOF);
    data.extend_from_slice(&[0u8; 8]);

    let reader = RdbReader::new(Cursor::new(data)).unwrap();
    let results: Vec<_> = reader.collect();
    assert_eq!(results.len(), 1);
    assert!(matches!(&results[0], Err(RdbError::CorruptData(_))));
}

#[test]
fn test_quicklist_v1_empty_node() {
    // Quicklist v1 with one ziplist node containing zero entries
    let empty_zl = {
        // zlbytes=11, zltail=10, zllen=0, end=0xFF
        let mut zl = Vec::new();
        zl.extend_from_slice(&11u32.to_le_bytes()); // zlbytes
        zl.extend_from_slice(&10u32.to_le_bytes()); // zltail
        zl.extend_from_slice(&0u16.to_le_bytes());  // zllen
        zl.push(0xFF);
        zl
    };

    let mut data = Vec::new();
    data.extend_from_slice(b"VALKEY080");
    data.push(RDB_OPCODE_SELECTDB);
    data.push(0);
    data.push(RDB_OPCODE_RESIZEDB);
    data.push(1);
    data.push(0);
    data.push(RDB_TYPE_LIST_QUICKLIST);
    data.push(4); // key "empt"
    data.extend_from_slice(b"empt");
    data.push(1); // 1 node
    // node blob (length-prefixed ziplist)
    data.push(empty_zl.len() as u8);
    data.extend_from_slice(&empty_zl);
    data.push(RDB_OPCODE_EOF);
    data.extend_from_slice(&[0u8; 8]);

    let reader = RdbReader::new(Cursor::new(data)).unwrap();
    let entries: Vec<_> = reader.map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, b"empt");
    assert!(matches!(&entries[0].value, RdbValue::List(v) if v.is_empty()));
}

#[test]
fn test_quicklist2_zero_nodes() {
    // Quicklist v2 with node count = 0 → empty list
    let mut data = Vec::new();
    data.extend_from_slice(b"VALKEY080");
    data.push(RDB_OPCODE_SELECTDB);
    data.push(0);
    data.push(RDB_OPCODE_RESIZEDB);
    data.push(1);
    data.push(0);
    data.push(RDB_TYPE_LIST_QUICKLIST_2);
    data.push(4); // key "zero"
    data.extend_from_slice(b"zero");
    data.push(0); // 0 nodes
    data.push(RDB_OPCODE_EOF);
    data.extend_from_slice(&[0u8; 8]);

    let reader = RdbReader::new(Cursor::new(data)).unwrap();
    let entries: Vec<_> = reader.map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, b"zero");
    assert!(matches!(&entries[0].value, RdbValue::List(v) if v.is_empty()));
}

/// Build a minimal RDB with one key-value entry from raw value bytes.
/// The caller provides the type code and pre-encoded value payload.
fn make_rdb_entry(type_code: u8, key: &[u8], value_payload: &[u8]) -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(b"VALKEY080");
    data.push(RDB_OPCODE_SELECTDB);
    data.push(0);
    data.push(RDB_OPCODE_RESIZEDB);
    data.push(1);
    data.push(0);
    data.push(type_code);
    data.push(key.len() as u8);
    data.extend_from_slice(key);
    data.extend_from_slice(value_payload);
    data.push(RDB_OPCODE_EOF);
    data.extend_from_slice(&[0u8; 8]); // dummy CRC
    data
}

/// Encode a length using RDB length encoding (for test payloads).
fn rdb_len(n: usize) -> Vec<u8> {
    if n < 64 {
        vec![n as u8]
    } else if n < 16384 {
        vec![(0x40 | (n >> 8)) as u8, (n & 0xFF) as u8]
    } else {
        panic!("test helper only supports lengths < 16384");
    }
}

/// Encode a string with RDB length prefix (for test payloads).
fn rdb_string(s: &[u8]) -> Vec<u8> {
    let mut buf = rdb_len(s.len());
    buf.extend_from_slice(s);
    buf
}

// --- Plain list (type 1) ---

#[test]
fn test_plain_list() {
    let mut payload = rdb_len(3); // 3 elements
    payload.extend_from_slice(&rdb_string(b"alpha"));
    payload.extend_from_slice(&rdb_string(b"beta"));
    payload.extend_from_slice(&rdb_string(b"gamma"));
    let data = make_rdb_entry(RDB_TYPE_LIST, b"mylist", &payload);

    let reader = RdbReader::new(Cursor::new(data)).unwrap();
    let entries: Vec<_> = reader.map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].type_name(), "list");
    assert_eq!(entries[0].encoding_name(), "linkedlist");
    match &entries[0].value {
        RdbValue::List(elems) => {
            assert_eq!(elems.len(), 3);
            assert_eq!(elems[0], b"alpha");
            assert_eq!(elems[1], b"beta");
            assert_eq!(elems[2], b"gamma");
        }
        other => panic!("expected List, got {:?}", other),
    }
}

// --- Plain set (type 2) ---

#[test]
fn test_plain_set() {
    let mut payload = rdb_len(2);
    payload.extend_from_slice(&rdb_string(b"x"));
    payload.extend_from_slice(&rdb_string(b"y"));
    let data = make_rdb_entry(RDB_TYPE_SET, b"myset", &payload);

    let reader = RdbReader::new(Cursor::new(data)).unwrap();
    let entries: Vec<_> = reader.map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].type_name(), "set");
    assert_eq!(entries[0].encoding_name(), "hashtable");
    match &entries[0].value {
        RdbValue::Set(members) => {
            assert_eq!(members.len(), 2);
            assert!(members.contains(&b"x".to_vec()));
            assert!(members.contains(&b"y".to_vec()));
        }
        other => panic!("expected Set, got {:?}", other),
    }
}

// --- Plain hash (type 4) ---

#[test]
fn test_plain_hash() {
    let mut payload = rdb_len(2); // 2 field-value pairs
    payload.extend_from_slice(&rdb_string(b"name"));
    payload.extend_from_slice(&rdb_string(b"alice"));
    payload.extend_from_slice(&rdb_string(b"age"));
    payload.extend_from_slice(&rdb_string(b"30"));
    let data = make_rdb_entry(RDB_TYPE_HASH, b"myhash", &payload);

    let reader = RdbReader::new(Cursor::new(data)).unwrap();
    let entries: Vec<_> = reader.map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].type_name(), "hash");
    assert_eq!(entries[0].encoding_name(), "hashtable");
    match &entries[0].value {
        RdbValue::Hash(fields) => {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].field, b"name");
            assert_eq!(fields[0].value, b"alice");
            assert_eq!(fields[0].expiry_ms, None);
            assert_eq!(fields[1].field, b"age");
            assert_eq!(fields[1].value, b"30");
        }
        other => panic!("expected Hash, got {:?}", other),
    }
}

// --- Sorted set v1 (type 3) ---

#[test]
fn test_plain_zset_v1() {
    let mut payload = rdb_len(2);
    payload.extend_from_slice(&rdb_string(b"first"));
    // rdbLoadDoubleValue: 3-byte ASCII "1.5"
    payload.push(3); // length byte
    payload.extend_from_slice(b"1.5");
    payload.extend_from_slice(&rdb_string(b"second"));
    payload.push(1);
    payload.extend_from_slice(b"3");
    let data = make_rdb_entry(RDB_TYPE_ZSET, b"myzset", &payload);

    let reader = RdbReader::new(Cursor::new(data)).unwrap();
    let entries: Vec<_> = reader.map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].type_name(), "zset");
    assert_eq!(entries[0].encoding_name(), "skiplist");
    match &entries[0].value {
        RdbValue::SortedSet(pairs) => {
            assert_eq!(pairs.len(), 2);
            assert_eq!(pairs[0].0, b"first");
            assert_eq!(pairs[0].1, 1.5);
            assert_eq!(pairs[1].0, b"second");
            assert_eq!(pairs[1].1, 3.0);
        }
        other => panic!("expected SortedSet, got {:?}", other),
    }
}

// --- Sorted set v2 (type 5) ---

#[test]
fn test_plain_zset_v2() {
    let mut payload = rdb_len(1);
    payload.extend_from_slice(&rdb_string(b"member"));
    payload.extend_from_slice(&2.5f64.to_le_bytes());
    let data = make_rdb_entry(RDB_TYPE_ZSET_2, b"myzset2", &payload);

    let reader = RdbReader::new(Cursor::new(data)).unwrap();
    let entries: Vec<_> = reader.map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].encoding_name(), "skiplist");
    match &entries[0].value {
        RdbValue::SortedSet(pairs) => {
            assert_eq!(pairs.len(), 1);
            assert_eq!(pairs[0].0, b"member");
            assert_eq!(pairs[0].1, 2.5);
        }
        other => panic!("expected SortedSet, got {:?}", other),
    }
}

// --- HASH_2 (type 22, per-field TTL) ---

#[test]
fn test_hash_2() {
    let mut payload = rdb_len(2);
    // field 1: no TTL (ttl=-1, Valkey's EXPIRY_NONE sentinel)
    payload.extend_from_slice(&rdb_string(b"name"));
    payload.extend_from_slice(&rdb_string(b"bob"));
    payload.extend_from_slice(&(-1i64).to_le_bytes());
    // field 2: with TTL
    payload.extend_from_slice(&rdb_string(b"session"));
    payload.extend_from_slice(&rdb_string(b"abc123"));
    payload.extend_from_slice(&1700000000000i64.to_le_bytes());
    let data = make_rdb_entry(RDB_TYPE_HASH_2, b"myhash2", &payload);

    let reader = RdbReader::new(Cursor::new(data)).unwrap();
    let entries: Vec<_> = reader.map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].type_name(), "hash");
    match &entries[0].value {
        RdbValue::Hash(fields) => {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].field, b"name");
            assert_eq!(fields[0].value, b"bob");
            assert_eq!(fields[0].expiry_ms, None); // ttl=-1 → None
            assert_eq!(fields[1].field, b"session");
            assert_eq!(fields[1].value, b"abc123");
            assert_eq!(fields[1].expiry_ms, Some(1_700_000_000_000));
        }
        other => panic!("expected Hash, got {:?}", other),
    }
}

// --- LZF round-trip through read_string ---

#[test]
fn test_lzf_string_through_read_string() {
    // Build an LZF-compressed string "aaaaa" (5 bytes of 'a')
    // LZF format: literal 'a' (ctrl=0, len=1), then backref (ctrl >> 5 = 2 → len=4, offset=0)
    let compressed = [0u8, b'a', 0x40, 0x00]; // literal 'a' + backref len=4 offset=1
    let expected = b"aaaaa";

    let mut data = Vec::new();
    // length prefix: special encoding (top 2 bits = 11 = ENCVAL)
    data.push(0xC3); // 11_000011 = ENCVAL, sub-type 3 = LZF
    // compressed length
    data.push(compressed.len() as u8); // 4
    // uncompressed length
    data.push(expected.len() as u8); // 6
    data.extend_from_slice(&compressed);

    let mut reader = make_test_reader(&data);
    let result = reader.read_string().unwrap();
    assert_eq!(result, expected);
}

// --- EXPIRETIME (seconds, opcode 253) ---

#[test]
fn test_expiretime_seconds() {
    let mut data = Vec::new();
    data.extend_from_slice(b"VALKEY080");
    data.push(RDB_OPCODE_SELECTDB);
    data.push(0);
    data.push(RDB_OPCODE_RESIZEDB);
    data.push(1);
    data.push(0);
    // EXPIRETIME with 4-byte seconds value
    data.push(RDB_OPCODE_EXPIRETIME);
    data.extend_from_slice(&1700000000i32.to_le_bytes());
    // STRING entry
    data.push(RDB_TYPE_STRING);
    data.push(3);
    data.extend_from_slice(b"key");
    data.push(3);
    data.extend_from_slice(b"val");
    data.push(RDB_OPCODE_EOF);
    data.extend_from_slice(&[0u8; 8]);

    let reader = RdbReader::new(Cursor::new(data)).unwrap();
    let entries: Vec<_> = reader.map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, b"key");
    // seconds * 1000 = milliseconds
    assert_eq!(entries[0].expiry_ms, Some(1_700_000_000_000));
}

// --- FUNCTION2 skip ---

#[test]
fn test_function2_skipped() {
    let mut data = Vec::new();
    data.extend_from_slice(b"VALKEY080");
    data.push(RDB_OPCODE_SELECTDB);
    data.push(0);
    data.push(RDB_OPCODE_RESIZEDB);
    data.push(1);
    data.push(0);
    // FUNCTION2 with a dummy payload
    data.push(RDB_OPCODE_FUNCTION2);
    data.push(5); // string length 5
    data.extend_from_slice(b"dummy");
    // STRING entry after
    data.push(RDB_TYPE_STRING);
    data.push(1);
    data.push(b'k');
    data.push(1);
    data.push(b'v');
    data.push(RDB_OPCODE_EOF);
    data.extend_from_slice(&[0u8; 8]);

    let reader = RdbReader::new(Cursor::new(data)).unwrap();
    let entries: Vec<_> = reader.map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, b"k");
}

// --- MODULE_AUX skip ---

#[test]
fn test_module_aux_skipped() {
    let mut data = Vec::new();
    data.extend_from_slice(b"VALKEY080");
    data.push(RDB_OPCODE_SELECTDB);
    data.push(0);
    data.push(RDB_OPCODE_RESIZEDB);
    data.push(1);
    data.push(0);
    // MODULE_AUX: module_id (length), when_opcode (length), when (length), then module data
    data.push(RDB_OPCODE_MODULE_AUX);
    data.push(42); // module_id (6-bit length value)
    data.push(0);  // when_opcode
    data.push(0);  // when
    // Module data: just an EOF opcode (0)
    data.push(0);  // RDB_MODULE_OPCODE_EOF
    // STRING entry after
    data.push(RDB_TYPE_STRING);
    data.push(1);
    data.push(b'k');
    data.push(1);
    data.push(b'v');
    data.push(RDB_OPCODE_EOF);
    data.extend_from_slice(&[0u8; 8]);

    let reader = RdbReader::new(Cursor::new(data)).unwrap();
    let entries: Vec<_> = reader.map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, b"k");
}

// --- decode_module_id ---

#[test]
fn test_decode_module_id_known_bit_patterns() {
    // Each pair is a 64-bit module ID (per Valkey's moduleTypeEncodeId
    // over the 64-char name charset) and the expected (name, version).
    // Names are always exactly 9 chars from the charset; the low 10
    // bits hold the version.
    let cases: &[(u64, &str, u32)] = &[
        // All-'A' padding with the maximum 10-bit version exercises both
        // the 'A' = charset[0] decode path and the version mask.
        (1023, "AAAAAAAAA", 1023),
        // Generic 9-char name mixing upper/lower/digit characters to
        // cover more of the charset table than all-'A' does. Version 3
        // is arbitrary but valid.
        (11180652778598880259, "mymodule1", 3),
    ];
    for &(id, expected_name, expected_version) in cases {
        let (name, version) = decode_module_id(id);
        assert_eq!(name, expected_name, "id {id:#x}: name");
        assert_eq!(version, expected_version, "id {id:#x}: version");
    }
}

// --- read_module_data ---

#[test]
#[allow(clippy::vec_init_then_push)]
fn test_read_module_data_mixed_values() {
    // Build a module opcode stream: UINT(42), SINT(-1 as u64), STRING("hi"), DOUBLE(1.234), EOF
    let mut data = Vec::new();
    // UINT opcode (2) + value 42
    data.push(2); // opcode = UINT (6-bit length encoding)
    data.push(42); // value = 42
    // SINT opcode (1) + value (u64::MAX = -1 as i64 reinterpreted)
    data.push(1); // opcode = SINT
    // u64::MAX needs 64-bit length encoding: 0x81 prefix + 8 bytes BE
    data.push(0x81); // RDB_64BITLEN
    data.extend_from_slice(&u64::MAX.to_be_bytes());
    // STRING opcode (5) + "hi"
    data.push(5); // opcode = STRING
    data.push(2); // string length
    data.extend_from_slice(b"hi");
    // DOUBLE opcode (4) + 2.718
    data.push(4); // opcode = DOUBLE
    data.extend_from_slice(&1.234f64.to_le_bytes());
    // EOF opcode (0)
    data.push(0);

    let mut rdr = make_test_reader(&data);
    let values = rdr.read_module_data().unwrap();
    assert_eq!(values.len(), 4);
    assert_eq!(values[0], ModuleValue::UnsignedInt(42));
    assert_eq!(values[1], ModuleValue::SignedInt(-1));
    assert_eq!(values[2], ModuleValue::String(b"hi".to_vec()));
    if let ModuleValue::Double(d) = values[3] {
        assert!((d - 1.234).abs() < 1e-10);
    } else {
        panic!("expected Double");
    }
}

#[test]
fn test_read_module_data_empty() {
    // Just an EOF opcode
    let data = vec![0u8]; // EOF
    let mut rdr = make_test_reader(&data);
    let values = rdr.read_module_data().unwrap();
    assert!(values.is_empty());
}

#[test]
fn test_module_2_yields_entry() {
    // Build a minimal RDB with a MODULE_2 entry
    let mut data = Vec::new();
    data.extend_from_slice(b"VALKEY080");
    data.push(RDB_OPCODE_SELECTDB);
    data.push(0);
    data.push(RDB_OPCODE_RESIZEDB);
    data.push(1);
    data.push(0);
    // MODULE_2 entry
    data.push(RDB_TYPE_MODULE_2);
    // key: "modkey"
    data.push(6);
    data.extend_from_slice(b"modkey");
    // module_id = moduleTypeEncodeId("testAAAAA", version=1) per Valkey's
    // module.c encoding (9-char name in upper 54 bits, 10-bit version).
    let module_id: u64 = 13108620618415210497;
    // module_id as length encoding (64-bit, big-endian per RDB spec)
    data.push(0x81); // RDB_64BITLEN
    data.extend_from_slice(&module_id.to_be_bytes());
    // module data: UINT(42), EOF
    data.push(2); // UINT opcode
    data.push(42); // value (must be <= 63 for 6-bit length encoding)
    data.push(0); // EOF
    // End of RDB
    data.push(RDB_OPCODE_EOF);
    data.extend_from_slice(&[0u8; 8]); // CRC placeholder

    let reader = RdbReader::new(Cursor::new(data)).unwrap();
    let entries: Vec<_> = reader
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, b"modkey");
    if let RdbValue::Module(ref m) = entries[0].value {
        assert_eq!(m.module_name, "testAAAAA");
        assert_eq!(m.module_version, 1);
        assert_eq!(m.values.len(), 1);
        assert_eq!(m.values[0], ModuleValue::UnsignedInt(42));
    } else {
        panic!("expected Module value");
    }
}

// --- read_double_value error paths ---

#[test]
fn test_read_double_value_invalid_string() {
    // A 3-byte ASCII string "abc" that isn't a valid float
    let mut rdr = make_test_reader(&[3u8, b'a', b'b', b'c']);
    assert!(rdr.read_double_value().is_err());
}

#[test]
fn test_read_double_value_stringified_inf_nan() {
    // Valkey can produce stringified "inf", "-inf", "nan" in ZSET v1 scores.
    // Rust's f64::from_str handles these correctly.
    let mut data = vec![3u8];
    data.extend_from_slice(b"inf");
    let mut rdr = make_test_reader(&data);
    let v = rdr.read_double_value().unwrap();
    assert!(v.is_infinite() && v.is_sign_positive());

    let mut data = vec![4u8];
    data.extend_from_slice(b"-inf");
    let mut rdr = make_test_reader(&data);
    let v = rdr.read_double_value().unwrap();
    assert!(v.is_infinite() && v.is_sign_negative());

    let mut data = vec![3u8];
    data.extend_from_slice(b"NaN");
    let mut rdr = make_test_reader(&data);
    let v = rdr.read_double_value().unwrap();
    assert!(v.is_nan());
}

// --- Chunked reading tests ---

#[test]
fn test_chunked_hash_splits_correctly() {
    // 10-field hash with max_key_elements=3 → 4 entries (3+3+3+1)
    let mut payload = rdb_len(10);
    for i in 0..10 {
        payload.extend_from_slice(&rdb_string(format!("field_{i}").as_bytes()));
        payload.extend_from_slice(&rdb_string(format!("value_{i}").as_bytes()));
    }
    let data = make_rdb_entry(RDB_TYPE_HASH, b"bighash", &payload);

    let reader = RdbReader::new(Cursor::new(data)).unwrap().with_max_key_elements(3);
    let entries: Vec<_> = reader.map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 4, "10 fields / 3 = 4 chunks (3+3+3+1)");

    // All chunks should have the same key and total_elements
    for entry in &entries {
        assert_eq!(entry.key, b"bighash");
        assert_eq!(entry.total_elements, Some(10));
    }

    // Check element_offset values
    assert_eq!(entries[0].element_offset, Some(0));
    assert_eq!(entries[1].element_offset, Some(3));
    assert_eq!(entries[2].element_offset, Some(6));
    assert_eq!(entries[3].element_offset, Some(9));

    // Check chunk sizes
    let sizes: Vec<usize> = entries
        .iter()
        .map(|e| match &e.value {
            RdbValue::Hash(f) => f.len(),
            other => panic!("expected Hash, got {:?}", other),
        })
        .collect();
    assert_eq!(sizes, vec![3, 3, 3, 1]);

    // Verify all 10 fields are present in order
    let all_fields: Vec<String> = entries
        .iter()
        .flat_map(|e| match &e.value {
            RdbValue::Hash(f) => f.iter().map(|hf| String::from_utf8(hf.field.clone()).unwrap()).collect::<Vec<_>>(),
            _ => vec![],
        })
        .collect();
    let expected: Vec<String> = (0..10).map(|i| format!("field_{i}")).collect();
    assert_eq!(all_fields, expected);
}

#[test]
fn test_chunked_list_index_continuity() {
    // 8-element list with max=3 → verify indices across chunks are 0..8
    let mut payload = rdb_len(8);
    for i in 0..8 {
        payload.extend_from_slice(&rdb_string(format!("item_{i}").as_bytes()));
    }
    let data = make_rdb_entry(RDB_TYPE_LIST, b"biglist", &payload);

    let reader = RdbReader::new(Cursor::new(data)).unwrap().with_max_key_elements(3);
    let entries: Vec<_> = reader.map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 3, "8 elements / 3 = 3 chunks (3+3+2)");

    // Verify element_offset continuity
    assert_eq!(entries[0].element_offset, Some(0));
    assert_eq!(entries[1].element_offset, Some(3));
    assert_eq!(entries[2].element_offset, Some(6));

    // All have total_elements=8
    for e in &entries {
        assert_eq!(e.total_elements, Some(8));
    }

    // Collect all elements in order
    let all_items: Vec<String> = entries
        .iter()
        .flat_map(|e| match &e.value {
            RdbValue::List(elems) => elems.iter().map(|el| String::from_utf8(el.clone()).unwrap()).collect::<Vec<_>>(),
            _ => vec![],
        })
        .collect();
    let expected: Vec<String> = (0..8).map(|i| format!("item_{i}")).collect();
    assert_eq!(all_items, expected);
}

#[test]
fn test_no_chunking_below_threshold() {
    // 5-element set with max=10 → single entry, total_elements=None
    let mut payload = rdb_len(5);
    for i in 0..5 {
        payload.extend_from_slice(&rdb_string(format!("m_{i}").as_bytes()));
    }
    let data = make_rdb_entry(RDB_TYPE_SET, b"smallset", &payload);

    let reader = RdbReader::new(Cursor::new(data)).unwrap().with_max_key_elements(10);
    let entries: Vec<_> = reader.map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].total_elements, None);
    assert_eq!(entries[0].element_offset, None);
    match &entries[0].value {
        RdbValue::Set(members) => assert_eq!(members.len(), 5),
        other => panic!("expected Set, got {:?}", other),
    }
}

#[test]
fn test_no_chunking_for_compact_types() {
    // Listpack-encoded hash — should never chunk regardless of max
    let f = std::fs::File::open(format!(
        "{}/../../tests/fixtures/hash_listpack.rdb",
        env!("CARGO_MANIFEST_DIR")
    )).unwrap();
    let reader = RdbReader::new(f).unwrap().with_max_key_elements(1);
    let entries: Vec<_> = reader.filter_map(|e| e.ok()).collect();
    // Should be one entry regardless of max_key_elements=1
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].total_elements, None);
    assert_eq!(entries[0].element_offset, None);
}

#[test]
fn test_chunked_zset_v2() {
    // 6-element zset_2 with max=4 → 2 chunks (4+2)
    let mut payload = rdb_len(6);
    for i in 0..6u64 {
        payload.extend_from_slice(&rdb_string(format!("member_{i}").as_bytes()));
        payload.extend_from_slice(&(i as f64).to_le_bytes());
    }
    let data = make_rdb_entry(RDB_TYPE_ZSET_2, b"myzset", &payload);

    let reader = RdbReader::new(Cursor::new(data)).unwrap().with_max_key_elements(4);
    let entries: Vec<_> = reader.map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].total_elements, Some(6));
    assert_eq!(entries[0].element_offset, Some(0));
    assert_eq!(entries[1].total_elements, Some(6));
    assert_eq!(entries[1].element_offset, Some(4));

    // Verify all members/scores
    let all_pairs: Vec<(String, f64)> = entries
        .iter()
        .flat_map(|e| match &e.value {
            RdbValue::SortedSet(pairs) => pairs.iter().map(|(m, s)| (String::from_utf8(m.clone()).unwrap(), *s)).collect::<Vec<_>>(),
            _ => vec![],
        })
        .collect();
    assert_eq!(all_pairs.len(), 6);
    for (i, (member, score)) in all_pairs.iter().enumerate() {
        assert_eq!(member, &format!("member_{i}"));
        assert!((score - i as f64).abs() < f64::EPSILON);
    }
}

#[test]
fn test_chunked_set() {
    // 7-element set with max=3 → 3 chunks (3+3+1)
    let mut payload = rdb_len(7);
    for i in 0..7 {
        payload.extend_from_slice(&rdb_string(format!("m_{i}").as_bytes()));
    }
    let data = make_rdb_entry(RDB_TYPE_SET, b"bigset", &payload);

    let reader = RdbReader::new(Cursor::new(data)).unwrap().with_max_key_elements(3);
    let entries: Vec<_> = reader.map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 3, "7 members / 3 = 3 chunks (3+3+1)");

    for e in &entries {
        assert_eq!(e.key, b"bigset");
        assert_eq!(e.total_elements, Some(7));
    }
    assert_eq!(entries[0].element_offset, Some(0));
    assert_eq!(entries[1].element_offset, Some(3));
    assert_eq!(entries[2].element_offset, Some(6));

    let all_members: Vec<String> = entries
        .iter()
        .flat_map(|e| match &e.value {
            RdbValue::Set(m) => m.iter().map(|v| String::from_utf8(v.clone()).unwrap()).collect::<Vec<_>>(),
            _ => vec![],
        })
        .collect();
    let expected: Vec<String> = (0..7).map(|i| format!("m_{i}")).collect();
    assert_eq!(all_members, expected);
}

#[test]
fn test_chunked_zset_v1() {
    // 5-element zset v1 with max=2 → 3 chunks (2+2+1)
    let mut payload = rdb_len(5);
    for i in 0..5 {
        payload.extend_from_slice(&rdb_string(format!("z_{i}").as_bytes()));
        let score_str = format!("{}.5", i);
        payload.push(score_str.len() as u8);
        payload.extend_from_slice(score_str.as_bytes());
    }
    let data = make_rdb_entry(RDB_TYPE_ZSET, b"zset_v1", &payload);

    let reader = RdbReader::new(Cursor::new(data)).unwrap().with_max_key_elements(2);
    let entries: Vec<_> = reader.map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 3, "5 elements / 2 = 3 chunks (2+2+1)");

    for e in &entries {
        assert_eq!(e.key, b"zset_v1");
        assert_eq!(e.total_elements, Some(5));
    }
    assert_eq!(entries[0].element_offset, Some(0));
    assert_eq!(entries[1].element_offset, Some(2));
    assert_eq!(entries[2].element_offset, Some(4));

    let all_pairs: Vec<(String, f64)> = entries
        .iter()
        .flat_map(|e| match &e.value {
            RdbValue::SortedSet(pairs) => pairs.iter().map(|(m, s)| (String::from_utf8(m.clone()).unwrap(), *s)).collect::<Vec<_>>(),
            _ => vec![],
        })
        .collect();
    assert_eq!(all_pairs.len(), 5);
    for (i, (member, score)) in all_pairs.iter().enumerate() {
        assert_eq!(member, &format!("z_{i}"));
        assert!((*score - (i as f64 + 0.5)).abs() < f64::EPSILON);
    }
}

// --- Intset MAX_INTSET_ELEMENTS cap ---

#[test]
fn test_intset_element_cap() {
    // Forge an intset header claiming 20M int16 elements (exceeds 10M cap)
    let mut blob = Vec::new();
    blob.extend_from_slice(&2u32.to_le_bytes());         // encoding: int16
    blob.extend_from_slice(&20_000_000u32.to_le_bytes()); // length: 20M
    // Don't need actual data — should fail before reading elements
    assert!(crate::intset::decode(&blob).is_err());
}

// --- CRC mismatch ---

#[test]
fn test_crc_mismatch_detected() {
    let mut data = Vec::new();
    data.extend_from_slice(b"VALKEY080");
    data.push(RDB_OPCODE_SELECTDB);
    data.push(0);
    data.push(RDB_OPCODE_RESIZEDB);
    data.push(1);
    data.push(0);
    data.push(RDB_TYPE_STRING);
    data.push(1); data.push(b'k'); // key
    data.push(1); data.push(b'v'); // value
    data.push(RDB_OPCODE_EOF);
    // Write a non-zero CRC that doesn't match the computed one
    data.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE]);

    let reader = RdbReader::new(Cursor::new(data)).unwrap();
    let results: Vec<_> = reader.collect();
    // Should get the entry OK, then a CRC error
    assert!(results.iter().any(|r| match r {
        Err(RdbError::CorruptData(msg)) => msg.contains("CRC64 mismatch"),
        _ => false,
    }), "expected CRC64 mismatch error");
}

// --- FREQ / IDLE opcodes ---

#[test]
fn test_freq_opcode() {
    let mut data = Vec::new();
    data.extend_from_slice(b"VALKEY080");
    data.push(RDB_OPCODE_SELECTDB);
    data.push(0);
    data.push(RDB_OPCODE_RESIZEDB);
    data.push(1);
    data.push(0);
    // FREQ opcode before a string entry
    data.push(RDB_OPCODE_FREQ);
    data.push(42); // frequency value
    data.push(RDB_TYPE_STRING);
    data.push(1); data.push(b'k');
    data.push(1); data.push(b'v');
    data.push(RDB_OPCODE_EOF);
    data.extend_from_slice(&[0u8; 8]); // CRC placeholder

    let reader = RdbReader::new(Cursor::new(data)).unwrap();
    let entries: Vec<_> = reader.filter_map(|e| e.ok()).collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].lfu_frequency, Some(42));
}

#[test]
fn test_idle_opcode() {
    let mut data = Vec::new();
    data.extend_from_slice(b"VALKEY080");
    data.push(RDB_OPCODE_SELECTDB);
    data.push(0);
    data.push(RDB_OPCODE_RESIZEDB);
    data.push(1);
    data.push(0);
    // IDLE opcode before a string entry
    data.push(RDB_OPCODE_IDLE);
    data.push(60); // idle seconds (6-bit length encoding, value=60)
    data.push(RDB_TYPE_STRING);
    data.push(1); data.push(b'k');
    data.push(1); data.push(b'v');
    data.push(RDB_OPCODE_EOF);
    data.extend_from_slice(&[0u8; 8]); // CRC placeholder

    let reader = RdbReader::new(Cursor::new(data)).unwrap();
    let entries: Vec<_> = reader.filter_map(|e| e.ok()).collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].lru_idle_secs, Some(60));
}

// --- Test helper ---

/// Create a minimal RdbReader from raw bytes (skips header parsing).
fn make_test_reader(data: &[u8]) -> RdbReader<Cursor<Vec<u8>>> {
    RdbReader {
        reader: CrcReader::new(BufReader::new(Cursor::new(data.to_vec())), 0),
        header: RdbHeader {
            magic: RdbMagic::Valkey,
            version: 80,
        },
        metadata: RdbMetadata::default(),
        current_db: 0,
        pending_expiry_ms: None,
        pending_lru_idle: None,
        pending_lfu_freq: None,
        preamble_byte: None,
        finished: false,
        max_key_elements: None,
        chunked: None,
        crc_checked: false,
    }
}
