//! Integration tests that parse real RDB fixture files through the public API.

use rdb_parser::{RdbError, RdbMagic, RdbReader, RdbValue};

fn fixture_path(name: &str) -> String {
    format!(
        "{}/../../tests/fixtures/{name}",
        env!("CARGO_MANIFEST_DIR")
    )
}

// --- Header parsing ---

#[test]
fn valkey_header() {
    let f = std::fs::File::open(fixture_path("basic.rdb")).unwrap();
    let reader = RdbReader::new(f).unwrap();
    assert_eq!(reader.header().magic, RdbMagic::Valkey);
    assert_eq!(reader.header().version, 80);
}

#[test]
fn redis_header() {
    let f = std::fs::File::open(fixture_path("redis_compat.rdb")).unwrap();
    let reader = RdbReader::new(f).unwrap();
    assert_eq!(reader.header().magic, RdbMagic::Redis);
    assert_eq!(reader.header().version, 9);
}

#[test]
fn empty_rdb() {
    let f = std::fs::File::open(fixture_path("empty.rdb")).unwrap();
    let reader = RdbReader::new(f).unwrap();
    let entries: Vec<_> = reader.collect();
    assert!(entries.is_empty(), "empty.rdb should have no entries");
}

#[test]
fn valkey_metadata() {
    let f = std::fs::File::open(fixture_path("basic.rdb")).unwrap();
    let reader = RdbReader::new(f).unwrap();
    assert!(
        reader.metadata().server_version().is_some(),
        "should have server version in AUX immediately after new()"
    );
}

// --- String fixtures ---

#[test]
fn redis_compat_strings() {
    let f = std::fs::File::open(fixture_path("redis_compat.rdb")).unwrap();
    let reader = RdbReader::new(f).unwrap();

    let mut found_mystring = false;
    for entry in reader {
        match entry {
            Ok(e) if e.key == b"mystring" => {
                assert_eq!(e.value, RdbValue::String(b"hello world".to_vec()));
                assert_eq!(e.db, 0);
                assert_eq!(e.type_name(), "string");
                found_mystring = true;
            }
            Ok(_) => {}
            Err(RdbError::UnknownType(_)) => {}
            Err(e) => panic!("unexpected error: {}", e),
        }
    }
    assert!(found_mystring, "should find mystring key in redis_compat.rdb");
}

#[test]
fn int_encoded_strings() {
    let f = std::fs::File::open(fixture_path("encodings.rdb")).unwrap();
    let reader = RdbReader::new(f).unwrap();

    let mut found_small = false;
    let mut found_large = false;

    for entry in reader {
        match entry {
            Ok(e) if e.key == b"int_string_small" => {
                assert_eq!(e.value, RdbValue::String(b"42".to_vec()));
                found_small = true;
            }
            Ok(e) if e.key == b"int_string_large" => {
                assert_eq!(e.value, RdbValue::String(b"1234567890".to_vec()));
                found_large = true;
            }
            _ => {}
        }
    }
    assert!(found_small, "should find int_string_small = 42");
    assert!(found_large, "should find int_string_large = 1234567890");
}

// --- Expiry ---

#[test]
fn expiry() {
    let f = std::fs::File::open(fixture_path("expiry.rdb")).unwrap();
    let reader = RdbReader::new(f).unwrap();

    let mut found_no_ttl = false;
    let mut found_future_ttl = false;

    for entry in reader {
        match entry {
            Ok(e) if e.key == b"no_ttl_key" => {
                assert!(e.expiry_ms.is_none());
                found_no_ttl = true;
            }
            Ok(e) if e.key == b"future_ttl_key" => {
                assert_eq!(e.expiry_ms, Some(4102444800000));
                found_future_ttl = true;
            }
            _ => {}
        }
    }
    assert!(found_no_ttl, "should find no_ttl_key");
    assert!(found_future_ttl, "should find future_ttl_key with TTL");
}

// --- Multi-database ---

#[test]
fn multi_db() {
    let f = std::fs::File::open(fixture_path("multi_db.rdb")).unwrap();
    let reader = RdbReader::new(f).unwrap();

    let mut found_db0 = false;
    let mut found_db1 = false;

    for entry in reader {
        match entry {
            Ok(e) if e.key == b"db0_key" => {
                assert_eq!(e.db, 0);
                found_db0 = true;
            }
            Ok(e) if e.key == b"db1_key" => {
                assert_eq!(e.db, 1);
                found_db1 = true;
            }
            _ => {}
        }
    }
    assert!(found_db0, "should find db0_key in db 0");
    assert!(found_db1, "should find db1_key in db 1");
}

// --- HASH_2 with per-field TTL ---

#[test]
fn hash_field_ttl() {
    let f = std::fs::File::open(fixture_path("hash_field_ttl.rdb")).unwrap();
    let reader = RdbReader::new(f).unwrap();
    let entries: Vec<_> = reader.map(|e| e.unwrap()).collect();

    let hashes: Vec<_> = entries.iter().filter(|e| e.type_name() == "hash").collect();
    assert_eq!(hashes.len(), 2, "expected 2 hash keys");

    // hfe_hash: HASH_2 with per-field TTL
    let hfe = hashes.iter().find(|e| e.key == b"hfe_hash").expect("hfe_hash key");
    let fields = match &hfe.value {
        RdbValue::Hash(f) => f,
        other => panic!("expected Hash, got {:?}", other),
    };
    assert_eq!(fields.len(), 3);

    let persist = fields.iter().find(|f| f.field == b"field_persist").unwrap();
    assert_eq!(persist.value, b"no expiry");
    assert_eq!(persist.expiry_ms, None);

    let future = fields.iter().find(|f| f.field == b"field_future").unwrap();
    assert_eq!(future.value, b"expires later");
    assert_eq!(future.expiry_ms, Some(4_102_444_800_000));

    let short = fields.iter().find(|f| f.field == b"field_short").unwrap();
    assert_eq!(short.value, b"expires soon");
    assert_eq!(short.expiry_ms, Some(4_102_444_800_000));

    // normal_hash: regular hash, no per-field TTL
    let normal = hashes.iter().find(|e| e.key == b"normal_hash").expect("normal_hash key");
    let fields = match &normal.value {
        RdbValue::Hash(f) => f,
        other => panic!("expected Hash, got {:?}", other),
    };
    assert_eq!(fields.len(), 2);
    assert!(
        fields.iter().all(|f| f.expiry_ms.is_none()),
        "normal hash should have no field TTLs"
    );
}

// --- Streams ---

type ExpectedField = (&'static [u8], &'static [u8]);
type ExpectedStreamEntry = (&'static str, &'static [ExpectedField]);

/// Assert that a decoded stream's entries match the expected sequence of
/// `(id, &[(field, value)])` tuples.
fn assert_stream_entries_eq(
    got: &[rdb_parser::StreamEntry],
    want: &[ExpectedStreamEntry],
) {
    assert_eq!(got.len(), want.len(), "stream entry count");
    for (se, (id, fields)) in got.iter().zip(want.iter()) {
        assert_eq!(se.id, *id, "stream entry id");
        assert_eq!(se.fields.len(), fields.len(), "field count for id {}", se.id);
        for (actual, expected) in se.fields.iter().zip(fields.iter()) {
            assert_eq!(actual.0.as_slice(), expected.0, "field name in id {}", se.id);
            assert_eq!(actual.1.as_slice(), expected.1, "field value in id {}", se.id);
        }
    }
}

#[test]
fn streams_roundtrip() {
    // streams.rdb contains two streams: `mystream` with three {name, age}
    // entries (exercises the stream listpack SAMEFIELDS path) and
    // `grouped_stream` with three event-log entries, one of which has a
    // third field (exercises the non-SAMEFIELDS path) — plus a consumer
    // group that the parser consumes but does not surface. These
    // assertions pin the full decoded structure so regressions in stream
    // IDs, field ordering, SAMEFIELDS handling, consumer-group skipping,
    // or length/last_id propagation are caught.

    let f = std::fs::File::open(fixture_path("streams.rdb")).unwrap();
    let reader = RdbReader::new(f).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();

    let streams: Vec<_> = entries
        .iter()
        .filter_map(|e| match &e.value {
            RdbValue::Stream(s) => Some((e.key.as_slice(), s)),
            _ => None,
        })
        .collect();
    assert_eq!(streams.len(), 2, "expected two streams in streams.rdb");

    let (_, mystream) = streams
        .iter()
        .find(|(k, _)| *k == b"mystream")
        .expect("mystream key missing");
    assert_eq!(mystream.length, 3);
    assert_eq!(mystream.last_id, "1772784406058-0");
    assert_stream_entries_eq(
        &mystream.entries,
        &[
            ("1772784406052-0", &[(b"name", b"alice"), (b"age", b"30")]),
            ("1772784406055-0", &[(b"name", b"bob"), (b"age", b"25")]),
            ("1772784406058-0", &[(b"name", b"charlie"), (b"age", b"35")]),
        ],
    );

    let (_, grouped) = streams
        .iter()
        .find(|(k, _)| *k == b"grouped_stream")
        .expect("grouped_stream key missing");
    assert_eq!(grouped.length, 3);
    assert_eq!(grouped.last_id, "1772784406065-0");
    assert_stream_entries_eq(
        &grouped.entries,
        &[
            ("1772784406060-0", &[(b"event", b"login"), (b"user", b"alice")]),
            (
                "1772784406062-0",
                &[
                    (b"event", b"purchase"),
                    (b"user", b"bob"),
                    (b"item", b"widget"),
                ],
            ),
            ("1772784406065-0", &[(b"event", b"logout"), (b"user", b"alice")]),
        ],
    );
}

// --- Listpack-encoded types ---

#[test]
fn set_listpack() {
    let f = std::fs::File::open(fixture_path("set_listpack.rdb")).unwrap();
    let reader = RdbReader::new(f).unwrap();
    let mut found = false;
    for entry in reader {
        let e = entry.unwrap();
        if e.key == b"myset" {
            if let RdbValue::Set(members) = &e.value {
                let mut sorted: Vec<&[u8]> = members.iter().map(|m| m.as_slice()).collect();
                sorted.sort();
                assert_eq!(
                    sorted,
                    vec![&b"alpha"[..], &b"beta"[..], &b"delta"[..], &b"gamma"[..]]
                );
                found = true;
            } else {
                panic!("expected Set, got {:?}", e.value);
            }
        }
    }
    assert!(found, "should find myset");
}

#[test]
fn hash_listpack() {
    let f = std::fs::File::open(fixture_path("hash_listpack.rdb")).unwrap();
    let reader = RdbReader::new(f).unwrap();
    let mut found = false;
    for entry in reader {
        let e = entry.unwrap();
        if e.key == b"myhash" {
            if let RdbValue::Hash(fields) = &e.value {
                let mut map: Vec<(&[u8], &[u8])> = fields
                    .iter()
                    .map(|f| (f.field.as_slice(), f.value.as_slice()))
                    .collect();
                map.sort_by_key(|&(k, _)| k);
                assert_eq!(
                    map,
                    vec![
                        (&b"age"[..], &b"30"[..]),
                        (&b"city"[..], &b"nyc"[..]),
                        (&b"name"[..], &b"alice"[..]),
                    ]
                );
                // Listpack hashes have no per-field TTL
                assert!(fields.iter().all(|f| f.expiry_ms.is_none()));
                found = true;
            } else {
                panic!("expected Hash, got {:?}", e.value);
            }
        }
    }
    assert!(found, "should find myhash");
}

#[test]
fn zset_listpack() {
    let f = std::fs::File::open(fixture_path("zset_listpack.rdb")).unwrap();
    let reader = RdbReader::new(f).unwrap();
    let mut found = false;
    for entry in reader {
        let e = entry.unwrap();
        if e.key == b"myzset" {
            if let RdbValue::SortedSet(pairs) = &e.value {
                let mut sorted: Vec<(&[u8], f64)> =
                    pairs.iter().map(|(m, s)| (m.as_slice(), *s)).collect();
                sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                assert_eq!(sorted[0].0, b"first");
                assert!((sorted[0].1 - 1.5).abs() < 1e-10);
                assert_eq!(sorted[1].0, b"second");
                assert!((sorted[1].1 - 2.7).abs() < 1e-10);
                assert_eq!(sorted[2].0, b"third");
                assert!((sorted[2].1 - 3.0).abs() < 1e-10);
                found = true;
            } else {
                panic!("expected SortedSet, got {:?}", e.value);
            }
        }
    }
    assert!(found, "should find myzset");
}

#[test]
fn listpack_all() {
    let f = std::fs::File::open(fixture_path("listpack_all.rdb")).unwrap();
    let reader = RdbReader::new(f).unwrap();
    let mut keys: Vec<String> = Vec::new();
    for entry in reader {
        let e = entry.unwrap();
        keys.push(String::from_utf8(e.key.clone()).unwrap());
        match e.type_name() {
            "set" => assert!(matches!(e.value, RdbValue::Set(_))),
            "hash" => assert!(matches!(e.value, RdbValue::Hash(_))),
            "zset" => assert!(matches!(e.value, RdbValue::SortedSet(_))),
            other => panic!("unexpected type: {}", other),
        }
    }
    keys.sort();
    assert_eq!(keys, vec!["myhash", "myset", "myzset"]);
}

// --- Ziplist-encoded types ---

#[test]
fn list_ziplist() {
    let f = std::fs::File::open(fixture_path("list_ziplist.rdb")).unwrap();
    let reader = RdbReader::new(f).unwrap();
    let mut found = false;
    for entry in reader {
        let e = entry.unwrap();
        if e.key == b"mylist" {
            if let RdbValue::List(ref elems) = e.value {
                assert_eq!(elems.len(), 4);
                assert_eq!(elems[0], b"alpha");
                assert_eq!(elems[1], b"beta");
                assert_eq!(elems[2], b"7");
                assert_eq!(elems[3], b"-500");
                found = true;
            } else {
                panic!("expected List, got {:?}", e.value);
            }
        }
    }
    assert!(found, "should find mylist");
}

#[test]
fn hash_ziplist() {
    let f = std::fs::File::open(fixture_path("hash_ziplist.rdb")).unwrap();
    let reader = RdbReader::new(f).unwrap();
    let mut found = false;
    for entry in reader {
        let e = entry.unwrap();
        if e.key == b"myhash" {
            if let RdbValue::Hash(ref fields) = e.value {
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0].field, b"name");
                assert_eq!(fields[0].value, b"alice");
                assert_eq!(fields[1].field, b"age");
                assert_eq!(fields[1].value, b"10");
                assert_eq!(fields[2].field, b"city");
                assert_eq!(fields[2].value, b"nyc");
                found = true;
            } else {
                panic!("expected Hash, got {:?}", e.value);
            }
        }
    }
    assert!(found, "should find myhash");
}

#[test]
fn zset_ziplist() {
    let f = std::fs::File::open(fixture_path("zset_ziplist.rdb")).unwrap();
    let reader = RdbReader::new(f).unwrap();
    let mut found = false;
    for entry in reader {
        let e = entry.unwrap();
        if e.key == b"myzset" {
            if let RdbValue::SortedSet(ref pairs) = e.value {
                assert_eq!(pairs.len(), 3);
                let mut sorted: Vec<_> =
                    pairs.iter().map(|(m, s)| (m.as_slice(), *s)).collect();
                sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                assert_eq!(sorted[0].0, b"first");
                assert!((sorted[0].1 - 1.5).abs() < 1e-10);
                assert_eq!(sorted[1].0, b"second");
                assert!((sorted[1].1 - 2.7).abs() < 1e-10);
                assert_eq!(sorted[2].0, b"third");
                assert!((sorted[2].1 - 3.0).abs() < 1e-10);
                found = true;
            } else {
                panic!("expected SortedSet, got {:?}", e.value);
            }
        }
    }
    assert!(found, "should find myzset");
}

#[test]
fn ziplist_all() {
    let f = std::fs::File::open(fixture_path("ziplist_all.rdb")).unwrap();
    let reader = RdbReader::new(f).unwrap();
    let mut keys: Vec<String> = Vec::new();
    for entry in reader {
        let e = entry.unwrap();
        keys.push(String::from_utf8(e.key.clone()).unwrap());
        match e.type_name() {
            "list" => assert!(matches!(e.value, RdbValue::List(_))),
            "hash" => assert!(matches!(e.value, RdbValue::Hash(_))),
            "zset" => assert!(matches!(e.value, RdbValue::SortedSet(_))),
            other => panic!("unexpected type: {}", other),
        }
    }
    keys.sort();
    assert_eq!(keys, vec!["myhash", "mylist", "myzset"]);
}

// --- Intset ---

#[test]
fn set_intset() {
    let f = std::fs::File::open(fixture_path("set_intset.rdb")).unwrap();
    let reader = RdbReader::new(f).unwrap();
    let entries: Vec<_> = reader.map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, b"myset");
    assert_eq!(entries[0].type_name(), "set");
    assert_eq!(entries[0].encoding_name(), "intset");
    match &entries[0].value {
        RdbValue::Set(members) => {
            assert_eq!(members.len(), 5);
            let strs: Vec<&str> = members
                .iter()
                .map(|m| std::str::from_utf8(m).unwrap())
                .collect();
            assert_eq!(strs, vec!["-100", "-1", "0", "42", "1000"]);
        }
        other => panic!("expected Set, got {:?}", other),
    }
}

// --- Quicklist v1 and v2 ---

#[test]
fn list_quicklist() {
    let f = std::fs::File::open(fixture_path("list_quicklist.rdb")).unwrap();
    let reader = RdbReader::new(f).unwrap();
    let entries: Vec<_> = reader.map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, b"mylist");
    assert_eq!(entries[0].type_name(), "list");
    assert_eq!(entries[0].encoding_name(), "quicklist");
    match &entries[0].value {
        RdbValue::List(elems) => {
            let strs: Vec<&str> = elems
                .iter()
                .map(|e| std::str::from_utf8(e).unwrap())
                .collect();
            assert_eq!(strs, vec!["hello", "world", "foo", "7"]);
        }
        other => panic!("expected List, got {:?}", other),
    }
}

#[test]
fn list_quicklist2() {
    let f = std::fs::File::open(fixture_path("list_quicklist2.rdb")).unwrap();
    let reader = RdbReader::new(f).unwrap();
    let entries: Vec<_> = reader.map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, b"mylist2");
    assert_eq!(entries[0].type_name(), "list");
    assert_eq!(entries[0].encoding_name(), "quicklist2");
    match &entries[0].value {
        RdbValue::List(elems) => {
            let strs: Vec<&str> = elems
                .iter()
                .map(|e| std::str::from_utf8(e).unwrap())
                .collect();
            // node1 packed: alpha, beta; node2 packed: 42, 100; node3 plain: standalone
            assert_eq!(strs, vec!["alpha", "beta", "42", "100", "standalone"]);
        }
        other => panic!("expected List, got {:?}", other),
    }
}
