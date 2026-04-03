//! Integration test: parse RDB fixture → Arrow batches → Parquet → read back.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, AsArray, RecordBatch};
use rdb_parser::RdbReader;
use rdb_to_arrow::{ArrowBatcher, BatcherConfig, TypeTag, metadata_from_rdb};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

fn fixture_path(name: &str) -> String {
    format!(
        "{}/../../tests/fixtures/{name}",
        env!("CARGO_MANIFEST_DIR")
    )
}

/// Collect all batches from an RDB fixture, grouped by TypeTag.
fn collect_batches(path: &str) -> (HashMap<TypeTag, Vec<RecordBatch>>, rdb_parser::RdbMetadata) {
    let f = std::fs::File::open(path).unwrap();
    let reader = RdbReader::new(f).unwrap();
    let meta = reader.metadata().clone();

    let batcher = ArrowBatcher::new(BatcherConfig::default());
    let mut by_type: HashMap<TypeTag, Vec<RecordBatch>> = HashMap::new();

    for result in batcher.process(reader) {
        let typed_batch = result.unwrap();
        by_type
            .entry(typed_batch.tag)
            .or_default()
            .push(typed_batch.batch);
    }

    (by_type, meta)
}

/// Write RecordBatches to Parquet in memory, then read back.
fn parquet_roundtrip(
    tag: TypeTag,
    batches: &[RecordBatch],
    file_metadata: &HashMap<String, String>,
) -> (Vec<RecordBatch>, HashMap<String, String>) {
    let schema = if file_metadata.is_empty() {
        rdb_to_arrow::schema_for(tag)
    } else {
        rdb_to_arrow::schema_for(tag).with_metadata(file_metadata.clone())
    };

    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .build();

    let mut buf = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buf, Arc::new(schema), Some(props)).unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.close().unwrap();

    let pq_reader =
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(buf)).unwrap();
    let schema_meta = pq_reader.schema().metadata().clone();
    let read_batches: Vec<_> = pq_reader
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    (read_batches, schema_meta)
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Parse basic.rdb through the full pipeline and verify the Parquet output.
#[test]
fn basic_rdb_to_parquet_roundtrip() {
    let (by_type, meta) = collect_batches(&fixture_path("basic.rdb"));

    let file_meta = metadata_from_rdb(&meta);
    assert!(
        file_meta.contains_key("rdb.valkey-ver") || file_meta.contains_key("rdb.redis-ver"),
        "should contain server version"
    );
    assert_eq!(file_meta["rdb.exported_by"], "valkey-rdb-tools");

    // --- String: mystring + expiring_key = 2 rows ---
    let string_batches = by_type.get(&TypeTag::String).expect("should have strings");
    assert_eq!(total_rows(string_batches), 2);

    let (pq_batches, pq_meta) = parquet_roundtrip(TypeTag::String, string_batches, &file_meta);
    assert_eq!(total_rows(&pq_batches), 2);
    assert_eq!(pq_batches[0].num_columns(), 9); // 8 common + value

    // Verify file metadata roundtrips.
    assert_eq!(pq_meta.get("rdb.exported_by").map(|s| s.as_str()), Some("valkey-rdb-tools"));
    assert!(
        pq_meta.contains_key("rdb.valkey-ver") || pq_meta.contains_key("rdb.redis-ver"),
        "server version should roundtrip through Parquet"
    );

    // --- List: mylist [a, b, c] = 3 rows ---
    let list_batches = by_type.get(&TypeTag::List).expect("should have lists");
    assert_eq!(total_rows(list_batches), 3);

    let (pq_batches, _) = parquet_roundtrip(TypeTag::List, list_batches, &file_meta);
    assert_eq!(total_rows(&pq_batches), 3);
    assert_eq!(pq_batches[0].num_columns(), 10); // 8 common + index + element

    // --- Set: myset {x, y, z} = 3 rows ---
    let set_batches = by_type.get(&TypeTag::Set).expect("should have sets");
    assert_eq!(total_rows(set_batches), 3);

    let (pq_batches, _) = parquet_roundtrip(TypeTag::Set, set_batches, &file_meta);
    assert_eq!(total_rows(&pq_batches), 3);

    // --- SortedSet: myzset {alice:1.5, bob:2.7, charlie:0.3} = 3 rows ---
    let zset_batches = by_type.get(&TypeTag::SortedSet).expect("should have zsets");
    assert_eq!(total_rows(zset_batches), 3);

    let (pq_batches, _) = parquet_roundtrip(TypeTag::SortedSet, zset_batches, &file_meta);
    assert_eq!(total_rows(&pq_batches), 3);
    assert_eq!(pq_batches[0].num_columns(), 10); // 8 common + member + score

    // --- Hash: myhash {field1:value1, field2:value2, field3:value3} = 3 rows ---
    let hash_batches = by_type.get(&TypeTag::Hash).expect("should have hashes");
    assert_eq!(total_rows(hash_batches), 3);

    let (pq_batches, _) = parquet_roundtrip(TypeTag::Hash, hash_batches, &file_meta);
    assert_eq!(total_rows(&pq_batches), 3);
    assert_eq!(pq_batches[0].num_columns(), 11); // 8 common + field + field_value + field_expiry_ms

    // No Geo or HLL in basic.rdb.
    assert!(!by_type.contains_key(&TypeTag::Geo));
    assert!(!by_type.contains_key(&TypeTag::HyperLogLog));
}

/// Verify that key values survive the roundtrip.
#[test]
fn basic_rdb_value_content() {
    let (by_type, _) = collect_batches(&fixture_path("basic.rdb"));

    // Check string values.
    let string_batches = by_type.get(&TypeTag::String).unwrap();
    let batch = &string_batches[0];

    // Find the "mystring" row and verify its value is "hello world".
    let key_col = batch.column(1).as_binary::<i32>(); // key is Binary
    let value_col = batch.column(8).as_binary::<i32>(); // value is the last column

    let mut found = false;
    for i in 0..batch.num_rows() {
        if key_col.value(i) == b"mystring" {
            assert_eq!(value_col.value(i), b"hello world");
            found = true;
        }
    }
    assert!(found, "should find mystring key");
}

/// Verify that expiry survives the roundtrip.
#[test]
fn basic_rdb_expiry() {
    let (by_type, _) = collect_batches(&fixture_path("basic.rdb"));
    let string_batches = by_type.get(&TypeTag::String).unwrap();
    let batch = &string_batches[0];

    let key_col = batch.column(1).as_binary::<i32>();
    let expiry_col = batch.column(3).as_primitive::<arrow::datatypes::Int64Type>(); // expiry_ms

    for i in 0..batch.num_rows() {
        if key_col.value(i) == b"expiring_key" {
            assert!(!expiry_col.is_null(i), "expiring_key should have TTL");
            assert_eq!(expiry_col.value(i), 4102444800000i64); // 2100-01-01
            return;
        }
    }
    panic!("should find expiring_key");
}

/// Parse multi_db.rdb and verify Parquet roundtrip preserves all rows.
#[test]
fn multi_db_roundtrip() {
    let (by_type, _) = collect_batches(&fixture_path("multi_db.rdb"));

    // multi_db.rdb has keys across db0 and db1.
    assert!(!by_type.is_empty(), "should parse some entries");

    // All batches should roundtrip through Parquet.
    for (tag, batches) in &by_type {
        let (pq_batches, _) = parquet_roundtrip(*tag, batches, &HashMap::new());
        assert_eq!(
            total_rows(&pq_batches),
            total_rows(batches),
            "Parquet roundtrip should preserve row count for {tag:?}"
        );
    }
}

/// Parse hash_field_ttl.rdb and verify per-field expiry survives the full
/// pipeline through Arrow and Parquet. This exercises HASH_2 (type 22)
/// which carries per-field TTL — a Valkey-specific encoding.
#[test]
fn hash_field_ttl_roundtrip() {
    let (by_type, meta) = collect_batches(&fixture_path("hash_field_ttl.rdb"));
    let file_meta = metadata_from_rdb(&meta);

    let hash_batches = by_type.get(&TypeTag::Hash).expect("should have hashes");
    // hfe_hash (3 fields) + normal_hash (2 fields) = 5 rows
    assert_eq!(total_rows(hash_batches), 5);

    let (pq_batches, _) = parquet_roundtrip(TypeTag::Hash, hash_batches, &file_meta);
    assert_eq!(total_rows(&pq_batches), 5);
    assert_eq!(pq_batches[0].num_columns(), 11); // 8 common + field + field_value + field_expiry_ms

    // Verify per-field expiry values survived the roundtrip.
    let batch = &pq_batches[0];
    let key_col = batch.column(1).as_binary::<i32>();
    let field_col = batch.column(8).as_binary::<i32>();
    let field_expiry_col = batch.column(10).as_primitive::<arrow::datatypes::Int64Type>();

    let mut found_persist = false;
    let mut found_with_ttl = false;
    let mut found_normal = false;

    for i in 0..batch.num_rows() {
        let key = key_col.value(i);
        let field = field_col.value(i);

        if key == b"hfe_hash" && field == b"field_persist" {
            assert!(field_expiry_col.is_null(i), "field_persist should have no TTL");
            found_persist = true;
        }
        if key == b"hfe_hash" && field == b"field_future" {
            assert!(!field_expiry_col.is_null(i), "field_future should have TTL");
            assert_eq!(field_expiry_col.value(i), 4_102_444_800_000i64);
            found_with_ttl = true;
        }
        if key == b"normal_hash" {
            assert!(field_expiry_col.is_null(i), "normal_hash fields should have no TTL");
            found_normal = true;
        }
    }

    assert!(found_persist, "should find hfe_hash.field_persist");
    assert!(found_with_ttl, "should find hfe_hash.field_future with TTL");
    assert!(found_normal, "should find normal_hash fields");
}

/// Parse encodings.rdb which has many different encoding types and sizes.
#[test]
fn encodings_rdb_all_types_parsed() {
    let (by_type, _) = collect_batches(&fixture_path("encodings.rdb"));

    // Should have all 5 base types.
    assert!(by_type.contains_key(&TypeTag::String), "should have strings");
    assert!(by_type.contains_key(&TypeTag::List), "should have lists");
    assert!(by_type.contains_key(&TypeTag::Set), "should have sets");
    assert!(by_type.contains_key(&TypeTag::SortedSet), "should have zsets");
    assert!(by_type.contains_key(&TypeTag::Hash), "should have hashes");

    // Verify large collections parsed fully.
    // big_hash: 200 fields, big_set: 200 members, big_zset: 200 members, big_list: 500 elements
    // Note: small_zset (scores 1.0, 2.0, 3.0) is detected as Geo (integer scores are valid geohashes).
    let hash_rows = total_rows(by_type.get(&TypeTag::Hash).unwrap());
    assert!(hash_rows >= 202, "should have at least 200 (big_hash) + 2 (small_hash) = 202 hash rows, got {hash_rows}");

    let set_rows = total_rows(by_type.get(&TypeTag::Set).unwrap());
    assert!(set_rows >= 209, "should have at least 200 (big_set) + 6 (int_set) + 3 (small_set) = 209 set rows, got {set_rows}");

    let zset_rows = total_rows(by_type.get(&TypeTag::SortedSet).unwrap());
    assert!(zset_rows >= 200, "should have at least 200 (big_zset) zset rows, got {zset_rows}");

    let list_rows = total_rows(by_type.get(&TypeTag::List).unwrap());
    assert!(list_rows >= 505, "should have at least 500 (big_list) + 5 (small_list) = 505 list rows, got {list_rows}");
}
