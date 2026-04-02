use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
    UInt32Builder, UInt64Builder, UInt8Builder,
};
use rdb_parser::{RdbEntry, RdbValue};

use crate::detect;
use crate::error::ArrowConvertError;
use crate::schema;

/// Builder for the 8 common prefix columns shared by all type schemas.
struct CommonColumnsBuilder {
    db: UInt32Builder,
    key: BinaryBuilder,
    type_col: StringBuilder,
    expiry_ms: Int64Builder,
    lru_idle_secs: UInt64Builder,
    lfu_frequency: UInt8Builder,
    encoding: StringBuilder,
    num_elements: UInt64Builder,
}

impl CommonColumnsBuilder {
    fn new() -> Self {
        Self {
            db: UInt32Builder::new(),
            key: BinaryBuilder::new(),
            type_col: StringBuilder::new(),
            expiry_ms: Int64Builder::new(),
            lru_idle_secs: UInt64Builder::new(),
            lfu_frequency: UInt8Builder::new(),
            encoding: StringBuilder::new(),
            num_elements: UInt64Builder::new(),
        }
    }

    fn append(&mut self, entry: &RdbEntry, num_elements: u64, type_str: &str) {
        self.db.append_value(entry.db);
        self.key.append_value(&entry.key);
        self.type_col.append_value(type_str);
        match entry.expiry_ms {
            Some(v) => self.expiry_ms.append_value(v),
            None => self.expiry_ms.append_null(),
        }
        match entry.lru_idle_secs {
            Some(v) => self.lru_idle_secs.append_value(v),
            None => self.lru_idle_secs.append_null(),
        }
        match entry.lfu_frequency {
            Some(v) => self.lfu_frequency.append_value(v),
            None => self.lfu_frequency.append_null(),
        }
        self.encoding.append_value(entry.encoding_name());
        self.num_elements.append_value(num_elements);
    }

    fn finish(&mut self) -> Vec<ArrayRef> {
        vec![
            Arc::new(self.db.finish()),
            Arc::new(self.key.finish()),
            Arc::new(self.type_col.finish()),
            Arc::new(self.expiry_ms.finish()),
            Arc::new(self.lru_idle_secs.finish()),
            Arc::new(self.lfu_frequency.finish()),
            Arc::new(self.encoding.finish()),
            Arc::new(self.num_elements.finish()),
        ]
    }
}

// ---------------------------------------------------------------------------
// String
// ---------------------------------------------------------------------------

pub(crate) struct StringBatchBuilder {
    common: CommonColumnsBuilder,
    value: BinaryBuilder,
    len: usize,
}

impl Default for StringBatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl StringBatchBuilder {
    pub(crate) fn new() -> Self {
        Self {
            common: CommonColumnsBuilder::new(),
            value: BinaryBuilder::new(),
            len: 0,
        }
    }

    pub(crate) fn push(&mut self, entry: &RdbEntry) {
        if let RdbValue::String(ref v) = entry.value {
            self.common.append(entry, 1, entry.type_name());
            self.value.append_value(v);
            self.len += 1;
        }
    }

    pub(crate) fn finish(&mut self) -> Result<RecordBatch, ArrowConvertError> {
        let mut columns = self.common.finish();
        columns.push(Arc::new(self.value.finish()));
        self.len = 0;
        Ok(RecordBatch::try_new(Arc::new(schema::string_schema()), columns)?)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }
}

// ---------------------------------------------------------------------------
// List
// ---------------------------------------------------------------------------

pub(crate) struct ListBatchBuilder {
    common: CommonColumnsBuilder,
    index: UInt64Builder,
    element: BinaryBuilder,
    len: usize,
}

impl Default for ListBatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ListBatchBuilder {
    pub(crate) fn new() -> Self {
        Self {
            common: CommonColumnsBuilder::new(),
            index: UInt64Builder::new(),
            element: BinaryBuilder::new(),
            len: 0,
        }
    }

    pub(crate) fn push(&mut self, entry: &RdbEntry) {
        if let RdbValue::List(ref elements) = entry.value {
            let num_elements = elements.len() as u64;
            if elements.is_empty() {
                // Emit one row with nulls so the key isn't dropped.
                self.common.append(entry, 0, entry.type_name());
                self.index.append_null();
                self.element.append_null();
                self.len += 1;
            } else {
                for (i, elem) in elements.iter().enumerate() {
                    self.common.append(entry, num_elements, entry.type_name());
                    self.index.append_value(i as u64);
                    self.element.append_value(elem);
                    self.len += 1;
                }
            }
        }
    }

    pub(crate) fn finish(&mut self) -> Result<RecordBatch, ArrowConvertError> {
        let mut columns = self.common.finish();
        columns.push(Arc::new(self.index.finish()));
        columns.push(Arc::new(self.element.finish()));
        self.len = 0;
        Ok(RecordBatch::try_new(Arc::new(schema::list_schema()), columns)?)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }
}

// ---------------------------------------------------------------------------
// Set
// ---------------------------------------------------------------------------

pub(crate) struct SetBatchBuilder {
    common: CommonColumnsBuilder,
    member: BinaryBuilder,
    len: usize,
}

impl Default for SetBatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SetBatchBuilder {
    pub(crate) fn new() -> Self {
        Self {
            common: CommonColumnsBuilder::new(),
            member: BinaryBuilder::new(),
            len: 0,
        }
    }

    pub(crate) fn push(&mut self, entry: &RdbEntry) {
        if let RdbValue::Set(ref members) = entry.value {
            let num_elements = members.len() as u64;
            if members.is_empty() {
                self.common.append(entry, 0, entry.type_name());
                self.member.append_null();
                self.len += 1;
            } else {
                for m in members {
                    self.common.append(entry, num_elements, entry.type_name());
                    self.member.append_value(m);
                    self.len += 1;
                }
            }
        }
    }

    pub(crate) fn finish(&mut self) -> Result<RecordBatch, ArrowConvertError> {
        let mut columns = self.common.finish();
        columns.push(Arc::new(self.member.finish()));
        self.len = 0;
        Ok(RecordBatch::try_new(Arc::new(schema::set_schema()), columns)?)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }
}

// ---------------------------------------------------------------------------
// SortedSet
// ---------------------------------------------------------------------------

pub(crate) struct SortedSetBatchBuilder {
    common: CommonColumnsBuilder,
    member: BinaryBuilder,
    score: Float64Builder,
    len: usize,
}

impl Default for SortedSetBatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SortedSetBatchBuilder {
    pub(crate) fn new() -> Self {
        Self {
            common: CommonColumnsBuilder::new(),
            member: BinaryBuilder::new(),
            score: Float64Builder::new(),
            len: 0,
        }
    }

    pub(crate) fn push(&mut self, entry: &RdbEntry) {
        if let RdbValue::SortedSet(ref pairs) = entry.value {
            let num_elements = pairs.len() as u64;
            if pairs.is_empty() {
                self.common.append(entry, 0, entry.type_name());
                self.member.append_null();
                self.score.append_null();
                self.len += 1;
            } else {
                for (m, s) in pairs {
                    self.common.append(entry, num_elements, entry.type_name());
                    self.member.append_value(m);
                    self.score.append_value(*s);
                    self.len += 1;
                }
            }
        }
    }

    pub(crate) fn finish(&mut self) -> Result<RecordBatch, ArrowConvertError> {
        let mut columns = self.common.finish();
        columns.push(Arc::new(self.member.finish()));
        columns.push(Arc::new(self.score.finish()));
        self.len = 0;
        Ok(RecordBatch::try_new(Arc::new(schema::sorted_set_schema()), columns)?)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }
}

// ---------------------------------------------------------------------------
// Hash
// ---------------------------------------------------------------------------

pub(crate) struct HashBatchBuilder {
    common: CommonColumnsBuilder,
    field: BinaryBuilder,
    field_value: BinaryBuilder,
    field_expiry_ms: Int64Builder,
    len: usize,
}

impl Default for HashBatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl HashBatchBuilder {
    pub(crate) fn new() -> Self {
        Self {
            common: CommonColumnsBuilder::new(),
            field: BinaryBuilder::new(),
            field_value: BinaryBuilder::new(),
            field_expiry_ms: Int64Builder::new(),
            len: 0,
        }
    }

    pub(crate) fn push(&mut self, entry: &RdbEntry) {
        if let RdbValue::Hash(ref fields) = entry.value {
            let num_elements = fields.len() as u64;
            if fields.is_empty() {
                self.common.append(entry, 0, entry.type_name());
                self.field.append_null();
                self.field_value.append_null();
                self.field_expiry_ms.append_null();
                self.len += 1;
            } else {
                for hf in fields {
                    self.common.append(entry, num_elements, entry.type_name());
                    self.field.append_value(&hf.field);
                    self.field_value.append_value(&hf.value);
                    match hf.expiry_ms {
                        Some(v) => self.field_expiry_ms.append_value(v),
                        None => self.field_expiry_ms.append_null(),
                    }
                    self.len += 1;
                }
            }
        }
    }

    pub(crate) fn finish(&mut self) -> Result<RecordBatch, ArrowConvertError> {
        let mut columns = self.common.finish();
        columns.push(Arc::new(self.field.finish()));
        columns.push(Arc::new(self.field_value.finish()));
        columns.push(Arc::new(self.field_expiry_ms.finish()));
        self.len = 0;
        Ok(RecordBatch::try_new(Arc::new(schema::hash_schema()), columns)?)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }
}

// ---------------------------------------------------------------------------
// Geo
// ---------------------------------------------------------------------------

pub(crate) struct GeoBatchBuilder {
    common: CommonColumnsBuilder,
    member: BinaryBuilder,
    longitude: Float64Builder,
    latitude: Float64Builder,
    geohash_score: Float64Builder,
    len: usize,
}

impl Default for GeoBatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl GeoBatchBuilder {
    pub(crate) fn new() -> Self {
        Self {
            common: CommonColumnsBuilder::new(),
            member: BinaryBuilder::new(),
            longitude: Float64Builder::new(),
            latitude: Float64Builder::new(),
            geohash_score: Float64Builder::new(),
            len: 0,
        }
    }

    pub(crate) fn push(&mut self, entry: &RdbEntry) {
        if let RdbValue::SortedSet(ref pairs) = entry.value {
            let num_elements = pairs.len() as u64;
            if pairs.is_empty() {
                self.common.append(entry, 0, "geo");
                self.member.append_null();
                self.longitude.append_null();
                self.latitude.append_null();
                self.geohash_score.append_null();
                self.len += 1;
            } else {
                for (m, s) in pairs {
                    self.common.append(entry, num_elements, "geo");
                    self.member.append_value(m);
                    if let Some((lng, lat)) = detect::geohash_decode(*s) {
                        self.longitude.append_value(lng);
                        self.latitude.append_value(lat);
                    } else {
                        self.longitude.append_null();
                        self.latitude.append_null();
                    }
                    self.geohash_score.append_value(*s);
                    self.len += 1;
                }
            }
        }
    }

    pub(crate) fn finish(&mut self) -> Result<RecordBatch, ArrowConvertError> {
        let mut columns = self.common.finish();
        columns.push(Arc::new(self.member.finish()));
        columns.push(Arc::new(self.longitude.finish()));
        columns.push(Arc::new(self.latitude.finish()));
        columns.push(Arc::new(self.geohash_score.finish()));
        self.len = 0;
        Ok(RecordBatch::try_new(Arc::new(schema::geo_schema()), columns)?)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }
}

// ---------------------------------------------------------------------------
// HyperLogLog
// ---------------------------------------------------------------------------

pub(crate) struct HllBatchBuilder {
    common: CommonColumnsBuilder,
    hll_encoding: StringBuilder,
    cached_cardinality: Int64Builder,
    raw_value: BinaryBuilder,
    len: usize,
}

impl Default for HllBatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl HllBatchBuilder {
    pub(crate) fn new() -> Self {
        Self {
            common: CommonColumnsBuilder::new(),
            hll_encoding: StringBuilder::new(),
            cached_cardinality: Int64Builder::new(),
            raw_value: BinaryBuilder::new(),
            len: 0,
        }
    }

    pub(crate) fn push(&mut self, entry: &RdbEntry) {
        if let RdbValue::String(ref data) = entry.value {
            self.common.append(entry, 1, "hyperloglog");
            self.hll_encoding
                .append_value(detect::hll_encoding(data));
            self.cached_cardinality
                .append_value(detect::hll_cached_cardinality(data));
            self.raw_value.append_value(data);
            self.len += 1;
        }
    }

    pub(crate) fn finish(&mut self) -> Result<RecordBatch, ArrowConvertError> {
        let mut columns = self.common.finish();
        columns.push(Arc::new(self.hll_encoding.finish()));
        columns.push(Arc::new(self.cached_cardinality.finish()));
        columns.push(Arc::new(self.raw_value.finish()));
        self.len = 0;
        Ok(RecordBatch::try_new(Arc::new(schema::hll_schema()), columns)?)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{Float64Type, Int64Type, UInt32Type, UInt64Type, UInt8Type};
    use rdb_parser::HashField;

    /// Entry with expiry + lfu set, for testing common column propagation.
    fn make_entry(value: RdbValue) -> RdbEntry {
        let mut e = crate::test_helpers::test_entry(b"mykey", value);
        e.expiry_ms = Some(1700000000000);
        e.lfu_frequency = Some(42);
        e
    }

    #[test]
    fn string_builder_one_row() {
        let mut b = StringBatchBuilder::new();
        b.push(&make_entry(RdbValue::String(b"hello".to_vec())));
        assert_eq!(b.len(), 1);
        let batch = b.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 9);

        // Check common columns
        let db = batch.column(0).as_primitive::<UInt32Type>();
        assert_eq!(db.value(0), 0);
        let key = batch.column(1).as_binary::<i32>();
        assert_eq!(key.value(0), b"mykey");
        let type_col = batch.column(2).as_string::<i32>();
        assert_eq!(type_col.value(0), "string");
        let expiry = batch.column(3).as_primitive::<Int64Type>();
        assert_eq!(expiry.value(0), 1700000000000);
        let lru = batch.column(4).as_primitive::<UInt64Type>();
        assert!(lru.is_null(0));
        let lfu = batch.column(5).as_primitive::<UInt8Type>();
        assert_eq!(lfu.value(0), 42);
        let num_elem = batch.column(7).as_primitive::<UInt64Type>();
        assert_eq!(num_elem.value(0), 1);

        // Type-specific
        let value = batch.column(8).as_binary::<i32>();
        assert_eq!(value.value(0), b"hello");
    }

    #[test]
    fn list_builder_three_elements() {
        let mut b = ListBatchBuilder::new();
        let entry = make_entry(RdbValue::List(vec![
            b"a".to_vec(),
            b"b".to_vec(),
            b"c".to_vec(),
        ]));
        b.push(&entry);
        assert_eq!(b.len(), 3);
        let batch = b.finish().unwrap();
        assert_eq!(batch.num_rows(), 3);

        let indices = batch.column(8).as_primitive::<UInt64Type>();
        assert_eq!(indices.value(0), 0);
        assert_eq!(indices.value(1), 1);
        assert_eq!(indices.value(2), 2);

        let elements = batch.column(9).as_binary::<i32>();
        assert_eq!(elements.value(0), b"a");
        assert_eq!(elements.value(1), b"b");
        assert_eq!(elements.value(2), b"c");

        // num_elements should be 3 for all rows
        let num_elem = batch.column(7).as_primitive::<UInt64Type>();
        assert_eq!(num_elem.value(0), 3);
        assert_eq!(num_elem.value(2), 3);
    }

    #[test]
    fn empty_set_emits_one_null_row() {
        let mut b = SetBatchBuilder::new();
        b.push(&make_entry(RdbValue::Set(vec![])));
        assert_eq!(b.len(), 1);
        let batch = b.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let member = batch.column(8).as_binary::<i32>();
        assert!(member.is_null(0));

        let num_elem = batch.column(7).as_primitive::<UInt64Type>();
        assert_eq!(num_elem.value(0), 0);
    }

    #[test]
    fn sorted_set_builder() {
        let mut b = SortedSetBatchBuilder::new();
        b.push(&make_entry(RdbValue::SortedSet(vec![
            (b"alice".to_vec(), 1.5),
            (b"bob".to_vec(), 2.7),
        ])));
        assert_eq!(b.len(), 2);
        let batch = b.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let members = batch.column(8).as_binary::<i32>();
        assert_eq!(members.value(0), b"alice");
        assert_eq!(members.value(1), b"bob");

        let scores = batch.column(9).as_primitive::<Float64Type>();
        assert!((scores.value(0) - 1.5).abs() < f64::EPSILON);
        assert!((scores.value(1) - 2.7).abs() < f64::EPSILON);
    }

    #[test]
    fn hash_builder_with_field_ttl() {
        let mut b = HashBatchBuilder::new();
        b.push(&make_entry(RdbValue::Hash(vec![
            HashField {
                field: b"name".to_vec(),
                value: b"valkey".to_vec(),
                expiry_ms: None,
            },
            HashField {
                field: b"session".to_vec(),
                value: b"abc123".to_vec(),
                expiry_ms: Some(9999999999999),
            },
        ])));
        assert_eq!(b.len(), 2);
        let batch = b.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let fields = batch.column(8).as_binary::<i32>();
        assert_eq!(fields.value(0), b"name");
        assert_eq!(fields.value(1), b"session");

        let fv = batch.column(9).as_binary::<i32>();
        assert_eq!(fv.value(0), b"valkey");

        let fe = batch.column(10).as_primitive::<Int64Type>();
        assert!(fe.is_null(0));
        assert_eq!(fe.value(1), 9999999999999);
    }

    #[test]
    fn finish_resets_builder() {
        let mut b = StringBatchBuilder::new();
        b.push(&make_entry(RdbValue::String(b"a".to_vec())));
        let batch1 = b.finish().unwrap();
        assert_eq!(batch1.num_rows(), 1);
        assert!(b.is_empty());

        b.push(&make_entry(RdbValue::String(b"b".to_vec())));
        b.push(&make_entry(RdbValue::String(b"c".to_vec())));
        let batch2 = b.finish().unwrap();
        assert_eq!(batch2.num_rows(), 2);
    }

    use crate::detect::{encode_geohash, make_hll};

    #[test]
    fn geo_builder_decodes_coords() {
        let rome_score = encode_geohash(12.4923, 41.8902);
        let sf_score = encode_geohash(-122.4194, 37.7749);
        let mut b = GeoBatchBuilder::new();
        b.push(&make_entry(RdbValue::SortedSet(vec![
            (b"Rome".to_vec(), rome_score),
            (b"SF".to_vec(), sf_score),
        ])));
        assert_eq!(b.len(), 2);
        let batch = b.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 12);

        // type column should say "geo"
        let type_col = batch.column(2).as_string::<i32>();
        assert_eq!(type_col.value(0), "geo");

        let members = batch.column(8).as_binary::<i32>();
        assert_eq!(members.value(0), b"Rome");
        assert_eq!(members.value(1), b"SF");

        let lngs = batch.column(9).as_primitive::<Float64Type>();
        let lats = batch.column(10).as_primitive::<Float64Type>();
        assert!((lats.value(0) - 41.8902).abs() < 0.001);
        assert!((lngs.value(0) - 12.4923).abs() < 0.001);
        assert!((lats.value(1) - 37.7749).abs() < 0.001);
        assert!((lngs.value(1) - (-122.4194)).abs() < 0.001);

        let scores = batch.column(11).as_primitive::<Float64Type>();
        assert!((scores.value(0) - rome_score).abs() < f64::EPSILON);
    }

    #[test]
    fn geo_builder_empty_set_null_row() {
        let mut b = GeoBatchBuilder::new();
        b.push(&make_entry(RdbValue::SortedSet(vec![])));
        assert_eq!(b.len(), 1);
        let batch = b.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let member = batch.column(8).as_binary::<i32>();
        assert!(member.is_null(0));
        let lng = batch.column(9).as_primitive::<Float64Type>();
        assert!(lng.is_null(0));
        let lat = batch.column(10).as_primitive::<Float64Type>();
        assert!(lat.is_null(0));
        let score = batch.column(11).as_primitive::<Float64Type>();
        assert!(score.is_null(0));
    }

    #[test]
    fn hll_builder_extracts_header() {
        let hll_data = make_hll(1, 42);
        let mut b = HllBatchBuilder::new();
        b.push(&make_entry(RdbValue::String(hll_data.clone())));
        assert_eq!(b.len(), 1);
        let batch = b.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 11);

        // type column should say "hyperloglog"
        let type_col = batch.column(2).as_string::<i32>();
        assert_eq!(type_col.value(0), "hyperloglog");

        let enc = batch.column(8).as_string::<i32>();
        assert_eq!(enc.value(0), "sparse");

        let card = batch.column(9).as_primitive::<Int64Type>();
        assert_eq!(card.value(0), 42);

        let raw = batch.column(10).as_binary::<i32>();
        assert_eq!(raw.value(0), &hll_data[..]);
    }

    #[test]
    fn hll_builder_dense_encoding() {
        let hll_data = make_hll(0, 999);
        let mut b = HllBatchBuilder::new();
        b.push(&make_entry(RdbValue::String(hll_data)));
        let batch = b.finish().unwrap();

        let enc = batch.column(8).as_string::<i32>();
        assert_eq!(enc.value(0), "dense");
        let card = batch.column(9).as_primitive::<Int64Type>();
        assert_eq!(card.value(0), 999);
    }
}
