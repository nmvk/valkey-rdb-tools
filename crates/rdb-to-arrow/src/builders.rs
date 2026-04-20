use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
    UInt32Builder, UInt64Builder, UInt8Builder,
};
use rdb_parser::{ModuleValue, RdbEntry, RdbValue};

use crate::detect;
use crate::error::ArrowConvertError;
use crate::schema;

/// Uniform interface over the nine per-type [`RecordBatch`] builders.
///
/// The batcher holds one builder per [`crate::schema::TypeTag`] and dispatches
/// through this trait so adding a new type only requires implementing it on
/// the new builder — no additional `match` arms in [`crate::batcher`].
pub(crate) trait BatchBuilder {
    /// Append an entry's rows to the builder.
    ///
    /// Entries with a value variant that does not match the builder's type
    /// are silently ignored — routing is the batcher's responsibility.
    fn push(&mut self, entry: &RdbEntry);

    /// Number of rows currently buffered (not yet flushed).
    fn len(&self) -> usize;

    /// Whether any rows have been pushed since the last `finish`.
    fn is_empty(&self) -> bool;

    /// Approximate bytes of variable-length data buffered — used by the
    /// byte-budget flush policy. Does not account for fixed-width columns.
    fn data_bytes(&self) -> usize;

    /// Emit the buffered rows as a `RecordBatch` and reset to empty.
    fn finish(&mut self) -> Result<RecordBatch, ArrowConvertError>;
}

/// Generate `Default` (delegating to `new`) for a builder type. The other
/// common behaviors — `len`, `is_empty`, `data_bytes`, `push`, `finish` —
/// live in the per-builder `impl BatchBuilder` block alongside type-specific
/// state.
macro_rules! batch_builder_default {
    ($ty:ident) => {
        impl Default for $ty {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}

/// Builder for the 8 common prefix columns shared by all type schemas.
///
/// Also owns the row counter and `data_bytes` accumulator for the whole
/// batch — every per-type builder delegates counting here so the
/// accounting can't drift between types (an earlier implementation had
/// each builder tracking its own counters, which led to per-type
/// inconsistencies in the Module builder).
struct CommonColumnsBuilder {
    db: UInt32Builder,
    key: BinaryBuilder,
    type_col: StringBuilder,
    expiry_ms: Int64Builder,
    lru_idle_secs: UInt64Builder,
    lfu_frequency: UInt8Builder,
    encoding: StringBuilder,
    num_elements: UInt64Builder,
    len: usize,
    data_bytes: usize,
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
            len: 0,
            data_bytes: 0,
        }
    }

    /// Append the common columns for one row and update the counters.
    ///
    /// `type_specific_bytes` is the number of variable-length bytes the
    /// caller's type-specific column(s) will contribute for this row. The
    /// key bytes are always counted; the row increment is always 1.
    fn append(
        &mut self,
        entry: &RdbEntry,
        num_elements: u64,
        type_str: &str,
        type_specific_bytes: usize,
    ) {
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
        self.len += 1;
        self.data_bytes += entry.key.len() + type_specific_bytes;
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn data_bytes(&self) -> usize {
        self.data_bytes
    }

    fn finish(&mut self) -> Vec<ArrayRef> {
        let out = vec![
            Arc::new(self.db.finish()) as ArrayRef,
            Arc::new(self.key.finish()),
            Arc::new(self.type_col.finish()),
            Arc::new(self.expiry_ms.finish()),
            Arc::new(self.lru_idle_secs.finish()),
            Arc::new(self.lfu_frequency.finish()),
            Arc::new(self.encoding.finish()),
            Arc::new(self.num_elements.finish()),
        ];
        self.len = 0;
        self.data_bytes = 0;
        out
    }
}

// ---------------------------------------------------------------------------
// String
// ---------------------------------------------------------------------------

pub(crate) struct StringBatchBuilder {
    common: CommonColumnsBuilder,
    value: BinaryBuilder,
}

batch_builder_default!(StringBatchBuilder);

impl StringBatchBuilder {
    pub(crate) fn new() -> Self {
        Self {
            common: CommonColumnsBuilder::new(),
            value: BinaryBuilder::new(),
        }
    }
}

impl BatchBuilder for StringBatchBuilder {
    fn len(&self) -> usize { self.common.len() }
    fn is_empty(&self) -> bool { self.common.is_empty() }
    fn data_bytes(&self) -> usize { self.common.data_bytes() }

    fn push(&mut self, entry: &RdbEntry) {
        if let RdbValue::String(ref v) = entry.value {
            self.common.append(entry, 1, entry.type_name(), v.len());
            self.value.append_value(v);
        }
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowConvertError> {
        let mut columns = self.common.finish();
        columns.push(Arc::new(self.value.finish()));
        Ok(RecordBatch::try_new(schema::schema_arc(schema::TypeTag::String), columns)?)
    }
}

// ---------------------------------------------------------------------------
// List
// ---------------------------------------------------------------------------

pub(crate) struct ListBatchBuilder {
    common: CommonColumnsBuilder,
    index: UInt64Builder,
    element: BinaryBuilder,
}

batch_builder_default!(ListBatchBuilder);

impl ListBatchBuilder {
    pub(crate) fn new() -> Self {
        Self {
            common: CommonColumnsBuilder::new(),
            index: UInt64Builder::new(),
            element: BinaryBuilder::new(),
        }
    }
}

impl BatchBuilder for ListBatchBuilder {
    fn len(&self) -> usize { self.common.len() }
    fn is_empty(&self) -> bool { self.common.is_empty() }
    fn data_bytes(&self) -> usize { self.common.data_bytes() }

    fn push(&mut self, entry: &RdbEntry) {
        if let RdbValue::List(ref elements) = entry.value {
            let num_elements = entry.total_elements.unwrap_or(elements.len() as u64);
            let offset = entry.element_offset.unwrap_or(0);
            if elements.is_empty() {
                self.common.append(entry, 0, entry.type_name(), 0);
                self.index.append_null();
                self.element.append_null();
            } else {
                for (i, elem) in elements.iter().enumerate() {
                    self.common.append(entry, num_elements, entry.type_name(), elem.len());
                    self.index.append_value(offset.saturating_add(i as u64));
                    self.element.append_value(elem);
                }
            }
        }
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowConvertError> {
        let mut columns = self.common.finish();
        columns.push(Arc::new(self.index.finish()));
        columns.push(Arc::new(self.element.finish()));
        Ok(RecordBatch::try_new(schema::schema_arc(schema::TypeTag::List), columns)?)
    }
}

// ---------------------------------------------------------------------------
// Set
// ---------------------------------------------------------------------------

pub(crate) struct SetBatchBuilder {
    common: CommonColumnsBuilder,
    member: BinaryBuilder,
}

batch_builder_default!(SetBatchBuilder);

impl SetBatchBuilder {
    pub(crate) fn new() -> Self {
        Self {
            common: CommonColumnsBuilder::new(),
            member: BinaryBuilder::new(),
        }
    }
}

impl BatchBuilder for SetBatchBuilder {
    fn len(&self) -> usize { self.common.len() }
    fn is_empty(&self) -> bool { self.common.is_empty() }
    fn data_bytes(&self) -> usize { self.common.data_bytes() }

    fn push(&mut self, entry: &RdbEntry) {
        if let RdbValue::Set(ref members) = entry.value {
            let num_elements = entry.total_elements.unwrap_or(members.len() as u64);
            if members.is_empty() {
                self.common.append(entry, 0, entry.type_name(), 0);
                self.member.append_null();
            } else {
                for m in members {
                    self.common.append(entry, num_elements, entry.type_name(), m.len());
                    self.member.append_value(m);
                }
            }
        }
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowConvertError> {
        let mut columns = self.common.finish();
        columns.push(Arc::new(self.member.finish()));
        Ok(RecordBatch::try_new(schema::schema_arc(schema::TypeTag::Set), columns)?)
    }
}

// ---------------------------------------------------------------------------
// SortedSet
// ---------------------------------------------------------------------------

pub(crate) struct SortedSetBatchBuilder {
    common: CommonColumnsBuilder,
    member: BinaryBuilder,
    score: Float64Builder,
}

batch_builder_default!(SortedSetBatchBuilder);

impl SortedSetBatchBuilder {
    pub(crate) fn new() -> Self {
        Self {
            common: CommonColumnsBuilder::new(),
            member: BinaryBuilder::new(),
            score: Float64Builder::new(),
        }
    }
}

impl BatchBuilder for SortedSetBatchBuilder {
    fn len(&self) -> usize { self.common.len() }
    fn is_empty(&self) -> bool { self.common.is_empty() }
    fn data_bytes(&self) -> usize { self.common.data_bytes() }

    fn push(&mut self, entry: &RdbEntry) {
        if let RdbValue::SortedSet(ref pairs) = entry.value {
            let num_elements = entry.total_elements.unwrap_or(pairs.len() as u64);
            if pairs.is_empty() {
                self.common.append(entry, 0, entry.type_name(), 0);
                self.member.append_null();
                self.score.append_null();
            } else {
                for (m, s) in pairs {
                    // +8 accounts for the Float64 score column.
                    self.common.append(entry, num_elements, entry.type_name(), m.len() + 8);
                    self.member.append_value(m);
                    self.score.append_value(*s);
                }
            }
        }
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowConvertError> {
        let mut columns = self.common.finish();
        columns.push(Arc::new(self.member.finish()));
        columns.push(Arc::new(self.score.finish()));
        Ok(RecordBatch::try_new(schema::schema_arc(schema::TypeTag::SortedSet), columns)?)
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
}

batch_builder_default!(HashBatchBuilder);

impl HashBatchBuilder {
    pub(crate) fn new() -> Self {
        Self {
            common: CommonColumnsBuilder::new(),
            field: BinaryBuilder::new(),
            field_value: BinaryBuilder::new(),
            field_expiry_ms: Int64Builder::new(),
        }
    }
}

impl BatchBuilder for HashBatchBuilder {
    fn len(&self) -> usize { self.common.len() }
    fn is_empty(&self) -> bool { self.common.is_empty() }
    fn data_bytes(&self) -> usize { self.common.data_bytes() }

    fn push(&mut self, entry: &RdbEntry) {
        if let RdbValue::Hash(ref fields) = entry.value {
            let num_elements = entry.total_elements.unwrap_or(fields.len() as u64);
            if fields.is_empty() {
                self.common.append(entry, 0, entry.type_name(), 0);
                self.field.append_null();
                self.field_value.append_null();
                self.field_expiry_ms.append_null();
            } else {
                for hf in fields {
                    self.common.append(
                        entry,
                        num_elements,
                        entry.type_name(),
                        hf.field.len() + hf.value.len(),
                    );
                    self.field.append_value(&hf.field);
                    self.field_value.append_value(&hf.value);
                    match hf.expiry_ms {
                        Some(v) => self.field_expiry_ms.append_value(v),
                        None => self.field_expiry_ms.append_null(),
                    }
                }
            }
        }
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowConvertError> {
        let mut columns = self.common.finish();
        columns.push(Arc::new(self.field.finish()));
        columns.push(Arc::new(self.field_value.finish()));
        columns.push(Arc::new(self.field_expiry_ms.finish()));
        Ok(RecordBatch::try_new(schema::schema_arc(schema::TypeTag::Hash), columns)?)
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
}

batch_builder_default!(GeoBatchBuilder);

impl GeoBatchBuilder {
    pub(crate) fn new() -> Self {
        Self {
            common: CommonColumnsBuilder::new(),
            member: BinaryBuilder::new(),
            longitude: Float64Builder::new(),
            latitude: Float64Builder::new(),
            geohash_score: Float64Builder::new(),
        }
    }
}

impl BatchBuilder for GeoBatchBuilder {
    fn len(&self) -> usize { self.common.len() }
    fn is_empty(&self) -> bool { self.common.is_empty() }
    fn data_bytes(&self) -> usize { self.common.data_bytes() }

    fn push(&mut self, entry: &RdbEntry) {
        if let RdbValue::SortedSet(ref pairs) = entry.value {
            let num_elements = entry.total_elements.unwrap_or(pairs.len() as u64);
            if pairs.is_empty() {
                self.common.append(entry, 0, "geo", 0);
                self.member.append_null();
                self.longitude.append_null();
                self.latitude.append_null();
                self.geohash_score.append_null();
            } else {
                for (m, s) in pairs {
                    // +24 accounts for 3 × Float64 columns
                    // (longitude, latitude, geohash_score).
                    self.common.append(entry, num_elements, "geo", m.len() + 24);
                    self.member.append_value(m);
                    if let Some((lng, lat)) = detect::geohash_decode(*s) {
                        self.longitude.append_value(lng);
                        self.latitude.append_value(lat);
                    } else {
                        self.longitude.append_null();
                        self.latitude.append_null();
                    }
                    self.geohash_score.append_value(*s);
                }
            }
        }
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowConvertError> {
        let mut columns = self.common.finish();
        columns.push(Arc::new(self.member.finish()));
        columns.push(Arc::new(self.longitude.finish()));
        columns.push(Arc::new(self.latitude.finish()));
        columns.push(Arc::new(self.geohash_score.finish()));
        Ok(RecordBatch::try_new(schema::schema_arc(schema::TypeTag::Geo), columns)?)
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
}

batch_builder_default!(HllBatchBuilder);

impl HllBatchBuilder {
    pub(crate) fn new() -> Self {
        Self {
            common: CommonColumnsBuilder::new(),
            hll_encoding: StringBuilder::new(),
            cached_cardinality: Int64Builder::new(),
            raw_value: BinaryBuilder::new(),
        }
    }
}

impl BatchBuilder for HllBatchBuilder {
    fn len(&self) -> usize { self.common.len() }
    fn is_empty(&self) -> bool { self.common.is_empty() }
    fn data_bytes(&self) -> usize { self.common.data_bytes() }

    fn push(&mut self, entry: &RdbEntry) {
        if let RdbValue::String(ref data) = entry.value {
            self.common.append(entry, 1, "hyperloglog", data.len());
            self.hll_encoding
                .append_value(detect::hll_encoding(data));
            self.cached_cardinality
                .append_value(detect::hll_cached_cardinality(data));
            self.raw_value.append_value(data);
        }
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowConvertError> {
        let mut columns = self.common.finish();
        columns.push(Arc::new(self.hll_encoding.finish()));
        columns.push(Arc::new(self.cached_cardinality.finish()));
        columns.push(Arc::new(self.raw_value.finish()));
        Ok(RecordBatch::try_new(schema::schema_arc(schema::TypeTag::HyperLogLog), columns)?)
    }
}

// ---------------------------------------------------------------------------
// Stream
// ---------------------------------------------------------------------------

pub(crate) struct StreamBatchBuilder {
    common: CommonColumnsBuilder,
    stream_id: StringBuilder,
    field: BinaryBuilder,
    field_value: BinaryBuilder,
}

batch_builder_default!(StreamBatchBuilder);

impl StreamBatchBuilder {
    pub(crate) fn new() -> Self {
        Self {
            common: CommonColumnsBuilder::new(),
            stream_id: StringBuilder::new(),
            field: BinaryBuilder::new(),
            field_value: BinaryBuilder::new(),
        }
    }
}

impl BatchBuilder for StreamBatchBuilder {
    fn len(&self) -> usize { self.common.len() }
    fn is_empty(&self) -> bool { self.common.is_empty() }
    fn data_bytes(&self) -> usize { self.common.data_bytes() }

    fn push(&mut self, entry: &RdbEntry) {
        if let RdbValue::Stream(ref data) = entry.value {
            // `num_elements` is the logical collection cardinality (matching
            // list len, set cardinality, etc.). For streams that is the number
            // of non-deleted entries — either `data.length` from the RDB
            // metadata or the count of decoded entries as a fallback. It is
            // NOT the exploded field/value row count, which would conflate a
            // stream of 3 entries × 2 fields with a 6-entry stream.
            let num_elements = if data.length > 0 {
                data.length
            } else {
                data.entries.len() as u64
            };

            if data.entries.is_empty() {
                self.common.append(entry, num_elements, "stream", 0);
                self.stream_id.append_null();
                self.field.append_null();
                self.field_value.append_null();
            } else {
                for stream_entry in &data.entries {
                    if stream_entry.fields.is_empty() {
                        // stream_id is a StringBuilder row; count its
                        // bytes toward the flush-size budget.
                        self.common.append(
                            entry,
                            num_elements,
                            "stream",
                            stream_entry.id.len(),
                        );
                        self.stream_id.append_value(&stream_entry.id);
                        self.field.append_null();
                        self.field_value.append_null();
                    } else {
                        for (f, v) in &stream_entry.fields {
                            self.common.append(
                                entry,
                                num_elements,
                                "stream",
                                stream_entry.id.len() + f.len() + v.len(),
                            );
                            self.stream_id.append_value(&stream_entry.id);
                            self.field.append_value(f);
                            self.field_value.append_value(v);
                        }
                    }
                }
            }
        }
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowConvertError> {
        let mut columns: Vec<ArrayRef> = self.common.finish();
        columns.push(Arc::new(self.stream_id.finish()));
        columns.push(Arc::new(self.field.finish()));
        columns.push(Arc::new(self.field_value.finish()));
        Ok(RecordBatch::try_new(schema::schema_arc(schema::TypeTag::Stream), columns)?)
    }
}

// ---------------------------------------------------------------------------
// Module
// ---------------------------------------------------------------------------

pub(crate) struct ModuleBatchBuilder {
    common: CommonColumnsBuilder,
    module_name: StringBuilder,
    module_version: UInt32Builder,
    value_index: UInt32Builder,
    opcode: StringBuilder,
    int_value: UInt64Builder,
    double_value: Float64Builder,
    string_value: BinaryBuilder,
}

batch_builder_default!(ModuleBatchBuilder);

impl ModuleBatchBuilder {
    pub(crate) fn new() -> Self {
        Self {
            common: CommonColumnsBuilder::new(),
            module_name: StringBuilder::new(),
            module_version: UInt32Builder::new(),
            value_index: UInt32Builder::new(),
            opcode: StringBuilder::new(),
            int_value: UInt64Builder::new(),
            double_value: Float64Builder::new(),
            string_value: BinaryBuilder::new(),
        }
    }
}

/// Opcode string emitted for an empty module (no sub-values were
/// decoded, so the builder writes one sentinel row).
const EMPTY_MODULE_OPCODE: &str = "eof";

/// The `opcode` column value for a given [`ModuleValue`] variant.
///
/// Single source of truth — both the push loop (which appends to the
/// Arrow builder) and [`module_opcode_bytes`] (which feeds the byte
/// budget) call this helper so they cannot drift out of sync.
fn module_opcode(val: &ModuleValue) -> &'static str {
    match val {
        ModuleValue::SignedInt(_) => "sint",
        ModuleValue::UnsignedInt(_) => "uint",
        ModuleValue::Float(_) => "float",
        ModuleValue::Double(_) => "double",
        ModuleValue::String(_) => "string",
        // Future ModuleValue variants round-trip as the `unknown` opcode
        // with all value columns null.
        _ => "unknown",
    }
}

/// Byte width of the variant-dependent value column (`int_value`,
/// `double_value`, or `string_value`).
///
/// `Float` and `Double` both report 8 bytes because the column is
/// `Float64` — the narrower 32-bit width is upcast on write.
fn module_value_bytes(val: &ModuleValue) -> usize {
    match val {
        ModuleValue::SignedInt(_) | ModuleValue::UnsignedInt(_) => 8,
        ModuleValue::Float(_) | ModuleValue::Double(_) => 8,
        ModuleValue::String(v) => v.len(),
        _ => 0,
    }
}

/// Byte width of the opcode string a given `ModuleValue` variant emits.
fn module_opcode_bytes(val: &ModuleValue) -> usize {
    module_opcode(val).len()
}

/// Total variable-length bytes a module row contributes across its
/// type-specific columns. Includes the per-row `module_name` string, the
/// fixed-width `module_version` (4B) and `value_index` (4B) integers,
/// the variant-specific opcode string, and the variant-specific value
/// column. Without this the byte-budget flusher under-counts module
/// batches and can let a single builder accumulate well past
/// `batch_bytes` before triggering a flush.
fn module_row_bytes(data: &rdb_parser::ModuleData, val: &ModuleValue) -> usize {
    data.module_name.len()
        + 4 // module_version (UInt32)
        + 4 // value_index (UInt32)
        + module_opcode_bytes(val)
        + module_value_bytes(val)
}

/// Row-byte contribution for the empty-module sentinel row.
fn empty_module_row_bytes(data: &rdb_parser::ModuleData) -> usize {
    data.module_name.len()
        + 4 // module_version (UInt32)
        + 4 // value_index (UInt32)
        + EMPTY_MODULE_OPCODE.len()
}

impl BatchBuilder for ModuleBatchBuilder {
    fn len(&self) -> usize { self.common.len() }
    fn is_empty(&self) -> bool { self.common.is_empty() }
    fn data_bytes(&self) -> usize { self.common.data_bytes() }

    fn push(&mut self, entry: &RdbEntry) {
        if let RdbValue::Module(ref data) = entry.value {
            let num_elements = entry.total_elements.unwrap_or(data.values.len() as u64);
            if data.values.is_empty() {
                // Empty-module sentinel row: name + version + index +
                // `EMPTY_MODULE_OPCODE`. No value column bytes contribute.
                self.common
                    .append(entry, 0, "module", empty_module_row_bytes(data));
                self.module_name.append_value(&data.module_name);
                self.module_version.append_value(data.module_version);
                self.value_index.append_value(0);
                self.opcode.append_value(EMPTY_MODULE_OPCODE);
                self.int_value.append_null();
                self.double_value.append_null();
                self.string_value.append_null();
            } else {
                for (i, val) in data.values.iter().enumerate() {
                    self.common.append(
                        entry,
                        num_elements,
                        "module",
                        module_row_bytes(data, val),
                    );
                    self.module_name.append_value(&data.module_name);
                    self.module_version.append_value(data.module_version);
                    self.value_index.append_value(i as u32);
                    // Opcode name comes from the single source of truth —
                    // keeping `module_opcode_bytes` and the Arrow builder
                    // pinned to the same string per variant.
                    self.opcode.append_value(module_opcode(val));
                    // Both SINT and UINT map to int_value (UInt64). In
                    // practice Valkey writes both via RDB_MODULE_OPCODE_UINT;
                    // the module knows the signedness, not the format.
                    match val {
                        ModuleValue::SignedInt(v) => {
                            self.int_value.append_value(*v as u64);
                            self.double_value.append_null();
                            self.string_value.append_null();
                        }
                        ModuleValue::UnsignedInt(v) => {
                            self.int_value.append_value(*v);
                            self.double_value.append_null();
                            self.string_value.append_null();
                        }
                        ModuleValue::Float(v) => {
                            self.int_value.append_null();
                            self.double_value.append_value(*v as f64);
                            self.string_value.append_null();
                        }
                        ModuleValue::Double(v) => {
                            self.int_value.append_null();
                            self.double_value.append_value(*v);
                            self.string_value.append_null();
                        }
                        ModuleValue::String(v) => {
                            self.int_value.append_null();
                            self.double_value.append_null();
                            self.string_value.append_value(v);
                        }
                        _ => {
                            // Future variants: all value columns null;
                            // opcode already written as "unknown".
                            self.int_value.append_null();
                            self.double_value.append_null();
                            self.string_value.append_null();
                        }
                    }
                }
            }
        }
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowConvertError> {
        let mut columns: Vec<ArrayRef> = self.common.finish();
        columns.push(Arc::new(self.module_name.finish()));
        columns.push(Arc::new(self.module_version.finish()));
        columns.push(Arc::new(self.value_index.finish()));
        columns.push(Arc::new(self.opcode.finish()));
        columns.push(Arc::new(self.int_value.finish()));
        columns.push(Arc::new(self.double_value.finish()));
        columns.push(Arc::new(self.string_value.finish()));
        Ok(RecordBatch::try_new(schema::schema_arc(schema::TypeTag::Module), columns)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{Float64Type, Int64Type, UInt32Type, UInt64Type, UInt8Type};
    use rdb_parser::HashField;
    // Bring the trait into scope so `.push`, `.finish`, `.len`, etc. resolve
    // on the concrete builder types used in the tests below.
    use super::BatchBuilder;

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

    #[test]
    fn test_list_builder_with_element_offset() {
        let mut b = ListBatchBuilder::new();
        let mut entry = make_entry(RdbValue::List(vec![
            b"x".to_vec(),
            b"y".to_vec(),
            b"z".to_vec(),
        ]));
        entry.total_elements = Some(300);
        entry.element_offset = Some(100);
        b.push(&entry);
        assert_eq!(b.len(), 3);
        let batch = b.finish().unwrap();

        let indices = batch.column(8).as_primitive::<UInt64Type>();
        assert_eq!(indices.value(0), 100);
        assert_eq!(indices.value(1), 101);
        assert_eq!(indices.value(2), 102);

        let num_elem = batch.column(7).as_primitive::<UInt64Type>();
        assert_eq!(num_elem.value(0), 300);
    }

    #[test]
    fn test_num_elements_from_total_elements() {
        let mut b = HashBatchBuilder::new();
        let mut entry = make_entry(RdbValue::Hash(vec![
            HashField { field: b"f1".to_vec(), value: b"v1".to_vec(), expiry_ms: None },
            HashField { field: b"f2".to_vec(), value: b"v2".to_vec(), expiry_ms: None },
            HashField { field: b"f3".to_vec(), value: b"v3".to_vec(), expiry_ms: None },
        ]));
        entry.total_elements = Some(1000);
        b.push(&entry);
        let batch = b.finish().unwrap();

        // num_elements column should be 1000, not 3
        let num_elem = batch.column(7).as_primitive::<UInt64Type>();
        assert_eq!(num_elem.value(0), 1000);
        assert_eq!(num_elem.value(1), 1000);
        assert_eq!(num_elem.value(2), 1000);
    }

    #[test]
    fn module_builder_mixed_values() {
        use rdb_parser::{ModuleData, ModuleValue};

        let mut b = ModuleBatchBuilder::new();
        b.push(&make_entry(RdbValue::Module(ModuleData {
            module_name: "testmod".to_string(),
            module_version: 2,
            values: vec![
                ModuleValue::UnsignedInt(42),
                ModuleValue::SignedInt(-7),
                ModuleValue::Double(1.234),
                ModuleValue::String(b"hello".to_vec()),
            ],
        })));
        assert_eq!(b.len(), 4);
        let batch = b.finish().unwrap();
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.num_columns(), 15);

        // module_name column (index 8)
        let names = batch.column(8).as_string::<i32>();
        assert_eq!(names.value(0), "testmod");

        // opcode column (index 11)
        let opcodes = batch.column(11).as_string::<i32>();
        assert_eq!(opcodes.value(0), "uint");
        assert_eq!(opcodes.value(1), "sint");
        assert_eq!(opcodes.value(2), "double");
        assert_eq!(opcodes.value(3), "string");

        // int_value column (index 12, UInt64) — both uint and sint produce values
        let ints = batch.column(12).as_primitive::<UInt64Type>();
        assert_eq!(ints.value(0), 42);
        // -7i64 reinterpreted as u64
        assert_eq!(ints.value(1), (-7i64) as u64);
        assert!(ints.is_null(2));
        assert!(ints.is_null(3));

        // string_value column (index 14) — only string row has value
        let strings = batch.column(14).as_binary::<i32>();
        assert!(strings.is_null(0));
        assert!(strings.is_null(1));
        assert!(strings.is_null(2));
        assert_eq!(strings.value(3), b"hello");
    }

    #[test]
    fn module_builder_empty_module() {
        use rdb_parser::ModuleData;

        let mut b = ModuleBatchBuilder::new();
        b.push(&make_entry(RdbValue::Module(ModuleData {
            module_name: "empty".to_string(),
            module_version: 0,
            values: vec![],
        })));
        assert_eq!(b.len(), 1);
        let batch = b.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let opcodes = batch.column(11).as_string::<i32>();
        assert_eq!(opcodes.value(0), "eof");
    }

    #[test]
    fn stream_builder_num_elements_is_entry_count_not_exploded_rows() {
        use rdb_parser::{StreamData, StreamEntry};

        // 3 entries with 2 fields each → 6 exploded rows. The common
        // `num_elements` column must reflect the logical stream length (3),
        // not the exploded row count (6), so a stream of {3 entries × 2
        // fields} cannot be confused with a 6-entry stream.
        let data = StreamData {
            entries: vec![
                StreamEntry {
                    id: "1-0".into(),
                    fields: vec![(b"f1".to_vec(), b"v1".to_vec()), (b"f2".to_vec(), b"v2".to_vec())],
                },
                StreamEntry {
                    id: "2-0".into(),
                    fields: vec![(b"f1".to_vec(), b"v1".to_vec()), (b"f2".to_vec(), b"v2".to_vec())],
                },
                StreamEntry {
                    id: "3-0".into(),
                    fields: vec![(b"f1".to_vec(), b"v1".to_vec()), (b"f2".to_vec(), b"v2".to_vec())],
                },
            ],
            length: 3,
            last_id: "3-0".into(),
        };

        let mut b = StreamBatchBuilder::new();
        b.push(&make_entry(RdbValue::Stream(data)));
        let batch = b.finish().unwrap();
        assert_eq!(batch.num_rows(), 6, "6 exploded rows (3 entries × 2 fields)");

        let num_elem = batch.column(7).as_primitive::<UInt64Type>();
        for row in 0..6 {
            assert_eq!(
                num_elem.value(row),
                3,
                "num_elements at row {row} should be stream entry count, not exploded row count"
            );
        }
    }

    #[test]
    fn stream_builder_empty_stream_num_elements_is_zero() {
        use rdb_parser::StreamData;

        let mut b = StreamBatchBuilder::new();
        b.push(&make_entry(RdbValue::Stream(StreamData {
            entries: vec![],
            length: 0,
            last_id: "0-0".into(),
        })));
        let batch = b.finish().unwrap();
        assert_eq!(batch.num_rows(), 1, "empty stream still emits one sentinel row");
        let num_elem = batch.column(7).as_primitive::<UInt64Type>();
        assert_eq!(num_elem.value(0), 0);
    }

    /// Pin the invariant that `module_opcode` (used by the byte counter)
    /// and the actual opcode written into the Arrow builder agree for
    /// every `ModuleValue` variant. A drift between the two is the same
    /// bug class that originally motivated the Module row-bytes fix.
    #[test]
    fn module_opcode_string_appears_in_output() {
        use rdb_parser::{ModuleData, ModuleValue};

        let values = vec![
            ModuleValue::SignedInt(0),
            ModuleValue::UnsignedInt(0),
            ModuleValue::Float(0.0),
            ModuleValue::Double(0.0),
            ModuleValue::String(b"x".to_vec()),
        ];
        let mut b = ModuleBatchBuilder::new();
        b.push(&make_entry(RdbValue::Module(ModuleData {
            module_name: "probe".into(),
            module_version: 1,
            values: values.clone(),
        })));
        let batch = b.finish().unwrap();
        let opcodes = batch.column(11).as_string::<i32>();

        for (i, val) in values.iter().enumerate() {
            assert_eq!(
                opcodes.value(i),
                module_opcode(val),
                "opcode column row {i} disagrees with module_opcode() for {val:?}"
            );
        }
    }
}
