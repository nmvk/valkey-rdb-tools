use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use rdb_parser::{RdbEntry, RdbValue};

use crate::detect;

/// Named heuristic detectors for virtual types.
///
/// Heuristics detect virtual types that aren't native RDB types but can be
/// inferred from data patterns (e.g., geo coordinates stored as sorted set
/// scores). Use [`BatcherConfig::heuristics`](crate::BatcherConfig) to control
/// which detectors are active.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Heuristic {
    /// Detect geo data from sorted sets with 52-bit geohash scores.
    Geo,
}

impl Heuristic {
    /// All built-in heuristics.
    pub const ALL: [Heuristic; 1] = [Heuristic::Geo];

    /// Parse a CLI-friendly heuristic name. Case-insensitive.
    pub fn from_name(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("geo") {
            Some(Heuristic::Geo)
        } else {
            None
        }
    }

    /// Lowercase string name of this heuristic (matches the CLI flag value).
    pub fn as_str(self) -> &'static str {
        match self {
            Heuristic::Geo => "geo",
        }
    }

    /// Parse a comma-separated heuristic string into a set.
    ///
    /// Accepts "all" (every built-in heuristic), "none" (empty set), "geo",
    /// or comma-separated combinations.
    pub fn parse_set(s: &str) -> Result<HashSet<Heuristic>, String> {
        if s.eq_ignore_ascii_case("all") {
            return Ok(Heuristic::ALL.iter().copied().collect());
        }
        if s.eq_ignore_ascii_case("none") {
            return Ok(HashSet::new());
        }
        let mut set = HashSet::new();
        for name in s.split(',') {
            let name = name.trim();
            set.insert(Heuristic::from_name(name).ok_or_else(|| {
                format!("unknown heuristic '{name}'. Valid: all, geo, none")
            })?);
        }
        Ok(set)
    }

    /// Serialize a heuristic set into its canonical string form, the
    /// inverse of [`parse_set`](Self::parse_set).
    ///
    /// Sorted lowercase names joined by `","`, or `"none"` for an empty
    /// set. Used as the `rdb.heuristics` Parquet file metadata value so
    /// downstream `validate` can reproduce the same heuristic set that
    /// produced the export.
    pub fn format_set(heuristics: &HashSet<Heuristic>) -> String {
        if heuristics.is_empty() {
            return "none".to_string();
        }
        let mut names: Vec<&'static str> = heuristics.iter().map(|h| h.as_str()).collect();
        names.sort_unstable();
        names.join(",")
    }
}

/// Logical type tag for an RDB entry, used to route entries to the correct schema and builder.
///
/// `Ord` is derived from variant declaration order for deterministic iteration
/// in `BTreeMap`. This ordering is not a semantic contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TypeTag {
    /// Plain string key (RDB type 0 and LZF-compressed variants).
    String,
    /// List of byte strings (quicklist, linkedlist, ziplist, listpack).
    List,
    /// Unordered collection of unique byte strings (hashtable, intset, listpack).
    Set,
    /// Sorted set of (member, score) pairs (skiplist, ziplist, listpack).
    SortedSet,
    /// Field/value map with optional per-field TTL (hashtable, ziplist,
    /// zipmap, listpack).
    Hash,
    /// Virtual type: a sorted set whose scores are all valid 52-bit
    /// geohashes. Emitted only when the Geo heuristic is enabled.
    Geo,
    /// Virtual type: a string whose bytes are a Valkey HyperLogLog
    /// sketch (dense or sparse encoding).
    HyperLogLog,
    /// Module-serialized value (`RDB_TYPE_MODULE_2`).
    Module,
    /// Stream (`RDB_TYPE_STREAM_LISTPACKS*`).
    Stream,
}

impl TypeTag {
    /// All type tags, for iteration.
    pub const ALL: [TypeTag; 9] = [
        TypeTag::String,
        TypeTag::List,
        TypeTag::Set,
        TypeTag::SortedSet,
        TypeTag::Hash,
        TypeTag::Geo,
        TypeTag::HyperLogLog,
        TypeTag::Module,
        TypeTag::Stream,
    ];

    /// Returns the type name as used in the `type` column.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::List => "list",
            Self::Set => "set",
            Self::SortedSet => "zset",
            Self::Hash => "hash",
            Self::Geo => "geo",
            Self::HyperLogLog => "hyperloglog",
            Self::Module => "module",
            Self::Stream => "stream",
        }
    }

    /// Comma-separated list of valid CLI type names.
    pub fn valid_cli_names() -> &'static str {
        "string, list, set, zset, hash, geo, hll, module, stream"
    }

    /// Parse a comma-separated type name string into a set of TypeTags.
    pub fn parse_set(s: &str) -> Result<HashSet<TypeTag>, String> {
        let mut tags = HashSet::new();
        for name in s.split(',') {
            let name = name.trim();
            tags.insert(Self::from_cli_name(name).ok_or_else(|| {
                format!("unknown type '{name}'. Valid: {}", Self::valid_cli_names())
            })?);
        }
        Ok(tags)
    }

    /// Parse a CLI-friendly type name into a TypeTag. Case-insensitive.
    ///
    /// Accepts: "string", "list", "set", "zset", "hash", "geo", "hll", "hyperloglog".
    pub fn from_cli_name(s: &str) -> Option<TypeTag> {
        match s.to_ascii_lowercase().as_str() {
            "string" => Some(TypeTag::String),
            "list" => Some(TypeTag::List),
            "set" => Some(TypeTag::Set),
            "zset" => Some(TypeTag::SortedSet),
            "hash" => Some(TypeTag::Hash),
            "geo" => Some(TypeTag::Geo),
            "hll" | "hyperloglog" => Some(TypeTag::HyperLogLog),
            "module" => Some(TypeTag::Module),
            "stream" => Some(TypeTag::Stream),
            _ => None,
        }
    }
}

impl std::fmt::Display for TypeTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Determines the primary TypeTag for an RDB entry.
///
/// Returns `None` only for RDB value variants that have no Arrow
/// representation. All built-in Valkey object types — including streams
/// and modules — return `Some(_)`.
///
/// Strings with the `HYLL` magic header are tagged as `HyperLogLog`
/// (the raw bytes still parse as `RdbValue::String`; this function
/// disambiguates). Sorted sets always return `SortedSet` — geo
/// detection is handled additively by [`is_geo_entry`] and writes a
/// second batch under the `Geo` tag rather than rerouting the primary
/// tag.
pub fn type_tag_for(entry: &RdbEntry) -> Option<TypeTag> {
    match &entry.value {
        RdbValue::String(data) => {
            if detect::is_hll(data) {
                Some(TypeTag::HyperLogLog)
            } else {
                Some(TypeTag::String)
            }
        }
        RdbValue::List(_) => Some(TypeTag::List),
        RdbValue::Set(_) => Some(TypeTag::Set),
        RdbValue::SortedSet(_) => Some(TypeTag::SortedSet),
        RdbValue::Hash(_) => Some(TypeTag::Hash),
        RdbValue::Module(_) => Some(TypeTag::Module),
        RdbValue::Stream(_) => Some(TypeTag::Stream),
        _ => None,
    }
}

/// The number of Arrow rows an RDB entry will produce.
///
/// Collection types produce one row per element (or one null sentinel for empty).
/// This is the single source of truth — used by both builders and validation.
pub fn expected_row_count(entry: &RdbEntry) -> u64 {
    match &entry.value {
        RdbValue::String(_) => 1,
        RdbValue::List(elems) => elems.len().max(1) as u64,
        RdbValue::Set(members) => members.len().max(1) as u64,
        RdbValue::SortedSet(pairs) => pairs.len().max(1) as u64,
        RdbValue::Hash(fields) => fields.len().max(1) as u64,
        RdbValue::Module(data) => data.values.len().max(1) as u64,
        RdbValue::Stream(data) => {
            let total: usize = data.entries.iter().map(|e| e.fields.len().max(1)).sum();
            total.max(1) as u64
        }
        _ => 1,
    }
}

/// Returns true if a sorted-set entry looks like geo data (all scores
/// are valid 52-bit geohashes).
///
/// Used by the batcher to additively emit geo output alongside the
/// regular zset output.
///
/// Chunked sorted sets are rejected unconditionally (returns `false` when
/// `total_elements.is_some()`). A chunk is only a window into the full
/// key; an early all-geohash chunk followed by a later non-geohash
/// member would otherwise be classified `geo` in the first chunk and
/// quietly excluded in the second, producing a partial geo export that
/// validate would agree with. Require the full membership to be
/// visible before classifying.
pub fn is_geo_entry(entry: &RdbEntry) -> bool {
    if entry.total_elements.is_some() {
        return false;
    }
    match &entry.value {
        RdbValue::SortedSet(members) => detect::is_geo(members),
        _ => false,
    }
}

/// Whether the Geo heuristic applies to an entry that has already been
/// classified with [`type_tag_for`].
///
/// The additive-geo rule ("a sorted set with all 52-bit geohash scores
/// also emits a geo batch") was inlined in both [`crate::ArrowBatcher`]
/// and [`crate::summarize_entries`] before this helper; the triple
/// `heuristics.contains(Geo) && tag == SortedSet && is_geo_entry(entry)`
/// had to stay in sync across the two call sites. Routing both through
/// this function is the single source of truth.
pub fn should_emit_geo(
    heuristics: &HashSet<Heuristic>,
    tag: TypeTag,
    entry: &RdbEntry,
) -> bool {
    heuristics.contains(&Heuristic::Geo)
        && tag == TypeTag::SortedSet
        && is_geo_entry(entry)
}

/// The 8 common prefix columns shared by all type schemas.
pub fn common_fields() -> Vec<Field> {
    vec![
        Field::new("db", DataType::UInt32, false),
        Field::new("key", DataType::Binary, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("expiry_ms", DataType::Int64, true),
        Field::new("lru_idle_secs", DataType::UInt64, true),
        Field::new("lfu_frequency", DataType::UInt8, true),
        Field::new("encoding", DataType::Utf8, false),
        Field::new("num_elements", DataType::UInt64, false),
    ]
}

pub fn string_schema() -> Schema {
    let mut fields = common_fields();
    fields.push(Field::new("value", DataType::Binary, false));
    Schema::new(fields)
}

pub fn list_schema() -> Schema {
    let mut fields = common_fields();
    fields.push(Field::new("index", DataType::UInt64, true));
    fields.push(Field::new("element", DataType::Binary, true));
    Schema::new(fields)
}

pub fn set_schema() -> Schema {
    let mut fields = common_fields();
    fields.push(Field::new("member", DataType::Binary, true));
    Schema::new(fields)
}

pub fn sorted_set_schema() -> Schema {
    let mut fields = common_fields();
    fields.push(Field::new("member", DataType::Binary, true));
    fields.push(Field::new("score", DataType::Float64, true));
    Schema::new(fields)
}

pub fn hash_schema() -> Schema {
    let mut fields = common_fields();
    fields.push(Field::new("field", DataType::Binary, true));
    fields.push(Field::new("field_value", DataType::Binary, true));
    fields.push(Field::new("field_expiry_ms", DataType::Int64, true));
    Schema::new(fields)
}

pub fn geo_schema() -> Schema {
    let mut fields = common_fields();
    fields.push(Field::new("member", DataType::Binary, true));
    fields.push(Field::new("longitude", DataType::Float64, true));
    fields.push(Field::new("latitude", DataType::Float64, true));
    fields.push(Field::new("geohash_score", DataType::Float64, true));
    Schema::new(fields)
}

pub fn hll_schema() -> Schema {
    let mut fields = common_fields();
    fields.push(Field::new("hll_encoding", DataType::Utf8, false));
    fields.push(Field::new("cached_cardinality", DataType::Int64, false));
    fields.push(Field::new("raw_value", DataType::Binary, false));
    Schema::new(fields)
}

pub fn module_schema() -> Schema {
    let mut fields = common_fields();
    fields.push(Field::new("module_name", DataType::Utf8, false));
    fields.push(Field::new("module_version", DataType::UInt32, false));
    fields.push(Field::new("value_index", DataType::UInt32, false));
    fields.push(Field::new("opcode", DataType::Utf8, false));
    // All module integers (both SaveSigned and SaveUnsigned) are stored with
    // RDB_MODULE_OPCODE_UINT in the RDB. The module knows the signedness, not
    // the format. We store the raw u64 bits; consumers reinterpret as needed.
    fields.push(Field::new("int_value", DataType::UInt64, true));
    fields.push(Field::new("double_value", DataType::Float64, true));
    fields.push(Field::new("string_value", DataType::Binary, true));
    Schema::new(fields)
}

pub fn stream_schema() -> Schema {
    let mut fields = common_fields();
    fields.push(Field::new("stream_id", DataType::Utf8, true));
    fields.push(Field::new("field", DataType::Binary, true));
    fields.push(Field::new("field_value", DataType::Binary, true));
    Schema::new(fields)
}

/// Returns the schema for a given TypeTag.
pub fn schema_for(tag: TypeTag) -> Schema {
    match tag {
        TypeTag::String => string_schema(),
        TypeTag::List => list_schema(),
        TypeTag::Set => set_schema(),
        TypeTag::SortedSet => sorted_set_schema(),
        TypeTag::Hash => hash_schema(),
        TypeTag::Geo => geo_schema(),
        TypeTag::HyperLogLog => hll_schema(),
        TypeTag::Module => module_schema(),
        TypeTag::Stream => stream_schema(),
    }
}

/// Cached `Arc<Schema>` for each [`TypeTag`] — built once on first
/// access and shared across every `RecordBatch::try_new` call.
///
/// Builders previously allocated a new `Arc::new(schema::foo_schema())`
/// on every `finish()`, which shows up in profiles for workloads that
/// emit many small batches. Schemas are immutable, so caching is safe.
pub fn schema_arc(tag: TypeTag) -> Arc<Schema> {
    use std::sync::OnceLock;
    // One `Arc<Schema>` per variant, populated lazily and indexed by
    // the tag's position in `TypeTag::ALL`. A stable variant order is
    // already a semantic contract of `TypeTag::ALL`, so this can't
    // silently reshuffle.
    static CACHE: OnceLock<[Arc<Schema>; 9]> = OnceLock::new();
    let schemas = CACHE.get_or_init(|| {
        [
            Arc::new(string_schema()),
            Arc::new(list_schema()),
            Arc::new(set_schema()),
            Arc::new(sorted_set_schema()),
            Arc::new(hash_schema()),
            Arc::new(geo_schema()),
            Arc::new(hll_schema()),
            Arc::new(module_schema()),
            Arc::new(stream_schema()),
        ]
    });
    // Keep the mapping from TypeTag variant → cache index tight against
    // `TypeTag::ALL` so reordering in one place catches in the other.
    let idx = TypeTag::ALL
        .iter()
        .position(|t| *t == tag)
        .expect("TypeTag::ALL must cover every variant");
    schemas[idx].clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn common_fields_count_and_types() {
        let fields = common_fields();
        assert_eq!(fields.len(), 8);
        assert_eq!(fields[0].name(), "db");
        assert_eq!(fields[0].data_type(), &DataType::UInt32);
        assert!(!fields[0].is_nullable());
        assert_eq!(fields[1].name(), "key");
        assert_eq!(fields[1].data_type(), &DataType::Binary);
        assert_eq!(fields[3].name(), "expiry_ms");
        assert!(fields[3].is_nullable());
        assert_eq!(fields[7].name(), "num_elements");
        assert!(!fields[7].is_nullable());
    }

    /// Expected (total_columns, type-specific fields with (name, type, nullable)).
    fn expected_schema(tag: TypeTag) -> (usize, Vec<(&'static str, DataType, bool)>) {
        match tag {
            TypeTag::String => (9, vec![
                ("value", DataType::Binary, false),
            ]),
            TypeTag::List => (10, vec![
                ("index", DataType::UInt64, true),
                ("element", DataType::Binary, true),
            ]),
            TypeTag::Set => (9, vec![
                ("member", DataType::Binary, true),
            ]),
            TypeTag::SortedSet => (10, vec![
                ("member", DataType::Binary, true),
                ("score", DataType::Float64, true),
            ]),
            TypeTag::Hash => (11, vec![
                ("field", DataType::Binary, true),
                ("field_value", DataType::Binary, true),
                ("field_expiry_ms", DataType::Int64, true),
            ]),
            TypeTag::Geo => (12, vec![
                ("member", DataType::Binary, true),
                ("longitude", DataType::Float64, true),
                ("latitude", DataType::Float64, true),
                ("geohash_score", DataType::Float64, true),
            ]),
            TypeTag::HyperLogLog => (11, vec![
                ("hll_encoding", DataType::Utf8, false),
                ("cached_cardinality", DataType::Int64, false),
                ("raw_value", DataType::Binary, false),
            ]),
            TypeTag::Module => (15, vec![
                ("module_name", DataType::Utf8, false),
                ("module_version", DataType::UInt32, false),
                ("value_index", DataType::UInt32, false),
                ("opcode", DataType::Utf8, false),
                ("int_value", DataType::UInt64, true),
                ("double_value", DataType::Float64, true),
                ("string_value", DataType::Binary, true),
            ]),
            TypeTag::Stream => (11, vec![
                ("stream_id", DataType::Utf8, true),
                ("field", DataType::Binary, true),
                ("field_value", DataType::Binary, true),
            ]),
        }
    }

    #[test]
    fn all_schemas_have_common_prefix_and_expected_columns() {
        let common = common_fields();
        for tag in TypeTag::ALL {
            let schema = schema_for(tag);
            let (expected_cols, type_fields) = expected_schema(tag);

            // Column count
            assert_eq!(
                schema.fields().len(), expected_cols,
                "{}: expected {} columns, got {}",
                tag.as_str(), expected_cols, schema.fields().len()
            );

            // Common prefix
            for (i, cf) in common.iter().enumerate() {
                let sf = schema.field(i);
                assert_eq!(sf.name(), cf.name(), "{}: common field {i} name mismatch", tag.as_str());
                assert_eq!(sf.data_type(), cf.data_type(), "{}: common field {i} type mismatch", tag.as_str());
            }

            // Type-specific fields: name, type, nullability
            for (name, expected_type, expected_nullable) in &type_fields {
                let field = schema.field_with_name(name).unwrap_or_else(|_| {
                    panic!("{}: missing type-specific field '{name}'", tag.as_str())
                });
                assert_eq!(
                    field.data_type(), expected_type,
                    "{}: field '{name}' type mismatch",
                    tag.as_str()
                );
                assert_eq!(
                    field.is_nullable(), *expected_nullable,
                    "{}: field '{name}' nullability mismatch",
                    tag.as_str()
                );
            }
        }
    }

    // `schema_for_roundtrip_all_types` was removed: it re-implemented
    // the same match inside `schema_for` and asserted they agreed,
    // which is a tautology after any copy-paste change. The existing
    // `all_schemas_have_common_prefix_and_expected_columns` test
    // already pins the per-tag column shape, which is what callers
    // actually care about.

    #[test]
    fn type_tag_as_str_all() {
        for tag in TypeTag::ALL {
            let s = tag.as_str();
            assert!(!s.is_empty(), "as_str should not be empty");
            assert_eq!(TypeTag::from_cli_name(s), Some(tag), "from_cli_name({s}) roundtrip failed");
        }
    }

    #[test]
    fn type_tag_from_cli_name_aliases() {
        // HLL has an alias
        assert_eq!(TypeTag::from_cli_name("hll"), Some(TypeTag::HyperLogLog));
        assert_eq!(TypeTag::from_cli_name("hyperloglog"), Some(TypeTag::HyperLogLog));
        // Case insensitive
        assert_eq!(TypeTag::from_cli_name("STRING"), Some(TypeTag::String));
        assert_eq!(TypeTag::from_cli_name("Hash"), Some(TypeTag::Hash));
        // Unknown
        assert_eq!(TypeTag::from_cli_name("nope"), None);
    }

    #[test]
    fn type_tag_for_detects_hll() {
        use crate::detect::make_hll;
        use crate::test_helpers::test_entry;
        let entry = test_entry(b"myhll", RdbValue::String(make_hll(0, 0)));
        assert_eq!(type_tag_for(&entry), Some(TypeTag::HyperLogLog));
    }

    #[test]
    fn type_tag_for_regular_string() {
        use crate::test_helpers::test_entry;
        let entry = test_entry(b"mystr", RdbValue::String(b"hello".to_vec()));
        assert_eq!(type_tag_for(&entry), Some(TypeTag::String));
    }

    #[test]
    fn type_tag_for_sorted_set_always_returns_zset() {
        use crate::test_helpers::test_entry_typed;
        // Even with valid geo scores, type_tag_for returns SortedSet (geo is additive)
        let entry = test_entry_typed(
            b"mygeo",
            RdbValue::SortedSet(vec![
                (b"Rome".to_vec(), 3479099956230698.0),
                (b"Paris".to_vec(), 3663941556696959.0),
            ]),
            5, // RDB_TYPE_ZSET_2
        );
        assert_eq!(type_tag_for(&entry), Some(TypeTag::SortedSet));
    }

    #[test]
    fn type_tag_for_regular_sorted_set() {
        use crate::test_helpers::test_entry_typed;
        let entry = test_entry_typed(
            b"myzset",
            RdbValue::SortedSet(vec![
                (b"alice".to_vec(), 1.5),
                (b"bob".to_vec(), 2.7),
            ]),
            5, // RDB_TYPE_ZSET_2
        );
        assert_eq!(type_tag_for(&entry), Some(TypeTag::SortedSet));
    }

    #[test]
    fn is_geo_entry_positive() {
        use crate::test_helpers::test_entry_typed;
        let entry = test_entry_typed(
            b"mygeo",
            RdbValue::SortedSet(vec![
                (b"Rome".to_vec(), 3479099956230698.0),
                (b"Paris".to_vec(), 3663941556696959.0),
            ]),
            5, // RDB_TYPE_ZSET_2
        );
        assert!(is_geo_entry(&entry));
    }

    #[test]
    fn is_geo_entry_negative() {
        use crate::test_helpers::{test_entry, test_entry_typed};
        // Fractional scores are not valid geohashes
        let entry = test_entry_typed(
            b"myzset",
            RdbValue::SortedSet(vec![
                (b"alice".to_vec(), 1.5),
                (b"bob".to_vec(), 2.7),
            ]),
            5, // RDB_TYPE_ZSET_2
        );
        assert!(!is_geo_entry(&entry));
        // Non-sorted-set entry
        let str_entry = test_entry(b"s", RdbValue::String(b"hello".to_vec()));
        assert!(!is_geo_entry(&str_entry));
    }

    #[test]
    fn is_geo_entry_rejects_chunked_zset() {
        // A chunked zset is only a window into the full key. An early
        // all-geohash chunk followed by a later non-geohash member would
        // otherwise be classified `geo` in the first chunk and silently
        // excluded in the second — validate would still agree with the
        // resulting partial geo export. Force the classifier to wait
        // until the complete membership is buffered.
        use crate::test_helpers::test_entry_typed;
        let mut entry = test_entry_typed(
            b"mygeo",
            RdbValue::SortedSet(vec![
                (b"Rome".to_vec(), 3479099956230698.0),
                (b"Paris".to_vec(), 3663941556696959.0),
            ]),
            5, // RDB_TYPE_ZSET_2
        );
        entry.total_elements = Some(100);
        entry.element_offset = Some(0);
        assert!(
            !is_geo_entry(&entry),
            "chunked zset must not be classified as geo even if the chunk is all-geohash"
        );
    }
}
