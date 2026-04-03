use arrow::datatypes::{DataType, Field, Schema};
use rdb_parser::{RdbEntry, RdbValue};

use crate::detect;

/// Logical type tag for an RDB entry, used to route entries to the correct schema and builder.
///
/// Ord is derived from variant declaration order (String < List < ... < HyperLogLog).
/// This gives deterministic iteration in BTreeMap but has no semantic meaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TypeTag {
    String,
    List,
    Set,
    SortedSet,
    Hash,
    Geo,
    HyperLogLog,
}

impl TypeTag {
    /// All type tags, for iteration.
    pub const ALL: [TypeTag; 7] = [
        TypeTag::String,
        TypeTag::List,
        TypeTag::Set,
        TypeTag::SortedSet,
        TypeTag::Hash,
        TypeTag::Geo,
        TypeTag::HyperLogLog,
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
        }
    }

    /// Parse a CLI-friendly type name into a TypeTag. Case-insensitive.
    ///
    /// Accepts: "string", "list", "set", "zset", "hash", "geo", "hll", "hyperloglog".
    pub fn from_cli_name(s: &str) -> Option<TypeTag> {
        if s.eq_ignore_ascii_case("string") {
            Some(TypeTag::String)
        } else if s.eq_ignore_ascii_case("list") {
            Some(TypeTag::List)
        } else if s.eq_ignore_ascii_case("set") {
            Some(TypeTag::Set)
        } else if s.eq_ignore_ascii_case("zset") {
            Some(TypeTag::SortedSet)
        } else if s.eq_ignore_ascii_case("hash") {
            Some(TypeTag::Hash)
        } else if s.eq_ignore_ascii_case("geo") {
            Some(TypeTag::Geo)
        } else if s.eq_ignore_ascii_case("hll") || s.eq_ignore_ascii_case("hyperloglog") {
            Some(TypeTag::HyperLogLog)
        } else {
            None
        }
    }
}

impl std::fmt::Display for TypeTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Determines the TypeTag for an RDB entry. Returns None for unsupported types (stream, module).
///
/// Performs virtual type detection: sorted sets where all scores are valid 52-bit geohashes
/// are tagged as `Geo`, and strings with the `HYLL` magic header are tagged as `HyperLogLog`.
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
        RdbValue::SortedSet(members) => {
            if detect::is_geo(members) {
                Some(TypeTag::Geo)
            } else {
                Some(TypeTag::SortedSet)
            }
        }
        RdbValue::Hash(_) => Some(TypeTag::Hash),
        _ => None,
    }
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
    }
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

    #[test]
    fn string_schema_fields() {
        let s = string_schema();
        assert_eq!(s.fields().len(), 9);
        let value_field = s.field_with_name("value").unwrap();
        assert_eq!(value_field.data_type(), &DataType::Binary);
        assert!(!value_field.is_nullable());
    }

    #[test]
    fn list_schema_fields() {
        let s = list_schema();
        assert_eq!(s.fields().len(), 10);
        let idx = s.field_with_name("index").unwrap();
        assert_eq!(idx.data_type(), &DataType::UInt64);
        assert!(idx.is_nullable());
        let elem = s.field_with_name("element").unwrap();
        assert_eq!(elem.data_type(), &DataType::Binary);
        assert!(elem.is_nullable());
    }

    #[test]
    fn set_schema_fields() {
        let s = set_schema();
        assert_eq!(s.fields().len(), 9);
        let member = s.field_with_name("member").unwrap();
        assert_eq!(member.data_type(), &DataType::Binary);
        assert!(member.is_nullable());
    }

    #[test]
    fn sorted_set_schema_fields() {
        let s = sorted_set_schema();
        assert_eq!(s.fields().len(), 10);
        let member = s.field_with_name("member").unwrap();
        assert_eq!(member.data_type(), &DataType::Binary);
        assert!(member.is_nullable());
        let score = s.field_with_name("score").unwrap();
        assert_eq!(score.data_type(), &DataType::Float64);
        assert!(score.is_nullable());
    }

    #[test]
    fn hash_schema_fields() {
        let s = hash_schema();
        assert_eq!(s.fields().len(), 11);
        let field = s.field_with_name("field").unwrap();
        assert_eq!(field.data_type(), &DataType::Binary);
        assert!(field.is_nullable());
        let fv = s.field_with_name("field_value").unwrap();
        assert_eq!(fv.data_type(), &DataType::Binary);
        assert!(fv.is_nullable());
        let fe = s.field_with_name("field_expiry_ms").unwrap();
        assert_eq!(fe.data_type(), &DataType::Int64);
        assert!(fe.is_nullable());
    }

    #[test]
    fn schema_for_roundtrip() {
        assert_eq!(schema_for(TypeTag::String), string_schema());
        assert_eq!(schema_for(TypeTag::List), list_schema());
        assert_eq!(schema_for(TypeTag::Set), set_schema());
        assert_eq!(schema_for(TypeTag::SortedSet), sorted_set_schema());
        assert_eq!(schema_for(TypeTag::Hash), hash_schema());
        assert_eq!(schema_for(TypeTag::Geo), geo_schema());
        assert_eq!(schema_for(TypeTag::HyperLogLog), hll_schema());
    }

    #[test]
    fn type_tag_as_str() {
        assert_eq!(TypeTag::String.as_str(), "string");
        assert_eq!(TypeTag::List.as_str(), "list");
        assert_eq!(TypeTag::Set.as_str(), "set");
        assert_eq!(TypeTag::SortedSet.as_str(), "zset");
        assert_eq!(TypeTag::Hash.as_str(), "hash");
        assert_eq!(TypeTag::Geo.as_str(), "geo");
        assert_eq!(TypeTag::HyperLogLog.as_str(), "hyperloglog");
    }

    #[test]
    fn geo_schema_fields() {
        let s = geo_schema();
        assert_eq!(s.fields().len(), 12);
        let member = s.field_with_name("member").unwrap();
        assert_eq!(member.data_type(), &DataType::Binary);
        assert!(member.is_nullable());
        let lng = s.field_with_name("longitude").unwrap();
        assert_eq!(lng.data_type(), &DataType::Float64);
        assert!(lng.is_nullable());
        let lat = s.field_with_name("latitude").unwrap();
        assert_eq!(lat.data_type(), &DataType::Float64);
        assert!(lat.is_nullable());
        let score = s.field_with_name("geohash_score").unwrap();
        assert_eq!(score.data_type(), &DataType::Float64);
        assert!(score.is_nullable());
    }

    #[test]
    fn hll_schema_fields() {
        let s = hll_schema();
        assert_eq!(s.fields().len(), 11);
        let enc = s.field_with_name("hll_encoding").unwrap();
        assert_eq!(enc.data_type(), &DataType::Utf8);
        assert!(!enc.is_nullable());
        let card = s.field_with_name("cached_cardinality").unwrap();
        assert_eq!(card.data_type(), &DataType::Int64);
        assert!(!card.is_nullable());
        let raw = s.field_with_name("raw_value").unwrap();
        assert_eq!(raw.data_type(), &DataType::Binary);
        assert!(!raw.is_nullable());
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
    fn type_tag_for_detects_geo() {
        use crate::test_helpers::test_entry_typed;
        let entry = test_entry_typed(
            b"mygeo",
            RdbValue::SortedSet(vec![
                (b"Rome".to_vec(), 3479099956230698.0),
                (b"Paris".to_vec(), 3663941556696959.0),
            ]),
            5, // RDB_TYPE_ZSET_2
        );
        assert_eq!(type_tag_for(&entry), Some(TypeTag::Geo));
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
}
