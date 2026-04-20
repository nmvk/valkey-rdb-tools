// RDB entry types — the typed structs yielded by the parser

use std::collections::BTreeMap;
use std::fmt;

/// RDB magic string type — REDIS (legacy) or VALKEY (9.0+).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RdbMagic {
    /// Legacy Redis magic; RDB versions 1–11.
    Redis,
    /// Valkey magic (file starts with `VALKEY`); RDB version 80.
    Valkey,
}

impl fmt::Display for RdbMagic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RdbMagic::Redis => write!(f, "REDIS"),
            RdbMagic::Valkey => write!(f, "VALKEY"),
        }
    }
}

/// Parsed RDB file header.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RdbHeader {
    /// Which magic string the file starts with.
    pub magic: RdbMagic,
    /// RDB format version (1–11 for Redis, 80 for Valkey 9.0+).
    pub version: u32,
}

/// File-level metadata extracted from AUX fields at the start of the RDB.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct RdbMetadata {
    /// All AUX key/value pairs encountered, in insertion order (by BTreeMap
    /// key ordering). Non-UTF-8 AUX keys are rejected as `CorruptData`;
    /// values with non-UTF-8 bytes are passed through lossy conversion.
    pub aux: BTreeMap<String, String>,
}

impl RdbMetadata {
    /// Server version string from the `valkey-ver` or `redis-ver` AUX field.
    pub fn server_version(&self) -> Option<&str> {
        self.aux
            .get("valkey-ver")
            .or_else(|| self.aux.get("redis-ver"))
            .map(|s| s.as_str())
    }

    /// Unix timestamp (seconds) when the RDB was written, from the `ctime`
    /// AUX field. `None` if the field is missing or not parseable as `u64`.
    pub fn ctime(&self) -> Option<u64> {
        self.aux.get("ctime").and_then(|s| s.parse().ok())
    }

    /// Server memory use (bytes) at the time of snapshot, from `used-mem`.
    pub fn used_mem(&self) -> Option<u64> {
        self.aux.get("used-mem").and_then(|s| s.parse().ok())
    }

    /// Replication ID, from the `repl-id` AUX field.
    pub fn repl_id(&self) -> Option<&str> {
        self.aux.get("repl-id").map(|s| s.as_str())
    }

    /// Replication offset, from the `repl-offset` AUX field.
    pub fn repl_offset(&self) -> Option<i64> {
        self.aux.get("repl-offset").and_then(|s| s.parse().ok())
    }
}

/// A single hash field with optional per-field TTL (Valkey 9.0 HASH_2).
#[derive(Debug, Clone, PartialEq)]
pub struct HashField {
    /// Field name (arbitrary bytes).
    pub field: Vec<u8>,
    /// Field value (arbitrary bytes).
    pub value: Vec<u8>,
    /// Per-field expiration in Unix milliseconds, if set. `None` means the
    /// field follows the key's TTL, not its own.
    pub expiry_ms: Option<i64>,
}

/// A single typed value from a module's RDB serialization stream.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ModuleValue {
    /// Signed 64-bit integer.
    SignedInt(i64),
    /// Unsigned 64-bit integer.
    UnsignedInt(u64),
    /// 32-bit IEEE 754 float.
    Float(f32),
    /// 64-bit IEEE 754 double.
    Double(f64),
    /// Arbitrary byte string.
    String(Vec<u8>),
}

/// Module data extracted from `RDB_TYPE_MODULE_2` entries.
#[derive(Debug, Clone, PartialEq)]
pub struct ModuleData {
    /// Decoded module name (9-char base-64 encoded identifier).
    pub module_name: String,
    /// Module encoding version (low 10 bits of the module ID).
    pub module_version: u32,
    /// Typed values emitted by the module's RDB serializer, in order.
    pub values: Vec<ModuleValue>,
}

/// A single stream entry (one XADD).
#[derive(Debug, Clone, PartialEq)]
pub struct StreamEntry {
    /// Stream ID in "ms-seq" format (e.g., "1700000000000-0").
    pub id: String,
    /// Field-value pairs for this entry.
    pub fields: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Parsed stream data from `RDB_TYPE_STREAM_LISTPACKS*` entries.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamData {
    /// Non-deleted stream entries.
    pub entries: Vec<StreamEntry>,
    /// Total length as reported by the RDB metadata (non-deleted entries).
    pub length: u64,
    /// Last entry ID.
    pub last_id: String,
}

/// The value portion of an RDB key-value entry.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum RdbValue {
    /// Simple string or integer-encoded value.
    String(Vec<u8>),

    /// Ordered list of elements.
    List(Vec<Vec<u8>>),

    /// Unordered set of members.
    Set(Vec<Vec<u8>>),

    /// Sorted set: (member, score) pairs.
    SortedSet(Vec<(Vec<u8>, f64)>),

    /// Hash: field-value pairs with optional per-field TTL.
    Hash(Vec<HashField>),

    /// Module data: typed value stream from any module type.
    Module(ModuleData),

    /// Stream data: entries with IDs and field-value pairs.
    Stream(StreamData),
}

/// A single parsed RDB key-value entry with metadata.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RdbEntry {
    /// Database number (from SELECTDB opcode).
    pub db: u32,

    /// Key bytes. Usually UTF-8 but can be arbitrary bytes.
    pub key: Vec<u8>,

    /// The parsed value.
    pub value: RdbValue,

    /// RDB type code that was used to encode this entry.
    pub type_code: u8,

    /// Key expiry as Unix epoch milliseconds, None if no TTL.
    pub expiry_ms: Option<i64>,

    /// LRU idle time in seconds (from IDLE opcode).
    pub lru_idle_secs: Option<u64>,

    /// LFU frequency counter 0-255 (from FREQ opcode).
    pub lfu_frequency: Option<u8>,

    /// Total element count across all chunks. `None` = complete (non-chunked) entry.
    /// `Some(N)` = this entry is one chunk of an N-element collection.
    pub total_elements: Option<u64>,

    /// Offset of the first element in this chunk within the full collection.
    /// Used by list builders for correct index computation.
    /// `None` when not chunked or not applicable.
    pub element_offset: Option<u64>,
}

impl RdbEntry {
    /// Create a new entry with the given key, value, and type code. All
    /// other fields default to their zero/`None` values; use the `with_*`
    /// methods to attach metadata.
    ///
    /// `RdbEntry` is `#[non_exhaustive]`, so external code must go through
    /// this constructor (or a crate that yields `RdbEntry`) rather than
    /// struct-literal syntax.
    pub fn new(key: Vec<u8>, value: RdbValue, type_code: u8) -> Self {
        Self {
            db: 0,
            key,
            value,
            type_code,
            expiry_ms: None,
            lru_idle_secs: None,
            lfu_frequency: None,
            total_elements: None,
            element_offset: None,
        }
    }

    /// Set the database number (SELECTDB).
    #[must_use]
    pub fn with_db(mut self, db: u32) -> Self {
        self.db = db;
        self
    }

    /// Set the expiry in Unix milliseconds. Pass `None` to clear.
    #[must_use]
    pub fn with_expiry_ms(mut self, expiry_ms: Option<i64>) -> Self {
        self.expiry_ms = expiry_ms;
        self
    }

    /// Set the LRU idle time in seconds (IDLE opcode).
    #[must_use]
    pub fn with_lru_idle_secs(mut self, lru_idle_secs: Option<u64>) -> Self {
        self.lru_idle_secs = lru_idle_secs;
        self
    }

    /// Set the LFU frequency counter (FREQ opcode, 0–255).
    #[must_use]
    pub fn with_lfu_frequency(mut self, lfu_frequency: Option<u8>) -> Self {
        self.lfu_frequency = lfu_frequency;
        self
    }

    /// Mark this entry as one chunk of a larger collection: `total` is the
    /// full element count across all chunks, `offset` is the index of the
    /// first element in this chunk.
    #[must_use]
    pub fn with_chunking(mut self, total: Option<u64>, offset: Option<u64>) -> Self {
        self.total_elements = total;
        self.element_offset = offset;
        self
    }

    /// Returns true if this is the first (or only) chunk of a key.
    /// For non-chunked entries this always returns true.
    pub fn is_first_chunk(&self) -> bool {
        self.element_offset.is_none() || self.element_offset == Some(0)
    }

    /// Returns the logical type name (string, list, set, zset, hash, stream, module).
    pub fn type_name(&self) -> &'static str {
        crate::opcodes::type_name(self.type_code)
    }

    /// Returns the encoding name (listpack, ziplist, hashtable, etc).
    pub fn encoding_name(&self) -> &'static str {
        crate::opcodes::encoding_name(self.type_code)
    }
}

/// Errors returned by the RDB parser.
#[derive(Debug)]
#[non_exhaustive]
pub enum RdbError {
    /// I/O error from the underlying reader.
    Io(std::io::Error),

    /// Invalid magic string (not REDIS0 or VALKEY).
    InvalidMagic,

    /// RDB version not supported.
    UnsupportedVersion(u32),

    /// Corrupt data encountered during parsing.
    CorruptData(String),

    /// Unknown or unsupported RDB type code.
    UnknownType(u8),
}

impl From<std::io::Error> for RdbError {
    fn from(e: std::io::Error) -> Self {
        RdbError::Io(e)
    }
}

impl fmt::Display for RdbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RdbError::Io(e) => write!(f, "I/O error: {}", e),
            RdbError::InvalidMagic => write!(f, "invalid RDB magic string"),
            RdbError::UnsupportedVersion(v) => write!(f, "unsupported RDB version: {}", v),
            RdbError::CorruptData(msg) => write!(f, "corrupt RDB data: {}", msg),
            RdbError::UnknownType(t) => write!(f, "unknown RDB type code: {}", t),
        }
    }
}

impl std::error::Error for RdbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RdbError::Io(e) => Some(e),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_metadata(pairs: &[(&str, &str)]) -> RdbMetadata {
        let mut m = RdbMetadata::default();
        for (k, v) in pairs {
            m.aux.insert(k.to_string(), v.to_string());
        }
        m
    }

    #[test]
    fn test_server_version_valkey() {
        let m = make_metadata(&[("valkey-ver", "9.0.1")]);
        assert_eq!(m.server_version(), Some("9.0.1"));
    }

    #[test]
    fn test_server_version_redis_fallback() {
        let m = make_metadata(&[("redis-ver", "7.2.4")]);
        assert_eq!(m.server_version(), Some("7.2.4"));
    }

    #[test]
    fn test_server_version_valkey_takes_precedence() {
        let m = make_metadata(&[("valkey-ver", "9.0.0"), ("redis-ver", "7.2.4")]);
        assert_eq!(m.server_version(), Some("9.0.0"));
    }

    #[test]
    fn test_server_version_missing() {
        let m = RdbMetadata::default();
        assert_eq!(m.server_version(), None);
    }

    #[test]
    fn test_ctime() {
        let m = make_metadata(&[("ctime", "1700000000")]);
        assert_eq!(m.ctime(), Some(1_700_000_000));
    }

    #[test]
    fn test_ctime_invalid() {
        let m = make_metadata(&[("ctime", "not_a_number")]);
        assert_eq!(m.ctime(), None);
    }

    #[test]
    fn test_used_mem() {
        let m = make_metadata(&[("used-mem", "1073741824")]);
        assert_eq!(m.used_mem(), Some(1_073_741_824));
    }

    #[test]
    fn test_repl_id() {
        let m = make_metadata(&[("repl-id", "abc123def456")]);
        assert_eq!(m.repl_id(), Some("abc123def456"));
    }

    #[test]
    fn test_repl_offset() {
        let m = make_metadata(&[("repl-offset", "-1")]);
        assert_eq!(m.repl_offset(), Some(-1));
    }

    #[test]
    fn test_repl_offset_missing() {
        let m = RdbMetadata::default();
        assert_eq!(m.repl_offset(), None);
    }
}
