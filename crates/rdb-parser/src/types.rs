// RDB entry types — the typed structs yielded by the parser

use std::collections::HashMap;
use std::fmt;

/// RDB magic string type — REDIS (legacy) or VALKEY (9.0+).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RdbMagic {
    Redis,
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
pub struct RdbHeader {
    pub magic: RdbMagic,
    pub version: u32,
}

/// File-level metadata extracted from AUX fields.
#[derive(Debug, Clone, Default)]
pub struct RdbMetadata {
    pub aux: HashMap<String, String>,
}

impl RdbMetadata {
    pub fn server_version(&self) -> Option<&str> {
        self.aux
            .get("valkey-ver")
            .or_else(|| self.aux.get("redis-ver"))
            .map(|s| s.as_str())
    }

    pub fn ctime(&self) -> Option<u64> {
        self.aux.get("ctime").and_then(|s| s.parse().ok())
    }

    pub fn used_mem(&self) -> Option<u64> {
        self.aux.get("used-mem").and_then(|s| s.parse().ok())
    }

    pub fn repl_id(&self) -> Option<&str> {
        self.aux.get("repl-id").map(|s| s.as_str())
    }

    pub fn repl_offset(&self) -> Option<i64> {
        self.aux.get("repl-offset").and_then(|s| s.parse().ok())
    }
}

/// A single hash field with optional per-field TTL (Valkey 9.0 HASH_2).
#[derive(Debug, Clone, PartialEq)]
pub struct HashField {
    pub field: Vec<u8>,
    pub value: Vec<u8>,
    pub expiry_ms: Option<i64>,
}

/// The value portion of an RDB key-value entry.
#[derive(Debug, Clone, PartialEq)]
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

    // TODO: Stream(StreamData) — to be added when stream parsing is implemented.
    // TODO: Module(Vec<u8>) — to be added when module parsing is implemented.
}

/// A single parsed RDB key-value entry with metadata.
#[derive(Debug, Clone)]
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
}

impl RdbEntry {
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

impl std::error::Error for RdbError {}
