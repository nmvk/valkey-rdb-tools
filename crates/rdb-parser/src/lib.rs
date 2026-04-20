//! Streaming parser for Valkey and Redis RDB dump files.
//!
//! The crate exposes two main types:
//! - [`RdbReader`] — an iterator that yields one [`RdbEntry`] per key in
//!   the file, handling chunking for very large collections.
//! - [`RdbValue`] — the decoded value of an entry (string, list, set,
//!   sorted set, hash, stream, module, …).
//!
//! The parser defends against malformed and adversarial input: it caps
//! per-allocation sizes ([`CAPACITY_HINT_MAX`]), uses checked arithmetic
//! on length fields from the wire, and returns
//! [`RdbError::CorruptData`](types::RdbError) rather than panicking on
//! bad input.

#![warn(missing_docs)]

pub(crate) mod compact;
pub(crate) mod crc64;
pub(crate) mod intset;
pub(crate) mod listpack;
pub(crate) mod lzf;
/// RDB wire-format constants: opcode bytes, type codes, and helpers
/// for classifying them (`is_object_type`, `type_name`, `encoding_name`).
/// Exposed for downstream tools that need to construct or inspect raw
/// RDB byte sequences (e.g. test fixture generators).
pub mod opcodes;
/// Streaming RDB reader — iterate key/value entries from a `Read` source.
pub mod reader;
pub(crate) mod stream;
/// Typed data structures yielded by the reader: entries, values, metadata.
pub mod types;
pub(crate) mod ziplist;

pub use reader::{RdbReader, DEFAULT_MAX_KEY_ELEMENTS};
pub use types::{HashField, ModuleData, ModuleValue, RdbEntry, RdbError, RdbHeader, RdbMagic, RdbMetadata, RdbValue, StreamData, StreamEntry};

/// Cap for `Vec::with_capacity` on element counts decoded from untrusted RDB
/// data. The loop still reads exactly `count` elements regardless of this
/// cap — it only limits the initial allocation so a crafted count field
/// cannot force a multi-GB reservation.
pub(crate) const CAPACITY_HINT_MAX: usize = 65_536;
