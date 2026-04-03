// RDB file reader — iterates over entries in an RDB file
//
// Reference: valkey/src/rdb.c (rdbLoadRioWithLoadingCtx, rdbLoadLen, rdbLoadStringObject)

use std::io::{BufReader, Read};

use crate::crc64;
use crate::opcodes::*;
use crate::types::*;

// Module serialized value sub-opcodes (from rdb.h)
const RDB_MODULE_OPCODE_EOF: u64 = 0;
const RDB_MODULE_OPCODE_SINT: u64 = 1;
const RDB_MODULE_OPCODE_UINT: u64 = 2;
const RDB_MODULE_OPCODE_FLOAT: u64 = 3;
const RDB_MODULE_OPCODE_DOUBLE: u64 = 4;
const RDB_MODULE_OPCODE_STRING: u64 = 5;

// REDIS magic: native versions 1-11, foreign 12-79 (rejected in strict mode)
const RDB_VERSION_MAX_REDIS_NATIVE: u32 = 11;
// VALKEY magic: version 80 only
const RDB_VERSION_VALKEY: u32 = 80;

/// Maximum single allocation from untrusted length fields.
/// Protects against corrupt or crafted RDB files that claim enormous lengths.
/// 512 MB is well above the largest legitimate RDB string (Valkey's max
/// value size is 512 MB) while still preventing multi-GB allocations from
/// crashing the process.
const MAX_ALLOC_BYTES: u64 = 512 * 1024 * 1024;

/// Result of reading a length-encoded value: either a plain length or a
/// special encoding indicator.
enum LenResult {
    /// A plain byte length.
    Len(u64),
    /// A special encoding sub-type (INT8, INT16, INT32, LZF).
    Special(u8),
}

/// A Read wrapper that accumulates CRC-64 over all bytes read.
struct CrcReader<R: Read> {
    inner: BufReader<R>,
    crc: u64,
}

impl<R: Read> CrcReader<R> {
    fn new(inner: BufReader<R>, initial_crc: u64) -> Self {
        Self {
            inner,
            crc: initial_crc,
        }
    }
}

impl<R: Read> Read for CrcReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.crc = crc64::crc64(self.crc, &buf[..n]);
        Ok(n)
    }
}

/// The main RDB reader. Wraps any `Read` source and yields `RdbEntry` items.
pub struct RdbReader<R: Read> {
    reader: CrcReader<R>,
    header: RdbHeader,
    metadata: RdbMetadata,

    // State carried between calls to next()
    current_db: u32,
    pending_expiry_ms: Option<i64>,
    pending_lru_idle: Option<u64>,
    pending_lfu_freq: Option<u8>,
    /// First non-AUX byte consumed by read_preamble(), replayed by next_inner().
    preamble_byte: Option<u8>,
    finished: bool,
}

impl<R: Read> RdbReader<R> {
    /// Create a new RDB reader. Reads and validates the header, then eagerly
    /// consumes all AUX fields up to the first database section or EOF.
    /// After `new()` returns, [`metadata()`](Self::metadata) is fully populated.
    pub fn new(reader: R) -> Result<Self, RdbError> {
        let mut reader = CrcReader::new(BufReader::new(reader), 0);
        let header = read_header(&mut reader)?;
        let mut rdr = Self {
            reader,
            header,
            metadata: RdbMetadata::default(),
            current_db: 0,
            pending_expiry_ms: None,
            pending_lru_idle: None,
            pending_lfu_freq: None,
            preamble_byte: None,
            finished: false,
        };
        rdr.read_preamble()?;
        Ok(rdr)
    }

    /// The parsed RDB header.
    pub fn header(&self) -> &RdbHeader {
        &self.header
    }

    /// File-level metadata from AUX fields.
    pub fn metadata(&self) -> &RdbMetadata {
        &self.metadata
    }

    // --- Private helpers ---

    fn read_u8(&mut self) -> Result<u8, RdbError> {
        let mut buf = [0u8; 1];
        self.reader.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn read_i32_le(&mut self) -> Result<i32, RdbError> {
        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf)?;
        Ok(i32::from_le_bytes(buf))
    }

    fn read_i64_le(&mut self) -> Result<i64, RdbError> {
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf)?;
        Ok(i64::from_le_bytes(buf))
    }

    fn read_exact_vec(&mut self, len: usize) -> Result<Vec<u8>, RdbError> {
        let mut buf = vec![0u8; len];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Safely convert a u64 length to usize, rejecting values that would
    /// overflow on 32-bit targets or exceed [`MAX_ALLOC_BYTES`].
    fn len_to_usize(len: u64, ctx: &str) -> Result<usize, RdbError> {
        if len > MAX_ALLOC_BYTES {
            return Err(RdbError::CorruptData(format!(
                "{}: length {} exceeds {} byte limit",
                ctx, len, MAX_ALLOC_BYTES
            )));
        }
        Ok(len as usize)
    }

    /// Read a length-encoded value. Returns either a plain length or a special
    /// encoding indicator.
    ///
    /// Reference: rdbLoadLenByRef() in valkey/src/rdb.c
    fn read_length(&mut self) -> Result<LenResult, RdbError> {
        let first = self.read_u8()?;
        let top2 = (first & 0xC0) >> 6;

        match top2 {
            RDB_6BITLEN => {
                // 00xxxxxx: 6-bit length in remaining bits
                Ok(LenResult::Len((first & 0x3F) as u64))
            }
            RDB_14BITLEN => {
                // 01xxxxxx: 14-bit length across 2 bytes (big-endian)
                let second = self.read_u8()?;
                let len = ((first & 0x3F) as u64) << 8 | second as u64;
                Ok(LenResult::Len(len))
            }
            2 => {
                // 10xxxxxx: check the full first byte
                if first == RDB_32BITLEN {
                    // Next 4 bytes, big-endian
                    let mut buf = [0u8; 4];
                    self.reader.read_exact(&mut buf)?;
                    Ok(LenResult::Len(u32::from_be_bytes(buf) as u64))
                } else if first == RDB_64BITLEN {
                    // Next 8 bytes, big-endian
                    let mut buf = [0u8; 8];
                    self.reader.read_exact(&mut buf)?;
                    Ok(LenResult::Len(u64::from_be_bytes(buf)))
                } else {
                    Err(RdbError::CorruptData(format!(
                        "unknown length encoding byte: 0x{:02x}",
                        first
                    )))
                }
            }
            RDB_ENCVAL => {
                // 11xxxxxx: special encoding, sub-type in remaining 6 bits
                Ok(LenResult::Special(first & 0x3F))
            }
            _ => unreachable!(),
        }
    }

    /// Read a plain length (not a special encoding). Returns an error if
    /// the length encoding indicates a special type.
    fn read_length_value(&mut self) -> Result<u64, RdbError> {
        match self.read_length()? {
            LenResult::Len(len) => Ok(len),
            LenResult::Special(_) => Err(RdbError::CorruptData(
                "expected plain length, got special encoding".into(),
            )),
        }
    }

    /// Read a string (the most common RDB primitive).
    ///
    /// Handles: raw bytes, integer-as-string (INT8/INT16/INT32), LZF compressed.
    /// Reference: rdbGenericLoadStringObject() in valkey/src/rdb.c
    fn read_string(&mut self) -> Result<Vec<u8>, RdbError> {
        match self.read_length()? {
            LenResult::Len(len) => {
                // Plain length-prefixed bytes
                let len = Self::len_to_usize(len, "string")?;
                self.read_exact_vec(len)
            }
            LenResult::Special(enc_type) => match enc_type {
                RDB_ENC_INT8 => {
                    let v = self.read_u8()? as i8;
                    Ok(v.to_string().into_bytes())
                }
                RDB_ENC_INT16 => {
                    let mut buf = [0u8; 2];
                    self.reader.read_exact(&mut buf)?;
                    let v = i16::from_le_bytes(buf);
                    Ok(v.to_string().into_bytes())
                }
                RDB_ENC_INT32 => {
                    let mut buf = [0u8; 4];
                    self.reader.read_exact(&mut buf)?;
                    let v = i32::from_le_bytes(buf);
                    Ok(v.to_string().into_bytes())
                }
                RDB_ENC_LZF => {
                    let compressed_len = Self::len_to_usize(
                        self.read_length_value()?,
                        "LZF compressed length",
                    )?;
                    let uncompressed_len = Self::len_to_usize(
                        self.read_length_value()?,
                        "LZF uncompressed length",
                    )?;
                    let compressed = self.read_exact_vec(compressed_len)?;
                    let decompressed = lzf_decompress(&compressed, uncompressed_len)?;
                    Ok(decompressed)
                }
                _ => Err(RdbError::CorruptData(format!(
                    "unknown string encoding sub-type: {}",
                    enc_type
                ))),
            },
        }
    }

    /// Read a double in rdbLoadDoubleValue format (ZSET v1 scores).
    /// Format: 1 byte length, then special (253=NaN, 254=+inf, 255=-inf)
    /// or `len` bytes of ASCII double string.
    fn read_double_value(&mut self) -> Result<f64, RdbError> {
        let len = self.read_u8()?;
        match len {
            255 => Ok(f64::NEG_INFINITY),
            254 => Ok(f64::INFINITY),
            253 => Ok(f64::NAN),
            n => {
                let buf = self.read_exact_vec(n as usize)?;
                let s = std::str::from_utf8(&buf).map_err(|_| {
                    RdbError::CorruptData("invalid UTF-8 in double value".into())
                })?;
                s.parse::<f64>().map_err(|_| {
                    RdbError::CorruptData(format!("invalid double string: {}", s))
                })
            }
        }
    }

    /// Skip a module value by consuming its opcode stream until EOF marker.
    /// Reference: rdbLoadCheckModuleValue() in valkey/src/rdb.c
    fn skip_module_data(&mut self) -> Result<(), RdbError> {
        loop {
            let opcode = self.read_length_value()?;
            if opcode == RDB_MODULE_OPCODE_EOF {
                return Ok(());
            }
            match opcode {
                RDB_MODULE_OPCODE_SINT | RDB_MODULE_OPCODE_UINT => {
                    let _val = self.read_length_value()?;
                }
                RDB_MODULE_OPCODE_STRING => {
                    let _s = self.read_string()?;
                }
                RDB_MODULE_OPCODE_FLOAT => {
                    let _f = self.read_exact_vec(4)?;
                }
                RDB_MODULE_OPCODE_DOUBLE => {
                    let _d = self.read_exact_vec(8)?;
                }
                _ => {
                    return Err(RdbError::CorruptData(format!(
                        "unknown module sub-opcode: {}",
                        opcode
                    )));
                }
            }
        }
    }

    /// Eagerly consume AUX fields that precede the first database section.
    /// Stores the first non-AUX byte in `preamble_byte` for replay.
    /// Read one AUX field (key + value strings) and insert into metadata.
    fn read_aux_field(&mut self) -> Result<(), RdbError> {
        let key = self.read_string()?;
        let value = self.read_string()?;
        let key_str = String::from_utf8(key)
            .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned());
        let val_str = String::from_utf8(value)
            .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned());
        self.metadata.aux.insert(key_str, val_str);
        Ok(())
    }

    fn read_preamble(&mut self) -> Result<(), RdbError> {
        loop {
            let byte = self.read_u8()?;
            if byte == RDB_OPCODE_AUX {
                self.read_aux_field()?;
            } else {
                self.preamble_byte = Some(byte);
                return Ok(());
            }
        }
    }

    /// Read a single key-value entry given the type byte.
    fn read_entry(&mut self, type_code: u8) -> Result<RdbEntry, RdbError> {
        // Take pending state upfront so it is consumed regardless of whether
        // read_value succeeds or returns Err(UnknownType) for skipped types.
        let expiry_ms = self.pending_expiry_ms.take();
        let lru_idle_secs = self.pending_lru_idle.take();
        let lfu_frequency = self.pending_lfu_freq.take();

        let key = self.read_string()?;
        let value = self.read_value(type_code)?;

        Ok(RdbEntry {
            db: self.current_db,
            key,
            value,
            type_code,
            expiry_ms,
            lru_idle_secs,
            lfu_frequency,
        })
    }

    /// Read the value for a given type code.
    /// All standard types are decoded except HASH_ZIPMAP (type 9), streams
    /// (types 15, 19, 21), and modules (types 6, 7) which return UnknownType.
    fn read_value(&mut self, type_code: u8) -> Result<RdbValue, RdbError> {
        match type_code {
            RDB_TYPE_STRING => {
                let data = self.read_string()?;
                Ok(RdbValue::String(data))
            }

            // --- Listpack-encoded types ---
            RDB_TYPE_SET_LISTPACK => {
                let blob = self.read_string()?;
                let entries = crate::listpack::decode(&blob)?;
                let members = entries.into_iter().map(|e| e.into_bytes()).collect();
                Ok(RdbValue::Set(members))
            }

            RDB_TYPE_HASH_LISTPACK => {
                let blob = self.read_string()?;
                let entries = crate::listpack::decode(&blob)?;
                Ok(RdbValue::Hash(decode_hash_pairs(entries, "hash listpack")?))
            }

            RDB_TYPE_ZSET_LISTPACK => {
                let blob = self.read_string()?;
                let entries = crate::listpack::decode(&blob)?;
                Ok(RdbValue::SortedSet(decode_zset_pairs(entries, "zset listpack")?))
            }

            // --- Ziplist-encoded types ---
            RDB_TYPE_LIST_ZIPLIST => {
                let blob = self.read_string()?;
                let entries = crate::ziplist::decode(&blob)?;
                let elements = entries.into_iter().map(|e| e.into_bytes()).collect();
                Ok(RdbValue::List(elements))
            }

            RDB_TYPE_HASH_ZIPLIST => {
                let blob = self.read_string()?;
                let entries = crate::ziplist::decode(&blob)?;
                Ok(RdbValue::Hash(decode_hash_pairs(entries, "hash ziplist")?))
            }

            RDB_TYPE_ZSET_ZIPLIST => {
                let blob = self.read_string()?;
                let entries = crate::ziplist::decode(&blob)?;
                Ok(RdbValue::SortedSet(decode_zset_pairs(entries, "zset ziplist")?))
            }

            // --- Intset: sorted integer set as a single blob ---
            RDB_TYPE_SET_INTSET => {
                let blob = self.read_string()?;
                let members = crate::intset::decode(&blob)?;
                Ok(RdbValue::Set(members))
            }

            // --- Zipmap: legacy hash encoding, skip for now ---
            RDB_TYPE_HASH_ZIPMAP => {
                let _blob = self.read_string()?;
                Err(RdbError::UnknownType(type_code))
            }

            // --- Hashtable-encoded hash: N field-value string pairs ---
            RDB_TYPE_HASH => {
                let count = Self::len_to_usize(self.read_length_value()?, "element count")?;
                let mut fields = Vec::with_capacity(count);
                for _ in 0..count {
                    let field = self.read_string()?;
                    let value = self.read_string()?;
                    fields.push(HashField {
                        field,
                        value,
                        expiry_ms: None,
                    });
                }
                Ok(RdbValue::Hash(fields))
            }

            // --- HASH_2: N (field, value, expiry_ms) triples (Valkey 9.0) ---
            RDB_TYPE_HASH_2 => {
                let count = Self::len_to_usize(self.read_length_value()?, "element count")?;
                let mut fields = Vec::with_capacity(count);
                for _ in 0..count {
                    let field = self.read_string()?;
                    let value = self.read_string()?;
                    let ttl = self.read_i64_le()?;
                    fields.push(HashField {
                        field,
                        value,
                        expiry_ms: if ttl == -1 { None } else { Some(ttl) },
                    });
                }
                Ok(RdbValue::Hash(fields))
            }

            // --- Plain list: N string elements ---
            RDB_TYPE_LIST => {
                let count = Self::len_to_usize(self.read_length_value()?, "element count")?;
                let mut elements = Vec::with_capacity(count);
                for _ in 0..count {
                    elements.push(self.read_string()?);
                }
                Ok(RdbValue::List(elements))
            }

            // --- Plain set: N string members ---
            RDB_TYPE_SET => {
                let count = Self::len_to_usize(self.read_length_value()?, "element count")?;
                let mut members = Vec::with_capacity(count);
                for _ in 0..count {
                    members.push(self.read_string()?);
                }
                Ok(RdbValue::Set(members))
            }

            // --- Sorted set v1: N (member, rdbLoadDoubleValue score) pairs ---
            RDB_TYPE_ZSET => {
                let count = Self::len_to_usize(self.read_length_value()?, "element count")?;
                let mut pairs = Vec::with_capacity(count);
                for _ in 0..count {
                    let member = self.read_string()?;
                    let score = self.read_double_value()?;
                    pairs.push((member, score));
                }
                Ok(RdbValue::SortedSet(pairs))
            }

            // --- Sorted set v2: N (member, 8-byte LE binary double) pairs ---
            RDB_TYPE_ZSET_2 => {
                let count = Self::len_to_usize(self.read_length_value()?, "element count")?;
                let mut pairs = Vec::with_capacity(count);
                for _ in 0..count {
                    let member = self.read_string()?;
                    let buf = self.read_exact_vec(8)?;
                    let score = f64::from_le_bytes(
                        buf.try_into().expect("read_exact_vec returned 8 bytes"),
                    );
                    pairs.push((member, score));
                }
                Ok(RdbValue::SortedSet(pairs))
            }

            // --- Quicklist v1: N ziplist nodes ---
            RDB_TYPE_LIST_QUICKLIST => {
                let count = Self::len_to_usize(self.read_length_value()?, "element count")?;
                // count is node count, not element count; cap initial capacity
                let mut elements = Vec::with_capacity(count.min(1024));
                for _ in 0..count {
                    let node = self.read_string()?;
                    let entries = crate::ziplist::decode(&node)?;
                    for entry in entries {
                        elements.push(entry.into_bytes());
                    }
                }
                Ok(RdbValue::List(elements))
            }

            // --- Quicklist v2: N (container_type, node_blob) pairs ---
            // Container 1 = PLAIN (raw single element), 2 = PACKED (listpack blob)
            RDB_TYPE_LIST_QUICKLIST_2 => {
                let count = Self::len_to_usize(self.read_length_value()?, "element count")?;
                // count is node count, not element count; cap initial capacity
                let mut elements = Vec::with_capacity(count.min(1024));
                for _ in 0..count {
                    let container = self.read_length_value()?;
                    let node = self.read_string()?;
                    match container {
                        1 => {
                            // PLAIN: the blob is a single raw element
                            elements.push(node);
                        }
                        2 => {
                            // PACKED: the blob is a listpack
                            let entries = crate::listpack::decode(&node)?;
                            for entry in entries {
                                elements.push(entry.into_bytes());
                            }
                        }
                        _ => {
                            return Err(RdbError::CorruptData(format!(
                                "quicklist2 unknown container type: {}",
                                container
                            )))
                        }
                    }
                }
                Ok(RdbValue::List(elements))
            }

            // --- Stream types: skip for now (complex radix tree) ---
            RDB_TYPE_STREAM_LISTPACKS | RDB_TYPE_STREAM_LISTPACKS_2
            | RDB_TYPE_STREAM_LISTPACKS_3 => {
                self.skip_stream(type_code)?;
                Err(RdbError::UnknownType(type_code)) // TODO: decode
            }

            // --- Module types: skip by consuming the opcode stream ---
            RDB_TYPE_MODULE_2 => {
                let _module_id = self.read_length_value()?;
                self.skip_module_data()?;
                Err(RdbError::UnknownType(type_code))
            }

            // MODULE_PRE_GA is rejected by Valkey; we reject it too
            RDB_TYPE_MODULE_PRE_GA => {
                Err(RdbError::CorruptData(
                    "MODULE_PRE_GA (type 6) is not supported".into(),
                ))
            }

            // Type code passed is_object_type() but has no arm above — this means
            // is_object_type and read_value are out of sync. Treat as terminal
            // since we cannot skip an unknown payload format.
            _ => Err(RdbError::CorruptData(format!(
                "type {} recognized as object type but has no decoder or skip logic",
                type_code
            ))),
        }
    }

    /// Skip a stream value by consuming its bytes without decoding.
    /// Streams have the most complex RDB format. This reads just enough
    /// structure to advance the reader past the payload.
    fn skip_stream(&mut self, type_code: u8) -> Result<(), RdbError> {
        // All stream versions start with N listpack entries in a radix tree
        let num_listpacks = Self::len_to_usize(self.read_length_value()?, "stream listpack count")?;
        for _ in 0..num_listpacks {
            let _master_id = self.read_string()?; // radix tree key (stream ID)
            let _listpack = self.read_string()?; // listpack blob
        }

        // Stream metadata
        let _length = self.read_length_value()?; // number of entries
        let _last_id_ms = self.read_length_value()?;
        let _last_id_seq = self.read_length_value()?;

        if type_code >= RDB_TYPE_STREAM_LISTPACKS_2 {
            let _first_id_ms = self.read_length_value()?;
            let _first_id_seq = self.read_length_value()?;
            let _max_deleted_id_ms = self.read_length_value()?;
            let _max_deleted_id_seq = self.read_length_value()?;
            let _entries_added = self.read_length_value()?;
        }

        // Consumer groups
        let num_cgroups = Self::len_to_usize(self.read_length_value()?, "stream cgroup count")?;
        for _ in 0..num_cgroups {
            let _name = self.read_string()?;
            let _last_id_ms = self.read_length_value()?;
            let _last_id_seq = self.read_length_value()?;

            if type_code >= RDB_TYPE_STREAM_LISTPACKS_2 {
                let _entries_read = self.read_length_value()?;
            }

            // PEL (pending entries list)
            let num_pel = Self::len_to_usize(self.read_length_value()?, "stream PEL count")?;
            for _ in 0..num_pel {
                let _id = self.read_exact_vec(16)?; // 128-bit stream ID
                let _delivery_time = self.read_i64_le()?;
                let _delivery_count = self.read_length_value()?;
            }

            // Consumers
            let num_consumers = Self::len_to_usize(self.read_length_value()?, "stream consumer count")?;
            for _ in 0..num_consumers {
                let _consumer_name = self.read_string()?;
                let _seen_time = self.read_i64_le()?;

                if type_code >= RDB_TYPE_STREAM_LISTPACKS_3 {
                    let _active_time = self.read_i64_le()?;
                }

                // Consumer's PEL (references into the group PEL)
                let consumer_pel = Self::len_to_usize(self.read_length_value()?, "stream consumer PEL count")?;
                for _ in 0..consumer_pel {
                    let _id = self.read_exact_vec(16)?; // 128-bit stream ID
                }
            }
        }

        Ok(())
    }
}

/// Iterator implementation — yields one RdbEntry per key-value pair.
impl<R: Read> Iterator for RdbReader<R> {
    type Item = Result<RdbEntry, RdbError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        match self.next_inner() {
            // Normal entry or EOF
            Ok(entry) => entry.map(Ok),
            // UnknownType after consuming bytes: recoverable, iterator continues
            Err(RdbError::UnknownType(t)) => Some(Err(RdbError::UnknownType(t))),
            // All other errors are terminal
            Err(e) => {
                self.finished = true;
                Some(Err(e))
            }
        }
    }
}

impl<R: Read> RdbReader<R> {
    /// Inner iteration logic. Returns Ok(Some(entry)) for a key, Ok(None) for
    /// EOF, or Err for any parse error. The caller (Iterator::next) decides
    /// whether the error is terminal.
    fn next_inner(&mut self) -> Result<Option<RdbEntry>, RdbError> {
        loop {
            let byte = if let Some(b) = self.preamble_byte.take() {
                b
            } else {
                self.read_u8()?
            };

            match byte {
                RDB_OPCODE_AUX => {
                    self.read_aux_field()?;
                    continue;
                }

                RDB_OPCODE_SELECTDB => {
                    let db = self.read_length_value()?;
                    self.current_db = u32::try_from(db).map_err(|_| {
                        RdbError::CorruptData(format!("db index {} exceeds u32", db))
                    })?;
                    continue;
                }

                RDB_OPCODE_RESIZEDB => {
                    let _db_size = self.read_length_value()?;
                    let _expires_size = self.read_length_value()?;
                    continue;
                }

                RDB_OPCODE_EXPIRETIME_MS => {
                    self.pending_expiry_ms = Some(self.read_i64_le()?);
                    continue;
                }

                RDB_OPCODE_EXPIRETIME => {
                    // rdbLoadTime() reads a signed 32-bit value
                    let secs = self.read_i32_le()?;
                    self.pending_expiry_ms = Some(secs as i64 * 1000);
                    continue;
                }

                RDB_OPCODE_IDLE => {
                    self.pending_lru_idle = Some(self.read_length_value()?);
                    continue;
                }

                RDB_OPCODE_FREQ => {
                    self.pending_lfu_freq = Some(self.read_u8()?);
                    continue;
                }

                RDB_OPCODE_EOF => {
                    // CRC was computed over all bytes up to and including the
                    // EOF opcode byte. Grab it before reading the trailer.
                    let computed_crc = self.reader.crc;

                    // Read the 8-byte LE CRC64 trailer (not included in CRC)
                    let mut crc_buf = [0u8; 8];
                    // Read directly from inner to avoid including trailer in CRC
                    self.reader.inner.read_exact(&mut crc_buf)?;
                    let stored_crc = u64::from_le_bytes(crc_buf);

                    // Validate if version >= 5 and stored checksum is non-zero
                    // (zero means checksumming was disabled at save time)
                    if self.header.version >= 5
                        && stored_crc != 0
                        && computed_crc != stored_crc
                    {
                        return Err(RdbError::CorruptData(format!(
                            "CRC64 mismatch: computed 0x{:016x}, stored 0x{:016x}",
                            computed_crc, stored_crc
                        )));
                    }

                    self.finished = true;
                    return Ok(None);
                }

                RDB_OPCODE_FUNCTION2 => {
                    let _payload = self.read_string()?;
                    continue;
                }

                RDB_OPCODE_FUNCTION_PRE_GA => {
                    return Err(RdbError::CorruptData(
                        "FUNCTION_PRE_GA opcode is not supported".into(),
                    ));
                }

                RDB_OPCODE_MODULE_AUX => {
                    let _module_id = self.read_length_value()?;
                    let _when_opcode = self.read_length_value()?;
                    let _when = self.read_length_value()?;
                    self.skip_module_data()?;
                    continue;
                }

                RDB_OPCODE_SLOT_INFO => {
                    let _slot_id = self.read_length_value()?;
                    let _slot_size = self.read_length_value()?;
                    let _expires_size = self.read_length_value()?;
                    continue;
                }

                RDB_OPCODE_SLOT_IMPORT => {
                    let _job_name = self.read_string()?;
                    let num_ranges = Self::len_to_usize(self.read_length_value()?, "slot import range count")?;
                    for _ in 0..num_ranges {
                        let _start = self.read_length_value()?;
                        let _end = self.read_length_value()?;
                    }
                    continue;
                }

                type_code if is_object_type(type_code) => {
                    return self.read_entry(type_code).map(Some);
                }

                unknown => {
                    return Err(RdbError::CorruptData(format!(
                        "unexpected byte 0x{:02x} in opcode position",
                        unknown
                    )));
                }
            }
        }
    }
}

/// Decode compact entries (listpack or ziplist) as hash field-value pairs.
fn decode_hash_pairs(
    entries: Vec<crate::compact::CompactEntry>,
    ctx: &str,
) -> Result<Vec<HashField>, RdbError> {
    if entries.len() % 2 != 0 {
        return Err(RdbError::CorruptData(format!(
            "{ctx} has odd number of entries"
        )));
    }
    let mut fields = Vec::with_capacity(entries.len() / 2);
    let mut iter = entries.into_iter();
    while let Some(field) = iter.next() {
        let value = iter.next().ok_or_else(|| {
            RdbError::CorruptData(format!("{ctx}: missing value for field"))
        })?;
        fields.push(HashField {
            field: field.into_bytes(),
            value: value.into_bytes(),
            expiry_ms: None,
        });
    }
    Ok(fields)
}

/// Decode compact entries (listpack or ziplist) as sorted set member-score pairs.
fn decode_zset_pairs(
    entries: Vec<crate::compact::CompactEntry>,
    ctx: &str,
) -> Result<Vec<(Vec<u8>, f64)>, RdbError> {
    if entries.len() % 2 != 0 {
        return Err(RdbError::CorruptData(format!(
            "{ctx} has odd number of entries"
        )));
    }
    let mut pairs = Vec::with_capacity(entries.len() / 2);
    let mut iter = entries.into_iter();
    while let Some(member) = iter.next() {
        let score_entry = iter.next().ok_or_else(|| {
            RdbError::CorruptData(format!("{ctx}: missing score for member"))
        })?;
        let score = score_entry.to_f64()?;
        pairs.push((member.into_bytes(), score));
    }
    Ok(pairs)
}

/// Format (from valkey/src/rdb.c rdbLoadRioWithLoadingCtx):
///   - Read 9 bytes
///   - If starts with "REDIS0" → Redis magic, version = atoi(buf[6..9])
///   - If starts with "VALKEY" → Valkey magic, version = atoi(buf[6..9])
///   - Otherwise → InvalidMagic error
fn read_header(reader: &mut impl Read) -> Result<RdbHeader, RdbError> {
    let mut buf = [0u8; 9];
    reader.read_exact(&mut buf)?;

    let magic = if &buf[..6] == b"REDIS0" {
        RdbMagic::Redis
    } else if &buf[..6] == b"VALKEY" {
        RdbMagic::Valkey
    } else {
        return Err(RdbError::InvalidMagic);
    };

    let version_str =
        std::str::from_utf8(&buf[6..9]).map_err(|_| RdbError::InvalidMagic)?;
    let version = version_str
        .parse::<u32>()
        .map_err(|_| RdbError::InvalidMagic)?;

    // Match Valkey's strict-mode version acceptance (rdbIsVersionAccepted):
    //   - version 0 always rejected
    //   - REDIS magic: accept native versions 1-11 only (12-79 are foreign, rejected)
    //   - VALKEY magic: accept version 80 only (<=79 rejected for VALKEY magic)
    let valid = match magic {
        RdbMagic::Redis => (1..=RDB_VERSION_MAX_REDIS_NATIVE).contains(&version),
        RdbMagic::Valkey => version == RDB_VERSION_VALKEY,
    };
    if !valid {
        return Err(RdbError::UnsupportedVersion(version));
    }

    Ok(RdbHeader { magic, version })
}

// --- LZF decompression ---

/// Decompress LZF-compressed data.
///
/// LZF is a simple byte-level compressor. Format:
///   - If first byte high bit = 0: literal run (length = byte + 1, copy N bytes)
///   - If first byte high 3 bits != 0b111: short back-reference
///     length = (byte >> 5) + 2, offset from high bits + next byte
///   - If first byte high 3 bits == 0b111: long back-reference
///     length = next byte + 9, offset from remaining bits + byte after
fn lzf_decompress(compressed: &[u8], expected_len: usize) -> Result<Vec<u8>, RdbError> {
    let mut output = Vec::with_capacity(expected_len);
    let mut i = 0;

    while i < compressed.len() {
        let ctrl = compressed[i] as usize;
        i += 1;

        if ctrl < 32 {
            // Literal run: copy ctrl+1 bytes
            let len = ctrl + 1;
            if i + len > compressed.len() {
                return Err(RdbError::CorruptData("LZF literal overrun".into()));
            }
            if output.len() + len > expected_len {
                return Err(RdbError::CorruptData("LZF output exceeds expected length".into()));
            }
            output.extend_from_slice(&compressed[i..i + len]);
            i += len;
        } else {
            // Back-reference
            let mut len = ctrl >> 5;
            let mut offset;

            if len == 7 {
                // Long match: length = next byte + 9
                if i >= compressed.len() {
                    return Err(RdbError::CorruptData("LZF long match overrun".into()));
                }
                len += compressed[i] as usize;
                i += 1;
            }
            len += 2; // minimum match length is 2 (stored as 0)

            if i >= compressed.len() {
                return Err(RdbError::CorruptData("LZF offset overrun".into()));
            }
            offset = ((ctrl & 0x1F) << 8) | compressed[i] as usize;
            i += 1;
            offset += 1; // offset is 1-based

            if offset > output.len() {
                return Err(RdbError::CorruptData("LZF offset beyond output".into()));
            }
            if output.len() + len > expected_len {
                return Err(RdbError::CorruptData("LZF output exceeds expected length".into()));
            }

            let start = output.len() - offset;
            for j in 0..len {
                let byte = output[start + j];
                output.push(byte);
            }
        }
    }

    if output.len() != expected_len {
        return Err(RdbError::CorruptData(format!(
            "LZF decompressed size mismatch: got {}, expected {}",
            output.len(),
            expected_len
        )));
    }

    Ok(output)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
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

    // --- LZF tests ---

    #[test]
    fn test_lzf_decompress_literal_only() {
        // ctrl=4 means literal of 5 bytes
        let compressed = [4u8, b'h', b'e', b'l', b'l', b'o'];
        let result = lzf_decompress(&compressed, 5).unwrap();
        assert_eq!(result, b"hello");
    }

    #[test]
    fn test_lzf_decompress_with_backref() {
        // "aaaa": literal "a" (ctrl=0, 'a'), then backref offset=1, len=3
        // backref: len=3 means stored as 1 (len-2), offset=0 (1-based → stored as 0)
        // ctrl byte: (1 << 5) | 0 = 0x20, offset low byte = 0x00
        let compressed = [0u8, b'a', 0x20, 0x00];
        let result = lzf_decompress(&compressed, 4).unwrap();
        assert_eq!(result, b"aaaa");
    }

    // --- Fixture integration tests ---

    #[test]
    fn test_fixture_valkey_header() {
        let f = std::fs::File::open("../../tests/fixtures/basic.rdb").unwrap();
        let reader = RdbReader::new(f).unwrap();
        assert_eq!(reader.header().magic, RdbMagic::Valkey);
        assert_eq!(reader.header().version, 80);
    }

    #[test]
    fn test_fixture_redis_header() {
        let f = std::fs::File::open("../../tests/fixtures/redis_compat.rdb").unwrap();
        let reader = RdbReader::new(f).unwrap();
        assert_eq!(reader.header().magic, RdbMagic::Redis);
        assert_eq!(reader.header().version, 9);
    }

    #[test]
    fn test_fixture_empty() {
        let f = std::fs::File::open("../../tests/fixtures/empty.rdb").unwrap();
        let reader = RdbReader::new(f).unwrap();
        let entries: Vec<_> = reader.collect();
        assert!(entries.is_empty(), "empty.rdb should have no entries");
    }

    #[test]
    fn test_fixture_valkey_metadata() {
        let f = std::fs::File::open("../../tests/fixtures/basic.rdb").unwrap();
        let reader = RdbReader::new(f).unwrap();
        // AUX fields are eagerly consumed in new(), so metadata is available immediately
        assert!(
            reader.metadata().server_version().is_some(),
            "should have server version in AUX immediately after new()"
        );
    }

    #[test]
    fn test_fixture_redis_compat_strings() {
        // redis_compat.rdb has mystring = "hello world"
        let f = std::fs::File::open("../../tests/fixtures/redis_compat.rdb").unwrap();
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
                Err(RdbError::UnknownType(_)) => {} // expected for unimplemented types
                Err(e) => panic!("unexpected error: {}", e),
            }
        }
        assert!(found_mystring, "should find mystring key in redis_compat.rdb");
    }

    #[test]
    fn test_fixture_expiry() {
        let f = std::fs::File::open("../../tests/fixtures/expiry.rdb").unwrap();
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

    #[test]
    fn test_fixture_multi_db() {
        let f = std::fs::File::open("../../tests/fixtures/multi_db.rdb").unwrap();
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

    #[test]
    fn test_fixture_int_encoded_strings() {
        let f = std::fs::File::open("../../tests/fixtures/encodings.rdb").unwrap();
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

    // --- Fixture: hash_field_ttl.rdb stays aligned ---

    #[test]
    fn test_fixture_hash_field_ttl() {
        let f = std::fs::File::open("../../tests/fixtures/hash_field_ttl.rdb").unwrap();
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
        assert!(fields.iter().all(|f| f.expiry_ms.is_none()), "normal hash should have no field TTLs");
    }

    // --- Fixture: streams.rdb stays aligned ---

    #[test]
    fn test_fixture_streams_no_crash() {
        let f = std::fs::File::open("../../tests/fixtures/streams.rdb").unwrap();
        let reader = RdbReader::new(f).unwrap();
        let mut count = 0;
        for entry in reader {
            count += 1;
            let _ = entry;
        }
        assert!(count > 0, "streams.rdb should have entries");
    }

    // --- Fixture: set_listpack.rdb ---

    #[test]
    fn test_fixture_set_listpack() {
        let f = std::fs::File::open("../../tests/fixtures/set_listpack.rdb").unwrap();
        let reader = RdbReader::new(f).unwrap();
        let mut found = false;
        for entry in reader {
            let e = entry.unwrap();
            if e.key == b"myset" {
                if let RdbValue::Set(members) = &e.value {
                    let mut sorted: Vec<&[u8]> = members.iter().map(|m| m.as_slice()).collect();
                    sorted.sort();
                    assert_eq!(sorted, vec![
                        &b"alpha"[..], &b"beta"[..], &b"delta"[..], &b"gamma"[..]
                    ]);
                    found = true;
                } else {
                    panic!("expected Set, got {:?}", e.value);
                }
            }
        }
        assert!(found, "should find myset");
    }

    // --- Fixture: hash_listpack.rdb ---

    #[test]
    fn test_fixture_hash_listpack() {
        let f = std::fs::File::open("../../tests/fixtures/hash_listpack.rdb").unwrap();
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
                    assert_eq!(map, vec![
                        (&b"age"[..], &b"30"[..]),
                        (&b"city"[..], &b"nyc"[..]),
                        (&b"name"[..], &b"alice"[..]),
                    ]);
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

    // --- Fixture: zset_listpack.rdb ---

    #[test]
    fn test_fixture_zset_listpack() {
        let f = std::fs::File::open("../../tests/fixtures/zset_listpack.rdb").unwrap();
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

    // --- Fixture: listpack_all.rdb (set + hash + zset in one file) ---

    #[test]
    fn test_fixture_listpack_all() {
        let f = std::fs::File::open("../../tests/fixtures/listpack_all.rdb").unwrap();
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

    // --- Fixture: list_ziplist.rdb ---

    #[test]
    fn test_fixture_list_ziplist() {
        let f = std::fs::File::open("../../tests/fixtures/list_ziplist.rdb").unwrap();
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

    // --- Fixture: hash_ziplist.rdb ---

    #[test]
    fn test_fixture_hash_ziplist() {
        let f = std::fs::File::open("../../tests/fixtures/hash_ziplist.rdb").unwrap();
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

    // --- Fixture: zset_ziplist.rdb ---

    #[test]
    fn test_fixture_zset_ziplist() {
        let f = std::fs::File::open("../../tests/fixtures/zset_ziplist.rdb").unwrap();
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

    // --- Fixture: set_intset.rdb (type 11) ---

    #[test]
    fn test_fixture_set_intset() {
        let f = std::fs::File::open("../../tests/fixtures/set_intset.rdb").unwrap();
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

    // --- Fixture: list_quicklist.rdb (type 14, quicklist v1) ---

    #[test]
    fn test_fixture_list_quicklist() {
        let f = std::fs::File::open("../../tests/fixtures/list_quicklist.rdb").unwrap();
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

    // --- Fixture: list_quicklist2.rdb (type 18, quicklist v2) ---

    #[test]
    fn test_fixture_list_quicklist2() {
        let f = std::fs::File::open("../../tests/fixtures/list_quicklist2.rdb").unwrap();
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

    // --- Fixture: ziplist_all.rdb (list + hash + zset in one file) ---

    #[test]
    fn test_fixture_ziplist_all() {
        let f = std::fs::File::open("../../tests/fixtures/ziplist_all.rdb").unwrap();
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

    // --- LZF overflow detection ---

    #[test]
    fn test_lzf_output_overflow_literal() {
        // Literal of 5 bytes but expected_len is 3
        let compressed = [4u8, b'h', b'e', b'l', b'l', b'o'];
        assert!(lzf_decompress(&compressed, 3).is_err());
    }

    #[test]
    fn test_lzf_output_overflow_backref() {
        // "a" literal + backref that would produce 4 total, but expected is 2
        let compressed = [0u8, b'a', 0x20, 0x00];
        assert!(lzf_decompress(&compressed, 2).is_err());
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
        }
    }
}
