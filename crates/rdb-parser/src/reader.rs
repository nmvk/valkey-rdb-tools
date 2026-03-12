// RDB file reader — iterates over entries in an RDB file
//
// Reference: valkey/src/rdb.c (rdbLoadRioWithLoadingCtx, rdbLoadLen, rdbLoadStringObject)

use std::io::{BufReader, Read};

use crate::opcodes::*;
use crate::types::*;

/// Result of reading a length-encoded value: either a plain length or a
/// special encoding indicator.
enum LenResult {
    /// A plain byte length.
    Len(u64),
    /// A special encoding sub-type (INT8, INT16, INT32, LZF).
    Special(u8),
}

/// The main RDB reader. Wraps any `Read` source and yields `RdbEntry` items.
pub struct RdbReader<R: Read> {
    reader: BufReader<R>,
    header: RdbHeader,
    metadata: RdbMetadata,

    // State carried between calls to next()
    current_db: u32,
    pending_expiry_ms: Option<i64>,
    pending_lru_idle: Option<u64>,
    pending_lfu_freq: Option<u8>,
    finished: bool,
}

impl<R: Read> RdbReader<R> {
    /// Create a new RDB reader. Reads and validates the header, then consumes
    /// all AUX fields up to the first database section or EOF.
    pub fn new(reader: R) -> Result<Self, RdbError> {
        let mut reader = BufReader::new(reader);
        let header = read_header(&mut reader)?;
        let mut rdr = Self {
            reader,
            header,
            metadata: RdbMetadata::default(),
            current_db: 0,
            pending_expiry_ms: None,
            pending_lru_idle: None,
            pending_lfu_freq: None,
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

    fn read_u32_le(&mut self) -> Result<u32, RdbError> {
        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }

    fn read_u64_le(&mut self) -> Result<u64, RdbError> {
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
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
                self.read_exact_vec(len as usize)
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
                    let compressed_len = self.read_length_value()? as usize;
                    let uncompressed_len = self.read_length_value()? as usize;
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

    /// Read the preamble: consume AUX fields, SELECTDB, RESIZEDB that appear
    /// before the first key-value entry. Stops when it hits a type byte or EOF,
    /// and stores the peeked byte for the iterator.
    fn read_preamble(&mut self) -> Result<(), RdbError> {
        // The preamble is handled in the iterator's next() method.
        // AUX fields before the first SELECTDB are consumed there.
        Ok(())
    }

    /// Read a single key-value entry given the type byte.
    fn read_entry(&mut self, type_code: u8) -> Result<RdbEntry, RdbError> {
        let key = self.read_string()?;

        let value = self.read_value(type_code)?;

        let entry = RdbEntry {
            db: self.current_db,
            key,
            value,
            type_code,
            expiry_ms: self.pending_expiry_ms.take(),
            lru_idle_secs: self.pending_lru_idle.take(),
            lfu_frequency: self.pending_lfu_freq.take(),
        };

        Ok(entry)
    }

    /// Read the value for a given type code.
    /// Currently only STRING is fully decoded. Other types skip the raw payload
    /// so the stream stays aligned for the next entry.
    fn read_value(&mut self, type_code: u8) -> Result<RdbValue, RdbError> {
        match type_code {
            RDB_TYPE_STRING => {
                let data = self.read_string()?;
                Ok(RdbValue::String(data))
            }

            // --- Compact-encoded types: single string blob ---
            // These store the entire data structure as one length-prefixed blob
            // (listpack, ziplist, intset). We skip the blob for now.
            RDB_TYPE_LIST_ZIPLIST | RDB_TYPE_SET_INTSET | RDB_TYPE_ZSET_ZIPLIST
            | RDB_TYPE_HASH_ZIPMAP | RDB_TYPE_HASH_ZIPLIST | RDB_TYPE_HASH_LISTPACK
            | RDB_TYPE_ZSET_LISTPACK | RDB_TYPE_SET_LISTPACK => {
                let _blob = self.read_string()?; // skip the blob
                Err(RdbError::UnknownType(type_code)) // TODO: decode
            }

            // --- Hashtable-encoded types: N key-value string pairs ---
            RDB_TYPE_HASH => {
                let count = self.read_length_value()?;
                for _ in 0..count {
                    let _field = self.read_string()?;
                    let _value = self.read_string()?;
                }
                Err(RdbError::UnknownType(type_code)) // TODO: decode
            }

            // --- HASH_2: N (field, value, ttl) triples ---
            RDB_TYPE_HASH_2 => {
                let count = self.read_length_value()?;
                for _ in 0..count {
                    let _ttl = self.read_length_value()?; // per-field TTL
                    let _field = self.read_string()?;
                    let _value = self.read_string()?;
                }
                Err(RdbError::UnknownType(type_code)) // TODO: decode
            }

            // --- Plain list: N string elements ---
            RDB_TYPE_LIST => {
                let count = self.read_length_value()?;
                for _ in 0..count {
                    let _elem = self.read_string()?;
                }
                Err(RdbError::UnknownType(type_code)) // TODO: decode
            }

            // --- Plain set: N string members ---
            RDB_TYPE_SET => {
                let count = self.read_length_value()?;
                for _ in 0..count {
                    let _member = self.read_string()?;
                }
                Err(RdbError::UnknownType(type_code)) // TODO: decode
            }

            // --- Sorted set v1: N (member, double-as-string) pairs ---
            RDB_TYPE_ZSET => {
                let count = self.read_length_value()?;
                for _ in 0..count {
                    let _member = self.read_string()?;
                    // Score is stored as a string-encoded double
                    let _score = self.read_string()?;
                }
                Err(RdbError::UnknownType(type_code)) // TODO: decode
            }

            // --- Sorted set v2: N (member, 8-byte binary double) pairs ---
            RDB_TYPE_ZSET_2 => {
                let count = self.read_length_value()?;
                for _ in 0..count {
                    let _member = self.read_string()?;
                    let _score = self.read_exact_vec(8)?; // IEEE 754 double
                }
                Err(RdbError::UnknownType(type_code)) // TODO: decode
            }

            // --- Quicklist v1: N compressed/raw ziplist nodes ---
            RDB_TYPE_LIST_QUICKLIST => {
                let count = self.read_length_value()?;
                for _ in 0..count {
                    let _node = self.read_string()?; // each node is a ziplist blob
                }
                Err(RdbError::UnknownType(type_code)) // TODO: decode
            }

            // --- Quicklist v2: N (container_type, node_blob) pairs ---
            RDB_TYPE_LIST_QUICKLIST_2 => {
                let count = self.read_length_value()?;
                for _ in 0..count {
                    let _container = self.read_length_value()?;
                    let _node = self.read_string()?;
                }
                Err(RdbError::UnknownType(type_code)) // TODO: decode
            }

            // --- Stream types: skip for now (complex radix tree) ---
            RDB_TYPE_STREAM_LISTPACKS | RDB_TYPE_STREAM_LISTPACKS_2
            | RDB_TYPE_STREAM_LISTPACKS_3 => {
                self.skip_stream(type_code)?;
                Err(RdbError::UnknownType(type_code)) // TODO: decode
            }

            // --- Module types: not supported ---
            RDB_TYPE_MODULE_PRE_GA | RDB_TYPE_MODULE_2 => {
                Err(RdbError::UnknownType(type_code))
            }

            _ => Err(RdbError::UnknownType(type_code)),
        }
    }

    /// Skip a stream value by consuming its bytes without decoding.
    /// Streams have the most complex RDB format. This reads just enough
    /// structure to advance the reader past the payload.
    fn skip_stream(&mut self, type_code: u8) -> Result<(), RdbError> {
        // All stream versions start with N listpack entries in a radix tree
        let num_listpacks = self.read_length_value()?;
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
        let num_cgroups = self.read_length_value()?;
        for _ in 0..num_cgroups {
            let _name = self.read_string()?;
            let _last_id_ms = self.read_length_value()?;
            let _last_id_seq = self.read_length_value()?;

            if type_code >= RDB_TYPE_STREAM_LISTPACKS_2 {
                let _entries_read = self.read_length_value()?;
            }

            // PEL (pending entries list)
            let num_pel = self.read_length_value()?;
            for _ in 0..num_pel {
                let _id = self.read_exact_vec(16)?; // 128-bit stream ID
                let _delivery_time = self.read_i64_le()?;
                let _delivery_count = self.read_length_value()?;
            }

            // Consumers
            let num_consumers = self.read_length_value()?;
            for _ in 0..num_consumers {
                let _consumer_name = self.read_string()?;
                let _seen_time = self.read_i64_le()?;

                if type_code >= RDB_TYPE_STREAM_LISTPACKS_3 {
                    let _active_time = self.read_i64_le()?;
                }

                // Consumer's PEL (references into the group PEL)
                let consumer_pel = self.read_length_value()?;
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

        loop {
            let byte = match self.read_u8() {
                Ok(b) => b,
                Err(e) => return Some(Err(e)),
            };

            match byte {
                RDB_OPCODE_AUX => {
                    // AUX field: two strings (key, value)
                    let key = match self.read_string() {
                        Ok(s) => s,
                        Err(e) => return Some(Err(e)),
                    };
                    let value = match self.read_string() {
                        Ok(s) => s,
                        Err(e) => return Some(Err(e)),
                    };
                    let key_str = String::from_utf8_lossy(&key).to_string();
                    let val_str = String::from_utf8_lossy(&value).to_string();
                    self.metadata.aux.insert(key_str, val_str);
                    continue;
                }

                RDB_OPCODE_SELECTDB => {
                    let db = match self.read_length_value() {
                        Ok(v) => v as u32,
                        Err(e) => return Some(Err(e)),
                    };
                    self.current_db = db;
                    continue;
                }

                RDB_OPCODE_RESIZEDB => {
                    // Two length-encoded ints: db size, expiry table size. Skip both.
                    if let Err(e) = self.read_length_value() {
                        return Some(Err(e));
                    }
                    if let Err(e) = self.read_length_value() {
                        return Some(Err(e));
                    }
                    continue;
                }

                RDB_OPCODE_EXPIRETIME_MS => {
                    let ms = match self.read_i64_le() {
                        Ok(v) => v,
                        Err(e) => return Some(Err(e)),
                    };
                    self.pending_expiry_ms = Some(ms);
                    continue;
                }

                RDB_OPCODE_EXPIRETIME => {
                    let secs = match self.read_u32_le() {
                        Ok(v) => v,
                        Err(e) => return Some(Err(e)),
                    };
                    self.pending_expiry_ms = Some(secs as i64 * 1000);
                    continue;
                }

                RDB_OPCODE_IDLE => {
                    let idle = match self.read_length_value() {
                        Ok(v) => v,
                        Err(e) => return Some(Err(e)),
                    };
                    self.pending_lru_idle = Some(idle);
                    continue;
                }

                RDB_OPCODE_FREQ => {
                    let freq = match self.read_u8() {
                        Ok(v) => v,
                        Err(e) => return Some(Err(e)),
                    };
                    self.pending_lfu_freq = Some(freq);
                    continue;
                }

                RDB_OPCODE_EOF => {
                    // 8-byte CRC64 checksum follows (skip for now)
                    // TODO: validate CRC64
                    self.finished = true;
                    return None;
                }

                // Function opcodes — skip the payload
                RDB_OPCODE_FUNCTION2 | RDB_OPCODE_FUNCTION_PRE_GA => {
                    if let Err(e) = self.read_string() {
                        return Some(Err(e));
                    }
                    continue;
                }

                // Module aux data — skip
                RDB_OPCODE_MODULE_AUX => {
                    // Skip module ID + module data (same as module type)
                    // For now, skip by reading the raw string blob
                    if let Err(e) = self.read_string() {
                        return Some(Err(e));
                    }
                    continue;
                }

                // Slot info (Valkey 9.0): 3 length-encoded ints (slot_id, slot_size, expires_size)
                RDB_OPCODE_SLOT_INFO => {
                    if let Err(e) = self.read_length_value() {
                        return Some(Err(e));
                    }
                    if let Err(e) = self.read_length_value() {
                        return Some(Err(e));
                    }
                    if let Err(e) = self.read_length_value() {
                        return Some(Err(e));
                    }
                    continue;
                }

                // Slot import (Valkey 9.0): string (job_name) + N slot ranges
                RDB_OPCODE_SLOT_IMPORT => {
                    if let Err(e) = self.read_string() {
                        return Some(Err(e));
                    }
                    let num_ranges = match self.read_length_value() {
                        Ok(v) => v,
                        Err(e) => return Some(Err(e)),
                    };
                    for _ in 0..num_ranges {
                        if let Err(e) = self.read_length_value() {
                            return Some(Err(e));
                        }
                        if let Err(e) = self.read_length_value() {
                            return Some(Err(e));
                        }
                    }
                    continue;
                }

                // Otherwise it must be a type byte for a key-value pair
                type_code if is_object_type(type_code) => {
                    return Some(self.read_entry(type_code));
                }

                unknown => {
                    return Some(Err(RdbError::UnknownType(unknown)));
                }
            }
        }
    }
}

// --- Header parsing ---

/// Read and validate the 9-byte RDB header.
///
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
        let mut reader = RdbReader::new(f).unwrap();
        // Consume all entries so AUX fields are parsed
        while let Some(_) = reader.next() {}
        assert!(
            reader.metadata().server_version().is_some(),
            "should have server version in AUX"
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
                Ok(_) => {} // other types will error for now, skip
                Err(_) => {} // expected for unimplemented types
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

    // --- Test helper ---

    /// Create a minimal RdbReader from raw bytes (skips header parsing).
    fn make_test_reader(data: &[u8]) -> RdbReader<Cursor<Vec<u8>>> {
        RdbReader {
            reader: BufReader::new(Cursor::new(data.to_vec())),
            header: RdbHeader {
                magic: RdbMagic::Valkey,
                version: 80,
            },
            metadata: RdbMetadata::default(),
            current_db: 0,
            pending_expiry_ms: None,
            pending_lru_idle: None,
            pending_lfu_freq: None,
            finished: false,
        }
    }
}
