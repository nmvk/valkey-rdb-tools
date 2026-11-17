// RDB file reader — iterates over entries in an RDB file
//
// Reference: valkey/src/rdb.c (rdbLoadRioWithLoadingCtx, rdbLoadLen, rdbLoadStringObject)

use std::io::{BufReader, Read};

use crate::crc64;
use crate::limits::{self, Limits};
use crate::opcodes::*;
use crate::types::*;
use crate::wipe::{Zeroize, Zeroizing};
use crate::CAPACITY_HINT_MAX;

// Module serialized value sub-opcodes (from rdb.h)
const RDB_MODULE_OPCODE_EOF: u64 = 0;
const RDB_MODULE_OPCODE_SINT: u64 = 1;
const RDB_MODULE_OPCODE_UINT: u64 = 2;
const RDB_MODULE_OPCODE_FLOAT: u64 = 3;
const RDB_MODULE_OPCODE_DOUBLE: u64 = 4;
const RDB_MODULE_OPCODE_STRING: u64 = 5;

/// Module type name character set (base-64 alphabet from Valkey's module.c).
const MODULE_TYPE_NAME_CHARSET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Decode a 64-bit module type ID into (name, encoding_version).
///
/// The module ID packs a 9-character name in the upper 54 bits (6 bits per
/// character, using a base-64 alphabet) and a 10-bit encoding version in the
/// lower 10 bits. Ported from `moduleTypeNameByID` in Valkey `module.c`.
fn decode_module_id(module_id: u64) -> (String, u32) {
    let mut name = [0u8; 9];
    let version = (module_id & 0x3FF) as u32; // lower 10 bits
    let mut id = module_id >> 10;
    for i in (0..9).rev() {
        name[i] = MODULE_TYPE_NAME_CHARSET[(id & 0x3F) as usize];
        id >>= 6;
    }
    // Module names are always exactly 9 characters
    (String::from_utf8_lossy(&name).into_owned(), version)
}

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
    remaining: Option<(u64, u64)>,
    crc: u64,
}

impl<R: Read> CrcReader<R> {
    fn new(inner: BufReader<R>, initial_crc: u64) -> Self {
        Self {
            inner,
            crc: initial_crc,
            remaining: None,
        }
    }
}

impl<R: Read> Read for CrcReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let allowed = if let Some((bytes, work)) = self.remaining {
            let allowed = bytes.min(work).min(buf.len() as u64) as usize;
            if allowed == 0 && !buf.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    "snapshot input/work bound",
                ));
            }
            allowed
        } else {
            buf.len()
        };
        let n = self.inner.read(&mut buf[..allowed])?;
        if let Some((bytes, work)) = &mut self.remaining {
            *bytes -= n as u64;
            *work -= n as u64;
        }
        self.crc = crc64::crc64(self.crc, &buf[..n]);
        Ok(n)
    }
}

/// Default maximum elements per chunk for plain-encoded collections.
/// Set below the typical Arrow batch size (65,536) so the batcher can flush
/// promptly without accumulating oversized batches.
pub const DEFAULT_MAX_KEY_ELEMENTS: usize = 50_000;

/// State for reading a large key in chunks.
///
/// The `key` field is cloned for each chunk entry. This is acceptable because
/// RDB keys are typically < 1KB. For adversarial key sizes (up to 512MB),
/// the clone cost is amortized across the chunk's element I/O.
struct ChunkedState {
    db: u32,
    key: Vec<u8>,
    type_code: u8,
    expiry_ms: Option<i64>,
    lru_idle_secs: Option<u64>,
    lfu_frequency: Option<u8>,
    total_count: usize,
    remaining: usize,
    element_offset: u64,
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
    /// Maximum elements per chunk. Defaults to [`DEFAULT_MAX_KEY_ELEMENTS`].
    /// `None` disables chunking entirely.
    max_key_elements: Option<usize>,
    /// Active chunk-reading state for a large key being split across entries.
    chunked: Option<ChunkedState>,
    /// Set to true when the CRC-64 checksum was actually validated at EOF.
    crc_checked: bool,
    strict: Option<Limits>,
    entry_limits: Option<(u64, u64)>,
}

impl<R: Read> RdbReader<R> {
    /// Create a new RDB reader. Reads and validates the header, then eagerly
    /// consumes all AUX fields up to the first database section or EOF.
    /// After `new()` returns, [`metadata()`](Self::metadata) is fully populated.
    pub fn new(reader: R) -> Result<Self, RdbError> {
        Self::construct(reader, None)
    }

    /// Admit a strict native snapshot with bounds installed before eager AUX parsing.
    /// Unsupported functions, modules, streams, slot sections and hash-field TTLs
    /// are terminal. Raw AUX bytes retain order and duplicates (in this mode
    /// [`metadata().aux`](Self::metadata) is not populated; use `raw_aux`).
    ///
    /// Input is read through an internal buffer, so the underlying source may be read up to
    /// 8 KiB beyond `input_bytes` even though the parse itself never consumes more.
    pub fn new_limited(reader: R, limits: Limits) -> Result<Self, RdbError> {
        if [
            limits.input_bytes,
            limits.allocated_bytes,
            limits.metadata_bytes,
            limits.string_bytes,
            limits.elements,
            limits.work,
        ]
        .contains(&0)
        {
            return Err(RdbError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "strict limits must all be greater than zero",
            )));
        }
        Self::construct(reader, Some(limits))
    }

    fn construct(reader: R, strict: Option<Limits>) -> Result<Self, RdbError> {
        let mut reader = CrcReader::new(BufReader::new(reader), 0);
        reader.remaining = strict.map(|v| (v.input_bytes, v.work));
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
            max_key_elements: Some(DEFAULT_MAX_KEY_ELEMENTS),
            crc_checked: false,
            entry_limits: strict.map(|v| (v.allocated_bytes, v.elements)),
            strict,
            chunked: None,
        };
        rdr.read_preamble()?;
        Ok(rdr)
    }

    /// Recover the caller-owned reader.
    ///
    /// After a strict parse that reached EOF successfully, the internal buffer is empty and
    /// the reader is positioned at end of input. After an error or an early stop, up to
    /// 8 KiB of read-ahead held in the internal buffer is discarded.
    pub fn into_inner(self) -> R {
        self.reader.inner.into_inner()
    }

    /// The parsed RDB header.
    pub fn header(&self) -> &RdbHeader {
        &self.header
    }

    /// File-level metadata from AUX fields.
    pub fn metadata(&self) -> &RdbMetadata {
        &self.metadata
    }

    /// Returns true if the CRC-64 checksum was validated at EOF.
    /// False before iteration completes, for RDB version < 5, or when
    /// the stored checksum was zero (checksumming disabled at save time).
    pub fn crc_checked(&self) -> bool {
        self.crc_checked
    }

    /// Set the maximum number of elements per chunk for large plain-encoded
    /// collections. When a key has more elements than this limit, it will be
    /// yielded as multiple `RdbEntry` values with `total_elements` and
    /// `element_offset` set. Default is [`DEFAULT_MAX_KEY_ELEMENTS`] (50,000).
    ///
    /// **Note:** Chunked sorted sets skip geo-key detection (which requires
    /// seeing all scores at once), so they will always be typed as `SortedSet`
    /// rather than `Geo`.
    /// # Panics
    ///
    /// Panics if `max` is 0 (would cause an infinite loop).
    #[must_use]
    pub fn with_max_key_elements(mut self, max: usize) -> Self {
        assert!(max > 0, "max_key_elements must be > 0");
        self.max_key_elements = Some(max);
        self
    }

    /// Disable chunking entirely. All collections will be read into a single
    /// `RdbEntry` regardless of size.
    #[must_use]
    pub fn without_chunking(mut self) -> Self {
        self.max_key_elements = None;
        self
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
        self.charge_bytes(len)?;
        let mut buf = Zeroizing::new(limits::vector(len)?);
        buf.resize(len, 0);
        self.reader.read_exact(&mut buf)?;
        Ok(std::mem::take(&mut *buf))
    }

    fn charge_bytes(&mut self, count: usize) -> Result<(), RdbError> {
        if let Some(limits) = &mut self.strict {
            if count as u64 > limits.string_bytes {
                return Err(limits::refused());
            }
            limits::debit(&mut limits.allocated_bytes, count as u64)?;
        }
        Ok(())
    }

    fn charge_elements(&mut self, count: usize) -> Result<(), RdbError> {
        if let Some(limits) = &mut self.strict {
            limits::debit(&mut limits.elements, count as u64)?;
            // Includes output vectors, temporary compact entries and conversion.
            limits::debit(
                &mut limits.allocated_bytes,
                (count as u64)
                    .checked_mul(128)
                    .ok_or_else(limits::refused)?,
            )?;
        }
        if let Some((_, work)) = &mut self.reader.remaining {
            limits::debit(work, count as u64)?;
        }
        Ok(())
    }

    fn compact(
        &mut self,
        bytes: &[u8],
        listpack: bool,
    ) -> Result<Vec<crate::compact::CompactEntry>, RdbError> {
        self.charge_elements(bytes.len())?;
        if listpack {
            crate::listpack::decode(bytes)
        } else {
            crate::ziplist::decode(bytes)
        }
    }

    fn zset_compact(
        &self,
        entries: Vec<crate::compact::CompactEntry>,
        ctx: &str,
    ) -> Result<RdbValue, RdbError> {
        if self.strict.is_none() {
            return Ok(RdbValue::SortedSet(decode_zset_pairs(entries, ctx)?));
        }
        if entries.len() % 2 != 0 {
            return Err(RdbError::CorruptData(format!(
                "{ctx} has odd number of entries"
            )));
        }
        let mut out = Zeroizing::new(limits::vector(entries.len() / 2)?);
        let mut entries = entries.into_iter();
        while let Some(member) = entries.next() {
            let score = entries.next().ok_or_else(|| {
                RdbError::CorruptData(format!("{ctx}: missing score for member"))
            })?;
            score.to_f64()?;
            out.push((member.into_bytes(), score.into_bytes()));
        }
        Ok(RdbValue::SortedSetBytes(std::mem::take(&mut *out)))
    }

    /// Like the iterator's `next`, but the decoded entry is wiped when dropped, on success
    /// as well as on error or cancellation. Requires the `zeroize` feature.
    #[cfg(feature = "zeroize")]
    pub fn next_protected(&mut self) -> Option<Result<Zeroizing<RdbEntry>, RdbError>> {
        self.next().map(|entry| entry.map(Zeroizing::new))
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
                    self.charge_bytes(uncompressed_len)?;
                    if let Some((_, work)) = &mut self.reader.remaining {
                        limits::debit(work, uncompressed_len as u64)?;
                    }
                    let compressed = Zeroizing::new(self.read_exact_vec(compressed_len)?);
                    let decompressed = crate::lzf::decompress(&compressed, uncompressed_len)?;
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
                let buf = Zeroizing::new(self.read_exact_vec(n as usize)?);
                let s = std::str::from_utf8(&buf).map_err(|_| {
                    RdbError::CorruptData("invalid UTF-8 in double value".into())
                })?;
                s.parse::<f64>().map_err(|_| {
                    RdbError::CorruptData(format!("invalid double string: {}", s))
                })
            }
        }
    }

    /// Maximum number of sub-values a single module payload may decode
    /// before the parser rejects it. A crafted module could otherwise
    /// emit millions of tiny UINT/STRING opcodes and force
    /// proportional allocation before `max_entry_bytes` or Arrow
    /// batching ever sees the entry. 5M matches the stream cap.
    const MAX_MODULE_VALUES: usize = 5_000_000;

    /// Maximum cumulative bytes of variable-length module string values.
    /// Fixed-width numeric variants don't count. 256 MiB is comfortably
    /// above realistic module payloads (document stores, time-series
    /// samples, large blobs) while still preventing a compressed-stream
    /// expansion from running away.
    const MAX_MODULE_STRING_BYTES: usize = 256 * 1024 * 1024;

    /// Consume a module's opcode stream, calling `emit` for each decoded value.
    ///
    /// Single dispatch point for all module sub-opcodes — both
    /// [`read_module_data`](Self::read_module_data) and
    /// [`skip_module_data`](Self::skip_module_data) delegate here to
    /// avoid drift. The emit callback may return `Err` to short-circuit
    /// the loop (used by `read_module_data` to enforce value and byte
    /// caps on adversarial input).
    ///
    /// Reference: `rdbLoadCheckModuleValue()` in valkey/src/rdb.c.
    fn for_each_module_value(
        &mut self,
        mut emit: impl FnMut(ModuleValue) -> Result<(), RdbError>,
    ) -> Result<(), RdbError> {
        loop {
            let opcode = self.read_length_value()?;
            if opcode == RDB_MODULE_OPCODE_EOF {
                return Ok(());
            }
            match opcode {
                RDB_MODULE_OPCODE_SINT => {
                    let raw = self.read_length_value()?;
                    emit(ModuleValue::SignedInt(raw as i64))?;
                }
                RDB_MODULE_OPCODE_UINT => {
                    let raw = self.read_length_value()?;
                    emit(ModuleValue::UnsignedInt(raw))?;
                }
                RDB_MODULE_OPCODE_STRING => {
                    let s = self.read_string()?;
                    emit(ModuleValue::String(s))?;
                }
                RDB_MODULE_OPCODE_FLOAT => {
                    let mut buf = [0u8; 4];
                    self.reader.read_exact(&mut buf)?;
                    emit(ModuleValue::Float(f32::from_le_bytes(buf)))?;
                }
                RDB_MODULE_OPCODE_DOUBLE => {
                    let mut buf = [0u8; 8];
                    self.reader.read_exact(&mut buf)?;
                    emit(ModuleValue::Double(f64::from_le_bytes(buf)))?;
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

    /// Read a module's opcode stream, collecting all values, with caps on
    /// both value count and accumulated string bytes to prevent an
    /// adversarial module payload from driving unbounded allocation.
    fn read_module_data(&mut self) -> Result<Vec<ModuleValue>, RdbError> {
        let mut values: Vec<ModuleValue> = Vec::new();
        let mut string_bytes: usize = 0;
        self.for_each_module_value(|v| {
            if values.len() >= Self::MAX_MODULE_VALUES {
                return Err(RdbError::CorruptData(format!(
                    "module value count exceeds {} limit",
                    Self::MAX_MODULE_VALUES
                )));
            }
            if let ModuleValue::String(ref s) = v {
                string_bytes = string_bytes.saturating_add(s.len());
                if string_bytes > Self::MAX_MODULE_STRING_BYTES {
                    return Err(RdbError::CorruptData(format!(
                        "module string bytes exceed {} limit",
                        Self::MAX_MODULE_STRING_BYTES
                    )));
                }
            }
            values.push(v);
            Ok(())
        })?;
        Ok(values)
    }

    /// Skip a module's opcode stream, discarding values without allocation.
    /// Used for MODULE_AUX data which is always discarded.
    fn skip_module_data(&mut self) -> Result<(), RdbError> {
        self.for_each_module_value(|_| Ok(()))
    }

    /// Eagerly consume AUX fields that precede the first database section.
    /// Stores the first non-AUX byte in `preamble_byte` for replay.
    /// Read one AUX field (key + value strings) and insert into metadata.
    fn read_aux_field(&mut self) -> Result<(), RdbError> {
        let mut key = Zeroizing::new(self.read_string()?);
        let mut value = Zeroizing::new(self.read_string()?);
        if let Some(limits) = &mut self.strict {
            limits::debit(
                &mut limits.metadata_bytes,
                (key.len() as u64)
                    .checked_add(value.len() as u64)
                    .and_then(|v| v.checked_add(48))
                    .ok_or_else(limits::refused)?,
            )?;
            limits::push(
                &mut self.metadata.raw_aux,
                (std::mem::take(&mut *key), std::mem::take(&mut *value)),
            )?;
            return Ok(());
        }
        let key = std::mem::take(&mut *key);
        let value = std::mem::take(&mut *value);
        // AUX keys must be valid UTF-8 (they're server-defined ASCII names
        // like "redis-ver", "ctime", etc.). Reject non-UTF-8 to avoid
        // lossy conversion collisions in the metadata map.
        let key_str = String::from_utf8(key).map_err(|_| {
            RdbError::CorruptData("AUX key is not valid UTF-8".into())
        })?;
        // AUX values are typically ASCII but could contain arbitrary bytes
        // in future extensions. Use lossy conversion for values only.
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

        let mut key = Zeroizing::new(self.read_string()?);

        // Check if this is a chunkable plain type that exceeds the threshold.
        // Both branches below return early — the fallthrough to read_value() at
        // the end of this function is only reached when chunking is disabled or
        // the type is not a plain type (compact encoding, string, stream, module).
        if let Some(max) = self.max_key_elements {
            if is_plain_type(type_code) {
                let count = Self::len_to_usize(self.read_length_value()?, "element count")?;
                if count > max {
                    let chunk_size = max; // count > max is guaranteed here
                    let value = self.read_n_elements(type_code, chunk_size)?;
                    let remaining = count - chunk_size;
                    if remaining > 0 {
                        self.chunked = Some(ChunkedState {
                            db: self.current_db,
                            key: {
                                self.charge_bytes(key.len())?;
                                if let Some((_, work)) = &mut self.reader.remaining {
                                    limits::debit(work, key.len() as u64)?;
                                }
                                limits::copy(&key)?
                            },
                            type_code,
                            expiry_ms,
                            lru_idle_secs,
                            lfu_frequency,
                            total_count: count,
                            remaining,
                            element_offset: chunk_size as u64,
                        });
                    }
                    return Ok(RdbEntry {
                        db: self.current_db,
                        key: std::mem::take(&mut *key),
                        value,
                        type_code,
                        expiry_ms,
                        lru_idle_secs,
                        lfu_frequency,
                        total_elements: Some(count as u64),
                        element_offset: Some(0),
                    });
                }
                // count <= max: read all elements normally (inline the read)
                let value = self.read_n_elements(type_code, count)?;
                return Ok(RdbEntry {
                    db: self.current_db,
                    key: std::mem::take(&mut *key),
                    value,
                    type_code,
                    expiry_ms,
                    lru_idle_secs,
                    lfu_frequency,
                    total_elements: None,
                    element_offset: None,
                });
            }
        }

        // Non-chunked path: compact types, strings, or no max_key_elements set
        let value = self.read_value(type_code)?;
        Ok(RdbEntry {
            db: self.current_db,
            key: std::mem::take(&mut *key),
            value,
            type_code,
            expiry_ms,
            lru_idle_secs,
            lfu_frequency,
            total_elements: None,
            element_offset: None,
        })
    }

    /// Read the value for a given type code.
    /// All standard types are decoded except HASH_ZIPMAP (type 9), streams
    /// (types 15, 19, 21) which return UnknownType. Modules (type 7) are fully parsed.
    fn read_value(&mut self, type_code: u8) -> Result<RdbValue, RdbError> {
        match type_code {
            RDB_TYPE_STRING => {
                let data = self.read_string()?;
                Ok(RdbValue::String(data))
            }

            // --- Listpack-encoded types ---
            RDB_TYPE_SET_LISTPACK => {
                let blob = Zeroizing::new(self.read_string()?);
                let entries = self.compact(&blob, true)?;
                let members = compact_bytes(entries)?;
                Ok(RdbValue::Set(members))
            }

            RDB_TYPE_HASH_LISTPACK => {
                let blob = Zeroizing::new(self.read_string()?);
                let entries = self.compact(&blob, true)?;
                Ok(RdbValue::Hash(decode_hash_pairs(entries, "hash listpack")?))
            }

            RDB_TYPE_ZSET_LISTPACK => {
                let blob = Zeroizing::new(self.read_string()?);
                let entries = self.compact(&blob, true)?;
                self.zset_compact(entries, "zset listpack")
            }

            // --- Ziplist-encoded types ---
            RDB_TYPE_LIST_ZIPLIST => {
                let blob = Zeroizing::new(self.read_string()?);
                let entries = self.compact(&blob, false)?;
                let elements = compact_bytes(entries)?;
                Ok(RdbValue::List(elements))
            }

            RDB_TYPE_HASH_ZIPLIST => {
                let blob = Zeroizing::new(self.read_string()?);
                let entries = self.compact(&blob, false)?;
                Ok(RdbValue::Hash(decode_hash_pairs(entries, "hash ziplist")?))
            }

            RDB_TYPE_ZSET_ZIPLIST => {
                let blob = Zeroizing::new(self.read_string()?);
                let entries = self.compact(&blob, false)?;
                self.zset_compact(entries, "zset ziplist")
            }

            // --- Intset: sorted integer set as a single blob ---
            RDB_TYPE_SET_INTSET => {
                let blob = Zeroizing::new(self.read_string()?);
                self.charge_elements(blob.len())?;
                let members = crate::intset::decode(&blob)?;
                Ok(RdbValue::Set(members))
            }

            // --- Zipmap: legacy hash encoding, skip for now ---
            RDB_TYPE_HASH_ZIPMAP => {
                let _blob = self.read_string()?;
                Err(RdbError::UnknownType(type_code))
            }

            // --- Plain collection types: read count, delegate to read_n_elements ---
            RDB_TYPE_HASH | RDB_TYPE_HASH_2 | RDB_TYPE_LIST | RDB_TYPE_SET
            | RDB_TYPE_ZSET | RDB_TYPE_ZSET_2 => {
                let count = Self::len_to_usize(self.read_length_value()?, "element count")?;
                self.read_n_elements(type_code, count)
            }

            // --- Quicklist v1: N ziplist nodes ---
            RDB_TYPE_LIST_QUICKLIST => {
                let count = Self::len_to_usize(self.read_length_value()?, "element count")?;
                // count is node count, not element count; cap initial capacity
                self.charge_elements(count)?;
                let mut elements = Zeroizing::new(limits::vector(count.min(1024))?);
                for _ in 0..count {
                    let node = Zeroizing::new(self.read_string()?);
                    let entries = self.compact(&node, false)?;
                    for entry in entries {
                        limits::push(&mut elements, entry.into_bytes())?;
                    }
                }
                Ok(RdbValue::List(std::mem::take(&mut *elements)))
            }

            // --- Quicklist v2: N (container_type, node_blob) pairs ---
            // Container 1 = PLAIN (raw single element), 2 = PACKED (listpack blob)
            RDB_TYPE_LIST_QUICKLIST_2 => {
                let count = Self::len_to_usize(self.read_length_value()?, "element count")?;
                // count is node count, not element count; cap initial capacity
                self.charge_elements(count)?;
                let mut elements = Zeroizing::new(limits::vector(count.min(1024))?);
                for _ in 0..count {
                    let container = self.read_length_value()?;
                    let mut node = Zeroizing::new(self.read_string()?);
                    match container {
                        1 => {
                            // PLAIN: the blob is a single raw element
                            limits::push(&mut elements, std::mem::take(&mut *node))?;
                        }
                        2 => {
                            // PACKED: the blob is a listpack
                            let entries = self.compact(&node, true)?;
                            for entry in entries {
                                limits::push(&mut elements, entry.into_bytes())?;
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
                Ok(RdbValue::List(std::mem::take(&mut *elements)))
            }

            // --- Stream types: decode listpacks into stream entries ---
            RDB_TYPE_STREAM_LISTPACKS | RDB_TYPE_STREAM_LISTPACKS_2
            | RDB_TYPE_STREAM_LISTPACKS_3 => {
                self.read_stream(type_code)
            }

            // --- Module types: parse the opcode stream ---
            RDB_TYPE_MODULE_2 => {
                let module_id = self.read_length_value()?;
                let (module_name, module_version) = decode_module_id(module_id);
                let values = self.read_module_data()?;
                Ok(RdbValue::Module(ModuleData {
                    module_name,
                    module_version,
                    values,
                }))
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

    /// Read a stream value, decoding each listpack as its bytes arrive so
    /// only one raw blob is resident at a time.
    ///
    /// Consumer group metadata is consumed but not included in the output
    /// (it is operational state, not data).
    ///
    /// A decode error is captured and deferred until after the entire
    /// stream payload has been drained — returning early would leave the
    /// parser misaligned for subsequent keys. I/O-level errors from
    /// `read_string` / `read_length_value` are still fatal immediately
    /// since the stream is already unrecoverable at that point.
    fn read_stream(&mut self, type_code: u8) -> Result<RdbValue, RdbError> {
        let num_listpacks =
            Self::len_to_usize(self.read_length_value()?, "stream listpack count")?;

        let mut all_entries: Vec<StreamEntry> = Vec::new();
        let mut deferred_err: Option<RdbError> = None;

        for _ in 0..num_listpacks {
            let master_id_bytes = self.read_string()?;
            let lp_data = self.read_string()?;

            // Once we've captured a decode error, keep consuming I/O to
            // stay byte-aligned but don't bother decoding further. The
            // blobs drop at end of scope, so memory stays bounded.
            if deferred_err.is_some() {
                continue;
            }

            if master_id_bytes.len() != 16 {
                deferred_err = Some(RdbError::CorruptData(format!(
                    "stream master ID is {} bytes, expected exactly 16",
                    master_id_bytes.len()
                )));
                continue;
            }
            let ms = u64::from_be_bytes(master_id_bytes[..8].try_into().unwrap());
            let seq = u64::from_be_bytes(master_id_bytes[8..16].try_into().unwrap());

            let budget = crate::stream::MAX_STREAM_ENTRIES.saturating_sub(all_entries.len());
            match crate::stream::decode_stream_listpack(ms, seq, &lp_data, budget) {
                Ok(entries) => all_entries.extend(entries),
                Err(e) => deferred_err = Some(e),
            }
            // `master_id_bytes` and `lp_data` drop here — next iteration
            // starts with only `all_entries` and accumulated deferred state.
        }

        let length = self.read_length_value()?;
        let last_id_ms = self.read_length_value()?;
        let last_id_seq = self.read_length_value()?;
        let last_id = format!("{last_id_ms}-{last_id_seq}");

        if type_code >= RDB_TYPE_STREAM_LISTPACKS_2 {
            let _first_id_ms = self.read_length_value()?;
            let _first_id_seq = self.read_length_value()?;
            let _max_deleted_id_ms = self.read_length_value()?;
            let _max_deleted_id_seq = self.read_length_value()?;
            let _entries_added = self.read_length_value()?;
        }

        self.skip_stream_consumer_groups(type_code)?;

        if let Some(e) = deferred_err {
            return Err(e);
        }

        // Cross-check the RDB-reported length against the decoded
        // non-deleted entry count. A mismatch means either the stream
        // metadata is wrong or a listpack silently under/over-delivered
        // rows — both corrupt. Without this, a tampered file could
        // export successfully while `StreamData.length` (and the Arrow
        // `num_elements` column) claims a different cardinality than
        // the exported rows.
        let decoded = all_entries.len() as u64;
        if length != decoded {
            return Err(RdbError::CorruptData(format!(
                "stream length metadata ({length}) does not match decoded entry count ({decoded})"
            )));
        }

        Ok(RdbValue::Stream(StreamData {
            entries: all_entries,
            length,
            last_id,
        }))
    }

    /// Consume stream consumer group data without decoding.
    fn skip_stream_consumer_groups(&mut self, type_code: u8) -> Result<(), RdbError> {
        let num_cgroups = Self::len_to_usize(self.read_length_value()?, "stream cgroup count")?;
        for _ in 0..num_cgroups {
            let _name = self.read_string()?;
            let _last_id_ms = self.read_length_value()?;
            let _last_id_seq = self.read_length_value()?;

            if type_code >= RDB_TYPE_STREAM_LISTPACKS_2 {
                let _entries_read = self.read_length_value()?;
            }

            let num_pel = Self::len_to_usize(self.read_length_value()?, "stream PEL count")?;
            for _ in 0..num_pel {
                let _id = self.read_exact_vec(16)?;
                let _delivery_time = self.read_i64_le()?;
                let _delivery_count = self.read_length_value()?;
            }

            let num_consumers = Self::len_to_usize(self.read_length_value()?, "stream consumer count")?;
            for _ in 0..num_consumers {
                let _consumer_name = self.read_string()?;
                let _seen_time = self.read_i64_le()?;

                if type_code >= RDB_TYPE_STREAM_LISTPACKS_3 {
                    let _active_time = self.read_i64_le()?;
                }

                let consumer_pel = Self::len_to_usize(self.read_length_value()?, "stream consumer PEL count")?;
                for _ in 0..consumer_pel {
                    let _id = self.read_exact_vec(16)?;
                }
            }
        }
        Ok(())
    }

    /// Read exactly `n` elements for a plain-encoded type, returning the
    /// appropriate `RdbValue`.
    fn read_n_elements(&mut self, type_code: u8, n: usize) -> Result<RdbValue, RdbError> {
        self.charge_elements(n)?;
        let cap = n.min(CAPACITY_HINT_MAX);
        if self.strict.is_some() && matches!(type_code, RDB_TYPE_ZSET | RDB_TYPE_ZSET_2) {
            let mut out = Zeroizing::new(limits::vector(n)?);
            for _ in 0..n {
                let mut member = Zeroizing::new(self.read_string()?);
                let score = if type_code == RDB_TYPE_ZSET_2 {
                    self.read_exact_vec(8)?
                } else {
                    let length = self.read_u8()?;
                    match length {
                        253 => limits::copy(b"nan")?,
                        254 => limits::copy(b"+inf")?,
                        255 => limits::copy(b"-inf")?,
                        length => self.read_exact_vec(length as usize)?,
                    }
                };
                out.push((std::mem::take(&mut *member), score));
            }
            return Ok(RdbValue::SortedSetBytes(std::mem::take(&mut *out)));
        }
        match type_code {
            RDB_TYPE_LIST => {
                let mut elements = Zeroizing::new(limits::vector(cap)?);
                for _ in 0..n {
                    elements.push(self.read_string()?);
                }
                Ok(RdbValue::List(std::mem::take(&mut *elements)))
            }
            RDB_TYPE_SET => {
                let mut members = Zeroizing::new(limits::vector(cap)?);
                for _ in 0..n {
                    members.push(self.read_string()?);
                }
                Ok(RdbValue::Set(std::mem::take(&mut *members)))
            }
            RDB_TYPE_ZSET => {
                let mut pairs = Zeroizing::new(limits::vector(cap)?);
                for _ in 0..n {
                    let mut member = Zeroizing::new(self.read_string()?);
                    let score = self.read_double_value()?;
                    pairs.push((std::mem::take(&mut *member), score));
                }
                Ok(RdbValue::SortedSet(std::mem::take(&mut *pairs)))
            }
            RDB_TYPE_HASH => {
                let mut fields = Zeroizing::new(limits::vector(cap)?);
                for _ in 0..n {
                    let mut field = Zeroizing::new(self.read_string()?);
                    let mut value = Zeroizing::new(self.read_string()?);
                    fields.push(HashField {
                        field: std::mem::take(&mut *field),
                        value: std::mem::take(&mut *value),
                        expiry_ms: None,
                    });
                }
                Ok(RdbValue::Hash(std::mem::take(&mut *fields)))
            }
            RDB_TYPE_ZSET_2 => {
                let mut pairs = Zeroizing::new(limits::vector(cap)?);
                for _ in 0..n {
                    let mut member = Zeroizing::new(self.read_string()?);
                    let mut score_buf = [0u8; 8];
                    self.reader.read_exact(&mut score_buf)?;
                    let score = f64::from_le_bytes(score_buf);
                    pairs.push((std::mem::take(&mut *member), score));
                }
                Ok(RdbValue::SortedSet(std::mem::take(&mut *pairs)))
            }
            RDB_TYPE_HASH_2 => {
                let mut fields = Zeroizing::new(limits::vector(cap)?);
                for _ in 0..n {
                    let mut field = Zeroizing::new(self.read_string()?);
                    let mut value = Zeroizing::new(self.read_string()?);
                    let ttl = self.read_i64_le()?;
                    fields.push(HashField {
                        field: std::mem::take(&mut *field),
                        value: std::mem::take(&mut *value),
                        expiry_ms: if ttl == -1 { None } else { Some(ttl) },
                    });
                }
                Ok(RdbValue::Hash(std::mem::take(&mut *fields)))
            }
            _ => Err(RdbError::CorruptData(format!(
                "read_n_elements called with non-plain type code {}",
                type_code
            ))),
        }
    }

    /// Continue reading the next chunk of a large key being split.
    fn read_chunk(&mut self) -> Result<RdbEntry, RdbError> {
        let state = self.chunked.as_mut().ok_or_else(|| {
            RdbError::CorruptData("read_chunk called without chunked state".into())
        })?;
        let max = self.max_key_elements.ok_or_else(|| {
            RdbError::CorruptData("chunked state without max_key_elements".into())
        })?;
        let chunk_size = max.min(state.remaining);
        let element_offset = state.element_offset;

        // Extract what we need before the mutable borrow in read_n_elements
        let type_code = state.type_code;
        let db = state.db;
        if let Some(limits) = &mut self.strict {
            limits::debit(&mut limits.allocated_bytes, state.key.len() as u64)?;
        }
        if let Some((_, work)) = &mut self.reader.remaining {
            limits::debit(work, state.key.len() as u64)?;
        }
        let mut key = Zeroizing::new(limits::copy(&state.key)?);
        let expiry_ms = state.expiry_ms;
        let lru_idle_secs = state.lru_idle_secs;
        let lfu_frequency = state.lfu_frequency;
        let total_count = state.total_count;

        let value = self.read_n_elements(type_code, chunk_size)?;

        let state = self.chunked.as_mut().ok_or_else(|| {
            RdbError::CorruptData("internal: chunked state missing after read_n_elements".into())
        })?;
        state.remaining -= chunk_size;
        state.element_offset += chunk_size as u64;
        if state.remaining == 0 {
            self.chunked = None;
        }

        Ok(RdbEntry {
            db,
            key: std::mem::take(&mut *key),
            value,
            type_code,
            expiry_ms,
            lru_idle_secs,
            lfu_frequency,
            total_elements: Some(total_count as u64),
            element_offset: Some(element_offset),
        })
    }
}

/// Returns true if the type code is a plain-encoded type that can be chunked.
fn is_plain_type(type_code: u8) -> bool {
    matches!(
        type_code,
        RDB_TYPE_LIST | RDB_TYPE_SET | RDB_TYPE_ZSET | RDB_TYPE_HASH | RDB_TYPE_ZSET_2 | RDB_TYPE_HASH_2
    )
}

/// Iterator implementation — yields one RdbEntry per key-value pair.
impl<R: Read> Iterator for RdbReader<R> {
    type Item = Result<RdbEntry, RdbError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let (Some(limits), Some((allocated, elements))) = (&mut self.strict, self.entry_limits) {
            // Prior outputs are owned by the caller. The parser retains only
            // bounded AUX plus one chunk key between iterator calls.
            limits.allocated_bytes = allocated;
            limits.elements = elements;
            if let Some(chunk) = &self.chunked {
                if limits::debit(&mut limits.allocated_bytes, chunk.key.len() as u64).is_err() {
                    self.finished = true;
                    return Some(Err(RdbError::ResourceLimit));
                }
            }
        }
        if self.finished {
            return None;
        }

        match self.next_inner() {
            // Normal entry or EOF
            Ok(entry) => entry.map(Ok),
            // UnknownType after consuming bytes: recoverable, iterator continues
            Err(RdbError::UnknownType(t)) => {
                if self.strict.is_some() {
                    self.finished = true;
                }
                Some(Err(RdbError::UnknownType(t)))
            }
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
        // If we're mid-key in a chunked read, continue reading elements.
        if self.chunked.is_some() {
            return self.read_chunk().map(Some);
        }

        loop {
            let byte = if let Some(b) = self.preamble_byte.take() {
                b
            } else {
                self.read_u8()?
            };

            if self.strict.is_some()
                && !matches!(byte,
                RDB_OPCODE_AUX | RDB_OPCODE_SELECTDB | RDB_OPCODE_RESIZEDB |
                RDB_OPCODE_EXPIRETIME_MS | RDB_OPCODE_EXPIRETIME | RDB_OPCODE_IDLE |
                RDB_OPCODE_FREQ | RDB_OPCODE_EOF | 0..=5 | 10..=14 | 16..=18 | 20)
            {
                return Err(RdbError::UnsupportedProfile(byte));
            }
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
                    let ms = (secs as i64).checked_mul(1000).ok_or_else(|| {
                        RdbError::CorruptData(format!(
                            "EXPIRETIME overflow: {secs} seconds"
                        ))
                    })?;
                    self.pending_expiry_ms = Some(ms);
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
                    // Read through the bounded reader so the trailer counts toward the
                    // strict input limit (the CRC was already captured above).
                    self.reader.read_exact(&mut crc_buf)?;
                    let stored_crc = u64::from_le_bytes(crc_buf);

                    // Validate if version >= 5 and stored checksum is non-zero
                    // (zero means checksumming was disabled at save time)
                    if self.header.version >= 5 && stored_crc != 0 {
                        if computed_crc != stored_crc {
                            return Err(RdbError::CorruptData(format!(
                                "CRC64 mismatch: computed 0x{:016x}, stored 0x{:016x}",
                                computed_crc, stored_crc
                            )));
                        }
                        self.crc_checked = true;
                    }

                    if self.strict.is_some() {
                        let mut trailing = [0u8; 1];
                        // The upstream source must be an exact bounded immutable snapshot.
                        if self.reader.inner.read(&mut trailing)? != 0 {
                            trailing.zeroize();
                            return Err(RdbError::CorruptData(
                                "trailing bytes after EOF".into(),
                            ));
                        }
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
    let mut fields = Zeroizing::new(limits::vector(entries.len() / 2)?);
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
    Ok(std::mem::take(&mut *fields))
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
    let mut pairs = Zeroizing::new(limits::vector(entries.len() / 2)?);
    let mut iter = entries.into_iter();
    while let Some(member) = iter.next() {
        let score_entry = iter.next().ok_or_else(|| {
            RdbError::CorruptData(format!("{ctx}: missing score for member"))
        })?;
        let score = score_entry.to_f64()?;
        pairs.push((member.into_bytes(), score));
    }
    Ok(std::mem::take(&mut *pairs))
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

fn compact_bytes(entries: Vec<crate::compact::CompactEntry>) -> Result<Vec<Vec<u8>>, RdbError> {
    let mut out = Zeroizing::new(limits::vector(entries.len())?);
    for entry in entries {
        out.push(entry.into_bytes());
    }
    Ok(std::mem::take(&mut *out))
}

impl Drop for ChunkedState {
    fn drop(&mut self) {
        self.key.zeroize();
    }
}


// =============================================================================
// Tests
// =============================================================================


#[cfg(test)]
#[path = "reader_tests.rs"]
mod tests;
