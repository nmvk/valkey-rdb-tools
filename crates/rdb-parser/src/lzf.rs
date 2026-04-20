// LZF decompression — pure function, no RdbReader dependency.
//
// Reference: valkey/src/lzf_d.c

use crate::types::RdbError;

/// Maximum LZF decompression ratio (`expected_len / compressed.len()`).
/// Prevents a small compressed blob from claiming a huge output buffer.
const LZF_MAX_RATIO: usize = 1024;

/// Decompress LZF-compressed data.
///
/// LZF is a simple byte-level compressor. Format:
/// - If first byte high bit = 0: literal run (length = byte + 1, copy N bytes)
/// - If first byte high 3 bits != 0b111: short back-reference
///   (length = (byte >> 5) + 2, offset from high bits + next byte)
/// - If first byte high 3 bits == 0b111: long back-reference
///   (length = next byte + 9, offset from remaining bits + byte after)
pub(crate) fn decompress(compressed: &[u8], expected_len: usize) -> Result<Vec<u8>, RdbError> {
    // Guard against a tiny compressed blob claiming a disproportionately large output
    if !compressed.is_empty() && expected_len / compressed.len() > LZF_MAX_RATIO {
        return Err(RdbError::CorruptData(format!(
            "LZF decompression ratio too high: {} -> {} (max {}x)",
            compressed.len(),
            expected_len,
            LZF_MAX_RATIO
        )));
    }
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
            if offset >= len {
                // Non-overlapping back-reference: the source bytes are
                // fully written before the copy begins, so a bulk memcpy
                // via `extend_from_within` is both correct and faster
                // than the per-byte loop.
                output.extend_from_within(start..start + len);
            } else {
                // Overlapping (self-referential) back-reference: LZF
                // permits emitting a run like `offset=1, len=N` to
                // replicate a single byte N times. Each iteration reads
                // a byte that was just pushed, so the copy must be
                // byte-by-byte with the in-flight vector state.
                for j in 0..len {
                    let byte = output[start + j];
                    output.push(byte);
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_only() {
        // "abc": 3 literal bytes, ctrl = 2 (len-1)
        let compressed = [2u8, b'a', b'b', b'c'];
        let result = decompress(&compressed, 3).unwrap();
        assert_eq!(result, b"abc");
    }

    #[test]
    fn with_backref() {
        // "aaaa": literal "a" (ctrl=0, 'a'), then backref offset=1, len=3
        // backref: len=3 means stored as 1 (len-2), offset=0 (1-based → stored as 0)
        // ctrl byte: (1 << 5) | 0 = 0x20, offset low byte = 0x00
        let compressed = [0u8, b'a', 0x20, 0x00];
        let result = decompress(&compressed, 4).unwrap();
        assert_eq!(result, b"aaaa");
    }

    #[test]
    fn output_overflow_literal() {
        // "abc" but expected is 2
        let compressed = [2u8, b'a', b'b', b'c'];
        assert!(decompress(&compressed, 2).is_err());
    }

    #[test]
    fn output_overflow_backref() {
        // "a" literal + backref that would produce 4 total, but expected is 2
        let compressed = [0u8, b'a', 0x20, 0x00];
        assert!(decompress(&compressed, 2).is_err());
    }

    #[test]
    fn ratio_cap() {
        // 5-byte compressed claiming 512MB output
        let compressed = [0u8; 5];
        assert!(decompress(&compressed, 512 * 1024 * 1024).is_err());
    }

    #[test]
    fn backref_non_overlapping_fast_path() {
        // Literal "abcd" (ctrl=3, 4 bytes) followed by backref offset=4, len=3
        // → output becomes "abcdabc". offset (4) >= len (3) so the fast path
        // `extend_from_within` is used. This test pins the non-overlapping
        // branch behavior so the optimization can't regress into an
        // incorrect bulk-copy for the overlapping case.
        let compressed = [3u8, b'a', b'b', b'c', b'd', 0x20, 3];
        let result = decompress(&compressed, 7).unwrap();
        assert_eq!(result, b"abcdabc");
    }

    #[test]
    fn backref_overlapping_slow_path() {
        // Literal "a" + backref offset=1 len=5 → "aaaaaa" (self-referential
        // run repeating the single byte). Exercises the overlapping branch
        // where each copied byte is a byte just pushed.
        let compressed = [0u8, b'a', (3 << 5), 0x00];
        let result = decompress(&compressed, 6).unwrap();
        assert_eq!(result, b"aaaaaa");
    }

    #[test]
    fn backref_offset_equals_len_uses_fast_path() {
        // offset == len is the exact boundary between fast and slow paths
        // (the fast `extend_from_within` branch requires `offset >= len`).
        // LZF short-match minimum length is 3, so pick offset = len = 3.
        // Literal "abc" (ctrl=2, 3 bytes) then backref ctrl=0x20, off_lo=2
        // — stored offset 2 decodes to actual offset 3, and `ctrl >> 5 = 1`
        // decodes to length 3. Output: "abcabc".
        let compressed = [2u8, b'a', b'b', b'c', 0x20, 2];
        let result = decompress(&compressed, 6).unwrap();
        assert_eq!(result, b"abcabc");
    }
}
