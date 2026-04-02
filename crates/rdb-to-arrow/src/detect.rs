//! Virtual type detection for Geo and HyperLogLog.
//!
//! Valkey stores Geo data as sorted sets (scores are 52-bit geohashes) and
//! HyperLogLog as strings (with a `HYLL` magic header). These functions detect
//! these virtual types so they can be given dedicated schemas with decoded columns.
//!
//! Ported from Valkey `src/geohash.c` and `src/hyperloglog.c`.

// ---------------------------------------------------------------------------
// HyperLogLog detection
// ---------------------------------------------------------------------------

/// Returns true if `data` looks like a HyperLogLog value.
///
/// Checks for the `HYLL` magic header, a valid encoding byte (0=dense, 1=sparse),
/// and minimum 16-byte header size. Zero false positives in practice.
pub fn is_hll(data: &[u8]) -> bool {
    data.len() >= 16 && &data[0..4] == b"HYLL" && matches!(data[4], 0 | 1)
}

/// Returns the HLL encoding name: `"dense"` or `"sparse"`.
///
/// Returns `"unknown"` if data is too short to read the encoding byte.
pub fn hll_encoding(data: &[u8]) -> &'static str {
    match data.get(4) {
        Some(0) => "dense",
        Some(1) => "sparse",
        _ => "unknown",
    }
}

/// Returns the cached cardinality from an HLL header, or -1 if the cache is
/// invalid or the data is too short.
///
/// The cardinality is a little-endian u64 at bytes 8..16. The MSB of byte 15
/// signals cache invalidity (set = invalid).
pub fn hll_cached_cardinality(data: &[u8]) -> i64 {
    let Some(slice) = data.get(8..16) else {
        return -1;
    };
    let bytes: [u8; 8] = slice.try_into().expect("slice is exactly 8 bytes");
    let raw = u64::from_le_bytes(bytes);
    // MSB set means the cache is invalid.
    if raw & (1u64 << 63) != 0 {
        -1
    } else {
        raw as i64
    }
}

// ---------------------------------------------------------------------------
// Geo detection
// ---------------------------------------------------------------------------

/// Decodes a Valkey geohash score to `(longitude, latitude)`.
///
/// Returns `None` if the score is not a valid 52-bit geohash (non-negative integer
/// that fits in 52 bits). Valkey stores geo coordinates as sorted-set scores using
/// `geohashAlign52Bits` from `geohash.c`.
pub fn geohash_decode(score: f64) -> Option<(f64, f64)> {
    // Must be a non-negative integer that fits in 52 bits.
    if score.is_nan() || score < 0.0 || score.fract() != 0.0 || score >= (1u64 << 52) as f64 {
        return None;
    }

    let bits = score as u64;
    let hash_sep = deinterleave64(bits);
    let ilato = hash_sep as u32;
    let ilono = (hash_sep >> 32) as u32;

    const STEP: u32 = 26;
    const LAT_MIN: f64 = -85.05112878;
    const LAT_MAX: f64 = 85.05112878;
    const LONG_MIN: f64 = -180.0;
    const LONG_MAX: f64 = 180.0;

    let lat_scale = LAT_MAX - LAT_MIN;
    let long_scale = LONG_MAX - LONG_MIN;
    let divisor = (1u64 << STEP) as f64;

    let lat_min = LAT_MIN + (ilato as f64 / divisor) * lat_scale;
    let lat_max = LAT_MIN + ((ilato as f64 + 1.0) / divisor) * lat_scale;
    let long_min = LONG_MIN + (ilono as f64 / divisor) * long_scale;
    let long_max = LONG_MIN + ((ilono as f64 + 1.0) / divisor) * long_scale;

    Some(((long_min + long_max) / 2.0, (lat_min + lat_max) / 2.0))
}

/// Returns true if a sorted set looks like a Geo set.
///
/// Requires non-empty AND every member's score must be a valid 52-bit geohash.
/// False positive rate is near-zero because normal sorted sets rarely have all
/// scores as non-negative integers in [0, 2^52).
pub fn is_geo(members: &[(Vec<u8>, f64)]) -> bool {
    !members.is_empty() && members.iter().all(|(_, score)| geohash_decode(*score).is_some())
}

/// Deinterleave a 64-bit value into separate even/odd bit components.
///
/// Returns `lat_bits | (lon_bits << 32)` where lat_bits come from even bit
/// positions and lon_bits from odd bit positions of the input.
///
/// Ported from Valkey `src/geohash.c` `deinterleave64`.
fn deinterleave64(interleaved: u64) -> u64 {
    const B: [u64; 6] = [
        0x5555555555555555,
        0x3333333333333333,
        0x0F0F0F0F0F0F0F0F,
        0x00FF00FF00FF00FF,
        0x0000FFFF0000FFFF,
        0x00000000FFFFFFFF,
    ];
    const S: [u32; 6] = [0, 1, 2, 4, 8, 16];

    let mut x = interleaved;
    let mut y = interleaved >> 1;

    x = (x | (x >> S[0])) & B[0];
    y = (y | (y >> S[0])) & B[0];

    x = (x | (x >> S[1])) & B[1];
    y = (y | (y >> S[1])) & B[1];

    x = (x | (x >> S[2])) & B[2];
    y = (y | (y >> S[2])) & B[2];

    x = (x | (x >> S[3])) & B[3];
    y = (y | (y >> S[3])) & B[3];

    x = (x | (x >> S[4])) & B[4];
    y = (y | (y >> S[4])) & B[4];

    x = (x | (x >> S[5])) & B[5];
    y = (y | (y >> S[5])) & B[5];

    x | (y << 32)
}

// ---------------------------------------------------------------------------
// Test helpers shared across crate tests (detect, builders)
// ---------------------------------------------------------------------------

#[cfg(test)]
fn interleave64(x: u32, y: u32) -> u64 {
    let mut x = x as u64;
    let mut y = y as u64;
    x = (x | (x << 16)) & 0x0000FFFF0000FFFF;
    y = (y | (y << 16)) & 0x0000FFFF0000FFFF;
    x = (x | (x << 8)) & 0x00FF00FF00FF00FF;
    y = (y | (y << 8)) & 0x00FF00FF00FF00FF;
    x = (x | (x << 4)) & 0x0F0F0F0F0F0F0F0F;
    y = (y | (y << 4)) & 0x0F0F0F0F0F0F0F0F;
    x = (x | (x << 2)) & 0x3333333333333333;
    y = (y | (y << 2)) & 0x3333333333333333;
    x = (x | (x << 1)) & 0x5555555555555555;
    y = (y | (y << 1)) & 0x5555555555555555;
    x | (y << 1)
}

/// Encode (longitude, latitude) to a Valkey geohash score (52-bit, step=26).
#[cfg(test)]
pub(crate) fn encode_geohash(lng: f64, lat: f64) -> f64 {
    const LAT_MIN: f64 = -85.05112878;
    const LAT_MAX: f64 = 85.05112878;
    const LONG_MIN: f64 = -180.0;
    const LONG_MAX: f64 = 180.0;
    const STEP: u32 = 26;
    let lat_norm = (lat - LAT_MIN) / (LAT_MAX - LAT_MIN);
    let lng_norm = (lng - LONG_MIN) / (LONG_MAX - LONG_MIN);
    let lat_bits = (lat_norm * (1u64 << STEP) as f64) as u32;
    let lng_bits = (lng_norm * (1u64 << STEP) as f64) as u32;
    interleave64(lat_bits, lng_bits) as f64
}

/// Build a synthetic HLL value for testing.
#[cfg(test)]
pub(crate) fn make_hll(encoding: u8, cardinality: u64) -> Vec<u8> {
    let mut data = vec![0u8; 16];
    data[0..4].copy_from_slice(b"HYLL");
    data[4] = encoding;
    data[8..16].copy_from_slice(&cardinality.to_le_bytes());
    data
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_hll_valid_dense() {
        let data = make_hll(0, 42);
        assert!(is_hll(&data));
    }

    #[test]
    fn is_hll_valid_sparse() {
        let data = make_hll(1, 100);
        assert!(is_hll(&data));
    }

    #[test]
    fn is_hll_wrong_magic() {
        let mut data = make_hll(0, 0);
        data[0] = b'X';
        assert!(!is_hll(&data));
    }

    #[test]
    fn is_hll_invalid_encoding_byte() {
        let mut data = make_hll(0, 0);
        data[4] = 2; // not 0 or 1
        assert!(!is_hll(&data));
    }

    #[test]
    fn is_hll_too_short() {
        assert!(!is_hll(b"HYLL"));
        assert!(!is_hll(&[]));
        assert!(!is_hll(&[b'H', b'Y', b'L', b'L', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])); // 15 bytes
    }

    #[test]
    fn is_hll_rejects_regular_string() {
        assert!(!is_hll(b"hello world"));
        assert!(!is_hll(b"HYLL but invalid encoding\x05"));
    }

    #[test]
    fn hll_encoding_dense() {
        let data = make_hll(0, 0);
        assert_eq!(hll_encoding(&data), "dense");
    }

    #[test]
    fn hll_encoding_sparse() {
        let data = make_hll(1, 0);
        assert_eq!(hll_encoding(&data), "sparse");
    }

    #[test]
    fn hll_cached_cardinality_valid() {
        let data = make_hll(0, 12345);
        assert_eq!(hll_cached_cardinality(&data), 12345);
    }

    #[test]
    fn hll_cached_cardinality_zero() {
        let data = make_hll(0, 0);
        assert_eq!(hll_cached_cardinality(&data), 0);
    }

    #[test]
    fn hll_cached_cardinality_invalid_cache() {
        // Set MSB to indicate cache is invalid.
        let data = make_hll(0, 1u64 << 63);
        assert_eq!(hll_cached_cardinality(&data), -1);
    }

    #[test]
    fn hll_cached_cardinality_invalid_cache_with_value() {
        // MSB set but lower bits have a value — still invalid.
        let data = make_hll(0, (1u64 << 63) | 999);
        assert_eq!(hll_cached_cardinality(&data), -1);
    }

    // -----------------------------------------------------------------------
    // Geo / geohash tests
    // -----------------------------------------------------------------------

    #[test]
    fn deinterleave_interleave_roundtrip() {
        let lat_bits: u32 = 50065025;
        let lng_bits: u32 = 35880437;
        let interleaved = interleave64(lat_bits, lng_bits);
        let result = deinterleave64(interleaved);
        assert_eq!(result as u32, lat_bits);
        assert_eq!((result >> 32) as u32, lng_bits);
    }

    #[test]
    fn geohash_decode_rome() {
        // Rome: 41.8902 N, 12.4923 E
        let score = encode_geohash(12.4923, 41.8902);
        let (lng, lat) = geohash_decode(score).expect("valid geohash");
        // Precision: ~0.00001 degrees (~1 meter)
        assert!((lat - 41.8902).abs() < 0.001, "lat={lat}");
        assert!((lng - 12.4923).abs() < 0.001, "lng={lng}");
    }

    #[test]
    fn geohash_decode_san_francisco() {
        // San Francisco: 37.7749 N, -122.4194 W
        let score = encode_geohash(-122.4194, 37.7749);
        let (lng, lat) = geohash_decode(score).expect("valid geohash");
        assert!((lat - 37.7749).abs() < 0.001, "lat={lat}");
        assert!((lng - (-122.4194)).abs() < 0.001, "lng={lng}");
    }

    #[test]
    fn geohash_decode_origin() {
        // Near (0, 0) — Gulf of Guinea
        let score = encode_geohash(0.0, 0.0);
        let (lng, lat) = geohash_decode(score).expect("valid geohash");
        assert!(lat.abs() < 0.001, "lat={lat}");
        assert!(lng.abs() < 0.001, "lng={lng}");
    }

    #[test]
    fn geohash_decode_negative_coords() {
        // Sydney: -33.8688 S, 151.2093 E
        let score = encode_geohash(151.2093, -33.8688);
        let (lng, lat) = geohash_decode(score).expect("valid geohash");
        assert!((lat - (-33.8688)).abs() < 0.001, "lat={lat}");
        assert!((lng - 151.2093).abs() < 0.001, "lng={lng}");
    }

    #[test]
    fn geohash_decode_rejects_negative() {
        assert!(geohash_decode(-1.0).is_none());
    }

    #[test]
    fn geohash_decode_rejects_fractional() {
        assert!(geohash_decode(1.5).is_none());
    }

    #[test]
    fn geohash_decode_rejects_too_large() {
        assert!(geohash_decode((1u64 << 52) as f64).is_none());
    }

    #[test]
    fn geohash_decode_rejects_nan() {
        assert!(geohash_decode(f64::NAN).is_none());
    }

    #[test]
    fn geohash_decode_rejects_infinity() {
        assert!(geohash_decode(f64::INFINITY).is_none());
        assert!(geohash_decode(f64::NEG_INFINITY).is_none());
    }

    #[test]
    fn geohash_decode_zero_is_valid() {
        // 0 is a valid geohash (near lat_min, lng_min corner).
        assert!(geohash_decode(0.0).is_some());
    }

    #[test]
    fn is_geo_valid_set() {
        let rome = encode_geohash(12.4923, 41.8902);
        let sf = encode_geohash(-122.4194, 37.7749);
        let members = vec![
            (b"Rome".to_vec(), rome),
            (b"San Francisco".to_vec(), sf),
        ];
        assert!(is_geo(&members));
    }

    #[test]
    fn is_geo_empty_set() {
        assert!(!is_geo(&[]));
    }

    #[test]
    fn is_geo_one_invalid_member() {
        let rome = encode_geohash(12.4923, 41.8902);
        let members = vec![
            (b"Rome".to_vec(), rome),
            (b"invalid".to_vec(), 1.5), // fractional → not a geohash
        ];
        assert!(!is_geo(&members));
    }

    #[test]
    fn is_geo_all_non_geohash() {
        let members = vec![
            (b"alice".to_vec(), 1.5),
            (b"bob".to_vec(), 2.7),
        ];
        assert!(!is_geo(&members));
    }
}
