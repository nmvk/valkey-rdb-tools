use rdb_parser::{RdbEntry, RdbValue};

/// Build an RdbEntry with sensible defaults. Tests only need to specify the value.
pub(crate) fn test_entry(key: &[u8], value: RdbValue) -> RdbEntry {
    test_entry_typed(key, value, 0)
}

/// Build an RdbEntry with an explicit type_code for testing type/encoding metadata.
pub(crate) fn test_entry_typed(key: &[u8], value: RdbValue, type_code: u8) -> RdbEntry {
    RdbEntry {
        db: 0,
        key: key.to_vec(),
        value,
        type_code,
        expiry_ms: None,
        lru_idle_secs: None,
        lfu_frequency: None,
    }
}
