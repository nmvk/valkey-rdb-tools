use rdb_parser::{RdbEntry, RdbValue};
use rdb_to_arrow::TypeTag;

/// Filters RDB entries by database, type tag, and key glob pattern.
pub struct EntryFilter {
    pub db: Option<u32>,
    pub type_tag: Option<TypeTag>,
    pub key_pattern: Option<String>,
}

impl EntryFilter {
    pub fn matches(&self, entry: &RdbEntry) -> bool {
        if let Some(db) = self.db {
            if entry.db != db {
                return false;
            }
        }

        if let Some(tag) = self.type_tag {
            if !self.matches_type(entry, tag) {
                return false;
            }
        }

        if let Some(ref pattern) = self.key_pattern {
            let key_str = String::from_utf8_lossy(&entry.key);
            if !glob_match::glob_match(pattern, &key_str) {
                return false;
            }
        }

        true
    }

    /// Check if entry matches the target type tag. Short-circuits for non-virtual
    /// types to avoid running geo/HLL detection on every entry.
    fn matches_type(&self, entry: &RdbEntry, tag: TypeTag) -> bool {
        match tag {
            // String but not HLL — need detection
            TypeTag::String => {
                matches!(rdb_to_arrow::type_tag_for(entry), Some(TypeTag::String))
            }
            TypeTag::List => matches!(entry.value, RdbValue::List(_)),
            TypeTag::Set => matches!(entry.value, RdbValue::Set(_)),
            TypeTag::SortedSet => {
                // SortedSet but not Geo — need detection
                matches!(rdb_to_arrow::type_tag_for(entry), Some(TypeTag::SortedSet))
            }
            TypeTag::Hash => matches!(entry.value, RdbValue::Hash(_)),
            // Virtual types: need full detection
            TypeTag::Geo | TypeTag::HyperLogLog => {
                rdb_to_arrow::type_tag_for(entry) == Some(tag)
            }
        }
    }
}

/// Iterator wrapper that filters entries before they reach the batcher.
pub struct FilteredEntries<I> {
    inner: I,
    filter: EntryFilter,
}

impl<I> FilteredEntries<I> {
    pub fn new(inner: I, filter: EntryFilter) -> Self {
        Self { inner, filter }
    }
}

impl<I> Iterator for FilteredEntries<I>
where
    I: Iterator<Item = Result<RdbEntry, rdb_parser::RdbError>>,
{
    type Item = Result<RdbEntry, rdb_parser::RdbError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next() {
                Some(Ok(entry)) => {
                    if self.filter.matches(&entry) {
                        return Some(Ok(entry));
                    }
                }
                other => return other,
            }
        }
    }
}
