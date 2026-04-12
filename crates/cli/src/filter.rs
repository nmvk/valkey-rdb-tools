use std::collections::HashSet;

use rdb_parser::RdbEntry;
use rdb_to_arrow::{is_geo_entry, Heuristic, TypeTag};

/// Filters RDB entries by database, type tags, and key glob pattern.
pub struct EntryFilter {
    pub db: Option<u32>,
    pub type_tags: Option<HashSet<TypeTag>>,
    pub key_pattern: Option<String>,
    pub heuristics: HashSet<Heuristic>,
}

impl EntryFilter {
    pub fn matches(&self, entry: &RdbEntry) -> bool {
        if let Some(db) = self.db {
            if entry.db != db {
                return false;
            }
        }

        if let Some(ref tags) = self.type_tags {
            if !self.matches_any_type(entry, tags) {
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

    /// Check if the entry will produce output for any of the requested type tags.
    /// A sorted set with geo scores produces both SortedSet and Geo output (additive).
    fn matches_any_type(&self, entry: &RdbEntry, tags: &HashSet<TypeTag>) -> bool {
        let primary = match rdb_to_arrow::type_tag_for(entry) {
            Some(t) => t,
            None => return false,
        };
        if tags.contains(&primary) {
            return true;
        }
        // A sorted set that looks like geo also matches TypeTag::Geo (only when heuristic is active)
        if self.heuristics.contains(&Heuristic::Geo)
            && primary == TypeTag::SortedSet
            && tags.contains(&TypeTag::Geo)
            && is_geo_entry(entry)
        {
            return true;
        }
        false
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
