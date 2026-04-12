use std::collections::HashSet;
use std::collections::VecDeque;

use arrow::array::RecordBatch;
use rdb_parser::RdbError;

use crate::builders::{
    GeoBatchBuilder, HashBatchBuilder, HllBatchBuilder, ListBatchBuilder, SetBatchBuilder,
    SortedSetBatchBuilder, StringBatchBuilder,
};
use crate::error::ArrowConvertError;
use rdb_parser::{RdbEntry, RdbValue};

use crate::schema::{type_tag_for, is_geo_entry, Heuristic, TypeTag};

/// Configuration for the Arrow batcher.
///
/// Use [`Default::default()`] or struct literal syntax with `..Default::default()`
/// to construct, so new fields added in future versions don't break your code.
#[derive(Debug, Clone)]
pub struct BatcherConfig {
    /// Approximate row threshold per RecordBatch. A flush is triggered after
    /// pushing an entry that causes a builder to reach this count. Because
    /// collection types (list, set, hash, zset) expand to one row per element,
    /// a single large collection may produce a batch larger than this value.
    pub batch_size: usize,
    /// Byte budget per builder. When a builder's accumulated variable-length
    /// data exceeds this threshold, it is flushed. `None` disables byte-budget
    /// flushing (only row-count is used).
    pub batch_bytes: Option<usize>,
    /// Skip RDB entries whose estimated in-memory size exceeds this threshold.
    /// Prevents a single large key from blowing up memory. `None` disables.
    pub max_entry_bytes: Option<usize>,
    /// Active heuristic detectors for virtual types. Default: all built-in heuristics.
    pub heuristics: HashSet<Heuristic>,
}

impl Default for BatcherConfig {
    fn default() -> Self {
        Self {
            batch_size: 65_536,
            batch_bytes: None,
            max_entry_bytes: None,
            heuristics: Heuristic::ALL.iter().copied().collect(),
        }
    }
}

/// Estimate the variable-length data size of an RDB entry (key + value payload).
fn estimate_entry_bytes(entry: &RdbEntry) -> usize {
    entry.key.len()
        + match &entry.value {
            RdbValue::String(v) => v.len(),
            RdbValue::List(elems) => elems.iter().map(|e| e.len()).sum(),
            RdbValue::Set(members) => members.iter().map(|m| m.len()).sum(),
            RdbValue::SortedSet(pairs) => pairs.iter().map(|(m, _)| m.len() + 8).sum(),
            RdbValue::Hash(fields) => fields.iter().map(|f| f.field.len() + f.value.len()).sum(),
            _ => 0,
        }
}

/// A RecordBatch tagged with its logical type.
#[derive(Debug)]
pub struct TypedBatch {
    pub tag: TypeTag,
    pub batch: RecordBatch,
}

/// Accumulates RDB entries into per-type Arrow RecordBatches.
///
/// Call [`push`](Self::push) for each entry. When a builder exceeds
/// `batch_size`, the returned `Vec` contains the flushed batch(es).
/// Call [`flush`](Self::flush) at the end to drain remaining rows.
pub struct ArrowBatcher {
    config: BatcherConfig,
    string: StringBatchBuilder,
    list: ListBatchBuilder,
    set: SetBatchBuilder,
    zset: SortedSetBatchBuilder,
    hash: HashBatchBuilder,
    geo: GeoBatchBuilder,
    hll: HllBatchBuilder,
}

impl ArrowBatcher {
    pub fn new(config: BatcherConfig) -> Self {
        Self {
            config,
            string: StringBatchBuilder::new(),
            list: ListBatchBuilder::new(),
            set: SetBatchBuilder::new(),
            zset: SortedSetBatchBuilder::new(),
            hash: HashBatchBuilder::new(),
            geo: GeoBatchBuilder::new(),
            hll: HllBatchBuilder::new(),
        }
    }

    /// Push an entry into the appropriate builder. Returns any batches that
    /// crossed the `batch_size` threshold.
    ///
    /// Sorted sets are always routed to the zset builder. If the entry also
    /// looks like geo data (all scores are valid 52-bit geohashes), it is
    /// additively pushed to the geo builder as well.
    pub fn push(
        &mut self,
        entry: &rdb_parser::RdbEntry,
    ) -> Result<Vec<TypedBatch>, ArrowConvertError> {
        let tag = match type_tag_for(entry) {
            Some(t) => t,
            None => return Ok(vec![]), // skip unsupported types
        };

        // Skip entries that exceed the max entry size
        if let Some(max) = self.config.max_entry_bytes {
            if estimate_entry_bytes(entry) > max {
                return Ok(vec![]);
            }
        }

        match tag {
            TypeTag::String => self.string.push(entry),
            TypeTag::List => self.list.push(entry),
            TypeTag::Set => self.set.push(entry),
            TypeTag::SortedSet => self.zset.push(entry),
            TypeTag::Hash => self.hash.push(entry),
            TypeTag::Geo => unreachable!("type_tag_for never returns Geo"),
            TypeTag::HyperLogLog => self.hll.push(entry),
        }

        // Additive geo: also push to geo builder if heuristic is enabled
        let also_geo = self.config.heuristics.contains(&Heuristic::Geo)
            && tag == TypeTag::SortedSet
            && is_geo_entry(entry);
        if also_geo {
            self.geo.push(entry);
        }

        let mut out = Vec::new();
        self.maybe_flush_tag(tag, &mut out)?;
        if also_geo {
            self.maybe_flush_tag(TypeTag::Geo, &mut out)?;
        }
        Ok(out)
    }

    /// Flush all remaining rows from every builder.
    pub fn flush(&mut self) -> Result<Vec<TypedBatch>, ArrowConvertError> {
        let mut out = Vec::new();
        self.flush_tag(TypeTag::String, &mut out)?;
        self.flush_tag(TypeTag::List, &mut out)?;
        self.flush_tag(TypeTag::Set, &mut out)?;
        self.flush_tag(TypeTag::SortedSet, &mut out)?;
        self.flush_tag(TypeTag::Hash, &mut out)?;
        self.flush_tag(TypeTag::Geo, &mut out)?;
        self.flush_tag(TypeTag::HyperLogLog, &mut out)?;
        Ok(out)
    }

    /// Create a [`BatchIterator`] that pulls entries from `iter` and yields
    /// `TypedBatch`es.
    pub fn process<I>(self, iter: I) -> BatchIterator<I>
    where
        I: Iterator<Item = Result<rdb_parser::RdbEntry, RdbError>>,
    {
        BatchIterator {
            batcher: self,
            iter,
            pending: VecDeque::new(),
            finished: false,
        }
    }

    fn maybe_flush_tag(
        &mut self,
        tag: TypeTag,
        out: &mut Vec<TypedBatch>,
    ) -> Result<(), ArrowConvertError> {
        let (len, bytes) = match tag {
            TypeTag::String => (self.string.len(), self.string.data_bytes()),
            TypeTag::List => (self.list.len(), self.list.data_bytes()),
            TypeTag::Set => (self.set.len(), self.set.data_bytes()),
            TypeTag::SortedSet => (self.zset.len(), self.zset.data_bytes()),
            TypeTag::Hash => (self.hash.len(), self.hash.data_bytes()),
            TypeTag::Geo => (self.geo.len(), self.geo.data_bytes()),
            TypeTag::HyperLogLog => (self.hll.len(), self.hll.data_bytes()),
        };
        let row_exceeded = len >= self.config.batch_size;
        let bytes_exceeded = self.config.batch_bytes.is_some_and(|max| bytes >= max);
        if row_exceeded || bytes_exceeded {
            self.flush_tag(tag, out)?;
        }
        Ok(())
    }

    fn flush_tag(
        &mut self,
        tag: TypeTag,
        out: &mut Vec<TypedBatch>,
    ) -> Result<(), ArrowConvertError> {
        let is_empty = match tag {
            TypeTag::String => self.string.is_empty(),
            TypeTag::List => self.list.is_empty(),
            TypeTag::Set => self.set.is_empty(),
            TypeTag::SortedSet => self.zset.is_empty(),
            TypeTag::Hash => self.hash.is_empty(),
            TypeTag::Geo => self.geo.is_empty(),
            TypeTag::HyperLogLog => self.hll.is_empty(),
        };
        if is_empty {
            return Ok(());
        }
        let batch = match tag {
            TypeTag::String => self.string.finish()?,
            TypeTag::List => self.list.finish()?,
            TypeTag::Set => self.set.finish()?,
            TypeTag::SortedSet => self.zset.finish()?,
            TypeTag::Hash => self.hash.finish()?,
            TypeTag::Geo => self.geo.finish()?,
            TypeTag::HyperLogLog => self.hll.finish()?,
        };
        out.push(TypedBatch { tag, batch });
        Ok(())
    }
}

/// Iterator adapter that wraps an `RdbEntry` iterator and yields `TypedBatch`es.
pub struct BatchIterator<I> {
    batcher: ArrowBatcher,
    iter: I,
    pending: VecDeque<TypedBatch>,
    finished: bool,
}

impl<I> Iterator for BatchIterator<I>
where
    I: Iterator<Item = Result<rdb_parser::RdbEntry, RdbError>>,
{
    type Item = Result<TypedBatch, ArrowConvertError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Drain any pending batches first.
            if let Some(batch) = self.pending.pop_front() {
                return Some(Ok(batch));
            }

            if self.finished {
                return None;
            }

            // Pull next entry from the inner iterator.
            match self.iter.next() {
                Some(Ok(entry)) => match self.batcher.push(&entry) {
                    Ok(batches) => {
                        self.pending.extend(batches);
                        // Loop back to drain pending or pull next entry.
                    }
                    Err(e) => return Some(Err(e)),
                },
                Some(Err(RdbError::UnknownType(_))) => {
                    // Unsupported type (stream, module) — skip silently.
                    continue;
                }
                Some(Err(e)) => return Some(Err(ArrowConvertError::Parser(e))),
                None => {
                    // Inner iterator exhausted — flush remaining.
                    self.finished = true;
                    match self.batcher.flush() {
                        Ok(batches) => {
                            self.pending.extend(batches);
                            // Loop back to drain pending.
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{test_entry, test_entry_typed};
    use rdb_parser::{RdbEntry, RdbValue};

    fn string_entry(key: &[u8], val: &[u8]) -> RdbEntry {
        test_entry(key, RdbValue::String(val.to_vec()))
    }

    fn list_entry(key: &[u8], elems: Vec<Vec<u8>>) -> RdbEntry {
        test_entry_typed(key, RdbValue::List(elems), 1) // RDB_TYPE_LIST
    }

    fn config(batch_size: usize) -> BatcherConfig {
        BatcherConfig { batch_size, ..Default::default() }
    }

    #[test]
    fn push_below_threshold_no_batches() {
        let mut batcher = ArrowBatcher::new(config(100));
        let batches = batcher.push(&string_entry(b"k", b"v")).unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn push_crosses_threshold() {
        let mut batcher = ArrowBatcher::new(config(3));
        for i in 0..3 {
            let key = format!("k{i}");
            let batches = batcher
                .push(&string_entry(key.as_bytes(), b"v"))
                .unwrap();
            if i < 2 {
                assert!(batches.is_empty());
            } else {
                assert_eq!(batches.len(), 1);
                assert_eq!(batches[0].tag, TypeTag::String);
                assert_eq!(batches[0].batch.num_rows(), 3);
            }
        }
    }

    #[test]
    fn flush_returns_partial() {
        let mut batcher = ArrowBatcher::new(config(100));
        batcher.push(&string_entry(b"a", b"1")).unwrap();
        batcher.push(&string_entry(b"b", b"2")).unwrap();
        let batches = batcher.flush().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].batch.num_rows(), 2);
    }

    #[test]
    fn list_explodes_rows() {
        let mut batcher = ArrowBatcher::new(config(5));
        // A list with 3 elements produces 3 rows.
        let batches = batcher
            .push(&list_entry(
                b"mylist",
                vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
            ))
            .unwrap();
        assert!(batches.is_empty()); // 3 < 5
        let batches = batcher.flush().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].tag, TypeTag::List);
        assert_eq!(batches[0].batch.num_rows(), 3);
    }

    #[test]
    fn batch_iterator_adapter() {
        let entries: Vec<Result<RdbEntry, RdbError>> = (0..5)
            .map(|i| {
                let key = format!("k{i}");
                Ok(string_entry(key.as_bytes(), b"v"))
            })
            .collect();

        let batcher = ArrowBatcher::new(config(3));
        let batches: Vec<_> = batcher
            .process(entries.into_iter())
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // 5 entries with batch_size=3 → 1 batch of 3 + 1 batch of 2
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].batch.num_rows(), 3);
        assert_eq!(batches[1].batch.num_rows(), 2);
    }

    #[test]
    fn multiple_types_separate_batches() {
        let mut batcher = ArrowBatcher::new(config(100));
        batcher.push(&string_entry(b"s1", b"v1")).unwrap();
        batcher
            .push(&list_entry(b"l1", vec![b"a".to_vec()]))
            .unwrap();
        batcher.push(&string_entry(b"s2", b"v2")).unwrap();
        let batches = batcher.flush().unwrap();

        // Should have 2 batches: one for String (2 rows), one for List (1 row)
        assert_eq!(batches.len(), 2);
        let string_batch = batches.iter().find(|b| b.tag == TypeTag::String).unwrap();
        assert_eq!(string_batch.batch.num_rows(), 2);
        let list_batch = batches.iter().find(|b| b.tag == TypeTag::List).unwrap();
        assert_eq!(list_batch.batch.num_rows(), 1);
    }

    fn make_hll_entry(key: &[u8]) -> RdbEntry {
        use crate::detect::make_hll;
        test_entry(key, RdbValue::String(make_hll(0, 0)))
    }

    fn geo_entry(key: &[u8], members: Vec<(Vec<u8>, f64)>) -> RdbEntry {
        test_entry_typed(key, RdbValue::SortedSet(members), 5) // RDB_TYPE_ZSET_2
    }

    #[test]
    fn hll_routes_to_hll_builder() {
        let mut batcher = ArrowBatcher::new(config(100));
        batcher.push(&make_hll_entry(b"hll1")).unwrap();
        batcher.push(&make_hll_entry(b"hll2")).unwrap();
        let batches = batcher.flush().unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].tag, TypeTag::HyperLogLog);
        assert_eq!(batches[0].batch.num_rows(), 2);
    }

    #[test]
    fn geo_additive_both_zset_and_geo() {
        // Valid geohash scores → produces both SortedSet and Geo batches (additive).
        let mut batcher = ArrowBatcher::new(config(100));
        batcher
            .push(&geo_entry(
                b"places",
                vec![
                    (b"A".to_vec(), 3479099956230698.0),
                    (b"B".to_vec(), 3663941556696959.0),
                ],
            ))
            .unwrap();
        let batches = batcher.flush().unwrap();

        assert_eq!(batches.len(), 2);
        let zset_b = batches.iter().find(|b| b.tag == TypeTag::SortedSet).unwrap();
        assert_eq!(zset_b.batch.num_rows(), 2);
        let geo_b = batches.iter().find(|b| b.tag == TypeTag::Geo).unwrap();
        assert_eq!(geo_b.batch.num_rows(), 2);
    }

    #[test]
    fn geo_heuristic_disabled_no_geo_output() {
        // When geo heuristic is disabled, valid geohash scores produce only SortedSet.
        let mut batcher = ArrowBatcher::new(BatcherConfig {
            batch_size: 100,
            heuristics: HashSet::new(), // no heuristics
            ..Default::default()
        });
        batcher
            .push(&geo_entry(
                b"places",
                vec![
                    (b"A".to_vec(), 3479099956230698.0),
                    (b"B".to_vec(), 3663941556696959.0),
                ],
            ))
            .unwrap();
        let batches = batcher.flush().unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].tag, TypeTag::SortedSet);
        assert_eq!(batches[0].batch.num_rows(), 2);
    }

    #[test]
    fn regular_string_not_hll() {
        let mut batcher = ArrowBatcher::new(config(100));
        batcher.push(&string_entry(b"s1", b"hello")).unwrap();
        let batches = batcher.flush().unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].tag, TypeTag::String);
    }

    #[test]
    fn regular_zset_not_geo() {
        let mut batcher = ArrowBatcher::new(config(100));
        batcher
            .push(&geo_entry(
                b"zset",
                vec![(b"alice".to_vec(), 1.5), (b"bob".to_vec(), 2.7)],
            ))
            .unwrap();
        let batches = batcher.flush().unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].tag, TypeTag::SortedSet);
    }

    #[test]
    fn batch_bytes_triggers_flush() {
        // Set batch_bytes low enough that a few entries trigger a flush before batch_size.
        let mut batcher = ArrowBatcher::new(BatcherConfig {
            batch_size: 1000, // high row limit — won't trigger
            batch_bytes: Some(20), // ~20 bytes budget
            max_entry_bytes: None,
            ..Default::default()
        });

        // Each string entry contributes key.len() + value.len() bytes.
        // "k0"(2) + "value"(5) = 7, "k1" = 7, "k2" = 7 → cumulative 21 ≥ 20 → flush.
        for i in 0..2 {
            let key = format!("k{i}");
            let batches = batcher.push(&string_entry(key.as_bytes(), b"value")).unwrap();
            assert!(batches.is_empty(), "no flush yet at entry {i}");
        }
        // Third entry should trigger the flush
        let batches = batcher.push(&string_entry(b"k2", b"value")).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].tag, TypeTag::String);
        assert_eq!(batches[0].batch.num_rows(), 3);
    }

    #[test]
    fn batch_bytes_resets_after_flush() {
        let mut batcher = ArrowBatcher::new(BatcherConfig {
            batch_size: 1000,
            batch_bytes: Some(15),
            max_entry_bytes: None,
            ..Default::default()
        });

        // Push 2 entries (~14 bytes), then a 3rd triggers flush (~21 bytes)
        batcher.push(&string_entry(b"k0", b"value")).unwrap();
        batcher.push(&string_entry(b"k1", b"value")).unwrap();
        let batches = batcher.push(&string_entry(b"k2", b"value")).unwrap();
        assert_eq!(batches.len(), 1);

        // After flush, byte counter resets. Push 2 more — should NOT flush.
        let batches = batcher.push(&string_entry(b"k3", b"value")).unwrap();
        assert!(batches.is_empty());
        let batches = batcher.flush().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].batch.num_rows(), 1);
    }

    #[test]
    fn max_entry_bytes_skips_large_entries() {
        let mut batcher = ArrowBatcher::new(BatcherConfig {
            batch_size: 100,
            batch_bytes: None,
            max_entry_bytes: Some(50), // skip entries > 50 bytes
            ..Default::default()
        });

        // Small entry: key(2) + value(5) = 7 → allowed
        batcher.push(&string_entry(b"k1", b"hello")).unwrap();

        // Large entry: key(2) + value(100) = 102 → skipped
        let big_val = vec![b'x'; 100];
        batcher.push(&string_entry(b"k2", &big_val)).unwrap();

        let batches = batcher.flush().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].batch.num_rows(), 1); // only the small entry
    }

    #[test]
    fn max_entry_bytes_allows_entries_at_threshold() {
        let mut batcher = ArrowBatcher::new(BatcherConfig {
            batch_size: 100,
            batch_bytes: None,
            max_entry_bytes: Some(10), // skip entries > 10 bytes
            ..Default::default()
        });

        // Exactly 10 bytes: key(2) + value(8) = 10 → allowed
        batcher.push(&string_entry(b"k1", b"12345678")).unwrap();

        // 11 bytes: key(2) + value(9) = 11 → skipped
        batcher.push(&string_entry(b"k2", b"123456789")).unwrap();

        let batches = batcher.flush().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].batch.num_rows(), 1);
    }

    #[test]
    fn estimate_entry_bytes_string() {
        let entry = string_entry(b"mykey", b"myvalue");
        assert_eq!(estimate_entry_bytes(&entry), 5 + 7); // key + value
    }

    #[test]
    fn estimate_entry_bytes_list() {
        let entry = list_entry(b"lk", vec![b"aaa".to_vec(), b"bb".to_vec()]);
        assert_eq!(estimate_entry_bytes(&entry), 2 + 3 + 2); // key + elem1 + elem2
    }

    #[test]
    fn estimate_entry_bytes_sorted_set() {
        let entry = geo_entry(
            b"zk",
            vec![(b"member".to_vec(), 1.0)],
        );
        // key(2) + member(6) + 8 (f64) = 16
        assert_eq!(estimate_entry_bytes(&entry), 2 + 6 + 8);
    }

    #[test]
    fn mixed_types_with_geo_and_hll() {
        let entries: Vec<Result<RdbEntry, RdbError>> = vec![
            Ok(string_entry(b"s1", b"val")),
            Ok(make_hll_entry(b"hll1")),
            Ok(geo_entry(
                b"geo1",
                vec![(b"P".to_vec(), 3479099956230698.0)],
            )),
            Ok(string_entry(b"s2", b"val2")),
        ];

        let batcher = ArrowBatcher::new(config(100));
        let batches: Vec<_> = batcher
            .process(entries.into_iter())
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // 4 batches: String(2), HyperLogLog(1), SortedSet(1), Geo(1)
        assert_eq!(batches.len(), 4);
        let string_b = batches.iter().find(|b| b.tag == TypeTag::String).unwrap();
        assert_eq!(string_b.batch.num_rows(), 2);
        let hll_b = batches
            .iter()
            .find(|b| b.tag == TypeTag::HyperLogLog)
            .unwrap();
        assert_eq!(hll_b.batch.num_rows(), 1);
        let zset_b = batches.iter().find(|b| b.tag == TypeTag::SortedSet).unwrap();
        assert_eq!(zset_b.batch.num_rows(), 1);
        let geo_b = batches.iter().find(|b| b.tag == TypeTag::Geo).unwrap();
        assert_eq!(geo_b.batch.num_rows(), 1);
    }
}
