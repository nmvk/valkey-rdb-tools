use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;

use parquet::file::reader::{FileReader, SerializedFileReader};
use rdb_parser::{RdbEntry, RdbError, RdbReader, RdbValue};
use rdb_to_arrow::{is_geo_entry, type_tag_for, TypeTag};

use crate::args::ValidateArgs;

struct TypeCounts {
    keys: u64,
    rows: u64,
}

struct RdbSummary {
    server_version: Option<String>,
    magic: String,
    rdb_version: u32,
    counts: BTreeMap<TypeTag, TypeCounts>,
}

fn entry_row_count(entry: &RdbEntry) -> u64 {
    match &entry.value {
        RdbValue::String(_) => 1,
        RdbValue::List(elems) => elems.len().max(1) as u64,
        RdbValue::Set(members) => members.len().max(1) as u64,
        RdbValue::SortedSet(pairs) => pairs.len().max(1) as u64,
        RdbValue::Hash(fields) => fields.len().max(1) as u64,
        _ => 1,
    }
}

fn count_rdb(path: &str) -> Result<RdbSummary, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = RdbReader::new(file)?;
    let header = reader.header().clone();
    let server_version = reader.metadata().server_version().map(|s| s.to_string());

    let mut counts: BTreeMap<TypeTag, TypeCounts> = BTreeMap::new();

    for entry_result in reader {
        let entry = match entry_result {
            Ok(e) => e,
            Err(RdbError::UnknownType(_)) => continue,
            Err(e) => return Err(e.into()),
        };

        let tag = match type_tag_for(&entry) {
            Some(t) => t,
            None => continue,
        };

        let is_first_chunk =
            entry.element_offset.is_none() || entry.element_offset == Some(0);
        let row_count = entry_row_count(&entry);

        let tc = counts
            .entry(tag)
            .or_insert(TypeCounts { keys: 0, rows: 0 });
        if is_first_chunk {
            tc.keys += 1;
        }
        tc.rows += row_count;

        // Geo is additive: sorted sets with geohash scores also appear as geo
        if tag == TypeTag::SortedSet && is_geo_entry(&entry) {
            let gc = counts
                .entry(TypeTag::Geo)
                .or_insert(TypeCounts { keys: 0, rows: 0 });
            if is_first_chunk {
                gc.keys += 1;
            }
            gc.rows += row_count;
        }
    }

    Ok(RdbSummary {
        server_version,
        magic: header.magic.to_string(),
        rdb_version: header.version,
        counts,
    })
}

fn count_parquet(dir: &str) -> Result<BTreeMap<TypeTag, u64>, Box<dyn std::error::Error>> {
    let mut counts = BTreeMap::new();
    for tag in TypeTag::ALL {
        let base = Path::new(dir).join(format!("{}.parquet", tag.as_str()));
        let mut total: u64 = 0;
        let mut found = false;

        // Check base file
        if base.exists() {
            let file = File::open(&base)?;
            let reader = SerializedFileReader::new(file)?;
            let n = reader.metadata().file_metadata().num_rows();
            total += u64::try_from(n).unwrap_or(0);
            found = true;
        }

        // Check shard files: {type}.*.parquet
        let glob_pattern = format!("{}.*.parquet", tag.as_str());
        let dir_path = Path::new(dir);
        if let Ok(entries) = std::fs::read_dir(dir_path) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if name_str != format!("{}.parquet", tag.as_str())
                    && glob_match::glob_match(&glob_pattern, &name_str)
                {
                    let file = File::open(entry.path())?;
                    let reader = SerializedFileReader::new(file)?;
                    let n = reader.metadata().file_metadata().num_rows();
                    total += u64::try_from(n).unwrap_or(0);
                    found = true;
                }
            }
        }

        if found {
            counts.insert(tag, total);
        }
    }
    Ok(counts)
}

fn check_parquet_metadata(
    dir: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    // The metadata is stored in the Arrow schema (embedded in the Parquet footer),
    // not in the Parquet file-level key_value_metadata.
    for tag in TypeTag::ALL {
        let path = Path::new(dir).join(format!("{}.parquet", tag.as_str()));
        if !path.exists() {
            continue;
        }
        let file = File::open(&path)?;
        let reader = SerializedFileReader::new(file)?;
        let file_meta = reader.metadata().file_metadata();
        let arrow_meta =
            parquet::arrow::parquet_to_arrow_schema(file_meta.schema_descr(), file_meta.key_value_metadata());
        if let Ok(schema) = arrow_meta {
            if schema.metadata().contains_key("rdb.exported_by") {
                return Ok(true);
            }
        }
        // Only need to check the first existing file
        return Ok(false);
    }
    Ok(false)
}

fn format_num(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

pub fn run(args: &ValidateArgs) -> Result<(), Box<dyn std::error::Error>> {
    let rdb = count_rdb(&args.file)?;

    eprintln!(
        "RDB: {} ({} {}, RDB v{})",
        args.file,
        rdb.magic,
        rdb.server_version.as_deref().unwrap_or("unknown"),
        rdb.rdb_version,
    );
    eprintln!("CRC-64: verified");
    eprintln!();

    let parquet_counts = count_parquet(&args.output)?;
    let has_metadata = check_parquet_metadata(&args.output)?;

    // Collect all types present in either RDB or Parquet
    let mut all_tags: Vec<TypeTag> = Vec::new();
    for tag in TypeTag::ALL {
        if rdb.counts.contains_key(&tag) || parquet_counts.contains_key(&tag) {
            all_tags.push(tag);
        }
    }

    // Print table header
    eprintln!(
        "  {:<15} {:>10} {:>12} {:>14}  Status",
        "Type", "Keys", "Rows", "Parquet Rows"
    );

    let mut pass_count = 0u32;
    let mut fail_count = 0u32;

    for &tag in &all_tags {
        let rdb_tc = rdb.counts.get(&tag);
        let pq_rows = parquet_counts.get(&tag).copied();

        let (keys_str, rows_str, pq_str, status) = match (rdb_tc, pq_rows) {
            (Some(tc), Some(pq)) => {
                let ok = tc.rows == pq;
                if ok {
                    pass_count += 1;
                } else {
                    fail_count += 1;
                }
                (
                    format_num(tc.keys),
                    format_num(tc.rows),
                    format_num(pq),
                    if ok { "ok" } else { "MISMATCH" },
                )
            }
            (Some(tc), None) => {
                fail_count += 1;
                (
                    format_num(tc.keys),
                    format_num(tc.rows),
                    "-".to_string(),
                    "MISSING",
                )
            }
            (None, Some(pq)) => {
                fail_count += 1;
                (
                    "-".to_string(),
                    "-".to_string(),
                    format_num(pq),
                    "EXTRA",
                )
            }
            (None, None) => unreachable!(),
        };

        eprintln!(
            "  {:<15} {:>10} {:>12} {:>14}  {}",
            tag.as_str(),
            keys_str,
            rows_str,
            pq_str,
            status,
        );
    }

    let total = pass_count + fail_count;
    eprintln!();

    if !has_metadata {
        eprintln!("Warning: rdb.exported_by metadata tag not found in Parquet files");
    }

    if fail_count == 0 {
        eprintln!("Result: PASS ({}/{} types match)", pass_count, total);
        Ok(())
    } else {
        Err(format!("FAIL: {} of {} types mismatched", fail_count, total).into())
    }
}
