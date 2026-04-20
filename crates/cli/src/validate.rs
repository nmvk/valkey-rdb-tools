use std::collections::BTreeMap;
use std::collections::HashSet;
use std::path::Path;

use parquet::file::reader::{FileReader, SerializedFileReader};
use rdb_to_arrow::{summarize_entries, EntrySummary, Heuristic, TypeTag};

use crate::args::ValidateArgs;
use crate::cli_error::CliError;
use crate::io::{build_rdb_reader, find_parquet_files, open_file};

fn count_rdb(
    path: &str,
    heuristics: &HashSet<Heuristic>,
) -> Result<EntrySummary, CliError> {
    let mut reader = build_rdb_reader(path)?;
    Ok(summarize_entries(&mut reader, heuristics)?)
}

fn count_parquet(dir: &str) -> Result<BTreeMap<TypeTag, u64>, CliError> {
    let dir_path = Path::new(dir);
    let mut counts = BTreeMap::new();
    for tag in TypeTag::ALL {
        let files = find_parquet_files(dir_path, tag);
        if !files.is_empty() {
            let mut total: u64 = 0;
            for path in &files {
                let path_str = path.to_string_lossy().into_owned();
                let file = open_file(&path_str)?;
                let reader = SerializedFileReader::new(file)?;
                let n = reader.metadata().file_metadata().num_rows();
                let n = u64::try_from(n).map_err(|_| {
                    format!("negative row count {} in {}", n, path.display())
                })?;
                total += n;
            }
            counts.insert(tag, total);
        }
    }
    Ok(counts)
}

struct ParquetMeta {
    has_exported_by: bool,
    heuristics: HashSet<Heuristic>,
}

fn read_parquet_metadata(dir: &str) -> Result<ParquetMeta, CliError> {
    // Find any parquet file to read metadata from
    let dir_path = Path::new(dir);
    let path = TypeTag::ALL
        .iter()
        .flat_map(|&tag| find_parquet_files(dir_path, tag))
        .next();
    let path = match path {
        Some(p) => p,
        None => {
            return Ok(ParquetMeta {
                has_exported_by: false,
                heuristics: Heuristic::ALL.iter().copied().collect(),
            });
        }
    };

    let file = open_file(&path.to_string_lossy())?;
    let reader = SerializedFileReader::new(file)?;
    let file_meta = reader.metadata().file_metadata();
    let arrow_meta =
        parquet::arrow::parquet_to_arrow_schema(file_meta.schema_descr(), file_meta.key_value_metadata());

    if let Ok(schema) = arrow_meta {
        let meta = schema.metadata();
        let has_exported_by = meta.contains_key(rdb_to_arrow::RDB_EXPORTED_BY_KEY);
        // Use the canonical parser so unknown heuristic names surface as
        // an error rather than being silently dropped (the hand-parsed
        // `filter_map` variant lost fidelity with the exporter side).
        let heuristics = match meta.get(rdb_to_arrow::RDB_HEURISTICS_KEY) {
            Some(s) => Heuristic::parse_set(s).map_err(|e| {
                CliError::Usage(format!("{} in {}", e, rdb_to_arrow::RDB_HEURISTICS_KEY))
            })?,
            // No key → legacy export, assume all heuristics were on.
            None => Heuristic::ALL.iter().copied().collect(),
        };
        return Ok(ParquetMeta { has_exported_by, heuristics });
    }

    Ok(ParquetMeta {
        has_exported_by: false,
        heuristics: Heuristic::ALL.iter().copied().collect(),
    })
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

pub fn run(args: &ValidateArgs) -> Result<(), CliError> {
    let parquet_meta = read_parquet_metadata(&args.output)?;
    let rdb = count_rdb(&args.file, &parquet_meta.heuristics)?;

    eprintln!(
        "RDB: {} ({}, RDB v{})",
        args.file,
        rdb.header.magic,
        rdb.header.version,
    );
    if rdb.crc_verified {
        eprintln!("CRC-64: verified");
    } else {
        eprintln!("CRC-64: not available (RDB v{})", rdb.header.version);
    }
    eprintln!();

    let parquet_counts = count_parquet(&args.output)?;

    // Collect all types present in either RDB or Parquet
    let mut all_tags: Vec<TypeTag> = Vec::new();
    for tag in TypeTag::ALL {
        if rdb.totals.contains_key(&tag) || parquet_counts.contains_key(&tag) {
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
        let rdb_tc = rdb.totals.get(&tag);
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
            (None, None) => continue, // filtered into all_tags but absent from both — skip
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

    if !parquet_meta.has_exported_by {
        eprintln!(
            "Warning: {} metadata tag not found in Parquet files",
            rdb_to_arrow::RDB_EXPORTED_BY_KEY
        );
    }

    if fail_count == 0 {
        eprintln!("Result: PASS ({}/{} types match)", pass_count, total);
        Ok(())
    } else {
        // Distinct exit code (5) — callers can distinguish a validation
        // mismatch from I/O, corruption, or usage errors.
        Err(CliError::ValidationFailed(format!(
            "FAIL: {fail_count} of {total} types mismatched"
        )))
    }
}
