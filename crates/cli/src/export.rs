use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

use std::collections::HashSet;

use rdb_to_arrow::{
    metadata_from_rdb, write_arrow_ipc, write_csv, write_json, write_parquet, ArrowBatcher,
    ArrowConvertError, BatcherConfig, Heuristic, ParquetConfig, TypeTag,
};

use crate::args::{ExportArgs, FormatArg};
use crate::cli_error::CliError;
use crate::filter::{EntryFilter, FilteredEntries};
use crate::io::{build_rdb_reader, shard_suffix};

pub fn run(args: &ExportArgs) -> Result<(), CliError> {
    // Validate arguments before any I/O
    let type_tags: Option<HashSet<TypeTag>> = match &args.type_names {
        Some(names) => Some(TypeTag::parse_set(names)?),
        None => None,
    };
    if let Some(ref s) = args.shard_id {
        if s.is_empty() || !s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
            return Err("shard-id must be non-empty and contain only [A-Za-z0-9_-]".into());
        }
    }

    // Open input — `build_rdb_reader` handles both "-" (stdin) and
    // regular files, wrapping I/O errors with the filename and keeping
    // the IO/Corrupt split for exit codes.
    let heuristics = Heuristic::parse_set(&args.heuristic)?;

    let reader = build_rdb_reader(&args.file)?;
    let reader = if args.no_chunking {
        reader.without_chunking()
    } else if let Some(max) = args.max_key_elements {
        reader.with_max_key_elements(max)
    } else {
        reader // uses DEFAULT_MAX_KEY_ELEMENTS
    };
    let metadata = metadata_from_rdb(reader.metadata(), &heuristics);

    // Determine output directory
    let output_dir = match &args.output {
        Some(dir) => PathBuf::from(dir),
        None => {
            if args.file == "-" {
                PathBuf::from("rdb_export")
            } else {
                let stem = Path::new(&args.file)
                    .file_stem()
                    .map(|s| s.to_string_lossy().into_owned())
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| "rdb_export".to_string());
                PathBuf::from(stem)
            }
        }
    };
    fs::create_dir_all(&output_dir)?;

    // Warn if output directory already contains parquet/arrow/csv/json files
    if let Ok(entries) = fs::read_dir(&output_dir) {
        let existing: Vec<_> = entries
            .flatten()
            .filter(|e| {
                let name = e.file_name();
                let s = name.to_string_lossy();
                s.ends_with(".parquet") || s.ends_with(".arrow")
                    || s.ends_with(".csv") || s.ends_with(".json")
            })
            .collect();
        if !existing.is_empty() {
            eprintln!(
                "Warning: output directory '{}' contains {} existing file(s) that will be overwritten",
                output_dir.display(),
                existing.len()
            );
        }
    }

    // Clone before moving into the filter — we need it again for batch-level output filtering.
    let output_tags = type_tags.clone();

    let filter = EntryFilter {
        db: args.db,
        type_tags,
        key_pattern: args.key_pattern.clone(),
        heuristics: heuristics.clone(),
    };

    let filtered = FilteredEntries::new(reader, filter);
    #[allow(clippy::field_reassign_with_default)] // conditional batch_bytes override
    let batcher_config = {
        let mut c = BatcherConfig::default();
        c.batch_size = args.batch_size;
        if let Some(bb) = args.batch_bytes {
            c.batch_bytes = Some(bb);
        }
        c.max_entry_bytes = args.max_entry_bytes;
        c.heuristics = heuristics.clone();
        c
    };
    let batcher = ArrowBatcher::new(batcher_config);
    let raw_batches = batcher.process(filtered);

    // The batcher emits additive output (e.g., both zset and geo for geo-like entries).
    // When --type is specified, only write the requested types.
    let batches: Box<dyn Iterator<Item = Result<rdb_to_arrow::TypedBatch, ArrowConvertError>>> =
        if let Some(tags) = output_tags {
            Box::new(raw_batches.filter(move |result| match result {
                Ok(tb) => tags.contains(&tb.tag),
                Err(_) => true,
            }))
        } else {
            Box::new(raw_batches)
        };

    let ext = match args.format {
        FormatArg::Parquet => "parquet",
        FormatArg::ArrowIpc => "arrow",
        FormatArg::Csv => "csv",
        FormatArg::Json => "json",
    };

    let shard_suffix = shard_suffix(args.shard_id.as_deref());

    let writer_factory = |tag: TypeTag| -> Result<File, ArrowConvertError> {
        let filename = format!("{}{}.{}", tag.as_str(), shard_suffix, ext);
        let path = output_dir.join(&filename);
        File::create(&path).map_err(|e| {
            ArrowConvertError::Io(io::Error::new(e.kind(), format!("{}: {e}", path.display())))
        })
    };

    match args.format {
        FormatArg::Parquet => {
            let config = ParquetConfig {
                compression: args.compression.to_parquet_compression(),
                max_row_group_size: args.row_group_size,
                file_metadata: metadata,
            };
            write_parquet(batches, &config, writer_factory)?;
        }
        FormatArg::ArrowIpc => {
            write_arrow_ipc(batches, writer_factory)?;
        }
        FormatArg::Csv => {
            write_csv(batches, writer_factory)?;
        }
        FormatArg::Json => {
            write_json(batches, writer_factory)?;
        }
    }

    eprintln!("Exported to {}", output_dir.display());
    Ok(())
}
