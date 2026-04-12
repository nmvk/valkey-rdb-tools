use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use rdb_parser::RdbReader;
use std::collections::HashSet;

use rdb_to_arrow::{
    metadata_from_rdb, write_arrow_ipc, write_csv, write_json, write_parquet, ArrowBatcher,
    ArrowConvertError, BatcherConfig, ParquetConfig, TypeTag,
};
use crate::args::parse_heuristics;

use crate::args::{ExportArgs, FormatArg};
use crate::filter::{EntryFilter, FilteredEntries};

pub fn run(args: &ExportArgs) -> Result<(), Box<dyn std::error::Error>> {
    let type_tags: Option<HashSet<TypeTag>> = match &args.type_names {
        Some(names) => {
            let mut tags = HashSet::new();
            for name in names.split(',') {
                let name = name.trim();
                tags.insert(TypeTag::from_cli_name(name).ok_or_else(|| {
                    format!("unknown type '{name}'. Valid: string, list, set, zset, hash, geo, hll")
                })?);
            }
            Some(tags)
        }
        None => None,
    };

    // Open input: file or stdin. RdbReader buffers internally.
    let input: Box<dyn Read> = if args.file == "-" {
        Box::new(io::stdin())
    } else {
        Box::new(File::open(&args.file)?)
    };

    let reader = RdbReader::new(input)?;
    let reader = if args.no_chunking {
        reader.without_chunking()
    } else if let Some(max) = args.max_key_elements {
        reader.with_max_key_elements(max)
    } else {
        reader // uses DEFAULT_MAX_KEY_ELEMENTS
    };
    let mut metadata = metadata_from_rdb(reader.metadata());

    // Determine output directory
    let output_dir = match &args.output {
        Some(dir) => PathBuf::from(dir),
        None => {
            if args.file == "-" {
                PathBuf::from("rdb_export")
            } else {
                let stem = Path::new(&args.file)
                    .file_stem()
                    .unwrap_or_default()
                    .to_string_lossy();
                PathBuf::from(stem.as_ref())
            }
        }
    };
    fs::create_dir_all(&output_dir)?;

    // Clone before moving into the filter — we need it again for batch-level output filtering.
    let output_tags = type_tags.clone();
    let heuristics = parse_heuristics(&args.heuristic)?;

    // Record active heuristics in Parquet metadata so validate can be self-describing.
    let heuristic_str: String = if heuristics.is_empty() {
        "none".to_string()
    } else {
        let mut names: Vec<&str> = heuristics.iter().map(|h| h.as_str()).collect();
        names.sort();
        names.join(",")
    };
    metadata.insert("rdb.heuristics".to_string(), heuristic_str);

    let filter = EntryFilter {
        db: args.db,
        type_tags,
        key_pattern: args.key_pattern.clone(),
        heuristics: heuristics.clone(),
    };

    let filtered = FilteredEntries::new(reader, filter);
    #[allow(clippy::needless_update)] // forward-compat guard for new BatcherConfig fields
    let batcher = ArrowBatcher::new(BatcherConfig {
        batch_size: args.batch_size,
        batch_bytes: args.batch_bytes,
        max_entry_bytes: args.max_entry_bytes,
        heuristics: heuristics.clone(),
        ..Default::default()
    });
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

    if let Some(ref s) = args.shard_id {
        if s.contains('/') || s.contains('\\') || s.contains("..") {
            return Err("shard-id must not contain path separators or '..'".into());
        }
    }

    let shard_suffix = args
        .shard_id
        .as_deref()
        .map(|s| format!(".{s}"))
        .unwrap_or_default();

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
