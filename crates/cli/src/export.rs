use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use rdb_parser::RdbReader;
use rdb_to_arrow::{
    metadata_from_rdb, write_arrow_ipc, write_csv, write_json, write_parquet, ArrowBatcher,
    ArrowConvertError, BatcherConfig, ParquetConfig, TypeTag,
};

use crate::args::{ExportArgs, FormatArg};
use crate::filter::{EntryFilter, FilteredEntries};

pub fn run(args: &ExportArgs) -> Result<(), Box<dyn std::error::Error>> {
    let type_tag = match &args.type_name {
        Some(name) => Some(TypeTag::from_cli_name(name).ok_or_else(|| {
            format!(
                "unknown type '{}'. Valid types: string, list, set, zset, hash, geo, hll",
                name
            )
        })?),
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
    let metadata = metadata_from_rdb(reader.metadata());

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

    let filter = EntryFilter {
        db: args.db,
        type_tag,
        key_pattern: args.key_pattern.clone(),
    };

    let filtered = FilteredEntries::new(reader, filter);
    let batcher = ArrowBatcher::new(BatcherConfig {
        batch_size: args.batch_size,
    });
    let batches = batcher.process(filtered);

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
