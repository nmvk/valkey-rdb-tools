use clap::{Parser, Subcommand, ValueEnum};
use parquet::basic::Compression;

fn parse_positive_usize(s: &str) -> Result<usize, String> {
    let n: usize = s.parse().map_err(|e| format!("{e}"))?;
    if n == 0 {
        return Err("value must be > 0".to_string());
    }
    Ok(n)
}

/// Parse a human-friendly byte size like "10mb", "512kb", "1gb", or raw bytes "1048576".
fn parse_byte_size(s: &str) -> Result<usize, String> {
    let s = s.trim().to_ascii_lowercase();
    let (num_str, multiplier) = if let Some(n) = s.strip_suffix("gb") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("mb") {
        (n, 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("kb") {
        (n, 1024)
    } else if let Some(n) = s.strip_suffix('b') {
        (n, 1)
    } else {
        (s.as_str(), 1)
    };
    let n: usize = num_str.trim().parse().map_err(|e| format!("{e}"))?;
    if n == 0 {
        return Err("value must be > 0".to_string());
    }
    Ok(n * multiplier)
}

#[derive(Parser)]
#[command(name = "valkey-rdb", about = "Export Valkey/Redis RDB files to columnar formats")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Export RDB data to Parquet, Arrow IPC, CSV, or JSON
    Export(ExportArgs),
    /// Print Arrow schema for each type
    Schema(SchemaArgs),
    /// Validate exported Parquet files against the source RDB
    Validate(ValidateArgs),
}

#[derive(Parser)]
pub struct ExportArgs {
    /// Path to the RDB file (use `-` for stdin)
    pub file: String,

    /// Output directory (default: derived from input filename)
    #[arg(short, long)]
    pub output: Option<String>,

    /// Output format
    #[arg(short, long, default_value = "parquet")]
    pub format: FormatArg,

    /// Parquet compression codec
    #[arg(long, default_value = "zstd")]
    pub compression: CompressionArg,

    /// Filter by database number
    #[arg(long)]
    pub db: Option<u32>,

    /// Filter by type (comma-separated: string, list, set, zset, hash, geo, hll)
    #[arg(long = "type")]
    pub type_names: Option<String>,

    /// Filter keys by glob pattern
    #[arg(long)]
    pub key_pattern: Option<String>,

    /// Rows per Arrow RecordBatch (must be > 0)
    #[arg(long, default_value = "65536", value_parser = parse_positive_usize)]
    pub batch_size: usize,

    /// Rows per Parquet row group (must be > 0)
    #[arg(long, default_value = "1048576", value_parser = parse_positive_usize)]
    pub row_group_size: usize,

    /// Shard identifier for conflict-free parallel writes
    #[arg(long)]
    pub shard_id: Option<String>,

    /// Maximum elements per collection chunk to bound memory [default: 50000]
    #[arg(long, value_parser = parse_positive_usize)]
    pub max_key_elements: Option<usize>,

    /// Disable collection chunking (read entire collections into memory)
    #[arg(long, conflicts_with = "max_key_elements")]
    pub no_chunking: bool,

    /// Byte budget per builder batch — flush when exceeded (e.g. 10mb, 50mb)
    #[arg(long, value_parser = parse_byte_size)]
    pub batch_bytes: Option<usize>,

    /// Skip entries larger than this size (e.g. 1mb, 10mb)
    #[arg(long, value_parser = parse_byte_size)]
    pub max_entry_bytes: Option<usize>,
}

#[derive(Parser)]
pub struct SchemaArgs {
    /// Show schema for a specific type only
    #[arg(long = "type")]
    pub type_name: Option<String>,

    /// Output format
    #[arg(long, default_value = "text")]
    pub output: OutputFormat,
}

#[derive(Parser)]
pub struct ValidateArgs {
    /// Path to the source RDB file
    pub file: String,

    /// Path to the export output directory
    pub output: String,
}

#[derive(Clone, ValueEnum)]
pub enum FormatArg {
    Parquet,
    #[value(name = "arrow-ipc")]
    ArrowIpc,
    Csv,
    Json,
}

#[derive(Clone, ValueEnum)]
pub enum CompressionArg {
    Zstd,
    Snappy,
    Lz4,
    Gzip,
    None,
}

impl CompressionArg {
    pub fn to_parquet_compression(&self) -> Compression {
        match self {
            CompressionArg::Zstd => Compression::ZSTD(Default::default()),
            CompressionArg::Snappy => Compression::SNAPPY,
            CompressionArg::Lz4 => Compression::LZ4,
            CompressionArg::Gzip => Compression::GZIP(Default::default()),
            CompressionArg::None => Compression::UNCOMPRESSED,
        }
    }
}

#[derive(Clone, ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
}
