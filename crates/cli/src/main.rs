mod args;
mod cli_error;
mod export;
mod filter;
mod io;
mod schema;
mod validate;

use clap::Parser;

use crate::cli_error::CliError;

fn main() {
    let cli = args::Cli::parse();

    let result: Result<(), CliError> = match &cli.command {
        args::Command::Export(a) => export::run(a),
        args::Command::Schema(a) => schema::run(a),
        args::Command::Validate(a) => validate::run(a),
    };

    if let Err(e) = result {
        eprintln!("error: {e}");
        // Walk the source chain so underlying cause is visible without
        // forcing every call site to format with `{:#}`.
        let mut cause = std::error::Error::source(&e);
        while let Some(c) = cause {
            eprintln!("  caused by: {c}");
            cause = c.source();
        }
        std::process::exit(e.exit_code());
    }
}
