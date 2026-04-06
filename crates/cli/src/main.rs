mod args;
mod export;
mod filter;
mod schema;
mod validate;

use clap::Parser;

fn main() {
    let cli = args::Cli::parse();

    let result = match &cli.command {
        args::Command::Export(a) => export::run(a),
        args::Command::Schema(a) => schema::run(a),
        args::Command::Validate(a) => validate::run(a),
    };

    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
