use clap::{Parser, Subcommand};
use garbagebin::config::Config;

#[derive(Parser)]
#[command(
    name = "gb",
    version,
    about = "Garbage Bin: a single-node S3-compatible object store"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    #[command(flatten)]
    config: Config,
}

#[derive(Subcommand)]
enum Command {
    /// Exit 0 if the server on the listen address answers `/health` with 200.
    Healthcheck,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Some(Command::Healthcheck) => garbagebin::healthcheck::check(cli.config.listen),
        None => {
            garbagebin::init_logging(&cli.config.log);
            tokio::runtime::Runtime::new()?.block_on(garbagebin::server::run(cli.config))
        }
    }
}
