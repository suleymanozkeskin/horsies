use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "acme", about = "Acme Clothing showcase")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create the Acme database and install its tables.
    Seed,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Seed => {
            let settings = acme_showcase::resolve_database_settings()?;
            println!(
                "database resolution: {} -> {}",
                settings.source, settings.database_name
            );
            acme_showcase::ensure_database(&settings).await?;
            let store = acme_showcase::Store::connect(&settings).await?;
            store.ensure_schema().await?;
            println!(
                "seed complete: database={} schema=acme",
                settings.database_name
            );
        }
    }
    Ok(())
}
