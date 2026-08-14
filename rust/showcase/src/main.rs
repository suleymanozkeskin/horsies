use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "acme", about = "Acme Clothing showcase")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create the Acme database, tables, and deterministic catalog.
    Seed,
    /// Run the long-lived task worker.
    Worker(WorkerOptions),
    /// Run the long-lived scheduler.
    Scheduler,
    /// Serve the Horsies monitoring UI.
    Web(WebOptions),
    /// Place a bounded or open-ended stream of orders.
    Steady {
        #[arg(long)]
        orders: Option<usize>,
        #[arg(long)]
        cover_errors: bool,
    },
    /// Submit the high-volume rush scenario.
    Rush,
    /// Submit deterministic business failures and returns.
    ProblemChild,
    /// Submit the catalog import workflow.
    BulkImport,
    /// Submit two campaigns and expiring price updates.
    FlashSale,
    /// Submit export recovery drills.
    Chaos,
    /// Submit scheduled maintenance workflows.
    Maintenance,
    /// Print deterministic simulation samples.
    Simulate,
}

#[derive(Debug, Clone, Args)]
struct WebOptions {
    /// Database URL. If omitted, use the five Acme resolution rules.
    #[arg(long)]
    database_url: Option<String>,
    /// Session URL for LISTEN/NOTIFY when the data URL is transaction pooled.
    #[arg(long)]
    session_database_url: Option<String>,
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 8600)]
    port: u16,
    #[arg(long, value_enum, default_value_t = horsies::WebAuthMode::None)]
    auth: horsies::WebAuthMode,
    #[arg(long, default_value = "X-Forwarded-User")]
    trusted_header: String,
    #[arg(long)]
    enable_actions: bool,
    #[arg(long)]
    custom_css_url: Option<String>,
    #[arg(long, default_value = "info")]
    loglevel: String,
}

fn settings() -> Result<acme_showcase::DatabaseSettings, String> {
    acme_showcase::resolve_database_settings().map_err(|error| error.to_string())
}

#[derive(Debug, clap::Args)]
struct WorkerOptions {
    /// Concurrent task slots in this worker process.
    #[arg(long, default_value_t = 12)]    concurrency: u32,
}

async fn run_worker(options: WorkerOptions) -> Result<(), Box<dyn std::error::Error>> {
    let settings = settings()?;
    let app = acme_showcase::app::build_app_for_url(settings.sqlx_url())?;
    let mut config = horsies::WorkerConfig {
        queues: acme_showcase::app::QUEUES
            .iter()
            .map(|(name, _, _)| (*name).to_owned())
            .collect(),
        concurrency: options.concurrency,
        ..horsies::WorkerConfig::default()
    };
    config.max_claim_batch = 0;
    app.run_worker_with(config).await?;
    Ok(())
}

async fn run_scheduler() -> Result<(), Box<dyn std::error::Error>> {
    let settings = settings()?;
    acme_showcase::app::build_app_for_url(settings.sqlx_url())?
        .run_scheduler()
        .await?;
    Ok(())
}

async fn run_web(options: WebOptions) -> Result<(), Box<dyn std::error::Error>> {
    let database_url = match options.database_url {
        Some(url) => url,
        None => settings()?.sqlx_url().to_owned(),
    };
    let loglevel = options
        .loglevel
        .parse::<horsies::LogLevel>()
        .map_err(|error| format!("invalid --loglevel: {error}"))?;
    horsies::execute_web(horsies::WebArgs {
        config: None,
        database_url: Some(database_url),
        session_database_url: options.session_database_url,
        pgbouncer_transaction_mode: false,
        host: options.host,
        port: options.port,
        auth: options.auth,
        trusted_header: options.trusted_header,
        enable_actions: options.enable_actions,
        custom_css_url: options.custom_css_url,
        loglevel,
    })
    .await
    .map_err(|error| Box::<dyn std::error::Error>::from(error))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Seed => acme_showcase::scenarios::seed::run()
            .await
            .map_err(Into::into),
        Command::Worker(options) => run_worker(options).await,
        Command::Scheduler => run_scheduler().await,
        Command::Web(options) => run_web(options).await,
        Command::Steady {
            orders,
            cover_errors,
        } => acme_showcase::scenarios::steady::run(orders, cover_errors)
            .await
            .map_err(Into::into),
        Command::Rush => acme_showcase::scenarios::rush::run()
            .await
            .map_err(Into::into),
        Command::ProblemChild => acme_showcase::scenarios::problem_child::run()
            .await
            .map_err(Into::into),
        Command::BulkImport => acme_showcase::scenarios::bulk_import::run()
            .await
            .map_err(Into::into),
        Command::FlashSale => acme_showcase::scenarios::flash_sale::run()
            .await
            .map_err(Into::into),
        Command::Chaos => acme_showcase::scenarios::chaos::run()
            .await
            .map_err(Into::into),
        Command::Maintenance => acme_showcase::scenarios::maintenance::run()
            .await
            .map_err(Into::into),
        Command::Simulate => {
            println!(
                "demand factor: {:.3}; catalog={} products; rush={} orders",
                acme_showcase::simulate::demand_factor(0.0),
                acme_showcase::tuning::CATALOG_SIZE,
                acme_showcase::tuning::RUSH_ORDER_COUNT,
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_exposes_every_scenario_and_service_command() {
        for command in [
            "seed",
            "worker",
            "scheduler",
            "rush",
            "problem-child",
            "bulk-import",
            "flash-sale",
            "chaos",
            "maintenance",
            "simulate",
        ] {
            Cli::try_parse_from(["acme", command]).expect(command);
        }
        Cli::try_parse_from(["acme", "steady", "--orders", "3", "--cover-errors"]).expect("steady");
        Cli::try_parse_from([
            "acme",
            "web",
            "--database-url",
            "postgresql://localhost/acme_demo",
            "--auth",
            "none",
        ])
        .expect("web");
    }
}
