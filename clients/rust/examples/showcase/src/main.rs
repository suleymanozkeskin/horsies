//! The `acme` binary: the showcase's own command line.
//!
//! The worker starts this binary again for each child, so `main` serves
//! child mode first (`horsies::child_mode`, `Horsies::serve_child`). Any
//! other start runs one command: the worker, the scheduler and the check
//! through the plain methods of the app, the monitoring UI through
//! `horsies::web::router`, or a scenario that places orders.

use std::process::ExitCode;

use acme_showcase::app::{build_app, WORKER_PROFILE};
use acme_showcase::scenarios;
use clap::{Args, Parser, Subcommand};
use horsies::{AppRole, ChildArgs, ChildHostExit, ChildMode, SchemaMode, WorkerRunOptions};

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
    /// Run a worker; its children are this binary in child mode.
    Worker(WorkerOptions),
    /// Run the scheduler of the app's schedule table.
    Scheduler,
    /// Check the app, its workflows and its schedules without a database.
    Check,
    /// Serve the Horsies monitoring UI.
    #[cfg(feature = "web")]
    Web(web::WebOptions),
    /// Place a bounded or open-ended stream of orders.
    Steady {
        #[arg(long)]
        orders: Option<usize>,
        #[arg(long)]
        cover_errors: bool,
        /// Divide the tuned inter-arrival delay by this factor.
        #[arg(long, default_value_t = 1.0)]
        pace: f64,
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
struct WorkerOptions {
    /// The worker profile of the app config.
    #[arg(long, default_value = WORKER_PROFILE)]
    profile: String,
}

fn main() -> ExitCode {
    install_logs();
    match horsies::child_mode() {
        ChildMode::Child(args) => serve_child(args),
        ChildMode::BadArguments(error) => {
            eprintln!("acme: child mode cannot start: {error}");
            exit_code(ChildHostExit::BadArguments.exit_code())
        }
        ChildMode::Worker => run_command(Cli::parse().command),
    }
}

/// One `tracing` subscriber on stderr at `RUST_LOG`, else `info`. A worker
/// child keeps it: `serve_child` installs none.
fn install_logs() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_unset| tracing_subscriber::EnvFilter::new("info"));
    let _installed = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

/// The process exit status of an exit code in `0..=255`.
fn exit_code(code: i32) -> ExitCode {
    ExitCode::from(u8::try_from(code).unwrap_or(u8::MAX))
}

/// Serve as a worker child: the same app as the worker, then the child host
/// until the worker stops the child.
fn serve_child(args: ChildArgs) -> ExitCode {
    match build_app(SchemaMode::MigrateAndValidate) {
        Ok(showcase) => showcase.app.serve_child(args),
        Err(refused) => {
            eprintln!("acme: the child cannot build the app: {refused}");
            ExitCode::FAILURE
        }
    }
}

/// Run `command` on a tokio runtime of its own and print its failure.
fn run_command(command: Command) -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("acme: the runtime did not start: {error}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(command)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(failure) => {
            eprintln!("acme: {failure}");
            ExitCode::FAILURE
        }
    }
}

async fn run(command: Command) -> Result<(), String> {
    match command {
        Command::Seed => scenarios::seed::run().await,
        Command::Worker(options) => run_worker(options).await,
        Command::Scheduler => run_scheduler().await,
        Command::Check => check(),
        #[cfg(feature = "web")]
        Command::Web(options) => web::run(options).await,
        Command::Steady {
            orders,
            cover_errors,
            pace,
        } => scenarios::steady::run(orders, cover_errors, pace).await,
        Command::Rush => scenarios::rush::run().await,
        Command::ProblemChild => scenarios::problem_child::run().await,
        Command::BulkImport => scenarios::bulk_import::run().await,
        Command::FlashSale => scenarios::flash_sale::run().await,
        Command::Chaos => scenarios::chaos::run().await,
        Command::Maintenance => scenarios::maintenance::run().await,
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

/// Run a worker of `options.profile` until `SIGTERM` or `SIGINT`.
async fn run_worker(options: WorkerOptions) -> Result<(), String> {
    let showcase = build_app(SchemaMode::MigrateAndValidate).map_err(|error| error.to_string())?;
    showcase
        .app
        .run_worker(WorkerRunOptions::profile(options.profile))
        .await
        .map(|_stopped| ())
        .map_err(|error| error.to_string())
}

/// Run the scheduler until `SIGTERM` or `SIGINT`.
async fn run_scheduler() -> Result<(), String> {
    let mut showcase =
        build_app(SchemaMode::MigrateAndValidate).map_err(|error| error.to_string())?;
    showcase.app.set_role(AppRole::Scheduler);
    showcase
        .app
        .run_scheduler()
        .await
        .map(|_stopped| ())
        .map_err(|error| error.to_string())
}

/// The static check of the app: its workflows, task options and schedules.
fn check() -> Result<(), String> {
    let showcase = build_app(SchemaMode::MigrateAndValidate).map_err(|error| error.to_string())?;
    let passed = showcase.app.check().map_err(|report| report.to_string())?;
    println!("{passed}");
    Ok(())
}

/// The monitoring UI of the showcase database, served with the app's
/// workflow definitions, so a resume can build the sub-workflows it owes.
#[cfg(feature = "web")]
mod web {
    use clap::{Args, ValueEnum};
    use horsies::web::{
        Actions, AllowAll, MonitoringConfig, MonitoringUiConfig, TraceLink, TrustedHeader, ViewOnly,
    };
    use horsies::SchemaMode;

    use acme_showcase::app::build_app;

    #[derive(Debug, Clone, Args)]
    pub(crate) struct WebOptions {
        /// Bind address.
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        /// Bind port.
        #[arg(long, default_value_t = 8600)]
        port: u16,
        /// none (a loopback host only) or trusted-header.
        #[arg(long, value_enum, default_value_t = AuthMode::None)]
        auth: AuthMode,
        /// The identity header that the reverse proxy sets.
        #[arg(long, default_value = "X-Forwarded-User")]
        trusted_header: String,
        /// Allow cancel, pause and resume (default: view only).
        #[arg(long)]
        enable_actions: bool,
        /// A stylesheet URL that the page loads after its own.
        #[arg(long)]
        custom_css_url: Option<String>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
    pub(crate) enum AuthMode {
        None,
        TrustedHeader,
    }

    /// Serve the UI until `SIGTERM` or `SIGINT`. The app never runs schema
    /// DDL (`SchemaMode::ObserveOnly`).
    pub(crate) async fn run(options: WebOptions) -> Result<(), String> {
        let showcase = build_app(SchemaMode::ObserveOnly).map_err(|error| error.to_string())?;
        let monitoring = MonitoringConfig {
            ui: MonitoringUiConfig {
                custom_css_url: options.custom_css_url.clone(),
                trace_link: TraceLink::NoTraceLink,
            },
            actions: match options.enable_actions {
                true => Actions::Enabled,
                false => Actions::Disabled,
            },
        };
        let router = match (options.auth, options.enable_actions) {
            (AuthMode::TrustedHeader, _) => {
                let policy = TrustedHeader::new(&options.trusted_header, options.enable_actions)
                    .map_err(|error| format!("invalid --trusted-header: {error}"))?;
                eprintln!(
                    "SECURITY: the reverse proxy in front of this server must strip or overwrite \
                     the {} header on each incoming request",
                    options.trusted_header
                );
                horsies::web::router(&showcase.app, monitoring, policy)
            }
            (AuthMode::None, _) if !is_loopback_host(&options.host) => {
                return Err(format!(
                    "--auth none is only allowed on a loopback host; {:?} is reachable from the \
                     network. Use --auth trusted-header behind a proxy.",
                    options.host
                ));
            }
            (AuthMode::None, true) => horsies::web::router(&showcase.app, monitoring, AllowAll),
            (AuthMode::None, false) => horsies::web::router(&showcase.app, monitoring, ViewOnly),
        };
        let listener = tokio::net::TcpListener::bind((options.host.as_str(), options.port))
            .await
            .map_err(|error| format!("bind {}:{}: {error}", options.host, options.port))?;
        let address = listener
            .local_addr()
            .map_err(|error| format!("the bound address cannot be read: {error}"))?;
        println!("monitoring UI on http://{address}");
        axum::serve(listener, router)
            .with_graceful_shutdown(stop_signal())
            .await
            .map_err(|error| format!("the monitoring server failed: {error}"))
    }

    /// Whether a bind address reaches only this machine. A host that does
    /// not parse as an address counts as reachable from the network.
    fn is_loopback_host(host: &str) -> bool {
        host == "localhost"
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    }

    /// Complete at `SIGINT` or `SIGTERM`. A signal that cannot be watched is
    /// reported, and the other one still stops the server.
    async fn stop_signal() {
        let interrupt = async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                eprintln!("acme: SIGINT is not watched: {error}");
                std::future::pending::<()>().await;
            }
        };
        let terminate = async {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut terminate) => {
                    let _received = terminate.recv().await;
                }
                Err(error) => {
                    eprintln!("acme: SIGTERM is not watched: {error}");
                    std::future::pending::<()>().await;
                }
            }
        };
        tokio::select! {
            () = interrupt => {}
            () = terminate => {}
        }
    }

    #[cfg(test)]
    mod tests {
        use super::is_loopback_host;

        #[test]
        fn only_a_loopback_host_counts_as_local() {
            for host in ["127.0.0.1", "::1", "localhost"] {
                assert!(is_loopback_host(host), "{host}");
            }
            for host in ["0.0.0.0", "192.168.1.10", "acme.example.invalid"] {
                assert!(!is_loopback_host(host), "{host}");
            }
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
            "check",
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
        Cli::try_parse_from(["acme", "worker", "--profile", "acme"]).expect("worker profile");
    }

    #[test]
    fn the_worker_runs_the_acme_profile_by_default() {
        let cli = Cli::try_parse_from(["acme", "worker"]).expect("worker");
        match cli.command {
            Command::Worker(options) => assert_eq!(options.profile, WORKER_PROFILE),
            other => panic!("expected worker, got {other:?}"),
        }
    }

    #[cfg(feature = "web")]
    #[test]
    fn web_takes_the_auth_mode() {
        Cli::try_parse_from(["acme", "web", "--auth", "none"]).expect("web");
        Cli::try_parse_from([
            "acme",
            "web",
            "--host",
            "0.0.0.0",
            "--auth",
            "trusted-header",
        ])
        .expect("web behind a proxy");
    }
}
