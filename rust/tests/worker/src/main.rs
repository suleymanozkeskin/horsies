#![allow(clippy::unwrap_used)]

//! Test worker binary for e2e tests.
//!
//! Mirrors Python's `tests/e2e/tasks/instance.py` — a worker binary with
//! all e2e test tasks compiled in.
//!
//! Usage:
//!   horsies-test-worker worker <db-url-or-config-path> [--concurrency N]
//!
//! Prints "worker started" to stderr when ready (e2e harness ready marker).

use horsies_test_worker::tasks;

use std::process::ExitCode;
use std::sync::Arc;

use horsies::{
    init_tracing, spawn_scheduler, AppConfig, Horsies, LogLevel, PostgresConfig, QueueMode,
    RecoveryConfig, Worker, WorkerConfig, WorkerResilienceConfig,
};

fn app_config_from_url(url: &str) -> AppConfig {
    AppConfig {
        queue_mode: QueueMode::Default,
        custom_queues: None,
        broker: PostgresConfig {
            database_url: url.to_owned(),
            session_database_url: None,
            pgbouncer_transaction_mode: false,
            pool_pre_ping: true,
            pool_size: 5,
            max_overflow: 5,
            pool_timeout: 10,
            pool_recycle: 600,
            echo: false,
        },
        cluster_wide_cap: None,
        prefetch_buffer: 0,
        claim_lease_ms: None,
        max_claim_renew_age_ms: 180_000,
        recovery: RecoveryConfig::default(),
        resilience: WorkerResilienceConfig::default(),
        schedule: None,
        resend_on_transient_err: false,
    }
}

fn load_config(source: &str) -> AppConfig {
    if source.starts_with("postgres://") || source.starts_with("postgresql://") {
        return app_config_from_url(source);
    }
    let content = std::fs::read_to_string(source)
        .unwrap_or_else(|e| panic!("failed to read config {}: {}", source, e));
    serde_json::from_str(&content)
        .unwrap_or_else(|e| panic!("failed to parse config {}: {}", source, e))
}

fn parse_concurrency(args: &[String]) -> u32 {
    for (i, arg) in args.iter().enumerate() {
        if arg == "--concurrency" {
            if let Some(val) = args.get(i + 1) {
                return val.parse().unwrap_or(4);
            }
        }
    }
    4
}

fn parse_queues(args: &[String]) -> Vec<String> {
    for (i, arg) in args.iter().enumerate() {
        if arg == "--queues" || arg == "-q" {
            if let Some(val) = args.get(i + 1) {
                return val.split(',').map(|s| s.trim().to_owned()).collect();
            }
        }
    }
    vec!["default".to_owned()]
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 3 {
        eprintln!("usage: horsies-test-worker <worker|scheduler> <config-or-db-url> [options]");
        return ExitCode::FAILURE;
    }

    let subcommand = args[1].as_str();
    if subcommand != "worker" && subcommand != "scheduler" {
        eprintln!("usage: horsies-test-worker <worker|scheduler> <config-or-db-url> [options]");
        return ExitCode::FAILURE;
    }

    init_tracing(LogLevel::Info);

    let config_source = &args[2];
    let app_config = load_config(config_source);
    let concurrency = parse_concurrency(&args);
    let queues = parse_queues(&args);

    // Validate.
    let errors = app_config.validate();
    if !errors.is_empty() {
        for e in &errors {
            tracing::error!("{}", e);
        }
        return ExitCode::FAILURE;
    }

    // Build app and register tasks.
    let mut app = match Horsies::new(app_config.clone()) {
        Ok(a) => a,
        Err(e) => {
            tracing::error!("Horsies::new failed: {}", e);
            return ExitCode::FAILURE;
        }
    };

    if let Err(e) = tasks::register(&mut app) {
        tracing::error!("task registration failed: {}", e);
        return ExitCode::FAILURE;
    }

    // Decompose app and connect broker.
    let (app_config, registry, wf_registry, broker) = match app.into_parts().await {
        Ok(parts) => parts,
        Err(e) => {
            tracing::error!("broker connect failed: {}", e);
            return ExitCode::FAILURE;
        }
    };

    if subcommand == "scheduler" {
        // Run scheduler.
        let Some(schedule_config) = app_config.schedule.clone() else {
            tracing::error!("no schedule config in AppConfig");
            return ExitCode::FAILURE;
        };

        let cancel = tokio_util::sync::CancellationToken::new();
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            cancel_clone.cancel();
        });

        eprintln!("scheduler started");

        let handle = spawn_scheduler(broker, schedule_config, app_config, cancel);
        let _ = handle.await;
    } else {
        // Run worker.
        let mut worker_config = WorkerConfig {
            queues,
            concurrency,
            ..WorkerConfig::default()
        };
        worker_config.apply_queue_config(&app_config);

        let worker = match Worker::new(
            broker,
            Arc::new(registry),
            Arc::new(wf_registry),
            app_config,
            worker_config,
        ) {
            Ok(w) => w,
            Err(e) => {
                tracing::error!("worker creation failed: {}", e);
                return ExitCode::FAILURE;
            }
        };

        let cancel = worker.cancel_token();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            cancel.cancel();
        });

        eprintln!("worker started");

        if let Err(e) = worker.run().await {
            tracing::error!("worker error: {}", e);
            return ExitCode::FAILURE;
        }
    }

    ExitCode::SUCCESS
}
