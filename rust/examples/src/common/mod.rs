#![allow(dead_code)]

pub mod custom_mode;
pub mod default_mode;
pub mod tasks;

use std::sync::Arc;

use horsies::{AppConfig, PostgresBroker, PostgresConfig};

/// Resolve the database URL.
///
/// 1. Returns `DATABASE_URL` env var if set.
/// 2. Otherwise reads `DB_PASSWORD` from `.env` at the project root and
///    builds `postgresql://postgres:<password>@localhost:5432/horsies-rust-port`.
/// 3. Exits with an error if neither source is available.
pub fn db_url() -> String {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return url;
    }

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let root = std::path::Path::new(manifest_dir)
        .ancestors()
        .find(|p| p.join(".env").exists());

    if let Some(root) = root {
        if let Ok(contents) = std::fs::read_to_string(root.join(".env")) {
            for line in contents.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                if let Some((key, value)) = line.split_once('=') {
                    if key.trim() == "DB_PASSWORD" {
                        let password = value.trim();
                        return format!(
                            "postgresql://postgres:{}@localhost:5432/horsies-rust-port",
                            password,
                        );
                    }
                }
            }
        }
    }

    eprintln!("error: could not determine database URL");
    eprintln!("set DATABASE_URL or add DB_PASSWORD to .env");
    std::process::exit(1);
}

/// Resolve an optional session-capable database URL.
///
/// When set, examples treat `DATABASE_URL` as the runtime SQL URL and this
/// value as the schema/LISTEN URL, enabling PgBouncer transaction-pool checks.
pub fn session_db_url() -> Option<String> {
    std::env::var("SESSION_DATABASE_URL")
        .or_else(|_| std::env::var("HORSIES_SESSION_DATABASE_URL"))
        .ok()
        .filter(|value| !value.trim().is_empty())
}

/// Build broker config from example environment variables.
pub fn postgres_config(db_url: &str) -> PostgresConfig {
    match session_db_url() {
        Some(session_url) => PostgresConfig::from_pgbouncer_urls(db_url, session_url),
        None => PostgresConfig::from_url(db_url),
    }
}

/// Build app config from example environment variables.
pub fn app_config(db_url: &str) -> AppConfig {
    let mut config = AppConfig::for_database_url(db_url);
    config.broker = postgres_config(db_url);
    config
}

/// Connect to Postgres and run migrations.
///
/// Returns an `Arc<PostgresBroker>` for sending tasks and retrieving results.
pub async fn connect_broker(db_url: &str) -> Arc<PostgresBroker> {
    let config = postgres_config(db_url);
    let broker = Arc::new(
        PostgresBroker::connect_with(&config)
            .await
            .expect("failed to connect to postgres"),
    );
    broker.migrate().await.expect("failed to run migrations");
    broker
}
