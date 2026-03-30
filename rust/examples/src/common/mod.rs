#![allow(dead_code)]

pub mod custom_mode;
pub mod default_mode;
pub mod tasks;

use std::sync::Arc;

use horsies::{PostgresBroker, PostgresConfig};

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

/// Connect to Postgres and run migrations.
///
/// Returns an `Arc<PostgresBroker>` for sending tasks and retrieving results.
pub async fn connect_broker(db_url: &str) -> Arc<PostgresBroker> {
    let config = PostgresConfig::from_url(db_url);
    let broker = Arc::new(
        PostgresBroker::connect_with(&config)
            .await
            .expect("failed to connect to postgres"),
    );
    broker.migrate().await.expect("failed to run migrations");
    broker
}
