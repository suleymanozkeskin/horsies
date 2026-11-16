//! A migrated database of its own for one monitoring read test.
//!
//! The worker list, the fleet check and the class report read every row of
//! their tables, so their tests do not share a database with other tests
//! (Python empties the tables instead: `tests/integration/test_class_coverage.py`,
//! `empty_fleet`).

use std::str::FromStr;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgConnection, PgPool};
use uuid::Uuid;

use crate::broker::migrations::run_horsies_migrations;
use crate::broker::PostgresBroker;

/// Every database this helper creates has this name prefix, then 32 hex
/// characters.
const MONITORING_TEST_DATABASE_PREFIX: &str = "core_monitoring_";

fn test_database_url() -> String {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return url;
    }
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let root = std::path::Path::new(manifest_dir)
        .ancestors()
        .find(|path| path.join(".env").exists());
    let password = root
        .and_then(|path| std::fs::read_to_string(path.join(".env")).ok())
        .and_then(|contents| {
            contents
                .lines()
                .filter_map(|line| line.trim().split_once('='))
                .find(|(key, _)| key.trim() == "DB_PASSWORD")
                .map(|(_, value)| value.trim().to_owned())
        })
        .unwrap_or_else(|| "W0rklane".to_owned());
    format!("postgresql://postgres:{password}@localhost:5432/horsies-core")
}

/// One migrated database. [`MonitoringTestDatabase::drop`] removes it; a test
/// that panics before the drop leaves it behind under the prefix.
pub(crate) struct MonitoringTestDatabase {
    pub(crate) pool: PgPool,
    name: String,
    admin_options: PgConnectOptions,
}

impl MonitoringTestDatabase {
    pub(crate) async fn create() -> Self {
        let base_options = PgConnectOptions::from_str(&test_database_url())
            .expect("invalid monitoring test database URL");
        let admin_options = base_options.clone().database("postgres");
        let name = format!(
            "{MONITORING_TEST_DATABASE_PREFIX}{}",
            Uuid::new_v4().simple()
        );
        let mut admin = PgConnection::connect_with(&admin_options)
            .await
            .expect("connect to the admin database");
        sqlx::query(&format!("CREATE DATABASE \"{name}\""))
            .execute(&mut admin)
            .await
            .expect("create the monitoring test database");
        admin.close().await.expect("close the admin connection");
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(base_options.database(&name))
            .await
            .expect("connect to the monitoring test database");
        run_horsies_migrations(&pool)
            .await
            .expect("migrate the monitoring test database");
        Self {
            pool,
            name,
            admin_options,
        }
    }

    /// A broker on this database.
    pub(crate) fn broker(&self) -> PostgresBroker {
        PostgresBroker::from_pool(self.pool.clone())
    }

    pub(crate) async fn drop(self) {
        self.pool.close().await;
        let mut admin = PgConnection::connect_with(&self.admin_options)
            .await
            .expect("connect to the admin database");
        sqlx::query(&format!(
            "DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)",
            self.name
        ))
        .execute(&mut admin)
        .await
        .expect("drop the monitoring test database");
    }
}
