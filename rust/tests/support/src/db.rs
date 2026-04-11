/// Database connection and cleanup helpers.
///
/// Mirrors Python's `conftest.py` database fixtures.
use std::str::FromStr;

use sqlx::postgres::PgConnectOptions;
use sqlx::{Connection, PgPool};
use url::Url;
use uuid::Uuid;

/// Create a connection pool using `DATABASE_URL` env var.
pub async fn create_pool() -> PgPool {
    let url = db_url();
    PgPool::connect(&url)
        .await
        .expect("failed to connect to test database")
}

/// Run migrations on the pool.
pub async fn run_migrations(pool: &PgPool) {
    sqlx::migrate!("../../horsies/migrations")
        .run(pool)
        .await
        .expect("failed to run migrations");
}

/// Create a fresh empty database and return its connection URL.
///
/// The returned database has no horsies tables and no `_sqlx_migrations`
/// metadata yet. Call [`drop_database`] when finished.
pub async fn create_empty_database() -> String {
    let base_url = db_url();
    let mut admin_url = Url::parse(&base_url).expect("invalid DATABASE_URL");
    let db_name = format!("horsies_tmp_{}", Uuid::new_v4().simple());

    admin_url.set_path("/postgres");

    let mut conn = sqlx::PgConnection::connect(admin_url.as_str())
        .await
        .expect("failed to connect to postgres admin database");
    sqlx::query(&format!("CREATE DATABASE \"{}\"", db_name))
        .execute(&mut conn)
        .await
        .expect("failed to create empty test database");

    let mut fresh_url = Url::parse(&base_url).expect("invalid DATABASE_URL");
    fresh_url.set_path(&format!("/{}", db_name));
    fresh_url.to_string()
}

/// Drop a database created by [`create_empty_database`].
pub async fn drop_database(database_url: &str) {
    let mut admin_url = Url::parse(database_url).expect("invalid database URL");
    let connect = PgConnectOptions::from_str(database_url).expect("invalid database URL");
    let db_name = connect.get_database().expect("database name missing");
    admin_url.set_path("/postgres");

    let mut conn = sqlx::PgConnection::connect(admin_url.as_str())
        .await
        .expect("failed to connect to postgres admin database");
    sqlx::query(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
    )
    .bind(db_name)
    .execute(&mut conn)
    .await
    .expect("failed to terminate active connections");
    sqlx::query(&format!("DROP DATABASE IF EXISTS \"{}\"", db_name))
        .execute(&mut conn)
        .await
        .expect("failed to drop empty test database");
}

/// Truncate all horsies tables. Call before each test for isolation.
pub async fn clean_tables(pool: &PgPool) {
    sqlx::query(
        "TRUNCATE horsies_workflow_tasks, horsies_workflows, \
         horsies_tasks, horsies_heartbeats, horsies_task_attempts, \
         horsies_worker_states, horsies_schedule_state CASCADE",
    )
    .execute(pool)
    .await
    .expect("failed to truncate tables");
}

/// Truncate only workflow-related tables.
pub async fn clean_workflow_tables(pool: &PgPool) {
    sqlx::query("TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE")
        .execute(pool)
        .await
        .expect("failed to truncate workflow tables");
}

/// Resolve the database URL.
///
/// 1. Returns `DATABASE_URL` env var if set.
/// 2. Otherwise reads `DB_PASSWORD` from `.env` at the project root and
///    builds `postgresql://postgres:<password>@localhost:5432/horsies-rust-port`.
/// 3. Panics if neither source is available.
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

    panic!(
        "database URL not found: set DATABASE_URL or add DB_PASSWORD to .env at the project root"
    );
}
