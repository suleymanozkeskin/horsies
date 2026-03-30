/// Database connection and cleanup helpers.
///
/// Mirrors Python's `conftest.py` database fixtures.
use sqlx::PgPool;

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
