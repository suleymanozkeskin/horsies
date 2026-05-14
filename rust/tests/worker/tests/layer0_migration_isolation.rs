#![allow(clippy::unwrap_used)]

//! Verifies that `broker.migrate()` writes its bookkeeping to
//! `horsies_migrations` and leaves the application-owned `_sqlx_migrations`
//! table alone.

use horsies::{run_horsies_migrations, PostgresBroker};
use horsies_test_support::db;
use serial_test::serial;
use sqlx::{Executor, PgPool};

async fn table_exists(pool: &PgPool, name: &str) -> bool {
    let regclass: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(name)
        .fetch_one(pool)
        .await
        .unwrap();
    regclass.is_some()
}

async fn row_count(pool: &PgPool, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Pre-create `_sqlx_migrations` with a fake application row, run horsies
/// migrate, and assert that:
/// - `horsies_migrations` is created and populated
/// - `_sqlx_migrations` is untouched (still has exactly the app's one row,
///   no horsies versions copied in)
#[tokio::test]
#[serial]
async fn horsies_migrate_does_not_write_to_sqlx_migrations() {
    let db_url = db::create_empty_database().await;
    let pool = PgPool::connect(&db_url).await.unwrap();

    sqlx::query(
        "CREATE TABLE _sqlx_migrations (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
            success BOOLEAN NOT NULL,
            checksum BYTEA NOT NULL,
            execution_time BIGINT NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Simulate one row from a host application's own sqlx migrator.
    sqlx::query(
        "INSERT INTO _sqlx_migrations \
             (version, description, success, checksum, execution_time) \
         VALUES (99999, 'host app migration', TRUE, $1, 0)",
    )
    .bind(vec![0u8; 32])
    .execute(&pool)
    .await
    .unwrap();

    let broker = PostgresBroker::from_pool(pool.clone());
    broker.migrate().await.unwrap();

    assert!(table_exists(&pool, "horsies_migrations").await);
    assert!(row_count(&pool, "horsies_migrations").await > 0);

    let sqlx_rows = row_count(&pool, "_sqlx_migrations").await;
    assert_eq!(
        sqlx_rows, 1,
        "_sqlx_migrations should be untouched (still contains only the host app row)"
    );
    let host_versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(host_versions, vec![99999]);

    pool.close().await;
    db::drop_database(&db_url).await;
}

/// On fresh databases with no `_sqlx_migrations`, horsies must still
/// initialize correctly and not create that table.
#[tokio::test]
#[serial]
async fn horsies_migrate_on_clean_db_does_not_create_sqlx_table() {
    let db_url = db::create_empty_database().await;
    let pool = PgPool::connect(&db_url).await.unwrap();

    let broker = PostgresBroker::from_pool(pool.clone());
    broker.migrate().await.unwrap();

    assert!(table_exists(&pool, "horsies_migrations").await);
    assert!(
        !table_exists(&pool, "_sqlx_migrations").await,
        "horsies must not create _sqlx_migrations on a clean DB"
    );

    pool.close().await;
    db::drop_database(&db_url).await;
}

/// Running migrate twice is a no-op (idempotent) and does not duplicate rows.
#[tokio::test]
#[serial]
async fn horsies_migrate_is_idempotent() {
    let db_url = db::create_empty_database().await;
    let pool = PgPool::connect(&db_url).await.unwrap();

    run_horsies_migrations(&pool).await.unwrap();
    let first = row_count(&pool, "horsies_migrations").await;
    run_horsies_migrations(&pool).await.unwrap();
    let second = row_count(&pool, "horsies_migrations").await;

    assert_eq!(first, second, "second migrate run must not add rows");
    assert!(first > 0);

    pool.close().await;
    db::drop_database(&db_url).await;
}

/// If a prior alpha left horsies versions inside `_sqlx_migrations`, the
/// first run should backfill them into `horsies_migrations` so we don't
/// attempt to re-execute already-applied SQL.
#[tokio::test]
#[serial]
async fn horsies_migrate_backfills_from_legacy_sqlx_migrations() {
    let db_url = db::create_empty_database().await;
    let pool = PgPool::connect(&db_url).await.unwrap();

    // Simulate the legacy state: a previous horsies alpha ran sqlx::migrate!()
    // which created `_sqlx_migrations` and applied all our migrations into it.
    sqlx::query(
        "CREATE TABLE _sqlx_migrations (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
            success BOOLEAN NOT NULL,
            checksum BYTEA NOT NULL,
            execution_time BIGINT NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    let legacy_migrator = sqlx::migrate!("../../horsies/migrations");
    for migration in legacy_migrator.iter() {
        if migration.migration_type.is_down_migration() {
            continue;
        }
        pool.execute(migration.sql.as_ref()).await.unwrap();
        sqlx::query(
            "INSERT INTO _sqlx_migrations \
                 (version, description, success, checksum, execution_time) \
             VALUES ($1, $2, TRUE, $3, 0)",
        )
        .bind(migration.version)
        .bind(migration.description.as_ref())
        .bind(migration.checksum.as_ref())
        .execute(&pool)
        .await
        .unwrap();
    }

    run_horsies_migrations(&pool).await.unwrap();

    let horsies_rows = row_count(&pool, "horsies_migrations").await;
    let legacy_rows = row_count(&pool, "_sqlx_migrations").await;
    assert_eq!(
        horsies_rows, legacy_rows,
        "all legacy versions should have been copied"
    );

    pool.close().await;
    db::drop_database(&db_url).await;
}

/// Regression for the alpha.14 backfill bug: when a host application has
/// already populated `_sqlx_migrations` with rows at horsies' version
/// numbers but with foreign checksums (its own migrations, not ours), the
/// backfill must copy *zero* rows and horsies' embedded migrations must
/// then apply fresh.
#[tokio::test]
#[serial]
async fn horsies_migrate_ignores_foreign_rows_at_same_versions() {
    let db_url = db::create_empty_database().await;
    let pool = PgPool::connect(&db_url).await.unwrap();

    sqlx::query(
        "CREATE TABLE _sqlx_migrations (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
            success BOOLEAN NOT NULL,
            checksum BYTEA NOT NULL,
            execution_time BIGINT NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Simulate a host app that ran its own sqlx migrator first: rows live
    // at versions 1..=16 (overlapping horsies' embedded range) but with
    // checksums that have nothing to do with horsies' migration SQL.
    let foreign_checksum = vec![0xAAu8; 48];
    for version in 1..=16i64 {
        sqlx::query(
            "INSERT INTO _sqlx_migrations \
                 (version, description, success, checksum, execution_time) \
             VALUES ($1, $2, TRUE, $3, 0)",
        )
        .bind(version)
        .bind(format!("host_app_migration_{version}"))
        .bind(&foreign_checksum)
        .execute(&pool)
        .await
        .unwrap();
    }

    run_horsies_migrations(&pool).await.unwrap();

    // The host app's rows must be left exactly as they were.
    let foreign_rows = row_count(&pool, "_sqlx_migrations").await;
    assert_eq!(foreign_rows, 16);

    // horsies_migrations must contain horsies' real rows (checksum-verified),
    // not the foreign ones we planted.
    let horsies_rows: Vec<(i64, Vec<u8>)> =
        sqlx::query_as("SELECT version, checksum FROM horsies_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(!horsies_rows.is_empty(), "embedded migrations must run");
    for (_v, checksum) in &horsies_rows {
        assert_ne!(
            checksum, &foreign_checksum,
            "horsies_migrations must not contain foreign checksums",
        );
    }

    pool.close().await;
    db::drop_database(&db_url).await;
}
