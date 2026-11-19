//! The split of the main URL and the session URL against PostgreSQL: the
//! worker session check, the listener and the schema install use the session
//! URL; the runtime statements use the main URL.
//!
//! Python specification: `clients/python/tests/unit/test_broker_postgres.py`,
//! `TestPostgresBrokerPgBouncerWiring` (baseline version on `program/0.8.0`).
//! The Python tests mock the engines; here the two URLs name two databases,
//! or one database under two application names, so each URL's use shows in
//! the server's state. A NOTIFY reaches only a LISTEN on its own database.

use uuid::Uuid;

use crate::broker::{PostgresBroker, SessionCheckFailure};
use crate::core::config::PostgresConfig;
use crate::test_support::TestDatabase;

/// `url` with `application_name` added to its parameters.
fn with_application_name(url: &str, application_name: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}application_name={application_name}")
}

/// A PgBouncer transaction mode config (the main URL behind the pooler, the
/// session URL direct) with small pools.
fn split_config(database_url: String, session_url: String) -> PostgresConfig {
    let mut config = PostgresConfig::from_pgbouncer_urls(database_url, session_url);
    config.pool_size = 2;
    config.max_overflow = 0;
    config
}

/// Whether the database of `database` holds the migration bookkeeping table.
async fn holds_the_bookkeeping(database: &TestDatabase) -> bool {
    database
        .client()
        .query_one("SELECT to_regclass('horsies_migrations') IS NOT NULL", &[])
        .await
        .expect("read the catalog")
        .get(0)
}

/// The backends whose application name is `application_name`.
async fn backends_named(database: &TestDatabase, application_name: &str) -> i64 {
    database
        .client()
        .query_one(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1",
            &[&application_name],
        )
        .await
        .expect("count the backends")
        .get(0)
}

/// Python `test_worker_validation_rejects_failed_session_behavior`, and the
/// listener half of `test_runtime_engine_uses_database_url_and_listener_uses_session_url`:
/// the session check LISTENs through the session URL and NOTIFYs through the
/// main URL. With the session URL on another database the NOTIFY never
/// reaches the LISTEN, so the check fails as `NotCapable`, which is not
/// retryable. A listener on the main URL would have received it.
#[tokio::test]
async fn a_session_url_on_another_database_fails_the_session_check_as_not_retryable() {
    let main = TestDatabase::create().await;
    let session = TestDatabase::create_empty().await;
    let broker = PostgresBroker::connect_with(
        &split_config(main.url().to_owned(), session.url().to_owned()),
        crate::ClientFamily::Rust,
    )
    .await
    .expect("connect the broker");

    let refused = broker
        .check_worker_session()
        .await
        .expect_err("the NOTIFY on the main URL cannot reach the session URL's LISTEN");

    assert!(
        matches!(refused, SessionCheckFailure::NotCapable { .. }),
        "{refused:?}"
    );
    assert!(!refused.is_retryable(), "{refused}");
    broker.close().await;
    drop(broker);
    main.drop().await;
    session.drop().await;
}

/// Python `test_worker_validation_connects_both_paths_and_probes_session_url`:
/// with both URLs on one database, the session check passes, each URL has a
/// connection of its own, and a close closes the runtime pool.
#[tokio::test]
async fn a_split_session_url_passes_the_session_check_on_its_own_connection() {
    let database = TestDatabase::create().await;
    let suffix = Uuid::new_v4().simple().to_string();
    let main_name = format!("split_main_{suffix}");
    let session_name = format!("split_session_{suffix}");
    let broker = PostgresBroker::connect_with(
        &split_config(
            with_application_name(database.url(), &main_name),
            with_application_name(database.url(), &session_name),
        ),
        crate::ClientFamily::Rust,
    )
    .await
    .expect("connect the broker");

    broker
        .check_worker_session()
        .await
        .expect("the NOTIFY on the main URL reaches the session URL's LISTEN");

    assert!(backends_named(&database, &main_name).await >= 1);
    assert!(backends_named(&database, &session_name).await >= 1);
    broker.close().await;
    drop(broker);
    database.drop().await;
}

/// Python `test_schema_initialization_uses_session_url_when_split`: the
/// install runs through the session URL. With the two URLs on two empty
/// databases, the session URL's database is installed and the main URL's
/// database stays empty.
#[tokio::test]
async fn a_split_config_installs_the_schema_through_the_session_url() {
    let main = TestDatabase::create_empty().await;
    let session = TestDatabase::create_empty().await;
    let broker = PostgresBroker::connect_with(
        &split_config(main.url().to_owned(), session.url().to_owned()),
        crate::ClientFamily::Rust,
    )
    .await
    .expect("connect the broker");

    broker.migrate().await.expect("install the schema");

    assert!(holds_the_bookkeeping(&session).await);
    assert!(!holds_the_bookkeeping(&main).await);
    broker.close().await;
    drop(broker);
    main.drop().await;
    session.drop().await;
}
