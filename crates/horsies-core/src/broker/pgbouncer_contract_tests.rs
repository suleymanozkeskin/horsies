//! PgBouncer contracts of the broker, against the fixture of
//! `tests/fixtures/pgbouncer/` (PostgreSQL direct, and PgBouncer in
//! transaction, session and statement mode, each pool with
//! `max_prepared_statements = 0`).
//!
//! Python specification: `tests/integration/test_pgbouncer_contract.py` and
//! `test_pgbouncer_prepared_statements.py` (baseline versions on
//! `program/0.8.0`).
//!
//! The tests are ignored by default. Run them with the fixture up and
//! `HORSIES_PGBOUNCER_TEST=1`:
//!
//! ```text
//! docker compose -f tests/fixtures/pgbouncer/compose.yaml up -d --wait
//! HORSIES_PGBOUNCER_TEST=1 cargo test -p horsies-core pgbouncer_contract -- --ignored --test-threads=1
//! ```
//!
//! The URLs come from the same variables as Python's
//! `tests/pgbouncer_utils.py` (`HORSIES_TEST_DATABASE_URL_DIRECT`,
//! `..._TRANSACTION`, `..._SESSION`, `..._STATEMENT`), with its defaults.
//! Each test creates a database of its own through the direct URL; every
//! PgBouncer pool routes any database name to the server.

use chrono::Utc;
use serial_test::serial;
use sqlx::{Connection, PgConnection, PgPool};
use uuid::Uuid;

use crate::broker::{BrokerError, PostgresBroker, SendKind};
use crate::core::config::{EnqueueRetention, PostgresConfig};
use crate::core::history::identity::uuid7::mint_task_id;
use crate::core::task::send_types::{send_error_from_enqueue, TaskSendErrorCode, TaskSendPayload};

/// Enqueues of the transaction-pool test: enough statements to meet a
/// prepared-statement collision if the broker used named statements.
const REPEATED_ENQUEUES: usize = 30;

/// Statements on one session connection that must all run on one backend.
const SESSION_STATEMENTS: usize = 8;

/// The advisory lock key of the session test (Python's key).
const SESSION_LOCK_KEY: i64 = 918_273_645;

/// The URLs of one test database through each endpoint of the fixture.
struct PgbouncerUrls {
    database_name: String,
    admin: String,
    direct: String,
    transaction: String,
    session: String,
    statement: String,
}

fn require_fixture() {
    assert_eq!(
        std::env::var("HORSIES_PGBOUNCER_TEST").as_deref(),
        Ok("1"),
        "the PgBouncer contract tests need the fixture of tests/fixtures/pgbouncer \
         and HORSIES_PGBOUNCER_TEST=1"
    );
}

/// The template URL of one endpoint: the variable, else Python's default.
/// A driver suffix in the scheme (`postgresql+psycopg://`) is removed: the
/// core takes `postgresql://` only.
fn template_url(variable: &str, port: u16) -> String {
    let url = std::env::var(variable).unwrap_or_else(|_| {
        let password = std::env::var("DB_PASSWORD").unwrap_or_else(|_| "testpassword".to_owned());
        format!("postgresql://postgres:{password}@localhost:{port}/horsies")
    });
    url.replacen("postgresql+psycopg://", "postgresql://", 1)
}

/// The same URL with another database name; the query is kept.
fn with_database(url: &str, database: &str) -> String {
    let (scheme, rest) = url.split_once("://").expect("the URL has a scheme");
    let authority_end = rest
        .find(|character: char| character == '/' || character == '?')
        .unwrap_or(rest.len());
    let (authority, remainder) = rest.split_at(authority_end);
    match remainder.split_once('?') {
        Some((_, query)) => format!("{scheme}://{authority}/{database}?{query}"),
        None => format!("{scheme}://{authority}/{database}"),
    }
}

/// The URL with one more query parameter.
fn with_query_parameter(url: &str, parameter: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}{parameter}")
}

impl PgbouncerUrls {
    async fn create(label: &str) -> Self {
        require_fixture();
        let direct = template_url("HORSIES_TEST_DATABASE_URL_DIRECT", 15432);
        let database_name = format!("horsies_core_pgbouncer_{label}_{}", Uuid::new_v4().simple());
        let admin = with_database(&direct, "postgres");
        let mut connection = PgConnection::connect(&admin)
            .await
            .expect("connect to the admin database");
        sqlx::query(&format!("CREATE DATABASE \"{database_name}\""))
            .execute(&mut connection)
            .await
            .expect("create the test database");
        connection
            .close()
            .await
            .expect("close the admin connection");
        Self {
            admin,
            direct: with_database(&direct, &database_name),
            transaction: with_database(
                &template_url("HORSIES_TEST_DATABASE_URL_TRANSACTION", 16432),
                &database_name,
            ),
            session: with_database(
                &template_url("HORSIES_TEST_DATABASE_URL_SESSION", 16433),
                &database_name,
            ),
            statement: with_database(
                &template_url("HORSIES_TEST_DATABASE_URL_STATEMENT", 16434),
                &database_name,
            ),
            database_name,
        }
    }

    async fn drop(self) {
        let mut connection = PgConnection::connect(&self.admin)
            .await
            .expect("connect to the admin database");
        sqlx::query(&format!(
            "DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)",
            self.database_name
        ))
        .execute(&mut connection)
        .await
        .expect("drop the test database");
        connection
            .close()
            .await
            .expect("close the admin connection");
    }
}

/// Python's pool settings of these tests: two connections, no overflow.
fn sized(mut config: PostgresConfig) -> PostgresConfig {
    config.pool_size = 2;
    config.max_overflow = 0;
    config
}

fn probe_payload(task_name: &str) -> TaskSendPayload {
    TaskSendPayload {
        task_name: task_name.to_owned(),
        queue_name: "default".to_owned(),
        priority: 100,
        input_json: "{}".to_owned(),
        sent_at: Utc::now(),
        good_until: None,
        enqueue_delay_seconds: None,
        task_options: None,
        idempotency_key: None,
        retention: EnqueueRetention::Forever,
    }
}

async fn enqueue_probe(
    broker: &PostgresBroker,
    task_id: Uuid,
    payload: &TaskSendPayload,
) -> Result<Uuid, BrokerError> {
    broker
        .enqueue(
            &payload.task_name,
            &payload.input_json,
            &payload.queue_name,
            payload.priority,
            Some(payload.sent_at),
            None,
            None,
            None,
            Some(task_id),
            None,
            None,
            payload.retention,
            None,
            SendKind::First,
        )
        .await
}

async fn direct_pool(url: &str) -> PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect the direct pool")
}

/// Python `test_pgbouncer_contract.py::test_transaction_pool_uses_pooled_runtime_and_direct_session`
/// and `test_pgbouncer_prepared_statements.py::test_horsies_transaction_mode_disables_psycopg_prepared_statements`:
/// a transaction-pooled runtime URL with a direct session URL installs the
/// schema, passes the health and session checks, keeps one backend for a
/// session lock on the session pool, and takes repeated enqueues through a
/// pool that tracks no prepared statements.
#[tokio::test]
#[serial]
#[ignore = "needs the PgBouncer fixture of tests/fixtures/pgbouncer and HORSIES_PGBOUNCER_TEST=1"]
async fn a_transaction_pool_with_a_direct_session_installs_checks_locks_and_enqueues() {
    let urls = PgbouncerUrls::create("transaction").await;
    let broker = PostgresBroker::connect_with(
        &sized(PostgresConfig::from_pgbouncer_urls(
            urls.transaction.clone(),
            urls.direct.clone(),
        )),
        crate::ClientFamily::Rust,
    )
    .await
    .expect("connect the broker");

    broker.migrate().await.expect("install the schema");
    broker.health_check().await.expect("the health check");
    broker
        .check_worker_session()
        .await
        .expect("the session URL delivers a NOTIFY");

    let mut session = broker
        .session_pool()
        .acquire()
        .await
        .expect("check out a session connection");
    let backend: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *session)
        .await
        .expect("read the backend");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(SESSION_LOCK_KEY)
        .execute(&mut *session)
        .await
        .expect("take the session lock");
    for _ in 0..SESSION_STATEMENTS {
        let current: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *session)
            .await
            .expect("read the backend");
        assert_eq!(current, backend, "the session connection changed backend");
    }
    let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
        .bind(SESSION_LOCK_KEY)
        .fetch_one(&mut *session)
        .await
        .expect("release the session lock");
    assert!(released, "the session lock was not held at the unlock");
    drop(session);

    let mut task_ids = Vec::with_capacity(REPEATED_ENQUEUES);
    for index in 0..REPEATED_ENQUEUES {
        let payload = probe_payload(&format!("pgbouncer_transaction_probe_{index}"));
        let task_id = mint_task_id().expect("mint a task id");
        task_ids.push(
            enqueue_probe(&broker, task_id, &payload)
                .await
                .expect("an enqueue through the transaction pool"),
        );
    }
    let direct = direct_pool(&urls.direct).await;
    let stored: i64 =
        sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE id = ANY($1::uuid[])")
            .bind(&task_ids)
            .fetch_one(&direct)
            .await
            .expect("count the enqueued rows");

    assert_eq!(stored, REPEATED_ENQUEUES as i64);
    direct.close().await;
    broker.close().await;
    urls.drop().await;
}

/// Python `test_pgbouncer_contract.py::test_transaction_pool_session_behavior_is_rejected`:
/// a session URL that reaches the transaction pool keeps no session, so
/// the worker's session check refuses it. The same URL is refused again, so
/// the refusal is not retryable.
#[tokio::test]
#[serial]
#[ignore = "needs the PgBouncer fixture of tests/fixtures/pgbouncer and HORSIES_PGBOUNCER_TEST=1"]
async fn a_session_url_on_the_transaction_pool_fails_the_worker_session_check() {
    let urls = PgbouncerUrls::create("bad_session").await;
    let session_url = with_query_parameter(
        &urls.transaction,
        "application_name=horsies_bad_session_probe",
    );
    let broker = PostgresBroker::connect_with(
        &sized(PostgresConfig::from_pgbouncer_urls(
            urls.transaction.clone(),
            session_url,
        )),
        crate::ClientFamily::Rust,
    )
    .await
    .expect("connect the broker");

    let refused = broker
        .check_worker_session()
        .await
        .expect_err("the transaction pool keeps no session");

    assert!(!refused.is_retryable(), "{refused}");
    broker.close().await;
    urls.drop().await;
}

/// Python `test_pgbouncer_contract.py::test_session_pool_works_without_transaction_mode`:
/// a session-pooled URL, with no transaction mode, installs the schema,
/// takes an enqueue and delivers a NOTIFY to a LISTEN.
#[tokio::test]
#[serial]
#[ignore = "needs the PgBouncer fixture of tests/fixtures/pgbouncer and HORSIES_PGBOUNCER_TEST=1"]
async fn a_session_pooled_url_installs_enqueues_and_delivers_a_notify() {
    let urls = PgbouncerUrls::create("session").await;
    let broker = PostgresBroker::connect_with(
        &sized(PostgresConfig::from_url(urls.session.clone())),
        crate::ClientFamily::Rust,
    )
    .await
    .expect("connect the broker");
    broker.migrate().await.expect("install the schema");
    let task_id = mint_task_id().expect("mint a task id");

    let enqueued = enqueue_probe(&broker, task_id, &probe_payload("pgbouncer_session_probe"))
        .await
        .expect("an enqueue through the session pool");
    broker
        .check_worker_session()
        .await
        .expect("the session pool delivers a NOTIFY");

    assert_eq!(enqueued, task_id);
    let direct = direct_pool(&urls.direct).await;
    let stored: i64 = sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(&direct)
        .await
        .expect("count the enqueued row");
    assert_eq!(stored, 1);
    direct.close().await;
    broker.close().await;
    urls.drop().await;
}

/// Python `test_pgbouncer_contract.py::test_statement_pool_fails_loudly_for_runtime_transactions`,
/// with the decided flag: a statement-pooled runtime URL cannot keep a
/// transaction, so the first send through it fails as `ENQUEUE_FAILED`
/// that is not retryable (decision "Public API changes forced by earlier
/// decisions": a statement-pooled URL answers `retryable = false`).
#[tokio::test]
#[serial]
#[ignore = "needs the PgBouncer fixture of tests/fixtures/pgbouncer and HORSIES_PGBOUNCER_TEST=1"]
async fn a_statement_pooled_runtime_url_fails_the_send_as_not_retryable() {
    let urls = PgbouncerUrls::create("statement").await;
    let broker = PostgresBroker::connect_with(
        &sized(PostgresConfig::from_pgbouncer_urls(
            urls.statement.clone(),
            urls.direct.clone(),
        )),
        crate::ClientFamily::Rust,
    )
    .await
    .expect("connect the broker");
    broker
        .migrate()
        .await
        .expect("install the schema on the direct session URL");
    let task_id = mint_task_id().expect("mint a task id");
    let payload = probe_payload("pgbouncer_statement_probe");

    let refused = enqueue_probe(&broker, task_id, &payload)
        .await
        .expect_err("a statement pool keeps no transaction");

    assert!(!refused.is_retryable(), "{refused}");
    let send_error = send_error_from_enqueue(task_id, payload, &refused);
    assert_eq!(send_error.code(), TaskSendErrorCode::EnqueueFailed);
    assert!(!send_error.retryable());
    let direct = direct_pool(&urls.direct).await;
    let stored: i64 = sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(&direct)
        .await
        .expect("count the refused row");
    assert_eq!(stored, 0);
    direct.close().await;
    broker.close().await;
    urls.drop().await;
}
