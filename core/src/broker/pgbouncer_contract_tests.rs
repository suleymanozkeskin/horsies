//! PgBouncer contracts of the broker, against the fixture of
//! `clients/python/tests/fixtures/pgbouncer/` (PostgreSQL direct, and PgBouncer in
//! transaction, session and statement mode, each pool with
//! `max_prepared_statements = 0`).
//!
//! Python specification: `clients/python/tests/integration/test_pgbouncer_contract.py` and
//! `test_pgbouncer_prepared_statements.py` (baseline versions on
//! `program/0.8.0`).
//!
//! The tests are ignored unless the feature `pgbouncer-fixture-tests` is on.
//! Run them with the fixture up (the `rust-pgbouncer` job of
//! `.github/workflows/tests-full.yml` does):
//!
//! ```text
//! docker compose -f clients/python/tests/fixtures/pgbouncer/compose.yaml up -d --wait
//! cargo test -p horsies-core --features pgbouncer-fixture-tests --lib pgbouncer_contract_tests
//! ```
//!
//! The URLs come from the same variables as Python's
//! `clients/python/tests/pgbouncer_utils.py` (`HORSIES_TEST_DATABASE_URL_DIRECT`,
//! `..._TRANSACTION`, `..._SESSION`, `..._STATEMENT`), with its defaults.
//! Each test creates a database of its own through the direct URL, named
//! like every core test database (`<prefix>rs_<32 hex>`,
//! `src/test_support/database.rs`); every PgBouncer pool routes any database
//! name to the server.

use chrono::Utc;
use uuid::Uuid;

use crate::broker::{BrokerError, PostgresBroker, SendKind};
use crate::core::config::{EnqueueRetention, PostgresConfig};
use crate::core::history::identity::uuid7::mint_task_id;
use crate::core::task::send_types::{send_error_from_enqueue, TaskSendErrorCode, TaskSendPayload};
use crate::db::{CheckoutClass, ParameterType, PooledConnection, StatementBound, TypedStatement};
use crate::test_support::new_test_database_name;
use crate::test_support::sql::{self, TestConnection, TestPool, TestPoolOptions};

/// Enqueues of the transaction-pool test: enough statements to meet a
/// prepared-statement collision if the broker used named statements.
const REPEATED_ENQUEUES: usize = 30;

/// Statements on one session connection that must all run on one backend.
const SESSION_STATEMENTS: usize = 8;

/// The advisory lock key of the session test (Python's key).
const SESSION_LOCK_KEY: i64 = 918_273_645;

/// The backend of a core connection.
static BACKEND_PID: TypedStatement =
    TypedStatement::new("test_backend_pid", "SELECT pg_backend_pid()", &[]);

/// A session advisory lock on its key.
static SESSION_LOCK: TypedStatement = TypedStatement::new(
    "test_session_lock",
    "SELECT pg_advisory_lock($1)",
    &[ParameterType::Int8],
);

/// The release of the session advisory lock on its key.
static SESSION_UNLOCK: TypedStatement = TypedStatement::new(
    "test_session_unlock",
    "SELECT pg_advisory_unlock($1)",
    &[ParameterType::Int8],
);

/// The backend pid of the core connection `session`.
async fn backend_pid(session: &mut PooledConnection) -> i32 {
    let rows = session
        .query(
            &BACKEND_PID.bind().finish(),
            StatementBound::ConnectionTimeouts,
        )
        .await
        .expect("read the backend");
    rows.first().expect("one row").get(0)
}

/// The URLs of one test database through each endpoint of the fixture.
struct PgbouncerUrls {
    database_name: String,
    admin: String,
    direct: String,
    transaction: String,
    session: String,
    statement: String,
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
    let authority_end = rest.find(['/', '?']).unwrap_or(rest.len());
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
    async fn create() -> Self {
        let direct = template_url("HORSIES_TEST_DATABASE_URL_DIRECT", 15432);
        let database_name = new_test_database_name();
        let admin = with_database(&direct, "postgres");
        let mut connection = TestConnection::connect(&admin)
            .await
            .expect("connect to the admin database");
        sql::query(&format!("CREATE DATABASE \"{database_name}\""))
            .execute(&mut connection)
            .await
            .expect("create the test database");
        connection.close().await;
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
        let mut connection = TestConnection::connect(&self.admin)
            .await
            .expect("connect to the admin database");
        sql::query(&format!(
            "DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)",
            self.database_name
        ))
        .execute(&mut connection)
        .await
        .expect("drop the test database");
        connection.close().await;
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

async fn direct_pool(url: &str) -> TestPool {
    TestPoolOptions::new()
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
#[cfg_attr(
    not(feature = "pgbouncer-fixture-tests"),
    ignore = "needs the PgBouncer fixture of clients/python/tests/fixtures/pgbouncer and the feature pgbouncer-fixture-tests"
)]
async fn a_transaction_pool_with_a_direct_session_installs_checks_locks_and_enqueues() {
    let urls = PgbouncerUrls::create().await;
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

    // A connection of the broker's core session pool keeps its server
    // session: a session lock and the backend hold across its statements.
    let pools = broker
        .core_pools_for_read()
        .expect("the broker's core pools");
    let mut session = pools
        .pools()
        .session_work()
        .checkout(CheckoutClass::Reserved)
        .await
        .expect("check out a session connection");
    let backend = backend_pid(&mut session).await;
    let _locked = session
        .query(
            &SESSION_LOCK.bind().value(&SESSION_LOCK_KEY).finish(),
            StatementBound::ConnectionTimeouts,
        )
        .await
        .expect("take the session lock");
    for _ in 0..SESSION_STATEMENTS {
        let current = backend_pid(&mut session).await;
        assert_eq!(current, backend, "the session connection changed backend");
    }
    let unlocked = session
        .query(
            &SESSION_UNLOCK.bind().value(&SESSION_LOCK_KEY).finish(),
            StatementBound::ConnectionTimeouts,
        )
        .await
        .expect("release the session lock");
    let released: bool = unlocked.first().expect("the unlock returns one row").get(0);
    assert!(released, "the session lock was not held at the unlock");
    drop(session);
    drop(pools);

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
        sql::query_scalar("SELECT count(*) FROM horsies_tasks WHERE id = ANY($1::uuid[])")
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
#[cfg_attr(
    not(feature = "pgbouncer-fixture-tests"),
    ignore = "needs the PgBouncer fixture of clients/python/tests/fixtures/pgbouncer and the feature pgbouncer-fixture-tests"
)]
async fn a_session_url_on_the_transaction_pool_fails_the_worker_session_check() {
    let urls = PgbouncerUrls::create().await;
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
#[cfg_attr(
    not(feature = "pgbouncer-fixture-tests"),
    ignore = "needs the PgBouncer fixture of clients/python/tests/fixtures/pgbouncer and the feature pgbouncer-fixture-tests"
)]
async fn a_session_pooled_url_installs_enqueues_and_delivers_a_notify() {
    let urls = PgbouncerUrls::create().await;
    let broker = PostgresBroker::connect_with(
        &sized(PostgresConfig::from_database_url(urls.session.clone())),
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
    let stored: i64 = sql::query_scalar("SELECT count(*) FROM horsies_tasks WHERE id = $1")
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
#[cfg_attr(
    not(feature = "pgbouncer-fixture-tests"),
    ignore = "needs the PgBouncer fixture of clients/python/tests/fixtures/pgbouncer and the feature pgbouncer-fixture-tests"
)]
async fn a_statement_pooled_runtime_url_fails_the_send_as_not_retryable() {
    let urls = PgbouncerUrls::create().await;
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
    let stored: i64 = sql::query_scalar("SELECT count(*) FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(&direct)
        .await
        .expect("count the refused row");
    assert_eq!(stored, 0);
    direct.close().await;
    broker.close().await;
    urls.drop().await;
}
