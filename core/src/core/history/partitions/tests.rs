use crate::test_support::history::{
    create_heartbeat_leaf_of, create_history_leaf_of, database_now, register_heartbeat_class,
    register_period_class,
};
use crate::test_support::sql::{self, TestConnection, TestPool, TestPoolOptions, TestTransaction};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{Duration, Timelike, Utc};
use serial_test::serial;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::broker::core_rows::StatementLog;
use crate::broker::pools::core_pool_for_test_url;
use crate::core::history::commands::{
    DetachExpiredHistoryLeaf, DropDetachedHistoryLeaf, InspectHistoryLeaf, LeafBounds, LeafRef,
};
use crate::core::history::ddl::classes::{finite_class_parent_name, FOREVER_CLASS_KEY};
use crate::core::history::ddl::runtime_names::{
    daily_leaf_name, leaf_enqueued_index_name, leaf_id_index_name, render_daily_leaf_ddl,
};
use crate::core::history::errors::HistoryError;
use crate::core::history::heartbeats::partitioning::{
    heartbeat_horizon, hourly_leaf_name, hourly_leaf_ref, probe_index_name,
    sweep_expired_heartbeat_leaves, EnsureHeartbeatCoverage,
};
use crate::core::history::maintenance::coverage::{
    ensure_partition_coverage_in_pool, ensure_periodic_coverage_in_pool,
    ensure_startup_coverage_in_pool, CoverageFailureStage, CoverageOutcome, StartupCoverageOutcome,
};
use crate::core::history::maintenance::pruning::{
    prune_expired_partitions, prune_expired_partitions_within, sweep_counts_toward_bound,
    sweep_expired_history_leaves, LeafSweep, PruneReach,
};
use crate::core::history::names::{
    HEARTBEATS_TABLE, HEARTBEAT_CLASS_KEY, LEAF_CATALOG, LEAF_LOCK_KEY_FUNCTION, RETENTION_CLASSES,
    TASK_HISTORY_FOREVER, TASK_HISTORY_FOREVER_LEAF, TASK_LOOKUP_MANIFEST,
};
use crate::core::history::outcomes::{CatalogConflictKind, LeafCreation, LeafDrop, LeafInspection};
use crate::core::history::periods::RetentionDays;
use crate::core::history::reads::publisher::StagedLoaderPublisher;
use crate::db::{
    CheckoutClass, CorePool, ParameterType, StatementBound, StatementFailure, TypedStatement,
};

use super::catalog::{LeafLowerBound, ORDERING_INDEX_COLUMN};
use super::manager::{
    detach_expired_leaf, drop_detached_leaf_in_pool, inspect_leaf_in_pool, DetachExpiredLeafOutcome,
};

mod lifecycle_ports;

/// A copy of the installed test template (`crate::test_support`) with a
/// test-client pool for the fixtures and the probes of the tests.
struct TestDatabase {
    pool: TestPool,
    database: crate::test_support::TestDatabase,
}

impl TestDatabase {
    async fn create() -> Self {
        Self::create_with_connections(1).await
    }

    async fn create_with_connections(max_connections: u32) -> Self {
        let database = crate::test_support::TestDatabase::create().await;
        let pool = TestPoolOptions::new()
            .max_connections(max_connections)
            .connect(database.url())
            .await
            .expect("connect to P3 test database");
        Self { pool, database }
    }

    fn url(&self) -> &str {
        self.database.url()
    }

    async fn drop(self) {
        self.pool.close().await;
        self.database.drop().await;
    }
}

mod pruning_ports;

mod read_round_trips;

mod retirement_ports;

/// Sets the session statement and lock timeouts of a core connection.
static SET_SESSION_TIMEOUTS: TypedStatement = TypedStatement::new(
    "set_test_session_timeouts",
    "SELECT set_config('statement_timeout', $1, false), set_config('lock_timeout', $2, false)",
    &[ParameterType::Text, ParameterType::Text],
);

/// Reads the session statement and lock timeouts of a core connection.
static READ_SESSION_TIMEOUTS: TypedStatement = TypedStatement::new(
    "read_test_session_timeouts",
    "SELECT current_setting('statement_timeout'), current_setting('lock_timeout')",
    &[],
);

/// Sets the session timeouts of the idle connection of a one-connection
/// core pool (its first connection when none is open yet).
async fn set_core_session_timeouts(pool: &CorePool, statement_timeout: &str, lock_timeout: &str) {
    let mut connection = pool
        .checkout(CheckoutClass::Reserved)
        .await
        .expect("check out the core connection");
    let _rows = connection
        .query(
            &SET_SESSION_TIMEOUTS
                .bind()
                .value(&statement_timeout)
                .value(&lock_timeout)
                .finish(),
            StatementBound::ConnectionTimeouts,
        )
        .await
        .expect("set the session timeouts");
}

/// The session statement and lock timeouts of the idle connection of a
/// one-connection core pool.
async fn core_session_timeouts(pool: &CorePool) -> (String, String) {
    let mut connection = pool
        .checkout(CheckoutClass::Reserved)
        .await
        .expect("check out the core connection");
    let rows = connection
        .query(
            &READ_SESSION_TIMEOUTS.bind().finish(),
            StatementBound::ConnectionTimeouts,
        )
        .await
        .expect("read the session timeouts");
    let row = rows.first().expect("current_setting returns one row");
    (row.get::<_, String>(0), row.get::<_, String>(1))
}

#[derive(Clone)]
struct StatementPause {
    pattern: Arc<str>,
    consumed: Arc<AtomicBool>,
    pending: Arc<AtomicBool>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl StatementPause {
    fn new(pattern: impl Into<Arc<str>>) -> Self {
        Self {
            pattern: pattern.into(),
            consumed: Arc::new(AtomicBool::new(false)),
            pending: Arc::new(AtomicBool::new(false)),
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        }
    }

    fn inspect(&self, query: &str) {
        if query.contains(self.pattern.as_ref()) && !self.consumed.swap(true, Ordering::SeqCst) {
            self.pending.store(true, Ordering::SeqCst);
        }
    }

    async fn pause_after_ready(&self) {
        if self.pending.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
    }

    async fn wait_until_entered(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(5), self.entered.notified())
            .await
            .expect("matching SQL statement did not reach its response barrier");
    }

    fn resume(&self) {
        self.release.notify_one();
    }
}

#[derive(Default)]
struct FrontendStatementParser {
    startup: bool,
    buffered: Vec<u8>,
}

impl FrontendStatementParser {
    fn new() -> Self {
        Self {
            startup: true,
            buffered: Vec::new(),
        }
    }

    fn push(
        &mut self,
        bytes: &[u8],
        statements: &AtomicUsize,
        pending_responses: &AtomicUsize,
        sql: &Mutex<Vec<String>>,
        pause: Option<&StatementPause>,
    ) {
        self.buffered.extend_from_slice(bytes);
        loop {
            if self.startup {
                if self.buffered.len() < 4 {
                    return;
                }
                let length = u32::from_be_bytes(self.buffered[..4].try_into().unwrap()) as usize;
                if length < 8 || self.buffered.len() < length {
                    return;
                }
                let code = u32::from_be_bytes(self.buffered[4..8].try_into().unwrap());
                self.buffered.drain(..length);
                if code != 80_877_103 && code != 80_877_104 {
                    self.startup = false;
                }
                continue;
            }

            if self.buffered.len() < 5 {
                return;
            }
            let tag = self.buffered[0];
            let length = u32::from_be_bytes(self.buffered[1..5].try_into().unwrap()) as usize;
            let message_length = length + 1;
            if length < 4 || self.buffered.len() < message_length {
                return;
            }
            let body = &self.buffered[5..message_length];
            match tag {
                b'Q' => {
                    statements.fetch_add(1, Ordering::SeqCst);
                    pending_responses.fetch_add(1, Ordering::SeqCst);
                    if let Some(query) = body.strip_suffix(&[0]) {
                        let query = String::from_utf8_lossy(query);
                        if let Some(pause) = pause {
                            pause.inspect(&query);
                        }
                        sql.lock()
                            .expect("statement SQL lock")
                            .push(query.into_owned());
                    }
                }
                b'E' => {
                    statements.fetch_add(1, Ordering::SeqCst);
                    pending_responses.fetch_add(1, Ordering::SeqCst);
                }
                b'P' => {
                    if let Some(name_end) = body.iter().position(|byte| *byte == 0) {
                        let query = &body[name_end + 1..];
                        if let Some(query_end) = query.iter().position(|byte| *byte == 0) {
                            let query = String::from_utf8_lossy(&query[..query_end]);
                            if let Some(pause) = pause {
                                pause.inspect(&query);
                            }
                            sql.lock()
                                .expect("statement SQL lock")
                                .push(query.into_owned());
                        }
                    }
                }
                _ => {}
            }
            self.buffered.drain(..message_length);
        }
    }
}

#[derive(Default)]
struct BackendReadyParser {
    buffered: Vec<u8>,
}

impl BackendReadyParser {
    fn push(&mut self, bytes: &[u8]) -> bool {
        self.buffered.extend_from_slice(bytes);
        let mut ready = false;
        loop {
            if self.buffered.len() < 5 {
                return ready;
            }
            let tag = self.buffered[0];
            let length = u32::from_be_bytes(self.buffered[1..5].try_into().unwrap()) as usize;
            let message_length = length + 1;
            if length < 4 || self.buffered.len() < message_length {
                return ready;
            }
            ready |= tag == b'Z';
            self.buffered.drain(..message_length);
        }
    }
}

struct SqlStatementProxy {
    port: u16,
    statements: Arc<AtomicUsize>,
    pending_responses: Arc<AtomicUsize>,
    sql: Arc<Mutex<Vec<String>>>,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl SqlStatementProxy {
    async fn start(backend_url: &str, delay_ms: usize) -> Self {
        Self::start_with_pause(backend_url, delay_ms, None).await
    }

    async fn start_with_pause(
        backend_url: &str,
        delay_ms: usize,
        pause: Option<StatementPause>,
    ) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind SQL statement proxy");
        let port = listener.local_addr().expect("proxy address").port();
        let (backend_host, backend_port) = sql::endpoint_of(backend_url);
        let statements = Arc::new(AtomicUsize::new(0));
        let pending_responses = Arc::new(AtomicUsize::new(0));
        let sql = Arc::new(Mutex::new(Vec::new()));
        let cancel = CancellationToken::new();
        let accept_cancel = cancel.clone();
        let accept_statements = Arc::clone(&statements);
        let accept_pending = Arc::clone(&pending_responses);
        let accept_sql = Arc::clone(&sql);
        let task = tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    _ = accept_cancel.cancelled() => return,
                    accepted = listener.accept() => accepted,
                };
                let (client, _) = accepted.expect("accept proxied PostgreSQL connection");
                let connection_statements = Arc::clone(&accept_statements);
                let connection_pending = Arc::clone(&accept_pending);
                let connection_sql = Arc::clone(&accept_sql);
                let connection_pause = pause.clone();
                let host = backend_host.clone();
                tokio::spawn(async move {
                    let server = TcpStream::connect((host.as_str(), backend_port))
                        .await
                        .expect("connect SQL statement proxy to PostgreSQL");
                    let (mut client_read, mut client_write) = client.into_split();
                    let (mut server_read, mut server_write) = server.into_split();
                    let client_to_server = async {
                        let mut parser = FrontendStatementParser::new();
                        let mut buffer = vec![0_u8; 16 * 1024];
                        loop {
                            let read = client_read
                                .read(&mut buffer)
                                .await
                                .expect("read proxied PostgreSQL client");
                            if read == 0 {
                                return;
                            }
                            parser.push(
                                &buffer[..read],
                                &connection_statements,
                                &connection_pending,
                                &connection_sql,
                                connection_pause.as_ref(),
                            );
                            server_write
                                .write_all(&buffer[..read])
                                .await
                                .expect("write proxied PostgreSQL server");
                        }
                    };
                    let server_to_client = async {
                        let mut ready_parser = BackendReadyParser::default();
                        let mut buffer = vec![0_u8; 16 * 1024];
                        loop {
                            let read = server_read
                                .read(&mut buffer)
                                .await
                                .expect("read proxied PostgreSQL server");
                            if read == 0 {
                                return;
                            }
                            let delayed = connection_pending.swap(0, Ordering::SeqCst);
                            if delayed > 0 && delay_ms > 0 {
                                tokio::time::sleep(std::time::Duration::from_millis(
                                    (delay_ms * delayed) as u64,
                                ))
                                .await;
                            }
                            if ready_parser.push(&buffer[..read]) {
                                if let Some(pause) = connection_pause.as_ref() {
                                    pause.pause_after_ready().await;
                                }
                            }
                            client_write
                                .write_all(&buffer[..read])
                                .await
                                .expect("write proxied PostgreSQL client");
                        }
                    };
                    tokio::select! {
                        _ = client_to_server => {}
                        _ = server_to_client => {}
                    }
                });
            }
        });
        Self {
            port,
            statements,
            pending_responses,
            sql,
            cancel,
            task,
        }
    }

    fn reset(&self) {
        self.statements.store(0, Ordering::SeqCst);
        self.pending_responses.store(0, Ordering::SeqCst);
        self.sql.lock().expect("statement SQL lock").clear();
    }

    fn statement_count(&self) -> usize {
        self.statements.load(Ordering::SeqCst)
    }

    fn sql(&self) -> Vec<String> {
        self.sql.lock().expect("statement SQL lock").clone()
    }

    async fn stop(self) {
        self.cancel.cancel();
        self.task.await.expect("stop SQL statement proxy");
    }
}

async fn proxied_pool(
    database: &TestDatabase,
    delay_ms: usize,
    max_connections: u32,
) -> (SqlStatementProxy, TestPool) {
    let proxy = SqlStatementProxy::start(database.url(), delay_ms).await;
    let pool = TestPoolOptions::new()
        .max_connections(max_connections)
        .connect(&sql::with_endpoint(database.url(), "127.0.0.1", proxy.port))
        .await
        .expect("connect through SQL statement proxy");
    sql::query("SELECT 1")
        .execute(&pool)
        .await
        .expect("warm proxied PostgreSQL connection");
    proxy.reset();
    (proxy, pool)
}

async fn pausing_proxied_pool(
    database: &TestDatabase,
    pattern: &str,
) -> (SqlStatementProxy, TestPool, StatementPause) {
    let pause = StatementPause::new(Arc::<str>::from(pattern));
    let proxy = SqlStatementProxy::start_with_pause(database.url(), 0, Some(pause.clone())).await;
    let pool = TestPoolOptions::new()
        .max_connections(1)
        .connect(&with_tls_disabled(&sql::with_endpoint(
            database.url(),
            "127.0.0.1",
            proxy.port,
        )))
        .await
        .expect("connect through pausing SQL statement proxy");
    (proxy, pool, pause)
}

/// `url` with `sslmode=disable` in place of any `sslmode` pair it holds.
/// The pause reads the server's bytes as tagged messages from the first
/// byte (`BackendReadyParser`); the one-byte answer to a TLS request is not
/// such a message, so a connection through the pausing proxy, and every
/// pool built from its URL, must not ask for TLS. Pure.
fn with_tls_disabled(url: &str) -> String {
    let (base, query) = url.split_once('?').unwrap_or((url, ""));
    let mut pairs: Vec<&str> = query
        .split('&')
        .filter(|pair| !pair.is_empty() && !pair.starts_with("sslmode="))
        .collect();
    pairs.push("sslmode=disable");
    format!("{base}?{}", pairs.join("&"))
}

/// Sets the session time zone of a proxied core connection.
static SET_SESSION_TIMEZONE: TypedStatement = TypedStatement::new(
    "set_session_timezone",
    "SELECT set_config('timezone', $1, false)",
    &[ParameterType::Text],
);

/// Reads the session time zone of a proxied core connection.
static READ_SESSION_TIMEZONE: TypedStatement = TypedStatement::new(
    "read_session_timezone",
    "SELECT current_setting('timezone')",
    &[],
);

/// A core session pool of `max_connections` through a statement proxy,
/// warm: its statement-pooling check ran on its first connection, which is
/// idle again, and the proxy counts from zero. With `session_timezone` the
/// warm connection's session time zone is set first.
async fn proxied_core_pool(
    database: &TestDatabase,
    delay_ms: usize,
    max_connections: u32,
    session_timezone: Option<&str>,
) -> (SqlStatementProxy, CorePool) {
    let (proxy, pool) = proxied_pool(database, delay_ms, max_connections).await;
    let core = crate::broker::pools::session_core_pool_for_test_url(pool.url(), max_connections);
    pool.close().await;
    {
        let mut connection = core
            .checkout(CheckoutClass::Reserved)
            .await
            .expect("warm the proxied core pool");
        if let Some(timezone) = session_timezone {
            let _rows = connection
                .query(
                    &SET_SESSION_TIMEZONE.bind().value(&timezone).finish(),
                    StatementBound::ConnectionTimeouts,
                )
                .await
                .expect("set the proxied session timezone");
        }
    }
    proxy.reset();
    (proxy, core)
}

/// The session time zone of the idle connection of a one-connection core
/// pool.
async fn core_session_timezone(pool: &CorePool) -> String {
    let mut connection = pool
        .checkout(CheckoutClass::Reserved)
        .await
        .expect("check out the proxied core connection");
    let rows = connection
        .query(
            &READ_SESSION_TIMEZONE.bind().finish(),
            StatementBound::ConnectionTimeouts,
        )
        .await
        .expect("read the proxied session timezone");
    rows.first()
        .expect("current_setting returns one row")
        .get::<_, String>(0)
}

async fn register_class(pool: &TestPool, class_key: &str, days: i64) -> String {
    let period = RetentionDays::parse_period(Duration::days(days)).expect("retention period");
    assert_eq!(
        period.class_key(),
        class_key,
        "a class key is derived from its period"
    );
    let outcome = register_period_class(pool, period).await;
    assert!(
        matches!(outcome.as_str(), "REGISTERED" | "VERIFIED"),
        "{outcome}"
    );
    finite_class_parent_name(class_key).expect("finite parent name")
}

/// The name of the first leaf that `predicate` selects in the leaf
/// catalog, in `(lower_anchor, leaf_name)` order.
async fn catalog_leaf_where(pool: &TestPool, predicate: &str, class_key: &str) -> String {
    sql::query_scalar(&format!(
        "SELECT leaf_name FROM {LEAF_CATALOG} WHERE {predicate}
         ORDER BY lower_anchor, leaf_name LIMIT 1"
    ))
    .bind(class_key)
    .fetch_one(pool)
    .await
    .expect("select catalog leaf")
}

/// Whether `leaf_name` carries a valid, ready, live, plain ascending btree
/// index on `enqueued_at` alone, the ordering index of a history leaf. An
/// absent relation has none.
async fn ordering_index_exists(pool: &TestPool, leaf_name: &str) -> bool {
    sql::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM pg_index AS i
             JOIN pg_class AS ic ON ic.oid = i.indexrelid
             JOIN pg_am AS am ON am.oid = ic.relam
             WHERE i.indrelid = to_regclass($1)
               AND am.amname = 'btree'
               AND i.indisvalid
               AND i.indisready
               AND i.indislive
               AND NOT i.indisunique
               AND NOT i.indisprimary
               AND NOT i.indisexclusion
               AND i.indpred IS NULL
               AND i.indexprs IS NULL
               AND i.indnkeyatts = 1
               AND i.indnatts = 1
               AND i.indkey[0] = (
                   SELECT a.attnum FROM pg_attribute AS a
                   WHERE a.attrelid = to_regclass($1) AND a.attname = $2
               )
               AND i.indcollation[0] = (
                   SELECT a.attcollation FROM pg_attribute AS a
                   WHERE a.attrelid = to_regclass($1) AND a.attname = $2
               )
               AND pg_get_indexdef(i.indexrelid, 1, false) = $2
               AND pg_get_indexdef(i.indexrelid)
                   LIKE '% USING btree (enqueued_at)'
               AND i.indoption[0] = 0
         )",
    )
    .bind(leaf_name)
    .bind(ORDERING_INDEX_COLUMN)
    .fetch_one(pool)
    .await
    .expect("probe the ordering index")
}

/// The definition of the index `index_name` and whether it is valid.
async fn index_state(pool: &TestPool, index_name: &str) -> (String, bool) {
    sql::query_as(
        "SELECT pg_get_indexdef(i.indexrelid), i.indisvalid
         FROM pg_index AS i WHERE i.indexrelid = to_regclass($1)",
    )
    .bind(index_name)
    .fetch_one(pool)
    .await
    .expect("read the index state")
}

fn leaf_ref(parent: &str, class_key: &str, lower: chrono::DateTime<Utc>) -> LeafRef {
    LeafRef::new(
        daily_leaf_name(parent, lower).expect("daily leaf name"),
        class_key,
        LeafBounds::new(lower, lower + Duration::days(1)).expect("daily bounds"),
    )
    .expect("daily leaf ref")
}

/// An open transaction that holds the catalog row of `leaf_name` with
/// `FOR UPDATE`. While it is held, `horsies_record_leaf_detached` of the leaf
/// waits for the row and fails at the 2 s DDL lock timeout of the detach
/// session (`55P03`): the leaf's sweep fails after its detach statement ran.
async fn hold_catalog_row(pool: &TestPool, leaf_name: &str) -> TestTransaction<'static> {
    let mut holder = pool.begin().await.expect("begin catalog row holder");
    sql::query(&format!(
        "SELECT 1 FROM {LEAF_CATALOG} WHERE leaf_name = $1 FOR UPDATE"
    ))
    .bind(leaf_name)
    .execute(&mut *holder)
    .await
    .expect("hold the catalog row");
    holder
}

/// Creates the daily leaf of `leaf` with the statement that the coverage
/// pass runs (`horsies_create_history_leaf`, no publication), in autocommit.
async fn create_leaf(pool: &TestPool, leaf: &LeafRef) -> LeafCreation {
    create_history_leaf_of(pool, leaf)
        .await
        .expect("create leaf")
}

#[test]
fn heartbeat_derivation_and_command_contracts_match_the_authority() {
    assert_eq!(
        heartbeat_horizon(Duration::minutes(10), Duration::minutes(45), 4),
        Ok(Duration::hours(3))
    );
    assert_eq!(
        heartbeat_horizon(Duration::seconds(30), Duration::seconds(45), 2),
        Ok(Duration::hours(1))
    );
    assert!(heartbeat_horizon(Duration::zero(), Duration::minutes(1), 2).is_err());
    assert!(heartbeat_horizon(Duration::minutes(1), Duration::minutes(1), 0).is_err());
    assert!(EnsureHeartbeatCoverage::new(1).is_err());
    assert!(EnsureHeartbeatCoverage::new(2).is_ok());
    let hour = chrono::DateTime::parse_from_rfc3339("2026-08-07T13:00:00Z")
        .expect("fixed heartbeat hour")
        .with_timezone(&Utc);
    assert_eq!(
        hourly_leaf_name(hour).expect("hourly leaf name"),
        "horsies_heartbeats_2026_08_07_13"
    );
    assert_eq!(
        probe_index_name("horsies_heartbeats_2026_08_07_13").expect("heartbeat probe index name"),
        "horsies_heartbeats_2026_08_07_13_probe_idx"
    );
    assert!(probe_index_name(&format!("horsies_heartbeats_{}", "x".repeat(50))).is_err());
}

/// A second detach of a leaf reports it detached, and both detaches count
/// toward the acted-leaf bound of a pass, as Python's
/// `counts_toward_prune_bound` counts every `LeafDetached`. Both report the
/// Python outcome name `LeafDetached`.
#[tokio::test]
async fn a_second_detach_reports_the_leaf_detached_by_the_first() {
    let database = TestDatabase::create().await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut clock = database.pool.acquire().await.expect("acquire clock");
    let lower = utc_day_start(database_now(&mut clock).await - Duration::days(5));
    drop(clock);
    let leaf = leaf_ref(&parent, class_key, lower);
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));
    let command = DetachExpiredHistoryLeaf::new(leaf.clone(), Some(5_000)).expect("detach command");
    let first = detach_expired_leaf(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
        &command,
    )
    .await
    .expect("first detach");
    assert_eq!(
        first,
        DetachExpiredLeafOutcome::Detached {
            leaf_name: leaf.leaf_name().to_owned(),
        }
    );
    let second = detach_expired_leaf(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
        &command,
    )
    .await
    .expect("second detach");
    assert!(matches!(
        second,
        DetachExpiredLeafOutcome::Inspection(LeafInspection::Detached { .. })
    ));
    assert!(sweep_counts_toward_bound(&second, None));
    assert!(sweep_counts_toward_bound(&first, None));
    assert_eq!(first.outcome_name(), "LeafDetached");
    assert_eq!(second.outcome_name(), "LeafDetached");
    database.drop().await;
}

#[tokio::test]
#[serial(timing)]
async fn detach_returns_busy_without_waiting_for_a_leaf_session_lock() {
    let database = TestDatabase::create_with_connections(3).await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut clock = database.pool.acquire().await.expect("acquire clock");
    let now = database_now(&mut clock).await;
    drop(clock);
    let lower = (now - Duration::days(5))
        .with_hour(0)
        .and_then(|value| value.with_minute(0))
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate expired day");
    let leaf = leaf_ref(&parent, class_key, lower);
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));

    let lock_sql = format!("SELECT pg_advisory_lock({LEAF_LOCK_KEY_FUNCTION}($1, $2))");
    let unlock_sql = format!("SELECT pg_advisory_unlock({LEAF_LOCK_KEY_FUNCTION}($1, $2))");
    let mut holder = database.pool.acquire().await.expect("acquire lock holder");
    sql::query(&lock_sql)
        .bind(class_key)
        .bind(lower)
        .execute(&mut *holder)
        .await
        .expect("hold detach leaf lock");
    let command = DetachExpiredHistoryLeaf::new(leaf.clone(), Some(5_000)).expect("detach command");
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        detach_expired_leaf(
            &core_pool_for_test_url(database.pool.url()),
            StatementLog::Off,
            &command,
        ),
    )
    .await
    .expect("busy detach must not wait")
    .expect("busy detach outcome");
    assert_eq!(
        outcome,
        DetachExpiredLeafOutcome::Busy {
            leaf_name: leaf.leaf_name().to_owned(),
        }
    );
    let released: bool = sql::query_scalar(&unlock_sql)
        .bind(class_key)
        .bind(lower)
        .fetch_one(&mut *holder)
        .await
        .expect("unlock detach leaf");
    assert!(released);
    drop(holder);

    let mut parent_holder = database.pool.begin().await.expect("begin parent lock");
    sql::query(&format!(
        "LOCK TABLE {parent} IN SHARE UPDATE EXCLUSIVE MODE"
    ))
    .execute(&mut *parent_holder)
    .await
    .expect("hold detach parent lock");
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        detach_expired_leaf(
            &core_pool_for_test_url(database.pool.url()),
            StatementLog::Off,
            &command,
        ),
    )
    .await
    .expect("parent-busy detach must be bounded")
    .expect("parent-busy detach outcome");
    assert_eq!(
        outcome,
        DetachExpiredLeafOutcome::Busy {
            leaf_name: leaf.leaf_name().to_owned(),
        }
    );
    parent_holder
        .rollback()
        .await
        .expect("release detach parent lock");
    database.drop().await;
}

/// The session lock of the reader publication key that
/// `horsies_publish_history_readers` takes as a transaction lock.
const PUBLICATION_LOCK_SQL: &str =
    "SELECT pg_advisory_lock(hashtextextended('horsies:reader-publication:v1', 1601))";
const PUBLICATION_UNLOCK_SQL: &str =
    "SELECT pg_advisory_unlock(hashtextextended('horsies:reader-publication:v1', 1601))";

/// Waits until a session of the test database waits for an advisory lock;
/// fails the test after 1.5 s, well inside the 2 s lock timeout of a detach
/// session.
async fn wait_for_an_advisory_lock_wait(pool: &TestPool) {
    for _ in 0..150 {
        let waiting: bool = sql::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND wait_event_type = 'Lock' AND wait_event = 'advisory'
             )",
        )
        .fetch_one(pool)
        .await
        .expect("read the lock waits");
        if waiting {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("no session waited for an advisory lock within 1.5 s");
}

#[tokio::test]
async fn cancelled_detach_closes_its_connection_and_releases_the_session_lock() {
    let database = TestDatabase::create_with_connections(3).await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut clock = database.pool.acquire().await.expect("acquire clock");
    let now = database_now(&mut clock).await;
    drop(clock);
    let lower = (now - Duration::days(5))
        .with_hour(0)
        .and_then(|value| value.with_minute(0))
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate expired day");
    let leaf = leaf_ref(&parent, class_key, lower);
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));
    // The published manifest names the leaf, so the detach record publishes
    // the readers while the session holds the leaf lock. Another session
    // holds the publication lock, so the record waits there.
    let mut publication = database.pool.acquire().await.expect("acquire publication");
    StagedLoaderPublisher
        .republish(&mut publication)
        .await
        .expect("publish the readers over the leaf");
    sql::query(PUBLICATION_LOCK_SQL)
        .execute(&mut *publication)
        .await
        .expect("hold the reader publication lock");
    let command = DetachExpiredHistoryLeaf::new(leaf.clone(), Some(5_000)).expect("detach command");
    let core = core_pool_for_test_url(database.pool.url());
    let detach =
        tokio::spawn(async move { detach_expired_leaf(&core, StatementLog::Off, &command).await });
    wait_for_an_advisory_lock_wait(&database.pool).await;
    detach.abort();
    assert!(detach
        .await
        .expect_err("detach must be cancelled")
        .is_cancelled());
    // The waiting backend gets the publication lock, then finds its client
    // gone and ends its session.
    sql::query(PUBLICATION_UNLOCK_SQL)
        .execute(&mut *publication)
        .await
        .expect("release the reader publication lock");
    drop(publication);

    let try_sql = format!("SELECT pg_try_advisory_lock({LEAF_LOCK_KEY_FUNCTION}($1, $2))");
    let unlock_sql = format!("SELECT pg_advisory_unlock({LEAF_LOCK_KEY_FUNCTION}($1, $2))");
    let mut probe = database.pool.acquire().await.expect("acquire lock probe");
    let mut acquired = false;
    for _ in 0..200 {
        acquired = sql::query_scalar(&try_sql)
            .bind(class_key)
            .bind(lower)
            .fetch_one(&mut *probe)
            .await
            .expect("try cancelled detach lock");
        if acquired {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(acquired, "cancelled detach must release its session lock");
    let released: bool = sql::query_scalar(&unlock_sql)
        .bind(class_key)
        .bind(lower)
        .fetch_one(&mut *probe)
        .await
        .expect("release lock probe");
    assert!(released);
    drop(probe);
    database.drop().await;
}

#[tokio::test]
async fn pool_coverage_releases_each_leaf_transaction_before_returning() {
    let database = TestDatabase::create_with_connections(4).await;
    let outcome = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("pool coverage");
    assert!(matches!(outcome, CoverageOutcome::Ensured(_)));
    let advisory_locks: i64 = sql::query_scalar(
        "SELECT count(*)
         FROM pg_locks
         WHERE locktype = 'advisory' AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
    )
    .fetch_one(&database.pool)
    .await
    .expect("count advisory locks");
    assert_eq!(advisory_locks, 0);
    database.drop().await;
}

#[tokio::test]
async fn healthy_pool_coverage_has_a_fixed_statement_budget() {
    let database = TestDatabase::create_with_connections(4).await;
    let declared: Vec<RetentionDays> = (0..50)
        .map(|index| {
            RetentionDays::parse_period(Duration::days(7 + i64::from(index)))
                .expect("declared period")
        })
        .collect();

    let first = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        8,
        8,
        &declared[..1],
    )
    .await
    .expect("create initial coverage");
    assert!(matches!(first, CoverageOutcome::Ensured(_)), "{first:?}");

    for (history_horizon, heartbeat_horizon) in [(2, 2), (8, 8)] {
        let setup = ensure_partition_coverage_in_pool(
            &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
            crate::broker::core_rows::StatementLog::Off,
            history_horizon,
            heartbeat_horizon,
            &declared[..1],
        )
        .await
        .expect("set healthy horizon coverage");
        assert!(matches!(setup, CoverageOutcome::Ensured(_)), "{setup:?}");
        let (proxy, pool) = proxied_core_pool(&database, 0, 1, Some("America/Los_Angeles")).await;
        let outcome = ensure_partition_coverage_in_pool(
            &pool,
            crate::broker::core_rows::StatementLog::Off,
            history_horizon,
            heartbeat_horizon,
            &declared[..1],
        )
        .await
        .expect("healthy horizon coverage");
        assert!(
            matches!(outcome, CoverageOutcome::Ensured(_)),
            "{outcome:?}"
        );
        assert_eq!(proxy.statement_count(), 1);
        assert!(!proxy.sql().iter().any(|statement| {
            statement.starts_with("BEGIN") || statement.contains("pg_try_advisory_xact_lock")
        }));
        assert_eq!(core_session_timezone(&pool).await, "America/Los_Angeles");
        pool.close();
        proxy.stop().await;
    }

    for class_count in [10, 50] {
        let setup = ensure_partition_coverage_in_pool(
            &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
            crate::broker::core_rows::StatementLog::Off,
            2,
            2,
            &declared[..class_count],
        )
        .await
        .expect("extend class coverage");
        assert!(matches!(setup, CoverageOutcome::Ensured(_)), "{setup:?}");

        let (proxy, pool) = proxied_core_pool(&database, 0, 1, None).await;
        let outcome = ensure_partition_coverage_in_pool(
            &pool,
            crate::broker::core_rows::StatementLog::Off,
            2,
            2,
            &declared[..class_count],
        )
        .await
        .expect("healthy class coverage");
        assert!(
            matches!(outcome, CoverageOutcome::Ensured(_)),
            "{outcome:?}"
        );
        assert_eq!(proxy.statement_count(), 1);
        pool.close();
        proxy.stop().await;
    }

    let delay_ms = 20_usize;
    let (proxy, pool) = proxied_core_pool(&database, delay_ms, 1, None).await;
    let started = tokio::time::Instant::now();
    let outcome = ensure_partition_coverage_in_pool(
        &pool,
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &declared,
    )
    .await
    .expect("healthy high-RTT coverage");
    let elapsed = started.elapsed();
    assert!(
        matches!(outcome, CoverageOutcome::Ensured(_)),
        "{outcome:?}"
    );
    assert_eq!(proxy.statement_count(), 1);
    assert!(elapsed >= std::time::Duration::from_millis(delay_ms as u64));
    // The exact statement count bounds RTT cost. CI load cannot give a stable
    // wall-clock upper bound.
    pool.close();
    proxy.stop().await;
    database.drop().await;
}

#[tokio::test]
async fn failed_post_commit_publication_is_retried_by_the_next_healthy_pass() {
    let database = TestDatabase::create_with_connections(4).await;
    let core_pool = crate::broker::pools::core_pool_for_test_url(database.pool.url());
    let setup = ensure_partition_coverage_in_pool(
        &core_pool,
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("create and publish initial coverage");
    assert!(matches!(setup, CoverageOutcome::Ensured(_)));
    // A created leaf is published by its own creation, so the readers go
    // stale through the manifest (Python `test_failed_final_publication_is_retried`).
    sql::query(&format!(
        "UPDATE {TASK_LOOKUP_MANIFEST} SET probe_position = probe_position + 100"
    ))
    .execute(&database.pool)
    .await
    .expect("move every manifest position");
    // The manifest refuses the positions that a publication writes, so the
    // final publication of the next pass fails in the database.
    sql::query(&format!(
        "ALTER TABLE {TASK_LOOKUP_MANIFEST} ADD CONSTRAINT test_reject_publication \
         CHECK (probe_position >= 100) NOT VALID"
    ))
    .execute(&database.pool)
    .await
    .expect("make the next publication fail");

    let error = ensure_partition_coverage_in_pool(
        &core_pool,
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect_err("the final publication fails");
    assert!(
        matches!(error, HistoryError::Statement(_)),
        "a refused publication is a statement failure: {error:?}"
    );
    let mut connection = database.pool.acquire().await.expect("acquire publisher");
    assert!(StagedLoaderPublisher
        .needs_republication(&mut connection)
        .await
        .expect("freshness check after the failed publication"));
    drop(connection);

    sql::query(&format!(
        "ALTER TABLE {TASK_LOOKUP_MANIFEST} DROP CONSTRAINT test_reject_publication"
    ))
    .execute(&database.pool)
    .await
    .expect("let the next publication pass");
    let retry = ensure_partition_coverage_in_pool(
        &core_pool,
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("retry publication on the next pass");
    let CoverageOutcome::Ensured(retry) = retry else {
        panic!("the next pass must publish the stale readers");
    };
    assert!(retry.republished);
    let mut connection = database.pool.acquire().await.expect("acquire publisher");
    assert!(!StagedLoaderPublisher
        .needs_republication(&mut connection)
        .await
        .expect("freshness check after the retry"));
    drop(connection);
    database.drop().await;
}

#[tokio::test]
async fn current_heartbeat_with_a_wrong_physical_range_refuses_startup() {
    let database = TestDatabase::create_with_connections(4).await;
    let setup = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("create heartbeat coverage");
    assert!(matches!(setup, CoverageOutcome::Ensured(_)));

    let (leaf_name, index_name, lower_anchor, upper_anchor): (
        String,
        String,
        chrono::DateTime<Utc>,
        chrono::DateTime<Utc>,
    ) = sql::query_as(&format!(
        "SELECT leaf_name, leaf_name || '_probe_idx', lower_anchor, upper_anchor
         FROM {LEAF_CATALOG}
         WHERE class_key = $1
           AND lower_anchor <= statement_timestamp()
           AND upper_anchor > statement_timestamp()
           AND detached_at IS NULL
           AND dropped_at IS NULL"
    ))
    .bind(HEARTBEAT_CLASS_KEY)
    .fetch_one(&database.pool)
    .await
    .expect("read current heartbeat leaf");
    sql::query(&format!(
        "ALTER TABLE {HEARTBEATS_TABLE} DETACH PARTITION {leaf_name}"
    ))
    .execute(&database.pool)
    .await
    .expect("detach current heartbeat leaf");
    sql::query(&format!("DROP TABLE {leaf_name}"))
        .execute(&database.pool)
        .await
        .expect("drop current heartbeat leaf");
    let wrong_bounds = LeafBounds::new(
        lower_anchor - Duration::days(100),
        upper_anchor - Duration::days(100),
    )
    .expect("valid wrong heartbeat bounds");
    let wrong_leaf = LeafRef::new(&leaf_name, HEARTBEAT_CLASS_KEY, wrong_bounds)
        .expect("wrong-range heartbeat leaf");
    sql::query(
        &render_daily_leaf_ddl(HEARTBEATS_TABLE, &wrong_leaf)
            .expect("render wrong-range heartbeat leaf"),
    )
    .execute(&database.pool)
    .await
    .expect("create wrong-range heartbeat leaf");
    sql::query(&format!(
        "CREATE INDEX {index_name} ON {leaf_name} (task_id, role, sent_at DESC)"
    ))
    .execute(&database.pool)
    .await
    .expect("create conformant heartbeat index on wrong range");

    let startup = ensure_startup_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("validate startup with a wrong current heartbeat range");
    let StartupCoverageOutcome::Refused(CoverageOutcome::Failed(failure)) = startup else {
        panic!("a wrong current-heartbeat range must refuse startup");
    };
    assert!(!failure.heartbeat_covered_now);
    assert!(
        failure.refusal.contains("PHYSICAL_NONCONFORMANT"),
        "{failure:?}"
    );
    database.drop().await;
}

#[tokio::test]
async fn coverage_repairs_same_name_wrong_shape_and_invalid_indexes() {
    let database = TestDatabase::create_with_connections(4).await;
    let setup = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("create index-repair coverage");
    assert!(matches!(setup, CoverageOutcome::Ensured(_)));

    let heartbeat = catalog_leaf_where(
        &database.pool,
        "class_key = $1
           AND lower_anchor <= statement_timestamp()
           AND upper_anchor > statement_timestamp()",
        HEARTBEAT_CLASS_KEY,
    )
    .await;
    let heartbeat_index = probe_index_name(&heartbeat).expect("heartbeat index name");
    sql::query(&format!("DROP INDEX {heartbeat_index}"))
        .execute(&database.pool)
        .await
        .expect("drop heartbeat index");
    sql::query(&format!(
        "CREATE INDEX {} ON {} (task_id, role, sent_at ASC)",
        heartbeat_index, heartbeat
    ))
    .execute(&database.pool)
    .await
    .expect("create same-name heartbeat index with wrong sort direction");

    let history = catalog_leaf_where(
        &database.pool,
        "class_key NOT IN ($1, 'forever')
           AND detached_at IS NULL
           AND dropped_at IS NULL",
        HEARTBEAT_CLASS_KEY,
    )
    .await;
    // The static forever leaf is install-owned and never repaired, so only
    // a daily history leaf's index is damaged here.
    let history_index = leaf_id_index_name(&history);
    sql::query("UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass($1)")
        .bind(&history_index)
        .execute(&database.pool)
        .await
        .expect("mark history index invalid");

    let outcome = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("repair malformed indexes");
    assert!(matches!(outcome, CoverageOutcome::Ensured(_)));
    // The repaired indexes are the canonical ones, valid.
    let (heartbeat_definition, heartbeat_valid) =
        index_state(&database.pool, &heartbeat_index).await;
    assert!(
        heartbeat_definition.ends_with("USING btree (task_id, role, sent_at DESC)"),
        "{heartbeat_definition}"
    );
    assert!(heartbeat_valid);
    let (history_definition, history_valid) = index_state(&database.pool, &history_index).await;
    assert!(
        history_definition.ends_with("USING btree (task_id)"),
        "{history_definition}"
    );
    assert!(history_valid);
    database.drop().await;
}

#[tokio::test]
async fn coverage_repairs_a_nondefault_heartbeat_operator_class() {
    let database = TestDatabase::create_with_connections(4).await;
    let setup = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("create operator-class repair coverage");
    assert!(matches!(setup, CoverageOutcome::Ensured(_)));
    let heartbeat = catalog_leaf_where(
        &database.pool,
        "class_key = $1
           AND lower_anchor <= statement_timestamp()
           AND upper_anchor > statement_timestamp()",
        HEARTBEAT_CLASS_KEY,
    )
    .await;
    let heartbeat_index = probe_index_name(&heartbeat).expect("heartbeat index name");
    sql::query(&format!("DROP INDEX {heartbeat_index}"))
        .execute(&database.pool)
        .await
        .expect("drop canonical heartbeat index");
    sql::query(&format!(
        "CREATE INDEX {} ON {} (
             task_id, role varchar_pattern_ops, sent_at DESC
         )",
        heartbeat_index, heartbeat
    ))
    .execute(&database.pool)
    .await
    .expect("create heartbeat index with a nondefault operator class");
    let index_definition: String = sql::query_scalar("SELECT pg_get_indexdef(to_regclass($1))")
        .bind(&heartbeat_index)
        .fetch_one(&database.pool)
        .await
        .expect("read nondefault heartbeat index definition");
    assert!(index_definition.contains("varchar_pattern_ops"));

    let outcome = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("repair nondefault heartbeat operator class");
    assert!(matches!(outcome, CoverageOutcome::Ensured(_)));
    let index_definition: String = sql::query_scalar("SELECT pg_get_indexdef(to_regclass($1))")
        .bind(&heartbeat_index)
        .fetch_one(&database.pool)
        .await
        .expect("read repaired heartbeat index definition");
    assert!(index_definition.ends_with("USING btree (task_id, role, sent_at DESC)"));
    database.drop().await;
}

#[tokio::test]
async fn pool_coverage_repairs_only_the_damaged_leaf_in_one_mutation_transaction() {
    let database = TestDatabase::create_with_connections(4).await;
    let setup = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("create repair coverage");
    assert!(matches!(setup, CoverageOutcome::Ensured(_)));
    let damaged_index: String = sql::query_scalar(&format!(
        "SELECT leaf_name || '_task_idx' FROM {LEAF_CATALOG}
         WHERE class_key = 'standard_30d' AND dropped_at IS NULL
         ORDER BY lower_anchor LIMIT 1"
    ))
    .fetch_one(&database.pool)
    .await
    .expect("read damaged index name");
    sql::query(&format!("DROP INDEX {damaged_index}"))
        .execute(&database.pool)
        .await
        .expect("drop one required index");

    let (proxy, pool) = proxied_core_pool(&database, 0, 4, None).await;
    let outcome = ensure_partition_coverage_in_pool(
        &pool,
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("repair one damaged leaf");
    let CoverageOutcome::Ensured(ensured) = outcome else {
        panic!("damaged index must be repairable");
    };
    assert_eq!(ensured.created_history_leaves, 0);
    assert_eq!(ensured.created_heartbeat_leaves, 0);
    let sql = proxy.sql();
    assert_eq!(
        sql.iter()
            .filter(|statement| statement.starts_with("BEGIN"))
            .count(),
        2,
        "one gate transaction and one leaf mutation transaction are required"
    );
    // The leaf lock and the index repair run inside the one creation call.
    assert_eq!(
        sql.iter()
            .filter(|statement| statement.contains("horsies_create_history_leaf("))
            .count(),
        1
    );
    assert!(!sql
        .iter()
        .any(|statement| statement.contains("horsies_create_heartbeat_leaf(")));
    let repaired: bool = sql::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(&damaged_index)
        .fetch_one(&database.pool)
        .await
        .expect("read repaired index");
    assert!(repaired);
    pool.close();
    proxy.stop().await;
    database.drop().await;
}

/// Python `coverage_gate_enabled`: on a pool of one connection a gated pass
/// takes no gate, so it does not wait for a second checkout until the pool
/// timeout; it repairs under the per-leaf locks.
#[tokio::test]
async fn a_gated_pass_on_one_connection_repairs_without_the_gate() {
    let database = TestDatabase::create_with_connections(4).await;
    let pool = crate::broker::pools::session_core_pool_for_test_url(database.pool.url(), 1);

    let outcome = ensure_partition_coverage_in_pool(
        &pool,
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("a gated pass on one connection");

    let CoverageOutcome::Ensured(ensured) = outcome else {
        panic!("one connection repairs the set: {outcome:?}");
    };
    assert!(ensured.created_history_leaves > 0, "{ensured:?}");
    assert!(ensured.heartbeat_covered_now);
    pool.close();
    database.drop().await;
}

#[tokio::test]
async fn busy_coverage_gate_keeps_startup_ready_when_current_heartbeat_exists() {
    let database = TestDatabase::create_with_connections(4).await;
    let setup = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("create current heartbeat coverage");
    assert!(matches!(setup, CoverageOutcome::Ensured(_)));

    let mut holder = database
        .pool
        .begin()
        .await
        .expect("begin coverage gate holder");
    let held: bool = sql::query_scalar(
        "SELECT pg_try_advisory_xact_lock(
             hashtextextended('horsies:partition-coverage:v1', 1601)
         )",
    )
    .fetch_one(&mut *holder)
    .await
    .expect("hold coverage maintenance gate");
    assert!(held);

    let startup = ensure_startup_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        3,
        3,
        &[],
    )
    .await
    .expect("check startup under a busy coverage gate");
    let StartupCoverageOutcome::Ready(CoverageOutcome::Failed(failure)) = startup else {
        panic!("current heartbeat coverage must keep non-owner startup ready");
    };
    assert_eq!(failure.stage, CoverageFailureStage::CoverageGateBusy);
    assert!(failure.heartbeat_covered_now);
    holder
        .rollback()
        .await
        .expect("release coverage gate holder");
    database.drop().await;
}

#[tokio::test]
async fn periodic_coverage_repairs_while_a_startup_pass_holds_the_gate() {
    let database = TestDatabase::create_with_connections(4).await;
    let setup = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("create coverage");
    assert!(matches!(setup, CoverageOutcome::Ensured(_)));
    let damaged_index: String = sql::query_scalar(&format!(
        "SELECT leaf_name || '_task_idx' FROM {LEAF_CATALOG}
         WHERE class_key = 'standard_30d' AND dropped_at IS NULL
         ORDER BY lower_anchor LIMIT 1"
    ))
    .fetch_one(&database.pool)
    .await
    .expect("read damaged index name");
    sql::query(&format!("DROP INDEX {damaged_index}"))
        .execute(&database.pool)
        .await
        .expect("drop one required index");

    let mut holder = database
        .pool
        .begin()
        .await
        .expect("begin coverage gate holder");
    let held: bool = sql::query_scalar(
        "SELECT pg_try_advisory_xact_lock(
             hashtextextended('horsies:partition-coverage:v1', 1601)
         )",
    )
    .fetch_one(&mut *holder)
    .await
    .expect("hold coverage maintenance gate");
    assert!(held);

    let gated = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("gated pass under a busy gate");
    let CoverageOutcome::Failed(failure) = gated else {
        panic!("a gated pass under a busy gate repairs nothing: {gated:?}");
    };
    assert_eq!(failure.stage, CoverageFailureStage::CoverageGateBusy);
    let periodic = ensure_periodic_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("periodic pass under a busy gate");
    assert!(
        matches!(periodic, CoverageOutcome::Ensured(_)),
        "{periodic:?}"
    );
    let repaired: bool = sql::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(&damaged_index)
        .fetch_one(&database.pool)
        .await
        .expect("read repaired index");
    assert!(repaired);
    holder
        .rollback()
        .await
        .expect("release coverage gate holder");
    database.drop().await;
}

/// A busy coverage gate with no current heartbeat leaf is the transient
/// `GateBusy` verdict, which the worker start retries, not a refusal.
#[tokio::test]
async fn busy_coverage_gate_makes_startup_wait_when_current_heartbeat_is_absent() {
    let database = TestDatabase::create_with_connections(4).await;
    let mut holder = database
        .pool
        .begin()
        .await
        .expect("begin coverage gate holder");
    let held: bool = sql::query_scalar(
        "SELECT pg_try_advisory_xact_lock(
             hashtextextended('horsies:partition-coverage:v1', 1601)
         )",
    )
    .fetch_one(&mut *holder)
    .await
    .expect("hold coverage maintenance gate");
    assert!(held);

    let startup = ensure_startup_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("check cold startup under a busy coverage gate");
    let StartupCoverageOutcome::GateBusy(failure) = startup else {
        panic!("a busy gate with no current heartbeat leaf must be GateBusy: {startup:?}");
    };
    assert_eq!(failure.stage, CoverageFailureStage::CoverageGateBusy);
    assert!(!failure.heartbeat_covered_now);
    holder
        .rollback()
        .await
        .expect("release coverage gate holder");
    database.drop().await;
}

#[tokio::test]
async fn pending_blocker_refuses_then_detach_restores_timeout_and_drop_reconciles_catalog() {
    let database = TestDatabase::create().await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut connection = database.pool.acquire().await.expect("acquire clock");
    let now = database_now(&mut connection).await;
    drop(connection);
    let lower = now - Duration::days(5);
    let lower = lower
        .with_hour(0)
        .and_then(|value| value.with_minute(0))
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate expired day");
    let leaf = leaf_ref(&parent, class_key, lower);
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));
    let mut transaction = database.pool.begin().await.expect("begin blocker insert");
    let workflow_id = Uuid::new_v4();
    let node_row_id = Uuid::new_v4();
    let pending_task_id = Uuid::new_v4();
    sql::query(
        "INSERT INTO horsies_workflows (
             id, name, definition_key, status, on_error, depth, root_workflow_id,
             sent_at, created_at, started_at, updated_at
         ) VALUES ($1, 'p3 blocker', NULL, 'RUNNING', 'fail', 0, $1,
                   NOW(), NOW(), NOW(), NOW())",
    )
    .bind(workflow_id)
    .execute(&mut *transaction)
    .await
    .expect("insert blocker workflow");
    sql::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow, id, workflow_id, task_index, task_name, input, queue_name, priority,
             dependencies, allow_failed_deps, join_type, max_retries, execution,
             retention_class_key, status, is_subworkflow, created_at
         ) VALUES ('NONE', $1, $2, 0, 'p3 blocker node', '{}', 'default', 100,
                   '{}', FALSE, 'all', 0, 'ISOLATED',
                   $3, 'RUNNING', FALSE, NOW())",
    )
    .bind(node_row_id)
    .bind(workflow_id)
    .bind(class_key)
    .execute(&mut *transaction)
    .await
    .expect("insert blocker workflow node");
    // The pending table holds only the deferred kinds, so the blocker is
    // COMPLETE_LOCKED.
    sql::query(
        "INSERT INTO horsies_workflow_phase2_pending (
             task_id, workflow_id, workflow_node_row_id, terminalization_kind,
             recovery_source, history_class, history_anchor, history_schema_version,
             result_digest, quarantine_task_id, created_at, attempt_count,
             last_attempt_at, last_failure_class
         ) VALUES ($1, $2, $3, 'COMPLETE_LOCKED', 'HISTORY',
                   $5, $4, 1, $6, NULL, statement_timestamp(), 0, NULL, NULL)",
    )
    .bind(pending_task_id)
    .bind(workflow_id)
    .bind(node_row_id)
    .bind(lower + Duration::hours(1))
    .bind(class_key)
    .bind(vec![7_u8; 32])
    .execute(&mut *transaction)
    .await
    .expect("insert pending blocker");
    transaction.commit().await.expect("commit blocker");
    let detach = DetachExpiredHistoryLeaf::new(leaf.clone(), Some(5_000)).expect("detach command");
    // One connection: the detach takes the session whose timeouts are set
    // below.
    let core = crate::broker::pools::session_core_pool_for_test_url(database.pool.url(), 1);
    let blocked = detach_expired_leaf(&core, StatementLog::Off, &detach)
        .await
        .expect("blocked detach");
    assert!(matches!(
        blocked,
        DetachExpiredLeafOutcome::Inspection(LeafInspection::PendingBlocked {
            blocker_count: 1,
            ..
        })
    ));
    sql::query("DELETE FROM horsies_workflow_phase2_pending")
        .execute(&database.pool)
        .await
        .expect("clear pending blocker");
    set_core_session_timeouts(&core, "17s", "3s").await;
    let detached = detach_expired_leaf(&core, StatementLog::Off, &detach)
        .await
        .expect("detach expired leaf");
    assert!(matches!(
        detached,
        DetachExpiredLeafOutcome::Detached { .. }
    ));
    assert_eq!(
        core_session_timeouts(&core).await,
        ("17s".to_owned(), "3s".to_owned())
    );
    let dropped = drop_detached_leaf_in_pool(
        &core,
        StatementLog::Off,
        &DropDetachedHistoryLeaf::new(leaf.clone()),
    )
    .await
    .expect("drop detached leaf");
    assert!(matches!(dropped, LeafDrop::Dropped { .. }));
    assert!(matches!(
        inspect_leaf_in_pool(&core, StatementLog::Off, &InspectHistoryLeaf::new(leaf))
            .await
            .expect("inspect dropped leaf"),
        LeafInspection::Dropped { .. }
    ));

    let referenced_leaf = leaf_ref(&parent, class_key, lower - Duration::days(2));
    assert!(matches!(
        create_leaf(&database.pool, &referenced_leaf).await,
        LeafCreation::Created { .. }
    ));
    // The published manifest names the leaf, and a detach out of band leaves
    // it detached with no record and no publication, so the manifest still
    // names the detached leaf and its drop is refused. (A detach of the pass
    // records it and publishes the readers without it.)
    let mut publication = database.pool.acquire().await.expect("acquire publication");
    StagedLoaderPublisher
        .republish(&mut publication)
        .await
        .expect("publish the readers over the referenced leaf");
    drop(publication);
    sql::query(&format!(
        "ALTER TABLE {parent} DETACH PARTITION {}",
        referenced_leaf.leaf_name()
    ))
    .execute(&database.pool)
    .await
    .expect("detach the referenced leaf out of band");
    let refused = drop_detached_leaf_in_pool(
        &core,
        StatementLog::Off,
        &DropDetachedHistoryLeaf::new(referenced_leaf.clone()),
    )
    .await
    .expect("loader-reference drop refusal");
    assert!(matches!(refused, LeafDrop::RefusedLoaderReferences { .. }));
    sql::query(&format!("DROP TABLE {parent} CASCADE"))
        .execute(&database.pool)
        .await
        .expect("drop parent while detached relation survives");
    let missing_parent = inspect_leaf_in_pool(
        &core,
        StatementLog::Off,
        &InspectHistoryLeaf::new(referenced_leaf.clone()),
    )
    .await
    .expect_err("detached leaf with missing parent must fail closed");
    let expected_message = format!(
        "parent {parent} of leaf {} does not exist",
        referenced_leaf.leaf_name()
    );
    assert!(
        matches!(
            &missing_parent,
            HistoryError::Statement(StatementFailure::StatementRefused { cause, .. })
                if cause.server_error().message() == expected_message
        ),
        "{missing_parent:?}"
    );
    core.close();
    database.drop().await;
}

#[tokio::test]
async fn dropping_a_leaf_deletes_the_clock_exceptions_of_its_rows() {
    let database = TestDatabase::create_with_connections(4).await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut clock = database.pool.acquire().await.expect("acquire clock");
    let now = database_now(&mut clock).await;
    drop(clock);
    let lower = utc_day_start(now - Duration::days(5));
    let leaf = leaf_ref(&parent, class_key, lower);
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));
    let inside = Uuid::new_v4();
    let next_day = Uuid::new_v4();
    let other_class = Uuid::new_v4();
    for (task_id, exception_class, terminal_at) in [
        (inside, class_key, lower + Duration::hours(1)),
        (next_day, class_key, lower + Duration::hours(25)),
        (other_class, "standard_30d", lower + Duration::hours(1)),
    ] {
        sql::query(
            "INSERT INTO horsies_task_history_clock_exceptions (
                 task_id, retention_class_key, terminal_at
             ) VALUES ($1, $2, $3)",
        )
        .bind(task_id)
        .bind(exception_class)
        .bind(terminal_at)
        .execute(&database.pool)
        .await
        .expect("seed clock exception");
    }
    let detach = DetachExpiredHistoryLeaf::new(leaf.clone(), Some(5_000)).expect("detach command");
    assert!(matches!(
        detach_expired_leaf(
            &core_pool_for_test_url(database.pool.url()),
            StatementLog::Off,
            &detach
        )
        .await
        .expect("detach expired leaf"),
        DetachExpiredLeafOutcome::Detached { .. }
    ));
    let dropper = core_pool_for_test_url(database.pool.url());
    assert!(matches!(
        drop_detached_leaf_in_pool(
            &dropper,
            StatementLog::Off,
            &DropDetachedHistoryLeaf::new(leaf)
        )
        .await
        .expect("drop detached leaf"),
        LeafDrop::Dropped { .. }
    ));
    dropper.close();
    let mut remaining: Vec<Uuid> =
        sql::query_scalar("SELECT task_id FROM horsies_task_history_clock_exceptions")
            .fetch_all(&database.pool)
            .await
            .expect("read remaining exceptions");
    remaining.sort();
    let mut expected = vec![next_day, other_class];
    expected.sort();
    assert_eq!(remaining, expected);
    database.drop().await;
}

#[tokio::test]
async fn heartbeat_sweep_reports_one_leaf_error_and_sweeps_the_next_leaf() {
    let database = TestDatabase::create_with_connections(3).await;
    let mut transaction = database.pool.begin().await.expect("begin heartbeat setup");
    let _registration = register_heartbeat_class(&mut transaction, 2).await;
    let now = database_now(&mut transaction).await;
    let current_hour = now
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate hour");
    let older = hourly_leaf_ref(current_hour - Duration::hours(7)).expect("older heartbeat ref");
    let newer = hourly_leaf_ref(current_hour - Duration::hours(6)).expect("newer heartbeat ref");
    for leaf in [&older, &newer] {
        assert!(matches!(
            create_heartbeat_leaf_of(&mut *transaction, leaf)
                .await
                .expect("create expired heartbeat leaf"),
            LeafCreation::Created { .. }
        ));
    }
    transaction.commit().await.expect("commit heartbeat setup");

    let holder = hold_catalog_row(&database.pool, older.leaf_name()).await;
    let LeafSweep {
        swept,
        errors,
        reach,
        ..
    } = sweep_expired_heartbeat_leaves(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
        64,
    )
    .await
    .expect("discover expired heartbeats");
    holder.rollback().await.expect("release the catalog row");
    assert_eq!(reach, PruneReach::WithinBounds);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].starts_with(older.leaf_name()));
    assert!(errors[0].contains("lock timeout"), "{errors:?}");
    assert_eq!(swept.len(), 1);
    assert_eq!(swept[0].leaf_name, newer.leaf_name());
    assert!(matches!(
        swept[0].detach,
        DetachExpiredLeafOutcome::Detached { .. }
    ));
    database.drop().await;
}

#[tokio::test]
async fn pruning_finalizes_a_timeout_interrupted_detach_before_sweeping_it() {
    let database = TestDatabase::create_with_connections(4).await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut clock = database
        .pool
        .acquire()
        .await
        .expect("acquire finalization clock");
    let now = database_now(&mut clock).await;
    drop(clock);
    let lower = (now - Duration::days(6))
        .with_hour(0)
        .and_then(|value| value.with_minute(0))
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate interrupted leaf day");
    let leaf = leaf_ref(&parent, class_key, lower);
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));

    let mut blocker = database.pool.acquire().await.expect("acquire old snapshot");
    sql::query("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *blocker)
        .await
        .expect("begin old snapshot");
    sql::query(&format!("SELECT count(*) FROM {}", leaf.leaf_name()))
        .execute(&mut *blocker)
        .await
        .expect("establish old snapshot on leaf");
    let command =
        DetachExpiredHistoryLeaf::new(leaf.clone(), Some(100)).expect("short detach command");
    let timed_out = detach_expired_leaf(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
        &command,
    )
    .await
    .expect_err("old snapshot must interrupt concurrent detach at the timeout");
    assert!(timed_out.to_string().contains("statement timeout"));

    let inspector = core_pool_for_test_url(database.pool.url());
    assert!(matches!(
        inspect_leaf_in_pool(
            &inspector,
            StatementLog::Off,
            &InspectHistoryLeaf::new(leaf.clone())
        )
        .await
        .expect("classify interrupted detach"),
        LeafInspection::DetachInterrupted { .. }
    ));
    inspector.close();
    sql::query("ROLLBACK")
        .execute(&mut *blocker)
        .await
        .expect("release old snapshot");
    drop(blocker);

    let pass = prune_expired_partitions(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
    )
    .await
    .expect("a pruning pass");
    assert_eq!(pass.finalized_leaves, vec![leaf.leaf_name().to_owned()]);
    assert_eq!(pass.dropped_count(), 1);
    assert!(pass.errors.is_empty(), "prune errors: {:?}", pass.errors);
    database.drop().await;
}

#[tokio::test]
async fn history_sweep_stops_at_its_leaf_bound_and_reports_truncation() {
    let database = TestDatabase::create_with_connections(4).await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut clock = database.pool.acquire().await.expect("acquire clock");
    let base = utc_day_start(database_now(&mut clock).await - Duration::days(8));
    drop(clock);
    for offset in 0..3 {
        let leaf = leaf_ref(&parent, class_key, base + Duration::days(offset));
        assert!(matches!(
            create_leaf(&database.pool, &leaf).await,
            LeafCreation::Created { .. }
        ));
    }
    let LeafSweep {
        swept: first,
        errors: first_errors,
        reach: first_reach,
        ..
    } = sweep_expired_history_leaves(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
        2,
    )
    .await
    .expect("a history sweep");
    assert!(first_errors.is_empty(), "{first_errors:?}");
    assert_eq!(first.len(), 2);
    assert_eq!(first_reach, PruneReach::Truncated);
    let LeafSweep {
        swept: second,
        errors: second_errors,
        reach: second_reach,
        ..
    } = sweep_expired_history_leaves(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
        2,
    )
    .await
    .expect("a history sweep");
    assert!(second_errors.is_empty(), "{second_errors:?}");
    assert_eq!(second.len(), 1);
    assert_eq!(second_reach, PruneReach::WithinBounds);
    assert!(first
        .iter()
        .chain(second.iter())
        .all(|entry| matches!(entry.drop, Some(LeafDrop::Dropped { .. }))));
    database.drop().await;
}

#[tokio::test]
async fn a_refused_leaf_does_not_count_toward_the_leaf_bound() {
    let database = TestDatabase::create_with_connections(4).await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut clock = database.pool.acquire().await.expect("acquire clock");
    let base = utc_day_start(database_now(&mut clock).await - Duration::days(8));
    drop(clock);
    let leaves: Vec<LeafRef> = (0..3)
        .map(|offset| leaf_ref(&parent, class_key, base + Duration::days(offset)))
        .collect();
    for leaf in &leaves {
        assert!(matches!(
            create_leaf(&database.pool, leaf).await,
            LeafCreation::Created { .. }
        ));
    }
    let mut holder = database.pool.acquire().await.expect("acquire lock holder");
    sql::query(&format!(
        "SELECT pg_advisory_lock({LEAF_LOCK_KEY_FUNCTION}($1, $2))"
    ))
    .bind(class_key)
    .bind(leaves[0].bounds().lower())
    .execute(&mut *holder)
    .await
    .expect("hold the oldest leaf's lock");

    let sweep = sweep_expired_history_leaves(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
        1,
    )
    .await
    .expect("a history sweep");
    assert!(sweep.errors.is_empty(), "{:?}", sweep.errors);
    assert_eq!(sweep.swept.len(), 2);
    assert_eq!(sweep.swept[0].leaf_name, leaves[0].leaf_name());
    assert!(matches!(
        sweep.swept[0].detach,
        DetachExpiredLeafOutcome::Busy { .. }
    ));
    assert_eq!(sweep.swept[1].leaf_name, leaves[1].leaf_name());
    assert!(matches!(
        sweep.swept[1].drop,
        Some(LeafDrop::Dropped { .. })
    ));
    assert_eq!(sweep.acted, 1);
    assert_eq!(sweep.reach, PruneReach::Truncated);

    let released: bool = sql::query_scalar(&format!(
        "SELECT pg_advisory_unlock({LEAF_LOCK_KEY_FUNCTION}($1, $2))"
    ))
    .bind(class_key)
    .bind(leaves[0].bounds().lower())
    .fetch_one(&mut *holder)
    .await
    .expect("release the oldest leaf's lock");
    assert!(released);
    drop(holder);
    database.drop().await;
}

/// Whether the relation `name` exists.
async fn relation_exists(pool: &TestPool, name: &str) -> bool {
    sql::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(name)
        .fetch_one(pool)
        .await
        .expect("read the relation")
}

/// Registers the heartbeat class and creates the expired hourly heartbeat
/// leaves `hours_ago` hours before the current hour, in order; returns their
/// refs.
async fn expired_heartbeat_leaves(pool: &TestPool, hours_ago: &[i64]) -> Vec<LeafRef> {
    let mut transaction = pool.begin().await.expect("begin heartbeat setup");
    let _registration = register_heartbeat_class(&mut transaction, 2).await;
    let now = database_now(&mut transaction).await;
    let current_hour = now
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate hour");
    let mut leaves = Vec::new();
    for &hours_back in hours_ago {
        let leaf = hourly_leaf_ref(current_hour - Duration::hours(hours_back))
            .expect("expired heartbeat ref");
        assert!(matches!(
            create_heartbeat_leaf_of(&mut *transaction, &leaf)
                .await
                .expect("create expired heartbeat leaf"),
            LeafCreation::Created { .. }
        ));
        leaves.push(leaf);
    }
    transaction.commit().await.expect("commit heartbeat setup");
    leaves
}

/// A pass acts on its leaf bound with heartbeat leaves first: with a bound
/// of 1, the expired heartbeat leaf is swept, the expired period leaf is
/// left for the next pass, and the pass is `Truncated`.
#[tokio::test]
async fn heartbeat_leaves_take_the_leaf_bound_first() {
    let database = TestDatabase::create_with_connections(4).await;
    let heartbeat_leaves = expired_heartbeat_leaves(&database.pool, &[6]).await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut clock = database.pool.acquire().await.expect("acquire clock");
    let base = utc_day_start(database_now(&mut clock).await - Duration::days(8));
    drop(clock);
    let history_leaf = leaf_ref(&parent, class_key, base);
    assert!(matches!(
        create_leaf(&database.pool, &history_leaf).await,
        LeafCreation::Created { .. }
    ));

    let pass = prune_expired_partitions_within(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
        1,
    )
    .await
    .expect("a pruning pass");

    let heartbeat_swept: Vec<&str> = pass
        .heartbeat_swept
        .iter()
        .map(|entry| entry.leaf_name.as_str())
        .collect();
    assert_eq!(heartbeat_swept, vec![heartbeat_leaves[0].leaf_name()]);
    assert!(pass.history_swept.is_empty(), "{:?}", pass.history_swept);
    assert_eq!(pass.reach, PruneReach::Truncated);
    assert!(relation_exists(&database.pool, history_leaf.leaf_name()).await);
    database.drop().await;
}

/// A leaf whose sweep fails counts toward the leaf bound, since its effects
/// are unknown: with a bound of 1, the failed older heartbeat leaf is the
/// only one reported, the newer expired leaf is left for the next pass, and
/// the sweep is `Truncated`.
#[tokio::test]
async fn a_failed_leaf_counts_toward_the_leaf_bound() {
    let database = TestDatabase::create_with_connections(4).await;
    let leaves = expired_heartbeat_leaves(&database.pool, &[7, 6]).await;
    let (failing, expired) = (&leaves[0], &leaves[1]);

    let holder = hold_catalog_row(&database.pool, failing.leaf_name()).await;
    let LeafSweep {
        swept,
        errors,
        reach,
        ..
    } = sweep_expired_heartbeat_leaves(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
        1,
    )
    .await
    .expect("discover expired heartbeats");
    holder.rollback().await.expect("release the catalog row");

    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].starts_with(failing.leaf_name()), "{errors:?}");
    assert!(swept.is_empty(), "{swept:?}");
    assert_eq!(reach, PruneReach::Truncated);
    assert!(relation_exists(&database.pool, expired.leaf_name()).await);
    database.drop().await;
}

/// A leaf bound of 0 is a budget that acts on nothing: the expired leaf
/// stays, and the sweep is `Truncated`.
#[tokio::test]
async fn a_zero_leaf_bound_acts_on_no_leaf() {
    let database = TestDatabase::create_with_connections(4).await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut clock = database.pool.acquire().await.expect("acquire clock");
    let base = utc_day_start(database_now(&mut clock).await - Duration::days(8));
    drop(clock);
    let leaf = leaf_ref(&parent, class_key, base);
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));

    let sweep = sweep_expired_history_leaves(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
        0,
    )
    .await
    .expect("a history sweep");

    assert!(sweep.swept.is_empty(), "{:?}", sweep.swept);
    assert!(sweep.errors.is_empty(), "{:?}", sweep.errors);
    assert_eq!(sweep.acted, 0);
    assert_eq!(sweep.reach, PruneReach::Truncated);
    assert!(relation_exists(&database.pool, leaf.leaf_name()).await);
    database.drop().await;
}

#[tokio::test]
async fn pruning_contains_one_leaf_error_and_one_drop_refusal_then_keeps_going() {
    let database = TestDatabase::create_with_connections(4).await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut clock = database
        .pool
        .acquire()
        .await
        .expect("acquire pruning clock");
    let now = database_now(&mut clock).await;
    drop(clock);
    let base = utc_day_start(now - Duration::days(8));
    let leaves: Vec<LeafRef> = (0..3)
        .map(|offset| leaf_ref(&parent, class_key, base + Duration::days(offset)))
        .collect();
    for leaf in &leaves {
        assert!(matches!(
            create_leaf(&database.pool, leaf).await,
            LeafCreation::Created { .. }
        ));
    }
    // The middle leaf is detached out of band, and another transaction holds
    // its leaf lock: the pass records its detach, and its drop is refused as
    // busy.
    sql::query(&format!(
        "ALTER TABLE {parent} DETACH PARTITION {}",
        leaves[1].leaf_name()
    ))
    .execute(&database.pool)
    .await
    .expect("detach the middle leaf out of band");
    let mut lock_holder = database.pool.begin().await.expect("begin leaf lock holder");
    sql::query(&format!(
        "SELECT pg_advisory_xact_lock({LEAF_LOCK_KEY_FUNCTION}($1, $2))"
    ))
    .bind(class_key)
    .bind(leaves[1].bounds().lower())
    .execute(&mut *lock_holder)
    .await
    .expect("hold the middle leaf's lock");
    // The detach record of the oldest leaf fails at the lock timeout.
    let holder = hold_catalog_row(&database.pool, leaves[0].leaf_name()).await;
    let pass = prune_expired_partitions(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
    )
    .await
    .expect("a pruning pass");
    holder.rollback().await.expect("release the catalog row");
    lock_holder
        .rollback()
        .await
        .expect("release the middle leaf's lock");

    assert_eq!(pass.history_swept.len(), 2);
    assert_eq!(pass.errors.len(), 1, "{:?}", pass.errors);
    assert!(pass.errors[0].starts_with(leaves[0].leaf_name()));
    assert!(pass.errors[0].contains("lock timeout"), "{:?}", pass.errors);
    assert_eq!(
        pass.refusals,
        vec![format!("{}: LeafMaintenanceBusy", leaves[1].leaf_name())]
    );
    // Python counts a leaf left detached, by this pass or before it.
    assert_eq!(pass.detached_count(), 2);
    assert_eq!(pass.dropped_count(), 1);
    assert!(pass.acted());
    database.drop().await;
}

/// The transaction that last wrote the published provenance reader. A reader
/// publication that changes anything replaces the four readers, so this
/// changes exactly when a publication ran and was not `ALREADY_CURRENT`.
async fn reader_write_version(pool: &TestPool) -> String {
    sql::query_scalar(
        "SELECT p.xmin::text FROM pg_proc AS p
         WHERE p.oid = to_regprocedure((
             SELECT s.signature FROM horsies_history_reader_signatures() AS s
             ORDER BY s.signature LIMIT 1
         ))",
    )
    .fetch_one(pool)
    .await
    .expect("read the reader write version")
}

/// A detach (or the finalize of an interrupted one) publishes the readers
/// once for a history leaf that the manifest names, and never for a
/// heartbeat leaf; a second finalize publishes nothing. After both, the
/// readers are current and do not name the leaf.
#[tokio::test]
async fn detach_publishes_changed_history_only_and_finalization_repairs_stale_readers() {
    use super::manager::finalize_interrupted_detach;
    use crate::core::history::commands::FinalizeInterruptedLeafDetach;

    for heartbeat in [true, false] {
        for interrupted in [false, true] {
            let database = TestDatabase::create_with_connections(4).await;
            let parent = register_class(&database.pool, "standard_1d", 1).await;
            let mut tx = database.pool.begin().await.unwrap();
            register_heartbeat_class(&mut tx, 3).await;
            let now = database_now(&mut tx).await;
            let lower = (now - Duration::days(6))
                .with_hour(0)
                .unwrap()
                .with_minute(0)
                .unwrap()
                .with_second(0)
                .unwrap()
                .with_nanosecond(0)
                .unwrap();
            let leaf = match heartbeat {
                true => {
                    let leaf = hourly_leaf_ref(lower).unwrap();
                    create_heartbeat_leaf_of(&mut *tx, &leaf).await.unwrap();
                    leaf
                }
                false => leaf_ref(&parent, "standard_1d", lower),
            };
            tx.commit().await.unwrap();
            if !heartbeat {
                assert!(matches!(
                    create_leaf(&database.pool, &leaf).await,
                    LeafCreation::Created { .. }
                ));
            }
            let mut connection = database.pool.acquire().await.unwrap();
            StagedLoaderPublisher
                .republish(&mut connection)
                .await
                .unwrap();
            drop(connection);
            let published = reader_write_version(&database.pool).await;
            let core = core_pool_for_test_url(database.pool.url());
            let command = DetachExpiredHistoryLeaf::new(leaf.clone(), Some(100)).unwrap();
            match interrupted {
                true => {
                    let mut blocker = database.pool.acquire().await.unwrap();
                    sql::query("BEGIN ISOLATION LEVEL REPEATABLE READ")
                        .execute(&mut *blocker)
                        .await
                        .unwrap();
                    sql::query(&format!("SELECT count(*) FROM {}", leaf.leaf_name()))
                        .execute(&mut *blocker)
                        .await
                        .unwrap();
                    let error = detach_expired_leaf(&core, StatementLog::Off, &command)
                        .await
                        .unwrap_err();
                    assert!(error.to_string().contains("statement timeout"));
                    sql::query("ROLLBACK").execute(&mut *blocker).await.unwrap();
                    drop(blocker);
                }
                false => {
                    let result = detach_expired_leaf(&core, StatementLog::Off, &command)
                        .await
                        .unwrap();
                    assert!(matches!(result, DetachExpiredLeafOutcome::Detached { .. }));
                }
            }
            let finalize = FinalizeInterruptedLeafDetach::new(leaf.clone(), Some(5_000)).unwrap();
            finalize_interrupted_detach(&core, StatementLog::Off, &finalize)
                .await
                .unwrap();
            let after_first = reader_write_version(&database.pool).await;
            assert_eq!(after_first != published, !heartbeat);
            finalize_interrupted_detach(&core, StatementLog::Off, &finalize)
                .await
                .unwrap();
            assert_eq!(reader_write_version(&database.pool).await, after_first);
            let mut connection = database.pool.acquire().await.unwrap();
            assert!(!StagedLoaderPublisher
                .needs_republication(&mut connection)
                .await
                .unwrap());
            assert!(!StagedLoaderPublisher
                .references_leaf(&mut connection, leaf.leaf_name())
                .await
                .unwrap());
            drop(connection);
            database.drop().await;
        }
    }
}

/// A publication that fails after the detach statement committed rolls back
/// the record; the leaf stays detached with the manifest naming it. The next
/// finalize records the detach and publishes the readers in
/// `horsies_begin_leaf_detach`, and a repeat publishes nothing.
#[tokio::test]
async fn finalization_retries_publication_after_detach_committed() {
    use super::manager::finalize_interrupted_detach;
    use crate::core::history::commands::FinalizeInterruptedLeafDetach;

    let database = TestDatabase::create_with_connections(4).await;
    let class = "standard_1d";
    let parent = register_class(&database.pool, class, 1).await;
    let mut connection = database.pool.acquire().await.unwrap();
    let now = database_now(&mut connection).await;
    drop(connection);
    let lower = (now - Duration::days(6))
        .with_hour(0)
        .unwrap()
        .with_minute(0)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();
    let leaf = leaf_ref(&parent, class, lower);
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));
    let mut connection = database.pool.acquire().await.unwrap();
    StagedLoaderPublisher
        .republish(&mut connection)
        .await
        .unwrap();
    drop(connection);
    // The manifest refuses every row that a publication writes (the static
    // forever leaf stays in it), so the publication of the detach record
    // fails in the database.
    sql::query(&format!(
        "ALTER TABLE {TASK_LOOKUP_MANIFEST} ADD CONSTRAINT test_reject_publication \
         CHECK (probe_position < 0) NOT VALID"
    ))
    .execute(&database.pool)
    .await
    .unwrap();
    let core = core_pool_for_test_url(database.pool.url());
    let command = DetachExpiredHistoryLeaf::new(leaf.clone(), Some(5_000)).unwrap();
    let error = detach_expired_leaf(&core, StatementLog::Off, &command)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("test_reject_publication"),
        "{error}"
    );
    sql::query(&format!(
        "ALTER TABLE {TASK_LOOKUP_MANIFEST} DROP CONSTRAINT test_reject_publication"
    ))
    .execute(&database.pool)
    .await
    .unwrap();
    let mut connection = database.pool.acquire().await.unwrap();
    assert!(StagedLoaderPublisher
        .references_leaf(&mut connection, leaf.leaf_name())
        .await
        .unwrap());
    drop(connection);
    // The leaf is DETACHED: horsies_begin_leaf_detach records its detach
    // and, because the published manifest still names it, publishes the
    // readers in its own statement. A repeat finds nothing left to publish.
    let finalize = FinalizeInterruptedLeafDetach::new(leaf.clone(), Some(5_000)).unwrap();
    finalize_interrupted_detach(&core, StatementLog::Off, &finalize)
        .await
        .unwrap();
    let mut connection = database.pool.acquire().await.unwrap();
    assert!(!StagedLoaderPublisher
        .references_leaf(&mut connection, leaf.leaf_name())
        .await
        .unwrap());
    drop(connection);
    let published = reader_write_version(&database.pool).await;
    finalize_interrupted_detach(&core, StatementLog::Off, &finalize)
        .await
        .unwrap();
    assert_eq!(reader_write_version(&database.pool).await, published);
    database.drop().await;
}

/// One catalog row of a leaf: name, lower bound kind, lower anchor, upper
/// anchor.
type LeafCatalogRow = (
    String,
    String,
    Option<chrono::DateTime<Utc>>,
    chrono::DateTime<Utc>,
);

async fn forever_leaves(
    connection: &mut TestConnection,
) -> Vec<(String, LeafLowerBound, chrono::DateTime<Utc>)> {
    let rows: Vec<LeafCatalogRow> = sql::query_as(&format!(
        "SELECT leaf_name, lower_bound_kind::text, lower_anchor, upper_anchor
             FROM {LEAF_CATALOG}
             WHERE class_key = 'forever' AND detached_at IS NULL AND dropped_at IS NULL
             ORDER BY lower_anchor NULLS FIRST"
    ))
    .fetch_all(connection)
    .await
    .expect("read forever leaves");
    rows.into_iter()
        .map(|(name, kind, lower, upper)| {
            let bound = LeafLowerBound::decode(&kind, lower).expect("stored lower bound pair");
            (name, bound, upper)
        })
        .collect()
}

#[tokio::test]
async fn pool_coverage_creates_the_period_leaves_and_no_forever_leaf() {
    let database = TestDatabase::create_with_connections(4).await;
    let mut reader = database.pool.acquire().await.unwrap();
    let before = forever_leaves(&mut reader).await;
    drop(reader);
    let first = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        3,
        2,
        &[],
    )
    .await
    .unwrap();
    let CoverageOutcome::Ensured(report) = first else {
        panic!("{first:?}");
    };
    // Four standard daily leaves; the static forever leaf exists from install.
    assert_eq!(report.created_history_leaves, 4);
    let mut reader = database.pool.acquire().await.unwrap();
    assert_eq!(forever_leaves(&mut reader).await, before);
    drop(reader);
    let second = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        3,
        2,
        &[],
    )
    .await
    .unwrap();
    let CoverageOutcome::Ensured(report) = second else {
        panic!("{second:?}");
    };
    assert_eq!(report.created_history_leaves, 0);
    database.drop().await;
}

/// The duplicate guard of an id born now probes the manifest leaves whose
/// upper anchor is after now. After a pass with a history horizon of 3 days,
/// these are the 4 daily leaves of the default class and the static forever
/// leaf: no other forever leaf and no heartbeat leaf.
#[tokio::test]
async fn guard_probe_set_is_standard_horizon_plus_the_static_forever_leaf() {
    let database = TestDatabase::create_with_connections(4).await;
    let coverage = ensure_partition_coverage_in_pool(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
        3,
        2,
        &[],
    )
    .await
    .expect("coverage pass");
    assert!(
        matches!(coverage, CoverageOutcome::Ensured(_)),
        "{coverage:?}"
    );
    let probed: Vec<(String, String)> = sql::query_as(&format!(
        "SELECT catalog.class_key, manifest.leaf_name
         FROM {TASK_LOOKUP_MANIFEST} AS manifest
         JOIN {LEAF_CATALOG} AS catalog USING (leaf_name)
         WHERE manifest.upper_anchor > statement_timestamp()
         ORDER BY 1, 2"
    ))
    .fetch_all(&database.pool)
    .await
    .expect("read the probed manifest leaves");
    let standard = probed
        .iter()
        .filter(|(class, _)| class == "standard_30d")
        .count();
    let forever: Vec<&str> = probed
        .iter()
        .filter(|(class, _)| class == FOREVER_CLASS_KEY)
        .map(|(_, name)| name.as_str())
        .collect();
    assert_eq!(standard, 4, "{probed:?}");
    assert_eq!(forever, vec![TASK_HISTORY_FOREVER_LEAF]);
    assert_eq!(probed.len(), standard + forever.len(), "{probed:?}");
    database.drop().await;
}

/// A history leaf removed out of band keeps its attached catalog row, is left
/// out of the reader manifest and is the one absent leaf. A heartbeat leaf
/// removed out of band is in neither list. The next coverage pass fails the
/// class of the removed history leaf and names the leaf.
#[tokio::test]
async fn manifest_excludes_absent_probes_and_keeps_the_complete_attached_set() {
    let database = TestDatabase::create_with_connections(4).await;
    let class_key = "standard_34d";
    let parent = register_class(&database.pool, class_key, 34).await;
    let today = utc_day_start(database_now(&database.pool).await);
    let gone = leaf_ref(&parent, class_key, today);
    let kept = leaf_ref(&parent, class_key, today + Duration::days(1));
    for leaf in [&gone, &kept] {
        assert!(matches!(
            create_leaf(&database.pool, leaf).await,
            LeafCreation::Created { .. }
        ));
    }
    let heartbeat_leaves = expired_heartbeat_leaves(&database.pool, &[10]).await;
    let heartbeat = heartbeat_leaves[0].leaf_name();
    for relation in [gone.leaf_name(), heartbeat] {
        sql::query(&format!("DROP TABLE {relation}"))
            .execute(&database.pool)
            .await
            .expect("drop a leaf out of band");
    }

    let attached: Vec<String> = sql::query_scalar(&format!(
        "SELECT leaf_name FROM {LEAF_CATALOG}
         WHERE detached_at IS NULL AND dropped_at IS NULL ORDER BY leaf_name"
    ))
    .fetch_all(&database.pool)
    .await
    .expect("read the attached catalog leaves");
    assert!(
        attached.iter().any(|name| name == gone.leaf_name()),
        "{attached:?}"
    );
    assert!(
        attached.iter().any(|name| name == kept.leaf_name()),
        "{attached:?}"
    );
    let manifest: Vec<String> =
        sql::query_scalar("SELECT leaf_name FROM horsies_history_reader_manifest()")
            .fetch_all(&database.pool)
            .await
            .expect("read the reader manifest");
    assert!(
        manifest.iter().any(|name| name == kept.leaf_name()),
        "{manifest:?}"
    );
    assert!(
        !manifest.iter().any(|name| name == gone.leaf_name()),
        "{manifest:?}"
    );
    assert!(
        !manifest.iter().any(|name| name == heartbeat),
        "{manifest:?}"
    );
    let absent: Vec<String> = sql::query_scalar("SELECT horsies_history_absent_leaves()")
        .fetch_one(&database.pool)
        .await
        .expect("read the absent history leaves");
    assert_eq!(absent, vec![gone.leaf_name().to_owned()]);

    let coverage = ensure_partition_coverage_in_pool(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("coverage pass");
    match coverage {
        CoverageOutcome::Failed(failure) => {
            assert_eq!(failure.stage, CoverageFailureStage::EnsureLeafCoverage);
            assert_eq!(failure.class_key.as_deref(), Some(class_key));
            assert!(failure.refusal.contains(gone.leaf_name()), "{failure:?}");
            assert_eq!(failure.absent_leaves, vec![gone.leaf_name().to_owned()]);
        }
        other => panic!("a removed in-horizon leaf fails its class: {other:?}"),
    }
    database.drop().await;
}

fn utc_day_start(value: chrono::DateTime<Utc>) -> chrono::DateTime<Utc> {
    value.date_naive().and_hms_opt(0, 0, 0).unwrap().and_utc()
}

mod coverage_ports;
