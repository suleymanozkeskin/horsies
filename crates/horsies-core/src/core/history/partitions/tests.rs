use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{Duration, Timelike, Utc};
use serial_test::serial;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use sqlx::{PgConnection, PgPool};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::broker::core_rows::StatementLog;
use crate::broker::pools::core_pool_for_test_pool;
use crate::core::history::commands::{
    open_end_anchor, CreateDailyHistoryLeaf, DetachExpiredHistoryLeaf, DropDetachedHistoryLeaf,
    EnsureLeafCoverage, InspectHistoryLeaf, LeafBounds, LeafRef,
};
use crate::core::history::ddl::classes::{
    finite_class_parent_name, register_period_class, ClassRegistration, FOREVER_CLASS_KEY,
};
use crate::core::history::ddl::runtime_names::{
    daily_leaf_name, leaf_enqueued_index_name, leaf_id_index_name, render_daily_leaf_ddl,
};
use crate::core::history::errors::HistoryError;
use crate::core::history::heartbeats::partitioning::{
    create_hourly_heartbeat_leaf, ensure_heartbeat_coverage, heartbeat_horizon, hourly_leaf_name,
    hourly_leaf_ref, probe_index_name, register_heartbeat_class, sweep_expired_heartbeat_leaves,
    CreateHourlyHeartbeatLeaf, EnsureHeartbeatCoverage, HeartbeatClassRegistration,
};
use crate::core::history::maintenance::coverage::{
    ensure_partition_coverage, ensure_partition_coverage_in_pool, ensure_periodic_coverage_in_pool,
    ensure_startup_coverage, ensure_startup_coverage_in_pool, CoverageOutcome,
    StartupCoverageOutcome,
};
use crate::core::history::maintenance::pruning::{
    prune_expired_partitions, sweep_counts_toward_bound, sweep_expired_history_leaves, LeafSweep,
    PruneReach,
};
use crate::core::history::names::{
    HEARTBEATS_TABLE, HEARTBEAT_CLASS_KEY, LEAF_CATALOG, LEAF_LOCK_KEY_FUNCTION, RETENTION_CLASSES,
    TASK_HISTORY_FOREVER, TASK_HISTORY_FOREVER_LEAF, TASK_LOOKUP_MANIFEST,
};
use crate::core::history::outcomes::{CatalogConflictKind, LeafCreation, LeafDrop, LeafInspection};
use crate::core::history::periods::{HeartbeatHorizonHours, RetentionDays};
use crate::core::history::reads::publisher::StagedLoaderPublisher;
use crate::db::{CheckoutClass, CorePool, ParameterType, StatementBound, TypedStatement};

use super::catalog::{
    capture_partition_bound_utc, database_now, read_leaf_catalog_row, read_leaf_physical_state,
    LeafCatalogRow, LeafIndexKind, LeafLowerBound, LeafPartitionBoundExpectation,
};
use super::manager::{
    create_daily_leaf, detach_expired_leaf, drop_detached_leaf, ensure_leaf_coverage, inspect_leaf,
    verify_static_forever_leaf, DetachExpiredLeafOutcome, STATIC_FOREVER_LEAF_BOUND,
};
use super::publication::{LoaderPublication, LoaderRepublished, UnpublishedLoader};

mod lifecycle_ports;

/// The attached history leaves whose relation is gone, in name order
/// (`horsies_history_absent_leaves()`, 0008).
async fn absent_history_leaves(
    connection: &mut PgConnection,
) -> Result<Vec<String>, crate::core::history::errors::HistoryError> {
    Ok(sqlx::query_scalar("SELECT horsies_history_absent_leaves()")
        .fetch_one(connection)
        .await?)
}

/// A publisher that renders nothing and reports the absent history leaves.
#[derive(Debug, Default)]
struct CatalogPublisher;

impl LoaderPublication for CatalogPublisher {
    async fn republish(
        &self,
        connection: &mut PgConnection,
    ) -> Result<LoaderRepublished, crate::core::history::errors::HistoryError> {
        Ok(LoaderRepublished {
            absent_leaves: absent_history_leaves(connection).await?,
        })
    }

    async fn references_leaf(
        &self,
        _connection: &mut PgConnection,
        _leaf_name: &str,
    ) -> Result<bool, crate::core::history::errors::HistoryError> {
        Ok(false)
    }

    async fn needs_republication(
        &self,
        connection: &mut PgConnection,
    ) -> Result<bool, crate::core::history::errors::HistoryError> {
        Ok(!absent_history_leaves(connection).await?.is_empty())
    }
}

/// A copy of the installed test template (`crate::test_support`) with a
/// sqlx pool for the history functions that still take a sqlx connection.
struct TestDatabase {
    pool: PgPool,
    database: crate::test_support::TestDatabase,
}

impl TestDatabase {
    async fn create() -> Self {
        Self::create_with_connections(1).await
    }

    async fn create_with_connections(max_connections: u32) -> Self {
        let database = crate::test_support::TestDatabase::create().await;
        let pool = PgPoolOptions::new()
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
    async fn start(backend: &PgConnectOptions, delay_ms: usize) -> Self {
        Self::start_with_pause(backend, delay_ms, None).await
    }

    async fn start_with_pause(
        backend: &PgConnectOptions,
        delay_ms: usize,
        pause: Option<StatementPause>,
    ) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind SQL statement proxy");
        let port = listener.local_addr().expect("proxy address").port();
        let backend_host = backend.get_host().to_owned();
        let backend_port = backend.get_port();
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
) -> (SqlStatementProxy, PgPool) {
    let backend = PgConnectOptions::from_str(database.url())
        .expect("invalid test database URL")
        .ssl_mode(PgSslMode::Disable);
    let proxy = SqlStatementProxy::start(&backend, delay_ms).await;
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .test_before_acquire(true)
        .connect_with(
            backend
                .clone()
                .host("127.0.0.1")
                .port(proxy.port)
                .ssl_mode(PgSslMode::Disable),
        )
        .await
        .expect("connect through SQL statement proxy");
    sqlx::query("SELECT 1")
        .execute(&pool)
        .await
        .expect("warm proxied PostgreSQL connection");
    proxy.reset();
    (proxy, pool)
}

async fn pausing_proxied_pool(
    database: &TestDatabase,
    pattern: &str,
) -> (SqlStatementProxy, PgPool, StatementPause) {
    let backend = PgConnectOptions::from_str(database.url())
        .expect("invalid test database URL")
        .ssl_mode(PgSslMode::Disable);
    let pause = StatementPause::new(Arc::<str>::from(pattern));
    let proxy = SqlStatementProxy::start_with_pause(&backend, 0, Some(pause.clone())).await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            backend
                .clone()
                .host("127.0.0.1")
                .port(proxy.port)
                .ssl_mode(PgSslMode::Disable),
        )
        .await
        .expect("connect through pausing SQL statement proxy");
    (proxy, pool, pause)
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
    let core = crate::broker::pools::session_core_pool_for_test_pool(&pool, max_connections);
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

async fn register_class(pool: &PgPool, class_key: &str, days: i64) -> String {
    let period = RetentionDays::parse_period(Duration::days(days)).expect("retention period");
    assert_eq!(
        period.class_key(),
        class_key,
        "a class key is derived from its period"
    );
    let mut transaction = pool.begin().await.expect("begin class registration");
    let outcome = register_period_class(&mut transaction, period)
        .await
        .expect("register finite class");
    assert!(matches!(
        outcome,
        ClassRegistration::Registered { .. } | ClassRegistration::AlreadyRegistered { .. }
    ));
    transaction
        .commit()
        .await
        .expect("commit class registration");
    finite_class_parent_name(class_key).expect("finite parent name")
}

fn hours(count: u32) -> HeartbeatHorizonHours {
    HeartbeatHorizonHours::new(count).expect("heartbeat horizon")
}

/// The catalog row of the first leaf that `predicate` selects, in
/// `(lower_anchor, leaf_name)` order.
async fn catalog_row_where(pool: &PgPool, predicate: &str, class_key: &str) -> LeafCatalogRow {
    let leaf_name: String = sqlx::query_scalar(&format!(
        "SELECT leaf_name FROM {LEAF_CATALOG} WHERE {predicate}
         ORDER BY lower_anchor, leaf_name LIMIT 1"
    ))
    .bind(class_key)
    .fetch_one(pool)
    .await
    .expect("select catalog leaf");
    let mut connection = pool.acquire().await.expect("acquire catalog reader");
    read_leaf_catalog_row(&mut connection, &leaf_name)
        .await
        .expect("read catalog row")
        .expect("selected leaf has a catalog row")
}

/// The time of a bounded leaf's lower bound.
fn bounded_lower(row: &LeafCatalogRow) -> chrono::DateTime<Utc> {
    row.lower_anchor
        .at()
        .expect("a leaf that maintenance creates has a time lower bound")
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
async fn hold_catalog_row(
    pool: &PgPool,
    leaf_name: &str,
) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut holder = pool.begin().await.expect("begin catalog row holder");
    sqlx::query(&format!(
        "SELECT 1 FROM {LEAF_CATALOG} WHERE leaf_name = $1 FOR UPDATE"
    ))
    .bind(leaf_name)
    .execute(&mut *holder)
    .await
    .expect("hold the catalog row");
    holder
}

async fn create_leaf(pool: &PgPool, leaf: &LeafRef) -> LeafCreation {
    let mut transaction = pool.begin().await.expect("begin leaf creation");
    let outcome = create_daily_leaf(
        &mut transaction,
        &CreateDailyHistoryLeaf::new(leaf.clone()).expect("daily command"),
        &UnpublishedLoader,
    )
    .await
    .expect("create leaf");
    transaction.commit().await.expect("commit leaf creation");
    outcome
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

#[tokio::test]
#[serial]
async fn lifecycle_is_idempotent_repairs_index_property_and_is_timezone_independent() {
    let database = TestDatabase::create().await;
    let class_key = "standard_31d";
    let parent = register_class(&database.pool, class_key, 31).await;
    let mut transaction = database.pool.begin().await.expect("begin coverage");
    sqlx::query("SELECT set_config('timezone', 'Etc/GMT+12', false)")
        .execute(&mut *transaction)
        .await
        .expect("set creating timezone");
    let command = EnsureLeafCoverage::new(class_key, 3).expect("coverage command");
    let first = ensure_leaf_coverage(&mut transaction, &command, &UnpublishedLoader)
        .await
        .expect("first coverage");
    assert_eq!(first.len(), 4);
    assert!(first
        .iter()
        .all(|item| matches!(item, LeafCreation::Created { .. })));
    transaction.commit().await.expect("commit coverage");

    let mut transaction = database.pool.begin().await.expect("begin verification");
    sqlx::query("SELECT set_config('timezone', 'Etc/GMT-12', false)")
        .execute(&mut *transaction)
        .await
        .expect("set inspecting timezone");
    let second = ensure_leaf_coverage(&mut transaction, &command, &UnpublishedLoader)
        .await
        .expect("second coverage");
    assert!(second
        .iter()
        .all(|item| matches!(item, LeafCreation::AlreadyConformant { .. })));
    let now = database_now(&mut transaction).await.expect("database now");
    let today = now
        .with_hour(0)
        .and_then(|value| value.with_minute(0))
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate today");
    let leaf = leaf_ref(&parent, class_key, today);
    assert!(read_leaf_catalog_row(&mut transaction, leaf.leaf_name())
        .await
        .expect("read cataloged leaf")
        .expect("cataloged current leaf")
        .describes(class_key, leaf.bounds()));
    let utc_bound = capture_partition_bound_utc(&mut transaction, leaf.leaf_name())
        .await
        .expect("capture UTC partition bound")
        .expect("current leaf has a partition bound");
    for timezone in ["UTC", "Etc/GMT+12", "Etc/GMT-12"] {
        sqlx::query("SELECT set_config('timezone', $1, false)")
            .bind(timezone)
            .execute(&mut *transaction)
            .await
            .expect("set bound probe timezone");
        assert_eq!(
            capture_partition_bound_utc(&mut transaction, leaf.leaf_name())
                .await
                .expect("capture UTC partition bound")
                .as_deref(),
            Some(utc_bound.as_str())
        );
    }
    let id_index = leaf_id_index_name(leaf.leaf_name());
    sqlx::query(&format!("DROP INDEX {id_index}"))
        .execute(&mut *transaction)
        .await
        .expect("drop task-ID index");
    assert!(matches!(
        create_daily_leaf(
            &mut transaction,
            &CreateDailyHistoryLeaf::new(leaf.clone()).expect("repair command"),
            &UnpublishedLoader,
        )
        .await
        .expect("repair task-ID index"),
        LeafCreation::IndexRepaired { .. }
    ));
    let canonical_order = leaf_enqueued_index_name(leaf.leaf_name());
    sqlx::query(&format!("DROP INDEX {canonical_order}"))
        .execute(&mut *transaction)
        .await
        .expect("drop canonical ordering index");
    sqlx::query(&format!(
        "CREATE INDEX p3_property_named_index ON {} (enqueued_at)",
        leaf.leaf_name()
    ))
    .execute(&mut *transaction)
    .await
    .expect("create property-equivalent index");
    let verified = create_daily_leaf(
        &mut transaction,
        &CreateDailyHistoryLeaf::new(leaf).expect("daily command"),
        &UnpublishedLoader,
    )
    .await
    .expect("verify property index");
    assert!(matches!(verified, LeafCreation::AlreadyConformant { .. }));
    let squatter = leaf_ref(&parent, class_key, today + Duration::days(10));
    sqlx::query(&format!(
        "CREATE TABLE {} (x integer)",
        squatter.leaf_name()
    ))
    .execute(&mut *transaction)
    .await
    .expect("create uncataloged squatter relation");
    let conflict = create_daily_leaf(
        &mut transaction,
        &CreateDailyHistoryLeaf::new(squatter).expect("squatter command"),
        &UnpublishedLoader,
    )
    .await
    .expect("classify uncataloged relation");
    assert!(matches!(
        conflict,
        LeafCreation::CatalogConflict {
            kind: crate::core::history::outcomes::CatalogConflictKind::RelationWithoutCatalog,
            ..
        }
    ));
    transaction.commit().await.expect("commit verification");
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn conformant_leaf_skips_a_busy_lock_and_missing_leaf_returns_busy() {
    let database = TestDatabase::create_with_connections(3).await;
    let class_key = "standard_32d";
    let parent = register_class(&database.pool, class_key, 32).await;
    let mut clock = database.pool.acquire().await.expect("acquire clock");
    let now = database_now(&mut clock).await.expect("database now");
    drop(clock);
    let today = now
        .with_hour(0)
        .and_then(|value| value.with_minute(0))
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate today");
    let conformant = leaf_ref(&parent, class_key, today);
    assert!(matches!(
        create_leaf(&database.pool, &conformant).await,
        LeafCreation::Created { .. }
    ));

    let lock_sql = format!("SELECT pg_advisory_lock({LEAF_LOCK_KEY_FUNCTION}($1, $2))");
    let unlock_sql = format!("SELECT pg_advisory_unlock({LEAF_LOCK_KEY_FUNCTION}($1, $2))");
    let mut holder = database.pool.acquire().await.expect("acquire lock holder");
    sqlx::query(&lock_sql)
        .bind(class_key)
        .bind(conformant.bounds().lower())
        .execute(&mut *holder)
        .await
        .expect("hold conformant leaf lock");
    let mut transaction = database.pool.begin().await.expect("begin fast path");
    let fast_path = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        create_daily_leaf(
            transaction.as_mut(),
            &CreateDailyHistoryLeaf::new(conformant.clone()).expect("conformant command"),
            &UnpublishedLoader,
        ),
    )
    .await
    .expect("conformant fast path must not wait")
    .expect("conformant fast path");
    assert!(matches!(fast_path, LeafCreation::AlreadyConformant { .. }));
    transaction.rollback().await.expect("rollback fast path");
    let released: bool = sqlx::query_scalar(&unlock_sql)
        .bind(class_key)
        .bind(conformant.bounds().lower())
        .fetch_one(&mut *holder)
        .await
        .expect("unlock conformant leaf");
    assert!(released);

    let missing = leaf_ref(&parent, class_key, today + Duration::days(10));
    sqlx::query(&lock_sql)
        .bind(class_key)
        .bind(missing.bounds().lower())
        .execute(&mut *holder)
        .await
        .expect("hold missing leaf lock");
    let mut transaction = database.pool.begin().await.expect("begin busy create");
    let busy = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        create_daily_leaf(
            transaction.as_mut(),
            &CreateDailyHistoryLeaf::new(missing.clone()).expect("missing command"),
            &UnpublishedLoader,
        ),
    )
    .await
    .expect("busy create must not wait")
    .expect("busy create outcome");
    assert_eq!(
        busy,
        LeafCreation::Busy {
            leaf_name: missing.leaf_name().to_owned(),
        }
    );
    transaction.rollback().await.expect("rollback busy create");
    let released: bool = sqlx::query_scalar(&unlock_sql)
        .bind(class_key)
        .bind(missing.bounds().lower())
        .fetch_one(&mut *holder)
        .await
        .expect("unlock missing leaf");
    assert!(released);
    drop(holder);

    let table_locked = leaf_ref(&parent, class_key, today + Duration::days(11));
    let mut parent_holder = database.pool.begin().await.expect("begin parent lock");
    sqlx::query(&format!("LOCK TABLE {parent} IN ACCESS SHARE MODE"))
        .execute(parent_holder.as_mut())
        .await
        .expect("hold parent relation lock");
    let mut transaction = database
        .pool
        .begin()
        .await
        .expect("begin parent-busy create");
    let busy = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        create_daily_leaf(
            transaction.as_mut(),
            &CreateDailyHistoryLeaf::new(table_locked.clone()).expect("table-locked command"),
            &UnpublishedLoader,
        ),
    )
    .await
    .expect("parent-busy create must not wait")
    .expect("parent-busy create outcome");
    assert_eq!(
        busy,
        LeafCreation::Busy {
            leaf_name: table_locked.leaf_name().to_owned(),
        }
    );
    transaction
        .rollback()
        .await
        .expect("rollback parent-busy create");
    parent_holder
        .rollback()
        .await
        .expect("release parent relation lock");
    database.drop().await;
}

/// A second detach of a leaf reports it detached, and both detaches count
/// toward the acted-leaf bound of a pass, as Python's
/// `counts_toward_prune_bound` counts every `LeafDetached`. Both report the
/// Python outcome name `LeafDetached`.
#[tokio::test]
#[serial]
async fn a_second_detach_reports_the_leaf_detached_by_the_first() {
    let database = TestDatabase::create().await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut clock = database.pool.acquire().await.expect("acquire clock");
    let lower =
        utc_day_start(database_now(&mut clock).await.expect("database now") - Duration::days(5));
    drop(clock);
    let leaf = leaf_ref(&parent, class_key, lower);
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));
    let command = DetachExpiredHistoryLeaf::new(leaf.clone(), Some(5_000)).expect("detach command");
    let first = detach_expired_leaf(
        &core_pool_for_test_pool(&database.pool),
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
        &core_pool_for_test_pool(&database.pool),
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
#[serial]
async fn detach_returns_busy_without_waiting_for_a_leaf_session_lock() {
    let database = TestDatabase::create_with_connections(3).await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut clock = database.pool.acquire().await.expect("acquire clock");
    let now = database_now(&mut clock).await.expect("database now");
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
    sqlx::query(&lock_sql)
        .bind(class_key)
        .bind(lower)
        .execute(&mut *holder)
        .await
        .expect("hold detach leaf lock");
    let command = DetachExpiredHistoryLeaf::new(leaf.clone(), Some(5_000)).expect("detach command");
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        detach_expired_leaf(
            &core_pool_for_test_pool(&database.pool),
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
    let released: bool = sqlx::query_scalar(&unlock_sql)
        .bind(class_key)
        .bind(lower)
        .fetch_one(&mut *holder)
        .await
        .expect("unlock detach leaf");
    assert!(released);
    drop(holder);

    let mut parent_holder = database.pool.begin().await.expect("begin parent lock");
    sqlx::query(&format!(
        "LOCK TABLE {parent} IN SHARE UPDATE EXCLUSIVE MODE"
    ))
    .execute(parent_holder.as_mut())
    .await
    .expect("hold detach parent lock");
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        detach_expired_leaf(
            &core_pool_for_test_pool(&database.pool),
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
async fn wait_for_an_advisory_lock_wait(pool: &PgPool) {
    for _ in 0..150 {
        let waiting: bool = sqlx::query_scalar(
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
#[serial]
async fn cancelled_detach_closes_its_connection_and_releases_the_session_lock() {
    let database = TestDatabase::create_with_connections(3).await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut clock = database.pool.acquire().await.expect("acquire clock");
    let now = database_now(&mut clock).await.expect("database now");
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
    sqlx::query(PUBLICATION_LOCK_SQL)
        .execute(&mut *publication)
        .await
        .expect("hold the reader publication lock");
    let command = DetachExpiredHistoryLeaf::new(leaf.clone(), Some(5_000)).expect("detach command");
    let core = core_pool_for_test_pool(&database.pool);
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
    sqlx::query(PUBLICATION_UNLOCK_SQL)
        .execute(&mut *publication)
        .await
        .expect("release the reader publication lock");
    drop(publication);

    let try_sql = format!("SELECT pg_try_advisory_lock({LEAF_LOCK_KEY_FUNCTION}($1, $2))");
    let unlock_sql = format!("SELECT pg_advisory_unlock({LEAF_LOCK_KEY_FUNCTION}($1, $2))");
    let mut probe = database.pool.acquire().await.expect("acquire lock probe");
    let mut acquired = false;
    for _ in 0..200 {
        acquired = sqlx::query_scalar(&try_sql)
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
    let released: bool = sqlx::query_scalar(&unlock_sql)
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
#[serial]
async fn pool_coverage_releases_each_leaf_transaction_before_returning() {
    let database = TestDatabase::create_with_connections(4).await;
    let outcome = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_pool(&database.pool),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("pool coverage");
    assert!(matches!(outcome, CoverageOutcome::Ensured(_)));
    let advisory_locks: i64 = sqlx::query_scalar(
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
#[serial]
async fn healthy_pool_coverage_has_a_fixed_statement_budget() {
    let database = TestDatabase::create_with_connections(4).await;
    let declared: Vec<RetentionDays> = (0..50)
        .map(|index| {
            RetentionDays::parse_period(Duration::days(7 + i64::from(index)))
                .expect("declared period")
        })
        .collect();

    let first = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_pool(&database.pool),
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
            &crate::broker::pools::core_pool_for_test_pool(&database.pool),
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
            &crate::broker::pools::core_pool_for_test_pool(&database.pool),
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
#[serial]
async fn failed_post_commit_publication_is_retried_by_the_next_healthy_pass() {
    let database = TestDatabase::create_with_connections(4).await;
    let core_pool = crate::broker::pools::core_pool_for_test_pool(&database.pool);
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
    sqlx::query(&format!(
        "UPDATE {TASK_LOOKUP_MANIFEST} SET probe_position = probe_position + 100"
    ))
    .execute(&database.pool)
    .await
    .expect("move every manifest position");
    // The manifest refuses the positions that a publication writes, so the
    // final publication of the next pass fails in the database.
    sqlx::query(&format!(
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

    sqlx::query(&format!(
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
#[serial]
async fn current_heartbeat_with_a_wrong_physical_range_refuses_startup() {
    let database = TestDatabase::create_with_connections(4).await;
    let setup = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_pool(&database.pool),
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
    ) = sqlx::query_as(&format!(
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
    sqlx::query(&format!(
        "ALTER TABLE {HEARTBEATS_TABLE} DETACH PARTITION {leaf_name}"
    ))
    .execute(&database.pool)
    .await
    .expect("detach current heartbeat leaf");
    sqlx::query(&format!("DROP TABLE {leaf_name}"))
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
    sqlx::query(
        &render_daily_leaf_ddl(HEARTBEATS_TABLE, &wrong_leaf)
            .expect("render wrong-range heartbeat leaf"),
    )
    .execute(&database.pool)
    .await
    .expect("create wrong-range heartbeat leaf");
    sqlx::query(&format!(
        "CREATE INDEX {index_name} ON {leaf_name} (task_id, role, sent_at DESC)"
    ))
    .execute(&database.pool)
    .await
    .expect("create conformant heartbeat index on wrong range");

    let startup = ensure_startup_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_pool(&database.pool),
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
#[serial]
async fn coverage_repairs_same_name_wrong_shape_and_invalid_indexes() {
    let database = TestDatabase::create_with_connections(4).await;
    let setup = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_pool(&database.pool),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("create index-repair coverage");
    assert!(matches!(setup, CoverageOutcome::Ensured(_)));

    let heartbeat = catalog_row_where(
        &database.pool,
        "class_key = $1
           AND lower_anchor <= statement_timestamp()
           AND upper_anchor > statement_timestamp()",
        HEARTBEAT_CLASS_KEY,
    )
    .await;
    let heartbeat_index = heartbeat.id_index_name().expect("heartbeat index name");
    sqlx::query(&format!("DROP INDEX {heartbeat_index}"))
        .execute(&database.pool)
        .await
        .expect("drop heartbeat index");
    sqlx::query(&format!(
        "CREATE INDEX {} ON {} (task_id, role, sent_at ASC)",
        heartbeat_index, heartbeat.leaf_name
    ))
    .execute(&database.pool)
    .await
    .expect("create same-name heartbeat index with wrong sort direction");

    let history = catalog_row_where(
        &database.pool,
        "class_key NOT IN ($1, 'forever')
           AND detached_at IS NULL
           AND dropped_at IS NULL",
        HEARTBEAT_CLASS_KEY,
    )
    .await;
    // The static forever leaf is install-owned and never repaired, so only
    // a daily history leaf's index is damaged here.
    let history_index = history.id_index_name().expect("history index name");
    sqlx::query("UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass($1)")
        .bind(&history_index)
        .execute(&database.pool)
        .await
        .expect("mark history index invalid");

    let outcome = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_pool(&database.pool),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("repair malformed indexes");
    assert!(matches!(outcome, CoverageOutcome::Ensured(_)));
    let heartbeat_bounds = LeafBounds::new(bounded_lower(&heartbeat), heartbeat.upper_anchor)
        .expect("heartbeat bounds");
    let history_bounds =
        LeafBounds::new(bounded_lower(&history), history.upper_anchor).expect("history bounds");
    let mut connection = database.pool.acquire().await.expect("acquire index reader");
    assert!(
        read_leaf_physical_state(
            &mut connection,
            &heartbeat.leaf_name,
            &heartbeat.parent_name().expect("heartbeat parent"),
            &heartbeat_index,
            LeafPartitionBoundExpectation::Requested(&heartbeat_bounds),
            LeafIndexKind::Heartbeat,
        )
        .await
        .expect("read repaired heartbeat index")
        .id_index_conformant
    );
    assert!(
        read_leaf_physical_state(
            &mut connection,
            &history.leaf_name,
            &history.parent_name().expect("history parent"),
            &history_index,
            LeafPartitionBoundExpectation::Requested(&history_bounds),
            LeafIndexKind::History,
        )
        .await
        .expect("read repaired history index")
        .id_index_conformant
    );
    drop(connection);
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn coverage_repairs_a_nondefault_heartbeat_operator_class() {
    let database = TestDatabase::create_with_connections(4).await;
    let setup = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_pool(&database.pool),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("create operator-class repair coverage");
    assert!(matches!(setup, CoverageOutcome::Ensured(_)));
    let heartbeat = catalog_row_where(
        &database.pool,
        "class_key = $1
           AND lower_anchor <= statement_timestamp()
           AND upper_anchor > statement_timestamp()",
        HEARTBEAT_CLASS_KEY,
    )
    .await;
    let heartbeat_index = heartbeat.id_index_name().expect("heartbeat index name");
    sqlx::query(&format!("DROP INDEX {heartbeat_index}"))
        .execute(&database.pool)
        .await
        .expect("drop canonical heartbeat index");
    sqlx::query(&format!(
        "CREATE INDEX {} ON {} (
             task_id, role varchar_pattern_ops, sent_at DESC
         )",
        heartbeat_index, heartbeat.leaf_name
    ))
    .execute(&database.pool)
    .await
    .expect("create heartbeat index with a nondefault operator class");
    let index_definition: String = sqlx::query_scalar("SELECT pg_get_indexdef(to_regclass($1))")
        .bind(&heartbeat_index)
        .fetch_one(&database.pool)
        .await
        .expect("read nondefault heartbeat index definition");
    assert!(index_definition.contains("varchar_pattern_ops"));

    let outcome = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_pool(&database.pool),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("repair nondefault heartbeat operator class");
    assert!(matches!(outcome, CoverageOutcome::Ensured(_)));
    let index_definition: String = sqlx::query_scalar("SELECT pg_get_indexdef(to_regclass($1))")
        .bind(&heartbeat_index)
        .fetch_one(&database.pool)
        .await
        .expect("read repaired heartbeat index definition");
    assert!(index_definition.ends_with("USING btree (task_id, role, sent_at DESC)"));
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn id_index_repair_refuses_a_name_reused_after_inspection() {
    let database = TestDatabase::create_with_connections(4).await;
    let class_key = "standard_33d";
    let parent = register_class(&database.pool, class_key, 33).await;
    let now = database_now(
        database
            .pool
            .acquire()
            .await
            .expect("acquire database clock")
            .as_mut(),
    )
    .await
    .expect("read database clock");
    let today = now
        .with_hour(0)
        .and_then(|value| value.with_minute(0))
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate current day");
    let leaf = leaf_ref(&parent, class_key, today);
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));
    let id_index_name = leaf_id_index_name(leaf.leaf_name());
    sqlx::query(&format!("DROP INDEX {id_index_name}"))
        .execute(&database.pool)
        .await
        .expect("drop canonical task-ID index");
    sqlx::query(&format!(
        "CREATE INDEX {id_index_name} ON {} (task_id DESC)",
        leaf.leaf_name()
    ))
    .execute(&database.pool)
    .await
    .expect("create malformed task-ID index");
    let foreign_table = "p3_index_reuse_foreign";
    sqlx::query(&format!("CREATE TABLE {foreign_table} (value integer)"))
        .execute(&database.pool)
        .await
        .expect("create foreign index owner");

    let (proxy, repair_pool, pause) = pausing_proxied_pool(
        &database,
        "SELECT CASE\n             WHEN to_regclass($2) IS NULL",
    )
    .await;
    let repair_leaf = leaf.clone();
    let repair = tokio::spawn(async move {
        let mut transaction = repair_pool.begin().await.expect("begin index repair");
        let outcome = create_daily_leaf(
            transaction.as_mut(),
            &CreateDailyHistoryLeaf::new(repair_leaf).expect("index repair command"),
            &UnpublishedLoader,
        )
        .await
        .expect("classify reused index name");
        transaction
            .commit()
            .await
            .expect("commit the refused index repair");
        outcome
    });
    pause.wait_until_entered().await;
    let moved_index_name = "p3_index_reuse_moved";
    sqlx::query(&format!(
        "ALTER INDEX {id_index_name} RENAME TO {moved_index_name}"
    ))
    .execute(&database.pool)
    .await
    .expect("rename the inspected malformed index");
    sqlx::query(&format!(
        "CREATE INDEX {id_index_name} ON {foreign_table} (value)"
    ))
    .execute(&database.pool)
    .await
    .expect("reuse the schema index name on a foreign table");
    pause.resume();

    let outcome = repair.await.expect("join index repair");
    assert!(matches!(
        outcome,
        LeafCreation::CatalogConflict {
            kind: CatalogConflictKind::PhysicalNonconformant,
            ..
        }
    ));
    let owner: String = sqlx::query_scalar(
        "SELECT index_state.indrelid::regclass::text
         FROM pg_index AS index_state
         WHERE index_state.indexrelid = to_regclass($1)",
    )
    .bind(&id_index_name)
    .fetch_one(&database.pool)
    .await
    .expect("read reused index owner");
    assert_eq!(owner, foreign_table);
    proxy.stop().await;
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn index_repair_lock_wait_is_bounded_and_restores_the_timeout() {
    let database = TestDatabase::create_with_connections(4).await;
    let class_key = "standard_35d";
    let parent = register_class(&database.pool, class_key, 35).await;
    let mut clock = database.pool.acquire().await.expect("acquire clock");
    let today = utc_day_start(database_now(&mut clock).await.expect("database now"));
    drop(clock);
    let leaf = leaf_ref(&parent, class_key, today);
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));
    let id_index_name = leaf_id_index_name(leaf.leaf_name());
    sqlx::query("UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass($1)")
        .bind(&id_index_name)
        .execute(&database.pool)
        .await
        .expect("mark the task-ID index invalid");
    // Another session holds the index relation lock: its uncommitted rename
    // takes the lock that the repair's rename needs.
    let mut blocker = database.pool.begin().await.expect("begin lock holder");
    sqlx::query(&format!(
        "ALTER INDEX {id_index_name} RENAME TO p3_lock_holder_idx"
    ))
    .execute(&mut *blocker)
    .await
    .expect("hold the index relation lock");

    let mut repair = database.pool.begin().await.expect("begin repair");
    let prior_timeout: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(&mut *repair)
        .await
        .expect("read prior lock timeout");
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        create_daily_leaf(
            repair.as_mut(),
            &CreateDailyHistoryLeaf::new(leaf.clone()).expect("repair command"),
            &UnpublishedLoader,
        ),
    )
    .await
    .expect("the repair's lock wait is bounded")
    .expect("classify the busy repair");
    assert!(matches!(outcome, LeafCreation::Busy { .. }), "{outcome:?}");
    let restored_timeout: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(&mut *repair)
        .await
        .expect("read restored lock timeout");
    assert_eq!(restored_timeout, prior_timeout);
    repair.rollback().await.expect("rollback repair");
    blocker.rollback().await.expect("release the index lock");
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn pool_coverage_repairs_only_the_damaged_leaf_in_one_mutation_transaction() {
    let database = TestDatabase::create_with_connections(4).await;
    let setup = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_pool(&database.pool),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("create repair coverage");
    assert!(matches!(setup, CoverageOutcome::Ensured(_)));
    let damaged_index: String = sqlx::query_scalar(&format!(
        "SELECT leaf_name || '_task_idx' FROM {LEAF_CATALOG}
         WHERE class_key = 'standard_30d' AND dropped_at IS NULL
         ORDER BY lower_anchor LIMIT 1"
    ))
    .fetch_one(&database.pool)
    .await
    .expect("read damaged index name");
    sqlx::query(&format!("DROP INDEX {damaged_index}"))
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
    let repaired: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
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
#[serial]
async fn a_gated_pass_on_one_connection_repairs_without_the_gate() {
    let database = TestDatabase::create_with_connections(4).await;
    let pool = crate::broker::pools::session_core_pool_for_test_pool(&database.pool, 1);

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
#[serial]
async fn busy_coverage_gate_keeps_startup_ready_when_current_heartbeat_exists() {
    let database = TestDatabase::create_with_connections(4).await;
    let setup = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_pool(&database.pool),
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
    let held: bool = sqlx::query_scalar(
        "SELECT pg_try_advisory_xact_lock(
             hashtextextended('horsies:partition-coverage:v1', 1601)
         )",
    )
    .fetch_one(holder.as_mut())
    .await
    .expect("hold coverage maintenance gate");
    assert!(held);

    let startup = ensure_startup_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_pool(&database.pool),
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
    assert_eq!(failure.stage, "coverage_gate_busy");
    assert!(failure.heartbeat_covered_now);
    holder
        .rollback()
        .await
        .expect("release coverage gate holder");
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn periodic_coverage_repairs_while_a_startup_pass_holds_the_gate() {
    let database = TestDatabase::create_with_connections(4).await;
    let setup = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_pool(&database.pool),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("create coverage");
    assert!(matches!(setup, CoverageOutcome::Ensured(_)));
    let damaged_index: String = sqlx::query_scalar(&format!(
        "SELECT leaf_name || '_task_idx' FROM {LEAF_CATALOG}
         WHERE class_key = 'standard_30d' AND dropped_at IS NULL
         ORDER BY lower_anchor LIMIT 1"
    ))
    .fetch_one(&database.pool)
    .await
    .expect("read damaged index name");
    sqlx::query(&format!("DROP INDEX {damaged_index}"))
        .execute(&database.pool)
        .await
        .expect("drop one required index");

    let mut holder = database
        .pool
        .begin()
        .await
        .expect("begin coverage gate holder");
    let held: bool = sqlx::query_scalar(
        "SELECT pg_try_advisory_xact_lock(
             hashtextextended('horsies:partition-coverage:v1', 1601)
         )",
    )
    .fetch_one(holder.as_mut())
    .await
    .expect("hold coverage maintenance gate");
    assert!(held);

    let gated = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_pool(&database.pool),
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
    assert_eq!(failure.stage, "coverage_gate_busy");
    let periodic = ensure_periodic_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_pool(&database.pool),
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
    let repaired: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
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

#[tokio::test]
#[serial]
async fn busy_coverage_gate_refuses_startup_when_current_heartbeat_is_absent() {
    let database = TestDatabase::create_with_connections(4).await;
    let mut holder = database
        .pool
        .begin()
        .await
        .expect("begin coverage gate holder");
    let held: bool = sqlx::query_scalar(
        "SELECT pg_try_advisory_xact_lock(
             hashtextextended('horsies:partition-coverage:v1', 1601)
         )",
    )
    .fetch_one(holder.as_mut())
    .await
    .expect("hold coverage maintenance gate");
    assert!(held);

    let startup = ensure_startup_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_pool(&database.pool),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("check cold startup under a busy coverage gate");
    let StartupCoverageOutcome::Refused(CoverageOutcome::Failed(failure)) = startup else {
        panic!("missing current heartbeat coverage must refuse startup");
    };
    assert_eq!(failure.stage, "coverage_gate_busy");
    assert!(!failure.heartbeat_covered_now);
    holder
        .rollback()
        .await
        .expect("release coverage gate holder");
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn pending_blocker_refuses_then_detach_restores_timeout_and_drop_reconciles_catalog() {
    let database = TestDatabase::create().await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut connection = database.pool.acquire().await.expect("acquire clock");
    let now = database_now(&mut connection).await.expect("database now");
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
    sqlx::query(
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
    sqlx::query(
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
    sqlx::query(
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
    let core = crate::broker::pools::session_core_pool_for_test_pool(&database.pool, 1);
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
    sqlx::query("DELETE FROM horsies_workflow_phase2_pending")
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
    let mut transaction = database.pool.begin().await.expect("begin drop");
    let dropped = drop_detached_leaf(
        &mut transaction,
        &DropDetachedHistoryLeaf::new(leaf.clone()),
    )
    .await
    .expect("drop detached leaf");
    assert!(matches!(dropped, LeafDrop::Dropped { .. }));
    assert!(matches!(
        inspect_leaf(&mut transaction, &InspectHistoryLeaf::new(leaf))
            .await
            .expect("inspect dropped leaf"),
        LeafInspection::Dropped { .. }
    ));
    transaction.commit().await.expect("commit drop");

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
    sqlx::query(&format!(
        "ALTER TABLE {parent} DETACH PARTITION {}",
        referenced_leaf.leaf_name()
    ))
    .execute(&database.pool)
    .await
    .expect("detach the referenced leaf out of band");
    let mut transaction = database.pool.begin().await.expect("begin refused drop");
    let refused = drop_detached_leaf(
        &mut transaction,
        &DropDetachedHistoryLeaf::new(referenced_leaf.clone()),
    )
    .await
    .expect("loader-reference drop refusal");
    assert!(matches!(refused, LeafDrop::RefusedLoaderReferences { .. }));
    transaction.commit().await.expect("commit refused drop");
    sqlx::query(&format!("DROP TABLE {parent} CASCADE"))
        .execute(&database.pool)
        .await
        .expect("drop parent while detached relation survives");
    let mut connection = database
        .pool
        .acquire()
        .await
        .expect("inspect missing parent");
    let missing_parent = inspect_leaf(&mut connection, &InspectHistoryLeaf::new(referenced_leaf))
        .await
        .expect_err("detached leaf with missing parent must fail closed");
    assert!(matches!(
        missing_parent,
        crate::core::history::errors::HistoryError::HistoryParentAbsent(_)
    ));
    drop(connection);
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn dropping_a_leaf_deletes_the_clock_exceptions_of_its_rows() {
    let database = TestDatabase::create_with_connections(4).await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut clock = database.pool.acquire().await.expect("acquire clock");
    let now = database_now(&mut clock).await.expect("database now");
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
        sqlx::query(
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
            &core_pool_for_test_pool(&database.pool),
            StatementLog::Off,
            &detach
        )
        .await
        .expect("detach expired leaf"),
        DetachExpiredLeafOutcome::Detached { .. }
    ));
    let mut transaction = database.pool.begin().await.expect("begin drop");
    assert!(matches!(
        drop_detached_leaf(&mut transaction, &DropDetachedHistoryLeaf::new(leaf),)
            .await
            .expect("drop detached leaf"),
        LeafDrop::Dropped { .. }
    ));
    transaction.commit().await.expect("commit drop");
    let mut remaining: Vec<Uuid> =
        sqlx::query_scalar("SELECT task_id FROM horsies_task_history_clock_exceptions")
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
#[serial]
async fn heartbeat_sweep_reports_one_leaf_error_and_sweeps_the_next_leaf() {
    let database = TestDatabase::create_with_connections(3).await;
    let mut transaction = database.pool.begin().await.expect("begin heartbeat setup");
    let _registration = register_heartbeat_class(&mut transaction, hours(2))
        .await
        .expect("register heartbeat class");
    let now = database_now(&mut transaction).await.expect("database now");
    let current_hour = now
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate hour");
    let older = hourly_leaf_ref(current_hour - Duration::hours(7)).expect("older heartbeat ref");
    let newer = hourly_leaf_ref(current_hour - Duration::hours(6)).expect("newer heartbeat ref");
    for leaf in [&older, &newer] {
        assert!(matches!(
            create_hourly_heartbeat_leaf(
                &mut transaction,
                &CreateHourlyHeartbeatLeaf::new(leaf.clone()).expect("hourly command"),
            )
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
        &core_pool_for_test_pool(&database.pool),
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
#[serial]
async fn heartbeat_registration_coverage_and_expiry_sweep_share_the_catalog() {
    let database = TestDatabase::create().await;
    let horizon = hours(2);
    let mut transaction = database.pool.begin().await.expect("begin heartbeat setup");
    let first = register_heartbeat_class(&mut transaction, horizon)
        .await
        .expect("register heartbeat class");
    assert!(matches!(
        first,
        HeartbeatClassRegistration::Registered { .. }
            | HeartbeatClassRegistration::HorizonUpdated { .. }
            | HeartbeatClassRegistration::Verified { .. }
    ));
    assert!(matches!(
        register_heartbeat_class(&mut transaction, horizon)
            .await
            .expect("verify heartbeat class"),
        HeartbeatClassRegistration::Verified { .. }
    ));
    assert!(matches!(
        register_heartbeat_class(&mut transaction, hours(3))
            .await
            .expect("update heartbeat horizon"),
        HeartbeatClassRegistration::HorizonUpdated {
            previous_horizon,
            horizon: updated,
        } if previous_horizon == horizon && updated == hours(3)
    ));
    let coverage = ensure_heartbeat_coverage(
        &mut transaction,
        &EnsureHeartbeatCoverage::new(2).expect("heartbeat coverage command"),
    )
    .await
    .expect("heartbeat coverage");
    assert_eq!(coverage.len(), 3);
    let now = database_now(&mut transaction).await.expect("database now");
    let expired_lower = now
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate hour")
        - Duration::hours(6);
    let expired = hourly_leaf_ref(expired_lower).expect("expired heartbeat ref");
    assert!(matches!(
        create_hourly_heartbeat_leaf(
            &mut transaction,
            &CreateHourlyHeartbeatLeaf::new(expired.clone()).expect("hourly command"),
        )
        .await
        .expect("create expired heartbeat leaf"),
        LeafCreation::Created { .. }
    ));
    let index_ddl: String = sqlx::query_scalar(
        "SELECT pg_get_indexdef(i.indexrelid) FROM pg_index AS i
         WHERE i.indrelid = to_regclass($1) AND NOT i.indisprimary",
    )
    .bind(expired.leaf_name())
    .fetch_one(&mut *transaction)
    .await
    .expect("heartbeat probe index");
    assert!(index_ddl.contains("(task_id, role, sent_at DESC)"));
    transaction.commit().await.expect("commit heartbeat setup");
    let LeafSweep {
        swept,
        errors,
        reach,
        ..
    } = sweep_expired_heartbeat_leaves(
        &core_pool_for_test_pool(&database.pool),
        StatementLog::Off,
        64,
    )
    .await
    .expect("discover expired heartbeats");
    assert!(errors.is_empty(), "{errors:?}");
    assert_eq!(reach, PruneReach::WithinBounds);
    assert_eq!(swept.len(), 1);
    assert_eq!(swept[0].leaf_name, expired.leaf_name());
    assert!(matches!(swept[0].drop, Some(LeafDrop::Dropped { .. })));
    let mut transaction = database
        .pool
        .begin()
        .await
        .expect("begin unpartitioned posture");
    sqlx::query("DROP TABLE horsies_heartbeats CASCADE")
        .execute(&mut *transaction)
        .await
        .expect("drop heartbeat partitioned parent");
    sqlx::query("CREATE TABLE horsies_heartbeats (sent_at timestamptz NOT NULL)")
        .execute(&mut *transaction)
        .await
        .expect("create unpartitioned heartbeat parent");
    assert_eq!(
        register_heartbeat_class(&mut transaction, hours(2))
            .await
            .expect("classify unpartitioned heartbeat parent"),
        HeartbeatClassRegistration::ParentUnpartitioned
    );
    let startup = ensure_startup_coverage(&mut transaction, 2, 2, &[], &UnpublishedLoader)
        .await
        .expect("startup coverage posture");
    assert!(matches!(startup, StartupCoverageOutcome::Refused(_)));
    transaction
        .commit()
        .await
        .expect("commit unpartitioned posture");
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn manifest_excludes_absent_probes_and_keeps_the_complete_attached_set() {
    let database = TestDatabase::create().await;
    let class_key = "standard_34d";
    let parent = register_class(&database.pool, class_key, 34).await;
    let mut transaction = database.pool.begin().await.expect("begin manifest setup");
    let now = database_now(&mut transaction).await.expect("database now");
    let today = now
        .with_hour(0)
        .and_then(|value| value.with_minute(0))
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate day");
    let gone = leaf_ref(&parent, class_key, today);
    let kept = leaf_ref(&parent, class_key, today + Duration::days(1));
    for leaf in [&gone, &kept] {
        assert!(matches!(
            create_daily_leaf(
                &mut transaction,
                &CreateDailyHistoryLeaf::new(leaf.clone()).expect("daily command"),
                &UnpublishedLoader,
            )
            .await
            .expect("create manifest leaf"),
            LeafCreation::Created { .. }
        ));
    }
    sqlx::query(&format!("DROP TABLE {}", gone.leaf_name()))
        .execute(&mut *transaction)
        .await
        .expect("drop history leaf out of band");
    let heartbeat = hourly_leaf_ref(
        now.with_minute(0)
            .and_then(|value| value.with_second(0))
            .and_then(|value| value.with_nanosecond(0))
            .expect("truncate hour")
            - Duration::hours(10),
    )
    .expect("heartbeat ref");
    let _ = register_heartbeat_class(&mut transaction, hours(2))
        .await
        .expect("register heartbeat class");
    let _ = create_hourly_heartbeat_leaf(
        &mut transaction,
        &CreateHourlyHeartbeatLeaf::new(heartbeat.clone()).expect("hourly command"),
    )
    .await
    .expect("create heartbeat leaf");
    sqlx::query(&format!("DROP TABLE {}", heartbeat.leaf_name()))
        .execute(&mut *transaction)
        .await
        .expect("drop heartbeat leaf out of band");
    // The catalog keeps both history leaves attached; the reader manifest
    // leaves out the one whose relation is gone, and the absent list names
    // it. The heartbeat leaf is in neither.
    let attached: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT leaf_name FROM {LEAF_CATALOG}
         WHERE detached_at IS NULL AND dropped_at IS NULL
         ORDER BY leaf_name"
    ))
    .fetch_all(&mut *transaction)
    .await
    .expect("read attached catalog leaves");
    assert!(attached.iter().any(|name| name == gone.leaf_name()));
    assert!(attached.iter().any(|name| name == kept.leaf_name()));
    let manifest: Vec<String> =
        sqlx::query_scalar("SELECT leaf_name FROM horsies_history_reader_manifest()")
            .fetch_all(&mut *transaction)
            .await
            .expect("read the reader manifest");
    assert!(manifest.iter().any(|name| name == kept.leaf_name()));
    assert!(!manifest.iter().any(|name| name == gone.leaf_name()));
    assert!(!manifest.iter().any(|name| name == heartbeat.leaf_name()));
    assert_eq!(
        absent_history_leaves(&mut transaction)
            .await
            .expect("read absent history leaves"),
        vec![gone.leaf_name().to_owned()]
    );
    let coverage = ensure_partition_coverage(&mut transaction, 2, 2, &[], &CatalogPublisher)
        .await
        .expect("self-correcting coverage pass");
    match coverage {
        CoverageOutcome::Failed(failure) => {
            assert!(failure.refusal.contains(class_key));
            assert_eq!(failure.absent_leaves, vec![gone.leaf_name().to_owned()]);
        }
        other => panic!("missing in-horizon relation must freeze its class: {other:?}"),
    }
    transaction.commit().await.expect("commit manifest setup");
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn coverage_savepoint_contains_database_failure_and_still_serves_other_classes() {
    let database = TestDatabase::create().await;
    // Coverage serves classes in key order: the broken class comes first.
    let survivor = "standard_7d";
    register_class(&database.pool, survivor, 7).await;
    let mut transaction = database.pool.begin().await.expect("begin bad class insert");
    let sql = format!(
        "INSERT INTO {RETENTION_CLASSES} (
             class_key, class_kind, retention_days, heartbeat_horizon_hours, created_at
         ) VALUES ('standard_10d', 'PERIOD', 10, NULL, statement_timestamp())"
    );
    sqlx::query(&sql)
        .execute(&mut *transaction)
        .await
        .expect("insert class with missing parent");
    let outcome = ensure_partition_coverage(&mut transaction, 2, 2, &[], &UnpublishedLoader)
        .await
        .expect("contained coverage pass");
    let failure = match outcome {
        CoverageOutcome::Failed(failure) => failure,
        other => panic!("expected contained failure, got {other:?}"),
    };
    assert!(failure.refusal.contains("standard_10d"));
    assert!(failure.heartbeat_covered_now);
    let survivor_count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {LEAF_CATALOG} WHERE class_key = $1"
    ))
    .bind(survivor)
    .fetch_one(&mut *transaction)
    .await
    .expect("connection remains usable after rollback to savepoint");
    assert_eq!(survivor_count, 3);
    transaction
        .rollback()
        .await
        .expect("rollback containment test");
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn pruning_finalizes_a_timeout_interrupted_detach_before_sweeping_it() {
    let database = TestDatabase::create_with_connections(4).await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut clock = database
        .pool
        .acquire()
        .await
        .expect("acquire finalization clock");
    let now = database_now(&mut clock).await.expect("database now");
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
    sqlx::query("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *blocker)
        .await
        .expect("begin old snapshot");
    sqlx::query(&format!("SELECT count(*) FROM {}", leaf.leaf_name()))
        .execute(&mut *blocker)
        .await
        .expect("establish old snapshot on leaf");
    let command =
        DetachExpiredHistoryLeaf::new(leaf.clone(), Some(100)).expect("short detach command");
    let timed_out = detach_expired_leaf(
        &core_pool_for_test_pool(&database.pool),
        StatementLog::Off,
        &command,
    )
    .await
    .expect_err("old snapshot must interrupt concurrent detach at the timeout");
    assert!(timed_out.to_string().contains("statement timeout"));

    let mut inspector = database
        .pool
        .acquire()
        .await
        .expect("inspect interrupted detach");
    assert!(matches!(
        inspect_leaf(&mut inspector, &InspectHistoryLeaf::new(leaf.clone()))
            .await
            .expect("classify interrupted detach"),
        LeafInspection::DetachInterrupted { .. }
    ));
    drop(inspector);
    sqlx::query("ROLLBACK")
        .execute(&mut *blocker)
        .await
        .expect("release old snapshot");
    drop(blocker);

    let pass =
        prune_expired_partitions(&core_pool_for_test_pool(&database.pool), StatementLog::Off)
            .await
            .expect("a pruning pass");
    assert_eq!(pass.finalized_leaves, vec![leaf.leaf_name().to_owned()]);
    assert_eq!(pass.dropped_count(), 1);
    assert!(pass.errors.is_empty(), "prune errors: {:?}", pass.errors);
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn history_sweep_stops_at_its_leaf_bound_and_reports_truncation() {
    let database = TestDatabase::create_with_connections(4).await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut clock = database.pool.acquire().await.expect("acquire clock");
    let base =
        utc_day_start(database_now(&mut clock).await.expect("database now") - Duration::days(8));
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
        &core_pool_for_test_pool(&database.pool),
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
        &core_pool_for_test_pool(&database.pool),
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
#[serial]
async fn a_refused_leaf_does_not_count_toward_the_leaf_bound() {
    let database = TestDatabase::create_with_connections(4).await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut clock = database.pool.acquire().await.expect("acquire clock");
    let base =
        utc_day_start(database_now(&mut clock).await.expect("database now") - Duration::days(8));
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
    sqlx::query(&format!(
        "SELECT pg_advisory_lock({LEAF_LOCK_KEY_FUNCTION}($1, $2))"
    ))
    .bind(class_key)
    .bind(leaves[0].bounds().lower())
    .execute(&mut *holder)
    .await
    .expect("hold the oldest leaf's lock");

    let sweep = sweep_expired_history_leaves(
        &core_pool_for_test_pool(&database.pool),
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

    let released: bool = sqlx::query_scalar(&format!(
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

#[tokio::test]
#[serial]
async fn pruning_contains_one_leaf_error_and_one_drop_refusal_then_keeps_going() {
    let database = TestDatabase::create_with_connections(4).await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut clock = database
        .pool
        .acquire()
        .await
        .expect("acquire pruning clock");
    let now = database_now(&mut clock).await.expect("database now");
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
    sqlx::query(&format!(
        "ALTER TABLE {parent} DETACH PARTITION {}",
        leaves[1].leaf_name()
    ))
    .execute(&database.pool)
    .await
    .expect("detach the middle leaf out of band");
    let mut lock_holder = database.pool.begin().await.expect("begin leaf lock holder");
    sqlx::query(&format!(
        "SELECT pg_advisory_xact_lock({LEAF_LOCK_KEY_FUNCTION}($1, $2))"
    ))
    .bind(class_key)
    .bind(leaves[1].bounds().lower())
    .execute(lock_holder.as_mut())
    .await
    .expect("hold the middle leaf's lock");
    // The detach record of the oldest leaf fails at the lock timeout.
    let holder = hold_catalog_row(&database.pool, leaves[0].leaf_name()).await;
    let pass =
        prune_expired_partitions(&core_pool_for_test_pool(&database.pool), StatementLog::Off)
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
async fn reader_write_version(pool: &PgPool) -> String {
    sqlx::query_scalar(
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
#[serial]
async fn detach_publishes_changed_history_only_and_finalization_repairs_stale_readers() {
    use super::manager::finalize_interrupted_detach;
    use crate::core::history::commands::FinalizeInterruptedLeafDetach;

    for heartbeat in [true, false] {
        for interrupted in [false, true] {
            let database = TestDatabase::create_with_connections(4).await;
            let parent = register_class(&database.pool, "standard_1d", 1).await;
            let mut tx = database.pool.begin().await.unwrap();
            register_heartbeat_class(&mut tx, hours(3)).await.unwrap();
            let now = database_now(&mut tx).await.unwrap();
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
                    create_hourly_heartbeat_leaf(
                        &mut tx,
                        &CreateHourlyHeartbeatLeaf::new(leaf.clone()).unwrap(),
                    )
                    .await
                    .unwrap();
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
            let core = core_pool_for_test_pool(&database.pool);
            let command = DetachExpiredHistoryLeaf::new(leaf.clone(), Some(100)).unwrap();
            match interrupted {
                true => {
                    let mut blocker = database.pool.acquire().await.unwrap();
                    sqlx::query("BEGIN ISOLATION LEVEL REPEATABLE READ")
                        .execute(&mut *blocker)
                        .await
                        .unwrap();
                    sqlx::query(&format!("SELECT count(*) FROM {}", leaf.leaf_name()))
                        .execute(&mut *blocker)
                        .await
                        .unwrap();
                    let error = detach_expired_leaf(&core, StatementLog::Off, &command)
                        .await
                        .unwrap_err();
                    assert!(error.to_string().contains("statement timeout"));
                    sqlx::query("ROLLBACK")
                        .execute(&mut *blocker)
                        .await
                        .unwrap();
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
#[serial]
async fn finalization_retries_publication_after_detach_committed() {
    use super::manager::finalize_interrupted_detach;
    use crate::core::history::commands::FinalizeInterruptedLeafDetach;

    let database = TestDatabase::create_with_connections(4).await;
    let class = "standard_1d";
    let parent = register_class(&database.pool, class, 1).await;
    let mut connection = database.pool.acquire().await.unwrap();
    let now = database_now(&mut connection).await.unwrap();
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
    sqlx::query(&format!(
        "ALTER TABLE {TASK_LOOKUP_MANIFEST} ADD CONSTRAINT test_reject_publication \
         CHECK (probe_position < 0) NOT VALID"
    ))
    .execute(&database.pool)
    .await
    .unwrap();
    let core = core_pool_for_test_pool(&database.pool);
    let command = DetachExpiredHistoryLeaf::new(leaf.clone(), Some(5_000)).unwrap();
    let error = detach_expired_leaf(&core, StatementLog::Off, &command)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("test_reject_publication"),
        "{error}"
    );
    sqlx::query(&format!(
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

async fn forever_leaves(
    connection: &mut PgConnection,
) -> Vec<(String, LeafLowerBound, chrono::DateTime<Utc>)> {
    let rows: Vec<(
        String,
        String,
        Option<chrono::DateTime<Utc>>,
        chrono::DateTime<Utc>,
    )> = sqlx::query_as(&format!(
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
#[serial]
async fn connection_coverage_verifies_the_static_forever_leaf_and_creates_none() {
    let database = TestDatabase::create().await;
    let mut tx = database.pool.begin().await.unwrap();
    let before = forever_leaves(tx.as_mut()).await;
    assert_eq!(
        before,
        vec![(
            TASK_HISTORY_FOREVER_LEAF.to_owned(),
            LeafLowerBound::Open,
            open_end_anchor()
        )]
    );
    for _ in 0..2 {
        let coverage = ensure_partition_coverage(&mut tx, 3, 2, &[], &StagedLoaderPublisher)
            .await
            .unwrap();
        assert!(
            matches!(coverage, CoverageOutcome::Ensured(_)),
            "{coverage:?}"
        );
        assert_eq!(forever_leaves(tx.as_mut()).await, before);
    }
    tx.rollback().await.unwrap();
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn static_forever_leaf_is_conformant_on_a_fresh_baseline() {
    let database = TestDatabase::create().await;
    let mut connection = database.pool.acquire().await.unwrap();
    let row = read_leaf_catalog_row(&mut connection, TASK_HISTORY_FOREVER_LEAF)
        .await
        .unwrap()
        .expect("the first migration catalogs the static forever leaf");
    assert_eq!(row.lower_anchor, LeafLowerBound::Open);
    assert_eq!(row.upper_anchor, open_end_anchor());
    assert_eq!(
        capture_partition_bound_utc(&mut connection, TASK_HISTORY_FOREVER_LEAF)
            .await
            .unwrap()
            .as_deref(),
        Some(STATIC_FOREVER_LEAF_BOUND)
    );
    assert_eq!(
        verify_static_forever_leaf(&mut connection).await.unwrap(),
        LeafCreation::AlreadyConformant {
            leaf_name: TASK_HISTORY_FOREVER_LEAF.to_owned(),
        }
    );
    drop(connection);
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn static_forever_leaf_missing_index_is_a_conflict_and_is_not_repaired() {
    let database = TestDatabase::create_with_connections(4).await;
    let index_name = leaf_id_index_name(TASK_HISTORY_FOREVER_LEAF);
    sqlx::query(&format!("DROP INDEX {index_name}"))
        .execute(&database.pool)
        .await
        .unwrap();
    let mut connection = database.pool.acquire().await.unwrap();
    assert!(matches!(
        verify_static_forever_leaf(&mut connection).await.unwrap(),
        LeafCreation::CatalogConflict {
            kind: CatalogConflictKind::PhysicalNonconformant,
            ..
        }
    ));
    drop(connection);
    let outcome = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_pool(&database.pool),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .unwrap();
    assert!(matches!(outcome, CoverageOutcome::Failed(_)), "{outcome:?}");
    let restored: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(&index_name)
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert!(!restored);
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn static_forever_leaf_missing_catalog_row_is_a_conflict() {
    let database = TestDatabase::create().await;
    let mut tx = database.pool.begin().await.unwrap();
    sqlx::query(&format!("DELETE FROM {LEAF_CATALOG} WHERE leaf_name = $1"))
        .bind(TASK_HISTORY_FOREVER_LEAF)
        .execute(tx.as_mut())
        .await
        .unwrap();
    assert!(matches!(
        verify_static_forever_leaf(tx.as_mut()).await.unwrap(),
        LeafCreation::CatalogConflict {
            kind: CatalogConflictKind::RelationWithoutCatalog,
            ..
        }
    ));
    tx.rollback().await.unwrap();
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn daily_forever_leaf_inside_the_open_ended_leaf_is_refused_without_change() {
    let database = TestDatabase::create().await;
    let mut tx = database.pool.begin().await.unwrap();
    let coverage = ensure_partition_coverage(&mut tx, 2, 2, &[], &StagedLoaderPublisher)
        .await
        .unwrap();
    assert!(
        matches!(coverage, CoverageOutcome::Ensured(_)),
        "{coverage:?}"
    );
    let before = forever_leaves(tx.as_mut()).await;
    let open = before
        .iter()
        .find(|(_, _, upper)| *upper == open_end_anchor())
        .expect("open-ended leaf")
        .clone();
    let today = utc_day_start(database_now(tx.as_mut()).await.unwrap());
    let lower = today + Duration::days(10);
    let leaf = LeafRef::new(
        daily_leaf_name(TASK_HISTORY_FOREVER, lower).unwrap(),
        FOREVER_CLASS_KEY,
        LeafBounds::new(lower, lower + Duration::days(1)).unwrap(),
    )
    .unwrap();
    let outcome = create_daily_leaf(
        tx.as_mut(),
        &CreateDailyHistoryLeaf::new(leaf.clone()).unwrap(),
        &StagedLoaderPublisher,
    )
    .await
    .unwrap();
    assert_eq!(
        outcome,
        LeafCreation::CoveredByOpenEndedLeaf {
            leaf_name: leaf.leaf_name().to_owned(),
            open_leaf_name: open.0.clone(),
        }
    );
    assert_eq!(forever_leaves(tx.as_mut()).await, before);
    let relation: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(leaf.leaf_name())
        .fetch_one(tx.as_mut())
        .await
        .unwrap();
    assert_eq!(relation, None);
    tx.rollback().await.unwrap();
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn pool_coverage_creates_the_period_leaves_and_no_forever_leaf() {
    let database = TestDatabase::create_with_connections(4).await;
    let mut reader = database.pool.acquire().await.unwrap();
    let before = forever_leaves(&mut reader).await;
    drop(reader);
    let first = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_pool(&database.pool),
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
        &crate::broker::pools::core_pool_for_test_pool(&database.pool),
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

fn utc_day_start(value: chrono::DateTime<Utc>) -> chrono::DateTime<Utc> {
    value.date_naive().and_hms_opt(0, 0, 0).unwrap().and_utc()
}

#[tokio::test]
#[serial]
async fn guard_probe_set_is_standard_horizon_plus_the_static_forever_leaf() {
    let database = TestDatabase::create().await;
    let mut tx = database.pool.begin().await.unwrap();
    let coverage = ensure_partition_coverage(&mut tx, 3, 2, &[], &StagedLoaderPublisher)
        .await
        .unwrap();
    assert!(
        matches!(coverage, CoverageOutcome::Ensured(_)),
        "{coverage:?}"
    );
    // The guard probes the manifest leaves whose upper bound is after the id's
    // birth: for an id born now, the leaves that end after now.
    let probed: Vec<(String, String)> = sqlx::query_as(&format!(
        "SELECT catalog.class_key, manifest.leaf_name
         FROM {TASK_LOOKUP_MANIFEST} AS manifest
         JOIN {LEAF_CATALOG} AS catalog USING (leaf_name)
         WHERE manifest.upper_anchor > statement_timestamp()
         ORDER BY 1, 2"
    ))
    .fetch_all(tx.as_mut())
    .await
    .unwrap();
    let standard = probed
        .iter()
        .filter(|(class, _)| class == "standard_30d")
        .count();
    let forever: Vec<&str> = probed
        .iter()
        .filter(|(class, _)| class == "forever")
        .map(|(_, name)| name.as_str())
        .collect();
    assert_eq!(standard, 4);
    // The static forever leaf only.
    assert_eq!(forever, vec![TASK_HISTORY_FOREVER_LEAF]);
    tx.rollback().await.unwrap();
    database.drop().await;
}

mod coverage_ports;
