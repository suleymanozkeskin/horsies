use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{Duration, Timelike, Utc};
use serial_test::serial;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use sqlx::{Connection, PgConnection, PgPool};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::broker::migrations::run_horsies_migrations;
use crate::core::history::commands::{
    CollectPartitionHealth, CreateDailyHistoryLeaf, DetachExpiredHistoryLeaf,
    DropDetachedHistoryLeaf, EnsureLeafCoverage, InspectHistoryLeaf, LeafBounds, LeafRef,
};
use crate::core::history::cutover::relocation::{relocate_terminal_batch, RelocationOutcome};
use crate::core::history::ddl::classes::{
    finite_class_parent_name, register_finite_retention_class, ClassRegistration,
};
use crate::core::history::ddl::runtime_names::{
    daily_leaf_name, leaf_enqueued_index_name, leaf_id_index_name, render_daily_leaf_ddl,
};
use crate::core::history::heartbeats::partitioning::{
    create_hourly_heartbeat_leaf, ensure_heartbeat_coverage, heartbeat_horizon, hourly_leaf_name,
    hourly_leaf_ref, probe_index_name, register_heartbeat_class, sweep_expired_heartbeat_leaves,
    CreateHourlyHeartbeatLeaf, EnsureHeartbeatCoverage, HeartbeatClassRegistration,
};
use crate::core::history::maintenance::coverage::{
    ensure_partition_coverage, ensure_partition_coverage_in_pool, ensure_startup_coverage,
    ensure_startup_coverage_in_pool, CoverageOutcome, DeclaredRetentionClass,
    StartupCoverageOutcome,
};
use crate::core::history::maintenance::gate::{
    active_maintenance_session, begin_archive_maintenance, finish_archive_maintenance,
    MaintenanceSessionError, ARCHIVE_AVAILABILITY_FUNCTION,
};
use crate::core::history::maintenance::pruning::prune_expired_partitions;
use crate::core::history::names::{
    HEARTBEATS_TABLE, HEARTBEAT_CLASS_KEY, LEAF_CATALOG, LEAF_LOCK_KEY_FUNCTION, RETENTION_CLASSES,
    TASK_HISTORY_FOREVER, TASK_HISTORY_PARENT, TASK_LOOKUP_MANIFEST,
};
use crate::core::history::outcomes::{
    CatalogConflictKind, HealthFault, LeafCreation, LeafDrop, LeafInspection,
};
use crate::core::history::reads::publisher::StagedLoaderPublisher;

use super::catalog::{
    capture_partition_bound_utc, database_now, read_attached_birth_floor, read_leaf_catalog_row,
    read_leaf_physical_state, read_manifest_leaf_rows, LeafIndexKind,
    LeafPartitionBoundExpectation,
};
use super::forever::{ensure_forever_range_partitioning, FOREVER_LEGACY_LEAF};
use super::health::collect_partition_health;
use super::manager::{
    create_daily_leaf, detach_expired_leaf, drop_detached_leaf, ensure_leaf_coverage, inspect_leaf,
    DetachExpiredLeafOutcome, LeafBlockerQuarantine, NoQuarantine, QuarantineRefusalVerdict,
    QuarantineRefused, QuarantineResult, TaskQuarantineRefusal,
};
use super::publication::{LoaderPublication, LoaderRepublished, UnpublishedLoader};

#[derive(Debug, Default)]
struct CatalogPublisher;

impl LoaderPublication for CatalogPublisher {
    async fn republish(
        &self,
        connection: &mut PgConnection,
    ) -> Result<LoaderRepublished, crate::core::history::errors::HistoryError> {
        let selection = read_manifest_leaf_rows(connection).await?;
        Ok(LoaderRepublished {
            absent_leaves: selection.absent_relations,
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
        Ok(!read_manifest_leaf_rows(connection)
            .await?
            .absent_relations
            .is_empty())
    }
}

#[derive(Debug, Default)]
struct ReferencingPublisher;

impl LoaderPublication for ReferencingPublisher {
    async fn republish(
        &self,
        _connection: &mut PgConnection,
    ) -> Result<LoaderRepublished, crate::core::history::errors::HistoryError> {
        Ok(LoaderRepublished {
            absent_leaves: Vec::new(),
        })
    }

    async fn references_leaf(
        &self,
        _connection: &mut PgConnection,
        _leaf_name: &str,
    ) -> Result<bool, crate::core::history::errors::HistoryError> {
        Ok(true)
    }
}

#[derive(Debug, Default)]
struct FailFirstRepublish {
    calls: AtomicUsize,
    reference_calls: AtomicUsize,
}

#[derive(Debug, Default)]
struct FailFirstStagedPublisher {
    calls: AtomicUsize,
}

#[derive(Debug)]
struct BlockingPublisher {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl LoaderPublication for BlockingPublisher {
    async fn republish(
        &self,
        _connection: &mut PgConnection,
    ) -> Result<LoaderRepublished, crate::core::history::errors::HistoryError> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(LoaderRepublished {
            absent_leaves: Vec::new(),
        })
    }

    async fn references_leaf(
        &self,
        _connection: &mut PgConnection,
        _leaf_name: &str,
    ) -> Result<bool, crate::core::history::errors::HistoryError> {
        Ok(false)
    }
}

impl LoaderPublication for FailFirstRepublish {
    async fn republish(
        &self,
        _connection: &mut PgConnection,
    ) -> Result<LoaderRepublished, crate::core::history::errors::HistoryError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(crate::core::history::errors::HistoryError::contract(
                "injected first-leaf publication failure",
            ));
        }
        Ok(LoaderRepublished {
            absent_leaves: Vec::new(),
        })
    }

    async fn references_leaf(
        &self,
        _connection: &mut PgConnection,
        _leaf_name: &str,
    ) -> Result<bool, crate::core::history::errors::HistoryError> {
        Ok(self.reference_calls.fetch_add(1, Ordering::SeqCst) == 0)
    }
}

impl LoaderPublication for FailFirstStagedPublisher {
    async fn republish(
        &self,
        connection: &mut PgConnection,
    ) -> Result<LoaderRepublished, crate::core::history::errors::HistoryError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(crate::core::history::errors::HistoryError::contract(
                "injected post-commit publication failure",
            ));
        }
        StagedLoaderPublisher.republish(connection).await
    }

    async fn references_leaf(
        &self,
        connection: &mut PgConnection,
        leaf_name: &str,
    ) -> Result<bool, crate::core::history::errors::HistoryError> {
        StagedLoaderPublisher
            .references_leaf(connection, leaf_name)
            .await
    }

    async fn needs_republication(
        &self,
        connection: &mut PgConnection,
    ) -> Result<bool, crate::core::history::errors::HistoryError> {
        StagedLoaderPublisher.needs_republication(connection).await
    }
}

#[derive(Debug, Clone)]
struct RefusingQuarantine {
    task_id: Uuid,
}

impl LeafBlockerQuarantine for RefusingQuarantine {
    async fn quarantine(
        &self,
        _connection: &mut PgConnection,
        leaf: &LeafRef,
        _horizon: Duration,
    ) -> Result<QuarantineResult, crate::core::history::errors::HistoryError> {
        Ok(QuarantineResult::Refused(QuarantineRefused {
            leaf_name: leaf.leaf_name().to_owned(),
            repointed: 0,
            refusals: vec![TaskQuarantineRefusal {
                task_id: self.task_id,
                verdict: QuarantineRefusalVerdict::SourceAbsent,
                detail: Some("history row absent at locator".to_owned()),
            }],
        }))
    }
}

struct TestDatabase {
    pool: PgPool,
    database_name: String,
    admin_options: PgConnectOptions,
}

impl TestDatabase {
    async fn create() -> Self {
        Self::create_with_connections(1).await
    }

    async fn create_with_connections(max_connections: u32) -> Self {
        let base = database_url();
        let base_options = PgConnectOptions::from_str(&base).expect("invalid test database URL");
        let admin_options = base_options.clone().database("postgres");
        let database_name = format!("horsies_p3_{}", Uuid::new_v4().simple());
        let mut admin = PgConnection::connect_with(&admin_options)
            .await
            .expect("connect to postgres admin database");
        sqlx::query(&format!("CREATE DATABASE \"{database_name}\""))
            .execute(&mut admin)
            .await
            .expect("create P3 test database");
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect_with(base_options.database(&database_name))
            .await
            .expect("connect to P3 test database");
        run_horsies_migrations(&pool)
            .await
            .expect("migrate P3 test database");
        Self {
            pool,
            database_name,
            admin_options,
        }
    }

    async fn drop(self) {
        self.pool.close().await;
        let mut admin = PgConnection::connect_with(&self.admin_options)
            .await
            .expect("connect for P3 database cleanup");
        sqlx::query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
             WHERE datname = $1 AND pid <> pg_backend_pid()",
        )
        .bind(&self.database_name)
        .execute(&mut admin)
        .await
        .expect("terminate P3 test database sessions");
        sqlx::query(&format!(
            "DROP DATABASE IF EXISTS \"{}\"",
            self.database_name
        ))
        .execute(&mut admin)
        .await
        .expect("drop P3 test database");
    }
}

fn database_url() -> String {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return url;
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root");
    let contents = std::fs::read_to_string(root.join(".env")).expect("read workspace .env");
    let password = contents
        .lines()
        .filter_map(|line| line.split_once('='))
        .find_map(|(key, value)| (key.trim() == "DB_PASSWORD").then(|| value.trim()))
        .expect("DB_PASSWORD in workspace .env");
    format!("postgresql://postgres:{password}@localhost:5432/horsies-rust-port")
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
    let backend = PgConnectOptions::from_str(&database_url())
        .expect("invalid test database URL")
        .database(&database.database_name)
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
    let backend = PgConnectOptions::from_str(&database_url())
        .expect("invalid test database URL")
        .database(&database.database_name)
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

async fn register_class(pool: &PgPool, class_key: &str, days: i64) -> String {
    let mut transaction = pool.begin().await.expect("begin class registration");
    let outcome =
        register_finite_retention_class(&mut transaction, class_key, Duration::days(days))
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

fn leaf_ref(parent: &str, class_key: &str, lower: chrono::DateTime<Utc>) -> LeafRef {
    LeafRef::new(
        daily_leaf_name(parent, lower).expect("daily leaf name"),
        class_key,
        LeafBounds::new(lower, lower + Duration::days(1)).expect("daily bounds"),
    )
    .expect("daily leaf ref")
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
    let class_key = "p3_lifecycle_30d";
    let parent = register_class(&database.pool, class_key, 30).await;
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
    let cataloged_bound = read_leaf_catalog_row(&mut transaction, leaf.leaf_name())
        .await
        .expect("read cataloged leaf")
        .expect("cataloged current leaf")
        .partition_bound;
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
            Some(cataloged_bound.as_str())
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
    sqlx::query(&format!(
        "UPDATE {LEAF_CATALOG} SET partition_bound = 'wrong bound' WHERE leaf_name = $1"
    ))
    .bind(leaf.leaf_name())
    .execute(&mut *transaction)
    .await
    .expect("corrupt cataloged partition bound");
    assert!(matches!(
        create_daily_leaf(
            &mut transaction,
            &CreateDailyHistoryLeaf::new(leaf.clone()).expect("conflict command"),
            &UnpublishedLoader,
        )
        .await
        .expect("classify physical mismatch"),
        LeafCreation::CatalogConflict {
            kind: crate::core::history::outcomes::CatalogConflictKind::PhysicalNonconformant,
            ..
        }
    ));
    sqlx::query(&format!(
        "UPDATE {LEAF_CATALOG} SET partition_bound = $1 WHERE leaf_name = $2"
    ))
    .bind(&cataloged_bound)
    .bind(leaf.leaf_name())
    .execute(&mut *transaction)
    .await
    .expect("restore cataloged partition bound");
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
    let report = collect_partition_health(
        &mut transaction,
        &CollectPartitionHealth::new(class_key, true).expect("health command"),
    )
    .await
    .expect("collect health");
    assert!(report.is_healthy(), "health faults: {:?}", report.faults);
    assert!(
        report
            .coverage
            .expect("class coverage")
            .complete_future_intervals
            >= 2
    );
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
    let class_key = "p3_nonblocking_30d";
    let parent = register_class(&database.pool, class_key, 30).await;
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

#[tokio::test]
#[serial]
async fn detach_returns_busy_without_waiting_for_a_leaf_session_lock() {
    let database = TestDatabase::create_with_connections(3).await;
    let class_key = "p3_nb_detach";
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
    let command =
        DetachExpiredHistoryLeaf::new(leaf.clone(), None, Some(5_000)).expect("detach command");
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        detach_expired_leaf(&database.pool, &command, &UnpublishedLoader, &NoQuarantine),
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
        detach_expired_leaf(&database.pool, &command, &UnpublishedLoader, &NoQuarantine),
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

#[tokio::test]
#[serial]
async fn cancelled_detach_closes_its_connection_and_releases_the_session_lock() {
    let database = TestDatabase::create_with_connections(3).await;
    let class_key = "p3_cancel_detach";
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
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let publisher = BlockingPublisher {
        entered: Arc::clone(&entered),
        release,
    };
    let command =
        DetachExpiredHistoryLeaf::new(leaf.clone(), None, Some(5_000)).expect("detach command");
    let pool = database.pool.clone();
    let detach = tokio::spawn(async move {
        detach_expired_leaf(&pool, &command, &publisher, &NoQuarantine).await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
        .await
        .expect("detach reached publication while holding the lock");
    detach.abort();
    assert!(detach
        .await
        .expect_err("detach must be cancelled")
        .is_cancelled());

    let try_sql = format!("SELECT pg_try_advisory_lock({LEAF_LOCK_KEY_FUNCTION}($1, $2))");
    let unlock_sql = format!("SELECT pg_advisory_unlock({LEAF_LOCK_KEY_FUNCTION}($1, $2))");
    let mut probe = database.pool.acquire().await.expect("acquire lock probe");
    let mut acquired = false;
    for _ in 0..20 {
        acquired = sqlx::query_scalar(&try_sql)
            .bind(class_key)
            .bind(lower)
            .fetch_one(&mut *probe)
            .await
            .expect("try cancelled detach lock");
        if acquired {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
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
    let outcome = ensure_partition_coverage_in_pool(&database.pool, 2, 2, &[], &UnpublishedLoader)
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
    let declared: Vec<DeclaredRetentionClass> = (0..50)
        .map(|index| DeclaredRetentionClass {
            class_key: format!("rtt_class_{index:02}"),
            duration: Duration::days(7 + i64::from(index)),
        })
        .collect();

    let first = ensure_partition_coverage_in_pool(
        &database.pool,
        8,
        8,
        &declared[..1],
        &StagedLoaderPublisher,
    )
    .await
    .expect("create initial coverage");
    assert!(matches!(first, CoverageOutcome::Ensured(_)));

    for (history_horizon, heartbeat_horizon) in [(2, 2), (8, 8)] {
        let setup = ensure_partition_coverage_in_pool(
            &database.pool,
            history_horizon,
            heartbeat_horizon,
            &declared[..1],
            &StagedLoaderPublisher,
        )
        .await
        .expect("set healthy horizon coverage");
        assert!(matches!(setup, CoverageOutcome::Ensured(_)));
        let (proxy, pool) = proxied_pool(&database, 0, 1).await;
        sqlx::query("SELECT set_config('timezone', 'America/Los_Angeles', false)")
            .execute(&pool)
            .await
            .expect("set proxied session timezone");
        proxy.reset();
        let outcome = ensure_partition_coverage_in_pool(
            &pool,
            history_horizon,
            heartbeat_horizon,
            &declared[..1],
            &StagedLoaderPublisher,
        )
        .await
        .expect("healthy horizon coverage");
        assert!(matches!(outcome, CoverageOutcome::Ensured(_)));
        assert_eq!(proxy.statement_count(), 3);
        assert!(!proxy.sql().iter().any(|statement| {
            statement.starts_with("BEGIN") || statement.contains("pg_try_advisory_xact_lock")
        }));
        let timezone: String = sqlx::query_scalar("SHOW timezone")
            .fetch_one(&pool)
            .await
            .expect("read proxied session timezone");
        assert_eq!(timezone, "America/Los_Angeles");
        pool.close().await;
        proxy.stop().await;
    }

    for class_count in [10, 50] {
        let setup = ensure_partition_coverage_in_pool(
            &database.pool,
            2,
            2,
            &declared[..class_count],
            &StagedLoaderPublisher,
        )
        .await
        .expect("extend class coverage");
        assert!(matches!(setup, CoverageOutcome::Ensured(_)));

        let (proxy, pool) = proxied_pool(&database, 0, 1).await;
        let outcome = ensure_partition_coverage_in_pool(
            &pool,
            2,
            2,
            &declared[..class_count],
            &StagedLoaderPublisher,
        )
        .await
        .expect("healthy class coverage");
        assert!(matches!(outcome, CoverageOutcome::Ensured(_)));
        assert_eq!(proxy.statement_count(), 3);
        pool.close().await;
        proxy.stop().await;
    }

    let delay_ms = 20_usize;
    let (proxy, pool) = proxied_pool(&database, delay_ms, 1).await;
    let started = tokio::time::Instant::now();
    let outcome = ensure_partition_coverage_in_pool(&pool, 2, 2, &declared, &StagedLoaderPublisher)
        .await
        .expect("healthy high-RTT coverage");
    let elapsed = started.elapsed();
    assert!(matches!(outcome, CoverageOutcome::Ensured(_)));
    assert_eq!(proxy.statement_count(), 3);
    assert!(elapsed >= std::time::Duration::from_millis((delay_ms * 3) as u64));
    assert!(elapsed < std::time::Duration::from_millis((delay_ms * 3 + 500) as u64));
    pool.close().await;
    proxy.stop().await;
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn failed_post_commit_publication_is_retried_by_the_next_healthy_pass() {
    let database = TestDatabase::create_with_connections(4).await;
    let setup =
        ensure_partition_coverage_in_pool(&database.pool, 2, 2, &[], &StagedLoaderPublisher)
            .await
            .expect("create and publish initial coverage");
    assert!(matches!(setup, CoverageOutcome::Ensured(_)));

    let publisher = FailFirstStagedPublisher::default();
    let error = ensure_partition_coverage_in_pool(&database.pool, 3, 3, &[], &publisher)
        .await
        .expect_err("inject final publication failure");
    assert!(error
        .to_string()
        .contains("injected post-commit publication failure"));
    assert_eq!(publisher.calls.load(Ordering::SeqCst), 1);

    let unpublished: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)
         FROM {LEAF_CATALOG} AS catalog
         WHERE catalog.detached_at IS NULL
           AND catalog.dropped_at IS NULL
           AND catalog.class_key <> $1
           AND to_regclass(catalog.leaf_name) IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM {TASK_LOOKUP_MANIFEST} AS manifest
               WHERE manifest.leaf_name = catalog.leaf_name
           )"
    ))
    .bind(HEARTBEAT_CLASS_KEY)
    .fetch_one(&database.pool)
    .await
    .expect("count committed unpublished leaves");
    assert!(unpublished > 0);

    let retry = ensure_partition_coverage_in_pool(&database.pool, 3, 3, &[], &publisher)
        .await
        .expect("retry publication on a healthy coverage pass");
    let CoverageOutcome::Ensured(retry) = retry else {
        panic!("the next healthy pass must publish the committed leaves");
    };
    assert!(retry.republished);
    assert_eq!(publisher.calls.load(Ordering::SeqCst), 2);
    let unpublished: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)
         FROM {LEAF_CATALOG} AS catalog
         WHERE catalog.detached_at IS NULL
           AND catalog.dropped_at IS NULL
           AND catalog.class_key <> $1
           AND to_regclass(catalog.leaf_name) IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM {TASK_LOOKUP_MANIFEST} AS manifest
               WHERE manifest.leaf_name = catalog.leaf_name
           )"
    ))
    .bind(HEARTBEAT_CLASS_KEY)
    .fetch_one(&database.pool)
    .await
    .expect("verify all attached leaves are published");
    assert_eq!(unpublished, 0);
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn current_heartbeat_with_a_wrong_physical_range_refuses_startup() {
    let database = TestDatabase::create_with_connections(4).await;
    let setup = ensure_partition_coverage_in_pool(&database.pool, 2, 2, &[], &UnpublishedLoader)
        .await
        .expect("create heartbeat coverage");
    assert!(matches!(setup, CoverageOutcome::Ensured(_)));

    let (leaf_name, index_name, lower_anchor, upper_anchor): (
        String,
        String,
        chrono::DateTime<Utc>,
        chrono::DateTime<Utc>,
    ) = sqlx::query_as(&format!(
        "SELECT leaf_name, id_index_name, lower_anchor, upper_anchor
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
    let mut connection = database.pool.acquire().await.expect("acquire bound reader");
    let wrong_bound = capture_partition_bound_utc(&mut connection, &leaf_name)
        .await
        .expect("capture wrong physical bound")
        .expect("wrong physical bound exists");
    drop(connection);
    sqlx::query(&format!(
        "UPDATE {LEAF_CATALOG} SET partition_bound = $1 WHERE leaf_name = $2"
    ))
    .bind(wrong_bound)
    .bind(&leaf_name)
    .execute(&database.pool)
    .await
    .expect("make stored bound match the wrong physical bound");

    let startup = ensure_startup_coverage_in_pool(&database.pool, 2, 2, &[], &UnpublishedLoader)
        .await
        .expect("validate startup with a wrong current heartbeat range");
    let StartupCoverageOutcome::Refused(CoverageOutcome::Failed(failure)) = startup else {
        panic!("a wrong current-heartbeat range must refuse startup");
    };
    assert!(!failure.heartbeat_covered_now);
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn coverage_repairs_same_name_wrong_shape_and_invalid_indexes() {
    let database = TestDatabase::create_with_connections(4).await;
    let setup = ensure_partition_coverage_in_pool(&database.pool, 2, 2, &[], &UnpublishedLoader)
        .await
        .expect("create index-repair coverage");
    assert!(matches!(setup, CoverageOutcome::Ensured(_)));

    let heartbeat = sqlx::query_as::<_, super::catalog::LeafCatalogRow>(&format!(
        "SELECT leaf_name, parent_name, class_key, lower_anchor, upper_anchor,
                index_schema_version, id_index_name, partition_bound, min_birth_at,
                min_birth_verified, created_at, detached_at, dropped_at
         FROM {LEAF_CATALOG}
         WHERE class_key = $1
           AND lower_anchor <= statement_timestamp()
           AND upper_anchor > statement_timestamp()"
    ))
    .bind(HEARTBEAT_CLASS_KEY)
    .fetch_one(&database.pool)
    .await
    .expect("read current heartbeat catalog row");
    sqlx::query(&format!("DROP INDEX {}", heartbeat.id_index_name))
        .execute(&database.pool)
        .await
        .expect("drop heartbeat index");
    sqlx::query(&format!(
        "CREATE INDEX {} ON {} (task_id, role, sent_at ASC)",
        heartbeat.id_index_name, heartbeat.leaf_name
    ))
    .execute(&database.pool)
    .await
    .expect("create same-name heartbeat index with wrong sort direction");

    let history = sqlx::query_as::<_, super::catalog::LeafCatalogRow>(&format!(
        "SELECT leaf_name, parent_name, class_key, lower_anchor, upper_anchor,
                index_schema_version, id_index_name, partition_bound, min_birth_at,
                min_birth_verified, created_at, detached_at, dropped_at
         FROM {LEAF_CATALOG}
         WHERE class_key <> $1
           AND detached_at IS NULL
           AND dropped_at IS NULL
         ORDER BY lower_anchor, leaf_name
         LIMIT 1"
    ))
    .bind(HEARTBEAT_CLASS_KEY)
    .fetch_one(&database.pool)
    .await
    .expect("read history catalog row");
    sqlx::query("UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass($1)")
        .bind(&history.id_index_name)
        .execute(&database.pool)
        .await
        .expect("mark history index invalid");

    let outcome = ensure_partition_coverage_in_pool(&database.pool, 2, 2, &[], &UnpublishedLoader)
        .await
        .expect("repair malformed indexes");
    assert!(matches!(outcome, CoverageOutcome::Ensured(_)));
    let heartbeat_bounds =
        LeafBounds::new(heartbeat.lower_anchor, heartbeat.upper_anchor).expect("heartbeat bounds");
    let history_bounds =
        LeafBounds::new(history.lower_anchor, history.upper_anchor).expect("history bounds");
    let mut connection = database.pool.acquire().await.expect("acquire index reader");
    assert!(
        read_leaf_physical_state(
            &mut connection,
            &heartbeat.leaf_name,
            &heartbeat.parent_name,
            &heartbeat.id_index_name,
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
            &history.parent_name,
            &history.id_index_name,
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
    let setup = ensure_partition_coverage_in_pool(&database.pool, 2, 2, &[], &UnpublishedLoader)
        .await
        .expect("create operator-class repair coverage");
    assert!(matches!(setup, CoverageOutcome::Ensured(_)));
    let heartbeat = sqlx::query_as::<_, super::catalog::LeafCatalogRow>(&format!(
        "SELECT leaf_name, parent_name, class_key, lower_anchor, upper_anchor,
                index_schema_version, id_index_name, partition_bound, min_birth_at,
                min_birth_verified, created_at, detached_at, dropped_at
         FROM {LEAF_CATALOG}
         WHERE class_key = $1
           AND lower_anchor <= statement_timestamp()
           AND upper_anchor > statement_timestamp()"
    ))
    .bind(HEARTBEAT_CLASS_KEY)
    .fetch_one(&database.pool)
    .await
    .expect("read current heartbeat catalog row");
    sqlx::query(&format!("DROP INDEX {}", heartbeat.id_index_name))
        .execute(&database.pool)
        .await
        .expect("drop canonical heartbeat index");
    sqlx::query(&format!(
        "CREATE INDEX {} ON {} (
             task_id, role varchar_pattern_ops, sent_at DESC
         )",
        heartbeat.id_index_name, heartbeat.leaf_name
    ))
    .execute(&database.pool)
    .await
    .expect("create heartbeat index with a nondefault operator class");
    let index_definition: String = sqlx::query_scalar("SELECT pg_get_indexdef(to_regclass($1))")
        .bind(&heartbeat.id_index_name)
        .fetch_one(&database.pool)
        .await
        .expect("read nondefault heartbeat index definition");
    assert!(index_definition.contains("varchar_pattern_ops"));

    let outcome = ensure_partition_coverage_in_pool(&database.pool, 2, 2, &[], &UnpublishedLoader)
        .await
        .expect("repair nondefault heartbeat operator class");
    assert!(matches!(outcome, CoverageOutcome::Ensured(_)));
    let index_definition: String = sqlx::query_scalar("SELECT pg_get_indexdef(to_regclass($1))")
        .bind(&heartbeat.id_index_name)
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
    let class_key = "p3_index_reuse_30d";
    let parent = register_class(&database.pool, class_key, 30).await;
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
async fn pool_coverage_repairs_only_the_damaged_leaf_in_one_mutation_transaction() {
    let database = TestDatabase::create_with_connections(4).await;
    let setup = ensure_partition_coverage_in_pool(&database.pool, 2, 2, &[], &UnpublishedLoader)
        .await
        .expect("create repair coverage");
    assert!(matches!(setup, CoverageOutcome::Ensured(_)));
    let damaged_index: String = sqlx::query_scalar(&format!(
        "SELECT id_index_name FROM {LEAF_CATALOG}
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

    let (proxy, pool) = proxied_pool(&database, 0, 4).await;
    let outcome = ensure_partition_coverage_in_pool(&pool, 2, 2, &[], &UnpublishedLoader)
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
    assert_eq!(
        sql.iter()
            .filter(|statement| {
                statement.contains("pg_try_advisory_xact_lock(horsies_task_history_leaf_lock_key")
            })
            .count(),
        1
    );
    assert_eq!(
        sql.iter()
            .filter(|statement| statement.contains(&format!("CREATE INDEX {damaged_index}")))
            .count(),
        1
    );
    pool.close().await;
    proxy.stop().await;
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn busy_coverage_gate_keeps_startup_ready_when_current_heartbeat_exists() {
    let database = TestDatabase::create_with_connections(4).await;
    let setup = ensure_partition_coverage_in_pool(&database.pool, 2, 2, &[], &UnpublishedLoader)
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

    let startup = ensure_startup_coverage_in_pool(&database.pool, 3, 3, &[], &UnpublishedLoader)
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

    let startup = ensure_startup_coverage_in_pool(&database.pool, 2, 2, &[], &UnpublishedLoader)
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
    let class_key = "p3_retire_1d";
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
    sqlx::query("INSERT INTO horsies_workflows (id, name) VALUES ($1, 'p3 blocker')")
        .bind(workflow_id)
        .execute(&mut *transaction)
        .await
        .expect("insert blocker workflow");
    sqlx::query(
        "INSERT INTO horsies_workflow_tasks
             (id, workflow_id, task_index, task_name)
         VALUES ($1, $2, 0, 'p3 blocker node')",
    )
    .bind(node_row_id)
    .bind(workflow_id)
    .execute(&mut *transaction)
    .await
    .expect("insert blocker workflow node");
    sqlx::query(
        "INSERT INTO horsies_workflow_phase2_pending (
             task_id, workflow_id, workflow_node_row_id, terminal_status, terminal_at,
             terminalization_kind, recovery_source, history_class, history_anchor,
             history_schema_version, result_digest, phase2_generation, created_at,
             attempt_count
         ) VALUES ($1, $2, $3, 'COMPLETED', $4, 'COMPLETE_FUSED', 'HISTORY',
                   $5, $4, 1, $6, $7, statement_timestamp(), 0)",
    )
    .bind(pending_task_id)
    .bind(workflow_id)
    .bind(node_row_id)
    .bind(lower + Duration::hours(1))
    .bind(class_key)
    .bind(vec![7_u8; 32])
    .bind(Uuid::new_v4())
    .execute(&mut *transaction)
    .await
    .expect("insert pending blocker");
    transaction.commit().await.expect("commit blocker");
    let quarantine_detach =
        DetachExpiredHistoryLeaf::new(leaf.clone(), Some(Duration::hours(1)), Some(5_000))
            .expect("quarantine detach command");
    let quarantine_refusal = detach_expired_leaf(
        &database.pool,
        &quarantine_detach,
        &UnpublishedLoader,
        &RefusingQuarantine {
            task_id: pending_task_id,
        },
    )
    .await
    .expect("typed quarantine refusal");
    match quarantine_refusal {
        DetachExpiredLeafOutcome::QuarantineRefused(refusal) => {
            assert_eq!(refusal.leaf_name, leaf.leaf_name());
            assert_eq!(refusal.repointed, 0);
            assert_eq!(refusal.refusals.len(), 1);
            assert_eq!(refusal.refusals[0].task_id, pending_task_id);
            assert_eq!(
                refusal.refusals[0].verdict,
                QuarantineRefusalVerdict::SourceAbsent
            );
            assert!(refusal.refusals[0].detail.is_some());
        }
        other => panic!("expected task-level quarantine refusal, got {other:?}"),
    }
    let detach =
        DetachExpiredHistoryLeaf::new(leaf.clone(), None, Some(5_000)).expect("detach command");
    let blocked = detach_expired_leaf(&database.pool, &detach, &UnpublishedLoader, &NoQuarantine)
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
    sqlx::query("SELECT set_config('statement_timeout', '17s', false)")
        .execute(&database.pool)
        .await
        .expect("set prior timeout");
    sqlx::query("SELECT set_config('lock_timeout', '3s', false)")
        .execute(&database.pool)
        .await
        .expect("set prior lock timeout");
    let detached = detach_expired_leaf(&database.pool, &detach, &UnpublishedLoader, &NoQuarantine)
        .await
        .expect("detach expired leaf");
    assert!(matches!(
        detached,
        DetachExpiredLeafOutcome::Inspection(LeafInspection::Detached { .. })
    ));
    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&database.pool)
        .await
        .expect("read restored timeout");
    assert_eq!(timeout, "17s");
    let lock_timeout: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(&database.pool)
        .await
        .expect("read restored lock timeout");
    assert_eq!(lock_timeout, "3s");
    let mut transaction = database.pool.begin().await.expect("begin drop");
    let dropped = drop_detached_leaf(
        &mut transaction,
        &DropDetachedHistoryLeaf::new(leaf.clone()),
        &UnpublishedLoader,
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
    let referenced_detach =
        DetachExpiredHistoryLeaf::new(referenced_leaf.clone(), None, Some(5_000))
            .expect("referenced detach command");
    assert!(matches!(
        detach_expired_leaf(
            &database.pool,
            &referenced_detach,
            &UnpublishedLoader,
            &NoQuarantine,
        )
        .await
        .expect("detach referenced leaf"),
        DetachExpiredLeafOutcome::Inspection(LeafInspection::Detached { .. })
    ));
    let mut transaction = database.pool.begin().await.expect("begin refused drop");
    let refused = drop_detached_leaf(
        &mut transaction,
        &DropDetachedHistoryLeaf::new(referenced_leaf.clone()),
        &ReferencingPublisher,
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
async fn heartbeat_registration_coverage_and_expiry_sweep_share_the_catalog() {
    let database = TestDatabase::create().await;
    let horizon = Duration::hours(2);
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
        register_heartbeat_class(&mut transaction, Duration::hours(3))
            .await
            .expect("update heartbeat horizon"),
        HeartbeatClassRegistration::HorizonUpdated {
            previous_horizon,
            horizon: updated,
        } if previous_horizon == horizon && updated == Duration::hours(3)
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
    let swept = sweep_expired_heartbeat_leaves(&database.pool, &UnpublishedLoader)
        .await
        .expect("sweep expired heartbeats");
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
        register_heartbeat_class(&mut transaction, Duration::hours(2))
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
async fn manifest_excludes_absent_probes_but_birth_floor_keeps_the_complete_attached_set() {
    let database = TestDatabase::create().await;
    let class_key = "p3_manifest_30d";
    let parent = register_class(&database.pool, class_key, 30).await;
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
    let gone_birth = now - Duration::hours(6);
    sqlx::query(&format!(
        "UPDATE {LEAF_CATALOG} SET min_birth_at = $1 WHERE leaf_name = $2"
    ))
    .bind(gone_birth)
    .bind(gone.leaf_name())
    .execute(&mut *transaction)
    .await
    .expect("set missing leaf birth");
    sqlx::query(&format!(
        "UPDATE {LEAF_CATALOG} SET min_birth_at = $1 WHERE leaf_name = $2"
    ))
    .bind(now + Duration::hours(6))
    .bind(kept.leaf_name())
    .execute(&mut *transaction)
    .await
    .expect("set kept leaf birth");
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
    let _ = register_heartbeat_class(&mut transaction, Duration::hours(2))
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
    let selection = read_manifest_leaf_rows(&mut transaction)
        .await
        .expect("read manifest leaves");
    assert!(selection
        .attached
        .iter()
        .any(|row| row.leaf_name == gone.leaf_name()));
    assert_eq!(
        selection.absent_relations,
        vec![gone.leaf_name().to_owned()]
    );
    assert_eq!(
        read_attached_birth_floor(&mut transaction)
            .await
            .expect("read attached birth floor"),
        Some(gone_birth)
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
    let survivor = "z_p3_survivor_7d";
    register_class(&database.pool, survivor, 7).await;
    let mut transaction = database.pool.begin().await.expect("begin bad class insert");
    let sql = format!(
        "INSERT INTO {RETENTION_CLASSES} (
             class_key, duration, partition_interval, finite_parent_name, created_at
         ) VALUES ('a_p3_broken_7d', interval '7 days', interval '1 day',
                   'horsies_task_history_a_p3_broken_7d', statement_timestamp())"
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
    assert!(failure.refusal.contains("a_p3_broken_7d"));
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
async fn maintenance_session_exclusively_gates_archive_transitions_until_finished() {
    let database = TestDatabase::create().await;
    let session_id = Uuid::new_v4();
    let mut transaction = database
        .pool
        .begin()
        .await
        .expect("begin maintenance session");
    let session = begin_archive_maintenance(&mut transaction, session_id)
        .await
        .expect("open maintenance session");
    assert_eq!(session.session_id, session_id);
    assert_eq!(
        active_maintenance_session(&mut transaction)
            .await
            .expect("read active maintenance session"),
        Some(session_id)
    );
    assert!(matches!(
        begin_archive_maintenance(&mut transaction, Uuid::new_v4()).await,
        Err(MaintenanceSessionError::AlreadyActive)
    ));
    transaction
        .commit()
        .await
        .expect("commit maintenance session");

    let unavailable = sqlx::query(&format!("SELECT {ARCHIVE_AVAILABILITY_FUNCTION}()"))
        .execute(&database.pool)
        .await
        .expect_err("archive transition must fail while maintenance is active");
    let sqlstate = match unavailable {
        sqlx::Error::Database(error) => error.code().map(|code| code.into_owned()),
        other => panic!("availability gate returned a non-database error: {other}"),
    };
    assert_eq!(sqlstate.as_deref(), Some("55006"));

    let mut transaction = database
        .pool
        .begin()
        .await
        .expect("begin maintenance finish");
    finish_archive_maintenance(&mut transaction, session_id)
        .await
        .expect("finish maintenance session");
    assert_eq!(
        active_maintenance_session(&mut transaction)
            .await
            .expect("read closed maintenance session"),
        None
    );
    transaction
        .commit()
        .await
        .expect("commit maintenance finish");
    sqlx::query(&format!("SELECT {ARCHIVE_AVAILABILITY_FUNCTION}()"))
        .execute(&database.pool)
        .await
        .expect("archive transition resumes after maintenance");
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn pruning_finalizes_a_timeout_interrupted_detach_before_sweeping_it() {
    let database = TestDatabase::create_with_connections(4).await;
    let class_key = "p3_finalize_1d";
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
        DetachExpiredHistoryLeaf::new(leaf.clone(), None, Some(100)).expect("short detach command");
    let timed_out =
        detach_expired_leaf(&database.pool, &command, &UnpublishedLoader, &NoQuarantine)
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

    let pass = prune_expired_partitions(&database.pool, &UnpublishedLoader).await;
    assert_eq!(pass.finalized_leaves, vec![leaf.leaf_name().to_owned()]);
    assert_eq!(pass.dropped_count(), 1);
    assert!(pass.errors.is_empty(), "prune errors: {:?}", pass.errors);
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn pruning_contains_one_leaf_error_and_one_drop_refusal_then_keeps_going() {
    let database = TestDatabase::create().await;
    let class_key = "p3_prune_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let mut clock = database
        .pool
        .acquire()
        .await
        .expect("acquire pruning clock");
    let now = database_now(&mut clock).await.expect("database now");
    drop(clock);
    let base = (now - Duration::days(8))
        .with_hour(0)
        .and_then(|value| value.with_minute(0))
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate prune base day");
    for offset in 0..3 {
        let leaf = leaf_ref(&parent, class_key, base + Duration::days(offset));
        assert!(matches!(
            create_leaf(&database.pool, &leaf).await,
            LeafCreation::Created { .. }
        ));
    }
    let publisher = FailFirstRepublish::default();
    let pass = prune_expired_partitions(&database.pool, &publisher).await;
    assert_eq!(pass.history_swept.len(), 2);
    assert_eq!(pass.errors.len(), 1);
    assert!(pass.errors[0].contains("injected first-leaf publication failure"));
    assert_eq!(pass.refusals.len(), 1);
    assert!(pass.refusals[0].contains("RefusedLoaderReferences"));
    assert_eq!(pass.detached_count(), 2);
    assert_eq!(pass.dropped_count(), 1);
    assert!(pass.acted());
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn forever_conversion_is_idempotent_and_reports_daily_coverage_health() {
    let database = TestDatabase::create().await;
    let mut transaction = database.pool.begin().await.expect("begin forever check");
    assert_eq!(
        ensure_forever_range_partitioning(&mut transaction)
            .await
            .expect("ensure forever range partitioning"),
        0
    );
    let creations = ensure_leaf_coverage(
        &mut transaction,
        &EnsureLeafCoverage::new("forever", 3).expect("forever coverage command"),
        &UnpublishedLoader,
    )
    .await
    .expect("ensure forever coverage");
    assert!(!creations
        .iter()
        .any(|item| matches!(item, LeafCreation::ForeverClassLeaf { .. })));
    let report = collect_partition_health(
        &mut transaction,
        &CollectPartitionHealth::new("forever", true).expect("forever health command"),
    )
    .await
    .expect("forever health");
    assert!(report.is_healthy(), "forever faults: {:?}", report.faults);
    assert!(!report
        .faults
        .iter()
        .any(|fault| matches!(fault, HealthFault::CoverageBelowFloor { .. })));
    transaction.commit().await.expect("commit forever check");
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn populated_v34_forever_leaf_converts_without_rewriting_old_rows() {
    let database = TestDatabase::create().await;
    let mut transaction = database.pool.begin().await.expect("begin v34 restoration");
    sqlx::query(&format!(
        "DELETE FROM {LEAF_CATALOG} WHERE class_key = 'forever'"
    ))
    .execute(&mut *transaction)
    .await
    .expect("remove v35 forever catalog rows");
    sqlx::query(&format!(
        "ALTER TABLE {TASK_HISTORY_PARENT} DETACH PARTITION {TASK_HISTORY_FOREVER}"
    ))
    .execute(&mut *transaction)
    .await
    .expect("detach v35 forever range parent");
    sqlx::query(&format!("DROP TABLE {TASK_HISTORY_FOREVER} CASCADE"))
        .execute(&mut *transaction)
        .await
        .expect("drop v35 forever range parent");
    sqlx::query(&format!(
        "CREATE TABLE {TASK_HISTORY_FOREVER}
         PARTITION OF {TASK_HISTORY_PARENT} FOR VALUES IN ('forever')"
    ))
    .execute(&mut *transaction)
    .await
    .expect("restore v34 unbounded forever leaf");
    sqlx::query(&format!(
        "CREATE INDEX {TASK_HISTORY_FOREVER}_task_idx
         ON {TASK_HISTORY_FOREVER} (task_id)"
    ))
    .execute(&mut *transaction)
    .await
    .expect("restore v34 forever task index");
    sqlx::query(&format!(
        "CREATE INDEX {TASK_HISTORY_FOREVER}_enqueued_idx
         ON {TASK_HISTORY_FOREVER} (enqueued_at)"
    ))
    .execute(&mut *transaction)
    .await
    .expect("restore v34 forever ordering index");

    let now = database_now(&mut transaction).await.expect("database now");
    let today = now
        .with_hour(0)
        .and_then(|value| value.with_minute(0))
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate UTC day");
    let old_anchor = today - Duration::days(30);
    let current_anchor = today + Duration::hours(1);
    let old_id = Uuid::new_v4();
    let current_id = Uuid::new_v4();
    let insert = format!(
        "INSERT INTO {TASK_HISTORY_PARENT} (
             task_id, task_name, queue_name, priority,
             command_fingerprint_version, command_fingerprint,
             status, terminalization_kind, terminal_at,
             retention_anchor_at, retention_class_key,
             enqueued_at, created_at, retry_count, max_retries,
             result_envelope_version, result_codec, result_content_type,
             is_workflow_task, history_schema_version,
             attempt_archive_version, attempt_snapshot_codec,
             attempt_snapshot_content_type, attempt_snapshot,
             attempt_snapshot_digest, rerun_input_disposition
         ) VALUES (
             $1, 'p3 v34 forever', 'default', 100,
             1, $2, 'COMPLETED', 'LEGACY_TERMINAL', $3,
             $3, 'forever', $3, $3, 0, 0,
             1, 'json-utf8', 'application/json', FALSE, 1,
             1, 'json-utf8', 'application/json', $4, $5,
             'NEVER_ELIGIBLE'
         )"
    );
    for (task_id, anchor) in [(old_id, old_anchor), (current_id, current_anchor)] {
        sqlx::query(&insert)
            .bind(task_id)
            .bind(vec![3_u8; 32])
            .bind(anchor)
            .bind(b"[]".as_slice())
            .bind(vec![5_u8; 32])
            .execute(&mut *transaction)
            .await
            .expect("seed populated v34 forever row");
    }
    let old_relation_oid: i64 = sqlx::query_scalar(&format!(
        "SELECT tableoid::oid::bigint FROM {TASK_HISTORY_PARENT} WHERE task_id = $1"
    ))
    .bind(old_id)
    .fetch_one(&mut *transaction)
    .await
    .expect("capture pre-conversion old-row relation");

    assert_eq!(
        ensure_forever_range_partitioning(&mut transaction)
            .await
            .expect("convert populated v34 forever leaf"),
        1
    );
    let relkind: String =
        sqlx::query_scalar("SELECT relkind::text FROM pg_class WHERE oid = to_regclass($1)")
            .bind(TASK_HISTORY_FOREVER)
            .fetch_one(&mut *transaction)
            .await
            .expect("read converted forever relkind");
    assert_eq!(relkind, "p");
    let current_leaf =
        daily_leaf_name(TASK_HISTORY_FOREVER, today).expect("current forever daily leaf name");
    let locations: Vec<(Uuid, String)> = sqlx::query_as(&format!(
        "SELECT task_id, tableoid::regclass::text
         FROM {TASK_HISTORY_PARENT} WHERE task_id = ANY($1) ORDER BY task_id"
    ))
    .bind(vec![old_id, current_id])
    .fetch_all(&mut *transaction)
    .await
    .expect("read converted row locations");
    assert_eq!(locations.len(), 2);
    for (task_id, relation) in locations {
        if task_id == old_id {
            assert_eq!(relation, FOREVER_LEGACY_LEAF);
        } else if task_id == current_id {
            assert_eq!(relation, current_leaf);
        } else {
            panic!("unexpected converted task ID {task_id}");
        }
    }
    let legacy_oid: i64 = sqlx::query_scalar("SELECT to_regclass($1)::oid::bigint")
        .bind(FOREVER_LEGACY_LEAF)
        .fetch_one(&mut *transaction)
        .await
        .expect("read legacy forever OID");
    assert_eq!(legacy_oid, old_relation_oid);
    let legacy = read_leaf_catalog_row(&mut transaction, FOREVER_LEGACY_LEAF)
        .await
        .expect("read legacy forever catalog row")
        .expect("legacy forever catalog row exists");
    assert_eq!(legacy.parent_name, TASK_HISTORY_FOREVER);
    assert_eq!(legacy.class_key, "forever");
    assert_eq!(
        legacy.lower_anchor,
        chrono::DateTime::from_timestamp(0, 0).unwrap()
    );
    assert_eq!(legacy.upper_anchor, today);
    assert!(!legacy.min_birth_verified);
    assert_eq!(
        legacy.id_index_name,
        leaf_id_index_name(FOREVER_LEGACY_LEAF)
    );
    assert!(
        read_leaf_physical_state(
            &mut transaction,
            FOREVER_LEGACY_LEAF,
            TASK_HISTORY_FOREVER,
            &legacy.id_index_name,
            LeafPartitionBoundExpectation::CatalogOnly,
            LeafIndexKind::History,
        )
        .await
        .expect("read legacy physical state")
        .id_index_conformant
    );
    assert_eq!(
        capture_partition_bound_utc(&mut transaction, FOREVER_LEGACY_LEAF)
            .await
            .expect("capture legacy bound")
            .as_deref(),
        Some(legacy.partition_bound.as_str())
    );
    assert!(
        super::catalog::read_leaf_ordering_index_exists(&mut transaction, FOREVER_LEGACY_LEAF,)
            .await
            .expect("read legacy ordering-index property")
    );
    assert_eq!(
        ensure_forever_range_partitioning(&mut transaction)
            .await
            .expect("rerun forever conversion"),
        0
    );
    sqlx::query("ALTER TABLE horsies_tasks DROP CONSTRAINT horsies_tasks_live_status_only")
        .execute(&mut *transaction)
        .await
        .expect("restore pre-cutover terminal live-row posture");
    let relocation_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, args, kwargs, status,
             sent_at, enqueued_at, completed_at, result, terminal_at,
             terminalization_kind, retry_count, max_retries, enqueue_sha,
             is_workflow_task, command_fingerprint_version,
             command_fingerprint, retention_class_key, retain_rerun_input,
             prepared_rerun_input_disposition, created_at, updated_at
         ) VALUES (
             $1, 'p3 legacy forever relocation', 'default', 100, '[]', '{}',
             'COMPLETED', $2, $2, $2, NULL, $2, 'COMPLETE_LOCKED', 0, 0,
             $1::text, FALSE, 1, $3, 'forever', FALSE, 'NEVER_ELIGIBLE',
             $2, $2
         )",
    )
    .bind(relocation_id)
    .bind(old_anchor)
    .bind(vec![7_u8; 32])
    .execute(&mut *transaction)
    .await
    .expect("seed pre-today forever relocation task");
    assert!(matches!(
        relocate_terminal_batch(&mut transaction, 1)
            .await
            .expect("relocate into the MINVALUE legacy forever leaf"),
        RelocationOutcome::Batch {
            rows_relocated: 1,
            ..
        }
    ));
    let relocation_relation: String = sqlx::query_scalar(&format!(
        "SELECT tableoid::regclass::text
         FROM {TASK_HISTORY_PARENT} WHERE task_id = $1"
    ))
    .bind(relocation_id)
    .fetch_one(&mut *transaction)
    .await
    .expect("read relocated legacy forever row");
    assert_eq!(relocation_relation, FOREVER_LEGACY_LEAF);
    let row_count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {TASK_HISTORY_PARENT} WHERE task_id = ANY($1)"
    ))
    .bind(vec![old_id, current_id])
    .fetch_one(&mut *transaction)
    .await
    .expect("verify rows survive idempotent rerun");
    assert_eq!(row_count, 2);
    transaction
        .commit()
        .await
        .expect("commit v34 conversion test");
    database.drop().await;
}
