//! Statement layer against a live PostgreSQL.
//!
//! `DATABASE_URL` names the server (the `horsies-core` database of the core
//! suite); each test creates its own empty database through the core's test
//! support (`src/test_support/database.rs`) and drops it at the end. The
//! PgBouncer tests are ignored unless the feature `pgbouncer-fixture-tests`
//! is on; they read their URLs from `HORSIES_STMT_PGBOUNCER_STATEMENT_URL`
//! (a pool in statement mode) and `HORSIES_STMT_PGBOUNCER_TRANSACTION_URL`
//! (transaction mode with `max_prepared_statements = 0`). The `rust-pgbouncer`
//! job of `.github/workflows/tests-full.yml` runs them on the fixture of
//! `clients/python/tests/fixtures/pgbouncer/`.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_postgres::error::SqlState;
use tokio_postgres::Client;
use uuid::Uuid;

use horsies_core::db::{
    open_connection, parse_connection_url, ChannelName, CheckoutClass, CheckoutFailure,
    ConnectFailure, ConnectPlan, ConnectionAge, ConnectionEnvironment, CorePool, DatabasePassword,
    EnvironmentPassword, HomeDirectory, IdlePing, IsolationLevel, Listener, ListenerClose,
    NotSentCause, ParameterType, PayloadEvent, PayloadFilter, PoolPlan, PoolRole, PoolSizing,
    PooledConnection, QueueCapacity, RefusalCause, Reserve, RollbackOutcome, StatementBound,
    StatementFailure, StatementProtocol, Transience, TypedStatement, UnknownOutcomeCause,
    WakeEvent, DEFAULT_SOCKET_SETTINGS, WORKER_RESERVED_CONNECTIONS,
};

const PID: TypedStatement =
    TypedStatement::new("stmt_test_pid", "SELECT pg_backend_pid() AS pid", &[]);

const SLEEP: TypedStatement = TypedStatement::new(
    "stmt_test_sleep",
    "SELECT pg_sleep($1)",
    &[ParameterType::Float8],
);

const ECHO: TypedStatement = TypedStatement::new(
    "stmt_test_echo",
    "SELECT $1::int8 AS number, $2::text AS label, $3::bool AS flag, $4::uuid AS id, \
     $5::jsonb AS document, $6::timestamptz AS at, $7::int4[] AS numbers, $8::bytea AS bytes",
    &[
        ParameterType::Int8,
        ParameterType::Text,
        ParameterType::Bool,
        ParameterType::Uuid,
        ParameterType::Jsonb,
        ParameterType::Timestamptz,
        ParameterType::Int4Array,
        ParameterType::Bytea,
    ],
);

const RAISE: TypedStatement = TypedStatement::new(
    "stmt_test_raise",
    "SELECT stmt_raise($1)",
    &[ParameterType::Text],
);

const INSERT_ITEM: TypedStatement = TypedStatement::new(
    "stmt_test_insert_item",
    "INSERT INTO stmt_items (id, label) VALUES ($1, $2)",
    &[ParameterType::Int4, ParameterType::Text],
);

const NOTIFY: TypedStatement = TypedStatement::new(
    "stmt_test_notify",
    "SELECT pg_notify($1, $2)",
    &[ParameterType::Text, ParameterType::Text],
);

const ADD: TypedStatement = TypedStatement::new(
    "stmt_test_add",
    "SELECT $1::int8 + $2::int8 AS total",
    &[ParameterType::Int8, ParameterType::Int8],
);

const COUNT_ITEMS: TypedStatement = TypedStatement::new(
    "stmt_test_count_items",
    "SELECT count(*) AS total FROM stmt_items WHERE id = $1",
    &[ParameterType::Int4],
);

const INSERT_DEFERRED: TypedStatement = TypedStatement::new(
    "stmt_test_insert_deferred",
    "INSERT INTO stmt_deferred (id) VALUES ($1)",
    &[ParameterType::Int4],
);

const INSERT_SLOW_COMMIT: TypedStatement = TypedStatement::new(
    "stmt_test_insert_slow_commit",
    "INSERT INTO stmt_slow_commit (id) VALUES ($1)",
    &[ParameterType::Int4],
);

const EVENT_BOUND: Duration = Duration::from_secs(15);

fn test_environment() -> ConnectionEnvironment {
    let password = match std::env::var("PGPASSWORD") {
        Ok(text) if !text.is_empty() => {
            EnvironmentPassword::Set(DatabasePassword::from_bytes(text.into_bytes()))
        }
        Ok(_) | Err(_) => EnvironmentPassword::NotSet,
    };
    let home = match std::env::var_os("HOME") {
        Some(path) => HomeDirectory::Known(path.into()),
        None => HomeDirectory::Unknown,
    };
    ConnectionEnvironment::new(password, Vec::new(), home)
}

fn server_url() -> String {
    test_database::server_url()
}

/// The test databases of the core (`src/test_support/database.rs`). This
/// binary creates empty databases only.
#[allow(dead_code)]
#[path = "../src/test_support/database.rs"]
mod test_database;

/// Installs the schema into a test template through the public API. This
/// binary creates empty databases only, so it never runs.
#[allow(dead_code)]
async fn install_template(url: &str) {
    let broker = horsies_core::PostgresBroker::connect(url, horsies_core::ClientFamily::Rust)
        .await
        .expect("connect the template broker");
    broker
        .migrate()
        .await
        .expect("install the schema into the test template");
    broker.close().await;
}

/// The same URL with another database and extra query parameters.
fn with_database(url: &str, database: &str, extra_query: &str) -> String {
    let (scheme, rest) = url.split_once("://").expect("URL has a scheme");
    let authority_end = rest
        .find(|character: char| character == '/' || character == '?')
        .unwrap_or(rest.len());
    let (authority, remainder) = rest.split_at(authority_end);
    let query = remainder
        .split_once('?')
        .map(|(_, query)| query)
        .unwrap_or("");
    let joined = [query, extra_query]
        .iter()
        .filter(|part| !part.is_empty())
        .copied()
        .collect::<Vec<&str>>()
        .join("&");
    match joined.is_empty() {
        true => format!("{scheme}://{authority}/{database}"),
        false => format!("{scheme}://{authority}/{database}?{joined}"),
    }
}

fn load_plan(url: &str) -> Arc<ConnectPlan> {
    let environment = test_environment();
    let settings = parse_connection_url(url, &environment).expect("test URL parses");
    Arc::new(
        ConnectPlan::load(&settings, &environment, &DEFAULT_SOCKET_SETTINGS)
            .expect("test plan loads"),
    )
}

async fn connect_direct(url: &str) -> (Client, JoinHandle<()>) {
    let plan = load_plan(url);
    let (client, connection) = open_connection(&plan).await.expect("direct connection");
    let driver = tokio::spawn(async move {
        let _ended = connection.await;
    });
    (client, driver)
}

/// A database of its own for one test.
struct TestDatabase {
    database: test_database::TestDatabase,
    admin: Client,
    admin_driver: JoinHandle<()>,
}

impl TestDatabase {
    async fn create() -> TestDatabase {
        let (admin, admin_driver) =
            connect_direct(&with_database(&server_url(), "postgres", "")).await;
        let database = TestDatabase {
            database: test_database::TestDatabase::create_empty().await,
            admin,
            admin_driver,
        };
        let (setup, setup_driver) = connect_direct(&database.url("")).await;
        setup
            .batch_execute(
                "CREATE TABLE stmt_items (id int4 PRIMARY KEY, label text NOT NULL); \
                 CREATE FUNCTION stmt_raise(code text) RETURNS int4 LANGUAGE plpgsql AS \
                 $$ BEGIN RAISE EXCEPTION 'raised %', code USING ERRCODE = code; END $$; \
                 CREATE TABLE stmt_deferred (id int4 PRIMARY KEY DEFERRABLE INITIALLY DEFERRED); \
                 CREATE TABLE stmt_slow_commit (id int4 PRIMARY KEY); \
                 CREATE FUNCTION stmt_sleep_at_commit() RETURNS trigger LANGUAGE plpgsql AS \
                 $$ BEGIN PERFORM pg_sleep(10); RETURN NULL; END $$; \
                 CREATE CONSTRAINT TRIGGER stmt_slow_commit_trigger AFTER INSERT ON \
                 stmt_slow_commit DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION \
                 stmt_sleep_at_commit();",
            )
            .await
            .expect("create test schema");
        drop(setup);
        setup_driver.abort();
        database
    }

    fn url(&self, extra_query: &str) -> String {
        with_database(&server_url(), self.database.name(), extra_query)
    }

    fn pool(
        &self,
        role: PoolRole,
        size: u32,
        reserve: Reserve,
        protocol: StatementProtocol,
    ) -> CorePool {
        CorePool::new(
            PoolPlan {
                role,
                sizing: PoolSizing::new(size, reserve).expect("valid sizing"),
                checkout_wait: Duration::from_millis(500),
                idle_ping: IdlePing::AfterIdle(Duration::ZERO),
                max_age: ConnectionAge::Unlimited,
                protocol,
            },
            load_plan(&self.url("")),
        )
    }

    async fn terminate(&self, pid: i32) {
        let row = self
            .admin
            .query_one("SELECT pg_terminate_backend($1)", &[&pid])
            .await
            .expect("terminate backend");
        let terminated: bool = row.try_get(0).expect("bool result");
        assert!(terminated, "backend {pid} was not terminated");
    }

    async fn terminate_application(&self, application_name: &str) -> i64 {
        let row = self
            .admin
            .query_one(
                "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity \
                 WHERE application_name = $1",
                &[&application_name],
            )
            .await
            .expect("terminate application backends");
        row.try_get(0).expect("count result")
    }

    async fn remove(self) {
        self.database.drop().await;
        self.admin_driver.abort();
    }
}

async fn notify(pool: &CorePool, channel: &str, payload: &str) {
    let channel = channel.to_owned();
    let payload = payload.to_owned();
    let _rows = pool
        .query(
            CheckoutClass::Reserved,
            &NOTIFY.bind().value(&channel).value(&payload).finish(),
            StatementBound::ConnectionTimeouts,
        )
        .await
        .expect("notify");
}

async fn item_count(pool: &CorePool, id: i32) -> i64 {
    let rows = pool
        .query(
            CheckoutClass::Reserved,
            &COUNT_ITEMS.bind().value(&id).finish(),
            StatementBound::ConnectionTimeouts,
        )
        .await
        .expect("count query");
    let [row] = rows.as_slice() else {
        panic!("expected one row, got {}", rows.len());
    };
    row.try_get("total").expect("total column")
}

async fn insert_item(
    transaction: &mut horsies_core::db::Transaction<'_>,
    id: i32,
) -> Result<u64, StatementFailure> {
    let label = format!("item {id}");
    transaction
        .execute(
            &INSERT_ITEM.bind().value(&id).value(&label).finish(),
            StatementBound::ConnectionTimeouts,
        )
        .await
}

async fn backend_pid(connection: &mut PooledConnection) -> i32 {
    let rows = connection
        .query(&PID.bind().finish(), StatementBound::ConnectionTimeouts)
        .await
        .expect("pid query");
    let [row] = rows.as_slice() else {
        panic!("expected one row, got {}", rows.len());
    };
    row.try_get("pid").expect("pid column")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn both_protocols_return_the_same_typed_row() {
    let database = TestDatabase::create().await;
    let number: i64 = 1 << 40;
    let label = "queue-a".to_owned();
    let flag = true;
    let id = Uuid::new_v4();
    let document = serde_json::json!({"k": [1, 2, 3]});
    let at = chrono::DateTime::parse_from_rfc3339("2026-11-16T10:00:00Z")
        .expect("timestamp")
        .with_timezone(&chrono::Utc);
    let numbers: Vec<i32> = vec![3, 1, 2];
    let bytes: Vec<u8> = vec![0, 255, 7];
    for protocol in [
        StatementProtocol::PreparedPerConnection,
        StatementProtocol::UnnamedOnly,
    ] {
        let pool = database.pool(PoolRole::Main, 2, Reserve::NoReserve, protocol);
        for _run in 0..3 {
            let statement = ECHO
                .bind()
                .value(&number)
                .value(&label)
                .value(&flag)
                .value(&id)
                .value(&document)
                .value(&at)
                .value(&numbers)
                .value(&bytes)
                .finish();
            let rows = pool
                .query(
                    CheckoutClass::Reserved,
                    &statement,
                    StatementBound::ConnectionTimeouts,
                )
                .await
                .expect("echo runs");
            let [row] = rows.as_slice() else {
                panic!("expected one row, got {}", rows.len());
            };
            assert_eq!(row.try_get::<_, i64>("number").expect("number"), number);
            assert_eq!(row.try_get::<_, String>("label").expect("label"), label);
            assert_eq!(row.try_get::<_, bool>("flag").expect("flag"), flag);
            assert_eq!(row.try_get::<_, Uuid>("id").expect("id"), id);
            assert_eq!(
                row.try_get::<_, serde_json::Value>("document")
                    .expect("document"),
                document,
            );
            assert_eq!(
                row.try_get::<_, chrono::DateTime<chrono::Utc>>("at")
                    .expect("at"),
                at,
            );
            assert_eq!(
                row.try_get::<_, Vec<i32>>("numbers").expect("numbers"),
                numbers
            );
            assert_eq!(row.try_get::<_, Vec<u8>>("bytes").expect("bytes"), bytes);
        }
        pool.close();
    }
    database.remove().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn execute_returns_the_affected_row_count() {
    let database = TestDatabase::create().await;
    let pool = database.pool(
        PoolRole::Main,
        1,
        Reserve::NoReserve,
        StatementProtocol::PreparedPerConnection,
    );
    let id: i32 = 1;
    let label = "first".to_owned();
    let count = pool
        .execute(
            CheckoutClass::Reserved,
            &INSERT_ITEM.bind().value(&id).value(&label).finish(),
            StatementBound::ConnectionTimeouts,
        )
        .await
        .expect("insert runs");
    assert_eq!(count, 1);
    pool.close();
    database.remove().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_error_is_refused_with_its_class_and_the_connection_stays() {
    let database = TestDatabase::create().await;
    for protocol in [
        StatementProtocol::PreparedPerConnection,
        StatementProtocol::UnnamedOnly,
    ] {
        let pool = database.pool(PoolRole::Main, 1, Reserve::NoReserve, protocol);
        let mut connection = pool
            .checkout(CheckoutClass::Reserved)
            .await
            .expect("checkout");
        let pid_before = backend_pid(&mut connection).await;
        for (code, expected) in [
            ("40001", Transience::Transient),
            ("55P03", Transience::Transient),
            ("23505", Transience::Permanent),
            ("22012", Transience::Permanent),
        ] {
            let code_text = code.to_owned();
            let failure = connection
                .query(
                    &RAISE.bind().value(&code_text).finish(),
                    StatementBound::ConnectionTimeouts,
                )
                .await
                .expect_err("the function raises");
            match &failure {
                StatementFailure::StatementRefused {
                    cause: RefusalCause::Server(error),
                    ..
                } => assert_eq!(error.code().code(), code),
                other => panic!("expected StatementRefused for {code}, got {other}"),
            }
            assert_eq!(failure.transience(), expected, "{code}");
        }
        assert_eq!(backend_pid(&mut connection).await, pid_before);
        drop(connection);
        pool.close();
    }
    database.remove().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backend_killed_during_a_statement_leaves_the_outcome_unknown() {
    let database = TestDatabase::create().await;
    let pool = database.pool(
        PoolRole::Main,
        1,
        Reserve::NoReserve,
        StatementProtocol::PreparedPerConnection,
    );
    let mut connection = pool
        .checkout(CheckoutClass::Reserved)
        .await
        .expect("checkout");
    let pid = backend_pid(&mut connection).await;
    let seconds: f64 = 10.0;
    let sleep = SLEEP.bind().value(&seconds).finish();
    let killer = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        database.terminate(pid).await;
    };
    let (outcome, ()) = tokio::join!(
        connection.query(&sleep, StatementBound::ConnectionTimeouts),
        killer
    );
    match outcome {
        Err(StatementFailure::OutcomeUnknown {
            cause: UnknownOutcomeCause::SessionTerminated(error),
            ..
        }) => assert_eq!(error.code(), &SqlState::ADMIN_SHUTDOWN),
        Err(StatementFailure::OutcomeUnknown {
            cause: UnknownOutcomeCause::ConnectionLost(_),
            ..
        }) => {}
        Err(other) => panic!("expected OutcomeUnknown, got {other}"),
        Ok(_) => panic!("expected OutcomeUnknown, got rows"),
    }
    drop(connection);
    let mut replacement = pool
        .checkout(CheckoutClass::Reserved)
        .await
        .expect("a new connection replaces the discarded one");
    assert_ne!(backend_pid(&mut replacement).await, pid);
    drop(replacement);
    pool.close();
    database.remove().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_statement_past_its_bound_is_cancelled_and_refused() {
    let database = TestDatabase::create().await;
    let pool = database.pool(
        PoolRole::Main,
        1,
        Reserve::NoReserve,
        StatementProtocol::UnnamedOnly,
    );
    let mut connection = pool
        .checkout(CheckoutClass::Reserved)
        .await
        .expect("checkout");
    let pid = backend_pid(&mut connection).await;
    let seconds: f64 = 30.0;
    let failure = connection
        .query(
            &SLEEP.bind().value(&seconds).finish(),
            StatementBound::CancelAfter(Duration::from_millis(300)),
        )
        .await
        .expect_err("the sleep is cancelled");
    match &failure {
        StatementFailure::StatementRefused {
            cause: RefusalCause::CancelledAfterBound { error, .. },
            ..
        } => assert_eq!(error.code(), &SqlState::QUERY_CANCELED),
        other => panic!("expected CancelledAfterBound, got {other}"),
    }
    assert_eq!(failure.transience(), Transience::Transient);
    assert_eq!(backend_pid(&mut connection).await, pid);
    drop(connection);
    pool.close();
    database.remove().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_reserve_keeps_connections_for_reserved_checkouts() {
    let database = TestDatabase::create().await;
    let pool = database.pool(
        PoolRole::WorkerProfile,
        3,
        Reserve::KeepFree(WORKER_RESERVED_CONNECTIONS),
        StatementProtocol::PreparedPerConnection,
    );
    let batch = pool
        .checkout(CheckoutClass::Batch)
        .await
        .expect("first batch checkout");
    match pool.checkout(CheckoutClass::Batch).await {
        Err(CheckoutFailure::TimedOut { role, .. }) => assert_eq!(role, PoolRole::WorkerProfile),
        Err(other) => panic!("expected TimedOut, got {other}"),
        Ok(_) => panic!("a second batch checkout passed the reserve gate"),
    }
    let reserved_one = pool
        .checkout(CheckoutClass::Reserved)
        .await
        .expect("first reserved checkout");
    let reserved_two = pool
        .checkout(CheckoutClass::Reserved)
        .await
        .expect("second reserved checkout");
    drop(batch);
    let batch_again = pool
        .checkout(CheckoutClass::Batch)
        .await
        .expect("the gate opens when the batch connection returns");
    drop((batch_again, reserved_one, reserved_two));
    pool.close();
    database.remove().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connection_killed_while_idle_is_replaced_at_checkout() {
    let database = TestDatabase::create().await;
    let pool = database.pool(
        PoolRole::Main,
        1,
        Reserve::NoReserve,
        StatementProtocol::PreparedPerConnection,
    );
    let pid = {
        let mut connection = pool
            .checkout(CheckoutClass::Reserved)
            .await
            .expect("checkout");
        backend_pid(&mut connection).await
    };
    database.terminate(pid).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut connection = pool
        .checkout(CheckoutClass::Reserved)
        .await
        .expect("the dead connection is replaced");
    assert_ne!(backend_pid(&mut connection).await, pid);
    drop(connection);
    pool.close();
    database.remove().await;
}

/// #1403: a pooled connection is opened and driven on the runtime of the
/// process, so it still serves after the runtime of the caller that opened
/// it ended: the second runtime gets the same backend.
#[test]
fn a_pooled_connection_outlives_the_runtime_that_opened_it() {
    let setup = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("the setup runtime starts");
    let database = setup.block_on(TestDatabase::create());
    let pool = database.pool(
        PoolRole::Main,
        1,
        Reserve::NoReserve,
        StatementProtocol::PreparedPerConnection,
    );
    let first = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the first caller runtime starts");
    let opened_by = first.block_on(async {
        let mut connection = pool
            .checkout(CheckoutClass::Reserved)
            .await
            .expect("checkout");
        backend_pid(&mut connection).await
    });
    drop(first);
    let second = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the second caller runtime starts");
    let served_by = second.block_on(async {
        let mut connection = pool
            .checkout(CheckoutClass::Reserved)
            .await
            .expect("the connection still serves");
        backend_pid(&mut connection).await
    });
    assert_eq!(served_by, opened_by);
    pool.close();
    setup.block_on(database.remove());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_closed_pool_refuses_checkouts_permanently() {
    let database = TestDatabase::create().await;
    let pool = database.pool(
        PoolRole::Session,
        1,
        Reserve::NoReserve,
        StatementProtocol::PreparedPerConnection,
    );
    pool.close();
    match pool.checkout(CheckoutClass::Reserved).await {
        Err(failure @ CheckoutFailure::PoolClosed { .. }) => {
            assert_eq!(failure.transience(), Transience::Permanent);
        }
        Err(other) => panic!("expected PoolClosed, got {other}"),
        Ok(_) => panic!("a closed pool handed out a connection"),
    }
    database.remove().await;
}

#[tokio::test]
async fn an_unreachable_server_is_not_sent_and_transient() {
    let pool = CorePool::new(
        PoolPlan {
            role: PoolRole::Main,
            sizing: PoolSizing::new(1, Reserve::NoReserve).expect("valid sizing"),
            checkout_wait: Duration::from_secs(5),
            idle_ping: IdlePing::Off,
            max_age: ConnectionAge::Unlimited,
            protocol: StatementProtocol::UnnamedOnly,
        },
        load_plan(
            "postgresql://stmt_user:pw@127.0.0.1:1/stmt_none?sslmode=disable&connect_timeout=2",
        ),
    );
    let failure = pool
        .query(
            CheckoutClass::Reserved,
            &PID.bind().finish(),
            StatementBound::ConnectionTimeouts,
        )
        .await
        .expect_err("nothing listens on port 1");
    match &failure {
        StatementFailure::NotSent {
            cause: NotSentCause::Checkout(CheckoutFailure::ConnectFailed { cause, .. }),
            ..
        } => assert!(matches!(
            cause.cause,
            ConnectFailure::TransportFailed(_) | ConnectFailure::TimedOut { .. }
        )),
        other => panic!("expected NotSent(ConnectFailed), got {other}"),
    }
    assert_eq!(failure.transience(), Transience::Transient);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_listener_delivers_payloads_and_a_gap_after_a_reconnect() {
    let database = TestDatabase::create().await;
    let application_name = format!(
        "stmt_listener_{}",
        &Uuid::new_v4().simple().to_string()[..8]
    );
    let listener = Listener::start(load_plan(
        &database.url(&format!("application_name={application_name}")),
    ));
    let pool = database.pool(
        PoolRole::Main,
        1,
        Reserve::NoReserve,
        StatementProtocol::PreparedPerConnection,
    );
    let channel_text = "stmt_channel".to_owned();
    let channel = ChannelName::parse(&channel_text).expect("valid channel");
    let subscription = listener
        .subscribe_payloads(channel, QueueCapacity::new(16).expect("valid capacity"))
        .await
        .expect("subscribe");
    notify(&pool, &channel_text, "first").await;
    assert_eq!(
        tokio::time::timeout(EVENT_BOUND, subscription.recv())
            .await
            .expect("first payload in time"),
        PayloadEvent::Payload("first".to_owned()),
    );
    assert!(database.terminate_application(&application_name).await >= 1);
    assert_eq!(
        tokio::time::timeout(EVENT_BOUND, subscription.recv())
            .await
            .expect("gap in time"),
        PayloadEvent::Gap,
    );
    notify(&pool, &channel_text, "second").await;
    assert_eq!(
        tokio::time::timeout(EVENT_BOUND, subscription.recv())
            .await
            .expect("second payload in time"),
        PayloadEvent::Payload("second".to_owned()),
    );
    assert_eq!(listener.close().await, ListenerClose::Closed);
    assert_eq!(subscription.recv().await, PayloadEvent::Stopped);
    pool.close();
    database.remove().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wake_subscription_follows_its_payload_filter() {
    let database = TestDatabase::create().await;
    let listener = Listener::start(load_plan(&database.url("")));
    let pool = database.pool(
        PoolRole::Main,
        1,
        Reserve::NoReserve,
        StatementProtocol::UnnamedOnly,
    );
    let channel_text = "stmt_wake".to_owned();
    let wake = listener
        .subscribe_wake(
            ChannelName::parse(&channel_text).expect("valid channel"),
            PayloadFilter::Payload("wanted".to_owned()),
        )
        .await
        .expect("subscribe");
    for payload in ["other", "wanted"] {
        notify(&pool, &channel_text, payload).await;
    }
    assert_eq!(
        tokio::time::timeout(EVENT_BOUND, wake.recv())
            .await
            .expect("wake in time"),
        WakeEvent::Notified,
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(500), wake.recv())
            .await
            .is_err(),
        "the filtered-out payload must not wake the subscription",
    );
    assert_eq!(listener.close().await, ListenerClose::Closed);
    assert_eq!(wake.recv().await, WakeEvent::Stopped);
    pool.close();
    database.remove().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_committed_transaction_persists_and_a_rolled_back_one_does_not() {
    let database = TestDatabase::create().await;
    let pool = database.pool(
        PoolRole::Main,
        1,
        Reserve::NoReserve,
        StatementProtocol::PreparedPerConnection,
    );
    let mut connection = pool
        .checkout(CheckoutClass::Reserved)
        .await
        .expect("checkout");
    let mut transaction = connection
        .begin(IsolationLevel::ServerDefault)
        .await
        .expect("begin");
    assert_eq!(insert_item(&mut transaction, 10).await.expect("insert"), 1);
    transaction.commit().await.expect("commit");
    let mut transaction = connection
        .begin(IsolationLevel::Serializable)
        .await
        .expect("begin");
    assert_eq!(insert_item(&mut transaction, 11).await.expect("insert"), 1);
    assert!(matches!(
        transaction.rollback().await,
        RollbackOutcome::RolledBack
    ));
    drop(connection);
    assert_eq!(item_count(&pool, 10).await, 1);
    assert_eq!(item_count(&pool, 11).await, 0);
    pool.close();
    database.remove().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_statement_aborts_the_transaction_and_commit_is_not_sent() {
    let database = TestDatabase::create().await;
    let pool = database.pool(
        PoolRole::Main,
        1,
        Reserve::NoReserve,
        StatementProtocol::UnnamedOnly,
    );
    let mut connection = pool
        .checkout(CheckoutClass::Reserved)
        .await
        .expect("checkout");
    let pid = backend_pid(&mut connection).await;
    let mut transaction = connection
        .begin(IsolationLevel::ServerDefault)
        .await
        .expect("begin");
    assert_eq!(insert_item(&mut transaction, 20).await.expect("insert"), 1);
    match insert_item(&mut transaction, 20).await {
        Err(StatementFailure::StatementRefused {
            cause: RefusalCause::Server(error),
            ..
        }) => assert_eq!(error.code(), &SqlState::UNIQUE_VIOLATION),
        other => panic!("expected a unique violation, got {other:?}"),
    }
    match insert_item(&mut transaction, 21).await {
        Err(StatementFailure::NotSent {
            cause: NotSentCause::TransactionAborted { failed_statement },
            ..
        }) => assert_eq!(failed_statement, INSERT_ITEM.name()),
        other => panic!("expected TransactionAborted, got {other:?}"),
    }
    match transaction.commit().await {
        Err(StatementFailure::NotSent {
            cause: NotSentCause::TransactionAborted { failed_statement },
            statement,
        }) => {
            assert_eq!(failed_statement, INSERT_ITEM.name());
            assert_eq!(statement, "commit");
        }
        other => panic!("expected TransactionAborted at commit, got {other:?}"),
    }
    assert_eq!(backend_pid(&mut connection).await, pid);
    drop(connection);
    assert_eq!(item_count(&pool, 20).await, 0);
    pool.close();
    database.remove().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deferred_constraint_refuses_the_commit() {
    let database = TestDatabase::create().await;
    let pool = database.pool(
        PoolRole::Main,
        1,
        Reserve::NoReserve,
        StatementProtocol::PreparedPerConnection,
    );
    let mut connection = pool
        .checkout(CheckoutClass::Reserved)
        .await
        .expect("checkout");
    let pid = backend_pid(&mut connection).await;
    let mut transaction = connection
        .begin(IsolationLevel::ServerDefault)
        .await
        .expect("begin");
    let id: i32 = 30;
    for _copy in 0..2 {
        let _inserted = transaction
            .execute(
                &INSERT_DEFERRED.bind().value(&id).finish(),
                StatementBound::ConnectionTimeouts,
            )
            .await
            .expect("a deferred constraint does not check the insert");
    }
    match transaction.commit().await {
        Err(failure @ StatementFailure::StatementRefused { .. }) => {
            assert_eq!(failure.transience(), Transience::Permanent);
            match failure {
                StatementFailure::StatementRefused {
                    cause: RefusalCause::Server(error),
                    statement,
                } => {
                    assert_eq!(error.code(), &SqlState::UNIQUE_VIOLATION);
                    assert_eq!(statement, "commit");
                }
                other => panic!("expected a server refusal, got {other}"),
            }
        }
        other => panic!("expected StatementRefused at commit, got {other:?}"),
    }
    assert_eq!(backend_pid(&mut connection).await, pid);
    drop(connection);
    pool.close();
    database.remove().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_commit_reply_leaves_the_outcome_unknown() {
    let database = TestDatabase::create().await;
    let pool = database.pool(
        PoolRole::Main,
        1,
        Reserve::NoReserve,
        StatementProtocol::PreparedPerConnection,
    );
    let mut connection = pool
        .checkout(CheckoutClass::Reserved)
        .await
        .expect("checkout");
    let pid = backend_pid(&mut connection).await;
    let mut transaction = connection
        .begin(IsolationLevel::ServerDefault)
        .await
        .expect("begin");
    let id: i32 = 1;
    let _inserted = transaction
        .execute(
            &INSERT_SLOW_COMMIT.bind().value(&id).finish(),
            StatementBound::ConnectionTimeouts,
        )
        .await
        .expect("insert");
    let killer = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        database.terminate(pid).await;
    };
    let (outcome, ()) = tokio::join!(transaction.commit(), killer);
    match outcome {
        Err(StatementFailure::OutcomeUnknown { statement, .. }) => assert_eq!(statement, "commit"),
        other => panic!("expected OutcomeUnknown at commit, got {other:?}"),
    }
    drop(connection);
    pool.close();
    database.remove().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_open_transaction_discards_its_connection() {
    let database = TestDatabase::create().await;
    let pool = database.pool(
        PoolRole::Main,
        1,
        Reserve::NoReserve,
        StatementProtocol::PreparedPerConnection,
    );
    let mut connection = pool
        .checkout(CheckoutClass::Reserved)
        .await
        .expect("checkout");
    {
        let mut transaction = connection
            .begin(IsolationLevel::ServerDefault)
            .await
            .expect("begin");
        assert_eq!(insert_item(&mut transaction, 40).await.expect("insert"), 1);
    }
    match connection
        .query(&PID.bind().finish(), StatementBound::ConnectionTimeouts)
        .await
    {
        Err(StatementFailure::NotSent {
            cause: NotSentCause::ConnectionDiscarded,
            ..
        }) => {}
        other => panic!("expected ConnectionDiscarded, got {other:?}"),
    }
    drop(connection);
    assert_eq!(item_count(&pool, 40).await, 0);
    pool.close();
    database.remove().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_pool_runs_the_statement_pooling_check() {
    let database = TestDatabase::create().await;
    let pool = database.pool(
        PoolRole::Session,
        1,
        Reserve::NoReserve,
        StatementProtocol::PreparedPerConnection,
    );
    let mut connection = pool
        .checkout(CheckoutClass::Reserved)
        .await
        .expect("a direct URL keeps transactions");
    let _pid = backend_pid(&mut connection).await;
    drop(connection);
    pool.close();
    database.remove().await;
}

#[tokio::test]
#[cfg_attr(
    not(feature = "pgbouncer-fixture-tests"),
    ignore = "needs PgBouncer in statement mode, HORSIES_STMT_PGBOUNCER_STATEMENT_URL and the feature pgbouncer-fixture-tests"
)]
async fn a_statement_pooled_url_is_refused_at_every_first_checkout() {
    let url = std::env::var("HORSIES_STMT_PGBOUNCER_STATEMENT_URL")
        .expect("HORSIES_STMT_PGBOUNCER_STATEMENT_URL is set");
    let pool = CorePool::new(
        PoolPlan {
            role: PoolRole::Main,
            sizing: PoolSizing::new(2, Reserve::NoReserve).expect("valid sizing"),
            checkout_wait: Duration::from_secs(5),
            idle_ping: IdlePing::Off,
            max_age: ConnectionAge::Unlimited,
            protocol: StatementProtocol::UnnamedOnly,
        },
        load_plan(&url),
    );
    for attempt in 0..2 {
        match pool.checkout(CheckoutClass::Reserved).await {
            Err(failure @ CheckoutFailure::StatementPooledUrl { .. }) => {
                assert_eq!(
                    failure.transience(),
                    Transience::Permanent,
                    "attempt {attempt}"
                );
            }
            Err(other) => panic!("attempt {attempt}: expected StatementPooledUrl, got {other}"),
            Ok(_) => panic!("attempt {attempt}: a statement-pooled URL passed the check"),
        }
    }
    let session_pool = CorePool::new(
        PoolPlan {
            role: PoolRole::Session,
            sizing: PoolSizing::new(1, Reserve::NoReserve).expect("valid sizing"),
            checkout_wait: Duration::from_secs(5),
            idle_ping: IdlePing::Off,
            max_age: ConnectionAge::Unlimited,
            protocol: StatementProtocol::PreparedPerConnection,
        },
        load_plan(&url),
    );
    assert!(matches!(
        session_pool.checkout(CheckoutClass::Reserved).await,
        Err(CheckoutFailure::StatementPooledUrl { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg_attr(
    not(feature = "pgbouncer-fixture-tests"),
    ignore = "needs PgBouncer in transaction mode, HORSIES_STMT_PGBOUNCER_TRANSACTION_URL and the feature pgbouncer-fixture-tests"
)]
async fn unnamed_statements_stay_correct_under_transaction_pooling() {
    const CLIENTS: i64 = 32;
    const QUERIES_PER_CLIENT: i64 = 500;
    let url = std::env::var("HORSIES_STMT_PGBOUNCER_TRANSACTION_URL")
        .expect("HORSIES_STMT_PGBOUNCER_TRANSACTION_URL is set");
    let pool = Arc::new(CorePool::new(
        PoolPlan {
            role: PoolRole::Main,
            sizing: PoolSizing::new(16, Reserve::NoReserve).expect("valid sizing"),
            checkout_wait: Duration::from_secs(30),
            idle_ping: IdlePing::AfterIdle(Duration::from_secs(1)),
            max_age: ConnectionAge::Unlimited,
            protocol: StatementProtocol::UnnamedOnly,
        },
        load_plan(&url),
    ));
    let mut clients = Vec::new();
    for client in 0..CLIENTS {
        let pool = Arc::clone(&pool);
        clients.push(tokio::spawn(async move {
            for query in 0..QUERIES_PER_CLIENT {
                let base = query * 1_000;
                let rows = pool
                    .query(
                        CheckoutClass::Reserved,
                        &ADD.bind().value(&base).value(&client).finish(),
                        StatementBound::ConnectionTimeouts,
                    )
                    .await
                    .expect("query through PgBouncer");
                let [row] = rows.as_slice() else {
                    panic!("expected one row, got {}", rows.len());
                };
                let total: i64 = row.try_get("total").expect("total column");
                assert_eq!(total, base + client, "client {client} query {query}");
            }
        }));
    }
    for client in clients {
        client.await.expect("client task");
    }
}
