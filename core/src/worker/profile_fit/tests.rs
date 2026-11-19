//! The profile fit at worker boot, read from the log of a worker that runs
//! (Python `tests/unit/test_cli.py::TestBootConnectionFit`, the order of the
//! limit read and the start of the worker; `python/horsies/core/cli.py:886` and
//! `:955`, the memory fit before the schema step and the connection fit after
//! it).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::instrument::WithSubscriber;

use crate::broker::PostgresBroker;
use crate::core::config::child_sizing::DEFAULT_ISOLATED_POOL_CONNECTIONS;
use crate::core::config::worker_profile::SelectedProfile;
use crate::core::config::AppConfig;
use crate::test_support::migrated_database_url;
use crate::test_support::migrated_pool;
use crate::test_support::sql;
use crate::worker::children::ChildLaunch;
use crate::worker::claim_pass::RegisteredClasses;
use crate::worker::config::WorkerConfig;
use crate::worker::connection_fit::{
    LISTENER_CONNECTIONS_PER_WORKER, LIVENESS_LOCK_CONNECTIONS_PER_WORKER,
    RUNNER_HEARTBEAT_CONNECTIONS_PER_WORKER,
};
use crate::worker::executor::protocol::{
    ChildConnection, ChildLogLevel, ChildProgram, DatabaseUrl,
};
use crate::worker::executor::slots::scripted::idle_child_of_any_kind;
use crate::worker::worker::Worker;

/// The longest wait for the worker to log its start.
const BOOT_BOUND: Duration = Duration::from_secs(60);

/// How often the test reads the log while it waits.
const LOG_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// The longest wait for the run to end after its cancel.
const STOP_BOUND: Duration = Duration::from_secs(30);

/// The worker pool that the profile of the test sets: the largest a profile
/// takes, so one worker needs more connections than the test server has.
const OVERSIZED_WORKER_POOL: u32 = 1000;

/// The record of the database start (`Worker::start_database`).
const DATABASE_STARTED: &str = "task-history startup coverage ready";

/// The record of the start of the service loops (`Worker::serve`), before
/// the listener and the claim loop.
const WORKER_STARTING: &str = "worker starting";

/// The start of every profile fit record of the profile `default`.
const PROFILE_RECORD: &str = "Worker profile 'default': ";

/// One event of the log: its level and its message.
#[derive(Debug, Clone)]
struct LoggedEvent {
    level: tracing::Level,
    message: String,
}

/// A subscriber that keeps the level and the message of every event.
#[derive(Clone, Default)]
struct EventLog {
    events: Arc<Mutex<Vec<LoggedEvent>>>,
    last_span: Arc<AtomicU64>,
}

impl EventLog {
    fn events(&self) -> Vec<LoggedEvent> {
        self.events
            .lock()
            .expect("no test thread panicked while it held the event log")
            .clone()
    }

    fn has_message(&self, message: &str) -> bool {
        self.events().iter().any(|event| event.message == message)
    }
}

/// The `message` field of one event.
struct EventMessage(String);

impl tracing::field::Visit for EventMessage {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.0 = value.to_owned();
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

impl tracing::Subscriber for EventLog {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        // A span id is never 0.
        tracing::span::Id::from_u64(self.last_span.fetch_add(1, Ordering::Relaxed) + 1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut message = EventMessage(String::new());
        event.record(&mut message);
        self.events
            .lock()
            .expect("no test thread panicked while it held the event log")
            .push(LoggedEvent {
                level: *event.metadata().level(),
                message: message.0,
            });
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

/// The usable slots of the test server, read here and not through the code
/// under test.
async fn usable_slots_of_the_test_server() -> i64 {
    let usable: i32 = sql::query_scalar(
        "SELECT current_setting('max_connections')::int
                - current_setting('superuser_reserved_connections')::int
                - COALESCE(current_setting('reserved_connections', true), '0')::int",
    )
    .fetch_one(&migrated_pool().await)
    .await
    .expect("read the usable slots of the test server");
    i64::from(usable)
}

/// Python `TestBootConnectionFit::test_a_profile_above_the_limit_warns_and_the_worker_starts`:
/// a worker whose profile needs more connections than the test server has
/// logs, in this order, the memory fit of its profile, the database start,
/// one warning for its connections, and the start of its service loops.
/// The worker starts: the warning refuses nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn the_boot_logs_the_memory_fit_then_the_connection_fit_and_the_worker_starts() {
    // The parent pool, the listener, the runner heartbeat, the liveness lock,
    // and the pool of the one isolated child.
    let need = i64::from(OVERSIZED_WORKER_POOL)
        + i64::from(LISTENER_CONNECTIONS_PER_WORKER)
        + i64::from(RUNNER_HEARTBEAT_CONNECTIONS_PER_WORKER)
        + i64::from(LIVENESS_LOCK_CONNECTIONS_PER_WORKER)
        + i64::from(DEFAULT_ISOLATED_POOL_CONNECTIONS);
    let usable = usable_slots_of_the_test_server().await;
    assert!(
        need > usable,
        "the test server has {usable} usable slots, not fewer than the {need} of the profile"
    );
    let app_config = AppConfig::for_database_url(migrated_database_url().await);
    let broker = Arc::new(
        PostgresBroker::connect_worker_with(&app_config.broker, crate::ClientFamily::Rust)
            .await
            .expect("connect the worker broker to the test server"),
    );
    let launch = ChildLaunch {
        command: idle_child_of_any_kind(),
        preload: crate::worker::children::PreloadProgram::NoPreloadProgram,
        program: ChildProgram::Rust,
        log_level: ChildLogLevel::Info,
        database_url: DatabaseUrl::new(app_config.broker.database_url.clone()),
        connection: ChildConnection::of_config(&app_config.broker)
            .expect("the config of the test database validates"),
    };
    let profile = SelectedProfile {
        name: "default".to_owned(),
        profile: serde_json::from_value(serde_json::json!({
            "isolated": {"kind": "isolated_children", "count": 1},
            "pool": {"max_size": {"kind": "pool_size", "connections": OVERSIZED_WORKER_POOL}}
        }))
        .expect("a valid test profile"),
    };
    let worker = Worker::of_child_specs(
        broker,
        crate::test_support::child_specs::no_child_specs(),
        app_config,
        WorkerConfig {
            queues: vec!["default".to_owned()],
            ..WorkerConfig::default()
        },
        profile,
        launch,
        RegisteredClasses::default(),
    )
    .expect("a valid test worker");
    let cancel = worker.cancel_token();
    let log = EventLog::default();
    let run = tokio::spawn(async move { worker.run().await }.with_subscriber(log.clone()));

    let deadline = tokio::time::Instant::now() + BOOT_BOUND;
    while !log.has_message(WORKER_STARTING) && !run.is_finished() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the worker neither started nor ended within {BOOT_BOUND:?}: {:?}",
            log.events()
        );
        tokio::time::sleep(LOG_POLL_INTERVAL).await;
    }
    cancel.cancel();
    // What the run returns after its start is not what this test reads.
    let _ended = tokio::time::timeout(STOP_BOUND, run)
        .await
        .expect("the run ends after its cancel")
        .expect("the run does not panic");

    let events = log.events();
    let memory = events
        .iter()
        .rposition(|event| {
            event.message.starts_with(PROFILE_RECORD) && event.message.contains("memory")
        })
        .unwrap_or_else(|| panic!("no memory fit record: {events:?}"));
    let database = events
        .iter()
        .position(|event| event.message == DATABASE_STARTED)
        .unwrap_or_else(|| panic!("no database start record: {events:?}"));
    let connections: Vec<(usize, &LoggedEvent)> = events
        .iter()
        .enumerate()
        .filter(|(_, event)| {
            event.message.starts_with(PROFILE_RECORD) && event.message.contains("connection")
        })
        .collect();
    let starting = events
        .iter()
        .position(|event| event.message == WORKER_STARTING)
        .unwrap_or_else(|| panic!("no record of the start of the service loops: {events:?}"));
    let [(connection, warning)] = connections.as_slice() else {
        panic!("expected one connection fit record: {events:?}");
    };
    assert!(memory < database, "{events:?}");
    assert!(database < *connection, "{events:?}");
    assert!(*connection < starting, "{events:?}");
    assert_eq!(warning.level, tracing::Level::WARN, "{}", warning.message);
    assert!(
        warning.message.starts_with(&format!(
            "{PROFILE_RECORD}one worker opens up to {need} Postgres connections itself, not \
             counting task code; the server has {usable} usable slots"
        )),
        "{}",
        warning.message
    );
}
