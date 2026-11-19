//! Port of Python `clients/python/tests/integration/test_class_coverage.py`
//! `TestReaperPassLogsUnservedStreams::test_the_pass_logs_an_unserved_isolated_retry`.
//!
//! The test enters where a worker does: it spawns the reaper loop and reads
//! the log lines of the loop. Nothing in the test calls the fleet check. The
//! fleet check reads every snapshot and every pending row, so the test has a
//! database of its own.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{new_reaper_health, spawn_reaper};
use crate::broker::monitoring_test_database::MonitoringTestDatabase;
use crate::core::config::recovery::RecoveryConfig;
use crate::core::config::retention::RetentionConfig;
use crate::core::history::maintenance::coverage::ensure_startup_coverage_in_pool;
use crate::test_support::sql::{self, TestPool};

/// The least reaper check interval. The loop reads the fleet check from one
/// interval after its start on.
const CHECK_INTERVAL_MS: u64 = 1_000;

/// How long the test waits for the line.
const LINE_WAIT_BOUND: Duration = Duration::from_secs(60);

/// How often the test reads the captured lines.
const LINE_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The start of the error line of an unserved `ISOLATED` stream on queue `a`.
const UNSERVED_ISOLATED_LINE: &str =
    "PENDING ISOLATED tasks on queue 'a' wait: no live worker serves this class";

/// The WARN and ERROR events of the thread that installs it, as level and
/// message text.
#[derive(Clone, Default)]
struct CapturedLines(Arc<Mutex<Vec<(tracing::Level, String)>>>);

impl CapturedLines {
    fn has_error_starting_with(&self, start: &str) -> bool {
        self.0
            .lock()
            .expect("the captured lines")
            .iter()
            .any(|(level, text)| *level == tracing::Level::ERROR && text.starts_with(start))
    }
}

/// The `message` field of an event as text.
#[derive(Default)]
struct MessageText(String);

impl tracing::field::Visit for MessageText {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

impl tracing::Subscriber for CapturedLines {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() <= tracing::Level::WARN
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut message = MessageText::default();
        event.record(&mut message);
        self.0
            .lock()
            .expect("the captured lines")
            .push((*event.metadata().level(), message.0));
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

/// A snapshot of `w-async` on queue `a`, written now, whose one budget runs
/// only `SHARED_ASYNC` rows.
async fn async_children_snapshot(pool: &TestPool) {
    sql::query(
        "INSERT INTO horsies_worker_states (
            worker_id, snapshot_at, hostname, pid, processes,
            max_claim_batch, max_claim_per_worker, queues,
            queue_priorities, queue_max_concurrency, recovery_config,
            tasks_running, tasks_claimed, worker_started_at,
            slots_down, class_capacity
        ) VALUES (
            'w-async', now(), 'itest-host', 4242, 4,
            0, 4, ARRAY['a'],
            '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
            0, 0, now() - interval '1 hour',
            '[]'::jsonb,
            '{\"SHARED_ASYNC\": {\"capacity\": 40, \"serves\": [\"SHARED_ASYNC\"]}}'::jsonb
        )",
    )
    .execute(pool)
    .await
    .expect("insert the worker snapshot");
}

/// A `PENDING` `ISOLATED` row on queue `a`, due 10 s ago: a shared task that
/// went back to the queue as `ISOLATED` waits as this row does.
async fn pending_isolated_row(pool: &TestPool) {
    sql::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, execution, retention_class_key,
            sent_at, created_at, command_fingerprint_version, command_fingerprint,
            status, enqueued_at, retry_count, max_retries, is_workflow_task, updated_at,
            retain_rerun_input
        ) VALUES (
            $1, 'class_task', 'a', 100, 'null', 'ISOLATED', 'forever',
            now(), now(), 1, decode(repeat('0c', 32), 'hex'),
            'PENDING', now() - interval '10 seconds', 0, 0, FALSE, now(),
            FALSE
        )",
    )
    .bind(Uuid::new_v4())
    .execute(pool)
    .await
    .expect("insert the pending row");
}

/// Python `test_the_pass_logs_an_unserved_isolated_retry`: a live worker
/// that runs only `SHARED_ASYNC` children serves queue `a`, and an
/// `ISOLATED` row waits there. The started reaper loop logs the stream at
/// error, with no direct call of the fleet check.
#[tokio::test]
async fn a_started_reaper_logs_an_unserved_isolated_stream() {
    let database = MonitoringTestDatabase::create().await;
    let retention = RetentionConfig::default();
    ensure_startup_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        retention.history_leaf_horizon_days,
        retention.heartbeat_leaf_horizon_hours,
        &[],
    )
    .await
    .expect("startup coverage");
    async_children_snapshot(&database.pool).await;
    pending_isolated_row(&database.pool).await;
    let captured = CapturedLines::default();
    let capture = tracing::subscriber::set_default(captured.clone());
    let cancel = CancellationToken::new();
    // A failed fleet check would log a warning, not this error.
    let reaper = spawn_reaper(
        Arc::new(crate::broker::PostgresBroker::from_test_url(
            database.pool.url(),
            crate::ClientFamily::Rust,
        )),
        crate::test_support::child_specs::no_child_specs(),
        RecoveryConfig {
            check_interval_ms: CHECK_INTERVAL_MS,
            ..RecoveryConfig::default()
        },
        retention,
        new_reaper_health(),
        crate::telemetry::worker::WorkerTelemetry::Off,
        cancel.clone(),
    );

    let logged = tokio::time::timeout(LINE_WAIT_BOUND, async {
        while !captured.has_error_starting_with(UNSERVED_ISOLATED_LINE) {
            tokio::time::sleep(LINE_POLL_INTERVAL).await;
        }
    })
    .await;
    cancel.cancel();
    reaper.await.expect("the reaper loop ends");
    drop(capture);
    database.drop().await;

    logged.unwrap_or_else(|_elapsed| {
        panic!(
            "no error line starting with {UNSERVED_ISOLATED_LINE:?} within {LINE_WAIT_BOUND:?} \
             of the reaper start; the reaper loop does not log unserved streams"
        )
    });
}
