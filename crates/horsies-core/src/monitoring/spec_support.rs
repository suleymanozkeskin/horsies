//! Databases and seeds of the monitoring read ports.
//!
//! Python specification: `tests/integration/test_monitoring_queries.py`
//! (baseline version on `program/0.8.0`), whose fixture empties every table
//! that the reads count. Facets and unfiltered reads count every row of the
//! database, so each test here runs on a migrated database of its own.
//!
//! Live rows name every column of the baseline task row. Terminal rows reach
//! history through the terminal writes (`broker::terminalization`), as
//! production writes them; a test changes a history fact afterwards only
//! where the move cannot write it (an error code on a COMPLETED row, a
//! status that is not a task status, an aged terminal time).

use std::str::FromStr;

use chrono::{DateTime, Duration, Utc};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgConnection, PgPool};
use uuid::Uuid;

use crate::broker::migrations::run_horsies_migrations;
use crate::broker::terminalization::terminalize;
use crate::broker::PostgresBroker;
use crate::core::history::maintenance::coverage::{ensure_partition_coverage, CoverageOutcome};
use crate::core::history::reads::pages::HistoryWindow;
use crate::core::history::reads::publisher::StagedLoaderPublisher;
use crate::core::lifecycle::{
    CallerHoldsRowLock, PriorLockedRead, TerminalizationCommand, TerminalizationOutcome,
};
use crate::monitoring::resolve_monitoring_window;
use crate::TaskStatus;

/// Every database of these tests has this name prefix, then 32 hex digits.
const DATABASE_PREFIX: &str = "core_monitoring_spec_";

/// The worker that moves a row when the seed names none: the move fences on
/// the claim holder, and the history row then drops the worker again.
const MOVING_WORKER: &str = "monitoring-spec-mover";

fn server_url() -> String {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return url;
    }
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let root = std::path::Path::new(manifest_dir)
        .ancestors()
        .find(|path| path.join(".env").exists());
    let password = root
        .and_then(|path| std::fs::read_to_string(path.join(".env")).ok())
        .and_then(|contents| {
            contents
                .lines()
                .filter_map(|line| line.trim().split_once('='))
                .find(|(key, _)| key.trim() == "DB_PASSWORD")
                .map(|(_, value)| value.trim().to_owned())
        })
        .unwrap_or_else(|| "W0rklane".to_owned());
    format!("postgresql://postgres:{password}@localhost:5432/horsies-core")
}

/// A migrated database with history coverage and published readers, for one
/// test. [`SpecDatabase::drop`] removes it; a test that panics before the
/// drop leaves it behind under [`DATABASE_PREFIX`].
pub(super) struct SpecDatabase {
    pub(super) pool: PgPool,
    name: String,
    admin_options: PgConnectOptions,
}

impl SpecDatabase {
    pub(super) async fn create() -> Self {
        let base_options =
            PgConnectOptions::from_str(&server_url()).expect("the test server URL parses");
        let admin_options = base_options.clone().database("postgres");
        let name = format!("{DATABASE_PREFIX}{}", Uuid::new_v4().simple());
        let mut admin = PgConnection::connect_with(&admin_options)
            .await
            .expect("connect to the admin database");
        sqlx::query(&format!("CREATE DATABASE \"{name}\""))
            .execute(&mut admin)
            .await
            .expect("create the test database");
        admin.close().await.expect("close the admin connection");
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&name))
            .await
            .expect("connect to the test database");
        run_horsies_migrations(&pool)
            .await
            .expect("migrate the test database");
        let mut transaction = pool.begin().await.expect("open the coverage transaction");
        let coverage =
            ensure_partition_coverage(&mut transaction, 2, 2, &[], &StagedLoaderPublisher)
                .await
                .expect("cover the history partitions");
        assert!(
            matches!(coverage, CoverageOutcome::Ensured(_)),
            "{coverage:?}"
        );
        transaction.commit().await.expect("commit the coverage");
        Self {
            pool,
            name,
            admin_options,
        }
    }

    pub(super) fn broker(&self) -> PostgresBroker {
        PostgresBroker::from_pool(self.pool.clone())
    }

    pub(super) async fn drop(self) {
        self.pool.close().await;
        let mut admin = PgConnection::connect_with(&self.admin_options)
            .await
            .expect("connect to the admin database");
        sqlx::query(&format!(
            "DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)",
            self.name
        ))
        .execute(&mut admin)
        .await
        .expect("drop the test database");
    }
}

/// A window wide enough that every seeded row is inside it (Python
/// `_test_window`: 29 days back, one hour ahead).
pub(super) fn window() -> HistoryWindow {
    let now = Utc::now();
    resolve_monitoring_window(
        Some(now - Duration::days(29)),
        Some(now + Duration::hours(1)),
        None,
    )
    .expect("the test window is accepted")
}

/// The live status of a seeded row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LiveStatus {
    Pending,
    Claimed,
    Running,
}

impl LiveStatus {
    fn as_sql(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Claimed => "CLAIMED",
            Self::Running => "RUNNING",
        }
    }
}

/// One live task row. `started_seconds_ago` is set for a RUNNING row only;
/// a CLAIMED row's lease runs one hour.
#[derive(Debug, Clone)]
pub(super) struct LiveTask {
    pub(super) id: Uuid,
    pub(super) task_name: String,
    pub(super) queue_name: String,
    pub(super) status: LiveStatus,
    pub(super) worker: Option<String>,
    pub(super) worker_hostname: Option<String>,
    pub(super) retry_count: i32,
    pub(super) enqueued_seconds_ago: i64,
    pub(super) started_seconds_ago: Option<i64>,
    pub(super) good_until: Option<DateTime<Utc>>,
    pub(super) is_workflow_task: bool,
}

impl LiveTask {
    /// A PENDING row of `task_name` on `default`, enqueued 60 s ago, with no
    /// worker (Python `make_task` defaults).
    pub(super) fn pending(task_name: &str) -> Self {
        Self {
            id: Uuid::new_v4(),
            task_name: task_name.to_owned(),
            queue_name: "default".to_owned(),
            status: LiveStatus::Pending,
            worker: None,
            worker_hostname: None,
            retry_count: 0,
            enqueued_seconds_ago: 60,
            started_seconds_ago: None,
            good_until: None,
            is_workflow_task: false,
        }
    }

    /// A CLAIMED row of `worker`.
    pub(super) fn claimed(task_name: &str, worker: &str) -> Self {
        Self {
            status: LiveStatus::Claimed,
            worker: Some(worker.to_owned()),
            ..Self::pending(task_name)
        }
    }

    /// A RUNNING row of `worker`, started 30 s ago.
    pub(super) fn running(task_name: &str, worker: &str) -> Self {
        Self {
            status: LiveStatus::Running,
            worker: Some(worker.to_owned()),
            started_seconds_ago: Some(30),
            ..Self::pending(task_name)
        }
    }

    pub(super) fn on_queue(mut self, queue_name: &str) -> Self {
        self.queue_name = queue_name.to_owned();
        self
    }

    pub(super) fn with_worker(mut self, worker: Option<&str>) -> Self {
        self.worker = worker.map(str::to_owned);
        self
    }

    pub(super) fn with_retry_count(mut self, retry_count: i32) -> Self {
        self.retry_count = retry_count;
        self
    }
}

/// Insert one live row. Every baseline column is named.
pub(super) async fn seed_live(pool: &PgPool, task: &LiveTask) {
    sqlx::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, task_options, execution,
            retention_class_key, sent_at, created_at,
            command_fingerprint_version, command_fingerprint, idempotency_key_digest,
            status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
            started_at, finalizing_at, finalizing_by_worker_id,
            worker_pid, worker_hostname, worker_process_name,
            retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
            updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
        ) VALUES (
            $1, $2, $3, 100, '{}', NULL, 'ISOLATED',
            'forever', NOW(), NOW(),
            CASE WHEN $9 THEN NULL ELSE 1 END,
            CASE WHEN $9 THEN NULL ELSE decode(repeat('0a', 32), 'hex') END, NULL,
            $4, NOW() - make_interval(secs => $5),
            CASE WHEN $6::text IS NULL THEN NULL ELSE NOW() END, $6,
            CASE WHEN $4 = 'CLAIMED' THEN NOW() + interval '1 hour' END,
            CASE WHEN $7::bigint IS NULL THEN NULL ELSE NOW() - make_interval(secs => $7) END,
            NULL, NULL,
            NULL, $10, NULL,
            $8, 3, NULL, $11, $9,
            NOW(), FALSE, NULL, NULL
        )",
    )
    .bind(task.id)
    .bind(&task.task_name)
    .bind(&task.queue_name)
    .bind(task.status.as_sql())
    .bind(task.enqueued_seconds_ago)
    .bind(&task.worker)
    .bind(task.started_seconds_ago)
    .bind(task.retry_count)
    .bind(task.is_workflow_task)
    .bind(&task.worker_hostname)
    .bind(task.good_until)
    .execute(pool)
    .await
    .expect("seed a live task row");
}

/// How a seeded task ends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Ending {
    /// COMPLETED.
    Completed,
    /// FAILED with this stored error code.
    Failed { error_code: Option<String> },
    /// CANCELLED by an operator before it started (error code
    /// `TASK_CANCELLED`).
    Cancelled,
}

impl Ending {
    pub(super) fn failed(error_code: &str) -> Self {
        Self::Failed {
            error_code: Some(error_code.to_owned()),
        }
    }
}

fn assert_applied(outcome: &[TerminalizationOutcome], task_id: Uuid) {
    assert!(
        matches!(outcome, [TerminalizationOutcome::Applied { .. }]),
        "the terminal write of {task_id} did not apply: {outcome:?}"
    );
}

/// Seed `task` RUNNING under its worker, or under [`MOVING_WORKER`] when it
/// names none, and return the worker that holds it.
async fn seed_running(pool: &PgPool, task: &LiveTask) -> String {
    let worker = task
        .worker
        .clone()
        .unwrap_or_else(|| MOVING_WORKER.to_owned());
    let mut running = task.clone();
    running.status = LiveStatus::Running;
    running.worker = Some(worker.clone());
    running.started_seconds_ago = Some(running.started_seconds_ago.unwrap_or(30));
    seed_live(pool, &running).await;
    worker
}

/// Seed `task` live and move it to history through the terminal write of
/// `ending`. A completed or failed task runs under its worker first; a
/// task with no worker runs under [`MOVING_WORKER`], and its history row
/// then names no worker. A cancelled task is cancelled from PENDING.
pub(super) async fn seed_terminal(pool: &PgPool, task: &LiveTask, ending: Ending) {
    seed_terminal_with_attempts(pool, task, ending, &[]).await;
}

/// [`seed_terminal`], with attempt rows written between the live seed and
/// the move, so the move folds them into the attempt snapshot. Each entry
/// is `(attempt, will_retry, text)`.
pub(super) async fn seed_terminal_with_attempts(
    pool: &PgPool,
    task: &LiveTask,
    ending: Ending,
    attempts: &[(i32, bool, AttemptText)],
) {
    let command = match &ending {
        Ending::Cancelled => {
            let mut pending = task.clone();
            pending.status = LiveStatus::Pending;
            pending.started_seconds_ago = None;
            seed_live(pool, &pending).await;
            TerminalizationCommand::CancelLockedTask {
                task_id: task.id,
                fence: CallerHoldsRowLock,
                permitted_source_statuses: vec![TaskStatus::Pending],
            }
        }
        Ending::Completed => TerminalizationCommand::CompleteLockedTask {
            task_id: task.id,
            fence: PriorLockedRead {
                worker_id: seed_running(pool, task).await,
            },
            result_json: r#"{"ok": true}"#.to_owned(),
        },
        Ending::Failed { error_code } => TerminalizationCommand::FailLockedTask {
            task_id: task.id,
            fence: PriorLockedRead {
                worker_id: seed_running(pool, task).await,
            },
            result_json: r#"{"err": {"error_code": "SEEDED"}}"#.to_owned(),
            error_code: error_code.clone(),
            failed_reason: Some("seeded failure".to_owned()),
        },
    };
    for (attempt, will_retry, text) in attempts {
        seed_attempt(pool, task.id, *attempt, *will_retry, text).await;
    }
    let outcome = terminalize(pool, &command)
        .await
        .expect("run the terminal write");
    assert_applied(&outcome, task.id);
    let moved_by_placeholder = match ending {
        Ending::Cancelled => false,
        Ending::Completed | Ending::Failed { .. } => task.worker.is_none(),
    };
    if moved_by_placeholder {
        sqlx::query(
            "UPDATE horsies_task_history SET last_claimed_worker_id = NULL WHERE task_id = $1",
        )
        .bind(task.id)
        .execute(pool)
        .await
        .expect("drop the moving worker from the history row");
    }
}

/// Store `error_code` on the history row of `task_id`.
pub(super) async fn set_history_error_code(pool: &PgPool, task_id: Uuid, error_code: Option<&str>) {
    sqlx::query("UPDATE horsies_task_history SET error_code = $2 WHERE task_id = $1")
        .bind(task_id)
        .bind(error_code)
        .execute(pool)
        .await
        .expect("store the history error code");
}

/// Store `status` on the history row of `task_id`. The history table has no
/// status CHECK, so a value that is not a task status can be stored, as
/// Python's `stored_history_status`.
pub(super) async fn set_history_status(pool: &PgPool, task_id: Uuid, status: &str) {
    sqlx::query("UPDATE horsies_task_history SET status = $2 WHERE task_id = $1")
        .bind(task_id)
        .bind(status)
        .execute(pool)
        .await
        .expect("store the history status");
}

/// Fail one task per code, each under its own task name `{prefix}{index}`.
pub(super) async fn seed_failures(pool: &PgPool, prefix: &str, error_codes: &[String]) {
    for (index, error_code) in error_codes.iter().enumerate() {
        let task = LiveTask::pending(&format!("{prefix}{index}"));
        seed_terminal(pool, &task, Ending::failed(error_code)).await;
    }
}

/// Date the history row of `task_id`: enqueued, started (or never started)
/// and ended the given number of seconds ago.
pub(super) async fn set_history_times(
    pool: &PgPool,
    task_id: Uuid,
    enqueued_seconds_ago: i64,
    started_seconds_ago: Option<i64>,
    terminal_seconds_ago: i64,
) {
    sqlx::query(
        "UPDATE horsies_task_history
         SET enqueued_at = NOW() - make_interval(secs => $2),
             started_at = CASE WHEN $3::bigint IS NULL THEN NULL
                               ELSE NOW() - make_interval(secs => $3) END,
             terminal_at = NOW() - make_interval(secs => $4)
         WHERE task_id = $1",
    )
    .bind(task_id)
    .bind(enqueued_seconds_ago)
    .bind(started_seconds_ago)
    .bind(terminal_seconds_ago)
    .execute(pool)
    .await
    .expect("date the history row");
}

/// The text columns of one attempt row.
#[derive(Debug, Clone)]
pub(super) struct AttemptText {
    pub(super) error_code: Option<String>,
    pub(super) error_message: Option<String>,
    pub(super) failed_reason: Option<String>,
}

/// Insert attempt `attempt` of a live task: FAILED, started 50 s and
/// finished 40 s ago. A later move folds it into the attempt snapshot.
pub(super) async fn seed_attempt(
    pool: &PgPool,
    task_id: Uuid,
    attempt: i32,
    will_retry: bool,
    text: &AttemptText,
) {
    sqlx::query(
        "INSERT INTO horsies_task_attempts (
            task_id, attempt, outcome, will_retry, started_at, finished_at,
            error_code, error_message, failed_reason, worker_id,
            worker_hostname, worker_pid, worker_process_name
        ) VALUES (
            $1, $2, 'FAILED', $3, NOW() - interval '50 seconds', NOW() - interval '40 seconds',
            $4, $5, $6, 'worker-1',
            'host-1', 4242, 'monitoring-spec-process'
        )",
    )
    .bind(task_id)
    .bind(attempt)
    .bind(will_retry)
    .bind(&text.error_code)
    .bind(&text.error_message)
    .bind(&text.failed_reason)
    .execute(pool)
    .await
    .expect("seed an attempt row");
}

/// One workflow run row. A child run hangs off node 0 of its parent.
#[derive(Debug, Clone)]
pub(super) struct RunSeed {
    pub(super) id: Uuid,
    pub(super) name: String,
    pub(super) status: String,
    pub(super) parent: Option<Uuid>,
    pub(super) created_seconds_ago: i64,
    pub(super) completed_seconds_ago: Option<i64>,
}

impl RunSeed {
    /// A RUNNING root run created 120 s ago (Python `make_workflow`).
    pub(super) fn running(name: &str) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.to_owned(),
            status: "RUNNING".to_owned(),
            parent: None,
            created_seconds_ago: 120,
            completed_seconds_ago: None,
        }
    }
}

/// Insert one workflow run. Every baseline column is named.
pub(super) async fn seed_run(pool: &PgPool, run: &RunSeed) {
    sqlx::query(
        "INSERT INTO horsies_workflows (
            id, name, definition_key, status, on_error, output_task_index, success_policy,
            result, error, parent_workflow_id, parent_task_index, root_workflow_id, depth,
            sent_at, created_at, started_at, completed_at, updated_at
        ) VALUES (
            $1, $2, 'tests.alpha.v1', $3, 'fail', NULL, NULL,
            NULL, NULL, $4, CASE WHEN $4::uuid IS NULL THEN NULL ELSE 0 END,
            COALESCE($4, $1), CASE WHEN $4::uuid IS NULL THEN 0 ELSE 1 END,
            NOW() - make_interval(secs => $5), NOW() - make_interval(secs => $5),
            NOW() - make_interval(secs => $5),
            CASE WHEN $6::bigint IS NULL THEN NULL ELSE NOW() - make_interval(secs => $6) END,
            NOW()
        )",
    )
    .bind(run.id)
    .bind(&run.name)
    .bind(&run.status)
    .bind(run.parent)
    .bind(run.created_seconds_ago)
    .bind(run.completed_seconds_ago)
    .execute(pool)
    .await
    .expect("seed a workflow run");
}

/// What a workflow node runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NodeKind {
    /// A task node, backed by this task row once it is enqueued.
    Task { task_id: Option<Uuid> },
    /// A sub-workflow node, linked to this child run once it is started.
    SubWorkflow { child: Option<Uuid> },
}

/// One workflow node row.
#[derive(Debug, Clone)]
pub(super) struct NodeSeed {
    pub(super) workflow_id: Uuid,
    pub(super) task_index: i32,
    pub(super) node_id: Option<String>,
    pub(super) status: String,
    pub(super) dependencies: Vec<i32>,
    pub(super) kind: NodeKind,
    pub(super) result: Option<String>,
    pub(super) started_seconds_ago: Option<i64>,
    pub(super) completed_seconds_ago: Option<i64>,
}

impl NodeSeed {
    /// A PENDING task node with no backing task and no dependency.
    pub(super) fn task(workflow_id: Uuid, task_index: i32) -> Self {
        Self {
            workflow_id,
            task_index,
            node_id: None,
            status: "PENDING".to_owned(),
            dependencies: Vec::new(),
            kind: NodeKind::Task { task_id: None },
            result: None,
            started_seconds_ago: None,
            completed_seconds_ago: None,
        }
    }
}

/// Insert one workflow node of task `alpha_task`. Every baseline column is
/// named.
pub(super) async fn seed_node(pool: &PgPool, node: &NodeSeed) {
    let (is_subworkflow, task_id, child) = match node.kind {
        NodeKind::Task { task_id } => (false, task_id, None),
        NodeKind::SubWorkflow { child } => (true, None, child),
    };
    sqlx::query(
        "INSERT INTO horsies_workflow_tasks (
            downstream_subworkflow,
            id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
            dependencies, args_from, workflow_ctx_from, allow_failed_deps, join_type,
            min_success, task_options, max_retries, execution, good_until,
            retention_class_key, status, task_id, is_subworkflow, sub_workflow_id,
            sub_workflow_name, sub_definition_key, sub_workflow_summary, result, error,
            created_at, started_at, completed_at
        ) VALUES (
            'NONE', $1, $2, $3, $4, 'alpha_task', '{}', 'default', 100,
            $5, NULL, NULL, FALSE, 'all',
            NULL, NULL, CASE WHEN $8 THEN NULL ELSE 0 END,
            CASE WHEN $8 THEN NULL ELSE 'ISOLATED' END, NULL,
            CASE WHEN $8 THEN NULL ELSE 'forever' END, $6, $7, $8, $9,
            NULL, NULL, NULL, $10, NULL,
            NOW() - interval '120 seconds',
            CASE WHEN $11::bigint IS NULL THEN NULL ELSE NOW() - make_interval(secs => $11) END,
            CASE WHEN $12::bigint IS NULL THEN NULL ELSE NOW() - make_interval(secs => $12) END
        )",
    )
    .bind(Uuid::new_v4())
    .bind(node.workflow_id)
    .bind(node.task_index)
    .bind(&node.node_id)
    .bind(&node.dependencies)
    .bind(&node.status)
    .bind(task_id)
    .bind(is_subworkflow)
    .bind(child)
    .bind(&node.result)
    .bind(node.started_seconds_ago)
    .bind(node.completed_seconds_ago)
    .execute(pool)
    .await
    .expect("seed a workflow node");
}
