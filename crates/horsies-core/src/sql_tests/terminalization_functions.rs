//! The terminalization functions on the installed schema, at the SQL level.
//!
//! - The refusals that the database decides before it changes a row (the
//!   live status check, the batch bounds, the id-keyed batch arrays). Each
//!   case has a database of its own, a copy of the installed schema
//!   ([`TestDatabase::create`]), and seeds one task row. The function call
//!   fails with its error, and the task row keeps its status.
//! - The outcomes of the writers of COMPLETE_LOCKED
//!   (`horsies_finish_workflow_success`), FAIL_RUNNING
//!   (`horsies_finish_failed_attempt`), FAIL_STALE
//!   (`horsies_finish_stale_attempt`) and CANCEL_ADMIN
//!   (`horsies_cancel_task`). Each test has a copy of the installed schema
//!   with one coverage pass ([`covered`]) and seeds its rows. It calls the
//!   writer with its own SQL and decodes the one outcome row with the
//!   decoder of the core ([`decode_outcome_row`]).
//!
//! The other tests of `tests/integration/test_terminalization_functions.py`
//! stay in Python. Each runs a call through the Python lifecycle
//! (`apply_async`, `apply_batch_async`, `apply_sync`, `call_for`,
//! `batch_call_for`).

use chrono::{DateTime, TimeZone, Utc};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::ToSql;
use tokio_postgres::Row;
use uuid::Uuid;

use super::harness::{describe, Session};
use crate::core::history::archive::attempts::{decode_attempt_snapshot, encode_attempt_snapshot};
use crate::core::history::archive::versions::{JSON_CONTENT_TYPE, JSON_UTF8_CODEC};
use crate::core::lifecycle::{decode_outcome_row, TerminalizationOutcome};
use crate::test_support::TestDatabase;

mod already_applied;
mod applied_transitions;
mod cancel_task;
mod fail_locked;
mod fail_stale;
mod refusals;

const WORKER: &str = "terminalization-test-worker";

/// The worker that holds the claim in the lost-claim cases.
const OTHER_WORKER: &str = "someone-else";

/// The result that the writer of COMPLETE_LOCKED stores.
const LOCKED_RESULT: &str = r#"{"ok": 1}"#;

/// The result that the writer of FAIL_RUNNING stores.
const FAIL_RUNNING_RESULT: &str = r#"{"err": 1}"#;

/// The error code that the writer of FAIL_RUNNING stores.
const FAIL_RUNNING_ERROR_CODE: &str = "TASK_EXCEPTION";

/// The result that the writer of FAIL_STALE stores.
const FAIL_STALE_RESULT: &str = r#"{"err": 2}"#;

/// The error code that the writer of FAIL_STALE stores.
const FAIL_STALE_ERROR_CODE: &str = "WORKER_CRASHED";

/// The failed reason, attempt message and attempt reason that the writer of
/// FAIL_STALE stores.
const FAIL_STALE_REASON: &str = "Worker crashed";

/// The error code that the writer of CANCEL_ADMIN stores.
const CANCEL_ADMIN_ERROR_CODE: &str = "TASK_CANCELLED";

/// The failed reason that the writer of CANCEL_ADMIN stores.
const CANCEL_ADMIN_REASON: &str = "Cancelled via monitoring API";

/// The time after which a silent runner is stale, in the stale cases.
const STALE_AFTER_MS: i32 = 60_000;

/// The time after which a silent finalizer is stale, in the stale cases.
const FINALIZING_STALE_AFTER_MS: i32 = 30_000;

/// [`STALE_AFTER_MS`] in seconds.
const STALE_AFTER_SECONDS: i32 = STALE_AFTER_MS / 1000;

/// [`FINALIZING_STALE_AFTER_MS`] in seconds.
const FINALIZING_STALE_AFTER_SECONDS: i32 = FINALIZING_STALE_AFTER_MS / 1000;

/// The failed reason of attempt one in the cases that requeue a task after
/// a failed attempt.
const ATTEMPT_ONE_REASON: &str = "attempt one crashed";

/// The retention class that every enqueue stamps unless the caller says
/// otherwise.
const DEFAULT_RETENTION_CLASS_KEY: &str = "standard_30d";

/// Every baseline column is named. A sent task carries a command
/// fingerprint; a workflow task carries none. A CLAIMED row carries a lease.
/// A row with a worker carries the worker identity that the start write
/// records. Parameters: `$1` id, `$2` retention class, `$3` workflow task,
/// `$4` status, `$5` claimed_at, `$6` worker id.
const SEED_SQL: &str = "
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution,
        retention_class_key, sent_at, created_at,
        command_fingerprint_version, command_fingerprint,
        idempotency_key_digest, status, enqueued_at, claimed_at,
        claimed_by_worker_id, claim_expires_at, started_at, finalizing_at,
        finalizing_by_worker_id, worker_pid, worker_hostname,
        worker_process_name, retry_count, max_retries, next_retry_at,
        good_until, is_workflow_task, updated_at, retain_rerun_input,
        rerun_of_task_id, rerun_root_task_id
    )
    VALUES (
        CAST($1 AS uuid), 'terminalization.test', 'default', 100, '{}', NULL,
        'ISOLATED',
        $2, NOW(), NOW(),
        CASE WHEN $3 THEN NULL ELSE 1 END,
        CASE
            WHEN $3 THEN NULL
            ELSE sha256(convert_to(CAST(CAST($1 AS uuid) AS text), 'UTF8'))
        END,
        NULL, CAST($4 AS text), NOW(), $5,
        CAST($6 AS text),
        CASE
            WHEN CAST($4 AS text) = 'CLAIMED'
            THEN NOW() + INTERVAL '5 minutes'
        END,
        NOW(), NULL,
        NULL,
        CASE WHEN CAST($6 AS text) IS NOT NULL THEN 4242 END,
        CASE
            WHEN CAST($6 AS text) IS NOT NULL
            THEN 'terminalization-host'
        END,
        CASE
            WHEN CAST($6 AS text) IS NOT NULL
            THEN 'terminalization-proc'
        END,
        0, 0, NULL,
        NULL, $3, NOW(), FALSE,
        NULL, NULL
    )
";

/// Sets the row's good_until `$2` seconds before now. Parameter `$1` is the
/// task id.
const DEADLINE_SQL: &str = "
    UPDATE horsies_tasks
    SET good_until = CASE
            WHEN CAST($2 AS INTEGER) IS NULL THEN NULL
            ELSE NOW() - make_interval(secs => $2)
        END
    WHERE id = $1
";

/// The status of a task on whichever side holds it: the live row or its
/// history row.
const STATUS_SQL: &str = "
    SELECT status FROM horsies_tasks WHERE id = $1
    UNION ALL
    SELECT status FROM horsies_task_history WHERE task_id = $1
";

/// The task as one row on whichever side holds it, with the live column
/// names (the `itest_task_rows` view of the Python suites). A live row has
/// no terminal columns: its error, failed reason, terminal times, result and
/// kind read NULL. `claimed` reads FALSE on both sides. `failed_at` is the
/// terminal instant of a history row that is not COMPLETED. Parameter `$1`
/// is the task id.
const TASK_ROW_SQL: &str = "
    SELECT status, NULL::text AS error_code, NULL::text AS failed_reason,
           NULL::timestamptz AS failed_at, NULL::timestamptz AS terminal_at,
           NULL::text AS terminalization_kind, NULL::text AS result,
           FALSE AS claimed, claimed_by_worker_id, claimed_at,
           finalizing_at, finalizing_by_worker_id
    FROM horsies_tasks WHERE id = $1
    UNION ALL
    SELECT status, error_code, final_failed_reason AS failed_reason,
           CASE WHEN status <> 'COMPLETED' THEN terminal_at END AS failed_at,
           terminal_at, terminalization_kind,
           convert_from(result_payload, 'UTF8') AS result,
           FALSE AS claimed, NULL AS claimed_by_worker_id, claimed_at,
           NULL::timestamptz AS finalizing_at, NULL AS finalizing_by_worker_id
    FROM horsies_task_history WHERE task_id = $1
";

/// The writer of COMPLETE_LOCKED: phase 1 of the success of a workflow task.
/// The ten columns are the outcome row that every writer returns first.
/// Parameters: `$1` task id, `$2` worker id, `$3` claimed_at, `$4` result.
const WORKFLOW_SUCCESS_SQL: &str = "
    SELECT task_id, ordinality, outcome, terminal_at, terminalization_kind,
           observed_status, observed_worker_id, observed_claimed_at,
           guard_kind, observed_guard
    FROM horsies_finish_workflow_success(
        CAST($1 AS uuid), $2, CAST($3 AS TIMESTAMPTZ), $4
    )
";

/// The writer of FAIL_RUNNING: a failed attempt with no retry (retry kind
/// NONE, a CLAIMED row refused). The attempt row records the failed reason
/// too. Parameters: `$1` task id, `$2` worker id, `$3` claimed_at, `$4`
/// result, `$5` error code, `$6` failed reason.
const FAILED_ATTEMPT_SQL: &str = "
    SELECT task_id, ordinality, outcome, terminal_at, terminalization_kind,
           observed_status, observed_worker_id, observed_claimed_at,
           guard_kind, observed_guard
    FROM horsies_finish_failed_attempt(
        CAST($1 AS uuid), $2, CAST($3 AS TIMESTAMPTZ),
        'NONE', NULL, NULL, NULL, $4, $5, $6,
        'FAILED', NULL, $6, 'REFUSE', TRUE
    )
";

/// The writer of FAIL_STALE: the stale RUNNING recovery with no retry
/// (retry kind NONE). Parameters: `$1` task id, `$2` stale_after_ms, `$3`
/// finalizing_stale_after_ms, `$4` result, `$5` error code, `$6` failed
/// reason, which is the attempt message and the attempt reason too.
const STALE_ATTEMPT_SQL: &str = "
    SELECT task_id, ordinality, outcome, terminal_at, terminalization_kind,
           observed_status, observed_worker_id, observed_claimed_at,
           guard_kind, observed_guard
    FROM horsies_finish_stale_attempt(
        CAST($1 AS uuid), CAST($2 AS INTEGER), CAST($3 AS INTEGER),
        'NONE', NULL, NULL,
        $4, $5, $6, $6, $6
    )
";

/// The writer of CANCEL_ADMIN: the administrative cancel of a plain task.
/// Parameters: `$1` task id, `$2` whether a RUNNING row may be cancelled.
const CANCEL_TASK_SQL: &str = "
    SELECT task_id, ordinality, outcome, terminal_at, terminalization_kind,
           observed_status, observed_worker_id, observed_claimed_at,
           guard_kind, observed_guard
    FROM horsies_cancel_task(CAST($1 AS uuid), CAST($2 AS BOOLEAN))
";

/// Sets the row's started_at `$2` seconds and finalizing_at `$3` seconds
/// before now; NULL clears the column. Parameter `$1` is the task id.
const AGE_SQL: &str = "
    UPDATE horsies_tasks
    SET started_at = CASE
            WHEN CAST($2 AS INTEGER) IS NULL THEN NULL
            ELSE NOW() - make_interval(secs => $2)
        END,
        finalizing_at = CASE
            WHEN CAST($3 AS INTEGER) IS NULL THEN NULL
            ELSE NOW() - make_interval(secs => $3)
        END
    WHERE id = $1
";

/// A heartbeat of the task, sent `$3` seconds before now in the role `$2`.
/// Parameter `$1` is the task id.
const HEARTBEAT_SQL: &str = "
    INSERT INTO horsies_heartbeats (
        task_id, sender_id, role, sent_at, hostname, pid
    )
    VALUES (
        $1, 'staleness-test', $2,
        NOW() - make_interval(secs => CAST($3 AS INTEGER)),
        'terminalization-test-host', 4242
    )
";

/// A RUNNING workflow. Parameter `$1` is the workflow id, which is its root
/// id too.
const WORKFLOW_SQL: &str = "
    INSERT INTO horsies_workflows (
        id, name, definition_key, status, on_error, output_task_index,
        success_policy, result, error, parent_workflow_id,
        parent_task_index, root_workflow_id, depth, sent_at,
        created_at, started_at, completed_at, updated_at
    )
    VALUES (
        $1, 'terminalization.test', NULL, 'RUNNING', 'fail',
        NULL, NULL, NULL, NULL, NULL,
        NULL, $1, 0, NOW(),
        NOW(), NOW(), NULL, NOW()
    )
";

/// The RUNNING node that links a task to its workflow. Parameters: `$1`
/// node id, `$2` workflow id, `$3` retention class, `$4` task id.
const WORKFLOW_NODE_SQL: &str = "
    INSERT INTO horsies_workflow_tasks (downstream_subworkflow,
        id, workflow_id, task_index, node_id, task_name, input,
        queue_name, priority, dependencies, args_from,
        workflow_ctx_from, allow_failed_deps, join_type, min_success,
        task_options, max_retries, execution, good_until,
        retention_class_key, status, task_id, is_subworkflow,
        sub_workflow_id, sub_workflow_name, sub_definition_key,
        sub_workflow_params, sub_workflow_summary, result, error,
        created_at, started_at, completed_at
    )
    VALUES ('NONE',
        $1, $2, 0, 'node_0', 'terminalization.test', '{}',
        'default', 100, '{}', NULL,
        NULL, FALSE, 'all', NULL,
        NULL, 0, 'ISOLATED', NULL,
        $3, 'RUNNING', $4, FALSE,
        NULL, NULL, NULL,
        NULL, NULL, NULL, NULL,
        NOW(), NULL, NULL
    )
";

/// Attempt 1 of a task, FAILED with `will_retry`. Parameters: `$1` task id,
/// `$2` failed reason, `$3` worker id.
const FAILED_ATTEMPT_ONE_SQL: &str = "
    INSERT INTO horsies_task_attempts (
        task_id, attempt, outcome, will_retry,
        started_at, finished_at,
        error_code, error_message, failed_reason, worker_id,
        worker_hostname, worker_pid, worker_process_name
    )
    VALUES (
        $1, 1, 'FAILED', TRUE,
        NOW() - INTERVAL '2 minutes', NOW() - INTERVAL '1 minute',
        'TASK_EXCEPTION', 'attempt one raised', $2, $3,
        'terminalization-test-host', 4242, 'terminalization-test-proc'
    )
";

/// The retry count that the retry of attempt 1 leaves. Parameter `$1` is the
/// task id.
const RETRY_COUNT_ONE_SQL: &str = "UPDATE horsies_tasks SET retry_count = 1 WHERE id = $1";

/// A history row copied from the live row, with the terminal status `$2`,
/// the kind `$3` and the attempt snapshot `$4` (version), `$5` (payload),
/// `$6` (digest). Parameter `$1` is the task id.
const HISTORY_COPY_SQL: &str = "
    INSERT INTO horsies_task_history (
        task_id, task_name, queue_name, priority,
        command_fingerprint_version, command_fingerprint,
        status, terminalization_kind, terminal_at,
        retention_class_key, sent_at,
        enqueued_at, claimed_at, started_at, created_at,
        retry_count, max_retries,
        last_claimed_worker_id, last_worker_hostname,
        result_envelope_version,
        error_code, final_failed_reason,
        is_workflow_task, history_schema_version,
        attempt_archive_version, attempt_snapshot,
        attempt_snapshot_digest, rerun_input_disposition
    )
    SELECT id, task_name, queue_name, priority,
           command_fingerprint_version, command_fingerprint,
           $2, $3, NOW(),
           retention_class_key, sent_at,
           enqueued_at, claimed_at, started_at, created_at,
           retry_count, max_retries,
           claimed_by_worker_id, worker_hostname,
           1,
           NULL, NULL,
           is_workflow_task, 1,
           $4, $5,
           $6, 'NEVER_ELIGIBLE'
    FROM horsies_tasks WHERE id = CAST($1 AS uuid)
";

/// The live attempt rows of a task. Parameter `$1` is the task id.
const DELETE_LIVE_ATTEMPTS_SQL: &str =
    "DELETE FROM horsies_task_attempts WHERE task_id = CAST($1 AS uuid)";

/// The live row of a task. Parameter `$1` is the task id.
const DELETE_LIVE_ROW_SQL: &str = "DELETE FROM horsies_tasks WHERE id = CAST($1 AS uuid)";

/// The live attempt rows of a task: number, outcome and failed reason, in
/// attempt order. Parameter `$1` is the task id.
const LIVE_ATTEMPTS_SQL: &str = "
    SELECT attempt, outcome, failed_reason
    FROM horsies_task_attempts
    WHERE task_id = $1
    ORDER BY attempt
";

/// The attempt snapshot of the history row of a task. Parameter `$1` is the
/// task id.
const ARCHIVED_ATTEMPTS_SQL: &str = "
    SELECT attempt_archive_version, attempt_snapshot, attempt_snapshot_digest
    FROM horsies_task_history
    WHERE task_id = $1
";

/// The batch sizes that the batch functions refuse: no bound, zero, and a
/// negative bound.
const INVALID_BATCH_SIZES: [&str; 3] = ["NULL", "0", "-1"];

/// The claim generation of the seeded rows.
fn generation() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 4, 9, 0, 0)
        .single()
        .expect("a fixed UTC time")
}

/// One task row to seed.
struct Seed {
    status: &'static str,
    worker_id: Option<&'static str>,
    claimed_at: Option<DateTime<Utc>>,
    is_workflow_task: bool,
}

impl Seed {
    /// A row of `status` that [`WORKER`] claimed at [`generation`].
    fn claimed_by_this_worker(status: &'static str, is_workflow_task: bool) -> Seed {
        Seed {
            status,
            worker_id: Some(WORKER),
            claimed_at: Some(generation()),
            is_workflow_task,
        }
    }
}

/// A copy of the installed schema and an autocommit session on it.
async fn installed() -> (TestDatabase, Session) {
    let database = TestDatabase::create().await;
    let session = Session::open(database.url()).await;
    (database, session)
}

/// Inserts the task row of `task` and returns its id.
async fn seed(session: &Session, task: &Seed) -> Uuid {
    let task_id = Uuid::new_v4();
    let _rows = session
        .client()
        .execute(
            SEED_SQL,
            &[
                &task_id,
                &DEFAULT_RETENTION_CLASS_KEY,
                &task.is_workflow_task,
                &task.status,
                &task.claimed_at,
                &task.worker_id,
            ],
        )
        .await
        .unwrap_or_else(|error| panic!("seed a task: {}", describe(&error)));
    task_id
}

/// The status of `task_id`; it is on exactly one side.
async fn status_of(session: &Session, task_id: Uuid) -> String {
    let rows = session
        .client()
        .query(STATUS_SQL, &[&task_id])
        .await
        .unwrap_or_else(|error| panic!("read the status: {}", describe(&error)));
    assert_eq!(rows.len(), 1, "the task is on exactly one side");
    rows[0].get(0)
}

/// Asserts that `outcome` is a server error whose message holds `fragment`.
fn assert_refused<T: std::fmt::Debug>(
    outcome: Result<T, tokio_postgres::Error>,
    fragment: &str,
    case: &str,
) {
    let error = outcome.expect_err(case);
    let database_error = error
        .as_db_error()
        .unwrap_or_else(|| panic!("{case}: not a server error: {error}"));
    assert!(
        database_error.message().contains(fragment),
        "{case}: {}",
        describe(&error)
    );
}

/// A copy of the installed schema with one coverage pass, and an autocommit
/// session on it. The pass creates the history leaves of two days and the
/// heartbeat leaves of two hours, and publishes the readers, as the Python
/// broker fixture does: a terminal move writes a history row. The database
/// is dropped when the runtime of the test shuts down.
async fn covered() -> Session {
    let url = crate::broker::terminalization_matrix::migrated_database_url().await;
    Session::open(&url).await
}

/// Runs `statement` with `parameters` and returns the number of rows that it
/// changed.
///
/// # Panics
///
/// When the statement fails, with `name` and the server error.
async fn run(
    session: &Session,
    name: &str,
    statement: &str,
    parameters: &[&(dyn ToSql + Sync)],
) -> u64 {
    session
        .client()
        .execute(statement, parameters)
        .await
        .unwrap_or_else(|error| panic!("{name}: {}", describe(&error)))
}

/// Runs one writer and decodes its one outcome row with the decoder of the
/// core, which refuses a row that breaks the outcome contract.
///
/// # Panics
///
/// When the call fails, returns other than one row, or the row does not
/// decode.
async fn outcome_of(
    session: &Session,
    name: &str,
    statement: &str,
    parameters: &[&(dyn ToSql + Sync)],
) -> TerminalizationOutcome {
    let rows = session
        .client()
        .query(statement, parameters)
        .await
        .unwrap_or_else(|error| panic!("{name}: {}", describe(&error)));
    assert_eq!(rows.len(), 1, "{name}: one outcome row");
    decode_outcome_row(&rows[0]).unwrap_or_else(|error| panic!("{name}: {error}"))
}

/// The writer of COMPLETE_LOCKED (`horsies_finish_workflow_success`) under
/// [`WORKER`] at [`generation`]. A miss is classified before the function
/// asks whether the task is a workflow task.
async fn locked(session: &Session, task_id: Uuid) -> TerminalizationOutcome {
    outcome_of(
        session,
        "horsies_finish_workflow_success",
        WORKFLOW_SUCCESS_SQL,
        &[&task_id, &WORKER, &generation(), &LOCKED_RESULT],
    )
    .await
}

/// The writer of FAIL_RUNNING (`horsies_finish_failed_attempt`) under
/// [`WORKER`] at [`generation`], with `failed_reason` (NULL for `None`).
async fn fail_locked(
    session: &Session,
    task_id: Uuid,
    failed_reason: Option<&str>,
) -> TerminalizationOutcome {
    outcome_of(
        session,
        "horsies_finish_failed_attempt",
        FAILED_ATTEMPT_SQL,
        &[
            &task_id,
            &WORKER,
            &generation(),
            &FAIL_RUNNING_RESULT,
            &FAIL_RUNNING_ERROR_CODE,
            &failed_reason,
        ],
    )
    .await
}

/// The writer of FAIL_STALE (`horsies_finish_stale_attempt`) with the
/// thresholds [`STALE_AFTER_MS`] and [`FINALIZING_STALE_AFTER_MS`].
async fn fail_stale(session: &Session, task_id: Uuid) -> TerminalizationOutcome {
    fail_stale_after(session, task_id, STALE_AFTER_MS, FINALIZING_STALE_AFTER_MS).await
}

/// The writer of FAIL_STALE (`horsies_finish_stale_attempt`) with the
/// thresholds `stale_after_ms` and `finalizing_stale_after_ms`.
async fn fail_stale_after(
    session: &Session,
    task_id: Uuid,
    stale_after_ms: i32,
    finalizing_stale_after_ms: i32,
) -> TerminalizationOutcome {
    outcome_of(
        session,
        "horsies_finish_stale_attempt",
        STALE_ATTEMPT_SQL,
        &[
            &task_id,
            &stale_after_ms,
            &finalizing_stale_after_ms,
            &FAIL_STALE_RESULT,
            &FAIL_STALE_ERROR_CODE,
            &FAIL_STALE_REASON,
        ],
    )
    .await
}

/// The writer of CANCEL_ADMIN (`horsies_cancel_task`). It cancels a RUNNING
/// row only with `include_running`.
async fn cancel_admin(
    session: &Session,
    task_id: Uuid,
    include_running: bool,
) -> TerminalizationOutcome {
    outcome_of(
        session,
        "horsies_cancel_task",
        CANCEL_TASK_SQL,
        &[&task_id, &include_running],
    )
    .await
}

/// Puts the liveness columns of the row where a staleness case needs them:
/// started_at and finalizing_at the given seconds before now, NULL for
/// `None`.
async fn age(
    session: &Session,
    task_id: Uuid,
    started_seconds_ago: Option<i32>,
    finalizing_seconds_ago: Option<i32>,
) {
    let _aged = run(
        session,
        "set the liveness columns",
        AGE_SQL,
        &[&task_id, &started_seconds_ago, &finalizing_seconds_ago],
    )
    .await;
}

/// A heartbeat of the task in `role`, sent `seconds_ago` seconds before
/// now.
async fn heartbeat(session: &Session, task_id: Uuid, seconds_ago: i32, role: &str) {
    let _sent = run(
        session,
        "insert a heartbeat",
        HEARTBEAT_SQL,
        &[&task_id, &role, &seconds_ago],
    )
    .await;
}

/// A RUNNING workflow task of [`WORKER`] with its RUNNING node in a RUNNING
/// workflow. Returns the task id.
async fn workflow_task(session: &Session) -> Uuid {
    let task_id = seed(session, &Seed::claimed_by_this_worker("RUNNING", true)).await;
    let workflow_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();
    session.run_script("BEGIN", "BEGIN").await;
    let _workflow = run(
        session,
        "insert the workflow",
        WORKFLOW_SQL,
        &[&workflow_id],
    )
    .await;
    let _node = run(
        session,
        "insert the workflow node",
        WORKFLOW_NODE_SQL,
        &[
            &node_id,
            &workflow_id,
            &DEFAULT_RETENTION_CLASS_KEY,
            &task_id,
        ],
    )
    .await;
    session.run_script("COMMIT", "COMMIT").await;
    task_id
}

/// Leaves the row and its attempts where an automatic retry leaves them:
/// attempt 1 FAILED with `will_retry` and `failed_reason`, and retry_count
/// 1. The live row stores no failed reason. One transaction.
async fn requeued_after_failed_attempt(session: &Session, task_id: Uuid, failed_reason: &str) {
    session.run_script("BEGIN", "BEGIN").await;
    let _attempt = run(
        session,
        "insert attempt one",
        FAILED_ATTEMPT_ONE_SQL,
        &[&task_id, &failed_reason, &WORKER],
    )
    .await;
    let _retry_count = run(
        session,
        "advance the retry count",
        RETRY_COUNT_ONE_SQL,
        &[&task_id],
    )
    .await;
    session.run_script("COMMIT", "COMMIT").await;
}

/// Puts the task where another operation would have left it: a history row
/// with `status`, `kind` and an empty attempt snapshot, no live row and no
/// live attempt rows. One transaction.
async fn force_terminal(session: &Session, task_id: Uuid, status: &str, kind: &str) {
    let snapshot = encode_attempt_snapshot(&[]).expect("encode the empty attempt snapshot");
    session.run_script("BEGIN", "BEGIN").await;
    let copied = run(
        session,
        "copy the live row to history",
        HISTORY_COPY_SQL,
        &[
            &task_id,
            &status,
            &kind,
            &snapshot.version,
            &snapshot.payload,
            &snapshot.digest.as_slice(),
        ],
    )
    .await;
    assert_eq!(copied, 1, "the task has a live row to copy");
    let _attempts = run(
        session,
        "delete the live attempts",
        DELETE_LIVE_ATTEMPTS_SQL,
        &[&task_id],
    )
    .await;
    let _live = run(
        session,
        "delete the live row",
        DELETE_LIVE_ROW_SQL,
        &[&task_id],
    )
    .await;
    session.run_script("COMMIT", "COMMIT").await;
}

/// The task as one row ([`TASK_ROW_SQL`]); it is on exactly one side.
async fn task_row(session: &Session, task_id: Uuid) -> Row {
    let mut rows = session
        .client()
        .query(TASK_ROW_SQL, &[&task_id])
        .await
        .unwrap_or_else(|error| panic!("read the task row: {}", describe(&error)));
    assert_eq!(rows.len(), 1, "the task is on exactly one side");
    rows.remove(0)
}

/// One recorded attempt: number, outcome and failed reason.
type AttemptSummary = (i32, String, Option<String>);

/// Every recorded attempt of the task, attempt ascending, wherever it lives:
/// the live attempt rows while the task is live, else the attempt snapshot
/// of its history row (the move purges the live attempt rows into it). No
/// attempt is an empty list.
async fn attempt_history(session: &Session, task_id: Uuid) -> Vec<AttemptSummary> {
    let live = session
        .client()
        .query(LIVE_ATTEMPTS_SQL, &[&task_id])
        .await
        .unwrap_or_else(|error| panic!("read the live attempts: {}", describe(&error)));
    if !live.is_empty() {
        return live
            .iter()
            .map(|row| -> AttemptSummary { (row.get(0), row.get(1), row.get(2)) })
            .collect();
    }
    let archived = session
        .client()
        .query(ARCHIVED_ATTEMPTS_SQL, &[&task_id])
        .await
        .unwrap_or_else(|error| panic!("read the attempt snapshot: {}", describe(&error)));
    match archived.as_slice() {
        [] => Vec::new(),
        [row] => archived_attempts(row),
        rows => panic!("{} history rows for one task", rows.len()),
    }
}

/// The attempts of an attempt snapshot row ([`ARCHIVED_ATTEMPTS_SQL`]). The
/// baseline stores no codec or content type: archive version 1 means the
/// json-utf8 codec and the application/json content type.
fn archived_attempts(row: &Row) -> Vec<AttemptSummary> {
    let version: i16 = row.get(0);
    let payload: Vec<u8> = row.get(1);
    let digest: Vec<u8> = row.get(2);
    decode_attempt_snapshot(
        version,
        JSON_UTF8_CODEC,
        JSON_CONTENT_TYPE,
        &payload,
        &digest,
    )
    .unwrap_or_else(|error| panic!("decode the attempt snapshot: {error}"))
    .iter()
    .map(|record| {
        (
            record.attempt(),
            record.outcome().to_owned(),
            record.failed_reason().map(str::to_owned),
        )
    })
    .collect()
}

mod live_status_invariant {
    use super::*;

    /// A terminal task leaves the live table; the live row cannot hold a
    /// terminal status (CHECK horsies_tasks_status_live).
    #[tokio::test]
    async fn the_live_table_refuses_a_terminal_status() {
        let (_database, session) = installed().await;
        let task_id = seed(
            &session,
            &Seed {
                status: "PENDING",
                worker_id: None,
                claimed_at: None,
                is_workflow_task: false,
            },
        )
        .await;

        let updated = session
            .client()
            .execute(
                "UPDATE horsies_tasks SET status = 'CANCELLED' WHERE id = $1",
                &[&task_id],
            )
            .await;
        let error = updated.expect_err("the live table refuses CANCELLED");
        let database_error = error.as_db_error().expect("a server error");
        assert_eq!(
            database_error.code(),
            &SqlState::CHECK_VIOLATION,
            "{}",
            describe(&error)
        );
        assert_eq!(
            database_error.constraint(),
            Some("horsies_tasks_status_live")
        );
        assert!(database_error
            .message()
            .contains("horsies_tasks_status_live"));

        let status: String = session
            .client()
            .query_one(
                "SELECT status FROM horsies_tasks WHERE id = $1",
                &[&task_id],
            )
            .await
            .unwrap_or_else(|error| panic!("read the status: {}", describe(&error)))
            .get(0);
        assert_eq!(status, "PENDING");
    }
}

mod expire_pending_tasks {
    use super::*;

    /// LIMIT NULL means no limit at all, so NULL must be an error.
    ///
    /// The bound exists to keep one pass from committing an unbounded
    /// notification burst; a call that disables it is refused before any
    /// row is touched. One case per refused batch size, each on a database
    /// of its own.
    #[tokio::test]
    async fn an_unbounded_or_absurd_batch_mutates_nothing() {
        for batch_size in INVALID_BATCH_SIZES {
            let (_database, session) = installed().await;
            let task_id = seed(&session, &Seed::claimed_by_this_worker("PENDING", false)).await;
            let _rows = session
                .client()
                .execute(DEADLINE_SQL, &[&task_id, &60_i32])
                .await
                .unwrap_or_else(|error| panic!("set the deadline: {}", describe(&error)));

            let expired = session
                .client()
                .batch_execute(&format!(
                    "SELECT * FROM horsies_expire_pending_tasks(
                        CAST({batch_size} AS INTEGER), '{{}}', 'TASK_EXPIRED'
                    )"
                ))
                .await;
            assert_refused(expired, "positive integer", batch_size);

            assert_eq!(
                status_of(&session, task_id).await,
                "PENDING",
                "{batch_size}"
            );
        }
    }
}

mod cancel_orphaned_tasks {
    use super::*;

    /// One case per refused batch size, each on a database of its own.
    #[tokio::test]
    async fn an_invalid_bound_raises_before_mutation() {
        for batch_size in INVALID_BATCH_SIZES {
            let (_database, session) = installed().await;
            let task_id = seed(&session, &Seed::claimed_by_this_worker("CLAIMED", true)).await;

            let cancelled = session
                .client()
                .batch_execute(&format!(
                    "SELECT * FROM horsies_cancel_orphaned_tasks(
                        CAST({batch_size} AS INTEGER)
                    )"
                ))
                .await;
            assert_refused(cancelled, "positive integer", batch_size);

            assert_eq!(
                status_of(&session, task_id).await,
                "CLAIMED",
                "{batch_size}"
            );
        }
    }
}

mod id_keyed_workflow_batches {
    use super::*;

    /// The id and generation arrays of each refused call, with the fragment
    /// of its error message. `one` stands for the id of the seeded task.
    const INVALID_ARRAYS: [(&str, &str, &str); 5] = [
        ("NULL", "ARRAY['2026-08-04 09:00:00+00']", "non-NULL"),
        ("ARRAY['one']", "NULL", "non-NULL"),
        (
            "ARRAY['one', '00000000-0000-4000-8000-000000000002']",
            "ARRAY['2026-08-04 09:00:00+00']",
            "lengths differ",
        ),
        (
            "ARRAY[NULL]::uuid[]",
            "ARRAY['2026-08-04 09:00:00+00']",
            "non-NULL",
        ),
        (
            "ARRAY['one', 'one']",
            "ARRAY['2026-08-04 09:00:00+00', '2026-08-04 09:00:01+00']",
            "distinct",
        ),
    ];

    /// Each function with each refused pair of arrays, each on a database of
    /// its own.
    #[tokio::test]
    async fn invalid_arrays_raise_before_mutation() {
        for function_name in ["horsies_abandon_owned_nodes", "horsies_cancel_owned_nodes"] {
            for (ids_sql, generations_sql, message) in INVALID_ARRAYS {
                let case = format!("{function_name} {ids_sql} {generations_sql}");
                let (_database, session) = installed().await;
                let task_id = seed(&session, &Seed::claimed_by_this_worker("CLAIMED", true)).await;
                let ids = ids_sql.replace("one", &task_id.to_string());

                let refused = session
                    .client()
                    .query(
                        &format!(
                            "SELECT * FROM {function_name}(
                                CAST({ids} AS uuid[]),
                                CAST({generations_sql} AS TIMESTAMPTZ[]),
                                $1
                            )"
                        ),
                        &[&WORKER],
                    )
                    .await;
                assert_refused(refused, message, &case);

                assert_eq!(status_of(&session, task_id).await, "CLAIMED", "{case}");
            }
        }
    }
}
