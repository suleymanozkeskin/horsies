//! Ports of Python `clients/python/tests/unit/test_broker_postgres.py` rows of the reaper's
//! passes: the stale RUNNING pass (`TestMarkStaleTasksAsFailed`), the
//! pending expiry (`TestExpirePendingTasks`) and the orphan audit
//! (`TestTerminateOrphanedWorkflowTasks`).
//!
//! The decision of a stale candidate is pure and is tested on its own. The
//! passes run on the database of their test runtime
//! (`terminalization_matrix::migrated_pool`), so the counts are exact: no
//! other test's rows are in it.

use chrono::{TimeZone, Utc};
use serial_test::serial;
use uuid::Uuid;

use super::*;
use crate::broker::expiry_and_orphans::transitioned_count;
use crate::broker::pools::core_pool_for_test_url;
use crate::broker::BrokerError;
use crate::core::lifecycle::TerminalizationOutcome;
use crate::test_support::migrated_pool;
use crate::test_support::sql::{self, TestPool};
use crate::test_support::{CapturedEvent, LogCapture};

/// The policy that retries a crash, as Python's test stores it, with the
/// limit `max_retries` and a fixed 60 s interval per retry.
fn crash_retry_options(max_retries: u32) -> String {
    let intervals: Vec<u32> = (0..max_retries).map(|_| 60).collect();
    serde_json::json!({
        "retry_policy": {
            "max_retries": max_retries,
            "intervals": intervals,
            "backoff_strategy": "fixed",
            "jitter": false,
            "max_delay_seconds": null,
            "auto_retry_for": ["WORKER_CRASHED"]
        }
    })
    .to_string()
}

/// A scanned stale candidate of `worker-1`, started at 2025-01-01T00:00 and
/// scanned ten minutes later.
fn candidate(retry_count: i32, max_retries: i32, task_options: Option<String>) -> StaleCandidate {
    StaleCandidate {
        task_id: Uuid::from_u128(1),
        task_name: "stale.task".to_owned(),
        queue_name: "default".to_owned(),
        trace_id: crate::telemetry::trace_carrier::StoredTraceId::NoTraceId,
        retry_count,
        max_retries,
        task_options,
        good_until: None,
        claimed_by_worker_id: Some("worker-1".to_owned()),
        worker_pid: Some(1234),
        worker_hostname: Some("host-1".to_owned()),
        worker_process_name: Some("proc-1".to_owned()),
        started_at: Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
        db_now: Utc.with_ymd_and_hms(2025, 1, 1, 0, 10, 0).unwrap(),
    }
}

fn thresholds_ms(stale_after: u64, finalizing_stale_after: u64) -> StaleRunningThresholds {
    StaleRunningThresholds {
        stale_after: ThresholdMs::from_millis(stale_after).expect("within the integer range"),
        finalizing_stale_after: ThresholdMs::from_millis(finalizing_stale_after)
            .expect("within the integer range"),
    }
}

fn whole_scan() -> RecoveryBatch {
    RecoveryBatch::new(1_000).expect("1000 rows is a recovery batch")
}

/// The shape of the rows that [`seed_tasks`] inserts.
struct Seed {
    status: &'static str,
    is_workflow_task: bool,
    /// For a RUNNING row: how long ago it started; it has no runner beat.
    started_seconds_ago: f64,
    /// `good_until` this many seconds before the database clock; `None` for
    /// no deadline.
    deadline_seconds_ago: Option<f64>,
    retry_count: i32,
    max_retries: i32,
    task_options: Option<String>,
}

impl Seed {
    /// A sent RUNNING row of `worker-1`, started 10 minutes ago, with no
    /// retry policy.
    fn stale_running() -> Seed {
        Seed {
            status: "RUNNING",
            is_workflow_task: false,
            started_seconds_ago: 600.0,
            deadline_seconds_ago: None,
            retry_count: 0,
            max_retries: 0,
            task_options: None,
        }
    }

    /// A sent PENDING row whose deadline passed a minute ago.
    fn due_pending() -> Seed {
        Seed {
            status: "PENDING",
            deadline_seconds_ago: Some(60.0),
            ..Seed::stale_running()
        }
    }

    /// A workflow task in `status` with no workflow link.
    fn unlinked_workflow_task(status: &'static str) -> Seed {
        Seed {
            status,
            is_workflow_task: true,
            ..Seed::stale_running()
        }
    }
}

/// Insert `count` live rows of `seed` with every baseline column named, and
/// return their ids. A sent row carries a command fingerprint; a workflow
/// task carries none.
async fn seed_tasks(pool: &TestPool, seed: &Seed, count: i32) -> Vec<Uuid> {
    sql::query_scalar(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, task_options, execution,
            retention_class_key, sent_at, created_at,
            command_fingerprint_version, command_fingerprint, idempotency_key_digest,
            status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
            started_at, finalizing_at, finalizing_by_worker_id,
            worker_pid, worker_hostname, worker_process_name,
            retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
            updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
         )
         SELECT
            gen_random_uuid(), 'reaper_port', 'default', 100, '{}', $1, 'ISOLATED',
            'forever', NOW() - interval '2 hours', NOW() - interval '2 hours',
            CASE WHEN $2 THEN NULL ELSE 1 END,
            CASE WHEN $2 THEN NULL ELSE decode(repeat('07', 32), 'hex') END, NULL,
            $3, NOW() - interval '2 hours',
            CASE WHEN $3 = 'PENDING' THEN NULL ELSE NOW() - interval '1 hour' END,
            CASE WHEN $3 = 'PENDING' THEN NULL ELSE 'worker-1' END,
            CASE WHEN $3 = 'CLAIMED' THEN NOW() + interval '5 minutes' END,
            CASE WHEN $3 = 'RUNNING' THEN NOW() - make_interval(secs => $4::float8) END,
            NULL, NULL,
            CASE WHEN $3 = 'RUNNING' THEN 4242 END,
            CASE WHEN $3 = 'RUNNING' THEN 'reaper-host' END,
            CASE WHEN $3 = 'RUNNING' THEN 'reaper-process' END,
            $5, $6, NULL,
            CASE WHEN $7::float8 IS NULL THEN NULL
                 ELSE NOW() - make_interval(secs => $7::float8) END,
            $2,
            NOW(), FALSE, NULL, NULL
         FROM generate_series(1, $8)
         RETURNING id",
    )
    .bind(seed.task_options.as_deref())
    .bind(seed.is_workflow_task)
    .bind(seed.status)
    .bind(seed.started_seconds_ago)
    .bind(seed.retry_count)
    .bind(seed.max_retries)
    .bind(seed.deadline_seconds_ago)
    .bind(count)
    .fetch_all(pool)
    .await
    .expect("seed live task rows")
}

async fn seed_task(pool: &TestPool, seed: &Seed) -> Uuid {
    seed_tasks(pool, seed, 1).await[0]
}

/// Set the finalizing marker of `worker-1`, `seconds_ago` before the
/// database clock.
async fn set_finalizing(pool: &TestPool, task_id: Uuid, seconds_ago: f64) {
    sql::query(
        "UPDATE horsies_tasks
         SET finalizing_at = NOW() - make_interval(secs => $2::float8),
             finalizing_by_worker_id = 'worker-1'
         WHERE id = $1",
    )
    .bind(task_id)
    .bind(seconds_ago)
    .execute(pool)
    .await
    .expect("set the finalizing marker");
}

/// The live status of a task, or `None` when it left the live table.
async fn live_status(pool: &TestPool, task_id: Uuid) -> Option<String> {
    sql::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("read the live status")
}

/// The terminal facts of a history row: status, terminalization kind,
/// error code, and the stored result as text.
type HistoryFacts = (String, String, Option<String>, Option<String>);

async fn history_facts(pool: &TestPool, task_id: Uuid) -> Option<HistoryFacts> {
    sql::query_as(
        "SELECT status::text, terminalization_kind::text, error_code,
                convert_from(result_payload, 'UTF8')
         FROM horsies_task_history WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_optional(pool)
    .await
    .expect("read the history row")
}

async fn live_attempt_count(pool: &TestPool, task_id: Uuid) -> i64 {
    sql::query_scalar("SELECT count(*) FROM horsies_task_attempts WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("count the live attempts")
}

// ---- The stale RUNNING pass ----

/// Python `TestMarkStaleTasksAsFailed::test_update_includes_worker_crashed_error_code`
/// (now `test_a_candidate_without_a_crash_retry_fails_in_one_statement`): a
/// candidate whose policy does not retry the crash fails with
/// `WORKER_CRASHED`, the crash reason in the task and the attempt, and a
/// result that holds the code and the scan time.
#[test]
fn a_candidate_without_a_crash_retry_fails_with_the_crash_code_reason_and_result() {
    let thresholds = thresholds_ms(300_000, 300_000);

    let command = stale_attempt_command(&candidate(0, 0, None), thresholds)
        .expect("the crash result encodes");

    let reason = "Worker process crashed (no runner heartbeat for 300000ms = 300.0s)";
    assert_eq!(command.task_id, Uuid::from_u128(1));
    assert_eq!(command.evidence, StaleEvidence::BeatAge(thresholds));
    assert_eq!(command.retry, StaleRetry::NoRetry);
    assert_eq!(command.error_code, "WORKER_CRASHED");
    assert_eq!(command.failed_reason, reason);
    assert_eq!(command.attempt_message, reason);
    assert_eq!(command.attempt_reason, reason);
    assert!(
        command.result_json.contains("\"WORKER_CRASHED\""),
        "{}",
        command.result_json
    );
    let result: serde_json::Value =
        serde_json::from_str(&command.result_json).expect("the crash result is JSON");
    assert_eq!(
        result["err"]["data"]["detected_at"],
        "2025-01-01T00:10:00+00:00"
    );
    assert_eq!(result["err"]["data"]["worker_id"], "worker-1");
    assert_eq!(result["err"]["message"], reason);
}

/// Python `TestMarkStaleTasksAsFailed::test_a_crash_retry_carries_the_scanned_count_and_wakes_its_queue`
/// (the retry half; the wake is
/// `broker::lifecycle_tests::stale_recovery_functions::reaper_pass`): a
/// policy that names the crash retries from the scanned retry count, after
/// its first interval.
#[test]
fn a_candidate_whose_policy_names_the_crash_retries_from_its_scanned_count() {
    let command = stale_attempt_command(
        &candidate(0, 2, Some(crash_retry_options(2))),
        thresholds_ms(300_000, 300_000),
    )
    .expect("the crash result encodes");

    assert_eq!(
        command.retry,
        StaleRetry::RetryAfter {
            expected_retry_count: 0,
            delay_seconds: 60.0,
        }
    );
}

/// Python `TestMarkStaleTasksAsFailed::test_stale_task_retries_exhausted_marks_failed`:
/// a policy that names the crash, with every retry spent, fails the task.
#[test]
fn a_candidate_whose_crash_retries_are_spent_fails() {
    let command = stale_attempt_command(
        &candidate(3, 3, Some(crash_retry_options(3))),
        thresholds_ms(300_000, 300_000),
    )
    .expect("the crash result encodes");

    assert_eq!(command.retry, StaleRetry::NoRetry);
}

/// The stored options of the stale recovery tests: a fixed
/// policy of 3 retries after 5, 10 and 20 s, with no jitter, for the codes
/// `auto_retry_for`.
fn fixed_policy_for(auto_retry_for: &[&str]) -> String {
    serde_json::json!({
        "retry_policy": {
            "max_retries": 3,
            "intervals": [5, 10, 20],
            "backoff_strategy": "fixed",
            "jitter": false,
            "auto_retry_for": auto_retry_for
        }
    })
    .to_string()
}

/// A policy that names the crash retries from the scanned count after the
/// interval of the next attempt (count 1: the second interval, 10 s). No
/// policy, or a policy for other codes, does not retry while retries
/// remain.
#[test]
fn the_crash_retry_of_a_candidate_follows_its_stored_policy() {
    let thresholds = thresholds_ms(300_000, 300_000);
    let retry_of = |retry_count: i32, task_options: Option<String>| {
        stale_attempt_command(&candidate(retry_count, 3, task_options), thresholds)
            .expect("the crash result encodes")
            .retry
    };

    assert_eq!(
        retry_of(1, Some(fixed_policy_for(&["WORKER_CRASHED"]))),
        StaleRetry::RetryAfter {
            expected_retry_count: 1,
            delay_seconds: 10.0,
        }
    );
    assert_eq!(retry_of(0, None), StaleRetry::NoRetry);
    let python_options_for_timeout = serde_json::json!({
        "retry_policy": {
            "max_retries": 3,
            "intervals": [5],
            "auto_retry_for": ["TIMEOUT"]
        }
    })
    .to_string();
    assert_eq!(
        retry_of(0, Some(python_options_for_timeout)),
        StaleRetry::NoRetry
    );
    assert_eq!(
        retry_of(0, Some(fixed_policy_for(&["TIMEOUT"]))),
        StaleRetry::NoRetry
    );
}

/// The crash result is a `WORKER_CRASHED` error whose message is the crash
/// reason and whose data are the facts of the silent attempt, the
/// threshold in ms and in s, and the scan's clock.
#[test]
fn the_crash_result_names_the_silent_attempt() {
    let command = stale_attempt_command(&candidate(0, 0, None), thresholds_ms(30_000, 60_000))
        .expect("the crash result encodes");

    let result: serde_json::Value =
        serde_json::from_str(&command.result_json).expect("the crash result is JSON");
    assert_eq!(
        result["err"]["message"],
        "Worker process crashed (no runner heartbeat for 30000ms = 30.0s)"
    );
    assert_eq!(
        result["err"]["data"],
        serde_json::json!({
            "stale_threshold_ms": 30_000,
            "stale_threshold_seconds": 30.0,
            "worker_pid": 1234,
            "worker_hostname": "host-1",
            "worker_id": "worker-1",
            "started_at": "2025-01-01T00:00:00+00:00",
            "detected_at": "2025-01-01T00:10:00+00:00"
        })
    );
}

/// Python `TestMarkStaleTasksAsFailed::test_fractional_second_thresholds_keep_millisecond_precision`
/// (now `test_the_scan_takes_the_thresholds_in_ms_and_the_batch_size`): the
/// pass judges both thresholds to the millisecond. With 1.9 s and 2.75 s, a
/// row started 1.1 s ago stays RUNNING (a threshold cut to 1 s would fail
/// it), a row started 2.7 s ago fails, and a row whose finalizing marker is
/// 2.2 s old stays (a threshold cut to 2 s would fail it).
#[tokio::test]
#[serial]
async fn the_stale_pass_judges_its_thresholds_to_the_millisecond() {
    let pool = migrated_pool().await;
    let core = core_pool_for_test_url(pool.url());
    let fresh = seed_task(
        &pool,
        &Seed {
            started_seconds_ago: 1.1,
            ..Seed::stale_running()
        },
    )
    .await;
    let stale = seed_task(
        &pool,
        &Seed {
            started_seconds_ago: 2.7,
            ..Seed::stale_running()
        },
    )
    .await;
    let finalizing = seed_task(&pool, &Seed::stale_running()).await;
    set_finalizing(&pool, finalizing, 2.2).await;

    let recovered = mark_stale_running_as_failed(
        &core,
        StatementLog::Off,
        thresholds_ms(1_900, 2_750),
        whole_scan(),
        &WorkerTelemetry::Off,
    )
    .await
    .expect("the stale pass");

    assert_eq!(recovered, 1);
    assert_eq!(live_status(&pool, fresh).await.as_deref(), Some("RUNNING"));
    assert_eq!(
        live_status(&pool, finalizing).await.as_deref(),
        Some("RUNNING")
    );
    let (status, kind, _, _) = history_facts(&pool, stale)
        .await
        .expect("the stale row moved to history");
    assert_eq!((status.as_str(), kind.as_str()), ("FAILED", "FAIL_STALE"));
    core.close();
}

/// Python `TestMarkStaleTasksAsFailed::test_no_stale_tasks_returns_zero`: a
/// pass with no stale row recovers none and writes nothing.
#[tokio::test]
#[serial]
async fn a_stale_pass_with_no_stale_row_recovers_none_and_writes_nothing() {
    let pool = migrated_pool().await;
    let core = core_pool_for_test_url(pool.url());
    let running = seed_task(
        &pool,
        &Seed {
            started_seconds_ago: 0.0,
            ..Seed::stale_running()
        },
    )
    .await;

    let recovered = mark_stale_running_as_failed(
        &core,
        StatementLog::Off,
        thresholds_ms(60_000, 600_000),
        whole_scan(),
        &WorkerTelemetry::Off,
    )
    .await
    .expect("the stale pass");

    assert_eq!(recovered, 0);
    assert_eq!(
        live_status(&pool, running).await.as_deref(),
        Some("RUNNING")
    );
    assert_eq!(live_attempt_count(&pool, running).await, 0);
    core.close();
}

/// Python `TestMarkStaleTasksAsFailed::test_stale_tasks_marked_failed_and_committed`:
/// each stale row with no crash retry fails, its failure is committed, and
/// the pass counts each one.
#[tokio::test]
#[serial]
async fn a_stale_pass_fails_each_stale_row_and_counts_it() {
    let pool = migrated_pool().await;
    let core = core_pool_for_test_url(pool.url());
    let stale = seed_tasks(&pool, &Seed::stale_running(), 2).await;

    let recovered = mark_stale_running_as_failed(
        &core,
        StatementLog::Off,
        thresholds_ms(300_000, 300_000),
        whole_scan(),
        &WorkerTelemetry::Off,
    )
    .await
    .expect("the stale pass");

    assert_eq!(recovered, 2);
    for task_id in stale {
        assert_eq!(live_status(&pool, task_id).await, None);
        let (status, kind, _, _) = history_facts(&pool, task_id)
            .await
            .expect("the stale row moved to history");
        assert_eq!((status.as_str(), kind.as_str()), ("FAILED", "FAIL_STALE"));
    }
    core.close();
}

/// A row whose finalizing marker is older than the finalizing threshold is
/// stale: the pass fails it with `WORKER_CRASHED`, and its live row, with
/// the marker, moves to history.
#[tokio::test]
#[serial]
async fn a_stale_pass_fails_a_row_whose_finalizing_marker_is_past_its_threshold() {
    let pool = migrated_pool().await;
    let core = core_pool_for_test_url(pool.url());
    let finalizing = seed_task(&pool, &Seed::stale_running()).await;
    set_finalizing(&pool, finalizing, 10.0).await;

    let recovered = mark_stale_running_as_failed(
        &core,
        StatementLog::Off,
        thresholds_ms(300_000, 5_000),
        whole_scan(),
        &WorkerTelemetry::Off,
    )
    .await
    .expect("the stale pass");

    assert_eq!(recovered, 1);
    assert_eq!(live_status(&pool, finalizing).await, None);
    let (status, kind, error_code, _) = history_facts(&pool, finalizing)
        .await
        .expect("the stale row moved to history");
    assert_eq!(
        (status.as_str(), kind.as_str(), error_code.as_deref()),
        ("FAILED", "FAIL_STALE", Some("WORKER_CRASHED"))
    );
    core.close();
}

/// A stale pass whose scan reads its whole batch warns on `horsies.broker`
/// that the rest is recovered next pass, and counts the rows it recovered.
#[tokio::test]
#[serial]
async fn a_stale_pass_whose_scan_reads_its_whole_batch_warns_that_the_rest_waits() {
    let pool = migrated_pool().await;
    let core = core_pool_for_test_url(pool.url());
    let _stale = seed_tasks(&pool, &Seed::stale_running(), 2).await;
    let capture = LogCapture::new();
    let _default = capture.set_default();

    let recovered = mark_stale_running_as_failed(
        &core,
        StatementLog::Off,
        thresholds_ms(300_000, 300_000),
        RecoveryBatch::new(1).expect("1 row is a recovery batch"),
        &WorkerTelemetry::Off,
    )
    .await
    .expect("the stale pass");

    assert_eq!(recovered, 1);
    let [warning]: [CapturedEvent; 1] = capture
        .lines_starting(
            tracing::Level::WARN,
            "mark_stale_tasks_as_failed read a full scan batch",
        )
        .try_into()
        .expect("one full-batch warning");
    assert_eq!(warning.target, "horsies.broker");
    assert_eq!(
        warning.message,
        "mark_stale_tasks_as_failed read a full scan batch of 1 candidates; the rest is \
         recovered next pass",
    );
    core.close();
}

/// A stale pass whose scan cannot run (its pool is closed) is an error, not
/// a count; nothing is written.
#[tokio::test]
#[serial]
async fn a_stale_pass_whose_scan_cannot_run_is_an_error() {
    let pool = migrated_pool().await;
    let core = core_pool_for_test_url(pool.url());
    core.close();

    let failed = mark_stale_running_as_failed(
        &core,
        StatementLog::Off,
        thresholds_ms(300_000, 300_000),
        whole_scan(),
        &WorkerTelemetry::Off,
    )
    .await;

    assert!(
        matches!(failed, Err(BrokerError::Statement(_))),
        "{failed:?}"
    );
}

/// Python `TestMarkStaleTasksAsFailed::test_update_includes_worker_crashed_error_code`,
/// at the database: the failed task stores `WORKER_CRASHED` as its code and
/// in its result, and its archived attempt 1 is FAILED with that code.
#[tokio::test]
#[serial]
async fn a_stale_failure_stores_the_crash_code_in_the_task_and_its_attempt() {
    let pool = migrated_pool().await;
    let core = core_pool_for_test_url(pool.url());
    let task_id = seed_task(&pool, &Seed::stale_running()).await;

    let recovered = mark_stale_running_as_failed(
        &core,
        StatementLog::Off,
        thresholds_ms(300_000, 300_000),
        whole_scan(),
        &WorkerTelemetry::Off,
    )
    .await
    .expect("the stale pass");

    assert_eq!(recovered, 1);
    let (_, _, error_code, result) = history_facts(&pool, task_id)
        .await
        .expect("the stale row moved to history");
    assert_eq!(error_code.as_deref(), Some("WORKER_CRASHED"));
    let result = result.expect("the failure stores its result");
    assert!(result.contains("\"WORKER_CRASHED\""), "{result}");
    let (version, payload, digest): (i16, Vec<u8>, Vec<u8>) = sql::query_as(
        "SELECT attempt_archive_version, attempt_snapshot, attempt_snapshot_digest
         FROM horsies_task_history WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(&pool)
    .await
    .expect("read the archived attempts");
    let attempts = crate::core::history::archive::attempts::decode_attempt_snapshot(
        version,
        crate::core::history::archive::versions::JSON_UTF8_CODEC,
        crate::core::history::archive::versions::JSON_CONTENT_TYPE,
        &payload,
        &digest,
    )
    .expect("decode the archived attempts");
    let archived: Vec<(i32, &str, Option<&str>)> = attempts
        .iter()
        .map(|attempt| (attempt.attempt(), attempt.outcome(), attempt.error_code()))
        .collect();
    assert_eq!(archived, [(1, "FAILED", Some("WORKER_CRASHED"))]);
    core.close();
}

// ---- The pending expiry ----

/// Python `TestExpirePendingTasks::test_no_expired_returns_zero`: a pass
/// with no due row expires none.
#[tokio::test]
#[serial]
async fn a_pending_expiry_pass_with_no_due_row_expires_none() {
    let pool = migrated_pool().await;
    let core = core_pool_for_test_url(pool.url());
    let not_due = seed_task(
        &pool,
        &Seed {
            deadline_seconds_ago: Some(-3_600.0),
            ..Seed::due_pending()
        },
    )
    .await;

    let expired = expire_pending_pass(&core, StatementLog::Off)
        .await
        .expect("the expiry pass");

    assert_eq!(expired, PendingExpiryPass::Drained { expired: 0 });
    assert_eq!(
        live_status(&pool, not_due).await.as_deref(),
        Some("PENDING")
    );
    core.close();
}

/// Python `TestExpirePendingTasks::test_expired_tasks_transitioned`: each
/// due row is expired with `TASK_EXPIRED` as its code and in its result, and
/// counted.
#[tokio::test]
#[serial]
async fn a_pending_expiry_pass_expires_each_due_row_with_task_expired() {
    let pool = migrated_pool().await;
    let core = core_pool_for_test_url(pool.url());
    let due = seed_tasks(&pool, &Seed::due_pending(), 3).await;

    let expired = expire_pending_pass(&core, StatementLog::Off)
        .await
        .expect("the expiry pass");

    assert_eq!(expired, PendingExpiryPass::Drained { expired: 3 });
    for task_id in due {
        let (status, _, error_code, result) = history_facts(&pool, task_id)
            .await
            .expect("the due row moved to history");
        assert_eq!(status, "EXPIRED");
        assert_eq!(error_code.as_deref(), Some("TASK_EXPIRED"));
        let result = result.expect("the expiry stores its result");
        assert!(result.contains("\"TASK_EXPIRED\""), "{result}");
    }
    core.close();
}

/// Python `TestExpirePendingTasks::test_a_full_batch_continues_until_a_short_batch`:
/// a full batch of 500 is followed by the next batch in the same pass, which
/// ends at the short one.
#[tokio::test]
#[serial]
async fn a_pending_expiry_pass_runs_batches_until_a_short_one() {
    let pool = migrated_pool().await;
    let core = core_pool_for_test_url(pool.url());
    let _due = seed_tasks(&pool, &Seed::due_pending(), 502).await;

    let expired = expire_pending_pass(&core, StatementLog::Off)
        .await
        .expect("the expiry pass");

    assert_eq!(expired, PendingExpiryPass::Drained { expired: 502 });
    let left: i64 = sql::query_scalar(
        "SELECT count(*) FROM horsies_tasks WHERE task_name = 'reaper_port' AND status = 'PENDING'",
    )
    .fetch_one(&pool)
    .await
    .expect("count the PENDING rows");
    assert_eq!(left, 0);
    core.close();
}

/// Python `TestExpirePendingTasks::test_a_non_applied_batch_outcome_fails_closed`,
/// the refusal: a discovery batch reports moved rows only, so any other
/// outcome is a contract error.
#[test]
fn a_pending_expiry_batch_with_an_outcome_other_than_applied_is_a_contract_error() {
    let refused = transitioned_count(&[TerminalizationOutcome::TaskAbsent {
        task_id: Uuid::from_u128(7),
        ordinality: None,
    }]);

    assert!(
        matches!(refused, Err(BrokerError::TerminalizationContract(_))),
        "{refused:?}"
    );
    assert_eq!(transitioned_count(&[]).expect("no outcome is no move"), 0);
}

/// Python `TestExpirePendingTasks::test_a_non_applied_batch_outcome_fails_closed`,
/// the commit: a batch that reports an outcome other than APPLIED commits
/// nothing. The batch function is replaced, in this test's database, by one
/// that writes a marker row and reports a TASK_ABSENT row.
#[tokio::test]
#[serial]
async fn a_pending_expiry_batch_with_an_outcome_other_than_applied_commits_nothing() {
    let pool = migrated_pool().await;
    sql::raw_sql(
        "CREATE TABLE expiry_batch_marker (written boolean NOT NULL);
         DROP FUNCTION horsies_expire_pending_tasks(integer, text, text);
         CREATE FUNCTION horsies_expire_pending_tasks(
             p_batch_size integer, p_result text, p_error_code text
         ) RETURNS SETOF horsies_terminalization_outcome
         LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO expiry_batch_marker VALUES (TRUE);
             RETURN QUERY SELECT gen_random_uuid(), NULL::bigint, 'TASK_ABSENT'::text,
                 NULL::timestamptz, NULL::text, NULL::text, NULL::varchar,
                 NULL::timestamptz, NULL::text, NULL::jsonb;
         END
         $$;",
    )
    .execute(&pool)
    .await
    .expect("replace the expiry batch function");
    let core = core_pool_for_test_url(pool.url());

    let refused = expire_pending_pass(&core, StatementLog::Off).await;

    assert!(
        matches!(
            refused,
            Err(PendingExpiryFailed {
                expired_before: 0,
                error: BrokerError::TerminalizationContract(_),
            })
        ),
        "{refused:?}"
    );
    let markers: i64 = sql::query_scalar("SELECT count(*) FROM expiry_batch_marker")
        .fetch_one(&pool)
        .await
        .expect("count the marker rows");
    assert_eq!(markers, 0, "the refused batch committed its writes");
    core.close();
}

/// Python `TestExpirePendingTasks::test_db_error_returns_err`: a batch that
/// cannot run is an error, not a count.
#[tokio::test]
#[serial]
async fn a_pending_expiry_pass_whose_batch_cannot_run_is_an_error() {
    let pool = migrated_pool().await;
    let core = core_pool_for_test_url(pool.url());
    core.close();

    let failed = expire_pending_pass(&core, StatementLog::Off).await;

    assert!(
        matches!(
            failed,
            Err(PendingExpiryFailed {
                expired_before: 0,
                error: BrokerError::Statement(_),
            })
        ),
        "{failed:?}"
    );
}

// ---- The orphan audit ----

/// Python `TestTerminateOrphanedWorkflowTasks::test_no_orphans_returns_zero`:
/// a page with no orphan cancels none and reports ready.
#[tokio::test]
#[serial]
async fn an_orphan_audit_with_no_orphan_cancels_none() {
    let pool = migrated_pool().await;
    let core = core_pool_for_test_url(pool.url());
    let _sent = seed_task(&pool, &Seed::due_pending()).await;

    let page = cancel_orphan_page(&core, StatementLog::Off)
        .await
        .expect("the orphan audit");

    assert_eq!(page.cancelled, 0);
    assert_eq!(page.candidates_returned, 0);
    let report = OrphanTaskAuditReport::ready(page, std::time::Instant::now());
    assert_eq!(report.state, "ready");
    core.close();
}

/// Python `TestTerminateOrphanedWorkflowTasks::test_counts_only_transitioned_rows`:
/// the audit cancels the PENDING and CLAIMED workflow tasks that have no
/// workflow link and counts them; a sent task stays.
#[tokio::test]
#[serial]
async fn an_orphan_audit_cancels_and_counts_only_unlinked_workflow_tasks() {
    let pool = migrated_pool().await;
    let pending = seed_task(&pool, &Seed::unlinked_workflow_task("PENDING")).await;
    let claimed = seed_task(&pool, &Seed::unlinked_workflow_task("CLAIMED")).await;
    let sent = seed_task(
        &pool,
        &Seed {
            status: "CLAIMED",
            ..Seed::stale_running()
        },
    )
    .await;

    let core = core_pool_for_test_url(pool.url());

    let report = cancel_orphan_page(&core, StatementLog::Off)
        .await
        .expect("the orphan audit");

    core.close();
    assert_eq!(report.rows_selected, 2);
    assert_eq!(report.candidates_returned, 2);
    assert_eq!(report.cancelled, 2);
    for task_id in [pending, claimed] {
        let (status, _, _, _) = history_facts(&pool, task_id)
            .await
            .expect("the orphan moved to history");
        assert_eq!(status, "CANCELLED");
    }
    assert_eq!(live_status(&pool, sent).await.as_deref(), Some("CLAIMED"));
}

/// Python `TestTerminateOrphanedWorkflowTasks::test_a_non_applied_outcome_fails_closed`:
/// a page that reports an outcome other than APPLIED fails and commits
/// nothing, its cursor update included. The page function is replaced, in
/// this test's database, by one that records one candidate in the cursor
/// and reports a TASK_ABSENT row.
#[tokio::test]
#[serial]
async fn an_orphan_audit_with_an_outcome_other_than_applied_fails_and_commits_nothing() {
    let pool = migrated_pool().await;
    sql::raw_sql(
        "DROP FUNCTION horsies_cancel_orphaned_tasks(integer);
         CREATE FUNCTION horsies_cancel_orphaned_tasks(p_batch_size integer)
         RETURNS SETOF horsies_terminalization_outcome
         LANGUAGE plpgsql AS $$
         BEGIN
             UPDATE horsies_recovery_scan_cursors
             SET last_scan_rows = 1, last_candidate_rows = 1, last_scan_at = now()
             WHERE scan_name = 'orphan_workflow_tasks';
             RETURN QUERY SELECT gen_random_uuid(), NULL::bigint, 'TASK_ABSENT'::text,
                 NULL::timestamptz, NULL::text, NULL::text, NULL::varchar,
                 NULL::timestamptz, NULL::text, NULL::jsonb;
         END
         $$;",
    )
    .execute(&pool)
    .await
    .expect("replace the orphan page function");

    let core = core_pool_for_test_url(pool.url());

    let refused = cancel_orphan_page(&core, StatementLog::Off).await;

    core.close();
    assert!(
        matches!(refused, Err(BrokerError::TerminalizationContract(_))),
        "{refused:?}"
    );
    let candidates: i32 = sql::query_scalar(
        "SELECT last_candidate_rows FROM horsies_recovery_scan_cursors
         WHERE scan_name = 'orphan_workflow_tasks'",
    )
    .fetch_one(&pool)
    .await
    .expect("read the orphan cursor");
    assert_eq!(
        candidates, 0,
        "the refused page committed its cursor update"
    );
}

/// Python `TestTerminateOrphanedWorkflowTasks::test_database_error_returns_err`:
/// a page that cannot run is an error and cancels nothing.
#[tokio::test]
#[serial]
async fn an_orphan_audit_whose_page_cannot_run_is_an_error() {
    let pool = migrated_pool().await;
    let orphan = seed_task(&pool, &Seed::unlinked_workflow_task("PENDING")).await;
    let closed = core_pool_for_test_url(pool.url());
    closed.close();

    let failed = cancel_orphan_page(&closed, StatementLog::Off).await;

    assert!(
        matches!(failed, Err(BrokerError::Statement(_))),
        "{failed:?}"
    );
    assert_eq!(live_status(&pool, orphan).await.as_deref(), Some("PENDING"));
}
