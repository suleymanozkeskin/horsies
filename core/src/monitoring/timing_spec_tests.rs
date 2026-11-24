//! The phase timing and the attempt phases through the monitoring reads, on
//! a migrated database of each test (`spec_support::SpecDatabase`).
//!
//! The composition rules are tested without a database in `timing_tests.rs`;
//! the rows of the run read in `sql_tests/cases/monitoring_timing_cases.sql`.
//! These tests read seeded rows through `get_workflow_run` and
//! `get_task_detail`, as a client does.

use uuid::Uuid;

use super::function_rows::MONITORING_WORKFLOW_RUN;
use super::spec_support::{seed_run, seed_terminal, Ending, LiveTask, RunSeed, SpecDatabase};
use super::timing_models::{
    AttemptClaimsInfo, AttemptFinalizingInfo, AttemptPhasesInfo, AttemptPlacementInfo,
    AttemptRecordedByInfo, NodeTimingInfo, RunCoverageInfo, RunTimingInfo, SegmentEndInfo,
    SegmentPhaseInfo, UnavailableReason,
};
use super::{get_task_detail, get_workflow_run, TaskDetailRead};
use crate::test_support::sql::{self, TestPool};

/// The nodes of the large run: each links one live task that failed once
/// and waits for its retry.
const BULK_NODES: usize = 1_000;

/// The most bytes of the JSON of the large run detail. The test prints the
/// size it measured.
const BULK_RESPONSE_BOUND_BYTES: usize = 4 * 1024 * 1024;

/// The phases of each node of the large run, in order: the failed attempt
/// (queued, claimed, running, finalizing), the retry wait, then the open
/// queue wait of the retry.
const BULK_NODE_PHASES: [&str; 6] = [
    "enqueued",
    "claimed",
    "running",
    "finalizing",
    "retry_wait",
    "enqueued",
];

/// Moves the start of the lifecycle record (the bookkeeping row of migration
/// 1001) one day back, so that the runs that a test seeds with past times
/// began after it.
async fn start_capture_a_day_ago(pool: &TestPool) {
    let moved = sql::query(
        "UPDATE horsies_migrations SET installed_on = NOW() - interval '1 day' WHERE version = 1001",
    )
    .execute(pool)
    .await
    .expect("move the capture start");
    assert_eq!(
        moved.rows_affected(),
        1,
        "the bookkeeping row of migration 1001"
    );
}

/// One run of [`BULK_NODES`] nodes, in one simple-query message (one
/// transaction, one `NOW()`). Node `i` links the live task `i`, which
/// failed its first attempt and waits for its retry; the attempt row holds
/// its phase record.
fn bulk_run_sql(run_id: Uuid) -> String {
    let last = BULK_NODES - 1;
    format!(
        "INSERT INTO horsies_workflows (
            id, name, definition_key, status, on_error, output_task_index, success_policy,
            result, error, parent_workflow_id, parent_task_index, root_workflow_id, depth,
            sent_at, created_at, started_at, completed_at, updated_at
        ) VALUES (
            '{run_id}', 'timing_bulk_flow', 'tests.alpha.v1', 'RUNNING', 'fail', NULL, NULL,
            NULL, NULL, NULL, NULL, '{run_id}', 0,
            NOW() - interval '600 seconds', NOW() - interval '600 seconds',
            NOW() - interval '600 seconds', NULL, NOW()
        );
        INSERT INTO horsies_workflow_tasks (
            downstream_subworkflow, sub_workflow_params,
            id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
            dependencies, args_from, workflow_ctx_from, allow_failed_deps, join_type,
            min_success, task_options, max_retries, execution, good_until,
            retention_class_key, status, task_id, is_subworkflow, sub_workflow_id,
            sub_workflow_name, sub_definition_key, sub_workflow_summary, result, error,
            created_at, started_at, completed_at
        )
        SELECT 'NONE', NULL,
               md5('{run_id}:node:' || i)::uuid, '{run_id}', i, NULL, 'timing_bulk_task', '{{}}',
               'default', 100,
               '{{}}'::integer[], NULL, NULL, FALSE, 'all',
               NULL, NULL, 3, 'ISOLATED', NULL,
               'forever', 'ENQUEUED', md5('{run_id}:task:' || i)::uuid, FALSE, NULL,
               NULL, NULL, NULL, NULL, NULL,
               NOW() - interval '600 seconds', NOW() - interval '600 seconds', NULL
        FROM generate_series(0, {last}) AS i;
        INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, task_options, execution,
            retention_class_key, sent_at, created_at,
            command_fingerprint_version, command_fingerprint, idempotency_key_digest,
            status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
            started_at, finalizing_at, finalizing_by_worker_id,
            worker_pid, worker_hostname, worker_process_name,
            retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
            updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id,
            trace_carrier, trace_id, attempt_claims, first_claimed_at
        )
        SELECT md5('{run_id}:task:' || i)::uuid, 'timing_bulk_task', 'default', 100, '{{}}', NULL,
               'ISOLATED', 'forever', NOW() - interval '600 seconds', NOW() - interval '600 seconds',
               NULL, NULL, NULL,
               'PENDING', NOW() - interval '300 seconds', NULL, NULL, NULL,
               NULL, NULL, NULL,
               NULL, NULL, NULL,
               1, 3, NULL, NULL, TRUE,
               NOW(), FALSE, NULL, NULL,
               NULL, NULL, 0, NULL
        FROM generate_series(0, {last}) AS i;
        INSERT INTO horsies_task_attempts (
            task_id, attempt, outcome, will_retry, started_at, finished_at,
            error_code, error_message, failed_reason, worker_id,
            worker_hostname, worker_pid, worker_process_name,
            phase_record_version, enqueued_at, claimed_at, attempt_claims, first_claimed_at,
            placed_at, finalizing_at, recorded_by, last_seen_at
        )
        SELECT md5('{run_id}:task:' || i)::uuid, 1, 'FAILED', TRUE,
               NOW() - interval '560 seconds', NOW() - interval '400 seconds',
               'TASK_EXCEPTION', 'boom', NULL, 'worker-1',
               'host-1', 4242, 'monitoring-spec-process',
               1, NOW() - interval '600 seconds', NOW() - interval '580 seconds', 1,
               NOW() - interval '580 seconds', NOW() - interval '570 seconds',
               NOW() - interval '410 seconds', 'WORKER', NULL
        FROM generate_series(0, {last}) AS i;"
    )
}

fn phase_name(phase: &SegmentPhaseInfo) -> &'static str {
    match phase {
        SegmentPhaseInfo::Pending => "pending",
        SegmentPhaseInfo::Ready => "ready",
        SegmentPhaseInfo::Enqueued => "enqueued",
        SegmentPhaseInfo::RetryWait => "retry_wait",
        SegmentPhaseInfo::Claimed { .. } => "claimed",
        SegmentPhaseInfo::Running => "running",
        SegmentPhaseInfo::Finalizing => "finalizing",
        SegmentPhaseInfo::Advancing => "advancing",
        SegmentPhaseInfo::Unknown { .. } => "unknown",
    }
}

/// The run read is one statement: the statement calls the run function
/// once, and the function reads with a fixed number of statements
/// (`monitoring_timing_cases.sql`).
#[test]
fn the_run_read_is_one_call_of_the_run_function() {
    assert_eq!(
        MONITORING_WORKFLOW_RUN
            .sql()
            .matches("horsies_monitoring_workflow_run(")
            .count(),
        1
    );
}

/// A run of 1,000 nodes, each with one retry: one read composes every node
/// from the rows of one statement, and the size of its JSON is bounded.
#[tokio::test]
async fn a_thousand_node_run_with_one_retry_each_is_composed_from_one_read() {
    let database = SpecDatabase::create().await;
    start_capture_a_day_ago(&database.pool).await;
    let run_id = Uuid::new_v4();
    let _seeded = sql::raw_sql(&bulk_run_sql(run_id))
        .execute(&database.pool)
        .await
        .expect("seed the run");

    let detail = get_workflow_run(&database.broker(), run_id)
        .await
        .expect("read the run")
        .expect("the run exists");

    assert_eq!(detail.nodes.len(), BULK_NODES);
    for node in &detail.nodes {
        let segments = match &node.timing {
            NodeTimingInfo::Exact { segments, .. } => segments,
            other => panic!("node {}: {other:?}", node.task_index),
        };
        let names: Vec<&str> = segments
            .iter()
            .map(|segment| phase_name(&segment.phase))
            .collect();
        assert_eq!(names, BULK_NODE_PHASES, "node {}", node.task_index);
        assert!(
            matches!(
                segments.last().map(|segment| segment.end),
                Some(SegmentEndInfo::Open)
            ),
            "node {}",
            node.task_index
        );
    }
    assert!(
        matches!(
            &detail.timing,
            RunTimingInfo::Composed {
                coverage: RunCoverageInfo::Exact,
                open_segments: true,
                ..
            }
        ),
        "{:?}",
        detail.timing
    );
    let size = serde_json::to_vec(&detail)
        .expect("serialize the run detail")
        .len();
    eprintln!("the run detail of {BULK_NODES} nodes with one retry each: {size} bytes");
    assert!(size <= BULK_RESPONSE_BOUND_BYTES, "{size} bytes");
    database.drop().await;
}

/// A run created before the start of the lifecycle record has no timing:
/// the run and each node are unavailable.
#[tokio::test]
async fn a_run_created_before_the_capture_start_is_unavailable() {
    let database = SpecDatabase::create().await;
    let moved =
        sql::query("UPDATE horsies_migrations SET installed_on = NOW() WHERE version = 1001")
            .execute(&database.pool)
            .await
            .expect("move the capture start");
    assert_eq!(moved.rows_affected(), 1);
    let run = RunSeed::running("timing_before_capture");
    seed_run(&database.pool, &run).await;

    let detail = get_workflow_run(&database.broker(), run.id)
        .await
        .expect("read the run")
        .expect("the run exists");

    assert!(
        matches!(
            detail.timing,
            RunTimingInfo::Unavailable {
                reason: UnavailableReason::RunBeforeCapture,
                ..
            }
        ),
        "{:?}",
        detail.timing
    );
    database.drop().await;
}

/// A task that failed through the terminal write of a run: its attempt row
/// holds the phase record that the writer filled. The seeded row counts no
/// claim (it names no claim count) and the test writer observes no
/// placement.
#[tokio::test]
async fn the_task_detail_carries_the_phase_record_of_each_attempt() {
    let database = SpecDatabase::create().await;
    let task = LiveTask::running("timing_detail_task", "worker-1");
    seed_terminal(&database.pool, &task, Ending::failed("TASK_EXCEPTION")).await;

    let detail = match get_task_detail(&database.broker(), task.id)
        .await
        .expect("read the task")
    {
        TaskDetailRead::Found(detail) => detail,
        TaskDetailRead::NotFound(absent) => panic!("the task is in history: {absent:?}"),
    };

    match detail.attempts.as_slice() {
        [attempt] => assert!(
            matches!(
                attempt.phases,
                AttemptPhasesInfo::Captured {
                    claims: AttemptClaimsInfo::NotCounted,
                    finalizing: AttemptFinalizingInfo::NotMarked,
                    recorded_by: AttemptRecordedByInfo::Worker {
                        placement: AttemptPlacementInfo::NotObserved,
                    },
                    ..
                }
            ),
            "{attempt:?}"
        ),
        other => panic!("one attempt: {other:?}"),
    }
    database.drop().await;
}
