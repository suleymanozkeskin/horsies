//! The finalize of a workflow node from the worker's side: phase 1 of a
//! success and its replay, the start check of a claimed node, and phase 2
//! with its capacity wake. Python:
//! `tests/integration/test_workflow_node_finalize.py`,
//! `test_workflow_node_handoff.py`, `test_worker_finalize_workflow_phase.py`.
//!
//! The worker's finalize of a result is phase 1 (`persist_terminal_state`),
//! then phase 2 (`finalize_workflow_phase`) when phase 1 owes it, as
//! `execute_and_finalize` and `run_phase2` run them. A test that leaves a
//! phase-2 record, a live claim or a trigger uses its own database.

use chrono::{DateTime, Utc};
use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::support::{
    broker, execute, finish_phase1, history_row, node, ok, poison_success_policy, seed_node,
    seed_node_task, seed_workflow, shared_pool, spec, start, task_node, workflow_status,
    IsolatedDatabase, LiveStatus, NodeSeed, Notifications, WorkflowSeed, WORKER,
};
use crate::broker::attempt_writes::{finish_workflow_success, FinishWorkflowSuccess};
use crate::broker::core_rows::StatementLog;
use crate::broker::lifecycle_tests::support::{
    database_options, unpinged_single_connection_pool, wait_until_idle, WireProxy,
};
use crate::broker::pools::core_pool_for_test_pool;
use crate::broker::{ClaimedTaskRow, PostgresBroker, SetRunningRow};
use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::lifecycle::{AttemptWriteRefusal, OwnedClaim, WorkflowSuccessOutcome};
use crate::core::{TaskResult, WorkflowSpecRegistry};
use crate::worker::execution::{
    confirm_ownership_and_set_running, finalize_workflow_phase, persist_terminal_state,
    FinalizeError, FinalizeOutcome, FinalizeStage, OwnershipOutcome,
};
use crate::worker::retry::read_stored_options;
use crate::workflow_engine::lifecycle::cancel_workflow;

/// The process facts that the start write records for [`WORKER`].
const WORKER_PID: i32 = 4242;
const WORKER_HOSTNAME: &str = "port-finalize-host";
const WORKER_PROCESS_NAME: &str = "port-finalize-process";

/// A queue name whose capacity channel (`task_queue_` and 60 bytes) is
/// longer than the 63 bytes of a PostgreSQL channel name: the server refuses
/// its NOTIFY (Python's `_TOO_LONG_QUEUE`).
const TOO_LONG_QUEUE: &str = "qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq";

/// The queue of the phase-2 wake tests, and its capacity channel.
const WAKE_QUEUE: &str = "port_finalize_wake";
const WAKE_CHANNEL: &str = "task_queue_port_finalize_wake";

/// Node A of an A -> B workflow, RUNNING under [`WORKER`], with the facts
/// that the claim and the start write give the worker.
struct RunningNode {
    workflow_id: Uuid,
    claimed: ClaimedTaskRow,
    running: SetRunningRow,
}

impl RunningNode {
    fn task_id(&self) -> Uuid {
        self.claimed.id
    }

    fn claimed_at(&self) -> DateTime<Utc> {
        self.claimed.claimed_at
    }
}

/// Starts a workflow A -> B: A is enqueued, B waits for it (Python's
/// `_start_chain`).
async fn start_chain(pool: &PgPool, name: &str) -> Uuid {
    let mut dependent = task_node(1, &format!("{name}_task"));
    dependent.dependencies = vec![0];
    start(
        pool,
        &spec(name, vec![task_node(0, &format!("{name}_task")), dependent]),
        &WorkflowSpecRegistry::new(),
    )
    .await
}

/// Node A and its task RUNNING under [`WORKER`], as a claim and the start
/// write leave them (Python's `_run_node_a`).
async fn run_node_a(pool: &PgPool, workflow_id: Uuid) -> RunningNode {
    let task_id = node(pool, workflow_id, 0)
        .await
        .task_id
        .expect("node A has a task");
    let held = sqlx::query(
        "UPDATE horsies_tasks
         SET status = 'RUNNING', claimed_at = NOW(), claimed_by_worker_id = $2,
             claim_expires_at = NULL, started_at = NOW(), worker_pid = $3,
             worker_hostname = $4, worker_process_name = $5, updated_at = NOW()
         WHERE id = $1 AND status = 'PENDING'",
    )
    .bind(task_id)
    .bind(WORKER)
    .bind(WORKER_PID)
    .bind(WORKER_HOSTNAME)
    .bind(WORKER_PROCESS_NAME)
    .execute(pool)
    .await
    .expect("run the task of node A");
    assert_eq!(held.rows_affected(), 1, "task {task_id} is not PENDING");
    execute(
        pool,
        &format!(
            "UPDATE horsies_workflow_tasks SET status = 'RUNNING', started_at = NOW()
             WHERE workflow_id = '{workflow_id}'::uuid AND task_index = 0"
        ),
    )
    .await;
    let claimed: ClaimedTaskRow = sqlx::query_as(
        "SELECT id, task_name, input, queue_name, is_workflow_task, task_options, claimed_at
         FROM horsies_tasks WHERE id = $1",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("read the claimed row");
    let running: SetRunningRow = sqlx::query_as(
        "SELECT id, started_at, retry_count, max_retries, good_until
         FROM horsies_tasks WHERE id = $1",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("read the running row");
    RunningNode {
        workflow_id,
        claimed,
        running,
    }
}

/// The success result of node A's task, as its bytes.
fn ok_bytes() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!(1)).expect("encode the result")
}

/// The success envelope that phase 1 stores.
fn ok_json() -> String {
    serde_json::to_string(&ok(serde_json::json!(1))).expect("encode the envelope")
}

/// The worker's finalize of node A's success under claim generation
/// `claimed_at`, on `pool` (Python's `_finalize`). Returns the outcome of
/// phase 1, or the error of the phase that failed.
async fn finalize(
    pool: &PgPool,
    node_a: &RunningNode,
    claimed_at: DateTime<Utc>,
) -> Result<FinalizeOutcome, FinalizeError> {
    let broker = PostgresBroker::from_pool(pool.clone());
    let claimed = ClaimedTaskRow {
        claimed_at,
        ..node_a.claimed.clone()
    };
    let outcome = persist_terminal_state(
        &broker,
        claimed.id,
        TaskResult::Ok(ok_bytes()),
        &claimed,
        &read_stored_options(claimed.task_options.as_deref()),
        &node_a.running,
        WORKER,
        &PayloadPolicy::default(),
    )
    .await?;
    let owed = match &outcome {
        FinalizeOutcome::Terminal {
            is_success,
            capacity_notified,
        } => Some((*is_success, *capacity_notified)),
        FinalizeOutcome::Retried | FinalizeOutcome::Finalized | FinalizeOutcome::Refused => None,
    };
    if let Some((is_success, capacity_notified)) = owed {
        finalize_workflow_phase(
            pool,
            &WorkflowSpecRegistry::new(),
            claimed.id,
            is_success,
            &claimed.queue_name,
            claimed.is_workflow_task,
            capacity_notified,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await?;
    }
    Ok(outcome)
}

/// A name for each finalize result, for assertion messages.
fn describe(result: &Result<FinalizeOutcome, FinalizeError>) -> String {
    match result {
        Ok(FinalizeOutcome::Terminal {
            is_success,
            capacity_notified,
        }) => format!(
            "Terminal {{ is_success: {is_success}, capacity_notified: {capacity_notified} }}"
        ),
        Ok(FinalizeOutcome::Retried) => "Retried".to_owned(),
        Ok(FinalizeOutcome::Finalized) => "Finalized".to_owned(),
        Ok(FinalizeOutcome::Refused) => "Refused".to_owned(),
        Err(error) => format!("Err({error})"),
    }
}

/// Whether phase 1 applied the success and phase 2 ran.
fn is_a_finalized_success(result: &Result<FinalizeOutcome, FinalizeError>) -> bool {
    matches!(
        result,
        Ok(FinalizeOutcome::Terminal {
            is_success: true,
            ..
        })
    )
}

/// (task_index, status, linked to a task) of each node (Python's `_nodes`).
async fn nodes(pool: &PgPool, workflow_id: Uuid) -> Vec<(i32, String, bool)> {
    sqlx::query_as(
        "SELECT task_index, status, task_id IS NOT NULL
         FROM horsies_workflow_tasks WHERE workflow_id = $1 ORDER BY task_index",
    )
    .bind(workflow_id)
    .fetch_all(pool)
    .await
    .expect("read the nodes")
}

fn completed_then_enqueued() -> Vec<(i32, String, bool)> {
    vec![
        (0, "COMPLETED".to_owned(), true),
        (1, "ENQUEUED".to_owned(), true),
    ]
}

/// The phase-2 records of a task.
async fn pending_rows(pool: &PgPool, task_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM horsies_workflow_phase2_pending WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("count the phase-2 records")
}

fn completed_locked() -> Option<(String, String)> {
    Some(("COMPLETED".to_owned(), "COMPLETE_LOCKED".to_owned()))
}

// ---------------------------------------------------------------------------
// test_workflow_node_finalize.py
// ---------------------------------------------------------------------------

/// `test_workflow_node_finalize.py::test_a_replay_of_a_committed_success_runs_phase_two`:
/// phase 1 committed and phase 2 never ran. The worker's finalize gets
/// `AlreadyApplied`, reads the phase-2 record and runs phase 2.
#[tokio::test]
#[serial]
async fn a_replay_of_a_committed_success_runs_phase_two() {
    let pool = shared_pool().await;
    let node_a = run_node_a(&pool, start_chain(&pool, "finalize_replay").await).await;
    let committed = finish_workflow_success(
        &core_pool_for_test_pool(&pool),
        StatementLog::Off,
        &FinishWorkflowSuccess {
            task_id: node_a.task_id(),
            fence: OwnedClaim {
                worker_id: WORKER.to_owned(),
                claimed_at: node_a.claimed_at(),
            },
            result_json: ok_json(),
        },
    )
    .await
    .expect("the success write");
    assert!(
        matches!(committed, WorkflowSuccessOutcome::SuccessApplied { .. }),
        "{committed:?}"
    );
    assert_eq!(pending_rows(&pool, node_a.task_id()).await, 1);
    assert_eq!(
        nodes(&pool, node_a.workflow_id).await[1],
        (1, "PENDING".to_owned(), false)
    );

    let finalized = finalize(&pool, &node_a, node_a.claimed_at()).await;

    assert!(
        is_a_finalized_success(&finalized),
        "{}",
        describe(&finalized)
    );
    assert_eq!(
        history_row(&pool, node_a.task_id()).await,
        completed_locked()
    );
    assert_eq!(
        nodes(&pool, node_a.workflow_id).await,
        completed_then_enqueued()
    );
    assert_eq!(pending_rows(&pool, node_a.task_id()).await, 0);
}

/// `test_workflow_node_finalize.py::test_a_lost_reply_is_retried_and_the_replay_runs_phase_two`:
/// the success commits and its reply never reaches the worker. Phase 1 is a
/// retryable error; its retry, the same statement on a new connection, is
/// `AlreadyApplied` and runs phase 2.
#[tokio::test]
#[serial]
async fn a_lost_reply_is_retried_and_the_replay_runs_phase_two() {
    let pool = shared_pool().await;
    let upstream = database_options(&pool);
    let proxy = WireProxy::start(&upstream).await;
    let proxied = unpinged_single_connection_pool(proxy.options(&upstream)).await;
    // Prepare the success statement on the proxied connection, so the write
    // is one request with one reply.
    let warm_up = finish_workflow_success(
        &core_pool_for_test_pool(&proxied),
        StatementLog::Off,
        &FinishWorkflowSuccess {
            task_id: Uuid::new_v4(),
            fence: OwnedClaim {
                worker_id: WORKER.to_owned(),
                claimed_at: Utc::now(),
            },
            result_json: ok_json(),
        },
    )
    .await
    .expect("warm up");
    assert!(
        matches!(
            warm_up,
            WorkflowSuccessOutcome::Refused(AttemptWriteRefusal::TaskAbsent { .. })
        ),
        "{warm_up:?}"
    );
    wait_until_idle(&proxied).await;
    let node_a = run_node_a(&pool, start_chain(&pool, "finalize_lost_reply").await).await;

    proxy.drop_next_reply();
    let first = finalize(&proxied, &node_a, node_a.claimed_at()).await;

    assert!(proxy.reply_was_dropped(), "the proxy relayed the reply");
    match &first {
        Err(error) => {
            assert_eq!(error.stage, FinalizeStage::Phase1Persist, "{error}");
            assert!(error.retryable, "{error}");
        }
        Ok(_) => panic!("finalize succeeded on a reply that was lost"),
    }
    // The premise of the replay: phase 1 committed, phase 2 did not run.
    assert_eq!(
        history_row(&pool, node_a.task_id()).await,
        completed_locked()
    );
    assert_eq!(pending_rows(&pool, node_a.task_id()).await, 1);

    let retry = finalize(&pool, &node_a, node_a.claimed_at()).await;

    assert!(is_a_finalized_success(&retry), "{}", describe(&retry));
    assert_eq!(
        nodes(&pool, node_a.workflow_id).await,
        completed_then_enqueued()
    );
    assert_eq!(pending_rows(&pool, node_a.task_id()).await, 0);
    proxied.close().await;
}

/// How the workflow of a running node ended.
#[derive(Debug, Clone, Copy)]
enum WorkflowEnd {
    /// A handle cancel.
    Cancelled,
    /// The FAILED row that the recovery limit leaves.
    Failed,
}

/// `test_workflow_node_finalize.py::test_a_node_of_an_ended_workflow_completes_and_enqueues_nothing`
/// (CANCELLED, FAILED): the workflow ended while node A ran. Phase 1
/// completes the task; phase 2 writes the node and consumes the phase-2
/// record, and the ended workflow promotes no dependent: B keeps the status
/// the end left it.
#[tokio::test]
#[serial]
#[ignore = "waits on #998"]
async fn a_node_of_an_ended_workflow_completes_and_enqueues_nothing() {
    let pool = shared_pool().await;
    for end in [WorkflowEnd::Cancelled, WorkflowEnd::Failed] {
        let node_a = run_node_a(&pool, start_chain(&pool, "finalize_ended").await).await;
        let ended_as = match end {
            WorkflowEnd::Cancelled => {
                let cancelled = cancel_workflow(&broker(&pool), node_a.workflow_id)
                    .await
                    .expect("cancel");
                assert!(cancelled, "{end:?}");
                "CANCELLED"
            }
            WorkflowEnd::Failed => {
                execute(
                    &pool,
                    &format!(
                        "UPDATE horsies_workflows
                         SET status = 'FAILED', completed_at = NOW(), updated_at = NOW()
                         WHERE id = '{}'::uuid",
                        node_a.workflow_id
                    ),
                )
                .await;
                "FAILED"
            }
        };
        let dependent_before = nodes(&pool, node_a.workflow_id).await[1].clone();

        let finalized = finalize(&pool, &node_a, node_a.claimed_at()).await;

        assert!(
            is_a_finalized_success(&finalized),
            "{end:?}: {}",
            describe(&finalized)
        );
        assert_eq!(
            history_row(&pool, node_a.task_id()).await,
            completed_locked(),
            "{end:?}"
        );
        assert_eq!(
            workflow_status(&pool, node_a.workflow_id).await,
            ended_as,
            "{end:?}"
        );
        assert_eq!(
            nodes(&pool, node_a.workflow_id).await,
            vec![(0, "COMPLETED".to_owned(), true), dependent_before.clone()],
            "{end:?}"
        );
        assert!(
            ["PENDING", "SKIPPED"].contains(&dependent_before.1.as_str()),
            "{end:?}: {dependent_before:?}"
        );
        assert!(!dependent_before.2, "{end:?}: {dependent_before:?}");
        assert_eq!(pending_rows(&pool, node_a.task_id()).await, 0, "{end:?}");
    }
}

// ---------------------------------------------------------------------------
// test_workflow_node_handoff.py
// ---------------------------------------------------------------------------

/// A RUNNING workflow whose COMPLETED node links a task that [`WORKER`]
/// claimed. Returns the workflow, the task and its claim generation.
async fn seed_claimed_task_of_a_completed_node(pool: &PgPool) -> (Uuid, Uuid, DateTime<Utc>) {
    let workflow_id = seed_workflow(pool, &WorkflowSeed::running()).await;
    let task_id = Uuid::new_v4();
    let claimed_at = seed_node_task(pool, task_id, LiveStatus::Claimed)
        .await
        .expect("a CLAIMED row has a claim generation");
    seed_node(
        pool,
        &NodeSeed::task(workflow_id, 0, "COMPLETED")
            .task_id(task_id)
            .result(ok_json()),
    )
    .await;
    (workflow_id, task_id, claimed_at)
}

/// The runner beats of a task (Python's `_beat_count`).
async fn runner_beats(pool: &PgPool, task_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM horsies_heartbeats WHERE task_id = $1 AND role = 'runner'",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("count the runner beats")
}

/// The status of a task's live row, or `None` when it has none.
async fn live_status(pool: &PgPool, task_id: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("read the live task status")
}

/// The start check of the worker for a claimed workflow task.
async fn start_check(
    pool: &PgPool,
    task_id: Uuid,
    claimed_at: DateTime<Utc>,
    orphan_self_heal: bool,
) -> OwnershipOutcome {
    confirm_ownership_and_set_running(
        &broker(pool),
        task_id,
        WORKER,
        WORKER_PID,
        WORKER_HOSTNAME,
        true,
        claimed_at,
        orphan_self_heal,
    )
    .await
}

/// `test_workflow_node_handoff.py::test_a_failed_workflow_check_keeps_no_beat`:
/// a claimed task whose node is already terminal fails the start check, and
/// no runner beat is written. With orphan self-heal on (the default), the
/// check cancels the task as an orphan; Python cancels it in the finalize of
/// the failed check.
#[tokio::test]
#[serial]
async fn a_failed_workflow_check_writes_no_runner_beat() {
    let db = IsolatedDatabase::create().await;
    let (_workflow_id, task_id, claimed_at) = seed_claimed_task_of_a_completed_node(&db.pool).await;

    let outcome = start_check(&db.pool, task_id, claimed_at, true).await;

    assert!(matches!(outcome, OwnershipOutcome::Aborted));
    assert_eq!(runner_beats(&db.pool, task_id).await, 0);
    assert_eq!(live_status(&db.pool, task_id).await, None);
    assert_eq!(
        history_row(&db.pool, task_id)
            .await
            .map(|(status, _kind)| status)
            .as_deref(),
        Some("CANCELLED")
    );
    db.drop().await;
}

/// `test_workflow_node_handoff.py::test_a_failed_workflow_check_keeps_no_beat`,
/// with orphan self-heal off: the failed check leaves the task CLAIMED, as
/// Python's check does, and writes no runner beat.
#[tokio::test]
#[serial]
#[ignore = "waits on #921"]
async fn a_failed_workflow_check_with_self_heal_off_keeps_the_claim_and_writes_no_beat() {
    let db = IsolatedDatabase::create().await;
    let (_workflow_id, task_id, claimed_at) = seed_claimed_task_of_a_completed_node(&db.pool).await;

    let outcome = start_check(&db.pool, task_id, claimed_at, false).await;

    assert!(matches!(outcome, OwnershipOutcome::Aborted));
    assert_eq!(
        live_status(&db.pool, task_id).await.as_deref(),
        Some("CLAIMED")
    );
    assert_eq!(runner_beats(&db.pool, task_id).await, 0);
    db.drop().await;
}

// ---------------------------------------------------------------------------
// test_worker_finalize_workflow_phase.py
// ---------------------------------------------------------------------------

/// A RUNNING workflow (with `success_policy`, if any) whose one RUNNING node
/// has a task that finished phase 1 with a success: phase 2 is owed. Returns
/// the workflow and the task.
async fn seed_owed_phase_two(
    pool: &PgPool,
    success_policy: Option<serde_json::Value>,
) -> (Uuid, Uuid) {
    let seed = match success_policy {
        Some(policy) => WorkflowSeed::running().policy(policy),
        None => WorkflowSeed::running(),
    };
    let workflow_id = seed_workflow(pool, &seed).await;
    let task_id = Uuid::new_v4();
    let _claimed_at = seed_node_task(pool, task_id, LiveStatus::Running).await;
    seed_node(
        pool,
        &NodeSeed::task(workflow_id, 0, "RUNNING").task_id(task_id),
    )
    .await;
    let _stored = finish_phase1(pool, task_id, &ok(serde_json::json!(1))).await;
    (workflow_id, task_id)
}

/// Phase 2 of a success of `task_id`, with its capacity wake on `queue_name`.
async fn phase_two(
    pool: &PgPool,
    task_id: Uuid,
    queue_name: &str,
    is_workflow_task: bool,
) -> Result<(), FinalizeError> {
    finalize_workflow_phase(
        pool,
        &WorkflowSpecRegistry::new(),
        task_id,
        true,
        queue_name,
        is_workflow_task,
        false,
        &PayloadPolicy::default(),
        &RetentionConfig::default(),
    )
    .await
}

fn assert_a_phase_two_error(result: &Result<(), FinalizeError>) -> &FinalizeError {
    match result {
        Err(error) => {
            assert_eq!(error.stage, FinalizeStage::Phase2Workflow, "{error}");
            error
        }
        Ok(()) => panic!("phase 2 succeeded"),
    }
}

/// `test_worker_finalize_workflow_phase.py::test_workflow_handler_exception_returns_err`:
/// a failure in the workflow advancement that is not a connection error is a
/// phase-2 error that is not retryable. The advancement fails on the stored
/// success policy, which does not parse (the completion check reads it).
#[tokio::test]
#[serial]
async fn a_failure_in_the_advancement_is_a_phase_two_error_that_is_not_retryable() {
    let db = IsolatedDatabase::create().await;
    let (workflow_id, task_id) = seed_owed_phase_two(&db.pool, Some(poison_success_policy())).await;

    let result = phase_two(&db.pool, task_id, WAKE_QUEUE, true).await;

    let error = assert_a_phase_two_error(&result);
    assert!(!error.retryable, "{error}");
    assert_eq!(node(&db.pool, workflow_id, 0).await.status, "RUNNING");
    assert_eq!(pending_rows(&db.pool, task_id).await, 1);
    db.drop().await;
}

/// `test_worker_finalize_workflow_phase.py::test_workflow_handler_exception_returns_err`,
/// for a database error: a refused node write (SQLSTATE `XX000`, not a
/// connection error) is a phase-2 error that is not retryable.
#[tokio::test]
#[serial]
async fn a_database_error_in_the_advancement_that_is_not_a_connection_error_is_not_retryable() {
    let db = IsolatedDatabase::create().await;
    let (_workflow_id, task_id) = seed_owed_phase_two(&db.pool, None).await;
    execute(
        &db.pool,
        "CREATE FUNCTION port_test_refuse_node_write() RETURNS trigger
         LANGUAGE plpgsql AS $body$
         BEGIN
             RAISE EXCEPTION 'node write refused by the test' USING ERRCODE = 'XX000';
         END
         $body$",
    )
    .await;
    execute(
        &db.pool,
        "CREATE TRIGGER port_test_refuse_node_write BEFORE UPDATE ON horsies_workflow_tasks
         FOR EACH ROW EXECUTE FUNCTION port_test_refuse_node_write()",
    )
    .await;

    let result = phase_two(&db.pool, task_id, WAKE_QUEUE, true).await;

    let error = assert_a_phase_two_error(&result);
    assert!(!error.retryable, "{error}");
    db.drop().await;
}

/// The node of `test_phase_two_wakes_the_queue_of_the_task_once`.
#[derive(Debug, Clone, Copy)]
enum WakeCase {
    /// A workflow task whose node phase 2 writes.
    Node,
    /// A task with no node.
    NoNode,
}

/// `test_worker_finalize_workflow_phase.py::test_phase_two_wakes_the_queue_of_the_task_once`
/// (`node`, `no_node`): phase 2 sends one capacity wake on the channel of
/// the task's queue, whether or not the task has a node.
#[tokio::test]
#[serial]
async fn phase_two_wakes_the_queue_of_the_task_once() {
    let pool = shared_pool().await;
    for case in [WakeCase::Node, WakeCase::NoNode] {
        let (task_id, is_workflow_task) = match case {
            WakeCase::Node => {
                let workflow_id = start(
                    &pool,
                    &spec("finalize_wake", vec![task_node(0, "finalize_wake_task")]),
                    &WorkflowSpecRegistry::new(),
                )
                .await;
                let task_id = node(&pool, workflow_id, 0)
                    .await
                    .task_id
                    .expect("the root has a task");
                let _stored = finish_phase1(&pool, task_id, &ok(serde_json::json!(1))).await;
                (task_id, true)
            }
            WakeCase::NoNode => (Uuid::new_v4(), false),
        };
        let mut listener = Notifications::listen(&pool, &[WAKE_CHANNEL]).await;

        let result = phase_two(&pool, task_id, WAKE_QUEUE, is_workflow_task).await;

        assert!(result.is_ok(), "{case:?}: {result:?}");
        assert_eq!(
            listener.received().await,
            vec![(WAKE_CHANNEL.to_owned(), format!("capacity:{task_id}"))],
            "{case:?}"
        );
    }
}

/// `test_worker_finalize_workflow_phase.py::test_phase_two_that_fails_sends_no_wake`:
/// a phase 2 that fails after the node write rolls the write back and sends
/// no wake.
#[tokio::test]
#[serial]
async fn a_phase_two_that_fails_sends_no_wake() {
    let db = IsolatedDatabase::create().await;
    let (workflow_id, task_id) = seed_owed_phase_two(&db.pool, Some(poison_success_policy())).await;
    let mut listener = Notifications::listen(&db.pool, &[WAKE_CHANNEL]).await;

    let result = phase_two(&db.pool, task_id, WAKE_QUEUE, true).await;

    let _error = assert_a_phase_two_error(&result);
    assert_eq!(listener.received().await, Vec::new());
    assert_eq!(node(&db.pool, workflow_id, 0).await.status, "RUNNING");
    db.drop().await;
}

/// `test_worker_finalize_workflow_phase.py::test_a_notify_that_fails_on_the_server_is_a_phase2_error`:
/// the node write and the wake share one transaction. A wake that the
/// server refuses is a phase-2 error, and the node write does not commit.
#[tokio::test]
#[serial]
#[ignore = "waits on #722"]
async fn a_wake_that_the_server_refuses_is_a_phase_two_error() {
    let db = IsolatedDatabase::create().await;
    let (workflow_id, task_id) = seed_owed_phase_two(&db.pool, None).await;

    let result = phase_two(&db.pool, task_id, TOO_LONG_QUEUE, true).await;

    let _error = assert_a_phase_two_error(&result);
    assert_eq!(node(&db.pool, workflow_id, 0).await.status, "RUNNING");
    assert_eq!(pending_rows(&db.pool, task_id).await, 1);
    db.drop().await;
}
