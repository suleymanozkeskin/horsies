//! The start of a workflow task in a Rust child and its node, on the test
//! database: `horsies_start_attempt` hands the node over to RUNNING with the
//! row, keeps a node start that a replay finds, does not touch a workflow
//! for a plain task, and misses with `NODE_NOT_LINKED` when the node is not
//! open; the child's stop before start then cancels the owned orphan when
//! the orphan self-heal of the run is on, and leaves the row CLAIMED when it
//! is off (Python `test_workflow_node_handoff.py`).
//!
//! These tests drove the worker's in-process start before it was removed
//! (#764); they now drive the start of the child.

use std::os::unix::net::UnixStream;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serial_test::serial;
use uuid::Uuid;

use super::host::{ChildApp, ChildLogSetup, ChildStartHook};
use super::link::FrameWriter;
use super::run::{start_attempt, HostContext, RunFacts, Started};
use crate::broker::claims::{RunWorkflow, WorkflowNodeIndex};
use crate::broker::{OrphanSelfHeal, PostgresBroker};
use crate::core::registry::task::TaskRegistry;
use crate::test_support::sql::{self, TestPool};
use crate::worker::executor::protocol::StopReason;

const WORKER_ID: &str = "w1";
const TASK_NAME: &str = "p1_task";

/// A child context on `pool`; its frames go to a socket no one reads.
fn context(pool: TestPool) -> (HostContext, UnixStream) {
    let (ours, theirs) = UnixStream::pair().expect("a socket pair");
    let writer = FrameWriter::start(ours, 16).expect("the frame writer starts");
    let context = HostContext {
        broker: Arc::new(PostgresBroker::from_test_url(
            pool.url(),
            crate::ClientFamily::Rust,
        )),
        app: Arc::new(ChildApp {
            registry: Arc::new(TaskRegistry::new()),
            hostname: "h1".to_owned(),
            start_hook: ChildStartHook::NoHook,
            producer_manifest: super::producer::test_producer_manifest(),
            log_setup: ChildLogSetup::NoSetup,
            family: crate::ClientFamily::Rust,
            body_scope: super::BodyScope::NoScope,
            handler_child: super::HandlerChildApp::without_handlers(),
        }),
        pid: 1,
        writer,
    };
    (context, theirs)
}

/// A CLAIMED row of [`WORKER_ID`]; a workflow task carries no command
/// fingerprint. Returns the claim generation.
async fn seed_claimed(pool: &TestPool, task_id: Uuid, is_workflow_task: bool) -> DateTime<Utc> {
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
        ) VALUES (
            $1, 'p1_task', 'default', 100, 'null', NULL, 'ISOLATED',
            'standard_30d', NOW(), NOW(),
            CASE WHEN $2 THEN NULL ELSE 1 END,
            CASE WHEN $2 THEN NULL ELSE decode(repeat('00', 32), 'hex') END, NULL,
            'CLAIMED', NOW(), NOW(), $3, NOW() + interval '1 minute',
            NULL, NULL, NULL,
            NULL, NULL, NULL,
            0, 3, NULL, NULL, $2,
            NOW(), FALSE, NULL, NULL
        ) RETURNING claimed_at",
    )
    .bind(task_id)
    .bind(is_workflow_task)
    .bind(WORKER_ID)
    .fetch_one(pool)
    .await
    .expect("seed a CLAIMED row")
}

async fn seed_workflow(pool: &TestPool, workflow_id: Uuid) {
    sql::query(
        "INSERT INTO horsies_workflows (
            id, name, status, on_error, output_task_index, definition_key, depth,
            root_workflow_id, sent_at, created_at, started_at, updated_at
        ) VALUES ($1, 'p1_wf', 'RUNNING', 'fail', NULL, 'test.p1.v1', 0, $1,
                  NOW(), NOW(), NOW(), NOW())",
    )
    .bind(workflow_id)
    .execute(pool)
    .await
    .expect("insert the workflow");
}

/// The node of `task_id` at index 0 in `status`, with `started_at`.
async fn seed_node(
    pool: &TestPool,
    node_id: Uuid,
    workflow_id: Uuid,
    task_id: Uuid,
    status: &str,
    started_at: Option<DateTime<Utc>>,
) {
    sql::query(
        "INSERT INTO horsies_workflow_tasks (
            downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
            queue_name, priority, dependencies, allow_failed_deps, join_type, status,
            is_subworkflow, task_id, started_at, created_at, max_retries, execution,
            retention_class_key
        ) VALUES (
            'NONE', $1, $2, 0, 'node_0', 'p1_task', 'null', 'default', 100, '{}', FALSE,
            'all', $3, FALSE, $4, $5, NOW(), 0, 'ISOLATED', 'standard_30d'
        )",
    )
    .bind(node_id)
    .bind(workflow_id)
    .bind(status)
    .bind(task_id)
    .bind(started_at)
    .execute(pool)
    .await
    .expect("insert the node");
}

/// The facts of a run of the seeded row with the orphan self-heal
/// `self_heal`.
fn facts_with(task_id: Uuid, claimed_at: DateTime<Utc>, self_heal: OrphanSelfHeal) -> RunFacts {
    RunFacts {
        task_id,
        task_name: TASK_NAME.to_owned(),
        queue_name: crate::events::QueueName::cut("default"),
        attempt: crate::events::AttemptNumber::SMALLEST,
        worker_id: WORKER_ID.to_owned(),
        claimed_at,
        orphan_self_heal: self_heal,
    }
}

/// [`facts_with`] with the orphan self-heal on (the default recovery
/// config).
fn facts(task_id: Uuid, claimed_at: DateTime<Utc>) -> RunFacts {
    facts_with(task_id, claimed_at, OrphanSelfHeal::On)
}

async fn remove_case(pool: &TestPool, workflow_id: Option<Uuid>, task_id: Uuid) {
    if let Some(workflow_id) = workflow_id {
        sql::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(workflow_id)
            .execute(pool)
            .await
            .expect("remove the nodes");
    }
    for statement in [
        "DELETE FROM horsies_heartbeats WHERE task_id = $1",
        "DELETE FROM horsies_tasks WHERE id = $1",
        "DELETE FROM horsies_task_history WHERE task_id = $1",
    ] {
        sql::query(statement)
            .bind(task_id)
            .execute(pool)
            .await
            .expect("remove the task");
    }
    if let Some(workflow_id) = workflow_id {
        sql::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(pool)
            .await
            .expect("remove the workflow");
    }
}

/// The start of a workflow task hands its node over to RUNNING, stamps the
/// node's first start, and gives the node: its workflow, its index and the
/// root of its tree (the workflow itself at depth 0).
#[tokio::test]
#[serial]
async fn workflow_task_still_transitions_node_to_running() {
    let pool = crate::test_support::migrated_pool().await;
    let workflow_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    seed_workflow(&pool, workflow_id).await;
    seed_node(
        &pool,
        Uuid::new_v4(),
        workflow_id,
        task_id,
        "ENQUEUED",
        None,
    )
    .await;
    let claimed_at = seed_claimed(&pool, task_id, true).await;
    let (context, _peer) = context(pool.clone());

    let started = start_attempt(&context, &facts(task_id, claimed_at)).await;

    match started {
        Started::Running { workflow, .. } => assert_eq!(
            workflow,
            RunWorkflow::InWorkflow {
                workflow_id,
                node_index: WorkflowNodeIndex::SMALLEST,
                root_workflow_id: workflow_id,
            }
        ),
        Started::Stopped(reason) => panic!("a linked workflow task starts, not {reason:?}"),
    }
    let (node_status, node_started_at): (String, Option<DateTime<Utc>>) = sql::query_as(
        "SELECT status, started_at FROM horsies_workflow_tasks WHERE workflow_id = $1",
    )
    .bind(workflow_id)
    .fetch_one(&pool)
    .await
    .expect("read the node");
    assert_eq!(
        node_status, "RUNNING",
        "workflow node must transition to RUNNING (gate must not skip it)"
    );
    assert!(
        node_started_at.is_some(),
        "first RUNNING handoff must stamp workflow-node started_at",
    );
    remove_case(&pool, Some(workflow_id), task_id).await;
}

/// The start of a node that is RUNNING already keeps its first start and
/// writes no new node tuple.
#[tokio::test]
#[serial]
async fn running_replay_preserves_the_first_node_start() {
    let pool = crate::test_support::migrated_pool().await;
    let workflow_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    let first_started_at =
        DateTime::<Utc>::from_timestamp_micros(Utc::now().timestamp_micros() - 3_600_000_000)
            .expect("a time an hour ago");
    seed_workflow(&pool, workflow_id).await;
    seed_node(
        &pool,
        Uuid::new_v4(),
        workflow_id,
        task_id,
        "RUNNING",
        Some(first_started_at),
    )
    .await;
    let claimed_at = seed_claimed(&pool, task_id, true).await;
    let before: String =
        sql::query_scalar("SELECT ctid::text FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(workflow_id)
            .fetch_one(&pool)
            .await
            .expect("read the node tuple");
    let (context, _peer) = context(pool.clone());

    let started = start_attempt(&context, &facts(task_id, claimed_at)).await;

    assert!(matches!(started, Started::Running { .. }));
    let persisted: DateTime<Utc> =
        sql::query_scalar("SELECT started_at FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(workflow_id)
            .fetch_one(&pool)
            .await
            .expect("read the node start");
    assert_eq!(persisted, first_started_at);
    let after: String =
        sql::query_scalar("SELECT ctid::text FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(workflow_id)
            .fetch_one(&pool)
            .await
            .expect("read the node tuple");
    assert_eq!(
        before, after,
        "RUNNING replay must not create a new node tuple"
    );
    remove_case(&pool, Some(workflow_id), task_id).await;
}

/// The start of a plain task sets its row RUNNING with no workflow write.
#[tokio::test]
#[serial]
async fn plain_task_starts_running_without_workflow_update() {
    let pool = crate::test_support::migrated_pool().await;
    let task_id = Uuid::new_v4();
    let claimed_at = seed_claimed(&pool, task_id, false).await;
    let (context, _peer) = context(pool.clone());

    let started = start_attempt(&context, &facts(task_id, claimed_at)).await;

    assert!(matches!(started, Started::Running { .. }));
    let status: String = sql::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(&pool)
        .await
        .expect("read the row");
    assert_eq!(status, "RUNNING");
    remove_case(&pool, None, task_id).await;
}

/// The handoff of each node state: an open node (`ENQUEUED`, `READY`,
/// `PENDING`, `RUNNING`) goes RUNNING with the row; a terminal node, or no
/// node, is a missed start (`NODE_NOT_LINKED`) that leaves the node as it
/// is, and the stop before start cancels the owned orphan. A node update
/// that holds the node's lock makes the start wait, and the start then sees
/// the new state.
#[tokio::test]
#[serial]
async fn handoff_preserves_link_states_and_observes_terminal_nodes() {
    let pool = crate::test_support::migrated_pool().await;
    for status in [
        Some("ENQUEUED"),
        Some("READY"),
        Some("PENDING"),
        Some("RUNNING"),
        Some("COMPLETED"),
        Some("FAILED"),
        Some("SKIPPED"),
        None,
    ] {
        let workflow_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        let node_id = Uuid::new_v4();
        seed_workflow(&pool, workflow_id).await;
        if let Some(status) = status {
            let started_at = DateTime::<Utc>::from_timestamp(1_767_225_600, 0);
            seed_node(&pool, node_id, workflow_id, task_id, status, started_at).await;
        }
        let claimed_at = seed_claimed(&pool, task_id, true).await;
        let (context, _peer) = context(pool.clone());

        let started = start_attempt(&context, &facts(task_id, claimed_at)).await;
        let outcome = match started {
            Started::Running { .. } => "running",
            Started::Stopped(StopReason::OrphanCancelled) => "aborted",
            Started::Stopped(other) => panic!("unexpected stop of the start: {other:?}"),
        };

        let persisted: Option<String> =
            sql::query_scalar("SELECT status FROM horsies_workflow_tasks WHERE id = $1")
                .bind(node_id)
                .fetch_optional(&pool)
                .await
                .expect("read the node");
        let task_state: Option<String> =
            sql::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
                .bind(task_id)
                .fetch_optional(&pool)
                .await
                .expect("read the row");
        let history: Option<String> =
            sql::query_scalar("SELECT status FROM horsies_task_history WHERE task_id = $1")
                .bind(task_id)
                .fetch_optional(&pool)
                .await
                .expect("read the history row");
        match status {
            Some("ENQUEUED" | "READY" | "PENDING" | "RUNNING") => {
                assert_eq!(outcome, "running");
                assert_eq!(persisted.as_deref(), Some("RUNNING"));
                assert_eq!(task_state.as_deref(), Some("RUNNING"));
                assert!(history.is_none());
            }
            _ => {
                assert_eq!(outcome, "aborted");
                assert_eq!(persisted.as_deref(), status);
                assert!(task_state.is_none());
                assert_eq!(history.as_deref(), Some("CANCELLED"));
            }
        }
        remove_case(&pool, Some(workflow_id), task_id).await;
    }

    let workflow_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();
    seed_workflow(&pool, workflow_id).await;
    seed_node(&pool, node_id, workflow_id, task_id, "ENQUEUED", None).await;
    let claimed_at = seed_claimed(&pool, task_id, true).await;
    let mut held = pool.begin().await.expect("begin the node update");
    sql::query("UPDATE horsies_workflow_tasks SET status = 'FAILED' WHERE id = $1")
        .bind(node_id)
        .execute(&mut *held)
        .await
        .expect("update the node");
    let (context, _peer) = context(pool.clone());
    let handoff =
        tokio::spawn(async move { start_attempt(&context, &facts(task_id, claimed_at)).await });
    let mut waited = false;
    for _ in 0..500 {
        waited = sql::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity
                           WHERE datname = current_database()
                             AND wait_event_type = 'Lock' AND pid <> pg_backend_pid())",
        )
        .fetch_one(&pool)
        .await
        .expect("read the lock waits");
        if waited {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(waited, "handoff must wait for the node update");
    held.commit().await.expect("commit the node update");
    let started = handoff.await.expect("the start ends");
    assert!(matches!(
        started,
        Started::Stopped(StopReason::OrphanCancelled)
    ));
    let node_status: String =
        sql::query_scalar("SELECT status FROM horsies_workflow_tasks WHERE id = $1")
            .bind(node_id)
            .fetch_one(&pool)
            .await
            .expect("read the node");
    assert_eq!(node_status, "FAILED");
    remove_case(&pool, Some(workflow_id), task_id).await;
}

/// With the orphan self-heal of the run on, the start of a workflow task
/// that no node links ends `ORPHAN_CANCELLED`, and the stop before start
/// moved the row to history as CANCELLED with `WORKFLOW_CHECK_FAILED`, in
/// the child.
#[tokio::test]
#[serial]
async fn an_unlinked_task_is_cancelled_in_the_child_when_the_self_heal_is_on() {
    let pool = crate::test_support::migrated_pool().await;
    let task_id = Uuid::new_v4();
    let claimed_at = seed_claimed(&pool, task_id, true).await;
    let (context, _peer) = context(pool.clone());

    let started = start_attempt(
        &context,
        &facts_with(task_id, claimed_at, OrphanSelfHeal::On),
    )
    .await;

    assert!(matches!(
        started,
        Started::Stopped(StopReason::OrphanCancelled)
    ));
    let (status, error_code): (String, Option<String>) = sql::query_as(
        "SELECT status::text, error_code FROM horsies_task_history WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(&pool)
    .await
    .expect("read the history row");
    assert_eq!(status, "CANCELLED");
    assert_eq!(error_code.as_deref(), Some("WORKFLOW_CHECK_FAILED"));
    remove_case(&pool, None, task_id).await;
}

/// With the orphan self-heal of the run off, the start ends
/// `NODE_NOT_LINKED`, and the row stays CLAIMED by this claim with no
/// history row.
#[tokio::test]
#[serial]
async fn an_unlinked_task_stays_claimed_when_the_self_heal_is_off() {
    let pool = crate::test_support::migrated_pool().await;
    let task_id = Uuid::new_v4();
    let claimed_at = seed_claimed(&pool, task_id, true).await;
    let (context, _peer) = context(pool.clone());

    let started = start_attempt(
        &context,
        &facts_with(task_id, claimed_at, OrphanSelfHeal::Off),
    )
    .await;

    assert!(matches!(
        started,
        Started::Stopped(StopReason::NodeNotLinked)
    ));
    let (status, holder): (String, Option<String>) =
        sql::query_as("SELECT status::text, claimed_by_worker_id FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .expect("read the row");
    assert_eq!(status, "CLAIMED");
    assert_eq!(holder.as_deref(), Some(WORKER_ID));
    let history: Option<String> =
        sql::query_scalar("SELECT status::text FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .fetch_optional(&pool)
            .await
            .expect("read the history row");
    assert_eq!(history, None);
    remove_case(&pool, None, task_id).await;
}
