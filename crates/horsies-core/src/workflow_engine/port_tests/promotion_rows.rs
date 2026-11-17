//! The promotion of dependents: the join on the promotion path, what a
//! promoted node and its task row store, the guards of the promotion write,
//! and a run across levels. Python:
//! `tests/integration/test_workflow_promotion_batched.py`.
//!
//! Each node's task finishes as a worker finishes it (phase 1, then phase 2),
//! so a dependent is promoted by the core's phase 2. The nodes run on
//! [`PORT_QUEUE`], which no worker test claims.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::support::{
    args_from_inner, broker, code, err, execute, finish_node, finish_phase1, history_row, node,
    node_task_input, ok, seed_node, seed_workflow, shared_pool, spec, start, stored_result_error,
    task_node, try_finish_task, workflow_status, IsolatedDatabase, NodeSeed, Notifications,
    WorkflowSeed, PORT_QUEUE,
};
use crate::broker::PostgresBroker;
use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::workflow::node::JoinType;
use crate::core::{AnyNode, OnError, TaskResult, WorkflowSpec, WorkflowSpecRegistry};
use crate::workflow_engine::engine::on_workflow_task_complete;
use crate::workflow_engine::lifecycle::{pause_workflow, resume_workflow};
use crate::workflow_engine::recovery::recover_stuck_workflows;

/// Stored task options that do not parse (Python's `_corrupt_stored_options`).
const CORRUPT_TASK_OPTIONS: &str = "{not-json";

/// `workflow_recovery_failure_limit` of a recovery pass.
const RECOVERY_FAILURE_LIMIT: u32 = 3;

/// The advisory lock that holds the case-0 step at its first node write.
const STEP_HOLD_LOCK: i64 = 7_311_361;

/// Bound of the wait for a backend to block on another: 500 polls of 10 ms.
const BLOCK_POLLS: u32 = 500;
const BLOCK_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);

/// Bound of the recovery pass and the pause, from the start of the pass.
const RACE_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

fn after(index: usize, task_name: &str, dependencies: &[usize]) -> AnyNode {
    let mut node = task_node(index, task_name);
    node.dependencies = dependencies.to_vec();
    node
}

async fn run(pool: &PgPool, workflow_spec: &WorkflowSpec) -> Uuid {
    start(
        pool,
        workflow_spec,
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
    )
    .await
}

async fn succeed(pool: &PgPool, workflow_id: Uuid, index: i32, value: serde_json::Value) {
    let _task = finish_node(
        pool,
        workflow_id,
        index,
        &ok(value),
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
    )
    .await;
}

/// A root with three dependents (Python's `_start_root_with_three_dependents`).
fn root_with_three_dependents(name: &str) -> WorkflowSpec {
    let task_name = format!("{name}_task");
    spec(
        name,
        vec![
            after(0, &task_name, &[]),
            after(1, &task_name, &[0]),
            after(2, &task_name, &[0]),
            after(3, &task_name, &[0]),
        ],
    )
}

/// A node row and the live task row it links, if any.
#[derive(Debug, sqlx::FromRow)]
struct NodeAndTask {
    task_index: i32,
    status: String,
    task_id: Option<Uuid>,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    task_name: Option<String>,
    queue_name: Option<String>,
    task_status: Option<String>,
    enqueued_at: Option<DateTime<Utc>>,
    created_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
}

/// Every node of `workflow_id` by index (Python's `_node_rows`).
async fn node_rows(pool: &PgPool, workflow_id: Uuid) -> BTreeMap<i32, NodeAndTask> {
    let rows: Vec<NodeAndTask> = sqlx::query_as(
        "SELECT wt.task_index, wt.status, wt.task_id, wt.started_at, wt.completed_at,
                t.task_name, t.queue_name, t.status AS task_status, t.enqueued_at,
                t.created_at, t.updated_at
         FROM horsies_workflow_tasks wt
         LEFT JOIN horsies_tasks t ON t.id = wt.task_id
         WHERE wt.workflow_id = $1
         ORDER BY wt.task_index",
    )
    .bind(workflow_id)
    .fetch_all(pool)
    .await
    .unwrap_or_else(|error| panic!("read the nodes of {workflow_id}: {error}"));
    rows.into_iter().map(|row| (row.task_index, row)).collect()
}

/// The live task rows of the database.
async fn live_task_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM horsies_tasks")
        .fetch_one(pool)
        .await
        .expect("count the live task rows")
}

/// `TestBatchedPromotionSemantics.test_any_join_promotes_on_first_success`
#[tokio::test]
#[serial]
async fn join_any_is_enqueued_on_the_first_of_two_successes() {
    let pool = shared_pool().await;
    let mut joined = after(2, "promotion_any_task", &[0, 1]);
    joined.join = JoinType::Any;
    let workflow_id = run(
        &pool,
        &spec(
            "promotion_any",
            vec![
                after(0, "promotion_any_task", &[]),
                after(1, "promotion_any_task", &[]),
                joined,
            ],
        ),
    )
    .await;

    succeed(&pool, workflow_id, 0, serde_json::json!(0)).await;

    let rows = node_rows(&pool, workflow_id).await;
    assert_eq!(rows[&2].status, "ENQUEUED");
    assert_eq!(rows[&2].task_status.as_deref(), Some("PENDING"));
    assert_eq!(rows[&1].status, "ENQUEUED", "the other root still runs");
}

/// `TestBatchedPromotionSemantics.test_mixed_fast_and_ctx_dependents_both_promote`:
/// a dependent that reads the workflow context and a plain dependent of the
/// same level are both enqueued.
#[tokio::test]
#[serial]
async fn a_context_dependent_and_a_plain_dependent_of_one_level_are_both_enqueued() {
    let pool = shared_pool().await;
    let mut reader = after(2, "promotion_mixed_ctx_task", &[0]);
    reader.workflow_ctx_from = Some(vec!["node_0".to_owned()]);
    let workflow_id = run(
        &pool,
        &spec(
            "promotion_mixed",
            vec![
                after(0, "promotion_mixed_task", &[]),
                after(1, "promotion_mixed_task", &[0]),
                reader,
            ],
        ),
    )
    .await;

    succeed(&pool, workflow_id, 0, serde_json::json!(0)).await;

    let rows = node_rows(&pool, workflow_id).await;
    for index in [1, 2] {
        assert_eq!(rows[&index].status, "ENQUEUED", "node {index}");
        assert_eq!(
            rows[&index].task_status.as_deref(),
            Some("PENDING"),
            "node {index}"
        );
    }
}

/// `TestPromotionWrite.test_promoted_dependents_carry_the_transaction_time_and_the_insert_notifies`:
/// each promoted node and its task row carry the time of the phase-2
/// transaction that completed the root; each task insert sends `task_new`
/// and `horsies_task_status` with its id, and the queue wake `new` arrives
/// once (PostgreSQL delivers identical notifications of one transaction
/// once). Phase 2 runs alone after phase 1, as in Python's `complete_task`.
#[tokio::test]
#[serial]
async fn promoted_nodes_and_their_tasks_carry_the_transaction_time_and_each_insert_notifies() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = run(&db.pool, &root_with_three_dependents("promotion_notify")).await;
    let root_task = node(&db.pool, workflow_id, 0)
        .await
        .task_id
        .expect("the root has a task");
    let _ = finish_phase1(&db.pool, root_task, &ok(serde_json::json!(0))).await;
    let queue_channel = format!("task_queue_{PORT_QUEUE}");
    let mut listener = Notifications::listen(
        &db.pool,
        &[
            "task_new",
            queue_channel.as_str(),
            "horsies_task_status",
            "workflow_done",
            "horsies_workflow_status",
        ],
    )
    .await;

    on_workflow_task_complete(
        &PostgresBroker::from_pool(db.pool.clone()),
        root_task,
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
        &RetentionConfig::default(),
    )
    .await
    .expect("phase 2 of the root");
    let received = listener.received().await;

    let rows = node_rows(&db.pool, workflow_id).await;
    let promoted_at = rows[&0].completed_at.expect("the root completed");
    let mut expected = Vec::new();
    for index in [1, 2, 3] {
        let row = &rows[&index];
        assert_eq!(row.status, "ENQUEUED", "node {index}");
        assert_eq!(row.task_status.as_deref(), Some("PENDING"), "node {index}");
        assert_eq!(row.started_at, Some(promoted_at), "node {index}");
        assert_eq!(row.enqueued_at, Some(promoted_at), "node {index}");
        assert_eq!(row.created_at, Some(promoted_at), "node {index}");
        assert_eq!(row.updated_at, Some(promoted_at), "node {index}");
        assert_eq!(row.completed_at, None, "node {index}");
        let task_id = row.task_id.expect("a promoted node links its task");
        for channel in ["task_new", "horsies_task_status"] {
            expected.push((channel.to_owned(), task_id.to_string()));
        }
    }
    expected.push((queue_channel, "new".to_owned()));
    expected.sort();
    assert_eq!(received, expected);
    db.drop().await;
}

/// `TestPromotionWrite.test_the_promotion_write_moves_only_pending_nodes`:
/// when the root completes, node 1 is enqueued at the time of that
/// transaction; node 2, whose join cannot be met, is skipped with no start
/// time; node 3, which is no longer PENDING, is not written.
#[tokio::test]
#[serial]
async fn the_promotion_moves_only_pending_nodes() {
    let pool = shared_pool().await;
    let task_name = "promotion_write_task";
    let workflow_id = run(
        &pool,
        &spec(
            "promotion_write",
            vec![
                after(0, task_name, &[]),
                after(1, task_name, &[0]),
                after(2, task_name, &[0, 4]),
                after(3, task_name, &[0]),
                after(4, task_name, &[]),
            ],
        ),
    )
    .await;
    sqlx::query(
        "UPDATE horsies_workflow_tasks SET status = 'SKIPPED'
         WHERE workflow_id = $1 AND task_index = 3",
    )
    .bind(workflow_id)
    .execute(&pool)
    .await
    .expect("node 3 leaves PENDING");
    let _failed = finish_node(
        &pool,
        workflow_id,
        4,
        &err("PROMOTION_WRITE_FAIL", "forced"),
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
    )
    .await;

    succeed(&pool, workflow_id, 0, serde_json::json!(0)).await;

    let rows = node_rows(&pool, workflow_id).await;
    let promoted_at = rows[&0].completed_at.expect("the root completed");
    assert_eq!(rows[&1].status, "ENQUEUED");
    assert_eq!(rows[&1].started_at, Some(promoted_at));
    assert_eq!(rows[&1].task_name.as_deref(), Some(task_name));
    assert_eq!(rows[&1].queue_name.as_deref(), Some(PORT_QUEUE));
    assert_eq!(
        (rows[&2].status.as_str(), rows[&2].started_at),
        ("SKIPPED", None)
    );
    assert_eq!(
        (
            rows[&3].status.as_str(),
            rows[&3].started_at,
            rows[&3].completed_at,
            rows[&3].task_id
        ),
        ("SKIPPED", None, None, None),
        "node 3 was not written"
    );
}

/// The one backend that waits on a lock that `holder_pid` holds, polled up
/// to [`BLOCK_POLLS`] times.
///
/// Panics when no backend waits within the bound, or when more than one
/// backend waits.
async fn sole_waiter_on(pool: &PgPool, holder_pid: i32) -> i32 {
    for _ in 0..BLOCK_POLLS {
        let waiters: Vec<i32> = sqlx::query_scalar(
            "SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)) ORDER BY pid",
        )
        .bind(holder_pid)
        .fetch_all(pool)
        .await
        .expect("read the backends that wait on the holder");
        match waiters.as_slice() {
            [] => tokio::time::sleep(BLOCK_POLL_INTERVAL).await,
            [waiter] => return *waiter,
            several => panic!("backends {several:?} wait on backend {holder_pid}; one expected"),
        }
    }
    panic!("no backend waits on backend {holder_pid}");
}

/// Replaces the port of
/// `TestPromotionWrite.test_the_promotion_write_leaves_a_node_ready_when_the_workflow_is_not_running`
/// (`tests/integration/test_workflow_promotion_batched.py:578`). That test
/// covers a promotion whose caller does not hold the workflow row lock: a
/// pause can land after its dependents read, and the promoted nodes stay
/// READY with no task. Recovery case 0 runs
/// `horsies_workflow_node_reevaluate`, which locks the workflow row
/// `FOR UPDATE` before it reads or writes a node, so that interleaving
/// cannot happen on the step path. The guarantee that replaces it: a pause
/// that arrives during the step waits for the step's commit, the promoted
/// nodes end ENQUEUED with their task rows, and the pause applies after.
///
/// Node 1 waits for the FAILED node 0, so case 0 skips it; its dependents
/// (2, 3, 4, `allow_failed_deps`) are promoted by the skip cascade. A
/// trigger holds the step at its first node write, on an advisory lock that
/// the test holds. The pause starts while the step waits there, and the test
/// releases the step once the pause waits on the step.
#[tokio::test]
#[serial]
async fn a_pause_during_the_case_zero_step_applies_after_its_commit() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    let failure =
        serde_json::to_string(&err("PROMOTION_PAUSE_FAIL", "forced")).expect("encode the failure");
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 0, "FAILED").result(failure),
    )
    .await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 1, "PENDING").deps(&[0]),
    )
    .await;
    for index in [2, 3, 4] {
        seed_node(
            &db.pool,
            &NodeSeed::task(workflow_id, index, "PENDING")
                .deps(&[1])
                .allow_failed_deps(),
        )
        .await;
    }
    execute(
        &db.pool,
        &format!(
            "CREATE FUNCTION port_test_hold_step() RETURNS trigger
             LANGUAGE plpgsql AS $body$
             BEGIN
                 PERFORM pg_advisory_xact_lock({STEP_HOLD_LOCK});
                 RETURN NEW;
             END
             $body$"
        ),
    )
    .await;
    execute(
        &db.pool,
        "CREATE TRIGGER port_test_hold_step BEFORE UPDATE ON horsies_workflow_tasks
         FOR EACH ROW WHEN (OLD.status = 'PENDING' AND NEW.status <> 'PENDING')
         EXECUTE FUNCTION port_test_hold_step()",
    )
    .await;
    let mut holder = db.pool.begin().await.expect("begin the step holder");
    let _held = sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(STEP_HOLD_LOCK)
        .execute(&mut *holder)
        .await
        .expect("take the lock that holds the step");
    let holder_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *holder)
        .await
        .expect("read the backend pid of the holder");
    let recovery_broker = PostgresBroker::from_pool(db.pool.clone());
    let pause_broker = PostgresBroker::from_pool(db.pool.clone());
    let registry = WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
    let retention = RetentionConfig::default();

    // On the heap: the join and the timeout below move their futures, and in
    // a debug build each move of the pass future inline takes stack space.
    let recovery = Box::pin(recover_stuck_workflows(
        &recovery_broker,
        &registry,
        RECOVERY_FAILURE_LIMIT,
        &retention,
    ));
    let pause_during_the_step = async {
        let step_pid = sole_waiter_on(&db.pool, holder_pid).await;
        let release_once_the_pause_waits = async {
            let _pause_pid = sole_waiter_on(&db.pool, step_pid).await;
            assert_eq!(
                workflow_status(&db.pool, workflow_id).await,
                "RUNNING",
                "the pause applied while the step held the workflow row"
            );
            holder.commit().await.expect("release the step");
        };
        let (paused, ()) = tokio::join!(
            pause_workflow(&pause_broker, workflow_id),
            release_once_the_pause_waits,
        );
        paused
    };
    let (report, paused) = tokio::time::timeout(RACE_WAIT, async {
        tokio::join!(recovery, pause_during_the_step)
    })
    .await
    .expect("the step and the pause end once the step is released");

    let report = report.expect("the recovery pass returns its report");
    assert_eq!(
        (report.case0_pending_reevaluated, report.errors),
        (1, 0),
        "the case-0 step applied"
    );
    assert!(
        paused.expect("pause"),
        "the pause found the workflow RUNNING"
    );
    let rows = node_rows(&db.pool, workflow_id).await;
    assert_eq!(rows[&1].status, "SKIPPED");
    for index in [2, 3, 4] {
        let row = &rows[&index];
        assert_eq!(row.status, "ENQUEUED", "node {index}");
        assert!(row.task_id.is_some(), "node {index} has its task");
        assert!(row.started_at.is_some(), "node {index} has its start time");
        assert_eq!(row.task_status.as_deref(), Some("PENDING"), "node {index}");
    }
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "PAUSED");
    assert_eq!(
        live_task_count(&db.pool).await,
        3,
        "one task row per promoted node"
    );
    recovery_broker.close().await;
    pause_broker.close().await;
    db.drop().await;
}

/// `TestMultiLevelDag.test_fan_out_and_fan_in_across_levels_complete_the_workflow`:
/// A -> {B, C, D}; E waits for B and C and takes B's result; F waits for D
/// and E; F -> {G, H}; I waits for G and H. Each phase 2 enqueues exactly
/// the nodes whose dependencies all completed, at the time of its own
/// transaction, each with one task; the last one completes the workflow.
#[tokio::test]
#[serial]
async fn a_fan_out_and_fan_in_across_levels_enqueues_each_node_once_and_completes() {
    let pool = shared_pool().await;
    let task_name = "promotion_levels_task";
    let mut receiver = after(4, "promotion_levels_receiver", &[1, 2]);
    receiver.args_from.insert("upstream".to_owned(), 1);
    let workflow_id = run(
        &pool,
        &spec(
            "promotion_levels",
            vec![
                after(0, task_name, &[]),
                after(1, task_name, &[0]),
                after(2, task_name, &[0]),
                after(3, task_name, &[0]),
                receiver,
                after(5, task_name, &[3, 4]),
                after(6, task_name, &[5]),
                after(7, task_name, &[5]),
                after(8, task_name, &[6, 7]),
            ],
        ),
    )
    .await;
    // (completed node, nodes its phase 2 enqueues)
    let steps: [(i32, &[i32]); 9] = [
        (0, &[1, 2, 3]),
        (1, &[]),
        (2, &[4]),
        (3, &[]),
        (4, &[5]),
        (5, &[6, 7]),
        (6, &[]),
        (7, &[8]),
        (8, &[]),
    ];
    let mut enqueued: BTreeSet<i32> = BTreeSet::from([0]);

    for (completed, promoted) in steps {
        let before = node_rows(&pool, workflow_id).await;
        succeed(
            &pool,
            workflow_id,
            completed,
            serde_json::json!(completed * 10),
        )
        .await;
        let after_step = node_rows(&pool, workflow_id).await;

        let changed: BTreeSet<i32> = after_step
            .values()
            .filter(|row| row.status == "ENQUEUED" && before[&row.task_index].status != "ENQUEUED")
            .map(|row| row.task_index)
            .collect();
        let promoted: BTreeSet<i32> = promoted.iter().copied().collect();
        assert_eq!(changed, promoted, "after node {completed}");
        let completed_at = after_step[&completed].completed_at;
        for index in &promoted {
            assert_eq!(after_step[index].started_at, completed_at, "node {index}");
            assert_eq!(
                after_step[index].task_status.as_deref(),
                Some("PENDING"),
                "node {index}"
            );
        }
        enqueued.extend(promoted.iter().copied());
        let pending: BTreeSet<i32> = after_step
            .values()
            .filter(|row| row.status == "PENDING")
            .map(|row| row.task_index)
            .collect();
        let expected_pending: BTreeSet<i32> = (0..9).filter(|i| !enqueued.contains(i)).collect();
        assert_eq!(pending, expected_pending, "after node {completed}");
        if promoted.contains(&4) {
            let input = node_task_input(&pool, workflow_id, 4).await;
            match args_from_inner(&input, "upstream") {
                TaskResult::Ok(value) => assert_eq!(value, serde_json::json!(10), "B's result"),
                TaskResult::Err(error) => panic!("E received an error: {error:?}"),
            }
        }
    }

    let rows = node_rows(&pool, workflow_id).await;
    assert!(
        rows.values().all(|row| row.status == "COMPLETED"),
        "{rows:?}"
    );
    let task_ids: BTreeSet<Uuid> = rows
        .values()
        .map(|row| row.task_id.expect("each node linked its task"))
        .collect();
    assert_eq!(task_ids.len(), 9, "one task per node");
    for task_id in task_ids {
        assert_eq!(
            history_row(&pool, task_id).await,
            Some(("COMPLETED".to_owned(), "COMPLETE_LOCKED".to_owned())),
            "task {task_id}"
        );
    }
    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// Starts a root with a good and a bad dependent (Python's
/// `test_build_failure_fails_only_its_node` shape) and makes the stored
/// options of node 2 text that does not parse.
async fn start_with_a_corrupt_dependent(pool: &PgPool, on_error: OnError) -> Uuid {
    let mut workflow_spec = spec(
        "promotion_isolation",
        vec![
            after(0, "promotion_isolation_task", &[]),
            after(1, "promotion_isolation_task", &[0]),
            after(2, "promotion_isolation_corrupt", &[0]),
        ],
    );
    workflow_spec.on_error = on_error;
    let workflow_id = run(pool, &workflow_spec).await;
    sqlx::query(
        "UPDATE horsies_workflow_tasks SET task_options = $2
         WHERE workflow_id = $1 AND task_index = 2",
    )
    .bind(workflow_id)
    .bind(CORRUPT_TASK_OPTIONS)
    .execute(pool)
    .await
    .expect("corrupt the stored options of node 2");
    workflow_id
}

/// Finishes the root with success and checks that phase 2 applied.
async fn complete_the_root(pool: &PgPool, workflow_id: Uuid) {
    let root_task = node(pool, workflow_id, 0)
        .await
        .task_id
        .expect("the root has a task");
    try_finish_task(
        pool,
        root_task,
        &ok(serde_json::json!(0)),
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
    )
    .await
    .expect("phase 2 of the root applies; the bad node fails alone");
}

/// `TestBatchedPromotionSemantics.test_build_failure_fails_only_its_node`: a
/// dependent whose stored task options do not parse fails alone, with no
/// task; its sibling is enqueued.
#[tokio::test]
#[serial]
async fn a_dependent_whose_options_do_not_parse_fails_alone() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = start_with_a_corrupt_dependent(&db.pool, OnError::Fail).await;

    complete_the_root(&db.pool, workflow_id).await;

    let rows = node_rows(&db.pool, workflow_id).await;
    assert_eq!(rows[&1].status, "ENQUEUED");
    assert_eq!(rows[&2].status, "FAILED");
    assert_eq!(rows[&2].task_id, None);
    let stored = node(&db.pool, workflow_id, 2)
        .await
        .result
        .expect("the failed node stores its error");
    assert_eq!(
        code(&stored_result_error(&stored)).as_deref(),
        Some("WORKER_SERIALIZATION_ERROR")
    );
    db.drop().await;
}

/// `TestBatchedPromotionSemantics.test_pause_policy_build_failure_reverts_siblings_to_ready`:
/// under `on_error = pause`, the failure of the bad dependent pauses the
/// workflow and its sibling goes back to READY with no task and no start
/// time; a resume enqueues the sibling.
#[tokio::test]
#[serial]
async fn a_dependent_whose_options_do_not_parse_pauses_and_its_sibling_stays_ready() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = start_with_a_corrupt_dependent(&db.pool, OnError::Pause).await;

    complete_the_root(&db.pool, workflow_id).await;

    let rows = node_rows(&db.pool, workflow_id).await;
    let statuses: BTreeMap<i32, &str> = rows
        .iter()
        .map(|(index, row)| (*index, row.status.as_str()))
        .collect();
    assert_eq!(
        statuses,
        BTreeMap::from([(0, "COMPLETED"), (1, "READY"), (2, "FAILED")])
    );
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "PAUSED");
    assert_eq!((rows[&1].task_id, rows[&1].started_at), (None, None));

    let resumed = resume_workflow(
        &broker(&db.pool),
        workflow_id,
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
        &PayloadPolicy::default(),
        &RetentionConfig::default(),
    )
    .await
    .expect("resume");

    assert!(resumed);
    let resumed_rows = node_rows(&db.pool, workflow_id).await;
    assert_eq!(resumed_rows[&1].status, "ENQUEUED");
    assert_eq!(resumed_rows[&1].task_status.as_deref(), Some("PENDING"));
    db.drop().await;
}

/// `test_workflow_recovery_limit.py` (`BAD_GOOD_UNTIL`): a stored deadline
/// that does not parse is refused at promotion. Python passes the stored text
/// to the database, which refuses it, so the promotion raises; the dependent
/// never gets a task without its deadline.
#[tokio::test]
#[serial]
async fn a_dependent_whose_stored_deadline_does_not_parse_gets_no_task() {
    let db = IsolatedDatabase::create().await;
    for stored_options in [r#"{"good_until":"not-a-date"}"#, r#"{"good_until":12345}"#] {
        let workflow_id = run(
            &db.pool,
            &spec(
                "promotion_bad_deadline",
                vec![
                    after(0, "promotion_bad_deadline_task", &[]),
                    after(1, "promotion_bad_deadline_task", &[0]),
                ],
            ),
        )
        .await;
        sqlx::query(
            "UPDATE horsies_workflow_tasks SET task_options = $2
             WHERE workflow_id = $1 AND task_index = 1",
        )
        .bind(workflow_id)
        .bind(stored_options)
        .execute(&db.pool)
        .await
        .expect("store a deadline that does not parse");
        let root_task = node(&db.pool, workflow_id, 0)
            .await
            .task_id
            .expect("the root has a task");

        let _phase_two = try_finish_task(
            &db.pool,
            root_task,
            &ok(serde_json::json!(0)),
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
        )
        .await;

        assert_eq!(
            node(&db.pool, workflow_id, 1).await.task_id,
            None,
            "{stored_options}"
        );
    }
    db.drop().await;
}
