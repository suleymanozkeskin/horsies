//! Round trips of the workflow operations, counted at the wire. Python:
//! `tests/integration/test_workflow_start_batched.py::TestBatchedStartShape::test_statement_count_flat_in_root_count`
//! and `test_subworkflow_start_batched.py::TestBulkChildStart::test_statement_count_flat_in_children_and_shape`.
//!
//! Each case runs on a [`CountedBroker`], whose core pools connect through a
//! proxy that counts round trips. The core sends one round trip per
//! statement: a step in autocommit is one, and a step that owes a child start
//! is `BEGIN`, the step, the child start and `COMMIT`. Each case runs once
//! before the count on the same broker: that run checks the schema and the
//! client family, runs the pool's first-use check and prepares the statements on the
//! connection. The workflows of both runs are started, and their node tasks
//! taken to RUNNING, through the test's own pool before the first run, so
//! the counted run follows the first within the idle ping limit of the pool
//! and no checkout ping is sent (Python's `pre_ping_off` cases).

use std::sync::Arc;

use serial_test::serial;
use uuid::Uuid;

use super::support::{
    child_specs_of, node, node_status, run_node_task, spec, start, sub_workflow_node, task_node,
    worker_phase_one, worker_phase_two, workflow_status, CountedBroker, IsolatedDatabase,
    NodeReport, RunningNodeTask,
};
use crate::broker::workflow_start::ClientStartOutcome;
use crate::broker::PostgresBroker;
use crate::core::{AnyNode, WorkflowSpec};
use crate::test_support::child_specs::TestChildSpecs;
use crate::test_support::sql::{self, TestPool};

/// The round trips of a step in autocommit.
const ONE_STATEMENT: u32 = 1;

/// The round trips of a step that owes one child start: `BEGIN`, the step,
/// `horsies_workflow_child_start`, `COMMIT`.
const STEP_WITH_CHILD_START: u32 = 4;

/// The dependents of node A in the `fanout` shape.
const FANOUT: usize = 5;

/// The fan-outs of the promotion statement count.
const SMALL_FAN_OUT: usize = 5;
const LARGE_FAN_OUT: usize = 20;

/// The roots of the large start of `test_statement_count_flat_in_root_count`.
const MANY_ROOTS: usize = 50;

/// The nodes of the child of `test_statement_count_flat_in_children_and_shape`.
const CHILD_NODES: usize = 20;

/// The DAG below node A (index 0).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// A -> B: phase 2 of A promotes B.
    Chain,
    /// A with this many task dependents: phase 2 of A promotes all of them.
    Fanout(usize),
    /// A alone: phase 2 of A completes the workflow and reads no dependents.
    Single,
    /// A -> a sub-workflow node: phase 2 of A owes the child start.
    Subworkflow,
}

/// The child of the `Subworkflow` shape: one task node.
fn round_trip_child() -> WorkflowSpec {
    spec(
        "round_trip_child",
        vec![task_node(0, "round_trip_child_task")],
    )
}

/// `dependent` with node A as its one dependency.
fn after_a(mut dependent: AnyNode) -> AnyNode {
    dependent.dependencies = vec![0];
    dependent
}

/// The spec of `shape`, with `on_error = fail`.
fn shape_spec(shape: Shape) -> WorkflowSpec {
    let mut tasks = vec![task_node(0, "round_trip_task")];
    let name = match shape {
        Shape::Chain => {
            tasks.push(after_a(task_node(1, "round_trip_task")));
            "round_trip_chain".to_owned()
        }
        Shape::Fanout(count) => {
            tasks.extend((1..=count).map(|index| after_a(task_node(index, "round_trip_task"))));
            format!("round_trip_fanout_{count}")
        }
        Shape::Single => "round_trip_single".to_owned(),
        Shape::Subworkflow => {
            tasks.push(after_a(sub_workflow_node(1, &round_trip_child())));
            "round_trip_subworkflow".to_owned()
        }
    };
    spec(&name, tasks)
}

/// The round trips of a node's finalize on the worker: phase 1, then phase 2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FinalizeRoundTrips {
    phase_one: u32,
    phase_two: u32,
}

/// The worker's finalize of `task` with `report` on `counted`, each phase
/// counted.
async fn finalize_counted(
    counted: &CountedBroker,
    child_specs: &TestChildSpecs,
    task: &RunningNodeTask,
    report: NodeReport,
) -> FinalizeRoundTrips {
    let (phase_one, done) = counted
        .count(worker_phase_one(&counted.broker, task, report))
        .await;
    let (phase_two, ()) = counted
        .count(worker_phase_two(&counted.broker, child_specs, task, done))
        .await;
    FinalizeRoundTrips {
        phase_one,
        phase_two,
    }
}

/// Two workflows of `shape`, started through `pool`, with node A of each
/// RUNNING under the worker: the first for the run that prepares the
/// statements, the second for the counted run. Both are set up before
/// either runs, so the counted run follows the first with no wait.
async fn two_running_node_a(
    pool: &TestPool,
    child_specs: &TestChildSpecs,
    shape: Shape,
) -> [(Uuid, RunningNodeTask); 2] {
    let first = start(pool, &shape_spec(shape), child_specs).await;
    let second = start(pool, &shape_spec(shape), child_specs).await;
    [
        (first, run_node_task(pool, first, 0).await),
        (second, run_node_task(pool, second, 0).await),
    ]
}

/// The finalize of node A of a workflow of `shape` with `report`, in the
/// steady state: one run on a new [`CountedBroker`], then the counted run.
/// Returns the workflow of the counted run and its round trips.
async fn measure_node_a(
    pool: &TestPool,
    shape: Shape,
    report: NodeReport,
) -> (Uuid, FinalizeRoundTrips) {
    let child_specs = child_specs_of(&[round_trip_child()]);
    let [(_, warm_up), (workflow_id, counted_task)] =
        two_running_node_a(pool, &child_specs, shape).await;
    let counted = CountedBroker::open(pool).await;
    let _warm_up = finalize_counted(&counted, &child_specs, &warm_up, report).await;
    let measured = finalize_counted(&counted, &child_specs, &counted_task, report).await;
    counted.close().await;
    (workflow_id, measured)
}

/// Starts `workflow_spec` on `broker` with a new id, through the core's
/// start of a client spec. Panics when the start fails or writes nothing.
async fn start_on(
    broker: &Arc<PostgresBroker>,
    workflow_spec: &WorkflowSpec,
    child_specs: &TestChildSpecs,
) -> Uuid {
    let workflow_id = Uuid::new_v4();
    let started = broker
        .start_client_workflow(
            &child_specs.client_spec(workflow_spec),
            workflow_id,
            chrono::Utc::now(),
            false,
            child_specs,
        )
        .await;
    match started {
        Ok(ClientStartOutcome::Started) => {}
        Ok(outcome) => panic!(
            "start of workflow '{}' wrote nothing: {outcome:?}",
            workflow_spec.name
        ),
        Err(error) => panic!(
            "start of workflow '{}' failed: {error:?}",
            workflow_spec.name
        ),
    }
    workflow_id
}

/// The statuses of the nodes of `workflow_id`, by index.
async fn node_statuses(pool: &TestPool, workflow_id: Uuid) -> Vec<String> {
    sql::query_scalar(
        "SELECT status FROM horsies_workflow_tasks WHERE workflow_id = $1 ORDER BY task_index",
    )
    .bind(workflow_id)
    .fetch_all(pool)
    .await
    .expect("read the node statuses")
}

// ---------------------------------------------------------------------------
// Node operations
// ---------------------------------------------------------------------------

/// `test_phase_one_of_a_success_is_one_statement` (`chain`, `single`): phase 1
/// of a success is one round trip, `horsies_finish_workflow_success`.
#[tokio::test]
#[serial]
async fn phase_one_of_a_success_is_one_round_trip() {
    let db = IsolatedDatabase::create().await;
    for shape in [Shape::Chain, Shape::Single] {
        let (_workflow_id, measured) = measure_node_a(&db.pool, shape, NodeReport::Success).await;
        assert_eq!(measured.phase_one, ONE_STATEMENT, "{shape:?}");
    }
    db.drop().await;
}

/// `test_phase_one_of_a_failure_is_one_statement` (`chain`, `single`): phase 1
/// of a failure is one round trip, `horsies_finish_failed_attempt`. The core
/// reads no row before it.
#[tokio::test]
#[serial]
async fn phase_one_of_a_failure_is_one_round_trip() {
    let db = IsolatedDatabase::create().await;
    for shape in [Shape::Chain, Shape::Single] {
        let (_workflow_id, measured) = measure_node_a(&db.pool, shape, NodeReport::Failure).await;
        assert_eq!(measured.phase_one, ONE_STATEMENT, "{shape:?}");
    }
    db.drop().await;
}

/// `test_phase_two_statements`: phase 2 is one statement in autocommit when no
/// sub-workflow node is below the node, and `BEGIN`, the step, the owed child
/// start and `COMMIT` when one is.
#[tokio::test]
#[serial]
async fn phase_two_round_trips_follow_the_shape() {
    let db = IsolatedDatabase::create().await;
    let cases = [
        (Shape::Chain, NodeReport::Success, ONE_STATEMENT),
        (Shape::Fanout(FANOUT), NodeReport::Success, ONE_STATEMENT),
        (Shape::Single, NodeReport::Success, ONE_STATEMENT),
        (
            Shape::Subworkflow,
            NodeReport::Success,
            STEP_WITH_CHILD_START,
        ),
        (Shape::Chain, NodeReport::Failure, ONE_STATEMENT),
        (Shape::Single, NodeReport::Failure, ONE_STATEMENT),
    ];
    for (shape, report, expected) in cases {
        let (workflow_id, measured) = measure_node_a(&db.pool, shape, report).await;

        assert_eq!(measured.phase_two, expected, "{shape:?} {report:?}");
        let statuses = node_statuses(&db.pool, workflow_id).await;
        let workflow = workflow_status(&db.pool, workflow_id).await;
        match (shape, report) {
            (Shape::Chain | Shape::Fanout(_), NodeReport::Success) => {
                assert!(
                    statuses[1..].iter().all(|each| each == "ENQUEUED"),
                    "{shape:?}: {statuses:?}"
                );
                assert_eq!(workflow, "RUNNING", "{shape:?}");
            }
            (Shape::Single, NodeReport::Success) => assert_eq!(workflow, "COMPLETED"),
            (Shape::Subworkflow, NodeReport::Success) => {
                assert!(
                    node(&db.pool, workflow_id, 1)
                        .await
                        .sub_workflow_id
                        .is_some(),
                    "phase 2 of A started the child"
                );
            }
            (Shape::Chain, NodeReport::Failure) => {
                assert_eq!(workflow, "FAILED");
                assert_eq!(statuses[1], "SKIPPED");
            }
            (Shape::Single, NodeReport::Failure) => assert_eq!(workflow, "FAILED"),
            (Shape::Fanout(_) | Shape::Subworkflow, NodeReport::Failure) => {
                unreachable!("no case fails node A of this shape")
            }
        }
    }
    db.drop().await;
}

// ---------------------------------------------------------------------------
// Completion
// ---------------------------------------------------------------------------

/// `TestCompletionStatementBudget.test_success_completion_statement_count`:
/// A -> B with `on_error = fail`. Phase 2 of A, which promotes B, is one round
/// trip; phase 2 of B, which has no dependents and completes the workflow,
/// is one round trip.
#[tokio::test]
#[serial]
async fn a_success_completion_is_one_round_trip_with_and_without_dependents() {
    let db = IsolatedDatabase::create().await;
    let child_specs = child_specs_of(&[]);
    let runs = two_running_node_a(&db.pool, &child_specs, Shape::Chain).await;
    let counted = CountedBroker::open(&db.pool).await;
    let mut measured = Vec::with_capacity(runs.len());
    for (workflow_id, node_a) in &runs {
        let promote = finalize_counted(&counted, &child_specs, node_a, NodeReport::Success).await;
        let node_b = run_node_task(&db.pool, *workflow_id, 1).await;
        let complete = finalize_counted(&counted, &child_specs, &node_b, NodeReport::Success).await;
        assert_eq!(workflow_status(&db.pool, *workflow_id).await, "COMPLETED");
        measured.push((promote.phase_two, complete.phase_two));
    }
    counted.close().await;

    assert_eq!(measured.last(), Some(&(ONE_STATEMENT, ONE_STATEMENT)));
    db.drop().await;
}

/// `TestCompletionStatementBudget.test_failed_completion_statement_count`:
/// the failure of A under `on_error = fail` runs the failure policy, the skip
/// of B and the completion (FAILED) in one round trip.
#[tokio::test]
#[serial]
async fn a_failed_completion_under_on_error_fail_is_one_round_trip() {
    let db = IsolatedDatabase::create().await;
    let (workflow_id, measured) = measure_node_a(&db.pool, Shape::Chain, NodeReport::Failure).await;

    assert_eq!(measured.phase_two, ONE_STATEMENT);
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "FAILED");
    assert_eq!(node_status(&db.pool, workflow_id, 1).await, "SKIPPED");
    db.drop().await;
}

// ---------------------------------------------------------------------------
// Promotion
// ---------------------------------------------------------------------------

/// `TestBatchedPromotionShape.test_fanout_statement_count_flat_in_f`: phase 2
/// of a root that promotes F task dependents is one round trip, for F = 5
/// and F = 20.
#[tokio::test]
#[serial]
async fn a_fan_out_promotion_is_one_round_trip_for_any_width() {
    let db = IsolatedDatabase::create().await;
    for width in [SMALL_FAN_OUT, LARGE_FAN_OUT] {
        let (workflow_id, measured) =
            measure_node_a(&db.pool, Shape::Fanout(width), NodeReport::Success).await;

        assert_eq!(measured.phase_two, ONE_STATEMENT, "F = {width}");
        let statuses = node_statuses(&db.pool, workflow_id).await;
        assert_eq!(statuses.len(), width + 1);
        assert!(
            statuses[1..].iter().all(|each| each == "ENQUEUED"),
            "F = {width}: {statuses:?}"
        );
    }
    db.drop().await;
}

// ---------------------------------------------------------------------------
// test_workflow_start_batched.py, test_subworkflow_start_batched.py
// ---------------------------------------------------------------------------

/// A spec of `count` plain roots.
fn roots(name: &str, count: usize) -> WorkflowSpec {
    spec(
        name,
        (0..count)
            .map(|index| task_node(index, "round_trip_root"))
            .collect(),
    )
}

/// A child of [`CHILD_NODES`] nodes: all roots (`flat`), or each node waiting
/// for the one before it (`chain`).
fn child_of_shape(name: &str, chained: bool) -> WorkflowSpec {
    let tasks = (0..CHILD_NODES)
        .map(|index| {
            let mut child_node = task_node(index, "round_trip_child_node");
            if chained && index > 0 {
                child_node.dependencies = vec![index - 1];
            }
            child_node
        })
        .collect();
    spec(name, tasks)
}

/// `TestBatchedStartShape.test_statement_count_flat_in_root_count`: a start
/// of plain roots is one round trip (`horsies_workflow_start`), for one root
/// and for 50.
#[tokio::test]
#[serial]
async fn a_start_is_one_round_trip_for_one_root_and_for_fifty() {
    let db = IsolatedDatabase::create().await;
    let counted = CountedBroker::open(&db.pool).await;
    let child_specs = child_specs_of(&[]);
    let _warm_up = start_on(
        &counted.broker,
        &roots("round_trip_start_warm_up", 1),
        &child_specs,
    )
    .await;

    let (one, one_root) = counted
        .count(start_on(
            &counted.broker,
            &roots("round_trip_start_one", 1),
            &child_specs,
        ))
        .await;
    let (many, many_roots) = counted
        .count(start_on(
            &counted.broker,
            &roots("round_trip_start_many", MANY_ROOTS),
            &child_specs,
        ))
        .await;
    counted.close().await;

    assert_eq!((one, many), (ONE_STATEMENT, ONE_STATEMENT));
    assert_eq!(
        node_statuses(&db.pool, one_root).await,
        vec!["ENQUEUED".to_owned()]
    );
    let statuses = node_statuses(&db.pool, many_roots).await;
    assert_eq!(statuses.len(), MANY_ROOTS);
    assert!(
        statuses.iter().all(|each| each == "ENQUEUED"),
        "{statuses:?}"
    );
    db.drop().await;
}

/// `TestBulkChildStart.test_statement_count_flat_in_children_and_shape`: a
/// start whose root is a sub-workflow node starts the child in the same
/// statement, one round trip for a child of 20 roots and for a chain of 20.
#[tokio::test]
#[serial]
async fn a_child_start_is_one_round_trip_for_any_child_shape() {
    let db = IsolatedDatabase::create().await;
    let flat = child_of_shape("round_trip_flat_child", false);
    let chain = child_of_shape("round_trip_chain_child", true);
    let child_specs = child_specs_of(&[flat.clone(), chain.clone()]);
    let counted = CountedBroker::open(&db.pool).await;
    let _warm_up = start_on(
        &counted.broker,
        &spec(
            "round_trip_parent_warm_up",
            vec![sub_workflow_node(0, &flat)],
        ),
        &child_specs,
    )
    .await;

    let (flat_round_trips, flat_parent) = counted
        .count(start_on(
            &counted.broker,
            &spec("round_trip_flat_parent", vec![sub_workflow_node(0, &flat)]),
            &child_specs,
        ))
        .await;
    let (chain_round_trips, chain_parent) = counted
        .count(start_on(
            &counted.broker,
            &spec(
                "round_trip_chain_parent",
                vec![sub_workflow_node(0, &chain)],
            ),
            &child_specs,
        ))
        .await;
    counted.close().await;

    assert_eq!(
        (flat_round_trips, chain_round_trips),
        (ONE_STATEMENT, ONE_STATEMENT)
    );
    let flat_child = node(&db.pool, flat_parent, 0)
        .await
        .sub_workflow_id
        .expect("the start started the flat child");
    let chain_child = node(&db.pool, chain_parent, 0)
        .await
        .sub_workflow_id
        .expect("the start started the chained child");
    let flat_statuses = node_statuses(&db.pool, flat_child).await;
    assert_eq!(flat_statuses.len(), CHILD_NODES);
    assert!(
        flat_statuses.iter().all(|each| each == "ENQUEUED"),
        "{flat_statuses:?}"
    );
    let chain_statuses = node_statuses(&db.pool, chain_child).await;
    assert_eq!(chain_statuses.len(), CHILD_NODES);
    assert_eq!(chain_statuses[0], "ENQUEUED");
    assert!(
        chain_statuses[1..].iter().all(|each| each == "PENDING"),
        "{chain_statuses:?}"
    );
    db.drop().await;
}
