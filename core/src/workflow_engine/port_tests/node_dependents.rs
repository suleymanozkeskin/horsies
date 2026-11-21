//! Whether phase 1 of a node's finalize names dependents, and the phase-2
//! form that the worker takes from it.
//!
//! Phase 1 reads whether a node of the same workflow lists the node in its
//! dependencies (`horsies_node_has_dependents`). Dependencies are written at
//! the workflow start and never change, so the fact holds for phase 2. A
//! node with no dependents gets [`PhaseTwoStep::NoDependents`]: its phase 2
//! calls `horsies_workflow_node_finish` with dependents `NONE`, which reads
//! none. Every node here runs through the worker's finalize: phase 1, then
//! phase 2 in the form that phase 1 named.

use uuid::Uuid;

use super::support::{
    broker, child_specs_of, execute, run_node_task, spec, start, task_node, worker_phase_one,
    worker_phase_two, workflow_status, CountedBroker, IsolatedDatabase, NodeReport,
};
use crate::broker::PostgresBroker;
use crate::core::lifecycle::PhaseTwoStep;
use crate::core::{AnyNode, WorkflowSpec};
use crate::test_support::sql::TestPool;
use crate::worker::execution::PhaseTwoForm;

/// The form of phase 2 of a node that another node of its workflow waits for.
const HAS_DEPENDENTS: PhaseTwoForm = PhaseTwoForm::Step(PhaseTwoStep::DependentsWithoutSubworkflow);

/// The form of phase 2 of a node that no node of its workflow waits for.
const NO_DEPENDENTS: PhaseTwoForm = PhaseTwoForm::Step(PhaseTwoStep::NoDependents);

/// A task node at `index` that waits for `dependencies`.
fn waiting(index: usize, dependencies: &[usize]) -> AnyNode {
    let mut waiting_node = task_node(index, "dependents_task");
    waiting_node.dependencies = dependencies.to_vec();
    waiting_node
}

/// A (0), B (1) waits for A, C (2) waits for A and B.
fn fan_in() -> WorkflowSpec {
    spec(
        "dependents_fan_in",
        vec![waiting(0, &[]), waiting(1, &[0]), waiting(2, &[0, 1])],
    )
}

/// 0 -> 1 -> 2 -> 3: node 3 lists index 2.
fn chain_of_four() -> WorkflowSpec {
    spec(
        "dependents_chain_of_four",
        vec![
            waiting(0, &[]),
            waiting(1, &[0]),
            waiting(2, &[1]),
            waiting(3, &[2]),
        ],
    )
}

/// A -> B.
fn chain_of_two() -> WorkflowSpec {
    spec(
        "dependents_chain_of_two",
        vec![waiting(0, &[]), waiting(1, &[0])],
    )
}

/// Starts `workflow_spec` with no child specs and returns its id.
async fn start_plain(pool: &TestPool, workflow_spec: &WorkflowSpec) -> Uuid {
    start(pool, workflow_spec, &child_specs_of(&[])).await
}

/// The worker's finalize of node `task_index` with a success on `broker`.
/// Returns the phase-2 form that phase 1 named.
async fn finalize_success(
    pool: &TestPool,
    broker: &PostgresBroker,
    workflow_id: Uuid,
    task_index: i32,
) -> PhaseTwoForm {
    let task = run_node_task(pool, workflow_id, task_index).await;
    let done = worker_phase_one(broker, &task, NodeReport::Success).await;
    worker_phase_two(broker, &child_specs_of(&[]), &task, done).await;
    done.phase_two
}

/// `test_a_node_that_another_node_waits_for_has_dependents`: the head of the
/// DAG and a node that a fan-in waits for.
#[tokio::test]
async fn a_node_that_another_node_waits_for_has_dependents() {
    let db = IsolatedDatabase::create().await;
    let broker = broker(&db.pool);
    let workflow_id = start_plain(&db.pool, &fan_in()).await;

    let head = finalize_success(&db.pool, &broker, workflow_id, 0).await;
    let waited_for = finalize_success(&db.pool, &broker, workflow_id, 1).await;

    assert_eq!(head, HAS_DEPENDENTS);
    assert_eq!(waited_for, HAS_DEPENDENTS);
    db.drop().await;
}

/// `test_a_leaf_has_no_dependents_when_another_workflow_waits_for_its_index`:
/// only the nodes of the same workflow count. In the chain of four a node
/// lists index 2; node 2 of the fan-in is still a leaf.
#[tokio::test]
async fn a_leaf_has_no_dependents_when_another_workflow_waits_for_its_index() {
    let db = IsolatedDatabase::create().await;
    let broker = broker(&db.pool);
    let fan_in_id = start_plain(&db.pool, &fan_in()).await;
    let chain_id = start_plain(&db.pool, &chain_of_four()).await;
    for task_index in [0, 1] {
        let _form = finalize_success(&db.pool, &broker, fan_in_id, task_index).await;
        let _form = finalize_success(&db.pool, &broker, chain_id, task_index).await;
    }

    let fan_in_leaf = finalize_success(&db.pool, &broker, fan_in_id, 2).await;
    let chain_node = finalize_success(&db.pool, &broker, chain_id, 2).await;

    assert_eq!(fan_in_leaf, NO_DEPENDENTS);
    assert_eq!(chain_node, HAS_DEPENDENTS);
    assert_eq!(workflow_status(&db.pool, fan_in_id).await, "COMPLETED");
    db.drop().await;
}

/// `test_a_node_whose_dependent_is_terminal_still_has_dependents`: the fact
/// is one of the DAG, not of the state of the dependents. Node 2 is SKIPPED
/// before node 0 finishes; node 0 still has dependents.
#[tokio::test]
async fn a_node_whose_dependent_is_terminal_still_has_dependents() {
    let db = IsolatedDatabase::create().await;
    let broker = broker(&db.pool);
    let workflow_id = start_plain(&db.pool, &fan_in()).await;
    execute(
        &db.pool,
        &format!(
            "UPDATE horsies_workflow_tasks SET status = 'SKIPPED', completed_at = NOW()
             WHERE workflow_id = '{workflow_id}'::uuid AND task_index = 2"
        ),
    )
    .await;

    let head = finalize_success(&db.pool, &broker, workflow_id, 0).await;

    assert_eq!(head, HAS_DEPENDENTS);
    db.drop().await;
}

/// `test_phase_two_of_a_leaf_does_not_read_dependents`: A -> B. Phase 1 of A
/// names its dependents, and its phase 2 promotes B; phase 1 of B names
/// none, and its phase 2 (dependents `NONE`) completes the workflow in one
/// round trip, counted after the same finalize of another workflow prepared
/// the statements.
#[tokio::test]
async fn phase_two_of_a_leaf_reads_no_dependents() {
    let db = IsolatedDatabase::create().await;
    let workflows = [
        start_plain(&db.pool, &chain_of_two()).await,
        start_plain(&db.pool, &chain_of_two()).await,
    ];
    let counted = CountedBroker::open(&db.pool).await;
    let mut measured = Vec::with_capacity(workflows.len());
    for workflow_id in workflows {
        let of_a = finalize_success(&db.pool, &counted.broker, workflow_id, 0).await;
        let task_b = run_node_task(&db.pool, workflow_id, 1).await;
        let done_b = worker_phase_one(&counted.broker, &task_b, NodeReport::Success).await;
        let (phase_two_of_b, ()) = counted
            .count(worker_phase_two(
                &counted.broker,
                &child_specs_of(&[]),
                &task_b,
                done_b,
            ))
            .await;
        assert_eq!(workflow_status(&db.pool, workflow_id).await, "COMPLETED");
        measured.push((of_a, done_b.phase_two, phase_two_of_b));
    }
    counted.close().await;

    assert_eq!(measured.last(), Some(&(HAS_DEPENDENTS, NO_DEPENDENTS, 1)));
    db.drop().await;
}
