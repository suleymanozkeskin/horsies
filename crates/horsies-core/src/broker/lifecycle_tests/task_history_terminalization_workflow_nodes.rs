//! Port of Python `tests/integration/test_task_history_terminalization_workflow_nodes.py`.
//!
//! `TestWorkflowStateMatrix::test_paused_sweep_takes_claimed_behind_runnable_nodes_only`
//! is `terminalization_functions::workflow_scoped_batches::pause_applies_only_under_both_live_guards`.

use serial_test::serial;
use uuid::Uuid;

use super::support::*;
use crate::broker::terminalization::on_test_pool::terminalize;
use crate::core::lifecycle::{TerminalizationCommand, TerminalizationOutcome};
use crate::test_support::sql;

/// Bound of the paused sweep that runs beside an open node update.
const SWEEP_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

fn paused_sweep(workflow_ids: Vec<Uuid>) -> TerminalizationCommand {
    TerminalizationCommand::AbandonNodesOfPausedWorkflows { workflow_ids }
}

fn cancelled_sweep(workflow_ids: Vec<Uuid>) -> TerminalizationCommand {
    TerminalizationCommand::CancelNodesOfCancelledWorkflow { workflow_ids }
}

fn claimed_workflow_task() -> TaskSeed {
    TaskSeed {
        origin: Origin::WorkflowNode,
        ..TaskSeed::claimed()
    }
}

fn swept_ids(outcomes: &[TerminalizationOutcome]) -> Vec<Uuid> {
    let mut ids: Vec<Uuid> = outcomes
        .iter()
        .map(TerminalizationOutcome::task_id)
        .collect();
    ids.sort_unstable();
    ids
}

/// Python `TestWorkflowStateMatrix::test_running_workflow_is_untouched_by_both_sweeps`.
#[tokio::test]
#[serial]
async fn running_workflow_is_untouched_by_both_sweeps() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &claimed_workflow_task()).await;
    let workflow_id = link_workflow_task(&pool, task_id, "ENQUEUED", "RUNNING").await;

    for command in [
        paused_sweep(vec![workflow_id]),
        cancelled_sweep(vec![workflow_id]),
    ] {
        let outcomes = terminalize(&pool, &command).await.expect("sweep");
        assert!(outcomes.is_empty(), "{command:?}: {outcomes:?}");
    }

    assert_eq!(status_of(&pool, task_id).await, "CLAIMED");
    assert_eq!(
        node_state(&pool, workflow_id).await,
        ("ENQUEUED".to_owned(), Some(task_id))
    );
    cleanup_tasks(&pool, &[task_id]).await;
    cleanup_workflows(&pool, &[workflow_id]).await;
}

/// Python `TestWorkflowStateMatrix::test_paused_sweep_judges_from_the_capture_not_a_reread`:
/// a node moves ENQUEUED to RUNNING in an open transaction while the sweep
/// runs. The sweep judges from its one capture and does not wait for the
/// node update.
#[tokio::test]
#[serial]
async fn paused_sweep_judges_from_the_capture_not_a_reread() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &claimed_workflow_task()).await;
    let workflow_id = Uuid::new_v4();
    seed_workflow(&pool, workflow_id, "PAUSED").await;
    let node_row_id = seed_node(&pool, workflow_id, Some(task_id), "ENQUEUED").await;

    let mut progress = pool.begin().await.expect("begin node progress");
    sql::query("UPDATE horsies_workflow_tasks SET status = 'RUNNING' WHERE id = $1")
        .bind(node_row_id)
        .execute(&mut *progress)
        .await
        .expect("move the node in an open transaction");
    let outcomes = tokio::time::timeout(
        SWEEP_WAIT,
        terminalize(&pool, &paused_sweep(vec![workflow_id])),
    )
    .await
    .expect("the sweep does not wait for the node update")
    .expect("paused sweep");
    progress
        .rollback()
        .await
        .expect("roll back the node update");

    assert_eq!(swept_ids(&outcomes), vec![task_id]);
    assert_eq!(
        history_row(&pool, task_id)
            .await
            .expect("history row")
            .terminalization_kind,
        "PAUSE_ABANDON_WORKFLOW"
    );
    cleanup_tasks(&pool, &[task_id]).await;
    cleanup_workflows(&pool, &[workflow_id]).await;
}

/// A workflow-scoped sweep.
#[derive(Debug, Clone, Copy)]
enum Sweep {
    Paused,
    Cancelled,
}

impl Sweep {
    fn workflow_status(self) -> &'static str {
        match self {
            Self::Paused => "PAUSED",
            Self::Cancelled => "CANCELLED",
        }
    }

    fn command(self, workflow_ids: Vec<Uuid>) -> TerminalizationCommand {
        match self {
            Self::Paused => paused_sweep(workflow_ids),
            Self::Cancelled => cancelled_sweep(workflow_ids),
        }
    }
}

/// Python `TestWorkflowStateMatrix::test_sweeps_respect_the_workflow_scope`:
/// a sweep touches only the workflows that it names.
#[tokio::test]
#[serial]
async fn sweeps_respect_the_workflow_scope() {
    let pool = pool().await;
    for sweep in [Sweep::Paused, Sweep::Cancelled] {
        let named_task = seed_task(&pool, &claimed_workflow_task()).await;
        let named_workflow =
            link_workflow_task(&pool, named_task, "ENQUEUED", sweep.workflow_status()).await;
        let other_task = seed_task(&pool, &claimed_workflow_task()).await;
        let other_workflow =
            link_workflow_task(&pool, other_task, "ENQUEUED", sweep.workflow_status()).await;

        let outcomes = terminalize(&pool, &sweep.command(vec![named_workflow]))
            .await
            .expect("sweep");

        assert_eq!(swept_ids(&outcomes), vec![named_task], "{sweep:?}");
        assert_eq!(status_of(&pool, other_task).await, "CLAIMED", "{sweep:?}");
        cleanup_tasks(&pool, &[named_task, other_task]).await;
        cleanup_workflows(&pool, &[named_workflow, other_workflow]).await;
    }
}
