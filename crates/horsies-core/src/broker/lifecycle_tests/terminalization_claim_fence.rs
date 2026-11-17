//! Port of Python `tests/integration/test_terminalization_claim_fence.py`.
//!
//! A worker's lease lapses, the reaper requeues the task, and the same
//! worker claims it again: the row is CLAIMED by that worker once more, so
//! only the claim generation tells the two dispatches apart. The post-claim
//! workflow check (`PostgresBroker::filter_non_runnable_workflow_tasks`) and
//! the pre-start stop (`horsies_workflow_task_stop_before_start`) fence on
//! it.

use chrono::{DateTime, Duration, Utc};
use serial_test::serial;
use uuid::Uuid;

use super::support::*;
use crate::broker::claims::claim_pass;
use crate::broker::core_rows::StatementLog;
use crate::broker::pools::core_pool_for_test_pool;
use crate::broker::postgres::PostgresBroker;
use crate::broker::terminalization::on_test_pool::terminalize;
use crate::broker::workflow_stop_before_start::{
    stop_before_start, OrphanSelfHeal, StopBeforeStart, StopMove,
};
use crate::core::lifecycle::{OwnedClaim, TerminalizationCommand};

const CLAIM_QUEUE: &str = "lifecycle_claim_fence";

/// The generation a dispatch carries, and the later generation of a
/// re-claim by the same worker.
fn generations() -> (DateTime<Utc>, DateTime<Utc>) {
    (generation(), generation() + Duration::minutes(1))
}

/// Whether a node still names its task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeLink {
    Linked,
    Detached,
}

fn link_of(task_id: Option<Uuid>) -> NodeLink {
    match task_id {
        Some(_) => NodeLink::Linked,
        None => NodeLink::Detached,
    }
}

/// Python `test_t03_stale_generation_does_not_cancel_reclaimed_task`.
#[tokio::test]
#[serial]
async fn t03_stale_generation_does_not_cancel_reclaimed_task() {
    let pool = pool().await;
    let (dispatched, reclaimed) = generations();
    let task_id = seed_task(&pool, &workflow_task_claimed_at(reclaimed)).await;
    let workflow_id = link_workflow_task(&pool, task_id, "ENQUEUED", "CANCELLED").await;

    PostgresBroker::from_pool(pool.clone())
        .filter_non_runnable_workflow_tasks(&[(task_id, dispatched)], WORKER)
        .await
        .expect("post-claim workflow check");

    assert_eq!(status_of(&pool, task_id).await, "CLAIMED");
    assert_eq!(
        node_state(&pool, workflow_id).await,
        ("ENQUEUED".to_owned(), Some(task_id))
    );
    cleanup_tasks(&pool, &[task_id]).await;
    cleanup_workflows(&pool, &[workflow_id]).await;
}

/// Python `test_batch_fences_each_task_on_its_own_generation`: one batch can
/// span claim transactions, so the fence is per task.
#[tokio::test]
#[serial]
async fn batch_fences_each_task_on_its_own_generation() {
    let pool = pool().await;
    let (dispatched, reclaimed) = generations();
    let fresh = seed_task(&pool, &workflow_task_claimed_at(dispatched)).await;
    let stale = seed_task(&pool, &workflow_task_claimed_at(reclaimed)).await;
    let fresh_workflow = link_workflow_task(&pool, fresh, "ENQUEUED", "PAUSED").await;
    let stale_workflow = link_workflow_task(&pool, stale, "ENQUEUED", "PAUSED").await;

    PostgresBroker::from_pool(pool.clone())
        .filter_non_runnable_workflow_tasks(&[(fresh, dispatched), (stale, dispatched)], WORKER)
        .await
        .expect("post-claim workflow check");

    assert_eq!(status_of(&pool, fresh).await, "CANCELLED");
    assert_eq!(status_of(&pool, stale).await, "CLAIMED");
    cleanup_tasks(&pool, &[fresh, stale]).await;
    cleanup_workflows(&pool, &[fresh_workflow, stale_workflow]).await;
}

/// Python `test_real_claim_path_arms_the_fence`: the rows of the production
/// claim carry the generation that the post-claim check fences on.
#[tokio::test]
#[serial]
async fn real_claim_path_arms_the_fence() {
    let pool = pool().await;
    let task_id = seed_task(
        &pool,
        &TaskSeed {
            origin: Origin::WorkflowNode,
            queue_name: CLAIM_QUEUE,
            ..TaskSeed::pending()
        },
    )
    .await;
    let workflow_id = link_workflow_task(&pool, task_id, "ENQUEUED", "PAUSED").await;
    let broker = PostgresBroker::from_pool(pool.clone());

    let pass = claim_pass(
        &core_pool_for_test_pool(&pool),
        StatementLog::Off,
        &claim_request(CLAIM_QUEUE),
    )
    .await
    .expect("claim pass");
    let claimed = pass
        .claimed
        .iter()
        .find(|row| row.id == task_id)
        .expect("the claim returned the seeded task");
    // The reaper requeues the row and the same worker claims it again.
    sqlx::query(
        "UPDATE horsies_tasks SET claimed_at = claimed_at + interval '1 minute' WHERE id = $1",
    )
    .bind(task_id)
    .execute(&pool)
    .await
    .expect("move the claim generation");
    broker
        .filter_non_runnable_workflow_tasks(&[(task_id, claimed.claimed_at)], WORKER)
        .await
        .expect("post-claim workflow check");

    assert_eq!(
        status_of(&pool, task_id).await,
        "CLAIMED",
        "the abandon ended a generation that the dispatch did not own"
    );
    cleanup_tasks(&pool, &[task_id]).await;
    cleanup_workflows(&pool, &[workflow_id]).await;
}

/// The stop before start of `task_id` under the claim of the dispatch that
/// carries `dispatched`, with the orphan self-heal `self_heal`.
async fn stop_dispatched_with(
    pool: &sqlx::PgPool,
    task_id: Uuid,
    dispatched: DateTime<Utc>,
    self_heal: OrphanSelfHeal,
) -> StopBeforeStart {
    stop_before_start(
        &core_pool_for_test_pool(pool),
        StatementLog::Off,
        task_id,
        &OwnedClaim {
            worker_id: WORKER.to_owned(),
            claimed_at: dispatched,
        },
        self_heal,
    )
    .await
    .expect("stop before start")
}

/// [`stop_dispatched_with`] with the orphan self-heal off.
async fn stop_dispatched(pool: &sqlx::PgPool, task_id: Uuid, dispatched: DateTime<Utc>) {
    let _stopped = stop_dispatched_with(pool, task_id, dispatched, OrphanSelfHeal::Off).await;
}

/// One case of `test_child_prestart_fences_on_claim_generation`.
struct PrestartCase {
    workflow_status: &'static str,
    kind: &'static str,
    fresh_node_status: &'static str,
    fresh_node_link: NodeLink,
}

/// The stale generation's row and node stay as they are; the dispatched
/// generation's row takes the stop, and its node takes the coupled write.
async fn child_prestart_fences(case: PrestartCase) {
    let pool = pool().await;
    let (dispatched, reclaimed) = generations();
    let stale = seed_task(&pool, &workflow_task_claimed_at(reclaimed)).await;
    let fresh = seed_task(&pool, &workflow_task_claimed_at(dispatched)).await;
    let stale_workflow = link_workflow_task(&pool, stale, "ENQUEUED", case.workflow_status).await;
    let fresh_workflow = link_workflow_task(&pool, fresh, "ENQUEUED", case.workflow_status).await;

    for task_id in [stale, fresh] {
        stop_dispatched(&pool, task_id, dispatched).await;
    }

    assert_eq!(status_of(&pool, stale).await, "CLAIMED");
    assert_eq!(history_row(&pool, stale).await, None);
    let fresh_image = history_row(&pool, fresh).await.expect("fresh history row");
    assert_eq!(fresh_image.status, "CANCELLED");
    assert_eq!(fresh_image.terminalization_kind, case.kind);
    assert_eq!(
        node_state(&pool, stale_workflow).await,
        ("ENQUEUED".to_owned(), Some(stale))
    );
    let (fresh_node_status, fresh_node_task) = node_state(&pool, fresh_workflow).await;
    assert_eq!(fresh_node_status, case.fresh_node_status);
    assert_eq!(link_of(fresh_node_task), case.fresh_node_link);

    cleanup_tasks(&pool, &[stale, fresh]).await;
    cleanup_workflows(&pool, &[stale_workflow, fresh_workflow]).await;
}

/// Python `test_child_prestart_fences_on_claim_generation`, PAUSED case.
#[tokio::test]
#[serial]
async fn child_prestart_fences_on_claim_generation_paused() {
    child_prestart_fences(PrestartCase {
        workflow_status: "PAUSED",
        kind: "PAUSE_ABANDON_CLAIM",
        fresh_node_status: "READY",
        fresh_node_link: NodeLink::Detached,
    })
    .await;
}

/// Python `test_child_prestart_fences_on_claim_generation`, CANCELLED case.
#[tokio::test]
#[serial]
async fn child_prestart_fences_on_claim_generation_cancelled() {
    child_prestart_fences(PrestartCase {
        workflow_status: "CANCELLED",
        kind: "WORKFLOW_CANCEL_CLAIM",
        fresh_node_status: "SKIPPED",
        fresh_node_link: NodeLink::Linked,
    })
    .await;
}

/// The orphan cancel of the stop before start fences on the claim
/// generation: a row that the same worker claimed again is not cancelled
/// and gets no history row; the orphan of the dispatched generation is
/// cancelled (Python
/// `test_lifecycle_classification.py::TestOrphanedWorkflowTask::test_orphan_cancel_fences_on_the_dispatched_generation`).
#[tokio::test]
#[serial]
async fn child_prestart_orphan_cancel_fences_on_claim_generation() {
    let pool = pool().await;
    let (dispatched, reclaimed) = generations();
    let stale = seed_task(&pool, &workflow_task_claimed_at(reclaimed)).await;
    let fresh = seed_task(&pool, &workflow_task_claimed_at(dispatched)).await;

    let stale_stop = stop_dispatched_with(&pool, stale, dispatched, OrphanSelfHeal::On).await;
    let fresh_stop = stop_dispatched_with(&pool, fresh, dispatched, OrphanSelfHeal::On).await;

    assert!(
        matches!(stale_stop, StopBeforeStart::NotHeld(_)),
        "{stale_stop:?}"
    );
    assert_eq!(status_of(&pool, stale).await, "CLAIMED");
    assert_eq!(history_row(&pool, stale).await, None);
    assert_eq!(
        fresh_stop,
        StopBeforeStart::OrphanCancelled(StopMove::Applied)
    );
    let fresh_image = history_row(&pool, fresh).await.expect("fresh history row");
    assert_eq!(fresh_image.status, "CANCELLED");
    assert_eq!(fresh_image.terminalization_kind, "CANCEL_ORPHAN");
    assert_eq!(
        fresh_image.error_code.as_deref(),
        Some("WORKFLOW_CHECK_FAILED")
    );

    cleanup_tasks(&pool, &[stale, fresh]).await;
}

/// Python `test_child_prestart_still_cancels_requeued_pending_row`: a row
/// that the reaper requeued carries no claim, so the fence must not gate it.
#[tokio::test]
#[serial]
async fn child_prestart_still_cancels_requeued_pending_row() {
    let pool = pool().await;
    let (dispatched, _) = generations();
    let task_id = seed_task(
        &pool,
        &TaskSeed {
            origin: Origin::WorkflowNode,
            ..TaskSeed::pending()
        },
    )
    .await;
    let workflow_id = link_workflow_task(&pool, task_id, "ENQUEUED", "CANCELLED").await;

    stop_dispatched(&pool, task_id, dispatched).await;

    assert_eq!(status_of(&pool, task_id).await, "CANCELLED");
    assert_eq!(
        node_state(&pool, workflow_id).await,
        ("SKIPPED".to_owned(), Some(task_id))
    );
    cleanup_tasks(&pool, &[task_id]).await;
    cleanup_workflows(&pool, &[workflow_id]).await;
}

/// Python `test_t16_skips_tasks_whose_workflow_is_not_cancelled`: the cancel
/// batch needs a live CANCELLED workflow.
#[tokio::test]
#[serial]
async fn t16_skips_tasks_whose_workflow_is_not_cancelled() {
    let pool = pool().await;
    let (_, reclaimed) = generations();
    let task_id = seed_task(&pool, &workflow_task_claimed_at(reclaimed)).await;
    let workflow_id = link_workflow_task(&pool, task_id, "ENQUEUED", "RUNNING").await;

    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::CancelNodesOfCancelledWorkflow {
            workflow_ids: vec![workflow_id],
        },
    )
    .await
    .expect("cancel sweep");

    assert!(outcomes.is_empty(), "{outcomes:?}");
    assert_eq!(status_of(&pool, task_id).await, "CLAIMED");
    cleanup_tasks(&pool, &[task_id]).await;
    cleanup_workflows(&pool, &[workflow_id]).await;
}
