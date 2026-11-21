//! The node read of a task by its id (`PostgresBroker::read_task_workflow_node`)
//! on the migrated test database: a task that a node of a top-level
//! workflow links, a node of a workflow with no stored root, and a task that
//! no node links.

use uuid::Uuid;

use super::support::*;
use crate::broker::claims::WorkflowNodeIndex;
use crate::broker::{PostgresBroker, TaskWorkflowNode};
use crate::test_support::sql;

fn broker_of(pool: &sql::TestPool) -> PostgresBroker {
    PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust)
}

/// The second node of a top-level workflow links the task: the read gives
/// the workflow, the index 1 and the workflow as its own root.
#[tokio::test]
async fn a_linked_task_reads_its_workflow_its_node_and_its_root() {
    let pool = pool().await;
    let workflow_id = Uuid::new_v4();
    seed_workflow(&pool, workflow_id, "RUNNING").await;
    let first_task = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    let _first = seed_node(&pool, workflow_id, Some(first_task), "ENQUEUED").await;
    let _second = seed_node(&pool, workflow_id, Some(task_id), "RUNNING").await;

    let read = broker_of(&pool).read_task_workflow_node(task_id).await;

    cleanup_workflows(&pool, &[workflow_id]).await;
    assert_eq!(
        read.unwrap(),
        TaskWorkflowNode::Linked {
            workflow_id,
            node_index: WorkflowNodeIndex::try_from(1).unwrap(),
            root_workflow_id: workflow_id,
        }
    );
}

/// A workflow row with no stored root is its own root, as the start write
/// reads it.
#[tokio::test]
async fn a_workflow_with_no_stored_root_is_its_own_root() {
    let pool = pool().await;
    let workflow_id = Uuid::new_v4();
    seed_workflow(&pool, workflow_id, "RUNNING").await;
    sql::query("UPDATE horsies_workflows SET root_workflow_id = NULL WHERE id = $1")
        .bind(workflow_id)
        .execute(&pool)
        .await
        .expect("clear the stored root");
    let task_id = Uuid::new_v4();
    let _node = seed_node(&pool, workflow_id, Some(task_id), "RUNNING").await;

    let read = broker_of(&pool).read_task_workflow_node(task_id).await;

    cleanup_workflows(&pool, &[workflow_id]).await;
    assert_eq!(
        read.unwrap(),
        TaskWorkflowNode::Linked {
            workflow_id,
            node_index: WorkflowNodeIndex::try_from(0).unwrap(),
            root_workflow_id: workflow_id,
        }
    );
}

/// No node links the task: not linked.
#[tokio::test]
async fn a_task_that_no_node_links_is_not_linked() {
    let pool = pool().await;

    let read = broker_of(&pool)
        .read_task_workflow_node(Uuid::new_v4())
        .await;

    assert_eq!(read.unwrap(), TaskWorkflowNode::NotLinked);
}
