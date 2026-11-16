/// Workflow helpers for e2e tests.
///
/// Mirrors Python's `tests/e2e/helpers/workflow.py`.
use std::time::{Duration, Instant};

use sqlx::PgPool;
use uuid::Uuid;

/// Get current status of a workflow.
pub async fn get_workflow_status(pool: &PgPool, workflow_id: &Uuid) -> String {
    sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
        .bind(workflow_id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| panic!("workflow {} not found", workflow_id))
}

/// Get status of a specific workflow task by index.
pub async fn get_workflow_task_status(
    pool: &PgPool,
    workflow_id: &Uuid,
    task_index: i32,
) -> String {
    sqlx::query_scalar(
        "SELECT status FROM horsies_workflow_tasks WHERE workflow_id = $1 AND task_index = $2",
    )
    .bind(workflow_id)
    .bind(task_index)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
    .unwrap_or_else(|| panic!("workflow task {}#{} not found", workflow_id, task_index,))
}

/// Fetch all workflow_tasks for a workflow, ordered by task_index.
pub async fn get_workflow_tasks(pool: &PgPool, workflow_id: &Uuid) -> Vec<WorkflowTaskRow> {
    sqlx::query_as(
        "SELECT task_index, task_name, status, started_at, completed_at \
         FROM horsies_workflow_tasks WHERE workflow_id = $1 ORDER BY task_index",
    )
    .bind(workflow_id)
    .fetch_all(pool)
    .await
    .expect("failed to fetch workflow tasks")
}

/// Row from the workflow tasks query.
#[derive(Debug, sqlx::FromRow)]
pub struct WorkflowTaskRow {
    pub task_index: i32,
    pub task_name: String,
    pub status: String,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Poll DB until workflow reaches terminal state. Returns the final status.
pub async fn wait_for_workflow_completion(
    pool: &PgPool,
    workflow_id: &Uuid,
    timeout: Duration,
) -> String {
    let deadline = Instant::now() + timeout;
    let mut last_status = "UNKNOWN".to_owned();

    while Instant::now() < deadline {
        let status = get_workflow_status(pool, workflow_id).await;
        last_status = status.clone();
        if matches!(
            status.as_str(),
            "COMPLETED" | "FAILED" | "CANCELLED" | "EXPIRED"
        ) {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    panic!(
        "workflow {} did not complete within {}s (last status: {})",
        workflow_id,
        timeout.as_secs(),
        last_status,
    );
}

/// Poll DB and return the max concurrent RUNNING tasks observed for a workflow.
pub async fn poll_max_running_tasks(pool: &PgPool, workflow_id: &Uuid, duration: Duration) -> i64 {
    let deadline = Instant::now() + duration;
    let mut max_observed: i64 = 0;

    while Instant::now() < deadline {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM horsies_tasks t \
             JOIN horsies_workflow_tasks wt ON wt.task_id = t.id \
             WHERE wt.workflow_id = $1 AND t.status = 'RUNNING'",
        )
        .bind(workflow_id)
        .fetch_one(pool)
        .await
        .unwrap_or(0);

        max_observed = max_observed.max(count);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    max_observed
}
