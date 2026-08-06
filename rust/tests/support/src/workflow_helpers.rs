/// Workflow test helpers for starting workflows and simulating task completion.
///
/// Mirrors Python's `conftest.py` helpers: `start_ok`, `complete_task`,
/// `get_task_status`, `get_workflow_status`.
use std::sync::Arc;

use serde::de::DeserializeOwned;
use sqlx::PgPool;

use horsies::{
    Horsies, PostgresBroker, TaskResult, WorkflowHandle, WorkflowSpec, WorkflowSpecRegistry,
};

use crate::fixtures;
use crate::tasks::result_json;

/// Start a workflow and panic on failure (for tests that expect success).
pub async fn start_ok<T>(
    pool: &PgPool,
    spec: &WorkflowSpec,
    _registry: &WorkflowSpecRegistry,
) -> WorkflowHandle<T>
where
    T: DeserializeOwned + Clone,
{
    let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
    let mut app = Horsies::with_broker(fixtures::default_app_config(), broker).unwrap();
    let workflow = app.register_workflow_spec::<T>(spec.clone()).unwrap();
    workflow
        .start()
        .await
        .unwrap_or_else(|e| panic!("workflow.start failed: {}", e))
}

/// Start a workflow with a specific ID and panic on failure.
pub async fn start_ok_with_id<T>(
    pool: &PgPool,
    spec: &WorkflowSpec,
    workflow_id: &str,
    _registry: &WorkflowSpecRegistry,
) -> WorkflowHandle<T>
where
    T: DeserializeOwned + Clone,
{
    let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
    let mut app = Horsies::with_broker(fixtures::default_app_config(), broker).unwrap();
    let workflow = app.register_workflow_spec::<T>(spec.clone()).unwrap();
    workflow
        .start_with_id(workflow_id.to_owned())
        .await
        .unwrap_or_else(|e| panic!("workflow.start_with_id failed: {}", e))
}

/// Get the horsies_tasks.id linked to a workflow task by index.
pub async fn get_task_id(pool: &PgPool, workflow_id: &str, task_index: i32) -> String {
    let id: Option<String> = sqlx::query_scalar(
        "SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = $1 AND task_index = $2",
    )
    .bind(workflow_id)
    .bind(task_index)
    .fetch_one(pool)
    .await
    .expect("workflow task not found");

    id.expect("task_id is NULL (task not yet enqueued)")
}

/// Simulate task completion by calling on_workflow_task_complete.
pub async fn complete_task(
    pool: &PgPool,
    workflow_id: &str,
    task_index: i32,
    result: &TaskResult<serde_json::Value>,
    registry: &WorkflowSpecRegistry,
) {
    let task_id = get_task_id(pool, workflow_id, task_index).await;

    // Mark the horsies_tasks row as COMPLETED/FAILED.
    let result_str = result_json(result);
    match result {
        TaskResult::Ok(_) => {
            sqlx::query(
                "UPDATE horsies_tasks SET status = 'COMPLETED', completed_at = NOW(), \
                 result = $2, error_code = NULL, terminal_at = NOW(), updated_at = NOW() \
                 WHERE id = $1",
            )
            .bind(&task_id)
            .bind(&result_str)
            .execute(pool)
            .await
            .expect("failed to complete task");
        }
        TaskResult::Err(err) => {
            let error_code = err.error_code.as_ref().map(|c| c.to_string());
            sqlx::query(
                "UPDATE horsies_tasks SET status = 'FAILED', failed_at = NOW(), \
                 result = $2, error_code = $3, terminal_at = NOW(), updated_at = NOW() \
                 WHERE id = $1",
            )
            .bind(&task_id)
            .bind(&result_str)
            .bind(&error_code)
            .execute(pool)
            .await
            .expect("failed to fail task");
        }
    }

    // Call the workflow engine's completion handler.
    let is_success = result.is_ok();
    horsies::on_workflow_task_complete(
        pool,
        &task_id,
        &result_str,
        is_success,
        registry,
        &horsies::PayloadPolicy::default(),
    )
    .await
    .expect("on_workflow_task_complete failed");
}

/// Get workflow task status by index.
pub async fn get_wf_task_status(pool: &PgPool, workflow_id: &str, task_index: i32) -> String {
    let status: Option<String> = sqlx::query_scalar(
        "SELECT status FROM horsies_workflow_tasks WHERE workflow_id = $1 AND task_index = $2",
    )
    .bind(workflow_id)
    .bind(task_index)
    .fetch_optional(pool)
    .await
    .expect("query failed");

    status.unwrap_or_else(|| "NOT_FOUND".to_owned())
}

/// Get workflow status by id.
pub async fn get_workflow_status(pool: &PgPool, workflow_id: &str) -> String {
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .fetch_optional(pool)
            .await
            .expect("query failed");

    status.unwrap_or_else(|| "NOT_FOUND".to_owned())
}

/// Get workflow result by id.
pub async fn get_workflow_result_raw(pool: &PgPool, workflow_id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT result FROM horsies_workflows WHERE id = $1")
        .bind(workflow_id)
        .fetch_optional(pool)
        .await
        .expect("query failed")
        .flatten()
}

/// Get workflow error by id.
pub async fn get_workflow_error_raw(pool: &PgPool, workflow_id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT error FROM horsies_workflows WHERE id = $1")
        .bind(workflow_id)
        .fetch_optional(pool)
        .await
        .expect("query failed")
        .flatten()
}

/// Get the number of workflow tasks in a given status.
pub async fn count_wf_tasks_in_status(pool: &PgPool, workflow_id: &str, status: &str) -> i64 {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = $1 AND status = $2",
    )
    .bind(workflow_id)
    .bind(status)
    .fetch_one(pool)
    .await
    .expect("count query failed");

    count
}

/// Insert a task directly into horsies_tasks (for unit-level integration tests).
pub async fn insert_task(
    pool: &PgPool,
    task_id: &str,
    task_name: &str,
    queue_name: &str,
    status: &str,
) {
    sqlx::query(
        "INSERT INTO horsies_tasks (id, task_name, queue_name, priority, status, \
         enqueued_at, enqueue_sha, created_at, updated_at, terminal_at) \
         VALUES ($1, $2, $3, 100, $4, NOW(), 'test-sha', NOW(), NOW(), \
                 CASE WHEN $4 IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED') \
                      THEN NOW() END)",
    )
    .bind(task_id)
    .bind(task_name)
    .bind(queue_name)
    .bind(status)
    .execute(pool)
    .await
    .expect("failed to insert task");
}
