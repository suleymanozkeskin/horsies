use std::collections::HashMap;
use std::time::Duration;

use serde::de::DeserializeOwned;
use sqlx::PgPool;
use tokio::time::Instant;

use crate::broker::SharedNotifyListener;
use crate::core::{
    OperationalErrorCode, OutcomeCode, RetrievalCode, TaskError, TaskResult, WorkflowStatus,
    WorkflowTaskStatus,
};

use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::info::WorkflowTaskInfo;

// ---------------------------------------------------------------------------
// SQL constants
// ---------------------------------------------------------------------------

const GET_WORKFLOW_STATUS_SQL: &str = "\
SELECT status FROM horsies_workflows WHERE id = $1";

const GET_WORKFLOW_RESULT_SQL: &str = "\
SELECT status, result, error FROM horsies_workflows WHERE id = $1";

const GET_ALL_TASK_RESULTS_SQL: &str = "\
SELECT node_id, task_index, task_name, status, result, started_at, completed_at,
       sub_workflow_id, sub_workflow_summary
FROM horsies_workflow_tasks
WHERE workflow_id = $1
ORDER BY task_index";

const GET_TASK_RESULT_BY_NODE_ID_SQL: &str = "\
SELECT result
FROM horsies_workflow_tasks
WHERE workflow_id = $1 AND node_id = $2
  AND result IS NOT NULL";

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

#[derive(Debug, sqlx::FromRow)]
struct StatusRow {
    status: String,
}

#[derive(Debug, sqlx::FromRow)]
struct WorkflowResultRow {
    status: String,
    result: Option<String>,
    error: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct TaskResultRow {
    node_id: Option<String>,
    task_index: i32,
    task_name: String,
    status: String,
    result: Option<String>,
    started_at: Option<chrono::DateTime<chrono::Utc>>,
    completed_at: Option<chrono::DateTime<chrono::Utc>>,
    sub_workflow_id: Option<String>,
    sub_workflow_summary: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct NodeResultRow {
    result: Option<String>,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Get the current status of a workflow.
pub async fn get_workflow_status(
    pool: &PgPool,
    workflow_id: &str,
) -> Result<WorkflowStatus, WorkflowError> {
    let row: Option<StatusRow> = sqlx::query_as(GET_WORKFLOW_STATUS_SQL)
        .bind(workflow_id)
        .fetch_optional(pool)
        .await?;

    match row {
        Some(r) => Ok(parse_workflow_status(&r.status)),
        None => Err(WorkflowError::WorkflowNotFound {
            workflow_id: workflow_id.to_owned(),
        }),
    }
}

/// Get the typed result of a workflow, optionally waiting for completion.
///
/// Follows the same LISTEN/NOTIFY pattern as `PostgresBroker::get_result`:
/// 1. Quick-check for terminal status
/// 2. Subscribe to the shared `workflow_done` listener
/// 3. Re-check (race guard)
/// 4. Wait for notification or timeout
///
/// The `shared_listener` parameter should be obtained from
/// `PostgresBroker::workflow_done_listener()`. All concurrent callers
/// share a single `PgListener` connection, avoiding pool exhaustion.
pub async fn get_workflow_result<T: DeserializeOwned>(
    pool: &PgPool,
    shared_listener: &SharedNotifyListener,
    workflow_id: &str,
    timeout: Option<Duration>,
) -> Result<TaskResult<T>, WorkflowError> {
    let start = Instant::now();

    // Quick check.
    match try_fetch_terminal_result(pool, workflow_id).await? {
        TerminalFetch::Terminal(result) => {
            return Ok(parse_workflow_result::<T>(workflow_id, &result));
        }
        TerminalFetch::NotFound => {
            return Ok(TaskResult::Err(TaskError::builtin(
                RetrievalCode::WorkflowNotFound,
                format!("workflow {} not found", workflow_id),
            )));
        }
        TerminalFetch::NotTerminal => {}
    }

    // Subscribe to shared workflow_done listener.
    let mut subscription = shared_listener.subscribe(workflow_id);

    // Re-check after subscribing (race guard).
    match try_fetch_terminal_result(pool, workflow_id).await? {
        TerminalFetch::Terminal(result) => {
            return Ok(parse_workflow_result::<T>(workflow_id, &result));
        }
        TerminalFetch::NotFound => {
            return Ok(TaskResult::Err(TaskError::builtin(
                RetrievalCode::WorkflowNotFound,
                format!("workflow {} not found", workflow_id),
            )));
        }
        TerminalFetch::NotTerminal => {}
    }

    let deadline = timeout.map(|t| start + t);

    loop {
        let remaining = match deadline {
            Some(d) => {
                let now = Instant::now();
                if now >= d {
                    return final_poll_or_timeout(pool, workflow_id, start).await;
                }
                Some(d - now)
            }
            None => None,
        };

        let wake = match remaining {
            Some(dur) => match tokio::time::timeout(dur, subscription.recv()).await {
                Ok(result) => result.map_err(WorkflowError::Broker),
                Err(_) => return final_poll_or_timeout(pool, workflow_id, start).await,
            },
            None => subscription.recv().await.map_err(WorkflowError::Broker),
        };

        match wake {
            Ok(()) => match try_fetch_terminal_result(pool, workflow_id).await? {
                TerminalFetch::Terminal(result) => {
                    return Ok(parse_workflow_result::<T>(workflow_id, &result));
                }
                TerminalFetch::NotFound => {
                    return Ok(TaskResult::Err(TaskError::builtin(
                        RetrievalCode::WorkflowNotFound,
                        format!("workflow {} not found", workflow_id),
                    )));
                }
                TerminalFetch::NotTerminal => {}
            },
            Err(e) => return Err(e),
        }
    }
}

/// Get all task results keyed by node_id.
pub async fn get_workflow_results(
    pool: &PgPool,
    workflow_id: &str,
) -> Result<HashMap<String, TaskResult<serde_json::Value>>, WorkflowError> {
    let rows: Vec<TaskResultRow> = sqlx::query_as(GET_ALL_TASK_RESULTS_SQL)
        .bind(workflow_id)
        .fetch_all(pool)
        .await?;

    let mut results = HashMap::new();
    for row in rows {
        let Some(node_id) = row.node_id else { continue };
        let Some(json) = row.result else { continue };
        let parsed = match serde_json::from_str::<TaskResult<serde_json::Value>>(&json) {
            Ok(value) => value,
            Err(e) => TaskResult::Err(TaskError::builtin(
                OperationalErrorCode::ResultDeserializationError,
                format!(
                    "failed to parse workflow result for node_id '{}': {}",
                    node_id, e
                ),
            )),
        };
        results.insert(node_id, parsed);
    }

    Ok(results)
}

/// Get a single typed result by node key.
pub async fn get_workflow_result_for<T: DeserializeOwned>(
    pool: &PgPool,
    workflow_id: &str,
    node_id: &str,
) -> Result<TaskResult<T>, WorkflowError> {
    let row: Option<NodeResultRow> = sqlx::query_as(GET_TASK_RESULT_BY_NODE_ID_SQL)
        .bind(workflow_id)
        .bind(node_id)
        .fetch_optional(pool)
        .await?;

    let Some(row) = row else {
        if !workflow_exists(pool, workflow_id).await? {
            return Ok(TaskResult::Err(TaskError::builtin(
                RetrievalCode::WorkflowNotFound,
                format!("workflow {} not found", workflow_id),
            )));
        }
        return Ok(TaskResult::Err(TaskError::builtin(
            RetrievalCode::ResultNotReady,
            format!(
                "result not ready for workflow {} node {}",
                workflow_id, node_id
            ),
        )));
    };

    if let Some(json) = row.result {
        let parsed: TaskResult<serde_json::Value> = match serde_json::from_str(&json) {
            Ok(value) => value,
            Err(e) => {
                return Ok(TaskResult::Err(TaskError::builtin(
                    OperationalErrorCode::ResultDeserializationError,
                    format!(
                        "failed to parse workflow result for node_id '{}': {}",
                        node_id, e
                    ),
                )));
            }
        };
        return Ok(map_task_result_value(parsed, workflow_id, Some(node_id)));
    }

    Ok(TaskResult::Err(TaskError::builtin(
        RetrievalCode::ResultNotReady,
        format!(
            "result not ready for workflow {} node {}",
            workflow_id, node_id
        ),
    )))
}

/// Get all tasks in a workflow with their metadata.
pub async fn get_workflow_tasks(
    pool: &PgPool,
    workflow_id: &str,
) -> Result<Vec<WorkflowTaskInfo>, WorkflowError> {
    let rows: Vec<TaskResultRow> = sqlx::query_as(GET_ALL_TASK_RESULTS_SQL)
        .bind(workflow_id)
        .fetch_all(pool)
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let result = row
                .result
                .and_then(|json| serde_json::from_str::<TaskResult<serde_json::Value>>(&json).ok());
            WorkflowTaskInfo {
                node_id: row.node_id,
                index: row.task_index,
                name: row.task_name,
                status: parse_task_status(&row.status),
                result,
                started_at: row.started_at,
                completed_at: row.completed_at,
                sub_workflow_id: row.sub_workflow_id,
                sub_workflow_summary: row.sub_workflow_summary,
            }
        })
        .collect())
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum TerminalFetch {
    Terminal(WorkflowResultRow),
    NotTerminal,
    NotFound,
}

/// Try to fetch a terminal workflow result. Returns None if not terminal yet.
async fn try_fetch_terminal_result(
    pool: &PgPool,
    workflow_id: &str,
) -> Result<TerminalFetch, WorkflowError> {
    let row: Option<WorkflowResultRow> = sqlx::query_as(GET_WORKFLOW_RESULT_SQL)
        .bind(workflow_id)
        .fetch_optional(pool)
        .await?;

    match row {
        Some(r) if is_terminal(&r.status) => Ok(TerminalFetch::Terminal(r)),
        Some(_) => Ok(TerminalFetch::NotTerminal),
        None => Ok(TerminalFetch::NotFound),
    }
}

/// Final poll before timeout — check one more time, then timeout.
async fn final_poll_or_timeout<T: DeserializeOwned>(
    pool: &PgPool,
    workflow_id: &str,
    start: Instant,
) -> Result<TaskResult<T>, WorkflowError> {
    match try_fetch_terminal_result(pool, workflow_id).await? {
        TerminalFetch::Terminal(result) => Ok(parse_workflow_result(workflow_id, &result)),
        TerminalFetch::NotFound => Ok(TaskResult::Err(TaskError::builtin(
            RetrievalCode::WorkflowNotFound,
            format!("workflow {} not found", workflow_id),
        ))),
        TerminalFetch::NotTerminal => Ok(TaskResult::Err(TaskError::builtin(
            RetrievalCode::WaitTimeout,
            format!(
                "workflow {} not terminal after {}ms",
                workflow_id,
                start.elapsed().as_millis()
            ),
        ))),
    }
}

async fn workflow_exists(pool: &PgPool, workflow_id: &str) -> Result<bool, WorkflowError> {
    let row: Option<StatusRow> = sqlx::query_as(GET_WORKFLOW_STATUS_SQL)
        .bind(workflow_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

/// Parse a terminal workflow result into a typed TaskResult.
fn parse_workflow_result<T: DeserializeOwned>(
    workflow_id: &str,
    row: &WorkflowResultRow,
) -> TaskResult<T> {
    match row.status.as_str() {
        "COMPLETED" => {
            let Some(json) = row.result.as_deref() else {
                return TaskResult::Err(TaskError::builtin(
                    RetrievalCode::ResultNotAvailable,
                    "workflow completed but result is null",
                ));
            };
            let raw: TaskResult<serde_json::Value> = match serde_json::from_str(json) {
                Ok(value) => value,
                Err(e) => {
                    return TaskResult::Err(TaskError::builtin(
                        OperationalErrorCode::ResultDeserializationError,
                        format!("failed to parse workflow result: {}", e),
                    ));
                }
            };
            map_task_result_value(raw, workflow_id, None)
        }
        "FAILED" => {
            let err = match row.error.as_deref() {
                Some(json) => serde_json::from_str::<TaskError>(json).unwrap_or_else(|_| {
                    TaskError::builtin(OperationalErrorCode::BrokerError, "workflow failed")
                }),
                None => TaskError::builtin(OperationalErrorCode::BrokerError, "workflow failed"),
            };
            TaskResult::Err(err)
        }
        "CANCELLED" => TaskResult::Err(TaskError::builtin(
            OutcomeCode::WorkflowCancelled,
            "workflow was cancelled",
        )),
        "PAUSED" => TaskResult::Err(TaskError::builtin(
            OutcomeCode::WorkflowPaused,
            "workflow is paused awaiting intervention",
        )),
        _ => TaskResult::Err(TaskError::builtin(
            RetrievalCode::ResultNotReady,
            format!("workflow {} result not ready", workflow_id),
        )),
    }
}

fn map_task_result_value<T: DeserializeOwned>(
    raw: TaskResult<serde_json::Value>,
    workflow_id: &str,
    node_id: Option<&str>,
) -> TaskResult<T> {
    let scope = match node_id {
        Some(id) => format!("workflow '{}' node '{}'", workflow_id, id),
        None => format!("workflow '{}'", workflow_id),
    };
    match raw {
        TaskResult::Ok(value) => match serde_json::from_value(value) {
            Ok(parsed) => TaskResult::Ok(parsed),
            Err(e) => TaskResult::Err(TaskError::builtin(
                OperationalErrorCode::ResultDeserializationError,
                format!("failed to deserialize result for {}: {}", scope, e),
            )),
        },
        TaskResult::Err(err) => TaskResult::Err(err),
    }
}

fn parse_workflow_status(s: &str) -> WorkflowStatus {
    match s {
        "PENDING" => WorkflowStatus::Pending,
        "RUNNING" => WorkflowStatus::Running,
        "COMPLETED" => WorkflowStatus::Completed,
        "FAILED" => WorkflowStatus::Failed,
        "PAUSED" => WorkflowStatus::Paused,
        "CANCELLED" => WorkflowStatus::Cancelled,
        _ => WorkflowStatus::Pending,
    }
}

fn parse_task_status(s: &str) -> WorkflowTaskStatus {
    match s {
        "PENDING" => WorkflowTaskStatus::Pending,
        "READY" => WorkflowTaskStatus::Ready,
        "ENQUEUED" => WorkflowTaskStatus::Enqueued,
        "RUNNING" => WorkflowTaskStatus::Running,
        "COMPLETED" => WorkflowTaskStatus::Completed,
        "FAILED" => WorkflowTaskStatus::Failed,
        "SKIPPED" => WorkflowTaskStatus::Skipped,
        _ => WorkflowTaskStatus::Pending,
    }
}

fn is_terminal(status: &str) -> bool {
    matches!(status, "COMPLETED" | "FAILED" | "CANCELLED" | "PAUSED")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        OperationalErrorCode, OutcomeCode, RetrievalCode, TaskErrorCode,
    };

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    fn row(status: &str, result: Option<&str>, error: Option<&str>) -> WorkflowResultRow {
        WorkflowResultRow {
            status: status.to_owned(),
            result: result.map(|s| s.to_owned()),
            error: error.map(|s| s.to_owned()),
        }
    }

    // -----------------------------------------------------------------------
    // parse_workflow_result
    // -----------------------------------------------------------------------

    #[test]
    fn completed_with_valid_result() {
        let wrapped = TaskResult::Ok(42);
        let json = serde_json::to_string(&wrapped).unwrap();
        let r = row("COMPLETED", Some(&json), None);

        let result: TaskResult<i32> = parse_workflow_result("wf-1", &r);
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn completed_with_null_result() {
        let r = row("COMPLETED", None, None);

        let result: TaskResult<i32> = parse_workflow_result("wf-1", &r);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            Some(TaskErrorCode::from(RetrievalCode::ResultNotAvailable)),
        );
        assert!(err.message.as_deref().unwrap().contains("result is null"));
    }

    #[test]
    fn completed_with_malformed_json() {
        let r = row("COMPLETED", Some("{not valid json"), None);

        let result: TaskResult<i32> = parse_workflow_result("wf-1", &r);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            Some(TaskErrorCode::from(
                OperationalErrorCode::ResultDeserializationError,
            )),
        );
    }

    #[test]
    fn completed_with_wrong_type() {
        // Result is a string but we try to deserialize as i32.
        let wrapped = TaskResult::Ok("hello".to_owned());
        let json = serde_json::to_string(&wrapped).unwrap();
        let r = row("COMPLETED", Some(&json), None);

        let result: TaskResult<i32> = parse_workflow_result("wf-1", &r);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            Some(TaskErrorCode::from(
                OperationalErrorCode::ResultDeserializationError,
            )),
        );
    }

    #[test]
    fn failed_with_valid_error() {
        let task_err = TaskError::new("VALIDATION", "bad input");
        let err_json = serde_json::to_string(&task_err).unwrap();
        let r = row("FAILED", None, Some(&err_json));

        let result: TaskResult<i32> = parse_workflow_result("wf-1", &r);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            Some(TaskErrorCode::User("VALIDATION".to_owned())),
        );
        assert_eq!(err.message.as_deref(), Some("bad input"));
    }

    #[test]
    fn failed_with_malformed_error_json() {
        let r = row("FAILED", None, Some("{garbage"));

        let result: TaskResult<i32> = parse_workflow_result("wf-1", &r);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            Some(TaskErrorCode::from(OperationalErrorCode::BrokerError)),
        );
        assert!(err.message.as_deref().unwrap().contains("workflow failed"));
    }

    #[test]
    fn failed_with_null_error() {
        let r = row("FAILED", None, None);

        let result: TaskResult<i32> = parse_workflow_result("wf-1", &r);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            Some(TaskErrorCode::from(OperationalErrorCode::BrokerError)),
        );
        assert!(err.message.as_deref().unwrap().contains("workflow failed"));
    }

    #[test]
    fn cancelled_produces_workflow_cancelled() {
        let r = row("CANCELLED", None, None);

        let result: TaskResult<i32> = parse_workflow_result("wf-1", &r);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            Some(TaskErrorCode::from(OutcomeCode::WorkflowCancelled)),
        );
    }

    #[test]
    fn paused_produces_workflow_paused() {
        let r = row("PAUSED", None, None);

        let result: TaskResult<i32> = parse_workflow_result("wf-1", &r);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            Some(TaskErrorCode::from(OutcomeCode::WorkflowPaused)),
        );
    }

    #[test]
    fn unknown_status_produces_result_not_ready() {
        let r = row("RUNNING", None, None);

        let result: TaskResult<i32> = parse_workflow_result("wf-1", &r);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            Some(TaskErrorCode::from(RetrievalCode::ResultNotReady)),
        );
        assert!(err.message.as_deref().unwrap().contains("wf-1"));
    }

    // -----------------------------------------------------------------------
    // map_task_result_value
    // -----------------------------------------------------------------------

    #[test]
    fn map_ok_with_matching_type() {
        let raw = TaskResult::Ok(serde_json::json!(42));

        let result: TaskResult<i32> = map_task_result_value(raw, "wf-1", None);
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn map_ok_with_mismatched_type() {
        let raw = TaskResult::Ok(serde_json::json!("not a number"));

        let result: TaskResult<i32> = map_task_result_value(raw, "wf-1", None);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            Some(TaskErrorCode::from(
                OperationalErrorCode::ResultDeserializationError,
            )),
        );
        assert!(err.message.as_deref().unwrap().contains("wf-1"));
    }

    #[test]
    fn map_ok_with_mismatched_type_includes_node_id() {
        let raw = TaskResult::Ok(serde_json::json!("not a number"));

        let result: TaskResult<i32> = map_task_result_value(raw, "wf-1", Some("step-a"));
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            Some(TaskErrorCode::from(
                OperationalErrorCode::ResultDeserializationError,
            )),
        );
        let msg = err.message.as_deref().unwrap();
        assert!(msg.contains("wf-1"), "expected workflow_id in message: {msg}");
        assert!(msg.contains("step-a"), "expected node_id in message: {msg}");
    }

    #[test]
    fn map_err_passes_through() {
        let original = TaskError::new("MY_CODE", "original error");
        let raw: TaskResult<serde_json::Value> = TaskResult::Err(original.clone());

        let result: TaskResult<i32> = map_task_result_value(raw, "wf-1", None);
        let err = result.unwrap_err();
        assert_eq!(err.error_code, original.error_code);
        assert_eq!(err.message, original.message);
    }
}
