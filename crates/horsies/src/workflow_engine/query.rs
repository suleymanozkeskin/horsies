use std::collections::HashMap;
use std::time::Duration;

use serde::de::DeserializeOwned;
use sqlx::PgPool;
use tokio::time::Instant;
use uuid::Uuid;

use crate::broker::postgres::RESULT_WAIT_REPOLL;
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
    sub_workflow_id: Option<Uuid>,
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
    workflow_id: Uuid,
) -> Result<WorkflowStatus, WorkflowError> {
    let row: Option<StatusRow> = sqlx::query_as(GET_WORKFLOW_STATUS_SQL)
        .bind(workflow_id)
        .fetch_optional(pool)
        .await?;

    match row {
        Some(r) => parse_workflow_status(&r.status),
        None => Err(WorkflowError::WorkflowNotFound { workflow_id }),
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
    workflow_id: Uuid,
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
    let workflow_id_text = workflow_id.to_string();
    let mut subscription = shared_listener.subscribe(&workflow_id_text);

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
        // For a timed wait, stop once the deadline has passed.
        if let Some(d) = deadline {
            if Instant::now() >= d {
                return final_poll_or_timeout(pool, workflow_id, start).await;
            }
        }

        // Cap each wait at the re-poll interval so a lost NOTIFY (listener
        // reconnect) is recovered by a fresh poll rather than hanging the
        // no-timeout wait forever (C3). Never wait past the deadline. Shares the
        // constant with the task-result wait loop.
        let wait = match deadline {
            Some(d) => d
                .saturating_duration_since(Instant::now())
                .min(RESULT_WAIT_REPOLL),
            None => RESULT_WAIT_REPOLL,
        };

        match tokio::time::timeout(wait, subscription.recv()).await {
            // NOTIFY delivered, or the re-poll interval elapsed — re-check.
            Ok(Ok(())) | Err(_) => match try_fetch_terminal_result(pool, workflow_id).await? {
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
            // Listener error propagates.
            Ok(Err(e)) => return Err(WorkflowError::Broker(e)),
        }
    }
}

/// Get all task results keyed by node_id.
pub async fn get_workflow_results(
    pool: &PgPool,
    workflow_id: Uuid,
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
    workflow_id: Uuid,
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
    workflow_id: Uuid,
) -> Result<Vec<WorkflowTaskInfo>, WorkflowError> {
    let rows: Vec<TaskResultRow> = sqlx::query_as(GET_ALL_TASK_RESULTS_SQL)
        .bind(workflow_id)
        .fetch_all(pool)
        .await?;

    rows.into_iter()
        .map(|row| {
            let result = row
                .result
                .and_then(|json| serde_json::from_str::<TaskResult<serde_json::Value>>(&json).ok());
            Ok(WorkflowTaskInfo {
                node_id: row.node_id,
                index: row.task_index,
                name: row.task_name,
                status: parse_task_status(&row.status)?,
                result,
                started_at: row.started_at,
                completed_at: row.completed_at,
                sub_workflow_id: row.sub_workflow_id,
                sub_workflow_summary: row.sub_workflow_summary,
            })
        })
        .collect()
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
    workflow_id: Uuid,
) -> Result<TerminalFetch, WorkflowError> {
    let row: Option<WorkflowResultRow> = sqlx::query_as(GET_WORKFLOW_RESULT_SQL)
        .bind(workflow_id)
        .fetch_optional(pool)
        .await?;

    match row {
        Some(r) => match parse_workflow_status(&r.status)? {
            WorkflowStatus::Completed
            | WorkflowStatus::Failed
            | WorkflowStatus::Paused
            | WorkflowStatus::Cancelled
            | WorkflowStatus::Expired => Ok(TerminalFetch::Terminal(r)),
            WorkflowStatus::Pending | WorkflowStatus::Running => Ok(TerminalFetch::NotTerminal),
        },
        None => Ok(TerminalFetch::NotFound),
    }
}

/// Final poll before timeout — check one more time, then timeout.
async fn final_poll_or_timeout<T: DeserializeOwned>(
    pool: &PgPool,
    workflow_id: Uuid,
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

async fn workflow_exists(pool: &PgPool, workflow_id: Uuid) -> Result<bool, WorkflowError> {
    let row: Option<StatusRow> = sqlx::query_as(GET_WORKFLOW_STATUS_SQL)
        .bind(workflow_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

/// Parse a terminal workflow result into a typed TaskResult.
fn parse_workflow_result<T: DeserializeOwned>(
    workflow_id: Uuid,
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
        "EXPIRED" => {
            let err = match row.error.as_deref() {
                Some(json) => serde_json::from_str::<TaskError>(json).unwrap_or_else(|error| {
                    TaskError::builtin(
                        OperationalErrorCode::ResultDeserializationError,
                        format!("failed to deserialize expired workflow error: {error}"),
                    )
                }),
                None => TaskError::builtin(OutcomeCode::WorkflowExpired, "workflow expired"),
            };
            TaskResult::Err(err)
        }
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
    workflow_id: Uuid,
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

fn parse_workflow_status(s: &str) -> Result<WorkflowStatus, WorkflowError> {
    WorkflowStatus::try_from(s).map_err(WorkflowError::InvalidStatus)
}

fn parse_task_status(s: &str) -> Result<WorkflowTaskStatus, WorkflowError> {
    match s {
        "PENDING" => Ok(WorkflowTaskStatus::Pending),
        "READY" => Ok(WorkflowTaskStatus::Ready),
        "ENQUEUED" => Ok(WorkflowTaskStatus::Enqueued),
        "RUNNING" => Ok(WorkflowTaskStatus::Running),
        "COMPLETED" => Ok(WorkflowTaskStatus::Completed),
        "FAILED" => Ok(WorkflowTaskStatus::Failed),
        "SKIPPED" => Ok(WorkflowTaskStatus::Skipped),
        unknown => Err(WorkflowError::InvalidStatus(unknown.to_owned())),
    }
}

#[cfg(test)]
fn is_terminal(status: &str) -> bool {
    matches!(
        status,
        "COMPLETED" | "FAILED" | "PAUSED" | "CANCELLED" | "EXPIRED"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{OperationalErrorCode, OutcomeCode, RetrievalCode, TaskErrorCode};

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

        let result: TaskResult<i32> = parse_workflow_result(Uuid::nil(), &r);
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn completed_with_null_result() {
        let r = row("COMPLETED", None, None);

        let result: TaskResult<i32> = parse_workflow_result(Uuid::nil(), &r);
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

        let result: TaskResult<i32> = parse_workflow_result(Uuid::nil(), &r);
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

        let result: TaskResult<i32> = parse_workflow_result(Uuid::nil(), &r);
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

        let result: TaskResult<i32> = parse_workflow_result(Uuid::nil(), &r);
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

        let result: TaskResult<i32> = parse_workflow_result(Uuid::nil(), &r);
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

        let result: TaskResult<i32> = parse_workflow_result(Uuid::nil(), &r);
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

        let result: TaskResult<i32> = parse_workflow_result(Uuid::nil(), &r);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            Some(TaskErrorCode::from(OutcomeCode::WorkflowCancelled)),
        );
    }

    #[test]
    fn expired_is_terminal_and_produces_workflow_expired() {
        let r = row("EXPIRED", None, None);

        assert_eq!(
            parse_workflow_status("EXPIRED").unwrap(),
            WorkflowStatus::Expired
        );
        assert!(is_terminal("EXPIRED"));

        let result: TaskResult<i32> = parse_workflow_result(Uuid::nil(), &r);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            Some(TaskErrorCode::from(OutcomeCode::WorkflowExpired)),
        );
    }

    #[test]
    fn paused_produces_workflow_paused() {
        let r = row("PAUSED", None, None);

        assert!(is_terminal("PAUSED"));

        let result: TaskResult<i32> = parse_workflow_result(Uuid::nil(), &r);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            Some(TaskErrorCode::from(OutcomeCode::WorkflowPaused)),
        );
    }

    #[test]
    fn unknown_status_produces_result_not_ready() {
        let r = row("RUNNING", None, None);

        let result: TaskResult<i32> = parse_workflow_result(Uuid::nil(), &r);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            Some(TaskErrorCode::from(RetrievalCode::ResultNotReady)),
        );
        assert!(err
            .message
            .as_deref()
            .unwrap()
            .contains(&Uuid::nil().to_string()));
    }

    #[test]
    fn unknown_persisted_status_fails_closed() {
        assert!(matches!(
            parse_workflow_status("CORRUPT"),
            Err(WorkflowError::InvalidStatus(status)) if status == "CORRUPT"
        ));
    }

    // -----------------------------------------------------------------------
    // map_task_result_value
    // -----------------------------------------------------------------------

    #[test]
    fn map_ok_with_matching_type() {
        let raw = TaskResult::Ok(serde_json::json!(42));

        let result: TaskResult<i32> = map_task_result_value(raw, Uuid::nil(), None);
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn map_ok_with_mismatched_type() {
        let raw = TaskResult::Ok(serde_json::json!("not a number"));

        let result: TaskResult<i32> = map_task_result_value(raw, Uuid::nil(), None);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            Some(TaskErrorCode::from(
                OperationalErrorCode::ResultDeserializationError,
            )),
        );
        assert!(err
            .message
            .as_deref()
            .unwrap()
            .contains(&Uuid::nil().to_string()));
    }

    #[test]
    fn map_ok_with_mismatched_type_includes_node_id() {
        let raw = TaskResult::Ok(serde_json::json!("not a number"));

        let result: TaskResult<i32> = map_task_result_value(raw, Uuid::nil(), Some("step-a"));
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            Some(TaskErrorCode::from(
                OperationalErrorCode::ResultDeserializationError,
            )),
        );
        let msg = err.message.as_deref().unwrap();
        assert!(
            msg.contains(&Uuid::nil().to_string()),
            "expected workflow_id in message: {msg}"
        );
        assert!(msg.contains("step-a"), "expected node_id in message: {msg}");
    }

    #[test]
    fn map_err_passes_through() {
        let original = TaskError::new("MY_CODE", "original error");
        let raw: TaskResult<serde_json::Value> = TaskResult::Err(original.clone());

        let result: TaskResult<i32> = map_task_result_value(raw, Uuid::nil(), None);
        let err = result.unwrap_err();
        assert_eq!(err.error_code, original.error_code);
        assert_eq!(err.message, original.message);
    }
}

#[cfg(test)]
mod wait_tests {
    //! C3 (workflow path): `get_workflow_result` must never block forever. A
    //! no-timeout wait on a workflow that does not exist returns `WorkflowNotFound`
    //! promptly. The lost-NOTIFY re-poll bound mirrors the task-path fix; its 30s
    //! interval is not deterministically unit-testable without injecting it, so
    //! this test pins the deterministic anti-hang property (missing workflow).
    use super::*;
    use crate::broker::postgres::PostgresBroker;
    use crate::core::{OutcomeCode, TaskErrorCode};
    use crate::workflow_engine::bound_handle::WorkflowHandle;
    use serial_test::serial;
    use std::sync::Arc;

    #[tokio::test]
    #[serial]
    async fn get_workflow_result_no_timeout_returns_not_found_for_missing_workflow() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let listener = broker.workflow_done_listener().await.expect("listener");
        let missing = Uuid::new_v4();

        // Wrap in an outer timeout: a regression to an unbounded wait hangs here.
        let res = tokio::time::timeout(
            Duration::from_secs(5),
            get_workflow_result::<i32>(broker.pool(), listener, missing, None),
        )
        .await
        .expect("get_workflow_result(None) must not hang for a missing workflow");

        let outcome = res.expect("no workflow error");
        let err = outcome.unwrap_err();
        assert_eq!(
            err.error_code,
            Some(TaskErrorCode::from(RetrievalCode::WorkflowNotFound)),
        );
    }

    #[tokio::test]
    #[serial]
    async fn workflow_handle_get_returns_immediately_for_paused_workflow() {
        let broker = Arc::new(PostgresBroker::from_pool(
            crate::broker::terminalization_matrix::migrated_pool().await,
        ));
        broker.ensure_schema_initialized().await.expect("schema");
        let workflow_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, definition_key, depth,
                 root_workflow_id, sent_at, created_at, started_at, updated_at
             ) VALUES ($1, 'p7_paused_get', 'PAUSED', 'fail', $2, 0, $1,
                       NOW(), NOW(), NOW(), NOW())",
        )
        .bind(workflow_id)
        .bind(format!("test.p7.paused-get.{workflow_id}"))
        .execute(broker.pool())
        .await
        .expect("seed paused workflow");

        let handle = WorkflowHandle::<i32>::new(
            workflow_id,
            Arc::clone(&broker),
            Arc::new(crate::core::registry::WorkflowSpecRegistry::new()),
            crate::core::config::payload::PayloadPolicy::default(),
            crate::core::RetentionConfig::default(),
        );
        let result = tokio::time::timeout(Duration::from_secs(5), handle.get(None))
            .await
            .expect("PAUSED must complete get() without a terminal notification");
        let error = result.unwrap_err();
        assert_eq!(
            error.error_code,
            Some(TaskErrorCode::from(OutcomeCode::WorkflowPaused)),
        );

        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(broker.pool())
            .await
            .expect("cleanup paused workflow");
    }

    #[tokio::test]
    #[serial]
    async fn workflow_handle_get_preserves_structured_expiry_error() {
        let broker = Arc::new(PostgresBroker::from_pool(
            crate::broker::terminalization_matrix::migrated_pool().await,
        ));
        broker.ensure_schema_initialized().await.expect("schema");
        let workflow_id = Uuid::new_v4();
        let persisted = TaskError {
            error_code: Some(OutcomeCode::WorkflowExpired.into()),
            message: Some("paused_workflow_auto_cancel_after elapsed: 3600 seconds".to_owned()),
            cause: None,
            data: Some(serde_json::json!({
                "policy": "paused_workflow_auto_cancel_after",
                "older_than_seconds": 3600.0,
            })),
        };
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, error, definition_key, depth,
                 root_workflow_id, sent_at, created_at, started_at,
                 completed_at, updated_at
             ) VALUES ($1, 'p7_expired_get', 'EXPIRED', 'fail', $2, $3, 0, $1,
                       NOW(), NOW(), NOW(), NOW(), NOW())",
        )
        .bind(workflow_id)
        .bind(serde_json::to_string(&persisted).unwrap())
        .bind(format!("test.p7.expired-get.{workflow_id}"))
        .execute(broker.pool())
        .await
        .expect("seed expired workflow");

        let handle = WorkflowHandle::<i32>::new(
            workflow_id,
            Arc::clone(&broker),
            Arc::new(crate::core::registry::WorkflowSpecRegistry::new()),
            crate::core::config::payload::PayloadPolicy::default(),
            crate::core::RetentionConfig::default(),
        );
        let error = handle.get(None).await.unwrap_err();
        assert_eq!(error.error_code, persisted.error_code);
        assert_eq!(error.message, persisted.message);
        assert_eq!(error.data, persisted.data);

        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(broker.pool())
            .await
            .expect("cleanup expired workflow");
    }

    #[tokio::test]
    #[serial]
    async fn workflow_handle_get_fails_closed_on_malformed_expiry_error() {
        let broker = Arc::new(PostgresBroker::from_pool(
            crate::broker::terminalization_matrix::migrated_pool().await,
        ));
        broker.ensure_schema_initialized().await.expect("schema");
        let workflow_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, error, definition_key, depth,
                 root_workflow_id, sent_at, created_at, started_at,
                 completed_at, updated_at
             ) VALUES ($1, 'p7_expired_corrupt_get', 'EXPIRED', 'fail',
                       '{corrupt', $2, 0, $1, NOW(), NOW(), NOW(), NOW(), NOW())",
        )
        .bind(workflow_id)
        .bind(format!("test.p7.expired-corrupt-get.{workflow_id}"))
        .execute(broker.pool())
        .await
        .expect("seed malformed expired workflow");

        let handle = WorkflowHandle::<i32>::new(
            workflow_id,
            Arc::clone(&broker),
            Arc::new(crate::core::registry::WorkflowSpecRegistry::new()),
            crate::core::config::payload::PayloadPolicy::default(),
            crate::core::RetentionConfig::default(),
        );
        let error = handle.get(None).await.unwrap_err();
        assert_eq!(
            error.error_code,
            Some(TaskErrorCode::from(
                OperationalErrorCode::ResultDeserializationError,
            )),
        );
        assert!(error
            .message
            .as_deref()
            .unwrap()
            .contains("failed to deserialize expired workflow error"));

        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(broker.pool())
            .await
            .expect("cleanup malformed expired workflow");
    }
}
