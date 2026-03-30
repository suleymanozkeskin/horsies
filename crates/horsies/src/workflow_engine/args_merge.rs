use std::collections::HashMap;

use sqlx::PgPool;

use crate::core::task::result::TaskResult;
use crate::core::{OutcomeCode, RetrievalCode, TaskError};

use crate::workflow_engine::error::WorkflowError;

/// Dependency result fetched in-memory (used by engine's synchronous path).
pub struct DepResult {
    pub status: String,
    pub result: Option<String>,
}

/// Merge args_from dependency results into kwargs JSON (synchronous variant).
///
/// For each (kwarg_name, dep_index) in args_from:
///   - If dep COMPLETED: injects `TaskResult::Ok(result_value)`
///   - Otherwise (SKIPPED/FAILED/missing): injects `TaskResult::Err(UPSTREAM_SKIPPED)`
pub fn merge_args_from_sync(
    existing_kwargs: Option<&str>,
    args_from: &Option<serde_json::Value>,
    dep_results: &HashMap<i32, DepResult>,
) -> Result<Option<String>, WorkflowError> {
    let args_from_map = match args_from {
        Some(v) => match v.as_object() {
            Some(obj) if !obj.is_empty() => obj,
            _ => return Ok(existing_kwargs.map(String::from)),
        },
        None => return Ok(existing_kwargs.map(String::from)),
    };

    let mut kwargs: serde_json::Map<String, serde_json::Value> = match existing_kwargs {
        Some(json) => serde_json::from_str(json).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "failed to parse existing kwargs in sync merge, using empty map");
            serde_json::Map::new()
        }),
        None => serde_json::Map::new(),
    };

    for (kwarg_name, dep_idx_value) in args_from_map {
        let dep_idx = dep_idx_value.as_i64().unwrap_or(-1) as i32;
        let wrapped = wrap_dep_result(dep_idx, dep_results.get(&dep_idx));
        kwargs.insert(kwarg_name.clone(), wrapped);
    }

    Ok(Some(serde_json::to_string(&kwargs)?))
}

#[derive(Debug, sqlx::FromRow)]
struct DepRow {
    task_index: i32,
    status: String,
    result: Option<String>,
}

/// Merge args_from dependency results into kwargs JSON (async variant).
///
/// Fetches dependency results from the database, then applies the same
/// wrapping logic as the synchronous variant.
pub async fn merge_args_from_async(
    pool: &PgPool,
    workflow_id: &str,
    existing_kwargs: Option<&str>,
    args_from: &Option<serde_json::Value>,
    dep_indices: &[i32],
) -> Result<Option<String>, WorkflowError> {
    let args_from_map = match args_from {
        Some(v) => match v.as_object() {
            Some(obj) if !obj.is_empty() => obj,
            _ => return Ok(existing_kwargs.map(String::from)),
        },
        None => return Ok(existing_kwargs.map(String::from)),
    };

    let rows: Vec<DepRow> = sqlx::query_as(
        "SELECT task_index, status, result FROM horsies_workflow_tasks \
         WHERE workflow_id = $1 AND task_index = ANY($2)",
    )
    .bind(workflow_id)
    .bind(dep_indices)
    .fetch_all(pool)
    .await?;

    let dep_map: HashMap<i32, DepResult> = rows
        .into_iter()
        .map(|r| {
            (
                r.task_index,
                DepResult {
                    status: r.status,
                    result: r.result,
                },
            )
        })
        .collect();

    let mut kwargs: serde_json::Map<String, serde_json::Value> = match existing_kwargs {
        Some(json) => serde_json::from_str(json).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "failed to parse existing kwargs in async merge, using empty map");
            serde_json::Map::new()
        }),
        None => serde_json::Map::new(),
    };

    for (kwarg_name, dep_idx_value) in args_from_map {
        let dep_idx = dep_idx_value.as_i64().unwrap_or(-1) as i32;
        let wrapped = wrap_dep_result(dep_idx, dep_map.get(&dep_idx));
        kwargs.insert(kwarg_name.clone(), wrapped);
    }

    Ok(Some(serde_json::to_string(&kwargs)?))
}

/// Wrap a single dependency result into a serialized `TaskResult` value.
fn wrap_dep_result(dep_idx: i32, dep: Option<&DepResult>) -> serde_json::Value {
    match dep {
        Some(d) => {
            if let Some(json) = &d.result {
                // Result already stored as TaskResult JSON; pass through as Value.
                serde_json::from_str(json).unwrap_or_else(|e| {
                    tracing::warn!(dep_idx, error = %e, "failed to parse dep result JSON");
                    serde_json::Value::Null
                })
            } else if d.status == "COMPLETED" || d.status == "FAILED" {
                let err = TaskError::builtin(
                    RetrievalCode::ResultNotAvailable,
                    format!("upstream task at index {} has no stored result", dep_idx),
                );
                let wrapped: TaskResult<serde_json::Value> = TaskResult::Err(err);
                serde_json::to_value(&wrapped).unwrap_or_else(|e| {
                    tracing::warn!(dep_idx, error = %e, "failed to serialize wrapped error");
                    serde_json::Value::Null
                })
            } else {
                let err = TaskError::builtin(
                    OutcomeCode::UpstreamSkipped,
                    format!("upstream task at index {} did not complete", dep_idx),
                );
                let wrapped: TaskResult<serde_json::Value> = TaskResult::Err(err);
                serde_json::to_value(&wrapped).unwrap_or_else(|e| {
                    tracing::warn!(dep_idx, error = %e, "failed to serialize wrapped error");
                    serde_json::Value::Null
                })
            }
        }
        None => {
            let err = TaskError::builtin(
                OutcomeCode::UpstreamSkipped,
                format!("upstream task at index {} did not complete", dep_idx),
            );
            let wrapped: TaskResult<serde_json::Value> = TaskResult::Err(err);
            serde_json::to_value(&wrapped).unwrap_or_else(|e| {
                tracing::warn!(dep_idx, error = %e, "failed to serialize wrapped error");
                serde_json::Value::Null
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn merge_no_args_from_returns_existing() {
        let result = merge_args_from_sync(Some(r#"{"x": 1}"#), &None, &HashMap::new()).unwrap();
        assert_eq!(result, Some(r#"{"x": 1}"#.to_owned()));
    }

    #[test]
    fn merge_empty_args_from_returns_existing() {
        let args_from = Some(json!({}));
        let result =
            merge_args_from_sync(Some(r#"{"x": 1}"#), &args_from, &HashMap::new()).unwrap();
        assert_eq!(result, Some(r#"{"x": 1}"#.to_owned()));
    }

    #[test]
    fn merge_completed_dep_wraps_in_task_result_ok() {
        let mut deps = HashMap::new();
        let wrapped = TaskResult::Ok(serde_json::Value::from(42));
        let wrapped_json = serde_json::to_string(&wrapped).unwrap();
        deps.insert(
            0,
            DepResult {
                status: "COMPLETED".to_owned(),
                result: Some(wrapped_json),
            },
        );

        let args_from = Some(json!({"answer": 0}));
        let result = merge_args_from_sync(None, &args_from, &deps)
            .unwrap()
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        let answer = &parsed["answer"];
        assert_eq!(answer["__type"], "ok");
        assert_eq!(answer["value"], 42);
    }

    #[test]
    fn merge_failed_dep_wraps_in_task_result_err() {
        let mut deps = HashMap::new();
        deps.insert(
            0,
            DepResult {
                status: "FAILED".to_owned(),
                result: None,
            },
        );

        let args_from = Some(json!({"answer": 0}));
        let result = merge_args_from_sync(None, &args_from, &deps)
            .unwrap()
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        let answer = &parsed["answer"];
        assert_eq!(answer["__type"], "err");
        assert!(answer["value"]["message"]
            .as_str()
            .unwrap()
            .contains("no stored result"));
    }

    #[test]
    fn merge_missing_dep_wraps_in_task_result_err() {
        let deps = HashMap::new(); // no deps
        let args_from = Some(json!({"answer": 0}));
        let result = merge_args_from_sync(None, &args_from, &deps)
            .unwrap()
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        let answer = &parsed["answer"];
        assert_eq!(answer["__type"], "err");
    }

    #[test]
    fn merge_preserves_existing_kwargs() {
        let mut deps = HashMap::new();
        let wrapped = TaskResult::Ok(serde_json::Value::from("hello"));
        let wrapped_json = serde_json::to_string(&wrapped).unwrap();
        deps.insert(
            0,
            DepResult {
                status: "COMPLETED".to_owned(),
                result: Some(wrapped_json),
            },
        );

        let args_from = Some(json!({"dep_val": 0}));
        let result = merge_args_from_sync(Some(r#"{"existing": true}"#), &args_from, &deps)
            .unwrap()
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["existing"], true);
        assert_eq!(parsed["dep_val"]["__type"], "ok");
        assert_eq!(parsed["dep_val"]["value"], "hello");
    }
}
