use std::collections::HashMap;

use sqlx::PgPool;
use uuid::Uuid;

use crate::core::task::result::TaskResult;
use crate::core::{OperationalErrorCode, OutcomeCode, TaskError, TaskErrorKind};

use crate::workflow_engine::error::WorkflowError;

/// Dependency result fetched in-memory (used by engine's synchronous path).
pub struct DepResult {
    pub status: String,
    pub result: Option<String>,
}

/// The object that injected members merge into: the node's stored input.
///
/// The stored text must pass the core's strict JSON read rule. The merge
/// re-encodes the object, so an integer that the parse would round is
/// refused instead of changed. Only an object takes named members: any other
/// input, `null` included, is refused. The client gives a node with members an
/// object input such as `{}`; the core does not read `null` as `{}`.
pub(crate) fn input_object(
    input: &str,
) -> Result<serde_json::Map<String, serde_json::Value>, WorkflowError> {
    crate::core::codec::check_json_text(input).map_err(|error| {
        WorkflowError::Validation(format!("stored node input refused: {error}"))
    })?;
    crate::core::history::rerun::input_envelope::validate_integer_domain(input).map_err(
        |detail| {
            WorkflowError::Validation(format!(
                "stored node input holds a number that the merge would round: {detail}"
            ))
        },
    )?;
    let parsed: serde_json::Value = serde_json::from_str(input).map_err(|e| {
        tracing::error!(error = %e, "failed to parse a node input for a member merge");
        e
    })?;
    match parsed {
        serde_json::Value::Object(object) => Ok(object),
        _ => Err(WorkflowError::Validation(
            "a node input that takes args_from or workflow context members must be a JSON object"
                .to_owned(),
        )),
    }
}

/// The node input after the `args_from` merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ArgsFromMerge {
    /// The input with one member per `args_from` key; the input unchanged when the node has no
    /// `args_from`.
    Merged(String),
    /// An `args_from` key is also a member of the node's static input. Nothing is merged: the node
    /// fails with [`ArgsFromConflict::node_error`] (Python `core/workflows/engine.py:834-839`,
    /// `:1151-1163`).
    StaticMemberConflict(ArgsFromConflict),
}

/// An `args_from` key that the node's static input also sets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ArgsFromConflict {
    /// The conflicting key.
    pub(crate) key: String,
}

/// The kind of the node whose input takes the `args_from` members. It names the node in the
/// conflict message, as Python does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergedNodeKind {
    Task,
    SubWorkflow,
}

impl ArgsFromConflict {
    /// The error that the failed node stores: `WORKFLOW_ENQUEUE_FAILED` with Python's message and
    /// `data` (`core/workflows/engine.py:472-555`, `:620-631`, `:834-839`, `:1151-1163`).
    pub(crate) fn node_error(
        &self,
        workflow_id: Uuid,
        task_index: i32,
        kind: MergedNodeKind,
    ) -> TaskError {
        let node = match kind {
            MergedNodeKind::Task => format!("task {task_index}"),
            MergedNodeKind::SubWorkflow => format!("subworkflow task {task_index}"),
        };
        TaskError {
            data: Some(serde_json::json!({
                "workflow_id": workflow_id.to_string(),
                "task_index": task_index,
            })),
            ..TaskError::builtin(
                OperationalErrorCode::WorkflowEnqueueFailed,
                format!(
                    "args_from key '{}' conflicts with static kwarg ({node})",
                    self.key
                ),
            )
        }
    }
}

/// The `args_from` key that the static input also sets, if any.
///
/// With several, the first in the order in which Python reads the stored `args_from` (`jsonb` key
/// order: shorter keys first, then by bytes), so the core names the key that Python names.
fn static_member_conflict(
    object: &serde_json::Map<String, serde_json::Value>,
    args_from: &serde_json::Map<String, serde_json::Value>,
) -> Option<ArgsFromConflict> {
    args_from
        .keys()
        .filter(|key| object.contains_key(key.as_str()))
        .min_by(|left, right| (left.len(), left.as_bytes()).cmp(&(right.len(), right.as_bytes())))
        .map(|key| ArgsFromConflict { key: key.clone() })
}

/// Merge args_from dependency results into the node's input (synchronous variant).
///
/// With no args_from the input is returned unchanged. An `args_from` key that the static input
/// also sets gives [`ArgsFromMerge::StaticMemberConflict`] and merges nothing. Otherwise, for each
/// (field_name, dep_index) in args_from:
///   - If dep COMPLETED: injects `TaskResult::Ok(result_value)`
///   - Otherwise (SKIPPED/FAILED/missing): injects `TaskResult::Err(UPSTREAM_SKIPPED)`
pub(crate) fn merge_args_from_sync(
    input: &str,
    args_from: &Option<serde_json::Value>,
    dep_results: &HashMap<i32, DepResult>,
) -> Result<ArgsFromMerge, WorkflowError> {
    let args_from_map = match args_from {
        Some(v) => match v.as_object() {
            Some(obj) if !obj.is_empty() => obj,
            _ => return Ok(ArgsFromMerge::Merged(input.to_owned())),
        },
        None => return Ok(ArgsFromMerge::Merged(input.to_owned())),
    };

    let mut object = input_object(input)?;
    if let Some(conflict) = static_member_conflict(&object, args_from_map) {
        return Ok(ArgsFromMerge::StaticMemberConflict(conflict));
    }

    for (field_name, dep_idx_value) in args_from_map {
        let dep_idx = dep_idx_value.as_i64().unwrap_or(-1) as i32;
        let wrapped = wrap_dep_result(dep_idx, dep_results.get(&dep_idx));
        object.insert(field_name.clone(), wrapped);
    }

    Ok(ArgsFromMerge::Merged(serde_json::to_string(&object)?))
}

#[derive(Debug, sqlx::FromRow)]
struct DepRow {
    task_index: i32,
    status: String,
    result: Option<String>,
}

/// Merge args_from dependency results into the node's input (async variant).
///
/// Fetches dependency results from the database, then applies the same
/// conflict check and wrapping logic as the synchronous variant.
pub(crate) async fn merge_args_from_async(
    pool: &PgPool,
    workflow_id: Uuid,
    input: &str,
    args_from: &Option<serde_json::Value>,
    dep_indices: &[i32],
) -> Result<ArgsFromMerge, WorkflowError> {
    let args_from_map = match args_from {
        Some(v) => match v.as_object() {
            Some(obj) if !obj.is_empty() => obj,
            _ => return Ok(ArgsFromMerge::Merged(input.to_owned())),
        },
        None => return Ok(ArgsFromMerge::Merged(input.to_owned())),
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

    let mut object = input_object(input)?;
    if let Some(conflict) = static_member_conflict(&object, args_from_map) {
        return Ok(ArgsFromMerge::StaticMemberConflict(conflict));
    }

    for (field_name, dep_idx_value) in args_from_map {
        let dep_idx = dep_idx_value.as_i64().unwrap_or(-1) as i32;
        let wrapped = wrap_dep_result(dep_idx, dep_map.get(&dep_idx));
        object.insert(field_name.clone(), wrapped);
    }

    Ok(ArgsFromMerge::Merged(serde_json::to_string(&object)?))
}

/// Wrap a single dependency result into a serialized `TaskResult` value. Python:
/// `get_dependency_results` (`core/workflows/engine.py:2816-2850`).
///
/// - SKIPPED: `UPSTREAM_SKIPPED` with `data.dependency_index`, also when a result is stored.
/// - A stored result: that result; one that does not parse gives `RESULT_DESERIALIZATION_ERROR`.
/// - COMPLETED or FAILED with no stored result: `RESULT_DESERIALIZATION_ERROR` naming the dependency
///   (decided: no writer stores such a node, so it is stored data that does not parse).
/// - A dependency that is not terminal, or has no row: `UPSTREAM_SKIPPED`. Python leaves the key out for
///   such a dependency; that rule belongs to the `args_from` injection of the node input.
fn wrap_dep_result(dep_idx: i32, dep: Option<&DepResult>) -> serde_json::Value {
    match dep {
        Some(DepResult { status, .. }) if status == "SKIPPED" => task_result_to_value(
            dep_idx,
            TaskResult::Err(upstream_skipped_error(dep_idx)),
            "failed to serialize wrapped upstream-skipped error",
        ),
        Some(DepResult {
            result: Some(json), ..
        }) => serde_json::from_str(json).unwrap_or_else(|e| {
            tracing::warn!(dep_idx, error = %e, "failed to parse dep result JSON");
            dep_parse_error_value(dep_idx, e)
        }),
        Some(DepResult {
            status,
            result: None,
        }) if status == "COMPLETED" || status == "FAILED" => task_result_to_value(
            dep_idx,
            TaskResult::Err(missing_result_error(dep_idx, status)),
            "failed to serialize wrapped missing-result error",
        ),
        Some(_) | None => task_result_to_value(
            dep_idx,
            TaskResult::Err(upstream_skipped_error(dep_idx)),
            "failed to serialize wrapped missing-dependency error",
        ),
    }
}

/// The entry of a SKIPPED dependency (Python `core/workflows/engine.py:2830-2835`).
pub(crate) fn upstream_skipped_error(dep_idx: i32) -> TaskError {
    TaskError {
        exception: None,
        error_code: OutcomeCode::UpstreamSkipped.into(),
        data: Some(serde_json::json!({ "dependency_index": dep_idx })),
        message: Some("Upstream dependency was SKIPPED".to_owned()),
        kind: TaskErrorKind::Plain,
    }
}

/// The entry of a terminal dependency with no stored result: `RESULT_DESERIALIZATION_ERROR` naming the
/// dependency.
pub(crate) fn missing_result_error(dep_idx: i32, status: &str) -> TaskError {
    TaskError {
        exception: None,
        error_code: OperationalErrorCode::ResultDeserializationError.into(),
        data: Some(serde_json::json!({ "dependency_index": dep_idx, "status": status })),
        message: Some(format!(
            "Upstream dependency at index {dep_idx} is {status} with no stored result"
        )),
        kind: TaskErrorKind::Plain,
    }
}

fn task_result_to_value(
    dep_idx: i32,
    wrapped: TaskResult<serde_json::Value>,
    context: &str,
) -> serde_json::Value {
    serde_json::to_value(&wrapped).unwrap_or_else(|e| {
        tracing::warn!(dep_idx, error = %e, context, "dependency result wrapping failed");
        // A decodable envelope with a code: no code-less fallback.
        serde_json::json!({
            "__h_task_result__": true,
            "ok": null,
            "err": {
                "exception": null,
                "error_code": {"__builtin_task_code__": "WORKER_SERIALIZATION_ERROR"},
                "data": null,
                "message": context,
            }
        })
    })
}

fn dep_parse_error_value(dep_idx: i32, error: serde_json::Error) -> serde_json::Value {
    let err = TaskError::builtin(
        OperationalErrorCode::ResultDeserializationError,
        format!(
            "failed to parse upstream task result at index {}: {}",
            dep_idx, error
        ),
    );
    let wrapped: TaskResult<serde_json::Value> = TaskResult::Err(err);
    task_result_to_value(
        dep_idx,
        wrapped,
        "failed to serialize dependency parse error wrapper",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The merged input; a test that reaches a conflict fails.
    fn merged(
        input: &str,
        args_from: &Option<serde_json::Value>,
        dep_results: &HashMap<i32, DepResult>,
    ) -> String {
        match merge_args_from_sync(input, args_from, dep_results).unwrap() {
            ArgsFromMerge::Merged(merged) => merged,
            ArgsFromMerge::StaticMemberConflict(conflict) => {
                panic!("unexpected args_from conflict on '{}'", conflict.key)
            }
        }
    }

    #[test]
    fn an_args_from_key_that_names_a_static_member_is_a_conflict() {
        let mut deps = HashMap::new();
        deps.insert(
            0,
            DepResult {
                status: "COMPLETED".to_owned(),
                result: Some(serde_json::to_string(&TaskResult::Ok(json!(1))).unwrap()),
            },
        );
        let outcome =
            merge_args_from_sync(r#"{"x": 5, "y": 6}"#, &Some(json!({"x": 0})), &deps).unwrap();
        assert_eq!(
            outcome,
            ArgsFromMerge::StaticMemberConflict(ArgsFromConflict {
                key: "x".to_owned()
            })
        );
    }

    #[test]
    fn with_several_conflicts_the_first_key_in_stored_order_is_named() {
        // jsonb key order: shorter keys first, then by bytes. Python iterates in that order.
        let outcome = merge_args_from_sync(
            r#"{"aa": 1, "b": 2, "c": 3}"#,
            &Some(json!({"aa": 0, "c": 0, "b": 0})),
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(
            outcome,
            ArgsFromMerge::StaticMemberConflict(ArgsFromConflict {
                key: "b".to_owned()
            })
        );
    }

    #[test]
    fn the_conflict_error_follows_python() {
        let workflow_id = Uuid::nil();
        let conflict = ArgsFromConflict {
            key: "x".to_owned(),
        };
        let task = conflict.node_error(workflow_id, 3, MergedNodeKind::Task);
        assert_eq!(task.error_code.to_string(), "WORKFLOW_ENQUEUE_FAILED");
        assert_eq!(
            task.message.as_deref(),
            Some("args_from key 'x' conflicts with static kwarg (task 3)")
        );
        assert_eq!(
            task.data,
            Some(json!({"workflow_id": workflow_id.to_string(), "task_index": 3}))
        );
        let child = conflict.node_error(workflow_id, 4, MergedNodeKind::SubWorkflow);
        assert_eq!(
            child.message.as_deref(),
            Some("args_from key 'x' conflicts with static kwarg (subworkflow task 4)")
        );
    }

    #[test]
    fn merge_no_args_from_returns_existing() {
        let result = merged(r#"{"x": 1}"#, &None, &HashMap::new());
        assert_eq!(result, r#"{"x": 1}"#);
    }

    #[test]
    fn merge_empty_args_from_returns_existing() {
        let args_from = Some(json!({}));
        let result = merged(r#"{"x": 1}"#, &args_from, &HashMap::new());
        assert_eq!(result, r#"{"x": 1}"#);
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
        let result = merged("{}", &args_from, &deps);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        let answer = &parsed["answer"];
        assert_eq!(answer["__h_task_result__"], true);
        assert_eq!(answer["ok"], 42);
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
        let result = merged("{}", &args_from, &deps);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        let answer = &parsed["answer"];
        assert!(answer["err"].is_object());
        assert!(answer["err"]["message"]
            .as_str()
            .unwrap()
            .contains("no stored result"));
    }

    #[test]
    fn merge_missing_dep_wraps_in_task_result_err() {
        let deps = HashMap::new(); // no deps
        let args_from = Some(json!({"answer": 0}));
        let result = merged("{}", &args_from, &deps);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        let answer = &parsed["answer"];
        assert!(answer["err"].is_object());
    }

    #[test]
    fn merge_preserves_existing_input_fields() {
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
        let result = merged(r#"{"existing": true}"#, &args_from, &deps);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["existing"], true);
        assert_eq!(parsed["dep_val"]["__h_task_result__"], true);
        assert_eq!(parsed["dep_val"]["ok"], "hello");
    }

    #[test]
    fn a_skipped_dependency_gives_upstream_skipped_with_its_index() {
        let mut deps = HashMap::new();
        deps.insert(
            3,
            DepResult {
                status: "SKIPPED".to_owned(),
                result: Some(serde_json::to_string(&TaskResult::Ok(json!(1))).unwrap()),
            },
        );
        let result = merged("{}", &Some(json!({"answer": 3})), &deps);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        let answer: TaskResult<serde_json::Value> =
            serde_json::from_value(parsed["answer"].clone()).unwrap();
        let TaskResult::Err(error) = answer else {
            panic!("a skipped dependency must give an error entry");
        };
        assert_eq!(error.error_code.to_string(), "UPSTREAM_SKIPPED");
        assert_eq!(
            error.message.as_deref(),
            Some("Upstream dependency was SKIPPED")
        );
        assert_eq!(error.data, Some(json!({"dependency_index": 3})));
    }

    #[test]
    fn a_terminal_dependency_with_no_result_gives_result_deserialization_error() {
        for status in ["COMPLETED", "FAILED"] {
            let mut deps = HashMap::new();
            deps.insert(
                2,
                DepResult {
                    status: status.to_owned(),
                    result: None,
                },
            );
            let result = merged("{}", &Some(json!({"answer": 2})), &deps);
            let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
            let answer: TaskResult<serde_json::Value> =
                serde_json::from_value(parsed["answer"].clone()).unwrap();
            let TaskResult::Err(error) = answer else {
                panic!("a dependency with no result must give an error entry");
            };
            assert_eq!(error.error_code.to_string(), "RESULT_DESERIALIZATION_ERROR");
            assert_eq!(
                error.data,
                Some(json!({"dependency_index": 2, "status": status}))
            );
        }
    }

    #[test]
    fn merge_invalid_existing_input_is_error() {
        let args_from = Some(json!({"dep_val": 0}));
        for input in [r#"{"existing": true"#, r#"{"a":1,"a":2}"#] {
            let err = merge_args_from_sync(input, &args_from, &HashMap::new()).unwrap_err();
            assert!(matches!(err, WorkflowError::Validation(_)), "{input}");
        }
    }

    #[test]
    fn merge_into_a_non_object_input_is_refused() {
        // `null` is not read as `{}`: only an object takes named members.
        let args_from = Some(json!({"dep_val": 0}));
        for input in ["[1]", "null"] {
            let err = merge_args_from_sync(input, &args_from, &HashMap::new()).unwrap_err();
            assert!(matches!(err, WorkflowError::Validation(_)), "{input}");
        }
    }

    #[test]
    fn merge_corrupt_dep_result_wraps_parse_error() {
        let mut deps = HashMap::new();
        deps.insert(
            0,
            DepResult {
                status: "COMPLETED".to_owned(),
                result: Some("{not-json".to_owned()),
            },
        );

        let args_from = Some(json!({"answer": 0}));
        let result = merged("{}", &args_from, &deps);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        let answer = &parsed["answer"];
        assert!(answer["err"].is_object());
        assert!(answer["err"]["message"]
            .as_str()
            .unwrap()
            .contains("failed to parse upstream task result"));
    }
}
