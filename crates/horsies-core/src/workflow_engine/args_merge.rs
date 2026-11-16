use std::collections::HashMap;

use serde::Serialize;
use sqlx::PgPool;
use uuid::Uuid;

use crate::core::codec::JsonReadError;
use crate::core::task::result::TaskResult;
use crate::core::{OperationalErrorCode, OutcomeCode, TaskError, TaskErrorKind};

use crate::workflow_engine::error::WorkflowError;

/// The marker key of an `args_from` entry (Python `_ENGINE_KEY_TASKRESULT_ENVELOPE`,
/// `core/workflows/engine.py:143`).
pub const ARGS_FROM_ENVELOPE_KEY: &str = "__h_taskresult_envelope__";

/// The keys of an `args_from` entry, besides the marker.
pub const ARGS_FROM_ENTRY_KEYS: [&str; 3] = ["source_task_name", "source_definition_key", "inner"];

/// A dependency row of the node that receives `args_from`: every status, not only terminal ones.
pub struct DepResult {
    /// The node id of the dependency; `None` for a node stored without one.
    pub node_id: Option<String>,
    /// The task name of the dependency node (`__sub_workflow:<name>` for a sub-workflow node).
    pub task_name: String,
    /// The definition key of a sub-workflow node; `None` for a task node.
    pub sub_definition_key: Option<String>,
    pub status: String,
    pub result: Option<String>,
}

/// The stored form of one `args_from` kwarg: Python's wrap around the dependency's result envelope
/// (`core/workflows/engine.py:891-896`).
#[derive(Serialize)]
struct ArgsFromEntry<'a> {
    #[serde(rename = "__h_taskresult_envelope__")]
    marker: bool,
    source_task_name: &'a str,
    source_definition_key: Option<&'a str>,
    inner: TaskResult<serde_json::Value>,
}

/// The object that injected members merge into: the node's stored input.
///
/// The stored text is read with the core's strict reader: 300 levels of room,
/// so a user value of 200 levels inside its envelopes reads (Python
/// `core/codec/json_value.py:54-75`). The merge re-encodes the object, so an
/// integer that the parse would round is refused instead of changed. Only an
/// object takes named members: any other input, `null` included, is refused.
/// The client gives a node with members an object input such as `{}`; the
/// core does not read `null` as `{}`.
pub(crate) fn input_object(
    input: &str,
) -> Result<serde_json::Map<String, serde_json::Value>, WorkflowError> {
    let parsed: serde_json::Value =
        crate::core::codec::from_json_str(input).map_err(|error| match error {
            JsonReadError::Refused(refusal) => {
                WorkflowError::Validation(format!("stored node input refused: {refusal}"))
            }
            JsonReadError::Decode(error) => {
                tracing::error!(error = %error, "failed to parse a node input for a member merge");
                WorkflowError::Serialization(error)
            }
        })?;
    crate::core::history::rerun::input_envelope::validate_integer_domain(input).map_err(
        |detail| {
            WorkflowError::Validation(format!(
                "stored node input holds a number that the merge would round: {detail}"
            ))
        },
    )?;
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
        crate::workflow_engine::engine::node_not_prepared_error(
            workflow_id,
            task_index,
            OperationalErrorCode::WorkflowEnqueueFailed,
            format!(
                "args_from key '{}' conflicts with static kwarg ({node})",
                self.key
            ),
        )
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
/// (field_name, dep_index) in args_from, the field holds Python's `args_from` wrap around the
/// dependency's result ([`dependency_result`]). A dependency index with no node row gets no field
/// (Python injects only the results that exist, `core/workflows/engine.py:841`); every node of a
/// workflow has a row from its start, so this is logged as an error.
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

    insert_args_from_entries(&mut object, args_from_map, dep_results)?;

    Ok(ArgsFromMerge::Merged(serde_json::to_string(&object)?))
}

#[derive(Debug, sqlx::FromRow)]
struct DepRow {
    task_index: i32,
    node_id: Option<String>,
    task_name: String,
    sub_definition_key: Option<String>,
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
        "SELECT task_index, node_id, task_name, sub_definition_key, status, result \
         FROM horsies_workflow_tasks \
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
                    node_id: r.node_id,
                    task_name: r.task_name,
                    sub_definition_key: r.sub_definition_key,
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

    insert_args_from_entries(&mut object, args_from_map, &dep_map)?;

    Ok(ArgsFromMerge::Merged(serde_json::to_string(&object)?))
}

/// Insert one `args_from` entry per kwarg of `args_from_map` into `kwargs`.
fn insert_args_from_entries(
    kwargs: &mut serde_json::Map<String, serde_json::Value>,
    args_from_map: &serde_json::Map<String, serde_json::Value>,
    dep_results: &HashMap<i32, DepResult>,
) -> Result<(), WorkflowError> {
    for (kwarg_name, dep_idx_value) in args_from_map {
        let dep_idx = dep_idx_value.as_i64().unwrap_or(-1) as i32;
        let Some(dep) = dep_results.get(&dep_idx) else {
            tracing::error!(
                dep_idx,
                kwarg = %kwarg_name,
                "args_from names a dependency index with no node row; the kwarg is left out",
            );
            continue;
        };
        let entry = ArgsFromEntry {
            marker: true,
            source_task_name: &dep.task_name,
            source_definition_key: dep.sub_definition_key.as_deref(),
            inner: dependency_result(dep_idx, dep),
        };
        kwargs.insert(kwarg_name.clone(), serde_json::to_value(&entry)?);
    }
    Ok(())
}

/// Why an `args_from` entry of a node's kwargs does not read (Python `core/worker/child_runner.py:1573-1612`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArgsFromEntryRefused {
    #[error("args_from envelope for kwarg '{kwarg}' has the unexpected key {key:?}")]
    UnexpectedKey { kwarg: String, key: String },
    #[error("args_from envelope for kwarg '{kwarg}' is missing source_task_name")]
    MissingSourceTaskName { kwarg: String },
    #[error("args_from envelope for kwarg '{kwarg}' has a source_definition_key that is not text")]
    SourceDefinitionKeyNotText { kwarg: String },
    #[error("args_from envelope for kwarg '{kwarg}' has no inner result")]
    MissingInner { kwarg: String },
}

/// Replace each `args_from` entry of `kwargs` by its `inner` result envelope; returns the names of those
/// kwargs. An entry is a kwarg value that is an object with `ARGS_FROM_ENVELOPE_KEY: true`; it must have a text
/// `source_task_name`, a text or null `source_definition_key`, an `inner` value and no other key. The receiving
/// `TaskResult` field decodes the envelope with its own type (Python `core/worker/child_runner.py:1566-1640`).
pub(crate) fn unwrap_args_from_entries(
    kwargs: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<std::collections::HashSet<String>, ArgsFromEntryRefused> {
    let mut unwrapped = std::collections::HashSet::new();
    for (name, value) in kwargs.iter_mut() {
        let serde_json::Value::Object(entry) = value else {
            continue;
        };
        if entry.get(ARGS_FROM_ENVELOPE_KEY) != Some(&serde_json::Value::Bool(true)) {
            continue;
        }
        let inner = args_from_inner(name, entry)?;
        *value = inner;
        unwrapped.insert(name.clone());
    }
    Ok(unwrapped)
}

fn args_from_inner(
    name: &str,
    entry: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<serde_json::Value, ArgsFromEntryRefused> {
    if let Some(key) = entry.keys().find(|key| {
        key.as_str() != ARGS_FROM_ENVELOPE_KEY && !ARGS_FROM_ENTRY_KEYS.contains(&key.as_str())
    }) {
        return Err(ArgsFromEntryRefused::UnexpectedKey {
            kwarg: name.to_owned(),
            key: key.clone(),
        });
    }
    if !matches!(
        entry.get("source_task_name"),
        Some(serde_json::Value::String(_))
    ) {
        return Err(ArgsFromEntryRefused::MissingSourceTaskName {
            kwarg: name.to_owned(),
        });
    }
    match entry.get("source_definition_key") {
        None | Some(serde_json::Value::Null) | Some(serde_json::Value::String(_)) => {}
        Some(_) => {
            return Err(ArgsFromEntryRefused::SourceDefinitionKeyNotText {
                kwarg: name.to_owned(),
            });
        }
    }
    entry
        .remove("inner")
        .ok_or_else(|| ArgsFromEntryRefused::MissingInner {
            kwarg: name.to_owned(),
        })
}

/// A stored node input from which [`child_builder_input`] cannot make the builder input: the text does not
/// read under the strict reader, or an `args_from` entry is malformed. The engine wrote that text, so this is
/// corrupt stored data, not a user error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredInputRefused {
    /// Why the text is refused.
    pub(crate) refusal: String,
}

/// The input that a child workflow's spec builder reads: each `args_from` entry of an object input replaced by
/// its `inner` result envelope. The builder runs in the engine, so it gets the result itself, not the wrap that
/// crosses to a worker (Python `core/workflows/engine.py:1164-1173`). An input with no entry is returned
/// unchanged.
///
/// Errors: [`StoredInputRefused`] for a text that does not read or a malformed entry. Pure.
pub(crate) fn child_builder_input(input: &str) -> Result<String, StoredInputRefused> {
    let parsed: serde_json::Value =
        crate::core::codec::from_json_str(input).map_err(|error| StoredInputRefused {
            refusal: error.to_string(),
        })?;
    let serde_json::Value::Object(mut object) = parsed else {
        return Ok(input.to_owned());
    };
    let unwrapped =
        unwrap_args_from_entries(&mut object).map_err(|refusal| StoredInputRefused {
            refusal: refusal.to_string(),
        })?;
    match unwrapped.is_empty() {
        true => Ok(input.to_owned()),
        false => Ok(serde_json::Value::Object(object).to_string()),
    }
}

/// The result of one dependency, as the `inner` envelope of its `args_from` entry. Python:
/// `get_dependency_results` (`core/workflows/engine.py:2816-2850`).
///
/// - SKIPPED: `UPSTREAM_SKIPPED` with `data.dependency_index`, also when a result is stored.
/// - COMPLETED or FAILED with a stored result: that result, read under the stored result rules; one that does
///   not read gives `RESULT_DESERIALIZATION_ERROR`.
/// - COMPLETED or FAILED with a NULL or empty stored result: [`missing_result_error`] (decided: no writer stores
///   such a node, so it is stored data that does not parse).
/// - A dependency that is not terminal: `UPSTREAM_SKIPPED`. Python leaves the key out for such a dependency
///   (workflow ledger row 18).
fn dependency_result(dep_idx: i32, dep: &DepResult) -> TaskResult<serde_json::Value> {
    match (dep.status.as_str(), dep.result.as_deref()) {
        ("SKIPPED", _) => TaskResult::Err(upstream_skipped_error(dep_idx)),
        ("COMPLETED" | "FAILED", Some(text)) if !text.is_empty() => {
            serde_json::from_str::<TaskResult<serde_json::Value>>(text).unwrap_or_else(|e| {
                tracing::warn!(dep_idx, error = %e, "failed to parse dep result JSON");
                TaskResult::Err(dep_parse_error(dep_idx, &e))
            })
        }
        (status @ ("COMPLETED" | "FAILED"), _) => TaskResult::Err(missing_result_error(
            dep_idx,
            dep.node_id.as_deref(),
            &dep.task_name,
            status,
        )),
        (_, _) => TaskResult::Err(upstream_skipped_error(dep_idx)),
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

/// The entry of a COMPLETED or FAILED dependency whose stored result is NULL or empty:
/// `RESULT_DESERIALIZATION_ERROR` naming the source node (its id, or `-` when it has none, its index and its
/// task) and its status, with `data` `{"dependency_index": <index>}`. One form for the three readers: the
/// baseline function `horsies_workflow_source_value` (`0005_workflow_functions.sql`) and Python
/// `_missing_result_entry` (`core/workflows/engine.py`) build the same entry. Pure.
pub(crate) fn missing_result_error(
    dep_idx: i32,
    node_id: Option<&str>,
    task_name: &str,
    status: &str,
) -> TaskError {
    let node = node_id.unwrap_or("-");
    TaskError {
        exception: None,
        error_code: OperationalErrorCode::ResultDeserializationError.into(),
        data: Some(serde_json::json!({ "dependency_index": dep_idx })),
        message: Some(format!(
            "Dependency node {node} (index {dep_idx}, task {task_name}) is {status} with no stored result"
        )),
        kind: TaskErrorKind::Plain,
    }
}

fn dep_parse_error(dep_idx: i32, error: &serde_json::Error) -> TaskError {
    TaskError::builtin(
        OperationalErrorCode::ResultDeserializationError,
        format!(
            "failed to parse upstream task result at index {}: {}",
            dep_idx, error
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn dep(status: &str, result: Option<String>) -> DepResult {
        DepResult {
            node_id: Some("source".to_owned()),
            task_name: "produce".to_owned(),
            sub_definition_key: None,
            status: status.to_owned(),
            result,
        }
    }

    /// The `inner` envelope of the entry of `kwarg`, after a check of the wrap.
    fn inner_of(merged: &str, kwarg: &str) -> TaskResult<serde_json::Value> {
        let parsed: serde_json::Value = serde_json::from_str(merged).unwrap();
        let entry = parsed[kwarg].as_object().expect("an args_from entry");
        assert_eq!(entry[ARGS_FROM_ENVELOPE_KEY], true);
        assert_eq!(entry.len(), 1 + ARGS_FROM_ENTRY_KEYS.len());
        serde_json::from_value(entry["inner"].clone()).unwrap()
    }

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
            dep(
                "COMPLETED",
                Some(serde_json::to_string(&TaskResult::Ok(json!(1))).unwrap()),
            ),
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

    /// Python `core/workflows/engine.py:891-896`: the kwarg holds the wrap with the source task name, the source
    /// definition key and the dependency's envelope.
    #[test]
    fn a_completed_dependency_is_stored_in_pythons_wrap() {
        let mut deps = HashMap::new();
        let stored = serde_json::to_string(&TaskResult::Ok(serde_json::Value::from(42))).unwrap();
        deps.insert(0, dep("COMPLETED", Some(stored)));

        let result = merged("{}", &Some(json!({"answer": 0})), &deps);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(
            parsed,
            json!({"answer": {
                "__h_taskresult_envelope__": true,
                "source_task_name": "produce",
                "source_definition_key": null,
                "inner": {"__h_task_result__": true, "ok": 42, "err": null},
            }})
        );
        assert!(matches!(inner_of(&result, "answer"), TaskResult::Ok(value) if value == json!(42)));
    }

    #[test]
    fn a_sub_workflow_dependency_names_its_definition_key() {
        let mut deps = HashMap::new();
        let stored = serde_json::to_string(&TaskResult::Ok(json!({"n": 1}))).unwrap();
        deps.insert(
            1,
            DepResult {
                node_id: Some("child".to_owned()),
                task_name: "__sub_workflow:child".to_owned(),
                sub_definition_key: Some("app.child".to_owned()),
                status: "COMPLETED".to_owned(),
                result: Some(stored),
            },
        );
        let result = merged("{}", &Some(json!({"child": 1})), &deps);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["child"]["source_task_name"], "__sub_workflow:child");
        assert_eq!(parsed["child"]["source_definition_key"], "app.child");
    }

    #[test]
    fn merge_failed_dep_wraps_in_task_result_err() {
        let mut deps = HashMap::new();
        deps.insert(0, dep("FAILED", None));

        let result = merged("{}", &Some(json!({"answer": 0})), &deps);
        let TaskResult::Err(error) = inner_of(&result, "answer") else {
            panic!("a dependency with no result must give an error entry");
        };
        assert!(error.message.unwrap().contains("no stored result"));
    }

    /// Python injects only the results that exist (`core/workflows/engine.py:841`): an index with no node row
    /// gets no kwarg.
    #[test]
    fn a_dependency_with_no_row_gets_no_kwarg() {
        let result = merged("{}", &Some(json!({"answer": 0})), &HashMap::new());
        assert_eq!(result, "{}");
    }

    #[test]
    fn a_dependency_that_is_not_terminal_gives_upstream_skipped() {
        for status in ["PENDING", "READY", "ENQUEUED", "RUNNING"] {
            let mut deps = HashMap::new();
            deps.insert(0, dep(status, None));
            let result = merged("{}", &Some(json!({"answer": 0})), &deps);
            let TaskResult::Err(error) = inner_of(&result, "answer") else {
                panic!("a {status} dependency must give an error entry");
            };
            assert_eq!(error.error_code.to_string(), "UPSTREAM_SKIPPED");
        }
    }

    #[test]
    fn merge_preserves_existing_input_fields() {
        let mut deps = HashMap::new();
        let stored =
            serde_json::to_string(&TaskResult::Ok(serde_json::Value::from("hello"))).unwrap();
        deps.insert(0, dep("COMPLETED", Some(stored)));

        let args_from = Some(json!({"dep_val": 0}));
        let result = merged(r#"{"existing": true}"#, &args_from, &deps);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["existing"], true);
        assert!(
            matches!(inner_of(&result, "dep_val"), TaskResult::Ok(value) if value == json!("hello"))
        );
    }

    #[test]
    fn a_skipped_dependency_gives_upstream_skipped_with_its_index() {
        let mut deps = HashMap::new();
        deps.insert(
            3,
            dep(
                "SKIPPED",
                Some(serde_json::to_string(&TaskResult::Ok(json!(1))).unwrap()),
            ),
        );
        let result = merged("{}", &Some(json!({"answer": 3})), &deps);
        let TaskResult::Err(error) = inner_of(&result, "answer") else {
            panic!("a skipped dependency must give an error entry");
        };
        assert_eq!(error.error_code.to_string(), "UPSTREAM_SKIPPED");
        assert_eq!(
            error.message.as_deref(),
            Some("Upstream dependency was SKIPPED")
        );
        assert_eq!(error.data, Some(json!({"dependency_index": 3})));
    }

    /// The entry of `horsies_workflow_source_value` (0005) and Python `_missing_result_entry`, for a NULL and an
    /// empty stored result.
    #[test]
    fn a_terminal_dependency_with_no_result_gives_result_deserialization_error() {
        for status in ["COMPLETED", "FAILED"] {
            for result in [None, Some(String::new())] {
                let mut deps = HashMap::new();
                deps.insert(2, dep(status, result.clone()));
                let merged = merged("{}", &Some(json!({"answer": 2})), &deps);
                let TaskResult::Err(error) = inner_of(&merged, "answer") else {
                    panic!("a {status} dependency with result {result:?} must give an error entry");
                };
                let expected = format!(
                    "Dependency node source (index 2, task produce) is {status} with no stored result"
                );
                assert_eq!(error.error_code.to_string(), "RESULT_DESERIALIZATION_ERROR");
                assert_eq!(error.message.as_deref(), Some(expected.as_str()));
                assert_eq!(error.data, Some(json!({"dependency_index": 2})));
            }
        }
    }

    /// A node stored without a node id is named `-`, as in the baseline function.
    #[test]
    fn a_dependency_with_no_node_id_is_named_with_a_dash() {
        let error = missing_result_error(4, None, "produce", "COMPLETED");
        assert_eq!(
            error.message.as_deref(),
            Some("Dependency node - (index 4, task produce) is COMPLETED with no stored result")
        );
        assert_eq!(error.data, Some(json!({"dependency_index": 4})));
    }

    /// A child workflow's builder gets the result envelope, not the wrap (Python
    /// `core/workflows/engine.py:1164-1173`).
    #[test]
    fn a_child_builder_reads_the_inner_envelope() {
        let mut deps = HashMap::new();
        let stored = serde_json::to_string(&TaskResult::Ok(json!(3))).unwrap();
        deps.insert(0, dep("COMPLETED", Some(stored)));
        let merged = merged(r#"{"x": 1}"#, &Some(json!({"answer": 0})), &deps);
        let builder_input = child_builder_input(&merged).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&builder_input).unwrap();
        assert_eq!(
            parsed,
            json!({"x": 1, "answer": {"__h_task_result__": true, "ok": 3, "err": null}})
        );
        for unchanged in ["null", "{}", r#"{"x":1}"#, "[1]"] {
            assert_eq!(child_builder_input(unchanged).unwrap(), unchanged);
        }
    }

    #[test]
    fn a_malformed_entry_names_its_kwarg() {
        let mut kwargs = json!({"dep": {"__h_taskresult_envelope__": true, "inner": null}})
            .as_object()
            .unwrap()
            .clone();
        assert_eq!(
            unwrap_args_from_entries(&mut kwargs),
            Err(ArgsFromEntryRefused::MissingSourceTaskName {
                kwarg: "dep".to_owned()
            })
        );
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
        for stored in [
            "{not-json",
            r#"{"__h_task_result__":true,"ok":{"__h_task_result__":true},"err":null}"#,
        ] {
            let mut deps = HashMap::new();
            deps.insert(0, dep("COMPLETED", Some(stored.to_owned())));

            let result = merged("{}", &Some(json!({"answer": 0})), &deps);
            let TaskResult::Err(error) = inner_of(&result, "answer") else {
                panic!("a stored result that does not read must give an error entry");
            };
            assert_eq!(error.error_code.to_string(), "RESULT_DESERIALIZATION_ERROR");
            assert!(error
                .message
                .unwrap()
                .contains("failed to parse upstream task result"));
        }
    }
}
