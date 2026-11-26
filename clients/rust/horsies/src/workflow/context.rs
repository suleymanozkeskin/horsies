//! The workflow context: the results of chosen dependencies, given to a
//! task that takes it (Python `WorkflowContext`).
//!
//! A node names the dependencies with `workflow_ctx_from`, and its task
//! takes the context as the input field `workflow_ctx` (the task attribute's
//! `workflow_ctx`). The child of the worker writes the context into that
//! field; its form is the core's `WorkflowContext`.

use std::collections::BTreeMap;

use horsies_core::{SubWorkflowSummary, TaskResult};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use uuid::Uuid;

use super::node::NodeKey;
use crate::handle::read_stored_result;
use crate::task::body::value_free_decode_text;
use crate::{TaskError, TaskOutcome, UnknownUserCode, UserErrorCode};

/// The context of one workflow task: its place in the workflow, and the
/// results and sub-workflow summaries of the dependencies that its node
/// names in `workflow_ctx_from`, by node id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowContext {
    workflow_id: Uuid,
    /// The index of the node. The stored key keeps the schema name
    /// `task_index`: the SQL function that writes the context writes it.
    #[serde(rename = "task_index")]
    node_index: i32,
    task_name: String,
    /// The stored result envelope of each context node, by node id.
    #[serde(default)]
    results_by_id: BTreeMap<String, StoredResultText>,
    /// The summary of each context sub-workflow node, by node id.
    #[serde(default)]
    summaries_by_id: BTreeMap<String, SubWorkflowSummary>,
}

/// The stored result envelope of one context node, as the text it was
/// written with. [`WorkflowContext::result_for`] decodes the value from its
/// own text, so an integer outside the `i64` and `u64` range reads as the
/// asked type (`u128`, `i128`), as Python reads any integer. Two texts are
/// equal when they are the same text.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
struct StoredResultText(Box<RawValue>);

impl PartialEq for StoredResultText {
    fn eq(&self, other: &Self) -> bool {
        self.0.get() == other.0.get()
    }
}

/// Why a context read gave no value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WorkflowContextError {
    /// The context holds no entry for the node: the node is not in the
    /// task node's `workflow_ctx_from`, or the dependency did not end
    /// (Python `KeyError`; the core's `WORKFLOW_CTX_MISSING_ID`).
    #[error("node '{node_id}' is not in the workflow context; name it in workflow_ctx_from")]
    NotInContext { node_id: String },
    /// The stored value does not read as the asked type
    /// (`RESULT_DESERIALIZATION_ERROR`). `cause` names no value.
    #[error("the stored value of node '{node_id}' does not read: {cause}")]
    Unreadable { node_id: String, cause: String },
    /// The stored error has a user code that the asked code type does not
    /// name.
    #[error("the stored error of node '{node_id}': {cause}")]
    UnknownUserCode {
        node_id: String,
        cause: UnknownUserCode,
    },
}

impl WorkflowContext {
    /// The workflow of the task.
    pub fn workflow_id(&self) -> Uuid {
        self.workflow_id
    }

    /// The index of the task's node in its workflow.
    pub fn node_index(&self) -> i32 {
        self.node_index
    }

    /// The task name of the node.
    pub fn task_name(&self) -> &str {
        &self.task_name
    }

    /// The outcome of the context node `key`, read with its output type `T`
    /// and the code type `E` (Python `result_for`).
    ///
    /// Errors: [`WorkflowContextError`].
    pub fn result_for<T, E>(
        &self,
        key: &NodeKey<T>,
    ) -> Result<TaskOutcome<T, E>, WorkflowContextError>
    where
        T: DeserializeOwned,
        E: UserErrorCode,
    {
        let node_id = key.node_id();
        let stored =
            self.results_by_id
                .get(node_id)
                .ok_or_else(|| WorkflowContextError::NotInContext {
                    node_id: node_id.to_owned(),
                })?;
        let read = read_stored_result::<T>(stored.0.get()).map_err(|refused| {
            WorkflowContextError::Unreadable {
                node_id: node_id.to_owned(),
                cause: refused.cause,
            }
        })?;
        match read {
            TaskResult::Ok(value) => Ok(TaskOutcome::Succeeded(value)),
            TaskResult::Err(stored) => TaskError::<E>::from_stored(stored)
                .map(TaskOutcome::Failed)
                .map_err(|cause| WorkflowContextError::UnknownUserCode {
                    node_id: node_id.to_owned(),
                    cause,
                }),
        }
    }

    /// Whether the context holds the result of the node `node_id`.
    pub fn has_result(&self, node_id: &str) -> bool {
        self.results_by_id.contains_key(node_id)
    }

    /// The summary of the context sub-workflow node `key`, its output as
    /// stored (Python `summary_for`).
    ///
    /// Errors: [`WorkflowContextError::NotInContext`].
    pub fn summary_for<T>(
        &self,
        key: &NodeKey<T>,
    ) -> Result<&SubWorkflowSummary, WorkflowContextError> {
        self.summaries_by_id
            .get(key.node_id())
            .ok_or_else(|| WorkflowContextError::NotInContext {
                node_id: key.node_id().to_owned(),
            })
    }

    /// The output of the context sub-workflow node `key`, read with the
    /// child's output type `T`: `None` when the child stored no output. The
    /// output decodes from its stored text, so an integer outside the `i64`
    /// and `u64` range reads as `u128` or `i128`, as Python reads any integer.
    ///
    /// Errors: [`WorkflowContextError::NotInContext`];
    /// [`WorkflowContextError::Unreadable`] for an output that does not read
    /// as `T`.
    pub fn output_for<T: DeserializeOwned>(
        &self,
        key: &NodeKey<T>,
    ) -> Result<Option<T>, WorkflowContextError> {
        let summary = self.summary_for(key)?;
        match &summary.output {
            None => Ok(None),
            Some(output) => {
                output
                    .decode::<T>()
                    .map(Some)
                    .map_err(|error| WorkflowContextError::Unreadable {
                        node_id: key.node_id().to_owned(),
                        cause: value_free_decode_text(&error),
                    })
            }
        }
    }

    /// Whether the context holds the summary of the node `node_id`.
    pub fn has_summary(&self, node_id: &str) -> bool {
        self.summaries_by_id.contains_key(node_id)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use serde_json::json;

    use super::*;
    use crate::task::body::decode_input;

    fn context() -> WorkflowContext {
        serde_json::from_value(json!({
            "workflow_id": "00000000-0000-0000-0000-000000000007",
            "task_index": 2,
            "task_name": "report",
            "results_by_id": {
                "fetch": {"__h_task_result__": true, "ok": [1, 2], "err": null},
                "check": {"__h_task_result__": true, "ok": null,
                          "err": {"error_code": "DECLINED", "message": "no"}},
            },
            "summaries_by_id": {},
        }))
        .unwrap()
    }

    #[test]
    fn a_result_reads_with_its_types() {
        let context = context();
        assert_eq!(context.node_index(), 2);
        let fetched = context
            .result_for::<Vec<u32>, Infallible>(&NodeKey::new("fetch"))
            .unwrap();
        assert_eq!(fetched, TaskOutcome::Succeeded(vec![1, 2]));
        let checked = context
            .result_for::<u32, horsies_core::UserCode>(&NodeKey::new("check"))
            .unwrap();
        assert!(matches!(checked, TaskOutcome::Failed(_)));
    }

    /// The value decodes from its own text, in a context that is a field of
    /// a task input read by the input decode (with its field path tracker):
    /// an integer outside the `i64` and `u64` range reads as `u128` or
    /// `i128`, as Python reads it. A read through `serde_json::Value` holds
    /// it as an `f64`, which both refuse.
    #[test]
    fn a_stored_integer_outside_i64_and_u64_reads_as_its_type() {
        #[derive(Deserialize)]
        struct Input {
            workflow_ctx: WorkflowContext,
        }
        let text = concat!(
            r#"{"workflow_ctx":{"workflow_id":"00000000-0000-0000-0000-000000000007","#,
            r#""task_index":2,"task_name":"report","results_by_id":{"#,
            r#""max":{"__h_task_result__":true,"ok":340282366920938463463374607431768211455,"err":null},"#,
            r#""below":{"__h_task_result__":true,"ok":-9223372036854775809,"err":null}},"#,
            r#""summaries_by_id":{}}}"#,
        );
        let context = decode_input::<Input>(text.as_bytes()).unwrap().workflow_ctx;
        assert_eq!(
            context.result_for::<u128, Infallible>(&NodeKey::new("max")),
            Ok(TaskOutcome::Succeeded(u128::MAX))
        );
        assert_eq!(
            context.result_for::<i128, Infallible>(&NodeKey::new("below")),
            Ok(TaskOutcome::Succeeded(i128::from(i64::MIN) - 1))
        );
    }

    /// The output of a sub-workflow summary decodes from its stored text, in
    /// a context read by the input decode: an integer outside the `i64` and
    /// `u64` range reads as `u128` or `i128` (#1862).
    #[test]
    fn a_summary_output_outside_i64_and_u64_reads_as_its_type() {
        #[derive(Deserialize)]
        struct Input {
            workflow_ctx: WorkflowContext,
        }
        let summary = |output: &str| {
            format!(
                r#"{{"status":"COMPLETED","output":{output},"total_nodes":1,"completed_nodes":1,"failed_nodes":0,"skipped_nodes":0,"error_summary":null}}"#
            )
        };
        let text = format!(
            r#"{{"workflow_ctx":{{"workflow_id":"00000000-0000-0000-0000-000000000007","task_index":2,"task_name":"report","results_by_id":{{}},"summaries_by_id":{{"max":{},"below":{}}}}}}}"#,
            summary("340282366920938463463374607431768211455"),
            summary("-9223372036854775809"),
        );
        let context = decode_input::<Input>(text.as_bytes()).unwrap().workflow_ctx;
        assert_eq!(
            context.output_for(&NodeKey::<u128>::new("max")),
            Ok(Some(u128::MAX))
        );
        assert_eq!(
            context.output_for(&NodeKey::<i128>::new("below")),
            Ok(Some(i128::from(i64::MIN) - 1))
        );
        assert_eq!(
            context
                .summary_for(&NodeKey::<u128>::new("max"))
                .unwrap()
                .output
                .as_ref()
                .map(horsies_core::JsonText::get),
            Some("340282366920938463463374607431768211455")
        );
        assert!(matches!(
            context.output_for(&NodeKey::<u64>::new("max")),
            Err(WorkflowContextError::Unreadable { .. })
        ));
    }

    #[test]
    fn each_failure_of_a_read_has_its_variant() {
        let context = context();
        assert_eq!(
            context.result_for::<u32, Infallible>(&NodeKey::new("absent")),
            Err(WorkflowContextError::NotInContext {
                node_id: "absent".to_owned()
            })
        );
        assert!(matches!(
            context.result_for::<String, Infallible>(&NodeKey::new("fetch")),
            Err(WorkflowContextError::Unreadable { .. })
        ));
        assert!(matches!(
            context.result_for::<u32, Infallible>(&NodeKey::new("check")),
            Err(WorkflowContextError::UnknownUserCode { .. })
        ));
        assert!(matches!(
            context.summary_for(&NodeKey::<u32>::new("fetch")),
            Err(WorkflowContextError::NotInContext { .. })
        ));
    }
}
