use std::collections::HashMap;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::core::task::error::TaskError;
use crate::core::task::result::TaskResult;
use crate::core::workflow::node::NodeKey;
use crate::core::workflow::summary::SubWorkflowSummary;

/// Reserved kwarg key used to transport workflow context from the engine.
pub const WORKFLOW_CTX_KWARG: &str = "__horsies_workflow_ctx__";

/// Context passed to workflow tasks that need access to upstream results.
///
/// Constructed by the workflow engine and injected into task kwargs when
/// a node declares `workflow_ctx_from`. The struct definition lives here
/// in horsies-core; the injection logic lives in horsies-workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowContext {
    /// ID of the running workflow instance.
    pub workflow_id: String,

    /// Index of the current task within the workflow.
    pub task_index: i32,

    /// Registered task function name.
    pub task_name: String,

    /// Upstream task results keyed by node_id.
    /// Values are TaskResult JSON values (serialized task results).
    #[serde(default)]
    results_by_id: HashMap<String, TaskResult<serde_json::Value>>,

    /// Sub-workflow summaries keyed by node_id.
    #[serde(default)]
    summaries_by_id: HashMap<String, SubWorkflowSummary>,
}

impl WorkflowContext {
    /// Create a new context with pre-populated upstream results.
    pub fn new(
        workflow_id: String,
        task_index: i32,
        task_name: String,
        results_by_id: HashMap<String, TaskResult<serde_json::Value>>,
        summaries_by_id: HashMap<String, SubWorkflowSummary>,
    ) -> Self {
        Self {
            workflow_id,
            task_index,
            task_name,
            results_by_id,
            summaries_by_id,
        }
    }

    /// Retrieve the typed result from an upstream node.
    ///
    /// Deserializes the stored JSON value into `T`. Returns a `TaskError`
    /// if the node_id is not found or deserialization fails.
    pub fn result_for<T: DeserializeOwned>(
        &self,
        key: &NodeKey<T>,
    ) -> Result<TaskResult<T>, TaskError> {
        let value = self.results_by_id.get(key.node_id()).ok_or_else(|| {
            TaskError::builtin(
                crate::core::task::error::ContractCode::WorkflowCtxMissingId,
                format!(
                    "no result found for node_id '{}' in workflow context",
                    key.node_id(),
                ),
            )
        })?;

        match value {
            TaskResult::Ok(ok_value) => {
                let parsed = serde_json::from_value(ok_value.clone()).map_err(|e| {
                    TaskError::builtin(
                        crate::core::task::error::OperationalErrorCode::ResultDeserializationError,
                        format!(
                            "failed to deserialize result for node_id '{}': {}",
                            key.node_id(),
                            e,
                        ),
                    )
                })?;
                Ok(TaskResult::Ok(parsed))
            }
            TaskResult::Err(err) => Ok(TaskResult::Err(err.clone())),
        }
    }

    /// Check whether a result exists for the given node_id.
    pub fn has_result(&self, node_id: &str) -> bool {
        self.results_by_id.contains_key(node_id)
    }

    /// Retrieve the sub-workflow summary for a node.
    pub fn summary_for<T>(&self, key: &NodeKey<T>) -> Result<SubWorkflowSummary, TaskError> {
        let summary = self.summaries_by_id.get(key.node_id()).ok_or_else(|| {
            TaskError::builtin(
                crate::core::task::error::ContractCode::WorkflowCtxMissingId,
                format!(
                    "no summary found for node_id '{}' in workflow context",
                    key.node_id(),
                ),
            )
        })?;
        Ok(summary.clone())
    }

    /// Check whether a summary exists for the given node_id.
    pub fn has_summary(&self, node_id: &str) -> bool {
        self.summaries_by_id.contains_key(node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ctx() -> WorkflowContext {
        let mut results = HashMap::new();
        results.insert(
            "fetch:0".to_owned(),
            TaskResult::Ok(serde_json::json!({"url": "https://example.com", "body": "hello"})),
        );
        results.insert("count:1".to_owned(), TaskResult::Ok(serde_json::json!(42)));
        WorkflowContext::new(
            "wf-123".to_owned(),
            2,
            "process".to_owned(),
            results,
            HashMap::new(),
        )
    }

    #[test]
    fn result_for_deserializes_correctly() {
        let ctx = make_ctx();
        let key: NodeKey<i32> = NodeKey::new("count:1".to_owned());
        let value: TaskResult<i32> = ctx.result_for(&key).unwrap();
        assert_eq!(value.unwrap(), 42);
    }

    #[test]
    fn result_for_struct_deserializes() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct FetchResult {
            url: String,
            body: String,
        }

        let ctx = make_ctx();
        let key: NodeKey<FetchResult> = NodeKey::new("fetch:0".to_owned());
        let value: TaskResult<FetchResult> = ctx.result_for(&key).unwrap();
        let ok = value.unwrap();
        assert_eq!(ok.url, "https://example.com");
        assert_eq!(ok.body, "hello");
    }

    #[test]
    fn result_for_missing_key() {
        let ctx = make_ctx();
        let key: NodeKey<String> = NodeKey::new("nonexistent".to_owned());
        let err = ctx.result_for(&key).unwrap_err();
        assert!(err.message.as_ref().unwrap().contains("nonexistent"));
    }

    #[test]
    fn result_for_type_mismatch() {
        let ctx = make_ctx();
        // count:1 is 42 (integer), try to deserialize as Vec<String>
        let key: NodeKey<Vec<String>> = NodeKey::new("count:1".to_owned());
        let err = ctx.result_for(&key).unwrap_err();
        assert!(err
            .message
            .as_ref()
            .unwrap()
            .contains("failed to deserialize"));
    }

    #[test]
    fn has_result_returns_true_for_existing() {
        let ctx = make_ctx();
        assert!(ctx.has_result("fetch:0"));
        assert!(ctx.has_result("count:1"));
    }

    #[test]
    fn has_result_returns_false_for_missing() {
        let ctx = make_ctx();
        assert!(!ctx.has_result("nope"));
    }

    #[test]
    fn context_serde_round_trip() {
        let ctx = make_ctx();
        let json = serde_json::to_string(&ctx).unwrap();
        let back: WorkflowContext = serde_json::from_str(&json).unwrap();
        assert_eq!(back.workflow_id, "wf-123");
        assert_eq!(back.task_index, 2);
        assert_eq!(back.task_name, "process");
        assert!(back.has_result("fetch:0"));
        assert!(back.has_result("count:1"));
    }

    #[test]
    fn context_empty_results() {
        let ctx = WorkflowContext::new(
            "wf-1".to_owned(),
            0,
            "root".to_owned(),
            HashMap::new(),
            HashMap::new(),
        );
        assert!(!ctx.has_result("anything"));
    }

    // -- Summary tests (ported from Python TestWorkflowContextSummary) --

    fn make_ctx_with_summary() -> WorkflowContext {
        let mut summaries = HashMap::new();
        summaries.insert(
            "sub_wf:0".to_owned(),
            SubWorkflowSummary {
                status: "COMPLETED".to_owned(),
                is_success: true,
                success_case: None,
                output: None,
                total_tasks: 2,
                completed_tasks: 2,
                failed_tasks: 0,
                skipped_tasks: 0,
                error_summary: None,
                child_workflow_id: "child-1".to_owned(),
            },
        );
        WorkflowContext::new(
            "wf-parent".to_owned(),
            1,
            "aggregate".to_owned(),
            HashMap::new(),
            summaries,
        )
    }

    #[test]
    fn summary_for_returns_correct_summary() {
        let ctx = make_ctx_with_summary();
        let key: NodeKey<()> = NodeKey::new("sub_wf:0".to_owned());
        let summary = ctx.summary_for(&key).unwrap();
        assert_eq!(summary.child_workflow_id, "child-1");
        assert_eq!(summary.status, "COMPLETED");
    }

    #[test]
    fn summary_for_missing_key_returns_error() {
        let ctx = make_ctx_with_summary();
        let key: NodeKey<()> = NodeKey::new("nonexistent".to_owned());
        let err = ctx.summary_for(&key).unwrap_err();
        assert!(err.message.as_ref().unwrap().contains("nonexistent"));
    }

    #[test]
    fn has_summary_true() {
        let ctx = make_ctx_with_summary();
        assert!(ctx.has_summary("sub_wf:0"));
    }

    #[test]
    fn has_summary_false_missing() {
        let ctx = make_ctx_with_summary();
        assert!(!ctx.has_summary("nonexistent"));
    }

    #[test]
    fn has_summary_false_empty() {
        let ctx = make_ctx();
        assert!(!ctx.has_summary("anything"));
    }

    // -- Error result variant (ported from Python TestWorkflowContext) --

    #[test]
    fn has_result_true_for_error_variant() {
        let mut results = HashMap::new();
        results.insert(
            "failed:0".to_owned(),
            TaskResult::Err(TaskError::new(
                "CUSTOM_ERR".to_owned(),
                "something broke".to_owned(),
            )),
        );
        let ctx = WorkflowContext::new(
            "wf-1".to_owned(),
            1,
            "next".to_owned(),
            results,
            HashMap::new(),
        );
        assert!(ctx.has_result("failed:0"));
    }

    #[test]
    fn result_for_error_variant_returns_err() {
        let mut results = HashMap::new();
        results.insert(
            "failed:0".to_owned(),
            TaskResult::Err(TaskError::new("CUSTOM_ERR".to_owned(), "broke".to_owned())),
        );
        let ctx = WorkflowContext::new(
            "wf-1".to_owned(),
            1,
            "next".to_owned(),
            results,
            HashMap::new(),
        );
        let key: NodeKey<String> = NodeKey::new("failed:0".to_owned());
        let result = ctx.result_for(&key).unwrap();
        assert!(result.is_err());
    }

    // -- JSON deserialization (ported from Python from_serialized_reconstruction) --

    #[test]
    fn from_json_reconstruction() {
        let json = r#"{
            "workflow_id": "wf-from-json",
            "task_index": 3,
            "task_name": "parse",
            "results_by_id": {
                "fetch:0": {"__type": "ok", "value": 99}
            }
        }"#;
        let ctx: WorkflowContext = serde_json::from_str(json).unwrap();
        assert_eq!(ctx.workflow_id, "wf-from-json");
        assert_eq!(ctx.task_index, 3);
        assert!(ctx.has_result("fetch:0"));
        assert!(!ctx.has_summary("anything"));
    }
}
