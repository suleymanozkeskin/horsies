use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Lightweight workflow execution metadata.
///
/// Contains only identifying information about the current task's position
/// within a workflow, without exposing upstream results. This is the Rust
/// equivalent of Python's `WorkflowMetadata`.
///
/// Use this when a task needs to know which workflow it belongs to
/// (e.g., for logging, metrics, or external correlation) but does not
/// need access to upstream task results.
///
/// For full result access, use [`WorkflowContext`] via `workflow_ctx_from`.
///
/// # Example
///
/// ```rust
/// use horsies_core::core::workflow::meta::WorkflowMetadata;
///
/// let meta = WorkflowMetadata {
///     workflow_id: uuid::Uuid::nil(),
///     node_index: 2,
///     task_name: "process_data".to_owned(),
/// };
///
/// assert_eq!(meta.workflow_id, uuid::Uuid::nil());
/// assert_eq!(meta.node_index, 2);
/// assert_eq!(meta.task_name, "process_data");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowMetadata {
    /// UUID of the workflow instance.
    pub workflow_id: Uuid,

    /// Index of the current node within the workflow. The stored key keeps
    /// the schema name `task_index`: the SQL function that writes the
    /// metadata writes that key.
    #[serde(rename = "task_index")]
    pub node_index: i32,

    /// Registered task function name.
    pub task_name: String,
}

impl WorkflowMetadata {
    /// Create a new workflow metadata instance.
    pub fn new(workflow_id: Uuid, node_index: i32, task_name: String) -> Self {
        Self {
            workflow_id,
            node_index,
            task_name,
        }
    }

    /// Extract metadata from a [`WorkflowContext`](super::context::WorkflowContext).
    ///
    /// This is useful when you have a full context but only need the metadata
    /// fields (e.g., for passing to a function that only needs identification).
    pub fn from_context(ctx: &super::context::WorkflowContext) -> Self {
        Self {
            workflow_id: ctx.workflow_id,
            node_index: ctx.node_index,
            task_name: ctx.task_name.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_new() {
        let meta = WorkflowMetadata::new(Uuid::nil(), 3, "fetch".to_owned());
        assert_eq!(meta.workflow_id, Uuid::nil());
        assert_eq!(meta.node_index, 3);
        assert_eq!(meta.task_name, "fetch");
    }

    #[test]
    fn meta_serde_round_trip() {
        let meta = WorkflowMetadata::new(Uuid::nil(), 5, "process".to_owned());
        let json = serde_json::to_string(&meta).unwrap();
        let back: WorkflowMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(back, meta);
    }

    #[test]
    fn meta_from_context() {
        use std::collections::HashMap;
        let ctx = super::super::context::WorkflowContext::new(
            Uuid::nil(),
            7,
            "analyze".to_owned(),
            HashMap::new(),
            HashMap::new(),
        );
        let meta = WorkflowMetadata::from_context(&ctx);
        assert_eq!(meta.workflow_id, Uuid::nil());
        assert_eq!(meta.node_index, 7);
        assert_eq!(meta.task_name, "analyze");
    }
}
