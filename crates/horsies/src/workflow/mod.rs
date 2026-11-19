//! Workflows: the typed DSL, workflow definitions, the conversion to the
//! core's spec data, the start forms and the workflow handle.
//!
//! The DSL builds a spec as data, in the client: the nodes of registered
//! tasks ([`TaskNode`], from `TaskFunction::node`), sub-workflow nodes
//! ([`SubWorkflowNode`], from a definition), their links, and the output.
//! `WorkflowSpecBuilder::build` checks the spec with Python's rules and
//! codes before anything is written.

mod context;
mod definition;
mod dependency;
mod function;
mod handle;
mod input;
mod node;
mod registry;
mod spec;
mod spec_data;
mod start;
mod sub_workflow;

pub use context::{WorkflowContext, WorkflowContextError};
pub use definition::{WorkflowDefConfig, WorkflowDefinition};
pub use dependency::OutputlessResults;
pub use function::{
    start_workflow, start_workflow_with, WorkflowCheckBuilderRegistration, WorkflowFunction,
    WorkflowTemplate,
};
pub(crate) use function::{start_workflow_as, start_workflow_with_as};
pub use handle::{NodeResult, WorkflowHandle, WorkflowNodeInfo, WorkflowReadError};
pub use node::{InputField, JoinType, NodeKey, NodeRef, TaskNode, TypedNodeRef};
pub use spec::{SuccessCase, SuccessPolicy, WorkflowSpec, WorkflowSpecBuilder};
pub use sub_workflow::{SubWorkflowNode, SubWorkflowParams};

pub(crate) use registry::WorkflowRegistry;

/// Nodes for the tests of other modules of the crate.
#[cfg(test)]
pub(crate) mod tests_support {
    /// A task node of `task_name` with no parameters and the output `u32`.
    pub(crate) fn unit_task_node(task_name: &str) -> crate::TaskNode<u32, ()> {
        super::node::tests::task_node::<u32, ()>(task_name)
    }
}
pub(crate) use start::{new_workflow_id, WorkflowStarter};
// The workflow spec data vectors of the stored forms, through the DSL.
#[cfg(test)]
mod stored_form_vector_tests;
// The stored options text of a node with its deadline, for the stored-form
// vectors (`crate::stored_form_vectors::task_options`).
#[cfg(test)]
pub(crate) use node::NodeDeadline;
#[cfg(test)]
pub(crate) use spec_data::task_options_text;

/// The start errors and the handle errors of the core's catalogs, with
/// Python's codes.
pub use horsies_core::{
    HandleErrorCode, HandleOperationError, HandleResult, StartRefusalCause, WorkflowStartError,
    WorkflowStartErrorCode,
};

/// The workflow types of the core: the error policy, the statuses, the
/// summary of a child workflow and the workflow meta of a task.
pub use horsies_core::{
    OnError, SubWorkflowSummary, WorkflowMeta, WorkflowStatus, WorkflowTaskStatus,
};

/// A JSON value kept as its stored text: the `output` of a
/// [`SubWorkflowSummary`].
pub use horsies_core::JsonText;
