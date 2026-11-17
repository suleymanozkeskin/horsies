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
mod input;
mod node;
mod registry;
mod spec;
mod spec_data;
mod sub_workflow;

pub use context::{WorkflowContext, WorkflowContextError};
pub use definition::{WorkflowDefConfig, WorkflowDefinition};
pub use dependency::OutputlessResults;
pub use node::{InputField, JoinType, NodeKey, NodeRef, TaskNode, TypedNodeRef};
pub use spec::{SuccessCase, SuccessPolicy, WorkflowSpec, WorkflowSpecBuilder};
pub use sub_workflow::{SubWorkflowNode, SubWorkflowParams};

pub(crate) use registry::WorkflowRegistry;

/// The workflow types of the core: the error policy, the statuses, the
/// summary of a child workflow and the workflow meta of a task.
pub use horsies_core::{
    OnError, SubWorkflowSummary, WorkflowMeta, WorkflowStatus, WorkflowTaskStatus,
};
