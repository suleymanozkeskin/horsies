use crate::core::workflow::node::AnyNode;
use crate::core::workflow::policy::SuccessPolicy;
use crate::core::workflow::status::OnError;

/// The most nodes that one workflow spec holds. A larger workflow splits its nodes into sub-workflows; each
/// child workflow has its own limit.
pub const MAX_WORKFLOW_NODES: usize = 10_000;

/// A workflow spec, as the core starts it: the facts that the workflow row
/// and the node rows store.
///
/// A client builds and checks the spec with its own workflow API, then
/// sends it as spec data ([`crate::parse_workflow_spec_data`]).
#[derive(Debug, Clone)]
pub struct WorkflowSpec {
    /// Workflow name.
    pub name: String,

    /// Stable definition identity for persistence and runtime lookup.
    /// Separate from `name` so the human-readable name can change without
    /// breaking DB identity. Can also serve as a version key in the future.
    pub definition_key: Option<String>,

    /// Ordered list of type-erased task nodes.
    pub tasks: Vec<AnyNode>,

    /// Error handling policy.
    pub on_error: OnError,

    /// Index of the task whose result becomes the workflow output.
    pub output_index: Option<usize>,

    /// Custom success criteria.
    pub success_policy: Option<SuccessPolicy>,
}
