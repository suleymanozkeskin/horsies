use std::collections::HashMap;
use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Dependency join semantics for workflow tasks.
///
/// Controls when a dependent task becomes ready based on its
/// upstream dependencies' statuses.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    /// Wait for all dependencies to reach a terminal state.
    #[default]
    All,
    /// Run as soon as any single dependency completes successfully.
    Any,
    /// Run when at least `min_success` dependencies complete successfully.
    Quorum,
}

impl std::fmt::Display for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::All => write!(f, "all"),
            Self::Any => write!(f, "any"),
            Self::Quorum => write!(f, "quorum"),
        }
    }
}

/// Typed key for accessing a workflow task's result.
///
/// The phantom type `T` encodes the expected result type at compile time.
#[derive(Debug)]
pub struct NodeKey<T> {
    node_id: String,
    _phantom: PhantomData<T>,
}

impl<T> NodeKey<T> {
    /// Create a new typed node key.
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            _phantom: PhantomData,
        }
    }

    /// The stable node identifier.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

impl<T> Clone for NodeKey<T> {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T> PartialEq for NodeKey<T> {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id
    }
}

impl<T> Eq for NodeKey<T> {}

impl<T> std::hash::Hash for NodeKey<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.node_id.hash(state);
    }
}

/// Whether a sub-workflow node gives its child workflow params. The client
/// sets it when it defines the node; the core branches on it and never reads
/// the choice from the input text.
///
/// The node's input text (`AnyNode::input_json`) is required in both cases:
/// the node row stores it, and a child definition with params receives it. For
/// `NotGiven` it is `{}` from every client, Python's stored form of no
/// params.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubWorkflowParameters {
    /// The node gives no params. A child definition without params takes
    /// such a node.
    NotGiven,
    /// The node gives params: a static input, `args_from` members, or both.
    /// A child definition without params refuses such a node.
    Given,
}

impl SubWorkflowParameters {
    /// The value that a sub-workflow node row stores in
    /// `horsies_workflow_tasks.sub_workflow_params`: `NOT_GIVEN` or `GIVEN`.
    /// A task node row stores `NULL`.
    pub fn as_stored(self) -> &'static str {
        match self {
            Self::NotGiven => "NOT_GIVEN",
            Self::Given => "GIVEN",
        }
    }

    /// The choice of a stored value; `None` for any other text.
    pub fn from_stored(value: &str) -> Option<Self> {
        match value {
            "NOT_GIVEN" => Some(Self::NotGiven),
            "GIVEN" => Some(Self::Given),
            _ => None,
        }
    }
}

/// Type-erased node stored in `WorkflowSpec.tasks`.
///
/// Contains all fields needed for validation, DB insertion, and engine
/// execution. Built from a client's spec data
/// ([`crate::parse_workflow_spec_data`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnyNode {
    /// Registered task function name.
    pub task_name: String,

    /// The encoded input of the node's task as one JSON value, as the client
    /// encoded it. Every node has one; the core never supplies a default.
    ///
    /// A node that sets no fields carries the client's encoding of an empty
    /// input: `{}` from Python (no keyword arguments), `null` from a Rust
    /// `()` input. The node row stores the text as written, and the task
    /// decodes it as stored. `args_from` and workflow context members merge
    /// into it at enqueue, so a node with members has an object input.
    pub input_json: String,

    /// Indices of upstream dependencies in the spec's task list.
    pub dependencies: Vec<usize>,

    /// Map of kwarg_name -> dependency index for result injection.
    pub args_from: HashMap<String, usize>,

    /// Node IDs of dependencies whose results should populate WorkflowContext.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_context_from: Option<Vec<String>>,

    /// Queue override (None = use default).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,

    /// Priority override (None = use default).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,

    /// If true, run even when upstream dependencies have failed.
    pub allow_failed_dependencies: bool,

    /// Join semantics for dependency resolution.
    pub join: JoinType,

    /// Minimum successful dependencies required for `JoinType::Quorum`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_success: Option<i32>,

    /// Task expiry deadline.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub good_until: Option<DateTime<Utc>>,

    /// Zero-based position in the spec's task list.
    pub index: usize,

    /// Stable node identifier, as the client gave it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,

    /// Serialized TaskOptions JSON.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_options_json: Option<String>,

    /// Whether this node represents a sub-workflow. Always false in core scope.
    pub is_subworkflow: bool,

    /// Stable definition key for sub-workflow runtime resolution.
    /// Set when the sub-workflow's spec has a `definition_key`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_definition_key: Option<String>,

    /// Whether a sub-workflow node gives its child params, as the client set
    /// it. A task node has no child, so it gives none: `NotGiven`.
    pub sub_workflow_params: SubWorkflowParameters,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sub_workflow_params_round_trip_through_the_stored_value() {
        for params in [
            SubWorkflowParameters::NotGiven,
            SubWorkflowParameters::Given,
        ] {
            assert_eq!(
                SubWorkflowParameters::from_stored(params.as_stored()),
                Some(params)
            );
        }
        assert_eq!(
            SubWorkflowParameters::as_stored(SubWorkflowParameters::NotGiven),
            "NOT_GIVEN"
        );
        assert_eq!(
            SubWorkflowParameters::as_stored(SubWorkflowParameters::Given),
            "GIVEN"
        );
        for refused in ["", "not_given", "NotGiven", "GIVEN "] {
            assert_eq!(
                SubWorkflowParameters::from_stored(refused),
                None,
                "{refused:?}"
            );
        }
    }

    #[test]
    fn join_type_serde() {
        let jt = JoinType::Quorum;
        let json = serde_json::to_string(&jt).unwrap();
        assert_eq!(json, "\"quorum\"");
        let back: JoinType = serde_json::from_str(&json).unwrap();
        assert_eq!(back, jt);
    }

    #[test]
    fn join_type_default_is_all() {
        assert_eq!(JoinType::default(), JoinType::All);
    }

    #[test]
    fn join_type_display() {
        assert_eq!(JoinType::All.to_string(), "all");
        assert_eq!(JoinType::Any.to_string(), "any");
        assert_eq!(JoinType::Quorum.to_string(), "quorum");
    }

    #[test]
    fn node_key_typed() {
        let key_i32: NodeKey<i32> = NodeKey::new("step_a".to_owned());
        let key_str: NodeKey<String> = NodeKey::new("step_b".to_owned());
        assert_eq!(key_i32.node_id(), "step_a");
        assert_eq!(key_str.node_id(), "step_b");
    }

    #[test]
    fn any_node_serde_round_trip() {
        let any = AnyNode {
            task_name: "fetch".to_owned(),
            input_json: r#"{"url": "https://example.com"}"#.to_owned(),
            dependencies: vec![],
            args_from: HashMap::new(),
            workflow_context_from: None,
            queue: None,
            priority: None,
            allow_failed_dependencies: false,
            join: JoinType::All,
            min_success: None,
            good_until: None,
            index: 0,
            node_id: Some("fetch:0".to_owned()),
            task_options_json: None,
            is_subworkflow: false,
            sub_definition_key: None,
            sub_workflow_params: SubWorkflowParameters::NotGiven,
        };

        let json = serde_json::to_string(&any).unwrap();
        let back: AnyNode = serde_json::from_str(&json).unwrap();
        assert_eq!(back.task_name, "fetch");
        assert_eq!(back.input_json, r#"{"url": "https://example.com"}"#);
        assert_eq!(back.sub_workflow_params, SubWorkflowParameters::NotGiven);
        assert_eq!(back.node_id.as_deref(), Some("fetch:0"));
        assert_eq!(back.join, JoinType::All);
        assert!(!back.is_subworkflow);
    }
}
