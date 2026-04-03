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
/// Created via `TaskNode::key()` after a `node_id` has been assigned.
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

/// Reference to a node within a `WorkflowSpecBuilder`.
///
/// Returned by `WorkflowSpecBuilder::task()`. Used to wire up
/// dependencies, args_from, and output selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeRef {
    /// Zero-based index into `WorkflowSpec.tasks`.
    pub index: usize,
}

/// Type-erased node stored in `WorkflowSpec.tasks`.
///
/// Contains all fields needed for validation, DB insertion, and engine
/// execution. Created from `TaskNode<T>` during builder finalization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnyNode {
    /// Registered task function name.
    pub task_name: String,

    /// Serialized positional arguments (JSON).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args_json: Option<String>,

    /// Serialized keyword arguments (JSON).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kwargs_json: Option<String>,

    /// Indices of upstream dependencies in the spec's task list.
    pub dependencies: Vec<usize>,

    /// Map of kwarg_name -> dependency index for result injection.
    pub args_from: HashMap<String, usize>,

    /// Node IDs of dependencies whose results should populate WorkflowContext.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_ctx_from: Option<Vec<String>>,

    /// Queue override (None = use default).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,

    /// Priority override (None = use default).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,

    /// If true, run even when upstream dependencies have failed.
    pub allow_failed_deps: bool,

    /// Join semantics for dependency resolution.
    pub join: JoinType,

    /// Minimum successful dependencies required for `JoinType::Quorum`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_success: Option<i32>,

    /// Task expiry deadline.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub good_until: Option<DateTime<Utc>>,

    /// Zero-based position in the spec's task list. Set during builder finalization.
    pub index: usize,

    /// Stable node identifier. Auto-assigned if not set explicitly.
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
}

/// Typed builder for a workflow task node.
///
/// The phantom type `T` encodes the task's result type for compile-time
/// safety when wiring up `args_from` or accessing results via `NodeKey`.
///
/// Use chainable setters to configure the node, then pass it to
/// `WorkflowSpecBuilder::task()`.
pub struct TaskNode<T> {
    task_name: String,
    args_json: Option<String>,
    kwargs_json: Option<String>,
    dependencies: Vec<usize>,
    args_from: HashMap<String, usize>,
    workflow_ctx_from: Option<Vec<String>>,
    queue: Option<String>,
    priority: Option<i32>,
    allow_failed_deps: bool,
    join: JoinType,
    min_success: Option<i32>,
    good_until: Option<DateTime<Utc>>,
    node_id: Option<String>,
    task_options_json: Option<String>,
    _phantom: PhantomData<T>,
}

impl<T> std::fmt::Debug for TaskNode<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskNode")
            .field("task_name", &self.task_name)
            .field("node_id", &self.node_id)
            .field("dependencies", &self.dependencies)
            .field("join", &self.join)
            .finish()
    }
}

impl<T> TaskNode<T> {
    /// Create a new task node for the given registered task name.
    pub fn new(task_name: impl Into<String>) -> Self {
        Self {
            task_name: task_name.into(),
            args_json: None,
            kwargs_json: None,
            dependencies: Vec::new(),
            args_from: HashMap::new(),
            workflow_ctx_from: None,
            queue: None,
            priority: None,
            allow_failed_deps: false,
            join: JoinType::All,
            min_success: None,
            good_until: None,
            node_id: None,
            task_options_json: None,
            _phantom: PhantomData,
        }
    }

    /// Set serialized positional arguments.
    pub fn args_json(mut self, args_json: impl Into<String>) -> Self {
        self.args_json = Some(args_json.into());
        self
    }

    /// Set serialized keyword arguments.
    pub fn kwargs_json(mut self, kwargs_json: impl Into<String>) -> Self {
        self.kwargs_json = Some(kwargs_json.into());
        self
    }

    /// Set serialized positional arguments.
    ///
    /// Prefer [`TaskNode::args_json`] in new code.
    pub fn args(self, args_json: impl Into<String>) -> Self {
        self.args_json(args_json)
    }

    /// Set serialized keyword arguments.
    ///
    /// Prefer [`TaskNode::kwargs_json`] in new code.
    pub fn kwargs(self, kwargs_json: impl Into<String>) -> Self {
        self.kwargs_json(kwargs_json)
    }

    /// Add a dependency on another node (by its `NodeRef`).
    pub fn waits_for(mut self, dep: NodeRef) -> Self {
        if !self.dependencies.contains(&dep.index) {
            self.dependencies.push(dep.index);
        }
        self
    }

    /// Add multiple dependencies at once.
    pub fn waits_for_all(mut self, deps: &[NodeRef]) -> Self {
        for dep in deps {
            if !self.dependencies.contains(&dep.index) {
                self.dependencies.push(dep.index);
            }
        }
        self
    }

    /// Inject a dependency's result as a keyword argument.
    ///
    /// The dependency is also added to `waits_for` automatically.
    pub fn args_from(mut self, kwarg_name: impl Into<String>, dep: NodeRef) -> Self {
        self.args_from.insert(kwarg_name.into(), dep.index);
        if !self.dependencies.contains(&dep.index) {
            self.dependencies.push(dep.index);
        }
        self
    }

    /// Set node IDs whose results should be available in WorkflowContext.
    pub fn workflow_ctx_from(mut self, node_ids: Vec<String>) -> Self {
        self.workflow_ctx_from = Some(node_ids);
        self
    }

    /// Set a stable node identifier.
    pub fn node_id(mut self, id: impl Into<String>) -> Self {
        self.node_id = Some(id.into());
        self
    }

    /// Override the queue for this task.
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    /// Override the priority for this task.
    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Allow this task to run even if upstream dependencies failed.
    pub fn allow_failed_deps(mut self, allow: bool) -> Self {
        self.allow_failed_deps = allow;
        self
    }

    /// Use `All` join semantics (wait for all deps).
    pub fn join_all(mut self) -> Self {
        self.join = JoinType::All;
        self
    }

    /// Use `Any` join semantics (run when any dep succeeds).
    pub fn join_any(mut self) -> Self {
        self.join = JoinType::Any;
        self
    }

    /// Use `Quorum` join semantics (run when `min` deps succeed).
    pub fn join_quorum(mut self, min: i32) -> Self {
        self.join = JoinType::Quorum;
        self.min_success = Some(min);
        self
    }

    /// Set task expiry deadline.
    pub fn good_until(mut self, deadline: DateTime<Utc>) -> Self {
        self.good_until = Some(deadline);
        self
    }

    /// Set serialized task options JSON.
    pub fn task_options(mut self, options_json: impl Into<String>) -> Self {
        self.task_options_json = Some(options_json.into());
        self
    }

    /// Get a typed key for this node's result.
    ///
    /// Only valid after `node_id` has been set (either explicitly or
    /// by auto-assignment during `WorkflowSpecBuilder::build()`).
    ///
    /// Returns `None` if `node_id` is not yet set.
    pub fn key(&self) -> Option<NodeKey<T>> {
        self.node_id.as_ref().map(|id| NodeKey::new(id.clone()))
    }

    /// The registered task function name.
    pub fn task_name(&self) -> &str {
        &self.task_name
    }

    /// Convert this typed node into a type-erased `AnyNode`.
    ///
    /// Called internally by `WorkflowSpecBuilder::task()`.
    pub fn into_any_node(self, index: usize) -> AnyNode {
        AnyNode {
            task_name: self.task_name,
            args_json: self.args_json,
            kwargs_json: self.kwargs_json,
            dependencies: self.dependencies,
            args_from: self.args_from,
            workflow_ctx_from: self.workflow_ctx_from,
            queue: self.queue,
            priority: self.priority,
            allow_failed_deps: self.allow_failed_deps,
            join: self.join,
            min_success: self.min_success,
            good_until: self.good_until,
            index,
            node_id: self.node_id,
            task_options_json: self.task_options_json,
            is_subworkflow: false,
            sub_definition_key: None,
        }
    }
}

/// Resolve task options from a registry lookup into workflow nodes.
///
/// For each non-subworkflow node where `task_options_json` is `None`, calls
/// `lookup(task_name)` to get the serialized retry options JSON from the task
/// registry. If found, sets `task_options_json` on the node.
///
/// Nodes that already have `task_options_json` set are left untouched
/// (all-or-nothing: no deep merge of individual fields).
///
/// The `lookup` closure should return `Some(json_string)` with the
/// serialized retry-relevant options, or `None` if the task has no options.
pub fn resolve_node_task_options(nodes: &mut [AnyNode], lookup: &dyn Fn(&str) -> Option<String>) {
    for node in nodes.iter_mut() {
        if node.is_subworkflow {
            continue;
        }
        if node.task_options_json.is_some() {
            continue;
        }
        if let Some(options_json) = lookup(&node.task_name) {
            node.task_options_json = Some(options_json);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn node_key_clone_eq_hash() {
        let k1: NodeKey<i32> = NodeKey::new("x".to_owned());
        let k2 = k1.clone();
        assert_eq!(k1, k2);

        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(k1);
        assert!(set.contains(&k2));
    }

    #[test]
    fn node_ref_copy() {
        let r = NodeRef { index: 3 };
        let r2 = r;
        assert_eq!(r, r2);
    }

    #[test]
    fn task_node_builder_defaults() {
        let node: TaskNode<String> = TaskNode::new("my_task");
        assert_eq!(node.task_name(), "my_task");
        assert!(node.args_json.is_none());
        assert!(node.kwargs_json.is_none());
        assert!(node.dependencies.is_empty());
        assert!(node.args_from.is_empty());
        assert!(node.workflow_ctx_from.is_none());
        assert!(node.queue.is_none());
        assert!(node.priority.is_none());
        assert!(!node.allow_failed_deps);
        assert_eq!(node.join, JoinType::All);
        assert!(node.min_success.is_none());
        assert!(node.good_until.is_none());
        assert!(node.node_id.is_none());
        assert!(node.task_options_json.is_none());
        assert!(node.key().is_none());
    }

    #[test]
    fn task_node_chainable_setters() {
        let node: TaskNode<i32> = TaskNode::new("add")
            .args_json(r#"[1, 2]"#)
            .kwargs_json(r#"{"mode": "fast"}"#)
            .node_id("add_step")
            .queue("high")
            .priority(10)
            .allow_failed_deps(true)
            .join_quorum(2);

        assert_eq!(node.args_json.as_deref(), Some(r#"[1, 2]"#));
        assert_eq!(node.kwargs_json.as_deref(), Some(r#"{"mode": "fast"}"#));
        assert_eq!(node.node_id.as_deref(), Some("add_step"));
        assert_eq!(node.queue.as_deref(), Some("high"));
        assert_eq!(node.priority, Some(10));
        assert!(node.allow_failed_deps);
        assert_eq!(node.join, JoinType::Quorum);
        assert_eq!(node.min_success, Some(2));
    }

    #[test]
    fn task_node_key_after_node_id() {
        let node: TaskNode<Vec<u8>> = TaskNode::new("compress").node_id("compress:0");
        let key = node.key().expect("key should be Some after node_id set");
        assert_eq!(key.node_id(), "compress:0");
    }

    #[test]
    fn task_node_waits_for_deduplicates() {
        let dep = NodeRef { index: 0 };
        let node: TaskNode<()> = TaskNode::new("t").waits_for(dep).waits_for(dep);
        assert_eq!(node.dependencies, vec![0]);
    }

    #[test]
    fn task_node_waits_for_all() {
        let d0 = NodeRef { index: 0 };
        let d1 = NodeRef { index: 1 };
        let d2 = NodeRef { index: 2 };
        let node: TaskNode<()> = TaskNode::new("t").waits_for_all(&[d0, d1, d2]);
        assert_eq!(node.dependencies, vec![0, 1, 2]);
    }

    #[test]
    fn task_node_args_from_adds_dependency() {
        let dep = NodeRef { index: 5 };
        let node: TaskNode<()> = TaskNode::new("t").args_from("raw_data", dep);
        assert_eq!(node.dependencies, vec![5]);
        assert_eq!(node.args_from.get("raw_data"), Some(&5));
    }

    #[test]
    fn task_node_into_any_node() {
        let dep = NodeRef { index: 0 };
        let node: TaskNode<String> = TaskNode::new("process")
            .args_json(r#""hello""#)
            .waits_for(dep)
            .args_from("input", dep)
            .node_id("process:1")
            .queue("default")
            .priority(100)
            .allow_failed_deps(true)
            .join_any();

        let any = node.into_any_node(1);
        assert_eq!(any.task_name, "process");
        assert_eq!(any.args_json.as_deref(), Some(r#""hello""#));
        assert_eq!(any.dependencies, vec![0]);
        assert_eq!(any.args_from.get("input"), Some(&0));
        assert_eq!(any.node_id.as_deref(), Some("process:1"));
        assert_eq!(any.queue.as_deref(), Some("default"));
        assert_eq!(any.priority, Some(100));
        assert!(any.allow_failed_deps);
        assert_eq!(any.join, JoinType::Any);
        assert_eq!(any.index, 1);
        assert!(!any.is_subworkflow);
    }

    #[test]
    fn task_node_compatibility_setters_delegate_to_json_variants() {
        let node: TaskNode<String> = TaskNode::new("process")
            .args(r#""hello""#)
            .kwargs(r#"{"mode":"fast"}"#);

        assert_eq!(node.args_json.as_deref(), Some(r#""hello""#));
        assert_eq!(node.kwargs_json.as_deref(), Some(r#"{"mode":"fast"}"#));
    }

    #[test]
    fn any_node_serde_round_trip() {
        let any = AnyNode {
            task_name: "fetch".to_owned(),
            args_json: Some(r#"{"url": "https://example.com"}"#.to_owned()),
            kwargs_json: None,
            dependencies: vec![],
            args_from: HashMap::new(),
            workflow_ctx_from: None,
            queue: None,
            priority: None,
            allow_failed_deps: false,
            join: JoinType::All,
            min_success: None,
            good_until: None,
            index: 0,
            node_id: Some("fetch:0".to_owned()),
            task_options_json: None,
            is_subworkflow: false,
            sub_definition_key: None,
        };

        let json = serde_json::to_string(&any).unwrap();
        let back: AnyNode = serde_json::from_str(&json).unwrap();
        assert_eq!(back.task_name, "fetch");
        assert_eq!(back.node_id.as_deref(), Some("fetch:0"));
        assert_eq!(back.join, JoinType::All);
        assert!(!back.is_subworkflow);
    }

    #[test]
    fn join_all_and_any_setters() {
        let node: TaskNode<()> = TaskNode::new("t").join_any().join_all();
        assert_eq!(node.join, JoinType::All);
        assert!(node.min_success.is_none());
    }

    // -----------------------------------------------------------------------
    // resolve_node_task_options tests
    // -----------------------------------------------------------------------

    fn make_any_node(task_name: &str, is_subworkflow: bool) -> AnyNode {
        AnyNode {
            task_name: task_name.to_owned(),
            args_json: None,
            kwargs_json: None,
            dependencies: vec![],
            args_from: HashMap::new(),
            workflow_ctx_from: None,
            queue: None,
            priority: None,
            allow_failed_deps: false,
            join: JoinType::All,
            min_success: None,
            good_until: None,
            index: 0,
            node_id: None,
            task_options_json: None,
            is_subworkflow,
            sub_definition_key: None,
        }
    }

    #[test]
    fn resolve_fills_missing_options() {
        let mut nodes = vec![make_any_node("task_a", false)];
        let lookup = |name: &str| -> Option<String> {
            if name == "task_a" {
                Some(r#"{"retry_policy":{"max_retries":3}}"#.to_owned())
            } else {
                None
            }
        };
        resolve_node_task_options(&mut nodes, &lookup);
        assert_eq!(
            nodes[0].task_options_json.as_deref(),
            Some(r#"{"retry_policy":{"max_retries":3}}"#),
        );
    }

    #[test]
    fn resolve_preserves_explicit_override() {
        let mut nodes = vec![make_any_node("task_a", false)];
        nodes[0].task_options_json = Some(r#"{"custom":"value"}"#.to_owned());

        let lookup = |_name: &str| -> Option<String> {
            Some(r#"{"retry_policy":{"max_retries":3}}"#.to_owned())
        };
        resolve_node_task_options(&mut nodes, &lookup);
        // Original value preserved, not overwritten.
        assert_eq!(
            nodes[0].task_options_json.as_deref(),
            Some(r#"{"custom":"value"}"#),
        );
    }

    #[test]
    fn resolve_skips_subworkflow_nodes() {
        let mut nodes = vec![make_any_node("__sub_workflow:child", true)];
        let lookup = |_name: &str| -> Option<String> {
            Some(r#"{"retry_policy":{"max_retries":3}}"#.to_owned())
        };
        resolve_node_task_options(&mut nodes, &lookup);
        assert!(nodes[0].task_options_json.is_none());
    }

    #[test]
    fn resolve_skips_unregistered_tasks() {
        let mut nodes = vec![make_any_node("unknown_task", false)];
        let lookup = |_name: &str| -> Option<String> { None };
        resolve_node_task_options(&mut nodes, &lookup);
        assert!(nodes[0].task_options_json.is_none());
    }
}
