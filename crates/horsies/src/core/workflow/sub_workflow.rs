use std::collections::HashMap;
use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::core::task::TaskResult;
use crate::core::workflow::node::{
    merge_kwarg_value, serialize_explicit_input, AnyNode, InputField, JoinType, NodeRef,
    TypedNodeRef,
};
use crate::HorsiesError;

/// Builder for a sub-workflow node within a parent workflow.
///
/// Creates an `AnyNode` with `is_subworkflow: true` and the spec name
/// stored in `task_name` (also available via `sub_workflow_spec_name`).
pub struct SubWorkflowNode<P = (), T = serde_json::Value> {
    spec_name: String,
    definition_key: Option<String>,
    args_json: Option<String>,
    kwargs_json: Option<String>,
    dependencies: Vec<usize>,
    args_from: HashMap<String, usize>,
    workflow_ctx_from_refs: Vec<usize>,
    workflow_ctx_from_ids: Option<Vec<String>>,
    queue: Option<String>,
    priority: Option<i32>,
    allow_failed_deps: bool,
    join: JoinType,
    min_success: Option<i32>,
    good_until: Option<DateTime<Utc>>,
    node_id: Option<String>,
    _phantom: PhantomData<fn() -> (P, T)>,
}

impl SubWorkflowNode<(), serde_json::Value> {
    /// Create a new sub-workflow node for the given registered spec name.
    pub fn new(spec_name: impl Into<String>) -> Self {
        Self::typed(spec_name)
    }
}

impl<P, T> SubWorkflowNode<P, T> {
    /// Create a typed sub-workflow node for the given registered spec name.
    pub fn typed(spec_name: impl Into<String>) -> Self {
        Self {
            spec_name: spec_name.into(),
            definition_key: None,
            args_json: None,
            kwargs_json: None,
            dependencies: Vec::new(),
            args_from: HashMap::new(),
            workflow_ctx_from_refs: Vec::new(),
            workflow_ctx_from_ids: None,
            queue: None,
            priority: None,
            allow_failed_deps: false,
            join: JoinType::All,
            min_success: None,
            good_until: None,
            node_id: None,
            _phantom: PhantomData,
        }
    }

    /// Create a sub-workflow node from a registered workflow definition.
    pub fn from_definition<D>() -> SubWorkflowNode<D::Params, D::Output>
    where
        D: crate::WorkflowDefinition,
    {
        SubWorkflowNode::<D::Params, D::Output> {
            spec_name: D::name().to_owned(),
            definition_key: Some(D::definition_key().to_owned()),
            args_json: None,
            kwargs_json: None,
            dependencies: Vec::new(),
            args_from: HashMap::new(),
            workflow_ctx_from_refs: Vec::new(),
            workflow_ctx_from_ids: None,
            queue: None,
            priority: None,
            allow_failed_deps: false,
            join: JoinType::All,
            min_success: None,
            good_until: None,
            node_id: None,
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
    /// Prefer [`SubWorkflowNode::args_json`] in new code.
    pub fn args(self, args_json: impl Into<String>) -> Self {
        self.args_json(args_json)
    }

    /// Set serialized keyword arguments.
    ///
    /// Prefer [`SubWorkflowNode::kwargs_json`] in new code.
    pub fn kwargs(self, kwargs_json: impl Into<String>) -> Self {
        self.kwargs_json(kwargs_json)
    }

    /// Set the child workflow's entire explicit params value.
    ///
    /// This replaces any prior explicit `.set(...)`, `.args_json(...)`, or
    /// `.kwargs_json(...)` bindings on the sub-workflow node.
    pub fn set_input(mut self, value: P) -> Result<Self, HorsiesError>
    where
        P: Serialize,
    {
        let (args_json, kwargs_json) = serialize_explicit_input(&value)?;
        self.args_json = args_json;
        self.kwargs_json = kwargs_json;
        Ok(self)
    }

    /// Set an explicit field-level child param binding on this sub-workflow node.
    pub fn set<F>(mut self, field: InputField<P, F>, value: F) -> Result<Self, HorsiesError>
    where
        F: Serialize,
    {
        if self.args_json.is_some() {
            return Err(HorsiesError::new(
                "cannot use field-level .set(...) on a sub-workflow node that already has args_json",
            )
            .with_help(
                "use .set_input(...) for whole child params, or keep explicit values in kwargs only",
            ));
        }

        let value = serde_json::to_value(&value).map_err(|e| {
            HorsiesError::new(format!(
                "failed to serialize child workflow field '{}' value: {}",
                field.name(),
                e
            ))
        })?;

        self.kwargs_json = merge_kwarg_value(self.kwargs_json, field.name(), value)?;
        Ok(self)
    }

    /// Add a dependency on another node.
    pub fn waits_for<R>(mut self, dep: R) -> Self
    where
        R: Into<NodeRef>,
    {
        let dep = dep.into();
        if !self.dependencies.contains(&dep.index) {
            self.dependencies.push(dep.index);
        }
        self
    }

    /// Add multiple dependencies.
    pub fn waits_for_all<D, R>(mut self, deps: D) -> Self
    where
        D: IntoIterator<Item = R>,
        R: Into<NodeRef>,
    {
        for dep in deps {
            let dep = dep.into();
            if !self.dependencies.contains(&dep.index) {
                self.dependencies.push(dep.index);
            }
        }
        self
    }

    /// Inject a dependency's `TaskResult<S>` into a typed child workflow param.
    pub fn arg_from<S>(
        mut self,
        field: InputField<P, TaskResult<S>>,
        dep: TypedNodeRef<S>,
    ) -> Self {
        self.args_from.insert(field.name().to_owned(), dep.index);
        if !self.dependencies.contains(&dep.index) {
            self.dependencies.push(dep.index);
        }
        self
    }

    /// Internal raw lowering hook for tests and spec machinery.
    #[allow(dead_code)]
    pub(crate) fn raw_arg_from(mut self, kwarg_name: impl Into<String>, dep: NodeRef) -> Self {
        self.args_from.insert(kwarg_name.into(), dep.index);
        if !self.dependencies.contains(&dep.index) {
            self.dependencies.push(dep.index);
        }
        self
    }

    /// Select upstream nodes whose results should populate WorkflowContext.
    ///
    /// This does not add dependencies automatically; every context source must
    /// still be present in `waits_for(...)` / `waits_for_all(...)`.
    pub fn workflow_ctx_from<D, R>(mut self, deps: D) -> Self
    where
        D: IntoIterator<Item = R>,
        R: Into<NodeRef>,
    {
        for dep in deps {
            let dep = dep.into();
            if !self.workflow_ctx_from_refs.contains(&dep.index) {
                self.workflow_ctx_from_refs.push(dep.index);
            }
        }
        self
    }

    /// Internal raw lowering hook for tests and spec machinery.
    #[allow(dead_code)]
    pub(crate) fn raw_workflow_ctx_from<D>(mut self, node_ids: D) -> Self
    where
        D: IntoIterator<Item = String>,
    {
        self.workflow_ctx_from_refs.clear();
        self.workflow_ctx_from_ids = Some(node_ids.into_iter().collect());
        self
    }

    /// Set a stable node identifier.
    pub fn node_id(mut self, id: impl Into<String>) -> Self {
        self.node_id = Some(id.into());
        self
    }

    /// Override the queue for this sub-workflow's task slot.
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    /// Override the priority.
    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Allow running even if upstream dependencies failed.
    pub fn allow_failed_deps(mut self, allow: bool) -> Self {
        self.allow_failed_deps = allow;
        self
    }

    /// Use `All` join semantics.
    pub fn join_all(mut self) -> Self {
        self.join = JoinType::All;
        self
    }

    /// Use `Any` join semantics.
    pub fn join_any(mut self) -> Self {
        self.join = JoinType::Any;
        self
    }

    /// Use `Quorum` join semantics.
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

    /// The registered workflow spec name.
    pub fn spec_name(&self) -> &str {
        &self.spec_name
    }

    pub(crate) fn definition_key(mut self, key: impl Into<String>) -> Self {
        self.definition_key = Some(key.into());
        self
    }

    /// Convert into a type-erased `AnyNode` with `is_subworkflow: true`.
    pub fn into_any_node(self, index: usize) -> AnyNode {
        AnyNode {
            task_name: format!("__sub_workflow:{}", self.spec_name),
            args_json: self.args_json,
            kwargs_json: self.kwargs_json,
            dependencies: self.dependencies,
            args_from: self.args_from,
            workflow_ctx_from: self.workflow_ctx_from_ids,
            workflow_ctx_from_refs: if self.workflow_ctx_from_refs.is_empty() {
                None
            } else {
                Some(self.workflow_ctx_from_refs)
            },
            queue: self.queue,
            priority: self.priority,
            allow_failed_deps: self.allow_failed_deps,
            join: self.join,
            min_success: self.min_success,
            good_until: self.good_until,
            index,
            node_id: self.node_id,
            task_options_json: None,
            is_subworkflow: true,
            sub_definition_key: self.definition_key,
        }
    }
}

impl<P, T> std::fmt::Debug for SubWorkflowNode<P, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubWorkflowNode")
            .field("spec_name", &self.spec_name)
            .field("node_id", &self.node_id)
            .field("dependencies", &self.dependencies)
            .field("join", &self.join)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TaskResult, WorkflowInput};

    #[test]
    fn sub_workflow_node_defaults() {
        let node = SubWorkflowNode::new("child_wf");
        assert_eq!(node.spec_name(), "child_wf");
        assert!(node.dependencies.is_empty());
        assert_eq!(node.join, JoinType::All);
    }

    #[test]
    fn sub_workflow_into_any_node() {
        let dep = NodeRef { index: 0 };
        let node = SubWorkflowNode::new("child_wf")
            .waits_for(dep)
            .node_id("sub:0")
            .priority(50);

        let any = node.into_any_node(1);
        assert!(any.is_subworkflow);
        assert_eq!(any.task_name, "__sub_workflow:child_wf");
        assert_eq!(any.dependencies, vec![0]);
        assert_eq!(any.node_id.as_deref(), Some("sub:0"));
        assert_eq!(any.priority, Some(50));
        assert_eq!(any.index, 1);
    }

    #[test]
    fn sub_workflow_no_args_kwargs() {
        let node = SubWorkflowNode::new("wf");
        let any = node.into_any_node(0);
        assert!(any.args_json.is_none());
        assert!(any.kwargs_json.is_none());
    }

    #[derive(serde::Serialize)]
    struct ChildParams {
        source: String,
        limit: usize,
    }

    #[derive(serde::Serialize, WorkflowInput)]
    struct MixedParams {
        source: TaskResult<String>,
        limit: usize,
    }

    #[test]
    fn set_input_scalar_serializes_to_args_json() {
        let node = SubWorkflowNode::<String, ()>::typed("child")
            .set_input("eu".to_owned())
            .unwrap()
            .into_any_node(0);

        assert_eq!(node.args_json.as_deref(), Some("\"eu\""));
        assert!(node.kwargs_json.is_none());
    }

    #[test]
    fn set_input_object_serializes_to_kwargs_json() {
        let node = SubWorkflowNode::<ChildParams, ()>::typed("child")
            .set_input(ChildParams {
                source: "eu".to_owned(),
                limit: 10,
            })
            .unwrap()
            .into_any_node(0);

        assert!(node.args_json.is_none());
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(node.kwargs_json.as_deref().unwrap())
                .unwrap(),
            serde_json::json!({"source":"eu","limit":10})
        );
    }

    #[test]
    fn field_level_set_merges_kwargs() {
        let node = SubWorkflowNode::<MixedParams, ()>::typed("child")
            .set(MixedParams::field_limit(), 25)
            .unwrap()
            .into_any_node(0);

        assert!(node.args_json.is_none());
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(node.kwargs_json.as_deref().unwrap())
                .unwrap(),
            serde_json::json!({"limit":25})
        );
    }

    #[test]
    fn field_level_set_rejects_after_args_json() {
        let err = SubWorkflowNode::<MixedParams, ()>::typed("child")
            .args_json("\"not-an-object\"")
            .set(MixedParams::field_limit(), 25)
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("cannot use field-level .set(...) on a sub-workflow node"));
    }
}
