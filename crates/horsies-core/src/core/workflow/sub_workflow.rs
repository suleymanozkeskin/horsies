use std::collections::HashMap;
use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::core::task::TaskResult;
use crate::core::workflow::node::{
    merge_kwarg_value, serialize_explicit_input, AnyNode, InputField, JoinType, NodeRef,
    SubWorkflowParams, TypedNodeRef,
};
use crate::HorsiesError;

/// Builder for a sub-workflow node within a parent workflow.
///
/// Creates an `AnyNode` with `is_subworkflow: true` and the spec name of the
/// child as its `task_name`, as Python stores it (the definition name). A
/// sub-workflow node is never claimed as a task, so it has no queue and no
/// priority; its stored row holds the constants `default` and 100.
pub struct SubWorkflowNode<P = (), T = serde_json::Value> {
    spec_name: String,
    definition_key: Option<String>,
    input_json: String,
    params: SubWorkflowParams,
    dependencies: Vec<usize>,
    args_from: HashMap<String, usize>,
    workflow_ctx_from_refs: Vec<usize>,
    workflow_ctx_from_ids: Option<Vec<String>>,
    allow_failed_deps: bool,
    join: JoinType,
    min_success: Option<i32>,
    good_until: Option<DateTime<Utc>>,
    node_id: Option<String>,
    _phantom: PhantomData<fn() -> (P, T)>,
}

impl SubWorkflowNode<(), serde_json::Value> {
    /// Create a new sub-workflow node for the given registered spec name, the
    /// child's encoded params (one JSON value, stored as written) and the
    /// client's choice whether the node gives params.
    pub fn new(
        spec_name: impl Into<String>,
        input_json: impl Into<String>,
        params: SubWorkflowParams,
    ) -> Self {
        Self::typed(spec_name, input_json, params)
    }
}

impl<P, T> SubWorkflowNode<P, T> {
    /// Create a typed sub-workflow node for the given registered spec name,
    /// the child's encoded params (one JSON value, stored as written) and the
    /// client's choice whether the node gives params.
    ///
    /// The text is required in both cases. With
    /// [`SubWorkflowParams::NotGiven`] it is `{}`, Python's stored form of no
    /// params. A node that sets fields later with `.set(...)` starts from an
    /// object such as `{}`. `.input_json`,
    /// `.set_input`, `.set` and `.arg_from` give params: they set
    /// [`SubWorkflowParams::Given`].
    pub fn typed(
        spec_name: impl Into<String>,
        input_json: impl Into<String>,
        params: SubWorkflowParams,
    ) -> Self {
        Self {
            spec_name: spec_name.into(),
            definition_key: None,
            input_json: input_json.into(),
            params,
            dependencies: Vec::new(),
            args_from: HashMap::new(),
            workflow_ctx_from_refs: Vec::new(),
            workflow_ctx_from_ids: None,
            allow_failed_deps: false,
            join: JoinType::All,
            min_success: None,
            good_until: None,
            node_id: None,
            _phantom: PhantomData,
        }
    }

    /// Create a sub-workflow node from a registered workflow definition, the
    /// child's encoded params (one JSON value, stored as written) and the
    /// client's choice whether the node gives params.
    pub fn from_definition<D>(
        input_json: impl Into<String>,
        params: SubWorkflowParams,
    ) -> SubWorkflowNode<D::Params, D::Output>
    where
        D: crate::WorkflowDefinition,
    {
        SubWorkflowNode::<D::Params, D::Output> {
            spec_name: D::name().to_owned(),
            definition_key: Some(D::definition_key().to_owned()),
            input_json: input_json.into(),
            params,
            dependencies: Vec::new(),
            args_from: HashMap::new(),
            workflow_ctx_from_refs: Vec::new(),
            workflow_ctx_from_ids: None,
            allow_failed_deps: false,
            join: JoinType::All,
            min_success: None,
            good_until: None,
            node_id: None,
            _phantom: PhantomData,
        }
    }

    /// Give the child workflow's encoded params as one JSON value, stored as
    /// written. Sets [`SubWorkflowParams::Given`].
    pub fn input_json(mut self, input_json: impl Into<String>) -> Self {
        self.input_json = input_json.into();
        self.params = SubWorkflowParams::Given;
        self
    }

    /// Set the child workflow's entire explicit params value.
    ///
    /// This replaces any prior explicit `.set(...)` or `.input_json(...)`
    /// binding on the sub-workflow node. Sets [`SubWorkflowParams::Given`].
    pub fn set_input(mut self, value: P) -> Result<Self, HorsiesError>
    where
        P: Serialize,
    {
        self.input_json = serialize_explicit_input(&value)?;
        self.params = SubWorkflowParams::Given;
        Ok(self)
    }

    /// Set an explicit field-level child param binding on this sub-workflow
    /// node. Sets [`SubWorkflowParams::Given`].
    pub fn set<F>(mut self, field: InputField<P, F>, value: F) -> Result<Self, HorsiesError>
    where
        F: Serialize,
    {
        if !matches!(
            serde_json::from_str::<serde_json::Value>(&self.input_json),
            Ok(serde_json::Value::Object(_))
        ) {
            return Err(HorsiesError::new(
                "cannot use field-level .set(...) on a sub-workflow node whose params are not a JSON object",
            )
            .with_help(
                "start the node from an object such as {}, or use .set_input(...) for whole child params",
            ));
        }

        let value = serde_json::to_value(&value).map_err(|e| {
            HorsiesError::new(format!(
                "failed to serialize child workflow field '{}' value: {}",
                field.name(),
                e
            ))
        })?;

        self.input_json = merge_kwarg_value(&self.input_json, field.name(), value)?;
        self.params = SubWorkflowParams::Given;
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
    /// Sets [`SubWorkflowParams::Given`].
    pub fn arg_from<S>(
        mut self,
        field: InputField<P, TaskResult<S>>,
        dep: TypedNodeRef<S>,
    ) -> Self {
        self.params = SubWorkflowParams::Given;
        self.args_from.insert(field.name().to_owned(), dep.index);
        if !self.dependencies.contains(&dep.index) {
            self.dependencies.push(dep.index);
        }
        self
    }

    /// Internal raw lowering hook for tests and spec machinery.
    #[allow(dead_code)]
    pub(crate) fn raw_arg_from(mut self, kwarg_name: impl Into<String>, dep: NodeRef) -> Self {
        self.params = SubWorkflowParams::Given;
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

    #[cfg(test)]
    pub(crate) fn definition_key(mut self, key: impl Into<String>) -> Self {
        self.definition_key = Some(key.into());
        self
    }

    /// Convert into a type-erased `AnyNode` with `is_subworkflow: true`.
    pub fn into_any_node(self, index: usize) -> AnyNode {
        AnyNode {
            task_name: self.spec_name,
            input_json: self.input_json,
            dependencies: self.dependencies,
            args_from: self.args_from,
            workflow_ctx_from: self.workflow_ctx_from_ids,
            workflow_ctx_from_refs: if self.workflow_ctx_from_refs.is_empty() {
                None
            } else {
                Some(self.workflow_ctx_from_refs)
            },
            queue: None,
            priority: None,
            allow_failed_deps: self.allow_failed_deps,
            join: self.join,
            min_success: self.min_success,
            good_until: self.good_until,
            index,
            node_id: self.node_id,
            task_options_json: None,
            is_subworkflow: true,
            sub_definition_key: self.definition_key,
            sub_workflow_params: self.params,
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
        let node = SubWorkflowNode::new("child_wf", "null", SubWorkflowParams::NotGiven);
        assert_eq!(node.spec_name(), "child_wf");
        assert!(node.dependencies.is_empty());
        assert_eq!(node.join, JoinType::All);
    }

    #[test]
    fn sub_workflow_into_any_node() {
        let dep = NodeRef { index: 0 };
        let node = SubWorkflowNode::new("child_wf", "null", SubWorkflowParams::NotGiven)
            .waits_for(dep)
            .node_id("sub:0");

        let any = node.into_any_node(1);
        assert!(any.is_subworkflow);
        assert_eq!(any.task_name, "child_wf");
        assert_eq!(any.dependencies, vec![0]);
        assert_eq!(any.node_id.as_deref(), Some("sub:0"));
        assert_eq!(any.queue, None);
        assert_eq!(any.priority, None);
        assert_eq!(any.index, 1);
    }

    #[test]
    fn sub_workflow_node_stores_its_params_as_given() {
        // The core never supplies default params: `null` stays `null`, `{}`
        // stays `{}`, and the client's choice is kept as set.
        let unit = SubWorkflowNode::new("wf", "null", SubWorkflowParams::NotGiven).into_any_node(0);
        assert_eq!(unit.input_json, "null");
        assert_eq!(unit.sub_workflow_params, SubWorkflowParams::NotGiven);
        let empty_object =
            SubWorkflowNode::new("wf", "{}", SubWorkflowParams::Given).into_any_node(0);
        assert_eq!(empty_object.input_json, "{}");
        assert_eq!(empty_object.sub_workflow_params, SubWorkflowParams::Given);
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
    fn set_input_scalar_serializes_to_input_json() {
        let node =
            SubWorkflowNode::<String, ()>::typed("child", "null", SubWorkflowParams::NotGiven)
                .set_input("eu".to_owned())
                .unwrap()
                .into_any_node(0);

        assert_eq!(node.input_json, "\"eu\"");
    }

    #[test]
    fn set_input_object_serializes_to_input_json() {
        let node =
            SubWorkflowNode::<ChildParams, ()>::typed("child", "null", SubWorkflowParams::NotGiven)
                .set_input(ChildParams {
                    source: "eu".to_owned(),
                    limit: 10,
                })
                .unwrap()
                .into_any_node(0);

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&node.input_json).unwrap(),
            serde_json::json!({"source":"eu","limit":10})
        );
    }

    #[test]
    fn field_level_set_merges_kwargs() {
        let node =
            SubWorkflowNode::<MixedParams, ()>::typed("child", "{}", SubWorkflowParams::NotGiven)
                .set(MixedParams::field_limit(), 25)
                .unwrap()
                .into_any_node(0);

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&node.input_json).unwrap(),
            serde_json::json!({"limit":25})
        );
    }

    #[test]
    fn giving_params_sets_given() {
        // `.input_json`, `.set_input`, `.set` and `.arg_from` give params.
        let not_given = || {
            SubWorkflowNode::<MixedParams, ()>::typed("child", "{}", SubWorkflowParams::NotGiven)
        };
        let by_text = not_given().input_json(r#"{"limit":1}"#).into_any_node(1);
        assert_eq!(by_text.sub_workflow_params, SubWorkflowParams::Given);
        let by_field = not_given()
            .set(MixedParams::field_limit(), 25)
            .unwrap()
            .into_any_node(1);
        assert_eq!(by_field.sub_workflow_params, SubWorkflowParams::Given);
        let by_member = not_given()
            .arg_from(MixedParams::field_source(), TypedNodeRef::<String>::new(0))
            .into_any_node(1);
        assert_eq!(by_member.sub_workflow_params, SubWorkflowParams::Given);
        let by_value =
            SubWorkflowNode::<String, ()>::typed("child", "null", SubWorkflowParams::NotGiven)
                .set_input("eu".to_owned())
                .unwrap()
                .into_any_node(1);
        assert_eq!(by_value.sub_workflow_params, SubWorkflowParams::Given);
    }

    #[test]
    fn field_level_set_rejects_a_null_input() {
        // `null` is not replaced by `{}`: a node that takes fields starts from
        // an object.
        let err =
            SubWorkflowNode::<MixedParams, ()>::typed("child", "null", SubWorkflowParams::NotGiven)
                .set(MixedParams::field_limit(), 25)
                .unwrap_err();

        assert!(err
            .to_string()
            .contains("cannot use field-level .set(...) on a sub-workflow node"));
    }

    #[test]
    fn field_level_set_rejects_after_a_non_object_input() {
        let err =
            SubWorkflowNode::<MixedParams, ()>::typed("child", "{}", SubWorkflowParams::NotGiven)
                .input_json("\"not-an-object\"")
                .set(MixedParams::field_limit(), 25)
                .unwrap_err();

        assert!(err
            .to_string()
            .contains("cannot use field-level .set(...) on a sub-workflow node"));
    }
}
