//! The sub-workflow node: a node whose run is a child workflow (Python
//! `SubWorkflowNode`).
//!
//! The node names the child by its definition. Whether it gives the child
//! parameters is a fact of the node, [`SubWorkflowParameters`]: the core stores
//! `GIVEN` or `NOT_GIVEN` and never reads the choice from the input text.
//! A child whose definition takes no parameters refuses a node that gives some.

use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use serde::Serialize;

use super::definition::WorkflowDefinition;
use super::input::NodeInput;
use super::node::{
    decode_check, input_with_member, whole_input, InputCheck, InputField, JoinType, NodeIdChoice,
    NodeKey, NodeLinks, NodeRef, TypedNodeRef,
};
use crate::{HorsiesError, TaskOutcome};

/// Whether a sub-workflow node gives its child workflow parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubWorkflowParameters<P> {
    /// The node gives none: the child builds as defined. Its stored input is
    /// `{}`, as Python stores a node with no kwargs.
    NotGiven,
    /// The node gives these parameters to the child's `build_with`.
    Given(P),
}

/// The parameters of a sub-workflow node, encoded.
#[derive(Debug, Clone)]
pub(crate) enum NodeParams {
    NotGiven,
    /// The parameters as one object; `args_from` members merge into it.
    Given(NodeInput),
}

/// A workflow node whose run is a child workflow with parameters `P` and output
/// `T`.
///
/// Make one from the child's definition with [`Self::of`] (or from a
/// registered template); add it with `WorkflowSpecBuilder::sub_workflow`.
pub struct SubWorkflowNode<P, T> {
    pub(crate) workflow_name: String,
    pub(crate) definition_key: String,
    pub(crate) params: NodeParams,
    pub(crate) params_check: InputCheck,
    pub(crate) links: NodeLinks,
    types: PhantomData<fn() -> (P, T)>,
}

impl<P, T> std::fmt::Debug for SubWorkflowNode<P, T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SubWorkflowNode")
            .field("workflow_name", &self.workflow_name)
            .field("definition_key", &self.definition_key)
            .field("node_id", &self.links.node_id)
            .field("join", &self.links.join)
            .finish_non_exhaustive()
    }
}

impl<P, T> SubWorkflowNode<P, T>
where
    P: Serialize + DeserializeOwned + 'static,
    T: 'static,
{
    /// A node of the child workflow defined by `D`.
    ///
    /// Errors: for `Given(parameters)`, the refusals of [`Self::set_parameters`].
    pub fn of<D>(parameters: SubWorkflowParameters<P>) -> Result<Self, HorsiesError>
    where
        D: WorkflowDefinition<Parameters = P, Output = T>,
    {
        Self::named(D::name(), D::definition_key()).set_parameters(parameters)
    }

    /// A node of the child named `workflow_name` with `definition_key`, with
    /// no parameters given.
    pub(crate) fn named(workflow_name: &str, definition_key: &str) -> Self {
        Self {
            workflow_name: workflow_name.to_owned(),
            definition_key: definition_key.to_owned(),
            params: NodeParams::NotGiven,
            params_check: decode_check::<P>,
            links: NodeLinks::new(),
            types: PhantomData,
        }
    }

    fn owner(&self) -> String {
        format!("'{}' (sub-workflow)", self.workflow_name)
    }

    /// Give or withdraw the child's parameters. `Given(parameters)` replaces every
    /// parameter set before; `NotGiven` also drops the `args_from` members.
    ///
    /// Errors: `WORKFLOW_KWARGS_NOT_SERIALIZABLE` (HRS-031) for parameters that
    /// do not encode or read back as `P`;
    /// `WORKFLOW_POSITIONAL_ARGS_NOT_SUPPORTED` (HRS-026) for parameters that do
    /// not encode as a JSON object.
    pub fn set_parameters(
        mut self,
        parameters: SubWorkflowParameters<P>,
    ) -> Result<Self, HorsiesError> {
        match parameters {
            SubWorkflowParameters::NotGiven => {
                self.params = NodeParams::NotGiven;
                self.links.args_from.clear();
            }
            SubWorkflowParameters::Given(value) => {
                self.params = NodeParams::Given(whole_input(&self.owner(), &value)?);
            }
        }
        Ok(self)
    }

    /// Give the child one parameter, encoded and read back as a send does. The
    /// node then gives parameters.
    ///
    /// Errors: `WORKFLOW_KWARGS_NOT_SERIALIZABLE` (HRS-031) for a value that
    /// does not encode or read back as `F`.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "a public builder method takes the parameter value by value, as a send does; a \
                  reference would change every caller"
    )]
    pub fn set<F>(mut self, field: InputField<P, F>, value: F) -> Result<Self, HorsiesError>
    where
        F: Serialize + DeserializeOwned,
    {
        let owner = self.owner();
        let input = self.given_input();
        self.params = NodeParams::Given(input_with_member(&owner, input, field.name(), &value)?);
        Ok(self)
    }

    /// Give the child the result of `source` as the parameter `field` (Python
    /// `args_from`). The node then gives parameters and waits for `source`.
    pub fn arg_from<S, E>(
        mut self,
        field: InputField<P, TaskOutcome<S, E>>,
        source: TypedNodeRef<S>,
    ) -> Self {
        self.params = NodeParams::Given(self.given_input());
        self.links.take_result(field.name(), source.untyped());
        self
    }

    /// The parameters given so far, or none.
    fn given_input(&mut self) -> NodeInput {
        match std::mem::replace(&mut self.params, NodeParams::NotGiven) {
            NodeParams::Given(input) => input,
            NodeParams::NotGiven => NodeInput::empty(),
        }
    }

    /// Wait for `dependency`.
    pub fn waits_for<R: Into<NodeRef>>(mut self, dependency: R) -> Self {
        self.links.wait_for(dependency.into());
        self
    }

    /// Wait for each of `dependencies`.
    pub fn waits_for_all<D, R>(mut self, dependencies: D) -> Self
    where
        D: IntoIterator<Item = R>,
        R: Into<NodeRef>,
    {
        for dependency in dependencies {
            self.links.wait_for(dependency.into());
        }
        self
    }

    /// Set the node id, as `TaskNode::node_id`.
    pub fn node_id(mut self, id: impl Into<String>) -> Self {
        self.links.node_id = NodeIdChoice::Given(id.into());
        self
    }

    /// Start the child even when a dependency failed.
    pub fn allow_failed_dependencies(mut self, allow: bool) -> Self {
        self.links.allow_failed_dependencies = allow;
        self
    }

    /// Start when every dependency is terminal (the default).
    pub fn join_all(mut self) -> Self {
        self.links.join = JoinType::All;
        self
    }

    /// Start when one dependency is COMPLETED.
    pub fn join_any(mut self) -> Self {
        self.links.join = JoinType::Any;
        self
    }

    /// Start when `min_success` dependencies are COMPLETED.
    pub fn join_quorum(mut self, min_success: u32) -> Self {
        self.links.join = JoinType::Quorum { min_success };
        self
    }

    /// The key of the node's result, when the node has a given node id.
    pub fn key(&self) -> Option<NodeKey<T>> {
        match &self.links.node_id {
            NodeIdChoice::Given(id) => Some(NodeKey::new(id.clone())),
            NodeIdChoice::Auto => None,
        }
    }

    /// The child's workflow name.
    pub fn workflow_name(&self) -> &str {
        &self.workflow_name
    }

    /// The child's definition key.
    pub fn definition_key(&self) -> &str {
        &self.definition_key
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;
    use crate::workflow::node::BuilderId;
    use crate::ErrorCode;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct RegionParams {
        region: String,
    }

    fn region() -> InputField<RegionParams, String> {
        InputField::raw("region")
    }

    fn node() -> SubWorkflowNode<RegionParams, u32> {
        SubWorkflowNode::named("regional", "tests.regional.v1")
    }

    #[test]
    fn a_new_node_gives_no_params() {
        assert!(matches!(node().params, NodeParams::NotGiven));
    }

    #[test]
    fn given_params_are_stored_as_an_object() {
        let node = node()
            .set_parameters(SubWorkflowParameters::Given(RegionParams {
                region: "eu".to_owned(),
            }))
            .unwrap();
        let NodeParams::Given(input) = &node.params else {
            panic!("the node gives parameters");
        };
        assert_eq!(input.to_text(), r#"{"region":"eu"}"#);
    }

    #[test]
    fn set_and_arg_from_give_params() {
        let set = node().set(region(), "us".to_owned()).unwrap();
        assert!(matches!(set.params, NodeParams::Given(_)));
        let source = TypedNodeRef::<u32>::new(NodeRef::new(BuilderId::next(), 0));
        let field: InputField<RegionParams, TaskOutcome<u32, std::convert::Infallible>> =
            InputField::raw("count");
        let taken = node().arg_from(field, source);
        assert!(matches!(taken.params, NodeParams::Given(_)));
        assert_eq!(taken.links.dependencies, vec![source.untyped()]);
    }

    #[test]
    fn not_given_drops_the_params_and_their_members() {
        let source = TypedNodeRef::<u32>::new(NodeRef::new(BuilderId::next(), 0));
        let field: InputField<RegionParams, TaskOutcome<u32, std::convert::Infallible>> =
            InputField::raw("count");
        let node = node()
            .arg_from(field, source)
            .set_parameters(SubWorkflowParameters::NotGiven)
            .unwrap();
        assert!(matches!(node.params, NodeParams::NotGiven));
        assert!(node.links.args_from.is_empty());
    }

    #[test]
    fn params_that_are_not_an_object_are_refused() {
        let refused = SubWorkflowNode::<u64, u32>::named("count", "tests.count.v1")
            .set_parameters(SubWorkflowParameters::Given(3))
            .unwrap_err();
        assert_eq!(
            refused.code,
            Some(ErrorCode::WorkflowPositionalArgsNotSupported)
        );
    }
}
