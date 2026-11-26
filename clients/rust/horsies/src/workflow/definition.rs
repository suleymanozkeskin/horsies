//! The workflow definition: a workflow's name, definition key, nodes,
//! output and policies in one place (Python `WorkflowDefinition`).
//!
//! A definition without params implements `define`; the default `build`
//! runs it. A definition with params implements `build_with`, which builds
//! the spec from the params; the engine calls it for each sub-workflow node
//! that gives params, and a start with params calls it. `build_with` may run
//! more than once for one node, so it must have no side effects.
//!
//! Each build gets a builder of the app that registered the definition,
//! named after the definition: its task nodes resolve against that app. A
//! definition without params builds when it registers, so the tasks of its
//! nodes register first.

use horsies_core::OnError;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::node::NodeRef;
use super::spec::{SuccessPolicy, WorkflowSpec, WorkflowSpecBuilder};
use super::sub_workflow::{SubWorkflowNode, SubWorkflowParams};
use crate::HorsiesError;

/// A workflow definition.
///
/// ```ignore
/// struct Checkout;
///
/// impl WorkflowDefinition for Checkout {
///     type Output = Receipt;
///     type Params = ();
///
///     fn name() -> &'static str { "checkout" }
///     fn definition_key() -> &'static str { "shop.checkout.v1" }
///
///     fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
///         let charged = builder.task(charge::node().set(charge::params::order_id(), 7)?)?;
///         let receipt = builder.task(receipt::node().arg_from(receipt::params::charge(), charged))?;
///         Ok(WorkflowDefConfig::new().output(receipt))
///     }
/// }
///
/// let checkout = app.register_workflow_definition::<Checkout>()?;
/// ```
pub trait WorkflowDefinition: 'static {
    /// The type of the workflow's result: the output node's type, or
    /// `OutputlessResults` for a workflow with no output node.
    type Output: 'static;

    /// The params of `build_with`; `()` for a definition without params.
    type Params: Serialize + DeserializeOwned + 'static;

    /// The workflow name, unique in the app.
    fn name() -> &'static str;

    /// The stable identity of the definition, unique in the app: a start
    /// and the engine find the workflow by it.
    fn definition_key() -> &'static str;

    /// What a failed node does to the workflow. Default: FAIL.
    fn on_error() -> OnError {
        OnError::Fail
    }

    /// Add the nodes to `builder`, a builder of the app with the name, the
    /// definition key and `on_error` of the definition, and return the
    /// output and the success policy.
    ///
    /// Errors: the default refuses: a definition implements `define` or
    /// `build_with`.
    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let _unused = builder;
        Err(HorsiesError::new(format!(
            "WorkflowDefinition '{}' implements neither define() nor build_with()",
            Self::name()
        )))
    }

    /// The spec of the definition without params, from `builder`: a
    /// builder of the app named after the definition
    /// (`app.workflow_builder(Self::name())`). Sets the definition key and
    /// `on_error`, runs `define`, then `WorkflowSpecBuilder::build`.
    ///
    /// Errors: the refusal of `define`, of the build, and
    /// `WORKFLOW_OUTPUT_TYPE_MISMATCH` (HRS-025) when the output node's type
    /// is not `Output`.
    fn build(mut builder: WorkflowSpecBuilder) -> Result<WorkflowSpec, HorsiesError> {
        builder.definition_key(Self::definition_key());
        builder.on_error(Self::on_error());
        let config = Self::define(&mut builder)?;
        if let Some(output) = config.output {
            builder.output(output);
        }
        if let Some(policy) = config.success_policy {
            builder.success_policy(policy);
        }
        let spec = builder.build()?;
        spec.check_output_type::<Self::Output>()?;
        Ok(spec)
    }

    /// The spec of the definition with `params`, from `builder`: a builder
    /// of the app named after the definition. The app calls it with such a
    /// builder for each sub-workflow node that gives params and for each
    /// build of the template (`WorkflowTemplate::build`, `start`). The
    /// default ignores the params and returns [`Self::build`]: a definition
    /// with params implements it.
    ///
    /// Errors: the refusals of the build; the app also refuses a built spec
    /// whose output type is not `Output`.
    fn build_with(
        builder: WorkflowSpecBuilder,
        params: Self::Params,
    ) -> Result<WorkflowSpec, HorsiesError> {
        let _unused = params;
        Self::build(builder)
    }

    /// A sub-workflow node of this definition.
    ///
    /// Errors: for `Given(params)`, the refusals of
    /// `SubWorkflowNode::set_params`.
    fn node(
        params: SubWorkflowParams<Self::Params>,
    ) -> Result<SubWorkflowNode<Self::Params, Self::Output>, HorsiesError>
    where
        Self: Sized,
    {
        SubWorkflowNode::of::<Self>(params)
    }
}

/// What `WorkflowDefinition::define` returns besides the nodes.
#[derive(Debug, Clone, Default)]
pub struct WorkflowDefConfig {
    output: Option<NodeRef>,
    success_policy: Option<SuccessPolicy>,
}

impl WorkflowDefConfig {
    /// No output node and no success policy.
    pub fn new() -> Self {
        Self::default()
    }

    /// The node whose result is the workflow's result.
    pub fn output<R: Into<NodeRef>>(mut self, node: R) -> Self {
        self.output = Some(node.into());
        self
    }

    /// When the workflow succeeds.
    pub fn success_policy(mut self, policy: SuccessPolicy) -> Self {
        self.success_policy = Some(policy);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::node::tests::{add, task_node, test_builder, ChargeInput};
    use crate::workflow::spec::{SpecOutput, SpecSuccessPolicy, SuccessCase};
    use crate::workflow::OutputlessResults;
    use crate::HorsiesErrorCode;

    fn charge_node() -> crate::TaskNode<f64, ChargeInput> {
        task_node::<f64, ChargeInput>("charge")
            .set_input(ChargeInput {
                order_id: 1,
                amount: 2.0,
            })
            .unwrap()
    }

    struct Charged;

    impl WorkflowDefinition for Charged {
        type Output = f64;
        type Params = ();

        fn name() -> &'static str {
            "charged"
        }

        fn definition_key() -> &'static str {
            "tests.charged.v1"
        }

        fn on_error() -> OnError {
            OnError::Pause
        }

        fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
            let charged = add(builder, charge_node());
            Ok(WorkflowDefConfig::new().output(charged))
        }
    }

    /// Declares `String` while its output node returns `f64`.
    struct WrongOutput;

    impl WorkflowDefinition for WrongOutput {
        type Output = String;
        type Params = ();

        fn name() -> &'static str {
            "wrong_output"
        }

        fn definition_key() -> &'static str {
            "tests.wrong_output.v1"
        }

        fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
            let charged = add(builder, charge_node());
            Ok(WorkflowDefConfig::new().output(charged))
        }
    }

    struct NoDefine;

    impl WorkflowDefinition for NoDefine {
        type Output = OutputlessResults;
        type Params = ();

        fn name() -> &'static str {
            "no_define"
        }

        fn definition_key() -> &'static str {
            "tests.no_define.v1"
        }
    }

    #[test]
    fn build_runs_define_with_the_definition_facts() {
        let spec = Charged::build(test_builder(Charged::name())).unwrap();
        assert_eq!(spec.name(), "charged");
        assert_eq!(spec.definition_key(), Some("tests.charged.v1"));
        assert_eq!(spec.on_error(), OnError::Pause);
        assert_eq!(spec.node_count(), 1);
    }

    #[test]
    fn a_declared_output_type_that_the_output_node_does_not_have_is_refused() {
        assert_eq!(
            WrongOutput::build(test_builder(WrongOutput::name()))
                .unwrap_err()
                .code,
            Some(HorsiesErrorCode::WorkflowOutputTypeMismatch)
        );
    }

    #[test]
    fn a_definition_without_define_is_refused() {
        assert!(NoDefine::build(test_builder(NoDefine::name()))
            .unwrap_err()
            .message
            .contains("no_define"));
    }

    /// Two nodes of one task, the second the output, with a success policy
    /// that needs the second and takes the first as optional.
    struct WithPolicy;

    impl WorkflowDefinition for WithPolicy {
        type Output = f64;
        type Params = ();

        fn name() -> &'static str {
            "with_policy"
        }

        fn definition_key() -> &'static str {
            "tests.with_policy.v1"
        }

        fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
            let first = add(builder, charge_node().node_id("first"));
            let second = add(builder, charge_node().waits_for(first).node_id("second"));
            let policy = SuccessPolicy::new(vec![SuccessCase::new([second])]).optional([first]);
            Ok(WorkflowDefConfig::new()
                .output(second)
                .success_policy(policy))
        }
    }

    #[test]
    fn the_config_of_define_sets_the_output_and_the_success_policy() {
        let spec = WithPolicy::build(test_builder(WithPolicy::name())).unwrap();
        assert_eq!(spec.node_ids().collect::<Vec<_>>(), vec!["first", "second"]);
        assert_eq!(spec.nodes[1].dependencies, vec![0]);
        assert!(matches!(spec.output, SpecOutput::Node { index: 1, .. }));
        assert_eq!(
            spec.success_policy,
            Some(SpecSuccessPolicy {
                cases: vec![vec![1]],
                optional: vec![0],
            })
        );
    }

    #[test]
    fn node_names_the_definition() {
        let node = Charged::node(SubWorkflowParams::NotGiven).unwrap();
        assert_eq!(node.workflow_name(), "charged");
        assert_eq!(node.definition_key(), "tests.charged.v1");
    }
}
