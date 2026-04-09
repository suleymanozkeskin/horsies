use crate::core::error::HorsiesError;
use crate::core::registry::workflow::RegisteredWorkflowSpec;
use crate::core::workflow::node::NodeRef;
use crate::core::workflow::policy::SuccessPolicy;
use crate::core::workflow::spec::{WorkflowSpec, WorkflowSpecBuilder};
use crate::core::workflow::status::OnError;

// ---------------------------------------------------------------------------
// WorkflowDefinition trait
// ---------------------------------------------------------------------------

/// A declarative, single-site workflow definition.
///
/// Provides a class-like pattern (analogous to Python's `WorkflowDefinition`)
/// where all aspects of a workflow — name, nodes, edges, output, error policy,
/// and success policy — are specified in one place by implementing this trait.
///
/// `build_with()` is the primary entry point. For static workflows (`Params = ()`),
/// override `define()` as a convenience — the default `build_with()` will call it.
/// For parameterized/dynamic workflows, override `build_with()` directly.
///
/// # Static workflow example
///
/// ```ignore
/// use horsies::{
///     WorkflowDefinition, WorkflowDefConfig, WorkflowSpecBuilder,
///     OnError, HorsiesError,
/// };
///
/// struct ScrapeWorkflow;
///
/// impl WorkflowDefinition for ScrapeWorkflow {
///     type Output = ();
///     type Params = ();
///
///     fn name() -> &'static str { "scrape_pipeline" }
///     fn definition_key() -> &'static str { "scrape_pipeline" }
///
///     fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
///         let fetch = builder.task(fetch_listing::node()?.node_id("fetch"));
///         let parse = builder.task(
///             parse_listing::node()?.arg_from(ParseInput::field_raw(), fetch).node_id("parse"),
///         );
///         let persist = builder.task(
///             persist_listing::node()?.arg_from(PersistInput::field_data(), parse).node_id("persist"),
///         );
///         Ok(WorkflowDefConfig::new().output(persist))
///     }
/// }
/// ```
///
/// # Parameterized workflow example
///
/// ```ignore
/// struct EnrichmentPipeline;
///
/// impl WorkflowDefinition for EnrichmentPipeline {
///     type Output = ();
///     type Params = EnrichmentParams;
///
///     fn name() -> &'static str { "enrich_auction" }
///     fn definition_key() -> &'static str { "schmart.enrich_auction.v1" }
///
///     fn build_with(params: EnrichmentParams) -> Result<WorkflowSpec, HorsiesError> {
///         let mut builder = WorkflowSpecBuilder::new("enrich_auction");
///         builder.definition_key("schmart.enrich_auction.v1");
///         // ... build dynamic DAG from params using typed node helpers ...
///         builder.build()
///     }
/// }
/// ```
pub trait WorkflowDefinition {
    /// The workflow output type.
    type Output;

    /// Runtime params used to build a parameterized workflow definition.
    ///
    /// Use `()` for workflows with no runtime params.
    type Params;

    /// The workflow name. Must be unique within the application.
    fn name() -> &'static str;

    /// Stable definition identity for persistence and runtime lookup.
    /// Separate from `name()` — the human-readable name can change without
    /// breaking DB identity. Can also serve as a version key in the future.
    fn definition_key() -> &'static str;

    /// Error handling policy. Defaults to `OnError::Fail`.
    fn on_error() -> OnError {
        OnError::Fail
    }

    /// Define the workflow's nodes, edges, and configuration.
    ///
    /// Receives a mutable `WorkflowSpecBuilder` pre-configured with the
    /// workflow name and error policy. Override this for static workflows
    /// (`Params = ()`). The default `build_with()` calls this method.
    ///
    /// For parameterized workflows, override `build_with()` directly
    /// instead — there is no need to implement `define()`.
    fn define(_builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        Err(HorsiesError::new(format!(
            "WorkflowDefinition '{}' must override either define() or build_with()",
            Self::name(),
        )))
    }

    /// Build a validated `WorkflowSpec` from this definition.
    ///
    /// Creates a `WorkflowSpecBuilder`, calls `define()`, applies the
    /// returned configuration, and validates through all phases.
    fn build() -> Result<WorkflowSpec, HorsiesError> {
        let mut builder = WorkflowSpecBuilder::new(Self::name());
        builder.definition_key(Self::definition_key());
        builder.on_error(Self::on_error());

        let config = Self::define(&mut builder)?;

        if let Some(output_ref) = config.output {
            builder.output(output_ref);
        }
        if let Some(policy) = config.success_policy {
            builder.success_policy(policy);
        }

        builder.build()
    }

    /// Build a validated `RegisteredWorkflowSpec` from this definition.
    fn build_registered() -> Result<RegisteredWorkflowSpec, HorsiesError> {
        let mut builder = WorkflowSpecBuilder::new(Self::name());
        builder.definition_key(Self::definition_key());
        builder.on_error(Self::on_error());

        let config = Self::define(&mut builder)?;

        if let Some(output_ref) = config.output {
            builder.output(output_ref);
        }
        if let Some(policy) = config.success_policy {
            builder.success_policy(policy);
        }

        builder.build_registered()
    }

    /// Build a validated `WorkflowSpec` with runtime parameters.
    ///
    /// Override this for parameterized/dynamic workflows. Use generated
    /// typed node helpers (`task_name::node_with(input)?`) to construct
    /// nodes with compile-time input type enforcement.
    ///
    /// Default implementation ignores params and forwards to `build()`,
    /// which calls `define()`.
    fn build_with(params: Self::Params) -> Result<WorkflowSpec, HorsiesError> {
        let _ = params;
        Self::build()
    }
}

// ---------------------------------------------------------------------------
// WorkflowDefConfig -- returned from define()
// ---------------------------------------------------------------------------

/// Configuration returned by [`WorkflowDefinition::define()`].
///
/// Carries the optional output node reference and success policy that the
/// trait's default `build()` / `build_registered()` methods apply to the
/// builder after `define()` completes.
#[derive(Debug, Default)]
pub struct WorkflowDefConfig {
    /// The node whose result becomes the workflow output. `None` means no
    /// output (the workflow completes without producing a typed result).
    pub output: Option<NodeRef>,

    /// Custom success criteria. `None` means use the default policy (all
    /// non-optional tasks must complete).
    pub success_policy: Option<SuccessPolicy>,
}

impl WorkflowDefConfig {
    /// Create a new config with no output and no success policy.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the output node.
    pub fn output<R>(mut self, node_ref: R) -> Self
    where
        R: Into<NodeRef>,
    {
        self.output = Some(node_ref.into());
        self
    }

    /// Set a custom success policy.
    pub fn success_policy(mut self, policy: SuccessPolicy) -> Self {
        self.success_policy = Some(policy);
        self
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::workflow::node::TaskNode;
    use crate::core::workflow::policy::{SuccessCase, SuccessPolicy};
    use crate::core::workflow::sub_workflow::SubWorkflowNode;

    // -----------------------------------------------------------------------
    // WorkflowDefinition trait tests
    // -----------------------------------------------------------------------

    struct SimpleWorkflow;

    impl WorkflowDefinition for SimpleWorkflow {
        type Output = ();
        type Params = ();

        fn name() -> &'static str {
            "simple_pipeline"
        }
        fn definition_key() -> &'static str {
            "simple_pipeline"
        }

        fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
            let fetch = builder.task(TaskNode::<String>::raw("fetch").node_id("fetch"));
            let process = builder.task(
                TaskNode::<String>::raw("process")
                    .raw_arg_from("raw", fetch.into())
                    .node_id("process"),
            );
            let persist = builder.task(
                TaskNode::<()>::raw("persist")
                    .raw_arg_from("data", process.into())
                    .node_id("persist"),
            );

            Ok(WorkflowDefConfig::new().output(persist))
        }
    }

    #[test]
    fn trait_build_simple() {
        let spec = SimpleWorkflow::build().unwrap();
        assert_eq!(spec.name, "simple_pipeline");
        assert_eq!(spec.tasks.len(), 3);
        assert_eq!(spec.output_index, Some(2));
        assert_eq!(spec.on_error, OnError::Fail);
        assert!(spec.success_policy.is_none());
    }

    #[test]
    fn trait_build_registered() {
        let registered = SimpleWorkflow::build_registered().unwrap();
        assert_eq!(registered.spec.name, "simple_pipeline");
        assert_eq!(registered.spec.tasks.len(), 3);
    }

    struct ParamWorkflow;

    impl WorkflowDefinition for ParamWorkflow {
        type Output = String;
        type Params = String;

        fn name() -> &'static str {
            "param_pipeline"
        }

        fn definition_key() -> &'static str {
            "param_pipeline"
        }

        fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
            let node = builder.task(TaskNode::<String>::raw("fetch").node_id("fetch"));
            Ok(WorkflowDefConfig::new().output(node))
        }

        fn build_with(params: Self::Params) -> Result<WorkflowSpec, HorsiesError> {
            let mut builder = WorkflowSpecBuilder::new(format!("param_pipeline_{}", params));
            builder.definition_key(format!("param_pipeline_{}", params));
            let node = builder.task(
                TaskNode::<String>::raw("fetch")
                    .node_id("fetch")
                    .args_json(serde_json::to_string(&params).unwrap()),
            );
            builder.output(node);
            builder.build()
        }
    }

    #[test]
    fn trait_build_with_typed_params() {
        let spec = ParamWorkflow::build_with("eu".to_owned()).unwrap();
        assert_eq!(spec.name, "param_pipeline_eu");
        assert_eq!(spec.definition_key.as_deref(), Some("param_pipeline_eu"));
        assert_eq!(spec.tasks[0].args_json.as_deref(), Some("\"eu\""));
    }

    struct CustomErrorWorkflow;

    impl WorkflowDefinition for CustomErrorWorkflow {
        type Output = ();
        type Params = ();

        fn name() -> &'static str {
            "pause_on_error"
        }
        fn definition_key() -> &'static str {
            "pause_on_error"
        }

        fn on_error() -> OnError {
            OnError::Pause
        }

        fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
            builder.task(TaskNode::<()>::raw("step_a").node_id("step_a"));
            builder.task(TaskNode::<()>::raw("step_b").node_id("step_b"));
            Ok(WorkflowDefConfig::new())
        }
    }

    #[test]
    fn trait_custom_on_error() {
        let spec = CustomErrorWorkflow::build().unwrap();
        assert_eq!(spec.on_error, OnError::Pause);
        assert!(spec.output_index.is_none());
    }

    struct PolicyWorkflow;

    impl WorkflowDefinition for PolicyWorkflow {
        type Output = ();
        type Params = ();

        fn name() -> &'static str {
            "policy_wf"
        }
        fn definition_key() -> &'static str {
            "policy_wf"
        }

        fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
            let a = builder.task(TaskNode::<()>::raw("task_a").node_id("task_a"));
            let b = builder.task(TaskNode::<()>::raw("task_b").node_id("task_b"));
            builder.task(TaskNode::<()>::raw("task_c").node_id("task_c"));

            let policy = SuccessPolicy {
                cases: vec![SuccessCase {
                    required_indices: vec![a.index, b.index],
                    name: None,
                }],
                optional_indices: Some(vec![2]),
            };

            Ok(WorkflowDefConfig::new().output(b).success_policy(policy))
        }
    }

    #[test]
    fn trait_with_success_policy() {
        let spec = PolicyWorkflow::build().unwrap();
        assert_eq!(spec.name, "policy_wf");
        assert_eq!(spec.output_index, Some(1));
        let policy = spec.success_policy.as_ref().unwrap();
        assert_eq!(policy.cases.len(), 1);
        assert_eq!(policy.cases[0].required_indices, vec![0, 1]);
        assert_eq!(policy.optional_indices, Some(vec![2]));
    }

    struct SubWorkflowDef;

    impl WorkflowDefinition for SubWorkflowDef {
        type Output = ();
        type Params = ();

        fn name() -> &'static str {
            "parent_wf"
        }
        fn definition_key() -> &'static str {
            "parent_wf"
        }

        fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
            let prep = builder.task(TaskNode::<()>::raw("prepare").node_id("prepare"));
            let child = builder.sub_workflow(
                SubWorkflowNode::new("child_pipeline")
                    .waits_for(prep)
                    .node_id("child"),
            );
            let finalize = builder.task(
                TaskNode::<()>::raw("finalize")
                    .waits_for(child)
                    .node_id("finalize"),
            );
            Ok(WorkflowDefConfig::new().output(finalize))
        }
    }

    #[test]
    fn trait_with_sub_workflow() {
        let spec = SubWorkflowDef::build().unwrap();
        assert_eq!(spec.name, "parent_wf");
        assert_eq!(spec.tasks.len(), 3);
        assert!(spec.tasks[1].is_subworkflow);
        assert_eq!(spec.output_index, Some(2));
    }

    struct DiamondWorkflow;

    impl WorkflowDefinition for DiamondWorkflow {
        type Output = ();
        type Params = ();

        fn name() -> &'static str {
            "diamond"
        }
        fn definition_key() -> &'static str {
            "diamond"
        }

        fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
            let root = builder.task(TaskNode::<()>::raw("root").node_id("root"));
            let left = builder.task(TaskNode::<()>::raw("left").waits_for(root).node_id("left"));
            let right = builder.task(
                TaskNode::<()>::raw("right")
                    .waits_for(root)
                    .node_id("right"),
            );
            let join = builder.task(
                TaskNode::<()>::raw("join")
                    .waits_for_all(&[left, right])
                    .node_id("join"),
            );
            Ok(WorkflowDefConfig::new().output(join))
        }
    }

    #[test]
    fn trait_diamond_dag() {
        let spec = DiamondWorkflow::build().unwrap();
        assert_eq!(spec.name, "diamond");
        assert_eq!(spec.tasks.len(), 4);
        assert_eq!(spec.output_index, Some(3));

        // Verify dependencies
        assert!(spec.tasks[0].dependencies.is_empty()); // root
        assert_eq!(spec.tasks[1].dependencies, vec![0]); // left -> root
        assert_eq!(spec.tasks[2].dependencies, vec![0]); // right -> root
        let mut join_deps = spec.tasks[3].dependencies.clone();
        join_deps.sort();
        assert_eq!(join_deps, vec![1, 2]); // join -> left, right
    }

    #[test]
    fn trait_node_ids_from_definition() {
        let spec = SimpleWorkflow::build().unwrap();
        assert_eq!(spec.tasks[0].node_id.as_deref(), Some("fetch"));
        assert_eq!(spec.tasks[1].node_id.as_deref(), Some("process"));
        assert_eq!(spec.tasks[2].node_id.as_deref(), Some("persist"));
    }

    // -----------------------------------------------------------------------
    // WorkflowDefConfig tests
    // -----------------------------------------------------------------------

    #[test]
    fn config_default_is_empty() {
        let config = WorkflowDefConfig::new();
        assert!(config.output.is_none());
        assert!(config.success_policy.is_none());
    }

    #[test]
    fn config_builder_chainable() {
        let policy = SuccessPolicy {
            cases: vec![SuccessCase {
                required_indices: vec![0],
                name: None,
            }],
            optional_indices: None,
        };
        let config = WorkflowDefConfig::new()
            .output(NodeRef { index: 2 })
            .success_policy(policy.clone());
        assert_eq!(config.output, Some(NodeRef { index: 2 }));
        assert_eq!(config.success_policy, Some(policy));
    }

    // -----------------------------------------------------------------------
    // Integration: trait + register
    // -----------------------------------------------------------------------

    #[test]
    fn trait_build_then_register() {
        use crate::core::registry::workflow::WorkflowSpecRegistry;

        let registered = SimpleWorkflow::build_registered().unwrap();
        let mut registry = WorkflowSpecRegistry::new();
        registry.register(registered).unwrap();
        assert!(registry.contains("simple_pipeline"));
    }

    // -----------------------------------------------------------------------
    // Validation error tests (ported from Python TestWorkflowDefinition)
    // -----------------------------------------------------------------------

    struct EmptyNameWorkflow;

    impl WorkflowDefinition for EmptyNameWorkflow {
        type Output = ();
        type Params = ();

        fn name() -> &'static str {
            ""
        }
        fn definition_key() -> &'static str {
            ""
        }

        fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
            builder.task(TaskNode::<()>::raw("task_a").node_id("task_a"));
            Ok(WorkflowDefConfig::new())
        }
    }

    #[test]
    fn trait_empty_name_validation() {
        use crate::core::error::ErrorCode;

        let result = EmptyNameWorkflow::build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowNoName));
    }

    struct NoTasksWorkflow;

    impl WorkflowDefinition for NoTasksWorkflow {
        type Output = ();
        type Params = ();

        fn name() -> &'static str {
            "empty_workflow"
        }
        fn definition_key() -> &'static str {
            "empty_workflow"
        }

        fn define(_builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
            // No tasks added
            Ok(WorkflowDefConfig::new())
        }
    }

    #[test]
    fn trait_no_tasks_validation() {
        use crate::core::error::ErrorCode;

        let result = NoTasksWorkflow::build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowNoNodes));
    }

    struct DuplicateTaskFnWorkflow;

    impl WorkflowDefinition for DuplicateTaskFnWorkflow {
        type Output = ();
        type Params = ();

        fn name() -> &'static str {
            "multi_fetch"
        }
        fn definition_key() -> &'static str {
            "multi_fetch"
        }

        fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
            builder.task(TaskNode::<()>::raw("fetch").node_id("fetch_a"));
            builder.task(TaskNode::<()>::raw("fetch").node_id("fetch_b"));
            builder.task(TaskNode::<()>::raw("fetch").node_id("fetch_c"));
            Ok(WorkflowDefConfig::new())
        }
    }

    #[test]
    fn trait_duplicate_task_function_allowed() {
        let spec = DuplicateTaskFnWorkflow::build().unwrap();
        assert_eq!(spec.tasks.len(), 3);
        // Each gets a unique node_id despite same task function name
        assert_eq!(spec.tasks[0].node_id.as_deref(), Some("fetch_a"));
        assert_eq!(spec.tasks[1].node_id.as_deref(), Some("fetch_b"));
        assert_eq!(spec.tasks[2].node_id.as_deref(), Some("fetch_c"));
        // All reference the same task function
        assert!(spec.tasks.iter().all(|t| t.task_name == "fetch"));
    }

    // -----------------------------------------------------------------------
    // Multi-error and phase-gating tests
    // (ported from Python TestMultiErrorWorkflowCollection)
    //
    // NOTE: cycle_collects_no_roots_and_cycle from Python requires mutating
    // internal builder state (forward-reference deps) which is private in
    // Rust. That test lives in workflow::spec::tests (e005_no_root_tasks,
    // e007_cycle_detected) where private fields are accessible.
    // The tests below cover the equivalent semantics reachable through
    // the public WorkflowSpecBuilder / WorkflowDefinition API.
    // -----------------------------------------------------------------------

    #[test]
    fn multiple_join_errors_collected() {
        // Two tasks with bad join settings should both be reported.
        let mut builder = WorkflowSpecBuilder::new("bad_joins");
        let root = builder.task(TaskNode::<()>::raw("root").node_id("root"));

        // Node A: quorum with min_success=5 but only 1 dependency
        builder.task(
            TaskNode::<()>::raw("task_a")
                .waits_for(root)
                .join_quorum(5)
                .node_id("task_a"),
        );
        // Node B: quorum with min_success=0 (must be >= 1)
        builder.task(
            TaskNode::<()>::raw("task_b")
                .waits_for(root)
                .join_quorum(0)
                .node_id("task_b"),
        );

        let result = builder.build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.message.contains("2 previous errors"),
            "expected multi-error, got: {}",
            err,
        );
    }

    #[test]
    fn node_id_errors_gate_remaining_validation() {
        use crate::core::error::ErrorCode;

        // Invalid node_id prevents subsequent DAG validation from running.
        let mut builder = WorkflowSpecBuilder::new("bad_id");
        builder.task(TaskNode::<()>::raw("task_a").node_id("invalid chars!"));

        let result = builder.build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowInvalidNodeId));
    }

    #[test]
    fn single_error_preserves_original_type() {
        use crate::core::error::ErrorCode;

        // A single validation error preserves its original ErrorCode
        // rather than being wrapped in a "N previous errors" envelope.
        let builder = WorkflowSpecBuilder::new("valid_name");
        let result = builder.build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowNoNodes));
        assert!(!err.message.contains("previous errors"));
    }

    #[test]
    fn duplicate_node_ids_collected() {
        // Three nodes sharing the same node_id produces multiple errors.
        let mut builder = WorkflowSpecBuilder::new("dup_ids");
        builder.task(TaskNode::<()>::raw("task_a").node_id("shared"));
        builder.task(TaskNode::<()>::raw("task_b").node_id("shared"));
        builder.task(TaskNode::<()>::raw("task_c").node_id("shared"));

        let result = builder.build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("duplicate node_id"),
            "expected duplicate node_id error, got: {}",
            msg,
        );
    }

    #[test]
    fn trait_error_propagation_from_define() {
        // Errors returned from define() propagate through build().
        struct FailingDefine;

        impl WorkflowDefinition for FailingDefine {
            type Output = ();
            type Params = ();

            fn name() -> &'static str {
                "fail_define"
            }
            fn definition_key() -> &'static str {
                "fail_define"
            }

            fn define(
                _builder: &mut WorkflowSpecBuilder,
            ) -> Result<WorkflowDefConfig, HorsiesError> {
                Err(HorsiesError::new("custom define error"))
            }
        }

        let result = FailingDefine::build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.message, "custom define error");
    }
}
