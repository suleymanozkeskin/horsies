//! The registry: registration refusals, the check of a spec against the
//! registered definitions, the core's view, and the nesting check.

use serde::{Deserialize, Serialize};

use super::*;
use crate::workflow::node::tests::task_node;
use crate::workflow::{
    OutputlessResults, SubWorkflowParams, WorkflowDefConfig, WorkflowSpecBuilder,
};

fn registry() -> WorkflowRegistry {
    WorkflowRegistry::new(AppConfig::for_database_url(
        "postgresql://localhost/horsies",
    ))
}

fn codes(findings: &[HorsiesError]) -> Vec<Option<ErrorCode>> {
    findings.iter().map(|finding| finding.code).collect()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Level {
    n: u32,
}

/// A spec of one task node and, when `child` is given, one sub-workflow
/// node of `D` after it (`waits` true) or as a root.
fn spec_with<D>(
    name: &str,
    key: &str,
    child: Option<(SubWorkflowParams<D::Params>, bool)>,
) -> Result<WorkflowSpec, HorsiesError>
where
    D: WorkflowDefinition,
{
    let mut builder = WorkflowSpecBuilder::new(name);
    builder.definition_key(key);
    let tick = builder.task(task_node::<u32, ()>("tick"));
    if let Some((params, waits)) = child {
        let node = D::node(params)?;
        let node = match waits {
            true => node.waits_for(tick),
            false => node,
        };
        builder.sub_workflow(node);
    }
    builder.build()
}

/// A definition with params that includes itself with `n - 1` until 0.
struct Countdown;

impl WorkflowDefinition for Countdown {
    type Output = OutputlessResults;
    type Params = Level;

    fn name() -> &'static str {
        "countdown"
    }

    fn definition_key() -> &'static str {
        "tests.countdown.v1"
    }

    fn build_with(params: Level) -> Result<WorkflowSpec, HorsiesError> {
        let child = match params.n {
            0 => None,
            n => Some((SubWorkflowParams::Given(Level { n: n - 1 }), false)),
        };
        spec_with::<Countdown>("countdown", "tests.countdown.v1", child)
    }
}

/// A definition with params that includes itself with `n + 1`, at once.
struct Countup;

impl WorkflowDefinition for Countup {
    type Output = OutputlessResults;
    type Params = Level;

    fn name() -> &'static str {
        "countup"
    }

    fn definition_key() -> &'static str {
        "tests.countup.v1"
    }

    fn build_with(params: Level) -> Result<WorkflowSpec, HorsiesError> {
        let next = SubWorkflowParams::Given(Level { n: params.n + 1 });
        spec_with::<Countup>("countup", "tests.countup.v1", Some((next, false)))
    }
}

/// A definition with params that includes itself with the same params:
/// at once (`Mirror`), or after its task (`Waiting`).
struct Mirror;

impl WorkflowDefinition for Mirror {
    type Output = OutputlessResults;
    type Params = Level;

    fn name() -> &'static str {
        "mirror"
    }

    fn definition_key() -> &'static str {
        "tests.mirror.v1"
    }

    fn build_with(params: Level) -> Result<WorkflowSpec, HorsiesError> {
        let same = SubWorkflowParams::Given(params);
        spec_with::<Mirror>("mirror", "tests.mirror.v1", Some((same, false)))
    }
}

struct Waiting;

impl WorkflowDefinition for Waiting {
    type Output = OutputlessResults;
    type Params = Level;

    fn name() -> &'static str {
        "waiting"
    }

    fn definition_key() -> &'static str {
        "tests.waiting.v1"
    }

    fn build_with(params: Level) -> Result<WorkflowSpec, HorsiesError> {
        let same = SubWorkflowParams::Given(params);
        spec_with::<Waiting>("waiting", "tests.waiting.v1", Some((same, true)))
    }
}

/// A definition without params whose one node is `D` with `Level { n: 3 }`.
struct Root<D>(std::marker::PhantomData<D>);

impl<D> WorkflowDefinition for Root<D>
where
    D: WorkflowDefinition<Params = Level>,
{
    type Output = OutputlessResults;
    type Params = ();

    fn name() -> &'static str {
        "root"
    }

    fn definition_key() -> &'static str {
        "tests.root.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        builder.sub_workflow(D::node(SubWorkflowParams::Given(Level { n: 3 }))?);
        Ok(WorkflowDefConfig::new())
    }
}

/// A definition without params of one task node.
struct Plain;

impl WorkflowDefinition for Plain {
    type Output = OutputlessResults;
    type Params = ();

    fn name() -> &'static str {
        "plain"
    }

    fn definition_key() -> &'static str {
        "tests.plain.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        builder.task(task_node::<u32, ()>("tick"));
        Ok(WorkflowDefConfig::new())
    }
}

#[test]
fn a_definition_registers_once_by_key_and_by_name() {
    let registry = registry();
    registry
        .register(Definition::without_params::<Plain>().unwrap())
        .unwrap();
    let again = registry
        .register(Definition::without_params::<Plain>().unwrap())
        .unwrap_err();
    assert_eq!(again.code, Some(ErrorCode::WorkflowDuplicateDefinitionKey));
    let same_name = Definition {
        definition_key: "tests.plain.v2".to_owned(),
        ..Definition::without_params::<Plain>().unwrap()
    };
    assert_eq!(
        registry.register(same_name).unwrap_err().code,
        Some(ErrorCode::WorkflowDuplicateDefinitionKey)
    );
    assert!(registry.definition("tests.plain.v1").is_some());
}

#[test]
fn an_empty_name_or_key_is_refused() {
    let registry = registry();
    let no_key = Definition {
        definition_key: " ".to_owned(),
        ..Definition::with_params::<Countdown>()
    };
    assert_eq!(
        registry.register(no_key).unwrap_err().code,
        Some(ErrorCode::WorkflowNoDefinitionKey)
    );
    let no_name = Definition {
        name: String::new(),
        ..Definition::with_params::<Countdown>()
    };
    assert_eq!(
        registry.register(no_name).unwrap_err().code,
        Some(ErrorCode::WorkflowNoName)
    );
}

#[test]
fn a_spec_with_an_unregistered_child_is_refused() {
    let registry = registry();
    let spec = Root::<Countdown>::build().unwrap();
    let refused = registry.prepare(&spec).unwrap_err();
    assert_eq!(refused.code, Some(ErrorCode::WorkflowMissingRequiredParams));
    registry
        .register(Definition::with_params::<Countdown>())
        .unwrap();
    let client = registry.prepare(&spec).unwrap();
    assert_eq!(client.name(), "root");
}

/// Python `_collect_subworkflow_default_build_with_param_errors`.
#[test]
fn params_for_a_definition_without_params_are_refused() {
    let registry = registry();
    // The node gives `Level { n: 3 }` to the key of Countdown, which is
    // registered here as a definition without params.
    registry
        .register(Definition {
            name: "countdown".to_owned(),
            definition_key: "tests.countdown.v1".to_owned(),
            kind: DefinitionKind::WithoutParams(Arc::new(Plain::build().unwrap())),
        })
        .unwrap();
    let spec = Root::<Countdown>::build().unwrap();
    assert_eq!(
        registry.prepare(&spec).unwrap_err().code,
        Some(ErrorCode::WorkflowSubworkflowParamsRequireBuildWith)
    );
}

#[test]
fn the_core_view_builds_each_definition_from_its_params_text() {
    let registry = registry();
    registry
        .register(Definition::with_params::<Countdown>())
        .unwrap();
    registry
        .register(Definition::without_params::<Plain>().unwrap())
        .unwrap();
    let core = registry.core_registry();
    let countdown = core
        .get_definition_by_definition_key("tests.countdown.v1")
        .unwrap();
    let built = (countdown.spec_builder)(r#"{"n": 0}"#).unwrap();
    assert_eq!(built.name, "countdown");
    assert_eq!(built.tasks.len(), 1);
    assert!(matches!(
        (countdown.spec_builder)(r#"{"n": "zero"}"#),
        Err(SpecBuildError::MemberRefused { ref key, .. }) if key == "n"
    ));
    assert!(matches!(
        (countdown.spec_builder)("{}"),
        Err(SpecBuildError::Failed(_))
    ));
    let plain = core
        .get_definition_by_definition_key("tests.plain.v1")
        .unwrap();
    assert_eq!((plain.spec_builder)("{}").unwrap().tasks.len(), 1);
}

#[test]
fn unit_params_read_an_empty_object() {
    assert!(decode_params::<()>("{}").is_ok());
    assert!(decode_params::<()>("null").is_ok());
    assert!(matches!(
        decode_params::<Level>("{}"),
        Err(ParamsBuildRefused::Failed(_))
    ));
}

/// A recursive definition whose nesting ends stays valid.
#[test]
fn a_nesting_that_ends_has_no_finding() {
    let registry = registry();
    registry
        .register(Definition::with_params::<Countdown>())
        .unwrap();
    registry
        .register(Definition::without_params::<Root<Countdown>>().unwrap())
        .unwrap();
    assert_eq!(codes(&registry.check_findings()), Vec::new());
}

/// A recursive definition whose child waits for a node stays valid: results
/// at run time decide, and the depth limit stops it there.
#[test]
fn a_repeat_after_a_dependency_has_no_finding() {
    let registry = registry();
    registry
        .register(Definition::with_params::<Waiting>())
        .unwrap();
    registry
        .register(Definition::without_params::<Root<Waiting>>().unwrap())
        .unwrap();
    assert_eq!(codes(&registry.check_findings()), Vec::new());
}

#[test]
fn a_repeat_with_the_same_params_at_once_is_reported() {
    let registry = registry();
    registry
        .register(Definition::with_params::<Mirror>())
        .unwrap();
    registry
        .register(Definition::without_params::<Root<Mirror>>().unwrap())
        .unwrap();
    assert_eq!(
        codes(&registry.check_findings()),
        vec![Some(ErrorCode::WorkflowSubworkflowSelfNesting)]
    );
}

#[test]
fn a_nesting_past_the_depth_limit_at_once_is_reported() {
    let registry = registry();
    registry
        .register(Definition::with_params::<Countup>())
        .unwrap();
    registry
        .register(Definition::without_params::<Root<Countup>>().unwrap())
        .unwrap();
    assert_eq!(
        codes(&registry.check_findings()),
        vec![Some(ErrorCode::WorkflowSubworkflowDepthLimit)]
    );
}

#[test]
fn a_check_case_that_its_builder_refuses_is_reported() {
    let registry = registry();
    registry.add_check_cases(vec![CheckCase {
        builder_name: "broken".to_owned(),
        build: Arc::new(|| -> Result<WorkflowSpec, HorsiesError> {
            Err(HorsiesError::new("no spec for this case"))
        }),
    }]);
    assert_eq!(
        codes(&registry.check_findings()),
        vec![Some(ErrorCode::WorkflowCheckCaseInvalid)]
    );
}

#[test]
fn the_decision_stops_at_the_depth_limit() {
    let step = |key: &str, start: ChildStart| NestingStep {
        definition_key: key.to_owned(),
        params_text: "{}".to_owned(),
        start,
    };
    let path: Vec<NestingStep> = (0..MAX_SUBWORKFLOW_DEPTH)
        .map(|level| step(&format!("level{level}"), ChildStart::AfterDependencies))
        .collect();
    assert!(matches!(
        decide_nesting(&path, "n", step("next", ChildStart::WithItsWorkflow)),
        NestingDecision::StopWithoutFinding
    ));
    assert!(matches!(
        decide_nesting(&path[..1], "n", step("next", ChildStart::WithItsWorkflow)),
        NestingDecision::WalkChild(walked) if walked.len() == 2
    ));
}

/// A definition with params whose build_with panics.
struct Panicking;

impl WorkflowDefinition for Panicking {
    type Output = OutputlessResults;
    type Params = Level;

    fn name() -> &'static str {
        "panicking"
    }

    fn definition_key() -> &'static str {
        "tests.panicking.v1"
    }

    fn build_with(params: Level) -> Result<WorkflowSpec, HorsiesError> {
        panic!("no spec for level {}", params.n)
    }
}

/// A panic of build_with is a refusal of the build: the core stores
/// WORKFLOW_ENQUEUE_FAILED on the node, as for a raised Python exception.
#[test]
fn a_builder_panic_is_a_failed_build() {
    let registry = registry();
    registry
        .register(Definition::with_params::<Panicking>())
        .unwrap();
    let core = registry.core_registry();
    let panicking = core
        .get_definition_by_definition_key("tests.panicking.v1")
        .unwrap();
    match (panicking.spec_builder)(r#"{"n": 4}"#) {
        Err(SpecBuildError::Failed(error)) => {
            assert!(error.message.contains("no spec for level 4"), "{error}");
        }
        other => panic!("expected a failed build, got {other:?}"),
    }
}

#[test]
fn a_check_case_that_panics_is_reported() {
    let registry = registry();
    registry.add_check_cases(vec![CheckCase {
        builder_name: "panicking".to_owned(),
        build: Arc::new(|| -> Result<WorkflowSpec, HorsiesError> {
            panic!("the case cannot be built")
        }),
    }]);
    assert_eq!(
        codes(&registry.check_findings()),
        vec![Some(ErrorCode::WorkflowCheckBuilderException)]
    );
}
