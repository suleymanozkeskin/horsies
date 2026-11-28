//! The registry: registration refusals, the check of a spec against the
//! registered definitions, the child builds it answers, and the nesting
//! check.

use serde::{Deserialize, Serialize};

use super::*;
use crate::workflow::node::tests::{add, task_node, test_builder};
use crate::workflow::{
    NoWorkflowOutput, SubWorkflowParameters, WorkflowDefinitionConfig, WorkflowSpecBuilder,
};

/// The typed functions of an app with no task: the definitions here add
/// their task nodes with `add`, which needs no registration.
fn tasks() -> Arc<TaskFunctions> {
    Arc::new(TaskFunctions::new())
}

fn registry() -> WorkflowRegistry {
    WorkflowRegistry::new(AppConfig::from_database_url(
        "postgresql://localhost/horsies",
    ))
}

fn codes(findings: &[HorsiesError]) -> Vec<Option<HorsiesErrorCode>> {
    findings.iter().map(|finding| finding.code).collect()
}

/// The request of the root sub-workflow node 0 of `definition_key`, with
/// the node input `node_input`.
fn child_request(definition_key: &str, node_input: &str) -> ChildSpecRequest {
    ChildSpecRequest {
        parent_workflow_id: uuid::Uuid::nil(),
        parent_workflow_name: "parent".to_owned(),
        task_index: 0,
        child_depth: 1,
        definition_key: Some(definition_key.to_owned()),
        params: horsies_core::SubWorkflowParameters::Given,
        node_input: node_input.to_owned(),
        args_from: None,
        sources: Vec::new(),
    }
}

/// The error that an answer stores on its node.
fn node_error(answer: Result<ClientChildSpec, ChildSpecUnavailable>) -> TaskError {
    let failure_result = match answer {
        Ok(ClientChildSpec::NodeFails { failure_result, .. }) => failure_result,
        other => panic!("expected the node to fail, got {other:?}"),
    };
    match serde_json::from_str::<horsies_core::TaskResult<serde_json::Value>>(&failure_result) {
        Ok(horsies_core::TaskResult::Err(error)) => error,
        other => panic!("expected an error envelope, got {other:?}"),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Level {
    n: u32,
}

/// A spec of one task node on `builder` and, when `child` is given, one
/// sub-workflow node of `D` after it (`waits` true) or as a root.
fn spec_with<D>(
    mut builder: WorkflowSpecBuilder,
    key: &str,
    child: Option<(SubWorkflowParameters<D::Parameters>, bool)>,
) -> Result<WorkflowSpec, HorsiesError>
where
    D: WorkflowDefinition,
{
    builder.definition_key(key);
    let tick = add(&mut builder, task_node::<u32, ()>("tick"));
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
    type Output = NoWorkflowOutput;
    type Parameters = Level;

    fn name() -> &'static str {
        "countdown"
    }

    fn definition_key() -> &'static str {
        "tests.countdown.v1"
    }

    fn build_with(
        builder: WorkflowSpecBuilder,
        params: Level,
    ) -> Result<WorkflowSpec, HorsiesError> {
        let child = match params.n {
            0 => None,
            n => Some((SubWorkflowParameters::Given(Level { n: n - 1 }), false)),
        };
        spec_with::<Countdown>(builder, "tests.countdown.v1", child)
    }
}

/// A definition with params that includes itself with `n + 1`, at once.
struct Countup;

impl WorkflowDefinition for Countup {
    type Output = NoWorkflowOutput;
    type Parameters = Level;

    fn name() -> &'static str {
        "countup"
    }

    fn definition_key() -> &'static str {
        "tests.countup.v1"
    }

    fn build_with(
        builder: WorkflowSpecBuilder,
        params: Level,
    ) -> Result<WorkflowSpec, HorsiesError> {
        let next = SubWorkflowParameters::Given(Level { n: params.n + 1 });
        spec_with::<Countup>(builder, "tests.countup.v1", Some((next, false)))
    }
}

/// A definition with params that includes itself with the same params:
/// at once (`Mirror`), or after its task (`Waiting`).
struct Mirror;

impl WorkflowDefinition for Mirror {
    type Output = NoWorkflowOutput;
    type Parameters = Level;

    fn name() -> &'static str {
        "mirror"
    }

    fn definition_key() -> &'static str {
        "tests.mirror.v1"
    }

    fn build_with(
        builder: WorkflowSpecBuilder,
        params: Level,
    ) -> Result<WorkflowSpec, HorsiesError> {
        let same = SubWorkflowParameters::Given(params);
        spec_with::<Mirror>(builder, "tests.mirror.v1", Some((same, false)))
    }
}

struct Waiting;

impl WorkflowDefinition for Waiting {
    type Output = NoWorkflowOutput;
    type Parameters = Level;

    fn name() -> &'static str {
        "waiting"
    }

    fn definition_key() -> &'static str {
        "tests.waiting.v1"
    }

    fn build_with(
        builder: WorkflowSpecBuilder,
        params: Level,
    ) -> Result<WorkflowSpec, HorsiesError> {
        let same = SubWorkflowParameters::Given(params);
        spec_with::<Waiting>(builder, "tests.waiting.v1", Some((same, true)))
    }
}

/// A definition without params whose one node is `D` with `Level { n: 3 }`.
struct Root<D>(std::marker::PhantomData<D>);

impl<D> WorkflowDefinition for Root<D>
where
    D: WorkflowDefinition<Parameters = Level>,
{
    type Output = NoWorkflowOutput;
    type Parameters = ();

    fn name() -> &'static str {
        "root"
    }

    fn definition_key() -> &'static str {
        "tests.root.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefinitionConfig, HorsiesError> {
        builder.sub_workflow(D::node(SubWorkflowParameters::Given(Level { n: 3 }))?);
        Ok(WorkflowDefinitionConfig::new())
    }
}

/// A definition without params of one task node.
struct Plain;

impl WorkflowDefinition for Plain {
    type Output = NoWorkflowOutput;
    type Parameters = ();

    fn name() -> &'static str {
        "plain"
    }

    fn definition_key() -> &'static str {
        "tests.plain.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefinitionConfig, HorsiesError> {
        add(builder, task_node::<u32, ()>("tick"));
        Ok(WorkflowDefinitionConfig::new())
    }
}

#[test]
fn a_definition_registers_once_by_key_and_by_name() {
    let registry = registry();
    registry
        .register(Definition::without_params::<Plain>(&tasks()).unwrap())
        .unwrap();
    let again = registry
        .register(Definition::without_params::<Plain>(&tasks()).unwrap())
        .unwrap_err();
    assert_eq!(
        again.code,
        Some(HorsiesErrorCode::WorkflowDuplicateDefinitionKey)
    );
    let same_name = Definition {
        definition_key: "tests.plain.v2".to_owned(),
        ..Definition::without_params::<Plain>(&tasks()).unwrap()
    };
    assert_eq!(
        registry.register(same_name).unwrap_err().code,
        Some(HorsiesErrorCode::WorkflowDuplicateDefinitionKey)
    );
    assert!(registry.definition("tests.plain.v1").is_some());
}

#[test]
fn an_empty_name_or_key_is_refused() {
    let registry = registry();
    let no_key = Definition {
        definition_key: " ".to_owned(),
        ..Definition::with_params::<Countdown>(&tasks())
    };
    assert_eq!(
        registry.register(no_key).unwrap_err().code,
        Some(HorsiesErrorCode::WorkflowNoDefinitionKey)
    );
    let no_name = Definition {
        name: String::new(),
        ..Definition::with_params::<Countdown>(&tasks())
    };
    assert_eq!(
        registry.register(no_name).unwrap_err().code,
        Some(HorsiesErrorCode::WorkflowNoName)
    );
}

#[test]
fn a_spec_with_an_unregistered_child_is_refused() {
    let registry = registry();
    let spec = Root::<Countdown>::build(test_builder(Root::<Countdown>::name())).unwrap();
    let refused = registry.prepare(&spec).unwrap_err();
    assert_eq!(
        refused.code,
        Some(HorsiesErrorCode::WorkflowMissingRequiredParams)
    );
    registry
        .register(Definition::with_params::<Countdown>(&tasks()))
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
            definition_type: TypeId::of::<Plain>(),
            kind: DefinitionKind::WithoutParams(Arc::new(
                Plain::build(test_builder(Plain::name())).unwrap(),
            )),
        })
        .unwrap();
    let spec = Root::<Countdown>::build(test_builder(Root::<Countdown>::name())).unwrap();
    assert_eq!(
        registry.prepare(&spec).unwrap_err().code,
        Some(HorsiesErrorCode::WorkflowSubworkflowParamsRequireBuildWith)
    );
}

/// The child build of each definition: one with params builds from the
/// node input; a member that does not decode fails the node with
/// `WORKER_SERIALIZATION_ERROR`, and params that do not decode as a whole
/// with `WORKFLOW_ENQUEUE_FAILED`; one without params gives its spec; an
/// unregistered key fails the node with `SUBWORKFLOW_LOAD_FAILED`.
#[test]
fn the_child_build_answers_each_definition_from_the_node_input() {
    let registry = registry();
    registry
        .register(Definition::with_params::<Countdown>(&tasks()))
        .unwrap();
    registry
        .register(Definition::without_params::<Plain>(&tasks()).unwrap())
        .unwrap();

    match registry.child_spec_answer(&child_request("tests.countdown.v1", r#"{"n": 0}"#)) {
        Ok(ClientChildSpec::Built(client)) => {
            assert_eq!(client.name(), "countdown");
            assert_eq!(client.spec().tasks.len(), 1);
        }
        other => panic!("expected the countdown child, got {other:?}"),
    }
    let member = node_error(
        registry.child_spec_answer(&child_request("tests.countdown.v1", r#"{"n": "zero"}"#)),
    );
    assert_eq!(member.error_code.to_string(), "WORKER_SERIALIZATION_ERROR");
    assert!(
        member.message.as_deref().is_some_and(
            |text| text.starts_with("Typed decode of subworkflow kwargs failed for task 0: ")
        ),
        "{member}"
    );
    let whole = node_error(registry.child_spec_answer(&child_request("tests.countdown.v1", "{}")));
    assert_eq!(whole.error_code.to_string(), "WORKFLOW_ENQUEUE_FAILED");
    match registry.child_spec_answer(&child_request("tests.plain.v1", "{}")) {
        Ok(ClientChildSpec::Built(client)) => assert_eq!(client.spec().tasks.len(), 1),
        other => panic!("expected the plain child, got {other:?}"),
    }
    let absent = node_error(registry.child_spec_answer(&child_request("tests.absent.v1", "{}")));
    assert_eq!(absent.error_code.to_string(), "SUBWORKFLOW_LOAD_FAILED");
    assert_eq!(
        absent
            .data
            .as_ref()
            .map(|data| data.decode::<serde_json::Value>().unwrap()),
        Some(serde_json::json!({"definition_key": "tests.absent.v1"}))
    );
}

#[test]
fn unit_params_read_an_empty_object() {
    assert!(decode_params::<()>("{}", &[]).is_ok());
    assert!(decode_params::<()>("null", &[]).is_ok());
    assert!(matches!(
        decode_params::<Level>("{}", &[]),
        Err(ParamsBuildRefused::Undecodable { .. })
    ));
}

/// The data that a node error of the request of [`child_request`] holds.
fn request_data() -> serde_json::Value {
    serde_json::json!({
        "workflow_id": uuid::Uuid::nil().to_string(),
        "task_index": 0,
    })
}

/// A member that no params field takes fails the node with
/// `WORKER_SERIALIZATION_ERROR` and Python's prefix (Python
/// `decode_subworkflow_kwargs`), although `Level` does not deny unknown
/// fields; `build_with` does not run.
#[test]
fn a_member_that_no_params_field_takes_fails_the_node() {
    let registry = registry();
    registry
        .register(Definition::with_params::<Countdown>(&tasks()))
        .unwrap();
    let error = node_error(registry.child_spec_answer(&child_request(
        "tests.countdown.v1",
        r#"{"n": 0, "colour": "red"}"#,
    )));
    assert_eq!(error.error_code.to_string(), "WORKER_SERIALIZATION_ERROR");
    assert_eq!(
        error.message.as_deref(),
        Some(
            "Typed decode of subworkflow kwargs failed for task 0: colour: unknown field `colour`"
        )
    );
    assert_eq!(
        error
            .data
            .as_ref()
            .map(|data| data.decode::<serde_json::Value>().unwrap()),
        Some(request_data())
    );
}

/// A definition with params whose params are `()`.
struct NoParams;

impl WorkflowDefinition for NoParams {
    type Output = NoWorkflowOutput;
    type Parameters = ();

    fn name() -> &'static str {
        "no_params"
    }

    fn definition_key() -> &'static str {
        "tests.no_params.v1"
    }

    fn build_with(builder: WorkflowSpecBuilder, _params: ()) -> Result<WorkflowSpec, HorsiesError> {
        spec_with::<NoParams>(builder, "tests.no_params.v1", None)
    }
}

/// `()` params take no member: a member fails the node with
/// `WORKER_SERIALIZATION_ERROR`, Python's prefix, and the text of the static
/// check for a type with no fields.
#[test]
fn a_member_for_unit_params_fails_the_node() {
    let registry = registry();
    registry
        .register(Definition::with_params::<NoParams>(&tasks()))
        .unwrap();
    let error = node_error(
        registry.child_spec_answer(&child_request("tests.no_params.v1", r#"{"colour": "red"}"#)),
    );
    assert_eq!(error.error_code.to_string(), "WORKER_SERIALIZATION_ERROR");
    assert_eq!(
        error.message.as_deref(),
        Some(
            "Typed decode of subworkflow kwargs failed for task 0: colour: unknown field \
             `colour`, there are no fields"
        )
    );
    assert_eq!(
        error
            .data
            .as_ref()
            .map(|data| data.decode::<serde_json::Value>().unwrap()),
        Some(request_data())
    );
    match registry.child_spec_answer(&child_request("tests.no_params.v1", "{}")) {
        Ok(ClientChildSpec::Built(client)) => assert_eq!(client.name(), "no_params"),
        other => panic!("expected the no_params child, got {other:?}"),
    }
}

/// The `args_from` members hold results: the read-back compare skips them.
/// The same member as a static member is refused.
#[test]
fn the_compare_skips_the_args_from_members() {
    let text = r#"{"n": 1, "source": {"ok": 5}}"#;
    assert_eq!(
        decode_params::<Level>(text, &["source".to_owned()]).ok(),
        Some(Level { n: 1 })
    );
    assert!(matches!(
        decode_params::<Level>(text, &[]),
        Err(ParamsBuildRefused::Member { key, .. }) if key == "source"
    ));
}

/// The params of the decode tests: two members.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct CountAndLabel {
    count: i64,
    label: String,
}

/// A member whose value does not fit its field is refused by its key, with
/// its key path and the decode's text, never the value; `build_with` does
/// not run.
#[test]
fn a_params_member_that_does_not_decode_is_refused_by_its_key() {
    match decode_params::<CountAndLabel>(r#"{"count":"seven","label":"a"}"#, &[]) {
        Err(ParamsBuildRefused::Member { key, cause }) => {
            assert_eq!(key, "count");
            assert_eq!(cause, "count: invalid type: <input>, expected i64");
        }
        other => panic!("expected a member refusal, got {other:?}"),
    }
}

/// The decode reads the members in the order of the text: of two refused
/// members, the first in the text is named, not the first in key order.
#[test]
fn the_first_refused_member_in_the_text_is_named() {
    match decode_params::<CountAndLabel>(r#"{"label":5,"count":"seven"}"#, &[]) {
        Err(ParamsBuildRefused::Member { key, .. }) => assert_eq!(key, "label"),
        other => panic!("expected a member refusal, got {other:?}"),
    }
}

/// The params of [`a_params_member_of_400_digits_passes_the_key_check`]: a
/// field that keeps the text of its value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct WideParams {
    amount: horsies_core::JsonText,
}

/// #1893: the static members and the read-back compare of params read
/// members as texts and parse no number, so an integer of 400 digits in a
/// `JsonText` field passes; `()` names the key of such a member.
#[test]
fn a_params_member_of_400_digits_passes_the_key_check() {
    let wide = "9".repeat(400);
    let text = format!(r#"{{"amount": {wide}}}"#);
    let params = decode_params::<WideParams>(&text, &[]).unwrap();
    assert_eq!(params.amount.get(), wide);
    assert_eq!(
        static_members(&text, &[]).unwrap(),
        vec!["amount".to_owned()]
    );
    match decode_params::<()>(&text, &[]) {
        Err(ParamsBuildRefused::Member { key, .. }) => assert_eq!(key, "amount"),
        other => panic!("expected a member refusal, got {other:?}"),
    }
}

#[test]
fn params_that_fit_decode() {
    assert_eq!(
        decode_params::<CountAndLabel>(r#"{"count":7,"label":"a"}"#, &[]).ok(),
        Some(CountAndLabel {
            count: 7,
            label: "a".to_owned(),
        })
    );
}

/// A params field with no member: `build_with` does not run, and the node
/// stores `WORKFLOW_ENQUEUE_FAILED` with Python's prefix, the decode's text
/// (no exception class) and Python's suffix, and the workflow id and the
/// node in `data`.
#[test]
fn a_params_field_with_no_member_fails_the_build_with_the_decode_text() {
    let registry = registry();
    registry
        .register(Definition::with_params::<Countdown>(&tasks()))
        .unwrap();
    let error = node_error(registry.child_spec_answer(&child_request("tests.countdown.v1", "{}")));
    let expected = format!(
        "Subworkflow build failed: the params do not decode as {}: .: missing field `n` \
         (workflow parent:0)",
        std::any::type_name::<Level>()
    );
    assert_eq!(error.error_code.to_string(), "WORKFLOW_ENQUEUE_FAILED");
    assert_eq!(error.message.as_deref(), Some(expected.as_str()));
    assert_eq!(
        error
            .data
            .as_ref()
            .map(|data| data.decode::<serde_json::Value>().unwrap()),
        Some(serde_json::json!({
            "workflow_id": uuid::Uuid::nil().to_string(),
            "task_index": 0,
        }))
    );
}

/// A recursive definition whose nesting ends stays valid.
#[test]
fn a_nesting_that_ends_has_no_finding() {
    let registry = registry();
    registry
        .register(Definition::with_params::<Countdown>(&tasks()))
        .unwrap();
    registry
        .register(Definition::without_params::<Root<Countdown>>(&tasks()).unwrap())
        .unwrap();
    assert_eq!(codes(&registry.check_findings()), Vec::new());
}

/// A recursive definition whose child waits for a node stays valid: results
/// at run time decide, and the depth limit stops it there.
#[test]
fn a_repeat_after_a_dependency_has_no_finding() {
    let registry = registry();
    registry
        .register(Definition::with_params::<Waiting>(&tasks()))
        .unwrap();
    registry
        .register(Definition::without_params::<Root<Waiting>>(&tasks()).unwrap())
        .unwrap();
    assert_eq!(codes(&registry.check_findings()), Vec::new());
}

#[test]
fn a_repeat_with_the_same_params_at_once_is_reported() {
    let registry = registry();
    registry
        .register(Definition::with_params::<Mirror>(&tasks()))
        .unwrap();
    registry
        .register(Definition::without_params::<Root<Mirror>>(&tasks()).unwrap())
        .unwrap();
    assert_eq!(
        codes(&registry.check_findings()),
        vec![Some(HorsiesErrorCode::WorkflowSubworkflowSelfNesting)]
    );
}

#[test]
fn a_nesting_past_the_depth_limit_at_once_is_reported() {
    let registry = registry();
    registry
        .register(Definition::with_params::<Countup>(&tasks()))
        .unwrap();
    registry
        .register(Definition::without_params::<Root<Countup>>(&tasks()).unwrap())
        .unwrap();
    assert_eq!(
        codes(&registry.check_findings()),
        vec![Some(HorsiesErrorCode::WorkflowSubworkflowDepthLimit)]
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
        vec![Some(HorsiesErrorCode::WorkflowCheckCaseInvalid)]
    );
}

/// The finding of a refused check case keeps the builder's error in a
/// note: its code and its text.
#[test]
fn a_refused_check_case_keeps_the_code_and_the_text_of_its_error() {
    let registry = registry();
    registry.add_check_cases(vec![CheckCase {
        builder_name: "config_err".to_owned(),
        build: Arc::new(|| -> Result<WorkflowSpec, HorsiesError> {
            Err(HorsiesError::new("bad config").with_code(HorsiesErrorCode::DatabaseInvalidUrl))
        }),
    }]);
    let findings = registry.check_findings();
    assert_eq!(
        codes(&findings),
        vec![Some(HorsiesErrorCode::WorkflowCheckCaseInvalid)]
    );
    let text = findings[0].to_string();
    assert!(text.contains("config_err"), "{text}");
    assert!(text.contains("HRS-203"), "{text}");
    assert!(text.contains("bad config"), "{text}");
}

/// Python's builder check requires a definition key
/// (`WorkflowSpec._require_definition_key`): the spec of a check case with
/// no key or a blank one is reported with `HRS-016`, naming the workflow,
/// and is not checked further.
#[test]
fn a_check_case_without_a_definition_key_is_reported() {
    let registry = registry();
    registry.add_check_cases(vec![
        CheckCase {
            builder_name: "keyless".to_owned(),
            build: Arc::new(|| -> Result<WorkflowSpec, HorsiesError> {
                let mut builder = test_builder("keyless");
                add(&mut builder, task_node::<u32, ()>("tick"));
                builder.build()
            }),
        },
        CheckCase {
            builder_name: "blank".to_owned(),
            build: Arc::new(|| spec_with::<Plain>(test_builder("blank_key"), " ", None)),
        },
    ]);
    let findings = registry.check_findings();
    assert_eq!(
        codes(&findings),
        vec![
            Some(HorsiesErrorCode::WorkflowNoDefinitionKey),
            Some(HorsiesErrorCode::WorkflowNoDefinitionKey),
        ]
    );
    assert!(
        findings[0].to_string().contains("'keyless'"),
        "{}",
        findings[0]
    );
    assert!(
        findings[1].to_string().contains("'blank_key'"),
        "{}",
        findings[1]
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
    type Output = NoWorkflowOutput;
    type Parameters = Level;

    fn name() -> &'static str {
        "panicking"
    }

    fn definition_key() -> &'static str {
        "tests.panicking.v1"
    }

    fn build_with(
        _builder: WorkflowSpecBuilder,
        params: Level,
    ) -> Result<WorkflowSpec, HorsiesError> {
        panic!("no spec for level {}", params.n)
    }
}

/// A panic of build_with is a refusal of the build: the node stores
/// WORKFLOW_ENQUEUE_FAILED, as for a raised Python exception.
#[test]
fn a_builder_panic_is_a_failed_build() {
    let registry = registry();
    registry
        .register(Definition::with_params::<Panicking>(&tasks()))
        .unwrap();
    let error =
        node_error(registry.child_spec_answer(&child_request("tests.panicking.v1", r#"{"n": 4}"#)));
    assert_eq!(error.error_code.to_string(), "WORKFLOW_ENQUEUE_FAILED");
    assert!(
        error
            .message
            .as_deref()
            .is_some_and(|text| text.contains("no spec for level 4")),
        "{error}"
    );
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
        vec![Some(HorsiesErrorCode::WorkflowCheckBuilderException)]
    );
}
