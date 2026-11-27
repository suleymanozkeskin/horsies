//! One test per refusal of `WorkflowSpecBuilder::build`, and the shape of a
//! built spec.

use std::convert::Infallible;

use serde::{Deserialize, Serialize};

use super::*;
use crate::workflow::node::tests::{
    add, default_registration, task_node, test_builder, ChargeInput,
};
use crate::workflow::node::InputField;
use crate::{TaskOutcome, WorkflowContext};

/// The input of a task that takes the result of a dependency.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReportInput {
    title: String,
    total: TaskOutcome<f64, Infallible>,
}

/// The same input with the dependency's result optional.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OptionalReportInput {
    title: String,
    total: Option<TaskOutcome<f64, Infallible>>,
}

/// The input of a task that takes the workflow context.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextInput {
    workflow_context: WorkflowContext,
}

fn total<I>() -> InputField<I, TaskOutcome<f64, Infallible>> {
    InputField::raw("total")
}

fn title<I>() -> InputField<I, String> {
    InputField::raw("title")
}

fn charge() -> TaskNode<f64, ChargeInput> {
    task_node::<f64, ChargeInput>("charge")
        .set_input(ChargeInput {
            order_id: 7,
            amount: 9.5,
        })
        .unwrap()
}

fn report() -> TaskNode<String, ReportInput> {
    task_node::<String, ReportInput>("report")
        .set(title(), "daily".to_owned())
        .unwrap()
}

/// The registration of a task whose input takes the workflow context.
fn context_registration() -> NodeRegistration {
    NodeRegistration {
        accepts_workflow_context: true,
        ..default_registration()
    }
}

fn refusal(builder: WorkflowSpecBuilder) -> HorsiesError {
    builder.build().unwrap_err()
}

fn code_of(builder: WorkflowSpecBuilder) -> Option<HorsiesErrorCode> {
    refusal(builder).code
}

#[test]
fn a_chain_builds_with_auto_node_ids() {
    let mut builder = test_builder("daily report");
    builder.definition_key("tests.daily_report.v1");
    let charged = add(&mut builder, charge());
    let reported = add(&mut builder, report().arg_from(total(), charged));
    builder.output(reported);
    let spec = builder.build().unwrap();
    assert_eq!(spec.name(), "daily report");
    assert_eq!(spec.definition_key(), Some("tests.daily_report.v1"));
    assert_eq!(
        spec.node_ids().collect::<Vec<_>>(),
        vec!["daily_report:0", "daily_report:1"]
    );
    assert_eq!(spec.key(reported).unwrap().node_id(), "daily_report:1");
    assert_eq!(spec.nodes[1].dependencies, vec![0]);
    assert_eq!(spec.nodes[1].args_from.get("total"), Some(&0));
    assert!(matches!(spec.output, SpecOutput::Node { index: 1, .. }));
    assert_eq!(spec.on_error(), OnError::Fail);
    spec.check_output_type::<String>().unwrap();
}

#[test]
fn an_empty_name_is_refused() {
    let mut builder = test_builder(" ");
    add(&mut builder, charge());
    assert_eq!(code_of(builder), Some(HorsiesErrorCode::WorkflowNoName));
}

#[test]
fn a_spec_without_a_node_is_refused() {
    assert_eq!(
        code_of(test_builder("empty")),
        Some(HorsiesErrorCode::WorkflowNoNodes)
    );
}

#[test]
fn more_nodes_than_the_limit_are_refused() {
    let mut builder = test_builder("wide");
    for _ in 0..=MAX_WORKFLOW_NODES {
        add(&mut builder, charge());
    }
    let refused = refusal(builder);
    assert_eq!(refused.code, Some(HorsiesErrorCode::WorkflowTooManyNodes));
    assert_eq!(
        HorsiesErrorCode::WorkflowTooManyNodes.as_code_str(),
        "HRS-038"
    );
    assert!(refused.message.contains("10001 nodes"), "{refused}");
}

#[test]
fn an_invalid_node_id_is_refused() {
    let mut builder = test_builder("ids");
    add(&mut builder, charge().node_id("has space"));
    assert_eq!(
        code_of(builder),
        Some(HorsiesErrorCode::WorkflowInvalidNodeId)
    );
    let mut builder = test_builder("ids");
    add(
        &mut builder,
        charge().node_id("x".repeat(MAX_NODE_ID_LENGTH + 1)),
    );
    assert_eq!(
        code_of(builder),
        Some(HorsiesErrorCode::WorkflowInvalidNodeId)
    );
    let mut builder = test_builder("ids");
    add(&mut builder, charge().node_id("step-1:sub_task.v2"));
    let spec = builder.build().unwrap();
    assert_eq!(
        spec.node_ids().collect::<Vec<_>>(),
        vec!["step-1:sub_task.v2"]
    );
}

/// A refusal of the first gate stops the build: the second gate does not
/// run, so its refusals are not reported.
#[test]
fn a_refusal_of_the_first_gate_stops_the_second_gate() {
    let mut builder = test_builder("gates");
    add(&mut builder, charge().node_id("has space").priority(0));
    let refused = refusal(builder);
    assert_eq!(refused.code, Some(HorsiesErrorCode::WorkflowInvalidNodeId));
    assert!(refused.notes.is_empty(), "{refused}");
}

#[test]
fn a_repeated_node_id_is_refused() {
    let mut builder = test_builder("ids");
    add(&mut builder, charge().node_id("same"));
    add(&mut builder, charge().node_id("same"));
    assert_eq!(
        code_of(builder),
        Some(HorsiesErrorCode::WorkflowDuplicateNodeId)
    );
}

#[test]
fn a_reference_of_another_builder_is_refused() {
    let mut other = test_builder("other");
    let foreign = add(&mut other, charge());
    let mut builder = test_builder("main");
    add(&mut builder, charge());
    add(&mut builder, report().arg_from(total(), foreign));
    assert_eq!(
        code_of(builder),
        Some(HorsiesErrorCode::WorkflowInvalidDependency)
    );

    let mut builder = test_builder("main");
    add(&mut builder, charge());
    builder.output(foreign);
    assert_eq!(
        code_of(builder),
        Some(HorsiesErrorCode::WorkflowInvalidOutput)
    );

    let mut builder = test_builder("main");
    add(&mut builder, charge());
    builder.success_policy(SuccessPolicy::new(vec![SuccessCase::new([foreign])]));
    assert_eq!(
        code_of(builder),
        Some(HorsiesErrorCode::WorkflowInvalidSuccessPolicy)
    );
}

#[test]
fn a_context_source_outside_the_dependencies_is_refused() {
    let mut builder = test_builder("ctx");
    let charged = add(&mut builder, charge());
    let node = task_node::<u32, ContextInput>("summarize").workflow_context_from([charged]);
    builder.add_task(node, context_registration());
    assert_eq!(
        code_of(builder),
        Some(HorsiesErrorCode::WorkflowInvalidContextFrom)
    );
}

#[test]
fn a_context_for_a_task_that_takes_none_is_refused() {
    let mut builder = test_builder("ctx");
    let charged = add(&mut builder, charge());
    add(
        &mut builder,
        task_node::<u32, ContextInput>("summarize")
            .waits_for(charged)
            .workflow_context_from([charged]),
    );
    let refused = refusal(builder);
    assert!(
        refused.code == Some(HorsiesErrorCode::WorkflowContextParameterMissing)
            || refused.notes.iter().any(|note| note
                .contains(HorsiesErrorCode::WorkflowContextParameterMissing.as_code_str())),
        "{refused}"
    );
}

#[test]
fn a_task_that_takes_the_context_gets_it_in_the_check() {
    let mut builder = test_builder("ctx");
    let charged = add(&mut builder, charge());
    let node = task_node::<u32, ContextInput>("summarize")
        .waits_for(charged)
        .workflow_context_from([charged]);
    builder.add_task(node, context_registration());
    let spec = builder.build().unwrap();
    assert_eq!(
        spec.nodes[1].workflow_context_from,
        vec!["ctx:0".to_owned()]
    );
}

#[test]
fn a_quorum_outside_its_bounds_is_refused() {
    let mut builder = test_builder("join");
    let charged = add(&mut builder, charge());
    add(&mut builder, charge().waits_for(charged).join_quorum(0));
    assert_eq!(
        code_of(builder),
        Some(HorsiesErrorCode::WorkflowInvalidJoin)
    );
    let mut builder = test_builder("join");
    let charged = add(&mut builder, charge());
    add(&mut builder, charge().waits_for(charged).join_quorum(2));
    assert_eq!(
        code_of(builder),
        Some(HorsiesErrorCode::WorkflowInvalidJoin)
    );
    // A quorum node with no dependency can never meet its quorum.
    let mut builder = test_builder("join");
    add(&mut builder, charge());
    add(&mut builder, charge().join_quorum(1));
    assert_eq!(
        code_of(builder),
        Some(HorsiesErrorCode::WorkflowInvalidJoin)
    );
}

#[test]
fn an_any_join_needs_a_default_for_each_result_member() {
    let mut builder = test_builder("join");
    let charged = add(&mut builder, charge());
    add(&mut builder, report().arg_from(total(), charged).join_any());
    assert_eq!(
        code_of(builder),
        Some(HorsiesErrorCode::WorkflowInvalidJoin)
    );

    let mut builder = test_builder("join");
    let charged = add(&mut builder, charge());
    add(
        &mut builder,
        task_node::<String, OptionalReportInput>("report")
            .set(title(), "daily".to_owned())
            .unwrap()
            .arg_from(total(), charged)
            .join_any(),
    );
    builder.build().unwrap();
}

#[test]
fn a_priority_outside_1_to_100_is_refused() {
    for priority in [0, 101] {
        let mut builder = test_builder("priority");
        add(&mut builder, charge().priority(priority));
        assert_eq!(
            code_of(builder),
            Some(HorsiesErrorCode::WorkflowInvalidPriority),
            "{priority}"
        );
    }
    let mut builder = test_builder("priority");
    add(&mut builder, charge().priority(1));
    builder.build().unwrap();
}

#[test]
fn a_success_policy_without_a_case_or_a_required_node_is_refused() {
    let mut builder = test_builder("policy");
    add(&mut builder, charge());
    builder.success_policy(SuccessPolicy::new(Vec::new()));
    assert_eq!(
        code_of(builder),
        Some(HorsiesErrorCode::WorkflowInvalidSuccessPolicy)
    );
    let mut builder = test_builder("policy");
    add(&mut builder, charge());
    builder.success_policy(SuccessPolicy::new(vec![SuccessCase::new(
        Vec::<NodeRef>::new(),
    )]));
    let refused = refusal(builder);
    assert_eq!(
        refused.code,
        Some(HorsiesErrorCode::WorkflowInvalidSuccessPolicy)
    );
    assert!(
        refused
            .message
            .contains("SuccessCase[0] has no required node"),
        "{refused}"
    );
}

#[test]
fn a_success_policy_lowers_to_indexes() {
    let mut builder = test_builder("policy");
    let first = add(&mut builder, charge());
    let second = add(&mut builder, charge());
    builder.success_policy(SuccessPolicy::new(vec![SuccessCase::new([first])]).optional([second]));
    let spec = builder.build().unwrap();
    assert_eq!(
        spec.success_policy,
        Some(SpecSuccessPolicy {
            cases: vec![vec![0]],
            optional: vec![1],
        })
    );
}

#[test]
fn a_static_member_that_args_from_also_names_is_refused() {
    let mut builder = test_builder("overlap");
    let charged = add(&mut builder, charge());
    add(
        &mut builder,
        report()
            .input_json(r#"{"title": "daily", "total": 1}"#)
            .unwrap()
            .arg_from(total(), charged),
    );
    assert_eq!(
        code_of(builder),
        Some(HorsiesErrorCode::WorkflowKwargsArgsFromOverlap)
    );
}

/// #571: a member that binds to no field of the input type.
#[test]
fn a_member_that_binds_to_no_field_is_refused() {
    let mut builder = test_builder("keys");
    add(
        &mut builder,
        task_node::<f64, ChargeInput>("charge")
            .input_json(r#"{"order_id": 1, "amount": 2.0, "amonut": 3}"#)
            .unwrap(),
    );
    assert_eq!(
        code_of(builder),
        Some(HorsiesErrorCode::WorkflowInvalidKwargKey)
    );
}

/// The input of a task whose types do not refuse an unknown key on read
/// (no `deny_unknown_fields`), as a user's types may be.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ShipInput {
    label: String,
    options: ShipOptions,
    parcels: Vec<Parcel>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ShipOptions {
    #[serde(default)]
    retries: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Parcel {
    weight: u32,
}

/// The refusal of a spec with one node of [`ShipInput`] whose input text is
/// `input`.
fn ship_refusal(input: &str) -> HorsiesError {
    let mut builder = test_builder("ship");
    add(
        &mut builder,
        task_node::<u32, ShipInput>("ship")
            .input_json(input)
            .unwrap(),
    );
    refusal(builder)
}

/// #571: a member that binds to no field of an input type that reads an
/// unknown key without an error (Python `extra='forbid'`).
#[test]
fn a_member_that_a_lenient_input_type_does_not_read_is_refused() {
    let refused = ship_refusal(r#"{"label": "a", "options": {}, "parcels": [], "lable": "b"}"#);
    assert_eq!(
        refused.code,
        Some(HorsiesErrorCode::WorkflowInvalidKwargKey)
    );
    assert!(refused.message.contains("lable"), "{refused}");
}

/// #571: a key inside a nested object that binds to no field, named by its
/// key path. Python's strict encode of the member's value refuses it with
/// `HRS-031`.
#[test]
fn a_nested_member_that_binds_to_no_field_is_refused_with_its_key_path() {
    let refused = ship_refusal(r#"{"label": "a", "options": {"retriess": 3}, "parcels": []}"#);
    assert_eq!(
        refused.code,
        Some(HorsiesErrorCode::WorkflowKwargsNotSerializable)
    );
    assert!(refused.message.contains("options.retriess"), "{refused}");
}

/// #571: a key inside an element of a list that binds to no field, named by
/// its key path, with `HRS-031`.
#[test]
fn a_list_element_member_that_binds_to_no_field_is_refused_with_its_key_path() {
    let refused =
        ship_refusal(r#"{"label": "a", "options": {}, "parcels": [{"weight": 1, "wieght": 2}]}"#);
    assert_eq!(
        refused.code,
        Some(HorsiesErrorCode::WorkflowKwargsNotSerializable)
    );
    assert!(refused.message.contains("parcels[0].wieght"), "{refused}");
}

/// The input of a task whose nested type refuses an unknown key on read
/// (`deny_unknown_fields`), so serde refuses the key before the compare.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictShipInput {
    label: String,
    options: StrictShipOptions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictShipOptions {
    retries: u32,
}

/// #571: a key that serde refuses inside a nested object is `HRS-031` with
/// its key path; at the top level it is `HRS-019`.
#[test]
fn a_key_that_a_strict_type_refuses_has_the_code_of_its_depth() {
    let strict_refusal = |input: &str| {
        let mut builder = test_builder("ship");
        add(
            &mut builder,
            task_node::<u32, StrictShipInput>("ship")
                .input_json(input)
                .unwrap(),
        );
        refusal(builder)
    };
    let nested = strict_refusal(r#"{"label": "a", "options": {"retries": 1, "retriess": 3}}"#);
    assert_eq!(
        nested.code,
        Some(HorsiesErrorCode::WorkflowKwargsNotSerializable)
    );
    assert!(nested.message.contains("options.retriess"), "{nested}");
    let top = strict_refusal(r#"{"label": "a", "options": {"retries": 1}, "lable": "b"}"#);
    assert_eq!(top.code, Some(HorsiesErrorCode::WorkflowInvalidKwargKey));
    assert!(top.message.contains("lable"), "{top}");
}

/// A member of the input of a task with no parameters binds to no
/// parameter: `HRS-019`, as Python's check of the parameter names.
#[test]
fn a_member_of_the_input_of_a_task_with_no_parameters_is_refused() {
    let mut builder = test_builder("unit");
    add(
        &mut builder,
        task_node::<u32, ()>("tick")
            .input_json(r#"{"count": 1}"#)
            .unwrap(),
    );
    assert_eq!(
        code_of(builder),
        Some(HorsiesErrorCode::WorkflowInvalidKwargKey)
    );
}

/// #572: a node whose static input misses a field of its input type.
#[test]
fn a_missing_field_is_refused() {
    let mut builder = test_builder("missing");
    add(&mut builder, task_node::<f64, ChargeInput>("charge"));
    assert_eq!(
        code_of(builder),
        Some(HorsiesErrorCode::WorkflowMissingRequiredParams)
    );
    let mut builder = test_builder("missing");
    let charged = add(&mut builder, charge());
    add(
        &mut builder,
        task_node::<String, ReportInput>("report").arg_from(total(), charged),
    );
    assert_eq!(
        code_of(builder),
        Some(HorsiesErrorCode::WorkflowMissingRequiredParams)
    );
}

/// The refusal of a spec with one node of `I` whose input text is `input`.
fn input_refusal<I>(input: &str) -> HorsiesError
where
    I: Serialize + serde::de::DeserializeOwned + 'static,
{
    let mut builder = test_builder("ship");
    add(
        &mut builder,
        task_node::<u32, I>("ship").input_json(input).unwrap(),
    );
    refusal(builder)
}

/// #1734: a field of the input itself that has no value (depth 0) is
/// `HRS-020`, as Python's check of the parameters.
#[test]
fn a_missing_field_of_the_input_is_refused_with_hrs_020() {
    let refused = input_refusal::<StrictShipInput>(r#"{"options": {"retries": 1}}"#);
    assert_eq!(
        refused.code,
        Some(HorsiesErrorCode::WorkflowMissingRequiredParams)
    );
    assert!(
        refused.message.contains(".: missing field `label`"),
        "{refused}"
    );
}

/// #1734: a field missing inside a member's value (depth 1, the path of
/// the member) is `HRS-031`, as Python's strict encode of the value.
#[test]
fn a_missing_field_of_a_member_s_value_is_refused_with_hrs_031() {
    let refused = input_refusal::<StrictShipInput>(r#"{"label": "a", "options": {}}"#);
    assert_eq!(
        refused.code,
        Some(HorsiesErrorCode::WorkflowKwargsNotSerializable)
    );
    assert!(
        refused.message.contains("options: missing field `retries`"),
        "{refused}"
    );
}

/// #1734: a field missing inside an element of a member's list (depth 2,
/// the path of the element) is `HRS-031`.
#[test]
fn a_missing_field_of_a_list_element_is_refused_with_hrs_031() {
    let refused = input_refusal::<ShipInput>(r#"{"label": "a", "options": {}, "parcels": [{}]}"#);
    assert_eq!(
        refused.code,
        Some(HorsiesErrorCode::WorkflowKwargsNotSerializable)
    );
    assert!(
        refused
            .message
            .contains("parcels[0]: missing field `weight`"),
        "{refused}"
    );
}

#[test]
fn a_member_of_another_type_is_refused() {
    let mut builder = test_builder("types");
    add(
        &mut builder,
        task_node::<f64, ChargeInput>("charge")
            .input_json(r#"{"order_id": "seven", "amount": 2.0}"#)
            .unwrap(),
    );
    assert_eq!(
        code_of(builder),
        Some(HorsiesErrorCode::WorkflowKwargsNotSerializable)
    );
}

#[test]
fn a_task_with_no_parameters_takes_an_empty_input() {
    let mut builder = test_builder("unit");
    add(&mut builder, task_node::<u32, ()>("tick"));
    let spec = builder.build().unwrap();
    let SpecBody::Task { input, .. } = &spec.nodes[0].body else {
        panic!("a task node");
    };
    assert_eq!(input.to_text(), "{}");
}

#[test]
fn every_refusal_of_the_second_gate_is_reported() {
    let mut builder = test_builder("many");
    let charged = add(&mut builder, charge().priority(0));
    add(&mut builder, charge().waits_for(charged).join_quorum(5));
    let refused = refusal(builder);
    assert_eq!(refused.code, None, "{refused}");
    assert_eq!(refused.notes.len(), 2, "{refused}");
}

/// #575: the output type of a spec is the type of its output node, or
/// `OutputlessResults` for a spec with no output node.
#[test]
fn the_output_type_is_checked() {
    let mut builder = test_builder("out");
    let charged = add(&mut builder, charge());
    builder.output(charged);
    let spec = builder.build().unwrap();
    spec.check_output_type::<f64>().unwrap();
    assert_eq!(
        spec.check_output_type::<String>().unwrap_err().code,
        Some(HorsiesErrorCode::WorkflowOutputTypeMismatch)
    );
    assert_eq!(
        spec.check_output_type::<OutputlessResults>()
            .unwrap_err()
            .code,
        Some(HorsiesErrorCode::WorkflowOutputTypeMismatch)
    );

    let mut builder = test_builder("no output");
    add(&mut builder, charge());
    let spec = builder.build().unwrap();
    assert!(matches!(spec.output, SpecOutput::NoOutput));
    assert_eq!(spec.success_policy, None);
    spec.check_output_type::<OutputlessResults>().unwrap();
    assert_eq!(
        spec.check_output_type::<f64>().unwrap_err().code,
        Some(HorsiesErrorCode::WorkflowOutputTypeMismatch)
    );
}

#[test]
fn a_key_of_another_builder_is_none() {
    let mut other = test_builder("other");
    let foreign = add(&mut other, charge());
    let mut builder = test_builder("main");
    add(&mut builder, charge());
    let spec = builder.build().unwrap();
    assert!(spec.key(foreign).is_none());
}

/// Python `test_workflow_models.py::TestNodeIdFromText` (spaces, valid
/// characters, invalid characters, mixed input, empty result, the node id
/// pattern), and a letter outside ASCII, which Python's
/// `[^A-Za-z0-9_\-:.]` removes.
#[test]
fn slugify_keeps_the_node_id_characters() {
    let cases = [
        ("daily report", "daily_report"),
        ("a/b*c", "abc"),
        ("***", "_"),
        ("v1.2:x-y", "v1.2:x-y"),
        ("Hello World", "Hello_World"),
        ("My Workflow Name", "My_Workflow_Name"),
        ("task:v2.0", "task:v2.0"),
        ("my-task-name", "my-task-name"),
        ("prefix.suffix", "prefix.suffix"),
        ("Hello!World", "HelloWorld"),
        ("test@#$%chars", "testchars"),
        ("parentheses(removed)", "parenthesesremoved"),
        ("My App: v2.0!", "My_App:_v2.0"),
        ("Company Name (Inc.)", "Company_Name_Inc."),
        ("my flow/v2.0", "my_flowv2.0"),
        ("!@#$%", "_"),
        ("", "_"),
        ("caf\u{e9}", "caf"),
    ];
    for (name, slug) in cases {
        assert_eq!(node_id_from_text(name), slug, "{name:?}");
        assert!(is_valid_node_id(&node_id_from_text(name)), "{name:?}");
    }
}
