//! One test per refusal of `WorkflowSpecBuilder::build`, and the shape of a
//! built spec.

use std::convert::Infallible;

use serde::{Deserialize, Serialize};

use super::*;
use crate::workflow::node::tests::{task_node, ChargeInput};
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
    workflow_ctx: WorkflowContext,
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

fn refusal(builder: WorkflowSpecBuilder) -> HorsiesError {
    builder.build().unwrap_err()
}

fn code_of(builder: WorkflowSpecBuilder) -> Option<ErrorCode> {
    refusal(builder).code
}

#[test]
fn a_chain_builds_with_auto_node_ids() {
    let mut builder = WorkflowSpecBuilder::new("daily report");
    builder.definition_key("tests.daily_report.v1");
    let charged = builder.task(charge());
    let reported = builder.task(report().arg_from(total(), charged));
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
    spec.check_output_type::<String>().unwrap();
}

#[test]
fn an_empty_name_is_refused() {
    let mut builder = WorkflowSpecBuilder::new(" ");
    builder.task(charge());
    assert_eq!(code_of(builder), Some(ErrorCode::WorkflowNoName));
}

#[test]
fn a_spec_without_a_node_is_refused() {
    assert_eq!(
        code_of(WorkflowSpecBuilder::new("empty")),
        Some(ErrorCode::WorkflowNoNodes)
    );
}

#[test]
fn more_nodes_than_the_limit_are_refused() {
    let mut builder = WorkflowSpecBuilder::new("wide");
    for _ in 0..=MAX_WORKFLOW_NODES {
        builder.task(charge());
    }
    assert_eq!(code_of(builder), Some(ErrorCode::WorkflowTooManyNodes));
}

#[test]
fn an_invalid_node_id_is_refused() {
    let mut builder = WorkflowSpecBuilder::new("ids");
    builder.task(charge().node_id("has space"));
    assert_eq!(code_of(builder), Some(ErrorCode::WorkflowInvalidNodeId));
    let mut builder = WorkflowSpecBuilder::new("ids");
    builder.task(charge().node_id("x".repeat(MAX_NODE_ID_LENGTH + 1)));
    assert_eq!(code_of(builder), Some(ErrorCode::WorkflowInvalidNodeId));
}

#[test]
fn a_repeated_node_id_is_refused() {
    let mut builder = WorkflowSpecBuilder::new("ids");
    builder.task(charge().node_id("same"));
    builder.task(charge().node_id("same"));
    assert_eq!(code_of(builder), Some(ErrorCode::WorkflowDuplicateNodeId));
}

#[test]
fn a_reference_of_another_builder_is_refused() {
    let mut other = WorkflowSpecBuilder::new("other");
    let foreign = other.task(charge());
    let mut builder = WorkflowSpecBuilder::new("main");
    builder.task(charge());
    builder.task(report().arg_from(total(), foreign));
    assert_eq!(code_of(builder), Some(ErrorCode::WorkflowInvalidDependency));

    let mut builder = WorkflowSpecBuilder::new("main");
    builder.task(charge());
    builder.output(foreign);
    assert_eq!(code_of(builder), Some(ErrorCode::WorkflowInvalidOutput));

    let mut builder = WorkflowSpecBuilder::new("main");
    builder.task(charge());
    builder.success_policy(SuccessPolicy::new(vec![SuccessCase::new([foreign])]));
    assert_eq!(
        code_of(builder),
        Some(ErrorCode::WorkflowInvalidSuccessPolicy)
    );
}

#[test]
fn a_context_source_outside_the_dependencies_is_refused() {
    let mut builder = WorkflowSpecBuilder::new("ctx");
    let charged = builder.task(charge());
    let mut node = task_node::<u32, ContextInput>("summarize").workflow_ctx_from([charged]);
    node.accepts_workflow_ctx = true;
    builder.task(node);
    assert_eq!(code_of(builder), Some(ErrorCode::WorkflowInvalidCtxFrom));
}

#[test]
fn a_context_for_a_task_that_takes_none_is_refused() {
    let mut builder = WorkflowSpecBuilder::new("ctx");
    let charged = builder.task(charge());
    builder.task(
        task_node::<u32, ContextInput>("summarize")
            .waits_for(charged)
            .workflow_ctx_from([charged]),
    );
    let refused = refusal(builder);
    assert!(
        refused.code == Some(ErrorCode::WorkflowCtxParamMissing)
            || refused
                .notes
                .iter()
                .any(|note| note.contains(ErrorCode::WorkflowCtxParamMissing.as_code_str())),
        "{refused}"
    );
}

#[test]
fn a_task_that_takes_the_context_gets_it_in_the_check() {
    let mut builder = WorkflowSpecBuilder::new("ctx");
    let charged = builder.task(charge());
    let mut node = task_node::<u32, ContextInput>("summarize")
        .waits_for(charged)
        .workflow_ctx_from([charged]);
    node.accepts_workflow_ctx = true;
    builder.task(node);
    let spec = builder.build().unwrap();
    assert_eq!(spec.nodes[1].workflow_ctx_from, vec!["ctx:0".to_owned()]);
}

#[test]
fn a_quorum_outside_its_bounds_is_refused() {
    let mut builder = WorkflowSpecBuilder::new("join");
    let charged = builder.task(charge());
    builder.task(charge().waits_for(charged).join_quorum(0));
    assert_eq!(code_of(builder), Some(ErrorCode::WorkflowInvalidJoin));
    let mut builder = WorkflowSpecBuilder::new("join");
    let charged = builder.task(charge());
    builder.task(charge().waits_for(charged).join_quorum(2));
    assert_eq!(code_of(builder), Some(ErrorCode::WorkflowInvalidJoin));
}

#[test]
fn an_any_join_needs_a_default_for_each_result_member() {
    let mut builder = WorkflowSpecBuilder::new("join");
    let charged = builder.task(charge());
    builder.task(report().arg_from(total(), charged).join_any());
    assert_eq!(code_of(builder), Some(ErrorCode::WorkflowInvalidJoin));

    let mut builder = WorkflowSpecBuilder::new("join");
    let charged = builder.task(charge());
    builder.task(
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
        let mut builder = WorkflowSpecBuilder::new("priority");
        builder.task(charge().priority(priority));
        assert_eq!(
            code_of(builder),
            Some(ErrorCode::WorkflowInvalidPriority),
            "{priority}"
        );
    }
    let mut builder = WorkflowSpecBuilder::new("priority");
    builder.task(charge().priority(1));
    builder.build().unwrap();
}

#[test]
fn a_success_policy_without_a_case_or_a_required_node_is_refused() {
    let mut builder = WorkflowSpecBuilder::new("policy");
    builder.task(charge());
    builder.success_policy(SuccessPolicy::new(Vec::new()));
    assert_eq!(
        code_of(builder),
        Some(ErrorCode::WorkflowInvalidSuccessPolicy)
    );
    let mut builder = WorkflowSpecBuilder::new("policy");
    builder.task(charge());
    builder.success_policy(SuccessPolicy::new(vec![SuccessCase::new(
        Vec::<NodeRef>::new(),
    )]));
    assert_eq!(
        code_of(builder),
        Some(ErrorCode::WorkflowInvalidSuccessPolicy)
    );
}

#[test]
fn a_success_policy_lowers_to_indexes() {
    let mut builder = WorkflowSpecBuilder::new("policy");
    let first = builder.task(charge());
    let second = builder.task(charge());
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
    let mut builder = WorkflowSpecBuilder::new("overlap");
    let charged = builder.task(charge());
    builder.task(
        report()
            .input_json(r#"{"title": "daily", "total": 1}"#)
            .unwrap()
            .arg_from(total(), charged),
    );
    assert_eq!(
        code_of(builder),
        Some(ErrorCode::WorkflowKwargsArgsFromOverlap)
    );
}

/// #571: a member that binds to no field of the input type.
#[test]
fn a_member_that_binds_to_no_field_is_refused() {
    let mut builder = WorkflowSpecBuilder::new("keys");
    builder.task(
        task_node::<f64, ChargeInput>("charge")
            .input_json(r#"{"order_id": 1, "amount": 2.0, "amonut": 3}"#)
            .unwrap(),
    );
    assert_eq!(code_of(builder), Some(ErrorCode::WorkflowInvalidKwargKey));
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
    let mut builder = WorkflowSpecBuilder::new("ship");
    builder.task(
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
    assert_eq!(refused.code, Some(ErrorCode::WorkflowInvalidKwargKey));
    assert!(refused.message.contains("lable"), "{refused}");
}

/// #571: a key inside a nested object that binds to no field, named by its
/// key path.
#[test]
fn a_nested_member_that_binds_to_no_field_is_refused_with_its_key_path() {
    let refused = ship_refusal(r#"{"label": "a", "options": {"retriess": 3}, "parcels": []}"#);
    assert_eq!(refused.code, Some(ErrorCode::WorkflowInvalidKwargKey));
    assert!(refused.message.contains("options.retriess"), "{refused}");
}

/// #571: a key inside an element of a list that binds to no field, named by
/// its key path.
#[test]
fn a_list_element_member_that_binds_to_no_field_is_refused_with_its_key_path() {
    let refused =
        ship_refusal(r#"{"label": "a", "options": {}, "parcels": [{"weight": 1, "wieght": 2}]}"#);
    assert_eq!(refused.code, Some(ErrorCode::WorkflowInvalidKwargKey));
    assert!(refused.message.contains("parcels[0].wieght"), "{refused}");
}

/// #572: a node whose static input misses a field of its input type.
#[test]
fn a_missing_field_is_refused() {
    let mut builder = WorkflowSpecBuilder::new("missing");
    builder.task(task_node::<f64, ChargeInput>("charge"));
    assert_eq!(
        code_of(builder),
        Some(ErrorCode::WorkflowMissingRequiredParams)
    );
    let mut builder = WorkflowSpecBuilder::new("missing");
    let charged = builder.task(charge());
    builder.task(task_node::<String, ReportInput>("report").arg_from(total(), charged));
    assert_eq!(
        code_of(builder),
        Some(ErrorCode::WorkflowMissingRequiredParams)
    );
}

#[test]
fn a_member_of_another_type_is_refused() {
    let mut builder = WorkflowSpecBuilder::new("types");
    builder.task(
        task_node::<f64, ChargeInput>("charge")
            .input_json(r#"{"order_id": "seven", "amount": 2.0}"#)
            .unwrap(),
    );
    assert_eq!(
        code_of(builder),
        Some(ErrorCode::WorkflowKwargsNotSerializable)
    );
}

#[test]
fn a_task_with_no_parameters_takes_an_empty_input() {
    let mut builder = WorkflowSpecBuilder::new("unit");
    builder.task(task_node::<u32, ()>("tick"));
    let spec = builder.build().unwrap();
    let SpecBody::Task { input, .. } = &spec.nodes[0].body else {
        panic!("a task node");
    };
    assert_eq!(input.to_text(), "{}");
}

#[test]
fn every_refusal_of_the_second_gate_is_reported() {
    let mut builder = WorkflowSpecBuilder::new("many");
    let charged = builder.task(charge().priority(0));
    builder.task(charge().waits_for(charged).join_quorum(5));
    let refused = refusal(builder);
    assert_eq!(refused.code, None, "{refused}");
    assert_eq!(refused.notes.len(), 2, "{refused}");
}

/// #575: the output type of a spec is the type of its output node, or
/// `OutputlessResults` for a spec with no output node.
#[test]
fn the_output_type_is_checked() {
    let mut builder = WorkflowSpecBuilder::new("out");
    let charged = builder.task(charge());
    builder.output(charged);
    let spec = builder.build().unwrap();
    spec.check_output_type::<f64>().unwrap();
    assert_eq!(
        spec.check_output_type::<String>().unwrap_err().code,
        Some(ErrorCode::WorkflowOutputTypeMismatch)
    );
    assert_eq!(
        spec.check_output_type::<OutputlessResults>()
            .unwrap_err()
            .code,
        Some(ErrorCode::WorkflowOutputTypeMismatch)
    );

    let mut builder = WorkflowSpecBuilder::new("no output");
    builder.task(charge());
    let spec = builder.build().unwrap();
    spec.check_output_type::<OutputlessResults>().unwrap();
    assert_eq!(
        spec.check_output_type::<f64>().unwrap_err().code,
        Some(ErrorCode::WorkflowOutputTypeMismatch)
    );
}

#[test]
fn a_key_of_another_builder_is_none() {
    let mut other = WorkflowSpecBuilder::new("other");
    let foreign = other.task(charge());
    let mut builder = WorkflowSpecBuilder::new("main");
    builder.task(charge());
    let spec = builder.build().unwrap();
    assert!(spec.key(foreign).is_none());
}

#[test]
fn slugify_keeps_the_node_id_characters() {
    assert_eq!(slugify("daily report"), "daily_report");
    assert_eq!(slugify("a/b*c"), "abc");
    assert_eq!(slugify("***"), "_");
    assert_eq!(slugify("v1.2:x-y"), "v1.2:x-y");
}
