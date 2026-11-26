//! A built spec as the core's spec data: the facts that the workflow row and
//! the node rows store, the same data that the Python client sends
//! (`to_workflow_spec_data`).
//!
//! The core's spec data types are built only by its parser, so the client
//! writes the JSON document and the parser reads it back: every bound (the
//! node indexes, the priority, the class keys, each node at its index) is
//! checked once, by the core. The app's config gives what the spec does not
//! hold: the queue of each task node is checked against the app's queues,
//! a node without its own priority takes its queue's priority, and each task
//! queue takes the retention class of the app's queue mapping.

use horsies_core::core::python_text::python_isoformat_utc;
use horsies_core::{
    parse_workflow_spec_data, ClientSpec, OnError, SpecDataRefused, WORKFLOW_SPEC_DATA_VERSION,
};
use serde_json::{json, Map, Value};

use super::input::NodeInput;
use super::node::{JoinType, NodeDeadline, NodePriority};
use super::spec::{SpecBody, SpecNode, SpecOutput, WorkflowSpec};
use super::sub_workflow::NodeParams;
use crate::registry::{check_queue, queue_priority};
use crate::{AppConfig, HorsiesErrorCode, HorsiesError};

/// The member of a task node's options that holds its deadline.
const GOOD_UNTIL_MEMBER: &str = "good_until";

/// The spec data of `spec` in the app of `config`, read by the core.
///
/// Pure. Errors: `WORKFLOW_UNRESOLVED_QUEUE` (HRS-014) for a task node on a
/// queue that the app does not admit; a refusal of the core's parser, with
/// its field path, for a fact outside the bounds of the spec data (a
/// configured queue priority outside 1 to 100).
pub(crate) fn client_spec(
    spec: &WorkflowSpec,
    config: &AppConfig,
) -> Result<ClientSpec, HorsiesError> {
    let text = spec_data_text(spec, config)?;
    parse_workflow_spec_data(&text).map_err(|refused| data_refused(spec, &refused))
}

/// The spec data text of `spec` in the app of `config`: the document that
/// the core's parser reads (`clients/python/tests/fixtures/stored_forms/workflow_spec_data.json`
/// of the core pins it). Pure. Errors: `WORKFLOW_UNRESOLVED_QUEUE` (HRS-014)
/// for a task node on a queue that the app does not admit; a task options
/// text that does not take the node's deadline.
pub(crate) fn spec_data_text(
    spec: &WorkflowSpec,
    config: &AppConfig,
) -> Result<String, HorsiesError> {
    // The class of each task queue, in the order of the nodes that first
    // name it (a Python dict): a later node on a queue keeps its place.
    let mut queue_classes: Map<String, Value> = Map::new();
    let mut nodes = Vec::with_capacity(spec.nodes.len());
    for node in &spec.nodes {
        if let SpecBody::Task { queue, .. } = &node.body {
            check_queue(config, queue).map_err(|refused| unresolved_queue(spec, node, &refused))?;
            let _previous = queue_classes.insert(
                queue.clone(),
                Value::String(config.retention.resolve_queue_retention(queue).class_key()),
            );
        }
        nodes.push(node_document(spec, node, config)?);
    }
    let document = json!({
        "data_version": WORKFLOW_SPEC_DATA_VERSION,
        "name": spec.name,
        "definition_key": match &spec.definition_key {
            Some(key) => json!({"kind": "KEYED", "key": key}),
            None => json!({"kind": "UNKEYED"}),
        },
        "on_error": match spec.on_error {
            OnError::Fail => "FAIL",
            OnError::Pause => "PAUSE",
        },
        "output": match spec.output {
            SpecOutput::Node { index, .. } => json!({"kind": "NODE", "index": index}),
            SpecOutput::NoOutput => json!({"kind": "NO_OUTPUT"}),
        },
        "success_policy": match &spec.success_policy {
            Some(policy) => json!({
                "kind": "CASES",
                "cases": policy
                    .cases
                    .iter()
                    .map(|required| json!({"required": required}))
                    .collect::<Vec<Value>>(),
                "optional": policy.optional,
            }),
            None => json!({"kind": "NO_POLICY"}),
        },
        "queue_classes": queue_classes,
        "nodes": nodes,
    });
    Ok(document.to_string())
}

/// The document of one node.
fn node_document(
    spec: &WorkflowSpec,
    node: &SpecNode,
    config: &AppConfig,
) -> Result<Value, HorsiesError> {
    let (input_json, body) = match &node.body {
        SpecBody::Task {
            input,
            queue,
            priority,
            task_options,
            deadline,
        } => {
            let priority = match priority {
                NodePriority::OfQueue => queue_priority(config, queue),
                NodePriority::Given(given) => *given,
            };
            let options = match task_options_text(task_options.as_deref(), *deadline) {
                Ok(Some(json)) => json!({"kind": "ENCODED", "json": json}),
                Ok(None) => json!({"kind": "NO_OPTIONS"}),
                Err(cause) => {
                    return Err(HorsiesError::new(format!(
                        "workflow '{}' node '{}': the task options do not take the deadline: \
                         {cause}",
                        spec.name, node.node_id
                    )))
                }
            };
            (
                input.to_text(),
                json!({
                    "kind": "TASK",
                    "queue": queue,
                    "priority": priority,
                    "task_options": options,
                }),
            )
        }
        SpecBody::SubWorkflow {
            definition_key,
            params,
        } => {
            let (input_json, given) = match params {
                NodeParams::NotGiven => (NodeInput::empty().to_text(), "NOT_GIVEN"),
                NodeParams::Given(input) => (input.to_text(), "GIVEN"),
            };
            (
                input_json,
                json!({
                    "kind": "SUB_WORKFLOW",
                    "definition_key": definition_key,
                    "params": given,
                }),
            )
        }
    };
    Ok(json!({
        "index": node.index,
        "node_id": node.node_id,
        "task_name": node.name,
        "input_json": input_json,
        "dependencies": node.dependencies,
        "args_from": node.args_from,
        "workflow_context_from": node.workflow_context_from,
        "allow_failed_dependencies": node.allow_failed_dependencies,
        "join": match node.join {
            JoinType::All => json!({"kind": "ALL"}),
            JoinType::Any => json!({"kind": "ANY"}),
            JoinType::Quorum { min_success } => {
                json!({"kind": "QUORUM", "min_success": min_success})
            }
        },
        "body": body,
    }))
}

/// The stored options text of a task node: the task's options with the
/// node's deadline as the member `good_until`, the members in their order.
/// The deadline is written as Python's `isoformat()` writes an aware UTC
/// time: `+00:00`, and 6 fraction digits when the microsecond is not 0.
/// `None` for a task with no options and a node with no deadline. Pure.
///
/// Errors: the cause, for an options text that is not a JSON object.
pub(crate) fn task_options_text(
    options: Option<&str>,
    deadline: NodeDeadline,
) -> Result<Option<String>, String> {
    let at = match deadline {
        NodeDeadline::NoDeadline => return Ok(options.map(str::to_owned)),
        NodeDeadline::At(at) => at,
    };
    let mut members = match options {
        Some(text) => NodeInput::from_object_text(text).map_err(|refused| refused.to_string())?,
        None => NodeInput::empty(),
    };
    let value = serde_json::value::RawValue::from_string(
        serde_json::to_string(&python_isoformat_utc(at)).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    members
        .set_member(GOOD_UNTIL_MEMBER, value)
        .map_err(|refused| refused.to_string())?;
    Ok(Some(members.to_text()))
}

fn unresolved_queue(spec: &WorkflowSpec, node: &SpecNode, refused: &HorsiesError) -> HorsiesError {
    HorsiesError::new(format!(
        "workflow '{}' node '{}' (task '{}') has a queue that the app does not admit",
        spec.name, node.node_id, node.name
    ))
    .with_code(HorsiesErrorCode::WorkflowUnresolvedQueue)
    .with_note(refused.message.clone())
    .with_help("name a queue of the app, or leave the node on the task's queue")
}

fn data_refused(spec: &WorkflowSpec, refused: &SpecDataRefused) -> HorsiesError {
    HorsiesError::new(format!(
        "the core refused the spec data of workflow '{}': {refused}",
        spec.name
    ))
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::workflow::node::tests::{add, task_node, test_builder, ChargeInput};
    use crate::workflow::{SuccessCase, SuccessPolicy};
    use crate::{CustomQueueConfig, QueueMode};

    fn config() -> AppConfig {
        AppConfig::for_database_url("postgresql://localhost/horsies")
    }

    fn charge() -> crate::TaskNode<f64, ChargeInput> {
        task_node::<f64, ChargeInput>("charge")
            .set_input(ChargeInput {
                order_id: 7,
                amount: 9.5,
            })
            .unwrap()
    }

    #[test]
    fn a_spec_reads_back_as_core_spec_data() {
        let mut builder = test_builder("checkout");
        builder.definition_key("tests.checkout.v1");
        builder.on_error(OnError::Pause);
        let first = add(&mut builder, charge().node_id("first"));
        let second = add(
            &mut builder,
            charge().waits_for(first).join_quorum(1).priority(3),
        );
        builder.output(second);
        builder
            .success_policy(SuccessPolicy::new(vec![SuccessCase::new([second])]).optional([first]));
        let spec = builder.build().unwrap();
        let client = client_spec(&spec, &config()).unwrap();
        let core = client.spec();
        assert_eq!(core.name, "checkout");
        assert_eq!(core.definition_key.as_deref(), Some("tests.checkout.v1"));
        assert_eq!(core.on_error, OnError::Pause);
        assert_eq!(core.output_index, Some(1));
        assert_eq!(core.tasks.len(), 2);
        assert_eq!(core.tasks[0].node_id.as_deref(), Some("first"));
        assert_eq!(core.tasks[0].input_json, r#"{"order_id":7,"amount":9.5}"#);
        assert_eq!(core.tasks[0].priority, Some(100));
        assert_eq!(core.tasks[1].priority, Some(3));
        assert_eq!(core.tasks[1].dependencies, vec![0]);
        assert_eq!(core.tasks[1].min_success, Some(1));
        let policy = core.success_policy.as_ref().unwrap();
        assert_eq!(policy.cases[0].required_indices, vec![1]);
        assert_eq!(policy.optional_indices, Some(vec![0]));
    }

    #[test]
    fn a_deadline_joins_the_task_options() {
        let deadline = Utc.with_ymd_and_hms(2026, 11, 18, 0, 0, 0).unwrap();
        assert_eq!(
            task_options_text(None, NodeDeadline::At(deadline)).unwrap(),
            Some(r#"{"good_until":"2026-11-18T00:00:00+00:00"}"#.to_owned())
        );
        assert_eq!(
            task_options_text(Some(r#"{"max_retries": 2}"#), NodeDeadline::At(deadline)).unwrap(),
            Some(r#"{"max_retries":2,"good_until":"2026-11-18T00:00:00+00:00"}"#.to_owned())
        );
        assert_eq!(
            task_options_text(Some(r#"{"max_retries": 2}"#), NodeDeadline::NoDeadline).unwrap(),
            Some(r#"{"max_retries": 2}"#.to_owned())
        );
        assert_eq!(
            task_options_text(None, NodeDeadline::NoDeadline).unwrap(),
            None
        );
        let whole_milliseconds = deadline + chrono::Duration::milliseconds(500);
        assert_eq!(
            task_options_text(None, NodeDeadline::At(whole_milliseconds)).unwrap(),
            Some(r#"{"good_until":"2026-11-18T00:00:00.500000+00:00"}"#.to_owned())
        );
    }

    #[test]
    fn a_queue_that_the_app_does_not_admit_is_refused() {
        let mut builder = test_builder("queues");
        add(&mut builder, charge().queue("bulk"));
        let spec = builder.build().unwrap();
        let refused = client_spec(&spec, &config()).unwrap_err();
        assert_eq!(refused.code, Some(HorsiesErrorCode::WorkflowUnresolvedQueue));
    }

    #[test]
    fn a_node_on_another_queue_takes_its_priority_and_class() {
        let custom = AppConfig {
            queue_mode: QueueMode::Custom,
            custom_queues: Some(vec![
                CustomQueueConfig {
                    name: "default".to_owned(),
                    priority: 50,
                    max_concurrency: Some(5),
                },
                CustomQueueConfig {
                    name: "bulk".to_owned(),
                    priority: 90,
                    max_concurrency: Some(5),
                },
            ]),
            ..config()
        };
        let mut builder = test_builder("queues");
        add(&mut builder, charge().queue("bulk"));
        add(&mut builder, charge().queue("bulk").priority(1));
        let spec = builder.build().unwrap();
        let client = client_spec(&spec, &custom).unwrap();
        assert_eq!(client.spec().tasks[0].queue.as_deref(), Some("bulk"));
        assert_eq!(client.spec().tasks[0].priority, Some(90));
        // A node's own priority is kept on another queue.
        assert_eq!(client.spec().tasks[1].queue.as_deref(), Some("bulk"));
        assert_eq!(client.spec().tasks[1].priority, Some(1));
    }
}
