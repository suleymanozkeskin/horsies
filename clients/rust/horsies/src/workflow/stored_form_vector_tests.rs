//! The workflow spec data vectors
//! (`crates/horsies-core/tests/fixtures/stored_forms/workflow_spec_data.json`)
//! through the client's DSL and `spec_data_text`: a spec built with the DSL
//! gives Python's text, or the vector's recorded `rust_drift`. The vector
//! `members_of_every_kind` (args_from, workflow context, encoded options, a
//! sub-workflow node) is read by the core's test; the DSL writes those
//! members with the same keys in the same order (`spec_data.rs`).

use serde::{Deserialize, Serialize};

use super::node::tests::{task_node, ChargeInput};
use super::spec_data::spec_data_text;
use super::{SuccessCase, SuccessPolicy, WorkflowSpec, WorkflowSpecBuilder};
use crate::stored_form_vectors::{vector_file, RustCase, RustDrift};
use crate::{AppConfig, CustomQueueConfig, OnError, QueueMode};

#[derive(Deserialize)]
struct SpecDataForm {
    vectors: Vec<SpecVector>,
}

#[derive(Deserialize)]
struct SpecVector {
    name: String,
    text: String,
    rust_drift: Option<RustDrift>,
}

fn vector(name: &str) -> SpecVector {
    let form: SpecDataForm = serde_json::from_str(&vector_file("workflow_spec_data.json"))
        .expect("workflow_spec_data.json parses");
    form.vectors
        .into_iter()
        .find(|vector| vector.name == name)
        .unwrap_or_else(|| panic!("workflow_spec_data.json has no vector {name}"))
}

fn config() -> AppConfig {
    AppConfig::for_database_url("postgresql://localhost/horsies")
}

/// The node of `app.charge` with the input `{"order_id":7,"amount":9.5}`.
fn charge() -> crate::TaskNode<f64, ChargeInput> {
    task_node::<f64, ChargeInput>("app.charge")
        .set_input(ChargeInput {
            order_id: 7,
            amount: 9.5,
        })
        .unwrap()
}

/// The input of a task that takes one text.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NoteInput {
    note: String,
}

/// The client's text of `spec` against the vector `name`.
fn assert_vector(name: &str, spec: &WorkflowSpec, config: &AppConfig) {
    let vector = vector(name);
    let case = RustCase::of(name, None, vector.rust_drift.clone());
    let expected = case
        .expected_text(&vector.text)
        .expect("the vector is expressible");
    let written = spec_data_text(spec, config).unwrap_or_else(|error| panic!("{name}: {error}"));
    assert_eq!(written, expected, "{name}");
}

#[test]
fn one_task_node() {
    let mut builder = WorkflowSpecBuilder::new("checkout");
    builder.definition_key("tests.checkout.v1");
    let node = builder.task(charge().node_id("charge"));
    builder.output(node);
    let spec = builder.build().unwrap();
    assert_vector("one_task_node", &spec, &config());
}

#[test]
fn two_nodes_with_a_policy_and_a_quorum() {
    let mut builder = WorkflowSpecBuilder::new("checkout");
    builder.definition_key("tests.checkout.v1");
    builder.on_error(OnError::Pause);
    let first = builder.task(charge().node_id("first"));
    let second = builder.task(
        charge()
            .node_id("second")
            .waits_for(first)
            .join_quorum(1)
            .priority(3),
    );
    builder.output(second);
    builder.success_policy(SuccessPolicy::new(vec![SuccessCase::new([second])]).optional([first]));
    let spec = builder.build().unwrap();
    assert_vector("two_nodes_with_a_policy_and_a_quorum", &spec, &config());
}

#[test]
fn queues_in_first_seen_order() {
    let custom = AppConfig {
        queue_mode: QueueMode::Custom,
        custom_queues: Some(vec![
            CustomQueueConfig {
                name: "zeta".to_owned(),
                priority: 10,
                max_concurrency: Some(5),
            },
            CustomQueueConfig {
                name: "alpha".to_owned(),
                priority: 20,
                max_concurrency: Some(5),
            },
        ]),
        ..config()
    };
    let mut builder = WorkflowSpecBuilder::new("queues");
    builder.definition_key("tests.queues.v1");
    let _first_seen = builder.task(charge().node_id("a").queue("zeta"));
    let _second_seen = builder.task(charge().node_id("b").queue("alpha"));
    let spec = builder.build().unwrap();
    assert_vector("queues_in_first_seen_order", &spec, &custom);
}

/// Text outside ASCII in the name, the definition key, the task name and
/// the node input is written as the characters, as Python's `dumps_json`
/// writes them.
#[test]
fn non_ascii_text() {
    let mut builder = WorkflowSpecBuilder::new("Ünïcode ✓ 🐎");
    builder.definition_key("tests.ünïcode.v1");
    let node = builder.task(
        task_node::<f64, NoteInput>("app.Ünïcode")
            .set_input(NoteInput {
                note: "Ünïcode ✓ 🐎".to_owned(),
            })
            .unwrap()
            .node_id("note"),
    );
    builder.output(node);
    let spec = builder.build().unwrap();
    assert_vector("non_ascii_text", &spec, &config());
}
