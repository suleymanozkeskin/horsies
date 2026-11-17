//! The task options vectors (`task_options.json`) through the core.
//!
//! The core writes the options text of a task (`TaskOptions::stored_options_text`)
//! and of a workflow node with its deadline merged
//! (`merge_good_until_into_options`). It reads every text at enqueue
//! (`prepare_enqueue_facts`: the execution class; `stored_max_retries`) and
//! at dispatch (`read_stored_options`: the timeout). A send's options with
//! its deadline are written by a client: the Rust client's text is a
//! `rust_drift` of the vector, tested by the client.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;

use super::vector_file;
use crate::core::history::enqueue::prepare_enqueue_facts;
use crate::core::task::{
    BackoffStrategy, BuiltInTaskCode, CancelGraceMs, Execution, RetryPolicy, TaskErrorCode,
    TaskOptions, UserCode,
};
use crate::worker::retry::{read_stored_options, stored_max_retries, RunPermit};
use crate::workflow_engine::merge_good_until_into_options;

#[derive(Deserialize)]
struct TaskOptionsForm {
    vectors: Vec<OptionsVector>,
}

#[derive(Deserialize)]
struct OptionsVector {
    name: String,
    kind: String,
    options: Value,
    deadline: Option<String>,
    text: String,
}

fn form() -> TaskOptionsForm {
    serde_json::from_str(&vector_file("task_options.json")).expect("task_options.json parses")
}

fn whole_number(name: &str, value: &Value) -> Option<u32> {
    match value {
        Value::Null => None,
        given => Some(
            given
                .as_u64()
                .and_then(|number| u32::try_from(number).ok())
                .unwrap_or_else(|| panic!("{name}: {given} is not a u32")),
        ),
    }
}

fn execution(name: &str, options: &Value) -> Execution {
    match options["execution"].as_str() {
        Some("ISOLATED") => Execution::Isolated,
        Some("SHARED_ASYNC") => Execution::SharedAsync,
        Some("SHARED_THREAD") => Execution::SharedThread,
        other => panic!("{name}: execution {other:?}"),
    }
}

fn code(name: &str, text: &str) -> TaskErrorCode {
    match BuiltInTaskCode::parse(text) {
        Some(built_in) => TaskErrorCode::BuiltIn(built_in),
        None => TaskErrorCode::User(
            UserCode::parse(text).unwrap_or_else(|refused| panic!("{name}: {refused}")),
        ),
    }
}

/// The core's `TaskOptions` of a vector's options.
fn core_options(vector: &OptionsVector) -> TaskOptions {
    let name = vector.name.as_str();
    let options = &vector.options;
    let policy = &options["retry_policy"];
    let (retry_policy, auto_retry_for) = match policy {
        Value::Null => (None, None),
        given => {
            let backoff_strategy = match given["backoff_strategy"].as_str() {
                Some("fixed") => BackoffStrategy::Fixed,
                Some("exponential") => BackoffStrategy::Exponential,
                other => panic!("{name}: backoff_strategy {other:?}"),
            };
            let intervals = given["intervals"]
                .as_array()
                .unwrap_or_else(|| panic!("{name}: intervals is a list"))
                .iter()
                .map(|interval| whole_number(name, interval).expect("an interval"))
                .collect();
            let codes = given["auto_retry_for"]
                .as_array()
                .unwrap_or_else(|| panic!("{name}: auto_retry_for is a list"))
                .iter()
                .map(|entry| code(name, entry.as_str().expect("a code text")))
                .collect();
            (
                Some(RetryPolicy {
                    max_retries: whole_number(name, &given["max_retries"]).expect("max_retries"),
                    intervals,
                    backoff_strategy,
                    jitter: given["jitter"].as_bool().expect("jitter is a bool"),
                    max_delay_seconds: whole_number(name, &given["max_delay_seconds"]),
                }),
                Some(codes),
            )
        }
    };
    TaskOptions {
        task_name: "stored_form_vector".to_owned(),
        queue_name: None,
        good_until: None,
        auto_retry_for,
        retry_policy,
        timeout_ms: whole_number(name, &options["timeout_ms"]),
        execution: execution(name, options),
        cancel_grace_ms: whole_number(name, &options["cancel_grace_ms"])
            .map(|grace| CancelGraceMs::new(grace).expect("a grace in its bounds")),
    }
}

fn deadline(vector: &OptionsVector) -> DateTime<Utc> {
    let text = vector
        .deadline
        .as_deref()
        .unwrap_or_else(|| panic!("{}: a deadline", vector.name));
    DateTime::parse_from_rfc3339(text)
        .unwrap_or_else(|error| panic!("{}: {error}", vector.name))
        .with_timezone(&Utc)
}

#[test]
fn each_task_and_node_options_set_encodes_to_its_text() {
    for vector in form().vectors {
        let name = vector.name.as_str();
        let task_text = core_options(&vector).stored_options_text();
        match vector.kind.as_str() {
            "task" => assert_eq!(task_text, vector.text, "{name}"),
            "node" => assert_eq!(
                merge_good_until_into_options(
                    0,
                    name,
                    Some(deadline(&vector)),
                    Some(task_text.as_str())
                )
                .unwrap_or_else(|error| panic!("{name}: {error}")),
                Some(vector.text.clone()),
                "{name}"
            ),
            "send" => {}
            other => panic!("{name}: unknown kind {other}"),
        }
    }
}

#[test]
fn each_text_reads_back_at_enqueue_and_dispatch() {
    for vector in form().vectors {
        let name = vector.name.as_str();
        let expected = core_options(&vector);
        let facts = prepare_enqueue_facts(
            "stored_form_vector",
            "default",
            100,
            "{}",
            None,
            None,
            Some(vector.text.as_str()),
            None,
            false,
            None,
        )
        .unwrap_or_else(|refused| panic!("{name}: the send refused the options: {refused}"));
        assert_eq!(facts.execution, expected.execution, "{name}");
        let max_retries = expected.retry_policy.as_ref().map_or(0, |policy| {
            i32::try_from(policy.max_retries).expect("at most 20")
        });
        assert_eq!(
            stored_max_retries(Some(vector.text.as_str())).expect("readable options"),
            max_retries,
            "{name}"
        );
        let permit = read_stored_options(Some(vector.text.as_str())).run_permit();
        match expected.timeout_ms {
            None => assert!(
                matches!(permit, RunPermit::RunWithoutTimeout),
                "{name}: {permit:?}"
            ),
            Some(timeout) => assert!(
                matches!(permit, RunPermit::RunWithTimeout(limit) if limit == Duration::from_millis(u64::from(timeout))),
                "{name}: {permit:?}"
            ),
        }
    }
}
