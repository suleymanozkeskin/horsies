//! The task options vectors (`task_options.json`) through the client.
//!
//! The client's send stores `send_options_text` (`send/function.rs`): the
//! task's options text, with the send's deadline as `good_until`; a workflow
//! node stores its task's options with its deadline merged as `good_until`
//! (`workflow/spec_data.rs`, `task_options_text`).

use chrono::{DateTime, Utc};
use horsies_core::{
    BackoffStrategy, BuiltInTaskCode, Execution, RetryPolicy, TaskErrorCode, TaskOptions, UserCode,
};
use serde::Deserialize;
use serde_json::Value;

use super::{vector_file, RustCase, RustDrift};
use crate::send::send_options_text;
use crate::workflow::{task_options_text, NodeDeadline};

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
    rust_drift: Option<RustDrift>,
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

/// The `TaskOptions` of a vector's options, as a task of the attribute
/// holds them.
fn task_options(vector: &OptionsVector) -> TaskOptions {
    let name = vector.name.as_str();
    let options = &vector.options;
    let (retry_policy, retry_for) = match &options["retry_policy"] {
        Value::Null => (None, None),
        given => {
            let backoff_strategy = match given["backoff_strategy"].as_str() {
                Some("fixed") => BackoffStrategy::Fixed,
                Some("exponential") => BackoffStrategy::Exponential,
                other => panic!("{name}: backoff_strategy {other:?}"),
            };
            let codes = given["retry_for"]
                .as_array()
                .unwrap_or_else(|| panic!("{name}: retry_for is a list"))
                .iter()
                .map(|entry| {
                    let text = entry.as_str().expect("a code text");
                    match BuiltInTaskCode::parse(text) {
                        Some(built_in) => TaskErrorCode::BuiltIn(built_in),
                        None => TaskErrorCode::User(UserCode::parse(text).expect("a user code")),
                    }
                })
                .collect();
            let policy = RetryPolicy {
                max_retries: whole_number(name, &given["max_retries"]).expect("max_retries"),
                intervals_seconds: given["intervals_seconds"]
                    .as_array()
                    .unwrap_or_else(|| panic!("{name}: intervals_seconds is a list"))
                    .iter()
                    .map(|interval| whole_number(name, interval).expect("an interval"))
                    .collect(),
                backoff_strategy,
                jitter: given["jitter"].as_bool().expect("jitter is a bool"),
                max_delay_seconds: whole_number(name, &given["max_delay_seconds"]),
            };
            (Some(policy), Some(codes))
        }
    };
    let execution = match options["execution"].as_str() {
        Some("ISOLATED") => Execution::Isolated,
        Some("SHARED_ASYNC") => Execution::SharedAsync,
        Some("SHARED_THREAD") => Execution::SharedThread,
        other => panic!("{name}: execution {other:?}"),
    };
    TaskOptions {
        task_name: "stored_form_vector".to_owned(),
        queue: None,
        good_until: None,
        retry_for,
        retry_policy,
        timeout_ms: whole_number(name, &options["timeout_ms"]),
        execution,
        cancel_grace_ms: whole_number(name, &options["cancel_grace_ms"]),
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
fn each_options_set_is_stored_as_its_text() {
    for vector in form().vectors {
        let name = vector.name.as_str();
        let case = RustCase::of(name, None, vector.rust_drift.clone());
        let expected = case
            .expected_text(&vector.text)
            .expect("every options set is expressible");
        let options = task_options(&vector);
        let task_text = options.stored_options_text();
        let written = match vector.kind.as_str() {
            "task" => task_text,
            "send" => {
                send_options_text(&options.task_name, Some(&options), Some(deadline(&vector)))
                    .unwrap_or_else(|| panic!("{name}: a send with a deadline stores options"))
            }
            "node" => task_options_text(
                Some(task_text.as_str()),
                NodeDeadline::At(deadline(&vector)),
            )
            .unwrap_or_else(|cause| panic!("{name}: {cause}"))
            .unwrap_or_else(|| panic!("{name}: a node with a deadline stores options")),
            other => panic!("{name}: unknown kind {other}"),
        };
        assert_eq!(written, expected, "{name}");
    }
}
