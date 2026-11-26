//! The result envelope vectors (`tests/fixtures/stored_forms/result_envelope.json`)
//! through the child host's envelope writer (`run::result_end`): the text of
//! a task body's ok value goes in the `ok` slot as it is, and a task error
//! is written through the codec (`error_envelope`). The error data vectors
//! of the refusals of stored options (`error_data.json`) through
//! `run::refusal_end`.

use super::run::{refusal_end, result_end};
use crate::core::task::result::{read_task_result_text, TaskResultText};
use crate::core::TaskResult;
use crate::stored_form_vectors::error_data::{vector, ErrorVector};
use crate::stored_form_vectors::result_envelope::form;
use crate::stored_form_vectors::task_error_of;
use crate::worker::executor::protocol::{RefusalReason, RunEnd, StoredOptionsRefusal};

#[test]
fn the_child_host_writes_each_envelope_text() {
    for vector in form().vectors {
        let name = vector.name.as_str();
        let body_result = match &vector.err {
            Some(notation) => TaskResult::Err(task_error_of(name, notation, None)),
            None => match read_task_result_text(&vector.text) {
                Ok(TaskResultText::Ok(raw)) => TaskResult::Ok(raw.get().as_bytes().to_vec()),
                other => panic!("{name}: Python's ok envelope does not read: {other:?}"),
            },
        };
        match result_end(body_result) {
            RunEnd::Result(envelope) => assert_eq!(envelope.as_str(), vector.text, "{name}"),
            other => panic!("{name}: the child host ended with {other:?}"),
        }
    }
}

/// The text input `key` of `vector`.
fn input_text<'a>(vector: &'a ErrorVector, key: &str) -> &'a str {
    vector.inputs[key]
        .as_str()
        .unwrap_or_else(|| panic!("{}: no text input {key}", vector.name))
}

/// The refusal of the inputs of `vector`: its variant name and its reason.
fn refusal_of(vector: &ErrorVector) -> StoredOptionsRefusal {
    let reason = RefusalReason::cut(input_text(vector, "reason"));
    match input_text(vector, "refusal") {
        "OptionsUnreadable" => StoredOptionsRefusal::OptionsUnreadable(reason),
        "TimeoutValueInvalid" => StoredOptionsRefusal::TimeoutValueInvalid(reason),
        "CancelGraceInvalid" => StoredOptionsRefusal::CancelGraceInvalid(reason),
        other => panic!("{}: unknown refusal {other}", vector.name),
    }
}

/// Each refusal of stored options fails the task with Python's text: the
/// reason as the message, and `data` with the task name, the stored field
/// and, for a timeout or a grace, its bounds.
#[test]
fn the_child_host_writes_python_s_text_for_each_stored_options_refusal() {
    for name in [
        "worker_serialization_error_stored_options_unreadable",
        "worker_serialization_error_stored_timeout_invalid",
        "worker_serialization_error_stored_cancel_grace_invalid",
    ] {
        let vector = vector(name);
        let end = refusal_end(input_text(&vector, "task_name"), &refusal_of(&vector));
        match end {
            RunEnd::Result(envelope) => assert_eq!(envelope.as_str(), vector.text, "{name}"),
            other => panic!("{name}: the child host ended with {other:?}"),
        }
    }
}
