//! The result envelope vectors (`tests/fixtures/stored_forms/result_envelope.json`)
//! through the child host's envelope writer (`run::result_end`): the text of
//! a task body's ok value goes in the `ok` slot as it is, and a task error
//! is written through the codec (`error_envelope`).

use super::run::result_end;
use crate::core::task::result::{read_task_result_text, TaskResultText};
use crate::core::TaskResult;
use crate::stored_form_vectors::result_envelope::form;
use crate::stored_form_vectors::task_error_of;
use crate::worker::executor::protocol::RunEnd;

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
