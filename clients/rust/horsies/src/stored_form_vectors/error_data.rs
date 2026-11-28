//! The error data vectors (`error_data.json`) through the client's read.
//!
//! The core writes every library error; the client reads it. A stored
//! envelope reads through `outcome_of_record` into a `TaskError` whose stored
//! form is the vector's error. An error object (the workflow error column)
//! reads through the core's `TaskError` read.

use horsies_core::{CustomCode, TaskStatus};
use serde::Deserialize;
use serde_json::Value;

use super::{record, stored_error, vector_file};
use crate::handle::outcome_of_record;
use crate::TaskOutcome;

#[derive(Deserialize)]
struct ErrorDataForm {
    vectors: Vec<ErrorVector>,
}

#[derive(Deserialize)]
struct ErrorVector {
    name: String,
    stored_as: String,
    error: Value,
    text: String,
}

fn form() -> ErrorDataForm {
    serde_json::from_str(&vector_file("error_data.json")).expect("error_data.json parses")
}

#[test]
fn each_stored_library_error_reads_back() {
    for vector in form().vectors {
        let name = vector.name.as_str();
        let expected = stored_error(name, &vector.error, None);
        let read = match vector.stored_as.as_str() {
            "envelope" => match outcome_of_record::<(), CustomCode>(record(
                name,
                TaskStatus::Failed,
                &vector.text,
            )) {
                Ok(TaskOutcome::Failed(error)) => error.into_stored(),
                Ok(TaskOutcome::Succeeded(())) => panic!("{name}: read a value"),
                Err(error) => panic!("{name}: the read refused Python's text: {error}"),
            },
            "error" => serde_json::from_str::<horsies_core::TaskError>(&vector.text)
                .unwrap_or_else(|error| panic!("{name}: the read refused Python's text: {error}")),
            other => panic!("{name}: unknown stored form {other}"),
        };
        assert_eq!(read, expected, "{name}");
    }
}
