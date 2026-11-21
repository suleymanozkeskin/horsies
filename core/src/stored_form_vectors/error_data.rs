//! The error data vectors (`error_data.json`) through the core: each library
//! error that the core writes, as an envelope (`task_result_to_json_string`)
//! or as the error object of the workflow error column, and its read. The
//! writers with their own tests: the engine's `args_merge` errors and the SQL
//! writer (here), the slot writes of a timeout and a killed child
//! (`worker::executor::slots`), the stale-crash result (`worker::recovery`).

use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use super::{task_error_of, vector_file};
use crate::core::codec::{
    from_json_str, task_result_from_json_str, task_result_to_json_string, to_json_string,
};
use crate::core::{TaskError, TaskResult};
use crate::workflow_engine::args_merge::{
    missing_result_error, upstream_skipped_error, ArgsFromConflict, MergedNodeKind,
};

#[derive(Deserialize)]
pub(crate) struct ErrorDataForm {
    pub(crate) vectors: Vec<ErrorVector>,
}

/// A library error, how it is stored (`envelope` or `error`), Python's
/// text, and the inputs of its writer.
#[derive(Deserialize)]
pub(crate) struct ErrorVector {
    pub(crate) name: String,
    pub(crate) stored_as: String,
    pub(crate) error: Value,
    pub(crate) text: String,
    #[serde(default)]
    pub(crate) inputs: Value,
}

pub(crate) fn form() -> ErrorDataForm {
    serde_json::from_str(&vector_file("error_data.json")).expect("error_data.json parses")
}

/// The vector `name`.
pub(crate) fn vector(name: &str) -> ErrorVector {
    form()
        .vectors
        .into_iter()
        .find(|vector| vector.name == name)
        .unwrap_or_else(|| panic!("error_data.json has no vector {name}"))
}

/// The stored text of `error` as the vector stores it.
fn stored_text(vector: &ErrorVector, error: TaskError) -> String {
    let written = match vector.stored_as.as_str() {
        "envelope" => task_result_to_json_string(&TaskResult::<Value>::Err(error)),
        "error" => to_json_string(&error),
        other => panic!("{}: unknown stored form {other}", vector.name),
    };
    written.unwrap_or_else(|error| panic!("{}: {error}", vector.name))
}

fn input<'a>(vector: &'a ErrorVector, key: &str) -> &'a Value {
    vector
        .inputs
        .get(key)
        .unwrap_or_else(|| panic!("{}: no input {key}", vector.name))
}

fn input_i32(vector: &ErrorVector, key: &str) -> i32 {
    input(vector, key)
        .as_i64()
        .and_then(|number| i32::try_from(number).ok())
        .unwrap_or_else(|| panic!("{}: {key} is not an i32", vector.name))
}

fn input_text<'a>(vector: &'a ErrorVector, key: &str) -> &'a str {
    input(vector, key)
        .as_str()
        .unwrap_or_else(|| panic!("{}: {key} is not text", vector.name))
}

#[test]
fn each_error_encodes_to_its_text() {
    for vector in form().vectors {
        let error = task_error_of(&vector.name, &vector.error, None);
        assert_eq!(stored_text(&vector, error), vector.text, "{}", vector.name);
    }
}

#[test]
fn each_text_reads_back_to_its_error() {
    for vector in form().vectors {
        let name = vector.name.as_str();
        let expected = task_error_of(name, &vector.error, None);
        let read = match vector.stored_as.as_str() {
            "envelope" => match task_result_from_json_str::<Value>(&vector.text) {
                Ok(TaskResult::Err(error)) => error,
                Ok(TaskResult::Ok(_)) => panic!("{name}: read a value"),
                Err(error) => panic!("{name}: refused: {error}"),
            },
            "error" => from_json_str::<TaskError>(&vector.text)
                .unwrap_or_else(|error| panic!("{name}: refused: {error}")),
            other => panic!("{name}: unknown stored form {other}"),
        };
        assert_eq!(read, expected, "{name}");
    }
}

/// The engine's errors of a node input merge (`workflow_engine/args_merge.rs`).
#[test]
fn the_engine_merge_errors_are_python_s_texts() {
    let skipped = vector("upstream_skipped");
    assert_eq!(
        stored_text(
            &skipped,
            upstream_skipped_error(input_i32(&skipped, "dependency_index"))
        ),
        skipped.text
    );
    for name in [
        "result_deserialization_error_dependency_without_result",
        "result_deserialization_error_non_ascii_task_name",
    ] {
        let missing = vector(name);
        assert_eq!(
            stored_text(
                &missing,
                missing_result_error(
                    input_i32(&missing, "dependency_index"),
                    Some(input_text(&missing, "node_id")),
                    input_text(&missing, "task_name"),
                    input_text(&missing, "status"),
                )
            ),
            missing.text,
            "{name}"
        );
    }
    let conflict = vector("workflow_enqueue_failed_args_from_conflict");
    let workflow_id = Uuid::parse_str(input_text(&conflict, "workflow_id")).expect("a UUID");
    let error = ArgsFromConflict {
        key: input_text(&conflict, "key").to_owned(),
    }
    .node_error(
        workflow_id,
        input_i32(&conflict, "task_index"),
        MergedNodeKind::Task,
    );
    assert_eq!(stored_text(&conflict, error), conflict.text);
}

/// The SQL writer of the engine's errors (`horsies_task_error_envelope`, and
/// `horsies_workflow_output_error_data` for the data of
/// `WORKFLOW_OUTPUT_SKIPPED`), given the code, the message and the data text
/// of each vector. The non-ASCII vectors pin that Postgres writes the
/// characters as Python does, not as `\u` escapes.
#[tokio::test]
async fn the_sql_writer_builds_python_s_engine_errors() {
    let pool = crate::test_support::migrated_pool().await;
    for name in [
        "upstream_skipped",
        "result_deserialization_error_dependency_without_result",
        "result_deserialization_error_non_ascii_task_name",
        "workflow_output_skipped",
        "workflow_output_skipped_non_ascii_task_name",
        "workflow_enqueue_failed_args_from_conflict",
        "subworkflow_depth_limit_reached",
        "workflow_recovery_limit_reached",
    ] {
        let vector = vector(name);
        let code = vector.error["error_code"]["built_in"]
            .as_str()
            .unwrap_or_else(|| panic!("{name}: a built-in code"));
        let message = vector.error["message"]
            .as_str()
            .unwrap_or_else(|| panic!("{name}: a message"));
        let data = to_json_string(&vector.error["data"]).expect("the data encodes");
        let written: String = crate::test_support::sql::query_scalar(
            "SELECT horsies_task_error_envelope($1, $2, $3, NULL, NULL)",
        )
        .bind(code)
        .bind(message)
        .bind(&data)
        .fetch_one(&pool)
        .await
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(written, vector.text, "{name}");
    }
    for name in [
        "workflow_output_skipped",
        "workflow_output_skipped_non_ascii_task_name",
    ] {
        let output = vector(name);
        let data: String = crate::test_support::sql::query_scalar(
            "SELECT horsies_workflow_output_error_data($1, $2, $3, $4)",
        )
        .bind(Uuid::parse_str(input_text(&output, "workflow_id")).expect("a UUID"))
        .bind(input_text(&output, "node_id"))
        .bind(input_i32(&output, "task_index"))
        .bind(input_text(&output, "task_name"))
        .fetch_one(&pool)
        .await
        .expect("the output error data");
        assert_eq!(
            data,
            to_json_string(&output.error["data"]).expect("the data encodes"),
            "{name}"
        );
    }
}
