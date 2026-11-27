//! The task input vectors (`task_input.json`) through the core.
//!
//! The core does not encode a task input: a client sends its text, the send
//! checks it with the strict read rule and stores it as written
//! (`prepare_enqueue_facts`), and the child host passes an input without
//! engine members to the task body byte for byte (`build_task_input`).

use serde::Deserialize;

use super::{refusal_name, vector_file};
use crate::core::history::enqueue::{prepare_enqueue_facts, EnqueuePreparationError, SendPart};
use crate::worker::execution::{build_task_input, UpstreamCodes};

#[derive(Deserialize)]
struct TaskInputForm {
    vectors: Vec<Vector>,
    refused_texts: Vec<RefusedText>,
}

/// A vector: the core reads only its name and Python's text.
#[derive(Deserialize)]
struct Vector {
    name: String,
    text: String,
}

#[derive(Deserialize)]
struct RefusedText {
    name: String,
    text: String,
    core_refusal: String,
    /// Python's text of the refusal, for a refusal that Python's reader
    /// words itself.
    refusal_text: Option<String>,
}

fn form() -> TaskInputForm {
    serde_json::from_str(&vector_file("task_input.json")).expect("task_input.json parses")
}

/// The send's preparation of `input_json` for a plain send with no options.
fn send(input_json: &str) -> Result<(), EnqueuePreparationError> {
    prepare_enqueue_facts(
        "stored_form_vector",
        "default",
        100,
        input_json,
        None,
        None,
        None,
        None,
        false,
        None,
    )
    .map(|_facts| ())
}

#[test]
fn the_send_accepts_each_python_text() {
    for vector in form().vectors {
        if let Err(refused) = send(&vector.text) {
            panic!("{}: the send refused Python's text: {refused}", vector.name);
        }
    }
}

#[test]
fn the_child_host_passes_each_python_text_byte_for_byte() {
    for vector in form().vectors {
        let passed = build_task_input(&vector.text, false, UpstreamCodes::NotChecked)
            .unwrap_or_else(|refused| panic!("{}: the child host refused: {refused}", vector.name));
        assert_eq!(passed, vector.text.as_bytes(), "{}", vector.name);
    }
}

#[test]
fn the_send_refuses_each_refused_text_with_its_refusal() {
    for refused in form().refused_texts {
        match send(&refused.text) {
            Err(EnqueuePreparationError::StrictJson {
                field: SendPart::Input,
                source,
            }) => {
                assert_eq!(
                    refusal_name(&source),
                    refused.core_refusal,
                    "{}",
                    refused.name
                );
                if let Some(python_text) = &refused.refusal_text {
                    assert_eq!(&source.to_string(), python_text, "{}", refused.name);
                }
            }
            other => panic!(
                "{}: expected the strict read refusal {}, got {other:?}",
                refused.name, refused.core_refusal
            ),
        }
    }
}

#[test]
fn the_child_host_refuses_each_refused_text() {
    for refused in form().refused_texts {
        assert!(
            build_task_input(&refused.text, false, UpstreamCodes::NotChecked).is_err(),
            "{}",
            refused.name
        );
    }
}
