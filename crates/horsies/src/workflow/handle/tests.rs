//! The decode of a workflow's stored values, one test per outcome.

use std::convert::Infallible;

use serde_json::json;

use super::*;
use crate::TaskErrorCode;

fn workflow() -> Uuid {
    Uuid::from_u128(9)
}

fn completed(result: Option<serde_json::Value>) -> WorkflowEnd {
    WorkflowEnd::Completed {
        result: result.map(|value| value.to_string()),
    }
}

fn library_code<E: UserErrorCode>(outcome: &TaskOutcome<impl std::fmt::Debug, E>) -> String {
    match outcome {
        TaskOutcome::Failed(error) => error.code().text(),
        TaskOutcome::Succeeded(value) => panic!("expected a failed outcome, got {value:?}"),
    }
}

#[test]
fn a_completed_result_reads_as_the_output_type() {
    let end = completed(Some(
        json!({"__h_task_result__": true, "ok": [1, 2], "err": null}),
    ));
    assert_eq!(
        end_outcome::<Vec<u32>>(workflow(), end).unwrap(),
        TaskOutcome::Succeeded(vec![1, 2])
    );
}

#[test]
fn a_completed_error_keeps_its_user_code_as_text() {
    let end = completed(Some(json!({
        "__h_task_result__": true,
        "ok": null,
        "err": {"error_code": "DECLINED", "message": "no"},
    })));
    let outcome = end_outcome::<u32>(workflow(), end).unwrap();
    assert_eq!(library_code(&outcome), "DECLINED");
}

#[test]
fn a_completed_workflow_without_a_result_is_unreadable() {
    for end in [
        completed(None),
        WorkflowEnd::Completed {
            result: Some(String::new()),
        },
    ] {
        assert!(matches!(
            end_outcome::<u32>(workflow(), end),
            Err(WorkflowReadError::StoredResultUnreadable { .. })
        ));
    }
}

#[test]
fn a_result_of_another_type_is_unreadable() {
    let end = completed(Some(
        json!({"__h_task_result__": true, "ok": "text", "err": null}),
    ));
    let refused = end_outcome::<u32>(workflow(), end).unwrap_err();
    assert_eq!(
        refused.code(),
        BuiltInTaskCode::from(OperationalErrorCode::ResultDeserializationError)
    );
}

#[test]
fn an_ended_workflow_without_a_stored_error_has_the_code_of_its_status() {
    for (status, code) in [
        (TerminalErrorStatus::Failed, "WORKFLOW_FAILED"),
        (TerminalErrorStatus::Cancelled, "WORKFLOW_CANCELLED"),
        (TerminalErrorStatus::Expired, "WORKFLOW_EXPIRED"),
    ] {
        let end = WorkflowEnd::Ended {
            status,
            error: None,
        };
        let outcome = end_outcome::<u32>(workflow(), end).unwrap();
        assert_eq!(library_code(&outcome), code);
    }
}

#[test]
fn an_ended_workflow_reads_its_stored_error() {
    let end = WorkflowEnd::Ended {
        status: TerminalErrorStatus::Failed,
        error: Some(json!({"error_code": {"__builtin_task_code__": "TASK_TIMEOUT"}}).to_string()),
    };
    let outcome = end_outcome::<u32>(workflow(), end).unwrap();
    assert_eq!(library_code(&outcome), "TASK_TIMEOUT");
}

#[test]
fn a_stored_error_that_does_not_read_is_unreadable() {
    for error in ["", "not json", "[1]", r#"{"unknown": 1}"#] {
        let end = WorkflowEnd::Ended {
            status: TerminalErrorStatus::Failed,
            error: Some(error.to_owned()),
        };
        assert!(
            matches!(
                end_outcome::<u32>(workflow(), end),
                Err(WorkflowReadError::StoredResultUnreadable { .. })
            ),
            "{error:?}"
        );
    }
}

#[test]
fn a_paused_workflow_is_the_paused_outcome() {
    let outcome = end_outcome::<u32>(workflow(), WorkflowEnd::Paused).unwrap();
    assert_eq!(library_code(&outcome), "WORKFLOW_PAUSED");
}

fn outputless() -> serde_json::Value {
    json!({
        "__h_task_result__": true,
        "__h_outputless_terminals__": true,
        "ok": {
            "results_by_id": {
                "fetch": {"__h_task_result__": true, "ok": 3, "err": null},
                "lost": null,
            },
            "task_name_by_id": {"fetch": "fetch", "lost": "lost"},
            "definition_key_by_id": {"fetch": null, "lost": null},
        },
        "err": null,
    })
}

#[test]
fn an_outputless_result_reads_each_node() {
    let outcome =
        end_outcome::<OutputlessResults>(workflow(), completed(Some(outputless()))).unwrap();
    let TaskOutcome::Succeeded(results) = outcome else {
        panic!("an outputless result reads as Succeeded");
    };
    assert_eq!(results.len(), 2);
    assert_eq!(
        results.get("fetch"),
        Some(&TaskOutcome::Succeeded(json!(3)))
    );
    assert_eq!(
        library_code(results.get("lost").unwrap()),
        "RESULT_DESERIALIZATION_ERROR"
    );
}

/// Python fails closed when the stored form and the handle's type disagree.
#[test]
fn an_outputless_result_read_as_a_type_and_the_reverse_are_unreadable() {
    assert!(matches!(
        end_outcome::<u32>(workflow(), completed(Some(outputless()))),
        Err(WorkflowReadError::StoredResultUnreadable { .. })
    ));
    let typed = completed(Some(
        json!({"__h_task_result__": true, "ok": 1, "err": null}),
    ));
    assert!(matches!(
        end_outcome::<OutputlessResults>(workflow(), typed),
        Err(WorkflowReadError::StoredResultUnreadable { .. })
    ));
}

#[test]
fn a_node_result_reads_with_the_node_types() {
    let text = json!({"__h_task_result__": true, "ok": 7, "err": null}).to_string();
    assert_eq!(
        envelope_outcome::<u32, Infallible>(workflow(), &text).unwrap(),
        TaskOutcome::Succeeded(7)
    );
    let declined = json!({
        "__h_task_result__": true,
        "ok": null,
        "err": {"error_code": "DECLINED"},
    })
    .to_string();
    assert!(matches!(
        envelope_outcome::<u32, Infallible>(workflow(), &declined),
        Err(WorkflowReadError::UnknownUserCode { .. })
    ));
    let TaskOutcome::Failed(error) =
        envelope_outcome::<u32, UserCode>(workflow(), &declined).unwrap()
    else {
        panic!("a stored error reads as Failed");
    };
    assert!(matches!(error.code(), TaskErrorCode::User(code) if code.as_str() == "DECLINED"));
}

#[test]
fn an_untyped_node_result_keeps_what_does_not_read() {
    assert_eq!(
        node_result(&json!({"__h_task_result__": true, "ok": [1], "err": null}).to_string()),
        NodeResult::Outcome(TaskOutcome::Succeeded(json!([1])))
    );
    assert!(matches!(node_result("{"), NodeResult::Unreadable { .. }));
}

#[test]
fn each_read_failure_has_python_s_code() {
    let cases = [
        (
            WorkflowReadError::WaitTimedOut {
                workflow_id: workflow(),
            },
            BuiltInTaskCode::from(RetrievalCode::WaitTimeout),
        ),
        (
            WorkflowReadError::WorkflowNotFound {
                workflow_id: workflow(),
            },
            RetrievalCode::WorkflowNotFound.into(),
        ),
        (
            WorkflowReadError::ResultNotReady {
                workflow_id: workflow(),
                node_id: "n".to_owned(),
            },
            RetrievalCode::ResultNotReady.into(),
        ),
        (
            WorkflowReadError::WaitInsideTask {
                message: "m".to_owned(),
            },
            RetrievalCode::WaitInsideTask.into(),
        ),
        (
            WorkflowReadError::Database {
                code: HandleErrorCode::DbOperationFailed,
                message: "m".to_owned(),
                retryable: true,
            },
            OperationalErrorCode::BrokerError.into(),
        ),
    ];
    for (error, code) in cases {
        assert_eq!(error.code(), code, "{error}");
    }
}

#[test]
fn a_handle_error_maps_to_its_read_failure() {
    let error = |code| HandleOperationError {
        code,
        message: "m".to_owned(),
        retryable: false,
        workflow_id: workflow(),
    };
    assert!(matches!(
        WorkflowReadError::of_handle_error(error(HandleErrorCode::WorkflowNotFound)),
        WorkflowReadError::WorkflowNotFound { .. }
    ));
    assert!(matches!(
        WorkflowReadError::of_handle_error(error(HandleErrorCode::StoredDataInvalid)),
        WorkflowReadError::StoredResultUnreadable { .. }
    ));
    assert!(matches!(
        WorkflowReadError::of_handle_error(error(HandleErrorCode::DbOperationFailed)),
        WorkflowReadError::Database { .. }
    ));
}
