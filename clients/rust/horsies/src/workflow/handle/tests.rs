//! The decode of a workflow's stored values, one test per outcome.

use std::convert::Infallible;

use serde_json::json;

use super::*;
use crate::{AppConfig, Horsies, TaskErrorCode, WaitLimitRefused, MAX_WAIT_LIMIT_MS};

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

/// The output decodes from its own text: an integer above `u64::MAX` reads
/// as `u128`, as Python reads it.
#[test]
fn a_completed_result_above_u64_reads_as_u128() {
    let end = WorkflowEnd::Completed {
        result: Some(
            r#"{"__h_task_result__":true,"ok":340282366920938463463374607431768211455,"err":null}"#
                .to_owned(),
        ),
    };
    assert_eq!(
        end_outcome::<u128>(workflow(), end).unwrap(),
        TaskOutcome::Succeeded(u128::MAX)
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
            Err(WorkflowReadError::ResultDeserializationError { .. })
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
                Err(WorkflowReadError::ResultDeserializationError { .. })
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
        Some(&TaskOutcome::Succeeded(JsonText::parse("3").unwrap()))
    );
    assert_eq!(
        library_code(results.get("lost").unwrap()),
        "RESULT_DESERIALIZATION_ERROR"
    );
}

/// Each value of an outputless result and of an untyped node result keeps
/// its stored text: an integer outside the `i64` and `u64` range keeps every
/// digit and decodes as `u128` or `i128`.
#[test]
fn an_untyped_value_keeps_its_stored_text() {
    let wide = "340282366920938463463374607431768211455";
    let below = "-9223372036854775809";
    let stored = format!(
        r#"{{"__h_task_result__":true,"__h_outputless_terminals__":true,"ok":{{"results_by_id":{{"wide":{{"__h_task_result__":true,"ok":{wide},"err":null}},"below":{{"__h_task_result__":true,"ok":{below},"err":null}}}},"task_name_by_id":{{"wide":"wide","below":"below"}},"definition_key_by_id":{{}}}},"err":null}}"#
    );
    let outcome = completed_outcome::<OutputlessResults>(workflow(), &stored).unwrap();
    let TaskOutcome::Succeeded(results) = outcome else {
        panic!("an outputless result reads as Succeeded");
    };
    let Some(TaskOutcome::Succeeded(value)) = results.get("wide") else {
        panic!("the wide entry reads");
    };
    assert_eq!(value.get(), wide);
    assert_eq!(value.decode::<u128>().unwrap(), u128::MAX);
    let Some(TaskOutcome::Succeeded(value)) = results.get("below") else {
        panic!("the below entry reads");
    };
    assert_eq!(value.decode::<i128>().unwrap(), i128::from(i64::MIN) - 1);
    let node = format!(r#"{{"__h_task_result__":true,"ok":{wide},"err":null}}"#);
    let NodeResult::Outcome(TaskOutcome::Succeeded(value)) = node_result(&node) else {
        panic!("the node result reads");
    };
    assert_eq!(value.get(), wide);
}

/// Python fails closed when the stored form and the handle's type disagree.
#[test]
fn an_outputless_result_read_as_a_type_and_the_reverse_are_unreadable() {
    assert!(matches!(
        end_outcome::<u32>(workflow(), completed(Some(outputless()))),
        Err(WorkflowReadError::ResultDeserializationError { .. })
    ));
    let typed = completed(Some(
        json!({"__h_task_result__": true, "ok": 1, "err": null}),
    ));
    assert!(matches!(
        end_outcome::<OutputlessResults>(workflow(), typed),
        Err(WorkflowReadError::ResultDeserializationError { .. })
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
        Err(WorkflowReadError::UnknownCustomCode { .. })
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
        NodeResult::Outcome(TaskOutcome::Succeeded(JsonText::parse("[1]").unwrap()))
    );
    assert!(matches!(node_result("{"), NodeResult::Unreadable { .. }));
}

#[test]
fn each_read_failure_has_python_s_code() {
    let cases = [
        (
            WorkflowReadError::WaitTimeout {
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
            WorkflowReadError::ResultDeserializationError {
                workflow_id: workflow(),
                cause: "x".to_owned(),
            },
            OperationalErrorCode::ResultDeserializationError.into(),
        ),
        (
            WorkflowReadError::UnknownCustomCode {
                workflow_id: workflow(),
                cause: UnknownCustomCode {
                    stored_code: "X".to_owned(),
                    code_type: "T",
                },
            },
            RetrievalCode::UnknownCustomCode.into(),
        ),
        (
            WorkflowReadError::InvalidInput {
                workflow_id: workflow(),
                cause: WaitLimitRefused,
            },
            RetrievalCode::InvalidInput.into(),
        ),
        (
            WorkflowReadError::WaitInsideTask {
                message: "m".to_owned(),
            },
            RetrievalCode::WaitInsideTask.into(),
        ),
        (
            WorkflowReadError::AsyncContext {
                message: "m".to_owned(),
            },
            RetrievalCode::AsyncContext.into(),
        ),
        (
            WorkflowReadError::DatabaseError {
                database_code: HandleErrorCode::DbOperationFailed,
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

/// The limit is the first check of `get`, before any statement: the app's
/// address has no database, so a wait that ran would fail as `DatabaseError`.
/// The limit at the bound passes the check.
#[tokio::test]
async fn a_get_above_the_bound_is_invalid_input_before_any_statement() {
    let app = Horsies::new(AppConfig::for_database_url(
        "postgresql://127.0.0.1:1/nothing",
    ))
    .unwrap();
    let handle = app.workflow_handle::<u32>(workflow());
    let above = WaitLimit::Within(std::time::Duration::from_millis(MAX_WAIT_LIMIT_MS + 1));
    let refused = handle.get(above).await.unwrap_err();
    assert_eq!(
        refused,
        WorkflowReadError::InvalidInput {
            workflow_id: workflow(),
            cause: WaitLimitRefused,
        },
    );
    assert_eq!(refused.code().value(), "INVALID_INPUT");
    assert_eq!(
        refused.to_string(),
        format!(
            "invalid input for the read of workflow {}: the wait limit is above 4294967295 ms \
             (about 49.7 days)",
            workflow()
        ),
    );
    let at_the_bound = WaitLimit::Within(std::time::Duration::from_millis(MAX_WAIT_LIMIT_MS));
    assert_eq!(
        handle.checked_wait(at_the_bound),
        Ok(Some(std::time::Duration::from_millis(MAX_WAIT_LIMIT_MS)))
    );
}

#[test]
fn a_handle_error_maps_to_its_read_failure() {
    let message = || "m".to_owned();
    let cause = || horsies_core::WorkflowError::InvalidStatus("BOGUS".to_owned());
    assert!(matches!(
        WorkflowReadError::of_handle_error(HandleOperationError::WorkflowNotFound {
            message: message(),
            workflow_id: workflow(),
        }),
        WorkflowReadError::WorkflowNotFound { .. }
    ));
    assert!(matches!(
        WorkflowReadError::of_handle_error(HandleOperationError::StoredDataInvalid {
            message: message(),
            workflow_id: workflow(),
            cause: cause(),
        }),
        WorkflowReadError::ResultDeserializationError { .. }
    ));
    assert!(matches!(
        WorkflowReadError::of_handle_error(HandleOperationError::DbOperationFailed {
            message: message(),
            retryable: false,
            workflow_id: workflow(),
            cause: cause(),
        }),
        WorkflowReadError::DatabaseError { .. }
    ));
    assert!(matches!(
        WorkflowReadError::of_handle_error(HandleOperationError::ResumeRecoveryFailed {
            message: message(),
            workflow_id: workflow(),
            cause: cause(),
        }),
        WorkflowReadError::DatabaseError { .. }
    ));
    assert!(matches!(
        WorkflowReadError::of_handle_error(HandleOperationError::AsyncContext {
            message: message(),
            workflow_id: workflow(),
        }),
        WorkflowReadError::AsyncContext { .. }
    ));
}

/// Python's handle: an outputless entry that is `null`, has no task name,
/// or is not a result envelope is a `RESULT_DESERIALIZATION_ERROR` outcome
/// with its node in `data.node_id`; the other entries stay readable.
#[test]
fn an_outputless_entry_without_a_payload_a_task_name_or_an_envelope_names_its_node() {
    let stored = json!({
        "__h_task_result__": true,
        "__h_outputless_terminals__": true,
        "ok": {
            "results_by_id": {
                "a": {"__h_task_result__": true, "ok": 1, "err": null},
                "b": null,
                "c": {"not": "an envelope"},
                "d": {"__h_task_result__": true, "ok": 2, "err": null},
            },
            "task_name_by_id": {"a": "t", "b": "t", "c": "t"},
            "definition_key_by_id": {"a": null, "b": null, "c": null, "d": null},
        },
        "err": null,
    });
    let TaskOutcome::Succeeded(results) =
        end_outcome::<OutputlessResults>(workflow(), completed(Some(stored))).unwrap()
    else {
        panic!("an outputless result reads as Succeeded");
    };
    let Some(TaskOutcome::Succeeded(value)) = results.get("a") else {
        panic!("entry a reads as Succeeded");
    };
    assert_eq!(
        value
            .decode::<serde_json::Value>()
            .expect("the stored value decodes"),
        json!(1)
    );
    for node in ["b", "c", "d"] {
        let Some(TaskOutcome::Failed(error)) = results.get(node) else {
            panic!("entry {node} reads as Failed");
        };
        assert_eq!(
            error.code().text(),
            "RESULT_DESERIALIZATION_ERROR",
            "{node}"
        );
        assert_eq!(
            error
                .data()
                .map(|data| data.decode::<serde_json::Value>().unwrap()),
            Some(json!({ "node_id": node })),
            "{node}"
        );
    }
}

/// Python's handle refuses an outputless envelope whose `ok` slot is not an
/// object, or whose `results_by_id`, `task_name_by_id` or
/// `definition_key_by_id` is not an object; a missing map is empty.
#[test]
fn a_malformed_outputless_envelope_is_unreadable_and_a_missing_map_is_empty() {
    let envelope = |ok: serde_json::Value| {
        json!({
            "__h_task_result__": true,
            "__h_outputless_terminals__": true,
            "ok": ok,
            "err": null,
        })
    };
    for ok in [
        json!(5),
        json!({"results_by_id": [1]}),
        json!({"results_by_id": {}, "task_name_by_id": [1]}),
        json!({"results_by_id": {}, "definition_key_by_id": "x"}),
    ] {
        let read =
            end_outcome::<OutputlessResults>(workflow(), completed(Some(envelope(ok.clone()))));
        assert!(
            matches!(
                read,
                Err(WorkflowReadError::ResultDeserializationError { .. })
            ),
            "{ok}"
        );
    }
    let TaskOutcome::Succeeded(results) =
        end_outcome::<OutputlessResults>(workflow(), completed(Some(envelope(json!({}))))).unwrap()
    else {
        panic!("an outputless result reads as Succeeded");
    };
    assert!(results.is_empty());
}

/// A stored result that is not JSON is unreadable.
#[test]
fn a_completed_result_that_is_not_json_is_unreadable() {
    let end = WorkflowEnd::Completed {
        result: Some("{not valid json".to_owned()),
    };
    assert!(matches!(
        end_outcome::<u32>(workflow(), end),
        Err(WorkflowReadError::ResultDeserializationError { .. })
    ));
}

/// Python `_decode_stored_workflow_error`: the stored error is the answer of
/// every ended status, a cancel included; an empty one is unreadable.
#[test]
fn every_ended_status_reads_its_stored_error_and_refuses_an_empty_one() {
    let stored = json!({"error_code": "STOPPED_BY_OPERATOR", "message": "stopped"}).to_string();
    for status in [
        TerminalErrorStatus::Failed,
        TerminalErrorStatus::Cancelled,
        TerminalErrorStatus::Expired,
    ] {
        let end = WorkflowEnd::Ended {
            status,
            error: Some(stored.clone()),
        };
        let outcome = end_outcome::<u32>(workflow(), end).unwrap();
        assert_eq!(library_code(&outcome), "STOPPED_BY_OPERATOR", "{status:?}");
        let empty = WorkflowEnd::Ended {
            status,
            error: Some(String::new()),
        };
        assert!(
            matches!(
                end_outcome::<u32>(workflow(), empty),
                Err(WorkflowReadError::ResultDeserializationError { .. })
            ),
            "{status:?}"
        );
    }
}

/// A stored sub-workflow node of node 0 with `status` and `summary`.
fn stored_node(status: &str, summary: Option<&str>) -> StoredNode {
    StoredNode {
        node_id: Some("node-9".to_owned()),
        task_index: 0,
        task_name: "child".to_owned(),
        status: status.to_owned(),
        result: None,
        started_at: None,
        completed_at: None,
        sub_workflow_id: None,
        sub_workflow_summary: summary.map(str::to_owned),
        is_subworkflow: true,
        sub_definition_key: Some("tests.child.v1".to_owned()),
    }
}

/// A stored node reads its status, its result and its summary.
#[test]
fn a_stored_node_reads_its_status_result_and_summary() {
    let mut node = stored_node(
        "COMPLETED",
        Some(
            r#"{"status": "COMPLETED", "output": null, "total_tasks": 3, "completed_tasks": 3, "failed_tasks": 0, "skipped_tasks": 0, "error_summary": null}"#,
        ),
    );
    node.result = Some(json!({"__h_task_result__": true, "ok": 42, "err": null}).to_string());
    let info = node_info(workflow(), node).unwrap();
    assert_eq!(info.status, WorkflowTaskStatus::Completed);
    assert_eq!(
        info.result,
        Some(NodeResult::Outcome(TaskOutcome::Succeeded(
            JsonText::parse("42").unwrap()
        )))
    );
    let summary = info
        .sub_workflow_summary
        .expect("the node gives its summary");
    assert_eq!(summary.status, WorkflowStatus::Completed);
    assert_eq!(
        (
            summary.total_tasks,
            summary.completed_tasks,
            summary.failed_tasks,
            summary.skipped_tasks,
        ),
        (3, 3, 0, 0)
    );
}

/// A stored summary output outside the `i64` and `u64` range keeps its
/// stored text and decodes as `u128` or `i128` (#1862).
#[test]
fn a_stored_summary_output_keeps_its_stored_text() {
    for output in [
        "340282366920938463463374607431768211455",
        "-9223372036854775809",
    ] {
        let text = format!(
            r#"{{"status": "COMPLETED", "output": {output}, "total_tasks": 1, "completed_tasks": 1, "failed_tasks": 0, "skipped_tasks": 0, "error_summary": null}}"#
        );
        let info = node_info(workflow(), stored_node("COMPLETED", Some(&text))).unwrap();
        let summary_output = info
            .sub_workflow_summary
            .expect("the node gives its summary")
            .output
            .expect("the summary holds the output");
        assert_eq!(summary_output.get(), output);
        match output.starts_with('-') {
            true => assert_eq!(
                summary_output.decode::<i128>().unwrap(),
                i128::from(i64::MIN) - 1
            ),
            false => assert_eq!(summary_output.decode::<u128>().unwrap(), u128::MAX),
        }
    }
}

/// Python `stored_node_status`: a stored status outside the node statuses
/// is `STORED_DATA_INVALID`, not retryable, and names its node and value.
#[test]
fn a_node_status_outside_the_enum_is_stored_data_invalid() {
    let refused = node_info(workflow(), stored_node("BOGUS", None)).unwrap_err();
    assert_eq!(refused.code(), HandleErrorCode::StoredDataInvalid);
    assert!(!refused.retryable());
    assert_eq!(refused.workflow_id(), workflow());
    assert_eq!(
        refused.failure_cause(),
        horsies_core::FailureCause::ContractViolation
    );
    assert!(matches!(
        refused,
        HandleOperationError::StoredDataInvalid {
            cause: horsies_core::WorkflowError::InvalidStatus(ref status),
            ..
        } if status == "BOGUS"
    ));
    assert!(
        refused.message().contains("node 0"),
        "{}",
        refused.message()
    );
    assert!(
        refused.message().contains("\"BOGUS\""),
        "{}",
        refused.message()
    );
}

/// A stored NULL is no summary.
#[test]
fn a_stored_null_is_no_summary() {
    let info = node_info(workflow(), stored_node("COMPLETED", None))
        .expect("a node without a summary reads");
    assert!(info.sub_workflow_summary.is_none());
}

/// An empty stored summary is `STORED_DATA_INVALID`, not retryable, and
/// names its node: NULL is the one stored form of no summary, and the empty
/// text is corrupt data.
#[test]
fn an_empty_stored_summary_is_stored_data_invalid() {
    let refused = node_info(workflow(), stored_node("COMPLETED", Some("")))
        .expect_err("an empty summary does not read");
    assert_eq!(refused.code(), HandleErrorCode::StoredDataInvalid);
    assert!(!refused.retryable());
    assert_eq!(refused.workflow_id(), workflow());
    assert!(
        refused
            .message()
            .starts_with("node 0 has a stored sub-workflow summary that does not read"),
        "{}",
        refused.message()
    );
}

/// A stored summary that is not JSON, that breaks the summary rules (an
/// unknown status, never read as FAILED), or that is nested past the read's
/// depth bound is `STORED_DATA_INVALID`, not retryable.
#[test]
fn a_summary_that_does_not_read_is_stored_data_invalid() {
    let deep = format!("{{\"status\":{}{}}}", "[".repeat(1_100), "]".repeat(1_100));
    for summary in [
        "{not valid json!!!",
        r#"{"status": "NOT_A_STATUS", "total_tasks": 1}"#,
        deep.as_str(),
    ] {
        let refused = node_info(workflow(), stored_node("COMPLETED", Some(summary))).unwrap_err();
        assert_eq!(refused.code(), HandleErrorCode::StoredDataInvalid);
        assert!(!refused.retryable());
        // The cause is the refused deserialize of the stored value: a
        // contract violation.
        assert_eq!(
            refused.failure_cause(),
            horsies_core::FailureCause::ContractViolation
        );
        assert!(matches!(
            refused,
            HandleOperationError::StoredDataInvalid {
                cause: horsies_core::WorkflowError::Serialization(_),
                ..
            }
        ));
        assert!(
            refused.message().contains("sub-workflow summary"),
            "{}",
            refused.message()
        );
    }
}

/// A handle prints and crosses threads for any output type: Debug, Send and
/// Sync.
#[test]
fn a_workflow_handle_is_debug_send_and_sync() {
    fn assert_debug_send_sync<V: std::fmt::Debug + Send + Sync>() {}
    assert_debug_send_sync::<WorkflowHandle<u32>>();
    assert_debug_send_sync::<WorkflowHandle<serde_json::Value>>();
    assert_debug_send_sync::<WorkflowHandle<std::rc::Rc<u8>>>();
}
