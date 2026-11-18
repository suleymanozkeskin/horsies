//! `WorkflowError::is_retryable`: one test per variant class.

use uuid::Uuid;

use super::WorkflowError;
use crate::broker::BrokerError;
use crate::workflow_engine::child_spec::ChildSpecUnavailable;
use crate::workflow_engine::steps::StepContractError;

#[test]
fn a_broker_error_follows_the_broker_rule() {
    let lost = WorkflowError::Broker(BrokerError::ConnectionFailed("reset".to_owned()));
    let refused = WorkflowError::Broker(BrokerError::InvalidStatus("BOGUS".to_owned()));

    assert!(lost.is_retryable());
    assert!(!refused.is_retryable());
}

#[test]
fn a_child_spec_source_error_follows_the_source_rule() {
    let not_run = WorkflowError::ChildSpecSource(ChildSpecUnavailable::NotRun {
        subject: "child build".to_owned(),
        cause: "callback queue full".to_owned(),
    });
    let interrupted = WorkflowError::ChildSpecSource(ChildSpecUnavailable::Interrupted {
        subject: "child build".to_owned(),
        cause: "KeyboardInterrupt".to_owned(),
    });
    let raised = WorkflowError::ChildSpecSource(ChildSpecUnavailable::Raised {
        subject: "child build".to_owned(),
        cause: "SerializationError: corrupt".to_owned(),
    });

    assert!(not_run.is_retryable());
    assert!(!interrupted.is_retryable());
    assert!(!raised.is_retryable());
}

#[test]
fn a_validation_error_is_not_retryable() {
    let error = WorkflowError::Validation("node 0 has no queue".to_owned());

    assert!(!error.is_retryable());
}

/// The variants that meet the same stored data or the same contract break
/// again on a repeat.
#[test]
fn stored_data_and_contract_errors_are_not_retryable() {
    let not_json = serde_json::from_str::<serde_json::Value>("{")
        .expect_err("an open brace alone is not JSON");
    let errors = [
        WorkflowError::Serialization(not_json),
        WorkflowError::WorkflowNotFound {
            workflow_id: Uuid::nil(),
        },
        WorkflowError::InvalidStatus("BOGUS".to_owned()),
        WorkflowError::StepContract(StepContractError::UnknownOutcome {
            function: "horsies_workflow_node_finish",
            outcome: "BOGUS".to_owned(),
        }),
    ];

    for error in errors {
        assert!(!error.is_retryable(), "{error}");
    }
}
