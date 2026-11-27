//! The cause record of a failed core operation.
//!
//! A failure record of an operation carries the cause of its failure: one
//! record per class of cause (`horsies_core::FailureCause`), with the
//! operation that failed as its subject.
//!
//! ```text
//! { kind: "server", subject, sqlstate, message, retryable }   the server answered with an error
//! { kind: "connection", subject, message, retryable }         no answer from the server
//! { kind: "contractViolation", subject, message }             a row or a stored value broke the read contract
//! { kind: "preconditionFailed", subject, message, retryable } a precondition of the operation does not hold
//! ```
//!
//! `retryable` is the core's rule for the cause. A contract violation never
//! clears, so its record has no such field. A server error carries the
//! server's primary message; every other cause carries the text of the core
//! error.

use horsies_core::{BrokerError, CoveragePassError, FailureCause, WorkflowError};
use napi_derive::napi;

/// The cause of a failed core operation.
#[napi(discriminant = "kind", discriminant_case = "camelCase")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CauseRecord {
    /// The server answered with an error.
    Server {
        /// The core operation that failed, for example `ping_database`.
        subject: String,
        /// The five-character SQLSTATE of the server's error.
        sqlstate: String,
        /// The server's primary message.
        message: String,
        /// Whether the cause may clear, so a repeat of the operation can
        /// succeed.
        retryable: bool,
    },
    /// The operation got no answer from the server: a connect or a checkout
    /// that failed or timed out, a lost connection, or a reply that did not
    /// come within its bound.
    Connection {
        subject: String,
        message: String,
        retryable: bool,
    },
    /// A row, a stored value or a result broke the contract that the core
    /// reads it with. The same call meets it again.
    ContractViolation { subject: String, message: String },
    /// A precondition of the operation does not hold: the settings, the
    /// database's client family or schema, an argument, a row that the call
    /// names, a key or a lock that another command holds.
    PreconditionFailed {
        subject: String,
        message: String,
        /// Whether the precondition may come to hold without a change by
        /// the caller, for example a lock that its holder releases.
        retryable: bool,
    },
}

impl CauseRecord {
    /// The cause record of a failure of the operation `subject`, from the
    /// text, the retry rule and the class of its cause. Pure.
    pub fn new(subject: &str, text: String, retryable: bool, cause: FailureCause) -> Self {
        let subject = subject.to_owned();
        match cause {
            FailureCause::Server(facts) => Self::Server {
                subject,
                sqlstate: facts.sqlstate().code().to_owned(),
                message: facts.primary_message().to_owned(),
                retryable,
            },
            FailureCause::Connection => Self::Connection {
                subject,
                message: text,
                retryable,
            },
            FailureCause::ContractViolation => Self::ContractViolation {
                subject,
                message: text,
            },
            FailureCause::PreconditionFailed => Self::PreconditionFailed {
                subject,
                message: text,
                retryable,
            },
        }
    }

    /// The cause record of the core error `error` of the operation
    /// `subject`. Pure.
    pub fn of(subject: &str, error: &impl CoreFailureSource) -> Self {
        Self::new(
            subject,
            error.to_string(),
            error.cause_retryable(),
            error.cause_class(),
        )
    }
}

/// A core error that a failure record carries as its cause: its text, its
/// class of cause and its retry rule.
pub trait CoreFailureSource: std::fmt::Display {
    fn cause_class(&self) -> FailureCause;
    fn cause_retryable(&self) -> bool;
}

impl CoreFailureSource for BrokerError {
    fn cause_class(&self) -> FailureCause {
        self.failure_cause()
    }
    fn cause_retryable(&self) -> bool {
        self.is_retryable()
    }
}

impl CoreFailureSource for WorkflowError {
    fn cause_class(&self) -> FailureCause {
        self.failure_cause()
    }
    fn cause_retryable(&self) -> bool {
        self.is_retryable()
    }
}

impl CoreFailureSource for CoveragePassError {
    fn cause_class(&self) -> FailureCause {
        self.failure_cause()
    }
    fn cause_retryable(&self) -> bool {
        self.is_retryable()
    }
}

#[cfg(test)]
mod tests {
    use horsies_core::ServerErrorFacts;

    use super::*;

    #[test]
    fn a_server_error_keeps_its_sqlstate_and_the_server_message() {
        let record = CauseRecord::new(
            "cancel_task",
            "database error: canceling statement".to_owned(),
            true,
            FailureCause::Server(ServerErrorFacts::of_code(
                "57014",
                "canceling statement due to statement timeout".to_owned(),
            )),
        );
        assert_eq!(
            record,
            CauseRecord::Server {
                subject: "cancel_task".to_owned(),
                sqlstate: "57014".to_owned(),
                message: "canceling statement due to statement timeout".to_owned(),
                retryable: true,
            }
        );
    }

    #[test]
    fn a_connection_failure_keeps_the_text_of_the_error() {
        let record = CauseRecord::new(
            "ping_database",
            "connection failed: refused".to_owned(),
            true,
            FailureCause::Connection,
        );
        assert_eq!(
            record,
            CauseRecord::Connection {
                subject: "ping_database".to_owned(),
                message: "connection failed: refused".to_owned(),
                retryable: true,
            }
        );
    }

    #[test]
    fn a_contract_violation_has_no_retry_rule() {
        let error = BrokerError::ClaimContract("ordinality 3 has no answer".to_owned());
        assert_eq!(
            CauseRecord::of("release_claims", &error),
            CauseRecord::ContractViolation {
                subject: "release_claims".to_owned(),
                message: error.to_string(),
            }
        );
    }

    #[test]
    fn a_precondition_keeps_its_retry_rule() {
        let record = CauseRecord::new(
            "install_schema",
            "the database holds the schema of another client family".to_owned(),
            false,
            FailureCause::PreconditionFailed,
        );
        assert_eq!(
            record,
            CauseRecord::PreconditionFailed {
                subject: "install_schema".to_owned(),
                message: "the database holds the schema of another client family".to_owned(),
                retryable: false,
            }
        );
    }
}
