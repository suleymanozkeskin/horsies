//! Typed failures returned by monitoring reads.

use serde::{Deserialize, Serialize};

use crate::broker::BrokerError;
use crate::core::history::errors::HistoryError;

/// Failure category for a monitoring read.
///
/// Each member names one variant of `MonitoringQueryError`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_core_derive::CodeCatalog,
)]
#[code_catalog(module = "monitoring", base = "str_enum")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MonitoringQueryErrorCode {
    /// The read failed with a database or driver error, or the core of this
    /// process could not run it. `retryable` is the core's classification of
    /// the cause.
    DatabaseOperationFailed,
    /// Stored history data does not decode: a status that is not a task
    /// status, an attempt snapshot or archive envelope that does not decode.
    /// Not retryable: a repeat gives the same result until the stored row
    /// changes (Python `clients/python/horsies/monitoring/errors.py:26-98`).
    HistoryContractViolation,
}

/// The code of a monitoring query that a client refuses before any connect:
/// a window, a page or a limit that the core refuses, or a value that the
/// client's types let through and the core cannot take (a count that is not
/// a whole number, a task index outside 32 bits, text that is not
/// well-formed). The Python client has no code for these inputs: its parse
/// functions answer `WindowRefused` and `PageRefused`, and its types cannot
/// hold the other values.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_core_derive::CodeCatalog,
)]
#[code_catalog(module = "monitoring", base = "str_enum")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MonitoringQueryRefusalCode {
    /// A value of the query is refused; no connect and no statement ran.
    InvalidInput,
}

#[derive(Debug, thiserror::Error)]
pub enum MonitoringQueryErrorSource {
    #[error(transparent)]
    History(#[from] HistoryError),
    #[error(transparent)]
    Broker(#[from] BrokerError),
}

impl MonitoringQueryErrorSource {
    /// Whether the error may clear: the error's own rule. Pure.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::History(error) => error.is_retryable(),
            Self::Broker(error) => error.is_retryable(),
        }
    }
}

/// The error of a read that met stored history data outside the read
/// contract: the cause of `HistoryContractViolation`. Python's exception of
/// the class is a `HistoryContractError` with the error's text.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct HistoryContractSource(pub MonitoringQueryErrorSource);

/// A failed monitoring read: one variant per class of Python's monitoring
/// query catalog, each with the facts of its state. A monitoring read
/// changes nothing. Each variant's doc is its Python class doc
/// (`ErrorCatalog`).
#[derive(
    Debug,
    thiserror::Error,
    horsies_core_derive::ErrorCatalog,
    horsies_core_derive::ErrorClassValues,
)]
#[error_catalog(
    module = "monitoring_errors",
    code = "MonitoringQueryErrorCode",
    union = "MonitoringQueryCoreError",
    union_doc = "Every failure of a monitoring query that the core gives: one class per state."
)]
pub enum MonitoringQueryError {
    /// The query failed with a database or driver error, or the core of this
    /// process could not run it. Nothing changed.
    ///
    /// ``retryable`` is the core's classification of the cause: ``True`` when
    /// the condition may clear, for example a lost connection. ``cause``
    /// is the core's class of the cause (a ``CoreFailure`` leaf class, with
    /// the read as its subject), or the ``CoreCallRefusal`` of the core
    /// boundary.
    #[error("{message}")]
    #[error_class(
        class = "MonitoringQueryDatabaseOperationFailed",
        code = "DatabaseOperationFailed",
        exception = "BaseException"
    )]
    DatabaseOperationFailed {
        #[error_slot(python = "Str", hidden)]
        message: String,
        #[error_slot(python = "Bool")]
        retryable: bool,
        /// The error that the read met; the exception is the core's class
        /// of its cause, with its text.
        #[error_slot(cause)]
        #[source]
        cause: MonitoringQueryErrorSource,
    },
    /// Stored history data does not decode. Nothing changed.
    ///
    /// ``cause`` is the ``HistoryContractError``: for example a history
    /// status that is not a ``TaskStatus`` member, an attempt snapshot that
    /// does not decode, or an EXPLAIN payload that the row estimate cannot
    /// read. A repeat gives the same result until the stored row or the
    /// server changes.
    #[error("{message}")]
    #[error_class(
        class = "MonitoringQueryHistoryContractViolation",
        code = "HistoryContractViolation",
        never_retryable,
        exception = "BaseException"
    )]
    HistoryContractViolation {
        #[error_slot(python = "Str", hidden)]
        message: String,
        /// The error that the read met; the exception is a
        /// `HistoryContractError` with its text.
        #[error_slot(cause)]
        #[source]
        cause: HistoryContractSource,
    },
}

impl MonitoringQueryError {
    /// The catalog code of the variant. Pure.
    pub fn code(&self) -> MonitoringQueryErrorCode {
        crate::error_catalog::ErrorEnum::error_code(self)
    }

    /// What failed and why. Pure.
    pub fn message(&self) -> &str {
        match self {
            Self::DatabaseOperationFailed { message, .. }
            | Self::HistoryContractViolation { message, .. } => message,
        }
    }

    /// Whether a repeat of the same read can succeed: the classification of
    /// the cause of a database failure; a broken read contract never
    /// clears. Pure.
    pub fn retryable(&self) -> bool {
        match self {
            Self::DatabaseOperationFailed { retryable, .. } => *retryable,
            Self::HistoryContractViolation { .. } => false,
        }
    }

    /// The error that the read met. Pure.
    pub fn error_source(&self) -> &MonitoringQueryErrorSource {
        match self {
            Self::DatabaseOperationFailed { cause, .. } => cause,
            Self::HistoryContractViolation { cause, .. } => &cause.0,
        }
    }

    /// The connect of the broker before the read `operation` failed: no
    /// statement ran. `DATABASE_OPERATION_FAILED` with the error's retry rule.
    /// Pure.
    pub fn connect_failed(operation: &str, error: BrokerError) -> Self {
        Self::DatabaseOperationFailed {
            message: format!("{operation} failed: {error}"),
            retryable: error.is_retryable(),
            cause: MonitoringQueryErrorSource::Broker(error),
        }
    }

    /// A history read failure: a failed statement keeps its transience;
    /// every other history error is stored data that does not decode.
    pub(crate) fn history(operation: &str, error: HistoryError) -> Self {
        let failure = match &error {
            HistoryError::Statement(_) | HistoryError::RowDecode(_) => ReadFailure::Database {
                retryable: error.is_retryable(),
            },
            HistoryError::ArchiveDecode(_)
            | HistoryError::Contract(_)
            | HistoryError::LeafLockNotHeld => ReadFailure::Contract,
        };
        failure.into_error(
            format!("{operation} failed: {error}"),
            MonitoringQueryErrorSource::History(error),
        )
    }

    /// A failure of a read on the statement layer: a stored history row that
    /// breaks the read contract is `HISTORY_CONTRACT_VIOLATION`, not
    /// retryable; every other failure is `DATABASE_OPERATION_FAILED` with the
    /// error's retryable flag (a statement failure by its transience; a
    /// column that does not decode is not retryable).
    pub(crate) fn broker(operation: &str, error: BrokerError) -> Self {
        let failure = match &error {
            BrokerError::HistoryReadContract(_) => ReadFailure::Contract,
            other => ReadFailure::Database {
                retryable: other.is_retryable(),
            },
        };
        failure.into_error(
            format!("{operation} failed: {error}"),
            MonitoringQueryErrorSource::Broker(error),
        )
    }
}

/// The class of a read failure, found before its error moves into the
/// variant.
enum ReadFailure {
    Database { retryable: bool },
    Contract,
}

impl ReadFailure {
    /// The variant of this class with `message` and `cause`. Pure.
    fn into_error(
        self,
        message: String,
        cause: MonitoringQueryErrorSource,
    ) -> MonitoringQueryError {
        match self {
            Self::Database { retryable } => MonitoringQueryError::DatabaseOperationFailed {
                message,
                retryable,
                cause,
            },
            Self::Contract => MonitoringQueryError::HistoryContractViolation {
                message,
                cause: HistoryContractSource(cause),
            },
        }
    }
}

pub type MonitoringResult<T> = Result<T, MonitoringQueryError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_history_statement_failure_keeps_its_transience() {
        let error = MonitoringQueryError::history(
            "read",
            HistoryError::Statement(crate::db::StatementFailure::NotSent {
                statement: "history_task_detail",
                cause: crate::db::NotSentCause::Checkout(crate::db::CheckoutFailure::TimedOut {
                    role: crate::db::PoolRole::Main,
                    waited: std::time::Duration::from_secs(30),
                }),
            }),
        );

        assert_eq!(
            error.code(),
            MonitoringQueryErrorCode::DatabaseOperationFailed
        );
        assert!(error.retryable());
    }

    #[test]
    fn a_statement_failure_keeps_its_transience_and_a_contract_break_is_a_violation() {
        let timed_out = MonitoringQueryError::broker(
            "read",
            BrokerError::Statement(crate::db::StatementFailure::NotSent {
                statement: "monitoring_task_list",
                cause: crate::db::NotSentCause::Checkout(crate::db::CheckoutFailure::TimedOut {
                    role: crate::db::PoolRole::Main,
                    waited: std::time::Duration::from_secs(30),
                }),
            }),
        );
        assert_eq!(
            timed_out.code(),
            MonitoringQueryErrorCode::DatabaseOperationFailed
        );
        assert!(timed_out.retryable());

        let broken = MonitoringQueryError::broker(
            "read",
            BrokerError::HistoryReadContract("unknown location".to_owned()),
        );
        assert_eq!(
            broken.code(),
            MonitoringQueryErrorCode::HistoryContractViolation
        );
        assert!(!broken.retryable());
    }

    /// Python `test_monitoring_queries.py::TestGetTaskDetail::
    /// test_history_row_with_an_unknown_status_is_a_contract_violation`.
    #[test]
    fn stored_history_data_that_does_not_decode_is_a_contract_violation() {
        let error = MonitoringQueryError::history(
            "task detail query",
            HistoryError::contract("history status 'BOGUS' is not a task status"),
        );

        assert_eq!(
            error.code(),
            MonitoringQueryErrorCode::HistoryContractViolation
        );
        assert!(!error.retryable());
        assert_eq!(
            serde_json::to_string(&error.code()).unwrap(),
            "\"HISTORY_CONTRACT_VIOLATION\""
        );
    }
}
