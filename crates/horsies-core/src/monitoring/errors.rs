//! Typed failures returned by monitoring reads.

use serde::{Deserialize, Serialize};

use crate::broker::{is_retryable_sqlx_error, BrokerError};
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
    /// The read failed with a database or driver error. `retryable` is the
    /// driver classification of the cause.
    DbOperationFailed,
    /// The broker already holds resource sets for its limit of open event
    /// loops, and this call ran on a new one. Nothing ran. Python client
    /// only; removed before 0.8.0.
    EventLoopLimit,
    /// Stored history data does not decode: a status that is not a task
    /// status, an attempt snapshot or archive envelope that does not decode.
    /// Not retryable: a repeat gives the same result until the stored row
    /// changes (Python `python/horsies/monitoring/errors.py:26-98`).
    HistoryContractViolation,
}

#[derive(Debug, thiserror::Error)]
pub enum MonitoringQueryErrorSource {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    History(#[from] HistoryError),
    #[error(transparent)]
    Broker(#[from] BrokerError),
}

/// A failed monitoring read: one variant per class of Python's monitoring
/// query catalog that the core produces, each with the facts of its state.
/// Python's `MonitoringQueryEventLoopLimit` is a class of the Python client
/// only, so it is not a variant. A monitoring read changes nothing. Each
/// variant's doc is its Python class doc (`ErrorCatalog`).
#[derive(Debug, thiserror::Error, horsies_core_derive::ErrorCatalog)]
#[error_catalog(
    module = "monitoring_errors",
    code = "MonitoringQueryErrorCode",
    union = "MonitoringQueryCoreError",
    union_doc = "Every failure of a monitoring query that the core gives: one class per state."
)]
pub enum MonitoringQueryError {
    /// The query failed with a database or driver error. Nothing changed.
    ///
    /// ``retryable`` is the driver classification of the cause: ``True`` when
    /// the condition may clear, for example a lost connection. ``exception``
    /// is the ``SQLAlchemyError``.
    #[error("{message}")]
    #[error_class(
        class = "MonitoringQueryDbOperationFailed",
        code = "DbOperationFailed",
        exception = "BaseException"
    )]
    DbOperationFailed {
        #[error_slot(python = "Str", hidden)]
        message: String,
        #[error_slot(python = "Bool")]
        retryable: bool,
        #[error_slot(skip)]
        #[source]
        source: MonitoringQueryErrorSource,
    },
    /// Stored history data does not decode. Nothing changed.
    ///
    /// ``exception`` is the ``HistoryContractError``: for example a history
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
        #[error_slot(skip)]
        #[source]
        source: MonitoringQueryErrorSource,
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
            Self::DbOperationFailed { message, .. }
            | Self::HistoryContractViolation { message, .. } => message,
        }
    }

    /// Whether a repeat of the same read can succeed: the classification of
    /// the cause of a database failure; a broken read contract never
    /// clears. Pure.
    pub fn retryable(&self) -> bool {
        match self {
            Self::DbOperationFailed { retryable, .. } => *retryable,
            Self::HistoryContractViolation { .. } => false,
        }
    }

    /// The error that the read met. Pure.
    pub fn error_source(&self) -> &MonitoringQueryErrorSource {
        match self {
            Self::DbOperationFailed { source, .. }
            | Self::HistoryContractViolation { source, .. } => source,
        }
    }

    /// A history read failure: a database error keeps its driver
    /// classification; every other history error is stored data that does
    /// not decode.
    pub(crate) fn history(operation: &str, error: HistoryError) -> Self {
        let failure = match &error {
            HistoryError::Database(database) => ReadFailure::Database {
                retryable: is_retryable_sqlx_error(database),
            },
            HistoryError::Statement(_) | HistoryError::RowDecode(_) => ReadFailure::Database {
                retryable: error.is_retryable(),
            },
            HistoryError::ArchiveDecode(_)
            | HistoryError::Contract(_)
            | HistoryError::LeafLockNotHeld
            | HistoryError::LeafLockBusy { .. }
            | HistoryError::HistoryParentAbsent(_) => ReadFailure::Contract,
        };
        failure.into_error(
            format!("{operation} failed: {error}"),
            MonitoringQueryErrorSource::History(error),
        )
    }

    /// A failure of a read on the statement layer: a stored history row that
    /// breaks the read contract is `HISTORY_CONTRACT_VIOLATION`, not
    /// retryable; every other failure is `DB_OPERATION_FAILED` with the
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
    /// The variant of this class with `message` and `source`. Pure.
    fn into_error(
        self,
        message: String,
        source: MonitoringQueryErrorSource,
    ) -> MonitoringQueryError {
        match self {
            Self::Database { retryable } => MonitoringQueryError::DbOperationFailed {
                message,
                retryable,
                source,
            },
            Self::Contract => MonitoringQueryError::HistoryContractViolation { message, source },
        }
    }
}

pub type MonitoringResult<T> = Result<T, MonitoringQueryError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_history_database_error_keeps_the_driver_classification() {
        let error = MonitoringQueryError::history(
            "read",
            HistoryError::Database(sqlx::Error::PoolTimedOut),
        );

        assert_eq!(error.code(), MonitoringQueryErrorCode::DbOperationFailed);
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
            MonitoringQueryErrorCode::DbOperationFailed
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
