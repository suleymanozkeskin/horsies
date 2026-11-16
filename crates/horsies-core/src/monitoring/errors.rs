//! Typed failures returned by monitoring reads.

use serde::{Deserialize, Serialize};

use crate::broker::is_retryable_sqlx_error;
use crate::core::history::errors::HistoryError;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::CodeCatalog,
)]
#[code_catalog(module = "monitoring", base = "str_enum")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MonitoringQueryErrorCode {
    /// The read failed with a database or driver error. `retryable` is the
    /// driver classification of the cause.
    DbOperationFailed,
    /// Stored history data does not decode: a status that is not a task
    /// status, an attempt snapshot or archive envelope that does not decode.
    /// Not retryable: a repeat gives the same result until the stored row
    /// changes (Python `horsies/monitoring/errors.py:26-98`).
    HistoryContractViolation,
}

#[derive(Debug, thiserror::Error)]
pub enum MonitoringQueryErrorSource {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    History(#[from] HistoryError),
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct MonitoringQueryError {
    pub code: MonitoringQueryErrorCode,
    pub message: String,
    pub retryable: bool,
    #[source]
    pub source: MonitoringQueryErrorSource,
}

impl MonitoringQueryError {
    pub(crate) fn database(operation: &str, error: sqlx::Error) -> Self {
        Self {
            code: MonitoringQueryErrorCode::DbOperationFailed,
            message: format!("{operation} failed: {error}"),
            retryable: is_retryable_sqlx_error(&error),
            source: MonitoringQueryErrorSource::Database(error),
        }
    }

    /// A history read failure: a database error keeps its driver
    /// classification; every other history error is stored data that does
    /// not decode.
    pub(crate) fn history(operation: &str, error: HistoryError) -> Self {
        let (code, retryable) = match &error {
            HistoryError::Database(database) => (
                MonitoringQueryErrorCode::DbOperationFailed,
                is_retryable_sqlx_error(database),
            ),
            HistoryError::ArchiveDecode(_)
            | HistoryError::Contract(_)
            | HistoryError::LeafLockNotHeld
            | HistoryError::LeafLockBusy { .. }
            | HistoryError::HistoryParentAbsent(_) => {
                (MonitoringQueryErrorCode::HistoryContractViolation, false)
            }
        };
        Self {
            code,
            message: format!("{operation} failed: {error}"),
            retryable,
            source: MonitoringQueryErrorSource::History(error),
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

        assert_eq!(error.code, MonitoringQueryErrorCode::DbOperationFailed);
        assert!(error.retryable);
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
            error.code,
            MonitoringQueryErrorCode::HistoryContractViolation
        );
        assert!(!error.retryable);
        assert_eq!(
            serde_json::to_string(&error.code).unwrap(),
            "\"HISTORY_CONTRACT_VIOLATION\""
        );
    }
}
