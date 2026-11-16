//! Typed failures returned by monitoring reads.

use serde::{Deserialize, Serialize};

use crate::broker::is_retryable_sqlx_error;
use crate::core::history::errors::HistoryError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MonitoringQueryErrorCode {
    DbOperationFailed,
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

    pub(crate) fn history(operation: &str, error: HistoryError) -> Self {
        let retryable = match &error {
            HistoryError::Database(database) => is_retryable_sqlx_error(database),
            _ => false,
        };
        Self {
            code: MonitoringQueryErrorCode::DbOperationFailed,
            message: format!("{operation} failed: {error}"),
            retryable,
            source: MonitoringQueryErrorSource::History(error),
        }
    }
}

pub type MonitoringResult<T> = Result<T, MonitoringQueryError>;
