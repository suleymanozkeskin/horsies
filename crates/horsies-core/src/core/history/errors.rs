//! History/database contract errors.

use crate::broker::core_rows::RowDecodeError;
use crate::broker::{is_retryable_sqlx_error, is_retryable_statement_failure};
use crate::db::StatementFailure;

#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// A statement on a core pool failed with one of the statement layer's
    /// three outcomes. Retryable when its cause may clear.
    #[error("database error: {0}")]
    Statement(#[from] StatementFailure),

    /// A row of a core statement does not have the shape its reader
    /// requires. Not retryable.
    #[error("{0}")]
    RowDecode(#[from] RowDecodeError),

    #[error("history archive decode error: {0}")]
    ArchiveDecode(#[from] crate::core::history::archive::versions::ArchiveDecodeError),

    #[error("history contract violation: {0}")]
    Contract(String),

    #[error("history leaf advisory lock is not held")]
    LeafLockNotHeld,

    #[error("history leaf advisory lock is busy: {leaf_name}")]
    LeafLockBusy { leaf_name: String },

    #[error("history parent is absent: {0}")]
    HistoryParentAbsent(String),
}

impl HistoryError {
    pub fn contract(detail: impl Into<String>) -> Self {
        Self::Contract(detail.into())
    }

    /// Whether the call may be retried: a driver error by the driver
    /// classification, a core statement failure by its transience. Every
    /// other variant is not retryable (the rule of
    /// `WorkerError::is_retryable` for history errors).
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Database(error) => is_retryable_sqlx_error(error),
            Self::Statement(failure) => is_retryable_statement_failure(failure),
            Self::RowDecode(_)
            | Self::ArchiveDecode(_)
            | Self::Contract(_)
            | Self::LeafLockNotHeld
            | Self::LeafLockBusy { .. }
            | Self::HistoryParentAbsent(_) => false,
        }
    }
}
