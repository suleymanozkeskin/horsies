//! History/database contract errors.

use crate::broker::core_rows::RowDecodeError;
use crate::broker::is_retryable_statement_failure;
use crate::db::StatementFailure;

/// The SQLSTATE of a terminal write whose row no history leaf can take: the
/// class partition or the leaf of the row's UTC day does not exist
/// (`horsies_require_history_leaf`, `crates/horsies-core/baseline/0006_history_functions.sql`).
/// The message names the class and the missing range; DETAIL names the
/// missing relation. Nothing of the write committed. The code is in class 55,
/// object not in prerequisite state, so the write is retryable: the next
/// coverage pass (the schema install, the worker start, the reaper) creates
/// the leaf, and the same write then succeeds.
pub const HISTORY_LEAF_MISSING_SQLSTATE: &str = "55H01";

#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
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
}

impl HistoryError {
    pub fn contract(detail: impl Into<String>) -> Self {
        Self::Contract(detail.into())
    }

    /// Whether the call may be retried: a core statement failure by its
    /// transience. Every other variant is not retryable (the rule of
    /// `WorkerError::is_retryable` for history errors).
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Statement(failure) => is_retryable_statement_failure(failure),
            Self::RowDecode(_)
            | Self::ArchiveDecode(_)
            | Self::Contract(_)
            | Self::LeafLockNotHeld => false,
        }
    }
}
