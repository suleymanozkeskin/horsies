//! History/database contract errors.

#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

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
}
