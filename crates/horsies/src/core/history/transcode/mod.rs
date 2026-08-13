//! Replacement-partition archive transcode.

pub mod executor;
pub mod jobs;
pub mod maintenance;
pub mod outcomes;
pub mod signature;
pub mod transforms;

use crate::core::history::errors::HistoryError;
use crate::core::history::maintenance::gate::MaintenanceSessionError;

#[derive(Debug, thiserror::Error)]
pub enum TranscodeError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    History(#[from] HistoryError),
    #[error(transparent)]
    Maintenance(#[from] MaintenanceSessionError),
    #[error("invalid transcode argument: {0}")]
    InvalidArgument(String),
    #[error("invalid transcode state: {0}")]
    State(String),
    #[error("transcode contract violation: {0}")]
    Contract(String),
}

impl TranscodeError {
    pub(crate) fn state(message: impl Into<String>) -> Self {
        Self::State(message.into())
    }

    pub(crate) fn contract(message: impl Into<String>) -> Self {
        Self::Contract(message.into())
    }
}

#[cfg(test)]
mod tests;
