use crate::broker::BrokerError;
use crate::core::HorsiesError;
use crate::worker::WorkerError;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error(transparent)]
    Validation(#[from] HorsiesError),

    #[error(transparent)]
    Broker(#[from] BrokerError),

    #[error(transparent)]
    Worker(#[from] WorkerError),

    #[error("invalid worker configuration: {0}")]
    InvalidWorkerConfig(String),

    #[error("scheduler configuration error: {0}")]
    SchedulerConfig(String),

    #[error("scheduler task join error: {0}")]
    SchedulerJoin(#[from] tokio::task::JoinError),
}

pub type AppResult<T> = Result<T, AppError>;
