use std::sync::Arc;

use crate::broker::{BrokerError, PostgresBroker};
use crate::core::PostgresConfig;
use tokio::sync::OnceCell;

pub struct LazyBroker {
    config: PostgresConfig,
    broker: OnceCell<Arc<PostgresBroker>>,
}

impl LazyBroker {
    pub fn new(config: PostgresConfig) -> Self {
        Self {
            config,
            broker: OnceCell::new(),
        }
    }

    pub async fn get(&self) -> Result<Arc<PostgresBroker>, BrokerError> {
        self.broker
            .get_or_try_init(|| async {
                let broker = PostgresBroker::connect_with(&self.config).await?;
                Ok::<Arc<PostgresBroker>, BrokerError>(Arc::new(broker))
            })
            .await
            .map(Arc::clone)
    }

    pub fn set(&self, broker: Arc<PostgresBroker>) -> Result<(), Arc<PostgresBroker>> {
        match self.broker.set(broker) {
            Ok(()) => Ok(()),
            Err(tokio::sync::SetError::AlreadyInitializedError(broker))
            | Err(tokio::sync::SetError::InitializingError(broker)) => Err(broker),
        }
    }

    pub fn get_if_initialized(&self) -> Option<Arc<PostgresBroker>> {
        self.broker.get().map(Arc::clone)
    }
}

impl std::fmt::Debug for LazyBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyBroker")
            .field("initialized", &self.broker.initialized())
            .finish()
    }
}
