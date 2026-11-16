use std::sync::Arc;

use crate::broker::{BrokerError, PostgresBroker, SchemaInitializationMode};
use crate::core::{PostgresConfig, WorkerResilienceConfig};
use crate::worker::backoff::RetryBackoff;
use tokio::sync::OnceCell;

pub struct LazyBroker {
    config: PostgresConfig,
    schema_initialization_mode: SchemaInitializationMode,
    broker: OnceCell<Arc<PostgresBroker>>,
}

impl LazyBroker {
    pub fn new(config: PostgresConfig) -> Self {
        Self {
            config,
            schema_initialization_mode: SchemaInitializationMode::MigrateAndValidate,
            broker: OnceCell::new(),
        }
    }

    pub fn observe_only(config: PostgresConfig) -> Self {
        Self {
            config,
            schema_initialization_mode: SchemaInitializationMode::ObserveOnly,
            broker: OnceCell::new(),
        }
    }

    pub async fn get(&self) -> Result<Arc<PostgresBroker>, BrokerError> {
        self.broker
            .get_or_try_init(|| async {
                let broker = match self.schema_initialization_mode {
                    SchemaInitializationMode::MigrateAndValidate => {
                        PostgresBroker::connect_with(&self.config).await?
                    }
                    SchemaInitializationMode::ObserveOnly => {
                        PostgresBroker::connect_observe_only(&self.config).await?
                    }
                };
                Ok::<Arc<PostgresBroker>, BrokerError>(Arc::new(broker))
            })
            .await
            .map(Arc::clone)
    }

    pub async fn get_ready(
        &self,
        resilience: &WorkerResilienceConfig,
    ) -> Result<Arc<PostgresBroker>, BrokerError> {
        let mut backoff = RetryBackoff::from_config(resilience);

        loop {
            match self.try_get_ready_once().await {
                Ok(broker) => return Ok(broker),
                Err(err) if err.is_retryable() && backoff.can_retry() => {
                    let delay = backoff.next_delay_seconds();
                    tracing::error!(
                        error = %err,
                        attempt = backoff.attempts(),
                        delay_seconds = delay,
                        "broker startup failed, retrying"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs_f64(delay)).await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn try_get_ready_once(&self) -> Result<Arc<PostgresBroker>, BrokerError> {
        let broker = self.get().await?;
        match self.schema_initialization_mode {
            SchemaInitializationMode::MigrateAndValidate => {
                broker.ensure_schema_initialized().await?;
            }
            SchemaInitializationMode::ObserveOnly => {}
        }
        Ok(broker)
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
