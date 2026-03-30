use sqlx::postgres::{PgListener, PgNotification, PgPool};

use crate::broker::error::BrokerError;

/// Thin wrapper around `sqlx::postgres::PgListener`.
///
/// Handles channel subscriptions and notification delivery.
/// sqlx manages reconnection internally.
pub struct NotifyListener {
    inner: PgListener,
}

impl NotifyListener {
    /// Create a new listener connected to the given pool.
    pub async fn connect(pool: &PgPool) -> Result<Self, BrokerError> {
        let listener = PgListener::connect_with(pool)
            .await
            .map_err(BrokerError::Database)?;
        Ok(Self { inner: listener })
    }

    /// Subscribe to a single channel.
    pub async fn listen(&mut self, channel: &str) -> Result<(), BrokerError> {
        self.inner
            .listen(channel)
            .await
            .map_err(BrokerError::Database)
    }

    /// Subscribe to multiple channels.
    pub async fn listen_all(&mut self, channels: &[&str]) -> Result<(), BrokerError> {
        for channel in channels {
            self.listen(channel).await?;
        }
        Ok(())
    }

    /// Unsubscribe from a channel.
    pub async fn unlisten(&mut self, channel: &str) -> Result<(), BrokerError> {
        self.inner
            .unlisten(channel)
            .await
            .map_err(BrokerError::Database)
    }

    /// Wait for the next notification.
    pub async fn recv(&mut self) -> Result<PgNotification, BrokerError> {
        self.inner.recv().await.map_err(BrokerError::Database)
    }

    /// Non-blocking receive: returns `Ok(Some(notification))` if one is
    /// already buffered, `Ok(None)` if no notification is ready, or an
    /// error on connection failure.
    ///
    /// Used by `coalesce_notifies` to drain burst notifications without
    /// blocking the main loop.
    pub async fn try_recv(&mut self) -> Result<Option<PgNotification>, BrokerError> {
        self.inner.try_recv().await.map_err(BrokerError::Database)
    }

    /// Get the underlying `PgListener` for advanced usage.
    pub fn inner_mut(&mut self) -> &mut PgListener {
        &mut self.inner
    }
}
