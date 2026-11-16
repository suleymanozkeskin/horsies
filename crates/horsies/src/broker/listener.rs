use sqlx::postgres::{PgListener, PgNotification, PgPool};

use crate::broker::error::BrokerError;

/// Send a task UUID as a text payload to its queue notification channel.
pub(crate) async fn notify_task_queue(
    pool: &PgPool,
    queue_name: &str,
    task_id: uuid::Uuid,
) -> Result<(), BrokerError> {
    let channel = format!("task_queue_{queue_name}");
    let payload = task_id.to_string();
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(channel)
        .bind(payload)
        .execute(pool)
        .await
        .map_err(BrokerError::Database)?;
    Ok(())
}

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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use uuid::Uuid;

    use super::{notify_task_queue, NotifyListener};

    #[tokio::test]
    async fn task_queue_notification_sends_uuid_as_text() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let queue_name = format!("notify_{}", Uuid::new_v4().simple());
        let channel = format!("task_queue_{queue_name}");
        let task_id = Uuid::new_v4();
        let mut listener = NotifyListener::connect(&pool).await.unwrap();
        listener.listen(&channel).await.unwrap();

        notify_task_queue(&pool, &queue_name, task_id)
            .await
            .unwrap();

        let notification = tokio::time::timeout(Duration::from_secs(2), listener.recv())
            .await
            .expect("task queue notification timeout")
            .unwrap();
        assert_eq!(notification.channel(), channel);
        assert_eq!(notification.payload(), task_id.to_string());
    }
}
