use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sqlx::postgres::PgPool;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::broker::error::BrokerError;

/// A shared LISTEN/NOTIFY listener that multiplexes one `PgListener` connection
/// across multiple subscribers, each waiting for a specific payload ID.
///
/// Instead of each `get_result()` / `get_workflow_result()` call creating its
/// own `PgListener` (pinning one pool connection per caller), a single
/// `SharedNotifyListener` holds one connection and fans out notifications to
/// registered subscribers by matching the notification payload against
/// subscriber IDs.
pub struct SharedNotifyListener {
    subscribers: Arc<Mutex<HashMap<String, Vec<mpsc::Sender<()>>>>>,
    task_handle: JoinHandle<()>,
}

/// A subscription handle returned by [`SharedNotifyListener::subscribe`].
///
/// Receives a wake-up `()` whenever the shared listener receives a
/// notification whose payload matches the subscribed ID. Drop this to
/// unsubscribe (the corresponding sender is cleaned up lazily on the next
/// fanout).
pub struct Subscription {
    rx: mpsc::Receiver<()>,
}

impl SharedNotifyListener {
    /// Create a new shared listener for the given NOTIFY channel.
    ///
    /// Spawns a background task that runs a single `PgListener` and fans
    /// out notifications to subscribers. The listener connection is taken
    /// from the pool (1 connection held for the lifetime of this struct).
    pub async fn new(pool: &PgPool, channel: &str) -> Result<Self, BrokerError> {
        let mut listener = sqlx::postgres::PgListener::connect_with(pool)
            .await
            .map_err(BrokerError::Database)?;
        listener
            .listen(channel)
            .await
            .map_err(BrokerError::Database)?;

        let subscribers: Arc<Mutex<HashMap<String, Vec<mpsc::Sender<()>>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let subs = Arc::clone(&subscribers);
        let channel_name = channel.to_owned();

        let task_handle = tokio::spawn(async move {
            loop {
                match listener.recv().await {
                    Ok(notif) => {
                        let payload = notif.payload();
                        let mut map = match subs.lock() {
                            Ok(guard) => guard,
                            Err(poisoned) => {
                                tracing::error!("subscriber map mutex poisoned, recovering");
                                poisoned.into_inner()
                            }
                        };
                        if let Some(senders) = map.get_mut(payload) {
                            // Send wake-up to all subscribers; remove closed ones.
                            // Use try_send to avoid blocking; full channels are ok (coalesce).
                            senders.retain(|tx| !tx.is_closed());
                            for tx in senders.iter() {
                                let _ = tx.try_send(()); // Ignore full/closed errors
                            }
                            if senders.is_empty() {
                                map.remove(payload);
                            }
                        }
                    }
                    Err(e) => {
                        // sqlx PgListener reconnects automatically on network
                        // failures and re-subscribes to channels. Log the error
                        // and pause briefly to avoid a tight spin if reconnection
                        // keeps failing.
                        tracing::error!(
                            channel = %channel_name,
                            error = %e,
                            "shared listener error, sqlx will attempt reconnect",
                        );
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        });

        Ok(Self {
            subscribers,
            task_handle,
        })
    }

    /// Register a subscriber for notifications with the given payload ID.
    ///
    /// Returns a [`Subscription`] that receives a `()` wake-up each time
    /// a notification arrives whose payload matches `id`. The caller
    /// should then query the database to check the actual state (same
    /// pattern as the non-shared listener).
    ///
    /// Drop the `Subscription` to unsubscribe. Cleanup is lazy: the
    /// dead sender is removed on the next fanout for that ID.
    pub fn subscribe(&self, id: &str) -> Subscription {
        // Use a small bounded channel - notifications are just wake-up signals.
        // If the buffer fills, the sender will drop overflow (see fanout loop).
        let (tx, rx) = mpsc::channel(8);
        let mut map = match self.subscribers.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::error!("subscriber map mutex poisoned in subscribe, recovering");
                poisoned.into_inner()
            }
        };
        map.entry(id.to_owned()).or_default().push(tx);
        Subscription { rx }
    }
}

impl Drop for SharedNotifyListener {
    fn drop(&mut self) {
        self.task_handle.abort();
    }
}

impl Subscription {
    /// Wait for the next wake-up notification.
    ///
    /// Returns `Ok(())` when a matching notification arrived, or
    /// `Err(BrokerError::ListenerClosed)` if the shared listener was
    /// dropped / its background task stopped.
    pub async fn recv(&mut self) -> Result<(), BrokerError> {
        self.rx.recv().await.ok_or(BrokerError::ListenerClosed)
    }
}
