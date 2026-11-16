use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sqlx::postgres::PgPool;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::broker::error::BrokerError;

/// Map from a NOTIFY payload ID to its live subscribers, each keyed by a unique
/// subscription id so a dropped [`Subscription`] can remove exactly its own
/// sender.
type SubscriberMap = Arc<Mutex<HashMap<String, HashMap<u64, mpsc::Sender<()>>>>>;

/// A shared LISTEN/NOTIFY listener that multiplexes one `PgListener` connection
/// across multiple subscribers, each waiting for a specific payload ID.
///
/// Instead of each `get_result()` / `get_workflow_result()` call creating its
/// own `PgListener` (pinning one pool connection per caller), a single
/// `SharedNotifyListener` holds one connection and fans out notifications to
/// registered subscribers by matching the notification payload against
/// subscriber IDs.
pub struct SharedNotifyListener {
    subscribers: SubscriberMap,
    next_sub_id: Arc<AtomicU64>,
    task_handle: JoinHandle<()>,
}

/// A subscription handle returned by [`SharedNotifyListener::subscribe`].
///
/// Receives a wake-up `()` whenever the shared listener receives a
/// notification whose payload matches the subscribed ID. Drop this to
/// unsubscribe: the corresponding sender (and the payload entry, once its last
/// subscriber is gone) is removed eagerly, without waiting for a matching
/// NOTIFY that may never arrive.
pub struct Subscription {
    rx: mpsc::Receiver<()>,
    subscribers: SubscriberMap,
    channel_id: String,
    sub_id: u64,
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

        let subscribers: SubscriberMap = Arc::new(Mutex::new(HashMap::new()));

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
                            // Drop any senders whose Subscription leaked without
                            // running Drop (defensive; Drop is the primary path),
                            // then wake the rest. try_send never blocks; a full
                            // channel coalesces (the wake-up is already pending).
                            senders.retain(|_sub_id, tx| !tx.is_closed());
                            for tx in senders.values() {
                                let _ = tx.try_send(());
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
            next_sub_id: Arc::new(AtomicU64::new(0)),
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
    /// Drop the `Subscription` to unsubscribe. Cleanup is eager: the sender is
    /// removed on drop, and the payload entry is removed once its last
    /// subscriber leaves — so entries for IDs that never receive a matching
    /// NOTIFY (result already visible on poll, timeout, lost NOTIFY) do not
    /// accumulate.
    pub fn subscribe(&self, id: &str) -> Subscription {
        // Use a small bounded channel - notifications are just wake-up signals.
        // If the buffer fills, the sender will drop overflow (see fanout loop).
        let (tx, rx) = mpsc::channel(8);
        let sub_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        {
            let mut map = match self.subscribers.lock() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    tracing::error!("subscriber map mutex poisoned in subscribe, recovering");
                    poisoned.into_inner()
                }
            };
            map.entry(id.to_owned()).or_default().insert(sub_id, tx);
        }
        Subscription {
            rx,
            subscribers: Arc::clone(&self.subscribers),
            channel_id: id.to_owned(),
            sub_id,
        }
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

impl Drop for Subscription {
    fn drop(&mut self) {
        let mut map = match self.subscribers.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::error!("subscriber map mutex poisoned in unsubscribe, recovering");
                poisoned.into_inner()
            }
        };
        if let Some(senders) = map.get_mut(&self.channel_id) {
            senders.remove(&self.sub_id);
            if senders.is_empty() {
                map.remove(&self.channel_id);
            }
        }
    }
}

#[cfg(test)]
impl SharedNotifyListener {
    /// Number of distinct payload IDs currently tracked (test-only).
    fn active_channel_count(&self) -> usize {
        self.subscribers.lock().expect("lock").len()
    }

    /// Number of live subscribers for one payload ID (test-only).
    fn subscriber_count(&self, id: &str) -> usize {
        self.subscribers
            .lock()
            .expect("lock")
            .get(id)
            .map_or(0, HashMap::len)
    }

    /// Build a listener whose background task is a no-op, so the subscribe/drop
    /// bookkeeping can be exercised without a live Postgres connection.
    fn new_for_test() -> Self {
        Self {
            subscribers: Arc::new(Mutex::new(HashMap::new())),
            next_sub_id: Arc::new(AtomicU64::new(0)),
            task_handle: tokio::spawn(async {}),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// N4: a dropped `Subscription` must remove its map entry immediately, even
    /// when no matching NOTIFY ever fans out for that ID — otherwise entries for
    /// results already visible on poll (or lost NOTIFYs) accumulate unboundedly.
    #[tokio::test]
    async fn dropped_subscription_is_cleaned_up_without_a_fanout() {
        let listener = SharedNotifyListener::new_for_test();
        {
            let _sub = listener.subscribe("task-42");
            assert_eq!(
                listener.active_channel_count(),
                1,
                "subscribe registers the id"
            );
        }
        assert_eq!(
            listener.active_channel_count(),
            0,
            "dropping the Subscription must remove its entry with no fanout",
        );
    }

    /// Dropping one of several subscribers on the same ID keeps the others; the
    /// ID entry is removed only once its last subscriber leaves.
    #[tokio::test]
    async fn dropping_one_subscriber_keeps_the_others() {
        let listener = SharedNotifyListener::new_for_test();
        let sub1 = listener.subscribe("task-x");
        let sub2 = listener.subscribe("task-x");
        assert_eq!(listener.subscriber_count("task-x"), 2);

        drop(sub1);
        assert_eq!(
            listener.subscriber_count("task-x"),
            1,
            "one drop must not remove the other subscriber",
        );
        assert_eq!(
            listener.active_channel_count(),
            1,
            "id stays while a subscriber remains"
        );

        drop(sub2);
        assert_eq!(
            listener.active_channel_count(),
            0,
            "the id entry is removed once its last subscriber leaves",
        );
    }
}
