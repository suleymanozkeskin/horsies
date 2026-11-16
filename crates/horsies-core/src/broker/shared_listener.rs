use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use sqlx::postgres::{PgListener, PgPool};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::broker::error::BrokerError;

/// Map from a NOTIFY payload ID to its live subscribers, each keyed by a unique
/// subscription id so a dropped [`Subscription`] can remove exactly its own
/// sender.
type SubscriberMap = Arc<Mutex<HashMap<String, HashMap<u64, mpsc::Sender<()>>>>>;

/// The shortest time between two health probes that notification activity
/// starts (Python `ACTIVITY_PROBE_MIN_INTERVAL_S`,
/// `horsies/core/brokers/listener.py:46`).
pub(crate) const ACTIVITY_PROBE_MIN_INTERVAL: Duration = Duration::from_secs(1);

/// The time with no notification after which the listener probes its
/// connection (Python's 60 s health wait, `horsies/core/brokers/listener.py:427`).
pub(crate) const IDLE_PROBE_INTERVAL: Duration = Duration::from_secs(60);

/// The longest a health probe may take before the connection counts as dead.
/// Python bounds the probe by the TCP user timeout of the listener connection
/// (`DEFAULT_TCP_USER_TIMEOUT_MS` = 30 s); the driver here sets no TCP user
/// timeout, so the probe carries the same bound itself.
pub(crate) const LISTENER_PROBE_TIMEOUT: Duration = Duration::from_secs(30);

/// First pause between two reconnect attempts; it doubles up to
/// [`RECONNECT_BACKOFF_MAX`] (Python, `horsies/core/brokers/listener.py:468-503`).
pub(crate) const RECONNECT_BACKOFF_INITIAL: Duration = Duration::from_millis(200);

/// Largest pause between two reconnect attempts.
pub(crate) const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// The statement of the health probe.
const PROBE_SQL: &str = "SELECT 1";

/// Whether a notification arrived since the last health probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActivitySinceProbe {
    /// No notification: the next probe is due [`IDLE_PROBE_INTERVAL`] after
    /// the last one.
    None,
    /// At least one notification: the next probe is due at once, but not
    /// within [`ACTIVITY_PROBE_MIN_INTERVAL`] of the last one.
    Seen,
}

/// When the next health probe is due (Python's rule,
/// `horsies/core/brokers/listener.py:412-456`).
fn next_probe_at(last_probe: Instant, activity: ActivitySinceProbe) -> Instant {
    match activity {
        ActivitySinceProbe::None => last_probe + IDLE_PROBE_INTERVAL,
        ActivitySinceProbe::Seen => last_probe + ACTIVITY_PROBE_MIN_INTERVAL,
    }
}

/// What the dispatcher saw in one step.
enum ListenerEvent {
    /// A notification on the channel.
    Notification(sqlx::postgres::PgNotification),
    /// The driver found the connection dead, connected again and listened
    /// again. Notifications sent in between are lost.
    Reconnected,
    /// The connection failed and the driver did not connect again.
    Failed(sqlx::Error),
    /// The next health probe is due.
    ProbeDue,
}

/// Why a health probe failed.
#[derive(Debug, thiserror::Error)]
enum ProbeFailure {
    #[error("probe failed: {0}")]
    Failed(#[source] sqlx::Error),
    #[error("probe got no answer within {} ms", LISTENER_PROBE_TIMEOUT.as_millis())]
    TimedOut,
}

/// A shared LISTEN/NOTIFY listener that multiplexes one `PgListener` connection
/// across multiple subscribers, each waiting for a specific payload ID.
///
/// Instead of each `get_result()` / `get_workflow_result()` call creating its
/// own `PgListener` (pinning one pool connection per caller), a single
/// `SharedNotifyListener` holds one connection and fans out notifications to
/// registered subscribers by matching the notification payload against
/// subscriber IDs.
///
/// Health: the dispatcher probes its connection with `SELECT 1` after
/// notification activity, at most once per [`ACTIVITY_PROBE_MIN_INTERVAL`],
/// and after [`IDLE_PROBE_INTERVAL`] with no activity. A probe that fails, or
/// that gets no answer within [`LISTENER_PROBE_TIMEOUT`], drops the connection
/// and connects again with a backoff from [`RECONNECT_BACKOFF_INITIAL`] to
/// [`RECONNECT_BACKOFF_MAX`].
///
/// Gap: after every reconnect, every subscriber gets one wake, because a
/// notification sent while the connection was down is lost. A woken waiter
/// reads the database again.
///
/// Stop: when the pool closes, the dispatcher stops and drops every sender,
/// so each subscription reads [`BrokerError::ListenerClosed`].
pub struct SharedNotifyListener {
    subscribers: SubscriberMap,
    next_sub_id: Arc<AtomicU64>,
    task_handle: JoinHandle<()>,
}

/// A subscription handle returned by [`SharedNotifyListener::subscribe`].
///
/// Receives a wake-up `()` whenever the shared listener receives a
/// notification whose payload matches the subscribed ID, and once after each
/// reconnect of the listener. Drop this to unsubscribe: the corresponding
/// sender (and the payload entry, once its last subscriber is gone) is
/// removed eagerly, without waiting for a matching NOTIFY that may never
/// arrive.
pub struct Subscription {
    rx: mpsc::Receiver<()>,
    subscribers: SubscriberMap,
    channel_id: String,
    sub_id: u64,
}

/// Lock the subscriber map. A poisoned lock is recovered: the map holds only
/// senders, and a panic while holding it cannot leave a sender half-written.
fn lock_subscribers(
    subscribers: &SubscriberMap,
) -> MutexGuard<'_, HashMap<String, HashMap<u64, mpsc::Sender<()>>>> {
    match subscribers.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::error!("subscriber map mutex poisoned, recovering");
            poisoned.into_inner()
        }
    }
}

/// Wake every subscriber of one payload. `try_send` never blocks; a full
/// channel coalesces, because a wake is already pending.
fn wake_payload(subscribers: &SubscriberMap, payload: &str) {
    let mut map = lock_subscribers(subscribers);
    if let Some(senders) = map.get_mut(payload) {
        // Drop any senders whose Subscription leaked without running Drop
        // (defensive; Drop is the primary path), then wake the rest.
        senders.retain(|_sub_id, tx| !tx.is_closed());
        for tx in senders.values() {
            let _ = tx.try_send(());
        }
        if senders.is_empty() {
            map.remove(payload);
        }
    }
}

/// Wake every subscriber of every payload: the gap signal after a reconnect.
fn wake_all(subscribers: &SubscriberMap) {
    let map = lock_subscribers(subscribers);
    for senders in map.values() {
        for tx in senders.values() {
            let _ = tx.try_send(());
        }
    }
}

/// Open a listener connection on `pool` and LISTEN on `channel`.
async fn connect_listener(pool: &PgPool, channel: &str) -> Result<PgListener, sqlx::Error> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen(channel).await?;
    Ok(listener)
}

/// Connect again with backoff until a connection listens on `channel`.
/// Returns `None` when the pool is closed, so the dispatcher stops.
async fn reconnect(pool: &PgPool, channel: &str) -> Option<PgListener> {
    let mut backoff = RECONNECT_BACKOFF_INITIAL;
    loop {
        if pool.is_closed() {
            return None;
        }
        match connect_listener(pool, channel).await {
            Ok(listener) => return Some(listener),
            Err(error) => {
                tracing::warn!(
                    channel,
                    error = %error,
                    retry_in_ms = backoff.as_millis() as u64,
                    "shared listener reconnect failed",
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_BACKOFF_MAX);
            }
        }
    }
}

/// Run `SELECT 1` on the listener connection within [`LISTENER_PROBE_TIMEOUT`].
/// Notifications that arrive during the probe stay in the listener's buffer.
async fn probe(listener: &mut PgListener) -> Result<(), ProbeFailure> {
    match tokio::time::timeout(
        LISTENER_PROBE_TIMEOUT,
        sqlx::query(PROBE_SQL).execute(&mut *listener),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(ProbeFailure::Failed(error)),
        Err(_elapsed) => Err(ProbeFailure::TimedOut),
    }
}

/// The dispatcher task: deliver notifications, probe the connection, connect
/// again after a failure, and wake every subscriber after each reconnect.
/// Ends only when the pool is closed; it then drops every sender.
async fn dispatch(
    mut listener: PgListener,
    pool: PgPool,
    channel: String,
    subscribers: SubscriberMap,
) {
    let mut last_probe = Instant::now();
    let mut activity = ActivitySinceProbe::None;
    loop {
        let wait = next_probe_at(last_probe, activity).saturating_duration_since(Instant::now());
        // `try_recv` is cancel-safe: it keeps a partly read message in the
        // connection buffer.
        let event = match tokio::time::timeout(wait, listener.try_recv()).await {
            Ok(Ok(Some(notification))) => ListenerEvent::Notification(notification),
            Ok(Ok(None)) => ListenerEvent::Reconnected,
            Ok(Err(error)) => ListenerEvent::Failed(error),
            Err(_elapsed) => ListenerEvent::ProbeDue,
        };
        let replaced = match event {
            ListenerEvent::Notification(notification) => {
                wake_payload(&subscribers, notification.payload());
                activity = ActivitySinceProbe::Seen;
                continue;
            }
            ListenerEvent::Reconnected => {
                tracing::warn!(
                    channel = %channel,
                    "shared listener connection was lost and is listening again; \
                     waking every subscriber",
                );
                wake_all(&subscribers);
                continue;
            }
            ListenerEvent::ProbeDue => {
                last_probe = Instant::now();
                activity = ActivitySinceProbe::None;
                match probe(&mut listener).await {
                    Ok(()) => continue,
                    Err(failure) => {
                        tracing::warn!(
                            channel = %channel,
                            error = %failure,
                            "shared listener health probe failed; connecting again",
                        );
                        reconnect(&pool, &channel).await
                    }
                }
            }
            ListenerEvent::Failed(error) => {
                tracing::error!(
                    channel = %channel,
                    error = %error,
                    "shared listener failed; connecting again",
                );
                reconnect(&pool, &channel).await
            }
        };
        match replaced {
            Some(fresh) => {
                listener = fresh;
                last_probe = Instant::now();
                activity = ActivitySinceProbe::None;
                wake_all(&subscribers);
            }
            None => {
                tracing::warn!(
                    channel = %channel,
                    "shared listener pool is closed; the listener stops",
                );
                lock_subscribers(&subscribers).clear();
                return;
            }
        }
    }
}

impl SharedNotifyListener {
    /// Create a new shared listener for the given NOTIFY channel.
    ///
    /// Spawns a background task that runs a single `PgListener` and fans
    /// out notifications to subscribers. The listener connection is taken
    /// from the pool (1 connection held for the lifetime of this struct).
    pub async fn new(pool: &PgPool, channel: &str) -> Result<Self, BrokerError> {
        let listener = connect_listener(pool, channel)
            .await
            .map_err(BrokerError::Database)?;

        let subscribers: SubscriberMap = Arc::new(Mutex::new(HashMap::new()));
        let task_handle = tokio::spawn(dispatch(
            listener,
            pool.clone(),
            channel.to_owned(),
            Arc::clone(&subscribers),
        ));

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
    ///
    /// When the dispatcher has stopped (its pool is closed), the subscription
    /// is closed at once: its first `recv` returns
    /// [`BrokerError::ListenerClosed`].
    pub fn subscribe(&self, id: &str) -> Subscription {
        // Use a small bounded channel - notifications are just wake-up signals.
        // If the buffer fills, the sender will drop overflow (see fanout loop).
        let (tx, rx) = mpsc::channel(8);
        let sub_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        match self.task_handle.is_finished() {
            true => drop(tx),
            false => {
                lock_subscribers(&self.subscribers)
                    .entry(id.to_owned())
                    .or_default()
                    .insert(sub_id, tx);
            }
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
        let mut map = lock_subscribers(&self.subscribers);
        if let Some(senders) = map.get_mut(&self.channel_id) {
            senders.remove(&self.sub_id);
            if senders.is_empty() {
                map.remove(&self.channel_id);
            }
        }
    }
}

/// Bounds the wait of a test for an aborted task to finish.
#[cfg(test)]
const STOP_WAIT_POLLS: usize = 1_000;

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
    /// The background task never ends, so the listener counts as running.
    pub(crate) fn new_for_test() -> Self {
        Self {
            subscribers: Arc::new(Mutex::new(HashMap::new())),
            next_sub_id: Arc::new(AtomicU64::new(0)),
            task_handle: tokio::spawn(std::future::pending::<()>()),
        }
    }

    /// Build a listener whose background task has already ended, as after
    /// its pool closed.
    pub(crate) async fn stopped_for_test() -> Self {
        let listener = Self::new_for_test();
        listener.task_handle.abort();
        for _ in 0..STOP_WAIT_POLLS {
            if listener.task_handle.is_finished() {
                return listener;
            }
            tokio::task::yield_now().await;
        }
        panic!("an aborted test listener task did not finish within {STOP_WAIT_POLLS} polls");
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

    /// Python's probe rule: after activity at most once per second, and after
    /// 60 s with no activity.
    #[test]
    fn the_next_probe_follows_activity_or_idleness() {
        let last_probe = Instant::now();

        assert_eq!(
            next_probe_at(last_probe, ActivitySinceProbe::Seen),
            last_probe + Duration::from_secs(1),
        );
        assert_eq!(
            next_probe_at(last_probe, ActivitySinceProbe::None),
            last_probe + Duration::from_secs(60),
        );
    }

    /// The gap signal reaches every subscriber of every payload.
    #[tokio::test]
    async fn wake_all_reaches_every_subscriber() {
        let listener = SharedNotifyListener::new_for_test();
        let mut first = listener.subscribe("task-a");
        let mut second = listener.subscribe("task-a");
        let mut third = listener.subscribe("task-b");

        wake_all(&listener.subscribers);

        for subscription in [&mut first, &mut second, &mut third] {
            assert_eq!(subscription.rx.try_recv(), Ok(()));
        }
    }

    #[tokio::test]
    async fn a_payload_wake_reaches_only_its_subscribers() {
        let listener = SharedNotifyListener::new_for_test();
        let mut matching = listener.subscribe("task-a");
        let mut other = listener.subscribe("task-b");

        wake_payload(&listener.subscribers, "task-a");

        assert_eq!(matching.rx.try_recv(), Ok(()));
        assert!(other.rx.try_recv().is_err());
    }

    /// A listener whose dispatcher has stopped hands out closed
    /// subscriptions, so a waiter learns at once that no wake will come.
    #[tokio::test]
    async fn a_subscription_on_a_stopped_listener_reads_closed() {
        let listener = SharedNotifyListener::stopped_for_test().await;
        let mut subscription = listener.subscribe("task-a");

        assert!(matches!(
            subscription.recv().await,
            Err(BrokerError::ListenerClosed)
        ));
        assert_eq!(listener.active_channel_count(), 0);
    }

    /// Bounds each wait of the backend-kill test.
    const GAP_WAIT: Duration = Duration::from_secs(10);

    /// A backend kill ends the listener connection. The listener connects
    /// again, listens again, and wakes every subscriber, because a
    /// notification sent in between is lost.
    #[tokio::test]
    async fn a_killed_connection_wakes_every_subscriber_and_listens_again() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let channel = format!("shared_listener_gap_{}", uuid::Uuid::new_v4().simple());
        let listener = SharedNotifyListener::new(&pool, &channel)
            .await
            .expect("the listener starts");
        let mut first = listener.subscribe("task-a");
        let mut second = listener.subscribe("task-b");

        let terminated: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM (
                 SELECT pg_terminate_backend(pid) FROM pg_stat_activity
                 WHERE pid <> pg_backend_pid() AND query LIKE '%' || $1 || '%'
             ) AS killed",
        )
        .bind(&channel)
        .fetch_one(&pool)
        .await
        .expect("terminate the listener backend");
        assert_eq!(terminated, 1, "exactly the listener backend is terminated");

        for subscription in [&mut first, &mut second] {
            tokio::time::timeout(GAP_WAIT, subscription.recv())
                .await
                .expect("the gap wake arrives within the bound")
                .expect("the listener keeps running");
        }

        sqlx::query("SELECT pg_notify($1, 'task-a')")
            .bind(&channel)
            .execute(&pool)
            .await
            .expect("notify the channel");
        tokio::time::timeout(GAP_WAIT, first.recv())
            .await
            .expect("the new connection listens on the channel")
            .expect("the listener keeps running");
    }
}
