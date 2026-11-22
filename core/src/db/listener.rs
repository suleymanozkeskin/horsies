//! The listener: one dedicated connection on the session URL that carries
//! every LISTEN of the process.
//!
//! - The connection opens at the first subscription and has the keepalives
//!   and the TCP user timeout of every core connection.
//! - A channel is LISTENed once, as a quoted identifier, and UNLISTENed when
//!   its last subscriber leaves.
//! - Subscriptions are bounded and never block delivery:
//!   [`WakeSubscription`] holds at most one pending signal;
//!   [`PayloadSubscription`] holds a bounded queue that drops its oldest entry
//!   when full and then reads [`PayloadEvent::Overflowed`] once.
//! - After a failure the listener reconnects with a backoff from 200 ms,
//!   doubling to 5 s, while any subscriber exists. After the LISTEN of every
//!   channel, every subscriber reads a gap: notifications may have been lost,
//!   so the subscriber reads its state again.
//! - Health: a probe (`SELECT 1`) on the same connection after notification
//!   activity at most once per second, and after 60 s with no activity.
//! - [`Listener::close`] stops every subscription; each reads `Stopped`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::sync::{oneshot, Notify};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_postgres::{AsyncMessage, Client};

use super::connect::{open_connection, ConnectPlan, DriverConnection};
use super::failure::{ConnectError, ConnectFailure, DriverErrorText, Transience};
use crate::process_runtime::spawn_on_process_runtime;

/// PostgreSQL's identifier limit (NAMEDATALEN - 1).
pub const MAX_CHANNEL_NAME_BYTES: usize = 63;

/// The most subscriptions one listener holds.
pub const MAX_SUBSCRIPTIONS: usize = 4096;

/// The payload queue size of Python's listener.
pub const DEFAULT_PAYLOAD_QUEUE_CAPACITY: usize = 4096;

/// The largest payload queue a subscription may ask for.
pub const MAX_PAYLOAD_QUEUE_CAPACITY: usize = 65_536;

/// The first wait before a reconnect.
pub const RECONNECT_BACKOFF_FIRST: Duration = Duration::from_millis(200);

/// The longest wait between reconnect attempts.
pub const RECONNECT_BACKOFF_LIMIT: Duration = Duration::from_secs(5);

/// The shortest time between two probes that notification activity starts.
pub const PROBE_AFTER_ACTIVITY: Duration = Duration::from_secs(1);

/// The probe interval when no notification arrives.
pub const PROBE_AFTER_SILENCE: Duration = Duration::from_secs(60);

/// The bound of one LISTEN, UNLISTEN or probe.
pub const LISTENER_STATEMENT_BOUND: Duration = Duration::from_secs(5);

/// The longest [`Listener::close`] waits for the listener task.
pub const LISTENER_CLOSE_BOUND: Duration = Duration::from_secs(5);

const PROBE_SQL: &str = "SELECT 1";

const LOG_TARGET: &str = "horsies.listener";

/// A channel name: 1 to 63 bytes, no NUL.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ChannelName(String);

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ChannelNameError {
    #[error("a channel name cannot be empty")]
    Empty,
    #[error("the channel name is {length} bytes; the limit is {limit}")]
    TooLong { length: usize, limit: usize },
    #[error("a channel name cannot contain NUL")]
    ContainsNul,
}

impl ChannelName {
    pub fn parse(text: &str) -> Result<ChannelName, ChannelNameError> {
        match (text.len(), text.contains('\0')) {
            (0, _) => Err(ChannelNameError::Empty),
            (length, _) if length > MAX_CHANNEL_NAME_BYTES => Err(ChannelNameError::TooLong {
                length,
                limit: MAX_CHANNEL_NAME_BYTES,
            }),
            (_, true) => Err(ChannelNameError::ContainsNul),
            (_, false) => Ok(ChannelName(text.to_owned())),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The name as a quoted SQL identifier: case is kept, `"` is doubled.
    fn quoted(&self) -> String {
        format!("\"{}\"", self.0.replace('"', "\"\""))
    }
}

/// Which notifications wake a [`WakeSubscription`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PayloadFilter {
    AnyPayload,
    Payload(String),
}

/// The queue size of a [`PayloadSubscription`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueCapacity(NonZeroUsize);

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum QueueCapacityError {
    #[error("a payload queue needs room for at least one payload")]
    Zero,
    #[error("a payload queue of {capacity} is above the limit of {limit}")]
    AboveLimit { capacity: usize, limit: usize },
}

impl QueueCapacity {
    pub fn new(capacity: usize) -> Result<QueueCapacity, QueueCapacityError> {
        match NonZeroUsize::new(capacity) {
            None => Err(QueueCapacityError::Zero),
            Some(_) if capacity > MAX_PAYLOAD_QUEUE_CAPACITY => {
                Err(QueueCapacityError::AboveLimit {
                    capacity,
                    limit: MAX_PAYLOAD_QUEUE_CAPACITY,
                })
            }
            Some(capacity) => Ok(QueueCapacity(capacity)),
        }
    }

    pub fn get(self) -> usize {
        self.0.get()
    }
}

/// What a [`WakeSubscription`] reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WakeEvent {
    /// A matching notification arrived.
    Notified,
    /// The listener reconnected: notifications may have been lost.
    Gap,
    /// The listener stopped; no further event comes.
    Stopped,
}

/// What a [`PayloadSubscription`] reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PayloadEvent {
    Payload(String),
    /// The queue was full and dropped its oldest payloads; read everything
    /// again.
    Overflowed,
    /// The listener reconnected: notifications may have been lost.
    Gap,
    /// The listener stopped; no further event comes.
    Stopped,
}

/// Why the listener could not start for a subscription.
#[derive(Debug, thiserror::Error)]
pub enum ListenerStartFailure {
    #[error("{0}")]
    Connect(ConnectError),
    #[error("LISTEN failed: {}", DriverErrorText(.0))]
    Listen(tokio_postgres::Error),
    #[error("LISTEN did not end within {bound:?}")]
    ListenTimedOut { bound: Duration },
}

impl ListenerStartFailure {
    pub fn transience(&self) -> Transience {
        match self {
            ListenerStartFailure::Connect(error) => error.transience(),
            ListenerStartFailure::Listen(_) | ListenerStartFailure::ListenTimedOut { .. } => {
                Transience::Transient
            }
        }
    }
}

/// A subscription was refused. Nothing is registered.
#[derive(Clone, Debug, thiserror::Error)]
pub enum SubscribeError {
    #[error("the listener is stopped")]
    ListenerStopped,
    #[error("the listener holds {limit} subscriptions, its limit")]
    SubscriptionLimit { limit: usize },
    #[error("the listener could not start: {cause}")]
    StartFailed { cause: Arc<ListenerStartFailure> },
    #[error("the listener did not confirm the subscription within {bound:?}")]
    TimedOut { bound: Duration },
}

impl SubscribeError {
    pub fn transience(&self) -> Transience {
        match self {
            SubscribeError::ListenerStopped => Transience::Permanent,
            SubscribeError::SubscriptionLimit { .. } | SubscribeError::TimedOut { .. } => {
                Transience::Transient
            }
            SubscribeError::StartFailed { cause } => cause.transience(),
        }
    }
}

/// How [`Listener::close`] ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ListenerClose {
    /// The listener task ended within the bound.
    Closed,
    /// An earlier close already ended the task.
    AlreadyClosed,
    /// The task did not end within the bound and was aborted.
    Aborted { bound: Duration },
}

/// Locks a mutex of the listener. A poisoned lock means a panic while it was
/// held; that panic already crashed its thread loudly. Every critical section
/// leaves the guarded value whole, so the value is used as it is.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WakeSignal {
    Notified,
    Gap,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WakeState {
    Empty,
    Pending(WakeSignal),
    Stopped,
}

/// At most one pending signal; a gap replaces a pending notification.
struct WakeSlot {
    state: Mutex<WakeState>,
    notify: Notify,
}

impl WakeSlot {
    fn new() -> WakeSlot {
        WakeSlot {
            state: Mutex::new(WakeState::Empty),
            notify: Notify::new(),
        }
    }

    fn push(&self, signal: WakeSignal) {
        {
            let mut state = lock(&self.state);
            *state = match (*state, signal) {
                (WakeState::Stopped, _) => WakeState::Stopped,
                (WakeState::Pending(WakeSignal::Gap), _) | (_, WakeSignal::Gap) => {
                    WakeState::Pending(WakeSignal::Gap)
                }
                (
                    WakeState::Empty | WakeState::Pending(WakeSignal::Notified),
                    WakeSignal::Notified,
                ) => WakeState::Pending(WakeSignal::Notified),
            };
        }
        self.notify.notify_one();
    }

    fn stop(&self) {
        *lock(&self.state) = WakeState::Stopped;
        self.notify.notify_one();
    }

    /// Waits for the next event. Each loop turn either returns or waits for
    /// one wake of `notify`.
    async fn recv(&self) -> WakeEvent {
        loop {
            let notified = self.notify.notified();
            let current = {
                let mut state = lock(&self.state);
                let current = *state;
                if let WakeState::Pending(_) = current {
                    *state = WakeState::Empty;
                }
                current
            };
            match current {
                WakeState::Pending(WakeSignal::Notified) => return WakeEvent::Notified,
                WakeState::Pending(WakeSignal::Gap) => return WakeEvent::Gap,
                WakeState::Stopped => return WakeEvent::Stopped,
                WakeState::Empty => notified.await,
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mark {
    Clear,
    Pending,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueueLifecycle {
    Open,
    Stopped,
}

struct PayloadQueueState {
    entries: VecDeque<String>,
    overflowed: Mark,
    gap: Mark,
    lifecycle: QueueLifecycle,
}

/// A bounded payload queue that drops its oldest entry when full.
struct PayloadQueue {
    state: Mutex<PayloadQueueState>,
    notify: Notify,
    capacity: usize,
}

impl PayloadQueue {
    fn new(capacity: QueueCapacity) -> PayloadQueue {
        PayloadQueue {
            state: Mutex::new(PayloadQueueState {
                entries: VecDeque::with_capacity(capacity.get()),
                overflowed: Mark::Clear,
                gap: Mark::Clear,
                lifecycle: QueueLifecycle::Open,
            }),
            notify: Notify::new(),
            capacity: capacity.get(),
        }
    }

    fn push_payload(&self, payload: &str) {
        {
            let mut state = lock(&self.state);
            match state.lifecycle {
                QueueLifecycle::Stopped => return,
                QueueLifecycle::Open => {}
            }
            if state.entries.len() >= self.capacity {
                let _dropped = state.entries.pop_front();
                state.overflowed = Mark::Pending;
            }
            state.entries.push_back(payload.to_owned());
        }
        self.notify.notify_one();
    }

    fn push_gap(&self) {
        lock(&self.state).gap = Mark::Pending;
        self.notify.notify_one();
    }

    fn stop(&self) {
        lock(&self.state).lifecycle = QueueLifecycle::Stopped;
        self.notify.notify_one();
    }

    /// Waits for the next event: an overflow first, then a gap, then the
    /// oldest payload, then `Stopped` once the queue is empty and stopped.
    async fn recv(&self) -> PayloadEvent {
        loop {
            let notified = self.notify.notified();
            let event = take_event(&mut lock(&self.state));
            match event {
                Some(event) => return event,
                None => notified.await,
            }
        }
    }
}

/// The next event of a payload queue, or `None` when nothing is ready yet.
fn take_event(state: &mut PayloadQueueState) -> Option<PayloadEvent> {
    if let Mark::Pending = state.overflowed {
        state.overflowed = Mark::Clear;
        return Some(PayloadEvent::Overflowed);
    }
    if let Mark::Pending = state.gap {
        state.gap = Mark::Clear;
        return Some(PayloadEvent::Gap);
    }
    if let Some(payload) = state.entries.pop_front() {
        return Some(PayloadEvent::Payload(payload));
    }
    match state.lifecycle {
        QueueLifecycle::Stopped => Some(PayloadEvent::Stopped),
        QueueLifecycle::Open => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct SubscriberId(u64);

enum Sink {
    Wake {
        filter: PayloadFilter,
        slot: Arc<WakeSlot>,
    },
    Payloads(Arc<PayloadQueue>),
}

impl Sink {
    fn push_gap(&self) {
        match self {
            Sink::Wake { slot, .. } => slot.push(WakeSignal::Gap),
            Sink::Payloads(queue) => queue.push_gap(),
        }
    }

    fn stop(&self) {
        match self {
            Sink::Wake { slot, .. } => slot.stop(),
            Sink::Payloads(queue) => queue.stop(),
        }
    }
}

/// Whether the subscriber's caller still waits for the LISTEN.
enum Confirmation {
    Waiting(oneshot::Sender<Result<(), SubscribeError>>),
    Confirmed,
}

struct Subscriber {
    channel: ChannelName,
    sink: Sink,
    confirmation: Confirmation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lifecycle {
    Running,
    Closing,
}

struct Registry {
    lifecycle: Lifecycle,
    next_id: u64,
    subscribers: HashMap<SubscriberId, Subscriber>,
}

/// State shared by the handle, the subscriptions, the listener task and the
/// connection driver.
struct Shared {
    registry: Mutex<Registry>,
    task_wake: Notify,
    notifications: AtomicU64,
}

struct Snapshot {
    lifecycle: Lifecycle,
    desired: HashSet<ChannelName>,
}

impl Shared {
    fn snapshot(&self) -> Snapshot {
        let registry = lock(&self.registry);
        Snapshot {
            lifecycle: registry.lifecycle,
            desired: registry
                .subscribers
                .values()
                .map(|subscriber| subscriber.channel.clone())
                .collect(),
        }
    }

    /// Delivers one notification to every subscriber of its channel. Never
    /// waits for a consumer.
    fn dispatch(&self, channel: &str, payload: &str) {
        let _previous = self.notifications.fetch_add(1, Ordering::Relaxed);
        let registry = lock(&self.registry);
        for subscriber in registry.subscribers.values() {
            if subscriber.channel.as_str() != channel {
                continue;
            }
            match &subscriber.sink {
                Sink::Wake {
                    filter: PayloadFilter::AnyPayload,
                    slot,
                } => slot.push(WakeSignal::Notified),
                Sink::Wake {
                    filter: PayloadFilter::Payload(expected),
                    slot,
                } if expected == payload => slot.push(WakeSignal::Notified),
                Sink::Wake { .. } => {}
                Sink::Payloads(queue) => queue.push_payload(payload),
            }
        }
    }

    fn notification_count(&self) -> u64 {
        self.notifications.load(Ordering::Relaxed)
    }

    fn confirm_waiting(&self) {
        let mut registry = lock(&self.registry);
        for subscriber in registry.subscribers.values_mut() {
            match std::mem::replace(&mut subscriber.confirmation, Confirmation::Confirmed) {
                Confirmation::Waiting(sender) => {
                    let _caller_gone = sender.send(Ok(()));
                }
                Confirmation::Confirmed => {}
            }
        }
    }

    /// Removes every subscriber whose caller still waits, and answers it.
    fn fail_waiting(&self, error: &SubscribeError) {
        let mut registry = lock(&self.registry);
        let waiting: Vec<SubscriberId> = registry
            .subscribers
            .iter()
            .filter(|(_, subscriber)| matches!(subscriber.confirmation, Confirmation::Waiting(_)))
            .map(|(id, _)| *id)
            .collect();
        for id in waiting {
            if let Some(subscriber) = registry.subscribers.remove(&id) {
                match subscriber.confirmation {
                    Confirmation::Waiting(sender) => {
                        let _caller_gone = sender.send(Err(error.clone()));
                    }
                    Confirmation::Confirmed => {}
                }
            }
        }
    }

    fn gap_all(&self) {
        let registry = lock(&self.registry);
        for subscriber in registry.subscribers.values() {
            subscriber.sink.push_gap();
        }
    }

    /// Stops every subscription and empties the registry.
    fn stop_all(&self) {
        let mut registry = lock(&self.registry);
        registry.lifecycle = Lifecycle::Closing;
        for (_, subscriber) in registry.subscribers.drain() {
            subscriber.sink.stop();
            match subscriber.confirmation {
                Confirmation::Waiting(sender) => {
                    let _caller_gone = sender.send(Err(SubscribeError::ListenerStopped));
                }
                Confirmation::Confirmed => {}
            }
        }
    }

    fn remove(&self, id: SubscriberId) {
        let _removed = lock(&self.registry).subscribers.remove(&id);
        self.task_wake.notify_one();
    }

    fn begin_close(&self) {
        lock(&self.registry).lifecycle = Lifecycle::Closing;
        self.task_wake.notify_one();
    }
}

enum TaskSlot {
    Running(JoinHandle<()>),
    Joined,
}

struct ListenerOwner {
    shared: Arc<Shared>,
    task: Mutex<TaskSlot>,
    subscribe_bound: Duration,
}

/// Dropping the last handle stops the listener task, as a close does.
impl Drop for ListenerOwner {
    fn drop(&mut self) {
        self.shared.begin_close();
    }
}

/// The handle of the process's listener. Cheap to clone.
#[derive(Clone)]
pub struct Listener {
    owner: Arc<ListenerOwner>,
}

impl Listener {
    /// Starts the listener task on the runtime of the process
    /// ([`crate::process_runtime`]), so its connection serves every runtime
    /// of the process. The connection opens at the first subscription.
    ///
    /// When the process runtime cannot be used, the task starts on the
    /// caller's runtime (which must exist) and fails every subscription with
    /// [`SubscribeError::StartFailed`] and the refusal as its cause.
    pub fn start(plan: Arc<ConnectPlan>) -> Listener {
        let shared = Arc::new(Shared {
            registry: Mutex::new(Registry {
                lifecycle: Lifecycle::Running,
                next_id: 0,
                subscribers: HashMap::new(),
            }),
            task_wake: Notify::new(),
            notifications: AtomicU64::new(0),
        });
        let subscribe_bound = RECONNECT_BACKOFF_LIMIT
            .saturating_add(plan.attempt_bound())
            .saturating_add(LISTENER_STATEMENT_BOUND.saturating_mul(2));
        let masked_url = plan.masked_url().clone();
        let task = match spawn_on_process_runtime(run_listener(Arc::clone(&shared), plan)) {
            Ok(task) => task,
            Err(refusal) => {
                tracing::error!(
                    target: LOG_TARGET,
                    %refusal,
                    "the listener cannot run: the process runtime cannot be used",
                );
                let cause = Arc::new(ListenerStartFailure::Connect(ConnectError {
                    masked_url,
                    cause: ConnectFailure::RuntimeUnavailable(refusal),
                }));
                tokio::spawn(refuse_subscriptions(Arc::clone(&shared), cause))
            }
        };
        Listener {
            owner: Arc::new(ListenerOwner {
                shared,
                task: Mutex::new(TaskSlot::Running(task)),
                subscribe_bound,
            }),
        }
    }

    /// Subscribes to wakes of a channel. Returns once the channel is LISTENed,
    /// or once a reconnect is under way (a gap follows it).
    pub async fn subscribe_wake(
        &self,
        channel: ChannelName,
        filter: PayloadFilter,
    ) -> Result<WakeSubscription, SubscribeError> {
        let slot = Arc::new(WakeSlot::new());
        let sink = Sink::Wake {
            filter,
            slot: Arc::clone(&slot),
        };
        let registration = self.subscribe(channel, sink).await?;
        Ok(WakeSubscription {
            _registration: registration,
            slot,
        })
    }

    /// Subscribes to the payloads of a channel.
    pub async fn subscribe_payloads(
        &self,
        channel: ChannelName,
        capacity: QueueCapacity,
    ) -> Result<PayloadSubscription, SubscribeError> {
        let queue = Arc::new(PayloadQueue::new(capacity));
        let registration = self
            .subscribe(channel, Sink::Payloads(Arc::clone(&queue)))
            .await?;
        Ok(PayloadSubscription {
            _registration: registration,
            queue,
        })
    }

    /// Whether the listener task still runs: it was not closed, and its
    /// runtime has not dropped it.
    pub fn is_running(&self) -> bool {
        match &*lock(&self.owner.task) {
            TaskSlot::Running(task) => !task.is_finished(),
            TaskSlot::Joined => false,
        }
    }

    /// Stops the listener: every subscription reads `Stopped`, the connection
    /// closes, and later subscriptions are refused. Waits at most
    /// [`LISTENER_CLOSE_BOUND`] for the task, then aborts it.
    pub async fn close(&self) -> ListenerClose {
        self.owner.shared.begin_close();
        let slot = std::mem::replace(&mut *lock(&self.owner.task), TaskSlot::Joined);
        let outcome = match slot {
            TaskSlot::Joined => ListenerClose::AlreadyClosed,
            TaskSlot::Running(mut task) => {
                match tokio::time::timeout(LISTENER_CLOSE_BOUND, &mut task).await {
                    Ok(Ok(())) => ListenerClose::Closed,
                    Ok(Err(join_error)) => match join_error.try_into_panic() {
                        Ok(panic) => std::panic::resume_unwind(panic),
                        Err(_cancelled) => ListenerClose::Closed,
                    },
                    Err(_elapsed) => {
                        task.abort();
                        ListenerClose::Aborted {
                            bound: LISTENER_CLOSE_BOUND,
                        }
                    }
                }
            }
        };
        self.owner.shared.stop_all();
        outcome
    }

    /// Puts a subscriber into the registry and waits until the listener
    /// confirms its LISTEN. The returned [`Registration`] owns the removal of
    /// the subscriber from the moment it enters the registry: a caller that
    /// is dropped while it waits, a refusal and a timeout all remove it.
    async fn subscribe(
        &self,
        channel: ChannelName,
        sink: Sink,
    ) -> Result<Registration, SubscribeError> {
        let shared = &self.owner.shared;
        let (sender, receiver) = oneshot::channel();
        let registration = {
            let mut registry = lock(&shared.registry);
            match registry.lifecycle {
                Lifecycle::Closing => return Err(SubscribeError::ListenerStopped),
                Lifecycle::Running => {}
            }
            if registry.subscribers.len() >= MAX_SUBSCRIPTIONS {
                return Err(SubscribeError::SubscriptionLimit {
                    limit: MAX_SUBSCRIPTIONS,
                });
            }
            let id = SubscriberId(registry.next_id);
            registry.next_id = registry.next_id.wrapping_add(1);
            let _previous = registry.subscribers.insert(
                id,
                Subscriber {
                    channel,
                    sink,
                    confirmation: Confirmation::Waiting(sender),
                },
            );
            Registration {
                id,
                shared: Arc::clone(shared),
            }
        };
        shared.task_wake.notify_one();
        match tokio::time::timeout(self.owner.subscribe_bound, receiver).await {
            Ok(Ok(Ok(()))) => Ok(registration),
            Ok(Ok(Err(error))) => Err(error),
            Ok(Err(_task_gone)) => Err(SubscribeError::ListenerStopped),
            Err(_elapsed) => Err(SubscribeError::TimedOut {
                bound: self.owner.subscribe_bound,
            }),
        }
    }
}

/// A subscriber in the registry. Dropping it removes the subscriber, so the
/// listener UNLISTENs a channel that no subscriber wants any more. A removal
/// of a subscriber that a failure already removed changes nothing.
struct Registration {
    id: SubscriberId,
    shared: Arc<Shared>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.shared.remove(self.id);
    }
}

/// A wake subscription. Dropping it unsubscribes.
pub struct WakeSubscription {
    /// Removes the subscriber when the subscription drops.
    _registration: Registration,
    slot: Arc<WakeSlot>,
}

impl WakeSubscription {
    /// Waits for the next event.
    pub async fn recv(&self) -> WakeEvent {
        self.slot.recv().await
    }
}

/// A payload subscription. Dropping it unsubscribes.
pub struct PayloadSubscription {
    /// Removes the subscriber when the subscription drops.
    _registration: Registration,
    queue: Arc<PayloadQueue>,
}

impl PayloadSubscription {
    /// Waits for the next event.
    pub async fn recv(&self) -> PayloadEvent {
        self.queue.recv().await
    }
}

/// Whether the listener has had a working connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum History {
    /// No connection worked yet: a failure fails the waiting subscribers.
    NeverConnected,
    /// A connection worked before: waiting subscribers are confirmed and read
    /// a gap after the reconnect.
    ConnectedBefore,
}

struct DownState {
    retry_at: Instant,
    delay: Duration,
    history: History,
}

impl DownState {
    fn after_failure(self) -> DownState {
        DownState {
            retry_at: Instant::now() + self.delay,
            delay: self.delay.saturating_mul(2).min(RECONNECT_BACKOFF_LIMIT),
            history: self.history,
        }
    }

    fn after_loss() -> DownState {
        DownState {
            retry_at: Instant::now() + RECONNECT_BACKOFF_FIRST,
            delay: RECONNECT_BACKOFF_FIRST
                .saturating_mul(2)
                .min(RECONNECT_BACKOFF_LIMIT),
            history: History::ConnectedBefore,
        }
    }
}

/// How the driver of a listener connection ended.
enum LinkEnd {
    Closed,
    Failed(tokio_postgres::Error),
}

struct UpState {
    client: Client,
    driver: JoinHandle<()>,
    ended: oneshot::Receiver<LinkEnd>,
    listened: HashSet<ChannelName>,
    last_probe: Instant,
    notifications_at_probe: u64,
}

enum Link {
    Down(DownState),
    Up(Box<UpState>),
}

/// The listener task. Runs until the listener closes; each turn handles one
/// change: a subscription change, a connect attempt, a lost connection or a
/// probe.
/// The listener task when the process runtime cannot be used: it fails
/// each waiting subscription with `cause`, until the close.
async fn refuse_subscriptions(shared: Arc<Shared>, cause: Arc<ListenerStartFailure>) {
    loop {
        match shared.snapshot().lifecycle {
            Lifecycle::Closing => {
                shared.stop_all();
                return;
            }
            Lifecycle::Running => {}
        }
        shared.fail_waiting(&SubscribeError::StartFailed {
            cause: Arc::clone(&cause),
        });
        // A subscription and the close each wake the task; a wake before
        // this wait is kept as a permit.
        shared.task_wake.notified().await;
    }
}

async fn run_listener(shared: Arc<Shared>, plan: Arc<ConnectPlan>) {
    let mut link = Link::Down(DownState {
        retry_at: Instant::now(),
        delay: RECONNECT_BACKOFF_FIRST,
        history: History::NeverConnected,
    });
    loop {
        let snapshot = shared.snapshot();
        match snapshot.lifecycle {
            Lifecycle::Closing => {
                if let Link::Up(up) = link {
                    up.driver.abort();
                }
                shared.stop_all();
                return;
            }
            Lifecycle::Running => {}
        }
        link = match link {
            Link::Down(down) => step_down(&shared, &plan, down, &snapshot).await,
            Link::Up(up) => step_up(&shared, up, &snapshot).await,
        };
    }
}

async fn step_down(
    shared: &Arc<Shared>,
    plan: &ConnectPlan,
    down: DownState,
    snapshot: &Snapshot,
) -> Link {
    if snapshot.desired.is_empty() {
        shared.task_wake.notified().await;
        return Link::Down(DownState {
            retry_at: down.retry_at.min(Instant::now()),
            ..down
        });
    }
    if Instant::now() < down.retry_at {
        if let History::ConnectedBefore = down.history {
            shared.confirm_waiting();
        }
        tokio::select! {
            () = tokio::time::sleep_until(down.retry_at) => {}
            () = shared.task_wake.notified() => {}
        }
        return Link::Down(down);
    }
    let started = match open_connection(plan).await {
        Ok((client, connection)) => bring_up(shared, client, connection, &snapshot.desired).await,
        Err(error) => Err(ListenerStartFailure::Connect(error)),
    };
    match started {
        Ok(up) => {
            shared.confirm_waiting();
            match down.history {
                History::ConnectedBefore => {
                    tracing::info!(target: LOG_TARGET, "the listener reconnected");
                    shared.gap_all();
                }
                History::NeverConnected => {}
            }
            Link::Up(Box::new(up))
        }
        Err(failure) => {
            tracing::warn!(target: LOG_TARGET, %failure, "the listener could not connect");
            match down.history {
                History::NeverConnected => shared.fail_waiting(&SubscribeError::StartFailed {
                    cause: Arc::new(failure),
                }),
                History::ConnectedBefore => shared.confirm_waiting(),
            }
            Link::Down(down.after_failure())
        }
    }
}

async fn bring_up(
    shared: &Arc<Shared>,
    client: Client,
    connection: DriverConnection,
    desired: &HashSet<ChannelName>,
) -> Result<UpState, ListenerStartFailure> {
    let (ended_sender, ended) = oneshot::channel();
    let driver = tokio::spawn(drive_connection(
        connection,
        Arc::clone(shared),
        ended_sender,
    ));
    let mut up = UpState {
        client,
        driver,
        ended,
        listened: HashSet::new(),
        last_probe: Instant::now(),
        notifications_at_probe: shared.notification_count(),
    };
    match reconcile(&mut up, desired).await {
        Ok(()) => Ok(up),
        Err(failure) => {
            up.driver.abort();
            Err(failure)
        }
    }
}

async fn step_up(shared: &Arc<Shared>, mut up: Box<UpState>, snapshot: &Snapshot) -> Link {
    if let Err(failure) = reconcile(&mut up, &snapshot.desired).await {
        tracing::warn!(target: LOG_TARGET, %failure, "the listener lost its connection");
        up.driver.abort();
        return Link::Down(DownState::after_loss());
    }
    shared.confirm_waiting();
    tokio::select! {
        end = &mut up.ended => {
            match end {
                Ok(LinkEnd::Failed(error)) => {
                    tracing::warn!(
                        target: LOG_TARGET,
                        %error,
                        "the listener connection failed: {error}",
                    );
                }
                Ok(LinkEnd::Closed) | Err(_) => {
                    tracing::warn!(target: LOG_TARGET, "the listener connection closed");
                }
            }
            up.driver.abort();
            Link::Down(DownState::after_loss())
        }
        () = shared.task_wake.notified() => Link::Up(up),
        () = tokio::time::sleep(PROBE_AFTER_ACTIVITY) => probe_if_due(shared, up).await,
    }
}

/// LISTENs every wanted channel that is not LISTENed, and UNLISTENs every
/// LISTENed channel that no subscriber wants. At most one statement per
/// subscription.
async fn reconcile(
    up: &mut UpState,
    desired: &HashSet<ChannelName>,
) -> Result<(), ListenerStartFailure> {
    let to_listen: Vec<ChannelName> = desired.difference(&up.listened).cloned().collect();
    let to_unlisten: Vec<ChannelName> = up.listened.difference(desired).cloned().collect();
    for channel in to_listen {
        run_bounded(&up.client, &format!("LISTEN {}", channel.quoted())).await?;
        let _new = up.listened.insert(channel);
    }
    for channel in to_unlisten {
        run_bounded(&up.client, &format!("UNLISTEN {}", channel.quoted())).await?;
        let _present = up.listened.remove(&channel);
    }
    Ok(())
}

async fn run_bounded(client: &Client, sql: &str) -> Result<(), ListenerStartFailure> {
    match tokio::time::timeout(LISTENER_STATEMENT_BOUND, client.batch_execute(sql)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(ListenerStartFailure::Listen(error)),
        Err(_elapsed) => Err(ListenerStartFailure::ListenTimedOut {
            bound: LISTENER_STATEMENT_BOUND,
        }),
    }
}

/// Whether a probe runs at this tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProbeDue {
    Now,
    Later,
}

/// Whether a notification arrived since the last probe.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ActivitySinceProbe {
    Seen,
    NotSeen,
}

/// The probe rule: after notification activity, a probe once
/// [`PROBE_AFTER_ACTIVITY`] has passed since the last probe; with no
/// activity, a probe once [`PROBE_AFTER_SILENCE`] has passed. Pure.
fn probe_due(activity: ActivitySinceProbe, since_probe: Duration) -> ProbeDue {
    match (
        activity,
        since_probe >= PROBE_AFTER_ACTIVITY,
        since_probe >= PROBE_AFTER_SILENCE,
    ) {
        (ActivitySinceProbe::Seen, true, _) | (_, _, true) => ProbeDue::Now,
        (ActivitySinceProbe::NotSeen, _, false) | (ActivitySinceProbe::Seen, false, false) => {
            ProbeDue::Later
        }
    }
}

async fn probe_if_due(shared: &Arc<Shared>, mut up: Box<UpState>) -> Link {
    let notifications = shared.notification_count();
    let activity = match notifications == up.notifications_at_probe {
        true => ActivitySinceProbe::NotSeen,
        false => ActivitySinceProbe::Seen,
    };
    let due = probe_due(activity, up.last_probe.elapsed());
    match due {
        ProbeDue::Later => Link::Up(up),
        ProbeDue::Now => match run_bounded(&up.client, PROBE_SQL).await {
            Ok(()) => {
                up.last_probe = Instant::now();
                up.notifications_at_probe = notifications;
                Link::Up(up)
            }
            Err(failure) => {
                tracing::warn!(target: LOG_TARGET, %failure, "the listener probe failed");
                up.driver.abort();
                Link::Down(DownState::after_loss())
            }
        },
    }
}

/// Drives the listener connection and delivers its notifications. Ends when
/// the connection ends, and reports how.
async fn drive_connection(
    mut connection: DriverConnection,
    shared: Arc<Shared>,
    ended: oneshot::Sender<LinkEnd>,
) {
    let end = loop {
        match std::future::poll_fn(|context| connection.poll_message(context)).await {
            Some(Ok(AsyncMessage::Notification(notification))) => {
                shared.dispatch(notification.channel(), notification.payload());
            }
            Some(Ok(_notice_or_other)) => {}
            Some(Err(error)) => break LinkEnd::Failed(error),
            None => break LinkEnd::Closed,
        }
    };
    let _listener_gone = ended.send(end);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_names_are_bounded() {
        assert!(ChannelName::parse("task_queue_default").is_ok());
        assert_eq!(ChannelName::parse(""), Err(ChannelNameError::Empty));
        assert_eq!(
            ChannelName::parse(&"c".repeat(MAX_CHANNEL_NAME_BYTES + 1)),
            Err(ChannelNameError::TooLong {
                length: MAX_CHANNEL_NAME_BYTES + 1,
                limit: MAX_CHANNEL_NAME_BYTES
            }),
        );
        assert!(ChannelName::parse(&"c".repeat(MAX_CHANNEL_NAME_BYTES)).is_ok());
        assert_eq!(
            ChannelName::parse("a\0b"),
            Err(ChannelNameError::ContainsNul)
        );
    }

    /// Notifications that keep arriving start at most one probe per
    /// [`PROBE_AFTER_ACTIVITY`]; with no notification the probe waits for
    /// [`PROBE_AFTER_SILENCE`].
    #[test]
    fn the_next_probe_follows_activity_or_silence() {
        let just_under = |bound: Duration| bound - Duration::from_millis(1);
        assert_eq!(
            probe_due(ActivitySinceProbe::Seen, Duration::ZERO),
            ProbeDue::Later
        );
        assert_eq!(
            probe_due(ActivitySinceProbe::Seen, just_under(PROBE_AFTER_ACTIVITY)),
            ProbeDue::Later
        );
        assert_eq!(
            probe_due(ActivitySinceProbe::Seen, PROBE_AFTER_ACTIVITY),
            ProbeDue::Now
        );
        assert_eq!(
            probe_due(ActivitySinceProbe::NotSeen, PROBE_AFTER_ACTIVITY),
            ProbeDue::Later
        );
        assert_eq!(
            probe_due(ActivitySinceProbe::NotSeen, just_under(PROBE_AFTER_SILENCE)),
            ProbeDue::Later
        );
        assert_eq!(
            probe_due(ActivitySinceProbe::NotSeen, PROBE_AFTER_SILENCE),
            ProbeDue::Now
        );
        assert_eq!(
            probe_due(ActivitySinceProbe::Seen, PROBE_AFTER_SILENCE),
            ProbeDue::Now
        );
    }

    #[test]
    fn a_channel_name_is_quoted_with_doubled_quotes() {
        let name = ChannelName::parse("Task\"Queue").expect("valid name");
        assert_eq!(name.quoted(), "\"Task\"\"Queue\"");
    }

    #[test]
    fn queue_capacity_is_bounded() {
        assert_eq!(QueueCapacity::new(0), Err(QueueCapacityError::Zero));
        assert_eq!(
            QueueCapacity::new(MAX_PAYLOAD_QUEUE_CAPACITY + 1),
            Err(QueueCapacityError::AboveLimit {
                capacity: MAX_PAYLOAD_QUEUE_CAPACITY + 1,
                limit: MAX_PAYLOAD_QUEUE_CAPACITY
            }),
        );
        assert_eq!(
            QueueCapacity::new(DEFAULT_PAYLOAD_QUEUE_CAPACITY).map(QueueCapacity::get),
            Ok(DEFAULT_PAYLOAD_QUEUE_CAPACITY)
        );
    }

    #[tokio::test]
    async fn a_wake_slot_holds_one_signal_and_a_gap_wins() {
        let slot = WakeSlot::new();
        slot.push(WakeSignal::Notified);
        slot.push(WakeSignal::Notified);
        assert_eq!(slot.recv().await, WakeEvent::Notified);
        slot.push(WakeSignal::Notified);
        slot.push(WakeSignal::Gap);
        slot.push(WakeSignal::Notified);
        assert_eq!(slot.recv().await, WakeEvent::Gap);
        slot.stop();
        slot.push(WakeSignal::Notified);
        assert_eq!(slot.recv().await, WakeEvent::Stopped);
        assert_eq!(slot.recv().await, WakeEvent::Stopped);
    }

    #[tokio::test]
    async fn a_full_payload_queue_drops_the_oldest_and_reads_overflowed_once() {
        let queue = PayloadQueue::new(QueueCapacity::new(2).expect("valid capacity"));
        queue.push_payload("one");
        queue.push_payload("two");
        queue.push_payload("three");
        assert_eq!(queue.recv().await, PayloadEvent::Overflowed);
        assert_eq!(queue.recv().await, PayloadEvent::Payload("two".to_owned()));
        assert_eq!(
            queue.recv().await,
            PayloadEvent::Payload("three".to_owned())
        );
        queue.push_gap();
        assert_eq!(queue.recv().await, PayloadEvent::Gap);
        queue.stop();
        queue.push_payload("after stop");
        assert_eq!(queue.recv().await, PayloadEvent::Stopped);
    }

    #[tokio::test]
    async fn dispatch_follows_channel_and_payload_filter() {
        let shared = Shared {
            registry: Mutex::new(Registry {
                lifecycle: Lifecycle::Running,
                next_id: 0,
                subscribers: HashMap::new(),
            }),
            task_wake: Notify::new(),
            notifications: AtomicU64::new(0),
        };
        let any = Arc::new(WakeSlot::new());
        let filtered = Arc::new(WakeSlot::new());
        let other_channel = Arc::new(WakeSlot::new());
        let queue = Arc::new(PayloadQueue::new(QueueCapacity::new(4).expect("capacity")));
        let channel = ChannelName::parse("task_done").expect("valid name");
        let other = ChannelName::parse("workflow_done").expect("valid name");
        {
            let mut registry = lock(&shared.registry);
            for (id, channel, sink) in [
                (
                    0,
                    channel.clone(),
                    Sink::Wake {
                        filter: PayloadFilter::AnyPayload,
                        slot: Arc::clone(&any),
                    },
                ),
                (
                    1,
                    channel.clone(),
                    Sink::Wake {
                        filter: PayloadFilter::Payload("wanted".to_owned()),
                        slot: Arc::clone(&filtered),
                    },
                ),
                (
                    2,
                    other,
                    Sink::Wake {
                        filter: PayloadFilter::AnyPayload,
                        slot: Arc::clone(&other_channel),
                    },
                ),
                (3, channel, Sink::Payloads(Arc::clone(&queue))),
            ] {
                let _previous = registry.subscribers.insert(
                    SubscriberId(id),
                    Subscriber {
                        channel,
                        sink,
                        confirmation: Confirmation::Confirmed,
                    },
                );
            }
        }
        shared.dispatch("task_done", "other");
        shared.dispatch("task_done", "wanted");
        assert_eq!(shared.notification_count(), 2);
        assert_eq!(any.recv().await, WakeEvent::Notified);
        assert_eq!(filtered.recv().await, WakeEvent::Notified);
        assert_eq!(
            queue.recv().await,
            PayloadEvent::Payload("other".to_owned())
        );
        assert_eq!(
            queue.recv().await,
            PayloadEvent::Payload("wanted".to_owned())
        );
        shared.stop_all();
        assert_eq!(other_channel.recv().await, WakeEvent::Stopped);
        assert_eq!(queue.recv().await, PayloadEvent::Stopped);
    }

    /// The subscribers in the registry of `listener`.
    fn subscriber_count(listener: &Listener) -> usize {
        lock(&listener.owner.shared.registry).subscribers.len()
    }

    /// A caller that is dropped while its subscribe waits for the LISTEN
    /// leaves no subscriber behind, so the listener UNLISTENs the channel.
    /// The server accepts the connection and never answers, so the LISTEN
    /// cannot confirm while the test runs.
    #[tokio::test]
    async fn a_subscribe_dropped_before_its_listen_confirms_leaves_no_subscriber() {
        use crate::db::connect::DEFAULT_SOCKET_SETTINGS;
        use crate::db::environment::ConnectionEnvironment;
        use crate::db::url::parse_connection_url;

        let silent_server = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a local port");
        let port = silent_server
            .local_addr()
            .expect("the bound address")
            .port();
        let environment = ConnectionEnvironment::isolated();
        let settings = parse_connection_url(
            &format!("postgresql://user:pw@127.0.0.1:{port}/db"),
            &environment,
        )
        .expect("the URL parses");
        let plan = ConnectPlan::load(&settings, &environment, &DEFAULT_SOCKET_SETTINGS)
            .expect("the plan loads");
        let listener = Listener::start(Arc::new(plan));
        let channel = ChannelName::parse("task_done").expect("valid name");

        let mut subscribe = Box::pin(listener.subscribe_wake(channel, PayloadFilter::AnyPayload));
        assert!(
            futures::poll!(&mut subscribe).is_pending(),
            "the LISTEN cannot confirm on a server that never answers",
        );
        assert_eq!(subscriber_count(&listener), 1, "the subscribe registered");
        drop(subscribe);

        assert_eq!(
            subscriber_count(&listener),
            0,
            "the dropped subscribe left its subscriber behind",
        );
        drop(listener);
        drop(silent_server);
    }
}
