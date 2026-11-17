//! PostgreSQL LISTEN to SSE invalidation events.
//!
//! The stream subscribes the three status channels on the broker's listener,
//! the one LISTEN connection of the broker; it opens no connection of its own.
//! A listener gap or a full subscription queue sends one event with no ids
//! for the topic ("refresh the whole topic"); a stopped listener sends the
//! degraded frame.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, MutexGuard, Weak};
use std::task::Poll;
use std::time::Duration;

use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::broker::process_listener::{default_payload_capacity, fixed_channel, ListenError};
use crate::broker::PostgresBroker;
use crate::db::{PayloadEvent, PayloadSubscription};

pub const CHANNEL_TOPICS: [(&str, &str); 3] = [
    ("horsies_task_status", "tasks"),
    ("horsies_workflow_status", "workflows"),
    ("horsies_worker_state", "workers"),
];
pub const TOPIC_DEGRADED: &str = "degraded";
pub const MAX_IDS_PER_EVENT: usize = 100;
pub const DEBOUNCE: Duration = Duration::from_millis(250);
pub const HEARTBEAT: Duration = Duration::from_secs(15);

/// The most items one web client can have queued. On overflow, the client's
/// queued events are dropped and it gets one event with no ids for every
/// topic ("refresh the whole topic"), so a slow client cannot grow the web
/// process. Python keeps an unbounded queue (`python/horsies/web/events.py:211`).
pub const SUBSCRIBER_QUEUE_CAPACITY: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicEvent {
    pub topic: String,
    pub ids: Vec<String>,
}

#[derive(Debug, Default)]
struct TopicWindow {
    ids: Vec<String>,
    seen: HashSet<String>,
    overflowed: bool,
}

#[derive(Debug)]
pub struct EventCoalescer {
    max_ids: usize,
    windows: HashMap<String, TopicWindow>,
    order: Vec<String>,
}

impl Default for EventCoalescer {
    fn default() -> Self {
        Self::new(MAX_IDS_PER_EVENT)
    }
}

impl EventCoalescer {
    pub fn new(max_ids: usize) -> Self {
        Self {
            max_ids,
            windows: HashMap::new(),
            order: Vec::new(),
        }
    }

    pub fn record(&mut self, topic: &str, entity_id: &str) -> bool {
        let created = !self.windows.contains_key(topic);
        let window = self.windows.entry(topic.to_owned()).or_insert_with(|| {
            self.order.push(topic.to_owned());
            TopicWindow::default()
        });
        if window.overflowed || !window.seen.insert(entity_id.to_owned()) {
            return created;
        }
        window.ids.push(entity_id.to_owned());
        if window.ids.len() > self.max_ids {
            window.overflowed = true;
            window.ids.clear();
            window.seen.clear();
        }
        created
    }

    pub fn drain(&mut self) -> Vec<TopicEvent> {
        self.order
            .drain(..)
            .filter_map(|topic| {
                self.windows.remove(&topic).map(|window| TopicEvent {
                    topic,
                    ids: window.ids,
                })
            })
            .collect()
    }

    fn drain_topic(&mut self, topic: &str) -> Option<TopicEvent> {
        self.order.retain(|pending| pending != topic);
        self.windows.remove(topic).map(|window| TopicEvent {
            topic: topic.to_owned(),
            ids: window.ids,
        })
    }
}

/// One item of a client stream: an event, or `None`, the degraded marker
/// that ends the stream.
type StreamItem = Option<TopicEvent>;

/// One event with no ids: the client reads the whole topic again.
fn refresh_event(topic: &str) -> TopicEvent {
    TopicEvent {
        topic: topic.to_owned(),
        ids: Vec::new(),
    }
}

/// One event with no ids for every topic: the client reads each whole topic
/// again.
pub(crate) fn refresh_events() -> Vec<TopicEvent> {
    CHANNEL_TOPICS
        .iter()
        .map(|(_, topic)| refresh_event(topic))
        .collect()
}

/// What the stream does with one event of a topic subscription.
#[derive(Debug, PartialEq, Eq)]
enum TopicStep {
    /// Record the entity id in the topic's window.
    Record(String),
    /// Notifications of the topic may be lost (a listener gap or a full
    /// queue): send one event with no ids for the topic.
    Refresh,
    /// The listener stopped: send the degraded frame and end.
    Degrade,
}

fn topic_step(event: PayloadEvent) -> TopicStep {
    match event {
        PayloadEvent::Payload(entity_id) => TopicStep::Record(entity_id),
        PayloadEvent::Gap | PayloadEvent::Overflowed => TopicStep::Refresh,
        PayloadEvent::Stopped => TopicStep::Degrade,
    }
}

/// The payload subscription of one status channel, with its topic.
struct TopicSubscription {
    topic: &'static str,
    subscription: PayloadSubscription,
}

/// One pending wait of [`TopicSubscriptions::next`].
type TopicWait<'a> = Pin<Box<dyn Future<Output = (&'static str, PayloadEvent)> + Send + 'a>>;

/// The subscriptions of the three status channels on the broker's listener.
/// Each is a bounded payload queue, so a slow stream cannot hold the result
/// waiters, and the waiters cannot hold the stream.
struct TopicSubscriptions(Vec<TopicSubscription>);

impl TopicSubscriptions {
    /// Subscribes every channel of [`CHANNEL_TOPICS`]. On a refusal, the
    /// channels already subscribed are dropped again.
    async fn subscribe(broker: &PostgresBroker) -> Result<TopicSubscriptions, ListenError> {
        let mut topics = Vec::with_capacity(CHANNEL_TOPICS.len());
        for (channel, topic) in CHANNEL_TOPICS {
            let subscription = broker
                .listener()
                .subscribe_payloads(fixed_channel(channel), default_payload_capacity())
                .await?;
            topics.push(TopicSubscription {
                topic,
                subscription,
            });
        }
        Ok(TopicSubscriptions(topics))
    }

    /// The next event of any topic. Cancel-safe: a subscription's `recv`
    /// keeps its event when it is dropped before it returns.
    async fn next(&self) -> (&'static str, PayloadEvent) {
        let mut waits: Vec<TopicWait<'_>> = self
            .0
            .iter()
            .map(|topic| {
                Box::pin(async move { (topic.topic, topic.subscription.recv().await) })
                    as TopicWait<'_>
            })
            .collect();
        std::future::poll_fn(|context| {
            for wait in waits.iter_mut() {
                if let Poll::Ready(event) = wait.as_mut().poll(context) {
                    return Poll::Ready(event);
                }
            }
            Poll::Pending
        })
        .await
    }
}

#[derive(Debug, Default)]
struct QueueState {
    items: VecDeque<StreamItem>,
    closed: bool,
}

/// The bounded queue of one web client, written by the broadcaster and read
/// by the client's stream. It holds at most [`SUBSCRIBER_QUEUE_CAPACITY`]
/// items.
#[derive(Debug, Default)]
struct SubscriberQueue {
    state: std::sync::Mutex<QueueState>,
    ready: Notify,
}

impl SubscriberQueue {
    /// Lock the state. A poisoned lock is recovered: a panic while holding
    /// it cannot leave an item half-written.
    fn lock(&self) -> MutexGuard<'_, QueueState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Queue one item. A full queue drops its items and takes one refresh
    /// event per topic instead; a degraded marker is kept after them. A
    /// closed queue takes nothing.
    fn push(&self, item: StreamItem) {
        {
            let mut state = self.lock();
            if state.closed {
                return;
            }
            match state.items.len() < SUBSCRIBER_QUEUE_CAPACITY {
                true => state.items.push_back(item),
                false => {
                    state.items.clear();
                    state.items.extend(refresh_events().into_iter().map(Some));
                    match item {
                        Some(_covered_by_the_refresh) => {}
                        None => state.items.push_back(None),
                    }
                }
            }
        }
        self.ready.notify_one();
    }

    /// End the stream after the items already queued.
    fn close(&self) {
        self.lock().closed = true;
        self.ready.notify_one();
    }

    /// The next item, or `None` once the queue is closed and empty.
    async fn pop(&self) -> Option<StreamItem> {
        loop {
            {
                let mut state = self.lock();
                if let Some(item) = state.items.pop_front() {
                    return Some(item);
                }
                if state.closed {
                    return None;
                }
            }
            // `notify_one` stores a permit when no task waits, so a push
            // between the check above and this wait is not lost.
            self.ready.notified().await;
        }
    }
}

pub struct EventSubscription {
    queue: Arc<SubscriberQueue>,
}

impl EventSubscription {
    pub(crate) async fn recv(&mut self) -> Option<Option<TopicEvent>> {
        self.queue.pop().await
    }
}

pub struct EventBroadcaster {
    broker: Arc<PostgresBroker>,
    debounce: Duration,
    subscribers: Mutex<Vec<Weak<SubscriberQueue>>>,
    start_lock: Mutex<()>,
    task: Mutex<Option<JoinHandle<()>>>,
    cancel: CancellationToken,
    started: AtomicBool,
    degraded: AtomicBool,
}

impl Drop for EventBroadcaster {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl EventBroadcaster {
    pub fn new(broker: Arc<PostgresBroker>) -> Arc<Self> {
        Self::with_debounce(broker, DEBOUNCE)
    }

    pub(crate) fn with_debounce(broker: Arc<PostgresBroker>, debounce: Duration) -> Arc<Self> {
        Arc::new(Self {
            broker,
            debounce,
            subscribers: Mutex::new(Vec::new()),
            start_lock: Mutex::new(()),
            task: Mutex::new(None),
            cancel: CancellationToken::new(),
            started: AtomicBool::new(false),
            degraded: AtomicBool::new(false),
        })
    }

    pub fn degraded(&self) -> bool {
        self.degraded.load(Ordering::Acquire)
    }

    pub async fn subscribe(self: &Arc<Self>) -> EventSubscription {
        let queue = Arc::new(SubscriberQueue::default());
        self.subscribers.lock().await.push(Arc::downgrade(&queue));
        if self.degraded() {
            self.degrade().await;
        } else if !self.started.load(Ordering::Acquire) {
            self.start().await;
        }
        EventSubscription { queue }
    }

    async fn start(self: &Arc<Self>) {
        let _start = self.start_lock.lock().await;
        if self.started.load(Ordering::Acquire) || self.degraded() {
            return;
        }

        let topics = match TopicSubscriptions::subscribe(&self.broker).await {
            Ok(topics) => topics,
            Err(error) => {
                tracing::warn!(error = %error, "monitoring event subscriptions failed to start");
                self.degrade().await;
                return;
            }
        };

        self.started.store(true, Ordering::Release);
        let broadcaster = Arc::downgrade(self);
        let debounce = self.debounce;
        let cancel = self.cancel.clone();
        let handle = tokio::spawn(async move {
            Self::run(broadcaster, topics, debounce, cancel).await;
        });
        *self.task.lock().await = Some(handle);
    }

    async fn run(
        broadcaster: Weak<Self>,
        topics: TopicSubscriptions,
        debounce: Duration,
        cancel: CancellationToken,
    ) {
        let mut coalescer = EventCoalescer::default();
        let mut deadlines: HashMap<String, tokio::time::Instant> = HashMap::new();
        loop {
            let next_deadline = deadlines
                .values()
                .copied()
                .min()
                .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(86_400));
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep_until(next_deadline) => {
                    let Some(broadcaster) = broadcaster.upgrade() else {
                        break;
                    };
                    let now = tokio::time::Instant::now();
                    let due: Vec<String> = deadlines
                        .iter()
                        .filter(|(_, deadline)| **deadline <= now)
                        .map(|(topic, _)| topic.clone())
                        .collect();
                    for topic in due {
                        deadlines.remove(&topic);
                        if let Some(event) = coalescer.drain_topic(&topic) {
                            broadcaster.publish(Some(event)).await;
                        }
                    }
                }
                (topic, event) = topics.next() => match topic_step(event) {
                    TopicStep::Record(entity_id) => {
                        if coalescer.record(topic, &entity_id) {
                            deadlines.insert(
                                topic.to_owned(),
                                tokio::time::Instant::now() + debounce,
                            );
                        }
                    }
                    // Notifications of the topic were lost: the pending ids
                    // are covered by one event with no ids for the topic.
                    TopicStep::Refresh => {
                        tracing::warn!(
                            topic,
                            "monitoring events may have been lost; sending a refresh of the topic",
                        );
                        let _covered_by_the_refresh = coalescer.drain_topic(topic);
                        let _pending_flush = deadlines.remove(topic);
                        let Some(broadcaster) = broadcaster.upgrade() else {
                            break;
                        };
                        broadcaster.publish(Some(refresh_event(topic))).await;
                    }
                    TopicStep::Degrade => {
                        tracing::warn!("the listener stopped; the monitoring event stream degrades");
                        if let Some(broadcaster) = broadcaster.upgrade() {
                            broadcaster.degrade().await;
                        }
                        break;
                    }
                },
            }
        }
        if let Some(broadcaster) = broadcaster.upgrade() {
            broadcaster.started.store(false, Ordering::Release);
        }
    }

    async fn publish(&self, event: Option<TopicEvent>) {
        self.subscribers
            .lock()
            .await
            .retain(|subscriber| match subscriber.upgrade() {
                Some(queue) => {
                    queue.push(event.clone());
                    true
                }
                None => false,
            });
    }

    async fn degrade(&self) {
        if !self.degraded.swap(true, Ordering::AcqRel) {
            self.publish(None).await;
        } else {
            self.publish(None).await;
        }
    }

    pub async fn close(&self) {
        self.cancel.cancel();
        if let Some(handle) = self.task.lock().await.take() {
            let _ = handle.await;
        }
        self.publish(None).await;
        for subscriber in self.subscribers.lock().await.drain(..) {
            if let Some(queue) = subscriber.upgrade() {
                queue.close();
            }
        }
    }
}

pub(crate) fn data_frame(event: &TopicEvent) -> String {
    let ids = event
        .ids
        .iter()
        .map(|id| serde_json::to_string(id).expect("string JSON encoding cannot fail"))
        .collect::<Vec<_>>()
        .join(", ");
    let topic = serde_json::to_string(&event.topic).expect("string JSON encoding cannot fail");
    format!("{{\"topic\": {topic}, \"ids\": [{ids}]}}")
}

pub(crate) fn degraded_frame() -> &'static str {
    "{\"topic\": \"degraded\"}"
}

#[cfg(test)]
mod queue_tests {
    use super::*;

    fn event(topic: &str, id: &str) -> StreamItem {
        Some(TopicEvent {
            topic: topic.to_owned(),
            ids: vec![id.to_owned()],
        })
    }

    async fn drain(queue: &SubscriberQueue) -> Vec<StreamItem> {
        queue.close();
        let mut items = Vec::new();
        while let Some(item) = queue.pop().await {
            items.push(item);
        }
        items
    }

    /// A payload is recorded; a gap or an overflow refreshes the topic; a
    /// stopped listener degrades the stream.
    #[test]
    fn a_topic_event_maps_to_its_step() {
        assert_eq!(
            topic_step(PayloadEvent::Payload("t1".to_owned())),
            TopicStep::Record("t1".to_owned())
        );
        assert_eq!(topic_step(PayloadEvent::Gap), TopicStep::Refresh);
        assert_eq!(topic_step(PayloadEvent::Overflowed), TopicStep::Refresh);
        assert_eq!(topic_step(PayloadEvent::Stopped), TopicStep::Degrade);
        assert_eq!(
            refresh_event("workers"),
            TopicEvent {
                topic: "workers".to_owned(),
                ids: Vec::new(),
            }
        );
    }

    #[tokio::test]
    async fn items_below_the_bound_arrive_in_order() {
        let queue = SubscriberQueue::default();
        queue.push(event("tasks", "a"));
        queue.push(event("workers", "w"));

        assert_eq!(
            drain(&queue).await,
            vec![event("tasks", "a"), event("workers", "w")]
        );
    }

    /// A full queue drops what it holds and asks the client to read every
    /// topic again, so it never holds more than the bound.
    #[tokio::test]
    async fn an_overflow_replaces_the_queue_with_one_refresh_per_topic() {
        let queue = SubscriberQueue::default();
        for index in 0..SUBSCRIBER_QUEUE_CAPACITY {
            queue.push(event("tasks", &index.to_string()));
        }
        assert_eq!(queue.lock().items.len(), SUBSCRIBER_QUEUE_CAPACITY);

        queue.push(event("tasks", "overflow"));

        let refreshed: Vec<StreamItem> = refresh_events().into_iter().map(Some).collect();
        assert_eq!(refreshed.len(), CHANNEL_TOPICS.len());
        assert!(refreshed
            .iter()
            .all(|item| item.as_ref().is_some_and(|event| event.ids.is_empty())));
        assert_eq!(drain(&queue).await, refreshed);
    }

    #[tokio::test]
    async fn an_overflow_keeps_the_degraded_marker_last() {
        let queue = SubscriberQueue::default();
        for index in 0..SUBSCRIBER_QUEUE_CAPACITY {
            queue.push(event("tasks", &index.to_string()));
        }

        queue.push(None);

        let items = drain(&queue).await;
        assert_eq!(items.len(), CHANNEL_TOPICS.len() + 1);
        assert_eq!(items.last(), Some(&None));
    }

    #[tokio::test]
    async fn a_closed_queue_takes_nothing_and_ends() {
        let queue = SubscriberQueue::default();
        queue.close();
        queue.push(event("tasks", "late"));

        assert_eq!(queue.pop().await, None);
    }

    #[tokio::test]
    async fn a_waiting_reader_wakes_on_a_push() {
        let queue = Arc::new(SubscriberQueue::default());
        let reader = tokio::spawn({
            let queue = Arc::clone(&queue);
            async move { queue.pop().await }
        });
        tokio::task::yield_now().await;

        queue.push(event("workflows", "wf"));

        let item = tokio::time::timeout(Duration::from_secs(5), reader)
            .await
            .expect("the reader wakes")
            .expect("the reader does not panic");
        assert_eq!(item, Some(event("workflows", "wf")));
    }

    /// A dropped client stream is removed at the next publish.
    #[tokio::test]
    async fn publish_drops_a_client_whose_stream_ended() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://horsies@127.0.0.1:1/horsies")
            .expect("a lazy pool opens no connection");
        let broadcaster = EventBroadcaster::new(Arc::new(PostgresBroker::from_pool(pool)));
        let kept = Arc::new(SubscriberQueue::default());
        let dropped = Arc::new(SubscriberQueue::default());
        broadcaster
            .subscribers
            .lock()
            .await
            .extend([Arc::downgrade(&kept), Arc::downgrade(&dropped)]);
        drop(dropped);

        broadcaster.publish(event("tasks", "a")).await;

        assert_eq!(broadcaster.subscribers.lock().await.len(), 1);
        assert_eq!(kept.lock().items.len(), 1);
    }
}
