//! PostgreSQL LISTEN to SSE invalidation events.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use sqlx::postgres::PgListener;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::broker::PostgresBroker;

pub const CHANNEL_TOPICS: [(&str, &str); 3] = [
    ("horsies_task_status", "tasks"),
    ("horsies_workflow_status", "workflows"),
    ("horsies_worker_state", "workers"),
];
pub const TOPIC_DEGRADED: &str = "degraded";
pub const MAX_IDS_PER_EVENT: usize = 100;
pub const DEBOUNCE: Duration = Duration::from_millis(250);
pub const HEARTBEAT: Duration = Duration::from_secs(15);

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

type Subscriber = mpsc::UnboundedSender<Option<TopicEvent>>;

pub struct EventSubscription {
    receiver: mpsc::UnboundedReceiver<Option<TopicEvent>>,
}

impl EventSubscription {
    pub(crate) async fn recv(&mut self) -> Option<Option<TopicEvent>> {
        self.receiver.recv().await
    }
}

pub struct EventBroadcaster {
    broker: Arc<PostgresBroker>,
    debounce: Duration,
    subscribers: Mutex<Vec<Subscriber>>,
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
        let (sender, receiver) = mpsc::unbounded_channel();
        self.subscribers.lock().await.push(sender);
        if self.degraded() {
            self.degrade().await;
        } else if !self.started.load(Ordering::Acquire) {
            self.start().await;
        }
        EventSubscription { receiver }
    }

    async fn start(self: &Arc<Self>) {
        let _start = self.start_lock.lock().await;
        if self.started.load(Ordering::Acquire) || self.degraded() {
            return;
        }

        let mut listener = match PgListener::connect_with(self.broker.session_pool()).await {
            Ok(listener) => listener,
            Err(error) => {
                tracing::warn!(error = %error, "monitoring event listener failed to start");
                self.degrade().await;
                return;
            }
        };
        for (channel, _) in CHANNEL_TOPICS {
            if let Err(error) = listener.listen(channel).await {
                tracing::warn!(channel, error = %error, "monitoring event listener failed to subscribe");
                self.degrade().await;
                return;
            }
        }

        self.started.store(true, Ordering::Release);
        let broadcaster = Arc::downgrade(self);
        let debounce = self.debounce;
        let cancel = self.cancel.clone();
        let handle = tokio::spawn(async move {
            Self::run(broadcaster, listener, debounce, cancel).await;
        });
        *self.task.lock().await = Some(handle);
    }

    async fn run(
        broadcaster: Weak<Self>,
        mut listener: PgListener,
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
                notification = listener.recv() => {
                    match notification {
                        Ok(notification) => {
                            if let Some((_, topic)) = CHANNEL_TOPICS
                                .iter()
                                .find(|(channel, _)| *channel == notification.channel())
                            {
                                if coalescer.record(topic, notification.payload()) {
                                    deadlines.insert(
                                        (*topic).to_owned(),
                                        tokio::time::Instant::now() + debounce,
                                    );
                                }
                            }
                        }
                        Err(error) => {
                            tracing::warn!(error = %error, "monitoring event listener failed");
                            if let Some(broadcaster) = broadcaster.upgrade() {
                                broadcaster.degrade().await;
                            }
                            break;
                        }
                    }
                }
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
            .retain(|subscriber| subscriber.send(event.clone()).is_ok());
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
        self.subscribers.lock().await.clear();
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
