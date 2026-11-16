//! The process listener: one [`crate::db::Listener`] per process, session URL
//! and socket settings, shared by every broker of the process. Every LISTEN of a broker and
//! of the modules that hold it subscribes here; none of them opens a listener
//! connection of its own.
//!
//! - Brokers on the same session URL with the same socket settings share one
//!   listener and its one connection. The listener stops when the last broker that uses it drops.
//!   A listener task runs on one tokio runtime, so a broker on another
//!   runtime of the process gets its own listener.
//! - The listener starts at the first subscription. The session URL is
//!   parsed and its connection plan is loaded then, not when the broker is
//!   built, so building a broker reads no file.
//! - Every broker built from a config has a listener, observe-only brokers
//!   included (Python's web tool runs with migrations off and keeps its
//!   listener). Only a broker built from a pool with no session URL has none:
//!   every subscription is refused with [`ListenError::NoSessionUrl`].
//! - A refused plan is not kept: the next subscription loads it again.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use tokio::sync::OnceCell;

use crate::db::{
    parse_connection_url, ChannelName, ConnectPlan, ConnectPlanError, ConnectionEnvironment,
    EnvironmentError, Listener, MaskedUrl, PayloadFilter, PayloadSubscription, QueueCapacity,
    SocketSettings, SubscribeError, Transience, UrlError, WakeSubscription,
    DEFAULT_PAYLOAD_QUEUE_CAPACITY,
};

/// The channel of task completions; the payload is the task id.
pub const TASK_DONE_CHANNEL: &str = "task_done";

/// The channel of workflow ends and pauses; the payload is the workflow id.
pub const WORKFLOW_DONE_CHANNEL: &str = "workflow_done";

/// The prefix of a worker ping's reply channel; a 32-digit hex correlation id
/// follows it.
pub const WORKER_PONG_CHANNEL_PREFIX: &str = "horsies_worker_pong_";

/// The channel name of a fixed name, or of a generated name whose length the
/// caller bounds below the channel limit.
///
/// Panics when the name is refused: every caller passes a name that fits, so a
/// refusal is a defect.
pub(crate) fn fixed_channel(name: &str) -> ChannelName {
    match ChannelName::parse(name) {
        Ok(channel) => channel,
        Err(refused) => unreachable!("the channel name {name:?} is built to fit: {refused}"),
    }
}

/// The queue size of every payload subscription of the broker: the size of
/// Python's listener queues.
pub(crate) fn default_payload_capacity() -> QueueCapacity {
    match QueueCapacity::new(DEFAULT_PAYLOAD_QUEUE_CAPACITY) {
        Ok(capacity) => capacity,
        Err(refused) => {
            unreachable!("DEFAULT_PAYLOAD_QUEUE_CAPACITY is within the queue limit: {refused}")
        }
    }
}

/// Loading the listener's connection plan failed. Nothing was connected.
#[derive(Debug, thiserror::Error)]
pub enum ListenerPlanError {
    #[error("the connection environment could not be read: {0}")]
    Environment(EnvironmentError),
    #[error("the session URL is refused: {0}")]
    Url(UrlError),
    #[error("the connection plan of the session URL could not be loaded: {0}")]
    Plan(ConnectPlanError),
    #[error("the runtime shut down while the connection plan of the session URL loaded")]
    LoadCancelled,
}

/// A subscription on the broker's listener was refused. Nothing is
/// subscribed.
#[derive(Debug, thiserror::Error)]
pub enum ListenError {
    /// The broker was built from a pool with no session URL, so it has no
    /// listener. It does not clear for this broker.
    #[error("this broker has no listener: it was built from a pool with no session URL")]
    NoSessionUrl,
    /// The session URL, the environment or a file the URL names was refused.
    /// The same settings are refused again.
    #[error("the listener cannot start: {0}")]
    PlanRefused(ListenerPlanError),
    /// The listener refused the subscription; its transience says whether a
    /// later subscription can succeed.
    #[error("{0}")]
    Subscribe(SubscribeError),
}

impl ListenError {
    pub fn transience(&self) -> Transience {
        match self {
            ListenError::NoSessionUrl | ListenError::PlanRefused(_) => Transience::Permanent,
            ListenError::Subscribe(refused) => refused.transience(),
        }
    }

    /// Whether a later subscription can succeed.
    pub fn is_retryable(&self) -> bool {
        match self.transience() {
            Transience::Transient => true,
            Transience::Permanent => false,
        }
    }
}

/// The session URL text. `Debug` does not show it: the URL can hold a
/// password.
#[derive(Clone)]
struct SessionUrl(String);

impl fmt::Debug for SessionUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SessionUrl(..)")
    }
}

#[derive(Debug)]
enum ListenerSource {
    SessionUrl {
        session_url: SessionUrl,
        socket: SocketSettings,
    },
    /// A broker built from a pool with no session URL.
    NoSessionUrl,
}

/// The started listener and the session URL it connects to, with the
/// password masked.
pub struct StartedListener {
    listener: Listener,
    masked_url: MaskedUrl,
}

impl StartedListener {
    /// The session URL, with the password masked.
    pub fn masked_url(&self) -> &MaskedUrl {
        &self.masked_url
    }

    /// Subscribes to wakes of `channel`; see [`Listener::subscribe_wake`].
    pub async fn subscribe_wake(
        &self,
        channel: ChannelName,
        filter: PayloadFilter,
    ) -> Result<WakeSubscription, ListenError> {
        self.listener
            .subscribe_wake(channel, filter)
            .await
            .map_err(ListenError::Subscribe)
    }

    /// Subscribes to the payloads of `channel`; see
    /// [`Listener::subscribe_payloads`].
    pub async fn subscribe_payloads(
        &self,
        channel: ChannelName,
        capacity: QueueCapacity,
    ) -> Result<PayloadSubscription, ListenError> {
        self.listener
            .subscribe_payloads(channel, capacity)
            .await
            .map_err(ListenError::Subscribe)
    }
}

/// The listener of one broker: a reference to the process listener of its
/// session URL. It holds no connection until the first subscription. When the
/// last broker that uses a process listener drops, the listener stops: every
/// subscription then reads `Stopped`.
pub struct ProcessListener {
    source: ListenerSource,
    started: OnceCell<Arc<StartedListener>>,
}

impl fmt::Debug for ProcessListener {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProcessListener")
            .field("source", &self.source)
            .field("started", &self.started.initialized())
            .finish()
    }
}

impl ProcessListener {
    /// A listener on `session_url` whose connection uses `socket`. Nothing is
    /// parsed or read here.
    pub(crate) fn on_session_url(session_url: &str, socket: SocketSettings) -> ProcessListener {
        ProcessListener {
            source: ListenerSource::SessionUrl {
                session_url: SessionUrl(session_url.to_owned()),
                socket,
            },
            started: OnceCell::new(),
        }
    }

    /// No listener: every subscription is refused with
    /// [`ListenError::NoSessionUrl`].
    pub(crate) fn without_session_url() -> ProcessListener {
        ProcessListener {
            source: ListenerSource::NoSessionUrl,
            started: OnceCell::new(),
        }
    }

    /// The started listener. The first call takes the running process
    /// listener of the session URL on this runtime, or loads the connection
    /// plan (blocking reads run off the runtime threads) and starts one. The
    /// connection opens at the first subscription.
    pub async fn start(&self) -> Result<&StartedListener, ListenError> {
        match &self.source {
            ListenerSource::NoSessionUrl => Err(ListenError::NoSessionUrl),
            ListenerSource::SessionUrl {
                session_url,
                socket,
            } => self
                .started
                .get_or_try_init(|| {
                    process_listener(ListenerKey {
                        session_url: session_url.clone(),
                        socket: *socket,
                    })
                })
                .await
                .map(|started| &**started),
        }
    }

    /// Subscribes to wakes of `channel`. Returns once the channel is
    /// LISTENed, or once a reconnect is under way (a gap follows it).
    pub async fn subscribe_wake(
        &self,
        channel: ChannelName,
        filter: PayloadFilter,
    ) -> Result<WakeSubscription, ListenError> {
        self.start().await?.subscribe_wake(channel, filter).await
    }

    /// Subscribes to the payloads of `channel`.
    pub async fn subscribe_payloads(
        &self,
        channel: ChannelName,
        capacity: QueueCapacity,
    ) -> Result<PayloadSubscription, ListenError> {
        self.start()
            .await?
            .subscribe_payloads(channel, capacity)
            .await
    }
}

/// What makes two brokers share one listener: the same session URL text and
/// the same socket settings (on the same runtime).
#[derive(Clone)]
struct ListenerKey {
    session_url: SessionUrl,
    socket: SocketSettings,
}

impl ListenerKey {
    fn matches(&self, other: &ListenerKey) -> bool {
        self.session_url.0 == other.session_url.0 && self.socket == other.socket
    }
}

/// A process listener of one runtime and key. The entry holds no strong
/// reference, so it does not keep its listener alive.
struct RegisteredListener {
    runtime: tokio::runtime::Id,
    key: ListenerKey,
    listener: Weak<StartedListener>,
}

/// The process listeners. It holds an entry per live listener: each
/// registration first drops the entries whose listener has stopped.
static PROCESS_LISTENERS: Mutex<Vec<RegisteredListener>> = Mutex::new(Vec::new());

/// Locks the process listeners. A poisoned lock means a panic while it was
/// held, which already crashed its thread loudly; every critical section
/// leaves the list whole, so the list is used as it is.
fn lock_process_listeners() -> MutexGuard<'static, Vec<RegisteredListener>> {
    PROCESS_LISTENERS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// The running listener of `runtime` and `key`, when one is registered.
fn find_running(
    registered: &[RegisteredListener],
    runtime: tokio::runtime::Id,
    key: &ListenerKey,
) -> Option<Arc<StartedListener>> {
    registered
        .iter()
        .filter(|entry| entry.runtime == runtime && entry.key.matches(key))
        .find_map(|entry| {
            entry
                .listener
                .upgrade()
                .filter(|started| started.listener.is_running())
        })
}

/// The process listener of `key` on the current runtime: the running one,
/// or a new one. A refused plan starts nothing.
async fn process_listener(key: ListenerKey) -> Result<Arc<StartedListener>, ListenError> {
    let runtime = tokio::runtime::Handle::current().id();
    let running = {
        let registered = lock_process_listeners();
        find_running(&registered, runtime, &key)
    };
    if let Some(running) = running {
        return Ok(running);
    }
    let plan = load_plan(key.clone())
        .await
        .map_err(ListenError::PlanRefused)?;
    Ok(register(runtime, key, plan))
}

/// Starts and registers the listener of `plan`. When another broker of the
/// same runtime registered a running listener on the same URL while this plan
/// loaded, that listener is returned and `plan` is dropped unused.
fn register(
    runtime: tokio::runtime::Id,
    key: ListenerKey,
    plan: ConnectPlan,
) -> Arc<StartedListener> {
    let mut registered = lock_process_listeners();
    registered.retain(|entry| entry.listener.strong_count() > 0);
    if let Some(running) = find_running(&registered, runtime, &key) {
        return running;
    }
    let masked_url = plan.masked_url().clone();
    let started = Arc::new(StartedListener {
        listener: Listener::start(Arc::new(plan)),
        masked_url,
    });
    registered.push(RegisteredListener {
        runtime,
        key,
        listener: Arc::downgrade(&started),
    });
    started
}

/// Runs [`read_plan`] on the blocking pool. A panic in it is resumed here.
async fn load_plan(key: ListenerKey) -> Result<ConnectPlan, ListenerPlanError> {
    match tokio::task::spawn_blocking(move || read_plan(&key)).await {
        Ok(loaded) => loaded,
        Err(join_error) => match join_error.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(_cancelled) => Err(ListenerPlanError::LoadCancelled),
        },
    }
}

/// Reads the connection environment, parses the session URL and loads its
/// plan with the key's socket settings. Blocking: reads the TLS files the URL
/// names, `~/.pgpass` when no other password is given, and the process user
/// name when the URL names no user.
fn read_plan(key: &ListenerKey) -> Result<ConnectPlan, ListenerPlanError> {
    let environment = ConnectionEnvironment::capture().map_err(ListenerPlanError::Environment)?;
    let settings =
        parse_connection_url(&key.session_url.0, &environment).map_err(ListenerPlanError::Url)?;
    ConnectPlan::load(&settings, &environment, &key.socket).map_err(ListenerPlanError::Plan)
}

#[cfg(test)]
impl ProcessListener {
    /// The shared listener itself, so a test can hold a weak reference.
    async fn started_for_test(&self) -> &Arc<StartedListener> {
        let _started = self.start().await.expect("the plan loads");
        self.started
            .get()
            .expect("start fills the cell when it succeeds")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{Keepalives, DEFAULT_SOCKET_SETTINGS};

    fn any_channel() -> ChannelName {
        fixed_channel("horsies_listener_unit")
    }

    #[tokio::test]
    async fn a_listener_without_a_session_url_refuses_every_subscription() {
        let listener = ProcessListener::without_session_url();

        let Err(refused) = listener
            .subscribe_wake(any_channel(), PayloadFilter::AnyPayload)
            .await
        else {
            panic!("a listener without a session URL must refuse a wake subscription");
        };
        assert!(matches!(refused, ListenError::NoSessionUrl));
        assert!(!refused.is_retryable());

        let Err(refused) = listener
            .subscribe_payloads(any_channel(), default_payload_capacity())
            .await
        else {
            panic!("a listener without a session URL must refuse a payload subscription");
        };
        assert!(matches!(refused, ListenError::NoSessionUrl));
    }

    /// A URL that the core's parser refuses fails the subscription, is not
    /// retryable, and leaves the listener unstarted.
    #[tokio::test]
    async fn a_refused_session_url_is_a_permanent_refusal() {
        let listener = ProcessListener::on_session_url(
            "mysql://horsies@127.0.0.1:1/horsies",
            DEFAULT_SOCKET_SETTINGS,
        );

        let Err(refused) = listener
            .subscribe_wake(any_channel(), PayloadFilter::AnyPayload)
            .await
        else {
            panic!("a refused URL must fail the subscription");
        };

        assert!(matches!(refused, ListenError::PlanRefused(_)), "{refused}");
        assert!(!refused.is_retryable());
        assert!(!listener.started.initialized());
    }

    /// A URL whose plan loads with no file and no environment password.
    const PLAN_ONLY_URL: &str = "postgresql://horsies:pw@127.0.0.1:1/horsies";

    /// Two brokers on the same session URL in one runtime share one listener;
    /// another URL gets its own. Starting connects to nothing.
    #[tokio::test]
    async fn brokers_on_one_session_url_share_one_listener() {
        let first = ProcessListener::on_session_url(PLAN_ONLY_URL, DEFAULT_SOCKET_SETTINGS);
        let second = ProcessListener::on_session_url(PLAN_ONLY_URL, DEFAULT_SOCKET_SETTINGS);
        let other = ProcessListener::on_session_url(
            &format!("{PLAN_ONLY_URL}?application_name=other"),
            DEFAULT_SOCKET_SETTINGS,
        );

        let first_started = first.start().await.expect("the plan loads");
        let second_started = second.start().await.expect("the plan loads");
        let other_started = other.start().await.expect("the plan loads");

        assert!(std::ptr::eq(first_started, second_started));
        assert!(!std::ptr::eq(first_started, other_started));
    }

    /// Other socket settings on the same URL get their own listener: one
    /// connection cannot carry two sets of socket settings.
    #[tokio::test]
    async fn other_socket_settings_get_their_own_listener() {
        let url = format!("{PLAN_ONLY_URL}?application_name=socket_settings");
        let default = ProcessListener::on_session_url(&url, DEFAULT_SOCKET_SETTINGS);
        let no_keepalives = ProcessListener::on_session_url(
            &url,
            SocketSettings {
                keepalives: Keepalives::Off,
                ..DEFAULT_SOCKET_SETTINGS
            },
        );

        let default_started = default.start().await.expect("the plan loads");
        let no_keepalives_started = no_keepalives.start().await.expect("the plan loads");

        assert!(!std::ptr::eq(default_started, no_keepalives_started));
    }

    /// The listener stops when the last broker that uses it drops; the
    /// process keeps no reference to it.
    #[tokio::test]
    async fn the_last_broker_drop_stops_the_shared_listener() {
        let url = format!("{PLAN_ONLY_URL}?application_name=last_drop");
        let first = ProcessListener::on_session_url(&url, DEFAULT_SOCKET_SETTINGS);
        let second = ProcessListener::on_session_url(&url, DEFAULT_SOCKET_SETTINGS);
        let shared = Arc::downgrade(first.started_for_test().await);
        assert!(second.start().await.is_ok(), "the plan loads");

        drop(first);
        assert!(
            shared.upgrade().is_some(),
            "the second broker still uses it"
        );
        drop(second);

        assert!(shared.upgrade().is_none());
    }

    #[test]
    fn debug_output_does_not_show_the_session_url() {
        let listener = ProcessListener::on_session_url(
            "postgresql://horsies:secret@db/horsies",
            DEFAULT_SOCKET_SETTINGS,
        );

        let shown = format!("{listener:?}");

        assert!(!shown.contains("secret"), "{shown}");
    }

    #[test]
    fn subscribe_refusals_keep_the_listener_s_transience() {
        let transient = ListenError::Subscribe(SubscribeError::SubscriptionLimit { limit: 1 });
        let permanent = ListenError::Subscribe(SubscribeError::ListenerStopped);

        assert!(transient.is_retryable());
        assert!(!permanent.is_retryable());
    }

    #[test]
    fn the_fixed_channels_fit_the_channel_limit() {
        for name in [TASK_DONE_CHANNEL, WORKFLOW_DONE_CHANNEL] {
            assert_eq!(fixed_channel(name).as_str(), name);
        }
        let reply = format!(
            "{WORKER_PONG_CHANNEL_PREFIX}{}",
            uuid::Uuid::new_v4().simple()
        );
        assert_eq!(fixed_channel(&reply).as_str(), reply);
    }
}
