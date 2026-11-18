//! The process listener: one [`crate::db::Listener`] per process, session URL
//! and socket settings, shared by every broker of the process. Every LISTEN of a broker and
//! of the modules that hold it subscribes here; none of them opens a listener
//! connection of its own.
//!
//! - Brokers on the same session URL with the same socket settings share one
//!   listener and its one connection, from every runtime of the process: the
//!   listener task and its connection run on the runtime of the process
//!   ([`crate::process_runtime`]). The listener stops when the last broker
//!   that uses it drops.
//! - When the runtime of the process cannot be used, each broker gets a
//!   listener of its own that refuses every subscription with the cause. It
//!   runs on the caller's runtime, so it is not shared, and a fork of the
//!   process never takes a listener of its parent.
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
use std::time::Duration;

use tokio::sync::OnceCell;

use crate::broker::error::is_retryable_statement_failure;
use crate::broker::pools::PoolSettingsError;
use crate::process_runtime::process_runtime;

use crate::db::{
    parse_connection_url, ChannelName, ConnectPlan, ConnectPlanError, ConnectionEnvironment,
    EnvironmentError, Listener, MaskedUrl, PayloadFilter, PayloadSubscription, QueueCapacity,
    SocketSettings, StatementFailure, SubscribeError, Transience, UrlError, WakeSubscription,
    DEFAULT_PAYLOAD_QUEUE_CAPACITY,
};

/// The channel of task completions; the payload is the task id.
pub const TASK_DONE_CHANNEL: &str = "task_done";

/// The channel of workflow ends and pauses; the payload is the workflow id.
pub const WORKFLOW_DONE_CHANNEL: &str = "workflow_done";

/// The prefix of a worker ping's reply channel; a 32-digit hex correlation id
/// follows it.
pub const WORKER_PONG_CHANNEL_PREFIX: &str = "horsies_worker_pong_";

/// The prefix of the worker boot's session check channel; a 32-digit hex id
/// follows it.
pub const SESSION_CHECK_CHANNEL_PREFIX: &str = "horsies_check_";

/// The longest the worker boot's session check waits for its NOTIFY
/// (Python's `SESSION_CAPABILITY_TIMEOUT`,
/// `python/horsies/core/brokers/session_capability.py:13`).
pub const SESSION_CAPABILITY_TIMEOUT: Duration = Duration::from_secs(2);

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

/// What ended the session check before its NOTIFY arrived.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckInterruption {
    /// The listener reconnected: the NOTIFY may have been sent while it was
    /// down.
    Reconnected,
    /// The listener stopped.
    Stopped,
}

impl fmt::Display for CheckInterruption {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CheckInterruption::Reconnected => formatter.write_str("reconnected"),
            CheckInterruption::Stopped => formatter.write_str("stopped"),
        }
    }
}

/// Why the session check of a worker boot did not pass. The check's
/// subscription is dropped; nothing else changed.
#[derive(Debug, thiserror::Error)]
pub enum SessionCheckFailure {
    /// LISTEN was accepted, but the NOTIFY did not arrive within the bound:
    /// the session URL does not keep session state (for example PgBouncer
    /// transaction pooling). Not retryable: the same URL fails again.
    #[error(
        "LISTEN on {session_url} received no NOTIFY within {} ms: the session URL does not \
         keep session state (for example PgBouncer transaction pooling); set \
         session_database_url to a direct or session-pooled URL",
        .waited.as_millis()
    )]
    NotCapable {
        session_url: MaskedUrl,
        waited: Duration,
    },
    /// The broker's core pools were closed and their settings were refused
    /// when they opened again. Not retryable: the same settings are refused
    /// again.
    #[error("the session check could not open the core pools: {0}")]
    PoolSettings(PoolSettingsError),
    /// The `SELECT 1` on the session core pool failed. A session URL that does
    /// not keep a transaction (PgBouncer statement pooling) is a permanent
    /// failure, not retryable; a database that cannot be reached is retryable.
    #[error("the session check's statement on the session pool failed: {0}")]
    SessionPool(StatementFailure),
    /// The check could not subscribe on the broker's listener. Retryable when
    /// the refusal is transient (a database that cannot be reached).
    #[error("the session check could not subscribe: {0}")]
    Subscribe(ListenError),
    /// The NOTIFY on the main core pool failed. A main URL that does not keep
    /// a transaction is a permanent failure, not retryable; a database that
    /// cannot be reached is retryable.
    #[error("the session check could not send its NOTIFY: {0}")]
    Notify(StatementFailure),
    /// The listener reconnected or stopped before the NOTIFY arrived, so the
    /// check proves nothing. Retryable.
    #[error("the listener {0} during the session check")]
    Interrupted(CheckInterruption),
}

impl SessionCheckFailure {
    /// Whether a later check can pass.
    pub fn is_retryable(&self) -> bool {
        match self {
            SessionCheckFailure::NotCapable { .. } | SessionCheckFailure::PoolSettings(_) => false,
            SessionCheckFailure::SessionPool(failure) | SessionCheckFailure::Notify(failure) => {
                is_retryable_statement_failure(failure)
            }
            SessionCheckFailure::Subscribe(refused) => refused.is_retryable(),
            SessionCheckFailure::Interrupted(_) => true,
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
    /// Starts the listener of `plan` ([`Listener::start`]). The connection
    /// opens at the first subscription.
    fn start(plan: ConnectPlan) -> StartedListener {
        let masked_url = plan.masked_url().clone();
        StartedListener {
            listener: Listener::start(Arc::new(plan)),
            masked_url,
        }
    }

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
    /// listener of the session URL, or loads the connection plan (blocking
    /// reads run off the runtime threads) and starts one. The connection
    /// opens at the first subscription.
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
/// the same socket settings.
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

/// The process listener of one key. The entry holds no strong reference, so
/// it does not keep its listener alive.
struct RegisteredListener {
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

/// The running listener of `key`, when one is registered.
fn find_running(
    registered: &[RegisteredListener],
    key: &ListenerKey,
) -> Option<Arc<StartedListener>> {
    registered
        .iter()
        .filter(|entry| entry.key.matches(key))
        .find_map(|entry| {
            entry
                .listener
                .upgrade()
                .filter(|started| started.listener.is_running())
        })
}

/// The listener of a broker on `key`. A refused plan starts nothing.
///
/// - The runtime of the process can be used: the shared listener of `key`
///   ([`shared_listener`]).
/// - It cannot: a listener of this broker alone. [`Listener::start`] meets
///   the same refusal and runs a task on the caller's runtime that refuses
///   every subscription with it. It is not registered, so no broker on
///   another runtime depends on the caller's runtime.
async fn process_listener(key: ListenerKey) -> Result<Arc<StartedListener>, ListenError> {
    match process_runtime() {
        Ok(_runtime) => shared_listener(key).await,
        Err(_refusal) => {
            let plan = load_plan(key).await.map_err(ListenError::PlanRefused)?;
            Ok(Arc::new(StartedListener::start(plan)))
        }
    }
}

/// The process listener of `key`, shared by every runtime of the process:
/// the running one, or a new one. Call it only when the runtime of the
/// process can be used, so the listener task runs there.
async fn shared_listener(key: ListenerKey) -> Result<Arc<StartedListener>, ListenError> {
    let running = {
        let registered = lock_process_listeners();
        find_running(&registered, &key)
    };
    if let Some(running) = running {
        return Ok(running);
    }
    let plan = load_plan(key.clone())
        .await
        .map_err(ListenError::PlanRefused)?;
    Ok(register(key, plan))
}

/// Starts and registers the listener of `plan`. When another broker of the
/// process registered a running listener on the same key while this plan
/// loaded, that listener is returned and `plan` is dropped unused.
fn register(key: ListenerKey, plan: ConnectPlan) -> Arc<StartedListener> {
    let mut registered = lock_process_listeners();
    registered.retain(|entry| entry.listener.strong_count() > 0);
    if let Some(running) = find_running(&registered, &key) {
        return running;
    }
    let started = Arc::new(StartedListener::start(plan));
    registered.push(RegisteredListener {
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
    use crate::db::{Keepalives, ListenerStartFailure, DEFAULT_SOCKET_SETTINGS};

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

    /// A runtime of a caller, as a test, a second `#[tokio::main]` or a
    /// blocking call has.
    fn caller_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a caller runtime starts")
    }

    /// #1495: brokers on one session URL share one listener from every
    /// runtime of the process. The listener runs on the runtime of the
    /// process, so it keeps running after the runtime of its first broker
    /// ends. Starting connects to nothing.
    #[test]
    fn every_runtime_of_the_process_shares_one_listener() {
        let url = format!("{PLAN_ONLY_URL}?application_name=two_runtimes");
        let in_first = ProcessListener::on_session_url(&url, DEFAULT_SOCKET_SETTINGS);
        let in_second = ProcessListener::on_session_url(&url, DEFAULT_SOCKET_SETTINGS);
        let first = caller_runtime();
        let first_started = first.block_on(in_first.start()).expect("the plan loads");
        drop(first);

        let second = caller_runtime();
        let second_started = second.block_on(in_second.start()).expect("the plan loads");

        assert!(std::ptr::eq(first_started, second_started));
        assert!(second_started.listener.is_running());
    }

    /// #1495: two runtimes of one process subscribe on one session URL, and
    /// the database sees one listener connection.
    #[test]
    #[serial_test::serial]
    fn two_runtimes_listen_on_one_connection() {
        let first = caller_runtime();
        let second = caller_runtime();
        let application_name = format!("process_listener_{}", uuid::Uuid::new_v4().simple());
        let (pool, url) = first.block_on(async {
            let url = crate::broker::terminalization_matrix::migrated_database_url().await;
            let pool = crate::broker::terminalization_matrix::migrated_pool().await;
            (pool, url)
        });
        let separator = match url.contains('?') {
            true => '&',
            false => '?',
        };
        let session_url = format!("{url}{separator}application_name={application_name}");
        let in_first = ProcessListener::on_session_url(&session_url, DEFAULT_SOCKET_SETTINGS);
        let in_second = ProcessListener::on_session_url(&session_url, DEFAULT_SOCKET_SETTINGS);
        let channel = fixed_channel("horsies_listener_two_runtimes");

        let from_first = first
            .block_on(in_first.subscribe_wake(channel.clone(), PayloadFilter::AnyPayload))
            .expect("the first runtime subscribes");
        let from_second = second
            .block_on(in_second.subscribe_wake(channel, PayloadFilter::AnyPayload))
            .expect("the second runtime subscribes");
        let connections: i64 = first
            .block_on(
                sqlx::query_scalar(
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1",
                )
                .bind(&application_name)
                .fetch_one(&pool),
            )
            .expect("count the listener connections");

        assert_eq!(connections, 1);
        drop((from_first, from_second, in_first, in_second));
        first.block_on(pool.close());
    }

    /// Python `tests/integration/test_tcp_user_timeout.py::test_both_listener_connections_carry_the_timeout`
    /// and `::test_the_monitoring_listener_carries_the_timeout`: the listener
    /// of a broker built from a config connects to the session URL with the
    /// socket settings of that config, a TCP user timeout that is not the
    /// default included. The monitoring event stream subscribes on this
    /// listener (`web::events`), and its plan sets the settings on the
    /// connection (`ConnectPlan::load`).
    #[tokio::test]
    async fn the_listener_of_a_broker_takes_the_socket_settings_of_its_config() {
        let session_url = "postgresql://horsies@127.0.0.1:2/horsies";
        let mut config = crate::core::config::PostgresConfig::from_url(
            "postgresql://horsies@127.0.0.1:1/horsies",
        );
        config.session_database_url = Some(session_url.to_owned());
        config.tcp_user_timeout = crate::core::config::TcpUserTimeoutConfig::After {
            milliseconds: crate::core::config::TcpUserTimeoutMs::try_from(12_345_u32)
                .expect("12345 ms is in bounds"),
        };

        let broker =
            crate::broker::PostgresBroker::connect_with(&config, crate::ClientFamily::Rust)
                .await
                .expect("a connect opens no connection, so it needs no server");

        let ListenerSource::SessionUrl {
            session_url: listened,
            socket,
        } = &broker.listener().source
        else {
            panic!("a broker built from a config has a listener on its session URL");
        };
        assert_eq!(listened.0, session_url);
        assert_eq!(*socket, config.socket_settings());
        assert_eq!(
            socket.tcp_user_timeout,
            crate::db::TcpUserTimeout::After(Duration::from_millis(12_345))
        );
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
        for prefix in [WORKER_PONG_CHANNEL_PREFIX, SESSION_CHECK_CHANNEL_PREFIX] {
            let generated = format!("{prefix}{}", uuid::Uuid::new_v4().simple());
            assert_eq!(fixed_channel(&generated).as_str(), generated);
        }
    }

    /// A URL that drops notifications is refused for good, with the masked
    /// URL and the fix; a check that the listener cut short is tried again.
    #[test]
    fn session_check_failures_follow_python_s_retry_rule() {
        let settings = crate::db::parse_connection_url(
            "postgresql://horsies:secret@pgbouncer:6432/horsies",
            &ConnectionEnvironment::isolated(),
        )
        .expect("a valid URL");
        let not_capable = SessionCheckFailure::NotCapable {
            session_url: settings.masked_url().clone(),
            waited: SESSION_CAPABILITY_TIMEOUT,
        };

        assert!(!not_capable.is_retryable());
        let message = not_capable.to_string();
        assert!(!message.contains("secret"), "{message}");
        assert!(message.contains("session_database_url"), "{message}");
        for interruption in [CheckInterruption::Reconnected, CheckInterruption::Stopped] {
            assert!(SessionCheckFailure::Interrupted(interruption).is_retryable());
        }
        let unreachable = |statement: &'static str| StatementFailure::NotSent {
            statement,
            cause: crate::db::NotSentCause::ConnectionClosed,
        };
        let statement_pooled = |statement: &'static str| StatementFailure::NotSent {
            statement,
            cause: crate::db::NotSentCause::Checkout(
                crate::db::CheckoutFailure::StatementPooledUrl {
                    masked_url: settings.masked_url().clone(),
                },
            ),
        };
        assert!(SessionCheckFailure::SessionPool(unreachable("session_check")).is_retryable());
        assert!(
            !SessionCheckFailure::SessionPool(statement_pooled("session_check")).is_retryable()
        );
        assert!(SessionCheckFailure::Notify(unreachable("session_check_notify")).is_retryable());
        assert!(
            !SessionCheckFailure::Notify(statement_pooled("session_check_notify")).is_retryable()
        );
        assert!(
            !SessionCheckFailure::PoolSettings(PoolSettingsError::Environment(
                EnvironmentError::NotUnicode { name: "PGPASSWORD" },
            ))
            .is_retryable()
        );
        assert!(!SessionCheckFailure::Subscribe(ListenError::NoSessionUrl).is_retryable());
    }
    /// A session URL whose server refuses every connection (port 1), of
    /// these tests only, so no other test shares its process listener.
    const REFUSED_URL: &str =
        "postgresql://horsies:pw@127.0.0.1:1/horsies?application_name=refused_connect_unit";

    /// The refusal of a subscription whose listener could not start: it may
    /// clear, and its cause is the failure that `is_cause` accepts.
    fn assert_start_refused(refused: &ListenError, is_cause: fn(&ListenerStartFailure) -> bool) {
        assert!(refused.is_retryable(), "{refused}");
        let ListenError::Subscribe(SubscribeError::StartFailed { cause }) = refused else {
            panic!("expected the failed start of the listener, got {refused}");
        };
        assert!(is_cause(cause), "{cause}");
    }

    fn a_failed_connect(failure: &ListenerStartFailure) -> bool {
        matches!(failure, ListenerStartFailure::Connect(_))
    }

    fn a_failed_listen(failure: &ListenerStartFailure) -> bool {
        matches!(
            failure,
            ListenerStartFailure::Listen(_) | ListenerStartFailure::ListenTimedOut { .. }
        )
    }

    /// Python `test_broker_listener.py::TestListenerResultSurface::test_start_returns_err_on_connect_failure`,
    /// `::test_start_err_preserves_original_exception`,
    /// `::test_listen_returns_err_on_connect_failure` and
    /// `::test_listen_many_returns_err_on_connect_failure`: a listener whose
    /// server refuses the connection refuses its first subscriptions, of a
    /// wake and of the payloads of two channels, with the failed connect
    /// kept as the cause; the refusal may clear. (Python names the channel
    /// in the message of `listen`; the core's refusal is the start of the
    /// one listener of every channel.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_listener_that_cannot_connect_refuses_its_first_subscriptions_with_the_cause() {
        let listener = ProcessListener::on_session_url(REFUSED_URL, DEFAULT_SOCKET_SETTINGS);

        let Err(refused) = listener
            .subscribe_wake(any_channel(), PayloadFilter::AnyPayload)
            .await
        else {
            panic!("a listener that cannot connect must refuse the wake subscription");
        };
        assert_start_refused(&refused, a_failed_connect);
        for channel in ["horsies_listener_unit_a", "horsies_listener_unit_b"] {
            let Err(refused) = listener
                .subscribe_payloads(fixed_channel(channel), default_payload_capacity())
                .await
            else {
                panic!("a listener that cannot connect must refuse the payloads of {channel}");
            };
            assert_start_refused(&refused, a_failed_connect);
        }
    }

    /// Python `TestListenerResultSurface::test_listen_returns_err_when_listen_sql_fails`:
    /// the connection opens but its LISTEN fails (the proxy drops the reply
    /// to the LISTEN and closes the connection). The first subscription is
    /// refused with the failed LISTEN as its cause, and the refusal may
    /// clear.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial]
    async fn a_listener_whose_listen_fails_refuses_its_first_subscription_with_the_cause() {
        use crate::broker::lifecycle_tests::support::WireProxy;

        let url = crate::broker::terminalization_matrix::migrated_database_url().await;
        let upstream: sqlx::postgres::PgConnectOptions =
            url.parse().expect("the test database URL parses");
        let proxy = WireProxy::start(&upstream).await;
        let proxied_url = crate::broker::pools::test_url_of(&proxy.options(&upstream));
        proxy.drop_reply_to_request_carrying(b"LISTEN");
        let listener = ProcessListener::on_session_url(&proxied_url, DEFAULT_SOCKET_SETTINGS);

        let Err(refused) = listener
            .subscribe_wake(any_channel(), PayloadFilter::AnyPayload)
            .await
        else {
            panic!("a listener whose LISTEN fails must refuse the subscription");
        };

        assert!(
            proxy.reply_was_dropped(),
            "the proxy relayed the LISTEN reply"
        );
        assert_start_refused(&refused, a_failed_listen);
    }

    /// Python `TestCallerUnwrapContract::test_broker_ensure_initialized_does_not_start_listener`:
    /// the schema install of a broker does not start its listener.
    #[tokio::test]
    #[serial_test::serial]
    async fn the_schema_install_does_not_start_the_listener() {
        let url = crate::broker::terminalization_matrix::migrated_database_url().await;
        let broker = crate::broker::PostgresBroker::connect(&url, crate::ClientFamily::Rust)
            .await
            .expect("connect a broker");

        broker
            .ensure_schema_initialized()
            .await
            .expect("the schema is installed");

        assert!(!broker.listener().started.initialized());
    }
}
