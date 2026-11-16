//! The broker's listener: one [`crate::db::Listener`] per broker, on the
//! session URL. Every LISTEN of the broker and of the modules that hold the
//! broker subscribes here; none of them opens a listener connection of its
//! own.
//!
//! - The listener starts at the first subscription. The session URL is
//!   parsed and its connection plan is loaded then, not when the broker is
//!   built, so building a broker reads no file.
//! - Every broker built from a config has a listener, observe-only brokers
//!   included (Python's web tool runs with migrations off and keeps its
//!   listener). Only a broker built from a pool with no session URL has none:
//!   every subscription is refused with [`ListenError::NoSessionUrl`].
//! - A refused plan is not kept: the next subscription loads it again.

use std::fmt;
use std::sync::Arc;

use tokio::sync::OnceCell;

use crate::db::{
    parse_connection_url, ChannelName, ConnectPlan, ConnectPlanError, ConnectionEnvironment,
    EnvironmentError, Listener, MaskedUrl, PayloadFilter, PayloadSubscription, QueueCapacity,
    SubscribeError, Transience, UrlError, WakeSubscription, DEFAULT_PAYLOAD_QUEUE_CAPACITY,
    DEFAULT_SOCKET_SETTINGS,
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
    SessionUrl(SessionUrl),
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

/// The listener of one broker. It holds no connection until the first
/// subscription. Dropping it stops the listener: every subscription then
/// reads `Stopped`.
pub struct ProcessListener {
    source: ListenerSource,
    started: OnceCell<StartedListener>,
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
    /// A listener on `session_url`. Nothing is parsed or read here.
    pub(crate) fn on_session_url(session_url: &str) -> ProcessListener {
        ProcessListener {
            source: ListenerSource::SessionUrl(SessionUrl(session_url.to_owned())),
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

    /// The started listener. The first call loads the connection plan of the
    /// session URL (blocking reads run off the runtime threads) and starts
    /// the listener task; the connection opens at the first subscription.
    pub async fn start(&self) -> Result<&StartedListener, ListenError> {
        match &self.source {
            ListenerSource::NoSessionUrl => Err(ListenError::NoSessionUrl),
            ListenerSource::SessionUrl(session_url) => {
                self.started
                    .get_or_try_init(|| start_listener(session_url.clone()))
                    .await
            }
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

async fn start_listener(session_url: SessionUrl) -> Result<StartedListener, ListenError> {
    let plan = load_plan(session_url)
        .await
        .map_err(ListenError::PlanRefused)?;
    let masked_url = plan.masked_url().clone();
    Ok(StartedListener {
        listener: Listener::start(Arc::new(plan)),
        masked_url,
    })
}

/// Runs [`read_plan`] on the blocking pool. A panic in it is resumed here.
async fn load_plan(session_url: SessionUrl) -> Result<ConnectPlan, ListenerPlanError> {
    match tokio::task::spawn_blocking(move || read_plan(&session_url)).await {
        Ok(loaded) => loaded,
        Err(join_error) => match join_error.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(_cancelled) => Err(ListenerPlanError::LoadCancelled),
        },
    }
}

/// Reads the connection environment, parses the session URL and loads its
/// plan. Blocking: reads the TLS files the URL names, `~/.pgpass` when no
/// other password is given, and the process user name when the URL names no
/// user.
fn read_plan(session_url: &SessionUrl) -> Result<ConnectPlan, ListenerPlanError> {
    let environment = ConnectionEnvironment::capture().map_err(ListenerPlanError::Environment)?;
    let settings =
        parse_connection_url(&session_url.0, &environment).map_err(ListenerPlanError::Url)?;
    ConnectPlan::load(&settings, &environment, &DEFAULT_SOCKET_SETTINGS)
        .map_err(ListenerPlanError::Plan)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let listener = ProcessListener::on_session_url("mysql://horsies@127.0.0.1:1/horsies");

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

    #[test]
    fn debug_output_does_not_show_the_session_url() {
        let listener = ProcessListener::on_session_url("postgresql://horsies:secret@db/horsies");

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
