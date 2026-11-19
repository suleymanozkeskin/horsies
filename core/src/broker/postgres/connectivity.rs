//! The connectivity step of a client's live check (`check --live`; Python
//! `Horsies._check_broker_connectivity`, `python/horsies/core/app.py`), and
//! the LISTEN probe that it shares with the worker boot's session check.
//!
//! Both clients call [`PostgresBroker::check_connectivity`], so the rule has
//! one implementation. Nothing is written: the steps read, and the probe
//! sends one NOTIFY.

use std::time::Duration;

use uuid::Uuid;

use super::{PostgresBroker, HEALTH_CHECK, SESSION_CHECK, SESSION_CHECK_NOTIFY};
use crate::broker::error::is_retryable_statement_failure;
use crate::broker::pools::{OpenPools, PoolSettingsError, SessionPool};
use crate::broker::process_listener::{
    fixed_channel, CheckInterruption, ListenError, ProcessListener, SessionCheckFailure,
    SESSION_CAPABILITY_TIMEOUT, SESSION_CHECK_CHANNEL_PREFIX,
};
use crate::db::{
    CheckoutClass, MaskedUrl, PayloadFilter, StatementBound, StatementFailure, WakeEvent,
};

/// Why the LISTEN probe did not prove that the session URL delivers a
/// NOTIFY. The probe's subscription is dropped; nothing else changed.
#[derive(Debug, thiserror::Error)]
pub enum ListenProbeFailure {
    /// LISTEN was accepted, but the NOTIFY did not arrive within the bound:
    /// the session URL does not keep session state (for example PgBouncer
    /// transaction pooling). Not retryable: the same URL fails again.
    #[error(
        "LISTEN on {session_url} received no NOTIFY within {} ms: the session URL does not \
         keep session state (for example PgBouncer transaction pooling)",
        .waited.as_millis()
    )]
    NotDelivered {
        session_url: MaskedUrl,
        waited: Duration,
    },
    /// The probe could not LISTEN on the session URL. Retryable when the
    /// refusal is transient (a database that cannot be reached).
    #[error("the probe could not LISTEN on the session URL: {0}")]
    Subscribe(ListenError),
    /// The NOTIFY on the main core pool failed.
    #[error("the probe could not send its NOTIFY: {0}")]
    Notify(StatementFailure),
    /// The listener reconnected or stopped before the NOTIFY arrived, so the
    /// probe proves nothing. Retryable.
    #[error("the listener {0} during the probe")]
    Interrupted(CheckInterruption),
}

impl ListenProbeFailure {
    /// Whether a later probe can pass.
    pub fn is_retryable(&self) -> bool {
        match self {
            ListenProbeFailure::NotDelivered { .. } => false,
            ListenProbeFailure::Subscribe(refused) => refused.is_retryable(),
            ListenProbeFailure::Notify(failure) => is_retryable_statement_failure(failure),
            ListenProbeFailure::Interrupted(_) => true,
        }
    }
}

/// The session check of a worker boot fails with the probe's cause, in the
/// variants it had before the probe was shared.
impl From<ListenProbeFailure> for SessionCheckFailure {
    fn from(failure: ListenProbeFailure) -> Self {
        match failure {
            ListenProbeFailure::NotDelivered {
                session_url,
                waited,
            } => SessionCheckFailure::NotCapable {
                session_url,
                waited,
            },
            ListenProbeFailure::Subscribe(refused) => SessionCheckFailure::Subscribe(refused),
            ListenProbeFailure::Notify(failure) => SessionCheckFailure::Notify(failure),
            ListenProbeFailure::Interrupted(interruption) => {
                SessionCheckFailure::Interrupted(interruption)
            }
        }
    }
}

/// Why the connectivity step of a live check refused. The first step that
/// failed; no later step ran. Nothing was written.
///
/// Python's live check answers `ListenerNotCapable` with "broker listener
/// capability check failed" and every other variant with "broker
/// connectivity check failed", both `HRS-203` (`BROKER_INVALID_URL`).
#[derive(Debug, thiserror::Error)]
pub enum ConnectivityRefusal {
    /// The core pools could not open with the broker's settings (a URL, a
    /// TLS file, `~/.pgpass`, a pool size). Not retryable.
    #[error("the core pools could not open: {0}")]
    PoolSettings(PoolSettingsError),
    /// `SELECT 1` on the main URL's pool failed. Its first checkout runs the
    /// statement-pooling check of the pool.
    #[error("SELECT 1 on database_url failed: {0}")]
    MainUnreachable(StatementFailure),
    /// `SELECT 1` on the session URL's own pool failed (a session URL that
    /// differs from the main URL).
    #[error("SELECT 1 on session_database_url failed: {0}")]
    SessionUnreachable(StatementFailure),
    /// PgBouncer transaction mode: the session URL did not deliver a NOTIFY
    /// to a LISTEN, or the probe could not run.
    #[error("{0}")]
    ListenerNotCapable(ListenProbeFailure),
}

impl ConnectivityRefusal {
    /// Whether a later check can pass.
    pub fn is_retryable(&self) -> bool {
        match self {
            ConnectivityRefusal::PoolSettings(_) => false,
            ConnectivityRefusal::MainUnreachable(failure)
            | ConnectivityRefusal::SessionUnreachable(failure) => {
                is_retryable_statement_failure(failure)
            }
            ConnectivityRefusal::ListenerNotCapable(failure) => failure.is_retryable(),
        }
    }
}

impl PostgresBroker {
    /// The connectivity step of a live check, for both clients (Python
    /// `_check_broker_connectivity`). In order, stopping at the first
    /// failure:
    ///
    /// 1. `SELECT 1` on the main core pool.
    /// 2. `SELECT 1` on the session core pool, when the session URL differs
    ///    from the main URL (a session URL equal to the main URL was checked
    ///    by step 1).
    /// 3. In PgBouncer transaction mode, the LISTEN probe on the session URL
    ///    ([`Self::probe_listen_delivery`]). It runs on a listener of the
    ///    session URL from the broker's config, so a broker built without a
    ///    listener (`AssumeInitialized`) is probed too, as Python's check
    ///    opens its own connections.
    ///
    /// The first checkout of each pool runs its statement-pooling check. No
    /// schema step runs. Reads only, plus the probe's one NOTIFY.
    ///
    /// Errors: [`ConnectivityRefusal`].
    pub async fn check_connectivity(&self) -> Result<(), ConnectivityRefusal> {
        let open = self
            .core
            .pools()
            .map_err(ConnectivityRefusal::PoolSettings)?;
        self.log_statement(&HEALTH_CHECK);
        let _selected = open
            .main()
            .execute(
                CheckoutClass::Batch,
                &HEALTH_CHECK.bind().finish(),
                StatementBound::ConnectionTimeouts,
            )
            .await
            .map_err(ConnectivityRefusal::MainUnreachable)?;
        match open.pools().session() {
            SessionPool::SameAsMain => {}
            SessionPool::Separate { pool, .. } => {
                self.log_statement(&SESSION_CHECK);
                let _selected = pool
                    .execute(
                        CheckoutClass::Batch,
                        &SESSION_CHECK.bind().finish(),
                        StatementBound::ConnectionTimeouts,
                    )
                    .await
                    .map_err(ConnectivityRefusal::SessionUnreachable)?;
            }
        }
        match self.pgbouncer_transaction_mode {
            false => Ok(()),
            true => {
                let config = self.core.config();
                let probe = ProcessListener::on_session_url(
                    config.effective_session_database_url(),
                    config.socket_settings(),
                );
                self.probe_listen_delivery(&probe, &open)
                    .await
                    .map_err(ConnectivityRefusal::ListenerNotCapable)
            }
        }
    }

    /// The LISTEN probe: whether a LISTEN on `listener` receives a NOTIFY
    /// (Python `probe_session_capability`).
    ///
    /// 1. Subscribe a wake on `horsies_check_<32 hex>` on `listener`.
    /// 2. One autocommit `SELECT pg_notify($1, 'ok')` on the main core pool
    ///    of `open`.
    /// 3. Wait at most [`SESSION_CAPABILITY_TIMEOUT`] (2 s) for the NOTIFY.
    ///
    /// The subscription is dropped at the end, and the listener UNLISTENs
    /// the channel. Nothing is written.
    ///
    /// Errors: [`ListenProbeFailure`].
    pub(super) async fn probe_listen_delivery(
        &self,
        listener: &ProcessListener,
        open: &OpenPools,
    ) -> Result<(), ListenProbeFailure> {
        let started = listener
            .start()
            .await
            .map_err(ListenProbeFailure::Subscribe)?;
        let channel = fixed_channel(&format!(
            "{SESSION_CHECK_CHANNEL_PREFIX}{}",
            Uuid::new_v4().simple()
        ));
        let delivery = started
            .subscribe_wake(channel.clone(), PayloadFilter::AnyPayload)
            .await
            .map_err(ListenProbeFailure::Subscribe)?;
        let channel_name: &str = channel.as_str();
        self.log_statement(&SESSION_CHECK_NOTIFY);
        let _notified = open
            .main()
            .execute(
                CheckoutClass::Batch,
                &SESSION_CHECK_NOTIFY.bind().value(&channel_name).finish(),
                StatementBound::ConnectionTimeouts,
            )
            .await
            .map_err(ListenProbeFailure::Notify)?;
        match tokio::time::timeout(SESSION_CAPABILITY_TIMEOUT, delivery.recv()).await {
            Ok(WakeEvent::Notified) => Ok(()),
            Ok(WakeEvent::Gap) => Err(ListenProbeFailure::Interrupted(
                CheckInterruption::Reconnected,
            )),
            Ok(WakeEvent::Stopped) => {
                Err(ListenProbeFailure::Interrupted(CheckInterruption::Stopped))
            }
            Err(_elapsed) => Err(ListenProbeFailure::NotDelivered {
                session_url: started.masked_url().clone(),
                waited: SESSION_CAPABILITY_TIMEOUT,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::PostgresConfig;
    use crate::test_support::TestDatabase;

    /// A URL with nothing listening on its port.
    const UNREACHABLE_URL: &str = "postgresql://user:secret@127.0.0.1:1/none";

    /// A config of `database_url` (and `session_url`, when given) with
    /// small pools.
    fn config(
        database_url: &str,
        session_url: Option<&str>,
        transaction_mode: bool,
    ) -> PostgresConfig {
        let mut config = PostgresConfig::from_url(database_url);
        config.session_database_url = session_url.map(str::to_owned);
        config.pgbouncer_transaction_mode = transaction_mode;
        config.pool_size = 2;
        config.max_overflow = 0;
        config
    }

    /// `url` with `application_name` added: the same database under
    /// another URL, which transaction mode needs for its session URL.
    fn with_application_name(url: &str, application_name: &str) -> String {
        let separator = if url.contains('?') { '&' } else { '?' };
        format!("{url}{separator}application_name={application_name}")
    }

    async fn connect(config: &PostgresConfig) -> PostgresBroker {
        PostgresBroker::connect_with(config, crate::ClientFamily::Rust)
            .await
            .expect("building a broker opens no connection")
    }

    /// Python `_test_all_connections` with no session URL and no
    /// transaction mode: the main URL answers, and nothing else runs.
    #[tokio::test]
    async fn a_reachable_main_url_passes() {
        let database = TestDatabase::create().await;
        let broker = connect(&config(database.url(), None, false)).await;
        broker
            .check_connectivity()
            .await
            .expect("the main URL answers SELECT 1");
        broker.close().await;
        drop(broker);
        database.drop().await;
    }

    /// The main URL is checked first: a main URL that cannot be reached is
    /// `MainUnreachable`, retryable.
    #[tokio::test]
    async fn an_unreachable_main_url_is_refused_first() {
        let broker = connect(&config(UNREACHABLE_URL, None, false)).await;
        let refused = broker
            .check_connectivity()
            .await
            .expect_err("nothing listens on port 1");
        assert!(
            matches!(refused, ConnectivityRefusal::MainUnreachable(_)),
            "{refused:?}"
        );
        assert!(refused.is_retryable(), "{refused}");
    }

    /// Python's second `_test_connection`: a session URL apart from the main
    /// URL is checked on its own; one that cannot be reached is
    /// `SessionUnreachable`.
    #[tokio::test]
    async fn an_unreachable_session_url_is_refused_after_the_main_url() {
        let database = TestDatabase::create().await;
        let broker = connect(&config(database.url(), Some(UNREACHABLE_URL), false)).await;
        let refused = broker
            .check_connectivity()
            .await
            .expect_err("nothing listens on the session URL's port");
        assert!(
            matches!(refused, ConnectivityRefusal::SessionUnreachable(_)),
            "{refused:?}"
        );
        broker.close().await;
        drop(broker);
        database.drop().await;
    }

    /// Python `_run_listener_probe` in transaction mode: with both URLs on
    /// one database the NOTIFY reaches the LISTEN, and the check passes.
    #[tokio::test]
    async fn transaction_mode_with_a_session_capable_url_passes_the_probe() {
        let database = TestDatabase::create().await;
        let session_url = with_application_name(database.url(), "connectivity_probe");
        let broker = connect(&config(database.url(), Some(&session_url), true)).await;
        broker
            .check_connectivity()
            .await
            .expect("the session URL delivers the NOTIFY");
        broker.close().await;
        drop(broker);
        database.drop().await;
    }

    /// The probe does not need the broker's own listener: a broker built
    /// without one (`AssumeInitialized`) is probed on a listener of its
    /// session URL, as Python's check opens its own connections.
    #[tokio::test]
    async fn a_broker_without_a_listener_is_probed_on_its_session_url() {
        let database = TestDatabase::create().await;
        let session_url = with_application_name(database.url(), "connectivity_no_listener");
        let broker = connect(&config(database.url(), Some(&session_url), true))
            .await
            .without_listener();
        broker
            .check_connectivity()
            .await
            .expect("the probe listens on the session URL of the config");
        broker.close().await;
        drop(broker);
        database.drop().await;
    }

    /// Python's listener refusal: a session URL whose LISTEN never sees the
    /// NOTIFY of the main URL (here another database) is
    /// `ListenerNotCapable` with `NotDelivered`, not retryable.
    #[tokio::test]
    async fn a_session_url_that_does_not_deliver_is_refused_as_not_capable() {
        let main = TestDatabase::create().await;
        let session = TestDatabase::create_empty().await;
        let broker = connect(&config(main.url(), Some(session.url()), true)).await;
        let refused = broker
            .check_connectivity()
            .await
            .expect_err("the NOTIFY on the main URL cannot reach the session URL's LISTEN");
        assert!(
            matches!(
                refused,
                ConnectivityRefusal::ListenerNotCapable(ListenProbeFailure::NotDelivered { .. })
            ),
            "{refused:?}"
        );
        assert!(!refused.is_retryable(), "{refused}");
        broker.close().await;
        drop(broker);
        main.drop().await;
        session.drop().await;
    }

    /// The worker boot's session check keeps its variants: each probe
    /// failure maps to the one it had before the probe was shared.
    #[test]
    fn a_probe_failure_keeps_the_session_check_variant() {
        let interrupted =
            SessionCheckFailure::from(ListenProbeFailure::Interrupted(CheckInterruption::Stopped));
        assert!(matches!(
            interrupted,
            SessionCheckFailure::Interrupted(CheckInterruption::Stopped)
        ));
        let subscribe =
            SessionCheckFailure::from(ListenProbeFailure::Subscribe(ListenError::NoSessionUrl));
        assert!(matches!(
            subscribe,
            SessionCheckFailure::Subscribe(ListenError::NoSessionUrl)
        ));
        assert!(!subscribe.is_retryable());
    }
}
