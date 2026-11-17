//! The lazy handle of a client process: a producer manifest, the pool
//! profile of the process and the client family, and the broker that the
//! first operation connects.
//!
//! Building a handle starts nothing: no runtime, no thread, no connection.
//! The first operation connects the broker; a failed connect is not kept,
//! so the next operation connects again. The schema is installed once per
//! handle, before its first operation that needs it, unless the manifest's
//! schema mode is `ObserveOnly` or `AssumeInitialized`. The broker checks
//! the client family of the database before its first write.
//!
//! The Python bridge (`CoreHandle`, `CoreChildHandle`) and the Rust client
//! (`Horsies`) each hold one.

use std::sync::Arc;

use tokio::sync::OnceCell;

use crate::broker::client_family::ClientFamily;
use crate::broker::{BrokerError, PoolProfile, PostgresBroker, SchemaInitializationMode};
use crate::manifest::{ProducerManifest, SchemaMode};

/// Whether a handle holds a connected broker.
pub enum HandleConnection {
    /// No connect succeeded: the handle holds no pool.
    NeverConnected,
    Connected(Arc<PostgresBroker>),
}

/// Whether the broker of a handle has a process listener. The schema mode of
/// the manifest decides it, so it is known before any connect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleListener {
    /// The broker listens on its session connection, which opens at the
    /// first subscription.
    Process,
    /// The broker has no listener (`AssumeInitialized`, Python's
    /// `assume_initialized=True`): a result wait polls, and an operation that
    /// needs the listener answers `LISTENER_DISABLED`.
    Disabled,
}

impl HandleListener {
    /// The listener of a handle of schema mode `mode`. Pure.
    pub const fn of(mode: SchemaMode) -> Self {
        match mode {
            SchemaMode::AssumeInitialized => Self::Disabled,
            SchemaMode::MigrateAndValidate | SchemaMode::ObserveOnly => Self::Process,
        }
    }
}

/// How [`CoreHandle::close`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleClose {
    /// The handle never connected: nothing was started, nothing closed.
    NeverConnected,
    /// The broker's pools closed; writes in flight ran to their end within
    /// the broker's close bound.
    Closed,
}

/// The lazy handle of one producer manifest, one pool profile and one
/// client family.
pub struct CoreHandle {
    manifest: ProducerManifest,
    profile: PoolProfile,
    family: ClientFamily,
    broker: OnceCell<Arc<PostgresBroker>>,
}

impl CoreHandle {
    /// A handle of `manifest` with the pools of `profile`, writing as
    /// `family`. Starts nothing: no runtime, no connection.
    pub fn new(manifest: ProducerManifest, profile: PoolProfile, family: ClientFamily) -> Self {
        Self {
            manifest,
            profile,
            family,
            broker: OnceCell::new(),
        }
    }

    /// The manifest the handle was built from.
    pub fn manifest(&self) -> &ProducerManifest {
        &self.manifest
    }

    /// The pool profile of the handle's process.
    pub fn profile(&self) -> PoolProfile {
        self.profile
    }

    /// The client family the handle writes as.
    pub fn family(&self) -> ClientFamily {
        self.family
    }

    /// The schema mode of the handle's manifest.
    pub fn schema_mode(&self) -> SchemaMode {
        self.manifest.schema
    }

    /// Whether the handle's broker has a process listener. Starts nothing.
    pub fn listener(&self) -> HandleListener {
        HandleListener::of(self.manifest.schema)
    }

    /// The connected broker. Connects at the first call; a failure is
    /// returned and not kept, so the next call connects again.
    ///
    /// Errors: the [`BrokerError`] of the connect.
    pub async fn broker(&self) -> Result<Arc<PostgresBroker>, BrokerError> {
        self.broker
            .get_or_try_init(|| connect(&self.manifest, self.profile, self.family))
            .await
            .map(Arc::clone)
    }

    /// The connected broker, with the schema installed once per handle
    /// first ([`PostgresBroker::ensure_schema_initialized`]: nothing in an
    /// `ObserveOnly` or `AssumeInitialized` handle), as Python's broker
    /// installs at its first enqueue or read. A failure of the connect or of
    /// the install is returned and not kept, so the next call runs it again.
    ///
    /// Errors: the [`BrokerError`] of the connect or of the install.
    pub async fn initialized_broker(&self) -> Result<Arc<PostgresBroker>, BrokerError> {
        let broker = self.broker().await?;
        broker.ensure_schema_initialized().await?;
        Ok(broker)
    }

    /// Whether a connect succeeded on this handle. Starts nothing and waits
    /// for nothing: a connect still in flight is `NeverConnected`.
    pub fn connection(&self) -> HandleConnection {
        match self.broker.get() {
            Some(broker) => HandleConnection::Connected(Arc::clone(broker)),
            None => HandleConnection::NeverConnected,
        }
    }

    /// Close the broker's pools when the handle connected; starts nothing
    /// when it did not. No new operation takes the closed pools; writes in
    /// flight run to their end within the close bound, and the broker logs
    /// the count of those that did not. The next operation on the handle
    /// opens new pools.
    pub async fn close(&self) -> HandleClose {
        match self.connection() {
            HandleConnection::NeverConnected => HandleClose::NeverConnected,
            HandleConnection::Connected(broker) => {
                broker.close().await;
                HandleClose::Closed
            }
        }
    }
}

/// Connect the broker of a manifest. `AssumeInitialized` runs no schema
/// step, as `ObserveOnly`, and has no listener (Python's
/// `assume_initialized=True`): a result wait polls.
///
/// Errors: the [`BrokerError`] of the connect.
async fn connect(
    manifest: &ProducerManifest,
    profile: PoolProfile,
    family: ClientFamily,
) -> Result<Arc<PostgresBroker>, BrokerError> {
    let config = manifest
        .database
        .to_postgres_config(manifest.send.retain_rerun_input_default);
    let mode = match manifest.schema {
        SchemaMode::MigrateAndValidate => SchemaInitializationMode::MigrateAndValidate,
        SchemaMode::ObserveOnly | SchemaMode::AssumeInitialized => {
            SchemaInitializationMode::ObserveOnly
        }
    };
    let window = manifest.send.idempotency_reservation_window.as_duration();
    let broker = PostgresBroker::connect_with_policies(&config, mode, window, profile)
        .await?
        .with_client_family(family);
    Ok(Arc::new(match HandleListener::of(manifest.schema) {
        HandleListener::Disabled => broker.without_listener(),
        HandleListener::Process => broker,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::parse_producer_manifest;

    const MANIFEST: &str = r#"{
        "manifest_version": 1,
        "database": {
            "database_url": "postgresql://user:secret@127.0.0.1:1/none",
            "session_database": {"kind": "SAME_AS_DATABASE_URL"},
            "pgbouncer_transaction_mode": false,
            "pool_pre_ping": true,
            "pool_size": 5,
            "max_overflow": 0,
            "pool_timeout": 30,
            "pool_recycle": {"kind": "AFTER_SECONDS", "seconds": 1800},
            "echo": false,
            "tcp_keepalives": {
                "kind": "ON",
                "idle_seconds": 30,
                "interval_seconds": 10,
                "probes": 3
            },
            "tcp_user_timeout": {"kind": "AFTER", "milliseconds": 30000}
        },
        "schema": "OBSERVE_ONLY",
        "send": {
            "retain_rerun_input_default": false,
            "idempotency_reservation_window": 86400000000
        }
    }"#;

    fn handle(family: ClientFamily) -> CoreHandle {
        let manifest = parse_producer_manifest(MANIFEST).expect("the manifest is complete");
        CoreHandle::new(manifest, PoolProfile::Producer, family)
    }

    /// Building a handle starts nothing; it keeps what it was built with.
    #[test]
    fn a_new_handle_keeps_its_inputs_and_starts_nothing() {
        let rust = handle(ClientFamily::Rust);
        assert_eq!(rust.profile(), PoolProfile::Producer);
        assert_eq!(rust.family(), ClientFamily::Rust);
        assert_eq!(rust.schema_mode(), SchemaMode::ObserveOnly);
        assert_eq!(rust.manifest().manifest_version.get(), 1);
        assert!(matches!(
            rust.connection(),
            HandleConnection::NeverConnected
        ));
        assert_eq!(handle(ClientFamily::Python).family(), ClientFamily::Python);
    }

    /// Only an `AssumeInitialized` handle has no listener; the rule needs no
    /// connect.
    #[test]
    fn only_an_assume_initialized_handle_has_no_listener() {
        assert_eq!(
            HandleListener::of(SchemaMode::AssumeInitialized),
            HandleListener::Disabled
        );
        assert_eq!(
            HandleListener::of(SchemaMode::ObserveOnly),
            HandleListener::Process
        );
        assert_eq!(
            HandleListener::of(SchemaMode::MigrateAndValidate),
            HandleListener::Process
        );
        assert_eq!(
            handle(ClientFamily::Python).listener(),
            HandleListener::Process
        );
    }

    /// A handle that never connected closes without a connect.
    #[tokio::test]
    async fn a_handle_that_never_connected_closes_without_a_connect() {
        let rust = handle(ClientFamily::Rust);
        assert_eq!(rust.close().await, HandleClose::NeverConnected);
        assert!(matches!(
            rust.connection(),
            HandleConnection::NeverConnected
        ));
    }

    /// The broker's pools open lazily, so the first call builds the broker
    /// with no database; the handle keeps it and gives the same broker to
    /// every later call.
    #[tokio::test]
    async fn the_first_connect_is_kept() {
        let rust = handle(ClientFamily::Rust);
        let first = rust.broker().await.expect("the pools open lazily");
        let second = rust.broker().await.expect("the kept broker");
        assert!(Arc::ptr_eq(&first, &second));
        assert!(matches!(
            rust.connection(),
            HandleConnection::Connected(kept) if Arc::ptr_eq(&kept, &first)
        ));
    }
}
