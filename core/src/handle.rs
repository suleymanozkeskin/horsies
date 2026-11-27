//! The lazy core connection of a client process: a producer manifest, the
//! pool profile of the process and the client family, and the broker that
//! the first operation connects.
//!
//! Building a handle starts nothing: no runtime, no thread, no connection.
//! The first operation connects the broker; a failed connect is not kept,
//! so the next operation connects again. The schema is installed once per
//! handle, before its first operation that needs it, unless the manifest's
//! schema mode is `ObserveOnly` or `AssumeInitialized`. The install ends with
//! the startup coverage pass of the handle's coverage request
//! ([`CoreConnection::with_install_coverage`]). The broker checks the client
//! family of the database before its first write.
//!
//! The Python bridge (its classes `CoreHandle`, `CoreChildHandle`) and the
//! Rust client (`Horsies`) each hold one.

use std::sync::Arc;

use tokio::sync::OnceCell;

use crate::broker::client_family::ClientFamily;
use crate::broker::partition_maintenance::default_install_coverage;
use crate::broker::{BrokerError, PoolProfile, PostgresBroker, SchemaInitializationMode};
use crate::core::history::maintenance::coverage::CoverageRequest;
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
    /// `SchemaMode.ASSUME_INITIALIZED`): a result wait polls, and an operation that
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

/// How [`CoreConnection::close`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleClose {
    /// The handle never connected: nothing was started, nothing closed.
    NeverConnected,
    /// The broker's pools closed; writes in flight ran to their end within
    /// the broker's close bound.
    Closed,
}

/// The lazy core connection of one producer manifest, one pool profile and
/// one client family. One or more apps can share it.
pub struct CoreConnection {
    manifest: ProducerManifest,
    pool_profile: PoolProfile,
    family: ClientFamily,
    /// The coverage pass of the schema install of the handle's broker.
    install_coverage: CoverageRequest,
    broker: OnceCell<Arc<PostgresBroker>>,
}

impl CoreConnection {
    /// A connection of `manifest` with the pools of `pool_profile`, writing
    /// as `family`, whose install covers the classes of the default
    /// retention config. Starts nothing: no runtime, no connection.
    pub fn new(
        manifest: ProducerManifest,
        pool_profile: PoolProfile,
        family: ClientFamily,
    ) -> Self {
        Self {
            manifest,
            pool_profile,
            family,
            install_coverage: default_install_coverage(),
            broker: OnceCell::new(),
        }
    }

    /// The same handle, whose schema install ends with the coverage pass of
    /// `request` ([`PostgresBroker::with_install_coverage`]): the request of
    /// the client's retention config, so the install covers every class
    /// that the client's sends can stamp. Starts nothing.
    pub fn with_install_coverage(mut self, request: CoverageRequest) -> Self {
        self.install_coverage = request;
        self
    }

    /// The coverage request of the schema install of the handle.
    pub fn install_coverage(&self) -> &CoverageRequest {
        &self.install_coverage
    }

    /// The manifest the handle was built from.
    pub fn manifest(&self) -> &ProducerManifest {
        &self.manifest
    }

    /// The pool profile of the handle's process.
    pub fn pool_profile(&self) -> PoolProfile {
        self.pool_profile
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
            .get_or_try_init(|| {
                connect(
                    &self.manifest,
                    self.pool_profile,
                    self.family,
                    &self.install_coverage,
                )
            })
            .await
            .map(Arc::clone)
    }

    /// The connected broker, with the schema installed once per handle
    /// first ([`PostgresBroker::ensure_schema_initialized`]: the migrations
    /// and the coverage pass of the handle's request; nothing in an
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

/// Connect the broker of a manifest, whose install ends with the coverage
/// pass of `install_coverage`. `AssumeInitialized` runs no schema step, as
/// `ObserveOnly`, and has no listener (Python's `SchemaMode.ASSUME_INITIALIZED`):
/// a result wait polls.
///
/// Errors: the [`BrokerError`] of the connect.
async fn connect(
    manifest: &ProducerManifest,
    profile: PoolProfile,
    family: ClientFamily,
    install_coverage: &CoverageRequest,
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
    let broker = PostgresBroker::connect_with_policies(&config, mode, window, profile, family)
        .await?
        .with_install_coverage(install_coverage.clone());
    Ok(Arc::new(match HandleListener::of(manifest.schema) {
        HandleListener::Disabled => broker.without_listener(),
        HandleListener::Process => broker,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::parse_producer_manifest;
    use crate::test_support::TestDatabase;

    const MANIFEST: &str = r#"{
        "manifest_version": 1,
        "database": {
            "database_url": "postgresql://user:secret@127.0.0.1:1/none",
            "session_database": {"kind": "SAME_AS_DATABASE_URL"},
            "pgbouncer_transaction_mode": false,
            "pool_pre_ping": true,
            "pool_size": 5,
            "max_overflow": 0,
            "pool_timeout_seconds": 30,
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

    fn handle(family: ClientFamily) -> CoreConnection {
        let manifest = parse_producer_manifest(MANIFEST).expect("the manifest is complete");
        CoreConnection::new(manifest, PoolProfile::Producer, family)
    }

    /// Building a handle starts nothing; it keeps what it was built with.
    #[test]
    fn a_new_handle_keeps_its_inputs_and_starts_nothing() {
        let rust = handle(ClientFamily::Rust);
        assert_eq!(rust.pool_profile(), PoolProfile::Producer);
        assert_eq!(rust.family(), ClientFamily::Rust);
        assert_eq!(rust.schema_mode(), SchemaMode::ObserveOnly);
        assert_eq!(rust.manifest().manifest_version.get(), 1);
        assert!(matches!(
            rust.connection(),
            HandleConnection::NeverConnected
        ));
        assert_eq!(handle(ClientFamily::Python).family(), ClientFamily::Python);
    }

    /// A new handle's install covers the classes of the default retention
    /// config; the client's request replaces it. Neither starts anything.
    #[test]
    fn the_install_coverage_is_the_default_until_the_client_sets_one() {
        let default = handle(ClientFamily::Rust);
        assert_eq!(default.install_coverage(), &default_install_coverage());

        let seven_days =
            crate::core::history::periods::RetentionDays::parse_period(chrono::Duration::days(7))
                .expect("seven days");
        let request = CoverageRequest::new(2, 2, &[seven_days]).expect("a request in bounds");
        let set = handle(ClientFamily::Python).with_install_coverage(request.clone());
        assert_eq!(set.install_coverage(), &request);
        assert_ne!(set.install_coverage(), default.install_coverage());
        assert!(matches!(set.connection(), HandleConnection::NeverConnected));
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

    /// The manifest of [`MANIFEST`] on the database of `url`, whose handle
    /// installs the schema at its first operation.
    fn installing_manifest_on(url: &str) -> ProducerManifest {
        let mut document: serde_json::Value =
            serde_json::from_str(MANIFEST).expect("the manifest is JSON");
        document["database"]["database_url"] = serde_json::Value::from(url);
        document["schema"] = serde_json::Value::from("MIGRATE_AND_VALIDATE");
        parse_producer_manifest(&document.to_string()).expect("the manifest is complete")
    }

    /// The first install of a handle on an empty database records the family
    /// that its client gave: `PYTHON` for the Python bridge's handle, `RUST`
    /// for the Rust client's.
    #[tokio::test]
    async fn the_first_install_of_a_handle_records_the_family_of_its_client() {
        for family in [ClientFamily::Python, ClientFamily::Rust] {
            let database = TestDatabase::create_empty().await;
            let handle = CoreConnection::new(
                installing_manifest_on(database.url()),
                PoolProfile::Producer,
                family,
            );
            let broker = match handle.initialized_broker().await {
                Ok(broker) => broker,
                Err(error) => panic!("the first install on an empty database: {error}"),
            };
            assert_eq!(broker.client_family(), family);
            let recorded: String = database
                .client()
                .query_one("SELECT family FROM horsies_client_family", &[])
                .await
                .expect("the install wrote one family row")
                .get(0);
            assert_eq!(recorded, family.as_str());
            drop(broker);
            assert_eq!(handle.close().await, HandleClose::Closed);
            database.drop().await;
        }
    }
}
