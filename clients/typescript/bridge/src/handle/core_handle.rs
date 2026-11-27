//! `CoreHandle`: the handle that the package builds from a producer manifest,
//! with the pool profile of its process.
//!
//! - Building a handle parses the manifest and starts nothing: no runtime,
//!   no connection. The first operation starts the core of the process and
//!   connects the handle's broker; a failed connect is not kept, so the next
//!   operation connects again.
//! - The lazy handle is the core's ([`horsies_core::handle::CoreConnection`]),
//!   with the client family `TYPESCRIPT`.
//! - The operations of other areas are free functions of those areas that
//!   take `&CoreHandle`: they use [`CoreHandle::connection`] and the child
//!   spec source slot, and never change this file.

use std::fmt;
use std::sync::{Arc, OnceLock};

use horsies_core::handle::{CoreConnection as LazyHandle, HandleConnection};
use horsies_core::manifest::{parse_producer_manifest, ProducerManifest, SchemaMode};
use horsies_core::{
    BrokerError, ChildSpecSource, ClientFamily, CoverageRequest, DatabaseOperationError,
    PoolProfile, PostgresBroker, RetentionDays,
};
use napi::bindgen_prelude::Object;
use napi::Env;
use napi_derive::napi;

use crate::call::guard::{sync_call, CallKind};
use crate::call::operation::{resolve, spawn_operation, EnvelopeResolver};
use crate::call::thrown::{Envelope, NativeThrown};
use crate::handle::schema::{install_operation, schema_status_operation};
use crate::values::boundary::{u32_of_number, BoundaryRefusal};
use crate::values::records::Outcome;

/// The pool profile of the process of a handle.
#[napi(string_enum = "UPPERCASE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandleProfile {
    /// A producer or a monitoring process: the main pool keeps no reserve.
    Producer,
    /// A worker parent: the main pool keeps connections free from batch
    /// checkouts for the per-task writes.
    Worker,
}

impl HandleProfile {
    /// The core's pool profile. Pure.
    fn pool_profile(self) -> PoolProfile {
        match self {
            Self::Producer => PoolProfile::Producer,
            Self::Worker => PoolProfile::Worker,
        }
    }
}

/// The core's lazy handle of a TypeScript process: a manifest, the pool
/// profile, the client family `TYPESCRIPT`, and the broker, connected at
/// first use. Clones share one handle.
#[derive(Clone)]
pub struct BrokerConnection {
    handle: Arc<LazyHandle>,
}

impl BrokerConnection {
    /// The connection of `manifest` with the pools of `profile`. Starts
    /// nothing.
    pub fn new(manifest: ProducerManifest, profile: PoolProfile) -> Self {
        Self {
            handle: Arc::new(LazyHandle::new(manifest, profile, ClientFamily::TypeScript)),
        }
    }

    /// A new connection of the same manifest and profile whose schema install
    /// ends with the coverage pass of `request`. It shares no broker with
    /// this one. Starts nothing.
    pub fn with_install_coverage(&self, request: CoverageRequest) -> Self {
        Self {
            handle: Arc::new(
                LazyHandle::new(
                    self.handle.manifest().clone(),
                    self.handle.pool_profile(),
                    ClientFamily::TypeScript,
                )
                .with_install_coverage(request),
            ),
        }
    }

    /// The connected broker. Connects at the first call; a failure is
    /// returned and not kept.
    pub async fn broker(&self) -> Result<Arc<PostgresBroker>, BrokerError> {
        self.handle.broker().await
    }

    /// The schema mode of the handle's manifest.
    pub fn schema_mode(&self) -> SchemaMode {
        self.handle.schema_mode()
    }

    /// The core's lazy handle, for a core function of a later area.
    pub fn lazy_handle(&self) -> &LazyHandle {
        &self.handle
    }

    /// Whether a connect succeeded on this handle. Starts nothing and waits
    /// for nothing.
    fn connect_state(&self) -> HandleConnection {
        self.handle.connection()
    }
}

/// The child spec source slot is set already.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChildSpecSourceSet;

/// The core handle of one producer manifest and the pool profile of its
/// process. JavaScript gets it from `buildCoreHandle`; it has no
/// constructor.
#[napi]
pub struct CoreHandle {
    connection: BrokerConnection,
    /// The child spec source of the handle's workflow operations: set once
    /// by the workflows area.
    child_spec_source: OnceLock<Arc<dyn ChildSpecSource>>,
}

impl CoreHandle {
    fn of(connection: BrokerConnection) -> Self {
        Self {
            connection,
            child_spec_source: OnceLock::new(),
        }
    }

    /// A clone of the connection, for an operation future.
    pub fn connection(&self) -> BrokerConnection {
        self.connection.clone()
    }

    /// Set the child spec source of the handle's workflow operations. A
    /// second set is refused. Starts nothing.
    pub fn set_child_spec_source(
        &self,
        source: Arc<dyn ChildSpecSource>,
    ) -> Result<(), ChildSpecSourceSet> {
        self.child_spec_source
            .set(source)
            .map_err(|_already_set| ChildSpecSourceSet)
    }

    /// The child spec source, or `None` before the workflows area set it.
    pub fn child_spec_source(&self) -> Option<Arc<dyn ChildSpecSource>> {
        self.child_spec_source.get().map(Arc::clone)
    }
}

/// The values of the coverage pass that a schema install runs: the client's
/// retention config. Each count crosses as a number and is checked as a
/// whole number from 0 to 4294967295 (`u32_of_number`).
#[napi(object, object_to_js = false)]
pub struct InstallCoverageArg {
    pub history_horizon_days: f64,
    pub heartbeat_horizon_hours: f64,
    /// The declared retention periods, in whole days.
    pub declared_period_days: Vec<f64>,
}

/// Why the coverage values do not make a request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CoverageRefusal {
    /// A count is not a whole number from 0 to 4294967295.
    Number(BoundaryRefusal),
    /// The core refuses a declared period of `days` days.
    Period { days: u32, cause: String },
    /// The core refuses the horizons.
    Request { cause: String },
}

impl fmt::Display for CoverageRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Number(refusal) => write!(formatter, "{}: {}", refusal.subject, refusal.rule),
            Self::Period { days, cause } => {
                write!(formatter, "declared period of {days} days: {cause}")
            }
            Self::Request { cause } => formatter.write_str(cause),
        }
    }
}

/// One round trip of a database ping.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq)]
pub struct DatabasePingRecord {
    pub latency_ms: f64,
}

/// Build a handle from the JSON text of a producer manifest and the pool
/// profile of its process. Starts nothing. A manifest that the core refuses
/// throws `MANIFEST_REFUSED` with the field path and the cause.
#[napi(ts_return_type = "NativeEnvelope<CoreHandle>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn build_core_handle(manifest_json: String, profile: HandleProfile) -> Envelope<CoreHandle> {
    sync_call("build_core_handle", || {
        match parse_producer_manifest(&manifest_json) {
            Ok(manifest) => Envelope::Value(CoreHandle::of(BrokerConnection::new(
                manifest,
                profile.pool_profile(),
            ))),
            Err(refused) => Envelope::Thrown(NativeThrown::ManifestRefused {
                field_path: refused.field_path,
                cause: refused.cause,
            }),
        }
    })
}

#[napi]
impl CoreHandle {
    /// The version of the manifest this handle was built from.
    #[napi(ts_return_type = "NativeEnvelope<number>")]
    pub fn manifest_version(&self) -> Envelope<u32> {
        sync_call("manifest_version", || {
            Envelope::Value(self.connection.handle.manifest().manifest_version.get())
        })
    }

    /// A new handle of the same manifest and profile whose schema install
    /// ends with the coverage pass of `coverage`. It shares no pool and no
    /// child spec source with this one. Values that the core refuses throw
    /// `CLIENT_DEFECT`: the package checks the retention config first.
    /// Starts nothing.
    #[napi(ts_return_type = "NativeEnvelope<CoreHandle>")]
    #[allow(
        clippy::needless_pass_by_value,
        reason = "napi-rs passes the arguments of a JavaScript call by value"
    )]
    pub fn with_install_coverage(&self, coverage: InstallCoverageArg) -> Envelope<CoreHandle> {
        sync_call("with_install_coverage", || {
            match coverage_request(&coverage) {
                Ok(request) => Envelope::Value(CoreHandle::of(
                    self.connection.with_install_coverage(request),
                )),
                Err(refusal) => Envelope::Thrown(NativeThrown::ClientDefect {
                    subject: "installCoverage".to_owned(),
                    rule: refusal.to_string(),
                }),
            }
        })
    }

    /// Close the handle's core pools. No new operation takes the closed
    /// pools; writes in flight run to their end within the close bound, and
    /// the ones that do not are logged with their count. A handle that never
    /// connected resolves at once and starts nothing. The next operation
    /// opens new pools.
    #[napi(ts_return_type = "Promise<NativeEnvelope<undefined>>")]
    pub fn close<'env>(&self, env: &'env Env) -> napi::Result<Object<'env>> {
        let broker = match self.connection.connect_state() {
            HandleConnection::NeverConnected => {
                let (deferred, promise) =
                    env.create_deferred::<Envelope<()>, EnvelopeResolver<()>>()?;
                resolve(deferred, Envelope::Value(()));
                return Ok(promise);
            }
            HandleConnection::Connected(broker) => broker,
        };
        spawn_operation(env, "close", CallKind::Write, move |_core| async move {
            broker.close().await;
        })
    }

    /// Ping the database: the round trip, or the catalog value with
    /// `DATABASE_PING_FAILED`.
    #[napi(ts_return_type = "Promise<NativeEnvelope<NativeOutcome<DatabasePingRecord>>>")]
    pub fn ping_database<'env>(&self, env: &'env Env) -> napi::Result<Object<'env>> {
        let connection = self.connection();
        spawn_operation(env, "ping_database", CallKind::Read, move |_core| {
            ping_database_operation(connection)
        })
    }

    /// Install the schema: the migrations, then the coverage pass of the
    /// handle's request. A handle of the `OBSERVE_ONLY` or the
    /// `ASSUME_INITIALIZED` mode runs nothing. A second install on the same
    /// handle runs nothing.
    #[napi(ts_return_type = "Promise<NativeEnvelope<InstallOutcome>>")]
    pub fn install_schema<'env>(&self, env: &'env Env) -> napi::Result<Object<'env>> {
        let connection = self.connection();
        spawn_operation(env, "install_schema", CallKind::Write, move |_core| {
            install_operation(connection)
        })
    }

    /// Read the schema state of the database. A database that cannot be
    /// reached or read is `UNKNOWN`.
    #[napi(ts_return_type = "Promise<NativeEnvelope<SchemaStatusRecord>>")]
    pub fn schema_status<'env>(&self, env: &'env Env) -> napi::Result<Object<'env>> {
        let connection = self.connection();
        spawn_operation(env, "schema_status", CallKind::Read, move |_core| {
            schema_status_operation(connection)
        })
    }
}

/// The checked request of the coverage values, or the refusal. Pure.
fn coverage_request(coverage: &InstallCoverageArg) -> Result<CoverageRequest, CoverageRefusal> {
    let history_days = u32_of_number("historyHorizonDays", coverage.history_horizon_days)
        .map_err(CoverageRefusal::Number)?;
    let heartbeat_hours = u32_of_number("heartbeatHorizonHours", coverage.heartbeat_horizon_hours)
        .map_err(CoverageRefusal::Number)?;
    let periods = coverage
        .declared_period_days
        .iter()
        .map(|days| {
            let days =
                u32_of_number("declaredPeriodDays", *days).map_err(CoverageRefusal::Number)?;
            RetentionDays::parse_period(chrono::Duration::days(i64::from(days))).map_err(
                |refused| CoverageRefusal::Period {
                    days,
                    cause: refused.to_string(),
                },
            )
        })
        .collect::<Result<Vec<_>, CoverageRefusal>>()?;
    CoverageRequest::new(history_days, heartbeat_hours, &periods).map_err(|refused| {
        CoverageRefusal::Request {
            cause: refused.to_string(),
        }
    })
}

async fn ping_database_operation(connection: BrokerConnection) -> Outcome<DatabasePingRecord> {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return Outcome::failed(
                "ping_database",
                &DatabaseOperationError::database_ping_failed(error),
            )
        }
    };
    match broker.ping_database().await {
        Ok(ping) => Outcome::Done(DatabasePingRecord {
            latency_ms: ping.latency_ms,
        }),
        Err(error) => Outcome::failed("ping_database", &error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        "schema": "MIGRATE_AND_VALIDATE",
        "send": {
            "retain_rerun_input_default": false,
            "idempotency_reservation_window": 86400000000
        }
    }"#;

    fn built(profile: HandleProfile) -> CoreHandle {
        match build_core_handle(MANIFEST.to_owned(), profile) {
            Envelope::Value(handle) => handle,
            Envelope::Thrown(thrown) => panic!("the manifest is complete, got {thrown:?}"),
        }
    }

    #[test]
    fn a_handle_writes_the_typescript_family_and_starts_nothing() {
        let handle = built(HandleProfile::Worker);
        assert_eq!(handle.connection.handle.pool_profile(), PoolProfile::Worker);
        assert_eq!(handle.connection.handle.family(), ClientFamily::TypeScript);
        assert!(matches!(
            handle.connection.connect_state(),
            HandleConnection::NeverConnected
        ));
        assert_eq!(handle.manifest_version(), Envelope::Value(1));
    }

    #[test]
    fn a_refused_manifest_throws_manifest_refused_with_its_field_path() {
        let envelope = build_core_handle(
            MANIFEST.replace("\"pool_size\": 5", "\"pool_size\": \"five\""),
            HandleProfile::Producer,
        );
        match envelope {
            Envelope::Thrown(NativeThrown::ManifestRefused { field_path, .. }) => {
                assert!(field_path.contains("pool_size"), "{field_path}");
            }
            Envelope::Thrown(other) => panic!("a refused manifest, got {other:?}"),
            Envelope::Value(_) => panic!("a refused manifest, got a handle"),
        }
    }

    #[test]
    fn a_handle_with_an_install_request_keeps_its_inputs_and_starts_nothing() {
        let handle = built(HandleProfile::Worker);
        let covered = match handle.with_install_coverage(InstallCoverageArg {
            history_horizon_days: 3.0,
            heartbeat_horizon_hours: 6.0,
            declared_period_days: vec![7.0],
        }) {
            Envelope::Value(covered) => covered,
            Envelope::Thrown(thrown) => panic!("a request in bounds, got {thrown:?}"),
        };
        let seven_days =
            RetentionDays::parse_period(chrono::Duration::days(7)).expect("seven days");
        let request = CoverageRequest::new(3, 6, &[seven_days]).expect("a request in bounds");
        assert_eq!(covered.connection.handle.install_coverage(), &request);
        assert_eq!(
            covered.connection.handle.pool_profile(),
            PoolProfile::Worker
        );
        assert_eq!(covered.connection.handle.family(), ClientFamily::TypeScript);
        assert!(matches!(
            covered.connection.connect_state(),
            HandleConnection::NeverConnected
        ));
    }

    #[test]
    fn coverage_values_that_the_core_refuses_are_a_client_defect() {
        let handle = built(HandleProfile::Producer);
        let envelope = handle.with_install_coverage(InstallCoverageArg {
            history_horizon_days: 0.0,
            heartbeat_horizon_hours: 6.0,
            declared_period_days: vec![],
        });
        match envelope {
            Envelope::Thrown(NativeThrown::ClientDefect { subject, .. }) => {
                assert_eq!(subject, "installCoverage");
            }
            Envelope::Thrown(other) => panic!("a client defect, got {other:?}"),
            Envelope::Value(_) => panic!("a client defect, got a handle"),
        }
    }

    #[test]
    fn a_count_that_is_not_a_whole_number_is_refused_with_its_field() {
        let refused = coverage_request(&InstallCoverageArg {
            history_horizon_days: 3.0,
            heartbeat_horizon_hours: 6.0,
            declared_period_days: vec![7.5],
        });
        assert!(
            matches!(
                refused,
                Err(CoverageRefusal::Number(ref refusal)) if refusal.subject == "declaredPeriodDays"
            ),
            "{refused:?}"
        );
    }

    #[test]
    fn a_horizon_past_u32_is_refused_not_wrapped() {
        let refused = coverage_request(&InstallCoverageArg {
            history_horizon_days: 4_294_967_299.0,
            heartbeat_horizon_hours: 6.0,
            declared_period_days: vec![],
        });
        assert!(
            matches!(
                refused,
                Err(CoverageRefusal::Number(ref refusal)) if refusal.subject == "historyHorizonDays"
            ),
            "{refused:?}"
        );
    }

    #[test]
    fn a_period_that_the_core_refuses_names_its_days() {
        let refused = coverage_request(&InstallCoverageArg {
            history_horizon_days: 3.0,
            heartbeat_horizon_hours: 6.0,
            declared_period_days: vec![0.0],
        });
        assert!(
            matches!(refused, Err(CoverageRefusal::Period { days: 0, .. })),
            "{refused:?}"
        );
    }

    #[test]
    fn the_child_spec_source_slot_is_set_once() {
        struct NoSource;
        impl ChildSpecSource for NoSource {
            fn child_spec(
                &self,
                _request: horsies_core::ChildSpecRequest,
            ) -> horsies_core::ChildSpecFuture<'_> {
                unreachable!("the slot test never asks for a spec")
            }
        }
        let handle = built(HandleProfile::Producer);
        assert!(handle.child_spec_source().is_none());
        assert_eq!(handle.set_child_spec_source(Arc::new(NoSource)), Ok(()));
        assert_eq!(
            handle.set_child_spec_source(Arc::new(NoSource)),
            Err(ChildSpecSourceSet)
        );
        assert!(handle.child_spec_source().is_some());
    }
}
