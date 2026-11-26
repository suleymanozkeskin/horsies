//! `prepareScheduler`, `runScheduler` and `PreparedScheduler.requestStop`:
//! the core's scheduler inside a Node or Bun process (the model: the Python
//! bridge's `scheduler.rs` and the Rust client's `scheduler.rs`).
//!
//! - `prepareScheduler(manifestJson)`: pure; starts no core. The core's
//!   parser reads the scheduler manifest and checks every bound once: a
//!   value outside its bound, a pattern that breaks its rules, a retention
//!   class key that is not one, a repeated schedule name. A refusal is
//!   `manifestRefused` with the field path, never a value. As the checks of
//!   `checks`, it applies no fork and stop rule: it reads only its argument,
//!   and `runScheduler` refuses a forked or stopped core.
//! - `runScheduler(scheduler, handle)`: a service run on the core's process
//!   runtime. It connects with the schema installed once per handle (a stop
//!   request before or during the connect and the install ends the run
//!   `stopped`, with no schedule state written), runs the core's start (the zone of each enabled schedule, its send command,
//!   the client family check, the state start), then one tick at once and
//!   one per check interval until a stop request. A start with nothing to
//!   fire (the master switch off, or no schedule) waits for the stop
//!   request. A failed tick is logged and the next one runs. A prepared
//!   scheduler runs once; a second run is `CLIENT_DEFECT`.
//! - `PreparedScheduler.requestStop()`: idempotent, returns at once, any
//!   thread. A request before the connect ends ends the run at once; a
//!   request during the core's start ends the run after the start; a fire in
//!   flight runs to its end.

use std::fmt;
use std::sync::{Mutex, PoisonError};

use horsies_core::scheduler_manifest::{parse_scheduler_manifest, SchedulerManifest};
use horsies_core::{
    BrokerError, SchedulerService, SchedulerStartError, SchedulerStarted, SchedulerStop,
};
use napi::bindgen_prelude::Object;
use napi::Env;
use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::service_run::spawn_refusable_service_run;
use crate::call::thrown::{Envelope, NativeThrown};
use crate::handle::core_handle::{BrokerConnection, CoreHandle};

/// The subject of `prepareScheduler` in its refusals.
const PREPARE_SUBJECT: &str = "prepare_scheduler";

/// The subject of the run, in its refusals.
const RUN_SUBJECT: &str = "run_scheduler";

/// The subject of a stop request.
const STOP_SUBJECT: &str = "PreparedScheduler.request_stop";

/// The manifest of a prepared scheduler until its run takes it.
enum ManifestSlot {
    Ready(Box<SchedulerManifest>),
    Taken,
}

/// The scheduler of one scheduler manifest, not run yet. JavaScript gets it
/// from `prepareScheduler`; it has no constructor.
#[napi]
pub struct PreparedScheduler {
    manifest: Mutex<ManifestSlot>,
    stop: SchedulerStop,
}

impl fmt::Debug for PreparedScheduler {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PreparedScheduler")
    }
}

impl PreparedScheduler {
    fn of(manifest: SchedulerManifest) -> Self {
        Self {
            manifest: Mutex::new(ManifestSlot::Ready(Box::new(manifest))),
            stop: SchedulerStop::new(),
        }
    }

    /// The manifest, once: the second take finds it taken.
    fn take_manifest(&self) -> Option<SchedulerManifest> {
        let mut slot = self.manifest.lock().unwrap_or_else(PoisonError::into_inner);
        match std::mem::replace(&mut *slot, ManifestSlot::Taken) {
            ManifestSlot::Ready(manifest) => Some(*manifest),
            ManifestSlot::Taken => None,
        }
    }
}

/// How the preparation of a scheduler ended.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Debug)]
pub enum SchedulerPreparation {
    /// The scheduler of the manifest, ready to run.
    Prepared { scheduler: PreparedScheduler },
    /// The core's parser refused the manifest: `field_path` names the field
    /// (`schedules[1].name`), `cause` the rule. Nothing started.
    ManifestRefused { field_path: String, cause: String },
}

/// Prepare the scheduler of the scheduler manifest text `manifestJson`.
/// Pure: starts no core.
#[napi(ts_return_type = "NativeEnvelope<SchedulerPreparation>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn prepare_scheduler(manifest_json: String) -> Envelope<SchedulerPreparation> {
    sync_call(PREPARE_SUBJECT, || {
        Envelope::Value(preparation(&manifest_json))
    })
}

/// The preparation of the manifest of `text`. Pure.
fn preparation(text: &str) -> SchedulerPreparation {
    match parse_scheduler_manifest(text) {
        Ok(manifest) => SchedulerPreparation::Prepared {
            scheduler: PreparedScheduler::of(manifest),
        },
        Err(refused) => SchedulerPreparation::ManifestRefused {
            field_path: refused.field_path,
            cause: refused.cause,
        },
    }
}

/// How a scheduler run ended.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchedulerRunEnded {
    /// The run ended at a stop request.
    Stopped,
    /// The start refused `schedule` before any state write: a zone that the
    /// core does not know, a send that the command rules refuse, or a
    /// pattern with no slot after the database time. `reason` names the
    /// schedule and the cause.
    StartRefused { schedule: String, reason: String },
    /// The connect, the schema install or the client family check failed.
    /// No schedule state was written. `retryable` tells whether a later
    /// start may pass.
    StartFailed { message: String, retryable: bool },
}

/// Run `scheduler` on the broker of `handle` until a stop request, or until
/// its start is refused or fails. The manifest is taken only after the core
/// of the process was taken for the call, so a refusal of the core changes
/// nothing. A prepared scheduler runs once: a second run resolves with
/// `CLIENT_DEFECT` at once.
#[napi(ts_return_type = "Promise<NativeEnvelope<SchedulerRunEnded>>")]
pub fn run_scheduler<'env>(
    env: &'env Env,
    scheduler: &PreparedScheduler,
    handle: &CoreHandle,
) -> napi::Result<Object<'env>> {
    let connection = handle.connection();
    let stop = scheduler.stop.clone();
    spawn_refusable_service_run(env, RUN_SUBJECT, move |_core| {
        let manifest = scheduler
            .take_manifest()
            .ok_or_else(|| NativeThrown::ClientDefect {
                subject: RUN_SUBJECT.to_owned(),
                rule: "a prepared scheduler runs once".to_owned(),
            })?;
        Ok(run_manifest(connection, manifest, stop))
    })
}

/// Connect with the schema installed once per handle, start the scheduler
/// of `manifest`, then tick until a stop request; a start with nothing to
/// fire waits for the stop request. A stop request before or during the
/// connect and the install ends the run with nothing written: the connect
/// future is dropped, as Python's scheduler cancels its start at the signal.
async fn run_manifest(
    connection: BrokerConnection,
    manifest: SchedulerManifest,
    stop: SchedulerStop,
) -> SchedulerRunEnded {
    let connected = tokio::select! {
        biased;
        () = stop.requested() => return SchedulerRunEnded::Stopped,
        connected = connection.lazy_handle().initialized_broker() => connected,
    };
    let broker = match connected {
        Ok(broker) => broker,
        Err(error) => return start_failed(&error),
    };
    match SchedulerService::from_manifest(broker, manifest, &stop)
        .start()
        .await
    {
        Ok(SchedulerStarted::Active(active)) => active.run().await,
        Ok(SchedulerStarted::Idle) => stop.requested().await,
        Err(error) => return start_error(&error),
    }
    SchedulerRunEnded::Stopped
}

/// The end of a start whose connect or schema install met `error`. Pure.
fn start_failed(error: &BrokerError) -> SchedulerRunEnded {
    SchedulerRunEnded::StartFailed {
        message: error.to_string(),
        retryable: error.is_retryable(),
    }
}

/// The end of a start that the core refused or that failed. Pure.
fn start_error(error: &SchedulerStartError) -> SchedulerRunEnded {
    match error {
        SchedulerStartError::UnknownTimezone { schedule, .. }
        | SchedulerStartError::CommandRefused { schedule, .. }
        | SchedulerStartError::NoNextSlot { schedule, .. } => SchedulerRunEnded::StartRefused {
            schedule: schedule.clone(),
            reason: error.to_string(),
        },
        SchedulerStartError::ClientFamily(cause) => SchedulerRunEnded::StartFailed {
            message: error.to_string(),
            retryable: cause.is_retryable(),
        },
    }
}

#[napi]
impl PreparedScheduler {
    /// Ask the run of this scheduler to stop at its next tick boundary; a
    /// fire in flight runs to its end. Idempotent; any thread; returns at
    /// once. A request before the run ends the run after its start.
    #[napi(ts_return_type = "NativeEnvelope<undefined>")]
    pub fn request_stop(&self) -> Envelope<()> {
        sync_call(STOP_SUBJECT, || {
            self.stop.request_stop();
            Envelope::Value(())
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use horsies_core::manifest::parse_producer_manifest;
    use horsies_core::PoolProfile;

    use super::*;

    /// A producer manifest whose database cannot be reached: nothing listens
    /// on port 1. A test that passes connects to nothing.
    const UNREACHABLE_PRODUCER_MANIFEST: &str = r#"{
        "manifest_version": 1,
        "database": {
            "database_url": "postgresql://horsies@127.0.0.1:1/none",
            "session_database": {"kind": "SAME_AS_DATABASE_URL"},
            "pgbouncer_transaction_mode": false,
            "pool_pre_ping": true,
            "pool_size": 5,
            "max_overflow": 0,
            "pool_timeout": 30,
            "pool_recycle": {"kind": "AFTER_SECONDS", "seconds": 1800},
            "echo": false,
            "tcp_keepalives": {"kind": "ON", "idle_seconds": 30, "interval_seconds": 10, "probes": 3},
            "tcp_user_timeout": {"kind": "AFTER", "milliseconds": 30000}
        },
        "schema": "MIGRATE_AND_VALIDATE",
        "send": {"retain_rerun_input_default": false, "idempotency_reservation_window": 86400000000}
    }"#;

    /// A scheduler manifest of `schedules` with the section defaults.
    fn manifest_text(schedules: &str) -> String {
        format!(
            r#"{{"manifest_version": 1, "enabled": true, "check_interval_seconds": 1, "schedules": {schedules}}}"#
        )
    }

    /// One enabled schedule `name` that fires every 30 seconds.
    fn enabled(name: &str) -> String {
        format!(
            r#"{{"name": "{name}", "activation": {{"kind": "ENABLED", "pattern_json": "{{\"type\":\"interval\",\"seconds\":30,\"minutes\":null,\"hours\":null,\"days\":null}}", "timezone": "UTC", "catch_up_missed": false, "max_catch_up_runs": 100, "send": {{"task_name": "ping", "queue_name": "default", "priority": 100, "input_json": "{{}}", "task_options": {{"kind": "NO_OPTIONS"}}, "retention_class_key": "standard_30d", "retain_rerun_input": false}}}}}}"#
        )
    }

    fn refused_path(text: &str) -> String {
        match preparation(text) {
            SchedulerPreparation::ManifestRefused { field_path, .. } => field_path,
            SchedulerPreparation::Prepared { .. } => panic!("the manifest is refused: {text}"),
        }
    }

    #[test]
    fn an_enabled_and_a_disabled_schedule_are_prepared() {
        let text = manifest_text(&format!(
            r#"[{}, {{"name": "paused", "activation": {{"kind": "DISABLED"}}}}]"#,
            enabled("nightly")
        ));
        assert!(matches!(
            preparation(&text),
            SchedulerPreparation::Prepared { .. }
        ));
    }

    #[test]
    fn a_repeated_name_is_refused_at_the_second_schedule() {
        let text = manifest_text(&format!("[{}, {}]", enabled("nightly"), enabled("nightly")));
        assert_eq!(refused_path(&text), "schedules[1].name");
    }

    #[test]
    fn a_check_interval_outside_its_bound_is_refused_with_its_path() {
        let text = manifest_text("[]").replace(
            r#""check_interval_seconds": 1"#,
            r#""check_interval_seconds": 61"#,
        );
        assert_eq!(refused_path(&text), "check_interval_seconds");
    }

    #[test]
    fn a_pattern_that_breaks_its_rules_is_refused_at_its_activation() {
        let text = manifest_text(&format!("[{}]", enabled("nightly")))
            .replace(r#"\"seconds\":30"#, r#"\"seconds\":0"#);
        assert!(refused_path(&text).starts_with("schedules[0].activation"));
    }

    #[test]
    fn a_stop_before_the_connect_ends_the_run_with_no_connect() {
        let producer = parse_producer_manifest(UNREACHABLE_PRODUCER_MANIFEST)
            .expect("the producer manifest is complete");
        let connection = BrokerConnection::new(producer, PoolProfile::Producer);
        let SchedulerPreparation::Prepared { scheduler } =
            preparation(&manifest_text(&format!("[{}]", enabled("nightly"))))
        else {
            panic!("the manifest is prepared");
        };
        let manifest = scheduler
            .take_manifest()
            .expect("a new scheduler holds its manifest");
        scheduler.stop.request_stop();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a test runtime builds");
        let ended = runtime.block_on(run_manifest(connection, manifest, scheduler.stop.clone()));
        assert_eq!(ended, SchedulerRunEnded::Stopped);
    }

    #[test]
    fn a_manifest_is_taken_once() {
        let SchedulerPreparation::Prepared { scheduler } = preparation(&manifest_text("[]")) else {
            panic!("an empty manifest is prepared");
        };
        assert!(scheduler.take_manifest().is_some());
        assert!(scheduler.take_manifest().is_none());
    }

    #[test]
    fn a_refused_schedule_is_a_start_refusal_that_names_it() {
        for refused in [
            SchedulerStartError::UnknownTimezone {
                schedule: "nightly".to_owned(),
                timezone: "Not/A_Zone".to_owned(),
            },
            SchedulerStartError::CommandRefused {
                schedule: "nightly".to_owned(),
                cause: Box::new(BrokerError::ClaimContract("refused options".to_owned())),
            },
            SchedulerStartError::NoNextSlot {
                schedule: "nightly".to_owned(),
                after: Utc::now(),
            },
        ] {
            assert_eq!(
                start_error(&refused),
                SchedulerRunEnded::StartRefused {
                    schedule: "nightly".to_owned(),
                    reason: refused.to_string(),
                }
            );
        }
    }

    #[test]
    fn a_failed_family_check_is_a_start_failure_with_its_retryable_flag() {
        for (cause, retryable) in [
            (BrokerError::ConnectionFailed("no route".to_owned()), true),
            (
                BrokerError::ClaimContract("a broken answer".to_owned()),
                false,
            ),
        ] {
            let error = SchedulerStartError::ClientFamily(cause);
            assert_eq!(
                start_error(&error),
                SchedulerRunEnded::StartFailed {
                    message: error.to_string(),
                    retryable,
                }
            );
        }
    }

    #[test]
    fn a_failed_connect_or_install_is_a_start_failure_with_its_cause() {
        for (error, retryable) in [
            (BrokerError::ConnectionFailed("no route".to_owned()), true),
            (
                BrokerError::ClaimContract("a broken answer".to_owned()),
                false,
            ),
        ] {
            assert_eq!(
                start_failed(&error),
                SchedulerRunEnded::StartFailed {
                    message: error.to_string(),
                    retryable,
                }
            );
        }
    }
}
