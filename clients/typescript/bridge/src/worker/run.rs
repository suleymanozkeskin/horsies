//! `prepareWorker`, `runWorker` and `PreparedWorker.requestStop`: the
//! core's worker parent inside a Node or Bun process (the model: the Python
//! bridge's `worker.rs` and the Rust client's `worker.rs`).
//!
//! - `prepareWorker(appConfigJson, startJson)`: pure; starts no core. It
//!   reads the config that `Horsies.create` accepted, builds the worker
//!   manifest (the config sections and the start members), parses it with
//!   the core's parser and picks the profile (`WorkerPlan::of_manifest`).
//!   Answers `prepared`, `manifestBound`, `profileRefused` or
//!   `connectionSettingsRefused`. A start member that the core's parser
//!   refuses is `MANIFEST_REFUSED`, a defect of the package.
//! - `runWorker(worker, handle)`: a service run on the core's process
//!   runtime. The worker builds its own broker for the client family
//!   `TYPESCRIPT`. Its workflow steps build sub-workflow children with the
//!   child spec source of `handle`; a handle that holds none belongs to an
//!   app that lists no workflow, and each sub-workflow node then fails as
//!   not listed. The fail-closed exits 77 and 79 end the process with
//!   `_exit` after a drain of the log channel to stderr
//!   (`ProcessExit::Immediate`): the JavaScript thread may still run code.
//!   A prepared worker runs once; a second run is `CLIENT_DEFECT`.
//! - `PreparedWorker.requestStop()`: idempotent, returns at once. A request
//!   before the run ends the run after its start.
//! - `workerLogBacklog()`: whether core log records still wait in the
//!   channel, for the bounded wait before the process ends. Starts nothing.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use horsies_core::manifest::{parse_producer_manifest, ManifestRefused, ProducerManifest};
use horsies_core::worker_manifest::{parse_worker_manifest, WorkerManifest, WorkerManifestVersion};
use horsies_core::{
    node_failure_result, run_end_of, subworkflow_load_failed_error, AppConfig, ChildSpecFuture,
    ChildSpecRequest, ChildSpecSource, ChildSpecUnavailable, ClientChildSpec, ClientFamily,
    ExitDrain, HorsiesError, ProcessExit, RunPhase, Worker, WorkerBootCause, WorkerFailCause,
    WorkerPlan, WorkerPlanRefused, WorkerRunEnd, WorkerStop, EXIT_DRAIN_BOUND,
};
use napi::bindgen_prelude::{BigInt, Object, ToNapiValue};
use napi::Env;
use napi_derive::napi;
use serde_json::Value;

use crate::call::guard::sync_call;
use crate::call::operation::{guarded_start, resolve, EnvelopeResolver};
use crate::call::service_run::spawn_service_run;
use crate::call::thrown::{Envelope, NativeThrown};
use crate::handle::core_handle::CoreHandle;
use crate::logging::bridge::{running_log, DrainOutcome};
use crate::logging::levels::RecordLevel;
use crate::process_core::runtime::{core_for_call, pure_call_refusal, Core};
use crate::sends::checks::{check_app_config, AppConfigCheck};
use crate::sends::refusals::{CoreRefusal, RefusalHelp};
use crate::values::boundary::bigint_of_u64;
use crate::worker::manifest::{config_sections, ConfigSections, ManifestBoundRefused};
use crate::worker::start::StartPart;

/// The subject of `prepareWorker` in its refusals.
const PREPARE_SUBJECT: &str = "prepare_worker";

/// The subject of the run, in its refusals and records.
const RUN_SUBJECT: &str = "run_worker";

/// The subject of a stop request.
const STOP_SUBJECT: &str = "PreparedWorker.request_stop";

/// The plan of a prepared worker until its run takes it.
enum PlanSlot {
    Ready(Box<WorkerPlan>),
    Taken,
}

/// The worker of one worker manifest, not run yet. JavaScript gets it from
/// `prepareWorker`; it has no constructor.
#[napi]
pub struct PreparedWorker {
    plan: Mutex<PlanSlot>,
    stop: WorkerStop,
}

impl fmt::Debug for PreparedWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PreparedWorker")
    }
}

impl PreparedWorker {
    fn of(plan: WorkerPlan) -> Self {
        Self {
            plan: Mutex::new(PlanSlot::Ready(Box::new(plan))),
            stop: WorkerStop::new(),
        }
    }

    /// The plan, once: the second take finds it taken.
    fn take_plan(&self) -> Option<WorkerPlan> {
        let mut slot = self.plan.lock().unwrap_or_else(PoisonError::into_inner);
        match std::mem::replace(&mut *slot, PlanSlot::Taken) {
            PlanSlot::Ready(plan) => Some(*plan),
            PlanSlot::Taken => None,
        }
    }
}

/// How the preparation of a worker ended.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Debug)]
pub enum WorkerPreparation {
    /// The worker of the profile, ready to run.
    Prepared { worker: PreparedWorker },
    /// A config value is outside the bound of its manifest field. `path`
    /// names the field as the TypeScript config writes it; the value is not
    /// kept. Nothing started.
    ManifestBound {
        path: String,
        min: BigInt,
        max: BigInt,
    },
    /// The profile set fails its checks, or the start names no profile or
    /// one that the app does not define (the core's report). Nothing
    /// started.
    ProfileRefused { refusals: Vec<CoreRefusal> },
    /// The connection settings fail the broker config check (for example
    /// PgBouncer transaction mode with no session URL). Nothing started.
    ConnectionSettingsRefused { cause: String },
}

/// Prepare the worker of the accepted app config `appConfigJson` (the core's
/// config text that `Horsies.create` checked) and the start members
/// `startJson`. Pure: starts no core.
#[napi(ts_return_type = "NativeEnvelope<WorkerPreparation>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn prepare_worker(app_config_json: String, start_json: String) -> Envelope<WorkerPreparation> {
    sync_call(PREPARE_SUBJECT, || {
        if let Some(refusal) = pure_call_refusal() {
            return Envelope::Thrown(refusal.thrown());
        }
        match preparation(&app_config_json, &start_json) {
            Ok(prepared) => Envelope::Value(prepared),
            Err(thrown) => Envelope::Thrown(thrown),
        }
    })
}

/// The preparation of a worker, in order: the config, the start members,
/// the config sections, the manifest, the plan. Pure.
fn preparation(app_config_json: &str, start_json: &str) -> Result<WorkerPreparation, NativeThrown> {
    let (config, producer) = accepted_config(app_config_json)?;
    let start = StartPart::parse(start_json).map_err(manifest_refused)?;
    let sections = match config_sections(&config, &producer) {
        Ok(sections) => sections,
        Err(refused) => return Ok(bound_refusal(refused)),
    };
    let manifest = worker_manifest(sections, start).map_err(manifest_refused)?;
    Ok(match WorkerPlan::of_manifest(&manifest) {
        Ok(plan) => WorkerPreparation::Prepared {
            worker: PreparedWorker::of(plan),
        },
        Err(WorkerPlanRefused::Profile(refused)) => WorkerPreparation::ProfileRefused {
            refusals: refused.errors().iter().map(coded_refusal).collect(),
        },
        Err(WorkerPlanRefused::ConnectionSettings { cause }) => {
            WorkerPreparation::ConnectionSettingsRefused { cause }
        }
    })
}

/// The core's config of `text` and the producer manifest of its broker
/// settings. `Horsies.create` accepted the same text, so a refusal here is a
/// defect of the package: `CLIENT_DEFECT`. Pure.
fn accepted_config(text: &str) -> Result<(AppConfig, ProducerManifest), NativeThrown> {
    let manifest_json = match check_app_config(text.to_owned()) {
        Envelope::Value(AppConfigCheck::Accepted { manifest_json, .. }) => manifest_json,
        Envelope::Value(AppConfigCheck::Refused { refusals }) => {
            return Err(config_defect(&format!(
                "the config check refuses the accepted config: {}",
                refusals
                    .iter()
                    .map(|refusal| refusal.message.as_str())
                    .collect::<Vec<_>>()
                    .join("; ")
            )))
        }
        Envelope::Thrown(thrown) => return Err(thrown),
    };
    let producer = parse_producer_manifest(&manifest_json).map_err(|refused| {
        config_defect(&format!(
            "the producer manifest of the config does not read at {}: {}",
            refused.field_path, refused.cause
        ))
    })?;
    let config = serde_json::from_str::<AppConfig>(text)
        .map_err(|error| config_defect(&format!("the config text does not read: {error}")))?;
    Ok((config, producer))
}

/// The refusal record of a core error with its catalog code, message, notes
/// and help. Pure.
///
/// Panics when the error has no catalog code: every error of a profile
/// refusal and of the connection check has one, so an error with none is a
/// defect of the core. In a bridge call, `sync_call` and `guard` turn the
/// panic into `CORE_PANIC`.
pub fn coded_refusal(error: &HorsiesError) -> CoreRefusal {
    let code = error.code.unwrap_or_else(|| {
        panic!(
            "a core error of a worker check has no catalog code: {}",
            error.message
        )
    });
    CoreRefusal::coded(
        code,
        error.message.clone(),
        error.notes.clone(),
        RefusalHelp::of(error.help_text.clone()),
    )
}

fn config_defect(rule: &str) -> NativeThrown {
    NativeThrown::ClientDefect {
        subject: "appConfigJson".to_owned(),
        rule: rule.to_owned(),
    }
}

fn manifest_refused(refused: ManifestRefused) -> NativeThrown {
    NativeThrown::ManifestRefused {
        field_path: refused.field_path,
        cause: refused.cause,
    }
}

/// The answer of a value outside its manifest bound. Pure.
fn bound_refusal(refused: ManifestBoundRefused) -> WorkerPreparation {
    WorkerPreparation::ManifestBound {
        path: refused.path,
        min: bigint_of_u64(refused.min),
        max: bigint_of_u64(refused.max),
    }
}

/// The worker manifest of `sections` and `start`, read by the core's
/// parser, which checks every member and every bound. Pure.
///
/// Errors: [`ManifestRefused`] for a start member that the parser refuses.
fn worker_manifest(
    sections: ConfigSections,
    start: StartPart,
) -> Result<WorkerManifest, ManifestRefused> {
    let mut members = start.into_members();
    let config_members: [(&str, serde_json::Result<Value>); 9] = [
        (
            "manifest_version",
            serde_json::to_value(WorkerManifestVersion::CURRENT),
        ),
        ("database", serde_json::to_value(&sections.database)),
        ("send", serde_json::to_value(sections.send)),
        ("profiles", serde_json::to_value(&sections.profiles)),
        ("claim", serde_json::to_value(&sections.claim)),
        ("recovery", serde_json::to_value(&sections.recovery)),
        ("retention", serde_json::to_value(&sections.retention)),
        ("resilience", serde_json::to_value(&sections.resilience)),
        ("payload", serde_json::to_value(&sections.payload)),
    ];
    for (name, written) in config_members {
        // A section writes as JSON: every map has text keys, and every value
        // is a number, a text, a flag or a list. Another outcome is a defect.
        let value = written.unwrap_or_else(|error| {
            panic!("the worker manifest member {name} does not write as JSON: {error}")
        });
        let _no_start_member_of_this_name = members.insert(name.to_owned(), value);
    }
    parse_worker_manifest(&Value::Object(members).to_string())
}

/// The boot cause of a worker start that failed, as the core names it.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerBootCauseName {
    SchemaRefused,
    ClientFamilyRefused,
    StatementPooledUrl,
    SessionNotCapable,
    StartTimedOut,
    DatabaseUnavailable,
    DatabaseRefused,
    BootAttemptsExhausted,
    ChildHookFailed,
    PreloadRefused,
    BaselineOverLimit,
    PlacementRefused,
    SpawnerStartFailed,
    ConfigRefused,
    ListenerRefused,
}

impl WorkerBootCauseName {
    /// The name of the core's cause. Pure.
    fn of(cause: WorkerBootCause) -> Self {
        match cause {
            WorkerBootCause::SchemaRefused => Self::SchemaRefused,
            WorkerBootCause::ClientFamilyRefused => Self::ClientFamilyRefused,
            WorkerBootCause::StatementPooledUrl => Self::StatementPooledUrl,
            WorkerBootCause::SessionNotCapable => Self::SessionNotCapable,
            WorkerBootCause::StartTimedOut => Self::StartTimedOut,
            WorkerBootCause::DatabaseUnavailable => Self::DatabaseUnavailable,
            WorkerBootCause::DatabaseRefused => Self::DatabaseRefused,
            WorkerBootCause::BootAttemptsExhausted => Self::BootAttemptsExhausted,
            WorkerBootCause::ChildHookFailed => Self::ChildHookFailed,
            WorkerBootCause::PreloadRefused => Self::PreloadRefused,
            WorkerBootCause::BaselineOverLimit => Self::BaselineOverLimit,
            WorkerBootCause::PlacementRefused => Self::PlacementRefused,
            WorkerBootCause::SpawnerStartFailed => Self::SpawnerStartFailed,
            WorkerBootCause::ConfigRefused => Self::ConfigRefused,
            WorkerBootCause::ListenerRefused => Self::ListenerRefused,
        }
    }
}

/// The cause of a worker that was ready and then failed, as the core names
/// it.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerFailCauseName {
    ServiceLoopDied,
    RunnerHeartbeatLost,
    PoolFailed,
    BaselineOverLimit,
    ClaimFailed,
}

impl WorkerFailCauseName {
    /// The name of the core's cause. Pure.
    fn of(cause: WorkerFailCause) -> Self {
        match cause {
            WorkerFailCause::ServiceLoopDied => Self::ServiceLoopDied,
            WorkerFailCause::RunnerHeartbeatLost => Self::RunnerHeartbeatLost,
            WorkerFailCause::PoolFailed => Self::PoolFailed,
            WorkerFailCause::BaselineOverLimit => Self::BaselineOverLimit,
            WorkerFailCause::ClaimFailed => Self::ClaimFailed,
        }
    }
}

/// How a worker run ended.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerRunEnded {
    /// The run ended at a stop request, after the drain.
    Stopped,
    /// The start failed; the worker claimed nothing. `message` names the
    /// subject and the cause.
    BootFailed {
        cause: WorkerBootCauseName,
        message: String,
    },
    /// The worker was ready, then failed; its running tasks drained first.
    Failed {
        cause: WorkerFailCauseName,
        message: String,
    },
}

impl WorkerRunEnded {
    /// The record of the core's run end. Pure.
    fn of(end: WorkerRunEnd) -> Self {
        match end {
            WorkerRunEnd::Stopped => Self::Stopped,
            WorkerRunEnd::BootFailed { cause, message } => Self::BootFailed {
                cause: WorkerBootCauseName::of(cause),
                message,
            },
            WorkerRunEnd::Failed { cause, message } => Self::Failed {
                cause: WorkerFailCauseName::of(cause),
                message,
            },
        }
    }
}

/// Run `worker` until a stop request or a failure, with the child spec
/// source of `handle`. A prepared worker runs once: a second run resolves
/// with `CLIENT_DEFECT` at once. A refusal of the core resolves at once too,
/// before the plan is taken, so it changes nothing.
#[napi(ts_return_type = "Promise<NativeEnvelope<WorkerRunEnded>>")]
pub fn run_worker<'env>(
    env: &'env Env,
    worker: &PreparedWorker,
    handle: &CoreHandle,
) -> napi::Result<Object<'env>> {
    let taken = guarded_start(RUN_SUBJECT, || {
        let _core = core_for_call().map_err(|refusal| refusal.thrown())?;
        worker
            .take_plan()
            .ok_or_else(|| NativeThrown::ClientDefect {
                subject: RUN_SUBJECT.to_owned(),
                rule: "a prepared worker runs once".to_owned(),
            })
    });
    match taken {
        Err(thrown) => thrown_now::<WorkerRunEnded>(env, thrown),
        Ok(plan) => {
            let source = handle
                .child_spec_source()
                .unwrap_or_else(|| Arc::new(NoWorkflowsListed));
            let stop = worker.stop.clone();
            spawn_service_run(env, RUN_SUBJECT, move |core| {
                run_plan(core, plan, source, stop)
            })
        }
    }
}

/// A Promise that resolves with `thrown` at once.
///
/// Failure exits: the `napi::Error` of a deferred that Node-API cannot
/// create.
pub fn thrown_now<'env, T: ToNapiValue + Send + 'static>(
    env: &'env Env,
    thrown: NativeThrown,
) -> napi::Result<Object<'env>> {
    let (deferred, promise) = env.create_deferred::<Envelope<T>, EnvelopeResolver<T>>()?;
    resolve(deferred, Envelope::Thrown(thrown));
    Ok(promise)
}

/// Build the worker of `plan` and run it to its end. A worker that does not
/// build is a failed start; its error is logged with its cause.
async fn run_plan(
    core: &'static Core,
    plan: WorkerPlan,
    source: Arc<dyn ChildSpecSource>,
    stop: WorkerStop,
) -> WorkerRunEnded {
    let built = Worker::from_plan(plan, source, ClientFamily::TypeScript, worker_exit()).await;
    let end = match built {
        Ok(worker) => worker.with_stop(&stop).run_to_end().await,
        Err(error) => {
            core.log().push_bridge_record(
                RecordLevel::Error,
                format!("{RUN_SUBJECT}: the worker did not build: {error}"),
            );
            run_end_of(Err(error), RunPhase::Starting)
        }
    };
    WorkerRunEnded::of(end)
}

/// How the core ends a worker process from one of its threads (the exits 77
/// and 79): the log records in the channel go to stderr, at most
/// `EXIT_DRAIN_BOUND`, then `_exit`. The JavaScript thread may run code or
/// be blocked, so the forwarder does not wait for the JavaScript sink.
fn worker_exit() -> ProcessExit {
    ProcessExit::Immediate(ExitDrain::WithHost(Arc::new(|| {
        if let Some(log) = running_log() {
            log.sink().switch_to_stderr();
            let _drained_or_left = log.wait_for_drain(EXIT_DRAIN_BOUND);
        }
    })))
}

#[napi]
impl PreparedWorker {
    /// Ask the run of this worker to stop: the claim loop ends, the
    /// children drain within the profile's drain timeout, and the run ends
    /// `stopped`. Idempotent; any thread; returns at once. A request before
    /// the run ends the run after its start.
    #[napi(ts_return_type = "NativeEnvelope<undefined>")]
    pub fn request_stop(&self) -> Envelope<()> {
        sync_call(STOP_SUBJECT, || {
            self.stop.request_stop();
            Envelope::Value(())
        })
    }
}

/// Whether core log records wait in the channel of this process.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogBacklog {
    /// No record waits, or the log of the core never started.
    Empty,
    /// Records wait; the forwarder hands them on.
    Waiting,
    /// Records wait and the forwarder does not run: they stay.
    NoForwarder,
}

/// Whether core log records wait in the channel. Starts nothing; reads
/// counters and returns within one poll pause of the log bridge.
#[napi(ts_return_type = "NativeEnvelope<LogBacklog>")]
pub fn worker_log_backlog() -> Envelope<LogBacklog> {
    sync_call("worker_log_backlog", || {
        Envelope::Value(match running_log() {
            None => LogBacklog::Empty,
            Some(log) => backlog_of(log.wait_for_drain(Duration::ZERO)),
        })
    })
}

/// The backlog of one drain read. Pure.
fn backlog_of(outcome: DrainOutcome) -> LogBacklog {
    match outcome {
        DrainOutcome::Drained => LogBacklog::Empty,
        DrainOutcome::BoundReached { .. } => LogBacklog::Waiting,
        DrainOutcome::ForwarderStopped { .. } => LogBacklog::NoForwarder,
    }
}

/// The child spec source of an app whose handle holds none: the app lists
/// no workflow, so each sub-workflow node fails as not listed, as the Rust
/// client's registry answers a definition key that it does not hold.
struct NoWorkflowsListed;

impl ChildSpecSource for NoWorkflowsListed {
    fn child_spec(&self, request: ChildSpecRequest) -> ChildSpecFuture<'_> {
        let answer = not_listed(&request);
        Box::pin(async move { answer })
    }
}

/// The failure of the node of `request` with the core's load error.
///
/// Errors: [`ChildSpecUnavailable::Raised`], not retryable, when the node
/// error does not encode. Pure.
fn not_listed(request: &ChildSpecRequest) -> Result<ClientChildSpec, ChildSpecUnavailable> {
    let error = subworkflow_load_failed_error(
        &request.parent_workflow_name,
        request.task_index,
        request.definition_key.as_deref(),
    );
    let message = error.to_string();
    match node_failure_result(error) {
        Ok(failure_result) => Ok(ClientChildSpec::NodeFails {
            message,
            failure_result,
        }),
        Err(refused) => Err(ChildSpecUnavailable::Raised {
            subject: format!(
                "build of the child of {}:{}",
                request.parent_workflow_name, request.task_index
            ),
            cause: refused.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONFIG: &str = r#"{"broker": {"database_url": "postgresql://127.0.0.1:55432/horsies"}, "worker_profiles": {"io": {"isolated": {"kind": "isolated_children", "count": 1}}}}"#;

    /// The start members of a TypeScript worker of the profile `profile`.
    fn start(profile: &str) -> String {
        format!(
            r#"{{"profile": {profile}, "metrics_endpoint": {{"kind": "NO_METRICS_ENDPOINT"}}, "tasks": {{"ping": {{"execution": "ISOLATED", "error_codes": []}}}}, "handlers": {{"kind": "NO_HANDLERS"}}, "worker_ended_spans": "NO_SPANS", "child_launch": {{"command": {{"program": "/usr/bin/node", "args": ["/app/main.js", "--horsies-child"]}}, "preload": {{"kind": "NO_PRELOAD_PROGRAM"}}, "program": {{"kind": "RUST"}}, "log_level": "INFO"}}}}"#
        )
    }

    const NAMED_IO: &str = r#"{"kind": "NAMED", "name": "io"}"#;

    fn prepared(start_json: &str) -> Result<WorkerPreparation, NativeThrown> {
        preparation(CONFIG, start_json)
    }

    #[test]
    fn a_named_profile_of_the_app_is_prepared() {
        assert!(matches!(
            prepared(&start(NAMED_IO)),
            Ok(WorkerPreparation::Prepared { .. })
        ));
    }

    #[test]
    fn a_start_with_no_profile_is_refused_with_the_core_report() {
        let Ok(WorkerPreparation::ProfileRefused { refusals }) =
            prepared(&start(r#"{"kind": "NOT_GIVEN"}"#))
        else {
            panic!("a start with no profile is refused");
        };
        assert!(!refusals.is_empty());
    }

    #[test]
    fn a_core_error_keeps_its_code_notes_and_help() {
        let error = HorsiesError::new("unknown worker profile 'absent'")
            .with_code(horsies_core::ErrorCode::CliInvalidArgs)
            .with_note("the app defines the profiles: io")
            .with_help("pass one of them to --profile");
        assert_eq!(
            coded_refusal(&error),
            CoreRefusal {
                code: horsies_core::ErrorCode::CliInvalidArgs
                    .as_code_str()
                    .to_owned(),
                message: "unknown worker profile 'absent'".to_owned(),
                notes: vec!["the app defines the profiles: io".to_owned()],
                help: RefusalHelp::Help {
                    line: "pass one of them to --profile".to_owned()
                },
            }
        );
    }

    #[test]
    #[should_panic(expected = "has no catalog code")]
    fn a_core_error_with_no_code_is_a_defect() {
        let _refusal = coded_refusal(&HorsiesError::new("no code"));
    }

    #[test]
    fn an_unknown_profile_is_refused_with_the_core_report() {
        assert!(matches!(
            prepared(&start(r#"{"kind": "NAMED", "name": "absent"}"#)),
            Ok(WorkerPreparation::ProfileRefused { .. })
        ));
    }

    #[test]
    fn pgbouncer_transaction_mode_with_no_session_url_is_a_connection_settings_refusal() {
        let config = r#"{"broker": {"database_url": "postgresql://127.0.0.1:6432/horsies", "pgbouncer_transaction_mode": true}, "worker_profiles": {"io": {"isolated": {"kind": "isolated_children", "count": 1}}}}"#;
        assert!(matches!(
            preparation(config, &start(NAMED_IO)),
            Ok(WorkerPreparation::ConnectionSettingsRefused { .. })
        ));
    }

    #[test]
    fn a_start_member_that_the_parser_refuses_is_manifest_refused_with_its_path() {
        let text =
            start(NAMED_IO).replace(r#""execution": "ISOLATED""#, r#""execution": "FORKED""#);
        match prepared(&text) {
            Err(NativeThrown::ManifestRefused { field_path, .. }) => {
                assert!(field_path.starts_with("tasks"), "{field_path}");
            }
            other => panic!("an unknown execution is refused, got {other:?}"),
        }
    }

    #[test]
    fn a_missing_start_member_is_manifest_refused() {
        let text = start(NAMED_IO).replace(r#""worker_ended_spans": "NO_SPANS", "#, "");
        assert!(matches!(
            prepared(&text),
            Err(NativeThrown::ManifestRefused { .. })
        ));
    }

    #[test]
    fn a_config_text_that_the_check_refuses_is_a_client_defect() {
        assert!(matches!(
            preparation(r#"{"broker": {}}"#, &start(NAMED_IO)),
            Err(NativeThrown::ClientDefect { .. })
        ));
    }

    #[test]
    fn a_plan_is_taken_once() {
        let Ok(WorkerPreparation::Prepared { worker }) = prepared(&start(NAMED_IO)) else {
            panic!("the worker is prepared");
        };
        assert!(worker.take_plan().is_some());
        assert!(worker.take_plan().is_none());
    }

    #[test]
    fn each_run_end_maps_to_its_record() {
        assert_eq!(
            WorkerRunEnded::of(WorkerRunEnd::Stopped),
            WorkerRunEnded::Stopped
        );
        assert_eq!(
            WorkerRunEnded::of(WorkerRunEnd::BootFailed {
                cause: WorkerBootCause::ChildHookFailed,
                message: "hook".to_owned(),
            }),
            WorkerRunEnded::BootFailed {
                cause: WorkerBootCauseName::ChildHookFailed,
                message: "hook".to_owned(),
            }
        );
        assert_eq!(
            WorkerRunEnded::of(WorkerRunEnd::Failed {
                cause: WorkerFailCause::RunnerHeartbeatLost,
                message: "beats".to_owned(),
            }),
            WorkerRunEnded::Failed {
                cause: WorkerFailCauseName::RunnerHeartbeatLost,
                message: "beats".to_owned(),
            }
        );
    }

    #[test]
    fn each_drain_read_maps_to_its_backlog() {
        assert_eq!(backlog_of(DrainOutcome::Drained), LogBacklog::Empty);
        assert_eq!(
            backlog_of(DrainOutcome::BoundReached { left: 3 }),
            LogBacklog::Waiting
        );
        assert_eq!(
            backlog_of(DrainOutcome::ForwarderStopped { left: 3 }),
            LogBacklog::NoForwarder
        );
    }

    #[test]
    fn a_sub_workflow_node_of_an_app_with_no_workflows_fails_as_not_listed() {
        let request = ChildSpecRequest {
            parent_workflow_id: uuid::Uuid::nil(),
            parent_workflow_name: "nightly".to_owned(),
            task_index: 2,
            child_depth: 1,
            definition_key: Some("region_sync".to_owned()),
            params: horsies_core::SubWorkflowParams::NotGiven,
            node_input: "{}".to_owned(),
            args_from: None,
            sources: Vec::new(),
        };
        assert!(matches!(
            not_listed(&request),
            Ok(ClientChildSpec::NodeFails { .. })
        ));
    }
}
