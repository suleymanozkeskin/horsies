//! A worker from a worker manifest: the inputs of [`Worker::of_child_specs`] resolved
//! from one [`WorkerManifest`] (Python `cli.py`, `worker_command` and
//! `run_worker`: `WorkerConfig`, the app's config sections, the profile, the
//! child program).
//!
//! - [`WorkerPlan::of_manifest`]: pure; picks and checks the profile and
//!   builds every config value. No I/O and no runtime.
//! - [`Worker::from_plan`]: builds the broker of the worker process (its
//!   pools are lazy, so nothing connects) for the client family of the
//!   caller, and the worker, whose workflow steps build sub-workflow children
//!   with the client's child spec source. With a handler child
//!   ([`HandlerChildPlan`]) or `--metrics-listen`, it also builds the
//!   telemetry of the worker: one metric registry, the event queue of its
//!   points and the child that reads it, and the metrics endpoint.
//!
//! The manifest holds values only, each inside its bounds, so every mapping
//! here is total except the profile choice and the broker config check.

use std::collections::{BTreeMap, HashMap};
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;

use crate::events::EventTypeName;
use crate::telemetry::declared::warn_folded_names;
use crate::telemetry::worker::{SentEventTypes, TelemetryOn, EVENT_QUEUE_CAPACITY};
use crate::telemetry::worker_registry::{worker_registry_with_handlers, WorkerRegistry};
use crate::worker::executor::handler_child::HandlerChildMetrics;

use crate::broker::client_family::ClientFamily;
use crate::broker::{BrokerError, PostgresBroker};
use crate::core::config::app::AppConfig;
use crate::core::config::queue::{CustomQueueConfig, QueueMode};
use crate::core::config::worker_profile::{ProfileSelectionRefused, SelectedProfile};
use crate::core::task::DeclaredErrorCodes;
use crate::core::PostgresConfig;
use crate::process_exit::ProcessExit;
use crate::telemetry::listen::ListenAddress;
use crate::worker::children::{ChildLaunch, PreloadProgram};
use crate::worker::claim_pass::RegisteredClasses;
use crate::worker::cli::LogLevel;
use crate::worker::config::WorkerConfig;
use crate::worker::error::WorkerError;
use crate::worker::executor::protocol::{
    ChildConnection, ChildLogLevel, ChildProgram, DatabaseUrl, PythonProgram,
};
use crate::worker::executor::supervisor::ChildCommand;
use crate::worker::worker::Worker;
use crate::worker_manifest::{
    ChildCommandManifest, ChildProgram as ManifestChildProgram, ClaimLease, ClusterWideCap,
    EventHandlers, MetricsEndpoint, PreloadLaunch, QueueConcurrency, Queues, TaskManifest,
    WorkerEndedSpans, WorkerLogLevel, WorkerManifest,
};
use crate::workflow_engine::child_spec::ChildSpecSource;

/// Everything [`Worker::of_child_specs`] takes but the broker and where the child
/// specs come from, resolved from a worker manifest.
#[derive(Clone)]
pub struct WorkerPlan {
    /// The app's config as the worker reads it: queues, claim settings and
    /// the recovery, retention, resilience and payload sections.
    pub app_config: AppConfig,
    /// Every queue of the app, with its priority and cap.
    pub worker_config: WorkerConfig,
    pub profile: SelectedProfile,
    pub launch: ChildLaunch,
    pub registered: RegisteredClasses,
    /// The broker config of the worker process: the resolved parent pool of
    /// the profile with no overflow, and the worker pre-ping.
    pub broker_config: PostgresConfig,
    /// The telemetry of the worker: its handler child and the names of its
    /// metric labels.
    pub telemetry: TelemetryPlan,
    /// The metrics endpoint that the start asks for.
    pub metrics_endpoint: MetricsEndpointPlan,
}

/// The metrics endpoint of a plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricsEndpointPlan {
    /// No `--metrics-listen`: no registry, no endpoint.
    NoMetricsEndpoint,
    /// `--metrics-listen`: the address, and the declared error codes of each
    /// registered task, which give the `task` and `error_code` labels of the
    /// registry.
    ListenAddress {
        address: ListenAddress,
        task_codes: BTreeMap<String, DeclaredErrorCodes>,
    },
}

/// The handler child that a worker manifest asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandlerChildPlan {
    /// The app has no event handler and worker-ended spans are off: no
    /// handler child and no event queue.
    NoHandlerChild,
    /// One handler child. The worker sends it the events of the types
    /// `sent`; `handlers` are the values of the `handler` label of its
    /// metrics, in the order of the manifest.
    HandlerChild {
        sent: SentEventTypes,
        handlers: Vec<String>,
    },
}

impl HandlerChildPlan {
    /// The handler child of `handlers` and `spans`: one child when the app
    /// has a handler or when the child makes the worker-ended spans. The
    /// child gets the event types of the handlers, and `AttemptEnded` for
    /// the spans. Pure.
    pub fn of(handlers: &EventHandlers, spans: WorkerEndedSpans) -> Self {
        let span_types: &[EventTypeName] = match spans {
            WorkerEndedSpans::MakeSpans => &[EventTypeName::AttemptEnded],
            WorkerEndedSpans::NoSpans => &[],
        };
        match (handlers, spans) {
            (EventHandlers::NoHandlers, WorkerEndedSpans::NoSpans) => Self::NoHandlerChild,
            (EventHandlers::NoHandlers, WorkerEndedSpans::MakeSpans) => Self::HandlerChild {
                sent: SentEventTypes::of(span_types.iter().copied()),
                handlers: Vec::new(),
            },
            (EventHandlers::Handlers { handlers }, _) => Self::HandlerChild {
                sent: SentEventTypes::of(
                    handlers
                        .handlers()
                        .iter()
                        .map(|handler| handler.event_type)
                        .chain(span_types.iter().copied()),
                ),
                handlers: handlers
                    .handlers()
                    .iter()
                    .map(|handler| handler.name.as_str().to_owned())
                    .collect(),
            },
        }
    }
}

/// The telemetry of a worker from its manifest.
#[derive(Debug, Clone)]
pub struct TelemetryPlan {
    pub handler_child: HandlerChildPlan,
    /// The values of the `task` label: every registered task.
    pub task_names: Vec<String>,
    /// The values of the `queue` label: every queue of the app.
    pub queue_names: Vec<String>,
    /// The user error codes that each task declares, for the `error_code`
    /// label.
    pub declared_codes: BTreeMap<String, DeclaredErrorCodes>,
}

impl TelemetryPlan {
    /// The telemetry of `manifest`, whose queues are `queue_names`. Pure.
    fn of_manifest(manifest: &WorkerManifest, queue_names: Vec<String>) -> Self {
        Self {
            handler_child: HandlerChildPlan::of(&manifest.handlers, manifest.worker_ended_spans),
            task_names: manifest.tasks.keys().cloned().collect(),
            queue_names,
            declared_codes: manifest
                .tasks
                .iter()
                .map(|(name, task)| (name.clone(), task.error_codes.clone()))
                .collect(),
        }
    }
}

/// Why a manifest gives no worker plan.
#[derive(Debug, Clone, thiserror::Error)]
pub enum WorkerPlanRefused {
    /// The profile set fails its checks, or the start names no profile or
    /// an unknown one (Python `_profile_selection_error`).
    #[error("the worker profile is refused")]
    Profile(ProfileSelectionRefused),
    /// The connection settings of the manifest fail the broker config check
    /// (for example PgBouncer transaction mode with no session URL).
    #[error("the connection settings are refused: {cause}")]
    ConnectionSettings { cause: String },
}

impl WorkerPlan {
    /// The plan of `manifest`. Pure.
    ///
    /// Errors: [`WorkerPlanRefused::Profile`],
    /// [`WorkerPlanRefused::ConnectionSettings`].
    pub fn of_manifest(manifest: &WorkerManifest) -> Result<Self, WorkerPlanRefused> {
        let profile = manifest
            .selected_profile()
            .map_err(WorkerPlanRefused::Profile)?;
        let app_broker = manifest.app_broker_config(&profile);
        let connection = ChildConnection::of_config(&app_broker).map_err(|error| {
            WorkerPlanRefused::ConnectionSettings {
                cause: error.to_string(),
            }
        })?;
        let app_config = app_config_of(manifest, app_broker);
        let worker_config = worker_config_of(manifest, &app_config);
        let telemetry = TelemetryPlan::of_manifest(manifest, worker_config.queues.clone());
        Ok(Self {
            launch: launch_of(manifest, connection),
            registered: registered_of(&manifest.tasks),
            metrics_endpoint: metrics_endpoint_of(manifest),
            broker_config: manifest.worker_process_config(&profile),
            app_config,
            worker_config,
            profile,
            telemetry,
        })
    }
}

impl Worker {
    /// The worker of `plan` for a client of `family`. The broker of the
    /// worker process is built with lazy pools, so nothing connects here; the
    /// run's start steps connect, with the resilience retry. The schema step
    /// of the start records `family` in a new database, and the family step
    /// refuses a database of the other family. The workflow steps of the
    /// worker (the parent advances of phase 2, workflow recovery, phase-2
    /// recovery) build each sub-workflow child with `child_specs`: the child
    /// spec source of the client, which builds each child in its own
    /// language (the `build_with` of the Python client, the registry of the Rust
    /// client).
    ///
    /// With [`MetricsEndpointPlan::ListenAddress`] or
    /// [`HandlerChildPlan::HandlerChild`], the worker gets its telemetry
    /// ([`with_planned_telemetry`]): one registry of its tasks, queues and
    /// handlers, the metrics endpoint of the address
    /// (`Worker::with_metrics_endpoint`) and the handler child
    /// (`Worker::with_handler_child`). The endpoint binds when the run
    /// starts. A task, queue or handler name that cannot be a label value of
    /// its own (empty, the fallback text of its label, or past the bound of
    /// names) is not declared: its samples count in the fallback series, and
    /// one WARNING line names it.
    ///
    /// The liveness and runner heartbeat watches of the worker end the
    /// process through `exit` when they fail closed (exits 79 and 77): a
    /// Rust binary and the Python bridge give [`ProcessExit::Standard`].
    ///
    /// Errors: [`WorkerError::Broker`] when the broker config or the core
    /// handle is refused; [`WorkerError::Config`] as [`Worker::of_child_specs`],
    /// and with an endpoint when the core is built without the feature
    /// `metrics-endpoint`.
    pub async fn from_plan(
        plan: WorkerPlan,
        child_specs: Arc<dyn ChildSpecSource>,
        family: ClientFamily,
        exit: ProcessExit,
    ) -> Result<Self, WorkerError> {
        let broker = worker_broker(&plan.broker_config, family).await?;
        let worker = Worker::of_child_specs(
            Arc::new(broker),
            child_specs,
            plan.app_config,
            plan.worker_config,
            plan.profile,
            plan.launch,
            plan.registered,
        )?
        .with_process_exit(exit);
        with_planned_telemetry(worker, &plan.telemetry, plan.metrics_endpoint)
    }
}

/// `worker` with the registry of `task_codes` and `queues`, and the metrics
/// endpoint of `address`. The endpoint thread starts with the run. Logs one
/// WARNING line that names each task or queue name that the registry does
/// not declare.
///
/// Errors: none in this build; the signature is the one of a build without
/// the feature `metrics-endpoint`.
#[cfg(feature = "metrics-endpoint")]
fn serve_metrics(
    worker: Worker,
    address: ListenAddress,
    task_codes: &BTreeMap<String, DeclaredErrorCodes>,
    queues: &[String],
) -> Result<Worker, WorkerError> {
    use crate::telemetry::worker_registry::worker_registry;

    let WorkerRegistry {
        registry,
        codes,
        folded,
    } = worker_registry(task_codes, queues, worker.worker_facts());
    warn_folded_names(&format!("--metrics-listen {address}"), &folded);
    let telemetry = match TelemetryOn::metrics_only(registry, codes) {
        Ok(telemetry) => telemetry,
        // `worker_registry` builds the registry of every worker metric.
        Err(refused) => panic!("the telemetry of a worker registry was refused: {refused}"),
    };
    Ok(worker.with_metrics_endpoint(address, Arc::new(telemetry)))
}

/// A core built without the feature `metrics-endpoint` has no endpoint.
///
/// Errors: [`WorkerError::Config`], always.
#[cfg(not(feature = "metrics-endpoint"))]
#[expect(
    clippy::needless_pass_by_value,
    reason = "the build with the feature `metrics-endpoint` takes the address by value; both builds \
              have one signature"
)]
fn serve_metrics(
    worker: Worker,
    address: ListenAddress,
    _task_codes: &BTreeMap<String, DeclaredErrorCodes>,
    _queues: &[String],
) -> Result<Worker, WorkerError> {
    drop(worker);
    Err(no_endpoint_in_this_build(&address))
}

/// The metrics endpoint of `manifest`, with the declared codes of each task
/// for an address. Pure.
fn metrics_endpoint_of(manifest: &WorkerManifest) -> MetricsEndpointPlan {
    match &manifest.metrics_endpoint {
        MetricsEndpoint::NoMetricsEndpoint => MetricsEndpointPlan::NoMetricsEndpoint,
        MetricsEndpoint::ListenAddress { host, port } => MetricsEndpointPlan::ListenAddress {
            address: ListenAddress::new(host.clone(), *port),
            task_codes: manifest
                .tasks
                .iter()
                .map(|(name, task)| (name.clone(), task.error_codes.clone()))
                .collect(),
        },
    }
}

/// `worker` with the telemetry of `telemetry` and `endpoint`:
///
/// - no handler child and no endpoint: `worker` as it is, with no registry,
///   no event queue and no child;
/// - an endpoint only: the registry of the tasks and queues, and the
///   endpoint ([`serve_metrics`]);
/// - a handler child: one registry of the tasks, the queues and the
///   handlers, the telemetry value of the points with an event queue of the
///   sent types, and the child that reads the queue. With an endpoint too,
///   the endpoint serves that registry (one telemetry value for both).
///
/// A name that cannot be a label value of its own (empty, the fallback text
/// of its label, or past the bound of names) is not declared: its samples
/// count in the fallback series, and one WARNING line names it. No label
/// value stops a worker.
///
/// Errors: [`WorkerError::Config`] for an endpoint when the core is built
/// without the feature `metrics-endpoint`.
fn with_planned_telemetry(
    worker: Worker,
    telemetry: &TelemetryPlan,
    endpoint: MetricsEndpointPlan,
) -> Result<Worker, WorkerError> {
    let (sent, handlers) = match &telemetry.handler_child {
        HandlerChildPlan::NoHandlerChild => {
            return match endpoint {
                MetricsEndpointPlan::NoMetricsEndpoint => Ok(worker),
                MetricsEndpointPlan::ListenAddress {
                    address,
                    task_codes,
                } => serve_metrics(worker, address, &task_codes, &telemetry.queue_names),
            };
        }
        HandlerChildPlan::HandlerChild { sent, handlers } => (*sent, handlers),
    };
    let WorkerRegistry {
        registry,
        codes,
        folded,
    } = worker_registry_with_handlers(
        &telemetry.declared_codes,
        &telemetry.queue_names,
        handlers,
        worker.worker_facts(),
    );
    warn_folded_names(&folded_names_subject(&endpoint), &folded);
    let (on, queue) = match TelemetryOn::with_event_queue(
        Arc::clone(&registry),
        codes,
        EVENT_QUEUE_CAPACITY,
        sent,
    ) {
        Ok(built) => built,
        Err(refused) => panic!("a worker registry holds the metrics of the points: {refused}"),
    };
    let metrics = match HandlerChildMetrics::of(&registry) {
        Ok(metrics) => metrics,
        Err(refused) => {
            panic!("a worker registry holds the metrics of the handler child: {refused}")
        }
    };
    let on = Arc::new(on);
    let worker = worker.with_handler_child(Arc::clone(&on), queue, metrics);
    match endpoint {
        MetricsEndpointPlan::NoMetricsEndpoint => Ok(worker),
        MetricsEndpointPlan::ListenAddress { address, .. } => serve_registry(worker, address, on),
    }
}

/// The subject of the WARNING line that names the label names that the
/// registry of a worker with a handler child does not declare. Pure.
fn folded_names_subject(endpoint: &MetricsEndpointPlan) -> String {
    match endpoint {
        MetricsEndpointPlan::NoMetricsEndpoint => "the handler child".to_owned(),
        MetricsEndpointPlan::ListenAddress { address, .. } => format!("--metrics-listen {address}"),
    }
}

/// `worker` with the metrics endpoint of `address`, which serves the
/// registry of `telemetry`, the telemetry value of its handler child.
///
/// Errors: none in this build; the signature is the one of a build without
/// the feature `metrics-endpoint`.
#[cfg(feature = "metrics-endpoint")]
fn serve_registry(
    worker: Worker,
    address: ListenAddress,
    telemetry: Arc<TelemetryOn>,
) -> Result<Worker, WorkerError> {
    Ok(worker.with_metrics_endpoint(address, telemetry))
}

/// A core built without the feature `metrics-endpoint` has no endpoint.
///
/// Errors: [`WorkerError::Config`], always.
#[cfg(not(feature = "metrics-endpoint"))]
#[expect(
    clippy::needless_pass_by_value,
    reason = "the build with the feature `metrics-endpoint` takes the address by value; both builds \
              have one signature"
)]
fn serve_registry(
    worker: Worker,
    address: ListenAddress,
    _telemetry: Arc<TelemetryOn>,
) -> Result<Worker, WorkerError> {
    drop(worker);
    Err(no_endpoint_in_this_build(&address))
}

/// The refusal of an endpoint by a core built without the feature
/// `metrics-endpoint`. Pure.
#[cfg(not(feature = "metrics-endpoint"))]
fn no_endpoint_in_this_build(address: &ListenAddress) -> WorkerError {
    WorkerError::Config(format!(
        "--metrics-listen {address}: this build has no metrics endpoint; build horsies-core with \
         its feature metrics-endpoint (the horsies feature cli turns it on)"
    ))
}

/// The broker of the worker process for a client of `family`, with lazy
/// pools: nothing connects.
///
/// Errors: [`BrokerError`] when the broker config is refused.
async fn worker_broker(
    config: &PostgresConfig,
    family: ClientFamily,
) -> Result<PostgresBroker, BrokerError> {
    PostgresBroker::connect_worker_with(config, family).await
}

/// The app config of the worker: the queue mode and the claim settings of
/// the claim section, the four config sections, every profile, and the
/// broker config of the app's connection settings. Pure.
fn app_config_of(manifest: &WorkerManifest, broker: PostgresConfig) -> AppConfig {
    let claim = &manifest.claim;
    let (queue_mode, custom_queues) = match &claim.queues {
        Queues::Default => (QueueMode::Default, None),
        Queues::Custom { queues } => (
            QueueMode::Custom,
            Some(
                queues
                    .iter()
                    .map(|queue| CustomQueueConfig {
                        name: queue.name.clone(),
                        priority: queue.priority.0.get(),
                        max_concurrency: match queue.max_concurrency {
                            QueueConcurrency::Tasks { tasks } => Some(tasks.0.get()),
                            QueueConcurrency::NoLimit => None,
                        },
                    })
                    .collect(),
            ),
        ),
    };
    AppConfig {
        queue_mode,
        custom_queues,
        broker,
        cluster_wide_cap: match claim.cluster_wide_cap {
            ClusterWideCap::Tasks { tasks } => Some(tasks.0.get()),
            ClusterWideCap::NoLimit => None,
        },
        prefetch_buffer: claim.prefetch_buffer.0.get(),
        claim_lease_ms: match claim.claim_lease {
            ClaimLease::WorkerDefault => None,
            ClaimLease::Milliseconds { ms } => Some(ms.0.get()),
        },
        max_claim_renew_age_ms: claim.max_claim_renew_age_ms.0.get(),
        payload: manifest.payload.to_config(),
        recovery: manifest.recovery.to_config(),
        retention: manifest.retention.to_config(),
        resilience: manifest.resilience.to_config(),
        worker_profiles: manifest.config_profiles(),
        schedule: None,
        resend_on_transient_err: false,
    }
}

/// The worker config: every queue of the app (a worker serves all of
/// them), the custom queues' priorities and caps, and the log level of the
/// children. Pure.
fn worker_config_of(manifest: &WorkerManifest, app_config: &AppConfig) -> WorkerConfig {
    let queues = match &manifest.claim.queues {
        Queues::Default => vec!["default".to_owned()],
        Queues::Custom { queues } => queues.iter().map(|queue| queue.name.clone()).collect(),
    };
    let mut config = WorkerConfig {
        queues,
        log_level: worker_log_level(manifest.child_launch.log_level),
        ..WorkerConfig::default()
    };
    config.apply_queue_config(app_config);
    config
}

/// The core log level of a manifest level. The worker logs `CRITICAL` as
/// `ERROR`: the core level has no higher step. Pure.
fn worker_log_level(level: WorkerLogLevel) -> LogLevel {
    match level {
        WorkerLogLevel::Debug => LogLevel::Debug,
        WorkerLogLevel::Info => LogLevel::Info,
        WorkerLogLevel::Warning => LogLevel::Warning,
        WorkerLogLevel::Error | WorkerLogLevel::Critical => LogLevel::Error,
    }
}

/// The child log level of a manifest level. Pure.
fn child_log_level(level: WorkerLogLevel) -> ChildLogLevel {
    match level {
        WorkerLogLevel::Debug => ChildLogLevel::Debug,
        WorkerLogLevel::Info => ChildLogLevel::Info,
        WorkerLogLevel::Warning => ChildLogLevel::Warning,
        WorkerLogLevel::Error => ChildLogLevel::Error,
        WorkerLogLevel::Critical => ChildLogLevel::Critical,
    }
}

/// A command of the manifest as the spawner's command. Pure.
fn command_of(command: &ChildCommandManifest) -> ChildCommand {
    ChildCommand {
        program: PathBuf::from(&command.program),
        args: command.args.iter().map(OsString::from).collect(),
    }
}

/// What every child runs: the child command, the preload command, the
/// program, the log level, the database URL and the connection settings.
/// Pure.
fn launch_of(manifest: &WorkerManifest, connection: ChildConnection) -> ChildLaunch {
    let launch = &manifest.child_launch;
    ChildLaunch {
        command: command_of(&launch.command),
        preload: match &launch.preload {
            PreloadLaunch::Command { command } => PreloadProgram::Command(command_of(command)),
            PreloadLaunch::NoPreloadProgram => PreloadProgram::NoPreloadProgram,
        },
        program: match &launch.program {
            ManifestChildProgram::Python {
                app_locator,
                imports,
                sys_path_roots,
                parent_sys_path,
                parent_argv,
            } => ChildProgram::Python(PythonProgram {
                app_locator: app_locator.clone(),
                imports: imports.clone(),
                sys_path_roots: sys_path_roots.clone(),
                parent_sys_path: parent_sys_path.clone(),
                parent_argv: parent_argv.clone(),
            }),
            ManifestChildProgram::Rust => ChildProgram::Rust,
        },
        log_level: child_log_level(launch.log_level),
        database_url: DatabaseUrl::new(manifest.database.database_url.expose().to_owned()),
        connection,
    }
}

/// The class of each registered task. Pure.
fn registered_of(tasks: &BTreeMap<String, TaskManifest>) -> RegisteredClasses {
    RegisteredClasses::new(
        tasks
            .iter()
            .map(|(name, task)| (name.clone(), task.execution))
            .collect::<HashMap<_, _>>(),
    )
}

#[cfg(test)]
mod tests;
