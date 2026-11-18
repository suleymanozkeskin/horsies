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
//!   with the client's child spec source.
//!
//! The manifest holds values only, each inside its bounds, so every mapping
//! here is total except the profile choice and the broker config check.

use std::collections::{BTreeMap, HashMap};
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;

use crate::broker::client_family::ClientFamily;
use crate::broker::{BrokerError, PostgresBroker};
use crate::core::config::app::AppConfig;
use crate::core::config::queue::{CustomQueueConfig, QueueMode};
use crate::core::config::worker_profile::{ProfileSelectionRefused, SelectedProfile};
use crate::core::task::Execution;
use crate::core::PostgresConfig;
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
    PreloadLaunch, QueueConcurrency, Queues, WorkerLogLevel, WorkerManifest,
};
use crate::workflow_engine::child_spec::ChildSpecs;

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
        Ok(Self {
            launch: launch_of(manifest, connection),
            registered: registered_of(&manifest.tasks),
            broker_config: manifest.worker_process_config(&profile),
            app_config,
            worker_config,
            profile,
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
    /// Errors: [`WorkerError::Broker`] when the broker config or the core
    /// handle is refused; [`WorkerError::Config`] as [`Worker::of_child_specs`].
    pub async fn from_plan(
        plan: WorkerPlan,
        child_specs: impl Into<ChildSpecs>,
        family: ClientFamily,
    ) -> Result<Self, WorkerError> {
        let child_specs = child_specs.into();
        let broker = worker_broker(&plan.broker_config, family).await?;
        Worker::of_child_specs(
            Arc::new(broker),
            child_specs,
            plan.app_config,
            plan.worker_config,
            plan.profile,
            plan.launch,
            plan.registered,
        )
    }
}

/// The broker of the worker process for a client of `family`, with lazy
/// pools: nothing connects.
///
/// Errors: [`BrokerError`] when the broker config is refused.
async fn worker_broker(
    config: &PostgresConfig,
    family: ClientFamily,
) -> Result<PostgresBroker, BrokerError> {
    Ok(PostgresBroker::connect_worker_with(config)
        .await?
        .with_client_family(family))
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
        worker_profiles: manifest.profile_set(),
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
        loglevel: worker_log_level(manifest.child_launch.log_level),
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
fn registered_of(tasks: &BTreeMap<String, Execution>) -> RegisteredClasses {
    RegisteredClasses::new(
        tasks
            .iter()
            .map(|(name, execution)| (name.clone(), *execution))
            .collect::<HashMap<_, _>>(),
    )
}

#[cfg(test)]
mod tests;
