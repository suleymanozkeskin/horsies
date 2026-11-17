//! The worker run of the app (Python `horsies worker`, `core_run.py`): the
//! static check, the worker manifest of a named profile, and the core's
//! worker parent, which runs task bodies in children of this binary.
//!
//! The worker parent runs on the core's process runtime: the claim loop,
//! the outcome writes, the heartbeats, the reaper and the children's
//! supervision. `SIGTERM` and `SIGINT` ask it to stop: the claim loop ends,
//! the children drain within the profile's drain timeout, and the run
//! answers [`WorkerStopped`].

use std::sync::Arc;

use horsies_core::core::config::worker_profile::ProfileRequest;
use horsies_core::process_runtime::{spawn_on_process_runtime, ProcessRuntimeRefusal};
use horsies_core::worker_manifest::WorkerManifest;
use horsies_core::{
    child_command, run_end_of, BuilderHookName, RunPhase, Worker, WorkerPlan, WorkerPlanRefused,
    WorkerRunEnd, WorkerStop, WorkflowSpecRegistry,
};

pub use horsies_core::worker_manifest::WorkerLogLevel;
pub use horsies_core::{WorkerBootCause, WorkerFailCause};

use crate::check::{CheckReport, CheckScope, WORKER_STARTUP};
use crate::config::{rust_child_launch, worker_manifest};
use crate::signals::stop_on_signals;
use crate::{AppRole, Horsies, HorsiesError};

/// How a worker starts: the profile of `AppConfig.worker_profiles` that it
/// runs, and the log level of its children.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerStart {
    profile: ProfileRequest,
    log_level: WorkerLogLevel,
}

impl WorkerStart {
    /// A worker of the profile `name`, with children that log at `INFO`.
    pub fn profile(name: impl Into<String>) -> Self {
        Self {
            profile: ProfileRequest::Named { name: name.into() },
            log_level: WorkerLogLevel::Info,
        }
    }

    /// This start with children that log at `level`. The worker process
    /// logs through the subscriber that the binary installed.
    pub fn with_log_level(self, level: WorkerLogLevel) -> Self {
        Self {
            log_level: level,
            ..self
        }
    }

    /// A start that names no profile: the CLI with no `--profile`, which
    /// the core refuses with Python's report.
    pub(crate) fn without_profile(log_level: WorkerLogLevel) -> Self {
        Self {
            profile: ProfileRequest::NotGiven,
            log_level,
        }
    }
}

/// The worker ran until a stop request (a signal) and drained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerStopped;

/// Why a worker run ended without a stop request. Python's `horsies
/// worker` exits 1 for each.
#[derive(Debug, Clone, thiserror::Error)]
pub enum WorkerRunError {
    /// The static check refused the app (Python's `worker-startup` report).
    /// Nothing started.
    #[error("{0}")]
    CheckRefused(CheckReport),
    /// A config value is outside the bound of its worker manifest field.
    /// `path` names the field as `AppConfig` writes it; the value is not
    /// kept. Nothing started.
    #[error("{path}: outside {min} to {max}, the bound of its worker manifest field")]
    ManifestBound { path: String, min: u64, max: u64 },
    /// The command that starts this binary in child mode cannot be built:
    /// the path of this executable could not be read, or a part of the
    /// command is not UTF-8. Nothing started.
    #[error("the child command cannot be built: {cause}")]
    ChildCommandRefused { cause: String },
    /// The profile set fails its checks, or the start names no profile or
    /// one that the app does not define (Python's report lines). Nothing
    /// started.
    #[error("the worker profile is refused: {}", join_lines(.refusals))]
    ProfileRefused { refusals: Vec<HorsiesError> },
    /// The connection settings fail the broker config check (for example
    /// PgBouncer transaction mode with no session URL). Nothing started.
    #[error("the connection settings are refused: {cause}")]
    ConnectionSettingsRefused { cause: String },
    /// The core's process runtime cannot run the worker. Nothing started.
    #[error("{0}")]
    RuntimeUnavailable(ProcessRuntimeRefusal),
    /// The start failed; the worker claimed nothing. `message` names the
    /// subject and the cause.
    #[error("worker start failed ({}): {message}", .cause.as_str())]
    BootFailed {
        cause: WorkerBootCause,
        message: String,
    },
    /// The worker was ready, then failed; its running tasks drained first.
    #[error("worker failed ({}): {message}", .cause.as_str())]
    Failed {
        cause: WorkerFailCause,
        message: String,
    },
}

impl Horsies {
    /// Run a worker of `start`'s profile until `SIGTERM` or `SIGINT` (Python
    /// `horsies worker --profile <name>`). Every task body runs in a child:
    /// this binary, started again in child mode, so `main` must hand child
    /// mode to [`Horsies::serve_child`] (or call `horsies::cli::run`).
    ///
    /// In order: the role becomes `Worker`; the static check without the
    /// schedules; the worker manifest of the app; the profile and the
    /// connection settings; then the core's worker parent on the process
    /// runtime, which connects with the resilience retry, starts the
    /// children and claims.
    ///
    /// Errors: [`WorkerRunError`]. The run returns `Ok` only at a stop
    /// request.
    pub async fn run_worker(mut self, start: WorkerStart) -> Result<WorkerStopped, WorkerRunError> {
        self.set_role(AppRole::Worker);
        self.check_scope(CheckScope::WithoutSchedules, WORKER_STARTUP)
            .map_err(WorkerRunError::CheckRefused)?;
        let manifest = self.worker_manifest_of(start)?;
        let plan = WorkerPlan::of_manifest(&manifest).map_err(plan_refusal)?;
        let stop = WorkerStop::new();
        let signal_stop = stop.clone();
        let signals = spawn_on_process_runtime(stop_on_signals("worker", move || {
            signal_stop.request_stop()
        }))
        .map_err(WorkerRunError::RuntimeUnavailable)?;
        let run = spawn_on_process_runtime(run_plan(plan, stop));
        let end = match run {
            Ok(run) => run.await,
            Err(refusal) => {
                signals.abort();
                return Err(WorkerRunError::RuntimeUnavailable(refusal));
            }
        };
        signals.abort();
        match end {
            Ok(end) => outcome_of(end),
            Err(join_error) => match join_error.try_into_panic() {
                Ok(panic) => std::panic::resume_unwind(panic),
                Err(join_error) => panic!(
                    "the worker run on the process runtime was cancelled, which only a runtime \
                     shutdown does, and the process runtime is never shut down: {join_error}"
                ),
            },
        }
    }

    /// The worker manifest of this app for `start`. Reads the path of this
    /// executable; no other I/O.
    fn worker_manifest_of(&self, start: WorkerStart) -> Result<WorkerManifest, WorkerRunError> {
        let command = child_command().map_err(|error| WorkerRunError::ChildCommandRefused {
            cause: format!("the path of this executable could not be read: {error}"),
        })?;
        let launch = rust_child_launch(command.program, command.args, start.log_level).map_err(
            |refused| WorkerRunError::ChildCommandRefused {
                cause: refused.to_string(),
            },
        )?;
        worker_manifest(self.config(), self.tasks(), start.profile, launch).map_err(|refused| {
            WorkerRunError::ManifestBound {
                path: refused.path,
                min: refused.min,
                max: refused.max,
            }
        })
    }
}

/// Build the worker of `plan` and run it to its end. A worker that does
/// not build is a failed start; its error is logged with its cause.
async fn run_plan(plan: WorkerPlan, stop: WorkerStop) -> WorkerRunEnd {
    match Worker::from_plan(plan, workflow_specs()).await {
        Ok(worker) => worker.with_stop(&stop).run_to_end().await,
        Err(error) => {
            tracing::error!("the worker did not build: {error}");
            run_end_of(Err(error), RunPhase::Starting)
        }
    }
}

/// The workflow specs that the worker's child workflow starts build from:
/// the app registers no workflow yet (the workflow part of the client,
/// #1406), so the registry is empty, with the hook name of the Rust API.
fn workflow_specs() -> Arc<WorkflowSpecRegistry> {
    Arc::new(WorkflowSpecRegistry::new(BuilderHookName::SPEC_BUILDER))
}

/// The error of a refused plan. Pure.
fn plan_refusal(refused: WorkerPlanRefused) -> WorkerRunError {
    match refused {
        WorkerPlanRefused::Profile(refused) => WorkerRunError::ProfileRefused {
            refusals: refused.errors(),
        },
        WorkerPlanRefused::ConnectionSettings { cause } => {
            WorkerRunError::ConnectionSettingsRefused { cause }
        }
    }
}

/// The outcome of a run end. Pure.
fn outcome_of(end: WorkerRunEnd) -> Result<WorkerStopped, WorkerRunError> {
    match end {
        WorkerRunEnd::Stopped => Ok(WorkerStopped),
        WorkerRunEnd::BootFailed { cause, message } => {
            Err(WorkerRunError::BootFailed { cause, message })
        }
        WorkerRunEnd::Failed { cause, message } => Err(WorkerRunError::Failed { cause, message }),
    }
}

/// The lines of a profile refusal, joined. Pure.
fn join_lines(refusals: &[HorsiesError]) -> String {
    refusals
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<String>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use horsies_core::core::config::worker_profile::ProfileSelectionRefused;

    use super::*;
    use crate::{AppConfig, ChildPreload, ErrorCode, WorkerProfile};

    fn app() -> Horsies {
        let mut config = AppConfig::for_database_url("postgresql://localhost/horsies");
        let _previous = config.worker_profiles.insert(
            "web".to_owned(),
            WorkerProfile {
                preload: ChildPreload::NoPreload,
                ..WorkerProfile::default()
            },
        );
        Horsies::new(config).unwrap()
    }

    #[test]
    fn a_start_names_its_profile_and_logs_at_info_unless_told() {
        let start = WorkerStart::profile("web");
        assert_eq!(
            start.profile,
            ProfileRequest::Named {
                name: "web".to_owned()
            }
        );
        assert_eq!(start.log_level, WorkerLogLevel::Info);
        let debug = start.with_log_level(WorkerLogLevel::Debug);
        assert_eq!(debug.log_level, WorkerLogLevel::Debug);
        assert_eq!(
            WorkerStart::without_profile(WorkerLogLevel::Info).profile,
            ProfileRequest::NotGiven
        );
    }

    #[test]
    fn the_manifest_names_the_profile_and_this_binary_in_child_mode() {
        let manifest = app()
            .worker_manifest_of(WorkerStart::profile("web").with_log_level(WorkerLogLevel::Error))
            .unwrap();
        assert_eq!(
            manifest.profile,
            ProfileRequest::Named {
                name: "web".to_owned()
            }
        );
        let launch = &manifest.child_launch;
        assert_eq!(launch.command.args, vec![crate::CHILD_MODE_ARGUMENT]);
        assert_eq!(launch.log_level, WorkerLogLevel::Error);
        assert!(WorkerPlan::of_manifest(&manifest).is_ok());
    }

    #[test]
    fn an_unknown_profile_is_refused_with_python_s_report() {
        let manifest = app()
            .worker_manifest_of(WorkerStart::profile("batch"))
            .unwrap();
        let refused = plan_refusal(WorkerPlan::of_manifest(&manifest).unwrap_err());
        let WorkerRunError::ProfileRefused { refusals } = refused else {
            panic!("an unknown profile is a profile refusal");
        };
        assert_eq!(refusals[0].code, Some(ErrorCode::CliInvalidArgs));
        assert!(refusals[0].message.contains("unknown worker profile"));
    }

    #[test]
    fn no_profile_is_refused_with_the_defined_names() {
        let refused = plan_refusal(WorkerPlanRefused::Profile(
            ProfileSelectionRefused::NotGiven {
                defined: vec!["web".to_owned()],
            },
        ));
        assert!(refused.to_string().contains("--profile is required"));
    }

    #[test]
    fn a_connection_refusal_keeps_its_cause() {
        let refused = plan_refusal(WorkerPlanRefused::ConnectionSettings {
            cause: "no session URL".to_owned(),
        });
        assert!(matches!(
            refused,
            WorkerRunError::ConnectionSettingsRefused { ref cause } if cause == "no session URL"
        ));
    }

    #[test]
    fn a_run_end_maps_to_its_outcome() {
        assert!(matches!(
            outcome_of(WorkerRunEnd::Stopped),
            Ok(WorkerStopped)
        ));
        let boot = outcome_of(WorkerRunEnd::BootFailed {
            cause: WorkerBootCause::ChildHookFailed,
            message: "hook".to_owned(),
        })
        .unwrap_err();
        assert!(boot.to_string().contains("CHILD_HOOK_FAILED"));
        let failed = outcome_of(WorkerRunEnd::Failed {
            cause: WorkerFailCause::PoolFailed,
            message: "pool".to_owned(),
        })
        .unwrap_err();
        assert!(matches!(
            failed,
            WorkerRunError::Failed {
                cause: WorkerFailCause::PoolFailed,
                ..
            }
        ));
    }
}
