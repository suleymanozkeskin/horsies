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
use horsies_core::worker_manifest::{MetricsEndpoint, WorkerManifest};
use horsies_core::{
    child_command, run_end_of, ClientFamily, ProcessExit, RunPhase, Worker, WorkerPlan,
    WorkerPlanRefused, WorkerRunEnd, WorkerStop,
};

pub use horsies_core::telemetry::listen::{ListenAddress, ListenAddressRefused};
pub use horsies_core::worker_manifest::WorkerLogLevel;
pub use horsies_core::{WorkerBootCause, WorkerFailCause};

use crate::check::{CheckReport, CheckScope, WORKER_STARTUP};
use crate::config::{rust_child_launch, worker_manifest};
use crate::process_state::{runtime_for, RuntimeStartFailed};
use crate::signals::stop_on_signals;
use crate::workflow::WorkflowRegistry;
use crate::{AppRole, Horsies, HorsiesError};

/// How a worker starts: the profile of `AppConfig.worker_profiles` that it
/// runs, the log level of its children, and its metrics endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerStart {
    profile: ProfileRequest,
    log_level: WorkerLogLevel,
    metrics_endpoint: MetricsEndpoint,
}

impl WorkerStart {
    /// A worker of the profile `name`, with children that log at `INFO`.
    pub fn profile(name: impl Into<String>) -> Self {
        Self {
            profile: ProfileRequest::Named { name: name.into() },
            log_level: WorkerLogLevel::Info,
            metrics_endpoint: MetricsEndpoint::NoMetricsEndpoint,
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

    /// This start with a metrics endpoint on `address` (`--metrics-listen
    /// HOST:PORT`): the worker serves its metrics at `GET /metrics` and its
    /// health probes at `GET /livez` and `GET /readyz`. There is no default
    /// address. A bind that fails does not stop the worker: the endpoint
    /// binds again with a backoff. The core serves it with its feature
    /// `metrics-endpoint`, which the `cli` feature turns on; a core without
    /// it ends the start with `BootFailed`.
    pub fn with_metrics_endpoint(self, address: ListenAddress) -> Self {
        Self {
            metrics_endpoint: MetricsEndpoint::of_address(address),
            ..self
        }
    }

    /// A start that names no profile: the CLI with no `--profile`, which
    /// the core refuses with Python's report.
    pub(crate) fn without_profile(log_level: WorkerLogLevel) -> Self {
        Self {
            profile: ProfileRequest::NotGiven,
            log_level,
            metrics_endpoint: MetricsEndpoint::NoMetricsEndpoint,
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
    /// The OS refused the threads of the core's process runtime. Nothing
    /// started; a later run tries the start again.
    #[error("{0}")]
    RuntimeUnavailable(RuntimeStartFailed),
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
    /// schedules; the worker manifest of the app, with its event handlers;
    /// the profile and the connection settings; then the core's worker
    /// parent on the process runtime, which connects with the resilience
    /// retry, starts the children and claims. An app with an event handler
    /// ([`Horsies::on_event`]), or with the spans of the attempts that the
    /// worker ended (the default with the feature `otel`), also gets a
    /// handler child: the worker waits at most 30 s for it before the first
    /// claim, and a handler child that fails never stops the worker.
    ///
    /// Errors: [`WorkerRunError`]. The run returns `Ok` only at a stop
    /// request.
    ///
    /// # Panics
    ///
    /// After the checks, in a process that was forked without `exec` after
    /// the core's process runtime started in its parent. Nothing started.
    pub async fn run_worker(mut self, start: WorkerStart) -> Result<WorkerStopped, WorkerRunError> {
        self.set_role(AppRole::Worker);
        self.check_scope(CheckScope::WithoutSchedules, WORKER_STARTUP)
            .map_err(WorkerRunError::CheckRefused)?;
        let manifest = self.worker_manifest_of(start)?;
        let plan = WorkerPlan::of_manifest(&manifest).map_err(plan_refusal)?;
        let runtime = runtime_for(format_args!("Horsies::run_worker"))
            .map_err(WorkerRunError::RuntimeUnavailable)?;
        let stop = WorkerStop::new();
        let signal_stop = stop.clone();
        let signals = runtime.handle().spawn(stop_on_signals("worker", move || {
            signal_stop.request_stop();
        }));
        let end = runtime
            .handle()
            .spawn(run_plan(plan, Arc::clone(self.workflows()), stop))
            .await;
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
        worker_manifest(
            self.config(),
            self.tasks(),
            self.event_handlers().manifest(),
            self.worker_ended_spans(),
            start.profile,
            start.metrics_endpoint,
            launch,
        )
        .map_err(|refused| WorkerRunError::ManifestBound {
            path: refused.path,
            min: refused.min,
            max: refused.max,
        })
    }
}

/// Build the worker of `plan` and run it to its end; the child workflows
/// that its steps start build through `workflows`, the app's workflow
/// registry. A worker that does not build is a failed start; its error is
/// logged with its cause.
async fn run_plan(
    plan: WorkerPlan,
    workflows: Arc<WorkflowRegistry>,
    stop: WorkerStop,
) -> WorkerRunEnd {
    // The fail-closed exits 77 and 79 run the exit handlers of the process,
    // as before.
    match Worker::from_plan(plan, workflows, ClientFamily::Rust, ProcessExit::Standard).await {
        Ok(worker) => worker.with_stop(&stop).run_to_end().await,
        Err(error) => {
            tracing::error!("the worker did not build: {error}");
            run_end_of(Err(error), RunPhase::Starting)
        }
    }
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
    use crate::{AppConfig, ChildPreload, ErrorCode, IsolatedChildren, WorkerProfile};

    /// An app with the profile `web`: two isolated children. The config
    /// check refuses a profile that starts no children.
    fn app() -> Horsies {
        let mut config = AppConfig::for_database_url("postgresql://localhost/horsies");
        let _previous = config.worker_profiles.insert(
            "web".to_owned(),
            WorkerProfile {
                isolated: IsolatedChildren::new(2).into(),
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

    /// The manifest carries the worker-ended span switch of the app:
    /// `MakeSpans` by default with the feature `otel`, `NoSpans` without
    /// it, and `NoSpans` after `set_worker_ended_spans(NoSpans)`.
    #[test]
    fn the_manifest_carries_the_worker_ended_span_switch_of_the_app() {
        use horsies_core::worker_manifest::WorkerEndedSpans;

        let manifest = app()
            .worker_manifest_of(WorkerStart::profile("web"))
            .unwrap();
        assert_eq!(
            manifest.worker_ended_spans,
            crate::telemetry::DEFAULT_WORKER_ENDED_SPANS
        );
        #[cfg(feature = "otel")]
        {
            assert_eq!(manifest.worker_ended_spans, WorkerEndedSpans::MakeSpans);
            let mut off = app();
            off.set_worker_ended_spans(WorkerEndedSpans::NoSpans);
            let manifest = off.worker_manifest_of(WorkerStart::profile("web")).unwrap();
            assert_eq!(manifest.worker_ended_spans, WorkerEndedSpans::NoSpans);
            assert!(WorkerPlan::of_manifest(&manifest).is_ok());
        }
        #[cfg(not(feature = "otel"))]
        assert_eq!(manifest.worker_ended_spans, WorkerEndedSpans::NoSpans);
    }

    #[test]
    fn a_start_with_a_metrics_endpoint_puts_its_address_in_the_manifest() {
        let address = ListenAddress::parse("0.0.0.0:9100").expect("a valid address");
        let manifest = app()
            .worker_manifest_of(WorkerStart::profile("web").with_metrics_endpoint(address.clone()))
            .unwrap();
        assert_eq!(
            manifest.metrics_endpoint,
            MetricsEndpoint::of_address(address)
        );
        let plan = WorkerPlan::of_manifest(&manifest).unwrap();
        assert!(matches!(
            plan.metrics_endpoint,
            horsies_core::MetricsEndpointPlan::ListenAddress { ref address, .. }
                if address.to_string() == "0.0.0.0:9100"
        ));
        let manifest = app()
            .worker_manifest_of(WorkerStart::profile("web"))
            .unwrap();
        assert_eq!(
            manifest.metrics_endpoint,
            MetricsEndpoint::NoMetricsEndpoint
        );
    }

    /// The worker of `config` with the profile `web`: the app accepts the
    /// config, the manifest asks for no preload process, and the plan builds.
    fn assert_web_starts_with_no_preload(config: AppConfig) {
        let app = Horsies::new(config).unwrap();
        let manifest = app.worker_manifest_of(WorkerStart::profile("web")).unwrap();
        assert_eq!(
            manifest.profiles["web"].preload,
            horsies_core::worker_manifest::Preload::NoPreload
        );
        assert!(WorkerPlan::of_manifest(&manifest).is_ok());
    }

    /// A profile built from `..WorkerProfile::default()` with no `preload`
    /// has `ChildPreload::NoPreload`, and its worker starts.
    #[test]
    fn a_default_profile_starts_with_no_preload() {
        let profile = WorkerProfile {
            isolated: IsolatedChildren::new(2).into(),
            ..WorkerProfile::default()
        };
        assert_eq!(profile.preload, ChildPreload::NoPreload);
        let mut config = AppConfig::for_database_url("postgresql://localhost/horsies");
        let _previous = config.worker_profiles.insert("web".to_owned(), profile);
        assert_web_starts_with_no_preload(config);
    }

    /// A TOML profile without the `preload` key reads as
    /// `ChildPreload::NoPreload`, and its worker starts. `toml` is built
    /// with the `cli` feature.
    #[cfg(feature = "cli")]
    #[test]
    fn a_toml_profile_without_preload_starts() {
        let config: AppConfig = toml::from_str(
            r#"
[broker]
database_url = "postgresql://localhost/horsies"

[worker_profiles.web]
isolated = { kind = "isolated_children", count = 2 }
"#,
        )
        .unwrap();
        assert_eq!(
            config.worker_profiles["web"].preload,
            ChildPreload::NoPreload
        );
        assert_web_starts_with_no_preload(config);
    }

    #[test]
    fn an_unknown_profile_is_refused_with_python_s_report() {
        let manifest = app()
            .worker_manifest_of(WorkerStart::profile("batch"))
            .unwrap();
        let Err(refused) = WorkerPlan::of_manifest(&manifest) else {
            panic!("an unknown profile is refused");
        };
        let refused = plan_refusal(refused);
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
