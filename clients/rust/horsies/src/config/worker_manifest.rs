//! The worker manifest of a Rust app (Python `to_worker_manifest`,
//! `clients/python/horsies/core/worker_manifest.py`).
//!
//! The app's validated config, its task registrations, the profile that
//! the start asks for and the child launch become the one manifest that the
//! core's worker reads. The core builds the sections that the config gives
//! (`horsies_core::worker_manifest::config_sections`): every profile of the
//! app, the queue and claim settings, and the recovery, retention,
//! resilience and payload sections. The core checks the profile set and
//! picks the requested profile, as for Python.
//!
//! The config validation bounds each value as Python's Pydantic models do.
//! A few values have a tighter bound in the manifest (a count that the claim
//! function takes as a Postgres `integer`, a duration that the core holds in
//! a `u32`): such a value is refused with its field path and its bound,
//! never the value ([`ManifestBoundRefused`]).

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::PathBuf;

use horsies_core::core::config::worker_profile::ProfileRequest;
use horsies_core::manifest::{
    core_database_url, send_manifest, session_database_manifest, WholeSeconds,
};
use horsies_core::worker_manifest as wire;
use horsies_core::{DeclaredErrorCodes, Execution, RegisteredTask, TaskRegistry};

use crate::{AppConfig, PostgresConfig};

/// A config value that the config validation accepted and that is outside
/// the bound of its worker manifest field.
///
/// Python has no such error: its `AppConfig` validation refuses a claim
/// value above its manifest bound (`CONFIG_INVALID_PREFETCH`,
/// `CONFIG_INVALID_CLUSTER_CAP`, `CONFIG_INVALID_QUEUE_MODE`) and a payload
/// limit above `PAYLOAD_BYTES_MAX`, and `to_worker_manifest` asserts each
/// bound.
///
/// `path` names the field as `AppConfig` writes it, for example
/// `custom_queues[0].max_concurrency` (`config_path`). The value itself is
/// not kept.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{path}: outside {min} to {max}, the bound of its worker manifest field")]
pub(crate) struct ManifestBoundRefused {
    pub(crate) path: String,
    pub(crate) min: u64,
    pub(crate) max: u64,
}

/// A part of the child command that is not UTF-8: the manifest holds text.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ChildLaunchRefused {
    #[error("the path of this executable is not UTF-8: {program}")]
    ProgramNotUtf8 { program: String },
    #[error("argument {index} of the child command is not UTF-8")]
    ArgumentNotUtf8 { index: usize },
}

/// The worker manifest of `config`. Pure: no I/O.
///
/// - The profiles, the claim settings and the recovery, retention,
///   resilience and payload sections: from the core's builder
///   (`wire::config_sections`).
/// - `tasks`: the app's registrations; each task's class is the
///   `execution` of its options, `ISOLATED` for a task without options, and
///   its declared error codes are the codes of its code type
///   ([`task_records`]).
/// - `profile`: the `--profile` value of the start.
/// - `metrics_endpoint`: the `--metrics-listen` value of the start.
/// - `child_launch`: from [`rust_child_launch`].
/// - `handlers`: the app's event handlers (`EventHandlerSet::manifest`):
///   `NO_HANDLERS`, or the name and the event type of each handler. With a
///   handler the worker starts its handler child.
/// - `worker_ended_spans`: the app's switch (`Horsies::worker_ended_spans`):
///   `MAKE_SPANS` by default with the feature `otel`, with which the worker
///   starts its handler child also when the app has no handler; `NO_SPANS`
///   without the feature.
///
/// Errors: [`ManifestBoundRefused`] for the first value outside its
/// manifest bound, in this order: the claim values (`prefetch_buffer`,
/// `claim_lease_ms`, `max_claim_renew_age_ms`, `cluster_wide_cap`, each
/// custom queue), then the recovery, retention, resilience and payload
/// sections.
pub(crate) fn worker_manifest(
    config: &AppConfig,
    tasks: &TaskRegistry,
    handlers: wire::EventHandlers,
    worker_ended_spans: wire::WorkerEndedSpans,
    profile: ProfileRequest,
    metrics_endpoint: wire::MetricsEndpoint,
    child_launch: wire::ChildLaunchManifest,
) -> Result<wire::WorkerManifest, ManifestBoundRefused> {
    let wire::ConfigSections {
        profiles,
        claim,
        recovery,
        retention,
        resilience,
        payload,
    } = wire::config_sections(config).map_err(|refused| ManifestBoundRefused {
        path: config_path(&refused.field),
        min: refused.min,
        max: refused.max,
    })?;
    Ok(wire::WorkerManifest {
        manifest_version: wire::WorkerManifestVersion::CURRENT,
        database: database_manifest(&config.database),
        send: send_manifest(&config.database),
        profiles,
        profile,
        metrics_endpoint,
        claim,
        recovery,
        retention,
        resilience,
        payload,
        tasks: task_records(tasks),
        handlers,
        worker_ended_spans,
        child_launch,
    })
}

/// The path of `field` as `AppConfig` writes it: the members joined with
/// `.`, a list position as `[n]` and a map key as a member
/// (`custom_queues[0].priority`, `retention.queue_retention.bulk`). Pure.
fn config_path(field: &wire::ConfigField) -> String {
    let mut path = String::new();
    for segment in field.segments() {
        match segment {
            wire::FieldSegment::Member(name) => push_member(&mut path, name),
            wire::FieldSegment::Key(key) => push_member(&mut path, key),
            wire::FieldSegment::Position(position) => path.push_str(&format!("[{position}]")),
        }
    }
    path
}

/// `name` after `path`, with a `.` between them when `path` has a member.
fn push_member(path: &mut String, name: &str) {
    if !path.is_empty() {
        path.push('.');
    }
    path.push_str(name);
}

/// How each child of a Rust worker starts: `program` with `args` (from the
/// core's `child_command()`: this executable and the child mode argument),
/// no preload process, the Rust child program, and the log level of the
/// start. Pure.
///
/// Errors: [`ChildLaunchRefused`] for a part of the command that is not
/// UTF-8.
pub(crate) fn rust_child_launch(
    program: PathBuf,
    args: Vec<OsString>,
    log_level: wire::WorkerLogLevel,
) -> Result<wire::ChildLaunchManifest, ChildLaunchRefused> {
    let program = program.into_os_string().into_string().map_err(|program| {
        ChildLaunchRefused::ProgramNotUtf8 {
            program: PathBuf::from(program).display().to_string(),
        }
    })?;
    let args = args
        .into_iter()
        .enumerate()
        .map(|(index, argument)| {
            argument
                .into_string()
                .map_err(|_| ChildLaunchRefused::ArgumentNotUtf8 { index })
        })
        .collect::<Result<Vec<String>, ChildLaunchRefused>>()?;
    Ok(wire::ChildLaunchManifest {
        command: wire::ChildCommandManifest { program, args },
        preload: wire::PreloadLaunch::NoPreloadProgram,
        program: wire::ChildProgram::Rust,
        log_level,
    })
}

/// The record of each registered task: its class, the `execution` of its
/// options (`ISOLATED` for a task registered without options), and the
/// error codes that it declares (`RegisteredTask::error_codes`). Pure.
pub(crate) fn task_records(tasks: &TaskRegistry) -> BTreeMap<String, wire::TaskManifest> {
    tasks
        .iter()
        .map(|(name, task)| {
            let execution = task
                .task_options()
                .map_or(Execution::Isolated, |options| options.execution);
            let record = wire::TaskManifest {
                execution,
                error_codes: declared_codes(name, task),
            };
            (name.to_owned(), record)
        })
        .collect()
}

/// The declared error codes of the registered task `name`. The
/// registration refused a set that breaks a rule (`register_task`), so a
/// refusal here is a defect: it panics with the task and the cause.
pub(crate) fn declared_codes(name: &str, task: &RegisteredTask) -> DeclaredErrorCodes {
    DeclaredErrorCodes::parse(task.error_codes()).unwrap_or_else(|refused| {
        panic!("task '{name}' was registered with error codes that the registration refuses: {refused}")
    })
}

/// The database section of a worker: the pool of the worker process is the
/// resolved pool of its profile with no overflow, so the app's `pool_size`
/// and `max_overflow` do not apply. Pure.
fn database_manifest(config: &PostgresConfig) -> wire::WorkerDatabaseManifest {
    wire::WorkerDatabaseManifest {
        database_url: core_database_url(&config.database_url),
        session_database: session_database_manifest(config),
        pgbouncer_transaction_mode: config.pgbouncer_transaction_mode,
        pool_pre_ping: config.pool_pre_ping,
        worker_pool_pre_ping: config.worker_pool_pre_ping,
        pool_timeout_seconds: WholeSeconds::from_secs(config.pool_timeout_seconds),
        pool_recycle: config.pool_recycle,
        echo: config.echo,
        tcp_keepalives: config
            .tcp_keepalives
            .checked()
            .expect("Horsies::new validated the keepalive values"),
        tcp_user_timeout: config
            .tcp_user_timeout
            .checked()
            .expect("Horsies::new validated the TCP user timeout"),
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::ffi::OsStringExt;
    use std::sync::Arc;

    use horsies_core::core::task::fn_trait::{BlockingTaskFn, RawTaskResult, TaskMetadata};
    use horsies_core::worker_manifest::parse_worker_manifest;
    use horsies_core::{RegisteredTask, TaskOptions, TaskResult, DEFAULT_CANCEL_GRACE_MS};

    use super::*;
    use crate::{
        ChildPreload, ClusterCapSetting, CustomQueueConfig, IsolatedChildren, QueueMode,
        WorkerProfile,
    };

    struct EchoTask;

    impl BlockingTaskFn for EchoTask {
        fn execute(&self, input: &[u8]) -> RawTaskResult {
            TaskResult::Ok(input.to_vec())
        }
    }

    fn task(execution: Option<Execution>) -> RegisteredTask {
        let plain = RegisteredTask::Blocking {
            task: Arc::new(EchoTask),
            metadata: TaskMetadata::default(),
        };
        match execution {
            None => plain,
            Some(execution) => plain.with_task_options(TaskOptions {
                task_name: "task".to_owned(),
                queue: None,
                good_until: None,
                retry_for: None,
                retry_policy: None,
                timeout_ms: Some(5_000),
                execution,
                cancel_grace_ms: match execution {
                    Execution::SharedAsync => Some(DEFAULT_CANCEL_GRACE_MS),
                    Execution::Isolated | Execution::SharedThread => None,
                },
            }),
        }
    }

    fn registry() -> TaskRegistry {
        let mut tasks = TaskRegistry::new();
        tasks.register("plain", task(None)).unwrap();
        tasks
            .register("fan_out", task(Some(Execution::SharedAsync)))
            .unwrap();
        tasks
            .register("resize", task(Some(Execution::SharedThread)))
            .unwrap();
        tasks
    }

    fn profile() -> WorkerProfile {
        WorkerProfile {
            isolated: IsolatedChildren::new(4).into(),
            preload: ChildPreload::NoPreload,
            ..WorkerProfile::default()
        }
    }

    fn config() -> AppConfig {
        let mut config = AppConfig::from_database_url("postgresql://app@db.example/horsies");
        config.worker_profiles.insert("web".to_owned(), profile());
        config
    }

    fn launch() -> wire::ChildLaunchManifest {
        rust_child_launch(
            PathBuf::from("/srv/app/bin/worker"),
            vec![OsString::from(horsies_core::CHILD_MODE_ARGUMENT)],
            wire::WorkerLogLevel::Info,
        )
        .unwrap()
    }

    fn named(name: &str) -> ProfileRequest {
        ProfileRequest::Named {
            name: name.to_owned(),
        }
    }

    fn build(config: &AppConfig) -> Result<wire::WorkerManifest, ManifestBoundRefused> {
        worker_manifest(
            config,
            &registry(),
            wire::EventHandlers::NoHandlers,
            wire::WorkerEndedSpans::NoSpans,
            named("web"),
            wire::MetricsEndpoint::NoMetricsEndpoint,
            launch(),
        )
    }

    #[test]
    fn the_metrics_endpoint_of_the_start_reads_back() {
        let address = horsies_core::telemetry::listen::ListenAddress::parse("0.0.0.0:9100")
            .expect("a valid address");
        let endpoint = wire::MetricsEndpoint::of_address(address);
        let manifest = worker_manifest(
            &config(),
            &registry(),
            wire::EventHandlers::NoHandlers,
            wire::WorkerEndedSpans::NoSpans,
            named("web"),
            endpoint.clone(),
            launch(),
        )
        .unwrap();
        assert_eq!(manifest.metrics_endpoint, endpoint);
        let text = serde_json::to_string(&manifest).unwrap();
        assert!(
            text.contains(
                r#""metrics_endpoint":{"kind":"LISTEN_ADDRESS","host":"0.0.0.0","port":9100}"#
            ),
            "{text}"
        );
        assert_eq!(parse_worker_manifest(&text).unwrap(), manifest);
        assert_eq!(
            build(&config()).unwrap().metrics_endpoint,
            wire::MetricsEndpoint::NoMetricsEndpoint
        );
    }

    #[test]
    fn the_core_parser_reads_the_worker_manifest_back() {
        let manifest = build(&config()).unwrap();
        let text = serde_json::to_string(&manifest).unwrap();
        let read = parse_worker_manifest(&text).unwrap();
        assert_eq!(read, manifest);
        let selected = read.selected_profile().unwrap();
        assert_eq!(selected.name, "web");
        assert_eq!(WorkerProfile::from(selected.profile), profile());
    }

    #[test]
    fn a_custom_queue_config_reads_back() {
        let mut config = config();
        config.queue_mode = QueueMode::Custom;
        config.custom_queues = Some(vec![
            CustomQueueConfig {
                name: "critical".to_owned(),
                priority: 1,
                max_concurrency: Some(0),
            },
            CustomQueueConfig {
                name: "bulk".to_owned(),
                priority: 90,
                max_concurrency: None,
            },
        ]);
        config.cluster_wide_cap = ClusterCapSetting::ClusterCap { tasks: 20 };
        config.claim_lease_ms = Some(30_000);
        let manifest = build(&config).unwrap();
        let text = serde_json::to_string(&manifest).unwrap();
        assert_eq!(parse_worker_manifest(&text).unwrap(), manifest);
        let wire::Queues::Custom { queues } = &manifest.claim.queues else {
            panic!("custom queues expected");
        };
        assert_eq!(queues[0].name, "critical");
        assert_eq!(queues[1].max_concurrency, wire::QueueConcurrency::NoLimit);
    }

    #[test]
    fn each_task_has_the_class_of_its_options() {
        let records = task_records(&registry());
        assert_eq!(records["plain"].execution, Execution::Isolated);
        assert_eq!(records["fan_out"].execution, Execution::SharedAsync);
        assert_eq!(records["resize"].execution, Execution::SharedThread);
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn each_task_carries_the_codes_of_its_code_type_sorted() {
        let mut tasks = registry();
        tasks
            .register(
                "charge",
                RegisteredTask::Blocking {
                    task: Arc::new(EchoTask),
                    metadata: TaskMetadata::default()
                        .with_error_codes(&["RATE_LIMITED", "CARD_DECLINED"]),
                },
            )
            .unwrap();
        let records = task_records(&tasks);
        let codes: Vec<&str> = records["charge"]
            .error_codes
            .iter()
            .map(|code| code.as_str())
            .collect();
        assert_eq!(codes, ["CARD_DECLINED", "RATE_LIMITED"]);
        assert!(records["plain"].error_codes.is_empty());
    }

    #[test]
    #[should_panic(expected = "task 'charge' was registered with error codes")]
    fn a_set_that_the_registration_refuses_is_a_defect() {
        let mut tasks = TaskRegistry::new();
        tasks
            .register(
                "charge",
                RegisteredTask::Blocking {
                    task: Arc::new(EchoTask),
                    metadata: TaskMetadata::default().with_error_codes(&["DUP", "DUP"]),
                },
            )
            .unwrap();
        let _records = task_records(&tasks);
    }

    #[test]
    fn the_rust_child_launch_has_no_preload_and_the_rust_program() {
        let launch = launch();
        assert_eq!(launch.command.program, "/srv/app/bin/worker");
        assert_eq!(launch.command.args, vec![horsies_core::CHILD_MODE_ARGUMENT]);
        assert_eq!(launch.preload, wire::PreloadLaunch::NoPreloadProgram);
        assert_eq!(launch.program, wire::ChildProgram::Rust);
    }

    #[test]
    fn a_program_path_that_is_not_utf8_is_refused() {
        let program = PathBuf::from(OsString::from_vec(vec![b'/', 0xff]));
        let refused = rust_child_launch(program, vec![], wire::WorkerLogLevel::Info).unwrap_err();
        assert!(matches!(refused, ChildLaunchRefused::ProgramNotUtf8 { .. }));
        let refused = rust_child_launch(
            PathBuf::from("/bin/worker"),
            vec![OsString::from_vec(vec![0xff])],
            wire::WorkerLogLevel::Info,
        )
        .unwrap_err();
        assert_eq!(refused, ChildLaunchRefused::ArgumentNotUtf8 { index: 0 });
    }

    /// The change of one case of the bound test below.
    type ConfigChange = Box<dyn Fn(&mut AppConfig)>;

    /// Each value outside its manifest bound is refused with its path and
    /// the bound of its field. The text is the path and the bound only, so
    /// it never holds the value. A case whose value has a text of its own
    /// also checks that text absent; `0` and a duration have none, as a
    /// bound can hold the digit `0` (`claim_lease_ms` has the bound `1 to
    /// 4294967295`, which is the value of `prefetch_buffer`).
    #[test]
    fn a_value_outside_its_manifest_bound_is_refused_with_its_path() {
        let cases: Vec<(ConfigChange, &str, Option<&str>)> = vec![
            (
                Box::new(|config: &mut AppConfig| config.prefetch_buffer = u32::MAX),
                "prefetch_buffer",
                Some("4294967295"),
            ),
            (
                Box::new(|config: &mut AppConfig| config.claim_lease_ms = Some(0)),
                "claim_lease_ms",
                None,
            ),
            (
                Box::new(|config: &mut AppConfig| {
                    config.cluster_wide_cap = ClusterCapSetting::ClusterCap { tasks: u32::MAX }
                }),
                "cluster_wide_cap",
                Some("4294967295"),
            ),
            (
                Box::new(|config: &mut AppConfig| {
                    config.queue_mode = QueueMode::Custom;
                    config.custom_queues = Some(vec![CustomQueueConfig {
                        name: "critical".to_owned(),
                        priority: 4_000_000,
                        max_concurrency: None,
                    }]);
                }),
                "custom_queues[0].priority",
                Some("4000000"),
            ),
            (
                Box::new(|config: &mut AppConfig| {
                    config.recovery.check_interval_ms = 4_000_000_000_000;
                }),
                "recovery.check_interval_ms",
                Some("4000000000000"),
            ),
            (
                Box::new(|config: &mut AppConfig| {
                    config.retention.retention_delete_batch_size = 4_000_000;
                }),
                "retention.retention_delete_batch_size",
                Some("4000000"),
            ),
            (
                Box::new(|config: &mut AppConfig| {
                    config.retention.retention_periods = vec![chrono::Duration::hours(36)];
                }),
                "retention.retention_periods[0]",
                None,
            ),
            (
                Box::new(|config: &mut AppConfig| {
                    config.retention.queue_retention = std::collections::HashMap::from([(
                        "bulk".to_owned(),
                        Some(chrono::Duration::hours(36)),
                    )]);
                }),
                "retention.queue_retention.bulk",
                None,
            ),
            (
                Box::new(|config: &mut AppConfig| {
                    config.resilience.notify_poll_interval_ms = 4_000_000_000_000;
                }),
                "resilience.notify_poll_interval_ms",
                Some("4000000000000"),
            ),
            (
                Box::new(|config: &mut AppConfig| config.payload.warn_bytes = Some(0)),
                "payload.warn_bytes",
                None,
            ),
        ];
        for (change, path, value_text) in cases {
            let mut config = config();
            change(&mut config);
            let refused = build(&config).unwrap_err();
            assert_eq!(refused.path, path);
            let text = refused.to_string();
            assert_eq!(
                text,
                format!(
                    "{path}: outside {} to {}, the bound of its worker manifest field",
                    refused.min, refused.max
                )
            );
            if let Some(value) = value_text {
                assert!(!text.contains(value), "{text}");
            }
        }
    }

    #[test]
    fn no_retry_attempt_limit_is_its_own_variant() {
        let mut config = config();
        config.resilience.database_retry_max_attempts = 0;
        let manifest = build(&config).unwrap();
        assert_eq!(
            manifest.resilience.database_retry_max_attempts,
            wire::RetryAttemptLimit::NoLimit
        );
    }

    #[test]
    fn a_drain_timeout_is_rounded_up_to_whole_milliseconds() {
        let mut config = config();
        let profile = WorkerProfile {
            drain_timeout: std::time::Duration::from_micros(1_500),
            ..profile()
        };
        config.worker_profiles.insert("web".to_owned(), profile);
        let manifest = build(&config).unwrap();
        assert_eq!(manifest.profiles["web"].drain_timeout_ms.get(), 2);
    }
}
