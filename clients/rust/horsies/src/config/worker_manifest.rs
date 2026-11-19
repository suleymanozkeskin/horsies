//! The worker manifest of a Rust app (Python `to_worker_manifest`,
//! `clients/python/horsies/core/worker_manifest.py`).
//!
//! The app's validated config, its task registrations, the profile that
//! the start asks for and the child launch become the one manifest that the
//! core's worker reads. Every profile of the app goes in: the core checks
//! the profile set and picks the requested profile, as for Python.
//!
//! The config validation bounds each value as Python's Pydantic models do.
//! A few values have a tighter bound in the manifest (a count that the claim
//! function takes as a Postgres `integer`, a duration that the core holds in
//! a `u32`): such a value is refused with its field path and its bound,
//! never the value ([`ManifestBoundRefused`]).

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::PathBuf;

use horsies_core::core::config::bounded::BoundedU32;
use horsies_core::core::config::worker_profile::{
    ExecutorThreadsSetting, IsolatedSetting, MemoryLimitSetting, PoolSizeSetting, ProfileRequest,
    SharedAsyncSetting, SharedPool, SharedRecycle, SharedThreadSetting, TaskLimitSetting,
};
use horsies_core::manifest::WholeSeconds;
use horsies_core::worker_manifest as wire;
use horsies_core::{DeclaredErrorCodes, Execution, RegisteredTask, RetentionDays, TaskRegistry};

use super::{core_database_url, send_manifest, session_database};
use crate::{
    AppConfig, ChildPreload, DrainTimeout, PayloadPolicy, PostgresConfig, QueueMode,
    RecoveryConfig, RetentionConfig, WorkerProfile, WorkerResilienceConfig,
};

/// Nanoseconds in a millisecond.
const NANOS_PER_MILLI: u128 = 1_000_000;

/// A config value that the config validation accepted and that is outside
/// the bound of its manifest field.
///
/// Python has no such error: its `AppConfig` validation refuses a claim
/// value above its manifest bound (`CONFIG_INVALID_PREFETCH`,
/// `CONFIG_INVALID_CLUSTER_CAP`, `CONFIG_INVALID_QUEUE_MODE`) and a payload
/// limit above `PAYLOAD_BYTES_MAX`, and `to_worker_manifest` asserts each
/// bound.
///
/// `path` names the field as `AppConfig` writes it, for example
/// `custom_queues[0].max_concurrency`. The value itself is not kept.
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
/// - `tasks`: the app's registrations; each task's class is the
///   `execution` of its options, `ISOLATED` for a task without options, and
///   its declared error codes are the codes of its code type
///   ([`task_records`]).
/// - `profile`: the `--profile` value of the start.
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
    child_launch: wire::ChildLaunchManifest,
) -> Result<wire::WorkerManifest, ManifestBoundRefused> {
    let claim = claim_manifest(config)?;
    let recovery = recovery_manifest(&config.recovery)?;
    let retention = retention_manifest(&config.retention)?;
    let resilience = resilience_manifest(&config.resilience)?;
    let payload = payload_manifest(&config.payload)?;
    Ok(wire::WorkerManifest {
        manifest_version: wire::WorkerManifestVersion::CURRENT,
        database: database_manifest(&config.broker),
        send: send_manifest(&config.broker),
        profiles: config
            .worker_profiles
            .iter()
            .map(|(name, worker_profile)| (name.clone(), profile_manifest(worker_profile)))
            .collect(),
        profile,
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
fn declared_codes(name: &str, task: &RegisteredTask) -> DeclaredErrorCodes {
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
        session_database: session_database(config),
        pgbouncer_transaction_mode: config.pgbouncer_transaction_mode,
        pool_pre_ping: config.pool_pre_ping,
        worker_pool_pre_ping: config.worker_pool_pre_ping,
        pool_timeout: WholeSeconds::from_secs(config.pool_timeout),
        pool_recycle: config.pool_recycle,
        echo: config.echo,
        tcp_keepalives: config.tcp_keepalives,
        tcp_user_timeout: config.tcp_user_timeout,
    }
}

/// `profile` with every value given. Total: each config value already has
/// the bound of its manifest field. Pure.
fn profile_manifest(profile: &WorkerProfile) -> wire::WorkerProfileManifest {
    wire::WorkerProfileManifest {
        isolated: isolated_manifest(&profile.isolated),
        shared_async: shared_async_manifest(&profile.shared_async),
        shared_thread: shared_thread_manifest(&profile.shared_thread),
        pool: wire::ParentPoolManifest {
            max_size: pool_max_size(profile.pool.max_size),
        },
        claim: wire::ClaimLimitsManifest {
            per_queue_per_pass: task_limit(profile.claim.per_queue_per_pass),
            claimed_per_worker: task_limit(profile.claim.claimed_per_worker),
        },
        drain_timeout_ms: drain_timeout_ms(profile.drain_timeout),
        preload: preload(profile.preload),
        parent_cpu: profile.parent_cpu,
    }
}

fn isolated_manifest(setting: &IsolatedSetting) -> wire::Isolated {
    match setting {
        IsolatedSetting::IsolatedChildren(children) => wire::Isolated::Children {
            count: wire::IsolatedCount(children.count),
            recycle: wire::IsolatedRecycleManifest {
                after: task_limit(children.recycle.after),
                above: memory_limit(children.recycle.above),
            },
            pool: wire::IsolatedPoolManifest {
                min_size: wire::ChildPoolMinSize(children.pool.min_size),
                max_size: wire::ChildPoolConnections(children.pool.max_size.connections),
                check: children.pool.check,
            },
            task_send_pool: wire::ChildPoolConnections(children.task_send_pool.connections),
        },
        IsolatedSetting::NoIsolatedChildren {} => wire::Isolated::NoChildren,
    }
}

fn shared_async_manifest(setting: &SharedAsyncSetting) -> wire::SharedAsync {
    match setting {
        SharedAsyncSetting::SharedAsyncChildren(children) => wire::SharedAsync::Children {
            count: wire::SharedAsyncCount(children.count),
            seats: wire::SharedAsyncSeatCount(children.seats),
            recycle: shared_recycle(&children.recycle),
            pool: shared_pool(&children.pool),
            executor: match children.executor {
                ExecutorThreadsSetting::ExecutorThreads { threads } => {
                    wire::DefaultExecutor::Threads {
                        threads: wire::DefaultExecutorThreadCount(threads),
                    }
                }
                ExecutorThreadsSetting::ExecutorThreadsFromSeats {} => {
                    wire::DefaultExecutor::FromSeats
                }
            },
            task_send_pool: wire::ChildPoolConnections(children.task_send_pool.connections),
        },
        SharedAsyncSetting::NoSharedAsyncChildren {} => wire::SharedAsync::NoChildren,
    }
}

fn shared_thread_manifest(setting: &SharedThreadSetting) -> wire::SharedThread {
    match setting {
        SharedThreadSetting::SharedThreadChildren(children) => wire::SharedThread::Children {
            count: wire::SharedThreadCount(children.count),
            seats: wire::SharedThreadSeatCount(children.seats),
            recycle: shared_recycle(&children.recycle),
            pool: shared_pool(&children.pool),
            task_send_pool: wire::ChildPoolConnections(children.task_send_pool.connections),
        },
        SharedThreadSetting::NoSharedThreadChildren {} => wire::SharedThread::NoChildren,
    }
}

fn shared_recycle(recycle: &SharedRecycle) -> wire::SharedRecycleManifest {
    wire::SharedRecycleManifest {
        after_per_seat: task_limit(recycle.after_per_seat),
        above: memory_limit(recycle.above),
    }
}

fn shared_pool(pool: &SharedPool) -> wire::SharedPoolManifest {
    wire::SharedPoolManifest {
        min_size: wire::ChildPoolMinSize(pool.min_size),
        max_size: pool_max_size(pool.max_size),
        check: pool.check,
    }
}

fn pool_max_size(setting: PoolSizeSetting) -> wire::PoolMaxSize {
    match setting {
        PoolSizeSetting::PoolSize { connections } => wire::PoolMaxSize::Connections {
            connections: wire::ChildPoolConnections(connections),
        },
        PoolSizeSetting::PoolSizeFromSeats {} => wire::PoolMaxSize::FromSeats,
    }
}

fn task_limit(setting: TaskLimitSetting) -> wire::TaskLimit {
    match setting {
        TaskLimitSetting::TaskLimit { tasks } => wire::TaskLimit::Tasks {
            tasks: wire::LimitTasks(tasks),
        },
        TaskLimitSetting::NoTaskLimit {} => wire::TaskLimit::NoLimit,
    }
}

fn memory_limit(setting: MemoryLimitSetting) -> wire::MemoryLimit {
    match setting {
        MemoryLimitSetting::MemoryLimit { mb } => wire::MemoryLimit::Megabytes {
            mb: wire::LimitMb(mb),
        },
        MemoryLimitSetting::NoMemoryLimit {} => wire::MemoryLimit::NoLimit,
    }
}

fn preload(preload: ChildPreload) -> wire::Preload {
    match preload {
        ChildPreload::Library => wire::Preload::Library,
        ChildPreload::App => wire::Preload::App,
        ChildPreload::NoPreload => wire::Preload::NoPreload,
    }
}

/// `drain_timeout` in whole milliseconds, rounded up, so the worker never
/// waits less than the profile asks (Python `drain_timeout_ms`). A
/// `DrainTimeout` is above 0 and at most 24 hours, so the count is inside
/// the manifest bound; another count is a defect.
fn drain_timeout_ms(drain_timeout: DrainTimeout) -> wire::DrainTimeoutMs {
    let milliseconds = drain_timeout.get().as_nanos().div_ceil(NANOS_PER_MILLI);
    u64::try_from(milliseconds)
        .map_err(|_| "more milliseconds than a u64 holds".to_owned())
        .and_then(wire::DrainTimeoutMs::try_from)
        .unwrap_or_else(|refused| {
            unreachable!(
                "a drain timeout above 0 and at most 24 hours fits the manifest: {refused}"
            )
        })
}

/// The queue and claim settings (Python `worker_claim_manifest`).
fn claim_manifest(config: &AppConfig) -> Result<wire::WorkerClaimManifest, ManifestBoundRefused> {
    let prefetch_buffer = wire::ClaimCount(bounded(config.prefetch_buffer, "prefetch_buffer")?);
    let claim_lease = match config.claim_lease_ms {
        None => wire::ClaimLease::WorkerDefault,
        Some(milliseconds) => wire::ClaimLease::Milliseconds {
            ms: wire::Milliseconds(bounded(milliseconds, "claim_lease_ms")?),
        },
    };
    let max_claim_renew_age_ms = wire::Milliseconds(bounded(
        config.max_claim_renew_age_ms,
        "max_claim_renew_age_ms",
    )?);
    let cluster_wide_cap = match config.cluster_wide_cap {
        None => wire::ClusterWideCap::NoLimit,
        Some(tasks) => wire::ClusterWideCap::Tasks {
            tasks: wire::LimitTasks(bounded(tasks, "cluster_wide_cap")?),
        },
    };
    let queues = queues_manifest(config)?;
    Ok(wire::WorkerClaimManifest {
        queues,
        prefetch_buffer,
        claim_lease,
        max_claim_renew_age_ms,
        cluster_wide_cap,
    })
}

/// The queues of the app; in `Custom` mode each configured queue, in the
/// order of the config.
fn queues_manifest(config: &AppConfig) -> Result<wire::Queues, ManifestBoundRefused> {
    match config.queue_mode {
        QueueMode::Default => Ok(wire::Queues::Default),
        QueueMode::Custom => {
            let queues = config
                .custom_queues
                .iter()
                .flatten()
                .enumerate()
                .map(|(position, queue)| -> Result<_, ManifestBoundRefused> {
                    let path = format!("custom_queues[{position}]");
                    Ok(wire::CustomQueueManifest {
                        name: queue.name.clone(),
                        priority: wire::QueuePriority(bounded(
                            queue.priority,
                            &format!("{path}.priority"),
                        )?),
                        max_concurrency: match queue.max_concurrency {
                            None => wire::QueueConcurrency::NoLimit,
                            Some(tasks) => wire::QueueConcurrency::Tasks {
                                tasks: wire::QueueConcurrencyLimit(bounded(
                                    tasks,
                                    &format!("{path}.max_concurrency"),
                                )?),
                            },
                        },
                    })
                })
                .collect::<Result<Vec<_>, ManifestBoundRefused>>()?;
            Ok(wire::Queues::Custom { queues })
        }
    }
}

/// The recovery section (Python `recovery_manifest`).
fn recovery_manifest(
    config: &RecoveryConfig,
) -> Result<wire::RecoveryManifest, ManifestBoundRefused> {
    Ok(wire::RecoveryManifest {
        auto_requeue_stale_claimed: config.auto_requeue_stale_claimed,
        claimed_stale_threshold_ms: wire::ClaimedStaleThresholdMs(bounded_wide(
            config.claimed_stale_threshold_ms,
            "recovery.claimed_stale_threshold_ms",
        )?),
        auto_terminate_orphaned_workflow_tasks: config.auto_terminate_orphaned_workflow_tasks,
        auto_fail_stale_running: config.auto_fail_stale_running,
        running_stale_threshold_ms: wire::StaleThresholdMs(bounded_wide(
            config.running_stale_threshold_ms,
            "recovery.running_stale_threshold_ms",
        )?),
        finalizing_stale_threshold_ms: wire::StaleThresholdMs(bounded_wide(
            config.finalizing_stale_threshold_ms,
            "recovery.finalizing_stale_threshold_ms",
        )?),
        crashed_worker_recovery_grace_ms: wire::RecoveryGraceMs(bounded_wide(
            config.crashed_worker_recovery_grace_ms,
            "recovery.crashed_worker_recovery_grace_ms",
        )?),
        phase2_quarantine_after_attempts: wire::RecoveryPassLimit(bounded(
            config.phase2_quarantine_after_attempts,
            "recovery.phase2_quarantine_after_attempts",
        )?),
        workflow_recovery_failure_limit: wire::RecoveryPassLimit(bounded(
            config.workflow_recovery_failure_limit,
            "recovery.workflow_recovery_failure_limit",
        )?),
        check_interval_ms: wire::CheckIntervalMs(bounded_wide(
            config.check_interval_ms,
            "recovery.check_interval_ms",
        )?),
        orphan_task_audit_interval_ms: wire::AuditIntervalMs(bounded_wide(
            config.orphan_task_audit_interval_ms,
            "recovery.orphan_task_audit_interval_ms",
        )?),
        runner_heartbeat_interval_ms: wire::HeartbeatIntervalMs(bounded_wide(
            config.runner_heartbeat_interval_ms,
            "recovery.runner_heartbeat_interval_ms",
        )?),
        claimer_heartbeat_interval_ms: wire::HeartbeatIntervalMs(bounded_wide(
            config.claimer_heartbeat_interval_ms,
            "recovery.claimer_heartbeat_interval_ms",
        )?),
        worker_state_snapshot_interval_ms: wire::SnapshotIntervalMs(bounded_wide(
            config.worker_state_snapshot_interval_ms,
            "recovery.worker_state_snapshot_interval_ms",
        )?),
    })
}

/// The retention section (Python `retention_manifest`): a `None` that turns
/// pruning or the paused expiry off is its own variant; a period is whole
/// days.
fn retention_manifest(
    config: &RetentionConfig,
) -> Result<wire::RetentionManifest, ManifestBoundRefused> {
    let worker_state_retention = match config.worker_state_retention_hours {
        None => wire::WorkerStateRetention::NoPruning,
        Some(hours) => wire::WorkerStateRetention::Hours {
            hours: wire::WorkerStateRetentionHours(bounded(
                hours,
                "retention.worker_state_retention_hours",
            )?),
        },
    };
    let terminal_record_retention = match config.terminal_record_retention_hours {
        None => wire::TerminalRecordRetention::NoPruning,
        Some(hours) => wire::TerminalRecordRetention::Hours {
            hours: wire::TerminalRecordRetentionHours(bounded(
                hours,
                "retention.terminal_record_retention_hours",
            )?),
        },
    };
    let paused_workflow_auto_cancel_after = match config.paused_workflow_auto_cancel_after {
        None => wire::PausedWorkflowExpiry::NoExpiry,
        Some(age) => wire::PausedWorkflowExpiry::After {
            micros: u64::try_from(age.micros())
                .ok()
                .and_then(|micros| wire::PausedAgeMicros::try_from(micros).ok())
                .ok_or_else(|| ManifestBoundRefused {
                    path: "retention.paused_workflow_auto_cancel_after".to_owned(),
                    min: 1,
                    max: wire::MAX_PAUSED_AGE_MICROS,
                })?,
        },
    };
    let retention_periods = config
        .retention_periods
        .iter()
        .enumerate()
        .map(|(position, period)| {
            period_days(*period, &format!("retention.retention_periods[{position}]"))
        })
        .collect::<Result<Vec<_>, ManifestBoundRefused>>()?;
    // In name order, so the first refusal does not depend on the map's
    // order.
    let queue_retention = config
        .queue_retention
        .iter()
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .map(|(queue, period)| -> Result<_, ManifestBoundRefused> {
            let retention = match period {
                None => wire::QueueRetention::Forever,
                Some(period) => wire::QueueRetention::Days {
                    days: period_days(*period, &format!("retention.queue_retention.{queue}"))?,
                },
            };
            Ok((queue.clone(), retention))
        })
        .collect::<Result<BTreeMap<_, _>, ManifestBoundRefused>>()?;
    Ok(wire::RetentionManifest {
        worker_state_retention,
        terminal_record_retention,
        paused_workflow_auto_cancel_after,
        history_leaf_horizon_days: wire::HistoryLeafHorizonDays(bounded(
            config.history_leaf_horizon_days,
            "retention.history_leaf_horizon_days",
        )?),
        heartbeat_leaf_horizon_hours: wire::HeartbeatLeafHorizonHours(bounded(
            config.heartbeat_leaf_horizon_hours,
            "retention.heartbeat_leaf_horizon_hours",
        )?),
        retention_periods,
        queue_retention,
        partition_maintenance_interval_s: wire::MaintenanceIntervalSeconds(bounded_wide(
            config.partition_maintenance_interval_s,
            "retention.partition_maintenance_interval_s",
        )?),
        retention_sweep_interval_s: wire::SweepIntervalSeconds(bounded_wide(
            config.retention_sweep_interval_s,
            "retention.retention_sweep_interval_s",
        )?),
        retention_delete_batch_size: wire::DeleteBatchSize(bounded(
            config.retention_delete_batch_size,
            "retention.retention_delete_batch_size",
        )?),
    })
}

/// A retention period in whole days (Python `_period_days`): a period of
/// whole days inside the period bounds, else refused with those bounds.
fn period_days(
    period: chrono::Duration,
    path: &str,
) -> Result<wire::RetentionPeriodDays, ManifestBoundRefused> {
    let days = RetentionDays::parse_period(period)
        .ok()
        .map(|days| u32::from(days.get()));
    Ok(wire::RetentionPeriodDays(within(days, path)?))
}

/// The resilience section (Python `resilience_manifest`): 0 attempts is no
/// limit.
fn resilience_manifest(
    config: &WorkerResilienceConfig,
) -> Result<wire::ResilienceManifest, ManifestBoundRefused> {
    Ok(wire::ResilienceManifest {
        db_retry_initial_ms: wire::RetryInitialMs(bounded_wide(
            config.db_retry_initial_ms,
            "resilience.db_retry_initial_ms",
        )?),
        db_retry_max_ms: wire::RetryMaxMs(bounded_wide(
            config.db_retry_max_ms,
            "resilience.db_retry_max_ms",
        )?),
        db_retry_max_attempts: match config.db_retry_max_attempts {
            0 => wire::RetryAttemptLimit::NoLimit,
            attempts => wire::RetryAttemptLimit::Attempts {
                attempts: wire::RetryAttemptCount(bounded_wide(
                    attempts,
                    "resilience.db_retry_max_attempts",
                )?),
            },
        },
        notify_poll_interval_ms: wire::PollIntervalMs(bounded_wide(
            config.notify_poll_interval_ms,
            "resilience.notify_poll_interval_ms",
        )?),
    })
}

/// The payload section (Python `payload_manifest`): no threshold is no
/// limit.
fn payload_manifest(config: &PayloadPolicy) -> Result<wire::PayloadManifest, ManifestBoundRefused> {
    Ok(wire::PayloadManifest {
        warn_bytes: payload_limit(config.warn_bytes, "payload.warn_bytes")?,
        reject_bytes: payload_limit(config.reject_bytes, "payload.reject_bytes")?,
    })
}

fn payload_limit(
    threshold: Option<u64>,
    path: &str,
) -> Result<wire::PayloadLimit, ManifestBoundRefused> {
    match threshold {
        None => Ok(wire::PayloadLimit::NoLimit),
        Some(bytes) => wire::PayloadBytes::try_from(bytes)
            .map(|bytes| wire::PayloadLimit::Bytes { bytes })
            .map_err(|_| refused(path, 1, wire::MAX_PAYLOAD_BYTES)),
    }
}

/// `value` as a bounded count of its manifest field.
fn bounded<const MIN: u32, const MAX: u32>(
    value: u32,
    path: &str,
) -> Result<BoundedU32<MIN, MAX>, ManifestBoundRefused> {
    within(Some(value), path)
}

/// `value`, a `u64` of the config, as a bounded count of its manifest field.
fn bounded_wide<const MIN: u32, const MAX: u32>(
    value: u64,
    path: &str,
) -> Result<BoundedU32<MIN, MAX>, ManifestBoundRefused> {
    within(u32::try_from(value).ok(), path)
}

/// `value` inside `MIN` to `MAX`; no value (one that has no `u32` form) or
/// a value outside is refused with the bounds of the field.
fn within<const MIN: u32, const MAX: u32>(
    value: Option<u32>,
    path: &str,
) -> Result<BoundedU32<MIN, MAX>, ManifestBoundRefused> {
    value
        .and_then(|value| BoundedU32::try_from(value).ok())
        .ok_or_else(|| refused(path, u64::from(MIN), u64::from(MAX)))
}

fn refused(path: &str, min: u64, max: u64) -> ManifestBoundRefused {
    ManifestBoundRefused {
        path: path.to_owned(),
        min,
        max,
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::ffi::OsStringExt;
    use std::sync::Arc;

    use horsies_core::core::task::fn_trait::{BlockingTaskFn, RawTaskResult, TaskMeta};
    use horsies_core::worker_manifest::parse_worker_manifest;
    use horsies_core::{CancelGraceMs, RegisteredTask, TaskOptions, TaskResult};

    use super::*;
    use crate::{CustomQueueConfig, IsolatedChildCount, IsolatedChildren};

    struct EchoTask;

    impl BlockingTaskFn for EchoTask {
        fn execute(&self, input: &[u8]) -> RawTaskResult {
            TaskResult::Ok(input.to_vec())
        }
    }

    fn task(execution: Option<Execution>) -> RegisteredTask {
        let plain = RegisteredTask::Blocking {
            task: Arc::new(EchoTask),
            meta: TaskMeta::default(),
        };
        match execution {
            None => plain,
            Some(execution) => plain.with_task_options(TaskOptions {
                task_name: "task".to_owned(),
                queue_name: None,
                good_until: None,
                auto_retry_for: None,
                retry_policy: None,
                timeout_ms: Some(5_000),
                execution,
                cancel_grace_ms: match execution {
                    Execution::SharedAsync => Some(CancelGraceMs::DEFAULT),
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
            isolated: IsolatedSetting::IsolatedChildren(IsolatedChildren::new(
                IsolatedChildCount::try_from(4).unwrap(),
            )),
            preload: ChildPreload::NoPreload,
            ..WorkerProfile::default()
        }
    }

    fn config() -> AppConfig {
        let mut config = AppConfig::for_database_url("postgresql://app@db.example/horsies");
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
            launch(),
        )
    }

    #[test]
    fn the_core_parser_reads_the_worker_manifest_back() {
        let manifest = build(&config()).unwrap();
        let text = serde_json::to_string(&manifest).unwrap();
        let read = parse_worker_manifest(&text).unwrap();
        assert_eq!(read, manifest);
        let selected = read.selected_profile().unwrap();
        assert_eq!(selected.name, "web");
        assert_eq!(selected.profile, profile());
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
        config.cluster_wide_cap = Some(20);
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
                    meta: TaskMeta::default().with_error_codes(&["RATE_LIMITED", "CARD_DECLINED"]),
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
                    meta: TaskMeta::default().with_error_codes(&["DUP", "DUP"]),
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
                Box::new(|config: &mut AppConfig| config.cluster_wide_cap = Some(u32::MAX)),
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
                    config.recovery.check_interval_ms = 4_000_000_000_000
                }),
                "recovery.check_interval_ms",
                Some("4000000000000"),
            ),
            (
                Box::new(|config: &mut AppConfig| {
                    config.retention.retention_delete_batch_size = 4_000_000
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
                    config.resilience.notify_poll_interval_ms = 4_000_000_000_000
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
        config.resilience.db_retry_max_attempts = 0;
        let manifest = build(&config).unwrap();
        assert_eq!(
            manifest.resilience.db_retry_max_attempts,
            wire::RetryAttemptLimit::NoLimit
        );
    }

    #[test]
    fn a_drain_timeout_is_rounded_up_to_whole_milliseconds() {
        let timeout = DrainTimeout::try_from(std::time::Duration::from_micros(1_500)).unwrap();
        assert_eq!(drain_timeout_ms(timeout).get(), 2);
    }
}
