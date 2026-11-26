//! The sections of a worker manifest that the app config gives (the model:
//! `worker_manifest` of the Rust client, `config/worker_manifest.rs`, and
//! Python's `to_worker_manifest`).
//!
//! - The input is the core's `AppConfig`, read from the config text that
//!   `Horsies.create` accepted, so every field that the TypeScript config
//!   left out has the core's default: each default has one source.
//! - The database and send sections come from the producer manifest of the
//!   same config (the `sends` area's `checkAppConfig`), so the URL form and
//!   the reservation window have one source in this addon. The pool of the
//!   worker process is the resolved pool of its profile with no overflow, so
//!   the app's `poolSize` and `maxOverflow` do not apply.
//! - Every profile has `NO_PRELOAD`: the TypeScript config has no preload
//!   field, and a JavaScript worker starts each child with `exec`.
//! - The config check bounds each value as the core's config rules do. A
//!   few manifest fields have a tighter bound (a count that the claim
//!   function takes as a Postgres `integer`, a duration that the core holds
//!   in a `u32`). Such a value is refused with its field path as the
//!   TypeScript config writes it and the bound, never the value
//!   ([`ManifestBoundRefused`]).

use std::collections::BTreeMap;

use horsies_core::core::config::bounded::BoundedU32;
use horsies_core::core::config::worker_profile::{
    DrainTimeout, ExecutorThreadsSetting, IsolatedSetting, MemoryLimitSetting, PoolSizeSetting,
    SharedAsyncSetting, SharedPool, SharedRecycle, SharedThreadSetting, TaskLimitSetting,
};
use horsies_core::core::config::WorkerProfile;
use horsies_core::manifest::ProducerManifest;
use horsies_core::worker_manifest as wire;
use horsies_core::{
    AppConfig, PayloadPolicy, PostgresConfig, QueueMode, RecoveryConfig, RetentionConfig,
    RetentionDays, WorkerResilienceConfig,
};

/// Nanoseconds in a millisecond.
const NANOS_PER_MILLI: u128 = 1_000_000;

/// A config value that the config check accepted and that is outside the
/// bound of its worker manifest field. `path` names the field as the
/// TypeScript config writes it (`customQueues[0].maxConcurrency.tasks`);
/// the value is not kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestBoundRefused {
    pub path: String,
    pub min: u64,
    pub max: u64,
}

/// The sections of a worker manifest that the app config gives.
#[derive(Debug, Clone)]
pub struct ConfigSections {
    pub database: wire::WorkerDatabaseManifest,
    pub send: horsies_core::manifest::SendManifest,
    pub profiles: BTreeMap<String, wire::WorkerProfileManifest>,
    pub claim: wire::WorkerClaimManifest,
    pub recovery: wire::RecoveryManifest,
    pub retention: wire::RetentionManifest,
    pub resilience: wire::ResilienceManifest,
    pub payload: wire::PayloadManifest,
}

/// The config sections of `config`, whose producer manifest is `producer`.
/// Pure: no I/O.
///
/// Errors: [`ManifestBoundRefused`] for the first value outside its
/// manifest bound, in this order: the claim values (`prefetchBuffer`,
/// `claimLeaseMs`, `maxClaimRenewAgeMs`, `clusterWideCap`, each custom
/// queue), then the recovery, retention, resilience and payload sections.
pub fn config_sections(
    config: &AppConfig,
    producer: &ProducerManifest,
) -> Result<ConfigSections, ManifestBoundRefused> {
    let claim = claim_manifest(config)?;
    let recovery = recovery_manifest(&config.recovery)?;
    let retention = retention_manifest(&config.retention)?;
    let resilience = resilience_manifest(&config.resilience)?;
    let payload = payload_manifest(&config.payload)?;
    Ok(ConfigSections {
        database: database_manifest(producer, &config.broker),
        send: producer.send,
        profiles: config
            .worker_profiles
            .iter()
            .map(|(name, profile)| (name.clone(), profile_manifest(profile)))
            .collect(),
        claim,
        recovery,
        retention,
        resilience,
        payload,
    })
}

/// The database section of a worker: the URL forms and the connection
/// settings of the producer manifest, and the pre-ping rule of the worker
/// process from the config. Pure.
fn database_manifest(
    producer: &ProducerManifest,
    broker: &PostgresConfig,
) -> wire::WorkerDatabaseManifest {
    let database = &producer.database;
    wire::WorkerDatabaseManifest {
        database_url: database.database_url.clone(),
        session_database: database.session_database.clone(),
        pgbouncer_transaction_mode: database.pgbouncer_transaction_mode,
        pool_pre_ping: database.pool_pre_ping,
        worker_pool_pre_ping: broker.worker_pool_pre_ping,
        pool_timeout: database.pool_timeout,
        pool_recycle: database.pool_recycle,
        echo: database.echo,
        tcp_keepalives: database.tcp_keepalives,
        tcp_user_timeout: database.tcp_user_timeout,
    }
}

/// `profile` with every value given and no preload. Total: each config
/// value already has the bound of its manifest field. Pure.
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
        preload: wire::Preload::NoPreload,
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
    let prefetch_buffer = wire::ClaimCount(bounded(config.prefetch_buffer, "prefetchBuffer")?);
    let claim_lease = match config.claim_lease_ms {
        None => wire::ClaimLease::WorkerDefault,
        Some(milliseconds) => wire::ClaimLease::Milliseconds {
            ms: wire::Milliseconds(bounded(milliseconds, "claimLeaseMs")?),
        },
    };
    let max_claim_renew_age_ms = wire::Milliseconds(bounded(
        config.max_claim_renew_age_ms,
        "maxClaimRenewAgeMs",
    )?);
    let cluster_wide_cap = match config.cluster_wide_cap {
        None => wire::ClusterWideCap::NoLimit,
        Some(tasks) => wire::ClusterWideCap::Tasks {
            tasks: wire::LimitTasks(bounded(tasks, "clusterWideCap.tasks")?),
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
                    let path = format!("customQueues[{position}]");
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
                                    &format!("{path}.maxConcurrency.tasks"),
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
            "recovery.claimedStaleThresholdMs",
        )?),
        auto_terminate_orphaned_workflow_tasks: config.auto_terminate_orphaned_workflow_tasks,
        auto_fail_stale_running: config.auto_fail_stale_running,
        running_stale_threshold_ms: wire::StaleThresholdMs(bounded_wide(
            config.running_stale_threshold_ms,
            "recovery.runningStaleThresholdMs",
        )?),
        finalizing_stale_threshold_ms: wire::StaleThresholdMs(bounded_wide(
            config.finalizing_stale_threshold_ms,
            "recovery.finalizingStaleThresholdMs",
        )?),
        crashed_worker_recovery_grace_ms: wire::RecoveryGraceMs(bounded_wide(
            config.crashed_worker_recovery_grace_ms,
            "recovery.crashedWorkerRecoveryGraceMs",
        )?),
        phase2_quarantine_after_attempts: wire::RecoveryPassLimit(bounded(
            config.phase2_quarantine_after_attempts,
            "recovery.phase2QuarantineAfterAttempts",
        )?),
        workflow_recovery_failure_limit: wire::RecoveryPassLimit(bounded(
            config.workflow_recovery_failure_limit,
            "recovery.workflowRecoveryFailureLimit",
        )?),
        check_interval_ms: wire::CheckIntervalMs(bounded_wide(
            config.check_interval_ms,
            "recovery.checkIntervalMs",
        )?),
        orphan_task_audit_interval_ms: wire::AuditIntervalMs(bounded_wide(
            config.orphan_task_audit_interval_ms,
            "recovery.orphanTaskAuditIntervalMs",
        )?),
        runner_heartbeat_interval_ms: wire::HeartbeatIntervalMs(bounded_wide(
            config.runner_heartbeat_interval_ms,
            "recovery.runnerHeartbeatIntervalMs",
        )?),
        claimer_heartbeat_interval_ms: wire::HeartbeatIntervalMs(bounded_wide(
            config.claimer_heartbeat_interval_ms,
            "recovery.claimerHeartbeatIntervalMs",
        )?),
        worker_state_snapshot_interval_ms: wire::SnapshotIntervalMs(bounded_wide(
            config.worker_state_snapshot_interval_ms,
            "recovery.workerStateSnapshotIntervalMs",
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
                "retention.workerStateRetention.hours",
            )?),
        },
    };
    let terminal_record_retention = match config.terminal_record_retention_hours {
        None => wire::TerminalRecordRetention::NoPruning,
        Some(hours) => wire::TerminalRecordRetention::Hours {
            hours: wire::TerminalRecordRetentionHours(bounded(
                hours,
                "retention.terminalRecordRetention.hours",
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
                    path: "retention.pausedWorkflowAutoCancelAfter.age".to_owned(),
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
            period_days(
                *period,
                &format!("retention.retentionPeriods[{position}].days"),
            )
        })
        .collect::<Result<Vec<_>, ManifestBoundRefused>>()?;
    let queue_retention = queue_retention_manifest(config)?;
    Ok(wire::RetentionManifest {
        worker_state_retention,
        terminal_record_retention,
        paused_workflow_auto_cancel_after,
        history_leaf_horizon_days: wire::HistoryLeafHorizonDays(bounded(
            config.history_leaf_horizon_days,
            "retention.historyLeafHorizonDays",
        )?),
        heartbeat_leaf_horizon_hours: wire::HeartbeatLeafHorizonHours(bounded(
            config.heartbeat_leaf_horizon_hours,
            "retention.heartbeatLeafHorizonHours",
        )?),
        retention_periods,
        queue_retention,
        partition_maintenance_interval_s: wire::MaintenanceIntervalSeconds(bounded_wide(
            config.partition_maintenance_interval_s,
            "retention.partitionMaintenanceIntervalSeconds",
        )?),
        retention_sweep_interval_s: wire::SweepIntervalSeconds(bounded_wide(
            config.retention_sweep_interval_s,
            "retention.retentionSweepIntervalSeconds",
        )?),
        retention_delete_batch_size: wire::DeleteBatchSize(bounded(
            config.retention_delete_batch_size,
            "retention.retentionDeleteBatchSize",
        )?),
    })
}

/// The retention of each named queue, in name order, so the first refusal
/// does not depend on the order of the map.
fn queue_retention_manifest(
    config: &RetentionConfig,
) -> Result<BTreeMap<String, wire::QueueRetention>, ManifestBoundRefused> {
    config
        .queue_retention
        .iter()
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .map(|(queue, period)| -> Result<_, ManifestBoundRefused> {
            let retention = match period {
                None => wire::QueueRetention::Forever,
                Some(period) => wire::QueueRetention::Days {
                    days: period_days(
                        *period,
                        &format!(
                            "retention.queueRetention[{}].days",
                            serde_json::Value::String(queue.clone())
                        ),
                    )?,
                },
            };
            Ok((queue.clone(), retention))
        })
        .collect()
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
            "resilience.dbRetryInitialMs",
        )?),
        db_retry_max_ms: wire::RetryMaxMs(bounded_wide(
            config.db_retry_max_ms,
            "resilience.dbRetryMaxMs",
        )?),
        db_retry_max_attempts: match config.db_retry_max_attempts {
            0 => wire::RetryAttemptLimit::NoLimit,
            attempts => wire::RetryAttemptLimit::Attempts {
                attempts: wire::RetryAttemptCount(bounded_wide(
                    attempts,
                    "resilience.dbRetryMaxAttempts.attempts",
                )?),
            },
        },
        notify_poll_interval_ms: wire::PollIntervalMs(bounded_wide(
            config.notify_poll_interval_ms,
            "resilience.notifyPollIntervalMs",
        )?),
    })
}

/// The payload section (Python `payload_manifest`): no threshold is no
/// limit.
fn payload_manifest(config: &PayloadPolicy) -> Result<wire::PayloadManifest, ManifestBoundRefused> {
    Ok(wire::PayloadManifest {
        warn_bytes: payload_limit(config.warn_bytes, "payload.warnBytes.bytes")?,
        reject_bytes: payload_limit(config.reject_bytes, "payload.rejectBytes.bytes")?,
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
    use horsies_core::manifest::parse_producer_manifest;

    use super::*;
    use crate::call::thrown::Envelope;
    use crate::sends::checks::{check_app_config, AppConfigCheck};

    /// The JSON text of an app config with `rest` after its broker section.
    fn config_text(rest: &str) -> String {
        format!(r#"{{"broker": {{"database_url": "postgres://127.0.0.1:55432/horsies"}}{rest}}}"#)
    }

    /// The config and the producer manifest of `text`, as `prepareWorker`
    /// reads them.
    fn read(text: &str) -> (AppConfig, ProducerManifest) {
        let config: AppConfig = serde_json::from_str(text).expect("the config text reads");
        let Envelope::Value(AppConfigCheck::Accepted { manifest_json, .. }) =
            check_app_config(text.to_owned())
        else {
            panic!("the config is accepted");
        };
        let producer = parse_producer_manifest(&manifest_json).expect("the manifest reads");
        (config, producer)
    }

    fn sections(rest: &str) -> Result<ConfigSections, ManifestBoundRefused> {
        let (config, producer) = read(&config_text(rest));
        config_sections(&config, &producer)
    }

    fn profile(rest: &str, name: &str) -> wire::WorkerProfileManifest {
        sections(rest)
            .expect("the sections are inside their bounds")
            .profiles
            .remove(name)
            .expect("the profile is in the manifest")
    }

    #[test]
    fn the_database_section_takes_the_url_form_of_the_producer_manifest() {
        let built = sections("").expect("the default sections");
        assert_eq!(
            built.database.database_url.expose(),
            "postgresql://127.0.0.1:55432/horsies"
        );
        assert!(!built.database.worker_pool_pre_ping);
    }

    #[test]
    fn an_isolated_profile_has_its_children_and_no_preload() {
        let built = profile(
            r#", "worker_profiles": {"io": {"isolated": {"kind": "isolated_children", "count": 2}}}"#,
            "io",
        );
        assert!(matches!(
            built.isolated,
            wire::Isolated::Children {
                count: wire::IsolatedCount(_),
                ..
            }
        ));
        assert_eq!(built.shared_async, wire::SharedAsync::NoChildren);
        assert_eq!(built.shared_thread, wire::SharedThread::NoChildren);
        assert_eq!(built.preload, wire::Preload::NoPreload);
    }

    #[test]
    fn a_shared_async_profile_with_no_isolated_children_has_seats_and_the_executor_from_seats() {
        let built = profile(
            r#", "worker_profiles": {"io": {"isolated": {"kind": "no_isolated_children"}, "shared_async": {"kind": "shared_async_children", "count": 1, "seats": 50}}}"#,
            "io",
        );
        assert_eq!(built.isolated, wire::Isolated::NoChildren);
        let wire::SharedAsync::Children { executor, .. } = built.shared_async else {
            panic!("the profile has shared async children");
        };
        assert_eq!(executor, wire::DefaultExecutor::FromSeats);
    }

    #[test]
    fn a_shared_thread_profile_has_its_children() {
        let built = profile(
            r#", "worker_profiles": {"cpu": {"isolated": {"kind": "no_isolated_children"}, "shared_thread": {"kind": "shared_thread_children", "count": 1, "seats": 4}}}"#,
            "cpu",
        );
        assert!(matches!(
            built.shared_thread,
            wire::SharedThread::Children { .. }
        ));
    }

    #[test]
    fn each_recycle_variant_maps_to_its_manifest_limit() {
        let built = profile(
            r#", "worker_profiles": {"io": {"isolated": {"kind": "isolated_children", "count": 1, "recycle": {"after": {"kind": "no_task_limit"}, "above": {"kind": "memory_limit", "mb": 512}}}, "shared_async": {"kind": "shared_async_children", "count": 1, "seats": 2, "recycle": {"after_per_seat": {"kind": "task_limit", "tasks": 7}, "above": {"kind": "no_memory_limit"}}}}}"#,
            "io",
        );
        let wire::Isolated::Children { recycle, .. } = built.isolated else {
            panic!("the profile has isolated children");
        };
        assert_eq!(recycle.after, wire::TaskLimit::NoLimit);
        assert!(matches!(recycle.above, wire::MemoryLimit::Megabytes { .. }));
        let wire::SharedAsync::Children { recycle, .. } = built.shared_async else {
            panic!("the profile has shared async children");
        };
        assert!(matches!(
            recycle.after_per_seat,
            wire::TaskLimit::Tasks { .. }
        ));
        assert_eq!(recycle.above, wire::MemoryLimit::NoLimit);
    }

    /// The change of one case of the bound test below.
    type ConfigChange = Box<dyn Fn(&mut AppConfig)>;

    /// Each value outside its manifest bound is refused with its path as the
    /// TypeScript config writes it and the bound of its field, never the
    /// value. The config is changed after its check, as a value that the
    /// config rules accept and the manifest bound refuses would be.
    #[test]
    fn a_value_outside_its_manifest_bound_is_refused_with_its_path_and_no_value() {
        let cases: Vec<(ConfigChange, &str)> = vec![
            (
                Box::new(|config: &mut AppConfig| config.prefetch_buffer = u32::MAX),
                "prefetchBuffer",
            ),
            (
                Box::new(|config: &mut AppConfig| config.claim_lease_ms = Some(0)),
                "claimLeaseMs",
            ),
            (
                Box::new(|config: &mut AppConfig| config.cluster_wide_cap = Some(u32::MAX)),
                "clusterWideCap.tasks",
            ),
            (
                Box::new(|config: &mut AppConfig| {
                    config.queue_mode = QueueMode::Custom;
                    config.custom_queues = Some(vec![horsies_core::CustomQueueConfig {
                        name: "critical".to_owned(),
                        priority: 4_000_000,
                        max_concurrency: None,
                    }]);
                }),
                "customQueues[0].priority",
            ),
            (
                Box::new(|config: &mut AppConfig| {
                    config.recovery.check_interval_ms = 4_000_000_000_000;
                }),
                "recovery.checkIntervalMs",
            ),
            (
                Box::new(|config: &mut AppConfig| {
                    config.retention.retention_periods = vec![chrono::Duration::hours(36)];
                }),
                "retention.retentionPeriods[0].days",
            ),
            (
                Box::new(|config: &mut AppConfig| {
                    config.resilience.notify_poll_interval_ms = 4_000_000_000_000;
                }),
                "resilience.notifyPollIntervalMs",
            ),
            (
                Box::new(|config: &mut AppConfig| config.payload.warn_bytes = Some(0)),
                "payload.warnBytes.bytes",
            ),
        ];
        for (change, path) in cases {
            let (mut config, producer) = read(&config_text(""));
            change(&mut config);
            let refused = config_sections(&config, &producer).expect_err(path);
            assert_eq!(refused.path, path);
            assert!(refused.min <= refused.max, "{refused:?}");
        }
    }

    #[test]
    fn no_retry_attempt_limit_is_its_own_variant() {
        let (mut config, producer) = read(&config_text(""));
        config.resilience.db_retry_max_attempts = 0;
        let built = config_sections(&config, &producer).expect("inside every bound");
        assert_eq!(
            built.resilience.db_retry_max_attempts,
            wire::RetryAttemptLimit::NoLimit
        );
    }

    #[test]
    fn a_drain_timeout_is_rounded_up_to_whole_milliseconds() {
        let timeout = DrainTimeout::try_from(std::time::Duration::from_micros(1_500))
            .expect("1.5 ms is a drain timeout");
        assert_eq!(drain_timeout_ms(timeout).get(), 2);
    }

    #[test]
    fn the_default_sections_are_inside_every_bound() {
        let built = sections("").expect("the core defaults fit the manifest");
        assert_eq!(built.claim.queues, wire::Queues::Default);
        assert_eq!(built.claim.cluster_wide_cap, wire::ClusterWideCap::NoLimit);
    }
}
