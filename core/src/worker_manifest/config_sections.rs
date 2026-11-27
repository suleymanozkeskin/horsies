//! The sections of a worker manifest that the app config gives: every
//! profile, the queue and claim settings, and the recovery, retention,
//! resilience and payload sections ([`config_sections`]). Each client builds
//! them here from the core's [`AppConfig`]. The database and send sections,
//! the profile request, the tasks, the handlers, the span switch, the
//! metrics endpoint and the child launch stay with each client.
//!
//! A few config values have a tighter bound in the manifest (a count that
//! the claim function takes as a Postgres `integer`, a duration that the
//! core holds in a `u32`). Such a value is refused with its field
//! ([`ConfigField`]) and the bound of its manifest field, never the value
//! ([`ManifestBoundRefused`]). Each client renders the field in the path
//! form of its own config.

use std::collections::BTreeMap;

use crate::core::config::bounded::BoundedU32;
use crate::core::config::worker_profile::{
    checked_worker_profiles, input, ChildPreload, DrainTimeout, ExecutorThreadsSetting,
    IsolatedSetting, MemoryLimitSetting, PoolSizeSetting, SharedAsyncSetting, SharedPool,
    SharedRecycle, SharedThreadSetting, TaskLimitSetting, WorkerProfile,
};
use crate::core::config::{
    AppConfig, ClusterCapSetting, CustomQueueConfig, PausedWorkflowAge, PayloadPolicy, QueueMode,
    RecoveryConfig, RetentionConfig, WorkerResilienceConfig,
};
use crate::core::history::periods::RetentionDays;
use crate::worker_manifest as wire;

/// Nanoseconds in a millisecond.
const NANOS_PER_MILLI: u128 = 1_000_000;

/// The smallest payload threshold, in bytes ([`wire::PayloadBytes`]).
const MIN_PAYLOAD_BYTES: u64 = 1;

/// The sections of a worker manifest that the app config gives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigSections {
    /// Every profile of the config, by name.
    pub profiles: BTreeMap<String, wire::WorkerProfileManifest>,
    pub claim: wire::WorkerClaimManifest,
    pub recovery: wire::RecoveryManifest,
    pub retention: wire::RetentionManifest,
    pub resilience: wire::ResilienceManifest,
    pub payload: wire::PayloadManifest,
}

/// A config value outside the bound of its worker manifest field.
///
/// - `field`: the config field of the value. The value is not kept.
/// - `min`, `max`: the bound of the manifest field; both are inside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestBoundRefused {
    pub field: ConfigField,
    pub min: u64,
    pub max: u64,
}

/// A field of the app config whose value has a tighter bound in the worker
/// manifest. [`ConfigField::segments`] gives its path in the core's config
/// text; a client renders it in the path form of its own config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigField {
    PrefetchBuffer,
    ClaimLeaseMs,
    MaxClaimRenewAgeMs,
    ClusterWideCap,
    /// A field of the custom queue at `position` of `custom_queues`, from 0.
    CustomQueue {
        position: usize,
        field: CustomQueueField,
    },
    Recovery(RecoveryField),
    Retention(RetentionField),
    Resilience(ResilienceField),
    Payload(PayloadField),
}

/// A field of one custom queue (`CustomQueueConfig`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CustomQueueField {
    Priority,
    MaxConcurrency,
}

/// A field of the recovery section (`RecoveryConfig`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryField {
    ClaimedStaleThresholdMs,
    RunningStaleThresholdMs,
    FinalizingStaleThresholdMs,
    CrashedWorkerRecoveryGraceMs,
    FinalizationQuarantineAfterAttempts,
    WorkflowRecoveryFailureLimit,
    CheckIntervalMs,
    OrphanTaskAuditIntervalMs,
    RunnerHeartbeatIntervalMs,
    ClaimerHeartbeatIntervalMs,
    WorkerStateSnapshotIntervalMs,
}

/// A field of the retention section (`RetentionConfig`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionField {
    WorkerStateRetentionHours,
    TerminalRecordRetentionHours,
    HistoryLeafHorizonDays,
    HeartbeatLeafHorizonHours,
    /// The period at `position` of `retention_periods`, from 0.
    RetentionPeriod {
        position: usize,
    },
    /// The period of the queue `queue` in `queue_retention`.
    QueueRetention {
        queue: String,
    },
    PartitionMaintenanceIntervalSeconds,
    RetentionSweepIntervalSeconds,
    RetentionDeleteBatchSize,
}

/// A field of the resilience section (`WorkerResilienceConfig`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResilienceField {
    DatabaseRetryInitialMs,
    DatabaseRetryMaxMs,
    DatabaseRetryMaxAttempts,
    NotifyPollIntervalMs,
}

/// A field of the payload section (`PayloadPolicy`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadField {
    WarnBytes,
    RejectBytes,
}

/// One step of the path of a [`ConfigField`] in the core's config text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldSegment<'a> {
    /// A member of an object, by its name in the config text.
    Member(&'static str),
    /// An item of a list, by its position from 0.
    Position(usize),
    /// An entry of a map, by its key.
    Key(&'a str),
}

impl ConfigField {
    /// The path of the field in the core's config text, from the top
    /// object: for the priority of the first custom queue,
    /// `[Member("custom_queues"), Position(0), Member("priority")]`. Pure.
    pub fn segments(&self) -> Vec<FieldSegment<'_>> {
        match self {
            Self::PrefetchBuffer => vec![FieldSegment::Member("prefetch_buffer")],
            Self::ClaimLeaseMs => vec![FieldSegment::Member("claim_lease_ms")],
            Self::MaxClaimRenewAgeMs => vec![FieldSegment::Member("max_claim_renew_age_ms")],
            Self::ClusterWideCap => vec![FieldSegment::Member("cluster_wide_cap")],
            Self::CustomQueue { position, field } => vec![
                FieldSegment::Member("custom_queues"),
                FieldSegment::Position(*position),
                FieldSegment::Member(field.config_name()),
            ],
            Self::Recovery(field) => vec![
                FieldSegment::Member("recovery"),
                FieldSegment::Member(field.config_name()),
            ],
            Self::Retention(field) => field.segments(),
            Self::Resilience(field) => vec![
                FieldSegment::Member("resilience"),
                FieldSegment::Member(field.config_name()),
            ],
            Self::Payload(field) => vec![
                FieldSegment::Member("payload"),
                FieldSegment::Member(field.config_name()),
            ],
        }
    }
}

impl CustomQueueField {
    const fn config_name(self) -> &'static str {
        match self {
            Self::Priority => "priority",
            Self::MaxConcurrency => "max_concurrency",
        }
    }
}

impl RecoveryField {
    const fn config_name(self) -> &'static str {
        match self {
            Self::ClaimedStaleThresholdMs => "claimed_stale_threshold_ms",
            Self::RunningStaleThresholdMs => "running_stale_threshold_ms",
            Self::FinalizingStaleThresholdMs => "finalizing_stale_threshold_ms",
            Self::CrashedWorkerRecoveryGraceMs => "crashed_worker_recovery_grace_ms",
            Self::FinalizationQuarantineAfterAttempts => "finalization_quarantine_after_attempts",
            Self::WorkflowRecoveryFailureLimit => "workflow_recovery_failure_limit",
            Self::CheckIntervalMs => "check_interval_ms",
            Self::OrphanTaskAuditIntervalMs => "orphan_task_audit_interval_ms",
            Self::RunnerHeartbeatIntervalMs => "runner_heartbeat_interval_ms",
            Self::ClaimerHeartbeatIntervalMs => "claimer_heartbeat_interval_ms",
            Self::WorkerStateSnapshotIntervalMs => "worker_state_snapshot_interval_ms",
        }
    }
}

impl RetentionField {
    /// The path from the top object, through `retention`.
    fn segments(&self) -> Vec<FieldSegment<'_>> {
        let section = FieldSegment::Member("retention");
        let member = |name| vec![section, FieldSegment::Member(name)];
        match self {
            Self::WorkerStateRetentionHours => member("worker_state_retention_hours"),
            Self::TerminalRecordRetentionHours => member("terminal_record_retention_hours"),
            Self::HistoryLeafHorizonDays => member("history_leaf_horizon_days"),
            Self::HeartbeatLeafHorizonHours => member("heartbeat_leaf_horizon_hours"),
            Self::RetentionPeriod { position } => vec![
                section,
                FieldSegment::Member("retention_periods"),
                FieldSegment::Position(*position),
            ],
            Self::QueueRetention { queue } => vec![
                section,
                FieldSegment::Member("queue_retention"),
                FieldSegment::Key(queue.as_str()),
            ],
            Self::PartitionMaintenanceIntervalSeconds => {
                member("partition_maintenance_interval_seconds")
            }
            Self::RetentionSweepIntervalSeconds => member("retention_sweep_interval_seconds"),
            Self::RetentionDeleteBatchSize => member("retention_delete_batch_size"),
        }
    }
}

impl ResilienceField {
    const fn config_name(self) -> &'static str {
        match self {
            Self::DatabaseRetryInitialMs => "database_retry_initial_ms",
            Self::DatabaseRetryMaxMs => "database_retry_max_ms",
            Self::DatabaseRetryMaxAttempts => "database_retry_max_attempts",
            Self::NotifyPollIntervalMs => "notify_poll_interval_ms",
        }
    }
}

impl PayloadField {
    const fn config_name(self) -> &'static str {
        match self {
            Self::WarnBytes => "warn_bytes",
            Self::RejectBytes => "reject_bytes",
        }
    }
}

/// The config sections of `config`. Pure: no I/O.
///
/// - Every profile of the config goes in, in its checked form
///   ([`checked_worker_profiles`]), whose bounds are the bounds of the
///   manifest fields: the worker checks the profile set and picks the
///   profile that the start asks for.
/// - For the other sections, the builder does not depend on
///   `AppConfig::validate`: it checks each manifest bound itself.
///
/// Errors: [`ManifestBoundRefused`] for the first value outside its
/// manifest bound, in this order: the claim values (`prefetch_buffer`,
/// `claim_lease_ms`, `max_claim_renew_age_ms`, `cluster_wide_cap`, each
/// custom queue in config order), then the recovery, retention (each mapped
/// queue in name order), resilience and payload sections.
///
/// # Panics
///
/// When a profile has a value outside its bound. `AppConfig::validate`
/// refuses each such value with its path, and a client builds the sections
/// only of a config that validation accepted (the Rust `Horsies::new`, the
/// TypeScript `Horsies.create`), so a value outside its bound here is a
/// defect of the client.
pub fn config_sections(config: &AppConfig) -> Result<ConfigSections, ManifestBoundRefused> {
    let claim = claim_manifest(config)?;
    let recovery = recovery_manifest(&config.recovery)?;
    let retention = retention_manifest(&config.retention)?;
    let resilience = resilience_manifest(&config.resilience)?;
    let payload = payload_manifest(&config.payload)?;
    Ok(ConfigSections {
        profiles: checked_profiles(&config.worker_profiles)
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

/// Every profile of the config in its checked form, by name. Pure.
///
/// Panics when a profile has a value outside its bound, with the path and
/// the bound of each such value: a defect of the client
/// ([`config_sections`]).
fn checked_profiles(
    profiles: &BTreeMap<String, input::WorkerProfile>,
) -> BTreeMap<String, WorkerProfile> {
    match checked_worker_profiles(profiles) {
        Ok(checked) => checked,
        Err(refusals) => panic!(
            "the worker manifest is built from a config that AppConfig::validate refuses, a \
             defect of the client: {}",
            refusals
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; "),
        ),
    }
}

/// `profile` with every value given. Total: each value of a checked
/// profile has the bound of its manifest field. Pure.
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
                pre_ping: children.pool.pre_ping,
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
        pre_ping: pool.pre_ping,
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
    let prefetch_buffer = wire::ClaimCount(bounded(
        config.prefetch_buffer,
        ConfigField::PrefetchBuffer,
    )?);
    let claim_lease = match config.claim_lease_ms {
        None => wire::ClaimLease::WorkerDefault,
        Some(milliseconds) => wire::ClaimLease::Milliseconds {
            ms: wire::Milliseconds(bounded(milliseconds, ConfigField::ClaimLeaseMs)?),
        },
    };
    let max_claim_renew_age_ms = wire::Milliseconds(bounded(
        config.max_claim_renew_age_ms,
        ConfigField::MaxClaimRenewAgeMs,
    )?);
    // The manifest keeps its own tags (`TASKS`, `NO_LIMIT`) for the two
    // config states.
    let cluster_wide_cap = match config.cluster_wide_cap {
        ClusterCapSetting::NoClusterCap => wire::ClusterWideCap::NoLimit,
        ClusterCapSetting::ClusterCap { tasks } => wire::ClusterWideCap::Tasks {
            tasks: wire::LimitTasks(bounded(tasks, ConfigField::ClusterWideCap)?),
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
                .map(|(position, queue)| custom_queue_manifest(position, queue))
                .collect::<Result<Vec<_>, ManifestBoundRefused>>()?;
            Ok(wire::Queues::Custom { queues })
        }
    }
}

/// The custom queue at `position` of the config: its priority, then its
/// concurrency limit.
fn custom_queue_manifest(
    position: usize,
    queue: &CustomQueueConfig,
) -> Result<wire::CustomQueueManifest, ManifestBoundRefused> {
    let queue_field = |field| ConfigField::CustomQueue { position, field };
    Ok(wire::CustomQueueManifest {
        name: queue.name.clone(),
        priority: wire::QueuePriority(bounded(
            queue.priority,
            queue_field(CustomQueueField::Priority),
        )?),
        max_concurrency: match queue.max_concurrency {
            None => wire::QueueConcurrency::NoLimit,
            Some(tasks) => wire::QueueConcurrency::Tasks {
                tasks: wire::QueueConcurrencyLimit(bounded(
                    tasks,
                    queue_field(CustomQueueField::MaxConcurrency),
                )?),
            },
        },
    })
}

/// The recovery section (Python `recovery_manifest`).
fn recovery_manifest(
    config: &RecoveryConfig,
) -> Result<wire::RecoveryManifest, ManifestBoundRefused> {
    let field = ConfigField::Recovery;
    Ok(wire::RecoveryManifest {
        requeue_stale_claimed: config.requeue_stale_claimed,
        claimed_stale_threshold_ms: wire::ClaimedStaleThresholdMs(bounded_wide(
            config.claimed_stale_threshold_ms,
            field(RecoveryField::ClaimedStaleThresholdMs),
        )?),
        terminate_orphaned_workflow_tasks: config.terminate_orphaned_workflow_tasks,
        fail_stale_running: config.fail_stale_running,
        running_stale_threshold_ms: wire::StaleThresholdMs(bounded_wide(
            config.running_stale_threshold_ms,
            field(RecoveryField::RunningStaleThresholdMs),
        )?),
        finalizing_stale_threshold_ms: wire::StaleThresholdMs(bounded_wide(
            config.finalizing_stale_threshold_ms,
            field(RecoveryField::FinalizingStaleThresholdMs),
        )?),
        crashed_worker_recovery_grace_ms: wire::RecoveryGraceMs(bounded_wide(
            config.crashed_worker_recovery_grace_ms,
            field(RecoveryField::CrashedWorkerRecoveryGraceMs),
        )?),
        finalization_quarantine_after_attempts: wire::RecoveryPassLimit(bounded(
            config.finalization_quarantine_after_attempts,
            field(RecoveryField::FinalizationQuarantineAfterAttempts),
        )?),
        workflow_recovery_failure_limit: wire::RecoveryPassLimit(bounded(
            config.workflow_recovery_failure_limit,
            field(RecoveryField::WorkflowRecoveryFailureLimit),
        )?),
        check_interval_ms: wire::CheckIntervalMs(bounded_wide(
            config.check_interval_ms,
            field(RecoveryField::CheckIntervalMs),
        )?),
        orphan_task_audit_interval_ms: wire::AuditIntervalMs(bounded_wide(
            config.orphan_task_audit_interval_ms,
            field(RecoveryField::OrphanTaskAuditIntervalMs),
        )?),
        runner_heartbeat_interval_ms: wire::HeartbeatIntervalMs(bounded_wide(
            config.runner_heartbeat_interval_ms,
            field(RecoveryField::RunnerHeartbeatIntervalMs),
        )?),
        claimer_heartbeat_interval_ms: wire::HeartbeatIntervalMs(bounded_wide(
            config.claimer_heartbeat_interval_ms,
            field(RecoveryField::ClaimerHeartbeatIntervalMs),
        )?),
        worker_state_snapshot_interval_ms: wire::SnapshotIntervalMs(bounded_wide(
            config.worker_state_snapshot_interval_ms,
            field(RecoveryField::WorkerStateSnapshotIntervalMs),
        )?),
    })
}

/// The retention section (Python `retention_manifest`): a `None` that turns
/// pruning or the paused expiry off is its own variant; a period is whole
/// days.
fn retention_manifest(
    config: &RetentionConfig,
) -> Result<wire::RetentionManifest, ManifestBoundRefused> {
    let field = ConfigField::Retention;
    let worker_state_retention = match config.worker_state_retention_hours {
        None => wire::WorkerStateRetention::NoPruning,
        Some(hours) => wire::WorkerStateRetention::Hours {
            hours: wire::WorkerStateRetentionHours(bounded(
                hours,
                field(RetentionField::WorkerStateRetentionHours),
            )?),
        },
    };
    let terminal_record_retention = match config.terminal_record_retention_hours {
        None => wire::TerminalRecordRetention::NoPruning,
        Some(hours) => wire::TerminalRecordRetention::Hours {
            hours: wire::TerminalRecordRetentionHours(bounded(
                hours,
                field(RetentionField::TerminalRecordRetentionHours),
            )?),
        },
    };
    let paused_workflow_cancel_after = match config.paused_workflow_cancel_after {
        None => wire::PausedWorkflowExpiry::NoExpiry,
        Some(age) => wire::PausedWorkflowExpiry::After {
            micros: paused_age_micros(age),
        },
    };
    let retention_periods = config
        .retention_periods
        .iter()
        .enumerate()
        .map(|(position, period)| {
            period_days(*period, field(RetentionField::RetentionPeriod { position }))
        })
        .collect::<Result<Vec<_>, ManifestBoundRefused>>()?;
    let queue_retention = queue_retention_manifest(config)?;
    Ok(wire::RetentionManifest {
        worker_state_retention,
        terminal_record_retention,
        paused_workflow_cancel_after,
        history_leaf_horizon_days: wire::HistoryLeafHorizonDays(bounded(
            config.history_leaf_horizon_days,
            field(RetentionField::HistoryLeafHorizonDays),
        )?),
        heartbeat_leaf_horizon_hours: wire::HeartbeatLeafHorizonHours(bounded(
            config.heartbeat_leaf_horizon_hours,
            field(RetentionField::HeartbeatLeafHorizonHours),
        )?),
        retention_periods,
        queue_retention,
        partition_maintenance_interval_seconds: wire::MaintenanceIntervalSeconds(bounded_wide(
            config.partition_maintenance_interval_seconds,
            field(RetentionField::PartitionMaintenanceIntervalSeconds),
        )?),
        retention_sweep_interval_seconds: wire::SweepIntervalSeconds(bounded_wide(
            config.retention_sweep_interval_seconds,
            field(RetentionField::RetentionSweepIntervalSeconds),
        )?),
        retention_delete_batch_size: wire::DeleteBatchSize(bounded(
            config.retention_delete_batch_size,
            field(RetentionField::RetentionDeleteBatchSize),
        )?),
    })
}

/// `age` in microseconds. A `PausedWorkflowAge` is 1 to
/// `PausedWorkflowAge::MAX_MICROS` microseconds, the bound of the manifest
/// field ([`wire::MAX_PAUSED_AGE_MICROS`]); another count is a defect.
fn paused_age_micros(age: PausedWorkflowAge) -> wire::PausedAgeMicros {
    u64::try_from(age.micros())
        .map_err(|_| "a count below zero".to_owned())
        .and_then(wire::PausedAgeMicros::try_from)
        .unwrap_or_else(|refused| {
            unreachable!(
                "a paused workflow age of {} microseconds fits the manifest: {refused}",
                age.micros()
            )
        })
}

/// The retention of each mapped queue, in name order, so the first refusal
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
                        ConfigField::Retention(RetentionField::QueueRetention {
                            queue: queue.clone(),
                        }),
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
    field: ConfigField,
) -> Result<wire::RetentionPeriodDays, ManifestBoundRefused> {
    let days = RetentionDays::parse_period(period)
        .ok()
        .map(|days| u32::from(days.get()));
    Ok(wire::RetentionPeriodDays(within(days, field)?))
}

/// The resilience section (Python `resilience_manifest`): 0 attempts is no
/// limit.
fn resilience_manifest(
    config: &WorkerResilienceConfig,
) -> Result<wire::ResilienceManifest, ManifestBoundRefused> {
    let field = ConfigField::Resilience;
    Ok(wire::ResilienceManifest {
        database_retry_initial_ms: wire::RetryInitialMs(bounded_wide(
            config.database_retry_initial_ms,
            field(ResilienceField::DatabaseRetryInitialMs),
        )?),
        database_retry_max_ms: wire::RetryMaxMs(bounded_wide(
            config.database_retry_max_ms,
            field(ResilienceField::DatabaseRetryMaxMs),
        )?),
        database_retry_max_attempts: match config.database_retry_max_attempts {
            0 => wire::RetryAttemptLimit::NoLimit,
            attempts => wire::RetryAttemptLimit::Attempts {
                attempts: wire::RetryAttemptCount(bounded_wide(
                    attempts,
                    field(ResilienceField::DatabaseRetryMaxAttempts),
                )?),
            },
        },
        notify_poll_interval_ms: wire::PollIntervalMs(bounded_wide(
            config.notify_poll_interval_ms,
            field(ResilienceField::NotifyPollIntervalMs),
        )?),
    })
}

/// The payload section (Python `payload_manifest`): no threshold is no
/// limit.
fn payload_manifest(config: &PayloadPolicy) -> Result<wire::PayloadManifest, ManifestBoundRefused> {
    Ok(wire::PayloadManifest {
        warn_bytes: payload_limit(config.warn_bytes, PayloadField::WarnBytes)?,
        reject_bytes: payload_limit(config.reject_bytes, PayloadField::RejectBytes)?,
    })
}

fn payload_limit(
    threshold: Option<u64>,
    field: PayloadField,
) -> Result<wire::PayloadLimit, ManifestBoundRefused> {
    match threshold {
        None => Ok(wire::PayloadLimit::NoLimit),
        Some(bytes) => wire::PayloadBytes::try_from(bytes)
            .map(|bytes| wire::PayloadLimit::Bytes { bytes })
            .map_err(|_| {
                refused(
                    ConfigField::Payload(field),
                    MIN_PAYLOAD_BYTES,
                    wire::MAX_PAYLOAD_BYTES,
                )
            }),
    }
}

/// `value` as a bounded count of its manifest field.
fn bounded<const MIN: u32, const MAX: u32>(
    value: u32,
    field: ConfigField,
) -> Result<BoundedU32<MIN, MAX>, ManifestBoundRefused> {
    within(Some(value), field)
}

/// `value`, a `u64` of the config, as a bounded count of its manifest field.
fn bounded_wide<const MIN: u32, const MAX: u32>(
    value: u64,
    field: ConfigField,
) -> Result<BoundedU32<MIN, MAX>, ManifestBoundRefused> {
    within(u32::try_from(value).ok(), field)
}

/// `value` inside `MIN` to `MAX`; no value (one that has no `u32` form) or
/// a value outside is refused with the bounds of the field.
fn within<const MIN: u32, const MAX: u32>(
    value: Option<u32>,
    field: ConfigField,
) -> Result<BoundedU32<MIN, MAX>, ManifestBoundRefused> {
    value
        .and_then(|value| BoundedU32::try_from(value).ok())
        .ok_or_else(|| refused(field, u64::from(MIN), u64::from(MAX)))
}

fn refused(field: ConfigField, min: u64, max: u64) -> ManifestBoundRefused {
    ManifestBoundRefused { field, min, max }
}

#[cfg(test)]
mod tests;
