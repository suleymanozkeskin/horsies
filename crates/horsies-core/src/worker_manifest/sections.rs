//! The sections of the worker manifest that set the worker's own loops:
//! recovery (Python `RecoveryConfig`), retention (`RetentionConfig`),
//! resilience (`WorkerResilienceConfig`) and the payload size guard
//! (`PayloadPolicy`).
//!
//! Each field is given: a Pydantic default reaches the manifest as the value
//! it holds. A Python `None` that turns a rule off is a variant of its own.
//! The bounds are the Pydantic bounds; a field that Pydantic does not bound
//! above has the bound that the core's type can hold. The rules between two
//! fields (a stale threshold against its beat interval, the backoff ceiling
//! against its start) stay with the config types, whose `validate` the
//! worker runs on the converted config.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use crate::core::config::bounded::BoundedU32;
use crate::core::config::recovery::WORKER_STATE_SNAPSHOT_INTERVAL_MAX_MS;
use crate::core::config::{PayloadPolicy, RecoveryConfig, RetentionConfig, WorkerResilienceConfig};
use crate::core::history::periods::{MAX_RETENTION_DAYS, MIN_RETENTION_DAYS};
use crate::manifest_schema::{IntBound, ManifestValue, PythonValue};

/// The largest `worker_state_snapshot_interval_ms` as the bound of its
/// manifest field. Half the live window fits a `u32`.
const SNAPSHOT_INTERVAL_MAX_MS: u32 = {
    assert!(WORKER_STATE_SNAPSHOT_INTERVAL_MAX_MS <= u32::MAX as u64);
    WORKER_STATE_SNAPSHOT_INTERVAL_MAX_MS as u32
};

/// The longest age of a paused workflow, in microseconds: the largest
/// count that `chrono::Duration::microseconds` takes.
pub const MAX_PAUSED_AGE_MICROS: u64 = i64::MAX as u64;

/// The largest payload threshold, in bytes.
pub const MAX_PAYLOAD_BYTES: u64 = u64::MAX;

bounded_count!(
    /// Milliseconds without a claimer heartbeat before a CLAIMED task is
    /// stale.
    ClaimedStaleThresholdMs(BoundedU32<1_000, 3_600_000>) as "CLAIMED_STALE_THRESHOLD_MS"
);
bounded_count!(
    /// Milliseconds without a runner heartbeat, or in the finalizing state,
    /// before a task is stale.
    StaleThresholdMs(BoundedU32<1_000, 7_200_000>) as "STALE_THRESHOLD_MS"
);
bounded_count!(
    /// Milliseconds the reaper waits before it recovers a workflow task
    /// whose workflow progression was not applied; 0 waits not at all.
    RecoveryGraceMs(BoundedU32<0, 3_600_000>) as "RECOVERY_GRACE_MS"
);
bounded_count!(
    /// Reaper passes that a recovery may fail before it stops retrying.
    RecoveryPassLimit(BoundedU32<3, 1_000>) as "RECOVERY_PASS_LIMIT"
);
bounded_count!(
    /// Milliseconds between two reaper passes.
    CheckIntervalMs(BoundedU32<1_000, 600_000>) as "CHECK_INTERVAL_MS"
);
bounded_count!(
    /// Milliseconds between two pages of the orphan task audit.
    AuditIntervalMs(BoundedU32<1_000, 86_400_000>) as "AUDIT_INTERVAL_MS"
);
bounded_count!(
    /// Milliseconds between two heartbeats.
    HeartbeatIntervalMs(BoundedU32<1_000, 120_000>) as "HEARTBEAT_INTERVAL_MS"
);
bounded_count!(
    /// Milliseconds between two worker state snapshots: at most half the
    /// live window.
    SnapshotIntervalMs(BoundedU32<1_000, SNAPSHOT_INTERVAL_MAX_MS>) as "SNAPSHOT_INTERVAL_MS"
);
bounded_count!(
    /// Hours that a worker state snapshot is kept.
    WorkerStateRetentionHours(BoundedU32<1, 8_760>) as "WORKER_STATE_RETENTION_HOURS"
);
bounded_count!(
    /// Hours that a terminal workflow record is kept.
    TerminalRecordRetentionHours(BoundedU32<1, 43_800>) as "TERMINAL_RECORD_RETENTION_HOURS"
);
bounded_count!(
    /// Daily history leaves kept ahead of the writes.
    HistoryLeafHorizonDays(BoundedU32<2, 14>) as "HISTORY_LEAF_HORIZON_DAYS"
);
bounded_count!(
    /// Hourly heartbeat leaves kept ahead of the writes.
    HeartbeatLeafHorizonHours(BoundedU32<2, 48>) as "HEARTBEAT_LEAF_HORIZON_HOURS"
);
bounded_count!(
    /// A retention period in whole days.
    RetentionPeriodDays(
        BoundedU32<{ MIN_RETENTION_DAYS as u32 }, { MAX_RETENTION_DAYS as u32 }>
    ) as "RETENTION_DAYS"
);
bounded_count!(
    /// Seconds between two partition coverage passes.
    MaintenanceIntervalSeconds(BoundedU32<60, 3_600>) as "MAINTENANCE_INTERVAL_SECONDS"
);
bounded_count!(
    /// Seconds between two retention sweeps.
    SweepIntervalSeconds(BoundedU32<30, 86_400>) as "SWEEP_INTERVAL_SECONDS"
);
bounded_count!(
    /// Rows of one retention delete.
    DeleteBatchSize(BoundedU32<50, 10_000>) as "DELETE_BATCH_SIZE"
);
bounded_count!(
    /// The first wait of the backoff after a database error, in
    /// milliseconds.
    RetryInitialMs(BoundedU32<100, 60_000>) as "RETRY_INITIAL_MS"
);
bounded_count!(
    /// The longest wait of the backoff after a database error, in
    /// milliseconds.
    RetryMaxMs(BoundedU32<500, 300_000>) as "RETRY_MAX_MS"
);
bounded_count!(
    /// Attempts of the backoff after a database error.
    RetryAttemptCount(BoundedU32<1, 10_000>) as "RETRY_ATTEMPTS"
);
bounded_count!(
    /// Milliseconds between two claim passes when no notification comes.
    PollIntervalMs(BoundedU32<1_000, 300_000>) as "POLL_INTERVAL_MS"
);

/// The age of a paused workflow in microseconds: 1 to
/// [`MAX_PAUSED_AGE_MICROS`]. Only the parser constructs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct PausedAgeMicros(u64);

impl PausedAgeMicros {
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The age as the duration type of the retention config, exact to the
    /// microsecond.
    pub fn as_duration(self) -> chrono::Duration {
        match i64::try_from(self.0) {
            Ok(micros) => chrono::Duration::microseconds(micros),
            Err(refused) => unreachable!(
                "a parsed paused age of {} microseconds fits an i64: {refused}",
                self.0
            ),
        }
    }
}

impl TryFrom<u64> for PausedAgeMicros {
    type Error = String;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            1..=MAX_PAUSED_AGE_MICROS => Ok(Self(value)),
            _ => Err(format!(
                "a paused workflow age must be from 1 to {MAX_PAUSED_AGE_MICROS} microseconds"
            )),
        }
    }
}

impl From<PausedAgeMicros> for u64 {
    fn from(age: PausedAgeMicros) -> Self {
        age.0
    }
}

impl ManifestValue for PausedAgeMicros {
    const PYTHON: PythonValue = PythonValue::Int(IntBound {
        name: "PAUSED_AGE_MICROS",
        min: 1,
        max: MAX_PAUSED_AGE_MICROS,
    });
}

/// A payload threshold in bytes of UTF-8 text: 1 to [`MAX_PAYLOAD_BYTES`].
/// Only the parser constructs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct PayloadBytes(u64);

impl PayloadBytes {
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for PayloadBytes {
    type Error = String;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            1..=MAX_PAYLOAD_BYTES => Ok(Self(value)),
            0 => Err("a payload threshold must be at least 1 byte".to_owned()),
        }
    }
}

impl From<PayloadBytes> for u64 {
    fn from(bytes: PayloadBytes) -> Self {
        bytes.0
    }
}

impl ManifestValue for PayloadBytes {
    const PYTHON: PythonValue = PythonValue::Int(IntBound {
        name: "PAYLOAD_BYTES",
        min: 1,
        max: MAX_PAYLOAD_BYTES,
    });
}

/// Stale task detection and crash recovery (Python `RecoveryConfig`).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(deny_unknown_fields)]
pub struct RecoveryManifest {
    pub auto_requeue_stale_claimed: bool,
    pub claimed_stale_threshold_ms: ClaimedStaleThresholdMs,
    pub auto_terminate_orphaned_workflow_tasks: bool,
    pub auto_fail_stale_running: bool,
    pub running_stale_threshold_ms: StaleThresholdMs,
    pub finalizing_stale_threshold_ms: StaleThresholdMs,
    pub crashed_worker_recovery_grace_ms: RecoveryGraceMs,
    pub phase2_quarantine_after_attempts: RecoveryPassLimit,
    pub workflow_recovery_failure_limit: RecoveryPassLimit,
    pub check_interval_ms: CheckIntervalMs,
    pub orphan_task_audit_interval_ms: AuditIntervalMs,
    pub runner_heartbeat_interval_ms: HeartbeatIntervalMs,
    pub claimer_heartbeat_interval_ms: HeartbeatIntervalMs,
    pub worker_state_snapshot_interval_ms: SnapshotIntervalMs,
}

/// How long worker state snapshots are kept.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum WorkerStateRetention {
    /// Prune a snapshot after `hours` hours.
    Hours { hours: WorkerStateRetentionHours },
    /// Never prune a snapshot.
    NoPruning,
}

/// How long terminal workflow records are kept.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum TerminalRecordRetention {
    /// Prune a terminal workflow record after `hours` hours.
    Hours { hours: TerminalRecordRetentionHours },
    /// Never prune a terminal workflow record.
    NoPruning,
}

/// When a paused workflow expires.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum PausedWorkflowExpiry {
    /// A workflow paused for longer than `micros` microseconds expires.
    After { micros: PausedAgeMicros },
    /// A paused workflow never expires.
    NoExpiry,
}

/// How long a task sent on a queue keeps its history record.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum QueueRetention {
    /// `days` whole days.
    Days { days: RetentionPeriodDays },
    /// For ever.
    Forever,
}

/// How long records live and how their storage is kept ahead (Python
/// `RetentionConfig`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema)]
#[serde(deny_unknown_fields)]
pub struct RetentionManifest {
    pub worker_state_retention: WorkerStateRetention,
    pub terminal_record_retention: TerminalRecordRetention,
    pub paused_workflow_auto_cancel_after: PausedWorkflowExpiry,
    pub history_leaf_horizon_days: HistoryLeafHorizonDays,
    pub heartbeat_leaf_horizon_hours: HeartbeatLeafHorizonHours,
    /// Finite periods a send may choose besides the default and the mapped
    /// periods.
    pub retention_periods: Vec<RetentionPeriodDays>,
    /// The retention of each mapped queue, by queue name.
    pub queue_retention: BTreeMap<String, QueueRetention>,
    pub partition_maintenance_interval_s: MaintenanceIntervalSeconds,
    pub retention_sweep_interval_s: SweepIntervalSeconds,
    pub retention_delete_batch_size: DeleteBatchSize,
}

/// How often the backoff after a database error tries again.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum RetryAttemptLimit {
    /// At most `attempts` attempts.
    Attempts { attempts: RetryAttemptCount },
    /// No limit on the attempts (Python `db_retry_max_attempts=0`).
    NoLimit,
}

/// Retry and backoff after database errors, and the poll interval (Python
/// `WorkerResilienceConfig`).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(deny_unknown_fields)]
pub struct ResilienceManifest {
    pub db_retry_initial_ms: RetryInitialMs,
    pub db_retry_max_ms: RetryMaxMs,
    pub db_retry_max_attempts: RetryAttemptLimit,
    pub notify_poll_interval_ms: PollIntervalMs,
}

/// A payload size threshold.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum PayloadLimit {
    /// A threshold of `bytes` bytes of UTF-8 text.
    Bytes { bytes: PayloadBytes },
    /// No threshold (Python `None`).
    NoLimit,
}

/// The size guard of serialized payloads (Python `PayloadPolicy`).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(deny_unknown_fields)]
pub struct PayloadManifest {
    /// Log a warning for a payload above this size.
    pub warn_bytes: PayloadLimit,
    /// Refuse an enqueue whose payload is above this size.
    pub reject_bytes: PayloadLimit,
}

impl RecoveryManifest {
    /// The section as the config type. Total: every value is in its bounds.
    /// Pure.
    pub fn to_config(&self) -> RecoveryConfig {
        RecoveryConfig {
            auto_requeue_stale_claimed: self.auto_requeue_stale_claimed,
            claimed_stale_threshold_ms: u64::from(self.claimed_stale_threshold_ms.0.get()),
            auto_fail_stale_running: self.auto_fail_stale_running,
            auto_terminate_orphaned_workflow_tasks: self.auto_terminate_orphaned_workflow_tasks,
            orphan_task_audit_interval_ms: u64::from(self.orphan_task_audit_interval_ms.0.get()),
            running_stale_threshold_ms: u64::from(self.running_stale_threshold_ms.0.get()),
            finalizing_stale_threshold_ms: u64::from(self.finalizing_stale_threshold_ms.0.get()),
            crashed_worker_recovery_grace_ms: u64::from(
                self.crashed_worker_recovery_grace_ms.0.get(),
            ),
            phase2_quarantine_after_attempts: self.phase2_quarantine_after_attempts.0.get(),
            workflow_recovery_failure_limit: self.workflow_recovery_failure_limit.0.get(),
            check_interval_ms: u64::from(self.check_interval_ms.0.get()),
            runner_heartbeat_interval_ms: u64::from(self.runner_heartbeat_interval_ms.0.get()),
            claimer_heartbeat_interval_ms: u64::from(self.claimer_heartbeat_interval_ms.0.get()),
            worker_state_snapshot_interval_ms: u64::from(
                self.worker_state_snapshot_interval_ms.0.get(),
            ),
        }
    }
}

impl RetentionPeriodDays {
    /// The period as the duration type of the retention config.
    fn as_duration(self) -> chrono::Duration {
        chrono::Duration::days(i64::from(self.0.get()))
    }
}

impl RetentionManifest {
    /// The section as the config type. Total: every value is in its bounds.
    /// Pure.
    pub fn to_config(&self) -> RetentionConfig {
        RetentionConfig {
            worker_state_retention_hours: match self.worker_state_retention {
                WorkerStateRetention::Hours { hours } => Some(hours.0.get()),
                WorkerStateRetention::NoPruning => None,
            },
            terminal_record_retention_hours: match self.terminal_record_retention {
                TerminalRecordRetention::Hours { hours } => Some(hours.0.get()),
                TerminalRecordRetention::NoPruning => None,
            },
            paused_workflow_auto_cancel_after: match self.paused_workflow_auto_cancel_after {
                PausedWorkflowExpiry::After { micros } => Some(micros.as_duration()),
                PausedWorkflowExpiry::NoExpiry => None,
            },
            history_leaf_horizon_days: self.history_leaf_horizon_days.0.get(),
            heartbeat_leaf_horizon_hours: self.heartbeat_leaf_horizon_hours.0.get(),
            retention_periods: self
                .retention_periods
                .iter()
                .map(|days| days.as_duration())
                .collect(),
            queue_retention: self
                .queue_retention
                .iter()
                .map(|(queue, retention)| {
                    let period = match retention {
                        QueueRetention::Days { days } => Some(days.as_duration()),
                        QueueRetention::Forever => None,
                    };
                    (queue.clone(), period)
                })
                .collect::<HashMap<String, Option<chrono::Duration>>>(),
            partition_maintenance_interval_s: u64::from(
                self.partition_maintenance_interval_s.0.get(),
            ),
            retention_sweep_interval_s: u64::from(self.retention_sweep_interval_s.0.get()),
            retention_delete_batch_size: self.retention_delete_batch_size.0.get(),
        }
    }
}

impl ResilienceManifest {
    /// The section as the config type, whose 0 attempts means no limit.
    /// Total. Pure.
    pub fn to_config(&self) -> WorkerResilienceConfig {
        WorkerResilienceConfig {
            db_retry_initial_ms: u64::from(self.db_retry_initial_ms.0.get()),
            db_retry_max_ms: u64::from(self.db_retry_max_ms.0.get()),
            db_retry_max_attempts: match self.db_retry_max_attempts {
                RetryAttemptLimit::Attempts { attempts } => u64::from(attempts.0.get()),
                RetryAttemptLimit::NoLimit => 0,
            },
            notify_poll_interval_ms: u64::from(self.notify_poll_interval_ms.0.get()),
        }
    }
}

impl PayloadLimit {
    fn threshold(self) -> Option<u64> {
        match self {
            Self::Bytes { bytes } => Some(bytes.get()),
            Self::NoLimit => None,
        }
    }
}

impl PayloadManifest {
    /// The section as the config type. Total. Pure.
    pub fn to_config(&self) -> PayloadPolicy {
        PayloadPolicy {
            warn_bytes: self.warn_bytes.threshold(),
            reject_bytes: self.reject_bytes.threshold(),
        }
    }
}
