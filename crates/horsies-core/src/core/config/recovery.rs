use serde::{de::Error as _, Deserialize, Deserializer, Serialize};

/// Configuration for automatic stale task detection and crash recovery.
///
/// - CLAIMED tasks that never start: safe to auto-requeue
/// - RUNNING tasks that go stale: mark as FAILED (may not be idempotent)
///
/// All time values are in milliseconds.
#[derive(Debug, Clone, Serialize)]
pub struct RecoveryConfig {
    /// Automatically requeue tasks stuck in CLAIMED (safe — user code never ran).
    #[serde(default = "default_true")]
    pub auto_requeue_stale_claimed: bool,

    /// Milliseconds without claimer heartbeat before CLAIMED task is stale (1s–1hr).
    #[serde(default = "default_claimed_stale_threshold")]
    pub claimed_stale_threshold_ms: u64,

    /// Automatically mark stale RUNNING tasks as FAILED.
    #[serde(default = "default_true")]
    pub auto_fail_stale_running: bool,

    /// Cancel orphaned workflow tasks — claimed or pending rows whose
    /// workflow_task linkage is no longer in a runnable state, so they can
    /// never legitimately reach RUNNING. `true` (default): the reaper sweeps
    /// them CANCELLED and the pre-start check cancels one it is handed;
    /// `false`: orphans are left CLAIMED for inspection.
    #[serde(default = "default_true")]
    pub auto_terminate_orphaned_workflow_tasks: bool,

    /// Minimum time between bounded orphan workflow-task audits.
    /// One audit examines at most 500 live workflow tasks.
    /// A full cycle takes about `ceil(live_workflow_tasks / 500) * interval`.
    /// The reaper check interval remains the lower scheduling unit (1s–24hr).
    #[serde(default = "default_orphan_task_audit_interval")]
    pub orphan_task_audit_interval_ms: u64,

    /// Milliseconds without runner heartbeat before RUNNING task is stale (1s–2hr).
    #[serde(default = "default_running_stale_threshold")]
    pub running_stale_threshold_ms: u64,

    /// Milliseconds a task may remain in finalization before the stale-RUNNING
    /// reaper may reclaim it. A worker stamps `finalizing_at` when it begins the
    /// two-phase finalize; until this threshold elapses the reaper skips the row
    /// even though its runner heartbeat has stopped (1s–2hr).
    #[serde(default = "default_finalizing_stale_threshold")]
    pub finalizing_stale_threshold_ms: u64,

    /// Grace before the workflow reaper consumes a phase-2 outbox row. Finalize
    /// writes the terminal history row and outbox evidence in Phase 1, then
    /// advances the DAG in Phase 2; the grace leaves an in-flight healthy
    /// finalizer alone. Decoupled from heartbeat-validated thresholds so it can
    /// be tuned independently. `0` means immediate recovery; range 0–3_600_000.
    #[serde(default = "default_crashed_worker_recovery_grace")]
    pub crashed_worker_recovery_grace_ms: u64,

    /// Recovery passes allowed before an unresolvable phase-2 row is quarantined.
    #[serde(default = "default_phase2_quarantine_after_attempts")]
    pub phase2_quarantine_after_attempts: u32,

    /// How often the reaper checks for stale tasks (1s–10min).
    #[serde(default = "default_check_interval")]
    pub check_interval_ms: u64,

    /// How often RUNNING tasks send heartbeats (1s–2min).
    #[serde(default = "default_heartbeat_interval")]
    pub runner_heartbeat_interval_ms: u64,

    /// How often worker sends heartbeats for CLAIMED tasks (1s–2min).
    #[serde(default = "default_heartbeat_interval")]
    pub claimer_heartbeat_interval_ms: u64,

    /// How often each worker persists a worker-state snapshot (monitoring
    /// timeseries) in milliseconds (1s–5min). Each snapshot is one row in
    /// `horsies_worker_states`, so shorter intervals grow the table faster.
    #[serde(default = "default_worker_state_snapshot_interval")]
    pub worker_state_snapshot_interval_ms: u64,
}

const MOVED_TO_RETENTION: [&str; 9] = [
    "heartbeat_leaf_horizon_hours",
    "history_leaf_horizon_days",
    "partition_maintenance_interval_s",
    "paused_workflow_auto_cancel_after",
    "retention_classes",
    "retention_delete_batch_size",
    "retention_sweep_interval_s",
    "terminal_record_retention_hours",
    "worker_state_retention_hours",
];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryConfigWire {
    #[serde(default = "default_true")]
    auto_requeue_stale_claimed: bool,
    #[serde(default = "default_claimed_stale_threshold")]
    claimed_stale_threshold_ms: u64,
    #[serde(default = "default_true")]
    auto_fail_stale_running: bool,
    #[serde(default = "default_true")]
    auto_terminate_orphaned_workflow_tasks: bool,
    #[serde(default = "default_orphan_task_audit_interval")]
    orphan_task_audit_interval_ms: u64,
    #[serde(default = "default_running_stale_threshold")]
    running_stale_threshold_ms: u64,
    #[serde(default = "default_finalizing_stale_threshold")]
    finalizing_stale_threshold_ms: u64,
    #[serde(default = "default_crashed_worker_recovery_grace")]
    crashed_worker_recovery_grace_ms: u64,
    #[serde(default = "default_phase2_quarantine_after_attempts")]
    phase2_quarantine_after_attempts: u32,
    #[serde(default = "default_check_interval")]
    check_interval_ms: u64,
    #[serde(default = "default_heartbeat_interval")]
    runner_heartbeat_interval_ms: u64,
    #[serde(default = "default_heartbeat_interval")]
    claimer_heartbeat_interval_ms: u64,
    #[serde(default = "default_worker_state_snapshot_interval")]
    worker_state_snapshot_interval_ms: u64,
}

impl<'de> Deserialize<'de> for RecoveryConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if let Some(fields) = value.as_object() {
            let moved: Vec<_> = MOVED_TO_RETENTION
                .iter()
                .filter(|name| fields.contains_key(**name))
                .map(|name| format!("{name} moved to AppConfig.retention.{name}"))
                .collect();
            if !moved.is_empty() {
                return Err(D::Error::custom(moved.join("; ")));
            }
            if fields.contains_key("queue_terminal_record_retention_hours") {
                return Err(D::Error::custom(
                    "queue_terminal_record_retention_hours was removed in 0.5.0: terminal task rows age by their retention class in the task-history archive; map the queue in AppConfig.retention.queue_retention instead, which takes a duration and drops partitions rather than deleting rows",
                ));
            }
            if fields.contains_key("heartbeat_retention_hours") {
                return Err(D::Error::custom(
                    "heartbeat_retention_hours was removed in 0.5.0: heartbeat rows live in time-partitioned leaves that drop whole; a row-delete window no longer exists",
                ));
            }
        }

        let wire: RecoveryConfigWire = serde_json::from_value(value).map_err(D::Error::custom)?;
        Ok(Self {
            auto_requeue_stale_claimed: wire.auto_requeue_stale_claimed,
            claimed_stale_threshold_ms: wire.claimed_stale_threshold_ms,
            auto_fail_stale_running: wire.auto_fail_stale_running,
            auto_terminate_orphaned_workflow_tasks: wire.auto_terminate_orphaned_workflow_tasks,
            orphan_task_audit_interval_ms: wire.orphan_task_audit_interval_ms,
            running_stale_threshold_ms: wire.running_stale_threshold_ms,
            finalizing_stale_threshold_ms: wire.finalizing_stale_threshold_ms,
            crashed_worker_recovery_grace_ms: wire.crashed_worker_recovery_grace_ms,
            phase2_quarantine_after_attempts: wire.phase2_quarantine_after_attempts,
            check_interval_ms: wire.check_interval_ms,
            runner_heartbeat_interval_ms: wire.runner_heartbeat_interval_ms,
            claimer_heartbeat_interval_ms: wire.claimer_heartbeat_interval_ms,
            worker_state_snapshot_interval_ms: wire.worker_state_snapshot_interval_ms,
        })
    }
}

fn default_true() -> bool {
    true
}
fn default_claimed_stale_threshold() -> u64 {
    120_000
}
fn default_running_stale_threshold() -> u64 {
    300_000
}
fn default_finalizing_stale_threshold() -> u64 {
    300_000
}
fn default_crashed_worker_recovery_grace() -> u64 {
    10_000
}
fn default_phase2_quarantine_after_attempts() -> u32 {
    25
}
fn default_orphan_task_audit_interval() -> u64 {
    60_000
}
fn default_check_interval() -> u64 {
    30_000
}
fn default_heartbeat_interval() -> u64 {
    30_000
}
fn default_worker_state_snapshot_interval() -> u64 {
    30_000
}
impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            auto_requeue_stale_claimed: true,
            claimed_stale_threshold_ms: 120_000,
            auto_fail_stale_running: true,
            auto_terminate_orphaned_workflow_tasks: true,
            orphan_task_audit_interval_ms: 60_000,
            running_stale_threshold_ms: 300_000,
            finalizing_stale_threshold_ms: 300_000,
            crashed_worker_recovery_grace_ms: 10_000,
            phase2_quarantine_after_attempts: 25,
            check_interval_ms: 30_000,
            runner_heartbeat_interval_ms: 30_000,
            claimer_heartbeat_interval_ms: 30_000,
            worker_state_snapshot_interval_ms: 30_000,
        }
    }
}

/// Validation error for RecoveryConfig.
#[derive(Debug, Clone, thiserror::Error)]
pub enum RecoveryConfigError {
    #[error(
        "running_stale_threshold_ms ({threshold}ms) must be at least 2x runner_heartbeat_interval_ms ({heartbeat}ms), minimum: {minimum}ms"
    )]
    RunningThresholdTooLow {
        threshold: u64,
        heartbeat: u64,
        minimum: u64,
    },

    #[error(
        "claimed_stale_threshold_ms ({threshold}ms) must be at least 2x claimer_heartbeat_interval_ms ({heartbeat}ms), minimum: {minimum}ms"
    )]
    ClaimedThresholdTooLow {
        threshold: u64,
        heartbeat: u64,
        minimum: u64,
    },

    #[error(
        "finalizing_stale_threshold_ms ({threshold}ms) must be at least 2x runner_heartbeat_interval_ms ({heartbeat}ms), minimum: {minimum}ms"
    )]
    FinalizingThresholdTooLow {
        threshold: u64,
        heartbeat: u64,
        minimum: u64,
    },

    #[error("{field} ({value}) must be >= {min}")]
    BelowMinimum {
        field: &'static str,
        value: u64,
        min: u64,
    },

    #[error("{field} ({value}) must be <= {max}")]
    AboveMaximum {
        field: &'static str,
        value: u64,
        max: u64,
    },
}

impl RecoveryConfig {
    /// Validate recovery config fields.
    ///
    /// Checks:
    /// - All millisecond fields >= 1000 (at least 1 second)
    /// - Maximum bounds per doc-comment ranges
    /// - Stale thresholds >= 2x heartbeat intervals
    ///
    /// Returns all validation errors found, not just the first.
    pub fn validate(&self) -> Vec<RecoveryConfigError> {
        let mut errors = Vec::new();

        // Minimum 1 second for all _ms fields.
        const MIN_MS: u64 = 1_000;

        let ms_fields: &[(&str, u64)] = &[
            (
                "claimed_stale_threshold_ms",
                self.claimed_stale_threshold_ms,
            ),
            (
                "running_stale_threshold_ms",
                self.running_stale_threshold_ms,
            ),
            (
                "finalizing_stale_threshold_ms",
                self.finalizing_stale_threshold_ms,
            ),
            (
                "orphan_task_audit_interval_ms",
                self.orphan_task_audit_interval_ms,
            ),
            ("check_interval_ms", self.check_interval_ms),
            (
                "runner_heartbeat_interval_ms",
                self.runner_heartbeat_interval_ms,
            ),
            (
                "claimer_heartbeat_interval_ms",
                self.claimer_heartbeat_interval_ms,
            ),
            (
                "worker_state_snapshot_interval_ms",
                self.worker_state_snapshot_interval_ms,
            ),
        ];

        for &(field, value) in ms_fields {
            if value < MIN_MS {
                errors.push(RecoveryConfigError::BelowMinimum {
                    field,
                    value,
                    min: MIN_MS,
                });
            }
        }

        // crashed_worker_recovery_grace_ms: 0 disables (no MIN check); max 1hr.
        if self.crashed_worker_recovery_grace_ms > 3_600_000 {
            errors.push(RecoveryConfigError::AboveMaximum {
                field: "crashed_worker_recovery_grace_ms",
                value: self.crashed_worker_recovery_grace_ms,
                max: 3_600_000,
            });
        }

        if self.phase2_quarantine_after_attempts < 3 {
            errors.push(RecoveryConfigError::BelowMinimum {
                field: "phase2_quarantine_after_attempts",
                value: u64::from(self.phase2_quarantine_after_attempts),
                min: 3,
            });
        } else if self.phase2_quarantine_after_attempts > 1_000 {
            errors.push(RecoveryConfigError::AboveMaximum {
                field: "phase2_quarantine_after_attempts",
                value: u64::from(self.phase2_quarantine_after_attempts),
                max: 1_000,
            });
        }

        // Maximum bounds per doc-comment ranges.
        if self.check_interval_ms > 600_000 {
            errors.push(RecoveryConfigError::AboveMaximum {
                field: "check_interval_ms",
                value: self.check_interval_ms,
                max: 600_000,
            });
        }
        if self.orphan_task_audit_interval_ms > 86_400_000 {
            errors.push(RecoveryConfigError::AboveMaximum {
                field: "orphan_task_audit_interval_ms",
                value: self.orphan_task_audit_interval_ms,
                max: 86_400_000,
            });
        }
        if self.runner_heartbeat_interval_ms > 120_000 {
            errors.push(RecoveryConfigError::AboveMaximum {
                field: "runner_heartbeat_interval_ms",
                value: self.runner_heartbeat_interval_ms,
                max: 120_000,
            });
        }
        if self.claimer_heartbeat_interval_ms > 120_000 {
            errors.push(RecoveryConfigError::AboveMaximum {
                field: "claimer_heartbeat_interval_ms",
                value: self.claimer_heartbeat_interval_ms,
                max: 120_000,
            });
        }
        if self.worker_state_snapshot_interval_ms > 300_000 {
            errors.push(RecoveryConfigError::AboveMaximum {
                field: "worker_state_snapshot_interval_ms",
                value: self.worker_state_snapshot_interval_ms,
                max: 300_000,
            });
        }
        if self.running_stale_threshold_ms > 7_200_000 {
            errors.push(RecoveryConfigError::AboveMaximum {
                field: "running_stale_threshold_ms",
                value: self.running_stale_threshold_ms,
                max: 7_200_000,
            });
        }
        if self.finalizing_stale_threshold_ms > 7_200_000 {
            errors.push(RecoveryConfigError::AboveMaximum {
                field: "finalizing_stale_threshold_ms",
                value: self.finalizing_stale_threshold_ms,
                max: 7_200_000,
            });
        }
        if self.claimed_stale_threshold_ms > 3_600_000 {
            errors.push(RecoveryConfigError::AboveMaximum {
                field: "claimed_stale_threshold_ms",
                value: self.claimed_stale_threshold_ms,
                max: 3_600_000,
            });
        }

        // Relational: thresholds must be >= 2x heartbeat.
        let min_running = self.runner_heartbeat_interval_ms * 2;
        if self.running_stale_threshold_ms < min_running {
            errors.push(RecoveryConfigError::RunningThresholdTooLow {
                threshold: self.running_stale_threshold_ms,
                heartbeat: self.runner_heartbeat_interval_ms,
                minimum: min_running,
            });
        }

        let min_claimed = self.claimer_heartbeat_interval_ms * 2;
        if self.claimed_stale_threshold_ms < min_claimed {
            errors.push(RecoveryConfigError::ClaimedThresholdTooLow {
                threshold: self.claimed_stale_threshold_ms,
                heartbeat: self.claimer_heartbeat_interval_ms,
                minimum: min_claimed,
            });
        }

        if self.finalizing_stale_threshold_ms < min_running {
            errors.push(RecoveryConfigError::FinalizingThresholdTooLow {
                threshold: self.finalizing_stale_threshold_ms,
                heartbeat: self.runner_heartbeat_interval_ms,
                minimum: min_running,
            });
        }

        errors
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_validates() {
        let config = RecoveryConfig::default();
        assert!(config.validate().is_empty());
    }

    #[test]
    fn running_threshold_too_low() {
        let config = RecoveryConfig {
            running_stale_threshold_ms: 10_000,
            runner_heartbeat_interval_ms: 30_000,
            ..Default::default()
        };
        let errors = config.validate();
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0],
            RecoveryConfigError::RunningThresholdTooLow { .. }
        ));
    }

    #[test]
    fn both_thresholds_too_low() {
        let config = RecoveryConfig {
            running_stale_threshold_ms: 10_000,
            runner_heartbeat_interval_ms: 30_000,
            claimed_stale_threshold_ms: 10_000,
            claimer_heartbeat_interval_ms: 30_000,
            ..Default::default()
        };
        let errors = config.validate();
        assert_eq!(errors.len(), 2);
    }

    #[test]
    fn finalizing_threshold_too_low() {
        let config = RecoveryConfig {
            finalizing_stale_threshold_ms: 10_000,
            runner_heartbeat_interval_ms: 30_000,
            ..Default::default()
        };
        let errors = config.validate();
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0],
            RecoveryConfigError::FinalizingThresholdTooLow { .. }
        ));
    }

    #[test]
    fn finalizing_threshold_default_validates() {
        // Default finalizing threshold (300_000) >= 2x default runner heartbeat.
        let config = RecoveryConfig::default();
        assert!(!config
            .validate()
            .iter()
            .any(|e| matches!(e, RecoveryConfigError::FinalizingThresholdTooLow { .. })));
    }

    // --- BelowMinimum tests ---

    #[test]
    fn field_set_to_zero_triggers_below_minimum() {
        let config = RecoveryConfig {
            check_interval_ms: 0,
            ..Default::default()
        };
        let errors = config.validate();
        let below: Vec<_> = errors
            .iter()
            .filter(|e| matches!(e, RecoveryConfigError::BelowMinimum { .. }))
            .collect();
        assert_eq!(below.len(), 1);
        match &below[0] {
            RecoveryConfigError::BelowMinimum { field, value, min } => {
                assert_eq!(*field, "check_interval_ms");
                assert_eq!(*value, 0);
                assert_eq!(*min, 1_000);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn field_set_to_500_triggers_below_minimum() {
        let config = RecoveryConfig {
            runner_heartbeat_interval_ms: 500,
            // Set running_stale_threshold_ms high to avoid relational error.
            running_stale_threshold_ms: 300_000,
            ..Default::default()
        };
        let errors = config.validate();
        let below: Vec<_> = errors
            .iter()
            .filter(|e| matches!(e, RecoveryConfigError::BelowMinimum { .. }))
            .collect();
        assert_eq!(below.len(), 1);
        match &below[0] {
            RecoveryConfigError::BelowMinimum { field, value, min } => {
                assert_eq!(*field, "runner_heartbeat_interval_ms");
                assert_eq!(*value, 500);
                assert_eq!(*min, 1_000);
            }
            _ => unreachable!(),
        }
    }

    // --- AboveMaximum tests ---

    #[test]
    fn check_interval_above_maximum() {
        let config = RecoveryConfig {
            check_interval_ms: 600_001,
            ..Default::default()
        };
        let errors = config.validate();
        let above: Vec<_> = errors
            .iter()
            .filter(|e| matches!(e, RecoveryConfigError::AboveMaximum { .. }))
            .collect();
        assert_eq!(above.len(), 1);
        match &above[0] {
            RecoveryConfigError::AboveMaximum { field, value, max } => {
                assert_eq!(*field, "check_interval_ms");
                assert_eq!(*value, 600_001);
                assert_eq!(*max, 600_000);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn runner_heartbeat_above_maximum() {
        let config = RecoveryConfig {
            runner_heartbeat_interval_ms: 120_001,
            // Set running threshold high enough to avoid relational error.
            running_stale_threshold_ms: 300_000,
            ..Default::default()
        };
        let errors = config.validate();
        let above: Vec<_> = errors
            .iter()
            .filter(|e| matches!(e, RecoveryConfigError::AboveMaximum { .. }))
            .collect();
        assert_eq!(above.len(), 1);
        match &above[0] {
            RecoveryConfigError::AboveMaximum { field, value, max } => {
                assert_eq!(*field, "runner_heartbeat_interval_ms");
                assert_eq!(*value, 120_001);
                assert_eq!(*max, 120_000);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn claimed_stale_above_maximum() {
        let config = RecoveryConfig {
            claimed_stale_threshold_ms: 3_600_001,
            ..Default::default()
        };
        let errors = config.validate();
        let above: Vec<_> = errors
            .iter()
            .filter(|e| matches!(e, RecoveryConfigError::AboveMaximum { .. }))
            .collect();
        assert_eq!(above.len(), 1);
        match &above[0] {
            RecoveryConfigError::AboveMaximum { field, value, max } => {
                assert_eq!(*field, "claimed_stale_threshold_ms");
                assert_eq!(*value, 3_600_001);
                assert_eq!(*max, 3_600_000);
            }
            _ => unreachable!(),
        }
    }

    // --- Multiple violations at once ---

    #[test]
    fn multiple_violations_collected() {
        let config = RecoveryConfig {
            // Below minimum: 500 < 1000
            check_interval_ms: 500,
            // Above maximum: 3_600_001 > 3_600_000
            claimed_stale_threshold_ms: 3_600_001,
            // Keep heartbeats valid to isolate the errors we care about.
            runner_heartbeat_interval_ms: 30_000,
            claimer_heartbeat_interval_ms: 30_000,
            running_stale_threshold_ms: 300_000,
            ..Default::default()
        };
        let errors = config.validate();
        let below_count = errors
            .iter()
            .filter(|e| matches!(e, RecoveryConfigError::BelowMinimum { .. }))
            .count();
        let above_count = errors
            .iter()
            .filter(|e| matches!(e, RecoveryConfigError::AboveMaximum { .. }))
            .count();
        assert!(below_count >= 1, "expected at least 1 BelowMinimum error");
        assert!(above_count >= 1, "expected at least 1 AboveMaximum error");
        assert!(
            errors.len() >= 2,
            "expected at least 2 total errors, got {}",
            errors.len()
        );
    }

    // --- worker_state_snapshot_interval_ms (parity with horsies PR #171) ---

    #[test]
    fn worker_state_snapshot_interval_defaults_to_30s() {
        assert_eq!(
            RecoveryConfig::default().worker_state_snapshot_interval_ms,
            30_000
        );
        let from_empty: RecoveryConfig = serde_json::from_str("{}").expect("defaults deserialize");
        assert_eq!(from_empty.worker_state_snapshot_interval_ms, 30_000);
    }

    #[test]
    fn worker_state_snapshot_interval_bounds() {
        let at_min = RecoveryConfig {
            worker_state_snapshot_interval_ms: 1_000,
            ..Default::default()
        };
        assert!(at_min.validate().is_empty());

        let at_max = RecoveryConfig {
            worker_state_snapshot_interval_ms: 300_000,
            ..Default::default()
        };
        assert!(at_max.validate().is_empty());

        let below = RecoveryConfig {
            worker_state_snapshot_interval_ms: 999,
            ..Default::default()
        };
        let errors = below.validate();
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0],
            RecoveryConfigError::BelowMinimum {
                field: "worker_state_snapshot_interval_ms",
                ..
            }
        ));

        let above = RecoveryConfig {
            worker_state_snapshot_interval_ms: 300_001,
            ..Default::default()
        };
        let errors = above.validate();
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0],
            RecoveryConfigError::AboveMaximum {
                field: "worker_state_snapshot_interval_ms",
                ..
            }
        ));
    }

    #[test]
    fn orphan_task_audit_interval_default_and_bounds_are_exact() {
        let from_empty: RecoveryConfig = serde_json::from_str("{}").expect("defaults deserialize");
        assert_eq!(from_empty.orphan_task_audit_interval_ms, 60_000);

        for value in [1_000, 86_400_000] {
            let config = RecoveryConfig {
                orphan_task_audit_interval_ms: value,
                ..Default::default()
            };
            assert!(config.validate().is_empty());
        }
        for value in [999, 86_400_001] {
            let config = RecoveryConfig {
                orphan_task_audit_interval_ms: value,
                ..Default::default()
            };
            assert_eq!(config.validate().len(), 1);
        }
    }

    #[test]
    fn phase2_quarantine_attempt_bounds_and_default_are_exact() {
        let from_empty: RecoveryConfig = serde_json::from_str("{}").expect("defaults deserialize");
        assert_eq!(from_empty.phase2_quarantine_after_attempts, 25);

        for value in [3, 1_000] {
            let config = RecoveryConfig {
                phase2_quarantine_after_attempts: value,
                ..Default::default()
            };
            assert!(config.validate().is_empty());
        }
        for value in [2, 1_001] {
            let config = RecoveryConfig {
                phase2_quarantine_after_attempts: value,
                ..Default::default()
            };
            assert_eq!(config.validate().len(), 1);
        }
    }

    #[test]
    fn moved_and_removed_retention_fields_fail_closed_with_successors() {
        for name in MOVED_TO_RETENTION {
            let json = format!(r#"{{"{name}": 1}}"#);
            let error = serde_json::from_str::<RecoveryConfig>(&json).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains(&format!("{name} moved to AppConfig.retention.{name}")),
                "{error}",
            );
        }

        let queue_error = serde_json::from_str::<RecoveryConfig>(
            r#"{"queue_terminal_record_retention_hours":{"default":24}}"#,
        )
        .unwrap_err();
        assert!(queue_error.to_string().contains("retention class"));

        let heartbeat_error =
            serde_json::from_str::<RecoveryConfig>(r#"{"heartbeat_retention_hours":24}"#)
                .unwrap_err();
        assert!(heartbeat_error.to_string().contains("drop whole"));
    }

    // --- Edge cases: exactly at minimum / maximum ---

    #[test]
    fn field_at_exactly_minimum_passes() {
        let config = RecoveryConfig {
            check_interval_ms: 1_000,
            runner_heartbeat_interval_ms: 1_000,
            claimer_heartbeat_interval_ms: 1_000,
            running_stale_threshold_ms: 2_000,
            claimed_stale_threshold_ms: 2_000,
            ..Default::default()
        };
        let errors = config.validate();
        // No BelowMinimum errors expected.
        let below: Vec<_> = errors
            .iter()
            .filter(|e| matches!(e, RecoveryConfigError::BelowMinimum { .. }))
            .collect();
        assert!(
            below.is_empty(),
            "expected no BelowMinimum errors, got {:?}",
            below
        );
    }

    #[test]
    fn field_at_exactly_maximum_passes() {
        let config = RecoveryConfig {
            check_interval_ms: 600_000,
            runner_heartbeat_interval_ms: 120_000,
            claimer_heartbeat_interval_ms: 120_000,
            running_stale_threshold_ms: 7_200_000,
            claimed_stale_threshold_ms: 3_600_000,
            ..Default::default()
        };
        let errors = config.validate();
        // No AboveMaximum errors expected.
        let above: Vec<_> = errors
            .iter()
            .filter(|e| matches!(e, RecoveryConfigError::AboveMaximum { .. }))
            .collect();
        assert!(
            above.is_empty(),
            "expected no AboveMaximum errors, got {:?}",
            above
        );
    }
}
