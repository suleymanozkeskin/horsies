use serde::{Deserialize, Serialize};

/// Configuration for automatic stale task detection and crash recovery.
///
/// - CLAIMED tasks that never start: safe to auto-requeue
/// - RUNNING tasks that go stale: mark as FAILED (may not be idempotent)
///
/// All time values are in milliseconds.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// Milliseconds without runner heartbeat before RUNNING task is stale (1s–2hr).
    #[serde(default = "default_running_stale_threshold")]
    pub running_stale_threshold_ms: u64,

    /// Milliseconds a task may remain in finalization before the stale-RUNNING
    /// reaper may reclaim it. A worker stamps `finalizing_at` when it begins the
    /// two-phase finalize; until this threshold elapses the reaper skips the row
    /// even though its runner heartbeat has stopped (1s–2hr).
    #[serde(default = "default_finalizing_stale_threshold")]
    pub finalizing_stale_threshold_ms: u64,

    /// Grace before the workflow reaper (Case 1.7) recovers a task that is
    /// terminal but whose workflow_task is not yet advanced. Finalize is two
    /// transactions (Phase 1 marks the task terminal; Phase 2 advances the DAG);
    /// without a grace the reaper recovers a task whose Phase 2 is merely in
    /// flight, adding latency and "recovered" log noise (recovery is idempotent,
    /// so this is not a correctness issue). Decoupled from the heartbeat-validated
    /// thresholds so it can be tuned independently. `0` disables (immediate
    /// recovery, legacy behavior); range 0–3_600_000.
    #[serde(default = "default_crashed_worker_recovery_grace")]
    pub crashed_worker_recovery_grace_ms: u64,

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

    /// How long to keep heartbeat rows in hours. None disables pruning.
    #[serde(default = "default_heartbeat_retention")]
    pub heartbeat_retention_hours: Option<u32>,

    /// How long to keep worker_state snapshots in hours. None disables pruning.
    #[serde(default = "default_worker_state_retention")]
    pub worker_state_retention_hours: Option<u32>,

    /// How long to keep terminal task/workflow rows in hours. None disables pruning.
    #[serde(default = "default_terminal_record_retention")]
    pub terminal_record_retention_hours: Option<u32>,

    /// Per-queue overrides of `terminal_record_retention_hours` for plain
    /// (non-workflow) tasks; queues not listed use the global window.
    /// Overrides apply even when the global window is `None`. Workflow-backing
    /// task rows always age under the global window so a workflow and its task
    /// rows are retained as a unit. Values in hours, 1h–5y.
    #[serde(default)]
    pub queue_terminal_record_retention_hours: std::collections::HashMap<String, u32>,

    /// Seconds between retention sweep passes (30s–24h). Frequent small
    /// sweeps keep each pass short instead of accumulating an hourly spike.
    #[serde(default = "default_retention_sweep_interval")]
    pub retention_sweep_interval_s: u64,

    /// Rows per retention DELETE batch (50–10_000). Bounds per-statement
    /// duration, row locks, and WAL; each batch commits independently.
    #[serde(default = "default_retention_delete_batch_size")]
    pub retention_delete_batch_size: u32,
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
fn default_check_interval() -> u64 {
    30_000
}
fn default_heartbeat_interval() -> u64 {
    30_000
}
fn default_worker_state_snapshot_interval() -> u64 {
    30_000
}
fn default_heartbeat_retention() -> Option<u32> {
    Some(24)
}
fn default_worker_state_retention() -> Option<u32> {
    Some(24 * 7)
}
fn default_terminal_record_retention() -> Option<u32> {
    Some(24 * 30)
}
fn default_retention_sweep_interval() -> u64 {
    300
}
fn default_retention_delete_batch_size() -> u32 {
    500
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            auto_requeue_stale_claimed: true,
            claimed_stale_threshold_ms: 120_000,
            auto_fail_stale_running: true,
            running_stale_threshold_ms: 300_000,
            finalizing_stale_threshold_ms: 300_000,
            crashed_worker_recovery_grace_ms: 10_000,
            check_interval_ms: 30_000,
            runner_heartbeat_interval_ms: 30_000,
            claimer_heartbeat_interval_ms: 30_000,
            worker_state_snapshot_interval_ms: 30_000,
            heartbeat_retention_hours: Some(24),
            worker_state_retention_hours: Some(24 * 7),
            terminal_record_retention_hours: Some(24 * 30),
            queue_terminal_record_retention_hours: std::collections::HashMap::new(),
            retention_sweep_interval_s: 300,
            retention_delete_batch_size: 500,
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

    #[error(
        "queue_terminal_record_retention_hours['{queue}'] ({value}h) must be within 1..={max}h"
    )]
    QueueRetentionOutOfRange { queue: String, value: u32, max: u32 },

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

        // Maximum bounds per doc-comment ranges.
        if self.check_interval_ms > 600_000 {
            errors.push(RecoveryConfigError::AboveMaximum {
                field: "check_interval_ms",
                value: self.check_interval_ms,
                max: 600_000,
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

        // Per-queue retention overrides: hours bounded 1h–5y per queue.
        const QUEUE_RETENTION_MAX_HOURS: u32 = 24 * 365 * 5;
        let mut override_queues: Vec<&String> =
            self.queue_terminal_record_retention_hours.keys().collect();
        override_queues.sort();
        for queue in override_queues {
            let value = self.queue_terminal_record_retention_hours[queue];
            if !(1..=QUEUE_RETENTION_MAX_HOURS).contains(&value) {
                errors.push(RecoveryConfigError::QueueRetentionOutOfRange {
                    queue: queue.clone(),
                    value,
                    max: QUEUE_RETENTION_MAX_HOURS,
                });
            }
        }

        // Retention sweep cadence (seconds) and batch size (rows).
        if self.retention_sweep_interval_s < 30 {
            errors.push(RecoveryConfigError::BelowMinimum {
                field: "retention_sweep_interval_s",
                value: self.retention_sweep_interval_s,
                min: 30,
            });
        }
        if self.retention_sweep_interval_s > 86_400 {
            errors.push(RecoveryConfigError::AboveMaximum {
                field: "retention_sweep_interval_s",
                value: self.retention_sweep_interval_s,
                max: 86_400,
            });
        }
        if self.retention_delete_batch_size < 50 {
            errors.push(RecoveryConfigError::BelowMinimum {
                field: "retention_delete_batch_size",
                value: u64::from(self.retention_delete_batch_size),
                min: 50,
            });
        }
        if self.retention_delete_batch_size > 10_000 {
            errors.push(RecoveryConfigError::AboveMaximum {
                field: "retention_delete_batch_size",
                value: u64::from(self.retention_delete_batch_size),
                max: 10_000,
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
    fn retention_sweep_and_batch_defaults() {
        assert_eq!(RecoveryConfig::default().retention_sweep_interval_s, 300);
        assert_eq!(RecoveryConfig::default().retention_delete_batch_size, 500);
        let from_empty: RecoveryConfig = serde_json::from_str("{}").expect("defaults deserialize");
        assert_eq!(from_empty.retention_sweep_interval_s, 300);
        assert_eq!(from_empty.retention_delete_batch_size, 500);
    }

    #[test]
    fn retention_sweep_interval_bounds() {
        let at_min = RecoveryConfig {
            retention_sweep_interval_s: 30,
            ..Default::default()
        };
        assert!(at_min.validate().is_empty());

        let at_max = RecoveryConfig {
            retention_sweep_interval_s: 86_400,
            ..Default::default()
        };
        assert!(at_max.validate().is_empty());

        let below = RecoveryConfig {
            retention_sweep_interval_s: 29,
            ..Default::default()
        };
        let errors = below.validate();
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0],
            RecoveryConfigError::BelowMinimum {
                field: "retention_sweep_interval_s",
                ..
            }
        ));

        let above = RecoveryConfig {
            retention_sweep_interval_s: 86_401,
            ..Default::default()
        };
        let errors = above.validate();
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0],
            RecoveryConfigError::AboveMaximum {
                field: "retention_sweep_interval_s",
                ..
            }
        ));
    }

    #[test]
    fn retention_delete_batch_size_bounds() {
        let at_min = RecoveryConfig {
            retention_delete_batch_size: 50,
            ..Default::default()
        };
        assert!(at_min.validate().is_empty());

        let at_max = RecoveryConfig {
            retention_delete_batch_size: 10_000,
            ..Default::default()
        };
        assert!(at_max.validate().is_empty());

        let below = RecoveryConfig {
            retention_delete_batch_size: 49,
            ..Default::default()
        };
        let errors = below.validate();
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0],
            RecoveryConfigError::BelowMinimum {
                field: "retention_delete_batch_size",
                ..
            }
        ));

        let above = RecoveryConfig {
            retention_delete_batch_size: 10_001,
            ..Default::default()
        };
        let errors = above.validate();
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0],
            RecoveryConfigError::AboveMaximum {
                field: "retention_delete_batch_size",
                ..
            }
        ));
    }

    #[test]
    fn queue_retention_override_bounds() {
        let mk = |value: u32| RecoveryConfig {
            queue_terminal_record_retention_hours: std::collections::HashMap::from([(
                "metrics".to_owned(),
                value,
            )]),
            ..Default::default()
        };

        assert!(mk(1).validate().is_empty());
        assert!(mk(24 * 365 * 5).validate().is_empty());
        assert!(RecoveryConfig::default().validate().is_empty());

        for bad in [0, 24 * 365 * 5 + 1] {
            let errors = mk(bad).validate();
            assert_eq!(errors.len(), 1);
            assert!(matches!(
                &errors[0],
                RecoveryConfigError::QueueRetentionOutOfRange { queue, value, .. }
                    if queue == "metrics" && *value == bad
            ));
        }
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
