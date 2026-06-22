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

    /// How long to keep heartbeat rows in hours. None disables pruning.
    #[serde(default = "default_heartbeat_retention")]
    pub heartbeat_retention_hours: Option<u32>,

    /// How long to keep worker_state snapshots in hours. None disables pruning.
    #[serde(default = "default_worker_state_retention")]
    pub worker_state_retention_hours: Option<u32>,

    /// How long to keep terminal task/workflow rows in hours. None disables pruning.
    #[serde(default = "default_terminal_record_retention")]
    pub terminal_record_retention_hours: Option<u32>,
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
fn default_heartbeat_retention() -> Option<u32> {
    Some(24)
}
fn default_worker_state_retention() -> Option<u32> {
    Some(24 * 7)
}
fn default_terminal_record_retention() -> Option<u32> {
    Some(24 * 30)
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
            heartbeat_retention_hours: Some(24),
            worker_state_retention_hours: Some(24 * 7),
            terminal_record_retention_hours: Some(24 * 30),
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

    #[error("{field} ({value}ms) must be >= {min}ms")]
    BelowMinimum {
        field: &'static str,
        value: u64,
        min: u64,
    },

    #[error("{field} ({value}ms) must be <= {max}ms")]
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
