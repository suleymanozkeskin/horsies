use serde::{Deserialize, Serialize};

/// Configuration for worker resilience against transient database failures.
///
/// Controls exponential backoff for DB reconnection, maximum retry attempts,
/// and the fallback poll interval when LISTEN/NOTIFY is unavailable.
///
/// All time values are in milliseconds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerResilienceConfig {
    /// Initial delay before first DB retry (100ms–60s).
    #[serde(default = "default_db_retry_initial_ms")]
    pub db_retry_initial_ms: u64,

    /// Maximum delay between DB retries (500ms–5min).
    #[serde(default = "default_db_retry_max_ms")]
    pub db_retry_max_ms: u64,

    /// Maximum number of consecutive retry attempts. 0 = infinite (0–10000).
    #[serde(default)]
    pub db_retry_max_attempts: u64,

    /// Fallback poll interval when NOTIFY is unavailable (1s–5min).
    #[serde(default = "default_notify_poll_interval_ms")]
    pub notify_poll_interval_ms: u64,
}

fn default_db_retry_initial_ms() -> u64 {
    500
}
fn default_db_retry_max_ms() -> u64 {
    30_000
}
fn default_notify_poll_interval_ms() -> u64 {
    5_000
}

impl Default for WorkerResilienceConfig {
    fn default() -> Self {
        Self {
            db_retry_initial_ms: 500,
            db_retry_max_ms: 30_000,
            db_retry_max_attempts: 0,
            notify_poll_interval_ms: 5_000,
        }
    }
}

/// Validation error for WorkerResilienceConfig.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ResilienceConfigError {
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

    #[error("db_retry_max_ms ({max_ms}ms) must be >= db_retry_initial_ms ({initial_ms}ms)")]
    MaxBelowInitial { max_ms: u64, initial_ms: u64 },
}

impl WorkerResilienceConfig {
    /// Validate resilience config fields.
    ///
    /// Checks:
    /// - Range bounds for each field
    /// - `db_retry_max_ms >= db_retry_initial_ms`
    ///
    /// Returns all validation errors found, not just the first.
    pub fn validate(&self) -> Vec<ResilienceConfigError> {
        let mut errors = Vec::new();

        // db_retry_initial_ms: [100, 60000]
        if self.db_retry_initial_ms < 100 {
            errors.push(ResilienceConfigError::BelowMinimum {
                field: "db_retry_initial_ms",
                value: self.db_retry_initial_ms,
                min: 100,
            });
        }
        if self.db_retry_initial_ms > 60_000 {
            errors.push(ResilienceConfigError::AboveMaximum {
                field: "db_retry_initial_ms",
                value: self.db_retry_initial_ms,
                max: 60_000,
            });
        }

        // db_retry_max_ms: [500, 300000]
        if self.db_retry_max_ms < 500 {
            errors.push(ResilienceConfigError::BelowMinimum {
                field: "db_retry_max_ms",
                value: self.db_retry_max_ms,
                min: 500,
            });
        }
        if self.db_retry_max_ms > 300_000 {
            errors.push(ResilienceConfigError::AboveMaximum {
                field: "db_retry_max_ms",
                value: self.db_retry_max_ms,
                max: 300_000,
            });
        }

        // db_retry_max_attempts: [0, 10000]
        if self.db_retry_max_attempts > 10_000 {
            errors.push(ResilienceConfigError::AboveMaximum {
                field: "db_retry_max_attempts",
                value: self.db_retry_max_attempts,
                max: 10_000,
            });
        }

        // notify_poll_interval_ms: [1000, 300000]
        if self.notify_poll_interval_ms < 1_000 {
            errors.push(ResilienceConfigError::BelowMinimum {
                field: "notify_poll_interval_ms",
                value: self.notify_poll_interval_ms,
                min: 1_000,
            });
        }
        if self.notify_poll_interval_ms > 300_000 {
            errors.push(ResilienceConfigError::AboveMaximum {
                field: "notify_poll_interval_ms",
                value: self.notify_poll_interval_ms,
                max: 300_000,
            });
        }

        // Cross-field: db_retry_max_ms >= db_retry_initial_ms
        if self.db_retry_max_ms < self.db_retry_initial_ms {
            errors.push(ResilienceConfigError::MaxBelowInitial {
                max_ms: self.db_retry_max_ms,
                initial_ms: self.db_retry_initial_ms,
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
        let config = WorkerResilienceConfig::default();
        assert!(config.validate().is_empty());
    }

    #[test]
    fn default_field_values() {
        let config = WorkerResilienceConfig::default();
        assert_eq!(config.db_retry_initial_ms, 500);
        assert_eq!(config.db_retry_max_ms, 30_000);
        assert_eq!(config.db_retry_max_attempts, 0);
        assert_eq!(config.notify_poll_interval_ms, 5_000);
    }

    // --- BelowMinimum tests ---

    #[test]
    fn db_retry_initial_below_minimum() {
        let config = WorkerResilienceConfig {
            db_retry_initial_ms: 50,
            ..Default::default()
        };
        let errors = config.validate();
        assert!(errors.iter().any(|e| matches!(
            e,
            ResilienceConfigError::BelowMinimum {
                field: "db_retry_initial_ms",
                ..
            }
        )));
    }

    #[test]
    fn db_retry_max_below_minimum() {
        let config = WorkerResilienceConfig {
            db_retry_max_ms: 200,
            ..Default::default()
        };
        let errors = config.validate();
        assert!(errors.iter().any(|e| matches!(
            e,
            ResilienceConfigError::BelowMinimum {
                field: "db_retry_max_ms",
                ..
            }
        )));
    }

    #[test]
    fn notify_poll_below_minimum() {
        let config = WorkerResilienceConfig {
            notify_poll_interval_ms: 500,
            ..Default::default()
        };
        let errors = config.validate();
        assert!(errors.iter().any(|e| matches!(
            e,
            ResilienceConfigError::BelowMinimum {
                field: "notify_poll_interval_ms",
                ..
            }
        )));
    }

    // --- AboveMaximum tests ---

    #[test]
    fn db_retry_initial_above_maximum() {
        let config = WorkerResilienceConfig {
            db_retry_initial_ms: 60_001,
            db_retry_max_ms: 300_000,
            ..Default::default()
        };
        let errors = config.validate();
        assert!(errors.iter().any(|e| matches!(
            e,
            ResilienceConfigError::AboveMaximum {
                field: "db_retry_initial_ms",
                ..
            }
        )));
    }

    #[test]
    fn db_retry_max_above_maximum() {
        let config = WorkerResilienceConfig {
            db_retry_max_ms: 300_001,
            ..Default::default()
        };
        let errors = config.validate();
        assert!(errors.iter().any(|e| matches!(
            e,
            ResilienceConfigError::AboveMaximum {
                field: "db_retry_max_ms",
                ..
            }
        )));
    }

    #[test]
    fn db_retry_max_attempts_above_maximum() {
        let config = WorkerResilienceConfig {
            db_retry_max_attempts: 10_001,
            ..Default::default()
        };
        let errors = config.validate();
        assert!(errors.iter().any(|e| matches!(
            e,
            ResilienceConfigError::AboveMaximum {
                field: "db_retry_max_attempts",
                ..
            }
        )));
    }

    #[test]
    fn notify_poll_above_maximum() {
        let config = WorkerResilienceConfig {
            notify_poll_interval_ms: 300_001,
            ..Default::default()
        };
        let errors = config.validate();
        assert!(errors.iter().any(|e| matches!(
            e,
            ResilienceConfigError::AboveMaximum {
                field: "notify_poll_interval_ms",
                ..
            }
        )));
    }

    // --- Cross-field validation ---

    #[test]
    fn max_below_initial_rejected() {
        let config = WorkerResilienceConfig {
            db_retry_initial_ms: 5_000,
            db_retry_max_ms: 1_000,
            ..Default::default()
        };
        let errors = config.validate();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ResilienceConfigError::MaxBelowInitial { .. })));
    }

    #[test]
    fn max_equal_to_initial_passes() {
        let config = WorkerResilienceConfig {
            db_retry_initial_ms: 500,
            db_retry_max_ms: 500,
            ..Default::default()
        };
        let errors = config.validate();
        assert!(
            !errors
                .iter()
                .any(|e| matches!(e, ResilienceConfigError::MaxBelowInitial { .. })),
            "equal values should pass cross-field check"
        );
    }

    // --- Edge cases: exactly at boundaries ---

    #[test]
    fn all_at_minimum_boundary_passes() {
        let config = WorkerResilienceConfig {
            db_retry_initial_ms: 100,
            db_retry_max_ms: 500,
            db_retry_max_attempts: 0,
            notify_poll_interval_ms: 1_000,
        };
        let errors = config.validate();
        assert!(errors.is_empty(), "got errors: {:?}", errors);
    }

    #[test]
    fn all_at_maximum_boundary_passes() {
        let config = WorkerResilienceConfig {
            db_retry_initial_ms: 60_000,
            db_retry_max_ms: 300_000,
            db_retry_max_attempts: 10_000,
            notify_poll_interval_ms: 300_000,
        };
        let errors = config.validate();
        assert!(errors.is_empty(), "got errors: {:?}", errors);
    }

    // --- Multiple violations ---

    #[test]
    fn multiple_violations_collected() {
        let config = WorkerResilienceConfig {
            db_retry_initial_ms: 50,
            db_retry_max_ms: 200,
            db_retry_max_attempts: 10_001,
            notify_poll_interval_ms: 500,
        };
        let errors = config.validate();
        // At least: initial below min, max below min, attempts above max,
        // poll below min, max < initial
        assert!(
            errors.len() >= 4,
            "expected at least 4 errors, got {}: {:?}",
            errors.len(),
            errors,
        );
    }

    // --- Serde roundtrip ---

    #[test]
    fn serde_defaults_applied() {
        let json = "{}";
        let config: WorkerResilienceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.db_retry_initial_ms, 500);
        assert_eq!(config.db_retry_max_ms, 30_000);
        assert_eq!(config.db_retry_max_attempts, 0);
        assert_eq!(config.notify_poll_interval_ms, 5_000);
    }

    #[test]
    fn serde_roundtrip() {
        let config = WorkerResilienceConfig {
            db_retry_initial_ms: 1_000,
            db_retry_max_ms: 10_000,
            db_retry_max_attempts: 5,
            notify_poll_interval_ms: 3_000,
        };
        let json = serde_json::to_string(&config).unwrap();
        let restored: WorkerResilienceConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.db_retry_initial_ms, 1_000);
        assert_eq!(restored.db_retry_max_ms, 10_000);
        assert_eq!(restored.db_retry_max_attempts, 5);
        assert_eq!(restored.notify_poll_interval_ms, 3_000);
    }
}
