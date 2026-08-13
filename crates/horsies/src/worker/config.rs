use std::collections::HashMap;

use crate::core::AppConfig;

use crate::worker::cli::LogLevel;

/// Worker-specific configuration.
///
/// Combined with `AppConfig` (which provides broker, recovery, prefetch settings)
/// to fully configure a worker instance.
#[derive(Clone)]
pub struct WorkerConfig {
    /// Queues this worker will serve.
    pub queues: Vec<String>,

    /// Maximum number of tasks executing concurrently.
    pub concurrency: u32,

    /// Per-queue claim limit per pass. `0` (the default) auto-fills the
    /// available local/global budget; a positive value is an explicit per-queue
    /// fairness cap. Mirrors Python's `WorkerConfig.max_claim_batch`.
    pub max_claim_batch: u32,

    /// Per-worker limit on total CLAIMED tasks to prevent over-claiming.
    /// 0 = auto (defaults to `concurrency` in hard-cap mode, or
    /// `concurrency + prefetch_buffer` in soft-cap mode).
    /// Mirrors Python's `WorkerConfig.max_claim_per_worker`.
    pub max_claim_per_worker: u32,

    /// Optional priority ordering for queues (lower number = higher priority).
    /// When set, queues are claimed in priority order (greedy fill).
    /// When empty, queues are claimed round-robin with `max_claim_batch` fairness.
    pub queue_priorities: HashMap<String, i32>,

    /// Optional per-queue concurrency limits.
    pub queue_max_concurrency: HashMap<String, u32>,

    /// Number of queued NOTIFY messages to drain after waking up,
    /// preventing thundering herd on burst inserts.
    /// Mirrors Python's `WorkerConfig.coalesce_notifies` (default 100).
    pub coalesce_notifies: u32,

    /// Log level for this worker. Stored in the config so that it can be
    /// propagated or inspected at runtime (mirrors Python's
    /// `WorkerConfig.loglevel`). Defaults to `Info`.
    pub loglevel: LogLevel,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        let concurrency = std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(2);
        Self {
            queues: vec!["default".to_owned()],
            concurrency,
            max_claim_batch: 0,
            max_claim_per_worker: 0,
            queue_priorities: HashMap::new(),
            queue_max_concurrency: HashMap::new(),
            coalesce_notifies: 100,
            loglevel: LogLevel::Info,
        }
    }
}

impl WorkerConfig {
    /// Validate configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.queues.is_empty() {
            return Err("at least one queue must be specified".to_owned());
        }
        if self.concurrency == 0 {
            return Err("concurrency must be at least 1".to_owned());
        }
        // max_claim_batch == 0 is valid: it means auto-fill the available budget.
        Ok(())
    }

    /// Apply custom queue concurrency limits and priorities from AppConfig.
    ///
    /// For each custom queue defined in `app_config.custom_queues`, inserts
    /// `max_concurrency` and `priority` into this worker's maps if not already
    /// set (i.e. does not override explicit worker-level overrides).
    ///
    /// An uncapped queue (`max_concurrency = None`) is omitted from the
    /// concurrency map: the claim pass only counts and limits queues present in
    /// it, so an absent key is genuinely uncapped (no per-queue limit, no
    /// in-flight count query). Priority is still recorded.
    pub fn apply_queue_config(&mut self, app_config: &AppConfig) {
        if let Some(ref queues) = app_config.custom_queues {
            for q in queues {
                if let Some(cap) = q.max_concurrency {
                    self.queue_max_concurrency
                        .entry(q.name.clone())
                        .or_insert(cap);
                }
                self.queue_priorities
                    .entry(q.name.clone())
                    .or_insert(q.priority as i32);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_valid() {
        let config = WorkerConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.queues, vec!["default"]);
        assert!(config.concurrency >= 1);
        assert_eq!(config.max_claim_batch, 0, "default is auto-fill");
    }

    #[test]
    fn max_claim_batch_zero_is_valid() {
        let config = WorkerConfig {
            max_claim_batch: 0,
            ..Default::default()
        };
        assert!(config.validate().is_ok(), "0 = auto-fill, must be valid");
    }

    #[test]
    fn max_claim_batch_positive_is_valid() {
        let config = WorkerConfig {
            max_claim_batch: 5,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn empty_queues_rejected() {
        let config = WorkerConfig {
            queues: vec![],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn zero_concurrency_rejected() {
        let config = WorkerConfig {
            concurrency: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    // --- apply_queue_config tests ---

    fn app_config_with_custom_queues() -> AppConfig {
        use crate::core::{
            CustomQueueConfig, PostgresConfig, QueueMode, RecoveryConfig, WorkerResilienceConfig,
        };

        AppConfig {
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Custom,
            custom_queues: Some(vec![
                CustomQueueConfig {
                    name: "fast".to_owned(),
                    priority: 1,
                    max_concurrency: Some(10),
                },
                CustomQueueConfig {
                    name: "slow".to_owned(),
                    priority: 50,
                    max_concurrency: Some(5),
                },
            ]),
            broker: PostgresConfig {
                database_url: "postgresql://localhost/test".to_owned(),
                session_database_url: None,
                pgbouncer_transaction_mode: false,
                pool_pre_ping: true,
                pool_size: 30,
                max_overflow: 30,
                retain_rerun_input_default: false,
                pool_timeout: 30,
                pool_recycle: 1800,
                echo: false,
            },
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            retention: crate::core::RetentionConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            schedule: None,
            resend_on_transient_err: false,
        }
    }

    #[test]
    fn apply_queue_config_sets_priorities_and_concurrency() {
        let app_config = app_config_with_custom_queues();
        let mut worker = WorkerConfig::default();

        worker.apply_queue_config(&app_config);

        assert_eq!(worker.queue_priorities.get("fast"), Some(&1));
        assert_eq!(worker.queue_priorities.get("slow"), Some(&50));
        assert_eq!(worker.queue_max_concurrency.get("fast"), Some(&10));
        assert_eq!(worker.queue_max_concurrency.get("slow"), Some(&5));
    }

    #[test]
    fn apply_queue_config_omits_uncapped_queue() {
        use crate::core::CustomQueueConfig;

        let mut app_config = app_config_with_custom_queues();
        // A capped queue plus an uncapped one (max_concurrency = None).
        app_config.custom_queues = Some(vec![
            CustomQueueConfig {
                name: "fast".to_owned(),
                priority: 1,
                max_concurrency: Some(10),
            },
            CustomQueueConfig {
                name: "bulk".to_owned(),
                priority: 2,
                max_concurrency: None,
            },
        ]);

        let mut worker = WorkerConfig::default();
        worker.apply_queue_config(&app_config);

        // Uncapped queue keeps its priority but is absent from the concurrency map.
        assert_eq!(worker.queue_priorities.get("bulk"), Some(&2));
        assert!(!worker.queue_max_concurrency.contains_key("bulk"));
        // Capped sibling is still mapped.
        assert_eq!(worker.queue_max_concurrency.get("fast"), Some(&10));
    }

    #[test]
    fn apply_queue_config_does_not_override_existing_values() {
        let app_config = app_config_with_custom_queues();
        let mut worker = WorkerConfig::default();

        // Pre-set worker-level overrides for "fast".
        worker.queue_priorities.insert("fast".to_owned(), 99);
        worker.queue_max_concurrency.insert("fast".to_owned(), 42);

        worker.apply_queue_config(&app_config);

        // Worker-level overrides should be preserved.
        assert_eq!(worker.queue_priorities.get("fast"), Some(&99));
        assert_eq!(worker.queue_max_concurrency.get("fast"), Some(&42));
        // "slow" should still be inserted from app config.
        assert_eq!(worker.queue_priorities.get("slow"), Some(&50));
        assert_eq!(worker.queue_max_concurrency.get("slow"), Some(&5));
    }

    #[test]
    fn apply_queue_config_noop_when_no_custom_queues() {
        use crate::core::{PostgresConfig, QueueMode, RecoveryConfig, WorkerResilienceConfig};

        let app_config = AppConfig {
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: PostgresConfig {
                database_url: "postgresql://localhost/test".to_owned(),
                session_database_url: None,
                pgbouncer_transaction_mode: false,
                pool_pre_ping: true,
                pool_size: 30,
                max_overflow: 30,
                retain_rerun_input_default: false,
                pool_timeout: 30,
                pool_recycle: 1800,
                echo: false,
            },
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            retention: crate::core::RetentionConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            schedule: None,
            resend_on_transient_err: false,
        };

        let mut worker = WorkerConfig::default();
        worker.apply_queue_config(&app_config);

        assert!(worker.queue_priorities.is_empty());
        assert!(worker.queue_max_concurrency.is_empty());
    }
}
