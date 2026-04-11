use serde::{Deserialize, Serialize};

use super::broker::PostgresConfig;
use super::queue::{CustomQueueConfig, QueueMode};
use super::recovery::RecoveryConfig;
use super::resilience::WorkerResilienceConfig;
use super::schedule::ScheduleConfig;
use crate::core::defaults::{DEFAULT_CLAIM_LEASE_MS, MAX_CLAIM_RENEW_AGE_MS};

/// Application configuration.
///
/// Validated on construction via [`AppConfig::validate`]. Collects all
/// independent errors and returns them together (gated validation).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    /// Queue routing mode.
    #[serde(default = "default_queue_mode")]
    pub queue_mode: QueueMode,

    /// Custom queue definitions (required when queue_mode = Custom).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_queues: Option<Vec<CustomQueueConfig>>,

    /// PostgreSQL broker configuration.
    pub broker: PostgresConfig,

    /// Cluster-wide cap on concurrently RUNNING tasks. None = unlimited.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_wide_cap: Option<u32>,

    /// Prefetch buffer: 0 = hard cap mode, >0 = soft cap with lease.
    #[serde(default)]
    pub prefetch_buffer: u32,

    /// Claim lease duration in ms. Required when prefetch_buffer > 0.
    /// When set in hard cap mode (prefetch_buffer=0), overrides the default 60s
    /// lease used for crash-recovery safety.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claim_lease_ms: Option<u32>,

    /// Maximum age (ms) of a CLAIMED task that heartbeat will renew.
    /// Older claims are left to expire, preventing indefinite renewal of
    /// orphaned CLAIMED tasks after dispatch failure + requeue DB error.
    #[serde(default = "default_max_claim_renew_age_ms")]
    pub max_claim_renew_age_ms: u32,

    /// Recovery configuration.
    #[serde(default)]
    pub recovery: RecoveryConfig,

    /// Worker resilience configuration (DB retry backoff, poll interval).
    #[serde(default)]
    pub resilience: WorkerResilienceConfig,

    /// Scheduler configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule: Option<ScheduleConfig>,

    /// Auto-retry transient `ENQUEUE_FAILED` errors for task sends and workflow starts.
    ///
    /// When `true`, retries up to 3 times with exponential backoff (200ms, 400ms,
    /// 800ms cap). Only retries errors classified as retryable by
    /// [`BrokerError::is_retryable`]:
    ///
    /// - I/O errors, pool timeouts, pool closed, worker crashed, protocol/TLS errors
    /// - PostgreSQL connection exceptions (08xxx), admin shutdown (57P01),
    ///   crash shutdown (57P02), cannot connect now (57P03)
    /// - `ConnectionFailed`, `ListenerClosed`
    ///
    /// Non-retryable errors (`Serialization`, `Migration`, `InvalidStatus`,
    /// `PayloadMismatch`) are returned immediately.
    #[serde(default)]
    pub resend_on_transient_err: bool,
}

fn default_queue_mode() -> QueueMode {
    QueueMode::Default
}

fn default_max_claim_renew_age_ms() -> u32 {
    MAX_CLAIM_RENEW_AGE_MS
}

/// Validation error for AppConfig.
#[derive(Debug, Clone, thiserror::Error)]
pub enum AppConfigError {
    #[error("{0}")]
    QueueMode(String),
    #[error("{0}")]
    ClusterCap(String),
    #[error("{0}")]
    Prefetch(String),
    #[error("{0}")]
    Broker(String),
    #[error("{0}")]
    Recovery(String),
    #[error("{0}")]
    Resilience(String),
    #[error("{0}")]
    Schedule(String),
}

impl AppConfig {
    /// Create an `AppConfig` from only a database URL, using library defaults
    /// for all other settings.
    ///
    /// This is the intended lightweight constructor for examples and simple
    /// apps. Override specific fields with struct update syntax when needed.
    ///
    /// ```rust
    /// use horsies::{AppConfig, QueueMode, CustomQueueConfig};
    ///
    /// let config = AppConfig {
    ///     queue_mode: QueueMode::Custom,
    ///     custom_queues: Some(vec![CustomQueueConfig {
    ///         name: "critical".into(),
    ///         priority: 1,
    ///         max_concurrency: 10,
    ///     }]),
    ///     ..AppConfig::for_database_url("postgresql://localhost/mydb")
    /// };
    /// ```
    pub fn for_database_url(url: impl Into<String>) -> Self {
        Self {
            queue_mode: default_queue_mode(),
            custom_queues: None,
            broker: PostgresConfig::from_url(url),
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: default_max_claim_renew_age_ms(),
            recovery: RecoveryConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            schedule: None,
            resend_on_transient_err: false,
        }
    }

    /// Format the configuration as a human-readable string for logging.
    ///
    /// Masks the password in the database URL to avoid leaking credentials.
    /// Mirrors Python's `AppConfig.log_config()` / `_format_for_logging()`.
    pub fn format_for_logging(&self) -> String {
        let mut lines = Vec::new();

        // Queue mode and queues
        let mode_str = match self.queue_mode {
            QueueMode::Default => "DEFAULT",
            QueueMode::Custom => "CUSTOM",
        };
        lines.push(format!("  queue_mode: {}", mode_str));
        if let (QueueMode::Custom, Some(ref queues)) = (self.queue_mode, &self.custom_queues) {
            lines.push("  custom_queues:".to_owned());
            for queue in queues {
                lines.push(format!(
                    "    - {} (priority={}, max_concurrency={})",
                    queue.name, queue.priority, queue.max_concurrency,
                ));
            }
        }

        // Cluster-wide cap and prefetch settings
        if let Some(cap) = self.cluster_wide_cap {
            lines.push(format!("  cluster_wide_cap: {}", cap));
        }
        lines.push(format!("  prefetch_buffer: {}", self.prefetch_buffer));
        if let Some(lease) = self.claim_lease_ms {
            lines.push(format!("  claim_lease_ms: {}ms", lease));
        }

        // Broker config with masked password
        let masked_url = mask_database_url(&self.broker.database_url);
        lines.push("  broker:".to_owned());
        lines.push(format!("    database_url: {}", masked_url));
        lines.push(format!("    pool_size: {}", self.broker.pool_size));
        lines.push(format!("    max_overflow: {}", self.broker.max_overflow));

        // Recovery config
        lines.push("  recovery:".to_owned());
        lines.push(format!(
            "    auto_requeue_stale_claimed: {}",
            self.recovery.auto_requeue_stale_claimed,
        ));
        lines.push(format!(
            "    claimed_stale_threshold: {}ms",
            self.recovery.claimed_stale_threshold_ms,
        ));
        lines.push(format!(
            "    auto_fail_stale_running: {}",
            self.recovery.auto_fail_stale_running,
        ));
        lines.push(format!(
            "    running_stale_threshold: {}ms",
            self.recovery.running_stale_threshold_ms,
        ));
        lines.push(format!(
            "    check_interval: {}ms",
            self.recovery.check_interval_ms,
        ));
        lines.push(format!(
            "    heartbeat_intervals: runner={}ms, claimer={}ms",
            self.recovery.runner_heartbeat_interval_ms, self.recovery.claimer_heartbeat_interval_ms,
        ));

        // Resilience config
        lines.push("  resilience:".to_owned());
        lines.push(format!(
            "    db_retry_initial: {}ms",
            self.resilience.db_retry_initial_ms,
        ));
        lines.push(format!(
            "    db_retry_max: {}ms",
            self.resilience.db_retry_max_ms,
        ));
        lines.push(format!(
            "    db_retry_max_attempts: {} (0=infinite)",
            self.resilience.db_retry_max_attempts,
        ));
        lines.push(format!(
            "    notify_poll_interval: {}ms",
            self.resilience.notify_poll_interval_ms,
        ));

        // Schedule config
        if let Some(ref schedule) = self.schedule {
            lines.push("  schedule:".to_owned());
            lines.push(format!("    enabled: {}", schedule.enabled));
            lines.push(format!(
                "    schedules: {} schedule(s)",
                schedule.schedules.len(),
            ));
            for sched in &schedule.schedules {
                lines.push(format!("      - {}: {}", sched.name, sched.task_name,));
            }
            lines.push(format!(
                "    check_interval: {}s",
                schedule.check_interval_seconds,
            ));
        }

        lines.join("\n")
    }

    /// Validate the configuration. Returns all validation errors found.
    pub fn validate(&self) -> Vec<AppConfigError> {
        let mut errors = Vec::new();

        // Queue mode validation
        match self.queue_mode {
            QueueMode::Default => {
                if self.custom_queues.is_some() {
                    errors.push(AppConfigError::QueueMode(
                        "custom_queues must be None in DEFAULT mode".to_owned(),
                    ));
                }
            }
            QueueMode::Custom => {
                match &self.custom_queues {
                    None => {
                        errors.push(AppConfigError::QueueMode(
                            "custom_queues required in CUSTOM mode".to_owned(),
                        ));
                    }
                    Some(queues) if queues.is_empty() => {
                        errors.push(AppConfigError::QueueMode(
                            "custom_queues must not be empty in CUSTOM mode".to_owned(),
                        ));
                    }
                    Some(queues) => {
                        // Check for duplicate names
                        let names: Vec<&str> = queues.iter().map(|q| q.name.as_str()).collect();
                        let mut seen = std::collections::HashSet::new();
                        for name in &names {
                            if !seen.insert(name) {
                                errors.push(AppConfigError::QueueMode(format!(
                                    "duplicate queue name: '{}'",
                                    name
                                )));
                            }
                        }
                        // Validate individual queue configs (name non-empty, priority 1-100)
                        for queue in queues {
                            for e in queue.validate() {
                                errors.push(AppConfigError::QueueMode(e.to_string()));
                            }
                        }
                    }
                }
            }
        }

        // Cluster-wide cap
        if let Some(cap) = self.cluster_wide_cap {
            if cap == 0 {
                errors.push(AppConfigError::ClusterCap(
                    "cluster_wide_cap must be positive".to_owned(),
                ));
            }
        }

        // Prefetch + claim lease validation
        if self.prefetch_buffer > 0 && self.claim_lease_ms.is_none() {
            errors.push(AppConfigError::Prefetch(
                "claim_lease_ms required when prefetch_buffer > 0".to_owned(),
            ));
        }
        if let Some(lease) = self.claim_lease_ms {
            if lease == 0 {
                errors.push(AppConfigError::Prefetch(
                    "claim_lease_ms must be positive".to_owned(),
                ));
            }
        }
        // Note: claim_lease_ms IS allowed in hard cap mode (prefetch_buffer=0).
        // When set, it overrides the default 60s lease used for crash-recovery safety.

        // Claim lease vs claimer heartbeat: lease must be >= 2x heartbeat
        // to survive a full heartbeat cycle without premature expiry.
        let effective_lease_ms = self.claim_lease_ms.unwrap_or(DEFAULT_CLAIM_LEASE_MS);
        let lease_value_valid =
            self.claim_lease_ms.is_none() || self.claim_lease_ms.unwrap_or(0) > 0;
        let min_lease = self.recovery.claimer_heartbeat_interval_ms as u32 * 2;
        if lease_value_valid && effective_lease_ms < min_lease {
            let source = match self.claim_lease_ms {
                Some(v) => format!("claim_lease_ms={}", v),
                None => format!("default lease={}ms", DEFAULT_CLAIM_LEASE_MS),
            };
            errors.push(AppConfigError::Prefetch(format!(
                "claim lease too short relative to claimer heartbeat: \
                 effective lease: {}ms ({}), \
                 claimer_heartbeat_interval_ms: {}ms, \
                 minimum lease: {}ms (2x heartbeat)",
                effective_lease_ms, source, self.recovery.claimer_heartbeat_interval_ms, min_lease,
            )));
        }

        // max_claim_renew_age_ms must be >= effective lease.
        if lease_value_valid
            && self.max_claim_renew_age_ms > 0
            && self.max_claim_renew_age_ms < effective_lease_ms
        {
            errors.push(AppConfigError::Prefetch(format!(
                "max_claim_renew_age_ms must be >= effective claim lease: \
                 max_claim_renew_age_ms={}ms, effective lease={}ms",
                self.max_claim_renew_age_ms, effective_lease_ms,
            )));
        }

        // Prefetch + cluster cap conflict
        if self.cluster_wide_cap.is_some() && self.prefetch_buffer > 0 {
            errors.push(AppConfigError::ClusterCap(
                "cluster_wide_cap incompatible with prefetch mode".to_owned(),
            ));
        }

        // Broker validation
        if let Err(e) = self.broker.validate() {
            errors.push(AppConfigError::Broker(e.to_string()));
        }

        // Recovery validation
        for e in self.recovery.validate() {
            errors.push(AppConfigError::Recovery(e.to_string()));
        }

        // Resilience validation
        for e in self.resilience.validate() {
            errors.push(AppConfigError::Resilience(e.to_string()));
        }

        errors
    }
}

/// Mask the password in a PostgreSQL database URL for secure logging.
///
/// Replaces the password portion of `user:pass@host` with `***`.
/// If the URL cannot be parsed, returns the original URL.
/// Uses the **last** `@` as the credentials/host separator to handle
/// passwords that contain literal `@` characters.
///
/// Mirrors Python's `AppConfig._mask_database_url()`.
pub fn mask_database_url(url: &str) -> String {
    // Find the `://` separator first.
    let Some(scheme_end) = url.find("://") else {
        return url.to_owned();
    };
    let after_scheme = &url[scheme_end + 3..];

    // Use the LAST `@` as the separator (handles `@` in passwords).
    let Some(at_pos) = after_scheme.rfind('@') else {
        return url.to_owned(); // No credentials
    };

    let userinfo = &after_scheme[..at_pos];

    // Find the first `:` in userinfo that separates username from password.
    let Some(colon_pos) = userinfo.find(':') else {
        return url.to_owned(); // No password
    };

    let username = &userinfo[..colon_pos];
    let host_and_rest = &after_scheme[at_pos..]; // includes the '@'

    format!("{}://{}:***{}", &url[..scheme_end], username, host_and_rest,)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::broker::PostgresConfig;
    use crate::core::config::resilience::WorkerResilienceConfig;

    fn valid_broker() -> PostgresConfig {
        PostgresConfig {
            database_url: "postgresql://localhost/test".to_owned(),
            pool_pre_ping: true,
            pool_size: 30,
            max_overflow: 30,
            pool_timeout: 30,
            pool_recycle: 1800,
            echo: false,
        }
    }

    #[test]
    fn valid_default_mode() {
        let config = AppConfig {
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: valid_broker(),
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            schedule: None,
            resend_on_transient_err: false,
        };
        assert!(config.validate().is_empty());
    }

    #[test]
    fn custom_queues_in_default_mode() {
        let config = AppConfig {
            queue_mode: QueueMode::Default,
            custom_queues: Some(vec![CustomQueueConfig {
                name: "high".to_owned(),
                priority: 1,
                max_concurrency: 5,
            }]),
            broker: valid_broker(),
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            schedule: None,
            resend_on_transient_err: false,
        };
        let errors = config.validate();
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn missing_custom_queues_in_custom_mode() {
        let config = AppConfig {
            queue_mode: QueueMode::Custom,
            custom_queues: None,
            broker: valid_broker(),
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            schedule: None,
            resend_on_transient_err: false,
        };
        let errors = config.validate();
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn prefetch_without_lease() {
        let config = AppConfig {
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: valid_broker(),
            cluster_wide_cap: None,
            prefetch_buffer: 5,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            schedule: None,
            resend_on_transient_err: false,
        };
        let errors = config.validate();
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn cluster_cap_with_prefetch_conflict() {
        let config = AppConfig {
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: valid_broker(),
            cluster_wide_cap: Some(100),
            prefetch_buffer: 5,
            // Must be >= 2x claimer_heartbeat_interval_ms (default 30_000ms).
            claim_lease_ms: Some(60_000),
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            schedule: None,
            resend_on_transient_err: false,
        };
        let errors = config.validate();
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn multiple_errors_collected() {
        let config = AppConfig {
            queue_mode: QueueMode::Custom,
            custom_queues: None,
            broker: PostgresConfig {
                database_url: "mysql://localhost/db".to_owned(),
                pool_pre_ping: true,
                pool_size: 30,
                max_overflow: 30,
                pool_timeout: 30,
                pool_recycle: 1800,
                echo: false,
            },
            cluster_wide_cap: Some(0),
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            schedule: None,
            resend_on_transient_err: false,
        };
        let errors = config.validate();
        // custom_queues missing + invalid broker url + cluster_wide_cap=0
        assert_eq!(errors.len(), 3);
    }

    #[test]
    fn queue_priority_out_of_range_rejected() {
        let config = AppConfig {
            queue_mode: QueueMode::Custom,
            custom_queues: Some(vec![CustomQueueConfig {
                name: "bad".to_owned(),
                priority: 0,
                max_concurrency: 5,
            }]),
            broker: valid_broker(),
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            schedule: None,
            resend_on_transient_err: false,
        };
        let errors = config.validate();
        assert!(
            errors.iter().any(|e| e.to_string().contains("priority")),
            "expected priority validation error, got: {:?}",
            errors,
        );
    }

    #[test]
    fn queue_priority_101_rejected() {
        let config = AppConfig {
            queue_mode: QueueMode::Custom,
            custom_queues: Some(vec![CustomQueueConfig {
                name: "over".to_owned(),
                priority: 101,
                max_concurrency: 5,
            }]),
            broker: valid_broker(),
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            schedule: None,
            resend_on_transient_err: false,
        };
        let errors = config.validate();
        assert!(
            errors.iter().any(|e| e.to_string().contains("priority")),
            "expected priority validation error, got: {:?}",
            errors,
        );
    }

    // --- mask_database_url tests ---

    #[test]
    fn mask_url_with_password() {
        let masked = mask_database_url("postgresql://user:secret@localhost/db");
        assert_eq!(masked, "postgresql://user:***@localhost/db");
    }

    #[test]
    fn mask_url_without_password() {
        let masked = mask_database_url("postgresql://localhost/db");
        assert_eq!(masked, "postgresql://localhost/db");
    }

    #[test]
    fn mask_url_without_at() {
        let masked = mask_database_url("postgresql://localhost/db");
        assert_eq!(masked, "postgresql://localhost/db");
    }

    #[test]
    fn mask_url_with_complex_password() {
        let masked = mask_database_url("postgresql://admin:p@ss:w0rd@db.host.com:5432/mydb");
        // Uses last @ as separator, masks everything between first : and last @.
        assert_eq!(masked, "postgresql://admin:***@db.host.com:5432/mydb");
    }

    #[test]
    fn mask_url_no_scheme() {
        let masked = mask_database_url("not-a-url");
        assert_eq!(masked, "not-a-url");
    }

    // --- format_for_logging tests ---

    #[test]
    fn format_for_logging_masks_password() {
        let config = AppConfig {
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: PostgresConfig {
                database_url: "postgresql://user:secret@localhost/db".to_owned(),
                pool_pre_ping: true,
                pool_size: 30,
                max_overflow: 30,
                pool_timeout: 30,
                pool_recycle: 1800,
                echo: false,
            },
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            schedule: None,
            resend_on_transient_err: false,
        };
        let output = config.format_for_logging();
        assert!(!output.contains("secret"), "password should be masked");
        assert!(
            output.contains("***"),
            "should contain masked password marker"
        );
        assert!(output.contains("queue_mode: DEFAULT"));
    }

    // --- Helper for reducing config boilerplate ---

    fn default_config() -> AppConfig {
        AppConfig {
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: valid_broker(),
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: MAX_CLAIM_RENEW_AGE_MS,
            recovery: RecoveryConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            schedule: None,
            resend_on_transient_err: false,
        }
    }

    // --- Claim lease validation (ported from Python test_app_config.py) ---

    #[test]
    fn claim_lease_ms_none_valid_with_no_prefetch() {
        let config = default_config();
        assert!(config.validate().is_empty());
    }

    #[test]
    fn claim_lease_ms_with_no_prefetch_accepted() {
        let mut config = default_config();
        // Must be >= 2x claimer_heartbeat_interval_ms (default 30_000ms).
        config.claim_lease_ms = Some(60_000);
        assert!(config.validate().is_empty());
    }

    #[test]
    fn claim_lease_ms_zero_raises() {
        let mut config = default_config();
        config.claim_lease_ms = Some(0);
        let errors = config.validate();
        assert!(!errors.is_empty());
        assert!(errors
            .iter()
            .any(|e| e.to_string().contains("claim_lease_ms")));
    }

    #[test]
    fn claim_lease_ms_positive_is_valid() {
        let mut config = default_config();
        // Must be >= 2x claimer_heartbeat_interval_ms (default 30_000ms).
        config.claim_lease_ms = Some(60_000);
        assert!(config.validate().is_empty());
    }

    // --- Default behavior (ported from Python test_app_config.py) ---

    #[test]
    fn defaults_queue_mode_is_default() {
        let config = default_config();
        assert!(matches!(config.queue_mode, QueueMode::Default));
    }

    #[test]
    fn defaults_cluster_wide_cap_is_none() {
        let config = default_config();
        assert!(config.cluster_wide_cap.is_none());
    }

    #[test]
    fn defaults_custom_queues_is_none() {
        let config = default_config();
        assert!(config.custom_queues.is_none());
    }

    #[test]
    fn for_database_url_uses_expected_defaults() {
        let config = AppConfig::for_database_url("postgresql://localhost/test");
        assert!(matches!(config.queue_mode, QueueMode::Default));
        assert!(config.custom_queues.is_none());
        assert_eq!(config.broker.database_url, "postgresql://localhost/test");
        assert_eq!(config.prefetch_buffer, 0);
        assert!(config.claim_lease_ms.is_none());
        assert_eq!(config.max_claim_renew_age_ms, MAX_CLAIM_RENEW_AGE_MS);
        assert!(config.schedule.is_none());
        assert!(!config.resend_on_transient_err);
    }

    #[test]
    fn defaults_prefetch_buffer_is_zero() {
        let config = default_config();
        assert_eq!(config.prefetch_buffer, 0);
    }

    #[test]
    fn defaults_max_claim_renew_age_ms() {
        let config = default_config();
        assert_eq!(config.max_claim_renew_age_ms, MAX_CLAIM_RENEW_AGE_MS);
    }

    // --- Multi-error collection (ported from Python test_app_config.py) ---

    #[test]
    fn multi_error_cluster_cap_and_prefetch() {
        let mut config = default_config();
        config.cluster_wide_cap = Some(10);
        config.prefetch_buffer = 5;
        // No claim_lease_ms => prefetch error + cluster_cap+prefetch conflict
        let errors = config.validate();
        assert!(errors.len() >= 2);
    }

    #[test]
    fn single_error_returns_one_error() {
        let mut config = default_config();
        config.claim_lease_ms = Some(0);
        let errors = config.validate();
        assert_eq!(errors.len(), 1);
    }

    // --- Queue mode validation (ported from Python test_app_config.py) ---

    #[test]
    fn custom_mode_with_empty_queues_raises() {
        let mut config = default_config();
        config.queue_mode = QueueMode::Custom;
        config.custom_queues = Some(vec![]);
        let errors = config.validate();
        assert!(errors
            .iter()
            .any(|e| e.to_string().contains("must not be empty")));
    }

    #[test]
    fn custom_mode_with_duplicate_names_raises() {
        let mut config = default_config();
        config.queue_mode = QueueMode::Custom;
        config.custom_queues = Some(vec![
            CustomQueueConfig {
                name: "q1".to_owned(),
                priority: 1,
                max_concurrency: 5,
            },
            CustomQueueConfig {
                name: "q1".to_owned(),
                priority: 2,
                max_concurrency: 10,
            },
        ]);
        let errors = config.validate();
        assert!(errors.iter().any(|e| e.to_string().contains("duplicate")));
    }

    #[test]
    fn custom_mode_with_valid_queues() {
        let mut config = default_config();
        config.queue_mode = QueueMode::Custom;
        config.custom_queues = Some(vec![
            CustomQueueConfig {
                name: "high".to_owned(),
                priority: 1,
                max_concurrency: 5,
            },
            CustomQueueConfig {
                name: "low".to_owned(),
                priority: 50,
                max_concurrency: 10,
            },
        ]);
        assert!(config.validate().is_empty());
    }

    // --- Cluster cap boundaries (ported from Python test_app_config.py) ---

    #[test]
    fn cluster_wide_cap_zero_raises() {
        let mut config = default_config();
        config.cluster_wide_cap = Some(0);
        let errors = config.validate();
        assert!(errors
            .iter()
            .any(|e| e.to_string().contains("cluster_wide_cap")));
    }

    #[test]
    fn cluster_wide_cap_minimum_valid() {
        let mut config = default_config();
        config.cluster_wide_cap = Some(1);
        assert!(config.validate().is_empty());
    }

    #[test]
    fn cluster_wide_cap_with_no_prefetch_valid() {
        let mut config = default_config();
        config.cluster_wide_cap = Some(10);
        // prefetch_buffer is 0 (default) — no conflict
        assert!(config.validate().is_empty());
    }

    // --- Format for logging (ported from Python test_app_config.py) ---

    #[test]
    fn format_for_logging_cluster_wide_cap_displayed() {
        let mut config = default_config();
        config.cluster_wide_cap = Some(50);
        let output = config.format_for_logging();
        assert!(output.contains("cluster_wide_cap: 50"));
    }

    #[test]
    fn format_for_logging_cluster_wide_cap_omitted_when_none() {
        let config = default_config();
        let output = config.format_for_logging();
        assert!(!output.contains("cluster_wide_cap"));
    }

    #[test]
    fn format_for_logging_claim_lease_displayed() {
        let mut config = default_config();
        config.claim_lease_ms = Some(5000);
        let output = config.format_for_logging();
        assert!(output.contains("claim_lease_ms: 5000ms"));
    }

    #[test]
    fn format_for_logging_claim_lease_omitted_when_none() {
        let config = default_config();
        let output = config.format_for_logging();
        assert!(!output.contains("claim_lease_ms"));
    }

    #[test]
    fn format_for_logging_custom_queues_listed() {
        let mut config = default_config();
        config.queue_mode = QueueMode::Custom;
        config.custom_queues = Some(vec![CustomQueueConfig {
            name: "high".to_owned(),
            priority: 1,
            max_concurrency: 5,
        }]);
        let output = config.format_for_logging();
        assert!(output.contains("queue_mode: CUSTOM"));
        assert!(output.contains("high"));
    }

    #[test]
    fn format_for_logging_schedule_displayed() {
        use crate::core::config::schedule::*;
        let mut config = default_config();
        config.schedule = Some(ScheduleConfig {
            enabled: true,
            schedules: vec![TaskSchedule {
                name: "daily-job".to_owned(),
                task_name: "my_task".to_owned(),
                pattern: SchedulePattern::Interval(IntervalSchedule {
                    seconds: Some(60),
                    minutes: None,
                    hours: None,
                    days: None,
                }),
                args: serde_json::Value::Null,
                kwargs: serde_json::Value::Null,
                queue_name: None,
                enabled: true,
                timezone: "UTC".to_owned(),
                catch_up_missed: false,
                max_catch_up_runs: 100,
            }],
            check_interval_seconds: 5,
        });
        let output = config.format_for_logging();
        assert!(output.contains("schedule:"));
        assert!(output.contains("daily-job"));
    }

    #[test]
    fn format_for_logging_schedule_omitted_when_none() {
        let config = default_config();
        let output = config.format_for_logging();
        assert!(!output.contains("schedule:"));
    }
}
