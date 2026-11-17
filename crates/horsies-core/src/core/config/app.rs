use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::broker::PostgresConfig;
use super::payload::PayloadPolicy;
use super::queue::{CustomQueueConfig, QueueMode};
use super::recovery::RecoveryConfig;
use super::resilience::WorkerResilienceConfig;
use super::retention::RetentionConfig;
use super::schedule::ScheduleConfig;
use super::worker_profile::{
    describe_worker_profile, worker_profiles_claim_errors, worker_profiles_shape_errors,
    WorkerProfile,
};
use crate::core::defaults::{DEFAULT_CLAIM_LEASE_MS, MAX_CLAIM_RENEW_AGE_MS};
use crate::core::error::HorsiesError;

/// Application configuration.
///
/// Validated on construction via [`AppConfig::validate`]. Collects all
/// independent errors and returns them together (gated validation).
/// Deserialization refuses an unknown key in this struct and in every nested
/// config struct (Python `extra='forbid'`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

    /// Payload-size guardrail applied at the encode boundaries.
    #[serde(default)]
    pub payload: PayloadPolicy,

    /// Recovery configuration.
    #[serde(default)]
    pub recovery: RecoveryConfig,

    /// Data-lifecycle and task-history retention policy.
    #[serde(default)]
    pub retention: RetentionConfig,

    /// Worker resilience configuration (DB retry backoff, poll interval).
    #[serde(default)]
    pub resilience: WorkerResilienceConfig,

    /// Named worker settings: a worker runs the profile of its name. An app
    /// that runs no worker defines none. Every profile is checked with the
    /// app config, not only the one that a worker starts with.
    #[serde(default)]
    pub worker_profiles: BTreeMap<String, WorkerProfile>,

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
    Retention(String),
    #[error("{0}")]
    Resilience(String),
    #[error("{0}")]
    Schedule(String),
    #[error("{0}")]
    Payload(String),
    /// A worker profile refusal. The message names the field path; the
    /// error also carries Python's notes and help.
    #[error("{}", .0.message)]
    WorkerProfile(HorsiesError),
}

impl AppConfigError {
    /// The catalog code of the refusal, as Python's config models report it.
    ///
    /// `Payload` has none: Python refuses a payload threshold below 1 through
    /// a field bound (`Field(ge=1)`), which carries no catalog code.
    pub fn error_code(&self) -> Option<crate::core::error::ErrorCode> {
        use crate::core::error::ErrorCode;
        match self {
            Self::QueueMode(_) => Some(ErrorCode::ConfigInvalidQueueMode),
            Self::ClusterCap(_) => Some(ErrorCode::ConfigInvalidClusterCap),
            Self::Prefetch(_) => Some(ErrorCode::ConfigInvalidPrefetch),
            Self::Broker(_) => Some(ErrorCode::BrokerInvalidUrl),
            Self::Recovery(_) => Some(ErrorCode::ConfigInvalidRecovery),
            Self::Retention(_) => Some(ErrorCode::ConfigInvalidRetention),
            Self::Resilience(_) => Some(ErrorCode::ConfigInvalidResilience),
            Self::Schedule(_) => Some(ErrorCode::ConfigInvalidSchedule),
            Self::Payload(_) => None,
            Self::WorkerProfile(_) => Some(ErrorCode::ConfigInvalidWorkerProfile),
        }
    }

    /// The refusal as a [`crate::core::error::HorsiesError`] that carries
    /// its catalog code.
    ///
    /// A worker profile refusal keeps its notes and help.
    pub fn to_horsies_error(&self) -> HorsiesError {
        match self {
            Self::WorkerProfile(refusal) => refusal.clone(),
            Self::QueueMode(_)
            | Self::ClusterCap(_)
            | Self::Prefetch(_)
            | Self::Broker(_)
            | Self::Recovery(_)
            | Self::Retention(_)
            | Self::Resilience(_)
            | Self::Schedule(_)
            | Self::Payload(_) => {
                let converted = HorsiesError::new(self.to_string());
                match self.error_code() {
                    Some(code) => converted.with_code(code),
                    None => converted,
                }
            }
        }
    }
}

impl AppConfig {
    /// Create an `AppConfig` from only a database URL, using library defaults
    /// for all other settings.
    ///
    /// This is the intended lightweight constructor for examples and simple
    /// apps. Override specific fields with struct update syntax when needed.
    ///
    /// ```rust
    /// use horsies_core::{AppConfig, QueueMode, CustomQueueConfig};
    ///
    /// let config = AppConfig {
    ///     queue_mode: QueueMode::Custom,
    ///     custom_queues: Some(vec![CustomQueueConfig {
    ///         name: "critical".into(),
    ///         priority: 1,
    ///         max_concurrency: Some(10),
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
            payload: PayloadPolicy::default(),
            recovery: RecoveryConfig::default(),
            retention: RetentionConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            worker_profiles: BTreeMap::new(),
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
                let concurrency_label = match queue.max_concurrency {
                    Some(n) => n.to_string(),
                    None => "uncapped".to_owned(),
                };
                lines.push(format!(
                    "    - {} (priority={}, max_concurrency={})",
                    queue.name, queue.priority, concurrency_label,
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
        if let Some(session_database_url) = &self.broker.session_database_url {
            lines.push(format!(
                "    session_database_url: {}",
                mask_database_url(session_database_url),
            ));
        }
        lines.push(format!(
            "    pgbouncer_transaction_mode: {}",
            self.broker.pgbouncer_transaction_mode,
        ));
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
            "    orphan_task_audit_interval: {}ms",
            self.recovery.orphan_task_audit_interval_ms,
        ));
        lines.push(format!(
            "    heartbeat_intervals: runner={}ms, claimer={}ms",
            self.recovery.runner_heartbeat_interval_ms, self.recovery.claimer_heartbeat_interval_ms,
        ));
        lines.push(format!(
            "    worker_state_snapshot_interval: {}ms",
            self.recovery.worker_state_snapshot_interval_ms,
        ));

        lines.push("  retention:".to_owned());
        lines.push(format!(
            "    history_leaf_horizon_days: {}",
            self.retention.history_leaf_horizon_days,
        ));
        lines.push(format!(
            "    heartbeat_leaf_horizon_hours: {}",
            self.retention.heartbeat_leaf_horizon_hours,
        ));
        lines.push(format!(
            "    retention_periods: {}",
            self.retention.retention_periods.len(),
        ));
        lines.push(format!(
            "    queue_retention: {}",
            self.retention.queue_retention.len(),
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

        // Worker profiles, in name order
        if !self.worker_profiles.is_empty() {
            lines.push("  worker_profiles:".to_owned());
            for (name, profile) in &self.worker_profiles {
                lines.push(format!("    {name}: {}", describe_worker_profile(profile)));
            }
        }

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

        let declared_queues: Vec<&str> = match self.queue_mode {
            QueueMode::Default => vec!["default"],
            QueueMode::Custom => self
                .custom_queues
                .iter()
                .flatten()
                .map(|queue| queue.name.as_str())
                .collect(),
        };
        for error in self.retention.validate() {
            errors.push(AppConfigError::Retention(error.to_string()));
        }
        for error in self.retention.validate_queues(declared_queues) {
            errors.push(AppConfigError::Retention(error.to_string()));
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

        // max_claim_renew_age_ms must be positive (0 would renew no claim)
        // and >= effective lease.
        if self.max_claim_renew_age_ms == 0 {
            errors.push(AppConfigError::Prefetch(
                "max_claim_renew_age_ms must be positive".to_owned(),
            ));
        } else if lease_value_valid && self.max_claim_renew_age_ms < effective_lease_ms {
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

        // Payload guardrail validation
        for e in self.payload.validate() {
            errors.push(AppConfigError::Payload(e));
        }

        // Schedule section: checked with the app config, every error
        // collected (Python validates ScheduleConfig when AppConfig is built).
        if let Some(schedule) = &self.schedule {
            for e in schedule.errors() {
                errors.push(AppConfigError::Schedule(e));
            }
        }

        // Every worker profile is checked, not only the one that a worker
        // starts with, so a worker with a valid profile does not start from
        // a config with an invalid one.
        for refusal in worker_profiles_shape_errors(&self.worker_profiles) {
            errors.push(AppConfigError::WorkerProfile(refusal));
        }
        for refusal in worker_profiles_claim_errors(&self.worker_profiles, self.prefetch_buffer) {
            errors.push(AppConfigError::WorkerProfile(refusal));
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
            session_database_url: None,
            pgbouncer_transaction_mode: false,
            pool_pre_ping: true,
            worker_pool_pre_ping: false,
            pool_size: 30,
            max_overflow: 30,
            retain_rerun_input_default: false,
            pool_timeout: 30,
            pool_recycle: crate::manifest::PoolRecycle::after_seconds(1800),
            echo: false,
            tcp_keepalives: Default::default(),
            tcp_user_timeout: Default::default(),
        }
    }

    #[test]
    fn valid_default_mode() {
        let config = AppConfig {
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: valid_broker(),
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            retention: RetentionConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            worker_profiles: std::collections::BTreeMap::new(),
            schedule: None,
            resend_on_transient_err: false,
        };
        assert!(config.validate().is_empty());
    }

    /// Retention overrides must name declared queues ("default" in DEFAULT
    /// mode, custom_queues names in CUSTOM); a typo'd key would otherwise be
    /// a silent no-op. Parity with horsies PR #207.
    #[test]
    fn queue_retention_override_must_name_declared_queue() {
        let overrides = |queue: &str| RetentionConfig {
            queue_retention: std::collections::HashMap::from([(
                queue.to_owned(),
                Some(chrono::Duration::hours(24)),
            )]),
            ..Default::default()
        };

        // DEFAULT mode: only "default" is declared.
        let mut config = AppConfig {
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: valid_broker(),
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            retention: overrides("default"),
            resilience: WorkerResilienceConfig::default(),
            worker_profiles: std::collections::BTreeMap::new(),
            schedule: None,
            resend_on_transient_err: false,
        };
        assert!(config.validate().is_empty());
        config.recovery = RecoveryConfig::default();
        config.retention = overrides("metricz");
        let errors = config.validate();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].to_string().contains("metricz"));

        // CUSTOM mode: declared = custom queue names.
        config.queue_mode = QueueMode::Custom;
        config.custom_queues = Some(vec![CustomQueueConfig {
            name: "metrics".to_owned(),
            priority: 1,
            max_concurrency: Some(5),
        }]);
        config.retention = overrides("metrics");
        assert!(config.validate().is_empty());
        config.retention = overrides("default");
        let errors = config.validate();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].to_string().contains("default"));
    }

    #[test]
    fn custom_queues_in_default_mode() {
        let config = AppConfig {
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Default,
            custom_queues: Some(vec![CustomQueueConfig {
                name: "high".to_owned(),
                priority: 1,
                max_concurrency: Some(5),
            }]),
            broker: valid_broker(),
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            retention: RetentionConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            worker_profiles: std::collections::BTreeMap::new(),
            schedule: None,
            resend_on_transient_err: false,
        };
        let errors = config.validate();
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn missing_custom_queues_in_custom_mode() {
        let config = AppConfig {
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Custom,
            custom_queues: None,
            broker: valid_broker(),
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            retention: RetentionConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            worker_profiles: std::collections::BTreeMap::new(),
            schedule: None,
            resend_on_transient_err: false,
        };
        let errors = config.validate();
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn prefetch_without_lease() {
        let config = AppConfig {
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: valid_broker(),
            cluster_wide_cap: None,
            prefetch_buffer: 5,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            retention: RetentionConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            worker_profiles: std::collections::BTreeMap::new(),
            schedule: None,
            resend_on_transient_err: false,
        };
        let errors = config.validate();
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn cluster_cap_with_prefetch_conflict() {
        let config = AppConfig {
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: valid_broker(),
            cluster_wide_cap: Some(100),
            prefetch_buffer: 5,
            // Must be >= 2x claimer_heartbeat_interval_ms (default 30_000ms).
            claim_lease_ms: Some(60_000),
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            retention: RetentionConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            worker_profiles: std::collections::BTreeMap::new(),
            schedule: None,
            resend_on_transient_err: false,
        };
        let errors = config.validate();
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn multiple_errors_collected() {
        let config = AppConfig {
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Custom,
            custom_queues: None,
            broker: PostgresConfig {
                database_url: "mysql://localhost/db".to_owned(),
                session_database_url: None,
                pgbouncer_transaction_mode: false,
                pool_pre_ping: true,
                worker_pool_pre_ping: false,
                pool_size: 30,
                max_overflow: 30,
                retain_rerun_input_default: false,
                pool_timeout: 30,
                pool_recycle: crate::manifest::PoolRecycle::after_seconds(1800),
                echo: false,
                tcp_keepalives: Default::default(),
                tcp_user_timeout: Default::default(),
            },
            cluster_wide_cap: Some(0),
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            retention: RetentionConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            worker_profiles: std::collections::BTreeMap::new(),
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
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Custom,
            custom_queues: Some(vec![CustomQueueConfig {
                name: "bad".to_owned(),
                priority: 0,
                max_concurrency: Some(5),
            }]),
            broker: valid_broker(),
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            retention: RetentionConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            worker_profiles: std::collections::BTreeMap::new(),
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
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Custom,
            custom_queues: Some(vec![CustomQueueConfig {
                name: "over".to_owned(),
                priority: 101,
                max_concurrency: Some(5),
            }]),
            broker: valid_broker(),
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            retention: RetentionConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            worker_profiles: std::collections::BTreeMap::new(),
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
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: PostgresConfig {
                database_url: "postgresql://user:secret@localhost/db".to_owned(),
                session_database_url: None,
                pgbouncer_transaction_mode: false,
                pool_pre_ping: true,
                worker_pool_pre_ping: false,
                pool_size: 30,
                max_overflow: 30,
                retain_rerun_input_default: false,
                pool_timeout: 30,
                pool_recycle: crate::manifest::PoolRecycle::after_seconds(1800),
                echo: false,
                tcp_keepalives: Default::default(),
                tcp_user_timeout: Default::default(),
            },
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            retention: RetentionConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            worker_profiles: std::collections::BTreeMap::new(),
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
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: valid_broker(),
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: MAX_CLAIM_RENEW_AGE_MS,
            recovery: RecoveryConfig::default(),
            retention: RetentionConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            worker_profiles: std::collections::BTreeMap::new(),
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
                max_concurrency: Some(5),
            },
            CustomQueueConfig {
                name: "q1".to_owned(),
                priority: 2,
                max_concurrency: Some(10),
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
                max_concurrency: Some(5),
            },
            CustomQueueConfig {
                name: "low".to_owned(),
                priority: 50,
                max_concurrency: Some(10),
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
            max_concurrency: Some(5),
        }]);
        let output = config.format_for_logging();
        assert!(output.contains("queue_mode: CUSTOM"));
        assert!(output.contains("high"));
    }

    #[test]
    fn format_for_logging_uncapped_queue_labelled() {
        let mut config = default_config();
        config.queue_mode = QueueMode::Custom;
        config.custom_queues = Some(vec![CustomQueueConfig {
            name: "bulk".to_owned(),
            priority: 1,
            max_concurrency: None,
        }]);
        let output = config.format_for_logging();
        assert!(output.contains("max_concurrency=uncapped"));
        assert!(!output.contains("max_concurrency=None"));
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
                input: serde_json::Value::Null,
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

    // --- Refusals ported from Python test_app_config.py ---

    /// Python `test_app_config.py` (match `max_claim_renew_age_ms must be
    /// positive`): 0 would renew no claim, so it is refused.
    #[test]
    fn max_claim_renew_age_ms_zero_is_refused() {
        let mut config = default_config();
        config.max_claim_renew_age_ms = 0;
        let errors = config.validate();
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(matches!(errors[0], AppConfigError::Prefetch(_)));
        assert!(errors[0]
            .to_string()
            .contains("max_claim_renew_age_ms must be positive"));
        assert_eq!(
            errors[0].error_code(),
            Some(crate::core::error::ErrorCode::ConfigInvalidPrefetch),
        );
    }

    /// Every config refusal carries Python's catalog code.
    #[test]
    fn every_refusal_carries_its_catalog_code() {
        use crate::core::error::ErrorCode;
        let cases = [
            (
                AppConfigError::QueueMode(String::new()),
                Some(ErrorCode::ConfigInvalidQueueMode),
            ),
            (
                AppConfigError::ClusterCap(String::new()),
                Some(ErrorCode::ConfigInvalidClusterCap),
            ),
            (
                AppConfigError::Prefetch(String::new()),
                Some(ErrorCode::ConfigInvalidPrefetch),
            ),
            (
                AppConfigError::Broker(String::new()),
                Some(ErrorCode::BrokerInvalidUrl),
            ),
            (
                AppConfigError::Recovery(String::new()),
                Some(ErrorCode::ConfigInvalidRecovery),
            ),
            (
                AppConfigError::Retention(String::new()),
                Some(ErrorCode::ConfigInvalidRetention),
            ),
            (
                AppConfigError::Resilience(String::new()),
                Some(ErrorCode::ConfigInvalidResilience),
            ),
            (
                AppConfigError::Schedule(String::new()),
                Some(ErrorCode::ConfigInvalidSchedule),
            ),
            (AppConfigError::Payload(String::new()), None),
        ];
        for (error, code) in cases {
            assert_eq!(error.error_code(), code, "{error:?}");
            assert_eq!(error.to_horsies_error().code, code, "{error:?}");
        }

        let mut config = default_config();
        config.cluster_wide_cap = Some(0);
        config.resilience.db_retry_initial_ms = 1;
        let codes: Vec<_> = config
            .validate()
            .iter()
            .map(AppConfigError::error_code)
            .collect();
        assert!(
            codes.contains(&Some(ErrorCode::ConfigInvalidClusterCap)),
            "{codes:?}"
        );
        assert!(
            codes.contains(&Some(ErrorCode::ConfigInvalidResilience)),
            "{codes:?}"
        );
    }

    /// Python validates `ScheduleConfig` when `AppConfig` is built
    /// (`core/models/app.py:60-62`): a bad schedule refuses the app config,
    /// and every schedule error is reported.
    #[test]
    fn schedule_section_is_checked_with_the_app_config() {
        use crate::core::config::schedule::*;
        let schedule = |name: &str| {
            TaskSchedule::new(
                name,
                "my_task",
                SchedulePattern::Interval(IntervalSchedule {
                    seconds: Some(60),
                    ..IntervalSchedule::default()
                }),
            )
        };
        let mut config = default_config();
        config.schedule = Some(ScheduleConfig::new(vec![
            schedule("bad_tz").timezone("Not/A_Timezone"),
            schedule("no_catch_up").max_catch_up_runs(0),
        ]));
        let errors = config.validate();
        assert_eq!(errors.len(), 2, "{errors:?}");
        assert!(errors
            .iter()
            .all(|error| matches!(error, AppConfigError::Schedule(_))));
        assert!(errors[0].to_string().contains("invalid timezone"));
        assert!(errors[1].to_string().contains("max_catch_up_runs"));

        config.schedule = Some(ScheduleConfig::new(vec![schedule("good")]));
        assert!(config.validate().is_empty());
    }

    /// Python `test_config_models_forbid_extra.py`: an unknown key in the app
    /// config or in a nested config is refused, not dropped.
    #[test]
    fn unknown_keys_are_refused_at_every_level() {
        let accepted = r#"{"broker": {"database_url": "postgresql://localhost/db"}}"#;
        let config: AppConfig = serde_json::from_str(accepted).expect("known keys only");
        assert!(config.validate().is_empty());

        let refused = [
            r#"{"broker": {"database_url": "postgresql://localhost/db"}, "prefetch": 1}"#,
            r#"{"broker": {"database_url": "postgresql://localhost/db", "pool": 1}}"#,
            r#"{"broker": {"database_url": "postgresql://localhost/db"},
                "queue_mode": "custom", "custom_queues": [{"name": "a", "prio": 1}]}"#,
            r#"{"broker": {"database_url": "postgresql://localhost/db"},
                "payload": {"reject": 10}}"#,
            r#"{"broker": {"database_url": "postgresql://localhost/db"},
                "resilience": {"db_retry_initial": 500}}"#,
            r#"{"broker": {"database_url": "postgresql://localhost/db"},
                "recovery": {"check_interval": 500}}"#,
            r#"{"broker": {"database_url": "postgresql://localhost/db"},
                "schedule": {"schedules": [], "interval": 1}}"#,
        ];
        for json in refused {
            let error = serde_json::from_str::<AppConfig>(json)
                .expect_err("an unknown key must be refused");
            assert!(
                error.to_string().contains("unknown field"),
                "{json}: {error}"
            );
        }
    }

    /// One TOML document per config level that `AppConfig` reaches, with its
    /// row key. The line `EXTRA` sits in the table of that level; the key
    /// goes there. The names are Python's models; the socket settings and
    /// `pool_recycle` of `broker` are Rust forms of Python's flat
    /// `PostgresConfig` fields, with a row per variant. A new nested config
    /// type needs a row.
    const CONFIG_LEVELS: [(&str, &str); 37] = [
        (
            "extra_app_config",
            "EXTRA
[broker]
database_url = \"postgresql://localhost/db\"
",
        ),
        (
            "extra_postgres_config",
            "[broker]
database_url = \"postgresql://localhost/db\"
EXTRA
",
        ),
        (
            "extra_pool_recycle",
            "[broker]
database_url = \"postgresql://localhost/db\"
[broker.pool_recycle]
kind = \"AFTER_SECONDS\"
seconds = 1800
EXTRA
",
        ),
        (
            "extra_tcp_keepalives",
            "[broker]
database_url = \"postgresql://localhost/db\"
[broker.tcp_keepalives]
kind = \"ON\"
idle_seconds = 30
interval_seconds = 10
probes = 3
EXTRA
",
        ),
        (
            "extra_tcp_user_timeout",
            "[broker]
database_url = \"postgresql://localhost/db\"
[broker.tcp_user_timeout]
kind = \"AFTER\"
milliseconds = 30000
EXTRA
",
        ),
        (
            "extra_pool_recycle_never",
            "[broker]
database_url = \"postgresql://localhost/db\"
[broker.pool_recycle]
kind = \"NEVER\"
EXTRA
",
        ),
        (
            "extra_tcp_keepalives_off",
            "[broker]
database_url = \"postgresql://localhost/db\"
[broker.tcp_keepalives]
kind = \"OFF\"
EXTRA
",
        ),
        (
            "extra_tcp_user_timeout_system_default",
            "[broker]
database_url = \"postgresql://localhost/db\"
[broker.tcp_user_timeout]
kind = \"SYSTEM_DEFAULT\"
EXTRA
",
        ),
        (
            "extra_custom_queue_config",
            "queue_mode = \"custom\"
[broker]
database_url = \"postgresql://localhost/db\"
[[custom_queues]]
name = \"a\"
EXTRA
",
        ),
        (
            "extra_payload_policy",
            "[broker]
database_url = \"postgresql://localhost/db\"
[payload]
EXTRA
",
        ),
        (
            "extra_recovery_config",
            "[broker]
database_url = \"postgresql://localhost/db\"
[recovery]
EXTRA
",
        ),
        (
            "extra_retention_config",
            "[broker]
database_url = \"postgresql://localhost/db\"
[retention]
EXTRA
",
        ),
        (
            "extra_worker_resilience_config",
            "[broker]
database_url = \"postgresql://localhost/db\"
[resilience]
EXTRA
",
        ),
        (
            "extra_worker_profile",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default]
EXTRA
",
        ),
        (
            "extra_isolated_children",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default.isolated]
kind = \"isolated_children\"
count = 1
EXTRA
",
        ),
        (
            "extra_no_isolated_children",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default.isolated]
kind = \"no_isolated_children\"
EXTRA
",
        ),
        (
            "extra_isolated_recycle",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default.isolated]
kind = \"isolated_children\"
count = 1
[worker_profiles.default.isolated.recycle]
EXTRA
",
        ),
        (
            "extra_task_limit",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default.isolated]
kind = \"isolated_children\"
count = 1
[worker_profiles.default.isolated.recycle.after]
kind = \"task_limit\"
tasks = 10
EXTRA
",
        ),
        (
            "extra_no_task_limit",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default.isolated]
kind = \"isolated_children\"
count = 1
[worker_profiles.default.isolated.recycle.after]
kind = \"no_task_limit\"
EXTRA
",
        ),
        (
            "extra_memory_limit",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default.isolated]
kind = \"isolated_children\"
count = 1
[worker_profiles.default.isolated.recycle.above]
kind = \"memory_limit\"
mb = 512
EXTRA
",
        ),
        (
            "extra_no_memory_limit",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default.isolated]
kind = \"isolated_children\"
count = 1
[worker_profiles.default.isolated.recycle.above]
kind = \"no_memory_limit\"
EXTRA
",
        ),
        (
            "extra_isolated_pool",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default.isolated]
kind = \"isolated_children\"
count = 1
[worker_profiles.default.isolated.pool]
EXTRA
",
        ),
        (
            "extra_pool_size",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default.isolated]
kind = \"isolated_children\"
count = 1
[worker_profiles.default.isolated.task_send_pool]
kind = \"pool_size\"
connections = 2
EXTRA
",
        ),
        (
            "extra_shared_async_children",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default.shared_async]
kind = \"shared_async_children\"
count = 1
seats = 2
EXTRA
",
        ),
        (
            "extra_no_shared_async_children",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default.shared_async]
kind = \"no_shared_async_children\"
EXTRA
",
        ),
        (
            "extra_shared_recycle",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default.shared_async]
kind = \"shared_async_children\"
count = 1
seats = 2
[worker_profiles.default.shared_async.recycle]
EXTRA
",
        ),
        (
            "extra_shared_pool",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default.shared_async]
kind = \"shared_async_children\"
count = 1
seats = 2
[worker_profiles.default.shared_async.pool]
EXTRA
",
        ),
        (
            "extra_pool_size_setting",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default.shared_async]
kind = \"shared_async_children\"
count = 1
seats = 2
[worker_profiles.default.shared_async.pool.max_size]
kind = \"pool_size\"
connections = 2
EXTRA
",
        ),
        (
            "extra_pool_size_from_seats",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default.shared_async]
kind = \"shared_async_children\"
count = 1
seats = 2
[worker_profiles.default.shared_async.pool.max_size]
kind = \"pool_size_from_seats\"
EXTRA
",
        ),
        (
            "extra_executor_threads",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default.shared_async]
kind = \"shared_async_children\"
count = 1
seats = 2
[worker_profiles.default.shared_async.executor]
kind = \"executor_threads\"
threads = 2
EXTRA
",
        ),
        (
            "extra_executor_threads_from_seats",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default.shared_async]
kind = \"shared_async_children\"
count = 1
seats = 2
[worker_profiles.default.shared_async.executor]
kind = \"executor_threads_from_seats\"
EXTRA
",
        ),
        (
            "extra_shared_thread_children",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default.shared_thread]
kind = \"shared_thread_children\"
count = 1
seats = 2
EXTRA
",
        ),
        (
            "extra_no_shared_thread_children",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default.shared_thread]
kind = \"no_shared_thread_children\"
EXTRA
",
        ),
        (
            "extra_parent_pool",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default.pool]
EXTRA
",
        ),
        (
            "extra_claim_limits",
            "[broker]
database_url = \"postgresql://localhost/db\"
[worker_profiles.default.claim]
EXTRA
",
        ),
        (
            "extra_schedule_config",
            "[broker]
database_url = \"postgresql://localhost/db\"
[schedule]
EXTRA
",
        ),
        (
            "extra_task_schedule",
            "[broker]
database_url = \"postgresql://localhost/db\"
[[schedule.schedules]]
name = \"a\"
task_name = \"t\"
pattern = { type = \"interval\", seconds = 5 }
EXTRA
",
        ),
    ];

    /// Python `test_config_models_forbid_extra.py::test_every_reachable_config_model_forbids_extra`,
    /// read from TOML as `horsies web <CONFIG>` reads the config: each config
    /// level that `AppConfig` reaches ([`CONFIG_LEVELS`]) refuses one unknown
    /// key, and the error names the key. The document without the key loads.
    /// The variants of the tagged schedule enums are in
    /// `schedule::tests::every_variant_of_the_tagged_schedule_enums_refuses_an_unknown_member`.
    #[test]
    fn an_unknown_toml_key_is_refused_by_name_at_every_config_level() {
        for (key, document) in CONFIG_LEVELS {
            let accepted = document.replace("EXTRA\n", "");
            let _config: AppConfig = toml::from_str(&accepted).unwrap_or_else(|error| {
                panic!("{key}: the document without the key must load: {error}")
            });
            let refused = document.replace("EXTRA\n", &format!("{key} = 1\n"));
            let error =
                toml::from_str::<AppConfig>(&refused).expect_err("an unknown key must be refused");
            let unknown_field = format!("unknown field `{key}`");
            assert!(error.to_string().contains(&unknown_field), "{key}: {error}");
        }
    }

    fn with_profiles(document: &str) -> AppConfig {
        serde_json::from_str(&format!(
            r#"{{"broker": {{"database_url": "postgresql://localhost/db"}},
                "worker_profiles": {document}}}"#
        ))
        .expect("the worker profiles parse")
    }

    /// Python `test_worker_profile.py::TestAppConfigProfileChecks::test_no_profiles_is_valid`
    /// and `::test_valid_profiles_are_kept`: an app that runs no worker
    /// defines none, and valid profiles are kept by name.
    #[test]
    fn worker_profiles_default_to_none_and_are_kept_by_name() {
        let config: AppConfig =
            serde_json::from_str(r#"{"broker": {"database_url": "postgresql://localhost/db"}}"#)
                .expect("known keys only");
        assert!(config.worker_profiles.is_empty());
        assert!(AppConfig::for_database_url("postgresql://localhost/db")
            .worker_profiles
            .is_empty());

        let config = with_profiles(
            r#"{"default": {"isolated": {"kind": "isolated_children", "count": 4}},
                "io": {"shared_async": {"kind": "shared_async_children", "count": 1, "seats": 4}}}"#,
        );
        assert!(config.validate().is_empty());
        let names: Vec<&str> = config.worker_profiles.keys().map(String::as_str).collect();
        assert_eq!(names, ["default", "io"]);
    }

    /// Python `AppConfig.validate_queue_configuration`: every worker profile
    /// is checked with the app config, and a refusal keeps
    /// `CONFIG_INVALID_WORKER_PROFILE`, its notes and its help.
    #[test]
    fn worker_profiles_are_checked_with_the_app_config() {
        let config = with_profiles(r#"{"empty": {}}"#);

        let errors = config.validate();

        assert_eq!(errors.len(), 1);
        assert_eq!(
            errors[0].to_string(),
            "worker_profiles['empty']: the profile starts no children",
        );
        assert_eq!(
            errors[0].error_code(),
            Some(crate::core::error::ErrorCode::ConfigInvalidWorkerProfile),
        );
        let refusal = errors[0].to_horsies_error();
        assert_eq!(
            refusal.code,
            Some(crate::core::error::ErrorCode::ConfigInvalidWorkerProfile),
        );
        assert_eq!(
            refusal.notes,
            ["a worker needs at least one child to run a task"],
        );
        assert!(refusal
            .help_text
            .as_deref()
            .is_some_and(|help| help.contains("isolated=IsolatedChildren(count=4)")));
        assert_eq!(
            crate::core::error::ErrorCode::ConfigInvalidWorkerProfile.to_string(),
            "HRS-218"
        );
    }

    /// Python `test_worker_profile.py::TestAppConfigProfileChecks::test_capacity_includes_the_prefetch_buffer`
    /// and `::test_capacity_with_prefetch_buffer_is_still_a_bound`: the claim
    /// check uses the app's `prefetch_buffer`.
    #[test]
    fn the_claim_check_of_a_profile_counts_the_prefetch_buffer() {
        let soft = |tasks: u32| {
            let mut config = with_profiles(&format!(
                r#"{{"soft": {{"isolated": {{"kind": "isolated_children", "count": 2}},
                    "claim": {{"claimed_per_worker": {{"kind": "task_limit", "tasks": {tasks}}}}}}}}}"#
            ));
            config.prefetch_buffer = 3;
            config.claim_lease_ms = Some(60_000);
            config
        };

        assert!(soft(5).validate().is_empty());
        let errors = soft(6).validate();
        assert_eq!(errors.len(), 1);
        assert!(
            errors[0].to_string().contains("capacity of the worker (5)"),
            "{}",
            errors[0],
        );
    }

    /// Python `test_worker_profile.py::TestAppConfigProfileChecks::test_log_config_lists_each_profile`.
    #[test]
    fn format_for_logging_lists_each_profile() {
        let config = with_profiles(
            r#"{"default": {"isolated": {"kind": "isolated_children", "count": 2}}}"#,
        );
        let profile = config.worker_profiles["default"];

        let formatted = config.format_for_logging();

        assert!(formatted.contains("  worker_profiles:"), "{formatted}");
        assert!(
            formatted.contains(&format!(
                "    default: {}",
                crate::core::config::worker_profile::describe_worker_profile(&profile)
            )),
            "{formatted}",
        );
        let without = AppConfig::for_database_url("postgresql://localhost/db").format_for_logging();
        assert!(!without.contains("worker_profiles"), "{without}");
    }
}
