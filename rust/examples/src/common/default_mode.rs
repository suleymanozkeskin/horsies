use horsies::{AppConfig, PostgresConfig, QueueMode, RecoveryConfig, WorkerResilienceConfig};

/// Create an AppConfig with QueueMode::Default (single "default" queue).
///
/// This is the equivalent of the Python `instance.py`.
pub fn app_config(db_url: &str) -> AppConfig {
    AppConfig {
        queue_mode: QueueMode::Default,
        custom_queues: None,
        broker: PostgresConfig::from_url(db_url),
        cluster_wide_cap: None,
        prefetch_buffer: 0,
        claim_lease_ms: None,
        max_claim_renew_age_ms: 180_000,
        recovery: RecoveryConfig::default(),
        resilience: WorkerResilienceConfig::default(),
        schedule: None,
        resend_on_transient_err: false,
    }
}
