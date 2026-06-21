use horsies::{AppConfig, CustomQueueConfig, QueueMode, RecoveryConfig};

/// Queue name constants.
pub const HIGH: &str = "high";
pub const NORMAL: &str = "normal";
pub const LOW: &str = "low";

/// Create an AppConfig with QueueMode::Custom and three priority queues.
///
/// This is the equivalent of the Python `instance_custom.py`.
///
/// Queues:
///   - `high`   — priority 1,  max_concurrency 32
///   - `normal` — priority 50, max_concurrency 40
///   - `low`    — priority 90, max_concurrency 48
///
/// Cluster-wide concurrency cap: 50
pub fn app_config(db_url: &str) -> AppConfig {
    AppConfig {
        queue_mode: QueueMode::Custom,
        custom_queues: Some(vec![
            CustomQueueConfig {
                name: HIGH.to_string(),
                priority: 1,
                max_concurrency: Some(32),
            },
            CustomQueueConfig {
                name: NORMAL.to_string(),
                priority: 50,
                max_concurrency: Some(40),
            },
            CustomQueueConfig {
                name: LOW.to_string(),
                priority: 90,
                max_concurrency: Some(48),
            },
        ]),
        cluster_wide_cap: Some(50),
        recovery: RecoveryConfig {
            auto_requeue_stale_claimed: true,
            claimed_stale_threshold_ms: 4500,
            auto_fail_stale_running: true,
            running_stale_threshold_ms: 3000,
            check_interval_ms: 1500,
            runner_heartbeat_interval_ms: 1500,
            claimer_heartbeat_interval_ms: 1500,
            ..RecoveryConfig::default()
        },
        ..super::app_config(db_url)
    }
}
