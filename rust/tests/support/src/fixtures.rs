/// Pre-built configurations for common test scenarios.
///
/// Mirrors Python's `conftest.py` fixtures (app_config, broker, app).
use horsies::{
    AppConfig, PostgresConfig, QueueMode, RecoveryConfig, RetentionConfig, WorkerResilienceConfig,
};

use crate::db;

/// Build a default `AppConfig` for tests (DEFAULT queue mode, local DB).
pub fn default_app_config() -> AppConfig {
    AppConfig {
        payload: horsies::PayloadPolicy::default(),
        queue_mode: QueueMode::Default,
        custom_queues: None,
        broker: PostgresConfig {
            database_url: db::db_url(),
            session_database_url: None,
            pgbouncer_transaction_mode: false,
            pool_pre_ping: true,
            pool_size: 5,
            max_overflow: 5,
            retain_rerun_input_default: false,
            pool_timeout: 10,
            pool_recycle: 600,
            echo: false,
        },
        cluster_wide_cap: None,
        prefetch_buffer: 0,
        claim_lease_ms: None,
        max_claim_renew_age_ms: 180_000,
        recovery: RecoveryConfig::default(),
        retention: RetentionConfig::default(),
        resilience: WorkerResilienceConfig::default(),
        schedule: None,
        resend_on_transient_err: false,
    }
}

/// Build an `AppConfig` with fast recovery thresholds for crash-detection tests.
pub fn fast_recovery_app_config() -> AppConfig {
    let mut config = default_app_config();
    config.recovery = RecoveryConfig {
        auto_requeue_stale_claimed: true,
        claimed_stale_threshold_ms: 2_000,
        auto_fail_stale_running: true,
        auto_terminate_orphaned_workflow_tasks: true,
        orphan_task_audit_interval_ms: 60_000,
        running_stale_threshold_ms: 2_000,
        finalizing_stale_threshold_ms: 2_000,
        crashed_worker_recovery_grace_ms: 2_000,
        phase2_quarantine_after_attempts: 25,
        check_interval_ms: 1_000,
        runner_heartbeat_interval_ms: 1_000,
        claimer_heartbeat_interval_ms: 1_000,
        worker_state_snapshot_interval_ms: 1_000,
    };
    config.retention.worker_state_retention_hours = Some(1);
    config.retention.terminal_record_retention_hours = Some(1);
    config.retention.retention_sweep_interval_s = 30;
    config
}

/// Build a `PostgresConfig` pointing at the test database.
pub fn test_postgres_config() -> PostgresConfig {
    PostgresConfig {
        database_url: db::db_url(),
        session_database_url: None,
        pgbouncer_transaction_mode: false,
        pool_pre_ping: true,
        pool_size: 5,
        max_overflow: 5,
        retain_rerun_input_default: false,
        pool_timeout: 10,
        pool_recycle: 600,
        echo: false,
    }
}
