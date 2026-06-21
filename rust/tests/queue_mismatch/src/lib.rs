//! Shared task definitions and config builders for queue mismatch guard tests.
//!
//! Each scenario test in `tests/` imports from here to build a real `Horsies`
//! app with real task functions, real `AppConfig`, and real `WorkerConfig`.

use horsies::{
    AppConfig, CustomQueueConfig, PostgresConfig, QueueMode, RecoveryConfig, WorkerResilienceConfig,
};

// ---------------------------------------------------------------------------
// Config builders
// ---------------------------------------------------------------------------

/// Fake DB URL — we never actually connect; queue validation fires first.
const FAKE_DB_URL: &str = "postgresql://guard_test:guard_test@localhost:5432/guard_test";

fn broker_config() -> PostgresConfig {
    PostgresConfig {
        database_url: FAKE_DB_URL.to_owned(),
        session_database_url: None,
        pgbouncer_transaction_mode: false,
        pool_pre_ping: true,
        pool_size: 5,
        max_overflow: 5,
        pool_timeout: 10,
        pool_recycle: 600,
        echo: false,
    }
}

/// DEFAULT mode — single implicit "default" queue.
pub fn default_mode_config() -> AppConfig {
    AppConfig {
        queue_mode: QueueMode::Default,
        custom_queues: None,
        broker: broker_config(),
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

/// CUSTOM mode with two queues: "fast" (priority 1) and "slow" (priority 50).
pub fn custom_mode_config() -> AppConfig {
    AppConfig {
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
        broker: broker_config(),
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

// ---------------------------------------------------------------------------
// Real task functions
// ---------------------------------------------------------------------------

pub mod tasks {
    use horsies::TaskError;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ComputeInput {
        pub a: f64,
        pub b: f64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ComputeResult {
        pub value: f64,
    }

    pub async fn add(input: ComputeInput) -> Result<ComputeResult, TaskError> {
        Ok(ComputeResult {
            value: input.a + input.b,
        })
    }

    pub async fn multiply(input: ComputeInput) -> Result<ComputeResult, TaskError> {
        Ok(ComputeResult {
            value: input.a * input.b,
        })
    }

    pub async fn ping(_: ()) -> Result<String, TaskError> {
        Ok("pong".to_owned())
    }
}
