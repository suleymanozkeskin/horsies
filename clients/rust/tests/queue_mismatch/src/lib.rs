//! The configurations and tasks of the queue guard scenarios (Python
//! `clients/python/tests/unit/test_queue_mismatch_guard.py`).
//!
//! An app is built with no I/O, so the scenarios that only register and
//! check use a URL of a server that does not exist. Scenario 5 connects to
//! the test server.

use std::convert::Infallible;

use horsies::{task, AppConfig, CustomQueueConfig, QueueMode, TaskError};
use serde::{Deserialize, Serialize};

/// A URL that no scenario connects to: the registration and the check run
/// before any connect.
pub const UNUSED_DATABASE_URL: &str = "postgresql://guard_test:guard_test@localhost:1/guard_test";

/// `Default` queue mode: the one implicit queue `default`.
pub fn default_mode_config(database_url: &str) -> AppConfig {
    AppConfig::from_database_url(database_url)
}

/// `Custom` queue mode with `fast` (priority 1, cap 10) and `slow`
/// (priority 50, cap 5).
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
        ..AppConfig::from_database_url(UNUSED_DATABASE_URL)
    }
}

/// A task with no queue: right in `Default` mode, refused in `Custom`.
#[task("ping")]
pub async fn ping() -> Result<String, TaskError<Infallible>> {
    Ok("pong".to_owned())
}

/// A task on `fast`: refused in `Default` mode, right in `Custom`.
#[task("fast_compute", queue = "fast")]
pub async fn fast_compute(a: i64, b: i64) -> Result<i64, TaskError<Infallible>> {
    Ok(a + b)
}

/// A task on `slow`.
#[task("slow_compute", queue = "slow")]
pub async fn slow_compute(a: i64, b: i64) -> Result<i64, TaskError<Infallible>> {
    Ok(a * b)
}

/// A task on `analytics`, which no scenario configures.
#[task("analytics_compute", queue = "analytics")]
pub async fn analytics_compute(a: i64, b: i64) -> Result<i64, TaskError<Infallible>> {
    Ok(a - b)
}

/// The input of the hand-registered task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputeInput {
    pub a: i64,
    pub b: i64,
}

/// The body of a task registered by hand with `Horsies::register_with_queue`.
pub async fn compute_body(input: ComputeInput) -> Result<i64, TaskError<Infallible>> {
    Ok(input.a + input.b)
}
