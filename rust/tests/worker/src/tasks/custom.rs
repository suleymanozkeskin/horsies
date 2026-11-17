//! Tasks of the custom queue instance (Python `tests/e2e/tasks/queues_custom.py`).
//! In `Custom` queue mode every task names one of the configured queues.

use std::convert::Infallible;
use std::time::Duration;

use horsies::{blocking_task, task, TaskError};

/// The readiness task of a worker of the custom queue instance.
#[task("e2e_custom_healthcheck", queue = "high")]
pub async fn custom_healthcheck() -> Result<String, TaskError<Infallible>> {
    Ok("ready".to_owned())
}

/// Python `e2e_high`.
#[task("e2e_high", queue = "high")]
pub async fn high() -> Result<String, TaskError<Infallible>> {
    Ok("high".to_owned())
}

/// Python `e2e_normal`.
#[task("e2e_normal", queue = "normal")]
pub async fn normal() -> Result<String, TaskError<Infallible>> {
    Ok("normal".to_owned())
}

/// Python `e2e_low`.
#[task("e2e_low", queue = "low")]
pub async fn low() -> Result<String, TaskError<Infallible>> {
    Ok("low".to_owned())
}

/// A body that sleeps `duration_ms` on the `high` queue (cap 5).
#[blocking_task("e2e_slow_high", queue = "high")]
pub fn slow_high(duration_ms: u64) -> Result<String, TaskError<Infallible>> {
    std::thread::sleep(Duration::from_millis(duration_ms));
    Ok(format!("slept_{duration_ms}"))
}

/// A body that sleeps `duration_ms` on the `normal` queue (cap 10).
#[blocking_task("e2e_slow_normal", queue = "normal")]
pub fn slow_normal(duration_ms: u64) -> Result<String, TaskError<Infallible>> {
    std::thread::sleep(Duration::from_millis(duration_ms));
    Ok(format!("slept_{duration_ms}"))
}

/// A body that sleeps `duration_ms` on the `low` queue (cap 20).
#[blocking_task("e2e_slow_low", queue = "low")]
pub fn slow_low(duration_ms: u64) -> Result<String, TaskError<Infallible>> {
    std::thread::sleep(Duration::from_millis(duration_ms));
    Ok(format!("slept_{duration_ms}"))
}
