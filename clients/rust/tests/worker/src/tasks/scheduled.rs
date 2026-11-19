//! Tasks of the scheduler instance (Python `tests/e2e/tasks/scheduler.py`).

use std::convert::Infallible;

use horsies::{task, TaskError};

/// Python `e2e_scheduled_simple`.
#[task("e2e_scheduled_simple")]
pub async fn scheduled_simple() -> Result<String, TaskError<Infallible>> {
    Ok("scheduled".to_owned())
}

/// Python `e2e_scheduled_with_args`: twice its scheduled input.
#[task("e2e_scheduled_with_args")]
pub async fn scheduled_with_args(value: i64) -> Result<i64, TaskError<Infallible>> {
    Ok(value * 2)
}

/// Python `e2e_catch_up_task`.
#[task("e2e_catch_up_task")]
pub async fn catch_up_task() -> Result<String, TaskError<Infallible>> {
    Ok("caught_up".to_owned())
}

/// Python `e2e_cron_task`.
#[task("e2e_cron_task")]
pub async fn cron_task() -> Result<String, TaskError<Infallible>> {
    Ok("cron".to_owned())
}
