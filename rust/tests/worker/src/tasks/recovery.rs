//! The tasks of the recovery queue instance (Python
//! `tests/e2e/tasks/instance_recovery.py`). In `Custom` queue mode every
//! task names one of the configured queues.

use std::convert::Infallible;
use std::time::Duration;

use horsies::{blocking_task, task, TaskError};

/// Python `e2e_recovery_healthcheck`: the readiness task of a worker of the
/// recovery queue instance.
#[task("e2e_recovery_healthcheck", queue = "default")]
pub async fn recovery_healthcheck() -> Result<String, TaskError<Infallible>> {
    Ok("ready".to_owned())
}

/// Python `e2e_recovery_wf_step`: a workflow step that runs `delay_ms`.
#[blocking_task("e2e_recovery_wf_step", queue = "default")]
pub fn recovery_wf_step(step: String, delay_ms: u64) -> Result<String, TaskError<Infallible>> {
    std::thread::sleep(Duration::from_millis(delay_ms));
    Ok(format!("step_{step}_done"))
}
