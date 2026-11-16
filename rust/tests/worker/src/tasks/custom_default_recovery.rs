//! Task registration for `QueueMode::Custom` with queues
//! `[default, recovery]`.
//!
//! Used by L6 workflow-recovery tests (see `test_workflow_recovers_after_worker_crash`).
//! Workflow specs set per-node queues via `.queue("recovery")`; registration
//! only needs `e2e_wf_slow_step` available on a valid queue.

use horsies::{async_task_fn, Horsies};

use super::defs::*;

pub fn register(app: &mut Horsies) -> Result<(), Box<dyn std::error::Error>> {
    // Worker-harness readiness check (see `start_worker`).
    app.register_with_queue(
        "e2e_healthcheck",
        async_task_fn!(healthcheck, ()),
        "default",
    )?;

    app.register_with_queue(
        "e2e_wf_slow_step",
        async_task_fn!(wf_slow_step, SlowStepInput),
        "default",
    )?;
    Ok(())
}
