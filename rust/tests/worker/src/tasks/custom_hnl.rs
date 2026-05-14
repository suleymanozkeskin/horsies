//! Task registration for `QueueMode::Custom` with queues
//! `[high, normal, low]`.
//!
//! Mirrors Python's `tests/e2e/tasks/queues_custom.py` + `instance_custom.py`.
//! Only registers the tasks that L1/L2 custom-queue tests actually invoke;
//! `#[task]`-macro `::register()` is bypassed because in Custom mode it
//! requires `.queue()` and we want explicit per-task routing.

use horsies::{async_task_fn, Horsies};

use super::defs::*;

pub fn register(app: &mut Horsies) -> Result<(), Box<dyn std::error::Error>> {
    // `start_worker` does a functional readiness check by enqueueing
    // `e2e_healthcheck` to the first queue passed via `--queues`, so this
    // task must be registered before the harness will consider the worker
    // ready.
    app.register_with_queue(
        "e2e_healthcheck",
        async_task_fn!(healthcheck, ()),
        "high",
    )?;

    app.register_with_queue("e2e_high", async_task_fn!(healthcheck, ()), "high")?;
    app.register_with_queue("e2e_normal", async_task_fn!(healthcheck, ()), "normal")?;
    app.register_with_queue("e2e_low", async_task_fn!(healthcheck, ()), "low")?;

    // `e2e_custom_slow` mirrors Python's queue_name='high' default. Tests
    // override at enqueue time, so any configured queue is acceptable here.
    app.register_with_queue(
        "e2e_custom_slow",
        async_task_fn!(slow_task, SlowInput),
        "high",
    )?;

    Ok(())
}
