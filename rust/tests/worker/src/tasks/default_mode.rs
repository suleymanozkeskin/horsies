//! Task registration for `QueueMode::Default`.
//!
//! All tasks are registered to the single `"default"` queue. The `#[task]`
//! macro-generated `::register()` is used for tasks that need the global
//! handle (so `::node()`, `::send()`, `::handle()` work from test code).
//!
//! Mirrors Python's `tests/e2e/tasks/instance.py`.

use horsies::{async_task_fn, Horsies};

use super::defs::*;

pub fn register(app: &mut Horsies) -> Result<(), Box<dyn std::error::Error>> {
    // Basic tasks (no macro, no TaskRuntime).
    app.register_with_queue(
        "e2e_healthcheck",
        async_task_fn!(healthcheck, ()),
        "default",
    )?;
    app.register_with_queue(
        "e2e_simple",
        async_task_fn!(simple_task, SimpleInput),
        "default",
    )?;
    app.register_with_queue(
        "e2e_kwargs",
        async_task_fn!(kwargs_task, KwargsInput),
        "default",
    )?;
    app.register_with_queue("e2e_error", async_task_fn!(error_task, ()), "default")?;
    app.register_with_queue("e2e_slow", async_task_fn!(slow_task, SlowInput), "default")?;
    app.register_with_queue("e2e_no_retry", async_task_fn!(no_retry_task, ()), "default")?;
    app.register_with_queue(
        "e2e_idempotent",
        async_task_fn!(idempotent_task, IdempotentInput),
        "default",
    )?;

    // #[task]-macro tasks (populate global handle for ::node()/::send()).
    wf_step::register(app)?;
    wf_slow_step::register(app)?;
    wf_final_result::register(app)?;
    wf_fail::register(app)?;
    wf_fail_int::register(app)?;
    rt_ping::register(app)?;
    dynamic_rt_start::register(app)?;
    dynamic_rt_start_no_args::register(app)?;
    runtime_helper_dispatch::register(app)?;
    runtime_helper_schedule::register(app)?;
    runtime_helper_handle::register(app)?;
    wf_ctx_reader::register(app)?;
    wf_ctx_sum::register(app)?;
    wf_mixed::register(app)?;
    wf_produce_int::register(app)?;
    wf_double::register(app)?;
    wf_sum_two::register(app)?;
    wf_produce_dict::register(app)?;
    wf_read_dict::register(app)?;
    wf_retry_then_ok::register(app)?;
    wf_retry_via_registration::register(app)?;

    // Child-label task + workflow (typed sub-workflow tests).
    let wf_child_label_task = app
        .task::<ChildLabelInput, String>(
            "e2e_wf_child_label",
            async_task_fn!(wf_child_label, ChildLabelInput),
        )?
        .register()?;
    register_child_label_workflow(app, &wf_child_label_task)?;

    // Sub-workflow context reader + nested pipelines (layer-7 e2e matrix).
    wf_subwf_ctx_reader::register(app)?;
    register_nested_workflows(app)?;

    // Complex result + builtin error code.
    app.register_with_queue(
        "e2e_complex_result",
        async_task_fn!(complex_result_task, ()),
        "default",
    )?;
    app.register_with_queue(
        "e2e_error_code",
        async_task_fn!(error_code_task, ()),
        "default",
    )?;

    // Retry tasks.
    app.register_with_queue(
        "e2e_retry_exhausted",
        async_task_fn!(retry_exhausted, ()),
        "default",
    )?;
    app.register_with_queue(
        "e2e_retry_success",
        async_task_fn!(retry_success, ()),
        "default",
    )?;

    // Softcap-specific (same fns, different names for instance parity with Python).
    app.register_with_queue(
        "e2e_softcap_blocker",
        async_task_fn!(slow_task, SlowInput),
        "default",
    )?;
    app.register_with_queue(
        "e2e_softcap_slow_idempotent",
        async_task_fn!(slow_idempotent_task, SlowIdempotentInput),
        "default",
    )?;

    // Custom queue task placeholders (Default mode also registers them so the
    // Default-config test_worker_resolution_error, etc., still finds them).
    app.register_with_queue(
        "e2e_custom_slow",
        async_task_fn!(slow_task, SlowInput),
        "default",
    )?;
    app.register_with_queue("e2e_high", async_task_fn!(healthcheck, ()), "default")?;
    app.register_with_queue("e2e_normal", async_task_fn!(healthcheck, ()), "default")?;
    app.register_with_queue("e2e_low", async_task_fn!(healthcheck, ()), "default")?;

    // Cluster cap tasks.
    app.register_with_queue(
        "e2e_cluster_cap_slow",
        async_task_fn!(slow_task, SlowInput),
        "default",
    )?;
    app.register_with_queue(
        "e2e_cluster_cap_fail",
        async_task_fn!(error_task, ()),
        "default",
    )?;

    // Recovery tasks.
    app.register_with_queue(
        "e2e_recovery_healthcheck",
        async_task_fn!(healthcheck, ()),
        "default",
    )?;
    app.register_with_queue(
        "e2e_recovery_slow",
        async_task_fn!(slow_task, SlowInput),
        "default",
    )?;

    // DB ledger task.
    app.register_with_queue(
        "e2e_softcap_db_ledger",
        async_task_fn!(db_ledger_task, LedgerInput),
        "default",
    )?;

    // Requeue guard task.
    app.register_with_queue(
        "e2e_requeue_guard",
        async_task_fn!(requeue_guard_task, RequeueGuardInput),
        "default",
    )?;

    // Scheduler tasks.
    app.register_with_queue(
        "e2e_scheduled_simple",
        async_task_fn!(healthcheck, ()),
        "default",
    )?;
    app.register_with_queue(
        "e2e_scheduled_with_args",
        async_task_fn!(simple_task, SimpleInput),
        "default",
    )?;
    app.register_with_queue(
        "e2e_catch_up_task",
        async_task_fn!(healthcheck, ()),
        "default",
    )?;

    Ok(())
}
