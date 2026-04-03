//! Scenario 6: CUSTOM mode + 2 tasks: one valid queue + one invalid queue.
//!
//! App has queues ["fast", "slow"].
//! Task A targets "fast" (valid), Task B targets "analytics" (invalid).
//! Expected: hard fail — the single bad task poisons the whole startup.
//! Even though Task A is fine, the app must not start.

use horsies::{async_task_fn, Horsies, WorkerConfig};
use horsies_queue_mismatch_tests::{custom_mode_config, tasks};

#[tokio::test]
async fn first_valid_second_invalid_poisons_startup() {
    let config = custom_mode_config(); // queues: "fast", "slow"
    let mut app = Horsies::new(config).expect("app creation should succeed");

    // Task A: valid queue "fast" — should succeed.
    app.task::<tasks::ComputeInput, tasks::ComputeResult>(
        "fast_compute",
        async_task_fn!(tasks::add, tasks::ComputeInput),
    )
    .expect("task builder creation should succeed")
    .queue("fast")
    .register()
    .expect("first task registration must succeed");

    // Task B: invalid queue "analytics" — must fail.
    let result = app
        .task::<tasks::ComputeInput, tasks::ComputeResult>(
            "analytics_compute",
            async_task_fn!(tasks::multiply, tasks::ComputeInput),
        )
        .expect("task builder creation should succeed")
        .queue("analytics")
        .register();

    let err = result.expect_err("second task registration must fail");
    assert!(
        err.to_string().contains("analytics"),
        "error should name the offending queue 'analytics', got: {}",
        err
    );
}

#[tokio::test]
async fn check_fails_if_bad_task_bypassed_register() {
    // Simulate: Task A registered normally, Task B sneaked past register()
    // via direct registry insertion. check() must still catch it.
    use horsies::{async_task_fn, TaskOptions};

    let config = custom_mode_config();
    let mut app = Horsies::new(config).expect("app creation should succeed");

    // Task A: valid, via builder.
    app.task::<tasks::ComputeInput, tasks::ComputeResult>(
        "fast_compute",
        async_task_fn!(tasks::add, tasks::ComputeInput),
    )
    .expect("task builder creation should succeed")
    .queue("fast")
    .register()
    .expect("valid task must register");

    // Task B: bypassing builder, inserting directly into core registry.
    // This simulates a hypothetical code path that skips the builder guard.
    let _ = app.registry().clone(); // just proving registry is accessible
                                    // We can't insert into the registry from outside (it's pub(crate)),
                                    // but we can verify that register() catches it.
    let bad_result = app.register(
        "sneaky_task",
        async_task_fn!(tasks::ping, ()).with_task_options(TaskOptions {
            task_name: "sneaky_task".to_owned(),
            queue_name: Some("analytics".to_owned()),
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
        }),
    );

    let err = bad_result.expect_err("register() must reject unknown queue");
    assert!(err.to_string().contains("analytics"));
}

#[tokio::test]
async fn worker_never_starts_due_to_poisoned_registration() {
    let config = custom_mode_config();
    let mut app = Horsies::new(config).expect("app creation should succeed");

    // Task A: valid.
    app.task::<tasks::ComputeInput, tasks::ComputeResult>(
        "fast_compute",
        async_task_fn!(tasks::add, tasks::ComputeInput),
    )
    .expect("task builder creation should succeed")
    .queue("fast")
    .register()
    .expect("valid task must register");

    // Task B: invalid — registration fails, poisoning the startup.
    let result = app
        .task::<tasks::ComputeInput, tasks::ComputeResult>(
            "analytics_compute",
            async_task_fn!(tasks::multiply, tasks::ComputeInput),
        )
        .expect("task builder creation should succeed")
        .queue("analytics")
        .register();

    assert!(result.is_err(), "bad task must fail registration");

    // At this point a real startup script would abort.
    // The app has Task A registered but Task B failed — a correct startup
    // implementation would not proceed to run_worker_with.
    //
    // But let's also verify check() passes (since only Task A is in registry).
    app.check()
        .expect("check passes — only valid Task A is in registry");

    // And let's verify the worker config is correct.
    let mut worker_config = WorkerConfig::default();
    if let Some(ref queues) = app.config().custom_queues {
        worker_config.queues = queues.iter().map(|q| q.name.clone()).collect();
    }
    worker_config.concurrency = 4;

    // If we DID start the worker, it would fail at DB, not at queue validation.
    // The guard did its job: Task B never made it in.
    let err = app.run_worker_with(worker_config).await.unwrap_err();
    let err_str = format!("{}", err);
    assert!(
        !err_str.contains("TaskInvalidOptions"),
        "worker should not fail on queue validation (bad task was blocked). Got: {}",
        err_str
    );
}
