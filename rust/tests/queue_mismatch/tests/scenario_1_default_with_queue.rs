//! Scenario 1: DEFAULT mode + task declares a queue ("fast").
//!
//! Expected: hard fail at registration — cannot specify queue in DEFAULT mode.
//! The app never reaches check(), the worker never starts.

use horsies::{async_task_fn, ErrorCode, Horsies, WorkerConfig};
use horsies_queue_mismatch_tests::{default_mode_config, tasks};

#[tokio::test]
async fn registration_fails_with_queue_in_default_mode() {
    let config = default_mode_config();
    let mut app = Horsies::new(config).expect("app creation should succeed");

    // Register a task targeting queue "fast" in DEFAULT mode.
    let result = app.task::<tasks::ComputeInput, tasks::ComputeResult>(
        "fast_compute",
        async_task_fn!(tasks::add, tasks::ComputeInput),
    )
    .expect("task builder creation should succeed")
    .queue("fast")
    .register();

    // Must fail: DEFAULT mode does not allow queue specification.
    let err = result.expect_err("registration must fail for queue in DEFAULT mode");
    assert_eq!(
        err.code,
        Some(ErrorCode::TaskInvalidOptions),
        "error code should be TaskInvalidOptions, got: {}",
        err
    );
    assert!(
        err.to_string().contains("Default queue mode"),
        "error should mention Default queue mode, got: {}",
        err
    );
}

#[tokio::test]
async fn check_never_reached() {
    let config = default_mode_config();
    let mut app = Horsies::new(config).expect("app creation should succeed");

    // Attempt registration — it fails.
    let result = app.task::<tasks::ComputeInput, tasks::ComputeResult>(
        "fast_compute",
        async_task_fn!(tasks::add, tasks::ComputeInput),
    )
    .expect("task builder creation should succeed")
    .queue("fast")
    .register();

    assert!(result.is_err(), "registration must fail");

    // check() on an app with zero tasks should pass — the bad task never made
    // it into the registry, so there is nothing to complain about.
    app.check().expect("check should pass (no tasks registered)");
}

#[tokio::test]
async fn worker_never_starts() {
    let config = default_mode_config();
    let mut app = Horsies::new(config).expect("app creation should succeed");

    // Registration fails — the bad task is never stored.
    let result = app.task::<tasks::ComputeInput, tasks::ComputeResult>(
        "fast_compute",
        async_task_fn!(tasks::add, tasks::ComputeInput),
    )
    .expect("task builder creation should succeed")
    .queue("fast")
    .register();

    assert!(result.is_err(), "registration must reject queue in DEFAULT mode");

    // Even if we tried to start the worker, the app has no tasks registered.
    // The worker startup path (run_worker_with) would hit broker connect,
    // but the point is: the mismatched task never reached the registry.
    // We cannot call run_worker_with without a real DB, but we can verify
    // the worker config is constructible and check passes.
    let worker_config = WorkerConfig::default();
    assert_eq!(worker_config.queues, vec!["default"]);
    app.check().expect("check passes — no bad tasks in registry");
}
