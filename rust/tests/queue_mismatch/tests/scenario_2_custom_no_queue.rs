//! Scenario 2: CUSTOM mode + task omits queue entirely.
//!
//! Expected: hard fail at registration — queue is required in CUSTOM mode.
//! The app never reaches check(), the worker never starts.

use horsies::{async_task_fn, ErrorCode, Horsies, WorkerConfig};
use horsies_queue_mismatch_tests::{custom_mode_config, tasks};

#[tokio::test]
async fn registration_fails_without_queue_in_custom_mode() {
    let config = custom_mode_config();
    let mut app = Horsies::new(config).expect("app creation should succeed");

    // Register a task WITHOUT specifying a queue in CUSTOM mode.
    let result = app
        .task::<(), String>("bare_ping", async_task_fn!(tasks::ping, ()))
        .expect("task builder creation should succeed")
        // Note: NO .queue() call — this is the mismatch.
        .register();

    // Must fail: CUSTOM mode requires an explicit queue.
    let err = result.expect_err("registration must fail without queue in CUSTOM mode");
    assert_eq!(
        err.code,
        Some(ErrorCode::TaskInvalidOptions),
        "error code should be TaskInvalidOptions, got: {}",
        err
    );
    assert!(
        err.to_string().contains("Custom queue mode"),
        "error should mention Custom queue mode, got: {}",
        err
    );
}

#[tokio::test]
async fn check_never_reached() {
    let config = custom_mode_config();
    let mut app = Horsies::new(config).expect("app creation should succeed");

    let result = app
        .task::<(), String>("bare_ping", async_task_fn!(tasks::ping, ()))
        .expect("task builder creation should succeed")
        .register();

    assert!(result.is_err(), "registration must fail");
    app.check()
        .expect("check passes — bad task never reached registry");
}

#[tokio::test]
async fn worker_config_subscribes_to_custom_queues() {
    let config = custom_mode_config();
    let app = Horsies::new(config).expect("app creation should succeed");

    // A real worker config for CUSTOM mode subscribes to the configured queues.
    let mut worker_config = WorkerConfig::default();
    if let Some(ref queues) = app.config().custom_queues {
        worker_config.queues = queues.iter().map(|q| q.name.clone()).collect();
    }

    assert_eq!(worker_config.queues, vec!["fast", "slow"]);
    // But the task registration above failed, so the worker would have
    // zero tasks even if we started it — the mismatch was caught.
}
