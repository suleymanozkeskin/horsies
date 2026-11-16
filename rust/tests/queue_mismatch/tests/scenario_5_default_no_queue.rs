//! Scenario 5: DEFAULT mode + task with no queue.
//!
//! Expected: pass. This is the standard happy path for DEFAULT mode.
//! Registration succeeds, check() passes, worker startup proceeds to DB.

use horsies::{async_task_fn, Horsies, WorkerConfig};
use horsies_queue_mismatch_tests::{default_mode_config, tasks};

#[tokio::test]
async fn registration_succeeds_without_queue() {
    let config = default_mode_config();
    let mut app = Horsies::new(config).expect("app creation should succeed");

    let task = app
        .task::<(), String>("ping", async_task_fn!(tasks::ping, ()))
        .expect("task builder creation should succeed")
        // No .queue() call — correct for DEFAULT mode.
        .register()
        .expect("registration must succeed in DEFAULT mode without queue");

    assert_eq!(task.task_name(), "ping");
    assert_eq!(task.queue_name(), "default");
}

#[tokio::test]
async fn check_passes() {
    let config = default_mode_config();
    let mut app = Horsies::new(config).expect("app creation should succeed");

    app.task::<tasks::ComputeInput, tasks::ComputeResult>(
        "compute",
        async_task_fn!(tasks::add, tasks::ComputeInput),
    )
    .expect("task builder creation should succeed")
    .register()
    .expect("registration must succeed");

    app.task::<(), String>("ping", async_task_fn!(tasks::ping, ()))
        .expect("task builder creation should succeed")
        .register()
        .expect("registration must succeed");

    app.check()
        .expect("check must pass with DEFAULT mode + no queues");
}

#[tokio::test]
async fn worker_startup_reaches_broker() {
    let config = default_mode_config();
    let mut app = Horsies::new(config).expect("app creation should succeed");

    app.task::<(), String>("ping", async_task_fn!(tasks::ping, ()))
        .expect("task builder creation should succeed")
        .register()
        .expect("registration must succeed");

    app.check().expect("check must pass");

    // Standard default worker config.
    let worker_config = WorkerConfig::default();
    assert_eq!(worker_config.queues, vec!["default"]);

    // run_worker_with fails at broker.get() — no real DB. But queue validation
    // is long past. The error must be a broker error, not a config error.
    let err = app
        .run_worker_with(worker_config)
        .await
        .expect_err("worker must fail to start without a real DB");
    let err_str = format!("{}", err);
    assert!(
        !err_str.contains("queue mode") && !err_str.contains("TaskInvalidOptions"),
        "worker should fail at DB connection, not queue validation. Got: {}",
        err_str
    );
}
