//! Scenario 4: CUSTOM mode + task targets a valid queue.
//!
//! App has queues ["fast", "slow"], task declares queue = "fast".
//! Expected: pass. Registration succeeds, check() passes, worker config is valid.
//! Worker startup will fail at DB connection — that's fine, it got past queue validation.

use horsies::{async_task_fn, Horsies, WorkerConfig};
use horsies_queue_mismatch_tests::{custom_mode_config, tasks};

#[tokio::test]
async fn registration_succeeds_with_valid_queue() {
    let config = custom_mode_config();
    let mut app = Horsies::new(config).expect("app creation should succeed");

    let task = app
        .task::<tasks::ComputeInput, tasks::ComputeResult>(
            "fast_compute",
            async_task_fn!(tasks::add, tasks::ComputeInput),
        )
        .expect("task builder creation should succeed")
        .queue("fast")
        .register()
        .expect("registration must succeed for valid queue");

    assert_eq!(task.task_name(), "fast_compute");
    assert_eq!(task.queue_name(), "fast");
}

#[tokio::test]
async fn check_passes() {
    let config = custom_mode_config();
    let mut app = Horsies::new(config).expect("app creation should succeed");

    app.task::<tasks::ComputeInput, tasks::ComputeResult>(
        "fast_compute",
        async_task_fn!(tasks::add, tasks::ComputeInput),
    )
    .expect("task builder creation should succeed")
    .queue("fast")
    .register()
    .expect("registration must succeed");

    app.check().expect("check must pass with valid queue");
}

#[tokio::test]
async fn worker_config_is_valid() {
    let config = custom_mode_config();
    let mut app = Horsies::new(config).expect("app creation should succeed");

    app.task::<tasks::ComputeInput, tasks::ComputeResult>(
        "fast_compute",
        async_task_fn!(tasks::add, tasks::ComputeInput),
    )
    .expect("task builder creation should succeed")
    .queue("fast")
    .register()
    .expect("registration must succeed");

    app.task::<tasks::ComputeInput, tasks::ComputeResult>(
        "slow_compute",
        async_task_fn!(tasks::multiply, tasks::ComputeInput),
    )
    .expect("task builder creation should succeed")
    .queue("slow")
    .register()
    .expect("registration must succeed");

    app.check().expect("check must pass");

    // Build a real worker config subscribing to all custom queues.
    let mut worker_config = WorkerConfig::default();
    if let Some(ref queues) = app.config().custom_queues {
        worker_config.queues = queues.iter().map(|q| q.name.clone()).collect();
    }
    worker_config.concurrency = 4;

    assert_eq!(worker_config.queues, vec!["fast", "slow"]);

    // run_worker_with would fail at broker.get() (no real DB), but the queue
    // validation is already past. We verify by checking the error type.
    let err = app.run_worker_with(worker_config).await.unwrap_err();
    let err_str = format!("{}", err);
    // The error must be a broker/connection error, NOT a queue validation error.
    assert!(
        !err_str.contains("queue") && !err_str.contains("TaskInvalidOptions"),
        "worker should fail at DB connection, not queue validation. Got: {}",
        err_str
    );
}
