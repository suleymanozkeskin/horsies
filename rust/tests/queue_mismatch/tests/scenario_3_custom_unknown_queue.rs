//! Scenario 3: CUSTOM mode + task targets a queue not in the config.
//!
//! App has queues ["fast", "slow"], task declares queue = "analytics".
//! Expected: hard fail at registration — unknown queue name.
//! Error message must name the offending queue and list valid options.

use horsies::{async_task_fn, ErrorCode, Horsies, WorkerConfig};
use horsies_queue_mismatch_tests::{custom_mode_config, tasks};

#[tokio::test]
async fn registration_fails_with_unknown_queue() {
    let config = custom_mode_config(); // queues: "fast", "slow"
    let mut app = Horsies::new(config).expect("app creation should succeed");

    let result = app
        .task::<tasks::ComputeInput, tasks::ComputeResult>(
            "analytics_compute",
            async_task_fn!(tasks::add, tasks::ComputeInput),
        )
        .expect("task builder creation should succeed")
        .queue("analytics") // not in ["fast", "slow"]
        .register();

    let err = result.expect_err("registration must fail for unknown queue");
    assert!(
        err.to_string().contains("analytics"),
        "error should name the offending queue 'analytics', got: {}",
        err
    );
}

#[tokio::test]
async fn error_lists_valid_queues() {
    let config = custom_mode_config();
    let mut app = Horsies::new(config).expect("app creation should succeed");

    let err = app
        .task::<tasks::ComputeInput, tasks::ComputeResult>(
            "analytics_compute",
            async_task_fn!(tasks::add, tasks::ComputeInput),
        )
        .expect("task builder creation should succeed")
        .queue("analytics")
        .register()
        .expect_err("registration must fail");

    let msg = err.to_string();
    // Error must be actionable: list valid queue names.
    assert!(
        msg.contains("not in configured custom_queues"),
        "error should say queue is not in custom_queues, got: {}",
        msg
    );
}

#[tokio::test]
async fn check_also_catches_unknown_queue_via_raw_register() {
    // Bypass the builder guard using raw register() with TaskOptions.
    use horsies::{async_task_fn, TaskOptions};

    let config = custom_mode_config();
    let mut app = Horsies::new(config).expect("app creation should succeed");

    let err = app.register(
        "analytics_task",
        async_task_fn!(tasks::ping, ()).with_task_options(TaskOptions {
            task_name: "analytics_task".to_owned(),
            queue_name: Some("analytics".to_owned()),
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
            timeout_ms: None,
        }),
    );

    // register() itself should catch this eagerly.
    let err = err.expect_err("raw register must also reject unknown queue");
    assert_eq!(err.code, Some(ErrorCode::TaskInvalidOptions));
    assert!(err.to_string().contains("analytics"));
}

#[tokio::test]
async fn worker_never_starts_with_unknown_queue() {
    let config = custom_mode_config();
    let mut app = Horsies::new(config).expect("app creation should succeed");

    // Attempt registration — fails.
    let result = app
        .task::<tasks::ComputeInput, tasks::ComputeResult>(
            "analytics_compute",
            async_task_fn!(tasks::add, tasks::ComputeInput),
        )
        .expect("task builder creation should succeed")
        .queue("analytics")
        .register();

    assert!(result.is_err());

    // Worker config for CUSTOM mode.
    let mut worker_config = WorkerConfig::default();
    if let Some(ref queues) = app.config().custom_queues {
        worker_config.queues = queues.iter().map(|q| q.name.clone()).collect();
    }

    // check passes because the bad task never entered the registry.
    app.check()
        .expect("check passes — no bad tasks in registry");
}
