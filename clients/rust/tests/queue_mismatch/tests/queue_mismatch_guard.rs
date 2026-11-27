//! The queue guard of task registration, in six scenarios (Python
//! `clients/python/tests/unit/test_queue_mismatch_guard.py`). A refused registration
//! leaves nothing registered: `main` returns the refusal with `?`, so a
//! binary with a mismatched task never reaches `horsies::cli::run`.

use horsies::{Horsies, HorsiesError, HorsiesErrorCode};
use horsies_queue_mismatch_tests::{
    analytics_compute, compute_body, custom_mode_config, default_mode_config, fast_compute, ping,
    slow_compute, ComputeInput, UNUSED_DATABASE_URL,
};

fn default_app() -> Horsies {
    Horsies::new(default_mode_config(UNUSED_DATABASE_URL)).expect("a Default mode app")
}

fn custom_app() -> Horsies {
    Horsies::new(custom_mode_config()).expect("a Custom mode app")
}

/// The check of an app with `count` registered tasks passes.
fn assert_check_passes(app: &Horsies, count: usize) {
    let passed = app
        .check()
        .unwrap_or_else(|report| panic!("the check refused: {report}"));
    assert_eq!(passed.task_count(), count);
}

/// The `main` of a binary that registers a valid and then an invalid task:
/// the first refusal ends it before any service starts.
fn build_mixed_app() -> Result<Horsies, HorsiesError> {
    let mut app = custom_app();
    let _fast = fast_compute::register(&mut app)?;
    let _analytics = analytics_compute::register(&mut app)?;
    Ok(app)
}

// ---- Scenario 1: Default mode, a task with a queue ----

/// Python: `TestDefaultModeRejectsQueueName::test_task_with_queue_name_raises`.
#[test]
fn scenario_1_registration_fails_with_queue_in_default_mode() {
    let mut app = default_app();
    let refused = fast_compute::register(&mut app).expect_err("a queue in Default mode");
    assert_eq!(refused.code, Some(HorsiesErrorCode::ConfigInvalidQueueMode));
    assert!(
        refused.notes.iter().any(|note| note.contains("fast")),
        "the refusal names the queue: {refused:?}"
    );
}

/// Python: `TestCheckCollectsQueueMismatch::test_check_catches_queue_mismatch_from_import`.
#[test]
fn scenario_1_check_never_reached() {
    let mut app = default_app();
    assert!(fast_compute::register(&mut app).is_err());
    assert_check_passes(&app, 0);
}

/// Python: `TestDefaultModeRejectsQueueName`; a worker of the app serves
/// only `default`.
#[test]
fn scenario_1_worker_never_starts() {
    let mut app = default_app();
    assert!(fast_compute::register(&mut app).is_err());
    assert_eq!(app.get_valid_queue_names(), vec!["default"]);
    assert_check_passes(&app, 0);
}

// ---- Scenario 2: Custom mode, a task with no queue ----

/// Python: `TestCustomModeRequiresQueueName::test_task_without_queue_name_raises`.
#[test]
fn scenario_2_registration_fails_without_queue_in_custom_mode() {
    let mut app = custom_app();
    let refused = ping::register(&mut app).expect_err("no queue in Custom mode");
    assert_eq!(refused.code, Some(HorsiesErrorCode::ConfigInvalidQueueMode));
    assert!(refused.help_text.is_some(), "the refusal suggests the fix");
}

/// Python: `TestCheckCollectsQueueMismatch::test_check_catches_queue_mismatch_from_import`.
#[test]
fn scenario_2_check_never_reached() {
    let mut app = custom_app();
    assert!(ping::register(&mut app).is_err());
    assert_check_passes(&app, 0);
}

/// Python: `TestCustomModeRequiresQueueName`; a worker of the app serves the
/// configured queues.
#[test]
fn scenario_2_worker_config_subscribes_to_custom_queues() {
    let app = custom_app();
    assert_eq!(app.get_valid_queue_names(), vec!["fast", "slow"]);
}

// ---- Scenario 3: Custom mode, a queue that is not configured ----

/// Python: `TestCustomModeRejectsUnknownQueue::test_task_with_unknown_queue_raises`.
#[test]
fn scenario_3_registration_fails_with_unknown_queue() {
    let mut app = custom_app();
    let refused = analytics_compute::register(&mut app).expect_err("an unknown queue");
    assert_eq!(refused.code, Some(HorsiesErrorCode::TaskInvalidQueue));
    assert!(refused.message.contains("analytics"), "{refused:?}");
}

/// Python: `TestErrorMessageQuality::test_custom_mode_invalid_queue_lists_valid_options`.
#[test]
fn scenario_3_error_lists_valid_queues() {
    let mut app = custom_app();
    let refused = analytics_compute::register(&mut app).expect_err("an unknown queue");
    assert!(
        refused
            .notes
            .iter()
            .any(|note| note.contains("fast") && note.contains("slow")),
        "the refusal lists the valid queues: {refused:?}"
    );
}

/// Python: `TestSendTimeValidation::test_validate_queue_custom_mode_rejects_unknown`.
/// Registration by hand meets the same guard.
#[test]
fn scenario_3_check_also_catches_unknown_queue_via_raw_register() {
    let mut app = custom_app();
    let refused = app
        .register_with_queue(
            "analytics_task",
            horsies::async_task_fn!(compute_body, ComputeInput),
            "analytics",
        )
        .expect_err("an unknown queue by hand");
    assert_eq!(refused.code, Some(HorsiesErrorCode::TaskInvalidQueue));
    assert!(refused.message.contains("analytics"));
    assert!(app.validate_queue("analytics").is_err());
}

/// Python: `TestCustomModeRejectsUnknownQueue`; the refused task never
/// reaches the registry.
#[test]
fn scenario_3_worker_never_starts_with_unknown_queue() {
    let mut app = custom_app();
    assert!(analytics_compute::register(&mut app).is_err());
    assert_check_passes(&app, 0);
}

// ---- Scenario 4: Custom mode, a configured queue ----

/// Python: `TestCustomModeAcceptsValidQueue::test_task_with_valid_queue_succeeds`.
#[test]
fn scenario_4_registration_succeeds_with_valid_queue() {
    let mut app = custom_app();
    let function = fast_compute::register(&mut app).expect("a configured queue");
    assert_eq!(function.task_name(), "fast_compute");
    assert_eq!(function.queue_name(), "fast");
    assert_eq!(function.priority(), 1);
}

/// Python: `TestCustomModeAcceptsValidQueue::test_task_with_valid_queue_succeeds`.
#[test]
fn scenario_4_check_passes() {
    let mut app = custom_app();
    let _fast = fast_compute::register(&mut app).expect("a configured queue");
    assert_check_passes(&app, 1);
}

/// Python: `TestCustomModeAcceptsValidQueue::test_multiple_tasks_on_different_valid_queues`.
#[test]
fn scenario_4_worker_config_is_valid() {
    let mut app = custom_app();
    let fast = fast_compute::register(&mut app).expect("fast");
    let slow = slow_compute::register(&mut app).expect("slow");
    assert_eq!((fast.queue_name(), slow.queue_name()), ("fast", "slow"));
    assert_eq!((fast.priority(), slow.priority()), (1, 50));
    assert_check_passes(&app, 2);
}

// ---- Scenario 5: Default mode, a task with no queue ----

/// Python: `TestDefaultModeAcceptsNoQueue::test_task_without_queue_succeeds`.
#[test]
fn scenario_5_registration_succeeds_without_queue() {
    let mut app = default_app();
    let function = ping::register(&mut app).expect("no queue in Default mode");
    assert_eq!(function.task_name(), "ping");
    assert_eq!(function.queue_name(), "default");
}

/// Python: `TestDefaultModeAcceptsNoQueue::test_task_without_queue_succeeds`.
#[test]
fn scenario_5_check_passes() {
    let mut app = default_app();
    let _ping = ping::register(&mut app).expect("no queue in Default mode");
    assert_check_passes(&app, 1);
}

/// Python: `TestDefaultModeAcceptsNoQueue`; the app of a passing check
/// reaches the database: its first operation installs the schema on a
/// fresh database of the test server.
#[tokio::test]
async fn scenario_5_worker_startup_reaches_broker() {
    let database = horsies_test_support::database::TestDatabase::create().await;
    let mut app = Horsies::new(default_mode_config(database.url())).expect("an app");
    let _ping = ping::register(&mut app).expect("no queue in Default mode");
    assert_check_passes(&app, 1);
    let reached = app
        .ping_database()
        .await
        .expect("the app reaches its database");
    assert!(reached.latency_ms >= 0.0);
}

// ---- Scenario 6: a valid task, then a task with an unknown queue ----

/// Python: `TestCustomModeRejectsUnknownQueue::test_task_with_unknown_queue_raises`
/// after a valid registration.
#[test]
fn scenario_6_first_valid_second_invalid_poisons_startup() {
    let refused = build_mixed_app().expect_err("the second registration refuses");
    assert_eq!(refused.code, Some(HorsiesErrorCode::TaskInvalidQueue));
    assert!(refused.message.contains("analytics"));
}

/// Python: `TestSendTimeValidation::test_validate_queue_custom_mode_rejects_unknown`.
/// The guard holds on the hand registration path too.
#[test]
fn scenario_6_check_fails_if_bad_task_bypassed_register() {
    let mut app = custom_app();
    let _fast = fast_compute::register(&mut app).expect("a configured queue");
    let refused = app
        .register_with_queue(
            "analytics_task",
            horsies::async_task_fn!(compute_body, ComputeInput),
            "analytics",
        )
        .expect_err("the hand path refuses too");
    assert!(refused.message.contains("analytics"));
    assert_check_passes(&app, 1);
}

/// Python: `TestCheckCollectsQueueMismatch`: no app comes out of a `main`
/// whose registrations refuse, so no worker starts.
#[test]
fn scenario_6_worker_never_starts_due_to_poisoned_registration() {
    assert!(build_mixed_app().is_err());
}
