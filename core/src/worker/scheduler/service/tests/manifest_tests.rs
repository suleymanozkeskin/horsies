//! A scheduler built from a client's manifest
//! ([`SchedulerService::from_manifest`]): the start checks of the manifest
//! path, and a start and a tick on the baseline schema with the send facts
//! of the manifest.

use serde_json::{json, Value};

use super::*;
use crate::scheduler_manifest::parse_scheduler_manifest;
use crate::test_support::sql::{self, TestPoolOptions};

/// The input text of each slot, in the key order of the client's text.
const INPUT_TEXT: &str = "{\"z\":1,\"a\":[true,null],\"m\":\"text\"}";

/// The options text of the task, as the client encoded it.
const OPTIONS_TEXT: &str = "{\"task_name\":\"manifest_task\",\"timeout_ms\":5000}";

/// An enabled schedule of a 60 s interval with the send facts of `task_name`.
fn enabled_schedule(name: &str, task_name: &str, timezone: &str, options: &Value) -> Value {
    json!({
        "name": name,
        "activation": {
            "kind": "ENABLED",
            "pattern_json":
                "{\"type\":\"interval\",\"seconds\":60,\"minutes\":null,\"hours\":null,\"days\":null}",
            "timezone": timezone,
            "catch_up_missed": false,
            "max_catch_up_runs": 100,
            "send": {
                "task_name": task_name,
                "queue_name": "default",
                "priority": 40,
                "input_json": INPUT_TEXT,
                "task_options": options,
                "retention_class_key": "standard_2d",
                "retain_rerun_input": true
            }
        }
    })
}

fn encoded_options() -> Value {
    json!({"kind": "ENCODED", "json": OPTIONS_TEXT})
}

fn manifest_of(enabled: bool, schedules: &Value) -> SchedulerManifest {
    let document = json!({
        "manifest_version": 1,
        "enabled": enabled,
        "check_interval_seconds": 1,
        "schedules": schedules
    });
    parse_scheduler_manifest(&document.to_string())
        .unwrap_or_else(|refused| panic!("the test manifest is complete: {refused}"))
}

/// A broker whose pool cannot connect: a statement would fail with a
/// connection error, not with a start refusal.
fn unreachable_broker() -> Arc<PostgresBroker> {
    let pool = TestPoolOptions::new()
        .acquire_timeout(Duration::from_millis(100))
        .connect_lazy("postgresql://horsies@127.0.0.1:1/unreachable");
    Arc::new(PostgresBroker::from_test_url(
        pool.url(),
        crate::ClientFamily::Rust,
    ))
}

async fn start_of(
    broker: Arc<PostgresBroker>,
    manifest: SchedulerManifest,
) -> Result<SchedulerStarted, SchedulerStartError> {
    SchedulerService::from_manifest(broker, manifest, &SchedulerStop::new())
        .start()
        .await
}

#[tokio::test]
async fn a_manifest_with_the_master_switch_off_or_no_schedule_starts_idle() {
    let off = manifest_of(
        false,
        &json!([enabled_schedule("off", "task", "UTC", &encoded_options())]),
    );
    assert!(matches!(
        start_of(unreachable_broker(), off).await,
        Ok(SchedulerStarted::Idle)
    ));
    assert!(matches!(
        start_of(unreachable_broker(), manifest_of(true, &json!([]))).await,
        Ok(SchedulerStarted::Idle)
    ));
}

#[tokio::test]
async fn a_zone_that_the_core_does_not_know_refuses_the_start_before_any_statement() {
    let manifest = manifest_of(
        true,
        &json!([
            enabled_schedule("known", "task", "Europe/Berlin", &encoded_options()),
            enabled_schedule("unknown", "task", "Not/A_Zone", &encoded_options())
        ]),
    );
    assert!(matches!(
        start_of(unreachable_broker(), manifest).await,
        Err(SchedulerStartError::UnknownTimezone { ref schedule, ref timezone })
            if schedule == "unknown" && timezone == "Not/A_Zone"
    ));
}

/// Options that the command rules refuse give no send command: the start
/// refuses the schedule (Python `_build_commands`, `CONFIG_INVALID_SCHEDULE`).
/// A disabled schedule holds no send facts, so nothing is checked for it.
#[tokio::test]
async fn options_that_the_command_rules_refuse_refuse_the_start_before_any_statement() {
    let manifest = manifest_of(
        true,
        &json!([
            {"name": "paused", "activation": {"kind": "DISABLED"}},
            enabled_schedule(
                "bad_options",
                "task",
                "UTC",
                &json!({"kind": "ENCODED", "json": "not json"})
            )
        ]),
    );
    assert!(matches!(
        start_of(unreachable_broker(), manifest).await,
        Err(SchedulerStartError::CommandRefused { ref schedule, .. }) if schedule == "bad_options"
    ));
}

/// The start writes the state row of the enabled schedule, with Python's
/// digest of its pattern and zone, and none for the disabled one. A tick
/// then fires the due slot with the send facts of the manifest: the input
/// and the options text as written, the queue's priority, the retention
/// class and the rerun-input posture of the manifest.
#[tokio::test]
async fn a_manifest_scheduler_starts_and_fires_with_the_send_facts_of_the_manifest() {
    let pool = test_pool().await;
    let name = format!("manifest_start_{}", Uuid::new_v4().simple());
    let task_name = name.clone();
    let paused = format!("manifest_paused_{}", Uuid::new_v4().simple());
    let manifest = manifest_of(
        true,
        &json!([
            enabled_schedule(&name, &task_name, "UTC", &encoded_options()),
            {"name": paused, "activation": {"kind": "DISABLED"}}
        ]),
    );
    let broker = Arc::new(PostgresBroker::from_test_url(
        pool.url(),
        crate::ClientFamily::Rust,
    ));

    let mut active = match start_of(broker, manifest).await {
        Ok(SchedulerStarted::Active(active)) => active,
        Ok(SchedulerStarted::Idle) => panic!("an enabled schedule starts an active scheduler"),
        Err(refused) => panic!("the start is refused: {refused}"),
    };

    let row = state::test_rows::state_row(&pool, &name)
        .await
        .expect("the start writes the state row of the enabled schedule");
    let digest = crate::worker::scheduler::config_digest::config_digest(
        &SchedulePattern::Interval(crate::core::config::IntervalSchedule {
            seconds: Some(60),
            minutes: None,
            hours: None,
            days: None,
        }),
        "UTC",
    );
    assert_eq!(row.config_hash, digest.to_vec());
    assert!(state::test_rows::state_row(&pool, &paused).await.is_none());

    let due = database_time(&pool).await - chrono::Duration::seconds(1);
    let _due =
        sql::query("UPDATE horsies_schedule_state SET next_run_at = $2 WHERE schedule_name = $1")
            .bind(&name)
            .bind(due)
            .execute(&pool)
            .await
            .expect("make the schedule due");
    active
        .tick()
        .await
        .expect("the tick reads the due schedules");

    let (input, task_options, priority, retention, retain): (
        String,
        Option<String>,
        i32,
        String,
        bool,
    ) = sql::query_as(
        "SELECT input, task_options, priority, retention_class_key, retain_rerun_input
             FROM horsies_tasks WHERE task_name = $1",
    )
    .bind(&task_name)
    .fetch_one(&pool)
    .await
    .expect("the tick fires one slot task");
    assert_eq!(input, INPUT_TEXT);
    assert_eq!(task_options.as_deref(), Some(OPTIONS_TEXT));
    assert_eq!(priority, 40);
    assert_eq!(retention, "standard_2d");
    assert!(retain);
    let row = state::test_rows::state_row(&pool, &name)
        .await
        .expect("the state row stays");
    assert_eq!(row.run_count, 1);
    cleanup(&pool, &task_name, &name).await;
}
