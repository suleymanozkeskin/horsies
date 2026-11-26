use super::*;
use crate::scheduler_manifest::{parse_scheduler_manifest, ScheduleTaskOptions};
use crate::test_support::sql::{self, TestPool};
use chrono::{TimeZone, Utc};
use serde_json::json;
use uuid::Uuid;

#[test]
fn compute_config_hash_deterministic() {
    let schedule = interval_schedule_secs("test", 60);

    let hash1 = compute_config_hash(&enabled(&schedule));
    let hash2 = compute_config_hash(&enabled(&schedule));
    assert_eq!(hash1, hash2);
}

#[test]
fn compute_config_hash_changes_on_different_config() {
    let schedule1 = interval_schedule_secs("test", 60);
    let schedule2 = interval_schedule_secs("test", 120);

    assert_ne!(
        compute_config_hash(&enabled(&schedule1)),
        compute_config_hash(&enabled(&schedule2))
    );
}

#[test]
fn compute_config_hash_is_stable_sha256() {
    // The config hash is SHA-256 of Python's digest text, not a
    // toolchain-dependent hasher; the golden value is the digest that
    // Python's `_compute_config_hash` stores for the same schedule.
    let schedule = interval_schedule_secs("test", 60);
    let hash = compute_config_hash(&enabled(&schedule));
    // SHA-256 of
    // {"pattern":{"type":"interval","seconds":60,"minutes":null,"hours":null,"days":null},"timezone":"UTC"}.
    let hex: String = hash.iter().map(|byte| format!("{byte:02x}")).collect();
    assert_eq!(
        hex,
        "a7bba31d521252fd1fb6e910224d2c9a13daa9d45b4c3a1c4569f7b76a2e92e1",
    );
}

/// Python `_compute_config_hash` covers the pattern and the timezone
/// only: a change of the send facts or the catch-up settings does not move
/// `next_run_at`.
#[test]
fn compute_config_hash_covers_pattern_and_timezone_only() {
    let base = interval_schedule_secs("hash_scope", 60);
    let hash = compute_config_hash(&enabled(&base));

    let mut other_settings = base.clone();
    other_settings.task_name = "another_task".to_owned();
    other_settings.input_json = json!({"changed": true}).to_string();
    other_settings.queue_name = "bulk".to_owned();
    other_settings.priority = 7;
    other_settings.task_options = ScheduleTaskOptions::Encoded {
        json: "{\"task_name\":\"another_task\",\"timeout_ms\":5000}".to_owned(),
    };
    other_settings.retention_class_key = "standard_2d".to_owned();
    other_settings.retain_rerun_input = true;
    other_settings.catch_up_missed = true;
    other_settings.max_catch_up_runs = 7;
    assert_eq!(compute_config_hash(&enabled(&other_settings)), hash);

    let mut other_timezone = base.clone();
    other_timezone.timezone = "Europe/Istanbul".to_owned();
    assert_ne!(compute_config_hash(&enabled(&other_timezone)), hash);

    let other_pattern = interval_schedule_secs("hash_scope", 61);
    assert_ne!(compute_config_hash(&enabled(&other_pattern)), hash);
}

/// A broker handle of the other client family is refused before the
/// first schedule-state write: the start gives
/// `SchedulerStartError::ClientFamily` with the mismatch, not retryable,
/// and no state row is written.
#[tokio::test]
async fn a_handle_of_the_other_family_starts_no_scheduler_and_writes_no_state() {
    use crate::broker::client_family::{ClientFamily, RecordedFamily};
    use crate::broker::BrokerError;

    let pool = test_pool().await;
    let broker = Arc::new(PostgresBroker::from_test_url(
        pool.url(),
        ClientFamily::Python,
    ));
    let name = format!("scheduler_family_{}", uuid::Uuid::new_v4().simple());
    let manifest = manifest_for(&[interval_schedule_secs(&name, 60)]);

    let outcome = SchedulerService::from_manifest(broker, manifest, &SchedulerStop::new())
        .start()
        .await;

    let refusal = match outcome {
        Err(SchedulerStartError::ClientFamily(refusal)) => refusal,
        Err(other) => panic!("expected the client family refusal, got {other:?}"),
        Ok(_started) => panic!("expected the client family refusal, the scheduler started"),
    };
    assert!(matches!(
        refusal,
        BrokerError::ClientFamilyMismatch {
            stored: RecordedFamily::Recorded(ClientFamily::Rust),
            own: ClientFamily::Python,
        }
    ));
    assert!(!refusal.is_retryable());
    assert!(state::test_rows::state_row(&pool, &name).await.is_none());
}

/// Python `test_scheduler_service.py::test_normal_run_permanent_failure_skips_slot`:
/// a fire that the database refuses on every try moves the row past the
/// slot with a second fire of no slots, records no run and writes no task
/// (horsies-rust #77).
#[tokio::test]
async fn normal_run_permanent_failure_skips_slot() {
    let pool = test_pool().await;
    let name = format!("permanent_skip_{}", Uuid::new_v4().simple());
    let mut schedule = interval_schedule_secs(&name, 60);
    schedule.task_name = name.clone();
    let slot = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    state::test_rows::seed_state(&pool, &name, slot).await;
    refuse_slot_rows(&pool, &name).await;

    let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 30).unwrap();
    let fired = fire_once(&pool, &schedule, slot, now).await;
    drop_slot_refusal(&pool).await;

    fired.expect("a refused fire is handled by the skip");
    let row = state::test_rows::state_row(&pool, &name).await.unwrap();
    assert_eq!(
        row.next_run_at,
        Utc.with_ymd_and_hms(2025, 6, 1, 12, 1, 0).unwrap()
    );
    assert_eq!(row.run_count, 0);
    assert!(row.last_task_id.is_none());
    assert_eq!(task_count(&pool, &name).await, 0);
    cleanup(&pool, &name, &name).await;
}

/// Python `test_scheduler_service.py::test_catch_up_permanent_failure_skips_doomed_slot`:
/// in catch-up mode a refused fire of three due slots moves past the first
/// slot only, records no run and writes no task.
#[tokio::test]
async fn catch_up_permanent_failure_skips_doomed_slot() {
    let pool = test_pool().await;
    let name = format!("catch_up_skip_{}", Uuid::new_v4().simple());
    let mut schedule = interval_schedule_secs(&name, 60);
    schedule.task_name = name.clone();
    schedule.catch_up_missed = true;
    let first_slot = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    state::test_rows::seed_state(&pool, &name, first_slot).await;
    refuse_slot_rows(&pool, &name).await;

    // Three slots are due: 12:00, 12:01, 12:02.
    let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 2, 30).unwrap();
    let fired = fire_once(&pool, &schedule, first_slot, now).await;
    drop_slot_refusal(&pool).await;

    fired.expect("a refused fire is handled by the skip");
    let row = state::test_rows::state_row(&pool, &name).await.unwrap();
    assert_eq!(
        row.next_run_at,
        Utc.with_ymd_and_hms(2025, 6, 1, 12, 1, 0).unwrap()
    );
    assert_eq!(row.run_count, 0);
    assert_eq!(task_count(&pool, &name).await, 0);
    cleanup(&pool, &name, &name).await;
}

#[test]
fn calculate_missed_runs_returns_consecutive_slots_up_to_cap() {
    let schedule = nightly_catch_up_schedule();
    let first_due = Utc.with_ymd_and_hms(2025, 3, 29, 12, 0, 0).unwrap();
    let now = Utc.with_ymd_and_hms(2025, 3, 29, 12, 4, 30).unwrap();

    let missed = calculate_missed_runs(&enabled(&schedule), first_due, now, 3);

    assert_eq!(
        missed,
        vec![
            Utc.with_ymd_and_hms(2025, 3, 29, 12, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2025, 3, 29, 12, 1, 0).unwrap(),
            Utc.with_ymd_and_hms(2025, 3, 29, 12, 2, 0).unwrap(),
        ]
    );
}

#[test]
fn calculate_missed_runs_returns_empty_when_first_due_is_in_the_future() {
    let schedule = nightly_catch_up_schedule();
    let first_due = Utc.with_ymd_and_hms(2025, 3, 29, 12, 5, 0).unwrap();
    let now = Utc.with_ymd_and_hms(2025, 3, 29, 12, 4, 59).unwrap();

    let missed = calculate_missed_runs(&enabled(&schedule), first_due, now, 3);

    assert!(missed.is_empty());
}

/// The main core pool on the test database of `pool`.
fn core_pool_of(pool: &TestPool) -> CorePool {
    crate::broker::pools::core_pool_for_test_url(pool.url())
}

/// The database time, read in its own statement.
async fn database_time(pool: &TestPool) -> DateTime<Utc> {
    sql::query_scalar("SELECT statement_timestamp()")
        .fetch_one(pool)
        .await
        .expect("read the database time")
}

/// Fire `schedule` once as a tick does: its row is due at `first_due`, and
/// the due read ran at `database_now`.
async fn fire_once(
    pool: &TestPool,
    schedule: &TestSchedule,
    first_due: DateTime<Utc>,
    database_now: DateTime<Utc>,
) -> Result<(), ScheduleCheckError> {
    let core = core_pool_of(pool);
    let command = command_for(schedule);
    let due = DueSchedule {
        schedule_name: schedule.name.clone(),
        next_run_at: first_due,
    };
    let outcome = fire_due_schedule(
        &core,
        StatementLog::Off,
        &enabled(schedule),
        &command,
        &due,
        database_now,
    )
    .await;
    core.close();
    outcome
}

/// The task rows of `task_name`.
async fn task_count(pool: &TestPool, task_name: &str) -> i64 {
    sql::query_scalar("SELECT count(*) FROM horsies_tasks WHERE task_name = $1")
        .bind(task_name)
        .fetch_one(pool)
        .await
        .expect("count the task rows")
}

/// Delete the task rows of `task_name` and the state row of `schedule_name`.
async fn cleanup(pool: &TestPool, task_name: &str, schedule_name: &str) {
    let _deleted = sql::query("DELETE FROM horsies_tasks WHERE task_name = $1")
        .bind(task_name)
        .execute(pool)
        .await
        .expect("delete the task rows");
    state::test_rows::delete_state(pool, schedule_name).await;
}

/// A trigger that refuses every task row of `task_name` with SQLSTATE
/// P0001: a database refusal of the fire that repeats on every try (as a
/// CHECK violation of the slot rows). [`drop_slot_refusal`] removes it.
async fn refuse_slot_rows(pool: &TestPool, task_name: &str) {
    let _created = sql::raw_sql(&format!(
        "CREATE FUNCTION test_refuse_slot_rows() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.task_name = '{task_name}' THEN
                 RAISE EXCEPTION 'injected slot refusal';
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER test_refuse_slot_rows BEFORE INSERT ON horsies_tasks
             FOR EACH ROW EXECUTE FUNCTION test_refuse_slot_rows();"
    ))
    .execute(pool)
    .await
    .expect("create the refusal trigger");
}

async fn drop_slot_refusal(pool: &TestPool) {
    let _dropped = sql::raw_sql(
        "DROP TRIGGER test_refuse_slot_rows ON horsies_tasks;
         DROP FUNCTION test_refuse_slot_rows();",
    )
    .execute(pool)
    .await
    .expect("drop the refusal trigger");
}

async fn test_pool() -> TestPool {
    crate::test_support::migrated_pool().await
}

/// One enabled schedule of a test manifest: an interval in whole seconds,
/// with the send facts that a client resolved.
#[derive(Debug, Clone)]
struct TestSchedule {
    name: String,
    task_name: String,
    interval_seconds: u32,
    timezone: String,
    catch_up_missed: bool,
    max_catch_up_runs: u32,
    queue_name: String,
    priority: u32,
    input_json: String,
    task_options: ScheduleTaskOptions,
    retention_class_key: String,
    retain_rerun_input: bool,
}

impl TestSchedule {
    /// The manifest entry of the schedule, as a client writes it.
    fn entry(&self) -> serde_json::Value {
        let task_options = match &self.task_options {
            ScheduleTaskOptions::Encoded { json } => json!({"kind": "ENCODED", "json": json}),
            ScheduleTaskOptions::NoOptions => json!({"kind": "NO_OPTIONS"}),
        };
        let pattern_json = format!(
            "{{\"type\":\"interval\",\"seconds\":{},\"minutes\":null,\"hours\":null,\"days\":null}}",
            self.interval_seconds
        );
        json!({
            "name": self.name,
            "activation": {
                "kind": "ENABLED",
                "pattern_json": pattern_json,
                "timezone": self.timezone,
                "catch_up_missed": self.catch_up_missed,
                "max_catch_up_runs": self.max_catch_up_runs,
                "send": {
                    "task_name": self.task_name,
                    "queue_name": self.queue_name,
                    "priority": self.priority,
                    "input_json": self.input_json,
                    "task_options": task_options,
                    "retention_class_key": self.retention_class_key,
                    "retain_rerun_input": self.retain_rerun_input
                }
            }
        })
    }
}

/// A UTC schedule of `my_task` every `seconds`, with no catch-up, on the
/// default queue: priority 100, the input `null`, no task options, kept
/// forever, no rerun input.
fn interval_schedule_secs(name: &str, seconds: u32) -> TestSchedule {
    TestSchedule {
        name: name.to_owned(),
        task_name: "my_task".to_owned(),
        interval_seconds: seconds,
        timezone: "UTC".to_owned(),
        catch_up_missed: false,
        max_catch_up_runs: 100,
        queue_name: "default".to_owned(),
        priority: 100,
        input_json: "null".to_owned(),
        task_options: ScheduleTaskOptions::NoOptions,
        retention_class_key: "forever".to_owned(),
        retain_rerun_input: false,
    }
}

/// A catch-up schedule every 60 s with at most three slots per fire.
fn nightly_catch_up_schedule() -> TestSchedule {
    let mut schedule = interval_schedule_secs("nightly-reindex", 60);
    schedule.task_name = "reindex".to_owned();
    schedule.catch_up_missed = true;
    schedule.max_catch_up_runs = 3;
    schedule
}

/// The manifest entry of a disabled schedule: its name only.
fn disabled_entry(name: &str) -> serde_json::Value {
    json!({"name": name, "activation": {"kind": "DISABLED"}})
}

/// A manifest with the master switch on, a tick of one second and
/// `entries` in order.
fn manifest_of_entries(entries: &[serde_json::Value]) -> SchedulerManifest {
    let document = json!({
        "manifest_version": 1,
        "enabled": true,
        "check_interval_seconds": 1,
        "schedules": entries
    });
    parse_scheduler_manifest(&document.to_string())
        .unwrap_or_else(|refused| panic!("the test manifest is complete: {refused}"))
}

/// The manifest of the enabled `schedules`.
fn manifest_for(schedules: &[TestSchedule]) -> SchedulerManifest {
    manifest_of_entries(
        &schedules
            .iter()
            .map(TestSchedule::entry)
            .collect::<Vec<_>>(),
    )
}

/// The start plan of `manifest`, as the start builds it before any
/// statement.
fn active_plan(manifest: SchedulerManifest) -> ActivePlan {
    match manifest_plan(manifest) {
        Ok(StartPlan::Active(plan)) => plan,
        Ok(StartPlan::Idle) => panic!("a test manifest with a schedule starts active"),
        Err(refused) => panic!("the start plan of the test manifest is refused: {refused}"),
    }
}

/// The enabled schedule of `schedule`, as the start reads it.
fn enabled(schedule: &TestSchedule) -> EnabledSchedule {
    active_plan(manifest_for(std::slice::from_ref(schedule)))
        .schedules
        .into_iter()
        .next()
        .expect("the plan holds the enabled schedule")
}

/// The send command of `schedule`, as the start builds it.
fn command_for(schedule: &TestSchedule) -> SendCommand {
    active_plan(manifest_for(std::slice::from_ref(schedule)))
        .commands
        .remove(&schedule.name)
        .expect("one command per enabled schedule")
}

/// A fired slot stores the slot's task id, the retention class and the
/// rerun-input posture of its manifest send, a command fingerprint and the
/// input as one JSON value.
#[tokio::test]
async fn scheduled_enqueue_persists_the_class_and_shared_facts_of_its_send() {
    let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
    let task_name = "p6_scheduled_facts";
    let mut schedule = interval_schedule_secs("p6-scheduled-facts", 60);
    schedule.task_name = task_name.to_owned();
    schedule.queue_name = "bulk".to_owned();
    schedule.retention_class_key = "standard_2d".to_owned();
    let input = json!({"value": 1, "named": true});
    schedule.input_json = input.to_string();
    cleanup(&pool, task_name, &schedule.name).await;
    let database_now = database_time(&pool).await;
    let slot = database_now - chrono::Duration::seconds(1);
    state::test_rows::seed_state(&pool, &schedule.name, slot).await;

    fire_once(&pool, &schedule, slot, database_now)
        .await
        .expect("the fire");
    let row: (String, String, i32, bool, String) = sql::query_as(
        "SELECT id::text, retention_class_key,
                octet_length(command_fingerprint), retain_rerun_input, input
         FROM horsies_tasks WHERE task_name = $1",
    )
    .bind(task_name)
    .fetch_one(&pool)
    .await
    .unwrap();
    let slot_id: Uuid = sql::query_scalar("SELECT horsies_schedule_slot_task_id($1, $2)")
        .bind(&schedule.name)
        .bind(slot)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.0, slot_id.to_string());
    assert_eq!(row.1, "standard_2d");
    assert_eq!(row.2, 32);
    assert!(!row.3);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&row.4).unwrap(),
        input
    );
    cleanup(&pool, task_name, &schedule.name).await;
}

/// A fired slot stores the task's options text and the retry limit it
/// sets, as a send of the task does (#480). The policy names
/// `TASK_EXCEPTION`, as the Python task does: a policy with no code does
/// not validate as Python `RetryPolicy`, and the send refuses it.
#[tokio::test]
async fn a_scheduled_run_stores_the_timeout_and_retry_policy_of_its_task() {
    let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
    let name = format!("fired_options_{}", Uuid::new_v4().simple());
    let mut schedule = interval_schedule_secs(&name, 60);
    schedule.task_name = name.clone();
    let options = crate::core::TaskOptions {
        task_name: name.clone(),
        queue: None,
        good_until: None,
        retry_for: Some(vec![crate::core::TaskErrorCode::from(
            crate::core::OperationalErrorCode::TaskException,
        )]),
        retry_policy: Some(crate::core::RetryPolicy::fixed(vec![60, 120], true).unwrap()),
        timeout_ms: Some(5_000),
        execution: crate::core::task::Execution::Isolated,
        cancel_grace_ms: None,
    };
    let options_text = options.stored_options_text();
    schedule.task_options = ScheduleTaskOptions::Encoded {
        json: options_text.clone(),
    };
    let database_now = database_time(&pool).await;
    let slot = database_now - chrono::Duration::seconds(1);
    state::test_rows::seed_state(&pool, &name, slot).await;

    fire_once(&pool, &schedule, slot, database_now)
        .await
        .expect("the fire");
    let (stored, max_retries, queue): (Option<String>, i32, String) = sql::query_as(
        "SELECT task_options, max_retries, queue_name FROM horsies_tasks WHERE task_name = $1",
    )
    .bind(&name)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stored, Some(options_text));
    assert_eq!(max_retries, 2);
    assert_eq!(queue, "default");
    cleanup(&pool, &name, &name).await;
}

/// The shared-async and shared-thread cases (the isolated case is the test
/// above): a fired slot stores the options text of its task and the
/// task's execution class, and a shared async task keeps its cancel grace.
#[tokio::test]
async fn a_scheduled_run_stores_the_execution_and_cancel_grace_of_its_task() {
    let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
    for (execution, cancel_grace_ms, stored_execution) in [
        (
            crate::core::task::Execution::SharedAsync,
            Some(2_000),
            "SHARED_ASYNC",
        ),
        (
            crate::core::task::Execution::SharedThread,
            None,
            "SHARED_THREAD",
        ),
    ] {
        let name = format!("fired_execution_{}", Uuid::new_v4().simple());
        let mut schedule = interval_schedule_secs(&name, 60);
        schedule.task_name = name.clone();
        let options = crate::core::TaskOptions {
            task_name: name.clone(),
            queue: None,
            good_until: None,
            retry_for: None,
            retry_policy: None,
            timeout_ms: Some(5_000),
            execution,
            cancel_grace_ms,
        };
        let options_text = options.stored_options_text();
        schedule.task_options = ScheduleTaskOptions::Encoded {
            json: options_text.clone(),
        };
        let database_now = database_time(&pool).await;
        let slot = database_now - chrono::Duration::seconds(1);
        state::test_rows::seed_state(&pool, &name, slot).await;

        fire_once(&pool, &schedule, slot, database_now)
            .await
            .expect("the fire");
        let (stored, stored_class): (Option<String>, String) =
            sql::query_as("SELECT task_options, execution FROM horsies_tasks WHERE task_name = $1")
                .bind(&name)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(stored, Some(options_text), "{stored_execution}");
        assert_eq!(stored_class, stored_execution);
        cleanup(&pool, &name, &name).await;
    }
}

/// A late tick still within one period fires the stored slot and advances
/// one period from it (slot-anchored, not database-clock; horsies PR #46).
/// No skip occurs because the following slot is future.
#[tokio::test]
async fn a_single_late_tick_fires_the_stored_slot_and_anchors_the_next_run_to_it() {
    let pool = test_pool().await;
    let name = "pr46_slot_anchor";
    let task_name = "pr46_anchor_task";
    cleanup(&pool, task_name, name).await;
    let mut schedule = interval_schedule_secs(name, 5);
    schedule.task_name = task_name.to_owned();

    // A due slot at 12:00:00; the due read ran late at 12:00:03, within one
    // 5 s period, so the following slot (12:00:05) is future.
    let due_slot = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    state::test_rows::seed_state(&pool, name, due_slot).await;
    let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 3).unwrap();
    fire_once(&pool, &schedule, due_slot, now)
        .await
        .expect("the fire");

    let row = state::test_rows::state_row(&pool, name).await.unwrap();
    assert_eq!(
        row.next_run_at,
        Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 5).unwrap(),
        "next_run must anchor to the due slot, not the database time",
    );
    assert_eq!(row.run_count, 1);
    let sent_at: DateTime<Utc> =
        sql::query_scalar("SELECT sent_at FROM horsies_tasks WHERE task_name = $1")
            .bind(task_name)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(sent_at, due_slot, "the fired slot is the due slot");
    cleanup(&pool, task_name, name).await;
}

/// C5: `catch_up_missed = false` with a backlog more than one period deep
/// fires only the latest due slot and stores a next run after the database
/// time of the due read. 5 s interval, stored slot 12:00:00, due read at
/// 12:00:17: missed 12:00:05/10/15, fire 12:00:15, next 12:00:20.
#[tokio::test]
async fn a_backlog_without_catch_up_fires_only_the_latest_due_slot() {
    let pool = test_pool().await;
    let name = "c5_skip_latest";
    let task_name = "c5_skip_task";
    cleanup(&pool, task_name, name).await;
    let mut schedule = interval_schedule_secs(name, 5);
    schedule.task_name = task_name.to_owned();

    let due_slot = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    state::test_rows::seed_state(&pool, name, due_slot).await;
    let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 17).unwrap();
    fire_once(&pool, &schedule, due_slot, now)
        .await
        .expect("the fire");

    let row = state::test_rows::state_row(&pool, name).await.unwrap();
    assert_eq!(
        row.next_run_at,
        Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 20).unwrap(),
        "next_run must skip past the due read to the latest slot's successor",
    );
    assert!(row.next_run_at > now);
    let slots: Vec<DateTime<Utc>> = sql::query_scalar(
        "SELECT sent_at FROM horsies_tasks WHERE task_name = $1 ORDER BY sent_at",
    )
    .bind(task_name)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        slots,
        vec![Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 15).unwrap()],
        "only the latest due slot fires",
    );
    cleanup(&pool, task_name, name).await;
}

#[test]
fn advance_to_latest_due_slot_skips_backlog() {
    // Pure-unit: 5s interval, stored slot 12:00:00, now 12:00:17 → latest
    // due slot 12:00:15, next 12:00:20, skipped 3.
    let schedule = interval_schedule_secs("unit_skip", 5);
    let first_due = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 17).unwrap();
    let (slot, next, skipped) = advance_to_latest_due_slot(&enabled(&schedule), first_due, now);
    assert_eq!(slot, Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 15).unwrap());
    assert_eq!(
        next,
        Some(Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 20).unwrap())
    );
    assert_eq!(skipped, 3);
}

#[test]
fn advance_to_latest_due_slot_no_skip_within_one_period() {
    // The following slot is future → no advance, skipped 0.
    let schedule = interval_schedule_secs("unit_noskip", 5);
    let first_due = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 3).unwrap();
    let (slot, next, skipped) = advance_to_latest_due_slot(&enabled(&schedule), first_due, now);
    assert_eq!(slot, first_due);
    assert_eq!(
        next,
        Some(Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 5).unwrap())
    );
    assert_eq!(skipped, 0);
}

/// 2025-06-01 12:00 UTC plus `second` seconds.
fn noon_plus(second: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap()
        + chrono::Duration::seconds(i64::from(second))
}

/// Python `test_scheduler_service.py::TestAdvanceToLatestDueSlot::test_scan_cap_returns_partial_progress`:
/// the latest-slot scan of one tick stops at its bound and returns its
/// progress with a next run that is still due.
#[test]
fn the_latest_slot_scan_stops_at_its_bound_with_its_progress() {
    let schedule = interval_schedule_secs("deep_backlog_scan", 1);
    let bound = i64::from(MAX_SKIP_SCAN_PER_TICK);
    let first_due = noon_plus(0);
    let now = first_due + chrono::Duration::seconds(2 * bound);

    let (slot, next, skipped) = advance_to_latest_due_slot(&enabled(&schedule), first_due, now);

    assert_eq!(skipped, MAX_SKIP_SCAN_PER_TICK);
    assert_eq!(slot, first_due + chrono::Duration::seconds(bound));
    let next = next.expect("an interval has a next run");
    assert_eq!(next, slot + chrono::Duration::seconds(1));
    assert!(next <= now, "the bound leaves the backlog due");
}

/// Python `test_scheduler_service.py::TestCheckScheduleSkipsMissedRuns::test_deep_backlog_persists_progress_without_firing`:
/// a fire plan at the scan bound sends no slot and moves the row to where
/// the scan stopped.
#[test]
fn a_fire_plan_at_the_scan_bound_sends_no_slot_and_moves_the_row_to_its_progress() {
    let schedule = interval_schedule_secs("deep_backlog_plan", 1);
    let bound = i64::from(MAX_SKIP_SCAN_PER_TICK);
    let first_due = noon_plus(0);
    let now = first_due + chrono::Duration::seconds(2 * bound);

    let plan = fire_plan(&enabled(&schedule), first_due, now).expect("an interval has a next run");

    assert!(plan.slots.is_empty(), "{plan:?}");
    assert_eq!(
        plan.next_run_at,
        first_due + chrono::Duration::seconds(bound + 1)
    );
    assert_eq!(plan.skip_to, plan.next_run_at);
}

/// A catch-up fire sends every due slot, moves the row past the last one,
/// and moves it past the first one when the fire is refused.
#[test]
fn a_catch_up_fire_plan_sends_every_due_slot_and_moves_past_the_last() {
    let mut schedule = interval_schedule_secs("catch_up_plan", 5);
    schedule.catch_up_missed = true;

    let plan = fire_plan(&enabled(&schedule), noon_plus(0), noon_plus(12))
        .expect("an interval has a next run");

    assert_eq!(
        plan,
        FirePlan {
            slots: vec![noon_plus(0), noon_plus(5), noon_plus(10)],
            next_run_at: noon_plus(15),
            skip_to: noon_plus(5),
        }
    );
}

/// Python `test_scheduler_service.py::TestCheckScheduleOutcomes::test_a_catch_up_sends_at_most_max_catch_up_runs_slots`:
/// a catch-up fire sends at most `max_catch_up_runs` slots; the row moves
/// to the slot after the last one sent, so the rest stays due.
#[test]
fn a_catch_up_fire_plan_sends_at_most_max_catch_up_runs_slots() {
    let mut schedule = interval_schedule_secs("capped_catch_up_plan", 5);
    schedule.catch_up_missed = true;
    schedule.max_catch_up_runs = 2;

    let plan = fire_plan(&enabled(&schedule), noon_plus(0), noon_plus(12))
        .expect("an interval has a next run");

    assert_eq!(
        plan,
        FirePlan {
            slots: vec![noon_plus(0), noon_plus(5)],
            next_run_at: noon_plus(10),
            skip_to: noon_plus(5),
        }
    );
}

/// Python `test_scheduler_service.py::TestCheckScheduleOutcomes::test_each_slot_of_an_advanced_fire_is_logged_by_its_outcome`:
/// each slot of an advanced fire is logged with its task id at the level
/// of its outcome.
#[test]
fn each_slot_of_an_advanced_fire_is_logged_by_its_outcome() {
    use crate::test_support::{CapturedEvent, LogCapture};

    let capture = LogCapture::new();
    let _default = capture.set_default();
    let schedule = interval_schedule_secs("logged_fire", 5);
    let slots = [
        (SlotOutcome::SameCommand, Uuid::from_u128(1)),
        (SlotOutcome::ConflictUnverifiable, Uuid::from_u128(2)),
        (SlotOutcome::Inserted, Uuid::from_u128(3)),
        (SlotOutcome::PayloadMismatch, Uuid::from_u128(4)),
    ];
    let fired: Vec<FiredSlot> = slots
        .iter()
        .zip([0, 5, 10, 15])
        .map(|(&(outcome, task_id), second)| FiredSlot {
            slot_at: noon_plus(second),
            task_id,
            outcome,
        })
        .collect();

    log_fired(&enabled(&schedule), &command_for(&schedule), &fired);

    let one_line = |level: tracing::Level, start: &str, task_id: Uuid| {
        let lines: Vec<CapturedEvent> = capture.lines_starting(level, start);
        assert_eq!(lines.len(), 1, "{start}: {lines:?}");
        assert_eq!(lines[0].field("schedule"), Some("logged_fire"));
        assert_eq!(
            lines[0].field("task_id"),
            Some(task_id.to_string().as_str())
        );
        lines[0].clone()
    };
    let _same = one_line(
        tracing::Level::INFO,
        "the slot already has its task; recorded as fired",
        Uuid::from_u128(1),
    );
    let _unverifiable = one_line(
        tracing::Level::ERROR,
        "the slot's task finished while the fire read it",
        Uuid::from_u128(2),
    );
    let inserted = one_line(
        tracing::Level::INFO,
        "scheduled task enqueued",
        Uuid::from_u128(3),
    );
    assert_eq!(inserted.field("task_name"), Some("my_task"));
    let _mismatch = one_line(
        tracing::Level::WARN,
        "the slot's task id already holds a task with a different command",
        Uuid::from_u128(4),
    );
}

/// Python `test_scheduler_service.py::TestInitializeSchedules::test_first_runs_come_from_the_database_time`:
/// the start write of a new schedule computes its first run from the
/// database time of the start read, not from the host clock, and stores the
/// schedule's digest.
#[test]
fn the_start_write_computes_each_first_run_from_the_database_time_of_its_read() {
    let schedule = interval_schedule_secs("first_run_of_the_read", 10);
    let enabled_schedule = enabled(&schedule);
    // A database time far from the host clock of the test.
    let database_now = Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap();
    let first_run = next_run_at(
        &enabled_schedule.pattern,
        database_now,
        &enabled_schedule.timezone,
    )
    .expect("an interval has a next run");

    let writes = start_writes(
        std::slice::from_ref(&enabled_schedule),
        &HashMap::new(),
        database_now,
    );

    assert!(database_now < first_run);
    assert!(first_run <= database_now + chrono::Duration::seconds(10));
    assert_eq!(
        writes,
        vec![state::ScheduleStateWrite {
            schedule_name: "first_run_of_the_read".to_owned(),
            next_run_at: first_run,
            config_hash: compute_config_hash(&enabled_schedule).to_vec(),
        }]
    );
}

/// Python `test_scheduler_service.py::TestInitializeSchedulesAdditionalPaths::test_config_hash_changed_recalculates_next_run`
/// and `::test_unchanged_digest_is_not_written`: the start write holds a
/// schedule whose stored digest differs, with its digest and its first run
/// after the database time of the read, and leaves out a schedule whose
/// stored digest is its own.
#[test]
fn the_start_write_holds_the_changed_digests_and_not_the_unchanged_ones() {
    let changed = enabled(&interval_schedule_secs("digest_changed", 10));
    let unchanged = enabled(&interval_schedule_secs("digest_unchanged", 10));
    let stale_digest = [0_u8; 32];
    let own_digest = compute_config_hash(&unchanged);
    let stored: HashMap<&str, &[u8]> = HashMap::from([
        ("digest_changed", stale_digest.as_slice()),
        ("digest_unchanged", own_digest.as_slice()),
    ]);
    let database_now = Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap();

    let writes = start_writes(&[changed.clone(), unchanged], &stored, database_now);

    assert_eq!(
        writes,
        vec![state::ScheduleStateWrite {
            schedule_name: "digest_changed".to_owned(),
            next_run_at: next_run_at(&changed.pattern, database_now, &changed.timezone)
                .expect("an interval has a next run"),
            config_hash: compute_config_hash(&changed).to_vec(),
        }]
    );
    assert_ne!(writes[0].config_hash, stale_digest.to_vec());
}

/// The send command of a schedule is the command of a send of the same
/// texts, and its fingerprint covers the options text: the same schedule
/// with no options has another fingerprint.
#[test]
fn the_command_of_a_schedule_is_the_send_command_of_its_texts() {
    let options_text = crate::core::TaskOptions {
        task_name: "my_task".to_owned(),
        queue: None,
        good_until: None,
        retry_for: None,
        retry_policy: None,
        timeout_ms: Some(30_000),
        execution: crate::core::task::Execution::Isolated,
        cancel_grace_ms: None,
    }
    .stored_options_text();
    let mut with_options = interval_schedule_secs("fingerprinted", 60);
    with_options.input_json = json!({"value": 5}).to_string();
    with_options.task_options = ScheduleTaskOptions::Encoded {
        json: options_text.clone(),
    };
    let mut without_options = with_options.clone();
    without_options.task_options = ScheduleTaskOptions::NoOptions;

    let command = command_for(&with_options);

    let expected = SendCommand::prepare(
        "my_task",
        "default",
        100,
        &with_options.input_json,
        Some(options_text.as_str()),
        crate::core::EnqueueRetention::Forever,
        false,
    )
    .expect("the send command of the texts builds");
    assert_eq!(command, expected);
    assert_eq!(command.stored_input, "{\"value\":5}");
    assert_eq!(command.task_options.as_deref(), Some(options_text.as_str()));
    assert_ne!(
        command.fingerprint,
        command_for(&without_options).fingerprint
    );
}

/// The self-heal creates a state row for an enabled schedule that has none
/// (with a strictly-future next_run), leaves an existing row's next_run
/// untouched, and skips disabled schedules. Parity with horsies PR #123.
#[tokio::test]
async fn ensure_states_exist_heals_missing_and_preserves_existing() {
    let pool = test_pool().await;
    let core = core_pool_of(&pool);
    let missing = format!("qw123_missing_{}", Uuid::new_v4());
    let disabled = format!("qw123_disabled_{}", Uuid::new_v4());

    let schedules = active_plan(manifest_of_entries(&[
        interval_schedule_secs(&missing, 60).entry(),
        disabled_entry(&disabled),
    ]))
    .schedules;

    let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    let healthy = ensure_states_exist(&core, StatementLog::Off, &schedules, now).await;
    assert!(healthy, "a pass that heals its missing row counts healthy");

    let healed = state::test_rows::state_row(&pool, &missing).await.unwrap();
    assert!(
        healed.next_run_at > now,
        "healed next_run must be strictly future"
    );
    // The first run after the database time, the schedule's digest, no run.
    assert_eq!(
        healed.next_run_at,
        next_run_at(&schedules[0].pattern, now, &schedules[0].timezone)
            .expect("an interval has a next run")
    );
    assert_eq!(
        healed.config_hash,
        compute_config_hash(&schedules[0]).to_vec()
    );
    assert_eq!(
        (healed.last_run_at, healed.last_task_id, healed.run_count),
        (None, None, 0)
    );
    assert!(state::test_rows::state_row(&pool, &disabled)
        .await
        .is_none());

    // A later tick leaves the existing row's next_run untouched.
    let later = now + chrono::Duration::seconds(30);
    let healthy = ensure_states_exist(&core, StatementLog::Off, &schedules, later).await;
    assert!(healthy, "all rows present is a healthy pass");
    let after = state::test_rows::state_row(&pool, &missing).await.unwrap();
    assert_eq!(
        after.next_run_at, healed.next_run_at,
        "existing next_run must be preserved across ticks",
    );

    core.close();
    for name in [missing, disabled] {
        state::test_rows::delete_state(&pool, &name).await;
    }
}

/// The start computes a new schedule's first run from the database time
/// of its read, and stores the schedule's digest.
#[tokio::test]
async fn initialize_schedules_computes_first_runs_from_the_database_time() {
    let pool = test_pool().await;
    let core = core_pool_of(&pool);
    let name = format!("qw728_start_{}", Uuid::new_v4());
    let schedule = interval_schedule_secs(&name, 60);
    let plan = active_plan(manifest_for(std::slice::from_ref(&schedule)));

    let before = database_time(&pool).await;
    initialize_schedules(
        &core,
        StatementLog::Off,
        &plan.configured_names,
        &plan.schedules,
    )
    .await
    .expect("every pattern has a next slot");
    let after = database_time(&pool).await;

    let row = state::test_rows::state_row(&pool, &name).await.unwrap();
    assert!(row.next_run_at > before);
    assert!(row.next_run_at <= after + chrono::Duration::seconds(60));
    assert_eq!(
        row.config_hash,
        compute_config_hash(&enabled(&schedule)).to_vec()
    );
    assert_eq!(row.run_count, 0);
    // The start writes no last run.
    assert_eq!((row.last_run_at, row.last_task_id), (None, None));

    core.close();
    state::test_rows::delete_state(&pool, &name).await;
}

#[test]
fn existence_cadence_ticks_derived_from_check_interval() {
    for (seconds, expected_ticks) in [(1, 60), (10, 6), (60, 1), (120, 1)] {
        let cadence = ExistenceCheckCadence::new(seconds);
        assert_eq!(
            cadence.interval_ticks, expected_ticks,
            "check_interval_seconds={seconds}",
        );
    }
}

/// While healthy, only the first tick checks; subsequent ticks inside the
/// interval skip, and the check runs again once the interval elapses.
#[test]
fn existence_cadence_healthy_skips_until_interval_elapses() {
    let mut cadence = ExistenceCheckCadence::new(10); // 6 ticks
    let mut checks = 0;
    for _ in 0..7 {
        if cadence.should_check() {
            checks += 1;
            cadence.record(true);
        }
        cadence.tick();
    }
    assert_eq!(checks, 2, "tick 0 and tick 6 check; ticks 1-5 skip");
}

/// An unhealthy pass re-checks every tick until a healthy one, then the
/// cadence resumes.
#[test]
fn existence_cadence_unhealthy_rechecks_every_tick_until_healthy() {
    let mut cadence = ExistenceCheckCadence::new(10); // 6 ticks
    let health = [false, false, true]; // two failed passes, then healthy
    let mut checks = 0;
    for tick in 0..6 {
        if cadence.should_check() {
            cadence.record(health[checks.min(2)]);
            checks += 1;
            assert!(tick <= 2, "checks must be consecutive while unhealthy");
        }
        cadence.tick();
    }
    assert_eq!(
        checks, 3,
        "ticks 0/1/2 check (unhealthy, unhealthy, healthy); 3-5 skip",
    );
}

#[cfg(test)]
mod fire_tests;

#[cfg(test)]
mod manifest_tests;

#[cfg(test)]
mod run_loop_tests;
