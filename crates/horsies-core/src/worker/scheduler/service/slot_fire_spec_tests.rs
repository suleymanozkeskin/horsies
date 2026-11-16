//! Port of Python
//! `tests/integration/test_scheduler_task_options.py::test_a_slot_that_an_earlier_version_enqueued_counts_as_fired`.
//!
//! A slot's deterministic task id already holds a row whose fingerprint
//! differs (an earlier version, or a change of the task's options or the
//! schedule's input since). The broker answers `PAYLOAD_MISMATCH`; the
//! scheduler keeps that row, records the slot as run and moves to the next
//! slot (decision "Schedule slot whose fingerprint differs"). The test enters
//! at `process_schedule`, the tick's caller of the enqueue.

use std::sync::Arc;

use chrono::{DateTime, Timelike, Utc};
use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::{process_schedule, schedule_slot_task_id, state};
use crate::broker::PostgresBroker;
use crate::core::config::{AppConfig, IntervalSchedule, SchedulePattern, TaskSchedule};

const SCHEDULE_INTERVAL_SECONDS: u32 = 60;

/// The fingerprint byte of the earlier row: no command of this schedule
/// hashes to 32 equal bytes.
const EARLIER_FINGERPRINT_BYTE: u8 = 0xEE;

/// The row an earlier version wrote for the slot: the same task, queue and
/// sent_at, another fingerprint, no stored options.
async fn seed_earlier_slot_row(pool: &PgPool, task_id: Uuid, task_name: &str, slot: DateTime<Utc>) {
    sqlx::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, task_options, execution,
            retention_class_key, sent_at, created_at,
            command_fingerprint_version, command_fingerprint, idempotency_key_digest,
            status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
            started_at, finalizing_at, finalizing_by_worker_id,
            worker_pid, worker_hostname, worker_process_name,
            retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
            updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
         ) VALUES (
            $1, $2, 'default', 100, '{\"value\":1}', NULL, 'ISOLATED',
            'forever', $3, $3,
            1, $4, NULL,
            'PENDING', $3, NULL, NULL, NULL,
            NULL, NULL, NULL,
            NULL, NULL, NULL,
            0, 0, NULL, NULL, FALSE,
            NOW(), FALSE, NULL, NULL
         )",
    )
    .bind(task_id)
    .bind(task_name)
    .bind(slot)
    .bind(vec![EARLIER_FINGERPRINT_BYTE; 32])
    .execute(pool)
    .await
    .expect("seed the earlier slot row");
}

#[tokio::test]
#[serial]
async fn a_slot_that_an_earlier_version_enqueued_counts_as_fired() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
    let suffix = Uuid::new_v4().simple().to_string();
    let task_name = format!("scheduled_options_task_{}", &suffix[..12]);
    let schedule_name = format!("scheduled_options_{}", &suffix[..12]);
    let schedule = TaskSchedule {
        name: schedule_name.clone(),
        task_name: task_name.clone(),
        pattern: SchedulePattern::Interval(IntervalSchedule {
            seconds: Some(SCHEDULE_INTERVAL_SECONDS),
            minutes: None,
            hours: None,
            days: None,
        }),
        input: serde_json::json!({"value": 1}),
        queue_name: None,
        enabled: true,
        timezone: "UTC".to_owned(),
        catch_up_missed: false,
        max_catch_up_runs: 100,
    };
    let check_time = Utc::now()
        .with_nanosecond(0)
        .expect("a whole second is a valid time");
    let first_due = check_time - chrono::Duration::seconds(1);
    let next_run = first_due + chrono::Duration::seconds(i64::from(SCHEDULE_INTERVAL_SECONDS));
    let slot_task_id =
        schedule_slot_task_id(&schedule_name, first_due).expect("the slot time has a slot id");
    seed_earlier_slot_row(&pool, slot_task_id, &task_name, first_due).await;
    state::seed_state(&pool, &schedule_name, first_due)
        .await
        .expect("seed the schedule state");

    process_schedule(
        &broker,
        &schedule,
        check_time,
        1,
        &AppConfig::for_database_url("postgresql://unused/test"),
    )
    .await
    .expect("the tick");

    let rows: Vec<(Uuid, Option<String>, Vec<u8>)> = sqlx::query_as(
        "SELECT id, task_options, command_fingerprint FROM horsies_tasks WHERE task_name = $1",
    )
    .bind(&task_name)
    .fetch_all(&pool)
    .await
    .expect("read the task rows");
    assert_eq!(
        rows,
        vec![(slot_task_id, None, vec![EARLIER_FINGERPRINT_BYTE; 32])],
        "the earlier row stays, and no second row is written"
    );
    let recorded = state::get_state(&pool, &schedule_name)
        .await
        .expect("read the schedule state")
        .expect("the state row exists");
    assert_eq!(recorded.last_task_id, Some(slot_task_id));
    assert_eq!(recorded.next_run_at, next_run);
    assert_eq!(recorded.run_count, 1);

    sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
        .bind(slot_task_id)
        .execute(&pool)
        .await
        .expect("delete the slot row");
    let _deleted = state::delete_state(&pool, &schedule_name)
        .await
        .expect("delete the schedule state");
}

/// Decided form of the slot id (rule-following row 16): a pure function of
/// the schedule name and the slot time, with the UUIDv7 layout whose 48 time
/// bits are the slot time in milliseconds, so the history reads can prune by
/// birth time.
#[test]
fn the_slot_task_id_has_the_uuid7_layout_of_its_slot_time() {
    let slot = Utc::now()
        .with_nanosecond(0)
        .expect("a whole second is a valid time");
    let id = schedule_slot_task_id("nightly-reindex", slot).expect("the slot time has a slot id");

    assert_eq!(Ok(id), schedule_slot_task_id("nightly-reindex", slot));
    assert_eq!(id.get_version_num(), 7);
    assert_eq!(id.get_variant(), uuid::Variant::RFC4122);
    let bytes = id.as_bytes();
    let birth_ms = bytes[..6]
        .iter()
        .fold(0_i64, |millis, byte| (millis << 8) | i64::from(*byte));
    assert_eq!(birth_ms, slot.timestamp_millis());
    assert_ne!(Ok(id), schedule_slot_task_id("other-schedule", slot));
}
