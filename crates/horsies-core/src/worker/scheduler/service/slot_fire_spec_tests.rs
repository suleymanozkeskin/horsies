//! Port of Python
//! `tests/integration/test_scheduler_task_options.py::test_a_slot_that_an_earlier_version_enqueued_counts_as_fired`.
//!
//! A slot's deterministic task id already holds a row whose fingerprint
//! differs (an earlier version, or a change of the task's options or the
//! schedule's input since). The fire answers `PAYLOAD_MISMATCH` for the
//! slot; the scheduler keeps that row, records the slot as run and moves to
//! the next slot (decision "Schedule slot whose fingerprint differs"). The
//! test enters at `fire_due_schedule`, the tick's fire of one due schedule.

use chrono::{DateTime, Utc};
use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::state::test_rows::{delete_state, seed_state, state_row};
use super::{
    build_commands, fire_due_schedule, DueSchedule, EnabledSchedule, PreparedSchedules,
    ScheduleSendFacts,
};
use crate::broker::core_rows::StatementLog;
use crate::broker::PostgresBroker;
use crate::core::config::{
    AppConfig, IntervalSchedule, ScheduleConfig, SchedulePattern, TaskSchedule,
};
use crate::core::{OperationalErrorCode, RetryPolicy, TaskErrorCode, TaskOptions};

const SCHEDULE_INTERVAL_SECONDS: u32 = 60;

/// The timeout that the scheduled task declares (Python `TASK_TIMEOUT_MS`).
const TASK_TIMEOUT_MS: u32 = 30_000;

/// The fixed retry intervals that the scheduled task declares (Python
/// `RETRY_INTERVALS_SECONDS`).
const RETRY_INTERVALS_SECONDS: [u32; 2] = [1, 2];

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

/// The send facts of the schedule: the default queue and the options text of
/// a task that declares a timeout and a retry policy (Python
/// `_app_with_scheduled_task`). The earlier row stores no options.
fn prepared_schedules(schedule_name: &str, task_name: &str) -> PreparedSchedules {
    let options = TaskOptions {
        task_name: task_name.to_owned(),
        queue_name: None,
        good_until: None,
        auto_retry_for: Some(vec![TaskErrorCode::from(
            OperationalErrorCode::TaskException,
        )]),
        retry_policy: Some(
            RetryPolicy::fixed(RETRY_INTERVALS_SECONDS.to_vec(), true)
                .expect("two intervals inside the bounds"),
        ),
        timeout_ms: Some(TASK_TIMEOUT_MS),
        execution: crate::core::task::Execution::Isolated,
        cancel_grace_ms: None,
    };
    PreparedSchedules::from([(
        schedule_name.to_owned(),
        ScheduleSendFacts {
            queue_name: "default".to_owned(),
            task_options: Some(options.stored_options_text()),
        },
    )])
}

#[tokio::test]
#[serial]
async fn a_slot_that_an_earlier_version_enqueued_counts_as_fired() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let broker = PostgresBroker::from_pool(pool.clone());
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
    let check_time: DateTime<Utc> =
        sqlx::query_scalar("SELECT date_trunc('second', statement_timestamp())")
            .fetch_one(&pool)
            .await
            .expect("read the database time");
    let first_due = check_time - chrono::Duration::seconds(1);
    let next_run = first_due + chrono::Duration::seconds(i64::from(SCHEDULE_INTERVAL_SECONDS));
    let slot_task_id: Uuid = sqlx::query_scalar("SELECT horsies_schedule_slot_task_id($1, $2)")
        .bind(&schedule_name)
        .bind(first_due)
        .fetch_one(&pool)
        .await
        .expect("the slot time has a slot id");
    seed_earlier_slot_row(&pool, slot_task_id, &task_name, first_due).await;
    seed_state(&pool, &schedule_name, first_due).await;
    let mut commands = build_commands(
        &broker,
        &ScheduleConfig::new(vec![schedule.clone()]),
        &AppConfig::for_database_url("postgresql://unused/test"),
        &prepared_schedules(&schedule_name, &task_name),
    )
    .expect("the send command builds");
    let command = commands
        .remove(&schedule_name)
        .expect("the schedule has its command");
    let core = crate::broker::pools::core_pool_for_test_pool(&pool);

    fire_due_schedule(
        &core,
        StatementLog::Off,
        &EnabledSchedule::of(&schedule).expect("the schedule is enabled"),
        &command,
        &DueSchedule {
            schedule_name: schedule_name.clone(),
            next_run_at: first_due,
        },
        check_time,
    )
    .await
    .expect("the fire");
    core.close();

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
    let recorded = state_row(&pool, &schedule_name)
        .await
        .expect("the state row exists");
    assert_eq!(recorded.last_task_id, Some(slot_task_id));
    assert_eq!(recorded.next_run_at, next_run);
    assert_eq!(recorded.run_count, 1);

    sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
        .bind(slot_task_id)
        .execute(&pool)
        .await
        .expect("delete the slot row");
    delete_state(&pool, &schedule_name).await;
}
