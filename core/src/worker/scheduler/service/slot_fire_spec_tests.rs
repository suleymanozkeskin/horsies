//! A slot that an earlier version enqueued counts as fired.
//!
//! A slot's deterministic task id already holds a row whose fingerprint
//! differs (an earlier version, or a change of the task's options or the
//! schedule's input since). The fire answers `PAYLOAD_MISMATCH` for the
//! slot; the scheduler keeps that row, records the slot as run and moves to
//! the next slot (decision "Schedule slot whose fingerprint differs"). The
//! test enters at `fire_due_schedule`, the tick's fire of one due schedule.

use chrono::{DateTime, Utc};
use serde_json::json;
use uuid::Uuid;

use super::state::test_rows::{delete_state, seed_state, state_row};
use super::{fire_due_schedule, manifest_plan, ActivePlan, DueSchedule, StartPlan};
use crate::broker::core_rows::StatementLog;
use crate::core::{OperationalCode, RetryPolicy, TaskErrorCode, TaskOptions};
use crate::scheduler_manifest::parse_scheduler_manifest;
use crate::test_support::sql::{self, TestPool};

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
async fn seed_earlier_slot_row(
    pool: &TestPool,
    task_id: Uuid,
    task_name: &str,
    slot: DateTime<Utc>,
) {
    sql::query(
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

/// The start plan of a manifest of one schedule every
/// [`SCHEDULE_INTERVAL_SECONDS`]: the default queue, the input
/// `{"value":1}`, and the options text of a task that declares a timeout and
/// a retry policy (Python `_app_with_scheduled_task`). The earlier row
/// stores no options.
fn schedule_plan(schedule_name: &str, task_name: &str) -> ActivePlan {
    let options = TaskOptions {
        task_name: task_name.to_owned(),
        queue_name: None,
        good_until: None,
        auto_retry_for: Some(vec![TaskErrorCode::from(OperationalCode::TaskException)]),
        retry_policy: Some(
            RetryPolicy::fixed(RETRY_INTERVALS_SECONDS.to_vec(), true)
                .expect("two intervals inside the bounds"),
        ),
        timeout_ms: Some(TASK_TIMEOUT_MS),
        execution: crate::core::task::Execution::Isolated,
        cancel_grace_ms: None,
    };
    let pattern_json = format!(
        "{{\"type\":\"interval\",\"seconds\":{SCHEDULE_INTERVAL_SECONDS},\"minutes\":null,\
         \"hours\":null,\"days\":null}}"
    );
    let document = json!({
        "manifest_version": 1,
        "enabled": true,
        "check_interval_seconds": 1,
        "schedules": [{
            "name": schedule_name,
            "activation": {
                "kind": "ENABLED",
                "pattern_json": pattern_json,
                "timezone": "UTC",
                "catch_up_missed": false,
                "max_catch_up_runs": 100,
                "send": {
                    "task_name": task_name,
                    "queue_name": "default",
                    "priority": 100,
                    "input_json": "{\"value\":1}",
                    "task_options": {"kind": "ENCODED", "json": options.stored_options_text()},
                    "retention_class_key": "forever",
                    "retain_rerun_input": false
                }
            }
        }]
    });
    let manifest = parse_scheduler_manifest(&document.to_string())
        .unwrap_or_else(|refused| panic!("the test manifest is complete: {refused}"));
    match manifest_plan(manifest) {
        Ok(StartPlan::Active(plan)) => plan,
        Ok(StartPlan::Idle) => panic!("a manifest with an enabled schedule starts active"),
        Err(refused) => panic!("the start plan of the test manifest is refused: {refused}"),
    }
}

#[tokio::test]
async fn a_slot_that_an_earlier_version_enqueued_counts_as_fired() {
    let pool = crate::test_support::migrated_pool().await;
    let suffix = Uuid::new_v4().simple().to_string();
    let task_name = format!("scheduled_options_task_{}", &suffix[..12]);
    let schedule_name = format!("scheduled_options_{}", &suffix[..12]);
    let check_time: DateTime<Utc> =
        sql::query_scalar("SELECT date_trunc('second', statement_timestamp())")
            .fetch_one(&pool)
            .await
            .expect("read the database time");
    let first_due = check_time - chrono::Duration::seconds(1);
    let next_run = first_due + chrono::Duration::seconds(i64::from(SCHEDULE_INTERVAL_SECONDS));
    let slot_task_id: Uuid = sql::query_scalar("SELECT horsies_schedule_slot_task_id($1, $2)")
        .bind(&schedule_name)
        .bind(first_due)
        .fetch_one(&pool)
        .await
        .expect("the slot time has a slot id");
    seed_earlier_slot_row(&pool, slot_task_id, &task_name, first_due).await;
    seed_state(&pool, &schedule_name, first_due).await;
    let mut plan = schedule_plan(&schedule_name, &task_name);
    let command = plan
        .commands
        .remove(&schedule_name)
        .expect("the schedule has its command");
    let schedule = plan
        .schedules
        .pop()
        .expect("the plan holds the enabled schedule");
    let core = crate::broker::pools::core_pool_for_test_url(pool.url());

    fire_due_schedule(
        &core,
        StatementLog::Off,
        &schedule,
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

    let rows: Vec<(Uuid, Option<String>, Vec<u8>)> = sql::query_as(
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

    sql::query("DELETE FROM horsies_tasks WHERE id = $1")
        .bind(slot_task_id)
        .execute(&pool)
        .await
        .expect("delete the slot row");
    delete_state(&pool, &schedule_name).await;
}
