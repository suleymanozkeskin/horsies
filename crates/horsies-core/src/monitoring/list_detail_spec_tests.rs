//! The task list, task detail, span rules, read failures and list totals.
//!
//! Python specification: `tests/integration/test_monitoring_queries.py`
//! (`TestListTasks`, `TestGetTaskDetail`, `TestTaskDurations`,
//! `TestDatabaseFailure`, `TestUnfilteredHistoryTotalContract`), baseline
//! version on `program/0.8.0`. Each database test runs on a database of its
//! own ([`SpecDatabase`]).

use chrono::{DateTime, Duration, Utc};
use serial_test::serial;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use super::spec_support::{
    seed_live, seed_node, seed_run, seed_terminal, set_history_status, set_history_times, window,
    Ending, LiveTask, NodeKind, NodeSeed, RunSeed, SpecDatabase,
};
use crate::broker::PostgresBroker;
use crate::core::history::reads::aggregates::plan_rows_from_explain_text;
use crate::core::lifecycle::{BatchSize, TerminalizationCommand, TerminalizationOutcome};
use crate::core::task::Execution;
use crate::monitoring::{
    get_task_detail, list_tasks, task_stats, MonitoringQueryError, MonitoringQueryErrorCode,
    SortDirection, TaskAbsenceReason, TaskDetail, TaskDetailNotFound, TaskDetailRead, TaskListPage,
    TaskListQuery, TaskSortField, TaskStatsQuery,
};

/// Upper bound of a span that counts up to now (Python
/// `_LIVE_SPAN_CEILING_S`): wide enough for a slow runner, tight enough to
/// catch a span measured from the wrong start.
const LIVE_SPAN_CEILING_S: i64 = 600;

/// A broker on a server that does not answer (Python's `unreachable_broker`).
fn unreachable_broker() -> PostgresBroker {
    let pool = PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(250))
        .connect_lazy("postgresql://postgres:none@127.0.0.1:1/none")
        .expect("a lazy pool on an unreachable server");
    PostgresBroker::from_pool(pool)
}

async fn page(broker: &PostgresBroker, query: TaskListQuery) -> TaskListPage {
    list_tasks(broker, &query).await.expect("list the tasks")
}

fn sorted(field: TaskSortField, direction: SortDirection) -> TaskListQuery {
    TaskListQuery::new(window()).with_sort(field, direction)
}

fn names_of(page: &TaskListPage) -> Vec<&str> {
    page.rows.iter().map(|row| row.task_name.as_str()).collect()
}

async fn detail_of(broker: &PostgresBroker, task_id: Uuid) -> TaskDetail {
    match get_task_detail(broker, task_id)
        .await
        .expect("read the task detail")
    {
        TaskDetailRead::Found(detail) => detail,
        TaskDetailRead::NotFound(absent) => panic!("the task has no detail: {absent:?}"),
    }
}

/// The detail read of a task id that no table holds: its absence reason.
async fn absence_of(broker: &PostgresBroker, task_id: Uuid) -> TaskDetailRead {
    get_task_detail(broker, task_id)
        .await
        .expect("read the task detail")
}

fn not_found(task_id: Uuid, reason: TaskAbsenceReason) -> TaskDetailRead {
    TaskDetailRead::NotFound(TaskDetailNotFound { task_id, reason })
}

/// A UUIDv7 whose embedded birth is `birth`, with random tail bits (Python
/// `_uuid7_born_at`).
fn uuid7_born_at(birth: DateTime<Utc>) -> Uuid {
    let milliseconds =
        u128::try_from(birth.timestamp_millis()).expect("the birth is after the Unix epoch");
    let tail = Uuid::new_v4().as_u128() & ((1_u128 << 80) - 1);
    let value = (milliseconds << 80) | tail;
    let value = (value & !(0xF_u128 << 76)) | (0x7_u128 << 76);
    let value = (value & !(0x3_u128 << 62)) | (0x2_u128 << 62);
    Uuid::from_u128(value)
}

/// The catalog row of a detached and dropped leaf of the default class
/// (`standard_30d`) whose upper anchor is `upper_anchor` (Python
/// `_detached_period_leaf`). Only the absence rule reads it: it names no
/// relation. The database of the test is dropped with it.
async fn record_detached_period_leaf(pool: &PgPool, upper_anchor: DateTime<Utc>) {
    sqlx::query(
        "INSERT INTO horsies_task_history_leaf_catalog (
             leaf_name, class_key, lower_bound_kind, lower_anchor, upper_anchor,
             index_schema_version, created_at, detached_at, dropped_at
         ) VALUES ($1, 'standard_30d', 'AT', $2, $3, 1, now(), now(), now())",
    )
    .bind(format!("spec_detached_{}", Uuid::new_v4().simple()))
    .bind(upper_anchor - Duration::days(1))
    .bind(upper_anchor)
    .execute(pool)
    .await
    .expect("record a detached period leaf");
}

/// The spans of the only row of the list (Python `_only_row`).
async fn only_spans(broker: &PostgresBroker) -> (Option<i64>, Option<i64>) {
    let page = page(
        broker,
        TaskListQuery::new(window())
            .with_pagination(0, 1)
            .expect("a page of one row is accepted"),
    )
    .await;
    let row = page.rows.first().expect("the list has a row");
    (row.queue_s, row.exec_s)
}

/// A completed task that started 40 s ago and ended 30 s ago.
async fn seed_finished(database: &SpecDatabase, task_name: &str) {
    let finished = LiveTask::pending(task_name);
    seed_terminal(&database.pool, &finished, Ending::Completed).await;
    set_history_times(&database.pool, finished.id, 60, Some(40), 30).await;
}

/// Python `TestListTasks::test_sort_direction`, both directions.
#[tokio::test]
#[serial]
async fn the_list_sorts_by_enqueue_time_in_both_directions() {
    let database = SpecDatabase::create().await;
    for (task_name, enqueued_seconds_ago) in [("first", 30), ("second", 20), ("third", 10)] {
        let mut task = LiveTask::pending(task_name);
        task.enqueued_seconds_ago = enqueued_seconds_ago;
        seed_live(&database.pool, &task).await;
    }
    let broker = database.broker();

    let ascending = page(
        &broker,
        sorted(TaskSortField::EnqueuedAt, SortDirection::Ascending),
    )
    .await;
    let descending = page(
        &broker,
        sorted(TaskSortField::EnqueuedAt, SortDirection::Descending),
    )
    .await;

    assert_eq!(names_of(&ascending), ["first", "second", "third"]);
    assert_eq!(names_of(&descending), ["third", "second", "first"]);
    database.drop().await;
}

/// Every sort key of the list reads a live row and a history row in both
/// directions: the function takes each key of `TaskSortField::ALL` with
/// `ASC` and with `DESC`.
#[tokio::test]
#[serial]
async fn every_sort_key_reads_both_sides_in_both_directions() {
    let database = SpecDatabase::create().await;
    seed_live(&database.pool, &LiveTask::running("live_task", "worker-1")).await;
    seed_terminal(
        &database.pool,
        &LiveTask::pending("history_task"),
        Ending::failed("BOOM"),
    )
    .await;
    let broker = database.broker();

    for field in TaskSortField::ALL {
        for direction in SortDirection::ALL {
            let listed = page(&broker, sorted(field, direction)).await;

            let mut names = names_of(&listed);
            names.sort_unstable();
            assert_eq!(
                names,
                ["history_task", "live_task"],
                "{field:?} {direction:?}"
            );
        }
    }
    database.drop().await;
}

/// Python `TestListTasks::test_live_rows_sort_as_null_on_derived_spans`: a
/// running row shows a growing `exec_s` but has no span to sort on, so it
/// sorts after a finished row.
#[tokio::test]
#[serial]
async fn a_running_row_sorts_as_null_on_the_execution_span() {
    let database = SpecDatabase::create().await;
    let mut running = LiveTask::running("running_now", "worker-1");
    running.started_seconds_ago = Some(20);
    seed_live(&database.pool, &running).await;
    seed_finished(&database, "finished").await;

    let page = page(
        &database.broker(),
        sorted(TaskSortField::ExecutionSeconds, SortDirection::Descending),
    )
    .await;

    assert_eq!(names_of(&page), ["finished", "running_now"]);
    database.drop().await;
}

/// Python `TestListTasks::test_null_sort_keys_come_last_in_both_directions`,
/// for a live row: a live row has no end time, so it sorts after every
/// history row on `completed_at` in both directions.
#[tokio::test]
#[serial]
async fn a_live_row_sorts_after_a_finished_row_on_the_end_time_in_both_directions() {
    let database = SpecDatabase::create().await;
    seed_live(&database.pool, &LiveTask::pending("never_ran")).await;
    seed_finished(&database, "finished").await;
    let broker = database.broker();

    for direction in SortDirection::ALL {
        let page = page(&broker, sorted(TaskSortField::CompletedAt, direction)).await;

        assert_eq!(names_of(&page), ["finished", "never_ran"], "{direction:?}");
    }
    database.drop().await;
}

/// Python `TestListTasks::test_rows_carry_derived_error_category_and_worker_id`.
#[tokio::test]
#[serial]
async fn a_list_row_carries_its_category_worker_and_host() {
    let database = SpecDatabase::create().await;
    let mut task = LiveTask::pending("alpha_task").with_worker(Some("worker-7"));
    task.worker_hostname = Some("host-7".to_owned());
    seed_terminal(&database.pool, &task, Ending::failed("TASK_EXCEPTION")).await;

    let page = page(&database.broker(), TaskListQuery::new(window())).await;

    let row = page.rows.first().expect("the list has the row");
    assert_eq!(row.error_category.as_deref(), Some("OPERATIONAL"));
    assert_eq!(row.worker_id.as_deref(), Some("worker-7"));
    assert_eq!(row.worker_hostname.as_deref(), Some("host-7"));
    database.drop().await;
}

/// Python `TestListTasks::test_empty_error_code_is_reported_as_absent`.
#[tokio::test]
#[serial]
async fn an_empty_stored_code_reads_as_no_code_and_no_category() {
    let database = SpecDatabase::create().await;
    seed_terminal(
        &database.pool,
        &LiveTask::pending("alpha_task"),
        Ending::failed(""),
    )
    .await;

    let page = page(&database.broker(), TaskListQuery::new(window())).await;

    let row = page.rows.first().expect("the list has the row");
    assert_eq!(row.error_code, None);
    assert_eq!(row.error_category, None);
    database.drop().await;
}

/// Python `TestListTasks::test_unsampled_table_falls_back_to_exact_total`:
/// a table with no sampled statistics (`reltuples = -1`) gives an exact
/// count. The value is set in the catalog, because whether a TRUNCATE
/// resets it depends on the server version.
#[tokio::test]
#[serial]
async fn an_unsampled_live_table_gives_an_exact_total() {
    let database = SpecDatabase::create().await;
    sqlx::query("UPDATE pg_class SET reltuples = -1 WHERE oid = 'horsies_tasks'::regclass")
        .execute(&database.pool)
        .await
        .expect("mark the live table as unsampled");
    for _ in 0..5 {
        seed_live(&database.pool, &LiveTask::pending("alpha_task")).await;
    }

    let page = page(&database.broker(), TaskListQuery::new(window())).await;

    assert_eq!(page.total, 5);
    database.drop().await;
}

/// Python `TestUnfilteredHistoryTotalContract::test_truncated_history_reports_zero_not_stale_estimate`:
/// an emptied history adds zero to the unfiltered total, not the rows of
/// statistics sampled before the TRUNCATE.
#[tokio::test]
#[serial]
async fn an_emptied_history_adds_nothing_to_the_unfiltered_total() {
    let database = SpecDatabase::create().await;
    for ending in [Ending::Completed, Ending::Completed, Ending::failed("BOOM")] {
        seed_terminal(&database.pool, &LiveTask::pending("alpha_task"), ending).await;
    }
    sqlx::query("ANALYZE horsies_tasks, horsies_task_history")
        .execute(&database.pool)
        .await
        .expect("sample both tables");
    sqlx::query("TRUNCATE horsies_task_history")
        .execute(&database.pool)
        .await
        .expect("empty the history");
    for _ in 0..2 {
        seed_live(&database.pool, &LiveTask::pending("alpha_task")).await;
    }
    sqlx::query("ANALYZE horsies_tasks")
        .execute(&database.pool)
        .await
        .expect("sample the live table");

    let page = page(&database.broker(), TaskListQuery::new(window())).await;

    assert_eq!(page.total, 2);
    database.drop().await;
}

/// Python `TestUnfilteredHistoryTotalContract::test_estimate_decode_failure_is_a_typed_error`:
/// an EXPLAIN payload that the estimate decode cannot read is a contract
/// violation, not retryable, named as the estimate decode. Python injects
/// the failure at the decode; the core cannot replace the decode, so the
/// test drives the decode and the error that `list_tasks` builds from it.
#[test]
fn an_unreadable_estimate_is_a_contract_violation_that_is_not_retryable() {
    let refused = plan_rows_from_explain_text(r#"{"Plan": "not a plan"}"#)
        .expect_err("an EXPLAIN payload without a plan list is refused");

    let error = MonitoringQueryError::history("task list estimate decode", refused);

    assert_eq!(
        error.code,
        MonitoringQueryErrorCode::HistoryContractViolation
    );
    assert!(!error.retryable);
    assert!(
        error.message.contains("estimate decode failed"),
        "{}",
        error.message
    );
}

/// Python `TestGetTaskDetail::test_missing_task_is_not_found`: a UUIDv4 has
/// no birth, so the absence is not classified.
#[tokio::test]
#[serial]
async fn a_missing_task_is_not_found_and_a_v4_id_is_not_classified() {
    let database = SpecDatabase::create().await;
    let task_id = Uuid::new_v4();

    let read = absence_of(&database.broker(), task_id).await;

    assert_eq!(read, not_found(task_id, TaskAbsenceReason::NotClassified));
    database.drop().await;
}

/// Python `TestGetTaskDetail::test_an_absent_v7_task_with_no_detached_leaf_is_within_retained_history`.
#[tokio::test]
#[serial]
async fn an_absent_v7_task_with_no_detached_leaf_is_within_retained_history() {
    let database = SpecDatabase::create().await;
    let task_id = uuid7_born_at(Utc::now() - Duration::days(2));

    let read = absence_of(&database.broker(), task_id).await;

    assert_eq!(
        read,
        not_found(task_id, TaskAbsenceReason::WithinRetainedHistory)
    );
    database.drop().await;
}

/// Python `TestGetTaskDetail::test_an_absent_v7_task_born_before_a_detached_leaf_predates_retained_history`:
/// a birth before the upper anchor of a detached period leaf predates the
/// retained history; a birth after it is within it.
#[tokio::test]
#[serial]
async fn an_absent_v7_task_born_before_a_detached_leaf_predates_retained_history() {
    let database = SpecDatabase::create().await;
    let detached_upper = Utc::now() - Duration::days(1);
    let before = uuid7_born_at(detached_upper - Duration::hours(1));
    let after = uuid7_born_at(detached_upper + Duration::hours(1));
    record_detached_period_leaf(&database.pool, detached_upper).await;

    let broker = database.broker();
    let before_read = absence_of(&broker, before).await;
    let after_read = absence_of(&broker, after).await;

    assert_eq!(
        before_read,
        not_found(before, TaskAbsenceReason::PredatesRetainedHistory)
    );
    assert_eq!(
        after_read,
        not_found(after, TaskAbsenceReason::WithinRetainedHistory)
    );
    database.drop().await;
}

/// Python `TestGetTaskDetail::test_history_row_with_an_unknown_status_is_a_contract_violation`:
/// a history status that is not a task status is a contract violation that
/// names the stored value; it is not read as absence.
#[tokio::test]
#[serial]
async fn an_unknown_stored_status_in_history_is_a_contract_violation() {
    let database = SpecDatabase::create().await;
    let task = LiveTask::pending("alpha_task");
    seed_terminal(&database.pool, &task, Ending::failed("BOOM")).await;
    set_history_status(&database.pool, task.id, "NOT_KNOWN").await;

    let error = get_task_detail(&database.broker(), task.id)
        .await
        .expect_err("an unknown status is refused");

    assert_eq!(
        error.code,
        MonitoringQueryErrorCode::HistoryContractViolation
    );
    assert!(!error.retryable);
    assert!(error.message.contains("NOT_KNOWN"), "{}", error.message);
    database.drop().await;
}

/// Python `TestGetTaskDetail::test_standalone_task_has_no_workflow_linkage`.
#[tokio::test]
#[serial]
async fn a_standalone_task_has_no_workflow_link() {
    let database = SpecDatabase::create().await;
    let task = LiveTask::pending("alpha_task");
    seed_live(&database.pool, &task).await;

    let detail = detail_of(&database.broker(), task.id).await;

    assert_eq!(detail.workflow_id, None);
    assert_eq!(detail.workflow_task_index, None);
    database.drop().await;
}

/// Python `TestGetTaskDetail::test_a_node_row_pointing_elsewhere_does_not_leak_in`:
/// the link matches on the task id, not on membership in some workflow.
#[tokio::test]
#[serial]
async fn a_node_of_another_task_does_not_link_this_task() {
    let database = SpecDatabase::create().await;
    let run = RunSeed::running("alpha_flow");
    seed_run(&database.pool, &run).await;
    let mut linked = LiveTask::pending("alpha_task");
    linked.is_workflow_task = true;
    let unrelated = LiveTask::pending("alpha_task");
    seed_live(&database.pool, &linked).await;
    seed_live(&database.pool, &unrelated).await;
    let mut node = NodeSeed::task(run.id, 0);
    node.kind = NodeKind::Task {
        task_id: Some(linked.id),
    };
    seed_node(&database.pool, &node).await;

    let detail = detail_of(&database.broker(), unrelated.id).await;

    assert_eq!(detail.workflow_id, None);
    assert_eq!(detail.workflow_task_index, None);
    database.drop().await;
}

/// Python `tests/unit/test_execution_class.py`, `TestMonitoringDetail`: the
/// leaf of a live task carries its stored class; a task read from history
/// carries none, as history does not keep the class.
#[tokio::test]
#[serial]
async fn a_live_leaf_shows_its_class_and_a_history_leaf_shows_none() {
    let database = SpecDatabase::create().await;
    let live = LiveTask::pending("alpha_task");
    seed_live(&database.pool, &live).await;
    let finished = LiveTask::pending("beta_task");
    seed_terminal(&database.pool, &finished, Ending::Completed).await;
    let broker = database.broker();

    let live_detail = detail_of(&broker, live.id).await;
    let finished_detail = detail_of(&broker, finished.id).await;

    assert_eq!(live_detail.leaf.execution, Some(Execution::Isolated));
    assert_eq!(finished_detail.leaf.execution, None);
    let encoded = serde_json::to_value(&live_detail.leaf).expect("the leaf encodes");
    assert_eq!(encoded["execution"], "ISOLATED");
    let encoded = serde_json::to_value(&finished_detail.leaf).expect("the leaf encodes");
    assert_eq!(encoded["execution"], serde_json::Value::Null);
    database.drop().await;
}

/// Python `TestGetTaskDetail::test_reports_good_until_and_workflow_membership`.
#[tokio::test]
#[serial]
async fn the_detail_reports_the_deadline_and_the_workflow_membership() {
    let database = SpecDatabase::create().await;
    let deadline = Utc::now() + Duration::hours(2);
    let mut task = LiveTask::pending("alpha_task");
    task.is_workflow_task = true;
    task.good_until = Some(deadline);
    seed_live(&database.pool, &task).await;

    let detail = detail_of(&database.broker(), task.id).await;

    assert!(detail.is_workflow_task);
    let stored = detail.leaf.good_until.expect("the deadline is read");
    assert!(
        (stored - deadline).num_milliseconds().abs() < 1_000,
        "{stored} against {deadline}"
    );
    database.drop().await;
}

/// Python `TestGetTaskDetail::test_empty_attempt_text_is_normalized_to_none`:
/// empty and blank stored texts of the leaf and of an attempt read as
/// absent.
#[tokio::test]
#[serial]
async fn empty_and_blank_attempt_texts_read_as_absent() {
    let database = SpecDatabase::create().await;
    let task = LiveTask::pending("alpha_task");
    // The failure writes the leaf's empty failed reason and its only attempt
    // row: the same empty code and reason, and a blank message.
    seed_terminal(
        &database.pool,
        &task,
        Ending::Failed {
            error_code: Some(String::new()),
            failed_reason: Some(String::new()),
            attempt_message: Some("   ".to_owned()),
        },
    )
    .await;

    let detail = detail_of(&database.broker(), task.id).await;

    assert_eq!(detail.leaf.failed_reason, None);
    let attempt = detail.attempts.first().expect("the attempt is read");
    assert_eq!(attempt.error_code, None);
    assert_eq!(attempt.error_message, None);
    assert_eq!(attempt.failed_reason, None);
    database.drop().await;
}

/// Python `TestTaskDurations::test_claimed_task_queue_time_counts_up`.
#[tokio::test]
#[serial]
async fn a_claimed_task_counts_its_queue_time_up() {
    let database = SpecDatabase::create().await;
    let mut task = LiveTask::claimed("alpha_task", "worker-1");
    task.enqueued_seconds_ago = 25;
    seed_live(&database.pool, &task).await;

    let (queue_s, exec_s) = only_spans(&database.broker()).await;

    let queue_s = queue_s.expect("a claimed task has a queue span");
    assert!((25..=LIVE_SPAN_CEILING_S).contains(&queue_s), "{queue_s}");
    assert_eq!(exec_s, None);
    database.drop().await;
}

/// Python `TestTaskDurations::test_completed_task_reports_both_closed_spans`.
#[tokio::test]
#[serial]
async fn a_completed_task_reports_both_closed_spans() {
    let database = SpecDatabase::create().await;
    let task = LiveTask::pending("alpha_task");
    seed_terminal(&database.pool, &task, Ending::Completed).await;
    set_history_times(&database.pool, task.id, 90, Some(60), 45).await;

    let spans = only_spans(&database.broker()).await;

    assert_eq!(spans, (Some(30), Some(15)));
    database.drop().await;
}

/// Python `TestTaskDurations::test_cancelled_before_start_reports_the_closed_queue_span`:
/// the terminal instant closes the queue span, and a task that never
/// started has no execution span.
#[tokio::test]
#[serial]
async fn a_task_cancelled_before_it_started_reports_a_closed_queue_span() {
    let database = SpecDatabase::create().await;
    let task = LiveTask::pending("alpha_task");
    seed_terminal(&database.pool, &task, Ending::Cancelled).await;
    set_history_times(&database.pool, task.id, 300, None, 0).await;

    let spans = only_spans(&database.broker()).await;

    assert_eq!(spans, (Some(300), None));
    database.drop().await;
}

/// Python `TestTaskDurations::test_expired_before_start_reports_the_closed_queue_span`.
#[tokio::test]
#[serial]
async fn a_task_expired_before_it_started_reports_a_closed_queue_span() {
    let database = SpecDatabase::create().await;
    let mut task = LiveTask::pending("alpha_task");
    task.good_until = Some(Utc::now() - Duration::minutes(1));
    seed_live(&database.pool, &task).await;
    let expired = crate::broker::terminalization::on_test_pool::terminalize(
        &database.pool,
        &TerminalizationCommand::ExpirePendingTasks {
            batch_size: BatchSize::new(10).expect("a positive batch size"),
            result_json: r#"{"err": {"error_code": "TASK_EXPIRED"}}"#.to_owned(),
            error_code: "TASK_EXPIRED".to_owned(),
        },
    )
    .await
    .expect("expire the due task");
    assert!(
        matches!(expired.as_slice(), [TerminalizationOutcome::Applied { .. }]),
        "{expired:?}"
    );
    set_history_times(&database.pool, task.id, 300, None, 0).await;

    let spans = only_spans(&database.broker()).await;

    assert_eq!(spans, (Some(300), None));
    database.drop().await;
}

/// Python `TestTaskDurations::test_detail_leaf_uses_the_same_spans_as_the_list_row`.
#[tokio::test]
#[serial]
async fn the_detail_leaf_and_the_list_row_report_the_same_spans() {
    let database = SpecDatabase::create().await;
    let task = LiveTask::pending("alpha_task");
    seed_terminal(&database.pool, &task, Ending::Cancelled).await;
    set_history_times(&database.pool, task.id, 300, None, 0).await;
    let broker = database.broker();

    let (queue_s, exec_s) = only_spans(&broker).await;
    let detail = detail_of(&broker, task.id).await;

    assert_eq!(detail.leaf.queue_s, queue_s);
    assert_eq!(detail.leaf.exec_s, exec_s);
    database.drop().await;
}

/// Python `TestDatabaseFailure::test_detail_failure_is_not_confused_with_absence`.
#[tokio::test]
async fn a_detail_read_on_an_unreachable_database_is_an_error_not_absence() {
    let error = get_task_detail(&unreachable_broker(), Uuid::new_v4())
        .await
        .expect_err("an unreachable database fails the read");

    assert_eq!(error.code, MonitoringQueryErrorCode::DbOperationFailed);
    assert!(error.retryable, "{error:?}");
}

/// Python `TestDatabaseFailure::test_aggregate_failure_is_reported`.
#[tokio::test]
async fn a_stats_read_on_an_unreachable_database_is_a_retryable_error() {
    let error = task_stats(&unreachable_broker(), &TaskStatsQuery::new(window()))
        .await
        .expect_err("an unreachable database fails the read");

    assert_eq!(error.code, MonitoringQueryErrorCode::DbOperationFailed);
    assert!(error.retryable, "{error:?}");
}
