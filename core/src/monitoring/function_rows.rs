//! The monitoring reads that call the baseline functions (`0007`): the
//! task list, facets, detail, status counts and breakdown
//! (`horsies_monitoring_task_*`), the workflow names, runs, run and node
//! (`horsies_monitoring_workflow_*`) and the schedules
//! (`horsies_monitoring_schedules`). The statements, their typed parameters,
//! and the decoding of their rows into the monitoring models.
//!
//! The functions own the SQL: filters, sort, page bounds, totals, the facet
//! caps, the list bounds, the snapshot and the absence rule. The client
//! passes typed values and reads plain rows.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, TimeDelta, Utc};
use uuid::Uuid;

use crate::broker::core_rows::{column, RowDecodeError};
use crate::broker::task_lists::{ListEnd, ListRow};
use crate::core::history::archive::attempts::{
    decode_attempt_archive, decode_attempt_snapshot, decode_live_attempt_archive,
    decode_live_attempt_snapshot, AttemptArchive, AttemptRecord,
};
use crate::core::history::archive::versions::{JSON_CONTENT_TYPE, JSON_UTF8_CODEC};
use crate::core::history::reads::aggregates::plan_rows_from_explain_text;
use crate::core::history::reads::detail::TaskAbsenceReason;
use crate::core::lifecycle::TerminalizationKind;
use crate::core::task::error::{ContractCode, OperationalCode, OutcomeCode, RetrievalCode};
use crate::core::task::result::TaskResult;
use crate::core::task::Execution;
use crate::core::types::status::TaskStatus;
use crate::db::{ParameterType, Row, TypedStatement};
use crate::telemetry::trace_carrier::stored_trace_id_text;

use super::models::{
    Breakdown, BreakdownCounts, BreakdownGroup, ErrorCategory, GroupRow, LeafTaskInfo,
    ScheduleStateInfo, StatusCount, TaskAttemptInfo, TaskDetail, TaskDetailNotFound,
    TaskDetailRead, TaskSummary, WorkflowEdge, WorkflowNodeInfo, WorkflowRunDetail,
    WorkflowRunSummary, WorkflowTaskDetail,
};
use super::queries::{elapsed, normalize_optional_text, span, TaskGroupBy};
use super::timing::{
    attempt_phases_info, compose_run, ChildRun, LiveStatus, LiveTask, MovedRecord, MovedTask,
    NodeEntry, NodeEventKind, NodeFacts, NodeStatus, NodeWork, RunEvent, RunEventKind, RunFacts,
    TaskAttempts, TaskMove, TaskTimes, TimingRecords, MAX_RUN_EVENTS, MAX_RUN_NODES,
    MAX_TASK_ATTEMPTS,
};
use super::timing_models::{NodeTimingInfo, RunTimingInfo};

/// One page of the task list. Parameters in order: window lower and upper
/// bound, statuses, task names, queue names, worker ids, error codes, error
/// categories, retried only, the built-in codes and their categories (two
/// parallel arrays), sort key, sort direction, offset, limit.
pub(super) static MONITORING_TASK_LIST: TypedStatement = TypedStatement::new(
    "monitoring_task_list",
    "SELECT row_kind, row_position, total_kind, live_total, history_total, history_estimate_plan,
            location, task_id, task_name, queue_name, priority, status, retry_count, max_retries,
            is_workflow_task, error_code, worker_id, worker_hostname, enqueued_at, started_at,
            terminal_at
     FROM horsies_monitoring_task_list(
         $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
     )
     ORDER BY row_position",
    &[
        ParameterType::Timestamptz,
        ParameterType::Timestamptz,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::Bool,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Int4,
        ParameterType::Int4,
    ],
);

/// The facets. Parameters in order: window lower and upper bound, statuses,
/// retried only, the built-in codes and their categories. Rows come ordered
/// by facet, then rank.
pub(super) static MONITORING_TASK_FACETS: TypedStatement = TypedStatement::new(
    "monitoring_task_facets",
    "SELECT facet, facet_value, facet_count, category, category_total
     FROM horsies_monitoring_task_facets($1, $2, $3, $4, $5, $6)",
    &[
        ParameterType::Timestamptz,
        ParameterType::Timestamptz,
        ParameterType::TextArray,
        ParameterType::Bool,
        ParameterType::TextArray,
        ParameterType::TextArray,
    ],
);

/// One task with its attempts and workflow link, or the reason it is
/// absent. Parameters: task id, with result, with attempts.
pub(super) static MONITORING_TASK_DETAIL: TypedStatement = TypedStatement::new(
    "monitoring_task_detail",
    "SELECT location, task_id, task_name, queue_name, priority, status, is_workflow_task,
            retry_count, max_retries, enqueued_at, started_at, good_until, worker_hostname,
            execution, terminal_at, error_code, final_failed_reason, attempt_archive_version,
            attempt_snapshot, attempt_snapshot_digest, workflow_id, workflow_task_index,
            absence_reason, trace_id
     FROM horsies_monitoring_task_detail($1, $2, $3)",
    &[
        ParameterType::Uuid,
        ParameterType::Bool,
        ParameterType::Bool,
    ],
);

/// The count of each task status. Parameters in order: window lower and
/// upper bound, task names, queue names, worker ids, error codes, error
/// categories, retried only, the built-in codes and their categories. Rows
/// come one per status, in the fixed status order.
pub(super) static MONITORING_TASK_STATS: TypedStatement = TypedStatement::new(
    "monitoring_task_stats",
    "SELECT row_position, status, task_count
     FROM horsies_monitoring_task_stats($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
     ORDER BY row_position",
    &[
        ParameterType::Timestamptz,
        ParameterType::Timestamptz,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::Bool,
        ParameterType::TextArray,
        ParameterType::TextArray,
    ],
);

/// The task counts per group and their total. Parameters in order: window
/// lower and upper bound, group column (`worker`, `task_name` or `queue`),
/// statuses, task names, queue names, worker ids, error codes, error
/// categories, retried only, the built-in codes and their categories, the
/// group limit. Rows come as the `TOTAL` row, then the `GROUP` rows in
/// final order.
pub(super) static MONITORING_TASK_BREAKDOWN: TypedStatement = TypedStatement::new(
    "monitoring_task_breakdown",
    "SELECT row_kind, row_position, group_kind, group_value, group_count, total, pending,
            claimed, running, completed, failed, cancelled, expired, retried
     FROM horsies_monitoring_task_breakdown(
         $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
     )
     ORDER BY row_position",
    &[
        ParameterType::Timestamptz,
        ParameterType::Timestamptz,
        ParameterType::Text,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::Bool,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::Int4,
    ],
);

/// The names of the root runs, then the `LIST_END` row.
pub(super) static MONITORING_WORKFLOW_NAMES: TypedStatement = TypedStatement::new(
    "monitoring_workflow_names",
    "SELECT row_kind, row_position, workflow_name, list_state, row_bound
     FROM horsies_monitoring_workflow_names()
     ORDER BY row_position NULLS LAST",
    &[],
);

/// The newest root runs. Parameters in order: name and status (each `NULL`
/// for no filter), limit.
pub(super) static MONITORING_WORKFLOW_RUNS: TypedStatement = TypedStatement::new(
    "monitoring_workflow_runs",
    "SELECT row_position, workflow_id, workflow_name, definition_key, status, created_at,
            completed_at
     FROM horsies_monitoring_workflow_runs($1, $2, $3)
     ORDER BY row_position",
    &[
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Int4,
    ],
);

/// One run: the `RUN` row, its `NODE` rows in task index order, then (when
/// the run row says `INCLUDED`) its `EVENT` rows in event order and its
/// `TASK` rows; or one `ABSENT` row. Parameter: the run id. One statement:
/// the function reads every row with a fixed number of statements, none per
/// node or per task.
pub(super) static MONITORING_WORKFLOW_RUN: TypedStatement = TypedStatement::new(
    "monitoring_workflow_run",
    "SELECT row_kind, row_position, workflow_id, workflow_name, definition_key, status,
            created_at, started_at, completed_at, task_index, node_id, task_name, dependencies,
            allow_failed_deps, is_subworkflow, sub_workflow_id, child_total, child_failed,
            trace_id, as_of, captured_since, timing_rows, node_created_at, node_task_id,
            child_created_at, child_completed_at, event_id, event_kind, occurred_at,
            terminalization_kind, location, task_id, enqueued_at, claimed_at,
            attempt_archive_version, attempt_snapshot, attempt_snapshot_digest
     FROM horsies_monitoring_workflow_run($1)
     ORDER BY row_position NULLS FIRST",
    &[ParameterType::Uuid],
);

/// One node with its backing task: one `NODE` row (the node columns
/// `node_*`, then the columns of the task detail under their own names), or
/// one `ABSENT` row. Parameters: the run id, the task index.
pub(super) static MONITORING_WORKFLOW_NODE: TypedStatement = TypedStatement::new(
    "monitoring_workflow_node",
    "SELECT row_kind, node_task_index, node_id, node_task_name, node_status,
            node_is_subworkflow, node_result, location, task_id, task_name, queue_name,
            priority, status, is_workflow_task, retry_count, max_retries, enqueued_at,
            started_at, good_until, worker_hostname, execution, terminal_at, error_code,
            final_failed_reason, attempt_archive_version, attempt_snapshot,
            attempt_snapshot_digest, workflow_id, workflow_task_index, absence_reason, trace_id
     FROM horsies_monitoring_workflow_node($1, $2)",
    &[ParameterType::Uuid, ParameterType::Int4],
);

/// Every schedule state, soonest next run first, then the `LIST_END` row.
pub(super) static MONITORING_SCHEDULES: TypedStatement = TypedStatement::new(
    "monitoring_schedules",
    "SELECT row_kind, row_position, schedule_name, last_run_at, next_run_at, last_task_id,
            run_count, updated_at, list_state, row_bound
     FROM horsies_monitoring_schedules()
     ORDER BY row_position NULLS LAST",
    &[],
);

/// The built-in error-code taxonomy as the two parallel arrays the functions
/// take: code `i` belongs to category `i`. A code outside it is a `CUSTOM`
/// code.
pub(super) struct BuiltinTaxonomy {
    pub(super) codes: Vec<String>,
    pub(super) categories: Vec<String>,
}

pub(super) fn builtin_taxonomy() -> BuiltinTaxonomy {
    let families: [(ErrorCategory, Vec<String>); 4] = [
        (
            ErrorCategory::Operational,
            OperationalCode::ALL
                .iter()
                .map(ToString::to_string)
                .collect(),
        ),
        (
            ErrorCategory::Contract,
            ContractCode::ALL.iter().map(ToString::to_string).collect(),
        ),
        (
            ErrorCategory::Retrieval,
            RetrievalCode::ALL.iter().map(ToString::to_string).collect(),
        ),
        (
            ErrorCategory::Outcome,
            OutcomeCode::ALL.iter().map(ToString::to_string).collect(),
        ),
    ];
    let mut codes = Vec::new();
    let mut categories = Vec::new();
    for (category, family) in families {
        for code in family {
            codes.push(code);
            categories.push(category.as_str().to_owned());
        }
    }
    BuiltinTaxonomy { codes, categories }
}

/// How the task list counts its total.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ListTotals {
    /// Exact counts of the matches (any filter).
    Exact { live: i64, history: i64 },
    /// No filter, and the history table has no row.
    EstimateHistoryEmpty { live: i64 },
    /// No filter: the live estimate and the `EXPLAIN (FORMAT JSON)` text of
    /// the history window read, which carries the planner's row estimate.
    EstimateHistoryPlan { live: i64, plan: String },
}

impl ListTotals {
    /// The total of the page. Errors: a plan text that does not carry a row
    /// estimate.
    pub(super) fn total(&self) -> Result<i64, String> {
        match self {
            ListTotals::Exact { live, history } => Ok(live + history),
            ListTotals::EstimateHistoryEmpty { live } => Ok(*live),
            ListTotals::EstimateHistoryPlan { live, plan } => plan_rows_from_explain_text(plan)
                .map(|history| live + history)
                .map_err(|error| error.to_string()),
        }
    }
}

/// One row of the task list function.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum TaskListRow {
    Totals(ListTotals),
    Task(Box<TaskSummary>),
}

/// Where a task row of the list or the detail comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Location {
    Live,
    History,
}

fn shape(statement: &TypedStatement, reason: String) -> RowDecodeError {
    RowDecodeError::Shape {
        statement: statement.name(),
        reason,
    }
}

fn location(statement: &TypedStatement, value: &str) -> Result<Location, RowDecodeError> {
    match value {
        "LIVE" => Ok(Location::Live),
        "HISTORY" => Ok(Location::History),
        other => Err(shape(statement, format!("unknown location {other:?}"))),
    }
}

/// Reads one row of the task list function.
pub(super) fn task_list_row(
    row: &Row,
    statement: &TypedStatement,
) -> Result<TaskListRow, RowDecodeError> {
    let kind: String = column(row, statement, "row_kind")?;
    match kind.as_str() {
        "TOTALS" => {
            let total_kind: String = column(row, statement, "total_kind")?;
            let live: i64 = column(row, statement, "live_total")?;
            let totals = match total_kind.as_str() {
                "EXACT" => ListTotals::Exact {
                    live,
                    history: column(row, statement, "history_total")?,
                },
                "ESTIMATE_HISTORY_EMPTY" => ListTotals::EstimateHistoryEmpty { live },
                "ESTIMATE_HISTORY_PLAN" => ListTotals::EstimateHistoryPlan {
                    live,
                    plan: column(row, statement, "history_estimate_plan")?,
                },
                other => return Err(shape(statement, format!("unknown total_kind {other:?}"))),
            };
            Ok(TaskListRow::Totals(totals))
        }
        "TASK" => {
            let place: String = column(row, statement, "location")?;
            let fields = SummaryFields {
                location: location(statement, &place)?,
                task_id: column(row, statement, "task_id")?,
                task_name: column(row, statement, "task_name")?,
                queue_name: column(row, statement, "queue_name")?,
                priority: column(row, statement, "priority")?,
                status: column(row, statement, "status")?,
                retry_count: column(row, statement, "retry_count")?,
                max_retries: column(row, statement, "max_retries")?,
                is_workflow_task: column(row, statement, "is_workflow_task")?,
                error_code: column(row, statement, "error_code")?,
                worker_id: column(row, statement, "worker_id")?,
                worker_hostname: column(row, statement, "worker_hostname")?,
                enqueued_at: column(row, statement, "enqueued_at")?,
                started_at: column(row, statement, "started_at")?,
                terminal_at: column(row, statement, "terminal_at")?,
            };
            summary_from_fields(fields)
                .map(|summary| TaskListRow::Task(Box::new(summary)))
                .map_err(|reason| shape(statement, reason))
        }
        other => Err(shape(statement, format!("unknown row_kind {other:?}"))),
    }
}

/// The columns of a task row of the list.
struct SummaryFields {
    location: Location,
    task_id: Uuid,
    task_name: String,
    queue_name: String,
    priority: i32,
    status: String,
    retry_count: i32,
    max_retries: i32,
    is_workflow_task: bool,
    error_code: Option<String>,
    worker_id: Option<String>,
    worker_hostname: Option<String>,
    enqueued_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    terminal_at: Option<DateTime<Utc>>,
}

/// The summary of a list row. A live row has no terminal time, no error
/// code and no completed or failed time; its queue and run spans grow to
/// now while it waits or runs. A history row has its terminal time. Pure
/// apart from the clock read of a live span.
fn summary_from_fields(fields: SummaryFields) -> Result<TaskSummary, String> {
    let (completed_at, failed_at, queue_s, exec_s) = match (fields.location, fields.terminal_at) {
        (Location::Live, None) => (
            None,
            None,
            span(
                Some(fields.enqueued_at),
                fields.started_at,
                matches!(fields.status.as_str(), "PENDING" | "CLAIMED"),
            ),
            span(fields.started_at, None, fields.status.as_str() == "RUNNING"),
        ),
        (Location::History, Some(terminal)) => (
            (fields.status == "COMPLETED").then_some(terminal),
            (fields.status != "COMPLETED").then_some(terminal),
            span(
                Some(fields.enqueued_at),
                fields.started_at.or(Some(terminal)),
                false,
            ),
            span(fields.started_at, Some(terminal), false),
        ),
        (Location::Live, Some(_)) => {
            return Err(format!("live task {} has a terminal time", fields.task_id))
        }
        (Location::History, None) => {
            return Err(format!(
                "history task {} has no terminal time",
                fields.task_id
            ))
        }
    };
    Ok(TaskSummary {
        id: fields.task_id,
        task_name: fields.task_name,
        queue_name: fields.queue_name,
        status: fields.status,
        priority: fields.priority,
        retry_count: fields.retry_count,
        max_retries: fields.max_retries,
        is_workflow_task: fields.is_workflow_task,
        error_category: super::queries::categorize_error_code(fields.error_code.as_deref())
            .map(|category| category.as_str().to_owned()),
        error_code: normalize_optional_text(fields.error_code.as_deref()),
        worker_hostname: fields.worker_hostname,
        worker_id: fields.worker_id,
        enqueued_at: Some(fields.enqueued_at),
        started_at: fields.started_at,
        completed_at,
        failed_at,
        queue_s,
        exec_s,
    })
}

/// The task list page from the rows of the function: one `TOTALS` row and
/// the task rows in page order.
pub(super) fn page_from_rows(
    rows: Vec<TaskListRow>,
) -> Result<(ListTotals, Vec<TaskSummary>), String> {
    let mut totals = None;
    let mut summaries = Vec::with_capacity(rows.len());
    for row in rows {
        match (row, &totals) {
            (TaskListRow::Totals(found), None) => totals = Some(found),
            (TaskListRow::Totals(_), Some(_)) => {
                return Err("the task list returned two TOTALS rows".to_owned())
            }
            (TaskListRow::Task(summary), _) => summaries.push(*summary),
        }
    }
    let totals = totals.ok_or_else(|| "the task list returned no TOTALS row".to_owned())?;
    Ok((totals, summaries))
}

/// What a detail row breaks: a row shape defect, or stored history data
/// that does not decode (a status that is not a task status, an attempt
/// snapshot that fails its check).
#[derive(Debug)]
pub(super) enum DetailRowError {
    Row(RowDecodeError),
    StoredData(String),
}

impl From<RowDecodeError> for DetailRowError {
    fn from(error: RowDecodeError) -> Self {
        DetailRowError::Row(error)
    }
}

/// Reads the one row of the detail function: the task, or the reason the
/// function gives for its absence. A reason the function does not define is
/// a row shape defect, as a location it does not define is.
pub(super) fn task_detail_row(
    row: &Row,
    statement: &TypedStatement,
) -> Result<TaskDetailRead, DetailRowError> {
    let place: String = column(row, statement, "location")?;
    let location = match place.as_str() {
        "ABSENT" => return Ok(TaskDetailRead::NotFound(absent_task(row, statement)?)),
        other => location(statement, other)?,
    };
    let task_id: Uuid = column(row, statement, "task_id")?;
    let status: String = column(row, statement, "status")?;
    let enqueued_at: DateTime<Utc> = column(row, statement, "enqueued_at")?;
    let started_at: Option<DateTime<Utc>> = column(row, statement, "started_at")?;
    let attempt_version: Option<i16> = column(row, statement, "attempt_archive_version")?;
    let attempt_payload: Option<Vec<u8>> = column(row, statement, "attempt_snapshot")?;
    let error_code: Option<String> = column(row, statement, "error_code")?;
    let failed_reason: Option<String> = column(row, statement, "final_failed_reason")?;
    let terminal_at: Option<DateTime<Utc>> = column(row, statement, "terminal_at")?;
    let stored_trace_id: Option<Vec<u8>> = column(row, statement, "trace_id")?;
    let trace_id = stored_trace_id_text(stored_trace_id.as_deref())
        .map_err(|refused| DetailRowError::StoredData(format!("task {task_id}: {refused}")))?;
    let records = match (location, attempt_version, attempt_payload) {
        (_, None, None) => Vec::new(),
        (Location::Live, Some(version), Some(payload)) => {
            decode_live_attempt_snapshot(version, &payload)
                .map_err(|error| DetailRowError::StoredData(error.to_string()))?
        }
        (Location::History, Some(version), Some(payload)) => {
            let digest: Option<Vec<u8>> = column(row, statement, "attempt_snapshot_digest")?;
            let digest = digest.ok_or_else(|| {
                DetailRowError::StoredData(format!(
                    "history attempt snapshot of task {task_id} has no digest"
                ))
            })?;
            decode_attempt_snapshot(
                version,
                JSON_UTF8_CODEC,
                JSON_CONTENT_TYPE,
                &payload,
                &digest,
            )
            .map_err(|error| DetailRowError::StoredData(error.to_string()))?
        }
        (_, Some(_), None) | (_, None, Some(_)) => {
            return Err(DetailRowError::Row(shape(
                statement,
                format!("task {task_id} has an attempt version without a snapshot or the reverse"),
            )))
        }
    };
    let (completed_at, failed_at, queue_s, exec_s) = match (location, terminal_at) {
        (Location::Live, None) => (
            None,
            None,
            span(
                Some(enqueued_at),
                started_at,
                matches!(status.as_str(), "PENDING" | "CLAIMED"),
            ),
            span(started_at, None, status.as_str() == "RUNNING"),
        ),
        (Location::History, Some(terminal)) => {
            if status.parse::<TaskStatus>().is_err() {
                return Err(DetailRowError::StoredData(format!(
                    "history row {task_id} has status {}, which is not a task status",
                    crate::core::python_text::python_repr(&status)
                )));
            }
            (
                (status == "COMPLETED").then_some(terminal),
                (status != "COMPLETED").then_some(terminal),
                span(Some(enqueued_at), started_at.or(Some(terminal)), false),
                span(started_at, Some(terminal), false),
            )
        }
        (Location::Live, Some(_)) | (Location::History, None) => {
            return Err(DetailRowError::Row(shape(
                statement,
                format!("task {task_id}: the terminal time does not match its location"),
            )))
        }
    };
    let execution = match location {
        Location::Live => {
            let stored: String = column(row, statement, "execution")?;
            let class = Execution::from_stored(&stored).ok_or_else(|| {
                DetailRowError::Row(shape(
                    statement,
                    format!("live task {task_id} has the execution class {stored:?}"),
                ))
            })?;
            Some(class)
        }
        Location::History => None,
    };
    let leaf = LeafTaskInfo {
        task_id,
        status,
        error_code: normalize_optional_text(error_code.as_deref()),
        failed_reason: normalize_optional_text(failed_reason.as_deref()),
        retry_count: column(row, statement, "retry_count")?,
        max_retries: column(row, statement, "max_retries")?,
        enqueued_at: Some(enqueued_at),
        started_at,
        completed_at,
        failed_at,
        queue_s,
        exec_s,
        worker_hostname: column(row, statement, "worker_hostname")?,
        good_until: column(row, statement, "good_until")?,
        execution,
        trace_id,
    };
    Ok(TaskDetailRead::Found(Box::new(TaskDetail {
        leaf,
        task_name: column(row, statement, "task_name")?,
        queue_name: column(row, statement, "queue_name")?,
        priority: column(row, statement, "priority")?,
        is_workflow_task: column(row, statement, "is_workflow_task")?,
        error_category: super::queries::categorize_error_code(error_code.as_deref())
            .map(|category| category.as_str().to_owned()),
        attempts: records.iter().map(attempt_info).collect(),
        workflow_id: column(row, statement, "workflow_id")?,
        workflow_task_index: column(row, statement, "workflow_task_index")?,
    })))
}

/// The `ABSENT` row of the detail function: the task id and the reason
/// that `horsies_task_absence_reason` classifies in the same snapshot.
fn absent_task(
    row: &Row,
    statement: &TypedStatement,
) -> Result<TaskDetailNotFound, RowDecodeError> {
    let task_id: Uuid = column(row, statement, "task_id")?;
    let reason: String = column(row, statement, "absence_reason")?;
    let reason =
        TaskAbsenceReason::parse(&reason).map_err(|error| shape(statement, error.to_string()))?;
    Ok(TaskDetailNotFound { task_id, reason })
}

fn attempt_info(record: &AttemptRecord) -> TaskAttemptInfo {
    TaskAttemptInfo {
        attempt: record.attempt(),
        outcome: record.outcome().to_owned(),
        will_retry: record.will_retry(),
        error_code: normalize_optional_text(record.error_code()),
        error_message: normalize_optional_text(record.error_message()),
        failed_reason: normalize_optional_text(record.failed_reason()),
        worker_hostname: record.worker_hostname().map(str::to_owned),
        started_at: Some(record.started_at()),
        finished_at: Some(record.finished_at()),
        phases: attempt_phases_info(record.phases()),
    }
}

/// The status counts of the stats function: one per task status, in the
/// fixed status order. Errors: a column that does not decode, or rows that
/// are not the seven statuses in that order.
pub(super) fn status_counts(
    rows: &[Row],
    statement: &TypedStatement,
) -> Result<Vec<StatusCount>, RowDecodeError> {
    let counts = rows
        .iter()
        .map(|row| {
            Ok(StatusCount {
                status: column(row, statement, "status")?,
                count: column(row, statement, "task_count")?,
            })
        })
        .collect::<Result<Vec<_>, RowDecodeError>>()?;
    let answered: Vec<&str> = counts.iter().map(|count| count.status.as_str()).collect();
    let expected: Vec<String> = TaskStatus::ALL.iter().map(ToString::to_string).collect();
    match answered == expected {
        true => Ok(counts),
        false => Err(shape(
            statement,
            format!("the stats function answered the statuses {answered:?}"),
        )),
    }
}

/// One row of the breakdown function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum BreakdownRow {
    /// The counts of every group, and the number of groups.
    Total {
        counts: BreakdownCounts,
        group_count: i64,
    },
    /// One group.
    Group(GroupRow),
}

fn breakdown_counts(
    row: &Row,
    statement: &TypedStatement,
) -> Result<BreakdownCounts, RowDecodeError> {
    Ok(BreakdownCounts {
        total: column(row, statement, "total")?,
        pending: column(row, statement, "pending")?,
        claimed: column(row, statement, "claimed")?,
        running: column(row, statement, "running")?,
        completed: column(row, statement, "completed")?,
        failed: column(row, statement, "failed")?,
        cancelled: column(row, statement, "cancelled")?,
        expired: column(row, statement, "expired")?,
        retried: column(row, statement, "retried")?,
    })
}

/// Reads one row of the breakdown function. A `GROUP` row is a stored value
/// (`VALUE` with its value) or the unclaimed tasks (`UNCLAIMED`, no value).
pub(super) fn breakdown_row(
    row: &Row,
    statement: &TypedStatement,
) -> Result<BreakdownRow, RowDecodeError> {
    let kind: String = column(row, statement, "row_kind")?;
    match kind.as_str() {
        "TOTAL" => Ok(BreakdownRow::Total {
            counts: breakdown_counts(row, statement)?,
            group_count: column(row, statement, "group_count")?,
        }),
        "GROUP" => {
            let group_kind: String = column(row, statement, "group_kind")?;
            let group_value: Option<String> = column(row, statement, "group_value")?;
            let group = match (group_kind.as_str(), group_value) {
                ("VALUE", Some(value)) => BreakdownGroup::Value { value },
                ("UNCLAIMED", None) => BreakdownGroup::Unclaimed,
                ("VALUE", None) => {
                    return Err(shape(statement, "a VALUE group has no value".to_owned()))
                }
                ("UNCLAIMED", Some(value)) => {
                    return Err(shape(
                        statement,
                        format!("the UNCLAIMED group has the value {value:?}"),
                    ))
                }
                (other, _) => {
                    return Err(shape(statement, format!("unknown group_kind {other:?}")))
                }
            };
            Ok(BreakdownRow::Group(GroupRow {
                counts: breakdown_counts(row, statement)?,
                group,
            }))
        }
        other => Err(shape(statement, format!("unknown row_kind {other:?}"))),
    }
}

/// The breakdown from the rows of the function, in their order: the one
/// `TOTAL` row first, then the `GROUP` rows in final order. Pure.
pub(super) fn breakdown_from_rows(
    group_by: TaskGroupBy,
    rows: Vec<BreakdownRow>,
) -> Result<Breakdown, String> {
    let mut rows = rows.into_iter();
    let (total, group_count) = match rows.next() {
        Some(BreakdownRow::Total {
            counts,
            group_count,
        }) => (counts, group_count),
        Some(BreakdownRow::Group(_)) => {
            return Err("the breakdown answered a GROUP row before its TOTAL row".to_owned())
        }
        None => return Err("the breakdown answered no TOTAL row".to_owned()),
    };
    let groups = rows
        .map(|row| match row {
            BreakdownRow::Group(group) => Ok(group),
            BreakdownRow::Total { .. } => {
                Err("the breakdown answered a second TOTAL row".to_owned())
            }
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(Breakdown {
        group_by: group_by.as_str().to_owned(),
        groups,
        total,
        group_count,
    })
}

/// Reads one row of a bounded list function: an item row of `item_kind`
/// through `item`, or the `LIST_END` row.
fn bounded_list_row<T>(
    row: &Row,
    statement: &TypedStatement,
    item_kind: &str,
    item: fn(&Row, &TypedStatement) -> Result<T, RowDecodeError>,
) -> Result<ListRow<T>, RowDecodeError> {
    let kind: String = column(row, statement, "row_kind")?;
    match kind.as_str() {
        "LIST_END" => {
            let state: String = column(row, statement, "list_state")?;
            match state.as_str() {
                "COMPLETE" => Ok(ListRow::End(ListEnd::Complete)),
                "TRUNCATED" => {
                    let bound: i32 = column(row, statement, "row_bound")?;
                    let bound = usize::try_from(bound)
                        .map_err(|_| shape(statement, format!("row_bound {bound} is negative")))?;
                    Ok(ListRow::End(ListEnd::Truncated { bound }))
                }
                other => Err(shape(statement, format!("unknown list_state {other:?}"))),
            }
        }
        other if other == item_kind => Ok(ListRow::Task(item(row, statement)?)),
        other => Err(shape(statement, format!("unknown row_kind {other:?}"))),
    }
}

fn workflow_name(row: &Row, statement: &TypedStatement) -> Result<String, RowDecodeError> {
    column(row, statement, "workflow_name")
}

/// Reads one row of the workflow names function.
pub(super) fn workflow_name_row(
    row: &Row,
    statement: &TypedStatement,
) -> Result<ListRow<String>, RowDecodeError> {
    bounded_list_row(row, statement, "NAME", workflow_name)
}

fn schedule_state(
    row: &Row,
    statement: &TypedStatement,
) -> Result<ScheduleStateInfo, RowDecodeError> {
    Ok(ScheduleStateInfo {
        schedule_name: column(row, statement, "schedule_name")?,
        last_run_at: column(row, statement, "last_run_at")?,
        next_run_at: column(row, statement, "next_run_at")?,
        last_task_id: column(row, statement, "last_task_id")?,
        run_count: column(row, statement, "run_count")?,
        updated_at: column(row, statement, "updated_at")?,
    })
}

/// Reads one row of the schedules function.
pub(super) fn schedule_row(
    row: &Row,
    statement: &TypedStatement,
) -> Result<ListRow<ScheduleStateInfo>, RowDecodeError> {
    bounded_list_row(row, statement, "SCHEDULE", schedule_state)
}

/// The summary of a run, from a row of the runs function or the `RUN` row of
/// the run function. The wall time of an open run counts up to now.
fn run_summary(
    row: &Row,
    statement: &TypedStatement,
) -> Result<WorkflowRunSummary, RowDecodeError> {
    let created_at: DateTime<Utc> = column(row, statement, "created_at")?;
    let completed_at: Option<DateTime<Utc>> = column(row, statement, "completed_at")?;
    Ok(WorkflowRunSummary {
        id: column(row, statement, "workflow_id")?,
        name: column(row, statement, "workflow_name")?,
        definition_key: column(row, statement, "definition_key")?,
        status: column(row, statement, "status")?,
        created_at: Some(created_at),
        completed_at,
        wall_s: elapsed(Some(created_at), completed_at),
    })
}

/// The runs of the runs function, in their order.
pub(super) fn workflow_runs(
    rows: &[Row],
    statement: &TypedStatement,
) -> Result<Vec<WorkflowRunSummary>, RowDecodeError> {
    rows.iter().map(|row| run_summary(row, statement)).collect()
}

/// The execution time of a node: a RUNNING node counts up to now; a finished
/// node with a completion time is closed; any other node has none. Exact,
/// as [`elapsed`].
fn node_exec_s(
    status: &str,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
) -> Option<TimeDelta> {
    match (status, completed_at) {
        ("RUNNING", _) => elapsed(started_at, None),
        ("COMPLETED" | "FAILED" | "SKIPPED" | "CANCELLED", Some(completed_at)) => {
            elapsed(started_at, Some(completed_at))
        }
        _ => None,
    }
}

/// One node of a run with the indexes it depends on.
struct RunNode {
    info: WorkflowNodeInfo,
    dependencies: Vec<i32>,
}

/// One `NODE` row of the run function: the node columns, the indexes the
/// node depends on, and the facts of its timing.
struct NodeRow {
    columns: NodeColumns,
    dependencies: Vec<i32>,
    facts: NodeFacts,
}

/// The fields of [`WorkflowNodeInfo`] that the `NODE` row holds, before the
/// timing of the node is composed.
struct NodeColumns {
    task_index: i32,
    node_id: Option<String>,
    task_name: String,
    node_status: String,
    is_subworkflow: bool,
    sub_workflow_id: Option<Uuid>,
    allow_failed_dependencies: bool,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    exec_s: Option<TimeDelta>,
    child_total: Option<i64>,
    child_failed: Option<i64>,
}

impl NodeRow {
    fn into_run_node(self, timing: NodeTimingInfo) -> RunNode {
        let columns = self.columns;
        RunNode {
            info: WorkflowNodeInfo {
                task_index: columns.task_index,
                node_id: columns.node_id,
                task_name: columns.task_name,
                node_status: columns.node_status,
                is_subworkflow: columns.is_subworkflow,
                sub_workflow_id: columns.sub_workflow_id,
                allow_failed_dependencies: columns.allow_failed_dependencies,
                started_at: columns.started_at,
                completed_at: columns.completed_at,
                exec_s: columns.exec_s,
                child_total: columns.child_total,
                child_failed: columns.child_failed,
                timing,
            },
            dependencies: self.dependencies,
        }
    }
}

fn node_row(row: &Row, statement: &TypedStatement) -> Result<NodeRow, RowDecodeError> {
    let node_status: String = column(row, statement, "status")?;
    let status = NodeStatus::parse(&node_status).ok_or_else(|| {
        shape(
            statement,
            format!("node status {node_status:?} is not a node status"),
        )
    })?;
    let task_index: i32 = column(row, statement, "task_index")?;
    let started_at: Option<DateTime<Utc>> = column(row, statement, "started_at")?;
    let completed_at: Option<DateTime<Utc>> = column(row, statement, "completed_at")?;
    let is_subworkflow: bool = column(row, statement, "is_subworkflow")?;
    let sub_workflow_id: Option<Uuid> = column(row, statement, "sub_workflow_id")?;
    let dependencies: Vec<i32> = column(row, statement, "dependencies")?;
    let work = match is_subworkflow {
        false => NodeWork::Task {
            linked: column(row, statement, "node_task_id")?,
        },
        true => NodeWork::SubWorkflow {
            child: child_run(row, statement, sub_workflow_id)?,
        },
    };
    let facts = NodeFacts {
        task_index,
        status,
        created_at: column(row, statement, "node_created_at")?,
        completed_at,
        entry: match dependencies.is_empty() {
            true => NodeEntry::Ready,
            false => NodeEntry::Pending,
        },
        work,
    };
    let exec_s = node_exec_s(&node_status, started_at, completed_at);
    Ok(NodeRow {
        columns: NodeColumns {
            task_index,
            node_id: column(row, statement, "node_id")?,
            task_name: column(row, statement, "task_name")?,
            node_status,
            is_subworkflow,
            sub_workflow_id,
            // The stored column name differs from the public name on purpose:
            // the function output keeps the stored form.
            allow_failed_dependencies: column(row, statement, "allow_failed_deps")?,
            started_at,
            completed_at,
            exec_s,
            child_total: column(row, statement, "child_total")?,
            child_failed: column(row, statement, "child_failed")?,
        },
        dependencies,
        facts,
    })
}

/// The child run of a sub-workflow node: none named, the row of the child,
/// or a name that no row holds (the child times are NULL).
fn child_run(
    row: &Row,
    statement: &TypedStatement,
    sub_workflow_id: Option<Uuid>,
) -> Result<ChildRun, RowDecodeError> {
    let created_at: Option<DateTime<Utc>> = column(row, statement, "child_created_at")?;
    let completed_at: Option<DateTime<Utc>> = column(row, statement, "child_completed_at")?;
    match (sub_workflow_id, created_at, completed_at) {
        (None, None, None) => Ok(ChildRun::NotLinked),
        (Some(_), Some(created_at), completed_at) => Ok(ChildRun::Found {
            created_at,
            completed_at,
        }),
        (Some(_), None, None) => Ok(ChildRun::NotFound),
        (None, _, _) | (Some(_), None, Some(_)) => Err(shape(
            statement,
            "a node has child run times that its child link does not give".to_owned(),
        )),
    }
}

/// The run, its nodes in task index order, its edges, its failed nodes, the
/// trace id of its start and its timing. An edge joins a dependency to the
/// node that names it; a dependency on an index that the run does not have
/// makes no edge. Pure.
fn run_detail(
    run: WorkflowRunSummary,
    nodes: Vec<RunNode>,
    trace_id: Option<String>,
    timing: RunTimingInfo,
) -> WorkflowRunDetail {
    let known: HashSet<i32> = nodes.iter().map(|node| node.info.task_index).collect();
    let mut edges = Vec::new();
    for node in &nodes {
        for dependency in node
            .dependencies
            .iter()
            .filter(|dependency| known.contains(*dependency))
        {
            edges.push(WorkflowEdge {
                from_index: *dependency,
                to_index: node.info.task_index,
            });
        }
    }
    let nodes: Vec<WorkflowNodeInfo> = nodes.into_iter().map(|node| node.info).collect();
    let failed_indices: Vec<i32> = nodes
        .iter()
        .filter(|node| node.node_status == "FAILED")
        .map(|node| node.task_index)
        .collect();
    WorkflowRunDetail {
        run,
        nodes,
        edges,
        failed_count: i64::try_from(failed_indices.len()).unwrap_or(i64::MAX),
        failed_indices,
        trace_id,
        timing,
    }
}

/// The answer of the run function.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum RunRead {
    Found(Box<WorkflowRunDetail>),
    /// No run has the id.
    Absent,
}

/// Reads the rows of the run function: the `RUN` row and the rows that
/// follow it, or the one `ABSENT` row. A row of the wrong shape is a
/// [`DetailRowError::Row`]; an attempt archive that does not decode is
/// [`DetailRowError::StoredData`].
pub(super) fn workflow_run(
    rows: &[Row],
    statement: &TypedStatement,
) -> Result<RunRead, DetailRowError> {
    let Some((first, rest)) = rows.split_first() else {
        return Err(DetailRowError::Row(shape(
            statement,
            "the run function answered no row".to_owned(),
        )));
    };
    let kind: String = column(first, statement, "row_kind")?;
    match (kind.as_str(), rest.is_empty()) {
        ("ABSENT", true) => Ok(RunRead::Absent),
        ("ABSENT", false) => Err(DetailRowError::Row(shape(
            statement,
            "a row follows the ABSENT row".to_owned(),
        ))),
        ("RUN", _) => {
            found_run(first, rest, statement).map(|detail| RunRead::Found(Box::new(detail)))
        }
        (other, _) => Err(DetailRowError::Row(shape(
            statement,
            format!("unknown row_kind {other:?}"),
        ))),
    }
}

/// What the `RUN` row says about the timing rows that follow it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimingRows {
    Included,
    BeforeCapture,
    BoundExceeded,
}

/// The rows that follow the `RUN` row, by kind.
#[derive(Default)]
struct RunRows {
    nodes: Vec<NodeRow>,
    events: Vec<EventRow>,
    tasks: Vec<TaskRow>,
}

fn found_run(
    first: &Row,
    rest: &[Row],
    statement: &TypedStatement,
) -> Result<WorkflowRunDetail, DetailRowError> {
    let run = run_summary(first, statement)?;
    let stored_trace_id: Option<Vec<u8>> = column(first, statement, "trace_id")?;
    let trace_id = stored_trace_id_text(stored_trace_id.as_deref())
        .map_err(|refused| shape(statement, format!("workflow run {}: {refused}", run.id)))?;
    let timing_rows: String = column(first, statement, "timing_rows")?;
    let timing_rows = match timing_rows.as_str() {
        "INCLUDED" => TimingRows::Included,
        "BEFORE_CAPTURE" => TimingRows::BeforeCapture,
        "BOUND_EXCEEDED" => TimingRows::BoundExceeded,
        other => {
            return Err(DetailRowError::Row(shape(
                statement,
                format!("unknown timing_rows {other:?}"),
            )))
        }
    };
    let rows = run_rows(rest, statement)?;
    let facts = RunFacts {
        as_of: column(first, statement, "as_of")?,
        captured_since: column(first, statement, "captured_since")?,
        created_at: column(first, statement, "created_at")?,
        completed_at: column(first, statement, "completed_at")?,
        records: timing_records(timing_rows, rows.events, rows.tasks, statement)?,
    };
    let node_facts: Vec<NodeFacts> = rows.nodes.iter().map(|node| node.facts).collect();
    let composed = compose_run(&facts, &node_facts);
    let nodes = rows
        .nodes
        .into_iter()
        .zip(composed.nodes)
        .map(|(node, timing)| node.into_run_node(timing))
        .collect();
    Ok(run_detail(run, nodes, trace_id, composed.run))
}

/// The `NODE`, `EVENT` and `TASK` rows, each kind in row order, at most
/// [`MAX_RUN_NODES`] nodes and [`MAX_RUN_EVENTS`] events.
fn run_rows(rows: &[Row], statement: &TypedStatement) -> Result<RunRows, DetailRowError> {
    let mut parts = RunRows::default();
    for row in rows {
        let kind: String = column(row, statement, "row_kind")?;
        match kind.as_str() {
            "NODE" => parts.nodes.push(node_row(row, statement)?),
            "EVENT" => parts.events.push(event_row(row, statement)?),
            "TASK" => parts.tasks.push(task_row(row, statement)?),
            other => {
                return Err(DetailRowError::Row(shape(
                    statement,
                    format!("a row of kind {other:?} follows the RUN row"),
                )))
            }
        }
    }
    match (
        parts.nodes.len() > MAX_RUN_NODES,
        parts.events.len() > MAX_RUN_EVENTS,
    ) {
        (false, false) => Ok(parts),
        (true, _) | (_, true) => Err(DetailRowError::Row(shape(
            statement,
            format!(
                "the run function answered {} nodes and {} events, more than its bounds {MAX_RUN_NODES} \
                 and {MAX_RUN_EVENTS}",
                parts.nodes.len(),
                parts.events.len()
            ),
        ))),
    }
}

/// One `EVENT` row, before the task row of a `TASK_MOVED` event is joined to
/// it.
struct EventRow {
    event_id: i64,
    occurred_at: DateTime<Utc>,
    kind: EventRowKind,
}

enum EventRowKind {
    WorkflowPaused,
    WorkflowResumed,
    NodeReady {
        task_index: i32,
    },
    NodeEnqueued {
        task_index: i32,
    },
    TaskMoved {
        task_index: i32,
        task_id: Uuid,
        move_kind: TerminalizationKind,
    },
}

fn event_row(row: &Row, statement: &TypedStatement) -> Result<EventRow, RowDecodeError> {
    let event_id: i64 = column(row, statement, "event_id")?;
    let event_kind: String = column(row, statement, "event_kind")?;
    let task_index: Option<i32> = column(row, statement, "task_index")?;
    let task_id: Option<Uuid> = column(row, statement, "task_id")?;
    let move_kind: Option<String> = column(row, statement, "terminalization_kind")?;
    let kind = match (event_kind.as_str(), task_index, task_id, move_kind) {
        ("WORKFLOW_PAUSED", None, None, None) => EventRowKind::WorkflowPaused,
        ("WORKFLOW_RESUMED", None, None, None) => EventRowKind::WorkflowResumed,
        ("NODE_READY", Some(task_index), None, None) => EventRowKind::NodeReady { task_index },
        ("NODE_ENQUEUED", Some(task_index), None, None) => {
            EventRowKind::NodeEnqueued { task_index }
        }
        ("TASK_MOVED", Some(task_index), Some(task_id), Some(stored)) => EventRowKind::TaskMoved {
            task_index,
            task_id,
            move_kind: TerminalizationKind::parse(&stored).ok_or_else(|| {
                shape(
                    statement,
                    format!("event {event_id} has the move kind {stored:?}"),
                )
            })?,
        },
        (other, _, _, _) => {
            return Err(shape(
                statement,
                format!(
                    "event {event_id} of kind {other:?} has columns that its kind does not take"
                ),
            ))
        }
    };
    Ok(EventRow {
        event_id,
        occurred_at: column(row, statement, "occurred_at")?,
        kind,
    })
}

/// One `TASK` row: a live task that a node links, the history row of a
/// moved task, or a moved task whose history row retention removed.
enum TaskRow {
    Live { task_id: Uuid, task: Box<LiveTask> },
    Moved { task_id: Uuid, record: MovedRecord },
}

fn task_times(row: &Row, statement: &TypedStatement) -> Result<TaskTimes, RowDecodeError> {
    Ok(TaskTimes {
        created_at: column(row, statement, "created_at")?,
        enqueued_at: column(row, statement, "enqueued_at")?,
        claimed_at: column(row, statement, "claimed_at")?,
        started_at: column(row, statement, "started_at")?,
    })
}

/// The attempts of a decoded archive, at most [`MAX_TASK_ATTEMPTS`].
fn task_attempts(task_id: Uuid, archive: AttemptArchive) -> Result<TaskAttempts, DetailRowError> {
    let open = archive.open();
    let records = archive.into_attempts();
    match records.len() > MAX_TASK_ATTEMPTS {
        true => Err(DetailRowError::StoredData(format!(
            "the attempt archive of task {task_id} holds {} attempts, more than {MAX_TASK_ATTEMPTS}",
            records.len()
        ))),
        false => Ok(TaskAttempts { records, open }),
    }
}

fn task_row(row: &Row, statement: &TypedStatement) -> Result<TaskRow, DetailRowError> {
    let place: String = column(row, statement, "location")?;
    let task_id: Uuid = column(row, statement, "task_id")?;
    match place.as_str() {
        "LIVE" => {
            let stored_status: String = column(row, statement, "status")?;
            let status = match stored_status.as_str() {
                "PENDING" => LiveStatus::Pending,
                "CLAIMED" => LiveStatus::Claimed,
                "RUNNING" => LiveStatus::Running,
                other => {
                    return Err(DetailRowError::Row(shape(
                        statement,
                        format!("live task {task_id} has the status {other:?}"),
                    )))
                }
            };
            let version: i16 = column(row, statement, "attempt_archive_version")?;
            let payload: Vec<u8> = column(row, statement, "attempt_snapshot")?;
            let archive = decode_live_attempt_archive(version, &payload)
                .map_err(|error| DetailRowError::StoredData(format!("task {task_id}: {error}")))?;
            Ok(TaskRow::Live {
                task_id,
                task: Box::new(LiveTask {
                    times: task_times(row, statement)?,
                    status,
                    attempts: task_attempts(task_id, archive)?,
                }),
            })
        }
        "HISTORY" => {
            let version: i16 = column(row, statement, "attempt_archive_version")?;
            let payload: Vec<u8> = column(row, statement, "attempt_snapshot")?;
            let digest: Vec<u8> = column(row, statement, "attempt_snapshot_digest")?;
            let archive = decode_attempt_archive(
                version,
                JSON_UTF8_CODEC,
                JSON_CONTENT_TYPE,
                &payload,
                &digest,
            )
            .map_err(|error| DetailRowError::StoredData(format!("task {task_id}: {error}")))?;
            Ok(TaskRow::Moved {
                task_id,
                record: MovedRecord::History(MovedTask {
                    times: task_times(row, statement)?,
                    attempts: task_attempts(task_id, archive)?,
                }),
            })
        }
        "PRUNED" => Ok(TaskRow::Moved {
            task_id,
            record: MovedRecord::Pruned,
        }),
        other => Err(DetailRowError::Row(shape(
            statement,
            format!("unknown task location {other:?}"),
        ))),
    }
}

/// The timing records of the run: none unless the `RUN` row says
/// `INCLUDED`. Each `TASK_MOVED` event takes the one moved task row of its
/// task; every moved task row belongs to one event, and no task id repeats.
/// Events come in strictly increasing `event_id` order.
fn timing_records(
    timing_rows: TimingRows,
    events: Vec<EventRow>,
    tasks: Vec<TaskRow>,
    statement: &TypedStatement,
) -> Result<TimingRecords, RowDecodeError> {
    let refused = |reason: String| shape(statement, reason);
    match (timing_rows, events.is_empty() && tasks.is_empty()) {
        (TimingRows::BeforeCapture, true) => return Ok(TimingRecords::BeforeCapture),
        (TimingRows::BoundExceeded, true) => return Ok(TimingRecords::BoundExceeded),
        (TimingRows::BeforeCapture | TimingRows::BoundExceeded, false) => {
            return Err(refused(format!(
                "timing rows {timing_rows:?} came with {} events and {} tasks",
                events.len(),
                tasks.len()
            )))
        }
        (TimingRows::Included, _) => {}
    }
    let mut live_tasks: HashMap<Uuid, LiveTask> = HashMap::new();
    let mut moved: HashMap<Uuid, MovedRecord> = HashMap::new();
    for task in tasks {
        let (task_id, repeated) = match task {
            TaskRow::Live { task_id, task } => {
                (task_id, live_tasks.insert(task_id, *task).is_some())
            }
            TaskRow::Moved { task_id, record } => {
                (task_id, moved.insert(task_id, record).is_some())
            }
        };
        if repeated || (live_tasks.contains_key(&task_id) && moved.contains_key(&task_id)) {
            return Err(refused(format!("task {task_id} has two task rows")));
        }
    }
    let mut run_events = Vec::with_capacity(events.len());
    let mut previous: Option<i64> = None;
    for event in events {
        if previous.is_some_and(|id| id >= event.event_id) {
            return Err(refused(format!(
                "event {} is out of event order",
                event.event_id
            )));
        }
        previous = Some(event.event_id);
        let kind = match event.kind {
            EventRowKind::WorkflowPaused => RunEventKind::WorkflowPaused,
            EventRowKind::WorkflowResumed => RunEventKind::WorkflowResumed,
            EventRowKind::NodeReady { task_index } => RunEventKind::Node {
                task_index,
                event: NodeEventKind::Ready,
            },
            EventRowKind::NodeEnqueued { task_index } => RunEventKind::Node {
                task_index,
                event: NodeEventKind::Enqueued,
            },
            EventRowKind::TaskMoved {
                task_index,
                task_id,
                move_kind,
            } => {
                let record = moved.remove(&task_id).ok_or_else(|| {
                    refused(format!(
                        "TASK_MOVED event {} has no task row",
                        event.event_id
                    ))
                })?;
                RunEventKind::Node {
                    task_index,
                    event: NodeEventKind::TaskMoved(Box::new(TaskMove {
                        task_id,
                        move_kind,
                        record,
                    })),
                }
            }
        };
        run_events.push(RunEvent {
            event_id: event.event_id,
            occurred_at: event.occurred_at,
            kind,
        });
    }
    match moved.keys().next() {
        Some(task_id) => Err(refused(format!(
            "moved task {task_id} has no TASK_MOVED event"
        ))),
        None => Ok(TimingRecords::Included {
            events: run_events,
            live_tasks,
        }),
    }
}

/// The error text of a node: the message of the error that its stored
/// result holds. The node `error` column has no writer (the result holds
/// the error). No result (NULL), a result with a value, and an error with
/// no message give `None`. Errors: a result that does not decode, as the
/// reason, naming the node. An empty text is such a result: no writer
/// stores it, so it is corrupt data, not "no result".
fn node_error_from_result(task_index: i32, result: Option<&str>) -> Result<Option<String>, String> {
    let text = match result {
        None => return Ok(None),
        Some("") => {
            return Err(format!(
                "stored result of workflow node {task_index} does not decode: the stored text \
                 is empty"
            ))
        }
        Some(text) => text,
    };
    // Python `_node_error` reads the text with `loads_json`: the JSON value
    // `null` is no result.
    if text.trim_matches([' ', '\t', '\n', '\r']) == "null" {
        return Ok(None);
    }
    // The value is not read: the envelope is checked and the ok slot skipped.
    match serde_json::from_str::<TaskResult<serde::de::IgnoredAny>>(text) {
        Ok(TaskResult::Ok(_)) => Ok(None),
        Ok(TaskResult::Err(error)) => Ok(normalize_optional_text(error.message.as_deref())),
        Err(cause) => Err(format!(
            "stored result of workflow node {task_index} does not decode: {cause}"
        )),
    }
}

/// The answer of the node function.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum NodeRead {
    Found(Box<WorkflowTaskDetail>),
    /// The run has no node at the index.
    Absent,
}

/// Reads the one row of the node function.
pub(super) fn workflow_node(
    rows: &[Row],
    statement: &TypedStatement,
) -> Result<NodeRead, DetailRowError> {
    let [row] = rows else {
        return Err(DetailRowError::Row(shape(
            statement,
            format!("the node function answered {} rows", rows.len()),
        )));
    };
    let kind: String = column(row, statement, "row_kind")?;
    match kind.as_str() {
        "ABSENT" => Ok(NodeRead::Absent),
        "NODE" => node_detail(row, statement).map(|detail| NodeRead::Found(Box::new(detail))),
        other => Err(DetailRowError::Row(shape(
            statement,
            format!("unknown row_kind {other:?}"),
        ))),
    }
}

/// A `NODE` row: the node, its error, and its backing task with the task's
/// attempts. A node with no backing task (a sub-workflow node, or a task
/// node before its task exists: `location` is `NULL`) and a node whose task
/// is in neither place (`ABSENT`) have no leaf and no attempt.
fn node_detail(
    row: &Row,
    statement: &TypedStatement,
) -> Result<WorkflowTaskDetail, DetailRowError> {
    let task_index: i32 = column(row, statement, "node_task_index")?;
    let node_result: Option<String> = column(row, statement, "node_result")?;
    let node_error = node_error_from_result(task_index, node_result.as_deref())
        .map_err(DetailRowError::StoredData)?;
    let location: Option<String> = column(row, statement, "location")?;
    let (leaf, attempts) = match location {
        None => (None, Vec::new()),
        Some(_) => match task_detail_row(row, statement)? {
            TaskDetailRead::Found(detail) => (Some(detail.leaf), detail.attempts),
            TaskDetailRead::NotFound(_) => (None, Vec::new()),
        },
    };
    Ok(WorkflowTaskDetail {
        task_index,
        node_id: column(row, statement, "node_id")?,
        task_name: column(row, statement, "node_task_name")?,
        node_status: column(row, statement, "node_status")?,
        is_subworkflow: column(row, statement, "node_is_subworkflow")?,
        node_error,
        leaf,
        attempts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitoring::timing_models::UnavailableReason;

    fn fields(
        location: Location,
        status: &str,
        terminal_at: Option<DateTime<Utc>>,
    ) -> SummaryFields {
        let enqueued_at = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).expect("valid time");
        SummaryFields {
            location,
            task_id: Uuid::nil(),
            task_name: "t".to_owned(),
            queue_name: "default".to_owned(),
            priority: 100,
            status: status.to_owned(),
            retry_count: 0,
            max_retries: 0,
            is_workflow_task: false,
            error_code: Some("TASK_EXCEPTION".to_owned()),
            worker_id: Some("w1".to_owned()),
            worker_hostname: Some("host".to_owned()),
            enqueued_at,
            started_at: Some(enqueued_at + chrono::Duration::seconds(3)),
            terminal_at,
        }
    }

    #[test]
    fn a_history_row_has_its_terminal_time_as_completed_or_failed() {
        let enqueued = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).expect("valid time");
        let terminal = enqueued + chrono::Duration::seconds(10);
        let completed = summary_from_fields(fields(Location::History, "COMPLETED", Some(terminal)))
            .expect("history summary");
        assert_eq!(completed.completed_at, Some(terminal));
        assert_eq!(completed.failed_at, None);
        assert_eq!(completed.queue_s, Some(TimeDelta::seconds(3)));
        assert_eq!(completed.exec_s, Some(TimeDelta::seconds(7)));
        assert_eq!(completed.error_code.as_deref(), Some("TASK_EXCEPTION"));

        let failed = summary_from_fields(fields(Location::History, "FAILED", Some(terminal)))
            .expect("history summary");
        assert_eq!(failed.failed_at, Some(terminal));
        assert_eq!(failed.completed_at, None);
    }

    #[test]
    fn a_live_row_has_no_terminal_time() {
        let live = summary_from_fields(fields(Location::Live, "RUNNING", None)).expect("live");
        assert_eq!(live.completed_at, None);
        assert_eq!(live.failed_at, None);
        assert_eq!(live.queue_s, Some(TimeDelta::seconds(3)));
        assert!(live.exec_s.is_some());

        assert!(summary_from_fields(fields(Location::Live, "RUNNING", Some(Utc::now()))).is_err());
        assert!(summary_from_fields(fields(Location::History, "FAILED", None)).is_err());
    }

    /// The spans of a row keep the microseconds of the stored times, in the
    /// row and in its JSON: 2.310417 s of queue wait and 3.189583 s of run
    /// time, not 2 s and 3 s.
    #[test]
    fn the_spans_of_a_row_keep_the_microseconds_of_the_stored_times() {
        let enqueued = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).expect("valid time");
        let terminal = enqueued + TimeDelta::microseconds(5_500_000);
        let mut stored = fields(Location::History, "COMPLETED", Some(terminal));
        stored.started_at = Some(enqueued + TimeDelta::microseconds(2_310_417));

        let summary = summary_from_fields(stored).expect("history summary");

        assert_eq!(summary.queue_s, Some(TimeDelta::microseconds(2_310_417)));
        assert_eq!(summary.exec_s, Some(TimeDelta::microseconds(3_189_583)));
        let encoded = serde_json::to_value(&summary).expect("the row serializes");
        assert_eq!(encoded["queue_s"], serde_json::json!(2.310417));
        assert_eq!(encoded["exec_s"], serde_json::json!(3.189583));
    }

    #[test]
    fn the_page_has_one_totals_row() {
        let summary = summary_from_fields(fields(Location::Live, "PENDING", None)).expect("live");
        let (totals, rows) = page_from_rows(vec![
            TaskListRow::Totals(ListTotals::EstimateHistoryEmpty { live: 4 }),
            TaskListRow::Task(Box::new(summary.clone())),
        ])
        .expect("page");
        assert_eq!(totals.total(), Ok(4));
        assert_eq!(rows, vec![summary]);

        assert!(page_from_rows(Vec::new()).is_err());
        assert!(page_from_rows(vec![
            TaskListRow::Totals(ListTotals::Exact {
                live: 1,
                history: 2
            }),
            TaskListRow::Totals(ListTotals::Exact {
                live: 1,
                history: 2
            }),
        ])
        .is_err());
    }

    #[test]
    fn totals_add_the_live_and_history_sides() {
        assert_eq!(
            ListTotals::Exact {
                live: 3,
                history: 4
            }
            .total(),
            Ok(7)
        );
        assert_eq!(
            ListTotals::EstimateHistoryPlan {
                live: 3,
                plan: r#"[{"Plan": {"Plan Rows": 40}}]"#.to_owned(),
            }
            .total(),
            Ok(43)
        );
        assert!(ListTotals::EstimateHistoryPlan {
            live: 3,
            plan: "not json".to_owned(),
        }
        .total()
        .is_err());
    }

    #[test]
    fn the_taxonomy_arrays_are_parallel_and_hold_only_built_in_categories() {
        let taxonomy = builtin_taxonomy();
        assert_eq!(taxonomy.codes.len(), taxonomy.categories.len());
        assert!(!taxonomy.codes.is_empty());
        for category in &taxonomy.categories {
            assert!(
                ["OPERATIONAL", "CONTRACT", "RETRIEVAL", "OUTCOME"].contains(&category.as_str())
            );
        }
        let mut unique = taxonomy.codes.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), taxonomy.codes.len());
    }

    fn counts(total: i64) -> BreakdownCounts {
        BreakdownCounts {
            total,
            pending: total,
            claimed: 0,
            running: 0,
            completed: 0,
            failed: 0,
            cancelled: 0,
            expired: 0,
            retried: 0,
        }
    }

    fn group(value: Option<&str>, total: i64) -> BreakdownRow {
        BreakdownRow::Group(GroupRow {
            counts: counts(total),
            group: match value {
                Some(value) => BreakdownGroup::Value {
                    value: value.to_owned(),
                },
                None => BreakdownGroup::Unclaimed,
            },
        })
    }

    #[test]
    fn the_breakdown_is_the_total_row_then_the_groups_in_their_order() {
        let breakdown = breakdown_from_rows(
            TaskGroupBy::Worker,
            vec![
                BreakdownRow::Total {
                    counts: counts(5),
                    group_count: 3,
                },
                group(Some("w1"), 3),
                group(None, 2),
            ],
        )
        .expect("rows in contract order");

        assert_eq!(breakdown.group_by, "worker");
        assert_eq!(breakdown.total, counts(5));
        assert_eq!(breakdown.group_count, 3);
        assert_eq!(
            breakdown
                .groups
                .iter()
                .map(|row| row.group.clone())
                .collect::<Vec<_>>(),
            vec![
                BreakdownGroup::Value {
                    value: "w1".to_owned()
                },
                BreakdownGroup::Unclaimed,
            ]
        );
    }

    #[test]
    fn breakdown_rows_out_of_contract_are_refused() {
        let total = || BreakdownRow::Total {
            counts: counts(0),
            group_count: 0,
        };
        assert!(breakdown_from_rows(TaskGroupBy::Queue, Vec::new()).is_err());
        assert!(breakdown_from_rows(TaskGroupBy::Queue, vec![group(Some("q"), 1)]).is_err());
        assert!(breakdown_from_rows(TaskGroupBy::Queue, vec![total(), total()]).is_err());
    }

    fn run() -> WorkflowRunSummary {
        WorkflowRunSummary {
            id: Uuid::nil(),
            name: "flow".to_owned(),
            definition_key: None,
            status: "RUNNING".to_owned(),
            created_at: None,
            completed_at: None,
            wall_s: None,
        }
    }

    fn node(task_index: i32, status: &str, dependencies: Vec<i32>) -> RunNode {
        RunNode {
            info: WorkflowNodeInfo {
                task_index,
                node_id: None,
                task_name: "t".to_owned(),
                node_status: status.to_owned(),
                is_subworkflow: false,
                sub_workflow_id: None,
                allow_failed_dependencies: false,
                started_at: None,
                completed_at: None,
                exec_s: None,
                child_total: None,
                child_failed: None,
                timing: NodeTimingInfo::Unavailable {
                    reason: UnavailableReason::RunBeforeCapture,
                },
            },
            dependencies,
        }
    }

    fn unavailable_run() -> RunTimingInfo {
        let at = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).expect("valid time");
        RunTimingInfo::Unavailable {
            as_of: at,
            captured_since: at,
            reason: UnavailableReason::RunBeforeCapture,
        }
    }

    /// Python `TestGetWorkflowRun::test_nodes_ordered_and_edges_derived_from_dependencies`
    /// and `::test_dependency_on_a_missing_index_is_dropped`, on the rows.
    #[test]
    fn edges_join_known_dependencies_and_failed_nodes_are_listed() {
        let detail = run_detail(
            run(),
            vec![
                node(0, "COMPLETED", vec![]),
                node(1, "FAILED", vec![0]),
                node(2, "FAILED", vec![0, 1, 99]),
            ],
            None,
            unavailable_run(),
        );

        assert_eq!(
            detail
                .edges
                .iter()
                .map(|edge| (edge.from_index, edge.to_index))
                .collect::<Vec<_>>(),
            vec![(0, 1), (0, 2), (1, 2)]
        );
        assert_eq!(detail.failed_indices, vec![1, 2]);
        assert_eq!(detail.failed_count, 2);
        assert_eq!(
            detail
                .nodes
                .iter()
                .map(|node| node.task_index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    }

    #[test]
    fn a_node_runs_until_it_finishes_and_only_a_finished_node_is_closed() {
        let started = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).expect("valid time");
        let completed = started + chrono::Duration::seconds(30);

        assert!(node_exec_s("RUNNING", Some(started), None)
            .is_some_and(|running| running > TimeDelta::seconds(30)));
        for status in ["COMPLETED", "FAILED", "SKIPPED", "CANCELLED"] {
            assert_eq!(
                node_exec_s(status, Some(started), Some(completed)),
                Some(TimeDelta::seconds(30)),
                "{status}"
            );
            assert_eq!(node_exec_s(status, Some(started), None), None, "{status}");
        }
        assert_eq!(
            node_exec_s("ENQUEUED", Some(started), Some(completed)),
            None
        );
    }

    fn stored(result: &TaskResult<serde_json::Value>) -> String {
        serde_json::to_string(result).expect("encode a stored result")
    }

    #[test]
    fn a_failed_node_shows_the_message_of_its_stored_error() {
        let failed = stored(&TaskResult::Err(
            crate::core::task::error::TaskError::built_in(
                OutcomeCode::UpstreamSkipped,
                "node blew up",
            ),
        ));

        assert_eq!(
            node_error_from_result(0, Some(&failed)).expect("decodes"),
            Some("node blew up".to_owned())
        );
    }

    #[test]
    fn no_result_a_value_and_a_blank_message_show_no_error() {
        let value = stored(&TaskResult::Ok(serde_json::json!({"rows": 3})));
        let blank = stored(&TaskResult::Err(
            crate::core::task::error::TaskError::built_in(OutcomeCode::UpstreamSkipped, "  "),
        ));

        assert_eq!(node_error_from_result(0, None).expect("decodes"), None);
        for no_result in ["null", " null\n"] {
            assert_eq!(
                node_error_from_result(0, Some(no_result)).expect("no result"),
                None,
                "{no_result:?}"
            );
        }
        assert_eq!(
            node_error_from_result(0, Some(&value)).expect("decodes"),
            None
        );
        assert_eq!(
            node_error_from_result(0, Some(&blank)).expect("decodes"),
            None
        );
    }

    /// A stored result that does not decode is refused, naming the node; the
    /// node read reports it as a broken history contract.
    #[test]
    fn a_stored_result_that_does_not_decode_is_refused_naming_the_node() {
        let reason = node_error_from_result(4, Some("not json"))
            .expect_err("a result that is not JSON is refused");

        assert!(reason.contains("workflow node 4"), "{reason}");
    }

    /// No writer stores an empty result text, so a stored one is corrupt
    /// data: it is refused as a result that does not decode, naming the node
    /// and the cause. NULL stays "no result". Python `_node_error` refuses it
    /// too (`loads_json` refuses an empty text).
    #[test]
    fn an_empty_stored_result_is_refused_naming_the_node() {
        let reason =
            node_error_from_result(5, Some("")).expect_err("an empty result text is refused");

        assert_eq!(
            reason,
            "stored result of workflow node 5 does not decode: the stored text is empty"
        );
        assert_eq!(node_error_from_result(5, None).expect("decodes"), None);
    }
}
