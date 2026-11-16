//! The monitoring reads that call the baseline functions (`0007`):
//! `horsies_monitoring_task_list`, `horsies_monitoring_task_facets` and
//! `horsies_monitoring_task_detail`. The statements, their typed parameters,
//! and the decoding of their rows into the monitoring models.
//!
//! The functions own the SQL: filters, sort, page bounds, totals, the facet
//! caps, the detail snapshot and the absence rule. The client passes typed
//! values and reads plain rows.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::core_rows::{column, RowDecodeError};
use crate::core::history::archive::attempts::{
    decode_attempt_snapshot, decode_live_attempt_snapshot, AttemptRecord,
};
use crate::core::history::archive::versions::{JSON_CONTENT_TYPE, JSON_UTF8_CODEC};
use crate::core::history::reads::aggregates::plan_rows_from_explain_text;
use crate::core::task::error::{ContractCode, OperationalErrorCode, OutcomeCode, RetrievalCode};
use crate::core::types::status::TaskStatus;
use crate::db::{ParameterType, Row, TypedStatement};

use super::models::{ErrorCategory, LeafTaskInfo, TaskAttemptInfo, TaskDetail, TaskSummary};
use super::queries::{normalize_optional_text, span_s};

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

/// One task with its attempts and workflow link. Parameters: task id, with
/// result, with attempts.
pub(super) static MONITORING_TASK_DETAIL: TypedStatement = TypedStatement::new(
    "monitoring_task_detail",
    "SELECT location, task_id, task_name, queue_name, priority, status, is_workflow_task,
            retry_count, max_retries, enqueued_at, started_at, good_until, worker_hostname,
            terminal_at, error_code, final_failed_reason, attempt_archive_version,
            attempt_snapshot, attempt_snapshot_digest, workflow_id, workflow_task_index
     FROM horsies_monitoring_task_detail($1, $2, $3)",
    &[
        ParameterType::Uuid,
        ParameterType::Bool,
        ParameterType::Bool,
    ],
);

/// Whether the detail function and the published reader it calls exist.
pub(super) static DETAIL_FUNCTIONS_PUBLISHED: TypedStatement = TypedStatement::new(
    "monitoring_detail_functions_published",
    "SELECT to_regprocedure('horsies_monitoring_task_detail(uuid,boolean,boolean)') IS NOT NULL
                AS detail_function,
            to_regprocedure('horsies_task_detail_staged(uuid)') IS NOT NULL AS staged_reader",
    &[],
);

/// The built-in error-code taxonomy as the two parallel arrays the functions
/// take: code `i` belongs to category `i`. A code outside it is a `DOMAIN`
/// code.
pub(super) struct BuiltinTaxonomy {
    pub(super) codes: Vec<String>,
    pub(super) categories: Vec<String>,
}

pub(super) fn builtin_taxonomy() -> BuiltinTaxonomy {
    let families: [(ErrorCategory, Vec<String>); 4] = [
        (
            ErrorCategory::Operational,
            OperationalErrorCode::ALL
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
    Task(TaskSummary),
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
                .map(TaskListRow::Task)
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
            span_s(
                Some(fields.enqueued_at),
                fields.started_at,
                matches!(fields.status.as_str(), "PENDING" | "CLAIMED"),
            ),
            span_s(fields.started_at, None, fields.status.as_str() == "RUNNING"),
        ),
        (Location::History, Some(terminal)) => (
            (fields.status == "COMPLETED").then_some(terminal),
            (fields.status != "COMPLETED").then_some(terminal),
            span_s(
                Some(fields.enqueued_at),
                fields.started_at.or(Some(terminal)),
                false,
            ),
            span_s(fields.started_at, Some(terminal), false),
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
            (TaskListRow::Task(summary), _) => summaries.push(summary),
        }
    }
    let totals = totals.ok_or_else(|| "the task list returned no TOTALS row".to_owned())?;
    Ok((totals, summaries))
}

/// The answer of the detail function.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum DetailRead {
    Found(TaskDetail),
    /// Neither the live table nor the history holds the task.
    Absent,
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

/// Reads the one row of the detail function.
pub(super) fn task_detail_row(
    row: &Row,
    statement: &TypedStatement,
) -> Result<DetailRead, DetailRowError> {
    let place: String = column(row, statement, "location")?;
    let location = match place.as_str() {
        "ABSENT" => return Ok(DetailRead::Absent),
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
            span_s(
                Some(enqueued_at),
                started_at,
                matches!(status.as_str(), "PENDING" | "CLAIMED"),
            ),
            span_s(started_at, None, status.as_str() == "RUNNING"),
        ),
        (Location::History, Some(terminal)) => {
            if let Err(reason) = status.parse::<TaskStatus>() {
                return Err(DetailRowError::StoredData(format!(
                    "history row of task {task_id} has an unknown status: {reason}"
                )));
            }
            (
                (status == "COMPLETED").then_some(terminal),
                (status != "COMPLETED").then_some(terminal),
                span_s(Some(enqueued_at), started_at.or(Some(terminal)), false),
                span_s(started_at, Some(terminal), false),
            )
        }
        (Location::Live, Some(_)) | (Location::History, None) => {
            return Err(DetailRowError::Row(shape(
                statement,
                format!("task {task_id}: the terminal time does not match its location"),
            )))
        }
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
    };
    Ok(DetailRead::Found(TaskDetail {
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
    }))
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
    }
}

/// A detail call that met an undefined function: whether the database
/// lacks only the published reader (the detail answers "not found", as
/// before the first coverage pass) or the detail function itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DetailPublication {
    /// The detail function exists and the published reader does not.
    ReaderNotPublished,
    /// Anything else: the refusal stands.
    RefusalStands,
}

pub(super) fn detail_publication(detail_function: bool, staged_reader: bool) -> DetailPublication {
    match (detail_function, staged_reader) {
        (true, false) => DetailPublication::ReaderNotPublished,
        (true, true) | (false, _) => DetailPublication::RefusalStands,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(completed.queue_s, Some(3));
        assert_eq!(completed.exec_s, Some(7));
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
        assert_eq!(live.queue_s, Some(3));
        assert!(live.exec_s.is_some());

        assert!(summary_from_fields(fields(Location::Live, "RUNNING", Some(Utc::now()))).is_err());
        assert!(summary_from_fields(fields(Location::History, "FAILED", None)).is_err());
    }

    #[test]
    fn the_page_has_one_totals_row() {
        let summary = summary_from_fields(fields(Location::Live, "PENDING", None)).expect("live");
        let (totals, rows) = page_from_rows(vec![
            TaskListRow::Totals(ListTotals::EstimateHistoryEmpty { live: 4 }),
            TaskListRow::Task(summary.clone()),
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

    #[test]
    fn only_a_missing_reader_answers_not_found() {
        assert_eq!(
            detail_publication(true, false),
            DetailPublication::ReaderNotPublished
        );
        assert_eq!(
            detail_publication(true, true),
            DetailPublication::RefusalStands
        );
        assert_eq!(
            detail_publication(false, false),
            DetailPublication::RefusalStands
        );
    }
}
