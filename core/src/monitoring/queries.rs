//! Monitoring query implementations: each read is one call of its baseline
//! function (`0007_scheduler_monitoring_functions.sql`) on the statement
//! layer, in autocommit, so every read is one snapshot.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeDelta, Utc};
use tokio_postgres::error::SqlState;
use uuid::Uuid;

use crate::broker::core_rows::{column, one_row, RowDecodeError};
use crate::broker::task_lists::{bounded_list_from_rows, BoundedTaskList, ListRow, ListShapeError};
use crate::broker::{BrokerError, PostgresBroker};
use crate::db::{Row as CoreRow, StatementFailure, TypedStatement};

use super::function_rows::{
    breakdown_from_rows, breakdown_row, builtin_taxonomy, page_from_rows, schedule_row,
    status_counts, task_detail_row, task_list_row, workflow_name_row, workflow_node, workflow_run,
    workflow_runs, DetailRowError, NodeRead, RunRead, MONITORING_SCHEDULES,
    MONITORING_TASK_BREAKDOWN, MONITORING_TASK_DETAIL, MONITORING_TASK_FACETS,
    MONITORING_TASK_LIST, MONITORING_TASK_STATS, MONITORING_WORKFLOW_NAMES,
    MONITORING_WORKFLOW_NODE, MONITORING_WORKFLOW_RUN, MONITORING_WORKFLOW_RUNS,
};
use crate::core::history::errors::HistoryError;
use crate::core::history::reads::pages::HistoryWindow;
use crate::core::task::error::BuiltInTaskCode;
use crate::core::types::status::TaskStatus;

use super::errors::{MonitoringQueryError, MonitoringResult};
use super::models::{
    Breakdown, ErrorCategory, ErrorFacet, FacetValue, Facets, ScheduleStateInfo, StatusCount,
    TaskDetailRead, TaskListPage, WorkflowRunDetail, WorkflowRunSummary, WorkflowTaskDetail,
};

/// The most `offset + limit` of one task list page.
pub const MAX_TASK_PAGE_REACH: i64 = 500;
/// The most rows of one task list page.
pub const MAX_TASK_PAGE_LIMIT: i64 = 200;
/// The most groups of one task breakdown.
pub const MAX_BREAKDOWN_LIMIT: i64 = 500;
/// The most runs of one workflow runs list.
pub const MAX_WORKFLOW_RUNS_LIMIT: i64 = 200;
/// The most rows of the workflow names list and of the schedules list: the
/// row bound (`c_row_bound`) of `horsies_monitoring_workflow_names` and
/// `horsies_monitoring_schedules`. A longer list is cut at the bound.
pub const LIST_ROW_BOUND: usize = 10_000;
const ERROR_FACET_CAP: i64 = 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskSortField {
    EnqueuedAt,
    StartedAt,
    CompletedAt,
    FailedAt,
    Status,
    TaskName,
    QueueName,
    Priority,
    RetryCount,
    QueueSeconds,
    ExecutionSeconds,
}

impl TaskSortField {
    pub const ALL: [Self; 11] = [
        Self::EnqueuedAt,
        Self::StartedAt,
        Self::CompletedAt,
        Self::FailedAt,
        Self::Status,
        Self::TaskName,
        Self::QueueName,
        Self::Priority,
        Self::RetryCount,
        Self::QueueSeconds,
        Self::ExecutionSeconds,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EnqueuedAt => "enqueued_at",
            Self::StartedAt => "started_at",
            Self::CompletedAt => "completed_at",
            Self::FailedAt => "failed_at",
            Self::Status => "status",
            Self::TaskName => "task_name",
            Self::QueueName => "queue_name",
            Self::Priority => "priority",
            Self::RetryCount => "retry_count",
            Self::QueueSeconds => "queue_s",
            Self::ExecutionSeconds => "exec_s",
        }
    }
}

impl std::fmt::Display for TaskSortField {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

impl SortDirection {
    pub const ALL: [Self; 2] = [Self::Ascending, Self::Descending];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ascending => "asc",
            Self::Descending => "desc",
        }
    }

    const fn sql(self) -> &'static str {
        match self {
            Self::Ascending => "ASC",
            Self::Descending => "DESC",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskGroupBy {
    Worker,
    TaskName,
    Queue,
}

impl TaskGroupBy {
    pub const ALL: [Self; 3] = [Self::Worker, Self::TaskName, Self::Queue];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Worker => "worker",
            Self::TaskName => "task_name",
            Self::Queue => "queue",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaskFilters {
    pub statuses: Vec<TaskStatus>,
    pub task_names: Vec<String>,
    pub queues: Vec<String>,
    pub workers: Vec<String>,
    pub error_codes: Vec<String>,
    pub error_categories: Vec<ErrorCategory>,
    pub retried_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{reason}")]
pub struct PaginationRefused {
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskStatsQuery {
    window: HistoryWindow,
    filters: TaskFilters,
}

impl TaskStatsQuery {
    pub fn new(window: HistoryWindow) -> Self {
        Self {
            window,
            filters: TaskFilters::default(),
        }
    }

    pub fn with_filters(mut self, mut filters: TaskFilters) -> Self {
        filters.statuses.clear();
        self.filters = filters;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskFacetsQuery {
    window: HistoryWindow,
    filters: TaskFilters,
}

impl TaskFacetsQuery {
    pub fn new(window: HistoryWindow) -> Self {
        Self {
            window,
            filters: TaskFilters::default(),
        }
    }

    pub fn with_filters(mut self, filters: TaskFilters) -> Self {
        self.filters = filters;
        self
    }

    pub fn with_statuses(mut self, statuses: Vec<TaskStatus>) -> Self {
        self.filters.statuses = statuses;
        self
    }

    pub fn with_error_categories(mut self, categories: Vec<ErrorCategory>) -> Self {
        self.filters.error_categories = categories;
        self
    }

    pub fn retried_only(mut self, enabled: bool) -> Self {
        self.filters.retried_only = enabled;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskBreakdownQuery {
    window: HistoryWindow,
    group_by: TaskGroupBy,
    filters: TaskFilters,
    limit: i64,
}

impl TaskBreakdownQuery {
    pub fn new(window: HistoryWindow, group_by: TaskGroupBy) -> Self {
        Self {
            window,
            group_by,
            filters: TaskFilters::default(),
            limit: 50,
        }
    }

    pub fn with_filters(mut self, filters: TaskFilters) -> Self {
        self.filters = filters;
        self
    }

    pub fn with_limit(mut self, limit: i64) -> Result<Self, PaginationRefused> {
        if !(1..=MAX_BREAKDOWN_LIMIT).contains(&limit) {
            return Err(PaginationRefused {
                reason: format!("limit must be between 1 and {MAX_BREAKDOWN_LIMIT}; got {limit}"),
            });
        }
        self.limit = limit;
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskListQuery {
    window: HistoryWindow,
    filters: TaskFilters,
    sort_by: TaskSortField,
    sort_direction: SortDirection,
    offset: i64,
    limit: i64,
}

impl TaskListQuery {
    pub fn new(window: HistoryWindow) -> Self {
        Self {
            window,
            filters: TaskFilters::default(),
            sort_by: TaskSortField::EnqueuedAt,
            sort_direction: SortDirection::Descending,
            offset: 0,
            limit: 50,
        }
    }

    pub fn with_filters(mut self, filters: TaskFilters) -> Self {
        self.filters = filters;
        self
    }

    pub fn with_sort(mut self, field: TaskSortField, direction: SortDirection) -> Self {
        self.sort_by = field;
        self.sort_direction = direction;
        self
    }

    pub fn with_pagination(mut self, offset: i64, limit: i64) -> Result<Self, PaginationRefused> {
        if offset < 0 {
            return Err(PaginationRefused {
                reason: format!("offset must be at least 0; got {offset}"),
            });
        }
        if !(1..=MAX_TASK_PAGE_LIMIT).contains(&limit) {
            return Err(PaginationRefused {
                reason: format!("limit must be between 1 and {MAX_TASK_PAGE_LIMIT}; got {limit}"),
            });
        }
        let reach = offset.checked_add(limit).ok_or_else(|| PaginationRefused {
            reason: "offset + limit exceeds the supported integer range".to_owned(),
        })?;
        if reach > MAX_TASK_PAGE_REACH {
            return Err(PaginationRefused {
                reason: format!("offset + limit must be <= {MAX_TASK_PAGE_REACH}; got {reach}"),
            });
        }
        self.offset = offset;
        self.limit = limit;
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowRunsQuery {
    name: Option<String>,
    status: Option<String>,
    limit: i64,
}

impl WorkflowRunsQuery {
    pub fn new() -> Self {
        Self {
            name: None,
            status: None,
            limit: 30,
        }
    }

    pub fn with_name(mut self, name: Option<String>) -> Self {
        self.name = name;
        self
    }

    pub fn with_status(mut self, status: Option<String>) -> Self {
        self.status = status;
        self
    }

    pub fn with_limit(mut self, limit: i64) -> Result<Self, PaginationRefused> {
        if !(1..=MAX_WORKFLOW_RUNS_LIMIT).contains(&limit) {
            return Err(PaginationRefused {
                reason: format!(
                    "limit must be between 1 and {MAX_WORKFLOW_RUNS_LIMIT}; got {limit}"
                ),
            });
        }
        self.limit = limit;
        Ok(self)
    }
}

impl Default for WorkflowRunsQuery {
    fn default() -> Self {
        Self::new()
    }
}

pub fn normalize_optional_text(value: Option<&str>) -> Option<String> {
    value.and_then(|text| {
        let trimmed = text.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    })
}

/// The time from `start` to `end`, or to now (the host clock) when `end` is
/// `None`; `None` when `start` is `None`. Exact: the time between two
/// stored times keeps their microseconds. Negative when `end` is before
/// `start`.
pub fn elapsed(start: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>) -> Option<TimeDelta> {
    let start = start?;
    Some(end.unwrap_or_else(Utc::now) - start)
}

/// The span from `start` to `end`. With no `end`, the span of a `live` task
/// runs to now (the host clock), and any other task has no span. `None`
/// when `start` is `None`. Exact and signed, as [`elapsed`].
pub fn span(
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
    live: bool,
) -> Option<TimeDelta> {
    match (start, end) {
        (None, _) => None,
        (Some(start), Some(end)) => Some(end - start),
        (Some(start), None) if live => Some(Utc::now() - start),
        (Some(_), None) => None,
    }
}

pub fn categorize_error_code(code: Option<&str>) -> Option<ErrorCategory> {
    let normalized = normalize_optional_text(code)?;
    match BuiltInTaskCode::parse(&normalized) {
        Some(BuiltInTaskCode::Operational(_)) => Some(ErrorCategory::Operational),
        Some(BuiltInTaskCode::Contract(_)) => Some(ErrorCategory::Contract),
        Some(BuiltInTaskCode::Retrieval(_)) => Some(ErrorCategory::Retrieval),
        Some(BuiltInTaskCode::Outcome(_)) => Some(ErrorCategory::Outcome),
        None => Some(ErrorCategory::Domain),
    }
}

/// The stored text of each error category filter value.
fn category_texts(categories: &[ErrorCategory]) -> Vec<String> {
    categories
        .iter()
        .map(|category| category.as_str().to_owned())
        .collect()
}

/// A failed call of a monitoring function that raises `data_corrupted`
/// (XX001) for stored data that breaks the history contract: that refusal
/// is `HISTORY_CONTRACT_VIOLATION`, not retryable; any other failure is the
/// database failure (Python `_function_err`).
fn function_failure(operation: &str, error: BrokerError) -> MonitoringQueryError {
    match is_data_corrupted(&error) {
        true => MonitoringQueryError::broker(
            operation,
            BrokerError::HistoryReadContract(error.to_string()),
        ),
        false => MonitoringQueryError::broker(operation, error),
    }
}

/// Whether the server refused the statement with `data_corrupted` (XX001).
fn is_data_corrupted(error: &BrokerError) -> bool {
    match error {
        BrokerError::Statement(StatementFailure::StatementRefused { cause, .. }) => {
            cause.server_error().code() == &SqlState::DATA_CORRUPTED
        }
        _ => false,
    }
}

/// Rows of a monitoring function that do not decode: `DB_OPERATION_FAILED`,
/// not retryable.
fn row_failure(operation: &str, error: RowDecodeError) -> MonitoringQueryError {
    MonitoringQueryError::broker(operation, BrokerError::RowDecode(error))
}

/// Rows of a list function that break its `LIST_END` contract.
fn list_shape_failure(
    operation: &str,
    statement: &TypedStatement,
    shape: ListShapeError,
) -> MonitoringQueryError {
    row_failure(
        operation,
        RowDecodeError::Shape {
            statement: statement.name(),
            reason: shape.to_string(),
        },
    )
}

/// A task row that does not decode: a row shape defect, or stored history
/// data that does not decode (`HISTORY_CONTRACT_VIOLATION`).
fn detail_row_failure(operation: &str, error: DetailRowError) -> MonitoringQueryError {
    match error {
        DetailRowError::Row(error) => row_failure(operation, error),
        DetailRowError::StoredData(reason) => {
            MonitoringQueryError::history(operation, HistoryError::contract(reason))
        }
    }
}

/// The rows of a bounded list. A list that the function cut at its bound is
/// logged as a warning (Python `_log_cut_list`).
fn list_items<T>(list: BoundedTaskList<T>, read: &str, items: &str) -> Vec<T> {
    match list {
        BoundedTaskList::Complete(rows) => rows,
        BoundedTaskList::Truncated { rows, bound } => {
            tracing::warn!(
                read,
                bound,
                "{read}: the list is cut at {bound} {items}; more exist and are not returned"
            );
            rows
        }
    }
}

/// One row of the facets function (`horsies_monitoring_task_facets`).
#[derive(Debug)]
struct FacetRow {
    facet: String,
    facet_value: String,
    facet_count: i64,
    /// Set on `error_code` rows only.
    category: Option<String>,
    /// Set on `error_code` rows only: the count of every code of the
    /// category, also codes past the cap.
    category_total: Option<i64>,
}

/// Build [`Facets`] from the rows of the facets function, as Python's
/// `task_facets` (`clients/python/horsies/monitoring/queries.py:942-1007`): the code list
/// follows the selected categories and holds at most [`ERROR_FACET_CAP`]
/// codes; the category totals cover every category, selected or not.
fn facets_from_rows(
    rows: Vec<FacetRow>,
    selected: &[ErrorCategory],
) -> Result<Facets, HistoryError> {
    let selected: Vec<&str> = selected.iter().map(|category| category.as_str()).collect();
    let mut facets = Facets {
        workers: Vec::new(),
        task_names: Vec::new(),
        queues: Vec::new(),
        error_codes: Vec::new(),
        error_category_totals: BTreeMap::new(),
    };
    for row in rows {
        let value = FacetValue {
            value: row.facet_value,
            count: row.facet_count,
        };
        match row.facet.as_str() {
            "worker" => facets.workers.push(value),
            "task_name" => facets.task_names.push(value),
            "queue_name" => facets.queues.push(value),
            "error_code" => {
                let (Some(category), Some(total)) = (row.category, row.category_total) else {
                    return Err(HistoryError::contract(
                        "an error-code facet row has no category or category total",
                    ));
                };
                facets.error_category_totals.insert(category.clone(), total);
                let wanted = selected.is_empty() || selected.contains(&category.as_str());
                if wanted && (facets.error_codes.len() as i64) < ERROR_FACET_CAP {
                    facets.error_codes.push(ErrorFacet {
                        value: value.value,
                        count: value.count,
                        category,
                    });
                }
            }
            other => {
                return Err(HistoryError::contract(format!(
                    "the facets statement returned an unknown facet {other:?}"
                )))
            }
        }
    }
    Ok(facets)
}

/// Distinct workers, task names, queues and error codes with counts, over
/// the live table and the history window, in one statement in autocommit.
/// Only status and `retried_only` scope them; `error_categories` narrows the
/// returned code list but not the category totals.
pub async fn task_facets(
    broker: &PostgresBroker,
    query: &TaskFacetsQuery,
) -> MonitoringResult<Facets> {
    let operation = "task facets query";
    let statement = &MONITORING_TASK_FACETS;
    let lower = query.window.lower();
    let upper = query.window.upper();
    let statuses = status_texts(&query.filters.statuses);
    let taxonomy = builtin_taxonomy();
    let rows = broker
        .query_main(
            &statement
                .bind()
                .value(&lower)
                .value(&upper)
                .value(&statuses)
                .value(&query.filters.retried_only)
                .value(&taxonomy.codes)
                .value(&taxonomy.categories)
                .finish(),
        )
        .await
        .map_err(|error| MonitoringQueryError::broker(operation, error))?;
    let rows = rows
        .iter()
        .map(|row| facet_row(row, statement))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| MonitoringQueryError::broker(operation, BrokerError::RowDecode(error)))?;
    facets_from_rows(rows, &query.filters.error_categories)
        .map_err(|error| MonitoringQueryError::history(operation, error))
}

/// Reads one row of the facets function.
fn facet_row(row: &CoreRow, statement: &TypedStatement) -> Result<FacetRow, RowDecodeError> {
    Ok(FacetRow {
        facet: column(row, statement, "facet")?,
        facet_value: column(row, statement, "facet_value")?,
        facet_count: column(row, statement, "facet_count")?,
        category: column(row, statement, "category")?,
        category_total: column(row, statement, "category_total")?,
    })
}

/// The stored text of each status filter value.
fn status_texts(statuses: &[TaskStatus]) -> Vec<String> {
    statuses.iter().map(ToString::to_string).collect()
}

/// The count of each task status over the live table and the history
/// window, read by `horsies_monitoring_task_stats` in one statement, so a
/// task that moves to history during the read counts once. Every filter
/// except the status scopes the counts; an error code or category filter
/// selects no live row (a live row has no error code). Always the seven
/// statuses in their fixed order, zeros included. A history row in the
/// window whose status is not a task status is
/// `HISTORY_CONTRACT_VIOLATION`, and no count is returned.
pub async fn task_stats(
    broker: &PostgresBroker,
    query: &TaskStatsQuery,
) -> MonitoringResult<Vec<StatusCount>> {
    let operation = "task stats query";
    let statement = &MONITORING_TASK_STATS;
    let lower = query.window.lower();
    let upper = query.window.upper();
    let categories = category_texts(&query.filters.error_categories);
    let taxonomy = builtin_taxonomy();
    let rows = broker
        .query_main(
            &statement
                .bind()
                .value(&lower)
                .value(&upper)
                .value(&query.filters.task_names)
                .value(&query.filters.queues)
                .value(&query.filters.workers)
                .value(&query.filters.error_codes)
                .value(&categories)
                .value(&query.filters.retried_only)
                .value(&taxonomy.codes)
                .value(&taxonomy.categories)
                .finish(),
        )
        .await
        .map_err(|error| function_failure(operation, error))?;
    status_counts(&rows, statement).map_err(|error| row_failure(operation, error))
}

/// The task counts per worker, task name or queue over the live table and
/// the history window, read by `horsies_monitoring_task_breakdown` in one
/// statement (one snapshot). Every filter scopes the rows. At most the
/// query's limit of groups, by total, highest first, then by the group text
/// in byte order; `total` and `group_count` cover every group. For the
/// `worker` grouping the tasks that no worker claimed are their own group
/// ([`crate::monitoring::BreakdownGroup::Unclaimed`]), apart from a worker
/// whose id is `unknown`.
pub async fn task_breakdown(
    broker: &PostgresBroker,
    query: &TaskBreakdownQuery,
) -> MonitoringResult<Breakdown> {
    let operation = "task breakdown query";
    let statement = &MONITORING_TASK_BREAKDOWN;
    let lower = query.window.lower();
    let upper = query.window.upper();
    let group_by = query.group_by.as_str();
    let statuses = status_texts(&query.filters.statuses);
    let categories = category_texts(&query.filters.error_categories);
    let taxonomy = builtin_taxonomy();
    // `with_limit` bounds the limit to 1..=MAX_BREAKDOWN_LIMIT.
    let limit =
        i32::try_from(query.limit).expect("the breakdown limit is at most MAX_BREAKDOWN_LIMIT");
    let rows = broker
        .query_main(
            &statement
                .bind()
                .value(&lower)
                .value(&upper)
                .value(&group_by)
                .value(&statuses)
                .value(&query.filters.task_names)
                .value(&query.filters.queues)
                .value(&query.filters.workers)
                .value(&query.filters.error_codes)
                .value(&categories)
                .value(&query.filters.retried_only)
                .value(&taxonomy.codes)
                .value(&taxonomy.categories)
                .value(&limit)
                .finish(),
        )
        .await
        .map_err(|error| MonitoringQueryError::broker(operation, error))?;
    let rows = rows
        .iter()
        .map(|row| breakdown_row(row, statement))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| row_failure(operation, error))?;
    breakdown_from_rows(query.group_by, rows).map_err(|reason| {
        row_failure(
            operation,
            RowDecodeError::Shape {
                statement: statement.name(),
                reason,
            },
        )
    })
}

/// One page of the task list over the live table and the history window,
/// read by `horsies_monitoring_task_list` in one statement (one snapshot):
/// the page and its total. With any filter the total is exact; with none it
/// is the live estimate plus the planner's estimate of the history window.
pub async fn list_tasks(
    broker: &PostgresBroker,
    query: &TaskListQuery,
) -> MonitoringResult<TaskListPage> {
    let operation = "task list query";
    let statement = &MONITORING_TASK_LIST;
    let lower = query.window.lower();
    let upper = query.window.upper();
    let statuses = status_texts(&query.filters.statuses);
    let categories = category_texts(&query.filters.error_categories);
    let taxonomy = builtin_taxonomy();
    let sort_key = query.sort_by.as_str();
    let sort_direction = query.sort_direction.sql();
    // `with_pagination` bounds both to the page reach (MAX_TASK_PAGE_REACH).
    let offset =
        i32::try_from(query.offset).expect("the page offset is at most MAX_TASK_PAGE_REACH");
    let limit = i32::try_from(query.limit).expect("the page limit is at most MAX_TASK_PAGE_LIMIT");
    let rows = broker
        .query_main(
            &statement
                .bind()
                .value(&lower)
                .value(&upper)
                .value(&statuses)
                .value(&query.filters.task_names)
                .value(&query.filters.queues)
                .value(&query.filters.workers)
                .value(&query.filters.error_codes)
                .value(&categories)
                .value(&query.filters.retried_only)
                .value(&taxonomy.codes)
                .value(&taxonomy.categories)
                .value(&sort_key)
                .value(&sort_direction)
                .value(&offset)
                .value(&limit)
                .finish(),
        )
        .await
        .map_err(|error| MonitoringQueryError::broker(operation, error))?;
    let rows = rows
        .iter()
        .map(|row| task_list_row(row, statement))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| MonitoringQueryError::broker(operation, BrokerError::RowDecode(error)))?;
    let (totals, rows) = page_from_rows(rows).map_err(|reason| {
        MonitoringQueryError::broker(
            operation,
            BrokerError::RowDecode(RowDecodeError::Shape {
                statement: statement.name(),
                reason,
            }),
        )
    })?;
    let total = totals.total().map_err(|reason| {
        MonitoringQueryError::history("task list estimate decode", HistoryError::contract(reason))
    })?;
    Ok(TaskListPage { rows, total })
}

/// One task, live or in history, with its attempts and workflow link, read
/// by `horsies_monitoring_task_detail` in one statement (one snapshot).
///
/// Outcomes:
/// - `Found`: the live row, or the history row when the live table has no
///   row. Attempts are oldest first.
/// - `NotFound`: neither place holds the task. The reason, classified in the
///   same snapshot, says whether retention can explain it: an id that is not
///   a UUIDv7 is not classified; a UUIDv7 born before the latest upper bound
///   of a detached leaf of a period class (plus 5 s) predates retained
///   history; any other UUIDv7 is within it.
/// - A history row that does not decode, or a published reader that reports
///   the task live after the live miss of the same snapshot, is
///   `HISTORY_CONTRACT_VIOLATION`; any other failure, a missing reader too,
///   is `DB_OPERATION_FAILED`.
pub async fn get_task_detail(
    broker: &PostgresBroker,
    task_id: Uuid,
) -> MonitoringResult<TaskDetailRead> {
    let operation = "task detail query";
    let statement = &MONITORING_TASK_DETAIL;
    let with_result = false;
    let with_attempts = true;
    let rows = broker
        .query_main(
            &statement
                .bind()
                .value(&task_id)
                .value(&with_result)
                .value(&with_attempts)
                .finish(),
        )
        .await
        .map_err(|error| function_failure(operation, error))?;
    let row = one_row(rows, statement).map_err(|error| row_failure(operation, error))?;
    task_detail_row(&row, statement).map_err(|error| detail_row_failure(operation, error))
}

/// The distinct names of the root runs, for the run filter, in the database
/// collation, read by `horsies_monitoring_workflow_names` in one statement.
/// At most [`LIST_ROW_BOUND`] names: a longer list is cut, and the cut is
/// logged as a warning.
pub async fn list_workflow_names(broker: &PostgresBroker) -> MonitoringResult<Vec<String>> {
    let operation = "workflow names query";
    let statement = &MONITORING_WORKFLOW_NAMES;
    let rows = broker
        .query_main(&statement.bind().finish())
        .await
        .map_err(|error| MonitoringQueryError::broker(operation, error))?;
    let rows = rows
        .iter()
        .map(|row| workflow_name_row(row, statement))
        .collect::<Result<Vec<ListRow<String>>, _>>()
        .map_err(|error| row_failure(operation, error))?;
    let list = bounded_list_from_rows(rows)
        .map_err(|shape| list_shape_failure(operation, statement, shape))?;
    Ok(list_items(list, "workflow names", "root workflow names"))
}

/// The newest root runs, read by `horsies_monitoring_workflow_runs` in one
/// statement. Runs created at one time come by id, highest first, so a page
/// does not reorder between polls. The name and the status are exact
/// filters that combine with AND; a status that no run has gives an empty
/// list.
pub async fn list_workflow_runs(
    broker: &PostgresBroker,
    query: &WorkflowRunsQuery,
) -> MonitoringResult<Vec<WorkflowRunSummary>> {
    let operation = "workflow runs query";
    let statement = &MONITORING_WORKFLOW_RUNS;
    // `with_limit` bounds the limit to 1..=MAX_WORKFLOW_RUNS_LIMIT.
    let limit =
        i32::try_from(query.limit).expect("the run limit is at most MAX_WORKFLOW_RUNS_LIMIT");
    let rows = broker
        .query_main(
            &statement
                .bind()
                .value(&query.name)
                .value(&query.status)
                .value(&limit)
                .finish(),
        )
        .await
        .map_err(|error| MonitoringQueryError::broker(operation, error))?;
    workflow_runs(&rows, statement).map_err(|error| row_failure(operation, error))
}

/// A run, root or child, with its nodes, edges, failed nodes and phase
/// timing, read by `horsies_monitoring_workflow_run` in one statement (one
/// snapshot), or `None` when no run has the id. An edge to an index that the
/// run does not have is dropped.
///
/// The timing of the run and of each node is composed in the core from the
/// rows of that statement: the events of the run and the attempt archive of
/// each task of a node, live or in history at the locator of its
/// `TASK_MOVED` event. No statement runs per node or per task.
///
/// Errors: `DB_OPERATION_FAILED`; `HISTORY_CONTRACT_VIOLATION` for an
/// attempt archive that does not decode, a run of more than 10,000 nodes, or
/// a database with no bookkeeping row of migration 1001.
pub async fn get_workflow_run(
    broker: &PostgresBroker,
    workflow_id: Uuid,
) -> MonitoringResult<Option<WorkflowRunDetail>> {
    let operation = "workflow run detail query";
    let statement = &MONITORING_WORKFLOW_RUN;
    let rows = broker
        .query_main(&statement.bind().value(&workflow_id).finish())
        .await
        .map_err(|error| function_failure(operation, error))?;
    match workflow_run(&rows, statement) {
        Ok(RunRead::Found(detail)) => Ok(Some(*detail)),
        Ok(RunRead::Absent) => Ok(None),
        Err(error) => Err(detail_row_failure(operation, error)),
    }
}

/// One node with its error, its backing task and the task's attempts, read
/// by `horsies_monitoring_workflow_node` in one statement (one snapshot), or
/// `None` when the run has no node at the index. A sub-workflow node, and a
/// node whose task no table holds, have no leaf and no attempt. A stored
/// node result or a history row that does not decode is
/// `HISTORY_CONTRACT_VIOLATION`.
pub async fn get_workflow_node(
    broker: &PostgresBroker,
    workflow_id: Uuid,
    task_index: i32,
) -> MonitoringResult<Option<WorkflowTaskDetail>> {
    let operation = "workflow node detail query";
    let statement = &MONITORING_WORKFLOW_NODE;
    let rows = broker
        .query_main(
            &statement
                .bind()
                .value(&workflow_id)
                .value(&task_index)
                .finish(),
        )
        .await
        .map_err(|error| function_failure(operation, error))?;
    match workflow_node(&rows, statement) {
        Ok(NodeRead::Found(detail)) => Ok(Some(*detail)),
        Ok(NodeRead::Absent) => Ok(None),
        Err(error) => Err(detail_row_failure(operation, error)),
    }
}

/// Every schedule state, soonest next run first, then by name, read by
/// `horsies_monitoring_schedules` in one statement. At most
/// [`LIST_ROW_BOUND`] schedules: a longer list is cut, and the cut is logged
/// as a warning.
pub async fn list_schedules(broker: &PostgresBroker) -> MonitoringResult<Vec<ScheduleStateInfo>> {
    let operation = "schedule state query";
    let statement = &MONITORING_SCHEDULES;
    let rows = broker
        .query_main(&statement.bind().finish())
        .await
        .map_err(|error| MonitoringQueryError::broker(operation, error))?;
    let rows = rows
        .iter()
        .map(|row| schedule_row(row, statement))
        .collect::<Result<Vec<ListRow<ScheduleStateInfo>>, _>>()
        .map_err(|error| row_failure(operation, error))?;
    let list = bounded_list_from_rows(rows)
        .map_err(|shape| list_shape_failure(operation, statement, shape))?;
    Ok(list_items(list, "schedule state", "schedules"))
}

#[cfg(test)]
mod facet_tests {
    use super::*;

    fn row(facet: &str, value: &str, count: i64, category: Option<(&str, i64)>) -> FacetRow {
        FacetRow {
            facet: facet.to_owned(),
            facet_value: value.to_owned(),
            facet_count: count,
            category: category.map(|(name, _)| name.to_owned()),
            category_total: category.map(|(_, total)| total),
        }
    }

    /// The facets come from the baseline function in one statement.
    #[test]
    fn the_facets_call_the_baseline_function() {
        assert!(super::super::function_rows::MONITORING_TASK_FACETS
            .sql()
            .contains("FROM horsies_monitoring_task_facets($1, $2, $3, $4, $5, $6)"));
    }

    #[test]
    fn rows_fill_each_facet_in_rank_order() {
        let facets = facets_from_rows(
            vec![
                row("queue_name", "default", 5, None),
                row("task_name", "send", 4, None),
                row("worker", "w1", 3, None),
                row("worker", "w2", 1, None),
            ],
            &[],
        )
        .expect("rows decode");

        assert_eq!(
            facets.workers,
            vec![
                FacetValue {
                    value: "w1".to_owned(),
                    count: 3
                },
                FacetValue {
                    value: "w2".to_owned(),
                    count: 1
                },
            ]
        );
        assert_eq!(facets.task_names.len(), 1);
        assert_eq!(facets.queues.len(), 1);
    }

    /// The code list follows the selected categories; the totals cover every
    /// category (Python `task_facets`).
    #[test]
    fn the_code_list_follows_the_selection_and_the_totals_do_not() {
        let rows = || {
            vec![
                row("error_code", "TASK_TIMEOUT", 7, Some(("OUTCOME", 9))),
                row("error_code", "BILLING_DOWN", 4, Some(("DOMAIN", 4))),
                row("error_code", "TASK_CANCELLED", 2, Some(("OUTCOME", 9))),
            ]
        };

        let all = facets_from_rows(rows(), &[]).expect("rows decode");
        assert_eq!(all.error_codes.len(), 3);

        let domain = facets_from_rows(rows(), &[ErrorCategory::Domain]).expect("rows decode");
        assert_eq!(
            domain.error_codes,
            vec![ErrorFacet {
                value: "BILLING_DOWN".to_owned(),
                count: 4,
                category: "DOMAIN".to_owned(),
            }]
        );
        assert_eq!(domain.error_category_totals.get("OUTCOME"), Some(&9));
        assert_eq!(domain.error_category_totals.get("DOMAIN"), Some(&4));
    }

    #[test]
    fn the_code_list_stops_at_its_cap() {
        let rows = (0..ERROR_FACET_CAP + 5)
            .map(|index| {
                row(
                    "error_code",
                    &format!("CODE_{index}"),
                    1,
                    Some(("DOMAIN", 35)),
                )
            })
            .collect();

        let facets = facets_from_rows(rows, &[]).expect("rows decode");

        assert_eq!(facets.error_codes.len() as i64, ERROR_FACET_CAP);
        assert_eq!(facets.error_category_totals.get("DOMAIN"), Some(&35));
    }

    #[test]
    fn a_row_the_statement_cannot_return_is_a_contract_violation() {
        assert!(facets_from_rows(vec![row("status", "x", 1, None)], &[]).is_err());
        assert!(facets_from_rows(vec![row("error_code", "X", 1, None)], &[]).is_err());
    }
}
