//! Monitoring query implementations over live and retained task data.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use sqlx::postgres::PgRow;
use sqlx::{FromRow, PgConnection, Postgres, QueryBuilder, Row};
use uuid::Uuid;

use crate::broker::PostgresBroker;
use crate::core::history::archive::attempts::AttemptRecord;
use crate::core::history::errors::HistoryError;
use crate::core::history::reads::aggregates::{
    history_breakdown_statement, history_count_statement, history_estimate_statement,
    history_scoped_status_counts_statement, plan_rows_from_explain, HistoryBreakdownGroup,
    HISTORY_NONEMPTY_PROBE_SQL,
};
use crate::core::history::reads::detail::{
    read_task_detail, staged_detail_published, HistoryTaskDetail, TaskDetailResult,
};
use crate::core::history::reads::pages::{
    HistoryPageQuery, HistoryScope, HistorySortField, HistoryStatement, HistoryWindow,
};
use crate::core::task::error::{
    BuiltInTaskCode, ContractCode, OperationalErrorCode, OutcomeCode, RetrievalCode,
};
use crate::core::types::status::TaskStatus;

use super::errors::{MonitoringQueryError, MonitoringResult};
use super::models::{
    Breakdown, ErrorCategory, ErrorFacet, FacetValue, Facets, GroupRow, LeafTaskInfo,
    ScheduleStateInfo, StatusCount, TaskAttemptInfo, TaskDetail, TaskListPage, TaskSummary,
    WorkflowEdge, WorkflowNodeInfo, WorkflowRunDetail, WorkflowRunSummary, WorkflowTaskDetail,
};

pub const MAX_TASK_PAGE_REACH: i64 = 500;
const FACET_VALUE_CAP: i64 = 50;
const ERROR_FACET_CAP: i64 = 30;
pub(super) const LIVE_SUMMARY_COLUMNS: &str =
    "id, task_name, queue_name, status, priority, retry_count, max_retries, \
     is_workflow_task, error_code, worker_hostname, claimed_by_worker_id, \
     enqueued_at, started_at, completed_at, failed_at";

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

    const fn live_expression(self) -> (&'static str, bool) {
        match self {
            Self::EnqueuedAt => ("enqueued_at", false),
            Self::StartedAt => ("started_at", true),
            Self::CompletedAt => ("completed_at", true),
            Self::FailedAt => ("failed_at", true),
            Self::Status => ("status", false),
            Self::TaskName => ("task_name", false),
            Self::QueueName => ("queue_name", false),
            Self::Priority => ("priority", false),
            Self::RetryCount => ("retry_count", false),
            Self::QueueSeconds => ("(started_at - enqueued_at)", true),
            Self::ExecutionSeconds => ("(COALESCE(completed_at, failed_at) - started_at)", true),
        }
    }

    const fn history_field(self) -> HistorySortField {
        match self {
            Self::EnqueuedAt => HistorySortField::EnqueuedAt,
            Self::StartedAt => HistorySortField::StartedAt,
            Self::CompletedAt => HistorySortField::CompletedAt,
            Self::FailedAt => HistorySortField::FailedAt,
            Self::Status => HistorySortField::Status,
            Self::TaskName => HistorySortField::TaskName,
            Self::QueueName => HistorySortField::QueueName,
            Self::Priority => HistorySortField::Priority,
            Self::RetryCount => HistorySortField::RetryCount,
            Self::QueueSeconds => HistorySortField::QueueSeconds,
            Self::ExecutionSeconds => HistorySortField::ExecutionSeconds,
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

    const fn live_column(self) -> &'static str {
        match self {
            Self::Worker => "claimed_by_worker_id",
            Self::TaskName => "task_name",
            Self::Queue => "queue_name",
        }
    }

    const fn history_group(self) -> HistoryBreakdownGroup {
        match self {
            Self::Worker => HistoryBreakdownGroup::Worker,
            Self::TaskName => HistoryBreakdownGroup::TaskName,
            Self::Queue => HistoryBreakdownGroup::QueueName,
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

impl TaskFilters {
    fn is_empty(&self) -> bool {
        self.statuses.is_empty()
            && self.task_names.is_empty()
            && self.queues.is_empty()
            && self.workers.is_empty()
            && self.error_codes.is_empty()
            && self.error_categories.is_empty()
            && !self.retried_only
    }
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
        if !(1..=500).contains(&limit) {
            return Err(PaginationRefused {
                reason: format!("limit must be between 1 and 500; got {limit}"),
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
        if !(1..=200).contains(&limit) {
            return Err(PaginationRefused {
                reason: format!("limit must be between 1 and 200; got {limit}"),
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
        if !(1..=200).contains(&limit) {
            return Err(PaginationRefused {
                reason: format!("limit must be between 1 and 200; got {limit}"),
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

pub fn elapsed_s(start: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>) -> Option<i64> {
    let start = start?;
    Some((end.unwrap_or_else(Utc::now) - start).num_seconds())
}

pub fn span_s(start: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>, live: bool) -> Option<i64> {
    match (start, end) {
        (None, _) => None,
        (Some(start), Some(end)) => Some((end - start).num_seconds()),
        (Some(start), None) if live => Some((Utc::now() - start).num_seconds()),
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

fn codes_for_category(category: ErrorCategory) -> Vec<String> {
    match category {
        ErrorCategory::Operational => OperationalErrorCode::ALL
            .iter()
            .map(ToString::to_string)
            .collect(),
        ErrorCategory::Contract => ContractCode::ALL.iter().map(ToString::to_string).collect(),
        ErrorCategory::Retrieval => RetrievalCode::ALL.iter().map(ToString::to_string).collect(),
        ErrorCategory::Outcome => OutcomeCode::ALL.iter().map(ToString::to_string).collect(),
        ErrorCategory::Domain => Vec::new(),
    }
}

fn all_builtin_codes() -> Vec<String> {
    [
        ErrorCategory::Operational,
        ErrorCategory::Contract,
        ErrorCategory::Retrieval,
        ErrorCategory::Outcome,
    ]
    .into_iter()
    .flat_map(codes_for_category)
    .collect()
}

fn history_scope(filters: &TaskFilters) -> HistoryScope {
    let mut category_families = Vec::new();
    let mut domain_complement = None;
    for category in &filters.error_categories {
        match category {
            ErrorCategory::Domain => domain_complement = Some(all_builtin_codes()),
            other => category_families.push(codes_for_category(*other)),
        }
    }
    HistoryScope {
        statuses: filters.statuses.iter().map(ToString::to_string).collect(),
        task_names: filters.task_names.clone(),
        queue_names: filters.queues.clone(),
        workers: filters.workers.clone(),
        error_codes: filters.error_codes.clone(),
        category_families,
        domain_complement,
        retried_only: filters.retried_only,
    }
}

fn push_error_categories(builder: &mut QueryBuilder<'_, Postgres>, categories: &[ErrorCategory]) {
    if categories.is_empty() {
        return;
    }
    builder.push(" AND (");
    for (index, category) in categories.iter().enumerate() {
        if index > 0 {
            builder.push(" OR ");
        }
        match category {
            ErrorCategory::Domain => {
                builder
                    .push("(error_code IS NOT NULL AND error_code <> '' AND error_code <> ALL(")
                    .push_bind(all_builtin_codes())
                    .push("::text[]))");
            }
            other => {
                builder
                    .push("error_code = ANY(")
                    .push_bind(codes_for_category(*other))
                    .push("::text[])");
            }
        }
    }
    builder.push(")");
}

fn push_live_scope(builder: &mut QueryBuilder<'_, Postgres>, filters: &TaskFilters) {
    builder.push(" WHERE TRUE");
    if !filters.statuses.is_empty() {
        builder
            .push(" AND status = ANY(")
            .push_bind(
                filters
                    .statuses
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
            )
            .push("::text[])");
    }
    for (column, values) in [
        ("task_name", &filters.task_names),
        ("queue_name", &filters.queues),
        ("claimed_by_worker_id", &filters.workers),
        ("error_code", &filters.error_codes),
    ] {
        if !values.is_empty() {
            builder
                .push(" AND ")
                .push(column)
                .push(" = ANY(")
                .push_bind(values.clone())
                .push("::text[])");
        }
    }
    push_error_categories(builder, &filters.error_categories);
    if filters.retried_only {
        builder.push(" AND retry_count > 0");
    }
}

fn category_value(code: Option<&str>) -> Option<String> {
    categorize_error_code(code).map(|category| category.as_str().to_owned())
}

#[derive(Debug, FromRow)]
struct LiveTaskSummaryRow {
    id: Uuid,
    task_name: String,
    queue_name: String,
    status: String,
    priority: i32,
    retry_count: i32,
    max_retries: i32,
    is_workflow_task: bool,
    error_code: Option<String>,
    worker_hostname: Option<String>,
    claimed_by_worker_id: Option<String>,
    enqueued_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    failed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, FromRow)]
struct HistoryTaskSummaryRow {
    task_id: Uuid,
    task_name: String,
    queue_name: String,
    priority: i32,
    status: String,
    terminal_at: DateTime<Utc>,
    enqueued_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    retry_count: i32,
    max_retries: i32,
    error_code: Option<String>,
    last_claimed_worker_id: Option<String>,
    last_worker_hostname: Option<String>,
    is_workflow_task: bool,
}

impl From<LiveTaskSummaryRow> for TaskSummary {
    fn from(row: LiveTaskSummaryRow) -> Self {
        let terminal = row.completed_at.or(row.failed_at);
        let queue_s = span_s(
            Some(row.enqueued_at),
            row.started_at.or(terminal),
            matches!(row.status.as_str(), "PENDING" | "CLAIMED"),
        );
        let exec_s = span_s(row.started_at, terminal, row.status.as_str() == "RUNNING");
        Self {
            id: row.id,
            task_name: row.task_name,
            queue_name: row.queue_name,
            status: row.status,
            priority: row.priority,
            retry_count: row.retry_count,
            max_retries: row.max_retries,
            is_workflow_task: row.is_workflow_task,
            error_category: category_value(row.error_code.as_deref()),
            error_code: normalize_optional_text(row.error_code.as_deref()),
            worker_hostname: row.worker_hostname,
            worker_id: row.claimed_by_worker_id,
            enqueued_at: Some(row.enqueued_at),
            started_at: row.started_at,
            completed_at: row.completed_at,
            failed_at: row.failed_at,
            queue_s,
            exec_s,
        }
    }
}

impl From<HistoryTaskSummaryRow> for TaskSummary {
    fn from(row: HistoryTaskSummaryRow) -> Self {
        let completed_at = (row.status == "COMPLETED").then_some(row.terminal_at);
        let failed_at = (row.status != "COMPLETED").then_some(row.terminal_at);
        Self {
            id: row.task_id,
            task_name: row.task_name,
            queue_name: row.queue_name,
            status: row.status,
            priority: row.priority,
            retry_count: row.retry_count,
            max_retries: row.max_retries,
            is_workflow_task: row.is_workflow_task,
            error_category: category_value(row.error_code.as_deref()),
            error_code: normalize_optional_text(row.error_code.as_deref()),
            worker_hostname: row.last_worker_hostname,
            worker_id: row.last_claimed_worker_id,
            enqueued_at: Some(row.enqueued_at),
            started_at: row.started_at,
            completed_at,
            failed_at,
            queue_s: span_s(
                Some(row.enqueued_at),
                row.started_at.or(Some(row.terminal_at)),
                false,
            ),
            exec_s: span_s(row.started_at, Some(row.terminal_at), false),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum SummarySortKey {
    Timestamp(DateTime<Utc>),
    Text(String),
    Integer(i64),
}

fn summary_sort_key(summary: &TaskSummary, field: TaskSortField) -> Option<SummarySortKey> {
    match field {
        TaskSortField::EnqueuedAt => summary.enqueued_at.map(SummarySortKey::Timestamp),
        TaskSortField::StartedAt => summary.started_at.map(SummarySortKey::Timestamp),
        TaskSortField::CompletedAt => summary.completed_at.map(SummarySortKey::Timestamp),
        TaskSortField::FailedAt => summary.failed_at.map(SummarySortKey::Timestamp),
        TaskSortField::Status => Some(SummarySortKey::Text(summary.status.clone())),
        TaskSortField::TaskName => Some(SummarySortKey::Text(summary.task_name.clone())),
        TaskSortField::QueueName => Some(SummarySortKey::Text(summary.queue_name.clone())),
        TaskSortField::Priority => Some(SummarySortKey::Integer(i64::from(summary.priority))),
        TaskSortField::RetryCount => Some(SummarySortKey::Integer(i64::from(summary.retry_count))),
        TaskSortField::QueueSeconds => match (summary.enqueued_at, summary.started_at) {
            (Some(enqueued), Some(started)) => {
                Some(SummarySortKey::Integer((started - enqueued).num_seconds()))
            }
            _ => None,
        },
        TaskSortField::ExecutionSeconds => {
            match (
                summary.started_at,
                summary.completed_at.or(summary.failed_at),
            ) {
                (Some(started), Some(terminal)) => {
                    Some(SummarySortKey::Integer((terminal - started).num_seconds()))
                }
                _ => None,
            }
        }
    }
}

fn compare_summaries(
    left: &TaskSummary,
    right: &TaskSummary,
    field: TaskSortField,
    direction: SortDirection,
) -> Ordering {
    match (
        summary_sort_key(left, field),
        summary_sort_key(right, field),
    ) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(left), Some(right)) => match direction {
            SortDirection::Ascending => left.cmp(&right),
            SortDirection::Descending => right.cmp(&left),
        },
    }
}

fn history_attempt(record: &AttemptRecord) -> TaskAttemptInfo {
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

fn history_leaf(detail: &HistoryTaskDetail) -> LeafTaskInfo {
    let completed_at = (detail.status == "COMPLETED").then_some(detail.terminal_at);
    let failed_at = (detail.status != "COMPLETED").then_some(detail.terminal_at);
    LeafTaskInfo {
        task_id: detail.task_id,
        status: detail.status.clone(),
        error_code: normalize_optional_text(detail.error_code.as_deref()),
        failed_reason: normalize_optional_text(detail.final_failed_reason.as_deref()),
        retry_count: detail.retry_count,
        max_retries: detail.max_retries,
        enqueued_at: Some(detail.enqueued_at),
        started_at: detail.started_at,
        completed_at,
        failed_at,
        queue_s: span_s(
            Some(detail.enqueued_at),
            detail.started_at.or(Some(detail.terminal_at)),
            false,
        ),
        exec_s: span_s(detail.started_at, Some(detail.terminal_at), false),
        worker_hostname: detail.last_worker_hostname.clone(),
        good_until: detail.good_until,
    }
}

async fn fetch_history_rows(
    connection: &mut PgConnection,
    statement: &HistoryStatement,
) -> Result<Vec<PgRow>, sqlx::Error> {
    statement.query().fetch_all(connection).await
}

pub async fn task_stats(
    broker: &PostgresBroker,
    query: &TaskStatsQuery,
) -> MonitoringResult<Vec<StatusCount>> {
    let operation = "task stats query";
    let mut connection = broker
        .pool()
        .acquire()
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))?;

    let mut live = QueryBuilder::<Postgres>::new("SELECT status, count(*) FROM horsies_tasks");
    push_live_scope(&mut live, &query.filters);
    live.push(" GROUP BY status");
    let live_rows: Vec<(String, i64)> = live
        .build_query_as()
        .fetch_all(&mut *connection)
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))?;

    let history_statement =
        history_scoped_status_counts_statement(query.window, &history_scope(&query.filters));
    let history_rows = fetch_history_rows(&mut connection, &history_statement)
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))?;

    let mut counts: BTreeMap<String, i64> = live_rows.into_iter().collect();
    for row in history_rows {
        let status: String = row
            .try_get("status")
            .map_err(|error| MonitoringQueryError::database(operation, error))?;
        let count: i64 = row
            .try_get("terminal_count")
            .map_err(|error| MonitoringQueryError::database(operation, error))?;
        *counts.entry(status).or_default() += count;
    }
    Ok(TaskStatus::ALL
        .into_iter()
        .map(|status| {
            let status = status.to_string();
            StatusCount {
                count: counts.get(&status).copied().unwrap_or_default(),
                status,
            }
        })
        .collect())
}

#[derive(Debug, Clone, Copy)]
enum CombinedFacet {
    Worker,
    TaskName,
    Queue,
    ErrorCode,
}

impl CombinedFacet {
    const fn live_column(self) -> &'static str {
        match self {
            Self::Worker => "claimed_by_worker_id",
            Self::TaskName => "task_name",
            Self::Queue => "queue_name",
            Self::ErrorCode => "error_code",
        }
    }

    const fn history_column(self) -> &'static str {
        match self {
            Self::Worker => "last_claimed_worker_id",
            Self::TaskName => "task_name",
            Self::Queue => "queue_name",
            Self::ErrorCode => "error_code",
        }
    }
}

fn push_facet_scope(
    builder: &mut QueryBuilder<'_, Postgres>,
    window: Option<HistoryWindow>,
    filters: &TaskFilters,
    worker_column: &str,
    error_categories: &[ErrorCategory],
) {
    builder.push(" WHERE TRUE");
    if let Some(window) = window {
        builder
            .push(" AND retention_anchor_at >= ")
            .push_bind(window.lower())
            .push(" AND retention_anchor_at < ")
            .push_bind(window.upper());
    }
    if !filters.statuses.is_empty() {
        builder
            .push(" AND status = ANY(")
            .push_bind(
                filters
                    .statuses
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
            )
            .push("::text[])");
    }
    if filters.retried_only {
        builder.push(" AND retry_count > 0");
    }
    for (column, values) in [
        ("task_name", &filters.task_names),
        ("queue_name", &filters.queues),
        (worker_column, &filters.workers),
        ("error_code", &filters.error_codes),
    ] {
        if !values.is_empty() {
            builder
                .push(" AND ")
                .push(column)
                .push(" = ANY(")
                .push_bind(values.clone())
                .push("::text[])");
        }
    }
    push_error_categories(builder, error_categories);
}

async fn combined_facet_rows(
    connection: &mut PgConnection,
    query: &TaskFacetsQuery,
    facet: CombinedFacet,
    limit: Option<i64>,
    error_categories: &[ErrorCategory],
) -> Result<Vec<(String, i64)>, sqlx::Error> {
    let live_column = facet.live_column();
    let history_column = facet.history_column();
    let mut builder = QueryBuilder::<Postgres>::new("WITH live_facet AS (SELECT ");
    builder
        .push(live_column)
        .push(" AS facet_value, count(*) AS facet_count FROM horsies_tasks");
    push_facet_scope(
        &mut builder,
        None,
        &query.filters,
        "claimed_by_worker_id",
        error_categories,
    );
    builder.push(" AND ").push(live_column).push(" IS NOT NULL");
    if matches!(facet, CombinedFacet::ErrorCode) {
        builder.push(" AND error_code <> ''");
    }
    builder
        .push(" GROUP BY ")
        .push(live_column)
        .push("), history_facet AS (SELECT ");
    builder
        .push(history_column)
        .push(" AS facet_value, count(*) AS facet_count FROM horsies_task_history");
    push_facet_scope(
        &mut builder,
        Some(query.window),
        &query.filters,
        "last_claimed_worker_id",
        error_categories,
    );
    builder
        .push(" AND ")
        .push(history_column)
        .push(" IS NOT NULL");
    if matches!(facet, CombinedFacet::ErrorCode) {
        builder.push(" AND error_code <> ''");
    }
    builder.push(" GROUP BY ").push(history_column).push(
        "), combined AS (
             SELECT facet_value, sum(facet_count)::bigint AS facet_count
             FROM (
                 SELECT facet_value, facet_count FROM live_facet
                 UNION ALL
                 SELECT facet_value, facet_count FROM history_facet
             ) AS lifecycle_facets
             GROUP BY facet_value
         )
         SELECT facet_value, facet_count FROM combined
         ORDER BY facet_count DESC, facet_value",
    );
    if let Some(limit) = limit {
        builder.push(" LIMIT ").push_bind(limit);
    }
    builder.build_query_as().fetch_all(connection).await
}

fn facet_values(rows: Vec<(String, i64)>) -> Vec<FacetValue> {
    rows.into_iter()
        .map(|(value, count)| FacetValue { value, count })
        .collect()
}

pub async fn task_facets(
    broker: &PostgresBroker,
    query: &TaskFacetsQuery,
) -> MonitoringResult<Facets> {
    let operation = "task facets query";
    let mut connection = broker
        .pool()
        .acquire()
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))?;

    let workers = combined_facet_rows(
        &mut connection,
        query,
        CombinedFacet::Worker,
        Some(FACET_VALUE_CAP),
        &[],
    )
    .await
    .map_err(|error| MonitoringQueryError::database(operation, error))?;
    let task_names = combined_facet_rows(
        &mut connection,
        query,
        CombinedFacet::TaskName,
        Some(FACET_VALUE_CAP),
        &[],
    )
    .await
    .map_err(|error| MonitoringQueryError::database(operation, error))?;
    let queues = combined_facet_rows(
        &mut connection,
        query,
        CombinedFacet::Queue,
        Some(FACET_VALUE_CAP),
        &[],
    )
    .await
    .map_err(|error| MonitoringQueryError::database(operation, error))?;
    let error_rows = combined_facet_rows(
        &mut connection,
        query,
        CombinedFacet::ErrorCode,
        Some(ERROR_FACET_CAP),
        &query.filters.error_categories,
    )
    .await
    .map_err(|error| MonitoringQueryError::database(operation, error))?;
    let uncapped_error_rows =
        combined_facet_rows(&mut connection, query, CombinedFacet::ErrorCode, None, &[])
            .await
            .map_err(|error| MonitoringQueryError::database(operation, error))?;

    let error_codes = error_rows
        .into_iter()
        .map(|(value, count)| ErrorFacet {
            category: categorize_error_code(Some(&value))
                .unwrap_or(ErrorCategory::Domain)
                .as_str()
                .to_owned(),
            value,
            count,
        })
        .collect();
    let mut error_category_totals = BTreeMap::new();
    for (value, count) in uncapped_error_rows {
        let category = categorize_error_code(Some(&value)).unwrap_or(ErrorCategory::Domain);
        *error_category_totals
            .entry(category.as_str().to_owned())
            .or_default() += count;
    }
    Ok(Facets {
        workers: facet_values(workers),
        task_names: facet_values(task_names),
        queues: facet_values(queues),
        error_codes,
        error_category_totals,
    })
}

fn apply_group_count(
    row: &mut GroupRow,
    status: &str,
    count: i64,
    retried: i64,
) -> Result<(), HistoryError> {
    row.total += count;
    row.retried += retried;
    match status {
        "PENDING" => row.pending += count,
        "CLAIMED" => row.claimed += count,
        "RUNNING" => row.running += count,
        "COMPLETED" => row.completed += count,
        "FAILED" => row.failed += count,
        "CANCELLED" => row.cancelled += count,
        "EXPIRED" => row.expired += count,
        other => {
            return Err(HistoryError::contract(format!(
                "monitoring breakdown found unknown task status {other:?}"
            )))
        }
    }
    Ok(())
}

fn add_group_rows(total: &mut GroupRow, row: &GroupRow) {
    total.total += row.total;
    total.pending += row.pending;
    total.claimed += row.claimed;
    total.running += row.running;
    total.completed += row.completed;
    total.failed += row.failed;
    total.cancelled += row.cancelled;
    total.expired += row.expired;
    total.retried += row.retried;
}

pub async fn task_breakdown(
    broker: &PostgresBroker,
    query: &TaskBreakdownQuery,
) -> MonitoringResult<Breakdown> {
    let operation = "task breakdown query";
    let mut connection = broker
        .pool()
        .acquire()
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))?;

    let column = query.group_by.live_column();
    let mut live = QueryBuilder::<Postgres>::new("SELECT COALESCE(");
    live.push(column).push(
        ", 'unknown') AS group_value, status, count(*) AS status_count,
         count(*) FILTER (WHERE retry_count > 0) AS retried_count
         FROM horsies_tasks",
    );
    push_live_scope(&mut live, &query.filters);
    live.push(" GROUP BY COALESCE(")
        .push(column)
        .push(", 'unknown'), status");
    let live_rows: Vec<(String, String, i64, i64)> = live
        .build_query_as()
        .fetch_all(&mut *connection)
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))?;

    let history_statement = history_breakdown_statement(
        query.window,
        &history_scope(&query.filters),
        query.group_by.history_group(),
    );
    let history_rows = fetch_history_rows(&mut connection, &history_statement)
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))?;

    let mut groups: BTreeMap<String, GroupRow> = BTreeMap::new();
    for (group, status, count, retried) in live_rows {
        let row = groups
            .entry(group.clone())
            .or_insert_with(|| GroupRow::empty(group));
        apply_group_count(row, &status, count, retried)
            .map_err(|error| MonitoringQueryError::history(operation, error))?;
    }
    for history_row in history_rows {
        let group: String = history_row
            .try_get("group_value")
            .map_err(|error| MonitoringQueryError::database(operation, error))?;
        let status: String = history_row
            .try_get("status")
            .map_err(|error| MonitoringQueryError::database(operation, error))?;
        let count: i64 = history_row
            .try_get("status_count")
            .map_err(|error| MonitoringQueryError::database(operation, error))?;
        let retried: i64 = history_row
            .try_get("retried_count")
            .map_err(|error| MonitoringQueryError::database(operation, error))?;
        let row = groups
            .entry(group.clone())
            .or_insert_with(|| GroupRow::empty(group));
        apply_group_count(row, &status, count, retried)
            .map_err(|error| MonitoringQueryError::history(operation, error))?;
    }

    let mut rows: Vec<GroupRow> = groups.into_values().collect();
    rows.sort_by(|left, right| {
        right
            .total
            .cmp(&left.total)
            .then_with(|| left.group.cmp(&right.group))
    });
    let mut total = GroupRow::empty("TOTAL");
    for row in &rows {
        add_group_rows(&mut total, row);
    }
    let group_count = i64::try_from(rows.len()).unwrap_or(i64::MAX);
    rows.truncate(query.limit as usize);
    Ok(Breakdown {
        group_by: query.group_by.as_str().to_owned(),
        groups: rows,
        total,
        group_count,
    })
}

async fn estimated_live_total(
    connection: &mut PgConnection,
    operation: &str,
) -> MonitoringResult<i64> {
    let estimate: i64 = sqlx::query_scalar(
        "SELECT reltuples::bigint FROM pg_class WHERE oid = 'horsies_tasks'::regclass",
    )
    .fetch_one(&mut *connection)
    .await
    .map_err(|error| MonitoringQueryError::database(operation, error))?;
    if estimate >= 0 {
        return Ok(estimate);
    }
    sqlx::query_scalar("SELECT count(*) FROM horsies_tasks")
        .fetch_one(connection)
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))
}

async fn estimated_history_total(
    connection: &mut PgConnection,
    operation: &str,
    window: HistoryWindow,
    scope: &HistoryScope,
) -> MonitoringResult<i64> {
    let nonempty: bool = sqlx::query_scalar(HISTORY_NONEMPTY_PROBE_SQL.as_str())
        .fetch_one(&mut *connection)
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))?;
    if !nonempty {
        return Ok(0);
    }
    let statement = history_estimate_statement(window, scope);
    let row = statement
        .query()
        .fetch_one(&mut *connection)
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))?;
    let payload: serde_json::Value = row
        .try_get(0)
        .map_err(|error| MonitoringQueryError::database(operation, error))?;
    plan_rows_from_explain(&payload)
        .map_err(|error| MonitoringQueryError::history("task list estimate decode", error))
}

async fn exact_live_total(
    connection: &mut PgConnection,
    operation: &str,
    filters: &TaskFilters,
) -> MonitoringResult<i64> {
    let mut builder = QueryBuilder::<Postgres>::new("SELECT count(*) FROM horsies_tasks");
    push_live_scope(&mut builder, filters);
    builder
        .build_query_scalar()
        .fetch_one(connection)
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))
}

async fn exact_history_total(
    connection: &mut PgConnection,
    operation: &str,
    window: HistoryWindow,
    scope: &HistoryScope,
) -> MonitoringResult<i64> {
    let statement = history_count_statement(window, scope);
    let row = statement
        .query()
        .fetch_one(connection)
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))?;
    row.try_get(0)
        .map_err(|error| MonitoringQueryError::database(operation, error))
}

pub async fn list_tasks(
    broker: &PostgresBroker,
    query: &TaskListQuery,
) -> MonitoringResult<TaskListPage> {
    let operation = "task list query";
    let reach = query.offset + query.limit;
    let scope = history_scope(&query.filters);
    let mut connection = broker
        .pool()
        .acquire()
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))?;

    let (live_total, history_total) = if query.filters.is_empty() {
        (
            estimated_live_total(&mut connection, operation).await?,
            estimated_history_total(&mut connection, operation, query.window, &scope).await?,
        )
    } else {
        (
            exact_live_total(&mut connection, operation, &query.filters).await?,
            exact_history_total(&mut connection, operation, query.window, &scope).await?,
        )
    };

    let mut live = QueryBuilder::<Postgres>::new("SELECT ");
    live.push(LIVE_SUMMARY_COLUMNS).push(" FROM horsies_tasks");
    push_live_scope(&mut live, &query.filters);
    let (sort_expression, nullable) = query.sort_by.live_expression();
    live.push(" ORDER BY ")
        .push(sort_expression)
        .push(" ")
        .push(query.sort_direction.sql());
    if nullable {
        live.push(" NULLS LAST");
    }
    live.push(" LIMIT ").push_bind(reach);
    let live_rows: Vec<LiveTaskSummaryRow> = live
        .build_query_as()
        .fetch_all(&mut *connection)
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))?;

    let page_query = HistoryPageQuery::new(query.window, reach)
        .map_err(|error| MonitoringQueryError::history(operation, error))?
        .with_scope(scope)
        .with_sort_field(
            query.sort_by.history_field(),
            query.sort_direction == SortDirection::Descending,
        );
    let history_statement = crate::core::history::reads::pages::history_page_statement(&page_query);
    let history_rows = fetch_history_rows(&mut connection, &history_statement)
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))?;
    let history_rows = history_rows
        .iter()
        .map(HistoryTaskSummaryRow::from_row)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| MonitoringQueryError::database(operation, error))?;

    let mut summaries: Vec<TaskSummary> = live_rows.into_iter().map(Into::into).collect();
    summaries.extend(history_rows.into_iter().map(Into::into));
    summaries
        .sort_by(|left, right| compare_summaries(left, right, query.sort_by, query.sort_direction));
    let rows = summaries
        .into_iter()
        .skip(query.offset as usize)
        .take(query.limit as usize)
        .collect();
    Ok(TaskListPage {
        rows,
        total: live_total + history_total,
    })
}

#[derive(Debug, FromRow)]
struct LiveTaskDetailRow {
    id: Uuid,
    task_name: String,
    queue_name: String,
    status: String,
    priority: i32,
    retry_count: i32,
    max_retries: i32,
    is_workflow_task: bool,
    error_code: Option<String>,
    failed_reason: Option<String>,
    worker_hostname: Option<String>,
    enqueued_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    failed_at: Option<DateTime<Utc>>,
    good_until: Option<DateTime<Utc>>,
}

#[derive(Debug, FromRow)]
struct LiveAttemptRow {
    attempt: i32,
    outcome: String,
    will_retry: bool,
    error_code: Option<String>,
    error_message: Option<String>,
    failed_reason: Option<String>,
    worker_hostname: Option<String>,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
}

impl From<LiveAttemptRow> for TaskAttemptInfo {
    fn from(row: LiveAttemptRow) -> Self {
        Self {
            attempt: row.attempt,
            outcome: row.outcome,
            will_retry: row.will_retry,
            error_code: normalize_optional_text(row.error_code.as_deref()),
            error_message: normalize_optional_text(row.error_message.as_deref()),
            failed_reason: normalize_optional_text(row.failed_reason.as_deref()),
            worker_hostname: row.worker_hostname,
            started_at: Some(row.started_at),
            finished_at: Some(row.finished_at),
        }
    }
}

impl LiveTaskDetailRow {
    fn leaf(&self) -> LeafTaskInfo {
        let terminal = self.completed_at.or(self.failed_at);
        LeafTaskInfo {
            task_id: self.id,
            status: self.status.clone(),
            error_code: normalize_optional_text(self.error_code.as_deref()),
            failed_reason: normalize_optional_text(self.failed_reason.as_deref()),
            retry_count: self.retry_count,
            max_retries: self.max_retries,
            enqueued_at: Some(self.enqueued_at),
            started_at: self.started_at,
            completed_at: self.completed_at,
            failed_at: self.failed_at,
            queue_s: span_s(
                Some(self.enqueued_at),
                self.started_at.or(terminal),
                matches!(self.status.as_str(), "PENDING" | "CLAIMED"),
            ),
            exec_s: span_s(self.started_at, terminal, self.status.as_str() == "RUNNING"),
            worker_hostname: self.worker_hostname.clone(),
            good_until: self.good_until,
        }
    }
}

async fn live_attempts(
    connection: &mut PgConnection,
    task_id: Uuid,
) -> Result<Vec<TaskAttemptInfo>, sqlx::Error> {
    let rows: Vec<LiveAttemptRow> = sqlx::query_as(
        "SELECT attempt, outcome, will_retry, error_code, error_message,
                failed_reason, worker_hostname, started_at, finished_at
         FROM horsies_task_attempts
         WHERE task_id = $1
         ORDER BY attempt",
    )
    .bind(task_id)
    .fetch_all(connection)
    .await?;
    Ok(rows.into_iter().map(Into::into).collect())
}

async fn workflow_link(
    connection: &mut PgConnection,
    task_id: Uuid,
) -> Result<Option<(Uuid, i32)>, sqlx::Error> {
    sqlx::query_as(
        "SELECT workflow_id, task_index
         FROM horsies_workflow_tasks
         WHERE task_id = $1
         LIMIT 1",
    )
    .bind(task_id)
    .fetch_optional(connection)
    .await
}

async fn history_detail_or_none(
    connection: &mut PgConnection,
    task_id: Uuid,
) -> Result<Option<HistoryTaskDetail>, HistoryError> {
    if !staged_detail_published(&mut *connection).await? {
        return Ok(None);
    }
    match read_task_detail(connection, task_id).await? {
        TaskDetailResult::History(detail) => Ok(Some(detail)),
        TaskDetailResult::Live { .. } | TaskDetailResult::Absent { .. } => Ok(None),
    }
}

pub async fn get_task_detail(
    broker: &PostgresBroker,
    task_id: Uuid,
) -> MonitoringResult<Option<TaskDetail>> {
    let operation = "task detail query";
    let mut connection = broker
        .pool()
        .acquire()
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))?;
    let live: Option<LiveTaskDetailRow> = sqlx::query_as(
        "SELECT id, task_name, queue_name, status, priority, retry_count,
                max_retries, is_workflow_task, error_code, failed_reason,
                worker_hostname, enqueued_at, started_at, completed_at,
                failed_at, good_until
         FROM horsies_tasks WHERE id = $1",
    )
    .bind(task_id)
    .fetch_optional(&mut *connection)
    .await
    .map_err(|error| MonitoringQueryError::database(operation, error))?;
    let link = workflow_link(&mut connection, task_id)
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))?;
    let (workflow_id, workflow_task_index) = link
        .map(|(workflow_id, index)| (Some(workflow_id), Some(index)))
        .unwrap_or((None, None));
    if let Some(live) = live {
        let attempts = live_attempts(&mut connection, task_id)
            .await
            .map_err(|error| MonitoringQueryError::database(operation, error))?;
        return Ok(Some(TaskDetail {
            leaf: live.leaf(),
            task_name: live.task_name,
            queue_name: live.queue_name,
            priority: live.priority,
            is_workflow_task: live.is_workflow_task,
            error_category: category_value(live.error_code.as_deref()),
            attempts,
            workflow_id,
            workflow_task_index,
        }));
    }
    let history = history_detail_or_none(&mut connection, task_id)
        .await
        .map_err(|error| MonitoringQueryError::history(operation, error))?;
    Ok(history.map(|history| TaskDetail {
        leaf: history_leaf(&history),
        task_name: history.task_name.clone(),
        queue_name: history.queue_name.clone(),
        priority: history.priority,
        is_workflow_task: history.is_workflow_task,
        error_category: category_value(history.error_code.as_deref()),
        attempts: history.attempts.iter().map(history_attempt).collect(),
        workflow_id,
        workflow_task_index,
    }))
}

#[derive(Debug, FromRow)]
struct WorkflowSummaryRow {
    id: Uuid,
    name: String,
    definition_key: Option<String>,
    status: String,
    created_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
}

impl From<WorkflowSummaryRow> for WorkflowRunSummary {
    fn from(row: WorkflowSummaryRow) -> Self {
        Self {
            id: row.id,
            name: row.name,
            definition_key: row.definition_key,
            status: row.status,
            created_at: Some(row.created_at),
            completed_at: row.completed_at,
            wall_s: elapsed_s(Some(row.created_at), row.completed_at),
        }
    }
}

#[derive(Debug, FromRow)]
struct WorkflowNodeRow {
    task_index: i32,
    node_id: Option<String>,
    task_name: String,
    status: String,
    dependencies: Vec<i32>,
    allow_failed_deps: bool,
    task_id: Option<Uuid>,
    is_subworkflow: bool,
    sub_workflow_id: Option<Uuid>,
    error: Option<String>,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
}

fn node_exec_s(row: &WorkflowNodeRow) -> Option<i64> {
    match row.status.as_str() {
        "RUNNING" => elapsed_s(row.started_at, None),
        "COMPLETED" | "FAILED" | "SKIPPED" | "CANCELLED" if row.completed_at.is_some() => {
            elapsed_s(row.started_at, row.completed_at)
        }
        _ => None,
    }
}

pub async fn list_workflow_names(broker: &PostgresBroker) -> MonitoringResult<Vec<String>> {
    let operation = "workflow names query";
    sqlx::query_scalar(
        "SELECT DISTINCT name FROM horsies_workflows
         WHERE parent_workflow_id IS NULL ORDER BY name",
    )
    .fetch_all(broker.pool())
    .await
    .map_err(|error| MonitoringQueryError::database(operation, error))
}

pub async fn list_workflow_runs(
    broker: &PostgresBroker,
    query: &WorkflowRunsQuery,
) -> MonitoringResult<Vec<WorkflowRunSummary>> {
    let operation = "workflow runs query";
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT id, name, definition_key, status, created_at, completed_at
         FROM horsies_workflows WHERE parent_workflow_id IS NULL",
    );
    if let Some(name) = &query.name {
        builder.push(" AND name = ").push_bind(name.clone());
    }
    if let Some(status) = &query.status {
        builder.push(" AND status = ").push_bind(status.clone());
    }
    builder
        .push(" ORDER BY created_at DESC LIMIT ")
        .push_bind(query.limit);
    let rows: Vec<WorkflowSummaryRow> = builder
        .build_query_as()
        .fetch_all(broker.pool())
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))?;
    Ok(rows.into_iter().map(Into::into).collect())
}

async fn fetch_workflow_nodes(
    connection: &mut PgConnection,
    workflow_id: Uuid,
) -> Result<Vec<WorkflowNodeRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT workflow_id, task_index, node_id, task_name, status,
                dependencies, allow_failed_deps, task_id, is_subworkflow,
                sub_workflow_id, error, started_at, completed_at
         FROM horsies_workflow_tasks
         WHERE workflow_id = $1
         ORDER BY task_index",
    )
    .bind(workflow_id)
    .fetch_all(connection)
    .await
}

pub async fn get_workflow_run(
    broker: &PostgresBroker,
    workflow_id: Uuid,
) -> MonitoringResult<Option<WorkflowRunDetail>> {
    let operation = "workflow run detail query";
    let mut connection = broker
        .pool()
        .acquire()
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))?;
    let run: Option<WorkflowSummaryRow> = sqlx::query_as(
        "SELECT id, name, definition_key, status, created_at, completed_at
         FROM horsies_workflows WHERE id = $1",
    )
    .bind(workflow_id)
    .fetch_optional(&mut *connection)
    .await
    .map_err(|error| MonitoringQueryError::database(operation, error))?;
    let Some(run) = run else {
        return Ok(None);
    };
    let rows = fetch_workflow_nodes(&mut connection, workflow_id)
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))?;
    let child_ids: Vec<Uuid> = rows
        .iter()
        .filter(|row| row.is_subworkflow)
        .filter_map(|row| row.sub_workflow_id)
        .collect();
    let child_rollup: HashMap<Uuid, (i64, i64)> = if child_ids.is_empty() {
        HashMap::new()
    } else {
        let rows: Vec<(Uuid, i64, i64)> = sqlx::query_as(
            "SELECT workflow_id, count(*)::bigint,
                    count(*) FILTER (WHERE status = 'FAILED')::bigint
             FROM horsies_workflow_tasks
             WHERE workflow_id = ANY($1)
             GROUP BY workflow_id",
        )
        .bind(child_ids)
        .fetch_all(&mut *connection)
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))?;
        rows.into_iter()
            .map(|(id, total, failed)| (id, (total, failed)))
            .collect()
    };
    let known_indices: Vec<i32> = rows.iter().map(|row| row.task_index).collect();
    let mut edges = Vec::new();
    let mut nodes = Vec::with_capacity(rows.len());
    for row in rows {
        for dependency in &row.dependencies {
            if known_indices.contains(dependency) {
                edges.push(WorkflowEdge {
                    from_index: *dependency,
                    to_index: row.task_index,
                });
            }
        }
        let child_counts = match (row.is_subworkflow, row.sub_workflow_id) {
            (true, Some(id)) => child_rollup.get(&id).copied(),
            (true, None) | (false, _) => None,
        };
        let (child_total, child_failed) = child_counts
            .map(|(total, failed)| (Some(total), Some(failed)))
            .unwrap_or((None, None));
        nodes.push(WorkflowNodeInfo {
            task_index: row.task_index,
            node_id: row.node_id.clone(),
            task_name: row.task_name.clone(),
            node_status: row.status.clone(),
            is_subworkflow: row.is_subworkflow,
            sub_workflow_id: row.sub_workflow_id,
            allow_failed_deps: row.allow_failed_deps,
            started_at: row.started_at,
            completed_at: row.completed_at,
            exec_s: node_exec_s(&row),
            child_total,
            child_failed,
        });
    }
    let failed_indices: Vec<i32> = nodes
        .iter()
        .filter(|node| node.node_status == "FAILED")
        .map(|node| node.task_index)
        .collect();
    Ok(Some(WorkflowRunDetail {
        run: run.into(),
        nodes,
        edges,
        failed_count: i64::try_from(failed_indices.len()).unwrap_or(i64::MAX),
        failed_indices,
    }))
}

pub async fn get_workflow_node(
    broker: &PostgresBroker,
    workflow_id: Uuid,
    task_index: i32,
) -> MonitoringResult<Option<WorkflowTaskDetail>> {
    let operation = "workflow node detail query";
    let mut connection = broker
        .pool()
        .acquire()
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))?;
    let row: Option<WorkflowNodeRow> = sqlx::query_as(
        "SELECT workflow_id, task_index, node_id, task_name, status,
                dependencies, allow_failed_deps, task_id, is_subworkflow,
                sub_workflow_id, error, started_at, completed_at
         FROM horsies_workflow_tasks
         WHERE workflow_id = $1 AND task_index = $2",
    )
    .bind(workflow_id)
    .bind(task_index)
    .fetch_optional(&mut *connection)
    .await
    .map_err(|error| MonitoringQueryError::database(operation, error))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let mut leaf = None;
    let mut attempts = Vec::new();
    if let Some(task_id) = row.task_id {
        attempts = live_attempts(&mut connection, task_id)
            .await
            .map_err(|error| MonitoringQueryError::database(operation, error))?;
        let live: Option<LiveTaskDetailRow> = sqlx::query_as(
            "SELECT id, task_name, queue_name, status, priority, retry_count,
                    max_retries, is_workflow_task, error_code, failed_reason,
                    worker_hostname, enqueued_at, started_at, completed_at,
                    failed_at, good_until
             FROM horsies_tasks WHERE id = $1",
        )
        .bind(task_id)
        .fetch_optional(&mut *connection)
        .await
        .map_err(|error| MonitoringQueryError::database(operation, error))?;
        if let Some(live) = live {
            leaf = Some(live.leaf());
        } else if let Some(history) = history_detail_or_none(&mut connection, task_id)
            .await
            .map_err(|error| MonitoringQueryError::history(operation, error))?
        {
            leaf = Some(history_leaf(&history));
            attempts = history.attempts.iter().map(history_attempt).collect();
        }
    }
    Ok(Some(WorkflowTaskDetail {
        task_index: row.task_index,
        node_id: row.node_id,
        task_name: row.task_name,
        node_status: row.status,
        is_subworkflow: row.is_subworkflow,
        node_error: normalize_optional_text(row.error.as_deref()),
        leaf,
        attempts,
    }))
}

pub async fn list_schedules(broker: &PostgresBroker) -> MonitoringResult<Vec<ScheduleStateInfo>> {
    let operation = "schedule state query";
    sqlx::query_as(
        "SELECT schedule_name, last_run_at, next_run_at, last_task_id,
                run_count, updated_at
         FROM horsies_schedule_state
         ORDER BY next_run_at ASC NULLS LAST",
    )
    .fetch_all(broker.pool())
    .await
    .map_err(|error| MonitoringQueryError::database(operation, error))
}
