//! Window-scoped history page and facet statement builders.

use chrono::{DateTime, Utc};
use sqlx::postgres::PgArguments;
use sqlx::query::Query;
use sqlx::Postgres;

use crate::core::history::errors::HistoryError;
use crate::core::history::names::TASK_HISTORY_PARENT;

pub const HISTORY_SUMMARY_COLUMNS: [&str; 18] = [
    "task_id",
    "task_name",
    "queue_name",
    "priority",
    "status",
    "terminalization_kind",
    "terminal_at",
    "retention_class_key",
    "enqueued_at",
    "started_at",
    "created_at",
    "retry_count",
    "max_retries",
    "error_code",
    "last_claimed_worker_id",
    "last_worker_hostname",
    "workflow_id",
    "is_workflow_task",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryFacet {
    TaskName,
    QueueName,
    Status,
    TerminalizationKind,
    ErrorCode,
    RetentionClassKey,
    Worker,
}

impl HistoryFacet {
    pub const ALL: [Self; 7] = [
        Self::TaskName,
        Self::QueueName,
        Self::Status,
        Self::TerminalizationKind,
        Self::ErrorCode,
        Self::RetentionClassKey,
        Self::Worker,
    ];

    pub fn column(self) -> &'static str {
        match self {
            Self::TaskName => "task_name",
            Self::QueueName => "queue_name",
            Self::Status => "status",
            Self::TerminalizationKind => "terminalization_kind",
            Self::ErrorCode => "error_code",
            Self::RetentionClassKey => "retention_class_key",
            Self::Worker => "last_claimed_worker_id",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistorySort {
    TerminalAtDescending,
    TerminalAtAscending,
}

impl HistorySort {
    pub const ALL: [Self; 2] = [Self::TerminalAtDescending, Self::TerminalAtAscending];

    fn expression(self) -> &'static str {
        match self {
            Self::TerminalAtDescending => "terminal_at DESC",
            Self::TerminalAtAscending => "terminal_at ASC",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistorySortField {
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

impl HistorySortField {
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

    pub fn parse(value: &str) -> Result<Self, HistoryError> {
        match value {
            "enqueued_at" => Ok(Self::EnqueuedAt),
            "started_at" => Ok(Self::StartedAt),
            "completed_at" => Ok(Self::CompletedAt),
            "failed_at" => Ok(Self::FailedAt),
            "status" => Ok(Self::Status),
            "task_name" => Ok(Self::TaskName),
            "queue_name" => Ok(Self::QueueName),
            "priority" => Ok(Self::Priority),
            "retry_count" => Ok(Self::RetryCount),
            "queue_s" => Ok(Self::QueueSeconds),
            "exec_s" => Ok(Self::ExecutionSeconds),
            other => Err(HistoryError::contract(format!(
                "unknown history sort field: {other:?}"
            ))),
        }
    }

    fn expression(self) -> (&'static str, bool) {
        match self {
            Self::EnqueuedAt => ("enqueued_at", false),
            Self::StartedAt => ("started_at", true),
            Self::CompletedAt => ("CASE WHEN status = 'COMPLETED' THEN terminal_at END", true),
            Self::FailedAt => ("CASE WHEN status <> 'COMPLETED' THEN terminal_at END", true),
            Self::Status => ("status", false),
            Self::TaskName => ("task_name", false),
            Self::QueueName => ("queue_name", false),
            Self::Priority => ("priority", false),
            Self::RetryCount => ("retry_count", false),
            Self::QueueSeconds => ("(started_at - enqueued_at)", true),
            Self::ExecutionSeconds => ("(terminal_at - started_at)", true),
        }
    }
}

pub fn history_sort_expression(field: HistorySortField, descending: bool) -> String {
    let (expression, nullable) = field.expression();
    let direction = if descending { "DESC" } else { "ASC" };
    let null_order = if nullable { " NULLS LAST" } else { "" };
    format!("{expression} {direction}{null_order}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryWindow {
    lower: DateTime<Utc>,
    upper: DateTime<Utc>,
}

impl HistoryWindow {
    pub fn new(lower: DateTime<Utc>, upper: DateTime<Utc>) -> Result<Self, HistoryError> {
        if lower >= upper {
            return Err(HistoryError::contract(
                "history window bounds must be increasing",
            ));
        }
        Ok(Self { lower, upper })
    }

    pub fn lower(self) -> DateTime<Utc> {
        self.lower
    }

    pub fn upper(self) -> DateTime<Utc> {
        self.upper
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HistoryScope {
    pub statuses: Vec<String>,
    pub task_names: Vec<String>,
    pub queue_names: Vec<String>,
    pub workers: Vec<String>,
    pub error_codes: Vec<String>,
    pub category_families: Vec<Vec<String>>,
    pub domain_complement: Option<Vec<String>>,
    pub retried_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryBindValue {
    Timestamp(DateTime<Utc>),
    TextArray(Vec<String>),
    Integer(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryStatement {
    sql: String,
    parameters: Vec<HistoryBindValue>,
}

impl HistoryStatement {
    pub(crate) fn new(sql: String, parameters: Vec<HistoryBindValue>) -> Self {
        Self { sql, parameters }
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn parameters(&self) -> &[HistoryBindValue] {
        &self.parameters
    }

    pub fn query(&self) -> Query<'_, Postgres, PgArguments> {
        let mut query = sqlx::query(&self.sql);
        for parameter in &self.parameters {
            query = match parameter {
                HistoryBindValue::Timestamp(value) => query.bind(*value),
                HistoryBindValue::TextArray(value) => query.bind(value.clone()),
                HistoryBindValue::Integer(value) => query.bind(*value),
            };
        }
        query
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryPageQuery {
    window: HistoryWindow,
    limit: i64,
    offset: i64,
    scope: HistoryScope,
    sort: HistorySort,
    order_by: Option<String>,
}

impl HistoryPageQuery {
    pub fn new(window: HistoryWindow, limit: i64) -> Result<Self, HistoryError> {
        if !(1..=500).contains(&limit) {
            return Err(HistoryError::contract(
                "page limit must be between 1 and 500",
            ));
        }
        Ok(Self {
            window,
            limit,
            offset: 0,
            scope: HistoryScope::default(),
            sort: HistorySort::TerminalAtDescending,
            order_by: None,
        })
    }

    pub fn with_offset(mut self, offset: i64) -> Result<Self, HistoryError> {
        if offset < 0 {
            return Err(HistoryError::contract("page offset must be non-negative"));
        }
        self.offset = offset;
        Ok(self)
    }

    pub fn with_scope(mut self, scope: HistoryScope) -> Self {
        self.scope = scope;
        self
    }

    pub fn with_terminal_sort(mut self, sort: HistorySort) -> Self {
        self.sort = sort;
        self.order_by = None;
        self
    }

    pub fn with_sort_field(mut self, field: HistorySortField, descending: bool) -> Self {
        self.order_by = Some(history_sort_expression(field, descending));
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryFacetQuery {
    window: HistoryWindow,
    facet: HistoryFacet,
    limit: i64,
    statuses: Vec<String>,
    retried_only: bool,
}

impl HistoryFacetQuery {
    pub fn new(window: HistoryWindow, facet: HistoryFacet) -> Self {
        Self {
            window,
            facet,
            limit: 50,
            statuses: Vec::new(),
            retried_only: false,
        }
    }

    pub fn with_limit(mut self, limit: i64) -> Result<Self, HistoryError> {
        if !(1..=200).contains(&limit) {
            return Err(HistoryError::contract(
                "facet limit must be between 1 and 200",
            ));
        }
        self.limit = limit;
        Ok(self)
    }

    pub fn with_statuses(mut self, statuses: Vec<String>) -> Self {
        self.statuses = statuses;
        self
    }

    pub fn retried_only(mut self, retried_only: bool) -> Self {
        self.retried_only = retried_only;
        self
    }
}

pub(crate) fn history_scope_conditions(
    window: HistoryWindow,
    scope: &HistoryScope,
) -> (Vec<String>, Vec<HistoryBindValue>) {
    let (mut conditions, mut parameters) = window_conditions(window);
    for (column, values) in [
        ("status", &scope.statuses),
        ("task_name", &scope.task_names),
        ("queue_name", &scope.queue_names),
        ("last_claimed_worker_id", &scope.workers),
    ] {
        if !values.is_empty() {
            let position = parameters.len() + 1;
            conditions.push(format!("{column} = ANY(${position}::text[])"));
            parameters.push(HistoryBindValue::TextArray(values.clone()));
        }
    }
    if !scope.error_codes.is_empty() {
        let position = parameters.len() + 1;
        conditions.push(format!("error_code = ANY(${position}::text[])"));
        parameters.push(HistoryBindValue::TextArray(scope.error_codes.clone()));
    }
    let mut category_arms = Vec::new();
    for family in &scope.category_families {
        let position = parameters.len() + 1;
        category_arms.push(format!("error_code = ANY(${position}::text[])"));
        parameters.push(HistoryBindValue::TextArray(family.clone()));
    }
    if let Some(domain_complement) = &scope.domain_complement {
        let position = parameters.len() + 1;
        category_arms.push(format!(
            "(error_code IS NOT NULL AND error_code <> '' AND error_code <> ALL(${position}::text[]))"
        ));
        parameters.push(HistoryBindValue::TextArray(domain_complement.clone()));
    }
    if !category_arms.is_empty() {
        conditions.push(format!("({})", category_arms.join(" OR ")));
    }
    if scope.retried_only {
        conditions.push("retry_count > 0".to_owned());
    }
    (conditions, parameters)
}

pub fn history_page_statement(query: &HistoryPageQuery) -> HistoryStatement {
    let (conditions, mut parameters) = history_scope_conditions(query.window, &query.scope);
    let limit_position = parameters.len() + 1;
    parameters.push(HistoryBindValue::Integer(query.limit));
    let offset_position = parameters.len() + 1;
    parameters.push(HistoryBindValue::Integer(query.offset));
    let order = query.order_by.as_deref().unwrap_or(query.sort.expression());
    HistoryStatement::new(
        format!(
            "SELECT {} FROM {TASK_HISTORY_PARENT} WHERE {} ORDER BY {order} LIMIT ${limit_position} OFFSET ${offset_position}",
            HISTORY_SUMMARY_COLUMNS.join(", "),
            conditions.join(" AND ")
        ),
        parameters,
    )
}

pub fn history_facet_statement(query: &HistoryFacetQuery) -> HistoryStatement {
    let (mut conditions, mut parameters) = window_conditions(query.window);
    if !query.statuses.is_empty() {
        let position = parameters.len() + 1;
        conditions.push(format!("status = ANY(${position}::text[])"));
        parameters.push(HistoryBindValue::TextArray(query.statuses.clone()));
    }
    if query.retried_only {
        conditions.push("retry_count > 0".to_owned());
    }
    let column = query.facet.column();
    conditions.push(format!("{column} IS NOT NULL"));
    if query.facet == HistoryFacet::ErrorCode {
        conditions.push("error_code <> ''".to_owned());
    }
    let limit_position = parameters.len() + 1;
    parameters.push(HistoryBindValue::Integer(query.limit));
    HistoryStatement::new(
        format!(
            "SELECT {column} AS facet_value, count(*) AS facet_count FROM {TASK_HISTORY_PARENT} WHERE {} GROUP BY {column} ORDER BY facet_count DESC, facet_value LIMIT ${limit_position}",
            conditions.join(" AND ")
        ),
        parameters,
    )
}

pub(crate) fn window_conditions(window: HistoryWindow) -> (Vec<String>, Vec<HistoryBindValue>) {
    (
        vec![
            "retention_anchor_at >= $1".to_owned(),
            "retention_anchor_at < $2".to_owned(),
        ],
        vec![
            HistoryBindValue::Timestamp(window.lower),
            HistoryBindValue::Timestamp(window.upper),
        ],
    )
}
