//! Time-scoped aggregates and planner estimates over history.

use std::sync::LazyLock;

use serde_json::Value;

use crate::core::history::errors::HistoryError;
use crate::core::history::names::TASK_HISTORY_PARENT;

use super::pages::{
    history_scope_conditions, window_conditions, HistoryScope, HistoryStatement, HistoryWindow,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryStatusAggregate {
    window: HistoryWindow,
}

impl HistoryStatusAggregate {
    pub fn new(window: HistoryWindow) -> Self {
        Self { window }
    }
}

pub fn history_status_aggregate_statement(query: HistoryStatusAggregate) -> HistoryStatement {
    let (conditions, parameters) = window_conditions(query.window);
    HistoryStatement::new(
        format!(
            "SELECT status, terminalization_kind, count(*) AS terminal_count FROM {TASK_HISTORY_PARENT} WHERE {} GROUP BY status, terminalization_kind ORDER BY status, terminalization_kind",
            conditions.join(" AND ")
        ),
        parameters,
    )
}

pub fn history_scoped_status_counts_statement(
    window: HistoryWindow,
    scope: &HistoryScope,
) -> HistoryStatement {
    let (conditions, parameters) = history_scope_conditions(window, scope);
    HistoryStatement::new(
        format!(
            "SELECT status, count(*) AS terminal_count FROM {TASK_HISTORY_PARENT} WHERE {} GROUP BY status ORDER BY status",
            conditions.join(" AND ")
        ),
        parameters,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryBreakdownGroup {
    TaskName,
    QueueName,
    Worker,
}

impl HistoryBreakdownGroup {
    pub const ALL: [Self; 3] = [Self::TaskName, Self::QueueName, Self::Worker];

    fn column(self) -> &'static str {
        match self {
            Self::TaskName => "task_name",
            Self::QueueName => "queue_name",
            Self::Worker => "last_claimed_worker_id",
        }
    }
}

pub fn history_breakdown_statement(
    window: HistoryWindow,
    scope: &HistoryScope,
    group: HistoryBreakdownGroup,
) -> HistoryStatement {
    let (conditions, parameters) = history_scope_conditions(window, scope);
    let column = group.column();
    HistoryStatement::new(
        format!(
            "SELECT COALESCE({column}, 'unknown') AS group_value, status, count(*) AS status_count, count(*) FILTER (WHERE retry_count > 0) AS retried_count FROM {TASK_HISTORY_PARENT} WHERE {} GROUP BY COALESCE({column}, 'unknown'), status",
            conditions.join(" AND ")
        ),
        parameters,
    )
}

pub fn history_count_statement(window: HistoryWindow, scope: &HistoryScope) -> HistoryStatement {
    let (conditions, parameters) = history_scope_conditions(window, scope);
    HistoryStatement::new(
        format!(
            "SELECT count(*) FROM {TASK_HISTORY_PARENT} WHERE {}",
            conditions.join(" AND ")
        ),
        parameters,
    )
}

pub static HISTORY_NONEMPTY_PROBE_SQL: LazyLock<String> = LazyLock::new(history_nonempty_probe_sql);

fn history_nonempty_probe_sql() -> String {
    format!("SELECT EXISTS (SELECT 1 FROM {TASK_HISTORY_PARENT})")
}

pub fn history_estimate_statement(window: HistoryWindow, scope: &HistoryScope) -> HistoryStatement {
    let (conditions, parameters) = history_scope_conditions(window, scope);
    HistoryStatement::new(
        format!(
            "EXPLAIN (FORMAT JSON) SELECT 1 FROM {TASK_HISTORY_PARENT} WHERE {}",
            conditions.join(" AND ")
        ),
        parameters,
    )
}

pub fn plan_rows_from_explain(payload: &Value) -> Result<i64, HistoryError> {
    let rows = payload
        .as_array()
        .and_then(|plans| plans.first())
        .and_then(|root| root.get("Plan"))
        .and_then(|plan| plan.get("Plan Rows"))
        .ok_or_else(|| {
            HistoryError::contract("EXPLAIN payload did not carry a top plan row estimate")
        })?;
    match rows {
        Value::Number(number) => {
            if let Some(rows) = number.as_i64() {
                Ok(rows)
            } else if let Some(rows) = number.as_u64() {
                i64::try_from(rows)
                    .map_err(|_| HistoryError::contract("EXPLAIN plan row estimate exceeded i64"))
            } else if let Some(rows) = number.as_f64() {
                Ok(rows as i64)
            } else {
                Err(HistoryError::contract(
                    "EXPLAIN payload did not carry a top plan row estimate",
                ))
            }
        }
        Value::Bool(_) => Err(HistoryError::contract(
            "EXPLAIN plan row estimate decoded as boolean",
        )),
        _ => Err(HistoryError::contract(
            "EXPLAIN payload did not carry a top plan row estimate",
        )),
    }
}

pub fn plan_rows_from_explain_text(payload: &str) -> Result<i64, HistoryError> {
    let parsed: Value = serde_json::from_str(payload)
        .map_err(|_| HistoryError::contract("EXPLAIN payload is not valid JSON"))?;
    plan_rows_from_explain(&parsed)
}
