//! Planner estimates over history: the row estimate of an
//! `EXPLAIN (FORMAT JSON)` text, which the unfiltered task list total reads
//! (`monitoring::function_rows`, `ListTotals`).

use serde_json::Value;

use crate::core::history::errors::HistoryError;

/// The top plan row estimate of an `EXPLAIN (FORMAT JSON)` value.
fn plan_rows_from_explain(payload: &Value) -> Result<i64, HistoryError> {
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

/// The top plan row estimate of an `EXPLAIN (FORMAT JSON)` text. Errors: a
/// text that is not JSON, or a JSON value without a numeric top
/// `Plan Rows`.
pub fn plan_rows_from_explain_text(payload: &str) -> Result<i64, HistoryError> {
    let parsed: Value = serde_json::from_str(payload)
        .map_err(|_| HistoryError::contract("EXPLAIN payload is not valid JSON"))?;
    plan_rows_from_explain(&parsed)
}
