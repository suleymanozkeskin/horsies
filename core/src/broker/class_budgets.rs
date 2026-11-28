//! The class budgets of a worker state snapshot: the `class_capacity` column.
//!
//! Python reference: `ExecutionClassBudget`, `ExecutionClassBudgets` and `parse_class_capacity`
//! (`clients/python/horsies/core/models/health.py:261-395`).
//!
//! This module is pure. It does no I/O.

use serde_json::{Map, Value};

use crate::broker::health::{stored_value_text, stored_whole_number, STORED_COUNT_LIMIT};
use crate::core::task::Execution;

/// One claim budget of a worker, as its snapshot recorded it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionClassBudget {
    /// The class whose children (`ISOLATED`) or seats (a shared class) make
    /// the budget.
    pub budget: Execution,
    /// The children or seats of the budget that were in service, 0 or more.
    pub capacity: u64,
    /// The classes whose rows the budget claims: each class once, in
    /// [`Execution`] order. It always holds `budget`.
    pub serves: Vec<Execution>,
}

/// The claim budgets of a worker, in [`Execution`] order. Only
/// [`parse_class_capacity`] builds it.
///
/// No class is in the `serves` of two budgets. A class that no budget serves
/// is not claimed by the worker. Empty when the worker serves no class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionClassBudgets(Vec<ExecutionClassBudget>);

impl ExecutionClassBudgets {
    /// The budgets, in [`Execution`] order.
    pub fn as_slice(&self) -> &[ExecutionClassBudget] {
        &self.0
    }

    /// The budget whose `serves` holds `execution`; `None` when the worker
    /// does not claim rows of that class.
    pub fn serving(&self, execution: Execution) -> Option<&ExecutionClassBudget> {
        self.0
            .iter()
            .find(|budget| budget.serves.contains(&execution))
    }
}

/// A stored `class_capacity` value does not have the shape that the worker
/// writes. `reason` names the field and the cause.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{reason}")]
pub struct ClassCapacityUnreadable {
    pub reason: String,
}

fn unreadable(reason: impl Into<String>) -> ClassCapacityUnreadable {
    ClassCapacityUnreadable {
        reason: reason.into(),
    }
}

/// The class budgets of a stored `class_capacity` value (Python
/// `parse_class_capacity`, `clients/python/horsies/core/models/health.py:298-329`).
///
/// Each key must be an [`Execution`] name, and each entry an object with a
/// `capacity` (a whole number from 0 to `2^63 - 1`) and a `serves` array of
/// distinct [`Execution`] names that holds the key. No class may be in the
/// `serves` of two budgets. Other keys of an entry are not read. Any other
/// value is [`ClassCapacityUnreadable`]. Reads only.
pub fn parse_class_capacity(
    value: &Value,
) -> Result<ExecutionClassBudgets, ClassCapacityUnreadable> {
    let Value::Object(entries) = value else {
        return Err(unreadable("class_capacity is not a JSON object"));
    };
    let mut budgets = entries
        .iter()
        .map(|(name, entry)| class_budget(name, entry))
        .collect::<Result<Vec<_>, _>>()?;
    let mut served: Vec<Execution> = budgets
        .iter()
        .flat_map(|budget| budget.serves.iter().copied())
        .collect();
    let served_count = served.len();
    served.sort_unstable();
    served.dedup();
    if served.len() != served_count {
        return Err(unreadable("a class is in the serves of two budgets"));
    }
    budgets.sort_by_key(|budget| budget.budget);
    Ok(ExecutionClassBudgets(budgets))
}

/// One entry (Python `_parse_class_budget`,
/// `clients/python/horsies/core/models/health.py:332-368`), checked in Python's order.
fn class_budget(
    name: &str,
    value: &Value,
) -> Result<ExecutionClassBudget, ClassCapacityUnreadable> {
    let budget = Execution::from_stored(name)
        .ok_or_else(|| unreadable(format!("budget '{name}' is not an execution class")))?;
    let Value::Object(fields) = value else {
        return Err(unreadable(format!(
            "budget {} is not a JSON object",
            budget.as_str()
        )));
    };
    let capacity = stored_whole_number(fields.get("capacity"), "capacity", 0..STORED_COUNT_LIMIT)
        .map_err(unreadable)?;
    let serves = served_classes(fields)?;
    if !serves.contains(&budget) {
        return Err(unreadable(format!(
            "budget {} does not serve its own class",
            budget.as_str()
        )));
    }
    Ok(ExecutionClassBudget {
        budget,
        capacity,
        serves,
    })
}

/// The distinct classes of a `serves` array, in [`Execution`] order (Python
/// `_served_classes`, `clients/python/horsies/core/models/health.py:371-386`).
fn served_classes(fields: &Map<String, Value>) -> Result<Vec<Execution>, ClassCapacityUnreadable> {
    let Some(Value::Array(entries)) = fields.get("serves") else {
        return Err(unreadable("serves is not a JSON array"));
    };
    let mut members = entries
        .iter()
        .map(served_class)
        .collect::<Result<Vec<_>, _>>()?;
    let member_count = members.len();
    members.sort_unstable();
    members.dedup();
    if members.len() != member_count {
        return Err(unreadable("serves names one class twice"));
    }
    Ok(members)
}

fn served_class(entry: &Value) -> Result<Execution, ClassCapacityUnreadable> {
    entry
        .as_str()
        .and_then(Execution::from_stored)
        .ok_or_else(|| {
            unreadable(format!(
                "serves entry {} is not an execution class",
                stored_value_text(Some(entry))
            ))
        })
}

#[cfg(test)]
mod tests {
    //! One test per outcome of `parse_class_capacity`. Python reference:
    //! `clients/python/tests/unit/test_health_models.py::TestClassCapacity`.
    use super::*;
    use serde_json::json;

    const ALL_CLASSES: [&str; 3] = ["ISOLATED", "SHARED_ASYNC", "SHARED_THREAD"];

    fn reason(value: &Value) -> String {
        parse_class_capacity(value)
            .expect_err("the value is unreadable")
            .reason
    }

    #[test]
    fn an_empty_object_is_a_worker_that_serves_no_class() {
        let budgets = parse_class_capacity(&json!({})).expect("readable");
        assert!(budgets.as_slice().is_empty());
        assert_eq!(budgets.serving(Execution::Isolated), None);
    }

    #[test]
    fn budgets_and_serves_come_back_in_execution_order() {
        let stored = json!({
            "SHARED_ASYNC": {"capacity": 40, "serves": ["SHARED_ASYNC"]},
            "ISOLATED": {"capacity": 0, "serves": ["SHARED_THREAD", "ISOLATED"], "seats": 9},
        });

        let budgets = parse_class_capacity(&stored).expect("readable");

        assert_eq!(
            budgets.as_slice(),
            [
                ExecutionClassBudget {
                    budget: Execution::Isolated,
                    capacity: 0,
                    serves: vec![Execution::Isolated, Execution::SharedThread],
                },
                ExecutionClassBudget {
                    budget: Execution::SharedAsync,
                    capacity: 40,
                    serves: vec![Execution::SharedAsync],
                },
            ]
        );
        assert_eq!(
            budgets
                .serving(Execution::SharedThread)
                .map(|budget| budget.budget),
            Some(Execution::Isolated)
        );
    }

    #[test]
    fn a_value_that_is_not_an_object_is_unreadable() {
        for stored in [json!(["ISOLATED"]), Value::Null] {
            assert_eq!(reason(&stored), "class_capacity is not a JSON object");
        }
    }

    #[test]
    fn the_reason_names_an_unknown_budget() {
        let stored = json!({"EXCLUSIVE": {"capacity": 1, "serves": []}});
        assert_eq!(
            reason(&stored),
            "budget 'EXCLUSIVE' is not an execution class"
        );
    }

    #[test]
    fn an_entry_that_is_not_an_object_is_unreadable() {
        assert_eq!(
            reason(&json!({"ISOLATED": 1})),
            "budget ISOLATED is not a JSON object"
        );
    }

    #[test]
    fn a_capacity_outside_a_bigint_or_not_a_whole_number_is_unreadable() {
        let cases = [
            (json!(null), "capacity None is out of range"),
            (json!(-1), "capacity -1 is out of range"),
            (
                json!(1_u64 << 63),
                "capacity 9223372036854775808 is out of range",
            ),
            (json!("1"), "capacity '1' is out of range"),
            (json!(true), "capacity True is not a whole number"),
        ];
        for (capacity, expected) in cases {
            let stored = json!({"ISOLATED": {"capacity": capacity, "serves": ["ISOLATED"]}});
            assert_eq!(reason(&stored), expected);
        }
        let missing = json!({"ISOLATED": {"serves": ["ISOLATED"]}});
        assert_eq!(reason(&missing), "capacity None is out of range");
    }

    #[test]
    fn a_serves_that_breaks_a_rule_is_unreadable() {
        let cases = [
            (json!({"capacity": 1}), "serves is not a JSON array"),
            (
                json!({"capacity": 1, "serves": "ISOLATED"}),
                "serves is not a JSON array",
            ),
            (
                json!({"capacity": 1, "serves": ["ISOLATED", "BATCH"]}),
                "serves entry 'BATCH' is not an execution class",
            ),
            (
                json!({"capacity": 1, "serves": ["ISOLATED", "ISOLATED"]}),
                "serves names one class twice",
            ),
            (
                json!({"capacity": 1, "serves": ["SHARED_ASYNC"]}),
                "budget ISOLATED does not serve its own class",
            ),
        ];
        for (entry, expected) in cases {
            assert_eq!(reason(&json!({"ISOLATED": entry})), expected);
        }
    }

    #[test]
    fn one_class_in_two_budgets_is_unreadable() {
        let stored = json!({
            "ISOLATED": {"capacity": 1, "serves": ALL_CLASSES},
            "SHARED_ASYNC": {"capacity": 1, "serves": ["SHARED_ASYNC"]},
        });
        assert_eq!(reason(&stored), "a class is in the serves of two budgets");
    }
}
