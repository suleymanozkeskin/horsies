//! Bounds of the stale and expired task lists.
//!
//! Each list reads at most [`TASK_LIST_BOUND`] rows and says when it stopped
//! there. The stale threshold is whole minutes from 0 to one year; another
//! value is `INVALID_INPUT` before any read, with Python's text.

use crate::broker::result_types::BrokerOperationError;

/// The most rows a stale or expired task list returns.
pub const TASK_LIST_BOUND: usize = 10_000;

/// The largest stale threshold, in minutes: one year of 365 days.
pub const MAX_STALE_THRESHOLD_MINUTES: i32 = 525_600;

/// A stale threshold of 0 to [`MAX_STALE_THRESHOLD_MINUTES`] whole minutes.
/// Only [`StaleMinutes::parse`] builds one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaleMinutes(i32);

impl StaleMinutes {
    /// Parse the caller's threshold. A value outside 0 to
    /// [`MAX_STALE_THRESHOLD_MINUTES`] is refused (`INVALID_INPUT`, not
    /// retryable, through [`StaleThresholdRefusal::error`]). Pure.
    pub fn parse(minutes: i64) -> Result<Self, StaleThresholdRefusal> {
        match i32::try_from(minutes) {
            Ok(fits) if (0..=MAX_STALE_THRESHOLD_MINUTES).contains(&fits) => Ok(Self(fits)),
            _ if minutes < 0 => Err(StaleThresholdRefusal::BelowZero),
            _ => Err(StaleThresholdRefusal::AboveMaximum),
        }
    }

    /// The threshold in minutes.
    pub const fn minutes(self) -> i32 {
        self.0
    }
}

/// Why a stale threshold is refused, in Python's terms
/// (`_stale_threshold_minutes`, `clients/python/horsies/core/brokers/postgres.py`).
/// A client that takes untyped values (Python) also refuses a `bool` and a
/// value that is not an integer with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StaleThresholdRefusal {
    /// A Python `bool`: an `int` subclass that the threshold refuses.
    Bool,
    /// A value that is not an integer; `type_name` is the name of its type.
    NotAnInteger {
        type_name: String,
    },
    BelowZero,
    AboveMaximum,
}

impl StaleThresholdRefusal {
    /// The `INVALID_INPUT` error of the refusal, not retryable, with
    /// Python's text: `stale_threshold_minutes <cause>; give whole minutes
    /// from 0 to 525600`. The text holds no value.
    pub fn error(&self) -> BrokerOperationError {
        let cause = match self {
            Self::Bool => "is a bool, not an int".to_owned(),
            Self::NotAnInteger { type_name } => format!("is a {type_name}, not an int"),
            Self::BelowZero => "is below 0".to_owned(),
            Self::AboveMaximum => format!("is above {MAX_STALE_THRESHOLD_MINUTES} (one year)"),
        };
        BrokerOperationError::invalid_input(format!(
            "stale_threshold_minutes {cause}; give whole minutes from 0 to \
             {MAX_STALE_THRESHOLD_MINUTES}"
        ))
    }
}

/// A task list and whether it stopped at its bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundedTaskList<T> {
    /// Every matching row.
    Complete(Vec<T>),
    /// The first `bound` matching rows; more rows match.
    Truncated { rows: Vec<T>, bound: usize },
}

impl<T> BoundedTaskList<T> {
    /// Build the list from a read of at most `bound + 1` rows: one row more
    /// than the bound means the list is truncated.
    pub fn from_read(mut rows: Vec<T>, bound: usize) -> Self {
        match rows.len() > bound {
            true => {
                rows.truncate(bound);
                Self::Truncated { rows, bound }
            }
            false => Self::Complete(rows),
        }
    }

    /// The rows, whether or not the list is truncated.
    pub fn rows(&self) -> &[T] {
        match self {
            Self::Complete(rows) | Self::Truncated { rows, .. } => rows,
        }
    }
}

/// How a list function's `LIST_END` row ends the list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ListEnd {
    /// Every matching row came back.
    Complete,
    /// The function stopped at `bound` rows; more rows match.
    Truncated { bound: usize },
}

/// One row of a list function (`horsies_list_stale_tasks`,
/// `horsies_list_expired_tasks`): a task row, or the one `LIST_END` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ListRow<T> {
    Task(T),
    End(ListEnd),
}

/// The rows of a list function that break its result contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ListShapeError {
    #[error("the list has no LIST_END row")]
    NoEnd,
    #[error("a row follows the LIST_END row")]
    RowAfterEnd,
}

/// The list from the rows of a list function, in their order: task rows,
/// then exactly one `LIST_END` row, last. Pure.
pub(crate) fn bounded_list_from_rows<T>(
    rows: Vec<ListRow<T>>,
) -> Result<BoundedTaskList<T>, ListShapeError> {
    let mut tasks = Vec::with_capacity(rows.len());
    let mut end = None;
    for row in rows {
        match (row, end) {
            (_, Some(_)) => return Err(ListShapeError::RowAfterEnd),
            (ListRow::Task(task), None) => tasks.push(task),
            (ListRow::End(list_end), None) => end = Some(list_end),
        }
    }
    match end {
        None => Err(ListShapeError::NoEnd),
        Some(ListEnd::Complete) => Ok(BoundedTaskList::Complete(tasks)),
        Some(ListEnd::Truncated { bound }) => Ok(BoundedTaskList::Truncated { rows: tasks, bound }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::result_types::BrokerErrorCode;

    #[test]
    fn a_threshold_of_zero_to_one_year_is_accepted() {
        for minutes in [0, 1, 60, MAX_STALE_THRESHOLD_MINUTES] {
            assert_eq!(
                StaleMinutes::parse(i64::from(minutes))
                    .map(StaleMinutes::minutes)
                    .ok(),
                Some(minutes)
            );
        }
    }

    /// The refusal texts.
    #[test]
    fn a_threshold_outside_the_range_is_invalid_input_with_python_text() {
        for (minutes, refusal) in [
            (-1, StaleThresholdRefusal::BelowZero),
            (i64::MIN, StaleThresholdRefusal::BelowZero),
            (
                i64::from(MAX_STALE_THRESHOLD_MINUTES) + 1,
                StaleThresholdRefusal::AboveMaximum,
            ),
            (i64::from(i32::MAX) + 1, StaleThresholdRefusal::AboveMaximum),
            (i64::MAX, StaleThresholdRefusal::AboveMaximum),
        ] {
            assert_eq!(
                StaleMinutes::parse(minutes),
                Err(refusal.clone()),
                "{minutes}"
            );
            let error = refusal.error();
            assert_eq!(error.code(), BrokerErrorCode::InvalidInput, "{minutes}");
            assert!(!error.retryable(), "{minutes}");
        }
        for (refusal, cause) in [
            (StaleThresholdRefusal::BelowZero, "is below 0"),
            (
                StaleThresholdRefusal::AboveMaximum,
                "is above 525600 (one year)",
            ),
            (StaleThresholdRefusal::Bool, "is a bool, not an int"),
            (
                StaleThresholdRefusal::NotAnInteger {
                    type_name: "str".to_owned(),
                },
                "is a str, not an int",
            ),
        ] {
            assert_eq!(
                refusal.error().message(),
                format!("stale_threshold_minutes {cause}; give whole minutes from 0 to 525600")
            );
        }
    }

    #[test]
    fn a_read_at_the_bound_is_complete() {
        assert_eq!(
            BoundedTaskList::from_read(vec![1, 2, 3], 3),
            BoundedTaskList::Complete(vec![1, 2, 3])
        );
    }

    #[test]
    fn a_read_past_the_bound_is_truncated_to_the_bound() {
        let list = BoundedTaskList::from_read(vec![1, 2, 3, 4], 3);

        assert_eq!(
            list,
            BoundedTaskList::Truncated {
                rows: vec![1, 2, 3],
                bound: 3
            }
        );
        assert_eq!(list.rows(), &[1, 2, 3]);
    }

    #[test]
    fn function_rows_end_with_their_list_end_row() {
        assert_eq!(
            bounded_list_from_rows(vec![
                ListRow::Task(1),
                ListRow::Task(2),
                ListRow::End(ListEnd::Complete)
            ]),
            Ok(BoundedTaskList::Complete(vec![1, 2]))
        );
        assert_eq!(
            bounded_list_from_rows(vec![
                ListRow::Task(1),
                ListRow::End(ListEnd::Truncated { bound: 1 })
            ]),
            Ok(BoundedTaskList::Truncated {
                rows: vec![1],
                bound: 1
            })
        );
        assert_eq!(
            bounded_list_from_rows::<i32>(vec![ListRow::End(ListEnd::Complete)]),
            Ok(BoundedTaskList::Complete(Vec::new()))
        );
    }

    #[test]
    fn function_rows_without_one_last_list_end_row_break_the_contract() {
        assert_eq!(
            bounded_list_from_rows(vec![ListRow::Task(1)]),
            Err(ListShapeError::NoEnd)
        );
        assert_eq!(
            bounded_list_from_rows(vec![ListRow::End(ListEnd::Complete), ListRow::Task(1)]),
            Err(ListShapeError::RowAfterEnd)
        );
        assert_eq!(
            bounded_list_from_rows::<i32>(vec![
                ListRow::End(ListEnd::Complete),
                ListRow::End(ListEnd::Complete)
            ]),
            Err(ListShapeError::RowAfterEnd)
        );
    }
}
