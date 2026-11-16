//! Bounds of the stale and expired task lists.
//!
//! Each list reads at most [`TASK_LIST_BOUND`] rows and says when it stopped
//! there. The stale threshold is whole minutes from 0 to one year; another
//! value is `INVALID_INPUT` before any read.

use crate::broker::result_types::{BrokerErrorCode, BrokerOperationError};

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
    /// [`MAX_STALE_THRESHOLD_MINUTES`] is `INVALID_INPUT`, not retryable.
    pub fn parse(minutes: i32) -> Result<Self, BrokerOperationError> {
        match (0..=MAX_STALE_THRESHOLD_MINUTES).contains(&minutes) {
            true => Ok(Self(minutes)),
            false => Err(BrokerOperationError {
                code: BrokerErrorCode::InvalidInput,
                message: format!(
                    "stale_threshold_minutes must be 0 to {MAX_STALE_THRESHOLD_MINUTES}, \
                     got {minutes}"
                ),
                retryable: false,
            }),
        }
    }

    /// The threshold in minutes.
    pub const fn minutes(self) -> i32 {
        self.0
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

    #[test]
    fn a_threshold_of_zero_to_one_year_is_accepted() {
        for minutes in [0, 1, 60, MAX_STALE_THRESHOLD_MINUTES] {
            assert_eq!(
                StaleMinutes::parse(minutes).map(StaleMinutes::minutes).ok(),
                Some(minutes)
            );
        }
    }

    #[test]
    fn a_threshold_outside_the_range_is_invalid_input() {
        for minutes in [-1, i32::MIN, MAX_STALE_THRESHOLD_MINUTES + 1, i32::MAX] {
            let refused = StaleMinutes::parse(minutes).expect_err("refused");
            assert_eq!(refused.code, BrokerErrorCode::InvalidInput, "{minutes}");
            assert!(!refused.retryable, "{minutes}");
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
