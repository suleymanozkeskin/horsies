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

/// The `LIMIT` of a list read: one row more than [`TASK_LIST_BOUND`], so a
/// truncated list can be told from a complete one.
pub(crate) const TASK_LIST_READ_LIMIT: i64 = TASK_LIST_BOUND as i64 + 1;

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
    fn the_read_limit_is_one_past_the_bound() {
        assert_eq!(TASK_LIST_READ_LIMIT, 10_001);
    }
}
