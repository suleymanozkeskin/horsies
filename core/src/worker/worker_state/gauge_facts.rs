//! The values of one worker snapshot that the worker gauges show, made from
//! the values that the snapshot writes to its columns. Pure.
//!
//! | Gauge | Value | Column of the same snapshot |
//! |---|---|---|
//! | `horsies.worker.seats{execution}` | [`seats_of`] | `class_capacity` |
//! | `horsies.worker.seats.in_use{execution}` | [`seats_in_use_of`] | `class_capacity`, and the statement that counts `tasks_running` and `tasks_claimed` |
//! | `horsies.worker.slots.down{kind}` | [`down_by_kind`] | `slots_down` |
//! | `horsies.worker.memory.rss{process="parent"}` | [`ParentRss`] | `memory_usage_mb` |

use super::BudgetColumn;
use crate::core::task::execution::Execution;
use crate::worker::executor::slots::DownState;

/// A count for each execution class.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClassCounts {
    pub isolated: u32,
    pub shared_async: u32,
    pub shared_thread: u32,
}

impl ClassCounts {
    /// The count of `class`.
    pub fn of(&self, class: Execution) -> u32 {
        match class {
            Execution::Isolated => self.isolated,
            Execution::SharedAsync => self.shared_async,
            Execution::SharedThread => self.shared_thread,
        }
    }

    /// These counts with `count` added to the count of `class`, at most
    /// `u32::MAX`.
    fn plus(self, class: Execution, count: u32) -> Self {
        match class {
            Execution::Isolated => Self {
                isolated: self.isolated.saturating_add(count),
                ..self
            },
            Execution::SharedAsync => Self {
                shared_async: self.shared_async.saturating_add(count),
                ..self
            },
            Execution::SharedThread => Self {
                shared_thread: self.shared_thread.saturating_add(count),
                ..self
            },
        }
    }
}

/// The resident memory of the worker parent at one snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParentRss {
    /// The RSS, in bytes.
    Bytes(u64),
    /// The process table gave no entry for the worker process.
    Unreadable,
}

/// The seats in use of each budget class at one snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeatsInUse {
    /// The snapshot counted the rows that the worker holds.
    Counted(ClassCounts),
    /// The count of the held rows failed: the snapshot has no value.
    NotCounted,
}

/// The seats of each budget class: the capacity in service of its budget,
/// as the `class_capacity` column stores it; 0 for a class with no budget.
pub fn seats_of(budgets: &[BudgetColumn]) -> ClassCounts {
    budgets
        .iter()
        .fold(ClassCounts::default(), |seats, column| {
            seats.plus(column.budget, column.capacity)
        })
}

/// The seats in use of each budget class: the held rows of the classes that
/// its budget serves, as the claim counts them (`RUNNING`, and `CLAIMED`
/// with a live lease, `in_flight`), at most the seats of the budget. A row
/// past the seats waits in the prefetch buffer and holds no seat. This is
/// the rule of `horsies.fleet.seats.in_use` for one worker. A held row of a
/// class that no budget serves is in no budget: the claim never takes one.
pub fn seats_in_use_of(budgets: &[BudgetColumn], in_flight: ClassCounts) -> ClassCounts {
    budgets
        .iter()
        .fold(ClassCounts::default(), |in_use, column| {
            let held = column.serves.iter().fold(0_u32, |held, class| {
                held.saturating_add(in_flight.of(*class))
            });
            in_use.plus(column.budget, held.min(column.capacity))
        })
}

/// The down slots of each kind: `down` is the list that the `slots_down`
/// column stores, and `kind_of` gives the kind of the children of a slot
/// from its board index.
pub fn down_by_kind(
    down: &[(usize, DownState)],
    kind_of: impl Fn(usize) -> Execution,
) -> ClassCounts {
    down.iter()
        .fold(ClassCounts::default(), |counts, (slot, _)| {
            counts.plus(kind_of(*slot), 1)
        })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use chrono::Utc;

    use super::*;
    use crate::worker::executor::supervisor::BootFailureCause;

    const ISOLATED: Execution = Execution::Isolated;
    const ASYNC: Execution = Execution::SharedAsync;
    const THREAD: Execution = Execution::SharedThread;

    fn budget(budget: Execution, capacity: u32, serves: &[Execution]) -> BudgetColumn {
        BudgetColumn {
            budget,
            capacity,
            serves: serves.to_vec(),
        }
    }

    fn counts(isolated: u32, shared_async: u32, shared_thread: u32) -> ClassCounts {
        ClassCounts {
            isolated,
            shared_async,
            shared_thread,
        }
    }

    #[test]
    fn the_seats_of_a_class_are_the_capacity_of_its_budget_and_0_with_none() {
        let budgets = [
            budget(ISOLATED, 3, &[ISOLATED, THREAD]),
            budget(ASYNC, 40, &[ASYNC]),
        ];

        assert_eq!(seats_of(&budgets), counts(3, 40, 0));
        assert_eq!(seats_of(&[]), ClassCounts::default());
    }

    /// The `ISOLATED` budget serves its own rows and the `SHARED_THREAD`
    /// rows; the rows past its seats wait in the prefetch buffer.
    #[test]
    fn the_seats_in_use_of_a_budget_are_the_rows_of_its_classes_up_to_its_seats() {
        let budgets = [
            budget(ISOLATED, 3, &[ISOLATED, THREAD]),
            budget(ASYNC, 40, &[ASYNC]),
        ];

        assert_eq!(seats_in_use_of(&budgets, counts(1, 7, 1)), counts(2, 7, 0));
        assert_eq!(
            seats_in_use_of(&budgets, counts(4, 41, 2)),
            counts(3, 40, 0)
        );
        assert_eq!(
            seats_in_use_of(&budgets, ClassCounts::default()),
            ClassCounts::default()
        );
    }

    #[test]
    fn a_held_row_of_a_class_that_no_budget_serves_is_in_no_budget() {
        let budgets = [budget(ASYNC, 40, &[ASYNC])];

        assert_eq!(seats_in_use_of(&budgets, counts(2, 1, 3)), counts(0, 1, 0));
    }

    #[test]
    fn the_down_slots_count_under_the_kind_of_their_children() {
        let down_state = || DownState {
            cause: BootFailureCause::ThreadStartRefused,
            detail: "can't start new thread".to_owned(),
            down_since: Utc::now(),
            failed_boots: NonZeroU32::MIN,
        };
        // Board slots 0-1 isolated, 2 shared async, 3-4 shared thread.
        let kind_of = |slot: usize| match slot {
            0 | 1 => ISOLATED,
            2 => ASYNC,
            _ => THREAD,
        };
        let down = [(1, down_state()), (3, down_state()), (4, down_state())];

        assert_eq!(down_by_kind(&down, kind_of), counts(1, 0, 2));
        assert_eq!(down_by_kind(&[], kind_of), ClassCounts::default());
    }
}
