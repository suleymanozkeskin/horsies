//! The fencing token of a run: what task code writes next to its writes to
//! another system, so that the system can refuse the writes of an earlier
//! attempt of the same task (Python: `TaskRuntime.fencing_token`,
//! `clients/python/horsies/core/task_runtime.py`).
//!
//! The value is the attempt number of the claim that runs the body. Each
//! retry of a RUNNING row adds 1 to its retry count before the row can be
//! claimed again, and a claim that never started runs no task code. So for
//! one task id, an attempt whose body runs after another attempt has a
//! greater value. The value depends on no clock.

use uuid::Uuid;

use crate::events::AttemptNumber;

/// The fencing token of one run: the task id and a value.
///
/// - Two reads in one attempt give the same token.
/// - An attempt that runs after another attempt of the same task gives a
///   greater value.
/// - The value depends on no clock.
/// - Today the value is the attempt number, from 1. Compare the values of
///   one task id only; do not depend on the number itself.
///
/// The token has no order: the tokens of two task ids are not comparable.
/// A rerun, and a workflow node that a resume starts again, get a new task
/// id, and their value starts again at 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FencingToken {
    task_id: Uuid,
    value: AttemptNumber,
}

impl FencingToken {
    /// The token of the run of attempt `value` of task `task_id`.
    pub(crate) fn new(task_id: Uuid, value: AttemptNumber) -> Self {
        Self { task_id, value }
    }

    /// The id of the task. The token orders the attempts of this task only.
    pub fn task_id(&self) -> Uuid {
        self.task_id
    }

    /// The value. `value().get()` gives the number as a `u32`, which fits a
    /// PostgreSQL `bigint` and a JSON number.
    pub fn value(&self) -> AttemptNumber {
        self.value
    }
}

/// Why no fencing token was read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FencingTokenRefused {
    /// The caller is not the body of a run in a worker child, nor work that
    /// an async body started with `spawn_in_run`, nor the thread of a
    /// blocking body.
    #[error(
        "the fencing token was read outside a task run: only the body of a run, work that an \
         async body started with spawn_in_run, and the thread of a blocking body have one"
    )]
    NotInARun,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attempt(value: u32) -> AttemptNumber {
        AttemptNumber::try_from(value).expect("an attempt number")
    }

    #[test]
    fn a_token_gives_its_task_id_and_value() {
        let task_id = Uuid::new_v4();
        let token = FencingToken::new(task_id, attempt(2));
        assert_eq!(token.task_id(), task_id);
        assert_eq!(token.value().get(), 2);
    }

    #[test]
    fn a_later_attempt_of_one_task_has_a_greater_value() {
        let task_id = Uuid::new_v4();
        let first = FencingToken::new(task_id, attempt(1));
        let second = FencingToken::new(task_id, attempt(2));
        assert_ne!(first, second);
        assert!(second.value() > first.value());
    }

    #[test]
    fn the_refusal_names_the_places_that_have_a_token() {
        let text = FencingTokenRefused::NotInARun.to_string();
        assert!(text.contains("outside a task run"));
        assert!(text.contains("spawn_in_run"));
    }
}
