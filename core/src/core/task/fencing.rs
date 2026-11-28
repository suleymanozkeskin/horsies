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

use crate::events::{AttemptNumber, MaxRetries, RetriesLeft};

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

/// The facts of one attempt of a task that its body reads: the task id, the
/// attempt number, and the retry budget of the task row.
///
/// - `task_id` is the same on every attempt of the task.
/// - `attempt` is 1 on the first run and rises by 1 on each retry.
/// - `max_retries` is the retry budget that the task row holds.
/// - `retries_left` is `max_retries - (attempt - 1)`, and 0 when the attempt
///   is past the budget. A non-retryable error code or `good_until` can end
///   the retries before it reaches 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskAttempt {
    task_id: Uuid,
    attempt: AttemptNumber,
    max_retries: MaxRetries,
}

impl TaskAttempt {
    /// The facts of attempt `attempt` of task `task_id`, whose row holds the
    /// retry budget `max_retries`.
    pub(crate) fn new(task_id: Uuid, attempt: AttemptNumber, max_retries: MaxRetries) -> Self {
        Self {
            task_id,
            attempt,
            max_retries,
        }
    }

    /// The id of the task. It is the same on every attempt.
    pub fn task_id(&self) -> Uuid {
        self.task_id
    }

    /// The attempt number: 1 is the first run.
    pub fn attempt(&self) -> AttemptNumber {
        self.attempt
    }

    /// The retry budget of the task.
    pub fn max_retries(&self) -> MaxRetries {
        self.max_retries
    }

    /// The retries left after this attempt: `max_retries - (attempt - 1)`,
    /// and 0 when the attempt is past the budget.
    pub fn retries_left(&self) -> RetriesLeft {
        let used = self.attempt.get() - 1;
        let left = self.max_retries.get().saturating_sub(used);
        match RetriesLeft::try_from(left) {
            Ok(left) => left,
            Err(outside) => unreachable!(
                "a budget of 0 to MAX_TASK_RETRIES less the retries used stays in that range: {outside}"
            ),
        }
    }

    /// The fencing token of this attempt.
    pub fn fencing_token(&self) -> FencingToken {
        FencingToken::new(self.task_id, self.attempt)
    }
}

/// Why no attempt facts were read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TaskAttemptRefused {
    /// The caller is not the body of a run in a worker child, nor work that
    /// an async body started with `spawn_in_run`, nor the thread of a
    /// blocking body.
    #[error(
        "the attempt facts were read outside a task run: only the body of a run, work that an \
         async body started with spawn_in_run, and the thread of a blocking body have them"
    )]
    NotInARun,
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

    fn facts(attempt_number: u32, max_retries: u32) -> TaskAttempt {
        TaskAttempt::new(
            Uuid::new_v4(),
            attempt(attempt_number),
            MaxRetries::try_from(max_retries).expect("a retry budget"),
        )
    }

    #[test]
    fn a_first_attempt_has_all_its_retries_left() {
        let first = facts(1, 3);
        assert_eq!(first.attempt().get(), 1);
        assert_eq!(first.max_retries().get(), 3);
        assert_eq!(first.retries_left().get(), 3);
    }

    #[test]
    fn a_retry_is_the_next_attempt_with_one_retry_less() {
        let retry = facts(2, 3);
        assert_eq!(retry.attempt().get(), 2);
        assert_eq!(retry.retries_left().get(), 2);
    }

    #[test]
    fn the_last_attempt_has_no_retries_left() {
        assert_eq!(facts(4, 3).retries_left().get(), 0);
    }

    #[test]
    fn an_attempt_past_the_budget_has_no_retries_left() {
        assert_eq!(facts(9, 3).retries_left().get(), 0);
    }

    #[test]
    fn the_facts_give_the_fencing_token_of_their_attempt() {
        let retry = facts(2, 3);
        assert_eq!(
            retry.fencing_token(),
            FencingToken::new(retry.task_id(), retry.attempt())
        );
    }

    #[test]
    fn the_refusal_names_the_places_that_have_the_facts() {
        let text = TaskAttemptRefused::NotInARun.to_string();
        assert!(text.contains("outside a task run"));
        assert!(text.contains("spawn_in_run"));
    }

    #[test]
    fn the_refusal_names_the_places_that_have_a_token() {
        let text = FencingTokenRefused::NotInARun.to_string();
        assert!(text.contains("outside a task run"));
        assert!(text.contains("spawn_in_run"));
    }
}
