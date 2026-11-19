//! The limit of a result wait, parsed once before any read.
//!
//! Python's rule (`clients/python/horsies/core/utils/wait_timeout.py:33-56`): a wait takes
//! no limit, or a limit from 0 to `MAX_TASK_TIMEOUT_MS` (2^32 - 1 ms, about
//! 49.7 days); a larger limit is refused as `INVALID_INPUT` before any read.
//! The bound also keeps `Instant + limit` far from overflow, which panics.

use std::time::Duration;

use tokio::time::Instant;

/// The longest limit a result wait accepts, in milliseconds: 2^32 - 1, the
/// bound of a task timeout (Python `MAX_TASK_TIMEOUT_MS`,
/// `clients/python/horsies/core/models/tasks.py:548`).
pub const MAX_WAIT_LIMIT_MS: u64 = 4_294_967_295;

/// How long a result wait may last.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitLimit {
    /// Wait until the task ends.
    NoLimit,
    /// Wait at most this long. A zero limit reads once and does not wait.
    Within(BoundedWait),
}

/// A wait of 0 to [`MAX_WAIT_LIMIT_MS`] milliseconds. Only
/// [`WaitLimit::parse`] builds one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundedWait(Duration);

impl BoundedWait {
    /// The length of the wait.
    pub const fn duration(self) -> Duration {
        self.0
    }
}

/// A limit above [`MAX_WAIT_LIMIT_MS`]. The message holds no input value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "timeout is above the limit of {} ms (about 49.7 days)",
    MAX_WAIT_LIMIT_MS
)]
pub struct WaitLimitRefused;

impl WaitLimit {
    /// Parse the caller's limit. `None` waits until the task ends.
    pub fn parse(timeout: Option<Duration>) -> Result<Self, WaitLimitRefused> {
        match timeout {
            None => Ok(Self::NoLimit),
            Some(limit) if limit > Duration::from_millis(MAX_WAIT_LIMIT_MS) => {
                Err(WaitLimitRefused)
            }
            Some(limit) => Ok(Self::Within(BoundedWait(limit))),
        }
    }

    /// The instant at which a wait that started at `start` ends, or `None`
    /// for a wait with no limit.
    pub fn deadline_from(self, start: Instant) -> Option<Instant> {
        match self {
            Self::NoLimit => None,
            Self::Within(bounded) => Some(start + bounded.duration()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_limit_waits_until_the_task_ends() {
        assert_eq!(WaitLimit::parse(None), Ok(WaitLimit::NoLimit));
        assert_eq!(WaitLimit::NoLimit.deadline_from(Instant::now()), None);
    }

    #[test]
    fn zero_and_the_bound_are_accepted() {
        for limit in [
            Duration::ZERO,
            Duration::from_millis(1_500),
            Duration::from_millis(MAX_WAIT_LIMIT_MS),
        ] {
            assert_eq!(
                WaitLimit::parse(Some(limit)),
                Ok(WaitLimit::Within(BoundedWait(limit))),
            );
        }
    }

    /// Python `test_wait_timeout.py::TestWaitTimeoutSeconds::test_refused`
    /// (`limit+1`); `Duration::MAX` is the value whose deadline panicked.
    #[test]
    fn a_limit_above_the_bound_is_refused() {
        for limit in [
            Duration::from_millis(MAX_WAIT_LIMIT_MS) + Duration::from_nanos(1),
            Duration::from_millis(MAX_WAIT_LIMIT_MS + 1),
            Duration::MAX,
        ] {
            assert_eq!(WaitLimit::parse(Some(limit)), Err(WaitLimitRefused));
        }
    }

    #[test]
    fn the_refusal_names_the_bound() {
        assert_eq!(
            WaitLimitRefused.to_string(),
            "timeout is above the limit of 4294967295 ms (about 49.7 days)",
        );
    }

    #[test]
    fn the_deadline_of_the_largest_limit_does_not_overflow() {
        let start = Instant::now();
        let limit = WaitLimit::parse(Some(Duration::from_millis(MAX_WAIT_LIMIT_MS)))
            .expect("the bound is accepted");

        assert_eq!(
            limit.deadline_from(start),
            Some(start + Duration::from_millis(MAX_WAIT_LIMIT_MS)),
        );
    }
}
