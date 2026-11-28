//! The limit of a result wait, checked once before any read.
//!
//! Python's rule (`clients/python/horsies/core/utils/wait_timeout.py:33-56`): a wait takes
//! no limit, or a limit from 0 to `MAX_TASK_TIMEOUT_MS` (2^32 - 1 ms, about
//! 49.7 days); a larger limit is refused as `INVALID_INPUT` before any read.
//! The bound also keeps `Instant + limit` far from overflow, which panics.
//!
//! The caller builds a [`WaitLimit`]. The Rust client checks a `Within`
//! limit with `BoundedWait::try_from` before any statement. A core read
//! takes the limit as `Option<Duration>` from every client and checks it
//! again with `WaitLimit::parse`.

use std::time::Duration;

use tokio::time::Instant;

/// The longest limit a result wait accepts, in milliseconds: 2^32 - 1, the
/// bound of a task timeout (Python `MAX_TASK_TIMEOUT_MS`,
/// `clients/python/horsies/core/models/tasks.py:548`).
pub const MAX_WAIT_LIMIT_MS: u64 = 4_294_967_295;

/// How long a result wait may last. A read refuses a `Within` limit above
/// [`MAX_WAIT_LIMIT_MS`] before any statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitLimit {
    /// Wait until the task ends.
    NoLimit,
    /// Wait at most this long. A zero limit reads once and does not wait.
    Within(Duration),
}

/// A wait of 0 to [`MAX_WAIT_LIMIT_MS`] milliseconds. Only
/// `BoundedWait::try_from` builds one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundedWait(Duration);

impl BoundedWait {
    /// The length of the wait.
    pub const fn duration(self) -> Duration {
        self.0
    }
}

impl TryFrom<Duration> for BoundedWait {
    type Error = WaitLimitRefused;

    /// `limit` when it is at most [`MAX_WAIT_LIMIT_MS`]. Pure.
    ///
    /// Errors: [`WaitLimitRefused`] for a longer limit.
    fn try_from(limit: Duration) -> Result<Self, Self::Error> {
        match limit {
            above if above > Duration::from_millis(MAX_WAIT_LIMIT_MS) => Err(WaitLimitRefused),
            within => Ok(Self(within)),
        }
    }
}

/// A limit above [`MAX_WAIT_LIMIT_MS`]. The message holds no input value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the wait limit is above {} ms (about 49.7 days)", MAX_WAIT_LIMIT_MS)]
pub struct WaitLimitRefused;

/// A wait limit that passed the bound check: the form a core wait runs
/// with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckedWaitLimit {
    /// Wait until the task ends.
    NoLimit,
    /// Wait at most this long.
    Within(BoundedWait),
}

impl CheckedWaitLimit {
    /// The instant at which a wait that started at `start` ends, or `None`
    /// for a wait with no limit. The bound keeps the sum far from overflow.
    pub(crate) fn deadline_from(self, start: Instant) -> Option<Instant> {
        match self {
            Self::NoLimit => None,
            Self::Within(bounded) => Some(start + bounded.duration()),
        }
    }
}

impl WaitLimit {
    /// Parse the limit of a core read, which takes it as `Option<Duration>`
    /// (`None`: no limit). Pure.
    ///
    /// Errors: [`WaitLimitRefused`] for a limit above [`MAX_WAIT_LIMIT_MS`].
    pub(crate) fn parse(timeout: Option<Duration>) -> Result<CheckedWaitLimit, WaitLimitRefused> {
        match timeout {
            None => Ok(CheckedWaitLimit::NoLimit),
            Some(limit) => BoundedWait::try_from(limit).map(CheckedWaitLimit::Within),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The limits just above the bound. `Duration::MAX` is the value whose
    /// deadline panicked.
    fn limits_above_the_bound() -> [Duration; 3] {
        [
            Duration::from_millis(MAX_WAIT_LIMIT_MS) + Duration::from_nanos(1),
            Duration::from_millis(MAX_WAIT_LIMIT_MS + 1),
            Duration::MAX,
        ]
    }

    #[test]
    fn zero_and_the_bound_are_bounded_waits() {
        for limit in [
            Duration::ZERO,
            Duration::from_millis(1_500),
            Duration::from_millis(MAX_WAIT_LIMIT_MS),
        ] {
            assert_eq!(
                BoundedWait::try_from(limit).map(BoundedWait::duration),
                Ok(limit)
            );
        }
    }

    /// Python `test_wait_timeout.py::TestWaitTimeoutSeconds::test_refused`
    /// (`limit+1`).
    #[test]
    fn a_limit_above_the_bound_is_not_a_bounded_wait() {
        for limit in limits_above_the_bound() {
            assert_eq!(
                BoundedWait::try_from(limit),
                Err(WaitLimitRefused),
                "{limit:?}"
            );
        }
    }

    #[test]
    fn the_refusal_names_the_bound() {
        assert_eq!(
            WaitLimitRefused.to_string(),
            "the wait limit is above 4294967295 ms (about 49.7 days)",
        );
    }

    #[test]
    fn a_core_read_with_no_limit_has_no_deadline() {
        assert_eq!(WaitLimit::parse(None), Ok(CheckedWaitLimit::NoLimit));
        assert_eq!(
            CheckedWaitLimit::NoLimit.deadline_from(Instant::now()),
            None
        );
    }

    #[test]
    fn a_core_read_refuses_a_limit_above_the_bound() {
        for limit in limits_above_the_bound() {
            assert_eq!(
                WaitLimit::parse(Some(limit)),
                Err(WaitLimitRefused),
                "{limit:?}"
            );
        }
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
