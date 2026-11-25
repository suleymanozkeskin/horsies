//! The placement of an attempt on a child of the worker, as the attempt
//! writers record it: `placed_at` of the phase record of an attempt row
//! (migration 1001).
//!
//! The worker measures the slot wait on its monotonic clock, from the read of
//! the claim result to the instant a child took the task. The writer
//! functions add the wait to the claim generation (`claimed_at`, a database
//! time), so `placed_at` is on the database clock and no worker wall clock
//! enters the record.

use std::time::Duration;

use tokio::time::Instant;

/// How long a claimed task waited for a slot: from the instant the worker
/// read the claim result to the instant a child took the task, on the
/// monotonic clock of the worker. The same wait as the `AttemptPlaced` event
/// and the `horsies.task.slot_wait.duration` metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotWait(Duration);

impl SlotWait {
    /// The wait from `read_at`, when the worker read the claim result, to
    /// `placed_at`, when a child took the task. A placement before the read
    /// cannot occur on a monotonic clock; it counts as no wait. Pure.
    pub fn between(read_at: Instant, placed_at: Instant) -> SlotWait {
        SlotWait(placed_at.saturating_duration_since(read_at))
    }

    pub fn duration(self) -> Duration {
        self.0
    }

    /// The wait in whole microseconds, the unit of the `p_slot_wait_us`
    /// argument of the writer functions. 2^63 - 1 microseconds (about
    /// 292,000 years) cannot pass in one worker process, so a longer wait
    /// saturates at `i64::MAX` and never wraps. Pure.
    pub fn as_micros(self) -> i64 {
        i64::try_from(self.0.as_micros()).unwrap_or(i64::MAX)
    }
}

/// Whether the writer of an attempt row observed the placement of the
/// attempt on a child.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptPlacement {
    /// A child took the task after `slot_wait`. The writer stores
    /// `placed_at = claimed_at + slot_wait`.
    Placed { slot_wait: SlotWait },
    /// The writer has no placement: the task reached no child, or the write
    /// comes from a caller that does not follow the placement (the Python
    /// bridge, a settlement with no placement). `placed_at` stays NULL.
    NotObserved,
}

/// The kind argument of [`AttemptPlacement::Placed`].
const PLACED_KIND: &str = "PLACED";
/// The kind argument of [`AttemptPlacement::NotObserved`].
const NOT_OBSERVED_KIND: &str = "NOT_OBSERVED";

impl AttemptPlacement {
    /// The `p_placement_kind` argument of the writer functions: `PLACED` or
    /// `NOT_OBSERVED`. Pure.
    pub fn kind_sql(self) -> &'static str {
        match self {
            Self::Placed { .. } => PLACED_KIND,
            Self::NotObserved => NOT_OBSERVED_KIND,
        }
    }

    /// The `p_slot_wait_us` argument of the writer functions: the wait in
    /// microseconds for `PLACED`, NULL for `NOT_OBSERVED`. Pure.
    pub fn slot_wait_us(self) -> Option<i64> {
        match self {
            Self::Placed { slot_wait } => Some(slot_wait.as_micros()),
            Self::NotObserved => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_slot_wait_is_the_time_from_the_read_to_the_placement() {
        let read_at = Instant::now();
        let placed_at = read_at + Duration::from_micros(1_250);
        let wait = SlotWait::between(read_at, placed_at);
        assert_eq!(wait.duration(), Duration::from_micros(1_250));
        assert_eq!(wait.as_micros(), 1_250);
    }

    #[test]
    fn a_placement_before_the_read_is_no_wait() {
        let placed_at = Instant::now();
        let read_at = placed_at + Duration::from_millis(3);
        assert_eq!(SlotWait::between(read_at, placed_at).as_micros(), 0);
    }

    #[test]
    fn a_wait_beyond_the_microsecond_range_saturates() {
        assert_eq!(SlotWait(Duration::MAX).as_micros(), i64::MAX);
    }

    #[test]
    fn each_placement_gives_its_writer_arguments() {
        let read_at = Instant::now();
        let placed = AttemptPlacement::Placed {
            slot_wait: SlotWait::between(read_at, read_at + Duration::from_micros(40)),
        };
        assert_eq!(placed.kind_sql(), "PLACED");
        assert_eq!(placed.slot_wait_us(), Some(40));
        assert_eq!(AttemptPlacement::NotObserved.kind_sql(), "NOT_OBSERVED");
        assert_eq!(AttemptPlacement::NotObserved.slot_wait_us(), None);
    }
}
