//! When the reaper's next partition maintenance is due.
//!
//! The coverage pass creates the heartbeat leaves of its own hour and the
//! next `heartbeat_leaf_horizon_hours` hours (2 to 48), and the history
//! leaves of its own day and the next `history_leaf_horizon_days` days. A
//! write whose hour or day has no leaf is refused (SQLSTATE 23514). So a
//! pass must run before the last leaf of the previous pass ends.
//!
//! The interval between passes (`partition_maintenance_interval_seconds`, 60 s to
//! 3600 s) is shorter than the smallest horizon. A deadline on the
//! monotonic clock alone does not hold that bound across a suspend of the
//! host: the monotonic clock of Linux and macOS stops while the host
//! sleeps, so after a sleep longer than the horizon the leaf of the current
//! hour is missing, and every heartbeat, task start and lease renewal of
//! the worker fails until the monotonic deadline passes. The wall clock
//! counts the sleep. The maintenance is due when either clock reaches its
//! deadline.

use std::time::{Duration, SystemTime};

use tokio::time::Instant;

/// The deadline of the next partition maintenance on both clocks. Only
/// [`MaintenanceDue::now`] and [`MaintenanceDue::after`] construct it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MaintenanceDue {
    monotonic: Instant,
    wall: SystemTime,
}

impl MaintenanceDue {
    /// Due at once: the first pass of a reaper.
    pub(crate) fn now() -> Self {
        Self {
            monotonic: Instant::now(),
            wall: SystemTime::now(),
        }
    }

    /// Due one `interval` after now, on both clocks.
    pub(crate) fn after(interval: Duration) -> Self {
        Self::after_from(Instant::now(), SystemTime::now(), interval)
    }

    /// Due one `interval` after the instants `monotonic` and `wall`. A wall
    /// deadline past the range of `SystemTime` leaves the monotonic
    /// deadline alone.
    pub(super) fn after_from(monotonic: Instant, wall: SystemTime, interval: Duration) -> Self {
        Self {
            monotonic: monotonic + interval,
            wall: wall.checked_add(interval).unwrap_or(wall),
        }
    }

    /// Whether the maintenance is due now.
    pub(crate) fn is_due(&self) -> bool {
        self.is_due_at(Instant::now(), SystemTime::now())
    }

    /// Whether the maintenance is due at `monotonic` and `wall`: either
    /// clock reached its deadline. A wall clock set back keeps the
    /// monotonic deadline; a wall clock set forward makes a pass early,
    /// which only repeats a coverage pass.
    fn is_due_at(&self, monotonic: Instant, wall: SystemTime) -> bool {
        monotonic >= self.monotonic || wall >= self.wall
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INTERVAL: Duration = Duration::from_secs(900);

    /// Inside the interval on both clocks: not due.
    #[test]
    fn inside_the_interval_on_both_clocks_it_is_not_due() {
        let (monotonic, wall) = (Instant::now(), SystemTime::now());
        let due = MaintenanceDue::after_from(monotonic, wall, INTERVAL);

        let seconds_later = Duration::from_secs(1);
        assert!(!due.is_due_at(monotonic + seconds_later, wall + seconds_later));
    }

    /// The interval passed on the monotonic clock: due.
    #[test]
    fn the_interval_on_the_monotonic_clock_makes_it_due() {
        let (monotonic, wall) = (Instant::now(), SystemTime::now());
        let due = MaintenanceDue::after_from(monotonic, wall, INTERVAL);

        assert!(due.is_due_at(monotonic + INTERVAL, wall + INTERVAL));
    }

    /// A host that slept seven hours: the monotonic clock moved one second,
    /// the wall clock seven hours, past the default heartbeat horizon of
    /// six hours. Due, so the pass creates the leaf of the current hour.
    #[test]
    fn a_host_sleep_that_the_monotonic_clock_did_not_count_makes_it_due() {
        let (monotonic, wall) = (Instant::now(), SystemTime::now());
        let due = MaintenanceDue::after_from(monotonic, wall, INTERVAL);

        let slept = Duration::from_secs(7 * 3600);
        assert!(due.is_due_at(monotonic + Duration::from_secs(1), wall + slept));
    }

    /// A wall clock set back keeps the monotonic deadline.
    #[test]
    fn a_wall_clock_set_back_keeps_the_monotonic_deadline() {
        let (monotonic, wall) = (Instant::now(), SystemTime::now());
        let due = MaintenanceDue::after_from(monotonic, wall, INTERVAL);

        let set_back = wall - Duration::from_secs(3600);
        assert!(!due.is_due_at(monotonic + Duration::from_secs(1), set_back));
        assert!(due.is_due_at(monotonic + INTERVAL, set_back));
    }
}
