//! The default sizes of the connection pools of a worker and of the default
//! executor of a shared async child (Python `core/models/child_sizing.py`).
//!
//! An isolated child runs one task at a time, so its pool has a fixed
//! default size. A shared child derives a size from its seats when its
//! profile sets `PoolSizeFromSeats` or `ExecutorThreadsFromSeats`, the
//! defaults. The worker process derives the size of its own pool from the
//! seats of all its children when the profile sets
//! `ParentPool { max_size: PoolSizeFromSeats }`, the default:
//!
//! ```text
//! size = min(cap, max(floor, ceil(seats / seats_per_unit)))
//! ```
//!
//! Each derived size has its own [`SeatRule`]. The measurements behind each
//! constant are in the Python module. Pure.

use std::num::NonZeroU32;

use super::worker_profile::{
    ExecutorThreadCount, PoolConnectionCount, MAX_CHILD_POOL_CONNECTIONS, MAX_EXECUTOR_THREADS,
};

/// The most connections in the pool of an isolated child when its profile
/// does not say. Its two writes run one after the other, so it never uses a
/// second connection.
pub const DEFAULT_ISOLATED_POOL_CONNECTIONS: u32 = 1;

/// The fewest connections of a pool size derived from the seats.
pub const POOL_CONNECTIONS_FLOOR: u32 = 2;

/// Seats of a shared async child per connection of a derived pool size.
pub const SHARED_ASYNC_SEATS_PER_POOL_CONNECTION: u32 = 10;

/// The most connections of a pool size derived from the seats of a shared
/// async child.
pub const SHARED_ASYNC_POOL_CONNECTIONS_CAP: u32 = 40;

/// Seats of a shared thread child per connection of a derived pool size.
pub const SHARED_THREAD_SEATS_PER_POOL_CONNECTION: u32 = 10;

/// The most connections of a pool size derived from the seats of a shared
/// thread child: the size that the most seats (256) derive.
pub const SHARED_THREAD_POOL_CONNECTIONS_CAP: u32 = 26;

/// The steady flushers of the batched success write.
pub const FINALIZE_BATCH_FLUSHERS: u32 = 2;

/// The connection of the parent pool that the claim loop uses while it
/// claims.
pub const PARENT_POOL_CLAIM_CONNECTIONS: u32 = 1;

/// Connections of the parent pool that the claim loop and the flushers of
/// the batched success write leave free for the other users of the pool:
/// the reaper, the claimer heartbeat, the worker-state snapshot, and the
/// writes of failures, retries and workflow nodes.
pub const PARENT_POOL_RESERVE: u32 = 2;

/// The fewest connections of the parent pool: the claim loop, the steady
/// flushers of the batched success write and [`PARENT_POOL_RESERVE`].
/// AppConfig validation refuses an explicit `ParentPool` size below it.
pub const PARENT_POOL_MIN_CONNECTIONS: u32 =
    PARENT_POOL_CLAIM_CONNECTIONS + FINALIZE_BATCH_FLUSHERS + PARENT_POOL_RESERVE;

/// The fewest connections of a parent pool size derived from the seats of
/// the worker: the minimum of an explicit size.
pub const PARENT_POOL_CONNECTIONS_FLOOR: u32 = PARENT_POOL_MIN_CONNECTIONS;

/// Seats of the worker per connection of a derived parent pool size.
pub const PARENT_SEATS_PER_POOL_CONNECTION: u32 = 10;

/// The most connections of a parent pool size derived from the seats of the
/// worker.
pub const PARENT_POOL_CONNECTIONS_CAP: u32 = 20;

/// The connections of the pool that task code opens in a child when it
/// sends, for each event loop that it sends from, when the profile does not
/// say.
pub const DEFAULT_TASK_SEND_POOL_CONNECTIONS: u32 = 2;

/// The fewest threads of a default executor size derived from the seats.
pub const EXECUTOR_THREADS_FLOOR: u32 = 2;

/// Seats of a shared async child per thread of a derived default executor
/// size.
pub const SHARED_ASYNC_SEATS_PER_EXECUTOR_THREAD: u32 = 1;

/// The most threads of a default executor size derived from the seats of a
/// shared async child: the cap of the asyncio default executor.
pub const SHARED_ASYNC_EXECUTOR_THREADS_CAP: u32 = 32;

// Every derived size fits the bound of the value it becomes.
const _: () = assert!(SHARED_ASYNC_POOL_CONNECTIONS_CAP <= MAX_CHILD_POOL_CONNECTIONS);
const _: () = assert!(SHARED_THREAD_POOL_CONNECTIONS_CAP <= MAX_CHILD_POOL_CONNECTIONS);
const _: () = assert!(PARENT_POOL_CONNECTIONS_CAP <= MAX_CHILD_POOL_CONNECTIONS);
const _: () = assert!(SHARED_ASYNC_EXECUTOR_THREADS_CAP <= MAX_EXECUTOR_THREADS);

/// A rule that derives a size from seats:
/// `min(cap, max(floor, ceil(seats / seats_per_unit)))`, with
/// `1 <= floor <= cap`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeatRule {
    seats_per_unit: NonZeroU32,
    floor: u32,
    cap: u32,
}

impl SeatRule {
    /// A rule of constants. Use it only in a `const` item: a rule with
    /// `seats_per_unit = 0` or without `1 <= floor <= cap` then fails the
    /// build.
    const fn from_constants(seats_per_unit: u32, floor: u32, cap: u32) -> Self {
        assert!(
            1 <= floor && floor <= cap,
            "a seat rule needs 1 <= floor <= cap"
        );
        let seats_per_unit = match NonZeroU32::new(seats_per_unit) {
            Some(seats_per_unit) => seats_per_unit,
            None => panic!("a seat rule needs seats_per_unit >= 1"),
        };
        Self {
            seats_per_unit,
            floor,
            cap,
        }
    }

    /// The size that `seats` seats derive.
    pub fn size(self, seats: NonZeroU32) -> u32 {
        seats
            .get()
            .div_ceil(self.seats_per_unit.get())
            .max(self.floor)
            .min(self.cap)
    }
}

/// The pool size rule of a shared async child.
pub const SHARED_ASYNC_POOL_RULE: SeatRule = SeatRule::from_constants(
    SHARED_ASYNC_SEATS_PER_POOL_CONNECTION,
    POOL_CONNECTIONS_FLOOR,
    SHARED_ASYNC_POOL_CONNECTIONS_CAP,
);

/// The pool size rule of a shared thread child.
pub const SHARED_THREAD_POOL_RULE: SeatRule = SeatRule::from_constants(
    SHARED_THREAD_SEATS_PER_POOL_CONNECTION,
    POOL_CONNECTIONS_FLOOR,
    SHARED_THREAD_POOL_CONNECTIONS_CAP,
);

/// The default executor size rule of a shared async child.
pub const SHARED_ASYNC_EXECUTOR_RULE: SeatRule = SeatRule::from_constants(
    SHARED_ASYNC_SEATS_PER_EXECUTOR_THREAD,
    EXECUTOR_THREADS_FLOOR,
    SHARED_ASYNC_EXECUTOR_THREADS_CAP,
);

/// The pool size rule of the worker process.
pub const PARENT_POOL_RULE: SeatRule = SeatRule::from_constants(
    PARENT_SEATS_PER_POOL_CONNECTION,
    PARENT_POOL_CONNECTIONS_FLOOR,
    PARENT_POOL_CONNECTIONS_CAP,
);

/// A derived pool size as the bounded type of a pool size. Every cap is at
/// most [`MAX_CHILD_POOL_CONNECTIONS`] (checked when the program compiles),
/// so a refusal here is a defect.
fn derived_connections(size: u32) -> PoolConnectionCount {
    PoolConnectionCount::try_from(size)
        .expect("a derived pool size is at most its cap, which is inside the pool bound")
}

/// The pool size that a shared async child with `seats` seats derives.
pub fn shared_async_pool_connections(seats: NonZeroU32) -> PoolConnectionCount {
    derived_connections(SHARED_ASYNC_POOL_RULE.size(seats))
}

/// The pool size that a shared thread child with `seats` seats derives.
pub fn shared_thread_pool_connections(seats: NonZeroU32) -> PoolConnectionCount {
    derived_connections(SHARED_THREAD_POOL_RULE.size(seats))
}

/// The default executor size that a shared async child with `seats` seats
/// derives. The cap is at most [`MAX_EXECUTOR_THREADS`] (checked when the
/// program compiles), so a refusal here is a defect.
pub fn shared_async_executor_threads(seats: NonZeroU32) -> ExecutorThreadCount {
    ExecutorThreadCount::try_from(SHARED_ASYNC_EXECUTOR_RULE.size(seats))
        .expect("a derived executor size is at most its cap, which is inside the thread bound")
}

/// The parent pool size that a worker with `seats` seats derives. A profile
/// with no children (0 seats), which AppConfig validation refuses, gets
/// [`PARENT_POOL_CONNECTIONS_FLOOR`].
pub fn parent_pool_connections(seats: u32) -> PoolConnectionCount {
    match NonZeroU32::new(seats) {
        Some(seats) => derived_connections(PARENT_POOL_RULE.size(seats)),
        None => derived_connections(PARENT_POOL_CONNECTIONS_FLOOR),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seats(count: u32) -> NonZeroU32 {
        NonZeroU32::new(count).expect("a test seat count is at least 1")
    }

    /// Python `test_worker_profile.py::TestChildPoolResolution::test_a_shared_async_pool_from_seats_is_derived`
    /// and `::test_a_shared_thread_pool_from_seats_is_derived`.
    #[test]
    fn a_shared_pool_size_follows_the_seats_between_floor_and_cap() {
        for (count, connections) in [(1, 2), (20, 2), (21, 3), (100, 10), (400, 40), (1000, 40)] {
            assert_eq!(
                shared_async_pool_connections(seats(count)).get(),
                connections,
                "{count} seats",
            );
        }
        for (count, connections) in [(1, 2), (24, 3), (64, 7), (256, 26)] {
            assert_eq!(
                shared_thread_pool_connections(seats(count)).get(),
                connections,
                "{count} seats",
            );
        }
    }

    /// Python `test_worker_profile.py::TestChildPoolResolution::test_the_executor_from_seats_is_derived`.
    #[test]
    fn the_executor_size_follows_the_seats_between_floor_and_cap() {
        for (count, threads) in [(1, 2), (2, 2), (3, 3), (32, 32), (1000, 32)] {
            assert_eq!(
                shared_async_executor_threads(seats(count)).get(),
                threads,
                "{count} seats",
            );
        }
    }

    /// Python `test_worker_profile.py::TestParentPool::test_the_examples_of_the_rule`
    /// and `::test_a_profile_with_no_children_resolves_to_the_floor`.
    #[test]
    fn the_parent_pool_follows_every_seat_of_the_worker() {
        for (count, connections) in [(0, 5), (1, 5), (50, 5), (51, 6), (200, 20), (1000, 20)] {
            assert_eq!(
                parent_pool_connections(count).get(),
                connections,
                "{count} seats",
            );
        }
        assert_eq!(PARENT_POOL_MIN_CONNECTIONS, 5);
        assert_eq!(PARENT_POOL_CONNECTIONS_FLOOR, PARENT_POOL_MIN_CONNECTIONS);
    }
}
