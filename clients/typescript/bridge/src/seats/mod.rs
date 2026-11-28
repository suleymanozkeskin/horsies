//! The thread seats of a `SHARED_THREAD` child: the bounds that the seats
//! take from the core.
//!
//! A seat run is a run of the core's seat thread with the blocking body
//! form of `child::body` (`JsBlockingBody`): it posts the run to the
//! JavaScript main thread and waits for its settle call, and it posts the
//! run's cancel. The main thread gives the run to a free seat isolate, and a
//! seat isolate never loads this addon. The start facts give the seat count
//! and the task-send pool of the child: the pool bounds the operations of
//! one isolate that wait for I/O of the main thread at once. The core's host
//! loop watch covers the main thread.
//!
//! - `seatBounds()`: pure. The core's constants that bound the seats: the
//!   operations that wait in one isolate (the bound of the work of one run),
//!   and the longest boot of a new isolate (the start-hook bound of a child).
//!   The main thread reads them and gives the first to each isolate in its
//!   seat marker.

use horsies_core::{CHILD_START_HOOK_BOUND, MAX_RUN_WORK};
use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::Envelope;

/// The bounds of the thread seats, from the core's constants.
#[napi(object, object_from_js = false)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeatBounds {
    /// The most operations that wait in one isolate: `MAX_RUN_WORK`.
    pub ops_waiting: u32,
    /// The longest boot of a new isolate, in milliseconds:
    /// `CHILD_START_HOOK_BOUND`.
    pub replacement_boot_bound_ms: u32,
}

/// The bounds of the core's constants. Pure.
fn core_seat_bounds() -> SeatBounds {
    SeatBounds {
        ops_waiting: u32::try_from(MAX_RUN_WORK)
            .unwrap_or_else(|_| panic!("MAX_RUN_WORK {MAX_RUN_WORK} fits a u32")),
        replacement_boot_bound_ms: u32::try_from(CHILD_START_HOOK_BOUND.as_millis())
            .unwrap_or_else(|_| panic!("CHILD_START_HOOK_BOUND fits a u32 of milliseconds")),
    }
}

/// The bounds of the thread seats. Pure: starts nothing.
#[napi(ts_return_type = "NativeEnvelope<SeatBounds>")]
pub fn seat_bounds() -> Envelope<SeatBounds> {
    sync_call("seat_bounds", || Envelope::Value(core_seat_bounds()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_seat_bounds_are_the_core_constants() {
        assert_eq!(
            core_seat_bounds(),
            SeatBounds {
                ops_waiting: 10_000,
                replacement_boot_bound_ms: 10_000,
            }
        );
    }
}
