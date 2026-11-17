//! The times of the reports a shared async child or an isolated child sends
//! about the stop of a run. The child sends by them and the parent waits by
//! them (Python `core/worker/loop_watchdog.py`, `core/worker/task_loop.py`).

use std::time::Duration;

/// A loop that has not turned, or a runtime thread held in one poll, for
/// this long is blocked: the child reports it, and the parent kills the
/// child (Python `LOOP_BLOCK_LIMIT_MS`).
pub const LOOP_BLOCK_LIMIT: Duration = Duration::from_millis(5_000);

/// After `second_cancel` or `leftover_cancel`, the wait before the stuck
/// report: two more cancel rounds 0.1 s apart and one interval (Python
/// `STUCK_REPORT_DELAY_S`, `EXTRA_CANCEL_ROUNDS`, `CANCEL_ROUND_INTERVAL_S`).
pub const STUCK_REPORT_DELAY: Duration = Duration::from_millis(300);

/// How long an isolated child waits for the work a body left before it
/// cancels it (Python `ISOLATED_LEFTOVER_GRACE_S`).
pub const ISOLATED_LEFTOVER_GRACE: Duration = Duration::from_secs(1);
