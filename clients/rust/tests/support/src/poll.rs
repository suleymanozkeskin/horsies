//! Bounded waits on a condition (Python `clients/python/tests/e2e/helpers/db.py`).

use std::time::{Duration, Instant};

/// The interval between two probes of a wait.
pub const POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Probe until `probe` returns a value, every [`POLL_INTERVAL`]. Panics
/// with `subject` when `limit` passes first.
pub async fn poll_until<T>(
    limit: Duration,
    subject: &str,
    mut probe: impl AsyncFnMut() -> Option<T>,
) -> T {
    let deadline = Instant::now() + limit;
    loop {
        if let Some(found) = probe().await {
            return found;
        }
        if Instant::now() >= deadline {
            panic!("{subject}: not reached within {limit:?}");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Probe for `duration`, every `interval`, and return the largest value
/// that `probe` answered (Python `poll_max_during`).
pub async fn max_during(
    duration: Duration,
    interval: Duration,
    mut probe: impl AsyncFnMut() -> i64,
) -> i64 {
    let deadline = Instant::now() + duration;
    let mut largest = i64::MIN;
    while Instant::now() < deadline {
        largest = largest.max(probe().await);
        tokio::time::sleep(interval).await;
    }
    largest
}

/// Probe for `duration`, every [`POLL_INTERVAL`]; panic with `subject` when
/// `probe` answers true at any probe. A check that a thing does not happen
/// within a window.
pub async fn never_during(duration: Duration, subject: &str, mut probe: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        if probe().await {
            panic!("{subject}: happened within {duration:?}");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}
