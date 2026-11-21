//! The stop of the outcome writes at the worker stop.
//!
//! Order:
//! 1. Wait for the outcome writes in flight (one task per attempt end), up
//!    to the drain bound; cancel the ones left.
//! 2. Then close the batcher ([`FINALIZE_BATCH_CLOSE_TIMEOUT`]). A write
//!    that the drain cancelled leaves its row in the batcher, and the
//!    flushers still write that row before the close returns.
//! 3. Log the rows that the close left: rows in a cancelled statement
//!    (commit unknown) and rows not sent (left RUNNING for stale recovery).

use std::time::Duration;

use tokio::task::{JoinError, JoinSet};

use super::{CloseReport, FinalizeBatcher, SuccessBatchWrite, FINALIZE_BATCH_CLOSE_TIMEOUT};

/// What the stop of the outcome writes did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutcomeWritesStopped {
    /// Outcome writes that the drain bound cancelled.
    pub writes_cancelled: usize,
    /// What the close of the batcher saw.
    pub close: CloseReport,
}

/// Drain `writes` for at most `drain`, cancel the rest, then close
/// `batcher` and log the rows its close left.
///
/// Raises the panic of a write task that panicked (a defect). Writes
/// nothing itself.
pub async fn drain_then_close<T, W>(
    writes: &mut JoinSet<T>,
    drain: Duration,
    batcher: &FinalizeBatcher<W>,
) -> OutcomeWritesStopped
where
    T: Send + 'static,
    W: SuccessBatchWrite,
{
    let deadline = tokio::time::Instant::now() + drain;
    loop {
        match tokio::time::timeout_at(deadline, writes.join_next()).await {
            Ok(None) | Err(_) => break,
            Ok(Some(joined)) => raise_panic_of(joined),
        }
    }
    let writes_cancelled = writes.len();
    if writes_cancelled > 0 {
        tracing::warn!(
            writes = writes_cancelled,
            drain_s = drain.as_secs_f64(),
            "the worker stop cancels the outcome writes still running after the drain",
        );
    }
    writes.abort_all();
    while let Some(joined) = writes.join_next().await {
        raise_panic_of(joined);
    }
    let close = batcher.close(FINALIZE_BATCH_CLOSE_TIMEOUT).await;
    if let Some(line) = rows_left_by_close(close) {
        tracing::warn!("{line}");
    }
    OutcomeWritesStopped {
        writes_cancelled,
        close,
    }
}

/// The log line of the rows that the close left, when it left any. Pure.
pub fn rows_left_by_close(report: CloseReport) -> Option<String> {
    match report {
        CloseReport::FlushersDrained => None,
        CloseReport::FlushersCancelled {
            rows_in_flight,
            rows_unsent,
        } => Some(format!(
            "the worker stop cancelled the finalize batch writes after {:.0}s: {rows_in_flight} \
             rows in a cancelled statement (commit unknown), {rows_unsent} rows not sent. Stale \
             RUNNING recovery fails each row that did not commit.",
            FINALIZE_BATCH_CLOSE_TIMEOUT.as_secs_f64(),
        )),
    }
}

/// Raise the panic of a joined task; a cancel (the drain bound) is expected.
fn raise_panic_of<T>(joined: Result<T, JoinError>) {
    if let Err(error) = joined {
        if let Ok(payload) = error.try_into_panic() {
            std::panic::resume_unwind(payload);
        }
    }
}
