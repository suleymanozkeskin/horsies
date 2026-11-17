//! The call bridge: how a Python call runs a core operation.
//!
//! - A blocking call ([`blocking::call_blocking`]) runs the operation on the
//!   calling thread with the GIL released, in slices, and checks signals
//!   between slices.
//! - An async call ([`completion_queue::call_async`]) spawns the operation on
//!   the core runtime and returns an asyncio future. The outcome goes into
//!   the completion queue of the caller's event loop; one wake descriptor
//!   wakes the loop, which sets every finished future in one callback. No
//!   core thread takes the GIL.
//!
//! Both paths run the operation inside [`guard`]: a panic becomes
//! `PanicException` with the subject and the cause, the core keeps running,
//! and a stop of the core ends the call with `CoreStopped`.
//!
//! A cancel depends on the [`CallKind`]: a read is dropped; a write runs on
//! to its end and the caller sees the outcome as unknown.
//!
//! A service run (a worker, the scheduler loop) lasts until a stop request,
//! so it has its own form with no total bound ([`service_run`]): a blocking
//! form on the main thread that runs the signal handlers between slices,
//! and an async form whose cancel asks the run to stop.
//!
//! The other direction, the core calling Python, goes through the callback
//! thread ([`callback_thread`]): a core task queues the Python call and
//! awaits its outcome, and only that thread attaches.

pub mod blocking;
pub mod callback_thread;
pub mod completion_queue;
pub mod service_run;

use std::future::Future;
use std::panic::AssertUnwindSafe;

use futures_util::FutureExt;
use pyo3::panic::PanicException;
use pyo3::prelude::*;
use tracing::instrument::WithSubscriber;

use crate::errors::CoreStopped;
use crate::logging::RecordLevel;
use crate::process_core::{take_panic_cause, Core, PanicReport};

/// Whether an operation changes database state. It decides what a cancel
/// does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallKind {
    /// Reads and waits. A cancel drops the operation; nothing changed.
    Read,
    /// Writes. A cancel detaches the caller; the operation runs on to its end
    /// and its outcome is unknown to the caller.
    Write,
}

impl CallKind {
    /// What remains true when the caller leaves before the end.
    pub const fn state_after_leave(self) -> &'static str {
        match self {
            Self::Read => "no_change",
            Self::Write => "outcome_unknown",
        }
    }
}

/// How one guarded operation ended.
pub enum Guarded<T> {
    Done(T),
    /// The operation panicked; the core runs on.
    Panicked {
        cause: String,
    },
    /// A core background task panicked while the operation ran.
    CoreStopped(PanicReport),
}

/// Run `op` with the core subscriber as the default dispatcher, catch its
/// panic, and end early when the core stops.
pub async fn guard<T, F>(core: &'static Core, subject: &'static str, op: F) -> Guarded<T>
where
    F: Future<Output = T> + Send,
{
    let operation = AssertUnwindSafe(op)
        .catch_unwind()
        .with_subscriber(core.log().dispatch().clone());
    let mut stop = core.stop_receiver();
    tokio::select! {
        biased;
        outcome = operation => match outcome {
            Ok(value) => Guarded::Done(value),
            Err(payload) => {
                let cause = take_panic_cause(payload.as_ref());
                core.log().push_bridge_record(
                    RecordLevel::Error,
                    format!("panic in core operation {subject}: {cause}"),
                );
                Guarded::Panicked { cause }
            }
        },
        report = stopped(core, &mut stop) => Guarded::CoreStopped(report),
    }
}

/// Wait until the core stops, and return the first panic report.
async fn stopped(core: &Core, stop: &mut tokio::sync::watch::Receiver<bool>) -> PanicReport {
    // The borrow of the watched value ends in this statement: it must not be
    // held across an await, or the future would not be `Send`.
    let signal_seen = stop.wait_for(|stopped| *stopped).await.is_ok();
    if !signal_seen {
        // The sender lives in the core, which lives for the process.
        std::future::pending::<()>().await;
    }
    match core.stopped_report() {
        Some(report) => report.clone(),
        None => unreachable!("the stop signal is sent only after the first report is kept"),
    }
}

/// The Python result of a guarded operation.
pub fn guarded_into_result<T>(subject: &'static str, guarded: Guarded<T>) -> PyResult<T> {
    match guarded {
        Guarded::Done(value) => Ok(value),
        Guarded::Panicked { cause } => Err(PanicException::new_err(format!(
            "panic in core operation {subject}: {cause}"
        ))),
        Guarded::CoreStopped(report) => Err(CoreStopped::new_err((report.subject, report.cause))),
    }
}
