//! The blocking form of a core call.
//!
//! The operation runs on the calling thread with the GIL released, in slices
//! of [`SIGNAL_CHECK_SLICE`]. Between slices the thread attaches and calls
//! `Python::check_signals`, so Ctrl+C works on the main thread. A raised
//! signal or the end of [`BLOCKING_CALL_BOUND`] ends the call:
//! - a read is dropped inside the runtime context;
//! - a write moves to the runtime and runs on to its end; its end is logged
//!   at DEBUG level.
//!
//! The operation runs on the calling thread, not on a runtime worker, so a
//! blocking call has no thread hand-off.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

use super::{guard, guarded_into_result, CallKind, Guarded};
use crate::errors::CoreCallTimedOut;
use crate::logging::RecordLevel;
use crate::process_core::Core;

/// Time the GIL stays released before a blocking call checks signals.
pub const SIGNAL_CHECK_SLICE: Duration = Duration::from_millis(100);

/// Upper bound of one blocking call. It is above every bound inside the core
/// that a call can meet: the install lock wait (5 minutes), the pool
/// checkout timeout, and the time Linux takes to give up on a silently dead
/// connection without a TCP user timeout (about 924 s).
pub const BLOCKING_CALL_BOUND: Duration = Duration::from_secs(30 * 60);

/// Slices of one blocking call: [`BLOCKING_CALL_BOUND`] in steps of
/// [`SIGNAL_CHECK_SLICE`].
const MAX_BLOCKING_SLICES: u128 = BLOCKING_CALL_BOUND.as_millis() / SIGNAL_CHECK_SLICE.as_millis();

type GuardedFuture<T> = Pin<Box<dyn Future<Output = Guarded<T>> + Send + 'static>>;

/// Run `op` and return its outcome as a Python object.
///
/// Failure exits: `PanicException` (the operation panicked), `CoreStopped`,
/// the exception of a signal handler (for example `KeyboardInterrupt`),
/// `CoreCallTimedOut`, and the error of the outcome conversion.
pub fn call_blocking<T, F>(
    py: Python<'_>,
    core: &'static Core,
    subject: &'static str,
    kind: CallKind,
    op: F,
) -> PyResult<Py<PyAny>>
where
    T: for<'py> IntoPyObject<'py> + Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    let mut guarded: GuardedFuture<T> = Box::pin(guard(core, subject, op));
    let handle = core.runtime_handle();
    for _slice in 0..MAX_BLOCKING_SLICES {
        let slice = py.detach(|| {
            // The timer is built inside `block_on`, where the runtime's
            // time driver is the current one.
            handle.block_on(async {
                tokio::time::timeout(SIGNAL_CHECK_SLICE, guarded.as_mut()).await
            })
        });
        match slice {
            Ok(outcome) => {
                let value = guarded_into_result(subject, outcome)?;
                return value.into_py_any(py);
            }
            Err(_slice_ended) => {}
        }
        match py.check_signals() {
            Ok(()) => {}
            Err(signal_error) => {
                leave(core, subject, kind, guarded);
                return Err(signal_error);
            }
        }
    }
    leave(core, subject, kind, guarded);
    Err(CoreCallTimedOut::new_err((
        subject,
        BLOCKING_CALL_BOUND.as_secs(),
        kind.state_after_leave(),
    )))
}

/// End the caller's wait on an operation that has not ended.
fn leave<T>(core: &'static Core, subject: &'static str, kind: CallKind, guarded: GuardedFuture<T>)
where
    T: Send + 'static,
{
    match kind {
        CallKind::Read => {
            // Dropping a pooled connection returns it through a runtime
            // task, so the drop runs inside the runtime context.
            let _context = core.runtime_handle().enter();
            drop(guarded);
        }
        CallKind::Write => {
            let _detached = core.runtime_handle().spawn(async move {
                let _outcome_unknown_to_caller = guarded.await;
                core.log().push_bridge_record(
                    RecordLevel::Debug,
                    format!("a write of {subject} ended after its caller left"),
                );
            });
        }
    }
}
