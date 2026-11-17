//! The blocking form of a core call.
//!
//! The operation runs on the calling thread with the GIL released, in slices
//! of [`SIGNAL_CHECK_SLICE`]. Between slices the thread attaches and calls
//! `Python::check_signals`, so Ctrl+C works on the main thread. A raised
//! signal or the end of the call's [`BlockingBound`] ends the call
//! ([`BLOCKING_CALL_BOUND`] unless the operation names another):
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

/// How long a blocking call waits for its operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockingBound {
    /// The call ends with `CoreCallTimedOut` once this time has passed, in
    /// steps of [`SIGNAL_CHECK_SLICE`].
    Within(Duration),
    /// The call waits until the operation ends, a signal handler raises, or
    /// the core stops. Only for an operation that the caller asked to wait
    /// with no limit (a result wait with no timeout, as Python's).
    UntilTheOperationEnds,
}

impl BlockingBound {
    /// Whether a call that waited `waited` slices of [`SIGNAL_CHECK_SLICE`]
    /// has reached its bound: the bound's limit when it has one. Pure.
    fn reached(self, waited: u128) -> Option<Duration> {
        match self {
            Self::Within(limit) => {
                let slices = limit.as_millis().div_ceil(SIGNAL_CHECK_SLICE.as_millis());
                (waited >= slices).then_some(limit)
            }
            Self::UntilTheOperationEnds => None,
        }
    }
}

type GuardedFuture<T> = Pin<Box<dyn Future<Output = Guarded<T>> + Send + 'static>>;

/// Run `op` and return its outcome as a Python object, within
/// [`BLOCKING_CALL_BOUND`].
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
    call_blocking_within(
        py,
        core,
        subject,
        kind,
        BlockingBound::Within(BLOCKING_CALL_BOUND),
        op,
    )
}

/// Run `op` within `bound` and return its outcome as a Python object.
///
/// Failure exits: as [`call_blocking`]; `CoreCallTimedOut` names the bound
/// in whole seconds, and never ends a call with
/// [`BlockingBound::UntilTheOperationEnds`].
pub fn call_blocking_within<T, F>(
    py: Python<'_>,
    core: &'static Core,
    subject: &'static str,
    kind: CallKind,
    bound: BlockingBound,
    op: F,
) -> PyResult<Py<PyAny>>
where
    T: for<'py> IntoPyObject<'py> + Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    let mut guarded: GuardedFuture<T> = Box::pin(guard(core, subject, op));
    let handle = core.runtime_handle();
    let mut waited: u128 = 0;
    // Each turn waits one slice. A call with a bound ends at its slice
    // count; every call ends at the end of the operation, a raised signal,
    // or the stop of the core (`guard`).
    loop {
        if let Some(limit) = bound.reached(waited) {
            leave(core, subject, kind, guarded);
            return Err(CoreCallTimedOut::new_err((
                subject,
                limit.as_secs(),
                kind.state_after_leave(),
            )));
        }
        waited += 1;
        let slice = py.detach(|| {
            // The timer is built inside `block_on`, where the runtime's
            // time driver is the current one.
            handle.block_on(async {
                tokio::time::timeout(SIGNAL_CHECK_SLICE, guarded.as_mut()).await
            })
        });
        if let Ok(outcome) = slice {
            let value = guarded_into_result(subject, outcome)?;
            return value.into_py_any(py);
        }
        if let Err(signal_error) = py.check_signals() {
            leave(core, subject, kind, guarded);
            return Err(signal_error);
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_bound_ends_after_its_slices() {
        let bound = BlockingBound::Within(BLOCKING_CALL_BOUND);
        let slices = BLOCKING_CALL_BOUND.as_millis() / SIGNAL_CHECK_SLICE.as_millis();
        assert_eq!(bound.reached(slices - 1), None);
        assert_eq!(bound.reached(slices), Some(BLOCKING_CALL_BOUND));
    }

    #[test]
    fn a_bound_that_is_not_whole_slices_waits_one_slice_more() {
        let bound = BlockingBound::Within(Duration::from_millis(250));
        assert_eq!(bound.reached(2), None);
        assert_eq!(bound.reached(3), Some(Duration::from_millis(250)));
    }

    #[test]
    fn a_call_with_no_bound_never_reaches_one() {
        assert_eq!(
            BlockingBound::UntilTheOperationEnds.reached(u128::MAX),
            None
        );
    }
}
