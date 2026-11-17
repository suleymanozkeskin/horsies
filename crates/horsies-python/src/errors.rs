//! The Python exception classes of the core boundary.
//!
//! Two families:
//! - `CoreError` (an `Exception`): the call could not use the core in this
//!   process, or did not end within its bound. Each class names its cause in
//!   its arguments.
//! - `CoreDefect` (a `BaseException`): the client sent a value that the core
//!   refuses although the client should have refused it first. `except
//!   Exception` does not catch it, as for `PanicException`.
//!
//! Expected failures of an operation are not exceptions: an operation returns
//! a typed failure record (`records.rs`).

use pyo3::create_exception;
use pyo3::exceptions::{PyBaseException, PyException};
use pyo3::prelude::*;

use crate::process_core::{CallRefusal, PanicRuleFixed as RuleFixed, StartFailure};

create_exception!(
    horsies._core,
    CoreError,
    PyException,
    "Base of the errors raised at the core boundary."
);
create_exception!(
    horsies._core,
    ForkedAfterCoreStart,
    CoreError,
    "This process was forked without exec after the core started in its parent. \
     The core cannot run in such a child. args: (core_pid, this_pid, advice)."
);
create_exception!(
    horsies._core,
    CoreStopped,
    CoreError,
    "A core background task panicked, so the core is stopped in this process. \
     args: (subject, cause) of the first panic."
);
create_exception!(
    horsies._core,
    CoreStartFailed,
    CoreError,
    "The core runtime or its log forwarder could not start. args: (cause,)."
);
create_exception!(
    horsies._core,
    CoreCallTimedOut,
    CoreError,
    "A blocking call did not end within its bound. \
     args: (subject, bound_seconds, state_after); state_after is 'no_change' for a read \
     and 'outcome_unknown' for a write, which runs on to its end."
);
create_exception!(
    horsies._core,
    TooManyPendingCalls,
    CoreError,
    "The event loop already has the most core calls in flight that it may have. \
     args: (subject, bound)."
);
create_exception!(
    horsies._core,
    CoreDefect,
    PyBaseException,
    "The client sent a value that the core refuses. This is a defect of the client."
);
create_exception!(
    horsies._core,
    ManifestRefused,
    CoreDefect,
    "The core refused a manifest. args: (field_path, cause)."
);
create_exception!(
    horsies._core,
    PanicRuleFixed,
    CoreDefect,
    "The panic rule of this process was set to another rule before, or fixed when the core \
     started. args: (current_rule,)."
);
create_exception!(
    horsies._core,
    NotMainThread,
    CoreDefect,
    "A run form was called off the main thread, where Python runs signal handlers. \
     The run did not start. args: (subject,)."
);

/// The advice of `ForkedAfterCoreStart`.
pub const FORK_ADVICE: &str =
    "use the 'spawn' or 'forkserver' start method for child processes that call horsies";

/// The Python error of a call that cannot use the core.
pub fn refusal_error(refusal: CallRefusal) -> PyErr {
    match refusal {
        CallRefusal::ForkedAfterStart { core_pid, this_pid } => {
            ForkedAfterCoreStart::new_err((core_pid, this_pid, FORK_ADVICE))
        }
        CallRefusal::Stopped(report) => CoreStopped::new_err((report.subject, report.cause)),
        CallRefusal::StartFailed(failure) => {
            CoreStartFailed::new_err((start_failure_text(&failure),))
        }
    }
}

/// The Python error of a refused panic rule.
pub fn panic_rule_error(refused: RuleFixed) -> PyErr {
    PanicRuleFixed::new_err((refused.current.as_str(),))
}

fn start_failure_text(failure: &StartFailure) -> String {
    match failure {
        StartFailure::StartLockPoisoned => {
            "the core start lock was poisoned by an earlier panic during a start".to_owned()
        }
        StartFailure::RuntimeBuildFailed { cause } => {
            format!("the tokio runtime could not be built: {cause}")
        }
        StartFailure::LogForwarderStartFailed { cause } => {
            format!("the log forwarder thread could not start: {cause}")
        }
    }
}

/// Add every exception class to the module, and PyO3's `PanicException`,
/// which Python cannot import from anywhere else.
pub fn add_errors(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = module.py();
    module.add("CoreError", py.get_type::<CoreError>())?;
    module.add(
        "ForkedAfterCoreStart",
        py.get_type::<ForkedAfterCoreStart>(),
    )?;
    module.add("CoreStopped", py.get_type::<CoreStopped>())?;
    module.add("CoreStartFailed", py.get_type::<CoreStartFailed>())?;
    module.add("CoreCallTimedOut", py.get_type::<CoreCallTimedOut>())?;
    module.add("TooManyPendingCalls", py.get_type::<TooManyPendingCalls>())?;
    module.add("CoreDefect", py.get_type::<CoreDefect>())?;
    module.add("ManifestRefused", py.get_type::<ManifestRefused>())?;
    module.add("PanicRuleFixed", py.get_type::<PanicRuleFixed>())?;
    module.add("NotMainThread", py.get_type::<NotMainThread>())?;
    module.add(
        "PanicException",
        py.get_type::<pyo3::panic::PanicException>(),
    )?;
    Ok(())
}
