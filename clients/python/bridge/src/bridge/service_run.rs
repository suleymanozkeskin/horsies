//! The run form of a core call: a service run that lasts until a stop
//! request, such as a worker or the scheduler loop.
//!
//! A bounded call (`blocking`, `completion_queue`) ends within its bound. A
//! service run lasts until something asks it to stop, often for the life of
//! the process, so it has its own form:
//! - The run is spawned on the core runtime inside [`guard`]: a panic in the
//!   run becomes `PanicException`, and a stop of the core `CoreStopped`.
//! - [`StopSignal`] asks the run to stop. It is idempotent, returns at once
//!   and may be called from any thread. The run ends at its own next
//!   boundary (the end of a tick, the end of a drain), and the run bounds
//!   its own drain.
//! - The blocking form ([`run_blocking`]) runs on the main thread only,
//!   where Python runs signal handlers. It waits for the end of the run with
//!   the GIL released, in slices of [`SIGNAL_CHECK_SLICE`], with no total
//!   bound. Between slices it runs the Python signal handlers
//!   (`check_signals`), so a handler that asks for the stop ends the run. A
//!   handler that raises (for example `KeyboardInterrupt` before the
//!   handlers are in place) asks for the stop; the form then waits for the
//!   end of the run and raises that exception. A further exception during
//!   that wait goes to `sys.unraisablehook`, and the wait goes on.
//! - The async form (`completion_queue::call_service_async`) returns an
//!   asyncio future of the end of the run. A cancel of the future asks for
//!   the stop; the run goes on to its boundary, and its outcome is discarded
//!   with a DEBUG record.

use std::future::Future;
use std::sync::Arc;

use pyo3::panic::PanicException;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use tokio::task::JoinError;

use super::blocking::SIGNAL_CHECK_SLICE;
use super::{guard, guarded_into_result, Guarded};
use crate::errors::NotMainThread;
use crate::logging::RecordLevel;
use crate::process_core::Core;

/// Asks a service run to stop.
#[derive(Clone)]
pub struct StopSignal {
    request: Arc<dyn Fn() + Send + Sync>,
}

impl StopSignal {
    /// A signal that calls `request` at each stop request. `request` must be
    /// idempotent, must return at once, and must not take the GIL: it may
    /// run on an event loop thread inside a done callback.
    pub fn new<F>(request: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        Self {
            request: Arc::new(request),
        }
    }

    /// Ask the run to stop.
    pub fn request(&self) {
        (self.request)();
    }
}

/// Run `run` until it ends and return its outcome as a Python object. Main
/// thread only. There is no total bound: a service run ends when it is
/// asked to stop, and it bounds its own drain.
///
/// Failure exits: `NotMainThread` (called off the main thread; the run did
/// not start), the exception of a signal handler (raised after the run
/// ended), `PanicException` (the run panicked), `CoreStopped`, and the error
/// of the outcome conversion.
pub fn run_blocking<T, F>(
    py: Python<'_>,
    core: &'static Core,
    subject: &'static str,
    stop: &StopSignal,
    run: F,
) -> PyResult<Py<PyAny>>
where
    T: for<'py> IntoPyObject<'py> + Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    refuse_off_main_thread(py, subject)?;
    let handle = core.runtime_handle();
    let mut ended = handle.spawn(guard(core, subject, run));
    let mut interrupt: Option<PyErr> = None;
    loop {
        let slice = py.detach(|| {
            // The timer is built inside `block_on`, where the runtime's time
            // driver is the current one.
            handle.block_on(async { tokio::time::timeout(SIGNAL_CHECK_SLICE, &mut ended).await })
        });
        if let Ok(joined) = slice {
            return finish(py, core, subject, joined, interrupt);
        }
        if let Err(raised) = py.check_signals() {
            interrupt = match interrupt.take() {
                None => {
                    stop.request();
                    Some(raised)
                }
                Some(first) => {
                    raised.write_unraisable(py, None);
                    Some(first)
                }
            };
        }
    }
}

/// The Python result of an ended run: the exception of a signal handler
/// when one raised, else the outcome of the run.
fn finish<T>(
    py: Python<'_>,
    core: &'static Core,
    subject: &'static str,
    joined: Result<Guarded<T>, JoinError>,
    interrupt: Option<PyErr>,
) -> PyResult<Py<PyAny>>
where
    T: for<'py> IntoPyObject<'py>,
{
    let guarded = match joined {
        Ok(guarded) => guarded,
        // `guard` catches the panics of the run, and the runtime lives for
        // the process, so the task cannot end without its outcome.
        Err(join_error) => {
            return Err(PanicException::new_err(format!(
                "the run {subject} ended without an outcome: {join_error}"
            )))
        }
    };
    match interrupt {
        Some(interrupt) => {
            core.log().push_bridge_record(
                RecordLevel::Debug,
                format!(
                    "the run {subject} ended after a signal handler raised; its outcome was \
                     discarded"
                ),
            );
            Err(interrupt)
        }
        None => guarded_into_result(subject, guarded)?.into_py_any(py),
    }
}

/// Refuse a run off the main thread: signal handlers run only there, so the
/// run could not be stopped by a signal.
pub(crate) fn refuse_off_main_thread(py: Python<'_>, subject: &'static str) -> PyResult<()> {
    let threading = py.import("threading")?;
    let current = threading.call_method0("current_thread")?;
    let main = threading.call_method0("main_thread")?;
    match current.is(&main) {
        true => Ok(()),
        false => Err(NotMainThread::new_err((subject,))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use pyo3::exceptions::PyKeyboardInterrupt;
    use pyo3::types::{PyCFunction, PyDict};
    use tokio::sync::watch;

    use super::*;
    use crate::bridge::completion_queue::call_service_async;
    use crate::errors::refusal_error;
    use crate::process_core::core_for_call;

    /// The child test to run, set in the environment of a child process. A
    /// run needs the Python main thread and the core of a process, so each
    /// test runs in a child process of the test binary, where the test
    /// thread initializes Python and so is its main thread.
    const CHILD_TEST_ENV: &str = "HORSIES_SERVICE_RUN_CHILD_TEST";

    /// A Python handler of `SIGUSR1` that raises `KeyboardInterrupt`.
    const INTERRUPT_ON_SIGUSR1: &std::ffi::CStr = c"import signal
def _interrupt(signum, frame):
    raise KeyboardInterrupt('planned interrupt')
signal.signal(signal.SIGUSR1, _interrupt)
";

    /// Send `SIGUSR1` to this process.
    const SEND_SIGUSR1: &std::ffi::CStr = c"import os, signal
os.kill(os.getpid(), signal.SIGUSR1)
";

    /// Start a service run, then cancel its future.
    const CANCEL_THE_RUN: &std::ffi::CStr = c"import asyncio
async def main():
    future = start_run()
    await asyncio.sleep(0.05)
    future.cancel()
    try:
        await future
    except asyncio.CancelledError:
        pass
asyncio.run(main())
";

    /// Longest wait for a run to set its end flag.
    const END_WAIT: Duration = Duration::from_secs(5);

    fn in_child(test: &str) -> bool {
        std::env::var(CHILD_TEST_ENV).is_ok_and(|name| name == test)
    }

    /// Run one test of this binary in a child process and check that it
    /// passed.
    fn assert_child_passed(test: &str) {
        let binary = std::env::current_exe().expect("the test binary has a path");
        let output = std::process::Command::new(binary)
            .args([test, "--exact", "--nocapture", "--test-threads=1"])
            .env(CHILD_TEST_ENV, test)
            .output()
            .expect("the child test process runs");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert!(stdout.contains(&format!("test {test} ... ok")), "{stdout}");
    }

    /// Start Python on this thread, which makes it the Python main thread,
    /// and start the core.
    fn start_core() -> &'static Core {
        Python::initialize();
        Python::attach(|py| core_for_call(py).map_err(refusal_error)).expect("the core starts")
    }

    /// A run that ends when its stop is asked, and sets `ended` at its end.
    fn stoppable_run(
        ended: Arc<AtomicBool>,
    ) -> (
        StopSignal,
        impl Future<Output = &'static str> + Send + 'static,
    ) {
        let (asked, mut seen) = watch::channel(false);
        let stop = StopSignal::new(move || {
            let _previous = asked.send_replace(true);
        });
        let run = async move {
            let _asked_or_closed: bool = seen.wait_for(|asked| *asked).await.is_ok();
            ended.store(true, Ordering::SeqCst);
            "drained"
        };
        (stop, run)
    }

    fn wait_until_set(flag: &AtomicBool, bound: Duration) {
        let deadline = Instant::now() + bound;
        while !flag.load(Ordering::SeqCst) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn a_run_that_ends_gives_its_outcome_child() {
        if !in_child("bridge::service_run::tests::a_run_that_ends_gives_its_outcome_child") {
            return;
        }
        let core = start_core();
        Python::attach(|py| {
            let run = async {
                tokio::time::sleep(Duration::from_millis(250)).await;
                7_i64
            };
            let outcome = run_blocking(py, core, "test run", &StopSignal::new(|| {}), run)
                .expect("the run ends");
            assert_eq!(outcome.bind(py).extract::<i64>().expect("an int"), 7);
        });
    }

    #[test]
    fn a_run_that_ends_gives_its_outcome() {
        assert_child_passed("bridge::service_run::tests::a_run_that_ends_gives_its_outcome_child");
    }

    #[test]
    fn a_raising_signal_handler_stops_the_run_and_raises_after_its_end_child() {
        if !in_child(
            "bridge::service_run::tests::a_raising_signal_handler_stops_the_run_and_raises_after_its_end_child",
        ) {
            return;
        }
        let core = start_core();
        let ended = Arc::new(AtomicBool::new(false));
        Python::attach(|py| {
            py.run(INTERRUPT_ON_SIGUSR1, None, None)
                .expect("the handler installs");
            let (stop, run) = stoppable_run(Arc::clone(&ended));
            let _signaller = std::thread::spawn(|| {
                std::thread::sleep(Duration::from_millis(200));
                Python::attach(|py| py.run(SEND_SIGUSR1, None, None)).expect("the signal is sent");
            });
            let raised = run_blocking(py, core, "test run", &stop, run)
                .expect_err("the exception of the handler is raised");
            assert!(raised.is_instance_of::<PyKeyboardInterrupt>(py), "{raised}");
        });
        assert!(
            ended.load(Ordering::SeqCst),
            "the form raises only after the end of the run"
        );
    }

    #[test]
    fn a_raising_signal_handler_stops_the_run_and_raises_after_its_end() {
        assert_child_passed(
            "bridge::service_run::tests::a_raising_signal_handler_stops_the_run_and_raises_after_its_end_child",
        );
    }

    #[test]
    fn a_run_off_the_main_thread_is_refused_child() {
        if !in_child("bridge::service_run::tests::a_run_off_the_main_thread_is_refused_child") {
            return;
        }
        let core = start_core();
        let ran = Arc::new(AtomicBool::new(false));
        let ran_in_run = Arc::clone(&ran);
        let refused = std::thread::spawn(move || {
            Python::attach(|py| {
                let run = async move {
                    ran_in_run.store(true, Ordering::SeqCst);
                };
                let raised = run_blocking(py, core, "test run", &StopSignal::new(|| {}), run)
                    .expect_err("a run off the main thread is refused");
                raised.is_instance_of::<NotMainThread>(py)
            })
        })
        .join()
        .expect("the thread ends");
        assert!(refused);
        assert!(!ran.load(Ordering::SeqCst), "a refused run does not start");
    }

    #[test]
    fn a_run_off_the_main_thread_is_refused() {
        assert_child_passed(
            "bridge::service_run::tests::a_run_off_the_main_thread_is_refused_child",
        );
    }

    #[test]
    fn a_cancelled_async_run_is_asked_to_stop_child() {
        if !in_child("bridge::service_run::tests::a_cancelled_async_run_is_asked_to_stop_child") {
            return;
        }
        let core = start_core();
        let ended = Arc::new(AtomicBool::new(false));
        let ended_in_runs = Arc::clone(&ended);
        Python::attach(|py| -> PyResult<()> {
            let start_run = PyCFunction::new_closure(
                py,
                None,
                None,
                move |args, _kwargs| -> PyResult<Py<PyAny>> {
                    let (stop, run) = stoppable_run(Arc::clone(&ended_in_runs));
                    call_service_async(args.py(), core, "test service run", stop, run)
                        .map(Bound::unbind)
                },
            )?;
            let globals = PyDict::new(py);
            globals.set_item("start_run", start_run)?;
            py.run(CANCEL_THE_RUN, Some(&globals), None)
        })
        .expect("the script runs");
        wait_until_set(&ended, END_WAIT);
        assert!(
            ended.load(Ordering::SeqCst),
            "a cancel of the future asks the run to stop"
        );
    }

    #[test]
    fn a_cancelled_async_run_is_asked_to_stop() {
        assert_child_passed(
            "bridge::service_run::tests::a_cancelled_async_run_is_asked_to_stop_child",
        );
    }
}
