//! How the core ends the process from one of its own threads: the control
//! and the writer thread of a child (the end of the parent, a bad parent
//! frame), and the liveness and runner heartbeat watches of a worker (its
//! fail-closed exits).
//!
//! - [`ProcessExit::Standard`]: `std::process::exit`. The exit handlers of
//!   the process run: the C `atexit` handlers and the flush of Rust's
//!   standard output. A Rust binary and the Python bridge.
//! - [`ProcessExit::Immediate`]: the drain of [`ExitDrain`], bounded by
//!   [`EXIT_DRAIN_BOUND`], then `libc::_exit`. No exit handler runs. A host
//!   whose runtime runs code on its main thread while a core thread ends the
//!   process: the exit handlers of such a runtime can crash while that code
//!   runs, and the process then ends by a signal in place of its code
//!   (Python's child ends with `os._exit` after a flush of its output for
//!   the same reason).

use std::fmt;
use std::io::Write;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// The longest wait for the drain before an immediate exit: the bound of
/// the log drain of a worker process of the Python bridge before an abort
/// (`ABORT_LOG_DRAIN_BOUND`). A drain past it is left; the process ends.
pub const EXIT_DRAIN_BOUND: Duration = Duration::from_secs(2);

const EXIT_DRAIN_THREAD_NAME: &str = "horsies-exit-drain";

/// How the core ends the process from one of its own threads.
#[derive(Clone)]
pub enum ProcessExit {
    /// `std::process::exit`: the exit handlers of the process run.
    Standard,
    /// The drain, at most [`EXIT_DRAIN_BOUND`], then `libc::_exit`: no exit
    /// handler runs.
    Immediate(ExitDrain),
}

/// What runs before an immediate exit, on a thread of its own.
///
/// [`EXIT_DRAIN_BOUND`] is one bound for every exit place. A child at the
/// end of its parent should end at once: with `WithHost`, a host drain that
/// waits on a busy runtime delays that exit up to the bound. A host whose
/// child must exit within a shorter time gives its child `OutputOnly`, or a
/// host drain that does not wait on its runtime.
#[derive(Clone)]
pub enum ExitDrain {
    /// The flush of Rust's standard output and standard error.
    OutputOnly,
    /// The host's own drain, then the flush: for example the hand-over of
    /// the log records in its channel to the host's runtime. The host's
    /// drain returns when it is done; the exit does not wait for it past
    /// the bound.
    WithHost(Arc<dyn Fn() + Send + Sync>),
}

impl fmt::Debug for ProcessExit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Standard => f.write_str("Standard"),
            Self::Immediate(drain) => f.debug_tuple("Immediate").field(drain).finish(),
        }
    }
}

impl fmt::Debug for ExitDrain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutputOnly => f.write_str("OutputOnly"),
            Self::WithHost(_) => f.write_str("WithHost"),
        }
    }
}

impl ProcessExit {
    /// End the process with `code`. With [`Self::Immediate`] the drain runs
    /// first, at most [`EXIT_DRAIN_BOUND`]. Does not return.
    pub fn end(&self, code: i32) -> ! {
        match self {
            Self::Standard => std::process::exit(code),
            Self::Immediate(drain) => {
                let _drained = drain_within(drain.clone(), EXIT_DRAIN_BOUND);
                // SAFETY: `_exit` takes an int and ends the process at once;
                // it runs no exit handler and touches no Rust state. What it
                // does not do: it flushes no C `stdio` buffer of the host
                // (the drain flushed only Rust's standard output and
                // standard error), and every other thread stops where it is,
                // as with `std::process::exit`.
                unsafe { libc::_exit(code) }
            }
        }
    }
}

impl ExitDrain {
    /// Run the host's drain, then flush Rust's standard output and standard
    /// error, so text that the host drain wrote through them with no final
    /// new line is written too. A flush that fails (a closed stream) is no
    /// failure: the process ends either way.
    fn run(&self) {
        match self {
            Self::OutputOnly => {}
            Self::WithHost(drain) => drain(),
        }
        let _stdout_flushed = std::io::stdout().flush();
        let _stderr_flushed = std::io::stderr().flush();
    }
}

/// How a drain before an exit ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DrainEnd {
    Drained,
    /// The drain was still running at the bound; it is left.
    BoundReached,
    /// The drain panicked.
    DrainPanicked,
    /// The OS refused the drain thread; nothing was drained.
    NoThread,
}

/// Run `drain` on a thread of its own and wait for it at most `bound`. The
/// thread keeps running past the bound; the caller ends the process.
fn drain_within(drain: ExitDrain, bound: Duration) -> DrainEnd {
    let (done, finished) = std::sync::mpsc::sync_channel::<()>(1);
    let started = thread::Builder::new()
        .name(EXIT_DRAIN_THREAD_NAME.to_owned())
        .spawn(move || {
            drain.run();
            // The receiver is gone only after the bound passed.
            let _sent = done.send(());
        });
    match started {
        Err(_refused) => DrainEnd::NoThread,
        Ok(_thread) => match finished.recv_timeout(bound) {
            Ok(()) => DrainEnd::Drained,
            Err(RecvTimeoutError::Timeout) => DrainEnd::BoundReached,
            Err(RecvTimeoutError::Disconnected) => DrainEnd::DrainPanicked,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::process::{Command, Stdio};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Instant;

    use super::*;

    /// A bound for the drain tests that pass it.
    const SHORT_BOUND: Duration = Duration::from_millis(200);
    /// The longest wait for a probe process.
    const PROBE_WAIT: Duration = Duration::from_secs(20);
    /// The run of a host drain that does not return within any test.
    const HUNG_DRAIN: Duration = Duration::from_secs(600);

    #[test]
    fn a_drain_that_returns_ends_before_the_bound_and_runs_the_host_drain() {
        let ran = Arc::new(AtomicBool::new(false));
        let host_ran = Arc::clone(&ran);
        let drain = ExitDrain::WithHost(Arc::new(move || host_ran.store(true, Ordering::Release)));
        assert_eq!(drain_within(drain, PROBE_WAIT), DrainEnd::Drained);
        assert!(ran.load(Ordering::Acquire));
        assert_eq!(
            drain_within(ExitDrain::OutputOnly, PROBE_WAIT),
            DrainEnd::Drained
        );
    }

    #[test]
    fn a_drain_that_does_not_return_is_left_at_the_bound() {
        let drain = ExitDrain::WithHost(Arc::new(|| thread::sleep(HUNG_DRAIN)));
        let started = Instant::now();
        assert_eq!(drain_within(drain, SHORT_BOUND), DrainEnd::BoundReached);
        let waited = started.elapsed();
        assert!(waited >= SHORT_BOUND, "{waited:?}");
        assert!(waited < SHORT_BOUND + Duration::from_secs(5), "{waited:?}");
    }

    #[test]
    fn a_drain_that_panics_ends_the_wait_at_once() {
        let drain = ExitDrain::WithHost(Arc::new(|| {
            if std::hint::black_box(true) {
                panic!("the host drain panicked");
            }
        }));
        assert_eq!(drain_within(drain, PROBE_WAIT), DrainEnd::DrainPanicked);
    }

    /// Set only in the process that the exit tests start; names the exit of
    /// the probe.
    const EXIT_PROBE_ENV: &str = "HORSIES_TEST_PROCESS_EXIT_PROBE";
    const EXIT_PROBE_TEST: &str = "process_exit::tests::exit_probe";
    /// The code that the probe exits with.
    const PROBE_CODE: i32 = 71;
    /// The line that the host drain of the probe writes.
    const DRAIN_LINE: &str = "the host drain ran";

    /// The body of the probe process: the main thread spins, and a second
    /// thread ends the process with the exit of the probe variable.
    /// Without the variable this test returns at once.
    #[test]
    fn exit_probe() {
        let Some(form) = std::env::var_os(EXIT_PROBE_ENV) else {
            return;
        };
        let exit = match form.to_str() {
            Some("standard") => ProcessExit::Standard,
            Some("immediate") => ProcessExit::Immediate(ExitDrain::WithHost(Arc::new(|| {
                // No final new line: only the flush after the host drain
                // writes this text.
                let _written = write!(std::io::stdout(), "{DRAIN_LINE}");
            }))),
            Some("immediate-hung-drain") => {
                ProcessExit::Immediate(ExitDrain::WithHost(Arc::new(|| thread::sleep(HUNG_DRAIN))))
            }
            other => panic!("unknown probe form {other:?}"),
        };
        let _ender = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            exit.end(PROBE_CODE);
        });
        let until = Instant::now() + PROBE_WAIT;
        while Instant::now() < until {
            std::hint::spin_loop();
        }
        panic!("the exit thread did not end the process");
    }

    /// The exit status and the standard output of a probe of `form`, and
    /// the time it ran.
    fn run_probe(form: &str) -> (Option<i32>, String, Duration) {
        let started = Instant::now();
        let output = Command::new(std::env::current_exe().expect("the test binary"))
            .args([
                "--exact",
                EXIT_PROBE_TEST,
                "--nocapture",
                "--test-threads=1",
            ])
            .env(EXIT_PROBE_ENV, form)
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .expect("the probe process runs");
        (
            output.status.code(),
            String::from_utf8_lossy(&output.stdout).into_owned(),
            started.elapsed(),
        )
    }

    /// An immediate exit from a thread that is not the main thread, while
    /// the main thread is busy: the process ends with the code, after the
    /// host drain ran and the flush after it wrote the drain's text.
    #[test]
    fn an_immediate_exit_ends_the_process_with_its_code_after_the_drain() {
        let (code, stdout, _ran) = run_probe("immediate");
        assert_eq!(code, Some(PROBE_CODE));
        assert!(stdout.contains(DRAIN_LINE), "{stdout}");
    }

    /// A host drain that does not return does not hold the exit past the
    /// bound.
    #[test]
    fn an_immediate_exit_does_not_wait_past_the_drain_bound() {
        let (code, _stdout, ran) = run_probe("immediate-hung-drain");
        assert_eq!(code, Some(PROBE_CODE));
        assert!(ran >= EXIT_DRAIN_BOUND, "{ran:?}");
        assert!(ran < EXIT_DRAIN_BOUND + PROBE_WAIT, "{ran:?}");
    }

    /// A standard exit from the same thread gives the code as before.
    #[test]
    fn a_standard_exit_ends_the_process_with_its_code() {
        let (code, _stdout, _ran) = run_probe("standard");
        assert_eq!(code, Some(PROBE_CODE));
    }
}
