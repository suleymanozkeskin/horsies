//! The core state of one process: the runtime, the process id stamp, the
//! stop rule and the panic hook.
//!
//! - Importing `horsies._core` starts nothing: no runtime, no thread, no
//!   connection.
//! - The first operation starts one tokio runtime and the log forwarder
//!   thread, and stamps them with the process id.
//! - A call in a process whose id differs from the stamp is refused with
//!   `ForkedAfterCoreStart`: the process was forked without `exec` after the
//!   core started, and tokio does not support a runtime in such a child. The
//!   process id is the one check, because `os.register_at_fork` hooks do not
//!   run for a fork from C code. The inherited state is never dropped: it
//!   lives in a static, and dropping a runtime waits for threads that do not
//!   exist in the child.
//! - A panic in a core background task stops the core in this process: every
//!   later call, and every call in flight, ends with `CoreStopped` and the
//!   subject and cause of the first panic.
//!
//! The start runs while the caller is attached to Python, and `os.fork` also
//! needs an attached thread, so a fork cannot happen in the middle of a start.

use std::cell::RefCell;
use std::io::Write;
use std::panic::PanicHookInfo;
use std::sync::{Mutex, Once, OnceLock};

use pyo3::prelude::*;
use tokio::sync::watch;

use crate::logging::{self, LogBridge};

/// Worker threads of the runtime of a producer process. In the bridge
/// measurement, two workers gave the same single-call latency as eight, and an
/// idle producer keeps one OS thread per worker.
pub const PRODUCER_RUNTIME_WORKER_THREADS: usize = 2;

/// Upper bound of tokio's blocking pool. The bridge does not use the pool;
/// the driver uses it for host name lookups.
pub const MAX_BLOCKING_THREADS: usize = 16;

/// Name of the runtime worker threads.
pub const RUNTIME_THREAD_NAME: &str = "horsies-core";

/// The first panic of a core background task: what failed, and why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PanicReport {
    pub subject: String,
    pub cause: String,
}

/// Why the core could not start.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartFailure {
    StartLockPoisoned,
    RuntimeBuildFailed { cause: String },
    LogForwarderStartFailed { cause: String },
}

/// Why a call cannot use the core in this process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallRefusal {
    /// The core started in process `core_pid`; this process is a fork child.
    ForkedAfterStart { core_pid: u32, this_pid: u32 },
    /// A core background task panicked; the core is stopped in this process.
    Stopped(PanicReport),
    /// The core could not start.
    StartFailed(StartFailure),
}

/// The state of the core in this process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoreStatus {
    NotStarted,
    Running,
    Stopped,
    /// The core started in a parent process and this process is its fork.
    Inherited,
}

/// The started core of one process.
pub struct Core {
    pid: u32,
    runtime: tokio::runtime::Runtime,
    first_panic: OnceLock<PanicReport>,
    stop_signal: watch::Sender<bool>,
    log: LogBridge,
}

static CORE: OnceLock<Core> = OnceLock::new();
static START_LOCK: Mutex<()> = Mutex::new(());
static PANIC_HOOK: Once = Once::new();

thread_local! {
    /// Cause of the last panic on this thread, written by the panic hook so
    /// that the catch site can report the source location.
    static LAST_PANIC: RefCell<Option<String>> = const { RefCell::new(None) };
}

impl Core {
    /// The handle of the core runtime.
    pub fn runtime_handle(&self) -> &tokio::runtime::Handle {
        self.runtime.handle()
    }

    /// The log bridge of this process.
    pub fn log(&self) -> &LogBridge {
        &self.log
    }

    /// Stop the core in this process: keep the first report, wake every call
    /// in flight, and refuse every later call. The report goes to stderr at
    /// once, without the GIL.
    pub fn stop(&self, report: PanicReport) {
        let _write_failure_ignored = writeln!(
            std::io::stderr(),
            "horsies: the core stopped in process {}: panic in {}: {}",
            self.pid,
            report.subject,
            report.cause
        );
        let _first_report_wins = self.first_panic.set(report);
        let _previous_value = self.stop_signal.send_replace(true);
    }

    /// A receiver that sees `true` once the core stopped.
    pub fn stop_receiver(&self) -> watch::Receiver<bool> {
        self.stop_signal.subscribe()
    }

    /// The first panic report, once the core stopped.
    pub fn stopped_report(&self) -> Option<&PanicReport> {
        self.first_panic.get()
    }
}

/// The core for one call. Starts the core at the first call of the process;
/// refuses a forked or stopped process. The caller is attached to Python.
pub fn core_for_call(py: Python<'_>) -> Result<&'static Core, CallRefusal> {
    let core = match CORE.get() {
        Some(core) => core,
        None => {
            let started = start_core()?;
            // Read the Python levels after the start lock is released: a
            // logging call can wait on a Python lock.
            if let Err(error) = started.log.refresh_levels(py) {
                error.write_unraisable(py, None);
            }
            started
        }
    };
    match pure_call_refusal() {
        Some(refusal) => Err(refusal),
        None => Ok(core),
    }
}

/// The refusal of a pure call (one that needs no runtime) in this process:
/// the fork rule and the stop rule, as for every core call. Starts nothing.
pub fn pure_call_refusal() -> Option<CallRefusal> {
    let core = CORE.get()?;
    let this_pid = std::process::id();
    match (core.pid == this_pid, core.stopped_report()) {
        (false, _) => Some(CallRefusal::ForkedAfterStart {
            core_pid: core.pid,
            this_pid,
        }),
        (true, Some(report)) => Some(CallRefusal::Stopped(report.clone())),
        (true, None) => None,
    }
}

/// The core of this process when it started here and runs. Starts nothing.
pub fn running_core() -> Option<&'static Core> {
    match (CORE.get(), core_status()) {
        (Some(core), CoreStatus::Running) => Some(core),
        (Some(_), CoreStatus::NotStarted | CoreStatus::Stopped | CoreStatus::Inherited)
        | (None, _) => None,
    }
}

/// The state of the core in this process. Starts nothing.
pub fn core_status() -> CoreStatus {
    match CORE.get() {
        None => CoreStatus::NotStarted,
        Some(core) => match (core.pid == std::process::id(), core.stopped_report()) {
            (false, _) => CoreStatus::Inherited,
            (true, Some(_)) => CoreStatus::Stopped,
            (true, None) => CoreStatus::Running,
        },
    }
}

/// Stop the core of this process after a panic in a core background task.
/// Before the core is set, the report goes to stderr only.
pub fn stop_after_background_panic(report: PanicReport) {
    match CORE.get() {
        Some(core) => core.stop(report),
        None => {
            let _write_failure_ignored = writeln!(
                std::io::stderr(),
                "horsies: panic in {} before the core started: {}",
                report.subject,
                report.cause
            );
        }
    }
}

fn start_core() -> Result<&'static Core, CallRefusal> {
    let _start_guard = START_LOCK
        .lock()
        .map_err(|_poisoned| CallRefusal::StartFailed(StartFailure::StartLockPoisoned))?;
    if let Some(core) = CORE.get() {
        return Ok(core);
    }
    install_panic_hook();
    let (log, records) = LogBridge::new();
    let runtime = build_runtime(log.dispatch().clone()).map_err(CallRefusal::StartFailed)?;
    logging::start_forwarder(records, log.sink()).map_err(|error| {
        CallRefusal::StartFailed(StartFailure::LogForwarderStartFailed {
            cause: error.to_string(),
        })
    })?;
    let (stop_signal, _first_receiver) = watch::channel(false);
    let core = Core {
        pid: std::process::id(),
        runtime,
        first_panic: OnceLock::new(),
        stop_signal,
        log,
    };
    // The start lock is held and the cell is empty, so this stores `core`.
    Ok(CORE.get_or_init(|| core))
}

fn build_runtime(dispatch: tracing::Dispatch) -> Result<tokio::runtime::Runtime, StartFailure> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(PRODUCER_RUNTIME_WORKER_THREADS)
        .max_blocking_threads(MAX_BLOCKING_THREADS)
        .thread_name(RUNTIME_THREAD_NAME)
        .on_thread_start(move || logging::make_thread_default(&dispatch))
        .enable_time()
        .enable_io()
        .build()
        .map_err(|error| StartFailure::RuntimeBuildFailed {
            cause: error.to_string(),
        })
}

/// Chain a hook that keeps the cause of each panic on its thread, so that
/// the catch site can report the source location and not only the message.
/// The previous hook still runs, so the panic still goes to stderr.
fn install_panic_hook() {
    PANIC_HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info: &PanicHookInfo<'_>| {
            let cause = describe_panic(info);
            let _thread_ending = LAST_PANIC.try_with(|slot| {
                if let Ok(mut kept) = slot.try_borrow_mut() {
                    *kept = Some(cause);
                }
            });
            previous(info);
        }));
    });
}

fn describe_panic(info: &PanicHookInfo<'_>) -> String {
    let message = payload_text(info.payload());
    match info.location() {
        Some(location) => format!("{message} (at {}:{})", location.file(), location.line()),
        None => format!("{message} (at an unknown location)"),
    }
}

fn payload_text(payload: &(dyn std::any::Any + Send)) -> String {
    match (
        payload.downcast_ref::<&str>(),
        payload.downcast_ref::<String>(),
    ) {
        (Some(text), _) => (*text).to_owned(),
        (None, Some(text)) => text.clone(),
        (None, None) => "the panic payload is not text".to_owned(),
    }
}

/// The cause that the hook kept on this thread, or the payload text when the
/// hook did not run (a hook installed later replaced ours).
pub fn take_panic_cause(payload: &(dyn std::any::Any + Send)) -> String {
    let kept = LAST_PANIC
        .try_with(|slot| match slot.try_borrow_mut() {
            Ok(mut kept) => kept.take(),
            Err(_already_borrowed) => None,
        })
        .ok()
        .flatten();
    match kept {
        Some(cause) => cause,
        None => payload_text(payload),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_text_payload_is_the_cause_when_no_hook_ran() {
        let payload: Box<dyn std::any::Any + Send> = Box::new("broken invariant");
        assert_eq!(take_panic_cause(payload.as_ref()), "broken invariant");
    }

    #[test]
    fn an_owned_text_payload_is_the_cause() {
        let payload: Box<dyn std::any::Any + Send> = Box::new(String::from("owned text"));
        assert_eq!(payload_text(payload.as_ref()), "owned text");
    }

    #[test]
    fn a_payload_that_is_not_text_is_named() {
        let payload: Box<dyn std::any::Any + Send> = Box::new(7_u8);
        assert_eq!(
            payload_text(payload.as_ref()),
            "the panic payload is not text"
        );
    }

    #[test]
    fn the_hook_keeps_the_source_location() {
        install_panic_hook();
        let caught = std::panic::catch_unwind(|| panic!("kept by the hook"));
        let payload = caught.expect_err("the closure panics");
        let cause = take_panic_cause(payload.as_ref());
        assert!(cause.starts_with("kept by the hook (at "), "{cause}");
        assert!(cause.contains("process_core.rs"), "{cause}");
    }
}
