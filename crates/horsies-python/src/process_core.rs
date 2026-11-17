//! The core state of one process: the runtime, the process id stamp, the
//! stop rule and the panic hook.
//!
//! - Importing `horsies._core` starts nothing: no runtime, no thread, no
//!   connection.
//! - The first operation starts the core's runtime of the process
//!   (`horsies_core::process_runtime`, shaped here: two workers, the log
//!   dispatch on each thread) and the log forwarder thread, and stamps them
//!   with the process id. The core's pools and listener drive their
//!   connections on that runtime. The callback thread starts later, at the
//!   first Python callback (`bridge::callback_thread`).
//! - A call in a process whose id differs from the stamp is refused with
//!   `ForkedAfterCoreStart`: the process was forked without `exec` after the
//!   core started, and tokio does not support a runtime in such a child. The
//!   process id is the one check, because `os.register_at_fork` hooks do not
//!   run for a fork from C code. The inherited state is never dropped: it
//!   lives in a static, and dropping a runtime waits for threads that do not
//!   exist in the child.
//! - A panic in a core background task follows the process panic rule
//!   ([`PanicRule`]), set once by the entry point and fixed when the core
//!   starts. Under `StopCore`, the default, the core stops in this process:
//!   every later call, and every call in flight, ends with `CoreStopped` and
//!   the subject and cause of the first panic. Under `AbortProcess`, the rule
//!   of a worker process, the panic goes to stderr and to Python `logging`,
//!   and the process aborts; its supervisor restarts it.
//!
//! The start runs while the caller is attached to Python, and `os.fork` also
//! needs an attached thread, so a fork cannot happen in the middle of a start.

use std::cell::RefCell;
use std::io::Write;
use std::num::NonZeroUsize;
use std::panic::PanicHookInfo;
use std::sync::{Arc, Mutex, Once, OnceLock};

use horsies_core::process_runtime::{
    process_runtime, shape_process_runtime, ProcessRuntime, RuntimeShape, ShapeOutcome, ThreadStart,
};
use pyo3::prelude::*;
use tokio::sync::watch;

use crate::bridge::callback_thread::{CallbackThreadSlot, CALLBACK_QUEUE_CAPACITY};
use crate::logging::{self, DrainOutcome, LogBridge, RecordLevel};

/// Worker threads of the runtime of a producer process. In the bridge
/// measurement, two workers gave the same single-call latency as eight, and an
/// idle producer keeps one OS thread per worker.
pub const PRODUCER_RUNTIME_WORKER_THREADS: usize = 2;

/// Upper bound of tokio's blocking pool. The bridge does not use the pool;
/// the driver uses it for host name lookups.
pub const MAX_BLOCKING_THREADS: usize = 16;

/// Name of the runtime worker threads.
pub const RUNTIME_THREAD_NAME: &str = "horsies-core";

/// How long an aborting worker process waits for the log forwarder to hand
/// the records in the channel to Python `logging`. A full channel is 8,192
/// records in batches of 256; at tens of microseconds per record, the
/// forwarder empties it well inside this bound.
pub const ABORT_LOG_DRAIN_BOUND: std::time::Duration = std::time::Duration::from_secs(2);

/// What a panic in a core background task does to this process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PanicRule {
    /// Stop the core in this process; every later call raises `CoreStopped`.
    /// The rule of a process that the core does not own, such as a producer
    /// inside a web app. The default.
    StopCore,
    /// Write the panic to stderr and to Python `logging`, wait for the log
    /// forwarder within [`ABORT_LOG_DRAIN_BOUND`], and abort the process; its
    /// supervisor restarts it. The rule of a worker process.
    AbortProcess,
}

impl PanicRule {
    /// The text form at the Python boundary.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StopCore => "STOP_CORE",
            Self::AbortProcess => "ABORT_PROCESS",
        }
    }
}

/// The panic rule was set to another rule before, or fixed by the start of
/// the core. `current` is the rule in force.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PanicRuleFixed {
    pub current: PanicRule,
}

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
    runtime: &'static ProcessRuntime,
    first_panic: OnceLock<PanicReport>,
    stop_signal: watch::Sender<bool>,
    log: LogBridge,
    callbacks: CallbackThreadSlot,
}

static CORE: OnceLock<Core> = OnceLock::new();
/// The process panic rule. Set once by the entry point; the start of the core
/// fixes it, at `StopCore` when no entry point set it.
static PANIC_RULE: OnceLock<PanicRule> = OnceLock::new();
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

    /// The callback thread of this process, started at the first callback.
    pub fn callbacks(&self) -> &CallbackThreadSlot {
        &self.callbacks
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

/// Set the panic rule of this process. The entry point calls it before the
/// first core call. Setting the rule in force again succeeds and changes
/// nothing; another rule is refused once a rule is set or the core started.
pub fn set_panic_rule(rule: PanicRule) -> Result<(), PanicRuleFixed> {
    let current = *PANIC_RULE.get_or_init(|| rule);
    match current == rule {
        true => Ok(()),
        false => Err(PanicRuleFixed { current }),
    }
}

/// The panic rule in force: the rule set or fixed, else the default.
pub fn panic_rule() -> PanicRule {
    PANIC_RULE.get().copied().unwrap_or(PanicRule::StopCore)
}

/// Apply the process panic rule to a panic in a core background task. Under
/// `AbortProcess` this does not return. The caller is not attached to Python.
pub fn background_task_panicked(report: PanicReport) {
    match panic_rule() {
        PanicRule::StopCore => stop_after_background_panic(report),
        PanicRule::AbortProcess => abort_after_background_panic(&report),
    }
}

/// Write the report to stderr, push one ERROR record, wait for the forwarder
/// within [`ABORT_LOG_DRAIN_BOUND`], and abort the process.
fn abort_after_background_panic(report: &PanicReport) -> ! {
    let pid = std::process::id();
    let _write_failure_ignored = writeln!(
        std::io::stderr(),
        "horsies: panic in core background task {} in worker process {pid}: {}; the process \
         aborts",
        report.subject,
        report.cause
    );
    if let Some(core) = CORE.get() {
        flush_log_before_abort(core, report);
    }
    std::process::abort()
}

fn flush_log_before_abort(core: &Core, report: &PanicReport) {
    core.log().push_bridge_record(
        RecordLevel::Error,
        format!(
            "panic in core background task {}: {}; the worker process aborts",
            report.subject, report.cause
        ),
    );
    let unsent = match core.log().wait_for_drain(ABORT_LOG_DRAIN_BOUND) {
        DrainOutcome::Drained => return,
        DrainOutcome::ForwarderStopped { left } => {
            format!("the log forwarder is not running; {left} log records")
        }
        DrainOutcome::BoundReached { left } => format!(
            "{left} log records were still in the channel after {} ms and",
            ABORT_LOG_DRAIN_BOUND.as_millis()
        ),
    };
    let _write_failure_ignored = writeln!(
        std::io::stderr(),
        "horsies: {unsent} did not reach Python logging before the abort"
    );
}

/// Stop the core of this process after a panic in a core background task.
/// Before the core is set, the report goes to stderr only.
fn stop_after_background_panic(report: PanicReport) {
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
    // Fix the panic rule: a rule set after this point could miss a panic of a
    // task that this start runs.
    let _rule_in_force = PANIC_RULE.get_or_init(|| PanicRule::StopCore);
    let (log, records) = LogBridge::new();
    let runtime = start_runtime(log.dispatch().clone()).map_err(CallRefusal::StartFailed)?;
    logging::start_forwarder(records, log.sink()).map_err(|error| {
        CallRefusal::StartFailed(StartFailure::LogForwarderStartFailed {
            cause: error.to_string(),
        })
    })?;
    let (stop_signal, _first_receiver) = watch::channel(false);
    let callbacks = CallbackThreadSlot::new(CALLBACK_QUEUE_CAPACITY, log.dispatch().clone());
    let core = Core {
        pid: std::process::id(),
        runtime,
        first_panic: OnceLock::new(),
        stop_signal,
        log,
        callbacks,
    };
    // The start lock is held and the cell is empty, so this stores `core`.
    Ok(CORE.get_or_init(|| core))
}

/// Shape the core's runtime of the process for a Python process and start
/// it. A runtime that runs already (an earlier start that failed after it,
/// at the log forwarder) is used as it is: it has this shape.
fn start_runtime(dispatch: tracing::Dispatch) -> Result<&'static ProcessRuntime, StartFailure> {
    let shape = RuntimeShape {
        worker_threads: NonZeroUsize::new(PRODUCER_RUNTIME_WORKER_THREADS)
            .expect("the producer runtime has worker threads"),
        max_blocking_threads: NonZeroUsize::new(MAX_BLOCKING_THREADS)
            .expect("the producer runtime has a blocking pool"),
        thread_name: RUNTIME_THREAD_NAME,
        on_thread_start: ThreadStart::Run(Arc::new(move || {
            logging::make_thread_default(&dispatch)
        })),
    };
    match shape_process_runtime(shape) {
        ShapeOutcome::Shaped | ShapeOutcome::AlreadyRunning => {}
    }
    process_runtime().map_err(|refusal| StartFailure::RuntimeBuildFailed {
        cause: refusal.to_string(),
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
    use std::os::unix::process::ExitStatusExt;

    use super::*;

    /// The child test to run, set in the environment of a child process. The
    /// panic rule and the core are per process, so the tests that set them
    /// run in a child process of the test binary.
    const CHILD_TEST_ENV: &str = "HORSIES_PROCESS_CORE_CHILD_TEST";

    /// The abort signal number on Linux and macOS.
    const SIGABRT: i32 = 6;

    /// The panic message of the child tests.
    const PLANNED_PANIC: &str = "planned background panic";

    /// Python logging of the child: every record on stderr, one line each.
    const LOGGING_TO_STDERR: &std::ffi::CStr = c"import logging, sys
logging.basicConfig(stream=sys.stderr, level=logging.INFO, format='LOG %(name)s %(levelname)s %(message)s', force=True)
";

    fn in_child(test: &str) -> bool {
        std::env::var(CHILD_TEST_ENV).is_ok_and(|name| name == test)
    }

    /// Run one test of this binary in a child process.
    fn run_child(test: &str) -> std::process::Output {
        let binary = std::env::current_exe().expect("the test binary has a path");
        std::process::Command::new(binary)
            .args([test, "--exact", "--nocapture", "--test-threads=1"])
            .env(CHILD_TEST_ENV, test)
            .output()
            .expect("the child test process runs")
    }

    /// Start Python, send its logging to stderr, and start the core.
    fn start_core_with_logging() {
        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            py.run(LOGGING_TO_STDERR, None, None)?;
            core_for_call(py)
                .map(|_core| ())
                .map_err(crate::errors::refusal_error)
        })
        .expect("the core starts");
    }

    /// Panic on a new thread and hand the panic to the rule, as the catch
    /// site of a core background task does.
    fn panic_in_background_task() {
        let task = std::thread::Builder::new()
            .name("rule-test-task".to_owned())
            .spawn(|| {
                let caught = std::panic::catch_unwind(|| panic!("{PLANNED_PANIC}"));
                if let Err(payload) = caught {
                    background_task_panicked(PanicReport {
                        subject: "thread rule-test-task".to_owned(),
                        cause: take_panic_cause(payload.as_ref()),
                    });
                }
            })
            .expect("the task thread starts");
        let _joined = task.join();
    }

    #[test]
    fn abort_rule_child() {
        if !in_child("process_core::tests::abort_rule_child") {
            return;
        }
        assert_eq!(set_panic_rule(PanicRule::AbortProcess), Ok(()));
        assert_eq!(set_panic_rule(PanicRule::AbortProcess), Ok(()));
        assert_eq!(
            set_panic_rule(PanicRule::StopCore),
            Err(PanicRuleFixed {
                current: PanicRule::AbortProcess
            })
        );
        start_core_with_logging();
        panic_in_background_task();
        // Under the abort rule the line above does not return; the parent
        // test fails on a normal exit.
    }

    #[test]
    fn a_worker_process_aborts_on_a_background_panic_after_its_log_record() {
        let output = run_child("process_core::tests::abort_rule_child");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.signal(), Some(SIGABRT), "{stderr}");
        assert!(
            stderr.contains(
                "horsies: panic in core background task thread rule-test-task in worker process"
            ),
            "{stderr}"
        );
        let last_record = stderr
            .lines()
            .rev()
            .find(|line| line.starts_with("LOG "))
            .unwrap_or_default();
        assert!(
            last_record.starts_with(&format!(
                "LOG horsies.bridge ERROR panic in core background task thread rule-test-task: \
                 {PLANNED_PANIC}"
            )),
            "{stderr}"
        );
        assert!(
            last_record.ends_with("the worker process aborts"),
            "{stderr}"
        );
    }

    #[test]
    fn stop_rule_child() {
        if !in_child("process_core::tests::stop_rule_child") {
            return;
        }
        start_core_with_logging();
        assert_eq!(panic_rule(), PanicRule::StopCore);
        assert_eq!(
            set_panic_rule(PanicRule::AbortProcess),
            Err(PanicRuleFixed {
                current: PanicRule::StopCore
            })
        );
        panic_in_background_task();
        assert_eq!(core_status(), CoreStatus::Stopped);
        match pure_call_refusal() {
            Some(CallRefusal::Stopped(report)) => {
                assert_eq!(report.subject, "thread rule-test-task");
                assert!(report.cause.starts_with(PLANNED_PANIC), "{}", report.cause);
            }
            other => panic!("a stopped core refuses every call, got {other:?}"),
        }
    }

    #[test]
    fn a_producer_process_stops_its_core_on_a_background_panic() {
        let output = run_child("process_core::tests::stop_rule_child");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert!(
            stdout.contains("test process_core::tests::stop_rule_child ... ok"),
            "{stdout}"
        );
    }

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
