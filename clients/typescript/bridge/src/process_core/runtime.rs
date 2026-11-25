//! The core of one process: the runtime, the process id stamp, and the stop
//! signal.
//!
//! - Loading the addon starts nothing: no runtime, no thread, no connection.
//!   A pure function never starts the core.
//! - The first core call of the process starts the log bridge and the core's
//!   process runtime (`horsies_core::process_runtime`, shaped here: two
//!   workers, the log dispatch on each thread), and stamps them with the
//!   process id. The core's pools and listener drive their connections on
//!   that runtime.
//! - In a task or handler child, the child host shapes the runtime first;
//!   the shape step here then answers `AlreadyRunning`, and the runtime keeps
//!   the child's shape.
//! - A call in a process whose id differs from the stamp is refused with
//!   `ForkedAfterCoreStart`. Node and Bun start child processes with exec, so
//!   it does not occur in practice; the refusal stays because the core's
//!   runtime has it.
//! - Under the panic rule `StopCore`, a panic in a core background task stops
//!   the core: every later call, and every call in flight, ends with
//!   `CoreStopped` and the subject and cause of the first panic.

use std::io::Write;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, OnceLock};

use horsies_core::process_runtime::{
    process_runtime, shape_process_runtime, ProcessRuntime, RuntimeShape, ShapeOutcome, ThreadStart,
};
use napi_derive::napi;
use tokio::sync::watch;

use crate::call::thrown::{NativeThrown, StateAfter};
use crate::logging::bridge::{make_thread_default, started_log, LogBridge};
use crate::process_core::panic_rule::{fix_panic_rule, install_panic_hook, PanicReport};

/// Worker threads of the runtime of a producer process and of a worker
/// parent. A call waits on one connection, so two workers give the latency
/// of one call that eight give, and an idle producer keeps one OS thread per
/// worker.
pub const RUNTIME_WORKER_THREADS: NonZeroUsize = match NonZeroUsize::new(2) {
    Some(threads) => threads,
    None => panic!("2 is not zero"),
};

/// Upper bound of tokio's blocking pool. The bridge does not use the pool;
/// the driver uses it for host name lookups.
pub const RUNTIME_MAX_BLOCKING_THREADS: NonZeroUsize = match NonZeroUsize::new(16) {
    Some(threads) => threads,
    None => panic!("16 is not zero"),
};

/// Name of the runtime worker threads.
pub const RUNTIME_THREAD_NAME: &str = "horsies-core";

/// Why the core could not start.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartFailure {
    StartLockPoisoned,
    RuntimeBuildFailed { cause: String },
    LogForwarderStartFailed { cause: String },
}

impl StartFailure {
    /// The cause as text.
    pub fn cause(&self) -> String {
        match self {
            Self::StartLockPoisoned => {
                "the core start lock was poisoned by an earlier panic during a start".to_owned()
            }
            Self::RuntimeBuildFailed { cause } => {
                format!("the tokio runtime could not be built: {cause}")
            }
            Self::LogForwarderStartFailed { cause } => {
                format!("the log forwarder thread could not start: {cause}")
            }
        }
    }
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

impl CallRefusal {
    /// The thrown code of the refusal. The call did not start, so nothing
    /// changed. Pure.
    pub fn thrown(self) -> NativeThrown {
        match self {
            Self::ForkedAfterStart { core_pid, this_pid } => {
                NativeThrown::ForkedAfterCoreStart { core_pid, this_pid }
            }
            Self::Stopped(report) => NativeThrown::CoreStopped {
                subject: report.subject,
                cause: report.cause,
                state_after: StateAfter::NoChange,
            },
            Self::StartFailed(failure) => NativeThrown::CoreStartFailed {
                cause: failure.cause(),
            },
        }
    }
}

/// The state of the core in this process. JavaScript reads it as
/// `CoreState` (`coreState()`).
#[napi(string_enum = "snake_case", js_name = "CoreState")]
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
    log: &'static LogBridge,
}

static CORE: OnceLock<Core> = OnceLock::new();
static START_LOCK: Mutex<()> = Mutex::new(());

impl Core {
    /// The handle of the core runtime.
    pub fn runtime_handle(&self) -> &tokio::runtime::Handle {
        self.runtime.handle()
    }

    /// The log bridge of this process.
    pub fn log(&self) -> &'static LogBridge {
        self.log
    }

    /// Stop the core in this process: keep the first report, wake every call
    /// in flight, and refuse every later call. The report goes to stderr at
    /// once.
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
/// refuses a forked or stopped process.
pub fn core_for_call() -> Result<&'static Core, CallRefusal> {
    let core = match CORE.get() {
        Some(core) => core,
        None => start_core()?,
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
    let _rule_in_force = fix_panic_rule();
    let log = started_log().map_err(|failure| {
        CallRefusal::StartFailed(StartFailure::LogForwarderStartFailed {
            cause: failure.cause,
        })
    })?;
    let runtime = start_runtime(log.dispatch().clone()).map_err(CallRefusal::StartFailed)?;
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

/// Shape the core's runtime of the process and start it. A runtime that runs
/// already (the child host shaped it, or an earlier start failed after it)
/// keeps its shape.
fn start_runtime(dispatch: tracing::Dispatch) -> Result<&'static ProcessRuntime, StartFailure> {
    let shape = RuntimeShape {
        worker_threads: RUNTIME_WORKER_THREADS,
        max_blocking_threads: RUNTIME_MAX_BLOCKING_THREADS,
        thread_name: RUNTIME_THREAD_NAME,
        on_thread_start: ThreadStart::Run(Arc::new(move || {
            make_thread_default(&dispatch);
        })),
    };
    match shape_process_runtime(shape) {
        ShapeOutcome::Shaped | ShapeOutcome::AlreadyRunning => {}
    }
    process_runtime().map_err(|refusal| StartFailure::RuntimeBuildFailed {
        cause: refusal.to_string(),
    })
}

/// A core of its own for a test: the process runtime and the log bridge of
/// this process, and its own stop signal, not the core of the process. A
/// test can stop it without stopping the core of the test binary.
#[cfg(test)]
pub fn standalone_core_for_tests() -> &'static Core {
    let log = started_log().unwrap_or_else(|failure| panic!("the log bridge starts: {failure:?}"));
    let runtime =
        process_runtime().unwrap_or_else(|refusal| panic!("the process runtime starts: {refusal}"));
    let (stop_signal, _first_receiver) = watch::channel(false);
    Box::leak(Box::new(Core {
        pid: std::process::id(),
        runtime,
        first_panic: OnceLock::new(),
        stop_signal,
        log,
    }))
}
