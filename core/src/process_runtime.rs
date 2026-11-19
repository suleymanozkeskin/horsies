//! The tokio runtime of a process. The core starts it at its first use in
//! the process and checks the process id at each use.
//!
//! The pools and the listener open and drive their connections on this
//! runtime. A tokio-postgres connection lives only while the task that
//! drives it lives, so a connection opened on the runtime of a caller would
//! end with that runtime. On the process runtime, the connections of an app
//! serve every runtime and every thread of the process: a second
//! `#[tokio::main]`, a test runtime, a blocking call.
//!
//! - An entry point may set the shape of the runtime before its first use
//!   ([`shape_process_runtime`]): the Python bridge (2 workers, its log
//!   dispatch on each thread), the Rust child host (the threads of its child
//!   kind). Without one, the runtime has the producer shape.
//! - A use in a process whose id differs from the stamp is refused with
//!   [`ProcessRuntimeRefusal::ForkedAfterStart`]: the process was forked
//!   without `exec` after the runtime started, and tokio does not support a
//!   runtime in such a child. The inherited runtime is never dropped: it
//!   lives in a static.

use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use tokio::runtime::{Handle, Runtime};
use tokio::task::JoinHandle;

/// Worker threads of the runtime of a producer process. Two gave the same
/// single-call latency as eight in the bridge measurement, and an idle
/// producer keeps one OS thread per worker.
pub const PRODUCER_WORKER_THREADS: NonZeroUsize = match NonZeroUsize::new(2) {
    Some(threads) => threads,
    None => panic!("2 is not zero"),
};

/// Upper bound of the blocking pool of a producer runtime. The driver uses
/// it for host name lookups.
pub const PRODUCER_MAX_BLOCKING_THREADS: NonZeroUsize = match NonZeroUsize::new(16) {
    Some(threads) => threads,
    None => panic!("16 is not zero"),
};

/// Name of the threads of a producer runtime.
pub const PRODUCER_THREAD_NAME: &str = "horsies-core";

/// What each thread of the runtime runs when it starts.
#[derive(Clone)]
pub enum ThreadStart {
    Nothing,
    /// For example the bridge's log dispatch, so that the core's `tracing`
    /// events of every runtime thread reach Python `logging`.
    Run(Arc<dyn Fn() + Send + Sync>),
}

/// How the runtime of the process is built.
#[derive(Clone)]
pub struct RuntimeShape {
    pub worker_threads: NonZeroUsize,
    pub max_blocking_threads: NonZeroUsize,
    pub thread_name: &'static str,
    pub on_thread_start: ThreadStart,
}

impl RuntimeShape {
    /// The shape of a producer process, the default.
    pub fn producer() -> Self {
        Self {
            worker_threads: PRODUCER_WORKER_THREADS,
            max_blocking_threads: PRODUCER_MAX_BLOCKING_THREADS,
            thread_name: PRODUCER_THREAD_NAME,
            on_thread_start: ThreadStart::Nothing,
        }
    }
}

/// How [`shape_process_runtime`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShapeOutcome {
    /// The runtime will start with this shape.
    Shaped,
    /// The runtime runs already, with the shape it started with; the new
    /// shape is not used.
    AlreadyRunning,
}

/// The OS error code of a failed start, when the failure was one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OsErrorCode {
    Code(i32),
    NotAnOsError,
}

/// Why the runtime of the process cannot be used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProcessRuntimeRefusal {
    /// The runtime started in process `core_pid`; this process is a fork of
    /// it without `exec`. It does not clear in this process.
    #[error(
        "the core runtime started in process {core_pid}, and this process ({this_pid}) is a fork \
         of it without exec; start the process with spawn or forkserver"
    )]
    ForkedAfterStart { core_pid: u32, this_pid: u32 },
    /// The OS refused the runtime's threads. A later use tries again.
    #[error("the core runtime could not start: {cause}")]
    StartFailed {
        cause: String,
        os_error: OsErrorCode,
    },
}

/// The started runtime of the process, stamped with the process id.
pub struct ProcessRuntime {
    pid: u32,
    runtime: Runtime,
}

impl ProcessRuntime {
    /// The id of the process that started the runtime.
    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn handle(&self) -> &Handle {
        self.runtime.handle()
    }

    /// The runtime, for an entry point that blocks on it (`block_on`).
    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }
}

/// The state of the runtime in this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessRuntimeStatus {
    NotStarted,
    Running,
    /// The runtime started in a parent process and this process is its fork.
    Inherited,
}

static RUNTIME: OnceLock<ProcessRuntime> = OnceLock::new();
/// The shape an entry point set; the start reads it.
static SHAPE: Mutex<Option<RuntimeShape>> = Mutex::new(None);
/// Held by a start and by a shape, so a shape cannot change during a start.
static START_LOCK: Mutex<()> = Mutex::new(());

/// Set the shape of the runtime before its first use. Starts nothing.
pub fn shape_process_runtime(shape: RuntimeShape) -> ShapeOutcome {
    // The locks guard no data a panic can break.
    let _start = START_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    if RUNTIME.get().is_some() {
        return ShapeOutcome::AlreadyRunning;
    }
    *SHAPE.lock().unwrap_or_else(PoisonError::into_inner) = Some(shape);
    ShapeOutcome::Shaped
}

/// The runtime of this process. Starts it at the first use, with the shape
/// an entry point set or the producer shape.
///
/// Errors: [`ProcessRuntimeRefusal::ForkedAfterStart`] in a fork of the
/// process that started it; [`ProcessRuntimeRefusal::StartFailed`] when the
/// OS refused its threads (a later use tries again).
pub fn process_runtime() -> Result<&'static ProcessRuntime, ProcessRuntimeRefusal> {
    let runtime = match RUNTIME.get() {
        Some(runtime) => runtime,
        None => start()?,
    };
    let this_pid = std::process::id();
    match runtime.pid == this_pid {
        true => Ok(runtime),
        false => Err(ProcessRuntimeRefusal::ForkedAfterStart {
            core_pid: runtime.pid,
            this_pid,
        }),
    }
}

/// Spawn `future` on the runtime of this process.
///
/// Errors: the [`ProcessRuntimeRefusal`] of [`process_runtime`]; nothing
/// was spawned then.
pub fn spawn_on_process_runtime<F>(
    future: F,
) -> Result<JoinHandle<F::Output>, ProcessRuntimeRefusal>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    Ok(process_runtime()?.handle().spawn(future))
}

/// The state of the runtime in this process. Starts nothing.
pub fn process_runtime_status() -> ProcessRuntimeStatus {
    match RUNTIME.get() {
        None => ProcessRuntimeStatus::NotStarted,
        Some(runtime) => match runtime.pid == std::process::id() {
            true => ProcessRuntimeStatus::Running,
            false => ProcessRuntimeStatus::Inherited,
        },
    }
}

fn start() -> Result<&'static ProcessRuntime, ProcessRuntimeRefusal> {
    let _start = START_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime);
    }
    let shape = SHAPE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
        .unwrap_or_else(RuntimeShape::producer);
    let runtime = build(&shape)?;
    // The start lock is held and the cell is empty, so this stores `runtime`.
    Ok(RUNTIME.get_or_init(|| ProcessRuntime {
        pid: std::process::id(),
        runtime,
    }))
}

fn build(shape: &RuntimeShape) -> Result<Runtime, ProcessRuntimeRefusal> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    let _configured = builder
        .worker_threads(shape.worker_threads.get())
        .max_blocking_threads(shape.max_blocking_threads.get())
        .thread_name(shape.thread_name)
        .enable_all();
    match &shape.on_thread_start {
        ThreadStart::Nothing => {}
        ThreadStart::Run(on_start) => {
            let on_start = Arc::clone(on_start);
            let _configured = builder.on_thread_start(move || on_start());
        }
    }
    builder
        .build()
        .map_err(|error| ProcessRuntimeRefusal::StartFailed {
            cause: error.to_string(),
            os_error: match error.raw_os_error() {
                Some(code) => OsErrorCode::Code(code),
                None => OsErrorCode::NotAnOsError,
            },
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The runtime is the process's: a use starts it once, and every later
    /// use gives the same one. The tests of the core share it.
    #[test]
    fn every_use_gives_the_one_runtime_of_the_process() {
        let first = process_runtime().expect("the runtime starts");
        let second = process_runtime().expect("the runtime runs");
        assert!(std::ptr::eq(first, second));
        assert_eq!(first.pid(), std::process::id());
        assert_eq!(process_runtime_status(), ProcessRuntimeStatus::Running);
        assert_eq!(
            shape_process_runtime(RuntimeShape::producer()),
            ShapeOutcome::AlreadyRunning
        );
    }

    /// #1403: a connection-like task on the process runtime outlives the
    /// runtime of the caller that started it, and serves a second runtime
    /// and a plain thread.
    #[test]
    fn work_on_the_process_runtime_outlives_the_runtime_of_its_caller() {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<u32>();
        let (answers, mut answered) = tokio::sync::mpsc::unbounded_channel::<u32>();
        let first = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a caller runtime starts");
        #[expect(
            clippy::async_yields_async,
            reason = "the block hands out the handle of the driver; the test keeps it, not its end"
        )]
        let _driver = first.block_on(async {
            spawn_on_process_runtime(async move {
                while let Some(value) = receiver.recv().await {
                    let _sent = answers.send(value + 1);
                }
            })
            .expect("the process runtime runs")
        });
        drop(first);

        let second = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a second caller runtime starts");
        sender.send(1).expect("the driver still runs");
        assert_eq!(second.block_on(answered.recv()), Some(2));

        let thread_sender = sender.clone();
        std::thread::spawn(move || thread_sender.send(41).expect("the driver still runs"))
            .join()
            .expect("the thread ends");
        assert_eq!(answered.blocking_recv(), Some(42));
    }

    #[test]
    fn a_refusal_names_both_processes() {
        let refusal = ProcessRuntimeRefusal::ForkedAfterStart {
            core_pid: 10,
            this_pid: 11,
        };
        let text = refusal.to_string();
        assert!(
            text.contains("process 10") && text.contains("(11)"),
            "{text}"
        );
    }
}
