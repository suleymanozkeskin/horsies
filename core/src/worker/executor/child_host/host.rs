//! The entry of child mode: boot, report ready, serve, and name the exit
//! code (Python `core/worker/child_main.py`).

use std::future::Future;
use std::io;
use std::num::{NonZeroU32, NonZeroUsize};
use std::os::fd::{FromRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::pin::Pin;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::Arc;
use std::time::Duration;

use super::args::{ChildArgs, CHILD_MODE_ARGUMENT};
use super::body_scope::BodyScope;
use super::event_loop::{EventLoopWatch, HostLoopWatched};
use super::handler_host::boot_and_serve_handlers;
use super::handlers::HandlerChildApp;
use super::link::{read_frame, start_control_thread, write_frame, FrameWriter, ReadEnd};
use super::panic::{install_panic_hook, CatchPanic};
use super::producer::{publish_child_producer, task_send_manifest, task_send_pool_of};
use super::role::mark_process_runs_task_bodies;
use super::rss::read_rss;
use super::run::HostContext;
use super::seats::{serve_isolated, serve_shared_async, serve_shared_thread, PollWatch, ServeEnd};
use crate::broker::client_family::ClientFamily;
use crate::broker::PostgresBroker;
use crate::core::config::PostgresConfig;
use crate::core::TaskRegistry;
use crate::db::KeptOpen;
use crate::manifest::{PoolRecycle, ProducerManifest};
use crate::process_runtime::{
    process_runtime, shape_process_runtime, OsErrorCode, ProcessRuntimeRefusal, RuntimeShape,
    ShapeOutcome, ThreadStart,
};
use crate::worker::executor::protocol::{
    decode_parent_frame, BootDetail, BootErrorName, BootFailedFrame, ChildConnection,
    ChildExitCode, ChildFrame, ChildKind, ChildLogLevel, ChildPool, ChildProgram, FrameReader,
    InitFrame, IsolatedChild, ParentFrame, ParentFrameRefusal, ProcessId, ReadyFrame,
    ReportedBootCause, ServerSessions, SharedAsyncChild, SharedThreadChild, ShortageErrno,
};
use crate::worker::executor::supervisor::{ChildCommand, MailboxBound};

/// The longest wait for the init frame (Python `INIT_FRAME_TIMEOUT_S`).
const INIT_FRAME_TIMEOUT: Duration = Duration::from_secs(30);
/// Runtime threads of an isolated or a shared thread child: the start and
/// marker writes and one async body at a time.
const SMALL_RUNTIME_THREADS: usize = 2;
const RUNTIME_THREAD_NAME: &str = "horsies-child-runtime";
/// The most threads of the blocking pool of a child runtime (tokio's
/// default).
const CHILD_MAX_BLOCKING_THREADS: NonZeroUsize = match NonZeroUsize::new(512) {
    Some(threads) => threads,
    None => panic!("512 is not zero"),
};
/// The checkout wait of the child's pool, in seconds (Python
/// `CHILD_POOL_CONNECTION_WAIT_S`).
pub(super) const CHILD_POOL_CHECKOUT_WAIT_SECONDS: u32 = 30;
/// The age at which the child's pool replaces a connection (Python
/// `CHILD_POOL_MAX_LIFETIME_S`).
pub(super) const CHILD_POOL_MAX_AGE: PoolRecycle = PoolRecycle::after_seconds(300);
/// The longest run of each start hook (Python `CHILD_HOOK_TIMEOUT_SECONDS`).
pub const CHILD_START_HOOK_BOUND: Duration = Duration::from_secs(10);

/// What a worker binary gives its child mode: its tasks and the settings a
/// child reads from the app.
pub struct ChildApp {
    pub registry: Arc<TaskRegistry>,
    /// The host name written with each start.
    pub hostname: String,
    /// The start hooks that the child runs before it reports ready.
    pub start_hook: ChildStartHook,
    /// The app's producer manifest. Task code in the child sends through a
    /// handle of it whose pool is the `task_send_pool` of the child's kind
    /// ([`super::child_producer`]).
    pub producer_manifest: ProducerManifest,
    /// What the child does with the log level of its init frame.
    pub log_setup: ChildLogSetup,
    /// The client family of the binary: the child's broker writes as it and
    /// checks it before its first write.
    pub family: ClientFamily,
    /// What the child does around each task body: nothing, or the span that
    /// the client opens for the run ([`super::BodySpans`]).
    pub body_scope: BodyScope,
    /// What a handler child runs: the app's event handlers and the start
    /// hooks of the handler child. A task child reads neither.
    pub handler_child: HandlerChildApp,
    /// The loops whose block a task child reports: the Rust runtime only,
    /// or also the host program's own event loop. A shared async child
    /// watches the host loop next to its Rust polls; a shared thread child
    /// watches it for its main thread; an isolated child and a handler
    /// child watch no loop.
    pub event_loop: EventLoopWatch,
}

/// What a child does with the log level of its init frame (the worker's
/// `--loglevel`), before its boot. Records before the init frame (a bad
/// socket, a frame that is not an init frame) reach only a subscriber that
/// the binary installed before child mode.
pub enum ChildLogSetup {
    /// The binary keeps its own log setup, or none.
    NoSetup,
    /// Called once with the level, before the boot: the binary installs its
    /// subscriber at that level. It must not fail: a setup that cannot
    /// install keeps the logs it has.
    Install(Box<dyn FnOnce(ChildLogLevel) + Send + Sync>),
}

/// Why a start hook failed, as the hook names it. The child logs it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{cause}")]
pub struct ChildHookFailure {
    pub cause: String,
}

/// The future of a start hook.
pub type ChildStartFuture = Pin<Box<dyn Future<Output = Result<(), ChildHookFailure>> + Send>>;

/// The start hooks of a child (Python `on_child_process_start`,
/// `on_shared_async_child_start`). They run once in every child, a
/// replacement child included, on the child's runtime: after the child
/// connected its pool, before it reports ready, one after the other in
/// registration order, each for at most [`CHILD_START_HOOK_BOUND`]. A hook
/// that returns an error, panics or runs longer ends the child with exit
/// code 173 and runs no later hook, and the worker stops with
/// `ChildHookFailed`. The logged cause names the hook. Work that a hook
/// starts and leaves running is not the work of any task. `Run` with an
/// empty list runs nothing, as `NoHook`.
pub enum ChildStartHook {
    NoHook,
    Run(Vec<NamedChildStartHook>),
}

/// One start hook of a child and the name that its failure cause gives
/// (Python logs the hook's `__qualname__`). The hook is `Sync` because the
/// child shares its [`ChildApp`] between threads; the child takes it out
/// and calls it once.
pub struct NamedChildStartHook {
    pub name: String,
    pub start: Box<dyn FnOnce() -> ChildStartFuture + Send + Sync>,
}

/// How a child ends. The binary exits with [`Self::code`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildHostExit {
    /// The stop frame came: exit 0.
    Stopped,
    ParentGone,
    BadParentFrame,
    BadArguments,
    /// After a `boot_failed` frame that names the cause.
    BootFailed,
    ProtocolMismatch,
    /// The start hook failed, panicked or ran past its bound: exit 173.
    ChildHookFailed,
}

impl ChildHostExit {
    pub fn code(self) -> i32 {
        match self {
            Self::Stopped => 0,
            Self::ParentGone => ChildExitCode::ParentGone.code(),
            Self::BadParentFrame => ChildExitCode::BadParentFrame.code(),
            Self::BadArguments => ChildExitCode::BadArguments.code(),
            Self::BootFailed => ChildExitCode::BootFailed.code(),
            Self::ProtocolMismatch => ChildExitCode::ProtocolMismatch.code(),
            Self::ChildHookFailed => ChildExitCode::ChildHookFailed.code(),
        }
    }
}

/// The command that starts this binary again in child mode. The spawner
/// appends the socket descriptor and the worker pid.
pub fn child_command() -> io::Result<ChildCommand> {
    Ok(ChildCommand {
        program: std::env::current_exe()?,
        args: vec![CHILD_MODE_ARGUMENT.into()],
    })
}

/// Serve as a Rust child until the stop frame or the end of the parent.
/// Code that the binary runs before this call runs in each child too, as a
/// Python child imports the app again. The init frame names the kind: a
/// task child runs task bodies; a handler child runs the app's event
/// handlers (`handler_host`).
pub fn run_child_host(app: ChildApp, args: ChildArgs) -> ChildHostExit {
    install_panic_hook();
    if let Err(exit) = watch_parent(args.worker_pid) {
        return exit;
    }
    let socket = match open_socket(args.socket_fd) {
        Ok(socket) => socket,
        Err(error) => {
            tracing::error!(
                fd = args.socket_fd,
                error = %error,
                "the child socket descriptor is not open: {error}",
            );
            return ChildHostExit::BadArguments;
        }
    };
    let mut reader = FrameReader::new();
    let init = match read_init(&socket, &mut reader) {
        Ok(init) => init,
        Err(exit) => return exit,
    };
    boot_and_serve(app, &socket, reader, &init)
}

/// Linux: die with the worker, and exit at once when it is gone already.
/// Other platforms: exit when the worker is not the parent; the control
/// thread sees the worker's end at the EOF of the socket.
fn watch_parent(worker_pid: ProcessId) -> Result<(), ChildHostExit> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: prctl with integer arguments; the signal is passed as the
        // unsigned long the kernel reads.
        let set = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL as libc::c_ulong) };
        if set == -1 {
            let error = io::Error::last_os_error();
            tracing::error!(%error, "the parent-death signal could not be set: {error}");
            return Err(ChildHostExit::ParentGone);
        }
    }
    // SAFETY: getppid takes no argument.
    match unsafe { libc::getppid() } == worker_pid.as_pid_t() {
        true => Ok(()),
        false => Err(ChildHostExit::ParentGone),
    }
}

/// The child end of the socket, close-on-exec so that subprocesses of task
/// code do not hold it open.
fn open_socket(fd: RawFd) -> io::Result<UnixStream> {
    // SAFETY: fcntl on a descriptor number with integer arguments.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: as above.
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the supervisor passed this descriptor to this process as its
    // socket, and nothing else in the process owns it.
    Ok(unsafe { UnixStream::from_raw_fd(fd) })
}

/// Read the first frame, bounded: it must be the init frame of this
/// protocol version, for a Rust child of any kind.
fn read_init(socket: &UnixStream, reader: &mut FrameReader) -> Result<InitFrame, ChildHostExit> {
    socket
        .set_read_timeout(Some(INIT_FRAME_TIMEOUT))
        .map_err(|_| ChildHostExit::ParentGone)?;
    let mut stream = socket;
    let raw = match read_frame(&mut stream, reader) {
        Ok(raw) => raw,
        Err(ReadEnd::Eof | ReadEnd::Failed) => return Err(ChildHostExit::ParentGone),
        Err(ReadEnd::OverBound(over)) => {
            tracing::error!(error = %over, "Parent sent a frame over the bounds: {over}");
            return Err(ChildHostExit::BadParentFrame);
        }
    };
    socket
        .set_read_timeout(None)
        .map_err(|_| ChildHostExit::ParentGone)?;
    let init = match decode_parent_frame(&raw.meta, raw.body) {
        Ok(ParentFrame::Init(init)) => init,
        Ok(
            ParentFrame::Run(_)
            | ParentFrame::SharedRun(_)
            | ParentFrame::CancelRun(_)
            | ParentFrame::Events(_)
            | ParentFrame::Stop,
        ) => {
            tracing::error!("the first parent frame was not an init frame");
            return Err(ChildHostExit::BadParentFrame);
        }
        Err(ParentFrameRefusal::ProtocolMismatch(mismatch)) => {
            tracing::error!(
                error = %mismatch,
                "the worker speaks another protocol version: {mismatch}",
            );
            return Err(ChildHostExit::ProtocolMismatch);
        }
        Err(ParentFrameRefusal::Malformed(malformed)) => {
            tracing::error!(error = %malformed, "Parent sent a bad frame: {malformed}");
            return Err(ChildHostExit::BadParentFrame);
        }
    };
    match init.program {
        ChildProgram::Rust => Ok(init),
        ChildProgram::Python(_) => {
            tracing::error!("a Rust child got the init frame of a Python child");
            Err(ChildHostExit::BadParentFrame)
        }
    }
}

/// A child kind that runs task bodies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskChild {
    Isolated(IsolatedChild),
    SharedAsync(SharedAsyncChild),
    SharedThread(SharedThreadChild),
}

/// What the init frame asks this host to boot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostedChild {
    /// A child that runs task bodies.
    Task(TaskChild),
    /// A child that runs the app's event handlers and no task.
    Handler,
}

/// The child that `kind` boots. Pure.
fn hosted_child(kind: ChildKind) -> HostedChild {
    match kind {
        ChildKind::Isolated(child) => HostedChild::Task(TaskChild::Isolated(child)),
        ChildKind::SharedAsync(child) => HostedChild::Task(TaskChild::SharedAsync(child)),
        ChildKind::SharedThread(child) => HostedChild::Task(TaskChild::SharedThread(child)),
        ChildKind::Handler(_) => HostedChild::Handler,
    }
}

/// The cause of an `io::Error` of a boot step: a resource shortage for a
/// clearable errno, else `step_cause`.
pub(super) fn shortage_or(error: &io::Error, step_cause: ReportedBootCause) -> ReportedBootCause {
    let os_error = match error.raw_os_error() {
        Some(code) => OsErrorCode::Code(code),
        None => OsErrorCode::NotAnOsError,
    };
    shortage_of(os_error, step_cause)
}

/// The cause of a boot step that failed with `os_error`: a resource
/// shortage for a clearable errno, else `step_cause`.
fn shortage_of(os_error: OsErrorCode, step_cause: ReportedBootCause) -> ReportedBootCause {
    match os_error {
        OsErrorCode::Code(libc::EMFILE) => {
            ReportedBootCause::ResourceShortage(ShortageErrno::Emfile)
        }
        OsErrorCode::Code(libc::ENFILE) => {
            ReportedBootCause::ResourceShortage(ShortageErrno::Enfile)
        }
        OsErrorCode::Code(libc::ENOMEM) => {
            ReportedBootCause::ResourceShortage(ShortageErrno::Enomem)
        }
        OsErrorCode::Code(libc::EAGAIN) => {
            ReportedBootCause::ResourceShortage(ShortageErrno::Eagain)
        }
        OsErrorCode::Code(_) | OsErrorCode::NotAnOsError => step_cause,
    }
}

/// The runtime of the process, shaped for a child with `worker_threads`
/// runtime threads: the child's async bodies, its writes and its
/// connections run on it, so the child starts one runtime. A runtime that
/// ran before child mode (code that `main` ran first) is used with the
/// shape it started with.
///
/// Errors: the boot cause and the detail of a runtime that cannot start.
pub(super) fn start_child_runtime(
    worker_threads: NonZeroUsize,
) -> Result<&'static tokio::runtime::Runtime, (ReportedBootCause, String)> {
    let shape = RuntimeShape {
        worker_threads,
        max_blocking_threads: CHILD_MAX_BLOCKING_THREADS,
        thread_name: RUNTIME_THREAD_NAME,
        on_thread_start: ThreadStart::Nothing,
    };
    match shape_process_runtime(shape) {
        ShapeOutcome::Shaped => {}
        ShapeOutcome::AlreadyRunning => tracing::warn!(
            "the process runtime ran before child mode started; the child runs on it with the \
             shape it started with",
        ),
    }
    match process_runtime() {
        Ok(process) => Ok(process.runtime()),
        Err(ProcessRuntimeRefusal::StartFailed { cause, os_error }) => Err((
            shortage_of(os_error, ReportedBootCause::ThreadStartRefused),
            cause,
        )),
        // A child starts by exec, so it inherits no runtime; a refusal names
        // the defect in the boot report.
        Err(refusal @ ProcessRuntimeRefusal::ForkedAfterStart { .. }) => {
            Err((ReportedBootCause::ThreadStartRefused, refusal.to_string()))
        }
    }
}

/// Report a failed boot on the socket and end with the boot-failed code.
pub(super) fn boot_failed(
    socket: &UnixStream,
    cause: ReportedBootCause,
    error_name: &str,
    detail: &str,
) -> ChildHostExit {
    let frame = ChildFrame::BootFailed(BootFailedFrame {
        cause,
        error_name: BootErrorName::cut(error_name),
        detail: BootDetail::cut(detail),
    });
    match write_frame(socket, &frame) {
        Ok(()) => ChildHostExit::BootFailed,
        Err(_) => ChildHostExit::ParentGone,
    }
}

/// The pool of the own writes of a task child: the start, the marker and
/// the release of each run. A handler child writes nothing itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OwnWritesPool {
    Pool(ChildPool),
    /// A handler child: it runs no task.
    NoOwnWrites,
}

pub(super) fn pool_of(kind: ChildKind) -> OwnWritesPool {
    match kind {
        ChildKind::Isolated(child) => OwnWritesPool::Pool(child.pool),
        ChildKind::SharedAsync(child) => OwnWritesPool::Pool(child.pool),
        ChildKind::SharedThread(child) => OwnWritesPool::Pool(child.pool),
        ChildKind::Handler(_) => OwnWritesPool::NoOwnWrites,
    }
}

/// The pool of the own writes of a task child.
fn task_pool_of(kind: TaskChild) -> ChildPool {
    match kind {
        TaskChild::Isolated(child) => child.pool,
        TaskChild::SharedAsync(child) => child.pool,
        TaskChild::SharedThread(child) => child.pool,
    }
}

/// The seats of a task child: one for an isolated child.
fn seats_of(kind: TaskChild) -> usize {
    let seats = match kind {
        TaskChild::Isolated(_) => 1,
        TaskChild::SharedAsync(child) => child.seats.get(),
        TaskChild::SharedThread(child) => child.seats.get(),
    };
    usize::try_from(seats).unwrap_or(usize::MAX).max(1)
}

/// The runtime threads of a task child.
fn runtime_threads(kind: TaskChild) -> NonZeroUsize {
    let threads = match kind {
        TaskChild::Isolated(_) | TaskChild::SharedThread(_) => SMALL_RUNTIME_THREADS,
        TaskChild::SharedAsync(child) => {
            usize::try_from(child.executor_threads.get()).unwrap_or(SMALL_RUNTIME_THREADS)
        }
    };
    NonZeroUsize::new(threads).unwrap_or(NonZeroUsize::MIN)
}

/// The broker config of a child: the URL of the init frame with the
/// connection settings of the parent's config, a pool of up to `max_size`
/// connections of the child kind with no overflow, `check` as the pre-ping
/// of a session pool, and Python's checkout wait and connection age of a
/// child pool. The `min_size` and the `check` of the main pool are its pool
/// profile ([`kept_open_of`], [`PostgresBroker::connect_child`]). Pure.
///
/// Python's child pool takes the same settings as psycopg connect kwargs
/// (`clients/python/horsies/core/worker/child_pool.py:123-132`, the `kwargs` of
/// `PostgresConfig.pooled_connect_args`). In PgBouncer transaction mode the
/// config also names the parent's session URL, which
/// [`PostgresConfig::validate`] requires in that mode.
fn child_broker_config(
    database_url: &str,
    connection: &ChildConnection,
    pool: ChildPool,
) -> PostgresConfig {
    let connections = u32::try_from(pool.max_size.get())
        .ok()
        .and_then(NonZeroU32::new)
        .expect("a child pool max_size is from 1 to MAX_CHILD_POOL_CONNECTIONS");
    let (pgbouncer_transaction_mode, session_database_url) = match &connection.server_sessions {
        ServerSessions::Kept => (false, None),
        ServerSessions::PgBouncerTransactionMode {
            session_database_url,
        } => (true, Some(session_database_url.as_str().to_owned())),
    };
    PostgresConfig {
        session_database_url,
        pgbouncer_transaction_mode,
        pool_pre_ping: pool.check,
        pool_timeout: CHILD_POOL_CHECKOUT_WAIT_SECONDS,
        pool_recycle: CHILD_POOL_MAX_AGE,
        tcp_keepalives: connection.tcp_keepalives,
        tcp_user_timeout: connection.tcp_user_timeout,
        ..PostgresConfig::from_url(database_url).with_exact_pool(connections)
    }
}

/// The connections that a child pool keeps open: its `min_size` (Python
/// `ConnectionPool(min_size=...)`, `clients/python/horsies/core/worker/child_pool.py:123-132`
/// and `:169-178`, opened at `:136` and `:179`). Pure.
pub(super) fn kept_open_of(pool: ChildPool) -> KeptOpen {
    let min_size = u32::try_from(pool.min_size.get())
        .expect("a child pool min_size is from 0 to MAX_CHILD_POOL_CONNECTIONS");
    match NonZeroU32::new(min_size) {
        None => KeptOpen::NoConnections,
        Some(kept) => KeptOpen::Connections(kept),
    }
}

/// The child's broker of `family` ([`PostgresBroker::connect_child`]): the
/// child applies no migration. Its main pool keeps `min_size` connections open and,
/// with `check`, pings every pooled connection at checkout (Python
/// `check=ConnectionPool.check_connection`,
/// `clients/python/horsies/core/worker/child_pool.py:122` and `:168`). Errors: the text of a
/// refused config or of a failed connect; a `min_size` above `max_size` is a
/// refused config, as Python's `ValueError`.
async fn connect(
    init: &InitFrame,
    kind: TaskChild,
    family: ClientFamily,
) -> Result<PostgresBroker, String> {
    let pool = task_pool_of(kind);
    let config = child_broker_config(init.database_url.as_str(), &init.connection, pool);
    PostgresBroker::connect_child(&config, kept_open_of(pool), pool.check, family)
        .await
        .map_err(|error| error.to_string())
}

/// What the serve loop of a child kind needs beyond the shared context.
enum Serving {
    Isolated,
    SharedAsync { watch: PollWatch, seats: usize },
    SharedThread { seats: usize },
}

/// Whether the watch thread of a task child of `kind` reads the host loop
/// of `event_loop`: in a shared async child, and for the main thread of a
/// shared thread child. An isolated child has no watch: the parent holds
/// its deadline. Pure.
fn host_loop_watched(kind: TaskChild, event_loop: &EventLoopWatch) -> HostLoopWatched {
    match (kind, event_loop) {
        (_, EventLoopWatch::RuntimeOnly) | (TaskChild::Isolated(_), EventLoopWatch::Host(_)) => {
            HostLoopWatched::NotWatched
        }
        (TaskChild::SharedAsync(_) | TaskChild::SharedThread(_), EventLoopWatch::Host(host)) => {
            HostLoopWatched::Watched(Arc::clone(host))
        }
    }
}

/// Start the writer thread, the watch thread (the poll watch of a shared
/// async child; the host loop watch of a shared thread child whose app
/// gives a host loop), and the control thread.
fn start_threads(
    socket: &UnixStream,
    reader: FrameReader,
    kind: TaskChild,
    event_loop: &EventLoopWatch,
) -> io::Result<(
    FrameWriter,
    Serving,
    tokio::sync::mpsc::Receiver<ParentFrame>,
)> {
    let seats = seats_of(kind);
    let bound =
        MailboxBound::for_seats(NonZeroUsize::new(seats).unwrap_or(NonZeroUsize::MIN)).get();
    let writer = FrameWriter::start(socket.try_clone()?, bound)?;
    let serving = match (kind, host_loop_watched(kind, event_loop)) {
        (TaskChild::Isolated(_), _) => Serving::Isolated,
        (TaskChild::SharedAsync(_), host) => Serving::SharedAsync {
            watch: PollWatch::start(writer.clone(), host)?,
            seats,
        },
        (TaskChild::SharedThread(_), HostLoopWatched::NotWatched) => {
            Serving::SharedThread { seats }
        }
        (TaskChild::SharedThread(_), host @ HostLoopWatched::Watched(_)) => {
            // The watch of the main thread runs on its own thread; no run
            // registers polls on it.
            let _main_thread_watch = PollWatch::start(writer.clone(), host)?;
            Serving::SharedThread { seats }
        }
    };
    let frames = start_control_thread(socket.try_clone()?, reader, bound)?;
    Ok((writer, serving, frames))
}

/// What the worker does after a start hook of a child failed: the end of
/// the log line of the failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AfterHookFailure {
    /// A task child: the worker stops with `CHILD_HOOK_FAILED`.
    WorkerStops,
    /// The handler child: the worker keeps it down until it restarts, and
    /// runs its tasks.
    HandlerChildStaysDown,
}

impl AfterHookFailure {
    fn text(self) -> &'static str {
        match self {
            Self::WorkerStops => "the child exits and the worker stops",
            Self::HandlerChildStaysDown => {
                "the handler child exits and stays down until the worker restarts; the worker \
                 runs on"
            }
        }
    }
}

/// Run the start hooks on `runtime` in their order, each bounded by
/// `bound` alone (the child passes [`CHILD_START_HOOK_BOUND`]). The first
/// failure ends the run: no later hook runs. A failure is logged with the
/// hook's position, its name, the cause and `after`, and gives
/// [`ChildHostExit::ChildHookFailed`].
pub(super) fn run_start_hook(
    runtime: &tokio::runtime::Runtime,
    hook: ChildStartHook,
    bound: Duration,
    after: AfterHookFailure,
) -> Result<(), ChildHostExit> {
    let hooks = match hook {
        ChildStartHook::NoHook => return Ok(()),
        ChildStartHook::Run(hooks) => hooks,
    };
    let count = hooks.len();
    for (index, hook) in hooks.into_iter().enumerate() {
        let name = hook.name;
        if let Err(cause) = run_one_start_hook(runtime, hook.start, bound) {
            tracing::error!(
                pid = std::process::id(),
                "start hook {} of {count} '{name}' of the child failed: {cause}; {}",
                index + 1,
                after.text(),
            );
            return Err(ChildHostExit::ChildHookFailed);
        }
    }
    Ok(())
}

/// Why one start hook failed. The text is the cause that the child logs
/// after the hook's name.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
enum StartHookFailure {
    #[error("it returned an error: {0}")]
    Returned(ChildHookFailure),
    /// The panic text.
    #[error("it panicked: {0}")]
    Panicked(String),
    /// The bound that the hook ran past; the hook is left running.
    #[error("it ran longer than {} ms", .0.as_millis())]
    RanPastBound(Duration),
    /// The runtime dropped the hook's task before it gave an outcome.
    #[error("it ended with no outcome")]
    NoOutcome,
}

/// Run one start hook on `runtime`, bounded by `bound`. The bound is kept by
/// this thread, not by a runtime timer, so a hook that holds every runtime
/// thread still passes it. A hook that runs past the bound is left running:
/// the child exits.
///
/// Errors: [`StartHookFailure`].
fn run_one_start_hook(
    runtime: &tokio::runtime::Runtime,
    start: Box<dyn FnOnce() -> ChildStartFuture + Send + Sync>,
    bound: Duration,
) -> Result<(), StartHookFailure> {
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let _hook_task = runtime.spawn(async move {
        let outcome = CatchPanic::new(async move { start().await }).await;
        // The receiver is gone only after the bound passed.
        let _sent = sender.send(outcome);
    });
    match receiver.recv_timeout(bound) {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(failure))) => Err(StartHookFailure::Returned(failure)),
        Ok(Err(record)) => Err(StartHookFailure::Panicked(record.text)),
        Err(RecvTimeoutError::Timeout) => Err(StartHookFailure::RanPastBound(bound)),
        Err(RecvTimeoutError::Disconnected) => Err(StartHookFailure::NoOutcome),
    }
}

fn boot_and_serve(
    mut app: ChildApp,
    socket: &UnixStream,
    reader: FrameReader,
    init: &InitFrame,
) -> ChildHostExit {
    match std::mem::replace(&mut app.log_setup, ChildLogSetup::NoSetup) {
        ChildLogSetup::NoSetup => {}
        ChildLogSetup::Install(install) => install(init.log_level),
    }
    publish_child_producer(
        task_send_manifest(&app.producer_manifest, task_send_pool_of(init.child_kind)),
        app.family,
    );
    match hosted_child(init.child_kind) {
        HostedChild::Task(kind) => boot_and_serve_tasks(app, socket, reader, init, kind),
        HostedChild::Handler => boot_and_serve_handlers(app.handler_child, socket, reader),
    }
}

/// Boot a task child of `kind` and serve its runs until the stop frame or
/// the end of the parent.
fn boot_and_serve_tasks(
    mut app: ChildApp,
    socket: &UnixStream,
    reader: FrameReader,
    init: &InitFrame,
    kind: TaskChild,
) -> ChildHostExit {
    mark_process_runs_task_bodies();
    let runtime = match start_child_runtime(runtime_threads(kind)) {
        Ok(runtime) => runtime,
        Err((cause, detail)) => {
            return boot_failed(socket, cause, "RuntimeStartFailed", &detail);
        }
    };
    let broker = match runtime.block_on(connect(init, kind, app.family)) {
        Ok(broker) => Arc::new(broker),
        Err(detail) => {
            return boot_failed(
                socket,
                ReportedBootCause::ChildPoolSetupFailed,
                "DatabaseError",
                &detail,
            )
        }
    };
    let start_hook = std::mem::replace(&mut app.start_hook, ChildStartHook::NoHook);
    if let Err(exit) = run_start_hook(
        runtime,
        start_hook,
        CHILD_START_HOOK_BOUND,
        AfterHookFailure::WorkerStops,
    ) {
        return exit;
    }
    let (writer, serving, mut frames) = match start_threads(socket, reader, kind, &app.event_loop) {
        Ok(threads) => threads,
        Err(error) => {
            let cause = shortage_or(&error, ReportedBootCause::ThreadStartRefused);
            return boot_failed(socket, cause, "ThreadStartFailed", &error.to_string());
        }
    };
    let pid = std::process::id();
    let own_pid =
        ProcessId::new(pid).expect("the OS gives a process a pid from 1 to the largest pid_t");
    writer.send_blocking(ChildFrame::Ready(ReadyFrame {
        pid: own_pid,
        rss: read_rss(),
    }));
    let context = HostContext {
        broker,
        app: Arc::new(app),
        pid: own_pid.as_pid_t(),
        writer,
    };
    let end = runtime.block_on(async move {
        match serving {
            Serving::Isolated => serve_isolated(context, &mut frames).await,
            Serving::SharedAsync { watch, seats } => {
                serve_shared_async(context, watch, &mut frames, seats).await
            }
            Serving::SharedThread { seats } => {
                serve_shared_thread(context, &mut frames, seats).await
            }
        }
    });
    // The process runtime is never shut down: the binary exits with the
    // code, so work that still runs (a stuck body) does not hold the exit.
    match end {
        ServeEnd::Stopped => ChildHostExit::Stopped,
        ServeEnd::BadParentFrame => ChildHostExit::BadParentFrame,
        ServeEnd::ParentGone => ChildHostExit::ParentGone,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::socket::{
        KeepaliveProbes, KeepaliveSeconds, TcpKeepalivesConfig, TcpUserTimeoutConfig,
        TcpUserTimeoutMs,
    };
    use crate::db::{Keepalives, SocketSettings, TcpUserTimeout};
    use crate::worker::executor::protocol::{
        AsyncSeats, DatabaseUrl, ExecutorThreads, HandlerChild, PoolMaxSize, PoolMinSize,
        TaskSendPool, ThreadSeats,
    };

    const URL: &str = "postgresql://user:secret@localhost:5432/db";
    const SESSION_URL: &str = "postgresql://user:secret@direct:5432/db";

    fn child_pool(max_size: u64, check: bool) -> ChildPool {
        ChildPool {
            min_size: PoolMinSize::new(0).expect("0 is a pool min size"),
            max_size: PoolMaxSize::new(max_size).expect("the test max size is inside its bounds"),
            check,
        }
    }

    fn small_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("a test runtime starts")
    }

    fn hook<C, F>(make: C) -> ChildStartHook
    where
        C: FnOnce() -> F + Send + Sync + 'static,
        F: Future<Output = Result<(), ChildHookFailure>> + Send + 'static,
    {
        ChildStartHook::Run(vec![named("hook", make)])
    }

    fn named<C, F>(name: &str, make: C) -> NamedChildStartHook
    where
        C: FnOnce() -> F + Send + Sync + 'static,
        F: Future<Output = Result<(), ChildHookFailure>> + Send + 'static,
    {
        NamedChildStartHook {
            name: name.to_owned(),
            start: Box::new(move || -> ChildStartFuture { Box::pin(make()) }),
        }
    }

    /// A hook that records `name` in `calls`, sleeps `sleep` on the runtime
    /// and then returns `Ok`.
    fn recording(
        calls: &Arc<std::sync::Mutex<Vec<&'static str>>>,
        name: &'static str,
        sleep: Duration,
    ) -> NamedChildStartHook {
        let calls = Arc::clone(calls);
        named(name, move || async move {
            calls.lock().expect("the test lock").push(name);
            tokio::time::sleep(sleep).await;
            Ok(())
        })
    }

    const SHORT_BOUND: Duration = Duration::from_millis(200);

    /// Python `test_child_process_hooks.py`: a hook that returns, and no
    /// hook, let the child report ready.
    #[test]
    fn a_hook_that_returns_lets_the_child_start() {
        let runtime = small_runtime();
        assert_eq!(
            run_start_hook(
                &runtime,
                ChildStartHook::NoHook,
                SHORT_BOUND,
                AfterHookFailure::WorkerStops
            ),
            Ok(())
        );
        assert_eq!(
            run_start_hook(
                &runtime,
                hook(|| async { Ok(()) }),
                SHORT_BOUND,
                AfterHookFailure::WorkerStops
            ),
            Ok(())
        );
        runtime.shutdown_background();
    }

    /// #1402: a hook that returns an error, one that panics, and one that
    /// runs past its bound each end the child with 173.
    #[test]
    fn a_hook_that_fails_panics_or_hangs_ends_the_child_with_173() {
        let runtime = small_runtime();
        let failing = hook(|| async {
            Err(ChildHookFailure {
                cause: "the cache did not open".to_owned(),
            })
        });
        let panicking = hook(|| async {
            if std::hint::black_box(true) {
                panic!("the hook panicked");
            }
            Ok(())
        });
        let hanging = hook(|| async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok(())
        });
        for start in [failing, panicking, hanging] {
            let ended = run_start_hook(&runtime, start, SHORT_BOUND, AfterHookFailure::WorkerStops);
            assert_eq!(ended, Err(ChildHostExit::ChildHookFailed));
            assert_eq!(ChildHostExit::ChildHookFailed.code(), 173);
        }
        runtime.shutdown_background();
    }

    /// #656, Python `on_shared_async_child_start`: each hook has the bound
    /// alone. Two hooks that each run for most of the bound pass, though
    /// together they run longer than it; they run in registration order.
    #[test]
    fn each_hook_has_the_bound_alone_and_the_hooks_run_in_order() {
        let runtime = small_runtime();
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let bound = Duration::from_millis(1_000);
        let most_of_the_bound = Duration::from_millis(600);
        let hooks = ChildStartHook::Run(vec![
            recording(&calls, "first", most_of_the_bound),
            recording(&calls, "second", most_of_the_bound),
        ]);
        let started = std::time::Instant::now();
        assert_eq!(
            run_start_hook(&runtime, hooks, bound, AfterHookFailure::WorkerStops),
            Ok(())
        );
        assert!(started.elapsed() > bound);
        assert_eq!(
            *calls.lock().expect("the test lock"),
            vec!["first", "second"]
        );
        runtime.shutdown_background();
    }

    /// Python `test_failing_hook_exits_173_and_skips_rest`: the first hook
    /// that fails ends the start; no later hook runs.
    #[test]
    fn the_first_failing_hook_ends_the_start_and_skips_the_rest() {
        let runtime = small_runtime();
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let failing = named("second", || async {
            Err(ChildHookFailure {
                cause: "the cache did not open".to_owned(),
            })
        });
        let hooks = ChildStartHook::Run(vec![
            recording(&calls, "first", Duration::ZERO),
            failing,
            recording(&calls, "third", Duration::ZERO),
        ]);
        assert_eq!(
            run_start_hook(&runtime, hooks, SHORT_BOUND, AfterHookFailure::WorkerStops),
            Err(ChildHostExit::ChildHookFailed)
        );
        assert_eq!(*calls.lock().expect("the test lock"), vec!["first"]);
        runtime.shutdown_background();
    }

    /// An empty list runs nothing, as `NoHook`.
    #[test]
    fn an_empty_hook_list_lets_the_child_start() {
        let runtime = small_runtime();
        assert_eq!(
            run_start_hook(
                &runtime,
                ChildStartHook::Run(Vec::new()),
                SHORT_BOUND,
                AfterHookFailure::WorkerStops
            ),
            Ok(())
        );
        runtime.shutdown_background();
    }

    /// The cause of each failure kind, which the log line gives after the
    /// hook's name.
    #[test]
    fn the_cause_names_the_failure() {
        let runtime = small_runtime();
        let failing = named("failing", || async {
            Err(ChildHookFailure {
                cause: "the cache did not open".to_owned(),
            })
        });
        let returned = run_one_start_hook(&runtime, failing.start, SHORT_BOUND).unwrap_err();
        assert_eq!(
            returned.to_string(),
            "it returned an error: the cache did not open"
        );
        let panicking = named("panicking", || async {
            if std::hint::black_box(true) {
                panic!("the hook panicked");
            }
            Ok(())
        });
        let panicked = run_one_start_hook(&runtime, panicking.start, SHORT_BOUND).unwrap_err();
        assert!(
            matches!(&panicked, StartHookFailure::Panicked(text) if text.contains("the hook panicked")),
            "{panicked:?}"
        );
        let hanging = named("hanging", || async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok(())
        });
        let ran_past = run_one_start_hook(&runtime, hanging.start, SHORT_BOUND).unwrap_err();
        assert_eq!(ran_past, StartHookFailure::RanPastBound(SHORT_BOUND));
        assert_eq!(ran_past.to_string(), "it ran longer than 200 ms");
        runtime.shutdown_background();
    }

    /// The bound holds when the hook holds the only runtime thread: the
    /// child's own thread keeps it, not a runtime timer.
    #[test]
    fn a_hook_that_holds_the_runtime_thread_still_passes_its_bound() {
        let runtime = small_runtime();
        let started = std::time::Instant::now();
        let holding = hook(|| async {
            std::thread::sleep(Duration::from_secs(5));
            Ok(())
        });
        assert_eq!(
            run_start_hook(
                &runtime,
                holding,
                SHORT_BOUND,
                AfterHookFailure::WorkerStops
            ),
            Err(ChildHostExit::ChildHookFailed)
        );
        assert!(started.elapsed() < Duration::from_secs(4));
        runtime.shutdown_background();
    }

    /// The failure line of a start hook names what the worker does after
    /// it: a failed task child stops the worker; a failed handler child
    /// stays down while the worker runs on.
    #[test]
    fn the_failure_line_names_what_the_worker_does_after_it() {
        assert!(AfterHookFailure::WorkerStops
            .text()
            .contains("the worker stops"));
        assert!(AfterHookFailure::HandlerChildStaysDown
            .text()
            .contains("the worker runs on"));
    }

    /// The handler kind boots a handler child; each task kind boots a task
    /// child with its own settings and runtime threads.
    #[test]
    fn the_init_kind_names_the_child_that_the_host_boots() {
        let task_send_pool = TaskSendPool::new(1).expect("1 is a task-send pool");
        assert_eq!(
            hosted_child(ChildKind::Handler(HandlerChild { task_send_pool })),
            HostedChild::Handler
        );
        let isolated = IsolatedChild {
            pool: child_pool(2, false),
            task_send_pool,
        };
        assert_eq!(
            hosted_child(ChildKind::Isolated(isolated)),
            HostedChild::Task(TaskChild::Isolated(isolated))
        );
        let shared = SharedAsyncChild {
            seats: AsyncSeats::new(8).expect("8 async seats"),
            pool: child_pool(3, true),
            executor_threads: ExecutorThreads::new(4).expect("4 executor threads"),
            task_send_pool,
        };
        let HostedChild::Task(kind) = hosted_child(ChildKind::SharedAsync(shared)) else {
            panic!("a shared async child runs task bodies");
        };
        assert_eq!(seats_of(kind), 8);
        assert_eq!(runtime_threads(kind).get(), 4);
        assert_eq!(task_pool_of(kind), child_pool(3, true));
        assert_eq!(runtime_threads(TaskChild::Isolated(isolated)).get(), 2);
        assert_eq!(seats_of(TaskChild::Isolated(isolated)), 1);
    }

    /// The watch thread reads the host loop of the app in a shared async
    /// child and for the main thread of a shared thread child; an isolated
    /// child, and an app with no host loop, watch none.
    #[test]
    fn the_host_loop_is_watched_in_shared_children_only() {
        let task_send_pool = TaskSendPool::new(1).expect("1 is a task-send pool");
        let isolated = TaskChild::Isolated(IsolatedChild {
            pool: child_pool(2, false),
            task_send_pool,
        });
        let shared_async = TaskChild::SharedAsync(SharedAsyncChild {
            seats: AsyncSeats::new(50).expect("50 async seats"),
            pool: child_pool(3, true),
            executor_threads: ExecutorThreads::new(4).expect("4 executor threads"),
            task_send_pool,
        });
        let shared_thread = TaskChild::SharedThread(SharedThreadChild {
            seats: ThreadSeats::new(4).expect("4 thread seats"),
            pool: child_pool(3, true),
            task_send_pool,
        });
        let host = Arc::new(super::super::HostLoop::new());
        let with_host = EventLoopWatch::Host(Arc::clone(&host));
        let watched = |kind: TaskChild, event_loop: &EventLoopWatch| -> Option<bool> {
            match host_loop_watched(kind, event_loop) {
                HostLoopWatched::NotWatched => None,
                HostLoopWatched::Watched(watched) => Some(Arc::ptr_eq(&watched, &host)),
            }
        };
        assert_eq!(watched(isolated, &with_host), None);
        assert_eq!(watched(shared_async, &with_host), Some(true));
        assert_eq!(watched(shared_thread, &with_host), Some(true));
        for kind in [isolated, shared_async, shared_thread] {
            assert_eq!(
                watched(kind, &EventLoopWatch::RuntimeOnly),
                None,
                "{kind:?}"
            );
        }
    }

    /// Python `min_size=0`: no connection stays open; another `min_size`
    /// stays open.
    #[test]
    fn the_min_size_of_a_child_pool_is_its_kept_connections() {
        assert_eq!(kept_open_of(child_pool(4, false)), KeptOpen::NoConnections);
        let pool = ChildPool {
            min_size: PoolMinSize::new(3).expect("3 is a pool min size"),
            ..child_pool(4, true)
        };
        assert_eq!(
            kept_open_of(pool),
            KeptOpen::Connections(NonZeroU32::new(3).expect("three"))
        );
    }

    /// Python `_initialize_worker_pool`: at most `max_size` connections,
    /// `CHILD_POOL_CONNECTION_WAIT_S` and `CHILD_POOL_MAX_LIFETIME_S`.
    #[test]
    fn the_child_pool_is_max_size_with_no_overflow() {
        let config = child_broker_config(URL, &ChildConnection::default(), child_pool(7, false));
        assert_eq!(config.database_url, URL);
        assert_eq!(config.session_database_url, None);
        assert_eq!(config.pool_size, 7);
        assert_eq!(config.max_overflow, 0);
        assert!(!config.pool_pre_ping);
        assert_eq!(config.pool_timeout, 30);
        assert_eq!(config.pool_recycle, PoolRecycle::after_seconds(300));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn check_turns_on_the_pre_ping_of_the_child_pool() {
        let config = child_broker_config(URL, &ChildConnection::default(), child_pool(1, true));
        assert_eq!(config.pool_size, 1);
        assert!(config.pool_pre_ping);
    }

    /// Python `PostgresConfig.pooled_connect_args`: the TCP settings of the
    /// parent reach each connection of the child.
    #[test]
    fn the_child_pool_takes_the_tcp_settings_of_the_parent() {
        let connection = ChildConnection {
            tcp_keepalives: TcpKeepalivesConfig::On {
                idle_seconds: KeepaliveSeconds::try_from(45_u32).expect("in bounds"),
                interval_seconds: KeepaliveSeconds::try_from(7_u32).expect("in bounds"),
                probes: KeepaliveProbes::try_from(5_u32).expect("in bounds"),
            },
            tcp_user_timeout: TcpUserTimeoutConfig::After {
                milliseconds: TcpUserTimeoutMs::try_from(1500_u32).expect("in bounds"),
            },
            server_sessions: ServerSessions::Kept,
        };
        let config = child_broker_config(URL, &connection, child_pool(2, false));
        assert_eq!(
            config.socket_settings(),
            SocketSettings {
                keepalives: Keepalives::On {
                    idle: Duration::from_secs(45),
                    interval: Duration::from_secs(7),
                    probes: 5,
                },
                tcp_user_timeout: TcpUserTimeout::After(Duration::from_millis(1500)),
            }
        );
        assert!(!config.pgbouncer_transaction_mode);
        assert_eq!(config.session_database_url, None);
    }

    /// Python `prepare_threshold=None` under PgBouncer transaction mode: the
    /// child's main pool sends unnamed statements, and its config names the
    /// parent's session URL, so it validates.
    #[test]
    fn transaction_mode_reaches_the_child_with_the_session_url() {
        let connection = ChildConnection {
            tcp_keepalives: TcpKeepalivesConfig::Off,
            tcp_user_timeout: TcpUserTimeoutConfig::SystemDefault,
            server_sessions: ServerSessions::PgBouncerTransactionMode {
                session_database_url: DatabaseUrl::new(SESSION_URL.to_owned()),
            },
        };
        let config = child_broker_config(URL, &connection, child_pool(2, false));
        assert!(config.pgbouncer_transaction_mode);
        assert_eq!(config.session_database_url.as_deref(), Some(SESSION_URL));
        assert_eq!(
            config.socket_settings(),
            SocketSettings {
                keepalives: Keepalives::Off,
                tcp_user_timeout: TcpUserTimeout::SystemDefault,
            }
        );
        assert!(config.validate().is_ok());
    }
}
