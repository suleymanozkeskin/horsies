//! The entry of child mode: boot, report ready, serve, and name the exit
//! code (Python `core/worker/child_main.py`).

use std::io;
use std::num::NonZeroUsize;
use std::os::fd::{FromRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;

use super::args::{ChildArgs, CHILD_MODE_ARGUMENT};
use super::link::{read_frame, start_control_thread, write_frame, FrameWriter, ReadEnd};
use super::rss::read_rss;
use super::run::HostContext;
use super::seats::{serve_isolated, serve_shared_async, serve_shared_thread, PollWatch, ServeEnd};
use crate::broker::PostgresBroker;
use crate::core::config::PostgresConfig;
use crate::core::TaskRegistry;
use crate::worker::executor::protocol::{
    decode_parent_frame, BootDetail, BootErrorName, BootFailedFrame, ChildExitCode, ChildFrame,
    ChildKind, ChildPool, ChildProgram, FrameReader, InitFrame, ParentFrame, ParentFrameRefusal,
    ProcessId, ReadyFrame, ReportedBootCause, ShortageErrno,
};
use crate::worker::executor::supervisor::{ChildCommand, MailboxBound};

/// The longest wait for the init frame (Python `INIT_FRAME_TIMEOUT_S`).
const INIT_FRAME_TIMEOUT: Duration = Duration::from_secs(30);
/// Runtime threads of an isolated or a shared thread child: the start and
/// marker writes and one async body at a time.
const SMALL_RUNTIME_THREADS: usize = 2;
const RUNTIME_THREAD_NAME: &str = "horsies-child-runtime";

/// What a worker binary gives its child mode: its tasks and the settings a
/// child reads from the app.
pub struct ChildApp {
    pub registry: Arc<TaskRegistry>,
    /// The host name written with each start.
    pub hostname: String,
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
/// Python child imports the app again.
pub fn run_child_host(app: ChildApp, args: ChildArgs) -> ChildHostExit {
    if let Err(exit) = watch_parent(args.worker_pid) {
        return exit;
    }
    let socket = match open_socket(args.socket_fd) {
        Ok(socket) => socket,
        Err(error) => {
            tracing::error!(fd = args.socket_fd, error = %error, "the child socket descriptor is not open");
            return ChildHostExit::BadArguments;
        }
    };
    let mut reader = FrameReader::new();
    let init = match read_init(&socket, &mut reader) {
        Ok(init) => init,
        Err(exit) => return exit,
    };
    boot_and_serve(app, socket, reader, init)
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
            tracing::error!(error = %io::Error::last_os_error(), "the parent-death signal could not be set");
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
/// protocol version, for a Rust child.
fn read_init(socket: &UnixStream, reader: &mut FrameReader) -> Result<InitFrame, ChildHostExit> {
    socket
        .set_read_timeout(Some(INIT_FRAME_TIMEOUT))
        .map_err(|_| ChildHostExit::ParentGone)?;
    let mut stream = socket;
    let raw = match read_frame(&mut stream, reader) {
        Ok(raw) => raw,
        Err(ReadEnd::Eof | ReadEnd::Failed(_)) => return Err(ChildHostExit::ParentGone),
        Err(ReadEnd::OverBound(over)) => {
            tracing::error!(error = %over, "the parent sent a frame over the bounds");
            return Err(ChildHostExit::BadParentFrame);
        }
    };
    socket
        .set_read_timeout(None)
        .map_err(|_| ChildHostExit::ParentGone)?;
    let init = match decode_parent_frame(&raw.meta, raw.body) {
        Ok(ParentFrame::Init(init)) => init,
        Ok(ParentFrame::Run(_) | ParentFrame::SharedRun(_) | ParentFrame::Stop) => {
            tracing::error!("the first parent frame was not an init frame");
            return Err(ChildHostExit::BadParentFrame);
        }
        Err(ParentFrameRefusal::ProtocolMismatch(mismatch)) => {
            tracing::error!(error = %mismatch, "the worker speaks another protocol version");
            return Err(ChildHostExit::ProtocolMismatch);
        }
        Err(ParentFrameRefusal::Malformed(malformed)) => {
            tracing::error!(error = %malformed, "the parent sent a bad init frame");
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

/// The cause of an `io::Error` of a boot step: a resource shortage for a
/// clearable errno, else `step_cause`.
fn shortage_or(error: &io::Error, step_cause: ReportedBootCause) -> ReportedBootCause {
    match error.raw_os_error() {
        Some(libc::EMFILE) => ReportedBootCause::ResourceShortage(ShortageErrno::Emfile),
        Some(libc::ENFILE) => ReportedBootCause::ResourceShortage(ShortageErrno::Enfile),
        Some(libc::ENOMEM) => ReportedBootCause::ResourceShortage(ShortageErrno::Enomem),
        Some(libc::EAGAIN) => ReportedBootCause::ResourceShortage(ShortageErrno::Eagain),
        _ => step_cause,
    }
}

/// Report a failed boot on the socket and end with the boot-failed code.
fn boot_failed(
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

fn pool_of(kind: ChildKind) -> ChildPool {
    match kind {
        ChildKind::Isolated(child) => child.pool,
        ChildKind::SharedAsync(child) => child.pool,
        ChildKind::SharedThread(child) => child.pool,
    }
}

/// The seats of a child kind: one for an isolated child.
fn seats_of(kind: ChildKind) -> usize {
    let seats = match kind {
        ChildKind::Isolated(_) => 1,
        ChildKind::SharedAsync(child) => child.seats.get(),
        ChildKind::SharedThread(child) => child.seats.get(),
    };
    usize::try_from(seats).unwrap_or(usize::MAX).max(1)
}

fn runtime_threads(kind: ChildKind) -> usize {
    match kind {
        ChildKind::Isolated(_) | ChildKind::SharedThread(_) => SMALL_RUNTIME_THREADS,
        ChildKind::SharedAsync(child) => {
            usize::try_from(child.executor_threads.get()).unwrap_or(SMALL_RUNTIME_THREADS)
        }
    }
}

/// The child's connection pool: the URL's settings, at most the pool of the
/// init frame. The child applies no migration.
async fn connect(init: &InitFrame) -> Result<PostgresBroker, String> {
    let url = init.database_url.as_str();
    PostgresConfig::from_url(url)
        .validate()
        .map_err(|error| error.to_string())?;
    let pool = pool_of(init.child_kind);
    let pg_pool = PgPoolOptions::new()
        .min_connections(u32::try_from(pool.min_size.get()).unwrap_or(u32::MAX))
        .max_connections(u32::try_from(pool.max_size.get()).unwrap_or(u32::MAX))
        .test_before_acquire(pool.check)
        .connect(url)
        .await
        .map_err(|error| error.to_string())?;
    Ok(PostgresBroker::from_pool_observe_only(pg_pool))
}

/// What the serve loop of a child kind needs beyond the shared context.
enum Serving {
    Isolated,
    SharedAsync { watch: PollWatch, seats: usize },
    SharedThread { seats: usize },
}

/// Start the writer thread, the poll watch of a shared async child, and the
/// control thread.
fn start_threads(
    socket: &UnixStream,
    reader: FrameReader,
    kind: ChildKind,
) -> io::Result<(
    FrameWriter,
    Serving,
    tokio::sync::mpsc::Receiver<ParentFrame>,
)> {
    let seats = seats_of(kind);
    let bound =
        MailboxBound::for_seats(NonZeroUsize::new(seats).unwrap_or(NonZeroUsize::MIN)).get();
    let writer = FrameWriter::start(socket.try_clone()?, bound)?;
    let serving = match kind {
        ChildKind::Isolated(_) => Serving::Isolated,
        ChildKind::SharedAsync(_) => Serving::SharedAsync {
            watch: PollWatch::start(writer.clone())?,
            seats,
        },
        ChildKind::SharedThread(_) => Serving::SharedThread { seats },
    };
    let frames = start_control_thread(socket.try_clone()?, reader, bound)?;
    Ok((writer, serving, frames))
}

fn boot_and_serve(
    app: ChildApp,
    socket: UnixStream,
    reader: FrameReader,
    init: InitFrame,
) -> ChildHostExit {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(runtime_threads(init.child_kind))
        .thread_name(RUNTIME_THREAD_NAME)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let cause = shortage_or(&error, ReportedBootCause::ThreadStartRefused);
            return boot_failed(&socket, cause, "RuntimeStartFailed", &error.to_string());
        }
    };
    let broker = match runtime.block_on(connect(&init)) {
        Ok(broker) => Arc::new(broker),
        Err(detail) => {
            return boot_failed(
                &socket,
                ReportedBootCause::ChildPoolSetupFailed,
                "DatabaseError",
                &detail,
            )
        }
    };
    let (writer, serving, mut frames) = match start_threads(&socket, reader, init.child_kind) {
        Ok(threads) => threads,
        Err(error) => {
            let cause = shortage_or(&error, ReportedBootCause::ThreadStartRefused);
            return boot_failed(&socket, cause, "ThreadStartFailed", &error.to_string());
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
    // Work that still runs (a stuck body) must not hold the exit.
    runtime.shutdown_background();
    match end {
        ServeEnd::Stopped => ChildHostExit::Stopped,
        ServeEnd::BadParentFrame => ChildHostExit::BadParentFrame,
        ServeEnd::ParentGone => ChildHostExit::ParentGone,
    }
}
