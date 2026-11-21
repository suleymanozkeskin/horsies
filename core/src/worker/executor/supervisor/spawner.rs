//! The spawner thread: the one place that starts, signals and reaps child
//! processes (Python executor design, section 2.5).
//!
//! - Every `exec` of a child runs on this thread. Linux sends a child's
//!   parent-death signal when the thread that forked it ends, and this
//!   thread lives as long as the worker.
//! - It is the one owner of reaping: no tokio process driver, no `SIGCHLD`
//!   handler, and no wait call on any other thread. It waits on its request
//!   waker and on every child at once ([`super::exit_watch`]) and reaps a
//!   child with `waitpid` of that pid when its exit is reported.
//! - Every kill goes through it. The kill of a child is `killpg` of its
//!   pid, which is also the id of its session, before or after the reap
//!   (Python `ChildProcess.kill_group`). While one process of that session
//!   lives, the OS gives its id to no new process, so the signal reaches
//!   only that session. After the reap, the signal is refused when a live
//!   child of this spawner holds the pid.
//! - It publishes each child's [`ExitState`] on a `watch` channel that the
//!   core runtime reads.
//! - On Linux it can serve spawns by fork from a preload process
//!   ([`preload`]); the waits of that path are states of this loop, so a
//!   reap or a kill is never held up by them.

use std::collections::{HashMap, VecDeque};
use std::ffi::OsString;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{mpsc, Arc, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tokio::sync::{oneshot, watch};

use super::exit::{ExitState, ReapedExit};
use super::exit_watch::{ChildWatch, ExitWatch, POLLED_EXIT_INTERVAL};
use super::socket::child_socket_pair;
use crate::worker::executor::protocol::{ProcessId, ShortageErrno};

mod preload;

pub use preload::{
    PreloadMode, PreloadPlan, PreloadStart, PreloadUnsupported, ORPHAN_SWEEP_LIMIT,
    PRELOAD_FORK_TIMEOUT, PRELOAD_READY_TIMEOUT, PRELOAD_RESTART_DELAY, PRELOAD_RESTART_LIMIT,
    PRELOAD_STOP_TIMEOUT,
};

/// The name of the spawner thread.
const SPAWNER_THREAD_NAME: &str = "horsies-spawner";
/// The longest wait of one loop pass when no child is polled. A wake or an
/// exit ends it sooner; the bound keeps a lost wake from stalling requests.
const IDLE_WAIT: Duration = Duration::from_secs(1);
/// The most requests that wait for the spawner thread at once.
pub const MAX_QUEUED_REQUESTS: usize = 4096;
/// The most children the spawner keeps at once, reaped or not yet reaped.
pub const MAX_LIVE_CHILDREN: usize = 8192;
/// A write to the waker that a signal interrupts is tried this often.
const WAKE_WRITE_ATTEMPTS: u32 = 3;
/// The waker is drained in reads of this size, at most this many reads per
/// pass; a pending byte left over only causes one more pass.
const WAKE_DRAIN_BYTES: usize = 64;
const WAKE_DRAIN_READS: u32 = 16;

/// The id of one spawned child: a serial number that is never reused, and
/// the pid of the child, which is also the id of its session. A kill or a
/// wait that names an id cannot reach the next child of the same slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChildId {
    serial: u64,
    pid: ProcessId,
}

impl ChildId {
    pub fn get(self) -> u64 {
        self.serial
    }

    /// The id of a reaped child whose pid `pid` now belongs to another
    /// process. Serials start at 1, so serial 0 names no child that the
    /// spawner started.
    #[cfg(test)]
    pub(super) fn reaped_child_with_pid(pid: ProcessId) -> Self {
        Self { serial: 0, pid }
    }
}

/// The program of a child and its leading arguments. The spawner appends
/// the descriptor number of the child's socket and the worker's pid, as the
/// Python child program reads them (`child_main <fd> <worker pid>`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildCommand {
    pub program: PathBuf,
    pub args: Vec<OsString>,
}

/// A child that the spawner started.
#[derive(Debug)]
pub struct SpawnedChild {
    pub id: ChildId,
    pub pid: ProcessId,
    /// The parent end of the child's socket, in blocking mode.
    pub socket: UnixStream,
    /// The process state, published by the spawner thread.
    pub exit: watch::Receiver<ExitState>,
}

/// Why a spawn gave no child. No process or descriptor of the request is
/// left.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SpawnError {
    /// The OS ran short of a resource. This can clear.
    #[error(
        "the OS ran short of a resource ({}) while starting a child: {detail}",
        .errno.as_str()
    )]
    ResourceShortage {
        errno: ShortageErrno,
        detail: String,
    },
    /// The OS refused the start for another reason, for example a program
    /// that does not exist. This repeats.
    #[error("the OS refused to start a child: {detail}")]
    SpawnFailed { detail: String },
    /// The spawner keeps [`MAX_LIVE_CHILDREN`] children already.
    #[error("the spawner already keeps {MAX_LIVE_CHILDREN} children")]
    TooManyChildren,
    /// [`MAX_QUEUED_REQUESTS`] requests wait already.
    #[error("{MAX_QUEUED_REQUESTS} spawner requests wait already")]
    QueueFull,
    /// The spawner is shutting down or its thread ended.
    #[error("the spawner is stopped")]
    SpawnerStopped,
}

/// What a kill request did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillOutcome {
    /// The child is not reaped. `SIGKILL` went to its session, or to its
    /// pid when it has no session of its own yet.
    Sent,
    /// The child is reaped. `SIGKILL` went to the processes left in its
    /// session.
    SentToReapedSession,
    /// The child is reaped and no process is left in its session (`ESRCH`).
    SessionGone,
    /// The child is reaped and a live child of this spawner holds its pid;
    /// nothing was signalled. The OS does not reuse a pid while a process
    /// of the session with that id lives, so no process is left in the
    /// session of the reaped child.
    PidReused,
    /// The OS refused the signal (`EPERM`).
    NotPermitted,
    /// The signal failed with this errno.
    Failed { errno: i32 },
    /// [`MAX_QUEUED_REQUESTS`] requests wait already.
    QueueFull,
    /// The spawner thread ended.
    SpawnerStopped,
}

/// The spawner thread could not start.
#[derive(Debug, thiserror::Error)]
pub enum SpawnerStartError {
    #[error("the spawner waker could not open: {0}")]
    Waker(io::Error),
    #[error("the spawner exit watch could not open: {0}")]
    ExitWatch(io::Error),
    #[error("the spawner thread could not start: {0}")]
    Thread(io::Error),
}

/// What remained when the spawner stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpawnerStopReport {
    /// Every child was reaped and the thread ended.
    AllReaped,
    /// The bound passed before every child was reaped. The thread keeps
    /// reaping until the process exits.
    BoundPassed,
}

enum Request {
    Spawn {
        command: ChildCommand,
        reply: oneshot::Sender<Result<SpawnedChild, SpawnError>>,
    },
    /// A spawn by `exec` alone, also while a preload process runs.
    SpawnExec {
        command: ChildCommand,
        reply: oneshot::Sender<Result<SpawnedChild, SpawnError>>,
    },
    Kill {
        child: ChildId,
        reply: oneshot::Sender<KillOutcome>,
    },
    StartPreload {
        plan: PreloadPlan,
        reply: oneshot::Sender<PreloadStart>,
    },
    Shutdown,
}

/// Whether the spawner takes new spawns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Intake {
    Open,
    ShuttingDown,
}

struct Queue {
    requests: VecDeque<Request>,
    intake: Intake,
}

struct Shared {
    queue: Mutex<Queue>,
    waker: Waker,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, Queue> {
        // A poisoned lock means a panic while the queue was held; the queue
        // is still a valid list of requests.
        self.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The self-pipe that wakes the spawner thread.
struct Waker {
    read: OwnedFd,
    write: OwnedFd,
}

fn set_fd_flag(fd: RawFd, get: libc::c_int, set: libc::c_int, flag: libc::c_int) -> io::Result<()> {
    // SAFETY: fcntl on an open descriptor with integer arguments.
    let flags = unsafe { libc::fcntl(fd, get) };
    if flags == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: as above.
    match unsafe { libc::fcntl(fd, set, flags | flag) } {
        -1 => Err(io::Error::last_os_error()),
        _ => Ok(()),
    }
}

impl Waker {
    fn new() -> io::Result<Self> {
        let mut fds: [libc::c_int; 2] = [-1, -1];
        // SAFETY: `fds` has room for the two descriptors pipe writes.
        if unsafe { libc::pipe(fds.as_mut_ptr()) } == -1 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: pipe returned two new descriptors that nothing else owns.
        let (read, write) = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
        for fd in [read.as_raw_fd(), write.as_raw_fd()] {
            set_fd_flag(fd, libc::F_GETFL, libc::F_SETFL, libc::O_NONBLOCK)?;
            set_fd_flag(fd, libc::F_GETFD, libc::F_SETFD, libc::FD_CLOEXEC)?;
        }
        Ok(Self { read, write })
    }

    /// Wake the spawner thread. A full pipe means a wake is pending already.
    fn wake(&self) {
        let byte: u8 = 1;
        for _ in 0..WAKE_WRITE_ATTEMPTS {
            // SAFETY: one byte from a live local to an open pipe.
            let written = unsafe {
                libc::write(
                    self.write.as_raw_fd(),
                    (&byte as *const u8).cast::<libc::c_void>(),
                    1,
                )
            };
            match written {
                1 => return,
                _ => {
                    let error = io::Error::last_os_error();
                    match error.kind() {
                        io::ErrorKind::Interrupted => continue,
                        io::ErrorKind::WouldBlock => return,
                        _ => {
                            tracing::error!(
                                error = %error,
                                "the spawner waker could not be written: {error}",
                            );
                            return;
                        }
                    }
                }
            }
        }
    }

    fn drain(&self) {
        let mut buffer = [0_u8; WAKE_DRAIN_BYTES];
        for _ in 0..WAKE_DRAIN_READS {
            // SAFETY: reads into a live local buffer of its own length.
            let read = unsafe {
                libc::read(
                    self.read.as_raw_fd(),
                    buffer.as_mut_ptr().cast::<libc::c_void>(),
                    buffer.len(),
                )
            };
            match usize::try_from(read) {
                Ok(count) if count == buffer.len() => continue,
                _ => return,
            }
        }
    }
}

/// A handle to the spawner thread, for the core runtime.
#[derive(Clone)]
pub struct SpawnerHandle {
    shared: Arc<Shared>,
}

impl SpawnerHandle {
    /// Start a child: a socket pair with explicit buffers, then `exec` of
    /// the command in a new session, with the child end as its only passed
    /// descriptor and stdin from `/dev/null`.
    pub async fn spawn(&self, command: ChildCommand) -> Result<SpawnedChild, SpawnError> {
        let (reply, answer) = oneshot::channel();
        self.queue_spawn(Request::Spawn { command, reply })?;
        answer.await.unwrap_or(Err(SpawnError::SpawnerStopped))
    }

    /// Start a child with `exec`, as [`SpawnerHandle::spawn`] does with no
    /// preload process: never by a fork from the preload process, also
    /// while one runs. The handler child starts this way: the preload
    /// process holds the import of the task children.
    pub async fn spawn_exec(&self, command: ChildCommand) -> Result<SpawnedChild, SpawnError> {
        let (reply, answer) = oneshot::channel();
        self.queue_spawn(Request::SpawnExec { command, reply })?;
        answer.await.unwrap_or(Err(SpawnError::SpawnerStopped))
    }

    /// Queue a spawn request and wake the spawner thread.
    ///
    /// Errors: [`SpawnError::SpawnerStopped`] after the stop;
    /// [`SpawnError::QueueFull`] at [`MAX_QUEUED_REQUESTS`].
    fn queue_spawn(&self, request: Request) -> Result<(), SpawnError> {
        {
            let mut queue = self.shared.lock();
            match (queue.intake, queue.requests.len() >= MAX_QUEUED_REQUESTS) {
                (Intake::ShuttingDown, _) => return Err(SpawnError::SpawnerStopped),
                (Intake::Open, true) => return Err(SpawnError::QueueFull),
                (Intake::Open, false) => queue.requests.push_back(request),
            }
        }
        self.shared.waker.wake();
        Ok(())
    }

    /// Start the preload process of `plan` and wait until it is ready or its
    /// start ended (Python `SlotPool._start_preload`): at most
    /// [`PRELOAD_READY_TIMEOUT`]. The spawner thread serves reaps and kills
    /// meanwhile. After any answer but `Ready`, every spawn uses `exec`.
    /// Call once per spawner, before the first spawn.
    pub async fn start_preload(&self, plan: PreloadPlan) -> PreloadStart {
        let (reply, answer) = oneshot::channel();
        {
            let mut queue = self.shared.lock();
            match (queue.intake, queue.requests.len() >= MAX_QUEUED_REQUESTS) {
                (Intake::ShuttingDown, _) => return PreloadStart::SpawnerStopped,
                (Intake::Open, true) => {
                    return PreloadStart::StartFailed {
                        detail: SpawnError::QueueFull.to_string(),
                    }
                }
                (Intake::Open, false) => queue
                    .requests
                    .push_back(Request::StartPreload { plan, reply }),
            }
        }
        self.shared.waker.wake();
        answer.await.unwrap_or(PreloadStart::SpawnerStopped)
    }

    /// Send `SIGKILL` to the session of `child`, so the subprocesses of its
    /// task end with it, except one that left the session itself. After the
    /// reap of `child`, the processes left in its session get the signal.
    pub async fn kill_session(&self, child: ChildId) -> KillOutcome {
        match self.request_kill(child) {
            Ok(answer) => answer.await.unwrap_or(KillOutcome::SpawnerStopped),
            Err(refused) => refused,
        }
    }

    /// Queue the kill of `child`'s session without waiting for it, for a
    /// thread outside the core runtime (the runner heartbeat watchdog). The
    /// spawner thread sends the answer on the receiver; a closed receiver
    /// means the spawner thread ended.
    ///
    /// `Err(KillOutcome::QueueFull)`: [`MAX_QUEUED_REQUESTS`] requests wait
    /// already, and nothing was queued.
    pub fn request_kill(
        &self,
        child: ChildId,
    ) -> Result<oneshot::Receiver<KillOutcome>, KillOutcome> {
        let (reply, answer) = oneshot::channel();
        {
            let mut queue = self.shared.lock();
            match queue.requests.len() >= MAX_QUEUED_REQUESTS {
                true => return Err(KillOutcome::QueueFull),
                false => queue.requests.push_back(Request::Kill { child, reply }),
            }
        }
        self.shared.waker.wake();
        Ok(answer)
    }
}

/// The spawner thread and its handle.
pub struct Spawner {
    handle: SpawnerHandle,
    stopped: mpsc::Receiver<()>,
    thread: JoinHandle<()>,
}

impl Spawner {
    /// Start the spawner thread. Call once per worker process, from a thread
    /// that is not a task of the core runtime.
    pub fn start() -> Result<Self, SpawnerStartError> {
        let waker = Waker::new().map_err(SpawnerStartError::Waker)?;
        let watch = ExitWatch::new(waker.read.as_raw_fd()).map_err(SpawnerStartError::ExitWatch)?;
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue {
                requests: VecDeque::new(),
                intake: Intake::Open,
            }),
            waker,
        });
        let (stopped_tx, stopped) = mpsc::channel();
        let thread_shared = Arc::clone(&shared);
        // The thread logs to the dispatcher of its starter.
        let thread = std::thread::Builder::new()
            .name(SPAWNER_THREAD_NAME.to_owned())
            .spawn(crate::worker::thread_dispatch::with_starter_dispatch(
                move || {
                    SpawnerLoop::new(thread_shared, watch).run();
                    // The receiver is gone only when `stop` is not waiting.
                    let _ = stopped_tx.send(());
                },
            ))
            .map_err(SpawnerStartError::Thread)?;
        Ok(Self {
            handle: SpawnerHandle { shared },
            stopped,
            thread,
        })
    }

    pub fn handle(&self) -> SpawnerHandle {
        self.handle.clone()
    }

    /// Stop taking spawns, and wait up to `bound` for every child to be
    /// reaped and the thread to end. Call after the pool stopped its
    /// children: on Linux, the end of this thread sends the parent-death
    /// signal to every child it started.
    pub fn stop(self, bound: Duration) -> SpawnerStopReport {
        {
            let mut queue = self.handle.shared.lock();
            queue.intake = Intake::ShuttingDown;
            queue.requests.push_back(Request::Shutdown);
        }
        self.handle.shared.waker.wake();
        match self.stopped.recv_timeout(bound) {
            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                // The thread ended: it sent its signal or it panicked. A
                // panic of the spawner thread is a defect; it shows here.
                match self.thread.join() {
                    Ok(()) => SpawnerStopReport::AllReaped,
                    Err(panic) => std::panic::resume_unwind(panic),
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => SpawnerStopReport::BoundPassed,
        }
    }
}

/// One child the spawner has not reaped. Its pid is in its [`ChildId`].
struct LiveChild {
    watch: ChildWatch,
    exit: watch::Sender<ExitState>,
}

/// How the spawner learned of a possible exit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExitReport {
    /// The child's watch reported its exit, once.
    ByWatch,
    /// A poll pass, which repeats.
    ByPoll,
}

/// The result of one `waitpid` with `WNOHANG`.
enum ReapAttempt {
    StillRunning,
    Reaped(ReapedExit),
    ReapedElsewhere,
}

struct SpawnerLoop {
    shared: Arc<Shared>,
    watch: ExitWatch,
    children: HashMap<ChildId, LiveChild>,
    by_pid: HashMap<ProcessId, ChildId>,
    next_id: u64,
    worker_pid: u32,
    shutting_down: bool,
    preload: preload::Preload,
}

impl SpawnerLoop {
    fn new(shared: Arc<Shared>, watch: ExitWatch) -> Self {
        Self {
            shared,
            watch,
            children: HashMap::new(),
            by_pid: HashMap::new(),
            next_id: 1,
            worker_pid: std::process::id(),
            shutting_down: false,
            preload: preload::Preload::default(),
        }
    }

    /// Serve requests and reap children until a shutdown finds no child
    /// left. Each pass waits for a request, an exit, input from the preload
    /// process, the next deadline of the preload path, or [`IDLE_WAIT`].
    fn run(mut self) {
        loop {
            self.serve_queued();
            if self.shutting_down && self.children.is_empty() {
                return;
            }
            let any_polled = self
                .children
                .values()
                .any(|child| matches!(child.watch, ChildWatch::Polled));
            let timeout = wait_bound(any_polled, self.preload.deadline(), Instant::now());
            let preload_socket = self.preload.socket_fd();
            let wake = self.watch.wait(
                self.children
                    .iter()
                    .map(|(id, child)| (id.pid, &child.watch)),
                preload_socket,
                timeout,
            );
            match wake {
                Ok(wake) => {
                    if wake.woken {
                        self.shared.waker.drain();
                    }
                    for pid in wake.exited {
                        self.reap(pid, ExitReport::ByWatch);
                    }
                    if wake.extra_ready {
                        self.read_preload();
                    }
                }
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        "the spawner exit watch failed: {error}; polling every child",
                    );
                    std::thread::sleep(POLLED_EXIT_INTERVAL);
                    self.reap_every_child();
                }
            }
            self.reap_polled();
            self.preload_tick(Instant::now());
        }
    }

    /// Serve every queued request. While a fork request is in flight, spawn
    /// requests go back to the front of the queue in their order: the
    /// preload process serves one request at a time.
    fn serve_queued(&mut self) {
        let requests: Vec<Request> = self.shared.lock().requests.drain(..).collect();
        let mut held: Vec<Request> = Vec::new();
        for request in requests {
            match (request, self.preload.is_forking()) {
                (spawn @ Request::Spawn { .. }, true) => held.push(spawn),
                (request, _) => self.serve(request),
            }
        }
        if !held.is_empty() {
            let mut queue = self.shared.lock();
            for request in held.into_iter().rev() {
                queue.requests.push_front(request);
            }
        }
    }

    fn serve(&mut self, request: Request) {
        match request {
            Request::Spawn { command, reply } => self.spawn_or_fork(command, reply),
            Request::SpawnExec { command, reply } => {
                let result = self.spawn(&command);
                self.answer_spawn(reply, result);
            }
            Request::Kill { child, reply } => {
                // The requester may have gone away; the kill is sent anyway.
                let _ = reply.send(self.kill(child));
            }
            Request::StartPreload { plan, reply } => self.start_preload(plan, reply),
            Request::Shutdown => {
                self.shutting_down = true;
                self.stop_preload(Instant::now());
            }
        }
    }

    /// Answer a spawn. A child whose requester went away is killed: no one
    /// holds it.
    fn answer_spawn(
        &self,
        reply: oneshot::Sender<Result<SpawnedChild, SpawnError>>,
        result: Result<SpawnedChild, SpawnError>,
    ) {
        if let Err(Ok(orphan)) = reply.send(result) {
            tracing::warn!(
                pid = orphan.pid.get(),
                "a spawned child has no requester; killing it"
            );
            let _outcome = self.kill(orphan.id);
        }
    }

    /// Start a child with `exec`.
    fn spawn(&mut self, command: &ChildCommand) -> Result<SpawnedChild, SpawnError> {
        match (self.shutting_down, self.children.len() >= MAX_LIVE_CHILDREN) {
            (true, _) => return Err(SpawnError::SpawnerStopped),
            (false, true) => return Err(SpawnError::TooManyChildren),
            (false, false) => {}
        }
        let (pid, socket) = exec_child(command, self.worker_pid, &[])?;
        let (id, exit, watch_error) = self.register(pid);
        if let Some(error) = &watch_error {
            tracing::warn!(
                pid = pid.get(),
                error = %error,
                "no exit notification for this child: {error}; its exit is polled"
            );
        }
        Ok(SpawnedChild {
            id,
            pid,
            socket,
            exit,
        })
    }

    /// `SIGKILL` to the session of child `id`, by the live child that holds
    /// its pid: the child itself (not reaped), another child (the pid was
    /// reused; refused), or none (reaped).
    fn kill(&self, id: ChildId) -> KillOutcome {
        match self.by_pid.get(&id.pid) {
            Some(holder) if *holder == id => kill_session(id.pid),
            Some(_) => KillOutcome::PidReused,
            None => kill_reaped_session(id.pid),
        }
    }

    /// Reap `pid` when its exit is there; publish it. A watch reports an
    /// exit once, so a child that its watch reported but `waitpid` did not
    /// reap is polled from then on.
    fn reap(&mut self, pid: ProcessId, report: ExitReport) {
        let Some(&id) = self.by_pid.get(&pid) else {
            return;
        };
        let state = match (try_reap(pid), report) {
            (ReapAttempt::StillRunning, ExitReport::ByPoll) => return,
            (ReapAttempt::StillRunning, ExitReport::ByWatch) => {
                if let Some(child) = self.children.get_mut(&id) {
                    child.watch = ChildWatch::Polled;
                }
                return;
            }
            (ReapAttempt::Reaped(exit), _) => ExitState::Reaped(exit),
            (ReapAttempt::ReapedElsewhere, _) => {
                tracing::error!(
                    pid = pid.get(),
                    "the exit status of a child was taken by another waiter in this process"
                );
                ExitState::ReapedElsewhere
            }
        };
        self.by_pid.remove(&pid);
        if let Some(child) = self.children.remove(&id) {
            let _running = child.exit.send_replace(state);
        }
    }

    fn reap_polled(&mut self) {
        let polled: Vec<ProcessId> = self
            .children
            .iter()
            .filter(|(_, child)| matches!(child.watch, ChildWatch::Polled))
            .map(|(id, _)| id.pid)
            .collect();
        for pid in polled {
            self.reap(pid, ExitReport::ByPoll);
        }
    }

    /// After a failure of the exit watch: try every child.
    fn reap_every_child(&mut self) {
        let pids: Vec<ProcessId> = self.children.keys().map(|id| id.pid).collect();
        for pid in pids {
            self.reap(pid, ExitReport::ByPoll);
        }
    }
}

/// The bound of one wait of the spawner loop: the poll interval when a
/// child is polled, else [`IDLE_WAIT`], and never past the next deadline of
/// the preload path. Pure.
fn wait_bound(any_polled: bool, preload_deadline: Option<Instant>, now: Instant) -> Duration {
    let base = match any_polled {
        true => POLLED_EXIT_INTERVAL,
        false => IDLE_WAIT,
    };
    match preload_deadline {
        Some(deadline) => base.min(deadline.saturating_duration_since(now)),
        None => base,
    }
}

fn try_reap(pid: ProcessId) -> ReapAttempt {
    let mut status: libc::c_int = 0;
    // SAFETY: waitpid of one pid, writing the status into a live local.
    let result = unsafe { libc::waitpid(pid.as_pid_t(), &mut status, libc::WNOHANG) };
    match result {
        0 => ReapAttempt::StillRunning,
        -1 => match io::Error::last_os_error().raw_os_error() {
            Some(libc::ECHILD) => ReapAttempt::ReapedElsewhere,
            _ => ReapAttempt::StillRunning,
        },
        reaped if reaped == pid.as_pid_t() => match ReapedExit::from_wait_status(status) {
            Some(exit) => ReapAttempt::Reaped(exit),
            None => ReapAttempt::StillRunning,
        },
        other => {
            tracing::error!(
                pid = pid.get(),
                returned = other,
                "waitpid of one pid returned another pid"
            );
            ReapAttempt::StillRunning
        }
    }
}

/// `SIGKILL` to the session of an unreaped child. A child that has no
/// session of its own yet (a forked child before its `setsid`) gets the
/// signal on its pid.
fn kill_session(pid: ProcessId) -> KillOutcome {
    // SAFETY: killpg sends a signal; it touches no memory of this process.
    let result = unsafe { libc::killpg(pid.as_pid_t(), libc::SIGKILL) };
    match result {
        0 => KillOutcome::Sent,
        _ => match io::Error::last_os_error().raw_os_error() {
            Some(libc::ESRCH) => kill_pid(pid),
            Some(libc::EPERM) => KillOutcome::NotPermitted,
            Some(errno) => KillOutcome::Failed { errno },
            None => KillOutcome::Failed { errno: 0 },
        },
    }
}

fn kill_pid(pid: ProcessId) -> KillOutcome {
    // SAFETY: kill sends a signal to an unreaped child of this process.
    let result = unsafe { libc::kill(pid.as_pid_t(), libc::SIGKILL) };
    match result {
        0 => KillOutcome::Sent,
        _ => match io::Error::last_os_error().raw_os_error() {
            Some(libc::ESRCH) => KillOutcome::Sent,
            Some(libc::EPERM) => KillOutcome::NotPermitted,
            Some(errno) => KillOutcome::Failed { errno },
            None => KillOutcome::Failed { errno: 0 },
        },
    }
}

/// `SIGKILL` to the session of a reaped child, whose pid no live child of
/// this spawner holds. Only the session is signalled: the pid itself names
/// no process of this child any more.
fn kill_reaped_session(pid: ProcessId) -> KillOutcome {
    // SAFETY: killpg sends a signal; it touches no memory of this process.
    let result = unsafe { libc::killpg(pid.as_pid_t(), libc::SIGKILL) };
    match result {
        0 => KillOutcome::SentToReapedSession,
        _ => match io::Error::last_os_error().raw_os_error() {
            Some(libc::ESRCH) => KillOutcome::SessionGone,
            Some(libc::EPERM) => KillOutcome::NotPermitted,
            Some(errno) => KillOutcome::Failed { errno },
            None => KillOutcome::Failed { errno: 0 },
        },
    }
}

/// The cause of an `io::Error` of a start step: a resource shortage for a
/// clearable errno, else a spawn failure.
fn spawn_error(error: &io::Error) -> SpawnError {
    let errno = match error.raw_os_error() {
        Some(libc::EMFILE) => Some(ShortageErrno::Emfile),
        Some(libc::ENFILE) => Some(ShortageErrno::Enfile),
        Some(libc::ENOMEM) => Some(ShortageErrno::Enomem),
        Some(libc::EAGAIN) => Some(ShortageErrno::Eagain),
        _ => None,
    };
    match errno {
        Some(errno) => SpawnError::ResourceShortage {
            errno,
            detail: error.to_string(),
        },
        None => SpawnError::SpawnFailed {
            detail: error.to_string(),
        },
    }
}

/// Start the child: socket pair, then `exec` with the child end as the only
/// passed descriptor. The arguments are the command's, the descriptor
/// number of the child end, the worker pid, then `trailing`. Our copy of the
/// child end closes when this returns.
fn exec_child(
    command: &ChildCommand,
    worker_pid: u32,
    trailing: &[&str],
) -> Result<(ProcessId, UnixStream), SpawnError> {
    let pair = child_socket_pair().map_err(|error| spawn_error(&error))?;
    let child_fd = pair.child.as_raw_fd();
    let parent_pid = libc::pid_t::try_from(worker_pid).map_err(|_| SpawnError::SpawnFailed {
        detail: format!("the worker pid {worker_pid} is not a pid_t"),
    })?;
    let mut process = Command::new(&command.program);
    process
        .args(&command.args)
        .arg(child_fd.to_string())
        .arg(worker_pid.to_string())
        .args(trailing)
        .stdin(Stdio::null());
    // SAFETY: the hook runs in the forked child before `exec` and calls only
    // async-signal-safe functions (setsid, fcntl, prctl, getppid, _exit).
    unsafe {
        process.pre_exec(move || prepare_child(child_fd, parent_pid));
    }
    let started = process.spawn().map_err(|error| spawn_error(&error))?;
    let pid = ProcessId::new(started.id()).ok_or_else(|| SpawnError::SpawnFailed {
        detail: format!("the OS returned pid {} for a child", started.id()),
    })?;
    // Dropping the std handle neither waits nor signals; the spawner reaps.
    drop(started);
    drop(pair.child);
    Ok((pid, pair.parent))
}

/// In the forked child: lead a new session, keep the child end of the
/// socket open across `exec`, and die with the worker.
fn prepare_child(child_fd: RawFd, parent_pid: libc::pid_t) -> io::Result<()> {
    // SAFETY: setsid takes no argument.
    if unsafe { libc::setsid() } == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fcntl on the open child end with integer arguments.
    let flags = unsafe { libc::fcntl(child_fd, libc::F_GETFD) };
    if flags == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: as above.
    if unsafe { libc::fcntl(child_fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } == -1 {
        return Err(io::Error::last_os_error());
    }
    set_parent_death_signal(parent_pid)
}

/// Linux: `SIGKILL` when the spawner thread ends; then exit at once when
/// the worker is gone already (Python `child_main.py` does the same check).
#[cfg(target_os = "linux")]
fn set_parent_death_signal(parent_pid: libc::pid_t) -> io::Result<()> {
    use crate::worker::executor::protocol::PARENT_GONE_EXIT_CODE;
    // SAFETY: prctl with integer arguments; the signal is passed as the
    // unsigned long the kernel reads.
    let set = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL as libc::c_ulong) };
    if set == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: getppid takes no argument.
    if unsafe { libc::getppid() } != parent_pid {
        // SAFETY: _exit ends the forked child without running destructors.
        unsafe { libc::_exit(PARENT_GONE_EXIT_CODE) };
    }
    Ok(())
}

/// Other platforms have no parent-death signal; the child exits at the EOF
/// of its socket.
#[cfg(not(target_os = "linux"))]
fn set_parent_death_signal(_parent_pid: libc::pid_t) -> io::Result<()> {
    Ok(())
}

/// Wait up to `bound` for the spawner to publish the end of a child.
pub async fn wait_for_exit(
    exit: &mut watch::Receiver<ExitState>,
    bound: Duration,
) -> super::exit::ChildExit {
    use super::exit::ChildExit;
    let ended = tokio::time::timeout(bound, exit.wait_for(|state| *state != ExitState::Running));
    match ended.await {
        Ok(Ok(state)) => match *state {
            ExitState::Reaped(reaped) => ChildExit::Reaped(reaped),
            ExitState::ReapedElsewhere => ChildExit::ReapedElsewhere,
            ExitState::Running => ChildExit::ExitUnknown,
        },
        // The sender is gone only when the spawner thread ended.
        Ok(Err(_)) | Err(_) => ChildExit::ExitUnknown,
    }
}
