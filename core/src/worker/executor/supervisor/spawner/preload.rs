//! The preload fork path of the spawner thread (Linux): one preload process
//! imports code once and forks a child for each spawn request.
//!
//! The two waits of the path are states of the spawner loop with a
//! deadline, not blocking calls: the start waits at most
//! [`PRELOAD_READY_TIMEOUT`] for `preload_ready`, and a fork request at most
//! [`PRELOAD_FORK_TIMEOUT`] for `child_forked`. The preload socket is one
//! more descriptor of the spawner's wait, so the spawner thread reaps,
//! kills and reads preload frames in the same loop; reaping never stops.
//!
//! Rules (Python 0.7.0):
//! - The preload process starts with `exec` in its own session, with the
//!   parent-death signal, as a child does. The spawner appends the
//!   descriptor of its socket, the worker pid and the mode (`library` or
//!   `app`), then sends the init frame.
//! - One fork request runs at a time. While it runs, spawn requests stay in
//!   the spawner queue; kills and the stop are served.
//! - A fork request sets `PR_SET_CHILD_SUBREAPER`, sends the byte `S` with
//!   the child end of a new socket pair through `SCM_RIGHTS`, and waits for
//!   `child_forked`. At its end the flag is cleared, and the exited orphans
//!   that landed on the worker are reaped, at most `ORPHAN_SWEEP_LIMIT`.
//!   The forked child is watched through a pidfd and reaped as an `exec`
//!   child is.
//! - A request that gives no child starts the child with `exec` and a new
//!   socket pair: a forked child may hold the first pair, and it reads EOF
//!   when the parent end closes. A preload process that fails in the middle
//!   of a request is killed: its channel may hold a partial frame.
//! - A preload process that dies restarts after [`PRELOAD_RESTART_DELAY`],
//!   at most [`PRELOAD_RESTART_LIMIT`] times over the life of the worker;
//!   then every child starts with `exec`. A failed first start is not
//!   retried. Running children are not touched: the worker is their parent.
//! - At the stop, the socket closes and the preload process exits at its
//!   EOF. One that still runs after [`PRELOAD_STOP_TIMEOUT`] is killed.
//! - Not Linux: `exec` by design. Linux without pidfd or the subreaper
//!   flag: `exec`, and the start answers why.

use std::fmt;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use super::{
    exec_child, kill_session, ChildCommand, ChildId, SpawnError, SpawnedChild, SpawnerLoop,
    MAX_LIVE_CHILDREN,
};
use crate::worker::executor::protocol::{
    decode_preload_frame, encode_parent_frame, FrameReader, InitFrame, ParentFrame, PreloadFrame,
    ProcessId,
};
use crate::worker::executor::supervisor::socket::child_socket_pair;

/// The longest wait for `preload_ready` after the start of the preload
/// process (Python `PRELOAD_READY_TIMEOUT_S`).
pub const PRELOAD_READY_TIMEOUT: Duration = Duration::from_secs(120);
/// The longest wait for the answer to one fork request (Python
/// `PRELOAD_FORK_TIMEOUT_S`).
pub const PRELOAD_FORK_TIMEOUT: Duration = Duration::from_secs(5);
/// How long the stop waits for the preload process to exit at the EOF of
/// its socket before it kills it (Python `PRELOAD_STOP_TIMEOUT_S`).
pub const PRELOAD_STOP_TIMEOUT: Duration = Duration::from_secs(5);
/// The wait between the death of a preload process and its restart (Python
/// `PRELOAD_RESTART_DELAY_S`).
pub const PRELOAD_RESTART_DELAY: Duration = Duration::from_secs(5);
/// The most restarts of the preload process over the life of a worker
/// (Python `PRELOAD_RESTART_LIMIT`).
pub const PRELOAD_RESTART_LIMIT: u32 = 5;
/// The most landed orphans that one sweep reaps (Python
/// `ORPHAN_SWEEP_LIMIT`). Only the Linux fork path sweeps.
#[cfg(target_os = "linux")]
pub const ORPHAN_SWEEP_LIMIT: u32 = 64;
/// The most reads of the preload socket in one pass of the spawner loop. A
/// preload frame has no body and fits one read; more input waits for the
/// next pass.
const PRELOAD_READS_PER_PASS: u32 = 16;

/// What the preload process imports before it forks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreloadMode {
    /// Library code only; each child imports the app itself.
    Library,
    /// The library and the app; children share the app's module state.
    App,
}

impl PreloadMode {
    /// The mode argument of the preload program (Python `ChildPreload`
    /// values).
    pub const fn argument(self) -> &'static str {
        match self {
            Self::Library => "library",
            Self::App => "app",
        }
    }
}

/// How the spawner starts the preload process of a worker.
#[derive(Debug, Clone)]
pub struct PreloadPlan {
    /// The program and its first arguments. The spawner appends the
    /// descriptor of the preload's socket, the worker pid and the mode.
    pub command: ChildCommand,
    pub mode: PreloadMode,
    /// The init frame that the preload process reads first.
    pub init: InitFrame,
}

/// Why this host runs no preload process. Every child starts with `exec`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreloadUnsupported {
    /// The platform has no preload path; `exec` is the normal path there.
    NotLinux { platform: &'static str },
    /// This Linux kernel refuses a pidfd or the subreaper flag. Only a Linux
    /// host builds it; the tests of every platform build it too.
    #[cfg(any(test, target_os = "linux"))]
    MissingKernelSupport { reason: String },
}

impl fmt::Display for PreloadUnsupported {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLinux { platform } => {
                write!(
                    formatter,
                    "the preload path needs Linux, this is {platform}"
                )
            }
            #[cfg(any(test, target_os = "linux"))]
            Self::MissingKernelSupport { reason } => formatter.write_str(reason),
        }
    }
}

/// How the start of the preload process ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreloadStart {
    /// The preload process reported ready; spawns fork from it.
    Ready { pid: ProcessId },
    /// This host runs no preload process; every spawn uses `exec`.
    Unsupported(PreloadUnsupported),
    /// The preload process refused: an import raised, or a fork from it is
    /// not safe. Every spawn uses `exec`.
    Refused { reason: String },
    /// The preload process did not start, or it ended or failed before it
    /// was ready. Every spawn uses `exec`.
    StartFailed { detail: String },
    /// The spawner stopped before the start ended.
    SpawnerStopped,
}

/// Why a start of the preload process gave no ready process.
#[derive(Debug, Clone, PartialEq, Eq)]
enum StartFailure {
    Refused(String),
    Failed(String),
}

impl fmt::Display for StartFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused(reason) => write!(formatter, "the preload process refused: {reason}"),
            Self::Failed(detail) => formatter.write_str(detail),
        }
    }
}

/// Who waits for the end of a start.
#[derive(Debug)]
enum StartPurpose {
    /// The first start, from [`super::SpawnerHandle::start_preload`].
    First {
        reply: oneshot::Sender<PreloadStart>,
    },
    /// Restart number `attempt`, from 1 to [`PRELOAD_RESTART_LIMIT`].
    Restart { attempt: u32 },
}

/// A running preload process: its id among the spawner's children, the
/// worker end of its socket (non-blocking), and the frames read from it.
#[derive(Debug)]
struct PreloadProcess {
    child: ChildId,
    socket: UnixStream,
    reader: FrameReader,
}

/// One fork request in flight.
#[derive(Debug)]
struct PendingFork {
    /// The `exec` command of the spawn, for the fallback.
    command: ChildCommand,
    reply: oneshot::Sender<Result<SpawnedChild, SpawnError>>,
    /// The parent end of the child's socket pair.
    parent: UnixStream,
}

/// The preload path of one spawner.
#[derive(Debug, Default)]
enum PreloadState {
    /// Every spawn uses `exec`: no plan, a failed first start, the restart
    /// limit reached, or a host without the preload path.
    #[default]
    ExecOnly,
    /// The preload process runs and has not reported ready by `deadline`.
    Starting {
        process: PreloadProcess,
        deadline: Instant,
        purpose: StartPurpose,
    },
    /// Spawns fork from the preload process. `restarts` were used so far.
    Ready {
        process: PreloadProcess,
        restarts: u32,
    },
    /// One fork request waits for its answer until `deadline`.
    Forking {
        process: PreloadProcess,
        fork: PendingFork,
        deadline: Instant,
        restarts: u32,
    },
    /// The preload process died; a new one starts at `at`.
    Restarting { at: Instant, restarts: u32 },
    /// The spawner stops: the socket is closed, and the preload process is
    /// killed at `kill_at` when it has not exited by then.
    Stopping { child: ChildId, kill_at: Instant },
    /// The spawner stops, and the preload process got `SIGKILL`.
    StoppingKilled { child: ChildId },
}

/// The preload path of a spawner: the plan of its preload process, kept
/// for the restarts, and where the path is.
#[derive(Debug, Default)]
pub(super) struct Preload {
    plan: Option<PreloadPlan>,
    state: PreloadState,
}

impl Preload {
    /// A fork request is in flight: spawn requests wait.
    pub(super) fn is_forking(&self) -> bool {
        matches!(self.state, PreloadState::Forking { .. })
    }

    /// The preload socket, for the spawner's wait.
    pub(super) fn socket_fd(&self) -> Option<RawFd> {
        match &self.state {
            PreloadState::Starting { process, .. }
            | PreloadState::Ready { process, .. }
            | PreloadState::Forking { process, .. } => Some(process.socket.as_raw_fd()),
            PreloadState::ExecOnly
            | PreloadState::Restarting { .. }
            | PreloadState::Stopping { .. }
            | PreloadState::StoppingKilled { .. } => None,
        }
    }

    /// The next deadline of the path, for the bound of the spawner's wait.
    pub(super) fn deadline(&self) -> Option<Instant> {
        match &self.state {
            PreloadState::Starting { deadline, .. } | PreloadState::Forking { deadline, .. } => {
                Some(*deadline)
            }
            PreloadState::Restarting { at, .. } => Some(*at),
            PreloadState::Stopping { kill_at, .. } => Some(*kill_at),
            PreloadState::ExecOnly
            | PreloadState::Ready { .. }
            | PreloadState::StoppingKilled { .. } => None,
        }
    }
}

/// How a fork request ended.
enum ForkEnd {
    /// The preload process reported the pid of the forked child.
    Forked(ProcessId),
    /// No child: the spawn uses `exec`. `kill_preload` when the preload
    /// process failed in the middle of the request.
    NoChild { detail: String, kill_preload: bool },
}

impl SpawnerLoop {
    /// Start the preload process of `plan` and answer on `reply` when it is
    /// ready or its start failed. Returns at once; the wait for
    /// `preload_ready` is a state of the loop.
    pub(super) fn start_preload(
        &mut self,
        plan: PreloadPlan,
        reply: oneshot::Sender<PreloadStart>,
    ) {
        let refused = match (self.shutting_down, &self.preload.plan) {
            (true, _) => Some(PreloadStart::SpawnerStopped),
            (false, Some(_)) => Some(PreloadStart::StartFailed {
                detail: "the spawner has started a preload process already".to_owned(),
            }),
            (false, None) => match preload_support() {
                Ok(()) => None,
                Err(unsupported) => Some(PreloadStart::Unsupported(unsupported)),
            },
        };
        if let Some(answer) = refused {
            answer_start(reply, answer);
            return;
        }
        match self.launch_preload(&plan) {
            Ok(process) => {
                self.preload.plan = Some(plan);
                self.preload.state = PreloadState::Starting {
                    process,
                    deadline: Instant::now() + PRELOAD_READY_TIMEOUT,
                    purpose: StartPurpose::First { reply },
                };
            }
            Err(detail) => answer_start(reply, PreloadStart::StartFailed { detail }),
        }
    }

    /// `exec` the preload process, register it as a child of this spawner,
    /// and send its init frame. On a failure no process of the call runs
    /// unwatched: a started process is killed and reaped as a child.
    fn launch_preload(&mut self, plan: &PreloadPlan) -> Result<PreloadProcess, String> {
        if self.children.len() >= MAX_LIVE_CHILDREN {
            return Err(SpawnError::TooManyChildren.to_string());
        }
        let (pid, mut socket) = exec_child(&plan.command, self.worker_pid, &[plan.mode.argument()])
            .map_err(|error| format!("the preload process did not start: {error}"))?;
        let (child, _exit, _watch_error) = self.register(pid);
        let sent = send_init(&mut socket, &plan.init).and_then(|()| socket.set_nonblocking(true));
        match sent {
            Ok(()) => Ok(PreloadProcess {
                child,
                socket,
                reader: FrameReader::new(),
            }),
            Err(error) => {
                let _outcome = kill_session(pid);
                Err(format!(
                    "the init frame did not reach the preload process: {error}"
                ))
            }
        }
    }

    /// Start a child: a fork from a ready preload process, else `exec`.
    pub(super) fn spawn_or_fork(
        &mut self,
        command: ChildCommand,
        reply: oneshot::Sender<Result<SpawnedChild, SpawnError>>,
    ) {
        match std::mem::take(&mut self.preload.state) {
            PreloadState::Ready { process, restarts } => {
                self.begin_fork(process, restarts, command, reply);
            }
            other => {
                self.preload.state = other;
                let result = self.spawn(&command);
                self.answer_spawn(reply, result);
            }
        }
    }

    /// Send one fork request. A request that cannot be sent starts the
    /// child with `exec`.
    fn begin_fork(
        &mut self,
        process: PreloadProcess,
        restarts: u32,
        command: ChildCommand,
        reply: oneshot::Sender<Result<SpawnedChild, SpawnError>>,
    ) {
        if self.children.len() >= MAX_LIVE_CHILDREN {
            self.preload.state = PreloadState::Ready { process, restarts };
            self.answer_spawn(reply, Err(SpawnError::TooManyChildren));
            return;
        }
        let pair = match child_socket_pair() {
            Ok(pair) => pair,
            Err(error) => {
                self.preload.state = PreloadState::Ready { process, restarts };
                self.answer_spawn(reply, Err(super::spawn_error(&error)));
                return;
            }
        };
        if let Err(error) = set_subreaper(true) {
            tracing::warn!("Preload parent gave no child: prctl is refused: {error}");
            self.preload.state = PreloadState::Ready { process, restarts };
            let result = self.spawn(&command);
            self.answer_spawn(reply, result);
            return;
        }
        let sent = send_spawn_request(process.socket.as_raw_fd(), pair.child.as_raw_fd());
        // The preload process holds its own copy of the child end now.
        drop(pair.child);
        let fork = PendingFork {
            command,
            reply,
            parent: pair.parent,
        };
        self.preload.state = PreloadState::Forking {
            process,
            fork,
            deadline: Instant::now() + PRELOAD_FORK_TIMEOUT,
            restarts,
        };
        if let Err(error) = sent {
            self.end_fork(ForkEnd::NoChild {
                detail: format!("the spawn request was not sent: {error}"),
                kill_preload: true,
            });
        }
    }

    /// End the fork request in flight: clear the subreaper flag, reap the
    /// landed orphans, and answer the spawn with the forked child or a child
    /// from `exec`.
    ///
    /// Panics: when no fork request is in flight; the callers check it.
    fn end_fork(&mut self, end: ForkEnd) {
        let PreloadState::Forking {
            process,
            fork,
            restarts,
            ..
        } = std::mem::take(&mut self.preload.state)
        else {
            panic!("a fork request ended while none was in flight");
        };
        if let Err(error) = set_subreaper(false) {
            tracing::error!(error = %error, "Could not clear the subreaper flag: {error}");
        }
        // A forked child enters the registry before the sweep, so the sweep
        // never takes its exit status.
        let forked = match end {
            ForkEnd::Forked(pid) => Some(self.register(pid)),
            ForkEnd::NoChild { .. } => None,
        };
        sweep_landed_orphans(&self.by_pid);
        let PendingFork {
            command,
            reply,
            parent,
        } = fork;
        let no_child = match (forked, end) {
            (Some((id, exit, None)), ForkEnd::Forked(pid)) => {
                self.preload.state = PreloadState::Ready { process, restarts };
                let child = SpawnedChild {
                    id,
                    pid,
                    socket: parent,
                    exit,
                };
                self.answer_spawn(reply, Ok(child));
                return;
            }
            (Some((id, _exit, Some(watch_error))), ForkEnd::Forked(pid)) => {
                // No exit can be seen for this child, so it must not run. The
                // spawner still reaps it: it is polled.
                let _outcome = self.kill(id);
                (
                    format!(
                        "pidfd_open of the forked child {} failed: {watch_error}",
                        pid.get()
                    ),
                    false,
                )
            }
            (
                _,
                ForkEnd::NoChild {
                    detail,
                    kill_preload,
                },
            ) => (detail, kill_preload),
            (None, ForkEnd::Forked(_)) => unreachable!("a forked child is registered above"),
        };
        let (detail, kill_preload) = no_child;
        tracing::warn!("Preload parent gave no child: {detail}");
        // A forked child may hold this pair; it reads EOF at this close.
        drop(parent);
        match kill_preload {
            true => self.preload_lost(process, restarts, &detail),
            false => self.preload.state = PreloadState::Ready { process, restarts },
        }
        let result = self.spawn(&command);
        self.answer_spawn(reply, result);
    }

    /// Read what the preload process wrote, and act on each whole frame.
    pub(super) fn read_preload(&mut self) {
        for _ in 0..PRELOAD_READS_PER_PASS {
            let read = match &mut self.preload.state {
                PreloadState::Starting { process, .. }
                | PreloadState::Ready { process, .. }
                | PreloadState::Forking { process, .. } => {
                    let space = process.reader.read_space();
                    match (&process.socket).read(space) {
                        Ok(0) => PreloadRead::Closed,
                        Ok(count) => {
                            process.reader.arrived(count);
                            PreloadRead::Arrived
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            PreloadRead::Drained
                        }
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                            PreloadRead::Arrived
                        }
                        Err(error) => PreloadRead::Failed(error.to_string()),
                    }
                }
                PreloadState::ExecOnly
                | PreloadState::Restarting { .. }
                | PreloadState::Stopping { .. }
                | PreloadState::StoppingKilled { .. } => return,
            };
            match read {
                PreloadRead::Arrived => {
                    if !self.take_preload_frames() {
                        return;
                    }
                }
                PreloadRead::Drained => return,
                PreloadRead::Closed => {
                    self.preload_failed("the preload process closed its socket".to_owned());
                    return;
                }
                PreloadRead::Failed(cause) => {
                    self.preload_failed(format!("the preload socket failed: {cause}"));
                    return;
                }
            }
        }
    }

    /// Act on every whole frame at hand. False when the preload process is
    /// gone from the active states, so no more input is read.
    fn take_preload_frames(&mut self) -> bool {
        loop {
            let next = match &mut self.preload.state {
                PreloadState::Starting { process, .. }
                | PreloadState::Ready { process, .. }
                | PreloadState::Forking { process, .. } => process.reader.next_frame(),
                PreloadState::ExecOnly
                | PreloadState::Restarting { .. }
                | PreloadState::Stopping { .. }
                | PreloadState::StoppingKilled { .. } => return false,
            };
            let frame = match next {
                Ok(Some(raw)) => decode_preload_frame(&raw.meta, &raw.body)
                    .map_err(|malformed| format!("bad preload frame: {malformed}")),
                Ok(None) => return true,
                Err(over) => Err(format!("preload frame over the bounds: {over}")),
            };
            match frame {
                Ok(frame) => self.on_preload_frame(frame),
                Err(cause) => {
                    self.preload_failed(cause);
                    return false;
                }
            }
        }
    }

    /// One frame of the preload process, in the state it arrived in.
    fn on_preload_frame(&mut self, frame: PreloadFrame) {
        match (std::mem::take(&mut self.preload.state), frame) {
            (
                PreloadState::Starting {
                    process, purpose, ..
                },
                PreloadFrame::PreloadReady { pid },
            ) => self.preload_ready(process, purpose, pid),
            (
                PreloadState::Starting {
                    process, purpose, ..
                },
                PreloadFrame::PreloadRefused { reason },
            ) => {
                // The preload process exits after a refusal; the kill only
                // makes sure, and the spawner reaps it.
                let _outcome = kill_session(process.child.pid);
                self.start_failed(purpose, StartFailure::Refused(reason.as_str().to_owned()));
            }
            (state @ PreloadState::Forking { .. }, PreloadFrame::ChildForked { pid }) => {
                self.preload.state = state;
                self.end_fork(ForkEnd::Forked(pid));
            }
            (state @ PreloadState::Forking { .. }, PreloadFrame::ForkFailed { reason }) => {
                self.preload.state = state;
                self.end_fork(ForkEnd::NoChild {
                    detail: reason.as_str().to_owned(),
                    kill_preload: false,
                });
            }
            (state, unexpected) => {
                self.preload.state = state;
                self.preload_failed(format!("unexpected preload frame {unexpected:?}"));
            }
        }
    }

    /// The preload process reported ready.
    fn preload_ready(&mut self, process: PreloadProcess, purpose: StartPurpose, pid: ProcessId) {
        let restarts = match purpose {
            StartPurpose::First { reply } => {
                answer_start(reply, PreloadStart::Ready { pid });
                0
            }
            StartPurpose::Restart { attempt } => {
                tracing::info!("Preload parent restarted, attempt {attempt}");
                attempt
            }
        };
        self.preload.state = PreloadState::Ready { process, restarts };
    }

    /// The preload process failed in an active state: a closed socket, a
    /// failed read, a bad or unexpected frame. It is killed; a fork in
    /// flight starts its child with `exec`.
    fn preload_failed(&mut self, cause: String) {
        match std::mem::take(&mut self.preload.state) {
            PreloadState::Starting {
                process, purpose, ..
            } => {
                let _outcome = kill_session(process.child.pid);
                self.start_failed(purpose, StartFailure::Failed(cause));
            }
            PreloadState::Ready { process, restarts } => {
                self.preload_lost(process, restarts, &cause);
            }
            state @ PreloadState::Forking { .. } => {
                self.preload.state = state;
                self.end_fork(ForkEnd::NoChild {
                    detail: cause,
                    kill_preload: true,
                });
            }
            other => self.preload.state = other,
        }
    }

    /// Kill a preload process that served, and plan its restart.
    fn preload_lost(&mut self, process: PreloadProcess, restarts: u32, cause: &str) {
        let pid = process.child.pid;
        let _outcome = kill_session(pid);
        drop(process);
        tracing::error!(
            "Preload parent pid={} is down ({cause}); new children use exec",
            pid.get()
        );
        self.schedule_restart(restarts);
    }

    /// A start ended with no ready process.
    fn start_failed(&mut self, purpose: StartPurpose, failure: StartFailure) {
        match purpose {
            StartPurpose::First { reply } => {
                // A failed first start is not retried (Python keeps no
                // watch of a preload that never started).
                self.preload.plan = None;
                self.preload.state = PreloadState::ExecOnly;
                let answer = match failure {
                    StartFailure::Refused(reason) => PreloadStart::Refused { reason },
                    StartFailure::Failed(detail) => PreloadStart::StartFailed { detail },
                };
                answer_start(reply, answer);
            }
            StartPurpose::Restart { attempt } => {
                tracing::error!("Preload parent restart {attempt} failed: {failure}");
                self.schedule_restart(attempt);
            }
        }
    }

    /// Restart after the delay while restarts are left; else `exec` for the
    /// life of the worker.
    fn schedule_restart(&mut self, restarts: u32) {
        self.preload.state = match (self.shutting_down, restarts >= PRELOAD_RESTART_LIMIT) {
            (true, _) => PreloadState::ExecOnly,
            (false, true) => {
                tracing::error!(
                    "Preload parent is down after {PRELOAD_RESTART_LIMIT} restarts; children use \
                     exec from now on"
                );
                PreloadState::ExecOnly
            }
            (false, false) => PreloadState::Restarting {
                at: Instant::now() + PRELOAD_RESTART_DELAY,
                restarts,
            },
        };
    }

    /// The deadlines of the path, and the death of the preload process seen
    /// by its reap. Runs once per pass of the spawner loop.
    pub(super) fn preload_tick(&mut self, now: Instant) {
        let reaped = |child: &ChildId, loop_: &SpawnerLoop| !loop_.children.contains_key(child);
        match std::mem::take(&mut self.preload.state) {
            PreloadState::Starting {
                process,
                deadline,
                purpose,
            } => {
                let failure = match (reaped(&process.child, self), now >= deadline) {
                    (true, _) => Some("the preload process exited before it was ready".to_owned()),
                    (false, true) => Some(format!(
                        "no preload_ready within {}s",
                        PRELOAD_READY_TIMEOUT.as_secs()
                    )),
                    (false, false) => None,
                };
                self.preload.state = PreloadState::Starting {
                    process,
                    deadline,
                    purpose,
                };
                if let Some(cause) = failure {
                    self.preload_failed(cause);
                }
            }
            PreloadState::Ready { process, restarts } => match reaped(&process.child, self) {
                true => self.preload_lost(process, restarts, "the preload process exited"),
                false => self.preload.state = PreloadState::Ready { process, restarts },
            },
            state @ PreloadState::Forking { .. } => {
                let failure = match &state {
                    PreloadState::Forking {
                        process, deadline, ..
                    } => match (reaped(&process.child, self), now >= *deadline) {
                        (true, _) => Some("the preload process exited".to_owned()),
                        (false, true) => Some(format!(
                            "no child_forked within {}s",
                            PRELOAD_FORK_TIMEOUT.as_secs()
                        )),
                        (false, false) => None,
                    },
                    _ => None,
                };
                self.preload.state = state;
                if let Some(cause) = failure {
                    self.preload_failed(cause);
                }
            }
            PreloadState::Restarting { at, restarts } if now >= at => self.restart(restarts),
            PreloadState::Stopping { child, kill_at } => {
                self.preload.state = match (reaped(&child, self), now >= kill_at) {
                    (true, _) => PreloadState::ExecOnly,
                    (false, true) => {
                        tracing::warn!(
                            "Preload parent pid={} ignored the close; killing it",
                            child.pid.get()
                        );
                        let _outcome = kill_session(child.pid);
                        PreloadState::StoppingKilled { child }
                    }
                    (false, false) => PreloadState::Stopping { child, kill_at },
                };
            }
            PreloadState::StoppingKilled { child } => {
                self.preload.state = match reaped(&child, self) {
                    true => PreloadState::ExecOnly,
                    false => PreloadState::StoppingKilled { child },
                };
            }
            other => self.preload.state = other,
        }
    }

    /// Start restart number `restarts + 1`.
    fn restart(&mut self, restarts: u32) {
        let attempt = restarts + 1;
        let launched = match self.preload.plan.clone() {
            Some(plan) => self.launch_preload(&plan),
            // A restart is planned only after a first start that kept its
            // plan.
            None => unreachable!("a preload restart has no plan"),
        };
        match launched {
            Ok(process) => {
                self.preload.state = PreloadState::Starting {
                    process,
                    deadline: Instant::now() + PRELOAD_READY_TIMEOUT,
                    purpose: StartPurpose::Restart { attempt },
                };
            }
            Err(detail) => {
                self.start_failed(
                    StartPurpose::Restart { attempt },
                    StartFailure::Failed(detail),
                );
            }
        }
    }

    /// At the spawner stop: close the socket, so the preload process exits
    /// at its EOF, and end every wait of the path.
    pub(super) fn stop_preload(&mut self, now: Instant) {
        let stopping = |child: ChildId| PreloadState::Stopping {
            child,
            kill_at: now + PRELOAD_STOP_TIMEOUT,
        };
        self.preload.state = match std::mem::take(&mut self.preload.state) {
            PreloadState::Starting {
                process, purpose, ..
            } => {
                if let StartPurpose::First { reply } = purpose {
                    answer_start(reply, PreloadStart::SpawnerStopped);
                }
                stopping(process.child)
            }
            PreloadState::Ready { process, .. } => stopping(process.child),
            PreloadState::Forking { process, fork, .. } => {
                if let Err(error) = set_subreaper(false) {
                    tracing::error!(error = %error, "Could not clear the subreaper flag: {error}");
                }
                self.answer_spawn(fork.reply, Err(SpawnError::SpawnerStopped));
                stopping(process.child)
            }
            PreloadState::Restarting { .. } | PreloadState::ExecOnly => PreloadState::ExecOnly,
            state @ (PreloadState::Stopping { .. } | PreloadState::StoppingKilled { .. }) => state,
        };
    }

    /// Register `pid` as a child of this spawner and open its exit watch.
    /// The error of a watch that could not open is returned; the child is
    /// polled then.
    pub(super) fn register(
        &mut self,
        pid: ProcessId,
    ) -> (
        ChildId,
        tokio::sync::watch::Receiver<super::ExitState>,
        Option<io::Error>,
    ) {
        let (watch, watch_error) = self.watch.watch(pid);
        let id = ChildId {
            _serial: self.next_id,
            pid,
        };
        self.next_id += 1;
        let (exit_tx, exit_rx) = tokio::sync::watch::channel(super::ExitState::Running);
        self.children.insert(
            id,
            super::LiveChild {
                watch,
                exit: exit_tx,
            },
        );
        self.by_pid.insert(pid, id);
        (id, exit_rx, watch_error)
    }
}

/// The result of one read of the preload socket.
enum PreloadRead {
    Arrived,
    Drained,
    Closed,
    Failed(String),
}

/// Answer a start; a requester that left needs no answer.
fn answer_start(reply: oneshot::Sender<PreloadStart>, answer: PreloadStart) {
    if let Err(answer) = reply.send(answer) {
        tracing::debug!(?answer, "the preload start has no requester");
    }
}

/// Write the init frame on the blocking socket of a new preload process.
fn send_init(socket: &mut UnixStream, init: &InitFrame) -> io::Result<()> {
    let frame = ParentFrame::Init(init.clone());
    let encoded = encode_parent_frame(&frame)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
    socket.write_all(encoded.head())?;
    socket.write_all(encoded.body())
}

/// Whether this host can run the preload path: Linux, a pidfd, and the
/// subreaper flag (Python `preload_support`).
#[cfg(target_os = "linux")]
fn preload_support() -> Result<(), PreloadUnsupported> {
    kernel_support(open_own_pidfd(), || set_subreaper(false))
}

/// The preload path of a Linux kernel, from the result of a `pidfd_open` of
/// this process. `clear_subreaper` clears the subreaper flag; it runs only
/// after the pidfd opened (Python `preload_support`).
#[cfg(target_os = "linux")]
fn kernel_support(
    pidfd_open: io::Result<()>,
    clear_subreaper: impl FnOnce() -> io::Result<()>,
) -> Result<(), PreloadUnsupported> {
    pidfd_open.map_err(|error| PreloadUnsupported::MissingKernelSupport {
        reason: format!("pidfd_open: {error}"),
    })?;
    clear_subreaper().map_err(|error| PreloadUnsupported::MissingKernelSupport {
        reason: format!("prctl is refused: {error}"),
    })
}

/// Open a pidfd of this process and close it again.
#[cfg(target_os = "linux")]
fn open_own_pidfd() -> io::Result<()> {
    // SAFETY: pidfd_open of this process's own pid; it returns a new
    // descriptor or -1 and touches no memory of this process.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, libc::getpid(), 0) };
    match RawFd::try_from(fd) {
        Ok(fd) if fd >= 0 => {
            // SAFETY: the kernel returned a new descriptor that nothing else
            // owns.
            drop(unsafe { <std::os::fd::OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(fd) });
            Ok(())
        }
        _ => Err(io::Error::last_os_error()),
    }
}

#[cfg(not(target_os = "linux"))]
fn preload_support() -> Result<(), PreloadUnsupported> {
    Err(PreloadUnsupported::NotLinux {
        platform: std::env::consts::OS,
    })
}

/// Set or clear `PR_SET_CHILD_SUBREAPER` of this process.
#[cfg(target_os = "linux")]
fn set_subreaper(enabled: bool) -> io::Result<()> {
    let value: libc::c_ulong = match enabled {
        true => 1,
        false => 0,
    };
    let unused: libc::c_ulong = 0;
    // SAFETY: prctl with integer arguments, passed as the unsigned longs
    // the kernel reads.
    let result =
        unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, value, unused, unused, unused) };
    match result {
        0 => Ok(()),
        _ => Err(io::Error::last_os_error()),
    }
}

/// Never called off Linux: no preload process starts there.
#[cfg(not(target_os = "linux"))]
fn set_subreaper(_enabled: bool) -> io::Result<()> {
    Err(io::Error::from(io::ErrorKind::Unsupported))
}

/// The most tries of one `sendmsg` that a signal interrupts.
#[cfg(target_os = "linux")]
const SEND_ATTEMPTS: u32 = 3;

/// Send the spawn request: the byte `S` with `child_end` through
/// `SCM_RIGHTS` (Python `socket.send_fds`).
#[cfg(target_os = "linux")]
fn send_spawn_request(socket: RawFd, child_end: RawFd) -> io::Result<()> {
    use crate::worker::executor::protocol::SPAWN_REQUEST_BYTE;

    let byte = [SPAWN_REQUEST_BYTE];
    let mut iov = libc::iovec {
        iov_base: byte.as_ptr().cast_mut().cast::<libc::c_void>(),
        iov_len: byte.len(),
    };
    let fd_bytes = u32::try_from(std::mem::size_of::<RawFd>())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    // SAFETY: CMSG_SPACE and CMSG_LEN compute sizes from an integer.
    let (space, length) = unsafe { (libc::CMSG_SPACE(fd_bytes), libc::CMSG_LEN(fd_bytes)) };
    // A u64 array keeps the control buffer aligned for a cmsghdr.
    let mut control = [0_u64; 4];
    let space = usize::try_from(space).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    assert!(
        space <= std::mem::size_of_val(&control),
        "the control message of one descriptor needs {space} bytes"
    );
    // SAFETY: an all-zero msghdr is a valid value (null pointers, no
    // flags).
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = &mut iov;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast::<libc::c_void>();
    message.msg_controllen = space as _;
    // SAFETY: `message` points at a control buffer of `space` bytes, so the
    // first header is inside it; its fields and data are written in place.
    unsafe {
        let header = libc::CMSG_FIRSTHDR(&message);
        assert!(!header.is_null(), "the control buffer holds one header");
        (*header).cmsg_level = libc::SOL_SOCKET;
        (*header).cmsg_type = libc::SCM_RIGHTS;
        (*header).cmsg_len = length as _;
        std::ptr::write_unaligned(libc::CMSG_DATA(header).cast::<RawFd>(), child_end);
    }
    for _ in 0..SEND_ATTEMPTS {
        // SAFETY: `message` and every buffer it points at live for the call.
        let sent = unsafe { libc::sendmsg(socket, &message, libc::MSG_NOSIGNAL) };
        match sent {
            1 => return Ok(()),
            -1 => {
                let error = io::Error::last_os_error();
                match error.kind() {
                    io::ErrorKind::Interrupted => continue,
                    _ => return Err(error),
                }
            }
            other => {
                return Err(io::Error::other(format!(
                    "sendmsg sent {other} bytes of the 1-byte spawn request"
                )))
            }
        }
    }
    Err(io::Error::from(io::ErrorKind::Interrupted))
}

/// Never called off Linux: no preload process starts there.
#[cfg(not(target_os = "linux"))]
fn send_spawn_request(_socket: RawFd, _child_end: RawFd) -> io::Result<()> {
    Err(io::Error::from(io::ErrorKind::Unsupported))
}

/// Reap the exited orphans that landed on the worker while the subreaper
/// flag was on (Python `reap_landed_orphans`).
///
/// An orphan is an exited child that this spawner did not register and
/// that left the worker's session. The sweep only peeks (`WNOWAIT`) and
/// stops at any other exited child, which keeps its status for its own
/// waiter: `waitid` would return it again.
#[cfg(target_os = "linux")]
fn sweep_landed_orphans(known: &std::collections::HashMap<ProcessId, ChildId>) {
    // SAFETY: getsid of this process takes no pointer.
    let own_session = unsafe { libc::getsid(0) };
    for _ in 0..ORPHAN_SWEEP_LIMIT {
        let Some(pid) = next_exited_child() else {
            return;
        };
        if known.contains_key(&pid) {
            return;
        }
        // SAFETY: getsid of a pid takes no pointer.
        let session = unsafe { libc::getsid(pid.as_pid_t()) };
        if session == -1 || session == own_session {
            return;
        }
        let mut status: libc::c_int = 0;
        // SAFETY: waitpid of one exited pid, writing into a live local.
        let reaped = unsafe { libc::waitpid(pid.as_pid_t(), &mut status, libc::WNOHANG) };
        if reaped != pid.as_pid_t() {
            return;
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn sweep_landed_orphans(_known: &std::collections::HashMap<ProcessId, ChildId>) {}

/// An exited child of this process, not reaped, or `None`.
#[cfg(target_os = "linux")]
fn next_exited_child() -> Option<ProcessId> {
    // SAFETY: an all-zero siginfo_t is a valid value.
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    // SAFETY: waitid writes into a live local siginfo_t.
    let result = unsafe {
        libc::waitid(
            libc::P_ALL,
            0,
            &mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if result == -1 {
        return None;
    }
    // SAFETY: waitid filled `info` for an exited child, or left it zeroed
    // when none exited; both have a readable pid field.
    let pid = unsafe { info.si_pid() };
    u32::try_from(pid).ok().and_then(ProcessId::new)
}

#[cfg(all(test, target_os = "linux"))]
mod tests;

#[cfg(all(test, not(target_os = "linux")))]
mod other_platform_tests;
