//! The parent side of one child: its socket, its frames and its process.
//!
//! Verdict rules, in this order:
//! - A result is a complete frame, and nothing else.
//! - A death is the reaped exit, and nothing else.
//! - EOF is a hint: a helper forked by task code can hold the socket open
//!   after the child is gone.
//! - After a death, read what the child already wrote, until
//!   [`DRAIN_QUIET_PASSES`] passes see no new byte or [`DRAIN_DEADLINE`]
//!   passes. A complete frame wins.
//! - EOF with a live child: [`CHANNEL_LOST_GRACE`], then a kill.
//! - A bad frame kills that child. The task is lost, the worker is not.

use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::exit::{ChildExit, ExitState};
use super::spawner::{wait_for_exit, ChildId, KillOutcome, SpawnedChild, SpawnerHandle};
use crate::worker::executor::protocol::{
    decode_child_frame, encode_parent_frame, ChildFrame, ChildFrameRefusal, FrameEncodeError,
    FrameOverBound, FrameReader, MalformedFrame, ParentFrame, ProcessId, ReceivedProtocol,
};

/// After an EOF, the wait for the child's exit before it is killed.
pub const CHANNEL_LOST_GRACE: Duration = Duration::from_millis(200);
/// After a death, quiet passes that end the read of what the child wrote.
pub const DRAIN_QUIET_PASSES: u8 = 3;
/// After a death, the longest read of what the child wrote.
pub const DRAIN_DEADLINE: Duration = Duration::from_secs(5);
/// One pass of that read waits this long for new bytes. A pass is a wait,
/// not a yield, because the reader task runs on another runtime thread.
pub const DRAIN_PASS_WAIT: Duration = Duration::from_millis(20);
/// The longest wait for an exit after a `SIGKILL`.
pub const CHILD_KILL_WAIT: Duration = Duration::from_secs(30);
/// The longest wait for a child to exit after a stop frame.
pub const CHILD_STOP_TIMEOUT: Duration = Duration::from_secs(10);
/// A frame with a body up to this size goes to the socket in one write.
pub const JOINED_WRITE_BODY_BYTES: usize = 1 << 16;
/// The longest write of one frame. A child that takes none of a frame's
/// bytes for this long is frozen; the caller kills it.
pub const FRAME_WRITE_BOUND: Duration = Duration::from_secs(30);

/// Why a child ended without a result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LossCause {
    /// The child died by itself.
    ChildDied,
    /// The socket closed while the child lived.
    ChannelLost,
    /// The child sent a bad frame, a frame at the wrong time, or more
    /// frames than its mailbox holds.
    ProtocolViolation,
    /// The ready frame named another protocol version.
    ProtocolMismatch(ReceivedProtocol),
    /// The worker killed the child.
    KilledByParent,
    /// The slot of the child failed.
    SlotDefect,
    /// The spawner thread ended, so no exit can be seen.
    SpawnerStopped,
}

impl fmt::Display for LossCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChildDied => f.write_str("child died"),
            Self::ChannelLost => f.write_str("channel to the child was lost"),
            Self::ProtocolViolation => f.write_str("child sent a bad frame"),
            Self::ProtocolMismatch(received) => {
                write!(f, "child speaks protocol {received}")
            }
            Self::KilledByParent => f.write_str("killed by the worker"),
            Self::SlotDefect => f.write_str("the worker slot failed"),
            Self::SpawnerStopped => f.write_str("the spawner stopped"),
        }
    }
}

/// Whether bytes of a frame that was not complete were at hand at the loss.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartialFrame {
    NoneAtHand,
    Discarded,
}

/// The child ended, or was ended, with no complete result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChildLost {
    pub cause: LossCause,
    pub exit: ChildExit,
    pub partial: PartialFrame,
    pub pid: ProcessId,
}

impl fmt::Display for ChildLost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.cause, self.exit)?;
        match self.partial {
            PartialFrame::NoneAtHand => Ok(()),
            PartialFrame::Discarded => f.write_str(", partial result discarded"),
        }
    }
}

/// What a wait for the next frame gave.
#[derive(Debug, Clone, PartialEq)]
pub enum FrameWait {
    Frame(ChildFrame),
    Lost(ChildLost),
    /// The wait's bound passed with no frame and no loss.
    Expired,
}

/// A frame could not be sent. The channel may hold part of the frame, so
/// the caller kills the child.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SendFailure {
    #[error(transparent)]
    Encode(FrameEncodeError),
    #[error("the write of a frame failed: {0:?}")]
    Write(io::ErrorKind),
    #[error("the child took no frame within {}s", FRAME_WRITE_BOUND.as_secs())]
    TimedOut,
}

/// How many frames a child may write ahead of the parent: `seats * 4 + 2`
/// (Python `core/worker/shared_child.py`). A child past it is a protocol
/// violation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MailboxBound(NonZeroUsize);

impl MailboxBound {
    pub fn for_seats(seats: NonZeroUsize) -> Self {
        let frames = seats.get().saturating_mul(4).saturating_add(2);
        Self(NonZeroUsize::MIN.saturating_add(frames - 1))
    }

    pub fn get(self) -> usize {
        self.0.get()
    }
}

/// Why the reader task of a channel ended.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ReaderEnd {
    /// The child end closed.
    Eof,
    /// A read failed.
    ReadFailed(io::ErrorKind),
    /// The child broke the protocol.
    Violation(Violation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Violation {
    OverBound(FrameOverBound),
    Malformed(MalformedFrame),
    ProtocolMismatch(ReceivedProtocol),
    MailboxFull { bound: usize },
}

impl Violation {
    fn loss_cause(&self) -> LossCause {
        match self {
            Self::ProtocolMismatch(received) => LossCause::ProtocolMismatch(*received),
            Self::OverBound(_) | Self::Malformed(_) | Self::MailboxFull { .. } => {
                LossCause::ProtocolViolation
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ReaderPhase {
    Reading,
    Ended(ReaderEnd),
}

/// What the reader task hands to the channel.
struct Inbox {
    frames: VecDeque<ChildFrame>,
    phase: ReaderPhase,
    bytes_received: u64,
    partial: PartialFrame,
}

struct InboxShared {
    inbox: Mutex<Inbox>,
    notify: Notify,
}

impl InboxShared {
    fn lock(&self) -> MutexGuard<'_, Inbox> {
        // A poisoned lock means a panic while the inbox was held; its
        // frames and counts are still valid values.
        self.inbox.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn end(&self, end: ReaderEnd, reader: &FrameReader) {
        {
            let mut inbox = self.lock();
            inbox.phase = ReaderPhase::Ended(end);
            inbox.bytes_received = reader.bytes_received();
            inbox.partial = partial_of(reader);
        }
        self.notify.notify_one();
    }
}

fn partial_of(reader: &FrameReader) -> PartialFrame {
    match reader.has_partial_frame() {
        true => PartialFrame::Discarded,
        false => PartialFrame::NoneAtHand,
    }
}

/// Read frames until EOF, a failed read or a violation, and hand each to
/// the inbox. Each pass of the outer loop is one read of the socket.
async fn read_frames(mut stream: OwnedReadHalf, shared: Arc<InboxShared>, bound: MailboxBound) {
    let mut reader = FrameReader::new();
    loop {
        loop {
            let raw = match reader.next_frame() {
                Ok(Some(raw)) => raw,
                Ok(None) => break,
                Err(over) => {
                    return shared.end(ReaderEnd::Violation(Violation::OverBound(over)), &reader)
                }
            };
            let frame = match decode_child_frame(&raw.meta, raw.body) {
                Ok(frame) => frame,
                Err(ChildFrameRefusal::Malformed(malformed)) => {
                    return shared.end(
                        ReaderEnd::Violation(Violation::Malformed(malformed)),
                        &reader,
                    )
                }
                Err(ChildFrameRefusal::ProtocolMismatch(mismatch)) => {
                    return shared.end(
                        ReaderEnd::Violation(Violation::ProtocolMismatch(mismatch.received)),
                        &reader,
                    )
                }
            };
            let full = {
                let mut inbox = shared.lock();
                match inbox.frames.len() >= bound.get() {
                    true => true,
                    false => {
                        inbox.frames.push_back(frame);
                        false
                    }
                }
            };
            match full {
                true => {
                    return shared.end(
                        ReaderEnd::Violation(Violation::MailboxFull { bound: bound.get() }),
                        &reader,
                    )
                }
                false => shared.notify.notify_one(),
            }
        }
        match stream.read(reader.read_space()).await {
            Ok(0) => return shared.end(ReaderEnd::Eof, &reader),
            Ok(count) => {
                reader.arrived(count);
                let mut inbox = shared.lock();
                inbox.bytes_received = reader.bytes_received();
                inbox.partial = partial_of(&reader);
            }
            Err(error) => match error.kind() {
                io::ErrorKind::Interrupted => {}
                kind => return shared.end(ReaderEnd::ReadFailed(kind), &reader),
            },
        }
        // New bytes are a change for a drain after a death.
        shared.notify.notify_one();
    }
}

/// What one look at the channel found.
enum Observed {
    Frame(ChildFrame),
    Exited,
    ChannelEnded(ReaderEnd),
    Nothing,
}

/// Whether the worker killed this child on purpose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KillMark {
    NotKilled,
    KilledByParent,
}

/// The mark of a kill by the worker, shared by the channel of one child and
/// the runner heartbeat watchdog. The watchdog marks a child before it kills
/// it, so the slot reads the loss as a kill by the worker (Python
/// `register_kill_mark`, `run_kill_mark`).
#[derive(Debug, Clone, Default)]
pub struct WorkerKillMark(Arc<AtomicBool>);

impl WorkerKillMark {
    /// Record that the worker kills this child.
    pub fn mark(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    pub(crate) fn read(&self) -> KillMark {
        match self.0.load(Ordering::SeqCst) {
            true => KillMark::KilledByParent,
            false => KillMark::NotKilled,
        }
    }
}

/// Whether a channel still kills its child when it is dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DropKill {
    Armed,
    Disarmed,
}

/// Kills the session of the child when its channel is dropped before it
/// was disposed of: the slot task that held it panicked or was aborted
/// (Python `_clear_slot`). [`ChildChannel::dispose`] disarms it, so a child
/// that exits by itself after the stop frame keeps the background
/// processes it started on purpose.
struct KillOnDrop {
    child: ChildId,
    pid: ProcessId,
    spawner: SpawnerHandle,
    state: DropKill,
}

impl KillOnDrop {
    fn disarm(mut self) {
        self.state = DropKill::Disarmed;
    }
}

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        match self.state {
            DropKill::Disarmed => {}
            DropKill::Armed => {
                // The answer is not awaited: the spawner kills the session
                // of the child, reaped or not, and the reader ends at the
                // EOF that follows.
                if let Err(refused) = self.spawner.request_kill(self.child) {
                    tracing::error!(
                        pid = self.pid.get(),
                        refused = ?refused,
                        "a dropped child channel could not ask for the kill of its child",
                    );
                }
            }
        }
    }
}

/// The parent side of one child.
pub struct ChildChannel {
    id: ChildId,
    pid: ProcessId,
    writer: OwnedWriteHalf,
    shared: Arc<InboxShared>,
    reader_task: JoinHandle<()>,
    exit: watch::Receiver<ExitState>,
    spawner: SpawnerHandle,
    kill_mark: WorkerKillMark,
    drop_kill: KillOnDrop,
    /// The longest wait for the exit after a `SIGKILL`.
    kill_wait: Duration,
}

impl ChildChannel {
    /// Take a spawned child: its socket moves onto the core runtime and a
    /// reader task starts. Must run inside the core runtime. `kill_wait` is
    /// the longest wait for its exit after a `SIGKILL`
    /// ([`CHILD_KILL_WAIT`] in a worker).
    ///
    /// `Err`: the socket could not join the runtime. The child is then
    /// killed by the caller; this call does not signal it.
    pub fn attach(
        spawned: SpawnedChild,
        spawner: SpawnerHandle,
        bound: MailboxBound,
        kill_wait: Duration,
    ) -> io::Result<Self> {
        spawned.socket.set_nonblocking(true)?;
        let stream = tokio::net::UnixStream::from_std(spawned.socket)?;
        let (read_half, writer) = stream.into_split();
        let shared = Arc::new(InboxShared {
            inbox: Mutex::new(Inbox {
                frames: VecDeque::new(),
                phase: ReaderPhase::Reading,
                bytes_received: 0,
                partial: PartialFrame::NoneAtHand,
            }),
            notify: Notify::new(),
        });
        let reader_task = tokio::spawn(read_frames(read_half, Arc::clone(&shared), bound));
        let drop_kill = KillOnDrop {
            child: spawned.id,
            pid: spawned.pid,
            spawner: spawner.clone(),
            state: DropKill::Armed,
        };
        Ok(Self {
            id: spawned.id,
            pid: spawned.pid,
            writer,
            shared,
            reader_task,
            exit: spawned.exit,
            spawner,
            kill_mark: WorkerKillMark::default(),
            drop_kill,
            kill_wait,
        })
    }

    pub fn id(&self) -> ChildId {
        self.id
    }

    pub fn pid(&self) -> ProcessId {
        self.pid
    }

    /// The longest wait for the exit of this child after a `SIGKILL`.
    pub fn kill_wait(&self) -> Duration {
        self.kill_wait
    }

    /// The kill mark of this child, for the runner heartbeat registry.
    pub fn kill_mark(&self) -> WorkerKillMark {
        self.kill_mark.clone()
    }

    /// The process state of the child, for the runner heartbeat: it beats
    /// only children that run.
    pub fn exit_watch(&self) -> watch::Receiver<ExitState> {
        self.exit.clone()
    }

    pub fn has_exited(&self) -> bool {
        *self.exit.borrow() != ExitState::Running
    }

    /// Whether the reader ended: EOF, a failed read, or a violation. A child
    /// whose channel closed can never report a result.
    pub fn channel_closed(&self) -> bool {
        matches!(self.shared.lock().phase, ReaderPhase::Ended(_))
    }

    /// Send one frame, bounded by [`FRAME_WRITE_BOUND`]. A body up to
    /// [`JOINED_WRITE_BODY_BYTES`] goes in one write with its header; a
    /// larger one in a write of its own, so it is not copied.
    pub async fn send(&mut self, frame: &ParentFrame) -> Result<(), SendFailure> {
        let encoded = encode_parent_frame(frame).map_err(SendFailure::Encode)?;
        let writer = &mut self.writer;
        let write = async {
            match encoded.body().len() <= JOINED_WRITE_BODY_BYTES {
                true => writer.write_all(&encoded.to_bytes()).await,
                false => match writer.write_all(encoded.head()).await {
                    Ok(()) => writer.write_all(encoded.body()).await,
                    Err(error) => Err(error),
                },
            }
        };
        match tokio::time::timeout(FRAME_WRITE_BOUND, write).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(SendFailure::Write(error.kind())),
            Err(_) => Err(SendFailure::TimedOut),
        }
    }

    fn observe(&self) -> Observed {
        let mut inbox = self.shared.lock();
        let frame = inbox.frames.pop_front();
        let exited = *self.exit.borrow() != ExitState::Running;
        match (frame, exited, &inbox.phase) {
            (Some(frame), _, _) => Observed::Frame(frame),
            (None, true, _) => Observed::Exited,
            (None, false, ReaderPhase::Ended(end)) => Observed::ChannelEnded(end.clone()),
            (None, false, ReaderPhase::Reading) => Observed::Nothing,
        }
    }

    /// Wait for the next frame, or for a verdict that none will come.
    /// `None` waits until a frame or a loss. One waiter at a time: the slot
    /// task that owns this child.
    pub async fn await_frame(&mut self, bound: Option<Duration>) -> FrameWait {
        let until = bound.map(|bound| Instant::now() + bound);
        loop {
            match self.observe() {
                Observed::Frame(frame) => return FrameWait::Frame(frame),
                Observed::Exited => return self.verdict_after_exit().await,
                Observed::ChannelEnded(end) => return self.channel_lost(end).await,
                Observed::Nothing => {}
            }
            let shared = Arc::clone(&self.shared);
            let notified = shared.notify.notified();
            let expired = async {
                match until {
                    Some(until) => tokio::time::sleep_until(until).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                () = notified => {}
                changed = self.exit.changed() => {
                    if changed.is_err() {
                        return FrameWait::Lost(self.lost(LossCause::SpawnerStopped, ChildExit::ExitUnknown));
                    }
                }
                () = expired => return FrameWait::Expired,
            }
        }
    }

    /// The child is reaped: read what it already wrote, then decide.
    async fn verdict_after_exit(&mut self) -> FrameWait {
        let until = Instant::now() + DRAIN_DEADLINE;
        let mut quiet_passes: u8 = 0;
        let mut seen = self.shared.lock().bytes_received;
        loop {
            let reader_ended = {
                let mut inbox = self.shared.lock();
                match inbox.frames.pop_front() {
                    Some(frame) => return FrameWait::Frame(frame),
                    None => matches!(inbox.phase, ReaderPhase::Ended(_)),
                }
            };
            // The reader ended: every byte the child wrote is in.
            let done =
                reader_ended || quiet_passes >= DRAIN_QUIET_PASSES || Instant::now() >= until;
            if done {
                return FrameWait::Lost(self.lost(self.loss_cause(), self.current_exit()));
            }
            let shared = Arc::clone(&self.shared);
            let pass = tokio::time::timeout(DRAIN_PASS_WAIT, shared.notify.notified());
            // A timeout of the pass is a quiet pass, not an error.
            let _ = pass.await;
            let received = self.shared.lock().bytes_received;
            quiet_passes = match received == seen {
                true => quiet_passes.saturating_add(1),
                false => 0,
            };
            seen = received;
        }
    }

    /// The reader ended while the child was not reaped.
    async fn channel_lost(&mut self, end: ReaderEnd) -> FrameWait {
        match end {
            ReaderEnd::Violation(violation) => {
                tracing::error!(
                    pid = self.pid.get(),
                    violation = ?violation,
                    "a child broke the frame protocol; killing it"
                );
                let _ = self.kill_group().await;
                let exit = self.wait_exit(self.kill_wait).await;
                FrameWait::Lost(self.lost(violation.loss_cause(), exit))
            }
            ReaderEnd::Eof | ReaderEnd::ReadFailed(_) => {
                let _ = self.wait_exit(CHANNEL_LOST_GRACE).await;
                if self.has_exited() {
                    return self.verdict_after_exit().await;
                }
                let _ = self.kill_group().await;
                let exit = self.wait_exit(self.kill_wait).await;
                let cause = match self.kill_mark.read() {
                    KillMark::KilledByParent => LossCause::KilledByParent,
                    KillMark::NotKilled => LossCause::ChannelLost,
                };
                FrameWait::Lost(self.lost(cause, exit))
            }
        }
    }

    /// The loss cause, read in this order: a violation, a kill by the
    /// parent, a channel lost with a live child, a death.
    fn loss_cause(&self) -> LossCause {
        let phase = self.shared.lock().phase.clone();
        match (phase, self.kill_mark.read(), self.has_exited()) {
            (ReaderPhase::Ended(ReaderEnd::Violation(violation)), _, _) => violation.loss_cause(),
            (_, KillMark::KilledByParent, _) => LossCause::KilledByParent,
            (
                ReaderPhase::Ended(ReaderEnd::Eof | ReaderEnd::ReadFailed(_)),
                KillMark::NotKilled,
                false,
            ) => LossCause::ChannelLost,
            (_, KillMark::NotKilled, _) => LossCause::ChildDied,
        }
    }

    fn current_exit(&self) -> ChildExit {
        match *self.exit.borrow() {
            ExitState::Running => ChildExit::ExitUnknown,
            ExitState::Reaped(reaped) => ChildExit::Reaped(reaped),
            ExitState::ReapedElsewhere => ChildExit::ReapedElsewhere,
        }
    }

    fn lost(&self, cause: LossCause, exit: ChildExit) -> ChildLost {
        ChildLost {
            cause,
            exit,
            partial: self.shared.lock().partial,
            pid: self.pid,
        }
    }

    /// Describe this child's end for a cause the caller knows.
    pub fn lost_for(&self, cause: LossCause) -> ChildLost {
        self.lost(cause, self.current_exit())
    }

    /// `SIGKILL` to the child's session. The caller releases the runner
    /// beats of the child's tasks first.
    pub async fn kill_group(&self) -> KillOutcome {
        self.spawner.kill_session(self.id).await
    }

    /// Kill on the caller's request, and report the loss as such.
    pub async fn kill_for_caller(&mut self) -> KillOutcome {
        self.kill_mark.mark();
        self.kill_group().await
    }

    /// Wait up to `bound` for the exit. Sends no signal.
    pub async fn wait_exit(&mut self, bound: Duration) -> ChildExit {
        wait_for_exit(&mut self.exit, bound).await
    }

    /// Kill a child that sent a valid frame at the wrong time, and wait for
    /// it.
    pub async fn discard_violating_child(&mut self) -> ChildLost {
        let _ = self.kill_group().await;
        let exit = self.wait_exit(self.kill_wait).await;
        self.lost(LossCause::ProtocolViolation, exit)
    }

    /// Ask a child with no task to exit. A child that exits by itself keeps
    /// any background process that a task started on purpose, so only a
    /// child that ignores the stop frame for [`CHILD_STOP_TIMEOUT`] is
    /// killed. Then the channel is closed.
    pub async fn retire(mut self) -> ChildExit {
        let sent = self.send(&ParentFrame::Stop).await;
        let exit = match sent {
            Ok(()) => self.wait_exit(CHILD_STOP_TIMEOUT).await,
            Err(_) => ChildExit::ExitUnknown,
        };
        if exit == ChildExit::ExitUnknown {
            tracing::warn!(
                pid = self.pid.get(),
                "a child did not exit {}s after the stop frame; killing it",
                CHILD_STOP_TIMEOUT.as_secs()
            );
            let _ = self.kill_group().await;
        }
        self.dispose().await
    }

    /// Wait for a child that said it ends itself once its last frame is
    /// written (its core stopped). No stop frame is sent: the child is
    /// already ending. A child that has not exited after
    /// [`CHILD_STOP_TIMEOUT`] is killed. Then the session of the child is
    /// killed and the channel closed, as for a child that ended by itself
    /// ([`Self::dispose_lost`]).
    pub async fn await_own_end(mut self) -> ChildExit {
        let exit = self.wait_exit(CHILD_STOP_TIMEOUT).await;
        if exit == ChildExit::ExitUnknown {
            tracing::warn!(
                pid = self.pid.get(),
                "a child that said it ends itself did not exit within {}s; killing it",
                CHILD_STOP_TIMEOUT.as_secs()
            );
        }
        self.dispose_lost().await
    }

    /// Close the socket and wait for the exit, bounded. A process that has
    /// not ended by then stays registered with the spawner, which reaps it
    /// whenever it ends.
    pub async fn dispose(self) -> ChildExit {
        let Self {
            pid,
            writer,
            reader_task,
            exit: mut exit_state,
            drop_kill,
            kill_wait,
            ..
        } = self;
        drop_kill.disarm();
        reader_task.abort();
        drop(writer);
        let exit = wait_for_exit(&mut exit_state, kill_wait).await;
        if exit == ChildExit::ExitUnknown {
            tracing::error!(
                pid = pid.get(),
                "a child did not end {}s after SIGKILL",
                kill_wait.as_secs_f64()
            );
        }
        exit
    }

    /// Kill a lost child with any subprocesses it left, then dispose of it.
    pub async fn dispose_lost(self) -> ChildExit {
        let _ = self.kill_group().await;
        self.dispose().await
    }

    /// Write raw bytes to the child, for tests of the reader rules.
    #[cfg(test)]
    pub(super) async fn send_raw(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.writer.write_all(bytes).await
    }
}
