//! Boot failure causes and the restart rules of a slot (Python
//! `core/worker/slots.py`, `_boot_child`; `core/models/boot_failure.py`).
//!
//! Pure: the slot keeps the counts, and these functions decide.

use std::fmt;
use std::num::NonZeroU32;
use std::time::Duration;

use super::channel::{ChildLost, LossCause};
use super::exit::{ChildExit, ReapedExit};
use crate::core::boot_failure::MAX_BOOT_FAILURE_DETAIL_CHARS;
use crate::worker::executor::protocol::{
    BootFailedFrame, BoundedText, ChildExitCode, ReceivedProtocol, ReportedBootCause,
    ShortageErrno, PROTOCOL_VERSION,
};

/// Counted attempts before the pool fails.
pub const CHILD_BOOT_ATTEMPTS: u32 = 3;
/// The wait before the next counted attempt, times the count.
pub const CHILD_BOOT_RETRY_DELAY: Duration = Duration::from_millis(500);
/// The wait before the next boot after an outside kill, times the kills in a
/// row, at most [`CHILD_BOOT_KILL_DELAY_MAX`].
pub const CHILD_BOOT_KILL_DELAY_STEP: Duration = Duration::from_millis(500);
pub const CHILD_BOOT_KILL_DELAY_MAX: Duration = Duration::from_secs(5);
/// The longest wait for the ready frame of a booting child.
pub const CHILD_READY_TIMEOUT: Duration = Duration::from_secs(120);
/// A slot that is down boots again after this wait, doubled for each failed
/// boot after the first, at most [`SLOT_DOWN_MAX_DELAY`].
pub const SLOT_DOWN_FIRST_DELAY: Duration = Duration::from_secs(1);
pub const SLOT_DOWN_MAX_DELAY: Duration = Duration::from_secs(60);
/// The doubling stops here: 1 s * 2^6 = 64 s is past the cap.
const SLOT_DOWN_DELAY_DOUBLINGS: u32 = 6;
/// The detail of a boot failure, at most
/// [`MAX_BOOT_FAILURE_DETAIL_CHARS`] characters.
pub type BootFailureDetail = BoundedText<MAX_BOOT_FAILURE_DETAIL_CHARS>;

/// How the parent saw that the child speaks another protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MismatchSeen {
    /// The ready frame named this version.
    ReadyNamed(ReceivedProtocol),
    /// The child exited with the protocol-mismatch code after the init
    /// frame. The child logs its own version.
    ChildRefusedInit,
}

/// Why one boot ended before the child was ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootFailureCause {
    /// Repeats: the app or a task module could not be imported.
    AppLoadFailed,
    /// Repeats: the child could not set up its connection pool.
    ChildPoolSetupFailed,
    /// Repeats: the child could not create the event loop for async tasks.
    TaskLoopCreateFailed,
    /// Repeats: the OS refused to start the child, or a forked child did not
    /// reach the worker.
    SpawnFailed,
    /// Repeats: the child ended before its ready frame and sent no boot
    /// failure frame.
    LostBeforeReady { loss: LossCause, exit: ChildExit },
    /// Repeats: no frame within [`CHILD_READY_TIMEOUT`].
    NotReadyInTime,
    /// Repeats: the child speaks another protocol version. A package upgrade
    /// under a running worker gives this; the process supervisor restarts the
    /// worker with the new code.
    ProtocolMismatch(MismatchSeen),
    /// Can clear: a boot step failed with an errno of a resource limit.
    ResourceShortage(ShortageErrno),
    /// Can clear: the OS refused a thread of the child.
    ThreadStartRefused,
}

/// Whether a cause is the same on every boot of the same code and settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CauseKind {
    Repeats,
    CanClear,
}

impl BootFailureCause {
    pub fn kind(self) -> CauseKind {
        match self {
            Self::AppLoadFailed
            | Self::ChildPoolSetupFailed
            | Self::TaskLoopCreateFailed
            | Self::SpawnFailed
            | Self::LostBeforeReady { .. }
            | Self::NotReadyInTime
            | Self::ProtocolMismatch(_) => CauseKind::Repeats,
            Self::ResourceShortage(_) | Self::ThreadStartRefused => CauseKind::CanClear,
        }
    }
}

impl From<ReportedBootCause> for BootFailureCause {
    fn from(cause: ReportedBootCause) -> Self {
        match cause {
            ReportedBootCause::AppLoadFailed => Self::AppLoadFailed,
            ReportedBootCause::ChildPoolSetupFailed => Self::ChildPoolSetupFailed,
            ReportedBootCause::ResourceShortage(errno) => Self::ResourceShortage(errno),
            ReportedBootCause::TaskLoopCreateFailed => Self::TaskLoopCreateFailed,
            ReportedBootCause::ThreadStartRefused => Self::ThreadStartRefused,
        }
    }
}

impl fmt::Display for BootFailureCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AppLoadFailed => f.write_str("the app or a task module could not be imported"),
            Self::ChildPoolSetupFailed => {
                f.write_str("the child could not set up its database connection pool")
            }
            Self::TaskLoopCreateFailed => {
                f.write_str("the child could not create the event loop for async tasks")
            }
            Self::SpawnFailed => f.write_str("the OS refused to start the child process"),
            Self::LostBeforeReady { loss, exit } => {
                write!(f, "the child ended before it was ready ({loss}: {exit})")
            }
            Self::NotReadyInTime => f.write_str("the child was not ready in time"),
            Self::ProtocolMismatch(MismatchSeen::ReadyNamed(received)) => write!(
                f,
                "the child speaks protocol {received}; this worker speaks version \
                 {PROTOCOL_VERSION}"
            ),
            Self::ProtocolMismatch(MismatchSeen::ChildRefusedInit) => write!(
                f,
                "the child refused protocol version {PROTOCOL_VERSION} of this worker"
            ),
            Self::ResourceShortage(errno) => {
                write!(f, "the OS ran short of a resource ({})", errno.as_str())
            }
            Self::ThreadStartRefused => f.write_str("the OS refused a thread of the child"),
        }
    }
}

/// One boot that ended before the child was ready.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootFailure {
    pub cause: BootFailureCause,
    pub detail: BootFailureDetail,
}

impl BootFailure {
    pub fn new(cause: BootFailureCause, detail: &str) -> Self {
        Self {
            cause,
            detail: BoundedText::cut(detail),
        }
    }
}

impl fmt::Display for BootFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.cause, self.detail)
    }
}

/// What the parent saw in place of a ready frame.
#[derive(Debug, Clone, PartialEq)]
pub enum BootReply {
    /// The child reported why its boot failed.
    BootFailed(BootFailedFrame),
    /// The child ended, or was ended, with no ready frame. A ready frame of
    /// another protocol version ends the channel this way too
    /// ([`LossCause::ProtocolMismatch`]).
    Lost(ChildLost),
    /// No frame within [`CHILD_READY_TIMEOUT`].
    NotReadyInTime,
}

/// How a boot ended when it did not give a ready child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootEnd {
    /// The pool decides with the restart rules.
    Failed(BootFailure),
    /// A start hook of the child failed or hung: the pool fails at once,
    /// with no restart loop.
    ChildHookFailed,
}

/// Name the cause of a boot that ended with `reply` in place of a ready
/// frame (Python `boot_failure_of`). A child with no frame is read by its
/// exit code: the hook code fails the pool, the reparent code is a spawn
/// failure, the mismatch code is a protocol mismatch, and any other end is
/// `LostBeforeReady`.
pub fn boot_end_of(reply: BootReply) -> BootEnd {
    match reply {
        BootReply::BootFailed(frame) => BootEnd::Failed(BootFailure::new(
            frame.cause.into(),
            &format!("{}: {}", frame.error_name, frame.detail),
        )),
        BootReply::NotReadyInTime => BootEnd::Failed(BootFailure::new(
            BootFailureCause::NotReadyInTime,
            &format!("no ready frame after {}s", CHILD_READY_TIMEOUT.as_secs()),
        )),
        BootReply::Lost(lost) => boot_end_of_loss(&lost),
    }
}

fn boot_end_of_loss(lost: &ChildLost) -> BootEnd {
    let exit_code = match lost.exit {
        ChildExit::Reaped(ReapedExit::ExitedWith(code)) => ChildExitCode::from_code(code),
        ChildExit::Reaped(ReapedExit::KilledBySignal(_))
        | ChildExit::ReapedElsewhere
        | ChildExit::ExitUnknown => None,
    };
    let cause = match (lost.cause, exit_code) {
        (LossCause::ProtocolMismatch(received), _) => {
            BootFailureCause::ProtocolMismatch(MismatchSeen::ReadyNamed(received))
        }
        (_, Some(ChildExitCode::ChildHookFailed)) => return BootEnd::ChildHookFailed,
        (_, Some(ChildExitCode::ReparentFailed)) => BootFailureCause::SpawnFailed,
        (_, Some(ChildExitCode::ProtocolMismatch)) => {
            BootFailureCause::ProtocolMismatch(MismatchSeen::ChildRefusedInit)
        }
        (
            _,
            Some(
                ChildExitCode::ParentGone
                | ChildExitCode::BadParentFrame
                | ChildExitCode::BadArguments
                | ChildExitCode::StoppedWithExecutorThreads
                | ChildExitCode::BootFailed,
            )
            | None,
        ) => BootFailureCause::LostBeforeReady {
            loss: lost.cause,
            exit: lost.exit,
        },
    };
    BootEnd::Failed(BootFailure::new(cause, &lost.to_string()))
}

/// The wait before the next boot after `kills_in_a_row` outside kills.
pub fn outside_kill_delay(kills_in_a_row: u32) -> Duration {
    CHILD_BOOT_KILL_DELAY_STEP
        .saturating_mul(kills_in_a_row)
        .min(CHILD_BOOT_KILL_DELAY_MAX)
}

/// The wait before the next boot of a slot that is down after
/// `failed_boots` failed boots.
pub fn slot_down_delay(failed_boots: NonZeroU32) -> Duration {
    let doublings = (failed_boots.get() - 1).min(SLOT_DOWN_DELAY_DOUBLINGS);
    SLOT_DOWN_FIRST_DELAY
        .saturating_mul(1_u32 << doublings)
        .min(SLOT_DOWN_MAX_DELAY)
}

/// What the rest of the pool looks like at a failed boot of one slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolAtFailure {
    /// Every slot booted once or is down, and at least one booted.
    pub first_generation_in: bool,
    /// Another slot holds a ready child (idle, busy, open or closing).
    pub another_child_serves: bool,
    /// Every other slot is down.
    pub every_other_slot_down: bool,
}

/// What the slot does after a failed boot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootDecision {
    /// A `SIGKILL` from outside while another child serves: the attempt does
    /// not count. Boot again after `wait`.
    OutsideKillSpared { kills_in_a_row: u32, wait: Duration },
    /// A cause that repeats counted one attempt. Boot again after `wait`.
    Retry { counted: u32, wait: Duration },
    /// A cause that can clear: the slot is down. It counted only while every
    /// other slot was down; otherwise the count started again. Boot again
    /// after `wait`.
    SlotDown {
        counted: u32,
        failed_boots: NonZeroU32,
        wait: Duration,
    },
    /// The last counted attempt failed. The slot records it as it records
    /// every counted attempt (`last`), then the pool fails with
    /// `BootAttemptsExhausted`.
    Exhausted { attempts: u32, last: LastRecord },
}

/// How a slot records its last counted attempt, before the pool fails
/// (Python `_boot_child`: `_record_failed_boot` runs before the check of the
/// bound).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LastRecord {
    /// A cause that repeats: the attempt failed, as at a
    /// [`BootDecision::Retry`].
    AttemptFailed,
    /// A cause that can clear: the slot is down, as at a
    /// [`BootDecision::SlotDown`]. `wait` is the wait that the log line
    /// names; no boot follows.
    SlotDown {
        failed_boots: NonZeroU32,
        wait: Duration,
    },
}

/// The boot counts of one slot, kept across its failed boots until a child
/// is ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BootCounts {
    counted: u32,
    outside_kills: u32,
}

impl BootCounts {
    /// Decide what follows a failed boot (Python `_boot_child`).
    ///
    /// `failed_boots_while_down` is the failed boots of the slot's current
    /// down state, 0 when the slot is not down.
    pub fn after_failure(
        &mut self,
        cause: BootFailureCause,
        pool: PoolAtFailure,
        failed_boots_while_down: u32,
    ) -> BootDecision {
        if outside_kill_spares(cause, pool) {
            self.outside_kills = self.outside_kills.saturating_add(1);
            return BootDecision::OutsideKillSpared {
                kills_in_a_row: self.outside_kills,
                wait: outside_kill_delay(self.outside_kills),
            };
        }
        let counts = match cause.kind() {
            CauseKind::Repeats => true,
            CauseKind::CanClear => pool.every_other_slot_down,
        };
        self.counted = match counts {
            true => self.counted.saturating_add(1),
            false => 0,
        };
        let exhausted = self.counted >= CHILD_BOOT_ATTEMPTS;
        let failed_boots = NonZeroU32::MIN.saturating_add(failed_boots_while_down);
        match (exhausted, cause.kind()) {
            (true, CauseKind::Repeats) => BootDecision::Exhausted {
                attempts: self.counted,
                last: LastRecord::AttemptFailed,
            },
            (true, CauseKind::CanClear) => BootDecision::Exhausted {
                attempts: self.counted,
                last: LastRecord::SlotDown {
                    failed_boots,
                    wait: slot_down_delay(failed_boots),
                },
            },
            (false, CauseKind::Repeats) => BootDecision::Retry {
                counted: self.counted,
                wait: CHILD_BOOT_RETRY_DELAY.saturating_mul(self.counted),
            },
            (false, CauseKind::CanClear) => BootDecision::SlotDown {
                counted: self.counted,
                failed_boots,
                wait: slot_down_delay(failed_boots),
            },
        }
    }
}

/// A `SIGKILL` from outside says nothing about the code, so it does not stop
/// a worker with healthy tasks. It still counts at the first start, and
/// when no other child serves: then a worker that does no work must fail
/// where its supervisor sees it.
fn outside_kill_spares(cause: BootFailureCause, pool: PoolAtFailure) -> bool {
    match cause {
        BootFailureCause::LostBeforeReady {
            exit: ChildExit::Reaped(exit),
            ..
        } => exit.is_sigkill() && pool.first_generation_in && pool.another_child_serves,
        _ => false,
    }
}
