//! The supervisor of child processes: start, watch, signal and reap them,
//! read their frames, and name how each one ended.
//!
//! - [`spawner`]: the spawner thread, the one owner of every spawn, kill
//!   and reap. It publishes each child's [`exit::ExitState`].
//! - [`socket`]: the socket pair of a child, with explicit buffer sizes.
//! - [`channel`]: the parent side of one child: the reader task, the frame
//!   writer, and the verdict rules after an exit, an EOF or a bad frame.
//! - [`exit`]: reaped exits and their meaning in the protocol.
//! - [`boot`]: boot failure causes and the restart rules of a slot.
//!
//! The preload fork path (a child forked from the Python preload process)
//! is not here: it needs the preload process, which the Python bridge
//! starts. Every child of this module comes from `exec`.

pub mod boot;
pub mod channel;
pub mod exit;
mod exit_watch;
pub mod socket;
pub mod spawner;

#[cfg(test)]
mod child_io_tests;
#[cfg(test)]
mod tests;

pub use boot::{
    boot_end_of, outside_kill_delay, slot_down_delay, BootCounts, BootDecision, BootEnd,
    BootFailure, BootFailureCause, BootReply, CauseKind, MismatchSeen, PoolAtFailure,
    CHILD_BOOT_ATTEMPTS, CHILD_READY_TIMEOUT,
};
pub use channel::{
    ChildChannel, ChildLost, FrameWait, LossCause, MailboxBound, PartialFrame, SendFailure,
    WorkerKillMark, CHILD_KILL_WAIT, CHILD_STOP_TIMEOUT,
};
pub use exit::{ChildExit, ExitMeaning, ExitState, ReapedExit};
pub use exit_watch::POLLED_EXIT_INTERVAL;
pub use socket::{child_socket_pair, ChildSocketPair, SOCKET_BUFFER_BYTES};
pub use spawner::{
    wait_for_exit, ChildCommand, ChildId, KillOutcome, SpawnError, SpawnedChild, Spawner,
    SpawnerHandle, SpawnerStartError, SpawnerStopReport,
};
