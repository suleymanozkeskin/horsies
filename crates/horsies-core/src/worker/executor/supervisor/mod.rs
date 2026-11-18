//! The supervisor of child processes: start, watch, signal and reap them,
//! read their frames, and name how each one ended.
//!
//! - [`spawner`]: the spawner thread, the one owner of every spawn, kill
//!   and reap. It publishes each child's [`exit::ExitState`]. On Linux it
//!   also runs the preload process and forks children from it.
//! - [`socket`]: the socket pair of a child, with explicit buffer sizes.
//! - [`channel`]: the parent side of one child: the reader task, the frame
//!   writer, and the verdict rules after an exit, an EOF or a bad frame.
//! - [`exit`]: reaped exits and their meaning in the protocol.
//! - [`boot`]: boot failure causes and the restart rules of a slot.

pub mod boot;
pub mod channel;
pub mod exit;
mod exit_watch;
pub mod socket;
pub mod source;
pub mod spawner;

#[cfg(test)]
mod child_io_tests;
#[cfg(test)]
mod tests;

pub use boot::{
    boot_end_of, outside_kill_delay, slot_down_delay, BootCounts, BootDecision, BootEnd,
    BootFailure, BootFailureCause, BootReply, CauseKind, LastRecord, MismatchSeen, PoolAtFailure,
    CHILD_BOOT_ATTEMPTS, CHILD_READY_TIMEOUT,
};
pub use channel::{
    ChildChannel, ChildLost, FrameWait, LossCause, MailboxBound, PartialFrame, SendFailure,
    WorkerKillMark, CHILD_KILL_WAIT, CHILD_STOP_TIMEOUT,
};
pub use exit::{ChildExit, ExitMeaning, ExitState, ReapedExit};
pub use exit_watch::POLLED_EXIT_INTERVAL;
pub use socket::{child_socket_pair, ChildSocketPair, SOCKET_BUFFER_BYTES};
pub use source::{ChildSource, SpawnFuture};
pub use spawner::{
    wait_for_exit, ChildCommand, ChildId, KillOutcome, PreloadMode, PreloadPlan, PreloadStart,
    PreloadUnsupported, SpawnError, SpawnedChild, Spawner, SpawnerHandle, SpawnerStartError,
    SpawnerStopReport, ORPHAN_SWEEP_LIMIT, PRELOAD_FORK_TIMEOUT, PRELOAD_READY_TIMEOUT,
    PRELOAD_RESTART_DELAY, PRELOAD_RESTART_LIMIT, PRELOAD_STOP_TIMEOUT,
};
