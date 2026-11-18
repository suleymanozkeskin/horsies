//! Exit codes of child processes: part of the protocol. The supervisor reads
//! them to name how a child ended; the Python child and the Rust child host
//! exit with them. Codes 77 and 79 belong to the worker process itself
//! ([`crate::worker::runner_beats::RUNNER_HEARTBEAT_LOST_EXIT_CODE`],
//! [`crate::worker::liveness::LIVENESS_LOCK_LOST_EXIT_CODE`]); no child uses
//! them.

/// The parent is gone: the socket reached EOF, or the parent-death check
/// found another parent.
pub const PARENT_GONE_EXIT_CODE: i32 = 71;
/// The parent sent a frame over the bounds, or a malformed frame.
pub const BAD_PARENT_FRAME_EXIT_CODE: i32 = 72;
/// The child program got arguments it cannot use.
pub const BAD_ARGUMENTS_EXIT_CODE: i32 = 73;
/// After a stop frame, threads of the task loop's executor still ran; the
/// child left without waiting for them.
pub const STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE: i32 = 74;
/// The boot failed, after a `boot_failed` frame that names the cause.
pub const BOOT_FAILED_EXIT_CODE: i32 = 75;
/// A forked child did not reach the worker as its parent in time.
pub const REPARENT_FAILED_EXIT_CODE: i32 = 76;
/// The init frame named another protocol version than the child speaks.
pub const PROTOCOL_MISMATCH_EXIT_CODE: i32 = 78;
/// A start hook of the child failed or hung.
pub const CHILD_HOOK_FAILED_EXIT_CODE: i32 = 173;

/// An exit code with a meaning in the protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChildExitCode {
    ParentGone,
    BadParentFrame,
    BadArguments,
    StoppedWithExecutorThreads,
    BootFailed,
    ReparentFailed,
    ProtocolMismatch,
    ChildHookFailed,
}

impl ChildExitCode {
    /// Every code of the protocol, for checks over the whole set.
    pub const ALL: [Self; 8] = [
        Self::ParentGone,
        Self::BadParentFrame,
        Self::BadArguments,
        Self::StoppedWithExecutorThreads,
        Self::BootFailed,
        Self::ReparentFailed,
        Self::ProtocolMismatch,
        Self::ChildHookFailed,
    ];

    pub fn code(self) -> i32 {
        match self {
            Self::ParentGone => PARENT_GONE_EXIT_CODE,
            Self::BadParentFrame => BAD_PARENT_FRAME_EXIT_CODE,
            Self::BadArguments => BAD_ARGUMENTS_EXIT_CODE,
            Self::StoppedWithExecutorThreads => STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE,
            Self::BootFailed => BOOT_FAILED_EXIT_CODE,
            Self::ReparentFailed => REPARENT_FAILED_EXIT_CODE,
            Self::ProtocolMismatch => PROTOCOL_MISMATCH_EXIT_CODE,
            Self::ChildHookFailed => CHILD_HOOK_FAILED_EXIT_CODE,
        }
    }

    /// The meaning of `code`, or `None` for a code outside the protocol.
    pub fn from_code(code: i32) -> Option<Self> {
        Self::ALL.into_iter().find(|known| known.code() == code)
    }
}
