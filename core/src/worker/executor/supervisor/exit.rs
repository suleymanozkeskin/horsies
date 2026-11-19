//! How a child process ended, and what its exit code means in the protocol.

use std::fmt;

use crate::worker::executor::protocol::{ChildExitCode, ProcessId, CORE_STOPPED_EXIT_CODE};

/// The exit of a child that the spawner thread reaped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReapedExit {
    /// The child called `exit` with this code.
    ExitedWith(i32),
    /// A signal ended the child.
    KilledBySignal(i32),
}

impl ReapedExit {
    /// Read a `waitpid` status. `None`: the status is neither an exit nor a
    /// signal end (a stop or a continue, which the spawner never asks for).
    pub(super) fn from_wait_status(status: libc::c_int) -> Option<Self> {
        match (libc::WIFEXITED(status), libc::WIFSIGNALED(status)) {
            (true, _) => Some(Self::ExitedWith(libc::WEXITSTATUS(status))),
            (false, true) => Some(Self::KilledBySignal(libc::WTERMSIG(status))),
            (false, false) => None,
        }
    }

    /// What this end means in the protocol.
    pub fn meaning(self) -> ExitMeaning {
        match self {
            Self::ExitedWith(0) => ExitMeaning::Clean,
            Self::ExitedWith(code) => match ChildExitCode::from_code(code) {
                Some(known) => ExitMeaning::Protocol(known),
                None => ExitMeaning::OtherCode(code),
            },
            Self::KilledBySignal(signal) => ExitMeaning::Signal(signal),
        }
    }

    /// Whether this is the end of a `SIGKILL`.
    pub fn is_sigkill(self) -> bool {
        matches!(self, Self::KilledBySignal(libc::SIGKILL))
    }
}

impl fmt::Display for ReapedExit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExitedWith(code) => write!(f, "exit code {code}"),
            Self::KilledBySignal(signal) => match signal_name(*signal) {
                Some(name) => write!(f, "signal {signal} ({name})"),
                None => write!(f, "signal {signal}"),
            },
        }
    }
}

/// The meaning of a reaped exit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExitMeaning {
    /// Exit code 0: the child stopped after a stop frame.
    Clean,
    /// A code of the protocol ([`ChildExitCode`]).
    Protocol(ChildExitCode),
    /// A code outside the protocol: task code or a library called `exit`.
    OtherCode(i32),
    /// A signal ended the child.
    Signal(i32),
}

/// What the parent knows about the end of a child process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChildExit {
    /// The spawner thread reaped the child.
    Reaped(ReapedExit),
    /// The child ended, but another waiter in this process took its exit
    /// status, so the code is not known ([`ExitState::ReapedElsewhere`]).
    ReapedElsewhere,
    /// No exit was seen within the wait after a `SIGKILL`.
    ExitUnknown,
}

impl fmt::Display for ChildExit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reaped(exit) => exit.fmt(f),
            Self::ReapedElsewhere => f.write_str("the exit status was taken by another waiter"),
            Self::ExitUnknown => f.write_str("no exit seen after SIGKILL"),
        }
    }
}

/// How a child that held no task ended by itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EndWithoutTask {
    /// Exit code [`ChildExitCode::CoreStopped`]: the core of the child
    /// stopped, and the child ended itself after it reported any start it
    /// refused. The slot logs it at error level and boots a new child.
    CoreStopped,
    /// Any other end. The slot replaces the child with no log of its own.
    Other,
}

impl EndWithoutTask {
    /// The reading of `exit`. Pure.
    pub fn of(exit: ChildExit) -> Self {
        let reaped = match exit {
            ChildExit::Reaped(reaped) => reaped,
            ChildExit::ReapedElsewhere | ChildExit::ExitUnknown => return Self::Other,
        };
        match reaped.meaning() {
            ExitMeaning::Protocol(ChildExitCode::CoreStopped) => Self::CoreStopped,
            ExitMeaning::Protocol(
                ChildExitCode::ParentGone
                | ChildExitCode::BadParentFrame
                | ChildExitCode::BadArguments
                | ChildExitCode::StoppedWithExecutorThreads
                | ChildExitCode::BootFailed
                | ChildExitCode::ReparentFailed
                | ChildExitCode::ProtocolMismatch
                | ChildExitCode::ChildHookFailed,
            )
            | ExitMeaning::Clean
            | ExitMeaning::OtherCode(_)
            | ExitMeaning::Signal(_) => Self::Other,
        }
    }
}

/// Log a child that ended with [`ChildExitCode::CoreStopped`] while it held
/// no task: error level, with the cause. The slot then boots a new child.
pub fn log_core_stopped_child(pid: ProcessId) {
    tracing::error!(
        pid = pid.get(),
        exit_code = CORE_STOPPED_EXIT_CODE,
        "child {} ended itself with exit code {CORE_STOPPED_EXIT_CODE}: its core stopped (a panic \
         in a core background task of the child, or a fork after its core started; the last \
         log line of the child names the cause). It held no task; a new child replaces it",
        pid.get(),
    );
}

/// The process state of a spawned child, as the spawner thread publishes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitState {
    /// The process has not been reaped.
    Running,
    /// The spawner thread reaped the process.
    Reaped(ReapedExit),
    /// Another waiter in this process took the exit status (`waitpid`
    /// answered `ECHILD`). The exit is not known. Only a defect elsewhere in
    /// the process leads here: the spawner thread is the one owner of child
    /// reaping.
    ReapedElsewhere,
}

/// The names of the signals a child most often ends with.
fn signal_name(signal: i32) -> Option<&'static str> {
    match signal {
        libc::SIGHUP => Some("SIGHUP"),
        libc::SIGINT => Some("SIGINT"),
        libc::SIGILL => Some("SIGILL"),
        libc::SIGABRT => Some("SIGABRT"),
        libc::SIGFPE => Some("SIGFPE"),
        libc::SIGKILL => Some("SIGKILL"),
        libc::SIGBUS => Some("SIGBUS"),
        libc::SIGSEGV => Some("SIGSEGV"),
        libc::SIGPIPE => Some("SIGPIPE"),
        libc::SIGTERM => Some("SIGTERM"),
        _ => None,
    }
}
