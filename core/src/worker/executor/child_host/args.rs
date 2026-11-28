//! The arguments of child mode: `<binary> --horsies-child <fd> <worker pid>`.
//! The supervisor appends the descriptor number of the child's socket and
//! the worker's pid, as for the Python child program.

use std::ffi::{OsStr, OsString};
use std::os::fd::RawFd;

use crate::worker::executor::protocol::ProcessId;

/// The first argument that starts the app program again in child mode.
pub const CHILD_MODE_ARGUMENT: &str = "--horsies-child";

/// The arguments of a child.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChildArgs {
    pub(super) socket_fd: RawFd,
    pub(super) worker_pid: ProcessId,
}

impl ChildArgs {
    /// The descriptor number of the child's socket, as the supervisor
    /// passed it. `run_child_host` closes the socket when it returns. A host
    /// whose process ends later keeps a duplicate open until its end, so the
    /// parent reads the exit and not an end of the socket before it. Pure.
    pub fn socket_fd(&self) -> RawFd {
        self.socket_fd
    }
}

/// Why child-mode arguments cannot be used. The child exits with the
/// bad-arguments code.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChildArgsError {
    #[error("child mode needs the socket descriptor and the worker pid")]
    Missing,
    #[error("the socket descriptor '{found}' is not a descriptor number")]
    SocketNotADescriptor { found: String },
    #[error("the worker pid '{found}' is not a pid")]
    WorkerPidNotAPid { found: String },
    #[error("child mode takes two arguments after {CHILD_MODE_ARGUMENT}; '{found}' is one more")]
    ExtraArgument { found: String },
}

/// How a worker binary was started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChildMode {
    /// Not in child mode: the binary runs as the worker or another command.
    Worker,
    /// Child mode: serve as a Rust child.
    Child(ChildArgs),
    /// Child mode with arguments that cannot be used.
    BadArguments(ChildArgsError),
}

fn text(argument: &OsStr) -> String {
    argument.to_string_lossy().into_owned()
}

/// Read the process arguments, the program name first.
pub fn child_mode<I: IntoIterator<Item = OsString>>(arguments: I) -> ChildMode {
    let mut arguments = arguments.into_iter().skip(1);
    match arguments.next() {
        Some(first) if first.as_os_str() == OsStr::new(CHILD_MODE_ARGUMENT) => {
            match parse_child_args(arguments) {
                Ok(args) => ChildMode::Child(args),
                Err(error) => ChildMode::BadArguments(error),
            }
        }
        Some(_) | None => ChildMode::Worker,
    }
}

fn parse_child_args(
    mut arguments: impl Iterator<Item = OsString>,
) -> Result<ChildArgs, ChildArgsError> {
    let (Some(fd), Some(pid)) = (arguments.next(), arguments.next()) else {
        return Err(ChildArgsError::Missing);
    };
    if let Some(extra) = arguments.next() {
        return Err(ChildArgsError::ExtraArgument {
            found: text(&extra),
        });
    }
    let socket_fd = fd
        .to_str()
        .and_then(|fd| fd.parse::<RawFd>().ok())
        .filter(|fd| *fd >= 0)
        .ok_or_else(|| ChildArgsError::SocketNotADescriptor { found: text(&fd) })?;
    let worker_pid = pid
        .to_str()
        .and_then(|pid| pid.parse::<u32>().ok())
        .and_then(ProcessId::new)
        .ok_or_else(|| ChildArgsError::WorkerPidNotAPid { found: text(&pid) })?;
    Ok(ChildArgs {
        socket_fd,
        worker_pid,
    })
}
