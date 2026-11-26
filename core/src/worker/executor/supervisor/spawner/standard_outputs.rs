//! The standard output and the standard error that a child gets.
//!
//! Rule: a child gets on its fd 1 and fd 2 only the standard output and the
//! standard error that the worker had when the process loaded the core, or
//! `/dev/null`. It never gets the descriptor that is at number 1 or 2 when it
//! starts: a worker that closed a stream can have another descriptor at that
//! number (a socket, a pipe of the core, a file of the app).
//!
//! - At load: a load-time constructor (`.init_array` on Linux,
//!   `__mod_init_func` on macOS) reads the identity of fds 1 and 2 with
//!   `fstat`, before the core opens any descriptor: at the start of a Rust
//!   binary, before `main`; at the `dlopen` of a library that links the
//!   core. It holds no descriptor.
//! - At the start of the spawner ([`ChildOutputs::hold`]): for a stream that
//!   was open at load and is still the same file (device and inode), the
//!   spawner keeps a close-on-exec duplicate above the standard numbers for
//!   its life. Each child gets that duplicate on its fd 1 or 2 (`dup2` by
//!   the spawn), whatever close-on-exec flag the runtime of the worker put
//!   on the stream (Node puts it on both).
//! - `/dev/null`: a stream that was closed at load, that is closed at the
//!   start of the spawner, or that is another file then.

use std::io;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::process::Stdio;
use std::sync::OnceLock;

/// The lowest number of a duplicate that the spawner keeps: above the
/// standard streams.
const FIRST_DUPLICATE_DESCRIPTOR: RawFd = libc::STDERR_FILENO + 1;

/// The device and the inode of an open file: two descriptors with the same
/// identity refer to the same file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: libc::dev_t,
    inode: libc::ino_t,
}

/// One standard output of the process when it loaded the core.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamAtLoad {
    Open(FileIdentity),
    /// `fstat` gave `EBADF`.
    Closed,
    /// `fstat` failed for another cause.
    Unreadable {
        errno: i32,
    },
}

/// The standard outputs of the process when it loaded the core.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LoadRecord {
    stdout: StreamAtLoad,
    stderr: StreamAtLoad,
}

/// The record of this process. Only [`record_at_load`] writes it.
static AT_LOAD: OnceLock<LoadRecord> = OnceLock::new();

/// The load-time constructor: the OS loader calls it when it loads the
/// object that links the core, before `main` in a binary.
#[used]
#[cfg_attr(target_os = "linux", link_section = ".init_array")]
#[cfg_attr(
    target_os = "macos",
    link_section = "__DATA,__mod_init_func,mod_init_funcs"
)]
static RECORD_AT_LOAD: extern "C" fn() = record_at_load;

/// Record the standard outputs of the process. It calls only `fstat` and
/// does not panic: it runs before `main`.
extern "C" fn record_at_load() {
    let _record = AT_LOAD.get_or_init(|| LoadRecord {
        stdout: stream_at_load(libc::STDOUT_FILENO),
        stderr: stream_at_load(libc::STDERR_FILENO),
    });
}

/// The identity of the open descriptor `fd`.
///
/// Errors: the `io::Error` of `fstat` (`EBADF` for a closed number).
fn identity_of(fd: RawFd) -> io::Result<FileIdentity> {
    let mut stat = MaybeUninit::<libc::stat>::uninit();
    // SAFETY: fstat writes one `stat` into the buffer, which is live and of
    // that size for the call.
    match unsafe { libc::fstat(fd, stat.as_mut_ptr()) } {
        0 => {
            // SAFETY: fstat returned 0, so it wrote the whole struct.
            let stat = unsafe { stat.assume_init() };
            Ok(FileIdentity {
                device: stat.st_dev,
                inode: stat.st_ino,
            })
        }
        _ => Err(io::Error::last_os_error()),
    }
}

fn stream_at_load(number: RawFd) -> StreamAtLoad {
    match identity_of(number) {
        Ok(identity) => StreamAtLoad::Open(identity),
        Err(error) => match error.raw_os_error() {
            Some(libc::EBADF) => StreamAtLoad::Closed,
            Some(errno) => StreamAtLoad::Unreadable { errno },
            None => StreamAtLoad::Unreadable { errno: 0 },
        },
    }
}

/// Why a child gets `/dev/null` on a standard output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(
    clippy::enum_variant_names,
    reason = "each cause names when the stream had its state: at the load of the core or since it"
)]
pub(super) enum DevNullCause {
    /// The stream was closed when the process loaded the core.
    ClosedAtLoad,
    /// The stream could not be read when the process loaded the core.
    NotReadAtLoad { errno: i32 },
    /// The stream was open at load and is closed at the start of the
    /// spawner.
    ClosedSinceLoad,
    /// The number holds another file at the start of the spawner than at
    /// load.
    ReplacedSinceLoad,
}

/// What a child gets on one standard output.
#[derive(Debug)]
pub(super) enum ChildOutput {
    /// A close-on-exec duplicate of the stream that the process had at load.
    Stream(OwnedFd),
    DevNull {
        cause: DevNullCause,
    },
}

impl ChildOutput {
    /// The `Stdio` of one spawn: a new duplicate of the stream, or
    /// `/dev/null`.
    ///
    /// Errors: the `io::Error` of a duplicate that the OS refuses (for
    /// example `EMFILE`).
    pub(super) fn stdio(&self) -> io::Result<Stdio> {
        match self {
            Self::Stream(stream) => stream.try_clone().map(Stdio::from),
            Self::DevNull { .. } => Ok(Stdio::null()),
        }
    }
}

/// What each child gets on its fd 1 and fd 2. The spawner keeps it for its
/// life.
#[derive(Debug)]
pub(super) struct ChildOutputs {
    pub(super) stdout: ChildOutput,
    pub(super) stderr: ChildOutput,
}

impl ChildOutputs {
    /// The outputs of the children of this process, from the record at load
    /// and the streams now.
    ///
    /// Errors: the `io::Error` of a duplicate or an `fstat` that the OS
    /// refuses (for example `EMFILE`); no descriptor of the call stays open
    /// then.
    ///
    /// Panics: when the load-time constructor did not run, a defect of the
    /// build (the core supports Linux and macOS).
    pub(super) fn hold() -> io::Result<Self> {
        let record = AT_LOAD
            .get()
            .expect("the load-time constructor records the standard outputs of the process");
        Ok(Self {
            stdout: hold_output(libc::STDOUT_FILENO, record.stdout)?,
            stderr: hold_output(libc::STDERR_FILENO, record.stderr)?,
        })
    }

    /// Log each output for which the children get `/dev/null` though the
    /// worker had a stream at load.
    pub(super) fn log_lost_streams(&self) {
        for (name, output) in [("output", &self.stdout), ("error", &self.stderr)] {
            match output {
                ChildOutput::Stream(_)
                | ChildOutput::DevNull {
                    cause: DevNullCause::ClosedAtLoad,
                } => {}
                ChildOutput::DevNull { cause } => tracing::warn!(
                    "children get /dev/null as their standard {name}: the standard {name} \
                     of the worker is not the one that it had at its start ({cause:?})"
                ),
            }
        }
    }
}

/// The output of a child on the standard number `number`, whose stream was
/// `at_load`.
fn hold_output(number: RawFd, at_load: StreamAtLoad) -> io::Result<ChildOutput> {
    let recorded = match at_load {
        StreamAtLoad::Open(identity) => identity,
        StreamAtLoad::Closed => {
            return Ok(ChildOutput::DevNull {
                cause: DevNullCause::ClosedAtLoad,
            })
        }
        StreamAtLoad::Unreadable { errno } => {
            return Ok(ChildOutput::DevNull {
                cause: DevNullCause::NotReadAtLoad { errno },
            })
        }
    };
    let Some(duplicate) = duplicate_above_standard(number)? else {
        return Ok(ChildOutput::DevNull {
            cause: DevNullCause::ClosedSinceLoad,
        });
    };
    match identity_of(duplicate.as_raw_fd())? == recorded {
        true => Ok(ChildOutput::Stream(duplicate)),
        false => Ok(ChildOutput::DevNull {
            cause: DevNullCause::ReplacedSinceLoad,
        }),
    }
}

/// A close-on-exec duplicate of `number` above the standard numbers, or
/// `None` when the number has no descriptor (`EBADF`).
///
/// Errors: another `io::Error` of `fcntl`.
fn duplicate_above_standard(number: RawFd) -> io::Result<Option<OwnedFd>> {
    // SAFETY: fcntl with integer arguments; a number with no descriptor
    // gives EBADF.
    let duplicate =
        unsafe { libc::fcntl(number, libc::F_DUPFD_CLOEXEC, FIRST_DUPLICATE_DESCRIPTOR) };
    if duplicate != -1 {
        // SAFETY: fcntl gave a new descriptor that nothing else owns.
        return Ok(Some(unsafe { OwnedFd::from_raw_fd(duplicate) }));
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EBADF) => Ok(None),
        _ => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The test binary links the core: its constructor ran at the start of
    /// the binary.
    #[test]
    fn the_constructor_recorded_the_streams_at_load() {
        assert!(AT_LOAD.get().is_some());
    }

    #[test]
    fn a_stream_that_was_closed_at_load_gives_dev_null() {
        let output = hold_output(libc::STDOUT_FILENO, StreamAtLoad::Closed).unwrap();
        assert!(
            matches!(
                output,
                ChildOutput::DevNull {
                    cause: DevNullCause::ClosedAtLoad
                }
            ),
            "{output:?}"
        );
    }

    #[test]
    fn a_number_with_no_descriptor_now_gives_dev_null() {
        let identity = FileIdentity {
            device: 0,
            inode: 0,
        };
        let output = hold_output(RawFd::MAX, StreamAtLoad::Open(identity)).unwrap();
        assert!(
            matches!(
                output,
                ChildOutput::DevNull {
                    cause: DevNullCause::ClosedSinceLoad
                }
            ),
            "{output:?}"
        );
    }

    #[test]
    fn the_same_file_is_kept_and_another_file_gives_dev_null() {
        let (socket, _peer) = std::os::unix::net::UnixStream::pair().unwrap();
        let number = socket.as_raw_fd();
        let same = hold_output(number, stream_at_load(number)).unwrap();
        assert!(matches!(same, ChildOutput::Stream(_)), "{same:?}");
        let other = std::os::unix::net::UnixStream::pair().unwrap().0;
        let other_identity = identity_of(other.as_raw_fd()).unwrap();
        let replaced = hold_output(number, StreamAtLoad::Open(other_identity)).unwrap();
        assert!(
            matches!(
                replaced,
                ChildOutput::DevNull {
                    cause: DevNullCause::ReplacedSinceLoad
                }
            ),
            "{replaced:?}"
        );
    }
}
