//! The exit rule of a JavaScript child.
//!
//! - The child host blocks the thread that runs it, and the JavaScript main
//!   thread must stay free to run the bodies. So the host runs on a thread of
//!   its own (`horsies-child-host`).
//! - When the host returns its exit, the thread resolves the serve's Promise
//!   with the code. The package awaits its telemetry flush, at most its flush
//!   bound (the plan gives it), then calls `process.exit(code)` on the
//!   JavaScript thread.
//! - The backstop: when the process still runs at the flush bound plus
//!   [`CHILD_EXIT_BACKSTOP_MARGIN`] after the host returned (the JavaScript
//!   thread is blocked, for example by a start hook past its bound), the host
//!   thread ends the process with the same code: the drain of Rust's
//!   standard output and standard error, then `_exit`
//!   (`ProcessExit::Immediate` with `ExitDrain::OutputOnly`). So a hook that
//!   hangs still gives 173.
//! - The socket: the core's host closes the child's socket when it returns,
//!   and a parent that reads the end of a child's socket waits only
//!   `CHANNEL_LOST_GRACE` (200 ms) for the exit, then kills the child. So the
//!   host thread keeps a duplicate of the socket ([`hold_socket`]) that only
//!   the end of the process closes: the parent reads the end of the socket
//!   with the exit, and its grace is only the wait for the reap.
//! - The order of the bounds after a start hook that ran past its bound: the
//!   hook ran `CHILD_START_HOOK_BOUND` (10 s); after the host returned,
//!   `process.exit` comes within the flush bound (at most
//!   [`MAX_CHILD_EXIT_FLUSH_BOUND`], 8 s), or the backstop starts at the
//!   flush bound plus the margin (1 s) and drains for at most
//!   `EXIT_DRAIN_BOUND` (2 s). The sum, 21 s, is inside the parent's wait for
//!   the ready frame (`CHILD_READY_TIMEOUT`, 120 s). After a stop frame, the
//!   backstop starts inside the parent's wait for the exit
//!   (`CHILD_STOP_TIMEOUT`, 10 s).
//! - The core's own exits from its threads (the end of the parent, a bad
//!   parent frame) use the same immediate exit (`ChildApp::exit`): the exit
//!   handlers of Node and Bun can crash while the JavaScript thread runs
//!   code, and the process would then end by a signal in place of its code.

use std::io;
use std::os::fd::{FromRawFd, OwnedFd, RawFd};
use std::thread::JoinHandle;
use std::time::Duration;

use horsies_core::{
    ChildHostExit, ExitDrain, ProcessExit, CHILD_READY_TIMEOUT, CHILD_START_HOOK_BOUND,
    CHILD_STOP_TIMEOUT, EXIT_DRAIN_BOUND,
};

/// The time past the package's flush bound that `process.exit` gets before
/// the host thread ends the process.
pub const CHILD_EXIT_BACKSTOP_MARGIN: Duration = Duration::from_secs(1);

/// The longest flush bound that a plan may give: the backstop must start
/// inside the parent's wait after a stop frame.
pub const MAX_CHILD_EXIT_FLUSH_BOUND: Duration = Duration::from_secs(8);

const _: () = assert!(
    MAX_CHILD_EXIT_FLUSH_BOUND.as_millis() + CHILD_EXIT_BACKSTOP_MARGIN.as_millis()
        < CHILD_STOP_TIMEOUT.as_millis(),
    "the backstop starts inside the parent's wait after a stop frame",
);

const _: () = assert!(
    CHILD_START_HOOK_BOUND.as_millis()
        + MAX_CHILD_EXIT_FLUSH_BOUND.as_millis()
        + CHILD_EXIT_BACKSTOP_MARGIN.as_millis()
        + EXIT_DRAIN_BOUND.as_millis()
        < CHILD_READY_TIMEOUT.as_millis(),
    "a child whose start hook ran past its bound ends inside the parent's wait for the ready frame",
);

/// The lowest number of the duplicate of the socket: above the standard
/// streams.
const FIRST_DUPLICATE_DESCRIPTOR: RawFd = libc::STDERR_FILENO + 1;

/// The flush bound of a plan, in milliseconds, at most
/// [`MAX_CHILD_EXIT_FLUSH_BOUND`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlushBound(Duration);

/// A flush bound above [`MAX_CHILD_EXIT_FLUSH_BOUND`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlushBoundTooLong;

impl FlushBound {
    /// The bound of `milliseconds`, or the refusal of a bound past the
    /// largest. Pure.
    pub fn of_millis(milliseconds: u32) -> Result<Self, FlushBoundTooLong> {
        let bound = Duration::from_millis(u64::from(milliseconds));
        match bound <= MAX_CHILD_EXIT_FLUSH_BOUND {
            true => Ok(Self(bound)),
            false => Err(FlushBoundTooLong),
        }
    }

    /// The wait of the backstop after the host returned: the flush bound and
    /// the margin. Pure.
    pub fn backstop(self) -> Duration {
        self.0 + CHILD_EXIT_BACKSTOP_MARGIN
    }
}

/// The name of the thread that runs the child host.
pub const CHILD_HOST_THREAD_NAME: &str = "horsies-child-host";

/// How the core ends a JavaScript child from one of its own threads, and
/// how the backstop ends it: no exit handler runs.
pub fn child_process_exit() -> ProcessExit {
    ProcessExit::Immediate(ExitDrain::OutputOnly)
}

/// What the host thread keeps of the child's socket.
#[derive(Debug)]
pub enum SocketHold {
    /// A close-on-exec duplicate of the socket. Only the end of the process
    /// closes it.
    Held(OwnedFd),
    /// The descriptor of the arguments is not open. The core's host names it
    /// and returns at once (`BAD_ARGUMENTS`); there is no socket to keep.
    NotOpen,
}

/// A close-on-exec duplicate of the child's socket `socket_fd`, numbered
/// above the standard streams, for the host thread to keep.
///
/// Failure exits: the `io::Error` of a duplicate that the OS refuses (for
/// example `EMFILE`); no descriptor is left open then.
pub fn hold_socket(socket_fd: RawFd) -> io::Result<SocketHold> {
    // SAFETY: fcntl with integer arguments; a number with no descriptor
    // gives EBADF.
    let duplicate =
        unsafe { libc::fcntl(socket_fd, libc::F_DUPFD_CLOEXEC, FIRST_DUPLICATE_DESCRIPTOR) };
    if duplicate != -1 {
        // SAFETY: fcntl gave a new descriptor that nothing else owns.
        return Ok(SocketHold::Held(unsafe { OwnedFd::from_raw_fd(duplicate) }));
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EBADF) => Ok(SocketHold::NotOpen),
        _ => Err(error),
    }
}

/// Start the host thread: `serve` runs the host and gives its exit;
/// `resolve` gives it to JavaScript; then the backstop waits for the flush
/// bound and the margin, and ends the process with the exit's code. The
/// thread keeps `socket` until the process ends.
///
/// Failure exits: the `io::Error` of a thread that the OS refuses; nothing
/// ran then, and `socket` is closed.
pub fn spawn_host_thread(
    serve: impl FnOnce() -> ChildHostExit + Send + 'static,
    resolve: impl FnOnce(ChildHostExit) + Send + 'static,
    flush_bound: FlushBound,
    socket: SocketHold,
) -> io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name(CHILD_HOST_THREAD_NAME.to_owned())
        .spawn(move || {
            // Kept to the end of the thread, which ends the process: the
            // parent reads the end of the socket with the exit.
            let _kept_socket = socket;
            let exit = serve();
            resolve(exit);
            std::thread::sleep(flush_bound.backstop());
            child_process_exit().end(exit.code())
        })
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;
    use std::os::unix::process::CommandExt;
    use std::process::{Child, Command, ExitStatus, Stdio};
    use std::time::Instant;

    use horsies_core::CHANNEL_LOST_GRACE;

    use super::*;

    /// Set only in the process that
    /// [`the_socket_of_a_blocked_child_ends_with_its_exit`]
    /// starts: the descriptor number of the socket end of the probe.
    const HELD_SOCKET_PROBE_ENV: &str = "HORSIES_TYPESCRIPT_HELD_SOCKET_PROBE";
    const HELD_SOCKET_PROBE_TEST: &str = "child::exit::tests::held_socket_probe";
    /// How long the probe holds its main thread: past its backstop (a flush
    /// bound of 0 ms and the margin of 1 s).
    const PROBE_BLOCK: Duration = Duration::from_secs(10);
    /// The longest wait of the test for the end of the socket of the probe.
    const PROBE_BOUND: Duration = Duration::from_secs(30);
    /// One poll of the exit of the probe.
    const EXIT_POLL: Duration = Duration::from_millis(10);
    /// The longest read that shows a socket with no end yet.
    const OPEN_SOCKET_READ: Duration = Duration::from_millis(100);

    /// Clear close-on-exec on `fd`, in the forked probe before `exec`.
    fn keep_across_exec(fd: RawFd) -> io::Result<()> {
        // SAFETY: fcntl with integer arguments.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if flags == -1 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: as above.
        match unsafe { libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } {
            -1 => Err(io::Error::last_os_error()),
            _ => Ok(()),
        }
    }

    /// The status of `probe` when it exits within `grace`; `None` while it
    /// runs.
    fn status_within(probe: &mut Child, grace: Duration) -> Option<ExitStatus> {
        let until = Instant::now() + grace;
        loop {
            match probe.try_wait().expect("the wait of the probe") {
                Some(status) => return Some(status),
                None if Instant::now() >= until => return None,
                None => std::thread::sleep(EXIT_POLL),
            }
        }
    }

    #[test]
    fn a_held_socket_stays_open_after_its_first_descriptor_closes() {
        let (parent_end, child_end) = UnixStream::pair().expect("a socket pair");
        let hold = hold_socket(child_end.as_raw_fd()).expect("the socket is held");
        assert!(matches!(hold, SocketHold::Held(_)), "{hold:?}");
        drop(child_end);
        parent_end
            .set_read_timeout(Some(OPEN_SOCKET_READ))
            .expect("a read timeout");
        let mut byte = [0_u8; 1];
        let open = (&parent_end).read(&mut byte);
        assert!(
            matches!(&open, Err(error) if error.kind() == io::ErrorKind::WouldBlock),
            "{open:?}"
        );
        drop(hold);
        assert_eq!(
            (&parent_end)
                .read(&mut byte)
                .expect("the end of the socket"),
            0
        );
    }

    #[test]
    fn a_descriptor_that_is_not_open_is_not_held() {
        assert!(matches!(hold_socket(RawFd::MAX), Ok(SocketHold::NotOpen)));
    }

    /// The body of the probe process: a child whose host returned
    /// `ChildHookFailed` while its JavaScript thread is blocked. The host
    /// closes its socket when it returns, as the core's host does; this
    /// thread stays blocked, and the backstop ends the process with 173.
    /// Without [`HELD_SOCKET_PROBE_ENV`] it returns at once.
    #[test]
    fn held_socket_probe() {
        let Some(number) = std::env::var_os(HELD_SOCKET_PROBE_ENV) else {
            return;
        };
        let socket_fd: RawFd = number
            .to_str()
            .and_then(|text| text.parse().ok())
            .expect("the probe gets a descriptor number");
        let hold = hold_socket(socket_fd).expect("the probe holds its socket");
        let flush_bound = FlushBound::of_millis(0).expect("0 ms is a flush bound");
        let _host = spawn_host_thread(
            move || {
                // SAFETY: the test passed this descriptor to the probe as its
                // socket, and nothing else in the probe owns it.
                drop(unsafe { UnixStream::from_raw_fd(socket_fd) });
                ChildHostExit::ChildHookFailed
            },
            |_exit| {},
            flush_bound,
            hold,
        )
        .expect("the host thread starts");
        std::thread::sleep(PROBE_BLOCK);
        panic!("the backstop did not end the probe");
    }

    /// The end of the socket of a child whose host returned while its
    /// JavaScript thread is blocked comes with its exit: within the grace
    /// that the parent gives after the end of a socket
    /// (`CHANNEL_LOST_GRACE`), the parent reads the exit code 173, not a
    /// live child that it kills.
    #[test]
    fn the_socket_of_a_blocked_child_ends_with_its_exit() {
        let (parent_end, child_end) = UnixStream::pair().expect("a socket pair");
        let child_fd = child_end.as_raw_fd();
        let mut command =
            Command::new(std::env::current_exe().expect("the test binary has a path"));
        command
            .args([
                HELD_SOCKET_PROBE_TEST,
                "--exact",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(HELD_SOCKET_PROBE_ENV, child_fd.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        // SAFETY: the hook runs in the forked probe before `exec` and calls
        // only fcntl.
        unsafe {
            command.pre_exec(move || keep_across_exec(child_fd));
        }
        let mut probe = command.spawn().expect("the probe starts");
        drop(child_end);
        parent_end
            .set_read_timeout(Some(PROBE_BOUND))
            .expect("a read timeout");
        let mut rest = Vec::new();
        let read = (&parent_end).read_to_end(&mut rest);
        let status = status_within(&mut probe, CHANNEL_LOST_GRACE);
        let _killed = probe.kill();
        let _reaped = probe.wait();
        assert!(
            read.is_ok(),
            "no end of the socket within {PROBE_BOUND:?}: {read:?}"
        );
        assert_eq!(
            status.and_then(|status| status.code()),
            Some(ChildHostExit::ChildHookFailed.code()),
            "the probe at {CHANNEL_LOST_GRACE:?} after the end of its socket: {status:?}"
        );
    }

    #[test]
    fn the_backstop_is_the_flush_bound_and_the_margin_and_a_long_bound_is_refused() {
        assert_eq!(
            FlushBound::of_millis(1000).map(FlushBound::backstop),
            Ok(Duration::from_secs(2))
        );
        assert_eq!(FlushBound::of_millis(8001), Err(FlushBoundTooLong));
        assert!(FlushBound::of_millis(8000).is_ok());
    }

    #[test]
    fn the_child_exits_with_no_exit_handler() {
        assert!(matches!(
            child_process_exit(),
            ProcessExit::Immediate(ExitDrain::OutputOnly)
        ));
    }
}
