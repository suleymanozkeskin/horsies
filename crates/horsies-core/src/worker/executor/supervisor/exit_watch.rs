//! How the spawner thread learns that a child exited, per platform. It
//! waits on its request waker and on every child at once.
//!
//! - Linux: a pidfd per child, all polled with the waker in one `poll`.
//! - macOS: one kqueue with the waker (`EVFILT_READ`) and each child
//!   (`EVFILT_PROC`, `NOTE_EXIT`).
//! - A child whose watch could not open (a Linux kernel without pidfd, a
//!   descriptor limit), and every child on other platforms: `waitpid` with
//!   `WNOHANG` every [`POLLED_EXIT_INTERVAL`].
//!
//! The watch only reports; the spawner thread reaps with `waitpid`.
//!
//! A wait can also watch one more descriptor for input: the socket of the
//! preload process (Linux only), so the spawner thread reads its frames in
//! the same wait as the exits.

use std::io;
use std::os::fd::RawFd;
use std::time::Duration;

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use crate::worker::executor::protocol::ProcessId;

/// The poll interval of children without an exit notification.
pub const POLLED_EXIT_INTERVAL: Duration = Duration::from_millis(50);

/// How one child is watched.
#[derive(Debug)]
pub(super) enum ChildWatch {
    /// The pidfd of the child turns readable at its exit.
    #[cfg(target_os = "linux")]
    PidFd(OwnedFd),
    /// The kqueue reports the exit of the child's pid.
    #[cfg(target_vendor = "apple")]
    Kqueue,
    /// No notification: `waitpid` with `WNOHANG` every
    /// [`POLLED_EXIT_INTERVAL`].
    Polled,
}

/// What a wait saw.
#[derive(Debug, Default)]
pub(super) struct Wake {
    /// Children whose exit was reported. Each still needs `waitpid`.
    pub(super) exited: Vec<ProcessId>,
    /// The request waker fired.
    pub(super) woken: bool,
    /// The extra descriptor of the wait has input, or its peer closed.
    pub(super) extra_ready: bool,
}

/// The milliseconds of a wait, at most `c_int::MAX`.
#[cfg(not(target_vendor = "apple"))]
fn wait_millis(timeout: Duration) -> libc::c_int {
    libc::c_int::try_from(timeout.as_millis()).unwrap_or(libc::c_int::MAX)
}

// ---------------------------------------------------------------------------
// Linux: pidfd and poll
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
pub(super) struct ExitWatch {
    waker_fd: RawFd,
}

#[cfg(target_os = "linux")]
impl ExitWatch {
    pub(super) fn new(waker_fd: RawFd) -> io::Result<Self> {
        Ok(Self { waker_fd })
    }

    /// Start watching `pid`. A pidfd that cannot open makes the child a
    /// polled one; the error is returned for the log.
    pub(super) fn watch(&mut self, pid: ProcessId) -> (ChildWatch, Option<io::Error>) {
        // SAFETY: pidfd_open takes a pid and flags and returns a new
        // descriptor or -1; it touches no memory of this process.
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid.as_pid_t(), 0) };
        match RawFd::try_from(fd) {
            Ok(fd) if fd >= 0 => {
                // SAFETY: the kernel returned a new descriptor that nothing
                // else owns.
                (ChildWatch::PidFd(unsafe { OwnedFd::from_raw_fd(fd) }), None)
            }
            _ => (ChildWatch::Polled, Some(io::Error::last_os_error())),
        }
    }

    /// Wait up to `timeout` for the waker, input on `extra`, or the exit of
    /// a child that has a pidfd.
    pub(super) fn wait<'a>(
        &mut self,
        children: impl Iterator<Item = (ProcessId, &'a ChildWatch)>,
        extra: Option<RawFd>,
        timeout: Duration,
    ) -> io::Result<Wake> {
        let mut fds = vec![libc::pollfd {
            fd: self.waker_fd,
            events: libc::POLLIN,
            revents: 0,
        }];
        // The extra descriptor, when given, is the second entry.
        let children_from = match extra {
            Some(fd) => {
                fds.push(libc::pollfd {
                    fd,
                    events: libc::POLLIN,
                    revents: 0,
                });
                2
            }
            None => 1,
        };
        let mut pids = Vec::new();
        for (pid, watch) in children {
            if let ChildWatch::PidFd(fd) = watch {
                fds.push(libc::pollfd {
                    fd: fd.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                });
                pids.push(pid);
            }
        }
        let count = libc::nfds_t::try_from(fds.len())
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        // SAFETY: `fds` holds `count` initialised pollfd entries and lives
        // for the whole call.
        let ready = unsafe { libc::poll(fds.as_mut_ptr(), count, wait_millis(timeout)) };
        match ready {
            -1 => {
                let error = io::Error::last_os_error();
                match error.kind() {
                    io::ErrorKind::Interrupted => Ok(Wake::default()),
                    _ => Err(error),
                }
            }
            _ => Ok(Wake {
                woken: fds[0].revents != 0,
                extra_ready: children_from == 2 && fds[1].revents != 0,
                exited: fds[children_from..]
                    .iter()
                    .zip(pids)
                    .filter(|(fd, _)| fd.revents != 0)
                    .map(|(_, pid)| pid)
                    .collect(),
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// macOS: kqueue
// ---------------------------------------------------------------------------

/// The most events one kqueue wait takes; more come with the next wait.
#[cfg(target_vendor = "apple")]
const KQUEUE_EVENTS: usize = 64;

#[cfg(target_vendor = "apple")]
pub(super) struct ExitWatch {
    queue: OwnedFd,
}

#[cfg(target_vendor = "apple")]
impl ExitWatch {
    pub(super) fn new(waker_fd: RawFd) -> io::Result<Self> {
        // SAFETY: kqueue takes no argument and returns a new descriptor or -1.
        let fd = unsafe { libc::kqueue() };
        if fd == -1 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the kernel returned a new descriptor that nothing else owns.
        let queue = unsafe { OwnedFd::from_raw_fd(fd) };
        let watch = Self { queue };
        let ident =
            usize::try_from(waker_fd).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        watch.change(ident, libc::EVFILT_READ, libc::EV_ADD, 0)?;
        Ok(watch)
    }

    fn change(&self, ident: usize, filter: i16, flags: u16, fflags: u32) -> io::Result<()> {
        // SAFETY: an all-zero kevent is a valid value (a null udata).
        let mut change: libc::kevent = unsafe { std::mem::zeroed() };
        change.ident = ident;
        change.filter = filter;
        change.flags = flags;
        change.fflags = fflags;
        // SAFETY: one change entry, no event list, no timeout; every
        // pointer is live for the call.
        let result = unsafe {
            libc::kevent(
                self.queue.as_raw_fd(),
                &change,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        match result {
            -1 => Err(io::Error::last_os_error()),
            _ => Ok(()),
        }
    }

    /// Start watching `pid`. A child the kqueue refuses (it already exited,
    /// or the queue is full) becomes a polled one; the error is returned for
    /// the log.
    pub(super) fn watch(&mut self, pid: ProcessId) -> (ChildWatch, Option<io::Error>) {
        let ident = match usize::try_from(pid.get()) {
            Ok(ident) => ident,
            Err(_) => {
                return (
                    ChildWatch::Polled,
                    Some(io::Error::from(io::ErrorKind::InvalidInput)),
                )
            }
        };
        match self.change(
            ident,
            libc::EVFILT_PROC,
            libc::EV_ADD | libc::EV_ONESHOT,
            libc::NOTE_EXIT,
        ) {
            Ok(()) => (ChildWatch::Kqueue, None),
            Err(error) => (ChildWatch::Polled, Some(error)),
        }
    }

    /// Wait up to `timeout` for the waker or the exit of a watched child.
    ///
    /// Panics: on an `extra` descriptor. Only the preload process gives
    /// one, and it runs only on Linux.
    pub(super) fn wait<'a>(
        &mut self,
        _children: impl Iterator<Item = (ProcessId, &'a ChildWatch)>,
        extra: Option<RawFd>,
        timeout: Duration,
    ) -> io::Result<Wake> {
        assert!(
            extra.is_none(),
            "the spawner waits on a preload socket only on Linux"
        );
        // SAFETY: an all-zero kevent array is a valid value.
        let mut events: [libc::kevent; KQUEUE_EVENTS] = unsafe { std::mem::zeroed() };
        // SAFETY: an all-zero timespec is a valid value.
        let mut wait: libc::timespec = unsafe { std::mem::zeroed() };
        wait.tv_sec = libc::time_t::try_from(timeout.as_secs()).unwrap_or(libc::time_t::MAX);
        wait.tv_nsec = libc::c_long::from(timeout.subsec_nanos());
        let capacity = libc::c_int::try_from(KQUEUE_EVENTS)
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        // SAFETY: no change list; `events` has room for `capacity` entries;
        // every pointer is live for the call.
        let count = unsafe {
            libc::kevent(
                self.queue.as_raw_fd(),
                std::ptr::null(),
                0,
                events.as_mut_ptr(),
                capacity,
                &wait,
            )
        };
        let count = match usize::try_from(count) {
            Ok(count) => count,
            Err(_) => {
                let error = io::Error::last_os_error();
                return match error.kind() {
                    io::ErrorKind::Interrupted => Ok(Wake::default()),
                    _ => Err(error),
                };
            }
        };
        let mut wake = Wake::default();
        for event in &events[..count] {
            match event.filter {
                libc::EVFILT_READ => wake.woken = true,
                libc::EVFILT_PROC => {
                    // `libc::kevent` is `repr(packed(4))` on Apple targets:
                    // copy the field, because the log macro takes a reference.
                    let ident = event.ident;
                    let pid = u32::try_from(ident).ok().and_then(ProcessId::new);
                    match pid {
                        Some(pid) => wake.exited.push(pid),
                        None => tracing::error!(
                            ident,
                            "the kqueue reported an exit for an ident that is not a pid"
                        ),
                    }
                }
                _ => {}
            }
        }
        Ok(wake)
    }
}

// ---------------------------------------------------------------------------
// Other platforms: every child is polled
// ---------------------------------------------------------------------------

#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
pub(super) struct ExitWatch {
    waker_fd: RawFd,
}

#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
impl ExitWatch {
    pub(super) fn new(waker_fd: RawFd) -> io::Result<Self> {
        Ok(Self { waker_fd })
    }

    pub(super) fn watch(&mut self, _pid: ProcessId) -> (ChildWatch, Option<io::Error>) {
        (ChildWatch::Polled, None)
    }

    /// Panics: on an `extra` descriptor. Only the preload process gives
    /// one, and it runs only on Linux.
    pub(super) fn wait<'a>(
        &mut self,
        _children: impl Iterator<Item = (ProcessId, &'a ChildWatch)>,
        extra: Option<RawFd>,
        timeout: Duration,
    ) -> io::Result<Wake> {
        assert!(
            extra.is_none(),
            "the spawner waits on a preload socket only on Linux"
        );
        let mut fds = [libc::pollfd {
            fd: self.waker_fd,
            events: libc::POLLIN,
            revents: 0,
        }];
        // SAFETY: one initialised pollfd entry, live for the call.
        let ready = unsafe { libc::poll(fds.as_mut_ptr(), 1, wait_millis(timeout)) };
        match ready {
            -1 => {
                let error = io::Error::last_os_error();
                match error.kind() {
                    io::ErrorKind::Interrupted => Ok(Wake::default()),
                    _ => Err(error),
                }
            }
            _ => Ok(Wake {
                woken: fds[0].revents != 0,
                extra_ready: false,
                exited: Vec::new(),
            }),
        }
    }
}
