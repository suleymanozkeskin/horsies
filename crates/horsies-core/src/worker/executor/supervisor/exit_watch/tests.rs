//! The Linux exit watch: one pidfd per child (Python `preload._open_pidfd`
//! and its kernel call).

use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::process::Command;
use std::time::{Duration, Instant};

use super::*;

/// The longest wait of a test for an exit.
const TEST_WAIT: Duration = Duration::from_secs(20);

/// A pid above every pid of Linux (`pid_max` is at most 2^22).
const NO_SUCH_PID: u32 = 1 << 30;

/// Python `TestSupport::test_the_kernel_call_reports_a_process_exit`: the
/// pidfd of a live child is not inherited by an `exec`, is not ready while
/// the child runs, and reports its exit.
#[test]
fn a_pidfd_is_not_inherited_and_reports_the_exit_of_its_process() {
    let (waker, _writer) = UnixStream::pair().unwrap();
    let mut watch = ExitWatch::new(waker.as_raw_fd()).unwrap();
    let mut child = Command::new("/bin/sh")
        .args(["-c", "exec sleep 60"])
        .spawn()
        .unwrap();
    let pid = ProcessId::new(child.id()).unwrap();

    let (child_watch, error) = watch.watch(pid);

    assert!(error.is_none(), "{error:?}");
    let ChildWatch::PidFd(fd) = &child_watch else {
        panic!("a live child has no pidfd: {child_watch:?}");
    };
    // SAFETY: fcntl F_GETFD of a descriptor that this test owns.
    let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFD) };
    assert_eq!(flags & libc::FD_CLOEXEC, libc::FD_CLOEXEC);
    let running = watch
        .wait(std::iter::once((pid, &child_watch)), None, Duration::ZERO)
        .unwrap();
    assert!(running.exited.is_empty());

    child.kill().unwrap();
    let deadline = Instant::now() + TEST_WAIT;
    let mut exited = Vec::new();
    while exited.is_empty() && Instant::now() < deadline {
        exited = watch
            .wait(std::iter::once((pid, &child_watch)), None, TEST_WAIT)
            .unwrap()
            .exited;
    }
    assert_eq!(exited, vec![pid]);
    let _status = child.wait().unwrap();
}

/// Python `TestSupport::test_the_kernel_call_reports_a_missing_process`:
/// the pidfd of a pid that names no process does not open (`ESRCH`), and
/// the child is polled.
#[test]
fn a_pidfd_of_a_missing_process_is_refused_and_the_child_is_polled() {
    let (waker, _writer) = UnixStream::pair().unwrap();
    let mut watch = ExitWatch::new(waker.as_raw_fd()).unwrap();

    let (child_watch, error) = watch.watch(ProcessId::new(NO_SUCH_PID).unwrap());

    assert!(matches!(child_watch, ChildWatch::Polled), "{child_watch:?}");
    assert_eq!(
        error.and_then(|error| error.raw_os_error()),
        Some(libc::ESRCH)
    );
}
