//! The socket pair of one child, with explicit buffer sizes.
//!
//! A Unix socket pair gets small default buffers on some systems (8 KiB on
//! macOS), so a 10 KB frame needs several write and read rounds. Both ends
//! get [`SOCKET_BUFFER_BYTES`] for sending and receiving.

use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;

/// The send and receive buffer of each end: 4 MiB.
pub const SOCKET_BUFFER_BYTES: libc::c_int = 4 << 20;

/// The parent end and the child end of one child's socket.
#[derive(Debug)]
pub struct ChildSocketPair {
    pub parent: UnixStream,
    pub child: UnixStream,
}

/// A new socket pair with both buffers of both ends set.
///
/// Both ends are close-on-exec; the spawn clears the flag of the child end
/// in the child only. `Err`: the OS refused the pair or a buffer size; no
/// descriptor of this call is left open then.
pub fn child_socket_pair() -> io::Result<ChildSocketPair> {
    let (parent, child) = UnixStream::pair()?;
    set_buffer_sizes(parent.as_raw_fd())?;
    set_buffer_sizes(child.as_raw_fd())?;
    Ok(ChildSocketPair { parent, child })
}

fn set_buffer_sizes(fd: RawFd) -> io::Result<()> {
    for option in [libc::SO_SNDBUF, libc::SO_RCVBUF] {
        set_int_option(fd, option, SOCKET_BUFFER_BYTES)?;
    }
    Ok(())
}

fn set_int_option(fd: RawFd, option: libc::c_int, value: libc::c_int) -> io::Result<()> {
    let length = libc::socklen_t::try_from(std::mem::size_of::<libc::c_int>())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    // SAFETY: `fd` is an open socket owned by the caller, and the option
    // value points to a live `c_int` of `length` bytes for the whole call.
    let result = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            option,
            (&value as *const libc::c_int).cast::<libc::c_void>(),
            length,
        )
    };
    match result {
        0 => Ok(()),
        _ => Err(io::Error::last_os_error()),
    }
}

/// The size of one buffer option of `fd`, as the OS reports it. Linux
/// reports twice the size it was given.
pub fn buffer_size(fd: RawFd, option: libc::c_int) -> io::Result<libc::c_int> {
    let mut value: libc::c_int = 0;
    let mut length = libc::socklen_t::try_from(std::mem::size_of::<libc::c_int>())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    // SAFETY: `fd` is an open socket; `value` and `length` are live for the
    // call and `length` holds the size of `value`.
    let result = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            option,
            (&mut value as *mut libc::c_int).cast::<libc::c_void>(),
            &mut length,
        )
    };
    match result {
        0 => Ok(value),
        _ => Err(io::Error::last_os_error()),
    }
}
