//! The exit rule of a JavaScript child.
//!
//! - The child host blocks the thread that runs it, and the JavaScript main
//!   thread must stay free to run the bodies. So the host runs on a thread of
//!   its own (`horsies-child-host`).
//! - When the host returns its exit, the thread resolves the serve's Promise
//!   with the code and the flush bound. The package awaits its telemetry
//!   flush, at most [`CHILD_EXIT_FLUSH_BOUND`], then calls
//!   `process.exit(code)` on the JavaScript thread.
//! - The backstop: when the process still runs [`CHILD_EXIT_BACKSTOP`] after
//!   the host returned (the JavaScript thread is blocked, for example by a
//!   start hook past its bound), the host thread ends the process with the
//!   same code: the drain of Rust's standard output and standard error, then
//!   `_exit` (`ProcessExit::Immediate` with `ExitDrain::OutputOnly`). So a
//!   hook that hangs still gives 173, inside the parent's stop wait of 10 s.
//! - The core's own exits from its threads (the end of the parent, a bad
//!   parent frame) use the same immediate exit (`ChildApp::exit`): the exit
//!   handlers of Node and Bun can crash while the JavaScript thread runs
//!   code, and the process would then end by a signal in place of its code.

use std::io;
use std::thread::JoinHandle;
use std::time::Duration;

use horsies_core::{ChildHostExit, ExitDrain, ProcessExit};

/// The longest wait of the package for its telemetry flush before it ends
/// the process.
pub const CHILD_EXIT_FLUSH_BOUND: Duration = Duration::from_secs(1);

/// The time after the host returned at which the host thread ends the
/// process: the flush bound and 1 s more for `process.exit` to run.
pub const CHILD_EXIT_BACKSTOP: Duration = Duration::from_secs(2);

const _: () = assert!(
    CHILD_EXIT_BACKSTOP.as_millis() > CHILD_EXIT_FLUSH_BOUND.as_millis(),
    "the backstop comes after the flush bound",
);
const _: () = assert!(
    CHILD_EXIT_BACKSTOP.as_secs() < 10,
    "the backstop comes inside the parent's stop wait of 10 s",
);

/// The name of the thread that runs the child host.
pub const CHILD_HOST_THREAD_NAME: &str = "horsies-child-host";

/// How the core ends a JavaScript child from one of its own threads, and
/// how the backstop ends it: no exit handler runs.
pub fn child_process_exit() -> ProcessExit {
    ProcessExit::Immediate(ExitDrain::OutputOnly)
}

/// The flush bound in milliseconds, for the serve's end record.
pub fn flush_bound_ms() -> u32 {
    u32::try_from(CHILD_EXIT_FLUSH_BOUND.as_millis())
        .unwrap_or_else(|_| panic!("the flush bound of 1 s fits a u32 of milliseconds"))
}

/// Start the host thread: `serve` runs the host and gives its exit;
/// `resolve` gives it to JavaScript; then the backstop waits and ends the
/// process with the exit's code.
///
/// Failure exits: the `io::Error` of a thread that the OS refuses; nothing
/// ran then.
pub fn spawn_host_thread(
    serve: impl FnOnce() -> ChildHostExit + Send + 'static,
    resolve: impl FnOnce(ChildHostExit) + Send + 'static,
) -> io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name(CHILD_HOST_THREAD_NAME.to_owned())
        .spawn(move || {
            let exit = serve();
            resolve(exit);
            std::thread::sleep(CHILD_EXIT_BACKSTOP);
            child_process_exit().end(exit.code())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_flush_bound_is_one_second() {
        assert_eq!(flush_bound_ms(), 1000);
    }

    #[test]
    fn the_child_exits_with_no_exit_handler() {
        assert!(matches!(
            child_process_exit(),
            ProcessExit::Immediate(ExitDrain::OutputOnly)
        ));
    }
}
