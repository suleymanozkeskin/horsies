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
//!   hangs still gives 173, inside the parent's stop wait of 10 s.
//! - The core's own exits from its threads (the end of the parent, a bad
//!   parent frame) use the same immediate exit (`ChildApp::exit`): the exit
//!   handlers of Node and Bun can crash while the JavaScript thread runs
//!   code, and the process would then end by a signal in place of its code.

use std::io;
use std::thread::JoinHandle;
use std::time::Duration;

use horsies_core::{ChildHostExit, ExitDrain, ProcessExit};

/// The time past the package's flush bound that `process.exit` gets before
/// the host thread ends the process.
pub const CHILD_EXIT_BACKSTOP_MARGIN: Duration = Duration::from_secs(1);

/// The longest flush bound that a plan may give: the backstop must come
/// inside the parent's stop wait of 10 s.
pub const MAX_CHILD_EXIT_FLUSH_BOUND: Duration = Duration::from_secs(8);

const _: () = assert!(
    MAX_CHILD_EXIT_FLUSH_BOUND.as_millis() + CHILD_EXIT_BACKSTOP_MARGIN.as_millis() < 10_000,
    "the backstop comes inside the parent's stop wait of 10 s",
);

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

/// Start the host thread: `serve` runs the host and gives its exit;
/// `resolve` gives it to JavaScript; then the backstop waits for the flush
/// bound and the margin, and ends the process with the exit's code.
///
/// Failure exits: the `io::Error` of a thread that the OS refuses; nothing
/// ran then.
pub fn spawn_host_thread(
    serve: impl FnOnce() -> ChildHostExit + Send + 'static,
    resolve: impl FnOnce(ChildHostExit) + Send + 'static,
    flush_bound: FlushBound,
) -> io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name(CHILD_HOST_THREAD_NAME.to_owned())
        .spawn(move || {
            let exit = serve();
            resolve(exit);
            std::thread::sleep(flush_bound.backstop());
            child_process_exit().end(exit.code())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

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
