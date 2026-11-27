//! The stop state of the host core: the core of a host program that runs
//! the child host next to its own core (a TypeScript child has the core of
//! its bridge). A panic in a core background task of the host stops that
//! core; the child then ends itself as a Python child does at a refused core
//! call (Python `child_main.end_child_after_core_refusal`):
//!
//! - Before the start write of a run: no start is written; the run ends
//!   `StoppedBeforeRunning(OWNERSHIP_UNCONFIRMED)`, so the parent releases
//!   the claim with no attempt charged.
//! - Before the finalizing marker of a run: no marker is written; the frame
//!   keeps what the body returned, so the parent writes that result.
//!
//! The frame says `child: ends_itself` (a `left_work_ended` frame has no
//! such field: the parent retires that child anyway). The child then flushes
//! its frames, logs the cause as its last line, and exits with
//! [`CORE_STOPPED_EXIT_CODE`] (80). A shared child ends after the first such
//! frame, as a Python shared child does; its other runs in flight take the
//! crashed-child path.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use super::link::{Flushed, FrameWriter};
use crate::worker::executor::protocol::CORE_STOPPED_EXIT_CODE;

/// The longest wait for the frames queued before the end of a child whose
/// host core stopped. The frames go to a local socket; the bound keeps a
/// parent that reads nothing from holding the exit.
pub const CORE_STOPPED_FLUSH_BOUND: Duration = Duration::from_secs(1);

/// The first panic of a core background task of the host: what failed, and
/// why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostCoreStop {
    /// What panicked: the core background task, as the host names it.
    pub subject: String,
    /// Why: the panic message with its source location.
    pub cause: String,
}

/// The state of the host core when the child host reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostCoreState {
    /// The host core takes calls.
    Running,
    /// The host core stopped after the first panic of a core background
    /// task, and refuses every call.
    Stopped(HostCoreStop),
}

/// The read of the host core's state.
pub type HostCoreRead = Arc<dyn Fn() -> HostCoreState + Send + Sync>;

/// Whether the child has a host core whose stop ends it. A Rust binary has
/// none: [`HostCoreWatch::NoHostCore`].
#[derive(Clone)]
pub enum HostCoreWatch {
    /// No core next to the child host: every run serves on.
    NoHostCore,
    /// The host's read of its core. It must return at once and must not
    /// panic; the child host calls it before each start write and before
    /// each finalizing marker.
    Host(HostCoreRead),
}

impl HostCoreWatch {
    /// The state of the host core now.
    pub(super) fn read(&self) -> HostCoreState {
        match self {
            Self::NoHostCore => HostCoreState::Running,
            Self::Host(read) => read(),
        }
    }
}

/// Where a run met the stopped host core.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CoreStopMoment {
    /// Before the start write of the task.
    AtStart(Uuid),
    /// After the body of the task, before its finalizing marker.
    AfterBody(Uuid),
}

impl fmt::Display for CoreStopMoment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AtStart(task_id) => write!(formatter, "At the start of task {task_id}"),
            Self::AfterBody(task_id) => write!(formatter, "After the body of task {task_id}"),
        }
    }
}

/// A run met the stopped host core: the child ends after its frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CoreStopEnd {
    pub(super) stop: HostCoreStop,
    pub(super) moment: CoreStopMoment,
}

impl CoreStopEnd {
    /// The last log line of the child (Python
    /// `child_main.end_child_after_core_refusal`). Pure.
    pub(super) fn line(&self) -> String {
        format!(
            "{}: the core of this child stopped after a panic in a core background task ({}: {}); \
             the child ends itself with exit code {CORE_STOPPED_EXIT_CODE}",
            self.moment, self.stop.subject, self.stop.cause,
        )
    }
}

/// End the serve of a child whose run met the stopped host core: wait for
/// the frames queued before this call, at most [`CORE_STOPPED_FLUSH_BOUND`],
/// then log the cause. The caller exits with the core-stopped code.
pub(super) async fn end_after_core_stop(writer: &FrameWriter, end: &CoreStopEnd) {
    match tokio::time::timeout(CORE_STOPPED_FLUSH_BOUND, writer.flush()).await {
        Ok(Flushed::Written) => {}
        // The writer thread exits the process when its write fails.
        Ok(Flushed::WriterGone) => {}
        Err(_elapsed) => tracing::warn!(
            "the frames of a child whose host core stopped were not written within {} ms; the \
             child exits",
            CORE_STOPPED_FLUSH_BOUND.as_millis(),
        ),
    }
    tracing::error!("{}", end.line());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ChildHostExit;

    fn stop() -> HostCoreStop {
        HostCoreStop {
            subject: "thread horsies-log-forwarder".to_owned(),
            cause: "planned panic (at forwarder.rs:9)".to_owned(),
        }
    }

    #[test]
    fn the_last_line_names_the_moment_the_cause_and_the_exit_code() {
        let task_id = Uuid::nil();
        let end = CoreStopEnd {
            stop: stop(),
            moment: CoreStopMoment::AtStart(task_id),
        };
        assert_eq!(
            end.line(),
            format!(
                "At the start of task {task_id}: the core of this child stopped after a panic in \
                 a core background task (thread horsies-log-forwarder: planned panic (at \
                 forwarder.rs:9)); the child ends itself with exit code 80"
            )
        );
        let after = CoreStopEnd {
            stop: stop(),
            moment: CoreStopMoment::AfterBody(task_id),
        };
        assert!(after
            .line()
            .starts_with(&format!("After the body of task {task_id}: ")));
    }

    #[test]
    fn a_child_with_no_host_core_reads_running() {
        assert_eq!(HostCoreWatch::NoHostCore.read(), HostCoreState::Running);
    }

    #[test]
    fn a_host_core_watch_gives_the_host_read() {
        let watch = HostCoreWatch::Host(Arc::new(|| HostCoreState::Stopped(stop())));
        assert_eq!(watch.read(), HostCoreState::Stopped(stop()));
    }

    #[test]
    fn a_child_whose_host_core_stopped_exits_with_80() {
        assert_eq!(ChildHostExit::CoreStopped.exit_code(), 80);
        assert_eq!(
            ChildHostExit::CoreStopped.exit_code(),
            CORE_STOPPED_EXIT_CODE
        );
    }
}
