//! The verdict of each run of a shared child that the worker kills. Pure.
//!
//! A shared child that has no run with time left is killed by the close
//! and kill path of its slot; its culprit retries isolated
//! ([`culprit_retry`]). The worker also kills a shared child for these
//! causes ([`ChildKill`]):
//!
//! - a blocked loop: the child reported that its loop has not turned for
//!   the block limit, so no run of it can move;
//! - no named cause: the backstop of a frozen child, the drain deadline of a
//!   stopping worker, a closed runner heartbeat registry.
//!
//! The verdict of each run at such a kill, whose exit the worker saw
//! ([`verdict_at_kill`]):
//!
//! | Run | Verdict | Retry |
//! |---|---|---|
//! | held the blocked loop, before its deadline | `TASK_BLOCKED_LOOP` | isolated |
//! | held the blocked loop, past its deadline | `TASK_TIMEOUT` | isolated |
//! | named culprit of a stuck stop | `TASK_TIMEOUT` | isolated |
//! | another run past its own deadline | `TASK_TIMEOUT` | same class |
//! | another run before its deadline, the loop held by a named run | `NEIGHBOUR_BLOCKED_LOOP` | same class |
//! | any other run | the crash path | same class |
//!
//! A run whose body returned before its deadline, and whose left work the
//! child reported, ends with what it returned at every end of its child
//! whose exit was seen; the slot decides that before it asks for a verdict.
//! A run that the worker sent an operator cancel to ends as cancelled at
//! every end of its child, with no verdict: the cancel statement recorded
//! its attempt. A cancelled run counts as having no time left only once its
//! child reported it stuck after its second cancel, so it never makes the
//! worker kill a child in which a neighbour run still has time.

use uuid::Uuid;

use crate::broker::attempt_writes::RetryPlacement;
use crate::worker::executor::protocol::{BlockedWhere, DispatchNumber, RunRef};

/// What held a blocked event loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopHolder {
    /// This run, still in flight, held it.
    Run(RunRef),
    /// Code of no run in flight held it.
    Unknown,
}

/// The named cause of a kill that the worker sent to a shared child. It
/// changes at most once, from `NoNamedKill`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChildKill {
    /// No kill with a named cause was sent to the child.
    NoNamedKill,
    /// The loop of the child did not turn for `blocked_ms`; `location` is
    /// the source line it ran.
    BlockedLoop {
        holder: LoopHolder,
        blocked_ms: u64,
        location: BlockedWhere,
    },
}

/// Whether the deadline of a run had passed at the kill, and whether the
/// run is the named culprit of the stuck stop of its child.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadlineState {
    Pending,
    Passed,
    /// The deadline passed, and the run is the named culprit: a run that an
    /// async child reported stuck, or the run whose deadline closed a
    /// thread child. It retries isolated at every kill whose exit was seen.
    Culprit,
}

/// The verdict of a run of a child that the worker killed, with its exit
/// seen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KilledRunVerdict {
    /// `TASK_TIMEOUT`, retried at `retry`.
    TimedOutAtKill { retry: RetryPlacement },
    /// `TASK_BLOCKED_LOOP`: this run held the loop past the block limit,
    /// before its deadline. Its retry runs isolated.
    BlockedTheLoop {
        blocked_ms: u64,
        location: BlockedWhere,
    },
    /// `NEIGHBOUR_BLOCKED_LOOP`: the task `holder_task_id` held the loop of
    /// this run's child past the block limit, before this run's deadline.
    BesideBlockedLoop {
        holder_task_id: Uuid,
        blocked_ms: u64,
    },
}

/// The verdict at a kill, or the crash path for a run the kill names no
/// cause for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KillVerdict {
    Verdict(KilledRunVerdict),
    /// `WORKER_CRASHED`, or its retry, in the run's class.
    CrashPath,
}

/// The retry of run `dispatch` of a child killed when no run in it had
/// time left: the culprit retries isolated, every other run in its class.
pub fn culprit_retry(culprit: RunRef, dispatch: DispatchNumber) -> RetryPlacement {
    match culprit.dispatch == dispatch {
        true => RetryPlacement::Isolated,
        false => RetryPlacement::SameClass,
    }
}

/// The verdict of run `dispatch` at a kill of its child by the worker,
/// whose exit was seen (Python `verdict_at_kill`).
pub fn verdict_at_kill(
    kill: &ChildKill,
    dispatch: DispatchNumber,
    deadline: DeadlineState,
) -> KillVerdict {
    if let ChildKill::BlockedLoop {
        holder: LoopHolder::Run(holder),
        blocked_ms,
        location,
    } = kill
    {
        if holder.dispatch == dispatch {
            return KillVerdict::Verdict(verdict_of_holder(*blocked_ms, location, deadline));
        }
    }
    match (deadline, kill) {
        (DeadlineState::Culprit, _) => KillVerdict::Verdict(KilledRunVerdict::TimedOutAtKill {
            retry: RetryPlacement::Isolated,
        }),
        (DeadlineState::Passed, _) => KillVerdict::Verdict(KilledRunVerdict::TimedOutAtKill {
            retry: RetryPlacement::SameClass,
        }),
        (
            DeadlineState::Pending,
            ChildKill::BlockedLoop {
                holder: LoopHolder::Run(holder),
                blocked_ms,
                ..
            },
        ) => KillVerdict::Verdict(KilledRunVerdict::BesideBlockedLoop {
            holder_task_id: holder.task_id,
            blocked_ms: *blocked_ms,
        }),
        (
            DeadlineState::Pending,
            ChildKill::BlockedLoop {
                holder: LoopHolder::Unknown,
                ..
            }
            | ChildKill::NoNamedKill,
        ) => KillVerdict::CrashPath,
    }
}

/// The run that held the blocked loop retries isolated.
fn verdict_of_holder(
    blocked_ms: u64,
    location: &BlockedWhere,
    deadline: DeadlineState,
) -> KilledRunVerdict {
    match deadline {
        DeadlineState::Passed | DeadlineState::Culprit => KilledRunVerdict::TimedOutAtKill {
            retry: RetryPlacement::Isolated,
        },
        DeadlineState::Pending => KilledRunVerdict::BlockedTheLoop {
            blocked_ms,
            location: location.clone(),
        },
    }
}
