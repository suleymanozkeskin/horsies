//! The state of the handler child of a worker, and its form in the
//! `telemetry` column of a worker snapshot. Pure: no I/O.

use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Value};
use tokio::sync::watch;

use crate::worker::executor::protocol::ProcessId;
use crate::worker::executor::supervisor::{BootFailureCause, LossCause};

/// The state of the handler child of a worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandlerChildState {
    /// The worker runs no handler child: the app has no event handler and
    /// worker-ended spans are off.
    Off,
    /// A child boots: the first one, or one after a restart wait. The
    /// events wait in the event queue.
    Starting,
    /// A child sent its ready frame and takes events.
    Up { pid: ProcessId },
    /// No child runs. The events drop and count `child_down`.
    Down(HandlerChildDown),
}

/// Why no handler child runs, since when, and what follows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerChildDown {
    pub cause: HandlerDownCause,
    /// The detail of the cause: the text of the boot failure or of the loss.
    pub detail: String,
    pub since: DateTime<Utc>,
    pub next: AfterDown,
}

/// The cause of a handler child that is down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandlerDownCause {
    /// A boot ended before the ready frame.
    BootFailed(BootFailureCause),
    /// The start hook of the child failed or hung.
    StartHookFailed,
    /// A child that was up ended.
    Lost(LossCause),
}

impl std::fmt::Display for HandlerDownCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BootFailed(cause) => write!(f, "{cause}"),
            Self::StartHookFailed => f.write_str("the start hook of the child failed or hung"),
            Self::Lost(loss) => write!(f, "the child ended: {loss}"),
        }
    }
}

/// What follows a handler child that is down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AfterDown {
    /// A new child boots after this wait.
    BootsAgainAfter(Duration),
    /// A cause that repeats: no child boots until the worker restarts.
    StaysDown,
}

/// A read view of the state of the handler child, for the worker snapshot
/// and the wait for the first ready frame. Clones share one state.
#[derive(Debug, Clone)]
pub struct HandlerChildView(watch::Receiver<HandlerChildState>);

impl HandlerChildView {
    /// The view of a worker that runs no handler child: always
    /// [`HandlerChildState::Off`].
    pub fn off() -> Self {
        let (_unused, state) = watch::channel(HandlerChildState::Off);
        Self(state)
    }

    /// The view of the state that `state` publishes.
    pub(super) fn of(state: watch::Receiver<HandlerChildState>) -> Self {
        Self(state)
    }

    /// The state now.
    pub fn state(&self) -> HandlerChildState {
        self.0.borrow().clone()
    }

    /// Wait until the state is not [`HandlerChildState::Starting`], at most
    /// `bound`; give the state then. A supervision that ended leaves its
    /// last state.
    pub async fn settled(&self, bound: Duration) -> HandlerChildState {
        let mut state = self.0.clone();
        let wait = state.wait_for(|now| !matches!(now, HandlerChildState::Starting));
        match tokio::time::timeout(bound, wait).await {
            Ok(Ok(settled)) => settled.clone(),
            Ok(Err(_gone)) | Err(_) => self.state(),
        }
    }
}

/// The `telemetry` column of a worker snapshot: the state of each part of
/// the telemetry of the worker, an object with the key `handler_child`.
/// Pure.
pub fn telemetry_json(handler_child: &HandlerChildState) -> Value {
    json!({ "handler_child": handler_child_json(handler_child) })
}

/// The state of the handler child: `{"state": "off"}`,
/// `{"state": "starting"}`, `{"state": "up", "pid": ..}`, or
/// `{"state": "down", "cause": .., "detail": .., "down_since": ..,
/// "next": ..}`. Pure.
fn handler_child_json(state: &HandlerChildState) -> Value {
    match state {
        HandlerChildState::Off => json!({ "state": "off" }),
        HandlerChildState::Starting => json!({ "state": "starting" }),
        HandlerChildState::Up { pid } => json!({ "state": "up", "pid": pid.get() }),
        HandlerChildState::Down(down) => json!({
            "state": "down",
            "cause": cause_json(down.cause),
            "detail": down.detail,
            "down_since": down.since.to_rfc3339_opts(SecondsFormat::Micros, false),
            "next": next_json(down.next),
        }),
    }
}

/// `{"kind": "boot_failed", "boot": ..}` (with `errno` for a resource
/// shortage), `{"kind": "start_hook_failed"}`, or
/// `{"kind": "lost", "loss": ..}`. Pure.
fn cause_json(cause: HandlerDownCause) -> Value {
    match cause {
        HandlerDownCause::BootFailed(BootFailureCause::ResourceShortage(errno)) => json!({
            "kind": "boot_failed",
            "boot": "resource_shortage",
            "errno": errno.as_str(),
        }),
        HandlerDownCause::BootFailed(boot) => {
            json!({ "kind": "boot_failed", "boot": boot_cause_name(boot) })
        }
        HandlerDownCause::StartHookFailed => json!({ "kind": "start_hook_failed" }),
        HandlerDownCause::Lost(loss) => json!({ "kind": "lost", "loss": loss_name(loss) }),
    }
}

/// `{"kind": "boots_again", "after_ms": ..}` or `{"kind": "stays_down"}`.
/// Pure.
fn next_json(next: AfterDown) -> Value {
    match next {
        AfterDown::BootsAgainAfter(wait) => json!({
            "kind": "boots_again",
            "after_ms": u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
        }),
        AfterDown::StaysDown => json!({ "kind": "stays_down" }),
    }
}

fn boot_cause_name(cause: BootFailureCause) -> &'static str {
    match cause {
        BootFailureCause::AppLoadFailed => "app_load_failed",
        BootFailureCause::ChildPoolSetupFailed => "child_pool_setup_failed",
        BootFailureCause::TaskLoopCreateFailed => "task_loop_create_failed",
        BootFailureCause::SpawnFailed => "spawn_failed",
        BootFailureCause::LostBeforeReady { .. } => "lost_before_ready",
        BootFailureCause::NotReadyInTime => "not_ready_in_time",
        BootFailureCause::ProtocolMismatch(_) => "protocol_mismatch",
        BootFailureCause::ResourceShortage(_) => "resource_shortage",
        BootFailureCause::ThreadStartRefused => "thread_start_refused",
    }
}

fn loss_name(loss: LossCause) -> &'static str {
    match loss {
        LossCause::ChildDied => "child_died",
        LossCause::ChannelLost => "channel_lost",
        LossCause::ProtocolViolation => "protocol_violation",
        LossCause::ProtocolMismatch(_) => "protocol_mismatch",
        LossCause::KilledByParent => "killed_by_worker",
        LossCause::SlotDefect => "slot_defect",
        LossCause::SpawnerStopped => "spawner_stopped",
    }
}
