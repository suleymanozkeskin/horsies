//! The boot and the restart rules of the handler child.
//!
//! - A ready child is served until the stop or its loss
//!   ([`super::serve`]).
//! - A child lost after its ready frame boots again after
//!   `slot_down_delay(n)`, `n` the losses in a row. A child that served
//!   [`HANDLER_CHILD_STABLE`] starts the count again.
//! - A boot failure of a cause that can clear boots again after
//!   `slot_down_delay(n)`, `n` the failed boots in a row.
//! - A boot failure of a cause that repeats, and a failed start hook, leave
//!   the child down: the event queue closes, so each event drops and counts
//!   `child_down` at its offer.
//! - While a child is down and boots again later, the events that arrive
//!   drop and count `child_down`.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::watch;
use tokio::time::Instant;

use super::serve::{serve, Served};
use super::state::{AfterDown, HandlerChildDown, HandlerChildState, HandlerDownCause};
use super::{
    HandlerChildConfig, HandlerChildMetrics, StopSignal, EVENT_BATCH_MAX, HANDLER_CHILD_STABLE,
    HANDLER_MAILBOX_FRAMES, STOP_FLUSH_PASSES,
};
use crate::telemetry::labels::EventDropCause;
use crate::telemetry::worker::{EventBatch, EventQueue};
use crate::worker::executor::slots::booting::{boot_attempt, BootAttempt, BootTarget};
use crate::worker::executor::supervisor::{
    slot_down_delay, BootEnd, BootFailure, CauseKind, ChildLost, ChildSource, MailboxBound,
    SpawnerHandle,
};

/// What the supervision of the handler child holds.
pub(super) struct Supervision {
    pub config: HandlerChildConfig,
    pub source: Arc<dyn ChildSource>,
    pub spawner: SpawnerHandle,
    pub metrics: HandlerChildMetrics,
    pub state: watch::Sender<HandlerChildState>,
    pub stop: StopSignal,
}

impl Supervision {
    /// One boot attempt of the handler child.
    fn target(&self) -> BootTarget<'_> {
        BootTarget {
            source: self.source.as_ref(),
            spawner: &self.spawner,
            command: &self.config.command,
            init: self.config.init.clone(),
            mailbox: MailboxBound::frames(HANDLER_MAILBOX_FRAMES),
            cpus: &self.config.cpus,
            kill_wait: self.config.kill_wait,
            ready_bound: self.config.bounds.ready,
            stop: &self.stop.token,
        }
    }

    /// Publish `state` to the views.
    fn publish(&self, state: HandlerChildState) {
        // The sender keeps the value with no view, so a replace never fails.
        let _previous = self.state.send_replace(state);
    }
}

/// The counts of the restart rules, kept across the boots of one worker.
#[derive(Debug, Default)]
struct RestartCounts {
    /// Failed boots of a cause that can clear, in a row.
    down_boots: u32,
    /// Children lost in a row, each before it served
    /// [`HANDLER_CHILD_STABLE`].
    losses: u32,
}

/// Supervise the handler child until the stop, then count the events that
/// it did not get.
pub(super) async fn supervise(supervision: Supervision, mut queue: EventQueue) {
    supervise_until_end(&supervision, &mut queue).await;
    close_and_count(&supervision.metrics, &mut queue);
}

/// Boot, serve, and boot again by the restart rules, until the stop or a
/// cause that leaves the child down.
async fn supervise_until_end(supervision: &Supervision, queue: &mut EventQueue) {
    let mut counts = RestartCounts::default();
    loop {
        supervision.publish(HandlerChildState::Starting);
        let down = match boot_attempt(&supervision.target()).await {
            BootAttempt::Stopped => return,
            BootAttempt::Ready(channel, rss) => {
                counts.down_boots = 0;
                let served_since = Instant::now();
                supervision.metrics.record_rss(rss);
                let pid = channel.pid();
                tracing::info!(pid = pid.get(), "the handler child is ready");
                supervision.publish(HandlerChildState::Up { pid });
                match serve(supervision, queue, channel).await {
                    Served::Stopped => return,
                    Served::Lost(lost) => down_after_loss(&mut counts, &lost, served_since),
                }
            }
            BootAttempt::Ended(BootEnd::ChildHookFailed) => HandlerChildDown {
                cause: HandlerDownCause::StartHookFailed,
                detail: HandlerDownCause::StartHookFailed.to_string(),
                since: Utc::now(),
                next: AfterDown::StaysDown,
            },
            BootAttempt::Ended(BootEnd::Failed(failure)) => down_after_boot(&mut counts, &failure),
        };
        log_down(&down);
        let next = down.next;
        supervision.publish(HandlerChildState::Down(down));
        match next {
            AfterDown::StaysDown => return,
            AfterDown::BootsAgainAfter(wait) => {
                match wait_dropping(supervision, queue, wait).await {
                    Waited::Stopped => return,
                    Waited::Elapsed => {}
                }
            }
        }
    }
}

/// The down state after `lost`, a child that was up since `served_since`.
fn down_after_loss(
    counts: &mut RestartCounts,
    lost: &ChildLost,
    served_since: Instant,
) -> HandlerChildDown {
    let before = match served_since.elapsed() >= HANDLER_CHILD_STABLE {
        true => 0,
        false => counts.losses,
    };
    let losses = NonZeroU32::MIN.saturating_add(before);
    counts.losses = losses.get();
    HandlerChildDown {
        cause: HandlerDownCause::Lost(lost.cause),
        detail: lost.to_string(),
        since: Utc::now(),
        next: AfterDown::BootsAgainAfter(slot_down_delay(losses)),
    }
}

/// The down state after a failed boot: down for good for a cause that
/// repeats, a boot after the backoff for one that can clear.
fn down_after_boot(counts: &mut RestartCounts, failure: &BootFailure) -> HandlerChildDown {
    let next = match failure.cause.kind() {
        CauseKind::Repeats => AfterDown::StaysDown,
        CauseKind::CanClear => {
            let down_boots = NonZeroU32::MIN.saturating_add(counts.down_boots);
            counts.down_boots = down_boots.get();
            AfterDown::BootsAgainAfter(slot_down_delay(down_boots))
        }
    };
    HandlerChildDown {
        cause: HandlerDownCause::BootFailed(failure.cause),
        detail: failure.detail.as_str().to_owned(),
        since: Utc::now(),
        next,
    }
}

/// One log line per down state: an error for a child that stays down, a
/// warning for one that boots again.
fn log_down(down: &HandlerChildDown) {
    match down.next {
        AfterDown::StaysDown => tracing::error!(
            cause = %down.cause,
            detail = %down.detail,
            "the handler child is down until the worker restarts: {}: {}; the worker runs its \
             tasks with no event handler, and its events drop",
            down.cause,
            down.detail,
        ),
        AfterDown::BootsAgainAfter(wait) => tracing::warn!(
            cause = %down.cause,
            detail = %down.detail,
            wait_s = wait.as_secs_f64(),
            "the handler child is down: {}: {}; it boots again in {:.1}s, and its events drop \
             until then",
            down.cause,
            down.detail,
            wait.as_secs_f64(),
        ),
    }
}

/// How a wait before the next boot ended.
enum Waited {
    Elapsed,
    Stopped,
}

/// Wait `wait` before the next boot; drop and count `child_down` each event
/// that arrives meanwhile.
async fn wait_dropping(
    supervision: &Supervision,
    queue: &mut EventQueue,
    wait: Duration,
) -> Waited {
    let until = Instant::now() + wait;
    loop {
        tokio::select! {
            () = tokio::time::sleep_until(until) => return Waited::Elapsed,
            () = supervision.stop.token.cancelled() => return Waited::Stopped,
            batch = queue.next_batch(EVENT_BATCH_MAX) => match batch {
                EventBatch::Events(events) => {
                    supervision.metrics.dropped(EventDropCause::ChildDown, events.len());
                }
                EventBatch::LinkClosed => return wait_without_queue(supervision, until).await,
            },
        }
    }
}

/// The rest of a wait when no event comes again.
async fn wait_without_queue(supervision: &Supervision, until: Instant) -> Waited {
    tokio::select! {
        () = tokio::time::sleep_until(until) => Waited::Elapsed,
        () = supervision.stop.token.cancelled() => Waited::Stopped,
    }
}

/// Close the queue, so each later offer drops its event and counts
/// `child_down`, and count the events that wait in it as `child_down`.
pub(super) fn close_and_count(metrics: &HandlerChildMetrics, queue: &mut EventQueue) {
    queue.close();
    // A closed queue takes no new event, so its events end within the
    // passes of a full queue.
    for _pass in 0..STOP_FLUSH_PASSES {
        let events = queue.take_waiting(EVENT_BATCH_MAX);
        match events.len() {
            0 => return,
            count => metrics.dropped(EventDropCause::ChildDown, count),
        }
    }
}
