//! A ready handler child: the writer of the event queue, the
//! `handler_stats` frames of the child, and its stop.
//!
//! The writer takes the events of the queue in batches: at most
//! [`EVENT_BATCH_MAX`] events, or the events that arrived within the flush
//! interval after the first one. It writes one `events` frame per batch and
//! waits for the socket with no time bound, so it never leaves part of a
//! frame: a slow child fills the socket, then the queue, and the offers
//! drop and count `queue_full`. Meanwhile the child may write up to
//! [`HANDLER_MAILBOX_FRAMES`] frames ahead.

use std::collections::HashMap;
use std::time::Duration;

use tokio::time::Instant;

use super::supervise::Supervision;
use super::{
    HandlerChildMetrics, StopSignal, EVENT_BATCH_MAX, HANDLER_MAILBOX_FRAMES, STOP_FLUSH_PASSES,
};
use crate::events::Event;
use crate::telemetry::labels::{EventDropCause, LabelValue, SpanReadFailure};
use crate::telemetry::worker::{EventBatch, EventQueue};
use crate::worker::executor::protocol::{
    ChildFrame, EventBatch as FrameEvents, EventsFrame, HandlerStatsFrame, ParentFrame,
    SpanReadCounts, WireEvent,
};
use crate::worker::executor::slots::booting::lost_after_kill;
use crate::worker::executor::supervisor::{ChildChannel, ChildLost, FrameWait, CHANNEL_LOST_GRACE};

/// How the service of a ready child ended.
pub(super) enum Served {
    /// The stop came; the child was stopped.
    Stopped,
    /// The child ended, or was ended, before the stop.
    Lost(ChildLost),
}

/// The next thing that a ready child or its queue gave.
enum Step {
    Stop,
    Frame(FrameWait),
    Batch(EventBatch),
}

/// Serve a ready child: write the events of `queue` to it and record its
/// `handler_stats` frames, until the stop or its loss. Disposes of the
/// child.
pub(super) async fn serve(
    supervision: &Supervision,
    queue: &mut EventQueue,
    mut channel: ChildChannel,
) -> Served {
    let mut totals = HandlerTotals::default();
    loop {
        let step = tokio::select! {
            biased;
            () = supervision.stop.token.cancelled() => Step::Stop,
            wait = channel.await_frame(None) => Step::Frame(wait),
            batch = queue.next_batch(EVENT_BATCH_MAX) => Step::Batch(batch),
        };
        match step {
            Step::Stop => {
                stop_child(supervision, queue, channel, &mut totals).await;
                return Served::Stopped;
            }
            Step::Frame(wait) => match frame_step(&supervision.metrics, &mut totals, wait) {
                FrameStep::Recorded | FrameStep::Expired => {}
                FrameStep::Violation => return discard(channel).await,
                FrameStep::Lost(lost) => return dispose_lost(channel, lost).await,
            },
            Step::Batch(EventBatch::Events(first)) => {
                let flush = supervision.config.bounds.flush_interval;
                let events = gather(queue, first, flush, &supervision.stop).await;
                match write_events(supervision, &mut channel, &events).await {
                    Written::Sent | Written::NothingToWrite => {}
                    Written::Failed => {
                        let lost = loss_after_failed_write(
                            &supervision.metrics,
                            &mut totals,
                            &mut channel,
                        )
                        .await;
                        return dispose_lost(channel, lost).await;
                    }
                    Written::StopFlushPassed => {
                        let _exit = channel.dispose_lost().await;
                        return Served::Stopped;
                    }
                }
            }
            Step::Batch(EventBatch::LinkClosed) => {
                return serve_frames(supervision, queue, channel, totals).await
            }
        }
    }
}

/// Serve a ready child after the event link is gone: no event comes again.
/// Record its frames until the stop or its loss.
async fn serve_frames(
    supervision: &Supervision,
    queue: &mut EventQueue,
    mut channel: ChildChannel,
    mut totals: HandlerTotals,
) -> Served {
    loop {
        let wait = tokio::select! {
            biased;
            () = supervision.stop.token.cancelled() => {
                stop_child(supervision, queue, channel, &mut totals).await;
                return Served::Stopped;
            }
            wait = channel.await_frame(None) => wait,
        };
        match frame_step(&supervision.metrics, &mut totals, wait) {
            FrameStep::Recorded | FrameStep::Expired => {}
            FrameStep::Violation => return discard(channel).await,
            FrameStep::Lost(lost) => return dispose_lost(channel, lost).await,
        }
    }
}

/// What one frame wait of a ready child gave.
enum FrameStep {
    /// A `handler_stats` frame, now recorded.
    Recorded,
    /// A frame that only a task child sends: the child breaks the protocol.
    Violation,
    /// The child ended, or was ended.
    Lost(ChildLost),
    /// The wait's bound passed with no frame.
    Expired,
}

/// Record a `handler_stats` frame of `wait`, or name what else it gave.
fn frame_step(
    metrics: &HandlerChildMetrics,
    totals: &mut HandlerTotals,
    wait: FrameWait,
) -> FrameStep {
    match wait {
        FrameWait::Frame(ChildFrame::HandlerStats(stats)) => {
            totals.record(metrics, &stats);
            FrameStep::Recorded
        }
        FrameWait::Frame(
            ChildFrame::Ready(_)
            | ChildFrame::BootFailed(_)
            | ChildFrame::Result(_)
            | ChildFrame::BodyReturned(_)
            | ChildFrame::LeftWorkEnded(_)
            | ChildFrame::LeftWorkStuck(_)
            | ChildFrame::SharedResult(_)
            | ChildFrame::DeadlineFired(_)
            | ChildFrame::SecondCancel(_)
            | ChildFrame::RunStuck(_)
            | ChildFrame::LeftoverCancel(_)
            | ChildFrame::LeftoverStuck(_)
            | ChildFrame::LoopBlocked(_),
        ) => {
            tracing::error!(
                "the handler child sent a frame that only a task child sends; it is replaced"
            );
            FrameStep::Violation
        }
        FrameWait::Lost(lost) => FrameStep::Lost(lost),
        FrameWait::Expired => FrameStep::Expired,
    }
}

/// Kill a child that broke the protocol and dispose of it.
async fn discard(mut channel: ChildChannel) -> Served {
    let lost = channel.discard_violating_child().await;
    let _exit = channel.dispose().await;
    Served::Lost(lost)
}

/// Kill what a lost child left and dispose of it.
async fn dispose_lost(channel: ChildChannel, lost: ChildLost) -> Served {
    let _exit = channel.dispose_lost().await;
    Served::Lost(lost)
}

/// The events of one frame: `first`, then the events that arrive within
/// `flush` after it, at most [`EVENT_BATCH_MAX`] in all. The stop ends the
/// wait: the stop writes the batch.
async fn gather(
    queue: &mut EventQueue,
    mut events: Vec<Event>,
    flush: Duration,
    stop: &StopSignal,
) -> Vec<Event> {
    let until = Instant::now() + flush;
    loop {
        let room = EVENT_BATCH_MAX.get().saturating_sub(events.len());
        let Some(room) = std::num::NonZeroUsize::new(room) else {
            return events;
        };
        tokio::select! {
            () = tokio::time::sleep_until(until) => return events,
            () = stop.token.cancelled() => return events,
            more = queue.next_batch(room) => match more {
                EventBatch::Events(more) => events.extend(more),
                EventBatch::LinkClosed => return events,
            },
        }
    }
}

/// The `events` frame of `events`, with the number of events that it
/// carries.
enum FrameOfEvents {
    Frame {
        frame: Box<ParentFrame>,
        events: usize,
    },
    /// No event of the batch has a wire form.
    NoEvent,
}

/// The frame of `events`. An event with no wire form, or one over the
/// bound of its wire form, is a defect of the core: it is logged and left
/// out. Pure but for the log.
fn frame_of(events: &[Event]) -> FrameOfEvents {
    let wire: Vec<WireEvent> = events
        .iter()
        .filter_map(|event| match WireEvent::of(event) {
            Ok(wire) => Some(wire),
            Err(refusal) => {
                tracing::error!(
                    event = event.event_type().name,
                    "an event has no place in an events frame and is dropped: {refusal}"
                );
                None
            }
        })
        .collect();
    match wire.len() {
        0 => FrameOfEvents::NoEvent,
        count => {
            let batch = FrameEvents::new(&wire).expect(
                "the writer takes at most EVENT_BATCH_MAX events, the bound of an events frame",
            );
            FrameOfEvents::Frame {
                frame: Box::new(ParentFrame::Events(EventsFrame { events: batch })),
                events: count,
            }
        }
    }
}

/// How the write of one batch ended.
enum Written {
    Sent,
    /// No event of the batch has a wire form; nothing was written.
    NothingToWrite,
    /// The write failed: the child is gone or its socket broke. The events
    /// counted `child_down`.
    Failed,
    /// The stop came and its flush time passed during the write: part of
    /// the frame may be on the socket. The events counted `child_down`.
    StopFlushPassed,
}

/// Write `events` in one frame. The write waits for the socket with no
/// bound, until the flush time of a stop.
async fn write_events(
    supervision: &Supervision,
    channel: &mut ChildChannel,
    events: &[Event],
) -> Written {
    let (frame, count) = match frame_of(events) {
        FrameOfEvents::Frame { frame, events } => (frame, events),
        FrameOfEvents::NoEvent => return Written::NothingToWrite,
    };
    let pid = channel.pid();
    tokio::select! {
        biased;
        sent = channel.send_unbounded(&frame) => match sent {
            Ok(()) => Written::Sent,
            Err(failure) => {
                tracing::warn!(
                    pid = pid.get(),
                    "an events frame could not be written to the handler child: {failure}"
                );
                supervision.metrics.dropped(EventDropCause::ChildDown, count);
                Written::Failed
            }
        },
        () = supervision.stop.flush_passed() => {
            supervision.metrics.dropped(EventDropCause::ChildDown, count);
            Written::StopFlushPassed
        }
    }
}

/// After a failed write: read what the child wrote, then name its end. A
/// child that is alive after [`CHANNEL_LOST_GRACE`] is killed.
async fn loss_after_failed_write(
    metrics: &HandlerChildMetrics,
    totals: &mut HandlerTotals,
    channel: &mut ChildChannel,
) -> ChildLost {
    // The mailbox holds at most its bound of frames, so the frames end
    // within one more wait.
    for _frame in 0..=HANDLER_MAILBOX_FRAMES.get() {
        let wait = channel.await_frame(Some(CHANNEL_LOST_GRACE)).await;
        match frame_step(metrics, totals, wait) {
            FrameStep::Recorded => {}
            FrameStep::Violation => return channel.discard_violating_child().await,
            FrameStep::Lost(lost) => return lost,
            FrameStep::Expired => break,
        }
    }
    lost_after_kill(channel).await
}

/// How the write of the queue and the stop frame ended at the stop.
enum Flushed {
    StopFrameSent,
    WriteFailed,
}

/// Stop a ready child: close the queue, write its events and the stop frame
/// until the flush time of the stop, then wait for the exit at most the
/// stop exit bound and record the last `handler_stats` frames, then kill a
/// child that is still there. The events that are not written count
/// `child_down`.
async fn stop_child(
    supervision: &Supervision,
    queue: &mut EventQueue,
    mut channel: ChildChannel,
    totals: &mut HandlerTotals,
) {
    queue.close();
    let mut in_flight = 0;
    let flush = flush_then_stop_frame(supervision, queue, &mut channel, &mut in_flight);
    let flushed = tokio::time::timeout_at(supervision.stop.flush_end(), flush).await;
    match flushed {
        Ok(Flushed::StopFrameSent) => await_exit(supervision, totals, channel).await,
        Ok(Flushed::WriteFailed) => {
            let _exit = channel.dispose_lost().await;
        }
        Err(_elapsed) => {
            supervision
                .metrics
                .dropped(EventDropCause::ChildDown, in_flight);
            tracing::warn!(
                pid = channel.pid().get(),
                "the handler child did not take its events and the stop frame within {:.1}s; \
                 killing it",
                supervision.config.bounds.stop_flush.as_secs_f64()
            );
            let _exit = channel.dispose_lost().await;
        }
    }
}

/// Write the events of the closed `queue`, then the stop frame.
/// `in_flight` holds the events of the frame in the write, for the count of
/// a write that the flush time ends.
async fn flush_then_stop_frame(
    supervision: &Supervision,
    queue: &mut EventQueue,
    channel: &mut ChildChannel,
    in_flight: &mut usize,
) -> Flushed {
    // The queue is closed, so it empties within the passes of a full queue.
    for _pass in 0..STOP_FLUSH_PASSES {
        let events = queue.take_waiting(EVENT_BATCH_MAX);
        let (frame, count) = match (events.len(), frame_of(&events)) {
            (0, _) => break,
            (_, FrameOfEvents::NoEvent) => continue,
            (_, FrameOfEvents::Frame { frame, events }) => (frame, events),
        };
        *in_flight = count;
        match channel.send_unbounded(&frame).await {
            Ok(()) => *in_flight = 0,
            Err(failure) => {
                tracing::warn!(
                    pid = channel.pid().get(),
                    "an events frame could not be written to the stopping handler child: \
                     {failure}"
                );
                supervision
                    .metrics
                    .dropped(EventDropCause::ChildDown, count);
                *in_flight = 0;
                return Flushed::WriteFailed;
            }
        }
    }
    match channel.send_unbounded(&ParentFrame::Stop).await {
        Ok(()) => Flushed::StopFrameSent,
        Err(_failure) => Flushed::WriteFailed,
    }
}

/// After the stop frame: record the last `handler_stats` frames until the
/// exit, at most the stop exit bound; kill a child that is still there.
async fn await_exit(
    supervision: &Supervision,
    totals: &mut HandlerTotals,
    mut channel: ChildChannel,
) {
    let until = Instant::now() + supervision.config.bounds.stop_exit;
    // The mailbox holds at most its bound of frames.
    for _frame in 0..=HANDLER_MAILBOX_FRAMES.get() {
        let left = until.saturating_duration_since(Instant::now());
        let wait = channel.await_frame(Some(left)).await;
        match frame_step(&supervision.metrics, totals, wait) {
            FrameStep::Recorded => {}
            FrameStep::Lost(_ended) => {
                let _exit = channel.dispose().await;
                return;
            }
            FrameStep::Violation => break,
            FrameStep::Expired => {
                tracing::warn!(
                    pid = channel.pid().get(),
                    "the handler child did not exit {:.1}s after the stop frame; killing it",
                    supervision.config.bounds.stop_exit.as_secs_f64()
                );
                break;
            }
        }
    }
    let _exit = channel.dispose_lost().await;
}

/// The totals of one handler that its child reported last.
#[derive(Debug, Clone, Copy, Default)]
struct Totals {
    errors: u64,
    dropped: u64,
}

/// The totals that the current child reported last: those of each handler,
/// by the position of the handler's label value (at most the declared
/// handlers and the fallback), and those of the span reads. A frame holds
/// totals since the child started, so a counter gets the growth of each
/// total; a total below the last one adds nothing.
#[derive(Debug, Default)]
struct HandlerTotals {
    handlers: HashMap<u32, Totals>,
    span_reads: SpanReadCounts,
}

impl HandlerTotals {
    /// Add the growth of each total of `stats` to the handler counters and
    /// to `horsies.spans.worker_ended.read_failures`, and set the RSS of the
    /// child.
    fn record(&mut self, metrics: &HandlerChildMetrics, stats: &HandlerStatsFrame) {
        let reads = stats.span_reads;
        let last = self.span_reads;
        let _recorded = metrics.span_read_failures.add(
            (SpanReadFailure::ReadFailed,),
            reads.read_failed.saturating_sub(last.read_failed),
        );
        let _recorded = metrics.span_read_failures.add(
            (SpanReadFailure::NotRead,),
            reads.not_read.saturating_sub(last.not_read),
        );
        self.span_reads = reads;
        for counts in stats.handlers.handlers() {
            let label = metrics.handlers.value_of(counts.handler.as_str());
            let last = self
                .handlers
                .get(&label.position())
                .copied()
                .unwrap_or_default();
            let errors = counts.errors.saturating_sub(last.errors);
            let dropped = counts.dropped.saturating_sub(last.dropped);
            let _recorded = metrics.handler_errors.add((label,), errors);
            let _recorded = metrics.handler_dropped.add((label,), dropped);
            let _previous = self.handlers.insert(
                label.position(),
                Totals {
                    errors: counts.errors,
                    dropped: counts.dropped,
                },
            );
        }
        metrics.record_rss(stats.rss);
    }
}
