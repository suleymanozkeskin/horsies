//! A Rust child of the `handler` kind: it runs the event handlers of the
//! app and no task (Python: the handler branch of the child program).
//!
//! Boot, after the log setup and the producer handle of the child:
//!
//! 1. the child's runtime;
//! 2. the start hooks of the handler child, each with the facts
//!    [`ChildStartFacts::Handler`] and bounded by
//!    [`CHILD_START_HOOK_BOUND`]; a failure ends the child with exit code
//!    173, and the worker keeps the handler child down and runs on;
//! 3. the thread and the queue of each handler ([`HANDLER_QUEUE_EVENTS`]
//!    events);
//! 4. the writer and the control thread of the socket;
//! 5. the ready frame.
//!
//! Service: each event of an `events` frame is decoded on its own. The span
//! step of an attempt that the worker ended comes first
//! ([`StartedSpans::step`]: the span of a plain task is made; the span of a
//! workflow task goes to the queue of the reader task, which reads its
//! node), then the event goes to the queue of each handler of its type. The
//! dispatch never waits for a handler or for the database. An event that
//! does not decode is logged and reaches no handler. A `handler_stats`
//! frame goes out every [`HANDLER_STATISTICS_INTERVAL`]. A run frame or a second
//! init frame ends the child with the bad-frame code.
//!
//! Stop frame: the queues close; the handlers and the reader task get
//! [`HANDLER_DRAIN`] together for the events and the spans left; each span
//! still waiting for its node is made with no node within
//! [`NODE_READER_STOP_BOUND`]; the last `handler_stats` frame is written
//! within [`HANDLER_LAST_STATS_BOUND`]; the child exits 0.
//!
//! [`NODE_READER_STOP_BOUND`]: super::ended_spans::NODE_READER_STOP_BOUND

use std::io;
use std::num::NonZeroUsize;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use super::ended_spans::{EndedSpans, SpanReadFailures, StartedSpans, NODE_READ_QUEUE_SPANS};
use super::handlers::{
    HandlerChildApp, HandlerTallies, RunningHandlers, HANDLER_DRAIN, HANDLER_ERROR_LOG_EVERY,
    HANDLER_LAST_STATS_BOUND, HANDLER_QUEUE_EVENTS, HANDLER_STATISTICS_INTERVAL,
};
use super::host::{
    boot_failed, run_start_hook, shortage_or, start_child_runtime, AfterHookFailure, ChildHostExit,
    ChildStartFacts, CHILD_START_HOOK_BOUND,
};
use super::link::{start_control_thread, Flushed, FrameWriter};
use super::rss::read_rss;
use super::seats::ServeEnd;
use crate::events::{decode_event, AnyEvent, EventDecodeError, WireValue};
use crate::process_exit::ProcessExit;
use crate::worker::executor::protocol::{
    ChildFrame, FrameReader, HandlerStatsFrame, ParentFrame, ProcessId, ReadyFrame,
    ReportedBootCause,
};

/// The runtime threads of a handler child: the dispatch of the events, the
/// `handler_stats` frames, the reader task of the workflow nodes of spans,
/// and the connections of the producer handle. The handlers run on threads
/// of their own.
const HANDLER_RUNTIME_THREADS: NonZeroUsize = match NonZeroUsize::new(2) {
    Some(threads) => threads,
    None => panic!("2 is not zero"),
};

/// The frames that a handler child holds in each direction: the parent
/// frames that the control thread read and the dispatch did not take yet,
/// and the child frames that the writer did not write yet.
const HANDLER_CHILD_FRAMES: NonZeroUsize = match NonZeroUsize::new(8) {
    Some(frames) => frames,
    None => panic!("8 is not zero"),
};

/// The times of the service of a handler child.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct HandlerChildTimes {
    /// The interval of the `handler_stats` frames.
    pub(super) stats_every: Duration,
    /// After the stop frame: the longest wait for the handlers to take the
    /// events of their queues, and for the reader task to make the spans
    /// of its queue.
    pub(super) drain: Duration,
    /// After the drain: the longest wait for the write of the last
    /// `handler_stats` frame.
    pub(super) last_stats: Duration,
}

impl HandlerChildTimes {
    /// The times of a handler child.
    pub(super) const CHILD: Self = Self {
        stats_every: HANDLER_STATISTICS_INTERVAL,
        drain: HANDLER_DRAIN,
        last_stats: HANDLER_LAST_STATS_BOUND,
    };
}

/// Boot a handler child on `socket`, whose init frame `reader` read, and
/// serve it until the stop frame or the end of the parent. Its writer and
/// control threads end the process through `exit`.
pub(super) fn boot_and_serve_handlers(
    app: HandlerChildApp,
    socket: &UnixStream,
    reader: FrameReader,
    exit: ProcessExit,
) -> ChildHostExit {
    let runtime = match start_child_runtime(HANDLER_RUNTIME_THREADS) {
        Ok(runtime) => runtime,
        Err((cause, detail)) => {
            return boot_failed(socket, cause, "RuntimeStartFailed", &detail);
        }
    };
    boot_and_serve_handlers_on(runtime, app, socket, reader, exit)
}

/// [`boot_and_serve_handlers`] on `runtime`, the runtime of the child.
pub(super) fn boot_and_serve_handlers_on(
    runtime: &tokio::runtime::Runtime,
    app: HandlerChildApp,
    socket: &UnixStream,
    reader: FrameReader,
    exit: ProcessExit,
) -> ChildHostExit {
    let HandlerChildApp {
        handlers,
        start_hook,
        ended_spans,
    } = app;
    if let Err(exit) = run_start_hook(
        runtime,
        start_hook,
        ChildStartFacts::Handler,
        CHILD_START_HOOK_BOUND,
        AfterHookFailure::HandlerChildStaysDown,
    ) {
        return exit;
    }
    let running = match handlers.start(HANDLER_QUEUE_EVENTS) {
        Ok(running) => running,
        Err(failed) => {
            let cause = shortage_or(&failed.error, ReportedBootCause::ThreadStartRefused);
            return boot_failed(socket, cause, "ThreadStartFailed", &failed.to_string());
        }
    };
    let (writer, mut frames) = match start_links(socket, reader, exit) {
        Ok(links) => links,
        Err(error) => {
            let cause = shortage_or(&error, ReportedBootCause::ThreadStartRefused);
            return boot_failed(socket, cause, "ThreadStartFailed", &error.to_string());
        }
    };
    let pid = ProcessId::new(std::process::id())
        .expect("the OS gives a process a pid from 1 to the largest pid_t");
    writer.send_blocking(ChildFrame::Ready(ReadyFrame {
        pid,
        rss: read_rss(),
    }));
    let ended_spans = EndedSpans::of_this_process(ended_spans);
    let end = runtime.block_on(serve_handlers(
        running,
        &ended_spans,
        writer,
        &mut frames,
        HandlerChildTimes::CHILD,
    ));
    // The process runtime is never shut down: the binary exits with the
    // code, so a handler that still runs does not hold the exit.
    match end {
        ServeEnd::Stopped => ChildHostExit::Stopped,
        ServeEnd::BadParentFrame => ChildHostExit::BadParentFrame,
        ServeEnd::ParentGone => ChildHostExit::ParentGone,
    }
}

/// Start the writer thread and the control thread of `socket`; both end the
/// process through `exit`.
fn start_links(
    socket: &UnixStream,
    reader: FrameReader,
    exit: ProcessExit,
) -> io::Result<(FrameWriter, mpsc::Receiver<ParentFrame>)> {
    let writer = FrameWriter::start(
        socket.try_clone()?,
        HANDLER_CHILD_FRAMES.get(),
        exit.clone(),
    )?;
    let frames = start_control_thread(
        socket.try_clone()?,
        reader,
        HANDLER_CHILD_FRAMES.get(),
        exit,
    )?;
    Ok((writer, frames))
}

/// Serve a ready handler child: the events of the parent frames go to the
/// handlers of `running`, after the span step of `ended_spans` for an
/// attempt that the worker ended, and a `handler_stats` frame goes out each
/// `times.stats_every`, until the stop frame, the end of the control thread
/// or a frame that a handler child does not take. After the stop frame the
/// handlers and the reader task of the spans get `times.drain` together,
/// and the last `handler_stats` frame is written within `times.last_stats`.
pub(super) async fn serve_handlers(
    running: RunningHandlers,
    ended_spans: &EndedSpans,
    writer: FrameWriter,
    frames: &mut mpsc::Receiver<ParentFrame>,
    times: HandlerChildTimes,
) -> ServeEnd {
    let tallies = running.tallies();
    let read_failures = ended_spans.read_failures();
    let stats = tokio::spawn(send_stats_every(
        writer.clone(),
        Counts {
            tallies: tallies.clone(),
            read_failures: Arc::clone(&read_failures),
        },
        times.stats_every,
    ));
    let spans = ended_spans.start(NODE_READ_QUEUE_SPANS);
    let end = dispatch_frames(&running, &spans, frames).await;
    stats.abort();
    // The task ends at an await, so no stats frame of it follows the last
    // one.
    match stats.await {
        Ok(()) => {}
        Err(joined) if joined.is_panic() => std::panic::resume_unwind(joined.into_panic()),
        Err(_aborted) => {}
    }
    match end {
        ServeEnd::Stopped => {
            let ((), _drained) = tokio::join!(spans.stop(times.drain), running.stop(times.drain));
            let counts = Counts {
                tallies,
                read_failures,
            };
            send_last_stats(&writer, &counts, times.last_stats).await;
            ServeEnd::Stopped
        }
        ServeEnd::BadParentFrame | ServeEnd::ParentGone => end,
    }
}

/// Take the parent frames until the stop frame, the end of the control
/// thread, or a frame that a handler child does not take. Each decoded
/// event gets its span step first, then goes to the handlers.
async fn dispatch_frames(
    running: &RunningHandlers,
    spans: &StartedSpans,
    frames: &mut mpsc::Receiver<ParentFrame>,
) -> ServeEnd {
    let mut undecoded: u64 = 0;
    loop {
        match frames.recv().await {
            None => return ServeEnd::ParentGone,
            Some(ParentFrame::Stop) => return ServeEnd::Stopped,
            Some(ParentFrame::Events(frame)) => {
                for text in frame.events.events() {
                    match decode_wire_event(text) {
                        Ok(event) => {
                            let _step = spans.step(&event);
                            running.offer(&event);
                        }
                        Err(refusal) => {
                            undecoded = undecoded.saturating_add(1);
                            log_undecoded(undecoded, &refusal);
                        }
                    }
                }
            }
            Some(ParentFrame::CancelRun(target)) => {
                // A handler child runs no task: the cancel names no run here.
                tracing::debug!(
                    cancel = ?target,
                    "the handler child got a cancel_run; it runs no task, so the cancel is ignored",
                );
            }
            Some(ParentFrame::Init(_) | ParentFrame::Run(_) | ParentFrame::SharedRun(_)) => {
                tracing::error!(
                    "the handler child got an init or a run frame, which it does not take; it \
                     exits",
                );
                return ServeEnd::BadParentFrame;
            }
        }
    }
}

/// Why one event of an `events` frame reaches no handler.
#[derive(Debug, thiserror::Error)]
pub(super) enum UndecodedEvent {
    /// The text of the event is not JSON. The frame decoder checked the
    /// body, so this is a defect of the sender.
    #[error("the event is not JSON: {0}")]
    NotJson(String),
    /// The event is not an event of this contract version.
    #[error(transparent)]
    Refused(EventDecodeError),
}

/// The event of the wire text `text` of one event of an `events` frame.
/// Pure.
///
/// Errors: [`UndecodedEvent`].
pub(super) fn decode_wire_event(text: &str) -> Result<AnyEvent, UndecodedEvent> {
    let wire: WireValue =
        serde_json::from_str(text).map_err(|error| UndecodedEvent::NotJson(error.to_string()))?;
    decode_event(&wire).map_err(UndecodedEvent::Refused)
}

fn log_undecoded(count: u64, refusal: &UndecodedEvent) {
    match count == 1 || count.is_multiple_of(HANDLER_ERROR_LOG_EVERY) {
        true => tracing::error!(
            undecoded = count,
            "an event of an events frame does not decode, so no handler gets it ({count} so far; \
             one line per {HANDLER_ERROR_LOG_EVERY}): {refusal}",
        ),
        false => {}
    }
}

/// The totals that a `handler_stats` frame carries: those of each handler,
/// and the spans made with no workflow node.
struct Counts {
    tallies: HandlerTallies,
    read_failures: Arc<SpanReadFailures>,
}

/// The `handler_stats` frame of `counts` now.
fn stats_frame(counts: &Counts) -> ChildFrame {
    ChildFrame::HandlerStats(HandlerStatsFrame {
        handlers: counts.tallies.counts(),
        span_reads: counts.read_failures.counts(),
        rss: read_rss(),
    })
}

/// Send a `handler_stats` frame of `counts` each `every`, the first one
/// `every` after the start. Runs until it is aborted.
async fn send_stats_every(writer: FrameWriter, counts: Counts, every: Duration) {
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let _tick = ticks.tick().await;
        writer.send(stats_frame(&counts)).await;
    }
}

/// Write the last `handler_stats` frame of `counts`, at most `bound`.
async fn send_last_stats(writer: &FrameWriter, counts: &Counts, bound: Duration) {
    let written = tokio::time::timeout(bound, async {
        writer.send(stats_frame(counts)).await;
        writer.flush().await
    })
    .await;
    match written {
        Ok(Flushed::Written) => {}
        // The writer thread exits the process when its write fails.
        Ok(Flushed::WriterGone) => {}
        Err(_elapsed) => tracing::warn!(
            "the last handler_stats frame was not written within {:.1}s; the handler child \
             exits",
            bound.as_secs_f64(),
        ),
    }
}
