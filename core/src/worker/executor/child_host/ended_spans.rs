//! The span of each attempt that the worker ended, made by the client in the
//! handler child.
//!
//! The task child opens the span of an attempt around its body, and a span
//! is exported only when it ends. When the worker ends the attempt itself
//! (an isolated task killed at its timeout, a shared async child killed for
//! a blocked loop, a child lost with its exit seen), the child is gone and
//! that span never ends. The `AttemptEnded` event of such an attempt
//! (`AttemptEnd::DecidedByWorker`) carries the facts of its span.
//!
//! The core has no OpenTelemetry dependency. A client that traces gives the
//! handler child an [`EndedSpanMaker`] in [`EndedSpanScope::MakeSpans`].
//! For each `AttemptEnded` whose end is `DecidedByWorker`, the dispatch of
//! the handler child calls [`EndedSpanMaker::make`] with the facts of the
//! span ([`EndedAttempt`]), then gives the event to the handlers. With
//! [`EndedSpanScope::NoSpans`] the child calls nothing and reads no event.
//!
//! A `make` that panics is caught and logged once per process; the event
//! still goes to the handlers, and the child lives.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::body_scope::BodyCarrier;
use super::panic::catch_body_panic;
use crate::core::task::Execution;
use crate::core::BuiltInTaskCode;
use crate::events::{
    AttemptEnd, AttemptEnded, AttemptNumber, Event, WorkerDecision, WorkflowPlace,
};

/// What the handler child does for an attempt that the worker ended.
#[derive(Clone)]
pub enum EndedSpanScope {
    /// No span: the event goes to the handlers only.
    NoSpans,
    /// The client makes the span of each such attempt.
    MakeSpans(Arc<dyn EndedSpanMaker>),
}

/// The spans of the attempts that the worker ended, made by the client.
///
/// The handler child calls [`EndedSpanMaker::make`] once per attempt, on
/// the task that dispatches the events, before the handlers get the event.
/// A slow `make` delays the dispatch of the events after it; it must not
/// block on I/O.
pub trait EndedSpanMaker: Send + Sync {
    /// Make and end the span of `attempt`.
    fn make(&self, attempt: &EndedAttempt<'_>);
}

/// The facts of the span of one attempt that the worker ended: the same
/// facts as the span of a child, from the `AttemptEnded` event.
#[derive(Debug, Clone, Copy)]
pub struct EndedAttempt<'a> {
    pub task_id: Uuid,
    pub task_name: &'a str,
    /// The queue of the task row.
    pub queue_name: &'a str,
    /// The attempt, from 1: the retry count of the row plus 1.
    pub attempt: AttemptNumber,
    /// The worker that claimed the task and ended the attempt.
    pub worker_id: &'a str,
    /// The kind of the child that ran the attempt.
    pub execution: Execution,
    /// What the worker knows of the workflow node of the task. The worker
    /// does not know the node of a workflow task
    /// ([`WorkflowPlace::WorkflowNotKnown`]): the start of the child read it.
    pub workflow: WorkflowPlace,
    /// The trace carrier that the send stored on the task row.
    pub carrier: &'a BodyCarrier,
    pub reason: WorkerDecision,
    /// The code that the attempt row records for `reason`.
    pub error_code: &'a BuiltInTaskCode,
    /// When the task reached its child (wall clock of the worker): the
    /// start of the span.
    pub placed_at: DateTime<Utc>,
    /// When the end reached the worker (wall clock of the worker): the end
    /// of the span.
    pub ended_at: DateTime<Utc>,
}

/// What the scope did for one event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EndedSpanStep {
    /// The client made the span.
    Made,
    /// [`EndedSpanScope::NoSpans`]: the event was not read.
    SpansOff,
    /// The event is not the `AttemptEnded` of an attempt that the worker
    /// ended: no span.
    NotEndedByWorker,
    /// The `make` of the client panicked: the span is not complete.
    Panicked,
}

/// Whether this process logged a `make` that panicked.
static MAKE_PANIC_LOGGED: AtomicBool = AtomicBool::new(false);

impl EndedSpanScope {
    /// The step of the handler child for `event`, before the handlers get
    /// it: with [`EndedSpanScope::MakeSpans`], the span of an
    /// `AttemptEnded` whose end is `DecidedByWorker`. A panic of the client
    /// is caught and logged once per process.
    pub(super) fn make_for(&self, event: &Event) -> EndedSpanStep {
        let spans = match self {
            EndedSpanScope::NoSpans => return EndedSpanStep::SpansOff,
            EndedSpanScope::MakeSpans(spans) => spans,
        };
        let ended = match event {
            Event::AttemptEnded(ended) => ended,
            // Every other event type of the union.
            _ => return EndedSpanStep::NotEndedByWorker,
        };
        match ended_attempt_of(ended, |attempt| catch_body_panic(|| spans.make(attempt))) {
            None => EndedSpanStep::NotEndedByWorker,
            Some(Ok(())) => EndedSpanStep::Made,
            Some(Err(panic)) => {
                log_make_panic(ended.task_id, &panic.text, &MAKE_PANIC_LOGGED);
                EndedSpanStep::Panicked
            }
        }
    }
}

/// Call `with` with the span facts of `ended` when the worker ended the
/// attempt; `None` for an end that the child reported or that is not
/// known. The stored carrier is read here (`BodyCarrier::of_trace`, which
/// logs a text that does not read).
fn ended_attempt_of<T>(
    ended: &AttemptEnded,
    with: impl FnOnce(&EndedAttempt<'_>) -> T,
) -> Option<T> {
    let (reason, error_code, trace, placed_at, child_kind, worker_id, workflow) = match &ended.end {
        AttemptEnd::DecidedByWorker {
            reason,
            error_code,
            trace,
            placed_at,
            child_kind,
            worker_id,
            workflow,
        } => (
            *reason,
            error_code,
            trace,
            *placed_at,
            *child_kind,
            worker_id,
            *workflow,
        ),
        AttemptEnd::ReportedByChild { .. } | AttemptEnd::NotKnown { .. } => return None,
    };
    let carrier = BodyCarrier::of_trace(ended.task_id, trace);
    let attempt = EndedAttempt {
        task_id: ended.task_id,
        task_name: ended.task_name.as_str(),
        queue_name: ended.queue.as_str(),
        attempt: ended.attempt,
        worker_id: worker_id.as_str(),
        execution: child_kind,
        workflow,
        carrier: &carrier,
        reason,
        error_code,
        placed_at,
        ended_at: ended.at,
    };
    Some(with(&attempt))
}

/// Log at ERROR, once per `logged`, that the client panicked when it made
/// the span of an attempt of task `task_id`.
fn log_make_panic(task_id: Uuid, text: &str, logged: &AtomicBool) {
    match logged.swap(true, Ordering::Relaxed) {
        true => {}
        false => tracing::error!(
            task_id = %task_id,
            "the span of the attempt of task {task_id} that the worker ended panicked: {text}. \
             The event still goes to the handlers; this span is not complete. Later panics are \
             not logged.",
        ),
    }
}
