//! The scope that a client puts around each task body of a Rust child: the
//! span of the attempt.
//!
//! The core has no OpenTelemetry dependency. A client that traces its task
//! bodies gives the host a [`BodySpans`] in [`BodyScope::Spans`]. For each
//! run whose body starts, the host:
//! 1. asks [`BodySpans::open`] for a `tracing` span, with the facts of the
//!    run (the task, its queue, the attempt), the workflow node that its
//!    start read, and the trace carrier that the send stored ([`BodyRun`]);
//! 2. runs the body inside that span: an async body is polled inside it, a
//!    blocking body runs inside it on its own thread, so a `tracing` span
//!    that the body opens, and an `#[instrument]` function that it calls,
//!    is a child of the span;
//! 3. gives the span back to [`BodySpans::close`] with how the body ended
//!    ([`BodyOutcome`]), then drops its own handle on the span.
//!
//! The span closes when the body's handle on it is dropped too: at the end
//! of the body, or, for a body that runs past its deadline, when it returns
//! or is aborted.
//!
//! A run whose body does not start gets no span: a start that missed, a
//! task that is not registered, an input that does not read, stored options
//! that the worker cannot use, a task whose kind does not fit its seat.
//!
//! With [`BodyScope::NoScope`] the host calls nothing, reads no carrier, and
//! runs the body inside `tracing::Span::none()`, whose enter does nothing.

use std::collections::BTreeMap;
use std::sync::Arc;

use uuid::Uuid;

use super::run::RunFacts;
use crate::broker::claims::RunWorkflow;
use crate::core::task::error::TaskErrorCode;
use crate::core::task::fn_trait::RawTaskResult;
use crate::core::task::result::TaskResult;
use crate::events::{AttemptNumber, RunTrace};
use crate::Execution;

/// What the host does around each task body of a child.
#[derive(Clone)]
pub enum BodyScope {
    /// The body runs with no span of its own.
    NoScope,
    /// Each body runs inside the span that the client opens.
    Spans(Arc<dyn BodySpans>),
}

/// The spans of the task bodies of a child, opened by the client.
///
/// The host calls [`BodySpans::open`] once before each body that starts,
/// and [`BodySpans::close`] once after that body ended, on the thread of
/// the run (a runtime thread for an async body and an isolated blocking
/// body, the seat thread for a shared thread body). A body never sees the
/// client's value, only the span.
pub trait BodySpans: Send + Sync {
    /// The span of one run, before its body starts. The host enters it
    /// while the body runs. A span that no subscriber records
    /// (`tracing::Span::none()`) is valid: the body then runs with no span.
    fn open(&self, run: &BodyRun<'_>) -> tracing::Span;

    /// Record how the body of `span`'s run ended. The host drops its handle
    /// on the span after this call.
    fn close(&self, span: &tracing::Span, end: &BodyOutcome<'_>);
}

/// The facts of one run that the host knows when its body starts.
#[derive(Debug, Clone, Copy)]
pub struct BodyRun<'a> {
    pub task_id: Uuid,
    pub task_name: &'a str,
    /// The queue of the task row.
    pub queue_name: &'a str,
    /// The attempt that the claim runs, from 1: the retry count of the row
    /// plus 1.
    pub attempt: AttemptNumber,
    /// The worker that claimed the task.
    pub worker_id: &'a str,
    /// The execution class of the child that runs the body.
    pub execution: Execution,
    /// The workflow node that links the task, as the start read it.
    pub workflow: RunWorkflow,
    /// The trace carrier that the send stored on the task row.
    pub carrier: &'a BodyCarrier,
}

/// The trace carrier of a run, in the form that a propagator reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyCarrier {
    /// The map that the propagator of the send wrote, each key as it was
    /// written.
    Carrier(BTreeMap<String, String>),
    /// The task row has no carrier: the send had no trace, or the scheduler
    /// inserted the task.
    NoCarrier,
}

impl BodyCarrier {
    /// The carrier of the stored trace `trace` of a run frame.
    ///
    /// The decoder of the run frames takes only a carrier text that reads
    /// as one JSON object of texts, so the read here does not fail for the
    /// trace of a decoded frame. A text that does not read is a defect: it
    /// is logged at ERROR, and the run gets no carrier, because telemetry
    /// never changes how a task ends.
    pub(super) fn of_trace(task_id: Uuid, trace: &RunTrace) -> BodyCarrier {
        let text = match trace {
            RunTrace::NoCarrier => return BodyCarrier::NoCarrier,
            RunTrace::Carrier { text } => text,
        };
        match serde_json::from_str::<BTreeMap<String, String>>(text.as_str()) {
            Ok(carrier) => BodyCarrier::Carrier(carrier),
            Err(error) => {
                tracing::error!(
                    task_id = %task_id,
                    error = %error,
                    "the trace carrier of task {task_id} is not one JSON object of texts, which \
                     the run frame decoder admits only: {error}; the attempt runs with no parent \
                     span",
                );
                BodyCarrier::NoCarrier
            }
        }
    }
}

/// How a body ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyOutcome<'a> {
    /// The body returned a value.
    Returned,
    /// The body returned an error, or panicked (`UNHANDLED_EXCEPTION`).
    Failed { error_code: &'a TaskErrorCode },
    /// The deadline of the run came before the end of the body: the run
    /// ends `TASK_TIMEOUT`, whatever the body returns later.
    TimedOut,
    /// An operator cancelled the task before the end of the body: the task
    /// is `CANCELLED` (`TASK_CANCELLED`), whatever the body returns.
    Cancelled,
}

impl<'a> BodyOutcome<'a> {
    /// How a body ended that returned `result`. Pure.
    pub(super) fn of_result(result: &'a RawTaskResult) -> BodyOutcome<'a> {
        match result {
            TaskResult::Ok(_) => BodyOutcome::Returned,
            TaskResult::Err(error) => BodyOutcome::Failed {
                error_code: &error.error_code,
            },
        }
    }
}

/// What the scope of a body takes besides the facts of its run: the trace
/// carrier that its run frame carried, and the workflow node that its start
/// read.
#[derive(Debug, Clone, Copy)]
pub(super) struct SpanSource<'a> {
    pub(super) trace: &'a RunTrace,
    pub(super) workflow: RunWorkflow,
}

/// The scope of one body, open. [`OpenBody::close`] ends it.
pub(super) enum OpenBody {
    /// No scope: the body runs inside `tracing::Span::none()`.
    Unscoped,
    /// The span that the client opened for the run.
    Scoped {
        spans: Arc<dyn BodySpans>,
        span: tracing::Span,
    },
}

impl BodyScope {
    /// Open the scope of the body of the run `facts` in a child of
    /// `execution`, with the trace carrier and the workflow node of
    /// `source`. With [`BodyScope::NoScope`] nothing is called and the
    /// carrier is not read.
    pub(super) fn open(
        &self,
        facts: &RunFacts,
        execution: Execution,
        source: SpanSource<'_>,
    ) -> OpenBody {
        match self {
            BodyScope::NoScope => OpenBody::Unscoped,
            BodyScope::Spans(spans) => {
                let carrier = BodyCarrier::of_trace(facts.task_id, source.trace);
                let run = BodyRun {
                    task_id: facts.task_id,
                    task_name: &facts.task_name,
                    queue_name: facts.queue_name.as_str(),
                    attempt: facts.attempt,
                    worker_id: &facts.worker_id,
                    execution,
                    workflow: source.workflow,
                    carrier: &carrier,
                };
                let span = spans.open(&run);
                OpenBody::Scoped {
                    spans: Arc::clone(spans),
                    span,
                }
            }
        }
    }
}

impl OpenBody {
    /// A handle on the span that the body runs inside: `Span::none()` with
    /// no scope.
    pub(super) fn span(&self) -> tracing::Span {
        match self {
            OpenBody::Unscoped => tracing::Span::none(),
            OpenBody::Scoped { span, .. } => span.clone(),
        }
    }

    /// Give the client how the body ended, then drop the host's handle on
    /// the span. Nothing is called with no scope.
    pub(super) fn close(self, end: &BodyOutcome<'_>) {
        match self {
            OpenBody::Unscoped => {}
            OpenBody::Scoped { spans, span } => spans.close(&span, end),
        }
    }
}

#[cfg(test)]
mod tests;
