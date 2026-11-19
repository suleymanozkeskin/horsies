//! The span of each attempt that the worker ended, made in the handler
//! child with the global tracer and propagator.
//!
//! The span of an attempt is a `tracing` span of the task child around the
//! body ([`super::attempt`]). When the worker ends the attempt itself (an
//! isolated task killed at its timeout, a shared async child killed for a
//! blocked loop, a child lost with its exit seen), that child is gone and
//! its span never ends. The handler child makes a span for the attempt from
//! the facts of its `AttemptEnded` event (`horsies_core::EndedAttempt`):
//!
//! - kind CONSUMER, name `run <task_name>`, as the span of a child;
//! - its parent is the context that the global propagator extracts from the
//!   stored carrier, as for the span of a child; a task with no carrier gets
//!   a root span;
//! - it starts when the task reached its child and ends when the end
//!   reached the worker (wall clock of the worker);
//! - status ERROR with `error.type` = the code of the attempt row;
//! - the attributes of the span of a child ([`super::attempt::run_attributes`],
//!   [`super::attempt::workflow_attributes`], and the task id, name and
//!   attempt), with the kind of the child as `horsies.task.execution`, and
//!   `horsies.span.source` = `worker`. The workflow node of a workflow task
//!   is the one that the handler child read (`EndedWorkflow::Known`); with
//!   `EndedWorkflow::WorkflowNotKnown` (a failed read, or no node) the span
//!   has no `horsies.workflow.*` attribute.
//!
//! The span is made with the OpenTelemetry API directly, not through a
//! `tracing` span, so it takes its start and end times.

use std::sync::Arc;
use std::time::SystemTime;

use horsies_core::telemetry::labels::LabelName;
use horsies_core::worker_manifest::WorkerEndedSpans;
use horsies_core::{EndedAttempt, EndedSpanMaker, EndedSpanScope, EndedWorkflow};
use opentelemetry::global;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::{Span as _, SpanBuilder, SpanKind, Status, Tracer};
use opentelemetry::{Context, KeyValue};

use super::attempt::{run_attributes, workflow_attributes};
use super::carrier::{attempt_parent_of, AttemptParent};
use super::{instrumentation_scope, ERROR_TYPE_KEY, TASK_ID_KEY};
use crate::Horsies;

/// The attribute that names who made an attempt span.
const SPAN_SOURCE_KEY: &str = "horsies.span.source";

/// `horsies.span.source` of the span of an attempt that the worker ended.
const WORKER_SPAN_SOURCE: &str = "worker";

/// The attempt of a span, from 1.
const TASK_ATTEMPT_KEY: &str = "horsies.task.attempt";

impl Horsies {
    /// Whether the handler child of a worker of this app makes the span of
    /// each attempt that the worker ended (feature `otel`):
    /// [`WorkerEndedSpans::MakeSpans`], the default with the feature, or
    /// [`WorkerEndedSpans::NoSpans`].
    ///
    /// With `MakeSpans` the worker starts its handler child also when the
    /// app has no event handler, and the child makes the span with the
    /// global tracer provider of that process: set it in `main`, or in a
    /// hook of [`Horsies::on_handler_child_start`]. The handler child exits
    /// after `horsies::cli::run` returns, so a batch processor exports its
    /// last spans only when `main` shuts the provider down after `run`.
    ///
    /// Set it before the hand-over to horsies: the worker and its handler
    /// child build the same app.
    pub fn set_worker_ended_spans(&mut self, spans: WorkerEndedSpans) {
        self.put_worker_ended_spans(spans);
    }
}

/// The span scope of the handler child for the switch `spans`.
pub(crate) fn ended_span_scope(spans: WorkerEndedSpans) -> EndedSpanScope {
    match spans {
        WorkerEndedSpans::MakeSpans => EndedSpanScope::MakeSpans(Arc::new(GlobalEndedSpans)),
        WorkerEndedSpans::NoSpans => EndedSpanScope::NoSpans,
    }
}

/// The spans of the attempts that the worker ended, with the global tracer
/// provider and propagator at each span.
struct GlobalEndedSpans;

impl EndedSpanMaker for GlobalEndedSpans {
    fn make(&self, attempt: &EndedAttempt<'_>) {
        let tracer = global::tracer_with_scope(instrumentation_scope());
        global::get_text_map_propagator(|propagator| {
            make_ended_span(&tracer, propagator, attempt);
        });
    }
}

/// Make and end the span of `attempt` with `tracer`, its parent read with
/// `propagator` (see the module docs).
pub(super) fn make_ended_span<T: Tracer>(
    tracer: &T,
    propagator: &dyn TextMapPropagator,
    attempt: &EndedAttempt<'_>,
) {
    let parent = match attempt_parent_of(attempt.carrier, propagator) {
        AttemptParent::Extracted(context) => context,
        AttemptParent::NoCarrier => Context::new(),
    };
    let builder = SpanBuilder::from_name(format!("run {}", attempt.task_name))
        .with_kind(SpanKind::Consumer)
        .with_start_time(SystemTime::from(attempt.placed_at))
        .with_attributes(ended_span_attributes(attempt));
    let mut span = tracer.build_with_context(builder, &parent);
    span.set_attribute(KeyValue::new(
        ERROR_TYPE_KEY,
        attempt.error_code.to_string(),
    ));
    span.set_status(Status::error(""));
    span.end_with_timestamp(SystemTime::from(attempt.ended_at));
}

/// The attributes of the span of `attempt`: those of the span of a child,
/// the workflow attributes of its node (none with `WorkflowNotKnown`), and
/// `horsies.span.source`. Pure.
pub(super) fn ended_span_attributes(attempt: &EndedAttempt<'_>) -> Vec<KeyValue> {
    let mut attributes = vec![
        KeyValue::new(TASK_ID_KEY, attempt.task_id.to_string()),
        KeyValue::new(
            LabelName::Task.attribute_key(),
            attempt.task_name.to_owned(),
        ),
        KeyValue::new(TASK_ATTEMPT_KEY, i64::from(attempt.attempt.get())),
        KeyValue::new(SPAN_SOURCE_KEY, WORKER_SPAN_SOURCE),
    ];
    attributes.extend(run_attributes(
        attempt.task_id,
        attempt.queue_name,
        attempt.worker_id,
        attempt.execution,
    ));
    match attempt.workflow {
        EndedWorkflow::Known(workflow) => attributes.extend(workflow_attributes(workflow)),
        EndedWorkflow::WorkflowNotKnown => {}
    }
    attributes
}

#[cfg(test)]
mod tests;
