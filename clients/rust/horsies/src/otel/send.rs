//! The spans of the task sends and the workflow starts, and the producer
//! metrics, with the global tracer, meter and propagator.
//!
//! - A task send is one span of kind PRODUCER, `send <task_name>`, around
//!   every database call of the send (a resend of a transient failure
//!   included, so every call carries one carrier). Its parent is the
//!   current OpenTelemetry context of the caller: the context of the
//!   `tracing` span that the caller's future runs in, through a
//!   `tracing-opentelemetry` layer, or one that the caller attached.
//! - A workflow start is one span `start <workflow_name>`.
//! - A call that returns an error sets the status ERROR and `error.type` to
//!   the error's code. A success leaves the status unset.
//! - A task send records `horsies.task.sent` (task, queue, result `ok` or
//!   the send error code) and `horsies.task.send.duration` (queue, seconds
//!   on the monotonic clock over every database call of the send), with
//!   the names, units, help texts, label keys and bucket bounds of the
//!   core's metric catalog.
//!
//! The tracer and the meter are taken from the global providers at each
//! send, so a provider set after the app was built is used from then on.

use std::time::Instant;

use horsies_core::telemetry::catalog::{MetricId, DURATION_BUCKET_BOUNDS};
use horsies_core::telemetry::labels::{LabelName, SEND_OK_RESULT};
use horsies_core::telemetry::trace_carrier::SendTrace;
use horsies_core::{TaskSendErrorCode, WorkflowStartErrorCode};
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, Meter};
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::{SpanBuilder, SpanKind, SpanRef, Status, TraceContextExt, Tracer};
use opentelemetry::{Context, KeyValue};

use super::carrier::send_trace_of;
use super::{
    instrumentation_scope, ERROR_TYPE_KEY, MESSAGING_DESTINATION_NAME_KEY,
    MESSAGING_MESSAGE_ID_KEY, MESSAGING_OPERATION_NAME_KEY, MESSAGING_OPERATION_TYPE_KEY,
    MESSAGING_SYSTEM, MESSAGING_SYSTEM_KEY, TASK_ID_KEY, WORKFLOW_ID_KEY, WORKFLOW_NAME_KEY,
};
use crate::telemetry::{CallEnd, TaskSendSubject, WorkflowStartSubject};

/// `messaging.operation.type` of a send and of a start, and
/// `messaging.operation.name` of a task send.
const SEND_OPERATION: &str = "send";

/// `messaging.operation.name` of a workflow start.
const START_OPERATION: &str = "start";

/// The producer metrics of the core's catalog, as instruments of one meter.
struct ProducerMetrics {
    sent: Counter<u64>,
    send_duration: Histogram<f64>,
}

impl ProducerMetrics {
    /// The instruments of `meter`. The SDK gives back the instrument that a
    /// meter already holds under the same name.
    fn of(meter: &Meter) -> ProducerMetrics {
        let sent = MetricId::TaskSent.spec();
        let send_duration = MetricId::TaskSendDuration.spec();
        ProducerMetrics {
            sent: meter
                .u64_counter(sent.name)
                .with_unit(sent.unit.symbol())
                .with_description(sent.help)
                .build(),
            send_duration: meter
                .f64_histogram(send_duration.name)
                .with_unit(send_duration.unit.symbol())
                .with_description(send_duration.help)
                .with_boundaries(
                    DURATION_BUCKET_BOUNDS
                        .iter()
                        .map(std::time::Duration::as_secs_f64)
                        .collect(),
                )
                .build(),
        }
    }
}

/// The open span of one task send, the start of its duration, and the
/// labels of its metrics.
pub(crate) struct TaskSendSpan {
    context: Context,
    trace: SendTrace,
    task_name: String,
    queue_name: String,
    metrics: ProducerMetrics,
    started: Instant,
}

impl TaskSendSpan {
    /// Open the span of the send `subject`, a child of the caller's current
    /// context, with the global tracer, meter and propagator.
    pub(crate) fn open(subject: &TaskSendSubject<'_>) -> TaskSendSpan {
        let tracer = global::tracer_with_scope(instrumentation_scope());
        let meter = global::meter_with_scope(instrumentation_scope());
        let parent = Context::current();
        global::get_text_map_propagator(|propagator| {
            open_task_send(&tracer, propagator, &meter, &parent, subject)
        })
    }

    /// The trace carrier that the send hands the core.
    pub(crate) fn trace(&self) -> SendTrace {
        self.trace.clone()
    }

    /// End the span with `end`, and record one point of each producer
    /// metric.
    pub(crate) fn end(self, end: CallEnd<TaskSendErrorCode>) {
        let elapsed = self.started.elapsed().as_secs_f64();
        let span = self.context.span();
        let result = match end {
            CallEnd::Succeeded => SEND_OK_RESULT.to_owned(),
            CallEnd::Failed(code) => {
                let code = code.to_string();
                mark_error(&span, code.clone());
                code
            }
        };
        self.metrics.sent.add(
            1,
            &[
                KeyValue::new(LabelName::Task.attribute_key(), self.task_name),
                KeyValue::new(LabelName::Queue.attribute_key(), self.queue_name.clone()),
                KeyValue::new(LabelName::Result.attribute_key(), result),
            ],
        );
        self.metrics.send_duration.record(
            elapsed,
            &[KeyValue::new(
                LabelName::Queue.attribute_key(),
                self.queue_name,
            )],
        );
        span.end();
    }
}

/// [`TaskSendSpan::open`] with the given tracer, propagator, meter and
/// parent context.
fn open_task_send<T>(
    tracer: &T,
    propagator: &dyn TextMapPropagator,
    meter: &Meter,
    parent: &Context,
    subject: &TaskSendSubject<'_>,
) -> TaskSendSpan
where
    T: Tracer,
    T::Span: Send + Sync + 'static,
{
    let task_id = subject.task_id.to_string();
    let builder = SpanBuilder::from_name(format!("send {}", subject.task_name))
        .with_kind(SpanKind::Producer)
        .with_attributes([
            KeyValue::new(MESSAGING_SYSTEM_KEY, MESSAGING_SYSTEM),
            KeyValue::new(MESSAGING_OPERATION_TYPE_KEY, SEND_OPERATION),
            KeyValue::new(MESSAGING_OPERATION_NAME_KEY, SEND_OPERATION),
            KeyValue::new(
                MESSAGING_DESTINATION_NAME_KEY,
                subject.queue_name.to_owned(),
            ),
            KeyValue::new(MESSAGING_MESSAGE_ID_KEY, task_id.clone()),
            KeyValue::new(TASK_ID_KEY, task_id),
            KeyValue::new(
                LabelName::Task.attribute_key(),
                subject.task_name.to_owned(),
            ),
            KeyValue::new(
                LabelName::Queue.attribute_key(),
                subject.queue_name.to_owned(),
            ),
        ]);
    let context = parent.with_span(tracer.build_with_context(builder, parent));
    TaskSendSpan {
        trace: send_trace_of(&context, propagator),
        context,
        task_name: subject.task_name.to_owned(),
        queue_name: subject.queue_name.to_owned(),
        metrics: ProducerMetrics::of(meter),
        started: Instant::now(),
    }
}

/// The open span of one workflow start.
pub(crate) struct WorkflowStartSpan {
    context: Context,
    trace: SendTrace,
}

impl WorkflowStartSpan {
    /// Open the span of the start `subject`, a child of the caller's
    /// current context, with the global tracer and propagator.
    pub(crate) fn open(subject: &WorkflowStartSubject<'_>) -> WorkflowStartSpan {
        let tracer = global::tracer_with_scope(instrumentation_scope());
        let parent = Context::current();
        global::get_text_map_propagator(|propagator| {
            open_workflow_start(&tracer, propagator, &parent, subject)
        })
    }

    /// The trace carrier that the start hands the core.
    pub(crate) fn trace(&self) -> SendTrace {
        self.trace.clone()
    }

    /// End the span with `end`.
    pub(crate) fn end(self, end: CallEnd<WorkflowStartErrorCode>) {
        let span = self.context.span();
        match end {
            CallEnd::Succeeded => {}
            CallEnd::Failed(code) => mark_error(&span, code.to_string()),
        }
        span.end();
    }
}

/// [`WorkflowStartSpan::open`] with the given tracer, propagator and parent
/// context.
fn open_workflow_start<T>(
    tracer: &T,
    propagator: &dyn TextMapPropagator,
    parent: &Context,
    subject: &WorkflowStartSubject<'_>,
) -> WorkflowStartSpan
where
    T: Tracer,
    T::Span: Send + Sync + 'static,
{
    let builder = SpanBuilder::from_name(format!("start {}", subject.workflow_name))
        .with_kind(SpanKind::Producer)
        .with_attributes([
            KeyValue::new(MESSAGING_SYSTEM_KEY, MESSAGING_SYSTEM),
            KeyValue::new(MESSAGING_OPERATION_TYPE_KEY, SEND_OPERATION),
            KeyValue::new(MESSAGING_OPERATION_NAME_KEY, START_OPERATION),
            KeyValue::new(WORKFLOW_ID_KEY, subject.workflow_id.to_string()),
            KeyValue::new(WORKFLOW_NAME_KEY, subject.workflow_name.to_owned()),
        ]);
    let context = parent.with_span(tracer.build_with_context(builder, parent));
    WorkflowStartSpan {
        trace: send_trace_of(&context, propagator),
        context,
    }
}

/// Set the status ERROR and `error.type` on `span`.
fn mark_error(span: &SpanRef<'_>, error_type: String) {
    span.set_attribute(KeyValue::new(ERROR_TYPE_KEY, error_type));
    span.set_status(Status::error(""));
}

#[cfg(test)]
mod tests;
