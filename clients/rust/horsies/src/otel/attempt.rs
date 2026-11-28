//! The attempt span of a task body in a worker child: a `tracing` span
//! whose OpenTelemetry parent is the context that the global propagator
//! extracts from the stored carrier (`tracing-opentelemetry`,
//! `OpenTelemetrySpanExt::set_parent`).
//!
//! How `tracing-opentelemetry` 0.34 picks the parent of a span
//! (`OpenTelemetryLayer::parent_context`): an explicit `tracing` parent
//! first; else, for a contextual span, the current OpenTelemetry context
//! (with context activation, the default) or the current `tracing` span;
//! else a root. `set_parent` replaces that parent while the span is not
//! started, and a span starts when it is first entered. So the attempt span
//! is made with `parent: None`, which no current span of the host can
//! change, and gets the extracted context with `set_parent` before the host
//! enters it. Each span that the body opens is contextual: its parent is
//! the attempt span, which the host has entered.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use horsies_core::telemetry::labels::LabelName;
use horsies_core::{
    BodyOutcome, BodyRun, BodyRunKey, BodyScope, BodySpans, Execution, OutcomeCode, RunWorkflow,
};
use opentelemetry::global;
use opentelemetry::trace::Status;
use opentelemetry::KeyValue;
use tracing_opentelemetry::{OpenTelemetrySpanExt, SetParentError};
use uuid::Uuid;

use super::carrier::{attempt_parent_of, AttemptParent};
use super::{
    ERROR_TYPE_KEY, MESSAGING_DESTINATION_NAME_KEY, MESSAGING_MESSAGE_ID_KEY,
    MESSAGING_OPERATION_TYPE_KEY, MESSAGING_SYSTEM, MESSAGING_SYSTEM_KEY, WORKFLOW_ID_KEY,
};
use crate::telemetry::{body_span, ATTEMPT_SPAN_TARGET};

/// `messaging.operation.type` of an attempt.
const PROCESS_OPERATION: &str = "process";
const EXECUTION_KEY: &str = "horsies.task.execution";
const WORKER_ID_KEY: &str = "horsies.worker.id";
/// The index of the workflow node that links the task.
const WORKFLOW_NODE_KEY: &str = "horsies.workflow.node";
/// The top-level workflow of the tree of the node that links the task.
const WORKFLOW_ROOT_ID_KEY: &str = "horsies.workflow.root_id";

/// The body scope of a worker child: each task body runs inside its
/// attempt span.
pub(crate) fn body_scope() -> BodyScope {
    BodyScope::Spans(Arc::new(AttemptSpans))
}

/// The attempt spans of a worker child, with the global propagator.
struct AttemptSpans;

impl BodySpans for AttemptSpans {
    fn open(&self, run: &BodyRun<'_>) -> tracing::Span {
        let parent = global::get_text_map_propagator(|propagator| {
            attempt_parent_of(run.carrier, propagator)
        });
        attempt_span(run, parent)
    }

    fn close(&self, _run: BodyRunKey, span: &tracing::Span, end: &BodyOutcome<'_>) {
        close_attempt_span(span, end);
    }
}

/// The attempt span of `run`: the span of its body ([`body_span`], the
/// same span that a client without `otel` enters), named `run <task_name>`,
/// of kind CONSUMER, with the parent `parent` (see the module docs) and the
/// attributes of the attempt. A span that no layer of
/// `tracing-opentelemetry` 0.34 records keeps no parent: it reaches no
/// OpenTelemetry span.
pub(super) fn attempt_span(run: &BodyRun<'_>, parent: AttemptParent) -> tracing::Span {
    let span = body_span(run);
    let name = format!("run {}", run.task_name);
    let _name = span.record("otel.name", name.as_str());
    let _kind = span.record("otel.kind", "consumer");
    for attribute in run_attributes(run.task_id, run.queue_name, run.worker_id, run.execution) {
        span.set_attribute(attribute.key, attribute.value);
    }
    set_workflow_attributes(&span, run.workflow);
    match parent {
        AttemptParent::NoCarrier => {}
        AttemptParent::Extracted(context) => match span.set_parent(context) {
            Ok(()) => {}
            Err(error) => log_parent_not_set(run, &error, &PARENT_NOT_SET_LOGGED),
        },
    }
    span
}

/// The attributes of an attempt span that are not fields of its `tracing`
/// span ([`body_span`]): the class of the child `execution`, the worker
/// `worker_id`, the queue `queue_name`, and the messaging attributes of the
/// task `task_id`. The span of an attempt that the worker ended has the same
/// set ([`super::ended`]). Pure.
pub(super) fn run_attributes(
    task_id: Uuid,
    queue_name: &str,
    worker_id: &str,
    execution: Execution,
) -> [KeyValue; 7] {
    [
        KeyValue::new(EXECUTION_KEY, execution.as_str()),
        KeyValue::new(WORKER_ID_KEY, worker_id.to_owned()),
        KeyValue::new(LabelName::Queue.attribute_key(), queue_name.to_owned()),
        KeyValue::new(MESSAGING_SYSTEM_KEY, MESSAGING_SYSTEM),
        KeyValue::new(MESSAGING_OPERATION_TYPE_KEY, PROCESS_OPERATION),
        KeyValue::new(MESSAGING_DESTINATION_NAME_KEY, queue_name.to_owned()),
        KeyValue::new(MESSAGING_MESSAGE_ID_KEY, task_id.to_string()),
    ]
}

/// Set the workflow attributes of `workflow` on `span`
/// ([`workflow_attributes`]).
fn set_workflow_attributes(span: &tracing::Span, workflow: RunWorkflow) {
    for attribute in workflow_attributes(workflow) {
        span.set_attribute(attribute.key, attribute.value);
    }
}

/// `horsies.workflow.id`, `horsies.workflow.node` (the node index) and
/// `horsies.workflow.root_id` for a task that a workflow node links; none
/// for another task. The span of an attempt that the worker ended has the
/// same set for the node that the handler child read. Pure.
pub(super) fn workflow_attributes(workflow: RunWorkflow) -> Vec<KeyValue> {
    match workflow {
        RunWorkflow::NotInWorkflow => Vec::new(),
        RunWorkflow::InWorkflow {
            workflow_id,
            node_index,
            root_workflow_id,
        } => vec![
            KeyValue::new(WORKFLOW_ID_KEY, workflow_id.to_string()),
            KeyValue::new(WORKFLOW_NODE_KEY, i64::from(node_index.get())),
            KeyValue::new(WORKFLOW_ROOT_ID_KEY, root_workflow_id.to_string()),
        ],
    }
}

/// Whether this process logged an attempt span that got no parent.
static PARENT_NOT_SET_LOGGED: AtomicBool = AtomicBool::new(false);

/// Log at WARNING, once per `logged`, that the attempt span of `run` keeps
/// no parent:
/// - `LayerNotFound`: no `tracing-opentelemetry` 0.34 layer is in the
///   subscriber;
/// - `SpanDisabled`: the subscriber's filter disabled the span;
/// - `AlreadyStarted`: another layer started the span when it was made.
///
/// The attempt still runs; its span, when recorded, is a root span.
fn log_parent_not_set(run: &BodyRun<'_>, error: &SetParentError, logged: &AtomicBool) {
    match logged.swap(true, Ordering::Relaxed) {
        true => {}
        false => tracing::warn!(
            task_id = %run.task_id,
            error = %error,
            "the attempt span of task {} has no OpenTelemetry parent: {error}. Add \
             horsies::otel::layer() to the tracing subscriber and let its filter pass the \
             target {ATTEMPT_SPAN_TARGET} at INFO. Later attempt spans with no parent are not \
             logged.",
            run.task_id,
        ),
    }
}

/// Set the status of the attempt span by how its body ended: unset for a
/// returned value; ERROR with `error.type` for an error, a panic
/// (`UNHANDLED_EXCEPTION`), a run past its deadline (`TASK_TIMEOUT`) and a
/// run that an operator cancelled (`TASK_CANCELLED`).
pub(super) fn close_attempt_span(span: &tracing::Span, end: &BodyOutcome<'_>) {
    match end {
        BodyOutcome::Returned => {}
        BodyOutcome::Failed { error_code } => mark_error(span, error_code.to_string()),
        BodyOutcome::TimedOut => mark_error(span, OutcomeCode::TaskTimeout.to_string()),
        BodyOutcome::Cancelled => mark_error(span, OutcomeCode::TaskCancelled.to_string()),
    }
}

/// Set the status ERROR and `error.type` on `span`.
fn mark_error(span: &tracing::Span, error_type: String) {
    span.set_attribute(ERROR_TYPE_KEY, error_type);
    span.set_status(Status::error(""));
}

#[cfg(test)]
mod tests;
