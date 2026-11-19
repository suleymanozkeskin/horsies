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
use horsies_core::{BodyOutcome, BodyRun, BodyScope, BodySpans, OutcomeCode, RunWorkflow};
use opentelemetry::global;
use opentelemetry::trace::Status;
use tracing_opentelemetry::{OpenTelemetrySpanExt, SetParentError};

use super::carrier::{attempt_parent_of, AttemptParent};
use super::{
    ERROR_TYPE_KEY, MESSAGING_DESTINATION_NAME_KEY, MESSAGING_MESSAGE_ID_KEY,
    MESSAGING_OPERATION_TYPE_KEY, MESSAGING_SYSTEM, MESSAGING_SYSTEM_KEY, WORKFLOW_ID_KEY,
};

/// The `tracing` target of the attempt span of each task body. A filter of
/// the binary's subscriber passes it at `INFO` so that every attempt has its
/// span; `horsies::cli::run` adds that directive itself.
pub const ATTEMPT_SPAN_TARGET: &str = "horsies::attempt";

/// The directive of the filter of `horsies::cli::run` that keeps the
/// attempt span at every log level.
#[cfg(feature = "cli")]
const ATTEMPT_SPAN_DIRECTIVE: &str = "horsies::attempt=info";

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

/// The filter directive that keeps the attempt span at every log level.
///
/// # Panics
///
/// Never: the directive is a fixed text that a test parses.
#[cfg(feature = "cli")]
pub(crate) fn attempt_span_directive() -> tracing_subscriber::filter::Directive {
    ATTEMPT_SPAN_DIRECTIVE
        .parse()
        .expect("the attempt span directive is a valid filter directive")
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

    fn close(&self, span: &tracing::Span, end: &BodyOutcome<'_>) {
        close_attempt_span(span, end);
    }
}

/// The attempt span of `run`, named `run <task_name>`, of kind CONSUMER,
/// with the parent `parent` (see the module docs). A span that no layer of
/// `tracing-opentelemetry` 0.34 records keeps no parent: it reaches no
/// OpenTelemetry span.
pub(super) fn attempt_span(run: &BodyRun<'_>, parent: AttemptParent) -> tracing::Span {
    let name = format!("run {}", run.task_name);
    let span = tracing::info_span!(
        target: ATTEMPT_SPAN_TARGET,
        parent: None,
        "run",
        otel.name = name.as_str(),
        otel.kind = "consumer",
        horsies.task.id = %run.task_id,
        horsies.task.name = run.task_name,
        horsies.task.attempt = run.attempt.get(),
    );
    span.set_attribute(EXECUTION_KEY, run.execution.as_str());
    span.set_attribute(WORKER_ID_KEY, run.worker_id.to_owned());
    span.set_attribute(LabelName::Queue.attribute_key(), run.queue_name.to_owned());
    span.set_attribute(MESSAGING_SYSTEM_KEY, MESSAGING_SYSTEM);
    span.set_attribute(MESSAGING_OPERATION_TYPE_KEY, PROCESS_OPERATION);
    span.set_attribute(MESSAGING_DESTINATION_NAME_KEY, run.queue_name.to_owned());
    span.set_attribute(MESSAGING_MESSAGE_ID_KEY, run.task_id.to_string());
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

/// Set `horsies.workflow.id`, `horsies.workflow.node` (the node index) and
/// `horsies.workflow.root_id` on `span` for a task that a workflow node
/// links; nothing for another task.
fn set_workflow_attributes(span: &tracing::Span, workflow: RunWorkflow) {
    match workflow {
        RunWorkflow::NotInWorkflow => {}
        RunWorkflow::InWorkflow {
            workflow_id,
            node_index,
            root_workflow_id,
        } => {
            span.set_attribute(WORKFLOW_ID_KEY, workflow_id.to_string());
            span.set_attribute(WORKFLOW_NODE_KEY, i64::from(node_index.get()));
            span.set_attribute(WORKFLOW_ROOT_ID_KEY, root_workflow_id.to_string());
        }
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

/// Set the status of the attempt span by how its body ended: OK for a
/// returned value; ERROR with `error.type` for an error, a panic
/// (`UNHANDLED_EXCEPTION`) and a run past its deadline (`TASK_TIMEOUT`).
pub(super) fn close_attempt_span(span: &tracing::Span, end: &BodyOutcome<'_>) {
    match end {
        BodyOutcome::Returned => span.set_status(Status::Ok),
        BodyOutcome::Failed { error_code } => mark_error(span, error_code.to_string()),
        BodyOutcome::TimedOut => mark_error(span, OutcomeCode::TaskTimeout.to_string()),
    }
}

/// Set the status ERROR and `error.type` on `span`.
fn mark_error(span: &tracing::Span, error_type: String) {
    span.set_attribute(ERROR_TYPE_KEY, error_type);
    span.set_status(Status::error(""));
}

#[cfg(test)]
mod tests;
