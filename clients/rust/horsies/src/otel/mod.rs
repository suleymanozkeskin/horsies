//! OpenTelemetry for the Rust client (feature `otel`).
//!
//! With the feature on, the client records:
//! - one span of kind PRODUCER around each task send, `send <task_name>`,
//!   and around each workflow start, `start <workflow_name>`. The send
//!   hands the core the carrier that the global propagator injects for that
//!   span, with its trace id; the task row (or, for a start, every row of
//!   the workflow tree) stores it.
//! - the producer metrics of the core's catalog: `horsies.task.sent` (task,
//!   queue, result) and `horsies.task.send.duration` (queue), one point of
//!   each per send that reached the database calls.
//! - a rerun ([`Horsies::rerun_task`], [`rerun_task_in`]) hands the core the
//!   carrier that the global propagator injects for the caller's current
//!   context, with its trace id; it opens no span. The new task stores it,
//!   so the attempt spans of the rerun are children of the caller's span.
//! - in a worker child, one span of kind CONSUMER around each task body,
//!   `run <task_name>`. It is a `tracing` span (target
//!   [`ATTEMPT_SPAN_TARGET`]) whose OpenTelemetry parent is the context
//!   that the global propagator extracts from the stored carrier; a task
//!   with no carrier gets a root span. The body is polled (async) or runs
//!   (blocking) inside it, so a `tracing` span that the body opens, and an
//!   `#[instrument]` function that it calls, is its child.
//!
//! The tracer, the meter and the propagator are the global ones of
//! `opentelemetry` 0.33 (`opentelemetry::global`): set them with that
//! version, in every process that sends and in every worker child. `main`
//! runs in each child before `horsies::cli::run`, so a provider that `main`
//! sets reaches the children; a provider set in an `on_child_start` hook
//! applies too, because the tracer is looked up at each span. Without a
//! provider the spans are not recorded, and a send stores no carrier.
//!
//! The attempt span reaches OpenTelemetry through a `tracing-opentelemetry`
//! layer in the process's `tracing` subscriber:
//! - `horsies::cli::run` adds [`layer`] to the subscriber that it installs,
//!   and keeps the attempt span at every `--log-level`.
//! - A binary that installs its own subscriber adds [`layer`] to it, and
//!   lets its filter pass the target [`ATTEMPT_SPAN_TARGET`] at `INFO`. A
//!   layer of another `tracing-opentelemetry` version records the span, but
//!   the span cannot set its parent through it.
//!
//! The spans end in the user's span processor. A worker child exits after
//! `horsies::cli::run` returns, so a batch processor exports the last spans
//! of a child only when `main` shuts the provider down after `run`.
//!
//! The attempt span has `horsies.task.id`, `horsies.task.name`,
//! `horsies.task.attempt` (from 1), `horsies.task.execution`,
//! `horsies.queue`, `horsies.worker.id`, `messaging.system`,
//! `messaging.operation.type` (`process`), `messaging.destination.name` (the
//! queue) and `messaging.message.id`, and, for a task that a workflow node
//! links, `horsies.workflow.id`, `horsies.workflow.node` (the node index) and
//! `horsies.workflow.root_id` (the top-level workflow of its tree), as the
//! start read them. The task id, the task name and the attempt are fields of
//! the `tracing` span, so each log record of the body carries them. Its
//! status stays unset when the body returned a value, and is ERROR with `error.type`
//! (the error code) when it returned an error, panicked
//! (`UNHANDLED_EXCEPTION`) or ran past its deadline (`TASK_TIMEOUT`).
//!
//! The span of an attempt that the worker ended itself with no result
//! frame of the child (an isolated task killed at its timeout, a shared
//! child killed for a blocked loop or past its deadlines, a child lost with
//! its exit seen) never ends in its child. The handler child of the worker
//! makes a span for it from the `AttemptEnded` event, with the global
//! tracer provider of that process: `run <task_name>` of kind CONSUMER
//! under the send span, from the time the task reached its child to the
//! time the end reached the worker,
//! status ERROR with `error.type`, the attributes of the span of a child
//! with the kind of the child as `horsies.task.execution`, and
//! `horsies.span.source` = `worker`. For a workflow task the handler child
//! reads the node by the task id (one bounded read of the core, only for
//! such an attempt, on its reader task: never on the dispatch of the
//! events) and the span gets the workflow attributes. A read that fails, a
//! full reader queue and the stop leave them out; the worker metric
//! `horsies.spans.worker_ended.read_failures` counts them. A span
//! that the body exported before the kill keeps the lost span as its
//! parent. [`Horsies::set_worker_ended_spans`] turns it off
//! ([`WorkerEndedSpans::NoSpans`]); with the default
//! ([`WorkerEndedSpans::MakeSpans`]) the worker starts its handler child also
//! when the app has no event handler.
//!
//! [`Horsies::set_worker_ended_spans`]: crate::Horsies::set_worker_ended_spans
//! [`Horsies::rerun_task`]: crate::Horsies::rerun_task
//! [`rerun_task_in`]: crate::rerun::rerun_task_in

mod attempt;
mod carrier;
mod ended;
mod send;
mod tracer;

pub use crate::telemetry::ATTEMPT_SPAN_TARGET;
pub(crate) use attempt::body_scope;
pub(crate) use ended::ended_span_scope;
pub use horsies_core::worker_manifest::WorkerEndedSpans;
pub(crate) use send::{rerun_trace, TaskSendSpan, WorkflowStartSpan};
pub use tracer::layer;

/// The name of the instrumentation scope of every span and metric of the
/// client.
pub const INSTRUMENTATION_SCOPE: &str = "horsies";

/// The instrumentation scope of the client: [`INSTRUMENTATION_SCOPE`] with
/// the version of the crate.
fn instrumentation_scope() -> opentelemetry::InstrumentationScope {
    opentelemetry::InstrumentationScope::builder(INSTRUMENTATION_SCOPE)
        .with_version(env!("CARGO_PKG_VERSION"))
        .build()
}

/// `messaging.system` of every span.
const MESSAGING_SYSTEM: &str = "horsies";
const MESSAGING_SYSTEM_KEY: &str = "messaging.system";
const MESSAGING_OPERATION_TYPE_KEY: &str = "messaging.operation.type";
const MESSAGING_OPERATION_NAME_KEY: &str = "messaging.operation.name";
const MESSAGING_DESTINATION_NAME_KEY: &str = "messaging.destination.name";
const MESSAGING_MESSAGE_ID_KEY: &str = "messaging.message.id";
const ERROR_TYPE_KEY: &str = "error.type";
const TASK_ID_KEY: &str = "horsies.task.id";
const WORKFLOW_ID_KEY: &str = "horsies.workflow.id";
const WORKFLOW_NAME_KEY: &str = "horsies.workflow.name";

#[cfg(test)]
mod test_support;
