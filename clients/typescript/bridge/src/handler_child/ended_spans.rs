//! The spans of the attempts that the worker ended, made by JavaScript in
//! the handler child.
//!
//! - The core's handler host calls [`EndedSpanMaker::make`] for each
//!   `AttemptEnded` whose end is `DecidedByWorker`. `make` must not block:
//!   this maker posts the facts of the span ([`EndedAttemptRecord`]) to the
//!   package's span maker through a threadsafe function in non-blocking
//!   mode, with a queue of [`ENDED_SPAN_QUEUE_BOUND`] spans.
//! - A full queue, an environment that is closing, or a status that
//!   Node-API refuses leaves the span not made. It is reported as every span
//!   of the package that is not made (`logging::span_not_made`): no metric,
//!   and one record at WARNING per process on `horsies.telemetry`. The event
//!   still goes to the handlers.
//! - The times are epoch microseconds, so the span keeps the precision of
//!   the worker's clock.

use horsies_core::{EndedAttempt, EndedSpanMaker, EndedWorkflow, HANDLER_QUEUE_EVENTS};
use napi::bindgen_prelude::{BigInt, Function};
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::Status;
use napi_derive::napi;

use crate::call::node_api::{call_status, CallStatus};
use crate::child::body_facts::{BodyCarrierRecord, BodyExecution, BodyWorkflowRecord};
use crate::logging::span_not_made::{report_span_not_made, NotMadeSpan};
use crate::values::boundary::epoch_micros_of;

/// The spans that wait for the JavaScript thread at once. The span maker
/// takes the `AttemptEnded` events that the worker decided, as a handler of
/// that type takes its events, so its queue has the bound of the queue of
/// one handler (`HANDLER_QUEUE_EVENTS`, 10,000): room for the ends of the
/// seats of several shared async children killed at once, and for a
/// JavaScript loop that a handler holds for a while.
pub const ENDED_SPAN_QUEUE_BOUND: usize = HANDLER_QUEUE_EVENTS.get();

/// The workflow node of the task of an attempt that the worker ended.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EndedWorkflowRecord {
    /// The node is known: not in a workflow, or the node that the handler
    /// child read.
    Known { workflow: BodyWorkflowRecord },
    /// A workflow node links the task, and the handler child has no node for
    /// it: the span has no workflow attribute.
    WorkflowNotKnown,
}

impl EndedWorkflowRecord {
    /// The record of the core's node. Pure.
    fn of(workflow: EndedWorkflow) -> Self {
        match workflow {
            EndedWorkflow::Known(run) => Self::Known {
                workflow: BodyWorkflowRecord::of(run),
            },
            EndedWorkflow::WorkflowNotKnown => Self::WorkflowNotKnown,
        }
    }
}

/// The facts of the span of one attempt that the worker ended. `attempt`
/// is from 1; `execution` is the kind of the child that ran the attempt;
/// `errorCode` is the code that the attempt row records.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EndedAttemptRecord {
    pub task_id: String,
    pub task_name: String,
    pub queue_name: String,
    pub attempt: u32,
    pub worker_id: String,
    pub execution: BodyExecution,
    pub workflow: EndedWorkflowRecord,
    pub carrier: BodyCarrierRecord,
    pub error_code: String,
    /// When the task reached its child, in epoch microseconds: the start of
    /// the span.
    pub started_at_micros: BigInt,
    /// When the end reached the worker, in epoch microseconds: the end of
    /// the span.
    pub ended_at_micros: BigInt,
}

impl EndedAttemptRecord {
    /// The record of the core's facts. Pure.
    fn of(attempt: &EndedAttempt<'_>) -> Self {
        Self {
            task_id: attempt.task_id.to_string(),
            task_name: attempt.task_name.to_owned(),
            queue_name: attempt.queue_name.to_owned(),
            attempt: attempt.attempt.get(),
            worker_id: attempt.worker_id.to_owned(),
            execution: BodyExecution::of(attempt.execution),
            workflow: EndedWorkflowRecord::of(attempt.workflow),
            carrier: BodyCarrierRecord::of(attempt.carrier),
            error_code: attempt.error_code.to_string(),
            started_at_micros: epoch_micros_of(attempt.started_at),
            ended_at_micros: epoch_micros_of(attempt.ended_at),
        }
    }
}

/// The threadsafe function of the span maker: weak, never throws into
/// Node-API, at most [`ENDED_SPAN_QUEUE_BOUND`] spans waiting.
type SpanFunction = ThreadsafeFunction<
    EndedAttemptRecord,
    (),
    EndedAttemptRecord,
    Status,
    false,
    true,
    ENDED_SPAN_QUEUE_BOUND,
>;

/// The span maker of the handler child: the package's function.
pub struct JsEndedSpans {
    function: SpanFunction,
}

impl JsEndedSpans {
    /// The maker of the package's function `function`. On the JavaScript
    /// thread.
    ///
    /// Failure exits: the `napi::Error` of a function that Node-API cannot
    /// turn into a threadsafe function.
    pub fn register(function: &Function<EndedAttemptRecord, ()>) -> napi::Result<Self> {
        Ok(Self {
            function: function
                .build_threadsafe_function::<EndedAttemptRecord>()
                .callee_handled::<false>()
                .weak::<true>()
                .max_queue_size::<ENDED_SPAN_QUEUE_BOUND>()
                .build()?,
        })
    }
}

/// Report, once per process, that the span of `attempt` was not made for
/// `cause`.
fn not_made(attempt: &EndedAttempt<'_>, cause: &str) {
    let span = NotMadeSpan::WorkerEnded {
        task_id: attempt.task_id.to_string(),
    };
    report_span_not_made(&span, cause);
}

impl EndedSpanMaker for JsEndedSpans {
    fn make(&self, attempt: &EndedAttempt<'_>) {
        let status = self.function.call(
            EndedAttemptRecord::of(attempt),
            ThreadsafeFunctionCallMode::NonBlocking,
        );
        match call_status(status) {
            CallStatus::Queued => {}
            CallStatus::QueueFull => {
                not_made(attempt, "the queue of the JavaScript thread is full")
            }
            CallStatus::Closing => not_made(attempt, "the JavaScript environment is closing"),
            CallStatus::Refused { status } => {
                not_made(attempt, &format!("Node-API refused the call: {status}"));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_queue_has_the_bound_of_the_queue_of_one_handler() {
        assert_eq!(ENDED_SPAN_QUEUE_BOUND, 10_000);
    }

    #[test]
    fn a_known_node_keeps_its_record_and_an_unknown_node_has_none() {
        assert_eq!(
            EndedWorkflowRecord::of(EndedWorkflow::Known(
                horsies_core::RunWorkflow::NotInWorkflow
            )),
            EndedWorkflowRecord::Known {
                workflow: BodyWorkflowRecord::NotInWorkflow
            }
        );
        assert_eq!(
            EndedWorkflowRecord::of(EndedWorkflow::WorkflowNotKnown),
            EndedWorkflowRecord::WorkflowNotKnown
        );
    }
}
