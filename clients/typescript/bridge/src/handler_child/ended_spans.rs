//! The spans of the attempts that the worker ended, made by JavaScript in
//! the handler child.
//!
//! - The core's handler host calls [`EndedSpanMaker::make`] for each
//!   `AttemptEnded` whose end is `DecidedByWorker`. `make` must not block:
//!   this maker posts the facts of the span ([`EndedAttemptRecord`]) to the
//!   package's span maker through a threadsafe function in non-blocking
//!   mode, with a queue of [`ENDED_SPAN_QUEUE_BOUND`] spans.
//! - A full queue, or an environment that is closing, drops the span and
//!   counts it; the first drop is logged, then one per
//!   `HANDLER_ERROR_LOG_EVERY`. A status that Node-API refuses is logged the
//!   same way.
//! - The times are epoch microseconds, so the span keeps the precision of
//!   the worker's clock.

use std::sync::atomic::{AtomicU64, Ordering};

use horsies_core::{
    EndedAttempt, EndedSpanMaker, EndedWorkflow, HANDLER_ERROR_LOG_EVERY, NODE_READ_QUEUE_SPANS,
};
use napi::bindgen_prelude::{BigInt, Function};
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::Status;
use napi_derive::napi;

use crate::call::node_api::{call_status, CallStatus};
use crate::child::body_facts::{BodyCarrierRecord, BodyExecution, BodyWorkflowRecord};
use crate::values::boundary::epoch_micros_of;

/// The spans that wait for the JavaScript thread at once: the most spans
/// that wait for the node read of their workflow task in the core.
pub const ENDED_SPAN_QUEUE_BOUND: usize = NODE_READ_QUEUE_SPANS.get();

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

/// The span maker of the handler child: the package's function, and the
/// count of the spans that did not reach it.
pub struct JsEndedSpans {
    function: SpanFunction,
    dropped: AtomicU64,
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
            dropped: AtomicU64::new(0),
        })
    }

    /// Count one span that did not reach JavaScript, and log the first and
    /// then one per `HANDLER_ERROR_LOG_EVERY`.
    fn count_drop(&self, attempt: &EndedAttempt<'_>, cause: &str) {
        let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
        if is_logged(dropped) {
            tracing::warn!(
                target: "horsies.bridge",
                task_id = %attempt.task_id,
                dropped,
                "the span of an attempt that the worker ended was not made: {cause}; {dropped} \
                 such spans so far"
            );
        }
    }
}

/// Whether the drop number `dropped` (from 1) is logged: the first, then
/// one per `HANDLER_ERROR_LOG_EVERY`. Pure.
fn is_logged(dropped: u64) -> bool {
    dropped == 1 || dropped % HANDLER_ERROR_LOG_EVERY == 0
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
                self.count_drop(attempt, "the queue of the JavaScript thread is full")
            }
            CallStatus::Closing => {
                self.count_drop(attempt, "the JavaScript environment is closing")
            }
            CallStatus::Refused { status } => {
                self.count_drop(attempt, &format!("Node-API refused the call: {status}"));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_drop_and_each_thousandth_are_logged() {
        assert!(is_logged(1));
        assert!(!is_logged(2));
        assert!(!is_logged(999));
        assert!(is_logged(1_000));
        assert!(is_logged(2_000));
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
