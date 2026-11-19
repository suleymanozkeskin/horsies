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
//! The worker does not know the workflow node of a workflow task: the start
//! write of the child read it, and the event carries
//! `WorkflowPlace::WorkflowNotKnown`. For such an attempt the handler child
//! reads the node by the task id through its producer handle
//! (`PostgresBroker::read_task_workflow_node`, one statement bounded by
//! [`crate::TASK_WORKFLOW_NODE_READ_END`]), so the span has the workflow
//! facts of the span of a child. A read that fails, or that finds no node,
//! gives [`EndedWorkflow::WorkflowNotKnown`]; a failed read is counted and
//! logged, the first and then one per `HANDLER_ERROR_LOG_EVERY`. A plain
//! task reads nothing. The read is awaited on the dispatch task, so it holds
//! the dispatch of the events after it for at most its bound.
//!
//! A `make` that panics is caught and logged once per process; the event
//! still goes to the handlers, and the child lives.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::body_scope::BodyCarrier;
use super::handlers::{is_logged, HANDLER_ERROR_LOG_EVERY};
use super::panic::catch_body_panic;
use super::producer::{child_producer, ChildProducer};
use crate::broker::claims::RunWorkflow;
use crate::broker::TaskWorkflowNode;
use crate::core::task::Execution;
use crate::core::BuiltInTaskCode;
use crate::events::{
    AttemptEnd, AttemptEnded, AttemptNumber, Event, RunTrace, WorkerDecision, WorkerIdText,
    WorkflowPlace,
};
use crate::handle::CoreHandle;

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

/// The workflow node of the task of an attempt that the worker ended, for
/// its span.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndedWorkflow {
    /// The node is known: `NotInWorkflow` for a plain task (from the
    /// claim), or the node that the handler child read for a workflow task.
    Known(RunWorkflow),
    /// A workflow node links the task, and the handler child has no node for
    /// it: the read failed, or it found no node. The span has no workflow
    /// attribute.
    WorkflowNotKnown,
}

/// The facts of the span of one attempt that the worker ended: the same
/// facts as the span of a child, from the `AttemptEnded` event and, for a
/// workflow task, the node read.
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
    /// The workflow node of the task.
    pub workflow: EndedWorkflow,
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

/// What one node read gave.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum NodeRead {
    /// The node that links the task.
    Linked(RunWorkflow),
    /// No workflow node links the task now.
    NotLinked,
    /// The read failed; the text names the cause.
    Failed(String),
}

/// The future of one node read.
pub(super) type NodeReadFuture = Pin<Box<dyn Future<Output = NodeRead> + Send>>;

/// Reads the workflow node of a task by its id.
pub(super) trait NodeReads: Send + Sync {
    /// The node that links `task_id`, within the bound of the read.
    fn read(&self, task_id: Uuid) -> NodeReadFuture;
}

/// The node reads of a handler child: through the producer handle of the
/// child, or none when this process has no producer handle.
pub(super) enum ChildNodeReads {
    /// `PostgresBroker::read_task_workflow_node` on the producer handle.
    Handle(Arc<CoreHandle>),
    /// The child host published no producer handle in this process: every
    /// read fails.
    NoHandle,
}

impl ChildNodeReads {
    /// The node reads of this process: through the producer handle that the
    /// child host published. Pure read; connects nothing.
    pub(super) fn of_this_process() -> ChildNodeReads {
        match child_producer() {
            ChildProducer::InChild(handle) => ChildNodeReads::Handle(handle),
            ChildProducer::NotInAChild => ChildNodeReads::NoHandle,
        }
    }
}

impl NodeReads for ChildNodeReads {
    fn read(&self, task_id: Uuid) -> NodeReadFuture {
        let handle = match self {
            ChildNodeReads::Handle(handle) => Arc::clone(handle),
            ChildNodeReads::NoHandle => {
                return Box::pin(std::future::ready(NodeRead::Failed(
                    "the handler child has no producer handle".to_owned(),
                )))
            }
        };
        Box::pin(async move {
            let broker = match handle.broker().await {
                Ok(broker) => broker,
                Err(error) => return NodeRead::Failed(error.to_string()),
            };
            match broker.read_task_workflow_node(task_id).await {
                Ok(TaskWorkflowNode::Linked {
                    workflow_id,
                    node_index,
                    root_workflow_id,
                }) => NodeRead::Linked(RunWorkflow::InWorkflow {
                    workflow_id,
                    node_index,
                    root_workflow_id,
                }),
                Ok(TaskWorkflowNode::NotLinked) => NodeRead::NotLinked,
                Err(error) => NodeRead::Failed(error.to_string()),
            }
        })
    }
}

/// The count of the failed node reads of a handler child.
#[derive(Debug, Default)]
pub(super) struct NodeReadFailures {
    count: AtomicU64,
}

impl NodeReadFailures {
    /// The failed reads since the child started.
    #[cfg(test)]
    pub(super) fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Count one failed read of the node of `task_id`, and log it: the
    /// first, then one line per [`HANDLER_ERROR_LOG_EVERY`].
    fn record(&self, task_id: Uuid, cause: &str) {
        let count = self.count.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        match is_logged(count) {
            true => tracing::warn!(
                task_id = %task_id,
                failed_reads = count,
                "the workflow node of task {task_id} was not read for the span of its attempt \
                 that the worker ended ({count} failed reads so far; one line per \
                 {HANDLER_ERROR_LOG_EVERY}): {cause}. The span has no workflow attribute.",
            ),
            false => {}
        }
    }
}

/// The spans of the attempts that the worker ended in a handler child: the
/// client's scope, the node reads, and the count of the failed reads.
pub(super) struct EndedSpans {
    pub(super) scope: EndedSpanScope,
    pub(super) nodes: Arc<dyn NodeReads>,
    pub(super) read_failures: NodeReadFailures,
}

impl EndedSpans {
    /// The spans of `scope` with the node reads of this process.
    pub(super) fn of_this_process(scope: EndedSpanScope) -> EndedSpans {
        EndedSpans::with_reads(scope, Arc::new(ChildNodeReads::of_this_process()))
    }

    /// The spans of `scope` with the node reads `nodes`.
    pub(super) fn with_reads(scope: EndedSpanScope, nodes: Arc<dyn NodeReads>) -> EndedSpans {
        EndedSpans {
            scope,
            nodes,
            read_failures: NodeReadFailures::default(),
        }
    }

    /// No span and no read.
    #[cfg(test)]
    pub(super) fn none() -> EndedSpans {
        EndedSpans::with_reads(EndedSpanScope::NoSpans, Arc::new(ChildNodeReads::NoHandle))
    }
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

/// The fields of a `DecidedByWorker` end that the span takes.
struct DecidedFacts<'a> {
    reason: WorkerDecision,
    error_code: &'a BuiltInTaskCode,
    trace: &'a RunTrace,
    placed_at: DateTime<Utc>,
    child_kind: Execution,
    worker_id: &'a WorkerIdText,
    workflow: WorkflowPlace,
}

impl<'a> DecidedFacts<'a> {
    /// The facts of `end` when the worker ended the attempt; `None` for an
    /// end that the child reported or that is not known. Pure.
    fn of(end: &'a AttemptEnd) -> Option<DecidedFacts<'a>> {
        match end {
            AttemptEnd::DecidedByWorker {
                reason,
                error_code,
                trace,
                placed_at,
                child_kind,
                worker_id,
                workflow,
            } => Some(DecidedFacts {
                reason: *reason,
                error_code,
                trace,
                placed_at: *placed_at,
                child_kind: *child_kind,
                worker_id,
                workflow: *workflow,
            }),
            AttemptEnd::ReportedByChild { .. } | AttemptEnd::NotKnown { .. } => None,
        }
    }
}

/// Whether this process logged a `make` that panicked.
static MAKE_PANIC_LOGGED: AtomicBool = AtomicBool::new(false);

impl EndedSpans {
    /// The step of the handler child for `event`, before the handlers get
    /// it: with [`EndedSpanScope::MakeSpans`], the span of an
    /// `AttemptEnded` whose end is `DecidedByWorker`, after the node read
    /// of a workflow task. A panic of the client is caught and logged once
    /// per process.
    pub(super) async fn make_for(&self, event: &Event) -> EndedSpanStep {
        let spans = match &self.scope {
            EndedSpanScope::NoSpans => return EndedSpanStep::SpansOff,
            EndedSpanScope::MakeSpans(spans) => spans,
        };
        let ended = match event {
            Event::AttemptEnded(ended) => ended,
            // Every other event type of the union.
            _ => return EndedSpanStep::NotEndedByWorker,
        };
        let Some(facts) = DecidedFacts::of(&ended.end) else {
            return EndedSpanStep::NotEndedByWorker;
        };
        let workflow = self.workflow_of(ended.task_id, facts.workflow).await;
        let carrier = BodyCarrier::of_trace(ended.task_id, facts.trace);
        let attempt = attempt_of(ended, &facts, workflow, &carrier);
        match catch_body_panic(|| spans.make(&attempt)) {
            Ok(()) => EndedSpanStep::Made,
            Err(panic) => {
                log_make_panic(ended.task_id, &panic.text, &MAKE_PANIC_LOGGED);
                EndedSpanStep::Panicked
            }
        }
    }

    /// The workflow node of the attempt of task `task_id` whose event
    /// carries `place`: known for a plain task with no read; for a
    /// workflow task, the node that the read gives, or `WorkflowNotKnown`
    /// when the read failed (counted) or found no node.
    async fn workflow_of(&self, task_id: Uuid, place: WorkflowPlace) -> EndedWorkflow {
        match place {
            WorkflowPlace::NotInWorkflow => {
                return EndedWorkflow::Known(RunWorkflow::NotInWorkflow)
            }
            WorkflowPlace::WorkflowNotKnown => {}
        }
        match self.nodes.read(task_id).await {
            NodeRead::Linked(node) => EndedWorkflow::Known(node),
            NodeRead::NotLinked => EndedWorkflow::WorkflowNotKnown,
            NodeRead::Failed(cause) => {
                self.read_failures.record(task_id, &cause);
                EndedWorkflow::WorkflowNotKnown
            }
        }
    }
}

/// The span facts of `ended` with `facts` of its end, the workflow node
/// `workflow` and the carrier `carrier`. Pure.
fn attempt_of<'a>(
    ended: &'a AttemptEnded,
    facts: &DecidedFacts<'a>,
    workflow: EndedWorkflow,
    carrier: &'a BodyCarrier,
) -> EndedAttempt<'a> {
    EndedAttempt {
        task_id: ended.task_id,
        task_name: ended.task_name.as_str(),
        queue_name: ended.queue.as_str(),
        attempt: ended.attempt,
        worker_id: facts.worker_id.as_str(),
        execution: facts.child_kind,
        workflow,
        carrier,
        reason: facts.reason,
        error_code: facts.error_code,
        placed_at: facts.placed_at,
        ended_at: ended.at,
    }
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
