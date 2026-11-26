//! The span of each attempt that the worker ended, made by the client in the
//! handler child.
//!
//! The task child opens the span of an attempt around its body, and a span
//! is exported only when it ends. When the worker ends the attempt itself
//! with no result frame of the child (an isolated task killed at its
//! timeout, a shared child killed for a blocked loop or past its deadlines,
//! a child lost with its exit seen), that span never ends. The
//! `AttemptEnded` event of such an attempt (`AttemptEnd::DecidedByWorker`)
//! carries the facts of its span. An end that the child reported, a shared
//! run that its child ended at the deadline included
//! (`ChildResult::TimedOut`), has the span of the child: the handler child
//! makes none, so one attempt has one span.
//!
//! The core has no OpenTelemetry dependency. A client that traces gives the
//! handler child an [`EndedSpanMaker`] in [`EndedSpanScope::MakeSpans`].
//! For each `AttemptEnded` whose end is `DecidedByWorker`, the handler child
//! calls [`EndedSpanMaker::make`] with the facts of the span
//! ([`EndedAttempt`]). With [`EndedSpanScope::NoSpans`] the child calls
//! nothing and reads no event.
//!
//! The worker does not know the workflow node of a workflow task: the start
//! write of the child read it, and the event carries
//! `WorkflowPlace::WorkflowNotKnown`. For such an attempt the handler child
//! reads the node by the task id through its producer handle
//! (`PostgresBroker::read_task_workflow_node`, one statement bounded by
//! [`crate::TASK_WORKFLOW_NODE_READ_END`]), so the span has the workflow
//! facts of the span of a child. The read never runs on the dispatch of the
//! events:
//!
//! - A plain task reads nothing: the dispatch makes its span, then gives
//!   the event to the handlers.
//! - A workflow task goes to the queue of the reader task (at most
//!   [`NODE_READ_QUEUE_SPANS`]), and the event goes to the handlers at
//!   once. The reader task reads the node of each span in turn, then makes
//!   it. A full queue makes the span at once with
//!   [`EndedWorkflow::WorkflowNotKnown`].
//! - At the stop, the reader task gets the drain of the handlers; then the
//!   span whose node it reads and each span left are made with
//!   [`EndedWorkflow::WorkflowNotKnown`], within [`NODE_READER_STOP_BOUND`].
//!
//! A read that finds no node gives [`EndedWorkflow::WorkflowNotKnown`]. A
//! read that fails (`read_failed`) and a span made with no read
//! (`not_read`) are counted ([`SpanReadFailures`], sent in the
//! `handler_stats` frames to the metric
//! `horsies.spans.worker_ended.read_failures`) and logged: the first of
//! each reason, then one line per `HANDLER_ERROR_LOG_EVERY`.
//!
//! A `make` that panics is caught and logged once per process; the event
//! still goes to the handlers, and the child lives.

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
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
    AnyEvent, AttemptEnd, AttemptEnded, AttemptNumber, RunTrace, WorkerDecision, WorkerIdText,
    WorkflowPlace,
};
use crate::handle::CoreHandle;
use crate::worker::executor::protocol::SpanReadCounts;

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
/// The handler child calls [`EndedSpanMaker::make`] once per attempt: for a
/// plain task on the task that dispatches the events, before the handlers
/// get the event; for a workflow task on the reader task, after the read of
/// its node, or at the stop. A slow `make` delays the events or the spans
/// after it; it must not block on I/O.
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
    /// it: the read failed, found no node, or did not run. The span has no
    /// workflow attribute.
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
    /// The execution class of the child that ran the attempt.
    pub execution: Execution,
    /// The workflow node of the task.
    pub workflow: EndedWorkflow,
    /// The trace carrier that the send stored on the task row.
    pub carrier: &'a BodyCarrier,
    pub reason: WorkerDecision,
    /// The code that the attempt row records for `reason`.
    pub error_code: &'a BuiltInTaskCode,
    /// When the span starts: when the task reached its child (wall clock
    /// of the worker).
    pub started_at: DateTime<Utc>,
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

/// The most spans that wait for the node read of their workflow task. The
/// dispatch never waits for a read: a span that does not fit is made at
/// once with [`EndedWorkflow::WorkflowNotKnown`] and counted `not_read`.
pub const NODE_READ_QUEUE_SPANS: NonZeroUsize = match NonZeroUsize::new(1_000) {
    Some(spans) => spans,
    None => panic!("1,000 is not zero"),
};

/// At the stop, after the drain: the longest wait for the reader task to
/// make the spans left with no workflow node. The reader task is aborted
/// after it.
pub const NODE_READER_STOP_BOUND: Duration = Duration::from_secs(1);

/// Why the node read of a span did not run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NotReadCause {
    /// The node read queue held [`NODE_READ_QUEUE_SPANS`] spans.
    QueueFull,
    /// The handler child stopped before the read ended.
    ChildStopped,
    /// The reader task ended before the child stopped: a defect, which
    /// the panic hook logged.
    ReaderEnded,
}

impl NotReadCause {
    fn text(self) -> &'static str {
        match self {
            NotReadCause::QueueFull => "the node read queue is full",
            NotReadCause::ChildStopped => "the handler child stopped before the read ended",
            NotReadCause::ReaderEnded => "the reader task ended",
        }
    }
}

/// The spans of attempts of workflow tasks that the worker ended, which a
/// handler child made with no workflow node since it started, by reason
/// (`horsies.spans.worker_ended.read_failures` in the worker parent). The
/// reader task records `read_failed`; the dispatch and the stop record
/// `not_read`; the `handler_stats` frames read the totals.
#[derive(Debug, Default)]
pub(super) struct SpanReadFailures {
    read_failed: AtomicU64,
    not_read: AtomicU64,
}

impl SpanReadFailures {
    /// The totals since the child started.
    pub(super) fn counts(&self) -> SpanReadCounts {
        SpanReadCounts {
            read_failed: self.read_failed.load(Ordering::Relaxed),
            not_read: self.not_read.load(Ordering::Relaxed),
        }
    }

    /// Count one failed node read of `task_id`, and log it: the first,
    /// then one line per [`HANDLER_ERROR_LOG_EVERY`].
    fn record_read_failed(&self, task_id: Uuid, cause: &str) {
        let count = self
            .read_failed
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
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

    /// Count one span of `task_id` made with no node read, and log it: the
    /// first, then one line per [`HANDLER_ERROR_LOG_EVERY`].
    fn record_not_read(&self, task_id: Uuid, cause: NotReadCause) {
        let count = self
            .not_read
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        match is_logged(count) {
            true => tracing::warn!(
                task_id = %task_id,
                not_read = count,
                "the span of the attempt of task {task_id} that the worker ended was made with \
                 no workflow node read ({count} so far; one line per \
                 {HANDLER_ERROR_LOG_EVERY}): {}. The span has no workflow attribute.",
                cause.text(),
            ),
            false => {}
        }
    }
}

/// The spans of the attempts that the worker ended in a handler child: the
/// client's scope, the node reads, and the spans made with no node.
pub(super) struct EndedSpans {
    scope: EndedSpanScope,
    nodes: Arc<dyn NodeReads>,
    read_failures: Arc<SpanReadFailures>,
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
            read_failures: Arc::new(SpanReadFailures::default()),
        }
    }

    /// No span and no read.
    #[cfg(test)]
    pub(super) fn none() -> EndedSpans {
        EndedSpans::with_reads(EndedSpanScope::NoSpans, Arc::new(ChildNodeReads::NoHandle))
    }

    /// The spans made with no workflow node, for the `handler_stats`
    /// frames.
    pub(super) fn read_failures(&self) -> Arc<SpanReadFailures> {
        Arc::clone(&self.read_failures)
    }

    /// Start the span step of the dispatch: with
    /// [`EndedSpanScope::MakeSpans`], the reader task of the workflow nodes
    /// on the current runtime, with a queue of `capacity` spans
    /// ([`read_nodes`]); with [`EndedSpanScope::NoSpans`], no task.
    pub(super) fn start(&self, capacity: NonZeroUsize) -> StartedSpans {
        let maker = match &self.scope {
            EndedSpanScope::NoSpans => return StartedSpans::Off,
            EndedSpanScope::MakeSpans(maker) => Arc::clone(maker),
        };
        let (queue, waiting) = mpsc::channel(capacity.get());
        let stop = CancellationToken::new();
        let task = tokio::spawn(read_nodes(ReaderTask {
            waiting,
            parts: ReaderParts {
                maker: Arc::clone(&maker),
                nodes: Arc::clone(&self.nodes),
                read_failures: Arc::clone(&self.read_failures),
            },
            stop: stop.clone(),
        }));
        StartedSpans::On(SpanReader {
            maker,
            queue,
            read_failures: Arc::clone(&self.read_failures),
            stop,
            task,
        })
    }
}

/// What the span step did for one event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EndedSpanStep {
    /// The client made the span.
    Made,
    /// The span of a workflow task waits in the node read queue; the reader
    /// task makes it.
    WaitsForNode,
    /// [`EndedSpanScope::NoSpans`]: the event was not read.
    SpansOff,
    /// The event is not the `AttemptEnded` of an attempt that the worker
    /// ended: no span.
    NotEndedByWorker,
    /// The `make` of the client panicked: the span is not complete.
    Panicked,
}

/// The span step of the dispatch of a serving handler child.
pub(super) enum StartedSpans {
    /// [`EndedSpanScope::NoSpans`].
    Off,
    /// The client makes the spans; the reader task reads the nodes.
    On(SpanReader),
}

/// The client, the queue and the task of the node reads of a serving
/// handler child.
pub(super) struct SpanReader {
    maker: Arc<dyn EndedSpanMaker>,
    queue: mpsc::Sender<AttemptEnded>,
    read_failures: Arc<SpanReadFailures>,
    stop: CancellationToken,
    task: JoinHandle<()>,
}

/// The queue, the parts and the stop of the reader task.
struct ReaderTask {
    waiting: mpsc::Receiver<AttemptEnded>,
    parts: ReaderParts,
    stop: CancellationToken,
}

/// What the reader task makes the spans with.
struct ReaderParts {
    maker: Arc<dyn EndedSpanMaker>,
    nodes: Arc<dyn NodeReads>,
    read_failures: Arc<SpanReadFailures>,
}

impl StartedSpans {
    /// The step of the dispatch for `event`, before the handlers get it:
    /// with [`StartedSpans::On`], the span of an `AttemptEnded` whose end is
    /// `DecidedByWorker`. A plain task gets its span at once. A workflow
    /// task goes to the node read queue; a full queue makes its span at
    /// once with [`EndedWorkflow::WorkflowNotKnown`], counted `not_read`.
    /// Never waits.
    pub(super) fn step(&self, event: &AnyEvent) -> EndedSpanStep {
        let reader = match self {
            StartedSpans::Off => return EndedSpanStep::SpansOff,
            StartedSpans::On(reader) => reader,
        };
        // Every other event type of the union is not an end by the worker.
        let AnyEvent::AttemptEnded(ended) = event else {
            return EndedSpanStep::NotEndedByWorker;
        };
        let Some(facts) = DecidedFacts::of(&ended.end) else {
            return EndedSpanStep::NotEndedByWorker;
        };
        match facts.workflow {
            WorkflowPlace::NotInWorkflow => {
                return make_span(
                    reader.maker.as_ref(),
                    ended,
                    EndedWorkflow::Known(RunWorkflow::NotInWorkflow),
                );
            }
            WorkflowPlace::WorkflowNotKnown => {}
        }
        let cause = match reader.queue.try_send(ended.clone()) {
            Ok(()) => return EndedSpanStep::WaitsForNode,
            Err(TrySendError::Full(_)) => NotReadCause::QueueFull,
            Err(TrySendError::Closed(_)) => NotReadCause::ReaderEnded,
        };
        reader.read_failures.record_not_read(ended.task_id, cause);
        make_span(
            reader.maker.as_ref(),
            ended,
            EndedWorkflow::WorkflowNotKnown,
        )
    }

    /// At the stop, after the dispatch ended: close the node read queue
    /// and give the reader task `drain` for the spans that wait. Then the
    /// reader task makes the span whose node it reads and each span left
    /// with [`EndedWorkflow::WorkflowNotKnown`], counted `not_read`, within
    /// [`NODE_READER_STOP_BOUND`]; after it the task is aborted. A panic of
    /// the reader task goes on to the caller.
    pub(super) async fn stop(self, drain: Duration) {
        let SpanReader {
            queue,
            stop,
            mut task,
            ..
        } = match self {
            StartedSpans::Off => return,
            StartedSpans::On(reader) => reader,
        };
        drop(queue);
        let joined = match tokio::time::timeout(drain, &mut task).await {
            Ok(joined) => joined,
            Err(_elapsed) => {
                stop.cancel();
                match tokio::time::timeout(NODE_READER_STOP_BOUND, &mut task).await {
                    Ok(joined) => joined,
                    Err(_elapsed) => {
                        task.abort();
                        tracing::warn!(
                            "the reader task of the workflow nodes of spans did not end within \
                             {:.1}s after the stop; spans may be lost",
                            NODE_READER_STOP_BOUND.as_secs_f64(),
                        );
                        return;
                    }
                }
            }
        };
        match joined {
            Ok(()) => {}
            Err(failed) if failed.is_panic() => std::panic::resume_unwind(failed.into_panic()),
            Err(_cancelled) => {}
        }
    }
}

/// The reader task: for each span of its queue, read its node (one bounded
/// read of the core), then make it, until the queue is closed and empty.
/// At its stop, the span whose node is read and each span left are made
/// with [`EndedWorkflow::WorkflowNotKnown`], counted `not_read`.
async fn read_nodes(task: ReaderTask) {
    let ReaderTask {
        mut waiting,
        parts,
        stop,
    } = task;
    loop {
        let ended = tokio::select! {
            biased;
            () = stop.cancelled() => break,
            next = waiting.recv() => match next {
                Some(ended) => ended,
                None => return,
            },
        };
        let workflow = tokio::select! {
            biased;
            () = stop.cancelled() => {
                made_with_no_node(&parts, &ended);
                break;
            }
            read = parts.nodes.read(ended.task_id) => {
                workflow_of_read(ended.task_id, &read, &parts.read_failures)
            }
        };
        let _step = make_span(parts.maker.as_ref(), &ended, workflow);
    }
    // Stopped: the queue holds at most its capacity, and takes no more.
    waiting.close();
    while let Ok(ended) = waiting.try_recv() {
        made_with_no_node(&parts, &ended);
    }
}

/// Make the span of `ended` with no workflow node at the stop, counted
/// `not_read`.
fn made_with_no_node(parts: &ReaderParts, ended: &AttemptEnded) {
    parts
        .read_failures
        .record_not_read(ended.task_id, NotReadCause::ChildStopped);
    let _step = make_span(parts.maker.as_ref(), ended, EndedWorkflow::WorkflowNotKnown);
}

/// The workflow node of the span of `task_id` that `read` gave:
/// `WorkflowNotKnown` when the read found no node, or failed (counted
/// `read_failed` in `read_failures`).
fn workflow_of_read(
    task_id: Uuid,
    read: &NodeRead,
    read_failures: &SpanReadFailures,
) -> EndedWorkflow {
    match read {
        NodeRead::Linked(node) => EndedWorkflow::Known(*node),
        NodeRead::NotLinked => EndedWorkflow::WorkflowNotKnown,
        NodeRead::Failed(cause) => {
            read_failures.record_read_failed(task_id, cause);
            EndedWorkflow::WorkflowNotKnown
        }
    }
}

/// Make the span of `ended` with the workflow node `workflow` through
/// `maker`. A panic of the client is caught and logged once per process.
fn make_span(
    maker: &dyn EndedSpanMaker,
    ended: &AttemptEnded,
    workflow: EndedWorkflow,
) -> EndedSpanStep {
    let Some(facts) = DecidedFacts::of(&ended.end) else {
        return EndedSpanStep::NotEndedByWorker;
    };
    let carrier = BodyCarrier::of_trace(ended.task_id, facts.trace);
    let attempt = attempt_of(ended, &facts, workflow, &carrier);
    match catch_body_panic(|| maker.make(&attempt)) {
        Ok(()) => EndedSpanStep::Made,
        Err(panic) => {
            log_make_panic(ended.task_id, &panic.text, &MAKE_PANIC_LOGGED);
            EndedSpanStep::Panicked
        }
    }
}

/// The fields of a `DecidedByWorker` end that the span takes.
struct DecidedFacts<'a> {
    reason: WorkerDecision,
    error_code: &'a BuiltInTaskCode,
    trace: &'a RunTrace,
    placed_at: DateTime<Utc>,
    child_execution: Execution,
    worker_id: &'a WorkerIdText,
    workflow: WorkflowPlace,
}

impl<'a> DecidedFacts<'a> {
    /// The facts of `end` when the worker ended the attempt; `None` for an
    /// end that the child reported (its own span covers the attempt, a
    /// timeout included) or that is not known. Pure.
    fn of(end: &'a AttemptEnd) -> Option<DecidedFacts<'a>> {
        match end {
            AttemptEnd::DecidedByWorker {
                reason,
                error_code,
                trace,
                placed_at,
                child_execution,
                worker_id,
                workflow,
            } => Some(DecidedFacts {
                reason: *reason,
                error_code,
                trace,
                placed_at: *placed_at,
                child_execution: *child_execution,
                worker_id,
                workflow: *workflow,
            }),
            AttemptEnd::ReportedByChild { .. } | AttemptEnd::NotKnown { .. } => None,
        }
    }
}

/// Whether this process logged a `make` that panicked.
static MAKE_PANIC_LOGGED: AtomicBool = AtomicBool::new(false);

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
        execution: facts.child_execution,
        workflow,
        carrier,
        reason: facts.reason,
        error_code: facts.error_code,
        started_at: facts.placed_at,
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
