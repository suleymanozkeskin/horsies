//! The span of an attempt that the worker ended, in the handler child: the
//! client gets the facts of the span once per such attempt, before the
//! handlers get the event for a plain task; no other event and no scope with
//! no spans calls the client; a client that panics is caught and the
//! handlers still get the event; the node of a workflow task is read on the
//! reader task, never on the dispatch; a failed read, a full queue and the
//! stop give `WorkflowNotKnown` and are counted. No database and no process:
//! the node reads are stubs, and the service writes to a socket pair.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::Semaphore;
use uuid::Uuid;

use super::body_scope::BodyCarrier;
use super::ended_spans::{
    EndedAttempt, EndedSpanMaker, EndedSpanScope, EndedSpanStep, EndedSpans, EndedWorkflow,
    NodeRead, NodeReadFuture, NodeReads, StartedSpans,
};
use super::handler_host::{serve_handlers, HandlerChildTimes};
use super::handlers::EventHandlerSet;
use super::link::FrameWriter;
use super::panic::install_panic_hook;
use super::seats::ServeEnd;
use crate::broker::claims::{RunWorkflow, WorkflowNodeIndex};
use crate::core::config::bounded::BoundedU32;
use crate::core::task::Execution;
use crate::core::{BuiltInTaskCode, OperationalErrorCode, OutcomeCode};
use crate::events::{
    AttemptEnd, AttemptEnded, AttemptStart, ChildResult, Event, RunTrace, SlotRecovered,
    WorkerDecision, WorkflowPlace,
};
use crate::process_exit::ProcessExit;
use crate::worker::executor::protocol::{
    BoundedText, EventBatch, EventsFrame, ParentFrame, ProcessId, SpanReadCounts, WireEvent,
};

/// The longest wait of a test for the service or a handler.
const WAIT: Duration = Duration::from_secs(10);

const TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
const TASK_NAME: &str = "app.tasks.charge";
const QUEUE_NAME: &str = "payments";
const WORKER_ID: &str = "worker-7";

fn placed_at() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_nanos(1_700_000_000_000_123_456)
}

/// The start that a shared child reported for an attempt whose run it
/// ended at the deadline.
fn reported_start() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_nanos(1_700_000_000_250_456_000)
}

/// The end of a shared run that its child ended at the deadline, with its
/// start: the child reported it.
fn timed_out_in_child() -> AttemptEnd {
    AttemptEnd::ReportedByChild {
        result: ChildResult::TimedOut,
        start: AttemptStart::Reported {
            started_at: reported_start(),
        },
    }
}

fn ended_at() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_nanos(1_700_000_030_500_654_321)
}

/// The node that links the task of the workflow tests.
fn linked_node() -> RunWorkflow {
    RunWorkflow::InWorkflow {
        workflow_id: Uuid::from_u128(0xf007),
        node_index: WorkflowNodeIndex::try_from(4).unwrap(),
        root_workflow_id: Uuid::from_u128(0xf001),
    }
}

/// Node reads that answer `answer` and record each task id that they read.
struct StubReads {
    answer: NodeRead,
    reads: Mutex<Vec<Uuid>>,
}

impl StubReads {
    fn answering(answer: NodeRead) -> Arc<StubReads> {
        Arc::new(StubReads {
            answer,
            reads: Mutex::new(Vec::new()),
        })
    }

    fn reads(&self) -> Vec<Uuid> {
        self.reads.lock().unwrap().clone()
    }
}

impl NodeReads for StubReads {
    fn read(&self, task_id: Uuid) -> NodeReadFuture {
        self.reads.lock().unwrap().push(task_id);
        Box::pin(std::future::ready(self.answer.clone()))
    }
}

/// Node reads that hold each read until the test adds a permit to `gate`,
/// then answer `answer`. Each read sends its task id to `entered` when it
/// starts.
struct HeldReads {
    answer: NodeRead,
    gate: Arc<Semaphore>,
    entered: Mutex<Sender<Uuid>>,
}

impl HeldReads {
    /// Held reads that answer `answer`, and the receiver of the reads that
    /// started.
    fn answering(answer: NodeRead) -> (Arc<HeldReads>, Receiver<Uuid>) {
        let (entered, started) = channel();
        let reads = Arc::new(HeldReads {
            answer,
            gate: Arc::new(Semaphore::new(0)),
            entered: Mutex::new(entered),
        });
        (reads, started)
    }

    /// Let every held read and every later read end.
    fn release(&self) {
        self.gate.add_permits(Semaphore::MAX_PERMITS / 2);
    }
}

impl NodeReads for HeldReads {
    fn read(&self, task_id: Uuid) -> NodeReadFuture {
        let _sent = self.entered.lock().unwrap().send(task_id);
        let gate = Arc::clone(&self.gate);
        let answer = self.answer.clone();
        Box::pin(async move {
            let _permit = gate.acquire_owned().await;
            answer
        })
    }
}

/// The spans of `scope` with `reads`.
fn spans_with<R: NodeReads + 'static>(scope: EndedSpanScope, reads: &Arc<R>) -> EndedSpans {
    EndedSpans::with_reads(scope, Arc::clone(reads) as Arc<dyn NodeReads>)
}

/// The spans of a client that records each span to `seen`, with `reads`.
fn recording_spans<R: NodeReads + 'static>(seen: Sender<Seen>, reads: &Arc<R>) -> EndedSpans {
    spans_with(
        EndedSpanScope::MakeSpans(Arc::new(RecordingSpans {
            seen: Mutex::new(seen),
        })),
        reads,
    )
}

/// The span step of `spans` with a queue of `capacity` spans, on the
/// current runtime.
fn started(spans: &EndedSpans, capacity: usize) -> StartedSpans {
    spans.start(NonZeroUsize::new(capacity).unwrap())
}

/// The next value of `receiver` within [`WAIT`], from a task of a
/// multi-thread runtime.
fn next_value<T>(receiver: &Receiver<T>) -> T {
    tokio::task::block_in_place(|| next_values(receiver, 1))
        .pop()
        .unwrap()
}

/// The end of an attempt that the worker ended for `reason`, of a task whose
/// send stored a carrier, on an isolated child.
fn decided(reason: WorkerDecision, workflow: WorkflowPlace) -> AttemptEnd {
    AttemptEnd::DecidedByWorker {
        reason,
        error_code: reason.error_code(),
        trace: RunTrace::Carrier {
            text: BoundedText::new(format!(r#"{{"traceparent":"{TRACEPARENT}"}}"#)).unwrap(),
        },
        placed_at: placed_at(),
        child_kind: Execution::Isolated,
        worker_id: BoundedText::new(WORKER_ID.to_owned()).unwrap(),
        workflow,
    }
}

fn reported() -> AttemptEnd {
    AttemptEnd::ReportedByChild {
        result: ChildResult::Completed,
        start: AttemptStart::NotReported,
    }
}

fn not_known() -> AttemptEnd {
    AttemptEnd::NotKnown {
        child_pid: ProcessId::new(4242).unwrap(),
    }
}

/// Attempt 2 of a task of the class `SHARED_THREAD`, which ended with `end`.
fn attempt_ended(end: AttemptEnd) -> Event {
    Event::from(AttemptEnded {
        at: ended_at(),
        task_id: Uuid::from_u128(7),
        task_name: BoundedText::new(TASK_NAME.to_owned()).unwrap(),
        queue: BoundedText::new(QUEUE_NAME.to_owned()).unwrap(),
        execution: Execution::SharedThread,
        attempt: BoundedU32::try_from(2).unwrap(),
        end,
        run_duration: Duration::from_secs(30),
    })
}

fn slot_recovered() -> Event {
    Event::from(SlotRecovered {
        at: ended_at(),
        child_kind: Execution::Isolated,
        slot: BoundedU32::try_from(0).unwrap(),
    })
}

/// The facts of one span that the client got, owned.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MadeSpan {
    task_id: Uuid,
    task_name: String,
    queue_name: String,
    attempt: u32,
    worker_id: String,
    execution: Execution,
    workflow: EndedWorkflow,
    carrier: BodyCarrier,
    reason: WorkerDecision,
    error_code: BuiltInTaskCode,
    started_at: DateTime<Utc>,
    ended_at: DateTime<Utc>,
}

impl MadeSpan {
    fn of(attempt: &EndedAttempt<'_>) -> Self {
        Self {
            task_id: attempt.task_id,
            task_name: attempt.task_name.to_owned(),
            queue_name: attempt.queue_name.to_owned(),
            attempt: attempt.attempt.get(),
            worker_id: attempt.worker_id.to_owned(),
            execution: attempt.execution,
            workflow: attempt.workflow,
            carrier: attempt.carrier.clone(),
            reason: attempt.reason,
            error_code: attempt.error_code.clone(),
            started_at: attempt.started_at,
            ended_at: attempt.ended_at,
        }
    }
}

/// The span that the client gets for an attempt of
/// `attempt_ended(decided(reason, ..))` with the workflow node `workflow`:
/// the kind of its child, not the class of its task; the carrier read into
/// its map.
fn expected_span(reason: WorkerDecision, workflow: EndedWorkflow) -> MadeSpan {
    MadeSpan {
        task_id: Uuid::from_u128(7),
        task_name: TASK_NAME.to_owned(),
        queue_name: QUEUE_NAME.to_owned(),
        attempt: 2,
        worker_id: WORKER_ID.to_owned(),
        execution: Execution::Isolated,
        workflow,
        carrier: BodyCarrier::Carrier(BTreeMap::from([(
            "traceparent".to_owned(),
            TRACEPARENT.to_owned(),
        )])),
        reason,
        error_code: reason.error_code(),
        started_at: placed_at(),
        ended_at: ended_at(),
    }
}

/// What a test sees, in order: a span that the client made, or an
/// `AttemptEnded` that a handler took, by the kind of its end.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Seen {
    Span(MadeSpan),
    Handled(&'static str),
}

/// A client that sends each span that it gets to a channel.
struct RecordingSpans {
    seen: Mutex<Sender<Seen>>,
}

impl EndedSpanMaker for RecordingSpans {
    fn make(&self, attempt: &EndedAttempt<'_>) {
        let _sent = self
            .seen
            .lock()
            .unwrap()
            .send(Seen::Span(MadeSpan::of(attempt)));
    }
}

/// A client whose every span panics.
struct PanickingSpans {
    calls: AtomicUsize,
}

impl EndedSpanMaker for PanickingSpans {
    fn make(&self, _attempt: &EndedAttempt<'_>) {
        let _counted = self.calls.fetch_add(1, Ordering::Relaxed);
        panic!("the exporter is gone");
    }
}

fn end_kind(end: &AttemptEnd) -> &'static str {
    match end {
        AttemptEnd::DecidedByWorker { .. } => "decided",
        AttemptEnd::ReportedByChild { .. } => "reported",
        AttemptEnd::NotKnown { .. } => "not known",
    }
}

/// The events frame of `events`, in order.
fn events_frame(events: &[Event]) -> EventsFrame {
    let wire: Vec<WireEvent> = events
        .iter()
        .map(|event| WireEvent::of(event).unwrap())
        .collect();
    EventsFrame {
        events: EventBatch::new(&wire).unwrap(),
    }
}

/// The next `count` values of `receiver`, each within [`WAIT`].
fn next_values<T>(receiver: &Receiver<T>, count: usize) -> Vec<T> {
    (0..count)
        .map(|_| {
            receiver
                .recv_timeout(WAIT)
                .unwrap_or_else(|error| panic!("no value within {WAIT:?}: {error}"))
        })
        .collect()
}

/// A handler child that serves `spans` with one handler of `AttemptEnded`,
/// which sends the kind of each end to `seen`.
struct Serving {
    parent: tokio::sync::mpsc::Sender<ParentFrame>,
    service: tokio::task::JoinHandle<ServeEnd>,
    /// The parent's end of the socket. It lives until the service ended:
    /// a write to a closed peer exits the process with the parent-gone
    /// code, which ends the whole test binary.
    _peer: UnixStream,
}

impl Serving {
    fn start(spans: EndedSpans, seen_tx: Sender<Seen>) -> Serving {
        let (ours, peer) = UnixStream::pair().unwrap();
        let writer = FrameWriter::start(ours, 8, ProcessExit::Standard).unwrap();
        let handler_seen = Mutex::new(seen_tx);
        let mut set = EventHandlerSet::new();
        set.add_blocking::<AttemptEnded>("ends".to_owned(), move |event| {
            let _sent = handler_seen
                .lock()
                .unwrap()
                .send(Seen::Handled(end_kind(&event.end)));
            Ok(())
        })
        .unwrap();
        let running = set.start(NonZeroUsize::new(16).unwrap()).unwrap();
        let (parent, mut frames) = tokio::sync::mpsc::channel(8);
        let times = HandlerChildTimes {
            stats_every: Duration::from_secs(60),
            drain: WAIT,
            last_stats: WAIT,
        };
        let service = tokio::spawn(async move {
            serve_handlers(running, &spans, writer, &mut frames, times).await
        });
        Serving {
            parent,
            service,
            _peer: peer,
        }
    }

    async fn send(&self, events: &[Event]) {
        self.parent
            .send(ParentFrame::Events(events_frame(events)))
            .await
            .unwrap();
    }

    async fn stop(self) {
        self.parent.send(ParentFrame::Stop).await.unwrap();
        assert_eq!(self.service.await.unwrap(), ServeEnd::Stopped);
    }
}

/// Serve `spans` with one handler of `AttemptEnded`, which sends the kind
/// of each end to `seen`, and hand it one events frame of `events`; stop
/// after `count` values reached `seen`. Returns the values in order.
async fn serve_and_see(
    spans: EndedSpans,
    seen_tx: Sender<Seen>,
    seen: &Receiver<Seen>,
    events: &[Event],
    count: usize,
) -> Vec<Seen> {
    let serving = Serving::start(spans, seen_tx);
    serving.send(events).await;
    let values = tokio::task::block_in_place(|| next_values(seen, count));
    serving.stop().await;
    values
}

/// The span of an attempt that the worker ended comes once, with the facts
/// of its span, before the handler gets the event; an end that the child
/// reported, an end not known and another event type get no span; a plain
/// task reads no node.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_span_of_an_attempt_that_the_worker_ended_comes_once_before_its_handlers() {
    let (seen_tx, seen) = channel::<Seen>();
    let reads = StubReads::answering(NodeRead::Failed("a plain task reads no node".to_owned()));
    let spans = recording_spans(seen_tx.clone(), &reads);
    let events = [
        attempt_ended(decided(
            WorkerDecision::TimedOut,
            WorkflowPlace::NotInWorkflow,
        )),
        attempt_ended(reported()),
        attempt_ended(not_known()),
        slot_recovered(),
    ];

    let order = serve_and_see(spans, seen_tx, &seen, &events, 4).await;

    assert_eq!(
        order,
        vec![
            Seen::Span(expected_span(
                WorkerDecision::TimedOut,
                EndedWorkflow::Known(RunWorkflow::NotInWorkflow)
            )),
            Seen::Handled("decided"),
            Seen::Handled("reported"),
            Seen::Handled("not known"),
        ]
    );
    assert!(seen.try_recv().is_err(), "no other span");
    assert!(reads.reads().is_empty(), "a plain task reads no node");
}

/// A shared run that its child ended at the deadline is an end that the
/// child reported: the span of the child is the span of the attempt, with
/// the timeout, so the handler child makes none and the handler gets the
/// event. A timeout at a kill gets the worker-made span, from the time the
/// task reached its child. One attempt, one span.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timeout_that_the_child_reported_gets_no_worker_made_span() {
    let (seen_tx, seen) = channel::<Seen>();
    let reads = StubReads::answering(NodeRead::Failed("a plain task reads no node".to_owned()));
    let spans = recording_spans(seen_tx.clone(), &reads);
    let events = [
        attempt_ended(decided(
            WorkerDecision::TimedOut,
            WorkflowPlace::NotInWorkflow,
        )),
        attempt_ended(timed_out_in_child()),
    ];

    let order = serve_and_see(spans, seen_tx, &seen, &events, 3).await;

    let killed = MadeSpan {
        started_at: placed_at(),
        ..expected_span(
            WorkerDecision::TimedOut,
            EndedWorkflow::Known(RunWorkflow::NotInWorkflow),
        )
    };
    assert_eq!(
        order,
        vec![
            Seen::Span(killed),
            Seen::Handled("decided"),
            Seen::Handled("reported"),
        ]
    );
    assert!(seen.try_recv().is_err(), "no other span");
}

/// Each reason of the worker gives one span with the code of its attempt
/// row; a workflow task (a timeout kill among them) waits for its node,
/// and the reader task makes its span with the node that the read gave.
/// `NoSpans` reads no event and no node.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_reason_gives_one_span_with_its_code_and_the_node_that_was_read() {
    let (seen_tx, seen) = channel::<Seen>();
    let reads = StubReads::answering(NodeRead::Linked(linked_node()));
    let spans = recording_spans(seen_tx, &reads);
    let step = started(&spans, 16);
    let cases = [
        (
            WorkerDecision::TimedOut,
            BuiltInTaskCode::from(OutcomeCode::TaskTimeout),
        ),
        (
            WorkerDecision::TaskBlockedLoop,
            OperationalErrorCode::TaskBlockedLoop.into(),
        ),
        (
            WorkerDecision::NeighbourBlockedLoop,
            OperationalErrorCode::NeighbourBlockedLoop.into(),
        ),
        (
            WorkerDecision::ChildLost,
            OperationalErrorCode::WorkerCrashed.into(),
        ),
    ];
    for (reason, code) in &cases {
        let event = attempt_ended(decided(*reason, WorkflowPlace::WorkflowNotKnown));
        assert_eq!(step.step(&event), EndedSpanStep::WaitsForNode, "{reason:?}");
        match next_value(&seen) {
            Seen::Span(span) => {
                assert_eq!(
                    span,
                    expected_span(*reason, EndedWorkflow::Known(linked_node())),
                    "{reason:?}"
                );
                assert_eq!(&span.error_code, code, "{reason:?}");
            }
            other => panic!("{reason:?} gives one span, not {other:?}"),
        }
    }
    for event in [
        attempt_ended(reported()),
        attempt_ended(not_known()),
        slot_recovered(),
    ] {
        assert_eq!(step.step(&event), EndedSpanStep::NotEndedByWorker);
    }
    step.stop(WAIT).await;
    assert!(seen.try_recv().is_err());
    assert_eq!(reads.reads(), vec![Uuid::from_u128(7); cases.len()]);
    assert_eq!(spans.read_failures().counts(), SpanReadCounts::default());
    let off = spans_with(EndedSpanScope::NoSpans, &reads);
    let off_step = started(&off, 16);
    let decided_event = attempt_ended(decided(
        WorkerDecision::TimedOut,
        WorkflowPlace::WorkflowNotKnown,
    ));
    assert_eq!(off_step.step(&decided_event), EndedSpanStep::SpansOff);
    off_step.stop(WAIT).await;
    assert_eq!(reads.reads().len(), cases.len(), "spans off read no node");
}

/// A workflow task killed at its timeout whose node read fails gets its
/// span with `WorkflowNotKnown`, and the failure is counted; a read that
/// finds no node gives `WorkflowNotKnown` and counts nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_read_gives_workflow_not_known_and_counts_the_failure() {
    let cases = [
        (NodeRead::Failed("the pool is closed".to_owned()), 1),
        (NodeRead::NotLinked, 0),
    ];
    for (answer, failures) in cases {
        let (seen_tx, seen) = channel::<Seen>();
        let reads = StubReads::answering(answer.clone());
        let spans = recording_spans(seen_tx, &reads);
        let event = attempt_ended(decided(
            WorkerDecision::TimedOut,
            WorkflowPlace::WorkflowNotKnown,
        ));

        let step = started(&spans, 16);

        assert_eq!(step.step(&event), EndedSpanStep::WaitsForNode, "{answer:?}");
        match next_value(&seen) {
            Seen::Span(span) => assert_eq!(
                span,
                expected_span(WorkerDecision::TimedOut, EndedWorkflow::WorkflowNotKnown),
                "{answer:?}"
            ),
            other => panic!("{answer:?} gives one span, not {other:?}"),
        }
        step.stop(WAIT).await;
        assert_eq!(reads.reads(), vec![Uuid::from_u128(7)], "{answer:?}");
        assert_eq!(
            spans.read_failures().counts(),
            SpanReadCounts {
                read_failed: failures,
                not_read: 0
            },
            "{answer:?}"
        );
    }
}

/// While the read of a node is held, the dispatch goes on: each later span
/// of a workflow task waits in the queue, and a span that does not fit is
/// made at once with `WorkflowNotKnown` and counted `not_read`. The spans
/// that waited get their node when the read ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_queue_makes_the_span_at_once_and_counts_it() {
    let (seen_tx, seen) = channel::<Seen>();
    let (reads, entered) = HeldReads::answering(NodeRead::Linked(linked_node()));
    let spans = recording_spans(seen_tx, &reads);
    let step = started(&spans, 1);
    let event = attempt_ended(decided(
        WorkerDecision::TimedOut,
        WorkflowPlace::WorkflowNotKnown,
    ));

    assert_eq!(step.step(&event), EndedSpanStep::WaitsForNode);
    let _first_read = tokio::task::block_in_place(|| entered.recv_timeout(WAIT).unwrap());
    assert_eq!(step.step(&event), EndedSpanStep::WaitsForNode);
    assert_eq!(step.step(&event), EndedSpanStep::Made);

    assert_eq!(
        next_value(&seen),
        Seen::Span(expected_span(
            WorkerDecision::TimedOut,
            EndedWorkflow::WorkflowNotKnown
        ))
    );
    assert_eq!(
        spans.read_failures().counts(),
        SpanReadCounts {
            read_failed: 0,
            not_read: 1
        }
    );
    reads.release();
    let waited = tokio::task::block_in_place(|| next_values(&seen, 2));
    assert_eq!(
        waited,
        vec![
            Seen::Span(expected_span(
                WorkerDecision::TimedOut,
                EndedWorkflow::Known(linked_node())
            ));
            2
        ]
    );
    step.stop(WAIT).await;
    assert!(seen.try_recv().is_err(), "no other span");
}

/// At the stop, the reader task gets the drain; then the span whose read is
/// held and each span left in the queue are made once with
/// `WorkflowNotKnown`, and counted `not_read`. A read that ends later makes
/// no second span.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stop_makes_each_span_left_once_with_no_node() {
    let (seen_tx, seen) = channel::<Seen>();
    let (reads, entered) = HeldReads::answering(NodeRead::Linked(linked_node()));
    let spans = recording_spans(seen_tx, &reads);
    let step = started(&spans, 16);
    let event = attempt_ended(decided(
        WorkerDecision::ChildLost,
        WorkflowPlace::WorkflowNotKnown,
    ));
    assert_eq!(step.step(&event), EndedSpanStep::WaitsForNode);
    let _first_read = tokio::task::block_in_place(|| entered.recv_timeout(WAIT).unwrap());
    assert_eq!(step.step(&event), EndedSpanStep::WaitsForNode);
    assert_eq!(step.step(&event), EndedSpanStep::WaitsForNode);

    step.stop(Duration::from_millis(50)).await;
    reads.release();

    let made = tokio::task::block_in_place(|| next_values(&seen, 3));
    assert_eq!(
        made,
        vec![
            Seen::Span(expected_span(
                WorkerDecision::ChildLost,
                EndedWorkflow::WorkflowNotKnown
            ));
            3
        ]
    );
    assert!(seen.try_recv().is_err(), "no second span");
    assert!(entered.try_recv().is_err(), "one read only");
    assert_eq!(
        spans.read_failures().counts(),
        SpanReadCounts {
            read_failed: 0,
            not_read: 3
        }
    );
}

/// The handlers get each event while the read of a node is held: the read
/// runs on the reader task, never on the dispatch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_node_read_never_holds_an_event() {
    let (seen_tx, seen) = channel::<Seen>();
    let (reads, entered) = HeldReads::answering(NodeRead::Linked(linked_node()));
    let spans = recording_spans(seen_tx.clone(), &reads);
    let serving = Serving::start(spans, seen_tx);

    serving
        .send(&[
            attempt_ended(decided(
                WorkerDecision::TimedOut,
                WorkflowPlace::WorkflowNotKnown,
            )),
            attempt_ended(reported()),
        ])
        .await;
    let while_held = tokio::task::block_in_place(|| {
        let _read = entered.recv_timeout(WAIT).unwrap();
        next_values(&seen, 2)
    });
    reads.release();
    let after = next_value(&seen);
    serving.stop().await;

    assert_eq!(
        while_held,
        vec![Seen::Handled("decided"), Seen::Handled("reported")]
    );
    assert_eq!(
        after,
        Seen::Span(expected_span(
            WorkerDecision::TimedOut,
            EndedWorkflow::Known(linked_node())
        ))
    );
}

/// A client that panics is caught at each span; the handler still gets the
/// event and the service goes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_that_panics_is_caught_and_the_handlers_still_get_the_event() {
    install_panic_hook();
    let maker = Arc::new(PanickingSpans {
        calls: AtomicUsize::new(0),
    });
    let reads = StubReads::answering(NodeRead::NotLinked);
    let panicking = || {
        spans_with(
            EndedSpanScope::MakeSpans(Arc::clone(&maker) as Arc<dyn EndedSpanMaker>),
            &reads,
        )
    };
    let event = attempt_ended(decided(
        WorkerDecision::ChildLost,
        WorkflowPlace::NotInWorkflow,
    ));
    let step = started(&panicking(), 16);
    assert_eq!(step.step(&event), EndedSpanStep::Panicked);
    step.stop(WAIT).await;

    let (seen_tx, seen) = channel::<Seen>();
    let order = serve_and_see(panicking(), seen_tx, &seen, &[event.clone(), event], 2).await;

    assert_eq!(
        order,
        vec![Seen::Handled("decided"), Seen::Handled("decided")]
    );
    assert_eq!(maker.calls.load(Ordering::Relaxed), 3);
}
