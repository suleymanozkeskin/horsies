//! The span of an attempt that the worker ended, in the handler child: the
//! client gets the facts of the span once per such attempt, before the
//! handlers get the event; no other event and no scope with no spans calls
//! the client; a client that panics is caught and the handlers still get
//! the event. No database and no process: the service writes to a socket
//! pair.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::body_scope::BodyCarrier;
use super::ended_spans::{EndedAttempt, EndedSpanMaker, EndedSpanScope, EndedSpanStep};
use super::handler_host::{serve_handlers, HandlerChildTimes};
use super::handlers::EventHandlerSet;
use super::link::FrameWriter;
use super::panic::install_panic_hook;
use super::seats::ServeEnd;
use crate::core::config::bounded::BoundedU32;
use crate::core::task::Execution;
use crate::core::{BuiltInTaskCode, OperationalErrorCode, OutcomeCode};
use crate::events::{
    AttemptEnd, AttemptEnded, AttemptStart, ChildResult, Event, RunTrace, SlotRecovered,
    WorkerDecision, WorkflowPlace,
};
use crate::worker::executor::protocol::{
    BoundedText, EventBatch, EventsFrame, ParentFrame, ProcessId, WireEvent,
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

fn ended_at() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_nanos(1_700_000_030_500_654_321)
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
    workflow: WorkflowPlace,
    carrier: BodyCarrier,
    reason: WorkerDecision,
    error_code: BuiltInTaskCode,
    placed_at: DateTime<Utc>,
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
            placed_at: attempt.placed_at,
            ended_at: attempt.ended_at,
        }
    }
}

/// The span that the client gets for the attempt of
/// `attempt_ended(decided(reason, workflow))`: the kind of its child, not
/// the class of its task; the carrier read into its map.
fn expected_span(reason: WorkerDecision, workflow: WorkflowPlace) -> MadeSpan {
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
        placed_at: placed_at(),
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

/// Serve `scope` with one handler of `AttemptEnded`, which sends the kind
/// of each end to `seen`, and hand it one events frame of `events`; stop
/// after `count` values reached `seen`. Returns the values in order.
async fn serve_and_see(
    scope: EndedSpanScope,
    seen_tx: Sender<Seen>,
    seen: &Receiver<Seen>,
    events: &[Event],
    count: usize,
) -> Vec<Seen> {
    let (ours, _theirs) = UnixStream::pair().unwrap();
    let writer = FrameWriter::start(ours, 8).unwrap();
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
    let service =
        tokio::spawn(
            async move { serve_handlers(running, &scope, writer, &mut frames, times).await },
        );
    parent
        .send(ParentFrame::Events(events_frame(events)))
        .await
        .unwrap();
    let values = tokio::task::block_in_place(|| next_values(seen, count));
    parent.send(ParentFrame::Stop).await.unwrap();
    assert_eq!(service.await.unwrap(), ServeEnd::Stopped);
    values
}

/// The span of an attempt that the worker ended comes once, with the facts
/// of its span, before the handler gets the event; an end that the child
/// reported, an end not known and another event type get no span.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_span_of_an_attempt_that_the_worker_ended_comes_once_before_its_handlers() {
    let (seen_tx, seen) = channel::<Seen>();
    let scope = EndedSpanScope::MakeSpans(Arc::new(RecordingSpans {
        seen: Mutex::new(seen_tx.clone()),
    }));
    let events = [
        attempt_ended(decided(
            WorkerDecision::TimedOut,
            WorkflowPlace::NotInWorkflow,
        )),
        attempt_ended(reported()),
        attempt_ended(not_known()),
        slot_recovered(),
    ];

    let order = serve_and_see(scope, seen_tx, &seen, &events, 4).await;

    assert_eq!(
        order,
        vec![
            Seen::Span(expected_span(
                WorkerDecision::TimedOut,
                WorkflowPlace::NotInWorkflow
            )),
            Seen::Handled("decided"),
            Seen::Handled("reported"),
            Seen::Handled("not known"),
        ]
    );
    assert!(seen.try_recv().is_err(), "no other span");
}

/// Each reason of the worker gives one span with the code of its attempt
/// row and the workflow place of the event; `NoSpans` reads no event.
#[test]
fn each_reason_gives_one_span_with_its_code_and_no_spans_reads_nothing() {
    let (seen_tx, seen) = channel::<Seen>();
    let scope = EndedSpanScope::MakeSpans(Arc::new(RecordingSpans {
        seen: Mutex::new(seen_tx),
    }));
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
        assert_eq!(scope.make_for(&event), EndedSpanStep::Made, "{reason:?}");
        match seen.try_recv() {
            Ok(Seen::Span(span)) => {
                assert_eq!(
                    span,
                    expected_span(*reason, WorkflowPlace::WorkflowNotKnown),
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
        assert_eq!(scope.make_for(&event), EndedSpanStep::NotEndedByWorker);
    }
    assert!(seen.try_recv().is_err());
    let decided_event = attempt_ended(decided(
        WorkerDecision::TimedOut,
        WorkflowPlace::NotInWorkflow,
    ));
    assert_eq!(
        EndedSpanScope::NoSpans.make_for(&decided_event),
        EndedSpanStep::SpansOff
    );
}

/// A client that panics is caught at each span; the handler still gets the
/// event and the service goes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_that_panics_is_caught_and_the_handlers_still_get_the_event() {
    install_panic_hook();
    let spans = Arc::new(PanickingSpans {
        calls: AtomicUsize::new(0),
    });
    let scope = EndedSpanScope::MakeSpans(Arc::clone(&spans) as Arc<dyn EndedSpanMaker>);
    let event = attempt_ended(decided(
        WorkerDecision::ChildLost,
        WorkflowPlace::NotInWorkflow,
    ));
    assert_eq!(scope.make_for(&event), EndedSpanStep::Panicked);

    let (seen_tx, seen) = channel::<Seen>();
    let order = serve_and_see(scope, seen_tx, &seen, &[event.clone(), event], 2).await;

    assert_eq!(
        order,
        vec![Seen::Handled("decided"), Seen::Handled("decided")]
    );
    assert_eq!(spans.calls.load(Ordering::Relaxed), 3);
}
