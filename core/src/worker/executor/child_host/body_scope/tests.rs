//! The body scope of a Rust child: the carrier and the end that the client
//! gets, the calls of a client scope, and the span that each body runs
//! inside (an async body, a blocking body on a thread of its own, and an
//! isolated run on the test database).

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use chrono::Utc;
use uuid::Uuid;

use super::super::in_process::run_isolated_in_process_scoped;
use super::super::run::{run_blocking_body, RunFacts};
use super::super::run_work::{CancelCause, RunCancelSignal, RunScope};
use super::super::seats::isolated_body;
use super::super::start_tests::{remove_task, seed_claimed, WORKER_ID};
use super::*;
use crate::broker::claims::WorkflowNodeIndex;
use crate::broker::{OrphanSelfHeal, PostgresBroker};
use crate::core::registry::task::TaskRegistry;
use crate::core::task::error::{OperationalCode, TaskError};
use crate::core::task::fencing::FencingToken;
use crate::core::task::fn_trait::{
    AsyncTaskFn, BlockingTaskFn, RegisteredTask, TaskMetadata, TaskModule,
};
use crate::events::{QueueName, TraceCarrierText};
use crate::worker::executor::protocol::{IsolatedWork, RunEnd, RunFrame, TaskInputText};

/// The task name of the row that `seed_claimed` writes.
const SEEDED_TASK_NAME: &str = "app.tasks.start";

/// A W3C carrier, as a send stores it.
const CARRIER_TEXT: &str =
    r#"{"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01","tracestate":""}"#;

/// The custom code of a body that fails.
const DECLINED: &str = "DECLINED";

thread_local! {
    /// The spans of the probe that this thread is inside, innermost last.
    static ENTERED: RefCell<Vec<u64>> = const { RefCell::new(Vec::new()) };
}

/// The innermost span of the probe that the calling thread is inside.
fn innermost_entered() -> Option<u64> {
    ENTERED.with(|entered| entered.borrow().last().copied())
}

/// A subscriber that numbers each span and keeps, per thread, the spans
/// that the thread entered and did not exit.
#[derive(Default)]
struct EnterProbe {
    last_span: AtomicU64,
}

impl tracing::Subscriber for EnterProbe {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        // A span id is never 0.
        tracing::span::Id::from_u64(self.last_span.fetch_add(1, Ordering::Relaxed) + 1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, _event: &tracing::Event<'_>) {}

    fn enter(&self, span: &tracing::span::Id) {
        ENTERED.with(|entered| entered.borrow_mut().push(span.into_u64()));
    }

    fn exit(&self, span: &tracing::span::Id) {
        ENTERED.with(|entered| {
            let mut entered = entered.borrow_mut();
            if let Some(position) = entered.iter().rposition(|id| *id == span.into_u64()) {
                let _exited = entered.remove(position);
            }
        });
    }
}

/// The facts of a run that a client scope got at its open.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OpenedRun {
    task_id: Uuid,
    task_name: String,
    queue_name: String,
    attempt: AttemptNumber,
    worker_id: String,
    execution: Execution,
    workflow: RunWorkflow,
    carrier: BodyCarrier,
}

/// How a body ended, as a client scope got it at its close.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ClosedEnd {
    Returned,
    Failed(TaskErrorCode),
    TimedOut,
    Cancelled,
}

/// One call of a client scope.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ScopeCall {
    Opened { run: OpenedRun, span: u64 },
    Closed { span: u64, end: ClosedEnd },
}

/// A client scope whose spans the probe numbers. It keeps each call.
struct RecordingSpans {
    probe: tracing::Dispatch,
    calls: Mutex<Vec<ScopeCall>>,
}

impl RecordingSpans {
    fn new() -> Self {
        Self {
            probe: tracing::Dispatch::new(EnterProbe::default()),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<ScopeCall> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn keep(&self, call: ScopeCall) {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(call);
    }
}

/// The probe's number of `span`, which the probe records.
fn span_number(span: &tracing::Span) -> u64 {
    span.id().expect("the probe records every span").into_u64()
}

impl BodySpans for RecordingSpans {
    fn open(&self, run: &BodyRun<'_>) -> tracing::Span {
        let span = tracing::dispatcher::with_default(&self.probe, || tracing::info_span!("run"));
        self.keep(ScopeCall::Opened {
            run: OpenedRun {
                task_id: run.task_id,
                task_name: run.task_name.to_owned(),
                queue_name: run.queue_name.to_owned(),
                attempt: run.attempt,
                worker_id: run.worker_id.to_owned(),
                execution: run.execution,
                workflow: run.workflow,
                carrier: run.carrier.clone(),
            },
            span: span_number(&span),
        });
        span
    }

    fn close(&self, span: &tracing::Span, end: &BodyOutcome<'_>) {
        let end = match end {
            BodyOutcome::Returned => ClosedEnd::Returned,
            BodyOutcome::Failed { error_code } => ClosedEnd::Failed((*error_code).clone()),
            BodyOutcome::TimedOut => ClosedEnd::TimedOut,
            BodyOutcome::Cancelled => ClosedEnd::Cancelled,
        };
        self.keep(ScopeCall::Closed {
            span: span_number(span),
            end,
        });
    }
}

/// The innermost span of the probe that each body saw while it ran.
type SeenInside = Arc<Mutex<Vec<Option<u64>>>>;

fn keep_seen(seen: &SeenInside) {
    seen.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(innermost_entered());
}

fn seen(seen: &SeenInside) -> Vec<Option<u64>> {
    seen.lock().unwrap_or_else(PoisonError::into_inner).clone()
}

/// An async body that records the span it is inside, then returns
/// `result`.
struct SeeingAsync {
    seen: SeenInside,
    result: fn() -> RawTaskResult,
}

impl AsyncTaskFn for SeeingAsync {
    fn execute(&self, _args: &[u8]) -> Pin<Box<dyn Future<Output = RawTaskResult> + Send + '_>> {
        Box::pin(async move {
            keep_seen(&self.seen);
            tokio::task::yield_now().await;
            keep_seen(&self.seen);
            (self.result)()
        })
    }
}

/// A blocking body that records the span it is inside, then returns
/// `result`.
struct SeeingBlocking {
    seen: SeenInside,
    result: fn() -> RawTaskResult,
}

impl BlockingTaskFn for SeeingBlocking {
    fn execute(&self, _args: &[u8]) -> RawTaskResult {
        keep_seen(&self.seen);
        (self.result)()
    }
}

fn returns_one() -> RawTaskResult {
    TaskResult::Ok(b"1".to_vec())
}

fn declines() -> RawTaskResult {
    TaskResult::Err(
        TaskError::custom(DECLINED, "the card was declined").expect("a free custom code"),
    )
}

fn panics() -> RawTaskResult {
    if std::hint::black_box(true) {
        panic!("the body panicked");
    }
    returns_one()
}

fn async_task(seen: &SeenInside, result: fn() -> RawTaskResult) -> RegisteredTask {
    RegisteredTask::Async {
        task: Arc::new(SeeingAsync {
            seen: Arc::clone(seen),
            result,
        }),
        metadata: TaskMetadata::default(),
    }
}

fn blocking_task(seen: &SeenInside, result: fn() -> RawTaskResult) -> RegisteredTask {
    RegisteredTask::Blocking {
        task: Arc::new(SeeingBlocking {
            seen: Arc::clone(seen),
            result,
        }),
        metadata: TaskMetadata::default(),
    }
}

/// The queue of the runs of [`facts`].
const SCOPE_QUEUE: &str = "scope-queue";

/// The workflow node that the start of a run of these tests gave: index 4
/// of workflow `0xf004`, in the tree of `0xf001`.
fn scope_workflow() -> RunWorkflow {
    RunWorkflow::InWorkflow {
        workflow_id: Uuid::from_u128(0xf004),
        node_index: WorkflowNodeIndex::try_from(4).expect("4 is a node index"),
        root_workflow_id: Uuid::from_u128(0xf001),
    }
}

/// The span source of a run with the stored trace `trace` whose start gave
/// [`scope_workflow`].
fn source(trace: &RunTrace) -> SpanSource<'_> {
    SpanSource {
        trace,
        workflow: scope_workflow(),
    }
}

/// The attempt of the runs of [`facts`].
fn third_attempt() -> AttemptNumber {
    AttemptNumber::try_from(3).expect("3 is an attempt")
}

fn facts(task_id: Uuid) -> RunFacts {
    RunFacts {
        task_id,
        task_name: "app.tasks.scoped".to_owned(),
        queue_name: QueueName::cut(SCOPE_QUEUE),
        attempt: third_attempt(),
        worker_id: "scope-worker".to_owned(),
        claimed_at: Utc::now(),
        orphan_self_heal: OrphanSelfHeal::Off,
    }
}

fn carrier_trace() -> RunTrace {
    RunTrace::Carrier {
        text: TraceCarrierText::new(CARRIER_TEXT.to_owned()).expect("a short carrier"),
    }
}

fn carrier_map() -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "traceparent".to_owned(),
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_owned(),
        ),
        ("tracestate".to_owned(), String::new()),
    ])
}

fn declined_code() -> TaskErrorCode {
    match declines() {
        TaskResult::Err(error) => error.error_code,
        TaskResult::Ok(_) => panic!("the body declines"),
    }
}

/// The stored carrier reaches the client as the map that the propagator
/// wrote; a row with no carrier gives none.
#[test]
fn a_stored_carrier_reaches_the_client_as_its_map() {
    let task_id = Uuid::new_v4();
    assert_eq!(
        BodyCarrier::of_trace(task_id, &carrier_trace()),
        BodyCarrier::Carrier(carrier_map())
    );
    assert_eq!(
        BodyCarrier::of_trace(task_id, &RunTrace::NoCarrier),
        BodyCarrier::NoCarrier
    );
}

/// A returned value ends the body `Returned`; an error ends it `Failed`
/// with the error's code.
#[test]
fn the_end_of_a_body_is_what_it_returned() {
    let returned = returns_one();
    assert_eq!(BodyOutcome::of_result(&returned), BodyOutcome::Returned);
    let declined = declines();
    assert_eq!(
        BodyOutcome::of_result(&declined),
        BodyOutcome::Failed {
            error_code: &declined_code()
        }
    );
}

/// With no scope, the body runs inside no span and no client is called.
#[test]
fn no_scope_gives_the_body_no_span() {
    let trace = carrier_trace();
    let open = BodyScope::NoScope.open(&facts(Uuid::new_v4()), Execution::Isolated, source(&trace));
    assert!(open.span().is_none());
    open.close(&BodyOutcome::Returned);
}

/// A client scope is opened once with the facts of the run (its queue and
/// its attempt among them), the workflow node of its start and its carrier,
/// and closed once with the end, on the span that it opened.
#[test]
fn a_client_scope_opens_with_the_run_facts_and_closes_with_the_end() {
    let spans = Arc::new(RecordingSpans::new());
    let scope = BodyScope::Spans(Arc::clone(&spans) as Arc<dyn BodySpans>);
    let task_id = Uuid::new_v4();
    let trace = carrier_trace();
    let open = scope.open(&facts(task_id), Execution::SharedThread, source(&trace));
    let span = span_number(&open.span());
    open.close(&BodyOutcome::TimedOut);
    assert_eq!(
        spans.calls(),
        vec![
            ScopeCall::Opened {
                run: OpenedRun {
                    task_id,
                    task_name: "app.tasks.scoped".to_owned(),
                    queue_name: SCOPE_QUEUE.to_owned(),
                    attempt: third_attempt(),
                    worker_id: "scope-worker".to_owned(),
                    execution: Execution::SharedThread,
                    workflow: scope_workflow(),
                    carrier: BodyCarrier::Carrier(carrier_map()),
                },
                span,
            },
            ScopeCall::Closed {
                span,
                end: ClosedEnd::TimedOut,
            },
        ]
    );
}

/// The run of `task` as an isolated body inside a client scope. Returns the
/// result, the calls of the scope and the span that the body saw in each of
/// its polls.
async fn isolated_run_of(
    make_task: fn(&SeenInside, fn() -> RawTaskResult) -> RegisteredTask,
    result: fn() -> RawTaskResult,
) -> (RawTaskResult, Vec<ScopeCall>, Vec<Option<u64>>) {
    isolated_run_with(CancelBeforeBody::NoCancel, make_task, result).await
}

/// Whether the run of [`isolated_run_with`] has an operator cancel before
/// its body starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelBeforeBody {
    NoCancel,
    OperatorCancel,
}

/// [`isolated_run_of`], with an operator cancel fired on the run before the
/// body starts when `cancel` says so.
async fn isolated_run_with(
    cancel: CancelBeforeBody,
    make_task: fn(&SeenInside, fn() -> RawTaskResult) -> RegisteredTask,
    result: fn() -> RawTaskResult,
) -> (RawTaskResult, Vec<ScopeCall>, Vec<Option<u64>>) {
    let spans = Arc::new(RecordingSpans::new());
    let scope = BodyScope::Spans(Arc::clone(&spans) as Arc<dyn BodySpans>);
    let seen_inside: SeenInside = Arc::new(Mutex::new(Vec::new()));
    let facts = facts(Uuid::new_v4());
    let open = scope.open(&facts, Execution::Isolated, source(&RunTrace::NoCarrier));
    let task = make_task(&seen_inside, result);
    let run_scope = RunScope::new(facts.fencing_token());
    match cancel {
        CancelBeforeBody::NoCancel => {}
        CancelBeforeBody::OperatorCancel => {
            let _fired = run_scope.fire_cancel(CancelCause::Operator);
        }
    }
    let result = isolated_body(&run_scope, &facts.task_name, task, b"{}".to_vec(), open).await;
    (result, spans.calls(), seen(&seen_inside))
}

/// The span number of the first open in `calls`.
fn opened_span(calls: &[ScopeCall]) -> u64 {
    match calls.first() {
        Some(ScopeCall::Opened { span, .. }) => *span,
        other => panic!("the scope is opened first, not {other:?}"),
    }
}

/// An async body is polled inside the span of its run, in every poll, and
/// the scope is closed with what it returned.
#[tokio::test]
async fn an_async_body_runs_inside_the_span_of_its_run() {
    let (result, calls, seen) = isolated_run_of(async_task, returns_one).await;
    assert!(matches!(result, TaskResult::Ok(_)));
    let span = opened_span(&calls);
    assert_eq!(seen, vec![Some(span), Some(span)]);
    assert_eq!(
        calls.last(),
        Some(&ScopeCall::Closed {
            span,
            end: ClosedEnd::Returned
        })
    );
    assert_eq!(innermost_entered(), None);
}

/// A blocking body runs inside the span of its run on its own thread.
#[tokio::test]
async fn a_blocking_body_runs_inside_the_span_of_its_run_on_its_thread() {
    let (result, calls, seen) = isolated_run_of(blocking_task, returns_one).await;
    assert!(matches!(result, TaskResult::Ok(_)));
    let span = opened_span(&calls);
    assert_eq!(seen, vec![Some(span)]);
    assert_eq!(
        calls.last(),
        Some(&ScopeCall::Closed {
            span,
            end: ClosedEnd::Returned
        })
    );
}

/// A body that returns an error closes its scope with the error's code.
#[tokio::test]
async fn a_failed_body_closes_its_scope_with_its_error_code() {
    for make_task in [async_task, blocking_task] {
        let (result, calls, _seen) = isolated_run_of(make_task, declines).await;
        assert!(matches!(result, TaskResult::Err(_)));
        let span = opened_span(&calls);
        assert_eq!(
            calls.last(),
            Some(&ScopeCall::Closed {
                span,
                end: ClosedEnd::Failed(declined_code())
            })
        );
    }
}

/// A body that panics closes its scope with `UNHANDLED_EXCEPTION`.
#[tokio::test]
async fn a_panicking_body_closes_its_scope_with_unhandled_exception() {
    let unhandled = TaskErrorCode::BuiltIn(OperationalCode::UnhandledException.into());
    for make_task in [async_task, blocking_task] {
        let (_result, calls, _seen) = isolated_run_of(make_task, panics).await;
        let span = opened_span(&calls);
        assert_eq!(
            calls.last(),
            Some(&ScopeCall::Closed {
                span,
                end: ClosedEnd::Failed(unhandled.clone())
            })
        );
    }
}

/// A body whose task an operator cancelled closes its scope as cancelled,
/// whatever it returned: the task is `CANCELLED`.
#[tokio::test]
async fn a_cancelled_body_closes_its_scope_as_cancelled() {
    for make_task in [async_task, blocking_task] {
        let (_result, calls, _seen) =
            isolated_run_with(CancelBeforeBody::OperatorCancel, make_task, returns_one).await;
        let span = opened_span(&calls);
        assert_eq!(
            calls.last(),
            Some(&ScopeCall::Closed {
                span,
                end: ClosedEnd::Cancelled
            })
        );
    }
}

/// The body of a thread seat runs inside the span on the calling thread,
/// and leaves it when it returns.
#[test]
fn a_thread_seat_body_runs_inside_the_span_on_the_seat_thread() {
    let spans = RecordingSpans::new();
    let seen_inside: SeenInside = Arc::new(Mutex::new(Vec::new()));
    let task: Arc<dyn BlockingTaskFn> = Arc::new(SeeingBlocking {
        seen: Arc::clone(&seen_inside),
        result: returns_one,
    });
    let carrier = BodyCarrier::NoCarrier;
    let task_id = Uuid::new_v4();
    let span = spans.open(&BodyRun {
        task_id,
        task_name: "app.tasks.scoped",
        queue_name: SCOPE_QUEUE,
        attempt: AttemptNumber::SMALLEST,
        worker_id: "scope-worker",
        execution: Execution::SharedThread,
        workflow: RunWorkflow::NotInWorkflow,
        carrier: &carrier,
    });
    let result = run_blocking_body(
        &task,
        b"{}",
        "app.tasks.scoped",
        TaskModule::default(),
        &span,
        FencingToken::new(task_id, AttemptNumber::SMALLEST),
        RunCancelSignal::new(),
    );
    assert!(matches!(result, TaskResult::Ok(_)));
    assert_eq!(seen(&seen_inside), vec![Some(span_number(&span))]);
    assert_eq!(innermost_entered(), None);
}

/// An isolated run on the database: the host opens the scope after the
/// start, with the facts of the run frame (its queue and attempt as the
/// frame carries them), the workflow node of its start (none for the seeded
/// plain task) and its stored carrier, runs the body inside the span, and
/// closes the scope with the end.
#[tokio::test]
async fn an_isolated_run_opens_its_scope_with_its_facts_and_its_stored_carrier() {
    let pool = crate::test_support::migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let spans = Arc::new(RecordingSpans::new());
    let seen_inside: SeenInside = Arc::new(Mutex::new(Vec::new()));
    let mut registry = TaskRegistry::new();
    registry
        .register(SEEDED_TASK_NAME, async_task(&seen_inside, returns_one))
        .expect("one task registers");
    let run = RunFrame {
        task_id,
        task_name: SEEDED_TASK_NAME.to_owned(),
        queue_name: QueueName::cut(SCOPE_QUEUE),
        attempt: third_attempt(),
        worker_id: WORKER_ID.to_owned(),
        claimed_at: generation,
        orphan_self_heal: OrphanSelfHeal::Off,
        work: IsolatedWork::Run,
        trace: carrier_trace(),
        input: TaskInputText::new("{}".to_owned()),
    };
    let broker = Arc::new(PostgresBroker::from_test_url(
        pool.url(),
        crate::ClientFamily::Rust,
    ));

    let end = run_isolated_in_process_scoped(
        broker,
        Arc::new(registry),
        run,
        BodyScope::Spans(Arc::clone(&spans) as Arc<dyn BodySpans>),
    )
    .await;

    assert!(matches!(end, RunEnd::Result(_)), "{end:?}");
    let calls = spans.calls();
    let span = opened_span(&calls);
    assert_eq!(
        calls,
        vec![
            ScopeCall::Opened {
                run: OpenedRun {
                    task_id,
                    task_name: SEEDED_TASK_NAME.to_owned(),
                    queue_name: SCOPE_QUEUE.to_owned(),
                    attempt: third_attempt(),
                    worker_id: WORKER_ID.to_owned(),
                    execution: Execution::Isolated,
                    workflow: RunWorkflow::NotInWorkflow,
                    carrier: BodyCarrier::Carrier(carrier_map()),
                },
                span,
            },
            ScopeCall::Closed {
                span,
                end: ClosedEnd::Returned,
            },
        ]
    );
    assert_eq!(seen(&seen_inside), vec![Some(span), Some(span)]);
    remove_task(&pool, task_id).await;
}
