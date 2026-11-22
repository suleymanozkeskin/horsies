//! The finalize batcher on a scripted batch write.
//!
//! Each test drives [`FinalizeBatcher`] with a write that records each
//! batch, counts the connections its writes hold (each write checks out one
//! for its statement), and answers from a script. Each test keeps the name
//! of its Python test; the tests of Python's held flusher connections are
//! tests of the per-batch checkout here.
//!
//! The runtime of `#[tokio::test]` has one thread: a flusher runs only when
//! the test yields, so rows submitted with no yield between them are queued
//! before any flusher takes one.

use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::TimeZone;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::*;
use crate::core::lifecycle::{ObservedTaskState, TerminalizationKind};
use crate::core::types::TaskStatus;
use crate::db::{
    CancelRequestOutcome, CheckoutFailure, NotSentCause, PoolRole, Reserve, UnknownOutcomeCause,
    WORKER_RESERVED_CONNECTIONS,
};

const WORKER: &str = "worker-1";

/// A wait in a test fails, not hangs, when it never ends.
const OUTER_BOUND: Duration = Duration::from_secs(5);

/// A queue bound that no test reaches.
const QUEUE_BOUND: NonZeroUsize = match NonZeroUsize::new(1000) {
    Some(bound) => bound,
    None => panic!("the test queue bound is not zero"),
};

fn claimed_at() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 11, 9, 0, 0)
        .single()
        .expect("the test claim generation is a valid instant")
}

/// The task id of a test row named `name`.
fn task(name: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes())
}

fn tasks(names: &[&str]) -> Vec<Uuid> {
    names.iter().map(|name| task(name)).collect()
}

fn numbered(prefix: &str, count: usize) -> Vec<String> {
    (0..count).map(|index| format!("{prefix}{index}")).collect()
}

fn row(name: &str) -> PlainSuccess {
    PlainSuccess {
        task_id: task(name),
        claimed_at: claimed_at(),
        result_json: r#"{"__type":"ok","value":null}"#.to_owned(),
        placement: crate::core::lifecycle::AttemptPlacement::NotObserved,
    }
}

fn observed(worker_id: &str) -> ObservedTaskState {
    ObservedTaskState {
        status: Some(TaskStatus::Running),
        worker_id: Some(worker_id.to_owned()),
        claimed_at: Some(claimed_at()),
    }
}

fn applied(task_id: Uuid, position: usize) -> TerminalizationOutcome {
    TerminalizationOutcome::Applied {
        task_id,
        ordinality: Some(i64::try_from(position).expect("a test position fits i64")),
        terminal_at: claimed_at(),
        kind: TerminalizationKind::CompleteFused,
        observed: observed(WORKER),
    }
}

fn all_applied(batch: &SuccessBatch) -> Result<BatchOutcomes, BatchWriteError> {
    let outcomes = batch
        .rows()
        .iter()
        .enumerate()
        .map(|(index, row)| applied(row.task_id, index + 1))
        .collect();
    BatchOutcomes::for_batch(batch, outcomes)
}

fn refused() -> BatchWriteError {
    BatchWriteError::Refused {
        transience: Transience::Permanent,
        detail: "statement `complete_plain_successes` was refused: no partition for the row"
            .to_owned(),
        server_error: crate::failure_cause::ServerErrorFacts::new(
            tokio_postgres::error::SqlState::CHECK_VIOLATION,
            "no partition of relation found for row".to_owned(),
        ),
    }
}

fn not_sent() -> BatchWriteError {
    BatchWriteError::NotSent {
        transience: Transience::Transient,
        detail: "the worker profile pool had no free connection within 30s".to_owned(),
        cause: crate::failure_cause::FailureCause::Connection,
    }
}

fn commit_unknown() -> BatchWriteError {
    BatchWriteError::CommitUnknown {
        detail: "the connection was lost after the statement was sent".to_owned(),
        cause: crate::failure_cause::FailureCause::Connection,
    }
}

/// A flusher bound of `flushers`: the bound of a worker pool of
/// `flushers + 3` connections (the claim pass and the reserve of 2).
fn bound(flushers: u32) -> FlusherBound {
    let sizing = PoolSizing::new(flushers + 3, Reserve::KeepFree(WORKER_RESERVED_CONNECTIONS))
        .expect("a test pool keeps a batch connection");
    FlusherBound::of_worker_pool(&sizing).expect("a test pool leaves a flusher")
}

/// A flusher bound with no room for a surge flusher.
fn steady_only() -> FlusherBound {
    bound(FINALIZE_BATCH_FLUSHERS)
}

/// Opens once; every write waits at it until then.
struct Gate {
    open: watch::Sender<bool>,
}

impl Gate {
    fn shut() -> Arc<Gate> {
        Arc::new(Gate {
            open: watch::Sender::new(false),
        })
    }

    fn opened() -> Arc<Gate> {
        Arc::new(Gate {
            open: watch::Sender::new(true),
        })
    }

    fn open(&self) {
        let _was_open = self.open.send_replace(true);
    }

    async fn pass(&self) {
        let mut receiver = self.open.subscribe();
        // The `;` drops the borrow that `wait_for` returns before `receiver`.
        match receiver.wait_for(|open| *open).await {
            Ok(_open) => {}
            Err(_closed) => panic!("the gate lives as long as the write"),
        };
    }
}

/// What the scripted write saw.
#[derive(Default)]
struct Record {
    batches: Mutex<Vec<Vec<Uuid>>>,
    /// Connections the writes hold now: one per write in progress.
    connections_out: AtomicUsize,
    most_connections_out: AtomicUsize,
}

impl Record {
    fn begin(&self, batch: &SuccessBatch) {
        self.batches
            .lock()
            .expect("record lock")
            .push(batch.rows().iter().map(|row| row.task_id).collect());
        let out = self.connections_out.fetch_add(1, Ordering::SeqCst) + 1;
        let _previous = self.most_connections_out.fetch_max(out, Ordering::SeqCst);
    }

    fn end(&self) {
        let _previous = self.connections_out.fetch_sub(1, Ordering::SeqCst);
    }

    fn batches(&self) -> Vec<Vec<Uuid>> {
        self.batches.lock().expect("record lock").clone()
    }

    fn sizes(&self) -> Vec<usize> {
        self.batches().iter().map(Vec::len).collect()
    }

    fn out(&self) -> usize {
        self.connections_out.load(Ordering::SeqCst)
    }

    fn most_out(&self) -> usize {
        self.most_connections_out.load(Ordering::SeqCst)
    }
}

type Script = Box<dyn Fn(&SuccessBatch) -> Result<BatchOutcomes, BatchWriteError> + Send + Sync>;

/// A batch write that records each batch, waits at its gate, and answers
/// with its script. It holds one connection from its start to its answer.
struct ScriptedWrite {
    record: Arc<Record>,
    gate: Arc<Gate>,
    script: Script,
}

impl SuccessBatchWrite for ScriptedWrite {
    async fn write(&self, batch: &SuccessBatch) -> Result<BatchOutcomes, BatchWriteError> {
        self.record.begin(batch);
        self.gate.pass().await;
        self.record.end();
        (self.script)(batch)
    }
}

/// A batcher on a scripted write, with the record and the gate of the write.
struct Harness {
    batcher: FinalizeBatcher<ScriptedWrite>,
    record: Arc<Record>,
    gate: Arc<Gate>,
}

fn harness_with(
    gate: Arc<Gate>,
    script: Script,
    max_queued_rows: NonZeroUsize,
    flushers: FlusherBound,
) -> Harness {
    let record = Arc::new(Record::default());
    let write = ScriptedWrite {
        record: Arc::clone(&record),
        gate: Arc::clone(&gate),
        script,
    };
    Harness {
        batcher: FinalizeBatcher::new(write, max_queued_rows, flushers),
        record,
        gate,
    }
}

/// Every write answers at once with every row applied.
fn open_harness(flushers: FlusherBound) -> Harness {
    harness_with(Gate::opened(), Box::new(all_applied), QUEUE_BOUND, flushers)
}

/// Every write waits for the gate, then applies every row.
fn gated_harness(flushers: FlusherBound) -> Harness {
    harness_with(Gate::shut(), Box::new(all_applied), QUEUE_BOUND, flushers)
}

/// Run every task that is ready until none is: a few turns of the runtime.
async fn settle() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
}

/// Submit `name` and wait for its answer in a task of its own.
fn call(batcher: &FinalizeBatcher<ScriptedWrite>, name: &str) -> JoinHandle<RowAnswer> {
    tokio::spawn(batcher.submit(row(name)).answer())
}

fn calls(batcher: &FinalizeBatcher<ScriptedWrite>, names: &[String]) -> Vec<JoinHandle<RowAnswer>> {
    names.iter().map(|name| call(batcher, name)).collect()
}

/// Submit one row and let the flushers take it before the next submit.
async fn submit_alone(
    batcher: &FinalizeBatcher<ScriptedWrite>,
    name: &str,
) -> JoinHandle<RowAnswer> {
    let handle = call(batcher, name);
    settle().await;
    handle
}

/// Submit rows with no yield between them, then let the flushers take them.
async fn submit_together(
    batcher: &FinalizeBatcher<ScriptedWrite>,
    names: &[String],
) -> Vec<JoinHandle<RowAnswer>> {
    let handles = calls(batcher, names);
    settle().await;
    handles
}

async fn answer_of(handle: JoinHandle<RowAnswer>) -> RowAnswer {
    tokio::time::timeout(OUTER_BOUND, handle)
        .await
        .expect("the answer came within the outer bound")
        .expect("the answer task ended without a panic")
}

async fn answers_of(handles: Vec<JoinHandle<RowAnswer>>) -> Vec<RowAnswer> {
    let mut answers = Vec::with_capacity(handles.len());
    for handle in handles {
        answers.push(answer_of(handle).await);
    }
    answers
}

fn ok_task_id(answer: &RowAnswer) -> Uuid {
    match answer {
        Ok(outcome) => outcome.task_id(),
        Err(not_written) => panic!("expected an outcome, got {not_written:?}"),
    }
}

fn panic_text(payload: &(dyn Any + Send)) -> String {
    match (
        payload.downcast_ref::<String>(),
        payload.downcast_ref::<&str>(),
    ) {
        (Some(text), _) => text.clone(),
        (None, Some(text)) => (*text).to_owned(),
        (None, None) => "a panic with no text".to_owned(),
    }
}

// ---------------------------------------------------------------------------
// Flush rule
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_lone_row_is_written_at_once() {
    let harness = open_harness(steady_only());

    let answer = tokio::time::timeout(OUTER_BOUND, harness.batcher.complete(row("a")))
        .await
        .expect("the answer came within the outer bound");

    assert_eq!(ok_task_id(&answer), task("a"));
    assert_eq!(harness.record.batches(), vec![tasks(&["a"])]);
}

#[tokio::test]
async fn rows_that_arrive_while_the_flushers_write_form_the_next_batch() {
    let harness = gated_harness(steady_only());
    let first = vec![
        submit_alone(&harness.batcher, "a").await,
        submit_alone(&harness.batcher, "b").await,
    ];
    let later = calls(
        &harness.batcher,
        &["c".to_owned(), "d".to_owned(), "e".to_owned()],
    );
    settle().await;

    assert_eq!(harness.record.batches(), vec![tasks(&["a"]), tasks(&["b"])]);
    harness.gate.open();
    let mut answers = answers_of(first).await;
    answers.extend(answers_of(later).await);

    assert_eq!(
        harness.record.batches(),
        vec![tasks(&["a"]), tasks(&["b"]), tasks(&["c", "d", "e"])]
    );
    assert_eq!(
        answers.iter().map(ok_task_id).collect::<Vec<_>>(),
        tasks(&["a", "b", "c", "d", "e"])
    );
}

#[tokio::test]
async fn a_batch_holds_at_most_the_max_rows() {
    let harness = open_harness(steady_only());
    let names = numbered("t", FUSED_BATCH_MAX_ROWS + 10);

    let _answers = answers_of(calls(&harness.batcher, &names)).await;

    assert_eq!(harness.record.sizes(), vec![FUSED_BATCH_MAX_ROWS, 10]);
    let written: Vec<Uuid> = harness.record.batches().into_iter().flatten().collect();
    let expected: Vec<Uuid> = names.iter().map(|name| task(name)).collect();
    assert_eq!(written, expected);
}

#[tokio::test]
async fn no_more_than_the_flusher_bound_write_at_once() {
    let harness = gated_harness(steady_only());
    let mut handles = calls(&harness.batcher, &numbered("t", 5));
    for index in 5..40 {
        settle().await;
        handles.push(call(&harness.batcher, &format!("t{index}")));
    }
    settle().await;

    assert_eq!(harness.record.out(), STEADY_FLUSHERS);
    harness.gate.open();
    let _answers = answers_of(handles).await;
    assert_eq!(harness.record.most_out(), STEADY_FLUSHERS);
}

#[tokio::test]
async fn a_repeated_task_id_waits_for_the_next_batch() {
    let harness = open_harness(steady_only());
    let names = ["a", "b", "a", "c"].map(str::to_owned);

    let answers = answers_of(calls(&harness.batcher, &names)).await;

    assert_eq!(
        harness.record.batches(),
        vec![tasks(&["a", "b", "c"]), tasks(&["a"])]
    );
    assert_eq!(
        answers.iter().map(ok_task_id).collect::<Vec<_>>(),
        tasks(&["a", "b", "a", "c"])
    );
}

// ---------------------------------------------------------------------------
// Answers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn each_row_gets_the_outcome_of_its_own_task() {
    let lost = task("lost");
    let script: Script = Box::new(move |batch| {
        let outcomes = batch
            .rows()
            .iter()
            .enumerate()
            .map(|(index, row)| match row.task_id == lost {
                true => TerminalizationOutcome::LostClaim {
                    task_id: lost,
                    ordinality: Some(i64::try_from(index + 1).expect("a test position fits i64")),
                    observed: observed("other"),
                },
                false => applied(row.task_id, index + 1),
            })
            .collect();
        BatchOutcomes::for_batch(batch, outcomes)
    });
    let harness = harness_with(Gate::opened(), script, QUEUE_BOUND, steady_only());

    let answers = answers_of(calls(
        &harness.batcher,
        &["mine".to_owned(), "lost".to_owned()],
    ))
    .await;

    match &answers[..] {
        [Ok(TerminalizationOutcome::Applied { task_id: mine, .. }), Ok(TerminalizationOutcome::LostClaim {
            task_id: lost_task, ..
        })] => {
            assert_eq!(*mine, task("mine"));
            assert_eq!(*lost_task, task("lost"));
        }
        other => panic!("expected Applied for mine and LostClaim for lost, got {other:?}"),
    }
}

#[tokio::test]
async fn the_answer_resolves_only_after_the_write_returned() {
    let harness = gated_harness(steady_only());

    let handle = call(&harness.batcher, "a");
    settle().await;

    assert_eq!(harness.record.batches(), vec![tasks(&["a"])]);
    assert!(!handle.is_finished());
    harness.gate.open();
    assert_eq!(ok_task_id(&answer_of(handle).await), task("a"));
}

#[tokio::test]
async fn a_refused_batch_writes_each_row_alone_once() {
    let single_good = [tasks(&["a"]), tasks(&["b"])];
    let script: Script = Box::new(move |batch| {
        let ids: Vec<Uuid> = batch.rows().iter().map(|row| row.task_id).collect();
        match single_good.contains(&ids) {
            true => all_applied(batch),
            false => Err(refused()),
        }
    });
    let harness = harness_with(Gate::opened(), script, QUEUE_BOUND, steady_only());

    let answers = answers_of(calls(
        &harness.batcher,
        &["a".to_owned(), "bad".to_owned(), "b".to_owned()],
    ))
    .await;

    // The first flusher takes the three rows; the second finds none.
    assert_eq!(
        harness.record.batches(),
        vec![
            tasks(&["a", "bad", "b"]),
            tasks(&["a"]),
            tasks(&["bad"]),
            tasks(&["b"]),
        ]
    );
    assert_eq!(ok_task_id(&answers[0]), task("a"));
    assert_eq!(ok_task_id(&answers[2]), task("b"));
    match &answers[1] {
        Err(RowNotWritten::WriteFailed(error @ BatchWriteError::Refused { .. })) => {
            assert_eq!(error.transience(), Transience::Permanent);
        }
        other => panic!("expected the refusal of the row alone, got {other:?}"),
    }
}

#[tokio::test]
async fn a_refused_row_of_one_is_not_written_again() {
    let harness = harness_with(
        Gate::opened(),
        Box::new(|_batch| Err(refused())),
        QUEUE_BOUND,
        steady_only(),
    );

    let answer = harness.batcher.complete(row("a")).await;

    assert_eq!(harness.record.batches(), vec![tasks(&["a"])]);
    assert_eq!(answer, Err(RowNotWritten::WriteFailed(refused())));
}

#[tokio::test]
async fn a_failure_of_the_connection_answers_every_row_without_a_split() {
    for error in [not_sent(), commit_unknown()] {
        let answered = error.clone();
        let harness = harness_with(
            Gate::opened(),
            Box::new(move |_batch| Err(answered.clone())),
            QUEUE_BOUND,
            steady_only(),
        );

        let answers = answers_of(calls(
            &harness.batcher,
            &["a".to_owned(), "b".to_owned(), "c".to_owned()],
        ))
        .await;

        assert_eq!(
            harness.record.batches(),
            vec![tasks(&["a", "b", "c"])],
            "{error}"
        );
        let expected = Err(RowNotWritten::WriteFailed(error.clone()));
        assert_eq!(
            answers,
            vec![expected.clone(), expected.clone(), expected],
            "{error}"
        );
    }
}

/// The server refusals (a deadlock is transient, an argument refusal is
/// not) need a real server error: the test database tests of the batch
/// statement read them.
#[test]
fn the_retryable_flag_follows_the_cause() {
    let checkout_timed_out = StatementFailure::NotSent {
        statement: "complete_plain_successes",
        cause: NotSentCause::Checkout(CheckoutFailure::TimedOut {
            role: PoolRole::WorkerProfile,
            waited: Duration::from_secs(30),
        }),
    };
    let pool_closed = StatementFailure::NotSent {
        statement: "complete_plain_successes",
        cause: NotSentCause::Checkout(CheckoutFailure::PoolClosed {
            role: PoolRole::WorkerProfile,
        }),
    };
    let no_reply = StatementFailure::OutcomeUnknown {
        statement: "complete_plain_successes",
        cause: UnknownOutcomeCause::NoReplyWithinBound {
            bound: Duration::from_secs(30),
            grace: Duration::from_secs(5),
            cancel: CancelRequestOutcome::Sent,
        },
    };

    let checkout = BatchWriteError::from(&checkout_timed_out);
    assert!(matches!(checkout, BatchWriteError::NotSent { .. }));
    assert_eq!(checkout.transience(), Transience::Transient);
    let closed = BatchWriteError::from(&pool_closed);
    assert!(matches!(closed, BatchWriteError::NotSent { .. }));
    assert_eq!(closed.transience(), Transience::Permanent);
    let unknown = BatchWriteError::from(&no_reply);
    assert!(matches!(unknown, BatchWriteError::CommitUnknown { .. }));
    assert_eq!(unknown.transience(), Transience::Transient);
    let broken = BatchWriteError::ContractBroken {
        detail: "a row without ordinality".to_owned(),
    };
    assert_eq!(broken.transience(), Transience::Permanent);
}

/// Python raises a defect of the write to every row of the batch. In the
/// core a broken row contract is the error of the write: every row of the
/// batch gets it, and the flusher goes on with the next batch.
#[tokio::test]
async fn a_defect_of_the_write_is_raised_to_every_row() {
    let writes = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&writes);
    let script: Script = Box::new(move |batch| match counted.fetch_add(1, Ordering::SeqCst) {
        0 => Err(BatchWriteError::ContractBroken {
            detail: "a row without ordinality".to_owned(),
        }),
        _ => all_applied(batch),
    });
    let harness = harness_with(Gate::opened(), script, QUEUE_BOUND, steady_only());

    let answers = answers_of(calls(&harness.batcher, &["a".to_owned(), "b".to_owned()])).await;
    let later = harness.batcher.complete(row("c")).await;

    for answer in &answers {
        assert!(
            matches!(
                answer,
                Err(RowNotWritten::WriteFailed(
                    BatchWriteError::ContractBroken { .. }
                ))
            ),
            "{answer:?}"
        );
    }
    assert_eq!(ok_task_id(&later), task("c"));
    assert_eq!(harness.record.sizes(), vec![2, 1]);
}

#[tokio::test]
async fn a_cancelled_caller_leaves_its_row_written() {
    let harness = gated_harness(steady_only());
    let held = submit_alone(&harness.batcher, "a").await;
    let queued = submit_alone(&harness.batcher, "b").await;
    let queued_later = submit_alone(&harness.batcher, "c").await;

    held.abort();
    queued_later.abort();
    harness.gate.open();
    assert_eq!(ok_task_id(&answer_of(queued).await), task("b"));
    settle().await;

    assert_eq!(
        harness.record.batches(),
        vec![tasks(&["a"]), tasks(&["b"]), tasks(&["c"])]
    );
    assert!(held
        .await
        .expect_err("the caller was cancelled")
        .is_cancelled());
    assert!(queued_later
        .await
        .expect_err("the caller was cancelled")
        .is_cancelled());
}

#[tokio::test]
async fn a_full_queue_is_a_defect() {
    let harness = harness_with(
        Gate::opened(),
        Box::new(all_applied),
        NonZeroUsize::new(2).expect("2 is not zero"),
        steady_only(),
    );
    let first = call(&harness.batcher, "a");
    let second = call(&harness.batcher, "b");

    // The three submit with no yield, before a flusher takes a row.
    let third = std::panic::catch_unwind(AssertUnwindSafe(|| harness.batcher.submit(row("c"))));

    let payload = match third {
        Ok(_pending) => panic!("a third row passed a queue bound of two"),
        Err(payload) => payload,
    };
    assert!(panic_text(payload.as_ref()).contains("its bound"));
    let answers = answers_of(vec![first, second]).await;
    assert_eq!(
        answers.iter().map(ok_task_id).collect::<Vec<_>>(),
        tasks(&["a", "b"])
    );
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

#[tokio::test]
async fn close_writes_the_queued_rows_first() {
    let harness = gated_harness(steady_only());
    let handles = calls(&harness.batcher, &["a", "b", "c"].map(str::to_owned));
    settle().await;

    let closer = harness.batcher.clone();
    let closing = tokio::spawn(async move { closer.close(OUTER_BOUND).await });
    settle().await;
    assert!(!closing.is_finished());
    harness.gate.open();

    let report = tokio::time::timeout(OUTER_BOUND, closing)
        .await
        .expect("the close ended within the outer bound")
        .expect("the close task ended without a panic");
    assert_eq!(report, CloseReport::FlushersDrained);
    let answers = answers_of(handles).await;
    assert_eq!(
        answers.iter().map(ok_task_id).collect::<Vec<_>>(),
        tasks(&["a", "b", "c"])
    );
    assert_eq!(harness.record.batches(), vec![tasks(&["a", "b", "c"])]);
}

#[tokio::test]
async fn close_cancels_the_flushers_at_its_timeout() {
    let harness = gated_harness(steady_only());
    let mut handles = Vec::new();
    for name in ["a", "b", "c", "d"] {
        handles.push(submit_alone(&harness.batcher, name).await);
    }

    let report = tokio::time::timeout(
        OUTER_BOUND,
        harness.batcher.close(Duration::from_millis(50)),
    )
    .await
    .expect("the close ended within the outer bound");

    assert_eq!(
        report,
        CloseReport::FlushersCancelled {
            rows_in_flight: 2,
            rows_unsent: 2,
        }
    );
    assert_eq!(
        answers_of(handles).await,
        vec![
            Err(RowNotWritten::StatementCancelledAtStop),
            Err(RowNotWritten::StatementCancelledAtStop),
            Err(RowNotWritten::NotSentAtStop),
            Err(RowNotWritten::NotSentAtStop),
        ]
    );
}

#[tokio::test]
async fn a_closed_batcher_refuses_rows() {
    let harness = open_harness(steady_only());

    assert_eq!(
        harness.batcher.close(OUTER_BOUND).await,
        CloseReport::FlushersDrained
    );
    assert_eq!(harness.batcher.state(), BatcherState::Closed);
    let refused = std::panic::catch_unwind(AssertUnwindSafe(|| harness.batcher.submit(row("a"))));
    let payload = match refused {
        Ok(_pending) => panic!("a closed batcher took a row"),
        Err(payload) => payload,
    };
    assert!(panic_text(payload.as_ref()).contains("after the finalize batcher closed"));
    assert_eq!(
        harness.batcher.close(OUTER_BOUND).await,
        CloseReport::FlushersDrained
    );
}

// ---------------------------------------------------------------------------
// The batch type
// ---------------------------------------------------------------------------

#[test]
fn a_batch_refuses_a_size_out_of_its_bound() {
    for size in [0, FUSED_BATCH_MAX_ROWS + 1] {
        let rows = numbered("t", size).iter().map(|name| row(name)).collect();
        assert_eq!(
            SuccessBatch::new(rows),
            Err(SuccessBatchError::SizeOutOfBound { size })
        );
    }
}

#[test]
fn a_batch_refuses_a_task_id_twice() {
    assert_eq!(
        SuccessBatch::new(vec![row("a"), row("a")]),
        Err(SuccessBatchError::TaskIdTwice { task_id: task("a") })
    );
}

#[test]
fn a_batch_takes_one_to_the_max_rows() {
    for size in [1, FUSED_BATCH_MAX_ROWS] {
        let rows = numbered("t", size).iter().map(|name| row(name)).collect();
        let batch = SuccessBatch::new(rows).expect("a batch inside its bound");
        assert_eq!(batch.rows().len(), size);
    }
}

#[test]
fn outcomes_of_another_count_or_another_task_break_the_contract() {
    let batch = SuccessBatch::new(vec![row("a"), row("b")]).expect("a batch of two");

    let short = BatchOutcomes::for_batch(&batch, vec![applied(task("a"), 1)]);
    let swapped =
        BatchOutcomes::for_batch(&batch, vec![applied(task("b"), 1), applied(task("a"), 2)]);

    assert!(matches!(short, Err(BatchWriteError::ContractBroken { .. })));
    assert!(matches!(
        swapped,
        Err(BatchWriteError::ContractBroken { .. })
    ));
    assert!(all_applied(&batch).is_ok());
}

// ---------------------------------------------------------------------------
// Surge flushers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_surge_flusher_starts_only_when_a_full_batch_waits() {
    let harness = gated_harness(bound(4));
    let mut handles = vec![
        submit_alone(&harness.batcher, "a").await,
        submit_alone(&harness.batcher, "b").await,
    ];
    handles
        .extend(submit_together(&harness.batcher, &numbered("w", FUSED_BATCH_MAX_ROWS - 1)).await);

    assert_eq!(harness.record.out(), STEADY_FLUSHERS);

    handles.extend(submit_together(&harness.batcher, &["last".to_owned()]).await);

    assert_eq!(harness.record.out(), STEADY_FLUSHERS + 1);
    assert_eq!(harness.record.sizes(), vec![1, 1, FUSED_BATCH_MAX_ROWS]);
    harness.gate.open();
    let _answers = answers_of(handles).await;
}

/// With the steady flushers busy, 70 rows wait: the surge flusher writes 64
/// of them and ends; a steady flusher writes the other 6.
#[tokio::test]
async fn a_surge_flusher_writes_full_batches_only() {
    let harness = gated_harness(bound(3));
    let mut handles = vec![
        submit_alone(&harness.batcher, "a").await,
        submit_alone(&harness.batcher, "b").await,
    ];
    handles.extend(submit_together(&harness.batcher, &numbered("w", 70)).await);

    assert_eq!(harness.record.sizes(), vec![1, 1, FUSED_BATCH_MAX_ROWS]);
    harness.gate.open();
    let _answers = answers_of(handles).await;

    let mut sizes = harness.record.sizes();
    sizes.sort_unstable();
    assert_eq!(
        sizes,
        vec![1, 1, 70 - FUSED_BATCH_MAX_ROWS, FUSED_BATCH_MAX_ROWS]
    );
    assert_eq!(harness.record.most_out(), 3);
}

#[tokio::test]
async fn the_flusher_bound_holds_for_both_roles() {
    let harness = gated_harness(bound(3));
    let mut handles = vec![
        submit_alone(&harness.batcher, "a").await,
        submit_alone(&harness.batcher, "b").await,
    ];
    handles
        .extend(submit_together(&harness.batcher, &numbered("w", 4 * FUSED_BATCH_MAX_ROWS)).await);

    assert_eq!(harness.record.out(), 3);
    harness.gate.open();
    let _answers = answers_of(handles).await;
    assert_eq!(harness.record.most_out(), 3);
}

#[tokio::test]
async fn a_pool_of_one_connection_has_one_flusher() {
    let harness = gated_harness(bound(1));
    let mut handles = vec![
        submit_alone(&harness.batcher, "a").await,
        submit_alone(&harness.batcher, "b").await,
    ];
    handles
        .extend(submit_together(&harness.batcher, &numbered("w", 2 * FUSED_BATCH_MAX_ROWS)).await);

    assert_eq!(harness.record.out(), 1);
    harness.gate.open();
    let _answers = answers_of(handles).await;
    assert_eq!(harness.record.most_out(), 1);
}

/// Python refuses a bound below 1. The core bound is a `NonZeroUsize` from
/// the worker pool: a pool whose batch connections the claim pass takes in
/// full gives no bound.
#[test]
fn the_flusher_bound_must_be_positive() {
    let sizing = PoolSizing::new(3, Reserve::KeepFree(WORKER_RESERVED_CONNECTIONS))
        .expect("a pool of 3 keeps 1 batch connection");

    assert_eq!(
        FlusherBound::of_worker_pool(&sizing),
        Err(NoFlusherConnection { batch_limit: 1 })
    );
}

/// Python `test_the_worker_leaves_the_claim_loop_and_the_reserve_free`: the
/// flushers leave the claim pass and the reserve of 2 free.
#[test]
fn the_flusher_bound_leaves_the_claim_pass_and_the_reserve_free() {
    for (pool, flushers) in [(5, 2), (20, 17)] {
        let sizing = PoolSizing::new(pool, Reserve::KeepFree(WORKER_RESERVED_CONNECTIONS))
            .expect("a worker pool keeps batch connections");
        let bound = FlusherBound::of_worker_pool(&sizing).expect("the pool leaves flushers");
        assert_eq!(bound.get().get(), flushers, "a pool of {pool}");
    }
}

/// The steady flushers stay busy; the surge flusher finishes its full batch
/// and ends: the 10 rows left wait for a steady flusher.
#[tokio::test]
async fn a_surge_flusher_leaves_a_partial_batch_to_the_steady_flushers() {
    let gates = Arc::new(PerWriteGates::default());
    let record = Arc::new(Record::default());
    let write = PerWriteGatedWrite {
        record: Arc::clone(&record),
        gates: Arc::clone(&gates),
    };
    let batcher = FinalizeBatcher::new(write, QUEUE_BOUND, bound(3));
    let mut handles = Vec::new();
    for name in ["a", "b"] {
        handles.push(tokio::spawn(batcher.submit(row(name)).answer()));
        settle().await;
    }
    for name in numbered("w", FUSED_BATCH_MAX_ROWS + 10) {
        handles.push(tokio::spawn(batcher.submit(row(&name)).answer()));
    }
    settle().await;
    assert_eq!(record.sizes(), vec![1, 1, FUSED_BATCH_MAX_ROWS]);

    gates.open(2);
    settle().await;

    assert_eq!(record.sizes(), vec![1, 1, FUSED_BATCH_MAX_ROWS]);
    gates.open(0);
    settle().await;
    assert_eq!(record.sizes(), vec![1, 1, FUSED_BATCH_MAX_ROWS, 10]);
    gates.open(1);
    gates.open(3);
    for handle in handles {
        let _answer = tokio::time::timeout(OUTER_BOUND, handle)
            .await
            .expect("the answer came within the outer bound")
            .expect("the answer task ended without a panic");
    }
}

/// A gate per write, in the order the writes start.
#[derive(Default)]
struct PerWriteGates {
    gates: Mutex<Vec<Arc<Gate>>>,
}

impl PerWriteGates {
    fn next(&self) -> Arc<Gate> {
        let gate = Gate::shut();
        self.gates
            .lock()
            .expect("gates lock")
            .push(Arc::clone(&gate));
        gate
    }

    fn open(&self, write: usize) {
        let gate = Arc::clone(&self.gates.lock().expect("gates lock")[write]);
        gate.open();
    }
}

/// A batch write whose every call waits for its own gate.
struct PerWriteGatedWrite {
    record: Arc<Record>,
    gates: Arc<PerWriteGates>,
}

impl SuccessBatchWrite for PerWriteGatedWrite {
    async fn write(&self, batch: &SuccessBatch) -> Result<BatchOutcomes, BatchWriteError> {
        self.record.begin(batch);
        let gate = self.gates.next();
        gate.pass().await;
        self.record.end();
        all_applied(batch)
    }
}

/// Python counts the asyncio tasks of surge flushers. The core counts the
/// running flushers of each role; a surge flusher is counted when its task
/// is spawned.
#[tokio::test]
async fn no_surge_flusher_is_created_while_less_than_a_full_batch_waits() {
    let harness = gated_harness(bound(4));
    let mut handles = vec![
        submit_alone(&harness.batcher, "a").await,
        submit_alone(&harness.batcher, "b").await,
    ];
    handles.extend(calls(
        &harness.batcher,
        &numbered("w", FUSED_BATCH_MAX_ROWS - 1),
    ));
    tokio::task::yield_now().await;

    assert_eq!(harness.batcher.running_flushers().surge, 0);

    handles.push(call(&harness.batcher, "last"));
    tokio::task::yield_now().await;

    assert_eq!(harness.batcher.running_flushers().surge, 1);
    harness.gate.open();
    let _answers = answers_of(handles).await;
}

// ---------------------------------------------------------------------------
// Connections: each write checks out one connection for its statement; no
// flusher holds one between batches (Python's held flusher connections)
// ---------------------------------------------------------------------------

/// Python `test_each_running_steady_flusher_writes_on_its_own_runner`.
#[tokio::test]
async fn each_running_steady_flusher_writes_on_a_connection_of_its_own() {
    let harness = gated_harness(steady_only());
    let handles = vec![
        submit_alone(&harness.batcher, "a").await,
        submit_alone(&harness.batcher, "b").await,
    ];

    assert_eq!(harness.batcher.running_flushers().steady, STEADY_FLUSHERS);
    assert_eq!(harness.record.out(), STEADY_FLUSHERS);
    harness.gate.open();
    let _answers = answers_of(handles).await;
    assert_eq!(harness.record.out(), 0);
}

/// Python `test_a_surge_flusher_writes_on_the_surge_runner`: a surge flusher
/// checks out its own connection, as a steady flusher does.
#[tokio::test]
async fn a_surge_flusher_writes_on_a_connection_of_its_own() {
    let harness = gated_harness(bound(3));
    let mut handles = vec![
        submit_alone(&harness.batcher, "a").await,
        submit_alone(&harness.batcher, "b").await,
    ];
    handles.extend(submit_together(&harness.batcher, &numbered("w", FUSED_BATCH_MAX_ROWS)).await);

    assert_eq!(harness.record.sizes(), vec![1, 1, FUSED_BATCH_MAX_ROWS]);
    assert_eq!(
        harness.batcher.running_flushers(),
        RunningFlushers {
            steady: STEADY_FLUSHERS,
            surge: 1,
        }
    );
    assert_eq!(harness.record.out(), 3);
    harness.gate.open();
    let _answers = answers_of(handles).await;
}

/// Python `test_a_steady_runner_goes_back_when_its_flusher_ends`: lone rows
/// one after another. Each flusher ends before the next row and holds no
/// connection after it; then two rows at once write on two connections.
#[tokio::test]
async fn a_flusher_holds_no_connection_after_its_batch() {
    let gate = Gate::opened();
    let harness = harness_with(
        Arc::clone(&gate),
        Box::new(all_applied),
        QUEUE_BOUND,
        steady_only(),
    );
    for name in ["a", "b", "c", "d", "e"] {
        let _answer = harness.batcher.complete(row(name)).await;
        settle().await;
        assert_eq!(harness.record.out(), 0);
        assert_eq!(
            harness.batcher.running_flushers(),
            RunningFlushers::default()
        );
    }
    assert_eq!(harness.record.batches().len(), 5);

    let _was_open = gate.open.send_replace(false);
    let handles = vec![
        submit_alone(&harness.batcher, "f").await,
        submit_alone(&harness.batcher, "g").await,
    ];

    assert_eq!(harness.record.out(), STEADY_FLUSHERS);
    gate.open();
    let _answers = answers_of(handles).await;
}

/// Python `test_a_pool_of_one_connection_writes_on_one_steady_runner`.
#[tokio::test]
async fn a_bound_of_one_writes_on_one_connection_at_a_time() {
    let harness = open_harness(bound(1));
    for name in ["a", "b", "c"] {
        let _answer = harness.batcher.complete(row(name)).await;
    }

    assert_eq!(harness.record.batches().len(), 3);
    assert_eq!(harness.record.most_out(), 1);
    assert_eq!(harness.batcher.max_flushers(), 1);
}

/// Python `test_the_worker_holds_a_connection_for_claims_and_one_per_steady_flusher`
/// and `test_fewer_runners_than_steady_flushers_is_refused`: the core holds
/// no flusher connection, so an idle batcher holds none and runs no
/// flusher. The bound and the connections come from one pool size
/// (`the_flusher_bound_must_be_positive`), so they cannot disagree.
#[tokio::test]
async fn an_idle_batcher_holds_no_connection() {
    let harness = open_harness(steady_only());

    assert_eq!(
        harness.batcher.running_flushers(),
        RunningFlushers::default()
    );
    assert_eq!(harness.record.out(), 0);
    let _answer = harness.batcher.complete(row("a")).await;
    settle().await;

    assert_eq!(
        harness.batcher.running_flushers(),
        RunningFlushers::default()
    );
    assert_eq!(harness.record.out(), 0);
}

// ---------------------------------------------------------------------------
// The stop of the outcome writes
// ---------------------------------------------------------------------------

/// An outcome write that the drain cancels leaves its row in the batcher,
/// and the close, which runs after the drain, still writes that row.
///
/// Time is paused: the drain bound passes at 50 ms with the write gated;
/// the gate opens at 100 ms, inside the close. With the close before the
/// drain, the write would get its answer and the drain would cancel nothing.
#[tokio::test(start_paused = true)]
async fn stop_closes_the_finalize_batcher_after_the_outcome_write_drain() {
    let harness = gated_harness(steady_only());
    let mut writes: JoinSet<RowAnswer> = JoinSet::new();
    let writer = harness.batcher.clone();
    let _abort = writes.spawn(async move { writer.complete(row("a")).await });
    settle().await;
    assert_eq!(harness.record.batches(), vec![tasks(&["a"])]);
    let gate = Arc::clone(&harness.gate);
    let _opener = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        gate.open();
    });

    let stopped =
        stop::drain_then_close(&mut writes, Duration::from_millis(50), &harness.batcher).await;

    assert_eq!(
        stopped,
        stop::OutcomeWritesStopped {
            writes_cancelled: 1,
            close: CloseReport::FlushersDrained,
        }
    );
    assert!(writes.is_empty());
    assert_eq!(harness.record.batches(), vec![tasks(&["a"])]);
    assert_eq!(harness.record.out(), 0);
    assert_eq!(harness.batcher.state(), BatcherState::Closed);
}

/// Python `test_stop_logs_the_rows_that_the_batcher_close_left`. The core
/// tests have no log capture; the line is built by a pure function that the
/// stop logs at warning level.
#[test]
fn stop_logs_the_rows_that_the_batcher_close_left() {
    let line = stop::rows_left_by_close(CloseReport::FlushersCancelled {
        rows_in_flight: 3,
        rows_unsent: 2,
    })
    .expect("a cancelled close leaves rows to log");

    assert!(line.contains("3 rows in a cancelled statement"), "{line}");
    assert!(line.contains("2 rows not sent"), "{line}");
    assert!(line.contains("after 10s"), "{line}");
    assert_eq!(stop::rows_left_by_close(CloseReport::FlushersDrained), None);
}

/// With the close timeout passed, the stop reports the rows the close left,
/// and every write still waiting gets its stop answer.
#[tokio::test(start_paused = true)]
async fn stop_reports_the_rows_of_a_close_that_timed_out() {
    let harness = gated_harness(steady_only());
    let mut writes: JoinSet<RowAnswer> = JoinSet::new();
    for name in ["a", "b", "c"] {
        let writer = harness.batcher.clone();
        let _abort = writes.spawn(async move { writer.complete(row(name)).await });
        settle().await;
    }

    let stopped =
        stop::drain_then_close(&mut writes, Duration::from_millis(50), &harness.batcher).await;

    assert_eq!(
        stopped,
        stop::OutcomeWritesStopped {
            writes_cancelled: 3,
            close: CloseReport::FlushersCancelled {
                rows_in_flight: 2,
                rows_unsent: 1,
            },
        }
    );
}
