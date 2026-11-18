//! Group commit of the plain-success writes of one worker.
//!
//! The outcome writer hands each plain success to [`FinalizeBatcher::submit`]
//! and waits for the answer of its own row. The batcher writes the queued
//! rows with one statement per batch: one round trip for up to
//! [`FUSED_BATCH_MAX_ROWS`] rows.
//!
//! Flush rule, with no timer:
//!
//! - A submit starts a steady flusher while fewer than
//!   [`FINALIZE_BATCH_FLUSHERS`] run. A steady flusher takes every queued
//!   row, up to [`FUSED_BATCH_MAX_ROWS`], writes them, and takes again. It
//!   ends when the queue is empty.
//! - A submit that leaves a full batch waiting starts a surge flusher, while
//!   fewer flushers than the [`FlusherBound`] run in all. A surge flusher
//!   writes full batches only, and ends when less than a full batch waits.
//!
//! So a lone row is written at once, and the rows that arrive while the
//! steady flushers write form the next batch.
//!
//! Connections: no flusher holds a connection. Each write checks out one
//! connection of the worker pool for its statement, through the reserve gate
//! of the pool, and gives it back when the statement ends. The flusher bound
//! is the batch connections of the pool less the connection of the claim
//! pass, so the flushers and the claim pass leave the reserve of the pool
//! free for the reaper and the per-task writes.
//!
//! An answer resolves only after the statement of its batch returned: an
//! `Applied` row is committed when its writer reads it.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinSet};
use uuid::Uuid;

use crate::core::config::child_sizing::{FINALIZE_BATCH_FLUSHERS, PARENT_POOL_CLAIM_CONNECTIONS};
use crate::core::lifecycle::TerminalizationOutcome;
use crate::db::{PoolSizing, StatementFailure, Transience};
use crate::failure_cause::{FailureCause, ServerErrorFacts};

pub mod statement;
pub mod stop;
#[cfg(test)]
mod tests;

/// The most rows of one batched plain-success statement. Each row holds its
/// task's advisory lock and row lock until the statement ends, so this
/// bounds the locks and the server time of one statement.
pub const FUSED_BATCH_MAX_ROWS: usize = 64;

/// How long the worker stop lets the flushers write the queued rows before
/// it cancels them (Python `FINALIZE_BATCH_CLOSE_TIMEOUT_S`).
pub const FINALIZE_BATCH_CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

/// [`FINALIZE_BATCH_FLUSHERS`] as a count of tasks.
const STEADY_FLUSHERS: usize = FINALIZE_BATCH_FLUSHERS as usize;

/// One plain success to write: the task, the claim generation that fences
/// the write, and the result envelope text that history stores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainSuccess {
    pub task_id: Uuid,
    pub claimed_at: DateTime<Utc>,
    pub result_json: String,
}

/// The rows of one statement, in input order: 1 to [`FUSED_BATCH_MAX_ROWS`]
/// rows with distinct task ids. Only [`SuccessBatch::new`] builds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuccessBatch {
    rows: Vec<PlainSuccess>,
}

/// Rows that cannot form one statement.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SuccessBatchError {
    /// No row, or more than [`FUSED_BATCH_MAX_ROWS`].
    #[error("a plain-success batch holds 1 to {FUSED_BATCH_MAX_ROWS} rows, got {size}")]
    SizeOutOfBound { size: usize },
    /// The statement refuses a repeated task id: a repeat would write one
    /// row twice in one statement.
    #[error("a plain-success batch names task {task_id} twice")]
    TaskIdTwice { task_id: Uuid },
}

impl SuccessBatch {
    /// Checks the size and that no task id repeats. Pure.
    pub fn new(rows: Vec<PlainSuccess>) -> Result<SuccessBatch, SuccessBatchError> {
        match rows.len() {
            1..=FUSED_BATCH_MAX_ROWS => {}
            size => return Err(SuccessBatchError::SizeOutOfBound { size }),
        }
        let mut seen = HashSet::with_capacity(rows.len());
        for row in &rows {
            if !seen.insert(row.task_id) {
                return Err(SuccessBatchError::TaskIdTwice {
                    task_id: row.task_id,
                });
            }
        }
        Ok(SuccessBatch { rows })
    }

    pub fn rows(&self) -> &[PlainSuccess] {
        &self.rows
    }
}

/// Why the statement of a batch gave no outcome per row. The same error is
/// the answer of every row of the batch.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BatchWriteError {
    /// Nothing reached the server: no row changed.
    #[error("the plain-success batch was not sent: {detail}")]
    NotSent {
        transience: Transience,
        detail: String,
        cause: FailureCause,
    },
    /// The server refused the statement: no row changed. One row of the
    /// batch can be the cause.
    #[error("the plain-success batch was refused: {detail}")]
    Refused {
        transience: Transience,
        detail: String,
        server_error: ServerErrorFacts,
    },
    /// The statement was sent and its reply was lost: each row may have
    /// committed or not. A replay of a row finds it `AlreadyApplied` when it
    /// committed.
    #[error("the plain-success batch has an unknown outcome: {detail}")]
    CommitUnknown { detail: String, cause: FailureCause },
    /// The statement returned rows that break its contract: a row that does
    /// not decode, a count other than the batch, or an outcome of another
    /// task at the position of a row. The statement completed, so what it
    /// wrote is committed; its rows do not say which rows it wrote.
    #[error("the plain-success batch statement broke its row contract: {detail}")]
    ContractBroken { detail: String },
}

impl BatchWriteError {
    /// Whether the cause may clear. An unknown outcome is transient: a
    /// replay classifies each row. A broken contract is permanent.
    pub fn transience(&self) -> Transience {
        match self {
            Self::NotSent { transience, .. } | Self::Refused { transience, .. } => *transience,
            Self::CommitUnknown { .. } => Transience::Transient,
            Self::ContractBroken { .. } => Transience::Permanent,
        }
    }
}

impl From<&StatementFailure> for BatchWriteError {
    fn from(failure: &StatementFailure) -> BatchWriteError {
        let detail = failure.to_string();
        let transience = failure.transience();
        match failure {
            StatementFailure::NotSent { .. } => BatchWriteError::NotSent {
                transience,
                detail,
                cause: failure.failure_cause(),
            },
            StatementFailure::StatementRefused { cause, .. } => BatchWriteError::Refused {
                transience,
                detail,
                server_error: ServerErrorFacts::of(cause.server_error()),
            },
            StatementFailure::OutcomeUnknown { .. } => BatchWriteError::CommitUnknown {
                detail,
                cause: failure.failure_cause(),
            },
        }
    }
}

/// One outcome per row of a batch, in input order: the outcome at a
/// position is the outcome of the task at that position. Only
/// [`BatchOutcomes::for_batch`] builds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchOutcomes {
    outcomes: Vec<TerminalizationOutcome>,
}

impl BatchOutcomes {
    /// Checks that `outcomes` holds one outcome per row of `batch`, each of
    /// the task at its position. Pure.
    ///
    /// Errors: [`BatchWriteError::ContractBroken`] naming the first
    /// difference.
    pub fn for_batch(
        batch: &SuccessBatch,
        outcomes: Vec<TerminalizationOutcome>,
    ) -> Result<BatchOutcomes, BatchWriteError> {
        if outcomes.len() != batch.rows.len() {
            return Err(BatchWriteError::ContractBroken {
                detail: format!(
                    "{} outcomes for a batch of {} rows",
                    outcomes.len(),
                    batch.rows.len()
                ),
            });
        }
        for (row, outcome) in batch.rows.iter().zip(&outcomes) {
            if outcome.task_id() != row.task_id {
                return Err(BatchWriteError::ContractBroken {
                    detail: format!(
                        "an outcome of task {} at the position of task {}",
                        outcome.task_id(),
                        row.task_id
                    ),
                });
            }
        }
        Ok(BatchOutcomes { outcomes })
    }

    pub fn into_outcomes(self) -> Vec<TerminalizationOutcome> {
        self.outcomes
    }
}

/// Writes one batch as one statement: the batched success statement on the
/// worker pool, or a scripted write in a test.
pub trait SuccessBatchWrite: Send + Sync + 'static {
    /// Write `batch` in one statement, on one checkout of the worker pool.
    ///
    /// Outcomes: `Ok` with one outcome per row; `Err` with the error of the
    /// statement, which every row of the batch gets.
    fn write(
        &self,
        batch: &SuccessBatch,
    ) -> impl Future<Output = Result<BatchOutcomes, BatchWriteError>> + Send;
}

/// The answer for one row: the outcome of its task, or why it has none.
pub type RowAnswer = Result<TerminalizationOutcome, RowNotWritten>;

/// Why a row has no outcome of its own.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RowNotWritten {
    /// The statement of the row's batch failed; the error says whether the
    /// row changed.
    #[error(transparent)]
    WriteFailed(BatchWriteError),
    /// The worker stop cancelled the flushers at the close timeout while
    /// the statement of the row ran: its commit is unknown.
    #[error("the worker stop cancelled the statement of the row; its commit is unknown")]
    StatementCancelledAtStop,
    /// The worker stop cancelled the flushers before the row was sent: the
    /// row stays RUNNING, and stale RUNNING recovery fails it.
    #[error("the worker stop cancelled the batch writes before the row was sent")]
    NotSentAtStop,
}

/// What [`FinalizeBatcher::close`] saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReport {
    /// Every queued row was written.
    FlushersDrained,
    /// The close timeout cancelled the flushers. `rows_in_flight` were in a
    /// statement that the cancel ended: their commit is unknown.
    /// `rows_unsent` were never sent: they stay RUNNING for stale recovery.
    FlushersCancelled {
        rows_in_flight: usize,
        rows_unsent: usize,
    },
}

/// Whether the batcher takes rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatcherState {
    /// Takes rows.
    Open,
    /// Refuses rows: the worker stops.
    Closed,
}

/// The most flushers that write at once, steady and surge together: the
/// batch connections of the worker pool ([`PoolSizing::batch_limit`]) less
/// the connection of the claim pass. Each flusher checks out at most one
/// connection, for the statement of one batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlusherBound(NonZeroUsize);

/// The worker pool has no batch connection for a flusher after the claim
/// pass. AppConfig validation refuses a worker pool below the minimum, so a
/// validated profile never meets it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "a worker pool with {batch_limit} batch connections leaves none for the batched success \
     writes after the claim pass"
)]
pub struct NoFlusherConnection {
    pub batch_limit: u32,
}

impl FlusherBound {
    /// The flusher bound of a worker pool. Pure.
    ///
    /// Errors: [`NoFlusherConnection`] when the claim pass takes every batch
    /// connection.
    pub fn of_worker_pool(sizing: &PoolSizing) -> Result<FlusherBound, NoFlusherConnection> {
        let batch_limit = sizing.batch_limit();
        let flushers = batch_limit
            .checked_sub(PARENT_POOL_CLAIM_CONNECTIONS)
            .and_then(|count| usize::try_from(count).ok())
            .and_then(NonZeroUsize::new);
        match flushers {
            Some(flushers) => Ok(FlusherBound(flushers)),
            None => Err(NoFlusherConnection { batch_limit }),
        }
    }

    pub fn get(self) -> NonZeroUsize {
        self.0
    }

    /// The steady flushers: [`FINALIZE_BATCH_FLUSHERS`], or the bound when
    /// it is smaller. The rest of the bound is for surge flushers.
    fn split(self) -> FlusherBounds {
        let steady = STEADY_FLUSHERS.min(self.0.get());
        FlusherBounds {
            steady,
            surge: self.0.get() - steady,
        }
    }
}

/// The bound of each role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FlusherBounds {
    steady: usize,
    surge: usize,
}

/// The flushers that run now, by role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RunningFlushers {
    pub steady: usize,
    pub surge: usize,
}

/// A steady flusher writes whenever rows wait. A surge flusher writes only
/// while a full batch waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlusherRole {
    Steady,
    Surge,
}

impl FlusherRole {
    fn as_str(self) -> &'static str {
        match self {
            Self::Steady => "steady",
            Self::Surge => "surge",
        }
    }
}

/// The key of one submitted row; a task id can be queued twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct RowKey(u64);

/// A row in the queue or in a statement. Its answer channel stays in
/// [`State::answers`] until the row is answered.
#[derive(Debug, Clone)]
struct QueuedRow {
    key: RowKey,
    row: PlainSuccess,
}

/// Why a submit is refused. Both are defects of the caller.
enum SubmitRefusal {
    Closed,
    QueueFull { held: usize },
}

/// What a flusher takes from the queue.
enum Taken {
    /// Rows for one statement.
    Batch(Vec<QueuedRow>),
    /// Nothing for this role: the flusher ends, and its count is already
    /// lowered.
    Ended,
}

/// The state behind the lock. Every critical section is free of panics, so
/// the lock is never poisoned.
struct State {
    phase: BatcherState,
    queue: VecDeque<QueuedRow>,
    /// The answer channel of every row not answered yet: queued or in a
    /// statement. At most the queue bound plus one batch per flusher.
    answers: HashMap<RowKey, oneshot::Sender<RowAnswer>>,
    running: RunningFlushers,
    rows_in_flight: usize,
    next_key: u64,
    flushers: JoinSet<()>,
}

impl State {
    /// Queue `row` and say which flusher the flush rule starts, if any. The
    /// count of that role is raised here, in the same critical section.
    fn admit(
        &mut self,
        row: PlainSuccess,
        answer: oneshot::Sender<RowAnswer>,
        max_queued_rows: NonZeroUsize,
        bounds: FlusherBounds,
    ) -> Result<Option<FlusherRole>, SubmitRefusal> {
        match self.phase {
            BatcherState::Open => {}
            BatcherState::Closed => return Err(SubmitRefusal::Closed),
        }
        if self.queue.len() >= max_queued_rows.get() {
            return Err(SubmitRefusal::QueueFull {
                held: self.queue.len(),
            });
        }
        let key = RowKey(self.next_key);
        self.next_key = self.next_key.wrapping_add(1);
        self.queue.push_back(QueuedRow { key, row });
        let _absent = self.answers.insert(key, answer);
        let role = match (
            self.running.steady < bounds.steady,
            self.running.surge < bounds.surge && self.queue.len() >= FUSED_BATCH_MAX_ROWS,
        ) {
            (true, _) => Some(FlusherRole::Steady),
            (false, true) => Some(FlusherRole::Surge),
            (false, false) => None,
        };
        match role {
            Some(FlusherRole::Steady) => self.running.steady += 1,
            Some(FlusherRole::Surge) => self.running.surge += 1,
            None => {}
        }
        Ok(role)
    }

    /// Take the queued rows, oldest first, up to [`FUSED_BATCH_MAX_ROWS`].
    ///
    /// A surge flusher takes nothing while less than a full batch waits. A
    /// row whose task id the batch already holds stays queued, first in
    /// line, for the next batch. Taking nothing ends the flusher: its count
    /// is lowered here, so a submit never sees a flusher that ended.
    fn take_batch(&mut self, role: FlusherRole) -> Taken {
        let full_batch_waits = self.queue.len() >= FUSED_BATCH_MAX_ROWS;
        let mut taken: Vec<QueuedRow> = Vec::new();
        match (role, full_batch_waits) {
            (FlusherRole::Surge, false) => {}
            (FlusherRole::Steady, _) | (FlusherRole::Surge, true) => {
                let mut task_ids = HashSet::new();
                let mut skipped: Vec<QueuedRow> = Vec::new();
                while taken.len() < FUSED_BATCH_MAX_ROWS {
                    let Some(queued) = self.queue.pop_front() else {
                        break;
                    };
                    match task_ids.insert(queued.row.task_id) {
                        true => taken.push(queued),
                        false => skipped.push(queued),
                    }
                }
                for queued in skipped.into_iter().rev() {
                    self.queue.push_front(queued);
                }
            }
        }
        match taken.is_empty() {
            true => {
                self.lower_count(role);
                Taken::Ended
            }
            false => {
                self.rows_in_flight += taken.len();
                Taken::Batch(taken)
            }
        }
    }

    fn lower_count(&mut self, role: FlusherRole) {
        match role {
            FlusherRole::Steady => self.running.steady = self.running.steady.saturating_sub(1),
            FlusherRole::Surge => self.running.surge = self.running.surge.saturating_sub(1),
        }
    }

    /// Joined flushers that ended. A flusher that panicked gives its panic,
    /// for the caller to raise after it leaves the lock.
    fn reap_finished(&mut self) -> Option<Box<dyn std::any::Any + Send>> {
        let mut panic = None;
        while let Some(joined) = self.flushers.try_join_next() {
            if let Err(error) = joined {
                if let Ok(payload) = error.try_into_panic() {
                    panic = Some(payload);
                }
            }
        }
        panic
    }
}

/// What the flushers share.
struct Shared<W: SuccessBatchWrite> {
    write: W,
    max_queued_rows: NonZeroUsize,
    bounds: FlusherBounds,
    state: Mutex<State>,
}

impl<W: SuccessBatchWrite> Shared<W> {
    fn lock_state(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect(
            "no critical section of the finalize batcher panics, so its lock is not poisoned",
        )
    }

    /// Send `answer` to the caller of `key`. A caller that left (its future
    /// was dropped) does not read it; its row is written all the same.
    fn answer(&self, key: RowKey, answer: RowAnswer) {
        let sender = self.lock_state().answers.remove(&key);
        let sender = sender
            .expect("a row in a statement keeps its answer channel until its flusher answers it");
        let _unread = sender.send(answer);
    }

    /// Write `rows` as one batch and answer each row.
    ///
    /// When the server refuses a batch of several rows, one row can be the
    /// cause: each row is then written alone, once, and gets the answer of
    /// its own write. A failure that is not about one row (not sent, an
    /// unknown outcome, a broken contract) is the answer of every row.
    async fn write_and_answer(&self, rows: &[QueuedRow]) {
        let batch = batch_of(rows);
        let error = match self.write.write(&batch).await {
            Ok(outcomes) => {
                tracing::debug!(rows = rows.len(), "the finalize batch wrote its rows");
                for (queued, outcome) in rows.iter().zip(outcomes.into_outcomes()) {
                    self.answer(queued.key, Ok(outcome));
                }
                return;
            }
            Err(error) => error,
        };
        match after_failure(&error, rows.len()) {
            AfterFailure::WriteEachAlone => {
                tracing::warn!(
                    rows = rows.len(),
                    %error,
                    "Finalize batch of {} rows refused ({error}); writing each row alone",
                    rows.len(),
                );
                for queued in rows {
                    let answer = self.write_alone(queued).await;
                    self.answer(queued.key, answer);
                }
            }
            AfterFailure::AnswerEveryRow => {
                tracing::error!(
                    rows = rows.len(),
                    %error,
                    "the finalize batch write failed: {error}; every row of it gets the error",
                );
                for queued in rows {
                    self.answer(queued.key, Err(RowNotWritten::WriteFailed(error.clone())));
                }
            }
        }
    }

    /// Write one row as a batch of one.
    async fn write_alone(&self, queued: &QueuedRow) -> RowAnswer {
        let batch = batch_of(std::slice::from_ref(queued));
        match self.write.write(&batch).await {
            Ok(outcomes) => Ok(outcomes
                .into_outcomes()
                .into_iter()
                .next()
                .expect("BatchOutcomes holds one outcome per row of its batch")),
            Err(error) => Err(RowNotWritten::WriteFailed(error)),
        }
    }
}

/// What follows a failed batch write.
enum AfterFailure {
    WriteEachAlone,
    AnswerEveryRow,
}

/// A refused batch of several rows is split; any other failure is the
/// answer of every row. Pure.
fn after_failure(error: &BatchWriteError, rows: usize) -> AfterFailure {
    match (error, rows) {
        (BatchWriteError::Refused { .. }, 0 | 1)
        | (
            BatchWriteError::NotSent { .. }
            | BatchWriteError::CommitUnknown { .. }
            | BatchWriteError::ContractBroken { .. },
            _,
        ) => AfterFailure::AnswerEveryRow,
        (BatchWriteError::Refused { .. }, _) => AfterFailure::WriteEachAlone,
    }
}

/// The batch of rows that [`State::take_batch`] took: 1 to the maximum, no
/// task id twice.
fn batch_of(rows: &[QueuedRow]) -> SuccessBatch {
    SuccessBatch::new(rows.iter().map(|queued| queued.row.clone()).collect())
        .expect("take_batch takes 1 to FUSED_BATCH_MAX_ROWS rows with distinct task ids")
}

/// The count and the rows of one running flusher. It lowers the count when
/// the flusher future is dropped before the flusher ended: an abort at the
/// close timeout, or a panic.
struct RunningFlusher<W: SuccessBatchWrite> {
    shared: Arc<Shared<W>>,
    role: FlusherRole,
    in_flight: Vec<RowKey>,
    ended: bool,
}

impl<W: SuccessBatchWrite> RunningFlusher<W> {
    /// The statement of the held rows returned and every row is answered.
    fn finish_batch(&mut self) {
        let mut state = self.shared.lock_state();
        state.rows_in_flight = state.rows_in_flight.saturating_sub(self.in_flight.len());
        self.in_flight.clear();
    }
}

impl<W: SuccessBatchWrite> Drop for RunningFlusher<W> {
    fn drop(&mut self) {
        if self.ended {
            return;
        }
        let mut guard = self.shared.lock_state();
        let state = &mut *guard;
        state.lower_count(self.role);
        state.rows_in_flight = state.rows_in_flight.saturating_sub(self.in_flight.len());
        match state.phase {
            // The close aborted this flusher and answers what is left.
            BatcherState::Closed => {}
            // Only a panic ends a flusher early while the batcher is open.
            // The answers of its rows are dropped, so each caller fails
            // loud; with no other flusher left, the queued rows fail too.
            BatcherState::Open => {
                for key in &self.in_flight {
                    let _dropped = state.answers.remove(key);
                }
                if state.running.steady + state.running.surge == 0 {
                    for queued in state.queue.drain(..) {
                        let _dropped = state.answers.remove(&queued.key);
                    }
                }
            }
        }
    }
}

/// One flusher: write batches while the flush rule of `role` finds one.
async fn flush<W: SuccessBatchWrite>(shared: Arc<Shared<W>>, role: FlusherRole) {
    let mut running = RunningFlusher {
        shared: Arc::clone(&shared),
        role,
        in_flight: Vec::new(),
        ended: false,
    };
    loop {
        let taken = shared.lock_state().take_batch(role);
        let rows = match taken {
            Taken::Batch(rows) => rows,
            Taken::Ended => {
                running.ended = true;
                tracing::trace!(role = role.as_str(), "a finalize batch flusher ended");
                return;
            }
        };
        running.in_flight = rows.iter().map(|queued| queued.key).collect();
        shared.write_and_answer(&rows).await;
        running.finish_batch();
    }
}

/// Raise the panic of a joined flusher; a cancel (the close) is expected.
fn raise_panic_of(joined: Result<(), JoinError>) {
    if let Err(error) = joined {
        if let Ok(payload) = error.try_into_panic() {
            std::panic::resume_unwind(payload);
        }
    }
}

/// The answer of one submitted row.
///
/// Dropping it leaves the row queued or in its statement: its batch is
/// still written, and nobody reads the answer.
#[must_use = "a dropped answer leaves the row written with nobody reading its outcome"]
pub struct PendingRow {
    task_id: Uuid,
    receiver: oneshot::Receiver<RowAnswer>,
}

impl PendingRow {
    /// Wait for the answer. It comes only after the statement of the row's
    /// batch returned, or at the close.
    ///
    /// Panics when the answer channel closed with no answer: the flusher
    /// that held the row panicked (a defect, raised to every row it held).
    pub async fn answer(self) -> RowAnswer {
        let PendingRow { task_id, receiver } = self;
        match receiver.await {
            Ok(answer) => answer,
            Err(_closed) => panic!(
                "task {task_id}: the finalize batcher dropped the answer of its row; the flusher \
                 that held it panicked"
            ),
        }
    }
}

/// Group commit of plain-success writes; see the module documentation.
///
/// Each row in flight submits once, so the queue never holds more rows than
/// the worker has in flight. `max_queued_rows` bounds it; a full queue is a
/// defect.
pub struct FinalizeBatcher<W: SuccessBatchWrite> {
    shared: Arc<Shared<W>>,
}

impl<W: SuccessBatchWrite> Clone for FinalizeBatcher<W> {
    fn clone(&self) -> Self {
        FinalizeBatcher {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<W: SuccessBatchWrite> FinalizeBatcher<W> {
    /// A batcher that writes with `write`. Starts no flusher.
    pub fn new(
        write: W,
        max_queued_rows: NonZeroUsize,
        flushers: FlusherBound,
    ) -> FinalizeBatcher<W> {
        FinalizeBatcher {
            shared: Arc::new(Shared {
                write,
                max_queued_rows,
                bounds: flushers.split(),
                state: Mutex::new(State {
                    phase: BatcherState::Open,
                    queue: VecDeque::new(),
                    answers: HashMap::new(),
                    running: RunningFlushers::default(),
                    rows_in_flight: 0,
                    next_key: 0,
                    flushers: JoinSet::new(),
                }),
            }),
        }
    }

    pub fn state(&self) -> BatcherState {
        self.shared.lock_state().phase
    }

    /// The most flushers of both roles that run at once.
    pub fn max_flushers(&self) -> usize {
        self.shared.bounds.steady + self.shared.bounds.surge
    }

    pub fn running_flushers(&self) -> RunningFlushers {
        self.shared.lock_state().running
    }

    /// Queue `row` and start the flusher that the flush rule asks for.
    /// Returns at once; the row is queued when this returns.
    ///
    /// Must run inside a tokio runtime: a flusher is a task.
    ///
    /// Panics (defects of the caller): the batcher is closed; the queue
    /// holds `max_queued_rows` rows. Also raises the panic of a flusher that
    /// ended with one.
    pub fn submit(&self, row: PlainSuccess) -> PendingRow {
        let task_id = row.task_id;
        let (sender, receiver) = oneshot::channel();
        let (admitted, flusher_panic) = {
            let mut state = self.shared.lock_state();
            let admitted =
                state.admit(row, sender, self.shared.max_queued_rows, self.shared.bounds);
            if let Ok(Some(role)) = admitted {
                let _abort = state.flushers.spawn(flush(Arc::clone(&self.shared), role));
            }
            (admitted, state.reap_finished())
        };
        if let Some(payload) = flusher_panic {
            std::panic::resume_unwind(payload);
        }
        match admitted {
            Ok(_role) => PendingRow { task_id, receiver },
            Err(SubmitRefusal::Closed) => {
                panic!("task {task_id}: a plain-success write after the finalize batcher closed")
            }
            Err(SubmitRefusal::QueueFull { held }) => panic!(
                "task {task_id}: the finalize batch queue holds {held} rows, its bound; each row \
                 in flight submits once"
            ),
        }
    }

    /// Queue `row` and wait for its answer ([`FinalizeBatcher::submit`],
    /// then [`PendingRow::answer`]).
    pub async fn complete(&self, row: PlainSuccess) -> RowAnswer {
        self.submit(row).answer().await
    }

    /// Refuse new rows, and wait up to `timeout` for the flushers to write
    /// every queued row.
    ///
    /// Past the timeout the flushers are cancelled. The rows not answered
    /// then get [`RowNotWritten::StatementCancelledAtStop`] (in a statement
    /// that the cancel ended) or [`RowNotWritten::NotSentAtStop`] (still
    /// queued). Idempotent. Raises the panic of a flusher that panicked.
    pub async fn close(&self, timeout: Duration) -> CloseReport {
        let mut flushers = {
            let mut state = self.shared.lock_state();
            state.phase = BatcherState::Closed;
            std::mem::take(&mut state.flushers)
        };
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            match tokio::time::timeout_at(deadline, flushers.join_next()).await {
                Ok(None) => return CloseReport::FlushersDrained,
                Ok(Some(joined)) => raise_panic_of(joined),
                Err(_elapsed) => break,
            }
        }
        let rows_in_flight = self.shared.lock_state().rows_in_flight;
        flushers.abort_all();
        while let Some(joined) = flushers.join_next().await {
            raise_panic_of(joined);
        }
        let (unsent, in_statement) = {
            let mut guard = self.shared.lock_state();
            let state = &mut *guard;
            let unsent: Vec<oneshot::Sender<RowAnswer>> = state
                .queue
                .drain(..)
                .filter_map(|queued| state.answers.remove(&queued.key))
                .collect();
            let in_statement: Vec<oneshot::Sender<RowAnswer>> =
                state.answers.drain().map(|(_key, sender)| sender).collect();
            (unsent, in_statement)
        };
        let rows_unsent = unsent.len();
        for sender in unsent {
            let _unread = sender.send(Err(RowNotWritten::NotSentAtStop));
        }
        for sender in in_statement {
            let _unread = sender.send(Err(RowNotWritten::StatementCancelledAtStop));
        }
        CloseReport::FlushersCancelled {
            rows_in_flight,
            rows_unsent,
        }
    }
}
