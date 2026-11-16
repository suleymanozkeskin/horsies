//! The pool of isolated children: the FIFO of tasks, the state of each
//! slot, the answer of each task, and the stop (Python
//! `core/worker/slots.py`, the isolated half of `SlotPool`).

use std::collections::VecDeque;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::{oneshot, Notify};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::budgets::SlotService;
use super::dispatch::{AttemptEnd, ClaimedTask, NotRunCause};
use super::isolated::run_slot;
use super::route::IsolatedDeadline;
use crate::worker::executor::protocol::{
    InitFrame, IsolatedWork, ProcessId, RunFrame, TaskInputText,
};
use crate::worker::executor::supervisor::{
    BootFailure, BootFailureCause, ChildCommand, PoolAtFailure, SpawnerHandle, CHILD_KILL_WAIT,
};
use crate::worker::runner_beats::LiveDispatches;

/// After the drain deadline kills the busy children, the slot tasks get
/// this long more before they are aborted (Python: 30 s).
const AFTER_DRAIN_KILL_WAIT: Duration = CHILD_KILL_WAIT;

/// Recycle an isolated child after this many tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskLimit {
    Off,
    After(NonZeroU64),
}

/// Recycle an isolated child when its reported RSS reaches this many MiB.
/// An unreadable RSS never recycles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryLimit {
    Off,
    AtMb(NonZeroU64),
}

/// What the slot does when a killed child's exit is not seen within the
/// wait after `SIGKILL`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitNotSeen {
    /// Stale RUNNING recovery is on: write nothing; recovery or the claim
    /// lease decides the row.
    LeaveToRecovery,
    /// Stale RUNNING recovery is off: wait for the exit with no bound but
    /// the life of the pool, then write as at a seen exit.
    WaitForExit,
}

/// The fixed facts of an isolated pool.
#[derive(Debug, Clone)]
pub struct IsolatedPoolConfig {
    /// The isolated slots (Python: 1 to 1024).
    pub children: NonZeroUsize,
    /// The program of each child. The spawner appends the socket descriptor
    /// and the worker pid.
    pub command: ChildCommand,
    /// The first frame of each child; its kind is isolated.
    pub init: InitFrame,
    pub worker_id: String,
    /// Tasks that wait for a slot, at most (Python: `2 * max(seats +
    /// prefetch, max_claim_per_worker)`).
    pub pending_limit: NonZeroUsize,
    pub tasks_per_child: TaskLimit,
    pub memory_per_child: MemoryLimit,
    pub exit_not_seen: ExitNotSeen,
}

/// Why no replacement child can start. The worker stops with a non-zero
/// exit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolFailure {
    /// A start hook of a child failed or hung.
    ChildHookFailed,
    /// The last counted boot attempt of a slot failed.
    BootAttemptsExhausted { attempts: u32, last: BootFailure },
}

/// The health of the pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolHealth {
    Serving,
    Failed(PoolFailure),
    Stopping,
}

/// A slot that is down after a boot failure that can clear.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownState {
    pub cause: BootFailureCause,
    pub detail: String,
    pub down_since: DateTime<Utc>,
    pub failed_boots: NonZeroU32,
}

/// The state of one isolated slot (Python executor design, section 8.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IsolatedSlotState {
    Booting,
    Idle { pid: ProcessId },
    Busy { pid: ProcessId },
    Retiring,
    Down(DownState),
}

impl IsolatedSlotState {
    fn service(&self) -> SlotService {
        match self {
            Self::Booting => SlotService::Booting,
            Self::Idle { .. } | Self::Busy { .. } => SlotService::InService,
            Self::Retiring => SlotService::Retiring,
            Self::Down(_) => SlotService::Down,
        }
    }
}

/// Whether a slot booted a child at least once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BootedOnce {
    No,
    Yes,
}

/// The answer to one submitted task. Exactly one end reaches the caller:
/// the one sent, or the fallback when the slot ends without sending one.
pub(super) struct Answer {
    sender: Option<oneshot::Sender<AttemptEnd>>,
    fallback: AttemptEnd,
}

impl Answer {
    pub(super) fn new(sender: oneshot::Sender<AttemptEnd>) -> Self {
        Self {
            sender: Some(sender),
            fallback: AttemptEnd::NotDispatched(NotRunCause::PoolStopping),
        }
    }

    /// The frame goes to the child `pid`: from now on, an answer that never
    /// comes means its end is not known.
    pub(super) fn child_took_it(&mut self, pid: ProcessId) {
        self.fallback = AttemptEnd::EndUnknown { pid };
    }

    pub(super) fn send(mut self, end: AttemptEnd) {
        if let Some(sender) = self.sender.take() {
            // The caller may have gone away; the end is then not needed.
            let _ = sender.send(end);
        }
    }
}

impl Drop for Answer {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(self.fallback.clone());
        }
    }
}

/// One task that waits for a slot.
pub(super) struct QueuedRun {
    pub(super) frame: RunFrame,
    pub(super) deadline: IsolatedDeadline,
    pub(super) answer: Answer,
}

struct Board {
    slots: Vec<IsolatedSlotState>,
    booted: Vec<BootedOnce>,
    failure: Option<PoolFailure>,
}

/// The state that the pool and its slot tasks share.
pub(super) struct PoolShared {
    pub(super) config: IsolatedPoolConfig,
    pub(super) spawner: SpawnerHandle,
    pub(super) live: LiveDispatches,
    queue: Mutex<VecDeque<QueuedRun>>,
    pub(super) queued: Notify,
    board: Mutex<Board>,
    /// Cancelled at `stop`: no new task, booting children are ended.
    pub(super) stop: CancellationToken,
    /// Cancelled when the pool fails: every slot stops taking tasks.
    pub(super) failed: CancellationToken,
    /// Cancelled at the drain deadline: busy children are killed.
    pub(super) drain_deadline: CancellationToken,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // A poisoned lock means a panic while it was held; the queue and the
    // board are still valid values.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl PoolShared {
    pub(super) fn pop(&self) -> Option<QueuedRun> {
        lock(&self.queue).pop_front()
    }

    pub(super) fn set(&self, index: usize, state: IsolatedSlotState) {
        let mut board = lock(&self.board);
        if let IsolatedSlotState::Idle { .. } = state {
            board.booted[index] = BootedOnce::Yes;
        }
        board.slots[index] = state;
    }

    pub(super) fn state(&self, index: usize) -> IsolatedSlotState {
        lock(&self.board).slots[index].clone()
    }

    /// What the rest of the pool looks like at a failed boot of `index`.
    pub(super) fn pool_at_failure(&self, index: usize) -> PoolAtFailure {
        let board = lock(&self.board);
        let others = || {
            board
                .slots
                .iter()
                .enumerate()
                .filter(move |(other, _)| *other != index)
                .map(|(_, state)| state)
        };
        let any_booted = board.booted.contains(&BootedOnce::Yes);
        let every_slot_booted_or_down =
            board
                .slots
                .iter()
                .zip(&board.booted)
                .all(|(state, booted)| {
                    matches!(state, IsolatedSlotState::Down(_)) || *booted == BootedOnce::Yes
                });
        PoolAtFailure {
            first_generation_in: any_booted && every_slot_booted_or_down,
            another_child_serves: others().any(|state| {
                matches!(
                    state,
                    IsolatedSlotState::Idle { .. } | IsolatedSlotState::Busy { .. }
                )
            }),
            every_other_slot_down: others()
                .all(|state| matches!(state, IsolatedSlotState::Down(_))),
        }
    }

    /// Fail the pool: the first failure wins. Waiting tasks get
    /// `NotDispatched(PoolFailed)`.
    pub(super) fn fail(&self, failure: PoolFailure) {
        {
            let mut board = lock(&self.board);
            if board.failure.is_none() {
                tracing::error!(failure = ?failure, "the isolated pool failed: no replacement child can start");
                board.failure = Some(failure);
            }
        }
        self.failed.cancel();
        self.answer_waiting(NotRunCause::PoolFailed);
    }

    fn answer_waiting(&self, cause: NotRunCause) {
        let waiting: Vec<QueuedRun> = lock(&self.queue).drain(..).collect();
        for run in waiting {
            run.answer.send(AttemptEnd::NotDispatched(cause));
        }
    }

    fn health(&self) -> PoolHealth {
        let failure = lock(&self.board).failure.clone();
        match (failure, self.stop.is_cancelled()) {
            (Some(failure), _) => PoolHealth::Failed(failure),
            (None, true) => PoolHealth::Stopping,
            (None, false) => PoolHealth::Serving,
        }
    }
}

/// What a submit gave.
#[derive(Debug)]
pub enum Submitted {
    /// The task waits for a slot; its end comes on the receiver, exactly
    /// once.
    Queued(oneshot::Receiver<AttemptEnd>),
    /// The task never reached a child; its row is still CLAIMED.
    NotDispatched(NotRunCause),
}

/// How the stop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolStopReport {
    /// Every slot ended within the drain timeout.
    Drained,
    /// The drain deadline killed the busy children; every slot ended after.
    KilledAtDrainDeadline,
    /// Slot tasks still ran 30 s after the kills and were aborted. Their
    /// tasks got the end their answer held.
    AbortedAfterDrain,
}

/// The isolated children of a worker.
pub struct IsolatedPool {
    shared: Arc<PoolShared>,
    slots: JoinSet<()>,
}

impl IsolatedPool {
    /// Start one slot task per isolated child on the current runtime. Each
    /// boots its child at once.
    pub fn start(config: IsolatedPoolConfig, spawner: SpawnerHandle, live: LiveDispatches) -> Self {
        let children = config.children.get();
        let shared = Arc::new(PoolShared {
            config,
            spawner,
            live,
            queue: Mutex::new(VecDeque::new()),
            queued: Notify::new(),
            board: Mutex::new(Board {
                slots: vec![IsolatedSlotState::Booting; children],
                booted: vec![BootedOnce::No; children],
                failure: None,
            }),
            stop: CancellationToken::new(),
            failed: CancellationToken::new(),
            drain_deadline: CancellationToken::new(),
        });
        let mut slots = JoinSet::new();
        for index in 0..children {
            slots.spawn(run_slot(Arc::clone(&shared), index));
        }
        Self { shared, slots }
    }

    /// Queue one task for the next idle slot. The input is dropped once the
    /// frame is sent.
    pub fn submit(
        &self,
        task: &ClaimedTask,
        input: TaskInputText,
        work: IsolatedWork,
        deadline: IsolatedDeadline,
    ) -> Submitted {
        match self.shared.health() {
            PoolHealth::Serving => {}
            PoolHealth::Failed(_) => return Submitted::NotDispatched(NotRunCause::PoolFailed),
            PoolHealth::Stopping => return Submitted::NotDispatched(NotRunCause::PoolStopping),
        }
        let (sender, receiver) = oneshot::channel();
        {
            let mut queue = lock(&self.shared.queue);
            if queue.len() >= self.shared.config.pending_limit.get() {
                return Submitted::NotDispatched(NotRunCause::QueueFull);
            }
            queue.push_back(QueuedRun {
                frame: RunFrame {
                    task_id: task.task_id,
                    task_name: task.task_name.clone(),
                    worker_id: self.shared.config.worker_id.clone(),
                    claimed_at: task.claimed_at,
                    work,
                    input,
                },
                deadline,
                answer: Answer::new(sender),
            });
        }
        self.shared.queued.notify_one();
        Submitted::Queued(receiver)
    }

    /// The service state of each slot, for the claim budget.
    pub fn service(&self) -> Vec<SlotService> {
        lock(&self.shared.board)
            .slots
            .iter()
            .map(IsolatedSlotState::service)
            .collect()
    }

    /// The state of each slot, for the worker snapshot.
    pub fn slot_states(&self) -> Vec<IsolatedSlotState> {
        lock(&self.shared.board).slots.clone()
    }

    pub fn health(&self) -> PoolHealth {
        self.shared.health()
    }

    /// Stop: waiting tasks get `NotDispatched(PoolStopping)`, booting
    /// children are ended, idle children get the stop frame, busy children
    /// get until `drain_timeout`. Then the busy children are killed with
    /// their sessions (their tasks take the crash path), and the slots get
    /// 30 s more before they are aborted.
    pub async fn stop(mut self, drain_timeout: Duration) -> PoolStopReport {
        self.shared.stop.cancel();
        self.shared.answer_waiting(NotRunCause::PoolStopping);
        if join_all(&mut self.slots, drain_timeout).await {
            return PoolStopReport::Drained;
        }
        self.shared.drain_deadline.cancel();
        if join_all(&mut self.slots, AFTER_DRAIN_KILL_WAIT).await {
            return PoolStopReport::KilledAtDrainDeadline;
        }
        self.slots.abort_all();
        let _ = join_all(&mut self.slots, AFTER_DRAIN_KILL_WAIT).await;
        PoolStopReport::AbortedAfterDrain
    }
}

/// Wait up to `bound` for every slot task. A panic of a slot task is a
/// defect and is raised here. Returns whether every task ended.
async fn join_all(slots: &mut JoinSet<()>, bound: Duration) -> bool {
    let all = async {
        while let Some(joined) = slots.join_next().await {
            match joined {
                Ok(()) => {}
                Err(error) => match error.try_into_panic() {
                    Ok(panic) => std::panic::resume_unwind(panic),
                    // A cancelled slot task: the abort after the drain.
                    Err(_cancelled) => {}
                },
            }
        }
    };
    tokio::time::timeout(bound, all).await.is_ok()
}
