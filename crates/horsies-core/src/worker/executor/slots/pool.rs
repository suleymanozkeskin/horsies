//! The pool of isolated children: the FIFO of tasks, the answer of each
//! task, and the stop (Python `core/worker/slots.py`, the isolated half of
//! `SlotPool`). The state of each slot, the failure and the stop live on
//! the [`PoolBoard`], which every pool of the worker shares.

use std::collections::VecDeque;
use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::sync::{oneshot, Notify};
use tokio::task::JoinSet;

use super::board::{PoolBoard, PoolHealth, SlotPhase};
use super::budgets::SlotService;
use super::dispatch::{AttemptEnd, ClaimedTask, ExecutionClass, NotRunCause};
use super::isolated::run_slot;
use super::route::IsolatedDeadline;
use crate::worker::executor::placement::ChildCpus;
use crate::worker::executor::protocol::{
    InitFrame, IsolatedWork, ProcessId, RunFrame, TaskInputText,
};
use crate::worker::executor::supervisor::{
    ChildCommand, ChildSource, SpawnerHandle, CHILD_KILL_WAIT,
};
use crate::worker::runner_beats::LiveDispatches;

/// After the drain deadline kills the busy children, the slot tasks get
/// this long more before they are aborted (Python: 30 s).
pub(super) const AFTER_DRAIN_KILL_WAIT: Duration = CHILD_KILL_WAIT;

/// Recycle a child after this many tasks (per seat for a shared child).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskLimit {
    Off,
    After(NonZeroU64),
}

/// Recycle a child when its reported RSS reaches this many MiB. An
/// unreadable RSS never recycles.
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

/// The fixed facts of an isolated pool. The number of its slots is on the
/// board.
#[derive(Debug, Clone)]
pub struct IsolatedPoolConfig {
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
    /// The CPUs of each child, set before its init frame.
    pub child_cpus: ChildCpus,
    /// The longest wait for the exit of a child after a `SIGKILL`:
    /// [`CHILD_KILL_WAIT`] in a worker.
    pub kill_wait: Duration,
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

/// The state that the pool and its slot tasks share.
pub(super) struct PoolShared {
    pub(super) config: IsolatedPoolConfig,
    /// Where the children come from: the spawner in a worker.
    pub(super) source: Arc<dyn ChildSource>,
    pub(super) spawner: SpawnerHandle,
    pub(super) live: LiveDispatches,
    pub(super) board: Arc<PoolBoard>,
    queue: Mutex<VecDeque<QueuedRun>>,
    pub(super) queued: Notify,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // A poisoned lock means a panic while it was held; the queue is still a
    // valid value.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl PoolShared {
    pub(super) fn pop(&self) -> Option<QueuedRun> {
        lock(&self.queue).pop_front()
    }

    fn answer_waiting(&self, cause: NotRunCause) {
        let waiting: Vec<QueuedRun> = lock(&self.queue).drain(..).collect();
        for run in waiting {
            run.answer.send(AttemptEnd::NotDispatched(cause));
        }
    }
}

/// Answer every waiting task with `PoolFailed` when the pools fail. Ends at
/// the stop, which answers the waiting tasks itself.
async fn answer_at_failure(shared: Arc<PoolShared>) {
    tokio::select! {
        () = shared.board.failed.cancelled() => shared.answer_waiting(NotRunCause::PoolFailed),
        () = shared.board.stop.cancelled() => {}
    }
}

/// The task never reaches a child when the pools stop or failed.
pub(super) fn not_run_cause(health: PoolHealth) -> Option<NotRunCause> {
    match health {
        PoolHealth::Serving => None,
        PoolHealth::Failed(_) => Some(NotRunCause::PoolFailed),
        PoolHealth::Stopping => Some(NotRunCause::PoolStopping),
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
    /// Start one slot task per isolated slot of `board` on the current
    /// runtime. Each boots its child at once.
    pub fn start(
        config: IsolatedPoolConfig,
        board: Arc<PoolBoard>,
        spawner: SpawnerHandle,
        live: LiveDispatches,
    ) -> Self {
        let source: Arc<dyn ChildSource> = Arc::new(spawner.clone());
        Self::start_with(config, board, spawner, source, live)
    }

    /// Start the pool with its children from `source`, a source over
    /// `spawner` that a test builds.
    #[cfg(test)]
    pub(super) fn start_from_source(
        config: IsolatedPoolConfig,
        board: Arc<PoolBoard>,
        spawner: SpawnerHandle,
        source: Arc<dyn ChildSource>,
        live: LiveDispatches,
    ) -> Self {
        Self::start_with(config, board, spawner, source, live)
    }

    fn start_with(
        config: IsolatedPoolConfig,
        board: Arc<PoolBoard>,
        spawner: SpawnerHandle,
        source: Arc<dyn ChildSource>,
        live: LiveDispatches,
    ) -> Self {
        let indices: Range<usize> = board.slots_of(ExecutionClass::Isolated);
        let shared = Arc::new(PoolShared {
            config,
            source,
            spawner,
            live,
            board,
            queue: Mutex::new(VecDeque::new()),
            queued: Notify::new(),
        });
        let mut slots = JoinSet::new();
        for index in indices {
            slots.spawn(run_slot(Arc::clone(&shared), index));
        }
        slots.spawn(answer_at_failure(Arc::clone(&shared)));
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
        let (sender, receiver) = oneshot::channel();
        {
            let mut queue = lock(&self.shared.queue);
            // Read under the queue lock: a failure that fires after this
            // read finds the task in the queue and answers it.
            if let Some(cause) = not_run_cause(self.shared.board.health()) {
                return Submitted::NotDispatched(cause);
            }
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

    /// The service state of each isolated slot, for the claim budget.
    pub fn service(&self) -> Vec<SlotService> {
        self.slot_states().iter().map(SlotPhase::service).collect()
    }

    /// The state of each isolated slot, for the worker snapshot.
    pub fn slot_states(&self) -> Vec<SlotPhase> {
        self.shared.board.phases_of(ExecutionClass::Isolated)
    }

    pub fn health(&self) -> PoolHealth {
        self.shared.board.health()
    }

    /// Stop: waiting tasks get `NotDispatched(PoolStopping)`, booting
    /// children are ended, idle children get the stop frame, busy children
    /// get until `drain_timeout`. Then the busy children are killed with
    /// their sessions (their tasks take the crash path), and the slots get
    /// 30 s more before they are aborted. The stop and the drain deadline
    /// are those of the board, so every pool of the worker stops with them.
    pub async fn stop(mut self, drain_timeout: Duration) -> PoolStopReport {
        self.shared.board.begin_stop();
        self.shared.answer_waiting(NotRunCause::PoolStopping);
        stop_slots(&mut self.slots, &self.shared.board, drain_timeout).await
    }
}

/// Wait for the slot tasks of a pool to end within the drain timeout; then
/// pass the drain deadline, wait 30 s more, and abort what is left.
pub(super) async fn stop_slots(
    slots: &mut JoinSet<()>,
    board: &PoolBoard,
    drain_timeout: Duration,
) -> PoolStopReport {
    if join_all(slots, drain_timeout).await {
        return PoolStopReport::Drained;
    }
    board.pass_drain_deadline();
    if join_all(slots, AFTER_DRAIN_KILL_WAIT).await {
        return PoolStopReport::KilledAtDrainDeadline;
    }
    slots.abort_all();
    let _ = join_all(slots, AFTER_DRAIN_KILL_WAIT).await;
    PoolStopReport::AbortedAfterDrain
}

/// Wait up to `bound` for every slot task. A panic of a slot task is a
/// defect and is raised here. Returns whether every task ended.
async fn join_all(slots: &mut JoinSet<()>, bound: Duration) -> bool {
    let all = async {
        while let Some(joined) = slots.join_next().await {
            // A cancelled slot task is the abort after the drain; only a
            // panic is raised.
            if let Err(error) = joined {
                if let Ok(panic) = error.try_into_panic() {
                    std::panic::resume_unwind(panic);
                }
            }
        }
    };
    tokio::time::timeout(bound, all).await.is_ok()
}
