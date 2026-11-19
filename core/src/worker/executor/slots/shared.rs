//! The pool of one kind of shared children, async or thread: the FIFO of
//! tasks, the placement of each task on a seat, and the stop.
//!
//! Placement runs when a task is submitted, when a seat frees and when a
//! child opens: the open child with the fewest runs and a free seat takes
//! the next task, the lowest slot among equals. A slot takes a task from
//! the FIFO only when the rule picks it; otherwise it wakes the slot the
//! rule picks.

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::sync::{oneshot, Notify};
use tokio::task::JoinSet;

use super::board::{PoolBoard, PoolHealth, SlotPhase};
use super::budgets::SharedSlotService;
use super::dispatch::{AttemptEnd, AttemptLog, ClaimedTask, ExecutionClass, NotRunCause};
use super::pool::{
    not_run_cause, stop_slots, Answer, DrainKills, ExitNotSeen, MemoryLimit, PlacementWatch,
    PoolStopReport, Submitted, TaskLimit,
};
use super::seat_runs::{place_on_shared_child, OpenChildLoad, Placement, SharedChildKind};
use super::shared_slot::run_shared_slot;
use crate::broker::OrphanSelfHeal;
use crate::events::QueueName;
use crate::worker::executor::placement::ChildCpus;
use crate::worker::executor::protocol::{
    ChildKind, DispatchNumber, InitFrame, SharedRunFrame, SharedWork, TaskInputText,
};
use crate::worker::executor::supervisor::{ChildCommand, ChildSource, SpawnerHandle};
use crate::worker::runner_beats::LiveDispatches;

/// The fixed facts of a shared pool. The number of its slots is on the
/// board; the kind and the seats of its children are in `init`.
#[derive(Debug, Clone)]
pub struct SharedPoolConfig {
    /// The program of each child. The spawner appends the socket descriptor
    /// and the worker pid.
    pub command: ChildCommand,
    /// The first frame of each child; its kind is shared async or shared
    /// thread, with the seats of each child.
    pub init: InitFrame,
    pub worker_id: String,
    /// The orphan self-heal that each run frame carries (the app's
    /// `auto_terminate_orphaned_workflow_tasks`).
    pub orphan_self_heal: OrphanSelfHeal,
    /// Tasks that wait for a seat, at most.
    pub pending_limit: NonZeroUsize,
    /// A child closes when the tasks that ended on it reach this many per
    /// seat.
    pub tasks_per_seat: TaskLimit,
    pub memory_per_child: MemoryLimit,
    pub exit_not_seen: ExitNotSeen,
    /// The CPUs of each child, set before its init frame.
    pub child_cpus: ChildCpus,
    /// The longest wait for the exit of a child after a `SIGKILL`:
    /// [`CHILD_KILL_WAIT`] in a worker.
    ///
    /// [`CHILD_KILL_WAIT`]: crate::worker::executor::supervisor::CHILD_KILL_WAIT
    pub kill_wait: Duration,
}

/// The kind of the shared children of an init frame. An isolated or a
/// handler init frame for a shared pool is a defect of the caller: it
/// crashes loud.
fn shared_kind_of(init: &InitFrame) -> (SharedChildKind, ExecutionClass, NonZeroUsize) {
    let (kind, class, seats) = match init.child_kind {
        ChildKind::SharedAsync(child) => (
            SharedChildKind::Async,
            ExecutionClass::SharedAsync,
            child.seats.get(),
        ),
        ChildKind::SharedThread(child) => (
            SharedChildKind::Thread,
            ExecutionClass::SharedThread,
            child.seats.get(),
        ),
        ChildKind::Isolated(_) => panic!("a shared pool was given an isolated init frame"),
        ChildKind::Handler(_) => panic!("a shared pool was given a handler init frame"),
    };
    let seats = usize::try_from(seats)
        .ok()
        .and_then(NonZeroUsize::new)
        .expect("the seats of a shared child are 1 to 1000 by their type");
    (kind, class, seats)
}

/// One task that waits for a seat. Its run number is set at the submit.
pub(super) struct QueuedSeatRun {
    pub(super) frame: SharedRunFrame,
    pub(super) answer: Answer,
    /// The log fields of the attempt.
    pub(super) log: AttemptLog,
}

/// Whether a slot's child takes new tasks, as placement sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Accepting {
    Open,
    NotOpen,
}

#[derive(Debug, Clone, Copy)]
struct SlotLoad {
    accepting: Accepting,
    runs: usize,
}

/// What a slot got from the FIFO.
pub(super) enum Take {
    Run(QueuedSeatRun),
    /// The rule picks another slot: wake it.
    WakeOther(usize),
    Nothing,
}

/// The FIFO, the load of each slot, and the run numbers, under one lock.
struct Placer {
    queue: VecDeque<QueuedSeatRun>,
    loads: Vec<SlotLoad>,
    last_dispatch: u64,
}

impl Placer {
    fn open_loads(&self, seats: NonZeroUsize) -> Vec<OpenChildLoad> {
        self.loads
            .iter()
            .enumerate()
            .filter(|(_, load)| load.accepting == Accepting::Open)
            .map(|(slot, load)| OpenChildLoad {
                slot,
                runs: load.runs,
                seats,
            })
            .collect()
    }

    fn choose(&self, seats: NonZeroUsize) -> Placement {
        match self.queue.is_empty() {
            true => Placement::NoSeatFree,
            false => place_on_shared_child(&self.open_loads(seats)),
        }
    }

    /// The seats that take a task at once: the free seats of the open
    /// children, less the tasks that wait for a seat.
    fn free_seats(&self, seats: NonZeroUsize) -> usize {
        let open_free: usize = self
            .loads
            .iter()
            .filter(|load| load.accepting == Accepting::Open)
            .map(|load| seats.get().saturating_sub(load.runs))
            .sum();
        open_free.saturating_sub(self.queue.len())
    }

    fn next_dispatch(&mut self) -> DispatchNumber {
        self.last_dispatch = self.last_dispatch.saturating_add(1);
        DispatchNumber::new(self.last_dispatch).expect("run numbers start at 1")
    }
}

/// The state that the pool and its slot tasks share.
pub(super) struct SharedPoolShared {
    pub(super) config: SharedPoolConfig,
    pub(super) kind: SharedChildKind,
    pub(super) class: ExecutionClass,
    pub(super) seats: NonZeroUsize,
    /// Where the children come from: the spawner in a worker.
    pub(super) source: Arc<dyn ChildSource>,
    pub(super) spawner: SpawnerHandle,
    pub(super) live: LiveDispatches,
    pub(super) board: Arc<PoolBoard>,
    /// The board index of this pool's first slot.
    pub(super) first: usize,
    placer: Mutex<Placer>,
    /// One per slot of this pool: placement picked the slot.
    pub(super) wakes: Vec<Notify>,
    pub(super) drain_kills: DrainKills,
}

fn lock(placer: &Mutex<Placer>) -> MutexGuard<'_, Placer> {
    // A poisoned lock means a panic while it was held; the queue and the
    // loads are still valid values.
    placer.lock().unwrap_or_else(PoisonError::into_inner)
}

impl SharedPoolShared {
    /// Publish the load of slot `local`, and wake the slot that placement
    /// picks for the next task.
    pub(super) fn publish_load(&self, local: usize, accepting: Accepting, runs: usize) {
        let chosen = {
            let mut placer = lock(&self.placer);
            placer.loads[local] = SlotLoad { accepting, runs };
            placer.choose(self.seats)
        };
        if let Placement::OnSlot(slot) = chosen {
            self.wakes[slot].notify_one();
        }
    }

    /// Take the next task for slot `local`, when placement picks it.
    pub(super) fn take_for(&self, local: usize) -> Take {
        let mut placer = lock(&self.placer);
        match placer.choose(self.seats) {
            Placement::NoSeatFree => Take::Nothing,
            Placement::OnSlot(slot) if slot != local => Take::WakeOther(slot),
            Placement::OnSlot(_) => match placer.queue.pop_front() {
                Some(queued) => {
                    placer.loads[local].runs += 1;
                    Take::Run(queued)
                }
                None => Take::Nothing,
            },
        }
    }

    fn answer_waiting(&self, cause: NotRunCause) {
        let waiting: Vec<QueuedSeatRun> = lock(&self.placer).queue.drain(..).collect();
        for queued in waiting {
            queued.answer.send(AttemptEnd::NotDispatched(cause));
        }
    }
}

/// Answer every waiting task with `PoolFailed` when the pools fail. Ends at
/// the stop, which answers the waiting tasks itself.
async fn answer_at_failure(shared: Arc<SharedPoolShared>) {
    tokio::select! {
        () = shared.board.failed.cancelled() => shared.answer_waiting(NotRunCause::PoolFailed),
        () = shared.board.stop.cancelled() => {}
    }
}

/// Whether `work` fits a child of `kind`; anything else would end the
/// child as a bad parent frame.
fn work_fits(kind: SharedChildKind, work: &SharedWork) -> bool {
    match (kind, work) {
        (SharedChildKind::Async, SharedWork::AsyncTask { .. })
        | (SharedChildKind::Thread, SharedWork::ThreadTask { .. })
        | (_, SharedWork::Refusal(_)) => true,
        (SharedChildKind::Async, SharedWork::ThreadTask { .. })
        | (SharedChildKind::Thread, SharedWork::AsyncTask { .. }) => false,
    }
}

/// The shared children of one kind of a worker.
pub struct SharedPool {
    shared: Arc<SharedPoolShared>,
    slots: JoinSet<()>,
}

impl SharedPool {
    /// Start one slot task per slot of the pool's class on `board`. Each
    /// boots its child at once.
    pub fn start(
        config: SharedPoolConfig,
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
        config: SharedPoolConfig,
        board: Arc<PoolBoard>,
        spawner: SpawnerHandle,
        source: Arc<dyn ChildSource>,
        live: LiveDispatches,
    ) -> Self {
        Self::start_with(config, board, spawner, source, live)
    }

    fn start_with(
        config: SharedPoolConfig,
        board: Arc<PoolBoard>,
        spawner: SpawnerHandle,
        source: Arc<dyn ChildSource>,
        live: LiveDispatches,
    ) -> Self {
        let (kind, class, seats) = shared_kind_of(&config.init);
        let indices = board.slots_of(class);
        let count = indices.len();
        let shared = Arc::new(SharedPoolShared {
            config,
            kind,
            class,
            seats,
            source,
            spawner,
            live,
            first: indices.start,
            board,
            placer: Mutex::new(Placer {
                queue: VecDeque::new(),
                loads: vec![
                    SlotLoad {
                        accepting: Accepting::NotOpen,
                        runs: 0,
                    };
                    count
                ],
                last_dispatch: 0,
            }),
            wakes: (0..count).map(|_| Notify::new()).collect(),
            drain_kills: DrainKills::default(),
        });
        let mut slots = JoinSet::new();
        for local in 0..count {
            slots.spawn(run_shared_slot(Arc::clone(&shared), local));
        }
        slots.spawn(answer_at_failure(Arc::clone(&shared)));
        Self { shared, slots }
    }

    /// Queue one task for a seat. Its run number is the next of the pool.
    /// The input is dropped once the frame is sent. Work that does not fit
    /// the pool's kind is a defect of the route: it crashes loud.
    pub fn submit(&self, task: &ClaimedTask, input: TaskInputText, work: SharedWork) -> Submitted {
        self.submit_watched(task, input, work, PlacementWatch::Unwatched)
    }

    /// As [`SharedPool::submit`]; `watch` is told when a child takes the
    /// task, with the class of this pool as the kind of the child.
    pub fn submit_watched(
        &self,
        task: &ClaimedTask,
        input: TaskInputText,
        work: SharedWork,
        watch: PlacementWatch,
    ) -> Submitted {
        assert!(
            work_fits(self.shared.kind, &work),
            "task {} was routed to the {} pool with work of another kind",
            task.task_id,
            self.shared.class
        );
        let (sender, receiver) = oneshot::channel();
        let chosen = {
            let mut placer = lock(&self.shared.placer);
            // Read under the placer lock: a failure that fires after this
            // read finds the task in the queue and answers it.
            if let Some(cause) = not_run_cause(self.shared.board.health()) {
                return Submitted::NotDispatched(cause);
            }
            if placer.queue.len() >= self.shared.config.pending_limit.get() {
                return Submitted::NotDispatched(NotRunCause::QueueFull);
            }
            let dispatch = placer.next_dispatch();
            placer.queue.push_back(QueuedSeatRun {
                frame: SharedRunFrame {
                    dispatch,
                    task_id: task.task_id,
                    task_name: task.task_name.clone(),
                    queue_name: QueueName::cut(&task.queue_name),
                    attempt: task.retry.attempt(),
                    worker_id: self.shared.config.worker_id.clone(),
                    claimed_at: task.claimed_at,
                    orphan_self_heal: self.shared.config.orphan_self_heal,
                    work,
                    workflow: task.workflow,
                    trace: task.trace.clone(),
                    input,
                },
                answer: Answer::watched(sender, watch, self.shared.class),
                log: task.log(),
            });
            placer.choose(self.shared.seats)
        };
        if let Placement::OnSlot(slot) = chosen {
            self.shared.wakes[slot].notify_one();
        }
        Submitted::Queued(receiver)
    }

    /// The seats that take a task at once: the free seats of the open
    /// children, less the tasks that wait for a seat. 0 once the pools stop
    /// or failed (Python `SlotPool.free_shared_async_seats`,
    /// `free_shared_thread_seats`).
    pub fn free_seats(&self) -> u32 {
        let placer = lock(&self.shared.placer);
        match self.shared.board.not_serving() {
            true => 0,
            false => u32::try_from(placer.free_seats(self.shared.seats)).unwrap_or(u32::MAX),
        }
    }

    /// The service of each slot with its seats, for the claim budget.
    pub fn service(&self) -> Vec<SharedSlotService> {
        self.slot_states()
            .iter()
            .map(|phase| SharedSlotService {
                service: phase.service(),
                seats: u32::try_from(self.shared.seats.get()).unwrap_or(u32::MAX),
            })
            .collect()
    }

    /// The state of each slot of the pool, for the worker snapshot.
    pub fn slot_states(&self) -> Vec<SlotPhase> {
        self.shared.board.phases_of(self.shared.class)
    }

    pub fn health(&self) -> PoolHealth {
        self.shared.board.health()
    }

    /// Stop: waiting tasks get `NotDispatched(PoolStopping)`; each open
    /// child closes, drains and gets the stop frame; at `drain_timeout` the
    /// children that still run tasks are killed, and each run gets its
    /// verdict; the slots get 30 s more, then are aborted. The report names
    /// the tasks of the children that the deadline killed.
    pub async fn stop(mut self, drain_timeout: Duration) -> PoolStopReport {
        self.shared.board.begin_stop();
        self.shared.answer_waiting(NotRunCause::PoolStopping);
        stop_slots(
            &mut self.slots,
            &self.shared.board,
            &self.shared.drain_kills,
            drain_timeout,
        )
        .await
    }
}
