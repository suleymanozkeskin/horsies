//! The children of a worker: one pool of each kind of child that the
//! worker's profile names, on one board, started through the spawner thread
//! (Python `core/worker/worker.py`, `_slot_pool_config`,
//! `_pending_task_limit`, `_start_slots`, `_claim_capacity`, `_down_slots`;
//! `core/worker/child_pool.py`, the init frame of each child kind).
//!
//! - [`ChildLaunch`]: what every child runs, from the host.
//! - [`WorkerChildren::start`]: the pools of the profile, booting at once.
//! - [`WorkerChildren::submit`]: the route of a claimed row and its submit
//!   to the pool of its kind.
//! - [`WorkerChildren::service`]: the capacity in service of each budget, for
//!   the claim; [`WorkerChildren::down_slots`] for the worker snapshot.
//! - [`WorkerChildren::stop`]: every pool stops with the drain timeout.
//!
//! The pools never run user code in the worker process: every task runs in a
//! child.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;

use crate::core::config::recovery::RecoveryConfig;
use crate::core::config::worker_profile::{
    IsolatedSetting, MemoryLimitSetting, ParentCpu, SharedAsyncSetting, SharedThreadSetting,
    TaskLimitSetting, WorkerProfile,
};
use crate::worker::executor::placement::{plan_own_core, CpuPlacement, PlacementRefused};
use crate::worker::executor::protocol::{
    AsyncSeats, ChildKind, ChildLogLevel, ChildPool, ChildProgram, DatabaseUrl, ExecutorThreads,
    InitFrame, IsolatedChild, PoolMaxSize, PoolMinSize, SharedAsyncChild, SharedThreadChild,
    TaskSendPool, ThreadSeats,
};
use crate::worker::executor::slots::{
    dispatch_route, AttemptEnd, BaselineLimits, ChildLayout, Claimed, DispatchRoute, DownState,
    ExecutionClass, ExitNotSeen, IsolatedPool, IsolatedPoolConfig, MemoryLimit, NotRunCause,
    PoolBoard, PoolService, PoolStopReport, SharedPool, SharedPoolConfig, SharedSlotService,
    SlotPhase, Submitted, TaskLimit,
};
use crate::worker::executor::supervisor::{ChildCommand, SpawnerHandle, CHILD_KILL_WAIT};
use crate::worker::runner_beats::{LiveDispatches, StaleRunningRecovery};

/// The process name of every child (Python `process_name='horsies-child'`).
pub const CHILD_PROCESS_NAME: &str = "horsies-child";

/// What every child of the worker runs, from the host: the program to
/// start, what it loads, its log level and its database URL.
#[derive(Debug, Clone)]
pub struct ChildLaunch {
    /// The program of each child. The spawner appends the socket descriptor
    /// and the worker pid.
    pub command: ChildCommand,
    /// The Python app to import, or the Rust tasks of the worker binary.
    pub program: ChildProgram,
    pub log_level: ChildLogLevel,
    /// The URL that a child connects with.
    pub database_url: DatabaseUrl,
}

/// The most tasks that may wait for a slot or a seat in one pool: twice the
/// claim budget of the worker (Python `_pending_task_limit`,
/// `horsies/core/worker/worker.py:529-536`): `2 * max(seats +
/// prefetch_buffer, claimed_per_worker)`, with no claimed limit as 0.
pub fn pending_task_limit(profile: &WorkerProfile, prefetch_buffer: u32) -> NonZeroUsize {
    let claimed_per_worker = match profile.claim.claimed_per_worker {
        TaskLimitSetting::TaskLimit { tasks } => u64::from(tasks.get()),
        TaskLimitSetting::NoTaskLimit {} => 0,
    };
    let budget =
        (u64::from(profile.seat_count()) + u64::from(prefetch_buffer)).max(claimed_per_worker);
    let limit = usize::try_from(budget.saturating_mul(2)).unwrap_or(usize::MAX);
    match NonZeroUsize::new(limit) {
        Some(limit) => limit,
        // AppConfig validation refuses a profile with no child, so every
        // validated profile has a seat.
        None => panic!("a validated worker profile has at least one seat"),
    }
}

/// The children of each kind of the profile, on the board.
pub fn child_layout(profile: &WorkerProfile) -> ChildLayout {
    let count = |children: u32| usize::try_from(children).unwrap_or(usize::MAX);
    ChildLayout {
        isolated: count(profile.isolated.child_count()),
        shared_async: count(profile.shared_async.child_count()),
        shared_thread: count(profile.shared_thread.child_count()),
    }
}

/// The recycle limit of a task count.
fn task_limit(setting: TaskLimitSetting) -> TaskLimit {
    match setting {
        TaskLimitSetting::TaskLimit { tasks } => {
            TaskLimit::After(NonZeroU64::from(std::num::NonZeroU32::from(tasks)))
        }
        TaskLimitSetting::NoTaskLimit {} => TaskLimit::Off,
    }
}

/// The recycle limit of the RSS of a child.
fn memory_limit(setting: MemoryLimitSetting) -> MemoryLimit {
    match setting {
        MemoryLimitSetting::MemoryLimit { mb } => {
            MemoryLimit::AtMb(NonZeroU64::from(std::num::NonZeroU32::from(mb)))
        }
        MemoryLimitSetting::NoMemoryLimit {} => MemoryLimit::Off,
    }
}

/// The memory limit of each kind, for the baseline guard (Python
/// `_check_memory_baseline`).
pub fn baseline_limits(profile: &WorkerProfile) -> BaselineLimits {
    BaselineLimits {
        isolated: match profile.isolated {
            IsolatedSetting::IsolatedChildren(children) => memory_limit(children.recycle.above),
            IsolatedSetting::NoIsolatedChildren {} => MemoryLimit::Off,
        },
        shared_async: match profile.shared_async {
            SharedAsyncSetting::SharedAsyncChildren(children) => {
                memory_limit(children.recycle.above)
            }
            SharedAsyncSetting::NoSharedAsyncChildren {} => MemoryLimit::Off,
        },
        shared_thread: match profile.shared_thread {
            SharedThreadSetting::SharedThreadChildren(children) => {
                memory_limit(children.recycle.above)
            }
            SharedThreadSetting::NoSharedThreadChildren {} => MemoryLimit::Off,
        },
    }
}

/// What a slot does when a killed child's exit is not seen: stale RUNNING
/// recovery decides the row when it is on (Python
/// `stale_running_recovery`).
pub fn exit_not_seen(recovery: &RecoveryConfig) -> ExitNotSeen {
    match StaleRunningRecovery::from_config(recovery) {
        StaleRunningRecovery::On { .. } => ExitNotSeen::LeaveToRecovery,
        StaleRunningRecovery::Off => ExitNotSeen::WaitForExit,
    }
}

/// The CPUs of the worker and its children (Python `plan_cpu_placement`,
/// `horsies/core/worker/worker.py:614-618`). `OWN_CORE` that the machine
/// cannot give fails the start before any child starts.
pub fn plan_placement(parent_cpu: ParentCpu) -> Result<CpuPlacement, PlacementRefused> {
    match parent_cpu {
        ParentCpu::Shared => Ok(CpuPlacement::SharedCpus),
        ParentCpu::OwnCore => plan_own_core().map(CpuPlacement::OwnCore),
    }
}

/// A count of the profile in the protocol's bound type. Both carry the same
/// bounds, so a value outside the protocol's is a defect.
fn protocol_count<T>(value: u32, convert: fn(u64) -> Option<T>, field: &str) -> T {
    match convert(u64::from(value)) {
        Some(converted) => converted,
        None => panic!("the profile's {field} {value} is outside the frame's bounds"),
    }
}

fn child_pool(pool: crate::core::config::worker_profile::ResolvedPool) -> ChildPool {
    ChildPool {
        min_size: protocol_count(pool.min_size().get(), PoolMinSize::new, "pool min_size"),
        max_size: protocol_count(pool.max_size().get(), PoolMaxSize::new, "pool max_size"),
        check: pool.check(),
    }
}

fn resolved_pool(
    resolved: Result<
        crate::core::config::worker_profile::ResolvedPool,
        crate::core::config::worker_profile::PoolMinAboveMax,
    >,
) -> ChildPool {
    match resolved {
        Ok(pool) => child_pool(pool),
        // AppConfig validation refuses a pool whose min_size is above its
        // resolved max_size.
        Err(refused) => panic!(
            "a validated worker profile has a pool with min_size {} above max_size {}",
            refused.min_size.get(),
            refused.max_size.get()
        ),
    }
}

fn init_frame(launch: &ChildLaunch, child_kind: ChildKind) -> InitFrame {
    InitFrame {
        program: launch.program.clone(),
        log_level: launch.log_level,
        database_url: launch.database_url.clone(),
        process_name: CHILD_PROCESS_NAME.to_owned(),
        child_kind,
    }
}

/// The fixed facts that every pool of the worker shares.
#[derive(Debug, Clone)]
pub struct PoolFacts {
    pub worker_id: String,
    pub pending_limit: NonZeroUsize,
    pub exit_not_seen: ExitNotSeen,
    pub placement: CpuPlacement,
}

/// The config of the isolated pool of the profile, when it has isolated
/// children (Python `_slot_pool_config`, `child_kind=IsolatedChild(...)`).
pub fn isolated_pool_config(
    profile: &WorkerProfile,
    launch: &ChildLaunch,
    facts: &PoolFacts,
) -> Option<IsolatedPoolConfig> {
    let IsolatedSetting::IsolatedChildren(children) = profile.isolated else {
        return None;
    };
    let kind = ChildKind::Isolated(IsolatedChild {
        pool: resolved_pool(children.resolved_pool()),
        task_send_pool: protocol_count(
            children.task_send_pool.connections.get(),
            TaskSendPool::new,
            "isolated task_send_pool",
        ),
    });
    Some(IsolatedPoolConfig {
        command: launch.command.clone(),
        init: init_frame(launch, kind),
        worker_id: facts.worker_id.clone(),
        pending_limit: facts.pending_limit,
        tasks_per_child: task_limit(children.recycle.after),
        memory_per_child: memory_limit(children.recycle.above),
        exit_not_seen: facts.exit_not_seen,
        child_cpus: facts.placement.child_cpus(),
        kill_wait: CHILD_KILL_WAIT,
    })
}

/// The config of the shared async pool of the profile, when it has shared
/// async children.
pub fn shared_async_pool_config(
    profile: &WorkerProfile,
    launch: &ChildLaunch,
    facts: &PoolFacts,
) -> Option<SharedPoolConfig> {
    let SharedAsyncSetting::SharedAsyncChildren(children) = profile.shared_async else {
        return None;
    };
    let kind = ChildKind::SharedAsync(SharedAsyncChild {
        seats: protocol_count(children.seats.get(), AsyncSeats::new, "shared async seats"),
        pool: resolved_pool(children.resolved_pool()),
        executor_threads: protocol_count(
            children.resolved_executor_threads().get(),
            ExecutorThreads::new,
            "shared async executor threads",
        ),
        task_send_pool: protocol_count(
            children.task_send_pool.connections.get(),
            TaskSendPool::new,
            "shared async task_send_pool",
        ),
    });
    Some(SharedPoolConfig {
        command: launch.command.clone(),
        init: init_frame(launch, kind),
        worker_id: facts.worker_id.clone(),
        pending_limit: facts.pending_limit,
        tasks_per_seat: task_limit(children.recycle.after_per_seat),
        memory_per_child: memory_limit(children.recycle.above),
        exit_not_seen: facts.exit_not_seen,
        child_cpus: facts.placement.child_cpus(),
        kill_wait: CHILD_KILL_WAIT,
    })
}

/// The config of the shared thread pool of the profile, when it has shared
/// thread children.
pub fn shared_thread_pool_config(
    profile: &WorkerProfile,
    launch: &ChildLaunch,
    facts: &PoolFacts,
) -> Option<SharedPoolConfig> {
    let SharedThreadSetting::SharedThreadChildren(children) = profile.shared_thread else {
        return None;
    };
    let kind = ChildKind::SharedThread(SharedThreadChild {
        seats: protocol_count(
            children.seats.get(),
            ThreadSeats::new,
            "shared thread seats",
        ),
        pool: resolved_pool(children.resolved_pool()),
        task_send_pool: protocol_count(
            children.task_send_pool.connections.get(),
            TaskSendPool::new,
            "shared thread task_send_pool",
        ),
    });
    Some(SharedPoolConfig {
        command: launch.command.clone(),
        init: init_frame(launch, kind),
        worker_id: facts.worker_id.clone(),
        pending_limit: facts.pending_limit,
        tasks_per_seat: task_limit(children.recycle.after_per_seat),
        memory_per_child: memory_limit(children.recycle.above),
        exit_not_seen: facts.exit_not_seen,
        child_cpus: facts.placement.child_cpus(),
        kill_wait: CHILD_KILL_WAIT,
    })
}

/// The seats of one child of each shared kind (0 for a kind with no
/// children).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeatsPerChild {
    pub shared_async: u32,
    pub shared_thread: u32,
}

impl SeatsPerChild {
    /// The seats of each shared kind of `profile`.
    pub fn of_profile(profile: &WorkerProfile) -> Self {
        Self {
            shared_async: match profile.shared_async {
                SharedAsyncSetting::SharedAsyncChildren(children) => children.seats.get(),
                SharedAsyncSetting::NoSharedAsyncChildren {} => 0,
            },
            shared_thread: match profile.shared_thread {
                SharedThreadSetting::SharedThreadChildren(children) => children.seats.get(),
                SharedThreadSetting::NoSharedThreadChildren {} => 0,
            },
        }
    }
}

/// A read view of the slots of a worker: its board and the seats of each
/// shared child. The claim loop and the snapshot loop read it; clones share
/// the board.
#[derive(Clone)]
pub struct ChildrenView {
    board: Arc<PoolBoard>,
    seats: SeatsPerChild,
}

impl ChildrenView {
    pub fn new(board: Arc<PoolBoard>, seats: SeatsPerChild) -> Self {
        Self { board, seats }
    }

    /// The service state of every slot, for the budgets of a claim pass
    /// (Python `_claim_capacity`): an isolated slot counts unless it is down;
    /// a shared slot counts its seats while it boots, serves or retires, the
    /// runs of a closing child, nothing when down.
    pub fn service(&self) -> PoolService {
        let shared = |class: ExecutionClass, seats: u32| -> Vec<SharedSlotService> {
            self.board
                .phases_of(class)
                .iter()
                .map(|phase| SharedSlotService {
                    service: phase.service(),
                    seats,
                })
                .collect()
        };
        PoolService {
            isolated: self
                .board
                .phases_of(ExecutionClass::Isolated)
                .iter()
                .map(SlotPhase::service)
                .collect(),
            shared_async: shared(ExecutionClass::SharedAsync, self.seats.shared_async),
            shared_thread: shared(ExecutionClass::SharedThread, self.seats.shared_thread),
        }
    }

    /// Every slot that is down, with its board index (Python `_down_slots`).
    pub fn down_slots(&self) -> Vec<(usize, DownState)> {
        self.board.down_slots()
    }
}

/// What the stop of the pools did: the worst report of any pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChildrenStopped {
    pub report: PoolStopReport,
}

/// The pools of a worker on one board.
pub struct WorkerChildren {
    board: Arc<PoolBoard>,
    view: ChildrenView,
    isolated: Option<IsolatedPool>,
    shared_async: Option<SharedPool>,
    shared_thread: Option<SharedPool>,
}

impl WorkerChildren {
    /// Start one pool per kind of child of the profile on a new board, on the
    /// current runtime. Each slot boots its child at once; the caller waits
    /// for the first generation with `start_children` on [`Self::board`].
    pub fn start(
        profile: &WorkerProfile,
        launch: &ChildLaunch,
        facts: &PoolFacts,
        spawner: &SpawnerHandle,
        live: &LiveDispatches,
    ) -> Self {
        let board = Arc::new(PoolBoard::new(child_layout(profile)));
        let isolated = isolated_pool_config(profile, launch, facts).map(|config| {
            IsolatedPool::start(config, Arc::clone(&board), spawner.clone(), live.clone())
        });
        let shared_async = shared_async_pool_config(profile, launch, facts).map(|config| {
            SharedPool::start(config, Arc::clone(&board), spawner.clone(), live.clone())
        });
        let shared_thread = shared_thread_pool_config(profile, launch, facts).map(|config| {
            SharedPool::start(config, Arc::clone(&board), spawner.clone(), live.clone())
        });
        Self {
            view: ChildrenView::new(Arc::clone(&board), SeatsPerChild::of_profile(profile)),
            board,
            isolated,
            shared_async,
            shared_thread,
        }
    }

    /// A read view of the slots, for the claim and the snapshot.
    pub fn view(&self) -> ChildrenView {
        self.view.clone()
    }

    /// The board of every slot: the first generation, the pool failure and
    /// the stop.
    pub fn board(&self) -> &Arc<PoolBoard> {
        &self.board
    }

    /// The service state of every slot, for the budgets of a claim pass.
    pub fn service(&self) -> PoolService {
        self.view.service()
    }

    /// Every slot that is down, with its board index, for the snapshot.
    pub fn down_slots(&self) -> Vec<(usize, DownState)> {
        self.view.down_slots()
    }

    /// Route one claimed row (Python `_dispatch_one`) and submit it to the
    /// pool of its kind. The end of the attempt comes on the receiver of
    /// `Submitted::Queued`, exactly once. A row of a class this worker does
    /// not serve, or a route to a kind with no pool, never reaches a child:
    /// `NotDispatched(NoChildOfItsKind)`; its claim goes back to the queue.
    pub fn submit(&self, claimed: Claimed) -> Submitted {
        let Claimed { task, input } = claimed;
        let route = match dispatch_route(task.execution, self.service().plan(), &task.options) {
            Ok(route) => route,
            Err(unserved) => {
                tracing::error!(task_id = %task.task_id, "{unserved}; the row goes back to the queue");
                return Submitted::NotDispatched(NotRunCause::NoChildOfItsKind);
            }
        };
        match (
            route,
            &self.isolated,
            &self.shared_async,
            &self.shared_thread,
        ) {
            (DispatchRoute::Isolated { work, deadline }, Some(pool), _, _) => {
                pool.submit(&task, input, work, deadline)
            }
            (DispatchRoute::SharedAsync(work), _, Some(pool), _) => pool.submit(&task, input, work),
            (DispatchRoute::SharedThread(work), _, _, Some(pool)) => {
                pool.submit(&task, input, work)
            }
            (
                DispatchRoute::Isolated { .. }
                | DispatchRoute::SharedAsync(_)
                | DispatchRoute::SharedThread(_),
                _,
                _,
                _,
            ) => Submitted::NotDispatched(NotRunCause::NoChildOfItsKind),
        }
    }

    /// Stop every pool with `drain_timeout` (Python `SlotPool.stop`): no new
    /// task; waiting tasks end `NotDispatched(PoolStopping)`; running tasks
    /// get until the drain timeout, then their children are killed. The
    /// pools stop together, on the one drain deadline of the board.
    pub async fn stop(self, drain_timeout: Duration) -> ChildrenStopped {
        let WorkerChildren {
            board,
            view: _,
            isolated,
            shared_async,
            shared_thread,
        } = self;
        board.begin_stop();
        let isolated = async move {
            match isolated {
                Some(pool) => pool.stop(drain_timeout).await,
                None => PoolStopReport::Drained,
            }
        };
        let shared_async = async move {
            match shared_async {
                Some(pool) => pool.stop(drain_timeout).await,
                None => PoolStopReport::Drained,
            }
        };
        let shared_thread = async move {
            match shared_thread {
                Some(pool) => pool.stop(drain_timeout).await,
                None => PoolStopReport::Drained,
            }
        };
        let (isolated, shared_async, shared_thread) =
            tokio::join!(isolated, shared_async, shared_thread);
        ChildrenStopped {
            report: [isolated, shared_async, shared_thread]
                .into_iter()
                .max_by_key(|report| stop_rank(*report))
                .unwrap_or(PoolStopReport::Drained),
        }
    }
}

/// The order of the stop reports, worst last.
fn stop_rank(report: PoolStopReport) -> u8 {
    match report {
        PoolStopReport::Drained => 0,
        PoolStopReport::KilledAtDrainDeadline => 1,
        PoolStopReport::AbortedAfterDrain => 2,
    }
}

/// The end of one submitted attempt: the end the pool sent, or at once for
/// a task that never reached a child.
pub async fn attempt_end(submitted: Submitted) -> AttemptEnd {
    match submitted {
        Submitted::NotDispatched(cause) => AttemptEnd::NotDispatched(cause),
        Submitted::Queued(receiver) => receive_end(receiver).await,
    }
}

/// Every answer sends one end, from its drop when the slot ends without one,
/// so a closed channel with no end is a defect.
async fn receive_end(receiver: oneshot::Receiver<AttemptEnd>) -> AttemptEnd {
    match receiver.await {
        Ok(end) => end,
        Err(closed) => panic!("a submitted task got no end from its pool: {closed}"),
    }
}

#[cfg(test)]
mod tests;
