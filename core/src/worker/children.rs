//! The children of a worker: one pool of each kind of child that the
//! worker's profile names, on one board, started through the spawner thread.
//!
//! - [`ChildLaunch`]: what every child runs, from the host.
//! - [`preload_plan`]: the preload process of the profile, when its children
//!   fork from one.
//! - [`WorkerChildren::start`]: the pools of the profile, booting at once.
//! - [`WorkerChildren::submit`]: the route of a claimed row and its submit
//!   to the pool of its kind.
//! - [`WorkerChildren::service`]: the capacity in service of each budget, for
//!   the claim; [`WorkerChildren::down_slots`] for the worker snapshot.
//! - [`WorkerChildren::free_capacity`]: the children or seats of each budget
//!   that take a task at once, for the soft-cap start check.
//! - [`WorkerChildren::stop`]: every pool stops with the drain timeout.
//!
//! The pools never run user code in the worker process: every task runs in a
//! child.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use uuid::Uuid;

use crate::broker::OrphanSelfHeal;
use crate::core::config::recovery::RecoveryConfig;
use crate::core::config::worker_profile::{
    ChildPreload, IsolatedSetting, MemoryLimitSetting, ParentCpu, SharedAsyncSetting,
    SharedThreadSetting, TaskLimitSetting, WorkerProfile,
};
use crate::telemetry::worker::WorkerTelemetry;
use crate::worker::executor::placement::{plan_own_core, CpuPlacement, PlacementRefused};
use crate::worker::executor::protocol::{
    AsyncSeats, ChildConnection, ChildKind, ChildLogLevel, ChildPool, ChildProgram, DatabaseUrl,
    ExecutorThreads, InitFrame, IsolatedChild, PoolMaxSize, PoolMinSize, SharedAsyncChild,
    SharedThreadChild, TaskSendPool, ThreadSeats,
};
use crate::worker::executor::slots::{
    dispatch_route, AttemptEnd, BaselineLimits, ChildLayout, Claimed, DispatchRoute, DownState,
    ExecutionClass, ExitNotSeen, FreeCapacity, IsolatedPool, IsolatedPoolConfig, MemoryLimit,
    NotRunCause, PlacementWatch, PoolBoard, PoolService, PoolStopReport, SharedPool,
    SharedPoolConfig, SharedSlotService, SlotPhase, Submitted, TaskLimit,
};
use crate::worker::executor::supervisor::{
    ChildCommand, PreloadMode, PreloadPlan, SpawnerHandle, CHILD_KILL_WAIT,
};
use crate::worker::runner_beats::{LiveDispatches, StaleRunningRecovery};

/// The process name of every child (Python `process_name='horsies-child'`).
pub const CHILD_PROCESS_NAME: &str = "horsies-child";

/// What every child of the worker runs, from the host: the program to
/// start, what it loads, its log level, its database URL and the connection
/// settings of the app's config.
#[derive(Debug, Clone)]
pub struct ChildLaunch {
    /// The program of each child. The spawner appends the socket descriptor
    /// and the worker pid.
    pub command: ChildCommand,
    /// How the preload process starts, for a profile whose children fork
    /// from one.
    pub preload: PreloadProgram,
    /// The Python app to import, or the Rust tasks of the worker binary.
    pub program: ChildProgram,
    pub log_level: ChildLogLevel,
    /// The URL that a child connects with.
    pub database_url: DatabaseUrl,
    /// The settings of each connection of a child
    /// ([`ChildConnection::of_config`] of the app's broker config; Python
    /// `WorkerConfig.child_connect_kwargs`).
    pub connection: ChildConnection,
}

/// How the preload process of the children starts (Linux).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreloadProgram {
    /// The program and its first arguments. The spawner appends the
    /// descriptor of the preload's socket, the worker pid and the mode.
    Command(ChildCommand),
    /// The child program has no preload process (a Rust child): every child
    /// starts with `exec`.
    NoPreloadProgram,
}

/// The preload process of `profile`: `None` when the profile asks for none
/// or the child program has none. Its init frame is the init frame of the
/// first kind of child of the profile, in the order isolated, shared async,
/// shared thread (Python sends the isolated init frame). Pure.
pub fn preload_plan(profile: &WorkerProfile, launch: &ChildLaunch) -> Option<PreloadPlan> {
    let mode = match profile.preload {
        ChildPreload::Library => PreloadMode::Library,
        ChildPreload::App => PreloadMode::App,
        ChildPreload::NoPreload => return None,
    };
    let command = match &launch.preload {
        PreloadProgram::Command(command) => command.clone(),
        PreloadProgram::NoPreloadProgram => return None,
    };
    let child_kind = isolated_kind(profile)
        .or_else(|| shared_async_kind(profile))
        .or_else(|| shared_thread_kind(profile))?;
    Some(PreloadPlan {
        command,
        mode,
        init: init_frame(launch, child_kind),
    })
}

/// The most tasks that may wait for a slot or a seat in one pool: twice the
/// claim budget of the worker: `2 * max(seats +
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

/// The CPUs of the worker and its children. `OWN_CORE` that the machine
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
        connection: launch.connection.clone(),
        process_name: CHILD_PROCESS_NAME.to_owned(),
        child_kind,
    }
}

/// The fixed facts that every pool of the worker shares.
#[derive(Debug, Clone)]
pub struct PoolFacts {
    pub worker_id: String,
    /// The orphan self-heal of the app's recovery config, for each run frame.
    pub orphan_self_heal: OrphanSelfHeal,
    pub pending_limit: NonZeroUsize,
    pub exit_not_seen: ExitNotSeen,
    pub placement: CpuPlacement,
    /// Where the boots of the slots are told.
    pub telemetry: WorkerTelemetry,
}

/// The child kind of the isolated children of the profile, when it has
/// some.
fn isolated_kind(profile: &WorkerProfile) -> Option<ChildKind> {
    let IsolatedSetting::IsolatedChildren(children) = profile.isolated else {
        return None;
    };
    Some(ChildKind::Isolated(IsolatedChild {
        pool: resolved_pool(children.resolved_pool()),
        task_send_pool: protocol_count(
            children.task_send_pool.connections.get(),
            TaskSendPool::new,
            "isolated task_send_pool",
        ),
    }))
}

/// The child kind of the shared async children of the profile, when it has
/// some.
fn shared_async_kind(profile: &WorkerProfile) -> Option<ChildKind> {
    let SharedAsyncSetting::SharedAsyncChildren(children) = profile.shared_async else {
        return None;
    };
    Some(ChildKind::SharedAsync(SharedAsyncChild {
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
    }))
}

/// The child kind of the shared thread children of the profile, when it has
/// some.
fn shared_thread_kind(profile: &WorkerProfile) -> Option<ChildKind> {
    let SharedThreadSetting::SharedThreadChildren(children) = profile.shared_thread else {
        return None;
    };
    Some(ChildKind::SharedThread(SharedThreadChild {
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
    }))
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
    let kind = isolated_kind(profile)?;
    Some(IsolatedPoolConfig {
        command: launch.command.clone(),
        init: init_frame(launch, kind),
        worker_id: facts.worker_id.clone(),
        orphan_self_heal: facts.orphan_self_heal,
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
    let kind = shared_async_kind(profile)?;
    Some(SharedPoolConfig {
        command: launch.command.clone(),
        init: init_frame(launch, kind),
        worker_id: facts.worker_id.clone(),
        orphan_self_heal: facts.orphan_self_heal,
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
    let kind = shared_thread_kind(profile)?;
    Some(SharedPoolConfig {
        command: launch.command.clone(),
        init: init_frame(launch, kind),
        worker_id: facts.worker_id.clone(),
        orphan_self_heal: facts.orphan_self_heal,
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

/// What the stop of the pools did: the worst report of any pool, with the
/// tasks that the drain deadline killed in every pool.
#[derive(Debug, Clone, PartialEq, Eq)]
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
        let board =
            Arc::new(PoolBoard::new(child_layout(profile)).with_telemetry(facts.telemetry.clone()));
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

    /// The children or seats of each budget that take a task at once (Python
    /// `Worker._free_children`): the isolated children that wait for a task
    /// and the free seats of the open shared children, less the tasks that
    /// wait for one. A kind with no pool has 0. Each pool is read under its
    /// own lock.
    pub fn free_capacity(&self) -> FreeCapacity {
        FreeCapacity {
            isolated: self.isolated.as_ref().map_or(0, IsolatedPool::free_slots),
            shared_async: self.shared_async.as_ref().map_or(0, SharedPool::free_seats),
            shared_thread: self
                .shared_thread
                .as_ref()
                .map_or(0, SharedPool::free_seats),
        }
    }

    /// Route one claimed row (Python `_dispatch_one`) and submit it to the
    /// pool of its kind. The end of the attempt comes on the receiver of
    /// `Submitted::Queued`, exactly once. A row of a class this worker does
    /// not serve, or a route to a kind with no pool, never reaches a child:
    /// `NotDispatched(NoChildOfItsKind)`; its claim goes back to the queue.
    pub fn submit(&self, claimed: Claimed) -> Submitted {
        self.submit_watched(claimed, PlacementWatch::Unwatched)
    }

    /// As [`WorkerChildren::submit`]; `watch` is told when a child takes the
    /// task. A task that reaches no child drops `watch` untold.
    pub fn submit_watched(&self, claimed: Claimed, watch: PlacementWatch) -> Submitted {
        let Claimed { task, input } = claimed;
        let route = match dispatch_route(task.execution, self.service().plan(), &task.options) {
            Ok(route) => route,
            Err(unserved) => {
                tracing::error!(task_id = %task.task_id, attempt = task.retry.attempt().get(), trace_id = task.trace_id.log_field(), "{unserved}; the row goes back to the queue");
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
                pool.submit_watched(&task, input, work, deadline, watch)
            }
            (DispatchRoute::SharedAsync(work), _, Some(pool), _) => {
                pool.submit_watched(&task, input, work, watch)
            }
            (DispatchRoute::SharedThread(work), _, _, Some(pool)) => {
                pool.submit_watched(&task, input, work, watch)
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
            report: join_stop_reports([isolated, shared_async, shared_thread]),
        }
    }
}

/// How far the stop of one pool went, worst last.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum StopReach {
    Drained,
    Killed,
    Aborted,
}

/// One report for the pools: the worst of `reports`, with the killed tasks
/// of every pool in pool order (Python joins one list of the one pool).
/// Pure.
fn join_stop_reports(reports: [PoolStopReport; 3]) -> PoolStopReport {
    let mut reach = StopReach::Drained;
    let mut killed: Vec<Uuid> = Vec::new();
    for report in reports {
        let (pool_reach, pool_killed) = match report {
            PoolStopReport::Drained => (StopReach::Drained, Vec::new()),
            PoolStopReport::KilledAtDrainDeadline { killed } => (StopReach::Killed, killed),
            PoolStopReport::AbortedAfterDrain { killed } => (StopReach::Aborted, killed),
        };
        reach = reach.max(pool_reach);
        killed.extend(pool_killed);
    }
    match reach {
        StopReach::Drained => PoolStopReport::Drained,
        StopReach::Killed => PoolStopReport::KilledAtDrainDeadline { killed },
        StopReach::Aborted => PoolStopReport::AbortedAfterDrain { killed },
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
