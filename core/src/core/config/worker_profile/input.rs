//! A worker profile as `AppConfig.worker_profiles` holds it: every count,
//! size and task limit is a plain number, and the drain timeout is a
//! `Duration` (Python `WorkerProfile`,
//! `clients/python/horsies/core/models/worker_profile.py`).
//!
//! A Rust app writes a profile with these types:
//! `WorkerProfile { isolated: IsolatedChildren::new(2).into(), ..WorkerProfile::default() }`.
//! The TypeScript client sends one as JSON text. The JSON form is the form
//! of the checked types of the parent module: an object with a `kind` key
//! for each choice, and every object refuses an unknown key.
//!
//! A number outside its bound parses. AppConfig validation refuses it with
//! its field path, together with every other refusal of the config
//! ([`super::worker_profiles_shape_errors`]). [`WorkerProfile::checked`]
//! builds the checked profile: its bounded types are the types of the
//! worker manifest and of the worker.

use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::{
    profile_error, profile_path, ChildPreload, DrainTimeout, DrainTimeoutRefused,
    ExecutorThreadCount, IsolatedChildCount, MemoryMb, ParentCpu, PoolConnectionCount, PoolMinSize,
    PoolSizeKind, SharedAsyncChildCount, SharedAsyncSeats, SharedThreadChildCount,
    SharedThreadSeats, TaskCount, DEFAULT_DRAIN_TIMEOUT, DEFAULT_RECYCLE_AFTER_TASKS,
    DEFAULT_RECYCLE_AFTER_TASKS_PER_SEAT, MAX_DRAIN_TIMEOUT,
};
use crate::core::config::bounded::{BoundedU32, OutOfBounds};
use crate::core::config::child_sizing::{
    DEFAULT_ISOLATED_POOL_CONNECTIONS, DEFAULT_TASK_SEND_POOL_CONNECTIONS,
};
use crate::core::error::HorsiesError;

/// Connections that a pool keeps open when the profile does not say: none.
const NO_OPEN_CONNECTIONS: u32 = 0;

/// A limit on a number of tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskLimitSetting {
    /// A limit of `tasks` tasks: 1 to [`super::MAX_TASK_LIMIT`].
    TaskLimit { tasks: u32 },
    /// No limit on the number of tasks.
    NoTaskLimit {},
}

/// A limit on the resident memory (RSS) of a child. At start the worker
/// refuses a limit at or below the measured child baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MemoryLimitSetting {
    /// A limit of `mb` MB of RSS: 1 to [`super::MAX_MEMORY_LIMIT_MB`].
    MemoryLimit { mb: u32 },
    /// No limit on the RSS of a child.
    NoMemoryLimit {},
}

/// The most connections of a pool whose size can follow the seats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PoolSizeSetting {
    /// A pool of at most `connections` connections: 1 to
    /// [`super::MAX_CHILD_POOL_CONNECTIONS`].
    PoolSize { connections: u32 },
    /// The size follows the seats (`core::config::child_sizing`).
    PoolSizeFromSeats {},
}

/// The threads of the default executor of a shared async child.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutorThreadsSetting {
    /// A default executor of `threads` threads: 1 to
    /// [`super::MAX_EXECUTOR_THREADS`].
    ExecutorThreads { threads: u32 },
    /// The threads follow the seats of the child (`core::config::child_sizing`).
    ExecutorThreadsFromSeats {},
}

/// A pool of at most `connections` connections (1 to
/// [`super::MAX_CHILD_POOL_CONNECTIONS`]), where no size follows the seats:
/// the pool of an isolated child and the pool that task code opens in a
/// child when it sends. The JSON form is
/// `{"kind": "pool_size", "connections": 2}`; `kind` may be left out, and
/// any other `kind` is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolSize {
    #[serde(default)]
    kind: PoolSizeKind,
    pub connections: u32,
}

impl PoolSize {
    /// A pool of at most `connections` connections.
    pub const fn new(connections: u32) -> Self {
        Self {
            kind: PoolSizeKind::PoolSize,
            connections,
        }
    }
}

/// The connection pool that each isolated child opens (the checked
/// [`super::IsolatedPool`] states each field).
///
/// - `min_size`: connections that the pool keeps open, 0 to
///   [`super::MAX_CHILD_POOL_CONNECTIONS`].
/// - `max_size`: the most connections.
/// - `check`: check each connection when a write takes it from the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IsolatedPool {
    #[serde(default = "no_open_connections")]
    pub min_size: u32,
    #[serde(default = "default_isolated_pool_size")]
    pub max_size: PoolSize,
    #[serde(default)]
    pub check: bool,
}

impl Default for IsolatedPool {
    fn default() -> Self {
        Self {
            min_size: NO_OPEN_CONNECTIONS,
            max_size: default_isolated_pool_size(),
            check: false,
        }
    }
}

/// The connection pool that each shared child opens. Its seats share it.
///
/// - `min_size`: connections that the pool keeps open, 0 to
///   [`super::MAX_CHILD_POOL_CONNECTIONS`].
/// - `max_size`: a set size, or one that follows the seats (the default).
/// - `check`: as for [`IsolatedPool`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedPool {
    #[serde(default = "no_open_connections")]
    pub min_size: u32,
    #[serde(default = "pool_size_from_seats")]
    pub max_size: PoolSizeSetting,
    #[serde(default)]
    pub check: bool,
}

impl Default for SharedPool {
    fn default() -> Self {
        Self {
            min_size: NO_OPEN_CONNECTIONS,
            max_size: pool_size_from_seats(),
            check: false,
        }
    }
}

/// When the worker replaces an isolated child: after `after` completed
/// tasks, or when its own RSS reaches `above`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IsolatedRecycle {
    #[serde(default = "default_recycle_after")]
    pub after: TaskLimitSetting,
    #[serde(default = "no_memory_limit")]
    pub above: MemoryLimitSetting,
}

impl Default for IsolatedRecycle {
    fn default() -> Self {
        Self {
            after: default_recycle_after(),
            above: no_memory_limit(),
        }
    }
}

/// When the worker replaces a shared child: after `after_per_seat.tasks ×
/// seats` completed tasks, or when its own RSS reaches `above`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedRecycle {
    #[serde(default = "default_recycle_after_per_seat")]
    pub after_per_seat: TaskLimitSetting,
    #[serde(default = "no_memory_limit")]
    pub above: MemoryLimitSetting,
}

impl Default for SharedRecycle {
    fn default() -> Self {
        Self {
            after_per_seat: default_recycle_after_per_seat(),
            above: no_memory_limit(),
        }
    }
}

/// `count` isolated children (1 to [`super::MAX_ISOLATED_CHILDREN`]): each
/// runs one task at a time in its own process and opens the pool `pool`.
/// Task code that sends opens a pool of up to `task_send_pool` connections
/// in its child.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IsolatedChildren {
    pub count: u32,
    #[serde(default)]
    pub recycle: IsolatedRecycle,
    #[serde(default)]
    pub pool: IsolatedPool,
    #[serde(default = "default_task_send_pool")]
    pub task_send_pool: PoolSize,
}

impl IsolatedChildren {
    /// `count` isolated children with the default recycle and pools.
    pub fn new(count: u32) -> Self {
        Self {
            count,
            recycle: IsolatedRecycle::default(),
            pool: IsolatedPool::default(),
            task_send_pool: default_task_send_pool(),
        }
    }
}

/// `count` shared async children (1 to
/// [`super::MAX_SHARED_ASYNC_CHILDREN`]), each with `seats` seats (1 to
/// [`super::MAX_SEATS_PER_SHARED_ASYNC_CHILD`]): a child runs up to `seats`
/// async tasks at once on one event loop. `executor` sizes the default
/// executor of its event loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedAsyncChildren {
    pub count: u32,
    pub seats: u32,
    #[serde(default)]
    pub recycle: SharedRecycle,
    #[serde(default)]
    pub pool: SharedPool,
    #[serde(default = "executor_threads_from_seats")]
    pub executor: ExecutorThreadsSetting,
    #[serde(default = "default_task_send_pool")]
    pub task_send_pool: PoolSize,
}

impl SharedAsyncChildren {
    /// `count` children of `seats` seats with the default recycle, pools and
    /// executor.
    pub fn new(count: u32, seats: u32) -> Self {
        Self {
            count,
            seats,
            recycle: SharedRecycle::default(),
            pool: SharedPool::default(),
            executor: executor_threads_from_seats(),
            task_send_pool: default_task_send_pool(),
        }
    }
}

/// `count` shared thread children (1 to
/// [`super::MAX_SHARED_THREAD_CHILDREN`]), each with `seats` seats (1 to
/// [`super::MAX_SEATS_PER_SHARED_THREAD_CHILD`]): a child runs up to `seats`
/// blocking tasks at once, each on a thread of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedThreadChildren {
    pub count: u32,
    pub seats: u32,
    #[serde(default)]
    pub recycle: SharedRecycle,
    #[serde(default)]
    pub pool: SharedPool,
    #[serde(default = "default_task_send_pool")]
    pub task_send_pool: PoolSize,
}

impl SharedThreadChildren {
    /// `count` children of `seats` seats with the default recycle and pools.
    pub fn new(count: u32, seats: u32) -> Self {
        Self {
            count,
            seats,
            recycle: SharedRecycle::default(),
            pool: SharedPool::default(),
            task_send_pool: default_task_send_pool(),
        }
    }
}

/// The isolated children of a profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum IsolatedSetting {
    IsolatedChildren(IsolatedChildren),
    /// The profile starts no isolated children.
    NoIsolatedChildren {},
}

/// The shared async children of a profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SharedAsyncSetting {
    SharedAsyncChildren(SharedAsyncChildren),
    /// The profile starts no shared async children.
    NoSharedAsyncChildren {},
}

/// The shared thread children of a profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SharedThreadSetting {
    SharedThreadChildren(SharedThreadChildren),
    /// The profile starts no shared thread children.
    NoSharedThreadChildren {},
}

impl From<IsolatedChildren> for IsolatedSetting {
    fn from(children: IsolatedChildren) -> Self {
        Self::IsolatedChildren(children)
    }
}

impl From<SharedAsyncChildren> for SharedAsyncSetting {
    fn from(children: SharedAsyncChildren) -> Self {
        Self::SharedAsyncChildren(children)
    }
}

impl From<SharedThreadChildren> for SharedThreadSetting {
    fn from(children: SharedThreadChildren) -> Self {
        Self::SharedThreadChildren(children)
    }
}

/// Limits on the tasks that the worker claims (the checked
/// [`super::ClaimLimits`] states each limit).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimLimits {
    #[serde(default = "no_task_limit")]
    pub per_queue_per_pass: TaskLimitSetting,
    #[serde(default = "no_task_limit")]
    pub claimed_per_worker: TaskLimitSetting,
}

impl Default for ClaimLimits {
    fn default() -> Self {
        Self {
            per_queue_per_pass: no_task_limit(),
            claimed_per_worker: no_task_limit(),
        }
    }
}

/// The connection pool of the worker process. By default its size follows
/// the seats of the profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParentPool {
    #[serde(default = "pool_size_from_seats")]
    pub max_size: PoolSizeSetting,
}

impl Default for ParentPool {
    fn default() -> Self {
        Self {
            max_size: pool_size_from_seats(),
        }
    }
}

/// The settings of one worker (the checked [`super::WorkerProfile`] states
/// each field).
///
/// - `isolated`, `shared_async`, `shared_thread`: the children of each mode.
/// - `pool`: the connection pool of the worker process.
/// - `claim`: the claim limits.
/// - `drain_timeout`: how long a stopping worker waits for running tasks:
///   above 0 and at most [`MAX_DRAIN_TIMEOUT`]. The JSON form is the serde
///   form of `std::time::Duration`: `{"secs": 300, "nanos": 0}`.
/// - `preload`: what a Linux worker imports once before it forks children.
/// - `parent_cpu`: where the worker process runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerProfile {
    #[serde(default = "no_isolated_children")]
    pub isolated: IsolatedSetting,
    #[serde(default = "no_shared_async_children")]
    pub shared_async: SharedAsyncSetting,
    #[serde(default = "no_shared_thread_children")]
    pub shared_thread: SharedThreadSetting,
    #[serde(default)]
    pub pool: ParentPool,
    #[serde(default)]
    pub claim: ClaimLimits,
    #[serde(default = "default_drain_timeout")]
    pub drain_timeout: Duration,
    #[serde(default)]
    pub preload: ChildPreload,
    #[serde(default)]
    pub parent_cpu: ParentCpu,
}

impl Default for WorkerProfile {
    fn default() -> Self {
        Self {
            isolated: no_isolated_children(),
            shared_async: no_shared_async_children(),
            shared_thread: no_shared_thread_children(),
            pool: ParentPool::default(),
            claim: ClaimLimits::default(),
            drain_timeout: DEFAULT_DRAIN_TIMEOUT,
            preload: ChildPreload::default(),
            parent_cpu: ParentCpu::default(),
        }
    }
}

fn no_open_connections() -> u32 {
    NO_OPEN_CONNECTIONS
}

fn default_isolated_pool_size() -> PoolSize {
    PoolSize::new(DEFAULT_ISOLATED_POOL_CONNECTIONS)
}

fn default_task_send_pool() -> PoolSize {
    PoolSize::new(DEFAULT_TASK_SEND_POOL_CONNECTIONS)
}

fn pool_size_from_seats() -> PoolSizeSetting {
    PoolSizeSetting::PoolSizeFromSeats {}
}

fn executor_threads_from_seats() -> ExecutorThreadsSetting {
    ExecutorThreadsSetting::ExecutorThreadsFromSeats {}
}

fn no_task_limit() -> TaskLimitSetting {
    TaskLimitSetting::NoTaskLimit {}
}

fn default_recycle_after() -> TaskLimitSetting {
    TaskLimitSetting::TaskLimit {
        tasks: DEFAULT_RECYCLE_AFTER_TASKS,
    }
}

fn default_recycle_after_per_seat() -> TaskLimitSetting {
    TaskLimitSetting::TaskLimit {
        tasks: DEFAULT_RECYCLE_AFTER_TASKS_PER_SEAT,
    }
}

fn no_memory_limit() -> MemoryLimitSetting {
    MemoryLimitSetting::NoMemoryLimit {}
}

fn no_isolated_children() -> IsolatedSetting {
    IsolatedSetting::NoIsolatedChildren {}
}

fn no_shared_async_children() -> SharedAsyncSetting {
    SharedAsyncSetting::NoSharedAsyncChildren {}
}

fn no_shared_thread_children() -> SharedThreadSetting {
    SharedThreadSetting::NoSharedThreadChildren {}
}

fn default_drain_timeout() -> Duration {
    DEFAULT_DRAIN_TIMEOUT
}

/// A value of a worker profile outside its bound. `path` is the field path
/// of the value in the config, for example
/// `worker_profiles['batch'].isolated.count`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfileValueRefused {
    /// A count, size or task limit outside its bounds.
    OutOfBounds { path: String, refused: OutOfBounds },
    /// A drain timeout of 0, or above [`MAX_DRAIN_TIMEOUT`].
    DrainTimeout {
        path: String,
        refused: DrainTimeoutRefused,
    },
}

impl ProfileValueRefused {
    /// The field path of the refused value.
    pub fn path(&self) -> &str {
        match self {
            Self::OutOfBounds { path, .. } | Self::DrainTimeout { path, .. } => path,
        }
    }

    /// The config refusal: `CONFIG_INVALID_WORKER_PROFILE`, with the path
    /// and the bound of the value in the message and the bound in the help.
    pub fn to_horsies_error(&self) -> HorsiesError {
        let help = match self {
            Self::OutOfBounds { refused, .. } => {
                format!("set a value from {} to {}", refused.min, refused.max)
            }
            Self::DrainTimeout { .. } => {
                format!("set a drain timeout above 0 and at most {MAX_DRAIN_TIMEOUT:?}")
            }
        };
        profile_error(self.to_string(), &[], help)
    }
}

impl fmt::Display for ProfileValueRefused {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfBounds { path, refused } => write!(
                formatter,
                "{path}: {} is outside the bounds {} to {}",
                refused.value, refused.min, refused.max,
            ),
            Self::DrainTimeout {
                path,
                refused: DrainTimeoutRefused::Zero,
            } => write!(
                formatter,
                "{path}: a drain timeout of 0 is refused; a stopping worker needs a deadline"
            ),
            Self::DrainTimeout {
                path,
                refused: DrainTimeoutRefused::AboveMaximum(timeout),
            } => write!(
                formatter,
                "{path}: {timeout:?} is above the maximum of {MAX_DRAIN_TIMEOUT:?}"
            ),
        }
    }
}

impl std::error::Error for ProfileValueRefused {}

impl WorkerProfile {
    /// The checked profile of the profile `name`, or the refusal of each of
    /// its values that is outside its bound, in field order. The checked
    /// profile has the same values. Pure.
    ///
    /// It does not check what needs more than one value (at least one
    /// child, a pool `min_size` against its `max_size`, the claim limits
    /// against the capacity): AppConfig validation checks them on the
    /// checked profile.
    pub fn checked(&self, name: &str) -> Result<super::WorkerProfile, Vec<ProfileValueRefused>> {
        let mut check = ProfileCheck::new(name);
        let isolated = isolated_setting(&mut check, &self.isolated);
        let shared_async = shared_async_setting(&mut check, &self.shared_async);
        let shared_thread = shared_thread_setting(&mut check, &self.shared_thread);
        let max_size = pool_size_setting(&mut check, "pool.max_size", self.pool.max_size);
        let per_queue_per_pass = task_limit(
            &mut check,
            "claim.per_queue_per_pass",
            self.claim.per_queue_per_pass,
        );
        let claimed_per_worker = task_limit(
            &mut check,
            "claim.claimed_per_worker",
            self.claim.claimed_per_worker,
        );
        let drain_timeout = check.drain_timeout(self.drain_timeout);
        let (
            Some(isolated),
            Some(shared_async),
            Some(shared_thread),
            Some(max_size),
            Some(per_queue_per_pass),
            Some(claimed_per_worker),
            Some(drain_timeout),
        ) = (
            isolated,
            shared_async,
            shared_thread,
            max_size,
            per_queue_per_pass,
            claimed_per_worker,
            drain_timeout,
        )
        else {
            assert!(
                !check.refusals.is_empty(),
                "a value of the profile {} is missing without a refusal",
                check.profile,
            );
            return Err(check.refusals);
        };
        Ok(super::WorkerProfile {
            isolated,
            shared_async,
            shared_thread,
            pool: super::ParentPool { max_size },
            claim: super::ClaimLimits {
                per_queue_per_pass,
                claimed_per_worker,
            },
            drain_timeout,
            preload: self.preload,
            parent_cpu: self.parent_cpu,
        })
    }
}

/// The refusals of one profile, kept while each of its values is checked.
/// Each check that keeps a refusal returns `None`, so a profile with a
/// refusal has no checked value.
struct ProfileCheck {
    /// The path of the profile: `worker_profiles['batch']`.
    profile: String,
    refusals: Vec<ProfileValueRefused>,
}

impl ProfileCheck {
    fn new(name: &str) -> Self {
        Self {
            profile: profile_path(name),
            refusals: Vec::new(),
        }
    }

    /// The path of `field` in the profile.
    fn path(&self, field: &str) -> String {
        format!("{}.{field}", self.profile)
    }

    /// `value` as a bounded number, or `None` after its refusal is kept.
    fn bounded<const MIN: u32, const MAX: u32>(
        &mut self,
        field: &str,
        value: u32,
    ) -> Option<BoundedU32<MIN, MAX>> {
        match BoundedU32::<MIN, MAX>::try_from(value) {
            Ok(bounded) => Some(bounded),
            Err(refused) => {
                let path = self.path(field);
                self.refusals
                    .push(ProfileValueRefused::OutOfBounds { path, refused });
                None
            }
        }
    }

    /// `value` as a drain timeout, or `None` after its refusal is kept.
    fn drain_timeout(&mut self, value: Duration) -> Option<DrainTimeout> {
        match DrainTimeout::try_from(value) {
            Ok(timeout) => Some(timeout),
            Err(refused) => {
                let path = self.path("drain_timeout");
                self.refusals
                    .push(ProfileValueRefused::DrainTimeout { path, refused });
                None
            }
        }
    }
}

fn task_limit(
    check: &mut ProfileCheck,
    field: &str,
    setting: TaskLimitSetting,
) -> Option<super::TaskLimitSetting> {
    match setting {
        TaskLimitSetting::TaskLimit { tasks } => {
            let tasks: Option<TaskCount> = check.bounded(&format!("{field}.tasks"), tasks);
            tasks.map(|tasks| super::TaskLimitSetting::TaskLimit { tasks })
        }
        TaskLimitSetting::NoTaskLimit {} => Some(super::TaskLimitSetting::NoTaskLimit {}),
    }
}

fn memory_limit(
    check: &mut ProfileCheck,
    field: &str,
    setting: MemoryLimitSetting,
) -> Option<super::MemoryLimitSetting> {
    match setting {
        MemoryLimitSetting::MemoryLimit { mb } => {
            let mb: Option<MemoryMb> = check.bounded(&format!("{field}.mb"), mb);
            mb.map(|mb| super::MemoryLimitSetting::MemoryLimit { mb })
        }
        MemoryLimitSetting::NoMemoryLimit {} => Some(super::MemoryLimitSetting::NoMemoryLimit {}),
    }
}

fn pool_size_setting(
    check: &mut ProfileCheck,
    field: &str,
    setting: PoolSizeSetting,
) -> Option<super::PoolSizeSetting> {
    match setting {
        PoolSizeSetting::PoolSize { connections } => {
            let connections: Option<PoolConnectionCount> =
                check.bounded(&format!("{field}.connections"), connections);
            connections.map(|connections| super::PoolSizeSetting::PoolSize { connections })
        }
        PoolSizeSetting::PoolSizeFromSeats {} => Some(super::PoolSizeSetting::PoolSizeFromSeats {}),
    }
}

fn executor_threads(
    check: &mut ProfileCheck,
    field: &str,
    setting: ExecutorThreadsSetting,
) -> Option<super::ExecutorThreadsSetting> {
    match setting {
        ExecutorThreadsSetting::ExecutorThreads { threads } => {
            let threads: Option<ExecutorThreadCount> =
                check.bounded(&format!("{field}.threads"), threads);
            threads.map(|threads| super::ExecutorThreadsSetting::ExecutorThreads { threads })
        }
        ExecutorThreadsSetting::ExecutorThreadsFromSeats {} => {
            Some(super::ExecutorThreadsSetting::ExecutorThreadsFromSeats {})
        }
    }
}

fn pool_size(check: &mut ProfileCheck, field: &str, size: PoolSize) -> Option<super::PoolSize> {
    let connections: Option<PoolConnectionCount> =
        check.bounded(&format!("{field}.connections"), size.connections);
    connections.map(super::PoolSize::new)
}

fn shared_recycle(
    check: &mut ProfileCheck,
    field: &str,
    recycle: SharedRecycle,
) -> Option<super::SharedRecycle> {
    let after_per_seat = task_limit(
        check,
        &format!("{field}.after_per_seat"),
        recycle.after_per_seat,
    );
    let above = memory_limit(check, &format!("{field}.above"), recycle.above);
    let (Some(after_per_seat), Some(above)) = (after_per_seat, above) else {
        return None;
    };
    Some(super::SharedRecycle {
        after_per_seat,
        above,
    })
}

fn shared_pool(
    check: &mut ProfileCheck,
    field: &str,
    pool: SharedPool,
) -> Option<super::SharedPool> {
    let min_size: Option<PoolMinSize> = check.bounded(&format!("{field}.min_size"), pool.min_size);
    let max_size = pool_size_setting(check, &format!("{field}.max_size"), pool.max_size);
    let (Some(min_size), Some(max_size)) = (min_size, max_size) else {
        return None;
    };
    Some(super::SharedPool {
        min_size,
        max_size,
        check: pool.check,
    })
}

fn isolated_children(
    check: &mut ProfileCheck,
    children: &IsolatedChildren,
) -> Option<super::IsolatedChildren> {
    let count: Option<IsolatedChildCount> = check.bounded("isolated.count", children.count);
    let after = task_limit(check, "isolated.recycle.after", children.recycle.after);
    let above = memory_limit(check, "isolated.recycle.above", children.recycle.above);
    let min_size: Option<PoolMinSize> =
        check.bounded("isolated.pool.min_size", children.pool.min_size);
    let max_size = pool_size(check, "isolated.pool.max_size", children.pool.max_size);
    let task_send_pool = pool_size(check, "isolated.task_send_pool", children.task_send_pool);
    let (
        Some(count),
        Some(after),
        Some(above),
        Some(min_size),
        Some(max_size),
        Some(task_send_pool),
    ) = (count, after, above, min_size, max_size, task_send_pool)
    else {
        return None;
    };
    Some(super::IsolatedChildren {
        count,
        recycle: super::IsolatedRecycle { after, above },
        pool: super::IsolatedPool {
            min_size,
            max_size,
            check: children.pool.check,
        },
        task_send_pool,
    })
}

fn shared_async_children(
    check: &mut ProfileCheck,
    children: &SharedAsyncChildren,
) -> Option<super::SharedAsyncChildren> {
    let count: Option<SharedAsyncChildCount> = check.bounded("shared_async.count", children.count);
    let seats: Option<SharedAsyncSeats> = check.bounded("shared_async.seats", children.seats);
    let recycle = shared_recycle(check, "shared_async.recycle", children.recycle);
    let pool = shared_pool(check, "shared_async.pool", children.pool);
    let executor = executor_threads(check, "shared_async.executor", children.executor);
    let task_send_pool = pool_size(
        check,
        "shared_async.task_send_pool",
        children.task_send_pool,
    );
    let (Some(count), Some(seats), Some(recycle), Some(pool), Some(executor), Some(task_send_pool)) =
        (count, seats, recycle, pool, executor, task_send_pool)
    else {
        return None;
    };
    Some(super::SharedAsyncChildren {
        count,
        seats,
        recycle,
        pool,
        executor,
        task_send_pool,
    })
}

fn shared_thread_children(
    check: &mut ProfileCheck,
    children: &SharedThreadChildren,
) -> Option<super::SharedThreadChildren> {
    let count: Option<SharedThreadChildCount> =
        check.bounded("shared_thread.count", children.count);
    let seats: Option<SharedThreadSeats> = check.bounded("shared_thread.seats", children.seats);
    let recycle = shared_recycle(check, "shared_thread.recycle", children.recycle);
    let pool = shared_pool(check, "shared_thread.pool", children.pool);
    let task_send_pool = pool_size(
        check,
        "shared_thread.task_send_pool",
        children.task_send_pool,
    );
    let (Some(count), Some(seats), Some(recycle), Some(pool), Some(task_send_pool)) =
        (count, seats, recycle, pool, task_send_pool)
    else {
        return None;
    };
    Some(super::SharedThreadChildren {
        count,
        seats,
        recycle,
        pool,
        task_send_pool,
    })
}

fn isolated_setting(
    check: &mut ProfileCheck,
    setting: &IsolatedSetting,
) -> Option<super::IsolatedSetting> {
    match setting {
        IsolatedSetting::IsolatedChildren(children) => {
            isolated_children(check, children).map(super::IsolatedSetting::IsolatedChildren)
        }
        IsolatedSetting::NoIsolatedChildren {} => {
            Some(super::IsolatedSetting::NoIsolatedChildren {})
        }
    }
}

fn shared_async_setting(
    check: &mut ProfileCheck,
    setting: &SharedAsyncSetting,
) -> Option<super::SharedAsyncSetting> {
    match setting {
        SharedAsyncSetting::SharedAsyncChildren(children) => shared_async_children(check, children)
            .map(super::SharedAsyncSetting::SharedAsyncChildren),
        SharedAsyncSetting::NoSharedAsyncChildren {} => {
            Some(super::SharedAsyncSetting::NoSharedAsyncChildren {})
        }
    }
}

fn shared_thread_setting(
    check: &mut ProfileCheck,
    setting: &SharedThreadSetting,
) -> Option<super::SharedThreadSetting> {
    match setting {
        SharedThreadSetting::SharedThreadChildren(children) => {
            shared_thread_children(check, children)
                .map(super::SharedThreadSetting::SharedThreadChildren)
        }
        SharedThreadSetting::NoSharedThreadChildren {} => {
            Some(super::SharedThreadSetting::NoSharedThreadChildren {})
        }
    }
}

// The config form of a checked profile, with the same values: the worker
// builds its `AppConfig` from the profiles of its manifest. Total.

impl From<super::TaskLimitSetting> for TaskLimitSetting {
    fn from(setting: super::TaskLimitSetting) -> Self {
        match setting {
            super::TaskLimitSetting::TaskLimit { tasks } => Self::TaskLimit { tasks: tasks.get() },
            super::TaskLimitSetting::NoTaskLimit {} => Self::NoTaskLimit {},
        }
    }
}

impl From<super::MemoryLimitSetting> for MemoryLimitSetting {
    fn from(setting: super::MemoryLimitSetting) -> Self {
        match setting {
            super::MemoryLimitSetting::MemoryLimit { mb } => Self::MemoryLimit { mb: mb.get() },
            super::MemoryLimitSetting::NoMemoryLimit {} => Self::NoMemoryLimit {},
        }
    }
}

impl From<super::PoolSizeSetting> for PoolSizeSetting {
    fn from(setting: super::PoolSizeSetting) -> Self {
        match setting {
            super::PoolSizeSetting::PoolSize { connections } => Self::PoolSize {
                connections: connections.get(),
            },
            super::PoolSizeSetting::PoolSizeFromSeats {} => Self::PoolSizeFromSeats {},
        }
    }
}

impl From<super::ExecutorThreadsSetting> for ExecutorThreadsSetting {
    fn from(setting: super::ExecutorThreadsSetting) -> Self {
        match setting {
            super::ExecutorThreadsSetting::ExecutorThreads { threads } => Self::ExecutorThreads {
                threads: threads.get(),
            },
            super::ExecutorThreadsSetting::ExecutorThreadsFromSeats {} => {
                Self::ExecutorThreadsFromSeats {}
            }
        }
    }
}

impl From<super::PoolSize> for PoolSize {
    fn from(size: super::PoolSize) -> Self {
        Self::new(size.connections.get())
    }
}

impl From<super::SharedRecycle> for SharedRecycle {
    fn from(recycle: super::SharedRecycle) -> Self {
        Self {
            after_per_seat: recycle.after_per_seat.into(),
            above: recycle.above.into(),
        }
    }
}

impl From<super::SharedPool> for SharedPool {
    fn from(pool: super::SharedPool) -> Self {
        Self {
            min_size: pool.min_size.get(),
            max_size: pool.max_size.into(),
            check: pool.check,
        }
    }
}

impl From<super::IsolatedChildren> for IsolatedChildren {
    fn from(children: super::IsolatedChildren) -> Self {
        Self {
            count: children.count.get(),
            recycle: IsolatedRecycle {
                after: children.recycle.after.into(),
                above: children.recycle.above.into(),
            },
            pool: IsolatedPool {
                min_size: children.pool.min_size.get(),
                max_size: children.pool.max_size.into(),
                check: children.pool.check,
            },
            task_send_pool: children.task_send_pool.into(),
        }
    }
}

impl From<super::SharedAsyncChildren> for SharedAsyncChildren {
    fn from(children: super::SharedAsyncChildren) -> Self {
        Self {
            count: children.count.get(),
            seats: children.seats.get(),
            recycle: children.recycle.into(),
            pool: children.pool.into(),
            executor: children.executor.into(),
            task_send_pool: children.task_send_pool.into(),
        }
    }
}

impl From<super::SharedThreadChildren> for SharedThreadChildren {
    fn from(children: super::SharedThreadChildren) -> Self {
        Self {
            count: children.count.get(),
            seats: children.seats.get(),
            recycle: children.recycle.into(),
            pool: children.pool.into(),
            task_send_pool: children.task_send_pool.into(),
        }
    }
}

impl From<super::IsolatedSetting> for IsolatedSetting {
    fn from(setting: super::IsolatedSetting) -> Self {
        match setting {
            super::IsolatedSetting::IsolatedChildren(children) => {
                Self::IsolatedChildren(children.into())
            }
            super::IsolatedSetting::NoIsolatedChildren {} => Self::NoIsolatedChildren {},
        }
    }
}

impl From<super::SharedAsyncSetting> for SharedAsyncSetting {
    fn from(setting: super::SharedAsyncSetting) -> Self {
        match setting {
            super::SharedAsyncSetting::SharedAsyncChildren(children) => {
                Self::SharedAsyncChildren(children.into())
            }
            super::SharedAsyncSetting::NoSharedAsyncChildren {} => Self::NoSharedAsyncChildren {},
        }
    }
}

impl From<super::SharedThreadSetting> for SharedThreadSetting {
    fn from(setting: super::SharedThreadSetting) -> Self {
        match setting {
            super::SharedThreadSetting::SharedThreadChildren(children) => {
                Self::SharedThreadChildren(children.into())
            }
            super::SharedThreadSetting::NoSharedThreadChildren {} => {
                Self::NoSharedThreadChildren {}
            }
        }
    }
}

impl From<super::WorkerProfile> for WorkerProfile {
    fn from(profile: super::WorkerProfile) -> Self {
        Self {
            isolated: profile.isolated.into(),
            shared_async: profile.shared_async.into(),
            shared_thread: profile.shared_thread.into(),
            pool: ParentPool {
                max_size: profile.pool.max_size.into(),
            },
            claim: ClaimLimits {
                per_queue_per_pass: profile.claim.per_queue_per_pass.into(),
                claimed_per_worker: profile.claim.claimed_per_worker.into(),
            },
            drain_timeout: profile.drain_timeout.get(),
            preload: profile.preload,
            parent_cpu: profile.parent_cpu,
        }
    }
}

#[cfg(test)]
mod tests;
