//! Named worker profiles: the settings of one worker, kept in the app config
//! (Python `core/models/worker_profile.py`).
//!
//! A profile states:
//!
//! - how many children the worker runs for each execution mode,
//! - when a child is replaced (recycled),
//! - the connection pool of each child, and the default executor of a shared
//!   async child,
//! - the pool that task code opens in a child when it sends,
//! - the connection pool of the worker process itself,
//! - how many tasks the worker claims,
//! - how long a stopping worker waits for running tasks,
//! - what a Linux worker imports before it forks children,
//! - where the worker process runs.
//!
//! Three execution modes have children: an isolated child runs one task at a
//! time in its own process; a shared async child runs up to `seats` async
//! tasks at once on one event loop; a shared thread child runs up to `seats`
//! blocking tasks at once, each on a thread of its own. The number of
//! children of a worker is the sum of the counts of its modes.
//!
//! Each value is bounded where it is parsed. The checks that need more than
//! one value (at least one child, claim limits against capacity, the
//! `min_size` of a child pool against its resolved `max_size`, the minimum of
//! the worker pool) run in AppConfig validation, which knows the profile name
//! and reports the field path, for example
//! `worker_profiles['batch'].claim.claimed_per_worker`.
//!
//! The JSON form of each choice is an object with a `kind` key, as Python's
//! discriminated unions: `{"kind": "task_limit", "tasks": 100}`. Every
//! object refuses an unknown key.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};

use super::bounded::BoundedU32;
use super::child_sizing::{
    parent_pool_connections, shared_async_executor_threads, shared_async_pool_connections,
    shared_thread_pool_connections, DEFAULT_ISOLATED_POOL_CONNECTIONS,
    DEFAULT_TASK_SEND_POOL_CONNECTIONS, FINALIZE_BATCH_FLUSHERS, PARENT_POOL_CLAIM_CONNECTIONS,
    PARENT_POOL_MIN_CONNECTIONS, PARENT_POOL_RESERVE,
};
use crate::core::error::{ErrorCode, HorsiesError};

/// The most isolated children that one profile starts.
pub const MAX_ISOLATED_CHILDREN: u32 = 1024;

/// The largest task count of a limit: the largest Postgres `integer`, the
/// type that the claim function takes.
pub const MAX_TASK_LIMIT: u32 = 2_147_483_647;

/// The largest memory limit of one child: 16 TiB, in MB.
pub const MAX_MEMORY_LIMIT_MB: u32 = 16 * 1024 * 1024;

/// The most shared async children that one profile starts.
pub const MAX_SHARED_ASYNC_CHILDREN: u32 = 1024;

/// The most tasks that one shared async child runs at once. A seat that
/// waits on the network holds a socket, and the common default limit of open
/// files of one process is 1024.
pub const MAX_SEATS_PER_SHARED_ASYNC_CHILD: u32 = 1000;

/// The most shared thread children that one profile starts.
pub const MAX_SHARED_THREAD_CHILDREN: u32 = 1024;

/// The most tasks that one shared thread child runs at once. Each seat is an
/// OS thread with a stack of its own: 256 seats reserve 2 GiB of address
/// space in one child on Linux with glibc.
pub const MAX_SEATS_PER_SHARED_THREAD_CHILD: u32 = 256;

/// The most connections in one pool of one process: the pool of a child, the
/// pool that task code opens when it sends, and the pool of the worker
/// process.
pub const MAX_CHILD_POOL_CONNECTIONS: u32 = 1000;

/// The most threads in the default executor of one shared async child.
pub const MAX_EXECUTOR_THREADS: u32 = 256;

/// Tasks after which an isolated child is replaced when the profile does not
/// say.
pub const DEFAULT_RECYCLE_AFTER_TASKS: u32 = 100;

/// Tasks per seat after which a shared child is replaced when the profile
/// does not say.
pub const DEFAULT_RECYCLE_AFTER_TASKS_PER_SEAT: u32 = 100;

/// Time that a stopping worker waits for running tasks by default.
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(300);

/// The longest time that a stopping worker waits for running tasks.
pub const MAX_DRAIN_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);

/// The most characters in a profile name.
pub const PROFILE_NAME_MAX_LENGTH: usize = 64;

/// A task count of a limit, 1 to [`MAX_TASK_LIMIT`].
pub type TaskCount = BoundedU32<1, MAX_TASK_LIMIT>;
/// A memory limit of one child in MB, 1 to [`MAX_MEMORY_LIMIT_MB`].
pub type MemoryMb = BoundedU32<1, MAX_MEMORY_LIMIT_MB>;
/// Isolated children of a profile, 1 to [`MAX_ISOLATED_CHILDREN`].
pub type IsolatedChildCount = BoundedU32<1, MAX_ISOLATED_CHILDREN>;
/// Shared async children of a profile, 1 to [`MAX_SHARED_ASYNC_CHILDREN`].
pub type SharedAsyncChildCount = BoundedU32<1, MAX_SHARED_ASYNC_CHILDREN>;
/// Seats of one shared async child, 1 to [`MAX_SEATS_PER_SHARED_ASYNC_CHILD`].
pub type SharedAsyncSeats = BoundedU32<1, MAX_SEATS_PER_SHARED_ASYNC_CHILD>;
/// Shared thread children of a profile, 1 to [`MAX_SHARED_THREAD_CHILDREN`].
pub type SharedThreadChildCount = BoundedU32<1, MAX_SHARED_THREAD_CHILDREN>;
/// Seats of one shared thread child, 1 to [`MAX_SEATS_PER_SHARED_THREAD_CHILD`].
pub type SharedThreadSeats = BoundedU32<1, MAX_SEATS_PER_SHARED_THREAD_CHILD>;
/// The most connections of a pool, 1 to [`MAX_CHILD_POOL_CONNECTIONS`].
pub type PoolConnectionCount = BoundedU32<1, MAX_CHILD_POOL_CONNECTIONS>;
/// Connections that a pool keeps open, 0 to [`MAX_CHILD_POOL_CONNECTIONS`].
pub type PoolMinSize = BoundedU32<0, MAX_CHILD_POOL_CONNECTIONS>;
/// Threads of a default executor, 1 to [`MAX_EXECUTOR_THREADS`].
pub type ExecutorThreadCount = BoundedU32<1, MAX_EXECUTOR_THREADS>;

/// A limit on a number of tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskLimitSetting {
    /// A limit of `tasks` tasks.
    TaskLimit { tasks: TaskCount },
    /// No limit on the number of tasks.
    NoTaskLimit {},
}

/// A limit on the resident memory (RSS) of a child.
///
/// No fixed floor applies: a constant cannot know the memory of a booted
/// child of an app. At start the worker refuses a limit at or below the
/// measured child baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MemoryLimitSetting {
    /// A limit of `mb` MB of RSS.
    MemoryLimit { mb: MemoryMb },
    /// No limit on the RSS of a child.
    NoMemoryLimit {},
}

/// The most connections of a pool whose size can follow the seats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PoolSizeSetting {
    /// A pool of at most `connections` connections.
    PoolSize { connections: PoolConnectionCount },
    /// The size follows the seats (`core::config::child_sizing`).
    PoolSizeFromSeats {},
}

/// The threads of the default executor of a shared async child.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutorThreadsSetting {
    /// A default executor of `threads` threads.
    ExecutorThreads { threads: ExecutorThreadCount },
    /// The threads follow the seats of the child (`core::config::child_sizing`).
    ExecutorThreadsFromSeats {},
}

/// The `kind` of a [`PoolSize`]: it has one value, and the JSON may leave it
/// out.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
enum PoolSizeKind {
    #[default]
    #[serde(rename = "pool_size")]
    PoolSize,
}

/// A pool of at most `connections` connections, where no size follows the
/// seats: the pool of an isolated child and the pool that task code opens in
/// a child when it sends. The JSON form is
/// `{"kind": "pool_size", "connections": 2}`; `kind` may be left out, and
/// any other `kind` is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolSize {
    #[serde(default)]
    kind: PoolSizeKind,
    pub connections: PoolConnectionCount,
}

impl PoolSize {
    /// A pool of at most `connections` connections.
    pub const fn new(connections: PoolConnectionCount) -> Self {
        Self {
            kind: PoolSizeKind::PoolSize,
            connections,
        }
    }
}

/// The connection pool of one child as it opens: from `min_size` to
/// `max_size` connections, and a check of each connection on checkout when
/// `check` is true. `min_size <= max_size` holds: only [`ResolvedPool::new`]
/// builds one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedPool {
    min_size: PoolMinSize,
    max_size: PoolConnectionCount,
    check: bool,
}

/// A pool whose `min_size` is above its resolved `max_size`. AppConfig
/// validation refuses a profile with one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolMinAboveMax {
    pub min_size: PoolMinSize,
    pub max_size: PoolConnectionCount,
}

impl ResolvedPool {
    /// The pool, or why it cannot open: `min_size` above `max_size`.
    pub fn new(
        min_size: PoolMinSize,
        max_size: PoolConnectionCount,
        check: bool,
    ) -> Result<Self, PoolMinAboveMax> {
        match min_size.get() <= max_size.get() {
            true => Ok(Self {
                min_size,
                max_size,
                check,
            }),
            false => Err(PoolMinAboveMax { min_size, max_size }),
        }
    }

    pub const fn min_size(self) -> PoolMinSize {
        self.min_size
    }

    pub const fn max_size(self) -> PoolConnectionCount {
        self.max_size
    }

    pub const fn check(self) -> bool {
        self.check
    }
}

/// The connection pool that each isolated child opens.
///
/// - `min_size`: connections that the pool keeps open. With 0, a connection
///   opens at its first use.
/// - `max_size`: the most connections. An isolated child has one seat, so no
///   size follows the seats.
/// - `check`: check each connection when a write takes it from the pool. Off
///   by default: a write of the child is one statement in autocommit, so a
///   check doubles its round trips. With it off, a dead pooled connection
///   fails the write that takes it: the RUNNING update ends as
///   `OWNERSHIP_UNCONFIRMED` and the worker releases the claim at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IsolatedPool {
    #[serde(default = "no_open_connections")]
    pub min_size: PoolMinSize,
    #[serde(default = "default_isolated_pool_size")]
    pub max_size: PoolSize,
    #[serde(default)]
    pub check: bool,
}

impl Default for IsolatedPool {
    fn default() -> Self {
        Self {
            min_size: no_open_connections(),
            max_size: default_isolated_pool_size(),
            check: false,
        }
    }
}

impl IsolatedPool {
    /// The pool as each isolated child opens it.
    pub fn resolved(&self) -> Result<ResolvedPool, PoolMinAboveMax> {
        ResolvedPool::new(self.min_size, self.max_size.connections, self.check)
    }
}

/// The connection pool that each shared child opens. Its seats share it.
///
/// - `min_size`: connections that the pool keeps open.
/// - `max_size`: the most connections: a set size, or one that follows the
///   seats of the child (the default).
/// - `check`: as for [`IsolatedPool`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedPool {
    #[serde(default = "no_open_connections")]
    pub min_size: PoolMinSize,
    #[serde(default = "pool_size_from_seats")]
    pub max_size: PoolSizeSetting,
    #[serde(default)]
    pub check: bool,
}

impl Default for SharedPool {
    fn default() -> Self {
        Self {
            min_size: no_open_connections(),
            max_size: pool_size_from_seats(),
            check: false,
        }
    }
}

impl SharedPool {
    /// The most connections of the pool. `from_seats` is the size that the
    /// kind of the child derives from its seats.
    pub fn resolved_max_size(&self, from_seats: PoolConnectionCount) -> PoolConnectionCount {
        match self.max_size {
            PoolSizeSetting::PoolSize { connections } => connections,
            PoolSizeSetting::PoolSizeFromSeats {} => from_seats,
        }
    }

    /// The pool as each shared child opens it.
    pub fn resolved(
        &self,
        from_seats: PoolConnectionCount,
    ) -> Result<ResolvedPool, PoolMinAboveMax> {
        ResolvedPool::new(
            self.min_size,
            self.resolved_max_size(from_seats),
            self.check,
        )
    }
}

/// When the worker replaces an isolated child with a new one. Each child has
/// its own counters, so children are replaced at different times.
///
/// - `after`: replace a child after it completes this many tasks.
/// - `above`: replace a child when its own RSS reaches this limit. The child
///   reports its RSS with each result; this bounds memory that stays after a
///   task, not the peak of a task.
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

/// When the worker replaces a shared child with a new one.
///
/// - `after_per_seat`: replace a child after it completes
///   `after_per_seat.tasks × seats` tasks.
/// - `above`: replace a child when its own RSS reaches this limit. Its tasks
///   share the process, so this bounds the memory of all of them together.
///
/// A child that reaches a limit closes: it takes no new task, its running
/// tasks finish, and then it stops and a new child starts.
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

/// `count` isolated children: each runs one task at a time in its own
/// process and opens the pool `pool`. Task code that sends opens a pool of up
/// to `task_send_pool` connections in its child, with no overflow, for each
/// event loop that it sends from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IsolatedChildren {
    pub count: IsolatedChildCount,
    #[serde(default)]
    pub recycle: IsolatedRecycle,
    #[serde(default)]
    pub pool: IsolatedPool,
    #[serde(default = "default_task_send_pool")]
    pub task_send_pool: PoolSize,
}

impl IsolatedChildren {
    /// `count` isolated children with the default recycle and pools.
    pub fn new(count: IsolatedChildCount) -> Self {
        Self {
            count,
            recycle: IsolatedRecycle::default(),
            pool: IsolatedPool::default(),
            task_send_pool: default_task_send_pool(),
        }
    }

    /// The pool that each child opens.
    pub fn resolved_pool(&self) -> Result<ResolvedPool, PoolMinAboveMax> {
        self.pool.resolved()
    }
}

/// `count` shared async children, each with `seats` seats: a child runs up to
/// `seats` async tasks at once on one event loop. Each child opens the pool
/// `pool`, which its seats share. `executor` sizes the default executor of
/// its event loop. Task code that sends opens a pool of up to
/// `task_send_pool` connections in its child, which its seats share.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedAsyncChildren {
    pub count: SharedAsyncChildCount,
    pub seats: SharedAsyncSeats,
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
    pub fn new(count: SharedAsyncChildCount, seats: SharedAsyncSeats) -> Self {
        Self {
            count,
            seats,
            recycle: SharedRecycle::default(),
            pool: SharedPool::default(),
            executor: executor_threads_from_seats(),
            task_send_pool: default_task_send_pool(),
        }
    }

    /// The most connections of the pool of each child.
    pub fn resolved_max_pool_size(&self) -> PoolConnectionCount {
        self.pool
            .resolved_max_size(shared_async_pool_connections(self.seats.into()))
    }

    /// The pool that each child opens.
    pub fn resolved_pool(&self) -> Result<ResolvedPool, PoolMinAboveMax> {
        self.pool
            .resolved(shared_async_pool_connections(self.seats.into()))
    }

    /// The threads of the default executor of each child.
    pub fn resolved_executor_threads(&self) -> ExecutorThreadCount {
        match self.executor {
            ExecutorThreadsSetting::ExecutorThreads { threads } => threads,
            ExecutorThreadsSetting::ExecutorThreadsFromSeats {} => {
                shared_async_executor_threads(self.seats.into())
            }
        }
    }
}

/// `count` shared thread children, each with `seats` seats: a child runs up
/// to `seats` blocking tasks at once, each on a thread of its own. Each child
/// opens the pool `pool`, which its seat threads share. Task code that sends
/// opens a pool of up to `task_send_pool` connections in its child.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedThreadChildren {
    pub count: SharedThreadChildCount,
    pub seats: SharedThreadSeats,
    #[serde(default)]
    pub recycle: SharedRecycle,
    #[serde(default)]
    pub pool: SharedPool,
    #[serde(default = "default_task_send_pool")]
    pub task_send_pool: PoolSize,
}

impl SharedThreadChildren {
    /// `count` children of `seats` seats with the default recycle and pools.
    pub fn new(count: SharedThreadChildCount, seats: SharedThreadSeats) -> Self {
        Self {
            count,
            seats,
            recycle: SharedRecycle::default(),
            pool: SharedPool::default(),
            task_send_pool: default_task_send_pool(),
        }
    }

    /// The most connections of the pool of each child.
    pub fn resolved_max_pool_size(&self) -> PoolConnectionCount {
        self.pool
            .resolved_max_size(shared_thread_pool_connections(self.seats.into()))
    }

    /// The pool that each child opens. Its seat threads share it.
    pub fn resolved_pool(&self) -> Result<ResolvedPool, PoolMinAboveMax> {
        self.pool
            .resolved(shared_thread_pool_connections(self.seats.into()))
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

impl IsolatedSetting {
    /// The children of this mode; each has one seat.
    pub fn child_count(&self) -> u32 {
        match self {
            Self::IsolatedChildren(children) => children.count.get(),
            Self::NoIsolatedChildren {} => 0,
        }
    }
}

impl SharedAsyncSetting {
    /// The children of this mode.
    pub fn child_count(&self) -> u32 {
        match self {
            Self::SharedAsyncChildren(children) => children.count.get(),
            Self::NoSharedAsyncChildren {} => 0,
        }
    }

    /// The seats of all children of this mode. At most 1024 × 1000, so the
    /// product fits `u32`.
    pub fn seat_count(&self) -> u32 {
        match self {
            Self::SharedAsyncChildren(children) => children.count.get() * children.seats.get(),
            Self::NoSharedAsyncChildren {} => 0,
        }
    }
}

impl SharedThreadSetting {
    /// The children of this mode.
    pub fn child_count(&self) -> u32 {
        match self {
            Self::SharedThreadChildren(children) => children.count.get(),
            Self::NoSharedThreadChildren {} => 0,
        }
    }

    /// The seats of all children of this mode. At most 1024 × 256, so the
    /// product fits `u32`.
    pub fn seat_count(&self) -> u32 {
        match self {
            Self::SharedThreadChildren(children) => children.count.get() * children.seats.get(),
            Self::NoSharedThreadChildren {} => 0,
        }
    }
}

/// Limits on the tasks that the worker claims.
///
/// The capacity of a worker is its seats plus `AppConfig.prefetch_buffer`:
/// it never holds more tasks than that. A limit above the capacity has no
/// effect, and validation refuses it.
///
/// - `per_queue_per_pass`: the most tasks that one claim pass takes from one
///   queue. With no limit, one pass fills the free capacity.
/// - `claimed_per_worker`: the most CLAIMED tasks (claimed, not yet running)
///   that the worker holds. With no limit, the capacity is the limit.
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

/// The connection pool of the worker process, the parent of the children.
/// It holds at most `max_size` connections and has no overflow
/// (`PostgresConfig::worker_process_config`). By default its size follows
/// the seats of the profile (`core::config::child_sizing::PARENT_POOL_RULE`).
/// AppConfig validation refuses a set size below
/// [`PARENT_POOL_MIN_CONNECTIONS`], which is also the floor of a derived
/// size.
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

impl ParentPool {
    /// The most connections of the pool. `from_seats` is the size that the
    /// profile derives from its seats.
    pub fn resolved(&self, from_seats: PoolConnectionCount) -> PoolConnectionCount {
        match self.max_size {
            PoolSizeSetting::PoolSize { connections } => connections,
            PoolSizeSetting::PoolSizeFromSeats {} => from_seats,
        }
    }
}

/// How long a stopping worker waits for running tasks: above 0 and at most
/// [`MAX_DRAIN_TIMEOUT`]. At this deadline it kills the children that still
/// run a task, and each of those tasks takes the crash path. The JSON form
/// is the serde form of `std::time::Duration`: `{"secs": 300, "nanos": 0}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DrainTimeout(Duration);

/// Why a duration is not a [`DrainTimeout`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DrainTimeoutRefused {
    #[error("drain_timeout must be above 0: a stopping worker needs a deadline")]
    Zero,
    #[error("drain_timeout of {0:?} is above the maximum of 24 hours")]
    AboveMaximum(Duration),
}

impl DrainTimeout {
    /// [`DEFAULT_DRAIN_TIMEOUT`].
    pub const DEFAULT: Self = Self(DEFAULT_DRAIN_TIMEOUT);

    /// The time to wait.
    pub const fn get(self) -> Duration {
        self.0
    }
}

impl TryFrom<Duration> for DrainTimeout {
    type Error = DrainTimeoutRefused;

    fn try_from(value: Duration) -> Result<Self, DrainTimeoutRefused> {
        match value {
            zero if zero.is_zero() => Err(DrainTimeoutRefused::Zero),
            above if above > MAX_DRAIN_TIMEOUT => Err(DrainTimeoutRefused::AboveMaximum(above)),
            within => Ok(Self(within)),
        }
    }
}

impl Serialize for DrainTimeout {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DrainTimeout {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Duration::deserialize(deserializer)?;
        Self::try_from(value).map_err(D::Error::custom)
    }
}

/// What a preload parent imports before it forks children. Linux only.
/// Elsewhere, and when a preload parent is down, children start with `exec`.
///
/// The default is `NoPreload`: the value of a profile that does not set
/// `preload` (`WorkerProfile::default()`, or a config text without the key).
/// A Rust child is an `exec` of the app's binary. The Python client does not
/// use this default: it writes the value of its own model (default
/// `library`) into each worker manifest, and the manifest has no default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildPreload {
    /// Library code only. Each child imports the app itself.
    Library,
    /// Library and app. Children share module state from the app's import.
    App,
    /// No preload parent. Every child starts with `exec`.
    #[default]
    #[serde(rename = "none")]
    NoPreload,
}

impl ChildPreload {
    /// The config value, as the JSON form writes it.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Library => "library",
            Self::App => "app",
            Self::NoPreload => "none",
        }
    }
}

/// Where the worker process runs.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    horsies_core_derive::ManifestSchema,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ParentCpu {
    /// The worker process and its children run on the CPUs they inherit.
    #[default]
    Shared,
    /// The worker process runs on one physical core, both SMT threads; the
    /// children run on the other CPUs that the worker may use. Linux only:
    /// the start fails on another platform or with fewer than 2 physical
    /// cores.
    OwnCore,
}

impl ParentCpu {
    /// The config value, as the JSON form writes it.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Shared => "SHARED",
            Self::OwnCore => "OWN_CORE",
        }
    }
}

/// The settings of one worker.
///
/// - `isolated`, `shared_async`, `shared_thread`: the children of each mode.
///   AppConfig validation requires at least one child.
/// - `pool`: the connection pool of the worker process.
/// - `claim`: the claim limits.
/// - `drain_timeout`: how long a stopping worker waits for running tasks.
///   Set it below the stop window of the supervisor.
/// - `preload`: what a Linux worker imports once before it forks children.
///   Default `NoPreload` ([`ChildPreload`]).
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
    pub drain_timeout: DrainTimeout,
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
            drain_timeout: default_drain_timeout(),
            preload: ChildPreload::default(),
            parent_cpu: ParentCpu::default(),
        }
    }
}

impl WorkerProfile {
    /// The number of children of the worker: the sum over the modes.
    pub fn child_count(&self) -> u32 {
        self.isolated.child_count()
            + self.shared_async.child_count()
            + self.shared_thread.child_count()
    }

    /// The tasks that the children of the worker run at once: one per
    /// isolated child, and `seats` per shared child.
    pub fn seat_count(&self) -> u32 {
        self.isolated.child_count()
            + self.shared_async.seat_count()
            + self.shared_thread.seat_count()
    }

    /// The most connections of the pool of the worker process: the resolved
    /// `pool.max_size`. `NonZeroU32::from` of it is the `parent_pool` of
    /// `PostgresConfig::worker_process_config`.
    pub fn resolved_parent_pool(&self) -> PoolConnectionCount {
        self.pool
            .resolved(parent_pool_connections(self.seat_count()))
    }
}

const DEFAULT_RECYCLE_AFTER_LIMIT: TaskCount =
    TaskCount::from_constant(DEFAULT_RECYCLE_AFTER_TASKS);
const DEFAULT_RECYCLE_AFTER_PER_SEAT_LIMIT: TaskCount =
    TaskCount::from_constant(DEFAULT_RECYCLE_AFTER_TASKS_PER_SEAT);
const DEFAULT_ISOLATED_POOL: PoolConnectionCount =
    PoolConnectionCount::from_constant(DEFAULT_ISOLATED_POOL_CONNECTIONS);
const DEFAULT_TASK_SEND_POOL: PoolConnectionCount =
    PoolConnectionCount::from_constant(DEFAULT_TASK_SEND_POOL_CONNECTIONS);

fn no_task_limit() -> TaskLimitSetting {
    TaskLimitSetting::NoTaskLimit {}
}

fn default_recycle_after() -> TaskLimitSetting {
    TaskLimitSetting::TaskLimit {
        tasks: DEFAULT_RECYCLE_AFTER_LIMIT,
    }
}

fn default_recycle_after_per_seat() -> TaskLimitSetting {
    TaskLimitSetting::TaskLimit {
        tasks: DEFAULT_RECYCLE_AFTER_PER_SEAT_LIMIT,
    }
}

fn no_memory_limit() -> MemoryLimitSetting {
    MemoryLimitSetting::NoMemoryLimit {}
}

fn no_open_connections() -> PoolMinSize {
    PoolMinSize::SMALLEST
}

fn default_isolated_pool_size() -> PoolSize {
    PoolSize::new(DEFAULT_ISOLATED_POOL)
}

fn default_task_send_pool() -> PoolSize {
    PoolSize::new(DEFAULT_TASK_SEND_POOL)
}

fn pool_size_from_seats() -> PoolSizeSetting {
    PoolSizeSetting::PoolSizeFromSeats {}
}

fn executor_threads_from_seats() -> ExecutorThreadsSetting {
    ExecutorThreadsSetting::ExecutorThreadsFromSeats {}
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

fn default_drain_timeout() -> DrainTimeout {
    DrainTimeout::DEFAULT
}

fn describe_task_limit(limit: TaskLimitSetting) -> String {
    match limit {
        TaskLimitSetting::TaskLimit { tasks } => format!("{} tasks", tasks.get()),
        TaskLimitSetting::NoTaskLimit {} => "no limit".to_owned(),
    }
}

fn describe_memory_limit(limit: MemoryLimitSetting) -> String {
    match limit {
        MemoryLimitSetting::MemoryLimit { mb } => format!("{} MB", mb.get()),
        MemoryLimitSetting::NoMemoryLimit {} => "no limit".to_owned(),
    }
}

/// Where a resolved size comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SizeOrigin {
    Profile,
    Seats,
}

impl SizeOrigin {
    fn of_pool_size(max_size: PoolSizeSetting) -> Self {
        match max_size {
            PoolSizeSetting::PoolSize { .. } => Self::Profile,
            PoolSizeSetting::PoolSizeFromSeats {} => Self::Seats,
        }
    }

    fn of_executor(executor: ExecutorThreadsSetting) -> Self {
        match executor {
            ExecutorThreadsSetting::ExecutorThreads { .. } => Self::Profile,
            ExecutorThreadsSetting::ExecutorThreadsFromSeats {} => Self::Seats,
        }
    }

    fn text(self) -> &'static str {
        match self {
            Self::Profile => "",
            Self::Seats => ", derived from seats",
        }
    }
}

/// The pool of one child, and where its max size comes from. The min size
/// is the set one, so a pool that validation refuses is still described.
fn describe_pool(
    min_size: PoolMinSize,
    max_size: PoolConnectionCount,
    check: bool,
    origin: SizeOrigin,
) -> String {
    let check = match check {
        true => "on",
        false => "off",
    };
    format!(
        "pool {} to {} connections{}, check {check}",
        min_size.get(),
        max_size.get(),
        origin.text(),
    )
}

fn describe_task_send_pool(task_send_pool: PoolSize) -> String {
    format!(
        "task_send_pool {} connections",
        task_send_pool.connections.get()
    )
}

fn describe_isolated(isolated: IsolatedSetting) -> String {
    match isolated {
        IsolatedSetting::IsolatedChildren(children) => format!(
            "isolated={} (recycle after {}, above {}; {}; {})",
            children.count.get(),
            describe_task_limit(children.recycle.after),
            describe_memory_limit(children.recycle.above),
            describe_pool(
                children.pool.min_size,
                children.pool.max_size.connections,
                children.pool.check,
                SizeOrigin::Profile,
            ),
            describe_task_send_pool(children.task_send_pool),
        ),
        IsolatedSetting::NoIsolatedChildren {} => "isolated=none".to_owned(),
    }
}

fn describe_shared_async(shared_async: SharedAsyncSetting) -> String {
    match shared_async {
        SharedAsyncSetting::SharedAsyncChildren(children) => format!(
            "shared_async={}x{} seats (recycle after {} per seat, above {}; {}; executor {} \
             threads{}; {})",
            children.count.get(),
            children.seats.get(),
            describe_task_limit(children.recycle.after_per_seat),
            describe_memory_limit(children.recycle.above),
            describe_pool(
                children.pool.min_size,
                children.resolved_max_pool_size(),
                children.pool.check,
                SizeOrigin::of_pool_size(children.pool.max_size),
            ),
            children.resolved_executor_threads().get(),
            SizeOrigin::of_executor(children.executor).text(),
            describe_task_send_pool(children.task_send_pool),
        ),
        SharedAsyncSetting::NoSharedAsyncChildren {} => "shared_async=none".to_owned(),
    }
}

fn describe_shared_thread(shared_thread: SharedThreadSetting) -> String {
    match shared_thread {
        SharedThreadSetting::SharedThreadChildren(children) => format!(
            "shared_thread={}x{} seats (recycle after {} per seat, above {}; {}; {})",
            children.count.get(),
            children.seats.get(),
            describe_task_limit(children.recycle.after_per_seat),
            describe_memory_limit(children.recycle.above),
            describe_pool(
                children.pool.min_size,
                children.resolved_max_pool_size(),
                children.pool.check,
                SizeOrigin::of_pool_size(children.pool.max_size),
            ),
            describe_task_send_pool(children.task_send_pool),
        ),
        SharedThreadSetting::NoSharedThreadChildren {} => "shared_thread=none".to_owned(),
    }
}

/// The pool of the worker process, and where its size comes from.
fn describe_parent_pool(profile: &WorkerProfile) -> String {
    let connections = profile.resolved_parent_pool().get();
    match SizeOrigin::of_pool_size(profile.pool.max_size) {
        SizeOrigin::Profile => format!("pool={connections} connections"),
        SizeOrigin::Seats => format!(
            "pool={connections} connections (derived from {} seats)",
            profile.seat_count(),
        ),
    }
}

/// One line that states every setting of a profile, for logs. The drain
/// timeout is in seconds, in the shortest decimal form.
pub fn describe_worker_profile(profile: &WorkerProfile) -> String {
    format!(
        "{}, {}, {}, {}, claim per queue per pass: {}, claimed per worker: {}, \
         drain_timeout={}s, preload={}, parent_cpu={}",
        describe_isolated(profile.isolated),
        describe_shared_async(profile.shared_async),
        describe_shared_thread(profile.shared_thread),
        describe_parent_pool(profile),
        describe_task_limit(profile.claim.per_queue_per_pass),
        describe_task_limit(profile.claim.claimed_per_worker),
        profile.drain_timeout.get().as_secs_f64(),
        profile.preload.as_str(),
        profile.parent_cpu.as_str(),
    )
}

/// The config path of a profile, as a user writes it in Python:
/// `worker_profiles['batch']`. A `\` or `'` in the name is escaped.
pub fn profile_path(name: &str) -> String {
    format!("worker_profiles[{}]", quoted_profile_name(name))
}

/// A profile name in single quotes, as Python's `repr` writes a name that
/// holds no `'`: `'batch'`. A `\` or `'` in the name is escaped.
pub fn quoted_profile_name(name: &str) -> String {
    let escaped = name.replace('\\', "\\\\").replace('\'', "\\'");
    format!("'{escaped}'")
}

fn profile_error(message: String, notes: &[String], help: String) -> HorsiesError {
    let refusal = HorsiesError::new(message).with_code(ErrorCode::ConfigInvalidWorkerProfile);
    notes
        .iter()
        .fold(refusal, |refusal, note| refusal.with_note(note.as_str()))
        .with_help(help)
}

/// A name that a `--profile` argument carries as it is: 1 to
/// [`PROFILE_NAME_MAX_LENGTH`] characters, letters, digits, `_`, `.` and
/// `-`, and the first is not `.` or `-`, so the name cannot read as a
/// command-line option. The CHECK `horsies_worker_states_profile_name_shape`
/// (`core/migrations/1004_worker_profile_name.sql`) holds the same rule.
pub(crate) fn is_valid_profile_name(name: &str) -> bool {
    let mut characters = name.chars();
    let first_is_valid = characters
        .next()
        .is_some_and(|first| first.is_ascii_alphanumeric() || first == '_');
    first_is_valid
        && name.len() <= PROFILE_NAME_MAX_LENGTH
        && characters.all(|rest| rest.is_ascii_alphanumeric() || matches!(rest, '_' | '.' | '-'))
}

/// Refuse a name that a `--profile` argument cannot carry as it is.
fn profile_name_error(name: &str) -> Option<HorsiesError> {
    match is_valid_profile_name(name) {
        true => None,
        false => Some(profile_error(
            format!("{}: the profile name is not valid", profile_path(name)),
            &[
                format!(
                    "a name has 1 to {PROFILE_NAME_MAX_LENGTH} characters: letters, digits, \
                     '_', '.' and '-'"
                ),
                "the first character is not '.' or '-'".to_owned(),
            ],
            "rename the profile, for example to \"default\" or \"batch-high-memory\"".to_owned(),
        )),
    }
}

/// Refuse a profile that starts no children.
fn profile_children_error(name: &str, profile: &WorkerProfile) -> Option<HorsiesError> {
    let path = profile_path(name);
    match profile.child_count() {
        0 => Some(profile_error(
            format!("{path}: the profile starts no children"),
            &["a worker needs at least one child to run a task".to_owned()],
            format!(
                "set {path}.isolated, {path}.shared_async or {path}.shared_thread, for \
                 example isolated=IsolatedChildren(count=4)"
            ),
        )),
        _ => None,
    }
}

/// Where the max size of a shared pool comes from, for a refusal.
fn shared_pool_size_note(pool: &SharedPool, seats: u32, max_size: PoolConnectionCount) -> String {
    match pool.max_size {
        PoolSizeSetting::PoolSize { connections } => {
            format!("max_size=PoolSize(connections={})", connections.get())
        }
        PoolSizeSetting::PoolSizeFromSeats {} => format!(
            "max_size=PoolSizeFromSeats() resolves to {} connections for {seats} seats",
            max_size.get(),
        ),
    }
}

/// The refusal of a pool whose `min_size` is above its resolved `max_size`.
fn pool_bounds_error(path: &str, refused: PoolMinAboveMax, size_note: String) -> HorsiesError {
    let (min_size, max_size) = (refused.min_size.get(), refused.max_size.get());
    profile_error(
        format!("{path}.min_size: {min_size} is above the pool max_size ({max_size})"),
        &[
            size_note,
            "a pool keeps min_size connections open and never opens more than max_size".to_owned(),
        ],
        format!(
            "set min_size to {max_size} or less, or set \
             max_size=PoolSize(connections={min_size}) or more"
        ),
    )
}

/// Refuse a child pool whose `min_size` is above its resolved `max_size`,
/// for each mode with children, in the order isolated, shared async, shared
/// thread.
fn profile_pool_errors(name: &str, profile: &WorkerProfile) -> Vec<HorsiesError> {
    let path = profile_path(name);
    let isolated = match profile.isolated {
        IsolatedSetting::IsolatedChildren(children) => {
            children.resolved_pool().err().map(|refused| {
                let note = format!(
                    "max_size=PoolSize(connections={})",
                    children.pool.max_size.connections.get()
                );
                pool_bounds_error(&format!("{path}.isolated.pool"), refused, note)
            })
        }
        IsolatedSetting::NoIsolatedChildren {} => None,
    };
    let shared_async = match profile.shared_async {
        SharedAsyncSetting::SharedAsyncChildren(children) => {
            children.resolved_pool().err().map(|refused| {
                let note =
                    shared_pool_size_note(&children.pool, children.seats.get(), refused.max_size);
                pool_bounds_error(&format!("{path}.shared_async.pool"), refused, note)
            })
        }
        SharedAsyncSetting::NoSharedAsyncChildren {} => None,
    };
    let shared_thread = match profile.shared_thread {
        SharedThreadSetting::SharedThreadChildren(children) => {
            children.resolved_pool().err().map(|refused| {
                let note =
                    shared_pool_size_note(&children.pool, children.seats.get(), refused.max_size);
                pool_bounds_error(&format!("{path}.shared_thread.pool"), refused, note)
            })
        }
        SharedThreadSetting::NoSharedThreadChildren {} => None,
    };
    [isolated, shared_async, shared_thread]
        .into_iter()
        .flatten()
        .collect()
}

/// Refuse a set pool of the worker process below
/// [`PARENT_POOL_MIN_CONNECTIONS`]. A size derived from the seats is at
/// least that minimum.
fn profile_parent_pool_error(name: &str, profile: &WorkerProfile) -> Option<HorsiesError> {
    match profile.pool.max_size {
        PoolSizeSetting::PoolSize { connections }
            if connections.get() < PARENT_POOL_MIN_CONNECTIONS =>
        {
            Some(profile_error(
                format!(
                    "{}.pool.max_size: {} connections is below the minimum of the pool of \
                     the worker process ({PARENT_POOL_MIN_CONNECTIONS})",
                    profile_path(name),
                    connections.get(),
                ),
                &[
                    format!(
                        "minimum = claim loop {PARENT_POOL_CLAIM_CONNECTIONS} + steady flushers \
                         of the batched success write {FINALIZE_BATCH_FLUSHERS} + reserve \
                         {PARENT_POOL_RESERVE}"
                    ),
                    "the claim loop and the steady flushers hold their connections while the \
                     worker is busy; the reserve is for the reaper, the claimer heartbeat and \
                     the writes of failures, retries and workflow nodes, which otherwise wait \
                     for the pool timeout and fail"
                        .to_owned(),
                ],
                format!(
                    "set max_size=PoolSize(connections={PARENT_POOL_MIN_CONNECTIONS}) or more, \
                     or max_size=PoolSizeFromSeats()"
                ),
            ))
        }
        PoolSizeSetting::PoolSize { .. } | PoolSizeSetting::PoolSizeFromSeats {} => None,
    }
}

/// The name, children, child pool and worker pool refusals of every profile,
/// in name order. Pure.
pub fn worker_profiles_shape_errors(
    profiles: &BTreeMap<String, WorkerProfile>,
) -> Vec<HorsiesError> {
    let mut errors = Vec::new();
    for (name, profile) in profiles {
        errors.extend(profile_name_error(name));
        errors.extend(profile_children_error(name, profile));
        errors.extend(profile_pool_errors(name, profile));
        errors.extend(profile_parent_pool_error(name, profile));
    }
    errors
}

/// The capacity of a worker of this profile, term by term.
fn capacity_formula(profile: &WorkerProfile, prefetch_buffer: u32) -> String {
    let mut terms = vec![format!(
        "{} isolated children",
        profile.isolated.child_count()
    )];
    match profile.shared_async {
        SharedAsyncSetting::SharedAsyncChildren(children) => terms.push(format!(
            "{} shared async children x {} seats",
            children.count.get(),
            children.seats.get(),
        )),
        SharedAsyncSetting::NoSharedAsyncChildren {} => {}
    }
    match profile.shared_thread {
        SharedThreadSetting::SharedThreadChildren(children) => terms.push(format!(
            "{} shared thread children x {} seats",
            children.count.get(),
            children.seats.get(),
        )),
        SharedThreadSetting::NoSharedThreadChildren {} => {}
    }
    terms.push(format!("prefetch_buffer {prefetch_buffer}"));
    format!("capacity = {}", terms.join(" + "))
}

/// Refuse a claim limit above the capacity of the worker: its seats plus
/// `prefetch_buffer`.
fn profile_claim_errors(
    name: &str,
    profile: &WorkerProfile,
    prefetch_buffer: u32,
) -> Vec<HorsiesError> {
    let capacity = u64::from(profile.seat_count()) + u64::from(prefetch_buffer);
    let claim_path = format!("{}.claim", profile_path(name));
    let limits = [
        ("per_queue_per_pass", profile.claim.per_queue_per_pass),
        ("claimed_per_worker", profile.claim.claimed_per_worker),
    ];
    let mut errors = Vec::new();
    for (field, limit) in limits {
        match limit {
            TaskLimitSetting::TaskLimit { tasks } if u64::from(tasks.get()) > capacity => {
                errors.push(profile_error(
                    format!(
                        "{claim_path}.{field}: {} tasks is above the capacity of the worker \
                         ({capacity})",
                        tasks.get(),
                    ),
                    &[
                        capacity_formula(profile, prefetch_buffer),
                        "the worker never holds more tasks than its capacity, so this limit \
                         has no effect"
                            .to_owned(),
                    ],
                    format!("set the limit to {capacity} or less, or use NoTaskLimit()"),
                ));
            }
            TaskLimitSetting::TaskLimit { .. } | TaskLimitSetting::NoTaskLimit {} => {}
        }
    }
    errors
}

/// The claim refusals of every profile with children, in name order. A
/// profile with no children is skipped: the shape check refuses it, and it
/// has no capacity to compare with. Pure.
pub fn worker_profiles_claim_errors(
    profiles: &BTreeMap<String, WorkerProfile>,
    prefetch_buffer: u32,
) -> Vec<HorsiesError> {
    profiles
        .iter()
        .filter(|(_, profile)| profile.child_count() >= 1)
        .flat_map(|(name, profile)| profile_claim_errors(name, profile, prefetch_buffer))
        .collect()
}

/// The profile that a worker runs, by its name in the profile set (Python
/// `SelectedProfile`, `clients/python/horsies/core/cli.py:425-430`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedProfile {
    pub name: String,
    pub profile: WorkerProfile,
}

/// The profile that a worker start asks for: the `--profile` value of
/// `horsies worker`.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, horsies_core_derive::ManifestSchema,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum ProfileRequest {
    /// The profile of this name.
    Named { name: String },
    /// No profile was named. There is no default profile.
    NotGiven,
}

/// Why no profile was selected (Python `ProfileNotGiven`, `ProfileUnknown`,
/// and the profile refusals of AppConfig validation).
#[derive(Debug, Clone)]
pub enum ProfileSelectionRefused {
    /// The profile set fails the checks of AppConfig validation: each
    /// refusal, in the order AppConfig validation reports them.
    InvalidProfiles { errors: Vec<HorsiesError> },
    /// The start named no profile. `defined` is every profile name, sorted.
    NotGiven { defined: Vec<String> },
    /// The start named a profile that the set does not have.
    Unknown {
        requested: String,
        defined: Vec<String>,
    },
}

impl ProfileSelectionRefused {
    /// The startup errors of the refusal, with Python's text
    /// (`clients/python/horsies/core/cli.py:467-518`, `_profile_selection_error`). Pure.
    pub fn errors(&self) -> Vec<HorsiesError> {
        match self {
            Self::InvalidProfiles { errors } => errors.clone(),
            Self::NotGiven { defined } => vec![HorsiesError::new("--profile is required")
                .with_code(ErrorCode::CliInvalidArgs)
                .with_note(
                    "a worker takes its settings from a named profile in \
                     AppConfig.worker_profiles; there is no default profile",
                )
                .with_note(defined_profiles_note(defined))
                .with_help(profile_help(defined))],
            Self::Unknown { requested, defined } => vec![HorsiesError::new(format!(
                "unknown worker profile {}",
                quoted_profile_name(requested)
            ))
            .with_code(ErrorCode::CliInvalidArgs)
            .with_note(defined_profiles_note(defined))
            .with_help(profile_help(defined))],
        }
    }
}

/// The note that names the defined profiles (Python `_defined_profiles_note`).
fn defined_profiles_note(defined: &[String]) -> String {
    match defined {
        [] => "the app defines no worker profiles: AppConfig.worker_profiles is empty".to_owned(),
        names => format!("defined profiles: {}", names.join(", ")),
    }
}

/// The help of a selection refusal (Python `_profile_help`).
fn profile_help(defined: &[String]) -> String {
    match defined {
        [] => "add a profile to the app config, for example:\n  \
               AppConfig(..., worker_profiles={\n      \
               'default': WorkerProfile(isolated=IsolatedChildren(count=4)),\n  \
               })\nthen start the worker with: horsies worker <app> --profile default"
            .to_owned(),
        [first, ..] => format!("start the worker with: horsies worker <app> --profile {first}"),
    }
}

/// Check every profile of `profiles` as AppConfig validation does (the
/// shape refusals, then the claim refusals with `prefetch_buffer`), then
/// pick the profile that `request` names (Python `select_worker_profile`,
/// `clients/python/horsies/core/cli.py:448-464`). Pure.
///
/// Errors: [`ProfileSelectionRefused`]: the refusals of the set, no
/// request, or a name that the set does not have.
pub fn select_worker_profile(
    profiles: &BTreeMap<String, WorkerProfile>,
    request: &ProfileRequest,
    prefetch_buffer: u32,
) -> Result<SelectedProfile, ProfileSelectionRefused> {
    let mut errors = worker_profiles_shape_errors(profiles);
    errors.extend(worker_profiles_claim_errors(profiles, prefetch_buffer));
    match errors.is_empty() {
        true => {}
        false => return Err(ProfileSelectionRefused::InvalidProfiles { errors }),
    }
    let defined: Vec<String> = profiles.keys().cloned().collect();
    match request {
        ProfileRequest::NotGiven => Err(ProfileSelectionRefused::NotGiven { defined }),
        ProfileRequest::Named { name } => match profiles.get(name) {
            Some(profile) => Ok(SelectedProfile {
                name: name.clone(),
                profile: *profile,
            }),
            None => Err(ProfileSelectionRefused::Unknown {
                requested: name.clone(),
                defined,
            }),
        },
    }
}

#[cfg(test)]
mod start_log_tests;

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use serde_json::{json, Value};

    use super::*;
    use crate::core::config::child_sizing::PARENT_POOL_CONNECTIONS_FLOOR;

    fn bounded<const MIN: u32, const MAX: u32>(value: u32) -> BoundedU32<MIN, MAX> {
        BoundedU32::try_from(value).expect("a test value is inside its bounds")
    }

    fn parsed(document: Value) -> WorkerProfile {
        serde_json::from_value(document).expect("the profile is valid")
    }

    fn refusal_text(document: Value) -> String {
        serde_json::from_value::<WorkerProfile>(document)
            .expect_err("the profile is refused")
            .to_string()
    }

    fn task_limit(tasks: u32) -> TaskLimitSetting {
        TaskLimitSetting::TaskLimit {
            tasks: bounded(tasks),
        }
    }

    fn isolated(count: u32) -> IsolatedSetting {
        IsolatedSetting::IsolatedChildren(IsolatedChildren::new(bounded(count)))
    }

    fn shared_async(count: u32, seats: u32) -> SharedAsyncSetting {
        SharedAsyncSetting::SharedAsyncChildren(SharedAsyncChildren::new(
            bounded(count),
            bounded(seats),
        ))
    }

    fn shared_thread(count: u32, seats: u32) -> SharedThreadSetting {
        SharedThreadSetting::SharedThreadChildren(SharedThreadChildren::new(
            bounded(count),
            bounded(seats),
        ))
    }

    fn isolated_profile(count: u32) -> WorkerProfile {
        WorkerProfile {
            isolated: isolated(count),
            ..WorkerProfile::default()
        }
    }

    fn named(entries: &[(&str, WorkerProfile)]) -> BTreeMap<String, WorkerProfile> {
        entries
            .iter()
            .map(|(name, profile)| ((*name).to_owned(), *profile))
            .collect()
    }

    fn messages(errors: &[HorsiesError]) -> Vec<&str> {
        errors.iter().map(|error| error.message.as_str()).collect()
    }

    /// The one refusal of a profile set, with its code checked.
    fn only_refusal(errors: Vec<HorsiesError>) -> HorsiesError {
        assert_eq!(errors.len(), 1, "{:?}", messages(&errors));
        let refusal = errors.into_iter().next().expect("one refusal");
        assert_eq!(refusal.code, Some(ErrorCode::ConfigInvalidWorkerProfile));
        refusal
    }

    /// Python `test_worker_profile.py`: `TestTaskLimitBounds`,
    /// `TestMemoryLimitBounds`, `TestIsolatedChildrenBounds`,
    /// `TestSharedAsyncChildrenBounds`, `TestSharedThreadChildrenBounds`,
    /// `TestPoolSizeBounds`, `TestExecutorThreadsBounds`,
    /// `TestChildPoolMinSizeBounds`.
    #[test]
    fn every_bounded_value_accepts_its_ends_and_refuses_one_step_out() {
        /// A bounded value: name, lowest, highest, and the profile that
        /// carries a value.
        type BoundCase = (&'static str, i64, i64, fn(i64) -> Value);
        let cases: [BoundCase; 12] = [
            (
                "isolated count",
                1,
                1024,
                |value| json!({"isolated": {"kind": "isolated_children", "count": value}}),
            ),
            (
                "shared async count",
                1,
                1024,
                |value| json!({"shared_async": {"kind": "shared_async_children", "count": value, "seats": 1}}),
            ),
            (
                "shared async seats",
                1,
                1000,
                |value| json!({"shared_async": {"kind": "shared_async_children", "count": 1, "seats": value}}),
            ),
            (
                "shared thread count",
                1,
                1024,
                |value| json!({"shared_thread": {"kind": "shared_thread_children", "count": value, "seats": 1}}),
            ),
            (
                "shared thread seats",
                1,
                256,
                |value| json!({"shared_thread": {"kind": "shared_thread_children", "count": 1, "seats": value}}),
            ),
            (
                "task limit",
                1,
                2_147_483_647,
                |value| json!({"claim": {"claimed_per_worker": {"kind": "task_limit", "tasks": value}}}),
            ),
            ("memory limit", 1, 16 * 1024 * 1024, |value| {
                json!({"isolated": {"kind": "isolated_children", "count": 1,
                    "recycle": {"above": {"kind": "memory_limit", "mb": value}}}})
            }),
            ("child pool size", 1, 1000, |value| {
                json!({"isolated": {"kind": "isolated_children", "count": 1,
                    "pool": {"max_size": {"kind": "pool_size", "connections": value}}}})
            }),
            ("child pool min size", 0, 1000, |value| {
                json!({"isolated": {"kind": "isolated_children", "count": 1,
                    "pool": {"min_size": value}}})
            }),
            ("task-send pool size", 1, 1000, |value| {
                json!({"shared_thread": {"kind": "shared_thread_children", "count": 1, "seats": 1,
                    "task_send_pool": {"connections": value}}})
            }),
            ("executor threads", 1, 256, |value| {
                json!({"shared_async": {"kind": "shared_async_children", "count": 1, "seats": 1,
                    "executor": {"kind": "executor_threads", "threads": value}}})
            }),
            (
                "worker pool size",
                1,
                1000,
                |value| json!({"pool": {"max_size": {"kind": "pool_size", "connections": value}}}),
            ),
        ];
        for (name, min, max, document) in cases {
            for accepted in [min, max] {
                assert!(
                    serde_json::from_value::<WorkerProfile>(document(accepted)).is_ok(),
                    "{name} {accepted} is accepted",
                );
            }
            for refused in [min - 1, max + 1] {
                assert!(
                    serde_json::from_value::<WorkerProfile>(document(refused)).is_err(),
                    "{name} {refused} is refused",
                );
            }
        }
        assert_eq!(
            (
                MAX_TASK_LIMIT,
                MAX_MEMORY_LIMIT_MB,
                MAX_CHILD_POOL_CONNECTIONS
            ),
            (2_147_483_647, 16_777_216, 1000),
        );
    }

    /// Python `test_worker_profile.py::TestDrainTimeoutBounds`.
    #[test]
    fn the_drain_timeout_is_above_zero_and_at_most_a_day() {
        let two_and_a_half_seconds =
            parsed(json!({"drain_timeout": {"secs": 2, "nanos": 500_000_000}}));
        assert_eq!(
            two_and_a_half_seconds.drain_timeout.get(),
            Duration::from_millis(2_500)
        );
        let day = parsed(json!({"drain_timeout": {"secs": 86_400, "nanos": 0}}));
        assert_eq!(day.drain_timeout.get(), MAX_DRAIN_TIMEOUT);

        let zero = refusal_text(json!({"drain_timeout": {"secs": 0, "nanos": 0}}));
        assert!(zero.contains("drain_timeout must be above 0"), "{zero}");
        let above = refusal_text(json!({"drain_timeout": {"secs": 86_400, "nanos": 1}}));
        assert!(above.contains("above the maximum of 24 hours"), "{above}");
        assert_eq!(
            DrainTimeout::try_from(Duration::ZERO),
            Err(DrainTimeoutRefused::Zero),
        );
    }

    /// Python `test_worker_profile.py::TestDefaults` and
    /// `::TestDrainTimeoutBounds::test_profile_defaults`, except `preload`:
    /// the core's default is `NoPreload` (a Rust child is an `exec`), and
    /// Python writes its own default, `library`, into each worker manifest.
    #[test]
    fn an_empty_profile_has_the_core_defaults() {
        let profile = parsed(json!({}));

        assert_eq!(profile, WorkerProfile::default());
        assert_eq!(profile.isolated, IsolatedSetting::NoIsolatedChildren {});
        assert_eq!(
            profile.shared_async,
            SharedAsyncSetting::NoSharedAsyncChildren {}
        );
        assert_eq!(
            profile.shared_thread,
            SharedThreadSetting::NoSharedThreadChildren {}
        );
        assert_eq!(profile.pool.max_size, PoolSizeSetting::PoolSizeFromSeats {});
        assert_eq!(
            profile.claim.per_queue_per_pass,
            TaskLimitSetting::NoTaskLimit {}
        );
        assert_eq!(
            profile.claim.claimed_per_worker,
            TaskLimitSetting::NoTaskLimit {}
        );
        assert_eq!(profile.drain_timeout.get(), Duration::from_secs(300));
        assert_eq!(profile.preload, ChildPreload::NoPreload);
        assert_eq!(profile.parent_cpu, ParentCpu::Shared);
    }

    /// Python `test_worker_profile.py::TestDefaults`, `::TestChildPoolDefaults`
    /// and `::TestTaskSendPool::test_each_kind_defaults_to_the_fixed_size`.
    #[test]
    fn each_child_kind_has_python_defaults() {
        let isolated_children = IsolatedChildren::new(bounded(1));
        assert_eq!(isolated_children.recycle.after, task_limit(100));
        assert_eq!(
            isolated_children.recycle.above,
            MemoryLimitSetting::NoMemoryLimit {}
        );
        assert_eq!(isolated_children.pool.min_size.get(), 0);
        assert_eq!(isolated_children.pool.max_size.connections.get(), 1);
        assert!(!isolated_children.pool.check);
        assert_eq!(isolated_children.task_send_pool.connections.get(), 2);
        assert_eq!(
            parsed(json!({"isolated": {"kind": "isolated_children", "count": 1}})).isolated,
            IsolatedSetting::IsolatedChildren(isolated_children),
        );

        let async_children = SharedAsyncChildren::new(bounded(1), bounded(40));
        assert_eq!(async_children.recycle.after_per_seat, task_limit(100));
        assert_eq!(
            async_children.recycle.above,
            MemoryLimitSetting::NoMemoryLimit {}
        );
        assert_eq!(async_children.pool, SharedPool::default());
        assert_eq!(
            async_children.pool.max_size,
            PoolSizeSetting::PoolSizeFromSeats {}
        );
        assert_eq!(
            async_children.executor,
            ExecutorThreadsSetting::ExecutorThreadsFromSeats {}
        );
        assert_eq!(async_children.task_send_pool.connections.get(), 2);

        let thread_children = SharedThreadChildren::new(bounded(1), bounded(40));
        assert_eq!(thread_children.recycle, SharedRecycle::default());
        assert_eq!(thread_children.pool, SharedPool::default());
        assert_eq!(thread_children.task_send_pool.connections.get(), 2);
    }

    /// Python `test_worker_profile.py::TestNamedVariants`,
    /// `::TestChildPoolVariants`, `::TestParentPool::test_the_size_parses_by_kind`
    /// and `::TestTaskSendPool::test_a_set_size_parses_from_a_dict`.
    #[test]
    fn each_choice_parses_by_kind() {
        let profile = parsed(json!({
            "isolated": {"kind": "isolated_children", "count": 2,
                "recycle": {"after": {"kind": "no_task_limit"},
                            "above": {"kind": "memory_limit", "mb": 512}},
                "task_send_pool": {"kind": "pool_size", "connections": 6}},
            "shared_async": {"kind": "shared_async_children", "count": 1, "seats": 4,
                "pool": {"max_size": {"kind": "pool_size", "connections": 3}},
                "executor": {"kind": "executor_threads", "threads": 8}},
            "shared_thread": {"kind": "no_shared_thread_children"},
            "pool": {"max_size": {"kind": "pool_size", "connections": 12}},
            "claim": {"per_queue_per_pass": {"kind": "task_limit", "tasks": 2}},
            "preload": "none",
            "parent_cpu": "OWN_CORE",
        }));

        let IsolatedSetting::IsolatedChildren(isolated_children) = profile.isolated else {
            panic!("isolated children parse by kind");
        };
        assert_eq!(
            isolated_children.recycle.after,
            TaskLimitSetting::NoTaskLimit {}
        );
        assert_eq!(
            isolated_children.recycle.above,
            MemoryLimitSetting::MemoryLimit { mb: bounded(512) },
        );
        assert_eq!(isolated_children.task_send_pool.connections.get(), 6);
        let SharedAsyncSetting::SharedAsyncChildren(async_children) = profile.shared_async else {
            panic!("shared async children parse by kind");
        };
        assert_eq!(
            async_children.pool.max_size,
            PoolSizeSetting::PoolSize {
                connections: bounded(3)
            },
        );
        assert_eq!(
            async_children.executor,
            ExecutorThreadsSetting::ExecutorThreads {
                threads: bounded(8)
            },
        );
        assert_eq!(
            profile.shared_thread,
            SharedThreadSetting::NoSharedThreadChildren {}
        );
        assert_eq!(profile.resolved_parent_pool().get(), 12);
        assert_eq!(profile.claim.per_queue_per_pass, task_limit(2));
        assert_eq!(profile.preload, ChildPreload::NoPreload);
        assert_eq!(profile.parent_cpu, ParentCpu::OwnCore);
        assert_eq!(parsed(json!({"preload": "app"})).preload, ChildPreload::App);

        let written = serde_json::to_value(profile).expect("a profile serializes");
        assert_eq!(parsed(written), profile);
    }

    /// Python `test_config_models_forbid_extra.py` for the profile models,
    /// `test_worker_profile.py::TestParentPool::test_an_unknown_field_is_refused`,
    /// `::TestChildPoolVariants::test_an_isolated_pool_has_no_size_from_seats`,
    /// `::test_a_shared_thread_child_has_no_executor_field` and
    /// `::TestTaskSendPool::test_a_size_from_seats_is_refused`.
    #[test]
    fn an_unknown_key_or_kind_is_refused_at_every_level() {
        let refused = [
            json!({"extra": 1}),
            json!({"isolated": {"kind": "isolated_children", "count": 1, "extra": 1}}),
            json!({"isolated": {"kind": "no_isolated_children", "extra": 1}}),
            json!({"claim": {"per_queue_per_pass": {"kind": "no_task_limit", "extra": 1}}}),
            json!({"claim": {"extra": 1}}),
            json!({"pool": {"max_overflow": 2}}),
            json!({"isolated": {"kind": "isolated_children", "count": 1,
                "pool": {"max_size": {"kind": "pool_size_from_seats"}}}}),
            json!({"shared_async": {"kind": "shared_async_children", "count": 1, "seats": 4,
                "task_send_pool": {"kind": "pool_size_from_seats"}}}),
            json!({"shared_thread": {"kind": "shared_thread_children", "count": 1, "seats": 4,
                "executor": {"kind": "executor_threads", "threads": 8}}}),
            json!({"isolated": {"kind": "shared_async_children", "count": 1, "seats": 1}}),
            json!({"preload": "everything"}),
            json!({"parent_cpu": "shared"}),
        ];
        for document in refused {
            assert!(
                serde_json::from_value::<WorkerProfile>(document.clone()).is_err(),
                "{document} is refused",
            );
        }
    }

    /// Python `test_worker_profile.py::TestIsolatedChildrenBounds::test_count_is_required`
    /// and `::TestSharedAsyncChildrenBounds::test_seats_are_required`.
    #[test]
    fn counts_and_seats_are_required() {
        for document in [
            json!({"isolated": {"kind": "isolated_children"}}),
            json!({"shared_async": {"kind": "shared_async_children", "count": 1}}),
            json!({"shared_thread": {"kind": "shared_thread_children", "count": 1}}),
        ] {
            let text = refusal_text(document);
            assert!(text.contains("missing field"), "{text}");
        }
    }

    /// Python `test_worker_profile.py::TestNamedVariants`: the child and seat
    /// counts.
    #[test]
    fn children_and_seats_are_summed_over_the_modes() {
        assert_eq!(isolated_profile(3).child_count(), 3);
        assert_eq!(isolated_profile(3).seat_count(), 3);
        assert_eq!(WorkerProfile::default().child_count(), 0);
        let mixed = WorkerProfile {
            isolated: isolated(2),
            shared_async: shared_async(3, 40),
            shared_thread: shared_thread(2, 16),
            ..WorkerProfile::default()
        };
        assert_eq!(mixed.child_count(), 7);
        assert_eq!(mixed.seat_count(), 2 + 120 + 32);
        let threads_only = WorkerProfile {
            shared_thread: shared_thread(2, 4),
            ..WorkerProfile::default()
        };
        assert_eq!(threads_only.child_count(), 2);
    }

    /// Python `test_worker_profile.py::TestChildPoolResolution`.
    #[test]
    fn child_pools_and_the_executor_resolve_from_their_seats_or_set_sizes() {
        for seats in [1, 9, 10, 11, 95, 1000] {
            let children = SharedAsyncChildren::new(bounded(1), bounded(seats));
            let derived = crate::core::config::child_sizing::shared_async_pool_connections(
                NonZeroU32::new(seats).expect("seats are at least 1"),
            );
            let pool = children.resolved_pool().expect("min_size 0 fits");
            assert_eq!(pool.max_size(), derived, "{seats} seats");
            assert_eq!(pool.min_size().get(), 0);
            assert!(!pool.check());
        }
        for seats in [1, 3, 4, 5, 39, 256] {
            let children = SharedThreadChildren::new(bounded(1), bounded(seats));
            let derived = crate::core::config::child_sizing::shared_thread_pool_connections(
                NonZeroU32::new(seats).expect("seats are at least 1"),
            );
            assert_eq!(children.resolved_max_pool_size(), derived, "{seats} seats");
        }

        let set_pool = SharedPool {
            max_size: PoolSizeSetting::PoolSize {
                connections: bounded(17),
            },
            ..SharedPool::default()
        };
        let mut few_seats = SharedAsyncChildren::new(bounded(1), bounded(1));
        few_seats.pool = set_pool;
        assert_eq!(few_seats.resolved_max_pool_size().get(), 17);
        let mut many_seats = SharedThreadChildren::new(bounded(1), bounded(200));
        many_seats.pool = set_pool;
        assert_eq!(many_seats.resolved_max_pool_size().get(), 17);

        assert_eq!(
            SharedAsyncChildren::new(bounded(1), bounded(33))
                .resolved_executor_threads()
                .get(),
            32,
        );
        let mut set_executor = SharedAsyncChildren::new(bounded(1), bounded(500));
        set_executor.executor = ExecutorThreadsSetting::ExecutorThreads {
            threads: bounded(3),
        };
        assert_eq!(set_executor.resolved_executor_threads().get(), 3);

        let mut isolated_children = IsolatedChildren::new(bounded(1));
        isolated_children.pool = IsolatedPool {
            min_size: bounded(2),
            max_size: PoolSize::new(bounded(5)),
            check: false,
        };
        let pool = isolated_children.resolved_pool().expect("2 is at most 5");
        assert_eq!((pool.min_size().get(), pool.max_size().get()), (2, 5));
    }

    /// A pool whose `min_size` is above its `max_size` does not resolve.
    #[test]
    fn a_pool_with_min_size_above_max_size_does_not_resolve() {
        assert_eq!(
            ResolvedPool::new(bounded(3), bounded(1), false),
            Err(PoolMinAboveMax {
                min_size: bounded(3),
                max_size: bounded(1),
            }),
        );
        assert!(ResolvedPool::new(bounded(2), bounded(2), true).is_ok());
    }

    /// Python `test_worker_profile.py::TestParentPool`.
    #[test]
    fn the_worker_pool_derives_from_every_seat_or_keeps_its_set_size() {
        let derived: Vec<u32> = [
            isolated_profile(64),
            WorkerProfile {
                shared_async: shared_async(1, 100),
                ..WorkerProfile::default()
            },
            WorkerProfile {
                shared_async: shared_async(1, 1000),
                ..WorkerProfile::default()
            },
            WorkerProfile {
                shared_thread: shared_thread(1, 256),
                ..WorkerProfile::default()
            },
        ]
        .iter()
        .map(|profile| profile.resolved_parent_pool().get())
        .collect();
        assert_eq!(derived, [7, 10, 20, 20]);
        assert_eq!(
            WorkerProfile::default().resolved_parent_pool().get(),
            PARENT_POOL_CONNECTIONS_FLOOR,
        );

        for (children, connections) in [(1, 3), (500, 3), (1, 1000)] {
            let mut profile = isolated_profile(children);
            profile.pool.max_size = PoolSizeSetting::PoolSize {
                connections: bounded(connections),
            };
            assert_eq!(profile.resolved_parent_pool().get(), connections);
        }
        assert_eq!(
            NonZeroU32::from(isolated_profile(8).resolved_parent_pool()).get(),
            5,
        );
    }

    /// Python `test_worker_profile.py::TestAppConfigProfileChecks::test_no_profiles_is_valid`
    /// and `::test_valid_profiles_are_kept`.
    #[test]
    fn valid_profiles_have_no_refusal() {
        let profiles = named(&[
            ("default", isolated_profile(4)),
            ("batch", isolated_profile(1)),
        ]);
        assert!(worker_profiles_shape_errors(&profiles).is_empty());
        assert!(worker_profiles_claim_errors(&profiles, 0).is_empty());
        assert!(worker_profiles_shape_errors(&BTreeMap::new()).is_empty());
    }

    /// Python `test_worker_profile.py::TestAppConfigProfileChecks::test_a_profile_with_no_children_names_the_profile`.
    #[test]
    fn a_profile_with_no_children_is_refused_by_name() {
        let refusal = only_refusal(worker_profiles_shape_errors(&named(&[(
            "empty",
            WorkerProfile::default(),
        )])));
        assert_eq!(
            refusal.message,
            "worker_profiles['empty']: the profile starts no children",
        );
        assert_eq!(
            refusal.notes,
            ["a worker needs at least one child to run a task"],
        );
        assert!(
            worker_profiles_claim_errors(&named(&[("empty", WorkerProfile::default())]), 0)
                .is_empty()
        );
    }

    /// Python `test_worker_profile.py::TestAppConfigProfileChecks::test_an_invalid_name_is_refused`
    /// and `::test_a_valid_name_is_accepted`.
    #[test]
    fn a_name_that_a_profile_argument_cannot_carry_is_refused() {
        let too_long = "x".repeat(PROFILE_NAME_MAX_LENGTH + 1);
        for name in [
            "",
            "has space",
            "-flag",
            ".hidden",
            "a/b",
            too_long.as_str(),
        ] {
            let refusal = only_refusal(worker_profiles_shape_errors(&named(&[(
                name,
                isolated_profile(1),
            )])));
            assert_eq!(
                refusal.message,
                format!("worker_profiles['{name}']: the profile name is not valid"),
            );
        }
        let longest = "x".repeat(PROFILE_NAME_MAX_LENGTH);
        for name in [
            "default",
            "batch-high-memory",
            "v1.2",
            "_internal",
            longest.as_str(),
        ] {
            assert!(
                worker_profiles_shape_errors(&named(&[(name, isolated_profile(1))])).is_empty(),
                "{name} is valid",
            );
        }
        assert_eq!(profile_path("it's"), "worker_profiles['it\\'s']");
    }

    /// Python `test_worker_profile.py::TestAppConfigPoolChecks::test_an_isolated_min_size_above_max_size_names_the_path`
    /// and `::test_a_min_size_equal_to_max_size_is_accepted`.
    #[test]
    fn an_isolated_pool_with_min_size_above_max_size_is_refused() {
        let mut profile = isolated_profile(1);
        if let IsolatedSetting::IsolatedChildren(children) = &mut profile.isolated {
            children.pool.min_size = bounded(3);
        }

        let refusal = only_refusal(worker_profiles_shape_errors(&named(&[("p", profile)])));

        assert_eq!(
            refusal.message,
            "worker_profiles['p'].isolated.pool.min_size: 3 is above the pool max_size (1)",
        );
        assert_eq!(
            refusal.notes,
            [
                "max_size=PoolSize(connections=1)",
                "a pool keeps min_size connections open and never opens more than max_size",
            ],
        );
        assert_eq!(
            refusal.help_text.as_deref(),
            Some("set min_size to 1 or less, or set max_size=PoolSize(connections=3) or more"),
        );

        if let IsolatedSetting::IsolatedChildren(children) = &mut profile.isolated {
            children.pool.min_size = bounded(1);
        }
        assert!(worker_profiles_shape_errors(&named(&[("p", profile)])).is_empty());
    }

    /// Python `test_worker_profile.py::TestAppConfigPoolChecks::test_a_shared_async_min_size_above_the_derived_size_names_the_seats`,
    /// `::test_a_shared_async_min_size_at_the_derived_size_is_accepted` and
    /// `::test_a_shared_thread_min_size_above_a_set_size_names_the_path`.
    #[test]
    fn a_shared_pool_with_min_size_above_max_size_is_refused() {
        let with_min = |min_size: u32| {
            let mut children = SharedAsyncChildren::new(bounded(1), bounded(1));
            children.pool.min_size = bounded(min_size);
            WorkerProfile {
                shared_async: SharedAsyncSetting::SharedAsyncChildren(children),
                ..WorkerProfile::default()
            }
        };
        let refusal = only_refusal(worker_profiles_shape_errors(&named(&[("p", with_min(3))])));
        assert_eq!(
            refusal.message,
            "worker_profiles['p'].shared_async.pool.min_size: 3 is above the pool max_size (2)",
        );
        assert_eq!(
            refusal.notes[0],
            "max_size=PoolSizeFromSeats() resolves to 2 connections for 1 seats",
        );
        assert!(worker_profiles_shape_errors(&named(&[("p", with_min(2))])).is_empty());

        let mut threads = SharedThreadChildren::new(bounded(1), bounded(4));
        threads.pool = SharedPool {
            min_size: bounded(5),
            max_size: PoolSizeSetting::PoolSize {
                connections: bounded(4),
            },
            check: false,
        };
        let profile = WorkerProfile {
            shared_thread: SharedThreadSetting::SharedThreadChildren(threads),
            ..WorkerProfile::default()
        };
        let refusal = only_refusal(worker_profiles_shape_errors(&named(&[("p", profile)])));
        assert_eq!(
            refusal.message,
            "worker_profiles['p'].shared_thread.pool.min_size: 5 is above the pool max_size (4)",
        );
        assert_eq!(refusal.notes[0], "max_size=PoolSize(connections=4)");
    }

    /// Python `test_worker_profile.py::TestAppConfigPoolChecks::test_each_mode_is_reported_at_once`.
    #[test]
    fn a_pool_refusal_of_each_mode_is_reported_at_once() {
        let set_pool = SharedPool {
            min_size: bounded(6),
            max_size: PoolSizeSetting::PoolSize {
                connections: bounded(5),
            },
            check: false,
        };
        let mut isolated_children = IsolatedChildren::new(bounded(1));
        isolated_children.pool.min_size = bounded(3);
        let mut async_children = SharedAsyncChildren::new(bounded(1), bounded(4));
        async_children.pool = set_pool;
        let mut thread_children = SharedThreadChildren::new(bounded(1), bounded(4));
        thread_children.pool = set_pool;
        let profile = WorkerProfile {
            isolated: IsolatedSetting::IsolatedChildren(isolated_children),
            shared_async: SharedAsyncSetting::SharedAsyncChildren(async_children),
            shared_thread: SharedThreadSetting::SharedThreadChildren(thread_children),
            ..WorkerProfile::default()
        };

        let errors = worker_profiles_shape_errors(&named(&[("p", profile)]));

        let paths: Vec<&str> = messages(&errors)
            .into_iter()
            .map(|message| message.split(':').next().unwrap_or_default())
            .collect();
        assert_eq!(
            paths,
            [
                "worker_profiles['p'].isolated.pool.min_size",
                "worker_profiles['p'].shared_async.pool.min_size",
                "worker_profiles['p'].shared_thread.pool.min_size",
            ],
        );
    }

    /// Python `test_worker_profile.py::TestAppConfigParentPoolChecks`.
    #[test]
    fn a_set_worker_pool_below_the_minimum_is_refused() {
        let with_pool = |max_size: PoolSizeSetting| {
            let mut profile = isolated_profile(1);
            profile.pool.max_size = max_size;
            named(&[("p", profile)])
        };

        let refusal = only_refusal(worker_profiles_shape_errors(&with_pool(
            PoolSizeSetting::PoolSize {
                connections: bounded(4),
            },
        )));

        assert_eq!(
            refusal.message,
            "worker_profiles['p'].pool.max_size: 4 connections is below the minimum of the \
             pool of the worker process (5)",
        );
        assert_eq!(
            refusal.notes,
            [
                "minimum = claim loop 1 + steady flushers of the batched success write 2 + \
                 reserve 2",
                "the claim loop and the steady flushers hold their connections while the \
                 worker is busy; the reserve is for the reaper, the claimer heartbeat and the \
                 writes of failures, retries and workflow nodes, which otherwise wait for the \
                 pool timeout and fail",
            ],
        );
        assert_eq!(
            refusal.help_text.as_deref(),
            Some("set max_size=PoolSize(connections=5) or more, or max_size=PoolSizeFromSeats()"),
        );
        assert!(
            worker_profiles_shape_errors(&with_pool(PoolSizeSetting::PoolSize {
                connections: bounded(5),
            }))
            .is_empty()
        );
        assert!(
            worker_profiles_shape_errors(&with_pool(PoolSizeSetting::PoolSizeFromSeats {}))
                .is_empty()
        );
    }

    /// Python `test_worker_profile.py::TestAppConfigProfileChecks::test_a_claim_limit_above_capacity_names_the_field_path`
    /// and `::test_a_claim_limit_equal_to_capacity_is_accepted`.
    #[test]
    fn a_claim_limit_above_the_capacity_is_refused_with_its_path() {
        for field in ["per_queue_per_pass", "claimed_per_worker"] {
            let with_limit = |tasks: u32| {
                let mut profile = isolated_profile(2);
                match field {
                    "per_queue_per_pass" => profile.claim.per_queue_per_pass = task_limit(tasks),
                    _ => profile.claim.claimed_per_worker = task_limit(tasks),
                }
                named(&[("small", profile)])
            };

            let refusal = only_refusal(worker_profiles_claim_errors(&with_limit(3), 0));

            assert_eq!(
                refusal.message,
                format!(
                    "worker_profiles['small'].claim.{field}: 3 tasks is above the capacity of \
                     the worker (2)"
                ),
            );
            assert_eq!(
                refusal.help_text.as_deref(),
                Some("set the limit to 2 or less, or use NoTaskLimit()"),
            );
            assert!(worker_profiles_claim_errors(&with_limit(2), 0).is_empty());
        }
    }

    /// Python `test_worker_profile.py::TestAppConfigProfileChecks::test_capacity_counts_the_seats_of_shared_children`,
    /// `::test_a_claim_limit_above_the_seats_names_each_term` and
    /// `::test_a_claim_limit_above_the_seats_names_the_thread_term`.
    #[test]
    fn the_capacity_counts_every_seat_and_names_each_term() {
        let split = |tasks: u32| {
            let mut profile = WorkerProfile {
                isolated: isolated(1),
                shared_async: shared_async(2, 10),
                ..WorkerProfile::default()
            };
            profile.claim.claimed_per_worker = task_limit(tasks);
            named(&[("split", profile)])
        };
        assert!(worker_profiles_claim_errors(&split(21), 0).is_empty());
        let refusal = only_refusal(worker_profiles_claim_errors(&split(22), 0));
        assert_eq!(
            refusal.message,
            "worker_profiles['split'].claim.claimed_per_worker: 22 tasks is above the capacity \
             of the worker (21)",
        );
        assert_eq!(
            refusal.notes[0],
            "capacity = 1 isolated children + 2 shared async children x 10 seats + \
             prefetch_buffer 0",
        );

        let mut with_threads = WorkerProfile {
            isolated: isolated(1),
            shared_async: shared_async(1, 10),
            shared_thread: shared_thread(2, 4),
            ..WorkerProfile::default()
        };
        with_threads.claim.claimed_per_worker = task_limit(20);
        let refusal = only_refusal(worker_profiles_claim_errors(
            &named(&[("split", with_threads)]),
            0,
        ));
        assert_eq!(
            refusal.notes[0],
            "capacity = 1 isolated children + 1 shared async children x 10 seats + 2 shared \
             thread children x 4 seats + prefetch_buffer 0",
        );
    }

    /// Python `test_worker_profile.py::TestAppConfigProfileChecks::test_capacity_includes_the_prefetch_buffer`
    /// and `::test_capacity_with_prefetch_buffer_is_still_a_bound`.
    #[test]
    fn the_capacity_includes_the_prefetch_buffer() {
        let soft = |tasks: u32| {
            let mut profile = isolated_profile(2);
            profile.claim.claimed_per_worker = task_limit(tasks);
            named(&[("soft", profile)])
        };
        assert!(worker_profiles_claim_errors(&soft(5), 3).is_empty());
        let refusal = only_refusal(worker_profiles_claim_errors(&soft(6), 3));
        assert!(
            refusal.message.contains("capacity of the worker (5)"),
            "{}",
            refusal.message,
        );
        assert!(worker_profiles_claim_errors(&soft(6), u32::MAX).is_empty());
    }

    /// Python `test_worker_profile.py::TestAppConfigProfileChecks::test_every_profile_is_reported_in_name_order`.
    #[test]
    fn every_profile_is_reported_in_name_order() {
        let mut zeta = isolated_profile(1);
        zeta.claim.per_queue_per_pass = task_limit(9);
        let profiles = named(&[
            ("zeta", zeta),
            ("alpha", WorkerProfile::default()),
            ("ok", isolated_profile(4)),
        ]);

        let mut errors = worker_profiles_shape_errors(&profiles);
        errors.extend(worker_profiles_claim_errors(&profiles, 0));

        assert_eq!(
            messages(&errors),
            [
                "worker_profiles['alpha']: the profile starts no children",
                "worker_profiles['zeta'].claim.per_queue_per_pass: 9 tasks is above the \
                 capacity of the worker (1)",
            ],
        );
    }

    /// Python `test_worker_profile.py::TestDescribeWorkerProfile::test_every_setting_is_named`
    /// and `::test_no_isolated_children`.
    #[test]
    fn the_description_names_every_setting() {
        let mut isolated_children = IsolatedChildren::new(bounded(8));
        isolated_children.recycle = IsolatedRecycle {
            after: TaskLimitSetting::NoTaskLimit {},
            above: MemoryLimitSetting::MemoryLimit { mb: bounded(1024) },
        };
        let mut profile = WorkerProfile {
            isolated: IsolatedSetting::IsolatedChildren(isolated_children),
            drain_timeout: DrainTimeout::try_from(Duration::from_secs(90))
                .expect("90 s is a drain timeout"),
            preload: ChildPreload::App,
            parent_cpu: ParentCpu::OwnCore,
            ..WorkerProfile::default()
        };
        profile.claim.per_queue_per_pass = task_limit(4);

        assert_eq!(
            describe_worker_profile(&profile),
            "isolated=8 (recycle after no limit, above 1024 MB; pool 0 to 1 connections, check \
             off; task_send_pool 2 connections), shared_async=none, shared_thread=none, pool=5 \
             connections (derived from 8 seats), claim per queue per pass: 4 tasks, claimed per \
             worker: no limit, drain_timeout=90s, preload=app, parent_cpu=OWN_CORE",
        );
        assert!(describe_worker_profile(&WorkerProfile::default()).starts_with("isolated=none, "));
    }

    /// Python `test_worker_profile.py::TestDescribeWorkerProfile::test_shared_async_children_are_named`,
    /// `::test_shared_thread_children_are_named` and
    /// `::test_the_parent_pool_names_the_seats_that_it_derives_from`.
    #[test]
    fn the_description_names_derived_sizes_and_their_seats() {
        let mut async_children = SharedAsyncChildren::new(bounded(3), bounded(25));
        async_children.recycle = SharedRecycle {
            after_per_seat: task_limit(40),
            above: MemoryLimitSetting::MemoryLimit { mb: bounded(2048) },
        };
        let async_profile = WorkerProfile {
            shared_async: SharedAsyncSetting::SharedAsyncChildren(async_children),
            ..WorkerProfile::default()
        };
        let described = describe_worker_profile(&async_profile);
        assert!(
            described.contains(
                "shared_async=3x25 seats (recycle after 40 tasks per seat, above 2048 MB; pool 0 \
                 to 3 connections, derived from seats, check off; executor 25 threads, derived \
                 from seats; task_send_pool 2 connections), "
            ),
            "{described}",
        );

        let mut thread_children = SharedThreadChildren::new(bounded(2), bounded(8));
        thread_children.recycle = SharedRecycle {
            after_per_seat: TaskLimitSetting::NoTaskLimit {},
            above: MemoryLimitSetting::MemoryLimit { mb: bounded(1024) },
        };
        let thread_profile = WorkerProfile {
            shared_thread: SharedThreadSetting::SharedThreadChildren(thread_children),
            ..WorkerProfile::default()
        };
        let described = describe_worker_profile(&thread_profile);
        assert!(
            described.contains(
                "shared_thread=2x8 seats (recycle after no limit per seat, above 1024 MB; pool 0 \
                 to 2 connections, derived from seats, check off; task_send_pool 2 connections), "
            ),
            "{described}",
        );

        let mixed = WorkerProfile {
            isolated: isolated(4),
            shared_async: shared_async(2, 100),
            ..WorkerProfile::default()
        };
        let described = describe_worker_profile(&mixed);
        assert!(
            described.contains(
                ", shared_thread=none, pool=20 connections (derived from 204 seats), claim per \
                 queue per pass: "
            ),
            "{described}",
        );
    }

    /// Python `test_worker_profile.py::TestDescribeWorkerProfile::test_set_sizes_are_named_without_their_origin`.
    #[test]
    fn the_description_names_set_sizes_without_their_origin() {
        let mut isolated_children = IsolatedChildren::new(bounded(1));
        isolated_children.pool = IsolatedPool {
            min_size: bounded(1),
            max_size: PoolSize::new(bounded(4)),
            check: false,
        };
        isolated_children.task_send_pool = PoolSize::new(bounded(3));
        let mut async_children = SharedAsyncChildren::new(bounded(1), bounded(50));
        async_children.pool = SharedPool {
            min_size: bounded(2),
            max_size: PoolSizeSetting::PoolSize {
                connections: bounded(9),
            },
            check: true,
        };
        async_children.executor = ExecutorThreadsSetting::ExecutorThreads {
            threads: bounded(7),
        };
        async_children.task_send_pool = PoolSize::new(bounded(5));
        let mut thread_children = SharedThreadChildren::new(bounded(1), bounded(6));
        thread_children.pool.max_size = PoolSizeSetting::PoolSize {
            connections: bounded(3),
        };
        thread_children.task_send_pool = PoolSize::new(bounded(1));
        let mut profile = WorkerProfile {
            isolated: IsolatedSetting::IsolatedChildren(isolated_children),
            shared_async: SharedAsyncSetting::SharedAsyncChildren(async_children),
            shared_thread: SharedThreadSetting::SharedThreadChildren(thread_children),
            ..WorkerProfile::default()
        };
        profile.pool.max_size = PoolSizeSetting::PoolSize {
            connections: bounded(11),
        };

        let described = describe_worker_profile(&profile);

        for part in [
            "isolated=1 (recycle after 100 tasks, above no limit; pool 1 to 4 connections, check \
             off; task_send_pool 3 connections), ",
            "shared_async=1x50 seats (recycle after 100 tasks per seat, above no limit; pool 2 \
             to 9 connections, check on; executor 7 threads; task_send_pool 5 connections), ",
            "shared_thread=1x6 seats (recycle after 100 tasks per seat, above no limit; pool 0 \
             to 3 connections, check off; task_send_pool 1 connections), ",
            ", pool=11 connections, claim per queue per pass: ",
        ] {
            assert!(described.contains(part), "{described}");
        }
    }
}
