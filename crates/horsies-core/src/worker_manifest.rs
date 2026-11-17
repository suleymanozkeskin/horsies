//! The typed manifest that a client passes to the core to run one worker.
//!
//! One manifest holds everything the worker needs: the database settings,
//! every worker profile of the app with the profile that the start asks for,
//! the queues and claim settings of the app, the recovery, retention,
//! resilience and payload settings of the worker's loops (`sections`), the
//! execution class of each registered task, and how a child and the preload
//! process start. The client resolves its user config (Pydantic in Python)
//! into it; the core picks and checks the profile
//! ([`select_worker_profile`]), so one resolver serves every client.
//!
//! As the producer manifest ([`crate::manifest`]), it holds values only: no
//! field has a default, and an unknown field is refused. A manifest that the
//! parser refuses is a defect of the client; the refusal names the field
//! path and never a value.
//!
//! The JSON form of each choice is an object with a `kind` key whose value is
//! the variant name in SCREAMING_SNAKE_CASE: `{"kind": "TASKS", "tasks": 100}`.
//! A duration is a whole number of milliseconds, except where a field name
//! gives another unit.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::core::config::bounded::BoundedU32;
use crate::core::config::socket::{TcpKeepalivesConfig, TcpUserTimeoutConfig};
use crate::core::config::worker_profile::{
    select_worker_profile, ChildPreload, ClaimLimits, DrainTimeout, ExecutorThreadCount,
    ExecutorThreadsSetting, IsolatedChildCount, IsolatedChildren, IsolatedPool, IsolatedRecycle,
    IsolatedSetting, MemoryLimitSetting, MemoryMb, ParentCpu, ParentPool, PoolConnectionCount,
    PoolMinSize, PoolSize, PoolSizeSetting, ProfileRequest, ProfileSelectionRefused,
    SelectedProfile, SharedAsyncChildCount, SharedAsyncChildren, SharedAsyncSeats,
    SharedAsyncSetting, SharedPool, SharedRecycle, SharedThreadChildCount, SharedThreadChildren,
    SharedThreadSeats, SharedThreadSetting, TaskCount, TaskLimitSetting, WorkerProfile,
    MAX_DRAIN_TIMEOUT, MAX_TASK_LIMIT,
};
use crate::core::task::Execution;
use crate::core::PostgresConfig;
use crate::manifest::{
    parse_manifest, DatabaseUrl, ManifestRefused, PoolRecycle, SendManifest, SessionDatabase,
    WholeSeconds,
};
use crate::manifest_schema::{IntBound, ManifestValue, PythonValue};

/// The one worker manifest version this core reads.
pub const WORKER_MANIFEST_VERSION: u32 = 1;

/// The longest drain timeout, in milliseconds: [`MAX_DRAIN_TIMEOUT`].
pub const MAX_DRAIN_TIMEOUT_MS: u64 = MAX_DRAIN_TIMEOUT.as_millis() as u64;

/// The lowest priority of a custom queue (Python `CustomQueueConfig.priority`,
/// 1 first and 100 last).
pub const MAX_QUEUE_PRIORITY: u32 = 100;

/// A count of the manifest: the bounded config type `$inner` as a Python
/// `int`, with the name `$bound` of its Python constants. The bounds are the
/// ones of `$inner`, so they have one definition.
macro_rules! bounded_count {
    ($(#[$attr:meta])* $name:ident($inner:ty) as $bound:literal) => {
        $(#[$attr])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub $inner);

        impl ManifestValue for $name {
            const PYTHON: PythonValue = PythonValue::Int(IntBound {
                name: $bound,
                min: <$inner>::SMALLEST.get() as u64,
                max: <$inner>::LARGEST.get() as u64,
            });
        }
    };
}

bounded_count!(
    /// A task count of a limit: 1 to `MAX_TASK_LIMIT`.
    LimitTasks(TaskCount) as "TASK_LIMIT"
);
bounded_count!(
    /// A memory limit of one child in MB.
    LimitMb(MemoryMb) as "MEMORY_LIMIT_MB"
);
bounded_count!(
    /// Isolated children of a profile.
    IsolatedCount(IsolatedChildCount) as "ISOLATED_CHILDREN"
);
bounded_count!(
    /// Shared async children of a profile.
    SharedAsyncCount(SharedAsyncChildCount) as "SHARED_ASYNC_CHILDREN"
);
bounded_count!(
    /// Seats of one shared async child.
    SharedAsyncSeatCount(SharedAsyncSeats) as "SHARED_ASYNC_SEATS"
);
bounded_count!(
    /// Shared thread children of a profile.
    SharedThreadCount(SharedThreadChildCount) as "SHARED_THREAD_CHILDREN"
);
bounded_count!(
    /// Seats of one shared thread child.
    SharedThreadSeatCount(SharedThreadSeats) as "SHARED_THREAD_SEATS"
);
bounded_count!(
    /// The most connections of a pool of one process.
    ChildPoolConnections(PoolConnectionCount) as "CHILD_POOL_CONNECTIONS"
);
bounded_count!(
    /// Connections that a child pool keeps open.
    ChildPoolMinSize(PoolMinSize) as "CHILD_POOL_MIN_SIZE"
);
bounded_count!(
    /// Threads of the default executor of a shared async child.
    DefaultExecutorThreadCount(ExecutorThreadCount) as "DEFAULT_EXECUTOR_THREADS"
);
bounded_count!(
    /// A count that the claim function takes as a Postgres `integer`: 0 to
    /// `MAX_TASK_LIMIT`.
    ClaimCount(BoundedU32<0, MAX_TASK_LIMIT>) as "CLAIM_COUNT"
);
bounded_count!(
    /// The most RUNNING tasks of one queue: 0 to `MAX_TASK_LIMIT`; 0 pauses
    /// the claims of the queue.
    QueueConcurrencyLimit(BoundedU32<0, MAX_TASK_LIMIT>) as "QUEUE_CONCURRENCY"
);
bounded_count!(
    /// The priority of a custom queue: 1 is claimed first, 100 last.
    QueuePriority(BoundedU32<1, MAX_QUEUE_PRIORITY>) as "QUEUE_PRIORITY"
);
bounded_count!(
    /// A duration above zero in whole milliseconds that fits the core's
    /// `u32` config fields.
    Milliseconds(BoundedU32<1, { u32::MAX }>) as "MILLISECONDS"
);

mod sections;

pub use sections::{
    AuditIntervalMs, CheckIntervalMs, ClaimedStaleThresholdMs, DeleteBatchSize,
    HeartbeatIntervalMs, HeartbeatLeafHorizonHours, HistoryLeafHorizonDays,
    MaintenanceIntervalSeconds, PausedAgeMicros, PausedWorkflowExpiry, PayloadBytes, PayloadLimit,
    PayloadManifest, PollIntervalMs, QueueRetention, RecoveryGraceMs, RecoveryManifest,
    RecoveryPassLimit, ResilienceManifest, RetentionManifest, RetentionPeriodDays,
    RetryAttemptCount, RetryAttemptLimit, RetryInitialMs, RetryMaxMs, SnapshotIntervalMs,
    StaleThresholdMs, SweepIntervalSeconds, TerminalRecordRetention, TerminalRecordRetentionHours,
    WorkerStateRetention, WorkerStateRetentionHours, MAX_PAUSED_AGE_MICROS, MAX_PAYLOAD_BYTES,
};

/// The manifest version. Only the parser constructs it, and it accepts only
/// [`WORKER_MANIFEST_VERSION`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct WorkerManifestVersion(u32);

impl WorkerManifestVersion {
    /// The version this core reads.
    pub const CURRENT: Self = Self(WORKER_MANIFEST_VERSION);

    pub const fn get(self) -> u32 {
        self.0
    }
}

impl TryFrom<u32> for WorkerManifestVersion {
    type Error = String;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            WORKER_MANIFEST_VERSION => Ok(Self(value)),
            other => Err(format!(
                "worker manifest version {other} is not supported; this core reads version \
                 {WORKER_MANIFEST_VERSION}"
            )),
        }
    }
}

impl From<WorkerManifestVersion> for u32 {
    fn from(version: WorkerManifestVersion) -> Self {
        version.0
    }
}

impl ManifestValue for WorkerManifestVersion {
    const PYTHON: PythonValue = PythonValue::Int(IntBound {
        name: "MANIFEST_VERSION",
        min: WORKER_MANIFEST_VERSION as u64,
        max: WORKER_MANIFEST_VERSION as u64,
    });
}

/// How long a stopping worker waits for running tasks, in milliseconds:
/// 1 to [`MAX_DRAIN_TIMEOUT_MS`] (Python `WorkerProfile.drain_timeout`,
/// above 0 and at most 24 hours).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct DrainTimeoutMs(u64);

impl DrainTimeoutMs {
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The drain timeout of the config.
    pub fn drain_timeout(self) -> DrainTimeout {
        match DrainTimeout::try_from(Duration::from_millis(self.0)) {
            Ok(timeout) => timeout,
            Err(refused) => unreachable!(
                "a parsed drain timeout of {} ms is above 0 and at most 24 hours: {refused}",
                self.0
            ),
        }
    }
}

impl TryFrom<u64> for DrainTimeoutMs {
    type Error = String;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            1..=MAX_DRAIN_TIMEOUT_MS => Ok(Self(value)),
            _ => Err(format!(
                "a drain timeout must be from 1 to {MAX_DRAIN_TIMEOUT_MS} milliseconds"
            )),
        }
    }
}

impl From<DrainTimeoutMs> for u64 {
    fn from(timeout: DrainTimeoutMs) -> Self {
        timeout.0
    }
}

impl ManifestValue for DrainTimeoutMs {
    const PYTHON: PythonValue = PythonValue::Int(IntBound {
        name: "DRAIN_TIMEOUT_MS",
        min: 1,
        max: MAX_DRAIN_TIMEOUT_MS,
    });
}

/// The manifest of a worker process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkerManifest {
    /// Always `WORKER_MANIFEST_VERSION`, the one version this core reads.
    pub manifest_version: WorkerManifestVersion,
    pub database: WorkerDatabaseManifest,
    pub send: SendManifest,
    /// Every worker profile of the app, by name. The core checks all of them,
    /// as AppConfig validation does, not only the one that the worker runs.
    pub profiles: BTreeMap<String, WorkerProfileManifest>,
    /// The profile that the start asks for (`horsies worker --profile`).
    pub profile: ProfileRequest,
    pub claim: WorkerClaimManifest,
    pub recovery: RecoveryManifest,
    pub retention: RetentionManifest,
    pub resilience: ResilienceManifest,
    pub payload: PayloadManifest,
    /// The execution class of each task that the worker registered, by task
    /// name.
    pub tasks: BTreeMap<String, Execution>,
    pub child_launch: ChildLaunchManifest,
}

/// The connection settings of a worker. The pool of the worker process
/// holds the resolved pool of its profile with no overflow; the size and the
/// overflow of the app's pool do not apply to a worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkerDatabaseManifest {
    /// The URL of the worker process and of each child.
    pub database_url: DatabaseUrl,
    pub session_database: SessionDatabase,
    /// The main URL points at a PgBouncer transaction-pool endpoint.
    pub pgbouncer_transaction_mode: bool,
    /// Ping a pooled connection before use in every pool but the pool of the
    /// worker process, for example the pool that task code in a child sends
    /// through (`PostgresConfig.pool_pre_ping`).
    pub pool_pre_ping: bool,
    /// The same rule for the pool of the worker process
    /// (`PostgresConfig.worker_pool_pre_ping`).
    pub worker_pool_pre_ping: bool,
    /// The longest a checkout waits for a free connection; 0 does not wait.
    pub pool_timeout: WholeSeconds,
    pub pool_recycle: PoolRecycle,
    /// Log every statement through the log bridge.
    pub echo: bool,
    /// TCP keepalives of every core connection (Python `tcp_keepalives` and
    /// its three values).
    pub tcp_keepalives: TcpKeepalivesConfig,
    /// TCP user timeout of every core connection (Python
    /// `tcp_user_timeout_ms`; its 0 is `SYSTEM_DEFAULT`).
    pub tcp_user_timeout: TcpUserTimeoutConfig,
}

/// A limit on a number of tasks.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum TaskLimit {
    /// A limit of `tasks` tasks.
    Tasks { tasks: LimitTasks },
    /// No limit on the number of tasks.
    NoLimit,
}

/// A limit on the resident memory (RSS) of a child.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum MemoryLimit {
    /// A limit of `mb` MB of RSS.
    Megabytes { mb: LimitMb },
    /// No limit on the RSS of a child.
    NoLimit,
}

/// The most connections of a pool whose size can follow the seats.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum PoolMaxSize {
    /// A pool of at most `connections` connections.
    Connections { connections: ChildPoolConnections },
    /// The size follows the seats (`core::config::child_sizing`).
    FromSeats,
}

/// The threads of the default executor of a shared async child.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum DefaultExecutor {
    /// A default executor of `threads` threads.
    Threads { threads: DefaultExecutorThreadCount },
    /// The threads follow the seats of the child (`core::config::child_sizing`).
    FromSeats,
}

/// When the worker replaces an isolated child with a new one.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(deny_unknown_fields)]
pub struct IsolatedRecycleManifest {
    /// Replace a child after it completes this many tasks.
    pub after: TaskLimit,
    /// Replace a child when its own RSS reaches this limit.
    pub above: MemoryLimit,
}

/// When the worker replaces a shared child with a new one.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(deny_unknown_fields)]
pub struct SharedRecycleManifest {
    /// Replace a child after it completes this many tasks per seat.
    pub after_per_seat: TaskLimit,
    /// Replace a child when its own RSS reaches this limit.
    pub above: MemoryLimit,
}

/// The connection pool that each isolated child opens.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(deny_unknown_fields)]
pub struct IsolatedPoolManifest {
    /// Connections that the pool keeps open.
    pub min_size: ChildPoolMinSize,
    /// The most connections of the pool.
    pub max_size: ChildPoolConnections,
    /// Check each connection when a write takes it from the pool.
    pub check: bool,
}

/// The connection pool that each shared child opens. Its seats share it.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(deny_unknown_fields)]
pub struct SharedPoolManifest {
    /// Connections that the pool keeps open.
    pub min_size: ChildPoolMinSize,
    pub max_size: PoolMaxSize,
    /// Check each connection when a write takes it from the pool.
    pub check: bool,
}

/// The isolated children of a profile.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum Isolated {
    /// `count` children; each runs one task at a time in its own process.
    /// Task code that sends opens a pool of up to `task_send_pool`
    /// connections in its child.
    Children {
        count: IsolatedCount,
        recycle: IsolatedRecycleManifest,
        pool: IsolatedPoolManifest,
        task_send_pool: ChildPoolConnections,
    },
    /// The profile starts no isolated children.
    NoChildren,
}

/// The shared async children of a profile.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum SharedAsync {
    /// `count` children; each runs up to `seats` async tasks at once on one
    /// event loop.
    Children {
        count: SharedAsyncCount,
        seats: SharedAsyncSeatCount,
        recycle: SharedRecycleManifest,
        pool: SharedPoolManifest,
        executor: DefaultExecutor,
        task_send_pool: ChildPoolConnections,
    },
    /// The profile starts no shared async children.
    NoChildren,
}

/// The shared thread children of a profile.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum SharedThread {
    /// `count` children; each runs up to `seats` blocking tasks at once, each
    /// on a thread of its own.
    Children {
        count: SharedThreadCount,
        seats: SharedThreadSeatCount,
        recycle: SharedRecycleManifest,
        pool: SharedPoolManifest,
        task_send_pool: ChildPoolConnections,
    },
    /// The profile starts no shared thread children.
    NoChildren,
}

/// The connection pool of the worker process, with no overflow.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(deny_unknown_fields)]
pub struct ParentPoolManifest {
    pub max_size: PoolMaxSize,
}

/// Limits on the tasks that the worker claims.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(deny_unknown_fields)]
pub struct ClaimLimitsManifest {
    /// The most tasks that one claim pass takes from one queue.
    pub per_queue_per_pass: TaskLimit,
    /// The most CLAIMED tasks that the worker holds.
    pub claimed_per_worker: TaskLimit,
}

/// What a preload parent imports before it forks children (Linux only).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Preload {
    /// Library code only. Each child imports the app itself.
    Library,
    /// Library and app. Children share module state from the app's import.
    App,
    /// No preload parent. Every child starts with `exec`.
    NoPreload,
}

/// The settings of one worker (Python `WorkerProfile`), every value given.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(deny_unknown_fields)]
pub struct WorkerProfileManifest {
    pub isolated: Isolated,
    pub shared_async: SharedAsync,
    pub shared_thread: SharedThread,
    /// The connection pool of the worker process.
    pub pool: ParentPoolManifest,
    pub claim: ClaimLimitsManifest,
    /// How long a stopping worker waits for running tasks, in milliseconds.
    pub drain_timeout_ms: DrainTimeoutMs,
    pub preload: Preload,
    pub parent_cpu: ParentCpu,
}

/// The most RUNNING tasks of one custom queue.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum QueueConcurrency {
    /// At most `tasks` RUNNING tasks; 0 pauses the claims of the queue.
    Tasks { tasks: QueueConcurrencyLimit },
    /// No limit of the queue; the worker and cluster limits still apply.
    NoLimit,
}

/// One custom queue of the app (Python `CustomQueueConfig`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema)]
#[serde(deny_unknown_fields)]
pub struct CustomQueueManifest {
    pub name: String,
    pub priority: QueuePriority,
    pub max_concurrency: QueueConcurrency,
}

/// The queues of the app (Python `AppConfig.queue_mode`). A worker serves
/// every queue of the app.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum Queues {
    /// The custom queues, in the order of the config.
    Custom { queues: Vec<CustomQueueManifest> },
    /// The one queue `default`.
    Default,
}

/// The lease of a claim (Python `AppConfig.claim_lease_ms`).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum ClaimLease {
    /// The app sets no lease: the worker uses its default lease.
    WorkerDefault,
    /// A lease of `ms` milliseconds.
    Milliseconds { ms: Milliseconds },
}

/// The most RUNNING tasks of the cluster (Python `AppConfig.cluster_wide_cap`).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum ClusterWideCap {
    /// At most `tasks` RUNNING tasks over every worker.
    Tasks { tasks: LimitTasks },
    /// No limit of the cluster.
    NoLimit,
}

/// The queue and claim settings of the app, which every worker of it shares.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkerClaimManifest {
    pub queues: Queues,
    /// Tasks that a worker claims past its seats (`AppConfig.prefetch_buffer`);
    /// 0 is the hard cap mode.
    pub prefetch_buffer: ClaimCount,
    pub claim_lease: ClaimLease,
    /// The oldest CLAIMED task whose lease the worker renews, in milliseconds
    /// (`AppConfig.max_claim_renew_age_ms`).
    pub max_claim_renew_age_ms: Milliseconds,
    pub cluster_wide_cap: ClusterWideCap,
}

/// The program that starts each child and its first arguments, the
/// interpreter flags included. The core appends the descriptor number of
/// the child's socket and the worker pid.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema)]
#[serde(deny_unknown_fields)]
pub struct ChildCommandManifest {
    pub program: String,
    pub args: Vec<String>,
}

/// How the preload process of a worker starts (Linux). A profile whose
/// `preload` is `LIBRARY` or `APP` runs it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum PreloadLaunch {
    /// The program and its first arguments. The core appends the descriptor
    /// number of the preload's socket, the worker pid and the preload mode
    /// (`library` or `app`).
    Command { command: ChildCommandManifest },
    /// The child program has no preload process: every child starts with
    /// `exec`.
    NoPreloadProgram,
}

/// What a child runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum ChildProgram {
    /// A Python child: it imports the app before it reports ready.
    /// `app_locator` is `module:attribute` or a file path; `imports` are the
    /// task modules; `sys_path_roots` go first on `sys.path`;
    /// `parent_sys_path` and `parent_argv` are the `sys.path` and `sys.argv`
    /// of the worker process.
    Python {
        app_locator: String,
        imports: Vec<String>,
        sys_path_roots: Vec<String>,
        parent_sys_path: Vec<String>,
        parent_argv: Vec<String>,
    },
    /// The worker binary in child mode; its tasks are compiled in.
    Rust,
}

/// The log level of the worker and its children (`horsies worker --loglevel`).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkerLogLevel {
    Debug,
    Info,
    Warning,
    Error,
    Critical,
}

/// How each child of the worker starts. The children connect with the
/// `database_url` of the database section.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema)]
#[serde(deny_unknown_fields)]
pub struct ChildLaunchManifest {
    pub command: ChildCommandManifest,
    pub preload: PreloadLaunch,
    pub program: ChildProgram,
    pub log_level: WorkerLogLevel,
}

/// Parse a worker manifest from its JSON text.
///
/// Pure: no I/O. Refuses unknown fields, missing fields, values outside
/// their bounds and every manifest version but [`WORKER_MANIFEST_VERSION`].
/// It does not check the profiles: [`WorkerManifest::selected_profile`]
/// does.
pub fn parse_worker_manifest(text: &str) -> Result<WorkerManifest, ManifestRefused> {
    parse_manifest(text)
}

/// Parse a set of worker profiles from its JSON text: an object of
/// [`WorkerProfileManifest`] by profile name, the form of the `profiles`
/// member of a worker manifest. The Python live check sends it
/// (`check_handle_worker_connections`); the worker manifest that holds the
/// same profiles is not built for a check.
///
/// Pure: no I/O. Refuses unknown fields, missing fields and values outside
/// their bounds, as [`parse_worker_manifest`] does for the member. It does
/// not check the set: AppConfig validation did.
pub fn parse_worker_profiles(
    text: &str,
) -> Result<BTreeMap<String, WorkerProfile>, ManifestRefused> {
    let manifests: BTreeMap<String, WorkerProfileManifest> = parse_manifest(text)?;
    Ok(manifests
        .iter()
        .map(|(name, profile)| (name.clone(), profile.to_profile()))
        .collect())
}

impl WorkerManifest {
    /// Every profile of the manifest as the config type, by name. Pure.
    pub fn profile_set(&self) -> BTreeMap<String, WorkerProfile> {
        self.profiles
            .iter()
            .map(|(name, profile)| (name.clone(), profile.to_profile()))
            .collect()
    }

    /// The profile that the worker runs: the profile set is checked as
    /// AppConfig validation checks it, then the request picks one
    /// ([`select_worker_profile`]). Pure.
    ///
    /// Errors: [`ProfileSelectionRefused`], which names every refusal of the
    /// set, a missing request or an unknown name.
    pub fn selected_profile(&self) -> Result<SelectedProfile, ProfileSelectionRefused> {
        select_worker_profile(
            &self.profile_set(),
            &self.profile,
            self.claim.prefetch_buffer.0.get(),
        )
    }

    /// The broker config of the worker process that runs `selected`: a pool
    /// of the resolved worker pool with no overflow and the worker pre-ping
    /// (`PostgresConfig::worker_process_config`). Pure.
    pub fn worker_process_config(&self, selected: &SelectedProfile) -> PostgresConfig {
        self.app_broker_config(selected)
            .worker_process_config(NonZeroU32::from(selected.profile.resolved_parent_pool()))
    }

    /// The broker config of the app's connection settings, with the
    /// resolved worker pool of `selected` and no overflow, and the pre-ping
    /// of every pool but the worker's (`PostgresConfig.pool_pre_ping`). The
    /// children's connection settings and the runner heartbeat pool read it.
    /// Pure.
    pub fn app_broker_config(&self, selected: &SelectedProfile) -> PostgresConfig {
        let database = &self.database;
        let session_database_url = match &database.session_database {
            SessionDatabase::SameAsDatabaseUrl => None,
            SessionDatabase::Separate { url } => Some(url.expose().to_owned()),
        };
        PostgresConfig {
            database_url: database.database_url.expose().to_owned(),
            session_database_url,
            pgbouncer_transaction_mode: database.pgbouncer_transaction_mode,
            pool_pre_ping: database.pool_pre_ping,
            worker_pool_pre_ping: database.worker_pool_pre_ping,
            // `worker_process_config` replaces both with the worker pool.
            pool_size: selected.profile.resolved_parent_pool().get(),
            max_overflow: 0,
            retain_rerun_input_default: self.send.retain_rerun_input_default,
            pool_timeout: database.pool_timeout.get(),
            pool_recycle: database.pool_recycle,
            echo: database.echo,
            tcp_keepalives: database.tcp_keepalives,
            tcp_user_timeout: database.tcp_user_timeout,
        }
    }
}

impl TaskLimit {
    fn setting(self) -> TaskLimitSetting {
        match self {
            Self::Tasks { tasks } => TaskLimitSetting::TaskLimit { tasks: tasks.0 },
            Self::NoLimit => TaskLimitSetting::NoTaskLimit {},
        }
    }
}

impl MemoryLimit {
    fn setting(self) -> MemoryLimitSetting {
        match self {
            Self::Megabytes { mb } => MemoryLimitSetting::MemoryLimit { mb: mb.0 },
            Self::NoLimit => MemoryLimitSetting::NoMemoryLimit {},
        }
    }
}

impl PoolMaxSize {
    fn setting(self) -> PoolSizeSetting {
        match self {
            Self::Connections { connections } => PoolSizeSetting::PoolSize {
                connections: connections.0,
            },
            Self::FromSeats => PoolSizeSetting::PoolSizeFromSeats {},
        }
    }
}

impl DefaultExecutor {
    fn setting(self) -> ExecutorThreadsSetting {
        match self {
            Self::Threads { threads } => {
                ExecutorThreadsSetting::ExecutorThreads { threads: threads.0 }
            }
            Self::FromSeats => ExecutorThreadsSetting::ExecutorThreadsFromSeats {},
        }
    }
}

impl SharedRecycleManifest {
    fn setting(self) -> SharedRecycle {
        SharedRecycle {
            after_per_seat: self.after_per_seat.setting(),
            above: self.above.setting(),
        }
    }
}

impl SharedPoolManifest {
    fn setting(self) -> SharedPool {
        SharedPool {
            min_size: self.min_size.0,
            max_size: self.max_size.setting(),
            check: self.check,
        }
    }
}

impl Isolated {
    fn setting(self) -> IsolatedSetting {
        match self {
            Self::Children {
                count,
                recycle,
                pool,
                task_send_pool,
            } => IsolatedSetting::IsolatedChildren(IsolatedChildren {
                count: count.0,
                recycle: IsolatedRecycle {
                    after: recycle.after.setting(),
                    above: recycle.above.setting(),
                },
                pool: IsolatedPool {
                    min_size: pool.min_size.0,
                    max_size: PoolSize::new(pool.max_size.0),
                    check: pool.check,
                },
                task_send_pool: PoolSize::new(task_send_pool.0),
            }),
            Self::NoChildren => IsolatedSetting::NoIsolatedChildren {},
        }
    }
}

impl SharedAsync {
    fn setting(self) -> SharedAsyncSetting {
        match self {
            Self::Children {
                count,
                seats,
                recycle,
                pool,
                executor,
                task_send_pool,
            } => SharedAsyncSetting::SharedAsyncChildren(SharedAsyncChildren {
                count: count.0,
                seats: seats.0,
                recycle: recycle.setting(),
                pool: pool.setting(),
                executor: executor.setting(),
                task_send_pool: PoolSize::new(task_send_pool.0),
            }),
            Self::NoChildren => SharedAsyncSetting::NoSharedAsyncChildren {},
        }
    }
}

impl SharedThread {
    fn setting(self) -> SharedThreadSetting {
        match self {
            Self::Children {
                count,
                seats,
                recycle,
                pool,
                task_send_pool,
            } => SharedThreadSetting::SharedThreadChildren(SharedThreadChildren {
                count: count.0,
                seats: seats.0,
                recycle: recycle.setting(),
                pool: pool.setting(),
                task_send_pool: PoolSize::new(task_send_pool.0),
            }),
            Self::NoChildren => SharedThreadSetting::NoSharedThreadChildren {},
        }
    }
}

impl Preload {
    fn setting(self) -> ChildPreload {
        match self {
            Self::Library => ChildPreload::Library,
            Self::App => ChildPreload::App,
            Self::NoPreload => ChildPreload::NoPreload,
        }
    }
}

impl WorkerProfileManifest {
    /// The profile as the config type. Every value is in its bounds, so the
    /// mapping is total. Pure.
    pub fn to_profile(&self) -> WorkerProfile {
        WorkerProfile {
            isolated: self.isolated.setting(),
            shared_async: self.shared_async.setting(),
            shared_thread: self.shared_thread.setting(),
            pool: ParentPool {
                max_size: self.pool.max_size.setting(),
            },
            claim: ClaimLimits {
                per_queue_per_pass: self.claim.per_queue_per_pass.setting(),
                claimed_per_worker: self.claim.claimed_per_worker.setting(),
            },
            drain_timeout: self.drain_timeout_ms.drain_timeout(),
            preload: self.preload.setting(),
            parent_cpu: self.parent_cpu,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
