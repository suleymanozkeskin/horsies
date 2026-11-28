//! Frames from the worker parent to a child: `init`, `run`, `shared_run`,
//! `cancel_run`, `events` and `stop`.

use std::collections::BTreeMap;
use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use super::bounds::{
    AsyncSeats, CancelGraceMs, ExecutorThreads, PoolMaxSize, PoolMinSize, TaskSendPool,
    TaskTimeoutMs, ThreadSeats,
};
use super::child_frames::RunRef;
use super::event_batch::{decode_event_batch, EventBatch};
use super::header::{pack, EncodedFrame, FrameEncodeError, NO_BODY};
use super::metadata::{
    check_protocol, no_body, parse_metadata, text_body, time_text, Fields, MalformedFrame,
    ProtocolCheck, ProtocolMismatch,
};
use super::values::{DispatchNumber, MemoryLimit, RefusalReason, TaskInputText};
use super::PROTOCOL_VERSION;
use crate::broker::OrphanSelfHeal;
use crate::core::codec::check_json_text;
use crate::core::config::socket::{TcpKeepalivesConfig, TcpUserTimeoutConfig};
use crate::core::config::{PostgresConfig, PostgresConfigError, SessionDatabase};
use crate::core::mask_database_url;
use crate::core::task::fencing::TaskAttempt;
use crate::events::{AttemptNumber, MaxRetries, QueueName, RunTrace, TraceCarrierText};

/// The database URL a child connects with. It holds a password, so it
/// travels in the init frame (never in argv, which `ps` shows) and its debug
/// form is masked.
#[derive(Clone, PartialEq, Eq)]
pub struct DatabaseUrl(String);

impl DatabaseUrl {
    pub fn new(url: String) -> Self {
        Self(url)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for DatabaseUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DatabaseUrl({})", mask_database_url(&self.0))
    }
}

/// The log level of a child.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildLogLevel {
    Debug,
    Info,
    Warning,
    Error,
    Critical,
}

impl ChildLogLevel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
            Self::Critical => "critical",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        match text {
            "debug" => Some(Self::Debug),
            "info" => Some(Self::Info),
            "warning" => Some(Self::Warning),
            "error" => Some(Self::Error),
            "critical" => Some(Self::Critical),
            _ => None,
        }
    }
}

/// Whether a claimed task is a node of a workflow: its phase 2 finishes the
/// node. The run frames do not carry it: the start statement
/// (`horsies_start_attempt`) reads it from the locked row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskKind {
    Plain,
    WorkflowTask,
}

impl TaskKind {
    pub fn is_workflow_task(self) -> bool {
        matches!(self, Self::WorkflowTask)
    }
}

/// What a Python child imports before it reports ready.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PythonProgram {
    /// Where the app is, for example `module:attribute` or a file path.
    pub app_locator: String,
    /// Task modules to import after the app.
    pub imports: Vec<String>,
    /// Directories the child puts first on `sys.path`.
    pub sys_path_roots: Vec<String>,
    /// The `sys.path` of the worker process.
    pub parent_sys_path: Vec<String>,
    /// The `sys.argv` of the worker process.
    pub parent_argv: Vec<String>,
}

/// The program that a child runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChildProgram {
    /// The Python child program; it imports the app.
    Python(PythonProgram),
    /// The app program started again in child mode; its tasks are compiled in.
    Host,
}

/// The connection pool of a child: `min_size` connections stay open,
/// at most `max_size`. `check` pings every pooled connection at checkout
/// (psycopg's `check_connection`). The pool refuses a `min_size` above
/// `max_size` when it opens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChildPool {
    pub min_size: PoolMinSize,
    pub max_size: PoolMaxSize,
    pub check: bool,
}

/// A child that runs one task at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsolatedChild {
    pub pool: ChildPool,
    pub task_send_pool: TaskSendPool,
}

/// A child that runs up to `seats` async tasks at once on one event loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedAsyncChild {
    pub seats: AsyncSeats,
    pub pool: ChildPool,
    pub executor_threads: ExecutorThreads,
    pub task_send_pool: TaskSendPool,
}

/// A child that runs up to `seats` blocking tasks at once, each on a thread.
///
/// `recycle_above` is the resident-memory limit of the child (the
/// `recycle.above` of the profile). The parent recycles the child at it. A
/// host that runs its own seats reads it from the start facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedThreadChild {
    pub seats: ThreadSeats,
    pub pool: ChildPool,
    pub task_send_pool: TaskSendPool,
    pub recycle_above: MemoryLimit,
}

/// A child that runs the event handlers of the app. It runs no task: it
/// takes `events` frames and no run frame, and writes nothing to the
/// database itself. A handler may send tasks, through a pool of
/// `task_send_pool` connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandlerChild {
    pub task_send_pool: TaskSendPool,
}

/// The kind of a child. It decides which frames the child takes after its
/// ready frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildKind {
    Isolated(IsolatedChild),
    SharedAsync(SharedAsyncChild),
    SharedThread(SharedThreadChild),
    Handler(HandlerChild),
}

/// How the server sessions of `database_url` are pooled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerSessions {
    /// A direct server or PgBouncer session pooling: each connection keeps
    /// its server session.
    Kept,
    /// `database_url` is a PgBouncer transaction-pool endpoint: a connection
    /// sends unnamed statements only (Python `prepare_threshold=None`).
    /// `session_database_url` is the session URL of the parent's config,
    /// which [`PostgresConfig::validate`] requires in this mode.
    PgBouncerTransactionMode { session_database_url: DatabaseUrl },
}

/// The connection settings of the parent's config that each connection of
/// a child takes (Python `PostgresConfig.pooled_connect_args`,
/// `clients/python/horsies/core/models/broker.py:384-395`, sent as the `connect_kwargs` of
/// the init frame).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildConnection {
    pub tcp_keepalives: TcpKeepalivesConfig,
    pub tcp_user_timeout: TcpUserTimeoutConfig,
    pub server_sessions: ServerSessions,
}

impl ChildConnection {
    /// The connection settings of `config`.
    ///
    /// Errors: the [`PostgresConfigError`] of [`PostgresConfig::validate`].
    pub fn of_config(config: &PostgresConfig) -> Result<Self, PostgresConfigError> {
        config.validate()?;
        let server_sessions = match (config.pgbouncer_transaction_mode, &config.session_database) {
            (false, _) => ServerSessions::Kept,
            (true, SessionDatabase::Separate { url }) => ServerSessions::PgBouncerTransactionMode {
                session_database_url: DatabaseUrl::new(url.clone()),
            },
            (true, SessionDatabase::SameAsDatabaseUrl) => unreachable!(
                "PostgresConfig::validate refuses transaction mode without a separate session database"
            ),
        };
        Ok(Self {
            tcp_keepalives: config
                .tcp_keepalives
                .checked()
                .expect("PostgresConfig::validate checked the keepalive values"),
            tcp_user_timeout: config
                .tcp_user_timeout
                .checked()
                .expect("PostgresConfig::validate checked the TCP user timeout"),
            server_sessions,
        })
    }
}

/// The defaults of [`PostgresConfig`]: keepalives on (30 s idle, 10 s
/// interval, 3 probes), a 30 s TCP user timeout, and kept server sessions.
impl Default for ChildConnection {
    fn default() -> Self {
        Self {
            tcp_keepalives: TcpKeepalivesConfig::default(),
            tcp_user_timeout: TcpUserTimeoutConfig::default(),
            server_sessions: ServerSessions::Kept,
        }
    }
}

/// The first frame to a child: everything it needs to boot. The protocol
/// version is always [`PROTOCOL_VERSION`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitFrame {
    pub program: ChildProgram,
    pub log_level: ChildLogLevel,
    pub database_url: DatabaseUrl,
    /// The settings of each connection of the child's pool.
    pub connection: ChildConnection,
    pub process_name: String,
    pub child_kind: ChildKind,
}

/// Stored options of a claimed row that the worker cannot use. The child
/// sets the row RUNNING and fails the task with `WORKER_SERIALIZATION_ERROR`
/// before it resolves the task. No task code runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredOptionsRefusal {
    /// The stored `task_options` text does not parse, or is not an object,
    /// or its `retry_policy` does not validate (the reason starts with
    /// `stored retry_policy does not validate: `, as Python's).
    OptionsUnreadable(RefusalReason),
    /// The stored `timeout_ms` is not a whole number inside its bounds, or a
    /// shared class has none.
    TimeoutValueInvalid(RefusalReason),
    /// The stored `cancel_grace_ms` of a `SHARED_ASYNC` row is missing, or
    /// the stored `cancel_grace_ms` of a `SHARED_ASYNC` or `ISOLATED` row is
    /// outside its bounds.
    CancelGraceInvalid(RefusalReason),
}

/// What an isolated child does with the task of a run frame. The parent
/// holds the deadline of an isolated task, so the frame carries none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IsolatedWork {
    Run,
    Refusal(StoredOptionsRefusal),
}

/// What a seat of a shared child does with the task of a run. A shared
/// async child takes `AsyncTask` or `Refusal`, a shared thread child
/// `ThreadTask` or `Refusal`; any other work is a bad parent frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SharedWork {
    /// The child cancels the task `timeout_ms` after it starts it; the task
    /// then gets `cancel_grace_ms` to stop.
    AsyncTask {
        timeout_ms: TaskTimeoutMs,
        cancel_grace_ms: CancelGraceMs,
    },
    /// The child reports the deadline `timeout_ms` after it takes the run;
    /// a thread cannot be cancelled.
    ThreadTask {
        timeout_ms: TaskTimeoutMs,
    },
    Refusal(StoredOptionsRefusal),
}

/// Run one task in an isolated child. The body is the task input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunFrame {
    pub task_id: Uuid,
    pub task_name: String,
    /// The queue of the task row.
    pub queue_name: QueueName,
    /// The attempt that the claim runs: the retry count of the row plus 1.
    pub attempt: AttemptNumber,
    /// The retry budget of the task row (`max_retries`), from 0.
    pub max_retries: MaxRetries,
    /// The worker that holds the claim. The child fences each write of the
    /// attempt on it and on `claimed_at`.
    pub worker_id: String,
    /// The claim generation: the `claimed_at` that the claim returned.
    pub claimed_at: DateTime<Utc>,
    /// Whether the stop before start of a workflow task cancels the task
    /// when no node links it live (the app's
    /// `terminate_orphaned_workflow_tasks`).
    pub orphan_self_heal: OrphanSelfHeal,
    pub work: IsolatedWork,
    /// The trace carrier that the send stored on the task row. The child
    /// extracts it with the app's propagator.
    pub trace: RunTrace,
    pub input: TaskInputText,
}

impl RunFrame {
    /// The attempt facts of the run: its task id, attempt number and retry
    /// budget.
    pub(crate) fn task_attempt(&self) -> TaskAttempt {
        TaskAttempt::new(self.task_id, self.attempt, self.max_retries)
    }
}

/// Run one task on a free seat of a shared child. Every frame of the child
/// about this run carries `dispatch`. The body is the task input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedRunFrame {
    pub dispatch: DispatchNumber,
    pub task_id: Uuid,
    pub task_name: String,
    /// As [`RunFrame::queue_name`].
    pub queue_name: QueueName,
    /// As [`RunFrame::attempt`].
    pub attempt: AttemptNumber,
    /// As [`RunFrame::max_retries`].
    pub max_retries: MaxRetries,
    pub worker_id: String,
    pub claimed_at: DateTime<Utc>,
    /// As [`RunFrame::orphan_self_heal`].
    pub orphan_self_heal: OrphanSelfHeal,
    pub work: SharedWork,
    /// As [`RunFrame::trace`].
    pub trace: RunTrace,
    pub input: TaskInputText,
}

/// Lifecycle events for a handler child, in the order that the core emitted
/// them. Sent only to a handler child. The body is the JSON array of their
/// wire forms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventsFrame {
    pub events: EventBatch,
}

/// The run that a `cancel_run` frame names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelTarget {
    /// The run of an isolated child, by its task.
    Isolated { task_id: Uuid },
    /// A run on a seat of a shared child.
    Shared(RunRef),
}

/// A frame from the worker parent to a child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParentFrame {
    Init(InitFrame),
    Run(RunFrame),
    SharedRun(SharedRunFrame),
    /// An operator cancelled the task of a run: the cancel statement moved
    /// its row to history and recorded the attempt as `CANCELLED`. The child
    /// gives the cancel to the task code and reports the end of the run as
    /// cancelled. A cancel of a run that the child does not run (it ended
    /// while the frame was on its way) is ignored. No body.
    CancelRun(CancelTarget),
    Events(EventsFrame),
    /// Exit normally. Sent only to a child with no task.
    Stop,
}

/// Why a child refuses a parent frame.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParentFrameRefusal {
    /// The child exits with the bad-frame code.
    #[error(transparent)]
    Malformed(#[from] MalformedFrame),
    /// Only an init frame gives this; the child exits with the
    /// protocol-mismatch code.
    #[error(transparent)]
    ProtocolMismatch(ProtocolMismatch),
}

fn protocol_refusal(check: ProtocolCheck) -> ParentFrameRefusal {
    match check {
        ProtocolCheck::Mismatch(mismatch) => ParentFrameRefusal::ProtocolMismatch(mismatch),
        ProtocolCheck::Malformed(malformed) => ParentFrameRefusal::Malformed(malformed),
    }
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ParentWire<'a> {
    Init {
        protocol: u32,
        program: ProgramWire<'a>,
        log_level: &'static str,
        database_url: &'a str,
        connection: ConnectionWire<'a>,
        process_name: &'a str,
        child_kind: ChildKindWire,
    },
    Run {
        task_id: Uuid,
        task_name: &'a str,
        queue_name: &'a str,
        attempt: u32,
        max_retries: u32,
        worker_id: &'a str,
        claimed_at: String,
        orphan_self_heal: bool,
        work: IsolatedWorkWire<'a>,
        trace: TraceWire<'a>,
    },
    SharedRun {
        dispatch: u64,
        task_id: Uuid,
        task_name: &'a str,
        queue_name: &'a str,
        attempt: u32,
        max_retries: u32,
        worker_id: &'a str,
        claimed_at: String,
        orphan_self_heal: bool,
        work: SharedWorkWire<'a>,
        trace: TraceWire<'a>,
    },
    CancelRun {
        target: CancelTargetWire,
    },
    Events,
    Stop,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CancelTargetWire {
    Isolated { task_id: Uuid },
    Shared { dispatch: u64, task_id: Uuid },
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ProgramWire<'a> {
    Python {
        app_locator: &'a str,
        imports: &'a [String],
        sys_path_roots: &'a [String],
        parent_sys_path: &'a [String],
        parent_argv: &'a [String],
    },
    Host,
}

#[derive(Serialize)]
struct ConnectionWire<'a> {
    tcp_keepalives: KeepalivesWire,
    tcp_user_timeout: UserTimeoutWire,
    server_sessions: ServerSessionsWire<'a>,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum KeepalivesWire {
    Off,
    On {
        idle_seconds: u32,
        interval_seconds: u32,
        probes: u32,
    },
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum UserTimeoutWire {
    SystemDefault,
    After { milliseconds: u32 },
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ServerSessionsWire<'a> {
    Kept,
    #[serde(rename = "pgbouncer_transaction_mode")]
    PgBouncerTransactionMode {
        session_database_url: &'a str,
    },
}

#[derive(Serialize)]
struct PoolWire {
    min_size: u64,
    max_size: u64,
    check: bool,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum MemoryLimitWire {
    Off,
    AtMb { mb: u64 },
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ChildKindWire {
    Isolated {
        pool: PoolWire,
        task_send_pool: u64,
    },
    SharedAsync {
        seats: u64,
        pool: PoolWire,
        executor_threads: u64,
        task_send_pool: u64,
    },
    SharedThread {
        seats: u64,
        pool: PoolWire,
        task_send_pool: u64,
        recycle_above: MemoryLimitWire,
    },
    Handler {
        task_send_pool: u64,
    },
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum TraceWire<'a> {
    Carrier { text: &'a str },
    NoCarrier,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RefusalWire<'a> {
    OptionsUnreadable { reason: &'a str },
    ValueInvalid { reason: &'a str },
    GraceInvalid { reason: &'a str },
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum IsolatedWorkWire<'a> {
    Run,
    Refusal { refusal: RefusalWire<'a> },
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum SharedWorkWire<'a> {
    Task {
        timeout_ms: u64,
        cancel_grace_ms: u64,
    },
    ThreadTask {
        timeout_ms: u64,
    },
    Refusal {
        refusal: RefusalWire<'a>,
    },
}

/// The wire flag of an orphan self-heal: `true` for on. Pure.
fn self_heal_flag(self_heal: OrphanSelfHeal) -> bool {
    match self_heal {
        OrphanSelfHeal::On => true,
        OrphanSelfHeal::Off => false,
    }
}

fn program_wire(program: &ChildProgram) -> ProgramWire<'_> {
    match program {
        ChildProgram::Python(python) => ProgramWire::Python {
            app_locator: &python.app_locator,
            imports: &python.imports,
            sys_path_roots: &python.sys_path_roots,
            parent_sys_path: &python.parent_sys_path,
            parent_argv: &python.parent_argv,
        },
        ChildProgram::Host => ProgramWire::Host,
    }
}

fn connection_wire(connection: &ChildConnection) -> ConnectionWire<'_> {
    ConnectionWire {
        tcp_keepalives: match connection.tcp_keepalives {
            TcpKeepalivesConfig::Off => KeepalivesWire::Off,
            TcpKeepalivesConfig::On {
                idle_seconds,
                interval_seconds,
                probes,
            } => KeepalivesWire::On {
                idle_seconds: idle_seconds.get(),
                interval_seconds: interval_seconds.get(),
                probes: probes.get(),
            },
        },
        tcp_user_timeout: match connection.tcp_user_timeout {
            TcpUserTimeoutConfig::SystemDefault => UserTimeoutWire::SystemDefault,
            TcpUserTimeoutConfig::After { milliseconds } => UserTimeoutWire::After {
                milliseconds: milliseconds.get(),
            },
        },
        server_sessions: match &connection.server_sessions {
            ServerSessions::Kept => ServerSessionsWire::Kept,
            ServerSessions::PgBouncerTransactionMode {
                session_database_url,
            } => ServerSessionsWire::PgBouncerTransactionMode {
                session_database_url: session_database_url.as_str(),
            },
        },
    }
}

fn pool_wire(pool: ChildPool) -> PoolWire {
    PoolWire {
        min_size: pool.min_size.get(),
        max_size: pool.max_size.get(),
        check: pool.check,
    }
}

fn memory_limit_wire(limit: MemoryLimit) -> MemoryLimitWire {
    match limit {
        MemoryLimit::Off => MemoryLimitWire::Off,
        MemoryLimit::AtMb(mb) => MemoryLimitWire::AtMb { mb: mb.get() },
    }
}

fn child_kind_wire(kind: ChildKind) -> ChildKindWire {
    match kind {
        ChildKind::Isolated(child) => ChildKindWire::Isolated {
            pool: pool_wire(child.pool),
            task_send_pool: child.task_send_pool.get(),
        },
        ChildKind::SharedAsync(child) => ChildKindWire::SharedAsync {
            seats: child.seats.get(),
            pool: pool_wire(child.pool),
            executor_threads: child.executor_threads.get(),
            task_send_pool: child.task_send_pool.get(),
        },
        ChildKind::SharedThread(child) => ChildKindWire::SharedThread {
            seats: child.seats.get(),
            pool: pool_wire(child.pool),
            task_send_pool: child.task_send_pool.get(),
            recycle_above: memory_limit_wire(child.recycle_above),
        },
        ChildKind::Handler(child) => ChildKindWire::Handler {
            task_send_pool: child.task_send_pool.get(),
        },
    }
}

fn trace_wire(trace: &RunTrace) -> TraceWire<'_> {
    match trace {
        RunTrace::Carrier { text } => TraceWire::Carrier {
            text: text.as_str(),
        },
        RunTrace::NoCarrier => TraceWire::NoCarrier,
    }
}

fn refusal_wire(refusal: &StoredOptionsRefusal) -> RefusalWire<'_> {
    match refusal {
        StoredOptionsRefusal::OptionsUnreadable(reason) => RefusalWire::OptionsUnreadable {
            reason: reason.as_str(),
        },
        StoredOptionsRefusal::TimeoutValueInvalid(reason) => RefusalWire::ValueInvalid {
            reason: reason.as_str(),
        },
        StoredOptionsRefusal::CancelGraceInvalid(reason) => RefusalWire::GraceInvalid {
            reason: reason.as_str(),
        },
    }
}

fn isolated_work_wire(work: &IsolatedWork) -> IsolatedWorkWire<'_> {
    match work {
        IsolatedWork::Run => IsolatedWorkWire::Run,
        IsolatedWork::Refusal(refusal) => IsolatedWorkWire::Refusal {
            refusal: refusal_wire(refusal),
        },
    }
}

fn cancel_target_wire(target: CancelTarget) -> CancelTargetWire {
    match target {
        CancelTarget::Isolated { task_id } => CancelTargetWire::Isolated { task_id },
        CancelTarget::Shared(run) => CancelTargetWire::Shared {
            dispatch: run.dispatch.get(),
            task_id: run.task_id,
        },
    }
}

fn shared_work_wire(work: &SharedWork) -> SharedWorkWire<'_> {
    match work {
        SharedWork::AsyncTask {
            timeout_ms,
            cancel_grace_ms,
        } => SharedWorkWire::Task {
            timeout_ms: timeout_ms.get(),
            cancel_grace_ms: cancel_grace_ms.get(),
        },
        SharedWork::ThreadTask { timeout_ms } => SharedWorkWire::ThreadTask {
            timeout_ms: timeout_ms.get(),
        },
        SharedWork::Refusal(refusal) => SharedWorkWire::Refusal {
            refusal: refusal_wire(refusal),
        },
    }
}

/// Encode a parent frame. `Err`: the metadata or the input is over a bound
/// of the header; nothing is sent then.
pub fn encode_parent_frame(frame: &ParentFrame) -> Result<EncodedFrame<'_>, FrameEncodeError> {
    match frame {
        ParentFrame::Init(init) => pack(
            &ParentWire::Init {
                protocol: PROTOCOL_VERSION,
                program: program_wire(&init.program),
                log_level: init.log_level.as_str(),
                database_url: init.database_url.as_str(),
                connection: connection_wire(&init.connection),
                process_name: &init.process_name,
                child_kind: child_kind_wire(init.child_kind),
            },
            NO_BODY,
        ),
        ParentFrame::Run(run) => pack(
            &ParentWire::Run {
                task_id: run.task_id,
                task_name: &run.task_name,
                queue_name: run.queue_name.as_str(),
                attempt: run.attempt.get(),
                max_retries: run.max_retries.get(),
                worker_id: &run.worker_id,
                // The claim generation keeps every sub-second digit, so the
                // child fences on the exact value.
                claimed_at: time_text(&run.claimed_at),
                orphan_self_heal: self_heal_flag(run.orphan_self_heal),
                work: isolated_work_wire(&run.work),
                trace: trace_wire(&run.trace),
            },
            run.input.as_str().as_bytes(),
        ),
        ParentFrame::SharedRun(run) => pack(
            &ParentWire::SharedRun {
                dispatch: run.dispatch.get(),
                task_id: run.task_id,
                task_name: &run.task_name,
                queue_name: run.queue_name.as_str(),
                attempt: run.attempt.get(),
                max_retries: run.max_retries.get(),
                worker_id: &run.worker_id,
                claimed_at: time_text(&run.claimed_at),
                orphan_self_heal: self_heal_flag(run.orphan_self_heal),
                work: shared_work_wire(&run.work),
                trace: trace_wire(&run.trace),
            },
            run.input.as_str().as_bytes(),
        ),
        ParentFrame::CancelRun(target) => pack(
            &ParentWire::CancelRun {
                target: cancel_target_wire(*target),
            },
            NO_BODY,
        ),
        ParentFrame::Events(events) => pack(&ParentWire::Events, events.events.body().as_bytes()),
        ParentFrame::Stop => pack(&ParentWire::Stop, NO_BODY),
    }
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// Build a parent frame from received bytes. Used by a child.
///
/// An init frame names its protocol version first: a frame of another
/// version gives [`ParentFrameRefusal::ProtocolMismatch`] before any other
/// field is read.
pub fn decode_parent_frame(meta: &[u8], body: Vec<u8>) -> Result<ParentFrame, ParentFrameRefusal> {
    let map = parse_metadata(meta)?;
    let fields = Fields::root(&map);
    match fields.kind()? {
        "init" => {
            check_protocol(&map).map_err(protocol_refusal)?;
            no_body("init", &body)?;
            Ok(ParentFrame::Init(decode_init(&fields)?))
        }
        "run" => Ok(ParentFrame::Run(decode_run(&fields, body)?)),
        "shared_run" => Ok(ParentFrame::SharedRun(decode_shared_run(&fields, body)?)),
        "cancel_run" => {
            fields.only(&["kind", "target"])?;
            no_body("cancel_run", &body)?;
            Ok(ParentFrame::CancelRun(decode_cancel_target(
                &fields.object("target")?,
            )?))
        }
        "events" => {
            fields.only(&["kind"])?;
            Ok(ParentFrame::Events(EventsFrame {
                events: decode_event_batch(body)?,
            }))
        }
        "stop" => {
            fields.only(&["kind"])?;
            no_body("stop", &body)?;
            Ok(ParentFrame::Stop)
        }
        other => Err(fields.unknown_kind(other).into()),
    }
}

/// The run of a `cancel_run` frame: `isolated` with its task, or `shared`
/// with its dispatch and task.
fn decode_cancel_target(target: &Fields<'_>) -> Result<CancelTarget, MalformedFrame> {
    match target.kind()? {
        "isolated" => {
            target.only(&["kind", "task_id"])?;
            Ok(CancelTarget::Isolated {
                task_id: target.task_id("task_id")?,
            })
        }
        "shared" => {
            target.only(&["kind", "dispatch", "task_id"])?;
            Ok(CancelTarget::Shared(RunRef {
                dispatch: target.dispatch("dispatch")?,
                task_id: target.task_id("task_id")?,
            }))
        }
        other => Err(target.unknown_kind(other)),
    }
}

fn decode_init(fields: &Fields<'_>) -> Result<InitFrame, MalformedFrame> {
    fields.only(&[
        "kind",
        "protocol",
        "program",
        "log_level",
        "database_url",
        "connection",
        "process_name",
        "child_kind",
    ])?;
    Ok(InitFrame {
        program: decode_program(&fields.object("program")?)?,
        log_level: decode_log_level(fields)?,
        database_url: DatabaseUrl::new(fields.owned_text("database_url")?),
        connection: decode_connection(&fields.object("connection")?)?,
        process_name: fields.owned_text("process_name")?,
        child_kind: decode_child_kind(&fields.object("child_kind")?)?,
    })
}

fn decode_program(program: &Fields<'_>) -> Result<ChildProgram, MalformedFrame> {
    match program.kind()? {
        "python" => {
            program.only(&[
                "kind",
                "app_locator",
                "imports",
                "sys_path_roots",
                "parent_sys_path",
                "parent_argv",
            ])?;
            Ok(ChildProgram::Python(PythonProgram {
                app_locator: program.owned_text("app_locator")?,
                imports: program.text_list("imports")?,
                sys_path_roots: program.text_list("sys_path_roots")?,
                parent_sys_path: program.text_list("parent_sys_path")?,
                parent_argv: program.text_list("parent_argv")?,
            }))
        }
        "host" => {
            program.only(&["kind"])?;
            Ok(ChildProgram::Host)
        }
        other => Err(program.unknown_kind(other)),
    }
}

fn decode_log_level(fields: &Fields<'_>) -> Result<ChildLogLevel, MalformedFrame> {
    ChildLogLevel::parse(fields.text("log_level")?).ok_or_else(|| {
        fields.broken_rule(
            "log_level",
            "is not one of debug, info, warning, error, critical",
        )
    })
}

/// A whole number read through the `TryFrom<u32>` of its config type, which
/// holds its bounds.
fn config_count<T: TryFrom<u32>>(fields: &Fields<'_>, key: &str) -> Result<T, MalformedFrame> {
    u32::try_from(fields.whole(key)?)
        .ok()
        .and_then(|value| T::try_from(value).ok())
        .ok_or_else(|| fields.out_of_bounds(key))
}

fn decode_connection(connection: &Fields<'_>) -> Result<ChildConnection, MalformedFrame> {
    connection.only(&["tcp_keepalives", "tcp_user_timeout", "server_sessions"])?;
    Ok(ChildConnection {
        tcp_keepalives: decode_keepalives(&connection.object("tcp_keepalives")?)?,
        tcp_user_timeout: decode_user_timeout(&connection.object("tcp_user_timeout")?)?,
        server_sessions: decode_server_sessions(&connection.object("server_sessions")?)?,
    })
}

fn decode_keepalives(keepalives: &Fields<'_>) -> Result<TcpKeepalivesConfig, MalformedFrame> {
    match keepalives.kind()? {
        "off" => {
            keepalives.only(&["kind"])?;
            Ok(TcpKeepalivesConfig::Off)
        }
        "on" => {
            keepalives.only(&["kind", "idle_seconds", "interval_seconds", "probes"])?;
            Ok(TcpKeepalivesConfig::On {
                idle_seconds: config_count(keepalives, "idle_seconds")?,
                interval_seconds: config_count(keepalives, "interval_seconds")?,
                probes: config_count(keepalives, "probes")?,
            })
        }
        other => Err(keepalives.unknown_kind(other)),
    }
}

fn decode_user_timeout(timeout: &Fields<'_>) -> Result<TcpUserTimeoutConfig, MalformedFrame> {
    match timeout.kind()? {
        "system_default" => {
            timeout.only(&["kind"])?;
            Ok(TcpUserTimeoutConfig::SystemDefault)
        }
        "after" => {
            timeout.only(&["kind", "milliseconds"])?;
            Ok(TcpUserTimeoutConfig::After {
                milliseconds: config_count(timeout, "milliseconds")?,
            })
        }
        other => Err(timeout.unknown_kind(other)),
    }
}

fn decode_server_sessions(sessions: &Fields<'_>) -> Result<ServerSessions, MalformedFrame> {
    match sessions.kind()? {
        "kept" => {
            sessions.only(&["kind"])?;
            Ok(ServerSessions::Kept)
        }
        "pgbouncer_transaction_mode" => {
            sessions.only(&["kind", "session_database_url"])?;
            Ok(ServerSessions::PgBouncerTransactionMode {
                session_database_url: DatabaseUrl::new(
                    sessions.owned_text("session_database_url")?,
                ),
            })
        }
        other => Err(sessions.unknown_kind(other)),
    }
}

fn decode_pool(pool: &Fields<'_>) -> Result<ChildPool, MalformedFrame> {
    pool.only(&["min_size", "max_size", "check"])?;
    Ok(ChildPool {
        min_size: pool.bounded_count("min_size")?,
        max_size: pool.bounded_count("max_size")?,
        check: pool.flag("check")?,
    })
}

fn decode_memory_limit(limit: &Fields<'_>) -> Result<MemoryLimit, MalformedFrame> {
    match limit.kind()? {
        "off" => {
            limit.only(&["kind"])?;
            Ok(MemoryLimit::Off)
        }
        "at_mb" => {
            limit.only(&["kind", "mb"])?;
            let mb: NonZeroU32 = config_count(limit, "mb")?;
            Ok(MemoryLimit::AtMb(NonZeroU64::from(mb)))
        }
        other => Err(limit.unknown_kind(other)),
    }
}

fn decode_child_kind(kind: &Fields<'_>) -> Result<ChildKind, MalformedFrame> {
    match kind.kind()? {
        "isolated" => {
            kind.only(&["kind", "pool", "task_send_pool"])?;
            Ok(ChildKind::Isolated(IsolatedChild {
                pool: decode_pool(&kind.object("pool")?)?,
                task_send_pool: kind.bounded_count("task_send_pool")?,
            }))
        }
        "shared_async" => {
            kind.only(&[
                "kind",
                "seats",
                "pool",
                "executor_threads",
                "task_send_pool",
            ])?;
            Ok(ChildKind::SharedAsync(SharedAsyncChild {
                seats: kind.bounded_count("seats")?,
                pool: decode_pool(&kind.object("pool")?)?,
                executor_threads: kind.bounded_count("executor_threads")?,
                task_send_pool: kind.bounded_count("task_send_pool")?,
            }))
        }
        "shared_thread" => {
            kind.only(&["kind", "seats", "pool", "task_send_pool", "recycle_above"])?;
            Ok(ChildKind::SharedThread(SharedThreadChild {
                seats: kind.bounded_count("seats")?,
                pool: decode_pool(&kind.object("pool")?)?,
                task_send_pool: kind.bounded_count("task_send_pool")?,
                recycle_above: decode_memory_limit(&kind.object("recycle_above")?)?,
            }))
        }
        "handler" => {
            kind.only(&["kind", "task_send_pool"])?;
            Ok(ChildKind::Handler(HandlerChild {
                task_send_pool: kind.bounded_count("task_send_pool")?,
            }))
        }
        other => Err(kind.unknown_kind(other)),
    }
}

fn decode_refusal(refusal: &Fields<'_>) -> Result<StoredOptionsRefusal, MalformedFrame> {
    let kind = refusal.kind()?;
    refusal.only(&["kind", "reason"])?;
    match kind {
        "options_unreadable" => Ok(StoredOptionsRefusal::OptionsUnreadable(
            refusal.bounded_text("reason")?,
        )),
        "value_invalid" => Ok(StoredOptionsRefusal::TimeoutValueInvalid(
            refusal.bounded_text("reason")?,
        )),
        "grace_invalid" => Ok(StoredOptionsRefusal::CancelGraceInvalid(
            refusal.bounded_text("reason")?,
        )),
        other => Err(refusal.unknown_kind(other)),
    }
}

fn decode_isolated_work(work: &Fields<'_>) -> Result<IsolatedWork, MalformedFrame> {
    match work.kind()? {
        "run" => {
            work.only(&["kind"])?;
            Ok(IsolatedWork::Run)
        }
        "refusal" => {
            work.only(&["kind", "refusal"])?;
            Ok(IsolatedWork::Refusal(decode_refusal(
                &work.object("refusal")?,
            )?))
        }
        other => Err(work.unknown_kind(other)),
    }
}

fn decode_shared_work(work: &Fields<'_>) -> Result<SharedWork, MalformedFrame> {
    match work.kind()? {
        "task" => {
            work.only(&["kind", "timeout_ms", "cancel_grace_ms"])?;
            Ok(SharedWork::AsyncTask {
                timeout_ms: work.bounded_count("timeout_ms")?,
                cancel_grace_ms: work.bounded_count("cancel_grace_ms")?,
            })
        }
        "thread_task" => {
            work.only(&["kind", "timeout_ms"])?;
            Ok(SharedWork::ThreadTask {
                timeout_ms: work.bounded_count("timeout_ms")?,
            })
        }
        "refusal" => {
            work.only(&["kind", "refusal"])?;
            Ok(SharedWork::Refusal(decode_refusal(
                &work.object("refusal")?,
            )?))
        }
        other => Err(work.unknown_kind(other)),
    }
}

/// The trace carrier of a run: the stored text, at most
/// [`crate::events::MAX_EVENT_TRACE_CARRIER_CHARS`] characters, which is one
/// JSON object of texts, the form that the send stores.
fn decode_trace(trace: &Fields<'_>) -> Result<RunTrace, MalformedFrame> {
    match trace.kind()? {
        "carrier" => {
            trace.only(&["kind", "text"])?;
            let text: TraceCarrierText = trace.bounded_text("text")?;
            match is_carrier_object(text.as_str()) {
                true => Ok(RunTrace::Carrier { text }),
                false => Err(trace.broken_rule("text", "is not one JSON object of texts")),
            }
        }
        "no_carrier" => {
            trace.only(&["kind"])?;
            Ok(RunTrace::NoCarrier)
        }
        other => Err(trace.unknown_kind(other)),
    }
}

/// Whether `text` is one strict JSON object whose values are texts. Pure.
fn is_carrier_object(text: &str) -> bool {
    check_json_text(text).is_ok() && serde_json::from_str::<BTreeMap<String, String>>(text).is_ok()
}

fn decode_run(fields: &Fields<'_>, body: Vec<u8>) -> Result<RunFrame, MalformedFrame> {
    fields.only(&[
        "kind",
        "task_id",
        "task_name",
        "queue_name",
        "attempt",
        "max_retries",
        "worker_id",
        "claimed_at",
        "orphan_self_heal",
        "work",
        "trace",
    ])?;
    Ok(RunFrame {
        task_id: fields.task_id("task_id")?,
        task_name: fields.owned_text("task_name")?,
        queue_name: fields.bounded_text("queue_name")?,
        attempt: config_count(fields, "attempt")?,
        max_retries: config_count(fields, "max_retries")?,
        worker_id: fields.owned_text("worker_id")?,
        claimed_at: fields.time("claimed_at")?,
        orphan_self_heal: OrphanSelfHeal::of_recovery_flag(fields.flag("orphan_self_heal")?),
        work: decode_isolated_work(&fields.object("work")?)?,
        trace: decode_trace(&fields.object("trace")?)?,
        input: TaskInputText::new(text_body(body)?),
    })
}

fn decode_shared_run(fields: &Fields<'_>, body: Vec<u8>) -> Result<SharedRunFrame, MalformedFrame> {
    fields.only(&[
        "kind",
        "dispatch",
        "task_id",
        "task_name",
        "queue_name",
        "attempt",
        "max_retries",
        "worker_id",
        "claimed_at",
        "orphan_self_heal",
        "work",
        "trace",
    ])?;
    Ok(SharedRunFrame {
        dispatch: fields.dispatch("dispatch")?,
        task_id: fields.task_id("task_id")?,
        task_name: fields.owned_text("task_name")?,
        queue_name: fields.bounded_text("queue_name")?,
        attempt: config_count(fields, "attempt")?,
        max_retries: config_count(fields, "max_retries")?,
        worker_id: fields.owned_text("worker_id")?,
        claimed_at: fields.time("claimed_at")?,
        orphan_self_heal: OrphanSelfHeal::of_recovery_flag(fields.flag("orphan_self_heal")?),
        work: decode_shared_work(&fields.object("work")?)?,
        trace: decode_trace(&fields.object("trace")?)?,
        input: TaskInputText::new(text_body(body)?),
    })
}
