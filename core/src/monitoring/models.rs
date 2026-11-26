//! Response models for the transport-free monitoring API.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::time_text;
use super::timing_models::{AttemptPhasesInfo, NodeTimingInfo, RunTimingInfo};
use crate::core::history::reads::detail::TaskAbsenceReason;
use crate::core::task::Execution;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCategory {
    Operational,
    Contract,
    Retrieval,
    Outcome,
    Domain,
}

impl ErrorCategory {
    pub const ALL: [Self; 5] = [
        Self::Operational,
        Self::Contract,
        Self::Retrieval,
        Self::Outcome,
        Self::Domain,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Operational => "OPERATIONAL",
            Self::Contract => "CONTRACT",
            Self::Retrieval => "RETRIEVAL",
            Self::Outcome => "OUTCOME",
            Self::Domain => "DOMAIN",
        }
    }
}

impl std::fmt::Display for ErrorCategory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// One recorded attempt of a task (Python `TaskAttemptDetail`). `phases` is
/// the phase record of the attempt row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct TaskAttemptDetail {
    pub attempt: i32,
    pub outcome: String,
    pub will_retry: bool,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub failed_reason: Option<String>,
    pub worker_hostname: Option<String>,
    #[serde(serialize_with = "time_text::optional_utc")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(serialize_with = "time_text::optional_utc")]
    pub finished_at: Option<DateTime<Utc>>,
    pub phases: AttemptPhasesInfo,
}

/// The task row behind a task detail or a workflow node (Python
/// `TaskRowInfo`). `execution` is the stored class of a live task, and
/// `None` for a task read from history, which does not keep the class.
/// `trace_id` is the trace id of the span that sent the task, as 32
/// lowercase hex digits, live and in history; `None` for a task sent with no
/// trace.
///
/// `queue_seconds` is the queue wait (enqueue to start) and
/// `execution_seconds` the run time (start to end), each a span of the stored
/// times; the open span of a live task runs to now. Each serializes as a
/// number of seconds, exact to the microsecond
/// (`time_text::optional_seconds`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct TaskRowInfo {
    pub task_id: Uuid,
    pub status: String,
    pub error_code: Option<String>,
    pub failed_reason: Option<String>,
    pub retry_count: i32,
    pub max_retries: i32,
    #[serde(serialize_with = "time_text::optional_utc")]
    pub enqueued_at: Option<DateTime<Utc>>,
    #[serde(serialize_with = "time_text::optional_utc")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(serialize_with = "time_text::optional_utc")]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(serialize_with = "time_text::optional_utc")]
    pub failed_at: Option<DateTime<Utc>>,
    #[serde(with = "time_text::optional_seconds")]
    #[cfg_attr(test, schemars(with = "Option<f64>"))]
    pub queue_seconds: Option<TimeDelta>,
    #[serde(with = "time_text::optional_seconds")]
    #[cfg_attr(test, schemars(with = "Option<f64>"))]
    pub execution_seconds: Option<TimeDelta>,
    pub worker_hostname: Option<String>,
    #[serde(serialize_with = "time_text::optional_utc")]
    pub good_until: Option<DateTime<Utc>>,
    pub execution: Option<Execution>,
    pub trace_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct StatusCount {
    pub status: String,
    pub count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct FacetValue {
    pub value: String,
    pub count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct ErrorFacet {
    pub value: String,
    pub count: i64,
    pub category: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct Facets {
    pub workers: Vec<FacetValue>,
    pub task_names: Vec<FacetValue>,
    pub queues: Vec<FacetValue>,
    pub error_codes: Vec<ErrorFacet>,
    pub error_category_totals: BTreeMap<String, i64>,
}

/// What one breakdown group holds, tagged by `kind` (Python
/// `BreakdownGroup`, `clients/python/horsies/monitoring/models.py`): `value`, the tasks
/// whose group column holds `value` (a worker id, a task name or a queue
/// name); or `unclaimed`, the tasks that no worker claimed, which only the
/// `worker` grouping has. A worker whose id is the text `unknown` is a
/// `value` group of its own, never the `unclaimed` group.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BreakdownGroup {
    Value { value: String },
    Unclaimed,
}

/// Task counts per status, the task total, and the tasks retried at least
/// once (Python `BreakdownCounts`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct BreakdownCounts {
    pub total: i64,
    pub pending: i64,
    pub claimed: i64,
    pub running: i64,
    pub completed: i64,
    pub failed: i64,
    pub cancelled: i64,
    pub expired: i64,
    pub retried: i64,
}

/// One group's per-status task counts (Python `GroupRow`): the counts, then
/// the group, in one JSON object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct GroupRow {
    #[serde(flatten)]
    pub counts: BreakdownCounts,
    pub group: BreakdownGroup,
}

/// Per-group status pivot plus the counts of every group (Python
/// `Breakdown`). `groups` holds at most the query's limit of groups, by
/// total, highest first, then by the group text in code-point order (the
/// unclaimed group sorts as the text `unknown`, after a value group
/// `unknown` with the same total). `group_count` is the number of distinct
/// groups, and `total` covers every matching task, not only the groups
/// shown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct Breakdown {
    pub group_by: String,
    pub groups: Vec<GroupRow>,
    pub total: BreakdownCounts,
    pub group_count: i64,
}

/// One row of the task list (Python `TaskSummary`). `queue_seconds` and
/// `execution_seconds` are the spans of [`TaskRowInfo`], and serialize as it
/// does.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct TaskSummary {
    pub task_id: Uuid,
    pub task_name: String,
    pub queue: String,
    pub status: String,
    pub priority: i32,
    pub retry_count: i32,
    pub max_retries: i32,
    pub is_workflow_task: bool,
    pub error_code: Option<String>,
    pub error_category: Option<String>,
    pub worker_hostname: Option<String>,
    pub worker_id: Option<String>,
    #[serde(serialize_with = "time_text::optional_utc")]
    pub enqueued_at: Option<DateTime<Utc>>,
    #[serde(serialize_with = "time_text::optional_utc")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(serialize_with = "time_text::optional_utc")]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(serialize_with = "time_text::optional_utc")]
    pub failed_at: Option<DateTime<Utc>>,
    #[serde(with = "time_text::optional_seconds")]
    #[cfg_attr(test, schemars(with = "Option<f64>"))]
    pub queue_seconds: Option<TimeDelta>,
    #[serde(with = "time_text::optional_seconds")]
    #[cfg_attr(test, schemars(with = "Option<f64>"))]
    pub execution_seconds: Option<TimeDelta>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct TaskListPage {
    pub rows: Vec<TaskSummary>,
    pub total: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct TaskDetail {
    pub task_row: TaskRowInfo,
    pub task_name: String,
    pub queue: String,
    pub priority: i32,
    pub is_workflow_task: bool,
    pub error_category: Option<String>,
    pub attempts: Vec<TaskAttemptDetail>,
    pub workflow_id: Option<Uuid>,
    pub workflow_node_index: Option<i32>,
}

/// The answer of the task detail read (Python `TaskDetailRead`, the union
/// `TaskDetail | TaskDetailNotFound`).
#[derive(Debug, Clone, PartialEq)]
pub enum TaskDetailRead {
    /// The live row, or the history row when the live table has no row.
    Found(Box<TaskDetail>),
    /// Neither the live table nor the history holds the task.
    NotFound(TaskDetailNotFound),
}

/// No live row and no history row has this task id. `reason` says whether
/// retention can explain the absence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskDetailNotFound {
    pub task_id: Uuid,
    pub reason: TaskAbsenceReason,
}

/// One workflow run, root or child (Python `WorkflowRunSummary`).
/// `wall_seconds` is the time from `created_at` to `completed_at`, or to now
/// while the run is open; it serializes as a number of seconds, exact to the microsecond.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct WorkflowRunSummary {
    pub workflow_id: Uuid,
    pub workflow_name: String,
    pub definition_key: Option<String>,
    pub status: String,
    #[serde(serialize_with = "time_text::optional_utc")]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(serialize_with = "time_text::optional_utc")]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(with = "time_text::optional_seconds")]
    #[cfg_attr(test, schemars(with = "Option<f64>"))]
    pub wall_seconds: Option<TimeDelta>,
}

/// One node of a run (Python `WorkflowNodeSummary`). `timing` is the phase
/// timing of the node. `execution_seconds` is the run time of the node: to
/// now while it runs, to `completed_at` once it finished, else `None`; it
/// serializes as a number of seconds, exact to the microsecond.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct WorkflowNodeSummary {
    pub node_index: i32,
    pub node_id: Option<String>,
    pub task_name: String,
    pub node_status: String,
    pub is_sub_workflow: bool,
    pub sub_workflow_id: Option<Uuid>,
    pub allow_failed_deps: bool,
    #[serde(serialize_with = "time_text::optional_utc")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(serialize_with = "time_text::optional_utc")]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(with = "time_text::optional_seconds")]
    #[cfg_attr(test, schemars(with = "Option<f64>"))]
    pub execution_seconds: Option<TimeDelta>,
    pub child_total: Option<i64>,
    pub child_failed: Option<i64>,
    pub timing: NodeTimingInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct WorkflowEdge {
    pub from_index: i32,
    pub to_index: i32,
}

/// One run with its nodes and edges (Python `WorkflowRunDetail`).
/// `trace_id` is the trace id of the span that started the run, as 32
/// lowercase hex digits; `None` for a start with no trace. `timing` is the
/// phase timing of the run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct WorkflowRunDetail {
    pub run: WorkflowRunSummary,
    pub nodes: Vec<WorkflowNodeSummary>,
    pub edges: Vec<WorkflowEdge>,
    pub failed_count: i64,
    pub failed_indices: Vec<i32>,
    pub trace_id: Option<String>,
    pub timing: RunTimingInfo,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct WorkflowNodeDetail {
    pub node_index: i32,
    pub node_id: Option<String>,
    pub task_name: String,
    pub node_status: String,
    pub is_sub_workflow: bool,
    pub node_error: Option<String>,
    pub task_row: Option<TaskRowInfo>,
    pub attempts: Vec<TaskAttemptDetail>,
}

/// One slot that was down when the worker wrote its snapshot (Python
/// `DownSlotInfo`, `clients/python/horsies/monitoring/models.py:307-318`).
///
/// `cause` describes the cause of the last failed child start, and `detail`
/// is its error text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct DownSlotInfo {
    pub slot: u32,
    pub cause: String,
    pub detail: String,
    #[serde(serialize_with = "time_text::utc")]
    pub down_since: DateTime<Utc>,
    pub failed_boots: u64,
}

/// Whether any slot of a worker was down.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SlotHealthKind {
    AllServing,
    SlotsDown,
}

/// The worker profile that a snapshot names, tagged by `kind`: `named` or
/// `not_recorded` (Python `ProfileNamedInfo`, `ProfileNotRecordedInfo`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SnapshotProfileInfo {
    /// The worker ran the profile `name` of `AppConfig.worker_profiles`.
    Named { name: String },
    /// The snapshot holds no profile name: a worker of a build before
    /// schema migration 1004 wrote it.
    NotRecorded,
}

/// Latest state snapshot for one worker (Python `WorkerStateInfo`,
/// `clients/python/horsies/monitoring/models.py`).
///
/// `down_slots` lists the down slots in slot order; it is empty unless
/// `slot_health` is `slots_down`. `profile` names the worker profile that
/// the worker ran.
///
/// `snapshot_age_seconds` (from `snapshot_at`) and `uptime_seconds` (from
/// `worker_started_at`) are measured to the database time of the read, and
/// serialize as numbers of seconds, exact to the microsecond.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct WorkerStateInfo {
    pub worker_id: String,
    pub hostname: String,
    pub pid: i32,
    #[serde(serialize_with = "time_text::utc")]
    pub snapshot_at: DateTime<Utc>,
    #[serde(with = "time_text::optional_seconds")]
    #[cfg_attr(test, schemars(with = "Option<f64>"))]
    pub snapshot_age_seconds: Option<TimeDelta>,
    pub stale: bool,
    #[serde(serialize_with = "time_text::utc")]
    pub worker_started_at: DateTime<Utc>,
    #[serde(with = "time_text::optional_seconds")]
    #[cfg_attr(test, schemars(with = "Option<f64>"))]
    pub uptime_seconds: Option<TimeDelta>,
    pub processes: i32,
    pub queues: Vec<String>,
    pub queue_max_concurrency: BTreeMap<String, i64>,
    pub tasks_running: i32,
    pub tasks_claimed: i32,
    pub cluster_wide_cap: Option<i32>,
    pub memory_usage_mb: Option<f64>,
    pub memory_percent: Option<f64>,
    pub cpu_percent: Option<f64>,
    pub slot_health: SlotHealthKind,
    pub down_slots: Vec<DownSlotInfo>,
    pub profile: SnapshotProfileInfo,
}

/// One claim budget of a live worker (Python `BudgetLoadInfo`,
/// `clients/python/horsies/monitoring/models.py:354-367`).
///
/// `capacity` (children or seats in service) and `serves` come from the
/// worker's latest snapshot. `in_flight` counts, at the read, its `RUNNING`
/// rows and `CLAIMED` rows with a live lease of the classes in `serves`.
/// `free` is `capacity` minus `in_flight`, at least 0.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct BudgetLoadInfo {
    pub budget: Execution,
    pub capacity: u64,
    pub serves: Vec<Execution>,
    pub in_flight: u64,
    pub free: u64,
}

/// The claim budgets of one live worker, in execution class order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct WorkerClassLoadInfo {
    pub worker_id: String,
    #[serde(serialize_with = "time_text::utc")]
    pub snapshot_at: DateTime<Utc>,
    pub budgets: Vec<BudgetLoadInfo>,
}

/// Whether live workers serve a pending stream, tagged by `kind`: `served`,
/// `no_capacity` or `unserved` (Python `StreamServedInfo`,
/// `StreamNoCapacityInfo`, `StreamUnservedInfo`,
/// `clients/python/horsies/monitoring/models.py:378-400`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StreamCoverageInfo {
    /// `workers` counts the live workers that serve the stream; `capacity`
    /// sums the capacity in service of the budget that serves the class on
    /// each.
    Served { workers: usize, capacity: u64 },
    /// Live workers serve the stream, and none has capacity in service for it.
    NoCapacity { workers: usize },
    /// No live worker serves the stream.
    Unserved,
}

/// How long the stream's rows wait, tagged by `kind`: `waiting` or
/// `none_due` (Python `StreamWaitingInfo`, `StreamNoneDueInfo`,
/// `clients/python/horsies/monitoring/models.py:403-415`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StreamWaitInfo {
    /// At least one row is due. `wait_s` is the wait of the oldest due row
    /// at `checked_at`, as a number of seconds, exact to the microsecond.
    Waiting {
        #[serde(serialize_with = "time_text::utc")]
        oldest_due_at: DateTime<Utc>,
        #[serde(serialize_with = "time_text::seconds")]
        #[cfg_attr(test, schemars(with = "f64"))]
        wait_s: TimeDelta,
    },
    /// Every row is due later; `next_due_at` is the earliest due time.
    NoneDue {
        #[serde(serialize_with = "time_text::utc")]
        next_due_at: DateTime<Utc>,
    },
}

/// The `PENDING` rows of one queue and one execution class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct PendingStreamInfo {
    pub queue_name: String,
    pub execution: Execution,
    pub coverage: StreamCoverageInfo,
    pub wait: StreamWaitInfo,
}

/// Pending streams and the budget load of each live worker (Python
/// `ClassReportInfo`, `clients/python/horsies/monitoring/models.py:433-447`).
///
/// `checked_at` is database time. A worker is live while its latest snapshot
/// is at most `live_window_s` old. `streams` is ordered by queue name, then
/// by execution class. `workers` holds the live workers, by worker id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct ClassReportInfo {
    #[serde(serialize_with = "time_text::utc")]
    pub checked_at: DateTime<Utc>,
    pub live_window_s: u64,
    pub streams: Vec<PendingStreamInfo>,
    pub workers: Vec<WorkerClassLoadInfo>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct WorkerPingInfo {
    pub worker_id: String,
    pub hostname: String,
    pub pid: i32,
    pub round_trip_ms: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct LivenessReport {
    pub db_latency_ms: Option<f64>,
    pub db_reachable: bool,
    pub workers: Vec<WorkerPingInfo>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct WorkerHistoryPoint {
    #[serde(serialize_with = "time_text::utc")]
    pub snapshot_at: DateTime<Utc>,
    pub tasks_running: i32,
    pub tasks_claimed: i32,
    pub cpu_percent: Option<f64>,
    pub memory_usage_mb: Option<f64>,
    pub memory_percent: Option<f64>,
}

/// One row of `horsies_schedule_state`. `last_task_id` is the `uuid` column
/// as a task id (a task id is a UUID in the public API); it serializes as the
/// hyphenated lowercase text, as Python's `str` of the id. `next_run_at` is
/// NOT NULL in the baseline, so every row has one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct ScheduleStateInfo {
    pub schedule_name: String,
    #[serde(serialize_with = "time_text::optional_utc")]
    pub last_run_at: Option<DateTime<Utc>>,
    #[serde(serialize_with = "time_text::utc")]
    pub next_run_at: DateTime<Utc>,
    pub last_task_id: Option<Uuid>,
    pub run_count: i32,
    #[serde(serialize_with = "time_text::utc")]
    pub updated_at: DateTime<Utc>,
}

/// The task statuses that an operator cancel acts on (Python
/// `CancelPolicy`). The web body writes it as `"PENDING_OR_CLAIMED_ONLY"` or
/// `"INCLUDE_RUNNING"`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CancelPolicy {
    /// A `PENDING` or `CLAIMED` task is cancelled; a `RUNNING` task is not
    /// cancellable. The policy of a cancel body with no `policy` key.
    #[default]
    PendingOrClaimedOnly,
    /// A `RUNNING` task is cancelled too.
    IncludeRunning,
}

/// The JSON body of `POST /api/tasks/{task_id}/cancel`. An empty body, and
/// a body with no `policy` key, cancel with
/// [`CancelPolicy::PendingOrClaimedOnly`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelTaskBody {
    #[serde(default)]
    pub policy: CancelPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionResponse {
    pub outcome: String,
    pub was_status: Option<String>,
    pub warning: Option<String>,
}
