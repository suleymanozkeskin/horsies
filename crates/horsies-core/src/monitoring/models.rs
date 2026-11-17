//! Response models for the transport-free monitoring API.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::time_text;
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct TaskAttemptInfo {
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
}

/// The task row behind a task detail or a workflow node (Python
/// `LeafTaskInfo`). `execution` is the stored class of a live task, and
/// `None` for a task read from history, which does not keep the class.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct LeafTaskInfo {
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
    pub queue_s: Option<i64>,
    pub exec_s: Option<i64>,
    pub worker_hostname: Option<String>,
    #[serde(serialize_with = "time_text::optional_utc")]
    pub good_until: Option<DateTime<Utc>>,
    pub execution: Option<Execution>,
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
/// `BreakdownGroup`, `python/horsies/monitoring/models.py`): `value`, the tasks
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct TaskSummary {
    pub id: Uuid,
    pub task_name: String,
    pub queue_name: String,
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
    pub queue_s: Option<i64>,
    pub exec_s: Option<i64>,
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
    pub leaf: LeafTaskInfo,
    pub task_name: String,
    pub queue_name: String,
    pub priority: i32,
    pub is_workflow_task: bool,
    pub error_category: Option<String>,
    pub attempts: Vec<TaskAttemptInfo>,
    pub workflow_id: Option<Uuid>,
    pub workflow_task_index: Option<i32>,
}

/// The answer of the task detail read (Python `TaskDetail |
/// TaskDetailNotFound`).
#[derive(Debug, Clone, PartialEq)]
pub enum TaskDetailRead {
    /// The live row, or the history row when the live table has no row.
    Found(TaskDetail),
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct WorkflowRunSummary {
    pub id: Uuid,
    pub name: String,
    pub definition_key: Option<String>,
    pub status: String,
    #[serde(serialize_with = "time_text::optional_utc")]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(serialize_with = "time_text::optional_utc")]
    pub completed_at: Option<DateTime<Utc>>,
    pub wall_s: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct WorkflowNodeInfo {
    pub task_index: i32,
    pub node_id: Option<String>,
    pub task_name: String,
    pub node_status: String,
    pub is_subworkflow: bool,
    pub sub_workflow_id: Option<Uuid>,
    pub allow_failed_deps: bool,
    #[serde(serialize_with = "time_text::optional_utc")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(serialize_with = "time_text::optional_utc")]
    pub completed_at: Option<DateTime<Utc>>,
    pub exec_s: Option<i64>,
    pub child_total: Option<i64>,
    pub child_failed: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct WorkflowEdge {
    pub from_index: i32,
    pub to_index: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct WorkflowRunDetail {
    pub run: WorkflowRunSummary,
    pub nodes: Vec<WorkflowNodeInfo>,
    pub edges: Vec<WorkflowEdge>,
    pub failed_count: i64,
    pub failed_indices: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct WorkflowTaskDetail {
    pub task_index: i32,
    pub node_id: Option<String>,
    pub task_name: String,
    pub node_status: String,
    pub is_subworkflow: bool,
    pub node_error: Option<String>,
    pub leaf: Option<LeafTaskInfo>,
    pub attempts: Vec<TaskAttemptInfo>,
}

/// One slot that was down when the worker wrote its snapshot (Python
/// `DownSlotInfo`, `python/horsies/monitoring/models.py:307-318`).
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

/// Whether any slot of a worker was down (Python `SlotHealthKind`,
/// `python/horsies/monitoring/execution_classes.py:42`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SlotHealthKind {
    AllServing,
    SlotsDown,
}

/// Latest state snapshot for one worker (Python `WorkerStateInfo`,
/// `python/horsies/monitoring/models.py:321-351`).
///
/// `down_slots` lists the down slots in slot order; it is empty unless
/// `slot_health` is `slots_down`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct WorkerStateInfo {
    pub worker_id: String,
    pub hostname: String,
    pub pid: i32,
    #[serde(serialize_with = "time_text::utc")]
    pub snapshot_at: DateTime<Utc>,
    pub snapshot_age_s: Option<i64>,
    pub stale: bool,
    #[serde(serialize_with = "time_text::utc")]
    pub worker_started_at: DateTime<Utc>,
    pub uptime_s: Option<i64>,
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
}

/// One claim budget of a live worker (Python `BudgetLoadInfo`,
/// `python/horsies/monitoring/models.py:354-367`).
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
/// `python/horsies/monitoring/models.py:378-400`).
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
/// `python/horsies/monitoring/models.py:403-415`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StreamWaitInfo {
    /// At least one row is due. `wait_s` is the wait of the oldest due row,
    /// in whole seconds.
    Waiting {
        #[serde(serialize_with = "time_text::utc")]
        oldest_due_at: DateTime<Utc>,
        wait_s: i64,
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
/// `ClassReportInfo`, `python/horsies/monitoring/models.py:433-447`).
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelTaskBody {
    #[serde(default)]
    pub include_running: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionResponse {
    pub outcome: String,
    pub was_status: Option<String>,
    pub warning: Option<String>,
}
