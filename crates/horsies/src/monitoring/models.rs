//! Response models for the transport-free monitoring API.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
pub struct TaskAttemptInfo {
    pub attempt: i32,
    pub outcome: String,
    pub will_retry: bool,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub failed_reason: Option<String>,
    pub worker_hostname: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LeafTaskInfo {
    pub task_id: Uuid,
    pub status: String,
    pub error_code: Option<String>,
    pub failed_reason: Option<String>,
    pub retry_count: i32,
    pub max_retries: i32,
    pub enqueued_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub queue_s: Option<i64>,
    pub exec_s: Option<i64>,
    pub worker_hostname: Option<String>,
    pub good_until: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusCount {
    pub status: String,
    pub count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FacetValue {
    pub value: String,
    pub count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorFacet {
    pub value: String,
    pub count: i64,
    pub category: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Facets {
    pub workers: Vec<FacetValue>,
    pub task_names: Vec<FacetValue>,
    pub queues: Vec<FacetValue>,
    pub error_codes: Vec<ErrorFacet>,
    pub error_category_totals: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupRow {
    pub group: String,
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

impl GroupRow {
    pub(crate) fn empty(group: impl Into<String>) -> Self {
        Self {
            group: group.into(),
            total: 0,
            pending: 0,
            claimed: 0,
            running: 0,
            completed: 0,
            failed: 0,
            cancelled: 0,
            expired: 0,
            retried: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Breakdown {
    pub group_by: String,
    pub groups: Vec<GroupRow>,
    pub total: GroupRow,
    pub group_count: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    pub enqueued_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub queue_s: Option<i64>,
    pub exec_s: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskListPage {
    pub rows: Vec<TaskSummary>,
    pub total: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowRunSummary {
    pub id: Uuid,
    pub name: String,
    pub definition_key: Option<String>,
    pub status: String,
    pub created_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub wall_s: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowNodeInfo {
    pub task_index: i32,
    pub node_id: Option<String>,
    pub task_name: String,
    pub node_status: String,
    pub is_subworkflow: bool,
    pub sub_workflow_id: Option<Uuid>,
    pub allow_failed_deps: bool,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub exec_s: Option<i64>,
    pub child_total: Option<i64>,
    pub child_failed: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowEdge {
    pub from_index: i32,
    pub to_index: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowRunDetail {
    pub run: WorkflowRunSummary,
    pub nodes: Vec<WorkflowNodeInfo>,
    pub edges: Vec<WorkflowEdge>,
    pub failed_count: i64,
    pub failed_indices: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerStateInfo {
    pub worker_id: String,
    pub hostname: String,
    pub pid: i32,
    pub snapshot_at: DateTime<Utc>,
    pub snapshot_age_s: Option<i64>,
    pub stale: bool,
    pub worker_started_at: DateTime<Utc>,
    pub uptime_s: Option<i64>,
    pub processes: i32,
    pub queues: Vec<String>,
    pub queue_max_concurrency: Option<BTreeMap<String, i32>>,
    pub tasks_running: i32,
    pub tasks_claimed: i32,
    pub cluster_wide_cap: Option<i32>,
    pub memory_usage_mb: Option<f64>,
    pub memory_percent: Option<f64>,
    pub cpu_percent: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerPingInfo {
    pub worker_id: String,
    pub hostname: String,
    pub pid: i32,
    pub round_trip_ms: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LivenessReport {
    pub db_latency_ms: Option<f64>,
    pub db_reachable: bool,
    pub workers: Vec<WorkerPingInfo>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerHistoryPoint {
    pub snapshot_at: DateTime<Utc>,
    pub tasks_running: i32,
    pub tasks_claimed: i32,
    pub cpu_percent: Option<f64>,
    pub memory_usage_mb: Option<f64>,
    pub memory_percent: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, sqlx::FromRow)]
pub struct ScheduleStateInfo {
    pub schedule_name: String,
    pub last_run_at: Option<DateTime<Utc>>,
    pub next_run_at: Option<DateTime<Utc>>,
    pub last_task_id: Option<String>,
    pub run_count: i32,
    pub updated_at: DateTime<Utc>,
}
