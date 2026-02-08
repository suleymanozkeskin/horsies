use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use strum::Display;

use crate::tui::NotifyBatch;

/// State of the PostgreSQL NOTIFY listener connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ListenerState {
    #[default]
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
}

use crate::models::{
    ActiveWorkerRow, AggregatedBreakdownRow, ClusterCapacitySummary, ClusterUtilizationPoint,
    DeadWorkerRow, OverloadedWorkerAlert, SnapshotAgeBucket, StaleClaimsAlert, TaskDetail,
    TaskStatusRow, WorkerLoadPoint, WorkerQueuesRow, WorkerUptimeRow, WorkflowRow,
    WorkflowSummary, WorkflowTaskRow,
};

#[derive(Debug, Clone, Display)]
pub enum Action {
    Tick,
    Render,
    Resize(u16, u16),
    Suspend,
    Resume,
    Quit,
    ClearScreen,
    Error(String),
    ToggleHelp,
    NextTheme,

    // Tab navigation
    SwitchTab(Tab),

    // Worker navigation (for Workers tab)
    NavigateWorkerUp,
    NavigateWorkerDown,
    NavigateTaskUp,
    NavigateTaskDown,

    // Task detail modal
    OpenTaskDetail(String),  // task_id
    CloseTaskDetail,
    ScrollTaskDetailUp,
    ScrollTaskDetailDown,
    CopyTaskToClipboard,
    NavigateTaskDetailNext,   // Go to next task in list while modal is open
    NavigateTaskDetailPrev,   // Go to previous task in list while modal is open

    // Task status filter (Tasks tab)
    ToggleTaskStatusFilter(TaskStatus),
    SelectAllTaskStatuses,
    ClearTaskStatuses,
    ToggleTaskRow,  // Expand/collapse selected worker row
    NavigateTaskIdUp,
    NavigateTaskIdDown,

    // Page/Home/End navigation (tab-agnostic, dispatched contextually)
    NavigatePageUp,
    NavigatePageDown,
    NavigateHome,
    NavigateEnd,

    // Time window selection (for Workers tab charts)
    CycleTimeWindowForward,
    CycleTimeWindowBackward,

    // Manual refresh
    RefreshCurrentTab,
    RefreshDashboard,
    RefreshWorkers,
    RefreshTasks,
    RefreshMaintenance,
    RefreshWorkflows,

    // Workflow navigation
    NavigateWorkflowUp,
    NavigateWorkflowDown,
    OpenWorkflowDetail(String),  // workflow_id
    CloseWorkflowDetail,
    ScrollWorkflowDetailUp,
    ScrollWorkflowDetailDown,
    NavigateWorkflowDetailNext,  // Go to next workflow while modal is open
    NavigateWorkflowDetailPrev,  // Go to previous workflow while modal is open
    CopyWorkflowToClipboard,     // Copy workflow detail JSON to clipboard

    // Workflow status filter
    ToggleWorkflowStatusFilter(WorkflowStatus),
    SelectAllWorkflowStatuses,
    ClearWorkflowStatuses,

    // Error handling
    OpenErrorModal,           // Open modal showing all errors
    CloseErrorModal,          // Close error modal
    CopyErrorToClipboard,     // Copy current error message to clipboard
    ClearAllErrors,           // Clear all error messages

    // Data loading actions
    DataLoaded(DataUpdate),
    DataLoadError(String, DataSource),
    StartLoading(DataSource),

    // Search actions
    OpenSearch,
    CloseSearch,
    SearchInput(char),
    SearchBackspace,
    SearchClear,
    SearchSelectUp,
    SearchSelectDown,
    SearchConfirm,

    // NOTIFY/LISTEN actions
    NotifyRefresh(NotifyBatch),
    ListenerStateChanged(ListenerState),
}

/// Represents a search match result
#[derive(Debug, Clone)]
pub enum SearchMatch {
    /// Worker tab match - navigate to worker row
    Worker {
        worker_id: String,
        hostname: String,
        status: String,
    },
    /// Task tab match - navigate to task
    Task {
        task_id: String,
        worker_id: String,
        status: String,
    },
    /// Workflow tab match - navigate to workflow row
    Workflow {
        workflow_id: String,
        name: String,
        status: String,
    },
    /// Modal content match - scroll to line
    ModalLine {
        line_number: usize,
        content: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, Serialize, Deserialize)]
pub enum TimeWindow {
    FiveMinutes,
    ThirtyMinutes,
    OneHour,
    SixHours,
    TwentyFourHours,
}

impl TimeWindow {
    pub fn label(&self) -> &'static str {
        match self {
            TimeWindow::FiveMinutes => "5m",
            TimeWindow::ThirtyMinutes => "30m",
            TimeWindow::OneHour => "1h",
            TimeWindow::SixHours => "6h",
            TimeWindow::TwentyFourHours => "24h",
        }
    }

    pub fn interval(&self) -> &'static str {
        match self {
            TimeWindow::FiveMinutes => "5 minutes",
            TimeWindow::ThirtyMinutes => "30 minutes",
            TimeWindow::OneHour => "1 hour",
            TimeWindow::SixHours => "6 hours",
            TimeWindow::TwentyFourHours => "24 hours",
        }
    }

    pub fn next(&self) -> Self {
        match self {
            TimeWindow::FiveMinutes => TimeWindow::ThirtyMinutes,
            TimeWindow::ThirtyMinutes => TimeWindow::OneHour,
            TimeWindow::OneHour => TimeWindow::SixHours,
            TimeWindow::SixHours => TimeWindow::TwentyFourHours,
            TimeWindow::TwentyFourHours => TimeWindow::FiveMinutes,
        }
    }

    pub fn prev(&self) -> Self {
        match self {
            TimeWindow::FiveMinutes => TimeWindow::TwentyFourHours,
            TimeWindow::ThirtyMinutes => TimeWindow::FiveMinutes,
            TimeWindow::OneHour => TimeWindow::ThirtyMinutes,
            TimeWindow::SixHours => TimeWindow::OneHour,
            TimeWindow::TwentyFourHours => TimeWindow::SixHours,
        }
    }
}

/// Individual task status values
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, Serialize, Deserialize)]
pub enum TaskStatus {
    Pending,
    Claimed,
    Running,
    Completed,
    Failed,
}

impl TaskStatus {
    pub fn all() -> Vec<Self> {
        vec![
            TaskStatus::Pending,
            TaskStatus::Claimed,
            TaskStatus::Running,
            TaskStatus::Completed,
            TaskStatus::Failed,
        ]
    }

    pub fn label(&self) -> &'static str {
        match self {
            TaskStatus::Pending => "Pending",
            TaskStatus::Claimed => "Claimed",
            TaskStatus::Running => "Running",
            TaskStatus::Completed => "Completed",
            TaskStatus::Failed => "Failed",
        }
    }

    pub fn db_value(&self) -> &'static str {
        match self {
            TaskStatus::Pending => "PENDING",
            TaskStatus::Claimed => "CLAIMED",
            TaskStatus::Running => "RUNNING",
            TaskStatus::Completed => "COMPLETED",
            TaskStatus::Failed => "FAILED",
        }
    }
}

/// Multi-select filter for task statuses
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskStatusFilter {
    pub selected: HashSet<TaskStatus>,
}

impl Default for TaskStatusFilter {
    fn default() -> Self {
        // Default: show Claimed + Running (active tasks)
        let mut selected = HashSet::new();
        selected.insert(TaskStatus::Claimed);
        selected.insert(TaskStatus::Running);
        Self { selected }
    }
}

impl TaskStatusFilter {
    pub fn toggle(&mut self, status: TaskStatus) {
        if self.selected.contains(&status) {
            self.selected.remove(&status);
        } else {
            self.selected.insert(status);
        }
    }

    pub fn is_selected(&self, status: &TaskStatus) -> bool {
        self.selected.contains(status)
    }

    pub fn select_all(&mut self) {
        self.selected = TaskStatus::all().into_iter().collect();
    }

    pub fn clear(&mut self) {
        self.selected.clear();
    }

    /// Get SQL-compatible list of status strings for query
    pub fn to_sql_values(&self) -> Vec<&'static str> {
        self.selected.iter().map(|s| s.db_value()).collect()
    }
}

/// Individual workflow status values
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, Serialize, Deserialize)]
pub enum WorkflowStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Paused,
    Cancelled,
}

impl WorkflowStatus {
    pub fn all() -> Vec<Self> {
        vec![
            WorkflowStatus::Pending,
            WorkflowStatus::Running,
            WorkflowStatus::Completed,
            WorkflowStatus::Failed,
            WorkflowStatus::Paused,
            WorkflowStatus::Cancelled,
        ]
    }

    pub fn label(&self) -> &'static str {
        match self {
            WorkflowStatus::Pending => "Pending",
            WorkflowStatus::Running => "Running",
            WorkflowStatus::Completed => "Completed",
            WorkflowStatus::Failed => "Failed",
            WorkflowStatus::Paused => "Paused",
            WorkflowStatus::Cancelled => "Cancelled",
        }
    }

    pub fn db_value(&self) -> &'static str {
        match self {
            WorkflowStatus::Pending => "PENDING",
            WorkflowStatus::Running => "RUNNING",
            WorkflowStatus::Completed => "COMPLETED",
            WorkflowStatus::Failed => "FAILED",
            WorkflowStatus::Paused => "PAUSED",
            WorkflowStatus::Cancelled => "CANCELLED",
        }
    }
}

/// Multi-select filter for workflow statuses
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowStatusFilter {
    pub selected: HashSet<WorkflowStatus>,
}

impl Default for WorkflowStatusFilter {
    fn default() -> Self {
        // Default: show all active workflows
        let mut selected = HashSet::new();
        selected.insert(WorkflowStatus::Pending);
        selected.insert(WorkflowStatus::Running);
        selected.insert(WorkflowStatus::Paused);
        Self { selected }
    }
}

impl WorkflowStatusFilter {
    pub fn toggle(&mut self, status: WorkflowStatus) {
        if self.selected.contains(&status) {
            self.selected.remove(&status);
        } else {
            self.selected.insert(status);
        }
    }

    pub fn is_selected(&self, status: &WorkflowStatus) -> bool {
        self.selected.contains(status)
    }

    pub fn select_all(&mut self) {
        self.selected = WorkflowStatus::all().into_iter().collect();
    }

    pub fn clear(&mut self) {
        self.selected.clear();
    }

    /// Get SQL-compatible list of status strings for query
    pub fn to_sql_values(&self) -> Vec<&'static str> {
        self.selected.iter().map(|s| s.db_value()).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Display, Serialize, Deserialize)]
pub enum Tab {
    Dashboard,
    Workers,
    Tasks,
    Workflows,
    Maintenance,
}

#[derive(Debug, Clone)]
pub enum DataUpdate {
    ClusterSummary(ClusterCapacitySummary),
    TaskStatusView(Vec<TaskStatusRow>),
    UtilizationTrend(Vec<ClusterUtilizationPoint>),
    Alerts(Vec<OverloadedWorkerAlert>, Vec<StaleClaimsAlert>),
    WorkerList(Vec<ActiveWorkerRow>),
    WorkerDetails(String, WorkerUptimeRow, WorkerQueuesRow),
    WorkerLoad(Vec<WorkerLoadPoint>),
    TaskAggregation(Vec<AggregatedBreakdownRow>),
    SnapshotAge(Vec<SnapshotAgeBucket>),
    DeadWorkers(Vec<DeadWorkerRow>),
    TaskDetailLoaded(TaskDetail),
    // Workflow data updates
    WorkflowSummary(WorkflowSummary),
    WorkflowList(Vec<WorkflowRow>),
    WorkflowDetailLoaded(WorkflowRow, Vec<WorkflowTaskRow>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, Serialize, Deserialize)]
pub enum DataSource {
    ClusterSummary,
    TaskStatus,
    UtilizationTrend,
    Alerts,
    WorkerList,
    WorkerDetails,
    TaskAggregation,
    SnapshotAge,
    DeadWorkers,
    TaskDetailData,
    // Workflow data sources
    WorkflowSummary,
    WorkflowList,
    WorkflowDetail,
}
