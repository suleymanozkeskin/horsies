//! Typed monitoring reads over a horsies deployment.
//!
//! This module has no dependency on the optional web transport. A task that
//! the detail read does not find is `Ok(TaskDetailRead::NotFound)` with its
//! absence reason; other missing rows are returned as `Ok(None)`. Database
//! and retained-history contract failures are returned as typed errors.

mod actions;
mod errors;
#[cfg(test)]
mod execution_classes;
mod failure_status;
pub mod fleet;
mod function_rows;
mod history_window;
mod models;
mod queries;
mod task_actions;
pub(crate) mod time_text;
mod timing;
mod timing_models;

pub use actions::{
    cancel_task_action, cancel_workflow_action, connect_failed_action, pause_workflow_action,
    resume_workflow_action, task_action_outcome, ActionBody, ActionConflictCode, ActionOutcome,
    INTERNAL_SERVER_ERROR_TEXT, RESUME_RECOVERY_WARNING, STATE_CONFLICT,
};

/// The health records of the worker routes: one type per record, the
/// broker's own (`crate::broker`).
pub use crate::broker::{
    BudgetLoad, DownSlot, DownSlots, ExecutionClassReport, PendingStream, SlotHealth,
    SnapshotProfile, StreamCoverageState, StreamWait, WorkerExecutionClassLoad, WorkerPong,
};
pub use crate::core::history::reads::detail::TaskAbsenceReason;
/// The window that the list and aggregate queries take; the monitoring
/// window resolver builds it.
pub use crate::core::history::reads::pages::MonitoringWindow;
pub use errors::{
    HistoryContractSource, MonitoringQueryError, MonitoringQueryErrorCode,
    MonitoringQueryErrorSource, MonitoringQueryRefusalCode, MonitoringResult,
};
pub use failure_status::database_failure_status;
pub use history_window::{
    resolve_monitoring_window, WindowRefused, MONITORING_WINDOW_DEFAULT, MONITORING_WINDOW_MAX,
};
pub use models::{
    ActionResponse, Breakdown, BreakdownCounts, BreakdownGroup, CancelPolicy, CancelTaskBody,
    DatabaseLiveness, ErrorCategory, ErrorFacet, FacetValue, Facets, GroupRow, LivenessReport,
    ScheduleStateInfo, StatusCount, TaskAttemptDetail, TaskDetail, TaskDetailNotFound,
    TaskDetailRead, TaskListPage, TaskRowInfo, TaskSummary, WorkerHistoryPoint, WorkerStateInfo,
    WorkflowEdge, WorkflowNodeDetail, WorkflowNodeSummary, WorkflowRunDetail, WorkflowRunSummary,
};
pub use queries::{
    categorize_error_code, duration, elapsed, get_task_detail, get_workflow_node, get_workflow_run,
    list_schedules, list_tasks, list_workflow_names, list_workflow_runs, normalize_optional_text,
    task_breakdown, task_facets, task_status_counts, PageRefused, SortDirection,
    TaskBreakdownQuery, TaskFacetsQuery, TaskFilters, TaskGroupBy, TaskListQuery, TaskSortField,
    TaskStatusCountsQuery, WorkflowRunsQuery, LIST_ROW_BOUND, MAX_BREAKDOWN_LIMIT,
    MAX_TASK_PAGE_LIMIT, MAX_TASK_PAGE_REACH, MAX_WORKFLOW_RUNS_LIMIT,
};
pub use task_actions::{
    cancel_task, TaskActionError, TaskActionErrorCode, TaskActionResult, TaskCancelled,
};
pub use timing_models::{
    AttemptClaimsInfo, AttemptFinalizingInfo, AttemptPhasesInfo, AttemptPlacementInfo,
    AttemptRecordedByInfo, MissingRecord, NodeEndpointInfo, NodeTimingInfo, PauseEndInfo,
    PauseIntervalInfo, RunConflictInfo, RunCoverageInfo, RunTimingInfo, SegmentClaimInfo,
    SegmentEndInfo, SegmentPhaseInfo, SegmentSourceInfo, TerminalNodeStatus, TimingBoundary,
    TimingConflictInfo, TimingSegmentInfo, UnavailableReason, UnknownReason, WorkflowEventKind,
};

#[cfg(test)]
mod action_tests;
#[cfg(test)]
mod aggregate_spec_tests;
#[cfg(test)]
mod cancel_spec_tests;
#[cfg(test)]
mod list_detail_spec_tests;
#[cfg(test)]
mod model_contract_tests;
#[cfg(test)]
mod spec_support;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod timing_spec_tests;
#[cfg(test)]
mod timing_tests;
#[cfg(test)]
mod workflow_schedule_spec_tests;
