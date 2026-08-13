//! Typed monitoring reads over a horsies deployment.
//!
//! This module has no dependency on the optional web transport. Missing rows
//! are returned as `Ok(None)`. Database and retained-history contract failures
//! are returned as typed errors.

mod errors;
mod history_window;
mod models;
mod queries;

pub use errors::{
    MonitoringQueryError, MonitoringQueryErrorCode, MonitoringQueryErrorSource, MonitoringResult,
};
pub use history_window::{
    resolve_monitoring_window, WindowRefused, MONITORING_WINDOW_DEFAULT, MONITORING_WINDOW_MAX,
};
pub use models::{
    Breakdown, ErrorCategory, ErrorFacet, FacetValue, Facets, GroupRow, LeafTaskInfo,
    LivenessReport, ScheduleStateInfo, StatusCount, TaskAttemptInfo, TaskDetail, TaskListPage,
    TaskSummary, WorkerHistoryPoint, WorkerPingInfo, WorkerStateInfo, WorkflowEdge,
    WorkflowNodeInfo, WorkflowRunDetail, WorkflowRunSummary, WorkflowTaskDetail,
};
pub use queries::{
    categorize_error_code, elapsed_s, get_task_detail, get_workflow_node, get_workflow_run,
    list_schedules, list_tasks, list_workflow_names, list_workflow_runs, normalize_optional_text,
    span_s, task_breakdown, task_facets, task_stats, PaginationRefused, SortDirection,
    TaskBreakdownQuery, TaskFacetsQuery, TaskFilters, TaskGroupBy, TaskListQuery, TaskSortField,
    TaskStatsQuery, WorkflowRunsQuery, MAX_TASK_PAGE_REACH,
};

#[cfg(test)]
mod tests;
