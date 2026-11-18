//! Monitoring reads and actions over the app, on the core's monitoring
//! queries (Python `horsies.monitoring`).
//!
//! Each function takes the app in place of a broker: it connects the app's
//! handle (in a worker child, the child's producer handle) and runs the
//! core's function of the same name. No function installs the schema, as
//! Python's monitoring reads do not.
//!
//! - A read returns [`MonitoringResult`]. A connect that fails is
//!   `DB_OPERATION_FAILED` with the read's name and the cause, and with the
//!   retry rule of the cause; no statement ran.
//! - The task detail answers [`TaskDetailRead::Found`], or
//!   [`TaskDetailRead::NotFound`] with its [`TaskAbsenceReason`]. A run or a
//!   node that no row holds is `Ok(None)`.
//! - The list and aggregate reads take a query built on a [`HistoryWindow`],
//!   which only [`resolve_monitoring_window`] builds.
//! - [`cancel_task`] writes; the `*_action` functions write and answer as
//!   the monitoring web routes do ([`ActionOutcome`]).
//! - In a process that was forked without `exec` after the core's process
//!   runtime started, every function panics before it connects: a fork is a
//!   state of the process, never a result.

use std::future::Future;
use std::sync::Arc;

use horsies_core::monitoring as core_monitoring;
use horsies_core::{BrokerError, ChildSpecSource, PostgresBroker};
use uuid::Uuid;

use crate::process_state::refuse_in_a_fork;
use crate::Horsies;

pub use horsies_core::monitoring::resolve_monitoring_window;
pub use horsies_core::monitoring::{
    categorize_error_code, elapsed_s, span_s, task_action_outcome, ActionBody, ActionConflictCode,
    ActionOutcome, ActionResponse, Breakdown, BreakdownCounts, BreakdownGroup, BudgetLoadInfo,
    CancelTaskBody, ClassReportInfo, DownSlotInfo, ErrorCategory, ErrorFacet, FacetValue, Facets,
    GroupRow, HistoryWindow, LeafTaskInfo, LivenessReport, MonitoringQueryError,
    MonitoringQueryErrorCode, MonitoringQueryErrorSource, MonitoringResult, PaginationRefused,
    PendingStreamInfo, ScheduleStateInfo, SlotHealthKind, SortDirection, StatusCount,
    StreamCoverageInfo, StreamWaitInfo, TaskAbsenceReason, TaskActionError, TaskActionErrorCode,
    TaskActionResult, TaskAttemptInfo, TaskBreakdownQuery, TaskCancelled, TaskDetail,
    TaskDetailNotFound, TaskDetailRead, TaskFacetsQuery, TaskFilters, TaskGroupBy, TaskListPage,
    TaskListQuery, TaskSortField, TaskStatsQuery, TaskSummary, WindowRefused, WorkerClassLoadInfo,
    WorkerHistoryPoint, WorkerPingInfo, WorkerStateInfo, WorkflowEdge, WorkflowNodeInfo,
    WorkflowRunDetail, WorkflowRunSummary, WorkflowRunsQuery, WorkflowTaskDetail,
    INTERNAL_SERVER_ERROR_TEXT, LIST_ROW_BOUND, MAX_BREAKDOWN_LIMIT, MAX_TASK_PAGE_LIMIT,
    MAX_TASK_PAGE_REACH, MAX_WORKFLOW_RUNS_LIMIT, MONITORING_WINDOW_DEFAULT, MONITORING_WINDOW_MAX,
    RESUME_RECOVERY_WARNING, STATE_CONFLICT,
};

/// The count of each task status over the live table and the history
/// window, all seven statuses in their fixed order.
///
/// Errors: `DB_OPERATION_FAILED`; `HISTORY_CONTRACT_VIOLATION` for a history
/// row whose status is not a task status.
pub async fn task_stats(
    app: &Horsies,
    query: &TaskStatsQuery,
) -> MonitoringResult<Vec<StatusCount>> {
    let broker = monitoring_broker(app, "task stats query").await?;
    core_monitoring::task_stats(&broker, query).await
}

/// The distinct workers, task names, queues and error codes with counts.
///
/// Errors: as [`task_stats`].
pub async fn task_facets(app: &Horsies, query: &TaskFacetsQuery) -> MonitoringResult<Facets> {
    let broker = monitoring_broker(app, "task facets query").await?;
    core_monitoring::task_facets(&broker, query).await
}

/// The task counts per worker, task name or queue, highest total first.
///
/// Errors: as [`task_stats`].
pub async fn task_breakdown(
    app: &Horsies,
    query: &TaskBreakdownQuery,
) -> MonitoringResult<Breakdown> {
    let broker = monitoring_broker(app, "task breakdown query").await?;
    core_monitoring::task_breakdown(&broker, query).await
}

/// One page of the task list over the live table and the history window,
/// with its total.
///
/// Errors: as [`task_stats`].
pub async fn list_tasks(app: &Horsies, query: &TaskListQuery) -> MonitoringResult<TaskListPage> {
    let broker = monitoring_broker(app, "task list query").await?;
    core_monitoring::list_tasks(&broker, query).await
}

/// The task `task_id`: found in the live table or in the history, or not
/// found with its absence reason.
///
/// Errors: `HISTORY_CONTRACT_VIOLATION` for a history row that does not
/// decode; `DB_OPERATION_FAILED` otherwise.
pub async fn get_task_detail(app: &Horsies, task_id: Uuid) -> MonitoringResult<TaskDetailRead> {
    let broker = monitoring_broker(app, "task detail query").await?;
    core_monitoring::get_task_detail(&broker, task_id).await
}

/// The distinct names of the root runs, at most [`LIST_ROW_BOUND`].
///
/// Errors: `DB_OPERATION_FAILED`.
pub async fn list_workflow_names(app: &Horsies) -> MonitoringResult<Vec<String>> {
    let broker = monitoring_broker(app, "workflow names query").await?;
    core_monitoring::list_workflow_names(&broker).await
}

/// The newest root runs, filtered by name and status.
///
/// Errors: `DB_OPERATION_FAILED`.
pub async fn list_workflow_runs(
    app: &Horsies,
    query: &WorkflowRunsQuery,
) -> MonitoringResult<Vec<WorkflowRunSummary>> {
    let broker = monitoring_broker(app, "workflow runs query").await?;
    core_monitoring::list_workflow_runs(&broker, query).await
}

/// The run `workflow_id` with its nodes and edges, or `None` when no run
/// has the id.
///
/// Errors: `DB_OPERATION_FAILED`.
pub async fn get_workflow_run(
    app: &Horsies,
    workflow_id: Uuid,
) -> MonitoringResult<Option<WorkflowRunDetail>> {
    let broker = monitoring_broker(app, "workflow run detail query").await?;
    core_monitoring::get_workflow_run(&broker, workflow_id).await
}

/// The node `task_index` of the run `workflow_id`, with its task and the
/// task's attempts, or `None` when the run has no such node.
///
/// Errors: as [`get_task_detail`].
pub async fn get_workflow_node(
    app: &Horsies,
    workflow_id: Uuid,
    task_index: i32,
) -> MonitoringResult<Option<WorkflowTaskDetail>> {
    let broker = monitoring_broker(app, "workflow node detail query").await?;
    core_monitoring::get_workflow_node(&broker, workflow_id, task_index).await
}

/// Every schedule state, soonest next run first, at most
/// [`LIST_ROW_BOUND`].
///
/// Errors: `DB_OPERATION_FAILED`.
pub async fn list_schedules(app: &Horsies) -> MonitoringResult<Vec<ScheduleStateInfo>> {
    let broker = monitoring_broker(app, "schedule state query").await?;
    core_monitoring::list_schedules(&broker).await
}

/// Move a live task that is not a workflow task to CANCELLED (Python
/// `horsies.monitoring.cancel_task`). PENDING and CLAIMED are eligible;
/// RUNNING only with `include_running`. Writes.
///
/// Errors: `TASK_NOT_FOUND`, `TASK_IS_WORKFLOW_TASK`,
/// `TASK_NOT_CANCELLABLE` (with the current status), `STORED_DATA_INVALID`,
/// and `DB_OPERATION_FAILED`, also for a failed connect (nothing written).
pub async fn cancel_task(
    app: &Horsies,
    task_id: Uuid,
    include_running: bool,
) -> TaskActionResult<TaskCancelled> {
    refuse_in_a_fork(format_args!("Task cancel of task {task_id}"));
    match app.reads_handle().broker().await {
        Ok(broker) => core_monitoring::cancel_task(&broker, task_id, include_running).await,
        Err(error) => Err(TaskActionError::DbOperationFailed {
            message: format!("Task cancel failed: {error}"),
            retryable: error.is_retryable(),
            task_id,
            cause: error.failure_cause(),
        }),
    }
}

/// [`cancel_task`], answered as the web route `POST /api/tasks/{id}/cancel`
/// answers it. Writes.
pub async fn cancel_task_action(
    app: &Horsies,
    task_id: Uuid,
    include_running: bool,
) -> ActionOutcome {
    task_action_outcome(cancel_task(app, task_id, include_running).await)
}

/// Pause the workflow `workflow_id`, answered as the web route
/// `POST /api/workflows/{id}/pause` answers it: 200, 404, 409 with the
/// current status, or 503 (a failed connect too). Writes.
pub async fn pause_workflow_action(app: &Horsies, workflow_id: Uuid) -> ActionOutcome {
    action(app, "Workflow pause", |broker| async move {
        core_monitoring::pause_workflow_action(&broker, workflow_id).await
    })
    .await
}

/// Resume the workflow `workflow_id`, answered as the web route
/// `POST /api/workflows/{id}/resume` answers it. The sub-workflow children
/// that the resume owes build from the workflow definitions registered on
/// `app`. Writes.
pub async fn resume_workflow_action(app: &Horsies, workflow_id: Uuid) -> ActionOutcome {
    let children = resume_children(app);
    action(app, "Workflow resume", |broker| async move {
        core_monitoring::resume_workflow_action(&broker, workflow_id, children.as_ref()).await
    })
    .await
}

/// Cancel the workflow `workflow_id`, answered as the web route
/// `POST /api/workflows/{id}/cancel` answers it. Writes.
pub async fn cancel_workflow_action(app: &Horsies, workflow_id: Uuid) -> ActionOutcome {
    action(app, "Workflow cancel", |broker| async move {
        core_monitoring::cancel_workflow_action(&broker, workflow_id).await
    })
    .await
}

/// Where a resume of `app` builds each owed sub-workflow child: the
/// workflow registry of `app`, with the definitions registered when the
/// child builds.
pub(crate) fn resume_children(app: &Horsies) -> Arc<dyn ChildSpecSource> {
    Arc::clone(app.workflows()) as Arc<dyn ChildSpecSource>
}

/// The connected broker of `app`'s read handle, for the monitoring read
/// `operation`. Installs no schema.
///
/// Errors: `DB_OPERATION_FAILED` with the retry rule of the failed connect.
///
/// # Panics
///
/// In a fork, without `exec`, of the process that started the core's
/// process runtime. Nothing ran.
async fn monitoring_broker(
    app: &Horsies,
    operation: &str,
) -> MonitoringResult<Arc<PostgresBroker>> {
    refuse_in_a_fork(format_args!("the monitoring {operation}"));
    app.reads_handle()
        .broker()
        .await
        .map_err(|error| connect_failed(operation, error))
}

/// The error of a failed connect before the read `operation`. Pure.
fn connect_failed(operation: &str, error: BrokerError) -> MonitoringQueryError {
    MonitoringQueryError::DbOperationFailed {
        message: format!("{operation} failed: {error}"),
        retryable: error.is_retryable(),
        source: MonitoringQueryErrorSource::Broker(error),
    }
}

/// Run the workflow action `run` on `app`'s broker; a failed connect is
/// answered 503 with the cause (`<subject> failed: <cause>`).
///
/// # Panics
///
/// In a fork, without `exec`, of the process that started the core's
/// process runtime. Nothing ran.
async fn action<F, Fut>(app: &Horsies, subject: &str, run: F) -> ActionOutcome
where
    F: FnOnce(Arc<PostgresBroker>) -> Fut,
    Fut: Future<Output = ActionOutcome>,
{
    refuse_in_a_fork(format_args!("{subject}"));
    match app.reads_handle().broker().await {
        Ok(broker) => run(broker).await,
        Err(error) => core_monitoring::connect_failed_action(&format!("{subject} failed: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppConfig;

    /// An app with no task and no workflow. Building it does no I/O.
    fn app() -> Horsies {
        Horsies::new(AppConfig::for_database_url(
            "postgresql://localhost/horsies",
        ))
        .unwrap()
    }

    #[test]
    fn a_failed_connect_is_db_operation_failed_with_the_read_and_the_retry_rule() {
        let error = connect_failed("task list query", BrokerError::ListenerClosed);
        assert_eq!(error.code(), MonitoringQueryErrorCode::DbOperationFailed);
        assert!(error.message().starts_with("task list query failed: "));
        assert_eq!(
            error.retryable(),
            BrokerError::ListenerClosed.is_retryable()
        );
        assert!(matches!(
            error,
            MonitoringQueryError::DbOperationFailed {
                source: MonitoringQueryErrorSource::Broker(BrokerError::ListenerClosed),
                ..
            }
        ));
    }

    /// A resume builds each owed child through the app's registry: a
    /// definition key that the app has not registered fails the node with
    /// `SUBWORKFLOW_LOAD_FAILED`.
    #[tokio::test]
    async fn a_resume_builds_children_from_the_app_s_definitions() {
        let app = app();
        let source = resume_children(&app);
        let request = horsies_core::ChildSpecRequest {
            parent_workflow_id: Uuid::nil(),
            parent_workflow_name: "parent".to_owned(),
            task_index: 0,
            child_depth: 1,
            definition_key: Some("tests.absent.v1".to_owned()),
            params: horsies_core::SubWorkflowParams::NotGiven,
            node_input: "{}".to_owned(),
            args_from: None,
            sources: Vec::new(),
        };
        match source.child_spec(request).await {
            Ok(horsies_core::ClientChildSpec::NodeFails { failure_result, .. }) => {
                assert!(
                    failure_result.contains("SUBWORKFLOW_LOAD_FAILED"),
                    "{failure_result}"
                );
            }
            other => panic!("expected the node to fail its load, got {other:?}"),
        }
    }
}
