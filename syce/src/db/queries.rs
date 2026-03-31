use crate::errors::Result;
use crate::models::{
    ActiveWorkerRow, AggregatedBreakdownRow, ClusterCapacitySummary, ClusterUtilizationPoint,
    DeadWorkerRow, DistinctFilterRow, FilterValue, OverloadedWorkerAlert, SnapshotAgeBucket,
    StaleClaimsAlert, TaskAttemptRow, TaskDetail, TaskListRow, TaskStatusRow, WorkerLoadPoint,
    WorkerQueuesRow, WorkerUptimeRow, WorkflowRow, WorkflowSummary, WorkflowTaskRow,
};
use sqlx::PgPool;

/// Cluster capacity summary
/// Source: ../../sql/worker/cluster-capacity-summary.sql
pub async fn fetch_cluster_capacity_summary(pool: &PgPool) -> Result<ClusterCapacitySummary> {
    let _query = include_str!("../../sql/worker/cluster-capacity-summary.sql");
    let summary = sqlx::query_as::<_, ClusterCapacitySummary>(_query)
        .fetch_one(pool)
        .await?;
    Ok(summary)
}

/// Task counts by status with TOTAL row
/// Source: ../../sql/tasks/task-status-view.sql
pub async fn fetch_task_status_view(pool: &PgPool) -> Result<Vec<TaskStatusRow>> {
    let _query = include_str!("../../sql/tasks/task-status-view.sql");
    let rows = sqlx::query_as::<_, TaskStatusRow>(_query)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

/// Cluster-wide utilization trend (1-minute buckets, last hour)
/// Source: ../../sql/worker/cluster-utilization-trend.sql
pub async fn fetch_cluster_utilization_trend(
    pool: &PgPool,
) -> Result<Vec<ClusterUtilizationPoint>> {
    let _query = include_str!("../../sql/worker/cluster-utilization-trend.sql");
    let rows = sqlx::query_as::<_, ClusterUtilizationPoint>(_query)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

/// Overloaded workers (high memory or CPU)
/// Source: ../../sql/worker/overloaded-workers.sql
pub async fn fetch_overloaded_workers(pool: &PgPool) -> Result<Vec<OverloadedWorkerAlert>> {
    let _query = include_str!("../../sql/worker/overloaded-workers.sql");
    let rows = sqlx::query_as::<_, OverloadedWorkerAlert>(_query)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

/// Workers with high claimed/running ratio (potential stale claims)
/// Source: ../../sql/worker/stale-claims-detection.sql
pub async fn fetch_stale_claims(pool: &PgPool) -> Result<Vec<StaleClaimsAlert>> {
    let _query = include_str!("../../sql/worker/stale-claims-detection.sql");
    let rows = sqlx::query_as::<_, StaleClaimsAlert>(_query)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

/// Active workers (latest snapshot) for list views
/// Source: ../../sql/worker/active-workers.sql
pub async fn fetch_active_workers_list(pool: &PgPool) -> Result<Vec<ActiveWorkerRow>> {
    let _query = include_str!("../../sql/worker/active-workers.sql");
    let rows = sqlx::query_as::<_, ActiveWorkerRow>(_query)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

/// Worker uptime rows
/// Source: ../../sql/worker/worker-uptime.sql
pub async fn fetch_worker_uptime(pool: &PgPool) -> Result<Vec<WorkerUptimeRow>> {
    let _query = include_str!("../../sql/worker/worker-uptime.sql");
    let rows = sqlx::query_as::<_, WorkerUptimeRow>(_query)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

/// Worker queues/config rows
/// Source: ../../sql/worker/worker-queues.sql
pub async fn fetch_worker_queues(pool: &PgPool) -> Result<Vec<WorkerQueuesRow>> {
    let _query = include_str!("../../sql/worker/worker-queues.sql");
    let rows = sqlx::query_as::<_, WorkerQueuesRow>(_query)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

/// Worker load for a custom time window
/// Dynamically fetches worker load data for the specified time interval
pub async fn fetch_worker_load(pool: &PgPool, interval: &str) -> Result<Vec<WorkerLoadPoint>> {
    let rows = sqlx::query_as::<_, WorkerLoadPoint>(
        r#"
        SELECT
            worker_id,
            snapshot_at,
            tasks_running,
            tasks_claimed,
            memory_percent,
            cpu_percent
        FROM horsies_worker_states
        WHERE snapshot_at > NOW() - $1::interval
        ORDER BY worker_id, snapshot_at
        "#,
    )
    .bind(interval)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Dead workers (offline > 2 minutes)
/// Source: ../../sql/worker/dead-workers.sql
pub async fn fetch_dead_workers(pool: &PgPool) -> Result<Vec<DeadWorkerRow>> {
    let _query = include_str!("../../sql/worker/dead-workers.sql");
    let rows = sqlx::query_as::<_, DeadWorkerRow>(_query)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

/// Worker task aggregation per worker (with TOTAL)
/// Source: ../../sql/tasks/aggregated-breakdown.sql
pub async fn fetch_task_aggregation(
    pool: &PgPool,
    retried_only: bool,
) -> Result<Vec<AggregatedBreakdownRow>> {
    let query = include_str!("../../sql/tasks/aggregated-breakdown.sql");
    let rows = sqlx::query_as::<_, AggregatedBreakdownRow>(query)
        .bind(retried_only)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

/// Worker task aggregation filtered by status
/// Source: ../../sql/tasks/filtered-aggregation.sql
pub async fn fetch_filtered_task_aggregation(
    pool: &PgPool,
    statuses: &[&str],
    retried_only: bool,
) -> Result<Vec<AggregatedBreakdownRow>> {
    let query = include_str!("../../sql/tasks/filtered-aggregation.sql");
    let rows = sqlx::query_as::<_, AggregatedBreakdownRow>(query)
        .bind(statuses)
        .bind(retried_only)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

/// Fetch distinct task_names, queue_names, and error_codes with counts.
/// Returns three categorized lists parsed from a single UNION ALL query.
pub async fn fetch_distinct_filter_values(
    pool: &PgPool,
    worker_id: Option<&str>,
    statuses: &[&str],
    retried_only: bool,
) -> Result<(Vec<FilterValue>, Vec<FilterValue>, Vec<FilterValue>)> {
    let query = include_str!("../../sql/tasks/distinct-filter-values.sql");
    let rows = sqlx::query_as::<_, DistinctFilterRow>(query)
        .bind(worker_id)
        .bind(statuses)
        .bind(retried_only)
        .fetch_all(pool)
        .await?;

    let mut task_names = Vec::new();
    let mut queues = Vec::new();
    let mut errors = Vec::new();

    for row in rows {
        let fv = FilterValue {
            value: row.value,
            count: row.count,
        };
        match row.kind.as_str() {
            "task_name" => task_names.push(fv),
            "queue" => queues.push(fv),
            "error" => errors.push(fv),
            _ => {}
        }
    }

    Ok((task_names, queues, errors))
}

/// Task list for a specific worker with composable filters (Layer 2 drill-down).
/// Source: ../../sql/tasks/task-list-by-worker.sql
pub async fn fetch_task_list_by_worker(
    pool: &PgPool,
    worker_id: Option<&str>,
    statuses: &[&str],
    retried_only: bool,
    name_filter: &[String],
    queue_filter: &[String],
    error_filter: &[String],
) -> Result<Vec<TaskListRow>> {
    let query = include_str!("../../sql/tasks/task-list-by-worker.sql");
    let rows = sqlx::query_as::<_, TaskListRow>(query)
        .bind(worker_id)
        .bind(statuses)
        .bind(retried_only)
        .bind(name_filter)
        .bind(queue_filter)
        .bind(error_filter)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

/// Snapshot age distribution buckets
/// Source: ../../sql/worker/snapshot-age-distribution.sql
pub async fn fetch_snapshot_age_distribution(pool: &PgPool) -> Result<Vec<SnapshotAgeBucket>> {
    let _query = include_str!("../../sql/worker/snapshot-age-distribution.sql");
    let rows = sqlx::query_as::<_, SnapshotAgeBucket>(_query)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

/// Fetch a single task by ID for the detail view, including attempt history.
/// Source: ../../../sql/tasks/task-detail-by-id.sql
/// Source: ../../../sql/tasks/task-attempts-by-task-id.sql
pub async fn fetch_task_by_id(pool: &PgPool, task_id: &str) -> Result<Option<TaskDetail>> {
    let query = include_str!("../../sql/tasks/task-detail-by-id.sql");
    let task = sqlx::query_as::<_, TaskDetail>(query)
        .bind(task_id)
        .fetch_optional(pool)
        .await?;

    let Some(mut task) = task else {
        return Ok(None);
    };

    let attempts_query = include_str!("../../sql/tasks/task-attempts-by-task-id.sql");
    let attempts = sqlx::query_as::<_, TaskAttemptRow>(attempts_query)
        .bind(task_id)
        .fetch_all(pool)
        .await?;

    task.attempts = attempts;
    Ok(Some(task))
}

// =========== Workflow Queries ===========

/// Fetch workflow summary statistics for dashboard
/// Source: ../../../sql/workflows/workflow-summary.sql
pub async fn fetch_workflow_summary(pool: &PgPool) -> Result<WorkflowSummary> {
    let query = include_str!("../../sql/workflows/workflow-summary.sql");
    let summary = sqlx::query_as::<_, WorkflowSummary>(query)
        .fetch_one(pool)
        .await?;
    Ok(summary)
}

/// Fetch workflow list with task progress
/// Source: ../../../sql/workflows/workflow-list.sql
pub async fn fetch_workflow_list(pool: &PgPool) -> Result<Vec<WorkflowRow>> {
    let query = include_str!("../../sql/workflows/workflow-list.sql");
    let rows = sqlx::query_as::<_, WorkflowRow>(query)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

/// Fetch workflow list filtered by status
/// Source: ../../../sql/workflows/filtered-workflow-list.sql
pub async fn fetch_filtered_workflow_list(
    pool: &PgPool,
    statuses: &[&str],
) -> Result<Vec<WorkflowRow>> {
    let query = include_str!("../../sql/workflows/filtered-workflow-list.sql");
    let rows = sqlx::query_as::<_, WorkflowRow>(query)
        .bind(statuses)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

/// Fetch a single workflow by ID
pub async fn fetch_workflow_by_id(pool: &PgPool, workflow_id: &str) -> Result<Option<WorkflowRow>> {
    let row = sqlx::query_as::<_, WorkflowRow>(
        r#"
        SELECT
            w.id,
            w.name,
            w.status,
            w.on_error,
            w.output_task_index,
            w.success_policy,
            w.result,
            w.error,
            w.sent_at,
            w.created_at,
            w.started_at,
            w.completed_at,
            w.updated_at,
            COUNT(wt.id) as total_tasks,
            COUNT(*) FILTER (WHERE wt.status = 'COMPLETED') as completed_tasks,
            COUNT(*) FILTER (WHERE wt.status = 'FAILED') as failed_tasks,
            COUNT(*) FILTER (WHERE wt.status = 'RUNNING') as running_tasks,
            COUNT(*) FILTER (WHERE wt.status = 'PENDING' OR wt.status = 'READY') as pending_tasks,
            COUNT(*) FILTER (WHERE wt.status = 'ENQUEUED') as enqueued_tasks,
            COUNT(*) FILTER (WHERE wt.status = 'SKIPPED') as skipped_tasks
        FROM horsies_workflows w
        LEFT JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id
        WHERE w.id = $1
        GROUP BY w.id
        "#,
    )
    .bind(workflow_id)
    .fetch_optional(pool)
    .await?;

    Ok(row)
}

/// Fetch workflow tasks for detail view
/// Source: ../../../sql/workflows/workflow-detail.sql
pub async fn fetch_workflow_tasks(pool: &PgPool, workflow_id: &str) -> Result<Vec<WorkflowTaskRow>> {
    let query = include_str!("../../sql/workflows/workflow-detail.sql");
    let rows = sqlx::query_as::<_, WorkflowTaskRow>(query)
        .bind(workflow_id)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}
