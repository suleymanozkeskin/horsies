use crate::errors::Result;
use crate::models::{
    ActiveWorkerRow, AggregatedBreakdownRow, ClusterCapacitySummary, ClusterUtilizationPoint,
    DeadWorkerRow, OverloadedWorkerAlert, SnapshotAgeBucket, StaleClaimsAlert, TaskDetail,
    TaskStatusRow, TaskSummary, WorkerLoadPoint, WorkerQueuesRow, WorkerState, WorkerUptimeRow,
    WorkflowRow, WorkflowSummary, WorkflowTaskRow,
};
use sqlx::PgPool;

/// Get current state of all active workers (latest snapshot per worker)
/// Uses: ../../sql/worker/active-workers.sql (Active workers query)
pub async fn get_active_workers(pool: &PgPool) -> Result<Vec<WorkerState>> {
    let _query = include_str!("../../sql/worker/active-workers.sql");

    // Extract the "Active workers only" query from the file
    // For now, inline it until we split the .sql file into separate query files
    let workers = sqlx::query_as::<_, WorkerState>(
        r#"
        SELECT DISTINCT ON (worker_id)
            worker_id,
            snapshot_at,
            hostname,
            pid,
            processes,
            max_claim_batch,
            max_claim_per_worker,
            cluster_wide_cap,
            queues,
            queue_priorities,
            queue_max_concurrency,
            recovery_config,
            tasks_running,
            tasks_claimed,
            memory_usage_mb,
            memory_percent,
            cpu_percent,
            worker_started_at
        FROM horsies_worker_states
        WHERE snapshot_at > NOW() - INTERVAL '2 minutes'
        ORDER BY worker_id, snapshot_at DESC
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(workers)
}

/// Get all workers (including stale ones)
pub async fn get_all_workers(pool: &PgPool) -> Result<Vec<WorkerState>> {
    let workers = sqlx::query_as::<_, WorkerState>(
        r#"
        SELECT DISTINCT ON (worker_id)
            worker_id,
            snapshot_at,
            hostname,
            pid,
            processes,
            max_claim_batch,
            max_claim_per_worker,
            cluster_wide_cap,
            queues,
            queue_priorities,
            queue_max_concurrency,
            recovery_config,
            tasks_running,
            tasks_claimed,
            memory_usage_mb,
            memory_percent,
            cpu_percent,
            worker_started_at
        FROM horsies_worker_states
        ORDER BY worker_id, snapshot_at DESC
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(workers)
}

/// Get cluster-wide task summary
/// Uses: ../../sql/tasks/aggregated-breakdown.sql
pub async fn get_task_summary(pool: &PgPool) -> Result<TaskSummary> {
    // This query is similar to aggregated-breakdown.sql but simplified for summary
    let summary = sqlx::query_as::<_, TaskSummary>(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE status = 'PENDING') as pending,
            COUNT(*) FILTER (WHERE status = 'CLAIMED') as claimed,
            COUNT(*) FILTER (WHERE status = 'RUNNING') as running,
            COUNT(*) FILTER (WHERE status = 'COMPLETED') as completed,
            COUNT(*) FILTER (WHERE status = 'FAILED') as failed
        FROM horsies_tasks
        "#,
    )
    .fetch_one(pool)
    .await?;

    Ok(summary)
}

/// Get worker state history for a specific worker (for charts)
pub async fn get_worker_history(
    pool: &PgPool,
    worker_id: &str,
    minutes: i32,
) -> Result<Vec<WorkerState>> {
    let history = sqlx::query_as::<_, WorkerState>(
        r#"
        SELECT
            worker_id,
            snapshot_at,
            hostname,
            pid,
            processes,
            max_claim_batch,
            max_claim_per_worker,
            cluster_wide_cap,
            queues,
            queue_priorities,
            queue_max_concurrency,
            recovery_config,
            tasks_running,
            tasks_claimed,
            memory_usage_mb,
            memory_percent,
            cpu_percent,
            worker_started_at
        FROM horsies_worker_states
        WHERE worker_id = $1
          AND snapshot_at > NOW() - ($2 || ' minutes')::INTERVAL
        ORDER BY snapshot_at ASC
        "#,
    )
    .bind(worker_id)
    .bind(minutes)
    .fetch_all(pool)
    .await?;

    Ok(history)
}

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

/// Worker load for last 5 minutes
/// Source: ../../sql/worker/worker-load-5min.sql
pub async fn fetch_worker_load_5m(pool: &PgPool) -> Result<Vec<WorkerLoadPoint>> {
    let _query = include_str!("../../sql/worker/worker-load-5min.sql");
    let rows = sqlx::query_as::<_, WorkerLoadPoint>(_query)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

/// Worker load for a custom time window
/// Dynamically fetches worker load data for the specified time interval
pub async fn fetch_worker_load(pool: &PgPool, interval: &str) -> Result<Vec<WorkerLoadPoint>> {
    let query = format!(
        r#"
        SELECT
            worker_id,
            snapshot_at,
            tasks_running,
            tasks_claimed,
            memory_percent,
            cpu_percent
        FROM horsies_worker_states
        WHERE snapshot_at > NOW() - INTERVAL '{}'
        ORDER BY worker_id, snapshot_at
        "#,
        interval
    );

    let rows = sqlx::query_as::<_, WorkerLoadPoint>(&query)
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
pub async fn fetch_task_aggregation(pool: &PgPool) -> Result<Vec<AggregatedBreakdownRow>> {
    let _query = include_str!("../../sql/tasks/aggregated-breakdown.sql");
    let rows = sqlx::query_as::<_, AggregatedBreakdownRow>(_query)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

/// Worker task aggregation filtered by status
/// Source: ../../../sql/tasks/filtered-aggregation.sql
pub async fn fetch_filtered_task_aggregation(
    pool: &PgPool,
    statuses: &[&str],
) -> Result<Vec<AggregatedBreakdownRow>> {
    let query = include_str!("../../sql/tasks/filtered-aggregation.sql");
    let rows = sqlx::query_as::<_, AggregatedBreakdownRow>(query)
        .bind(statuses)
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

/// Fetch a single task by ID for the detail view
/// Source: ../../../sql/tasks/task-detail-by-id.sql
pub async fn fetch_task_by_id(pool: &PgPool, task_id: &str) -> Result<Option<TaskDetail>> {
    let query = include_str!("../../sql/tasks/task-detail-by-id.sql");
    let task = sqlx::query_as::<_, TaskDetail>(query)
        .bind(task_id)
        .fetch_optional(pool)
        .await?;

    Ok(task)
}

/// Refresh cadence hints (seconds)
pub const REFRESH_FAST_SECS: u64 = 2;
pub const REFRESH_NORMAL_SECS: u64 = 5;
pub const REFRESH_SLOW_SECS: u64 = 15;

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
            w.created_at,
            w.started_at,
            w.completed_at,
            w.updated_at,
            COUNT(wt.id) as total_tasks,
            COUNT(*) FILTER (WHERE wt.status = 'COMPLETED') as completed_tasks,
            COUNT(*) FILTER (WHERE wt.status = 'FAILED') as failed_tasks,
            COUNT(*) FILTER (WHERE wt.status = 'RUNNING') as running_tasks,
            COUNT(*) FILTER (WHERE wt.status = 'PENDING' OR wt.status = 'READY') as pending_tasks
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

// TODO: Create individual .sql files for each query in horsies/sql/observartion/queries/
// Then use sqlx::query_file!() like:
// sqlx::query_file_as!(WorkerState, "horsies/sql/observartion/queries/active_workers.sql")
