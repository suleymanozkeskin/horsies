use std::sync::Arc;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use sysinfo::{Pid, System};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::core::config::app::AppConfig;

use crate::worker::config::WorkerConfig;

const INSERT_WORKER_STATE_SQL: &str = "\
INSERT INTO horsies_worker_states (
    worker_id, snapshot_at, hostname, pid, processes, max_claim_batch, max_claim_per_worker,
    cluster_wide_cap, queues, queue_priorities, queue_max_concurrency,
    recovery_config, tasks_running, tasks_claimed, memory_usage_mb,
    memory_percent, cpu_percent, worker_started_at
) VALUES (
    $1, NOW(), $2, $3, $4, $5, $6, $7, $8::text[], $9::jsonb, $10::jsonb,
    $11::jsonb, $12, $13, $14, $15, $16, $17
)";

/// Resource metrics collected from sysinfo.
struct ResourceMetrics {
    memory_usage_mb: Option<f64>,
    memory_percent: Option<f64>,
    cpu_percent: Option<f64>,
}

/// Collect resource metrics (memory and CPU) for the current process.
///
/// Uses the `sysinfo` crate to mirror Python's psutil-based metrics:
/// - `memory_usage_mb`: resident set size in megabytes
/// - `memory_percent`: RSS as a percentage of total system memory
/// - `cpu_percent`: CPU usage percentage
fn collect_resource_metrics(sys: &mut System, pid: Pid) -> ResourceMetrics {
    sys.refresh_processes_specifics(
        sysinfo::ProcessesToUpdate::Some(&[pid]),
        true,
        sysinfo::ProcessRefreshKind::new().with_cpu().with_memory(),
    );

    if let Some(process) = sys.process(pid) {
        let rss_bytes = process.memory();
        let memory_mb = rss_bytes as f64 / 1024.0 / 1024.0;

        let total_memory = sys.total_memory();
        let memory_percent = if total_memory > 0 {
            (rss_bytes as f64 / total_memory as f64) * 100.0
        } else {
            0.0
        };

        let cpu_percent = process.cpu_usage() as f64;

        ResourceMetrics {
            memory_usage_mb: Some((memory_mb * 100.0).round() / 100.0),
            memory_percent: Some((memory_percent * 100.0).round() / 100.0),
            cpu_percent: Some((cpu_percent * 100.0).round() / 100.0),
        }
    } else {
        ResourceMetrics {
            memory_usage_mb: None,
            memory_percent: None,
            cpu_percent: None,
        }
    }
}

/// Query the count of tasks currently claimed by this worker.
async fn query_tasks_claimed(pool: &PgPool, worker_id: &str) -> Result<i32, sqlx::Error> {
    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM horsies_tasks WHERE claimed_by_worker_id = $1 AND status = 'CLAIMED'",
    )
    .bind(worker_id)
    .fetch_one(pool)
    .await?;
    Ok(count.0 as i32)
}

/// Spawn a background loop that inserts worker state snapshots on
/// `recovery.worker_state_snapshot_interval_ms` (default 30s).
///
/// Tracks the worker's liveness, task counts, and resource metrics
/// (memory/CPU via sysinfo) in `horsies_worker_states` as a timeseries.
/// Each snapshot is a new row (INSERT), enabling historical analysis.
pub fn spawn_worker_state_loop(
    pool: PgPool,
    worker_id: String,
    hostname: String,
    pid: i32,
    worker_config: WorkerConfig,
    app_config: AppConfig,
    semaphore: Arc<Semaphore>,
    worker_started_at: DateTime<Utc>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let total_permits = worker_config.concurrency as usize;
        let snapshot_interval =
            std::time::Duration::from_millis(app_config.recovery.worker_state_snapshot_interval_ms);

        // Serialize configuration to JSONB.
        let queue_priorities_json = if worker_config.queue_priorities.is_empty() {
            None
        } else {
            serde_json::to_string(&worker_config.queue_priorities).ok()
        };

        let queue_max_concurrency_json = if worker_config.queue_max_concurrency.is_empty() {
            None
        } else {
            serde_json::to_string(&worker_config.queue_max_concurrency).ok()
        };

        let recovery_config_json = serde_json::to_string(&app_config.recovery).ok();

        // Initialize sysinfo System for resource metrics.
        let sys_pid = Pid::from(pid as usize);
        let mut sys = System::new();
        sys.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::Some(&[sys_pid]),
            true,
            sysinfo::ProcessRefreshKind::new().with_cpu().with_memory(),
        );
        sys.refresh_memory();

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(snapshot_interval) => {}
            }

            if cancel.is_cancelled() {
                break;
            }

            let tasks_running = (total_permits - semaphore.available_permits()) as i32;

            let tasks_claimed = match query_tasks_claimed(&pool, &worker_id).await {
                Ok(count) => count,
                Err(e) => {
                    tracing::warn!(error = %e, "failed to query tasks_claimed");
                    0
                }
            };

            let metrics = collect_resource_metrics(&mut sys, sys_pid);

            if let Err(e) = sqlx::query(INSERT_WORKER_STATE_SQL)
                .bind(&worker_id)
                .bind(&hostname)
                .bind(pid)
                .bind(worker_config.concurrency as i32)
                .bind(worker_config.max_claim_batch as i32)
                .bind(worker_config.max_claim_per_worker as i32)
                .bind(app_config.cluster_wide_cap.map(|c| c as i32))
                .bind(&worker_config.queues)
                .bind(&queue_priorities_json)
                .bind(&queue_max_concurrency_json)
                .bind(&recovery_config_json)
                .bind(tasks_running)
                .bind(tasks_claimed)
                .bind(metrics.memory_usage_mb)
                .bind(metrics.memory_percent)
                .bind(metrics.cpu_percent)
                .bind(worker_started_at)
                .execute(&pool)
                .await
            {
                tracing::warn!(error = %e, "failed to insert worker state");
            }
        }

        tracing::debug!(worker_id = %worker_id, "worker state loop stopped");
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_sql_has_expected_placeholders() {
        assert!(INSERT_WORKER_STATE_SQL.contains("$1"));
        assert!(INSERT_WORKER_STATE_SQL.contains("$17"));
        assert!(INSERT_WORKER_STATE_SQL.contains("INSERT INTO"));
        assert!(!INSERT_WORKER_STATE_SQL.contains("ON CONFLICT"));
    }

    #[test]
    fn collect_resource_metrics_returns_values() {
        let pid = Pid::from(std::process::id() as usize);
        let mut sys = System::new();
        sys.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::Some(&[pid]),
            true,
            sysinfo::ProcessRefreshKind::new().with_cpu().with_memory(),
        );
        sys.refresh_memory();
        let metrics = collect_resource_metrics(&mut sys, pid);
        // On a real system, we should get memory metrics.
        assert!(metrics.memory_usage_mb.is_some());
        assert!(metrics.memory_percent.is_some());
        assert!(metrics.cpu_percent.is_some());
    }
}
