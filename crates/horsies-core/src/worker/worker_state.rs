use std::sync::Arc;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use sysinfo::{Pid, System};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::core::config::app::AppConfig;
use crate::core::task::execution::Execution;

use crate::worker::config::WorkerConfig;
use crate::worker::recovery::ReaperHealth;

/// One worker snapshot. Every NOT NULL column of the baseline table is named
/// (the baseline has no column defaults); `children_memory_mb` is left NULL:
/// this worker runs no children.
const INSERT_WORKER_STATE_SQL: &str = "\
INSERT INTO horsies_worker_states (
    worker_id, snapshot_at, hostname, pid, processes, max_claim_batch, max_claim_per_worker,
    cluster_wide_cap, queues, queue_priorities, queue_max_concurrency,
    recovery_config, tasks_running, tasks_claimed, memory_usage_mb,
    memory_percent, cpu_percent, worker_started_at, slots_down, class_capacity
) VALUES (
    $1, NOW(), $2, $3, $4, $5, $6, $7, $8::text[], $9::jsonb, $10::jsonb,
    $11::jsonb, $12, $13, $14, $15, $16, $17, $18::jsonb, $19::jsonb
)";

/// The slot health of a worker that runs task bodies in its own process. It
/// has no child slots, so no slot is down: Python stores `[]` for a worker
/// with no slot down (`core/models/health.py`, `slots_down_json`). Pure.
fn in_process_slots_down() -> serde_json::Value {
    serde_json::Value::Array(Vec::new())
}

/// The class capacity of a worker that runs task bodies in its own process.
///
/// Its claim has one budget of `concurrency` and takes rows of every
/// execution class. The stored form of that plan is one `ISOLATED` budget
/// that serves every class, as Python stores it for a worker that runs only
/// isolated children (`core/worker/claim_plan.py`, `isolated_children_plan`
/// and `class_capacity_json`): `{"ISOLATED": {"capacity": concurrency,
/// "serves": ["ISOLATED", "SHARED_ASYNC", "SHARED_THREAD"]}}`. Pure.
fn in_process_class_capacity(concurrency: u32) -> serde_json::Value {
    let serves: Vec<&str> = [
        Execution::Isolated,
        Execution::SharedAsync,
        Execution::SharedThread,
    ]
    .into_iter()
    .map(Execution::as_str)
    .collect();
    let mut capacity = serde_json::Map::new();
    capacity.insert(
        Execution::Isolated.as_str().to_owned(),
        serde_json::json!({ "capacity": concurrency, "serves": serves }),
    );
    serde_json::Value::Object(capacity)
}

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
    reaper_health: ReaperHealth,
    semaphore: Arc<Semaphore>,
    worker_started_at: DateTime<Utc>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let total_permits = worker_config.concurrency as usize;
        let snapshot_interval =
            std::time::Duration::from_millis(app_config.recovery.worker_state_snapshot_interval_ms);

        // The queue maps are stored as they are: an empty map is `{}`, never
        // NULL (both columns are NOT NULL on the baseline).
        let queue_priorities = sqlx::types::Json(&worker_config.queue_priorities);
        let queue_max_concurrency = sqlx::types::Json(&worker_config.queue_max_concurrency);
        let slots_down = in_process_slots_down();
        let class_capacity = in_process_class_capacity(worker_config.concurrency);

        // Initialize sysinfo System for resource metrics.
        let sys_pid = Pid::from(pid as usize);
        let mut sys = System::new();
        sys.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::Some(&[sys_pid]),
            true,
            sysinfo::ProcessRefreshKind::new().with_cpu().with_memory(),
        );
        sys.refresh_memory();

        // The first snapshot is written at start, then one per interval
        // (Python `_worker_state_heartbeat_loop`: write, then wait), so the
        // fleet check sees a starting worker at once.
        while !cancel.is_cancelled() {
            let tasks_running = (total_permits - semaphore.available_permits()) as i32;

            let tasks_claimed = match query_tasks_claimed(&pool, &worker_id).await {
                Ok(count) => count,
                Err(e) => {
                    tracing::warn!(error = %e, "failed to query tasks_claimed");
                    0
                }
            };

            let metrics = collect_resource_metrics(&mut sys, sys_pid);
            let health = reaper_health.read().await.clone();
            let mut recovery_value = serde_json::to_value(&app_config.recovery)
                .unwrap_or_else(|_| serde_json::json!({}));
            if let Some(object) = recovery_value.as_object_mut() {
                object.insert(
                    "worker_state_retention_hours".to_owned(),
                    serde_json::to_value(app_config.retention.worker_state_retention_hours)
                        .unwrap_or(serde_json::Value::Null),
                );
                object.insert(
                    "terminal_record_retention_hours".to_owned(),
                    serde_json::to_value(app_config.retention.terminal_record_retention_hours)
                        .unwrap_or(serde_json::Value::Null),
                );
                object.insert(
                    "partition_coverage".to_owned(),
                    health.partition_coverage.unwrap_or(serde_json::Value::Null),
                );
                object.insert(
                    "partition_pruning".to_owned(),
                    health.partition_pruning.unwrap_or(serde_json::Value::Null),
                );
                object.insert(
                    "workflow_recovery".to_owned(),
                    health.workflow_recovery.unwrap_or(serde_json::Value::Null),
                );
                object.insert(
                    "phase2_recovery".to_owned(),
                    health.phase2_recovery.unwrap_or(serde_json::Value::Null),
                );
            }

            if let Err(e) = sqlx::query(INSERT_WORKER_STATE_SQL)
                .bind(&worker_id)
                .bind(&hostname)
                .bind(pid)
                .bind(worker_config.concurrency as i32)
                .bind(worker_config.max_claim_batch as i32)
                .bind(worker_config.max_claim_per_worker as i32)
                .bind(app_config.cluster_wide_cap.map(|c| c as i32))
                .bind(&worker_config.queues)
                .bind(queue_priorities)
                .bind(queue_max_concurrency)
                .bind(&recovery_value)
                .bind(tasks_running)
                .bind(tasks_claimed)
                .bind(metrics.memory_usage_mb)
                .bind(metrics.memory_percent)
                .bind(metrics.cpu_percent)
                .bind(worker_started_at)
                .bind(&slots_down)
                .bind(&class_capacity)
                .execute(&pool)
                .await
            {
                tracing::warn!(error = %e, "failed to insert worker state");
            }

            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(snapshot_interval) => {}
            }
        }

        tracing::debug!(worker_id = %worker_id, "worker state loop stopped");
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[serial_test::serial]
    async fn worker_snapshots_persist_workflow_recovery_health() {
        use crate::worker::recovery::new_reaper_health;
        use serde_json::json;
        use std::time::Duration;

        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let worker_id = uuid::Uuid::new_v4().to_string();
        let health = new_reaper_health();
        let phase2 = json!({"applied": 2, "failed": 0});
        health.write().await.phase2_recovery = Some(phase2.clone());
        let mut app_config = AppConfig::for_database_url("postgresql://unused/test");
        app_config.recovery.worker_state_snapshot_interval_ms = 10;
        let worker_config = WorkerConfig {
            concurrency: 1,
            ..Default::default()
        };
        let cancel = CancellationToken::new();
        let task = spawn_worker_state_loop(
            pool.clone(),
            worker_id.clone(),
            "snapshot-test".to_owned(),
            std::process::id() as i32,
            worker_config,
            app_config,
            health.clone(),
            Arc::new(Semaphore::new(1)),
            Utc::now(),
            cancel.clone(),
        );
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            for workflow in [
                serde_json::Value::Null,
                json!({"metrics": {"case2_3": {"rows_selected": 200,
                    "candidates_returned": 5, "duration_ms": 12}},
                    "completion_candidate_yield": 0.025}),
                json!({"state": "error", "error": "audit unavailable", "errors": 1,
                    "completion_candidate_yield": null}),
            ] {
                health.write().await.workflow_recovery = match &workflow {
                    serde_json::Value::Null => None,
                    value => Some(value.clone()),
                };
                loop {
                    let snapshot: Option<serde_json::Value> = sqlx::query_scalar(
                        "SELECT recovery_config FROM horsies_worker_states
                         WHERE worker_id = $1 ORDER BY snapshot_at DESC LIMIT 1",
                    )
                    .bind(&worker_id)
                    .fetch_optional(&pool)
                    .await
                    .unwrap();
                    if let Some(value) = snapshot {
                        if value.get("workflow_recovery") == Some(&workflow) {
                            assert_eq!(value["phase2_recovery"], phase2);
                            assert!(value.get("partition_coverage").is_some());
                            assert!(value.get("partition_pruning").is_some());
                            break;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        })
        .await;
        cancel.cancel();
        task.await.unwrap();
        sqlx::query("DELETE FROM horsies_worker_states WHERE worker_id = $1")
            .bind(&worker_id)
            .execute(&pool)
            .await
            .unwrap();
        result.expect("worker snapshots must retain each recovery health state");
    }

    #[test]
    fn insert_sql_has_expected_placeholders() {
        assert!(INSERT_WORKER_STATE_SQL.contains("$1"));
        assert!(INSERT_WORKER_STATE_SQL.contains("$19"));
        assert!(INSERT_WORKER_STATE_SQL.contains("INSERT INTO"));
        assert!(!INSERT_WORKER_STATE_SQL.contains("ON CONFLICT"));
    }

    #[test]
    fn an_in_process_worker_has_no_slot_down() {
        assert_eq!(in_process_slots_down(), serde_json::json!([]));
    }

    #[test]
    fn an_in_process_worker_has_one_budget_that_serves_every_class() {
        assert_eq!(
            in_process_class_capacity(3),
            serde_json::json!({
                "ISOLATED": {
                    "capacity": 3,
                    "serves": ["ISOLATED", "SHARED_ASYNC", "SHARED_THREAD"]
                }
            })
        );
    }

    /// A worker with no queue priorities and no queue caps writes its
    /// snapshot on the baseline: the empty maps are `{}`, and the slot
    /// health and class capacity columns hold the worker's values (Python
    /// `core/worker/health.py`, `_update_worker_state`). The old statement
    /// named neither column and bound NULL for the empty maps, so every
    /// insert failed and no row landed.
    #[tokio::test]
    #[serial_test::serial]
    async fn a_snapshot_with_empty_queue_maps_lands_with_every_baseline_column() {
        use crate::worker::recovery::new_reaper_health;
        use std::time::Duration;

        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let worker_id = uuid::Uuid::new_v4().to_string();
        let mut app_config = AppConfig::for_database_url("postgresql://unused/test");
        app_config.recovery.worker_state_snapshot_interval_ms = 60_000;
        let worker_config = WorkerConfig {
            concurrency: 2,
            ..Default::default()
        };
        assert!(worker_config.queue_priorities.is_empty());
        assert!(worker_config.queue_max_concurrency.is_empty());
        let cancel = CancellationToken::new();
        let task = spawn_worker_state_loop(
            pool.clone(),
            worker_id.clone(),
            "baseline-columns-test".to_owned(),
            std::process::id() as i32,
            worker_config,
            app_config,
            new_reaper_health(),
            Arc::new(Semaphore::new(2)),
            Utc::now(),
            cancel.clone(),
        );
        type Columns = (
            serde_json::Value,
            serde_json::Value,
            serde_json::Value,
            serde_json::Value,
        );
        let written = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let row: Option<Columns> = sqlx::query_as(
                    "SELECT queue_priorities, queue_max_concurrency, slots_down, class_capacity
                     FROM horsies_worker_states WHERE worker_id = $1",
                )
                .bind(&worker_id)
                .fetch_optional(&pool)
                .await
                .expect("read the snapshot");
                if let Some(columns) = row {
                    break columns;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        cancel.cancel();
        task.await.expect("the snapshot loop joins");
        sqlx::query("DELETE FROM horsies_worker_states WHERE worker_id = $1")
            .bind(&worker_id)
            .execute(&pool)
            .await
            .expect("delete the snapshots");

        let (queue_priorities, queue_max_concurrency, slots_down, class_capacity) =
            written.expect("the first snapshot must land on the baseline schema");
        assert_eq!(queue_priorities, serde_json::json!({}));
        assert_eq!(queue_max_concurrency, serde_json::json!({}));
        assert_eq!(slots_down, serde_json::json!([]));
        assert_eq!(class_capacity, in_process_class_capacity(2));
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

    /// The first snapshot is written at start, not after one interval
    /// (Python `_worker_state_heartbeat_loop`), so the fleet check sees a
    /// starting worker at once.
    #[tokio::test]
    #[serial_test::serial]
    async fn first_snapshot_is_written_at_start() {
        use crate::worker::recovery::new_reaper_health;
        use std::time::Duration;

        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let worker_id = uuid::Uuid::new_v4().to_string();
        let mut app_config = AppConfig::for_database_url("postgresql://unused/test");
        app_config.recovery.worker_state_snapshot_interval_ms = 60_000;
        let worker_config = WorkerConfig {
            concurrency: 1,
            ..Default::default()
        };
        let cancel = CancellationToken::new();
        let task = spawn_worker_state_loop(
            pool.clone(),
            worker_id.clone(),
            "first-snapshot-test".to_owned(),
            std::process::id() as i32,
            worker_config,
            app_config,
            new_reaper_health(),
            Arc::new(Semaphore::new(1)),
            Utc::now(),
            cancel.clone(),
        );
        let written = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let count: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM horsies_worker_states WHERE worker_id = $1",
                )
                .bind(&worker_id)
                .fetch_one(&pool)
                .await
                .unwrap();
                if count > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        cancel.cancel();
        task.await.unwrap();
        sqlx::query("DELETE FROM horsies_worker_states WHERE worker_id = $1")
            .bind(&worker_id)
            .execute(&pool)
            .await
            .unwrap();
        written.expect("the first snapshot must land well before one 60 s interval");
    }
}
