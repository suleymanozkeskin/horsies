use chrono::{DateTime, SecondsFormat, Utc};
use sqlx::PgPool;
use sysinfo::{Pid, System};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::core::config::app::AppConfig;
use crate::core::config::worker_profile::{TaskLimitSetting, WorkerProfile};
use crate::core::task::execution::Execution;

use crate::worker::children::ChildrenView;
use crate::worker::config::WorkerConfig;
use crate::worker::executor::slots::{DownState, PoolService};
use crate::worker::executor::supervisor::BootFailureCause;
use crate::worker::recovery::ReaperHealth;

/// One worker snapshot. Every NOT NULL column of the baseline table is named
/// (the baseline has no column defaults); `children_memory_mb` is left NULL:
/// the core does not read the RSS of the children yet.
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

/// The JSON of the cause of a down slot (Python `boot_cause_json`). A slot
/// goes down only for a cause that can clear, so any other cause is a
/// defect of the board.
fn down_cause_json(slot: usize, cause: &BootFailureCause) -> serde_json::Value {
    match cause {
        BootFailureCause::ResourceShortage(errno) => {
            serde_json::json!({ "kind": "resource_shortage", "errno": errno.as_str() })
        }
        BootFailureCause::ThreadStartRefused => {
            serde_json::json!({ "kind": "thread_start_refused" })
        }
        BootFailureCause::AppLoadFailed
        | BootFailureCause::ChildPoolSetupFailed
        | BootFailureCause::TaskLoopCreateFailed
        | BootFailureCause::SpawnFailed
        | BootFailureCause::LostBeforeReady { .. }
        | BootFailureCause::NotReadyInTime
        | BootFailureCause::ProtocolMismatch(_) => {
            panic!("slot {slot} is down for a cause that repeats: {cause}")
        }
    }
}

/// The `slots_down` column: each slot that is down, in slot order (Python
/// `core/models/health.py`, `slots_down_json`). An empty list means that no
/// slot was down. Pure.
pub fn slots_down_json(down: &[(usize, DownState)]) -> serde_json::Value {
    serde_json::Value::Array(
        down.iter()
            .map(|(slot, state)| {
                serde_json::json!({
                    "slot": slot,
                    "cause": down_cause_json(*slot, &state.cause),
                    "detail": state.detail,
                    "down_since": state.down_since.to_rfc3339_opts(SecondsFormat::Micros, false),
                    "failed_boots": state.failed_boots.get(),
                })
            })
            .collect(),
    )
}

/// One budget of a worker's claim plan, as the snapshot stores it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetColumn {
    /// The class of the budget.
    pub budget: Execution,
    /// The children (`ISOLATED`) or seats (a shared class) of the budget that
    /// are in service.
    pub capacity: u32,
    /// The classes whose rows the budget claims, in `Execution` order.
    pub serves: Vec<Execution>,
}

/// The `class_capacity` column: each budget class maps to its capacity in
/// service and the classes whose rows it claims (Python
/// `core/worker/claim_plan.py`, `class_capacity_json`). A class that no
/// `serves` list names is not served by the worker. Pure.
pub fn class_capacity_json(budgets: &[BudgetColumn]) -> serde_json::Value {
    let mut capacity = serde_json::Map::new();
    for column in budgets {
        let serves: Vec<&str> = column
            .serves
            .iter()
            .copied()
            .map(Execution::as_str)
            .collect();
        capacity.insert(
            column.budget.as_str().to_owned(),
            serde_json::json!({ "capacity": column.capacity, "serves": serves }),
        );
    }
    serde_json::Value::Object(capacity)
}

/// The `class_capacity` column of the worker's slots now: each budget of
/// the plan with its capacity in service and the classes it claims (Python
/// `class_capacity_json(self.cfg.claim_plan(), self._claim_capacity())`).
/// Pure.
pub fn class_capacity_of(service: &PoolService) -> serde_json::Value {
    let plan = service.plan();
    let columns: Vec<BudgetColumn> = service
        .capacities()
        .into_iter()
        .map(|capacity| BudgetColumn {
            budget: capacity.budget,
            capacity: capacity.in_service,
            serves: plan.served_by(capacity.budget),
        })
        .collect();
    class_capacity_json(&columns)
}

/// The columns of a worker snapshot that the children and the claim limits
/// of the worker decide (Python `core/worker/health.py:233-239`, from the
/// `WorkerConfig` that `execution_settings(profile)` builds).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimColumns {
    /// `processes`: the children of the worker.
    pub processes: u32,
    /// `max_claim_batch`: the most rows that one pass claims from one queue;
    /// 0 is no limit.
    pub max_claim_batch: u32,
    /// `max_claim_per_worker`: the most CLAIMED rows of the worker, resolved.
    /// With no set limit it is the seats of the worker, never 0.
    pub max_claim_per_worker: u32,
}

impl ClaimColumns {
    /// The columns of a worker that runs `profile`: its child count, its per
    /// queue limit or 0, and its claimed limit or else its seat count
    /// (Python: `cfg.max_claim_per_worker if > 0 else cfg.seat_count()`).
    /// Pure.
    pub fn of_profile(profile: &WorkerProfile) -> Self {
        Self {
            processes: profile.child_count(),
            max_claim_batch: match profile.claim.per_queue_per_pass {
                TaskLimitSetting::TaskLimit { tasks } => tasks.get(),
                TaskLimitSetting::NoTaskLimit {} => 0,
            },
            max_claim_per_worker: match profile.claim.claimed_per_worker {
                TaskLimitSetting::TaskLimit { tasks } => tasks.get(),
                TaskLimitSetting::NoTaskLimit {} => profile.seat_count(),
            },
        }
    }
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

/// The RUNNING and the CLAIMED rows that this worker holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HeldRows {
    running: i32,
    claimed: i32,
}

/// Count the RUNNING and the CLAIMED rows of this worker in one statement
/// (Python `_count_only_running_for_worker` and `_count_claimed_for_worker`,
/// two statements on one session).
async fn query_held_rows(pool: &PgPool, worker_id: &str) -> Result<HeldRows, sqlx::Error> {
    let (running, claimed): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*) FILTER (WHERE status = 'RUNNING'),
                COUNT(*) FILTER (WHERE status = 'CLAIMED')
         FROM horsies_tasks
         WHERE claimed_by_worker_id = $1 AND status IN ('RUNNING', 'CLAIMED')",
    )
    .bind(worker_id)
    .fetch_one(pool)
    .await?;
    Ok(HeldRows {
        running: i32::try_from(running).unwrap_or(i32::MAX),
        claimed: i32::try_from(claimed).unwrap_or(i32::MAX),
    })
}

/// Spawn a background loop that inserts worker state snapshots on
/// `recovery.worker_state_snapshot_interval_ms` (default 30s).
///
/// Tracks the worker's liveness, task counts, and resource metrics
/// (memory/CPU via sysinfo) in `horsies_worker_states` as a timeseries.
/// Each snapshot is a new row (INSERT), enabling historical analysis.
///
/// `children` gives the slots of the worker at each snapshot: the down
/// slots (`slots_down`) and the capacity in service of each budget
/// (`class_capacity`). `claim_columns` are the profile's
/// ([`ClaimColumns::of_profile`]).
#[allow(clippy::too_many_arguments)]
pub fn spawn_worker_state_loop(
    pool: PgPool,
    worker_id: String,
    hostname: String,
    pid: i32,
    worker_config: WorkerConfig,
    app_config: AppConfig,
    reaper_health: ReaperHealth,
    children: ChildrenView,
    claim_columns: ClaimColumns,
    worker_started_at: DateTime<Utc>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let snapshot_interval =
            std::time::Duration::from_millis(app_config.recovery.worker_state_snapshot_interval_ms);

        // The queue maps are stored as they are: an empty map is `{}`, never
        // NULL (both columns are NOT NULL on the baseline).
        let queue_priorities = sqlx::types::Json(&worker_config.queue_priorities);
        let queue_max_concurrency = sqlx::types::Json(&worker_config.queue_max_concurrency);

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
            // Python counts and inserts under one `try`, so a failed count
            // writes no snapshot (`core/worker/health.py`,
            // `_update_worker_state`). Storing 0 would record "no claimed
            // row" for a count that was not taken.
            let held = match query_held_rows(&pool, &worker_id).await {
                Ok(held) => held,
                Err(error) => {
                    tracing::error!(
                        worker_id = %worker_id,
                        %error,
                        "failed to count claimed tasks; no worker snapshot this interval",
                    );
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = tokio::time::sleep(snapshot_interval) => {}
                    }
                    continue;
                }
            };

            let slots_down = slots_down_json(&children.down_slots());
            let class_capacity = class_capacity_of(&children.service());
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
                .bind(claim_columns.processes as i32)
                .bind(claim_columns.max_claim_batch as i32)
                .bind(claim_columns.max_claim_per_worker as i32)
                .bind(app_config.cluster_wide_cap.map(|c| c as i32))
                .bind(&worker_config.queues)
                .bind(queue_priorities)
                .bind(queue_max_concurrency)
                .bind(&recovery_value)
                .bind(held.running)
                .bind(held.claimed)
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
    use std::sync::Arc;

    use super::*;
    use crate::worker::children::SeatsPerChild;
    use crate::worker::executor::slots::{ChildLayout, PoolBoard, SlotService};

    /// The view of a worker of `isolated` isolated children, every slot
    /// booting.
    fn isolated_children(isolated: usize) -> ChildrenView {
        ChildrenView::new(
            Arc::new(PoolBoard::new(ChildLayout {
                isolated,
                shared_async: 0,
                shared_thread: 0,
            })),
            SeatsPerChild {
                shared_async: 0,
                shared_thread: 0,
            },
        )
    }

    /// The claim columns of a worker of `isolated` isolated children with no
    /// claim limit.
    fn isolated_claim_columns(isolated: u32) -> ClaimColumns {
        ClaimColumns {
            processes: isolated,
            max_claim_batch: 0,
            max_claim_per_worker: isolated,
        }
    }

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
        let worker_config = WorkerConfig::default();
        let cancel = CancellationToken::new();
        let task = spawn_worker_state_loop(
            pool.clone(),
            worker_id.clone(),
            "snapshot-test".to_owned(),
            std::process::id() as i32,
            worker_config,
            app_config,
            health.clone(),
            isolated_children(1),
            isolated_claim_columns(1),
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

    /// Python `core/models/health.py`, `slots_down_json`: no slot down is
    /// `[]`.
    #[test]
    fn no_down_slot_is_an_empty_list() {
        assert_eq!(slots_down_json(&[]), serde_json::json!([]));
        assert_eq!(
            slots_down_json(&isolated_children(2).down_slots()),
            serde_json::json!([])
        );
    }

    /// Python `slots_down_json`: each down slot with its index, its cause as
    /// `boot_cause_json` writes it, its detail, the time it went down and
    /// its failed boots.
    #[test]
    fn a_down_slot_is_written_with_its_cause_and_boots() {
        use crate::worker::executor::protocol::ShortageErrno;
        use chrono::TimeZone;
        use std::num::NonZeroU32;

        let down_since = Utc
            .with_ymd_and_hms(2026, 11, 16, 9, 30, 0)
            .single()
            .expect("a valid test instant");
        let down = [
            (
                1,
                DownState {
                    cause: BootFailureCause::ResourceShortage(ShortageErrno::Emfile),
                    detail: "Too many open files".to_owned(),
                    down_since,
                    failed_boots: NonZeroU32::new(3).expect("3"),
                },
            ),
            (
                2,
                DownState {
                    cause: BootFailureCause::ThreadStartRefused,
                    detail: "can't start new thread".to_owned(),
                    down_since,
                    failed_boots: NonZeroU32::new(1).expect("1"),
                },
            ),
        ];
        assert_eq!(
            slots_down_json(&down),
            serde_json::json!([
                {"slot": 1, "cause": {"kind": "resource_shortage", "errno": "EMFILE"},
                 "detail": "Too many open files", "down_since": "2026-11-16T09:30:00.000000+00:00",
                 "failed_boots": 3},
                {"slot": 2, "cause": {"kind": "thread_start_refused"},
                 "detail": "can't start new thread", "down_since": "2026-11-16T09:30:00.000000+00:00",
                 "failed_boots": 1},
            ])
        );
    }

    /// Python `test_claim_plan.py::test_budgets_and_worker_state_capacity`:
    /// each budget maps to its capacity and the classes it serves.
    #[test]
    fn class_capacity_names_each_budget_with_the_classes_it_serves() {
        let budgets = [
            BudgetColumn {
                budget: Execution::Isolated,
                capacity: 3,
                serves: vec![Execution::Isolated, Execution::SharedThread],
            },
            BudgetColumn {
                budget: Execution::SharedAsync,
                capacity: 40,
                serves: vec![Execution::SharedAsync],
            },
        ];

        assert_eq!(
            class_capacity_json(&budgets),
            serde_json::json!({
                "ISOLATED": {"capacity": 3, "serves": ["ISOLATED", "SHARED_THREAD"]},
                "SHARED_ASYNC": {"capacity": 40, "serves": ["SHARED_ASYNC"]}
            }),
        );
        assert_eq!(class_capacity_json(&[]), serde_json::json!({}));
    }

    /// Python `core/worker/health.py:233-239` with the `WorkerConfig` of
    /// `execution_settings(profile)`: `processes` is the child count,
    /// `max_claim_batch` the per-queue limit or 0, and `max_claim_per_worker`
    /// the claimed limit or else the seat count.
    #[test]
    fn the_claim_columns_of_a_profile_are_resolved_as_python() {
        use crate::core::config::worker_profile::TaskCount;

        let mut profile: WorkerProfile = serde_json::from_value(serde_json::json!({
            "isolated": {"kind": "isolated_children", "count": 2},
            "shared_async": {"kind": "shared_async_children", "count": 1, "seats": 10},
            "shared_thread": {"kind": "shared_thread_children", "count": 2, "seats": 4}
        }))
        .expect("the profile is valid");

        assert_eq!(
            ClaimColumns::of_profile(&profile),
            ClaimColumns {
                processes: 5,
                max_claim_batch: 0,
                max_claim_per_worker: 2 + 10 + 8,
            },
        );

        let tasks = |count: u32| TaskLimitSetting::TaskLimit {
            tasks: TaskCount::try_from(count).expect("a test limit is inside its bounds"),
        };
        profile.claim.per_queue_per_pass = tasks(3);
        profile.claim.claimed_per_worker = tasks(7);
        assert_eq!(
            ClaimColumns::of_profile(&profile),
            ClaimColumns {
                processes: 5,
                max_claim_batch: 3,
                max_claim_per_worker: 7,
            },
        );
    }

    /// Python `class_capacity_json(plan, capacity)` of a worker of isolated
    /// children: one `ISOLATED` budget of its slots in service that serves
    /// every class.
    #[test]
    fn the_class_capacity_of_isolated_slots_serves_every_class() {
        let service = PoolService {
            isolated: vec![
                SlotService::Booting,
                SlotService::InService,
                SlotService::Down,
            ],
            shared_async: Vec::new(),
            shared_thread: Vec::new(),
        };
        assert_eq!(
            class_capacity_of(&service),
            serde_json::json!({
                "ISOLATED": {
                    "capacity": 2,
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
        let worker_config = WorkerConfig::default();
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
            isolated_children(2),
            isolated_claim_columns(2),
            Utc::now(),
            cancel.clone(),
        );
        type Columns = (
            serde_json::Value,
            serde_json::Value,
            serde_json::Value,
            serde_json::Value,
            i32,
        );
        let written = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let row: Option<Columns> = sqlx::query_as(
                    "SELECT queue_priorities, queue_max_concurrency, slots_down, class_capacity,
                            max_claim_per_worker
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

        let (
            queue_priorities,
            queue_max_concurrency,
            slots_down,
            class_capacity,
            max_claim_per_worker,
        ) = written.expect("the first snapshot must land on the baseline schema");
        assert_eq!(queue_priorities, serde_json::json!({}));
        assert_eq!(queue_max_concurrency, serde_json::json!({}));
        assert_eq!(slots_down, serde_json::json!([]));
        assert_eq!(
            class_capacity,
            class_capacity_of(&isolated_children(2).service())
        );
        // No limit is stored resolved: the seats of the worker (#804).
        assert_eq!(max_claim_per_worker, 2);
    }

    /// Run the snapshot loop on `pool` for `window` with a 10 ms interval,
    /// then stop it and return the number of snapshot rows of `worker_id`.
    async fn snapshots_written_in(
        pool: &PgPool,
        worker_id: &str,
        window: std::time::Duration,
    ) -> i64 {
        use crate::worker::recovery::new_reaper_health;

        let mut app_config = AppConfig::for_database_url("postgresql://unused/test");
        app_config.recovery.worker_state_snapshot_interval_ms = 10;
        let cancel = CancellationToken::new();
        let task = spawn_worker_state_loop(
            pool.clone(),
            worker_id.to_owned(),
            "claimed-count-test".to_owned(),
            std::process::id() as i32,
            WorkerConfig::default(),
            app_config,
            new_reaper_health(),
            isolated_children(1),
            isolated_claim_columns(1),
            Utc::now(),
            cancel.clone(),
        );
        tokio::time::sleep(window).await;
        cancel.cancel();
        task.await.expect("the snapshot loop joins");
        sqlx::query_scalar("SELECT count(*) FROM horsies_worker_states WHERE worker_id = $1")
            .bind(worker_id)
            .fetch_one(pool)
            .await
            .expect("count the snapshots")
    }

    /// A failed count of claimed rows writes no snapshot (Python counts and
    /// inserts under one `try`, `core/worker/health.py:201-291`). The old
    /// loop wrote the snapshot with `tasks_claimed = 0`.
    #[tokio::test]
    #[serial_test::serial]
    async fn a_failed_claimed_count_writes_no_snapshot() {
        /// About 100 intervals of the loop.
        const LOOP_WINDOW: std::time::Duration = std::time::Duration::from_secs(1);

        let migrated = crate::broker::terminalization_matrix::migrated_pool().await;
        // On this pool a temporary `horsies_tasks` without the counted
        // columns hides the table of that name, so the count fails; the
        // snapshot table is untouched, so the insert alone would succeed.
        let shadowed = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .after_connect(|connection, _| {
                Box::pin(async move {
                    sqlx::query("CREATE TEMP TABLE horsies_tasks (id integer)")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect_with((*migrated.connect_options()).clone())
            .await
            .expect("connect the shadowed pool");
        let control_worker = uuid::Uuid::new_v4().to_string();
        let failing_worker = uuid::Uuid::new_v4().to_string();

        let control = snapshots_written_in(&migrated, &control_worker, LOOP_WINDOW).await;
        let written = snapshots_written_in(&shadowed, &failing_worker, LOOP_WINDOW).await;

        sqlx::query("DELETE FROM horsies_worker_states WHERE worker_id = ANY($1)")
            .bind(vec![control_worker, failing_worker])
            .execute(&migrated)
            .await
            .expect("delete the snapshots");
        shadowed.close().await;
        assert!(control > 0, "the loop writes when the count works");
        assert_eq!(written, 0, "a failed count must write no snapshot");
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
        let worker_config = WorkerConfig::default();
        let cancel = CancellationToken::new();
        let task = spawn_worker_state_loop(
            pool.clone(),
            worker_id.clone(),
            "first-snapshot-test".to_owned(),
            std::process::id() as i32,
            worker_config,
            app_config,
            new_reaper_health(),
            isolated_children(1),
            isolated_claim_columns(1),
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

#[cfg(test)]
mod state_read_spec_tests;
