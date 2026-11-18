use std::sync::Arc;

use chrono::{DateTime, SecondsFormat, Utc};
use sysinfo::{Pid, System};
use tokio::task::JoinHandle;
use tokio_postgres::types::Json;
use tokio_util::sync::CancellationToken;

use crate::broker::core_rows::{column, one_row, query_on, StatementLog};
use crate::broker::{BrokerError, PostgresBroker};
use crate::db::{
    CheckoutClass, CorePool, NotSentCause, ParameterType, PooledConnection, StatementBound,
    StatementFailure, TypedStatement,
};

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

/// The count of the RUNNING and the CLAIMED rows of a worker: `$1` the
/// worker id.
static HELD_ROWS: TypedStatement = TypedStatement::new(
    "worker_held_rows",
    "SELECT COUNT(*) FILTER (WHERE status = 'RUNNING') AS running,
            COUNT(*) FILTER (WHERE status = 'CLAIMED') AS claimed
     FROM horsies_tasks
     WHERE claimed_by_worker_id = $1 AND status IN ('RUNNING', 'CLAIMED')",
    &[ParameterType::Text],
);

/// [`INSERT_WORKER_STATE_SQL`] with its parameter types, in column order.
static INSERT_WORKER_STATE: TypedStatement = TypedStatement::new(
    "insert_worker_state",
    INSERT_WORKER_STATE_SQL,
    &[
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Int4,
        ParameterType::Int4,
        ParameterType::Int4,
        ParameterType::Int4,
        ParameterType::Int4,
        ParameterType::TextArray,
        ParameterType::Jsonb,
        ParameterType::Jsonb,
        ParameterType::Jsonb,
        ParameterType::Int4,
        ParameterType::Int4,
        ParameterType::Float8,
        ParameterType::Float8,
        ParameterType::Float8,
        ParameterType::Timestamptz,
        ParameterType::Jsonb,
        ParameterType::Jsonb,
    ],
);

/// Count the RUNNING and the CLAIMED rows of this worker in one statement
/// on `connection` (Python `_count_only_running_for_worker` and
/// `_count_claimed_for_worker`, two statements on one session).
///
/// Errors: [`BrokerError::Statement`] for a failed statement;
/// [`BrokerError::RowDecode`] when the row does not read.
async fn query_held_rows(
    connection: &mut PooledConnection,
    log: StatementLog,
    worker_id: &str,
) -> Result<HeldRows, BrokerError> {
    let statement = &HELD_ROWS;
    let bound = statement.bind().value(&worker_id).finish();
    let rows = query_on(connection, log, &bound).await?;
    let row = one_row(rows, statement)?;
    let running: i64 = column(&row, statement, "running")?;
    let claimed: i64 = column(&row, statement, "claimed")?;
    Ok(HeldRows {
        running: i32::try_from(running).unwrap_or(i32::MAX),
        claimed: i32::try_from(claimed).unwrap_or(i32::MAX),
    })
}

/// What a worker snapshot is built from. The resource reader keeps its
/// state between snapshots (the CPU usage is a difference of two reads).
pub(crate) struct SnapshotSource {
    pub worker_id: String,
    pub hostname: String,
    pub pid: i32,
    pub worker_config: WorkerConfig,
    pub app_config: AppConfig,
    pub reaper_health: ReaperHealth,
    pub children: ChildrenView,
    pub claim_columns: ClaimColumns,
    pub worker_started_at: DateTime<Utc>,
    sys: System,
    sys_pid: Pid,
}

impl SnapshotSource {
    /// A source whose resource reader has read the process once.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        worker_id: String,
        hostname: String,
        pid: i32,
        worker_config: WorkerConfig,
        app_config: AppConfig,
        reaper_health: ReaperHealth,
        children: ChildrenView,
        claim_columns: ClaimColumns,
        worker_started_at: DateTime<Utc>,
    ) -> Self {
        let sys_pid = Pid::from(pid as usize);
        let mut sys = System::new();
        sys.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::Some(&[sys_pid]),
            true,
            sysinfo::ProcessRefreshKind::new().with_cpu().with_memory(),
        );
        sys.refresh_memory();
        Self {
            worker_id,
            hostname,
            pid,
            worker_config,
            app_config,
            reaper_health,
            children,
            claim_columns,
            worker_started_at,
            sys,
            sys_pid,
        }
    }

    /// The `recovery_config` column: the recovery config, the two retention
    /// values and the health of each reaper step.
    async fn recovery_value(&self) -> serde_json::Value {
        let health = self.reaper_health.read().await.clone();
        let mut recovery_value = serde_json::to_value(&self.app_config.recovery)
            .unwrap_or_else(|_| serde_json::json!({}));
        if let Some(object) = recovery_value.as_object_mut() {
            object.insert(
                "worker_state_retention_hours".to_owned(),
                serde_json::to_value(self.app_config.retention.worker_state_retention_hours)
                    .unwrap_or(serde_json::Value::Null),
            );
            object.insert(
                "terminal_record_retention_hours".to_owned(),
                serde_json::to_value(self.app_config.retention.terminal_record_retention_hours)
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
        recovery_value
    }
}

/// Why no snapshot row was written.
#[derive(Debug)]
pub(crate) enum SnapshotFailure {
    /// The checkout or the count of the held rows failed, so no snapshot
    /// was built: storing 0 would record "no claimed row" for a count that
    /// was not taken.
    NotCounted(BrokerError),
    /// The insert of the snapshot failed; nothing was written, or the
    /// outcome is unknown.
    InsertFailed(BrokerError),
}

/// Count the held rows and insert one snapshot, on one connection of `pool`
/// that passes the reserve gate ([`CheckoutClass::Batch`]): the snapshot is
/// monitoring, not a per-task write (Python `_update_worker_state`: the
/// counts and the insert on one session, under one `try`).
///
/// Errors: [`SnapshotFailure`].
pub(crate) async fn write_snapshot(
    pool: &CorePool,
    log: StatementLog,
    source: &mut SnapshotSource,
) -> Result<(), SnapshotFailure> {
    let mut connection = pool.checkout(CheckoutClass::Batch).await.map_err(|cause| {
        SnapshotFailure::NotCounted(BrokerError::Statement(StatementFailure::NotSent {
            statement: HELD_ROWS.name(),
            cause: NotSentCause::Checkout(cause),
        }))
    })?;
    let held = query_held_rows(&mut connection, log, &source.worker_id)
        .await
        .map_err(SnapshotFailure::NotCounted)?;

    let slots_down = slots_down_json(&source.children.down_slots());
    let class_capacity = class_capacity_of(&source.children.service());
    let metrics = collect_resource_metrics(&mut source.sys, source.sys_pid);
    let recovery_value = source.recovery_value().await;
    let processes = source.claim_columns.processes as i32;
    let max_claim_batch = source.claim_columns.max_claim_batch as i32;
    let max_claim_per_worker = source.claim_columns.max_claim_per_worker as i32;
    let cluster_wide_cap = source.app_config.cluster_wide_cap.map(|cap| cap as i32);
    // The queue maps are stored as they are: an empty map is `{}`, never
    // NULL (both columns are NOT NULL on the baseline).
    let queue_priorities = Json(&source.worker_config.queue_priorities);
    let queue_max_concurrency = Json(&source.worker_config.queue_max_concurrency);
    let bound = INSERT_WORKER_STATE
        .bind()
        .value(&source.worker_id)
        .value(&source.hostname)
        .value(&source.pid)
        .value(&processes)
        .value(&max_claim_batch)
        .value(&max_claim_per_worker)
        .value(&cluster_wide_cap)
        .value(&source.worker_config.queues)
        .value(&queue_priorities)
        .value(&queue_max_concurrency)
        .value(&recovery_value)
        .value(&held.running)
        .value(&held.claimed)
        .value(&metrics.memory_usage_mb)
        .value(&metrics.memory_percent)
        .value(&metrics.cpu_percent)
        .value(&source.worker_started_at)
        .value(&slots_down)
        .value(&class_capacity)
        .finish();
    log.log(&INSERT_WORKER_STATE);
    let _inserted = connection
        .execute(&bound, StatementBound::ConnectionTimeouts)
        .await
        .map_err(|failure| SnapshotFailure::InsertFailed(BrokerError::Statement(failure)))?;
    Ok(())
}

/// Spawn a background loop that inserts worker state snapshots on
/// `recovery.worker_state_snapshot_interval_ms` (default 30s).
///
/// Tracks the worker's liveness, task counts, and resource metrics
/// (memory/CPU via sysinfo) in `horsies_worker_states` as a timeseries.
/// Each snapshot is a new row (INSERT), enabling historical analysis. Each
/// snapshot runs on the main core pool of `broker` ([`write_snapshot`]); a
/// close of the broker waits for a snapshot in flight.
///
/// `children` gives the slots of the worker at each snapshot: the down
/// slots (`slots_down`) and the capacity in service of each budget
/// (`class_capacity`). `claim_columns` are the profile's
/// ([`ClaimColumns::of_profile`]).
#[allow(clippy::too_many_arguments)]
pub fn spawn_worker_state_loop(
    broker: Arc<PostgresBroker>,
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
        let mut source = SnapshotSource::new(
            worker_id,
            hostname,
            pid,
            worker_config,
            app_config,
            reaper_health,
            children,
            claim_columns,
            worker_started_at,
        );

        // The first snapshot is written at start, then one per interval
        // (Python `_worker_state_heartbeat_loop`: write, then wait), so the
        // fleet check sees a starting worker at once.
        while !cancel.is_cancelled() {
            let written = match broker.core_pools_for_write() {
                Ok((open, _write)) => {
                    write_snapshot(open.main(), broker.statement_log(), &mut source).await
                }
                Err(error) => Err(SnapshotFailure::NotCounted(error)),
            };
            match written {
                Ok(()) => {}
                Err(SnapshotFailure::NotCounted(error)) => {
                    tracing::error!(
                        worker_id = %source.worker_id,
                        %error,
                        "failed to count claimed tasks: {error}; no worker snapshot this interval",
                    );
                }
                Err(SnapshotFailure::InsertFailed(error)) => {
                    tracing::warn!(error = %error, "Failed to update worker state: {error}");
                }
            }

            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(snapshot_interval) => {}
            }
        }

        tracing::debug!(worker_id = %source.worker_id, "worker state loop stopped");
    })
}

#[cfg(test)]
mod tests {
    use sqlx::PgPool;

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

        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let worker_id = uuid::Uuid::new_v4().to_string();
        let health = new_reaper_health();
        let phase2 = json!({"applied": 2, "failed": 0});
        health.write().await.phase2_recovery = Some(phase2.clone());
        let mut app_config = AppConfig::for_database_url("postgresql://unused/test");
        app_config.recovery.worker_state_snapshot_interval_ms = 10;
        let worker_config = WorkerConfig::default();
        let cancel = CancellationToken::new();
        let task = spawn_worker_state_loop(
            Arc::new(crate::broker::PostgresBroker::from_pool(
                pool.clone(),
                crate::ClientFamily::Rust,
            )),
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

    /// Python `test_health_models.py::TestSlotHealth::test_each_cause_that_can_clear_round_trips`:
    /// a down slot of each cause that can clear reads back from the written
    /// column as it was written.
    #[test]
    fn each_cause_that_can_clear_round_trips_through_the_column() {
        use crate::broker::slot_health::{
            parse_slot_health, ClearableBootCause, DownSlot, SlotHealth,
        };
        use crate::worker::executor::protocol::ShortageErrno;
        use chrono::TimeZone;
        use std::num::{NonZeroU32, NonZeroU64};

        let down_since = Utc
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .single()
            .expect("a valid test instant");
        for (written, read) in [
            (
                BootFailureCause::ResourceShortage(ShortageErrno::Enomem),
                ClearableBootCause::ResourceShortage(ShortageErrno::Enomem),
            ),
            (
                BootFailureCause::ThreadStartRefused,
                ClearableBootCause::ThreadStartRefused,
            ),
        ] {
            let column = slots_down_json(&[(
                2,
                DownState {
                    cause: written,
                    detail: "x".to_owned(),
                    down_since,
                    failed_boots: NonZeroU32::new(1).expect("1"),
                },
            )]);

            let Ok(SlotHealth::SlotsDown(slots)) = parse_slot_health(&column, 4) else {
                panic!("the written down slot reads back: {column}");
            };

            assert_eq!(
                slots.as_slice(),
                [DownSlot {
                    slot: 2,
                    cause: read,
                    detail: "x".to_owned(),
                    down_since,
                    failed_boots: NonZeroU64::new(1).expect("1"),
                }]
            );
        }
    }

    /// Python `test_health_models.py::TestSlotHealth::test_the_writer_covers_every_down_slot_field`:
    /// each field of the read `DownSlot` reaches the stored entry, so a new
    /// field cannot be read without a writer. The pattern names every field
    /// with no rest: a new field does not compile here until it is checked
    /// and named in the written keys.
    #[test]
    fn the_writer_covers_every_down_slot_field() {
        use crate::broker::slot_health::{
            parse_slot_health, ClearableBootCause, DownSlot, SlotHealth,
        };
        use crate::worker::executor::protocol::ShortageErrno;
        use chrono::TimeZone;
        use std::collections::BTreeSet;
        use std::num::NonZeroU32;

        let written_since = Utc
            .with_ymd_and_hms(2026, 1, 1, 12, 0, 0)
            .single()
            .expect("a valid test instant");
        let column = slots_down_json(&[(
            1,
            DownState {
                cause: BootFailureCause::ResourceShortage(ShortageErrno::Emfile),
                detail: "OSError: [Errno 24] Too many open files".to_owned(),
                down_since: written_since,
                failed_boots: NonZeroU32::new(3).expect("3"),
            },
        )]);
        let Ok(SlotHealth::SlotsDown(slots)) = parse_slot_health(&column, 4) else {
            panic!("the written down slot reads back: {column}");
        };
        let [DownSlot {
            slot,
            cause,
            detail,
            down_since,
            failed_boots,
        }] = slots.as_slice()
        else {
            panic!("one down slot: {:?}", slots.as_slice());
        };

        assert_eq!(*slot, 1);
        assert_eq!(
            *cause,
            ClearableBootCause::ResourceShortage(ShortageErrno::Emfile)
        );
        assert_eq!(detail, "OSError: [Errno 24] Too many open files");
        assert_eq!(*down_since, written_since);
        assert_eq!(failed_boots.get(), 3);
        let keys: BTreeSet<&str> = column[0]
            .as_object()
            .expect("an entry is a JSON object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            BTreeSet::from(["slot", "cause", "detail", "down_since", "failed_boots"])
        );
    }

    /// Python `test_health_models.py::TestClassCapacity::test_the_writer_covers_every_read_field`:
    /// each field of the read `ClassBudget` but the budget (the key of its
    /// entry) reaches the stored entry, so a new field cannot be read
    /// without a writer. The pattern names every field with no rest.
    #[test]
    fn the_writer_covers_every_read_field_of_a_budget() {
        use crate::broker::class_budgets::{parse_class_capacity, ClassBudget};
        use std::collections::BTreeSet;

        let column = class_capacity_json(&[BudgetColumn {
            budget: Execution::Isolated,
            capacity: 1,
            serves: vec![Execution::Isolated],
        }]);
        let budgets = parse_class_capacity(&column).expect("the written column reads back");
        let [ClassBudget {
            budget,
            capacity,
            serves,
        }] = budgets.as_slice()
        else {
            panic!("one budget: {:?}", budgets.as_slice());
        };

        assert_eq!(*budget, Execution::Isolated);
        assert_eq!(*capacity, 1);
        assert_eq!(serves.as_slice(), [Execution::Isolated]);
        let keys: BTreeSet<&str> = column["ISOLATED"]
            .as_object()
            .expect("an entry is a JSON object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, BTreeSet::from(["capacity", "serves"]));
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

        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let worker_id = uuid::Uuid::new_v4().to_string();
        let mut app_config = AppConfig::for_database_url("postgresql://unused/test");
        app_config.recovery.worker_state_snapshot_interval_ms = 60_000;
        let worker_config = WorkerConfig::default();
        assert!(worker_config.queue_priorities.is_empty());
        assert!(worker_config.queue_max_concurrency.is_empty());
        let cancel = CancellationToken::new();
        let task = spawn_worker_state_loop(
            Arc::new(crate::broker::PostgresBroker::from_pool(
                pool.clone(),
                crate::ClientFamily::Rust,
            )),
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

    /// A snapshot source of one isolated child with no claim limit, for
    /// `worker_id`.
    fn one_child_source(worker_id: &str) -> SnapshotSource {
        use crate::worker::recovery::new_reaper_health;

        SnapshotSource::new(
            worker_id.to_owned(),
            "claimed-count-test".to_owned(),
            std::process::id() as i32,
            WorkerConfig::default(),
            AppConfig::for_database_url("postgresql://unused/test"),
            new_reaper_health(),
            isolated_children(1),
            isolated_claim_columns(1),
            Utc::now(),
        )
    }

    /// The snapshot rows of `worker_id`.
    async fn snapshot_rows(pool: &PgPool, worker_id: &str) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM horsies_worker_states WHERE worker_id = $1")
            .bind(worker_id)
            .fetch_one(pool)
            .await
            .expect("count the snapshots")
    }

    /// A temporary `horsies_tasks` without the counted columns: on the one
    /// connection of a pool, it hides the table of that name.
    static SHADOW_TASKS_TABLE: TypedStatement = TypedStatement::new(
        "shadow_tasks_table",
        "CREATE TEMP TABLE horsies_tasks (id integer)",
        &[],
    );

    /// A failed count of claimed rows writes no snapshot (Python counts and
    /// inserts under one `try`, `core/worker/health.py:201-291`). The old
    /// loop wrote the snapshot with `tasks_claimed = 0`.
    #[tokio::test]
    #[serial_test::serial]
    async fn a_failed_claimed_count_writes_no_snapshot() {
        let migrated = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        // On the one connection of this pool a temporary `horsies_tasks`
        // without the counted columns hides the table of that name, so the
        // count fails; the snapshot table is untouched, so the insert alone
        // would succeed.
        let shadowed =
            crate::broker::pools::unpinged_single_connection_core_pool(&migrated.connect_options());
        {
            let mut connection = shadowed
                .checkout(CheckoutClass::Batch)
                .await
                .expect("check out the one connection");
            let _created = connection
                .execute(
                    &SHADOW_TASKS_TABLE.bind().finish(),
                    StatementBound::ConnectionTimeouts,
                )
                .await
                .expect("create the shadow table");
        }
        let control = crate::broker::pools::core_pool_for_test_pool(&migrated);
        let control_worker = uuid::Uuid::new_v4().to_string();
        let failing_worker = uuid::Uuid::new_v4().to_string();

        let control_written = write_snapshot(
            &control,
            StatementLog::Off,
            &mut one_child_source(&control_worker),
        )
        .await;
        let failed = write_snapshot(
            &shadowed,
            StatementLog::Off,
            &mut one_child_source(&failing_worker),
        )
        .await;
        let control_rows = snapshot_rows(&migrated, &control_worker).await;
        let written = snapshot_rows(&migrated, &failing_worker).await;

        sqlx::query("DELETE FROM horsies_worker_states WHERE worker_id = ANY($1)")
            .bind(vec![control_worker, failing_worker])
            .execute(&migrated)
            .await
            .expect("delete the snapshots");
        shadowed.close();
        control.close();
        assert!(control_written.is_ok(), "{control_written:?}");
        assert_eq!(
            control_rows, 1,
            "the snapshot is written when the count works"
        );
        assert!(
            matches!(failed, Err(SnapshotFailure::NotCounted(_))),
            "{failed:?}"
        );
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

        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let worker_id = uuid::Uuid::new_v4().to_string();
        let mut app_config = AppConfig::for_database_url("postgresql://unused/test");
        app_config.recovery.worker_state_snapshot_interval_ms = 60_000;
        let worker_config = WorkerConfig::default();
        let cancel = CancellationToken::new();
        let task = spawn_worker_state_loop(
            Arc::new(crate::broker::PostgresBroker::from_pool(
                pool.clone(),
                crate::ClientFamily::Rust,
            )),
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
