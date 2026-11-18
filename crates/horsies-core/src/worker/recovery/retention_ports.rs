//! Ports of the Python reaper retention tests: the maintenance pass registers
//! the declared periods and keeps a leaf inside its period, row retention
//! keeps recent and running records, and a finished row lands in a leaf of
//! its class.
//!
//! Python specification: `tests/integration/test_reaper_registers_declared_classes.py`,
//! `test_reaper_partition_pruning_wiring.py`, `test_reaper_retention_cleanup.py`,
//! `test_retention_class_at_enqueue.py` and `test_retention_period_classes.py`.
//! Each test runs on its own database: the reaper pass and the class registry
//! are global to a database.

use chrono::{DateTime, Utc};
use serial_test::serial;
use uuid::Uuid;

use super::*;
use crate::broker::SendKind;
use crate::core::history::commands::{CreateDailyHistoryLeaf, LeafBounds, LeafRef};
use crate::core::history::ddl::classes::{finite_class_parent_name, register_period_class};
use crate::core::history::ddl::runtime_names::daily_leaf_name;
use crate::core::history::maintenance::coverage::ensure_partition_coverage_in_pool;
use crate::core::history::names::TASK_HISTORY_FOREVER_LEAF;
use crate::core::history::partitions::catalog::database_now;
use crate::core::history::partitions::manager::create_daily_leaf;
use crate::core::history::partitions::publication::UnpublishedLoader;
use crate::core::history::periods::RetentionDays;
use crate::core::lifecycle::TerminalizationOutcome;
use crate::test_support::sql::{self, TestPool, TestPoolOptions};
use crate::test_support::terminal_writes;

/// The row retention window of the Python tests.
const RETENTION_HOURS: u32 = 24;

/// One database per test, on the baseline schema with the Rust client family:
/// a copy of the installed test template (`crate::test_support`) with a sqlx
/// pool for the reaper pass.
struct RetentionDatabase {
    pool: TestPool,
    database: crate::test_support::TestDatabase,
}

impl RetentionDatabase {
    async fn create() -> Self {
        let database = crate::test_support::TestDatabase::create().await;
        let pool = TestPoolOptions::new()
            .max_connections(4)
            .connect(database.url())
            .await
            .expect("connect retention database");
        Self { pool, database }
    }

    async fn drop(self) {
        self.pool.close().await;
        self.database.drop().await;
    }
}

fn period(days: i64) -> RetentionDays {
    RetentionDays::parse_period(chrono::Duration::days(days)).expect("retention period")
}

/// A reaper pass that runs partition maintenance only: the recovery steps
/// are off and row retention is not due.
async fn run_maintenance_pass(
    pool: &TestPool,
    retention: &RetentionConfig,
) -> ReaperHealthSnapshot {
    let mut recovery = RecoveryConfig::default();
    recovery.auto_requeue_stale_claimed = false;
    recovery.auto_fail_stale_running = false;
    recovery.auto_terminate_orphaned_workflow_tasks = false;
    let health = new_reaper_health();
    let mut next_retention_cleanup = tokio::time::Instant::now() + Duration::from_secs(3_600);
    let mut next_partition_maintenance = tokio::time::Instant::now();
    let mut orphan_state = OrphanSweepState::default();
    run_reaper_pass(
        &crate::broker::PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
        crate::test_support::child_specs::no_child_specs().as_ref(),
        &recovery,
        retention,
        &health,
        &mut next_retention_cleanup,
        &mut next_partition_maintenance,
        &mut orphan_state,
        &mut UnheldOwners::default(),
    )
    .await;
    let snapshot = health.read().await.clone();
    snapshot
}

async fn database_today(pool: &TestPool) -> DateTime<Utc> {
    let mut connection = pool.acquire().await.expect("acquire clock");
    let now = database_now(&mut connection).await.expect("database now");
    now.date_naive()
        .and_hms_opt(0, 0, 0)
        .expect("midnight")
        .and_utc()
}

async fn create_period_leaf(pool: &TestPool, days: RetentionDays, lower: DateTime<Utc>) -> LeafRef {
    let class_key = days.class_key();
    let mut transaction = pool.begin().await.expect("begin leaf creation");
    let _ = register_period_class(&mut transaction, days)
        .await
        .expect("register the class");
    let parent = finite_class_parent_name(&class_key).expect("class parent");
    let leaf = LeafRef::new(
        daily_leaf_name(&parent, lower).expect("leaf name"),
        class_key,
        LeafBounds::new(lower, lower + chrono::Duration::days(1)).expect("one day"),
    )
    .expect("leaf ref");
    let created = create_daily_leaf(
        &mut transaction,
        &CreateDailyHistoryLeaf::new(leaf.clone()).expect("daily command"),
        &UnpublishedLoader,
    )
    .await
    .expect("create leaf");
    transaction.commit().await.expect("commit leaf creation");
    assert!(
        matches!(
            created,
            crate::core::history::outcomes::LeafCreation::Created { .. }
        ),
        "{created:?}"
    );
    leaf
}

async fn relation_exists(pool: &TestPool, relation: &str) -> bool {
    sql::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(relation)
        .fetch_one(pool)
        .await
        .expect("read relation presence")
}

async fn registered_classes(pool: &TestPool) -> Vec<String> {
    sql::query_scalar("SELECT class_key FROM horsies_retention_classes ORDER BY class_key")
        .fetch_all(pool)
        .await
        .expect("read the class registry")
}

/// `TestReaperPrunesExpiredLeaves.test_pass_keeps_a_leaf_inside_its_horizon`:
/// the pass drops the leaf past its period and keeps an older leaf that is
/// still inside it.
#[tokio::test]
#[serial]
async fn the_pass_keeps_a_leaf_inside_its_period() {
    let database = RetentionDatabase::create().await;
    let today = database_today(&database.pool).await;
    let overdue = create_period_leaf(
        &database.pool,
        period(30),
        today - chrono::Duration::days(40),
    )
    .await;
    let inside = create_period_leaf(
        &database.pool,
        period(30),
        today - chrono::Duration::days(10),
    )
    .await;

    let health = run_maintenance_pass(&database.pool, &RetentionConfig::default()).await;

    let pruning = health.partition_pruning.expect("pruning health");
    assert_eq!(pruning["errors"].as_array().map(Vec::len), Some(0));
    assert!(!relation_exists(&database.pool, overdue.leaf_name()).await);
    assert!(
        relation_exists(&database.pool, inside.leaf_name()).await,
        "a leaf inside its period was dropped"
    );
    database.drop().await;
}

/// `TestReaperRegistersDeclaredClasses.test_the_pass_registers_a_declared_class`.
#[tokio::test]
#[serial]
async fn the_pass_registers_a_declared_period() {
    let database = RetentionDatabase::create().await;
    let mut retention = RetentionConfig::default();
    retention.retention_periods = vec![chrono::Duration::days(11)];

    let _ = run_maintenance_pass(&database.pool, &retention).await;

    let stored: Option<i16> = sql::query_scalar(
        "SELECT retention_days FROM horsies_retention_classes WHERE class_key = 'standard_11d'",
    )
    .fetch_optional(&database.pool)
    .await
    .expect("read the declared class");
    assert_eq!(
        stored,
        Some(11),
        "the declared period was not registered by the pass"
    );
    database.drop().await;
}

/// `TestReaperRegistersDeclaredClasses.test_declaring_nothing_registers_nothing_extra`.
#[tokio::test]
#[serial]
async fn declaring_nothing_registers_nothing_extra() {
    let database = RetentionDatabase::create().await;

    let _ = run_maintenance_pass(&database.pool, &RetentionConfig::default()).await;

    assert_eq!(
        registered_classes(&database.pool).await,
        vec![
            "forever".to_owned(),
            "heartbeats".to_owned(),
            "standard_30d".to_owned(),
        ]
    );
    database.drop().await;
}

async fn seed_worker_state(pool: &TestPool, worker_id: &str, age_hours: i32) {
    sql::query(
        "INSERT INTO horsies_worker_states (
             worker_id, snapshot_at, hostname, pid, processes,
             max_claim_batch, max_claim_per_worker, cluster_wide_cap,
             queues, queue_priorities, queue_max_concurrency, recovery_config,
             tasks_running, tasks_claimed,
             memory_usage_mb, memory_percent, cpu_percent, children_memory_mb,
             worker_started_at, slots_down, class_capacity
         ) VALUES (
             $1, NOW() - make_interval(hours => $2), 'host', 1, 1,
             10, 10, NULL,
             '{default}', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
             0, 0,
             NULL, NULL, NULL, NULL,
             NOW() - make_interval(hours => $2 + 1), '[]'::jsonb, '{}'::jsonb
         )",
    )
    .bind(worker_id)
    .bind(age_hours)
    .execute(pool)
    .await
    .expect("insert worker state");
}

/// `test_expired_worker_states_deleted`: a snapshot older than the window is
/// deleted; a recent one stays.
#[tokio::test]
#[serial]
async fn expired_worker_states_go_and_recent_ones_stay() {
    let database = RetentionDatabase::create().await;
    seed_worker_state(&database.pool, "port-old-worker", 48).await;
    seed_worker_state(&database.pool, "port-recent-worker", 1).await;
    let mut retention = RetentionConfig::default();
    retention.worker_state_retention_hours = Some(RETENTION_HOURS);
    retention.terminal_record_retention_hours = None;

    run_retention_cleanup(
        &crate::broker::PostgresBroker::from_test_url(
            database.pool.url(),
            crate::ClientFamily::Rust,
        ),
        &retention,
    )
    .await;

    let left: Vec<String> =
        sql::query_scalar("SELECT worker_id FROM horsies_worker_states ORDER BY worker_id")
            .fetch_all(&database.pool)
            .await
            .expect("read worker states");
    assert_eq!(left, vec!["port-recent-worker".to_owned()]);
    database.drop().await;
}

async fn seed_workflow(pool: &TestPool, status: &str, age_hours: i32) -> Uuid {
    let workflow_id = Uuid::new_v4();
    sql::query(
        "INSERT INTO horsies_workflows (
             id, name, definition_key, status, on_error, output_task_index,
             success_policy, result, error, parent_workflow_id, parent_task_index,
             root_workflow_id, depth, sent_at, created_at, started_at, completed_at,
             updated_at
         ) VALUES (
             $1, 'port_retention', NULL, $2, 'fail', NULL,
             NULL, NULL, NULL, NULL, NULL,
             $1, 0, NOW() - make_interval(hours => $3), NOW() - make_interval(hours => $3),
             NOW() - make_interval(hours => $3),
             CASE WHEN $2 = 'RUNNING' THEN NULL ELSE NOW() - make_interval(hours => $3) END,
             NOW() - make_interval(hours => $3)
         )",
    )
    .bind(workflow_id)
    .bind(status)
    .bind(age_hours)
    .execute(pool)
    .await
    .expect("insert workflow");
    workflow_id
}

async fn seed_node(pool: &TestPool, workflow_id: Uuid) {
    sql::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
             dependencies, args_from, workflow_ctx_from, allow_failed_deps, join_type,
             min_success, task_options, max_retries, execution, good_until,
             retention_class_key, status, task_id, is_subworkflow, sub_workflow_id,
             sub_workflow_name, sub_definition_key, sub_workflow_summary, result, error,
             created_at, started_at, completed_at
         ) VALUES (
             'NONE', $1, $2, 0, 'node_0', 'retention_test', '{}', 'default', 100,
             '{}', NULL, NULL, FALSE, 'all',
             NULL, NULL, 0, 'ISOLATED', NULL,
             'standard_30d', 'COMPLETED', NULL, FALSE, NULL,
             NULL, NULL, NULL, NULL, NULL,
             NOW(), NULL, NULL
         )",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .execute(pool)
    .await
    .expect("insert workflow node");
}

/// `test_expired_terminal_workflows_deleted`: an old terminal workflow goes
/// with its nodes; an old RUNNING workflow and a recent terminal one stay.
#[tokio::test]
#[serial]
async fn expired_terminal_workflows_go_and_running_or_recent_ones_stay() {
    let database = RetentionDatabase::create().await;
    let old_completed = seed_workflow(&database.pool, "COMPLETED", 48).await;
    let old_running = seed_workflow(&database.pool, "RUNNING", 48).await;
    let recent_completed = seed_workflow(&database.pool, "COMPLETED", 1).await;
    seed_node(&database.pool, old_completed).await;
    seed_node(&database.pool, old_running).await;
    let mut retention = RetentionConfig::default();
    retention.worker_state_retention_hours = None;
    retention.terminal_record_retention_hours = Some(RETENTION_HOURS);

    run_retention_cleanup(
        &crate::broker::PostgresBroker::from_test_url(
            database.pool.url(),
            crate::ClientFamily::Rust,
        ),
        &retention,
    )
    .await;

    let mut workflows: Vec<Uuid> = sql::query_scalar("SELECT id FROM horsies_workflows")
        .fetch_all(&database.pool)
        .await
        .expect("read workflows");
    workflows.sort();
    let mut expected = vec![old_running, recent_completed];
    expected.sort();
    assert_eq!(workflows, expected);
    let nodes: Vec<Uuid> = sql::query_scalar("SELECT workflow_id FROM horsies_workflow_tasks")
        .fetch_all(&database.pool)
        .await
        .expect("read workflow nodes");
    assert_eq!(nodes, vec![old_running]);
    database.drop().await;
}

/// A RUNNING plain task of `class_key`, claimed by `worker_id`. Every baseline
/// column is named.
async fn seed_running_task(pool: &TestPool, class_key: &str, worker_id: &str) -> Uuid {
    let task_id = Uuid::new_v4();
    sql::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, input, task_options, execution,
             retention_class_key, sent_at, created_at,
             command_fingerprint_version, command_fingerprint, idempotency_key_digest,
             status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
             started_at, finalizing_at, finalizing_by_worker_id,
             worker_pid, worker_hostname, worker_process_name,
             retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
             updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
         ) VALUES (
             $1, 'port_retention_task', 'default', 100, '{}', NULL, 'ISOLATED',
             $2, NOW(), NOW(),
             1, sha256(convert_to($1::text, 'UTF8')), NULL,
             'RUNNING', NOW(), NOW(), $3, NULL,
             NOW(), NULL, NULL,
             4242, 'port-host', 'port-process',
             0, 0, NULL, NULL, FALSE,
             NOW(), FALSE, NULL, NULL
         )",
    )
    .bind(task_id)
    .bind(class_key)
    .bind(worker_id)
    .execute(pool)
    .await
    .expect("insert running task");
    task_id
}

/// `test_retention_class_at_enqueue.py::test_forever_row_terminalizes_into_the_forever_partition`,
/// `::test_default_row_terminalizes_into_its_daily_partition` and
/// `test_retention_period_classes.py::test_a_finished_row_lands_in_a_leaf_of_the_period_class`:
/// a completed row is stored in the static forever leaf, or in the leaf of
/// its class for the day of its terminal time.
#[tokio::test]
#[serial]
async fn a_finished_row_lands_in_the_leaf_of_its_class() {
    let database = RetentionDatabase::create().await;
    let coverage = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[period(7)],
    )
    .await
    .expect("coverage pass");
    assert!(
        matches!(coverage, CoverageOutcome::Ensured(_)),
        "{coverage:?}"
    );
    let worker_id = "port-retention-worker";
    let result = serde_json::to_string(&crate::core::TaskResult::Ok(serde_json::json!({
        "stored": true
    })))
    .expect("encode result");
    for class_key in ["forever", "standard_30d", "standard_7d"] {
        let task_id = seed_running_task(&database.pool, class_key, worker_id).await;
        let outcomes =
            terminal_writes::complete_plain_task(&database.pool, task_id, worker_id, &result)
                .await
                .expect("complete the task");
        assert!(
            matches!(
                outcomes.as_slice(),
                [TerminalizationOutcome::Applied { .. }]
            ),
            "{outcomes:?}"
        );
        let (leaf, terminal_at): (String, DateTime<Utc>) = sql::query_as(
            "SELECT tableoid::regclass::text, terminal_at
             FROM horsies_task_history WHERE task_id = $1",
        )
        .bind(task_id)
        .fetch_one(&database.pool)
        .await
        .expect("read the history row");
        let expected = match class_key {
            "forever" => TASK_HISTORY_FOREVER_LEAF.to_owned(),
            period_key => {
                let parent = finite_class_parent_name(period_key).expect("class parent");
                let day = terminal_at
                    .date_naive()
                    .and_hms_opt(0, 0, 0)
                    .expect("midnight")
                    .and_utc();
                daily_leaf_name(&parent, day).expect("leaf name")
            }
        };
        assert_eq!(leaf, expected, "{class_key}");
    }
    database.drop().await;
}

/// `TestEveryPathStampsThePeriodClass.test_a_delayed_send_stamps_the_period_class`:
/// a delayed send to a queue mapped to 7 days stores `standard_7d`.
#[tokio::test]
#[serial]
async fn a_delayed_send_stamps_the_mapped_period_class() {
    let database = RetentionDatabase::create().await;
    let mut retention = RetentionConfig::default();
    retention
        .queue_retention
        .insert("bulk".to_owned(), Some(chrono::Duration::days(7)));
    let broker = crate::broker::PostgresBroker::from_test_url(
        database.pool.url(),
        crate::ClientFamily::Rust,
    );
    let task_id = broker
        .enqueue(
            "port_delayed_send",
            "{}",
            "bulk",
            50,
            Some(Utc::now()),
            None,
            None,
            None,
            None,
            Some(60),
            None,
            retention.resolve_queue_retention("bulk"),
            None,
            SendKind::First,
        )
        .await
        .expect("delayed send");
    let (class_key, delayed): (String, bool) = sql::query_as(
        "SELECT retention_class_key, enqueued_at > NOW() FROM horsies_tasks WHERE id = $1",
    )
    .bind(task_id)
    .fetch_one(&database.pool)
    .await
    .expect("read the delayed row");
    assert_eq!(class_key, "standard_7d");
    assert!(delayed, "the send is delayed");
    database.drop().await;
}
