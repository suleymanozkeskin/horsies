use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::broker::listener::notify_task_queue;
use crate::core::config::payload::PayloadPolicy;
use crate::core::config::recovery::RecoveryConfig;
use crate::core::config::retention::RetentionConfig;
use crate::core::history::maintenance::coverage::{
    ensure_partition_coverage_in_pool, CoverageOutcome, DeclaredRetentionClass,
};
use crate::core::history::maintenance::pruning::prune_expired_partitions;
use crate::core::history::reads::publisher::StagedLoaderPublisher;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::core::task::retry_utils::check_retry_eligibility;
use crate::core::{OperationalErrorCode, TaskError, TaskResult};

use crate::worker::retry::calculate_retry_delay;

/// SQL: Phase 1 scan — find stale RUNNING task IDs (no row locks).
/// Releases immediately so workers can finalize between scan and per-task lock.
const FIND_STALE_RUNNING_IDS_SQL: &str = "\
SELECT t.id
FROM horsies_tasks t
LEFT JOIN LATERAL (
    SELECT sent_at AS last_heartbeat
    FROM horsies_heartbeats
    WHERE task_id = t.id AND role = 'runner'
    ORDER BY sent_at DESC
    LIMIT 1
) hb ON TRUE
WHERE t.status = 'RUNNING'
  AND t.started_at IS NOT NULL
  AND (
      t.finalizing_at IS NULL
      OR t.finalizing_at < NOW() - $2 * INTERVAL '1 second'
  )
  AND COALESCE(hb.last_heartbeat, t.started_at) < NOW() - $1 * INTERVAL '1 second'
LIMIT $3";

/// SQL: Phase 2 per-task — re-acquire row with full context for retry eligibility.
///
/// Returns no rows if:
/// - the task is no longer RUNNING (worker finalized it between phases), OR
/// - a fresh runner heartbeat arrived after Phase 1 (closes the scan race).
///
/// The `NOT EXISTS` subquery re-checks heartbeat freshness using the same
/// stale threshold ($2) as Phase 1, ensuring a heartbeat that lands between
/// Phase 1 and Phase 2 saves the task from being falsely crashed.
const SELECT_STALE_TASK_FOR_UPDATE_SQL: &str = "\
SELECT
    t.id, t.retry_count, t.worker_pid, t.worker_hostname,
    t.claimed_by_worker_id, t.started_at, t.worker_process_name,
    t.max_retries, t.task_options, t.good_until, t.queue_name,
    clock_timestamp() AS db_now
FROM horsies_tasks t
WHERE t.id = $1 AND t.status = 'RUNNING'
  AND (
      t.finalizing_at IS NULL
      OR t.finalizing_at < NOW() - $3 * INTERVAL '1 second'
  )
  AND NOT EXISTS (
    SELECT 1 FROM horsies_heartbeats
    WHERE task_id = t.id AND role = 'runner'
      AND sent_at > NOW() - $2 * INTERVAL '1 second'
  )
FOR UPDATE OF t";

use crate::broker::UPSERT_TASK_ATTEMPT_SQL;

/// SQL: Requeue a stale RUNNING task for retry (clears all claim fields).
const SCHEDULE_STALE_TASK_RETRY_SQL: &str = "\
UPDATE horsies_tasks
SET status = 'PENDING',
    retry_count = $2,
    next_retry_at = $3,
    enqueued_at = $3,
    error_code = NULL,
    claimed = FALSE,
    claimed_at = NULL,
    claimed_by_worker_id = NULL,
    claim_expires_at = NULL,
    finalizing_at = NULL,
    finalizing_by_worker_id = NULL,
    updated_at = NOW()
WHERE id = $1
  AND status = 'RUNNING'
  AND (good_until IS NULL OR $3 < good_until)";

// The terminal stale-failure statement is `horsies_fail_stale_task`
// (broker/terminalization.rs): the function re-captures heartbeat/finalizing
// state under its own lock and judges staleness authoritatively, so the
// Phase 1 scan and the locked re-check here are advisory — a heartbeat
// landing between scan and call refuses with STALENESS evidence instead of
// failing a live task.

/// Row from Phase 1 scan — just the task ID.
#[derive(Debug, FromRow)]
struct StaleTaskId {
    id: Uuid,
}

#[cfg(test)]
mod p7_maintenance_tests {
    use super::*;
    use crate::broker::terminalization::terminalize;
    use crate::core::history::commands::{CreateDailyHistoryLeaf, LeafBounds, LeafRef};
    use crate::core::history::ddl::classes::{
        finite_class_parent_name, register_finite_retention_class,
    };
    use crate::core::history::ddl::runtime_names::daily_leaf_name;
    use crate::core::history::heartbeats::partitioning::{
        create_hourly_heartbeat_leaf, hourly_leaf_ref, CreateHourlyHeartbeatLeaf,
    };
    use crate::core::history::maintenance::coverage::{
        ensure_startup_coverage, StartupCoverageOutcome,
    };
    use crate::core::history::partitions::catalog::database_now;
    use crate::core::history::partitions::manager::create_daily_leaf;
    use crate::core::history::partitions::publication::LoaderPublication;
    use crate::core::lifecycle::{PriorLockedRead, TerminalizationCommand, TerminalizationOutcome};
    use chrono::Timelike;
    use serial_test::serial;

    async fn seed_terminal_workflow_with_pending(pool: &PgPool, workflow_id: Uuid, task_id: Uuid) {
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
             ) VALUES ($1, 'p7_retention', 'RUNNING', 'fail', NULL,
                       'test.p7.retention.v1', 0, $1,
                       NOW(), NOW(), NOW(), NOW())",
        )
        .bind(workflow_id)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs, status,
                sent_at, enqueued_at, started_at, claimed, claimed_at,
                claimed_by_worker_id, is_workflow_task, retry_count, max_retries,
                enqueue_sha, command_fingerprint_version, command_fingerprint,
                retention_class_key, retain_rerun_input,
                prepared_rerun_input_disposition, created_at, updated_at
             ) VALUES ($1, 'p7_retention_task', 'default', 100, '[]', '{}', 'RUNNING',
                       NOW(), NOW(), NOW(), TRUE, NOW(), 'p7-retention-worker', TRUE,
                       0, 0, $1::text, 1, $2, 'forever', FALSE,
                       'NEVER_ELIGIBLE', NOW(), NOW())",
        )
        .bind(task_id)
        .bind(vec![23_u8; 32])
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                id, workflow_id, task_index, node_id, task_name, task_args,
                task_kwargs, queue_name, priority, dependencies, allow_failed_deps,
                join_type, status, is_subworkflow, task_id, created_at
             ) VALUES ($1, $2, 0, 'root', 'p7_retention_task', '[]', '{}',
                       'default', 100, '{}', FALSE, 'all', 'RUNNING', FALSE, $3, NOW())",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .bind(task_id)
        .execute(pool)
        .await
        .unwrap();
        let result = serde_json::to_string(&crate::core::TaskResult::Ok(
            serde_json::json!({"retained": true}),
        ))
        .unwrap();
        let outcomes = terminalize(
            pool,
            &TerminalizationCommand::CompleteLockedTask {
                task_id,
                fence: PriorLockedRead {
                    worker_id: "p7-retention-worker".to_owned(),
                },
                result_json: result,
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ));
        sqlx::query(
            "UPDATE horsies_workflows
             SET status = 'COMPLETED', completed_at = NOW() - INTERVAL '48 hours',
                 created_at = NOW() - INTERVAL '48 hours',
                 updated_at = NOW() - INTERVAL '48 hours'
             WHERE id = $1",
        )
        .bind(workflow_id)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn retention_owns_only_worker_state_and_workflow_rows() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let mut coverage = pool.begin().await.unwrap();
        let outcome = ensure_startup_coverage(coverage.as_mut(), 2, 2, &[], &StagedLoaderPublisher)
            .await
            .unwrap();
        assert!(matches!(outcome, StartupCoverageOutcome::Ready(_)));
        coverage.commit().await.unwrap();

        let workflow_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        let heartbeat_task_id = Uuid::new_v4();
        let worker_id = format!("p7-retention-{}", Uuid::new_v4());
        seed_terminal_workflow_with_pending(&pool, workflow_id, task_id).await;
        sqlx::query(
            "INSERT INTO horsies_worker_states (
                worker_id, snapshot_at, hostname, pid, processes,
                max_claim_batch, max_claim_per_worker, queues,
                tasks_running, tasks_claimed, worker_started_at
             ) VALUES ($1, NOW() - INTERVAL '48 hours', 'host', 1, 1,
                       1, 1, '{default}', 0, 0, NOW() - INTERVAL '48 hours')",
        )
        .bind(&worker_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_heartbeats
                (task_id, sender_id, role, sent_at, hostname, pid)
             VALUES ($1, $2, 'runner', NOW(), 'host', 1)",
        )
        .bind(heartbeat_task_id)
        .bind(&worker_id)
        .execute(&pool)
        .await
        .unwrap();

        let mut config = RetentionConfig::default();
        config.worker_state_retention_hours = Some(1);
        config.terminal_record_retention_hours = Some(1);
        run_retention_cleanup(&pool, &config).await;

        let state: (i64, i64, i64, i64, i64) = sqlx::query_as(
            "SELECT
                (SELECT count(*) FROM horsies_worker_states WHERE worker_id = $1),
                (SELECT count(*) FROM horsies_workflows WHERE id = $2),
                (SELECT count(*) FROM horsies_workflow_phase2_pending WHERE task_id = $3),
                (SELECT count(*) FROM horsies_task_history WHERE task_id = $3),
                (SELECT count(*) FROM horsies_heartbeats WHERE task_id = $4)",
        )
        .bind(&worker_id)
        .bind(workflow_id)
        .bind(task_id)
        .bind(heartbeat_task_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            state,
            (0, 0, 0, 1, 1),
            "row retention deletes worker-state/workflow rows, workflow cascade owns pending evidence, and partitioned task/heartbeat facts remain",
        );

        sqlx::query("DELETE FROM horsies_heartbeats WHERE task_id = $1")
            .bind(heartbeat_task_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn periodic_maintenance_publishes_all_health_and_contains_coverage_failure() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let mut recovery = RecoveryConfig::default();
        recovery.auto_requeue_stale_claimed = false;
        recovery.auto_fail_stale_running = false;
        recovery.auto_terminate_orphaned_workflow_tasks = false;
        let mut retention = RetentionConfig::default();
        retention.worker_state_retention_hours = None;
        retention.terminal_record_retention_hours = None;
        retention
            .retention_classes
            .push(crate::core::config::retention::RetentionClassConfig {
                key: "unsafe-class-key".to_owned(),
                duration: chrono::Duration::days(7),
            });
        let health = new_reaper_health();
        let mut next_retention_cleanup = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut next_partition_maintenance = tokio::time::Instant::now();
        let mut orphan_state = OrphanSweepState::default();

        run_reaper_pass(
            &pool,
            &pool,
            &WorkflowSpecRegistry::new(),
            &recovery,
            &PayloadPolicy::default(),
            &retention,
            &health,
            &mut next_retention_cleanup,
            &mut next_partition_maintenance,
            &mut orphan_state,
        )
        .await;

        let snapshot = health.read().await.clone();
        assert_eq!(
            snapshot
                .partition_coverage
                .as_ref()
                .and_then(|value| value["state"].as_str()),
            Some("error"),
        );
        assert!(
            snapshot.partition_pruning.is_some(),
            "pruning still runs and publishes health after coverage fails",
        );
        assert!(
            snapshot.phase2_recovery.is_some(),
            "every pass publishes phase-2 health independently",
        );
        assert!(
            snapshot.workflow_recovery.is_some(),
            "every pass publishes non-phase2 workflow-recovery health independently",
        );
        assert!(
            next_partition_maintenance > tokio::time::Instant::now(),
            "the periodic maintenance tick advances after contained failures",
        );
    }

    #[tokio::test]
    #[serial]
    async fn periodic_maintenance_runs_non_phase2_workflow_recovery() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, definition_key, depth,
                 root_workflow_id, sent_at, created_at, started_at, updated_at
             ) VALUES ($1, 'p7_periodic_orphan', 'RUNNING', 'fail', $2, 0,
                       $1, NOW(), NOW(), NOW(), NOW())",
        )
        .bind(workflow_id)
        .bind(format!("test.p7.periodic-orphan.{workflow_id}"))
        .execute(&pool)
        .await
        .unwrap();
        let mut recovery = RecoveryConfig::default();
        recovery.auto_requeue_stale_claimed = false;
        recovery.auto_fail_stale_running = false;
        recovery.auto_terminate_orphaned_workflow_tasks = false;
        let retention = RetentionConfig::default();
        let health = new_reaper_health();
        let mut next_retention_cleanup = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut next_partition_maintenance = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut orphan_state = OrphanSweepState::default();

        run_reaper_pass(
            &pool,
            &pool,
            &WorkflowSpecRegistry::new(),
            &recovery,
            &PayloadPolicy::default(),
            &retention,
            &health,
            &mut next_retention_cleanup,
            &mut next_partition_maintenance,
            &mut orphan_state,
        )
        .await;

        let status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(workflow_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "FAILED");
        let snapshot = health.read().await.clone();
        let report = snapshot.workflow_recovery.unwrap();
        assert!(report["case4_orphaned_failed"].as_u64().unwrap() >= 1);
        assert_eq!(report["errors"], 0);

        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn periodic_maintenance_creates_ahead_prunes_both_partition_families_and_contains_a_blocker(
    ) {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let mut recovery = RecoveryConfig::default();
        recovery.auto_requeue_stale_claimed = false;
        recovery.auto_fail_stale_running = false;
        recovery.auto_terminate_orphaned_workflow_tasks = false;
        let mut retention = RetentionConfig::default();
        retention.worker_state_retention_hours = Some(1);
        retention.terminal_record_retention_hours = Some(1);
        let blocked_class = "p7_pass_blocked_1d";
        let valid_class = "p7_pass_valid_1d";

        // Settle all pre-existing classes at the same horizons so the pass's
        // creation counts are attributable to the two classes introduced here.
        let mut baseline = pool.begin().await.unwrap();
        let baseline_outcome = ensure_startup_coverage(
            baseline.as_mut(),
            retention.history_leaf_horizon_days,
            retention.heartbeat_leaf_horizon_hours,
            &[],
            &StagedLoaderPublisher,
        )
        .await
        .unwrap();
        assert!(matches!(baseline_outcome, StartupCoverageOutcome::Ready(_)));
        baseline.commit().await.unwrap();

        let mut setup = pool.begin().await.unwrap();
        for class_key in [blocked_class, valid_class] {
            register_finite_retention_class(&mut setup, class_key, chrono::Duration::days(1))
                .await
                .unwrap();
        }
        let now = database_now(&mut setup).await.unwrap();
        let old_day = (now - chrono::Duration::days(6))
            .with_hour(0)
            .and_then(|value| value.with_minute(0))
            .and_then(|value| value.with_second(0))
            .and_then(|value| value.with_nanosecond(0))
            .unwrap();
        let mut history_leaves = Vec::new();
        for (offset, class_key) in [blocked_class, valid_class].into_iter().enumerate() {
            let lower = old_day + chrono::Duration::days(offset as i64);
            let parent = finite_class_parent_name(class_key).unwrap();
            let leaf = LeafRef::new(
                daily_leaf_name(&parent, lower).unwrap(),
                class_key,
                LeafBounds::new(lower, lower + chrono::Duration::days(1)).unwrap(),
            )
            .unwrap();
            create_daily_leaf(
                &mut setup,
                &CreateDailyHistoryLeaf::new(leaf.clone()).unwrap(),
                &StagedLoaderPublisher,
            )
            .await
            .unwrap();
            history_leaves.push(leaf);
        }
        let old_hour = (now - chrono::Duration::hours(12))
            .with_minute(0)
            .and_then(|value| value.with_second(0))
            .and_then(|value| value.with_nanosecond(0))
            .unwrap();
        let heartbeat_leaf = hourly_leaf_ref(old_hour).unwrap();
        create_hourly_heartbeat_leaf(
            &mut setup,
            &CreateHourlyHeartbeatLeaf::new(heartbeat_leaf.clone()).unwrap(),
        )
        .await
        .unwrap();

        let workflow_id = Uuid::new_v4();
        let node_id = Uuid::new_v4();
        let blocker_task_id = Uuid::new_v4();
        sqlx::query("INSERT INTO horsies_workflows (id, name) VALUES ($1, 'p7 pass blocker')")
            .bind(workflow_id)
            .execute(&mut *setup)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (id, workflow_id, task_index, task_name)
             VALUES ($1, $2, 0, 'p7 pass blocker')",
        )
        .bind(node_id)
        .bind(workflow_id)
        .execute(&mut *setup)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflow_phase2_pending (
                 task_id, workflow_id, workflow_node_row_id, terminal_status,
                 terminal_at, terminalization_kind, recovery_source,
                 history_class, history_anchor, history_schema_version,
                 result_digest, phase2_generation, created_at, attempt_count
             ) VALUES ($1, $2, $3, 'COMPLETED', $4, 'COMPLETE_LOCKED',
                       'HISTORY', $5, $4, 1, $6, $7,
                       NOW() - INTERVAL '2 hours', $8)",
        )
        .bind(blocker_task_id)
        .bind(workflow_id)
        .bind(node_id)
        .bind(history_leaves[0].bounds().lower() + chrono::Duration::hours(1))
        .bind(blocked_class)
        .bind(vec![31_u8; 32])
        .bind(Uuid::new_v4())
        .bind(recovery.phase2_quarantine_after_attempts as i32)
        .execute(&mut *setup)
        .await
        .unwrap();

        // Statement-level sentinels discriminate the removed row-retention
        // SQL from partition pruning, including an empty DELETE statement.
        for statement in [
            "CREATE TABLE p7_reaper_row_delete_audit (relation_name text NOT NULL)",
            "CREATE OR REPLACE FUNCTION p7_reaper_record_row_delete() RETURNS trigger
             LANGUAGE plpgsql AS $body$
             BEGIN
                 INSERT INTO p7_reaper_row_delete_audit VALUES (TG_TABLE_NAME);
                 RETURN NULL;
             END
             $body$",
            "CREATE TRIGGER p7_reaper_task_row_delete
                 AFTER DELETE ON horsies_tasks FOR EACH STATEMENT
                 EXECUTE FUNCTION p7_reaper_record_row_delete()",
            "CREATE TRIGGER p7_reaper_heartbeat_row_delete
                 AFTER DELETE ON horsies_heartbeats FOR EACH STATEMENT
                 EXECUTE FUNCTION p7_reaper_record_row_delete()",
        ] {
            sqlx::query(statement).execute(&mut *setup).await.unwrap();
        }
        setup.commit().await.unwrap();

        retention.retention_classes.extend([
            crate::core::config::retention::RetentionClassConfig {
                key: blocked_class.to_owned(),
                duration: chrono::Duration::days(1),
            },
            crate::core::config::retention::RetentionClassConfig {
                key: valid_class.to_owned(),
                duration: chrono::Duration::days(1),
            },
        ]);
        let health = new_reaper_health();
        let mut next_retention_cleanup = tokio::time::Instant::now();
        let mut next_partition_maintenance = tokio::time::Instant::now();
        let mut orphan_state = OrphanSweepState::default();
        run_reaper_pass(
            &pool,
            &pool,
            &WorkflowSpecRegistry::new(),
            &recovery,
            &PayloadPolicy::default(),
            &retention,
            &health,
            &mut next_retention_cleanup,
            &mut next_partition_maintenance,
            &mut orphan_state,
        )
        .await;

        let snapshot = health.read().await.clone();
        let coverage = snapshot.partition_coverage.unwrap();
        assert_eq!(coverage["state"], "ensured");
        assert_eq!(coverage["created_history_leaves"], 8);
        assert_eq!(coverage["created_heartbeat_leaves"], 0);
        assert_eq!(coverage["heartbeat_covered_now"], true);
        let pruning = snapshot.partition_pruning.unwrap();
        assert_eq!(pruning["finalized"], 0);
        assert_eq!(pruning["detached"], 2);
        assert_eq!(pruning["dropped"], 2);
        assert_eq!(pruning["refusals"].as_array().unwrap().len(), 1);
        assert!(pruning["refusals"][0]
            .as_str()
            .unwrap()
            .contains(history_leaves[0].leaf_name()));
        assert_eq!(pruning["errors"].as_array().unwrap().len(), 0);
        let phase2 = snapshot.phase2_recovery.unwrap();
        assert_eq!(phase2["considered"], 0);
        assert_eq!(phase2["applied"], 0);
        assert_eq!(phase2["retained"], 0);
        assert_eq!(phase2["failed"], 0);
        assert_eq!(phase2["over_attempt_bound"], 1);

        let physical: (bool, bool, bool, i64) = sqlx::query_as(
            "SELECT
                 to_regclass($1) IS NOT NULL,
                 to_regclass($2) IS NULL,
                 to_regclass($3) IS NULL,
                 (SELECT count(*) FROM p7_reaper_row_delete_audit)",
        )
        .bind(history_leaves[0].leaf_name())
        .bind(history_leaves[1].leaf_name())
        .bind(heartbeat_leaf.leaf_name())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(physical, (true, true, true, 0));
        for class_key in [blocked_class, valid_class] {
            let parent = finite_class_parent_name(class_key).unwrap();
            for offset in 0..=retention.history_leaf_horizon_days {
                let lower = now
                    .with_hour(0)
                    .and_then(|value| value.with_minute(0))
                    .and_then(|value| value.with_second(0))
                    .and_then(|value| value.with_nanosecond(0))
                    .unwrap()
                    + chrono::Duration::days(i64::from(offset));
                let leaf_name = daily_leaf_name(&parent, lower).unwrap();
                let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
                    .bind(leaf_name)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                assert!(exists, "periodic pass omitted create-ahead for {class_key}");
            }
        }

        let mut cleanup = pool.begin().await.unwrap();
        sqlx::query("DELETE FROM horsies_workflow_phase2_pending WHERE task_id = $1")
            .bind(blocker_task_id)
            .execute(&mut *cleanup)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(workflow_id)
            .execute(&mut *cleanup)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&mut *cleanup)
            .await
            .unwrap();
        sqlx::query("DROP TABLE p7_reaper_row_delete_audit CASCADE")
            .execute(&mut *cleanup)
            .await
            .unwrap();
        sqlx::query("DROP FUNCTION p7_reaper_record_row_delete() CASCADE")
            .execute(&mut *cleanup)
            .await
            .unwrap();
        for class_key in [blocked_class, valid_class] {
            let parent = finite_class_parent_name(class_key).unwrap();
            sqlx::query(&format!("DROP TABLE {parent} CASCADE"))
                .execute(&mut *cleanup)
                .await
                .unwrap();
            sqlx::query("DELETE FROM horsies_task_history_leaf_catalog WHERE class_key = $1")
                .bind(class_key)
                .execute(&mut *cleanup)
                .await
                .unwrap();
            sqlx::query("DELETE FROM horsies_retention_classes WHERE class_key = $1")
                .bind(class_key)
                .execute(&mut *cleanup)
                .await
                .unwrap();
        }
        StagedLoaderPublisher.republish(&mut cleanup).await.unwrap();
        cleanup.commit().await.unwrap();
    }
}

/// Row from Phase 2 per-task FOR UPDATE — full context for retry eligibility.
#[derive(Debug, FromRow)]
struct StaleTaskContext {
    retry_count: i32,
    worker_pid: Option<i32>,
    worker_hostname: Option<String>,
    claimed_by_worker_id: Option<String>,
    started_at: Option<DateTime<Utc>>,
    worker_process_name: Option<String>,
    max_retries: i32,
    task_options: Option<String>,
    good_until: Option<DateTime<Utc>>,
    queue_name: String,
    db_now: DateTime<Utc>,
}

/// SQL: Requeue stale CLAIMED tasks back to PENDING.
///
/// A task is stale if:
/// - it has a lease (`claim_expires_at`) and the lease has expired, OR
/// - it has no lease and `claimed_at` is older than the threshold.
///
/// We intentionally ignore claimer heartbeats for CLAIMED tasks without a lease.
/// Otherwise a worker can keep a task CLAIMED forever even if it never starts.
const REQUEUE_STALE_CLAIMED_SQL: &str = "\
WITH stale AS (
    SELECT t.id
    FROM horsies_tasks t
    WHERE t.status = 'CLAIMED'
      AND (
        (t.claim_expires_at IS NOT NULL AND t.claim_expires_at < NOW())
        OR (t.claim_expires_at IS NULL
            AND t.claimed_at < NOW() - $1 * INTERVAL '1 second')
      )
    FOR UPDATE OF t SKIP LOCKED
)
UPDATE horsies_tasks
SET status = 'PENDING',
    claimed = FALSE,
    claimed_at = NULL,
    claimed_by_worker_id = NULL,
    claim_expires_at = NULL,
    updated_at = NOW()
FROM stale
WHERE horsies_tasks.id = stale.id";

// ---------------------------------------------------------------------------
// Retention cleanup SQL
// ---------------------------------------------------------------------------

const DELETE_EXPIRED_WORKER_STATES_SQL: &str = "\
DELETE FROM horsies_worker_states
WHERE id IN (
    SELECT id FROM horsies_worker_states
    WHERE snapshot_at < NOW() - CAST($1 || ' hours' AS INTERVAL)
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)";

// One workflow-batched statement deletes a workflow and its node rows together
// (parity with horsies PR #216; replaces the former workflow_tasks + workflows
// statement pair, which re-evaluated the live-task guard per batch as a
// per-candidate "NOT terminal" probe — an inequality no index serves — and
// re-waded through drained node-less workflows on every later batch).
//
// The live-task guard retains a terminal+expired workflow (and its linkage)
// until EVERY backing horsies_tasks row is terminal. Defense-in-depth (parity
// with horsies PR #143): the invariant "terminal workflow ⇒ all backing tasks
// terminal" holds today (cancel cancels all linked task rows; complete/fail
// require all workflow_tasks terminal, which trails their task rows), so the
// guard never fires now — but it ensures a future change can never strand a
// live task row by deleting its workflow_task linkage. The `live` CTE computes
// it ONCE per statement from the non-terminal side: 'CLAIMED', 'PENDING',
// 'RUNNING' is the complement of the terminal set (together they cover every
// task status; keep both lists in sync), and in-flight work is small by
// definition, so the probe rides ix_horsies_tasks_status.
//
// The workflow status list + COALESCE expression must stay structurally
// aligned with idx_horsies_workflows_retention and
// stx_horsies_workflows_retention (migration 0028): the partial index serves
// the scan only while the status literals imply its predicate, and the
// statistics object supplies the whole-table estimate only while the parsed
// expression matches.
//
// `budgeted` keeps candidates while their running node total fits $2 (the
// knob keeps its rows-per-statement meaning), always keeping the first
// candidate so a workflow larger than the whole budget drains alone instead
// of starving. Node rows are purged set-wise in `purged_nodes` (the
// task_attempts pattern); the workflow_id FK cascade remains the correctness
// net for non-retention deletes.
//
// The top-level DELETE's rowcount counts WORKFLOWS, which under the node
// budget is routinely smaller than $2 while backlog remains — the reaper
// therefore drives this statement with DrainedWhen::EmptyBatch rather than
// the short-batch heuristic the row-batched statements use.
const DELETE_EXPIRED_WORKFLOWS_SQL: &str = "\
WITH live AS MATERIALIZED (
    SELECT DISTINCT wt.workflow_id
    FROM horsies_tasks t
    JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
    WHERE t.status IN ('CLAIMED', 'PENDING', 'RUNNING')
),
doomed AS (
    SELECT w.id,
           (SELECT count(*)
            FROM horsies_workflow_tasks wt
            WHERE wt.workflow_id = w.id) AS node_count
    FROM horsies_workflows w
    WHERE w.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
      AND COALESCE(w.completed_at, w.updated_at, w.created_at) < NOW() - CAST($1 || ' hours' AS INTERVAL)
      AND NOT EXISTS (
          SELECT 1 FROM live WHERE live.workflow_id = w.id
      )
    LIMIT $2
    FOR UPDATE SKIP LOCKED
),
budgeted AS (
    SELECT id
    FROM (
        SELECT id,
               SUM(node_count) OVER (ORDER BY id) AS nodes_running,
               ROW_NUMBER() OVER (ORDER BY id) AS position
        FROM doomed
    ) ranked
    WHERE nodes_running <= $2 OR position = 1
),
purged_nodes AS (
    DELETE FROM horsies_workflow_tasks
    WHERE workflow_id IN (SELECT id FROM budgeted)
)
DELETE FROM horsies_workflows
WHERE id IN (SELECT id FROM budgeted)";

/// Max stale-RUNNING candidates a single reaper pass processes. Phase 2 handles
/// each in its own transaction under the cluster-wide reaper gate, so this bounds
/// how long one pass holds the gate; successive passes drain any larger backlog
/// (P8).
const STALE_RUNNING_SCAN_LIMIT: i64 = 1_000;

/// Wall-clock budget for one retention pass across both retained-row statements. A
/// backlog that does not drain within the budget resumes on the next pass;
/// every statement still runs at least one batch per pass so a deep backlog
/// in an earlier table cannot starve the later ones indefinitely.
const RETENTION_PASS_TIME_BUDGET: Duration = Duration::from_secs(60);

/// Reaper-owned health attached to worker-state snapshots.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ReaperHealthSnapshot {
    pub workflow_recovery: Option<serde_json::Value>,
    pub partition_coverage: Option<serde_json::Value>,
    pub partition_pruning: Option<serde_json::Value>,
    pub phase2_recovery: Option<serde_json::Value>,
}

pub type ReaperHealth = Arc<RwLock<ReaperHealthSnapshot>>;

pub fn new_reaper_health() -> ReaperHealth {
    Arc::new(RwLock::new(ReaperHealthSnapshot::default()))
}

/// Spawn the reaper loop for stale task recovery.
///
/// Periodically checks for stale RUNNING and CLAIMED tasks, marking them
/// as FAILED or requeuing them respectively.
#[allow(clippy::needless_pass_by_value)]
pub fn spawn_reaper(
    runtime_pool: PgPool,
    maintenance_pool: PgPool,
    registry: Arc<WorkflowSpecRegistry>,
    config: RecoveryConfig,
    payload: PayloadPolicy,
    retention: RetentionConfig,
    health: ReaperHealth,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let check_interval = Duration::from_millis(config.check_interval_ms);
        let mut next_retention_cleanup =
            tokio::time::Instant::now() + Duration::from_secs(retention.retention_sweep_interval_s);
        let mut next_partition_maintenance = tokio::time::Instant::now();
        let mut orphan_state = OrphanSweepState::default();

        tracing::info!(
            auto_requeue_claimed = config.auto_requeue_stale_claimed,
            auto_fail_running = config.auto_fail_stale_running,
            check_interval_ms = config.check_interval_ms,
            "reaper started",
        );

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(check_interval) => {
                    // Cluster-wide gate: only one worker runs a pass per interval.
                    // The passes are safe to run concurrently (SKIP LOCKED), but
                    // redundant across a cluster; the gate elides the duplicate work.
                    match acquire_gate(&runtime_pool, advisory_key_reaper()).await {
                        GatePass::Skip => {
                            tracing::debug!("reaper pass skipped: another worker holds the gate");
                        }
                        GatePass::Ungated => {
                            run_reaper_pass(&runtime_pool, &maintenance_pool, &registry, &config, &payload, &retention, &health, &mut next_retention_cleanup, &mut next_partition_maintenance, &mut orphan_state).await;
                        }
                        GatePass::Held(tx) => {
                            run_reaper_pass(&runtime_pool, &maintenance_pool, &registry, &config, &payload, &retention, &health, &mut next_retention_cleanup, &mut next_partition_maintenance, &mut orphan_state).await;
                            release_gate(tx).await;
                        }
                    }
                }
            }
        }
    })
}

/// Outcome of trying to acquire a cluster-wide periodic-pass gate.
enum GatePass {
    /// Gate held by an otherwise-idle transaction; commit after the pass to
    /// release the xact-scoped lock.
    Held(sqlx::Transaction<'static, sqlx::Postgres>),
    /// Another worker holds the gate this interval; skip the pass.
    Skip,
    /// Gating disabled (single-connection pool): run the pass ungated.
    Ungated,
}

/// Derive a fixed 64-bit advisory key from a label (first 8 bytes of SHA-256).
fn advisory_key_from(label: &[u8]) -> i64 {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(label);
    let digest = hasher.finalize();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    i64::from_be_bytes(bytes)
}

/// Fixed advisory key for the cluster-wide reaper gate (distinct from the claim
/// key). Parity with horsies PR #101 7a3eb0d6.
fn advisory_key_reaper() -> i64 {
    advisory_key_from(b"horsies:reaper:v1")
}

/// Try to acquire a periodic-pass gate as a transaction-scoped advisory lock on
/// `key`, held by an otherwise-idle transaction for the duration of the pass.
///
/// Xact scoping keeps acquire and release on one server backend under
/// PgBouncer transaction pooling (a session-level lock would not survive
/// between round-trips there), and rollback-on-drop releases the lock on any
/// error path.
async fn acquire_gate(pool: &PgPool, key: i64) -> GatePass {
    // The gate holds one connection while the pass body needs another; on a
    // single-connection pool that would deadlock, so run ungated. SKIP LOCKED
    // keeps an ungated pass correct (just possibly duplicated). Parity with
    // horsies PR #101 4a7344ec.
    if pool.options().get_max_connections() < 2 {
        return GatePass::Ungated;
    }
    let mut tx = match pool.begin().await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(error = %e, "periodic-pass gate connection unavailable; skipping interval");
            return GatePass::Skip;
        }
    };
    let acquired: bool = match sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
        .bind(key)
        .fetch_one(tx.as_mut())
        .await
    {
        Ok(acquired) => acquired,
        Err(error) => {
            tracing::warn!(%error, "periodic-pass advisory gate failed; skipping interval");
            return GatePass::Skip;
        }
    };
    if acquired {
        GatePass::Held(tx)
    } else {
        GatePass::Skip
    }
}

/// Release a periodic-pass gate by committing its holder transaction (the
/// xact-scoped lock frees on commit; on error it frees via rollback-on-drop).
async fn release_gate(tx: sqlx::Transaction<'static, sqlx::Postgres>) {
    if let Err(e) = tx.commit().await {
        tracing::warn!(error = %e, "periodic-pass gate commit failed; lock frees when the connection closes");
    }
}

/// Run one reaper pass: stale-RUNNING recovery, PENDING expiry, stale-CLAIMED
/// requeue, and periodic retention cleanup.
async fn run_reaper_pass(
    runtime_pool: &PgPool,
    maintenance_pool: &PgPool,
    registry: &WorkflowSpecRegistry,
    config: &RecoveryConfig,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
    health: &ReaperHealth,
    next_retention_cleanup: &mut tokio::time::Instant,
    next_partition_maintenance: &mut tokio::time::Instant,
    orphan_state: &mut OrphanSweepState,
) {
    if config.auto_fail_stale_running {
        let threshold_secs = config.running_stale_threshold_ms as f64 / 1000.0;
        let finalizing_threshold_secs = config.finalizing_stale_threshold_ms as f64 / 1000.0;
        match mark_stale_running_as_failed(
            runtime_pool,
            threshold_secs,
            finalizing_threshold_secs,
            STALE_RUNNING_SCAN_LIMIT,
        )
        .await
        {
            Ok(count) if count > 0 => {
                tracing::warn!(count, "reaper marked stale RUNNING tasks as FAILED");
            }
            Err(e) => {
                tracing::error!(error = %e, "reaper: failed to mark stale running tasks");
            }
            _ => {}
        }
    }

    // Expire unclaimed PENDING tasks whose good_until has passed.
    match expire_pending_tasks(runtime_pool).await {
        Ok(count) if count > 0 => {
            tracing::info!(count, "reaper expired unclaimed PENDING tasks");
        }
        Err(e) => {
            tracing::warn!(error = %e, "reaper: failed to expire pending tasks");
        }
        _ => {}
    }

    if config.auto_requeue_stale_claimed {
        let threshold_secs = config.claimed_stale_threshold_ms as f64 / 1000.0;
        match requeue_stale_claimed(runtime_pool, threshold_secs).await {
            Ok(count) if count > 0 => {
                tracing::info!(count, "reaper requeued stale CLAIMED tasks");
            }
            Err(e) => {
                tracing::error!(error = %e, "reaper: failed to requeue stale claimed tasks");
            }
            _ => {}
        }
    }

    // Cancel orphaned workflow tasks (no live workflow_task linkage). These
    // cannot reach RUNNING, so the requeue above skips them and they would
    // otherwise stay CLAIMED forever; cancelling frees claim budget and lets
    // retention sweep them.
    if config.auto_terminate_orphaned_workflow_tasks && !orphan_state.disabled {
        match terminate_orphaned_workflow_tasks(runtime_pool).await {
            Ok(count) => {
                orphan_state.permanent_failures = 0;
                if count > 0 {
                    tracing::warn!(
                        count,
                        "reaper cancelled orphaned workflow task(s) (no live \
                         workflow_task linkage)",
                    );
                }
            }
            Err(e) if e.is_retryable() => {
                orphan_state.permanent_failures = 0;
                tracing::warn!(
                    error = %e,
                    "reaper orphan sweep transient failure (will retry next cycle)",
                );
            }
            Err(e) => {
                orphan_state.permanent_failures += 1;
                if orphan_state.permanent_failures >= ORPHAN_SWEEP_MAX_PERMANENT_FAILURES {
                    orphan_state.disabled = true;
                    tracing::error!(
                        error = %e,
                        failures = orphan_state.permanent_failures,
                        "reaper orphan sweep disabled after consecutive permanent \
                         failures; requires deploy or manual intervention",
                    );
                } else {
                    tracing::error!(
                        error = %e,
                        failures = orphan_state.permanent_failures,
                        max = ORPHAN_SWEEP_MAX_PERMANENT_FAILURES,
                        "reaper orphan sweep permanent failure",
                    );
                }
            }
        }
    }

    // Exact outbox recovery. Each candidate owns its transaction; retaining
    // dispositions remain visible and count toward bounded quarantine.
    let workflow_recovery = crate::workflow_engine::recovery::recover_stuck_workflows(
        runtime_pool,
        registry,
        config.crashed_worker_recovery_grace_ms,
        payload,
        retention,
    )
    .await;
    health.write().await.workflow_recovery = Some(match workflow_recovery {
        Ok(report) => serde_json::to_value(report).unwrap_or_else(
            |error| serde_json::json!({"state": "error", "error": error.to_string()}),
        ),
        Err(error) => {
            tracing::error!(%error, "workflow recovery pass failed");
            serde_json::json!({"state": "error", "error": error.to_string()})
        }
    });

    let phase2 = crate::workflow_engine::phase2_recovery::drive_phase2_recovery(
        runtime_pool,
        registry,
        config.crashed_worker_recovery_grace_ms,
        crate::workflow_engine::recovery::GLOBAL_SCAN_ROW_CAP,
        config.phase2_quarantine_after_attempts,
        payload,
        retention,
    )
    .await;
    health.write().await.phase2_recovery = Some(match phase2 {
        Ok(summary) => {
            if summary.applied > 0 || summary.retained > 0 || summary.failed > 0 {
                tracing::info!(?summary, "phase-2 recovery pass completed");
            }
            serde_json::to_value(summary).unwrap_or_else(
                |error| serde_json::json!({"state": "error", "error": error.to_string()}),
            )
        }
        Err(error) => {
            tracing::error!(%error, "phase-2 recovery pass failed");
            serde_json::json!({"state": "error", "error": error.to_string()})
        }
    });

    // Retention cleanup (runs every retention_sweep_interval_s).
    if tokio::time::Instant::now() >= *next_retention_cleanup {
        run_retention_cleanup(runtime_pool, retention).await;
        *next_retention_cleanup =
            tokio::time::Instant::now() + Duration::from_secs(retention.retention_sweep_interval_s);
    }

    if let Some(age) = retention.paused_workflow_auto_cancel_after {
        match crate::workflow_engine::lifecycle::expire_paused_workflows(runtime_pool, age, 50)
            .await
        {
            Ok(count) if count > 0 => {
                tracing::info!(
                    count,
                    "expired paused workflows past the declared age policy"
                );
            }
            Err(error) => {
                tracing::error!(%error, "paused-workflow expiry sweep failed");
            }
            _ => {}
        }
    }

    if tokio::time::Instant::now() >= *next_partition_maintenance {
        let declared: Vec<DeclaredRetentionClass> = retention
            .registrable_classes()
            .into_iter()
            .map(|class| DeclaredRetentionClass {
                class_key: class.key,
                duration: class.duration,
            })
            .collect();
        let coverage_health = match ensure_partition_coverage_in_pool(
            maintenance_pool,
            retention.history_leaf_horizon_days,
            retention.heartbeat_leaf_horizon_hours,
            &declared,
            &StagedLoaderPublisher,
        )
        .await
        {
            Ok(outcome) => {
                let encoded = match &outcome {
                    CoverageOutcome::Ensured(ensured) => serde_json::json!({
                        "state": "ensured",
                        "created_history_leaves": ensured.created_history_leaves,
                        "created_heartbeat_leaves": ensured.created_heartbeat_leaves,
                        "republished": ensured.republished,
                        "heartbeat_covered_now": ensured.heartbeat_covered_now,
                        "history_covered_through": ensured.history_covered_through,
                        "heartbeats_covered_through": ensured.heartbeats_covered_through,
                        "absent_leaves": ensured.absent_leaves,
                    }),
                    CoverageOutcome::Failed(failed) => serde_json::json!({
                        "state": "failed",
                        "stage": failed.stage,
                        "class_key": failed.class_key,
                        "refusal": failed.refusal,
                        "heartbeat_covered_now": failed.heartbeat_covered_now,
                        "absent_leaves": failed.absent_leaves,
                    }),
                };
                Some(encoded)
            }
            Err(error) => {
                tracing::error!(%error, "partition coverage ensure failed");
                Some(serde_json::json!({"state": "error", "error": error.to_string()}))
            }
        };
        health.write().await.partition_coverage = coverage_health;

        // Coverage and pruning are independently contained: either half runs
        // and publishes health even when the other refuses.
        let prune = prune_expired_partitions(maintenance_pool, &StagedLoaderPublisher).await;
        health.write().await.partition_pruning = Some(serde_json::json!({
            "finalized": prune.finalized_leaves.len(),
            "detached": prune.detached_count(),
            "dropped": prune.dropped_count(),
            "refusals": prune.refusals,
            "errors": prune.errors,
        }));
        *next_partition_maintenance = tokio::time::Instant::now()
            + Duration::from_secs(retention.partition_maintenance_interval_s);
    }
}

/// Recover stale RUNNING tasks: retry if eligible, otherwise mark FAILED.
///
/// Two-phase approach matching Python's `mark_stale_tasks_as_failed`:
/// - Phase 1 (scan): Find stale task IDs without holding row locks.
/// - Phase 2 (per-task): For each candidate, re-acquire with SELECT FOR UPDATE.
///   If the task is no longer RUNNING (worker finalized it), skip.
///   If retry-eligible, requeue to PENDING. Otherwise, mark FAILED with
///   a structured WORKER_CRASHED result.
///
/// Each task commits independently (partial progress is durable).
/// Returns the number of tasks processed (retried or failed).
pub async fn mark_stale_running_as_failed(
    pool: &PgPool,
    threshold_secs: f64,
    finalizing_threshold_secs: f64,
    scan_limit: i64,
) -> Result<u64, crate::broker::BrokerError> {
    // Phase 1: Scan for stale task IDs (no row locks). A task that is actively
    // finalizing (finalizing_at set within finalizing_threshold_secs) is skipped.
    // Bounded by `scan_limit`: Phase 2 processes candidates serially, one
    // transaction each, while the cluster-wide reaper gate is held — an unbounded
    // mass-stale event (a crashed fleet) would make one worker process the whole
    // backlog under the gate while others skip their passes. Successive passes
    // drain the remainder (P8).
    let stale_ids: Vec<StaleTaskId> = sqlx::query_as(FIND_STALE_RUNNING_IDS_SQL)
        .bind(threshold_secs)
        .bind(finalizing_threshold_secs)
        .bind(scan_limit)
        .fetch_all(pool)
        .await?;

    if stale_ids.is_empty() {
        return Ok(0);
    }

    let threshold_ms = (threshold_secs * 1000.0) as u64;
    let error_code_str = OperationalErrorCode::WorkerCrashed.to_string();
    let mut count: u64 = 0;

    // Phase 2: Process each task independently.
    for stale in &stale_ids {
        let result = process_single_stale_task(
            pool,
            stale.id,
            threshold_secs,
            finalizing_threshold_secs,
            threshold_ms,
            &error_code_str,
        )
        .await;

        match result {
            Ok(true) => count += 1,
            Ok(false) => {
                // Task no longer RUNNING — worker finalized between scan and lock.
                tracing::debug!(task_id = %stale.id, "stale task already finalized, skipping");
            }
            Err(e) => {
                tracing::error!(task_id = %stale.id, error = %e, "failed to process stale task");
            }
        }
    }

    Ok(count)
}

/// Process a single stale task: retry or fail.
/// Returns `Ok(true)` if processed, `Ok(false)` if skipped (no longer RUNNING).
async fn process_single_stale_task(
    pool: &PgPool,
    task_id: Uuid,
    threshold_secs: f64,
    finalizing_threshold_secs: f64,
    threshold_ms: u64,
    error_code_str: &str,
) -> Result<bool, crate::broker::BrokerError> {
    let mut tx = pool
        .begin()
        .await
        .map_err(crate::broker::BrokerError::Database)?;

    // Re-acquire row with full context. Returns None if the task is no longer
    // RUNNING, if a fresh heartbeat arrived after the Phase 1 scan, or if the
    // task is actively finalizing within finalizing_threshold_secs.
    let ctx: Option<StaleTaskContext> = sqlx::query_as(SELECT_STALE_TASK_FOR_UPDATE_SQL)
        .bind(task_id)
        .bind(threshold_secs)
        .bind(finalizing_threshold_secs)
        .fetch_optional(&mut *tx)
        .await?;

    let Some(row) = ctx else {
        // Task already finalized by worker — skip.
        tx.rollback().await?;
        return Ok(false);
    };

    let detected_at = row.db_now;
    let attempt_num = row.retry_count + 1;
    let attempt_started = row.started_at.unwrap_or(detected_at);

    let failed_reason = format!(
        "Worker process crashed (no runner heartbeat for {}ms = {:.1}s)",
        threshold_ms, threshold_secs,
    );

    // Check retry eligibility using fresh DB timestamp.
    let eligible = check_retry_eligibility(
        row.retry_count,
        row.max_retries,
        row.task_options.as_deref(),
        error_code_str,
        row.good_until,
        detected_at,
    );

    if eligible {
        // Retry path: attempt requeue, fall through to fail if good_until blocks.
        let new_count = row.retry_count + 1;
        let delay = calculate_retry_delay(new_count as u32, row.task_options.as_deref());
        let next_retry_at = detected_at + chrono::Duration::milliseconds((delay * 1000.0) as i64);

        let schedule_result = sqlx::query(SCHEDULE_STALE_TASK_RETRY_SQL)
            .bind(task_id)
            .bind(new_count)
            .bind(next_retry_at)
            .execute(&mut *tx)
            .await?;

        if schedule_result.rows_affected() > 0 {
            // Retry scheduled — record attempt with will_retry=true.
            sqlx::query(UPSERT_TASK_ATTEMPT_SQL)
                .bind(task_id)
                .bind(attempt_num)
                .bind("WORKER_FAILURE")
                .bind(true) // will_retry
                .bind(attempt_started)
                .bind(detected_at)
                .bind(Some(error_code_str))
                .bind(Some(&failed_reason))
                .bind(Some(&failed_reason))
                .bind(row.claimed_by_worker_id.as_deref())
                .bind(row.worker_hostname.as_deref())
                .bind(row.worker_pid)
                .bind(row.worker_process_name.as_deref())
                .execute(&mut *tx)
                .await?;

            tx.commit().await?;

            tracing::info!(
                task_id = %task_id,
                retry_count = new_count,
                next_retry_at = %next_retry_at,
                "stale RUNNING task scheduled for retry",
            );

            // Best-effort NOTIFY to wake workers.
            let _ = notify_task_queue(pool, &row.queue_name, task_id).await;

            return Ok(true);
        }

        // good_until guard blocked the retry — fall through to fail path.
        tracing::info!(
            task_id = %task_id,
            "stale task retry blocked by good_until, falling through to fail",
        );
    }

    {
        // Failure path: the operation re-judges staleness from its own
        // capture (authoritative); the attempt row is written only for a
        // transition that applied, in the same transaction.
        let task_error = TaskError {
            error_code: Some(OperationalErrorCode::WorkerCrashed.into()),
            message: Some(failed_reason.clone()),
            cause: None,
            data: Some(serde_json::json!({
                "stale_threshold_ms": threshold_ms,
                "stale_threshold_seconds": threshold_secs,
                "worker_pid": row.worker_pid,
                "worker_hostname": row.worker_hostname,
                "worker_id": row.claimed_by_worker_id,
                "started_at": row.started_at.map(|dt| dt.to_rfc3339()),
                "detected_at": detected_at.to_rfc3339(),
            })),
        };

        let task_result: TaskResult<()> = TaskResult::Err(task_error);
        let result_json = serde_json::to_string(&task_result).unwrap_or_else(|e| {
            tracing::error!(task_id = %task_id, error = %e, "failed to serialize stale task result");
            r#"{"__type":"err","value":{"message":"serialization failed"}}"#.to_owned()
        });

        // Write the attempt before the move so the terminalization function
        // archives it. A refusal rolls this row back with the rest of the
        // caller-owned transaction.
        sqlx::query(UPSERT_TASK_ATTEMPT_SQL)
            .bind(task_id)
            .bind(attempt_num)
            .bind("WORKER_FAILURE")
            .bind(false) // will_retry
            .bind(attempt_started)
            .bind(detected_at)
            .bind(Some(error_code_str))
            .bind(Some(&failed_reason))
            .bind(Some(&failed_reason))
            .bind(row.claimed_by_worker_id.as_deref())
            .bind(row.worker_hostname.as_deref())
            .bind(row.worker_pid)
            .bind(row.worker_process_name.as_deref())
            .execute(&mut *tx)
            .await
            .map_err(crate::broker::BrokerError::Database)?;

        let command = crate::core::lifecycle::TerminalizationCommand::FailStaleTask {
            task_id,
            stale_after_ms: threshold_ms as i32,
            finalizing_stale_after_ms: (finalizing_threshold_secs * 1000.0) as i32,
            result_json,
            error_code: error_code_str.to_owned(),
            failed_reason: failed_reason.clone(),
        };
        let outcomes = crate::broker::terminalization::terminalize_in_tx(&mut tx, &command).await?;

        if !matches!(
            outcomes.first(),
            Some(crate::core::lifecycle::TerminalizationOutcome::Applied { .. })
        ) {
            // The authoritative capture disagreed with the advisory scan
            // (e.g. a heartbeat landed in between): discard the speculative
            // attempt and every other write.
            tx.rollback()
                .await
                .map_err(crate::broker::BrokerError::Database)?;
            return Ok(false);
        }

        tx.commit()
            .await
            .map_err(crate::broker::BrokerError::Database)?;

        tracing::info!(task_id = %task_id, "stale RUNNING task marked FAILED");
    }

    Ok(true)
}

/// Batch size per expiry statement.
const EXPIRE_BATCH_SIZE: i32 = 500;
/// Max batches per reaper pass, bounding work and trigger-NOTIFY volume.
const EXPIRE_MAX_BATCHES_PER_PASS: u32 = 200;

/// Orphan-sweep bounds: same batch/pass convention as pending expiry.
const ORPHAN_BATCH_SIZE: i32 = 500;
const ORPHAN_MAX_BATCHES_PER_PASS: u32 = 200;
/// Consecutive permanent failures before the orphan sweep disables itself.
const ORPHAN_SWEEP_MAX_PERMANENT_FAILURES: u32 = 3;

/// Per-reaper state for the orphan sweep's disable-after-permanent-failures
/// guard: a sweep that keeps failing non-retryably (a contract breach, not a
/// network blip) stops burning every cycle on it.
#[derive(Default)]
struct OrphanSweepState {
    permanent_failures: u32,
    disabled: bool,
}

/// Cancel orphaned workflow tasks in bounded batches.
///
/// A discovery batch reports one row per transition it made, and every row
/// of a discovery batch is APPLIED — anything else is a contract breach
/// surfaced as an error by the adapter. Early-stops on a short batch.
async fn terminate_orphaned_workflow_tasks(
    pool: &PgPool,
) -> Result<u64, crate::broker::BrokerError> {
    let command = crate::core::lifecycle::TerminalizationCommand::CancelOrphanedTasks {
        batch_size: crate::core::lifecycle::BatchSize::new(ORPHAN_BATCH_SIZE)
            .expect("ORPHAN_BATCH_SIZE is positive"),
    };
    let mut total: u64 = 0;
    for _ in 0..ORPHAN_MAX_BATCHES_PER_PASS {
        let cancelled = crate::broker::terminalization::terminalize(pool, &command).await?;
        let affected = cancelled.len() as u64;
        total += affected;
        if affected < ORPHAN_BATCH_SIZE as u64 {
            break;
        }
    }
    Ok(total)
}

/// Expire unclaimed PENDING tasks whose `good_until` has passed.
///
/// Runs `horsies_expire_pending_tasks` in bounded batches (earliest
/// deadlines first, SKIP LOCKED) so a mass expiry is spread across several
/// committed statements instead of one transaction that row-locks every
/// match and flushes two trigger NOTIFYs per row in a single commit (which
/// can overflow listener queues). No attempt rows are written (the task was
/// never executed). Returns the number of expired tasks.
pub async fn expire_pending_tasks(pool: &PgPool) -> Result<u64, crate::broker::BrokerError> {
    let task_error = TaskError::builtin(
        crate::core::OutcomeCode::TaskExpired,
        "task expired before being claimed (good_until passed)",
    );
    let task_result = TaskResult::<()>::Err(task_error);
    let result_json = serde_json::to_string(&task_result)
        .unwrap_or_else(|_| r#"{"__type":"err","value":{"message":"expired"}}"#.to_owned());
    let command = crate::core::lifecycle::TerminalizationCommand::ExpirePendingTasks {
        batch_size: crate::core::lifecycle::BatchSize::new(EXPIRE_BATCH_SIZE)
            .expect("EXPIRE_BATCH_SIZE is positive"),
        result_json,
        error_code: "TASK_EXPIRED".to_owned(),
    };

    let mut total: u64 = 0;
    for _ in 0..EXPIRE_MAX_BATCHES_PER_PASS {
        let expired = crate::broker::terminalization::terminalize(pool, &command).await?;
        let affected = expired.len() as u64;
        total += affected;
        if affected < EXPIRE_BATCH_SIZE as u64 {
            break;
        }
    }
    Ok(total)
}

/// Requeue stale CLAIMED tasks back to PENDING. Returns the number of affected rows.
pub async fn requeue_stale_claimed(pool: &PgPool, threshold_secs: f64) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(REQUEUE_STALE_CLAIMED_SQL)
        .bind(threshold_secs)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

/// The drained signal for one retention DELETE's batching loop.
#[derive(Clone, Copy)]
enum DrainedWhen {
    /// The statement's rowcount equals the rows the batch selects: a short
    /// batch means nothing eligible is left.
    ShortBatch,
    /// The workflow statement: its rowcount counts workflows while the node
    /// budget keeps batches routinely short of `batch_size` — only a
    /// zero-row batch means drained, at the cost of one empty statement per
    /// drained pass.
    EmptyBatch,
}

/// Run one retention DELETE in bounded batches (autocommit per batch).
///
/// Always runs at least one batch; stops when the backlog reads as drained
/// (per `drained_when`) or the pass deadline is reached (backlog resumes next
/// pass). Bounded batches keep per-transaction WAL and row locks flat
/// regardless of backlog size.
async fn delete_expired_in_batches(
    pool: &PgPool,
    sql: &str,
    retention_hours: u32,
    batch_size: i64,
    deadline: tokio::time::Instant,
    drained_when: DrainedWhen,
) -> Result<u64, sqlx::Error> {
    let hours = retention_hours.to_string();
    let mut total: u64 = 0;
    loop {
        let deleted = sqlx::query(sql)
            .bind(&hours)
            .bind(batch_size)
            .execute(pool)
            .await?
            .rows_affected();
        total += deleted;
        let drained = match drained_when {
            DrainedWhen::ShortBatch => deleted < batch_size as u64,
            DrainedWhen::EmptyBatch => deleted == 0,
        };
        if drained {
            return Ok(total);
        }
        if tokio::time::Instant::now() >= deadline {
            tracing::info!(
                total_deleted = total,
                "retention pass time budget reached; remaining backlog resumes next pass",
            );
            return Ok(total);
        }
    }
}

/// Run retention cleanup for relational monitoring and workflow records.
///
/// Matches Python's retention cleanup logic in the worker's reaper loop.
/// Each category is gated by its config (None = disabled). A workflow and its
/// node rows are deleted together by the workflow-batched statement.
/// Deletes run in bounded batches under a shared pass time budget.
///
/// Called by the reaper loop every `retention_sweep_interval_s`.
pub async fn run_retention_cleanup(pool: &PgPool, config: &RetentionConfig) {
    let mut deleted_worker_states: u64 = 0;
    let mut deleted_workflows: u64 = 0;

    let batch_size = i64::from(config.retention_delete_batch_size);

    // Shared wall-clock budget across the statements. A backlog that
    // outlives the budget resumes next pass.
    let deadline = tokio::time::Instant::now() + RETENTION_PASS_TIME_BUDGET;

    let result: Result<(), sqlx::Error> = async {
        if let Some(hours) = config.worker_state_retention_hours {
            deleted_worker_states = delete_expired_in_batches(
                pool,
                DELETE_EXPIRED_WORKER_STATES_SQL,
                hours,
                batch_size,
                deadline,
                DrainedWhen::ShortBatch,
            )
            .await?;
        }

        if let Some(hours) = config.terminal_record_retention_hours {
            // Workflows and their node rows go together in one
            // workflow-batched statement. Terminal tasks already moved to
            // partitioned history and are never row-deleted here.
            deleted_workflows = delete_expired_in_batches(
                pool,
                DELETE_EXPIRED_WORKFLOWS_SQL,
                hours,
                batch_size,
                deadline,
                DrainedWhen::EmptyBatch,
            )
            .await?;
        }

        Ok(())
    }
    .await;

    match result {
        Ok(()) => {
            let total = deleted_worker_states + deleted_workflows;
            if total > 0 {
                tracing::info!(
                    deleted_worker_states,
                    deleted_workflows,
                    "retention cleanup completed",
                );
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "retention cleanup failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::history::maintenance::coverage::{
        ensure_startup_coverage, StartupCoverageOutcome,
    };
    use serial_test::serial;
    use uuid::Uuid;

    /// Batch size for direct retention-statement tests (the default
    /// `RecoveryConfig::retention_delete_batch_size`).
    const TEST_RETENTION_BATCH: i64 = 500;

    #[tokio::test]
    async fn gate_runs_ungated_only_for_static_single_connection_capacity() {
        let single = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy(&test_db_url())
            .unwrap();
        assert!(matches!(
            acquire_gate(&single, advisory_key_reaper()).await,
            GatePass::Ungated
        ));

        let unavailable = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect_lazy(&test_db_url())
            .unwrap();
        unavailable.close().await;
        assert!(matches!(
            acquire_gate(&unavailable, advisory_key_reaper()).await,
            GatePass::Skip
        ));
    }

    fn test_db_url() -> String {
        if let Ok(url) = std::env::var("DATABASE_URL") {
            return url;
        }
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let root = std::path::Path::new(manifest_dir)
            .ancestors()
            .find(|p| p.join(".env").exists());
        if let Some(root) = root {
            if let Ok(contents) = std::fs::read_to_string(root.join(".env")) {
                for line in contents.lines() {
                    let line = line.trim();
                    if line.is_empty() || line.starts_with('#') {
                        continue;
                    }
                    if let Some((key, value)) = line.split_once('=') {
                        if key.trim() == "DB_PASSWORD" {
                            return format!(
                                "postgresql://postgres:{}@localhost:5432/horsies-rust-port",
                                value.trim(),
                            );
                        }
                    }
                }
            }
        }
        panic!("database URL not found: set DATABASE_URL or add DB_PASSWORD to .env");
    }

    async fn test_pool() -> PgPool {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let mut coverage = pool.begin().await.expect("begin startup coverage");
        let outcome = ensure_startup_coverage(
            coverage.as_mut(),
            RetentionConfig::default().history_leaf_horizon_days,
            RetentionConfig::default().heartbeat_leaf_horizon_hours,
            &[],
            &StagedLoaderPublisher,
        )
        .await
        .expect("startup coverage");
        assert!(matches!(outcome, StartupCoverageOutcome::Ready(_)));
        coverage.commit().await.expect("commit startup coverage");
        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = 'reaper_test'")
            .execute(&pool)
            .await
            .expect("clean live reaper fixtures");
        sqlx::query("DELETE FROM horsies_task_history WHERE task_name = 'reaper_test'")
            .execute(&pool)
            .await
            .expect("clean archived reaper fixtures");
        pool
    }

    /// Insert a RUNNING task whose runner heartbeat is already stale, with the
    /// given `finalizing_at` (NULL or a timestamp).
    async fn insert_stale_running_task(pool: &PgPool, task_id: Uuid, finalizing_at_sql: &str) {
        let sql = format!(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs, status,
                sent_at, started_at, created_at, updated_at, claimed,
                claimed_by_worker_id, retry_count, max_retries, enqueue_sha,
                finalizing_at, command_fingerprint_version, command_fingerprint,
                retention_class_key, retain_rerun_input, prepared_rerun_input_disposition
            ) VALUES (
                $1, 'reaper_test', 'default', 100, '[]', '{{}}', 'RUNNING',
                NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour', NOW(), NOW(), TRUE,
                'worker-1', 0, 0,
                '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                {finalizing_at_sql}, 1, decode(repeat('00', 32), 'hex'),
                'standard_30d', FALSE, 'NEVER_ELIGIBLE'
            )"
        );
        sqlx::query(&sql).bind(task_id).execute(pool).await.unwrap();
    }

    async fn task_status(pool: &PgPool, task_id: Uuid) -> String {
        sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// P8: the Phase-1 scan is bounded by `scan_limit`, so one pass processes at
    /// most that many stale tasks and successive passes drain the rest.
    #[tokio::test]
    #[serial]
    async fn stale_running_scan_is_bounded_by_limit() {
        let pool = test_pool().await;
        // Clean this test's namespace so only our stale tasks are in play.
        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = 'reaper_test'")
            .execute(&pool)
            .await
            .unwrap();

        let mut ids = Vec::new();
        for _ in 0..3 {
            let id = Uuid::new_v4();
            insert_stale_running_task(&pool, id, "NULL").await;
            ids.push(id);
        }

        // scan_limit = 2 with 3 stale candidates → exactly 2 processed this pass.
        let count = mark_stale_running_as_failed(&pool, 1.0, 300.0, 2)
            .await
            .unwrap();
        assert_eq!(
            count, 2,
            "the bounded scan must process at most scan_limit tasks"
        );

        let state: (i64, i64) = sqlx::query_as(
            "SELECT
                 (SELECT count(*) FROM horsies_tasks WHERE id = ANY($1::uuid[])),
                 (SELECT count(*) FROM horsies_task_history
                  WHERE task_id = ANY($1::uuid[]) AND status = 'FAILED'
                    AND terminalization_kind = 'FAIL_STALE')",
        )
        .bind(&ids)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(state, (1, 2));

        for id in &ids {
            sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
                .bind(id)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("DELETE FROM horsies_task_history WHERE task_id = $1")
                .bind(id)
                .execute(&pool)
                .await
                .unwrap();
        }
    }

    /// A task actively finalizing (recent finalizing_at) must NOT be reclaimed
    /// by the stale-RUNNING reaper even though its runner heartbeat has stopped.
    #[tokio::test]
    #[serial]
    async fn reaper_skips_actively_finalizing_task() {
        let pool = test_pool().await;
        let task_id = Uuid::new_v4();
        // finalizing_at = NOW(): within the finalizing threshold → skip.
        insert_stale_running_task(&pool, task_id, "NOW()").await;

        // running stale threshold 1s (heartbeat is 1h old → stale), finalizing
        // threshold 300s (finalizing_at is fresh → protected). The reaper scan is
        // global (shared test DB), so assert on this task's status, not the count.
        mark_stale_running_as_failed(&pool, 1.0, 300.0, STALE_RUNNING_SCAN_LIMIT)
            .await
            .unwrap();

        assert_eq!(
            task_status(&pool, task_id).await,
            "RUNNING",
            "actively-finalizing task must be skipped by the reaper"
        );

        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// A task whose finalizing stamp is older than the finalizing threshold is
    /// treated as genuinely stuck and IS reclaimed (marked FAILED).
    #[tokio::test]
    #[serial]
    async fn reaper_reclaims_task_finalizing_past_threshold() {
        let pool = test_pool().await;
        let task_id = Uuid::new_v4();
        // finalizing_at well in the past → past the finalizing threshold.
        insert_stale_running_task(&pool, task_id, "NOW() - INTERVAL '1 hour'").await;

        let count = mark_stale_running_as_failed(&pool, 1.0, 300.0, STALE_RUNNING_SCAN_LIMIT)
            .await
            .unwrap();
        assert!(count >= 1, "the targeted stale row must be reclaimed");
        let archived: (String, String) = sqlx::query_as(
            "SELECT status, terminalization_kind
             FROM horsies_task_history WHERE task_id = $1",
        )
        .bind(task_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(archived, ("FAILED".to_owned(), "FAIL_STALE".to_owned()));

        sqlx::query("DELETE FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// Retention must NOT delete a terminal+expired workflow's linkage while a
    /// backing task row is still live; once that task is terminal, it sweeps.
    /// Parity with horsies PR #143 (defensive prevent lever).
    #[tokio::test]
    #[serial]
    async fn retention_retains_workflow_with_live_backing_task() {
        let pool = test_pool().await;

        // Drain pre-existing eligible candidates (other tests' leftovers) so
        // the rows_affected assertions below see only this test's workflow.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        delete_expired_in_batches(
            &pool,
            DELETE_EXPIRED_WORKFLOWS_SQL,
            0,
            TEST_RETENTION_BATCH,
            deadline,
            DrainedWhen::EmptyBatch,
        )
        .await
        .unwrap();

        let wf_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();

        // Terminal + expired workflow.
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at, completed_at
            ) VALUES (
                $1, 'ret_wf', 'CANCELLED', 'fail', 'test.ret.v1', 0, $1,
                NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours',
                NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours',
                NOW() - INTERVAL '2 hours'
            )",
        )
        .bind(wf_id)
        .execute(&pool)
        .await
        .unwrap();

        // A still-live (RUNNING) backing task row + its workflow_task linkage.
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs, status,
                sent_at, started_at, created_at, updated_at, retry_count, max_retries, enqueue_sha,
                command_fingerprint_version, command_fingerprint, retention_class_key,
                retain_rerun_input, prepared_rerun_input_disposition
            ) VALUES (
                $1, 'ret_task', 'default', 100, '[]', '{}', 'RUNNING',
                NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours',
                NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours', 0, 0, $1,
                1, decode(repeat('00', 32), 'hex'), 'standard_30d', FALSE, 'NEVER_ELIGIBLE'
            )",
        )
        .bind(task_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
                queue_name, priority, dependencies, allow_failed_deps, join_type,
                status, is_subworkflow, task_id, created_at
            ) VALUES (
                $1, $2, 0, 'node_0', 'ret_task', '[]', '{}',
                'default', 100, '{}', FALSE, 'all',
                'ENQUEUED', FALSE, $3, NOW() - INTERVAL '2 hours'
            )",
        )
        .bind(Uuid::new_v4())
        .bind(wf_id)
        .bind(task_id)
        .execute(&pool)
        .await
        .unwrap();

        let wt_count = |pool: PgPool, wf: Uuid| async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = $1",
            )
            .bind(wf)
            .fetch_one(&pool)
            .await
            .unwrap()
        };

        let wf_count = |pool: PgPool, wf: Uuid| async move {
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM horsies_workflows WHERE id = $1")
                .bind(wf)
                .fetch_one(&pool)
                .await
                .unwrap()
        };

        // hours = 0 → everything terminal+expired qualifies by age; only the
        // live-backing-task guard should hold the workflow (and linkage) back.
        let deleted = sqlx::query(DELETE_EXPIRED_WORKFLOWS_SQL)
            .bind("0")
            .bind(TEST_RETENTION_BATCH)
            .execute(&pool)
            .await
            .unwrap()
            .rows_affected();
        assert_eq!(
            deleted, 0,
            "no workflow deleted while a backing task is live"
        );
        assert_eq!(
            wt_count(pool.clone(), wf_id).await,
            1,
            "linkage must be retained while a backing task is live",
        );
        assert_eq!(
            wf_count(pool.clone(), wf_id).await,
            1,
            "workflow must be retained while a backing task is live",
        );

        // Canonical workflow cancellation moves the backing task to history;
        // workflow and linkage then sweep together in one statement.
        let outcomes = crate::broker::terminalization::terminalize(
            &pool,
            &crate::core::lifecycle::TerminalizationCommand::CancelNodesOfCancelledWorkflow {
                workflow_ids: vec![wf_id],
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            outcomes.as_slice(),
            [crate::core::lifecycle::TerminalizationOutcome::Applied { .. }]
        ));
        let deleted = sqlx::query(DELETE_EXPIRED_WORKFLOWS_SQL)
            .bind("0")
            .bind(TEST_RETENTION_BATCH)
            .execute(&pool)
            .await
            .unwrap()
            .rows_affected();
        assert_eq!(deleted, 1, "rowcount counts workflows");
        assert_eq!(
            wt_count(pool.clone(), wf_id).await,
            0,
            "linkage must leave with its workflow",
        );
        assert_eq!(
            wf_count(pool.clone(), wf_id).await,
            0,
            "workflow swept once all backing tasks terminal",
        );

        sqlx::query("DELETE FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// The node budget bounds each statement's node deletions: 4 workflows ×
    /// 3 nodes against budget 6 → two workflows per statement; the empty-batch
    /// drain loop still removes the whole backlog in one
    /// `delete_expired_in_batches` call (a short-batch heuristic would stop
    /// after the first 2-row batch — the revert-proof). A workflow larger than
    /// the whole budget drains alone instead of starving.
    /// Parity with horsies PR #216.
    #[tokio::test]
    #[serial]
    async fn workflow_retention_budgets_nodes_and_drains_on_empty_batch() {
        let pool = test_pool().await;

        // Drain pre-existing eligible candidates (other tests' leftovers) so
        // the rows_affected assertions below see only this test's workflows.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        delete_expired_in_batches(
            &pool,
            DELETE_EXPIRED_WORKFLOWS_SQL,
            0,
            TEST_RETENTION_BATCH,
            deadline,
            DrainedWhen::EmptyBatch,
        )
        .await
        .unwrap();

        let seed_workflow = |pool: PgPool, nodes: i64| async move {
            let wf_id = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO horsies_workflows (
                    id, name, status, on_error, definition_key, depth, root_workflow_id,
                    sent_at, created_at, started_at, updated_at, completed_at
                ) VALUES (
                    $1, 'ret_budget_wf', 'COMPLETED', 'fail', 'test.ret.v1', 0, $1,
                    NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours',
                    NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours',
                    NOW() - INTERVAL '2 hours'
                )",
            )
            .bind(wf_id)
            .execute(&pool)
            .await
            .unwrap();
            for i in 0..nodes {
                sqlx::query(
                    "INSERT INTO horsies_workflow_tasks (
                        id, workflow_id, task_index, node_id, task_name, task_args,
                        task_kwargs, queue_name, priority, dependencies,
                        allow_failed_deps, join_type, status, is_subworkflow, created_at
                    ) VALUES (
                        $1, $2, $3, 'node_' || $3, 'ret_budget_task', '[]', '{}',
                        'default', 100, '{}', FALSE, 'all',
                        'COMPLETED', FALSE, NOW() - INTERVAL '2 hours'
                    )",
                )
                .bind(Uuid::new_v4())
                .bind(wf_id)
                .bind(i as i32)
                .execute(&pool)
                .await
                .unwrap();
            }
            wf_id
        };

        let count_workflows = |pool: PgPool| async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM horsies_workflows WHERE name = 'ret_budget_wf'",
            )
            .fetch_one(&pool)
            .await
            .unwrap()
        };

        // 4 workflows × 3 nodes, budget 6 → 2 workflows per statement.
        for _ in 0..4 {
            seed_workflow(pool.clone(), 3).await;
        }
        let first = sqlx::query(DELETE_EXPIRED_WORKFLOWS_SQL)
            .bind("0")
            .bind(6_i64)
            .execute(&pool)
            .await
            .unwrap()
            .rows_affected();
        assert_eq!(first, 2, "node budget 6 admits two 3-node workflows");
        assert_eq!(count_workflows(pool.clone()).await, 2);

        // The empty-batch drain loop removes the rest in one call (2 + 0-row
        // statements); short-batch semantics would have returned after the
        // first 2-row batch above.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let total = delete_expired_in_batches(
            &pool,
            DELETE_EXPIRED_WORKFLOWS_SQL,
            0,
            6,
            deadline,
            DrainedWhen::EmptyBatch,
        )
        .await
        .unwrap();
        assert_eq!(total, 2, "drain loop continues past short batches to empty");
        assert_eq!(count_workflows(pool.clone()).await, 0);

        // Jumbo: 9 nodes against budget 4 → drains alone (position = 1 escape).
        let jumbo = seed_workflow(pool.clone(), 9).await;
        let deleted = sqlx::query(DELETE_EXPIRED_WORKFLOWS_SQL)
            .bind("0")
            .bind(4_i64)
            .execute(&pool)
            .await
            .unwrap()
            .rows_affected();
        assert_eq!(deleted, 1, "over-budget workflow must not starve");
        let nodes_left: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = $1",
        )
        .bind(jumbo)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(nodes_left, 0, "jumbo's nodes leave with it");
    }

    /// The workflow retention DELETE must execute via
    /// idx_horsies_workflows_retention (migration 0028). EXPLAIN ANALYZE runs
    /// the exact production statement inside a rolled-back transaction, so
    /// the assertion covers the plan the executor ran: a drifted COALESCE, a
    /// status-literal regression, or a lost statistics object (whose default
    /// 1/3 estimate flips the planner back to a full-table walk) fails here.
    /// The plan must also carry the set-wise node purge as its own Delete
    /// node (parity with horsies PR #216).
    #[tokio::test]
    #[serial]
    async fn workflow_retention_deletes_use_retention_index() {
        let pool = test_pool().await;

        // Re-runnable after a failed run: drop any leftover seed rows.
        sqlx::query("DELETE FROM horsies_workflows WHERE name = 'ret_explain_wf'")
            .execute(&pool)
            .await
            .unwrap();

        // Realistic statistics: 500 old terminal workflows (eligible) + 2000
        // recent terminal workflows (in-window). The recent population makes
        // the retention index's cutoff range decisively more selective — with
        // only eligible rows the planner's index pick is arbitrary.
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at, completed_at
            )
            SELECT
                id, 'ret_explain_wf', 'COMPLETED', 'fail', 'test.ret.v1', 0,
                id,
                NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days',
                NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days',
                NOW() - INTERVAL '30 days'
            FROM (SELECT gen_random_uuid() AS id FROM generate_series(1, 500)) seeded",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at, completed_at
            )
            SELECT
                id, 'ret_explain_wf', 'COMPLETED', 'fail', 'test.ret.v1', 0,
                id,
                NOW(), NOW(), NOW(), NOW(), NOW()
            FROM (SELECT gen_random_uuid() AS id FROM generate_series(1, 2000)) seeded",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("ANALYZE horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();

        // A 500-row table fits in a few pages, so the planner still prefers a
        // seq scan; disable it (transaction-local) to force the index choice a
        // production-sized heap produces on its own. ANALYZE executes the
        // DELETE — the rollback reverts it.
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("SET LOCAL enable_seqscan = off")
            .execute(&mut *tx)
            .await
            .unwrap();
        let plan_rows: Vec<(String,)> = sqlx::query_as(&format!(
            "EXPLAIN (ANALYZE, BUFFERS) {DELETE_EXPIRED_WORKFLOWS_SQL}"
        ))
        .bind("240")
        .bind(TEST_RETENTION_BATCH)
        .fetch_all(&mut *tx)
        .await
        .unwrap();
        tx.rollback().await.unwrap();

        let plan = plan_rows
            .iter()
            .map(|(line,)| line.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            plan.contains("idx_horsies_workflows_retention"),
            "workflow retention delete must execute via the workflows retention index; plan:\n{plan}",
        );
        assert!(
            plan.contains("Delete on horsies_workflow_tasks"),
            "node rows must be purged set-wise in the statement; plan:\n{plan}",
        );

        sqlx::query("DELETE FROM horsies_workflows WHERE name = 'ret_explain_wf'")
            .execute(&pool)
            .await
            .unwrap();
    }
}
