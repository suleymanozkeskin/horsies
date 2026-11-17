use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::broker::core_rows::StatementLog;
use crate::broker::expiry_and_orphans::{
    cancel_orphan_page, expire_pending_pass, OrphanAuditPage, PendingExpiryFailed,
    PendingExpiryPass, PENDING_EXPIRY_MAX_BATCHES_PER_PASS,
};
use crate::broker::stale_recovery::{
    finish_stale_attempt, requeue_stale_claimed_pass, scan_stale_running, wake_retried_queue,
    FinishStaleAttempt, RecoveryBatch, StaleCandidate, StaleClaimsRequeuePass, StaleRetry,
    StaleRunningPass, StaleRunningThresholds, ThresholdMs, ThresholdOutOfRange,
    STALE_CLAIMED_REQUEUE_MAX_BATCHES_PER_PASS, STALE_RUNNING_SCAN_BATCH,
};
use crate::broker::CoveragePassError;
use crate::broker::PostgresBroker;
use crate::core::config::recovery::RecoveryConfig;
use crate::core::config::retention::RetentionConfig;
use crate::core::history::identity::reservations::{
    cleanup_expired_reservations_pass, ReservationCleanupPass, RESERVATION_CLEANUP_BATCHES_PER_PASS,
};
use crate::core::history::maintenance::coverage::{
    CoverageOutcome, CoverageRequest, CoverageSerialization,
};
use crate::core::history::maintenance::pruning::PruneReach;
use crate::core::lifecycle::StaleAttemptOutcome;
use crate::core::{OperationalErrorCode, TaskError, TaskErrorKind, TaskResult};
use crate::db::CorePool;
use crate::log_targets::STALE_RUNNING_PASS;

use crate::worker::class_coverage_log::{check_class_coverage, ClassCoverageLog};
use crate::worker::retry::{admit_retry, read_stored_options, RetryAdmission};
use crate::workflow_engine::child_spec::ChildSpecs;

#[cfg(test)]
mod p7_maintenance_tests {
    use super::*;
    use crate::core::history::commands::{CreateDailyHistoryLeaf, LeafBounds, LeafRef};
    use crate::core::history::ddl::classes::{finite_class_parent_name, register_period_class};
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
    use crate::core::history::periods::RetentionDays;
    use crate::core::history::reads::publisher::StagedLoaderPublisher;
    use crate::core::lifecycle::TerminalizationOutcome;
    use crate::core::registry::workflow::WorkflowSpecRegistry;
    use crate::test_support::terminal_writes;
    use chrono::Timelike;
    use serial_test::serial;
    use uuid::Uuid;

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
                 id, task_name, queue_name, priority, input, task_options, execution,
                 retention_class_key, sent_at, created_at,
                 command_fingerprint_version, command_fingerprint, idempotency_key_digest,
                 status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
                 started_at, finalizing_at, finalizing_by_worker_id,
                 worker_pid, worker_hostname, worker_process_name,
                 retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
                 updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
             ) VALUES (
                 $1, 'p7_retention_task', 'default', 100, 'null', NULL, 'ISOLATED',
                 'forever', NOW(), NOW(),
                 NULL, NULL, NULL,
                 'RUNNING', NOW(), NOW(), 'p7-retention-worker', NULL,
                 NOW(), NULL, NULL,
                 4242, 'p7-host', 'p7-process',
                 0, 0, NULL, NULL, TRUE,
                 NOW(), FALSE, NULL, NULL
             )",
        )
        .bind(task_id)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, task_id,
                created_at, max_retries, execution, retention_class_key
            ) VALUES (
                'NONE', $1, $2, 0, 'root', 'p7_retention_task', 'null', 'default', 100, '{}', FALSE,
                'all', 'RUNNING', FALSE, $3, NOW(), 0, 'ISOLATED', 'standard_30d'
            )",
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
        let outcomes =
            terminal_writes::complete_workflow_task(pool, task_id, "p7-retention-worker", &result)
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
        // Every NOT NULL column of the baseline table is named: it has no
        // column defaults.
        sqlx::query(
            "INSERT INTO horsies_worker_states (
                worker_id, snapshot_at, hostname, pid, processes,
                max_claim_batch, max_claim_per_worker, queues,
                queue_priorities, queue_max_concurrency, recovery_config,
                tasks_running, tasks_claimed, worker_started_at,
                slots_down, class_capacity
             ) VALUES ($1, NOW() - INTERVAL '48 hours', 'host', 1, 1,
                       1, 1, '{default}',
                       '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
                       0, 0, NOW() - INTERVAL '48 hours',
                       '[]'::jsonb, '{}'::jsonb)",
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
        // Below the coverage floor of two future leaves: the coverage call
        // fails before any write. Config validation refuses this value, so
        // the pass sees it only when a config bypasses validation.
        retention.history_leaf_horizon_days = 1;
        let health = new_reaper_health();
        let mut next_retention_cleanup = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut next_partition_maintenance = tokio::time::Instant::now();
        let mut orphan_state = OrphanSweepState::default();

        run_reaper_pass(
            &PostgresBroker::from_pool(pool.clone()),
            &pool,
            &pool,
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
            &recovery,
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

    /// Python's order (`reaper.py:144-638`) requeues stale CLAIMED rows before
    /// it expires PENDING rows, so a stale claim whose `good_until` has passed
    /// is requeued and expired in one pass. Expiry first would leave it
    /// PENDING after the pass.
    #[tokio::test]
    #[serial]
    async fn a_stale_claim_past_its_deadline_is_requeued_then_expired_in_one_pass() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let coverage =
            crate::core::history::maintenance::coverage::ensure_partition_coverage_in_pool(
                &crate::broker::pools::core_pool_for_test_pool(&pool),
                crate::broker::core_rows::StatementLog::Off,
                2,
                2,
                &[],
            )
            .await
            .expect("partition coverage");
        assert!(matches!(
            coverage,
            crate::core::history::maintenance::coverage::CoverageOutcome::Ensured(_)
        ));
        let task_id = Uuid::new_v4();
        sqlx::query(
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
                 $1, 'reaper_order', 'default', 100, '{}', NULL, 'ISOLATED',
                 'forever', NOW() - interval '1 day', NOW() - interval '1 day',
                 1, decode(repeat('00', 32), 'hex'), NULL,
                 'CLAIMED', NOW() - interval '1 day', NOW() - interval '1 day',
                 'gone-worker', NOW() + interval '1 hour',
                 NULL, NULL, NULL,
                 NULL, NULL, NULL,
                 0, 0, NULL, NOW() - interval '1 minute', FALSE,
                 NOW(), FALSE, NULL, NULL
             )",
        )
        .bind(task_id)
        .execute(&pool)
        .await
        .expect("seed a stale claim past its deadline");
        let mut recovery = RecoveryConfig::default();
        recovery.auto_requeue_stale_claimed = true;
        recovery.auto_fail_stale_running = false;
        recovery.auto_terminate_orphaned_workflow_tasks = false;
        let retention = RetentionConfig::default();
        let health = new_reaper_health();
        let mut next_retention_cleanup = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut next_partition_maintenance = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut orphan_state = OrphanSweepState::default();

        run_reaper_pass(
            &PostgresBroker::from_pool(pool.clone()),
            &pool,
            &pool,
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
            &recovery,
            &retention,
            &health,
            &mut next_retention_cleanup,
            &mut next_partition_maintenance,
            &mut orphan_state,
        )
        .await;

        let live: Option<String> =
            sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
                .bind(task_id)
                .fetch_optional(&pool)
                .await
                .expect("read the live row");
        assert_eq!(live, None, "the expired task leaves the live table");
        let history: String =
            sqlx::query_scalar("SELECT status FROM horsies_task_history WHERE task_id = $1")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .expect("read the history row");
        assert_eq!(history, "EXPIRED");
        sqlx::query("DELETE FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .execute(&pool)
            .await
            .expect("remove the history row");
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
            &PostgresBroker::from_pool(pool.clone()),
            &pool,
            &pool,
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
            &recovery,
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
        // Two short periods that no other test of this database registers.
        let blocked_period = RetentionDays::parse_period(chrono::Duration::days(1)).unwrap();
        let valid_period = RetentionDays::parse_period(chrono::Duration::days(2)).unwrap();
        let blocked_class_key = blocked_period.class_key();
        let valid_class_key = valid_period.class_key();
        let blocked_class = blocked_class_key.as_str();
        let valid_class = valid_class_key.as_str();

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
        for period in [blocked_period, valid_period] {
            register_period_class(&mut setup, period).await.unwrap();
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
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, definition_key, status, on_error, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at
             ) VALUES ($1, 'p7 pass blocker', NULL, 'RUNNING', 'fail', 0, $1,
                       NOW(), NOW(), NOW(), NOW())",
        )
        .bind(workflow_id)
        .execute(&mut *setup)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                 downstream_subworkflow, id, workflow_id, task_index, task_name, input, queue_name, priority,
                 dependencies, allow_failed_deps, join_type, max_retries, execution,
                 retention_class_key, status, is_subworkflow, created_at
             ) VALUES ('NONE', $1, $2, 0, 'p7 pass blocker', '{}', 'default', 100,
                       '{}', FALSE, 'all', 0, 'ISOLATED',
                       $3, 'RUNNING', FALSE, NOW())",
        )
        .bind(node_id)
        .bind(workflow_id)
        .bind(blocked_class)
        .execute(&mut *setup)
        .await
        .unwrap();
        // A record at the attempt bound has a last attempt and its failure
        // class (baseline CHECK horsies_workflow_phase2_pending_attempts).
        sqlx::query(
            "INSERT INTO horsies_workflow_phase2_pending (
                 task_id, workflow_id, workflow_node_row_id, terminalization_kind,
                 recovery_source, history_class, history_anchor, history_schema_version,
                 result_digest, quarantine_task_id, created_at, attempt_count,
                 last_attempt_at, last_failure_class
             ) VALUES ($1, $2, $3, 'COMPLETE_LOCKED',
                       'HISTORY', $5, $4, 1, $6, NULL,
                       NOW() - INTERVAL '2 hours', $7,
                       NOW() - INTERVAL '1 hour', 'RAISED')",
        )
        .bind(blocker_task_id)
        .bind(workflow_id)
        .bind(node_id)
        .bind(history_leaves[0].bounds().lower() + chrono::Duration::hours(1))
        .bind(blocked_class)
        .bind(vec![31_u8; 32])
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

        retention
            .retention_periods
            .extend([blocked_period.as_duration(), valid_period.as_duration()]);
        let health = new_reaper_health();
        let mut next_retention_cleanup = tokio::time::Instant::now();
        let mut next_partition_maintenance = tokio::time::Instant::now();
        let mut orphan_state = OrphanSweepState::default();
        run_reaper_pass(
            &PostgresBroker::from_pool(pool.clone()),
            &pool,
            &pool,
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
            &recovery,
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
        assert_eq!(pruning["key_reservations"]["state"], "complete");
        assert_eq!(pruning["reach"], "within_bounds");
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

// ---------------------------------------------------------------------------
// Retention cleanup SQL
// ---------------------------------------------------------------------------

// The baseline table has no id column: (worker_id, snapshot_at) is its
// identity (unique index horsies_worker_states_identity). Python
// `DELETE_EXPIRED_WORKER_STATES_SQL`, `python/horsies/core/worker/sql.py:310-318`.
const DELETE_EXPIRED_WORKER_STATES_SQL: &str = "\
DELETE FROM horsies_worker_states
WHERE (worker_id, snapshot_at) IN (
    SELECT worker_id, snapshot_at FROM horsies_worker_states
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
pub(crate) const DELETE_EXPIRED_WORKFLOWS_SQL: &str = "\
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

/// Wall-clock budget for one retention pass across both retained-row statements. A
/// backlog that does not drain within the budget resumes on the next pass;
/// every statement still runs at least one batch per pass so a deep backlog
/// in an earlier table cannot starve the later ones indefinitely.
const RETENTION_PASS_TIME_BUDGET: Duration = Duration::from_secs(60);

/// Reaper-owned health attached to worker-state snapshots.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ReaperHealthSnapshot {
    pub workflow_recovery: Option<serde_json::Value>,
    pub orphan_task_recovery: Option<serde_json::Value>,
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
/// as FAILED or requeuing them respectively. The caller has checked the
/// client family of the handle that owns both pools
/// (`Worker::start_database`); the reaper's partition maintenance writes
/// without a second check.
#[allow(clippy::needless_pass_by_value)]
pub fn spawn_reaper(
    broker: Arc<PostgresBroker>,
    runtime_pool: PgPool,
    maintenance_pool: PgPool,
    child_specs: ChildSpecs,
    config: RecoveryConfig,
    retention: RetentionConfig,
    health: ReaperHealth,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let check_interval = Duration::from_millis(config.check_interval_ms);
        // The first pass runs at start and includes the retention cleanup
        // (Python `_ReaperPassState(next_retention_cleanup_at=time.monotonic())`).
        let mut next_retention_cleanup = tokio::time::Instant::now();
        let mut next_partition_maintenance = tokio::time::Instant::now();
        let mut orphan_state = OrphanSweepState::default();
        // The fleet check runs after each pass, from one check interval after
        // the start on (Python `reaper.py:622-628, 747-750`).
        let mut coverage_log =
            ClassCoverageLog::for_reaper_start(tokio::time::Instant::now(), check_interval);

        tracing::info!(
            auto_requeue_claimed = config.auto_requeue_stale_claimed,
            auto_fail_running = config.auto_fail_stale_running,
            check_interval_ms = config.check_interval_ms,
            orphan_task_audit_interval_ms = config.orphan_task_audit_interval_ms,
            "reaper started",
        );

        // The first pass runs at start, then one per check interval (Python
        // `_reaper_loop`: pass, then wait).
        while !cancel.is_cancelled() {
            // Cluster-wide gate: only one worker runs a pass per interval.
            // The passes are safe to run concurrently (SKIP LOCKED), but
            // redundant across a cluster; the gate elides the duplicate work.
            match acquire_gate(&runtime_pool, advisory_key_reaper()).await {
                GatePass::Skip => {
                    tracing::debug!("reaper pass skipped: another worker holds the gate");
                }
                GatePass::Ungated => {
                    run_reaper_pass(
                        &broker,
                        &runtime_pool,
                        &maintenance_pool,
                        &child_specs,
                        &config,
                        &retention,
                        &health,
                        &mut next_retention_cleanup,
                        &mut next_partition_maintenance,
                        &mut orphan_state,
                    )
                    .await;
                    check_class_coverage(&runtime_pool, &mut coverage_log).await;
                }
                GatePass::Held(tx) => {
                    run_reaper_pass(
                        &broker,
                        &runtime_pool,
                        &maintenance_pool,
                        &child_specs,
                        &config,
                        &retention,
                        &health,
                        &mut next_retention_cleanup,
                        &mut next_partition_maintenance,
                        &mut orphan_state,
                    )
                    .await;
                    check_class_coverage(&runtime_pool, &mut coverage_log).await;
                    release_gate(tx).await;
                }
            }

            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(check_interval) => {}
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

/// Run one reaper pass, in Python's order: stale-CLAIMED requeue, orphan
/// audit, stale-RUNNING recovery, PENDING expiry, workflow recovery, then
/// periodic retention cleanup and partition maintenance.
async fn run_reaper_pass(
    broker: &PostgresBroker,
    runtime_pool: &PgPool,
    maintenance_pool: &PgPool,
    child_specs: &ChildSpecs,
    config: &RecoveryConfig,
    retention: &RetentionConfig,
    health: &ReaperHealth,
    next_retention_cleanup: &mut tokio::time::Instant,
    next_partition_maintenance: &mut tokio::time::Instant,
    orphan_state: &mut OrphanSweepState,
) {
    // Python's order (`reaper.py:144-638`): requeue stale CLAIMED rows,
    // audit orphaned workflow tasks, fail stale RUNNING rows, expire PENDING
    // rows, then the workflow and maintenance steps. No step stops for the
    // life of the process: a step that keeps failing logs an error on every
    // pass and its health keeps reporting the failure.
    // The stale CLAIMED requeue runs on the main core pool with the reserve.
    if config.auto_requeue_stale_claimed {
        match ThresholdMs::from_millis(config.claimed_stale_threshold_ms) {
            Ok(threshold) => {
                let requeued = match broker.core_pools_for_write() {
                    Ok((open, _write)) => {
                        requeue_stale_claimed(open.main(), broker.statement_log(), threshold).await
                    }
                    Err(error) => Err(error),
                };
                match requeued {
                    Ok(count) if count > 0 => {
                        tracing::info!(count, "reaper requeued stale CLAIMED tasks");
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "reaper: failed to requeue stale claimed tasks");
                    }
                    _ => {}
                }
            }
            Err(error) => {
                tracing::error!(%error, "reaper: the stale CLAIMED threshold is out of range");
            }
        }
    }

    // Cancel orphaned workflow tasks (no live workflow_task linkage). These
    // cannot reach RUNNING, so the requeue above skips them and they would
    // otherwise stay CLAIMED forever; cancelling frees claim budget and lets
    // retention sweep them.
    if config.auto_terminate_orphaned_workflow_tasks {
        let now = tokio::time::Instant::now();
        if orphan_state.schedule_if_due(
            now,
            Duration::from_millis(config.orphan_task_audit_interval_ms),
        ) {
            let started = std::time::Instant::now();
            // One page on the main core pool with the reserve, in one
            // transaction with its cursor counts.
            let page = match broker.core_pools_for_write() {
                Ok((open, _write)) => cancel_orphan_page(open.main(), broker.statement_log()).await,
                Err(error) => Err(error),
            };
            match page.map(|page| OrphanTaskAuditReport::ready(page, started)) {
                Ok(report) => {
                    orphan_state.permanent_failures = 0;
                    if report.cancelled > 0 {
                        tracing::warn!(
                            count = report.cancelled,
                            "reaper cancelled orphaned workflow task(s) (no live \
                         workflow_task linkage)",
                        );
                    }
                    health.write().await.orphan_task_recovery = Some(
                        serde_json::to_value(report).unwrap_or_else(|error| {
                            serde_json::json!({"state": "error", "error": error.to_string()})
                        }),
                    );
                }
                Err(error) => {
                    let (kind, snapshot) = orphan_audit_failure_snapshot(&error, started);
                    health.write().await.orphan_task_recovery = Some(snapshot);
                    match kind {
                        OrphanAuditFailureKind::Refusal => {
                            orphan_state.permanent_failures = 0;
                            tracing::warn!(
                                %error,
                                "reaper orphan audit lock was refused and will retry on its next schedule",
                            );
                        }
                        OrphanAuditFailureKind::Transient => {
                            orphan_state.permanent_failures = 0;
                            tracing::error!(
                                %error,
                                "reaper orphan audit failed transiently and will retry on its next schedule",
                            );
                        }
                        OrphanAuditFailureKind::Permanent => {
                            // The audit never stops on a live worker: each
                            // failing pass logs, and the health snapshot
                            // above keeps reporting the failure.
                            orphan_state.permanent_failures =
                                orphan_state.permanent_failures.saturating_add(1);
                            tracing::error!(
                                %error,
                                consecutive_failures = orphan_state.permanent_failures,
                                "reaper orphan audit failed and will run again on its next schedule",
                            );
                        }
                    }
                }
            }
        }
    }

    // Stale RUNNING recovery on the main core pool with the reserve: one
    // scan, then one statement per candidate.
    if config.auto_fail_stale_running {
        match stale_running_thresholds(config) {
            Ok(thresholds) => {
                let recovered = match broker.core_pools_for_write() {
                    Ok((open, _write)) => {
                        mark_stale_running_as_failed(
                            open.main(),
                            broker.statement_log(),
                            thresholds,
                            STALE_RUNNING_SCAN_BATCH,
                        )
                        .await
                    }
                    Err(error) => Err(error),
                };
                match recovered {
                    Ok(count) if count > 0 => {
                        tracing::warn!(count, "reaper marked stale RUNNING tasks as FAILED");
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "reaper: failed to mark stale running tasks");
                    }
                    _ => {}
                }
            }
            Err(error) => {
                tracing::error!(%error, "reaper: the stale RUNNING thresholds are out of range");
            }
        }
    }

    // Expire unclaimed PENDING tasks whose good_until has passed, on the
    // main core pool with the reserve.
    let expired = match broker.core_pools_for_write() {
        Ok((open, _write)) => expire_pending_pass(open.main(), broker.statement_log()).await,
        Err(error) => Err(PendingExpiryFailed {
            expired_before: 0,
            error,
        }),
    };
    match expired {
        Ok(PendingExpiryPass::Drained { expired: count }) if count > 0 => {
            tracing::info!(count, "reaper expired unclaimed PENDING tasks");
        }
        Ok(PendingExpiryPass::Drained { .. }) => {}
        Ok(PendingExpiryPass::BoundReached { expired: count }) => {
            tracing::warn!(
                expired = count,
                batches = PENDING_EXPIRY_MAX_BATCHES_PER_PASS,
                "pending expiry reached its per-pass bound; the next pass goes on",
            );
        }
        Err(failed) => {
            tracing::warn!(error = %failed, "reaper: failed to expire pending tasks");
        }
    }

    // Exact outbox recovery. Each candidate owns its transaction; retaining
    // dispositions remain visible and count toward bounded quarantine.
    // The children of the sub-workflow nodes that a step starts come from
    // the worker's child spec source.
    let workflow_recovery =
        crate::workflow_engine::recovery::recover_stuck_workflows_observed_with(
            broker,
            child_specs.build(retention),
            config.workflow_recovery_failure_limit,
        )
        .await;
    health.write().await.workflow_recovery = Some(match workflow_recovery {
        Ok(report) => serde_json::to_value(report).unwrap_or_else(
            |error| serde_json::json!({"state": "error", "error": error.to_string()}),
        ),
        Err(failure) => {
            tracing::error!(error = %failure.error, "workflow recovery pass failed");
            failure.into_health_snapshot()
        }
    });

    // Discovery and each record run on the main core pool with the reserve.
    let phase2 = crate::workflow_engine::phase2_recovery::drive_phase2_recovery_with(
        broker,
        child_specs.build(retention),
        config.crashed_worker_recovery_grace_ms,
        crate::workflow_engine::recovery::GLOBAL_SCAN_ROW_CAP,
        config.phase2_quarantine_after_attempts,
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
        match crate::workflow_engine::lifecycle::expire_paused_workflows(broker, age, 50).await {
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
        let declared = retention.registrable_periods();
        let coverage = match CoverageRequest::new(
            retention.history_leaf_horizon_days,
            retention.heartbeat_leaf_horizon_hours,
            &declared,
        ) {
            Ok(request) => {
                broker
                    .ensure_partition_coverage(&request, CoverageSerialization::PerLeafLocks)
                    .await
            }
            Err(refused) => Err(CoveragePassError::History(refused)),
        };
        let coverage_health = match coverage {
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
        let prune = broker.prune_expired_partitions().await;
        // Expired key reservations are removed after pruning, in bounded
        // batches; the rest waits for the next pass.
        let key_reservations = match cleanup_expired_reservations_pass(
            maintenance_pool,
            i32::try_from(retention.retention_delete_batch_size).unwrap_or(i32::MAX),
            RESERVATION_CLEANUP_BATCHES_PER_PASS,
        )
        .await
        {
            Ok(ReservationCleanupPass::Complete { deleted }) => {
                serde_json::json!({"state": "complete", "deleted": deleted})
            }
            Ok(ReservationCleanupPass::Truncated { deleted }) => {
                serde_json::json!({"state": "truncated", "deleted": deleted})
            }
            Err(error) => {
                tracing::error!(%error, "expired key reservation cleanup failed");
                serde_json::json!({"state": "error", "error": error.to_string()})
            }
        };
        let pruning_health = match prune {
            Ok(prune) => serde_json::json!({
                "finalized": prune.finalized_leaves.len(),
                "detached": prune.detached_count(),
                "dropped": prune.dropped_count(),
                "refusals": prune.refusals,
                "errors": prune.errors,
                "reach": match prune.reach {
                    PruneReach::WithinBounds => "within_bounds",
                    PruneReach::Truncated => "truncated",
                },
                "key_reservations": key_reservations,
            }),
            Err(error) => {
                tracing::error!(%error, "partition pruning failed");
                serde_json::json!({
                    "state": "error",
                    "error": error.to_string(),
                    "key_reservations": key_reservations,
                })
            }
        };
        health.write().await.partition_pruning = Some(pruning_health);
        *next_partition_maintenance = tokio::time::Instant::now()
            + Duration::from_secs(retention.partition_maintenance_interval_s);
    }
}

/// Recover the stale RUNNING tasks of one pass: the crash retry that the
/// task's policy admits, else the terminal failure (Python
/// `mark_stale_tasks_as_failed`, `core/brokers/postgres.py:3129-3423`).
///
/// - `horsies_stale_running_scan` reads at most `batch` candidates with no
///   lock.
/// - Each candidate gets one `horsies_finish_stale_attempt` with the retry
///   that its policy admits for `WORKER_CRASHED`, computed from the scanned
///   retry facts. The statement judges the row again under its locks: a row
///   that is no longer RUNNING or stale, or whose retry count moved, is
///   refused and nothing is written.
/// - A scheduled retry wakes its queue (best-effort, as Python).
///
/// Each statement runs alone on `pool`, checked out as reserved work, and
/// commits alone, so the candidates before a failure stay recovered. A
/// candidate whose statement fails is logged, and the pass goes on with the
/// next candidate. Returns the number of candidates retried or failed, and
/// logs Python's warning when the scan read a full batch.
///
/// Errors: the scan's [`crate::broker::BrokerError::Statement`] or
/// [`crate::broker::BrokerError::RowDecode`]; nothing was written.
pub async fn mark_stale_running_as_failed(
    pool: &CorePool,
    log: StatementLog,
    thresholds: StaleRunningThresholds,
    batch: RecoveryBatch,
) -> Result<u64, crate::broker::BrokerError> {
    let pass = stale_running_pass(pool, log, thresholds, batch).await?;
    match pass {
        StaleRunningPass::Complete { .. } => {}
        StaleRunningPass::ScanFull { scan_batch, .. } => tracing::warn!(
            target: STALE_RUNNING_PASS,
            scan_batch,
            "mark_stale_tasks_as_failed read a full scan batch of {scan_batch} candidates; the \
             rest is recovered next pass",
        ),
    }
    Ok(pass.recovered())
}

/// One stale RUNNING pass, as [`mark_stale_running_as_failed`] describes,
/// with whether the scan read a full batch. Logs each candidate's outcome
/// and failure; the caller reports the end of the pass.
///
/// Errors: the scan's [`crate::broker::BrokerError::Statement`] or
/// [`crate::broker::BrokerError::RowDecode`]; nothing was written.
pub(crate) async fn stale_running_pass(
    pool: &CorePool,
    log: StatementLog,
    thresholds: StaleRunningThresholds,
    batch: RecoveryBatch,
) -> Result<StaleRunningPass, crate::broker::BrokerError> {
    let candidates = scan_stale_running(pool, log, thresholds, batch).await?;
    let mut recovered: u64 = 0;
    for candidate in &candidates {
        let command = match stale_attempt_command(candidate, thresholds) {
            Ok(command) => command,
            Err(error) => {
                // Python skips the task when its crash result does not
                // serialize (`core/brokers/postgres.py:3351-3358`).
                tracing::error!(
                    target: STALE_RUNNING_PASS,
                    task_id = %candidate.task_id,
                    %error,
                    "failed to serialize the crash result of a stale RUNNING task",
                );
                continue;
            }
        };
        match finish_stale_attempt(pool, log, &command).await {
            Ok(outcome) => match after_stale_attempt(pool, log, &command, outcome).await {
                StaleRecovery::Recovered => recovered += 1,
                StaleRecovery::NotRecovered => {}
            },
            Err(error) => {
                tracing::error!(
                    target: STALE_RUNNING_PASS,
                    task_id = %candidate.task_id,
                    %error,
                    "failed to recover a stale RUNNING task",
                );
            }
        }
    }
    Ok(stale_running_end(candidates.len(), batch, recovered))
}

/// The end of a stale RUNNING pass whose scan read `scanned` candidates of
/// at most `batch`. Pure.
fn stale_running_end(scanned: usize, batch: RecoveryBatch, recovered: u64) -> StaleRunningPass {
    let scan_batch = batch.get().unsigned_abs();
    let full = usize::try_from(scan_batch).is_ok_and(|bound| scanned >= bound);
    match full {
        true => StaleRunningPass::ScanFull {
            recovered,
            scan_batch,
        },
        false => StaleRunningPass::Complete { recovered },
    }
}

/// Whether one stale candidate was retried or failed.
enum StaleRecovery {
    Recovered,
    NotRecovered,
}

/// The statement for one candidate: the retry that the policy admits, and
/// the crash result and reason (Python `core/brokers/postgres.py:3211-3350`).
///
/// Errors: the crash result does not serialize.
fn stale_attempt_command(
    candidate: &StaleCandidate,
    thresholds: StaleRunningThresholds,
) -> Result<FinishStaleAttempt, serde_json::Error> {
    let error_code = OperationalErrorCode::WorkerCrashed.to_string();
    let threshold_ms = thresholds.stale_after.get();
    let threshold_seconds = f64::from(threshold_ms) / 1000.0;
    let failed_reason = format!(
        "Worker process crashed (no runner heartbeat for {threshold_ms}ms = {threshold_seconds:.1}s)",
    );
    let task_error = TaskError {
        exception: None,
        error_code: OperationalErrorCode::WorkerCrashed.into(),
        message: Some(failed_reason.clone()),
        kind: TaskErrorKind::Plain,
        data: Some(serde_json::json!({
            "stale_threshold_ms": threshold_ms,
            "stale_threshold_seconds": threshold_seconds,
            "worker_pid": candidate.worker_pid,
            "worker_hostname": candidate.worker_hostname,
            "worker_id": candidate.claimed_by_worker_id,
            "started_at": candidate.started_at.to_rfc3339(),
            "detected_at": candidate.db_now.to_rfc3339(),
        })),
    };
    let result_json = serde_json::to_string(&TaskResult::<()>::Err(task_error))?;
    Ok(FinishStaleAttempt {
        task_id: candidate.task_id,
        thresholds,
        retry: stale_retry(candidate, &error_code),
        result_json,
        error_code,
        failed_reason: failed_reason.clone(),
        attempt_message: failed_reason.clone(),
        attempt_reason: failed_reason,
    })
}

/// The crash retry that the task's policy admits, from the scanned retry
/// facts (Python `check_retry_eligibility` and `calculate_retry_delay`). The
/// one retry rule of the core, shared with the worker: the stored policy
/// names the crash code by its value and retries remain. The deadline
/// (`good_until`) is judged by the statement on the database clock under
/// the lock.
fn stale_retry(candidate: &StaleCandidate, error_code: &str) -> StaleRetry {
    let stored_options = read_stored_options(candidate.task_options.as_deref());
    match admit_retry(
        candidate.retry_count,
        candidate.max_retries,
        stored_options.retry(),
        Some(error_code),
    ) {
        RetryAdmission::Admitted(rule) => StaleRetry::RetryAfter {
            expected_retry_count: candidate.retry_count,
            delay_seconds: rule.delay_seconds((candidate.retry_count + 1).unsigned_abs()),
        },
        RetryAdmission::RetriesSpent | RetryAdmission::NoPolicy | RetryAdmission::CodeNotListed => {
            StaleRetry::NoRetry
        }
    }
}

/// Log what the statement reported, and wake the queue of a scheduled
/// retry. A failed wake is logged: the retried row is PENDING, and the
/// queue's poll finds it.
async fn after_stale_attempt(
    pool: &CorePool,
    log: StatementLog,
    command: &FinishStaleAttempt,
    outcome: StaleAttemptOutcome,
) -> StaleRecovery {
    match outcome {
        StaleAttemptOutcome::FailureApplied { retry, after, .. } => {
            tracing::info!(
                target: STALE_RUNNING_PASS,
                task_id = %command.task_id,
                ?retry,
                ?after,
                "stale RUNNING task marked FAILED",
            );
            StaleRecovery::Recovered
        }
        StaleAttemptOutcome::RetryScheduled {
            retry_count,
            next_retry_at,
            queue_name,
            ..
        } => {
            tracing::info!(
                target: STALE_RUNNING_PASS,
                task_id = %command.task_id,
                retry_count,
                %next_retry_at,
                "stale RUNNING task scheduled for retry",
            );
            if let Err(error) = wake_retried_queue(pool, log, &queue_name, command.task_id).await {
                tracing::warn!(
                    target: STALE_RUNNING_PASS,
                    task_id = %command.task_id,
                    queue = %queue_name,
                    %error,
                    "the wake of a stale retry failed; the queue's poll finds the row",
                );
            }
            StaleRecovery::Recovered
        }
        StaleAttemptOutcome::Refused(refusal) => {
            tracing::warn!(
                target: STALE_RUNNING_PASS,
                task_id = %command.task_id,
                outcome = refusal.as_str(),
                ?refusal,
                retry = ?command.retry,
                "stale RUNNING recovery wrote nothing",
            );
            StaleRecovery::NotRecovered
        }
    }
}

/// The stale RUNNING thresholds of the recovery config.
///
/// Errors: [`ThresholdOutOfRange`] for a threshold above the `integer`
/// range of the recovery functions.
fn stale_running_thresholds(
    config: &RecoveryConfig,
) -> Result<StaleRunningThresholds, ThresholdOutOfRange> {
    Ok(StaleRunningThresholds {
        stale_after: ThresholdMs::from_millis(config.running_stale_threshold_ms)?,
        finalizing_stale_after: ThresholdMs::from_millis(config.finalizing_stale_threshold_ms)?,
    })
}

/// The reaper's stale recovery steps for tests that hold a sqlx pool: each
/// runs on a main core pool on the same database, built for the call and
/// closed after it. The thresholds are in seconds and the scan limit is an
/// `i64`, as the earlier sqlx forms took them, so a test call site keeps its
/// form.
#[cfg(test)]
pub(crate) mod on_test_pool {
    use super::{RecoveryBatch, StaleRunningThresholds, StatementLog, ThresholdMs};

    pub(crate) async fn mark_stale_running_as_failed(
        pool: &sqlx::PgPool,
        threshold_secs: f64,
        finalizing_threshold_secs: f64,
        scan_limit: i64,
    ) -> Result<u64, crate::broker::BrokerError> {
        let thresholds = StaleRunningThresholds {
            stale_after: threshold_of(threshold_secs),
            finalizing_stale_after: threshold_of(finalizing_threshold_secs),
        };
        let batch = i32::try_from(scan_limit)
            .ok()
            .and_then(RecoveryBatch::new)
            .expect("a test scan limit of 1 to 10000 rows");
        let core = crate::broker::pools::core_pool_for_test_pool(pool);
        let recovered =
            super::mark_stale_running_as_failed(&core, StatementLog::Off, thresholds, batch).await;
        core.close();
        recovered
    }

    /// [`super::requeue_stale_claimed`] on a core pool built from a test's
    /// sqlx pool, with the threshold in seconds.
    pub(crate) async fn requeue_stale_claimed(
        pool: &sqlx::PgPool,
        threshold_secs: f64,
    ) -> Result<u64, crate::broker::BrokerError> {
        let core = crate::broker::pools::core_pool_for_test_pool(pool);
        let requeued =
            super::requeue_stale_claimed(&core, StatementLog::Off, threshold_of(threshold_secs))
                .await;
        core.close();
        requeued
    }

    fn threshold_of(seconds: f64) -> ThresholdMs {
        let millis = std::time::Duration::from_secs_f64(seconds).as_millis();
        u64::try_from(millis)
            .ok()
            .and_then(|millis| ThresholdMs::from_millis(millis).ok())
            .expect("a test threshold within the integer range")
    }
}

/// Per-reaper state of the orphan audit: when it next runs, and how many
/// permanent failures in a row it had (for the log only; the audit never
/// stops on a live worker).
struct OrphanSweepState {
    next_audit: tokio::time::Instant,
    permanent_failures: u32,
}

impl Default for OrphanSweepState {
    fn default() -> Self {
        Self {
            next_audit: tokio::time::Instant::now(),
            permanent_failures: 0,
        }
    }
}

impl OrphanSweepState {
    fn schedule_if_due(&mut self, now: tokio::time::Instant, interval: Duration) -> bool {
        if now < self.next_audit {
            return false;
        }
        self.next_audit = now + interval;
        true
    }
}

#[cfg(test)]
mod orphan_audit_schedule_tests {
    use super::*;

    #[test]
    fn orphan_task_audit_uses_its_own_interval() {
        let now = tokio::time::Instant::now();
        let mut state = OrphanSweepState {
            next_audit: now,
            permanent_failures: 0,
        };
        let interval = Duration::from_secs(60);
        assert!(state.schedule_if_due(now, interval));
        assert!(!state.schedule_if_due(now + Duration::from_secs(59), interval));
        assert!(state.schedule_if_due(now + Duration::from_secs(60), interval));
    }

    /// The orphan audit never stops on a live worker: permanent failures in
    /// a row do not change its schedule (Python stops it after 3; the core
    /// does not).
    #[test]
    fn orphan_audit_stays_scheduled_after_permanent_failures() {
        let now = tokio::time::Instant::now();
        let mut state = OrphanSweepState {
            next_audit: now,
            permanent_failures: u32::MAX,
        };
        let interval = Duration::from_secs(60);
        assert!(state.schedule_if_due(now, interval));
        assert!(state.schedule_if_due(now + interval, interval));
    }

    #[test]
    fn transient_non_lock_failure_is_an_error_in_the_health_snapshot() {
        let error = crate::broker::BrokerError::Database(sqlx::Error::PoolTimedOut);
        let (kind, snapshot) = orphan_audit_failure_snapshot(&error, std::time::Instant::now());

        assert_eq!(kind, OrphanAuditFailureKind::Transient);
        assert_eq!(snapshot["state"], "error");
        assert_eq!(snapshot["rows_selected"], 0);
        assert_eq!(snapshot["candidates_returned"], 0);
        assert_eq!(snapshot["refusals"], 0);
        assert_eq!(snapshot["errors"], 1);
    }
}

#[derive(Debug, serde::Serialize)]
struct OrphanTaskAuditReport {
    state: &'static str,
    rows_selected: u32,
    candidates_returned: u32,
    cancelled: u32,
    duration_ms: u64,
    refusals: u32,
    errors: u32,
}

impl OrphanTaskAuditReport {
    /// The health report of a page that committed, timed from `started`.
    fn ready(page: OrphanAuditPage, started: std::time::Instant) -> Self {
        Self {
            state: "ready",
            rows_selected: page.rows_selected,
            candidates_returned: page.candidates_returned,
            cancelled: page.cancelled,
            duration_ms: elapsed_millis(started),
            refusals: 0,
            errors: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OrphanAuditFailureKind {
    Refusal,
    Transient,
    Permanent,
}

fn orphan_audit_failure_snapshot(
    error: &crate::broker::BrokerError,
    started: std::time::Instant,
) -> (OrphanAuditFailureKind, serde_json::Value) {
    let kind = match (error.is_lock_not_available(), error.is_retryable()) {
        (true, _) => OrphanAuditFailureKind::Refusal,
        (false, true) => OrphanAuditFailureKind::Transient,
        (false, false) => OrphanAuditFailureKind::Permanent,
    };
    let (state, refusals, errors) = match kind {
        OrphanAuditFailureKind::Refusal => ("refused", 1, 0),
        OrphanAuditFailureKind::Transient | OrphanAuditFailureKind::Permanent => ("error", 0, 1),
    };
    (
        kind,
        serde_json::json!({
            "state": state,
            "rows_selected": 0,
            "candidates_returned": 0,
            "cancelled": 0,
            "duration_ms": elapsed_millis(started),
            "refusals": refusals,
            "errors": errors,
            "error": error.to_string(),
        }),
    )
}

fn elapsed_millis(started: std::time::Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Requeue stale CLAIMED tasks back to PENDING (Python
/// `requeue_stale_claimed`, `core/brokers/postgres.py:3505-3530`): one
/// [`requeue_stale_claimed_pass`] on `pool`. Logs a warning when the pass
/// reached its bound. Returns the number of requeued rows.
///
/// Errors: the failed batch's [`crate::broker::BrokerError`]; the batches
/// before it stay committed.
pub async fn requeue_stale_claimed(
    pool: &CorePool,
    log: StatementLog,
    stale_after: ThresholdMs,
) -> Result<u64, crate::broker::BrokerError> {
    match requeue_stale_claimed_pass(pool, log, stale_after).await {
        Ok(StaleClaimsRequeuePass::Drained { requeued }) => Ok(requeued),
        Ok(StaleClaimsRequeuePass::BoundReached { requeued }) => {
            tracing::warn!(
                requeued,
                batches = STALE_CLAIMED_REQUEUE_MAX_BATCHES_PER_PASS,
                "stale CLAIMED requeue reached its per-pass bound; the next pass goes on",
            );
            Ok(requeued)
        }
        Err(failed) => Err(failed.error),
    }
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
    use super::on_test_pool::{mark_stale_running_as_failed, requeue_stale_claimed};
    use super::*;
    use crate::core::history::maintenance::coverage::{
        ensure_startup_coverage, StartupCoverageOutcome,
    };
    use crate::core::history::reads::publisher::StagedLoaderPublisher;
    use crate::core::registry::workflow::WorkflowSpecRegistry;
    use serial_test::serial;
    use uuid::Uuid;

    /// The scan limit of a reaper pass, as the test helper takes it.
    const STALE_RUNNING_SCAN_LIMIT: i64 = STALE_RUNNING_SCAN_BATCH.get() as i64;

    /// Batch size for direct retention-statement tests (the default
    /// `RecoveryConfig::retention_delete_batch_size`).
    const TEST_RETENTION_BATCH: i64 = 500;

    #[tokio::test]
    async fn gate_runs_ungated_only_for_static_single_connection_capacity() {
        let single = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy(&crate::test_support::server_url())
            .unwrap();
        assert!(matches!(
            acquire_gate(&single, advisory_key_reaper()).await,
            GatePass::Ungated
        ));

        let unavailable = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect_lazy(&crate::test_support::server_url())
            .unwrap();
        unavailable.close().await;
        assert!(matches!(
            acquire_gate(&unavailable, advisory_key_reaper()).await,
            GatePass::Skip
        ));
    }

    /// #772: the reaper's gate and pass run on the worker reserve, so a pass
    /// completes while every connection of the runtime pool is held.
    #[tokio::test]
    #[serial]
    async fn a_reaper_pass_completes_on_the_reserve_while_the_runtime_pool_is_exhausted() {
        let seed = test_pool().await;
        let task_id = Uuid::new_v4();
        insert_stale_running_task(&seed, task_id, "NULL").await;

        let runtime = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_millis(500))
            .connect_with((*seed.connect_options()).clone())
            .await
            .unwrap();
        let broker = crate::broker::PostgresBroker::from_pool(runtime.clone());
        let held = [
            runtime.acquire().await.unwrap(),
            runtime.acquire().await.unwrap(),
        ];
        assert!(
            matches!(
                acquire_gate(&runtime, advisory_key_reaper()).await,
                GatePass::Skip
            ),
            "the exhausted runtime pool cannot take the gate",
        );

        let GatePass::Held(gate) = acquire_gate(broker.reserve_pool(), advisory_key_reaper()).await
        else {
            panic!("the reserve takes the reaper gate");
        };
        let mut recovery = RecoveryConfig::default();
        recovery.auto_requeue_stale_claimed = false;
        recovery.auto_terminate_orphaned_workflow_tasks = false;
        recovery.auto_fail_stale_running = true;
        let health = new_reaper_health();
        let mut next_retention_cleanup = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut next_partition_maintenance = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut orphan_state = OrphanSweepState::default();
        let pass = tokio::time::timeout(
            Duration::from_secs(30),
            run_reaper_pass(
                &broker,
                broker.reserve_pool(),
                broker.reserve_maintenance_pool(),
                &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
                &recovery,
                &RetentionConfig::default(),
                &health,
                &mut next_retention_cleanup,
                &mut next_partition_maintenance,
                &mut orphan_state,
            ),
        )
        .await;
        assert!(pass.is_ok(), "the reaper pass completes on the reserve");
        release_gate(gate).await;
        drop(held);

        let failed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM horsies_task_history
             WHERE task_id = $1 AND status = 'FAILED' AND terminalization_kind = 'FAIL_STALE'",
        )
        .bind(task_id)
        .fetch_one(&seed)
        .await
        .unwrap();
        assert_eq!(failed, 1, "the pass failed the stale RUNNING task");

        sqlx::query("DELETE FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .execute(&seed)
            .await
            .unwrap();
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
                id, task_name, queue_name, priority, input, task_options, execution,
                retention_class_key, sent_at, created_at,
                command_fingerprint_version, command_fingerprint, idempotency_key_digest,
                status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
                started_at, finalizing_at, finalizing_by_worker_id,
                worker_pid, worker_hostname, worker_process_name,
                retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
                updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
            ) VALUES (
                $1, 'reaper_test', 'default', 100, '{{}}', NULL, 'ISOLATED',
                'standard_30d', NOW() - INTERVAL '1 hour', NOW(),
                1, decode(repeat('00', 32), 'hex'), NULL,
                'RUNNING', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour', 'worker-1', NULL,
                NOW() - INTERVAL '1 hour', {finalizing_at_sql}, NULL,
                4242, 'reaper-host', 'reaper-process',
                0, 0, NULL, NULL, FALSE,
                NOW(), FALSE, NULL, NULL
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
                id, task_name, queue_name, priority, input, task_options, execution,
                retention_class_key, sent_at, created_at,
                command_fingerprint_version, command_fingerprint, idempotency_key_digest,
                status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
                started_at, finalizing_at, finalizing_by_worker_id,
                worker_pid, worker_hostname, worker_process_name,
                retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
                updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
            ) VALUES (
                $1, 'ret_task', 'default', 100, 'null', NULL, 'ISOLATED',
                'standard_30d', NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours',
                1, decode(repeat('00', 32), 'hex'), NULL,
                'RUNNING', NOW() - INTERVAL '2 hours', NULL, NULL, NULL,
                NOW() - INTERVAL '2 hours', NULL, NULL,
                NULL, NULL, NULL,
                0, 0, NULL, NULL, FALSE,
                NOW() - INTERVAL '2 hours', FALSE, NULL, NULL
            )",
        )
        .bind(task_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, task_id,
                created_at, max_retries, execution, retention_class_key
            ) VALUES (
                'NONE', $1, $2, 0, 'node_0', 'ret_task', 'null', 'default', 100, '{}', FALSE, 'all',
                'ENQUEUED', FALSE, $3, NOW() - INTERVAL '2 hours', 0, 'ISOLATED',
                'standard_30d'
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
        let outcomes = crate::broker::terminalization::on_test_pool::terminalize(
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
                        downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name,
                        priority, dependencies, allow_failed_deps, join_type, status,
                        is_subworkflow, created_at, max_retries, execution,
                        retention_class_key
                    ) VALUES (
                        'NONE', $1, $2, $3, 'node_' || $3, 'ret_budget_task', 'null', 'default', 100,
                        '{}', FALSE, 'all', 'COMPLETED', FALSE, NOW() - INTERVAL '2 hours', 0,
                        'ISOLATED', 'standard_30d'
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

    // --- stale CLAIMED requeue and stale RUNNING retry (baseline seeds) ----

    /// A `reaper_test` task with every column of the baseline task row named.
    /// `stale_since` is how long ago it was claimed and started.
    async fn seed_baseline_task(
        pool: &PgPool,
        task_id: Uuid,
        status: &str,
        is_workflow_task: bool,
        max_retries: i32,
        task_options: Option<&str>,
    ) {
        let fingerprint: Option<Vec<u8>> = match is_workflow_task {
            true => None,
            false => Some(vec![9_u8; 32]),
        };
        let fingerprint_version: Option<i16> = match is_workflow_task {
            true => None,
            false => Some(1),
        };
        sqlx::query(
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
                $1, 'reaper_test', 'default', 100, '{}', $2, 'ISOLATED',
                'forever', NOW() - interval '2 hours', NOW() - interval '2 hours',
                $3, $4, NULL,
                $5, NOW() - interval '2 hours', NOW() - interval '1 hour', 'worker-1',
                CASE WHEN $5 = 'CLAIMED' THEN NOW() + interval '1 minute' END,
                CASE WHEN $5 = 'RUNNING' THEN NOW() - interval '1 hour' END, NULL, NULL,
                CASE WHEN $5 = 'RUNNING' THEN 4242 END,
                CASE WHEN $5 = 'RUNNING' THEN 'reaper-host' END,
                CASE WHEN $5 = 'RUNNING' THEN 'reaper-process' END,
                0, $6, NULL, NULL, $7,
                NOW(), FALSE, NULL, NULL
             )",
        )
        .bind(task_id)
        .bind(task_options)
        .bind(fingerprint_version)
        .bind(fingerprint)
        .bind(status)
        .bind(max_retries)
        .bind(is_workflow_task)
        .execute(pool)
        .await
        .expect("seed baseline task");
    }

    /// Python `core/brokers/postgres.py` `REQUEUE_STALE_CLAIMED_SQL` and
    /// `test_orphaned_workflow_task_repro.py::test_requeue_skips_orphaned_workflow_task`:
    /// staleness is judged by claimer beats, an orphaned workflow task is
    /// never requeued, and the requeue clears the finalizing marker.
    #[tokio::test]
    #[serial]
    async fn stale_claimed_requeue_follows_claimer_beats_and_skips_orphans() {
        let pool = test_pool().await;
        let silent = Uuid::new_v4();
        let beating = Uuid::new_v4();
        let orphan = Uuid::new_v4();
        let marked = Uuid::new_v4();
        for (task_id, is_workflow_task) in [
            (silent, false),
            (beating, false),
            (orphan, true),
            (marked, false),
        ] {
            seed_baseline_task(&pool, task_id, "CLAIMED", is_workflow_task, 0, None).await;
        }
        sqlx::query(
            "INSERT INTO horsies_heartbeats (task_id, role, sent_at, sender_id, hostname, pid)
             VALUES ($1, 'claimer', NOW(), 'worker-1', 'reaper-host', 4242)",
        )
        .bind(beating)
        .execute(&pool)
        .await
        .expect("fresh claimer beat");
        sqlx::query(
            "UPDATE horsies_tasks
             SET finalizing_at = NOW() - interval '1 hour', finalizing_by_worker_id = 'worker-1'
             WHERE id = $1",
        )
        .bind(marked)
        .execute(&pool)
        .await
        .expect("stamp marker");

        let requeued = requeue_stale_claimed(&pool, 60.0).await.expect("requeue");

        // The shared test database can hold stale claims of other modules.
        assert!(requeued >= 2, "requeued {requeued}");
        assert_eq!(task_status(&pool, silent).await, "PENDING");
        assert_eq!(task_status(&pool, beating).await, "CLAIMED");
        assert_eq!(task_status(&pool, orphan).await, "CLAIMED");
        assert_eq!(task_status(&pool, marked).await, "PENDING");
        let (claimed_at, owner, marker): (
            Option<chrono::DateTime<chrono::Utc>>,
            Option<String>,
            Option<chrono::DateTime<chrono::Utc>>,
        ) = sqlx::query_as(
            "SELECT claimed_at, claimed_by_worker_id, finalizing_at FROM horsies_tasks WHERE id = $1",
        )
        .bind(marked)
        .fetch_one(&pool)
        .await
        .expect("requeued row");
        assert_eq!((claimed_at, owner, marker), (None, None, None));
        sqlx::query("DELETE FROM horsies_heartbeats WHERE task_id = $1")
            .bind(beating)
            .execute(&pool)
            .await
            .expect("delete beat");
    }

    /// Python `test_task_attempt_history.py::test_stale_cleanup_with_retry_policy_schedules_retry`
    /// and `::test_stale_cleanup_writes_failed_attempt_worker_crashed`: a
    /// policy that names WORKER_CRASHED retries a stale RUNNING task (the
    /// built-in code compares by value, horsies-rust #68), the attempt
    /// outcome is FAILED, and the retried row keeps no claim or run field.
    #[tokio::test]
    #[serial]
    async fn a_stale_running_task_retries_when_its_policy_names_the_crash() {
        let pool = test_pool().await;
        let task_id = Uuid::new_v4();
        let task_options = serde_json::json!({
            "retry_policy": {
                "max_retries": 1,
                "intervals": [5],
                "backoff_strategy": "fixed",
                "jitter": false,
                "max_delay_seconds": null,
                "auto_retry_for": ["WORKER_CRASHED"]
            }
        })
        .to_string();
        seed_baseline_task(&pool, task_id, "RUNNING", false, 1, Some(&task_options)).await;

        let processed = mark_stale_running_as_failed(&pool, 1.0, 300.0, STALE_RUNNING_SCAN_LIMIT)
            .await
            .expect("stale recovery");

        // The shared test database can hold stale rows of other modules.
        assert!(processed >= 1, "processed {processed}");
        let row: serde_json::Value =
            sqlx::query_scalar("SELECT to_jsonb(t) FROM horsies_tasks t WHERE id = $1")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .expect("retried row");
        assert_eq!(row["status"], "PENDING");
        assert_eq!(row["retry_count"], 1);
        for cleared in [
            "claimed_at",
            "claimed_by_worker_id",
            "claim_expires_at",
            "started_at",
            "worker_pid",
            "worker_hostname",
            "worker_process_name",
            "finalizing_at",
            "finalizing_by_worker_id",
        ] {
            assert!(row[cleared].is_null(), "a stale retry must clear {cleared}");
        }
        let attempts: Vec<(i32, String, bool, Option<String>)> = sqlx::query_as(
            "SELECT attempt, outcome::text, will_retry, error_code
             FROM horsies_task_attempts WHERE task_id = $1",
        )
        .bind(task_id)
        .fetch_all(&pool)
        .await
        .expect("attempts");
        assert_eq!(
            attempts,
            vec![(
                1,
                "FAILED".to_owned(),
                true,
                Some("WORKER_CRASHED".to_owned())
            )]
        );
        sqlx::query("DELETE FROM horsies_task_attempts WHERE task_id = $1")
            .bind(task_id)
            .execute(&pool)
            .await
            .expect("delete attempts");
    }
}

#[cfg(test)]
mod reaper_loop_tests;

#[cfg(test)]
mod class_coverage_loop_tests;

#[cfg(test)]
mod retention_ports;

#[cfg(test)]
mod reaper_pass_ports;

#[cfg(test)]
mod stale_running_end_tests {
    use super::{stale_running_end, RecoveryBatch, StaleRunningPass};

    fn batch(rows: i32) -> RecoveryBatch {
        RecoveryBatch::new(rows).expect("a recovery batch")
    }

    /// Python `mark_stale_tasks_as_failed`: a scan that read its whole batch
    /// warns that the rest is recovered next pass; a shorter scan does not.
    #[test]
    fn a_scan_of_a_whole_batch_is_full_and_a_shorter_one_is_complete() {
        assert_eq!(
            stale_running_end(1_000, batch(1_000), 7),
            StaleRunningPass::ScanFull {
                recovered: 7,
                scan_batch: 1_000
            }
        );
        assert_eq!(
            stale_running_end(999, batch(1_000), 7),
            StaleRunningPass::Complete { recovered: 7 }
        );
        assert_eq!(
            stale_running_end(0, batch(1), 0),
            StaleRunningPass::Complete { recovered: 0 }
        );
        assert_eq!(StaleRunningPass::Complete { recovered: 3 }.recovered(), 3);
        assert_eq!(
            StaleRunningPass::ScanFull {
                recovered: 4,
                scan_batch: 1
            }
            .recovered(),
            4
        );
    }
}
