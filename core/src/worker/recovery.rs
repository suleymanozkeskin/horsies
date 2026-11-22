use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::broker::core_rows::{one_row, query_in, scalar, StatementLog};
use crate::broker::expiry_and_orphans::{
    cancel_orphan_page, expire_pending_pass, OrphanAuditPage, PendingExpiryFailed,
    PendingExpiryPass, PENDING_EXPIRY_MAX_BATCHES_PER_PASS,
};
use crate::broker::stale_recovery::{
    finish_stale_attempt, requeue_stale_claimed_pass, scan_stale_running, wake_retried_queue,
    FinishStaleAttempt, RecoveredTask, RecoveryBatch, StaleCandidate, StaleClaimsRequeuePass,
    StaleEvidence, StaleRetry, StaleRunningPass, StaleRunningThresholds, ThresholdMs,
    ThresholdOutOfRange, STALE_CLAIMED_REQUEUE_MAX_BATCHES_PER_PASS, STALE_RUNNING_SCAN_BATCH,
};
use crate::broker::CoveragePassError;
use crate::broker::{BrokerError, PostgresBroker, WorkflowRetentionPass};
use crate::core::codec::task_result_to_json_string;
use crate::core::config::recovery::RecoveryConfig;
use crate::core::config::retention::RetentionConfig;
use crate::core::history::errors::HistoryError;
use crate::core::history::identity::reservations::{
    cleanup_expired_reservations_pass, ReservationCleanupPass, RESERVATION_CLEANUP_BATCHES_PER_PASS,
};
use crate::core::history::maintenance::coverage::{
    CoverageOutcome, CoverageRequest, CoverageSerialization,
};
use crate::core::history::maintenance::pruning::PruneReach;
use crate::core::lifecycle::StaleAttemptOutcome;
use crate::core::python_text::python_isoformat_utc;
use crate::core::{OperationalErrorCode, TaskError, TaskErrorKind, TaskResult};
use crate::db::{
    CheckoutClass, CorePool, IsolationLevel, ParameterType, RollbackOutcome, StatementBound,
    Transaction, TypedStatement,
};
use crate::log_targets::STALE_RUNNING_PASS;
use crate::telemetry::labels::{RecoveryAction, RecoveryStep};
use crate::telemetry::worker::WorkerTelemetry;

use crate::worker::class_coverage_log::{check_class_coverage, ClassCoverageLog};
use crate::worker::executor::slots::dispatch::attempt_of;
use crate::worker::executor::slots::AttemptLog;
use crate::worker::liveness::OWNER_GONE_SPAN;
use crate::worker::retry::{admit_retry, read_stored_options, RetryAdmission};
use crate::workflow_engine::child_spec::ChildSpecSource;

pub(crate) mod failure_log;
mod maintenance_due;
pub(crate) mod owner_gone;

use failure_log::RecoveryFailures;
use maintenance_due::MaintenanceDue;
pub(crate) use owner_gone::{forget_ended_workers, owner_gone_step, UnheldOwners};

#[cfg(test)]
mod p7_maintenance_tests {
    use super::*;
    use crate::broker::core_rows::StatementLog;
    use crate::broker::pools::core_pool_for_test_url;
    use crate::core::history::commands::{LeafBounds, LeafRef};
    use crate::core::history::ddl::classes::finite_class_parent_name;
    use crate::core::history::ddl::runtime_names::daily_leaf_name;
    use crate::core::history::heartbeats::partitioning::hourly_leaf_ref;
    use crate::core::history::maintenance::coverage::{
        ensure_startup_coverage_in_pool, StartupCoverageOutcome,
    };
    use crate::core::history::outcomes::LeafCreation;
    use crate::core::history::periods::RetentionDays;
    use crate::core::history::reads::publisher::StagedLoaderPublisher;
    use crate::core::lifecycle::TerminalizationOutcome;
    use crate::telemetry::registry::OptionalCounter;
    use crate::test_support::history::{
        create_heartbeat_leaf_of, create_history_leaf_of, database_now, register_period_class,
    };
    use crate::test_support::sql::{self, TestPool};
    use crate::test_support::terminal_writes;
    use chrono::Timelike;
    use uuid::Uuid;

    async fn seed_terminal_workflow_with_pending(
        pool: &TestPool,
        workflow_id: Uuid,
        task_id: Uuid,
    ) {
        sql::query(
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
        sql::query(
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
        sql::query(
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
    async fn retention_owns_only_worker_state_and_workflow_rows() {
        let pool = crate::test_support::migrated_pool().await;
        let outcome = ensure_startup_coverage_in_pool(
            &core_pool_for_test_url(pool.url()),
            StatementLog::Off,
            2,
            2,
            &[],
        )
        .await
        .unwrap();
        assert!(matches!(outcome, StartupCoverageOutcome::Ready(_)));

        let workflow_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        let heartbeat_task_id = Uuid::new_v4();
        let worker_id = format!("p7-retention-{}", Uuid::new_v4());
        seed_terminal_workflow_with_pending(&pool, workflow_id, task_id).await;
        // Every NOT NULL column of the baseline table is named: it has no
        // column defaults.
        sql::query(
            "INSERT INTO horsies_worker_states (
                worker_id, snapshot_at, hostname, pid, processes,
                max_claim_batch, max_claim_per_worker, queues,
                queue_priorities, queue_max_concurrency, recovery_config,
                tasks_running, tasks_claimed, worker_started_at,
                slots_down, class_capacity, telemetry
             ) VALUES ($1, NOW() - INTERVAL '48 hours', 'host', 1, 1,
                       1, 1, '{default}',
                       '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
                       0, 0, NOW() - INTERVAL '48 hours',
                       '[]'::jsonb, '{}'::jsonb, jsonb_build_object('endpoint', jsonb_build_object('state', 'off')))",
        )
        .bind(&worker_id)
        .execute(&pool)
        .await
        .unwrap();
        sql::query(
            "INSERT INTO horsies_heartbeats
                (task_id, sender_id, role, sent_at, hostname, pid)
             VALUES ($1, $2, 'runner', NOW(), 'host', 1)",
        )
        .bind(heartbeat_task_id)
        .bind(&worker_id)
        .execute(&pool)
        .await
        .unwrap();

        let config = RetentionConfig {
            worker_state_retention_hours: Some(1),
            terminal_record_retention_hours: Some(1),
            ..RetentionConfig::default()
        };
        run_retention_cleanup(
            &PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
            &config,
            &mut RecoveryFailures::new(OptionalCounter::NotRecorded),
        )
        .await;

        let state: (i64, i64, i64, i64, i64) = sql::query_as(
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

        sql::query("DELETE FROM horsies_heartbeats WHERE task_id = $1")
            .bind(heartbeat_task_id)
            .execute(&pool)
            .await
            .unwrap();
        sql::query("DELETE FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// A host that slept seven hours, past the default heartbeat horizon of
    /// six: the last maintenance was seven hours ago on the wall clock and
    /// now on the monotonic clock, which stops while the host sleeps. The
    /// pass runs the partition maintenance (its coverage health is set) and
    /// sets the next deadline one interval ahead.
    #[tokio::test]
    async fn a_pass_after_a_host_sleep_runs_the_partition_maintenance() {
        let pool = crate::test_support::migrated_pool().await;
        let recovery = RecoveryConfig {
            auto_requeue_stale_claimed: false,
            auto_fail_stale_running: false,
            auto_terminate_orphaned_workflow_tasks: false,
            ..RecoveryConfig::default()
        };
        let retention = RetentionConfig {
            worker_state_retention_hours: None,
            terminal_record_retention_hours: None,
            ..RetentionConfig::default()
        };
        let interval = Duration::from_secs(retention.partition_maintenance_interval_s);
        let slept = Duration::from_secs(7 * 3600);
        let health = new_reaper_health();
        let mut next_retention_cleanup = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut next_partition_maintenance = MaintenanceDue::after_from(
            tokio::time::Instant::now(),
            std::time::SystemTime::now() - slept,
            interval,
        );
        let mut orphan_state = OrphanSweepState::default();

        run_reaper_pass(
            &PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
            crate::test_support::child_specs::no_child_specs().as_ref(),
            &recovery,
            &retention,
            &health,
            &mut next_retention_cleanup,
            &mut next_partition_maintenance,
            &mut orphan_state,
            &mut UnheldOwners::default(),
            &mut RecoveryFailures::new(OptionalCounter::NotRecorded),
            &WorkerTelemetry::Off,
        )
        .await;

        let snapshot = health.read().await.clone();
        assert_eq!(
            snapshot
                .partition_coverage
                .as_ref()
                .and_then(|value| value["state"].as_str()),
            Some("ensured"),
        );
        assert!(!next_partition_maintenance.is_due());
    }

    #[tokio::test]
    async fn periodic_maintenance_publishes_all_health_and_contains_coverage_failure() {
        let pool = crate::test_support::migrated_pool().await;
        let recovery = RecoveryConfig {
            auto_requeue_stale_claimed: false,
            auto_fail_stale_running: false,
            auto_terminate_orphaned_workflow_tasks: false,
            ..RecoveryConfig::default()
        };
        let retention = RetentionConfig {
            worker_state_retention_hours: None,
            terminal_record_retention_hours: None,
            // Below the coverage floor of two future leaves: the coverage call
            // fails before any write. Config validation refuses this value, so
            // the pass sees it only when a config bypasses validation.
            history_leaf_horizon_days: 1,
            ..RetentionConfig::default()
        };
        let health = new_reaper_health();
        let mut next_retention_cleanup = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut next_partition_maintenance = MaintenanceDue::now();
        let mut orphan_state = OrphanSweepState::default();

        run_reaper_pass(
            &PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
            crate::test_support::child_specs::no_child_specs().as_ref(),
            &recovery,
            &retention,
            &health,
            &mut next_retention_cleanup,
            &mut next_partition_maintenance,
            &mut orphan_state,
            &mut UnheldOwners::default(),
            &mut RecoveryFailures::new(OptionalCounter::NotRecorded),
            &WorkerTelemetry::Off,
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
            !next_partition_maintenance.is_due(),
            "the periodic maintenance tick advances after contained failures",
        );
    }

    /// The pass requeues stale CLAIMED rows before it expires PENDING rows,
    /// so a stale claim whose `good_until` has passed is requeued and expired
    /// in one pass. Expiry first would leave it PENDING after the pass.
    #[tokio::test]
    async fn a_stale_claim_past_its_deadline_is_requeued_then_expired_in_one_pass() {
        let pool = crate::test_support::migrated_pool().await;
        let coverage =
            crate::core::history::maintenance::coverage::ensure_partition_coverage_in_pool(
                &crate::broker::pools::core_pool_for_test_url(pool.url()),
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
        let recovery = RecoveryConfig {
            auto_requeue_stale_claimed: true,
            auto_fail_stale_running: false,
            auto_terminate_orphaned_workflow_tasks: false,
            ..RecoveryConfig::default()
        };
        let retention = RetentionConfig::default();
        let health = new_reaper_health();
        let mut next_retention_cleanup = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut next_partition_maintenance = MaintenanceDue::after(Duration::from_secs(60));
        let mut orphan_state = OrphanSweepState::default();

        run_reaper_pass(
            &PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
            crate::test_support::child_specs::no_child_specs().as_ref(),
            &recovery,
            &retention,
            &health,
            &mut next_retention_cleanup,
            &mut next_partition_maintenance,
            &mut orphan_state,
            &mut UnheldOwners::default(),
            &mut RecoveryFailures::new(OptionalCounter::NotRecorded),
            &WorkerTelemetry::Off,
        )
        .await;

        let live: Option<String> =
            sql::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
                .bind(task_id)
                .fetch_optional(&pool)
                .await
                .expect("read the live row");
        assert_eq!(live, None, "the expired task leaves the live table");
        let history: String =
            sql::query_scalar("SELECT status FROM horsies_task_history WHERE task_id = $1")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .expect("read the history row");
        assert_eq!(history, "EXPIRED");
        sql::query("DELETE FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .execute(&pool)
            .await
            .expect("remove the history row");
    }

    #[tokio::test]
    async fn periodic_maintenance_runs_non_phase2_workflow_recovery() {
        let pool = crate::test_support::migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        sql::query(
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
        let recovery = RecoveryConfig {
            auto_requeue_stale_claimed: false,
            auto_fail_stale_running: false,
            auto_terminate_orphaned_workflow_tasks: false,
            ..RecoveryConfig::default()
        };
        let retention = RetentionConfig::default();
        let health = new_reaper_health();
        let mut next_retention_cleanup = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut next_partition_maintenance = MaintenanceDue::after(Duration::from_secs(60));
        let mut orphan_state = OrphanSweepState::default();

        run_reaper_pass(
            &PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
            crate::test_support::child_specs::no_child_specs().as_ref(),
            &recovery,
            &retention,
            &health,
            &mut next_retention_cleanup,
            &mut next_partition_maintenance,
            &mut orphan_state,
            &mut UnheldOwners::default(),
            &mut RecoveryFailures::new(OptionalCounter::NotRecorded),
            &WorkerTelemetry::Off,
        )
        .await;

        let status: String =
            sql::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(workflow_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "FAILED");
        let snapshot = health.read().await.clone();
        let report = snapshot.workflow_recovery.unwrap();
        assert!(report["case4_orphaned_failed"].as_u64().unwrap() >= 1);
        assert_eq!(report["errors"], 0);

        sql::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn periodic_maintenance_creates_ahead_prunes_both_partition_families_and_contains_a_blocker(
    ) {
        let pool = crate::test_support::migrated_pool().await;
        let recovery = RecoveryConfig {
            auto_requeue_stale_claimed: false,
            auto_fail_stale_running: false,
            auto_terminate_orphaned_workflow_tasks: false,
            ..RecoveryConfig::default()
        };
        let mut retention = RetentionConfig {
            worker_state_retention_hours: Some(1),
            terminal_record_retention_hours: Some(1),
            ..RetentionConfig::default()
        };
        // Two short periods that no other test of this database registers.
        let blocked_period = RetentionDays::parse_period(chrono::Duration::days(1)).unwrap();
        let valid_period = RetentionDays::parse_period(chrono::Duration::days(2)).unwrap();
        let blocked_class_key = blocked_period.class_key();
        let valid_class_key = valid_period.class_key();
        let blocked_class = blocked_class_key.as_str();
        let valid_class = valid_class_key.as_str();

        // Settle all pre-existing classes at the same horizons so the pass's
        // creation counts are attributable to the two classes introduced here.
        let baseline_outcome = ensure_startup_coverage_in_pool(
            &core_pool_for_test_url(pool.url()),
            StatementLog::Off,
            retention.history_leaf_horizon_days,
            retention.heartbeat_leaf_horizon_hours,
            &[],
        )
        .await
        .unwrap();
        assert!(matches!(baseline_outcome, StartupCoverageOutcome::Ready(_)));

        let mut setup = pool.begin().await.unwrap();
        for period in [blocked_period, valid_period] {
            let _registered = register_period_class(&mut setup, period).await;
        }
        let now = database_now(&mut setup).await;
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
            // A created leaf publishes the readers, as the coverage pass does
            // after its leaves.
            match create_history_leaf_of(&mut setup, &leaf).await.unwrap() {
                LeafCreation::Created { .. } => {
                    let _republished = StagedLoaderPublisher.republish(&mut setup).await.unwrap();
                }
                _not_created => {}
            }
            history_leaves.push(leaf);
        }
        let old_hour = (now - chrono::Duration::hours(12))
            .with_minute(0)
            .and_then(|value| value.with_second(0))
            .and_then(|value| value.with_nanosecond(0))
            .unwrap();
        let heartbeat_leaf = hourly_leaf_ref(old_hour).unwrap();
        let _created = create_heartbeat_leaf_of(&mut setup, &heartbeat_leaf)
            .await
            .unwrap();

        let workflow_id = Uuid::new_v4();
        let node_id = Uuid::new_v4();
        let blocker_task_id = Uuid::new_v4();
        sql::query(
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
        sql::query(
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
        sql::query(
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
            sql::query(statement).execute(&mut *setup).await.unwrap();
        }
        setup.commit().await.unwrap();

        retention
            .retention_periods
            .extend([blocked_period.as_duration(), valid_period.as_duration()]);
        let health = new_reaper_health();
        let mut next_retention_cleanup = tokio::time::Instant::now();
        let mut next_partition_maintenance = MaintenanceDue::now();
        let mut orphan_state = OrphanSweepState::default();
        run_reaper_pass(
            &PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
            crate::test_support::child_specs::no_child_specs().as_ref(),
            &recovery,
            &retention,
            &health,
            &mut next_retention_cleanup,
            &mut next_partition_maintenance,
            &mut orphan_state,
            &mut UnheldOwners::default(),
            &mut RecoveryFailures::new(OptionalCounter::NotRecorded),
            &WorkerTelemetry::Off,
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

        let physical: (bool, bool, bool, i64) = sql::query_as(
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
                let exists: bool = sql::query_scalar("SELECT to_regclass($1) IS NOT NULL")
                    .bind(leaf_name)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                assert!(exists, "periodic pass omitted create-ahead for {class_key}");
            }
        }

        let mut cleanup = pool.begin().await.unwrap();
        sql::query("DELETE FROM horsies_workflow_phase2_pending WHERE task_id = $1")
            .bind(blocker_task_id)
            .execute(&mut *cleanup)
            .await
            .unwrap();
        sql::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(workflow_id)
            .execute(&mut *cleanup)
            .await
            .unwrap();
        sql::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&mut *cleanup)
            .await
            .unwrap();
        sql::query("DROP TABLE p7_reaper_row_delete_audit CASCADE")
            .execute(&mut *cleanup)
            .await
            .unwrap();
        sql::query("DROP FUNCTION p7_reaper_record_row_delete() CASCADE")
            .execute(&mut *cleanup)
            .await
            .unwrap();
        for class_key in [blocked_class, valid_class] {
            let parent = finite_class_parent_name(class_key).unwrap();
            sql::query(&format!("DROP TABLE {parent} CASCADE"))
                .execute(&mut *cleanup)
                .await
                .unwrap();
            sql::query("DELETE FROM horsies_task_history_leaf_catalog WHERE class_key = $1")
                .bind(class_key)
                .execute(&mut *cleanup)
                .await
                .unwrap();
            sql::query("DELETE FROM horsies_retention_classes WHERE class_key = $1")
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
// identity (unique index horsies_worker_states_identity).
const DELETE_EXPIRED_WORKER_STATES_SQL: &str = "\
DELETE FROM horsies_worker_states
WHERE (worker_id, snapshot_at) IN (
    SELECT worker_id, snapshot_at FROM horsies_worker_states
    WHERE snapshot_at < NOW() - CAST($1 || ' hours' AS INTERVAL)
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)";

/// The worker-state retention delete: one batch of snapshots older than `$1`
/// hours (text), at most `$2` rows. Its row count counts snapshots.
static DELETE_EXPIRED_WORKER_STATES: TypedStatement = TypedStatement::new(
    "delete_expired_worker_states",
    DELETE_EXPIRED_WORKER_STATES_SQL,
    &[ParameterType::Text, ParameterType::Int8],
);

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
// `budgeted` keeps candidates while their running total of owned rows (node
// rows plus event rows of horsies_workflow_events, each counted through the
// workflow_id prefix of its key) fits $2 (the knob keeps its
// rows-per-statement meaning), always keeping the first candidate so a
// workflow larger than the whole budget drains alone instead of starving.
// Event rows and node rows are purged set-wise in `purged_events` and
// `purged_nodes` (the task_attempts pattern); the foreign-key cascades (run
// events by run_workflow_id, node events by their node, nodes by
// workflow_id) remain the correctness net for non-retention deletes.
//
// The top-level DELETE's rowcount counts WORKFLOWS, which under the node
// budget is routinely smaller than $2 while backlog remains — the workflow
// retention pass (`PostgresBroker::delete_expired_workflows`) therefore runs
// batches until one deletes no workflow, rather than the short-batch
// heuristic the row-batched statements use.
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
            WHERE wt.workflow_id = w.id)
           + (SELECT count(*)
              FROM horsies_workflow_events e
              WHERE e.workflow_id = w.id) AS owned_rows
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
               SUM(owned_rows) OVER (ORDER BY id) AS rows_running,
               ROW_NUMBER() OVER (ORDER BY id) AS position
        FROM doomed
    ) ranked
    WHERE rows_running <= $2 OR position = 1
),
purged_events AS (
    DELETE FROM horsies_workflow_events
    WHERE workflow_id IN (SELECT id FROM budgeted)
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
/// as FAILED or requeuing them respectively: the tasks of a worker whose
/// liveness lock stays free (`owner_gone_step`), and the tasks whose
/// heartbeats stopped. Every statement of the loop
/// runs on the broker's core pools: the gate, the recovery steps, the
/// retention cleanup and the fleet check on the main core pool as reserved
/// work ([`CheckoutClass::Reserved`]), the partition maintenance on the
/// session-work pool. The caller has checked the client family of the
/// handle (`Worker::start_database`); the reaper's partition maintenance
/// writes without a second check. With telemetry on, each failed step adds
/// 1 to `horsies.recovery.pass.failures{step}`, and each task row that a
/// recovery step changed is told to `telemetry` (`TaskRecovered`,
/// `horsies.recovery.tasks{action}`).
#[allow(clippy::needless_pass_by_value)]
pub fn spawn_reaper(
    broker: Arc<PostgresBroker>,
    child_specs: Arc<dyn ChildSpecSource>,
    config: RecoveryConfig,
    retention: RetentionConfig,
    health: ReaperHealth,
    telemetry: WorkerTelemetry,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let check_interval = Duration::from_millis(config.check_interval_ms);
        // The first pass runs at start and includes the retention cleanup
        // (Python `_ReaperPassState(next_retention_cleanup_at=time.monotonic())`).
        let mut next_retention_cleanup = tokio::time::Instant::now();
        let mut next_partition_maintenance = MaintenanceDue::now();
        let mut orphan_state = OrphanSweepState::default();
        // When this reaper first read each worker's liveness lock free; it
        // lives as long as the loop.
        let mut unheld_owners = UnheldOwners::default();
        // The failures in a row of each step, for the failure log; they live
        // as long as the loop.
        let mut failures = RecoveryFailures::new(telemetry.recovery_failures());
        // The fleet check runs after each pass, from one check interval after
        // the start on.
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
            let pass = async {
                run_reaper_pass(
                    &broker,
                    child_specs.as_ref(),
                    &config,
                    &retention,
                    &health,
                    &mut next_retention_cleanup,
                    &mut next_partition_maintenance,
                    &mut orphan_state,
                    &mut unheld_owners,
                    &mut failures,
                    &telemetry,
                )
                .await;
                check_class_coverage(&broker, &mut coverage_log).await;
            };
            let gated = match broker.core_pools_for_read() {
                Ok(open) => {
                    run_under_gate(
                        open.main(),
                        broker.statement_log(),
                        advisory_key_reaper(),
                        pass,
                    )
                    .await
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "periodic-pass gate connection unavailable: {error}; skipping interval",
                    );
                    GatedPass::Unavailable
                }
            };
            match gated {
                GatedPass::Busy => {
                    tracing::debug!("reaper pass skipped: another worker holds the gate");
                }
                GatedPass::Held | GatedPass::Ungated | GatedPass::Unavailable => {}
            }

            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(check_interval) => {}
            }
        }
    })
}

/// How one interval of a periodic pass ran under its cluster-wide gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GatedPass {
    /// The gate was held for the pass; its transaction ended after the pass.
    Held,
    /// Another worker holds the gate this interval: the pass did not run.
    Busy,
    /// The gate could not be taken (no connection, or its statement failed):
    /// the pass did not run. The cause is logged.
    Unavailable,
    /// The pool has fewer than [`GATED_PASS_MIN_CONNECTIONS`] connections:
    /// the pass ran with no gate.
    Ungated,
}

/// The fewest connections of the pool with which a pass takes its gate: the
/// gate transaction holds one connection while the pass body checks out
/// another (Python `_reaper_gate_enabled`, `parent_pool_size >= 2`).
const GATED_PASS_MIN_CONNECTIONS: u32 = 2;

/// `pg_try_advisory_xact_lock` on a periodic-pass gate key (Python
/// `REAPER_GATE_TRY_LOCK_SQL`).
static GATE_TRY_LOCK: TypedStatement = TypedStatement::new(
    "reaper_gate_try_lock",
    "SELECT pg_try_advisory_xact_lock($1)",
    &[ParameterType::Int8],
);

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

/// Run `pass` under a periodic-pass gate: a transaction-scoped advisory lock
/// on `key`, held by an otherwise idle transaction on a connection of `pool`
/// for the duration of the pass. The gate's connection is reserved work
/// ([`CheckoutClass::Reserved`]).
///
/// Xact scoping keeps acquire and release on one server backend under
/// PgBouncer transaction pooling (a session-level lock would not survive
/// between round-trips there). The lock ends with the gate transaction: at
/// its commit after the pass, at its rollback when another worker holds the
/// gate, and when the server closes a discarded connection.
///
/// A pool of fewer than [`GATED_PASS_MIN_CONNECTIONS`] connections runs the
/// pass ungated: the gate would hold its only connection while the pass body
/// waits for a second one. SKIP LOCKED keeps an ungated pass correct (just
/// possibly duplicated). Parity with horsies PR #101 4a7344ec.
///
/// `pass` runs only for [`GatedPass::Held`] and [`GatedPass::Ungated`]. A
/// failure to take the gate is logged as a warning, and the interval is
/// skipped.
async fn run_under_gate(
    pool: &CorePool,
    log: StatementLog,
    key: i64,
    pass: impl Future<Output = ()>,
) -> GatedPass {
    if pool.sizing().size() < GATED_PASS_MIN_CONNECTIONS {
        pass.await;
        return GatedPass::Ungated;
    }
    let mut connection = match pool.checkout(CheckoutClass::Reserved).await {
        Ok(connection) => connection,
        Err(error) => {
            tracing::warn!(
                %error,
                "periodic-pass gate connection unavailable: {error}; skipping interval",
            );
            return GatedPass::Unavailable;
        }
    };
    let mut gate = match connection.begin(IsolationLevel::ServerDefault).await {
        Ok(gate) => gate,
        Err(error) => {
            tracing::warn!(
                %error,
                "periodic-pass gate connection unavailable: {error}; skipping interval",
            );
            return GatedPass::Unavailable;
        }
    };
    match try_gate_lock(&mut gate, log, key).await {
        Ok(true) => {}
        Ok(false) => {
            end_gate(gate).await;
            return GatedPass::Busy;
        }
        Err(error) => {
            tracing::warn!(
                %error,
                "periodic-pass advisory gate failed: {error}; skipping interval",
            );
            end_gate(gate).await;
            return GatedPass::Unavailable;
        }
    }
    pass.await;
    if let Err(error) = gate.commit().await {
        tracing::warn!(
            %error,
            "periodic-pass gate commit failed: {error}; lock frees when the connection closes",
        );
    }
    GatedPass::Held
}

/// One `pg_try_advisory_xact_lock` on `key` in the gate transaction: whether
/// this transaction now holds the gate.
///
/// Errors: the statement's [`BrokerError::Statement`], or
/// [`BrokerError::RowDecode`] for a reply that is not one boolean.
async fn try_gate_lock(
    gate: &mut Transaction<'_>,
    log: StatementLog,
    key: i64,
) -> Result<bool, BrokerError> {
    let statement = &GATE_TRY_LOCK;
    let rows = query_in(gate, log, &statement.bind().value(&key).finish()).await?;
    let acquired: bool = scalar(&one_row(rows, statement)?, statement)?;
    Ok(acquired)
}

/// Ends a gate transaction that does not hold the gate, or whose lock
/// statement failed. A failed rollback discards the connection; the server
/// ends the transaction when the socket closes.
async fn end_gate(gate: Transaction<'_>) {
    let _ended: RollbackOutcome = gate.rollback().await;
}

/// Run one reaper pass: the owner-gone step, then Python's order:
/// stale-CLAIMED requeue, orphan audit, stale-RUNNING recovery, PENDING
/// expiry, workflow recovery, then periodic retention cleanup (with the
/// liveness rows of ended workers) and partition maintenance.
#[allow(clippy::too_many_arguments)]
async fn run_reaper_pass(
    broker: &PostgresBroker,
    child_specs: &dyn ChildSpecSource,
    config: &RecoveryConfig,
    retention: &RetentionConfig,
    health: &ReaperHealth,
    next_retention_cleanup: &mut tokio::time::Instant,
    next_partition_maintenance: &mut MaintenanceDue,
    orphan_state: &mut OrphanSweepState,
    unheld_owners: &mut UnheldOwners,
    failures: &mut RecoveryFailures,
    telemetry: &WorkerTelemetry,
) {
    // The tasks of workers whose liveness lock stays free, on the main core
    // pool with the reserve. It runs when either recovery of task rows is on.
    if config.auto_fail_stale_running || config.auto_requeue_stale_claimed {
        let step = match broker.core_pools_for_write() {
            Ok((open, _write)) => {
                owner_gone_step(
                    open.main(),
                    broker.statement_log(),
                    config,
                    unheld_owners,
                    OWNER_GONE_SPAN,
                    telemetry,
                )
                .await
            }
            Err(error) => Err(error),
        };
        match step {
            Ok(step) if step.gone_owners > 0 => {
                failures.succeeded(RecoveryStep::OwnerGone);
                tracing::warn!(
                    target: STALE_RUNNING_PASS,
                    gone_owners = step.gone_owners,
                    running_recovered = step.running_recovered,
                    claims_requeued = step.claims_requeued,
                    "reaper recovered the tasks of workers whose liveness lock stayed free",
                );
            }
            Ok(_) => failures.succeeded(RecoveryStep::OwnerGone),
            Err(error) => failures.failed(RecoveryStep::OwnerGone, &error),
        }
    }

    // The order: requeue stale CLAIMED rows,
    // audit orphaned workflow tasks, fail stale RUNNING rows, expire PENDING
    // rows, then the workflow and maintenance steps. No step stops for the
    // life of the process: a step that keeps failing runs again on later
    // passes, its health keeps reporting the failure, and its log follows
    // `failure_log` (the first failure, one line per
    // `RECOVERY_FAILURE_LOG_EVERY` failures in a row, the recovery).
    // The stale CLAIMED requeue runs on the main core pool with the reserve.
    if config.auto_requeue_stale_claimed {
        match ThresholdMs::from_millis(config.claimed_stale_threshold_ms) {
            Ok(threshold) => {
                let requeued = match broker.core_pools_for_write() {
                    Ok((open, _write)) => {
                        requeue_stale_claimed(
                            open.main(),
                            broker.statement_log(),
                            threshold,
                            telemetry,
                        )
                        .await
                    }
                    Err(error) => Err(error),
                };
                match requeued {
                    Ok(count) if count > 0 => {
                        failures.succeeded(RecoveryStep::StaleClaimedRequeue);
                        tracing::info!(count, "reaper requeued stale CLAIMED tasks");
                    }
                    Ok(_) => failures.succeeded(RecoveryStep::StaleClaimedRequeue),
                    Err(error) => failures.failed(RecoveryStep::StaleClaimedRequeue, &error),
                }
            }
            Err(error) => failures.failed(
                RecoveryStep::StaleClaimedRequeue,
                &format_args!("the stale CLAIMED threshold is out of range: {error}"),
            ),
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
                Ok((open, _write)) => {
                    let told = |cancelled: &[RecoveredTask]| {
                        telemetry.tasks_recovered(cancelled, RecoveryAction::OrphanCancelled);
                    };
                    cancel_orphan_page(open.main(), broker.statement_log(), told).await
                }
                Err(error) => Err(error),
            };
            match page.map(|page| OrphanTaskAuditReport::ready(page, started)) {
                Ok(report) => {
                    failures.succeeded(RecoveryStep::OrphanTaskAudit);
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
                        // Another worker holds the audit lock: the audit did
                        // not run, so its failures in a row do not change.
                        OrphanAuditFailureKind::Refusal => {
                            tracing::warn!(
                                %error,
                                "Reaper orphan workflow-task audit lock was busy: {error}; the \
                                 next scheduled audit will retry",
                            );
                        }
                        OrphanAuditFailureKind::Transient => failures.failed(
                            RecoveryStep::OrphanTaskAudit,
                            &format_args!("transient: {error}"),
                        ),
                        // The audit never stops on a live worker.
                        OrphanAuditFailureKind::Permanent => {
                            failures.failed(RecoveryStep::OrphanTaskAudit, &error);
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
                            telemetry,
                        )
                        .await
                    }
                    Err(error) => Err(error),
                };
                match recovered {
                    Ok(count) if count > 0 => {
                        failures.succeeded(RecoveryStep::StaleRunningRecovery);
                        tracing::warn!(count, "reaper marked stale RUNNING tasks as FAILED");
                    }
                    Ok(_) => failures.succeeded(RecoveryStep::StaleRunningRecovery),
                    Err(error) => failures.failed(RecoveryStep::StaleRunningRecovery, &error),
                }
            }
            Err(error) => failures.failed(
                RecoveryStep::StaleRunningRecovery,
                &format_args!("the stale RUNNING thresholds are out of range: {error}"),
            ),
        }
    }

    // Expire unclaimed PENDING tasks whose good_until has passed, on the
    // main core pool with the reserve.
    let expired = match broker.core_pools_for_write() {
        Ok((open, _write)) => {
            let told = |expired: &[RecoveredTask]| {
                telemetry.tasks_recovered(expired, RecoveryAction::PendingExpired);
            };
            expire_pending_pass(open.main(), broker.statement_log(), told).await
        }
        Err(error) => Err(PendingExpiryFailed {
            expired_before: 0,
            error,
        }),
    };
    match expired {
        Ok(PendingExpiryPass::Drained { expired: count }) if count > 0 => {
            failures.succeeded(RecoveryStep::PendingExpiry);
            tracing::info!(count, "reaper expired unclaimed PENDING tasks");
        }
        Ok(PendingExpiryPass::Drained { .. }) => failures.succeeded(RecoveryStep::PendingExpiry),
        Ok(PendingExpiryPass::BoundReached { expired: count }) => {
            failures.succeeded(RecoveryStep::PendingExpiry);
            tracing::warn!(
                expired = count,
                batches = PENDING_EXPIRY_MAX_BATCHES_PER_PASS,
                "pending expiry reached its per-pass bound; the next pass goes on",
            );
        }
        Err(failed) => failures.failed(RecoveryStep::PendingExpiry, &failed),
    }

    // Exact outbox recovery. Each candidate owns its transaction; retaining
    // dispositions remain visible and count toward bounded quarantine.
    // The children of the sub-workflow nodes that a step starts come from
    // the worker's child spec source.
    let workflow_recovery =
        crate::workflow_engine::recovery::recover_stuck_workflows_observed_with(
            broker,
            child_specs,
            config.workflow_recovery_failure_limit,
        )
        .await;
    let workflow_health = match workflow_recovery {
        Ok(report) => {
            failures.succeeded(RecoveryStep::WorkflowRecovery);
            serde_json::to_value(report).unwrap_or_else(
                |error| serde_json::json!({"state": "error", "error": error.to_string()}),
            )
        }
        Err(failure) => {
            failures.failed(RecoveryStep::WorkflowRecovery, &failure.error);
            failure.into_health_snapshot()
        }
    };
    health.write().await.workflow_recovery = Some(workflow_health);

    // Discovery and each record run on the main core pool with the reserve.
    let phase2 = crate::workflow_engine::phase2_recovery::drive_phase2_recovery_with(
        broker,
        child_specs,
        config.crashed_worker_recovery_grace_ms,
        crate::workflow_engine::recovery::GLOBAL_SCAN_ROW_CAP,
        config.phase2_quarantine_after_attempts,
    )
    .await;
    let phase2_health = match phase2 {
        Ok(summary) => {
            failures.succeeded(RecoveryStep::Phase2Recovery);
            if summary.applied > 0 || summary.retained > 0 || summary.failed > 0 {
                tracing::info!(?summary, "phase-2 recovery pass completed");
            }
            serde_json::to_value(summary).unwrap_or_else(
                |error| serde_json::json!({"state": "error", "error": error.to_string()}),
            )
        }
        Err(error) => {
            failures.failed(RecoveryStep::Phase2Recovery, &error);
            serde_json::json!({"state": "error", "error": error.to_string()})
        }
    };
    health.write().await.phase2_recovery = Some(phase2_health);

    // Retention cleanup (runs every retention_sweep_interval_s), then the
    // liveness rows of ended workers, on the main core pool with the reserve.
    if tokio::time::Instant::now() >= *next_retention_cleanup {
        run_retention_cleanup(broker, retention, failures).await;
        let forgotten = match broker.core_pools_for_write() {
            Ok((open, _write)) => forget_ended_workers(open.main(), broker.statement_log()).await,
            Err(error) => Err(error),
        };
        match forgotten {
            Ok(0) => failures.succeeded(RecoveryStep::EndedWorkerRows),
            Ok(count) => {
                failures.succeeded(RecoveryStep::EndedWorkerRows);
                tracing::info!(count, "reaper deleted the liveness rows of ended workers");
            }
            Err(error) => failures.failed(RecoveryStep::EndedWorkerRows, &error),
        }
        *next_retention_cleanup =
            tokio::time::Instant::now() + Duration::from_secs(retention.retention_sweep_interval_s);
    }

    if let Some(age) = retention.paused_workflow_auto_cancel_after {
        match crate::workflow_engine::lifecycle::expire_paused_workflows(broker, age, 50).await {
            Ok(count) if count > 0 => {
                failures.succeeded(RecoveryStep::PausedWorkflowExpiry);
                tracing::info!(
                    count,
                    "expired paused workflows past the declared age policy"
                );
            }
            Ok(_) => failures.succeeded(RecoveryStep::PausedWorkflowExpiry),
            Err(error) => failures.failed(RecoveryStep::PausedWorkflowExpiry, &error),
        }
    }

    if next_partition_maintenance.is_due() {
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
            Ok(CoverageOutcome::Ensured(ensured)) => {
                failures.succeeded(RecoveryStep::PartitionCoverage);
                serde_json::json!({
                    "state": "ensured",
                    "created_history_leaves": ensured.created_history_leaves,
                    "created_heartbeat_leaves": ensured.created_heartbeat_leaves,
                    "republished": ensured.republished,
                    "heartbeat_covered_now": ensured.heartbeat_covered_now,
                    "history_covered_through": ensured.history_covered_through,
                    "heartbeats_covered_through": ensured.heartbeats_covered_through,
                    "absent_leaves": ensured.absent_leaves,
                })
            }
            Ok(CoverageOutcome::Failed(failed)) => {
                failures.failed(
                    RecoveryStep::PartitionCoverage,
                    &format_args!(
                        "the coverage pass stopped at {}: {}",
                        failed.stage.as_str(),
                        failed.refusal,
                    ),
                );
                serde_json::json!({
                    "state": "failed",
                    "stage": failed.stage.as_str(),
                    "class_key": failed.class_key,
                    "refusal": failed.refusal,
                    "heartbeat_covered_now": failed.heartbeat_covered_now,
                    "absent_leaves": failed.absent_leaves,
                })
            }
            Err(error) => {
                failures.failed(RecoveryStep::PartitionCoverage, &error);
                serde_json::json!({"state": "error", "error": error.to_string()})
            }
        };
        health.write().await.partition_coverage = Some(coverage_health);

        // Coverage and pruning are independently contained: either half runs
        // and publishes health even when the other refuses.
        let prune = broker.prune_expired_partitions().await;
        // Expired key reservations are removed after pruning, in bounded
        // batches; the rest waits for the next pass.
        let key_reservations = match cleanup_expired_key_reservations(
            broker,
            i32::try_from(retention.retention_delete_batch_size).unwrap_or(i32::MAX),
        )
        .await
        {
            Ok(ReservationCleanupPass::Complete { deleted }) => {
                failures.succeeded(RecoveryStep::KeyReservationCleanup);
                serde_json::json!({"state": "complete", "deleted": deleted})
            }
            Ok(ReservationCleanupPass::Truncated { deleted }) => {
                failures.succeeded(RecoveryStep::KeyReservationCleanup);
                serde_json::json!({"state": "truncated", "deleted": deleted})
            }
            Err(error) => {
                failures.failed(RecoveryStep::KeyReservationCleanup, &error);
                serde_json::json!({"state": "error", "error": error.to_string()})
            }
        };
        let pruning_health = match prune {
            Ok(prune) => {
                failures.succeeded(RecoveryStep::PartitionPruning);
                serde_json::json!({
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
                })
            }
            Err(error) => {
                failures.failed(RecoveryStep::PartitionPruning, &error);
                serde_json::json!({
                    "state": "error",
                    "error": error.to_string(),
                    "key_reservations": key_reservations,
                })
            }
        };
        health.write().await.partition_pruning = Some(pruning_health);
        *next_partition_maintenance = MaintenanceDue::after(Duration::from_secs(
            retention.partition_maintenance_interval_s,
        ));
    }
}

/// Why the expired key reservation cleanup of a maintenance pass has no
/// outcome.
#[derive(Debug, thiserror::Error)]
enum KeyReservationCleanupFailed {
    /// The core pools could not open; nothing was deleted.
    #[error(transparent)]
    Pools(BrokerError),
    /// A cleanup statement failed, or its reply broke the function's
    /// contract. The batches before it stay committed.
    #[error(transparent)]
    Cleanup(HistoryError),
}

/// The expired key reservation cleanup of a maintenance pass
/// ([`cleanup_expired_reservations_pass`]): at most
/// [`RESERVATION_CLEANUP_BATCHES_PER_PASS`] batches of `batch_size` on the
/// main core pool, as reserved work. Holds a write token, so
/// [`PostgresBroker::close`] waits for it.
///
/// Errors: [`KeyReservationCleanupFailed`].
async fn cleanup_expired_key_reservations(
    broker: &PostgresBroker,
    batch_size: i32,
) -> Result<ReservationCleanupPass, KeyReservationCleanupFailed> {
    let (open, _write) = broker
        .core_pools_for_write()
        .map_err(KeyReservationCleanupFailed::Pools)?;
    cleanup_expired_reservations_pass(
        open.main(),
        broker.statement_log(),
        batch_size,
        RESERVATION_CLEANUP_BATCHES_PER_PASS,
    )
    .await
    .map_err(KeyReservationCleanupFailed::Cleanup)
}

/// Recover the stale RUNNING tasks of one pass: the crash retry that the
/// task's policy admits, else the terminal failure (Python
/// `PostgresBroker.mark_stale_tasks_as_failed` runs this pass).
///
/// - `horsies_stale_running_scan` reads at most `batch` candidates with no
///   lock.
/// - Each candidate gets one `horsies_finish_stale_attempt` with the retry
///   that its policy admits for `WORKER_CRASHED`, computed from the scanned
///   retry facts. The statement judges the row again under its locks: a row
///   that is no longer RUNNING or stale, or whose retry count moved, is
///   refused and nothing is written.
/// - A scheduled retry wakes its queue (best-effort, as Python).
/// - Each candidate whose retry or failure committed is told to `telemetry`
///   (`RunningRetried` or `RunningFailed`).
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
    telemetry: &WorkerTelemetry,
) -> Result<u64, crate::broker::BrokerError> {
    let pass = stale_running_pass(pool, log, thresholds, batch, telemetry).await?;
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
/// and failure, and tells each recovered candidate to `telemetry`; the
/// caller reports the end of the pass.
///
/// Errors: the scan's [`crate::broker::BrokerError::Statement`] or
/// [`crate::broker::BrokerError::RowDecode`]; nothing was written.
pub(crate) async fn stale_running_pass(
    pool: &CorePool,
    log: StatementLog,
    thresholds: StaleRunningThresholds,
    batch: RecoveryBatch,
    telemetry: &WorkerTelemetry,
) -> Result<StaleRunningPass, crate::broker::BrokerError> {
    let candidates = scan_stale_running(pool, log, thresholds, batch).await?;
    let mut recovered: u64 = 0;
    for candidate in &candidates {
        let attempt = candidate_log(candidate);
        let command = match stale_attempt_command(candidate, thresholds) {
            Ok(command) => command,
            Err(error) => {
                // Python skips the task when its crash result does not
                // serialize (`core/brokers/postgres.py:3351-3358`).
                tracing::error!(
                    target: STALE_RUNNING_PASS,
                    task_id = %candidate.task_id,
                    attempt = attempt.attempt.get(),
                    trace_id = attempt.trace_id.log_field(),
                    %error,
                    "Failed to serialize crash result for task {}: {error}",
                    candidate.task_id,
                );
                continue;
            }
        };
        match finish_stale_attempt(pool, log, &command).await {
            Ok(outcome) => match after_stale_attempt(pool, log, &command, attempt, outcome).await {
                StaleRecovery::Recovered(action) => {
                    recovered += 1;
                    telemetry.task_recovered(
                        candidate.task_id,
                        &candidate.task_name,
                        &candidate.queue_name,
                        action,
                    );
                }
                StaleRecovery::NotRecovered => {}
            },
            Err(error) => {
                tracing::error!(
                    target: STALE_RUNNING_PASS,
                    task_id = %candidate.task_id,
                    attempt = attempt.attempt.get(),
                    trace_id = attempt.trace_id.log_field(),
                    %error,
                    "Reaper failed to process stale task {}: {error}",
                    candidate.task_id,
                );
            }
        }
    }
    Ok(stale_running_end(candidates.len(), batch, recovered))
}

/// The log fields of the attempt that a stale candidate runs: its retry
/// count plus 1, and the trace id that the send stored on its row. Pure.
pub(crate) fn candidate_log(candidate: &StaleCandidate) -> AttemptLog {
    AttemptLog {
        attempt: attempt_of(candidate.retry_count),
        trace_id: candidate.trace_id,
    }
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
    /// The retry or the failure committed: `RunningRetried` or
    /// `RunningFailed`.
    Recovered(RecoveryAction),
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
        data: Some(crate::core::codec::JsonText::of_engine_value(
            &serde_json::json!({
                "stale_threshold_ms": threshold_ms,
                "stale_threshold_seconds": threshold_seconds,
                "worker_pid": candidate.worker_pid,
                "worker_hostname": candidate.worker_hostname,
                "worker_id": candidate.claimed_by_worker_id,
                "started_at": python_isoformat_utc(candidate.started_at),
                "detected_at": python_isoformat_utc(candidate.db_now),
            }),
        )),
    };
    let result_json = task_result_to_json_string(&TaskResult::<()>::Err(task_error))?;
    Ok(FinishStaleAttempt {
        task_id: candidate.task_id,
        evidence: StaleEvidence::BeatAge(thresholds),
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

/// Log what the statement reported, naming the attempt of `command` with
/// `attempt`, and wake the queue of a scheduled retry. A failed wake is
/// logged: the retried row is PENDING, and the queue's poll finds it.
async fn after_stale_attempt(
    pool: &CorePool,
    log: StatementLog,
    command: &FinishStaleAttempt,
    attempt: AttemptLog,
    outcome: StaleAttemptOutcome,
) -> StaleRecovery {
    match outcome {
        StaleAttemptOutcome::FailureApplied { retry, after, .. } => {
            tracing::info!(
                target: STALE_RUNNING_PASS,
                task_id = %command.task_id,
                attempt = attempt.attempt.get(),
                trace_id = attempt.trace_id.log_field(),
                ?retry,
                ?after,
                "stale RUNNING task marked FAILED",
            );
            StaleRecovery::Recovered(RecoveryAction::RunningFailed)
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
                attempt = attempt.attempt.get(),
                trace_id = attempt.trace_id.log_field(),
                retry_count,
                %next_retry_at,
                "stale RUNNING task scheduled for retry",
            );
            if let Err(error) = wake_retried_queue(pool, log, &queue_name, command.task_id).await {
                tracing::warn!(
                    target: STALE_RUNNING_PASS,
                    task_id = %command.task_id,
                    attempt = attempt.attempt.get(),
                    trace_id = attempt.trace_id.log_field(),
                    queue = %queue_name,
                    %error,
                    "Wake of stale retry {} failed: {error}",
                    command.task_id,
                );
            }
            StaleRecovery::Recovered(RecoveryAction::RunningRetried)
        }
        StaleAttemptOutcome::Refused(refusal) => {
            tracing::warn!(
                target: STALE_RUNNING_PASS,
                task_id = %command.task_id,
                attempt = attempt.attempt.get(),
                trace_id = attempt.trace_id.log_field(),
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

/// The reaper's stale recovery steps for tests that hold a test pool: each
/// runs on a main core pool on the same database, built for the call and
/// closed after it. The thresholds are in seconds and the scan limit is an
/// `i64`, as the earlier sqlx forms took them, so a test call site keeps its
/// form.
#[cfg(test)]
pub(crate) mod on_test_pool {
    use super::{
        RecoveryBatch, StaleRunningThresholds, StatementLog, ThresholdMs, WorkerTelemetry,
    };
    use crate::test_support::sql::TestDatabaseUrl;

    pub(crate) async fn mark_stale_running_as_failed(
        pool: &impl TestDatabaseUrl,
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
        let core = crate::broker::pools::core_pool_for_test_url(&pool.test_url());
        let recovered = super::mark_stale_running_as_failed(
            &core,
            StatementLog::Off,
            thresholds,
            batch,
            &WorkerTelemetry::Off,
        )
        .await;
        core.close();
        recovered
    }

    /// [`super::requeue_stale_claimed`] on a core pool on the database of a
    /// test pool, with the threshold in seconds.
    pub(crate) async fn requeue_stale_claimed(
        pool: &impl TestDatabaseUrl,
        threshold_secs: f64,
    ) -> Result<u64, crate::broker::BrokerError> {
        let core = crate::broker::pools::core_pool_for_test_url(&pool.test_url());
        let requeued = super::requeue_stale_claimed(
            &core,
            StatementLog::Off,
            threshold_of(threshold_secs),
            &WorkerTelemetry::Off,
        )
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

/// Per-reaper state of the orphan audit: when it next runs. Its failures in
/// a row are in the reaper's [`RecoveryFailures`]; the audit never stops on
/// a live worker.
struct OrphanSweepState {
    next_audit: tokio::time::Instant,
}

impl Default for OrphanSweepState {
    fn default() -> Self {
        Self {
            next_audit: tokio::time::Instant::now(),
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
    use crate::telemetry::registry::OptionalCounter;

    #[test]
    fn orphan_task_audit_uses_its_own_interval() {
        let now = tokio::time::Instant::now();
        let mut state = OrphanSweepState { next_audit: now };
        let interval = Duration::from_secs(60);
        assert!(state.schedule_if_due(now, interval));
        assert!(!state.schedule_if_due(now + Duration::from_secs(59), interval));
        assert!(state.schedule_if_due(now + Duration::from_secs(60), interval));
    }

    /// The orphan audit never stops on a live worker: its failures in a
    /// row do not change its schedule (Python stops it after 3; the core
    /// does not).
    #[test]
    fn orphan_audit_stays_scheduled_after_permanent_failures() {
        let now = tokio::time::Instant::now();
        let mut state = OrphanSweepState { next_audit: now };
        let mut failures = RecoveryFailures::new(OptionalCounter::NotRecorded);
        let interval = Duration::from_secs(60);
        assert!(state.schedule_if_due(now, interval));
        for _ in 0..3 {
            failures.failed(RecoveryStep::OrphanTaskAudit, &"permanent");
        }
        assert!(state.schedule_if_due(now + interval, interval));
    }

    #[test]
    fn transient_non_lock_failure_is_an_error_in_the_health_snapshot() {
        let error = crate::broker::BrokerError::Statement(crate::db::StatementFailure::NotSent {
            statement: "orphan_audit_page",
            cause: crate::db::NotSentCause::Checkout(crate::db::CheckoutFailure::TimedOut {
                role: crate::db::PoolRole::Main,
                waited: std::time::Duration::from_secs(30),
            }),
        });
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
/// [`requeue_stale_claimed_pass`] on `pool`. Each row of each committed
/// batch is told to `telemetry` as `ClaimRequeued`. Logs a warning when the
/// pass reached its bound. Returns the number of requeued rows.
///
/// Errors: the failed batch's [`crate::broker::BrokerError`]; the batches
/// before it stay committed, and their rows were told.
pub async fn requeue_stale_claimed(
    pool: &CorePool,
    log: StatementLog,
    stale_after: ThresholdMs,
    telemetry: &WorkerTelemetry,
) -> Result<u64, crate::broker::BrokerError> {
    let told = |requeued: &[RecoveredTask]| {
        telemetry.tasks_recovered(requeued, RecoveryAction::ClaimRequeued);
    };
    match requeue_stale_claimed_pass(pool, log, stale_after, told).await {
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

/// What one retention pass deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RetentionDeleted {
    worker_states: u64,
    workflows: u64,
}

/// Why a retention pass stopped before its end. The batches that committed
/// before the failure stay committed; the backlog resumes on the next pass.
#[derive(Debug, thiserror::Error)]
enum RetentionCleanupFailed {
    /// A setting that the retention config check holds at 1 or more is 0
    /// (a config that did not pass the check). Nothing was deleted by the
    /// step that reads it.
    #[error("{setting} is 0; the retention config check requires at least 1")]
    ZeroSetting { setting: &'static str },
    /// The worker-state step failed after deleting `deleted_before` rows;
    /// the workflow step did not run. After `OutcomeUnknown` the failed batch
    /// may have committed.
    #[error("worker-state retention failed after {deleted_before} deleted row(s): {error}")]
    WorkerStates {
        deleted_before: u64,
        error: BrokerError,
    },
    /// The workflow step failed after deleting `deleted_before` workflows.
    /// After `OutcomeUnknown` the failed batch may have committed.
    #[error("workflow retention failed after {deleted_before} deleted workflow(s): {error}")]
    Workflows {
        deleted_before: u64,
        error: BrokerError,
    },
}

/// `value` of the retention setting `setting` as a count above zero.
///
/// Errors: [`RetentionCleanupFailed::ZeroSetting`] for 0.
fn retention_setting(
    value: u32,
    setting: &'static str,
) -> Result<NonZeroU32, RetentionCleanupFailed> {
    NonZeroU32::new(value).ok_or(RetentionCleanupFailed::ZeroSetting { setting })
}

/// One worker-state retention step (Python `_delete_expired_in_batches` with
/// `DELETE_EXPIRED_WORKER_STATES_SQL`): batches of at most `batch_size`
/// snapshots older than `retention_hours`, each one statement in autocommit
/// on the main core pool as reserved work ([`CheckoutClass::Reserved`]),
/// until a batch deletes fewer than `batch_size` rows (nothing eligible is
/// left) or `deadline` has passed after a batch. The first batch always
/// runs. Holds a write token, so [`PostgresBroker::close`] waits for it.
///
/// Returns: the count of snapshots deleted.
///
/// Errors: [`RetentionCleanupFailed::WorkerStates`] with the count of the
/// batches that committed before the failure.
async fn delete_expired_worker_states(
    broker: &PostgresBroker,
    retention_hours: NonZeroU32,
    batch_size: NonZeroU32,
    deadline: tokio::time::Instant,
) -> Result<u64, RetentionCleanupFailed> {
    let failed = |deleted_before: u64, error: BrokerError| RetentionCleanupFailed::WorkerStates {
        deleted_before,
        error,
    };
    let (open, _write) = broker
        .core_pools_for_write()
        .map_err(|error| failed(0, error))?;
    let hours = retention_hours.get().to_string();
    let limit = i64::from(batch_size.get());
    let full_batch = u64::from(batch_size.get());
    let mut deleted: u64 = 0;
    loop {
        let bound = DELETE_EXPIRED_WORKER_STATES
            .bind()
            .value(&hours)
            .value(&limit)
            .finish();
        broker.statement_log().log(&DELETE_EXPIRED_WORKER_STATES);
        let batch = open
            .main()
            .execute(
                CheckoutClass::Reserved,
                &bound,
                StatementBound::ConnectionTimeouts,
            )
            .await
            .map_err(|failure| failed(deleted, BrokerError::from(failure)))?;
        deleted += batch;
        if batch < full_batch {
            return Ok(deleted);
        }
        if tokio::time::Instant::now() >= deadline {
            tracing::info!(
                total_deleted = deleted,
                "retention pass time budget reached; remaining backlog resumes next pass",
            );
            return Ok(deleted);
        }
    }
}

/// The steps of one retention pass, in Python's order: the worker-state
/// snapshots, then the workflows with their node rows. Each step is gated by
/// its config (`None` = disabled). A failed step ends the pass.
///
/// Errors: [`RetentionCleanupFailed`].
async fn retention_pass(
    broker: &PostgresBroker,
    config: &RetentionConfig,
    deadline: tokio::time::Instant,
) -> Result<RetentionDeleted, RetentionCleanupFailed> {
    let worker_states = match config.worker_state_retention_hours {
        None => 0,
        Some(hours) => {
            delete_expired_worker_states(
                broker,
                retention_setting(hours, "worker_state_retention_hours")?,
                retention_setting(
                    config.retention_delete_batch_size,
                    "retention_delete_batch_size",
                )?,
                deadline,
            )
            .await?
        }
    };
    // Workflows and their node rows go together in one workflow-batched
    // statement. Terminal tasks already moved to partitioned history and are
    // never row-deleted here. The step's budget is what is left of the pass
    // budget; its first batch always runs.
    let workflows = match config.terminal_record_retention_hours {
        None => 0,
        Some(hours) => broker
            .delete_expired_workflows(WorkflowRetentionPass {
                retention_hours: retention_setting(hours, "terminal_record_retention_hours")?,
                batch_size: retention_setting(
                    config.retention_delete_batch_size,
                    "retention_delete_batch_size",
                )?,
                budget: deadline.saturating_duration_since(tokio::time::Instant::now()),
            })
            .await
            .map_err(|failed| RetentionCleanupFailed::Workflows {
                deleted_before: failed.deleted_before,
                error: failed.error,
            })?,
    };
    Ok(RetentionDeleted {
        worker_states,
        workflows,
    })
}

/// Run retention cleanup for relational monitoring and workflow records.
///
/// The worker-state snapshots, then the workflows with their node rows
/// ([`PostgresBroker::delete_expired_workflows`]), each gated by its config
/// (`None` = disabled). Deletes run in bounded batches under one pass time
/// budget ([`RETENTION_PASS_TIME_BUDGET`]); each step runs at least one
/// batch. Every batch is one statement in autocommit on the main core pool,
/// as reserved work. A failure goes to `failures` (its log and its counter);
/// the batches before it stay committed, and the backlog resumes on the next
/// pass.
///
/// Called by the reaper loop every `retention_sweep_interval_s`.
pub async fn run_retention_cleanup(
    broker: &PostgresBroker,
    config: &RetentionConfig,
    failures: &mut RecoveryFailures,
) {
    // Shared wall-clock budget across the statements. A backlog that
    // outlives the budget resumes next pass.
    let deadline = tokio::time::Instant::now() + RETENTION_PASS_TIME_BUDGET;
    match retention_pass(broker, config, deadline).await {
        Ok(deleted) => {
            failures.succeeded(RecoveryStep::RetentionCleanup);
            if deleted.worker_states + deleted.workflows > 0 {
                tracing::info!(
                    deleted_worker_states = deleted.worker_states,
                    deleted_workflows = deleted.workflows,
                    "retention cleanup completed",
                );
            }
        }
        Err(error) => failures.failed(RecoveryStep::RetentionCleanup, &error),
    }
}

#[cfg(test)]
mod tests {
    use super::on_test_pool::{mark_stale_running_as_failed, requeue_stale_claimed};
    use super::*;
    use crate::core::history::maintenance::coverage::{
        ensure_startup_coverage_in_pool, StartupCoverageOutcome,
    };
    use crate::telemetry::registry::OptionalCounter;
    use crate::test_support::sql::{self, TestPool, TestPoolOptions};
    use uuid::Uuid;

    /// The scan limit of a reaper pass, as the test helper takes it.
    const STALE_RUNNING_SCAN_LIMIT: i64 = STALE_RUNNING_SCAN_BATCH.get() as i64;

    /// Batch size for direct retention-statement tests (the default
    /// `RecoveryConfig::retention_delete_batch_size`).
    const TEST_RETENTION_BATCH: i64 = 500;

    /// The most batches [`drain_expired_workflows`] runs before the test
    /// fails.
    const DRAIN_BATCH_LIMIT: u32 = 1_000;

    /// Delete every terminal workflow of the test database with no live
    /// backing task, of any age, so that the counts of a test see only the
    /// workflows that it seeds after the drain.
    async fn drain_expired_workflows(pool: &TestPool) {
        for _ in 0..DRAIN_BATCH_LIMIT {
            let deleted = sql::query(DELETE_EXPIRED_WORKFLOWS_SQL)
                .bind("0")
                .bind(TEST_RETENTION_BATCH)
                .execute(pool)
                .await
                .expect("drain the expired workflows")
                .rows_affected();
            if deleted == 0 {
                return;
            }
        }
        panic!("the expired workflows did not drain within {DRAIN_BATCH_LIMIT} batches");
    }

    /// A pool of one connection runs the pass with no gate; a pool whose
    /// connection cannot be checked out skips the interval and does not run
    /// the pass.
    #[tokio::test]
    async fn gate_runs_ungated_only_for_static_single_connection_capacity() {
        let lazy = TestPoolOptions::new().connect_lazy(&crate::test_support::server_url());

        let single = crate::broker::pools::session_core_pool_for_test_url(lazy.url(), 1);
        let mut ran = false;
        let gated = run_under_gate(&single, StatementLog::Off, advisory_key_reaper(), async {
            ran = true;
        })
        .await;
        assert_eq!(gated, GatedPass::Ungated);
        assert!(ran, "an ungated pass runs");

        let unavailable = crate::broker::pools::session_core_pool_for_test_url(lazy.url(), 2);
        unavailable.close();
        let mut ran = false;
        let gated = run_under_gate(
            &unavailable,
            StatementLog::Off,
            advisory_key_reaper(),
            async {
                ran = true;
            },
        )
        .await;
        assert_eq!(gated, GatedPass::Unavailable);
        assert!(!ran, "a pass whose gate cannot be taken does not run");
    }

    /// A gate that another session holds is busy: the pass does not run.
    #[tokio::test]
    async fn a_gate_held_by_another_session_skips_the_pass() {
        let pool = crate::test_support::migrated_pool().await;
        let mut holder = pool.begin().await.expect("begin the holder");
        let taken: bool = sql::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
            .bind(advisory_key_reaper())
            .fetch_one(&mut *holder)
            .await
            .expect("take the gate");
        assert!(taken, "setup: the holder takes the gate");

        let core = crate::broker::pools::session_core_pool_for_test_url(pool.url(), 2);
        let mut ran = false;
        let gated = run_under_gate(&core, StatementLog::Off, advisory_key_reaper(), async {
            ran = true;
        })
        .await;
        holder.rollback().await.expect("release the gate");
        core.close();

        assert_eq!(gated, GatedPass::Busy);
        assert!(!ran, "a pass whose gate another worker holds does not run");
    }

    /// #772: the reaper's gate and pass run on the reserve of the worker
    /// profile pool, so a pass completes while batch checkouts hold every
    /// batch connection.
    #[tokio::test]
    async fn a_reaper_pass_completes_on_the_reserve_while_batch_checkouts_hold_the_pool() {
        let seed = test_pool().await;
        let task_id = Uuid::new_v4();
        insert_stale_running_task(&seed, task_id, "NULL").await;

        // Three connections: one for batch checkouts and the reserve of two.
        let mut config = crate::core::config::PostgresConfig::from_url(
            crate::test_support::database_of_this_test().await.url(),
        );
        config.pool_size = 3;
        config.max_overflow = 0;
        config.pool_timeout = 1;
        let broker = PostgresBroker::connect_worker_with(&config, crate::ClientFamily::Rust)
            .await
            .expect("connect the broker of a worker");
        let open = broker.core_pools_for_read().expect("open the core pools");
        let held = open
            .main()
            .checkout(CheckoutClass::Batch)
            .await
            .expect("check out the one batch connection");
        assert!(
            open.main()
                .checkout_within(CheckoutClass::Batch, Duration::ZERO)
                .await
                .is_err(),
            "the batch checkouts hold every batch connection",
        );

        let recovery = RecoveryConfig {
            auto_requeue_stale_claimed: false,
            auto_terminate_orphaned_workflow_tasks: false,
            auto_fail_stale_running: true,
            ..RecoveryConfig::default()
        };
        let health = new_reaper_health();
        let mut next_retention_cleanup = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut next_partition_maintenance = MaintenanceDue::after(Duration::from_secs(60));
        let mut orphan_state = OrphanSweepState::default();
        let pass = tokio::time::timeout(
            Duration::from_secs(30),
            run_under_gate(
                open.main(),
                StatementLog::Off,
                advisory_key_reaper(),
                run_reaper_pass(
                    &broker,
                    crate::test_support::child_specs::no_child_specs().as_ref(),
                    &recovery,
                    &RetentionConfig::default(),
                    &health,
                    &mut next_retention_cleanup,
                    &mut next_partition_maintenance,
                    &mut orphan_state,
                    &mut UnheldOwners::default(),
                    &mut RecoveryFailures::new(OptionalCounter::NotRecorded),
                    &WorkerTelemetry::Off,
                ),
            ),
        )
        .await;
        drop(held);
        assert_eq!(
            pass.expect("the reaper pass completes on the reserve"),
            GatedPass::Held,
            "the reserve takes the reaper gate",
        );

        let failed: i64 = sql::query_scalar(
            "SELECT count(*) FROM horsies_task_history
             WHERE task_id = $1 AND status = 'FAILED' AND terminalization_kind = 'FAIL_STALE'",
        )
        .bind(task_id)
        .fetch_one(&seed)
        .await
        .unwrap();
        assert_eq!(failed, 1, "the pass failed the stale RUNNING task");

        sql::query("DELETE FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .execute(&seed)
            .await
            .unwrap();
    }

    async fn test_pool() -> TestPool {
        let pool = crate::test_support::migrated_pool().await;
        let outcome = ensure_startup_coverage_in_pool(
            &crate::broker::pools::core_pool_for_test_url(pool.url()),
            crate::broker::core_rows::StatementLog::Off,
            RetentionConfig::default().history_leaf_horizon_days,
            RetentionConfig::default().heartbeat_leaf_horizon_hours,
            &[],
        )
        .await
        .expect("startup coverage");
        assert!(matches!(outcome, StartupCoverageOutcome::Ready(_)));
        sql::query("DELETE FROM horsies_tasks WHERE task_name = 'reaper_test'")
            .execute(&pool)
            .await
            .expect("clean live reaper fixtures");
        sql::query("DELETE FROM horsies_task_history WHERE task_name = 'reaper_test'")
            .execute(&pool)
            .await
            .expect("clean archived reaper fixtures");
        pool
    }

    /// Insert a RUNNING task whose runner heartbeat is already stale, with the
    /// given `finalizing_at` (NULL or a timestamp).
    async fn insert_stale_running_task(pool: &TestPool, task_id: Uuid, finalizing_at_sql: &str) {
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
        sql::query(&sql).bind(task_id).execute(pool).await.unwrap();
    }

    async fn task_status(pool: &TestPool, task_id: Uuid) -> String {
        sql::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// P8: the Phase-1 scan is bounded by `scan_limit`, so one pass processes at
    /// most that many stale tasks and successive passes drain the rest.
    #[tokio::test]
    async fn stale_running_scan_is_bounded_by_limit() {
        let pool = test_pool().await;
        // Clean this test's namespace so only our stale tasks are in play.
        sql::query("DELETE FROM horsies_tasks WHERE task_name = 'reaper_test'")
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

        let state: (i64, i64) = sql::query_as(
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
            sql::query("DELETE FROM horsies_tasks WHERE id = $1")
                .bind(id)
                .execute(&pool)
                .await
                .unwrap();
            sql::query("DELETE FROM horsies_task_history WHERE task_id = $1")
                .bind(id)
                .execute(&pool)
                .await
                .unwrap();
        }
    }

    /// A task actively finalizing (recent finalizing_at) must NOT be reclaimed
    /// by the stale-RUNNING reaper even though its runner heartbeat has stopped.
    #[tokio::test]
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

        sql::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// A task whose finalizing stamp is older than the finalizing threshold is
    /// treated as genuinely stuck and IS reclaimed (marked FAILED).
    #[tokio::test]
    async fn reaper_reclaims_task_finalizing_past_threshold() {
        let pool = test_pool().await;
        let task_id = Uuid::new_v4();
        // finalizing_at well in the past → past the finalizing threshold.
        insert_stale_running_task(&pool, task_id, "NOW() - INTERVAL '1 hour'").await;

        let count = mark_stale_running_as_failed(&pool, 1.0, 300.0, STALE_RUNNING_SCAN_LIMIT)
            .await
            .unwrap();
        assert!(count >= 1, "the targeted stale row must be reclaimed");
        let archived: (String, String) = sql::query_as(
            "SELECT status, terminalization_kind
             FROM horsies_task_history WHERE task_id = $1",
        )
        .bind(task_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(archived, ("FAILED".to_owned(), "FAIL_STALE".to_owned()));

        sql::query("DELETE FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// Retention must NOT delete a terminal+expired workflow's linkage while a
    /// backing task row is still live; once that task is terminal, it sweeps.
    /// Parity with horsies PR #143 (defensive prevent lever).
    #[tokio::test]
    async fn retention_retains_workflow_with_live_backing_task() {
        let pool = test_pool().await;

        // Drain the eligible candidates that the database holds already, so
        // the rows_affected assertions below see only this test's workflow.
        drain_expired_workflows(&pool).await;

        let wf_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();

        // Terminal + expired workflow.
        sql::query(
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
        sql::query(
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

        let wt_count = |pool: TestPool, wf: Uuid| async move {
            sql::query_scalar::<i64>(
                "SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = $1",
            )
            .bind(wf)
            .fetch_one(&pool)
            .await
            .unwrap()
        };

        let wf_count = |pool: TestPool, wf: Uuid| async move {
            sql::query_scalar::<i64>("SELECT COUNT(*) FROM horsies_workflows WHERE id = $1")
                .bind(wf)
                .fetch_one(&pool)
                .await
                .unwrap()
        };

        // hours = 0 → everything terminal+expired qualifies by age; only the
        // live-backing-task guard should hold the workflow (and linkage) back.
        let deleted = sql::query(DELETE_EXPIRED_WORKFLOWS_SQL)
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
        let deleted = sql::query(DELETE_EXPIRED_WORKFLOWS_SQL)
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

        sql::query("DELETE FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// The node budget bounds each statement's node deletions: 4 workflows ×
    /// 3 nodes against budget 6 → two workflows per statement; the empty-batch
    /// drain loop of the reaper's workflow retention still removes the whole
    /// backlog in one `PostgresBroker::delete_expired_workflows` call (a
    /// short-batch heuristic would stop after the first 2-row batch — the
    /// revert-proof). A workflow larger than
    /// the whole budget drains alone instead of starving.
    /// Parity with horsies PR #216.
    #[tokio::test]
    async fn workflow_retention_budgets_nodes_and_drains_on_empty_batch() {
        let pool = test_pool().await;

        // Drain the eligible candidates that the database holds already, so
        // the rows_affected assertions below see only this test's workflows.
        drain_expired_workflows(&pool).await;

        let seed_workflow = |pool: TestPool, nodes: i64| async move {
            let wf_id = Uuid::new_v4();
            sql::query(
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
                sql::query(
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

        let count_workflows = |pool: TestPool| async move {
            sql::query_scalar::<i64>(
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
        let first = sql::query(DELETE_EXPIRED_WORKFLOWS_SQL)
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
        // first 2-row batch above. The seeded workflows are two hours old.
        let total = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust)
            .delete_expired_workflows(WorkflowRetentionPass {
                retention_hours: NonZeroU32::new(1).expect("one hour is above zero"),
                batch_size: NonZeroU32::new(6).expect("six nodes is above zero"),
                budget: Duration::from_secs(60),
            })
            .await
            .expect("the workflow retention pass");
        assert_eq!(total, 2, "drain loop continues past short batches to empty");
        assert_eq!(count_workflows(pool.clone()).await, 0);

        // Jumbo: 9 nodes against budget 4 → drains alone (position = 1 escape).
        let jumbo = seed_workflow(pool.clone(), 9).await;
        let deleted = sql::query(DELETE_EXPIRED_WORKFLOWS_SQL)
            .bind("0")
            .bind(4_i64)
            .execute(&pool)
            .await
            .unwrap()
            .rows_affected();
        assert_eq!(deleted, 1, "over-budget workflow must not starve");
        let nodes_left: i64 =
            sql::query_scalar("SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = $1")
                .bind(jumbo)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(nodes_left, 0, "jumbo's nodes leave with it");
    }

    /// One terminal workflow, two hours old, with one node and the given
    /// events: `run_events` WORKFLOW_PAUSED rows and `node_events`
    /// NODE_READY rows of its node. Returns its id.
    async fn seed_workflow_with_events(pool: &TestPool, run_events: u32, node_events: u32) -> Uuid {
        let workflow_id = Uuid::new_v4();
        let node_id = Uuid::new_v4();
        sql::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at, completed_at
            ) VALUES (
                $1, 'ret_events_wf', 'COMPLETED', 'fail', 'test.ret.v1', 0, $1,
                NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours',
                NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours',
                NOW() - INTERVAL '2 hours'
            )",
        )
        .bind(workflow_id)
        .execute(pool)
        .await
        .expect("seed the workflow");
        sql::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name,
                priority, dependencies, allow_failed_deps, join_type, status,
                is_subworkflow, created_at, max_retries, execution,
                retention_class_key
            ) VALUES (
                'NONE', $1, $2, 0, 'node_0', 'ret_events_task', 'null', 'default', 100,
                '{}', FALSE, 'all', 'COMPLETED', FALSE, NOW() - INTERVAL '2 hours', 0,
                'ISOLATED', 'standard_30d'
            )",
        )
        .bind(node_id)
        .bind(workflow_id)
        .execute(pool)
        .await
        .expect("seed the node");
        for _ in 0..run_events {
            sql::query(
                "INSERT INTO horsies_workflow_events (
                    workflow_id, event_id, event_kind, occurred_at, workflow_node_row_id, task_id,
                    history_class, terminalization_kind
                ) VALUES (
                    $1, nextval('horsies_workflow_event_id'), 'WORKFLOW_PAUSED', NOW() - INTERVAL '2 hours',
                    NULL, NULL, NULL, NULL
                )",
            )
            .bind(workflow_id)
            .execute(pool)
            .await
            .expect("seed a run event");
        }
        for _ in 0..node_events {
            sql::query(
                "INSERT INTO horsies_workflow_events (
                    workflow_id, event_id, event_kind, occurred_at, workflow_node_row_id, task_id,
                    history_class, terminalization_kind
                ) VALUES (
                    $1, nextval('horsies_workflow_event_id'), 'NODE_READY', NOW() - INTERVAL '2 hours',
                    $2, NULL, NULL, NULL
                )",
            )
            .bind(workflow_id)
            .bind(node_id)
            .execute(pool)
            .await
            .expect("seed a node event");
        }
        workflow_id
    }

    /// The events of `workflow_ids`, run events and node events together.
    async fn events_of(pool: &TestPool, workflow_ids: Vec<Uuid>) -> i64 {
        sql::query_scalar(
            "SELECT count(*) FROM horsies_workflow_events WHERE workflow_id = ANY($1)",
        )
        .bind(workflow_ids)
        .fetch_one(pool)
        .await
        .expect("count the events")
    }

    /// The row budget counts the event rows of a workflow with its node
    /// rows, and the statement deletes the events with their workflow: two
    /// workflows of one node and two events (three rows each) against budget
    /// 3 go one per statement (a budget of node rows alone takes both: the
    /// revert-proof). A workflow of one node and five events against budget 4
    /// drains alone, with every event.
    #[tokio::test]
    async fn workflow_retention_counts_events_and_deletes_them_with_the_run() {
        let pool = test_pool().await;
        drain_expired_workflows(&pool).await;

        let first = seed_workflow_with_events(&pool, 1, 1).await;
        let second = seed_workflow_with_events(&pool, 1, 1).await;
        assert_eq!(events_of(&pool, vec![first, second]).await, 4);

        let deleted = sql::query(DELETE_EXPIRED_WORKFLOWS_SQL)
            .bind("0")
            .bind(3_i64)
            .execute(&pool)
            .await
            .expect("the first retention batch")
            .rows_affected();
        assert_eq!(
            deleted, 1,
            "budget 3 admits one workflow of three owned rows"
        );
        assert_eq!(
            events_of(&pool, vec![first, second]).await,
            2,
            "the events of the deleted workflow leave with it; the others stay"
        );

        let deleted = sql::query(DELETE_EXPIRED_WORKFLOWS_SQL)
            .bind("0")
            .bind(3_i64)
            .execute(&pool)
            .await
            .expect("the second retention batch")
            .rows_affected();
        assert_eq!(deleted, 1, "the second workflow goes in the next batch");
        assert_eq!(events_of(&pool, vec![first, second]).await, 0);

        let large = seed_workflow_with_events(&pool, 2, 3).await;
        let deleted = sql::query(DELETE_EXPIRED_WORKFLOWS_SQL)
            .bind("0")
            .bind(4_i64)
            .execute(&pool)
            .await
            .expect("the batch of the large workflow")
            .rows_affected();
        assert_eq!(deleted, 1, "a workflow over the budget drains alone");
        assert_eq!(events_of(&pool, vec![large]).await, 0);
        let nodes_left: i64 =
            sql::query_scalar("SELECT count(*) FROM horsies_workflow_tasks WHERE workflow_id = $1")
                .bind(large)
                .fetch_one(&pool)
                .await
                .expect("count the nodes of the large workflow");
        assert_eq!(nodes_left, 0);
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
    async fn workflow_retention_deletes_use_retention_index() {
        let pool = test_pool().await;

        // Re-runnable after a failed run: drop any leftover seed rows.
        sql::query("DELETE FROM horsies_workflows WHERE name = 'ret_explain_wf'")
            .execute(&pool)
            .await
            .unwrap();

        // Realistic statistics: 500 old terminal workflows (eligible) + 2000
        // recent terminal workflows (in-window). The recent population makes
        // the retention index's cutoff range decisively more selective — with
        // only eligible rows the planner's index pick is arbitrary.
        sql::query(
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
        sql::query(
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
        sql::query("ANALYZE horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();

        // A 500-row table fits in a few pages, so the planner still prefers a
        // seq scan; disable it (transaction-local) to force the index choice a
        // production-sized heap produces on its own. ANALYZE executes the
        // DELETE — the rollback reverts it.
        let mut tx = pool.begin().await.unwrap();
        sql::query("SET LOCAL enable_seqscan = off")
            .execute(&mut *tx)
            .await
            .unwrap();
        let plan_rows: Vec<(String,)> = sql::query_as(&format!(
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
        assert!(
            plan.contains("Delete on horsies_workflow_events"),
            "event rows must be purged set-wise in the statement; plan:\n{plan}",
        );

        sql::query("DELETE FROM horsies_workflows WHERE name = 'ret_explain_wf'")
            .execute(&pool)
            .await
            .unwrap();
    }

    // --- stale CLAIMED requeue and stale RUNNING retry (baseline seeds) ----

    /// A `reaper_test` task with every column of the baseline task row named.
    /// `stale_since` is how long ago it was claimed and started.
    async fn seed_baseline_task(
        pool: &TestPool,
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
        sql::query(
            "INSERT INTO horsies_heartbeats (task_id, role, sent_at, sender_id, hostname, pid)
             VALUES ($1, 'claimer', NOW(), 'worker-1', 'reaper-host', 4242)",
        )
        .bind(beating)
        .execute(&pool)
        .await
        .expect("fresh claimer beat");
        sql::query(
            "UPDATE horsies_tasks
             SET finalizing_at = NOW() - interval '1 hour', finalizing_by_worker_id = 'worker-1'
             WHERE id = $1",
        )
        .bind(marked)
        .execute(&pool)
        .await
        .expect("stamp marker");

        let requeued = requeue_stale_claimed(&pool, 60.0).await.expect("requeue");

        // The count holds every stale claim of the database, not only these.
        assert!(requeued >= 2, "requeued {requeued}");
        assert_eq!(task_status(&pool, silent).await, "PENDING");
        assert_eq!(task_status(&pool, beating).await, "CLAIMED");
        assert_eq!(task_status(&pool, orphan).await, "CLAIMED");
        assert_eq!(task_status(&pool, marked).await, "PENDING");
        let (claimed_at, owner, marker): (
            Option<chrono::DateTime<chrono::Utc>>,
            Option<String>,
            Option<chrono::DateTime<chrono::Utc>>,
        ) = sql::query_as(
            "SELECT claimed_at, claimed_by_worker_id, finalizing_at FROM horsies_tasks WHERE id = $1",
        )
        .bind(marked)
        .fetch_one(&pool)
        .await
        .expect("requeued row");
        assert_eq!((claimed_at, owner, marker), (None, None, None));
        sql::query("DELETE FROM horsies_heartbeats WHERE task_id = $1")
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

        // The count holds every stale row of the database, not only this one.
        assert!(processed >= 1, "processed {processed}");
        let row: serde_json::Value =
            sql::query_scalar("SELECT to_jsonb(t) FROM horsies_tasks t WHERE id = $1")
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
        let attempts: Vec<(i32, String, bool, Option<String>)> = sql::query_as(
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
        sql::query("DELETE FROM horsies_task_attempts WHERE task_id = $1")
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
mod stored_form_vector_tests;

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
