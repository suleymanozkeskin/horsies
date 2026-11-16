//! Bounded recovery of workflow progression from terminalization evidence.

use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::history::phase2::consumption::{
    consume_phase2, Phase2Disposition, Phase2DispositionKind,
};
use crate::core::history::phase2::quarantine::{
    quarantine_one, QuarantineOutcome, QuarantineVerdict,
};
use crate::core::types::status::TaskStatus;
use crate::workflow_engine::engine::apply_phase2_progression_in_tx;

use super::WorkflowError;

const DISCOVER_PENDING_SQL: &str = "
SELECT task_id, terminal_status
FROM horsies_workflow_phase2_pending
WHERE created_at < NOW() - ($1::double precision / 1000.0) * INTERVAL '1 second'
  AND attempt_count < $2::integer
ORDER BY created_at, task_id
LIMIT $3::bigint";

const RECORD_RETAINING_ATTEMPT_SQL: &str = "
UPDATE horsies_workflow_phase2_pending
SET attempt_count = attempt_count + 1,
    last_attempt_at = statement_timestamp(),
    last_failure_class = $2
WHERE task_id = $1
RETURNING attempt_count";

const COUNT_OVER_ATTEMPT_BOUND_SQL: &str = "
SELECT count(*) FROM horsies_workflow_phase2_pending
WHERE attempt_count >= $1::integer";

#[derive(Debug, FromRow)]
struct PendingCandidate {
    task_id: Uuid,
    terminal_status: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct Phase2RecoverySummary {
    pub considered: u64,
    pub applied: u64,
    pub already_applied: u64,
    pub superseded: u64,
    pub retained: u64,
    pub failed: u64,
    pub quarantined: u64,
    pub over_attempt_bound: i64,
    pub retained_details: Vec<String>,
    pub quarantine_refusals: Vec<String>,
}

/// What the quarantine step did for one row of a recovery pass.
#[derive(Debug, Clone, PartialEq, Eq)]
enum QuarantineStep {
    /// The row is under the attempt bound, or its disposition is durable.
    NotDue,
    /// The pending row points at a verified copy, or was already drained.
    Quarantined { reason: String },
    /// The quarantine function refused; the pending row keeps its locator.
    Refused { refusal: String },
}

/// Classifies one quarantine verdict as Python's `_quarantine_exhausted_row`
/// does (`horsies/core/workflows/phase2_recovery.py:701-725`): a repoint or a
/// drained verdict is quarantined; every other verdict is a refusal.
fn quarantine_step(
    task_id: Uuid,
    attempt_count: i32,
    disposition: Phase2DispositionKind,
    outcome: &QuarantineOutcome,
) -> QuarantineStep {
    match outcome.verdict {
        QuarantineVerdict::Repointed
        | QuarantineVerdict::PendingGone
        | QuarantineVerdict::AlreadyQuarantined => QuarantineStep::Quarantined {
            reason: format!(
                "attempt bound {attempt_count} reached; last disposition {}",
                disposition.as_str()
            ),
        },
        QuarantineVerdict::NodeRowAbsent
        | QuarantineVerdict::SourceAbsent
        | QuarantineVerdict::CopyVerificationFailed => QuarantineStep::Refused {
            refusal: format!(
                "{task_id}: quarantine refused with {}{}",
                outcome.verdict.as_str(),
                outcome
                    .detail
                    .as_deref()
                    .map(|detail| format!(": {detail}"))
                    .unwrap_or_default()
            ),
        },
    }
}

fn node_status_for_terminal_task(status: &str) -> Result<&'static str, WorkflowError> {
    let status: TaskStatus = status
        .parse()
        .map_err(|error: String| WorkflowError::InvalidStatus(error))?;
    if !status.is_terminal() {
        return Err(WorkflowError::InvalidStatus(format!(
            "node status requested for non-terminal task status {status}"
        )));
    }
    Ok(if status == TaskStatus::Completed {
        "COMPLETED"
    } else {
        "FAILED"
    })
}

async fn apply_progression(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    disposition: &Phase2Disposition,
    registry: &crate::core::registry::workflow::WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    let (Some(workflow_id), Some(task_index), Some(node_status)) = (
        disposition.workflow_id,
        disposition.task_index,
        disposition.node_status.as_deref(),
    ) else {
        return Err(WorkflowError::Validation(
            "APPLIED_TO_NODE omitted progression context".to_owned(),
        ));
    };
    apply_phase2_progression_in_tx(
        transaction,
        workflow_id,
        disposition.node_row_id,
        task_index,
        node_status,
        disposition.terminal_status.as_deref(),
        disposition.on_error.as_deref(),
        disposition.workflow_status.as_deref(),
        registry,
        payload,
        retention,
    )
    .await
}

/// Healthy finalizer path: consume one pending row and apply every remaining
/// workflow effect in one caller-owned transaction. The outbox row is crash
/// evidence, not a second progression mechanism for successful finalizers.
pub async fn finalize_phase2(
    pool: &PgPool,
    task_id: Uuid,
    terminal_status: &str,
    registry: &crate::core::registry::workflow::WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<Phase2Disposition, WorkflowError> {
    let node_status = node_status_for_terminal_task(terminal_status)?;
    let mut transaction = pool.begin().await?;
    let disposition = consume_phase2(&mut transaction, task_id, node_status)
        .await
        .map_err(|error| WorkflowError::Validation(error.to_string()))?;
    if disposition.disposition == Phase2DispositionKind::AppliedToNode {
        apply_progression(&mut transaction, &disposition, registry, payload, retention).await?;
    }
    transaction.commit().await?;
    Ok(disposition)
}

pub async fn drive_phase2_recovery(
    pool: &PgPool,
    registry: &crate::core::registry::workflow::WorkflowSpecRegistry,
    grace_ms: u64,
    max_rows: i64,
    quarantine_after_attempts: u32,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<Phase2RecoverySummary, WorkflowError> {
    let candidates = sqlx::query_as::<_, PendingCandidate>(DISCOVER_PENDING_SQL)
        .bind(grace_ms as f64)
        .bind(quarantine_after_attempts as i32)
        .bind(max_rows)
        .fetch_all(pool)
        .await?;
    let over_attempt_bound = sqlx::query_scalar(COUNT_OVER_ATTEMPT_BOUND_SQL)
        .bind(quarantine_after_attempts as i32)
        .fetch_one(pool)
        .await?;
    let mut summary = Phase2RecoverySummary {
        over_attempt_bound,
        ..Phase2RecoverySummary::default()
    };
    for candidate in candidates {
        summary.considered += 1;
        let node_status = match node_status_for_terminal_task(&candidate.terminal_status) {
            Ok(status) => status,
            Err(error) => {
                summary.failed += 1;
                tracing::error!(task_id = %candidate.task_id, %error, "phase-2 status refused");
                continue;
            }
        };
        let mut transaction = match pool.begin().await {
            Ok(transaction) => transaction,
            Err(error) => {
                summary.failed += 1;
                tracing::error!(task_id = %candidate.task_id, %error, "phase-2 transaction failed");
                continue;
            }
        };
        let row_result: Result<(Phase2Disposition, QuarantineStep), WorkflowError> = async {
            let disposition = consume_phase2(&mut transaction, candidate.task_id, node_status)
                .await
                .map_err(|error| WorkflowError::Validation(error.to_string()))?;
            if disposition.disposition == Phase2DispositionKind::AppliedToNode {
                apply_progression(&mut transaction, &disposition, registry, payload, retention)
                    .await?;
            }
            let mut quarantine = QuarantineStep::NotDue;
            if !disposition.disposition.is_durable() {
                let attempt_count: i32 = sqlx::query_scalar(RECORD_RETAINING_ATTEMPT_SQL)
                    .bind(candidate.task_id)
                    .bind(disposition.disposition.as_str())
                    .fetch_one(transaction.as_mut())
                    .await?;
                if attempt_count >= quarantine_after_attempts as i32 {
                    let outcome = quarantine_one(&mut transaction, candidate.task_id)
                        .await
                        .map_err(|error| WorkflowError::Validation(error.to_string()))?;
                    quarantine = quarantine_step(
                        candidate.task_id,
                        attempt_count,
                        disposition.disposition,
                        &outcome,
                    );
                }
            }
            transaction.commit().await?;
            Ok((disposition, quarantine))
        }
        .await;
        let (disposition, quarantine) = match row_result {
            Ok(result) => result,
            Err(error) => {
                summary.failed += 1;
                tracing::error!(task_id = %candidate.task_id, %error, "phase-2 row failed");
                continue;
            }
        };
        // Logged after the commit, so a failed commit logs no quarantine.
        // Python logs both at error level (`phase2_recovery.py:716-724`).
        match &quarantine {
            QuarantineStep::NotDue => {}
            QuarantineStep::Quarantined { reason } => {
                tracing::error!(task_id = %candidate.task_id, %reason, "phase-2 recovery quarantined the task");
            }
            QuarantineStep::Refused { refusal } => {
                tracing::error!(%refusal, "phase-2 recovery quarantine refused");
            }
        }
        match disposition.disposition {
            Phase2DispositionKind::AppliedToNode => summary.applied += 1,
            Phase2DispositionKind::AlreadyApplied => summary.already_applied += 1,
            Phase2DispositionKind::SupersededByWorkflowTerminal => summary.superseded += 1,
            retaining => {
                summary.retained += 1;
                summary.retained_details.push(format!(
                    "{}: {}{}",
                    candidate.task_id,
                    retaining.as_str(),
                    disposition
                        .detail
                        .as_deref()
                        .map(|detail| format!(": {detail}"))
                        .unwrap_or_default()
                ));
                match quarantine {
                    QuarantineStep::NotDue => {}
                    QuarantineStep::Quarantined { .. } => summary.quarantined += 1,
                    QuarantineStep::Refused { refusal } => {
                        summary.quarantine_refusals.push(refusal);
                    }
                }
            }
        }
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::terminalization::terminalize;
    use crate::core::config::payload::PayloadPolicy;
    use crate::core::config::retention::RetentionConfig;
    use crate::core::lifecycle::{PriorLockedRead, TerminalizationCommand, TerminalizationOutcome};
    use crate::core::types::status::TASK_TERMINAL_STATES;
    use serial_test::serial;

    async fn seed_workflow_task(
        pool: &PgPool,
        workflow_id: Uuid,
        task_id: Uuid,
        with_dependent: bool,
    ) {
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
             ) VALUES ($1, 'p7_phase2', 'RUNNING', 'fail', NULL,
                       'test.p7.phase2.v1', 0, $1, NOW(), NOW(), NOW(), NOW())",
        )
        .bind(workflow_id)
        .execute(pool)
        .await
        .expect("seed workflow");
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, input, status, sent_at, enqueued_at,
                started_at, claimed_at, claimed_by_worker_id, is_workflow_task, retry_count,
                max_retries, command_fingerprint_version, command_fingerprint,
                retention_class_key, retain_rerun_input, created_at, updated_at, execution
            ) VALUES (
                $1, 'p7_phase2_root', 'default', 100, 'null', 'RUNNING', NOW(), NOW(), NOW(),
                NOW(), 'p7-worker', TRUE, 0, 0, NULL, NULL, 'forever', FALSE, NOW(), NOW(),
                'ISOLATED'
            )",
        )
        .bind(task_id)
        .execute(pool)
        .await
        .expect("seed task");
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, task_id,
                created_at, max_retries, execution, retention_class_key
            ) VALUES (
                $1, $2, 0, 'root', 'p7_phase2_root', 'null', 'default', 100, '{}', FALSE,
                'all', 'RUNNING', FALSE, $3, NOW(), 0, 'ISOLATED', 'standard_30d'
            )",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .bind(task_id)
        .execute(pool)
        .await
        .expect("seed workflow task");
        if with_dependent {
            sqlx::query(
                "INSERT INTO horsies_workflow_tasks (
                    id, workflow_id, task_index, node_id, task_name, input, queue_name,
                    priority, dependencies, allow_failed_deps, join_type, status,
                    is_subworkflow, created_at, max_retries, execution, retention_class_key
                ) VALUES (
                    $1, $2, 1, 'dependent', 'p7_phase2_dependent', 'null', 'default', 100,
                    '{0}', FALSE, 'all', 'PENDING', FALSE, NOW(), 0, 'ISOLATED',
                    'standard_30d'
                )",
            )
            .bind(Uuid::new_v4())
            .bind(workflow_id)
            .execute(pool)
            .await
            .expect("seed dependent");
        }
    }

    async fn terminalize_workflow_task(pool: &PgPool, task_id: Uuid) {
        let result = serde_json::to_string(&crate::core::TaskResult::Ok(
            serde_json::json!({"value": 7}),
        ))
        .unwrap();
        let outcomes = terminalize(
            pool,
            &TerminalizationCommand::CompleteLockedTask {
                task_id,
                fence: PriorLockedRead {
                    worker_id: "p7-worker".to_owned(),
                },
                result_json: result,
            },
        )
        .await
        .expect("terminalize workflow task");
        assert!(matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ));
    }

    async fn cleanup_case(pool: &PgPool, workflow_id: Uuid, task_id: Uuid) {
        sqlx::query("DELETE FROM horsies_workflow_phase2_pending WHERE task_id = $1")
            .bind(task_id)
            .execute(pool)
            .await
            .expect("remove the pending recovery fixture");
        sqlx::query(
            "DELETE FROM horsies_tasks WHERE id IN (
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = $1 AND task_id IS NOT NULL
             )",
        )
        .bind(workflow_id)
        .execute(pool)
        .await
        .ok();
        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(workflow_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .execute(pool)
            .await
            .ok();
    }

    #[test]
    fn every_terminal_task_status_maps_and_nonterminal_statuses_refuse() {
        for status in TASK_TERMINAL_STATES {
            let mapped = node_status_for_terminal_task(&status.to_string()).expect("terminal");
            assert_eq!(
                mapped,
                if *status == TaskStatus::Completed {
                    "COMPLETED"
                } else {
                    "FAILED"
                }
            );
        }
        for status in [
            TaskStatus::Pending,
            TaskStatus::Claimed,
            TaskStatus::Running,
        ] {
            assert!(node_status_for_terminal_task(&status.to_string()).is_err());
        }
        assert!(node_status_for_terminal_task("UNKNOWN").is_err());
    }

    #[test]
    fn durable_vocabulary_is_exact() {
        assert_eq!(Phase2DispositionKind::DURABLE.len(), 3);
        assert!(Phase2DispositionKind::AppliedToNode.is_durable());
        assert!(!Phase2DispositionKind::SourceAbsent.is_durable());
        assert!(DISCOVER_PENDING_SQL.contains("ORDER BY created_at, task_id"));
        assert!(DISCOVER_PENDING_SQL.contains("attempt_count <"));
        assert!(DISCOVER_PENDING_SQL.contains("LIMIT"));
    }

    #[tokio::test]
    async fn discovery_failure_is_typed_instead_of_published_as_zero_health() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://postgres@localhost/postgres")
            .unwrap();
        pool.close().await;
        let error = drive_phase2_recovery(
            &pool,
            &crate::core::WorkflowSpecRegistry::new(),
            0,
            1,
            3,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect_err("a failed discovery/count query must cross the containment seam");
        assert!(matches!(error, WorkflowError::Database(_)));
    }

    #[tokio::test]
    #[serial]
    async fn healthy_finalizer_consumes_outbox_promotes_dependent_and_replays_idempotently() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        seed_workflow_task(&pool, workflow_id, task_id, true).await;
        terminalize_workflow_task(&pool, task_id).await;

        let live: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM horsies_tasks WHERE id = $1)")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(!live, "phase 1 moved the terminal row before progression");

        let disposition = finalize_phase2(
            &pool,
            task_id,
            "COMPLETED",
            &crate::core::WorkflowSpecRegistry::new(),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("healthy phase 2");
        assert_eq!(
            disposition.disposition,
            Phase2DispositionKind::AppliedToNode
        );

        let statuses: Vec<(
            i32,
            String,
            Option<Uuid>,
            Option<chrono::DateTime<chrono::Utc>>,
            Option<chrono::DateTime<chrono::Utc>>,
        )> = sqlx::query_as(
            "SELECT task_index, status, task_id, started_at, completed_at
             FROM horsies_workflow_tasks
             WHERE workflow_id = $1 ORDER BY task_index",
        )
        .bind(workflow_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(statuses[0].0, 0);
        assert_eq!(statuses[0].1, "COMPLETED");
        assert_eq!(statuses[0].2, Some(task_id));
        assert_eq!(statuses[1].0, 1);
        assert_eq!(statuses[1].1, "ENQUEUED");
        assert_eq!(statuses[1].2.expect("dependent task").get_version_num(), 7);
        assert!(statuses[0].4.is_some(), "source completion must be stamped");
        assert!(
            statuses[1].3.is_none(),
            "ENQUEUED is not a workflow-node start",
        );

        let replay = finalize_phase2(
            &pool,
            task_id,
            "COMPLETED",
            &crate::core::WorkflowSpecRegistry::new(),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("idempotent phase-2 replay");
        assert_eq!(replay.disposition, Phase2DispositionKind::AlreadyApplied);
        cleanup_case(&pool, workflow_id, task_id).await;
    }

    #[tokio::test]
    #[serial]
    async fn recovery_contains_retaining_row_advances_healthy_row_and_quarantines_at_bound() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        sqlx::query("DELETE FROM horsies_workflow_phase2_pending")
            .execute(&pool)
            .await
            .expect("clear pending rows before global recovery");
        let bad_workflow = Uuid::new_v4();
        let bad_task = Uuid::new_v4();
        let good_workflow = Uuid::new_v4();
        let good_task = Uuid::new_v4();
        seed_workflow_task(&pool, bad_workflow, bad_task, false).await;
        seed_workflow_task(&pool, good_workflow, good_task, false).await;
        terminalize_workflow_task(&pool, bad_task).await;
        terminalize_workflow_task(&pool, good_task).await;
        sqlx::query(
            "UPDATE horsies_workflow_phase2_pending
             SET result_digest = decode(repeat('ff', 32), 'hex'),
                 created_at = NOW() - INTERVAL '2 hours'
             WHERE task_id = $1",
        )
        .bind(bad_task)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE horsies_workflow_phase2_pending
             SET created_at = NOW() - INTERVAL '1 hour'
             WHERE task_id = $1",
        )
        .bind(good_task)
        .execute(&pool)
        .await
        .unwrap();

        let first = drive_phase2_recovery(
            &pool,
            &crate::core::WorkflowSpecRegistry::new(),
            0,
            2,
            2,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("first recovery pass");
        assert_eq!(first.considered, 2);
        assert_eq!(first.applied, 1, "{first:?}");
        assert_eq!(first.retained, 1);
        assert_eq!(first.failed, 0);
        assert_eq!(first.quarantined, 0);

        let second = drive_phase2_recovery(
            &pool,
            &crate::core::WorkflowSpecRegistry::new(),
            0,
            2,
            2,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("second recovery pass");
        assert_eq!(second.considered, 1);
        assert_eq!(second.retained, 1);
        assert_eq!(second.quarantined, 0);
        assert_eq!(second.quarantine_refusals.len(), 1);
        let (source, attempts): (String, i32) = sqlx::query_as(
            "SELECT recovery_source, attempt_count
             FROM horsies_workflow_phase2_pending WHERE task_id = $1",
        )
        .bind(bad_task)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(source, "HISTORY");
        assert_eq!(attempts, 2);

        let excluded = drive_phase2_recovery(
            &pool,
            &crate::core::WorkflowSpecRegistry::new(),
            0,
            2,
            2,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("over-bound recovery pass");
        assert_eq!(excluded.considered, 0);
        assert_eq!(excluded.over_attempt_bound, 1);
        cleanup_case(&pool, bad_workflow, bad_task).await;
        cleanup_case(&pool, good_workflow, good_task).await;
    }

    #[tokio::test]
    #[serial]
    async fn recovery_rolls_back_one_failed_row_and_advances_the_next_candidate() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        sqlx::query("DELETE FROM horsies_workflow_phase2_pending")
            .execute(&pool)
            .await
            .expect("clear pending rows before global recovery");
        let bad_workflow = Uuid::new_v4();
        let bad_task = Uuid::new_v4();
        let good_workflow = Uuid::new_v4();
        let good_task = Uuid::new_v4();
        seed_workflow_task(&pool, bad_workflow, bad_task, false).await;
        seed_workflow_task(&pool, good_workflow, good_task, false).await;
        terminalize_workflow_task(&pool, bad_task).await;
        terminalize_workflow_task(&pool, good_task).await;
        sqlx::query(
            "UPDATE horsies_workflow_phase2_pending
             SET created_at = NOW() - CASE task_id
                 WHEN $1 THEN INTERVAL '2 hours' ELSE INTERVAL '1 hour' END
             WHERE task_id IN ($1, $2)",
        )
        .bind(bad_task)
        .bind(good_task)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(&format!(
            "CREATE OR REPLACE FUNCTION p7_phase2_inject_row_failure() RETURNS trigger
             LANGUAGE plpgsql AS $body$
             BEGIN
                 IF NEW.workflow_id = '{bad_workflow}'::uuid THEN
                     RAISE EXCEPTION 'injected phase-2 row failure';
                 END IF;
                 RETURN NEW;
             END
             $body$",
        ))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "CREATE TRIGGER p7_phase2_inject_row_failure
             BEFORE UPDATE ON horsies_workflow_tasks FOR EACH ROW
             EXECUTE FUNCTION p7_phase2_inject_row_failure()",
        )
        .execute(&pool)
        .await
        .unwrap();

        let summary = drive_phase2_recovery(
            &pool,
            &crate::core::WorkflowSpecRegistry::new(),
            0,
            2,
            3,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("contained recovery pass");
        assert_eq!(summary.considered, 2, "{summary:?}");
        assert_eq!(summary.failed, 1, "{summary:?}");
        assert_eq!(summary.applied, 1, "{summary:?}");
        assert_eq!(summary.retained, 0, "{summary:?}");
        let states: (String, i32, i64, String, i64) = sqlx::query_as(
            "SELECT
                 (SELECT status FROM horsies_workflow_tasks
                  WHERE workflow_id = $1 AND task_index = 0),
                 (SELECT attempt_count FROM horsies_workflow_phase2_pending
                  WHERE task_id = $2),
                 (SELECT count(*) FROM horsies_task_history WHERE task_id = $2),
                 (SELECT status FROM horsies_workflow_tasks
                  WHERE workflow_id = $3 AND task_index = 0),
                 (SELECT count(*) FROM horsies_workflow_phase2_pending WHERE task_id = $4)",
        )
        .bind(bad_workflow)
        .bind(bad_task)
        .bind(good_workflow)
        .bind(good_task)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            states,
            ("RUNNING".to_owned(), 0, 1, "COMPLETED".to_owned(), 0)
        );

        sqlx::query("DROP TRIGGER p7_phase2_inject_row_failure ON horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP FUNCTION p7_phase2_inject_row_failure()")
            .execute(&pool)
            .await
            .unwrap();
        cleanup_case(&pool, bad_workflow, bad_task).await;
        cleanup_case(&pool, good_workflow, good_task).await;
    }

    #[tokio::test]
    #[serial]
    async fn pause_records_the_exact_phase2_node_failure_not_an_earlier_failure() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        let prior_task = Uuid::new_v4();
        let current_task = Uuid::new_v4();
        seed_workflow_task(&pool, workflow_id, prior_task, false).await;
        let prior_error =
            crate::core::TaskError::new("PRIOR_FAILURE", "prior node failed").unwrap();
        let current_error =
            crate::core::TaskError::new("CURRENT_FAILURE", "current node failed").unwrap();
        let prior_result = serde_json::to_string(
            &crate::core::TaskResult::<serde_json::Value>::Err(prior_error),
        )
        .unwrap();
        sqlx::query("UPDATE horsies_workflows SET on_error = 'pause' WHERE id = $1;")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE horsies_workflow_tasks
             SET status = 'FAILED', result = $2, completed_at = NOW()
             WHERE workflow_id = $1 AND task_index = 0",
        )
        .bind(workflow_id)
        .bind(&prior_result)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, input, status, sent_at, enqueued_at,
                started_at, claimed_at, claimed_by_worker_id, is_workflow_task, retry_count,
                max_retries, command_fingerprint_version, command_fingerprint,
                retention_class_key, retain_rerun_input, created_at, updated_at, execution
            ) VALUES (
                $1, 'p7_phase2_current', 'default', 100, 'null', 'RUNNING', NOW(), NOW(),
                NOW(), NOW(), 'p7-worker', TRUE, 0, 0, NULL, NULL, 'forever', FALSE, NOW(),
                NOW(), 'ISOLATED'
            )",
        )
        .bind(current_task)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, task_id,
                created_at, max_retries, execution, retention_class_key
            ) VALUES (
                $1, $2, 1, 'current', 'p7_phase2_current', 'null', 'default', 100, '{}', FALSE,
                'all', 'RUNNING', FALSE, $3, NOW(), 0, 'ISOLATED', 'standard_30d'
            )",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .bind(current_task)
        .execute(&pool)
        .await
        .unwrap();
        let current_result = serde_json::to_string(
            &crate::core::TaskResult::<serde_json::Value>::Err(current_error.clone()),
        )
        .unwrap();
        let outcomes = terminalize(
            &pool,
            &TerminalizationCommand::FailLockedTask {
                task_id: current_task,
                fence: PriorLockedRead {
                    worker_id: "p7-worker".to_owned(),
                },
                result_json: current_result,
                error_code: Some("CURRENT_FAILURE".to_owned()),
                failed_reason: Some("current node failed".to_owned()),
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ));
        let disposition = finalize_phase2(
            &pool,
            current_task,
            "FAILED",
            &crate::core::WorkflowSpecRegistry::new(),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            disposition.disposition,
            Phase2DispositionKind::AppliedToNode
        );
        let (status, error_json): (String, String) =
            sqlx::query_as("SELECT status, error FROM horsies_workflows WHERE id = $1")
                .bind(workflow_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "PAUSED");
        let stored: crate::core::TaskError = serde_json::from_str(&error_json).unwrap();
        assert_eq!(stored.error_code, current_error.error_code);
        assert_eq!(stored.message, current_error.message);
        cleanup_case(&pool, workflow_id, current_task).await;
        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(prior_task)
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[serial]
    async fn terminal_workflow_supersedes_pending_evidence_without_touching_the_node() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        for workflow_status in ["COMPLETED", "FAILED", "CANCELLED", "EXPIRED"] {
            let workflow_id = Uuid::new_v4();
            let task_id = Uuid::new_v4();
            seed_workflow_task(&pool, workflow_id, task_id, false).await;
            terminalize_workflow_task(&pool, task_id).await;
            sqlx::query(
                "UPDATE horsies_workflows
                 SET status = $2, completed_at = NOW(), updated_at = NOW()
                 WHERE id = $1",
            )
            .bind(workflow_id)
            .bind(workflow_status)
            .execute(&pool)
            .await
            .unwrap();

            let disposition = finalize_phase2(
                &pool,
                task_id,
                "COMPLETED",
                &crate::core::WorkflowSpecRegistry::new(),
                &PayloadPolicy::default(),
                &RetentionConfig::default(),
            )
            .await
            .unwrap();
            assert_eq!(
                disposition.disposition,
                Phase2DispositionKind::SupersededByWorkflowTerminal,
                "{workflow_status}",
            );
            let state: (String, i64) = sqlx::query_as(
                "SELECT wt.status,
                        (SELECT count(*) FROM horsies_workflow_phase2_pending p
                         WHERE p.task_id = $2)
                 FROM horsies_workflow_tasks wt
                 WHERE wt.workflow_id = $1 AND wt.task_index = 0",
            )
            .bind(workflow_id)
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(state, ("RUNNING".to_owned(), 0), "{workflow_status}");
            cleanup_case(&pool, workflow_id, task_id).await;
        }
    }

    #[tokio::test]
    #[serial]
    async fn every_retaining_disposition_preserves_available_evidence() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;

        let mut transaction = pool.begin().await.unwrap();
        let absent = consume_phase2(&mut transaction, Uuid::new_v4(), "FAILED")
            .await
            .unwrap();
        assert_eq!(absent.disposition, Phase2DispositionKind::PendingAbsent);
        transaction.rollback().await.unwrap();

        let source_absent_workflow = Uuid::new_v4();
        let source_absent_task = Uuid::new_v4();
        seed_workflow_task(&pool, source_absent_workflow, source_absent_task, false).await;
        terminalize_workflow_task(&pool, source_absent_task).await;
        sqlx::query(
            "UPDATE horsies_workflow_phase2_pending
             SET history_anchor = history_anchor + INTERVAL '90 days'
             WHERE task_id = $1",
        )
        .bind(source_absent_task)
        .execute(&pool)
        .await
        .unwrap();
        let mut transaction = pool.begin().await.unwrap();
        let absent_source = consume_phase2(&mut transaction, source_absent_task, "FAILED")
            .await
            .unwrap();
        assert_eq!(
            absent_source.disposition,
            Phase2DispositionKind::SourceAbsent
        );
        transaction.commit().await.unwrap();
        let still_pending: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = $1)",
        )
        .bind(source_absent_task)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(still_pending);
        cleanup_case(&pool, source_absent_workflow, source_absent_task).await;

        let version_workflow = Uuid::new_v4();
        let version_task = Uuid::new_v4();
        seed_workflow_task(&pool, version_workflow, version_task, false).await;
        terminalize_workflow_task(&pool, version_task).await;
        sqlx::query(
            "UPDATE horsies_workflow_phase2_pending
             SET history_schema_version = 2 WHERE task_id = $1",
        )
        .bind(version_task)
        .execute(&pool)
        .await
        .unwrap();
        let mut transaction = pool.begin().await.unwrap();
        let version = consume_phase2(&mut transaction, version_task, "FAILED")
            .await
            .unwrap();
        assert_eq!(
            version.disposition,
            Phase2DispositionKind::SourceVersionConflict
        );
        transaction.commit().await.unwrap();
        let still_pending: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = $1)",
        )
        .bind(version_task)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(still_pending);
        cleanup_case(&pool, version_workflow, version_task).await;

        let state_workflow = Uuid::new_v4();
        let state_task = Uuid::new_v4();
        seed_workflow_task(&pool, state_workflow, state_task, false).await;
        terminalize_workflow_task(&pool, state_task).await;
        let mut transaction = pool.begin().await.unwrap();
        sqlx::query(
            "ALTER TABLE horsies_workflow_phase2_pending
             DROP CONSTRAINT horsies_workflow_phase2_pending_node_fkey",
        )
        .execute(transaction.as_mut())
        .await
        .unwrap();
        sqlx::query(
            "DELETE FROM horsies_workflow_tasks
             WHERE workflow_id = $1 AND task_index = 0",
        )
        .bind(state_workflow)
        .execute(transaction.as_mut())
        .await
        .unwrap();
        let state = consume_phase2(&mut transaction, state_task, "FAILED")
            .await
            .unwrap();
        assert_eq!(
            state.disposition,
            Phase2DispositionKind::SourceStateConflict
        );
        let still_pending: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = $1)",
        )
        .bind(state_task)
        .fetch_one(transaction.as_mut())
        .await
        .unwrap();
        assert!(still_pending);
        transaction.rollback().await.unwrap();
        cleanup_case(&pool, state_workflow, state_task).await;
    }

    #[tokio::test]
    #[serial]
    async fn discovery_is_oldest_first_strictly_graced_and_capped() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let mut cases = Vec::new();
        for age in ["3 hours", "2 hours", "1 minute"] {
            let workflow_id = Uuid::new_v4();
            let task_id = Uuid::new_v4();
            seed_workflow_task(&pool, workflow_id, task_id, false).await;
            terminalize_workflow_task(&pool, task_id).await;
            sqlx::query(
                "UPDATE horsies_workflow_phase2_pending
                 SET result_digest = decode(repeat('ff', 32), 'hex'),
                     created_at = NOW() - $2::interval
                 WHERE task_id = $1",
            )
            .bind(task_id)
            .bind(age)
            .execute(&pool)
            .await
            .unwrap();
            cases.push((workflow_id, task_id));
        }

        let summary = drive_phase2_recovery(
            &pool,
            &crate::core::WorkflowSpecRegistry::new(),
            60 * 60 * 1_000,
            1,
            25,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("graced recovery pass");
        assert_eq!(summary.considered, 1);
        assert_eq!(summary.retained, 1);
        let attempts: Vec<i32> = sqlx::query_scalar(
            "SELECT attempt_count FROM horsies_workflow_phase2_pending
             WHERE task_id = ANY($1::uuid[])
             ORDER BY created_at, task_id",
        )
        .bind(
            cases
                .iter()
                .map(|(_, task_id)| *task_id)
                .collect::<Vec<_>>(),
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(attempts, vec![1, 0, 0]);
        for (workflow_id, task_id) in cases {
            cleanup_case(&pool, workflow_id, task_id).await;
        }
    }

    /// The quarantine step of the recovery loop on the baseline function
    /// (#861): a row whose consumption keeps retaining is quarantined at the
    /// attempt bound, and its pending row points at the verified copy.
    /// Python: `horsies/core/workflows/phase2_recovery.py:681-725`
    /// (`_quarantine_exhausted_row`). The history row and the pending row
    /// agree on schema version 2: the consumer refuses it
    /// (`SOURCE_VERSION_CONFLICT`), and the copy check accepts it.
    #[tokio::test]
    #[serial]
    async fn the_recovery_loop_quarantines_a_retaining_row_at_the_attempt_bound() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        sqlx::query("DELETE FROM horsies_workflow_phase2_pending")
            .execute(&pool)
            .await
            .expect("clear pending rows before global recovery");
        let workflow_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        seed_workflow_task(&pool, workflow_id, task_id, false).await;
        terminalize_workflow_task(&pool, task_id).await;
        sqlx::query(
            "UPDATE horsies_task_history SET history_schema_version = 2 WHERE task_id = $1",
        )
        .bind(task_id)
        .execute(&pool)
        .await
        .expect("set the history schema version");
        sqlx::query(
            "UPDATE horsies_workflow_phase2_pending
             SET history_schema_version = 2,
                 created_at = NOW() - INTERVAL '1 hour'
             WHERE task_id = $1",
        )
        .bind(task_id)
        .execute(&pool)
        .await
        .expect("age the pending row");

        let summary = drive_phase2_recovery(
            &pool,
            &crate::core::WorkflowSpecRegistry::new(),
            0,
            1,
            1,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("recovery pass");

        assert_eq!(summary.considered, 1, "{summary:?}");
        assert_eq!(summary.failed, 0, "{summary:?}");
        assert_eq!(summary.retained, 1, "{summary:?}");
        assert_eq!(summary.quarantined, 1, "{summary:?}");
        assert!(summary.quarantine_refusals.is_empty(), "{summary:?}");
        let (source, attempts, failure_class, copies): (String, i32, Option<String>, i64) =
            sqlx::query_as(
                "SELECT p.recovery_source, p.attempt_count, p.last_failure_class,
                        (SELECT count(*) FROM horsies_workflow_phase2_quarantine AS q
                         WHERE q.task_id = p.task_id)
                 FROM horsies_workflow_phase2_pending AS p
                 WHERE p.task_id = $1",
            )
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .expect("read the pending row");
        assert_eq!(source, "QUARANTINE");
        assert_eq!(attempts, 1);
        assert_eq!(failure_class.as_deref(), Some("SOURCE_VERSION_CONFLICT"));
        assert_eq!(copies, 1);
        cleanup_case(&pool, workflow_id, task_id).await;
    }

    #[tokio::test]
    #[serial]
    async fn quarantine_repoints_verified_evidence_and_consumer_drains_it() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        seed_workflow_task(&pool, workflow_id, task_id, false).await;
        terminalize_workflow_task(&pool, task_id).await;

        let mut transaction = pool.begin().await.unwrap();
        let quarantined = quarantine_one(&mut transaction, task_id).await.unwrap();
        assert_eq!(quarantined.verdict, QuarantineVerdict::Repointed);
        transaction.commit().await.unwrap();

        let disposition = finalize_phase2(
            &pool,
            task_id,
            "COMPLETED",
            &crate::core::WorkflowSpecRegistry::new(),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            disposition.disposition,
            Phase2DispositionKind::AppliedToNode
        );
        let evidence: (i64, i64) = sqlx::query_as(
            "SELECT
                (SELECT count(*) FROM horsies_workflow_phase2_pending WHERE task_id = $1),
                (SELECT count(*) FROM horsies_workflow_phase2_quarantine WHERE task_id = $1)",
        )
        .bind(task_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(evidence, (0, 0));
        cleanup_case(&pool, workflow_id, task_id).await;
    }
}
