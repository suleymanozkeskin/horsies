//! The read of a phase-1 replay (`replay_of_committed_write`) on the
//! baseline phase-2 record, on the main core pool of the broker. The replay
//! reads only whether the record exists: the node finish takes the node
//! status from the record's kind. A replay of a committed workflow write
//! owes the phase 2 of its committed outcome; a plain task owes nothing.

use uuid::Uuid;

use super::{replay_of_committed_write, FinalizeError, FinalizeOutcome, PhaseTwoForm};
use crate::broker::PostgresBroker;
use crate::test_support::sql::{self, TestPool};

/// The ids of one seeded workflow node and its task.
struct SeededRecord {
    task_id: Uuid,
    workflow_id: Uuid,
}

/// A RUNNING workflow with one RUNNING task node for a new task id, and the
/// phase-2 record of that task with `kind`, as the terminal move writes it
/// (`horsies_move_task_to_history`). The task row itself has moved to
/// history, so no live row is seeded.
async fn seed_record(pool: &TestPool, kind: &str) -> SeededRecord {
    let task_id = Uuid::new_v4();
    let workflow_id = Uuid::new_v4();
    let node_row_id = Uuid::new_v4();
    sql::query(
        "INSERT INTO horsies_workflows (
             id, name, definition_key, status, on_error, depth, root_workflow_id,
             sent_at, created_at, started_at, updated_at
         ) VALUES ($1, 'replay_read_workflow', NULL, 'RUNNING', 'fail', 0, $1,
                   NOW(), NOW(), NOW(), NOW())",
    )
    .bind(workflow_id)
    .execute(pool)
    .await
    .expect("seed workflow");
    sql::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
             dependencies, allow_failed_deps, join_type, max_retries, execution,
             retention_class_key, status, task_id, is_subworkflow, created_at, started_at
         ) VALUES ('NONE', $1, $2, 0, 'only', 'replay_read_task', '{}', 'default', 100,
                   '{}', FALSE, 'all', 0, 'ISOLATED',
                   'forever', 'RUNNING', $3, FALSE, NOW(), NOW())",
    )
    .bind(node_row_id)
    .bind(workflow_id)
    .bind(task_id)
    .execute(pool)
    .await
    .expect("seed workflow node");
    sql::query(
        "INSERT INTO horsies_workflow_phase2_pending (
             task_id, workflow_id, workflow_node_row_id, terminalization_kind,
             recovery_source, history_class, history_anchor, history_schema_version,
             result_digest, quarantine_task_id, created_at, attempt_count,
             last_attempt_at, last_failure_class
         ) VALUES ($1, $2, $3, $4,
                   'HISTORY', 'forever', NOW(), 1,
                   decode(repeat('00', 32), 'hex'), NULL, NOW(), 0,
                   NULL, NULL)",
    )
    .bind(task_id)
    .bind(workflow_id)
    .bind(node_row_id)
    .bind(kind)
    .execute(pool)
    .await
    .expect("seed phase-2 record");
    SeededRecord {
        task_id,
        workflow_id,
    }
}

/// Delete the workflow; its node and the phase-2 record go with it (both
/// foreign keys cascade).
async fn cleanup(pool: &TestPool, seeded: &SeededRecord) {
    sql::query("DELETE FROM horsies_workflows WHERE id = $1")
        .bind(seeded.workflow_id)
        .execute(pool)
        .await
        .expect("delete the seeded workflow");
}

/// A name for each outcome, for assertion messages.
fn describe(outcome: &Result<FinalizeOutcome, FinalizeError>) -> String {
    match outcome {
        Ok(FinalizeOutcome::Terminal {
            capacity_notified,
            phase_two,
        }) => format!(
            "Terminal {{ capacity_notified: {capacity_notified}, phase_two: {phase_two:?} }}"
        ),
        Ok(FinalizeOutcome::Retried { .. }) => "Retried".to_owned(),
        Ok(FinalizeOutcome::Finalized) => "Finalized".to_owned(),
        Ok(FinalizeOutcome::Refused) => "Refused".to_owned(),
        Err(error) => format!("Err({error})"),
    }
}

/// A replay of a committed workflow success owes phase 2.
#[tokio::test]
async fn a_complete_locked_record_owes_phase_two() {
    let pool = crate::test_support::migrated_pool().await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let seeded = seed_record(&pool, "COMPLETE_LOCKED").await;

    let outcome = replay_of_committed_write(&broker, seeded.task_id, true).await;

    cleanup(&pool, &seeded).await;
    assert!(
        matches!(
            outcome,
            Ok(FinalizeOutcome::Terminal {
                capacity_notified: false,
                phase_two: PhaseTwoForm::NotKnown,
            })
        ),
        "{}",
        describe(&outcome)
    );
}

/// A replay of a committed failure or expiry owes phase 2.
#[tokio::test]
async fn failure_and_expiry_records_owe_phase_two() {
    let pool = crate::test_support::migrated_pool().await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    for kind in [
        "FAIL_RUNNING",
        "FAIL_STALE",
        "EXPIRE_CLAIMED",
        "EXPIRE_PENDING",
    ] {
        let seeded = seed_record(&pool, kind).await;

        let outcome = replay_of_committed_write(&broker, seeded.task_id, true).await;

        cleanup(&pool, &seeded).await;
        assert!(
            matches!(
                outcome,
                Ok(FinalizeOutcome::Terminal {
                    capacity_notified: false,
                    phase_two: PhaseTwoForm::NotKnown,
                })
            ),
            "{kind}: {}",
            describe(&outcome)
        );
    }
}

/// With no phase-2 record, phase 2 consumed it: the replay owes nothing.
#[tokio::test]
async fn a_consumed_record_owes_nothing() {
    let pool = crate::test_support::migrated_pool().await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);

    let outcome = replay_of_committed_write(&broker, Uuid::new_v4(), true).await;

    assert!(
        matches!(outcome, Ok(FinalizeOutcome::Finalized)),
        "{}",
        describe(&outcome)
    );
}

/// A plain task owes no phase 2, whatever the record table holds.
#[tokio::test]
async fn a_plain_task_replay_owes_nothing() {
    let pool = crate::test_support::migrated_pool().await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);

    let outcome = replay_of_committed_write(&broker, Uuid::new_v4(), false).await;

    assert!(
        matches!(outcome, Ok(FinalizeOutcome::Finalized)),
        "{}",
        describe(&outcome)
    );
}
