//! Port of Python `clients/python/tests/integration/test_workflow_success_finish.py`
//! (`horsies_finish_workflow_success`).
//!
//! The success goes through the worker's client (`finish_workflow_success`,
//! called by `support::write_workflow_success`), one statement on a core pool
//! on the test database, as phase 1 calls it. The refused calls run the
//! function directly with the row's own claim generation, as the Python test
//! does, so the fence holds and the function's own refusal answers.
//!
//! Rows that a core test already checks in full are not repeated (test map:
//! COVERED): the NULL generation, the replay, the lost claim, the CLAIMED
//! row, the foreign terminalization and the unknown task are in
//! `terminalization_matrix`, `attempt_writes::tests`, `worker::worker` and
//! `worker::finalizing_tests`.

use uuid::Uuid;

use super::support::*;
use crate::broker::attempt_writes::FinishWorkflowSuccess;
use crate::core::lifecycle::{PhaseTwoOwed, PhaseTwoStep, WorkflowSuccessOutcome};
use crate::core::types::status::TaskStatus;
use crate::test_support::sql::{self, TestPool};

const QUEUE: &str = "lifecycle_workflow_success";
const CHANNEL: &str = "task_queue_lifecycle_workflow_success";
const OK_RESULT: &str = r#"{"__h_task_result__":true,"ok":7,"err":null}"#;

/// The SQL of a direct call with the row's own claim generation (Python's
/// `_DIRECT_SQL`).
const DIRECT_SQL: &str = "SELECT * FROM horsies_finish_workflow_success(
    $1, $2, (SELECT claimed_at FROM horsies_tasks WHERE id = $1), $3, 'NOT_OBSERVED', NULL
)";

/// A RUNNING row of `WORKER` with `retry_count`, `max_retries = 3`, in
/// [`QUEUE`]. A workflow task gets a RUNNING node of a RUNNING workflow.
/// Returns the task and, for a workflow task, its workflow.
async fn seed(pool: &TestPool, retry_count: i32, origin: Origin) -> (Uuid, Option<Uuid>) {
    let task_id = seed_task(
        pool,
        &TaskSeed {
            origin,
            queue_name: QUEUE,
            retry_count,
            max_retries: 3,
            ..TaskSeed::running()
        },
    )
    .await;
    let workflow_id = match origin {
        Origin::WorkflowNode => Some(link_workflow_task(pool, task_id, "RUNNING", "RUNNING").await),
        Origin::Send => None,
    };
    (task_id, workflow_id)
}

fn success(task_id: Uuid) -> FinishWorkflowSuccess {
    FinishWorkflowSuccess {
        task_id,
        fence: owned_claim(),
        result_json: OK_RESULT.to_owned(),
        placement: crate::core::lifecycle::AttemptPlacement::NotObserved,
    }
}

async fn cleanup(pool: &TestPool, task_id: Uuid, workflow_id: Option<Uuid>) {
    cleanup_tasks(pool, &[task_id]).await;
    let workflows: Vec<Uuid> = workflow_id.into_iter().collect();
    cleanup_workflows(pool, &workflows).await;
}

/// Python `TestApplied::test_the_success_moves_the_row_with_its_attempt_and_owes_phase_two`:
/// one statement moves the row with its attempt and writes the phase-2
/// record; it sends no capacity wake (phase 2 sends it).
#[tokio::test]
async fn the_success_moves_the_row_with_its_attempt_and_owes_phase_two() {
    let pool = pool().await;
    let mut wakes = Wakes::listen(&pool, CHANNEL).await;
    let (task_id, workflow_id) = seed(&pool, 0, Origin::WorkflowNode).await;

    let outcome = write_workflow_success(&pool, &success(task_id))
        .await
        .expect("success write");

    let WorkflowSuccessOutcome::SuccessApplied {
        task_id: applied,
        observed,
        phase_two:
            PhaseTwoOwed {
                task_name,
                queue_name,
                step,
            },
        ..
    } = outcome
    else {
        panic!("expected the success to apply, got {outcome:?}");
    };
    assert_eq!(applied, task_id);
    assert_eq!(
        (task_name.as_str(), queue_name.as_str()),
        ("lifecycle.test", QUEUE)
    );
    assert_eq!(step, PhaseTwoStep::NoDependents);
    assert_eq!(observed.status, Some(TaskStatus::Running));
    assert_eq!(observed.worker_id.as_deref(), Some(WORKER));
    assert_eq!(observed.claimed_at, Some(generation()));
    assert_eq!(live_row(&pool, task_id).await, None);
    let image = history_row(&pool, task_id).await.expect("history row");
    assert_eq!(
        (
            image.status.as_str(),
            image.terminalization_kind.as_str(),
            image.error_code.as_deref(),
            image.result_payload.as_deref(),
        ),
        (
            "COMPLETED",
            "COMPLETE_LOCKED",
            None,
            Some(OK_RESULT.as_bytes())
        )
    );
    let records = archived_attempt_records(&pool, task_id).await;
    let [attempt] = records.as_slice() else {
        panic!("expected one attempt, got {records:?}");
    };
    assert_eq!(
        (
            attempt.attempt(),
            attempt.outcome(),
            attempt.will_retry(),
            attempt.error_code(),
            attempt.error_message(),
            attempt.failed_reason(),
        ),
        (1, "COMPLETED", false, None, None, None)
    );
    assert_eq!(
        (
            attempt.worker_id(),
            attempt.worker_hostname(),
            attempt.worker_pid(),
            attempt.worker_process_name(),
        ),
        (
            Some(WORKER),
            Some("lifecycle-host"),
            Some(4242),
            Some("lifecycle-process")
        )
    );
    assert!(attempt.started_at() <= attempt.finished_at(), "{attempt:?}");
    assert_eq!(
        phase2_record_of(&pool, task_id).await.terminalization_kind,
        "COMPLETE_LOCKED"
    );
    assert!(wakes.payloads(&pool).await.is_empty());
    cleanup(&pool, task_id, workflow_id).await;
}

/// Python `TestApplied::test_the_attempt_number_follows_the_retry_count`: two
/// failed attempts were retried; the success is the third.
#[tokio::test]
async fn the_attempt_number_follows_the_retry_count() {
    let pool = pool().await;
    let (task_id, workflow_id) = seed(&pool, 2, Origin::WorkflowNode).await;
    sql::query(
        "INSERT INTO horsies_task_attempts (
             task_id, attempt, outcome, will_retry, started_at, finished_at,
             error_code, error_message, failed_reason, worker_id,
             worker_hostname, worker_pid, worker_process_name
         )
         SELECT $1, n, 'FAILED', TRUE, NOW() - interval '1 minute', NOW(),
                'FLAKY', NULL, NULL, $2, 'lifecycle-host', 4242, 'lifecycle-process'
         FROM generate_series(1, 2) AS n",
    )
    .bind(task_id)
    .bind(WORKER)
    .execute(&pool)
    .await
    .expect("record two failed attempts");

    let outcome = write_workflow_success(&pool, &success(task_id))
        .await
        .expect("success write");

    assert!(
        matches!(outcome, WorkflowSuccessOutcome::SuccessApplied { .. }),
        "{outcome:?}"
    );
    let attempts: Vec<(i32, String, bool)> = archived_attempt_records(&pool, task_id)
        .await
        .iter()
        .map(|record| {
            (
                record.attempt(),
                record.outcome().to_owned(),
                record.will_retry(),
            )
        })
        .collect();
    assert_eq!(
        attempts,
        vec![
            (1, "FAILED".to_owned(), true),
            (2, "FAILED".to_owned(), true),
            (3, "COMPLETED".to_owned(), false),
        ]
    );
    cleanup(&pool, task_id, workflow_id).await;
}

/// A call that the function refuses (Python `TestRefusedCall`'s parameters).
#[derive(Debug, Clone, Copy)]
enum RefusedCall {
    /// A task that is not a workflow task, with a result.
    PlainTask,
    /// A workflow task with no result payload.
    NoResult,
}

/// Python `TestRefusedCall::test_it_raises_and_writes_nothing` (`plain_task`,
/// `no_result`): the function's own refusal, by its SQLSTATE and its
/// message, and no write.
#[tokio::test]
async fn a_refused_call_raises_and_writes_nothing() {
    let pool = pool().await;
    for call in [RefusedCall::PlainTask, RefusedCall::NoResult] {
        let (origin, result, sqlstate, message) = match call {
            RefusedCall::PlainTask => (
                Origin::Send,
                Some(OK_RESULT),
                "22023",
                "is not a workflow task",
            ),
            RefusedCall::NoResult => (
                Origin::WorkflowNode,
                None,
                "23502",
                "a workflow task success needs a result payload",
            ),
        };
        let (task_id, workflow_id) = seed(&pool, 0, origin).await;
        let before = live_row(&pool, task_id).await;

        let refused = sql::query(DIRECT_SQL)
            .bind(task_id)
            .bind(WORKER)
            .bind(result)
            .fetch_all(&pool)
            .await;

        match refused {
            Err(sql::Error::Database(error)) => {
                assert_eq!(Some(error.code().code()), Some(sqlstate), "{call:?}");
                assert!(
                    error.message().contains(message),
                    "{call:?}: {}",
                    error.message()
                );
            }
            Err(other) => panic!("{call:?}: expected a database error, got {other}"),
            Ok(rows) => panic!("{call:?}: the function answered {} rows", rows.len()),
        }
        assert_eq!(live_row(&pool, task_id).await, before, "{call:?}");
        assert!(live_attempts(&pool, task_id).await.is_empty(), "{call:?}");
        assert_eq!(history_row(&pool, task_id).await, None, "{call:?}");
        cleanup(&pool, task_id, workflow_id).await;
    }
}
