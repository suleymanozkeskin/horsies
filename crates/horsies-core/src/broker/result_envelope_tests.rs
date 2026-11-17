//! The typed result read of a stored result that is not a result envelope:
//! `get_result` refuses it and never decodes it.
//!
//! Python specification: `tests/e2e/test_layer1_tasks.py::test_legacy_pydantic_result_envelope_rejected`,
//! and the read of a stored `{"random": 1}` of
//! `tests/integration/test_worker_load_persisted_task_result.py::test_invalid_task_result_structure_returns_err`
//! (that file is replaced by `test_worker_load_persisted_terminal.py`: the
//! replay load no longer decodes the stored result, so the strict read is
//! the one left to test).

use std::time::Duration;

use chrono::{DateTime, Utc};
use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use crate::broker::terminalization::on_test_pool::terminalize;
use crate::broker::terminalization_matrix::migrated_pool;
use crate::broker::{BrokerError, PostgresBroker};
use crate::core::lifecycle::{OwnedClaim, TerminalizationCommand, TerminalizationOutcome};
use crate::core::task::result::TASK_RESULT_ENVELOPE_KEY;

const WORKER: &str = "result-envelope-worker";

/// The wait of each read (Python `timeout_ms=1000`). The task is terminal
/// before the read, so the first poll answers.
const READ_WAIT: Duration = Duration::from_secs(1);

/// A RUNNING plain row of `WORKER`. Every baseline column is named. Returns
/// the claim generation.
async fn seed_running(pool: &PgPool, task_id: Uuid) -> DateTime<Utc> {
    sqlx::query_scalar(
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
            $1, 'result_envelope_task', 'default', 100, '{}', NULL, 'ISOLATED',
            'forever', NOW(), NOW(),
            1, decode(repeat('00', 32), 'hex'), NULL,
            'RUNNING', NOW(), NOW(), $2, NULL,
            NOW(), NULL, NULL,
            4242, 'result-envelope-host', 'result-envelope-process',
            0, 0, NULL, NULL, FALSE,
            NOW(), FALSE, NULL, NULL
        )
        RETURNING claimed_at",
    )
    .bind(task_id)
    .bind(WORKER)
    .fetch_one(pool)
    .await
    .expect("seed a running row")
}

/// Complete the RUNNING row with `stored` as its result. The fused
/// completion copies the result text; it does not read its structure.
async fn complete_with(pool: &PgPool, task_id: Uuid, generation: DateTime<Utc>, stored: &str) {
    let outcomes = terminalize(
        pool,
        &TerminalizationCommand::CompleteTaskFused {
            task_id,
            fence: OwnedClaim {
                worker_id: WORKER.to_owned(),
                claimed_at: generation,
            },
            result_json: stored.to_owned(),
        },
    )
    .await
    .expect("complete the task");
    assert!(
        matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ),
        "{outcomes:?}"
    );
}

async fn remove_history(pool: &PgPool, task_id: Uuid) {
    sqlx::query("DELETE FROM horsies_task_history WHERE task_id = $1")
        .bind(task_id)
        .execute(pool)
        .await
        .expect("remove the history row");
}

/// Python `test_legacy_pydantic_result_envelope_rejected` (a stored result
/// in the legacy `__task_result__` shape) and the read of
/// `test_invalid_task_result_structure_returns_err` (`{"random": 1}`): a
/// COMPLETED task whose stored result has no `__h_task_result__` marker is
/// refused by the strict envelope read of `get_result`, not decoded. The
/// refusal is `BrokerError::Serialization`, which the core's read catalogs
/// report as `INVALID_JSON_PAYLOAD`, and is not retryable.
#[tokio::test]
#[serial]
async fn a_stored_result_that_is_not_an_envelope_is_refused_by_the_typed_read() {
    let pool = migrated_pool().await;
    let broker = PostgresBroker::from_pool(pool.clone());
    let legacy_envelope = serde_json::json!({
        "__task_result__": true,
        "ok": {
            "__pydantic_model__": true,
            "module": "nonexistent.module.that.does.not.exist",
            "qualname": "FakeModel",
            "data": {"field": "value"},
        },
        "err": null,
    })
    .to_string();

    for (case, stored) in [
        ("legacy_envelope", legacy_envelope.as_str()),
        ("no_envelope", r#"{"random": 1}"#),
    ] {
        let task_id = Uuid::new_v4();
        let generation = seed_running(&pool, task_id).await;
        complete_with(&pool, task_id, generation, stored).await;

        let refused = broker
            .get_result::<serde_json::Value>(task_id, Some(READ_WAIT))
            .await
            .expect_err(case);

        assert!(
            matches!(refused, BrokerError::Serialization(_)),
            "{case}: {refused:?}"
        );
        assert!(!refused.is_retryable(), "{case}");
        assert!(
            refused.to_string().contains(TASK_RESULT_ENVELOPE_KEY),
            "{case}: {refused}"
        );
        remove_history(&pool, task_id).await;
    }
}
