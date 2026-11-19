//! Port of Python `clients/python/tests/integration/test_task_history_terminalization_failure.py`.
//!
//! The Python file and the ports drive `horsies_finish_failed_attempt` (kind
//! `FAIL_RUNNING`) and `horsies_finish_stale_attempt` (kind `FAIL_STALE`).
//! `TestStalenessGuard::test_absent_task_classifies_through_the_null_claim_path`
//! is `terminalization_functions::fail_stale::a_task_that_does_not_exist`.

use serial_test::serial;
use uuid::Uuid;

use super::support::*;
use crate::broker::attempt_writes::{
    AttemptFailure, AttemptFailureKind, FinishFailedAttempt, RetryRequest,
};
use crate::core::history::rerun::input_envelope::decode_framed_rerun_input;
use crate::core::lifecycle::FailedAttemptOutcome;
use crate::test_support::sql::TestPool;

const FAILURE_RESULT: &str = r#"{"error":{"code":"BOOM"}}"#;

fn failure(task_id: Uuid) -> FinishFailedAttempt {
    FinishFailedAttempt {
        task_id,
        fence: owned_claim(),
        retry: RetryRequest::NoRetry,
        result_json: FAILURE_RESULT.to_owned(),
        error_code: Some("BOOM".to_owned()),
        failed_reason: None,
        attempt: AttemptFailure {
            kind: AttemptFailureKind::Failed,
            error_message: Some("boom".to_owned()),
            failed_reason: None,
        },
    }
}

async fn fail(pool: &TestPool, task_id: Uuid) {
    let outcome = write_failed_attempt(pool, &failure(task_id))
        .await
        .expect("failure write");
    assert!(
        matches!(outcome, FailedAttemptOutcome::FailureApplied { .. }),
        "{outcome:?}"
    );
}

mod disposition_ladder {
    use super::*;

    /// Python `TestDispositionLadder::test_declined_snapshot_archives_declined_by_policy`.
    #[tokio::test]
    #[serial]
    async fn declined_snapshot_archives_declined_by_policy() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;

        fail(&pool, task_id).await;

        assert_eq!(
            history_row(&pool, task_id)
                .await
                .expect("history row")
                .status,
            "FAILED"
        );
        let rerun = rerun_input_of(&pool, task_id).await;
        assert_eq!(rerun.rerun_input_disposition, "DECLINED_BY_POLICY");
        assert_eq!(rerun.rerun_input_inline, None);
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestDispositionLadder::test_retained_input_is_framed_byte_identically`
    /// (the test map names it `test_retained_prepared_envelope_carries_byte_identically`):
    /// the move frames the stored input byte for byte, with its digest.
    #[tokio::test]
    #[serial]
    async fn retained_input_is_framed_byte_identically() {
        let pool = pool().await;
        let input = r#"{"precious":"failure input"}"#;
        let task_id = seed_task(
            &pool,
            &TaskSeed {
                retain_rerun_input: true,
                input,
                ..TaskSeed::running()
            },
        )
        .await;

        fail(&pool, task_id).await;

        let rerun = rerun_input_of(&pool, task_id).await;
        let framed = format!(r#"{{"input":{input},"options":null}}"#).into_bytes();
        assert_eq!(rerun.rerun_input_disposition, "INLINE");
        assert_eq!(rerun.rerun_input_version, Some(1));
        assert_eq!(rerun.rerun_input_inline.as_deref(), Some(framed.as_slice()));
        assert_eq!(rerun.rerun_input_digest, Some(sha256_of(&framed)));
        let decoded = decode_framed_rerun_input(1, &framed, &sha256_of(&framed))
            .expect("the core decodes the framed input");
        assert_eq!(decoded.input, input);
        assert_eq!(decoded.options, None);
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestDispositionLadder::test_workflow_backing_failure_is_never_eligible_with_pending`.
    #[tokio::test]
    #[serial]
    async fn workflow_backing_failure_is_never_eligible_with_pending() {
        let pool = pool().await;
        let task_id = seed_task(
            &pool,
            &TaskSeed {
                origin: Origin::WorkflowNode,
                retain_rerun_input: true,
                ..TaskSeed::running()
            },
        )
        .await;
        let workflow_id = Uuid::new_v4();
        seed_workflow(&pool, workflow_id, "RUNNING").await;
        let node_row_id = seed_node(&pool, workflow_id, Some(task_id), "PENDING").await;

        fail(&pool, task_id).await;

        let rerun = rerun_input_of(&pool, task_id).await;
        assert_eq!(rerun.rerun_input_disposition, "NEVER_ELIGIBLE");
        assert_eq!(rerun.rerun_input_inline, None);
        let record = phase2_record_of(&pool, task_id).await;
        assert_eq!(record.workflow_id, workflow_id);
        assert_eq!(record.workflow_node_row_id, node_row_id);
        assert_eq!(record.recovery_source, "HISTORY");
        assert_eq!(record.history_class.as_deref(), Some("forever"));
        assert_eq!(record.terminalization_kind, "FAIL_RUNNING");
        assert_eq!(record.result_digest, sha256_of(FAILURE_RESULT.as_bytes()));
        assert_eq!(
            record.history_anchor,
            Some(terminal_at_of(&pool, task_id).await)
        );
        cleanup_tasks(&pool, &[task_id]).await;
        cleanup_workflows(&pool, &[workflow_id]).await;
    }
}

mod deferred_result_fence {
    use super::*;

    /// Python `TestDeferredResultFence::test_workflow_failure_without_result_raises_typed`:
    /// the phase-2 record needs the digest of a result, so the move refuses a
    /// deferred failure with no result, for both failure kinds.
    #[tokio::test]
    #[serial]
    async fn workflow_failure_without_result_raises_typed() {
        let pool = pool().await;
        for kind in ["FAIL_RUNNING", "FAIL_STALE"] {
            let task_id = seed_task(
                &pool,
                &TaskSeed {
                    origin: Origin::WorkflowNode,
                    ..TaskSeed::running()
                },
            )
            .await;
            let workflow_id = link_workflow_task(&pool, task_id, "PENDING", "RUNNING").await;

            let refused = move_to_history(&pool, task_id, "FAILED", kind, None).await;

            assert_refused_with_message(refused, "23502", "requires a result payload");
            assert!(live_row(&pool, task_id).await.is_some(), "{kind}");
            cleanup_tasks(&pool, &[task_id]).await;
            cleanup_workflows(&pool, &[workflow_id]).await;
        }
    }
}
