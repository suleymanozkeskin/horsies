//! Port of the Python rerun refusal of values that a stored envelope may not
//! hold.
//!
//! Python specification: `tests/integration/test_task_history_rerun.py`
//! (`TestTypedRefusals.test_refused_value_in_a_stored_envelope_fails_closed`).

use super::*;

/// A stored envelope whose input holds `Infinity`, a number outside the
/// binary64 range, or a lone UTF-16 surrogate is input corrupt, and the rerun
/// writes no task.
#[tokio::test]
#[serial]
async fn a_refused_value_in_a_stored_envelope_fails_closed() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let broker = PostgresBroker::from_pool(pool.clone());
    let (source, _) = seed_source(&pool, TerminalSource::Failed, false, None).await;
    for input in ["{\"x\":Infinity}", "{\"x\":1e9999}", "{\"x\":\"\\ud800\"}"] {
        let payload = format!("{{\"input\":{input},\"options\":null}}").into_bytes();
        let digest = Sha256::digest(&payload).to_vec();
        sqlx::query(
            "UPDATE horsies_task_history
             SET rerun_input_disposition = 'INLINE', rerun_input_version = 1,
                 rerun_input_digest = $2, rerun_input_inline = $3
             WHERE task_id = $1",
        )
        .bind(source)
        .bind(digest)
        .bind(payload)
        .execute(&pool)
        .await
        .expect("store the refused value");
        let outcome = broker
            .rerun_task(
                RerunTask::new(source, None, None),
                policy(SOURCE_CLASS, true),
            )
            .await
            .expect("rerun outcome");
        assert!(
            matches!(outcome, RerunOutcome::InputCorrupt { task_id, .. } if task_id == source),
            "{input}: {outcome:?}"
        );
    }
    let reruns: i64 =
        sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE rerun_of_task_id = $1")
            .bind(source)
            .fetch_one(&pool)
            .await
            .expect("count reruns");
    assert_eq!(reruns, 0);
    cleanup(&pool, &[source], &[]).await;
}
