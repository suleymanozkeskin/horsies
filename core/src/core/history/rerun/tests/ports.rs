//! Port of the Python rerun refusal of values that a stored envelope may not
//! hold.
//!
//! Python specification: `clients/python/tests/integration/test_task_history_rerun.py`
//! (`TestTypedRefusals.test_refused_value_in_a_stored_envelope_fails_closed`).

use super::*;
use crate::test_support::sql;

/// A stored envelope whose input holds `Infinity`, a number outside the
/// binary64 range, or a lone UTF-16 surrogate is input corrupt, and the rerun
/// writes no task. The detail carries the text of Python's reader
/// (`clients/python/horsies/core/codec/json_value.py:156, 180, 294`).
#[tokio::test]
#[serial]
async fn a_refused_value_in_a_stored_envelope_fails_closed() {
    let pool = crate::test_support::migrated_pool().await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let (source, _) = seed_source(&pool, TerminalSource::Failed, false, None).await;
    for (input, python_text) in [
        (
            "{\"x\":Infinity}",
            "Non-standard JSON constant is not allowed: Infinity",
        ),
        (
            "{\"x\":1e9999}",
            "JSON number is outside the finite binary64 range",
        ),
        ("{\"x\":\"\\ud800\"}", "JSON string holds a lone surrogate"),
    ] {
        let payload = format!("{{\"input\":{input},\"options\":null}}").into_bytes();
        let digest = Sha256::digest(&payload).to_vec();
        sql::query(
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
            matches!(
                &outcome,
                RerunOutcome::InputCorrupt { task_id, detail }
                    if *task_id == source && detail.contains(python_text)
            ),
            "{input}: {outcome:?}"
        );
    }
    let reruns: i64 =
        sql::query_scalar("SELECT count(*) FROM horsies_tasks WHERE rerun_of_task_id = $1")
            .bind(source)
            .fetch_one(&pool)
            .await
            .expect("count reruns");
    assert_eq!(reruns, 0);
    cleanup(&pool, &[source], &[]).await;
}
