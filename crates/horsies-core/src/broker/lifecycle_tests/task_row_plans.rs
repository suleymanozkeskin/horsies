//! Port of Python `tests/integration/test_task_row_plans.py`.
//!
//! `test_the_finalizing_marker_reads_its_row_through_the_primary_key` is
//! `worker::finalizing_tests::the_finalizing_marker_reads_its_row_through_the_primary_key`
//! (#588).

use serial_test::serial;

use super::support::*;
use crate::broker::postgres::SET_RUNNING_SQL;

/// Every index that a plan reads on `relation`.
fn indexes_read_on(plan: &serde_json::Value, relation: &str, found: &mut Vec<String>) {
    match plan {
        serde_json::Value::Array(values) => {
            for value in values {
                indexes_read_on(value, relation, found);
            }
        }
        serde_json::Value::Object(fields) => {
            let on_relation = fields
                .get("Relation Name")
                .and_then(serde_json::Value::as_str)
                == Some(relation);
            let index = fields.get("Index Name").and_then(serde_json::Value::as_str);
            if let (true, Some(index)) = (on_relation, index) {
                found.push(index.to_owned());
            }
            for value in fields.values() {
                indexes_read_on(value, relation, found);
            }
        }
        _ => {}
    }
}

/// Python `test_the_start_reads_its_row_through_the_primary_key`: the start
/// reads its task row through the primary key, whatever the statistics
/// make other indexes look like.
#[tokio::test]
#[serial]
async fn the_start_reads_its_row_through_the_primary_key() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &TaskSeed::claimed()).await;
    let statement = SET_RUNNING_SQL
        .replace("$1", &format!("'{task_id}'::uuid"))
        .replace("$2", "4242")
        .replace("$3", "'lifecycle-host'")
        .replace("$4", "'lifecycle-process'")
        .replace("$5", &format!("'{WORKER}'"))
        .replace(
            "$6",
            &format!("'{}'::timestamptz", generation().to_rfc3339()),
        );

    let mut transaction = pool.begin().await.expect("begin");
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *transaction)
        .await
        .expect("turn sequential scans off");
    let plan: serde_json::Value = sqlx::query_scalar(&format!("EXPLAIN (FORMAT JSON) {statement}"))
        .fetch_one(&mut *transaction)
        .await
        .expect("explain the start");
    transaction.rollback().await.expect("roll back");

    let mut indexes = Vec::new();
    indexes_read_on(&plan, "horsies_tasks", &mut indexes);
    assert!(!indexes.is_empty(), "the start read no index: {plan}");
    assert!(
        indexes.iter().all(|index| index == "horsies_tasks_pkey"),
        "the start must read its row through the primary key alone: {indexes:?}"
    );
    cleanup_tasks(&pool, &[task_id]).await;
}
