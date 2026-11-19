//! Port of Python `clients/python/tests/integration/test_task_row_plans.py`.
//!
//! `test_the_finalizing_marker_reads_its_row_through_the_primary_key` is
//! `worker::finalizing_tests::the_finalizing_marker_reads_its_row_through_the_primary_key`
//! (#588).
//!
//! The start is one `horsies_start_attempt` call (`broker::claims::start_attempt`),
//! as in Python. Its row read is planned here from the installed function
//! body.

use crate::test_support::sql;
use serial_test::serial;

use super::support::*;

/// The installed start, as `regprocedure` text.
const START_FUNCTION: &str =
    "horsies_start_attempt(uuid,text,timestamp with time zone,integer,text,text,text)";
/// First and last words of the start's row read in its body.
const ROW_READ_START: &str = "SELECT t.status";
const ROW_READ_END: &str = "FOR NO KEY UPDATE";
/// The plpgsql target of the row read, which plain SQL does not take.
const ROW_READ_TARGET: &str = "INTO v_task";

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

/// The row read of the installed start: the statement from
/// [`ROW_READ_START`] to [`ROW_READ_END`], without its plpgsql target, with
/// the task id `task_id` in place of the argument.
fn start_row_read(definition: &str, task_id: uuid::Uuid) -> String {
    let start = definition
        .find(ROW_READ_START)
        .expect("the start reads its task row");
    let length = definition[start..]
        .find(ROW_READ_END)
        .expect("the start locks its task row")
        + ROW_READ_END.len();
    let statement = &definition[start..start + length];
    assert!(
        statement.contains(ROW_READ_TARGET),
        "the row read of the start has no {ROW_READ_TARGET:?}: {statement}"
    );
    statement
        .replace(ROW_READ_TARGET, "")
        .replace("p_task_id", &format!("'{task_id}'::uuid"))
}

/// Python `test_the_start_reads_its_row_through_the_primary_key`, on the
/// start of the baseline: `horsies_start_attempt` reads its task row
/// through the primary key, whatever the statistics make other indexes look
/// like.
#[tokio::test]
#[serial]
async fn the_start_reads_its_row_through_the_primary_key() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &TaskSeed::claimed()).await;
    let definition: String = sql::query_scalar("SELECT pg_get_functiondef($1::regprocedure)")
        .bind(START_FUNCTION)
        .fetch_one(&pool)
        .await
        .expect("read the start definition");
    let statement = start_row_read(&definition, task_id);

    let mut transaction = pool.begin().await.expect("begin");
    sql::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *transaction)
        .await
        .expect("turn sequential scans off");
    let plan: serde_json::Value = sql::query_scalar(&format!("EXPLAIN (FORMAT JSON) {statement}"))
        .fetch_one(&mut *transaction)
        .await
        .expect("explain the start row read");
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
