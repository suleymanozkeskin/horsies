//! Port of Python `tests/integration/test_claim_in_flight_count.py` and
//! `tests/integration/test_monitoring_query_plans.py::test_claim_streams_plan_on_the_class_indexes`:
//! the plans of the statements inside `horsies_claim`, read from
//! `auto_explain` notices, and the dead index entries that the in-flight
//! count marks.
//!
//! Each test runs on a database of its own (`TestDatabase`): the tests
//! empty `horsies_tasks`, turn its autovacuum off, and sample it.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use serial_test::serial;

use crate::test_support::{connect_collecting_notices, TestDatabase};

/// The rows of the in-flight count tests.
const ROWS: i64 = 2_000;
/// The worker of the claims of these tests.
const WORKER_ID: &str = "in-flight-count-worker";
/// One claim of a worker with isolated children only, on `default`, with a
/// budget of 20 and every class routed to it (Python `CLAIM_PARAMS`).
/// Parameter: the worker id.
const CLAIM: &str = r#"SELECT row_kind FROM horsies_claim(
    $1, '["default"]'::jsonb, '{"default": 100}'::jsonb, '{}'::jsonb, TRUE,
    '{"ISOLATED": 20}'::jsonb,
    '{"ISOLATED": "ISOLATED", "SHARED_ASYNC": "ISOLATED", "SHARED_THREAD": "ISOLATED"}'::jsonb,
    0, 0, 0, NULL, 30000, '[]'::jsonb
)"#;
/// The settings that send the plans of the statements inside a function as
/// notices, for the rest of the transaction.
const AUTO_EXPLAIN_SETTINGS: &str = "LOAD 'auto_explain';
     SET LOCAL auto_explain.log_min_duration = 0;
     SET LOCAL auto_explain.log_nested_statements = on;
     SET LOCAL auto_explain.log_level = 'notice';";
const STATUS_INDEX_SCAN: &str = "Index Scan using horsies_tasks_status on horsies_tasks";
/// The baseline file of the claim functions.
const LIFECYCLE_FUNCTIONS_SQL: &str =
    include_str!("../../../baseline/0004_lifecycle_functions.sql");
/// The RUNNING rows, for a bitmap scan of the status index.
const RUNNING_ROWS: &str = "SELECT task_name FROM horsies_tasks WHERE status = 'RUNNING'";
/// The polls of the wait for older transactions: at most 60 s.
const OLDER_HOLDER_POLLS: u32 = 300;
const OLDER_HOLDER_POLL_INTERVAL: Duration = Duration::from_millis(200);
/// Each holder of an xid or a snapshot xmin at or before the xid `$1`, in
/// any database of the server. `age` compares the two xids: the older one
/// has the larger age.
const OLDER_HOLDERS: &str = "
    SELECT format(
        'backend pid %s (database %s, state %s, transaction start %s, query %L)',
        pid, datname, state, xact_start, left(query, 200)
    )
    FROM pg_stat_activity
    WHERE pid <> pg_backend_pid()
      AND (
          age(backend_xid) >= age($1::text::xid)
          OR age(backend_xmin) >= age($1::text::xid)
      )
    UNION ALL
    SELECT format('prepared transaction %L (database %s, prepared %s)', gid, database, prepared)
    FROM pg_prepared_xacts
    WHERE age(transaction) >= age($1::text::xid)
    UNION ALL
    SELECT format('replication slot %L (database %s)', slot_name, database)
    FROM pg_replication_slots
    WHERE age(xmin) >= age($1::text::xid)";

/// The in-flight row select of the baseline `horsies_claim_in_flight`.
fn in_flight_rows_sql() -> &'static str {
    let function = LIFECYCLE_FUNCTIONS_SQL
        .find("CREATE FUNCTION horsies_claim_in_flight(")
        .expect("the baseline defines horsies_claim_in_flight");
    let opening = "WITH in_flight AS (";
    let start = LIFECYCLE_FUNCTIONS_SQL[function..]
        .find(opening)
        .expect("the function counts its in-flight rows in one CTE")
        + function
        + opening.len();
    let end = LIFECYCLE_FUNCTIONS_SQL[start..]
        .find("\n    )\n")
        .expect("the CTE of the in-flight rows ends")
        + start;
    LIFECYCLE_FUNCTIONS_SQL[start..end].trim()
}

/// An empty task table with autovacuum off, so only the statements of the
/// test touch its dead index entries, and `ROWS` PENDING tasks on `default`.
async fn seed_pending(client: &tokio_postgres::Client) {
    client
        .batch_execute(&format!(
            "TRUNCATE horsies_task_attempts, horsies_tasks CASCADE;
             ALTER TABLE horsies_tasks SET (autovacuum_enabled = off);
             INSERT INTO horsies_tasks (
                 id, task_name, queue_name, priority, input, task_options, execution,
                 retention_class_key, sent_at, created_at,
                 command_fingerprint_version, command_fingerprint, idempotency_key_digest,
                 status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
                 started_at, finalizing_at, finalizing_by_worker_id,
                 worker_pid, worker_hostname, worker_process_name,
                 retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
                 updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
             )
             SELECT gen_random_uuid(), 'in_flight_task', 'default', 100, '{{}}', NULL,
                    'ISOLATED', 'standard_30d', NOW(), NOW(),
                    1, sha256(convert_to(gen_random_uuid()::text, 'UTF8')), NULL,
                    'PENDING', NOW(), NULL, NULL, NULL,
                    NULL, NULL, NULL,
                    NULL, NULL, NULL,
                    0, 0, NULL, NULL, FALSE,
                    NOW(), FALSE, NULL, NULL
             FROM generate_series(1, {ROWS})"
        ))
        .await
        .expect("seed the PENDING tasks");
}

/// Move `rows` PENDING tasks to RUNNING, held by [`WORKER_ID`].
async fn start(client: &tokio_postgres::Client, rows: i64) {
    client
        .execute(
            "UPDATE horsies_tasks
             SET status = 'RUNNING', claimed_at = NOW(),
                 claimed_by_worker_id = $1, started_at = NOW()
             WHERE id IN (
                 SELECT id FROM horsies_tasks WHERE status = 'PENDING' ORDER BY id LIMIT $2
             )",
            &[&WORKER_ID, &rows],
        )
        .await
        .expect("start the tasks");
}

/// The plans of the statements inside one claim, from `auto_explain`, with
/// `settings` set first. The claim is rolled back.
async fn nested_plans_of_a_claim(
    client: &tokio_postgres::Client,
    notices: &Arc<Mutex<Vec<String>>>,
    worker_id: &str,
    settings: &str,
) -> String {
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute(settings)
        .await
        .expect("send the nested plans as notices");
    let _claimed = client
        .query(CLAIM, &[&worker_id])
        .await
        .expect("the claim statement");
    client.batch_execute("ROLLBACK").await.expect("rollback");
    std::mem::take(&mut *notices.lock().unwrap_or_else(PoisonError::into_inner)).join("\n")
}

/// The entries that a bitmap scan of `horsies_tasks_status` returns for
/// RUNNING. A bitmap scan skips the entries that are marked dead and marks
/// none itself. Rolled back.
async fn bitmap_running_entries(client: &tokio_postgres::Client) -> f64 {
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute(
            "SET LOCAL enable_seqscan = off;
             SET LOCAL enable_indexscan = off;
             SET LOCAL enable_indexonlyscan = off;",
        )
        .await
        .expect("force a bitmap scan");
    let plan: serde_json::Value = client
        .query_one(
            &format!("EXPLAIN (ANALYZE, FORMAT JSON) {RUNNING_ROWS}"),
            &[],
        )
        .await
        .expect("explain the RUNNING rows")
        .get(0);
    client.batch_execute("ROLLBACK").await.expect("rollback");
    let mut nodes = vec![plan[0]["Plan"].clone()];
    while let Some(node) = nodes.pop() {
        if node["Node Type"] == "Bitmap Index Scan" {
            assert_eq!(node["Index Name"], "horsies_tasks_status", "{node}");
            return node["Actual Rows"]
                .as_f64()
                .unwrap_or_else(|| panic!("a row count in {node}"));
        }
        if let Some(children) = node["Plans"].as_array() {
            nodes.extend(children.iter().cloned());
        }
    }
    panic!("no bitmap index scan in {plan}");
}

/// Wait until no transaction on the server is as old as `delete_xid`.
/// Fails the test with the holders when they remain: then no scan can mark
/// the deleted entries.
async fn wait_for_older_transactions(client: &tokio_postgres::Client, delete_xid: &str) {
    let mut holders: Vec<String> = Vec::new();
    for _ in 0..OLDER_HOLDER_POLLS {
        holders = client
            .query(OLDER_HOLDERS, &[&delete_xid])
            .await
            .expect("read the older transactions")
            .iter()
            .map(|row| row.get(0))
            .collect();
        if holders.is_empty() {
            return;
        }
        tokio::time::sleep(OLDER_HOLDER_POLL_INTERVAL).await;
    }
    panic!(
        "transactions from before the delete (xid {delete_xid}) still run, so no scan can \
         mark the deleted entries: {}",
        holders.join("; ")
    );
}

/// Python `test_claim_in_flight_count.py::test_the_count_reads_the_status_index_when_most_rows_are_in_flight`:
/// with statistics that show most rows in flight, the in-flight rows of
/// the claim plan as a sequential scan under the default settings, and the
/// count inside the claim reads the status index (the function turns the
/// sequential and the bitmap scan off).
#[tokio::test]
#[serial]
async fn the_count_reads_the_status_index_when_most_rows_are_in_flight() {
    let database = TestDatabase::create().await;
    let (client, notices) = connect_collecting_notices(database.url()).await;
    seed_pending(&client).await;
    start(&client, ROWS * 9 / 10).await;
    client
        .batch_execute("ANALYZE horsies_tasks")
        .await
        .expect("sample the task table");
    let default_plan: String = client
        .query(&format!("EXPLAIN {}", in_flight_rows_sql()), &[])
        .await
        .expect("explain the in-flight rows")
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n");

    let plans = nested_plans_of_a_claim(&client, &notices, WORKER_ID, AUTO_EXPLAIN_SETTINGS).await;

    assert!(
        default_plan.contains("Seq Scan on horsies_tasks"),
        "{default_plan}"
    );
    let counts: Vec<&str> = plans
        .split("duration:")
        .filter(|plan| plan.contains("WITH in_flight AS"))
        .collect();
    assert_eq!(counts.len(), 1, "{plans}");
    assert!(counts[0].contains(STATUS_INDEX_SCAN), "{}", counts[0]);
    assert!(
        !counts[0].contains("Seq Scan on horsies_tasks"),
        "{}",
        counts[0]
    );
    assert!(
        !counts[0].contains("Bitmap Heap Scan on horsies_tasks"),
        "{}",
        counts[0]
    );
    drop(client);
    database.drop().await;
}

/// Python `test_claim_in_flight_count.py::test_a_claim_marks_the_dead_running_entries`:
/// every task ran and was deleted, so each RUNNING entry of the status
/// index is dead. Once no transaction from before the delete runs, one
/// claim (rolled back) marks all of them: a later bitmap scan returns none.
#[tokio::test]
#[serial]
async fn a_claim_marks_the_dead_running_entries() {
    let database = TestDatabase::create().await;
    let (client, notices) = connect_collecting_notices(database.url()).await;
    seed_pending(&client).await;
    start(&client, ROWS).await;
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("DELETE FROM horsies_tasks")
        .await
        .expect("delete every task");
    let delete_xid: String = client
        .query_one("SELECT pg_current_xact_id()::xid::text", &[])
        .await
        .expect("read the xid of the delete")
        .get(0);
    client.batch_execute("COMMIT").await.expect("commit");
    let before = bitmap_running_entries(&client).await;
    wait_for_older_transactions(&client, &delete_xid).await;

    let _plans = nested_plans_of_a_claim(&client, &notices, WORKER_ID, "SELECT 1").await;

    assert_eq!(before, 2_000.0);
    assert_eq!(bitmap_running_entries(&client).await, 0.0);
    drop(client);
    database.drop().await;
}

/// The live rows of the claim stream plan test (Python
/// `test_monitoring_query_plans.py::_seed_tasks`): 500 RUNNING rows over
/// five task names and a backlog of 2,000 PENDING rows, each with its own
/// enqueue time, then sampled.
async fn seed_plan_population(client: &tokio_postgres::Client) {
    for (status, task_name, started_at, rows) in [
        ("RUNNING", "'plan_task_' || (g % 5)", "NOW()", 500_i32),
        ("PENDING", "'plan_task_pending'", "NULL", 2_000_i32),
    ] {
        client
            .batch_execute(&format!(
                "INSERT INTO horsies_tasks (
                    id, task_name, queue_name, priority, input, task_options, execution,
                    retention_class_key, sent_at, created_at,
                    command_fingerprint_version, command_fingerprint, idempotency_key_digest,
                    status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
                    started_at, finalizing_at, finalizing_by_worker_id,
                    worker_pid, worker_hostname, worker_process_name,
                    retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
                    updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
                )
                SELECT gen_random_uuid(), {task_name}, 'default', 100, '{{}}', NULL,
                       'ISOLATED', 'standard_30d', NOW(), NOW(),
                       1, sha256(convert_to(gen_random_uuid()::text, 'UTF8')), NULL,
                       '{status}', NOW() - make_interval(secs => g), NULL, NULL, NULL,
                       {started_at}, NULL, NULL,
                       NULL, NULL, NULL,
                       0, 0, NULL, NULL, FALSE,
                       NOW(), FALSE, NULL, NULL
                FROM generate_series(1, {rows}) AS g"
            ))
            .await
            .unwrap_or_else(|error| panic!("seed the {status} rows of the plan test: {error}"));
    }
    client
        .batch_execute("ANALYZE horsies_tasks")
        .await
        .expect("sample the task table");
}

/// Python `test_monitoring_query_plans.py::test_claim_streams_plan_on_the_class_indexes`:
/// each claim stream of a worker with isolated children rides a class
/// index, in the plan phase (`horsies_claim_plan`, the head of each stream)
/// and in the take phase (`horsies_claim_candidates`, the lock of the
/// planned rows): the pending arm `horsies_tasks_claim_pending` once, the
/// expired arm one of the two expired indexes. The monitoring enqueue index
/// does not appear, and no stream scans the heap (sequential scans off, as
/// a production-sized table plans).
#[tokio::test]
#[serial]
async fn the_claim_streams_plan_on_the_class_indexes() {
    let database = TestDatabase::create().await;
    let (client, notices) = connect_collecting_notices(database.url()).await;
    seed_plan_population(&client).await;

    let plans = nested_plans_of_a_claim(
        &client,
        &notices,
        "plan-test-worker",
        &format!("SET LOCAL enable_seqscan = off; {AUTO_EXPLAIN_SETTINGS}"),
    )
    .await;

    let statements: Vec<&str> = plans.split("duration:").collect();
    let phase = |marker: &str| -> String {
        statements
            .iter()
            .find(|plan| plan.contains(marker))
            .unwrap_or_else(|| panic!("no plan contains {marker:?}: {plans}"))
            .to_string()
    };
    for (name, plan) in [
        ("plan", phase("SELECT s.queue_slot")),
        ("take", phase("WITH planned AS")),
    ] {
        assert_eq!(
            plan.matches("Index Scan using horsies_tasks_claim_pending ")
                .count(),
            1,
            "{name}: {plan}"
        );
        assert!(
            plan.contains("horsies_tasks_claim_expired_order")
                || plan.contains("horsies_tasks_claim_expired_at"),
            "{name}: {plan}"
        );
        for absent in ["Seq Scan on horsies_tasks", "idx_horsies_tasks_enqueued_at"] {
            assert!(!plan.contains(absent), "{name}: {absent}: {plan}");
        }
    }
    drop(client);
    database.drop().await;
}
