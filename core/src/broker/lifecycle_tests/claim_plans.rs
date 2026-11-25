//! The plans of the statements inside `horsies_claim`, read from
//! `auto_explain` notices, the dead index entries that the in-flight count
//! marks, and the counts of `horsies_claim_in_flight`.
//!
//! Each test runs on a database of its own (`TestDatabase`): the tests
//! empty `horsies_tasks`, turn its autovacuum off, and sample it.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

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
/// The status index of the task table.
const STATUS_INDEX: &str = "horsies_tasks_status";
/// The RUNNING rows, for a bitmap scan of the status index.
const RUNNING_ROWS: &str = "SELECT task_name FROM horsies_tasks WHERE status = 'RUNNING'";
/// A table of one row that the delete transaction of the dead-entry test
/// also deletes. An index scan marks the dead entry of this row only when
/// that transaction is removable for the session of the scan: the same
/// condition as for the dead RUNNING entries.
const PROBE_SETUP: &str = "CREATE TABLE dead_entry_probe (id integer PRIMARY KEY)
         WITH (autovacuum_enabled = off);
     INSERT INTO dead_entry_probe VALUES (1);";
/// The index of the probe row.
const PROBE_INDEX: &str = "dead_entry_probe_pkey";
/// The probe row, for a bitmap scan of [`PROBE_INDEX`].
const PROBE_ROWS: &str = "SELECT id FROM dead_entry_probe WHERE id = 1";
/// An index scan of the probe row (the sequential, bitmap and index-only
/// scans off), rolled back.
const PROBE_INDEX_SCAN: &str = "BEGIN;
     SET LOCAL enable_seqscan = off;
     SET LOCAL enable_bitmapscan = off;
     SET LOCAL enable_indexonlyscan = off;
     SELECT id FROM dead_entry_probe WHERE id = 1;
     ROLLBACK;";
/// The attempts of the wait until the delete is removable: at most 60 s
/// plus the time of the attempts.
const REMOVABLE_ATTEMPTS: u32 = 300;
const REMOVABLE_ATTEMPT_INTERVAL: Duration = Duration::from_millis(200);
/// Each holder of an xid or a snapshot xmin at or before the xid `$1`, in
/// any database of the server, that `pg_stat_activity`,
/// `pg_prepared_xacts` and `pg_replication_slots` show: the report of a
/// wait that ends with the delete not removable. A backend that is still
/// starting up does not show, and on PostgreSQL 14 its xmin counts for the
/// horizon of every database until the backend has a database
/// (`ComputeXidHorizons` in `procarray.c`). `age` compares the two xids:
/// the older one has the larger age.
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
             SET status = 'RUNNING', attempt_claims = 1, first_claimed_at = NOW(), claimed_at = NOW(),
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

/// One claim of [`WORKER_ID`], rolled back.
async fn claim_and_roll_back(client: &tokio_postgres::Client) {
    client.batch_execute("BEGIN").await.expect("begin");
    let _claimed = client
        .query(CLAIM, &[&WORKER_ID])
        .await
        .expect("the claim statement");
    client.batch_execute("ROLLBACK").await.expect("rollback");
}

/// The entries of `index` that a bitmap scan of the select `rows` returns.
/// A bitmap scan skips the entries that are marked dead and marks none
/// itself. Rolled back.
async fn bitmap_index_entries(client: &tokio_postgres::Client, rows: &str, index: &str) -> f64 {
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
        .query_one(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {rows}"), &[])
        .await
        .unwrap_or_else(|error| panic!("explain {rows}: {error}"))
        .get(0);
    client.batch_execute("ROLLBACK").await.expect("rollback");
    let mut nodes = vec![plan[0]["Plan"].clone()];
    while let Some(node) = nodes.pop() {
        if node["Node Type"] == "Bitmap Index Scan" {
            assert_eq!(node["Index Name"], index, "{node}");
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

/// A new session on `database` in which the delete transaction (xid
/// `delete_xid`) is removable: in that session, an index scan marks the
/// dead entry of the probe row, which the same transaction deleted.
///
/// A session keeps the removal horizon that it computed last, and computes
/// it again only when the xmin of its newest snapshot differs from the xmin
/// at that computation (`GlobalVisTestShouldUpdate` in `procarray.c`). An
/// older snapshot that ends with no xid does not change that xmin. So each
/// attempt opens a new session, which computes the horizon at the attempt.
///
/// Fails the test after [`REMOVABLE_ATTEMPTS`] attempts, with the
/// transactions from before the delete that the server shows.
async fn session_where_the_delete_is_removable(
    database: &TestDatabase,
    delete_xid: &str,
) -> tokio_postgres::Client {
    for _ in 0..REMOVABLE_ATTEMPTS {
        let session = database.connect().await;
        session
            .batch_execute(PROBE_INDEX_SCAN)
            .await
            .expect("scan the probe row through its index");
        if bitmap_index_entries(&session, PROBE_ROWS, PROBE_INDEX).await == 0.0 {
            return session;
        }
        drop(session);
        tokio::time::sleep(REMOVABLE_ATTEMPT_INTERVAL).await;
    }
    let holders: Vec<String> = database
        .client()
        .query(OLDER_HOLDERS, &[&delete_xid])
        .await
        .expect("read the older transactions")
        .iter()
        .map(|row| row.get(0))
        .collect();
    panic!(
        "the delete (xid {delete_xid}) is not removable after {REMOVABLE_ATTEMPTS} attempts, \
         so no scan can mark the deleted entries; older transactions that the server shows: [{}]",
        holders.join("; ")
    );
}

/// With statistics that show most rows in flight, the in-flight rows of
/// the claim plan as a sequential scan under the default settings, and the
/// count inside the claim reads the status index (the function turns the
/// sequential and the bitmap scan off).
#[tokio::test]
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

/// Every task ran and was deleted, so each RUNNING entry of the status
/// index is dead. Once the delete is removable for a session, one claim
/// (rolled back) in that session marks all of them: a later bitmap scan
/// returns none.
#[tokio::test]
async fn a_claim_marks_the_dead_running_entries() {
    let database = TestDatabase::create().await;
    let client = database.client();
    seed_pending(client).await;
    start(client, ROWS).await;
    client
        .batch_execute(PROBE_SETUP)
        .await
        .expect("create the probe row");
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("DELETE FROM horsies_tasks; DELETE FROM dead_entry_probe;")
        .await
        .expect("delete every task and the probe row");
    let delete_xid: String = client
        .query_one("SELECT pg_current_xact_id()::xid::text", &[])
        .await
        .expect("read the xid of the delete")
        .get(0);
    client.batch_execute("COMMIT").await.expect("commit");
    let before = bitmap_index_entries(client, RUNNING_ROWS, STATUS_INDEX).await;
    let session = session_where_the_delete_is_removable(&database, &delete_xid).await;

    claim_and_roll_back(&session).await;

    assert_eq!(before, 2_000.0);
    assert_eq!(
        bitmap_index_entries(client, RUNNING_ROWS, STATUS_INDEX).await,
        0.0
    );
    drop(session);
    database.drop().await;
}

/// The live rows of the claim stream plan test: 500 RUNNING rows over
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

/// Each claim stream of a worker with isolated children rides a class
/// index, in the plan phase (`horsies_claim_plan`, the head of each stream)
/// and in the take phase (`horsies_claim_candidates`, the lock of the
/// planned rows): the pending arm `horsies_tasks_claim_pending` once, the
/// expired arm one of the two expired indexes. The monitoring enqueue index
/// does not appear, and no stream scans the heap (sequential scans off, as
/// a production-sized table plans).
#[tokio::test]
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

/// The claim lease of a seeded row.
#[derive(Debug, Clone, Copy)]
enum Lease {
    /// The lease runs five more minutes.
    Live,
    /// The lease ended a minute ago.
    Expired,
    /// No lease: a RUNNING or PENDING row.
    Absent,
}

/// The values of one task row of `status` on `queue`, held by `worker`
/// when it is held, for the column list of [`seed_plan_population`].
fn row_values(status: &str, queue: &str, worker: Option<&str>, lease: Lease) -> String {
    let (claimed_at, holder) = match worker {
        Some(worker) => ("NOW()", format!("'{worker}'")),
        None => ("NULL", "NULL".to_owned()),
    };
    let expires = match lease {
        Lease::Live => "NOW() + interval '5 minutes'",
        Lease::Expired => "NOW() - interval '1 minute'",
        Lease::Absent => "NULL",
    };
    let started_at = match status {
        "RUNNING" => "NOW()",
        _ => "NULL",
    };
    format!(
        "(gen_random_uuid(), 'in_flight_task', '{queue}', 100, '{{}}', NULL,
          'ISOLATED', 'standard_30d', NOW(), NOW(),
          1, sha256(convert_to(gen_random_uuid()::text, 'UTF8')), NULL,
          '{status}', NOW(), {claimed_at}, {holder}, {expires},
          {started_at}, NULL, NULL,
          NULL, NULL, NULL,
          0, 0, NULL, NULL, FALSE,
          NOW(), FALSE, NULL, NULL)"
    )
}

/// The counts of `horsies_claim_in_flight` for one worker over capped
/// queues: its live claims, its rows in flight, every row in flight, and
/// the count of each capped queue.
async fn in_flight_counts(
    client: &tokio_postgres::Client,
    hard_cap_mode: bool,
) -> (i64, i64, i64, serde_json::Value) {
    let row = client
        .query_one(
            "SELECT my_claimed, my_in_flight, global_in_flight, queue_counts
             FROM horsies_claim_in_flight($1, ARRAY['q_a', 'q_b'], $2,
                                          '{\"ISOLATED\": \"ISOLATED\"}'::jsonb)",
            &[&WORKER_ID, &hard_cap_mode],
        )
        .await
        .expect("read the in-flight counts");
    (row.get(0), row.get(1), row.get(2), row.get(3))
}

/// The concrete counts of the claim: across rows of this
/// worker and of another, live and expired leases, and two queues, the
/// claim counts the live claims of this worker (2), its RUNNING rows (1),
/// its rows in flight (3), every row in flight (5: an expired lease and a
/// PENDING row count nowhere), and per capped queue the rows in flight
/// under the hard cap and the RUNNING rows under the soft cap.
#[tokio::test]
async fn the_in_flight_counts_of_a_claim_pass() {
    const OTHER_WORKER: &str = "in-flight-count-other";
    let database = TestDatabase::create().await;
    let client = database.client();
    let rows = [
        row_values("CLAIMED", "q_a", Some(WORKER_ID), Lease::Live),
        row_values("CLAIMED", "q_a", Some(WORKER_ID), Lease::Expired),
        row_values("CLAIMED", "q_b", Some(WORKER_ID), Lease::Live),
        row_values("RUNNING", "q_b", Some(WORKER_ID), Lease::Absent),
        row_values("CLAIMED", "q_a", Some(OTHER_WORKER), Lease::Live),
        row_values("CLAIMED", "q_b", Some(OTHER_WORKER), Lease::Expired),
        row_values("RUNNING", "q_a", Some(OTHER_WORKER), Lease::Absent),
        row_values("PENDING", "q_a", None, Lease::Absent),
    ];
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
             ) VALUES {}",
            rows.join(",\n")
        ))
        .await
        .expect("seed the rows of each kind");

    let (my_claimed, my_in_flight, global_in_flight, hard) = in_flight_counts(client, true).await;
    let (_, _, _, soft) = in_flight_counts(client, false).await;

    assert_eq!(my_claimed, 2);
    assert_eq!(
        my_in_flight - my_claimed,
        1,
        "the RUNNING rows of this worker"
    );
    assert_eq!(my_in_flight, 3);
    assert_eq!(global_in_flight, 5);
    assert_eq!(hard, serde_json::json!({"q_a": 3, "q_b": 2}));
    assert_eq!(soft, serde_json::json!({"q_a": 1, "q_b": 1}));
    database.drop().await;
}
