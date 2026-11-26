//! The fleet check and the class report against PostgreSQL. Python
//! reference: `clients/python/tests/integration/test_class_coverage.py`.
//!
//! - A pending (queue, class) is served, served without capacity, or
//!   unserved, from the live snapshots' queues and budgets.
//! - A snapshot older than the live window does not serve anything.
//! - A live snapshot with unreadable budgets fails the read and names the
//!   worker.
//! - The report gives the wait of each stream's oldest due row and the load
//!   of each budget of each live worker, counted as the claim counts it.
//! - The stream and head parts probe `horsies_tasks_claim_pending`.
//!
//! Each test has a database of its own: both reads cover the whole fleet.

use chrono::{DateTime, TimeDelta, Utc};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::broker::class_coverage::{
    BudgetLoad, StreamCoverage, StreamCoverageState, StreamWait, WorkerExecutionClassLoad,
};
use crate::broker::class_coverage_reads::{CLASS_COVERAGE_SQL, CLASS_REPORT_SQL};
use crate::broker::monitoring_test_database::MonitoringTestDatabase;
use crate::broker::result_types::BrokerErrorCode;
use crate::core::defaults::WORKER_STATE_LIVE_WINDOW_SECONDS;
use crate::core::task::Execution;
use crate::test_support::sql::{self, TestPool};

const ISOLATED: Execution = Execution::Isolated;
const ASYNC: Execution = Execution::SharedAsync;
const THREAD: Execution = Execution::SharedThread;

fn isolated_children() -> Value {
    json!({"ISOLATED": {"capacity": 4, "serves": ["ISOLATED", "SHARED_ASYNC", "SHARED_THREAD"]}})
}

fn async_children_only() -> Value {
    json!({"SHARED_ASYNC": {"capacity": 40, "serves": ["SHARED_ASYNC"]}})
}

/// One worker snapshot on `queues`, `age_s` seconds old in database time.
async fn snapshot(
    pool: &TestPool,
    worker_id: &str,
    class_capacity: Value,
    queues: &[&str],
    age_s: f64,
) {
    let queues: Vec<String> = queues.iter().map(|queue| (*queue).to_owned()).collect();
    sql::query(
        "INSERT INTO horsies_worker_states (
            worker_id, snapshot_at, hostname, pid, processes,
            max_claim_batch, max_claim_per_worker, queues,
            queue_priorities, queue_max_concurrency, recovery_config,
            tasks_running, tasks_claimed, worker_started_at,
            slots_down, class_capacity, telemetry
        ) VALUES (
            $1, now() - make_interval(secs => $2::double precision), 'itest-host', 4242, 4,
            0, 4, $3::text[],
            '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
            0, 0, now() - interval '1 hour',
            '[]'::jsonb, $4, jsonb_build_object('endpoint', jsonb_build_object('state', 'off'))
        )",
    )
    .bind(worker_id)
    .bind(age_s)
    .bind(queues)
    .bind(class_capacity)
    .execute(pool)
    .await
    .expect("insert a worker snapshot");
}

/// The lifecycle state of a seeded live row.
enum Seeded {
    /// `due_in_s` sets `enqueued_at` relative to `now()`.
    Pending {
        priority: i32,
        due_in_s: f64,
    },
    /// `lease_s` sets `claim_expires_at` relative to `now()`.
    Claimed {
        owner: &'static str,
        lease_s: f64,
    },
    Running {
        owner: &'static str,
    },
}

/// A pending row due 10 s ago at priority 100.
const DUE: Seeded = Seeded::Pending {
    priority: 100,
    due_in_s: -10.0,
};

async fn task(pool: &TestPool, queue: &str, execution: Execution, seeded: Seeded) {
    let (status, priority, due_in_s, owner, lease_s) = match seeded {
        Seeded::Pending { priority, due_in_s } => ("PENDING", priority, due_in_s, None, None),
        Seeded::Claimed { owner, lease_s } => ("CLAIMED", 100, -10.0, Some(owner), Some(lease_s)),
        Seeded::Running { owner } => ("RUNNING", 100, -10.0, Some(owner), None),
    };
    sql::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, execution, retention_class_key,
            sent_at, created_at, command_fingerprint_version, command_fingerprint,
            status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
            started_at, retry_count, max_retries, is_workflow_task, updated_at,
            retain_rerun_input
        ) VALUES (
            $1, 'class_task', $2, $3, 'null', $4, 'forever',
            now(), now(), 1, decode(repeat('0c', 32), 'hex'),
            $5, now() + make_interval(secs => $6::double precision),
            CASE WHEN $7::text IS NULL THEN NULL ELSE now() END,
            $7::text,
            CASE WHEN $8::double precision IS NULL THEN NULL
                 ELSE now() + make_interval(secs => $8::double precision) END,
            CASE WHEN $5::text = 'RUNNING' THEN now() ELSE NULL END,
            0, 0, FALSE, now(), FALSE
        )",
    )
    .bind(Uuid::new_v4())
    .bind(queue)
    .bind(priority)
    .bind(execution.as_str())
    .bind(status)
    .bind(due_in_s)
    .bind(owner)
    .bind(lease_s)
    .execute(pool)
    .await
    .expect("insert a task row");
}

fn served(workers: usize, capacity: u64) -> StreamCoverageState {
    StreamCoverageState::Served { workers, capacity }
}

fn stream(queue: &str, execution: Execution, state: StreamCoverageState) -> StreamCoverage {
    StreamCoverage {
        queue: queue.to_owned(),
        execution,
        coverage: state,
    }
}

#[tokio::test]
async fn each_coverage_state_from_the_live_snapshots() {
    let database = MonitoringTestDatabase::create().await;
    let pool = &database.pool;
    snapshot(pool, "w-iso", isolated_children(), &["a"], 5.0).await;
    let down = json!({"ISOLATED": {"capacity": 0, "serves": ["ISOLATED", "SHARED_ASYNC", "SHARED_THREAD"]}});
    snapshot(pool, "w-down", down, &["b"], 5.0).await;
    task(pool, "a", ASYNC, DUE).await;
    task(pool, "b", ISOLATED, DUE).await;
    task(pool, "c", THREAD, DUE).await;

    let coverage = database
        .broker()
        .get_execution_class_coverage()
        .await
        .expect("the fleet check answers");

    assert_eq!(coverage.live_workers, 2);
    assert_eq!(
        coverage.streams,
        [
            stream("a", ASYNC, served(1, 4)),
            stream(
                "b",
                ISOLATED,
                StreamCoverageState::NoCapacity { workers: 1 }
            ),
            stream("c", THREAD, StreamCoverageState::Unserved),
        ]
    );
    database.drop().await;
}

#[tokio::test]
async fn a_snapshot_older_than_the_live_window_serves_nothing() {
    let database = MonitoringTestDatabase::create().await;
    let pool = &database.pool;
    let gone = WORKER_STATE_LIVE_WINDOW_SECONDS as f64 + 5.0;
    snapshot(pool, "w-gone", isolated_children(), &["a"], gone).await;
    task(pool, "a", ISOLATED, DUE).await;

    let coverage = database
        .broker()
        .get_execution_class_coverage()
        .await
        .expect("the fleet check answers");

    assert_eq!(coverage.live_workers, 0);
    assert_eq!(
        coverage.streams,
        [stream("a", ISOLATED, StreamCoverageState::Unserved)]
    );
    database.drop().await;
}

/// A worker whose slots went down after an earlier snapshot.
#[tokio::test]
async fn only_the_latest_snapshot_of_a_worker_counts() {
    let database = MonitoringTestDatabase::create().await;
    let pool = &database.pool;
    snapshot(pool, "w1", isolated_children(), &["a"], 60.0).await;
    let down = json!({"ISOLATED": {"capacity": 0, "serves": ["ISOLATED", "SHARED_ASYNC", "SHARED_THREAD"]}});
    snapshot(pool, "w1", down, &["a"], 1.0).await;
    task(pool, "a", ISOLATED, DUE).await;

    let coverage = database
        .broker()
        .get_execution_class_coverage()
        .await
        .expect("the fleet check answers");

    assert_eq!(coverage.live_workers, 1);
    assert_eq!(
        coverage.streams[0].coverage,
        StreamCoverageState::NoCapacity { workers: 1 }
    );
    database.drop().await;
}

/// The shared task went back to the queue as ISOLATED; every live worker
/// runs only SHARED_ASYNC children.
#[tokio::test]
async fn an_isolated_retry_of_a_shared_task_is_unserved_without_isolated_children() {
    let database = MonitoringTestDatabase::create().await;
    let pool = &database.pool;
    snapshot(pool, "w-async", async_children_only(), &["a"], 5.0).await;
    task(pool, "a", ASYNC, DUE).await;
    task(pool, "a", ISOLATED, DUE).await;

    let coverage = database
        .broker()
        .get_execution_class_coverage()
        .await
        .expect("the fleet check answers");

    assert_eq!(
        coverage.streams,
        [
            stream("a", ISOLATED, StreamCoverageState::Unserved),
            stream("a", ASYNC, served(1, 40)),
        ]
    );
    database.drop().await;
}

/// A row due later still needs a worker of its class; rows that are not
/// PENDING make no stream.
#[tokio::test]
async fn a_delayed_row_is_pending_and_a_running_row_is_not() {
    let database = MonitoringTestDatabase::create().await;
    let pool = &database.pool;
    let delayed = Seeded::Pending {
        priority: 100,
        due_in_s: 3600.0,
    };
    task(pool, "a", ISOLATED, delayed).await;
    task(pool, "b", ISOLATED, Seeded::Running { owner: "w-gone" }).await;

    let coverage = database
        .broker()
        .get_execution_class_coverage()
        .await
        .expect("the fleet check answers");

    assert_eq!(
        coverage.streams,
        [stream("a", ISOLATED, StreamCoverageState::Unserved)]
    );
    database.drop().await;
}

#[tokio::test]
async fn a_live_snapshot_with_unreadable_budgets_fails_both_reads() {
    let database = MonitoringTestDatabase::create().await;
    let pool = &database.pool;
    let odd = json!({"EXCLUSIVE": {"capacity": 1, "serves": []}});
    snapshot(pool, "w-odd", odd, &["a"], 5.0).await;
    task(pool, "a", ISOLATED, DUE).await;
    let broker = database.broker();

    let coverage = broker
        .get_execution_class_coverage()
        .await
        .expect_err("refused");
    let report = broker
        .get_execution_class_report()
        .await
        .expect_err("refused");

    for error in [coverage, report] {
        assert_eq!(error.code(), BrokerErrorCode::MonitoringQueryFailed);
        assert!(!error.retryable());
        assert!(error.message().contains("w-odd"), "{}", error.message());
        assert!(
            error.message().contains("class_capacity is unreadable"),
            "{}",
            error.message()
        );
    }
    database.drop().await;
}

#[tokio::test]
async fn the_oldest_due_row_across_priorities_is_the_wait() {
    let database = MonitoringTestDatabase::create().await;
    let pool = &database.pool;
    snapshot(pool, "w1", isolated_children(), &["a"], 5.0).await;
    for (priority, due_in_s) in [(1, -30.0), (50, -300.0), (50, -100.0), (90, 600.0)] {
        task(pool, "a", ISOLATED, Seeded::Pending { priority, due_in_s }).await;
    }
    let async_later = Seeded::Pending {
        priority: 100,
        due_in_s: 120.0,
    };
    task(pool, "a", ASYNC, async_later).await;

    let report = database
        .broker()
        .get_execution_class_report()
        .await
        .expect("the report answers");

    let oldest: DateTime<Utc> = sql::query_scalar(
        "SELECT min(enqueued_at) FROM horsies_tasks WHERE execution = 'ISOLATED'",
    )
    .fetch_one(pool)
    .await
    .expect("oldest isolated row");
    let next_due: DateTime<Utc> =
        sql::query_scalar("SELECT enqueued_at FROM horsies_tasks WHERE execution = 'SHARED_ASYNC'")
            .fetch_one(pool)
            .await
            .expect("the shared row");
    let [isolated, shared] = report.streams.as_slice() else {
        panic!("two streams: {:?}", report.streams);
    };
    assert_eq!(
        (isolated.queue.as_str(), isolated.execution),
        ("a", ISOLATED)
    );
    assert_eq!(
        isolated.wait,
        StreamWait::Due {
            oldest_due_at: oldest,
            wait: report.checked_at - oldest,
        }
    );
    assert!(report.checked_at - oldest >= TimeDelta::seconds(300));
    assert_eq!(isolated.coverage, served(1, 4));
    assert_eq!(
        shared.wait,
        StreamWait::NoneDue {
            next_due_at: next_due
        }
    );
    database.drop().await;
}

/// RUNNING and live CLAIMED rows count; a CLAIMED row with an expired lease
/// and another worker's rows do not.
#[tokio::test]
async fn budget_load_counts_rows_as_the_claim_does() {
    let database = MonitoringTestDatabase::create().await;
    let pool = &database.pool;
    let two_budgets = json!({
        "ISOLATED": {"capacity": 4, "serves": ["ISOLATED", "SHARED_THREAD"]},
        "SHARED_ASYNC": {"capacity": 40, "serves": ["SHARED_ASYNC"]},
    });
    snapshot(pool, "w1", two_budgets, &["a"], 5.0).await;
    task(pool, "a", ISOLATED, Seeded::Running { owner: "w1" }).await;
    let live_lease = Seeded::Claimed {
        owner: "w1",
        lease_s: 60.0,
    };
    task(pool, "a", THREAD, live_lease).await;
    let expired_lease = Seeded::Claimed {
        owner: "w1",
        lease_s: -60.0,
    };
    task(pool, "a", THREAD, expired_lease).await;
    task(pool, "a", ASYNC, Seeded::Running { owner: "w1" }).await;
    task(pool, "a", ASYNC, Seeded::Running { owner: "w9" }).await;

    let report = database
        .broker()
        .get_execution_class_report()
        .await
        .expect("the report answers");

    let [worker] = report.workers.as_slice() else {
        panic!("one live worker: {:?}", report.workers);
    };
    assert_eq!(
        *worker,
        WorkerExecutionClassLoad {
            worker_id: "w1".to_owned(),
            snapshot_at: worker.snapshot_at,
            budgets: vec![
                BudgetLoad {
                    budget: ISOLATED,
                    capacity: 4,
                    serves: vec![ISOLATED, THREAD],
                    in_flight: 2,
                },
                BudgetLoad {
                    budget: ASYNC,
                    capacity: 40,
                    serves: vec![ASYNC],
                    in_flight: 1,
                },
            ],
        }
    );
    let free: Vec<u64> = worker.budgets.iter().map(BudgetLoad::free).collect();
    assert_eq!(free, [2, 39]);
    database.drop().await;
}

#[tokio::test]
async fn rows_of_a_class_the_worker_does_not_serve_fail_the_report() {
    let database = MonitoringTestDatabase::create().await;
    let pool = &database.pool;
    snapshot(pool, "w1", async_children_only(), &["a"], 5.0).await;
    task(pool, "a", ISOLATED, Seeded::Running { owner: "w1" }).await;

    let error = database
        .broker()
        .get_execution_class_report()
        .await
        .expect_err("the report is refused");

    assert_eq!(error.code(), BrokerErrorCode::MonitoringQueryFailed);
    assert!(!error.retryable());
    assert!(error.message().contains("w1"), "{}", error.message());
    assert!(error.message().contains("ISOLATED"), "{}", error.message());
    database.drop().await;
}

/// Whether a JSON plan has a node of `node_type` on `relation`. A plan tree
/// is a few levels deep; the walk stops at `depth` 64.
fn plan_has_node(plan: &Value, node_type: &str, relation: &str, depth: usize) -> bool {
    assert!(depth < 64, "plan deeper than 64 levels");
    match plan {
        Value::Array(values) => values
            .iter()
            .any(|value| plan_has_node(value, node_type, relation, depth + 1)),
        Value::Object(fields) => {
            let this = fields.get("Node Type").and_then(Value::as_str) == Some(node_type)
                && fields.get("Relation Name").and_then(Value::as_str) == Some(relation);
            this || fields
                .values()
                .any(|value| plan_has_node(value, node_type, relation, depth + 1))
        }
        _ => false,
    }
}

/// Python `TestClassReadsUseTheClaimIndex`: with 20,000 PENDING rows over 4
/// queues, 3 classes and 100 priorities, the planner picks the claim index
/// for the stream and head parts on its own, and reads no task row by a
/// sequential scan.
#[tokio::test]
async fn the_stream_and_head_parts_probe_the_pending_class_index() {
    let database = MonitoringTestDatabase::create().await;
    let pool = &database.pool;
    sql::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, execution, retention_class_key,
            sent_at, created_at, command_fingerprint_version, command_fingerprint,
            status, enqueued_at, retry_count, max_retries, is_workflow_task, updated_at,
            retain_rerun_input
        )
        SELECT gen_random_uuid(), 'class_task', 'q' || (g % 4), 1 + (g % 100), 'null',
               (ARRAY['ISOLATED', 'SHARED_ASYNC', 'SHARED_THREAD'])[1 + g % 3], 'forever',
               now(), now(), 1, sha256(convert_to(CAST(g AS text), 'UTF8')),
               'PENDING', now() - g * interval '1 ms', 0, 0, FALSE, now(), FALSE
        FROM generate_series(1, 20000) AS g",
    )
    .execute(pool)
    .await
    .expect("seed the pending rows");
    sql::query("ANALYZE horsies_tasks")
        .execute(pool)
        .await
        .expect("analyze the task table");

    for statement in [CLASS_COVERAGE_SQL, CLASS_REPORT_SQL] {
        let literal = statement
            .replace("$1", &WORKER_STATE_LIVE_WINDOW_SECONDS.to_string())
            .replace("$2", "20001")
            .replace("$3", "11");
        let plan: Value = sql::query_scalar(&format!("EXPLAIN (FORMAT JSON) {literal}"))
            .fetch_one(pool)
            .await
            .expect("explain the class read");
        assert!(
            plan.to_string().contains("horsies_tasks_claim_pending"),
            "{plan}"
        );
        assert!(
            !plan_has_node(&plan, "Seq Scan", "horsies_tasks", 0),
            "{plan}"
        );
    }
    database.drop().await;
}
