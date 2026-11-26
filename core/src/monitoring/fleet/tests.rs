//! The fleet read against PostgreSQL: one test per outcome of each part, and
//! the index use of the queue tasks statement.
//!
//! Each test has a database of its own: every part reads the whole fleet.
//! The broker observes only: the read never migrates.

use serde_json::{json, Value};
use uuid::Uuid;

use super::statements::{OLDEST_PENDING_SQL, QUEUE_TASKS_SQL, WORKERS_SQL};
use super::*;
use crate::broker::monitoring_test_database::MonitoringTestDatabase;
use crate::telemetry::labels::FleetQuery;
use crate::test_support::sql::{self, TestPool};
use crate::{
    expected_schema_version, ClientFamily, PostgresBroker, StreamCoverage, StreamCoverageState,
    StreamWait,
};

const ISOLATED: Execution = Execution::Isolated;
const ASYNC: Execution = Execution::SharedAsync;
const THREAD: Execution = Execution::SharedThread;

fn observer(database: &MonitoringTestDatabase) -> PostgresBroker {
    PostgresBroker::from_test_url_observe_only(database.pool.url(), ClientFamily::Rust)
}

/// One snapshot of `worker_id` on queue `a`, `age_s` seconds old in database
/// time, with 4 processes, no metrics endpoint and no handler child.
async fn snapshot(
    pool: &TestPool,
    worker_id: &str,
    class_capacity: Value,
    slots_down: Value,
    age_s: f64,
) {
    let telemetry = json!({"endpoint": {"state": "off"}, "handler_child": {"state": "off"}});
    snapshot_with_telemetry(
        pool,
        worker_id,
        class_capacity,
        slots_down,
        telemetry,
        age_s,
    )
    .await;
}

/// As [`snapshot`], with `telemetry` as its `telemetry` column.
async fn snapshot_with_telemetry(
    pool: &TestPool,
    worker_id: &str,
    class_capacity: Value,
    slots_down: Value,
    telemetry: Value,
    age_s: f64,
) {
    sql::query(
        "INSERT INTO horsies_worker_states (
            worker_id, snapshot_at, hostname, pid, processes,
            max_claim_batch, max_claim_per_worker, queues,
            queue_priorities, queue_max_concurrency, recovery_config,
            tasks_running, tasks_claimed, worker_started_at,
            slots_down, class_capacity, telemetry
        ) VALUES (
            $1, now() - make_interval(secs => $2::double precision), 'itest-host', 4242, 4,
            0, 4, ARRAY['a']::text[],
            '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
            0, 0, now() - interval '1 hour',
            $3, $4, $5
        )",
    )
    .bind(worker_id)
    .bind(age_s)
    .bind(slots_down)
    .bind(class_capacity)
    .bind(telemetry)
    .execute(pool)
    .await
    .expect("insert a worker snapshot");
}

/// Slot 1 down: the OS refused a thread of the child.
fn one_slot_down() -> Value {
    json!([{
        "slot": 1,
        "cause": {"kind": "thread_start_refused"},
        "detail": "can't start new thread",
        "down_since": "2026-11-19T11:00:00.000000+00:00",
        "failed_boots": 1,
    }])
}

/// `rows` live rows of one queue and class in `status`, held by `owner` when
/// not `PENDING`; a `CLAIMED` row has a lease of 60 s.
async fn tasks(
    pool: &TestPool,
    queue: &str,
    execution: Execution,
    status: &str,
    owner: Option<&str>,
    rows: i64,
) {
    sql::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, execution, retention_class_key,
            sent_at, created_at, command_fingerprint_version, command_fingerprint,
            status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
            started_at, retry_count, max_retries, is_workflow_task, updated_at,
            retain_rerun_input
        )
        SELECT gen_random_uuid(), 'fleet_task', $1::text, 100, 'null', $2::text, 'forever',
               now(), now(), 1, sha256(convert_to(gen_random_uuid()::text, 'UTF8')),
               $3::text, now() - interval '10 s',
               CASE WHEN $4::text IS NULL THEN NULL ELSE now() END,
               $4::text,
               CASE WHEN $3::text = 'CLAIMED' THEN now() + interval '60 s' END,
               CASE WHEN $3::text = 'RUNNING' THEN now() END,
               0, 0, FALSE, now(), FALSE
        FROM generate_series(1, $5::bigint) AS g",
    )
    .bind(queue)
    .bind(execution.as_str())
    .bind(status)
    .bind(owner)
    .bind(rows)
    .execute(pool)
    .await
    .expect("insert task rows");
}

async fn workflows(pool: &TestPool, status: &str, rows: i64) {
    sql::query(
        "INSERT INTO horsies_workflows (
            id, name, definition_key, status, on_error, output_task_index, success_policy,
            result, error, parent_workflow_id, parent_task_index, root_workflow_id, depth,
            sent_at, created_at, started_at, completed_at, updated_at
        )
        SELECT gen_random_uuid(), 'fleet_flow', NULL, $1::text, 'fail', NULL, NULL,
               NULL, NULL, NULL, NULL, NULL, 0,
               now(), now(), now(),
               CASE WHEN $1::text IN ('RUNNING', 'PAUSED') THEN NULL ELSE now() END, now()
        FROM generate_series(1, $2::bigint) AS g",
    )
    .bind(status)
    .bind(rows)
    .execute(pool)
    .await
    .expect("insert workflow rows");
}

/// A schedule state row; `ran_s_ago` is the age of its last run, `None` for
/// a schedule that never ran.
async fn schedule(pool: &TestPool, name: &str, ran_s_ago: Option<f64>) {
    sql::query(
        "INSERT INTO horsies_schedule_state (
            schedule_name, next_run_at, last_run_at, last_task_id, run_count, config_hash,
            updated_at
        ) VALUES (
            $1::text, now() + interval '1 min',
            now() - make_interval(secs => $2::double precision),
            CASE WHEN $2::double precision IS NULL THEN NULL ELSE $3::uuid END,
            CASE WHEN $2::double precision IS NULL THEN 0 ELSE 1 END,
            decode(repeat('0d', 32), 'hex'), now()
        )",
    )
    .bind(name)
    .bind(ran_s_ago)
    .bind(Uuid::new_v4())
    .execute(pool)
    .await
    .expect("insert a schedule state row");
}

#[tokio::test]
async fn an_empty_fleet_answers_every_part() {
    let database = MonitoringTestDatabase::create().await;

    let state = read_fleet_state(&observer(&database)).await;

    assert_eq!(
        state.queue_tasks.outcome,
        Ok(QueueTasks {
            pending: vec![],
            held: vec![],
        })
    );
    let oldest = state
        .oldest_pending
        .outcome
        .expect("the oldest pending part answers");
    assert!(oldest.streams.is_empty());
    let workers = state.workers.outcome.expect("the workers part answers");
    assert_eq!(
        workers.workers,
        LiveWorkerCounts {
            serving: 0,
            degraded: 0
        }
    );
    for execution in Execution::ALL {
        assert_eq!(workers.seats.of(execution), SeatCount::default());
    }
    let coverage = state
        .class_coverage
        .outcome
        .expect("the class coverage part answers");
    assert_eq!(coverage.live_workers, 0);
    assert!(coverage.streams.is_empty());
    assert_eq!(
        state.workflows.outcome,
        Ok(WorkflowCounts {
            running: 0,
            paused: 0
        })
    );
    assert_eq!(
        state.schedules.outcome.map(|runs| runs.schedules),
        Ok(vec![])
    );
    assert_eq!(
        state.schema_version.outcome,
        Ok(SchemaVersion {
            version: expected_schema_version()
        })
    );
    database.drop().await;
}

#[tokio::test]
async fn queue_tasks_count_the_live_rows_per_status_queue_and_class() {
    let database = MonitoringTestDatabase::create().await;
    let pool = &database.pool;
    tasks(pool, "a", ISOLATED, "PENDING", None, 3).await;
    tasks(pool, "a", ASYNC, "PENDING", None, 1).await;
    tasks(pool, "b", THREAD, "PENDING", None, 2).await;
    tasks(pool, "a", ISOLATED, "CLAIMED", Some("w1"), 2).await;
    tasks(pool, "a", ISOLATED, "RUNNING", Some("w1"), 4).await;
    tasks(pool, "b", ASYNC, "RUNNING", Some("w2"), 1).await;

    let state = read_fleet_state(&observer(&database)).await;

    let pending = |queue: &str, execution: Execution, rows: u64| PendingTasks {
        queue_name: queue.to_owned(),
        execution,
        count: PendingCount::Exact(rows),
    };
    let held = |status: HeldStatus, queue: &str, execution: Execution, rows: u64| HeldTasks {
        status,
        queue_name: queue.to_owned(),
        execution,
        count: rows,
    };
    assert_eq!(
        state.queue_tasks.outcome,
        Ok(QueueTasks {
            pending: vec![
                pending("a", ISOLATED, 3),
                pending("a", ASYNC, 1),
                pending("b", THREAD, 2),
            ],
            held: vec![
                held(HeldStatus::Claimed, "a", ISOLATED, 2),
                held(HeldStatus::Running, "a", ISOLATED, 4),
                held(HeldStatus::Running, "b", ASYNC, 1),
            ],
        })
    );
    database.drop().await;
}

/// The count of a stream stops one row past the limit; a stream at the limit
/// is exact.
#[tokio::test]
async fn a_stream_past_the_count_limit_reads_as_at_least_the_limit() {
    let database = MonitoringTestDatabase::create().await;
    let pool = &database.pool;
    let limit = i64::try_from(PENDING_COUNT_LIMIT).expect("the limit fits in i64");
    tasks(pool, "a", ISOLATED, "PENDING", None, limit).await;
    tasks(pool, "b", ISOLATED, "PENDING", None, limit + 1).await;

    let state = read_fleet_state(&observer(&database)).await;

    let counts: Vec<(String, PendingCount)> = state
        .queue_tasks
        .outcome
        .expect("the queue tasks part answers")
        .pending
        .into_iter()
        .map(|stream| (stream.queue_name, stream.count))
        .collect();
    assert_eq!(
        counts,
        [
            ("a".to_owned(), PendingCount::Exact(PENDING_COUNT_LIMIT)),
            ("b".to_owned(), PendingCount::AtLeastLimit),
        ]
    );
    database.drop().await;
}

/// w1 serves with all slots; w2 has a slot down; w-gone's snapshot is older
/// than the live window. w1's ISOLATED budget holds one row past its seats.
/// Queue `a` SHARED_ASYNC has a live worker that serves it; queue `c` has
/// none.
#[tokio::test]
async fn the_class_parts_give_workers_seats_waits_and_coverage() {
    let database = MonitoringTestDatabase::create().await;
    let pool = &database.pool;
    let w1_budgets = json!({
        "ISOLATED": {"capacity": 2, "serves": ["ISOLATED", "SHARED_THREAD"]},
        "SHARED_ASYNC": {"capacity": 40, "serves": ["SHARED_ASYNC"]},
    });
    let w2_budgets = json!({"ISOLATED": {"capacity": 4, "serves": ["ISOLATED"]}});
    let gone_age = crate::core::defaults::WORKER_STATE_LIVE_WINDOW_SECONDS as f64 + 5.0;
    snapshot(pool, "w1", w1_budgets, json!([]), 5.0).await;
    snapshot(pool, "w2", w2_budgets.clone(), one_slot_down(), 5.0).await;
    snapshot(pool, "w-gone", w2_budgets, json!([]), gone_age).await;
    tasks(pool, "a", ISOLATED, "RUNNING", Some("w1"), 2).await;
    tasks(pool, "a", THREAD, "CLAIMED", Some("w1"), 1).await;
    tasks(pool, "a", ASYNC, "RUNNING", Some("w1"), 7).await;
    tasks(pool, "a", ISOLATED, "RUNNING", Some("w2"), 1).await;
    tasks(pool, "a", ASYNC, "PENDING", None, 1).await;
    tasks(pool, "c", ISOLATED, "PENDING", None, 1).await;

    let state = read_fleet_state(&observer(&database)).await;

    let workers = state.workers.outcome.expect("the workers part answers");
    assert_eq!(
        workers.workers,
        LiveWorkerCounts {
            serving: 1,
            degraded: 1
        }
    );
    assert_eq!(
        workers.seats.of(ISOLATED),
        SeatCount {
            seats: 6,
            in_use: 3
        }
    );
    assert_eq!(
        workers.seats.of(ASYNC),
        SeatCount {
            seats: 40,
            in_use: 7
        }
    );
    assert_eq!(workers.seats.of(THREAD), SeatCount::default());
    let oldest = state
        .oldest_pending
        .outcome
        .expect("the oldest pending part answers");
    let waits: Vec<(&str, Execution)> = oldest
        .streams
        .iter()
        .map(|stream| (stream.queue_name.as_str(), stream.execution))
        .collect();
    assert_eq!(waits, [("a", ASYNC), ("c", ISOLATED)]);
    for stream in &oldest.streams {
        assert!(matches!(stream.wait, StreamWait::Due { .. }), "{stream:?}");
    }
    let coverage = state
        .class_coverage
        .outcome
        .expect("the class coverage part answers");
    assert_eq!(coverage.live_workers, 2);
    assert_eq!(
        coverage.streams,
        [
            StreamCoverage {
                queue: "a".to_owned(),
                execution: ASYNC,
                coverage: StreamCoverageState::Served {
                    workers: 1,
                    capacity: 40
                },
            },
            StreamCoverage {
                queue: "c".to_owned(),
                execution: ISOLATED,
                coverage: StreamCoverageState::Unserved,
            },
        ]
    );
    database.drop().await;
}

/// The live workers whose latest snapshot records a telemetry part as down,
/// per part. The handler child member comes from the core's writer of the
/// column. A down state in an older snapshot of a worker, a worker outside
/// the live window, another state (`binding`, `starting`) and a snapshot
/// with no member of a part do not count.
#[tokio::test]
async fn the_workers_part_counts_the_live_workers_whose_telemetry_part_is_down() {
    use crate::worker::executor::handler_child::{
        AfterDown, HandlerChildDown, HandlerChildState, HandlerDownCause,
    };
    use crate::worker::metrics_endpoint::{telemetry_json, WorkerEndpoint, TELEMETRY_PART_DOWN};

    let database = MonitoringTestDatabase::create().await;
    let pool = &database.pool;
    let budgets = json!({"ISOLATED": {"capacity": 2, "serves": ["ISOLATED"]}});
    let handler_down = telemetry_json(
        &WorkerEndpoint::NoMetricsEndpoint,
        &HandlerChildState::Down(HandlerChildDown {
            cause: HandlerDownCause::StartHookFailed,
            detail: "the start hook raised".to_owned(),
            since: chrono::Utc::now(),
            next: AfterDown::StaysDown,
        }),
    );
    let endpoint_down = |handler_child: Value| {
        json!({
            "endpoint": {
                "state": TELEMETRY_PART_DOWN,
                "host": "0.0.0.0",
                "port": 9100,
                "cause": {"kind": "thread_refused", "detail": "no thread"},
                "down_since": "2026-11-19T11:00:00.000000+00:00",
            },
            "handler_child": handler_child,
        })
    };
    let gone_age = crate::core::defaults::WORKER_STATE_LIVE_WINDOW_SECONDS as f64 + 5.0;
    let snapshots = [
        (
            "w-endpoint",
            endpoint_down(json!({"state": "up", "pid": 4243})),
            5.0,
        ),
        (
            "w-both",
            endpoint_down(handler_down["handler_child"].clone()),
            5.0,
        ),
        ("w-handler", handler_down.clone(), 5.0),
        // Down in an older snapshot, up in the latest.
        ("w-back", handler_down.clone(), 20.0),
        (
            "w-back",
            json!({"endpoint": {"state": "off"}, "handler_child": {"state": "up", "pid": 4244}}),
            5.0,
        ),
        (
            "w-gone",
            endpoint_down(handler_down["handler_child"].clone()),
            gone_age,
        ),
        (
            "w-other",
            json!({
                "endpoint": {"state": "binding", "host": "0.0.0.0", "port": 9100},
                "handler_child": {"state": "starting"},
            }),
            5.0,
        ),
        ("w-no-member", json!({"endpoint": {"state": "off"}}), 5.0),
    ];
    for (worker_id, telemetry, age_s) in snapshots {
        snapshot_with_telemetry(
            pool,
            worker_id,
            budgets.clone(),
            json!([]),
            telemetry,
            age_s,
        )
        .await;
    }

    let state = read_fleet_state(&observer(&database)).await;

    let workers = state.workers.outcome.expect("the workers part answers");
    assert_eq!(
        workers.telemetry_down,
        TelemetryDownCounts {
            endpoint: 2,
            handler_child: 2,
        }
    );
    assert_eq!(
        workers.workers,
        LiveWorkerCounts {
            serving: 6,
            degraded: 0
        }
    );
    database.drop().await;
}

/// The slot health is read only by the workers part: the class coverage
/// reads the same snapshot and still answers.
#[tokio::test]
async fn an_unreadable_slots_down_refuses_the_workers_part_only() {
    let database = MonitoringTestDatabase::create().await;
    let pool = &database.pool;
    let budgets = json!({"ISOLATED": {"capacity": 4, "serves": ["ISOLATED"]}});
    snapshot(pool, "w-odd", budgets, json!({"slot": 1}), 5.0).await;
    tasks(pool, "a", ISOLATED, "PENDING", None, 1).await;

    let state = read_fleet_state(&observer(&database)).await;

    assert_eq!(
        state.workers.outcome,
        Err(FleetQueryError {
            query: FleetQuery::Workers,
            failure: FleetQueryFailure::Refused {
                reason: "worker w-odd: slots_down is unreadable: slots_down is not a JSON array"
                    .to_owned(),
            },
        })
    );
    let coverage = state
        .class_coverage
        .outcome
        .expect("the class coverage part answers");
    assert_eq!(coverage.live_workers, 1);
    assert!(state.queue_tasks.outcome.is_ok());
    assert!(state.oldest_pending.outcome.is_ok());
    assert!(state.workflows.outcome.is_ok());
    assert!(state.schedules.outcome.is_ok());
    assert!(state.schema_version.outcome.is_ok());
    database.drop().await;
}

/// Terminal workflows are not counted.
#[tokio::test]
async fn workflows_count_running_and_paused_rows() {
    let database = MonitoringTestDatabase::create().await;
    let pool = &database.pool;
    workflows(pool, "RUNNING", 3).await;
    workflows(pool, "PAUSED", 2).await;
    workflows(pool, "COMPLETED", 5).await;
    workflows(pool, "FAILED", 1).await;

    let state = read_fleet_state(&observer(&database)).await;

    assert_eq!(
        state.workflows.outcome,
        Ok(WorkflowCounts {
            running: 3,
            paused: 2
        })
    );
    database.drop().await;
}

#[tokio::test]
async fn schedules_give_the_age_of_each_last_run() {
    let database = MonitoringTestDatabase::create().await;
    let pool = &database.pool;
    schedule(pool, "nightly", Some(90.0)).await;
    schedule(pool, "hourly", None).await;

    let state = read_fleet_state(&observer(&database)).await;

    let runs = state.schedules.outcome.expect("the schedules part answers");
    let [hourly, nightly] = runs.schedules.as_slice() else {
        panic!("two schedules: {:?}", runs.schedules);
    };
    assert_eq!(hourly.schedule_name, "hourly");
    assert_eq!(hourly.last_run, LastRun::NeverRan);
    assert_eq!(nightly.schedule_name, "nightly");
    let LastRun::RanAt { at, age } = nightly.last_run else {
        panic!("nightly ran: {nightly:?}");
    };
    assert_eq!(age, runs.checked_at - at);
    assert_eq!(age.num_seconds(), 90);
    database.drop().await;
}

/// A statement that waits for a table lock past the bound is cancelled; the
/// other parts answer.
#[tokio::test]
async fn a_statement_past_the_timeout_answers_timed_out() {
    let database = MonitoringTestDatabase::create().await;
    let pool = &database.pool;
    let mut lock_holder = pool.begin().await.expect("open the lock holder");
    sql::query("LOCK TABLE horsies_workflows IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock_holder)
        .await
        .expect("lock the workflow table");

    let state = read_fleet_state(&observer(&database)).await;
    lock_holder
        .rollback()
        .await
        .expect("release the workflow table lock");

    assert_eq!(
        state.workflows.outcome,
        Err(FleetQueryError {
            query: FleetQuery::Workflows,
            failure: FleetQueryFailure::TimedOut {
                bound: FLEET_QUERY_TIMEOUT
            },
        })
    );
    assert!(state.workflows.took >= FLEET_QUERY_TIMEOUT);
    assert!(state.queue_tasks.outcome.is_ok());
    assert!(state.oldest_pending.outcome.is_ok());
    assert!(state.workers.outcome.is_ok());
    assert!(state.class_coverage.outcome.is_ok());
    assert!(state.schedules.outcome.is_ok());
    assert!(state.schema_version.outcome.is_ok());
    database.drop().await;
}

/// The server refuses a statement on a table that is gone: not transient.
#[tokio::test]
async fn a_statement_the_server_refuses_answers_database_failed() {
    let database = MonitoringTestDatabase::create().await;
    sql::query("DROP TABLE horsies_schedule_state")
        .execute(&database.pool)
        .await
        .expect("drop the schedule table");

    let state = read_fleet_state(&observer(&database)).await;

    let error = state.schedules.outcome.expect_err("the table is gone");
    assert_eq!(error.query, FleetQuery::Schedules);
    let FleetQueryFailure::DatabaseFailed { transience, detail } = &error.failure else {
        panic!("a database failure: {error:?}");
    };
    assert_eq!(*transience, Transience::Permanent);
    assert!(detail.contains("horsies_schedule_state"), "{detail}");
    assert!(state.workflows.outcome.is_ok());
    database.drop().await;
}

#[tokio::test]
async fn no_recorded_migration_refuses_the_schema_version() {
    let database = MonitoringTestDatabase::create().await;
    sql::query("DELETE FROM horsies_migrations")
        .execute(&database.pool)
        .await
        .expect("delete the bookkeeping rows");

    let state = read_fleet_state(&observer(&database)).await;

    assert_eq!(
        state.schema_version.outcome,
        Err(FleetQueryError {
            query: FleetQuery::SchemaVersion,
            failure: FleetQueryFailure::Refused {
                reason: "horsies_migrations records no successful migration".to_owned(),
            },
        })
    );
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

/// With 20,000 PENDING rows over 4 queues and 3 classes and a few held rows,
/// the planner picks the claim index and the status index on its own for
/// each statement of the read that reads task rows, and reads no task row by
/// a sequential scan.
#[tokio::test]
async fn the_task_statements_read_no_task_row_by_a_sequential_scan() {
    let database = MonitoringTestDatabase::create().await;
    let pool = &database.pool;
    for (queue, execution) in [
        ("q0", ISOLATED),
        ("q1", ASYNC),
        ("q2", THREAD),
        ("q3", ISOLATED),
    ] {
        tasks(pool, queue, execution, "PENDING", None, 5_000).await;
    }
    tasks(pool, "q0", ISOLATED, "RUNNING", Some("w1"), 20).await;
    tasks(pool, "q1", ASYNC, "CLAIMED", Some("w1"), 20).await;
    sql::query("ANALYZE horsies_tasks")
        .execute(pool)
        .await
        .expect("analyze the task table");

    let queue_tasks = QUEUE_TASKS_SQL
        .replace("$1", &(PENDING_STREAM_LIMIT + 1).to_string())
        .replace("$2", &(PENDING_COUNT_LIMIT + 1).to_string())
        .replace("$3", &(HELD_GROUP_LIMIT + 1).to_string());
    let oldest_pending =
        OLDEST_PENDING_SQL.replace("$1", &(crate::PENDING_HEAD_LIMIT + 1).to_string());
    let workers = WORKERS_SQL
        .replace(
            "$1",
            &crate::core::defaults::WORKER_STATE_LIVE_WINDOW_SECONDS.to_string(),
        )
        .replace("$2", &(crate::LIVE_SNAPSHOT_ROW_LIMIT + 1).to_string())
        .replace(
            "$3",
            &format!("'{}'", crate::worker::metrics_endpoint::TELEMETRY_PART_DOWN),
        );
    let statements: [(String, &[&str]); 3] = [
        (
            queue_tasks,
            &["horsies_tasks_claim_pending", "horsies_tasks_status"],
        ),
        (oldest_pending, &["horsies_tasks_claim_pending"]),
        (workers, &["horsies_tasks_status"]),
    ];

    for (literal, indexes) in statements {
        let plan: Value = sql::query_scalar(&format!("EXPLAIN (FORMAT JSON) {literal}"))
            .fetch_one(pool)
            .await
            .expect("explain a statement of the fleet read");
        for index in indexes {
            assert!(plan.to_string().contains(index), "{index}: {plan}");
        }
        assert!(
            !plan_has_node(&plan, "Seq Scan", "horsies_tasks", 0),
            "{plan}"
        );
    }
    database.drop().await;
}
