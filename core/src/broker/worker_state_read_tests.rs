//! The worker state reads against PostgreSQL: the slot health, the class
//! budgets and the child memory of each snapshot, and the refusal of a stored
//! value of another shape. Python reference:
//! `clients/python/tests/integration/test_worker_health_api.py`.

use std::num::NonZeroU64;
use std::sync::Arc;

use chrono::TimeZone;
use chrono::Utc;
use serde_json::{json, Value};

use crate::broker::monitoring_test_database::MonitoringTestDatabase;
use crate::broker::result_types::BrokerErrorCode;
use crate::broker::slot_health::{ClearableBootCause, DownSlot, ShortageErrno, SlotHealth};
use crate::core::config::worker_profile::WorkerProfile;
use crate::core::task::Execution;
use crate::test_support::sql::{self, TestPool};
use crate::worker::children::{child_layout, ChildrenView, SeatsPerChild};
use crate::worker::executor::slots::board::PoolBoard;
use crate::worker::worker_state::class_capacity_of;

/// One snapshot of a worker with 2 processes on queue `default`, written now.
async fn insert_snapshot(
    pool: &TestPool,
    worker_id: &str,
    slots_down: Value,
    class_capacity: Value,
) {
    sql::query(
        "INSERT INTO horsies_worker_states (
            worker_id, snapshot_at, hostname, pid, processes,
            max_claim_batch, max_claim_per_worker, queues,
            queue_priorities, queue_max_concurrency, recovery_config,
            tasks_running, tasks_claimed, children_memory_mb, worker_started_at,
            slots_down, class_capacity, telemetry
        ) VALUES (
            $1, now(), 'itest-host', 4242, 2,
            2, 2, ARRAY['default'],
            '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
            0, 0, 96.5, now(),
            $2, $3, jsonb_build_object('endpoint', jsonb_build_object('state', 'off'))
        )",
    )
    .bind(worker_id)
    .bind(slots_down)
    .bind(class_capacity)
    .execute(pool)
    .await
    .expect("insert a worker snapshot");
}

/// Python `test_worker_state_reads_the_slot_health`: a row with down slots
/// reads as `SlotsDown`, an empty list as `AllSlotsServing`.
#[tokio::test]
async fn worker_state_reads_the_slot_health() {
    let database = MonitoringTestDatabase::create().await;
    let stored = json!([{
        "slot": 1,
        "cause": {"kind": "resource_shortage", "errno": "EMFILE"},
        "detail": "OSError: [Errno 24] Too many open files",
        "down_since": "2026-01-01T00:00:00+00:00",
        "failed_boots": 2,
    }]);
    insert_snapshot(&database.pool, "w-degraded", stored, json!({})).await;
    insert_snapshot(&database.pool, "w-serving", json!([]), json!({})).await;
    let broker = database.broker();

    let degraded = broker
        .get_worker_state("w-degraded")
        .await
        .expect("the read answers")
        .expect("the worker reported");
    let serving = broker
        .get_worker_state("w-serving")
        .await
        .expect("the read answers")
        .expect("the worker reported");

    let SlotHealth::SlotsDown(slots) = &degraded.slot_health else {
        panic!("a degraded worker: {:?}", degraded.slot_health);
    };
    assert_eq!(
        slots.as_slice(),
        [DownSlot {
            slot: 1,
            cause: ClearableBootCause::ResourceShortage(ShortageErrno::Emfile),
            detail: "OSError: [Errno 24] Too many open files".to_owned(),
            down_since: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            failed_boots: NonZeroU64::new(2).unwrap(),
        }]
    );
    assert_eq!(serving.slot_health, SlotHealth::AllSlotsServing);
    assert_eq!(serving.children_memory_mb, Some(96.5));
    database.drop().await;
}

/// Python `test_worker_state_reads_the_class_capacity`: the budgets read the
/// same in the single, history and list reads.
#[tokio::test]
async fn worker_state_reads_the_class_capacity() {
    let database = MonitoringTestDatabase::create().await;
    let stored = json!({
        "SHARED_ASYNC": {"capacity": 40, "serves": ["SHARED_ASYNC"]},
        "ISOLATED": {"capacity": 2, "serves": ["ISOLATED", "SHARED_THREAD"]},
    });
    insert_snapshot(&database.pool, "w1", json!([]), stored).await;
    let broker = database.broker();

    let latest = broker
        .get_worker_state("w1")
        .await
        .expect("the read answers")
        .expect("the worker reported");
    let history = broker
        .get_worker_state_history("w1", None)
        .await
        .expect("the history read answers");
    let listed = broker
        .list_worker_states()
        .await
        .expect("the list read answers");

    let budgets: Vec<(Execution, u64, Vec<Execution>)> = latest
        .class_capacity
        .as_slice()
        .iter()
        .map(|budget| (budget.budget, budget.capacity, budget.serves.clone()))
        .collect();
    assert_eq!(
        budgets,
        [
            (
                Execution::Isolated,
                2,
                vec![Execution::Isolated, Execution::SharedThread]
            ),
            (Execution::SharedAsync, 40, vec![Execution::SharedAsync]),
        ]
    );
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].class_capacity, latest.class_capacity);
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].class_capacity, latest.class_capacity);
    database.drop().await;
}

/// Python `test_an_unreadable_slot_health_fails_the_read`: a stored value of
/// another shape is a monitoring query failure that names the worker, not a
/// quiet default.
#[tokio::test]
async fn an_unreadable_slot_health_fails_the_read() {
    let database = MonitoringTestDatabase::create().await;
    insert_snapshot(&database.pool, "w-odd", json!({"slot": 1}), json!({})).await;
    let broker = database.broker();

    let failed = broker
        .get_worker_state("w-odd")
        .await
        .expect_err("an unreadable slots_down fails the read");
    let listed = broker
        .list_worker_states()
        .await
        .expect_err("the list read meets the same row");

    for error in [failed, listed] {
        assert_eq!(error.code(), BrokerErrorCode::MonitoringQueryFailed);
        assert!(!error.retryable());
        assert!(error.message().contains("w-odd"), "{}", error.message());
        assert!(
            error.message().contains("slots_down is unreadable"),
            "{}",
            error.message()
        );
    }
    database.drop().await;
}

/// Python `test_an_unreadable_class_capacity_fails_the_read`: an unknown
/// budget name fails the read and is named.
#[tokio::test]
async fn an_unreadable_class_capacity_fails_the_read() {
    let database = MonitoringTestDatabase::create().await;
    insert_snapshot(
        &database.pool,
        "w-odd",
        json!([]),
        json!({"EXCLUSIVE": {"capacity": 1, "serves": ["EXCLUSIVE"]}}),
    )
    .await;
    let broker = database.broker();

    let failed = broker
        .get_worker_state_history("w-odd", Some(10))
        .await
        .expect_err("an unreadable class_capacity fails the read");

    assert_eq!(failed.code(), BrokerErrorCode::MonitoringQueryFailed);
    assert!(!failed.retryable());
    assert!(failed.message().contains("w-odd"), "{}", failed.message());
    assert!(
        failed.message().contains("class_capacity is unreadable"),
        "{}",
        failed.message()
    );
    assert!(
        failed.message().contains("'EXCLUSIVE'"),
        "{}",
        failed.message()
    );
    database.drop().await;
}

/// One snapshot of `worker_id` taken `seconds_ago` before the database clock,
/// with `tasks_running` rows running; every other column as
/// [`insert_snapshot`] writes it, with all slots serving and no budget.
async fn insert_snapshot_at(
    pool: &TestPool,
    worker_id: &str,
    seconds_ago: f64,
    tasks_running: i32,
) {
    sql::query(
        "INSERT INTO horsies_worker_states (
            worker_id, snapshot_at, hostname, pid, processes,
            max_claim_batch, max_claim_per_worker, queues,
            queue_priorities, queue_max_concurrency, recovery_config,
            tasks_running, tasks_claimed, children_memory_mb, worker_started_at,
            slots_down, class_capacity, telemetry
        ) VALUES (
            $1, now() - make_interval(secs => $2::float8), 'itest-host', 4242, 2,
            2, 2, ARRAY['default'],
            '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
            $3, 0, 96.5, now() - interval '1 hour',
            '[]'::jsonb, '{}'::jsonb, jsonb_build_object('endpoint', jsonb_build_object('state', 'off'))
        )",
    )
    .bind(worker_id)
    .bind(seconds_ago)
    .bind(tasks_running)
    .execute(pool)
    .await
    .expect("insert a worker snapshot");
}

/// Python `test_broker_postgres.py::TestWorkerStateReads::test_list_maps_rows_to_snapshots`:
/// the list holds the latest snapshot of each worker, in worker id order,
/// with the stored counts and queues.
#[tokio::test]
async fn the_list_holds_the_latest_snapshot_of_each_worker() {
    let database = MonitoringTestDatabase::create().await;
    insert_snapshot_at(&database.pool, "w2", 5.0, 1).await;
    insert_snapshot_at(&database.pool, "w1", 60.0, 1).await;
    insert_snapshot_at(&database.pool, "w1", 5.0, 2).await;
    let broker = database.broker();

    let listed = broker
        .list_worker_states()
        .await
        .expect("the list read answers");

    let read: Vec<(&str, i32, &[String], &str, i32)> = listed
        .iter()
        .map(|snapshot| {
            (
                snapshot.worker_id.as_str(),
                snapshot.tasks_running,
                snapshot.queues.as_slice(),
                snapshot.hostname.as_str(),
                snapshot.pid,
            )
        })
        .collect();
    let default_queue = ["default".to_owned()];
    assert_eq!(
        read,
        [
            ("w1", 2, default_queue.as_slice(), "itest-host", 4242),
            ("w2", 1, default_queue.as_slice(), "itest-host", 4242),
        ]
    );
    database.drop().await;
}

/// Python `test_broker_postgres.py::TestWorkerStateReads::test_get_returns_none_when_unknown`:
/// a worker that never reported reads as `None`, not as an error.
#[tokio::test]
async fn a_worker_that_never_reported_reads_as_none() {
    let database = MonitoringTestDatabase::create().await;
    insert_snapshot_at(&database.pool, "w-known", 5.0, 0).await;
    let broker = database.broker();

    let read = broker
        .get_worker_state("missing")
        .await
        .expect("the read answers");

    assert_eq!(read, None);
    database.drop().await;
}

/// Python `test_broker_postgres.py::TestWorkerStateReads::test_get_returns_snapshot`:
/// a worker that reported reads as its latest snapshot.
#[tokio::test]
async fn a_worker_that_reported_reads_as_its_latest_snapshot() {
    let database = MonitoringTestDatabase::create().await;
    insert_snapshot_at(&database.pool, "w9", 60.0, 1).await;
    insert_snapshot_at(&database.pool, "w9", 5.0, 2).await;
    let broker = database.broker();

    let read = broker
        .get_worker_state("w9")
        .await
        .expect("the read answers")
        .expect("the worker reported");

    assert_eq!(read.worker_id, "w9");
    assert_eq!(read.tasks_running, 2);
    database.drop().await;
}

/// Python `test_broker_postgres.py::TestWorkerStateReads::test_history_returns_snapshots`:
/// the history holds every snapshot of the worker, newest first, up to the
/// limit, and no snapshot of another worker.
#[tokio::test]
async fn the_history_holds_the_snapshots_of_the_worker_newest_first_up_to_the_limit() {
    let database = MonitoringTestDatabase::create().await;
    insert_snapshot_at(&database.pool, "worker-1", 60.0, 1).await;
    insert_snapshot_at(&database.pool, "worker-1", 5.0, 2).await;
    insert_snapshot_at(&database.pool, "worker-2", 5.0, 3).await;
    let broker = database.broker();

    let within_limit = broker
        .get_worker_state_history("worker-1", Some(10))
        .await
        .expect("the history read answers");
    let at_limit = broker
        .get_worker_state_history("worker-1", Some(1))
        .await
        .expect("the history read answers");

    let running: Vec<i32> = within_limit
        .iter()
        .map(|snapshot| snapshot.tasks_running)
        .collect();
    assert_eq!(running, [2, 1]);
    assert!(within_limit
        .iter()
        .all(|snapshot| snapshot.worker_id == "worker-1"));
    assert_eq!(at_limit.len(), 1);
    assert_eq!(at_limit[0].tasks_running, 2);
    database.drop().await;
}

/// Python `test_broker_postgres.py::TestWorkerStateReads::test_the_snapshot_carries_the_class_capacity`,
/// case `serves-no-class`: a worker with no budget reads as no budgets. The
/// case `one-budget` is [`worker_state_reads_the_class_capacity`].
#[tokio::test]
async fn a_worker_with_no_budget_reads_as_no_budgets() {
    let database = MonitoringTestDatabase::create().await;
    insert_snapshot(&database.pool, "w-none", json!([]), json!({})).await;
    let broker = database.broker();

    let read = broker
        .get_worker_state("w-none")
        .await
        .expect("the read answers")
        .expect("the worker reported");

    assert!(
        read.class_capacity.as_slice().is_empty(),
        "{:?}",
        read.class_capacity
    );
    database.drop().await;
}

/// The `class_capacity` that a new worker of `profile` writes in its first
/// snapshot: the slots of a new board (each booting) with the seats of each
/// shared child, as `WorkerChildren::start` builds them.
fn first_class_capacity_of(profile: &WorkerProfile) -> Value {
    let board = Arc::new(PoolBoard::new(child_layout(profile)));
    let children = ChildrenView::new(board, SeatsPerChild::of_profile(profile));
    class_capacity_of(&children.service())
}

/// Python `test_claim_plan.py::TestClassCapacityOfAProfile::test_the_snapshot_value_reads_back`:
/// the class capacity that a worker of each profile writes in its snapshot
/// reads back through the worker state read as the budgets of the profile.
#[tokio::test]
async fn the_class_capacity_a_worker_of_each_profile_writes_reads_back() {
    let database = MonitoringTestDatabase::create().await;
    let broker = database.broker();
    let cases: [(&str, Value, Vec<(Execution, u64, Vec<Execution>)>); 4] = [
        (
            "isolated-and-shared",
            json!({
                "isolated": {"kind": "isolated_children", "count": 2},
                "shared_async": {"kind": "shared_async_children", "count": 3, "seats": 5},
            }),
            vec![
                (
                    Execution::Isolated,
                    2,
                    vec![Execution::Isolated, Execution::SharedThread],
                ),
                (Execution::SharedAsync, 15, vec![Execution::SharedAsync]),
            ],
        ),
        (
            "shared-only",
            json!({
                "shared_async": {"kind": "shared_async_children", "count": 1, "seats": 4},
            }),
            vec![(Execution::SharedAsync, 4, vec![Execution::SharedAsync])],
        ),
        (
            "isolated-only",
            json!({
                "isolated": {"kind": "isolated_children", "count": 2},
            }),
            vec![(
                Execution::Isolated,
                2,
                vec![
                    Execution::Isolated,
                    Execution::SharedAsync,
                    Execution::SharedThread,
                ],
            )],
        ),
        (
            "thread",
            json!({
                "isolated": {"kind": "isolated_children", "count": 1},
                "shared_thread": {"kind": "shared_thread_children", "count": 2, "seats": 3},
            }),
            vec![
                (
                    Execution::Isolated,
                    1,
                    vec![Execution::Isolated, Execution::SharedAsync],
                ),
                (Execution::SharedThread, 6, vec![Execution::SharedThread]),
            ],
        ),
    ];

    for (case, profile, expected) in cases {
        let profile: WorkerProfile = serde_json::from_value(profile).expect("the profile is valid");
        let worker_id = format!("w-{case}");
        insert_snapshot(
            &database.pool,
            &worker_id,
            json!([]),
            first_class_capacity_of(&profile),
        )
        .await;

        let read = broker
            .get_worker_state(&worker_id)
            .await
            .expect("the read answers")
            .expect("the worker reported");

        let budgets: Vec<(Execution, u64, Vec<Execution>)> = read
            .class_capacity
            .as_slice()
            .iter()
            .map(|budget| (budget.budget, budget.capacity, budget.serves.clone()))
            .collect();
        assert_eq!(budgets, expected, "{case}");
    }
    database.drop().await;
}
