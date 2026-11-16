//! The worker state reads against PostgreSQL: the slot health, the class
//! budgets and the child memory of each snapshot, and the refusal of a stored
//! value of another shape. Python reference:
//! `tests/integration/test_worker_health_api.py`.

use std::num::NonZeroU64;

use chrono::TimeZone;
use chrono::Utc;
use serde_json::{json, Value};
use sqlx::PgPool;

use crate::broker::monitoring_test_database::MonitoringTestDatabase;
use crate::broker::result_types::BrokerErrorCode;
use crate::broker::slot_health::{ClearableBootCause, DownSlot, ShortageErrno, SlotHealth};
use crate::core::task::Execution;

/// One snapshot of a worker with 2 processes on queue `default`, written now.
async fn insert_snapshot(pool: &PgPool, worker_id: &str, slots_down: Value, class_capacity: Value) {
    sqlx::query(
        "INSERT INTO horsies_worker_states (
            worker_id, snapshot_at, hostname, pid, processes,
            max_claim_batch, max_claim_per_worker, queues,
            queue_priorities, queue_max_concurrency, recovery_config,
            tasks_running, tasks_claimed, children_memory_mb, worker_started_at,
            slots_down, class_capacity
        ) VALUES (
            $1, now(), 'itest-host', 4242, 2,
            2, 2, ARRAY['default'],
            '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
            0, 0, 96.5, now(),
            $2, $3
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
        assert_eq!(error.code, BrokerErrorCode::MonitoringQueryFailed);
        assert!(!error.retryable);
        assert!(error.message.contains("w-odd"), "{}", error.message);
        assert!(
            error.message.contains("slots_down is unreadable"),
            "{}",
            error.message
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

    assert_eq!(failed.code, BrokerErrorCode::MonitoringQueryFailed);
    assert!(!failed.retryable);
    assert!(failed.message.contains("w-odd"), "{}", failed.message);
    assert!(
        failed.message.contains("class_capacity is unreadable"),
        "{}",
        failed.message
    );
    assert!(failed.message.contains("'EXCLUSIVE'"), "{}", failed.message);
    database.drop().await;
}
