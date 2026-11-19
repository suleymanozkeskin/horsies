//! Ports of the unreadable-value rows of Python
//! `clients/python/tests/integration/test_worker_health_api.py`: a stored slot health or
//! class capacity of another shape fails the worker state read with the
//! worker named, instead of reading as a default.

use serial_test::serial;
use uuid::Uuid;

use crate::broker::{BrokerErrorCode, PostgresBroker};
use crate::test_support::sql::{self, TestPool};

/// One snapshot row with every NOT NULL column of the baseline named.
async fn seed_snapshot(pool: &TestPool, worker_id: &str, slots_down: &str, class_capacity: &str) {
    sql::query(
        "INSERT INTO horsies_worker_states (
            worker_id, snapshot_at, hostname, pid, processes,
            max_claim_batch, max_claim_per_worker, cluster_wide_cap, queues,
            queue_priorities, queue_max_concurrency, recovery_config,
            tasks_running, tasks_claimed, memory_usage_mb, memory_percent, cpu_percent,
            children_memory_mb, worker_started_at, slots_down, class_capacity
         ) VALUES (
            $1, NOW(), 'itest-host', 4242, 2,
            2, 2, NULL, ARRAY['default'],
            '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
            0, 0, NULL, NULL, NULL,
            NULL, NOW(), $2::jsonb, $3::jsonb
         )",
    )
    .bind(worker_id)
    .bind(slots_down)
    .bind(class_capacity)
    .execute(pool)
    .await
    .expect("seed a worker snapshot");
}

async fn delete_snapshots(pool: &TestPool, worker_id: &str) {
    sql::query("DELETE FROM horsies_worker_states WHERE worker_id = $1")
        .bind(worker_id)
        .execute(pool)
        .await
        .expect("delete the snapshots");
}

/// Python `test_an_unreadable_slot_health_fails_the_read`.
#[tokio::test]
#[serial]
async fn an_unreadable_slot_health_fails_the_read() {
    let pool = crate::test_support::migrated_pool().await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let worker_id = format!("itest-{}", Uuid::new_v4().simple());
    seed_snapshot(&pool, &worker_id, r#"{"slot": 1}"#, "{}").await;

    let result = broker.get_worker_state(&worker_id).await;

    delete_snapshots(&pool, &worker_id).await;
    let error = result.expect_err("a slot health of another shape fails the read");
    assert_eq!(error.code(), BrokerErrorCode::MonitoringQueryFailed);
    assert!(error.message().contains(&worker_id), "{}", error.message());
    assert!(
        error.message().contains("slots_down is unreadable"),
        "{}",
        error.message()
    );
}

/// Python `test_an_unreadable_class_capacity_fails_the_read`: an unknown
/// budget name fails the read and is named.
#[tokio::test]
#[serial]
async fn an_unreadable_class_capacity_fails_the_read() {
    let pool = crate::test_support::migrated_pool().await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let worker_id = format!("itest-{}", Uuid::new_v4().simple());
    seed_snapshot(
        &pool,
        &worker_id,
        "[]",
        r#"{"EXCLUSIVE": {"capacity": 1, "serves": ["EXCLUSIVE"]}}"#,
    )
    .await;

    let result = broker.get_worker_state(&worker_id).await;

    delete_snapshots(&pool, &worker_id).await;
    let error = result.expect_err("an unknown budget name fails the read");
    assert_eq!(error.code(), BrokerErrorCode::MonitoringQueryFailed);
    assert!(error.message().contains(&worker_id), "{}", error.message());
    assert!(
        error.message().contains("class_capacity is unreadable"),
        "{}",
        error.message()
    );
    assert!(
        error.message().contains("'EXCLUSIVE'"),
        "{}",
        error.message()
    );
}
