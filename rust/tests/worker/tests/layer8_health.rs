#![allow(clippy::unwrap_used)]

//! Layer 8 e2e tests: worker & database health API.
//!
//! Mirrors Python's `tests/e2e/test_worker_health.py` and
//! `tests/integration/test_worker_health_api.py`.
//!
//! Run with:
//!   cargo test -p horsies-test-worker --test layer8_health -- --test-threads=1

use horsies::PostgresBroker;
use horsies_test_support::db;

async fn broker() -> PostgresBroker {
    let pool = db::create_pool().await;
    db::run_migrations(&pool).await;
    PostgresBroker::from_pool(pool)
}

// ---------------------------------------------------------------------------
// ping_database
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_ping_database_returns_latency() {
    let broker = broker().await;
    let ping = broker.ping_database().await.unwrap();
    assert!(
        ping.latency_ms >= 0.0,
        "latency should be non-negative, got {}",
        ping.latency_ms
    );
    assert!(
        ping.latency_ms < 10_000.0,
        "a local SELECT 1 should be fast, got {}ms",
        ping.latency_ms
    );
}

// ---------------------------------------------------------------------------
// worker-state reads (list / get / history)
// ---------------------------------------------------------------------------

const INSERT_SNAPSHOT_SQL: &str = "\
INSERT INTO horsies_worker_states (
    worker_id, snapshot_at, hostname, pid, processes, max_claim_batch,
    max_claim_per_worker, cluster_wide_cap, queues, queue_priorities,
    queue_max_concurrency, recovery_config, tasks_running, tasks_claimed,
    memory_usage_mb, memory_percent, cpu_percent, worker_started_at
) VALUES (
    $1, $2, 'host1', 100, 4, 2, 8, NULL, ARRAY['default']::text[], NULL, NULL,
    NULL, $3, 0, NULL, NULL, NULL, $4
)";

async fn insert_snapshot(
    pool: &sqlx::PgPool,
    worker_id: &str,
    snapshot_at: chrono::DateTime<chrono::Utc>,
    tasks_running: i32,
    started_at: chrono::DateTime<chrono::Utc>,
) {
    sqlx::query(INSERT_SNAPSHOT_SQL)
        .bind(worker_id)
        .bind(snapshot_at)
        .bind(tasks_running)
        .bind(started_at)
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test]
#[serial_test::serial]
async fn test_list_worker_states_latest_per_worker_includes_idle() {
    let pool = db::create_pool().await;
    db::run_migrations(&pool).await;
    db::clean_tables(&pool).await;
    let broker = horsies::PostgresBroker::from_pool(pool.clone());

    let t0 = chrono::Utc::now() - chrono::Duration::seconds(30);
    let t1 = chrono::Utc::now() - chrono::Duration::seconds(10);
    // Worker A: two snapshots; the latest (t1) reports 5 running tasks.
    insert_snapshot(&pool, "worker-a", t0, 1, t0).await;
    insert_snapshot(&pool, "worker-a", t1, 5, t0).await;
    // Worker B: a single snapshot, idle (0 running) — must still appear.
    insert_snapshot(&pool, "worker-b", t0, 0, t0).await;

    let states = broker.list_worker_states().await.unwrap();
    assert_eq!(states.len(), 2, "one latest snapshot per worker");

    let a = states.iter().find(|s| s.worker_id == "worker-a").unwrap();
    assert_eq!(a.tasks_running, 5, "should be the latest snapshot for A");
    assert!(
        states.iter().any(|s| s.worker_id == "worker-b"),
        "idle worker-b must appear"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn test_get_worker_state_latest_and_unknown() {
    let pool = db::create_pool().await;
    db::run_migrations(&pool).await;
    db::clean_tables(&pool).await;
    let broker = horsies::PostgresBroker::from_pool(pool.clone());

    let t0 = chrono::Utc::now() - chrono::Duration::seconds(20);
    let t1 = chrono::Utc::now() - chrono::Duration::seconds(5);
    insert_snapshot(&pool, "worker-a", t0, 2, t0).await;
    insert_snapshot(&pool, "worker-a", t1, 7, t0).await;

    let latest = broker.get_worker_state("worker-a").await.unwrap();
    let latest = latest.expect("worker-a has snapshots");
    assert_eq!(latest.tasks_running, 7, "returns the newest snapshot");

    let unknown = broker.get_worker_state("does-not-exist").await.unwrap();
    assert!(unknown.is_none(), "unknown worker yields None");
}

#[tokio::test]
#[serial_test::serial]
async fn test_get_worker_state_history_newest_first_and_limit() {
    let pool = db::create_pool().await;
    db::run_migrations(&pool).await;
    db::clean_tables(&pool).await;
    let broker = horsies::PostgresBroker::from_pool(pool.clone());

    let base = chrono::Utc::now() - chrono::Duration::seconds(60);
    for i in 0..3i64 {
        let at = base + chrono::Duration::seconds(i * 10);
        insert_snapshot(&pool, "worker-a", at, i as i32, base).await;
    }

    // No limit returns all, newest first.
    let all = broker.get_worker_state_history("worker-a", None).await.unwrap();
    assert_eq!(all.len(), 3);
    assert!(
        all[0].snapshot_at > all[1].snapshot_at && all[1].snapshot_at > all[2].snapshot_at,
        "history must be ordered newest first"
    );
    assert_eq!(all[0].tasks_running, 2, "newest snapshot first");

    // Explicit limit bounds the fetch.
    let capped = broker
        .get_worker_state_history("worker-a", Some(2))
        .await
        .unwrap();
    assert_eq!(capped.len(), 2);
}
