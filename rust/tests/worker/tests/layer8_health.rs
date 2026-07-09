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

/// Regression for the recursive-CTE skip-scan rewrite of
/// LIST_WORKER_STATES_SQL (parity with horsies PR #170): fresh, stale
/// (days-old snapshots only), and single-snapshot workers each appear
/// exactly once with their newest snapshot.
#[tokio::test]
#[serial_test::serial]
async fn test_list_worker_states_fresh_stale_and_single_snapshot() {
    let pool = db::create_pool().await;
    db::run_migrations(&pool).await;
    db::clean_tables(&pool).await;
    let broker = horsies::PostgresBroker::from_pool(pool.clone());

    let now = chrono::Utc::now();
    let days_ago = now - chrono::Duration::days(3);
    // Fresh worker: old + recent snapshots.
    insert_snapshot(&pool, "worker-fresh", now - chrono::Duration::seconds(30), 1, days_ago).await;
    insert_snapshot(&pool, "worker-fresh", now - chrono::Duration::seconds(5), 4, days_ago).await;
    // Stale worker: days-old snapshots only — no time-window filter, still listed.
    insert_snapshot(&pool, "worker-stale", days_ago, 2, days_ago).await;
    insert_snapshot(&pool, "worker-stale", days_ago + chrono::Duration::minutes(1), 3, days_ago).await;
    // Single-snapshot worker.
    insert_snapshot(&pool, "worker-single", now - chrono::Duration::seconds(10), 7, days_ago).await;

    let states = broker.list_worker_states().await.unwrap();
    assert_eq!(states.len(), 3, "each worker appears exactly once");

    let fresh = states.iter().find(|s| s.worker_id == "worker-fresh").unwrap();
    assert_eq!(fresh.tasks_running, 4, "newest snapshot for the fresh worker");
    let stale = states.iter().find(|s| s.worker_id == "worker-stale").unwrap();
    assert_eq!(stale.tasks_running, 3, "newest snapshot for the stale worker");
    let single = states.iter().find(|s| s.worker_id == "worker-single").unwrap();
    assert_eq!(single.tasks_running, 7, "single snapshot returned as-is");
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

// ---------------------------------------------------------------------------
// ping_workers (active LISTEN/NOTIFY liveness ping-pong)
// ---------------------------------------------------------------------------

fn write_worker_config() -> tempfile::NamedTempFile {
    use std::io::Write;
    let config = serde_json::json!({
        "queue_mode": "default",
        "broker": {
            "database_url": db::db_url(),
            "pool_pre_ping": true,
            "pool_size": 5,
            "max_overflow": 5,
            "pool_timeout": 10,
            "pool_recycle": 600,
            "echo": false
        },
        "prefetch_buffer": 0,
        "max_claim_renew_age_ms": 180000,
        "resend_on_transient_err": false
    });
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(serde_json::to_string_pretty(&config).unwrap().as_bytes())
        .unwrap();
    f.flush().unwrap();
    f
}

#[tokio::test]
#[serial_test::serial]
async fn test_ping_workers_rejects_zero_timeout() {
    let broker = broker().await;
    let err = broker
        .ping_workers(None, std::time::Duration::from_secs(0), None)
        .await
        .unwrap_err();
    assert_eq!(err.code, horsies::BrokerErrorCode::WorkerPingFailed);
    assert!(!err.retryable, "a bad-argument error is not retryable");
}

#[tokio::test]
#[serial_test::serial]
async fn test_ping_workers_rejects_zero_min_responses() {
    let broker = broker().await;
    let err = broker
        .ping_workers(None, std::time::Duration::from_secs(2), Some(0))
        .await
        .unwrap_err();
    assert_eq!(err.code, horsies::BrokerErrorCode::WorkerPingFailed);
    assert!(!err.retryable, "min_responses validation is not retryable");
    assert!(err.message.contains("min_responses"));
}

#[tokio::test]
#[serial_test::serial]
async fn test_ping_workers_collects_pong_and_targets_worker() {
    use horsies_test_support::e2e::worker::start_worker;

    let pool = db::create_pool().await;
    db::run_migrations(&pool).await;
    db::clean_tables(&pool).await;

    let config_file = write_worker_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();
    let _worker = start_worker(
        &config_path,
        &["--concurrency", "2"],
        "worker started",
        std::time::Duration::from_secs(20),
    );

    let broker = horsies::PostgresBroker::from_pool(pool.clone());

    // Broadcast: collect at least one pong from the live worker.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut pongs = Vec::new();
    while std::time::Instant::now() < deadline {
        pongs = broker
            .ping_workers(None, std::time::Duration::from_secs(2), None)
            .await
            .unwrap();
        if !pongs.is_empty() {
            break;
        }
    }
    assert!(
        !pongs.is_empty(),
        "expected at least one pong from the live worker"
    );
    let pong = pongs[0].clone();
    assert!(!pong.worker_id.is_empty(), "pong carries a worker id");
    assert!(!pong.hostname.is_empty(), "pong carries a hostname");
    assert!(pong.pid > 0, "pong carries a pid");
    assert!(pong.round_trip_ms >= 0.0, "round-trip latency is measured");

    // Targeted: only the addressed worker replies.
    let targeted = broker
        .ping_workers(Some(&pong.worker_id), std::time::Duration::from_secs(2), None)
        .await
        .unwrap();
    assert_eq!(targeted.len(), 1, "exactly the targeted worker replies");
    assert_eq!(targeted[0].worker_id, pong.worker_id);

    // A non-existent target yields no pongs within the window.
    let none = broker
        .ping_workers(
            Some("worker-that-does-not-exist"),
            std::time::Duration::from_millis(800),
            None,
        )
        .await
        .unwrap();
    assert!(none.is_empty(), "no worker matches a bogus target id");

    // min_responses=Some(1): fast fail-open gate returns well before the window.
    let window = std::time::Duration::from_secs(5);
    let start = std::time::Instant::now();
    let gated = broker.ping_workers(None, window, Some(1)).await.unwrap();
    let elapsed = start.elapsed();
    assert!(!gated.is_empty(), "fast gate should collect at least one pong");
    assert!(
        elapsed < std::time::Duration::from_secs(3),
        "min_responses=1 must return well before the {}s window, took {:?}",
        window.as_secs(),
        elapsed,
    );

    // Pongs are de-duplicated by worker_id even across the full window.
    let all = broker
        .ping_workers(None, std::time::Duration::from_secs(2), None)
        .await
        .unwrap();
    let mut ids: Vec<&str> = all.iter().map(|p| p.worker_id.as_str()).collect();
    let count = ids.len();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), count, "each worker_id appears at most once");
}
