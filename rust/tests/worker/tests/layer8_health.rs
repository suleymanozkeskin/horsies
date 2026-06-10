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
