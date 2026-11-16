use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// SQL: Insert a runner heartbeat for a single task.
const RUNNER_HEARTBEAT_SQL: &str = "\
INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
VALUES ($1, $2, 'runner', NOW(), $3, $4)";

/// SQL: Insert claimer heartbeats for all CLAIMED tasks owned by this worker.
const CLAIMER_HEARTBEAT_SQL: &str = "\
INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
SELECT id, $1::VARCHAR, 'claimer', NOW(), $2, $3
FROM horsies_tasks
WHERE status = 'CLAIMED' AND claimed_by_worker_id = $1::VARCHAR";

/// SQL: Renew claim leases for CLAIMED tasks owned by this worker.
///
/// Extends `claim_expires_at` to keep active claims from expiring while the
/// worker is alive. The `claimed_at` age guard ($3) prevents renewing claims
/// that are unreasonably old (matching Python's `max_claim_renew_age_ms`).
const RENEW_CLAIM_LEASE_SQL: &str = "\
UPDATE horsies_tasks
SET claim_expires_at = $2,
    updated_at = NOW()
WHERE status = 'CLAIMED'
  AND claimed_by_worker_id = $1
  AND claimed_at >= NOW() - $3 * INTERVAL '1 millisecond'";

/// Consecutive heartbeat failures before escalating the log to error level.
///
/// With a 30s heartbeat interval, 5 consecutive failures = 150s of degraded
/// heartbeating. Neither loop abandons at this point — it escalates once and
/// keeps retrying, so beats resume the instant connectivity recovers.
const MAX_CONSECUTIVE_HEARTBEAT_FAILURES: u32 = 5;

/// Spawn a runner heartbeat loop for a single task.
///
/// Repeats heartbeats at `interval` until the cancellation token fires. Transient
/// DB errors are logged and retried; the loop never abandons a live task — after
/// `MAX_CONSECUTIVE_HEARTBEAT_FAILURES` it escalates the log once and keeps
/// retrying (mirrors the claimer loop), because abandoning would let the reaper
/// reclaim a healthy task once its last beat ages past the stale threshold (C4).
///
/// The first beat is NOT sent here: it is written atomically with the CLAIMED →
/// RUNNING transition (`SET_RUNNING_SQL`), so a task is never observable RUNNING
/// without heartbeat coverage. This loop provides the ongoing beats. Parity with
/// horsies PR #134.
pub fn spawn_runner_heartbeat(
    pool: PgPool,
    task_id: Uuid,
    worker_id: String,
    hostname: String,
    pid: i32,
    interval: Duration,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut consecutive_failures: u32 = 0;
        let mut escalated = false;

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(interval) => {
                    match send_runner_heartbeat(&pool, task_id, &worker_id, &hostname, pid).await {
                        Ok(()) => {
                            if consecutive_failures > 0 {
                                tracing::info!(
                                    task_id = %task_id,
                                    previous_failures = consecutive_failures,
                                    "runner heartbeat recovered",
                                );
                            }
                            consecutive_failures = 0;
                            escalated = false;
                        }
                        Err(e) => {
                            consecutive_failures += 1;
                            tracing::warn!(
                                task_id = %task_id,
                                error = %e,
                                consecutive_failures,
                                max = MAX_CONSECUTIVE_HEARTBEAT_FAILURES,
                                "runner heartbeat failed",
                            );
                            if consecutive_failures >= MAX_CONSECUTIVE_HEARTBEAT_FAILURES
                                && !escalated
                            {
                                escalated = true;
                                tracing::error!(
                                    task_id = %task_id,
                                    consecutive_failures,
                                    "runner heartbeat degraded for too long; the reaper may reclaim this task until DB connectivity recovers",
                                );
                            }
                        }
                    }
                }
            }
        }
    })
}

/// Spawn a claimer heartbeat loop for all CLAIMED tasks owned by this worker.
///
/// In addition to inserting heartbeat rows, this also renews claim leases
/// (extending `claim_expires_at`) so the reaper does not reclaim tasks that
/// belong to a live worker. Matches Python's combined heartbeat + lease renewal.
///
/// `claim_lease_ms`: lease duration used to compute new `claim_expires_at`.
///     `None` means leases are not used and renewal is skipped.
/// `max_claim_renew_age_ms`: safety cap — claims older than this are NOT renewed.
pub fn spawn_claimer_heartbeat(
    pool: PgPool,
    worker_id: String,
    hostname: String,
    pid: i32,
    interval: Duration,
    claim_lease_ms: Option<u32>,
    max_claim_renew_age_ms: u32,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut consecutive_failures: u32 = 0;
        let mut escalated = false;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(interval) => {
                    let mut had_error = false;

                    if let Err(e) = send_claimer_heartbeat(&pool, &worker_id, &hostname, pid).await {
                        had_error = true;
                        tracing::warn!(
                            error = %e,
                            consecutive_failures = consecutive_failures + 1,
                            max = MAX_CONSECUTIVE_HEARTBEAT_FAILURES,
                            "claimer heartbeat failed",
                        );
                    }

                    // Renew claim leases if configured.
                    if let Some(lease_ms) = claim_lease_ms {
                        if let Err(e) = renew_claim_leases(&pool, &worker_id, lease_ms, max_claim_renew_age_ms).await {
                            had_error = true;
                            tracing::warn!(
                                error = %e,
                                consecutive_failures = consecutive_failures + 1,
                                max = MAX_CONSECUTIVE_HEARTBEAT_FAILURES,
                                "claim lease renewal failed",
                            );
                        }
                    }

                    if had_error {
                        consecutive_failures += 1;
                        if consecutive_failures >= MAX_CONSECUTIVE_HEARTBEAT_FAILURES && !escalated {
                            escalated = true;
                            tracing::error!(
                                consecutive_failures,
                                "claimer heartbeat degraded for too long; CLAIMED tasks may be requeued until DB connectivity recovers",
                            );
                        }
                    } else {
                        if consecutive_failures > 0 {
                            tracing::info!(
                                previous_failures = consecutive_failures,
                                "claimer heartbeat recovered",
                            );
                        }
                        consecutive_failures = 0;
                        escalated = false;
                    }
                }
            }
        }
    })
}

async fn send_runner_heartbeat(
    pool: &PgPool,
    task_id: Uuid,
    worker_id: &str,
    hostname: &str,
    pid: i32,
) -> Result<(), sqlx::Error> {
    sqlx::query(RUNNER_HEARTBEAT_SQL)
        .bind(task_id)
        .bind(worker_id)
        .bind(hostname)
        .bind(pid)
        .execute(pool)
        .await?;
    Ok(())
}

async fn send_claimer_heartbeat(
    pool: &PgPool,
    worker_id: &str,
    hostname: &str,
    pid: i32,
) -> Result<(), sqlx::Error> {
    sqlx::query(CLAIMER_HEARTBEAT_SQL)
        .bind(worker_id)
        .bind(hostname)
        .bind(pid)
        .execute(pool)
        .await?;
    Ok(())
}

/// Extend `claim_expires_at` for all CLAIMED tasks owned by this worker.
async fn renew_claim_leases(
    pool: &PgPool,
    worker_id: &str,
    claim_lease_ms: u32,
    max_claim_renew_age_ms: u32,
) -> Result<(), sqlx::Error> {
    let new_expires_at: DateTime<Utc> =
        Utc::now() + chrono::Duration::milliseconds(i64::from(claim_lease_ms));
    sqlx::query(RENEW_CLAIM_LEASE_SQL)
        .bind(worker_id)
        .bind(new_expires_at)
        .bind(i64::from(max_claim_renew_age_ms))
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db_url() -> String {
        if let Ok(url) = std::env::var("DATABASE_URL") {
            return url;
        }
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let root = std::path::Path::new(manifest_dir)
            .ancestors()
            .find(|p| p.join(".env").exists());
        let pw = root
            .and_then(|r| std::fs::read_to_string(r.join(".env")).ok())
            .and_then(|c| {
                c.lines()
                    .filter_map(|l| l.trim().split_once('='))
                    .find(|(k, _)| k.trim() == "DB_PASSWORD")
                    .map(|(_, v)| v.trim().to_owned())
            })
            .unwrap_or_else(|| "W0rklane".to_owned());
        format!("postgresql://postgres:{pw}@localhost:5432/horsies-rust-port")
    }

    /// C4: the runner heartbeat loop must keep retrying after
    /// `MAX_CONSECUTIVE_HEARTBEAT_FAILURES`, never abandoning a live task —
    /// otherwise the reaper reclaims a healthy task once its last beat ages out.
    /// A closed pool makes every beat fail instantly (`PoolClosed`); with a 5ms
    /// interval, 200ms is ~40 failures (>> 5), yet the loop must still be running.
    #[tokio::test]
    async fn runner_heartbeat_survives_many_consecutive_failures() {
        let pool = PgPool::connect(&test_db_url()).await.expect("connect");
        // Close the pool so every heartbeat fails immediately and the loop
        // actually iterates past the failure threshold within the test window.
        pool.close().await;

        let cancel = CancellationToken::new();
        let handle = spawn_runner_heartbeat(
            pool,
            Uuid::new_v4(),
            "worker-1".to_owned(),
            "host-1".to_owned(),
            123,
            Duration::from_millis(5),
            cancel.clone(),
        );

        // Far more than 5 failure intervals; before the fix the loop breaks at 5.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !handle.is_finished(),
            "runner heartbeat must keep running after >5 consecutive failures",
        );

        // The cancellation token still stops it cleanly.
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("runner heartbeat must exit promptly on cancel")
            .ok();
    }
}
