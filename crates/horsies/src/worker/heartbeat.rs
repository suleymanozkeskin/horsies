use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

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

/// Maximum consecutive heartbeat failures before abandoning the loop.
///
/// With a 30s heartbeat interval, 5 consecutive failures = 150s of dead
/// time. The default stale threshold is 300s, leaving headroom for
/// transient DB issues to resolve before the reaper acts.
const MAX_CONSECUTIVE_HEARTBEAT_FAILURES: u32 = 5;

/// Spawn a runner heartbeat loop for a single task.
///
/// Sends an immediate best-effort heartbeat, then repeats at `interval`
/// until the cancellation token fires or consecutive failures exceed the
/// internal threshold. Transient DB errors are logged and retried —
/// a single failure does not kill heartbeating.
pub fn spawn_runner_heartbeat(
    pool: PgPool,
    task_id: String,
    worker_id: String,
    hostname: String,
    pid: i32,
    interval: Duration,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut consecutive_failures: u32 = 0;

        // Immediate best-effort heartbeat so a freshly-RUNNING task is not stale.
        // If it fails, the loop still starts — the next attempt in `interval` can
        // succeed, and the stale threshold provides headroom.
        if let Err(e) = send_runner_heartbeat(&pool, &task_id, &worker_id, &hostname, pid).await {
            tracing::warn!(
                task_id = %task_id,
                error = %e,
                "initial runner heartbeat failed, will retry in loop",
            );
            consecutive_failures = 1;
        }

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(interval) => {
                    match send_runner_heartbeat(&pool, &task_id, &worker_id, &hostname, pid).await {
                        Ok(()) => {
                            if consecutive_failures > 0 {
                                tracing::info!(
                                    task_id = %task_id,
                                    previous_failures = consecutive_failures,
                                    "runner heartbeat recovered",
                                );
                            }
                            consecutive_failures = 0;
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
                            if consecutive_failures >= MAX_CONSECUTIVE_HEARTBEAT_FAILURES {
                                tracing::error!(
                                    task_id = %task_id,
                                    consecutive_failures,
                                    "runner heartbeat abandoned after too many consecutive failures",
                                );
                                break;
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
    task_id: &str,
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
