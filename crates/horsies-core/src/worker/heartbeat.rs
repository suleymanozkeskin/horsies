use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

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

/// Consecutive claimer heartbeat failures before escalating the log to error
/// level. Runner heartbeats are written by `runner_beats`.
///
/// With a 30s heartbeat interval, 5 consecutive failures = 150s of degraded
/// heartbeating. Neither loop abandons at this point — it escalates once and
/// keeps retrying, so beats resume the instant connectivity recovers.
const MAX_CONSECUTIVE_HEARTBEAT_FAILURES: u32 = 5;

/// Spawn a claimer heartbeat loop for all CLAIMED tasks owned by this worker.
///
/// In addition to inserting heartbeat rows, this also renews claim leases
/// (extending `claim_expires_at`) so the reaper does not reclaim tasks that
/// belong to a live worker. Matches Python's combined heartbeat + lease renewal.
///
/// `claim_lease_ms`: lease duration used to compute new `claim_expires_at`.
///     Every claim has a lease (Python `claiming.py:697-708`), so every beat
///     renews.
/// `max_claim_renew_age_ms`: safety cap — claims older than this are NOT renewed.
///
/// The first beat runs at start, then one per `interval` (Python
/// `_claimer_heartbeat_loop`: beat, then wait).
pub fn spawn_claimer_heartbeat(
    pool: PgPool,
    worker_id: String,
    hostname: String,
    pid: i32,
    interval: Duration,
    claim_lease_ms: u32,
    max_claim_renew_age_ms: u32,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut consecutive_failures: u32 = 0;
        let mut escalated = false;
        while !cancel.is_cancelled() {
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

            // Renew the lease of every claim this worker holds.
            if let Err(e) =
                renew_claim_leases(&pool, &worker_id, claim_lease_ms, max_claim_renew_age_ms).await
            {
                had_error = true;
                tracing::warn!(
                    error = %e,
                    consecutive_failures = consecutive_failures + 1,
                    max = MAX_CONSECUTIVE_HEARTBEAT_FAILURES,
                    "claim lease renewal failed",
                );
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

            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(interval) => {}
            }
        }
    })
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
