use std::time::Duration;

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
/// worker is alive. The new expiry is the database time plus the lease, so it
/// does not move with the worker clock. The `claimed_at` age guard ($3)
/// prevents renewing claims that are unreasonably old. Python:
/// `RENEW_CLAIM_LEASE_SQL` (`core/worker/sql.py`).
const RENEW_CLAIM_LEASE_SQL: &str = "\
UPDATE horsies_tasks
SET claim_expires_at = NOW() + $2 * INTERVAL '1 millisecond',
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

            // The beats and the lease renewal of every claim this worker
            // holds, in one transaction.
            if let Err(e) = send_claimer_heartbeat(
                &pool,
                &worker_id,
                &hostname,
                pid,
                claim_lease_ms,
                max_claim_renew_age_ms,
            )
            .await
            {
                had_error = true;
                tracing::warn!(
                    error = %e,
                    consecutive_failures = consecutive_failures + 1,
                    max = MAX_CONSECUTIVE_HEARTBEAT_FAILURES,
                    "claimer heartbeat and lease renewal failed",
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

/// Write the claimer beats and renew the lease of every claim this worker
/// holds, in one transaction (Python `core/worker/health.py`: the beat
/// insert, then the lease update on the database clock with the age guard).
async fn send_claimer_heartbeat(
    pool: &PgPool,
    worker_id: &str,
    hostname: &str,
    pid: i32,
    claim_lease_ms: u32,
    max_claim_renew_age_ms: u32,
) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;
    sqlx::query(CLAIMER_HEARTBEAT_SQL)
        .bind(worker_id)
        .bind(hostname)
        .bind(pid)
        .execute(transaction.as_mut())
        .await?;
    sqlx::query(RENEW_CLAIM_LEASE_SQL)
        .bind(worker_id)
        .bind(i64::from(claim_lease_ms))
        .bind(i64::from(max_claim_renew_age_ms))
        .execute(transaction.as_mut())
        .await?;
    transaction.commit().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use serial_test::serial;
    use uuid::Uuid;

    const BEAT_WORKER: &str = "beat-worker";

    /// The shared test database, with heartbeat leaves for the current hours.
    async fn covered_pool() -> PgPool {
        use crate::core::history::heartbeats::partitioning::{
            ensure_heartbeat_coverage, EnsureHeartbeatCoverage,
        };
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let mut connection = pool.acquire().await.expect("acquire");
        ensure_heartbeat_coverage(
            &mut connection,
            &EnsureHeartbeatCoverage::new(2).expect("coverage command"),
        )
        .await
        .expect("heartbeat leaves");
        drop(connection);
        pool
    }

    /// Python `core/worker/health.py`: the claimer beat and the lease renewal
    /// run in one transaction, and the new lease is the database time plus
    /// the lease length.
    #[tokio::test]
    #[serial]
    async fn the_claimer_beat_renews_the_lease_on_the_database_clock() {
        let pool = covered_pool().await;
        let task_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, input, task_options, execution,
                retention_class_key, sent_at, created_at,
                command_fingerprint_version, command_fingerprint, idempotency_key_digest,
                status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
                started_at, finalizing_at, finalizing_by_worker_id,
                worker_pid, worker_hostname, worker_process_name,
                retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
                updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
             ) VALUES (
                $1, 'beat_task', 'default', 100, '{}', NULL, 'ISOLATED',
                'forever', NOW(), NOW(),
                1, decode(repeat('00', 32), 'hex'), NULL,
                'CLAIMED', NOW(), NOW() - interval '5 seconds', $2, NOW() + interval '1 second',
                NULL, NULL, NULL,
                NULL, NULL, NULL,
                0, 0, NULL, NULL, FALSE,
                NOW(), FALSE, NULL, NULL
             )",
        )
        .bind(task_id)
        .bind(BEAT_WORKER)
        .execute(&pool)
        .await
        .expect("seed CLAIMED task");

        let before: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&pool)
            .await
            .expect("clock");
        send_claimer_heartbeat(&pool, BEAT_WORKER, "h", 1, 60_000, 180_000)
            .await
            .expect("beat and renew");
        let after: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&pool)
            .await
            .expect("clock");

        let expires: DateTime<Utc> =
            sqlx::query_scalar("SELECT claim_expires_at FROM horsies_tasks WHERE id = $1")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .expect("lease");
        assert!(before + chrono::Duration::seconds(60) <= expires);
        assert!(expires <= after + chrono::Duration::seconds(60));
        let beats: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM horsies_heartbeats WHERE task_id = $1 AND role = 'claimer'",
        )
        .bind(task_id)
        .fetch_one(&pool)
        .await
        .expect("count beats");
        assert_eq!(beats, 1);

        sqlx::query("DELETE FROM horsies_heartbeats WHERE task_id = $1")
            .bind(task_id)
            .execute(&pool)
            .await
            .expect("delete beats");
        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .execute(&pool)
            .await
            .expect("delete task");
    }
}
