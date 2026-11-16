use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// SQL: Insert a runner heartbeat for a single task, fenced like every other
/// write of a dispatch: RUNNING, this worker, and the claim generation of the
/// dispatch. A row with the finalizing marker gets no beat: its attempt has
/// ended, and the reaper reads the marker for that row. A beat that matches
/// no row writes nothing, so a beat cannot keep a finished or re-claimed row
/// looking alive. Python: `INSERT_RUNNER_HEARTBEATS_SQL`
/// (`core/worker/runner_heartbeat.py`), for one dispatch.
const RUNNER_HEARTBEAT_SQL: &str = "\
INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
SELECT t.id, $2, 'runner', NOW(), $3, $4
FROM horsies_tasks t
WHERE t.id = $1
  AND t.status = 'RUNNING'
  AND t.claimed_by_worker_id = $2
  AND t.claimed_at = $5
  AND t.finalizing_at IS NULL";

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
///
/// Each beat is fenced on `claimed_at`, the claim generation of the dispatch
/// (see `RUNNER_HEARTBEAT_SQL`).
#[allow(clippy::too_many_arguments)]
pub fn spawn_runner_heartbeat(
    pool: PgPool,
    task_id: Uuid,
    worker_id: String,
    claimed_at: DateTime<Utc>,
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
                    match send_runner_heartbeat(&pool, task_id, &worker_id, claimed_at, &hostname, pid).await {
                        Ok(beat) => {
                            match beat {
                                RunnerBeat::Written => {}
                                RunnerBeat::NotHeld => tracing::debug!(
                                    task_id = %task_id,
                                    "runner heartbeat wrote nothing: the row is not RUNNING under this claim",
                                ),
                            }
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

/// What one runner beat wrote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunnerBeat {
    /// The row is RUNNING under this worker's claim generation with no
    /// finalizing marker; the beat is written.
    Written,
    /// The fence matched no row; nothing is written.
    NotHeld,
}

async fn send_runner_heartbeat(
    pool: &PgPool,
    task_id: Uuid,
    worker_id: &str,
    claimed_at: DateTime<Utc>,
    hostname: &str,
    pid: i32,
) -> Result<RunnerBeat, sqlx::Error> {
    let written = sqlx::query(RUNNER_HEARTBEAT_SQL)
        .bind(task_id)
        .bind(worker_id)
        .bind(hostname)
        .bind(pid)
        .bind(claimed_at)
        .execute(pool)
        .await?
        .rows_affected();
    match written {
        0 => Ok(RunnerBeat::NotHeld),
        _ => Ok(RunnerBeat::Written),
    }
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
        format!("postgresql://postgres:{pw}@localhost:5432/horsies-core")
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
            Utc::now(),
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

    // --- fences and clock (database) ---------------------------------------

    use serial_test::serial;

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

    /// A task of `BEAT_WORKER` in `status`, with every column of the baseline
    /// task row named. Returns the claim generation.
    async fn seed(pool: &PgPool, task_id: Uuid, status: &str) -> DateTime<Utc> {
        sqlx::query_scalar(
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
                $2, NOW(), NOW() - interval '5 seconds', $3,
                CASE WHEN $2 = 'CLAIMED' THEN NOW() + interval '1 second' END,
                CASE WHEN $2 = 'RUNNING' THEN NOW() END, NULL, NULL,
                NULL, NULL, NULL,
                0, 0, NULL, NULL, FALSE,
                NOW(), FALSE, NULL, NULL
             ) RETURNING claimed_at",
        )
        .bind(task_id)
        .bind(status)
        .bind(BEAT_WORKER)
        .fetch_one(pool)
        .await
        .expect("seed task")
    }

    async fn beats(pool: &PgPool, task_id: Uuid, role: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM horsies_heartbeats WHERE task_id = $1 AND role = $2",
        )
        .bind(task_id)
        .bind(role)
        .fetch_one(pool)
        .await
        .expect("count beats")
    }

    async fn remove(pool: &PgPool, task_id: Uuid) {
        sqlx::query("DELETE FROM horsies_heartbeats WHERE task_id = $1")
            .bind(task_id)
            .execute(pool)
            .await
            .expect("delete beats");
        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .execute(pool)
            .await
            .expect("delete task");
    }

    /// Python `tests/integration/test_runner_heartbeat_writer.py`: a beat is
    /// fenced like every write of a dispatch (RUNNING, this worker, this claim
    /// generation, no finalizing marker).
    #[tokio::test]
    #[serial]
    async fn a_runner_beat_is_fenced_on_the_claim_generation() {
        let pool = covered_pool().await;
        let task_id = Uuid::new_v4();
        let generation = seed(&pool, task_id, "RUNNING").await;

        let stale = generation - chrono::Duration::seconds(1);
        for (worker, claimed_at) in [(BEAT_WORKER, stale), ("other-worker", generation)] {
            assert_eq!(
                send_runner_heartbeat(&pool, task_id, worker, claimed_at, "h", 1)
                    .await
                    .expect("beat"),
                RunnerBeat::NotHeld
            );
        }
        assert_eq!(beats(&pool, task_id, "runner").await, 0);

        assert_eq!(
            send_runner_heartbeat(&pool, task_id, BEAT_WORKER, generation, "h", 1)
                .await
                .expect("beat"),
            RunnerBeat::Written
        );
        assert_eq!(beats(&pool, task_id, "runner").await, 1);

        sqlx::query(
            "UPDATE horsies_tasks SET finalizing_at = NOW(), finalizing_by_worker_id = $2
             WHERE id = $1",
        )
        .bind(task_id)
        .bind(BEAT_WORKER)
        .execute(&pool)
        .await
        .expect("mark finalizing");
        assert_eq!(
            send_runner_heartbeat(&pool, task_id, BEAT_WORKER, generation, "h", 1)
                .await
                .expect("beat"),
            RunnerBeat::NotHeld,
            "a row with the finalizing marker gets no beat"
        );
        assert_eq!(beats(&pool, task_id, "runner").await, 1);
        remove(&pool, task_id).await;
    }

    /// Python `core/worker/health.py`: the claimer beat and the lease renewal
    /// run in one transaction, and the new lease is the database time plus
    /// the lease length.
    #[tokio::test]
    #[serial]
    async fn the_claimer_beat_renews_the_lease_on_the_database_clock() {
        let pool = covered_pool().await;
        let task_id = Uuid::new_v4();
        let _generation = seed(&pool, task_id, "CLAIMED").await;

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
        assert_eq!(beats(&pool, task_id, "claimer").await, 1);
        remove(&pool, task_id).await;
    }
}
