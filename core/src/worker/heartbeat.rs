use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::broker::core_rows::execute_in;
use crate::broker::{BrokerError, PostgresBroker};
use crate::db::{
    CheckoutClass, IsolationLevel, NotSentCause, ParameterType, StatementFailure, TypedStatement,
};

/// SQL: Insert claimer heartbeats for all CLAIMED tasks owned by this worker.
const CLAIMER_HEARTBEAT_SQL: &str = "\
INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
SELECT id, $1::VARCHAR, 'claimer', NOW(), $2, $3
FROM horsies_tasks
WHERE status = 'CLAIMED' AND claimed_by_worker_id = $1::VARCHAR";

/// [`CLAIMER_HEARTBEAT_SQL`] with its parameter types: worker id, hostname,
/// pid.
static CLAIMER_HEARTBEAT: TypedStatement = TypedStatement::new(
    "claimer_heartbeat",
    CLAIMER_HEARTBEAT_SQL,
    &[
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Int4,
    ],
);

/// SQL: Renew claim leases for CLAIMED tasks owned by this worker.
///
/// Extends `claim_expires_at` to keep active claims from expiring while the
/// worker is alive. The new expiry is the database time plus the lease, so it
/// does not move with the worker clock. The `claimed_at` age guard ($3)
/// prevents renewing claims that are unreasonably old.
const RENEW_CLAIM_LEASE_SQL: &str = "\
UPDATE horsies_tasks
SET claim_expires_at = NOW() + $2 * INTERVAL '1 millisecond',
    updated_at = NOW()
WHERE status = 'CLAIMED'
  AND claimed_by_worker_id = $1
  AND claimed_at >= NOW() - $3 * INTERVAL '1 millisecond'";

/// [`RENEW_CLAIM_LEASE_SQL`] with its parameter types: worker id, lease in
/// milliseconds, renew age limit in milliseconds.
static RENEW_CLAIM_LEASE: TypedStatement = TypedStatement::new(
    "renew_claim_lease",
    RENEW_CLAIM_LEASE_SQL,
    &[
        ParameterType::Text,
        ParameterType::Int8,
        ParameterType::Int8,
    ],
);

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
/// belong to a live worker.
///
/// `claim_lease_ms`: lease duration used to compute new `claim_expires_at`.
///     Every claim has a lease, so every beat renews.
/// `max_claim_renew_age_ms`: safety cap — claims older than this are NOT renewed.
///
/// The first beat runs at start, then one per `interval` (Python
/// `_claimer_heartbeat_loop`: beat, then wait). Each beat runs on the main
/// core pool of `broker` (the worker profile pool), on the reserve: Python's
/// claim loop and flushers leave `PARENT_POOL_RESERVE` connections free for
/// every other write, the beat among them.
pub fn spawn_claimer_heartbeat(
    broker: Arc<PostgresBroker>,
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
                &broker,
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
                    "Claimer heartbeat error: {e}",
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
/// holds, in one transaction on the main core pool of `broker`
/// ([`CheckoutClass::Reserved`]): the beat insert, then the lease update on
/// the database clock with the age guard.
///
/// Errors: [`BrokerError::PoolSettings`] when a closed handle cannot open
/// its pools again; [`BrokerError::Statement`] for a failed checkout,
/// statement or `COMMIT`. After an error nothing committed, except after an
/// `OutcomeUnknown` of the `COMMIT`, when both writes may have committed.
async fn send_claimer_heartbeat(
    broker: &PostgresBroker,
    worker_id: &str,
    hostname: &str,
    pid: i32,
    claim_lease_ms: u32,
    max_claim_renew_age_ms: u32,
) -> Result<(), BrokerError> {
    let (open, _write) = broker.core_pools_for_write()?;
    let log = broker.statement_log();
    let mut connection = open
        .main()
        .checkout(CheckoutClass::Reserved)
        .await
        .map_err(|cause| StatementFailure::NotSent {
            statement: CLAIMER_HEARTBEAT.name(),
            cause: NotSentCause::Checkout(cause),
        })?;
    let mut transaction = connection.begin(IsolationLevel::ServerDefault).await?;
    let beat = CLAIMER_HEARTBEAT
        .bind()
        .value(&worker_id)
        .value(&hostname)
        .value(&pid)
        .finish();
    let _beats = execute_in(&mut transaction, log, &beat).await?;
    let lease_ms = i64::from(claim_lease_ms);
    let renew_age_ms = i64::from(max_claim_renew_age_ms);
    let renew = RENEW_CLAIM_LEASE
        .bind()
        .value(&worker_id)
        .value(&lease_ms)
        .value(&renew_age_ms)
        .finish();
    let _renewed = execute_in(&mut transaction, log, &renew).await?;
    transaction.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::sql::{self, TestPool};
    use chrono::{DateTime, Utc};
    use uuid::Uuid;

    const BEAT_WORKER: &str = "beat-worker";

    /// The shared test database, with heartbeat leaves for the current hours.
    async fn covered_pool() -> TestPool {
        use crate::core::history::maintenance::coverage::{
            ensure_partition_coverage_in_pool, CoverageOutcome,
        };
        let pool = crate::test_support::migrated_pool().await;
        let coverage = ensure_partition_coverage_in_pool(
            &crate::broker::pools::core_pool_for_test_url(pool.url()),
            crate::broker::core_rows::StatementLog::Off,
            2,
            2,
            &[],
        )
        .await
        .expect("heartbeat leaves");
        assert!(
            matches!(coverage, CoverageOutcome::Ensured(_)),
            "{coverage:?}"
        );
        pool
    }

    /// The claimer beat and the lease renewal run in one transaction, and the
    /// new lease is the database time plus the lease length.
    #[tokio::test]
    async fn the_claimer_beat_renews_the_lease_on_the_database_clock() {
        let pool = covered_pool().await;
        let task_id = Uuid::new_v4();
        sql::query(
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

        let before: DateTime<Utc> = sql::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&pool)
            .await
            .expect("clock");
        let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
        send_claimer_heartbeat(&broker, BEAT_WORKER, "h", 1, 60_000, 180_000)
            .await
            .expect("beat and renew");
        let after: DateTime<Utc> = sql::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&pool)
            .await
            .expect("clock");

        let expires: DateTime<Utc> =
            sql::query_scalar("SELECT claim_expires_at FROM horsies_tasks WHERE id = $1")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .expect("lease");
        assert!(before + chrono::Duration::seconds(60) <= expires);
        assert!(expires <= after + chrono::Duration::seconds(60));
        let beats: i64 = sql::query_scalar(
            "SELECT COUNT(*) FROM horsies_heartbeats WHERE task_id = $1 AND role = 'claimer'",
        )
        .bind(task_id)
        .fetch_one(&pool)
        .await
        .expect("count beats");
        assert_eq!(beats, 1);

        sql::query("DELETE FROM horsies_heartbeats WHERE task_id = $1")
            .bind(task_id)
            .execute(&pool)
            .await
            .expect("delete beats");
        sql::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .execute(&pool)
            .await
            .expect("delete task");
    }

    /// Seconds from the database clock to the lease of `task_id`.
    async fn lease_left_s(pool: &TestPool, task_id: Uuid) -> f64 {
        sql::query_scalar(
            "SELECT EXTRACT(EPOCH FROM claim_expires_at - NOW())::float8
             FROM horsies_tasks WHERE id = $1",
        )
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("lease")
    }

    /// Python `test_worker_helpers.py::TestLeaseRenewalAgeGuard`: the claimer
    /// loop renews the lease of a claim of its own worker that is younger
    /// than `max_claim_renew_age_ms`; a claim older than that, and a claim of
    /// another worker, keep their lease.
    #[tokio::test]
    async fn the_claimer_loop_renews_only_young_claims_of_its_own_worker() {
        let pool = covered_pool().await;
        let young = Uuid::new_v4();
        let old = Uuid::new_v4();
        let other = Uuid::new_v4();
        let claims: [(Uuid, &str, f64); 3] = [
            (young, BEAT_WORKER, 5.0),
            (old, BEAT_WORKER, 7_200.0),
            (other, "other-worker", 5.0),
        ];
        for (task_id, owner, age_s) in claims {
            sql::query(
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
                    'CLAIMED', NOW(), NOW() - $3 * interval '1 second', $2,
                    NOW() + interval '1 second',
                    NULL, NULL, NULL,
                    NULL, NULL, NULL,
                    0, 0, NULL, NULL, FALSE,
                    NOW(), FALSE, NULL, NULL
                 )",
            )
            .bind(task_id)
            .bind(owner)
            .bind(age_s)
            .execute(&pool)
            .await
            .expect("seed CLAIMED task");
        }

        let cancel = CancellationToken::new();
        let beats = spawn_claimer_heartbeat(
            Arc::new(PostgresBroker::from_test_url(
                pool.url(),
                crate::ClientFamily::Rust,
            )),
            BEAT_WORKER.to_owned(),
            "h".to_owned(),
            1,
            Duration::from_secs(3_600),
            600_000,
            3_600_000,
            cancel.clone(),
        );
        // The first beat runs at start; wait until it renewed the young claim.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while lease_left_s(&pool, young).await < 300.0 {
            assert!(
                std::time::Instant::now() < deadline,
                "the young claim was not renewed in time"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        cancel.cancel();
        beats.await.expect("the claimer loop ends at its cancel");

        assert!(
            lease_left_s(&pool, old).await < 60.0,
            "an old claim kept its lease"
        );
        assert!(
            lease_left_s(&pool, other).await < 60.0,
            "a claim of another worker kept its lease"
        );

        let ids = vec![young, old, other];
        sql::query("DELETE FROM horsies_heartbeats WHERE task_id = ANY($1)")
            .bind(&ids)
            .execute(&pool)
            .await
            .expect("delete beats");
        sql::query("DELETE FROM horsies_tasks WHERE id = ANY($1)")
            .bind(&ids)
            .execute(&pool)
            .await
            .expect("delete tasks");
    }
}
