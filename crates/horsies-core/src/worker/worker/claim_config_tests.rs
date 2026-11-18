//! The claim pass of a worker from its config, on the test database: the
//! queue order, the caps and the advisory keys that guard their counts.
//! Ports of Python `tests/integration/test_claim_function.py`,
//! `test_claim_lock_scoping.py` and `test_claim_cap_expired.py`.
//!
//! Each claimer builds its pass as `Worker::claim_request` does
//! (`compute_ordered_queues`, `compute_claim_lock_keys`, the priority 100
//! of a queue with none, hard-cap mode) with one isolated budget of
//! `processes` slots that every class routes to, and runs it with
//! `claim_pass` on a core pool of its own. Each test runs on the database
//! of its own runtime.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::time::Duration;

use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    advisory_key_global, advisory_key_queue, compute_claim_lock_keys, compute_ordered_queues,
};
use crate::broker::claims::{claim_pass, ClaimClasses, ClaimPass, ClaimPassRequest};
use crate::broker::core_rows::StatementLog;
use crate::broker::pools::core_pool_for_test_pool;
use crate::core::task::Execution;
use crate::db::CorePool;

/// The passes of each concurrent claimer, and the pause between them
/// (Python's `for _ in range(20)` and `asyncio.sleep(0.005)`).
const CONCURRENT_PASSES: u32 = 20;
const PASS_PAUSE: Duration = Duration::from_millis(5);
const CONCURRENT_CLAIMERS: usize = 6;
/// The bound of a pass that no lock holds back (Python `_PASS_TIMEOUT_S`).
const PASS_TIMEOUT: Duration = Duration::from_secs(3);
/// The wait that proves a pass is held back (Python `_BLOCK_PROOF_TIMEOUT_S`).
const BLOCK_PROOF: Duration = Duration::from_secs(1);

/// The claim settings of one worker (Python `_probe` and `WorkerConfig`).
#[derive(Debug, Clone, Default)]
struct ClaimerConfig {
    queues: Vec<String>,
    processes: u32,
    queue_priorities: HashMap<String, i32>,
    queue_max_concurrency: HashMap<String, u32>,
    cluster_wide_cap: Option<u32>,
    max_claim_per_worker: u32,
    max_claim_batch: u32,
}

fn names(queues: &[&str]) -> Vec<String> {
    queues.iter().map(|queue| (*queue).to_owned()).collect()
}

fn priorities(pairs: &[(&str, i32)]) -> HashMap<String, i32> {
    pairs
        .iter()
        .map(|(queue, priority)| ((*queue).to_owned(), *priority))
        .collect()
}

fn caps(pairs: &[(&str, u32)]) -> HashMap<String, u32> {
    pairs
        .iter()
        .map(|(queue, cap)| ((*queue).to_owned(), *cap))
        .collect()
}

/// The pass of `config`, as `Worker::claim_request` builds it.
fn request(config: &ClaimerConfig, worker_id: &str) -> ClaimPassRequest {
    let ordered = compute_ordered_queues(
        &config.queues,
        &config.queue_priorities,
        &config.queue_max_concurrency,
    );
    let queue_priority = ordered
        .iter()
        .map(|queue| {
            let priority = config.queue_priorities.get(queue).copied().unwrap_or(100);
            (queue.clone(), priority)
        })
        .collect();
    let queue_max_concurrency = ordered
        .iter()
        .filter_map(|queue| {
            config
                .queue_max_concurrency
                .get(queue)
                .map(|&cap| (queue.clone(), cap))
        })
        .collect();
    ClaimPassRequest {
        worker_id: worker_id.to_owned(),
        lock_keys: compute_claim_lock_keys(
            config.cluster_wide_cap,
            &ordered,
            &config.queue_max_concurrency,
        ),
        queues: ordered,
        queue_priority,
        queue_max_concurrency,
        hard_cap_mode: true,
        classes: ClaimClasses::new(
            HashMap::from([(Execution::Isolated, config.processes)]),
            HashMap::from([
                (Execution::Isolated, Execution::Isolated),
                (Execution::SharedAsync, Execution::Isolated),
                (Execution::SharedThread, Execution::Isolated),
            ]),
        )
        .expect("every route names the ISOLATED budget"),
        prefetch_buffer: 0,
        max_claim_per_worker: config.max_claim_per_worker,
        max_claim_batch: config.max_claim_batch,
        cluster_wide_cap: config.cluster_wide_cap,
        claim_lease_ms: NonZeroU32::new(60_000).expect("positive lease"),
    }
}

fn worker_id() -> String {
    format!("probe-{}", &Uuid::new_v4().simple().to_string()[..8])
}

async fn pass(core_pool: &CorePool, request: &ClaimPassRequest) -> ClaimPass {
    claim_pass(core_pool, StatementLog::Off, request)
        .await
        .expect("one claim pass")
}

/// One PENDING ISOLATED row of `queue` with `priority`, enqueued
/// `age_secs` ago. Returns its id.
async fn seed(pool: &PgPool, queue: &str, priority: i32, age_secs: f64) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, status, enqueued_at,
            is_workflow_task, created_at, updated_at, retry_count, max_retries,
            retention_class_key, command_fingerprint_version, command_fingerprint,
            retain_rerun_input, execution, sent_at
        ) VALUES (
            $1, 'bench', $2, $3, '{}', 'PENDING', now() - ($4 * interval '1 second'),
            FALSE, now(), now(), 0, 0, 'forever', 1,
            sha256(convert_to(CAST($1 AS text), 'UTF8')), FALSE, 'ISOLATED', now()
        )",
    )
    .bind(id)
    .bind(queue)
    .bind(priority)
    .bind(age_secs)
    .execute(pool)
    .await
    .expect("seed one pending row");
    id
}

/// `count` PENDING ISOLATED rows of `queue` with `priority`.
async fn seed_n(pool: &PgPool, queue: &str, priority: i32, count: i32) {
    sqlx::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, status, enqueued_at,
            is_workflow_task, created_at, updated_at, retry_count, max_retries,
            retention_class_key, command_fingerprint_version, command_fingerprint,
            retain_rerun_input, execution, sent_at
        )
        SELECT g.id, 'bench', $1, $2, '{}', 'PENDING', now(), FALSE, now(), now(), 0, 0,
               'forever', 1, sha256(convert_to(CAST(g.id AS text), 'UTF8')), FALSE,
               'ISOLATED', now()
        FROM (SELECT gen_random_uuid() AS id FROM generate_series(1, $3)) AS g",
    )
    .bind(queue)
    .bind(priority)
    .bind(count)
    .execute(pool)
    .await
    .expect("seed pending rows");
}

/// CLAIMED rows per queue.
async fn claimed_per_queue(pool: &PgPool) -> HashMap<String, i64> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT queue_name, count(*) FROM horsies_tasks
         WHERE status = 'CLAIMED' GROUP BY queue_name",
    )
    .fetch_all(pool)
    .await
    .expect("count the claimed rows");
    rows.into_iter().collect()
}

/// `CONCURRENT_CLAIMERS` claimers of `config`, each with a core pool of
/// its own, each running `CONCURRENT_PASSES` passes.
async fn claim_concurrently(pool: &PgPool, config: &ClaimerConfig) {
    let claimers: Vec<(CorePool, ClaimPassRequest)> = (0..CONCURRENT_CLAIMERS)
        .map(|_| (core_pool_for_test_pool(pool), request(config, &worker_id())))
        .collect();
    let loops = claimers.iter().map(|(core_pool, request)| async move {
        for _ in 0..CONCURRENT_PASSES {
            let _pass = pass(core_pool, request).await;
            tokio::time::sleep(PASS_PAUSE).await;
        }
    });
    futures::future::join_all(loops).await;
}

mod claim_function {
    use super::*;

    /// `test_equal_queue_priority_fifo`: equal queue priority and equal task
    /// priority: the older enqueued task wins, whatever its queue name.
    #[tokio::test]
    #[serial]
    async fn equal_queue_priority_fifo() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let older = seed(&pool, "q2", 100, 30.0).await;
        seed(&pool, "q1", 100, 5.0).await;
        let config = ClaimerConfig {
            queues: names(&["q1", "q2"]),
            processes: 1,
            queue_priorities: priorities(&[("q1", 100), ("q2", 100)]),
            ..ClaimerConfig::default()
        };

        let claimed = pass(
            &core_pool_for_test_pool(&pool),
            &request(&config, &worker_id()),
        )
        .await;

        let ids: Vec<Uuid> = claimed.claimed.iter().map(|row| row.id).collect();
        assert_eq!(ids, vec![older]);
    }

    /// `test_distinct_queue_priority_wins`: with a budget of one, the task of
    /// the queue with the better priority is claimed, though the task of the
    /// other queue is older.
    #[tokio::test]
    #[serial]
    async fn distinct_queue_priority_wins() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let high = seed(&pool, "q_hi", 1, 0.0).await;
        seed(&pool, "q_lo", 10, 10.0).await;
        let config = ClaimerConfig {
            queues: names(&["q_hi", "q_lo"]),
            processes: 1,
            queue_priorities: priorities(&[("q_hi", 1), ("q_lo", 10)]),
            ..ClaimerConfig::default()
        };

        let claimed = pass(
            &core_pool_for_test_pool(&pool),
            &request(&config, &worker_id()),
        )
        .await;

        let rows: Vec<(Uuid, &str)> = claimed
            .claimed
            .iter()
            .map(|row| (row.id, row.queue_name.as_str()))
            .collect();
        assert_eq!(rows, vec![(high, "q_hi")]);
    }

    /// `test_equal_queue_priority_explicit_task_priority_wins`: with equal
    /// queue priorities, a task of a better task priority (a workflow node,
    /// for example) is claimed before an older task of the default priority.
    #[tokio::test]
    #[serial]
    async fn equal_queue_priority_explicit_task_priority_wins() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        seed(&pool, "q1", 100, 30.0).await;
        let urgent = seed(&pool, "q2", 1, 5.0).await;
        let config = ClaimerConfig {
            queues: names(&["q1", "q2"]),
            processes: 1,
            queue_priorities: priorities(&[("q1", 100), ("q2", 100)]),
            ..ClaimerConfig::default()
        };

        let claimed = pass(
            &core_pool_for_test_pool(&pool),
            &request(&config, &worker_id()),
        )
        .await;

        let ids: Vec<Uuid> = claimed.claimed.iter().map(|row| row.id).collect();
        assert_eq!(ids, vec![urgent]);
    }

    /// `test_claim_candidate_bounds.py::test_claim_returns_rows_in_dispatch_order`:
    /// the rows come back in dispatch order across the pending and the
    /// expired-claim arms: queue priority first, then task priority, then
    /// age. The rows are seeded in the opposite order of that rank.
    #[tokio::test]
    #[serial]
    async fn claim_returns_rows_in_dispatch_order() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let low_queue = seed(&pool, "bounds_low", 1, 50.0).await;
        let late = seed(&pool, "bounds_high", 50, 1.0).await;
        let early = seed(&pool, "bounds_high", 50, 30.0).await;
        let expired_urgent = seed(&pool, "bounds_high", 10, 5.0).await;
        sqlx::query(
            "UPDATE horsies_tasks
             SET status = 'CLAIMED', claimed_at = now() - interval '10 minutes',
                 claimed_by_worker_id = 'dead-worker',
                 claim_expires_at = now() - interval '5 minutes', updated_at = now()
             WHERE id = $1",
        )
        .bind(expired_urgent)
        .execute(&pool)
        .await
        .expect("expire the seeded claim");
        let config = ClaimerConfig {
            queues: names(&["bounds_low", "bounds_high"]),
            processes: 4,
            queue_priorities: priorities(&[("bounds_high", 1), ("bounds_low", 2)]),
            ..ClaimerConfig::default()
        };

        let claimed = pass(
            &core_pool_for_test_pool(&pool),
            &request(&config, &worker_id()),
        )
        .await;

        let ids: Vec<Uuid> = claimed.claimed.iter().map(|row| row.id).collect();
        assert_eq!(ids, vec![expired_urgent, early, late, low_queue]);
    }

    /// `test_per_queue_cap_never_over_claims`: concurrent claimers never
    /// exceed the `max_concurrency` of a queue in hard-cap mode.
    #[tokio::test]
    #[serial]
    async fn per_queue_cap_never_over_claims() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        seed_n(&pool, "a", 50, 300).await;
        seed_n(&pool, "b", 50, 300).await;
        let config = ClaimerConfig {
            queues: names(&["a", "b"]),
            processes: 100,
            queue_priorities: priorities(&[("a", 1), ("b", 2)]),
            queue_max_concurrency: caps(&[("a", 3), ("b", 5)]),
            ..ClaimerConfig::default()
        };

        claim_concurrently(&pool, &config).await;

        let got = claimed_per_queue(&pool).await;
        assert!(got.get("a").copied().unwrap_or(0) <= 3, "{got:?}");
        assert!(got.get("b").copied().unwrap_or(0) <= 5, "{got:?}");
    }

    /// `test_cluster_cap_never_over_claims`: concurrent claimers never
    /// exceed `cluster_wide_cap`.
    #[tokio::test]
    #[serial]
    async fn cluster_cap_never_over_claims() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        seed_n(&pool, "a", 50, 300).await;
        seed_n(&pool, "b", 50, 300).await;
        let config = ClaimerConfig {
            queues: names(&["a", "b"]),
            processes: 100,
            queue_priorities: priorities(&[("a", 1), ("b", 2)]),
            cluster_wide_cap: Some(7),
            ..ClaimerConfig::default()
        };

        claim_concurrently(&pool, &config).await;

        let total: i64 = claimed_per_queue(&pool).await.values().sum();
        assert!(total <= 7, "{total}");
    }

    /// `test_cap_only_queue_is_serviced_and_capped`: a queue with
    /// `max_concurrency` and no priorities is serviced, and claimed only up
    /// to its cap.
    #[tokio::test]
    #[serial]
    async fn cap_only_queue_is_serviced_and_capped() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        seed_n(&pool, "capped", 50, 20).await;
        let config = ClaimerConfig {
            queues: names(&["capped"]),
            processes: 100,
            queue_max_concurrency: caps(&[("capped", 4)]),
            ..ClaimerConfig::default()
        };
        let core_pool = core_pool_for_test_pool(&pool);
        let request = request(&config, &worker_id());

        for _ in 0..5 {
            let _pass = pass(&core_pool, &request).await;
        }

        let claimed: i64 = claimed_per_queue(&pool).await.values().sum();
        assert!(claimed > 0, "the cap-only queue was never serviced");
        assert_eq!(claimed, 4);
    }

    /// `test_cap_without_priorities_never_over_claims`: concurrent claimers
    /// keep the caps of their queues with no priorities set.
    #[tokio::test]
    #[serial]
    async fn cap_without_priorities_never_over_claims() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        seed_n(&pool, "a", 50, 300).await;
        seed_n(&pool, "b", 50, 300).await;
        let config = ClaimerConfig {
            queues: names(&["a", "b"]),
            processes: 100,
            queue_max_concurrency: caps(&[("a", 3), ("b", 5)]),
            ..ClaimerConfig::default()
        };

        claim_concurrently(&pool, &config).await;

        let got = claimed_per_queue(&pool).await;
        assert!(got.get("a").copied().unwrap_or(0) <= 3, "{got:?}");
        assert!(got.get("b").copied().unwrap_or(0) <= 5, "{got:?}");
    }
}

mod claim_lock_scoping {
    use super::*;

    /// A transaction on `pool` that holds the advisory lock `key` until it
    /// ends.
    async fn hold(pool: &PgPool, key: i64) -> sqlx::Transaction<'static, sqlx::Postgres> {
        let mut holder = pool.begin().await.expect("begin the holder");
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(key)
            .execute(&mut *holder)
            .await
            .expect("take the advisory lock");
        holder
    }

    /// `test_disjoint_capped_queue_does_not_block`: the lock of capped queue
    /// A held elsewhere does not hold back a pass over capped queue B.
    #[tokio::test]
    #[serial]
    async fn disjoint_capped_queue_does_not_block() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let passer = ClaimerConfig {
            queues: names(&["lock_qb"]),
            processes: 1,
            queue_max_concurrency: caps(&[("lock_qb", 1)]),
            ..ClaimerConfig::default()
        };
        let holder = hold(&pool, advisory_key_queue("lock_qa")).await;
        let core_pool = core_pool_for_test_pool(&pool);

        let ended = tokio::time::timeout(
            PASS_TIMEOUT,
            claim_pass(
                &core_pool,
                StatementLog::Off,
                &request(&passer, &worker_id()),
            ),
        )
        .await;
        holder.rollback().await.expect("end the holder");

        assert!(
            matches!(ended, Ok(Ok(_))),
            "the pass over lock_qb waited on the lock of lock_qa: {ended:?}"
        );
    }

    /// `test_same_capped_queue_blocks`: the lock of the same capped queue
    /// held elsewhere holds back the pass until it ends.
    #[tokio::test]
    #[serial]
    async fn same_capped_queue_blocks() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let passer = ClaimerConfig {
            queues: names(&["lock_qc"]),
            processes: 1,
            queue_max_concurrency: caps(&[("lock_qc", 1)]),
            ..ClaimerConfig::default()
        };
        let holder = hold(&pool, advisory_key_queue("lock_qc")).await;
        let core_pool = core_pool_for_test_pool(&pool);
        let pass_request = request(&passer, &worker_id());
        let blocked = claim_pass(&core_pool, StatementLog::Off, &pass_request);
        tokio::pin!(blocked);

        let while_held = tokio::time::timeout(BLOCK_PROOF, &mut blocked).await;
        holder.rollback().await.expect("end the holder");
        let after = tokio::time::timeout(PASS_TIMEOUT, &mut blocked).await;

        assert!(while_held.is_err(), "the pass ran while the lock was held");
        assert!(matches!(after, Ok(Ok(_))), "{after:?}");
    }

    /// `test_global_cap_still_blocks_across_queues`: with
    /// `cluster_wide_cap` the pass takes the one global key, so a pass over
    /// any queue waits while that key is held.
    #[tokio::test]
    #[serial]
    async fn global_cap_still_blocks_across_queues() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let passer = ClaimerConfig {
            queues: names(&["lock_qd"]),
            processes: 1,
            cluster_wide_cap: Some(5),
            ..ClaimerConfig::default()
        };
        let holder = hold(&pool, advisory_key_global()).await;
        let core_pool = core_pool_for_test_pool(&pool);
        let pass_request = request(&passer, &worker_id());
        let blocked = claim_pass(&core_pool, StatementLog::Off, &pass_request);
        tokio::pin!(blocked);

        let while_held = tokio::time::timeout(BLOCK_PROOF, &mut blocked).await;
        holder.rollback().await.expect("end the holder");
        let after = tokio::time::timeout(PASS_TIMEOUT, &mut blocked).await;

        assert!(
            while_held.is_err(),
            "the pass ran while the global key was held"
        );
        assert!(matches!(after, Ok(Ok(_))), "{after:?}");
    }
}

mod claim_cap_expired {
    use super::*;

    /// `TestExpiredClaimsExcludedFromCap::test_expired_claim_not_counted_in_queue_hard_cap`:
    /// the per-queue count of the claim leaves out a CLAIMED row whose lease
    /// expired; a live claim of each queue counts.
    #[tokio::test]
    #[serial]
    async fn an_expired_claim_is_not_counted_in_the_queue_hard_cap() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let expired_default = seed(&pool, "default", 100, 0.0).await;
        let active_default = seed(&pool, "default", 100, 0.0).await;
        let active_critical = seed(&pool, "critical", 100, 0.0).await;
        for (id, worker, lease) in [
            (expired_default, "dead-worker", "- interval '5 minutes'"),
            (
                active_default,
                "live-worker-default",
                "+ interval '5 minutes'",
            ),
            (
                active_critical,
                "live-worker-critical",
                "+ interval '5 minutes'",
            ),
        ] {
            sqlx::query(&format!(
                "UPDATE horsies_tasks
                 SET status = 'CLAIMED', claimed_at = now() - interval '10 minutes',
                     claimed_by_worker_id = $2, claim_expires_at = now() {lease},
                     updated_at = now()
                 WHERE id = $1"
            ))
            .bind(id)
            .bind(worker)
            .execute(&pool)
            .await
            .expect("claim the seeded row");
        }

        let counts: serde_json::Value = sqlx::query_scalar(
            "SELECT queue_counts FROM horsies_claim_in_flight(
                 'cap-test-worker', ARRAY['default', 'critical'], TRUE,
                 '{\"ISOLATED\": \"ISOLATED\"}'::jsonb
             )",
        )
        .fetch_one(&pool)
        .await
        .expect("read the in-flight counts");

        assert_eq!(counts, serde_json::json!({"default": 1, "critical": 1}));
    }

    /// `TestReclaimUnderCap::test_worker_claim_loop_claims_pending_under_cap_with_expired_claim_present`:
    /// with a cluster cap of 1 and a claim whose lease expired, the pass
    /// still claims the pending row that ranks first.
    #[tokio::test]
    #[serial]
    async fn a_pass_under_the_caps_claims_the_pending_row_beside_an_expired_claim() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let pending = seed(&pool, "default", 100, 30.0 * 60.0).await;
        let expired = seed(&pool, "default", 100, 20.0 * 60.0).await;
        sqlx::query(
            "UPDATE horsies_tasks
             SET status = 'CLAIMED', claimed_at = now() - interval '10 minutes',
                 claimed_by_worker_id = 'dead-worker',
                 claim_expires_at = now() - interval '5 minutes', updated_at = now()
             WHERE id = $1",
        )
        .bind(expired)
        .execute(&pool)
        .await
        .expect("expire the seeded claim");
        let config = ClaimerConfig {
            queues: names(&["default"]),
            processes: 1,
            max_claim_batch: 1,
            max_claim_per_worker: 1,
            cluster_wide_cap: Some(1),
            ..ClaimerConfig::default()
        };
        let worker = worker_id();

        let claimed = pass(&core_pool_for_test_pool(&pool), &request(&config, &worker)).await;

        assert!(
            claimed.claimed.iter().any(|row| row.id == pending),
            "{claimed:?}"
        );
        let (status, owner): (String, Option<String>) =
            sqlx::query_as("SELECT status, claimed_by_worker_id FROM horsies_tasks WHERE id = $1")
                .bind(pending)
                .fetch_one(&pool)
                .await
                .expect("read the pending row");
        assert_eq!(status, "CLAIMED");
        assert_eq!(owner.as_deref(), Some(worker.as_str()));
    }
}
