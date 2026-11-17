//! Port of Python `tests/integration/test_soft_cap_start_check.py`.
//!
//! In soft-cap mode the claim counts only RUNNING rows against a queue's
//! `max_concurrency`, so passes that follow each other claim rows of a capped
//! queue while the rows that they claimed before are still CLAIMED. Each pass
//! submits a claimed row of a capped queue only while the queue's RUNNING
//! rows, plus the claims that this worker admitted that are not RUNNING yet,
//! stay under the cap. The other rows stay CLAIMED, held by the worker.
//!
//! A pass here is the one of `Worker::claim_and_dispatch` without the
//! children: the 0004 claim of a worker with one isolated slot and
//! `prefetch_buffer = 2` (a claim takes up to 3 rows), the hold of the rows
//! of the capped queue, then the start check (`admit_held_rows`). The
//! submitted rows are recorded; the test holds the guard of each admitted
//! claim, as its outcome write does, and drops it to end the write.

use std::collections::HashMap;
use std::num::NonZeroU32;

use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::support::*;
use crate::broker::claims::{claim_pass, ClaimClasses, ClaimPassRequest};
use crate::broker::core_rows::StatementLog;
use crate::broker::pools::core_pool_for_test_pool;
use crate::broker::PostgresBroker;
use crate::core::task::Execution;
use crate::worker::queue_cap_hold::{admit_held_rows, QueueCapHold, StartCheckCaps, StartedClaim};

/// The prefetch buffer of the test worker: soft-cap mode.
const PREFETCH_BUFFER: u32 = 2;

/// The queues and the worker of one test. Fresh names keep the rows of other
/// tests out of the counts and out of the worker's claim budget.
struct Names {
    capped: String,
    free: String,
    worker_id: String,
}

impl Names {
    fn fresh() -> Names {
        let suffix = &Uuid::new_v4().simple().to_string()[..8];
        Names {
            capped: format!("soft_cap_capped_{suffix}"),
            free: format!("soft_cap_free_{suffix}"),
            worker_id: format!("soft-cap-worker-{suffix}"),
        }
    }
}

/// One PENDING row of `queue_name` (Python `_seed_pending`); an older row is
/// claimed first.
async fn seed_pending(pool: &PgPool, queue_name: &str, age_s: i32) -> Uuid {
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
            $1, 'soft_cap_start_task', $2, 100, '{}', NULL, 'ISOLATED',
            'forever', NOW(), NOW(),
            1, decode(repeat('07', 32), 'hex'), NULL,
            'PENDING', NOW() - $3 * INTERVAL '1 second', NULL, NULL, NULL,
            NULL, NULL, NULL,
            NULL, NULL, NULL,
            0, 0, NULL, NULL, FALSE,
            NOW(), FALSE, NULL, NULL
        )",
    )
    .bind(task_id)
    .bind(queue_name)
    .bind(age_s)
    .execute(pool)
    .await
    .expect("seed a pending row");
    task_id
}

/// Python `_set_running`: the row is RUNNING on `worker_id`.
async fn set_running(pool: &PgPool, task_id: Uuid, worker_id: &str) {
    sqlx::query(
        "UPDATE horsies_tasks
         SET status = 'RUNNING', claimed_at = NOW(), claim_expires_at = NULL,
             claimed_by_worker_id = $2, started_at = NOW(), updated_at = NOW()
         WHERE id = $1",
    )
    .bind(task_id)
    .bind(worker_id)
    .execute(pool)
    .await
    .expect("set the row RUNNING");
}

/// The status and the owner of each row.
async fn statuses(pool: &PgPool, task_ids: &[Uuid]) -> HashMap<Uuid, (String, Option<String>)> {
    let rows: Vec<(Uuid, String, Option<String>)> = sqlx::query_as(
        "SELECT id, status::text, claimed_by_worker_id::text FROM horsies_tasks WHERE id = ANY($1)",
    )
    .bind(task_ids)
    .fetch_all(pool)
    .await
    .expect("read the statuses");
    rows.into_iter()
        .map(|(task_id, status, owner)| (task_id, (status, owner)))
        .collect()
}

/// The claim of the test worker: one `ISOLATED` slot that every class
/// routes to, soft-cap mode, both queues at priority 1, `cap` on the capped
/// queue.
fn claim_request(names: &Names, budget: u32, prefetch_buffer: u32, cap: u32) -> ClaimPassRequest {
    ClaimPassRequest {
        worker_id: names.worker_id.clone(),
        queues: vec![names.capped.clone(), names.free.clone()],
        queue_priority: HashMap::from([(names.capped.clone(), 1), (names.free.clone(), 1)]),
        queue_max_concurrency: HashMap::from([(names.capped.clone(), cap)]),
        hard_cap_mode: false,
        classes: ClaimClasses::new(
            HashMap::from([(Execution::Isolated, budget)]),
            HashMap::from([
                (Execution::Isolated, Execution::Isolated),
                (Execution::SharedAsync, Execution::Isolated),
                (Execution::SharedThread, Execution::Isolated),
            ]),
        )
        .expect("every route names the ISOLATED budget"),
        prefetch_buffer,
        max_claim_per_worker: 0,
        max_claim_batch: 0,
        cluster_wide_cap: None,
        claim_lease_ms: NonZeroU32::new(60_000).expect("a lease above 0"),
        lock_keys: Vec::new(),
    }
}

/// The claim loop of the test worker, with the submits recorded (Python
/// `_soft_cap_worker`, `_RecordedDispatch`).
struct SoftCapWorker {
    names: Names,
    request: ClaimPassRequest,
    hold: QueueCapHold,
    /// The submitted rows, in submit order.
    started: Vec<Uuid>,
    /// The guard of each admitted claim whose write has not ended.
    writes: HashMap<Uuid, StartedClaim>,
}

impl SoftCapWorker {
    /// A worker of one isolated slot and `prefetch_buffer = 2`.
    fn new(names: Names, cap: u32) -> SoftCapWorker {
        Self::with_budget(names, 1, PREFETCH_BUFFER, cap)
    }

    fn with_budget(names: Names, budget: u32, prefetch_buffer: u32, cap: u32) -> SoftCapWorker {
        let request = claim_request(&names, budget, prefetch_buffer, cap);
        let hold = QueueCapHold::new(StartCheckCaps::of_mode(
            prefetch_buffer,
            &request.queue_max_concurrency,
        ));
        SoftCapWorker {
            names,
            request,
            hold,
            started: Vec::new(),
            writes: HashMap::new(),
        }
    }

    /// One pass: the claim, each row of an uncapped queue submitted, each
    /// row of the capped queue held, then the start check.
    async fn pass(&mut self, pool: &PgPool) {
        let claimed = claim_pass(
            &core_pool_for_test_pool(pool),
            StatementLog::Off,
            &self.request,
        )
        .await
        .expect("claim pass");
        for row in claimed.claimed {
            match self.hold.holds(&row.queue_name) {
                true => self.hold.hold(row),
                false => self.started.push(row.id),
            }
        }
        let broker = PostgresBroker::from_pool(pool.clone());
        let admitted = admit_held_rows(&broker, &self.names.worker_id, &mut self.hold)
            .await
            .expect("start check");
        for admitted in admitted {
            self.started.push(admitted.row.id);
            let _earlier = self.writes.insert(admitted.row.id, admitted.started);
        }
    }

    async fn passes(&mut self, pool: &PgPool, count: u32) {
        for _ in 0..count {
            self.pass(pool).await;
        }
    }

    /// The outcome write of `task_id` ended (Python `_RecordedDispatch.end`).
    fn end(&mut self, task_id: Uuid) {
        let ended = self
            .writes
            .remove(&task_id)
            .expect("the test ends only an admitted claim");
        drop(ended);
    }
}

/// Python `test_rows_of_a_capped_queue_past_the_cap_are_claimed_and_not_started`:
/// cap 2 with one row RUNNING on another worker: one claimed row starts; the
/// rows that later passes claim stay CLAIMED and are not started.
#[tokio::test]
#[serial]
async fn rows_of_a_capped_queue_past_the_cap_are_claimed_and_not_started() {
    let pool = pool().await;
    let names = Names::fresh();
    let running_elsewhere = seed_pending(&pool, &names.capped, 100).await;
    set_running(&pool, running_elsewhere, OTHER_WORKER).await;
    let mut seeded = Vec::new();
    for n in 0..3 {
        seeded.push(seed_pending(&pool, &names.capped, 50 - n).await);
    }
    let mut worker = SoftCapWorker::new(names, 2);

    worker.passes(&pool, 3).await;

    assert_eq!(worker.started, vec![seeded[0]]);
    let statuses = statuses(&pool, &seeded).await;
    for task_id in &seeded {
        assert_eq!(
            statuses[task_id],
            ("CLAIMED".to_owned(), Some(worker.names.worker_id.clone()))
        );
    }
    let mut all = seeded.clone();
    all.push(running_elsewhere);
    cleanup_tasks(&pool, &all).await;
}

/// Python `test_a_started_row_counts_before_its_child_sets_it_running`:
/// cap 1: the first row starts; while it is still CLAIMED, the next claimed
/// row is not started.
#[tokio::test]
#[serial]
async fn a_started_row_counts_before_its_child_sets_it_running() {
    let pool = pool().await;
    let names = Names::fresh();
    let seeded = vec![
        seed_pending(&pool, &names.capped, 50).await,
        seed_pending(&pool, &names.capped, 49).await,
    ];
    let mut worker = SoftCapWorker::new(names, 1);

    worker.passes(&pool, 2).await;

    assert_eq!(worker.started, vec![seeded[0]]);
    assert_eq!(
        statuses(&pool, &seeded).await[&seeded[1]],
        ("CLAIMED".to_owned(), Some(worker.names.worker_id.clone()))
    );
    cleanup_tasks(&pool, &seeded).await;
}

/// Python `test_a_held_row_starts_when_the_queue_has_room_again`: cap 1: a
/// held row does not start while the started row runs, and starts on the
/// first pass after that row ended.
#[tokio::test]
#[serial]
async fn a_held_row_starts_when_the_queue_has_room_again() {
    let pool = pool().await;
    let names = Names::fresh();
    let seeded = vec![
        seed_pending(&pool, &names.capped, 50).await,
        seed_pending(&pool, &names.capped, 49).await,
    ];
    let mut worker = SoftCapWorker::new(names, 1);
    worker.passes(&pool, 2).await;
    assert_eq!(worker.started, vec![seeded[0]]);

    let worker_id = worker.names.worker_id.clone();
    set_running(&pool, seeded[0], &worker_id).await;
    worker.passes(&pool, 1).await;
    assert_eq!(worker.started, vec![seeded[0]]);

    cleanup_tasks(&pool, &seeded[..1]).await;
    worker.end(seeded[0]);
    worker.passes(&pool, 1).await;
    assert_eq!(worker.started, vec![seeded[0], seeded[1]]);
    cleanup_tasks(&pool, &seeded).await;
}

/// Python `test_rows_of_an_uncapped_queue_start_while_a_capped_queue_waits`.
#[tokio::test]
#[serial]
async fn rows_of_an_uncapped_queue_start_while_a_capped_queue_waits() {
    let pool = pool().await;
    let names = Names::fresh();
    let capped = vec![
        seed_pending(&pool, &names.capped, 50).await,
        seed_pending(&pool, &names.capped, 49).await,
    ];
    let free = seed_pending(&pool, &names.free, 10).await;
    let mut worker = SoftCapWorker::new(names, 1);

    worker.passes(&pool, 2).await;

    let mut started = worker.started.clone();
    started.sort();
    let mut expected = vec![capped[0], free];
    expected.sort();
    assert_eq!(started, expected);
    let mut all = capped.clone();
    all.push(free);
    cleanup_tasks(&pool, &all).await;
}

/// Port of the removed in-process test of the same name: in soft-cap mode
/// the start of a capped queue's rows stays within its `max_concurrency`.
/// A worker of 10 slots and `prefetch_buffer = 4` claims all 6 rows of a
/// queue of cap 2 over three passes (the claim counts only RUNNING rows);
/// the cap's worth starts, and the queue never has more RUNNING rows than
/// its cap.
#[tokio::test]
#[serial]
async fn buffered_dispatch_respects_queue_cap_in_soft_mode() {
    let pool = pool().await;
    let names = Names::fresh();
    let mut seeded = Vec::new();
    for n in 0..6 {
        seeded.push(seed_pending(&pool, &names.capped, 60 - n).await);
    }
    let mut worker = SoftCapWorker::with_budget(names, 10, 4, 2);

    worker.passes(&pool, 3).await;
    assert_eq!(worker.started, vec![seeded[0], seeded[1]]);
    let worker_id = worker.names.worker_id.clone();
    for task_id in &worker.started {
        set_running(&pool, *task_id, &worker_id).await;
    }
    worker.passes(&pool, 1).await;

    let statuses = statuses(&pool, &seeded).await;
    let running = statuses
        .values()
        .filter(|(status, _)| status == "RUNNING")
        .count();
    assert_eq!(
        running, 2,
        "the cap's worth of claimed rows starts, and no more"
    );
    assert_eq!(worker.started.len(), 2);
    assert_eq!(worker.hold.held_count(), 4);
    for task_id in &seeded[2..] {
        assert_eq!(
            statuses[task_id],
            ("CLAIMED".to_owned(), Some(worker_id.clone()))
        );
    }
    cleanup_tasks(&pool, &seeded).await;
}
