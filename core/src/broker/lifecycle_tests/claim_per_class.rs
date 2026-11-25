//! The claim per execution class, against the baseline `horsies_claim`.
//!
//! - Each budget claims the classes that the routes give it, in priority
//!   order within the merge of those classes, and never a class routed
//!   elsewhere.
//! - This worker's RUNNING and live CLAIMED rows count against the budget of
//!   their class; the worker-wide limits still bound the whole pass.
//! - The expired arm follows the same routes.
//! - A claim locks only the rows it takes, and a row that another claim
//!   holds is skipped, not waited for.
//!
//! Each test runs on the database of its own runtime (`pool()`), so the
//! queue holds only the rows the test seeds. Each claim is one committed
//! statement, as Python's `_claim`.

use crate::test_support::sql::{self, TestPool, TestTransaction};
use serde_json::{json, Value};
use uuid::Uuid;

use super::support::pool;

const QUEUE: &str = "class_q";
const LEASE_MS: i64 = 60_000;

/// One claim pass of `horsies_claim`: the kind, id, class and short count of
/// each row it returns.
const CLAIM_SQL: &str = "SELECT c.row_kind, c.id, c.execution, c.short_by
     FROM horsies_claim(
         $1, $2::jsonb, $3::jsonb, $4::jsonb, $5, $6::jsonb, $7::jsonb,
         $8, $9, $10, NULL, $11, '[]'::jsonb
     ) AS c";

/// The lease of a seeded CLAIMED row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lease {
    Live,
    Expired,
}

/// The state of a seeded row (Python `_seed` arguments `status`, `owner`
/// and `lease`).
#[derive(Debug, Clone, PartialEq, Eq)]
enum SeedState {
    Pending,
    Running { owner: String },
    Claimed { owner: String, lease: Lease },
}

/// One seeded row: Python `_seed` with its defaults (priority 100, enqueued
/// 10 s ago, queue `class_q`).
#[derive(Debug, Clone)]
struct Seed {
    execution: &'static str,
    priority: i32,
    age_secs: i32,
    state: SeedState,
    queue: &'static str,
}

impl Seed {
    fn pending(execution: &'static str) -> Self {
        Self {
            execution,
            priority: 100,
            age_secs: 10,
            state: SeedState::Pending,
            queue: QUEUE,
        }
    }

    fn priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    fn age_secs(mut self, age_secs: i32) -> Self {
        self.age_secs = age_secs;
        self
    }

    fn queue(mut self, queue: &'static str) -> Self {
        self.queue = queue;
        self
    }

    fn running(mut self, owner: &str) -> Self {
        self.state = SeedState::Running {
            owner: owner.to_owned(),
        };
        self
    }

    fn claimed(mut self, owner: &str, lease: Lease) -> Self {
        self.state = SeedState::Claimed {
            owner: owner.to_owned(),
            lease,
        };
        self
    }
}

/// Insert one live task row, committed. Returns its id.
async fn seed(pool: &TestPool, seed: &Seed) -> Uuid {
    let id = Uuid::new_v4();
    let (status, owner): (&str, Option<&str>) = match &seed.state {
        SeedState::Pending => ("PENDING", None),
        SeedState::Running { owner } => ("RUNNING", Some(owner.as_str())),
        SeedState::Claimed { owner, .. } => ("CLAIMED", Some(owner.as_str())),
    };
    sql::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, status, enqueued_at, claimed_at,
            claimed_by_worker_id, is_workflow_task, created_at, updated_at, retry_count,
            max_retries, retention_class_key, command_fingerprint_version,
            command_fingerprint, retain_rerun_input, execution, sent_at, claim_expires_at,
            started_at, worker_pid, worker_hostname, worker_process_name
        ) VALUES (
            $1, 'class_task', $2, $3, '{}', $4, now() - ($5 * interval '1 second'),
            CASE WHEN $4 = 'PENDING' THEN NULL ELSE now() END,
            $6, FALSE, now(), now(), 0, 0, 'forever', 1,
            sha256(convert_to(CAST($1 AS text), 'UTF8')), FALSE, $7, now(),
            CASE WHEN $4 = 'CLAIMED' THEN now() + interval '1 hour' END,
            CASE WHEN $4 = 'RUNNING' THEN now() END,
            CASE WHEN $4 = 'RUNNING' THEN 4242 END,
            CASE WHEN $4 = 'RUNNING' THEN 'host-1' END,
            CASE WHEN $4 = 'RUNNING' THEN 'child-1' END
        )",
    )
    .bind(id)
    .bind(seed.queue)
    .bind(seed.priority)
    .bind(status)
    .bind(f64::from(seed.age_secs))
    .bind(owner)
    .bind(seed.execution)
    .execute(pool)
    .await
    .expect("seed one task row");
    if let SeedState::Claimed {
        lease: Lease::Expired,
        ..
    } = &seed.state
    {
        sql::query(
            "UPDATE horsies_tasks
             SET claimed_at = now() - interval '2 minutes',
                 claim_expires_at = now() - interval '1 minute'
             WHERE id = $1",
        )
        .bind(id)
        .execute(pool)
        .await
        .expect("expire the lease of the seeded claim");
    }
    id
}

async fn seed_many(pool: &TestPool, seeds: &[Seed]) -> Vec<Uuid> {
    let mut ids = Vec::with_capacity(seeds.len());
    for one in seeds {
        ids.push(seed(pool, one).await);
    }
    ids
}

/// The arguments of one claim (Python `_params`).
#[derive(Debug, Clone)]
struct Params {
    worker_id: String,
    budgets: Value,
    routes: Value,
    prefetch_buffer: i32,
    max_claim_per_worker: i32,
    max_claim_batch: i32,
    queues: Vec<&'static str>,
    queue_priority: Value,
    queue_max_concurrency: Value,
}

impl Params {
    fn new(budgets: Value, routes: Value) -> Self {
        Self {
            worker_id: worker_id(),
            budgets,
            routes,
            prefetch_buffer: 0,
            max_claim_per_worker: 0,
            max_claim_batch: 0,
            queues: vec![QUEUE],
            queue_priority: json!({}),
            queue_max_concurrency: json!({}),
        }
    }
}

fn worker_id() -> String {
    format!("class-{}", &Uuid::new_v4().simple().to_string()[..8])
}

fn isolated_routes() -> Value {
    json!({"ISOLATED": "ISOLATED", "SHARED_ASYNC": "ISOLATED", "SHARED_THREAD": "ISOLATED"})
}

fn isolated_and_async_routes() -> Value {
    json!({"ISOLATED": "ISOLATED", "SHARED_ASYNC": "SHARED_ASYNC", "SHARED_THREAD": "ISOLATED"})
}

/// What one claim returned: the claimed rows (id, class) in dispatch order,
/// and the short count of its SHORT row (0 when it has none).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Claimed {
    rows: Vec<(Uuid, String)>,
    short_by: i32,
}

/// One row of the claim statement: kind, task id, execution class, short
/// count.
type ClaimRow = (String, Option<Uuid>, Option<String>, Option<i32>);

async fn claim_on<'e>(
    target: impl Into<sql::Target<'e>>,
    params: &Params,
) -> Result<Claimed, sql::Error> {
    let rows: Vec<ClaimRow> = sql::query_as(CLAIM_SQL)
        .bind(&params.worker_id)
        .bind(json!(params.queues))
        .bind(&params.queue_priority)
        .bind(&params.queue_max_concurrency)
        .bind(params.prefetch_buffer == 0)
        .bind(&params.budgets)
        .bind(&params.routes)
        .bind(params.prefetch_buffer)
        .bind(params.max_claim_per_worker)
        .bind(params.max_claim_batch)
        .bind(LEASE_MS)
        .fetch_all(target)
        .await?;
    let mut claimed = Claimed {
        rows: Vec::new(),
        short_by: 0,
    };
    for (kind, id, execution, short_by) in rows {
        match (kind.as_str(), id, execution, short_by) {
            ("CLAIMED", Some(id), Some(execution), _) => claimed.rows.push((id, execution)),
            ("SHORT", None, None, Some(short_by)) => claimed.short_by = short_by,
            other => panic!("a claim row out of the claim's contract: {other:?}"),
        }
    }
    Ok(claimed)
}

/// One committed claim (Python `_claim`).
async fn claim(pool: &TestPool, params: &Params) -> Claimed {
    claim_on(pool, params).await.expect("one claim pass")
}

fn count_by_class(claimed: &Claimed) -> std::collections::BTreeMap<String, usize> {
    let mut counts = std::collections::BTreeMap::new();
    for (_id, execution) in &claimed.rows {
        *counts.entry(execution.clone()).or_insert(0) += 1;
    }
    counts
}

fn counts(pairs: &[(&str, usize)]) -> std::collections::BTreeMap<String, usize> {
    pairs
        .iter()
        .map(|(class, count)| ((*class).to_owned(), *count))
        .collect()
}

fn ids_with(claimed: &Claimed) -> Vec<(Uuid, &str)> {
    claimed
        .rows
        .iter()
        .map(|(id, execution)| (*id, execution.as_str()))
        .collect()
}

mod budgets {
    use super::*;

    /// `test_an_isolated_budget_claims_the_merge_of_every_class`: rows of
    /// three classes interleave in priority; the isolated budget reads them
    /// as one order and returns each row's class.
    #[tokio::test]
    async fn an_isolated_budget_claims_the_merge_of_every_class() {
        let pool = pool().await;
        let ids = seed_many(
            &pool,
            &[
                Seed::pending("SHARED_ASYNC").priority(1),
                Seed::pending("ISOLATED").priority(2),
                Seed::pending("SHARED_THREAD").priority(3),
                Seed::pending("SHARED_ASYNC").priority(4),
                Seed::pending("ISOLATED").priority(5),
            ],
        )
        .await;

        let claimed = claim(
            &pool,
            &Params::new(json!({"ISOLATED": 3}), isolated_routes()),
        )
        .await;

        assert_eq!(
            ids_with(&claimed),
            vec![
                (ids[0], "SHARED_ASYNC"),
                (ids[1], "ISOLATED"),
                (ids[2], "SHARED_THREAD"),
            ]
        );
    }

    /// `test_priority_holds_within_each_budget_not_across`: async rows of
    /// better priority do not take isolated capacity: each budget takes its
    /// own head, and the isolated children never run a class that has
    /// children of its own.
    #[tokio::test]
    async fn priority_holds_within_each_budget_not_across() {
        let pool = pool().await;
        let async_rows =
            seed_many(&pool, &vec![Seed::pending("SHARED_ASYNC").priority(1); 3]).await;
        let isolated_rows =
            seed_many(&pool, &vec![Seed::pending("ISOLATED").priority(50); 2]).await;
        let thread_row = seed(&pool, &Seed::pending("SHARED_THREAD").priority(60)).await;

        let claimed = claim(
            &pool,
            &Params::new(
                json!({"ISOLATED": 3, "SHARED_ASYNC": 1}),
                isolated_and_async_routes(),
            ),
        )
        .await;

        assert_eq!(
            count_by_class(&claimed),
            counts(&[("ISOLATED", 2), ("SHARED_ASYNC", 1), ("SHARED_THREAD", 1)])
        );
        let claimed_ids: std::collections::HashSet<Uuid> =
            claimed.rows.iter().map(|(id, _)| *id).collect();
        assert!(isolated_rows.iter().all(|id| claimed_ids.contains(id)));
        assert!(claimed_ids.contains(&thread_row));
        assert_eq!(
            async_rows
                .iter()
                .filter(|id| claimed_ids.contains(id))
                .count(),
            1
        );
    }

    /// `test_a_budget_never_takes_more_than_its_capacity_over_classes`: the
    /// isolated budget of 2 claims ISOLATED and SHARED_THREAD rows; each
    /// stream alone could fill it, and the worker total is 4.
    #[tokio::test]
    async fn a_budget_never_takes_more_than_its_capacity_over_classes() {
        let pool = pool().await;
        for _ in 0..3 {
            seed(&pool, &Seed::pending("ISOLATED")).await;
            seed(&pool, &Seed::pending("SHARED_THREAD")).await;
        }

        let claimed = claim(
            &pool,
            &Params::new(
                json!({"ISOLATED": 2, "SHARED_ASYNC": 2}),
                isolated_and_async_routes(),
            ),
        )
        .await;

        assert_eq!(claimed.rows.len(), 2);
    }

    /// `test_this_workers_rows_count_against_their_budget`: one RUNNING
    /// async row and one live CLAIMED isolated row of this worker leave one
    /// seat in each budget of 2. Rows of another worker do not count.
    #[tokio::test]
    async fn this_workers_rows_count_against_their_budget() {
        let pool = pool().await;
        let params = Params::new(
            json!({"ISOLATED": 2, "SHARED_ASYNC": 2}),
            isolated_and_async_routes(),
        );
        seed(
            &pool,
            &Seed::pending("SHARED_ASYNC").running(&params.worker_id),
        )
        .await;
        seed(
            &pool,
            &Seed::pending("ISOLATED").claimed(&params.worker_id, Lease::Live),
        )
        .await;
        seed(&pool, &Seed::pending("ISOLATED").running("someone-else")).await;
        for _ in 0..3 {
            seed(&pool, &Seed::pending("SHARED_ASYNC")).await;
            seed(&pool, &Seed::pending("ISOLATED")).await;
        }

        let claimed = claim(&pool, &params).await;

        assert_eq!(
            count_by_class(&claimed),
            counts(&[("ISOLATED", 1), ("SHARED_ASYNC", 1)])
        );
    }

    /// `test_claimed_per_worker_bounds_the_worker_across_budgets`.
    #[tokio::test]
    async fn claimed_per_worker_bounds_the_worker_across_budgets() {
        let pool = pool().await;
        for _ in 0..3 {
            seed(&pool, &Seed::pending("SHARED_ASYNC")).await;
            seed(&pool, &Seed::pending("ISOLATED")).await;
        }

        let claimed = claim(
            &pool,
            &Params {
                max_claim_per_worker: 2,
                ..Params::new(
                    json!({"ISOLATED": 3, "SHARED_ASYNC": 3}),
                    isolated_and_async_routes(),
                )
            },
        )
        .await;

        assert_eq!(claimed.rows.len(), 2);
    }

    /// `test_the_batch_limit_bounds_each_queue_across_budgets`.
    #[tokio::test]
    async fn the_batch_limit_bounds_each_queue_across_budgets() {
        let pool = pool().await;
        for _ in 0..3 {
            seed(&pool, &Seed::pending("SHARED_ASYNC")).await;
            seed(&pool, &Seed::pending("ISOLATED")).await;
        }

        let claimed = claim(
            &pool,
            &Params {
                max_claim_batch: 1,
                ..Params::new(
                    json!({"ISOLATED": 3, "SHARED_ASYNC": 3}),
                    isolated_and_async_routes(),
                )
            },
        )
        .await;

        assert_eq!(claimed.rows.len(), 1);
    }

    /// `test_soft_cap_lets_each_budget_prefetch_within_the_worker_total`:
    /// capacity 1 + 1 and prefetch 1: each budget may hold 2, the worker 3.
    /// Isolated rows rank first, so the third row is the async one.
    #[tokio::test]
    async fn soft_cap_lets_each_budget_prefetch_within_the_worker_total() {
        let pool = pool().await;
        for _ in 0..4 {
            seed(&pool, &Seed::pending("ISOLATED").priority(1)).await;
            seed(&pool, &Seed::pending("SHARED_ASYNC").priority(2)).await;
        }

        let claimed = claim(
            &pool,
            &Params {
                prefetch_buffer: 1,
                ..Params::new(
                    json!({"ISOLATED": 1, "SHARED_ASYNC": 1}),
                    isolated_and_async_routes(),
                )
            },
        )
        .await;

        assert_eq!(
            count_by_class(&claimed),
            counts(&[("ISOLATED", 2), ("SHARED_ASYNC", 1)])
        );
    }

    /// `test_a_budget_with_no_capacity_in_service_gets_no_prefetch`: every
    /// SHARED_ASYNC seat is down (capacity 0) and prefetch is 3. The async
    /// rows rank first and stay for a worker with seats; the ISOLATED budget
    /// of 1 still prefetches 3.
    #[tokio::test]
    async fn a_budget_with_no_capacity_in_service_gets_no_prefetch() {
        let pool = pool().await;
        for _ in 0..5 {
            seed(&pool, &Seed::pending("SHARED_ASYNC").priority(1)).await;
            seed(&pool, &Seed::pending("ISOLATED").priority(2)).await;
        }

        let claimed = claim(
            &pool,
            &Params {
                prefetch_buffer: 3,
                ..Params::new(
                    json!({"ISOLATED": 1, "SHARED_ASYNC": 0}),
                    isolated_and_async_routes(),
                )
            },
        )
        .await;

        assert_eq!(count_by_class(&claimed), counts(&[("ISOLATED", 4)]));
    }

    /// `test_a_worker_with_no_capacity_in_service_claims_nothing`: every
    /// child of the worker is down: prefetch takes no row.
    #[tokio::test]
    async fn a_worker_with_no_capacity_in_service_claims_nothing() {
        let pool = pool().await;
        for _ in 0..3 {
            seed(&pool, &Seed::pending("SHARED_ASYNC")).await;
            seed(&pool, &Seed::pending("ISOLATED")).await;
        }

        let claimed = claim(
            &pool,
            &Params {
                prefetch_buffer: 2,
                ..Params::new(
                    json!({"ISOLATED": 0, "SHARED_ASYNC": 0}),
                    isolated_and_async_routes(),
                )
            },
        )
        .await;

        assert_eq!(
            claimed,
            Claimed {
                rows: Vec::new(),
                short_by: 0
            }
        );
    }

    /// `test_a_route_to_a_budget_that_is_not_passed_is_refused`: 22023 with
    /// the function's own message.
    #[tokio::test]
    async fn a_route_to_a_budget_that_is_not_passed_is_refused() {
        let pool = pool().await;

        let refused = claim_on(
            &pool,
            &Params::new(
                json!({"ISOLATED": 2}),
                json!({"SHARED_ASYNC": "SHARED_ASYNC"}),
            ),
        )
        .await;

        match refused {
            Err(sql::Error::Database(error)) => {
                assert_eq!(Some(error.code().code()), Some("22023"));
                assert_eq!(
                    error.message(),
                    "horsies_claim: p_routes names a budget class that p_budgets does not hold"
                );
            }
            other => panic!("expected the 22023 refusal, got {other:?}"),
        }
    }
}

mod one_walk {
    use super::*;

    /// `test_a_row_its_budget_drops_takes_no_queue_room`: queue P ranks
    /// first and fills the ISOLATED budget. Queue Q (cap 2) holds older
    /// ISOLATED rows, which the full budget drops, then SHARED_ASYNC rows:
    /// Q's room of 2 goes to those.
    #[tokio::test]
    async fn a_row_its_budget_drops_takes_no_queue_room() {
        let pool = pool().await;
        let mut p_rows = Vec::new();
        for i in 0..5 {
            p_rows.push(
                seed(
                    &pool,
                    &Seed::pending("ISOLATED").age_secs(1000 - i).queue("walk_p"),
                )
                .await,
            );
        }
        for i in 0..5 {
            seed(
                &pool,
                &Seed::pending("ISOLATED").age_secs(900 - i).queue("walk_q"),
            )
            .await;
        }
        let mut q_async = Vec::new();
        for i in 0..5 {
            q_async.push(
                seed(
                    &pool,
                    &Seed::pending("SHARED_ASYNC")
                        .age_secs(100 - i)
                        .queue("walk_q"),
                )
                .await,
            );
        }

        let claimed = claim(
            &pool,
            &Params {
                queues: vec!["walk_p", "walk_q"],
                queue_priority: json!({"walk_p": 1, "walk_q": 2}),
                queue_max_concurrency: json!({"walk_q": 2}),
                ..Params::new(
                    json!({"ISOLATED": 2, "SHARED_ASYNC": 2}),
                    isolated_and_async_routes(),
                )
            },
        )
        .await;

        assert_eq!(
            ids_with(&claimed),
            vec![
                (p_rows[0], "ISOLATED"),
                (p_rows[1], "ISOLATED"),
                (q_async[0], "SHARED_ASYNC"),
                (q_async[1], "SHARED_ASYNC"),
            ]
        );
    }

    /// `test_expired_rows_of_a_full_budget_leave_the_other_budget_its_seats`:
    /// a crashed worker left 10 ISOLATED rows with expired leases. They and
    /// the pending ISOLATED rows rank before the SHARED_ASYNC rows, and the
    /// ISOLATED budget takes 2: the 2 async seats still fill.
    #[tokio::test]
    async fn expired_rows_of_a_full_budget_leave_the_other_budget_its_seats() {
        let pool = pool().await;
        let mut expired = Vec::new();
        for i in 0..10 {
            expired.push(
                seed(
                    &pool,
                    &Seed::pending("ISOLATED")
                        .age_secs(2000 - i)
                        .claimed("crashed-worker", Lease::Expired),
                )
                .await,
            );
        }
        for i in 0..5 {
            seed(&pool, &Seed::pending("ISOLATED").age_secs(1000 - i)).await;
        }
        let mut pending_async = Vec::new();
        for i in 0..5 {
            pending_async.push(seed(&pool, &Seed::pending("SHARED_ASYNC").age_secs(100 - i)).await);
        }

        let claimed = claim(
            &pool,
            &Params::new(
                json!({"ISOLATED": 2, "SHARED_ASYNC": 2}),
                isolated_and_async_routes(),
            ),
        )
        .await;

        assert_eq!(
            ids_with(&claimed),
            vec![
                (expired[0], "ISOLATED"),
                (expired[1], "ISOLATED"),
                (pending_async[0], "SHARED_ASYNC"),
                (pending_async[1], "SHARED_ASYNC"),
            ]
        );
    }

    /// `test_a_stream_reads_its_pending_and_expired_rows_as_one_order`:
    /// pending and expired rows of one class interleave in priority; a
    /// budget of 3 takes the 3 best of both, not 3 of each.
    #[tokio::test]
    async fn a_stream_reads_its_pending_and_expired_rows_as_one_order() {
        let pool = pool().await;
        let first = seed(&pool, &Seed::pending("ISOLATED").priority(1)).await;
        let second = seed(
            &pool,
            &Seed::pending("ISOLATED")
                .priority(2)
                .claimed("crashed-worker", Lease::Expired),
        )
        .await;
        let third = seed(&pool, &Seed::pending("ISOLATED").priority(3)).await;
        seed(
            &pool,
            &Seed::pending("ISOLATED")
                .priority(4)
                .claimed("crashed-worker", Lease::Expired),
        )
        .await;
        seed(&pool, &Seed::pending("ISOLATED").priority(5)).await;

        let claimed = claim(
            &pool,
            &Params::new(json!({"ISOLATED": 3}), isolated_routes()),
        )
        .await;

        assert_eq!(
            ids_with(&claimed),
            vec![
                (first, "ISOLATED"),
                (second, "ISOLATED"),
                (third, "ISOLATED"),
            ]
        );
    }
}

mod expired_arm {
    use super::*;

    /// `test_an_expired_claim_follows_the_routes`.
    #[tokio::test]
    async fn an_expired_claim_follows_the_routes() {
        let pool = pool().await;
        let lapsed = seed(
            &pool,
            &Seed::pending("SHARED_ASYNC").claimed("lapsed-worker", Lease::Expired),
        )
        .await;

        let not_served = claim(
            &pool,
            &Params::new(json!({"ISOLATED": 2}), json!({"ISOLATED": "ISOLATED"})),
        )
        .await;
        let served = claim(
            &pool,
            &Params::new(
                json!({"ISOLATED": 2, "SHARED_ASYNC": 2}),
                isolated_and_async_routes(),
            ),
        )
        .await;

        assert!(not_served.rows.is_empty(), "{not_served:?}");
        assert_eq!(ids_with(&served), vec![(lapsed, "SHARED_ASYNC")]);
    }

    /// `test_an_unserved_expired_row_takes_no_budget`: an expired async row
    /// ranks first, and this worker serves only ISOLATED: the one seat goes
    /// to the pending isolated row.
    #[tokio::test]
    async fn an_unserved_expired_row_takes_no_budget() {
        let pool = pool().await;
        seed(
            &pool,
            &Seed::pending("SHARED_ASYNC")
                .priority(1)
                .claimed("lapsed-worker", Lease::Expired),
        )
        .await;
        let pending = seed(&pool, &Seed::pending("ISOLATED").priority(5)).await;

        let claimed = claim(
            &pool,
            &Params::new(json!({"ISOLATED": 1}), json!({"ISOLATED": "ISOLATED"})),
        )
        .await;

        assert_eq!(ids_with(&claimed), vec![(pending, "ISOLATED")]);
    }

    /// `test_the_expired_arm_takes_only_a_served_class`: two expired rows;
    /// the async one ranks first and is not served.
    #[tokio::test]
    async fn the_expired_arm_takes_only_a_served_class() {
        let pool = pool().await;
        seed(
            &pool,
            &Seed::pending("SHARED_ASYNC")
                .priority(1)
                .claimed("lapsed-worker", Lease::Expired),
        )
        .await;
        let isolated = seed(
            &pool,
            &Seed::pending("ISOLATED")
                .priority(5)
                .claimed("lapsed-worker", Lease::Expired),
        )
        .await;

        let claimed = claim(
            &pool,
            &Params::new(json!({"ISOLATED": 1}), json!({"ISOLATED": "ISOLATED"})),
        )
        .await;

        assert_eq!(ids_with(&claimed), vec![(isolated, "ISOLATED")]);
    }
}

mod locks {
    use super::*;

    /// `test_a_claim_locks_only_the_rows_it_takes`: 30 rows of two classes
    /// in one isolated budget of 5: while the claim is open, another worker
    /// can still lock the other 25.
    #[tokio::test]
    async fn a_claim_locks_only_the_rows_it_takes() {
        let pool = pool().await;
        for _ in 0..15 {
            seed(&pool, &Seed::pending("ISOLATED")).await;
            seed(&pool, &Seed::pending("SHARED_ASYNC")).await;
        }
        let params = Params::new(json!({"ISOLATED": 5}), isolated_routes());

        let mut claimer: TestTransaction<'_> = pool.begin().await.expect("begin the claim");
        let claimed = claim_on(&mut *claimer, &params)
            .await
            .expect("the claim inside the open transaction");
        let mut other_worker = pool.begin().await.expect("begin the other worker");
        let unlocked: i64 = sql::query_scalar(
            "SELECT count(*) FROM (
                 SELECT id FROM horsies_tasks
                 WHERE queue_name = $1 AND status = 'PENDING'
                 FOR UPDATE SKIP LOCKED
             ) AS free",
        )
        .bind(QUEUE)
        .fetch_one(&mut *other_worker)
        .await
        .expect("lock the free rows");
        other_worker.rollback().await.expect("end the other worker");
        claimer.rollback().await.expect("end the claim");

        assert_eq!(claimed.rows.len(), 5);
        assert_eq!(unlocked, 25);
    }

    /// `test_a_row_held_by_another_claim_is_skipped`: the head of the stream
    /// is locked elsewhere: the claim takes the next row of the same class
    /// instead of none.
    #[tokio::test]
    async fn a_row_held_by_another_claim_is_skipped() {
        let pool = pool().await;
        let head = seed(&pool, &Seed::pending("ISOLATED").priority(1)).await;
        let second = seed(&pool, &Seed::pending("ISOLATED").priority(2)).await;

        let mut holder = pool.begin().await.expect("begin the holder");
        sql::query("SELECT id FROM horsies_tasks WHERE id = $1 FOR UPDATE")
            .bind(head)
            .execute(&mut *holder)
            .await
            .expect("lock the head");
        let claimed = claim(
            &pool,
            &Params::new(json!({"ISOLATED": 1}), isolated_routes()),
        )
        .await;
        holder.rollback().await.expect("end the holder");

        assert_eq!(ids_with(&claimed), vec![(second, "ISOLATED")]);
    }
}

mod short_take {
    use super::*;

    /// `test_a_take_short_of_its_plan_asks_for_another_pass` (the claim
    /// statement; the pass that asks for another pass on a short take is
    /// `worker::claim_pass::tests::a_short_take_asks_for_a_bounded_run_of_passes`):
    /// another claim holds the 4 ISOLATED rows that rank first, and the
    /// budget of 4 plans them. The take gets none of them, and the stream
    /// has no other row: the claim reports the 4 it missed. After the other
    /// claim commits, the next claim takes the SHARED_THREAD rows that the
    /// first plan left out.
    #[tokio::test]
    async fn a_take_short_of_its_plan_reports_the_rows_it_missed() {
        let pool = pool().await;
        let held = seed_many(&pool, &vec![Seed::pending("ISOLATED").priority(1); 4]).await;
        let waiting = seed_many(&pool, &vec![Seed::pending("SHARED_THREAD").priority(10); 2]).await;
        let params = Params {
            queue_priority: Value::Object(serde_json::Map::from_iter([(
                QUEUE.to_owned(),
                json!(100),
            )])),
            ..Params::new(json!({"ISOLATED": 4}), isolated_routes())
        };

        let mut other_claim = pool.begin().await.expect("begin the other claim");
        sql::query(
            "UPDATE horsies_tasks
             SET status = 'CLAIMED',
                 claimed_by_worker_id = 'other-worker',
                 attempt_claims = 1, first_claimed_at = now(), claimed_at = now(),
                 claim_expires_at = now() + interval '1 hour'
             WHERE id = ANY($1)",
        )
        .bind(&held)
        .execute(&mut *other_claim)
        .await
        .expect("claim the held rows elsewhere");
        let first = claim(&pool, &params).await;
        other_claim.commit().await.expect("commit the other claim");
        let second = claim(&pool, &params).await;

        assert_eq!(
            first,
            Claimed {
                rows: Vec::new(),
                short_by: 4
            }
        );
        let mut second_ids: Vec<Uuid> = second.rows.iter().map(|(id, _)| *id).collect();
        second_ids.sort();
        let mut expected = waiting.clone();
        expected.sort();
        assert_eq!(second_ids, expected);
        assert_eq!(second.short_by, 0);
    }
}
