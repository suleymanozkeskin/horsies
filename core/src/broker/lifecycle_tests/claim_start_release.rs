//! The claim pass, the start and the release on the 0004 functions
//! (`broker::claims`), run against the test database: the class budget and
//! route of a pass (#711), the miss reasons of the start (#758), and the
//! release of claimed rows (#764).

use chrono::Duration;
use uuid::Uuid;

use super::support::*;
use crate::broker::claims::{
    claim_pass, release_claims, start_attempt, start_attempt_within, ClaimClasses, ClaimTake,
    ReleaseBatch, ReleaseClaims, ReleaseOutcome, ReleasePlacement, StartAttempt, StartMiss,
    StartOutcome, StartWindow, WindowedStart,
};
use crate::broker::core_rows::StatementLog;
use crate::broker::pools::core_pool_for_test_url;
use crate::core::task::Execution;
use crate::events::{RunTrace, TraceCarrierText};
use crate::test_support::sql::{self, TestPool};

const QUEUE: &str = "lifecycle_claim_start_release";

fn pending(execution: &'static str) -> TaskSeed {
    TaskSeed {
        queue_name: QUEUE,
        execution,
        max_retries: 2,
        ..TaskSeed::pending()
    }
}

fn claimed() -> TaskSeed {
    TaskSeed {
        queue_name: QUEUE,
        ..TaskSeed::claimed()
    }
}

fn start(task_id: Uuid) -> StartAttempt {
    StartAttempt {
        task_id,
        fence: owned_claim(),
        worker_pid: 4242,
        worker_hostname: "lifecycle-host".to_owned(),
        worker_process_name: "lifecycle-process".to_owned(),
    }
}

fn release(task_id: Uuid, placement: ReleasePlacement) -> ReleaseClaims {
    ReleaseClaims {
        worker_id: WORKER.to_owned(),
        batch: ReleaseBatch::new(&[(task_id, generation())]).expect("one claim"),
        placement,
    }
}

async fn runner_beats(pool: &TestPool, task_id: Uuid) -> i64 {
    sql::query_scalar(
        "SELECT count(*) FROM horsies_heartbeats WHERE task_id = $1 AND role = 'runner'",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("count runner beats")
}

mod claim {
    use super::*;

    /// A pass whose plan routes a row's class to a budget claims the row,
    /// with its class and retry facts.
    #[tokio::test]
    async fn a_routed_class_is_claimed_with_its_class_and_retry_facts() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &pending("SHARED_THREAD")).await;

        let pass = claim_pass(
            &core_pool_for_test_url(pool.url()),
            StatementLog::Off,
            &claim_request(QUEUE),
        )
        .await
        .expect("claim pass");

        let row = pass
            .claimed
            .iter()
            .find(|row| row.id == task_id)
            .expect("the pass claimed the seeded row");
        assert_eq!(row.execution, Execution::SharedThread);
        assert_eq!((row.retry_count, row.max_retries), (0, 2));
        assert_eq!(row.queue_name, QUEUE);
        assert_eq!(pass.take, ClaimTake::Full);
        assert_eq!(status_of(&pool, task_id).await, "CLAIMED");
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// The claim returns the trace carrier that the send stored on the row,
    /// as its text; a row with no carrier claims with no carrier.
    #[tokio::test]
    async fn a_claimed_row_carries_its_stored_trace_carrier() {
        let carrier =
            r#"{"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}"#;
        let pool = pool().await;
        let traced = seed_task(&pool, &pending("SHARED_THREAD")).await;
        let untraced = seed_task(&pool, &pending("SHARED_THREAD")).await;
        sql::query(
            "UPDATE horsies_tasks SET trace_carrier = $2, \
             trace_id = decode(repeat('ab', 16), 'hex') WHERE id = $1",
        )
        .bind(traced)
        .bind(carrier)
        .execute(&pool)
        .await
        .expect("store a trace carrier on the row");

        let pass = claim_pass(
            &core_pool_for_test_url(pool.url()),
            StatementLog::Off,
            &claim_request(QUEUE),
        )
        .await
        .expect("claim pass");

        let trace_of = |task_id: Uuid| {
            pass.claimed
                .iter()
                .find(|row| row.id == task_id)
                .map(|row| row.trace.clone())
                .expect("the pass claimed the seeded row")
        };
        assert_eq!(
            trace_of(traced),
            RunTrace::Carrier {
                text: TraceCarrierText::new(carrier.to_owned()).expect("a short carrier"),
            }
        );
        assert_eq!(trace_of(untraced), RunTrace::NoCarrier);
        cleanup_tasks(&pool, &[traced, untraced]).await;
    }

    /// A class that no route names is not served: its row stays PENDING.
    #[tokio::test]
    async fn a_class_with_no_route_is_not_claimed() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &pending("SHARED_ASYNC")).await;
        let request = crate::broker::claims::ClaimPassRequest {
            classes: ClaimClasses::new(
                std::collections::HashMap::from([(Execution::Isolated, 10)]),
                std::collections::HashMap::from([(Execution::Isolated, Execution::Isolated)]),
            )
            .expect("the route names its budget"),
            ..claim_request(QUEUE)
        };

        let pass = claim_pass(
            &core_pool_for_test_url(pool.url()),
            StatementLog::Off,
            &request,
        )
        .await
        .expect("claim pass");

        assert!(pass.claimed.iter().all(|row| row.id != task_id));
        assert_eq!(status_of(&pool, task_id).await, "PENDING");
        cleanup_tasks(&pool, &[task_id]).await;
    }
}

mod start {
    use super::*;

    /// A row CLAIMED by this worker at this generation becomes RUNNING. The
    /// start writes no runner beat: the stale RUNNING rule judges the
    /// attempt from its `started_at` until the first beat of the worker.
    #[tokio::test]
    async fn a_held_claim_starts_and_writes_no_runner_beat() {
        let pool = pool().await;
        ensure_recent_heartbeat_leaves(&pool).await;
        let task_id = seed_task(&pool, &claimed()).await;

        let outcome = start_attempt(
            &core_pool_for_test_url(pool.url()),
            StatementLog::Off,
            &start(task_id),
            crate::broker::child_writes::WriteDeadline::start_attempt(),
        )
        .await
        .expect("start");

        assert!(
            matches!(outcome, StartOutcome::Started { .. }),
            "{outcome:?}"
        );
        let row = live_row(&pool, task_id).await.expect("live row");
        assert_eq!(row["status"], "RUNNING");
        assert_eq!(row["claim_expires_at"], serde_json::Value::Null);
        assert_eq!(row["worker_pid"], 4242);
        assert_eq!(runner_beats(&pool, task_id).await, 0);
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// The miss reasons of a plain task, each with nothing written.
    #[tokio::test]
    async fn a_start_that_does_not_hold_writes_nothing_and_names_its_reason() {
        let pool = pool().await;
        ensure_recent_heartbeat_leaves(&pool).await;

        let other_generation = seed_task(
            &pool,
            &TaskSeed {
                holder: Holder::Claim {
                    worker_id: WORKER.to_owned(),
                    claimed_at: generation() + Duration::seconds(1),
                },
                ..claimed()
            },
        )
        .await;
        let lease_expired = seed_task(&pool, &claimed()).await;
        sql::query(
            "UPDATE horsies_tasks SET claim_expires_at = NOW() - interval '1 second'
             WHERE id = $1",
        )
        .bind(lease_expired)
        .execute(&pool)
        .await
        .expect("expire the lease");
        let deadline_passed = seed_task(&pool, &claimed()).await;
        set_deadline(&pool, deadline_passed, Deadline::PassedSecondsAgo(1.0)).await;
        let absent = Uuid::new_v4();

        for (task_id, miss) in [
            (other_generation, StartMiss::NotHeld),
            (lease_expired, StartMiss::LeaseExpired),
            (deadline_passed, StartMiss::DeadlinePassed),
            (absent, StartMiss::NotHeld),
        ] {
            let before = live_row(&pool, task_id).await;
            let outcome = start_attempt(
                &core_pool_for_test_url(pool.url()),
                StatementLog::Off,
                &start(task_id),
                crate::broker::child_writes::WriteDeadline::start_attempt(),
            )
            .await
            .expect("start");
            assert_eq!(outcome, StartOutcome::Missed(miss), "{task_id}");
            assert_eq!(live_row(&pool, task_id).await, before, "{task_id}");
            assert_eq!(runner_beats(&pool, task_id).await, 0, "{task_id}");
        }
        cleanup_tasks(&pool, &[other_generation, lease_expired, deadline_passed]).await;
    }

    /// A window that closed before the pool gave the connection sends no
    /// statement: the row stays CLAIMED with no runner beat (Python
    /// `DeadlineBeforeStart`). An open window, and a window that is still
    /// open at the checkout, start the row.
    #[tokio::test]
    async fn a_closed_window_sends_no_statement_and_an_open_one_starts() {
        let pool = pool().await;
        ensure_recent_heartbeat_leaves(&pool).await;
        let closed = seed_task(&pool, &claimed()).await;
        let still_open = seed_task(&pool, &claimed()).await;
        let open = seed_task(&pool, &claimed()).await;
        let core_pool = core_pool_for_test_url(pool.url());

        let before = live_row(&pool, closed).await;
        let passed = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(1))
            .expect("the monotonic clock runs past one second");
        let outcome = start_attempt_within(
            &core_pool,
            StatementLog::Off,
            &start(closed),
            StartWindow::Until(passed),
            crate::broker::child_writes::WriteDeadline::start_attempt(),
        )
        .await
        .expect("start");
        assert_eq!(outcome, WindowedStart::WindowClosed);
        assert_eq!(live_row(&pool, closed).await, before);
        assert_eq!(runner_beats(&pool, closed).await, 0);

        let later = std::time::Instant::now() + std::time::Duration::from_secs(60);
        for (task_id, window) in [
            (still_open, StartWindow::Until(later)),
            (open, StartWindow::Open),
        ] {
            let outcome = start_attempt_within(
                &core_pool,
                StatementLog::Off,
                &start(task_id),
                window,
                crate::broker::child_writes::WriteDeadline::start_attempt(),
            )
            .await
            .expect("start");
            assert!(
                matches!(outcome, WindowedStart::Ran(StartOutcome::Started { .. })),
                "{outcome:?}"
            );
            assert_eq!(status_of(&pool, task_id).await, "RUNNING");
        }
        cleanup_tasks(&pool, &[closed, still_open, open]).await;
    }
}

mod release {
    use super::*;

    /// A held claim goes back to PENDING with its class; a claim of another
    /// generation is not touched.
    #[tokio::test]
    async fn a_held_claim_is_released_and_another_generation_is_not() {
        let pool = pool().await;
        let held = seed_task(&pool, &claimed()).await;
        let reclaimed = seed_task(
            &pool,
            &TaskSeed {
                holder: Holder::Claim {
                    worker_id: WORKER.to_owned(),
                    claimed_at: generation() + Duration::seconds(1),
                },
                ..claimed()
            },
        )
        .await;
        let before = live_row(&pool, reclaimed).await;
        let command = ReleaseClaims {
            worker_id: WORKER.to_owned(),
            batch: ReleaseBatch::new(&[(held, generation()), (reclaimed, generation())])
                .expect("two claims"),
            placement: ReleasePlacement::SameClass,
        };

        let outcomes = release_claims(
            &core_pool_for_test_url(pool.url()),
            StatementLog::Off,
            &command,
        )
        .await
        .expect("release");

        assert_eq!(
            outcomes,
            vec![
                (held, ReleaseOutcome::Released),
                (reclaimed, ReleaseOutcome::NotHeld),
            ]
        );
        let row = live_row(&pool, held).await.expect("live row");
        assert_eq!(row["status"], "PENDING");
        assert_eq!(row["claimed_by_worker_id"], serde_json::Value::Null);
        assert_eq!(row["execution"], "ISOLATED");
        assert_eq!(live_row(&pool, reclaimed).await, before);
        assert!(live_attempts(&pool, held).await.is_empty());
        cleanup_tasks(&pool, &[held, reclaimed]).await;
    }

    /// A release clears every claim and run field and the finalizing
    /// marker: a released PENDING row names no worker, no claim and no run.
    #[tokio::test]
    async fn a_release_clears_every_claim_and_run_field() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &claimed()).await;
        sql::query(
            "UPDATE horsies_tasks
             SET started_at = NOW(), worker_pid = 321, worker_hostname = 'host1',
                 worker_process_name = 'worker', finalizing_at = NOW(),
                 finalizing_by_worker_id = $2
             WHERE id = $1",
        )
        .bind(task_id)
        .bind(WORKER)
        .execute(&pool)
        .await
        .expect("stamp the run fields");

        let outcomes = release_claims(
            &core_pool_for_test_url(pool.url()),
            StatementLog::Off,
            &release(task_id, ReleasePlacement::SameClass),
        )
        .await
        .expect("release");

        assert_eq!(outcomes, vec![(task_id, ReleaseOutcome::Released)]);
        let row = live_row(&pool, task_id).await.expect("live row");
        assert_eq!(row["status"], "PENDING");
        for field in [
            "claimed_at",
            "claimed_by_worker_id",
            "claim_expires_at",
            "started_at",
            "worker_pid",
            "worker_hostname",
            "worker_process_name",
            "finalizing_at",
            "finalizing_by_worker_id",
        ] {
            assert_eq!(
                row[field],
                serde_json::Value::Null,
                "a release must clear {field}"
            );
        }
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// The placement ISOLATED writes the class (Python's return of a row
    /// whose class does not fit its task).
    #[tokio::test]
    async fn an_isolated_release_writes_the_class() {
        let pool = pool().await;
        let task_id = seed_task(
            &pool,
            &TaskSeed {
                execution: "SHARED_ASYNC",
                ..claimed()
            },
        )
        .await;

        let outcomes = release_claims(
            &core_pool_for_test_url(pool.url()),
            StatementLog::Off,
            &release(task_id, ReleasePlacement::Isolated),
        )
        .await
        .expect("release");

        assert_eq!(outcomes, vec![(task_id, ReleaseOutcome::Released)]);
        let row = live_row(&pool, task_id).await.expect("live row");
        assert_eq!(row["status"], "PENDING");
        assert_eq!(row["execution"], "ISOLATED");
        cleanup_tasks(&pool, &[task_id]).await;
    }
}
