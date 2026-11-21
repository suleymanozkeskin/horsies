//! The statement of a plain-success batch on the test database
//! (`worker::finalize_batch::statement`, `horsies_complete_tasks_fused` of
//! 0004), and the connection rows of the batcher that need a real pool.

use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use super::support::*;
use crate::broker::core_rows::StatementLog;
use crate::broker::pools::{core_pool_for_test_url, worker_core_pool_for_test_url};
use crate::core::lifecycle::{TerminalizationKind, TerminalizationOutcome};
use crate::db::{CheckoutClass, StatementBound, Transience, TypedStatement};
use crate::test_support::sql::{self, TestConnection};
use crate::worker::finalize_batch::statement::complete_plain_successes;
use crate::worker::finalize_batch::{BatchWriteError, PlainSuccess, SuccessBatch};

/// Polls of a wait for the pool state: 100 polls of 20 ms.
const POOL_WAIT_POLLS: u32 = 100;
const POOL_WAIT_INTERVAL: Duration = Duration::from_millis(20);

fn success(task_id: Uuid) -> PlainSuccess {
    PlainSuccess {
        task_id,
        claimed_at: generation(),
        result_json: r#"{"__type":"ok","value":1}"#.to_owned(),
    }
}

fn batch_of(task_ids: &[Uuid]) -> SuccessBatch {
    SuccessBatch::new(task_ids.iter().map(|task_id| success(*task_id)).collect())
        .expect("a test batch inside its bound")
}

/// One statement answers each row with the
/// outcome of its own task, in input order, whatever order the function
/// writes the rows in (task id order).
#[tokio::test]
async fn each_row_gets_the_outcome_of_its_own_task_in_input_order() {
    let pool = pool().await;
    let mine = seed_task(&pool, &TaskSeed::running()).await;
    let theirs = seed_task(
        &pool,
        &TaskSeed {
            holder: Holder::other_worker(),
            ..TaskSeed::running()
        },
    )
    .await;
    let absent = Uuid::new_v4();
    let core = core_pool_for_test_url(pool.url());

    let outcomes = complete_plain_successes(
        &core,
        StatementLog::Off,
        WORKER,
        &batch_of(&[theirs, absent, mine]),
    )
    .await
    .expect("the batch statement answers every row")
    .into_outcomes();

    match &outcomes[..] {
        [TerminalizationOutcome::LostClaim { task_id: lost, .. }, TerminalizationOutcome::TaskAbsent {
            task_id: missing, ..
        }, TerminalizationOutcome::Applied {
            task_id: applied,
            kind: TerminalizationKind::CompleteFused,
            ..
        }] => {
            assert_eq!(*lost, theirs);
            assert_eq!(*missing, absent);
            assert_eq!(*applied, mine);
        }
        other => panic!("expected LostClaim, TaskAbsent, Applied in input order, got {other:?}"),
    }
    let history = history_row(&pool, mine)
        .await
        .expect("the applied row moved");
    assert_eq!(history.status, "COMPLETED");
    assert!(live_row(&pool, theirs).await.is_some());
    cleanup_tasks(&pool, &[mine, theirs]).await;
}

/// A batch sent again after its commit (the replay after an unknown
/// outcome) answers each committed row `AlreadyApplied` and writes nothing.
#[tokio::test]
async fn a_replay_of_a_committed_batch_answers_already_applied() {
    let pool = pool().await;
    let first = seed_task(&pool, &TaskSeed::running()).await;
    let second = seed_task(&pool, &TaskSeed::running()).await;
    let core = core_pool_for_test_url(pool.url());
    let batch = batch_of(&[first, second]);

    let _written = complete_plain_successes(&core, StatementLog::Off, WORKER, &batch)
        .await
        .expect("the first write");
    let replayed = complete_plain_successes(&core, StatementLog::Off, WORKER, &batch)
        .await
        .expect("the replay")
        .into_outcomes();

    assert!(
        replayed
            .iter()
            .all(|outcome| matches!(outcome, TerminalizationOutcome::AlreadyApplied { .. })),
        "{replayed:?}"
    );
    cleanup_tasks(&pool, &[first, second]).await;
}

static RAISE_DEADLOCK: TypedStatement = TypedStatement::new(
    "raise_deadlock",
    "DO $$ BEGIN RAISE EXCEPTION 'a deadlock for the test' USING ERRCODE = '40P01'; END $$",
    &[],
);

static REFUSED_BATCH_ARGUMENTS: TypedStatement = TypedStatement::new(
    "refused_batch_arguments",
    "SELECT * FROM horsies_complete_tasks_fused(NULL, NULL, NULL, NULL, TRUE)",
    &[],
);

/// The server half of Python `test_the_retryable_flag_follows_the_cause`: a
/// deadlock refusal (40P01) is transient; the batch function's argument
/// refusal (22023) is not.
#[tokio::test]
async fn a_server_refusal_keeps_the_transience_of_its_sqlstate() {
    let pool = pool().await;
    let core = core_pool_for_test_url(pool.url());

    for (statement, transience) in [
        (&RAISE_DEADLOCK, Transience::Transient),
        (&REFUSED_BATCH_ARGUMENTS, Transience::Permanent),
    ] {
        let failure = core
            .query(
                CheckoutClass::Batch,
                &statement.bind().finish(),
                StatementBound::ConnectionTimeouts,
            )
            .await
            .expect_err("the server refuses the statement");
        let error = BatchWriteError::from(&failure);
        assert!(
            matches!(error, BatchWriteError::Refused { .. }),
            "{}: {error:?}",
            statement.name()
        );
        assert_eq!(error.transience(), transience, "{}", statement.name());
    }
}

/// Python `test_the_worker_holds_a_connection_for_claims_and_one_per_steady_flusher`,
/// for the core's per-batch checkout: the write of a batch checks out one
/// connection of the worker pool for its statement and gives it back when
/// the statement ends; between batches it holds none.
///
/// The statement waits on the task's advisory lock, which the test holds, so
/// the pool is read while the statement runs.
#[tokio::test]
async fn a_batch_holds_its_connection_only_while_its_statement_runs() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &TaskSeed::running()).await;
    let core = Arc::new(worker_core_pool_for_test_url(pool.url(), 5));
    let mut holder = TestConnection::connect(pool.url())
        .await
        .expect("connect the lock holder");
    sql::query("SELECT pg_advisory_lock(hashtextextended($1::text, 731))")
        .bind(task_id)
        .execute(&mut holder)
        .await
        .expect("hold the task's advisory lock");

    let writer = Arc::clone(&core);
    let write = tokio::spawn(async move {
        complete_plain_successes(&writer, StatementLog::Off, WORKER, &batch_of(&[task_id])).await
    });
    let mut in_use = 0;
    for _ in 0..POOL_WAIT_POLLS {
        let status = core.status();
        in_use = status.open - status.idle;
        if in_use == 1 {
            break;
        }
        tokio::time::sleep(POOL_WAIT_INTERVAL).await;
    }
    assert_eq!(
        in_use, 1,
        "the statement holds one connection while it runs"
    );
    assert!(!write.is_finished());

    sql::query("SELECT pg_advisory_unlock(hashtextextended($1::text, 731))")
        .bind(task_id)
        .execute(&mut holder)
        .await
        .expect("release the task's advisory lock");
    let outcomes = tokio::time::timeout(Duration::from_secs(10), write)
        .await
        .expect("the write ends once the lock is free")
        .expect("the write task ended without a panic")
        .expect("the batch statement answers its row")
        .into_outcomes();

    assert!(
        matches!(&outcomes[..], [TerminalizationOutcome::Applied { .. }]),
        "{outcomes:?}"
    );
    let status = core.status();
    assert_eq!(
        status.open, status.idle,
        "after its statement the write holds no connection"
    );
    cleanup_tasks(&pool, &[task_id]).await;
}
