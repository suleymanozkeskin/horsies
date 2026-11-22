//! Slot defects of shared slots: a run frame that
//! cannot be encoded costs the child and every run of it, and three
//! defects in a row fail the pools. The isolated rows are in
//! `isolated_stop_tests.rs`.

use std::sync::Arc;
use std::time::Duration;

use super::scripted::{
    answer_of, async_work, boot, ended, layout, read_frame, serving, shared_async_init,
    shared_config, shell, start_shared, submit_shared, task, ticket, tickets, until, Shown,
    TestDir, SPAWNER_STOP_WAIT, STATE_WAIT,
};
use super::*;
use crate::worker::executor::supervisor::Spawner;

/// A task whose shared run frame cannot be encoded: its metadata is over
/// the bound.
fn unsendable(number: u128) -> ClaimedTask {
    ClaimedTask {
        task_name: "x".repeat(2 << 20),
        ..task(number, ExecutionClass::SharedAsync)
    }
}

fn crash_reason(end: AttemptEnd) -> String {
    match end {
        AttemptEnd::Crashed { reason } => reason.into_string(),
        other => panic!("expected the crash path, got {other:?}"),
    }
}

async fn failure_of(board: &PoolBoard) -> PoolFailure {
    until(
        "the pool failed",
        STATE_WAIT,
        &[Shown::Board(board)],
        || matches!(board.health(), PoolHealth::Failed(_)),
    )
    .await;
    match board.health() {
        PoolHealth::Failed(failure) => failure,
        other => panic!("expected a failed pool, got {other:?}"),
    }
}

/// Python `TestSlotDefects::test_a_defect_costs_only_its_task`, for a
/// shared child: the defect kills the child, every run of it takes the
/// crash path with the defect as its cause, and the slot boots a new
/// child.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_frame_that_cannot_be_encoded_costs_the_runs_of_its_child() {
    let dir = TestDir::new();
    let init = shared_async_init(2);
    let script = format!(
        "{}{}{}exec sleep 60",
        ticket(&dir),
        boot(&init),
        read_frame()
    );
    let spawner = Spawner::start().unwrap();
    let board = Arc::new(PoolBoard::new(layout(0, 1, 0)));
    let pool = start_shared(&spawner, &board, shared_config(shell(&script), init));
    let first_child = serving(&board, 0).await;

    let neighbour = submit_shared(
        &pool,
        &task(1, ExecutionClass::SharedAsync),
        async_work(60_000, 1_000),
    );
    let defect = submit_shared(&pool, &unsendable(2), async_work(60_000, 1_000));
    let neighbour_reason = crash_reason(answer_of(neighbour).await);
    let defect_reason = crash_reason(answer_of(defect).await);

    assert!(
        neighbour_reason.contains("the worker slot failed"),
        "{neighbour_reason}"
    );
    assert!(
        defect_reason.contains("the worker slot failed"),
        "{defect_reason}"
    );
    ended(first_child.as_pid_t()).await;
    until(
        "the slot boots a new child",
        STATE_WAIT,
        &[Shown::Board(&board), Shown::Tickets(&dir)],
        || tickets(&dir) == 2,
    )
    .await;
    assert_eq!(board.health(), PoolHealth::Serving);
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// Python `TestSlotDefects::test_repeated_defects_fail_the_pool`, for a
/// shared slot: the failure names the slot by its board index.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_shared_frame_defects_in_a_row_fail_the_pools() {
    let init = shared_async_init(1);
    let script = format!("{}{}exec sleep 60", boot(&init), read_frame());
    let spawner = Spawner::start().unwrap();
    // Slot 0 is an isolated slot that no pool serves; the shared slot is
    // slot 1 on the board.
    let board = Arc::new(PoolBoard::new(layout(1, 1, 0)));
    let pool = start_shared(&spawner, &board, shared_config(shell(&script), init));

    for number in 1..=2 {
        let reason = crash_reason(
            answer_of(submit_shared(
                &pool,
                &unsendable(number),
                async_work(60_000, 1_000),
            ))
            .await,
        );
        assert!(reason.contains("the worker slot failed"), "{reason}");
    }
    assert_eq!(board.health(), PoolHealth::Serving);
    let _ = answer_of(submit_shared(
        &pool,
        &unsendable(3),
        async_work(60_000, 1_000),
    ))
    .await;

    match failure_of(&board).await {
        PoolFailure::SlotDefectsRepeated { slot, detail } => {
            assert_eq!(slot, 1);
            assert!(detail.as_str().contains("could not be encoded"), "{detail}");
        }
        other => panic!("expected repeated slot defects, got {other:?}"),
    }
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}
