//! The wake of the claim loop when a child becomes ready after a boot
//! (#1148). An isolated slot notes it on the board when it waits for its
//! first task of a generation; a shared slot notes it the first time its
//! child is open with a free seat. The note comes after the slot counts as
//! free, so the claim pass that it wakes places a task there.
//!
//! Conditions of the issue: `max_tasks_per_child = 1` and one isolated
//! slot. The task ends; the replacement child boots after the outcome write
//! woke the claim loop, so that pass found no free slot. Without this wake,
//! the next pass comes at the poll interval.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use super::scripted::{
    act_on_runs, answer_of, answer_runs, async_work, boot, ended, isolated_config, isolated_init,
    layout, reported, result_frame, run_ref, send, serving, shared_async_init, shared_config,
    shared_result, shell, start_isolated, start_shared, submit_isolated, submit_shared, task,
    ticket, tickets, TestDir, SPAWNER_STOP_WAIT, STATE_WAIT,
};
use super::*;
use crate::worker::executor::protocol::SharedWork;
use crate::worker::executor::supervisor::Spawner;

/// The seats of the shared child of the shared test.
const SEATS: u64 = 2;

/// Wait for the next child-ready wake of `board`; fail the test after
/// [`STATE_WAIT`].
async fn ready_wake(board: &PoolBoard, what: &str) {
    tokio::time::timeout(STATE_WAIT, board.child_ready())
        .await
        .unwrap_or_else(|_elapsed| panic!("no child-ready wake within {STATE_WAIT:?}: {what}"));
}

/// A run with a long deadline: no backstop comes due in the test.
fn long_work() -> SharedWork {
    async_work(60_000, 1_000)
}

/// Isolated: the first child and its replacement after a recycle each wake
/// the claim loop once they wait for a task, and the slot counts as free
/// when the wake comes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replacement_isolated_child_wakes_the_claim_loop_when_it_waits() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let init = isolated_init();
    let script = format!(
        "{}{}{}",
        ticket(&dir),
        boot(&init),
        answer_runs(&[(&first, vec![result_frame(&first, 1)])], 1),
    );
    let spawner = Spawner::start().unwrap();
    let config = IsolatedPoolConfig {
        tasks_per_child: TaskLimit::After(NonZeroU64::MIN),
        ..isolated_config(shell(&script))
    };
    let (pool, board) = start_isolated(&spawner, 1, config);

    ready_wake(&board, "the first child waits for a task").await;
    assert_eq!(
        pool.free_slots(),
        1,
        "the wake comes after the slot is free"
    );
    let first_child = serving(&board, 0).await;
    let answer = answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;
    assert_eq!(answer, reported(1));
    ready_wake(&board, "the replacement child waits for a task").await;

    assert_eq!(
        pool.free_slots(),
        1,
        "the wake comes after the slot is free"
    );
    assert_eq!(tickets(&dir), 2, "a replacement child booted");
    ended(first_child.as_pid_t()).await;
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// Shared: the first child and its replacement after a recycle each wake
/// the claim loop once they are open with a free seat, and the seats count
/// as free when the wake comes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replacement_shared_child_wakes_the_claim_loop_when_it_opens() {
    let dir = TestDir::new();
    let tasks: Vec<ClaimedTask> = (1..=2)
        .map(|number| task(number, ExecutionClass::SharedAsync))
        .collect();
    let actions: Vec<(&ClaimedTask, String)> = tasks
        .iter()
        .zip(1_u32..)
        .map(|(task, value)| {
            (
                task,
                send(&[shared_result(run_ref(task, u64::from(value)), value)]),
            )
        })
        .collect();
    let script = format!(
        "{}{}{}",
        ticket(&dir),
        boot(&shared_async_init(SEATS)),
        act_on_runs(&actions, tasks.len()),
    );
    let spawner = Spawner::start().unwrap();
    // One task per seat: the child closes after two tasks and retires.
    let config = SharedPoolConfig {
        tasks_per_seat: TaskLimit::After(NonZeroU64::MIN),
        ..shared_config(shell(&script), shared_async_init(SEATS))
    };
    let board = Arc::new(PoolBoard::new(layout(0, 1, 0)));
    let pool = start_shared(&spawner, &board, config);

    ready_wake(&board, "the first child is open").await;
    assert_eq!(
        pool.free_seats(),
        2,
        "the wake comes after the seats are free"
    );
    let first_child = serving(&board, 0).await;
    for (task, value) in tasks.iter().zip(1_u32..) {
        assert_eq!(
            answer_of(submit_shared(&pool, task, long_work())).await,
            reported(value)
        );
    }
    ready_wake(&board, "the replacement child is open").await;

    assert_eq!(
        pool.free_seats(),
        2,
        "the wake comes after the seats are free"
    );
    assert_eq!(tickets(&dir), 2, "a replacement child booted");
    ended(first_child.as_pid_t()).await;
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}
