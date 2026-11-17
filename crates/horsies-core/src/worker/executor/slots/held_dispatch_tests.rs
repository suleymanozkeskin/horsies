//! Ports of Python `tests/unit/test_runner_heartbeat.py` rows that need an
//! isolated pool with scripted children: which dispatches the runner beats
//! hold when a slot goes down.

use std::sync::Arc;
use std::time::Duration;

use super::scripted::{
    act_on_runs, answer_of, boot, fail_boot, isolated_config, isolated_init, serving_pid, shell,
    short_of_descriptors, submit_isolated, task, ticket, until, TestDir, SPAWNER_STOP_WAIT,
    STATE_WAIT,
};
use super::*;
use crate::worker::executor::protocol::ProcessId;
use crate::worker::executor::supervisor::Spawner;
use crate::worker::runner_beats::LiveDispatches;

/// Python `test_a_slot_that_goes_down_holds_no_task`: the child of one slot
/// dies in its task, and its replacement cannot boot for a cause that can
/// clear (a resource shortage). That slot is down, and only the task of the
/// serving slot is held for the runner beats, on the serving child.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slot_that_goes_down_holds_no_task() {
    let dir = TestDir::new();
    let lost = task(1, ExecutionClass::Isolated);
    let running = task(2, ExecutionClass::Isolated);
    let init = isolated_init();
    // The first two children serve: the lost task ends its child, the
    // running task holds its child. Every later child is short of
    // descriptors at boot.
    let script = format!(
        "{}case $n in 1|2) {}{};; *) {};; esac",
        ticket(&dir),
        boot(&init),
        act_on_runs(
            &[
                (&lost, "exit 1".to_owned()),
                (&running, "exec sleep 60".to_owned()),
            ],
            1,
        ),
        fail_boot(&init, &short_of_descriptors()),
    );
    let spawner = Spawner::start().unwrap();
    let board = Arc::new(PoolBoard::new(ChildLayout {
        isolated: 2,
        shared_async: 0,
        shared_thread: 0,
    }));
    let live = LiveDispatches::default();
    let pool = IsolatedPool::start(
        isolated_config(shell(&script)),
        Arc::clone(&board),
        spawner.handle(),
        live.clone(),
    );
    until("both slots serve", STATE_WAIT, || pool.free_slots() == 2).await;

    let lost_end = answer_of(submit_isolated(&pool, &lost, IsolatedDeadline::NoDeadline)).await;
    assert!(
        matches!(lost_end, AttemptEnd::Crashed { .. }),
        "{lost_end:?}"
    );
    until("one slot is down", STATE_WAIT, || {
        board.down_slots().len() == 1
    })
    .await;
    let _running_end = submit_isolated(&pool, &running, IsolatedDeadline::NoDeadline);
    until("the running task is held", STATE_WAIT, || {
        live.held_task_ids() == vec![running.task_id]
    })
    .await;

    let serving_pids: Vec<ProcessId> = (0..board.slot_count())
        .filter_map(|index| serving_pid(&board, index))
        .collect();
    assert_eq!(board.down_slots().len(), 1, "the down slot stays down");
    assert_eq!(live.held_task_ids(), vec![running.task_id]);
    assert_eq!(
        live.held_child_pids(),
        serving_pids,
        "the held task runs on the one serving child"
    );

    let _ = pool.stop(Duration::ZERO).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
    assert!(
        live.held_task_ids().is_empty(),
        "the stop releases every held task"
    );
}
