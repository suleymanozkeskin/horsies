//! The placement watch of a submitted task: the child that took the task,
//! told once, with the kind of the child; a task that reached no child is
//! not placed; the placement that the attempt row keeps.

use std::time::Duration;

use super::scripted::{
    answer_of, answer_runs, boot, idle_child, input, isolated_config, isolated_init, queued,
    reported, result_frame, serving, shell, start_isolated, task, ANSWER_WAIT, SPAWNER_STOP_WAIT,
};
use super::*;
use crate::worker::executor::protocol::IsolatedWork;
use crate::worker::executor::supervisor::Spawner;

async fn stop_pool(pool: IsolatedPool, spawner: Spawner) {
    let _report = pool.stop(Duration::from_secs(5)).await;
    let _stopped = spawner.stop(SPAWNER_STOP_WAIT);
}

/// A task of a shared class that its route sends to the isolated pool runs
/// on an isolated child: the placement names that child and the kind
/// `ISOLATED`, once, before the end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_watched_task_is_placed_once_on_the_child_that_took_it() {
    let routed = task(1, ExecutionClass::SharedAsync);
    let script = format!(
        "{}{}",
        boot(&isolated_init()),
        answer_runs(&[(&routed, vec![result_frame(&routed, 1)])], 1)
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));
    let pid = serving(&board, 0).await;
    let (watch, receipt) = watched_placement();
    let before = tokio::time::Instant::now();

    let answer = queued(pool.submit_watched(
        &routed,
        input(),
        IsolatedWork::Run,
        IsolatedDeadline::NoDeadline,
        watch,
    ));
    let placement = tokio::time::timeout(ANSWER_WAIT, receipt.placement())
        .await
        .expect("the placement is told within the test wait");

    match placement {
        ChildPlacement::Placed(placed) => {
            assert_eq!(placed.child_pid, pid);
            assert_eq!(placed.child_kind, ExecutionClass::Isolated);
            assert!(placed.at >= before);
        }
        other => panic!("the child took the task, not {other:?}"),
    }
    assert_eq!(answer_of(answer).await, reported(1));
    stop_pool(pool, spawner).await;
}

/// A task that the pool refuses (the pools stop) reaches no child: its
/// placement is `NotPlaced`, at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_that_reaches_no_child_is_not_placed() {
    let refused = task(1, ExecutionClass::Isolated);
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(
        &spawner,
        1,
        isolated_config(shell(&idle_child(&isolated_init()))),
    );
    board.begin_stop();
    let (watch, receipt) = watched_placement();

    let submitted = pool.submit_watched(
        &refused,
        input(),
        IsolatedWork::Run,
        IsolatedDeadline::NoDeadline,
        watch,
    );
    let placement = tokio::time::timeout(ANSWER_WAIT, receipt.placement())
        .await
        .expect("an untold placement ends at once");

    assert!(
        matches!(
            submitted,
            Submitted::NotDispatched(NotRunCause::PoolStopping)
        ),
        "{submitted:?}"
    );
    assert_eq!(placement, ChildPlacement::NotPlaced);
    stop_pool(pool, spawner).await;
}

/// A placed task gives its attempt row the slot wait from the read of the
/// claim result to the placement.
#[test]
fn a_placed_task_gives_its_attempt_the_slot_wait_from_the_read() {
    use crate::broker::claims::ClaimRead;
    use crate::core::lifecycle::{AttemptPlacement, SlotWait};
    use crate::worker::executor::protocol::ProcessId;
    let read = ClaimRead::now();
    let placed = Placed {
        child_pid: ProcessId::new(4242).unwrap(),
        child_kind: ExecutionClass::Isolated,
        at: read.at + Duration::from_micros(2_500),
        wall: read.wall,
    };

    let placement = ChildPlacement::Placed(placed).attempt_placement(read);

    assert_eq!(
        placement,
        AttemptPlacement::Placed {
            slot_wait: SlotWait::between(read.at, placed.at),
        }
    );
    assert_eq!(placement.slot_wait_us(), Some(2_500));
}

/// A task that reached no child gives its attempt row no placement.
#[test]
fn a_task_that_reached_no_child_gives_no_attempt_placement() {
    use crate::broker::claims::ClaimRead;
    use crate::core::lifecycle::AttemptPlacement;
    assert_eq!(
        ChildPlacement::NotPlaced.attempt_placement(ClaimRead::now()),
        AttemptPlacement::NotObserved
    );
}
