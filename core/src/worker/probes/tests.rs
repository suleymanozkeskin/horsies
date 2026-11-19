//! The answers of `/livez` and `/readyz` in each phase of the run, and the
//! claim-loop deadline.

use super::*;
use crate::worker::executor::protocol::ProcessId;
use crate::worker::executor::slots::{ChildLayout, PoolFailure, SlotPhase};

/// A board of two isolated slots that both boot.
fn booting_board() -> Arc<PoolBoard> {
    Arc::new(PoolBoard::new(ChildLayout {
        isolated: 2,
        shared_async: 0,
        shared_thread: 0,
    }))
}

fn serving_phase() -> SlotPhase {
    SlotPhase::Serving {
        pid: ProcessId::new(4242).expect("4242 is a process id"),
    }
}

#[test]
fn the_start_and_the_stop_are_live_and_not_ready() {
    let probes = WorkerProbes::starting();
    assert_eq!(probes.live(), LiveAnswer::Starting);
    assert_eq!(probes.live().status(), ProbeStatus::Pass);
    assert_eq!(probes.ready(), ReadyAnswer::Starting);
    assert_eq!(probes.ready().status(), ProbeStatus::Fail);

    probes.stopping();
    assert_eq!(probes.live(), LiveAnswer::Stopping);
    assert_eq!(probes.live().status(), ProbeStatus::Pass);
    assert_eq!(probes.ready(), ReadyAnswer::Stopping);
    assert_eq!(probes.ready().status(), ProbeStatus::Fail);
}

#[test]
fn the_claim_loop_is_live_until_the_deadline_of_its_next_turn() {
    let origin = Instant::now();
    let turns = ClaimTurns::new(origin);
    let within = Duration::from_secs(10);
    turns.turn_within(origin, within);
    assert_eq!(turns.check(origin), TurnCheck::OnTime);
    assert_eq!(turns.check(origin + within), TurnCheck::OnTime);
    assert_eq!(
        turns.check(origin + within + Duration::from_millis(1)),
        TurnCheck::Late(Duration::from_millis(1))
    );

    // A later turn moves the deadline.
    turns.turn_within(origin + within, within);
    assert_eq!(
        turns.check(origin + within + Duration::from_millis(1)),
        TurnCheck::OnTime
    );

    assert_eq!(
        live_of_turn(TurnCheck::OnTime),
        LiveAnswer::ClaimLoopTurning
    );
    let late = live_of_turn(TurnCheck::Late(Duration::from_secs(2)));
    assert_eq!(
        late,
        LiveAnswer::ClaimLoopLate {
            late_by: Duration::from_secs(2)
        }
    );
    assert_eq!(late.status(), ProbeStatus::Fail);
    assert!(
        late.text().contains("2.000s past the deadline"),
        "{}",
        late.text()
    );
}

#[test]
fn a_claim_loop_that_just_turned_is_live() {
    let probes = WorkerProbes::starting();
    probes.claiming(ServingFacts {
        board: booting_board(),
        lock: LockHoldView::held_for_test(),
    });
    assert_eq!(probes.live(), LiveAnswer::ClaimLoopTurning);
    probes.claim_turn_within(Duration::from_secs(5));
    assert_eq!(probes.live(), LiveAnswer::ClaimLoopTurning);
}

#[test]
fn readiness_needs_serving_pools_a_serving_child_and_the_lock() {
    let held = || LockHold::Held;
    assert_eq!(
        ready_of(PoolHealth::Failed(PoolFailure::ChildHookFailed), 2, held()),
        ReadyAnswer::PoolsFailed
    );
    assert_eq!(
        ready_of(PoolHealth::Stopping, 2, held()),
        ReadyAnswer::Stopping
    );
    assert_eq!(
        ready_of(PoolHealth::Serving, 0, held()),
        ReadyAnswer::NoChildServing
    );
    assert_eq!(
        ready_of(
            PoolHealth::Serving,
            1,
            LockHold::NotHeld {
                cause: "connection lost".to_owned()
            }
        ),
        ReadyAnswer::LockNotHeld {
            cause: "connection lost".to_owned()
        }
    );
    assert_eq!(ready_of(PoolHealth::Serving, 1, held()), ReadyAnswer::Ready);
    assert_eq!(ReadyAnswer::Ready.status(), ProbeStatus::Pass);
    for answer in [
        ReadyAnswer::Starting,
        ReadyAnswer::Stopping,
        ReadyAnswer::PoolsFailed,
        ReadyAnswer::NoChildServing,
        ReadyAnswer::LockNotHeld {
            cause: "lost".to_owned(),
        },
    ] {
        assert_eq!(answer.status(), ProbeStatus::Fail, "{answer:?}");
    }
}

#[test]
fn the_readiness_of_the_claim_loop_reads_the_board_and_the_lock() {
    let board = booting_board();
    let probes = WorkerProbes::starting();
    probes.claiming(ServingFacts {
        board: Arc::clone(&board),
        lock: LockHoldView::held_for_test(),
    });
    assert_eq!(probes.ready(), ReadyAnswer::NoChildServing);

    board.set(0, serving_phase());
    assert_eq!(probes.ready(), ReadyAnswer::Ready);

    let lost = WorkerProbes::starting();
    lost.claiming(ServingFacts {
        board: Arc::clone(&board),
        lock: LockHoldView::lost_for_test("the session ended"),
    });
    assert_eq!(
        lost.ready(),
        ReadyAnswer::LockNotHeld {
            cause: "the session ended".to_owned()
        }
    );

    board.fail(PoolFailure::ChildHookFailed);
    assert_eq!(probes.ready(), ReadyAnswer::PoolsFailed);
}
