//! The last RSS reading of the child of each slot, on the board: the ready
//! frame of the child, then each result frame of the same child, for the
//! worker snapshot. A slot with no serving child has none, so a replaced
//! child's reading leaves with it. No process: the tests set the board as
//! the pools do.

use std::num::NonZeroU32;

use chrono::Utc;

use super::*;
use crate::worker::executor::protocol::{ProcessId, RssMb, RssReading};
use crate::worker::executor::supervisor::BootFailureCause;

fn pid(value: u32) -> ProcessId {
    ProcessId::new(value).unwrap()
}

fn mib(value: f64) -> RssReading {
    RssReading::Mb(RssMb::new(value).unwrap())
}

/// A board of one slot of each kind: slot 0 isolated, slot 1 shared
/// async, slot 2 shared thread.
fn one_of_each() -> PoolBoard {
    PoolBoard::new(ChildLayout {
        isolated: 1,
        shared_async: 1,
        shared_thread: 1,
    })
}

fn reading(slot: usize, class: ExecutionClass, child: u32, rss: RssReading) -> ChildRss {
    ChildRss {
        slot,
        class,
        pid: pid(child),
        rss,
    }
}

/// A slot that boots has no reading; a ready frame gives the first one, of
/// each kind of child, an unreadable one included.
#[test]
fn a_ready_frame_gives_the_first_reading_of_a_child_of_each_kind() {
    let board = one_of_each();
    assert_eq!(board.child_rss(), Vec::new());

    board.ready(
        0,
        ReadyChild {
            pid: pid(101),
            rss: mib(10.0),
        },
    );
    board.ready(
        1,
        ReadyChild {
            pid: pid(102),
            rss: RssReading::Unreadable,
        },
    );
    board.ready(
        2,
        ReadyChild {
            pid: pid(103),
            rss: mib(30.0),
        },
    );

    assert_eq!(
        board.child_rss(),
        vec![
            reading(0, ExecutionClass::Isolated, 101, mib(10.0)),
            reading(1, ExecutionClass::SharedAsync, 102, RssReading::Unreadable),
            reading(2, ExecutionClass::SharedThread, 103, mib(30.0)),
        ]
    );
}

/// A result frame of the serving child replaces its reading, an unreadable
/// one too; a reading of another process is dropped.
#[test]
fn a_result_frame_of_the_serving_child_is_its_last_reading() {
    let board = one_of_each();
    board.ready(
        0,
        ReadyChild {
            pid: pid(101),
            rss: mib(10.0),
        },
    );

    board.note_rss(0, pid(101), mib(12.5));
    assert_eq!(
        board.child_rss(),
        vec![reading(0, ExecutionClass::Isolated, 101, mib(12.5))]
    );

    board.note_rss(0, pid(101), RssReading::Unreadable);
    assert_eq!(
        board.child_rss(),
        vec![reading(
            0,
            ExecutionClass::Isolated,
            101,
            RssReading::Unreadable
        )]
    );

    board.note_rss(0, pid(999), mib(99.0));
    board.note_rss(1, pid(102), mib(99.0));
    assert_eq!(
        board.child_rss(),
        vec![reading(
            0,
            ExecutionClass::Isolated,
            101,
            RssReading::Unreadable
        )]
    );
}

/// A shared child that serves and then closes keeps its reading; a slot
/// that retires, boots or goes down has none, and the next child starts
/// with the reading of its own ready frame. The reading of the replaced
/// child never comes back.
#[test]
fn a_replaced_child_takes_its_reading_with_it() {
    let board = one_of_each();
    board.ready(
        1,
        ReadyChild {
            pid: pid(102),
            rss: mib(40.0),
        },
    );
    board.set(1, SlotPhase::Serving { pid: pid(102) });
    board.set(
        1,
        SlotPhase::Closing {
            pid: pid(102),
            runs: 1,
        },
    );
    assert_eq!(
        board.child_rss(),
        vec![reading(1, ExecutionClass::SharedAsync, 102, mib(40.0))]
    );

    board.set(1, SlotPhase::Retiring);
    assert_eq!(board.child_rss(), Vec::new());
    board.note_rss(1, pid(102), mib(41.0));
    assert_eq!(board.child_rss(), Vec::new());

    board.set(1, SlotPhase::Booting);
    board.ready(
        1,
        ReadyChild {
            pid: pid(202),
            rss: mib(20.0),
        },
    );
    assert_eq!(
        board.child_rss(),
        vec![reading(1, ExecutionClass::SharedAsync, 202, mib(20.0))]
    );

    board.set(
        1,
        SlotPhase::Down(DownState {
            cause: BootFailureCause::ThreadStartRefused,
            detail: "can't start new thread".to_owned(),
            down_since: Utc::now(),
            failed_boots: NonZeroU32::MIN,
        }),
    );
    assert_eq!(board.child_rss(), Vec::new());
}

/// A phase that names another child drops the reading of the earlier one.
#[test]
fn a_phase_of_another_child_drops_the_reading() {
    let board = one_of_each();
    board.ready(
        2,
        ReadyChild {
            pid: pid(103),
            rss: mib(30.0),
        },
    );
    board.set(2, SlotPhase::Serving { pid: pid(303) });
    assert_eq!(board.child_rss(), Vec::new());
}
