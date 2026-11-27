//! The JSON of the worker routes: the slot health and the profile of a
//! worker state, and the execution class report.
//!
//! The worker routes serialize the broker's health types as they are
//! (`SlotHealth`, `SnapshotProfile`, `ExecutionClassReport`); these tests
//! hold their wire shape.

use chrono::{TimeDelta, TimeZone, Utc};
use serde_json::json;

use crate::broker::{
    BudgetLoad, ClearableBootCause, ExecutionClassReport, PendingStream, ShortageErrno, SlotHealth,
    SnapshotProfile, StreamCoverageState, StreamWait, WorkerExecutionClassLoad,
};
use crate::core::task::Execution;

#[test]
fn all_slots_serving_is_a_tag_with_no_slot() {
    let body = serde_json::to_value(SlotHealth::AllServing).expect("serialize");

    assert_eq!(body, json!({"kind": "all_serving"}));
}

#[test]
fn down_slots_are_the_slots_of_their_tag_with_the_cause_as_text() {
    let stored = json!([{
        "slot": 1,
        "cause": {"kind": "resource_shortage", "errno": "EMFILE"},
        "detail": "OSError: [Errno 24] Too many open files",
        "down_since": "2026-11-10T12:00:00+00:00",
        "failed_boots": 2,
    }]);
    let health = crate::broker::slot_health::parse_slot_health(&stored, 2).expect("readable");
    let SlotHealth::SlotsDown { slots } = &health else {
        panic!("one down slot");
    };
    assert_eq!(
        slots.as_slice()[0].cause,
        ClearableBootCause::ResourceShortage(ShortageErrno::Emfile)
    );

    let body = serde_json::to_value(&health).expect("serialize");

    assert_eq!(
        body,
        json!({
            "kind": "slots_down",
            "slots": [{
                "slot": 1,
                "cause": "the OS ran short of a resource (EMFILE)",
                "detail": "OSError: [Errno 24] Too many open files",
                "down_since": "2026-11-10T12:00:00Z",
                "failed_boots": 2,
            }],
        })
    );
}

#[test]
fn a_profile_is_tagged_by_kind() {
    let named = SnapshotProfile::Named {
        name: "batch".to_owned(),
    };

    assert_eq!(
        serde_json::to_value(&named).expect("serialize"),
        json!({"kind": "named", "name": "batch"})
    );
    assert_eq!(
        serde_json::to_value(&SnapshotProfile::NotRecorded).expect("serialize"),
        json!({"kind": "not_recorded"})
    );
}

#[test]
fn a_report_serializes_with_tagged_coverage_and_wait() {
    let at = Utc.with_ymd_and_hms(2026, 11, 10, 12, 0, 0).unwrap();
    let report = ExecutionClassReport {
        checked_at: at,
        live_window: TimeDelta::seconds(120),
        streams: vec![
            PendingStream {
                queue: "a".to_owned(),
                execution: Execution::Isolated,
                coverage: StreamCoverageState::Served {
                    workers: 2,
                    capacity: 6,
                },
                wait: StreamWait::Due {
                    oldest_due_at: at - TimeDelta::milliseconds(90_500),
                    wait: TimeDelta::milliseconds(90_500),
                },
            },
            PendingStream {
                queue: "a".to_owned(),
                execution: Execution::SharedAsync,
                coverage: StreamCoverageState::NoCapacity { workers: 1 },
                wait: StreamWait::NoneDue {
                    next_due_at: at + TimeDelta::minutes(1),
                },
            },
            PendingStream {
                queue: "b".to_owned(),
                execution: Execution::SharedThread,
                coverage: StreamCoverageState::Unserved,
                wait: StreamWait::NoneDue {
                    next_due_at: at + TimeDelta::minutes(1),
                },
            },
        ],
        workers: vec![WorkerExecutionClassLoad {
            worker_id: "w1".to_owned(),
            snapshot_at: at,
            budgets: vec![BudgetLoad {
                budget: Execution::Isolated,
                capacity: 2,
                serves: vec![Execution::Isolated, Execution::SharedThread],
                in_flight: 5,
            }],
        }],
    };

    let body = serde_json::to_value(&report).expect("serialize");

    assert_eq!(
        body,
        json!({
            "checked_at": "2026-11-10T12:00:00Z",
            "live_window_seconds": 120.0,
            "streams": [
                {
                    "queue": "a",
                    "execution": "ISOLATED",
                    "coverage": {"kind": "served", "workers": 2, "capacity": 6},
                    "wait": {
                        "kind": "due",
                        "oldest_due_at": "2026-11-10T11:58:29.500000Z",
                        "wait_seconds": 90.5,
                    },
                },
                {
                    "queue": "a",
                    "execution": "SHARED_ASYNC",
                    "coverage": {"kind": "no_capacity", "workers": 1},
                    "wait": {"kind": "none_due", "next_due_at": "2026-11-10T12:01:00Z"},
                },
                {
                    "queue": "b",
                    "execution": "SHARED_THREAD",
                    "coverage": {"kind": "unserved"},
                    "wait": {"kind": "none_due", "next_due_at": "2026-11-10T12:01:00Z"},
                },
            ],
            "workers": [{
                "worker_id": "w1",
                "snapshot_at": "2026-11-10T12:00:00Z",
                "budgets": [{
                    "budget": "ISOLATED",
                    "capacity": 2,
                    "serves": ["ISOLATED", "SHARED_THREAD"],
                    "in_flight": 5,
                }],
            }],
        })
    );
}
