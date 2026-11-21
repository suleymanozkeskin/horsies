//! One test per outcome of the pure decode of the fleet read: each part from
//! its statement row, each bound at its limit and one past it.

use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use serde_json::{json, Value};

use super::*;
use crate::telemetry::labels::{
    FleetWorkerState, LiveTaskStatus, LiveWorkflowStatus, TelemetryPart,
};
use crate::{StreamWait, LIVE_SNAPSHOT_ROW_LIMIT};

const ISOLATED: Execution = Execution::Isolated;
const ASYNC: Execution = Execution::SharedAsync;
const THREAD: Execution = Execution::SharedThread;

fn at() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 19, 12, 0, 0)
        .single()
        .expect("a valid test instant")
}

fn pending_entry(queue: &str, execution: &str, counted: u64) -> PendingEntry {
    (queue.to_owned(), execution.to_owned(), counted)
}

fn held_entry(status: &str, queue: &str, execution: &str, count: u64) -> HeldEntry {
    (
        status.to_owned(),
        queue.to_owned(),
        execution.to_owned(),
        count,
    )
}

fn pending(queue: &str, execution: Execution, count: PendingCount) -> PendingTasks {
    PendingTasks {
        queue_name: queue.to_owned(),
        execution,
        count,
    }
}

fn held(status: HeldStatus, queue: &str, execution: Execution, count: u64) -> HeldTasks {
    HeldTasks {
        status,
        queue_name: queue.to_owned(),
        execution,
        count,
    }
}

/// One entry of the workers statement's `live` part, on queue `a`, with 4
/// processes.
fn live_entry(worker_id: &str, class_capacity: Value, slots_down: Value) -> Value {
    json!({
        "worker_id": worker_id,
        "snapshot_at": "2026-11-19T11:59:50+00:00",
        "queues": ["a"],
        "class_capacity": class_capacity,
        "processes": 4,
        "slots_down": slots_down,
    })
}

/// Slot 1 down: the OS refused a thread of the child.
fn one_slot_down() -> Value {
    json!([{
        "slot": 1,
        "cause": {"kind": "thread_start_refused"},
        "detail": "can't start new thread",
        "down_since": "2026-11-19T11:00:00.000000+00:00",
        "failed_boots": 1,
    }])
}

fn workers_part(live: Vec<Value>, in_flight: Value) -> WorkersRow {
    WorkersRow {
        checked_at: at(),
        recent_rows: i64::try_from(live.len()).expect("a few test entries"),
        live: Value::Array(live),
        in_flight,
        endpoint_down: 0,
        handler_child_down: 0,
    }
}

fn heads_part(heads: Value) -> OldestPendingRow {
    OldestPendingRow {
        checked_at: at(),
        heads,
    }
}

#[test]
fn each_label_value_maps_to_its_count() {
    let workers = LiveWorkerCounts {
        serving: 3,
        degraded: 1,
    };
    assert_eq!(workers.of(FleetWorkerState::Serving), 3);
    assert_eq!(workers.of(FleetWorkerState::Degraded), 1);
    let workflows = WorkflowCounts {
        running: 9,
        paused: 2,
    };
    assert_eq!(workflows.of(LiveWorkflowStatus::Running), 9);
    assert_eq!(workflows.of(LiveWorkflowStatus::Paused), 2);
    let telemetry_down = TelemetryDownCounts {
        endpoint: 4,
        handler_child: 1,
    };
    assert_eq!(telemetry_down.of(TelemetryPart::Endpoint), 4);
    assert_eq!(telemetry_down.of(TelemetryPart::HandlerChild), 1);
    assert_eq!(HeldStatus::Claimed.live_status(), LiveTaskStatus::Claimed);
    assert_eq!(HeldStatus::Running.live_status(), LiveTaskStatus::Running);
}

#[test]
fn queue_tasks_come_out_typed_and_ordered() {
    let entries = vec![
        pending_entry("b", "ISOLATED", 3),
        pending_entry("a", "SHARED_THREAD", PENDING_COUNT_LIMIT + 1),
        pending_entry("a", "ISOLATED", PENDING_COUNT_LIMIT),
    ];
    let groups = vec![
        held_entry("RUNNING", "a", "ISOLATED", 2),
        held_entry("CLAIMED", "b", "SHARED_ASYNC", 1),
    ];

    let tasks = queue_tasks(entries, groups).expect("within the bounds");

    assert_eq!(
        tasks.pending,
        [
            pending("a", ISOLATED, PendingCount::Exact(PENDING_COUNT_LIMIT)),
            pending("a", THREAD, PendingCount::AtLeastLimit),
            pending("b", ISOLATED, PendingCount::Exact(3)),
        ]
    );
    assert_eq!(
        tasks.held,
        [
            held(HeldStatus::Claimed, "b", ASYNC, 1),
            held(HeldStatus::Running, "a", ISOLATED, 2),
        ]
    );
}

#[test]
fn a_count_at_the_limit_is_exact_and_one_past_it_is_at_least_the_limit() {
    assert_eq!(pending_count(1), PendingCount::Exact(1));
    assert_eq!(
        pending_count(PENDING_COUNT_LIMIT),
        PendingCount::Exact(PENDING_COUNT_LIMIT)
    );
    assert_eq!(
        pending_count(PENDING_COUNT_LIMIT + 1),
        PendingCount::AtLeastLimit
    );
}

#[test]
fn the_stream_bound_refuses_one_stream_past_it() {
    let streams = |count: usize| -> Vec<PendingEntry> {
        (0..count)
            .map(|index| pending_entry(&format!("q{index}"), "ISOLATED", 1))
            .collect()
    };

    let at_bound = queue_tasks(streams(PENDING_STREAM_LIMIT), vec![]).expect("at the bound");
    assert_eq!(at_bound.pending.len(), PENDING_STREAM_LIMIT);

    let past = queue_tasks(streams(PENDING_STREAM_LIMIT + 1), vec![]).expect_err("past it");
    assert_eq!(
        past.reason,
        "more than 3000 (queue, class) streams have PENDING rows"
    );
}

#[test]
fn the_held_group_bound_refuses_one_group_past_it() {
    let groups = |count: usize| -> Vec<HeldEntry> {
        (0..count)
            .map(|index| held_entry("RUNNING", &format!("q{index}"), "ISOLATED", 1))
            .collect()
    };

    let at_bound = queue_tasks(vec![], groups(HELD_GROUP_LIMIT)).expect("at the bound");
    assert_eq!(at_bound.held.len(), HELD_GROUP_LIMIT);

    let past = queue_tasks(vec![], groups(HELD_GROUP_LIMIT + 1)).expect_err("past it");
    assert_eq!(
        past.reason,
        "more than 6000 (status, queue, class) groups have CLAIMED or RUNNING rows"
    );
}

#[test]
#[should_panic(expected = "the held part of fleet_queue_tasks holds status \"PENDING\"")]
fn a_held_status_outside_the_statement_is_a_defect() {
    let _ = queue_tasks(vec![], vec![held_entry("PENDING", "a", "ISOLATED", 1)]);
}

/// Queue `a` ISOLATED: the oldest due head of three priorities, one of them
/// due later. Queue `b` SHARED_ASYNC: every row due later.
#[test]
fn the_oldest_due_head_of_each_stream_is_its_wait() {
    let oldest = at() - TimeDelta::seconds(300);
    let row = heads_part(json!([
        ["a", "ISOLATED", "2026-11-19T11:59:30+00:00"],
        ["a", "ISOLATED", "2026-11-19T11:55:00+00:00"],
        ["a", "ISOLATED", "2026-11-19T12:10:00+00:00"],
        ["b", "SHARED_ASYNC", "2026-11-19T12:02:00+00:00"],
    ]));

    let pending = oldest_pending(row).expect("a readable heads part");

    assert_eq!(pending.checked_at, at());
    assert_eq!(
        pending.streams,
        [
            StreamOldest {
                queue_name: "a".to_owned(),
                execution: ISOLATED,
                wait: StreamWait::OldestWaiting {
                    enqueued_at: oldest,
                    wait: TimeDelta::seconds(300),
                },
            },
            StreamOldest {
                queue_name: "b".to_owned(),
                execution: ASYNC,
                wait: StreamWait::NoneDue {
                    next_due_at: at() + TimeDelta::seconds(120),
                },
            },
        ]
    );
}

#[test]
fn a_heads_part_that_does_not_decode_refuses_the_oldest_pending_part() {
    let refusal = oldest_pending(heads_part(json!({}))).expect_err("not an array");

    assert!(
        refusal
            .reason
            .starts_with("the heads part of the read did not decode"),
        "{}",
        refusal.reason
    );
}

/// w1 serves; its ISOLATED budget also serves SHARED_THREAD and holds one
/// row past its 4 seats. w2 has a slot down.
#[test]
fn workers_count_by_slot_health_and_sum_seats_per_budget_class() {
    let w1_budgets = json!({
        "ISOLATED": {"capacity": 4, "serves": ["ISOLATED", "SHARED_THREAD"]},
        "SHARED_ASYNC": {"capacity": 40, "serves": ["SHARED_ASYNC"]},
    });
    let w2_budgets = json!({"ISOLATED": {"capacity": 2, "serves": ["ISOLATED"]}});
    let row = workers_part(
        vec![
            live_entry("w1", w1_budgets, json!([])),
            live_entry("w2", w2_budgets, one_slot_down()),
        ],
        json!([
            ["w1", "ISOLATED", 3],
            ["w1", "SHARED_THREAD", 2],
            ["w1", "SHARED_ASYNC", 10],
            ["w2", "ISOLATED", 1],
        ]),
    );

    let workers = fleet_workers(row).expect("readable snapshots");

    assert_eq!(workers.checked_at, at());
    assert_eq!(
        workers.workers,
        LiveWorkerCounts {
            serving: 1,
            degraded: 1
        }
    );
    assert_eq!(
        workers.seats.of(ISOLATED),
        SeatCount {
            seats: 6,
            in_use: 5
        }
    );
    assert_eq!(
        workers.seats.of(ASYNC),
        SeatCount {
            seats: 40,
            in_use: 10
        }
    );
    assert_eq!(workers.seats.of(THREAD), SeatCount::default());
}

/// The statement counts the live workers whose part is down; the decode
/// keeps each count under its part.
#[test]
fn the_workers_part_holds_the_live_workers_whose_telemetry_part_is_down() {
    let budgets = json!({"ISOLATED": {"capacity": 2, "serves": ["ISOLATED"]}});
    let row = WorkersRow {
        endpoint_down: 2,
        handler_child_down: 1,
        ..workers_part(
            vec![
                live_entry("w1", budgets.clone(), json!([])),
                live_entry("w2", budgets.clone(), json!([])),
                live_entry("w3", budgets, json!([])),
            ],
            json!([]),
        )
    };

    let workers = fleet_workers(row).expect("readable snapshots");

    assert_eq!(
        workers.telemetry_down,
        TelemetryDownCounts {
            endpoint: 2,
            handler_child: 1,
        }
    );
}

#[test]
fn no_live_worker_gives_zero_workers_and_zero_seats() {
    let workers = fleet_workers(workers_part(vec![], json!([]))).expect("an empty fleet");

    assert_eq!(
        workers.workers,
        LiveWorkerCounts {
            serving: 0,
            degraded: 0
        }
    );
    for execution in Execution::ALL {
        assert_eq!(workers.seats.of(execution), SeatCount::default());
    }
    assert_eq!(
        workers.telemetry_down,
        TelemetryDownCounts {
            endpoint: 0,
            handler_child: 0,
        }
    );
}

#[test]
fn an_unreadable_slots_down_refuses_the_workers_part_and_names_the_worker() {
    let budgets = json!({"ISOLATED": {"capacity": 4, "serves": ["ISOLATED"]}});
    let row = workers_part(
        vec![live_entry("w-odd", budgets, json!({"slot": 1}))],
        json!([]),
    );

    let refusal = fleet_workers(row).expect_err("slots_down is not an array");

    assert_eq!(
        refusal.reason,
        "worker w-odd: slots_down is unreadable: slots_down is not a JSON array"
    );
}

#[test]
fn an_unreadable_class_capacity_refuses_the_workers_part_and_names_the_worker() {
    let odd = json!({"EXCLUSIVE": {"capacity": 1, "serves": []}});
    let row = workers_part(vec![live_entry("w-odd", odd, json!([]))], json!([]));

    let refusal = fleet_workers(row).expect_err("an unknown budget class");

    assert!(refusal.reason.contains("w-odd"), "{}", refusal.reason);
    assert!(
        refusal.reason.contains("class_capacity is unreadable"),
        "{}",
        refusal.reason
    );
}

#[test]
fn snapshots_past_the_bound_refuse_the_workers_part() {
    let row = WorkersRow {
        recent_rows: i64::try_from(LIVE_SNAPSHOT_ROW_LIMIT + 1).expect("fits in i64"),
        ..workers_part(vec![], json!([]))
    };

    let refusal = fleet_workers(row).expect_err("past the snapshot bound");

    assert_eq!(
        refusal.reason,
        "more than 20000 worker snapshots are inside the live window of 120 s"
    );
}

/// The claim takes only rows of a class that a budget serves, so such a row
/// means the stored rows and the recorded budgets disagree.
#[test]
fn held_rows_of_a_class_no_budget_serves_refuse_the_workers_part() {
    let budgets = json!({"SHARED_ASYNC": {"capacity": 40, "serves": ["SHARED_ASYNC"]}});
    let row = workers_part(
        vec![live_entry("w1", budgets, json!([]))],
        json!([["w1", "ISOLATED", 1]]),
    );

    let refusal = fleet_workers(row).expect_err("an unserved class is held");

    assert!(refusal.reason.contains("w1"), "{}", refusal.reason);
    assert!(refusal.reason.contains("ISOLATED"), "{}", refusal.reason);
}

/// Three budgets of `2^63 - 1` seats: the sum passes `u64::MAX`.
#[test]
fn seats_that_do_not_fit_in_64_bits_refuse_the_workers_part() {
    let huge = json!({"ISOLATED": {"capacity": i64::MAX, "serves": ["ISOLATED"]}});
    let row = workers_part(
        vec![
            live_entry("w1", huge.clone(), json!([])),
            live_entry("w2", huge.clone(), json!([])),
            live_entry("w3", huge, json!([])),
        ],
        json!([]),
    );

    let refusal = fleet_workers(row).expect_err("the sum overflows");

    assert_eq!(
        refusal.reason,
        "the seats of class ISOLATED do not fit in 64 bits"
    );
}

#[test]
fn workflow_counts_take_both_counts() {
    assert_eq!(
        workflow_counts(9, 2),
        WorkflowCounts {
            running: 9,
            paused: 2
        }
    );
}

#[test]
#[should_panic(expected = "count(*) of PAUSED workflows is -1")]
fn a_negative_count_is_a_defect() {
    let _ = workflow_counts(0, -1);
}

#[test]
fn a_schedule_that_ran_has_its_age_and_one_that_never_ran_has_none() {
    let ran_at = at() - TimeDelta::seconds(90);
    let entries = vec![
        ("nightly".to_owned(), Some(ran_at)),
        ("hourly".to_owned(), None),
    ];

    let runs = schedule_runs(at(), entries).expect("within the bound");

    assert_eq!(runs.checked_at, at());
    assert_eq!(
        runs.schedules,
        [
            ScheduleLastRun {
                schedule_name: "hourly".to_owned(),
                last_run: LastRun::NeverRan,
            },
            ScheduleLastRun {
                schedule_name: "nightly".to_owned(),
                last_run: LastRun::RanAt {
                    at: ran_at,
                    age: TimeDelta::seconds(90),
                },
            },
        ]
    );
}

#[test]
fn the_schedule_bound_refuses_one_schedule_past_it() {
    let entries = |count: usize| -> Vec<ScheduleEntry> {
        (0..count)
            .map(|index| (format!("schedule-{index}"), None))
            .collect()
    };

    let at_bound = schedule_runs(at(), entries(SCHEDULE_LIMIT)).expect("at the bound");
    assert_eq!(at_bound.schedules.len(), SCHEDULE_LIMIT);

    let past = schedule_runs(at(), entries(SCHEDULE_LIMIT + 1)).expect_err("past it");
    assert_eq!(past.reason, "more than 10000 schedules have a state row");
}

#[test]
fn the_newest_migration_is_the_schema_version() {
    assert_eq!(
        schema_version(Some(1000)),
        Ok(SchemaVersion { version: 1000 })
    );
}

#[test]
fn no_recorded_migration_refuses_the_schema_version() {
    let refusal = schema_version(None).expect_err("no migration");
    assert_eq!(
        refusal.reason,
        "horsies_migrations records no successful migration"
    );
}
