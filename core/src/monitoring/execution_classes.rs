//! Response models of the slot health, the profile of a snapshot and the
//! execution class report.
//!
//! The worker routes read the broker's typed worker-health methods; these
//! functions turn their results into the wire models of the monitoring UI.
//! Pure, no I/O.

use crate::broker::{
    ClassReport, PendingStream, SlotHealth, SnapshotProfile, StreamCoverageState, StreamWait,
    WorkerClassLoad,
};
use crate::core::defaults::WORKER_STATE_LIVE_WINDOW_S;

use super::models::{
    BudgetLoadInfo, ClassReportInfo, DownSlotInfo, PendingStreamInfo, SlotHealthKind,
    SnapshotProfileInfo, StreamCoverageInfo, StreamWaitInfo, WorkerClassLoadInfo,
};

/// The `profile` field of a worker state: the profile name of the snapshot,
/// or `not_recorded`.
pub fn snapshot_profile_info(profile: &SnapshotProfile) -> SnapshotProfileInfo {
    match profile {
        SnapshotProfile::Named { name } => SnapshotProfileInfo::Named { name: name.clone() },
        SnapshotProfile::NotRecorded => SnapshotProfileInfo::NotRecorded,
    }
}

/// The `slot_health` and `down_slots` fields of a worker state.
pub fn slot_health_info(slot_health: &SlotHealth) -> (SlotHealthKind, Vec<DownSlotInfo>) {
    match slot_health {
        SlotHealth::AllSlotsServing => (SlotHealthKind::AllServing, Vec::new()),
        SlotHealth::SlotsDown(slots) => (
            SlotHealthKind::SlotsDown,
            slots
                .as_slice()
                .iter()
                .map(|down| DownSlotInfo {
                    slot: down.slot,
                    cause: down.cause.to_string(),
                    detail: down.detail.clone(),
                    down_since: down.down_since,
                    failed_boots: down.failed_boots.get(),
                })
                .collect(),
        ),
    }
}

/// The wire model of a class report.
pub fn class_report_info(report: &ClassReport) -> ClassReportInfo {
    ClassReportInfo {
        checked_at: report.checked_at,
        live_window_s: WORKER_STATE_LIVE_WINDOW_S,
        streams: report.streams.iter().map(stream_info).collect(),
        workers: report.workers.iter().map(worker_info).collect(),
    }
}

fn stream_info(stream: &PendingStream) -> PendingStreamInfo {
    let coverage = match stream.state {
        StreamCoverageState::Served { workers, capacity } => {
            StreamCoverageInfo::Served { workers, capacity }
        }
        StreamCoverageState::WithoutCapacity { workers } => {
            StreamCoverageInfo::NoCapacity { workers }
        }
        StreamCoverageState::Unserved => StreamCoverageInfo::Unserved,
    };
    let wait = match stream.wait {
        StreamWait::OldestWaiting { enqueued_at, wait } => StreamWaitInfo::Waiting {
            oldest_due_at: enqueued_at,
            wait_s: wait.num_seconds(),
        },
        StreamWait::NoneDue { next_due_at } => StreamWaitInfo::NoneDue { next_due_at },
    };
    PendingStreamInfo {
        queue_name: stream.queue_name.clone(),
        execution: stream.execution,
        coverage,
        wait,
    }
}

fn worker_info(worker: &WorkerClassLoad) -> WorkerClassLoadInfo {
    WorkerClassLoadInfo {
        worker_id: worker.worker_id.clone(),
        snapshot_at: worker.snapshot_at,
        budgets: worker
            .budgets
            .iter()
            .map(|budget| BudgetLoadInfo {
                budget: budget.budget,
                capacity: budget.capacity,
                serves: budget.serves.clone(),
                in_flight: budget.in_flight,
                free: budget.free(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    //! The wire shape of the class report.
    use super::*;
    use crate::broker::BudgetLoad;
    use crate::core::task::Execution;
    use chrono::{TimeDelta, TimeZone, Utc};
    use serde_json::json;

    #[test]
    fn all_slots_serving_has_no_down_slot() {
        let (kind, down) = slot_health_info(&SlotHealth::AllSlotsServing);
        assert_eq!(kind, SlotHealthKind::AllServing);
        assert_eq!(down.len(), 0);
    }

    #[test]
    fn a_report_serializes_with_tagged_coverage_and_wait() {
        let at = Utc.with_ymd_and_hms(2026, 11, 10, 12, 0, 0).unwrap();
        let report = ClassReport {
            checked_at: at,
            streams: vec![
                PendingStream {
                    queue_name: "a".to_owned(),
                    execution: Execution::Isolated,
                    wait: StreamWait::OldestWaiting {
                        enqueued_at: at - TimeDelta::milliseconds(90_500),
                        wait: TimeDelta::milliseconds(90_500),
                    },
                    state: StreamCoverageState::Served {
                        workers: 2,
                        capacity: 6,
                    },
                },
                PendingStream {
                    queue_name: "a".to_owned(),
                    execution: Execution::SharedAsync,
                    wait: StreamWait::NoneDue {
                        next_due_at: at + TimeDelta::minutes(1),
                    },
                    state: StreamCoverageState::WithoutCapacity { workers: 1 },
                },
                PendingStream {
                    queue_name: "b".to_owned(),
                    execution: Execution::SharedThread,
                    wait: StreamWait::NoneDue {
                        next_due_at: at + TimeDelta::minutes(1),
                    },
                    state: StreamCoverageState::Unserved,
                },
            ],
            workers: vec![WorkerClassLoad {
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

        let body = serde_json::to_value(class_report_info(&report)).expect("serialize");

        assert_eq!(
            body,
            json!({
                "checked_at": "2026-11-10T12:00:00Z",
                "live_window_s": 120,
                "streams": [
                    {
                        "queue_name": "a",
                        "execution": "ISOLATED",
                        "coverage": {"kind": "served", "workers": 2, "capacity": 6},
                        "wait": {
                            "kind": "waiting",
                            "oldest_due_at": "2026-11-10T11:58:29.500000Z",
                            "wait_s": 90,
                        },
                    },
                    {
                        "queue_name": "a",
                        "execution": "SHARED_ASYNC",
                        "coverage": {"kind": "no_capacity", "workers": 1},
                        "wait": {"kind": "none_due", "next_due_at": "2026-11-10T12:01:00Z"},
                    },
                    {
                        "queue_name": "b",
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
                        "free": 0,
                    }],
                }],
            })
        );
    }
}
