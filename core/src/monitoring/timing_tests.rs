//! The composer of the phase timing (`timing.rs`): one test per rule of the
//! composition, per coverage label and per conflict. No database: the facts
//! are built here as the row decoder builds them.

use std::collections::HashMap;
use std::num::NonZeroU32;

use chrono::{DateTime, Duration, TimeZone, Utc};
use uuid::Uuid;

use super::timing::{
    attempt_phases_info, compose_run, ChildRun, LiveStatus, LiveTask, MovedRecord, MovedTask,
    NodeEntry, NodeEventKind, NodeFacts, NodeStatus, NodeWork, RunEvent, RunEventKind, RunFacts,
    TaskAttempts, TaskMove, TaskTimes, TimingRecords,
};
use super::timing_models::{
    AttemptClaimsInfo, AttemptFinalizingInfo, AttemptPhasesInfo, AttemptPlacementInfo,
    AttemptRecordedByInfo, MissingRecord, NodeEndpointInfo, NodeTimingInfo, PauseEndInfo,
    PauseIntervalInfo, RunConflictInfo, RunCoverageInfo, RunTimingInfo, SegmentClaimInfo,
    SegmentEndInfo, SegmentPhaseInfo, SegmentSourceInfo, TerminalNodeStatus, TimingBoundary,
    TimingConflictInfo, TimingSegmentInfo, UnavailableReason, UnknownReason, WorkflowEventKind,
};
use crate::core::history::archive::attempts::{
    AttemptPhases, AttemptRecord, ClaimRecord, Finalizing, OpenAttempt, OpenAttemptFacts,
    OpenClaims, PhaseRecord, Placement, RecordedBy,
};
use crate::core::lifecycle::TerminalizationKind;

/// The read time of every run here, in seconds after [`at`]`(0)`.
const AS_OF: i64 = 1_000;

const TASK_A: Uuid = Uuid::from_u128(0xa);
const TASK_B: Uuid = Uuid::from_u128(0xb);

/// `seconds` after the creation of the run.
fn at(seconds: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 22, 12, 0, 0)
        .single()
        .expect("a valid time")
        + Duration::seconds(seconds)
}

fn seconds(time: DateTime<Utc>) -> i64 {
    (time - at(0)).num_seconds()
}

fn run(events: Vec<RunEvent>, live_tasks: Vec<(Uuid, LiveTask)>) -> RunFacts {
    RunFacts {
        as_of: at(AS_OF),
        captured_since: at(-3_600),
        created_at: at(0),
        completed_at: None,
        records: TimingRecords::Included {
            events,
            live_tasks: live_tasks.into_iter().collect::<HashMap<_, _>>(),
        },
    }
}

fn node(status: NodeStatus, entry: NodeEntry, work: NodeWork, completed: Option<i64>) -> NodeFacts {
    NodeFacts {
        task_index: 0,
        status,
        created_at: at(0),
        completed_at: completed.map(at),
        entry,
        work,
    }
}

fn task(linked: Uuid) -> NodeWork {
    NodeWork::Task {
        linked: Some(linked),
    }
}

/// The timing of the only node of `run`.
fn compose_one(run: &RunFacts, node: NodeFacts) -> NodeTimingInfo {
    compose_run(run, &[node])
        .nodes
        .into_iter()
        .next()
        .expect("one timing per node")
}

/// The phase and the times of each segment, in seconds.
fn phases(timing: &NodeTimingInfo) -> Vec<(SegmentPhaseInfo, i64, Option<i64>)> {
    let segments = match timing {
        NodeTimingInfo::Exact { segments, .. } | NodeTimingInfo::Partial { segments, .. } => {
            segments
        }
        other => panic!("a timing with no segment: {other:?}"),
    };
    segments
        .iter()
        .map(|segment| {
            let end = match segment.end {
                SegmentEndInfo::Ended { at } => Some(seconds(at)),
                SegmentEndInfo::Open => None,
            };
            (segment.phase, seconds(segment.started_at), end)
        })
        .collect()
}

fn sources(timing: &NodeTimingInfo) -> Vec<SegmentSourceInfo> {
    match timing {
        NodeTimingInfo::Exact { segments, .. } | NodeTimingInfo::Partial { segments, .. } => {
            segments.iter().map(|segment| segment.source).collect()
        }
        other => panic!("a timing with no segment: {other:?}"),
    }
}

fn endpoint(timing: &NodeTimingInfo) -> NodeEndpointInfo {
    match timing {
        NodeTimingInfo::Exact { endpoint, .. } | NodeTimingInfo::Partial { endpoint, .. } => {
            *endpoint
        }
        other => panic!("a timing with no endpoint: {other:?}"),
    }
}

fn conflict(timing: &NodeTimingInfo) -> TimingConflictInfo {
    match timing {
        NodeTimingInfo::Inconsistent { conflict } => *conflict,
        other => panic!("not inconsistent: {other:?}"),
    }
}

fn record(
    attempt: i32,
    outcome: &str,
    started: i64,
    finished: i64,
    phases: PhaseRecord,
) -> AttemptRecord {
    AttemptRecord::new(
        attempt,
        outcome,
        outcome != "COMPLETED",
        at(started),
        at(finished),
        None,
        None,
        None,
        Some("worker-1".to_owned()),
        Some("host-1".to_owned()),
        Some(1),
        Some("proc-1".to_owned()),
    )
    .expect("a valid attempt")
    .with_phases(phases)
}

fn by_worker(
    enqueued: i64,
    claimed: i64,
    claims: ClaimRecord,
    finalizing: Finalizing,
    placement: Placement,
) -> PhaseRecord {
    PhaseRecord::Captured(AttemptPhases::new(
        at(enqueued),
        at(claimed),
        claims,
        finalizing,
        RecordedBy::Worker { placement },
    ))
}

fn counted(count: u32, first: i64) -> ClaimRecord {
    ClaimRecord::Counted {
        count: NonZeroU32::new(count).expect("a count of 1 or more"),
        first_claimed_at: at(first),
    }
}

fn open_counted(count: u32, first: i64) -> OpenClaims {
    OpenClaims::Counted {
        count: NonZeroU32::new(count).expect("a count of 1 or more"),
        first_claimed_at: at(first),
    }
}

fn open(claims: OpenClaims, finalizing: Finalizing) -> OpenAttempt {
    OpenAttempt::Open(OpenAttemptFacts::new(claims, finalizing))
}

fn times(created: i64, enqueued: i64, claimed: Option<i64>, started: Option<i64>) -> TaskTimes {
    TaskTimes {
        created_at: at(created),
        enqueued_at: at(enqueued),
        claimed_at: claimed.map(at),
        started_at: started.map(at),
    }
}

fn history(times: TaskTimes, records: Vec<AttemptRecord>, open: OpenAttempt) -> MovedRecord {
    MovedRecord::History(MovedTask {
        times,
        attempts: TaskAttempts { records, open },
    })
}

fn live(
    times: TaskTimes,
    status: LiveStatus,
    records: Vec<AttemptRecord>,
    open: OpenAttempt,
) -> LiveTask {
    LiveTask {
        times,
        status,
        attempts: TaskAttempts { records, open },
    }
}

fn node_event(event_id: i64, occurred: i64, event: NodeEventKind) -> RunEvent {
    RunEvent {
        event_id,
        occurred_at: at(occurred),
        kind: RunEventKind::Node {
            task_index: 0,
            event,
        },
    }
}

fn moved(
    event_id: i64,
    occurred: i64,
    task_id: Uuid,
    move_kind: TerminalizationKind,
    record: MovedRecord,
) -> RunEvent {
    node_event(
        event_id,
        occurred,
        NodeEventKind::TaskMoved(Box::new(TaskMove {
            task_id,
            move_kind,
            record,
        })),
    )
}

fn run_event(event_id: i64, occurred: i64, kind: RunEventKind) -> RunEvent {
    RunEvent {
        event_id,
        occurred_at: at(occurred),
        kind,
    }
}

fn claimed(placement: AttemptPlacementInfo) -> SegmentPhaseInfo {
    SegmentPhaseInfo::Claimed {
        claim: SegmentClaimInfo::Recorded { placement },
    }
}

fn earlier(count: u32) -> SegmentPhaseInfo {
    SegmentPhaseInfo::Claimed {
        claim: SegmentClaimInfo::Earlier { count },
    }
}

fn not_recorded() -> SegmentPhaseInfo {
    SegmentPhaseInfo::Claimed {
        claim: SegmentClaimInfo::NotRecorded,
    }
}

fn unknown(reason: UnknownReason) -> SegmentPhaseInfo {
    SegmentPhaseInfo::Unknown { reason }
}

fn placed(seconds: i64) -> AttemptPlacementInfo {
    AttemptPlacementInfo::Placed {
        placed_at: at(seconds),
    }
}

/// One completed attempt that moved with `kind` at 9 s: queued 0-2 s,
/// placed at 3 s, running 4-8 s, finalizing 8-9 s.
fn completed_attempt(kind: TerminalizationKind) -> RunEvent {
    moved(
        1,
        9,
        TASK_A,
        kind,
        history(
            times(0, 0, Some(2), Some(4)),
            vec![record(
                1,
                "COMPLETED",
                4,
                9,
                by_worker(
                    0,
                    2,
                    counted(1, 2),
                    Finalizing::Marked { at: at(8) },
                    Placement::Placed { at: at(3) },
                ),
            )],
            OpenAttempt::Ended,
        ),
    )
}

#[test]
fn one_attempt_gives_each_phase_with_its_placement_and_then_advancing() {
    let timing = compose_one(
        &run(
            vec![completed_attempt(TerminalizationKind::CompleteLocked)],
            vec![],
        ),
        node(
            NodeStatus::Completed,
            NodeEntry::Ready,
            task(TASK_A),
            Some(11),
        ),
    );

    assert_eq!(
        phases(&timing),
        vec![
            (SegmentPhaseInfo::Enqueued, 0, Some(2)),
            (claimed(placed(3)), 2, Some(4)),
            (SegmentPhaseInfo::Running, 4, Some(8)),
            (SegmentPhaseInfo::Finalizing, 8, Some(9)),
            (SegmentPhaseInfo::Advancing, 9, Some(11)),
        ]
    );
    assert!(matches!(timing, NodeTimingInfo::Exact { .. }), "{timing:?}");
    assert_eq!(
        endpoint(&timing),
        NodeEndpointInfo::Terminal {
            status: TerminalNodeStatus::Completed,
            at: at(11),
        }
    );
    let attempt = SegmentSourceInfo::Attempt {
        generation: 1,
        task_id: TASK_A,
        attempt: 1,
    };
    assert_eq!(
        sources(&timing),
        vec![attempt, attempt, attempt, attempt, SegmentSourceInfo::Node]
    );
}

#[test]
fn a_retry_waits_from_the_recorded_end_to_its_due_time() {
    let attempts = vec![
        record(
            1,
            "FAILED",
            4,
            9,
            by_worker(
                0,
                2,
                counted(1, 2),
                Finalizing::NotMarked,
                Placement::NotObserved,
            ),
        ),
        record(
            2,
            "COMPLETED",
            17,
            20,
            by_worker(
                15,
                16,
                counted(1, 16),
                Finalizing::NotMarked,
                Placement::Placed { at: at(16) },
            ),
        ),
    ];
    let timing = compose_one(
        &run(
            vec![moved(
                1,
                20,
                TASK_A,
                TerminalizationKind::CompleteLocked,
                history(
                    times(0, 15, Some(16), Some(17)),
                    attempts,
                    OpenAttempt::Ended,
                ),
            )],
            vec![],
        ),
        node(
            NodeStatus::Completed,
            NodeEntry::Ready,
            task(TASK_A),
            Some(22),
        ),
    );

    assert_eq!(
        phases(&timing),
        vec![
            (SegmentPhaseInfo::Enqueued, 0, Some(2)),
            (claimed(AttemptPlacementInfo::NotObserved), 2, Some(4)),
            (SegmentPhaseInfo::Running, 4, Some(9)),
            (SegmentPhaseInfo::RetryWait, 9, Some(15)),
            (SegmentPhaseInfo::Enqueued, 15, Some(16)),
            (claimed(placed(16)), 16, Some(17)),
            (SegmentPhaseInfo::Running, 17, Some(20)),
            (SegmentPhaseInfo::Advancing, 20, Some(22)),
        ]
    );
    assert_eq!(
        sources(&timing)[4],
        SegmentSourceInfo::Attempt {
            generation: 1,
            task_id: TASK_A,
            attempt: 2,
        }
    );
}

/// Lost leases: the claims before the claim that ran the attempt are one
/// `claimed` segment with their count. The node still waits for phase 2.
#[test]
fn earlier_claims_are_one_segment_and_advancing_stays_open_before_phase_2() {
    let timing = compose_one(
        &run(
            vec![moved(
                1,
                10,
                TASK_A,
                TerminalizationKind::CompleteLocked,
                history(
                    times(0, 0, Some(6), Some(8)),
                    vec![record(
                        1,
                        "COMPLETED",
                        8,
                        10,
                        by_worker(
                            0,
                            6,
                            counted(3, 2),
                            Finalizing::NotMarked,
                            Placement::Placed { at: at(7) },
                        ),
                    )],
                    OpenAttempt::Ended,
                ),
            )],
            vec![],
        ),
        node(NodeStatus::Running, NodeEntry::Ready, task(TASK_A), None),
    );

    assert_eq!(
        phases(&timing),
        vec![
            (SegmentPhaseInfo::Enqueued, 0, Some(2)),
            (earlier(2), 2, Some(6)),
            (claimed(placed(7)), 6, Some(8)),
            (SegmentPhaseInfo::Running, 8, Some(10)),
            (SegmentPhaseInfo::Advancing, 10, None),
        ]
    );
    assert_eq!(endpoint(&timing), NodeEndpointInfo::NotTerminal);
}

/// Released claims: the live row waits in the queue again; every claim it
/// had is earlier.
#[test]
fn a_released_attempt_shows_its_claims_as_earlier_and_stays_open() {
    let timing = compose_one(
        &run(
            vec![],
            vec![(
                TASK_A,
                live(
                    times(0, 0, None, None),
                    LiveStatus::Pending,
                    vec![],
                    open(open_counted(2, 3), Finalizing::NotMarked),
                ),
            )],
        ),
        node(NodeStatus::Enqueued, NodeEntry::Ready, task(TASK_A), None),
    );

    assert_eq!(
        phases(&timing),
        vec![
            (SegmentPhaseInfo::Enqueued, 0, Some(3)),
            (earlier(2), 3, None),
        ]
    );
}

#[test]
fn claims_not_counted_make_the_queue_wait_unknown() {
    let timing = compose_one(
        &run(
            vec![moved(
                1,
                9,
                TASK_A,
                TerminalizationKind::FailRunning,
                history(
                    times(0, 0, Some(2), Some(4)),
                    vec![record(
                        1,
                        "FAILED",
                        4,
                        9,
                        by_worker(
                            0,
                            2,
                            ClaimRecord::NotCounted,
                            Finalizing::NotMarked,
                            Placement::NotObserved,
                        ),
                    )],
                    OpenAttempt::Ended,
                ),
            )],
            vec![],
        ),
        node(NodeStatus::Failed, NodeEntry::Ready, task(TASK_A), Some(10)),
    );

    assert_eq!(
        phases(&timing),
        vec![
            (unknown(UnknownReason::ClaimsNotCounted), 0, Some(2)),
            (claimed(AttemptPlacementInfo::NotObserved), 2, Some(4)),
            (SegmentPhaseInfo::Running, 4, Some(9)),
            (SegmentPhaseInfo::Advancing, 9, Some(10)),
        ]
    );
    assert!(
        matches!(&timing, NodeTimingInfo::Partial { reasons, .. } if *reasons == [UnknownReason::ClaimsNotCounted]),
        "{timing:?}"
    );
}

#[test]
fn an_attempt_with_no_phase_record_is_one_unknown_segment() {
    let timing = compose_one(
        &run(
            vec![moved(
                1,
                9,
                TASK_A,
                TerminalizationKind::FailRunning,
                history(
                    times(0, 0, Some(2), Some(4)),
                    vec![record(1, "FAILED", 4, 9, PhaseRecord::NotCaptured)],
                    OpenAttempt::Ended,
                ),
            )],
            vec![],
        ),
        node(NodeStatus::Failed, NodeEntry::Ready, task(TASK_A), Some(10)),
    );

    assert_eq!(
        phases(&timing),
        vec![
            (unknown(UnknownReason::AttemptNotCaptured), 0, Some(9)),
            (SegmentPhaseInfo::Advancing, 9, Some(10)),
        ]
    );
    assert!(
        matches!(&timing, NodeTimingInfo::Partial { reasons, .. } if *reasons == [UnknownReason::AttemptNotCaptured]),
        "{timing:?}"
    );
}

/// Stale RUNNING recovery: running to the finalizing marker, finalizing to
/// the last time the attempt was known alive, unknown to the recovery.
#[test]
fn a_recovered_attempt_ends_with_an_unknown_tail_after_its_last_sign_of_life() {
    let phases_record = PhaseRecord::Captured(AttemptPhases::new(
        at(0),
        at(2),
        counted(1, 2),
        Finalizing::Marked { at: at(6) },
        RecordedBy::Recovery {
            last_seen_at: at(7),
        },
    ));
    let timing = compose_one(
        &run(
            vec![moved(
                1,
                12,
                TASK_A,
                TerminalizationKind::FailStale,
                history(
                    times(0, 0, Some(2), Some(4)),
                    vec![record(1, "WORKER_FAILURE", 4, 12, phases_record)],
                    OpenAttempt::Ended,
                ),
            )],
            vec![],
        ),
        node(NodeStatus::Failed, NodeEntry::Ready, task(TASK_A), Some(13)),
    );

    assert_eq!(
        phases(&timing),
        vec![
            (SegmentPhaseInfo::Enqueued, 0, Some(2)),
            (claimed(AttemptPlacementInfo::NotObserved), 2, Some(4)),
            (SegmentPhaseInfo::Running, 4, Some(6)),
            (SegmentPhaseInfo::Finalizing, 6, Some(7)),
            (unknown(UnknownReason::WorkerLost), 7, Some(12)),
            (SegmentPhaseInfo::Advancing, 12, Some(13)),
        ]
    );
    assert!(
        matches!(&timing, NodeTimingInfo::Partial { reasons, .. } if *reasons == [UnknownReason::WorkerLost]),
        "{timing:?}"
    );
}

/// The attempt before a live retry, recorded at 9 s.
fn failed_first_attempt() -> AttemptRecord {
    record(
        1,
        "FAILED",
        4,
        9,
        by_worker(
            0,
            2,
            counted(1, 2),
            Finalizing::NotMarked,
            Placement::NotObserved,
        ),
    )
}

#[test]
fn an_open_retry_that_is_not_due_waits_open() {
    let timing = compose_one(
        &run(
            vec![],
            vec![(
                TASK_A,
                live(
                    times(0, AS_OF + 50, None, None),
                    LiveStatus::Pending,
                    vec![failed_first_attempt()],
                    open(OpenClaims::NotClaimed, Finalizing::NotMarked),
                ),
            )],
        ),
        node(NodeStatus::Enqueued, NodeEntry::Ready, task(TASK_A), None),
    );

    assert_eq!(
        phases(&timing),
        vec![
            (SegmentPhaseInfo::Enqueued, 0, Some(2)),
            (claimed(AttemptPlacementInfo::NotObserved), 2, Some(4)),
            (SegmentPhaseInfo::Running, 4, Some(9)),
            (SegmentPhaseInfo::RetryWait, 9, None),
        ]
    );
}

#[test]
fn an_open_retry_that_is_due_waits_in_the_queue() {
    let timing = compose_one(
        &run(
            vec![],
            vec![(
                TASK_A,
                live(
                    times(0, 15, None, None),
                    LiveStatus::Pending,
                    vec![failed_first_attempt()],
                    open(OpenClaims::NotClaimed, Finalizing::NotMarked),
                ),
            )],
        ),
        node(NodeStatus::Enqueued, NodeEntry::Ready, task(TASK_A), None),
    );

    assert_eq!(
        phases(&timing)[3..],
        [
            (SegmentPhaseInfo::RetryWait, 9, Some(15)),
            (SegmentPhaseInfo::Enqueued, 15, None),
        ]
    );
    assert_eq!(
        sources(&timing)[4],
        SegmentSourceInfo::Attempt {
            generation: 1,
            task_id: TASK_A,
            attempt: 2,
        }
    );
}

#[test]
fn an_open_claimed_attempt_shows_its_earlier_claims_and_an_unrecorded_claim() {
    let timing = compose_one(
        &run(
            vec![],
            vec![(
                TASK_A,
                live(
                    times(0, 0, Some(5), None),
                    LiveStatus::Claimed,
                    vec![],
                    open(open_counted(2, 3), Finalizing::NotMarked),
                ),
            )],
        ),
        node(NodeStatus::Enqueued, NodeEntry::Ready, task(TASK_A), None),
    );

    assert_eq!(
        phases(&timing),
        vec![
            (SegmentPhaseInfo::Enqueued, 0, Some(3)),
            (earlier(1), 3, Some(5)),
            (not_recorded(), 5, None),
        ]
    );
}

#[test]
fn an_open_running_attempt_with_its_marker_is_finalizing() {
    let timing = compose_one(
        &run(
            vec![],
            vec![(
                TASK_A,
                live(
                    times(0, 0, Some(2), Some(4)),
                    LiveStatus::Running,
                    vec![],
                    open(open_counted(1, 2), Finalizing::Marked { at: at(8) }),
                ),
            )],
        ),
        node(NodeStatus::Running, NodeEntry::Ready, task(TASK_A), None),
    );

    assert_eq!(
        phases(&timing),
        vec![
            (SegmentPhaseInfo::Enqueued, 0, Some(2)),
            (not_recorded(), 2, Some(4)),
            (SegmentPhaseInfo::Running, 4, Some(8)),
            (SegmentPhaseInfo::Finalizing, 8, None),
        ]
    );
}

/// A cancel ends a claimed task with no attempt row: its segments end at the
/// move, and no segment follows a cancel.
#[test]
fn an_open_attempt_in_history_ends_at_the_move() {
    let timing = compose_one(
        &run(
            vec![moved(
                1,
                8,
                TASK_A,
                TerminalizationKind::WorkflowCancelWorkflow,
                history(
                    times(0, 0, Some(5), None),
                    vec![],
                    open(open_counted(1, 5), Finalizing::NotMarked),
                ),
            )],
            vec![],
        ),
        node(NodeStatus::Skipped, NodeEntry::Ready, task(TASK_A), Some(8)),
    );

    assert_eq!(
        phases(&timing),
        vec![
            (SegmentPhaseInfo::Enqueued, 0, Some(5)),
            (not_recorded(), 5, Some(8)),
        ]
    );
    assert_eq!(
        endpoint(&timing),
        NodeEndpointInfo::Terminal {
            status: TerminalNodeStatus::Skipped,
            at: at(8),
        }
    );
}

#[test]
fn a_dependent_node_is_pending_until_a_ready_event_then_ready_until_its_task() {
    let facts = node(NodeStatus::Enqueued, NodeEntry::Pending, task(TASK_A), None);
    let timing = compose_one(
        &run(
            vec![node_event(1, 3, NodeEventKind::Ready)],
            vec![(
                TASK_A,
                live(
                    times(10, 10, None, None),
                    LiveStatus::Pending,
                    vec![],
                    open(OpenClaims::NotClaimed, Finalizing::NotMarked),
                ),
            )],
        ),
        facts,
    );

    assert_eq!(
        phases(&timing),
        vec![
            (SegmentPhaseInfo::Pending, 0, Some(3)),
            (SegmentPhaseInfo::Ready, 3, Some(10)),
            (SegmentPhaseInfo::Enqueued, 10, None),
        ]
    );
}

/// A pause abandons the claimed task (the first task of the node), the node
/// is ready again, and the resume gives it a second task.
#[test]
fn a_pause_abandon_ends_a_task_and_the_resume_starts_the_next_one() {
    let events = vec![
        run_event(1, 5, RunEventKind::WorkflowPaused),
        moved(
            2,
            5,
            TASK_A,
            TerminalizationKind::PauseAbandonWorkflow,
            history(
                times(0, 0, Some(3), None),
                vec![],
                open(open_counted(1, 3), Finalizing::NotMarked),
            ),
        ),
        node_event(3, 5, NodeEventKind::Ready),
        run_event(4, 12, RunEventKind::WorkflowResumed),
    ];
    let composed = compose_run(
        &run(
            events,
            vec![(
                TASK_B,
                live(
                    times(12, 12, None, None),
                    LiveStatus::Pending,
                    vec![],
                    open(OpenClaims::NotClaimed, Finalizing::NotMarked),
                ),
            )],
        ),
        &[node(
            NodeStatus::Enqueued,
            NodeEntry::Ready,
            task(TASK_B),
            None,
        )],
    );
    let timing = &composed.nodes[0];

    assert_eq!(
        phases(timing),
        vec![
            (SegmentPhaseInfo::Enqueued, 0, Some(3)),
            (not_recorded(), 3, Some(5)),
            (SegmentPhaseInfo::Ready, 5, Some(12)),
            (SegmentPhaseInfo::Enqueued, 12, None),
        ]
    );
    assert_eq!(
        sources(timing),
        vec![
            SegmentSourceInfo::Attempt {
                generation: 1,
                task_id: TASK_A,
                attempt: 1,
            },
            SegmentSourceInfo::Attempt {
                generation: 1,
                task_id: TASK_A,
                attempt: 1,
            },
            SegmentSourceInfo::Node,
            SegmentSourceInfo::Attempt {
                generation: 2,
                task_id: TASK_B,
                attempt: 1,
            },
        ]
    );
    match &composed.run {
        RunTimingInfo::Composed {
            coverage, pauses, ..
        } => {
            assert_eq!(*coverage, RunCoverageInfo::Exact);
            assert_eq!(
                *pauses,
                vec![PauseIntervalInfo {
                    started_at: at(5),
                    end: PauseEndInfo::Resumed { at: at(12) },
                }]
            );
        }
        other => panic!("a composed run: {other:?}"),
    }
}

#[test]
fn a_sub_workflow_node_waits_for_its_child_then_runs_and_advances() {
    let timing = compose_one(
        &run(vec![node_event(1, 5, NodeEventKind::Enqueued)], vec![]),
        node(
            NodeStatus::Completed,
            NodeEntry::Pending,
            NodeWork::SubWorkflow {
                child: ChildRun::Found {
                    created_at: at(8),
                    completed_at: Some(at(20)),
                },
            },
            Some(21),
        ),
    );

    assert_eq!(
        phases(&timing),
        vec![
            (SegmentPhaseInfo::Pending, 0, Some(5)),
            (SegmentPhaseInfo::Enqueued, 5, Some(8)),
            (SegmentPhaseInfo::Running, 8, Some(20)),
            (SegmentPhaseInfo::Advancing, 20, Some(21)),
        ]
    );
    assert!(sources(&timing)
        .iter()
        .all(|source| *source == SegmentSourceInfo::Node));
}

/// A child built at the start goes in with its parent node: the node runs
/// from its creation, with no enqueued wait.
#[test]
fn a_sub_workflow_node_with_a_child_built_at_the_start_runs_from_its_creation() {
    let timing = compose_one(
        &run(vec![], vec![]),
        node(
            NodeStatus::Running,
            NodeEntry::Ready,
            NodeWork::SubWorkflow {
                child: ChildRun::Found {
                    created_at: at(0),
                    completed_at: None,
                },
            },
            None,
        ),
    );

    assert_eq!(phases(&timing), vec![(SegmentPhaseInfo::Running, 0, None)]);
}

#[test]
fn a_skipped_node_with_no_task_waits_until_its_completion() {
    let timing = compose_one(
        &run(vec![], vec![]),
        node(
            NodeStatus::Skipped,
            NodeEntry::Pending,
            NodeWork::Task { linked: None },
            Some(30),
        ),
    );

    assert_eq!(
        phases(&timing),
        vec![(SegmentPhaseInfo::Pending, 0, Some(30))]
    );
    assert_eq!(
        endpoint(&timing),
        NodeEndpointInfo::Terminal {
            status: TerminalNodeStatus::Skipped,
            at: at(30),
        }
    );
}

/// A workflow cancel leaves a started task running: its segments go on past
/// the end of the run, and the range grows to the read.
#[test]
fn a_task_that_runs_past_a_cancel_keeps_its_open_segment_and_grows_the_range() {
    let mut facts = run(
        vec![],
        vec![(
            TASK_A,
            live(
                times(0, 0, Some(2), Some(4)),
                LiveStatus::Running,
                vec![],
                open(open_counted(1, 2), Finalizing::NotMarked),
            ),
        )],
    );
    facts.completed_at = Some(at(10));
    let composed = compose_run(
        &facts,
        &[node(
            NodeStatus::Running,
            NodeEntry::Ready,
            task(TASK_A),
            None,
        )],
    );

    assert_eq!(
        phases(&composed.nodes[0]).last(),
        Some(&(SegmentPhaseInfo::Running, 4, None))
    );
    match composed.run {
        RunTimingInfo::Composed {
            range_started_at,
            range_ended_at,
            open_segments,
            ..
        } => {
            assert_eq!(range_started_at, at(0));
            assert_eq!(range_ended_at, at(AS_OF));
            assert!(open_segments);
        }
        other => panic!("a composed run: {other:?}"),
    }
}

/// The recovery limit fails a node whose task went on and completed later:
/// the endpoint comes before the end of the task, and no advancing segment
/// follows.
#[test]
fn a_node_that_ended_before_its_task_keeps_the_task_segments_and_no_advancing() {
    let timing = compose_one(
        &run(
            vec![completed_attempt(TerminalizationKind::CompleteLocked)],
            vec![],
        ),
        node(NodeStatus::Failed, NodeEntry::Ready, task(TASK_A), Some(6)),
    );

    assert_eq!(
        phases(&timing).last(),
        Some(&(SegmentPhaseInfo::Finalizing, 8, Some(9)))
    );
    assert_eq!(
        endpoint(&timing),
        NodeEndpointInfo::Terminal {
            status: TerminalNodeStatus::Failed,
            at: at(6),
        }
    );
}

#[test]
fn a_pause_that_a_cancel_ended_ends_at_the_end_of_the_run() {
    let mut facts = run(vec![run_event(1, 5, RunEventKind::WorkflowPaused)], vec![]);
    facts.completed_at = Some(at(9));
    match compose_run(&facts, &[]).run {
        RunTimingInfo::Composed {
            pauses,
            open_segments,
            ..
        } => {
            assert_eq!(
                pauses,
                vec![PauseIntervalInfo {
                    started_at: at(5),
                    end: PauseEndInfo::RunEnded { at: at(9) },
                }]
            );
            assert!(!open_segments);
        }
        other => panic!("a composed run: {other:?}"),
    }
}

#[test]
fn a_pause_of_a_run_that_is_still_paused_is_open() {
    let facts = run(vec![run_event(1, 5, RunEventKind::WorkflowPaused)], vec![]);
    match compose_run(&facts, &[]).run {
        RunTimingInfo::Composed {
            pauses,
            open_segments,
            range_ended_at,
            ..
        } => {
            assert_eq!(
                pauses,
                vec![PauseIntervalInfo {
                    started_at: at(5),
                    end: PauseEndInfo::Open,
                }]
            );
            assert!(open_segments);
            assert_eq!(range_ended_at, at(AS_OF));
        }
        other => panic!("a composed run: {other:?}"),
    }
}

#[test]
fn a_pruned_history_row_gives_one_unknown_segment_up_to_the_move() {
    let timing = compose_one(
        &run(
            vec![moved(
                1,
                10,
                TASK_A,
                TerminalizationKind::CompleteLocked,
                MovedRecord::Pruned,
            )],
            vec![],
        ),
        node(
            NodeStatus::Completed,
            NodeEntry::Ready,
            task(TASK_A),
            Some(12),
        ),
    );

    assert_eq!(
        phases(&timing),
        vec![
            (unknown(UnknownReason::HistoryPruned), 0, Some(10)),
            (SegmentPhaseInfo::Advancing, 10, Some(12)),
        ]
    );
    assert_eq!(
        sources(&timing)[0],
        SegmentSourceInfo::Task {
            generation: 1,
            task_id: TASK_A,
        }
    );
    assert!(
        matches!(&timing, NodeTimingInfo::Partial { reasons, .. } if *reasons == [UnknownReason::HistoryPruned]),
        "{timing:?}"
    );
}

#[test]
fn a_run_before_the_capture_start_and_a_run_past_the_bound_are_unavailable() {
    let nodes = [node(
        NodeStatus::Pending,
        NodeEntry::Pending,
        NodeWork::Task { linked: None },
        None,
    )];
    for (records, reason) in [
        (
            TimingRecords::BeforeCapture,
            UnavailableReason::RunBeforeCapture,
        ),
        (
            TimingRecords::BoundExceeded,
            UnavailableReason::TimingBoundExceeded,
        ),
    ] {
        let mut facts = run(vec![], vec![]);
        facts.records = records;
        let composed = compose_run(&facts, &nodes);

        assert_eq!(
            composed.run,
            RunTimingInfo::Unavailable {
                as_of: at(AS_OF),
                captured_since: at(-3_600),
                reason,
            }
        );
        assert_eq!(composed.nodes, vec![NodeTimingInfo::Unavailable { reason }]);
    }
}

/// A start before its claim cannot be: the node is inconsistent and names
/// the pair. A placement after the start is the same.
#[test]
fn times_in_the_wrong_order_make_the_node_inconsistent_with_the_pair() {
    for (placement, expected) in [
        (
            Placement::NotObserved,
            TimingConflictInfo::ClockOrder {
                earlier: TimingBoundary::Claim,
                earlier_at: at(5),
                later: TimingBoundary::Start,
                later_at: at(4),
            },
        ),
        (
            Placement::Placed { at: at(6) },
            TimingConflictInfo::ClockOrder {
                earlier: TimingBoundary::Placement,
                earlier_at: at(6),
                later: TimingBoundary::Start,
                later_at: at(4),
            },
        ),
    ] {
        let claim = match placement {
            Placement::NotObserved => 5,
            Placement::Placed { .. } => 2,
        };
        let timing = compose_one(
            &run(
                vec![moved(
                    1,
                    9,
                    TASK_A,
                    TerminalizationKind::CompleteLocked,
                    history(
                        times(0, 0, Some(claim), Some(4)),
                        vec![record(
                            1,
                            "COMPLETED",
                            4,
                            9,
                            by_worker(
                                0,
                                claim,
                                counted(1, claim),
                                Finalizing::NotMarked,
                                placement,
                            ),
                        )],
                        OpenAttempt::Ended,
                    ),
                )],
                vec![],
            ),
            node(
                NodeStatus::Completed,
                NodeEntry::Ready,
                task(TASK_A),
                Some(11),
            ),
        );

        assert_eq!(conflict(&timing), expected);
    }
}

/// A claim and a start at one time give no claimed segment; the order of the
/// other segments stays.
#[test]
fn a_segment_of_zero_length_is_dropped() {
    let timing = compose_one(
        &run(
            vec![moved(
                1,
                9,
                TASK_A,
                TerminalizationKind::CompleteLocked,
                history(
                    times(0, 0, Some(4), Some(4)),
                    vec![record(
                        1,
                        "COMPLETED",
                        4,
                        9,
                        by_worker(
                            0,
                            4,
                            counted(1, 4),
                            Finalizing::NotMarked,
                            Placement::NotObserved,
                        ),
                    )],
                    OpenAttempt::Ended,
                ),
            )],
            vec![],
        ),
        node(
            NodeStatus::Completed,
            NodeEntry::Ready,
            task(TASK_A),
            Some(9),
        ),
    );

    assert_eq!(
        phases(&timing),
        vec![
            (SegmentPhaseInfo::Enqueued, 0, Some(4)),
            (SegmentPhaseInfo::Running, 4, Some(9)),
        ]
    );
}

#[test]
fn records_that_a_node_state_needs_and_does_not_have_are_named() {
    let pending_task = || {
        live(
            times(0, 0, None, None),
            LiveStatus::Running,
            vec![],
            open(open_counted(1, 2), Finalizing::NotMarked),
        )
    };
    let cases = [
        (
            run(vec![], vec![]),
            node(
                NodeStatus::Completed,
                NodeEntry::Ready,
                NodeWork::Task { linked: None },
                None,
            ),
            MissingRecord::CompletionTime,
        ),
        (
            run(vec![], vec![]),
            node(
                NodeStatus::Enqueued,
                NodeEntry::Ready,
                NodeWork::Task { linked: None },
                None,
            ),
            MissingRecord::LinkedWork,
        ),
        (
            run(vec![], vec![]),
            node(NodeStatus::Running, NodeEntry::Ready, task(TASK_A), None),
            MissingRecord::LinkedTask,
        ),
        (
            run(vec![], vec![]),
            node(
                NodeStatus::Running,
                NodeEntry::Ready,
                NodeWork::SubWorkflow {
                    child: ChildRun::NotFound,
                },
                None,
            ),
            MissingRecord::ChildRun,
        ),
        (
            run(vec![], vec![]),
            node(
                NodeStatus::Running,
                NodeEntry::Ready,
                NodeWork::SubWorkflow {
                    child: ChildRun::NotLinked,
                },
                None,
            ),
            MissingRecord::LinkedWork,
        ),
        (
            run(vec![], vec![(TASK_A, pending_task())]),
            node(NodeStatus::Running, NodeEntry::Ready, task(TASK_A), None),
            MissingRecord::TaskTimes,
        ),
        (
            run(
                vec![],
                vec![(
                    TASK_A,
                    live(
                        times(0, 0, None, None),
                        LiveStatus::Pending,
                        vec![],
                        OpenAttempt::Ended,
                    ),
                )],
            ),
            node(NodeStatus::Enqueued, NodeEntry::Ready, task(TASK_A), None),
            MissingRecord::OpenAttempt,
        ),
        (
            run(
                vec![moved(
                    1,
                    9,
                    TASK_A,
                    TerminalizationKind::CompleteLocked,
                    history(times(0, 0, None, None), vec![], OpenAttempt::Ended),
                )],
                vec![],
            ),
            node(NodeStatus::Running, NodeEntry::Ready, task(TASK_A), None),
            MissingRecord::AttemptRow,
        ),
    ];
    for (facts, facts_of_node, record) in cases {
        let timing = compose_one(&facts, facts_of_node);

        assert_eq!(
            conflict(&timing),
            TimingConflictInfo::MissingRecord { record },
            "{facts_of_node:?}"
        );
    }
}

#[test]
fn an_event_that_the_kind_of_the_node_does_not_have_is_unexpected() {
    let task_node = compose_one(
        &run(vec![node_event(7, 5, NodeEventKind::Enqueued)], vec![]),
        node(
            NodeStatus::Pending,
            NodeEntry::Pending,
            NodeWork::Task { linked: None },
            None,
        ),
    );
    let sub_workflow_node = compose_one(
        &run(
            vec![moved(
                8,
                5,
                TASK_A,
                TerminalizationKind::CompleteLocked,
                MovedRecord::Pruned,
            )],
            vec![],
        ),
        node(
            NodeStatus::Pending,
            NodeEntry::Pending,
            NodeWork::SubWorkflow {
                child: ChildRun::NotLinked,
            },
            None,
        ),
    );

    assert_eq!(
        conflict(&task_node),
        TimingConflictInfo::UnexpectedEvent {
            event_id: 7,
            event: WorkflowEventKind::NodeEnqueued,
        }
    );
    assert_eq!(
        conflict(&sub_workflow_node),
        TimingConflictInfo::UnexpectedEvent {
            event_id: 8,
            event: WorkflowEventKind::TaskMoved,
        }
    );
}

/// The run is exact when every node is; partial with the reasons of every
/// node; inconsistent, naming the node or the pause, when one contradicts
/// itself.
#[test]
fn the_run_coverage_aggregates_the_nodes_and_the_pauses() {
    let exact = node(
        NodeStatus::Skipped,
        NodeEntry::Pending,
        NodeWork::Task { linked: None },
        Some(30),
    );
    let mut partial = node(
        NodeStatus::Completed,
        NodeEntry::Ready,
        task(TASK_A),
        Some(12),
    );
    partial.task_index = 1;
    let mut broken = node(
        NodeStatus::Failed,
        NodeEntry::Pending,
        NodeWork::Task { linked: None },
        None,
    );
    broken.task_index = 2;
    let pruned = RunEvent {
        kind: RunEventKind::Node {
            task_index: 1,
            event: NodeEventKind::TaskMoved(Box::new(TaskMove {
                task_id: TASK_A,
                move_kind: TerminalizationKind::CompleteLocked,
                record: MovedRecord::Pruned,
            })),
        },
        ..run_event(1, 10, RunEventKind::WorkflowPaused)
    };
    let coverage =
        |events: Vec<RunEvent>, nodes: &[NodeFacts]| match compose_run(&run(events, vec![]), nodes)
            .run
        {
            RunTimingInfo::Composed { coverage, .. } => coverage,
            other => panic!("a composed run: {other:?}"),
        };

    assert_eq!(coverage(vec![], &[exact]), RunCoverageInfo::Exact);
    assert_eq!(
        coverage(vec![pruned.clone()], &[exact, partial]),
        RunCoverageInfo::Partial {
            reasons: vec![UnknownReason::HistoryPruned],
        }
    );
    assert_eq!(
        coverage(vec![pruned], &[exact, partial, broken]),
        RunCoverageInfo::Inconsistent {
            conflicts: vec![RunConflictInfo::Node { task_index: 2 }],
        }
    );
    assert_eq!(
        coverage(
            vec![run_event(4, 6, RunEventKind::WorkflowResumed)],
            &[exact]
        ),
        RunCoverageInfo::Inconsistent {
            conflicts: vec![RunConflictInfo::Pause {
                conflict: TimingConflictInfo::UnexpectedEvent {
                    event_id: 4,
                    event: WorkflowEventKind::WorkflowResumed,
                },
            }],
        }
    );
}

#[test]
fn the_phase_record_of_an_attempt_row_maps_each_state() {
    assert_eq!(
        attempt_phases_info(PhaseRecord::NotCaptured),
        AttemptPhasesInfo::NotCaptured
    );
    assert_eq!(
        attempt_phases_info(by_worker(
            0,
            2,
            counted(2, 1),
            Finalizing::Marked { at: at(8) },
            Placement::Placed { at: at(3) },
        )),
        AttemptPhasesInfo::Captured {
            enqueued_at: at(0),
            claimed_at: at(2),
            claims: AttemptClaimsInfo::Counted {
                count: 2,
                first_claimed_at: at(1),
            },
            finalizing: AttemptFinalizingInfo::Marked {
                finalizing_at: at(8),
            },
            recorded_by: AttemptRecordedByInfo::Worker {
                placement: placed(3),
            },
        }
    );
    assert_eq!(
        attempt_phases_info(PhaseRecord::Captured(AttemptPhases::new(
            at(0),
            at(2),
            ClaimRecord::NotCounted,
            Finalizing::NotMarked,
            RecordedBy::Recovery {
                last_seen_at: at(7),
            },
        ))),
        AttemptPhasesInfo::Captured {
            enqueued_at: at(0),
            claimed_at: at(2),
            claims: AttemptClaimsInfo::NotCounted,
            finalizing: AttemptFinalizingInfo::NotMarked,
            recorded_by: AttemptRecordedByInfo::Recovery {
                last_seen_at: at(7),
            },
        }
    );
    assert_eq!(
        attempt_phases_info(by_worker(
            0,
            2,
            counted(1, 2),
            Finalizing::NotMarked,
            Placement::NotObserved,
        )),
        AttemptPhasesInfo::Captured {
            enqueued_at: at(0),
            claimed_at: at(2),
            claims: AttemptClaimsInfo::Counted {
                count: 1,
                first_claimed_at: at(2),
            },
            finalizing: AttemptFinalizingInfo::NotMarked,
            recorded_by: AttemptRecordedByInfo::Worker {
                placement: AttemptPlacementInfo::NotObserved,
            },
        }
    );
}

#[test]
fn every_stored_node_status_parses_and_no_other_text_does() {
    for (stored, status) in [
        ("PENDING", NodeStatus::Pending),
        ("READY", NodeStatus::Ready),
        ("ENQUEUED", NodeStatus::Enqueued),
        ("RUNNING", NodeStatus::Running),
        ("COMPLETED", NodeStatus::Completed),
        ("FAILED", NodeStatus::Failed),
        ("SKIPPED", NodeStatus::Skipped),
    ] {
        assert_eq!(NodeStatus::parse(stored), Some(status), "{stored}");
    }
    assert_eq!(NodeStatus::parse("CANCELLED"), None);
}

/// The JSON of each variant, which `clients/python/tests/unit/test_monitoring_timing_models.py`
/// validates into the Pydantic models.
const SHARED_EXAMPLES: &str = include_str!("timing_examples.json");

fn ended_segment(
    phase: SegmentPhaseInfo,
    from: i64,
    to: Option<i64>,
    source: SegmentSourceInfo,
) -> TimingSegmentInfo {
    TimingSegmentInfo {
        phase,
        started_at: at(from),
        end: match to {
            Some(seconds) => SegmentEndInfo::Ended { at: at(seconds) },
            None => SegmentEndInfo::Open,
        },
        source,
    }
}

fn attempt_source(generation: i32, task_id: Uuid, attempt: i32) -> SegmentSourceInfo {
    SegmentSourceInfo::Attempt {
        generation,
        task_id,
        attempt,
    }
}

fn example_attempt_phases() -> Vec<AttemptPhasesInfo> {
    vec![
        AttemptPhasesInfo::NotCaptured,
        attempt_phases_info(by_worker(
            0,
            2,
            counted(2, 1),
            Finalizing::Marked { at: at(8) },
            Placement::Placed { at: at(3) },
        )),
        attempt_phases_info(PhaseRecord::Captured(AttemptPhases::new(
            at(0),
            at(2),
            ClaimRecord::NotCounted,
            Finalizing::NotMarked,
            RecordedBy::Recovery {
                last_seen_at: at(7),
            },
        ))),
        attempt_phases_info(by_worker(
            0,
            2,
            counted(1, 2),
            Finalizing::NotMarked,
            Placement::NotObserved,
        )),
    ]
}

fn example_node_timings() -> Vec<NodeTimingInfo> {
    let first = attempt_source(1, TASK_A, 1);
    let retry = attempt_source(1, TASK_A, 2);
    vec![
        NodeTimingInfo::Exact {
            segments: vec![
                ended_segment(
                    SegmentPhaseInfo::Pending,
                    0,
                    Some(1),
                    SegmentSourceInfo::Node,
                ),
                ended_segment(SegmentPhaseInfo::Ready, 1, Some(2), SegmentSourceInfo::Node),
                ended_segment(SegmentPhaseInfo::Enqueued, 2, Some(3), first),
                ended_segment(earlier(1), 3, Some(4), first),
                ended_segment(claimed(placed(5)), 4, Some(6), first),
                ended_segment(SegmentPhaseInfo::Running, 6, Some(7), first),
                ended_segment(SegmentPhaseInfo::Finalizing, 7, Some(8), first),
                ended_segment(SegmentPhaseInfo::RetryWait, 8, Some(9), retry),
                ended_segment(not_recorded(), 9, Some(10), retry),
                ended_segment(
                    SegmentPhaseInfo::Advancing,
                    10,
                    Some(11),
                    SegmentSourceInfo::Node,
                ),
            ],
            endpoint: NodeEndpointInfo::Terminal {
                status: TerminalNodeStatus::Completed,
                at: at(12),
            },
        },
        NodeTimingInfo::Partial {
            reasons: vec![UnknownReason::HistoryPruned, UnknownReason::WorkerLost],
            segments: vec![
                ended_segment(
                    unknown(UnknownReason::HistoryPruned),
                    0,
                    Some(5),
                    SegmentSourceInfo::Task {
                        generation: 1,
                        task_id: TASK_A,
                    },
                ),
                ended_segment(
                    unknown(UnknownReason::WorkerLost),
                    5,
                    None,
                    attempt_source(2, TASK_B, 1),
                ),
            ],
            endpoint: NodeEndpointInfo::NotTerminal,
        },
        NodeTimingInfo::Unavailable {
            reason: UnavailableReason::RunBeforeCapture,
        },
        NodeTimingInfo::Inconsistent {
            conflict: TimingConflictInfo::ClockOrder {
                earlier: TimingBoundary::Claim,
                earlier_at: at(5),
                later: TimingBoundary::Start,
                later_at: at(4),
            },
        },
        NodeTimingInfo::Inconsistent {
            conflict: TimingConflictInfo::MissingRecord {
                record: MissingRecord::CompletionTime,
            },
        },
        NodeTimingInfo::Inconsistent {
            conflict: TimingConflictInfo::UnexpectedEvent {
                event_id: 7,
                event: WorkflowEventKind::NodeEnqueued,
            },
        },
    ]
}

fn example_run_timings() -> Vec<RunTimingInfo> {
    let composed =
        |coverage: RunCoverageInfo, ended: i64, open: bool, pauses: Vec<PauseIntervalInfo>| {
            RunTimingInfo::Composed {
                as_of: at(100),
                captured_since: at(0),
                coverage,
                range_started_at: at(0),
                range_ended_at: at(ended),
                open_segments: open,
                pauses,
            }
        };
    vec![
        RunTimingInfo::Unavailable {
            as_of: at(100),
            captured_since: at(0),
            reason: UnavailableReason::TimingBoundExceeded,
        },
        composed(
            RunCoverageInfo::Exact,
            100,
            true,
            vec![
                PauseIntervalInfo {
                    started_at: at(1),
                    end: PauseEndInfo::Resumed { at: at(2) },
                },
                PauseIntervalInfo {
                    started_at: at(3),
                    end: PauseEndInfo::RunEnded { at: at(4) },
                },
                PauseIntervalInfo {
                    started_at: at(5),
                    end: PauseEndInfo::Open,
                },
            ],
        ),
        composed(
            RunCoverageInfo::Partial {
                reasons: vec![UnknownReason::ClaimsNotCounted],
            },
            50,
            false,
            vec![],
        ),
        composed(
            RunCoverageInfo::Inconsistent {
                conflicts: vec![
                    RunConflictInfo::Node { task_index: 2 },
                    RunConflictInfo::Pause {
                        conflict: TimingConflictInfo::UnexpectedEvent {
                            event_id: 4,
                            event: WorkflowEventKind::WorkflowResumed,
                        },
                    },
                ],
            },
            50,
            false,
            vec![],
        ),
    ]
}

/// The core writes the JSON of the shared examples: each variant of each
/// union of the timing, with the time text of the other monitoring models.
/// The Python test validates the same file into the Pydantic models.
#[test]
fn the_json_of_every_variant_is_the_shared_example() {
    let expected: serde_json::Value =
        serde_json::from_str(SHARED_EXAMPLES).expect("timing_examples.json is JSON");
    let actual = serde_json::json!({
        "attempt_phases": example_attempt_phases(),
        "node_timings": example_node_timings(),
        "run_timings": example_run_timings(),
    });

    assert_eq!(actual, expected);
}
