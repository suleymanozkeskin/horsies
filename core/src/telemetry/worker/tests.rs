//! The telemetry value of a worker: the drops of the event queue at its
//! bound and at a gone reader, the order of the queue, and no event built
//! without a handler child.

use std::cell::Cell;
use std::num::NonZeroUsize;
use std::sync::Arc;

use chrono::Utc;

use super::*;
use crate::broker::claims::ClaimRead;
use crate::core::task::error::OperationalErrorCode;
use crate::core::task::{Execution, TaskErrorCode, UserCode};
use crate::events::{
    self, AttemptErrorCode, AttemptRecord, AttemptStart, ChildResult, RetryCount, RunTrace,
    SlotIndex, SlotRecovered, WorkerDecision,
};
use crate::telemetry::labels::ReleaseReason;
use crate::telemetry::registry::SampleValue;
use crate::test_support::telemetry::{series_of, worker_registry, WatchedTelemetry};
use crate::worker::executor::protocol::StopReason;
use crate::worker::executor::slots::outcome::{EndView, ResultRead};
use crate::worker::executor::slots::{
    ChildPlacement, LoopKillCode, NotRunCause, Settlement, TerminalState, WrittenEnd,
};

/// An event of slot `slot`.
fn slot_event(slot: u32) -> SlotRecovered {
    SlotRecovered {
        at: Utc::now(),
        child_kind: Execution::Isolated,
        slot: SlotIndex::try_from(slot).unwrap(),
    }
}

fn capacity(events: usize) -> NonZeroUsize {
    NonZeroUsize::new(events).unwrap()
}

#[tokio::test]
async fn an_offer_to_a_full_queue_drops_the_event_and_counts_queue_full() {
    let registry = worker_registry();
    let (on, mut queue) = TelemetryOn::with_event_queue(
        Arc::clone(&registry),
        DeclaredCodesByTask::none(),
        capacity(2),
        SentEventTypes::all(),
    )
    .expect("the telemetry of a worker registry");

    let emitted: Vec<Emitted> = (0..3).map(|slot| on.emit(|| slot_event(slot))).collect();

    assert_eq!(
        emitted,
        vec![
            Emitted::Offered(Offer::Queued),
            Emitted::Offered(Offer::Queued),
            Emitted::Offered(Offer::Dropped(EventDropCause::QueueFull)),
        ]
    );
    assert_eq!(
        series_of(&registry, MetricId::EventsDropped),
        vec![(vec!["queue_full".to_owned()], SampleValue::Counter(1))]
    );
    match queue.next_batch(capacity(8)).await {
        EventBatch::Events(events) => assert_eq!(slots_of(&events), vec![0, 1]),
        EventBatch::LinkClosed => panic!("the queue holds two events"),
    }
}

/// The slot of each event of `events`, in order.
fn slots_of(events: &[Event]) -> Vec<u32> {
    events
        .iter()
        .map(|event| match event {
            Event::SlotRecovered(recovered) => recovered.slot.get(),
            other => panic!("the queue holds only slot events, not {other:?}"),
        })
        .collect()
}

#[tokio::test]
async fn an_offer_after_the_reader_is_gone_drops_the_event_and_counts_child_down() {
    let registry = worker_registry();
    let (on, queue) = TelemetryOn::with_event_queue(
        Arc::clone(&registry),
        DeclaredCodesByTask::none(),
        capacity(2),
        SentEventTypes::all(),
    )
    .expect("the telemetry of a worker registry");
    drop(queue);

    let emitted = on.emit(|| slot_event(0));

    assert_eq!(
        emitted,
        Emitted::Offered(Offer::Dropped(EventDropCause::ChildDown))
    );
    assert_eq!(
        series_of(&registry, MetricId::EventsDropped),
        vec![(vec!["child_down".to_owned()], SampleValue::Counter(1))]
    );
}

#[tokio::test]
async fn the_queue_ends_after_the_link_is_gone_and_its_events_are_read() {
    let (on, mut queue) = TelemetryOn::with_event_queue(
        worker_registry(),
        DeclaredCodesByTask::none(),
        capacity(4),
        SentEventTypes::all(),
    )
    .expect("the telemetry of a worker registry");
    let _first = on.emit(|| slot_event(0));
    drop(on);

    let first = queue.next_batch(capacity(4)).await;
    let second = queue.next_batch(capacity(4)).await;

    assert!(
        matches!(&first, EventBatch::Events(events) if events.len() == 1),
        "{first:?}"
    );
    assert!(matches!(second, EventBatch::LinkClosed), "{second:?}");
}

/// The worker sends its handler child only the event types that the child
/// takes: an event of another type is not built and not counted as a drop.
#[test]
fn an_event_of_a_type_that_the_child_does_not_take_is_not_built() {
    let registry = worker_registry();
    let (on, mut queue) = TelemetryOn::with_event_queue(
        Arc::clone(&registry),
        DeclaredCodesByTask::none(),
        capacity(4),
        SentEventTypes::of([EventTypeName::SlotRecovered]),
    )
    .expect("the telemetry of a worker registry");
    let built = Cell::new(false);

    let not_sent = on.emit(|| {
        built.set(true);
        events::ChildStarted {
            at: Utc::now(),
            child_kind: Execution::Isolated,
            slot: SlotIndex::try_from(0).unwrap(),
            child_pid: crate::worker::executor::protocol::ProcessId::new(7).unwrap(),
            boot_duration: std::time::Duration::from_millis(5),
        }
    });
    let sent = on.emit(|| slot_event(1));

    assert_eq!(not_sent, Emitted::TypeNotSent);
    assert!(!built.get());
    assert_eq!(sent, Emitted::Offered(Offer::Queued));
    assert_eq!(slots_of(&queue.take_waiting(capacity(8))), vec![1]);
    assert!(series_of(&registry, MetricId::EventsDropped).is_empty());
}

/// A set of types holds each type that it was given, and no other.
#[test]
fn a_set_of_sent_types_holds_exactly_its_types() {
    let sent = SentEventTypes::of([
        EventTypeName::TaskClaimed,
        EventTypeName::AttemptEnded,
        EventTypeName::TaskClaimed,
    ]);

    let held: Vec<EventTypeName> = EventTypeName::ALL
        .iter()
        .copied()
        .filter(|name| sent.sends(*name))
        .collect();

    assert_eq!(
        held,
        vec![EventTypeName::TaskClaimed, EventTypeName::AttemptEnded]
    );
    assert!(EventTypeName::ALL
        .iter()
        .all(|name| SentEventTypes::all().sends(*name)));
}

#[test]
fn with_no_handler_child_a_point_builds_no_event() {
    let registry = worker_registry();
    let on = TelemetryOn::metrics_only(Arc::clone(&registry), DeclaredCodesByTask::none())
        .expect("the telemetry of a worker registry");
    let built = Cell::new(false);

    let emitted = on.emit(|| {
        built.set(true);
        slot_event(0)
    });

    assert_eq!(emitted, Emitted::NotBuilt);
    assert!(!built.get());
    assert!(series_of(&registry, MetricId::EventsDropped).is_empty());
}

#[test]
fn the_reaper_counts_its_failures_only_with_telemetry_on() {
    let off = WorkerTelemetry::Off.recovery_failures();
    let on = WorkerTelemetry::On(Arc::new(
        TelemetryOn::metrics_only(worker_registry(), DeclaredCodesByTask::none())
            .expect("the telemetry of a worker registry"),
    ))
    .recovery_failures();

    assert!(matches!(off, OptionalCounter::NotRecorded));
    assert!(matches!(on, OptionalCounter::Recording(_)));
}

/// A plain task of `execution` at attempt 3, on queue `default`.
fn claimed_task(execution: Execution) -> crate::worker::executor::slots::ClaimedTask {
    use crate::worker::executor::slots::{ClaimedTask, RetryFacts, TaskKind};
    ClaimedTask {
        task_id: uuid::Uuid::from_u128(7),
        task_name: "app.tasks.send_email".to_owned(),
        queue_name: "default".to_owned(),
        claimed_at: Utc::now(),
        execution,
        kind: TaskKind::Plain,
        retry: RetryFacts {
            retry_count: 2,
            max_retries: 3,
        },
        options: crate::worker::retry::read_stored_options(None),
        trace: crate::events::RunTrace::NoCarrier,
        trace_id: crate::telemetry::trace_carrier::StoredTraceId::NoTraceId,
    }
}

/// A placement of a task on an isolated child, one second after `read`.
fn placed_after(read: ClaimRead) -> crate::worker::executor::slots::Placed {
    use crate::worker::executor::protocol::ProcessId;
    crate::worker::executor::slots::Placed {
        child_pid: ProcessId::new(4242).unwrap(),
        child_kind: Execution::Isolated,
        at: read.at + std::time::Duration::from_secs(1),
        wall: read.wall + chrono::Duration::seconds(1),
    }
}

#[test]
fn a_placed_task_gives_one_attempt_placed_with_its_child_and_one_slot_wait() {
    let mut watched = WatchedTelemetry::new();
    let task = claimed_task(Execution::SharedAsync);
    let read = ClaimRead::now();
    let placed = placed_after(read);

    watched
        .telemetry
        .attempt_placed(&task, read, &ChildPlacement::Placed(placed));

    let events = watched.events();
    assert_eq!(events.len(), 1, "{events:?}");
    match &events[0] {
        Event::AttemptPlaced(event) => {
            assert_eq!(event.task_id, task.task_id);
            assert_eq!(event.execution, Execution::SharedAsync);
            assert_eq!(event.child_kind, Execution::Isolated);
            assert_eq!(event.child_pid, placed.child_pid);
            assert_eq!(event.attempt.get(), 3);
            assert_eq!(event.at, placed.wall);
            assert_eq!(event.slot_wait, std::time::Duration::from_secs(1));
        }
        other => panic!("the placement emits AttemptPlaced, not {other:?}"),
    }
    match watched.series(MetricId::TaskSlotWaitDuration).as_slice() {
        [(labels, SampleValue::Histogram { buckets, .. })] => {
            assert_eq!(labels, &vec!["SHARED_ASYNC".to_owned()]);
            assert_eq!(buckets.iter().sum::<u64>(), 1);
        }
        other => panic!("one slot wait series, not {other:?}"),
    }
}

#[test]
fn a_task_that_no_child_took_or_telemetry_off_gives_no_placement() {
    use crate::worker::executor::slots::{PlacementReceipt, PlacementWatch};
    let mut watched = WatchedTelemetry::new();
    let task = claimed_task(Execution::Isolated);
    let read = ClaimRead::now();

    watched
        .telemetry
        .attempt_placed(&task, read, &ChildPlacement::NotPlaced);
    WorkerTelemetry::Off.attempt_placed(&task, read, &ChildPlacement::Placed(placed_after(read)));

    assert!(watched.events().is_empty());
    assert!(watched.series(MetricId::TaskSlotWaitDuration).is_empty());
    assert!(matches!(
        WorkerTelemetry::Off.placement_watch(),
        (PlacementWatch::Unwatched, PlacementReceipt::Unwatched)
    ));
}

/// A watched telemetry value whose registry declares the queue `default`
/// and the task `app.tasks.send_email`, which declares `RATE_LIMITED`.
fn declaring_telemetry() -> WatchedTelemetry {
    use crate::core::task::DeclaredErrorCodes;
    use crate::telemetry::declared::{DeclaredNames, ErrorCodeValues};
    use crate::telemetry::registry::{ProcessFacts, RegistryLabels, WorkerFacts};
    let declared = DeclaredErrorCodes::parse(["RATE_LIMITED"]).unwrap();
    let registry = Registry::for_worker(RegistryLabels {
        tasks: DeclaredNames::new(["app.tasks.send_email"]).declared,
        queues: DeclaredNames::new(["default"]).declared,
        handlers: DeclaredNames::none(),
        schedules: DeclaredNames::none(),
        error_codes: ErrorCodeValues::new([&declared]).declared,
        process: ProcessFacts::Worker(WorkerFacts {
            version: "0.8.0".to_owned(),
            profile: "default".to_owned(),
            worker_id: "worker-1".to_owned(),
        }),
    })
    .unwrap();
    let codes = DeclaredCodesByTask::new(
        [("app.tasks.send_email".to_owned(), declared)]
            .into_iter()
            .collect(),
    );
    WatchedTelemetry::with(Arc::new(registry), codes)
}

fn user_code(code: &str) -> TaskErrorCode {
    TaskErrorCode::User(UserCode::parse(code).unwrap())
}

/// How the event of a run end is expected to read.
#[derive(Debug)]
enum ExpectedEnd {
    Reported(ChildResult),
    Decided(WorkerDecision),
}

/// Each end of a run on a child gives one `AttemptEnded` with its end, and
/// one count of `horsies.task.attempts` with its outcome and code label: a
/// declared user code is its own label, another user code is `undeclared`.
#[test]
fn each_end_of_a_run_gives_one_attempt_ended_with_its_outcome_and_code() {
    let value = ResultRead::Value;
    let declared = ResultRead::TaskError(user_code("RATE_LIMITED"));
    let other = ResultRead::TaskError(user_code("OTHER_CODE"));
    let unreadable = ResultRead::Unreadable;
    let failed_with = |code: &str| {
        ExpectedEnd::Reported(ChildResult::Failed {
            error_code: AttemptErrorCode::from(&user_code(code)),
        })
    };
    let cases: Vec<(EndView<'_>, ExpectedEnd, &str, &str)> = vec![
        (
            EndView::Result {
                read: &value,
                start: AttemptStart::NotReported,
            },
            ExpectedEnd::Reported(ChildResult::Completed),
            "COMPLETED",
            "",
        ),
        (
            EndView::Result {
                read: &declared,
                start: AttemptStart::NotReported,
            },
            failed_with("RATE_LIMITED"),
            "FAILED",
            "RATE_LIMITED",
        ),
        (
            EndView::Result {
                read: &other,
                start: AttemptStart::NotReported,
            },
            failed_with("OTHER_CODE"),
            "FAILED",
            "undeclared",
        ),
        (
            EndView::Result {
                read: &unreadable,
                start: AttemptStart::NotReported,
            },
            ExpectedEnd::Reported(ChildResult::Failed {
                error_code: AttemptErrorCode::BuiltIn {
                    code: OperationalErrorCode::WorkerSerializationError.into(),
                },
            }),
            "FAILED",
            "WORKER_SERIALIZATION_ERROR",
        ),
        (
            EndView::WorkerFailure {
                start: AttemptStart::NotReported,
            },
            ExpectedEnd::Reported(ChildResult::WorkerFailure),
            "WORKER_FAILURE",
            "BROKER_ERROR",
        ),
        (
            EndView::TimedOut,
            ExpectedEnd::Decided(WorkerDecision::TimedOut),
            "FAILED",
            "TASK_TIMEOUT",
        ),
        (
            EndView::KilledForLoop(LoopKillCode::TaskBlockedLoop),
            ExpectedEnd::Decided(WorkerDecision::TaskBlockedLoop),
            "FAILED",
            "TASK_BLOCKED_LOOP",
        ),
        (
            EndView::KilledForLoop(LoopKillCode::NeighbourBlockedLoop),
            ExpectedEnd::Decided(WorkerDecision::NeighbourBlockedLoop),
            "FAILED",
            "NEIGHBOUR_BLOCKED_LOOP",
        ),
        (
            EndView::Crashed,
            ExpectedEnd::Decided(WorkerDecision::ChildLost),
            "FAILED",
            "WORKER_CRASHED",
        ),
    ];
    for (view, expected, outcome, code) in cases {
        let mut watched = declaring_telemetry();
        let task = claimed_task(Execution::Isolated);
        let read = ClaimRead::now();
        let placed = placed_after(read);

        let noted = watched
            .telemetry
            .attempt_ended(&task, &ChildPlacement::Placed(placed), view);

        let events = watched.events();
        let expected_end = match expected {
            ExpectedEnd::Reported(result) => events::AttemptEnd::ReportedByChild {
                result,
                start: AttemptStart::NotReported,
            },
            ExpectedEnd::Decided(reason) => events::AttemptEnd::DecidedByWorker {
                reason,
                trace: RunTrace::NoCarrier,
                placed_at: placed.wall,
            },
        };
        match events.as_slice() {
            [Event::AttemptEnded(ended)] => {
                assert_eq!(ended.end, expected_end, "{view:?}");
                assert_eq!(ended.execution, Execution::Isolated, "{view:?}");
                assert_eq!(ended.attempt.get(), 3, "{view:?}");
            }
            other => panic!("{view:?} gives one AttemptEnded, not {other:?}"),
        }
        assert!(matches!(noted, EndNoted::Seen { .. }), "{view:?}");
        assert_eq!(
            watched.series(MetricId::TaskAttempts),
            vec![(
                vec![
                    "app.tasks.send_email".to_owned(),
                    "default".to_owned(),
                    "ISOLATED".to_owned(),
                    outcome.to_owned(),
                    code.to_owned(),
                ],
                SampleValue::Counter(1)
            )],
            "{view:?}"
        );
    }
}

/// The start that the child reported reaches the `AttemptEnded` of a
/// reported end with its time: a result and a worker failure that report a
/// start carry it, and a worker failure that came before the start write
/// returned carries `NotReported`.
#[test]
fn a_reported_end_carries_the_start_that_its_child_reported() {
    let started_at =
        chrono::DateTime::<Utc>::from_timestamp(1_767_225_600, 0).expect("a time in range");
    let reported = AttemptStart::Reported { started_at };
    let value = ResultRead::Value;
    let cases = [
        (
            EndView::Result {
                read: &value,
                start: reported,
            },
            ChildResult::Completed,
            reported,
        ),
        (
            EndView::WorkerFailure { start: reported },
            ChildResult::WorkerFailure,
            reported,
        ),
        (
            EndView::WorkerFailure {
                start: AttemptStart::NotReported,
            },
            ChildResult::WorkerFailure,
            AttemptStart::NotReported,
        ),
    ];
    for (view, result, start) in cases {
        let mut watched = declaring_telemetry();
        let task = claimed_task(Execution::Isolated);
        let placed = placed_after(ClaimRead::now());

        let _noted = watched
            .telemetry
            .attempt_ended(&task, &ChildPlacement::Placed(placed), view);

        match watched.events().as_slice() {
            [Event::AttemptEnded(ended)] => assert_eq!(
                ended.end,
                events::AttemptEnd::ReportedByChild { result, start },
                "{view:?}"
            ),
            other => panic!("{view:?} gives one AttemptEnded, not {other:?}"),
        }
    }
}

/// A child killed with its exit not seen: `NotKnown`, counted in
/// `end_not_known` and in no outcome.
#[test]
fn an_end_not_known_counts_apart_from_the_outcomes() {
    let mut watched = declaring_telemetry();
    let task = claimed_task(Execution::Isolated);
    let placed = placed_after(ClaimRead::now());

    let _noted = watched.telemetry.attempt_ended(
        &task,
        &ChildPlacement::Placed(placed),
        EndView::EndUnknown {
            pid: placed.child_pid,
        },
    );

    match watched.events().as_slice() {
        [Event::AttemptEnded(ended)] => assert_eq!(
            ended.end,
            events::AttemptEnd::NotKnown {
                child_pid: placed.child_pid
            }
        ),
        other => panic!("one AttemptEnded, not {other:?}"),
    }
    assert!(watched.series(MetricId::TaskAttempts).is_empty());
    assert_eq!(
        watched.series(MetricId::TaskAttemptsEndNotKnown),
        vec![(
            vec![
                "app.tasks.send_email".to_owned(),
                "default".to_owned(),
                "ISOLATED".to_owned(),
            ],
            SampleValue::Counter(1)
        )]
    );
}

/// A stop before RUNNING gives `AttemptNotStarted` and no `AttemptEnded`; a
/// task that reached no child gives `ClaimReleased` and nothing to record.
#[test]
fn a_stop_before_running_and_a_task_with_no_child_give_their_own_events() {
    let mut watched = declaring_telemetry();
    let task = claimed_task(Execution::Isolated);

    let stopped = watched.telemetry.attempt_ended(
        &task,
        &ChildPlacement::Placed(placed_after(ClaimRead::now())),
        EndView::StoppedBeforeRunning(StopReason::WorkflowStopped),
    );
    let not_run = watched.telemetry.attempt_ended(
        &task,
        &ChildPlacement::NotPlaced,
        EndView::NotDispatched(NotRunCause::QueueFull),
    );

    match watched.events().as_slice() {
        [Event::AttemptNotStarted(not_started), Event::ClaimReleased(released)] => {
            assert_eq!(not_started.reason, StopReason::WorkflowStopped);
            assert_eq!(released.reason, ReleaseReason::QueueFull);
        }
        other => panic!("AttemptNotStarted then ClaimReleased, not {other:?}"),
    }
    assert!(matches!(stopped, EndNoted::Seen { .. }));
    assert_eq!(not_run, EndNoted::NothingToRecord);
    assert_eq!(
        watched.series(MetricId::TaskStartsRefused),
        vec![(vec!["workflow_stopped".to_owned()], SampleValue::Counter(1))]
    );
    assert_eq!(
        watched.series(MetricId::TaskClaimsReleased),
        vec![(vec!["queue_full".to_owned()], SampleValue::Counter(1))]
    );
}

/// Each settlement gives one `AttemptRecorded` with its record; a scheduled
/// retry also counts in `horsies.task.retries` under the code of the end.
#[test]
fn each_settlement_gives_one_attempt_recorded_with_its_record() {
    let next_retry_at = Utc::now();
    let cases = [
        (
            Settlement::Written(WrittenEnd::Terminal(TerminalState::Completed)),
            AttemptRecord::Completed,
        ),
        (
            Settlement::Written(WrittenEnd::Terminal(TerminalState::Failed)),
            AttemptRecord::Failed,
        ),
        (
            Settlement::Written(WrittenEnd::RetryScheduled {
                retry_count: 4,
                next_retry_at,
            }),
            AttemptRecord::RetryScheduled {
                retry_count: RetryCount::try_from(4).unwrap(),
                next_retry_at,
            },
        ),
        (
            Settlement::Written(WrittenEnd::ClaimReleased),
            AttemptRecord::ClaimReleased,
        ),
        (Settlement::NothingOwed, AttemptRecord::NothingOwed),
        (Settlement::NotHeld, AttemptRecord::NotHeld),
        (Settlement::LeftToRecovery, AttemptRecord::LeftToRecovery),
        (
            Settlement::RetriesExhausted,
            AttemptRecord::RetriesExhausted,
        ),
        (Settlement::WriteFailed, AttemptRecord::WriteFailed),
    ];
    for (settled, record) in cases {
        let mut watched = declaring_telemetry();
        let task = claimed_task(Execution::SharedAsync);
        let declared = user_code("RATE_LIMITED");
        let noted = EndNoted::Seen {
            seen: tokio::time::Instant::now(),
            error_code: watched.registry.labels().error_codes.value_of(
                crate::core::task::error_code_label(
                    &crate::core::task::DeclaredErrorCodes::parse(["RATE_LIMITED"]).unwrap(),
                    &declared,
                ),
            ),
        };

        watched.telemetry.attempt_recorded(&task, noted, settled);

        match watched.events().as_slice() {
            [Event::AttemptRecorded(recorded)] => {
                assert_eq!(recorded.record, record, "{settled:?}");
            }
            other => panic!("{settled:?} gives one AttemptRecorded, not {other:?}"),
        }
        let retries = match settled {
            Settlement::Written(WrittenEnd::RetryScheduled { .. }) => vec![(
                vec![
                    "app.tasks.send_email".to_owned(),
                    "default".to_owned(),
                    "RATE_LIMITED".to_owned(),
                ],
                SampleValue::Counter(1),
            )],
            _ => Vec::new(),
        };
        assert_eq!(
            watched.series(MetricId::TaskRetries),
            retries,
            "{settled:?}"
        );
    }
}

/// Nothing to record, or telemetry off: no `AttemptRecorded`.
#[test]
fn nothing_to_record_gives_no_attempt_recorded() {
    let mut watched = declaring_telemetry();
    let task = claimed_task(Execution::Isolated);
    let settled = Settlement::Written(WrittenEnd::Terminal(TerminalState::Completed));

    watched
        .telemetry
        .attempt_recorded(&task, EndNoted::NothingToRecord, settled);
    WorkerTelemetry::Off.attempt_recorded(
        &task,
        EndNoted::Seen {
            seen: tokio::time::Instant::now(),
            error_code: crate::telemetry::declared::ErrorCodeValue::NO_ERROR_CODE,
        },
        settled,
    );

    assert!(watched.events().is_empty());
    assert!(watched.series(MetricId::TaskRecordDuration).is_empty());
}

/// Each finalize write sent again counts once under its stage.
#[test]
fn each_finalize_retry_counts_under_its_stage() {
    use crate::worker::executor::slots::outcome::WriteStage;
    let watched = declaring_telemetry();

    watched.telemetry.finalize_retried(WriteStage::PhaseOne);
    watched.telemetry.finalize_retried(WriteStage::PhaseOne);
    watched
        .telemetry
        .finalize_retried(WriteStage::CrashRecovery);
    WorkerTelemetry::Off.finalize_retried(WriteStage::TimeoutWrite);

    assert_eq!(
        watched.series(MetricId::FinalizeRetries),
        vec![
            (vec!["future".to_owned()], SampleValue::Counter(1)),
            (vec!["phase1_persist".to_owned()], SampleValue::Counter(2)),
        ]
    );
}

#[test]
fn each_recovered_row_gives_one_task_recovered_with_its_action_and_one_count() {
    use crate::broker::stale_recovery::RecoveredTask;
    use crate::telemetry::labels::RecoveryAction;
    let mut watched = WatchedTelemetry::new();
    let requeued = [7_u128, 8].map(|id| RecoveredTask {
        task_id: uuid::Uuid::from_u128(id),
        task_name: "app.tasks.send_email".to_owned(),
        queue_name: "default".to_owned(),
    });

    watched.telemetry.task_recovered(
        uuid::Uuid::from_u128(6),
        "app.tasks.report",
        "slow",
        RecoveryAction::RunningFailed,
    );
    watched
        .telemetry
        .tasks_recovered(&requeued, RecoveryAction::ClaimRequeued);

    let told: Vec<(u128, RecoveryAction, String, String)> = watched
        .events()
        .into_iter()
        .map(|event| match event {
            Event::TaskRecovered(recovered) => (
                recovered.task_id.as_u128(),
                recovered.action,
                recovered.task_name.as_str().to_owned(),
                recovered.queue.as_str().to_owned(),
            ),
            other => panic!("only TaskRecovered, not {other:?}"),
        })
        .collect();
    assert_eq!(
        told,
        vec![
            (
                6,
                RecoveryAction::RunningFailed,
                "app.tasks.report".to_owned(),
                "slow".to_owned()
            ),
            (
                7,
                RecoveryAction::ClaimRequeued,
                "app.tasks.send_email".to_owned(),
                "default".to_owned()
            ),
            (
                8,
                RecoveryAction::ClaimRequeued,
                "app.tasks.send_email".to_owned(),
                "default".to_owned()
            ),
        ]
    );
    assert_eq!(
        watched.series(MetricId::RecoveryTasks),
        vec![
            (vec!["claim_requeued".to_owned()], SampleValue::Counter(2)),
            (vec!["running_failed".to_owned()], SampleValue::Counter(1)),
        ]
    );
}

#[test]
fn with_telemetry_off_a_recovered_row_tells_nothing() {
    use crate::broker::stale_recovery::RecoveredTask;
    use crate::telemetry::labels::RecoveryAction;
    let watched = WatchedTelemetry::new();
    let off = WorkerTelemetry::Off;

    off.task_recovered(
        uuid::Uuid::from_u128(6),
        "app.tasks.report",
        "slow",
        RecoveryAction::RunningRetried,
    );
    off.tasks_recovered(
        &[RecoveredTask {
            task_id: uuid::Uuid::from_u128(7),
            task_name: "app.tasks.send_email".to_owned(),
            queue_name: "default".to_owned(),
        }],
        RecoveryAction::ClaimRequeued,
    );

    assert!(watched.series(MetricId::RecoveryTasks).is_empty());
}
