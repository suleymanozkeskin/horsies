//! The lifecycle points of one attempt through the dispatch of the worker
//! ([`crate::worker::worker::follow_attempt`]), on a scripted isolated or
//! shared child and a writer that settles every end as its script says:
//! each event once per step, in order; none with telemetry off; the start
//! that the child reported in the `AttemptEnded` of a reported end; the
//! placement that the write of the end gets.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};

use super::outcome::{read_attempt_end, EndView, EndWrites, ReadEnd};
use super::scripted::{
    answer_runs, async_work, boot, idle_child, input, isolated_config, isolated_init, layout,
    ok_envelope, read_frame, result_frame, run_ref, shared_async_init, shared_config, shell,
    start_isolated, start_shared, started_at, task, SPAWNER_STOP_WAIT,
};
use super::*;
use crate::broker::claims::ClaimRead;
use crate::core::config::payload::PayloadPolicy;
use crate::core::lifecycle::AttemptPlacement;
use crate::core::task::error::TaskError;
use crate::core::task::result::TaskResult;
use crate::events::{AttemptRecord, AttemptStart, ChildResult, Event, WorkerDecision};
use crate::telemetry::catalog::MetricId;
use crate::telemetry::labels::ReleaseReason;
use crate::telemetry::registry::SampleValue;
use crate::telemetry::worker::WorkerTelemetry;
use crate::test_support::telemetry::WatchedTelemetry;
use crate::worker::executor::protocol::{
    ChildAfterFrame, ChildFrame, FailureDetail, IsolatedWork, NoLeftWork, ResultEnvelopeText,
    ResultFrame, RssReading, RunEnd, SeatEnd, SharedResultFrame,
};
use crate::worker::executor::supervisor::Spawner;
use crate::worker::worker::follow_attempt;

/// The longest wait for one attempt of these tests.
const ATTEMPT_WAIT: Duration = Duration::from_secs(60);

/// A writer that reads each end as the outcome writer does and settles it
/// as `settlement`, with no database. It keeps the placement of each write.
struct ScriptedWrites {
    settlement: Settlement,
    placements: Mutex<Vec<AttemptPlacement>>,
}

impl ScriptedWrites {
    fn settling(settlement: Settlement) -> Self {
        Self {
            settlement,
            placements: Mutex::new(Vec::new()),
        }
    }

    /// The placement of each write, in order.
    fn placements(&self) -> Vec<AttemptPlacement> {
        self.placements
            .lock()
            .expect("the placements lock is not poisoned")
            .clone()
    }
}

impl EndWrites for ScriptedWrites {
    type Read = ReadEnd;

    fn read_end(&self, task: &ClaimedTask, end: AttemptEnd) -> ReadEnd {
        read_attempt_end(task, end, &PayloadPolicy::default())
    }

    fn view(read: &ReadEnd) -> EndView<'_> {
        read.view()
    }

    async fn write_end(
        &self,
        _task: &ClaimedTask,
        _read: ReadEnd,
        placement: AttemptPlacement,
    ) -> Settlement {
        self.placements
            .lock()
            .expect("the placements lock is not poisoned")
            .push(placement);
        self.settlement
    }
}

/// The start that the scripted frames report: [`started_at`] of every
/// scripted frame.
fn scripted_start() -> AttemptStart {
    AttemptStart::Reported {
        started_at: started_at(),
    }
}

/// A start that no scripted frame reports.
fn distinct_start() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(1_767_225_600, 0).expect("a time in range")
}

/// The result frame of `task` with the error envelope of code `code`.
fn error_frame(task: &ClaimedTask, code: &str) -> ChildFrame {
    let error = TaskError::new(code, "the task failed").expect("a code of the task's own");
    let envelope = ResultEnvelopeText::new(
        serde_json::to_string(&TaskResult::<serde_json::Value>::Err(error))
            .expect("a task error holds only JSON values"),
    );
    ChildFrame::Result(ResultFrame {
        task_id: task.task_id,
        end: RunEnd::Result(envelope),
        started_at: scripted_start(),
        rss: RssReading::Unreadable,
        task_loop: NoLeftWork::NoTaskLoop,
        child: ChildAfterFrame::ServesOn,
    })
}

/// The result frame of an isolated `task` that ended as `end`, with the
/// start `start`.
fn frame_with_start(task: &ClaimedTask, end: RunEnd, start: AttemptStart) -> ChildFrame {
    ChildFrame::Result(ResultFrame {
        task_id: task.task_id,
        end,
        started_at: start,
        rss: RssReading::Unreadable,
        task_loop: NoLeftWork::NoTaskLoop,
        child: ChildAfterFrame::ServesOn,
    })
}

/// Run one attempt of `task` on a fresh isolated child that answers it with
/// `frame`, through the dispatch of the worker, with `telemetry` and a
/// writer that settles as `settlement`.
async fn run_attempt(
    telemetry: &WorkerTelemetry,
    task: &ClaimedTask,
    frame: ChildFrame,
    settlement: Settlement,
) -> Settlement {
    let script = format!(
        "{}{}",
        boot(&isolated_init()),
        answer_runs(&[(task, vec![frame])], 1)
    );
    run_scripted_attempt(telemetry, task, &script, settlement).await
}

/// Run one attempt of `task` on a fresh isolated child that runs `script`,
/// through the dispatch of the worker, with `telemetry` and a writer that
/// settles as `settlement`.
async fn run_scripted_attempt(
    telemetry: &WorkerTelemetry,
    task: &ClaimedTask,
    script: &str,
    settlement: Settlement,
) -> Settlement {
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(script)));
    let read = ClaimRead::now();
    let (watch, receipt) = watched_placement();
    let submitted = pool.submit_watched(
        task,
        input(),
        IsolatedWork::Run,
        IsolatedDeadline::NoDeadline,
        watch,
    );
    let writer = ScriptedWrites::settling(settlement);
    let settled = tokio::time::timeout(
        ATTEMPT_WAIT,
        follow_attempt(&writer, telemetry, task, read, receipt, submitted),
    )
    .await
    .expect("the attempt ends within the test wait");
    let _report = pool.stop(Duration::from_secs(5)).await;
    let _stopped = spawner.stop(SPAWNER_STOP_WAIT);
    settled
}

/// The name of each event, in order.
fn names(events: &[Event]) -> Vec<&'static str> {
    events.iter().map(|event| event.event_type().name).collect()
}

/// The count of the one series of a histogram, with its label texts.
fn histogram_count(watched: &WatchedTelemetry, metric: MetricId) -> Vec<(Vec<String>, u64)> {
    watched
        .series(metric)
        .into_iter()
        .map(|(labels, value)| match value {
            SampleValue::Histogram { buckets, .. } => (labels, buckets.iter().sum()),
            other => panic!("a histogram series holds no {other:?}"),
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_completed_attempt_is_placed_ended_and_recorded_once_each() {
    let mut watched = WatchedTelemetry::new();
    let completed = task(1, ExecutionClass::Isolated);
    let written = Settlement::Written(WrittenEnd::Terminal(TerminalState::Completed));

    let settled = run_attempt(
        &watched.telemetry,
        &completed,
        result_frame(&completed, 1),
        written,
    )
    .await;

    assert_eq!(settled, written);
    let events = watched.events();
    assert_eq!(
        names(&events),
        vec!["AttemptPlaced", "AttemptEnded", "AttemptRecorded"]
    );
    match (&events[0], &events[1], &events[2]) {
        (
            Event::AttemptPlaced(placed),
            Event::AttemptEnded(ended),
            Event::AttemptRecorded(record),
        ) => {
            assert_eq!(placed.child_kind, ExecutionClass::Isolated);
            assert_eq!(
                ended.end,
                crate::events::AttemptEnd::ReportedByChild {
                    result: ChildResult::Completed,
                    start: scripted_start(),
                }
            );
            assert_eq!(record.record, AttemptRecord::Completed);
            for task_id in [placed.task_id, ended.task_id, record.task_id] {
                assert_eq!(task_id, completed.task_id);
            }
        }
        other => panic!("placed, ended, recorded, not {other:?}"),
    }
    assert_eq!(
        watched.series(MetricId::TaskAttempts),
        vec![(
            vec![
                "(unregistered)".to_owned(),
                "(unknown)".to_owned(),
                "ISOLATED".to_owned(),
                "COMPLETED".to_owned(),
                String::new(),
            ],
            SampleValue::Counter(1)
        )]
    );
    for metric in [
        MetricId::TaskRunDuration,
        MetricId::TaskSlotWaitDuration,
        MetricId::TaskRecordDuration,
    ] {
        let counts = histogram_count(&watched, metric);
        assert_eq!(counts.len(), 1, "{metric:?}: {counts:?}");
        assert_eq!(counts[0].1, 1, "{metric:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_attempt_that_retries_counts_its_retry_under_its_code_once() {
    let mut watched = WatchedTelemetry::new();
    let failed = task(2, ExecutionClass::Isolated);
    let next_retry_at = Utc::now();
    let retried = Settlement::Written(WrittenEnd::RetryScheduled {
        retry_count: 1,
        next_retry_at,
    });

    let _settled = run_attempt(
        &watched.telemetry,
        &failed,
        error_frame(&failed, "RATE_LIMITED"),
        retried,
    )
    .await;

    let events = watched.events();
    assert_eq!(
        names(&events),
        vec!["AttemptPlaced", "AttemptEnded", "AttemptRecorded"]
    );
    match (&events[1], &events[2]) {
        (Event::AttemptEnded(ended), Event::AttemptRecorded(record)) => {
            match &ended.end {
                crate::events::AttemptEnd::ReportedByChild {
                    result: ChildResult::Failed { error_code },
                    start,
                } if *start == scripted_start() => assert_eq!(
                    error_code,
                    &crate::events::AttemptErrorCode::User {
                        code: crate::events::UserCodeText::new("RATE_LIMITED".to_owned()).unwrap(),
                    }
                ),
                other => panic!("a reported failure, not {other:?}"),
            }
            match record.record {
                AttemptRecord::RetryScheduled {
                    retry_count,
                    next_retry_at: due,
                } => {
                    assert_eq!(retry_count.get(), 1);
                    assert_eq!(due, next_retry_at);
                }
                other => panic!("a scheduled retry, not {other:?}"),
            }
        }
        other => panic!("ended, recorded, not {other:?}"),
    }
    // The task declares no code, so its user code is `undeclared` in every
    // label; the event keeps the exact code.
    assert_eq!(
        watched.series(MetricId::TaskRetries),
        vec![(
            vec![
                "(unregistered)".to_owned(),
                "(unknown)".to_owned(),
                "undeclared".to_owned(),
            ],
            SampleValue::Counter(1)
        )]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_that_reaches_no_child_is_released_once_and_not_placed() {
    let mut watched = WatchedTelemetry::new();
    let refused = task(3, ExecutionClass::Isolated);
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
    let writer = ScriptedWrites::settling(Settlement::Written(WrittenEnd::ClaimReleased));

    let _settled = follow_attempt(
        &writer,
        &watched.telemetry,
        &refused,
        ClaimRead::now(),
        receipt,
        submitted,
    )
    .await;

    assert_eq!(writer.placements(), vec![AttemptPlacement::NotObserved]);
    let events = watched.events();
    assert_eq!(names(&events), vec!["ClaimReleased"]);
    match &events[0] {
        Event::ClaimReleased(released) => {
            assert_eq!(released.reason, ReleaseReason::PoolStopping);
        }
        other => panic!("a release, not {other:?}"),
    }
    assert!(watched.series(MetricId::TaskSlotWaitDuration).is_empty());
    assert!(watched.series(MetricId::TaskRecordDuration).is_empty());
    let _report = pool.stop(Duration::from_secs(5)).await;
    let _stopped = spawner.stop(SPAWNER_STOP_WAIT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn with_telemetry_off_an_attempt_emits_nothing() {
    let mut watched = WatchedTelemetry::new();
    let completed = task(4, ExecutionClass::Isolated);
    let written = Settlement::Written(WrittenEnd::Terminal(TerminalState::Completed));

    let settled = run_attempt(
        &WorkerTelemetry::Off,
        &completed,
        result_frame(&completed, 1),
        written,
    )
    .await;

    assert_eq!(settled, written);
    assert!(watched.events().is_empty());
    for metric in [
        MetricId::TaskAttempts,
        MetricId::TaskRunDuration,
        MetricId::TaskSlotWaitDuration,
        MetricId::TaskRecordDuration,
    ] {
        assert!(watched.series(metric).is_empty(), "{metric:?}");
    }
}

/// The one `AttemptEnded` of `events`, after its placement and before its
/// record.
fn the_attempt_ended(events: &[Event]) -> &crate::events::AttemptEnded {
    assert_eq!(
        names(events),
        vec!["AttemptPlaced", "AttemptEnded", "AttemptRecorded"]
    );
    match &events[1] {
        Event::AttemptEnded(ended) => ended,
        other => panic!("an AttemptEnded, not {other:?}"),
    }
}

/// The start that an isolated child reported in its result frame reaches the
/// `AttemptEnded` of the attempt with its time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_start_reported_by_an_isolated_child_reaches_attempt_ended_with_its_time() {
    let mut watched = WatchedTelemetry::new();
    let completed = task(5, ExecutionClass::Isolated);
    let start = AttemptStart::Reported {
        started_at: distinct_start(),
    };
    let frame = frame_with_start(&completed, RunEnd::Result(ok_envelope(1)), start);

    let _settled = run_attempt(
        &watched.telemetry,
        &completed,
        frame,
        Settlement::Written(WrittenEnd::Terminal(TerminalState::Completed)),
    )
    .await;

    let events = watched.events();
    assert_eq!(
        the_attempt_ended(&events).end,
        crate::events::AttemptEnd::ReportedByChild {
            result: ChildResult::Completed,
            start,
        }
    );
}

/// The start that a shared child reported in its result frame reaches the
/// `AttemptEnded` of the attempt with its time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_start_reported_by_a_shared_child_reaches_attempt_ended_with_its_time() {
    let mut watched = WatchedTelemetry::new();
    let completed = task(6, ExecutionClass::SharedAsync);
    let start = AttemptStart::Reported {
        started_at: distinct_start(),
    };
    let frame = ChildFrame::SharedResult(SharedResultFrame {
        run: run_ref(&completed, 1),
        end: SeatEnd::Finished(RunEnd::Result(ok_envelope(1))),
        started_at: start,
        rss: RssReading::Unreadable,
        child: ChildAfterFrame::ServesOn,
    });
    let init = shared_async_init(1);
    let script = format!(
        "{}{}",
        boot(&init),
        answer_runs(&[(&completed, vec![frame])], 1)
    );
    let spawner = Spawner::start().unwrap();
    let board = Arc::new(PoolBoard::new(layout(0, 1, 0)));
    let pool = start_shared(&spawner, &board, shared_config(shell(&script), init));
    let (watch, receipt) = watched_placement();
    let submitted = pool.submit_watched(&completed, input(), async_work(60_000, 1_000), watch);
    let writer = ScriptedWrites::settling(Settlement::Written(WrittenEnd::Terminal(
        TerminalState::Completed,
    )));

    let _settled = tokio::time::timeout(
        ATTEMPT_WAIT,
        follow_attempt(
            &writer,
            &watched.telemetry,
            &completed,
            ClaimRead::now(),
            receipt,
            submitted,
        ),
    )
    .await
    .expect("the attempt ends within the test wait");
    let _report = pool.stop(Duration::from_secs(5)).await;
    let _stopped = spawner.stop(SPAWNER_STOP_WAIT);

    let events = watched.events();
    assert_eq!(
        the_attempt_ended(&events).end,
        crate::events::AttemptEnd::ReportedByChild {
            result: ChildResult::Completed,
            start,
        }
    );
}

/// A worker failure that its child reported before the start write
/// returned has no start: its `AttemptEnded` carries `NotReported`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_failure_before_the_start_reaches_attempt_ended_with_no_start() {
    let mut watched = WatchedTelemetry::new();
    let failed = task(7, ExecutionClass::Isolated);
    let end = RunEnd::WorkerFailure(FailureDetail::cut("the start write failed"));
    let frame = frame_with_start(&failed, end, AttemptStart::NotReported);

    let _settled = run_attempt(
        &watched.telemetry,
        &failed,
        frame,
        Settlement::Written(WrittenEnd::Terminal(TerminalState::Failed)),
    )
    .await;

    let events = watched.events();
    assert_eq!(
        the_attempt_ended(&events).end,
        crate::events::AttemptEnd::ReportedByChild {
            result: ChildResult::WorkerFailure,
            start: AttemptStart::NotReported,
        }
    );
}

/// An attempt that the worker ended (its child exited before a result
/// frame) is decided by the worker: its `AttemptEnded` asks the handler
/// child for the span of the attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_attempt_whose_child_was_lost_is_decided_by_the_worker() {
    let mut watched = WatchedTelemetry::new();
    let lost = task(8, ExecutionClass::Isolated);
    let script = format!("{}{}exit 9", boot(&isolated_init()), read_frame());

    let _settled = run_scripted_attempt(
        &watched.telemetry,
        &lost,
        &script,
        Settlement::Written(WrittenEnd::Terminal(TerminalState::Failed)),
    )
    .await;

    let events = watched.events();
    let ended = the_attempt_ended(&events);
    assert!(
        matches!(
            ended.end,
            crate::events::AttemptEnd::DecidedByWorker {
                reason: WorkerDecision::ChildLost,
                ..
            }
        ),
        "{:?}",
        ended.end
    );
}

/// A shared run that its child ended at the deadline is a timeout that the
/// child reported: its `AttemptEnded` is `ReportedByChild` with
/// `ChildResult::TimedOut` and the start of its `shared_result` frame, so
/// the handler child makes no second span of the attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timeout_that_a_shared_child_ended_is_reported_by_the_child() {
    let mut watched = WatchedTelemetry::new();
    let late = task(9, ExecutionClass::SharedAsync);
    let run = run_ref(&late, 1);
    let start = AttemptStart::Reported {
        started_at: distinct_start(),
    };
    let timed_out = ChildFrame::SharedResult(SharedResultFrame {
        run,
        end: SeatEnd::TimedOut,
        started_at: start,
        rss: RssReading::Unreadable,
        child: ChildAfterFrame::ServesOn,
    });
    let init = shared_async_init(1);
    let script = format!(
        "{}{}",
        boot(&init),
        answer_runs(
            &[(&late, vec![ChildFrame::DeadlineFired(run), timed_out])],
            1
        )
    );
    let spawner = Spawner::start().unwrap();
    let board = Arc::new(PoolBoard::new(layout(0, 1, 0)));
    let pool = start_shared(&spawner, &board, shared_config(shell(&script), init));
    let (watch, receipt) = watched_placement();
    let submitted = pool.submit_watched(&late, input(), async_work(1_000, 100), watch);
    let writer = ScriptedWrites::settling(Settlement::Written(WrittenEnd::Terminal(
        TerminalState::Failed,
    )));

    let _settled = tokio::time::timeout(
        ATTEMPT_WAIT,
        follow_attempt(
            &writer,
            &watched.telemetry,
            &late,
            ClaimRead::now(),
            receipt,
            submitted,
        ),
    )
    .await
    .expect("the attempt ends within the test wait");
    let _report = pool.stop(Duration::from_secs(5)).await;
    let _stopped = spawner.stop(SPAWNER_STOP_WAIT);

    let events = watched.events();
    let ended = the_attempt_ended(&events);
    assert_eq!(
        ended.end,
        crate::events::AttemptEnd::ReportedByChild {
            result: ChildResult::TimedOut,
            start,
        }
    );
}

/// With telemetry off the worker still follows the placement: the write of
/// the end of an attempt that a child took gets `Placed`, with the slot wait
/// from the read of the claim result to the placement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_write_of_a_placed_attempt_gets_its_slot_wait_with_telemetry_off() {
    let completed = task(9, ExecutionClass::Isolated);
    let script = format!(
        "{}{}",
        boot(&isolated_init()),
        answer_runs(&[(&completed, vec![result_frame(&completed, 1)])], 1)
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));
    let read = ClaimRead::now();
    let (watch, receipt) = watched_placement();
    let submitted = pool.submit_watched(
        &completed,
        input(),
        IsolatedWork::Run,
        IsolatedDeadline::NoDeadline,
        watch,
    );
    let writer = ScriptedWrites::settling(Settlement::Written(WrittenEnd::Terminal(
        TerminalState::Completed,
    )));

    let _settled = tokio::time::timeout(
        ATTEMPT_WAIT,
        follow_attempt(
            &writer,
            &WorkerTelemetry::Off,
            &completed,
            read,
            receipt,
            submitted,
        ),
    )
    .await
    .expect("the attempt ends within the test wait");
    let waited_at_most = read.at.elapsed();
    let _report = pool.stop(Duration::from_secs(5)).await;
    let _stopped = spawner.stop(SPAWNER_STOP_WAIT);

    match writer.placements().as_slice() {
        [AttemptPlacement::Placed { slot_wait }] => {
            assert!(slot_wait.duration() <= waited_at_most, "{slot_wait:?}");
        }
        other => panic!("one Placed write, not {other:?}"),
    }
}
