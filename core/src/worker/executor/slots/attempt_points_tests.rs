//! The lifecycle points of one attempt through the dispatch of the worker
//! ([`crate::worker::worker::follow_attempt`]), on a scripted isolated child
//! and a writer that settles every end as its script says: each event once
//! per step, in order; none with telemetry off.

use std::time::Duration;

use chrono::Utc;

use super::outcome::{read_attempt_end, EndView, EndWrites, ReadEnd};
use super::scripted::{
    answer_runs, boot, idle_child, input, isolated_config, isolated_init, result_frame, shell,
    start_isolated, task, SPAWNER_STOP_WAIT,
};
use super::*;
use crate::broker::claims::ClaimRead;
use crate::core::config::payload::PayloadPolicy;
use crate::core::task::error::TaskError;
use crate::core::task::result::TaskResult;
use crate::events::{AttemptRecord, AttemptStart, ChildResult, Event};
use crate::telemetry::catalog::MetricId;
use crate::telemetry::labels::ReleaseReason;
use crate::telemetry::registry::SampleValue;
use crate::telemetry::worker::WorkerTelemetry;
use crate::test_support::telemetry::WatchedTelemetry;
use crate::worker::executor::protocol::{
    ChildAfterFrame, ChildFrame, IsolatedWork, NoLeftWork, ResultEnvelopeText, ResultFrame,
    RssReading, RunEnd,
};
use crate::worker::executor::supervisor::Spawner;
use crate::worker::worker::follow_attempt;

/// The longest wait for one attempt of these tests.
const ATTEMPT_WAIT: Duration = Duration::from_secs(60);

/// A writer that reads each end as the outcome writer does and settles it
/// as `settlement`, with no database.
struct ScriptedWrites {
    settlement: Settlement,
}

impl EndWrites for ScriptedWrites {
    type Read = ReadEnd;

    fn read_end(&self, task: &ClaimedTask, end: AttemptEnd) -> ReadEnd {
        read_attempt_end(task, end, "worker-1", &PayloadPolicy::default())
    }

    fn view(read: &ReadEnd) -> EndView<'_> {
        read.view()
    }

    async fn write_end(&self, _task: &ClaimedTask, _read: ReadEnd) -> Settlement {
        self.settlement
    }
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
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));
    let read = ClaimRead::now();
    let (watch, receipt) = telemetry.placement_watch();
    let submitted = pool.submit_watched(
        task,
        input(),
        IsolatedWork::Run,
        IsolatedDeadline::NoDeadline,
        watch,
    );
    let writer = ScriptedWrites { settlement };
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
                    start: AttemptStart::NotReported,
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
                    start: AttemptStart::NotReported,
                } => assert_eq!(
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
    let (watch, receipt) = watched.telemetry.placement_watch();
    let submitted = pool.submit_watched(
        &refused,
        input(),
        IsolatedWork::Run,
        IsolatedDeadline::NoDeadline,
        watch,
    );
    let writer = ScriptedWrites {
        settlement: Settlement::Written(WrittenEnd::ClaimReleased),
    };

    let _settled = follow_attempt(
        &writer,
        &watched.telemetry,
        &refused,
        ClaimRead::now(),
        receipt,
        submitted,
    )
    .await;

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
