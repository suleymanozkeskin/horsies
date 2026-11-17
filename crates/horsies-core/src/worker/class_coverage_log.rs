//! The worker's log of pending streams that no live worker can run.
//!
//! After each reaper pass the reaper reads the fleet check
//! (`read_class_coverage`). A pending (queue, class) stream that no live
//! worker serves is an error; a stream whose serving workers have no
//! capacity in service is a warning; a failed check is a warning. Each
//! worker process logs one stream at most once per
//! [`CLASS_COVERAGE_LOG_INTERVAL`], and a failed check at most once per the
//! same interval.
//!
//! Python reference: `python/horsies/core/worker/class_coverage_log.py` and
//! `ReaperMixin._check_class_coverage` (`python/horsies/core/worker/reaper.py:630-664`).

use std::collections::BTreeMap;
use std::time::Duration;

use tokio::time::Instant;

use crate::broker::class_coverage::{ClassCoverage, StreamCoverage, StreamCoverageState};
use crate::broker::class_coverage_reads::read_class_coverage;
use crate::broker::result_types::BrokerResult;
use crate::broker::PostgresBroker;
use crate::core::task::Execution;

/// The least time between two log lines of one stream, or of a failed
/// check, from one worker process (Python `CLASS_COVERAGE_LOG_INTERVAL_S`,
/// `class_coverage_log.py:24-26`).
pub(crate) const CLASS_COVERAGE_LOG_INTERVAL: Duration = Duration::from_secs(300);

/// The log key of one pending stream or of a failed check (Python
/// `StreamKey` and `CheckFailedKey`, `class_coverage_log.py:29-42`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum CoverageLogKey {
    Stream {
        queue_name: String,
        execution: Execution,
    },
    CheckFailed,
}

/// Why no live worker can run a pending stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CoverageGap {
    /// No live worker serves the class on the queue (Python
    /// `StreamUnserved`).
    NoServingWorker,
    /// `workers` live workers serve the class on the queue, and none of them
    /// has a child or seat in service for it (Python `StreamWithoutCapacity`).
    NoCapacityInService { workers: usize },
}

/// A pending stream that no live worker can run. A served stream has no
/// value of this type, so it has no log line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UncoveredStream {
    pub(crate) queue_name: String,
    pub(crate) execution: Execution,
    pub(crate) gap: CoverageGap,
}

impl UncoveredStream {
    /// The stream as uncovered; `None` for a served stream.
    pub(crate) fn of(stream: &StreamCoverage) -> Option<Self> {
        let gap = match stream.state {
            StreamCoverageState::Served { .. } => return None,
            StreamCoverageState::WithoutCapacity { workers } => {
                CoverageGap::NoCapacityInService { workers }
            }
            StreamCoverageState::Unserved => CoverageGap::NoServingWorker,
        };
        Some(Self {
            queue_name: stream.queue_name.clone(),
            execution: stream.execution,
            gap,
        })
    }

    fn key(&self) -> CoverageLogKey {
        CoverageLogKey::Stream {
            queue_name: self.queue_name.clone(),
            execution: self.execution,
        }
    }

    /// The log text (Python `describe_uncovered`,
    /// `class_coverage_log.py:88-109`). Python quotes the queue name with
    /// `repr`; here it is in single quotes with no escapes.
    pub(crate) fn describe(&self) -> String {
        let subject = format!(
            "PENDING {} tasks on queue '{}'",
            self.execution.as_str(),
            self.queue_name
        );
        match self.gap {
            CoverageGap::NoServingWorker => format!(
                "{subject} wait: no live worker serves this class. {}",
                serving_workers_hint(self.execution)
            ),
            CoverageGap::NoCapacityInService { workers } => format!(
                "{subject} wait: {workers} live worker(s) serve this class, and none has a \
                 child in service for it. Check the down slots of those workers."
            ),
        }
    }

    /// The line of this stream: an error when no live worker serves it, a
    /// warning when its serving workers have no capacity in service (Python
    /// `reaper.py:656-660`).
    fn line(&self) -> CoverageLogLine {
        match self.gap {
            CoverageGap::NoServingWorker => CoverageLogLine::Error(self.describe()),
            CoverageGap::NoCapacityInService { .. } => CoverageLogLine::Warning(self.describe()),
        }
    }
}

/// What starts a worker that serves `execution` (Python
/// `_serving_workers_hint`, `class_coverage_log.py:112-125`).
fn serving_workers_hint(execution: Execution) -> String {
    match execution {
        Execution::Isolated => "Start a worker with isolated children on this queue. A shared \
                                task that went back to the queue as ISOLATED also waits here."
            .to_owned(),
        Execution::SharedAsync | Execution::SharedThread => format!(
            "Start a worker with {} children or with isolated children on this queue.",
            execution.as_str()
        ),
    }
}

/// One line of the fleet check log, at its level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CoverageLogLine {
    Error(String),
    Warning(String),
}

impl CoverageLogLine {
    fn emit(&self) {
        match self {
            Self::Error(text) => tracing::error!("{text}"),
            Self::Warning(text) => tracing::warn!("{text}"),
        }
    }
}

/// When this process last logged each key (Python `CoverageLogBound`,
/// `class_coverage_log.py:45-85`).
///
/// It holds only the keys logged within the last
/// [`CLASS_COVERAGE_LOG_INTERVAL`]: the uncovered streams that the checks of
/// that interval logged, plus the failed check. One check returns at most
/// `PENDING_STREAM_LIMIT` streams.
#[derive(Debug, Default)]
pub(crate) struct CoverageLogBound {
    last_logged_at: BTreeMap<CoverageLogKey, Instant>,
}

impl CoverageLogBound {
    /// Whether `key` may log at `now`. A `true` records it as logged at
    /// `now`. Keys logged an interval or more before `now` are dropped first.
    pub(crate) fn take(&mut self, key: CoverageLogKey, now: Instant) -> bool {
        self.last_logged_at.retain(|_, logged_at| {
            now.saturating_duration_since(*logged_at) < CLASS_COVERAGE_LOG_INTERVAL
        });
        if self.last_logged_at.contains_key(&key) {
            return false;
        }
        self.last_logged_at.insert(key, now);
        true
    }

    /// The uncovered streams of `streams` that may log at `now`, in the
    /// order given. Each one returned is recorded as logged.
    pub(crate) fn take_uncovered(
        &mut self,
        streams: &[StreamCoverage],
        now: Instant,
    ) -> Vec<UncoveredStream> {
        streams
            .iter()
            .filter_map(UncoveredStream::of)
            .filter(|stream| self.take(stream.key(), now))
            .collect()
    }
}

/// The fleet check state of one reaper loop (Python
/// `_ReaperPassState.class_coverage_checks_from` and `coverage_log`,
/// `python/horsies/core/worker/runtime.py:173-177`).
#[derive(Debug)]
pub(crate) struct ClassCoverageLog {
    /// The loop reads the fleet check from this time on.
    checks_from: Instant,
    bound: CoverageLogBound,
}

impl ClassCoverageLog {
    /// The state of a reaper loop that starts at `started_at` and passes
    /// every `check_interval`. The first check is one interval after the
    /// start, so the workers that start with this one have written their
    /// first snapshot (Python `reaper.py:747-750`).
    pub(crate) fn for_reaper_start(started_at: Instant, check_interval: Duration) -> Self {
        Self {
            checks_from: started_at + check_interval,
            bound: CoverageLogBound::default(),
        }
    }

    /// Whether the loop reads the fleet check at `now`.
    pub(crate) fn due(&self, now: Instant) -> bool {
        now >= self.checks_from
    }

    /// The lines that the fleet check read `coverage` gives at `now`, each
    /// within the bound (Python `_check_class_coverage` after its read,
    /// `reaper.py:645-664`):
    /// - a read: one error per unserved stream and one warning per stream
    ///   without capacity in service, in the order of the read;
    /// - a failed read: one warning `Class coverage check failed: <message>`.
    pub(crate) fn lines(
        &mut self,
        coverage: &BrokerResult<ClassCoverage>,
        now: Instant,
    ) -> Vec<CoverageLogLine> {
        match coverage {
            Ok(coverage) => self
                .bound
                .take_uncovered(&coverage.streams, now)
                .into_iter()
                .map(|stream| stream.line())
                .collect(),
            Err(error) => {
                if self.bound.take(CoverageLogKey::CheckFailed, now) {
                    vec![CoverageLogLine::Warning(format!(
                        "Class coverage check failed: {}",
                        error.message
                    ))]
                } else {
                    Vec::new()
                }
            }
        }
    }
}

/// One fleet check of the reaper loop, after its pass (Python
/// `ReaperMixin._check_class_coverage`, `reaper.py:630-664`).
///
/// Before the first check time of `log` it reads nothing. Then it reads the
/// fleet check on the main core pool of `broker`, as reserved work, and logs
/// the lines of [`ClassCoverageLog::lines`]. Reads only; a failed read is
/// one of the lines, not an error of the call.
pub(crate) async fn check_class_coverage(broker: &PostgresBroker, log: &mut ClassCoverageLog) {
    if !log.due(Instant::now()) {
        return;
    }
    let coverage = read_class_coverage(broker).await;
    for line in log.lines(&coverage, Instant::now()) {
        line.emit();
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::broker::result_types::{BrokerErrorCode, BrokerOperationError};

    fn stream(queue: &str, execution: Execution, state: StreamCoverageState) -> StreamCoverage {
        StreamCoverage {
            queue_name: queue.to_owned(),
            execution,
            state,
        }
    }

    fn unserved() -> StreamCoverage {
        stream(
            "default",
            Execution::Isolated,
            StreamCoverageState::Unserved,
        )
    }

    fn without_capacity() -> StreamCoverage {
        stream(
            "default",
            Execution::SharedAsync,
            StreamCoverageState::WithoutCapacity { workers: 2 },
        )
    }

    fn served() -> StreamCoverage {
        stream(
            "default",
            Execution::SharedThread,
            StreamCoverageState::Served {
                workers: 1,
                capacity: 4,
            },
        )
    }

    fn stream_key(queue: &str, execution: Execution) -> CoverageLogKey {
        CoverageLogKey::Stream {
            queue_name: queue.to_owned(),
            execution,
        }
    }

    fn coverage(streams: Vec<StreamCoverage>) -> BrokerResult<ClassCoverage> {
        Ok(ClassCoverage {
            checked_at: Utc
                .with_ymd_and_hms(2026, 11, 10, 12, 0, 0)
                .single()
                .expect("a valid instant"),
            live_workers: 1,
            streams,
        })
    }

    /// A log whose checks are due at `now`.
    fn due_log(now: Instant) -> ClassCoverageLog {
        ClassCoverageLog::for_reaper_start(now, Duration::ZERO)
    }

    /// Python `TestCoverageLogBound::test_a_key_logs_once_per_interval`.
    #[test]
    fn a_key_logs_once_per_interval() {
        let start = Instant::now();
        let mut bound = CoverageLogBound::default();
        let key = stream_key("default", Execution::Isolated);

        assert!(bound.take(key.clone(), start));
        assert!(!bound.take(
            key.clone(),
            start + CLASS_COVERAGE_LOG_INTERVAL - Duration::from_secs(1)
        ));
        assert!(bound.take(key, start + CLASS_COVERAGE_LOG_INTERVAL));
    }

    /// Python `TestCoverageLogBound::test_keys_are_bounded_independently`.
    #[test]
    fn keys_are_bounded_independently() {
        let start = Instant::now();
        let mut bound = CoverageLogBound::default();

        assert!(bound.take(stream_key("a", Execution::Isolated), start));
        assert!(bound.take(stream_key("b", Execution::Isolated), start));
        assert!(bound.take(stream_key("a", Execution::SharedAsync), start));
        assert!(bound.take(CoverageLogKey::CheckFailed, start));
    }

    /// Python `TestCoverageLogBound::test_keys_past_the_interval_are_dropped`.
    #[test]
    fn keys_past_the_interval_are_dropped() {
        let start = Instant::now();
        let mut bound = CoverageLogBound::default();

        let _logged_a = bound.take(stream_key("a", Execution::Isolated), start);
        let _logged_b = bound.take(
            stream_key("b", Execution::Isolated),
            start + CLASS_COVERAGE_LOG_INTERVAL,
        );

        let kept: Vec<&CoverageLogKey> = bound.last_logged_at.keys().collect();
        assert_eq!(kept, [&stream_key("b", Execution::Isolated)]);
    }

    /// Python `TestCoverageLogBound::test_served_streams_never_log`.
    #[test]
    fn served_streams_never_log() {
        let mut bound = CoverageLogBound::default();

        assert!(bound.take_uncovered(&[served()], Instant::now()).is_empty());
        assert!(bound.last_logged_at.is_empty());
    }

    /// Python `TestCoverageLogBound::test_unserved_and_capacity_less_streams_log_in_order`.
    #[test]
    fn unserved_and_capacity_less_streams_log_in_order() {
        let mut bound = CoverageLogBound::default();

        let due = bound.take_uncovered(&[unserved(), served(), without_capacity()], Instant::now());

        assert_eq!(
            due,
            [
                UncoveredStream::of(&unserved()).expect("uncovered"),
                UncoveredStream::of(&without_capacity()).expect("uncovered"),
            ]
        );
    }

    /// Python `TestCoverageLogBound::test_a_stream_does_not_log_again_within_the_interval`.
    #[test]
    fn a_stream_does_not_log_again_within_the_interval() {
        let start = Instant::now();
        let mut bound = CoverageLogBound::default();
        let _first = bound.take_uncovered(&[unserved()], start);

        assert!(bound
            .take_uncovered(&[unserved()], start + Duration::from_secs(1))
            .is_empty());
        assert_eq!(
            bound
                .take_uncovered(&[unserved()], start + CLASS_COVERAGE_LOG_INTERVAL)
                .len(),
            1
        );
    }

    /// Python `TestDescribeUncovered::test_an_unserved_isolated_stream_names_the_isolated_retry`.
    #[test]
    fn an_unserved_isolated_stream_names_the_isolated_retry() {
        let text = UncoveredStream::of(&unserved())
            .expect("uncovered")
            .describe();

        assert_eq!(
            text,
            "PENDING ISOLATED tasks on queue 'default' wait: no live worker serves this \
             class. Start a worker with isolated children on this queue. A shared task \
             that went back to the queue as ISOLATED also waits here."
        );
    }

    /// Python `TestDescribeUncovered::test_an_unserved_shared_stream_names_both_kinds_of_children`.
    #[test]
    fn an_unserved_shared_stream_names_both_kinds_of_children() {
        for execution in [Execution::SharedAsync, Execution::SharedThread] {
            let text = UncoveredStream::of(&stream("q", execution, StreamCoverageState::Unserved))
                .expect("uncovered")
                .describe();

            assert_eq!(
                text,
                format!(
                    "PENDING {0} tasks on queue 'q' wait: no live worker serves this class. \
                     Start a worker with {0} children or with isolated children on this queue.",
                    execution.as_str()
                )
            );
        }
    }

    /// Python `TestDescribeUncovered::test_a_capacity_less_stream_names_its_serving_workers`.
    #[test]
    fn a_capacity_less_stream_names_its_serving_workers() {
        let text = UncoveredStream::of(&without_capacity())
            .expect("uncovered")
            .describe();

        assert_eq!(
            text,
            "PENDING SHARED_ASYNC tasks on queue 'default' wait: 2 live worker(s) serve this \
             class, and none has a child in service for it. Check the down slots of those \
             workers."
        );
    }

    /// Python `TestDescribeUncovered::test_a_served_stream_has_no_text`: a
    /// served stream is not uncovered.
    #[test]
    fn a_served_stream_is_not_uncovered() {
        assert_eq!(UncoveredStream::of(&served()), None);
    }

    /// Python `TestReaperCoverageCheck::test_an_unserved_stream_logs_an_error_once`.
    #[test]
    fn an_unserved_stream_logs_an_error_once() {
        let now = Instant::now();
        let mut log = due_log(now);
        let read = coverage(vec![unserved(), served()]);

        let first = log.lines(&read, now);
        let second = log.lines(&read, now + Duration::from_secs(30));

        let expected = UncoveredStream::of(&unserved())
            .expect("uncovered")
            .describe();
        assert_eq!(first, [CoverageLogLine::Error(expected)]);
        assert!(second.is_empty());
    }

    /// Python `TestReaperCoverageCheck::test_a_capacity_less_stream_logs_a_warning`.
    #[test]
    fn a_capacity_less_stream_logs_a_warning() {
        let now = Instant::now();
        let mut log = due_log(now);

        let lines = log.lines(&coverage(vec![without_capacity()]), now);

        let expected = UncoveredStream::of(&without_capacity())
            .expect("uncovered")
            .describe();
        assert_eq!(lines, [CoverageLogLine::Warning(expected)]);
    }

    /// Python `TestReaperCoverageCheck::test_a_failed_check_logs_one_warning_per_interval`.
    #[test]
    fn a_failed_check_logs_one_warning_per_interval() {
        let now = Instant::now();
        let mut log = due_log(now);
        let failed: BrokerResult<ClassCoverage> = Err(BrokerOperationError {
            code: BrokerErrorCode::MonitoringQueryFailed,
            message: "get_class_coverage refused: worker w1 ...".to_owned(),
            retryable: false,
            cause: crate::failure_cause::FailureCause::ContractViolation,
        });

        let first = log.lines(&failed, now);
        let second = log.lines(&failed, now + Duration::from_secs(30));
        let after_interval = log.lines(&failed, now + CLASS_COVERAGE_LOG_INTERVAL);

        assert_eq!(
            first,
            [CoverageLogLine::Warning(
                "Class coverage check failed: get_class_coverage refused: worker w1 ...".to_owned()
            )]
        );
        assert!(second.is_empty());
        assert_eq!(after_interval.len(), 1);
    }

    /// Python `TestReaperCoverageCheck::test_no_read_before_the_first_check_time`
    /// and `::test_the_reaper_loop_starts_checks_one_interval_after_its_start`.
    #[test]
    fn checks_start_one_check_interval_after_the_reaper_start() {
        let start = Instant::now();
        let check_interval = Duration::from_millis(7_000);
        let log = ClassCoverageLog::for_reaper_start(start, check_interval);

        assert!(!log.due(start));
        assert!(!log.due(start + check_interval - Duration::from_millis(1)));
        assert!(log.due(start + check_interval));
    }
}
