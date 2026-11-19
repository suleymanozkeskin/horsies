//! `GET /metrics` of the monitoring handler: the fleet collector of the
//! handler's broker, read when a request comes.
//!
//! - A request reads the fleet state when no read has started yet, or when
//!   the last read started at least
//!   [`FLEET_COLLECT_INTERVAL`](crate::monitoring::fleet::FLEET_COLLECT_INTERVAL)
//!   before it, and answers after that read is recorded. Every other
//!   request answers the last read and reads nothing. So the first answer
//!   holds the fleet series, and nothing is read while nobody scrapes.
//! - Each statement of a read is bounded by
//!   [`FLEET_QUERY_TIMEOUT`](crate::monitoring::fleet::FLEET_QUERY_TIMEOUT).
//!   A part without an answer (a failed statement, a statement past its
//!   bound) is `horsies_exporter_collect_failed{query} 1` and no series of
//!   its gauges; the answer is still the text.
//! - Reads never overlap. A read holds the lock of the last read until it is
//!   recorded; a request that comes during a read waits for it and answers
//!   it. The read runs in its own task, so a request that ends during the
//!   read does not stop it.
//! - The reads stop when the handler is dropped.

use std::future::Future;
use std::sync::Arc;

use tokio::sync::{Mutex, OwnedMutexGuard};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::broker::PostgresBroker;
use crate::monitoring::fleet::{
    log_recorded, read_fleet_state, FleetCollector, FleetLabels, FleetState, FLEET_COLLECT_INTERVAL,
};

/// When the last read of the fleet state started.
#[derive(Debug, Clone, Copy)]
enum LastRead {
    /// No read has started.
    NotStarted,
    /// The last read started at this instant.
    StartedAt(Instant),
}

impl LastRead {
    /// Whether a request at `now` reads: no read has started, or the last
    /// one started at least [`FLEET_COLLECT_INTERVAL`] before `now`.
    fn read_is_due(self, now: Instant) -> bool {
        match self {
            Self::NotStarted => true,
            Self::StartedAt(started) => now.duration_since(started) >= FLEET_COLLECT_INTERVAL,
        }
    }
}

/// The fleet collector of one handler.
pub(crate) struct WebFleetMetrics {
    collector: Arc<FleetCollector>,
    broker: Arc<PostgresBroker>,
    /// The start of the last read. A read holds this lock until it is
    /// recorded, so reads never overlap.
    last_read: Arc<Mutex<LastRead>>,
    /// The stop of the reads: a clone goes to each read.
    reads_stop: CancellationToken,
    /// Cancels `reads_stop` when the handler's state is dropped.
    _stop_on_drop: DropGuard,
}

impl WebFleetMetrics {
    /// The collector of `broker`, with the queue and schedule names of
    /// `labels`. Reads nothing.
    pub(crate) fn new(broker: Arc<PostgresBroker>, labels: FleetLabels) -> Self {
        let stop = CancellationToken::new();
        Self {
            collector: Arc::new(FleetCollector::new(labels)),
            broker,
            last_read: Arc::new(Mutex::new(LastRead::NotStarted)),
            reads_stop: stop.clone(),
            _stop_on_drop: stop.drop_guard(),
        }
    }

    /// The Prometheus text of the fleet state of the broker. Reads the state
    /// first when a read is due ([`LastRead::read_is_due`]), and waits for a
    /// read in flight. The read is a task of the tokio runtime that serves
    /// the request.
    pub(crate) async fn render(&self) -> String {
        let broker = Arc::clone(&self.broker);
        self.render_after_due_read(move || async move { read_fleet_state(&broker).await })
            .await
    }

    /// [`Self::render`] with the read `read`, which is called only when a
    /// read is due.
    async fn render_after_due_read<R, F>(&self, read: R) -> String
    where
        R: FnOnce() -> F,
        F: Future<Output = FleetState> + Send + 'static,
    {
        let mut last_read = Arc::clone(&self.last_read).lock_owned().await;
        let now = Instant::now();
        let read_is_due = last_read.read_is_due(now);
        match read_is_due {
            true => {
                *last_read = LastRead::StartedAt(now);
                let reading = tokio::spawn(record_read(
                    Arc::clone(&self.collector),
                    read(),
                    self.reads_stop.clone(),
                    last_read,
                ));
                wait_for_read(reading).await;
            }
            false => drop(last_read),
        }
        self.collector.render()
    }
}

/// Record `read` in `collector`, then release `_held`, the lock of the last
/// read. Logs one WARNING line per part without an answer. A cancel of
/// `stop` drops the read unrecorded; the read only reads, so nothing
/// changed.
async fn record_read<F>(
    collector: Arc<FleetCollector>,
    read: F,
    stop: CancellationToken,
    _held: OwnedMutexGuard<LastRead>,
) where
    F: Future<Output = FleetState>,
{
    let state = tokio::select! {
        biased;
        () = stop.cancelled() => return,
        state = read => state,
    };
    log_recorded(&collector.record(&state));
}

/// Wait for the read task `reading`. A panic of the read is a defect: it
/// resumes in the request. A task that the runtime cancelled at its
/// shutdown leaves the last read.
async fn wait_for_read(reading: JoinHandle<()>) {
    match reading.await {
        Ok(()) => {}
        Err(ended) => match ended.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(_cancelled_at_shutdown) => {}
        },
    }
}

#[cfg(test)]
mod tests {
    //! One test per outcome of a scrape: the first scrape reads, a failed
    //! read is the failure gauge, concurrent scrapes share one read, a
    //! scrape within the interval reads nothing, and a scrape that ends
    //! during its read leaves the read to the next scrape.

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use chrono::{DateTime, TimeZone, Utc};

    use super::*;
    use crate::monitoring::fleet::{
        ClassSeats, FleetPart, FleetQueryError, FleetQueryFailure, FleetWorkers, LiveWorkerCounts,
        OldestPending, QueueTasks, ScheduleRuns, SchemaVersion, SeatCount, WorkflowCounts,
        FLEET_QUERY_TIMEOUT,
    };
    use crate::telemetry::labels::FleetQuery;
    use crate::{ClassCoverage, ClientFamily};

    /// How long a test read takes.
    const READ_TIME: Duration = Duration::from_secs(1);

    /// Sample lines of the fleet gauges that [`answered_state`] gives.
    const FLEET_SERIES: [&str; 3] = [
        "horsies_fleet_workers{state=\"serving\"} 2\n",
        "horsies_workflows{status=\"RUNNING\"} 7\n",
        "horsies_schema_version 1001\n",
    ];

    /// The `query` label value of each [`FleetQuery`].
    const QUERIES: [&str; 7] = [
        "queue_tasks",
        "oldest_pending",
        "workers",
        "class_coverage",
        "workflows",
        "schedules",
        "schema_version",
    ];

    /// A collector on a broker that the test reads never use.
    fn metrics() -> WebFleetMetrics {
        let broker = PostgresBroker::from_test_url(
            "postgresql://postgres:pw@127.0.0.1:1/none",
            ClientFamily::Rust,
        );
        WebFleetMetrics::new(Arc::new(broker), FleetLabels::none())
    }

    fn checked_at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 19, 12, 0, 0)
            .single()
            .expect("a valid test instant")
    }

    fn answered<T>(answer: T) -> FleetPart<T> {
        FleetPart {
            outcome: Ok(answer),
            took: Duration::from_millis(20),
        }
    }

    fn timed_out<T>(query: FleetQuery) -> FleetPart<T> {
        FleetPart {
            outcome: Err(FleetQueryError {
                query,
                failure: FleetQueryFailure::TimedOut {
                    bound: FLEET_QUERY_TIMEOUT,
                },
            }),
            took: FLEET_QUERY_TIMEOUT,
        }
    }

    /// A read in which every part answers: no live task, two serving
    /// workers, seven running workflows, schema version 1001.
    fn answered_state() -> FleetState {
        let no_seats = SeatCount::default();
        FleetState {
            queue_tasks: answered(QueueTasks {
                pending: Vec::new(),
                held: Vec::new(),
            }),
            oldest_pending: answered(OldestPending {
                checked_at: checked_at(),
                streams: Vec::new(),
            }),
            workers: answered(FleetWorkers {
                checked_at: checked_at(),
                workers: LiveWorkerCounts {
                    serving: 2,
                    degraded: 0,
                },
                seats: ClassSeats {
                    isolated: no_seats,
                    shared_async: no_seats,
                    shared_thread: no_seats,
                },
            }),
            class_coverage: answered(ClassCoverage {
                checked_at: checked_at(),
                live_workers: 2,
                streams: Vec::new(),
            }),
            workflows: answered(WorkflowCounts {
                running: 7,
                paused: 0,
            }),
            schedules: answered(ScheduleRuns {
                checked_at: checked_at(),
                schedules: Vec::new(),
            }),
            schema_version: answered(SchemaVersion { version: 1_001 }),
        }
    }

    /// A read in which every statement ran past its bound.
    fn timed_out_state() -> FleetState {
        FleetState {
            queue_tasks: timed_out(FleetQuery::QueueTasks),
            oldest_pending: timed_out(FleetQuery::OldestPending),
            workers: timed_out(FleetQuery::Workers),
            class_coverage: timed_out(FleetQuery::ClassCoverage),
            workflows: timed_out(FleetQuery::Workflows),
            schedules: timed_out(FleetQuery::Schedules),
            schema_version: timed_out(FleetQuery::SchemaVersion),
        }
    }

    async fn read_taking(time: Duration, state: FleetState) -> FleetState {
        tokio::time::sleep(time).await;
        state
    }

    /// One scrape whose read, when one is due, counts its start in `reads`,
    /// takes [`READ_TIME`] and answers `state`.
    async fn scrape(metrics: &WebFleetMetrics, reads: &AtomicUsize, state: FleetState) -> String {
        metrics
            .render_after_due_read(|| {
                let _before = reads.fetch_add(1, Ordering::SeqCst);
                read_taking(READ_TIME, state)
            })
            .await
    }

    fn assert_fleet_series(text: &str) {
        for series in FLEET_SERIES {
            assert!(text.contains(series), "{series}: {text}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_first_scrape_reads_once_and_answers_the_fleet_series() {
        let metrics = metrics();
        let reads = AtomicUsize::new(0);
        let started = Instant::now();

        let text = scrape(&metrics, &reads, answered_state()).await;

        assert_eq!(reads.load(Ordering::SeqCst), 1);
        assert_eq!(
            started.elapsed(),
            READ_TIME,
            "the answer waits for the read"
        );
        assert_fleet_series(&text);
        for query in QUERIES {
            assert!(
                text.contains(&format!(
                    "horsies_exporter_collect_failed{{query=\"{query}\"}} 0\n"
                )),
                "{query}: {text}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_timed_out_first_read_answers_the_failure_gauges_and_no_fleet_series() {
        let metrics = metrics();
        let reads = AtomicUsize::new(0);

        let text = scrape(&metrics, &reads, timed_out_state()).await;

        assert_eq!(reads.load(Ordering::SeqCst), 1);
        assert_eq!(FleetQuery::ALL.len(), QUERIES.len());
        for query in QUERIES {
            assert!(
                text.contains(&format!(
                    "horsies_exporter_collect_failed{{query=\"{query}\"}} 1\n"
                )),
                "{query}: {text}"
            );
        }
        for series in FLEET_SERIES {
            assert!(!text.contains(series), "{series}: {text}");
        }
        assert!(!text.contains("\nhorsies_queue_tasks{"), "{text}");
    }

    #[tokio::test(start_paused = true)]
    async fn two_concurrent_first_scrapes_share_one_read() {
        let metrics = metrics();
        let reads = AtomicUsize::new(0);
        let started = Instant::now();

        let (first, second) = tokio::join!(
            scrape(&metrics, &reads, answered_state()),
            scrape(&metrics, &reads, answered_state()),
        );

        assert_eq!(reads.load(Ordering::SeqCst), 1);
        assert_eq!(started.elapsed(), READ_TIME, "both wait for the one read");
        assert_fleet_series(&first);
        assert_fleet_series(&second);
    }

    #[tokio::test(start_paused = true)]
    async fn a_scrape_within_the_interval_reads_nothing() {
        let metrics = metrics();
        let reads = AtomicUsize::new(0);
        let started = Instant::now();
        let _first = scrape(&metrics, &reads, answered_state()).await;

        tokio::time::sleep_until(started + FLEET_COLLECT_INTERVAL - Duration::from_millis(1)).await;
        let within = scrape(&metrics, &reads, timed_out_state()).await;
        assert_eq!(
            reads.load(Ordering::SeqCst),
            1,
            "no read within the interval"
        );
        assert_fleet_series(&within);

        tokio::time::sleep_until(started + 3 * FLEET_COLLECT_INTERVAL).await;
        assert_eq!(
            reads.load(Ordering::SeqCst),
            1,
            "nothing is read while nobody scrapes"
        );
        let after = scrape(&metrics, &reads, timed_out_state()).await;
        assert_eq!(reads.load(Ordering::SeqCst), 2, "a scrape after it reads");
        assert!(
            after.contains("horsies_exporter_collect_failed{query=\"workflows\"} 1\n"),
            "{after}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_scrape_that_ends_during_its_read_leaves_the_read_to_the_next_scrape() {
        let metrics = metrics();
        let reads = AtomicUsize::new(0);
        let started = Instant::now();

        let ended =
            tokio::time::timeout(READ_TIME / 2, scrape(&metrics, &reads, answered_state())).await;
        assert!(ended.is_err(), "the first scrape ends before its read");
        let text = scrape(&metrics, &reads, timed_out_state()).await;

        assert_eq!(
            reads.load(Ordering::SeqCst),
            1,
            "the read in flight is shared"
        );
        assert_eq!(started.elapsed(), READ_TIME);
        assert_fleet_series(&text);
    }
}
