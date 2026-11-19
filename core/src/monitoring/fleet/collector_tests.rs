//! One test per outcome of the fleet collector: the series of each part, the
//! fallback series of undeclared names, the exposition, a part without an
//! answer, the read interval and the cache, and the stop.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use tokio::time::Instant;

use super::*;
use crate::monitoring::fleet::{
    ClassSeats, FleetQueryFailure, HeldStatus, HeldTasks, LastRun, LiveWorkerCounts, PendingCount,
    PendingTasks, ScheduleLastRun, SeatCount, StreamOldest, FLEET_QUERY_TIMEOUT,
};
use crate::{
    CustomQueueConfig, IntervalSchedule, ScheduleConfig, SchedulePattern, StreamCoverage,
    StreamCoverageState, StreamWait, TaskSchedule,
};

const ISOLATED: Execution = Execution::Isolated;
const ASYNC: Execution = Execution::SharedAsync;

fn at() -> DateTime<Utc> {
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

fn oldest(queue: &str, execution: Execution, wait: StreamWait) -> StreamOldest {
    StreamOldest {
        queue_name: queue.to_owned(),
        execution,
        wait,
    }
}

/// The oldest due row has waited `seconds`.
fn waiting(seconds: i64) -> StreamWait {
    StreamWait::OldestWaiting {
        enqueued_at: at() - TimeDelta::seconds(seconds),
        wait: TimeDelta::seconds(seconds),
    }
}

fn coverage(queue: &str, execution: Execution, state: StreamCoverageState) -> StreamCoverage {
    StreamCoverage {
        queue_name: queue.to_owned(),
        execution,
        state,
    }
}

/// The last run was `seconds` before the read.
fn ran(name: &str, seconds: i64) -> ScheduleLastRun {
    ScheduleLastRun {
        schedule_name: name.to_owned(),
        last_run: LastRun::RanAt {
            at: at() - TimeDelta::seconds(seconds),
            age: TimeDelta::seconds(seconds),
        },
    }
}

fn never_ran(name: &str) -> ScheduleLastRun {
    ScheduleLastRun {
        schedule_name: name.to_owned(),
        last_run: LastRun::NeverRan,
    }
}

/// The queues `a` and `b`, and the schedule `nightly`.
fn labels() -> FleetLabels {
    FleetLabels {
        queues: DeclaredNames::new(["a", "b"]).expect("valid test queue names"),
        schedules: DeclaredNames::new(["nightly"]).expect("valid test schedule names"),
    }
}

fn queue_tasks() -> QueueTasks {
    QueueTasks {
        pending: vec![
            pending("a", ISOLATED, PendingCount::Exact(5)),
            pending("b", ASYNC, PendingCount::AtLeastLimit),
        ],
        held: vec![
            held(HeldStatus::Claimed, "a", ISOLATED, 2),
            held(HeldStatus::Running, "a", ISOLATED, 3),
        ],
    }
}

/// A state where every part answers.
fn full_state() -> FleetState {
    FleetState {
        queue_tasks: answered(queue_tasks()),
        oldest_pending: answered(OldestPending {
            checked_at: at(),
            streams: vec![
                oldest("a", ISOLATED, waiting(90)),
                oldest(
                    "b",
                    ASYNC,
                    StreamWait::NoneDue {
                        next_due_at: at() + TimeDelta::seconds(60),
                    },
                ),
            ],
        }),
        workers: answered(FleetWorkers {
            checked_at: at(),
            workers: LiveWorkerCounts {
                serving: 2,
                degraded: 1,
            },
            seats: ClassSeats {
                isolated: SeatCount {
                    seats: 8,
                    in_use: 3,
                },
                shared_async: SeatCount {
                    seats: 100,
                    in_use: 0,
                },
                shared_thread: SeatCount::default(),
            },
        }),
        class_coverage: answered(ClassCoverage {
            checked_at: at(),
            live_workers: 3,
            streams: vec![
                coverage(
                    "a",
                    ISOLATED,
                    StreamCoverageState::Served {
                        workers: 3,
                        capacity: 8,
                    },
                ),
                coverage("b", ASYNC, StreamCoverageState::Unserved),
            ],
        }),
        workflows: answered(WorkflowCounts {
            running: 7,
            paused: 1,
        }),
        schedules: answered(ScheduleRuns {
            checked_at: at(),
            schedules: vec![ran("nightly", 3_600), never_ran("weekly")],
        }),
        schema_version: answered(SchemaVersion { version: 1_001 }),
    }
}

/// The sample lines of the metric `name` in `text`, in text order.
fn samples(text: &str, name: &str) -> Vec<String> {
    text.lines()
        .filter(|line| {
            line.strip_prefix(name)
                .is_some_and(|rest| rest.starts_with('{') || rest.starts_with(' '))
        })
        .map(str::to_owned)
        .collect()
}

fn lines(expected: &[&str]) -> Vec<String> {
    expected.iter().map(|line| (*line).to_owned()).collect()
}

// ---------------------------------------------------------------------------
// The series of each part
// ---------------------------------------------------------------------------

#[test]
fn an_exact_pending_count_is_a_queue_tasks_series_and_one_past_the_limit_is_a_lower_bound() {
    let collector = FleetCollector::new(labels());
    let recorded = collector.record(&full_state());
    assert_eq!(recorded.failed, Vec::new());
    assert_eq!(recorded.dropped_at_series_bound, 0);
    let text = collector.render();
    assert_eq!(
        samples(&text, "horsies_queue_tasks"),
        lines(&[
            "horsies_queue_tasks{queue=\"a\",execution=\"ISOLATED\",status=\"CLAIMED\"} 2",
            "horsies_queue_tasks{queue=\"a\",execution=\"ISOLATED\",status=\"PENDING\"} 5",
            "horsies_queue_tasks{queue=\"a\",execution=\"ISOLATED\",status=\"RUNNING\"} 3",
        ])
    );
    assert_eq!(
        samples(&text, "horsies_queue_pending_more_than"),
        lines(&["horsies_queue_pending_more_than{queue=\"b\",execution=\"SHARED_ASYNC\"} 1000"])
    );
}

#[test]
fn the_oldest_due_wait_is_in_seconds_and_a_stream_with_no_due_row_has_no_series() {
    let collector = FleetCollector::new(labels());
    let _recorded = collector.record(&full_state());
    assert_eq!(
        samples(
            &collector.render(),
            "horsies_queue_oldest_pending_age_seconds"
        ),
        lines(&["horsies_queue_oldest_pending_age_seconds{queue=\"a\",execution=\"ISOLATED\"} 90"])
    );
}

#[test]
fn workers_and_seats_have_a_series_per_state_and_class_with_zeros() {
    let collector = FleetCollector::new(labels());
    let _recorded = collector.record(&full_state());
    let text = collector.render();
    assert_eq!(
        samples(&text, "horsies_fleet_workers"),
        lines(&[
            "horsies_fleet_workers{state=\"degraded\"} 1",
            "horsies_fleet_workers{state=\"serving\"} 2",
        ])
    );
    assert_eq!(
        samples(&text, "horsies_fleet_seats"),
        lines(&[
            "horsies_fleet_seats{execution=\"ISOLATED\"} 8",
            "horsies_fleet_seats{execution=\"SHARED_ASYNC\"} 100",
            "horsies_fleet_seats{execution=\"SHARED_THREAD\"} 0",
        ])
    );
    assert_eq!(
        samples(&text, "horsies_fleet_seats_in_use"),
        lines(&[
            "horsies_fleet_seats_in_use{execution=\"ISOLATED\"} 3",
            "horsies_fleet_seats_in_use{execution=\"SHARED_ASYNC\"} 0",
            "horsies_fleet_seats_in_use{execution=\"SHARED_THREAD\"} 0",
        ])
    );
}

#[test]
fn an_unserved_class_is_one_and_a_served_class_or_one_without_capacity_is_zero() {
    let collector = FleetCollector::new(labels());
    let mut state = full_state();
    state.class_coverage = answered(ClassCoverage {
        checked_at: at(),
        live_workers: 2,
        streams: vec![
            coverage(
                "a",
                ISOLATED,
                StreamCoverageState::Served {
                    workers: 2,
                    capacity: 4,
                },
            ),
            coverage(
                "a",
                ASYNC,
                StreamCoverageState::WithoutCapacity { workers: 1 },
            ),
            coverage("b", ISOLATED, StreamCoverageState::Unserved),
        ],
    });
    let _recorded = collector.record(&state);
    assert_eq!(
        samples(&collector.render(), "horsies_fleet_unserved_classes"),
        lines(&[
            "horsies_fleet_unserved_classes{queue=\"a\",execution=\"ISOLATED\"} 0",
            "horsies_fleet_unserved_classes{queue=\"a\",execution=\"SHARED_ASYNC\"} 0",
            "horsies_fleet_unserved_classes{queue=\"b\",execution=\"ISOLATED\"} 1",
        ])
    );
}

#[test]
fn workflows_schedules_and_the_schema_version_have_their_series() {
    let collector = FleetCollector::new(labels());
    let _recorded = collector.record(&full_state());
    let text = collector.render();
    assert_eq!(
        samples(&text, "horsies_workflows"),
        lines(&[
            "horsies_workflows{status=\"PAUSED\"} 1",
            "horsies_workflows{status=\"RUNNING\"} 7",
        ])
    );
    assert_eq!(
        samples(&text, "horsies_schedule_last_run_age_seconds"),
        lines(&["horsies_schedule_last_run_age_seconds{schedule=\"nightly\"} 3600"])
    );
    assert_eq!(
        samples(&text, "horsies_schema_version"),
        lines(&["horsies_schema_version 1001"])
    );
}

#[test]
fn undeclared_names_share_the_fallback_series_counts_add_up_and_ages_take_the_largest() {
    let collector = FleetCollector::new(FleetLabels {
        queues: DeclaredNames::new(["a"]).expect("a valid test queue name"),
        schedules: DeclaredNames::none(),
    });
    let mut state = full_state();
    state.queue_tasks = answered(QueueTasks {
        pending: vec![
            pending("x", ISOLATED, PendingCount::Exact(4)),
            pending("y", ISOLATED, PendingCount::Exact(6)),
            pending("x", ASYNC, PendingCount::Exact(4)),
            pending("z", ASYNC, PendingCount::AtLeastLimit),
        ],
        held: vec![
            held(HeldStatus::Running, "x", ISOLATED, 1),
            held(HeldStatus::Running, "y", ISOLATED, 2),
        ],
    });
    state.oldest_pending = answered(OldestPending {
        checked_at: at(),
        streams: vec![
            oldest("x", ISOLATED, waiting(30)),
            oldest("y", ISOLATED, waiting(90)),
        ],
    });
    state.class_coverage = answered(ClassCoverage {
        checked_at: at(),
        live_workers: 1,
        streams: vec![
            coverage("x", ISOLATED, StreamCoverageState::Unserved),
            coverage("y", ISOLATED, StreamCoverageState::Unserved),
            coverage(
                "z",
                ASYNC,
                StreamCoverageState::WithoutCapacity { workers: 1 },
            ),
        ],
    });
    state.schedules = answered(ScheduleRuns {
        checked_at: at(),
        schedules: vec![ran("old", 100), ran("older", 5_000), never_ran("new")],
    });
    let _recorded = collector.record(&state);
    let text = collector.render();
    assert_eq!(
        samples(&text, "horsies_queue_tasks"),
        lines(&[
            "horsies_queue_tasks{queue=\"(unknown)\",execution=\"ISOLATED\",status=\"PENDING\"} 10",
            "horsies_queue_tasks{queue=\"(unknown)\",execution=\"ISOLATED\",status=\"RUNNING\"} 3",
        ])
    );
    assert_eq!(
        samples(&text, "horsies_queue_pending_more_than"),
        lines(&[
            "horsies_queue_pending_more_than{queue=\"(unknown)\",execution=\"SHARED_ASYNC\"} 1004"
        ])
    );
    assert_eq!(
        samples(&text, "horsies_queue_oldest_pending_age_seconds"),
        lines(&[
            "horsies_queue_oldest_pending_age_seconds{queue=\"(unknown)\",execution=\"ISOLATED\"} 90"
        ])
    );
    assert_eq!(
        samples(&text, "horsies_fleet_unserved_classes"),
        lines(&[
            "horsies_fleet_unserved_classes{queue=\"(unknown)\",execution=\"ISOLATED\"} 2",
            "horsies_fleet_unserved_classes{queue=\"(unknown)\",execution=\"SHARED_ASYNC\"} 0",
        ])
    );
    assert_eq!(
        samples(&text, "horsies_schedule_last_run_age_seconds"),
        lines(&["horsies_schedule_last_run_age_seconds{schedule=\"(unknown)\"} 5000"])
    );
}

// ---------------------------------------------------------------------------
// The exposition and the collector's own metrics
// ---------------------------------------------------------------------------

#[test]
fn the_exposition_holds_each_collector_metric_once_with_its_type() {
    let collector = FleetCollector::new(labels());
    let _recorded = collector.record(&full_state());
    let text = collector.render();
    for metric in FLEET_COLLECTOR_METRICS {
        let spec = metric.spec();
        let name = spec.prometheus_name();
        let type_line = format!("# TYPE {name} {}", spec.kind.prometheus_type());
        assert_eq!(
            text.lines().filter(|line| *line == type_line).count(),
            1,
            "{name}"
        );
    }
    assert_eq!(
        samples(&text, "horsies_fleet_workers_telemetry_down"),
        Vec::<String>::new()
    );
    assert!(text.contains("# TYPE horsies_telemetry_series_dropped_total counter\n"));
    assert!(text.contains(
        "horsies_exporter_collect_duration_seconds_sum{query=\"workers\"} 0.02\n\
         horsies_exporter_collect_duration_seconds_count{query=\"workers\"} 1\n"
    ));
}

#[test]
fn a_collector_with_no_read_has_no_fleet_series() {
    let collector = FleetCollector::new(labels());
    let text = collector.render();
    for name in [
        "horsies_queue_tasks",
        "horsies_fleet_workers",
        "horsies_schema_version",
        "horsies_exporter_collect_failed",
        "horsies_exporter_collect_errors_total",
    ] {
        assert_eq!(samples(&text, name), Vec::<String>::new(), "{name}");
    }
}

#[test]
fn a_part_without_an_answer_has_no_series_counts_an_error_and_flags_its_query() {
    let collector = FleetCollector::new(labels());
    let _first = collector.record(&full_state());

    let mut state = full_state();
    state.queue_tasks = timed_out(FleetQuery::QueueTasks);
    let recorded = collector.record(&state);
    assert_eq!(
        recorded.failed,
        vec![FleetQueryError {
            query: FleetQuery::QueueTasks,
            failure: FleetQueryFailure::TimedOut {
                bound: FLEET_QUERY_TIMEOUT,
            },
        }]
    );
    let text = collector.render();
    assert_eq!(samples(&text, "horsies_queue_tasks"), Vec::<String>::new());
    assert_eq!(
        samples(&text, "horsies_queue_pending_more_than"),
        Vec::<String>::new()
    );
    assert_eq!(
        samples(&text, "horsies_exporter_collect_errors_total"),
        lines(&["horsies_exporter_collect_errors_total{query=\"queue_tasks\"} 1"])
    );
    assert_eq!(
        samples(&text, "horsies_exporter_collect_failed"),
        lines(&[
            "horsies_exporter_collect_failed{query=\"class_coverage\"} 0",
            "horsies_exporter_collect_failed{query=\"oldest_pending\"} 0",
            "horsies_exporter_collect_failed{query=\"queue_tasks\"} 1",
            "horsies_exporter_collect_failed{query=\"schedules\"} 0",
            "horsies_exporter_collect_failed{query=\"schema_version\"} 0",
            "horsies_exporter_collect_failed{query=\"workers\"} 0",
            "horsies_exporter_collect_failed{query=\"workflows\"} 0",
        ])
    );
    assert_eq!(
        samples(&text, "horsies_workflows").len(),
        2,
        "the other parts keep their series"
    );

    let _answered = collector.record(&full_state());
    let text = collector.render();
    assert_eq!(samples(&text, "horsies_queue_tasks").len(), 3);
    assert!(text.contains("horsies_exporter_collect_failed{query=\"queue_tasks\"} 0\n"));
    assert!(text.contains("horsies_exporter_collect_errors_total{query=\"queue_tasks\"} 1\n"));
}

#[test]
fn a_series_of_an_earlier_read_that_the_next_read_does_not_hold_is_gone() {
    let collector = FleetCollector::new(labels());
    let _first = collector.record(&full_state());
    let mut state = full_state();
    state.queue_tasks = answered(QueueTasks {
        pending: Vec::new(),
        held: vec![held(HeldStatus::Running, "b", ASYNC, 1)],
    });
    let _second = collector.record(&state);
    let text = collector.render();
    assert_eq!(
        samples(&text, "horsies_queue_tasks"),
        lines(&[
            "horsies_queue_tasks{queue=\"b\",execution=\"SHARED_ASYNC\",status=\"RUNNING\"} 1"
        ])
    );
    assert_eq!(
        samples(&text, "horsies_queue_pending_more_than"),
        Vec::<String>::new()
    );
}

// ---------------------------------------------------------------------------
// Labels from the app config
// ---------------------------------------------------------------------------

#[test]
fn the_labels_of_a_default_config_are_the_default_queue_and_no_schedule() {
    let labels = FleetLabels::of_config(&AppConfig::for_database_url("postgresql://db/app"))
        .expect("the default config gives valid labels");
    assert_eq!(labels.queues.names().to_vec(), vec!["default".to_owned()]);
    assert_eq!(labels.schedules.names().to_vec(), Vec::<String>::new());
}

#[test]
fn the_labels_of_a_custom_config_are_its_queues_and_its_schedules() {
    let mut config = AppConfig::for_database_url("postgresql://db/app");
    config.queue_mode = QueueMode::Custom;
    config.custom_queues = Some(vec![
        CustomQueueConfig {
            name: "fast".to_owned(),
            priority: 1,
            max_concurrency: Some(5),
        },
        CustomQueueConfig {
            name: "slow".to_owned(),
            priority: 50,
            max_concurrency: None,
        },
    ]);
    config.schedule = Some(ScheduleConfig::new(vec![TaskSchedule::new(
        "nightly",
        "cleanup",
        SchedulePattern::Interval(IntervalSchedule::default()),
    )]));
    let labels = FleetLabels::of_config(&config).expect("valid labels");
    assert_eq!(
        labels.queues.names().to_vec(),
        vec!["fast".to_owned(), "slow".to_owned()]
    );
    assert_eq!(
        labels.schedules.names().to_vec(),
        vec!["nightly".to_owned()]
    );
}

#[test]
fn a_config_name_that_is_the_fallback_text_is_refused() {
    let mut config = AppConfig::for_database_url("postgresql://db/app");
    config.schedule = Some(ScheduleConfig::new(vec![TaskSchedule::new(
        "(unknown)",
        "cleanup",
        SchedulePattern::Interval(IntervalSchedule::default()),
    )]));
    assert_eq!(
        FleetLabels::of_config(&config).err(),
        Some(DeclaredNamesRefused::FallbackName {
            set: crate::telemetry::labels::DeclaredSource::ScheduleNames,
            name: "(unknown)".to_owned(),
        })
    );
}

// ---------------------------------------------------------------------------
// The read interval, the cache and the stop
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn the_state_is_read_at_once_then_once_per_interval_and_a_render_reads_nothing() {
    let collector = FleetCollector::new(labels());
    let reads = AtomicUsize::new(0);
    let stop = CancellationToken::new();
    let start = Instant::now();
    let read = || {
        let _before = reads.fetch_add(1, Ordering::SeqCst);
        async { full_state() }
    };
    let checks = async {
        tokio::time::sleep_until(start + Duration::from_millis(1)).await;
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        for _ in 0..50 {
            assert_eq!(
                samples(&collector.render(), "horsies_schema_version"),
                lines(&["horsies_schema_version 1001"])
            );
        }
        tokio::time::sleep_until(start + FLEET_COLLECT_INTERVAL - Duration::from_millis(1)).await;
        assert_eq!(reads.load(Ordering::SeqCst), 1, "scrapes start no read");
        tokio::time::sleep_until(start + FLEET_COLLECT_INTERVAL + Duration::from_millis(1)).await;
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        tokio::time::sleep_until(start + 2 * FLEET_COLLECT_INTERVAL + Duration::from_millis(1))
            .await;
        assert_eq!(reads.load(Ordering::SeqCst), 3);
        stop.cancel();
    };
    let ((), ()) = tokio::join!(collect_with(&collector, read, &stop), checks);
}

#[tokio::test(start_paused = true)]
async fn a_read_longer_than_the_interval_delays_the_next_one_and_reads_never_overlap() {
    const READ_TIME: Duration = Duration::from_secs(20);
    let collector = FleetCollector::new(labels());
    let starts: Mutex<Vec<Duration>> = Mutex::new(Vec::new());
    let stop = CancellationToken::new();
    let start = Instant::now();
    let read = || {
        starts
            .lock()
            .expect("the test lock is never poisoned")
            .push(start.elapsed());
        async {
            tokio::time::sleep(READ_TIME).await;
            full_state()
        }
    };
    let checks = async {
        tokio::time::sleep_until(start + 2 * READ_TIME + Duration::from_millis(1)).await;
        stop.cancel();
    };
    let ((), ()) = tokio::join!(collect_with(&collector, read, &stop), checks);
    assert_eq!(
        *starts.lock().expect("the test lock is never poisoned"),
        vec![Duration::ZERO, READ_TIME, 2 * READ_TIME]
    );
}

#[tokio::test(start_paused = true)]
async fn a_stop_during_a_read_ends_the_collection_without_recording_the_read() {
    let collector = FleetCollector::new(labels());
    let stop = CancellationToken::new();
    let start = Instant::now();
    let read = || async {
        tokio::time::sleep(Duration::from_secs(60)).await;
        full_state()
    };
    let checks = async {
        tokio::time::sleep_until(start + Duration::from_secs(1)).await;
        stop.cancel();
    };
    let ((), ()) = tokio::join!(collect_with(&collector, read, &stop), checks);
    assert_eq!(start.elapsed(), Duration::from_secs(1));
    assert_eq!(
        samples(&collector.render(), "horsies_schema_version"),
        Vec::<String>::new()
    );
}
