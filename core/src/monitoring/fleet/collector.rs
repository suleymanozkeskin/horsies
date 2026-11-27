//! The fleet collector: the fleet state as the gauges of one registry, and
//! their Prometheus text.
//!
//! [`collect_fleet_state`] reads the fleet state at once and then at most
//! once per [`FLEET_COLLECT_INTERVAL`], and records each read in the
//! collector ([`FleetCollector::record`]). A scrape renders the registry
//! ([`FleetCollector::render`]) and never starts a read, so the number of
//! scrapes does not change the load on the database. The registry holds the
//! series of the last read until the next read replaces them.
//!
//! Each part of a read gives its own series. A part without an answer
//! (a failed statement, a statement past its time limit, a refused answer)
//! leaves its gauges with no series: never a value of an earlier read, and
//! never 0. It also counts in `horsies.exporter.collect.errors{query}`, and
//! `horsies.exporter.collect.failed{query}` is 1 until a read of the query
//! answers. Every read of a query adds its time to
//! `horsies.exporter.collect.duration{query}`. A failed part is one log line
//! at WARNING.

use std::future::Future;
use std::time::Duration;

use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::core::task::Execution;
use crate::handle::CoreConnection;
use crate::telemetry::catalog::MetricId;
use crate::telemetry::declared::{DeclaredNames, ErrorCodeValues, Folded, QueueName, ScheduleName};
use crate::telemetry::exposition::render;
use crate::telemetry::labels::{FleetQuery, FleetWorkerState, LiveWorkflowStatus, TelemetryPart};
use crate::telemetry::registry::{
    Counter, Gauge, HandleRefused, Histogram, ProcessFacts, Recorded, Registry, RegistryLabels,
    Replaced,
};
use crate::{AppConfig, ExecutionClassCoverage, PostgresBroker, QueueMode};

use super::gauges::{
    oldest_pending_series, queue_task_series, schedule_series, schema_version_series,
    unserved_series, worker_series, workflow_series, QueueTaskLabels, QueueTaskSeries,
    ScheduleLabels, StreamLabels, WorkerSeries,
};
use super::{
    read_fleet_state, read_fleet_state_through, FleetPart, FleetQueryError, FleetState,
    FleetWorkers, OldestPending, QueueTasks, ScheduleRuns, SchemaVersion, WorkflowCounts,
};

/// The least time between the starts of two reads of the fleet state.
pub const FLEET_COLLECT_INTERVAL: Duration = Duration::from_secs(15);

/// The metrics of the fleet collector.
pub const FLEET_COLLECTOR_METRICS: [MetricId; 14] = [
    MetricId::QueueTasks,
    MetricId::QueuePendingMoreThan,
    MetricId::QueueOldestPendingAge,
    MetricId::FleetWorkers,
    MetricId::FleetWorkersTelemetryDown,
    MetricId::FleetSeats,
    MetricId::FleetSeatsInUse,
    MetricId::FleetUnservedClasses,
    MetricId::Workflows,
    MetricId::ScheduleLastRunAge,
    MetricId::SchemaVersion,
    MetricId::CollectDuration,
    MetricId::CollectErrors,
    MetricId::CollectFailed,
];

/// The queue of an app in `QueueMode::Default`.
const DEFAULT_QUEUE_NAME: &str = "default";

/// The names that the fleet metrics take as label values. A queue or a
/// schedule name outside them is `(unknown)`.
#[derive(Debug, Clone)]
pub struct FleetLabels {
    /// The queues of the app config.
    pub queues: DeclaredNames<QueueName>,
    /// The schedules of the app config.
    pub schedules: DeclaredNames<ScheduleName>,
}

impl FleetLabels {
    /// No declared name: every queue and every schedule is `(unknown)`.
    pub fn none() -> Self {
        Self {
            queues: DeclaredNames::none(),
            schedules: DeclaredNames::none(),
        }
    }

    /// The queues and the schedules of `config`: `default` in
    /// `QueueMode::Default`, the custom queues in `QueueMode::Custom`, and
    /// the name of each schedule of the schedule config. Pure.
    ///
    /// Never refuses: a name that is empty, is the fallback text
    /// `(unknown)`, or comes after
    /// [`MAX_DECLARED_NAMES`](crate::telemetry::declared::MAX_DECLARED_NAMES)
    /// names of its kind is not declared, and `folded` names it. Its samples
    /// count in the `(unknown)` series.
    pub fn of_config(config: &AppConfig) -> Folded<Self> {
        let queues: Vec<&str> = match config.queue_mode {
            QueueMode::Default => vec![DEFAULT_QUEUE_NAME],
            QueueMode::Custom => config
                .custom_queues
                .iter()
                .flatten()
                .map(|queue| queue.name.as_str())
                .collect(),
        };
        let schedules = config
            .schedule
            .iter()
            .flat_map(|schedule| schedule.schedules.iter())
            .map(|schedule| schedule.name.as_str());
        let queues = DeclaredNames::new(queues);
        let schedules = DeclaredNames::new(schedules);
        Folded {
            declared: Self {
                queues: queues.declared,
                schedules: schedules.declared,
            },
            folded: queues.folded.and(schedules.folded),
        }
    }
}

/// The handles of the metrics of the collector.
struct FleetGauges {
    queue_tasks: Gauge<QueueTaskLabels>,
    pending_more_than: Gauge<StreamLabels>,
    oldest_pending_age: Gauge<StreamLabels>,
    workers: Gauge<(FleetWorkerState,)>,
    telemetry_down: Gauge<(TelemetryPart,)>,
    seats: Gauge<(Execution,)>,
    seats_in_use: Gauge<(Execution,)>,
    unserved_classes: Gauge<StreamLabels>,
    workflows: Gauge<(LiveWorkflowStatus,)>,
    schedule_last_run_age: Gauge<ScheduleLabels>,
    schema_version: Gauge<()>,
    collect_duration: Histogram<(FleetQuery,)>,
    collect_errors: Counter<(FleetQuery,)>,
    collect_failed: Gauge<(FleetQuery,)>,
}

/// The handle of one metric of the collector. Every handle of
/// [`FleetGauges`] has the kind and the label types of its catalog entry,
/// and the registry holds every metric of [`FLEET_COLLECTOR_METRICS`]: a
/// refusal is a defect, and it panics with the metric.
fn handle<T>(handle: Result<T, HandleRefused>) -> T {
    match handle {
        Ok(handle) => handle,
        Err(refused) => unreachable!("fleet collector: {refused}"),
    }
}

impl FleetGauges {
    fn of(registry: &Registry) -> Self {
        Self {
            queue_tasks: handle(registry.gauge(MetricId::QueueTasks)),
            pending_more_than: handle(registry.gauge(MetricId::QueuePendingMoreThan)),
            oldest_pending_age: handle(registry.gauge(MetricId::QueueOldestPendingAge)),
            workers: handle(registry.gauge(MetricId::FleetWorkers)),
            telemetry_down: handle(registry.gauge(MetricId::FleetWorkersTelemetryDown)),
            seats: handle(registry.gauge(MetricId::FleetSeats)),
            seats_in_use: handle(registry.gauge(MetricId::FleetSeatsInUse)),
            unserved_classes: handle(registry.gauge(MetricId::FleetUnservedClasses)),
            workflows: handle(registry.gauge(MetricId::Workflows)),
            schedule_last_run_age: handle(registry.gauge(MetricId::ScheduleLastRunAge)),
            schema_version: handle(registry.gauge(MetricId::SchemaVersion)),
            collect_duration: handle(registry.histogram(MetricId::CollectDuration)),
            collect_errors: handle(registry.counter(MetricId::CollectErrors)),
            collect_failed: handle(registry.gauge(MetricId::CollectFailed)),
        }
    }
}

/// What one record of a fleet state did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetRecorded {
    /// The parts without an answer, in the order of [`FleetQuery::ALL`].
    /// Their gauges hold no series.
    pub failed: Vec<FleetQueryError>,
    /// The series that a series bound dropped. 0 while each part is within
    /// the bounds of the read: the largest part holds 10,000 series, the
    /// bound of a metric.
    pub dropped_at_series_bound: usize,
}

impl FleetRecorded {
    fn new() -> Self {
        Self {
            failed: Vec::new(),
            dropped_at_series_bound: 0,
        }
    }

    /// Count what a sample of a label set from the collector's own labels
    /// did.
    fn sampled(&mut self, recorded: Recorded) {
        match recorded {
            Recorded::Added => {}
            Recorded::DroppedAtSeriesBound => self.dropped_at_series_bound += 1,
            Recorded::OutsideLabelSet { label } => {
                unreachable!("fleet collector: a value of its own {label:?} labels is outside them")
            }
        }
    }

    /// Count what a replacement of the series of a gauge did.
    fn replaced(&mut self, replaced: Replaced) {
        assert_eq!(
            replaced.outside_label_set, 0,
            "fleet collector: values of its own labels are outside them: {replaced:?}"
        );
        self.dropped_at_series_bound += replaced.dropped_at_series_bound;
    }
}

/// The gauges of the fleet state in a registry of the fleet metrics.
pub struct FleetCollector {
    registry: Registry,
    gauges: FleetGauges,
}

impl FleetCollector {
    /// A collector with no read recorded: its gauges hold no series.
    pub fn new(labels: FleetLabels) -> Self {
        let registry = Registry::new(
            &FLEET_COLLECTOR_METRICS,
            RegistryLabels {
                tasks: DeclaredNames::none(),
                queues: labels.queues,
                handlers: DeclaredNames::none(),
                schedules: labels.schedules,
                error_codes: ErrorCodeValues::none(),
                process: ProcessFacts::NotAWorker,
            },
        );
        let registry = match registry {
            Ok(registry) => registry,
            Err(refused) => {
                unreachable!("fleet collector: no fleet metric has a worker label: {refused}")
            }
        };
        let gauges = FleetGauges::of(&registry);
        Self { registry, gauges }
    }

    /// Record one read: replace the series of each part's gauges with its
    /// answer, or with none when the part has no answer; count the time of
    /// each part, and count and flag each part without an answer. Logs
    /// nothing.
    pub fn record(&self, state: &FleetState) -> FleetRecorded {
        let mut recorded = FleetRecorded::new();
        self.record_queue_tasks(&state.queue_tasks, &mut recorded);
        self.record_oldest_pending(&state.oldest_pending, &mut recorded);
        self.record_workers(&state.workers, &mut recorded);
        self.record_class_coverage(&state.class_coverage, &mut recorded);
        self.record_workflows(&state.workflows, &mut recorded);
        self.record_schedules(&state.schedules, &mut recorded);
        self.record_schema_version(&state.schema_version, &mut recorded);
        recorded
    }

    /// The Prometheus text of the registry: the series of the last read.
    /// Reads nothing from the database.
    pub fn render(&self) -> String {
        render(&self.registry)
    }

    /// The queue values of the collector's labels.
    fn queues(&self) -> &DeclaredNames<QueueName> {
        &self.registry.labels().queues
    }

    /// Count the time of `part`, and count and flag it when it has no
    /// answer.
    fn note_part<T>(&self, query: FleetQuery, part: &FleetPart<T>, recorded: &mut FleetRecorded) {
        recorded.sampled(self.gauges.collect_duration.observe((query,), part.took));
        match &part.outcome {
            Ok(_) => recorded.sampled(self.gauges.collect_failed.set((query,), 0.0)),
            Err(error) => {
                recorded.sampled(self.gauges.collect_errors.add((query,), 1));
                recorded.sampled(self.gauges.collect_failed.set((query,), 1.0));
                recorded.failed.push(error.clone());
            }
        }
    }

    fn record_queue_tasks(&self, part: &FleetPart<QueueTasks>, recorded: &mut FleetRecorded) {
        self.note_part(FleetQuery::QueueTasks, part, recorded);
        let series = match &part.outcome {
            Ok(tasks) => queue_task_series(tasks, self.queues()),
            Err(_) => QueueTaskSeries::default(),
        };
        recorded.replaced(self.gauges.queue_tasks.replace_all(series.tasks));
        recorded.replaced(
            self.gauges
                .pending_more_than
                .replace_all(series.pending_more_than),
        );
    }

    fn record_oldest_pending(&self, part: &FleetPart<OldestPending>, recorded: &mut FleetRecorded) {
        self.note_part(FleetQuery::OldestPending, part, recorded);
        let series = match &part.outcome {
            Ok(oldest) => oldest_pending_series(oldest, self.queues()),
            Err(_) => Vec::new(),
        };
        recorded.replaced(self.gauges.oldest_pending_age.replace_all(series));
    }

    fn record_workers(&self, part: &FleetPart<FleetWorkers>, recorded: &mut FleetRecorded) {
        self.note_part(FleetQuery::Workers, part, recorded);
        let series = match &part.outcome {
            Ok(workers) => worker_series(workers),
            Err(_) => WorkerSeries::default(),
        };
        recorded.replaced(self.gauges.workers.replace_all(series.workers));
        recorded.replaced(
            self.gauges
                .telemetry_down
                .replace_all(series.telemetry_down),
        );
        recorded.replaced(self.gauges.seats.replace_all(series.seats));
        recorded.replaced(self.gauges.seats_in_use.replace_all(series.seats_in_use));
    }

    fn record_class_coverage(
        &self,
        part: &FleetPart<ExecutionClassCoverage>,
        recorded: &mut FleetRecorded,
    ) {
        self.note_part(FleetQuery::ClassCoverage, part, recorded);
        let series = match &part.outcome {
            Ok(coverage) => unserved_series(coverage, self.queues()),
            Err(_) => Vec::new(),
        };
        recorded.replaced(self.gauges.unserved_classes.replace_all(series));
    }

    fn record_workflows(&self, part: &FleetPart<WorkflowCounts>, recorded: &mut FleetRecorded) {
        self.note_part(FleetQuery::Workflows, part, recorded);
        let series = match &part.outcome {
            Ok(workflows) => workflow_series(workflows),
            Err(_) => Vec::new(),
        };
        recorded.replaced(self.gauges.workflows.replace_all(series));
    }

    fn record_schedules(&self, part: &FleetPart<ScheduleRuns>, recorded: &mut FleetRecorded) {
        self.note_part(FleetQuery::Schedules, part, recorded);
        let series = match &part.outcome {
            Ok(runs) => schedule_series(runs, &self.registry.labels().schedules),
            Err(_) => Vec::new(),
        };
        recorded.replaced(self.gauges.schedule_last_run_age.replace_all(series));
    }

    fn record_schema_version(&self, part: &FleetPart<SchemaVersion>, recorded: &mut FleetRecorded) {
        self.note_part(FleetQuery::SchemaVersion, part, recorded);
        let series = match &part.outcome {
            Ok(version) => schema_version_series(version),
            Err(_) => Vec::new(),
        };
        recorded.replaced(self.gauges.schema_version.replace_all(series));
    }
}

/// Read the fleet state of `broker` at once and then at most once per
/// [`FLEET_COLLECT_INTERVAL`], and record each read in `collector`, until
/// `stop` is cancelled. Logs one WARNING line per part without an answer.
///
/// The interval runs from the start of one read to the start of the next;
/// a read that takes longer delays the next one, and reads never overlap.
/// Each read holds at most one connection of the main core pool, and each
/// of its statements is bounded by
/// [`FLEET_QUERY_TIMEOUT`](super::FLEET_QUERY_TIMEOUT). A cancel during a
/// read drops the read; it only reads, so nothing changed.
pub async fn collect_fleet_state(
    collector: &FleetCollector,
    broker: &PostgresBroker,
    stop: &CancellationToken,
) {
    collect_with(collector, || read_fleet_state(broker), stop).await;
}

/// [`collect_fleet_state`] through `handle`: a read connects first while no
/// connect has succeeded ([`read_fleet_state_through`]); a failed connect is
/// a read in which every part failed.
pub async fn collect_fleet_state_through(
    collector: &FleetCollector,
    handle: &CoreConnection,
    stop: &CancellationToken,
) {
    collect_with(collector, || read_fleet_state_through(handle), stop).await;
}

/// [`collect_fleet_state`] with the read `read`.
pub(crate) async fn collect_with<R, F>(
    collector: &FleetCollector,
    mut read: R,
    stop: &CancellationToken,
) where
    R: FnMut() -> F,
    F: Future<Output = FleetState>,
{
    let mut ticks = tokio::time::interval(FLEET_COLLECT_INTERVAL);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            _started = ticks.tick() => {}
        }
        let state = tokio::select! {
            biased;
            () = stop.cancelled() => return,
            state = read() => state,
        };
        log_recorded(&collector.record(&state));
    }
}

/// One WARNING line per part without an answer, and one for dropped
/// series.
pub(crate) fn log_recorded(recorded: &FleetRecorded) {
    for error in &recorded.failed {
        tracing::warn!("fleet collector: {error}; its gauges have no series until a read answers");
    }
    match recorded.dropped_at_series_bound {
        0 => {}
        dropped => tracing::warn!(
            "fleet collector: {dropped} series passed the series bound of their metric and were \
             dropped"
        ),
    }
}

#[cfg(test)]
#[path = "collector_tests.rs"]
mod tests;
