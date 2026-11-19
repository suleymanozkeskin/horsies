//! The telemetry of a worker under test: a worker registry, the series of
//! one metric, and a worker telemetry value whose events a test reads.

use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::events::Event;
use crate::telemetry::catalog::MetricId;
use crate::telemetry::declared::{DeclaredNames, ErrorCodeValues};
use crate::telemetry::registry::{
    ProcessFacts, Registry, RegistryLabels, SampleValue, WorkerFacts,
};
use crate::telemetry::worker::{DeclaredCodesByTask, EventQueue, TelemetryOn, WorkerTelemetry};

/// The most events that a test reads at once.
const MOST_TEST_EVENTS: NonZeroUsize = match NonZeroUsize::new(4_096) {
    Some(events) => events,
    None => panic!("4,096 is not zero"),
};

/// The registry of a worker with no declared name.
pub(crate) fn worker_registry() -> Arc<Registry> {
    let registry = Registry::for_worker(RegistryLabels {
        tasks: DeclaredNames::none(),
        queues: DeclaredNames::none(),
        handlers: DeclaredNames::none(),
        schedules: DeclaredNames::none(),
        error_codes: ErrorCodeValues::none(),
        process: ProcessFacts::Worker(WorkerFacts {
            version: "0.8.0".to_owned(),
            profile: "default".to_owned(),
            worker_id: "worker-1".to_owned(),
        }),
    })
    .expect("the registry of a worker builds");
    Arc::new(registry)
}

/// The series of `metric` in `registry`: the label texts and the value of
/// each, sorted by label texts.
pub(crate) fn series_of(registry: &Registry, metric: MetricId) -> Vec<(Vec<String>, SampleValue)> {
    let mut series: Vec<(Vec<String>, SampleValue)> = registry
        .snapshot()
        .into_iter()
        .find(|family| family.metric == metric)
        .expect("the registry holds the metric")
        .series
        .into_iter()
        .map(|series| (series.label_texts, series.value))
        .collect();
    series.sort_by(|left, right| left.0.cmp(&right.0));
    series
}

/// A worker telemetry value that records in a fresh worker registry and
/// queues its events, with the registry and the queue.
pub(crate) struct WatchedTelemetry {
    pub telemetry: WorkerTelemetry,
    pub registry: Arc<Registry>,
    pub queue: EventQueue,
}

impl WatchedTelemetry {
    /// On a worker registry with no declared name.
    pub(crate) fn new() -> Self {
        Self::with(worker_registry(), DeclaredCodesByTask::none())
    }

    /// On `registry`, with the declared codes `codes`.
    pub(crate) fn with(registry: Arc<Registry>, codes: DeclaredCodesByTask) -> Self {
        let (on, queue) = TelemetryOn::with_event_queue(
            Arc::clone(&registry),
            codes,
            crate::telemetry::worker::EVENT_QUEUE_CAPACITY,
        )
        .expect("the telemetry of a worker registry");
        Self {
            telemetry: WorkerTelemetry::On(Arc::new(on)),
            registry,
            queue,
        }
    }

    /// The events that the points emitted since the last read, in order.
    pub(crate) fn events(&mut self) -> Vec<Event> {
        self.queue.take_waiting(MOST_TEST_EVENTS)
    }

    /// The series of `metric`, sorted by label texts.
    pub(crate) fn series(&self, metric: MetricId) -> Vec<(Vec<String>, SampleValue)> {
        series_of(&self.registry, metric)
    }
}
