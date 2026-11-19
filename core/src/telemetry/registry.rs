//! The metric registry of one process: the series of each metric, and their
//! values.
//!
//! A series is one combination of label values of a metric. Its key is the
//! position of each label value in its closed set ([`SeriesKey`]). The
//! registry keeps the series of one metric in one map:
//! - a sample of a series that exists takes the read lock of the map and
//!   updates atomics (relaxed order);
//! - a sample of a new series takes the write lock once, to insert it;
//! - a metric holds at most [`MAX_SERIES_PER_METRIC`] series. A sample of a
//!   new series past the bound is dropped and counted in
//!   `horsies.telemetry.series.dropped{metric}`.
//!
//! No update waits for anything but these short locks. A render
//! ([`Registry::snapshot`]) holds the read lock of one map while it loads
//! the atomics of that map.
//!
//! Handles ([`Counter`], [`Gauge`], [`Histogram`]) take their label values as
//! [`LabelValue`] types, so a value outside the set of its label does not
//! type-check:
//!
//! ```
//! use horsies_core::telemetry::registry::{Counter, Recorded};
//! use horsies_core::Execution;
//!
//! fn count_start(started: &Counter<(Execution,)>) -> Recorded {
//!     started.add((Execution::Isolated,), 1)
//! }
//! ```
//!
//! ```compile_fail
//! use horsies_core::telemetry::registry::{Counter, Recorded};
//! use horsies_core::Execution;
//!
//! fn count_start(started: &Counter<(Execution,)>) -> Recorded {
//!     started.add(("ISOLATED",), 1)
//! }
//! ```
//!
//! # Series count
//!
//! A series exists from its first sample. A histogram series is 24 lines of
//! the exposition: one per bucket bound (21), `+Inf`, `_sum` and `_count`.
//! For a worker that runs `P` pairs of task and queue, each in one execution
//! class, the series of the task metrics are at most:
//!
//! | Metric | Series |
//! |---|---|
//! | `horsies.task.attempts` | `P × (1 + F + W)`: one `COMPLETED` series with no code, and one per error code that ended an attempt `FAILED` (`F`) or `WORKER_FAILURE` (`W`) |
//! | `horsies.task.attempts.end_not_known` | `P` |
//! | `horsies.task.run.duration` | `P × 3` (one per outcome) |
//! | `horsies.task.retries` | `P × R`, `R` the error codes that scheduled a retry |
//! | `horsies.task.queue.duration` | queues × 3 |
//!
//! Every other worker metric has a fixed bound: the product of the sizes of
//! its label sets (for example `horsies.worker.children.replaced`: 3 kinds ×
//! 13 reasons), or the number of event handlers.
//!
//! Example: 200 tasks on one queue, each in one class, each with one error
//! code that fails it and `WORKER_CRASHED`, retried on its code:
//! `horsies.task.attempts` 200 × 3 = 600, `end_not_known` 200,
//! `run.duration` 600 series (14,400 lines), `retries` 200: 1,600 series of
//! task metrics in 15,400 lines. The other worker metrics hold at most 131
//! series, plus 2 per event handler.
//!
//! When each task runs on one queue, the `queue` label adds no series: the
//! task gives the queue. It adds bytes to each line; a scrape config can
//! drop it (`labeldrop`). A task sent to `N` queues has `N` times the series.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

use crate::telemetry::catalog::{MetricId, MetricKind, Recorder};
use crate::telemetry::declared::{
    DeclaredNames, ErrorCodeValues, HandlerName, QueueName, ScheduleName, TaskName,
};
use crate::telemetry::labels::{
    group_texts, DeclaredSource, LabelName, LabelValue, LabelValues, ProcessFact,
};

/// The most series of one metric in one registry.
pub const MAX_SERIES_PER_METRIC: usize = 10_000;

/// The most labels of one metric: the length of a [`SeriesKey`].
pub const MAX_LABELS_PER_METRIC: usize = 5;

/// The key of a series: the position of each label value, in label order.
/// The positions after the last label of the metric are 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SeriesKey([u32; MAX_LABELS_PER_METRIC]);

impl SeriesKey {
    /// The key of `positions`, in label order.
    fn of(positions: &[u32]) -> Self {
        let mut key = [0_u32; MAX_LABELS_PER_METRIC];
        for (slot, position) in key.iter_mut().zip(positions) {
            *slot = *position;
        }
        Self(key)
    }
}

/// The label values of one series, as a tuple of [`LabelValue`] types in
/// the label order of the metric.
pub trait LabelSet: Copy {
    /// The value sets of the labels, in label order.
    fn values() -> Vec<LabelValues>;

    /// The key of the series.
    fn key(self) -> SeriesKey;
}

impl LabelSet for () {
    fn values() -> Vec<LabelValues> {
        Vec::new()
    }

    fn key(self) -> SeriesKey {
        SeriesKey::of(&[])
    }
}

impl<A: LabelValue> LabelSet for (A,) {
    fn values() -> Vec<LabelValues> {
        vec![A::VALUES]
    }

    fn key(self) -> SeriesKey {
        SeriesKey::of(&[self.0.position()])
    }
}

impl<A: LabelValue, B: LabelValue> LabelSet for (A, B) {
    fn values() -> Vec<LabelValues> {
        vec![A::VALUES, B::VALUES]
    }

    fn key(self) -> SeriesKey {
        SeriesKey::of(&[self.0.position(), self.1.position()])
    }
}

impl<A: LabelValue, B: LabelValue, C: LabelValue> LabelSet for (A, B, C) {
    fn values() -> Vec<LabelValues> {
        vec![A::VALUES, B::VALUES, C::VALUES]
    }

    fn key(self) -> SeriesKey {
        SeriesKey::of(&[self.0.position(), self.1.position(), self.2.position()])
    }
}

impl<A: LabelValue, B: LabelValue, C: LabelValue, D: LabelValue> LabelSet for (A, B, C, D) {
    fn values() -> Vec<LabelValues> {
        vec![A::VALUES, B::VALUES, C::VALUES, D::VALUES]
    }

    fn key(self) -> SeriesKey {
        SeriesKey::of(&[
            self.0.position(),
            self.1.position(),
            self.2.position(),
            self.3.position(),
        ])
    }
}

impl<A: LabelValue, B: LabelValue, C: LabelValue, D: LabelValue, E: LabelValue> LabelSet
    for (A, B, C, D, E)
{
    fn values() -> Vec<LabelValues> {
        vec![A::VALUES, B::VALUES, C::VALUES, D::VALUES, E::VALUES]
    }

    fn key(self) -> SeriesKey {
        SeriesKey::of(&[
            self.0.position(),
            self.1.position(),
            self.2.position(),
            self.3.position(),
            self.4.position(),
        ])
    }
}

/// What a sample did.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recorded {
    /// The sample is in its series.
    Added,
    /// The series is new and the metric holds [`MAX_SERIES_PER_METRIC`]
    /// series: the sample was dropped and counted in
    /// `horsies.telemetry.series.dropped{metric}`.
    DroppedAtSeriesBound,
    /// A label value is a position past the values of its label in this
    /// registry: a value of the declared names of another registry. The
    /// sample was dropped.
    OutsideLabelSet { label: LabelName },
}

/// The facts of a worker process, the labels of `horsies.worker.info`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerFacts {
    /// The horsies version of the process.
    pub version: String,
    /// The worker profile that the process runs.
    pub profile: String,
    /// The worker id.
    pub worker_id: String,
}

/// Whether the registry is the registry of a worker process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessFacts {
    /// A worker process, with its facts.
    Worker(WorkerFacts),
    /// Another process (the fleet collector): no metric with a process fact
    /// label.
    NotAWorker,
}

/// The values of the labels that are not fixed in the core.
#[derive(Debug, Clone)]
pub struct RegistryLabels {
    pub tasks: DeclaredNames<TaskName>,
    pub queues: DeclaredNames<QueueName>,
    pub handlers: DeclaredNames<HandlerName>,
    pub schedules: DeclaredNames<ScheduleName>,
    pub error_codes: ErrorCodeValues,
    pub process: ProcessFacts,
}

impl RegistryLabels {
    /// The declared names of `source`, then its fallback.
    fn declared_texts(&self, source: DeclaredSource) -> Vec<String> {
        match source {
            DeclaredSource::TaskNames => self.tasks.texts(),
            DeclaredSource::QueueNames => self.queues.texts(),
            DeclaredSource::HandlerNames => self.handlers.texts(),
            DeclaredSource::ScheduleNames => self.schedules.texts(),
            DeclaredSource::UserErrorCodes => {
                let mut texts = self.error_codes.declared().to_vec();
                texts.push(source.fallback().to_owned());
                texts
            }
        }
    }

    /// The label texts by position of a label with `values`.
    fn texts(&self, metric: MetricId, values: LabelValues) -> Result<Vec<String>, RegistryRefused> {
        let owned = |texts: Vec<&'static str>| -> Vec<String> {
            texts.into_iter().map(str::to_owned).collect()
        };
        match (values, &self.process) {
            (LabelValues::Fixed(groups), _) => Ok(owned(group_texts(groups))),
            (LabelValues::Declared(source), _) => Ok(self.declared_texts(source)),
            (LabelValues::FixedThenDeclared(groups, source), _) => {
                let mut texts = owned(group_texts(groups));
                texts.extend(self.declared_texts(source));
                Ok(texts)
            }
            (LabelValues::OneProcessValue(fact), ProcessFacts::Worker(facts)) => {
                let text = match fact {
                    ProcessFact::Version => facts.version.clone(),
                    ProcessFact::Profile => facts.profile.clone(),
                    ProcessFact::WorkerId => facts.worker_id.clone(),
                };
                Ok(vec![text])
            }
            (LabelValues::OneProcessValue(_), ProcessFacts::NotAWorker) => {
                Err(RegistryRefused::NoWorkerFacts { metric })
            }
        }
    }
}

/// Why a registry is not built.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RegistryRefused {
    /// The metric has a label of a worker fact, and the process is not a
    /// worker.
    #[error("{} needs the facts of a worker process", .metric.spec().name)]
    NoWorkerFacts { metric: MetricId },
}

/// Why a handle is not given.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HandleRefused {
    /// The registry does not hold the metric.
    #[error("the registry does not hold {}", .metric.spec().name)]
    NotInRegistry { metric: MetricId },
    /// The metric is of another kind than the handle.
    #[error("{} is a {}", .metric.spec().name, .metric.spec().kind.prometheus_type())]
    KindDiffers { metric: MetricId },
    /// The label types of the handle are not the label sets of the metric.
    #[error("the label types of the handle are not the labels of {}", .metric.spec().name)]
    LabelsDiffer { metric: MetricId },
    /// The registry counts this metric itself.
    #[error("the registry counts {} itself", .metric.spec().name)]
    CountedByTheRegistry { metric: MetricId },
}

/// The cells of one series of a kind.
trait Cell: Send + Sync + Sized {
    /// What every series of a metric shares: the bucket bounds of a
    /// histogram.
    type Shape: Copy + Send + Sync;

    fn new(shape: Self::Shape) -> Self;
}

/// The count of a counter series.
struct CounterCell(AtomicU64);

impl Cell for CounterCell {
    type Shape = ();

    fn new(_shape: ()) -> Self {
        Self(AtomicU64::new(0))
    }
}

/// The value of a gauge series: the bits of an `f64`.
struct GaugeCell(AtomicU64);

impl Cell for GaugeCell {
    type Shape = ();

    fn new(_shape: ()) -> Self {
        Self(AtomicU64::new(0_f64.to_bits()))
    }
}

/// The counts of a histogram series: one per bucket bound, then one for the
/// durations above the last bound; and the sum of the durations in whole
/// microseconds, rounded to the nearest. The count of the series is the sum
/// of the buckets, so `+Inf` and `_count` always agree. 2^64 microseconds
/// are about 584,000 years of summed time.
struct HistogramCell {
    bounds: &'static [Duration],
    buckets: Box<[AtomicU64]>,
    sum_micros: AtomicU64,
}

impl Cell for HistogramCell {
    type Shape = &'static [Duration];

    fn new(bounds: &'static [Duration]) -> Self {
        Self {
            bounds,
            buckets: (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect(),
            sum_micros: AtomicU64::new(0),
        }
    }
}

impl HistogramCell {
    fn observe(&self, duration: Duration) {
        let index = self
            .bounds
            .iter()
            .position(|bound| duration <= *bound)
            .unwrap_or(self.bounds.len());
        match self.buckets.get(index) {
            Some(bucket) => {
                let _before = bucket.fetch_add(1, Ordering::Relaxed);
            }
            None => unreachable!("a histogram has one bucket per bound and one above them"),
        }
        let _before = self
            .sum_micros
            .fetch_add(rounded_micros(duration), Ordering::Relaxed);
    }
}

/// `duration` in whole microseconds, rounded to the nearest; at most
/// `u64::MAX`.
fn rounded_micros(duration: Duration) -> u64 {
    let micros = duration.as_nanos().saturating_add(500) / 1_000;
    u64::try_from(micros).unwrap_or(u64::MAX)
}

/// The series of one metric.
struct Family<C: Cell> {
    metric: MetricId,
    /// The texts of each label, by position.
    texts: Vec<Vec<String>>,
    shape: C::Shape,
    series: RwLock<HashMap<SeriesKey, C>>,
    drops: Arc<SeriesDrops>,
}

fn read_series<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

fn write_series<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}

impl<C: Cell> Family<C> {
    fn new(
        metric: MetricId,
        texts: Vec<Vec<String>>,
        shape: C::Shape,
        drops: Arc<SeriesDrops>,
    ) -> Self {
        Self {
            metric,
            texts,
            shape,
            series: RwLock::new(HashMap::new()),
            drops,
        }
    }

    /// Run `apply` on the series of `key`; make the series when it is new
    /// and the metric is under its series bound.
    fn update(&self, key: SeriesKey, apply: impl FnOnce(&C)) -> Recorded {
        {
            let series = read_series(&self.series);
            if let Some(cell) = series.get(&key) {
                apply(cell);
                return Recorded::Added;
            }
        }
        if let Some(label) = self.label_outside(key) {
            return Recorded::OutsideLabelSet { label };
        }
        let mut series = write_series(&self.series);
        if let Some(cell) = series.get(&key) {
            apply(cell);
            return Recorded::Added;
        }
        match series.len() < MAX_SERIES_PER_METRIC {
            true => {
                let cell = C::new(self.shape);
                apply(&cell);
                let _replaced = series.insert(key, cell);
                Recorded::Added
            }
            false => {
                self.drops.count(self.metric);
                Recorded::DroppedAtSeriesBound
            }
        }
    }

    /// The first label of `key` whose position has no text.
    fn label_outside(&self, key: SeriesKey) -> Option<LabelName> {
        self.metric
            .spec()
            .labels
            .iter()
            .zip(&self.texts)
            .zip(key.0)
            .find(|((_, texts), position)| *position as usize >= texts.len())
            .map(|((label, _), _)| label.name)
    }

    /// The texts of the labels of `key`.
    fn label_texts(&self, key: &SeriesKey) -> Vec<String> {
        self.texts
            .iter()
            .zip(key.0)
            .map(|(texts, position)| match texts.get(position as usize) {
                Some(text) => text.clone(),
                None => unreachable!("a series is made only with positions that have texts"),
            })
            .collect()
    }

    /// The labels and the value of each series.
    fn snapshot(&self, value: impl Fn(&C) -> SampleValue) -> Vec<SeriesSnapshot> {
        let series = read_series(&self.series);
        series
            .iter()
            .map(|(key, cell)| SeriesSnapshot {
                label_texts: self.label_texts(key),
                value: value(cell),
            })
            .collect()
    }
}

/// The samples that the series bound dropped, by metric.
struct SeriesDrops([AtomicU64; MetricId::ALL.len()]);

impl SeriesDrops {
    fn new() -> Self {
        Self(std::array::from_fn(|_| AtomicU64::new(0)))
    }

    fn count(&self, metric: MetricId) {
        let _before = self.0[metric as usize].fetch_add(1, Ordering::Relaxed);
    }

    fn get(&self, metric: MetricId) -> u64 {
        self.0[metric as usize].load(Ordering::Relaxed)
    }
}

/// The series of one metric of a registry.
enum Cells {
    Counters(Arc<Family<CounterCell>>),
    Gauges(Arc<Family<GaugeCell>>),
    Histograms(Arc<Family<HistogramCell>>),
    /// `horsies.telemetry.series.dropped`: the registry's own drop counts.
    SeriesDrops,
}

/// A counter of one metric.
pub struct Counter<L> {
    family: Arc<Family<CounterCell>>,
    labels: PhantomData<fn(L)>,
}

impl<L> Clone for Counter<L> {
    fn clone(&self) -> Self {
        Self {
            family: Arc::clone(&self.family),
            labels: PhantomData,
        }
    }
}

impl<L: LabelSet> Counter<L> {
    /// Add `by` to the series of `labels`.
    pub fn add(&self, labels: L, by: u64) -> Recorded {
        self.family.update(labels.key(), |cell| {
            let _before = cell.0.fetch_add(by, Ordering::Relaxed);
        })
    }
}

/// The counter of a metric in a process that records metrics, or no
/// counter in a process that records none.
pub enum OptionalCounter<L> {
    /// The process records no metric.
    NotRecorded,
    /// The counter of the process's registry.
    Recording(Counter<L>),
}

impl<L> Clone for OptionalCounter<L> {
    fn clone(&self) -> Self {
        match self {
            Self::NotRecorded => Self::NotRecorded,
            Self::Recording(counter) => Self::Recording(counter.clone()),
        }
    }
}

/// A gauge of one metric.
pub struct Gauge<L> {
    family: Arc<Family<GaugeCell>>,
    labels: PhantomData<fn(L)>,
}

impl<L> Clone for Gauge<L> {
    fn clone(&self) -> Self {
        Self {
            family: Arc::clone(&self.family),
            labels: PhantomData,
        }
    }
}

impl<L: LabelSet> Gauge<L> {
    /// Set the series of `labels` to `value`.
    pub fn set(&self, labels: L, value: f64) -> Recorded {
        self.family.update(labels.key(), |cell| {
            cell.0.store(value.to_bits(), Ordering::Relaxed);
        })
    }
}

/// A histogram of durations of one metric.
pub struct Histogram<L> {
    family: Arc<Family<HistogramCell>>,
    labels: PhantomData<fn(L)>,
}

impl<L> Clone for Histogram<L> {
    fn clone(&self) -> Self {
        Self {
            family: Arc::clone(&self.family),
            labels: PhantomData,
        }
    }
}

impl<L: LabelSet> Histogram<L> {
    /// Count `duration` in the series of `labels`.
    pub fn observe(&self, labels: L, duration: Duration) -> Recorded {
        self.family
            .update(labels.key(), |cell| cell.observe(duration))
    }
}

/// The value of one series at a snapshot.
#[derive(Debug, Clone, PartialEq)]
pub enum SampleValue {
    Counter(u64),
    Gauge(f64),
    /// The count of each bucket, not cumulative: one per bound, then the
    /// count above the last bound; and the sum in whole microseconds.
    Histogram {
        buckets: Vec<u64>,
        sum_micros: u64,
    },
}

/// One series at a snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesSnapshot {
    /// The label values, in the label order of the metric.
    pub label_texts: Vec<String>,
    pub value: SampleValue,
}

/// One metric at a snapshot. The series are in no order.
#[derive(Debug, Clone, PartialEq)]
pub struct FamilySnapshot {
    pub metric: MetricId,
    pub series: Vec<SeriesSnapshot>,
}

/// The registry of one process.
pub struct Registry {
    /// The metrics of the registry, in catalog order.
    families: Vec<(MetricId, Cells)>,
    drops: Arc<SeriesDrops>,
    labels: RegistryLabels,
}

impl Registry {
    /// The registry of a worker: every metric that the worker records.
    ///
    /// Errors: as [`Registry::new`].
    pub fn for_worker(labels: RegistryLabels) -> Result<Self, RegistryRefused> {
        let metrics: Vec<MetricId> = MetricId::ALL
            .into_iter()
            .filter(|metric| metric.spec().recorder == Recorder::Worker)
            .collect();
        Self::new(&metrics, labels)
    }

    /// The registry of `metrics` and `horsies.telemetry.series.dropped`,
    /// which every registry holds. A metric listed twice counts once. The
    /// `horsies.worker.info` series is 1 from the start.
    ///
    /// The texts of each label are unique: the fixed values of the catalog
    /// are unique, a declared set holds each name once and never its
    /// fallback, and a declared error code is never a built-in code or
    /// `undeclared`. Every metric of the catalog has at most
    /// [`MAX_LABELS_PER_METRIC`] labels. Tests hold both facts.
    ///
    /// Errors: [`RegistryRefused::NoWorkerFacts`] for a metric with a worker
    /// fact label when `labels.process` is [`ProcessFacts::NotAWorker`].
    pub fn new(metrics: &[MetricId], labels: RegistryLabels) -> Result<Self, RegistryRefused> {
        let drops = Arc::new(SeriesDrops::new());
        let mut families = Vec::new();
        for metric in MetricId::ALL {
            match (metric, metrics.contains(&metric)) {
                (MetricId::SeriesDropped, _) => families.push((metric, Cells::SeriesDrops)),
                (_, true) => families.push((metric, Self::cells(metric, &labels, &drops)?)),
                (_, false) => {}
            }
        }
        let registry = Self {
            families,
            drops,
            labels,
        };
        registry.set_worker_info();
        Ok(registry)
    }

    /// The empty series map of `metric`.
    fn cells(
        metric: MetricId,
        labels: &RegistryLabels,
        drops: &Arc<SeriesDrops>,
    ) -> Result<Cells, RegistryRefused> {
        let spec = metric.spec();
        let mut texts = Vec::with_capacity(spec.labels.len());
        for label in spec.labels {
            texts.push(labels.texts(metric, label.values)?);
        }
        let drops = Arc::clone(drops);
        Ok(match spec.kind {
            MetricKind::Counter => Cells::Counters(Arc::new(Family::new(metric, texts, (), drops))),
            MetricKind::Gauge => Cells::Gauges(Arc::new(Family::new(metric, texts, (), drops))),
            MetricKind::Histogram(bounds) => {
                Cells::Histograms(Arc::new(Family::new(metric, texts, bounds, drops)))
            }
        })
    }

    /// Set the one series of `horsies.worker.info` to 1, when the registry
    /// holds it.
    fn set_worker_info(&self) {
        let info = self.gauge::<(VersionValue, ProfileValue, WorkerIdValue)>(MetricId::WorkerInfo);
        match info {
            Ok(info) => {
                let _recorded = info.set((VersionValue, ProfileValue, WorkerIdValue), 1.0);
            }
            Err(HandleRefused::NotInRegistry { .. }) => {}
            Err(
                refused @ (HandleRefused::KindDiffers { .. }
                | HandleRefused::LabelsDiffer { .. }
                | HandleRefused::CountedByTheRegistry { .. }),
            ) => unreachable!("the info labels are the labels of its catalog entry: {refused}"),
        }
    }

    /// The values of the labels that are not fixed in the core.
    pub fn labels(&self) -> &RegistryLabels {
        &self.labels
    }

    fn cells_of(&self, metric: MetricId) -> Result<&Cells, HandleRefused> {
        match self.families.iter().find(|(held, _)| *held == metric) {
            Some((_, cells)) => Ok(cells),
            None => Err(HandleRefused::NotInRegistry { metric }),
        }
    }

    /// Whether `L` are the label sets of `metric`.
    fn check_labels<L: LabelSet>(metric: MetricId) -> Result<(), HandleRefused> {
        let expected: Vec<LabelValues> = metric
            .spec()
            .labels
            .iter()
            .map(|label| label.values)
            .collect();
        match L::values() == expected {
            true => Ok(()),
            false => Err(HandleRefused::LabelsDiffer { metric }),
        }
    }

    /// The counter handle of `metric`.
    ///
    /// Errors: [`HandleRefused::NotInRegistry`], [`HandleRefused::KindDiffers`],
    /// [`HandleRefused::LabelsDiffer`], [`HandleRefused::CountedByTheRegistry`].
    pub fn counter<L: LabelSet>(&self, metric: MetricId) -> Result<Counter<L>, HandleRefused> {
        let cells = self.cells_of(metric)?;
        Self::check_labels::<L>(metric)?;
        match cells {
            Cells::Counters(family) => Ok(Counter {
                family: Arc::clone(family),
                labels: PhantomData,
            }),
            Cells::Gauges(_) | Cells::Histograms(_) => Err(HandleRefused::KindDiffers { metric }),
            Cells::SeriesDrops => Err(HandleRefused::CountedByTheRegistry { metric }),
        }
    }

    /// The gauge handle of `metric`.
    ///
    /// Errors: as [`Registry::counter`].
    pub fn gauge<L: LabelSet>(&self, metric: MetricId) -> Result<Gauge<L>, HandleRefused> {
        let cells = self.cells_of(metric)?;
        Self::check_labels::<L>(metric)?;
        match cells {
            Cells::Gauges(family) => Ok(Gauge {
                family: Arc::clone(family),
                labels: PhantomData,
            }),
            Cells::Counters(_) | Cells::Histograms(_) => Err(HandleRefused::KindDiffers { metric }),
            Cells::SeriesDrops => Err(HandleRefused::CountedByTheRegistry { metric }),
        }
    }

    /// The histogram handle of `metric`.
    ///
    /// Errors: as [`Registry::counter`].
    pub fn histogram<L: LabelSet>(&self, metric: MetricId) -> Result<Histogram<L>, HandleRefused> {
        let cells = self.cells_of(metric)?;
        Self::check_labels::<L>(metric)?;
        match cells {
            Cells::Histograms(family) => Ok(Histogram {
                family: Arc::clone(family),
                labels: PhantomData,
            }),
            Cells::Counters(_) | Cells::Gauges(_) => Err(HandleRefused::KindDiffers { metric }),
            Cells::SeriesDrops => Err(HandleRefused::CountedByTheRegistry { metric }),
        }
    }

    /// The samples that the series bound dropped for `metric`.
    pub fn series_dropped(&self, metric: MetricId) -> u64 {
        self.drops.get(metric)
    }

    /// Every metric of the registry with the value of each of its series,
    /// in catalog order.
    pub fn snapshot(&self) -> Vec<FamilySnapshot> {
        self.families
            .iter()
            .map(|(metric, cells)| FamilySnapshot {
                metric: *metric,
                series: self.series_of(cells),
            })
            .collect()
    }

    fn series_of(&self, cells: &Cells) -> Vec<SeriesSnapshot> {
        match cells {
            Cells::Counters(family) => {
                family.snapshot(|cell| SampleValue::Counter(cell.0.load(Ordering::Relaxed)))
            }
            Cells::Gauges(family) => family.snapshot(|cell| {
                SampleValue::Gauge(f64::from_bits(cell.0.load(Ordering::Relaxed)))
            }),
            Cells::Histograms(family) => family.snapshot(|cell| SampleValue::Histogram {
                buckets: cell
                    .buckets
                    .iter()
                    .map(|bucket| bucket.load(Ordering::Relaxed))
                    .collect(),
                sum_micros: cell.sum_micros.load(Ordering::Relaxed),
            }),
            Cells::SeriesDrops => self
                .families
                .iter()
                .filter(|(_, cells)| !matches!(cells, Cells::SeriesDrops))
                .map(|(metric, _)| SeriesSnapshot {
                    label_texts: vec![metric.spec().name.to_owned()],
                    value: SampleValue::Counter(self.drops.get(*metric)),
                })
                .collect(),
        }
    }
}

/// The one value of the `version` label of `horsies.worker.info`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VersionValue;

impl LabelValue for VersionValue {
    const VALUES: LabelValues = LabelValues::OneProcessValue(ProcessFact::Version);

    fn position(self) -> u32 {
        0
    }
}

/// The one value of the `profile` label of `horsies.worker.info`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProfileValue;

impl LabelValue for ProfileValue {
    const VALUES: LabelValues = LabelValues::OneProcessValue(ProcessFact::Profile);

    fn position(self) -> u32 {
        0
    }
}

/// The one value of the `worker_id` label of `horsies.worker.info`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkerIdValue;

impl LabelValue for WorkerIdValue {
    const VALUES: LabelValues = LabelValues::OneProcessValue(ProcessFact::WorkerId);

    fn position(self) -> u32 {
        0
    }
}
