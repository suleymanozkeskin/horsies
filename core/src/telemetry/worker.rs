//! The telemetry of a worker parent: the metric handles that its lifecycle
//! points record, and the bounded queue of its events to the handler child.
//!
//! A worker holds one [`WorkerTelemetry`], chosen at start:
//! - [`WorkerTelemetry::Off`] (the default): a point is one branch. No event
//!   is built and no metric is recorded.
//! - [`WorkerTelemetry::On`] with [`EventRoute::NoHandlerChild`]: a point
//!   records its metrics; no event is built.
//! - [`WorkerTelemetry::On`] with [`EventRoute::HandlerChildLink`]: a point
//!   also builds its event, when the handler child takes events of its type
//!   ([`SentEventTypes`]), and offers it to the event queue
//!   ([`EventLink::offer`]). An offer never waits: a full queue drops the
//!   event and counts `queue_full`, a closed queue drops it and counts
//!   `child_down` (`horsies.telemetry.events.dropped{cause}`). An event of
//!   a type that the child does not take is not built.
//!
//! A point never fails and never changes what the worker does. A sample
//! past the series bound of its metric is counted by the registry; an event
//! that is not queued is counted here.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{TryRecvError, TrySendError};

use crate::core::task::{DeclaredErrorCodes, Execution};
use crate::events::{Event, EventTypeName, UnionEvent};
use crate::telemetry::catalog::MetricId;
use crate::telemetry::declared::{self, DeclaredValue, ErrorCodeValue};
use crate::telemetry::labels::{
    ClaimPassResult, EventDropCause, RecoveryAction, RecoveryStep, ReleaseReason, ReplaceReason,
};
use crate::telemetry::registry::{Counter, HandleRefused, Histogram, OptionalCounter, Registry};
use crate::worker::executor::protocol::StopReason;
use crate::worker::executor::slots::outcome::WriteStage;
use crate::TaskAttemptOutcome;

mod end_points;
mod points;
mod recovery_points;
mod slot_points;
#[cfg(test)]
mod tests;

pub(crate) use end_points::EndNoted;

/// The most events that wait in the event queue of a worker. An offer to a
/// full queue drops its event.
pub const EVENT_QUEUE_CAPACITY: NonZeroUsize = match NonZeroUsize::new(8_192) {
    Some(capacity) => capacity,
    None => panic!("8,192 is not zero"),
};

/// The telemetry of a worker, chosen at start. Clones share one value.
#[derive(Clone, Default)]
pub enum WorkerTelemetry {
    /// No metric is recorded and no event is built.
    #[default]
    Off,
    /// The points record their metrics, and offer their events when a
    /// handler child is linked.
    On(Arc<TelemetryOn>),
}

impl std::fmt::Debug for WorkerTelemetry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Off => formatter.write_str("WorkerTelemetry::Off"),
            Self::On(_) => formatter.write_str("WorkerTelemetry::On"),
        }
    }
}

impl WorkerTelemetry {
    /// The counter of the failed steps of the reaper passes
    /// (`horsies.recovery.pass.failures{step}`): no counter when telemetry
    /// is off.
    pub(crate) fn recovery_failures(&self) -> OptionalCounter<(RecoveryStep,)> {
        match self {
            Self::Off => OptionalCounter::NotRecorded,
            Self::On(on) => OptionalCounter::Recording(on.metrics.recovery_failures.clone()),
        }
    }
}

/// The event types that a worker sends to its handler child. A point does
/// not build an event of another type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SentEventTypes(u32);

// One bit per event type.
const _: () = assert!(EventTypeName::ALL.len() <= u32::BITS as usize);

impl SentEventTypes {
    /// Every event type.
    pub fn all() -> Self {
        Self::of(EventTypeName::ALL.iter().copied())
    }

    /// The types of `names`; a name given twice counts once.
    pub fn of(names: impl IntoIterator<Item = EventTypeName>) -> Self {
        Self(
            names
                .into_iter()
                .fold(0, |bits, name| bits | type_bit(name)),
        )
    }

    /// Whether the events of the type `name` are sent.
    pub fn sends(self, name: EventTypeName) -> bool {
        self.0 & type_bit(name) != 0
    }
}

/// The bit of `name` in a [`SentEventTypes`].
fn type_bit(name: EventTypeName) -> u32 {
    1_u32 << (name as u32)
}

/// Where the events of the points go.
pub enum EventRoute {
    /// No handler child runs: no event is built.
    NoHandlerChild,
    /// The events go to the event queue, which the writer of the handler
    /// child reads.
    HandlerChildLink(EventLink),
}

/// The user error codes that each task of a worker declares: the
/// `error_codes` of each task of the worker manifest. The `error_code` label
/// of a user code is the code when its task declares it, else `undeclared`.
#[derive(Debug, Clone, Default)]
pub struct DeclaredCodesByTask {
    by_task: HashMap<String, DeclaredErrorCodes>,
    /// The set of a task that the worker manifest does not name.
    none: DeclaredErrorCodes,
}

impl DeclaredCodesByTask {
    /// The declared codes of each task, by task name.
    pub fn new(by_task: HashMap<String, DeclaredErrorCodes>) -> Self {
        Self {
            by_task,
            none: DeclaredErrorCodes::NONE,
        }
    }

    /// No task declares a code.
    pub fn none() -> Self {
        Self::new(HashMap::new())
    }

    /// The codes that task `task_name` declares; none for a task that the
    /// worker manifest does not name.
    pub fn of(&self, task_name: &str) -> &DeclaredErrorCodes {
        self.by_task.get(task_name).unwrap_or(&self.none)
    }
}

/// The telemetry of a worker that records metrics.
pub struct TelemetryOn {
    registry: Arc<Registry>,
    codes: DeclaredCodesByTask,
    metrics: PointMetrics,
    events: EventRoute,
}

/// The value of the `task` label.
type TaskLabel = DeclaredValue<declared::TaskName>;
/// The value of the `queue` label.
type QueueLabel = DeclaredValue<declared::QueueName>;

/// The metric handles that the points of a worker record.
struct PointMetrics {
    attempts: Counter<(
        TaskLabel,
        QueueLabel,
        Execution,
        TaskAttemptOutcome,
        ErrorCodeValue,
    )>,
    end_not_known: Counter<(TaskLabel, QueueLabel, Execution)>,
    run_duration: Histogram<(TaskLabel, QueueLabel, Execution, TaskAttemptOutcome)>,
    queue_duration: Histogram<(QueueLabel, Execution)>,
    slot_wait: Histogram<(Execution,)>,
    record_duration: Histogram<(Execution,)>,
    retries: Counter<(TaskLabel, QueueLabel, ErrorCodeValue)>,
    claims_released: Counter<(ReleaseReason,)>,
    starts_refused: Counter<(StopReason,)>,
    finalize_retries: Counter<(WriteStage,)>,
    claim_passes: Counter<(ClaimPassResult,)>,
    recovery_tasks: Counter<(RecoveryAction,)>,
    recovery_failures: Counter<(RecoveryStep,)>,
    children_started: Counter<(Execution,)>,
    children_replaced: Counter<(Execution, ReplaceReason)>,
    child_boot: Histogram<(Execution,)>,
}

impl PointMetrics {
    /// The handles of `registry`.
    ///
    /// Errors: [`HandleRefused`] when `registry` does not hold a metric of
    /// the points with the labels of the catalog.
    fn of(registry: &Registry) -> Result<Self, HandleRefused> {
        Ok(Self {
            attempts: registry.counter(MetricId::TaskAttempts)?,
            end_not_known: registry.counter(MetricId::TaskAttemptsEndNotKnown)?,
            run_duration: registry.histogram(MetricId::TaskRunDuration)?,
            queue_duration: registry.histogram(MetricId::TaskQueueDuration)?,
            slot_wait: registry.histogram(MetricId::TaskSlotWaitDuration)?,
            record_duration: registry.histogram(MetricId::TaskRecordDuration)?,
            retries: registry.counter(MetricId::TaskRetries)?,
            claims_released: registry.counter(MetricId::TaskClaimsReleased)?,
            starts_refused: registry.counter(MetricId::TaskStartsRefused)?,
            finalize_retries: registry.counter(MetricId::FinalizeRetries)?,
            claim_passes: registry.counter(MetricId::ClaimPasses)?,
            recovery_tasks: registry.counter(MetricId::RecoveryTasks)?,
            recovery_failures: registry.counter(MetricId::RecoveryPassFailures)?,
            children_started: registry.counter(MetricId::ChildrenStarted)?,
            children_replaced: registry.counter(MetricId::ChildrenReplaced)?,
            child_boot: registry.histogram(MetricId::ChildBootDuration)?,
        })
    }
}

/// What a point did with its event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Emitted {
    /// No handler child is linked: the event was not built.
    NotBuilt,
    /// The handler child takes no event of this type: the event was not
    /// built.
    TypeNotSent,
    /// The event was built and offered to the event queue.
    Offered(Offer),
}

impl TelemetryOn {
    /// Telemetry that records the metrics of `registry` and builds no event.
    /// `codes` are the declared codes of each task, for the `error_code`
    /// label; give the registry the same sets (`ErrorCodeValues::new`).
    ///
    /// Errors: [`HandleRefused`] when `registry` does not hold a metric of
    /// the points with the labels of the catalog: a registry that
    /// [`Registry::for_worker`] did not build. Nothing is changed.
    pub fn metrics_only(
        registry: Arc<Registry>,
        codes: DeclaredCodesByTask,
    ) -> Result<Self, HandleRefused> {
        Ok(Self {
            metrics: PointMetrics::of(&registry)?,
            registry,
            codes,
            events: EventRoute::NoHandlerChild,
        })
    }

    /// Telemetry that records the metrics of `registry` and offers each
    /// event of a type in `sent` to a queue of at most `capacity` events.
    /// The writer of the handler child reads the queue that this returns.
    ///
    /// Errors: as [`TelemetryOn::metrics_only`], and when `registry` does
    /// not hold `horsies.telemetry.events.dropped`. Nothing is changed.
    pub fn with_event_queue(
        registry: Arc<Registry>,
        codes: DeclaredCodesByTask,
        capacity: NonZeroUsize,
        sent: SentEventTypes,
    ) -> Result<(Self, EventQueue), HandleRefused> {
        let dropped = registry.counter(MetricId::EventsDropped)?;
        let metrics = PointMetrics::of(&registry)?;
        let (link, queue) = event_link(capacity, dropped, sent);
        let on = Self {
            registry,
            codes,
            metrics,
            events: EventRoute::HandlerChildLink(link),
        };
        Ok((on, queue))
    }

    /// The registry that the points record in.
    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    /// Build the event of a point with `build` and offer it to the event
    /// queue, when a handler child is linked and takes events of the type
    /// `E`. Otherwise `build` is not called. Never waits.
    pub(crate) fn emit<E: UnionEvent>(&self, build: impl FnOnce() -> E) -> Emitted {
        match &self.events {
            EventRoute::NoHandlerChild => Emitted::NotBuilt,
            EventRoute::HandlerChildLink(link) => match link.sent.sends(E::NAME) {
                true => Emitted::Offered(link.offer(build().into())),
                false => Emitted::TypeNotSent,
            },
        }
    }
}

/// What an offer did with its event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Offer {
    /// The event is in the queue.
    Queued,
    /// The event was dropped, and the drop counted under `cause`.
    Dropped(EventDropCause),
}

/// The points' end of the event queue.
pub struct EventLink {
    sender: mpsc::Sender<Event>,
    dropped: Counter<(EventDropCause,)>,
    /// The event types that the points build and offer.
    sent: SentEventTypes,
}

impl EventLink {
    /// Put `event` in the queue, or drop it and count the drop: `queue_full`
    /// when the queue holds its capacity, `child_down` when its reader is
    /// gone. Never waits.
    pub fn offer(&self, event: Event) -> Offer {
        let cause = match self.sender.try_send(event) {
            Ok(()) => return Offer::Queued,
            Err(TrySendError::Full(_dropped)) => EventDropCause::QueueFull,
            Err(TrySendError::Closed(_dropped)) => EventDropCause::ChildDown,
        };
        let _recorded = self.dropped.add((cause,), 1);
        Offer::Dropped(cause)
    }
}

/// What one read of the event queue gives.
#[derive(Debug)]
pub enum EventBatch {
    /// One or more events, in the order of their offers.
    Events(Vec<Event>),
    /// The link is gone and the queue is empty: no event comes again.
    LinkClosed,
}

/// The reader's end of the event queue: the writer of the handler child.
pub struct EventQueue {
    receiver: mpsc::Receiver<Event>,
}

impl EventQueue {
    /// Wait for one or more events, and take at most `limit` of them, in the
    /// order of their offers. Cancel safe: a read dropped before it returns
    /// takes no event.
    pub async fn next_batch(&mut self, limit: NonZeroUsize) -> EventBatch {
        let mut events = Vec::new();
        match self.receiver.recv_many(&mut events, limit.get()).await {
            0 => EventBatch::LinkClosed,
            _taken => EventBatch::Events(events),
        }
    }

    /// Close the queue to new events: an offer after it drops its event and
    /// counts `child_down`. The events that wait stay, for
    /// [`EventQueue::take_waiting`].
    pub fn close(&mut self) {
        self.receiver.close();
    }

    /// Take the events that wait now, at most `limit`, in the order of
    /// their offers; none when the queue is empty or the link is gone.
    /// Never waits.
    pub fn take_waiting(&mut self, limit: NonZeroUsize) -> Vec<Event> {
        let mut events = Vec::new();
        while events.len() < limit.get() {
            match self.receiver.try_recv() {
                Ok(event) => events.push(event),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }
        events
    }
}

/// A queue of at most `capacity` events of the types `sent`: the link that
/// offers them, with the counter of its drops, and the queue that reads
/// them.
fn event_link(
    capacity: NonZeroUsize,
    dropped: Counter<(EventDropCause,)>,
    sent: SentEventTypes,
) -> (EventLink, EventQueue) {
    let (sender, receiver) = mpsc::channel(capacity.get());
    (
        EventLink {
            sender,
            dropped,
            sent,
        },
        EventQueue { receiver },
    )
}
