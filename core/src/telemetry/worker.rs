//! The telemetry of a worker parent: the metric handles that its lifecycle
//! points record, and the bounded queue of its events to the handler child.
//!
//! A worker holds one [`WorkerTelemetry`], chosen at start:
//! - [`WorkerTelemetry::Off`] (the default): a point is one branch. No event
//!   is built and no metric is recorded.
//! - [`WorkerTelemetry::On`] with [`EventRoute::NoHandlerChild`]: a point
//!   records its metrics; no event is built.
//! - [`WorkerTelemetry::On`] with [`EventRoute::HandlerChildLink`]: a point
//!   also builds its event and offers it to the event queue
//!   ([`EventLink::offer`]). An offer never waits: a full queue drops the
//!   event and counts `queue_full`, a closed queue drops it and counts
//!   `child_down` (`horsies.telemetry.events.dropped{cause}`).
//!
//! A point never fails and never changes what the worker does. A sample
//! past the series bound of its metric is counted by the registry; an event
//! that is not queued is counted here.

use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{TryRecvError, TrySendError};

use crate::core::task::Execution;
use crate::events::Event;
use crate::telemetry::catalog::MetricId;
use crate::telemetry::declared::{self, DeclaredValue};
use crate::telemetry::labels::{ClaimPassResult, EventDropCause, RecoveryStep, ReleaseReason};
use crate::telemetry::registry::{Counter, HandleRefused, Histogram, OptionalCounter, Registry};

mod points;
#[cfg(test)]
mod tests;

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

/// Where the events of the points go.
pub enum EventRoute {
    /// No handler child runs: no event is built.
    NoHandlerChild,
    /// The events go to the event queue, which the writer of the handler
    /// child reads.
    HandlerChildLink(EventLink),
}

/// The telemetry of a worker that records metrics.
pub struct TelemetryOn {
    registry: Arc<Registry>,
    metrics: PointMetrics,
    events: EventRoute,
}

/// The metric handles that the points of a worker record.
struct PointMetrics {
    queue_duration: Histogram<(DeclaredValue<declared::QueueName>, Execution)>,
    claims_released: Counter<(ReleaseReason,)>,
    claim_passes: Counter<(ClaimPassResult,)>,
    recovery_failures: Counter<(RecoveryStep,)>,
}

impl PointMetrics {
    /// The handles of `registry`.
    ///
    /// Errors: [`HandleRefused`] when `registry` does not hold a metric of
    /// the points with the labels of the catalog.
    fn of(registry: &Registry) -> Result<Self, HandleRefused> {
        Ok(Self {
            queue_duration: registry.histogram(MetricId::TaskQueueDuration)?,
            claims_released: registry.counter(MetricId::TaskClaimsReleased)?,
            claim_passes: registry.counter(MetricId::ClaimPasses)?,
            recovery_failures: registry.counter(MetricId::RecoveryPassFailures)?,
        })
    }
}

/// What a point did with its event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Emitted {
    /// No handler child is linked: the event was not built.
    NotBuilt,
    /// The event was built and offered to the event queue.
    Offered(Offer),
}

impl TelemetryOn {
    /// Telemetry that records the metrics of `registry` and builds no event.
    ///
    /// Errors: [`HandleRefused`] when `registry` does not hold a metric of
    /// the points with the labels of the catalog: a registry that
    /// [`Registry::for_worker`] did not build. Nothing is changed.
    pub fn metrics_only(registry: Arc<Registry>) -> Result<Self, HandleRefused> {
        Ok(Self {
            metrics: PointMetrics::of(&registry)?,
            registry,
            events: EventRoute::NoHandlerChild,
        })
    }

    /// Telemetry that records the metrics of `registry` and offers each
    /// event to a queue of at most `capacity` events. The writer of the
    /// handler child reads the queue that this returns.
    ///
    /// Errors: as [`TelemetryOn::metrics_only`], and when `registry` does
    /// not hold `horsies.telemetry.events.dropped`. Nothing is changed.
    pub fn with_event_queue(
        registry: Arc<Registry>,
        capacity: NonZeroUsize,
    ) -> Result<(Self, EventQueue), HandleRefused> {
        let dropped = registry.counter(MetricId::EventsDropped)?;
        let metrics = PointMetrics::of(&registry)?;
        let (link, queue) = event_link(capacity, dropped);
        let on = Self {
            registry,
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
    /// queue, when a handler child is linked. With no handler child, `build`
    /// is not called. Never waits.
    pub(crate) fn emit(&self, build: impl FnOnce() -> Event) -> Emitted {
        match &self.events {
            EventRoute::NoHandlerChild => Emitted::NotBuilt,
            EventRoute::HandlerChildLink(link) => Emitted::Offered(link.offer(build())),
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

/// A queue of at most `capacity` events: the link that offers them, with
/// the counter of its drops, and the queue that reads them.
fn event_link(
    capacity: NonZeroUsize,
    dropped: Counter<(EventDropCause,)>,
) -> (EventLink, EventQueue) {
    let (sender, receiver) = mpsc::channel(capacity.get());
    (EventLink { sender, dropped }, EventQueue { receiver })
}
