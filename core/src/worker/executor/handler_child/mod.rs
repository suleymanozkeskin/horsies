//! The handler child of a worker: one child process that runs the event
//! handlers of the app, and makes the spans of the attempts that the worker
//! ended. The worker sends it the lifecycle events of its points in
//! `events` frames; it sends `handler_stats` frames back.
//!
//! It is not a slot: the slot board, the claim budgets and the pool failure
//! rule do not see it. Its failures never stop the worker and never touch a
//! task child or the dispatch:
//!
//! - A boot failure of a cause that repeats (an import error, a failed or
//!   hung start hook, no ready frame in time, another protocol version)
//!   leaves the child down until the worker restarts. A cause that can
//!   clear boots again after the slot backoff ([`slot_down_delay`]).
//! - A child lost after its ready frame boots again after the slot backoff
//!   of the losses in a row.
//! - While no child is up, the events drop and count `child_down`.
//!
//! - [`state`]: the state of the child, and its form in the worker
//!   snapshot.
//! - `supervise`: the boot and the restart rules.
//! - `serve`: a ready child: the writer of the event queue, the
//!   `handler_stats` frames and the stop.
//!
//! [`slot_down_delay`]: crate::worker::executor::supervisor::slot_down_delay

use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::telemetry::catalog::MetricId;
use crate::telemetry::declared::{DeclaredNames, DeclaredValue, HandlerName};
use crate::telemetry::labels::{EventDropCause, MemoryProcess};
use crate::telemetry::registry::{Counter, Gauge, HandleRefused, Registry};
use crate::telemetry::worker::{EventQueue, EVENT_QUEUE_CAPACITY};
use crate::worker::executor::placement::ChildCpus;
use crate::worker::executor::protocol::{InitFrame, RssReading, MAX_EVENTS_PER_FRAME};
use crate::worker::executor::supervisor::boot::SLOT_DOWN_MAX_DELAY;
use crate::worker::executor::supervisor::{
    ChildCommand, ChildSource, SpawnFuture, SpawnerHandle, CHILD_STOP_TIMEOUT,
};

mod serve;
pub mod state;
mod supervise;

#[cfg(test)]
mod tests;

pub use state::{
    telemetry_json, AfterDown, HandlerChildDown, HandlerChildState, HandlerChildView,
    HandlerDownCause,
};

/// The longest wait for the ready frame of one boot of the handler child.
/// The worker waits at most this long for its first ready frame before the
/// first claim, then claims in every case.
pub const HANDLER_READY_TIMEOUT: Duration = Duration::from_secs(30);

/// The most events of one `events` frame that the writer builds: the bound
/// of the frame.
pub const EVENT_BATCH_MAX: NonZeroUsize = match NonZeroUsize::new(MAX_EVENTS_PER_FRAME) {
    Some(events) => events,
    None => panic!("an events frame carries one event or more"),
};

/// After the first event of a batch, the writer waits this long for more
/// events before it writes the frame.
pub const EVENT_FLUSH_INTERVAL: Duration = Duration::from_millis(25);

/// At the stop, the writer gets this long to write the events of the queue
/// and the stop frame. A child that does not take them in time is killed.
pub const HANDLER_STOP_FLUSH: Duration = Duration::from_secs(2);

/// The frames that a handler child may write ahead of the worker: its
/// `handler_stats` frames (one per few seconds) while the write of an
/// `events` frame waits for the socket. A child past it breaks the
/// protocol and is replaced.
pub const HANDLER_MAILBOX_FRAMES: NonZeroUsize = match NonZeroUsize::new(64) {
    Some(frames) => frames,
    None => panic!("64 is not zero"),
};

/// A child that served this long starts the count of its losses in a row
/// again, so its next loss boots again after the first backoff.
pub const HANDLER_CHILD_STABLE: Duration = SLOT_DOWN_MAX_DELAY;

/// The process name of the handler child.
pub const HANDLER_CHILD_PROCESS_NAME: &str = "horsies-child-handler";

/// The most passes of the writer over the queue at the stop: the passes
/// that a full queue takes in batches of [`EVENT_BATCH_MAX`], and one.
const STOP_FLUSH_PASSES: usize = EVENT_QUEUE_CAPACITY.get() / EVENT_BATCH_MAX.get() + 1;

/// The bounds of the supervision of a handler child.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandlerChildBounds {
    /// The longest wait for the ready frame of one boot.
    pub ready: Duration,
    /// After the first event of a batch, the wait for more events.
    pub flush_interval: Duration,
    /// At the stop: the longest write of the queue and the stop frame.
    pub stop_flush: Duration,
    /// After the stop frame: the longest wait for the exit before a kill.
    pub stop_exit: Duration,
}

impl HandlerChildBounds {
    /// The bounds of a worker.
    pub const WORKER: Self = Self {
        ready: HANDLER_READY_TIMEOUT,
        flush_interval: EVENT_FLUSH_INTERVAL,
        stop_flush: HANDLER_STOP_FLUSH,
        stop_exit: CHILD_STOP_TIMEOUT,
    };
}

/// What the handler child of a worker runs.
#[derive(Debug, Clone)]
pub struct HandlerChildConfig {
    /// The program of the child: the child command of the worker. The
    /// spawner appends the socket descriptor and the worker pid.
    pub command: ChildCommand,
    /// The first frame of the child. Its kind is the handler kind.
    pub init: InitFrame,
    /// The CPUs of the child, set before the init frame.
    pub cpus: ChildCpus,
    /// The longest wait for the exit of the child after a `SIGKILL`.
    pub kill_wait: Duration,
    pub bounds: HandlerChildBounds,
}

/// The metric handles that the supervision of the handler child records.
#[derive(Clone)]
pub struct HandlerChildMetrics {
    /// `horsies.telemetry.events.dropped{cause}`.
    events_dropped: Counter<(EventDropCause,)>,
    /// `horsies.telemetry.handler.errors{handler}`.
    handler_errors: Counter<(DeclaredValue<HandlerName>,)>,
    /// `horsies.telemetry.handler.dropped{handler}`.
    handler_dropped: Counter<(DeclaredValue<HandlerName>,)>,
    /// `horsies.worker.memory.rss{process}`.
    rss: Gauge<(MemoryProcess,)>,
    /// The handler names that the registry declares.
    handlers: DeclaredNames<HandlerName>,
}

/// Bytes in one MiB, the unit of a child's RSS reading.
const BYTES_PER_MIB: f64 = 1_048_576.0;

impl HandlerChildMetrics {
    /// The handles of `registry`, a worker registry.
    ///
    /// Errors: [`HandleRefused`] when `registry` does not hold one of the
    /// metrics with the labels of the catalog. Nothing is changed.
    pub fn of(registry: &Registry) -> Result<Self, HandleRefused> {
        Ok(Self {
            events_dropped: registry.counter(MetricId::EventsDropped)?,
            handler_errors: registry.counter(MetricId::HandlerErrors)?,
            handler_dropped: registry.counter(MetricId::HandlerDropped)?,
            rss: registry.gauge(MetricId::MemoryRss)?,
            handlers: registry.labels().handlers.clone(),
        })
    }

    /// Count `events` events that did not reach the child under `cause`.
    fn dropped(&self, cause: EventDropCause, events: usize) {
        let count = u64::try_from(events).unwrap_or(u64::MAX);
        match count {
            0 => {}
            count => {
                let _recorded = self.events_dropped.add((cause,), count);
            }
        }
    }

    /// Set the RSS of the handler child; an unreadable reading leaves the
    /// last one.
    fn record_rss(&self, rss: RssReading) {
        match rss {
            RssReading::Mb(mib) => {
                let _recorded = self
                    .rss
                    .set((MemoryProcess::HandlerChild,), mib.get() * BYTES_PER_MIB);
            }
            RssReading::Unreadable => {}
        }
    }
}

/// Where the handler child comes from: a spawn by `exec`, never a fork from
/// the preload process, which holds the import of the task children.
struct ExecSpawns(SpawnerHandle);

impl ChildSource for ExecSpawns {
    fn spawn(&self, command: ChildCommand) -> SpawnFuture<'_> {
        Box::pin(self.0.spawn_exec(command))
    }
}

/// The stop of the supervision: the token that ends a boot, a wait or a
/// write, and the time until which the writer may still write.
#[derive(Clone)]
struct StopSignal {
    token: CancellationToken,
    flush_until: Arc<OnceLock<Instant>>,
}

impl StopSignal {
    fn new() -> Self {
        Self {
            token: CancellationToken::new(),
            flush_until: Arc::new(OnceLock::new()),
        }
    }

    /// Ask for the stop; the writer may write until `flush_until`. A second
    /// request keeps the first time.
    fn request(&self, flush_until: Instant) {
        let _first = self.flush_until.set(flush_until);
        self.token.cancel();
    }

    /// The end of the write at the stop. Read only after the token is
    /// cancelled: [`StopSignal::request`] sets the time before the cancel.
    fn flush_end(&self) -> Instant {
        match self.flush_until.get() {
            Some(until) => *until,
            None => panic!("the stop of the handler child was read before it was requested"),
        }
    }

    /// Wait until the stop is asked for and its write time has passed.
    async fn flush_passed(&self) {
        self.token.cancelled().await;
        tokio::time::sleep_until(self.flush_end()).await;
    }
}

/// A task that is aborted when its handle is dropped.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The handler child of a running worker. Dropping it aborts the
/// supervision: the channel of a child that runs then kills it.
pub struct HandlerChildRun {
    view: HandlerChildView,
    stop: StopSignal,
    bounds: HandlerChildBounds,
    kill_wait: Duration,
    task: AbortOnDrop,
}

/// How the stop of the handler child ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandlerChildStopped {
    /// The supervision ended within its stop bound.
    Ended,
    /// The supervision did not end within its stop bound and was aborted;
    /// the child that ran was killed.
    Aborted,
}

impl HandlerChildRun {
    /// Start the supervision of the handler child of `config` on the current
    /// runtime: the child boots at once, and the events of `queue` go to it.
    /// `spawner` starts the child by `exec`. Returns at once.
    pub fn start(
        config: HandlerChildConfig,
        queue: EventQueue,
        metrics: HandlerChildMetrics,
        spawner: SpawnerHandle,
    ) -> Self {
        let source: Arc<dyn ChildSource> = Arc::new(ExecSpawns(spawner.clone()));
        Self::start_from_source(config, queue, metrics, spawner, source)
    }

    /// As [`HandlerChildRun::start`], with the children of `source`.
    pub(crate) fn start_from_source(
        config: HandlerChildConfig,
        queue: EventQueue,
        metrics: HandlerChildMetrics,
        spawner: SpawnerHandle,
        source: Arc<dyn ChildSource>,
    ) -> Self {
        let (state, view) = watch::channel(HandlerChildState::Starting);
        let stop = StopSignal::new();
        let bounds = config.bounds;
        let kill_wait = config.kill_wait;
        let supervision = supervise::Supervision {
            config,
            source,
            spawner,
            metrics,
            state,
            stop: stop.clone(),
        };
        let task = tokio::spawn(supervise::supervise(supervision, queue));
        Self {
            view: HandlerChildView::of(view),
            stop,
            bounds,
            kill_wait,
            task: AbortOnDrop(task),
        }
    }

    /// A read view of the state of the child.
    pub fn view(&self) -> HandlerChildView {
        self.view.clone()
    }

    /// Wait for the end of the first boot, at most the ready bound: the
    /// worker claims after it in every case. Gives the state then.
    pub async fn first_boot(&self) -> HandlerChildState {
        self.view.settled(self.bounds.ready).await
    }

    /// Stop the child: the writer gets the stop flush bound to write the
    /// events of the queue and the stop frame, then the child gets the stop
    /// exit bound to exit, then it is killed. The events that are not
    /// written count `child_down`. Bounded: a supervision that does not end
    /// within these bounds and two kill waits is aborted.
    pub async fn stop(mut self) -> HandlerChildStopped {
        self.stop.request(Instant::now() + self.bounds.stop_flush);
        let bound = self.bounds.stop_flush + self.bounds.stop_exit + self.kill_wait * 2;
        match tokio::time::timeout(bound, &mut self.task.0).await {
            Ok(Ok(())) => HandlerChildStopped::Ended,
            Ok(Err(joined)) if joined.is_panic() => std::panic::resume_unwind(joined.into_panic()),
            Ok(Err(_cancelled)) => HandlerChildStopped::Aborted,
            Err(_elapsed) => {
                tracing::error!(
                    "the handler child did not stop within {:.1}s; its supervision is aborted",
                    bound.as_secs_f64()
                );
                HandlerChildStopped::Aborted
            }
        }
    }
}
