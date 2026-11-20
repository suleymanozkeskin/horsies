//! The event handlers of a Rust app, and their threads in a handler child.
//!
//! An app registers each handler with a name and the one event type that it
//! takes ([`EventHandlerSet`]): a handler never takes the union of the
//! event types, so a later event type changes no handler. The worker
//! manifest names each handler and its type ([`EventHandlerSet::manifest`]).
//!
//! A handler child runs each handler on a thread of its own, with a bounded
//! queue of the events of its type:
//!
//! - an async handler on a current-thread runtime that its thread drives,
//!   so a handler that blocks its thread blocks no other handler;
//! - a blocking handler on its thread.
//!
//! The child offers each event to the queue of each handler of its type,
//! with no wait. A full queue drops the event for that handler only and
//! counts it. A handler that returns an error or panics is counted and
//! logged with its name, and takes its next event: the child lives. The
//! counts go to the worker in `handler_stats` frames.

use std::fmt;
use std::io;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};

use super::host::ChildStartHook;
use super::panic::{catch_body_panic, CatchPanic, PanicRecord};
use crate::events::{Event, EventTypeName, UnionEvent};
use crate::worker::executor::protocol::{
    HandlerCounts, HandlerCountsList, HandlerName, MAX_EVENT_HANDLERS,
};
use crate::worker::executor::supervisor::CHILD_STOP_TIMEOUT;
use crate::worker_manifest::{EventHandlers, HandlerList, HandlerManifest, HandlerNameText};

/// The events that the queue of one handler holds. When it is full, the
/// next event of that handler drops and is counted.
pub const HANDLER_QUEUE_EVENTS: NonZeroUsize = match NonZeroUsize::new(10_000) {
    Some(events) => events,
    None => panic!("10,000 is not zero"),
};

/// A handler that fails is logged at its first failure, then once per this
/// many failures; the events that its full queue drops are logged the same
/// way. Every failure and every drop is counted in the `handler_stats`
/// frames.
pub const HANDLER_ERROR_LOG_EVERY: u64 = 1_000;

/// The interval of the `handler_stats` frames of a handler child. Each frame
/// holds the totals since the child started, so a lost frame loses no
/// count.
pub const HANDLER_STATS_INTERVAL: Duration = Duration::from_secs(5);

/// After the stop frame, the handlers get this long, together, to take the
/// events of their queues. The events still in a queue then are not
/// handled.
pub const HANDLER_DRAIN: Duration = Duration::from_secs(2);

/// After the drain, the last `handler_stats` frame gets this long to be
/// written.
pub const HANDLER_LAST_STATS_BOUND: Duration = Duration::from_secs(1);

// The stop of a handler child ends before the worker kills it.
const _: () = assert!(
    HANDLER_DRAIN.as_secs() + HANDLER_LAST_STATS_BOUND.as_secs() < CHILD_STOP_TIMEOUT.as_secs()
);

/// The name of the thread of each handler.
const HANDLER_THREAD_NAME: &str = "horsies-event-handler";

/// Why an event handler failed for one event: the text that the handler
/// child logs with the handler's name. `?` converts any
/// [`std::error::Error`] into it; [`HandlerError::new`] takes any other
/// text. The child counts the failure and gives the handler its next
/// event.
///
/// It is not a [`std::error::Error`] itself, so that the conversion from
/// every error does not overlap the conversion of a value into itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerError {
    cause: String,
}

impl HandlerError {
    /// The failure with the text of `cause`.
    pub fn new(cause: impl fmt::Display) -> Self {
        Self {
            cause: cause.to_string(),
        }
    }

    /// The text that the child logs.
    pub fn cause(&self) -> &str {
        &self.cause
    }
}

impl fmt::Display for HandlerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.cause)
    }
}

impl<E: std::error::Error> From<E> for HandlerError {
    fn from(error: E) -> Self {
        Self::new(error)
    }
}

/// Why an app does not take an event handler. Nothing is registered.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EventHandlerRefused {
    /// The name is empty, longer than 255 characters, or `(unknown)`: the
    /// value of the `handler` metric label for a name outside the declared
    /// handlers.
    #[error("the event handler name is refused: {cause}")]
    InvalidName { cause: String },
    /// An earlier handler has the name. The name is the value of the
    /// `handler` label of the handler's metrics, so each name is once.
    #[error("an event handler named '{name}' is registered already; each handler name is once")]
    RepeatedName { name: String },
    /// The app has [`MAX_EVENT_HANDLERS`] handlers already.
    #[error(
        "the app has {MAX_EVENT_HANDLERS} event handlers, the most that it takes; '{name}' is not \
         registered"
    )]
    TooMany { name: String },
}

/// The start of the thread of one handler, with a queue of its event type.
type HandlerStart = Box<dyn FnOnce(HandlerThreadPlan) -> io::Result<RunningHandler> + Send + Sync>;

/// One registered event handler: its name, the event type that it takes,
/// and the start of its thread.
struct EventHandler {
    name: HandlerNameText,
    event_type: EventTypeName,
    start: HandlerStart,
}

/// The event handlers of an app, in registration order: at most
/// [`MAX_EVENT_HANDLERS`], each name once. A handler child runs them.
pub struct EventHandlerSet {
    handlers: Vec<EventHandler>,
}

impl Default for EventHandlerSet {
    fn default() -> Self {
        Self::new()
    }
}

impl EventHandlerSet {
    /// A set with no handler.
    pub fn new() -> Self {
        Self {
            handlers: Vec::new(),
        }
    }

    /// Register `handler` as `name`: an async handler of the events of type
    /// `E`. In a handler child it runs on a thread of its own, on a
    /// current-thread runtime of that thread, one event at a time.
    ///
    /// Errors: [`EventHandlerRefused`]. After a refusal nothing is
    /// registered.
    pub fn add_async<E>(
        &mut self,
        name: String,
        handler: impl AsyncFn(E) -> Result<(), HandlerError> + Send + Sync + 'static,
    ) -> Result<(), EventHandlerRefused>
    where
        E: UnionEvent + Clone + Send + 'static,
    {
        let name = self.free_name(name)?;
        self.handlers.push(EventHandler {
            name,
            event_type: E::NAME,
            start: Box::new(move |plan: HandlerThreadPlan| plan.start_async::<E, _>(handler)),
        });
        Ok(())
    }

    /// Register `handler` as `name`: a blocking handler of the events of
    /// type `E`. In a handler child it runs on a thread of its own, one
    /// event at a time.
    ///
    /// Errors: [`EventHandlerRefused`]. After a refusal nothing is
    /// registered.
    pub fn add_blocking<E>(
        &mut self,
        name: String,
        handler: impl Fn(E) -> Result<(), HandlerError> + Send + Sync + 'static,
    ) -> Result<(), EventHandlerRefused>
    where
        E: UnionEvent + Clone + Send + 'static,
    {
        let name = self.free_name(name)?;
        self.handlers.push(EventHandler {
            name,
            event_type: E::NAME,
            start: Box::new(move |plan: HandlerThreadPlan| plan.start_blocking::<E, _>(handler)),
        });
        Ok(())
    }

    /// `name` as the name of a new handler of this set. Pure.
    ///
    /// Errors: [`EventHandlerRefused`].
    fn free_name(&self, name: String) -> Result<HandlerNameText, EventHandlerRefused> {
        let name = HandlerNameText::try_from(name)
            .map_err(|cause| EventHandlerRefused::InvalidName { cause })?;
        let repeated = self.handlers.iter().any(|handler| handler.name == name);
        let full = self.handlers.len() >= MAX_EVENT_HANDLERS;
        match (repeated, full) {
            (true, _) => Err(EventHandlerRefused::RepeatedName { name: name.into() }),
            (false, true) => Err(EventHandlerRefused::TooMany { name: name.into() }),
            (false, false) => Ok(name),
        }
    }

    /// The number of handlers.
    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    /// Whether the set has no handler.
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    /// The name and the event type of each handler, in registration order.
    pub fn names(&self) -> impl ExactSizeIterator<Item = (&str, EventTypeName)> + '_ {
        self.handlers
            .iter()
            .map(|handler| (handler.name.as_str(), handler.event_type))
    }

    /// The `handlers` field of the worker manifest: `NO_HANDLERS` for a set
    /// with no handler, else the name and the event type of each handler in
    /// registration order. Pure.
    pub fn manifest(&self) -> EventHandlers {
        match self.handlers.is_empty() {
            true => EventHandlers::NoHandlers,
            false => {
                let handlers: Vec<HandlerManifest> = self
                    .handlers
                    .iter()
                    .map(|handler| HandlerManifest {
                        name: handler.name.clone(),
                        event_type: handler.event_type,
                    })
                    .collect();
                let handlers = HandlerList::try_from(handlers).unwrap_or_else(|refused| {
                    panic!(
                        "a handler set holds 1 to MAX_EVENT_HANDLERS handlers, each name once: \
                         {refused}"
                    )
                });
                EventHandlers::Handlers { handlers }
            }
        }
    }

    /// Start the thread of each handler, with a queue of `capacity` events.
    ///
    /// Errors: [`HandlerStartFailed`] for the first thread or runtime that
    /// did not start. The threads that started end when their queues are
    /// dropped with the error.
    pub(super) fn start(
        self,
        capacity: NonZeroUsize,
    ) -> Result<RunningHandlers, HandlerStartFailed> {
        let mut running = Vec::with_capacity(self.handlers.len());
        for handler in self.handlers {
            let EventHandler {
                name,
                event_type: _,
                start: start_thread,
            } = handler;
            let plan = HandlerThreadPlan {
                tally: Arc::new(HandlerTally::new(name.clone())),
                capacity,
            };
            match start_thread(plan) {
                Ok(started) => running.push(started),
                Err(error) => {
                    return Err(HandlerStartFailed {
                        name: name.into(),
                        error,
                    })
                }
            }
        }
        let tallies = HandlerTallies(
            running
                .iter()
                .map(|handler| Arc::clone(&handler.tally))
                .collect(),
        );
        Ok(RunningHandlers {
            handlers: running,
            tallies,
        })
    }
}

/// What a handler child of the app runs: the event handlers, and the start
/// hooks of the handler child (Python `on_handler_child_start`). A task
/// child reads neither.
pub struct HandlerChildApp {
    pub handlers: EventHandlerSet,
    /// The start hooks of the handler child. They run once in each handler
    /// child, on its runtime, before its handlers start and before it
    /// reports ready; in registration order, each for at most
    /// [`super::CHILD_START_HOOK_BOUND`]. A hook that returns an error,
    /// panics or runs longer ends the child with exit code 173: the worker
    /// keeps the handler child down until it restarts, and runs its tasks.
    pub start_hook: ChildStartHook,
}

impl HandlerChildApp {
    /// No event handler and no start hook of the handler child.
    pub fn without_handlers() -> Self {
        Self {
            handlers: EventHandlerSet::new(),
            start_hook: ChildStartHook::NoHook,
        }
    }
}

/// The thread of a handler did not start.
#[derive(Debug, thiserror::Error)]
#[error("the thread of event handler '{name}' did not start: {error}")]
pub(super) struct HandlerStartFailed {
    pub(super) name: String,
    pub(super) error: io::Error,
}

/// What the thread of one handler starts with.
pub(super) struct HandlerThreadPlan {
    tally: Arc<HandlerTally>,
    capacity: NonZeroUsize,
}

impl HandlerThreadPlan {
    /// Start the thread of the async handler `handler`: a current-thread
    /// runtime that the thread drives, and the queue of the events of `E`.
    ///
    /// Errors: the `io::Error` of the runtime or the thread that did not
    /// start.
    fn start_async<E, H>(self, handler: H) -> io::Result<RunningHandler>
    where
        E: UnionEvent + Clone + Send + 'static,
        H: AsyncFn(E) -> Result<(), HandlerError> + Send + 'static,
    {
        let (inlet, mut queue) = mpsc::channel::<E>(self.capacity.get());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let (finished, finish) = oneshot::channel::<()>();
        let tally = Arc::clone(&self.tally);
        let _thread = thread::Builder::new()
            .name(HANDLER_THREAD_NAME.to_owned())
            .spawn(move || {
                runtime.block_on(async {
                    while let Some(event) = queue.recv().await {
                        let outcome = CatchPanic::new(async { handler(event).await }).await;
                        tally.record(outcome);
                    }
                });
                // The stop may have stopped waiting for the mark.
                let _marked = finished.send(());
                // Work that the handler spawned and left does not hold the
                // thread.
                runtime.shutdown_background();
            })?;
        Ok(RunningHandler {
            inlet: Box::new(inlet),
            tally: self.tally,
            finish,
        })
    }

    /// Start the thread of the blocking handler `handler`, with the queue of
    /// the events of `E`.
    ///
    /// Errors: the `io::Error` of the thread that did not start.
    fn start_blocking<E, H>(self, handler: H) -> io::Result<RunningHandler>
    where
        E: UnionEvent + Clone + Send + 'static,
        H: Fn(E) -> Result<(), HandlerError> + Send + 'static,
    {
        let (inlet, mut queue) = mpsc::channel::<E>(self.capacity.get());
        let (finished, finish) = oneshot::channel::<()>();
        let tally = Arc::clone(&self.tally);
        let _thread = thread::Builder::new()
            .name(HANDLER_THREAD_NAME.to_owned())
            .spawn(move || {
                while let Some(event) = queue.blocking_recv() {
                    let outcome = catch_body_panic(|| handler(event));
                    tally.record(outcome);
                }
                // The stop may have stopped waiting for the mark.
                let _marked = finished.send(());
            })?;
        Ok(RunningHandler {
            inlet: Box::new(inlet),
            tally: self.tally,
            finish,
        })
    }
}

/// What an offer of one event to one handler gave.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Offer {
    /// The event is in the queue of the handler.
    Queued,
    /// The event is of another type: the handler does not take it.
    OtherType,
    /// The queue of the handler is full: the event drops for this handler.
    QueueFull,
    /// The thread of the handler ended: the event drops for this handler.
    HandlerEnded,
}

/// The queue of one handler, as the dispatch sees it.
trait HandlerInlet: Send + Sync {
    /// Offer `event` with no wait: the handler takes it when it is of its
    /// type and its queue has room.
    fn offer(&self, event: &Event) -> Offer;
}

impl<E: UnionEvent + Clone + Send + 'static> HandlerInlet for mpsc::Sender<E> {
    fn offer(&self, event: &Event) -> Offer {
        let Some(record) = E::of_event(event) else {
            return Offer::OtherType;
        };
        match self.try_send(record.clone()) {
            Ok(()) => Offer::Queued,
            Err(TrySendError::Full(_dropped)) => Offer::QueueFull,
            Err(TrySendError::Closed(_dropped)) => Offer::HandlerEnded,
        }
    }
}

/// One handler whose thread runs.
struct RunningHandler {
    inlet: Box<dyn HandlerInlet>,
    tally: Arc<HandlerTally>,
    /// Answered when the thread took the last event of its closed queue.
    finish: oneshot::Receiver<()>,
}

/// Why one event dropped for one handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DropCause {
    QueueFull,
    HandlerEnded,
}

/// Why one call of a handler failed.
#[derive(Debug, Clone, PartialEq, Eq)]
enum HandlerFailure {
    Returned(HandlerError),
    /// The panic text.
    Panicked(String),
}

impl fmt::Display for HandlerFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Returned(error) => write!(formatter, "it returned an error: {error}"),
            Self::Panicked(text) => write!(formatter, "it panicked: {text}"),
        }
    }
}

/// The counts of one handler since its child started.
pub(super) struct HandlerTally {
    name: HandlerNameText,
    errors: AtomicU64,
    dropped: AtomicU64,
}

impl HandlerTally {
    fn new(name: HandlerNameText) -> Self {
        Self {
            name,
            errors: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        }
    }

    /// The name of the handler.
    pub(super) fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Record the outcome of one call of the handler: a returned error or a
    /// panic is counted, and logged by [`is_logged`].
    fn record(&self, outcome: Result<Result<(), HandlerError>, PanicRecord>) {
        let failure = match outcome {
            Ok(Ok(())) => return,
            Ok(Err(error)) => HandlerFailure::Returned(error),
            Err(panic) => HandlerFailure::Panicked(panic.text),
        };
        let count = self
            .errors
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        match is_logged(count) {
            true => log_failure(self.name(), count, &failure),
            false => {}
        }
    }

    /// Count one event that dropped for the handler under `cause`, and log
    /// it by [`is_logged`].
    fn dropped(&self, cause: DropCause) {
        let count = self
            .dropped
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        match is_logged(count) {
            true => log_drop(self.name(), count, cause),
            false => {}
        }
    }

    /// The counts of the handler for a `handler_stats` frame.
    fn counts(&self) -> HandlerCounts {
        let handler = HandlerName::new(self.name().to_owned()).unwrap_or_else(|over| {
            panic!("a handler name has at most MAX_HANDLER_NAME_CHARS characters: {over}")
        });
        HandlerCounts {
            handler,
            errors: self.errors.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
        }
    }
}

/// Whether the `count`-th failure or drop of a handler is logged: the
/// first, then each [`HANDLER_ERROR_LOG_EVERY`]-th. Pure.
pub(super) fn is_logged(count: u64) -> bool {
    count == 1 || count.is_multiple_of(HANDLER_ERROR_LOG_EVERY)
}

fn log_failure(name: &str, count: u64, failure: &HandlerFailure) {
    match count {
        1 => tracing::error!(
            handler = name,
            failures = count,
            "event handler '{name}' failed: {failure}; the handler child gives it its next event, \
             and logs one line per {HANDLER_ERROR_LOG_EVERY} failures of it",
        ),
        _ => tracing::error!(
            handler = name,
            failures = count,
            "event handler '{name}' failed {count} times; the last failure: {failure}",
        ),
    }
}

fn log_drop(name: &str, count: u64, cause: DropCause) {
    match cause {
        DropCause::QueueFull => tracing::warn!(
            handler = name,
            dropped = count,
            "the queue of event handler '{name}' is full: {count} events dropped for it so far",
        ),
        DropCause::HandlerEnded => tracing::error!(
            handler = name,
            dropped = count,
            "the thread of event handler '{name}' ended: {count} events dropped for it so far",
        ),
    }
}

/// The counts of each running handler, in registration order. Clones share
/// the counts.
#[derive(Clone)]
pub(super) struct HandlerTallies(Arc<[Arc<HandlerTally>]>);

impl HandlerTallies {
    /// The counts of each handler for a `handler_stats` frame, in
    /// registration order.
    pub(super) fn counts(&self) -> HandlerCountsList {
        let counts: Vec<HandlerCounts> = self.0.iter().map(|tally| tally.counts()).collect();
        HandlerCountsList::new(counts).unwrap_or_else(|invalid| {
            panic!(
                "a handler set holds at most MAX_EVENT_HANDLERS handlers, each name once and not \
                 empty: {invalid}"
            )
        })
    }
}

/// How the stop of the handlers ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Drained {
    /// Every handler took the events of its queue.
    AllFinished,
    /// These handlers did not take the events of their queues within the
    /// drain bound, or their threads ended before; the events left are not
    /// handled.
    Unfinished(Vec<String>),
}

/// The handlers of a handler child, each on its thread.
pub(super) struct RunningHandlers {
    handlers: Vec<RunningHandler>,
    tallies: HandlerTallies,
}

impl RunningHandlers {
    /// Offer `event` to each handler, with no wait; count the drops.
    pub(super) fn offer(&self, event: &Event) {
        for handler in &self.handlers {
            match handler.inlet.offer(event) {
                Offer::Queued | Offer::OtherType => {}
                Offer::QueueFull => handler.tally.dropped(DropCause::QueueFull),
                Offer::HandlerEnded => handler.tally.dropped(DropCause::HandlerEnded),
            }
        }
    }

    /// The counts of the handlers; they stay readable after the stop.
    pub(super) fn tallies(&self) -> HandlerTallies {
        self.tallies.clone()
    }

    /// Close every queue, then wait until each handler took the events of
    /// its queue, at most `drain` in all. Each handler that did not finish
    /// is logged with its name.
    pub(super) async fn stop(self, drain: Duration) -> Drained {
        let until = tokio::time::Instant::now() + drain;
        let mut waits = Vec::with_capacity(self.handlers.len());
        for handler in self.handlers {
            let RunningHandler {
                inlet,
                tally,
                finish,
            } = handler;
            // The thread takes the events left, then sees the closed queue.
            drop(inlet);
            waits.push((tally, finish));
        }
        let mut unfinished = Vec::new();
        for (tally, finish) in waits {
            match tokio::time::timeout_at(until, finish).await {
                Ok(Ok(())) => {}
                Ok(Err(_thread_ended)) => {
                    tracing::error!(
                        handler = tally.name(),
                        "the thread of event handler '{}' ended before it took the events of its \
                         queue",
                        tally.name(),
                    );
                    unfinished.push(tally.name().to_owned());
                }
                Err(_elapsed) => {
                    tracing::warn!(
                        handler = tally.name(),
                        "event handler '{}' did not take the events of its queue within {:.1}s \
                         of the stop; they are not handled",
                        tally.name(),
                        drain.as_secs_f64(),
                    );
                    unfinished.push(tally.name().to_owned());
                }
            }
        }
        match unfinished.is_empty() {
            true => Drained::AllFinished,
            false => Drained::Unfinished(unfinished),
        }
    }
}
