//! The lifecycle events of the worker, and the app's handlers of them
//! (Python `horsies.telemetry` and `@app.on_event`).
//!
//! The worker emits one event at each step of a task and of its children:
//! [`TaskClaimed`], [`ClaimReleased`], [`AttemptPlaced`],
//! [`AttemptNotStarted`], [`AttemptEnded`], [`AttemptRecorded`],
//! [`TaskRecovered`], [`ChildStarted`], [`ChildReplaced`], [`SlotDown`] and
//! [`SlotRecovered`]. The types are the core's: one definition for both
//! clients.
//!
//! - [`Horsies::on_event`] registers an async handler of one event type,
//!   [`Horsies::on_event_blocking`] a blocking one. A handler takes one
//!   type, never all events, so a later event type changes no handler.
//! - The worker starts one handler child when the app has a handler: this
//!   binary in child mode, as for the task children. The handlers run there
//!   and never in the worker process, so a handler that fails, panics or
//!   blocks never touches a task or a claim.
//! - [`Horsies::on_handler_child_start`] runs code once in each handler
//!   child before its handlers start.
//! - Delivery is best effort: at most once. A full queue of a handler drops
//!   the event for that handler and counts it in
//!   `horsies.telemetry.handler.dropped`; a failure or a panic counts in
//!   `horsies.telemetry.handler.errors`. The history tables are the record
//!   of each task.

use horsies_core::EventHandlerRefused;

pub use horsies_core::events::{
    AttemptEnd, AttemptEnded, AttemptErrorCode, AttemptNotStarted, AttemptNumber, AttemptPlaced,
    AttemptRecord, AttemptRecorded, AttemptStart, ChildReplaced, ChildResult, ChildStarted,
    ClaimReleased, OperatorCancelEnd, Priority, QueueName, RecoveryAction, ReleaseReason,
    ReplaceReason, RetryCount, RunTrace, ShortageErrno, SlotDown, SlotDownCause, SlotIndex,
    SlotRecovered, StopReason, TaskClaimed, TaskName, TaskRecovered, TraceCarrierText, UnionEvent,
    UserCodeText, WorkerDecision, WorkerIdText, WorkflowPlace,
};
pub use horsies_core::{
    HandlerError, HANDLER_DRAIN, HANDLER_ERROR_LOG_EVERY, HANDLER_QUEUE_EVENTS,
    HANDLER_STATS_INTERVAL,
};

use crate::child::named_start_hook;
use crate::{Horsies, HorsiesError};

impl Horsies {
    /// Run `handler` on each event of type `E` in the worker's handler
    /// child (Python `@app.on_event(E)` with an `async def`).
    ///
    /// ```ignore
    /// use horsies::events::{AttemptEnded, HandlerError};
    ///
    /// async fn record_end(event: AttemptEnded) -> Result<(), HandlerError> {
    ///     tracing::info!(task_id = %event.task_id, "attempt ended");
    ///     Ok(())
    /// }
    ///
    /// app.on_event::<AttemptEnded>("audit.attempt_ended", record_end)?;
    /// ```
    ///
    /// - `name` names the handler in the logs and is the `handler` label
    ///   of its metrics: 1 to 255 characters, not `(unknown)`, once per app.
    /// - The handler runs on a thread of its own, on a current-thread
    ///   runtime of that thread, one event at a time, in the order of the
    ///   worker. A handler that blocks its thread delays only its own
    ///   events.
    /// - Its queue holds [`HANDLER_QUEUE_EVENTS`] events; when it is full,
    ///   the next event drops for this handler and is counted.
    /// - A returned [`HandlerError`] or a panic is counted and logged with
    ///   `name` (the first, then one per [`HANDLER_ERROR_LOG_EVERY`]), and
    ///   the handler takes its next event.
    /// - A handler may send tasks: the child has a producer pool of one
    ///   connection.
    ///
    /// Register every handler in `main` before the hand-over to horsies:
    /// the worker reads the handlers from the app, and its handler child
    /// builds the same app.
    ///
    /// Errors: a [`HorsiesError`] with the code of the same refusal of the
    /// Python registration: `HRS-225`
    /// ([`ErrorCode::EventHandlerInvalidName`](crate::ErrorCode::EventHandlerInvalidName))
    /// for a name that is empty, longer than 255 characters or `(unknown)`,
    /// `HRS-224`
    /// ([`ErrorCode::EventHandlerDuplicateName`](crate::ErrorCode::EventHandlerDuplicateName))
    /// for a name that another handler has, and `HRS-226`
    /// ([`ErrorCode::EventHandlerLimit`](crate::ErrorCode::EventHandlerLimit))
    /// for a 33rd handler. Nothing is registered then.
    pub fn on_event<E>(
        &mut self,
        name: impl Into<String>,
        handler: impl AsyncFn(E) -> Result<(), HandlerError> + Send + Sync + 'static,
    ) -> Result<(), HorsiesError>
    where
        E: UnionEvent + Clone + Send + 'static,
    {
        self.event_handlers_mut()
            .add_async::<E>(name.into(), handler)
            .map_err(|refused| handler_refusal(&refused))
    }

    /// Run the blocking `handler` on each event of type `E` in the worker's
    /// handler child (Python `@app.on_event(E)` with a `def`). It runs on a
    /// thread of its own, one event at a time; every other rule is that of
    /// [`Self::on_event`].
    ///
    /// Errors: as [`Self::on_event`].
    pub fn on_event_blocking<E>(
        &mut self,
        name: impl Into<String>,
        handler: impl Fn(E) -> Result<(), HandlerError> + Send + Sync + 'static,
    ) -> Result<(), HorsiesError>
    where
        E: UnionEvent + Clone + Send + 'static,
    {
        self.event_handlers_mut()
            .add_blocking::<E>(name.into(), handler)
            .map_err(|refused| handler_refusal(&refused))
    }

    /// Run `hook` once in each handler child, a replacement child included
    /// (Python `on_handler_child_start`): set up what the handlers use, for
    /// example a tracer provider.
    ///
    /// - It runs on the child's runtime before the handlers start and
    ///   before the child reports ready; hooks run in registration order,
    ///   each for at most [`crate::CHILD_START_HOOK_BOUND`] (10 s).
    /// - A hook that returns an error, panics or runs past its bound ends
    ///   the handler child with exit code 173. The worker keeps the handler
    ///   child down until it restarts, and runs its tasks: a handler child
    ///   never stops the worker. The logged cause names the hook.
    ///
    /// Only the handler child runs these hooks; a task child runs those of
    /// [`Self::on_child_start`].
    pub fn on_handler_child_start<F, Fut, E>(&mut self, hook: F)
    where
        F: FnOnce() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), E>> + Send + 'static,
        E: std::fmt::Display,
    {
        self.add_handler_child_start_hook(named_start_hook(hook));
    }
}

/// The error of a refused handler registration, with the code of the same
/// refusal of the Python registration ([`EventHandlerRefused::code`]).
/// Pure.
fn handler_refusal(refused: &EventHandlerRefused) -> HorsiesError {
    let help = match refused {
        EventHandlerRefused::InvalidName { .. } => {
            "name the handler with 1 to 255 characters, other than (unknown)"
        }
        EventHandlerRefused::RepeatedName { .. } => "give each event handler a name of its own",
        EventHandlerRefused::TooMany { .. } => {
            "handle several event types in one handler of each type"
        }
    };
    HorsiesError::new(refused.to_string())
        .with_code(refused.code())
        .with_help(help)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use horsies_core::worker_manifest::EventHandlers;
    use horsies_core::ChildStartHook;

    use super::*;
    use crate::{AppConfig, ErrorCode};

    fn app() -> Horsies {
        Horsies::new(AppConfig::for_database_url(
            "postgresql://localhost/horsies",
        ))
        .unwrap()
    }

    async fn record_end(_event: AttemptEnded) -> Result<(), HandlerError> {
        Ok(())
    }

    fn record_claim(_event: TaskClaimed) -> Result<(), HandlerError> {
        Ok(())
    }

    #[test]
    fn a_registered_handler_is_in_the_worker_manifest_with_its_event_type() {
        let mut app = app();
        assert_eq!(app.event_handlers().manifest(), EventHandlers::NoHandlers);
        app.on_event::<AttemptEnded>("audit.attempt_ended", record_end)
            .unwrap();
        app.on_event_blocking::<TaskClaimed>("audit.task_claimed", record_claim)
            .unwrap();
        assert_eq!(
            serde_json::to_value(app.event_handlers().manifest()).unwrap(),
            serde_json::json!({
                "kind": "HANDLERS",
                "handlers": [
                    {"name": "audit.attempt_ended", "event_type": "AttemptEnded"},
                    {"name": "audit.task_claimed", "event_type": "TaskClaimed"}
                ]
            })
        );
    }

    /// A repeated name is `HRS-224` and an invalid name `HRS-225`, the codes
    /// of the same refusals of the Python registration; nothing is
    /// registered.
    #[test]
    fn a_repeated_or_invalid_name_is_refused_and_nothing_is_registered() {
        let mut app = app();
        app.on_event::<AttemptEnded>("audit", record_end).unwrap();
        let repeated = app
            .on_event_blocking::<TaskClaimed>("audit", record_claim)
            .unwrap_err();
        assert!(
            repeated.message.contains("'audit' is registered already"),
            "{repeated}"
        );
        assert_eq!(repeated.code, Some(ErrorCode::EventHandlerDuplicateName));
        assert!(
            repeated.to_string().starts_with("error[HRS-224]: "),
            "{repeated}"
        );
        assert!(repeated.help_text.is_some());
        for name in ["", "(unknown)"] {
            let refused = app.on_event::<AttemptEnded>(name, record_end).unwrap_err();
            assert!(refused.message.contains("name is refused"), "{refused}");
            assert_eq!(refused.code, Some(ErrorCode::EventHandlerInvalidName));
            assert!(
                refused.to_string().starts_with("error[HRS-225]: "),
                "{refused}"
            );
        }
        assert_eq!(app.event_handlers().len(), 1);
    }

    /// A 33rd handler is `HRS-226`, the code of the Python refusal of one
    /// handler too many; nothing is registered.
    #[test]
    fn a_handler_past_the_most_is_refused_with_the_limit_code() {
        let mut app = app();
        for index in 0..horsies_core::frame_protocol::MAX_EVENT_HANDLERS {
            app.on_event_blocking::<TaskClaimed>(format!("audit.{index}"), record_claim)
                .unwrap();
        }
        let refused = app
            .on_event::<AttemptEnded>("one.more", record_end)
            .unwrap_err();
        assert_eq!(refused.code, Some(ErrorCode::EventHandlerLimit));
        assert!(
            refused.to_string().starts_with("error[HRS-226]: "),
            "{refused}"
        );
        assert!(refused.is_config_error());
        assert_eq!(
            app.event_handlers().len(),
            horsies_core::frame_protocol::MAX_EVENT_HANDLERS
        );
    }

    #[test]
    fn a_closure_handler_of_each_kind_registers() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut app = app();
        let async_seen = Arc::clone(&seen);
        app.on_event::<SlotRecovered>(
            "slots.async",
            async move |event: SlotRecovered| -> Result<(), HandlerError> {
                async_seen.lock().unwrap().push(event.slot.get());
                Ok(())
            },
        )
        .unwrap();
        let blocking_seen = Arc::clone(&seen);
        app.on_event_blocking::<SlotRecovered>("slots.blocking", move |event| {
            blocking_seen.lock().unwrap().push(event.slot.get());
            Ok(())
        })
        .unwrap();
        let names: Vec<&str> = app
            .event_handlers()
            .names()
            .map(|(name, _event_type)| name)
            .collect();
        assert_eq!(names, vec!["slots.async", "slots.blocking"]);
    }

    async fn tracer_hook() -> Result<(), &'static str> {
        Err("the tracer provider did not build")
    }

    #[tokio::test]
    async fn on_handler_child_start_keeps_the_hook_for_the_handler_child_only() {
        let mut app = app();
        app.on_handler_child_start(tracer_hook);
        app.on_child_start(|| async { Ok::<(), std::convert::Infallible>(()) });
        let parts = app.into_child_parts();
        assert_eq!(parts.start_hooks.len(), 1);
        let mut hooks = match crate::child::start_hook_of(parts.handler_child_start_hooks) {
            ChildStartHook::NoHook => panic!("the handler child has one hook"),
            ChildStartHook::Run(hooks) => hooks.into_iter(),
        };
        let hook = hooks.next().unwrap();
        assert!(hook.name.ends_with("::tracer_hook"), "{}", hook.name);
        let failure = (hook.start)().await.unwrap_err();
        assert_eq!(failure.cause, "the tracer provider did not build");
        assert!(hooks.next().is_none());
    }
}
