//! Child mode of a worker binary: the Rust child host.
//!
//! A Rust worker starts its own binary again with [`CHILD_MODE_ARGUMENT`],
//! the socket descriptor and the worker pid. The binary's entry checks
//! [`child_mode`] first; in child mode it calls [`run_child_host`] with its
//! task registry and exits with the code it returns. Code that `main` runs
//! before that call runs in each child too, as a Python child imports the
//! app again.
//!
//! The host speaks the same frames as the Python child and gives the same
//! guarantees: the start write and the finalizing marker through the core,
//! a result envelope only after the body stopped, the deadline reports of
//! each seat kind, the cancel signal of a run ([`run_cancel_signal`],
//! [`run_cancel_state`]) at the deadline of a shared run and at an operator
//! cancel (`cancel_run`) in every child kind, the reports of work a body
//! left ([`spawn_in_run`]), and the protocol's exit codes.
//!
//! A host program that runs task code outside the Rust runtime takes the
//! handle of a run in its async body ([`current_run`]) and registers the work
//! that it runs there as host-held work ([`RunHandle::register_host_work`]):
//! the cancel rounds ask such work to stop and do not drop it, and the run
//! keeps its seat until the host ends it. A host that runs that code on an
//! event loop of its own gives the child its [`HostLoop`]
//! ([`EventLoopWatch::Host`]): a block of that loop is reported with
//! `loop_blocked`, as a blocked poll of a Rust future is.
//!
//! A client that traces its task bodies gives the host a [`BodySpans`]: the
//! host runs each body inside the `tracing` span that it opens for the run
//! ([`BodyScope`]).
//!
//! The same binary serves the handler child of a worker: an init frame of
//! the `handler` kind boots a child that runs the app's event handlers
//! ([`EventHandlerSet`]) and no task. Each handler runs on a thread of its
//! own with a bounded queue; a full queue drops and counts, and a handler
//! that fails or panics is counted and logged with its name while the
//! child lives. The child sends `handler_stats` frames every
//! [`HANDLER_STATISTICS_INTERVAL`].

mod args;
mod body_scope;
mod ended_spans;
mod event_loop;
mod handle_setup;
mod handler_host;
mod handlers;
mod host;
mod link;
mod panic;
mod producer;
mod role;
mod rss;
mod run;
mod run_work;
mod seats;

#[cfg(test)]
mod cancel_tests;
#[cfg(test)]
mod ended_spans_tests;
#[cfg(test)]
mod handler_tests;
#[cfg(test)]
pub(crate) mod in_process;
#[cfg(test)]
mod start_node_tests;
#[cfg(test)]
mod start_tests;
#[cfg(test)]
mod stored_form_vector_tests;
#[cfg(test)]
mod tests;

pub use args::{child_mode, ChildArgs, ChildArgsError, ChildMode, CHILD_MODE_ARGUMENT};
pub use body_scope::{BodyCarrier, BodyOutcome, BodyRun, BodyScope, BodySpans};
pub use ended_spans::{
    EndedAttempt, EndedSpanMaker, EndedSpanScope, EndedWorkflow, NODE_READER_STOP_BOUND,
    NODE_READ_QUEUE_SPANS,
};
pub use event_loop::{EventLoopWatch, HostLoop, HOST_LOOP_TURN_TICK};
pub use handle_setup::{child_handle_setup, ChildHandleSetup, HandlerChildHasNoHandle};
pub use handlers::{
    EventHandlerRefused, EventHandlerSet, HandlerChildApp, HandlerError, HANDLER_DRAIN,
    HANDLER_ERROR_LOG_EVERY, HANDLER_LAST_STATS_BOUND, HANDLER_QUEUE_EVENTS,
    HANDLER_STATISTICS_INTERVAL,
};
pub use host::{
    child_command, run_child_host, ChildApp, ChildHookFailure, ChildHostExit, ChildLogSetup,
    ChildStartFacts, ChildStartFuture, ChildStartHook, NamedChildStartHook, CHILD_START_HOOK_BOUND,
};
pub use producer::{child_producer, ChildProducer};
pub use role::{process_role, thread_role, wait_inside_task_message, ProcessRole, ThreadRole};
pub use run_work::{
    current_run, run_cancel_signal, run_cancel_state, run_fencing_token, run_task_attempt,
    spawn_in_run, CancelCause, HostStopState, HostWork, HostWorkRefused, RunCancelSignal,
    RunCancelState, RunHandle, RunHandleRefused, RunSignalRefused, RunSpawnRefused, WorkStopCause,
    MAX_RUN_WORK,
};
