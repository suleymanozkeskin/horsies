---
title: Event Handlers
summary: Register typed handlers of the worker's lifecycle events with on_event and on_event_blocking; they run in the worker's handler child, each on its own thread with a bounded queue.
related: [../workers/worker-setup, ../cli, ../../workers/worker-architecture]
tags: [rust, events, on_event, on_event_blocking, on_handler_child_start, HandlerError, handler child]
---

# Event Handlers

The worker emits one event at each step of a task and of its children. A handler takes the events of one event type. The handlers run in the worker's handler child: the app's binary again in child mode, as for the task children. A handler never runs in the worker process, so a handler that fails, panics or blocks does not change a task outcome and does not delay a claim.

## Register a Handler

```rust
use horsies::events::{AttemptEnd, AttemptEnded, ChildResult, HandlerError, TaskClaimed};
use horsies::{Horsies, HorsiesError};

async fn record_attempt_end(event: AttemptEnded) -> Result<(), HandlerError> {
    let how = match &event.end {
        AttemptEnd::ReportedByChild { result: ChildResult::Completed, .. } => "completed".to_owned(),
        AttemptEnd::ReportedByChild { result: ChildResult::Cancelled { end }, .. } => {
            format!("cancelled ({end:?})")
        }
        AttemptEnd::ReportedByChild { .. } => "failed".to_owned(),
        AttemptEnd::DecidedByWorker { reason, .. } => format!("{reason:?}"),
        AttemptEnd::NotKnown { .. } => "not known".to_owned(),
    };
    println!(
        "task {} attempt {} {how} after {:?}",
        event.task_id,
        event.attempt.get(),
        event.run_duration
    );
    Ok(())
}

fn record_claim(event: TaskClaimed) -> Result<(), HandlerError> {
    println!("task {} waited {:?} in its queue", event.task_id, event.queue_wait);
    Ok(())
}

fn add_handlers(app: &mut Horsies) -> Result<(), HorsiesError> {
    app.on_event::<AttemptEnded>("audit.attempt_ended", record_attempt_end)?;
    app.on_event_blocking::<TaskClaimed>("audit.task_claimed", record_claim)?;
    Ok(())
}
```

Register every handler in `main`, before `horsies::cli::run(app)`: the worker reads the handlers from the app to start its handler child, and the handler child builds the same app.

- `on_event::<E>` takes an async handler: an `async fn`, an async closure, or a closure that returns a future. `on_event_blocking::<E>` takes a function that blocks.
- `E` is one event type of `horsies::events`. The handler takes the events of that type only, so a later event type changes no handler.
- `name` names the handler in the logs, and is the `handler` label of its metrics: 1 to 255 characters, not `(unknown)`, once per app. An app takes at most 32 handlers. A refusal has the code of the same Python refusal: `HRS-225`, `HRS-224`, `HRS-226`.
- A handler returns `Result<(), HandlerError>`. `?` converts any `std::error::Error`; `HandlerError::new(cause)` takes any other text.

## Event Types

| Type | Emitted when |
|------|--------------|
| `TaskClaimed` | The worker claimed a task |
| `ClaimReleased` | A claimed task went back to the queue before it reached a child |
| `AttemptPlaced` | A task reached a child |
| `AttemptNotStarted` | The child stopped before it set the task `RUNNING`; no task code ran |
| `AttemptEnded` | An attempt ended: the child reported its end, the worker ended it, or the worker killed the child and did not see its exit (`end: AttemptEnd`) |
| `AttemptRecorded` | The worker recorded the end of an attempt (`record: AttemptRecord`) |
| `TaskRecovered` | A recovery pass of the worker changed a task |
| `ChildStarted` | A child of a slot sent its ready frame |
| `ChildReplaced` | A slot replaced its child |
| `SlotDown` | A slot is down after a boot failure that can clear |
| `SlotRecovered` | A slot that was down serves again |

Each event has `at`, the wall-clock time of the worker at the step. An event holds ids, names, codes, numbers and times, never task input, a result or an error message. The types are the core's, so the Python client reads the same events.

## Delivery

- The handler child runs each handler on a thread of its own, one event at a time, in the order of the worker. An async handler runs on a current-thread runtime of its thread, so a handler that blocks its thread delays only its own events.
- Each handler has a queue of `HANDLER_QUEUE_EVENTS` (10,000) events. When it is full, the next event drops for that handler only and counts in `horsies.telemetry.handler.dropped{handler}`.
- A returned `HandlerError` or a panic counts in `horsies.telemetry.handler.errors{handler}` and is logged with the handler's name: the first one, then one line per `HANDLER_ERROR_LOG_EVERY` (1,000). The handler takes its next event.
- The handler child sends its counts to the worker every `HANDLER_STATS_INTERVAL` (5 s), and once more at its stop.
- Delivery is at most once: an event that drops is not sent again. The history tables are the record of each task.
- At the stop of the worker, the handlers get `HANDLER_DRAIN` (2 s) to take the events of their queues. The events left then are not handled.
- A handler may send tasks: the handler child has a producer pool of one connection. A handler may wait for a task result, since it holds no task capacity.

## Handler Child Start Hooks

```rust
fn add_handler_child_hook(app: &mut Horsies) {
    app.on_handler_child_start(|| async { connect_audit_sink().await });
}

async fn connect_audit_sink() -> Result<(), String> {
    Ok(())
}
```

- The hook runs once in each handler child, a replacement child included, on the child's runtime, before the handlers start.
- Hooks run in registration order. The first failure ends the rest. Each hook runs for at most `CHILD_START_HOOK_BOUND` (10 s).
- A hook that returns an error, panics or runs past its bound ends the handler child with exit code 173. The worker keeps the handler child down until the worker restarts, and runs its tasks: the handler child never stops the worker. The logged cause names the hook.
- The handler child runs only these hooks. A task child runs the hooks of `on_child_start` ([Worker Setup](/horsies/rust/workers/worker-setup/#child-start-hooks)).

## The Handler Child

| Fact | Behaviour |
|------|-----------|
| Start | When the app has a handler, or makes the spans of the attempts that the worker ended (the default with the feature `otel`): after the task children, before the first claim |
| Ready wait | The worker waits at most 30 s for its ready frame, then claims in every case |
| Boot failure that repeats | A failed start hook, no ready frame in time: the child stays down until the worker restarts |
| Boot failure that can clear | A resource shortage, a refused thread: the child boots again after the slot backoff |
| Loss after a good start | The child boots again after the slot backoff. Meanwhile each event drops and counts in `horsies.telemetry.events.dropped{cause="child_down"}` |
| Full event queue of the worker | Each event drops and counts in `horsies.telemetry.events.dropped{cause="queue_full"}`. The worker never waits for the child |

Each state snapshot of the worker writes the state of its handler child in the `telemetry` column of `horsies_worker_states`: `off`, `starting`, `up` with its pid, or `down` with its cause.

## Spans of the Attempts That the Worker Ended

With the feature `otel`, a task body runs inside its attempt span in the task child (`horsies::otel`). When the worker ends the attempt itself, that child is gone and the span never ends:

- an isolated task killed at its timeout (`TASK_TIMEOUT`);
- a run past its deadline in a shared child that the worker killed (`TASK_TIMEOUT`);
- a shared async child killed for a blocked runtime (`TASK_BLOCKED_LOOP`, `NEIGHBOUR_BLOCKED_LOOP`);
- a child lost before it reported the end, with its exit seen (`WORKER_CRASHED`);
- a child that the worker killed, or that was lost, after an operator cancel of its task (`TASK_CANCELLED`).

The `AttemptEnded` event of such an attempt is `AttemptEnd::DecidedByWorker`. A shared run that its child ended at its deadline is not one of them: its child reports the timeout (`AttemptEnd::ReportedByChild` with `ChildResult::TimedOut`), and the span of the child has `TASK_TIMEOUT`. One attempt has one span.

The handler child makes a span for each such attempt from its `AttemptEnded` event, with the global tracer provider and propagator of that process. It is the default with the feature: the worker starts its handler child also when the app has no handler. Set the provider in `main` before `horsies::cli::run`, or in an `on_handler_child_start` hook, and shut it down after `run` returns, so that a batch processor exports the last spans of the child. `Horsies::set_worker_ended_spans(WorkerEndedSpans::NoSpans)` turns the spans off:

<!-- rust-check: skip partial -- set_worker_ended_spans is built with the feature otel, which the docs check does not enable -->
```rust
use horsies::otel::WorkerEndedSpans;

fn no_worker_ended_spans(app: &mut Horsies) {
    app.set_worker_ended_spans(WorkerEndedSpans::NoSpans);
}
```

| Item | Value |
|------|-------|
| Name and kind | `run <task_name>`, CONSUMER |
| Parent | The span that sent the task, from the carrier that the send stored; a task with no carrier gets a root span |
| Start | When the task reached its child (wall clock of the worker), earlier than the start of the body |
| End | When the end reached the worker |
| Status | ERROR, with `error.type` = the error code of the attempt |
| Attributes | `horsies.task.id`, `horsies.task.name`, `horsies.task.attempt`, `horsies.task.execution` (the kind of the child), `horsies.queue`, `horsies.worker.id`, `messaging.system`, `messaging.operation.type`, `messaging.destination.name`, `messaging.message.id`, for a workflow task `horsies.workflow.id`, `horsies.workflow.node` and `horsies.workflow.root_id`, and `horsies.span.source` = `worker` |

The worker does not know the workflow node of a workflow task: the start of the child reads it. For an attempt of a workflow task that the worker ended, the handler child reads the node by the task id through its producer pool, with one read of the core: at most 1 second for a connection and 1 second for the statement. Only these attempts pay for the read.

The read never holds an event: the span waits in a queue of at most 1,000 spans (`NODE_READ_QUEUE_SPANS`), and one reader task reads the node of each span in turn, then makes it. The events go to the handlers at once.

Limits:

- A span that the body made and exported before the kill keeps the id of the lost span as its parent. A trace shows it next to the span of the worker, not under it.
- When the node read fails, or finds no node (the workflow was deleted), the span of a workflow task has no `horsies.workflow.*` attribute. The span stays in the trace of the workflow start through the stored carrier.
- When the queue of the reader is full, the span is made at once with no `horsies.workflow.*` attribute. At the stop of the worker, the reader gets the 2 seconds of the handlers; the span whose read still runs and each span still in the queue are then made with no `horsies.workflow.*` attribute, within 1 second (`NODE_READER_STOP_BOUND`).
- The worker metric `horsies.spans.worker_ended.read_failures` counts the spans made with no workflow node, by `reason`: `read_failed` (the read ran and failed) and `not_read` (the queue was full, or the child stopped first). The handler child also logs them with the task id: the first of each reason, then one line per 1,000.
- Without the feature `otel` the app makes no such span, and the worker starts no handler child for it.

## Things to Avoid

**Don't block in an async handler.** A blocked async handler takes no event while it blocks: its queue fills and its events drop. Register a handler that makes a blocking call with `on_event_blocking`.

<!-- rust-check: skip elided -- a Wrong / Correct pair -->
```rust
// Wrong: the blocking call holds the runtime of the async handler.
async fn write_audit_row(event: AttemptEnded) -> Result<(), HandlerError> {
    std::fs::write("/var/log/audit/last_end", event.task_id.to_string())?;
    Ok(())
}
app.on_event::<AttemptEnded>("audit.write", write_audit_row)?;

// Correct: a blocking handler on its own thread.
fn write_audit_row(event: AttemptEnded) -> Result<(), HandlerError> {
    std::fs::write("/var/log/audit/last_end", event.task_id.to_string())?;
    Ok(())
}
app.on_event_blocking::<AttemptEnded>("audit.write", write_audit_row)?;
```

## API Reference

### `Horsies::on_event::<E>(name, handler) -> Result<(), HorsiesError>`

| Parameter | Type | Description |
|-----------|------|-------------|
| `E` | An event type of `horsies::events` (`UnionEvent + Clone + Send + 'static`) | The events that the handler takes |
| `name` | `impl Into<String>` | The handler's name and metric label |
| `handler` | `impl AsyncFn(E) -> Result<(), HandlerError> + Send + Sync + 'static` | The async handler |

**Errors:** `HorsiesError` with the code of the same refusal of the Python registration: `HRS-225` (`HorsiesErrorCode::EventHandlerInvalidName`) for a name that is empty, longer than 255 characters or `(unknown)`, `HRS-224` (`HorsiesErrorCode::EventHandlerDuplicateName`) for a name that another handler has, `HRS-226` (`HorsiesErrorCode::EventHandlerLimit`) for a 33rd handler. Nothing is registered then.

### `Horsies::on_event_blocking::<E>(name, handler) -> Result<(), HorsiesError>`

| Parameter | Type | Description |
|-----------|------|-------------|
| `E` | An event type of `horsies::events` | The events that the handler takes |
| `name` | `impl Into<String>` | The handler's name and metric label |
| `handler` | `impl Fn(E) -> Result<(), HandlerError> + Send + Sync + 'static` | The blocking handler |

**Errors:** as `on_event`.

### `Horsies::set_worker_ended_spans(spans: WorkerEndedSpans)` (feature `otel`)

| Parameter | Type | Description |
|-----------|------|-------------|
| `spans` | `horsies::otel::WorkerEndedSpans` | `MakeSpans` (the default with the feature): the handler child makes the span of each attempt that the worker ended. `NoSpans`: no such span |

### `Horsies::on_handler_child_start(hook)`

| Parameter | Type | Description |
|-----------|------|-------------|
| `hook` | `F: FnOnce() -> Fut + Send + Sync + 'static`, `Fut: Future<Output = Result<(), E>> + Send + 'static`, `E: Display` | Runs once in each handler child |

### `HandlerError`

| Item | Description |
|------|-------------|
| `HandlerError::new(cause: impl Display)` | A failure with the text of `cause` |
| `cause() -> &str` | The text that the handler child logs |
| `From<E: std::error::Error>` | `?` on any error in a handler |

### Constants of `horsies::events`

| Constant | Value |
|----------|-------|
| `HANDLER_QUEUE_EVENTS` | 10,000 events per handler queue |
| `HANDLER_ERROR_LOG_EVERY` | 1,000: one log line per this many failures or drops of a handler, after the first |
| `HANDLER_STATS_INTERVAL` | 5 s between two `handler_stats` reports |
| `HANDLER_DRAIN` | 2 s for the handlers to take their queued events at the stop |
