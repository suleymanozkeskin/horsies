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
use horsies::events::{AttemptEnded, HandlerError, TaskClaimed};
use horsies::{Horsies, HorsiesError};

async fn record_attempt_end(event: AttemptEnded) -> Result<(), HandlerError> {
    println!(
        "task {} attempt {} ended after {:?}",
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
- `name` names the handler in the logs, and is the `handler` label of its metrics: 1 to 255 characters, not `(unknown)`, once per app. An app takes at most 32 handlers.
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
| Start | When the app has a handler: after the task children, before the first claim |
| Ready wait | The worker waits at most 30 s for its ready frame, then claims in every case |
| Boot failure that repeats | A failed start hook, no ready frame in time: the child stays down until the worker restarts |
| Boot failure that can clear | A resource shortage, a refused thread: the child boots again after the slot backoff |
| Loss after a good start | The child boots again after the slot backoff. Meanwhile each event drops and counts in `horsies.telemetry.events.dropped{cause="child_down"}` |
| Full event queue of the worker | Each event drops and counts in `horsies.telemetry.events.dropped{cause="queue_full"}`. The worker never waits for the child |

Each state snapshot of the worker writes the state of its handler child in the `telemetry` column of `horsies_worker_states`: `off`, `starting`, `up` with its pid, or `down` with its cause.

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

**Errors:** `HorsiesError` for a name that is empty, longer than 255 characters or `(unknown)`, a name that another handler has, or a 33rd handler. Nothing is registered then.

### `Horsies::on_event_blocking::<E>(name, handler) -> Result<(), HorsiesError>`

| Parameter | Type | Description |
|-----------|------|-------------|
| `E` | An event type of `horsies::events` | The events that the handler takes |
| `name` | `impl Into<String>` | The handler's name and metric label |
| `handler` | `impl Fn(E) -> Result<(), HandlerError> + Send + Sync + 'static` | The blocking handler |

**Errors:** as `on_event`.

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
