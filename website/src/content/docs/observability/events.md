---
title: Lifecycle Events
section: shared
summary: The typed lifecycle events of a worker - the event types and their fields, the contract and its version, best-effort delivery, and the handler child that runs the event handlers, with its drops, stats and start hook.
related: [overview, metrics, tracing, ../workers/event-handlers, ../rust/monitoring/event-handlers]
tags: [observability, events, handlers, handler child, telemetry]
---

The worker emits one typed event at each lifecycle step of a task and of its children: a claim, the placement on a child, the end of an attempt, its record, a recovery, a child start or replacement. Event handlers that the app registers take these events. They run in the handler child of the worker, a process apart from the worker and from its task children. A handler that raises, panics, blocks or crashes never changes a task outcome and never stops the worker.

| Client | Register a handler | API page |
|--------|--------------------|----------|
| Python | `@app.on_event(EventType)` on a `def` or an `async def` function; the event types are in `horsies.telemetry` | [Event Handlers (Python)](/horsies/workers/event-handlers/) |
| Rust | `app.on_event::<E>(name, handler)` for an async handler, `app.on_event_blocking::<E>(name, handler)` for a blocking one; the event types are in `horsies::events` | [Event Handlers (Rust)](/horsies/rust/monitoring/event-handlers/) |

## Why and When

Use a handler to send what a worker does to another system: your own metrics, an audit stream, an alert on a slot that is down. Delivery is best effort, so a handler is for telemetry. The task tables and the history tables in PostgreSQL stay the record of each task.

## The Event Contract

The core defines each event type once. The Python client generates its classes from the core, and the Rust client re-exports the core types, so both clients read the same events.

- The contract has a version. This release has version 8: `core/tests/fixtures/event_contract/v8.txt` in the repository holds its text. Version 8 renames the variants of an error code that the task chose to `Custom` and `CustomOverBound`. A change of an event type, a field or a closed type of a field is a new version.
- An event holds ids, names, codes, numbers and times. It never holds task input, a task result, a failed reason or an exception text.
- Each field has a closed type or a bound. A duration is a duration type, a time is a UTC time, and a state that can be missing or not known is a named variant.
- A handler takes the events of one type, never the union of the types. A later version can add an event type, and that changes no handler of another type.
- The events of one task arrive in the order that the worker emitted them. No order holds across tasks.
- The worker builds only the events of the types that its handler child takes: the types of the handlers, and `AttemptEnded` when the spans of the attempts that the worker ended are on.

### Event Types

Each event also has `at`: the wall-clock time of the worker at the step.

| Event | When | Fields |
|-------|------|--------|
| `TaskClaimed` | The worker claimed a task | `task_id`, `task_name`, `queue`, `execution`, `attempt`, `priority`, `queue_wait` |
| `ClaimReleased` | A claimed task went back to the queue before it reached a child | `task_id`, `task_name`, `queue`, `reason` |
| `AttemptPlaced` | A task reached a child | `task_id`, `task_name`, `queue`, `execution`, `attempt`, `child_kind`, `child_pid`, `slot_wait` |
| `AttemptNotStarted` | The child stopped before it set the task `RUNNING`: no task code ran | `task_id`, `task_name`, `queue`, `attempt`, `reason` |
| `AttemptEnded` | An attempt ended | `task_id`, `task_name`, `queue`, `execution`, `attempt`, `end`, `run_duration` |
| `AttemptRecorded` | The worker recorded the end of an attempt | `task_id`, `task_name`, `queue`, `attempt`, `record`, `record_duration` |
| `TaskRecovered` | A recovery pass of the worker changed a task | `task_id`, `task_name`, `queue`, `action` |
| `ChildStarted` | A child of a slot sent its ready frame | `child_kind`, `slot`, `child_pid`, `boot_duration` |
| `ChildReplaced` | A slot replaced its child | `child_kind`, `slot`, `child_pid`, `reason` |
| `SlotDown` | A slot is down after a boot failure that can clear | `child_kind`, `slot`, `cause` |
| `SlotRecovered` | A slot that was down serves again | `child_kind`, `slot` |

- `attempt` is the retry count of the row plus 1. `task_name` has at most 255 characters, `queue` at most 52. `priority` is 1 to 100.
- `execution` is the execution class of the task. `child_kind` is the class of the child that took it: a shared task can run on an isolated child.
- `queue_wait` runs from the due time of the row to its claim. `slot_wait` runs from the claim result to the placement. `run_duration` runs from the placement to the end that the worker saw. `record_duration` runs from that end to the end of the outcome write.

### The End of an Attempt

`AttemptEnded.end` has one variant per way that an attempt ends:

| Variant | The attempt | Fields |
|---------|-------------|--------|
| `ReportedByChild` | The child reported the end in its result frame | `result`: `Completed`, `Failed` with its error code, `WorkerFailure`, `TimedOut` (the child ended a shared run at its deadline), or `Cancelled` with its `end` (an operator cancelled the run, and the run ended); `start` |
| `DecidedByWorker` | The worker ended it, with no result frame of the child | `reason`: `TIMED_OUT` (the worker killed the child at the deadline), `TASK_BLOCKED_LOOP`, `NEIGHBOUR_BLOCKED_LOOP`, `CHILD_LOST`, `KILLED_AFTER_CANCEL` (the worker killed the child after an operator cancel: the kill after `cancel_grace_ms` of an `ISOLATED` task, the deadline, a blocked loop or the stop of the worker) or `LOST_AFTER_CANCEL` (the child was lost after an operator cancel, with its exit seen) |
| `NotKnown` | The worker killed the child and did not see its exit; recovery decides the row | `child_pid` |

An error code is `BuiltIn` with a built-in code, `Custom` with a code that the task chose, of at most 128 characters, or `CustomOverBound` for a longer code that the task chose. `start` is `Reported` with the time of the `RUNNING` write, or `NotReported`. `DecidedByWorker` also has internal fields for the span of the attempt; they are not part of the stable contract.

The `end` of `Cancelled` tells how the run ended after the cancel:

| `end` | The run |
|-------|---------|
| `RETURNED_AFTER_SIGNAL` | The task code got the cancel and ended: within `cancel_grace_ms` for a shared async run, at its return for the other runs |
| `DROPPED_AFTER_GRACE` | A shared async run was alive `cancel_grace_ms` after the cancel: the child sent the second cancel |
| `BEFORE_BODY` | The cancel came after the start of the run and before the task code started: the code did not run |

`AttemptRecorded.record` is one of `Completed`, `Failed`, `RetryScheduled` (with `retry_count` and `next_retry_at`), `ClaimReleased`, `NothingOwed`, `NotHeld`, `LeftToRecovery`, `RetriesExhausted`, `WriteFailed` and `RecordedByCancel`. `RecordedByCancel`: the cancel statement wrote the `CANCELLED` attempt of the run, so the worker wrote nothing. An attempt whose end reached the worker before its child got the cancel keeps its `Completed` or `Failed` end, with the record `NotHeld`: the cancel had moved the row first.

The error code of `KILLED_AFTER_CANCEL` and `LOST_AFTER_CANCEL` is `TASK_CANCELLED`, the code of the attempt row. The `run_duration` of a cancelled attempt runs from the placement to the end that the worker saw, so it tells when the task code stopped; the `finished_at` of the attempt row is the time of the cancel.

`ClaimReleased.reason`, `AttemptNotStarted.reason`, `TaskRecovered.action` and `ChildReplaced.reason` take the values of the `reason` label of `horsies.task.claims.released`, the `reason` label of `horsies.task.starts.refused`, the `action` label of `horsies.recovery.tasks` and the `reason` label of `horsies.worker.children.replaced` on the [Metric Reference](/horsies/observability/metric-reference/), in upper case. `SlotDown.cause` is `ResourceShortage` with its errno (`EMFILE`, `ENFILE`, `ENOMEM` or `EAGAIN`), or `ThreadStartRefused`.

## The Handler Child

The worker starts one handler child when the app has an event handler, or when the spans of the attempts that the worker ended are on. With neither, it starts no child and builds no event.

| Step | Behaviour |
|------|-----------|
| Start | After the task children start, before the first claim. The worker waits at most 30 seconds for the ready frame of the child, then claims in every case |
| Boot | The child loads the app, runs the handler child start hooks, then starts one queue of 10,000 events and one thread for each handler |
| Boot failure that repeats | The app does not load, a start hook fails or runs past its bound, no ready frame within 30 seconds, another protocol version: the child stays down until the worker restarts |
| Boot failure that can clear | A resource shortage or a refused thread: the child boots again after a backoff of 1 second, doubled after each failure, at most 60 seconds |
| Loss after a good start | The child boots again after the same backoff |
| Stop | After the outcome writes of the worker are drained, the worker gets 2 seconds to write the events of its queue, then sends the stop frame. The handlers get 2 seconds together for the events of their queues. The worker kills a child that has not exited 10 seconds after the stop frame |

The handler child never stops the worker, and it is not a slot: the claim budgets and the pool failure rule do not see it. Each worker snapshot writes its state in `horsies_worker_states.telemetry`: `off`, `starting`, `up` with its pid, or `down` with its cause, the time and what follows. `horsies.fleet.workers.telemetry_down{part="handler_child"}` counts the live workers whose handler child is down.

### Start Hook

A handler child runs its own start hooks, never the start hooks of the task children: Python `@app.on_handler_child_start`, Rust `app.on_handler_child_start(hook)`. Each hook runs once in each handler child, a replacement child included, before the handlers start, for at most 10 seconds. A hook that fails or runs longer ends the child and leaves it down until the worker restarts; the worker runs its tasks. Set up what the handlers share there, for example the OpenTelemetry SDK of the child.

### Delivery, Drops and Stats

| Where | What happens |
|-------|--------------|
| The event queue of the worker | 8,192 events. An offer never waits: a full queue drops the event and counts `queue_full`; while the child is down, each event drops and counts `child_down`. Metric: `horsies.telemetry.events.dropped{cause}` |
| The writer of the worker | Sends the events in batches of at most 256, or of what arrived within 25 milliseconds after the first one |
| The queue of a handler | 10,000 events. A full queue drops the event for that handler only. Metric: `horsies.telemetry.handler.dropped{handler}` |
| A handler that raises, panics or returns an error | Counted, and logged with the handler name: the first failure, then one line per 1,000. The handler takes its next event. Metric: `horsies.telemetry.handler.errors{handler}` |

- Delivery is at most once. An event that drops is never sent again.
- The child sends the totals of each handler to the worker every 5 seconds, and once more at its stop. The worker metrics take them, with the resident memory of the child: `horsies.worker.memory.rss{process="handler_child"}`.
- The `handler` label is the name of the handler: the qualified name of the function in Python, the name given at registration in Rust. An app has at most 32 handlers.
- A handler that blocks delays only its own queue. A handler that never returns fills its own queue, and its events drop.

## Things to Avoid

**Don't use a handler as the record of a task.** An event can drop. Read the outcome of a task from the task tables, the history tables or a result read.

**Don't block in a handler with no timeout.** Its queue fills, and its events drop once 10,000 wait. Give each call to another system a timeout.
