---
title: Event Handlers
section: python
summary: Run code of the app for the lifecycle events of a worker with @app.on_event, in a handler child apart from the tasks.
related: [child-process-hooks, worker-architecture, ../tasks/errors]
tags: [workers, events, handlers, telemetry, hooks]
---

`@app.on_event(EventType)` registers a `def` or an `async def` function that a worker calls for each lifecycle event of one type: a claim, the placement of a task on a child, the end of an attempt, its record, a recovery, a child start or replacement. The worker runs the handlers in its handler child, a process apart from the worker and from its task children. A handler that raises, blocks or crashes never changes a task outcome and never stops the worker.

## Why and When

Use a handler to send what a worker does to another system: count the attempts of each task in your own metrics, write the end of each attempt to an audit stream, alert on a slot that is down. Delivery is best effort: an event that a full queue cannot hold is dropped and counted. The task tables and the history tables in Postgres stay the record of each task.

## How To

Import the event type from `horsies.telemetry` and decorate a function that takes one event of that type:

```python
import logging

from horsies.telemetry import (
    AttemptEndDecidedByWorker,
    AttemptEnded,
    AttemptEndNotKnown,
    AttemptEndReportedByChild,
    ChildResultCompleted,
)

attempt_log = logging.getLogger('myapp.attempts')


@app.on_event(AttemptEnded)
def log_attempt_end(event: AttemptEnded) -> None:
    match event.end:
        case AttemptEndReportedByChild(result=ChildResultCompleted()):
            outcome = 'completed'
        case AttemptEndReportedByChild():
            outcome = 'failed'
        case AttemptEndDecidedByWorker(reason=reason):
            outcome = reason
        case AttemptEndNotKnown():
            outcome = 'not known'
    attempt_log.info(
        '%s attempt %d: %s after %.3fs',
        event.task_name,
        event.attempt,
        outcome,
        event.run_duration.total_seconds(),
    )
```

An `async def` handler runs on an event loop of its own:

```python
import asyncio
import json
from pathlib import Path

from horsies.telemetry import AttemptRecorded

AUDIT_FILE = Path('/var/log/myapp/attempt-records.jsonl')


def append_line(line: str) -> None:
    with AUDIT_FILE.open('a', encoding='utf-8') as audit:
        audit.write(line + '\n')


@app.on_event(AttemptRecorded)
async def audit_attempt_record(event: AttemptRecorded) -> None:
    line = json.dumps(
        {
            'task_id': str(event.task_id),
            'attempt': event.attempt,
            'record': type(event.record).__name__,
        },
    )
    await asyncio.to_thread(append_line, line)
```

Register a handler in the app module or in a task module that the app discovers: the worker and its handler child import the same modules. `@app.on_handler_child_start` registers a sync function that runs once at the boot of the handler child, before its first event. Set up what the handlers share there, for example a log handler or the OpenTelemetry SDK of the child:

```python
import logging

event_log = logging.getLogger('myapp.events')


@app.on_handler_child_start
def configure_event_log() -> None:
    event_log.addHandler(logging.FileHandler('/var/log/myapp/events.log'))
```

The `on_child_process_start` hooks do not run in the handler child.

## Event Types

Each event is a frozen dataclass with `at`, the wall clock time of the worker when the event happened. A handler takes the events of one type. Match on the closed types that an event holds (`AttemptEnd`, `AttemptRecord`, `SlotDownCause`) with `match`.

| Event | When |
|-------|------|
| `TaskClaimed` | The worker claimed a task. |
| `ClaimReleased` | A claimed task went back to the queue before it reached a child. |
| `AttemptPlaced` | A task reached a child. |
| `AttemptNotStarted` | The child stopped before it set the task `RUNNING`. No task code ran. |
| `AttemptEnded` | An attempt ended: the child reported its end, the worker ended it, or the worker killed the child and did not see its exit. |
| `AttemptRecorded` | The worker recorded the end of an attempt. |
| `TaskRecovered` | A recovery pass of the worker changed a task. |
| `ChildStarted` | A child of a slot sent its ready frame. |
| `ChildReplaced` | A slot replaced its child. |
| `SlotDown` | A slot is down after a boot failure that can clear. |
| `SlotRecovered` | A slot that was down serves again. |

An event holds ids, names, codes, numbers and times. It never holds task input, a task result or an exception text. The worker builds and sends only the events of the types that a handler takes.

## How the Worker Runs Handlers

- The worker starts its handler child when the app has a handler, or when the spans of the attempts that the worker ended are on, after the task children and before its first claim. It waits at most 30 seconds for the child to be ready, then claims in every case.
- The child imports the app, runs the `on_handler_child_start` hooks, and starts one thread and one queue of 10,000 events for each handler. An `async def` handler runs on an event loop that its thread owns.
- Each handler gets the events of its type in the order that the worker emitted them. A handler takes its next event when its call returns.
- A handler whose queue is full misses the events that do not fit. The other handlers go on.
- A handler that raises is counted, and logged with its name: the first failure with its traceback, then one line per 1,000 failures. It gets the next event.
- The child reports its counts to the worker every 5 seconds: `horsies.telemetry.handler.errors{handler}` and `horsies.telemetry.handler.dropped{handler}`. The `handler` label is the handler name. Events that did not reach the child count in `horsies.telemetry.events.dropped{cause}`.
- At the stop of the worker, the handlers get 2 seconds together for the events left in their queues. A handler that does not return in that time does not delay the exit; the events left are logged.

The name of a handler is `module.qualname` of its function, for example `myapp.events.log_attempt_end`.

## Spans of the Attempts That the Worker Ended

The task child opens the span of an attempt around the body, and OpenTelemetry exports a span only when it ends. When the worker ends an attempt itself, the child is gone and its span never ends:

- an isolated task killed at its `timeout_ms` (`TASK_TIMEOUT`);
- a run past its deadline in a shared child that the worker killed (`TASK_TIMEOUT`);
- a shared async child killed for a blocked event loop (`TASK_BLOCKED_LOOP`, `NEIGHBOUR_BLOCKED_LOOP`);
- a child lost before it reported the end, with its exit seen (`WORKER_CRASHED`).

The `AttemptEnded` event of such an attempt is `AttemptEndDecidedByWorker`. A shared run that its child ended at its deadline is not one of them: its child reports the timeout in its result frame (`AttemptEndReportedByChild` with `ChildResultTimedOut`), and the span of the child has the timeout. One attempt has one span.

With `OtelTracing()`, the handler child makes the span of each such attempt from the `AttemptEnded` event, with the global tracer provider of the handler child. `OtelTracing(worker_ended_spans=WorkerEndedSpans.MAKE_SPANS)` is the default: the worker starts its handler child also when the app has no handler. Set the tracer provider in `@app.on_handler_child_start`:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter


@app.on_handler_child_start
def set_tracer_provider() -> None:
    provider = TracerProvider()
    provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(provider)
```

Use the exporter of your backend in place of `ConsoleSpanExporter`, for example the OTLP exporter of `opentelemetry-exporter-otlp`.

The span stands in for the span of the child:

| Item | Value |
|------|-------|
| Name and kind | `run <task_name>`, CONSUMER |
| Parent | The span that sent the task, from the carrier that the send stored. `LinkedToSend()` starts a trace of its own with a link to that span. A task sent with no span gets a span with no parent. |
| Start | When the task reached its child (wall clock of the worker): earlier than the start of the span of a child by the transit of the run frame and the `RUNNING` write. |
| End | When the end reached the worker. |
| Status | ERROR, with `error.type` = the error code of the attempt |
| Attributes | The attributes of the span of a child: `horsies.task.id`, `horsies.task.name`, `horsies.task.attempt`, `horsies.task.execution` (the kind of the child), `horsies.queue`, `horsies.worker.id`, `messaging.system`, `messaging.operation.type`, `messaging.operation.name`, `messaging.destination.name`, `messaging.message.id`, and for a workflow task `horsies.workflow.id`, `horsies.workflow.node` and `horsies.workflow.root_id`; and `horsies.span.source` = `worker` |

The worker does not know the workflow node of a workflow task: the start of the child reads it. For an attempt of a workflow task that the worker ended, the handler child reads the node by the task id with one read of the core, at most 1 second for a connection and 1 second for the statement. Only these attempts pay for the read.

The read never holds an event: the span waits in a queue of at most 1,000 spans, and one reader thread reads the node of each span in turn, then makes it. The events go to the handlers at once.

Limits:

- A span that the task code made inside the killed attempt, and that its child exported before the kill, keeps the id of the lost span as its parent. A trace shows it next to the span of the worker, not under it.
- When the node read fails, or finds no node (the workflow was deleted), the span of a workflow task has no `horsies.workflow.*` attribute. The span is still in the trace of the workflow start, through the stored carrier.
- When the queue of the reader is full, the span is made at once with no `horsies.workflow.*` attribute. At the stop of the worker, the reader gets the 2 seconds of the handlers; each span still in its queue is then made with no `horsies.workflow.*` attribute.
- The worker metric `horsies.spans.worker_ended.read_failures` counts the spans made with no workflow node, by `reason`: `read_failed` (the read ran and failed) and `not_read` (the queue was full, or the child stopped first). The handler child also logs them with the task id: the first of each reason, then one line per 1,000.
- At the stop of the worker, the handler child flushes the tracer provider for at most 1 second after the 2 seconds of the handlers.

`OtelTracing(worker_ended_spans=WorkerEndedSpans.NO_SPANS)` makes no such span; the trace of such an attempt then has the send span and no span of the attempt. With `NoTracing()` the handler child imports no `opentelemetry` module.

## Failure Semantics

The handler child never stops the worker. The worker snapshot (`horsies_worker_states.telemetry`) shows the state of the child: `off`, `starting`, `up` or `down` with its cause.

| Failure | What happens |
|---------|--------------|
| The app does not import in the child | The child stays down until the worker restarts. The worker runs its tasks. |
| An `on_handler_child_start` hook raises or runs longer than 10 seconds | The child ends with the hook named in the log, and stays down until the worker restarts. |
| The child is not ready within 30 seconds | The child stays down until the worker restarts. |
| The OS is short of a resource at the boot (a file descriptor, memory, a thread) | The worker boots the child again after a backoff. |
| The child dies after it was ready | The worker boots it again after a backoff. While it is down, events drop and count. |

## Things to Avoid

**Don't block in a handler.** A handler that waits on I/O with no timeout holds its own queue: its events drop once 10,000 wait. Give each call to another system a timeout.

**Don't subscribe to the union of the events.** `app.on_event(AnyEvent)` is refused (`HRS-222`). Register one handler per event type: a later version can add an event type.

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
from horsies.telemetry import AnyEvent, AttemptEnded, TaskClaimed

# Wrong: refused with HRS-222
@app.on_event(AnyEvent)
def log_any(event: AnyEvent) -> None: ...

# Correct: one handler per event type
@app.on_event(TaskClaimed)
def log_claim(event: TaskClaimed) -> None: ...

@app.on_event(AttemptEnded)
def log_end(event: AttemptEnded) -> None: ...
```

**Don't register one function twice.** The name of a handler is the name of its function, so a second registration is refused (`HRS-224`). Write one function per event type.

## API Reference

### `app.on_event(event_type: type[E]) -> EventHandlerDecorator[E]`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `event_type` | `type[E]` | Yes | One event type of `horsies.telemetry` |

**Returns:** a decorator that registers a `def` function `(event: E) -> None` or an `async def` function `(event: E) -> Coroutine[..., None]`, and gives it back unchanged.

**Raises:** `ConfigurationError` with one code per rule:

<!-- members: ErrorCode subset -->
| Code | When |
|------|------|
| `HRS-222` | `event_type` is not an event type (at the call of `on_event`) |
| `HRS-223` | The handler is not a function or a bound method, is a generator function, does not take a call with one event, or annotates its event parameter with another event type |
| `HRS-224` | An earlier handler has the same name |
| `HRS-225` | The function has no module name, or its name is longer than 255 characters |
| `HRS-226` | The app has 32 handlers already |

### `app.on_handler_child_start(fn: HandlerChildStartHook) -> HandlerChildStartHook`

Registers a sync function with no arguments. Hooks run in registration order, each for at most 10 seconds, in each new handler child. A function is registered once.

**Raises:** `ConfigurationError` (`HRS-214`) for a value that is not callable, or for an `async def` function.

### `app.get_event_handlers() -> tuple[EventHandler, ...]`

The registered handlers in registration order. Each record (`horsies.core.event_handlers.EventHandler`) holds the `name`, the `event_type` (its `event_class` and its `name`), the `kind` (`HandlerKind.DEF` or `HandlerKind.ASYNC_DEF`) and the `function`.

### `app.get_handler_child_start_hooks() -> list[HandlerChildStartHook]`

The registered hooks in registration order.
