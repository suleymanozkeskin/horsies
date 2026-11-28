---
title: Tracing
section: shared
summary: The OpenTelemetry spans of horsies - the send, workflow start and attempt spans, the trace carrier that a task row stores, propagators, attributes, status, workflows, and the limits.
related: [logs, events, tracing-setup, ../rust/observability/tracing-setup, ../workers/event-handlers, ../rust/monitoring/event-handlers, ../monitoring/web-ui-mount]
tags: [observability, tracing, opentelemetry, spans, propagation]
---

With tracing on, the send of a task and each attempt of that task are spans of one trace. The client opens the spans with the OpenTelemetry API. The OpenTelemetry SDK that you set in each process exports them. The core stores the trace carrier of the send on the task row, and the worker child that runs an attempt reads it back.

Tracing is off by default. Turn it on in each client:

| Client | Turn on | Setup |
|--------|---------|-------|
| Python | `AppConfig(tracing=OtelTracing())`, with the extra `horsies[otel]` | [Tracing Setup (Python)](/horsies/observability/tracing-setup/) |
| Rust | The feature `otel` of the crate `horsies` | [Tracing Setup (Rust)](/horsies/rust/observability/tracing-setup/) |
| TypeScript | `tracing: { kind: "otelTracing" }` in `Horsies.create`, with the peer `@opentelemetry/api` | [Tracing Setup (TypeScript)](/horsies/typescript/observability/tracing-setup/) |

With tracing off, a client imports or compiles no OpenTelemetry code, opens no span, and stores no carrier.

## Why and When

Use tracing to follow one task from the code that sent it to each attempt in a worker, and to see the spans that the task body makes under its attempt. A trace shows where the time went: the send, the wait in the queue, the run, a retry.

## Spans

| Span | Kind | Name | Process | Covers |
|------|------|------|---------|--------|
| Send | PRODUCER | `send <task_name>` | The process that sends | Every database call of one task send, a resend of a transient failure included |
| Workflow start | PRODUCER | `start <workflow_name>` | The process that starts the workflow | One workflow start |
| Attempt | CONSUMER | `run <task_name>` | The worker child | The task body of one attempt, after the child set the task `RUNNING` |

- The parent of a send span and of a start span is the current context of the caller.
- The parent of an attempt span is the span that sent the task. The configured propagator of the worker child extracts it from the carrier that the task row stores.
- A task row with no carrier gives an attempt span with no parent: a new trace.
- The worker process opens no span. It runs no user code, so it has no OpenTelemetry SDK of yours.
- A span that the task body starts is a child of the attempt span. A task that the body sends gets a send span under the attempt span.

### Attempt Parentage

| Client | Setting | Attempt span |
|--------|---------|--------------|
| Python | `OtelTracing(parentage=ChildOfSend())` (default) | A child of the send span: the send and its attempts are one trace |
| Python | `OtelTracing(parentage=LinkedToSend())` | The root of a new trace, with a link to the send span. The other entries of the extracted context, such as baggage, stay |
| Rust | No setting | A child of the send span |
| TypeScript | `parentage: "childOfSend"` (default) or `parentage: "linkedToSend"` | As the Python settings of the same names |

## The Trace Carrier

At a send, the client injects the carrier of the send span with the configured propagator: a map of text to text. The client passes the map and the 16-byte trace id of the span to the core. The core does not parse the map. It checks the bounds, then stores the map as one JSON text in `horsies_tasks.trace_carrier`, and the trace id in `horsies_tasks.trace_id`.

| Bound | Value |
|-------|-------|
| Keys per carrier | At most 8 |
| Bytes per key | 1 to 128 |
| Key characters | The characters of an HTTP token: letters of either case, digits, and ``!#$%&'*+-.^_`\|~`` |
| Bytes per value | At most 1,024 |
| Value characters | Printable ASCII, `0x20` to `0x7e` |
| Bytes of the stored JSON text | At most 2,048 |
| Trace id | 16 bytes, not all zero |

- A carrier that breaks a bound is not stored. The send goes on, and the row has no carrier. The first refusal of a process logs one WARNING that names the bound; the line holds no key and no value. Later refusals log nothing.
- An empty carrier stores nothing and logs nothing. A send has an empty carrier when the process has no SDK, or when the span is not valid.
- A retry keeps the row, so each attempt of a task has the carrier of its send.
- A send that answers from an existing row (the same task id or idempotency key) keeps the carrier of that row.
- The history row of a task keeps the trace id (`horsies_task_history.trace_id`), not the carrier.

### Propagators

The core stores the carrier of any propagator. The send injects with the global propagator of the sending process, and the worker child extracts with the global propagator of the child:

- W3C Trace Context writes `traceparent` and `tracestate`.
- The core keeps each key as the propagator wrote it. The AWS X-Ray propagator writes and reads `X-Amzn-Trace-Id`, so an X-Ray carrier works.
- A propagator that writes more than 8 keys, or a value that is not printable ASCII, breaks a bound: the row stores no carrier.

Set the same propagator in each process that sends and in each worker child.

## Workflows

- A workflow start stores its carrier and its trace id on every workflow row of the started tree.
- A sub-workflow that starts later copies the pair of its parent workflow.
- Each task row of a workflow node copies the pair of its workflow.

So every attempt of every task of a workflow, at every depth, is a child of the one `start <workflow_name>` span. An attempt span of a task that a workflow node links has `horsies.workflow.id`, `horsies.workflow.node` and `horsies.workflow.root_id`.

## Scheduled Tasks and Reruns

- The scheduler inserts the task row of a schedule run in SQL, with no send span and no carrier. Each attempt of a scheduled task is the root of a new trace.
- A rerun of the Rust client (feature `otel`) opens no span and stores the carrier of the caller's current context: the attempts of the new task are children of the caller's span. With no valid span in that context it stores no carrier.
- A rerun of the Python client through the app (`app.rerun_task_in`, `app.rerun_task_in_async`) with `OtelTracing()` does the same. With `NoTracing()` it stores no carrier.
- The free function `rerun_task_in_async` of the Python client has no app, so it stores no carrier. Each attempt of its new task is the root of a new trace.

## Attributes

The `horsies.*` attributes are the names of horsies. The `messaging.*` attributes use the names of the OpenTelemetry messaging semantic conventions.

| Attribute | Send | Start | Attempt | Value |
|-----------|------|-------|---------|-------|
| `messaging.system` | Yes | Yes | Yes | `horsies` |
| `messaging.operation.type` | Yes | Yes | Yes | `send` for a send and a start; `process` for an attempt |
| `messaging.operation.name` | Yes | Yes | Python only | `send`, `start` or `run` |
| `messaging.destination.name` | Yes | No | Yes | The queue name |
| `messaging.message.id` | Yes | No | Yes | The task id |
| `horsies.task.id` | Yes | No | Yes | The task id |
| `horsies.task.name` | Yes | No | Yes | The task name |
| `horsies.queue` | Yes | No | Yes | The queue name |
| `horsies.task.attempt` | No | No | Yes | The attempt number, from 1 |
| `horsies.task.execution` | No | No | Yes | The execution class of the child that ran the attempt: `ISOLATED`, `SHARED_ASYNC` or `SHARED_THREAD` |
| `horsies.worker.id` | No | No | Yes | The id of the worker that claimed the task |
| `horsies.workflow.id` | No | Yes | Workflow tasks | The id of the workflow of the node; for a start, the id of the started workflow |
| `horsies.workflow.name` | No | Yes | No | The workflow name |
| `horsies.workflow.node` | No | No | Workflow tasks | The index of the node |
| `horsies.workflow.root_id` | No | No | Workflow tasks | The id of the top-level workflow of the tree |
| `error.type` | On error | On error | On error | See [Status](#status) |

## Status

| Span | End | Status |
|------|-----|--------|
| Send, start | The call returned success | Not set |
| Send, start | The call returned an error | ERROR, `error.type` = the error code |
| Send, start | The call raised (Python) | ERROR, `error.type` = the qualified class name of the exception |
| Attempt | The body returned a value | Not set (Python); OK (Rust) |
| Attempt | The body returned a task error | ERROR, `error.type` = the error code. Python records the live exception that the task error keeps |
| Attempt | A panic of the body (Rust) | ERROR, `error.type` = `UNHANDLED_EXCEPTION` |
| Attempt | The deadline of a shared run fired in its child before the end of the body: the end that the child reports | ERROR, `error.type` = `TASK_TIMEOUT`. What the body returned is not on the span |
| Attempt | An exception left the steps of the run, or a refusal of the core ended the body (Python) | ERROR, `error.type` = the class name of the exception, which is recorded |

In the Python client, a failure of your OpenTelemetry SDK never changes the outcome of a send or of an attempt: an SDK that raises when a span opens leaves the call with no span, and one that raises later is logged once per process.

## Spans of the Attempts That the Worker Ended

The attempt span ends when the body returns. The worker ends some attempts itself, with no result frame of the child, and then the child is gone and its span never ends:

- an isolated task killed at its `timeout_ms` (`TASK_TIMEOUT`);
- a run past its deadline in a shared child that the worker killed because no run in it had time left (`TASK_TIMEOUT`);
- a shared async child killed for a blocked event loop (`TASK_BLOCKED_LOOP`, `NEIGHBOUR_BLOCKED_LOOP`);
- a child lost before it reported the end, with its exit seen (`WORKER_CRASHED`).

A shared run that its child ended at its deadline is not one of them: the child reports the timeout in its result frame, and the span of the child ends with `TASK_TIMEOUT`. One attempt has one span.

For each such attempt, the [handler child](/horsies/observability/events/#the-handler-child) of the worker makes a span in its place, from the `AttemptEnded` event, with the global tracer provider and propagator of the handler child. It is on by default with tracing on, and the worker then starts its handler child also when the app has no event handler.

| Client | On (default with tracing) | Off |
|--------|---------------------------|-----|
| Python | `OtelTracing(worker_ended_spans=WorkerEndedSpans.MAKE_SPANS)` | `OtelTracing(worker_ended_spans=WorkerEndedSpans.NO_SPANS)` |
| Rust | The feature `otel` | `app.set_worker_ended_spans(WorkerEndedSpans::NoSpans)` |
| TypeScript | `workerEndedSpans: "makeSpans"` | `workerEndedSpans: "noSpans"` |

Set the tracer provider of the handler child in its start hook (Python `@app.on_handler_child_start`), or in `main` before `horsies::cli::run` (Rust), or at the top of the entry file or in `app.onHandlerChildStart` (TypeScript).

| Item | Value |
|------|-------|
| Name and kind | `run <task_name>`, CONSUMER, as the span of a child |
| Parent | The span that sent the task, from the stored carrier, placed as the attempt span of a child is |
| Start | When the task reached its child, on the wall clock of the worker. This is earlier than the start of the span of a child by the transit of the run frame and the `RUNNING` write |
| End | When the end reached the worker |
| Status | ERROR, with `error.type` = the error code of the attempt |
| Attributes | The attributes of the attempt span of the client, with `horsies.task.execution` = the class of the child, and `horsies.span.source` = `worker` |

The worker does not know the workflow node of a workflow task: the start write of the child reads it. For an attempt of a workflow task that the worker ended, the handler child reads the node by the task id, with at most 1 second for a connection and 1 second for the statement. The read never holds an event: the span waits in a queue of at most 1,000 spans, and one reader reads the node of each span in turn, then makes it.

- When the read fails or finds no node, or the queue is full, or the worker stops first, the span has no `horsies.workflow.*` attribute. The stored carrier still places it in the trace of the workflow start. `horsies.spans.worker_ended.read_failures{reason}` counts these spans: `read_failed` and `not_read`.
- The span is made on the thread or task of the handler child that hands out the events. A span processor that exports in `on_end` delays the events by its export: use a batch span processor.

## The Trace Id of a Task

The task detail, the workflow node detail and the workflow run detail of the monitoring reads return the trace id of the send span (or of the start span): 32 lowercase hex digits, live and in history. A task with no trace has none. The web UI shows it in the task detail, as a link when the UI has a trace link template: `horsies web --trace-link TEMPLATE`, or the `trace_link` field of the config of a mount ([Python](/horsies/monitoring/web-ui-mount/#monitoringuiconfig), [Rust](/horsies/rust/monitoring/web-ui-mount/#monitoringconfig)).

The log records of the worker for an attempt carry the same trace id. See [Logs](/horsies/observability/logs/).

## Limits

- **The span of an attempt that the worker ended starts at the placement.** It stands in for the span of the child, which was never exported. With the worker-ended spans off, the trace of such an attempt has the send span and no span of the attempt. The attempt record, the metrics and the history row of the task still record it.
- **An attempt with an end that is not known has no span.** When the worker killed the child and did not see its exit, recovery decides the row, and no span is made.
- **Spans of a killed attempt keep the lost parent.** A span that the body made inside a killed attempt, and that its child exported before the kill, keeps the id of the lost attempt span as its parent. A trace shows it next to the span of the worker, not under it.
- **The last spans of a child.** At its stop, a Python worker child, and a Python handler child after the 2 seconds of its handlers, flush the global tracer provider for at most 1 second. The Rust client has no flush of its own: a batch span processor exports the last spans of a child only when `main` shuts the provider down after `horsies::cli::run` returns. A TypeScript task child calls `forceFlush()` of the global tracer provider before it exits, together with the wait for its log records for at most 1 second.
- **No span for the time in the queue.** The wait from the due time to the claim is the metric `horsies.task.queue.duration`. See the [Metric Reference](/horsies/observability/metric-reference/).

## Things to Avoid

**Don't set a different propagator in the worker children.** The child extracts the carrier with its own global propagator. A propagator that does not read the keys that the sender wrote gives the attempt span no parent.

**Don't expect a trace for a scheduled task, or for a rerun through the free function `rerun_task_in_async` of the Python client, to start at the scheduler or at the rerun call.** Neither stores a carrier: each attempt is the root of its own trace. Use `app.rerun_task_in` or `app.rerun_task_in_async` for a rerun that continues the trace of the caller.
