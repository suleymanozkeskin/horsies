---
title: Tracing Setup
summary: Turn on the OpenTelemetry spans and the producer metrics of a TypeScript app - the tracing field of Horsies.create, parentage, workerEndedSpans, the SDK of each process and seat, the attempt spans, currentTask() for logs, and the flush at exit.
related: [../configuration/app-config, ../monitoring/event-handlers, ../workers/thread-seats, ../../observability/tracing, ../../observability/logs]
tags: [typescript, observability, tracing, opentelemetry, TracingConfigInput, AttemptParentage, WorkerEndedSpans, currentTask]
---

# Tracing Setup

`tracing: { kind: "otelTracing" }` makes the TypeScript client open the spans of [Tracing](/horsies/observability/tracing/) and record the producer metrics `horsies.task.sent` and `horsies.task.send.duration`. The client calls the OpenTelemetry API only (`@opentelemetry/api` 1.x). Your OpenTelemetry SDK, set in each process, decides what is exported and where.

## Install the API

`@opentelemetry/api` is an optional peer dependency of `horsies`, from 1.7.0 to below 2. Install it with the SDK packages of your choice:

```bash
npm install @opentelemetry/api @opentelemetry/sdk-trace @opentelemetry/core @opentelemetry/context-async-hooks
```

- The package loads the API only for an app with `otelTracing`. An app with `noTracing` never loads it.
- `Horsies.create` with `otelTracing` refuses the config with `HRS-220` when the API does not load.

## Turn Tracing On

<!-- ts-check: alone -->
```ts
import { defineTask, h, Horsies } from "horsies";

const chargeCard = defineTask({ name: "charge_card", input: { orderId: h.uuid() }, output: h.null() });

export const created = Horsies.create({
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  tasks: [chargeCard],
  tracing: { kind: "otelTracing", parentage: "childOfSend", workerEndedSpans: "makeSpans" },
});
```

`TracingConfigInput` is one of:

| Value | Does |
|-------|------|
| `{ kind: "noTracing" }` | The default. No span and no producer metric. The API is never loaded |
| `{ kind: "otelTracing", parentage?, workerEndedSpans? }` | Spans and producer metrics through the global providers of the OpenTelemetry API |

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `parentage` | `AttemptParentage`: `"childOfSend"` or `"linkedToSend"` | `"childOfSend"` | How the attempt span relates to the span that sent its task. See [Parentage](#parentage) |
| `workerEndedSpans` | `WorkerEndedSpans`: `"makeSpans"` or `"noSpans"` | `"makeSpans"` | Whether the handler child makes the span of each attempt that the worker ended. See [Spans of the Attempts That the Worker Ended](#spans-of-the-attempts-that-the-worker-ended) |

## Parentage

| `parentage` | Attempt span |
|-------------|--------------|
| `"childOfSend"` | A child of the send span. The send and its attempts are one trace |
| `"linkedToSend"` | The root of a new trace, with a link to the send span. The other entries of the extracted context, such as baggage, stay |

A task sent with no span (no SDK in the sending process, or a scheduled task) has no carrier. Its attempt span has no parent with both values.

## Set the SDK in Each Process

The worker starts the app's entry file again for each child, and every line before `cli.run` runs in each child too. Set the global context manager, propagator and tracer provider at the top of the entry file. Then the process that sends, each task child, each seat isolate and the handler child have them:

<!-- ts-check: alone -->
```ts
import { context, propagation, trace } from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import { BatchSpanProcessor, type SpanExporter, TracerProvider } from "@opentelemetry/sdk-trace";

export function installTracing(exporter: SpanExporter): TracerProvider {
  const provider = new TracerProvider({ spanProcessors: [new BatchSpanProcessor({ exporter })] });

  context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
  propagation.setGlobalPropagator(new W3CTraceContextPropagator());
  trace.setGlobalTracerProvider(provider);

  return provider;
}
```

Call `installTracing` before `Horsies.create`, with the span exporter of your backend.

- The client takes the global tracer, meter and propagator at each use. A provider that the process sets after `Horsies.create`, or in an `app.onChildStart` hook, applies too.
- The context manager makes a span active in a body and carries it across `await`. The client installs none. Without one, a span that the body opens, and a send from the body, has no parent.
- Without a tracer provider, no span is recorded and a send stores no carrier.
- Without a propagator that writes keys, a send stores no carrier: each attempt span is the root of a new trace.
- Set the same propagator in each process. The carrier bounds and the propagators that work are on [The Trace Carrier](/horsies/observability/tracing/#the-trace-carrier).
- Set a meter provider with `metrics.setGlobalMeterProvider` for the [Producer Metrics](#producer-metrics).

### Shared Thread Seats

Each seat isolate of a `SHARED_THREAD` child has its own globals. The isolate runs the entry file from its first line, so an SDK that the entry sets before `cli.run` reaches each seat. A provider of `app.provide` also runs in each seat. An `app.onChildStart` hook runs on the main thread of the child only: it does not set the SDK of a seat. See [Thread Seats](/horsies/typescript/workers/thread-seats/#what-runs-in-a-seat).

## Spans

| Span | Kind | Name | Where |
|------|------|------|-------|
| Send | PRODUCER | `send <task_name>` | The process that sends: every database call of one `app.send`, a resend of a transient failure included |
| Workflow start | PRODUCER | `start <workflow_name>` | The process that calls `app.start` or `app.resendStart` |
| Attempt | CONSUMER | `run <task_name>` | The task child, or the seat isolate, that runs the body |
| Attempt that the worker ended | CONSUMER | `run <task_name>` | The handler child, or the main thread of a shared thread child |

- The parent of a send span and of a start span is the active context of the caller.
- A start that the client refuses before the core (an unlisted workflow, a workflow id that is not a UUID, a build that fails its checks) opens no span.
- A rerun (`app.rerunTask`, `app.rerunTaskIn`) opens no span. It stores the carrier of the caller's active context, so the attempt spans of the new task are children of the caller's span.
- The worker process opens no span: it runs no user code.

### The Attempt Span

The body runs inside its attempt span. A span that the body opens nests under it, also after an `await`. A task that the body sends gets a send span under it.

| End of the body | Status |
|-----------------|--------|
| `ok(value)` | Not set |
| `err({ code })` | ERROR, `error.type` = the code |
| A thrown value | The value is recorded on the span; ERROR, `error.type` = the stored code |
| The deadline of the run | ERROR, `error.type` = `TASK_TIMEOUT` |
| An operator cancel | ERROR, `error.type` = `TASK_CANCELLED` |

- The span ends at the core's end of the body. In a `SHARED_ASYNC` child, the core ends a run at its deadline or at an operator cancel while the body can run on. The span ends there, so a body that never settles still gets its span.
- In a seat isolate, the span ends at the settle of the run. A sync body that holds its isolate past its deadline ends its span at that settle.
- A body that does not start gets no span.
- The attributes are those of [Attributes](/horsies/observability/tracing/#attributes). `horsies.task.execution` is `ISOLATED`, `SHARED_ASYNC` or `SHARED_THREAD`.

### Spans of the Attempts That the Worker Ended

The worker ends some attempts itself. Then the child is gone and its span is never exported:

- an isolated task killed at its timeout (`TASK_TIMEOUT`);
- a run past its deadline in a shared child that the worker killed (`TASK_TIMEOUT`);
- a shared async child killed for a blocked event loop (`TASK_BLOCKED_LOOP`, `NEIGHBOUR_BLOCKED_LOOP`);
- a child lost with its exit seen (`WORKER_CRASHED`).

With `workerEndedSpans: "makeSpans"`, the handler child makes a span for each such attempt from its `AttemptEnded` event, with the global tracer provider and propagator of the handler child. The worker then starts its handler child also when the app has no event handler. The name, the parent, the start, the end and the attributes are on [Spans of the Attempts That the Worker Ended](/horsies/observability/tracing/#spans-of-the-attempts-that-the-worker-ended). The span has `horsies.span.source` = `worker`.

- Set the SDK of the handler child at the top of the entry file, or in an `app.onHandlerChildStart` hook. See [Event Handlers](/horsies/typescript/monitoring/event-handlers/#handler-child-start-hooks).
- All handlers of the handler child and the span maker share one event loop. A handler that holds the loop delays the spans.
- `workerEndedSpans: "noSpans"` turns these spans off.

A seat isolate that ends during a run (an uncaught throw, `process.exit`, the heap cap) loses its own span. The main thread of the child makes the span in its place: the same name, kind, parent and attributes, with `horsies.span.source` = `worker`, status ERROR and `error.type` = `WORKER_CRASHED`. It starts when the main thread posted the run and ends at the exit of the isolate.

## Logs With the Task Ids

`currentTask()` gives the task whose body runs in the current async context, with the ids of its attempt span. A log library puts them on each record:

<!-- ts-check: alone -->
```ts
import { currentTask } from "horsies";

export function taskLogFields(): Readonly<Record<string, string | number>> {
  const task = currentTask();

  switch (task.kind) {
    case "noTask":
      return {};
    case "running":
      switch (task.span.kind) {
        case "noSpanIds":
          return { task_id: task.taskId, task_name: task.taskName, attempt: task.attempt };
        case "spanIds":
          return {
            task_id: task.taskId,
            task_name: task.taskName,
            attempt: task.attempt,
            trace_id: task.span.traceId,
            span_id: task.span.spanId,
          };
      }
  }
}
```

| Type | Fields |
|------|--------|
| `RunningTask` | `kind: "running"`, `taskId: TaskId`, `taskName`, `attempt` (from 1), `span: SpanIds \| NoSpanIds` |
| `NoRunningTask` | `kind: "noTask"`: no task body runs in the current async context |
| `SpanIds` | `kind: "spanIds"`, `traceId` (32 lower-case hex digits), `spanId` (16 lower-case hex digits) |
| `NoSpanIds` | `kind: "noSpanIds"`: the app does not trace, or the SDK gave a span that is not valid |

- `currentTask()` is `running` inside a body, also after an `await`, in a timer that the body set, and in the work of `runtime.spawn`. It reads two async local stores and never throws.
- The fields of the core's own records are on [Logs](/horsies/observability/logs/).

## The Last Spans of a Child

A task child calls `forceFlush()` of the global tracer provider before it exits, for at most 1,000 ms together with the wait for its log records. A seat isolate flushes its own provider at the stop of its child. The exit does not wait for more: use a batch span processor with an export that ends within the bound.

A process that sends owns its exit. Call `provider.shutdown()` before it ends, so that the batch processor exports its last send spans.

## Producer Metrics

Each send call that reached the database records, with the global meter:

| Metric | Kind | Attributes |
|--------|------|------------|
| `horsies.task.sent` | counter | `horsies.task.name`, `horsies.queue`, `horsies.result`: `ok`, or the send error code |
| `horsies.task.send.duration` | histogram, seconds | `horsies.queue` |

The names, units, help texts, attribute keys and bucket bounds are those of the [Metric Reference](/horsies/observability/metric-reference/).

## When the SDK Fails

A throw of your SDK never changes a send, a start or a run:

- A throw when a span opens gives no span and no carrier: the send goes on.
- A throw at a later stage of a span (the record of a thrown value, the end) is a span not made. The client writes one WARNING record on the logger `horsies.telemetry`, once per process and kind of span.
- A throw of a metric point, of the carrier of a rerun, or of the flush is one warning per process and kind.

## Things to Avoid

**Do not set the SDK after `cli.run`.** In a child, `cli.run` serves the child and ends the process. A line after it never runs there.

**Do not set the SDK of a seat in `app.onChildStart`.** The hook runs on the main thread of a shared thread child. Set the SDK at the top of the entry file.

**Do not leave out the context manager.** A span that a body opens then has no parent, and the send spans of the body leave the trace of the attempt.
