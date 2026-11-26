---
title: Tracing Setup
section: python
summary: Turn on the OpenTelemetry spans and the producer metrics of the Python client - the extra horsies[otel], AppConfig.tracing, the SDK of the sending process and of each worker child, and the propagator.
related: [tracing, log-setup, ../workers/child-process-hooks, ../configuration/app-config]
tags: [observability, tracing, opentelemetry, python, configuration]
---

`AppConfig(tracing=OtelTracing())` makes the Python client open the spans of [Tracing](/horsies/observability/tracing/) with the OpenTelemetry API, and record the producer metrics `horsies.task.sent` and `horsies.task.send.duration` with the OpenTelemetry meter. The client uses the API only. Your OpenTelemetry SDK, set in each process, decides what is exported and where.

## How To

### Install the Extra

```bash
pip install 'horsies[otel]'
```

The extra installs `opentelemetry-api` 1.30 or later. Install the SDK and the exporter of your backend yourself, for example `opentelemetry-sdk` and `opentelemetry-exporter-otlp`.

### Turn Tracing On

```python
from pydantic import SecretStr

from horsies import AppConfig, Horsies, OtelTracing, PostgresConfig

config = AppConfig(
    database=PostgresConfig(
        database_url=SecretStr('postgresql://user:pass@localhost:5432/mydb'),
    ),
    tracing=OtelTracing(),
)
app = Horsies(config)
```

- Without the OpenTelemetry API, the config is refused when it is built: `HRS-220` (`CONFIG_OTEL_API_MISSING`). The help names the extra.
- The success text of `horsies check` ends with `tracing: OpenTelemetry`, or `tracing: off` for `NoTracing()`.
- With `NoTracing()` (the default) the client does not import `opentelemetry`.

### Set the SDK of the Sending Process

The client takes the tracer, the meter and the propagator from the global providers of the OpenTelemetry API at each use. Set the providers once, at the start of each process that sends tasks or starts workflows:

```python
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter


def set_up_opentelemetry() -> None:
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(tracer_provider)
    metric_reader = PeriodicExportingMetricReader(ConsoleMetricExporter())
    metrics.set_meter_provider(MeterProvider(metric_readers=[metric_reader]))
```

Use the exporters of your backend in place of the console exporters. A send inside a span of your code gets that span as its parent:

```python
checkout_tracer = trace.get_tracer('myapp.checkout')


def place_order(order_id: str) -> None:
    with checkout_tracer.start_as_current_span('place_order'):
        my_task.send(order_id=order_id)
```

### Set the SDK of Each Worker Child

The worker process runs no task body. Each attempt span is made in a worker child, so set the providers in each child with `@app.on_child_start`. The hook runs once in every worker child, after the fork:

```python
@app.on_child_start
def set_up_child_opentelemetry() -> None:
    set_up_opentelemetry()
```

A task body that sends records its send spans and producer metrics with the same providers.

At its stop, a worker child calls `force_flush` of the global tracer provider, for at most 1 second. A provider with no `force_flush` is not flushed.

### Set the Propagator

The client injects the carrier at a send and at a rerun of the app (`app.rerun_task`, `app.rerun_task_async`), and extracts it in the worker child, with the global propagator of `opentelemetry.propagate`. Set the same propagator in each process: with `opentelemetry.propagate.set_global_textmap`, or with the environment variable `OTEL_PROPAGATORS` that the OpenTelemetry API reads. The core stores the carrier of any propagator that fits the [carrier bounds](/horsies/observability/tracing/#the-trace-carrier).

### Link the Attempt to the Send

`OtelTracing(parentage=ChildOfSend())` (the default) makes each attempt span a child of the send span. `OtelTracing(parentage=LinkedToSend())` starts a new trace for each attempt, with a link to the send span:

```python
from horsies import LinkedToSend

linked_config = AppConfig(
    database=PostgresConfig(
        database_url=SecretStr('postgresql://user:pass@localhost:5432/mydb'),
    ),
    tracing=OtelTracing(parentage=LinkedToSend()),
)
```

## Producer Metrics

Each task send that returns records, with the global meter:

| Metric | Kind | Attributes |
|--------|------|------------|
| `horsies.task.sent` | counter | `horsies.task.name`, `horsies.queue`, `horsies.result`: `ok`, or the `TaskSendErrorCode` value of the error |
| `horsies.task.send.duration` | histogram, seconds | `horsies.queue` |

The histogram takes the bucket bounds of the [Metric Reference](/horsies/observability/metric-reference/#duration-buckets). A send that raised records neither metric. A workflow start records no metric.

## Things to Avoid

**Don't set the providers only in the module of the app.** The worker children need them too, and `@app.on_child_start` is the hook that runs in each child. With `preload=ChildPreload.APP` on Linux, module code runs once, in the preload process, before the fork. See [Child-Process Hooks](/horsies/workers/child-process-hooks/).

**Don't use `OtelTracing()` without an SDK and expect spans.** With the API only, every span is a no-op span, and a send stores no carrier.

## API Reference

### `OtelTracing(parentage=ChildOfSend(), worker_ended_spans=WorkerEndedSpans.MAKE_SPANS)`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `parentage` | `ChildOfSend \| LinkedToSend` | No | How each attempt span relates to the span that sent the task. Default `ChildOfSend()` |
| `worker_ended_spans` | `WorkerEndedSpans` | No | `MAKE_SPANS` (the default): the handler child of the worker makes the span of each attempt that the worker ended. `NO_SPANS`: no such span. See [Spans of the Attempts That the Worker Ended](/horsies/observability/tracing/#spans-of-the-attempts-that-the-worker-ended) |

A frozen model. `AppConfig.tracing` takes `OtelTracing` or `NoTracing`.

### `NoTracing()`

No span and no producer metric. The default of `AppConfig.tracing`.

### `ChildOfSend()`

The span of an attempt is a child of the span that sent the task.

### `LinkedToSend()`

The span of an attempt starts a new trace, with a link to the span that sent the task.

**Raises:** `ConfigurationError` (`HRS-220`) at the build of an `AppConfig` with `OtelTracing` when the OpenTelemetry API is not installed.
