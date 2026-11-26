---
title: Tracing Setup
summary: Turn on the OpenTelemetry spans and the producer metrics of the Rust client - the feature otel, the global providers and propagator, the tracing layer, and the flush at exit.
related: [../../observability/tracing, ../../observability/logs, ../cli, ../workers/worker-setup]
tags: [rust, observability, tracing, opentelemetry]
---

# Tracing Setup

The feature `otel` makes the Rust client open the spans of [Tracing](/horsies/observability/tracing/) and record the producer metrics `horsies.task.sent` and `horsies.task.send.duration`. The client uses the global tracer provider, meter provider and propagator of `opentelemetry` 0.33. Your SDK decides what is exported and where. Without the feature, no OpenTelemetry crate is compiled.

## Add the Feature

```bash
cargo add horsies --features otel
cargo add opentelemetry@0.33 opentelemetry_sdk@0.33
```

Set the providers with `opentelemetry` 0.33, the version that the crate uses. The attempt span reaches OpenTelemetry through `tracing-opentelemetry` 0.34.

## Set the Providers in `main`

The worker starts the app's binary again for each child, and every line of `main` before `horsies::cli::run` runs in each child too. Set the global propagator and providers first in `main`, so the process that sends and each worker child have them:

<!-- rust-check: skip partial -- the docs check builds the crate without the feature otel -->
```rust
use std::process::ExitCode;

use horsies::{AppConfig, Horsies, ValidationReport};
use opentelemetry::global;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanExporter};

fn build_app() -> Result<Horsies, ValidationReport> {
    let config = AppConfig::from_database_url("postgresql://app:secret@db.example.com:5432/horsies");
    Horsies::new(config)
}

fn tracer_provider(exporter: impl SpanExporter + 'static) -> SdkTracerProvider {
    SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .build()
}

fn run_with_tracing(exporter: impl SpanExporter + 'static) -> ExitCode {
    global::set_text_map_propagator(TraceContextPropagator::new());
    let provider = tracer_provider(exporter);
    global::set_tracer_provider(provider.clone());
    let exit = match build_app() {
        Ok(app) => horsies::cli::run(app),
        Err(refused) => {
            eprintln!("{refused}");
            ExitCode::FAILURE
        }
    };
    if let Err(error) = provider.shutdown() {
        eprintln!("the tracer provider did not shut down: {error}");
    }
    exit
}
```

Call `run_with_tracing` from `main` with the span exporter of your backend, for example the OTLP exporter of the crate `opentelemetry-otlp`. Set a meter provider with `global::set_meter_provider` for the producer metrics.

- The client looks up the tracer at each span, so a provider that an `on_child_start` hook sets applies too.
- Without a provider the spans are not recorded, and a send stores no carrier.
- Without a propagator that writes keys, a send stores no carrier: each attempt span is the root of a new trace.
- A rerun (`app.rerun_task`, `horsies::rerun::rerun_task_in`) opens no span: it hands the core the carrier that the propagator injects for the caller's current context, so the attempt spans of the new task are children of the caller's span. See [Rerun in Your Transaction](/horsies/rust/operations/rerun-in-your-transaction/#trace-carrier).

## The `tracing` Layer

Each attempt span is a `tracing` span `run` (target `horsies::attempt`, `horsies::ATTEMPT_SPAN_TARGET`). Its OpenTelemetry parent is set through the layer of `horsies::otel`:

- `horsies::cli::run` adds `horsies::otel::layer()` to the subscriber that it installs, and its filter passes the attempt span at every `--log-level`.
- A binary that installs its own subscriber adds the layer, and lets its filter pass the target `horsies::attempt` at `INFO`:

<!-- rust-check: skip partial -- the docs check builds the crate without the feature otel -->
```rust
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

fn install_logs() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("info,horsies::attempt=info"))
        .with(tracing_subscriber::fmt::layer())
        .with(horsies::otel::layer())
        .init();
}
```

An attempt span with no layer, or one that the filter turns off, has no OpenTelemetry parent: the first such attempt of a process logs one WARNING that names the fix.

A `tracing` span that the body opens, and an `#[instrument]` function that it calls, is a child of the attempt span. A send from a body gets the attempt span as its parent.

## The Last Spans of a Child

A worker child exits after `horsies::cli::run` returns. A batch span processor exports the last spans of a child only when `main` shuts the provider down after `run`, as in the example above.

## Producer Metrics

Each task send that reached the database calls records, with the global meter:

| Metric | Kind | Attributes |
|--------|------|------------|
| `horsies.task.sent` | counter | `horsies.task.name`, `horsies.queue`, `horsies.result`: `ok`, or the send error code |
| `horsies.task.send.duration` | histogram, seconds | `horsies.queue` |

The names, units, help texts, attribute keys and bucket bounds are those of the [Metric Reference](/horsies/observability/metric-reference/).

## Without the Feature

Each task body still runs inside the `tracing` span `run` (target `horsies::attempt`) with the fields `horsies.task.id`, `horsies.task.name` and `horsies.task.attempt`, so each log record of the body carries them. A send and a rerun carry no trace carrier. See [Logs](/horsies/observability/logs/).

## Things to Avoid

**Don't set the providers after `horsies::cli::run`.** `run` serves a worker child and returns only at its exit. A provider set after it never records the spans of the child.

**Don't set them with another `opentelemetry` version.** The client reads the global providers of `opentelemetry` 0.33. A provider of another version is a separate global, and the client does not see it.
