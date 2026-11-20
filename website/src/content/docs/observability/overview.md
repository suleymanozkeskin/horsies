---
title: Observability Overview
section: shared
summary: What horsies gives for metrics, traces, logs, dashboards and alerts; what it costs when it is off; the three rules that every part keeps; and how the parts fit.
related: [metrics, fleet-collector, tracing, logs, dashboards-and-alerts, opentelemetry-collector, deployment, metric-reference]
tags: [observability, overview, metrics, tracing, logs, prometheus, opentelemetry]
---

horsies gives a worker fleet Prometheus metrics, OpenTelemetry traces, log records that name the task and the trace, and a Grafana dashboard with alert rules. Each part is off until you turn it on, and the Python client and the Rust client get each part from the one core.

## What Ships

| Need | What ships | Turn on |
|------|------------|---------|
| Metrics in Prometheus | The metrics endpoint of each worker: `GET /metrics`, `GET /livez`, `GET /readyz`. See [Metrics](/horsies/observability/metrics/) | `--metrics-listen HOST:PORT` on the worker command |
| The state of the fleet | One fleet collector: queue depth, oldest pending age, live workers, seats, unserved classes, workflows, schedules. See [Fleet Collector](/horsies/observability/fleet-collector/) | `horsies metrics <app> --listen HOST:PORT`, or `GET /metrics` of the web UI |
| Metrics in OpenTelemetry | A Collector config that scrapes the endpoints and exports OTLP. See [OpenTelemetry Collector](/horsies/observability/opentelemetry-collector/) | `deploy/otel-collector/collector.yaml` |
| Traces | One trace from the send of a task to each of its attempts, workflows included. See [Tracing](/horsies/observability/tracing/) | Python: `AppConfig(tracing=OtelTracing())` and the extra `horsies[otel]`. Rust: the feature `otel` |
| Producer metrics | `horsies.task.sent` and `horsies.task.send.duration`, through your OpenTelemetry meter | With the traces |
| Logs | `task_id`, `attempt` and `trace_id` on the records of the worker; the task on the records of a task body; JSON lines. See [Logs](/horsies/observability/logs/) | Python: `AppConfig(log_format=JsonLogs())`. Rust: `horsies::cli::run_with_log_format(app, LogFormat::Json)` |
| A trace link in the web UI | The trace id of a task in the task detail, as a link | `horsies web --trace-link TEMPLATE` |
| Dashboards and alerts | A Grafana dashboard and Prometheus alert rules in `deploy/`. See [Dashboards and Alerts](/horsies/observability/dashboards-and-alerts/) | Import the files |

Every metric, with its labels and their values, is on the [Metric Reference](/horsies/observability/metric-reference/). The examples of [Observability Deployment](/horsies/observability/deployment/) run every part on Kubernetes and with docker compose.

## What It Costs When It Is Off

- A worker with no `--metrics-listen` has no telemetry: each lifecycle point of the worker is one branch. It records no metric, starts no thread for an endpoint and builds no event.
- A client with tracing off (Python `NoTracing()`, the default; Rust without the feature `otel`) imports or compiles no OpenTelemetry code, opens no span and stores no carrier.
- Telemetry adds no SQL statement per task. The trace carrier and its trace id ride on the statements that insert, claim and start a task.
- The fleet collector is a process of its own. The web UI reads the fleet state only when a request comes.

## The Three Rules

Every part keeps three rules:

1. **Telemetry never changes a task outcome and never stops a running worker.** A port that does not bind leaves the worker running with its endpoint down. A carrier that breaks a bound leaves the send with no carrier. A failure of your OpenTelemetry SDK leaves the call with no span. A fleet query that fails leaves its gauges with no series.
2. **The worker process never waits for telemetry.** It records a metric with atomic adds; a new series takes a short lock once. A scrape runs on the thread of the endpoint, not on the claim path.
3. **No user code runs in the worker process.** Your OpenTelemetry SDK and your start hooks run in the worker children and in the processes that send.

## How the Parts Fit

```text
process that sends (Python or Rust client)
  send span, workflow start span           -> your OpenTelemetry SDK
  producer metrics                         -> your OpenTelemetry meter
  carrier + trace id -----> core send -----> horsies_tasks row (one INSERT)

worker process (the core worker)
  claim -> placement -> end -> outcome write -> retry      lifecycle points
    | with --metrics-listen: atomic counters and histograms
    v
  metric registry <--- read at a scrape --- endpoint thread
                                           GET /metrics  GET /livez  GET /readyz
  worker snapshot (30 s default) -> seat, slot and memory gauges;
                                horsies_worker_states.telemetry
  run frame with the stored carrier ------> worker child

worker child (Python or Rust)
  attempt span around the task body        -> your OpenTelemetry SDK
  task context on the log records of the body

fleet collector: horsies metrics, or GET /metrics of the web UI
  PostgreSQL -> queue, worker, workflow and schedule gauges

Prometheus, or the OpenTelemetry Collector -> OTLP
  scrapes each worker endpoint and the one fleet collector
```

- The worker records its metrics where the facts are: at its claim, at the placement of a task on a child, at the end of an attempt, at the outcome write and at each recovery pass.
- The endpoint serves the metrics of one worker. The fleet collector serves the facts of the database, so one collector serves the whole fleet.
- The core stores the trace carrier that the client gives it, and hands it to the worker child of each attempt. The core parses no carrier and has no OpenTelemetry dependency.
