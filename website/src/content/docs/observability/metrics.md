---
title: Metrics
section: shared
summary: The metrics endpoint of a worker (--metrics-listen, /metrics, /livez, /readyz), when the worker records each metric, the label rules (closed sets, fallback values, undeclared, the fold of a name that cannot be a label value), and the series count.
related: [metric-reference, fleet-collector, opentelemetry-collector, dashboards-and-alerts, ../tasks/errors]
tags: [observability, metrics, prometheus, probes, labels]
---

horsies has three sources of metrics. Each metric of the [Metric Reference](/horsies/observability/metric-reference/) names its recorder.

| Source | Metrics | Served by |
|--------|---------|-----------|
| Each worker | Attempts, durations, retries, claims, children, seats, recovery, memory, telemetry drops | The metrics endpoint of the worker: `--metrics-listen HOST:PORT` |
| The fleet collector | Queue depth, oldest pending age, live workers, seats, unserved classes, workflows, schedules | `horsies metrics` and `GET /metrics` of the web UI. See [Fleet Collector](/horsies/observability/fleet-collector/) |
| Each process that sends | `horsies.task.sent`, `horsies.task.send.duration` | Your OpenTelemetry SDK. See [Tracing](/horsies/observability/tracing/) |

The worker records its metrics natively: no client library, no child process and no Prometheus package is needed.

## Why and When

Turn on the endpoint of each worker to count attempts by outcome and error code, to measure run time and queue wait, and to watch the children and the seats of the worker. Scrape the fleet collector once per database for the facts of the fleet.

## How To

Give each worker an address:

```bash
horsies worker myapp.instance:app --profile default --metrics-listen 0.0.0.0:9464
```

```bash
./myapp worker --profile default --metrics-listen 0.0.0.0:9464
```

```yaml
scrape_configs:
  - job_name: horsies-worker
    static_configs:
      - targets: ['worker-1.internal:9464', 'worker-2.internal:9464']
```

- The endpoint has no default address and no config field: without `--metrics-listen` the worker serves no endpoint and starts no thread for it.
- The value is `HOST:PORT`, or `[IPV6]:PORT` for an IPv6 address. The host has 1 to 253 bytes; the port is 1 to 65535. The host is resolved when the endpoint binds. A value that does not parse is a usage error: exit code 2, before the worker starts.
- Each worker process has its own endpoint. Two workers on one host need two ports.

## The Endpoint

The worker serves the endpoint on an OS thread of its own, so a scrape never runs on the thread that claims tasks.

| Route | Answer |
|-------|--------|
| `GET /metrics` | The metrics of the worker, in the Prometheus text format 0.0.4 |
| `GET /livez` | 200 when the worker process is live, 503 when its claim loop is stuck |
| `GET /readyz` | 200 when the worker can claim, 503 otherwise |

- Each route also answers `HEAD`. Another path answers 404; another method answers 405.
- `/metrics` answers the content type `text/plain; version=0.0.4; charset=utf-8`. There is no OpenMetrics negotiation: a scraper that asks for OpenMetrics gets the 0.0.4 text.
- A probe answers one line of text that names the state.
- The endpoint has no authentication. Bind it to an address of the network of the scraper.

| Bound | Value |
|-------|-------|
| Connections at once | 16. A connection over it is closed with no answer |
| Time to read a request head | 5 seconds. Past it, the connection is closed with no answer |
| Bytes of a request | 8 KiB. A request head that does not fit is answered 431 |

### Liveness: `/livez`

| Phase of the worker | Answer |
|---------------------|--------|
| The start (children, database, listener, liveness lock) | 200: each step of the start has its own bound |
| The claim loop, which turned by its deadline | 200 |
| The claim loop, past the deadline of its next turn | 503: the worker process is stuck |
| The stop (the drain) | 200: the drain timeout bounds it |

Before each pass and before each wait, the claim loop sets the deadline of its next turn: the bound of the wait (the poll interval or the backoff delay) plus 30 seconds.

### Readiness: `/readyz`

| State | Answer |
|-------|--------|
| The claim loop runs, the pools have no failure, one or more slots have a serving child, and the worker holds its liveness lock | 200 |
| The worker starts, or stops | 503 |
| The pools failed | 503 |
| No slot has a serving child | 503 |
| The liveness lock is not held | 503, with the cause |

### When the Address Does Not Bind

A bind that fails never stops the worker. The worker starts and claims, and the endpoint binds again after a backoff: 1 second, doubled after each failed bind, at most 60 seconds. The probes answer only once the address is bound. The worker snapshot (`horsies_worker_states.telemetry`) shows the endpoint `binding`, `serving` with its address, or `down` with the address, the cause and the count of failed binds. `horsies.fleet.workers.telemetry_down{part="endpoint"}` counts the live workers whose endpoint is down.

At the stop, the endpoint serves until the drain of the worker ends, so the last scrape sees the attempts of the drain.

## When the Worker Records

| Metrics | Recorded |
|---------|----------|
| `horsies.task.queue.duration` | At each claim: from the due time of the row to its claim, both from the database clock |
| `horsies.task.slot_wait.duration` | When a child takes the task: from the claim result to the placement |
| `horsies.task.attempts`, `horsies.task.attempts.end_not_known`, `horsies.task.run.duration` | When the end of an attempt reaches the worker |
| `horsies.task.record.duration`, `horsies.task.retries` | When the write of the end returns |
| `horsies.task.claims.released`, `horsies.task.starts.refused` | When the worker releases a claim, or a child stops before it sets the task `RUNNING` |
| `horsies.finalize.retries` | When an outcome write is sent again |
| `horsies.recovery.tasks`, `horsies.recovery.pass.failures` | At each pass of the reaper |
| `horsies.worker.children.started`, `horsies.worker.children.replaced`, `horsies.worker.child.boot.duration` | When a child sends its ready frame, or a slot replaces its child |
| `horsies.worker.claim.passes` | At each pass of the claim loop |
| `horsies.worker.seats`, `horsies.worker.seats.in_use`, `horsies.worker.slots.down`, `horsies.worker.memory.rss{process="parent"}`, `horsies.worker.memory.rss{process="children"}` | At each worker snapshot, from the values that the snapshot writes: once per `recovery.worker_state_snapshot_interval_ms` (30 seconds by default), not at a scrape |

- `horsies.task.run.duration` runs from the placement of the attempt on a child to the end that the worker saw. It includes the transit of the run frame and the `RUNNING` write of the child.
- A duration above one day counts only in the `+Inf` bucket of its histogram. The longest task timeout is about 49.7 days.
- `horsies.worker.seats.in_use` counts, for the worker, the `RUNNING` rows and the `CLAIMED` rows with a live lease, at most the seats of the budget. It has no series after a count of the held rows that failed.
- `horsies.worker.memory.rss{process="children"}` is the sum of the last RSS reading of each task child (its ready frame, then each result frame), the value of the `children_memory_mb` column of the same snapshot. A child whose last reading is unreadable is not in the sum: the `children_rss` member of the `telemetry` column names it. A snapshot in which no child read its size leaves the last value. A replaced child's reading leaves the sum.
- `horsies.worker.info` is always 1. Its labels hold the version, the profile and the worker id.

## Label Rules

Every label takes values from a closed set that the worker knows at its start. A task id, a host name and an error message are never label values. `worker_id` is a label of `horsies.worker.info` only.

| Label | Values | Any other value |
|-------|--------|-----------------|
| `task` | The registered tasks of the app | `(unregistered)` |
| `queue` | The queues of the app config | `(unknown)` |
| `error_code` | `""` (no code: a completed attempt), every built-in code, and the codes that the task declares | `undeclared` |
| `handler` | The registered event handlers | `(unknown)` |
| `version`, `profile`, `worker_id` | The version, the profile and the worker id of the process: one value each | none |
| Every other label | The values of a closed enum of the core, listed on the [Metric Reference](/horsies/observability/metric-reference/) | none |

### Error Codes

The `error_code` label takes the codes that each task declares: Python `@app.task(..., error_codes=...)` and the codes of the exception mappers of the task and of the app; Rust `UserErrorCode::CODES` of the code type of the task, with no user work. A user code that the task does not declare counts as `undeclared` in this label only: the task error, the attempt record, the events, the spans and the log records keep the exact code. See [Declared Error Codes](/horsies/tasks/errors/#declared-error-codes).

### A Name That Cannot Be a Label Value

A task, queue or error code name that cannot be a value of its own is not declared, and its samples count in the series of the value for any other name:

- the empty name;
- the text of the value for any other name, for example a queue named `(unknown)` or a task named `(unregistered)`;
- a name after the first 100,000 names of its set, or an error code after the first 100,000 codes.

The start of the worker goes on. One WARNING line at the start names each such name with its set and its reason: at most 32 names, then the count of the others.

## Series Count

A series exists from its first sample. A metric holds at most 10,000 series in one worker. A sample of a new series past that bound is dropped and counted in `horsies.telemetry.series.dropped{metric}`.

The task metrics grow with the tasks of the worker. For a worker that runs `P` pairs of task and queue, each in one execution class:

| Metric | Series |
|--------|--------|
| `horsies.task.attempts` | `P × (1 + F + W)`: one `COMPLETED` series with no code, one per error code that ended an attempt `FAILED` (`F`) or `WORKER_FAILURE` (`W`) |
| `horsies.task.attempts.end_not_known` | `P` |
| `horsies.task.run.duration` | `P × 3`: one per outcome |
| `horsies.task.retries` | `P × R`: `R` the error codes that scheduled a retry |
| `horsies.task.queue.duration` | The queues × 3 |

One series of a histogram is 24 lines of the text: 21 buckets, `+Inf`, `_sum` and `_count`. The other worker metrics have a fixed bound, on the [Metric Reference](/horsies/observability/metric-reference/#series-bounds), and the two handler metrics have 2 series per event handler.

Example: 200 tasks on one queue, each in one class, each with one error code that fails it and `WORKER_CRASHED`, and a retry on its code. `horsies.task.attempts` has 200 × 3 = 600 series, `end_not_known` 200, `run.duration` 600 series (14,400 lines), `retries` 200: 1,600 series of task metrics in 15,400 lines.

When each task runs on one queue, the `queue` label adds no series: the task gives the queue. It adds bytes to each line, and a scrape config can drop it:

```yaml
scrape_configs:
  - job_name: horsies-worker
    static_configs:
      - targets: ['worker-1.internal:9464']
    metric_relabel_configs:
      - action: labeldrop
        regex: queue
```

A task sent to `N` queues has `N` times the series. Do not drop `queue` then: two series would get one label set.

## Things to Avoid

**Don't scrape the queue depth from each worker.** A worker records only its own attempts. The queue depth, the oldest pending age and the unserved classes are facts of the database: scrape them from one [fleet collector](/horsies/observability/fleet-collector/).

**Don't give two workers on one host the same port.** The second worker runs, but its endpoint stays down and binds again with a backoff.

**Don't use `/readyz` as a liveness probe.** A worker with every slot down, or one that waits for its liveness lock, is not ready, but it is not stuck. A restart does not help it.
