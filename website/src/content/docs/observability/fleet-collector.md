---
title: Fleet Collector
section: shared
summary: The fleet metrics of one database - horsies metrics and GET /metrics of the web UI - with the read cadence, the bounded queue depth, the oldest pending age, the failed queries, and the label values.
related: [metrics, metric-reference, dashboards-and-alerts, opentelemetry-collector, ../monitoring/worker-health, ../cli, ../rust/cli]
tags: [observability, metrics, prometheus, fleet, queue depth]
---

The fleet collector reads the state of the whole fleet from PostgreSQL and serves it as Prometheus gauges: the live tasks of each queue, the age of the oldest pending task, the live workers and their seats, the classes that no worker serves, the workflows, the schedules and the schema version. These are facts of the database, not of one worker, so one collector serves them for the fleet.

## Why and When

Alert on a backlog and on a queue that no worker serves, and chart the size of the fleet. Run one collector per database: N workers that each served the queue depth would give N copies of one number.

## How To

Run the collector as a process of its own, or scrape the web UI:

| Where | Command or route |
|-------|------------------|
| Python | `horsies metrics <app> --metrics-listen HOST:PORT` |
| Rust | `<binary> metrics --metrics-listen HOST:PORT`, the app's own binary through `horsies::cli::run` |
| TypeScript | `<entry> metrics --metrics-listen HOST:PORT`, the app's own entry through `cli.run(app)` |
| Web UI | `GET /metrics` of `horsies web` and of a mounted monitoring app, in each client |

```bash
horsies metrics myapp.instance:app --metrics-listen 0.0.0.0:9465
```

```yaml
scrape_configs:
  - job_name: horsies-fleet
    static_configs:
      - targets: ['metrics.internal:9465']
```

- `--metrics-listen` is required and has no default: `HOST:PORT`, or `[IPV6]:PORT` for an IPv6 address. The host has 1 to 253 bytes; the port is 1 to 65535. The host is resolved when the server binds. A text that is not such an address is an argument error: exit code 2.
- The command serves `GET /metrics` (and `HEAD`). Another path answers 404, another method 405.
- `SIGTERM` or `SIGINT` stops it with exit code 0. The app does not load, the connection settings do not build, or the address does not bind: exit code 1.
- A database that does not answer never ends the command: the gauges of each failed query have no series until a read answers.

The `GET /metrics` route of the web UI answers only a request that the view verdict of the access policy allows. See [Web UI Deployment & Authentication](/horsies/monitoring/web-ui-deployment/).

## When the Collector Reads

Each read runs one statement per query, one after another, on one connection at a time. Each statement has a time limit of 2 seconds.

| Server | Reads |
|--------|-------|
| `horsies metrics` | At start, then at most once per 15 seconds. A scrape renders the last read and never starts a read, so the scrapes add no load on the database |
| `GET /metrics` of the web UI | On a request, when no read has run yet or the last read started 15 seconds or more before it. The request waits for that read. Any other request answers the last read. Reads never overlap, and nothing is read while nobody scrapes |

## The Gauges

The metrics, their labels and their values are on the [Metric Reference](/horsies/observability/metric-reference/#fleet-metrics). The rules of each gauge:

| Metric | Rule |
|--------|------|
| `horsies.queue.tasks{queue, execution, status}` | The live rows: `CLAIMED`, `RUNNING`, and `PENDING` for each queue and class whose count is exact |
| `horsies.queue.pending.more_than{queue, execution}` | A queue and class with more than 1,000 `PENDING` rows: the value is 1,000, a lower bound. Such a queue and class has no `PENDING` series in `horsies.queue.tasks` |
| `horsies.queue.oldest_pending.age{queue, execution}` | The wait of the oldest due `PENDING` row, in seconds |
| `horsies.fleet.workers{state}` | The live workers: `serving` with no slot down at their latest snapshot, `degraded` with a slot down |
| `horsies.fleet.workers.telemetry_down{part}` | The live workers whose latest snapshot records the part (`endpoint`, `handler_child`) as down |
| `horsies.fleet.seats{execution}` | The seats of the live workers, as their latest snapshots recorded them |
| `horsies.fleet.seats.in_use{execution}` | The seats that hold a task: per worker, `RUNNING` rows and `CLAIMED` rows with a live lease, at most the seats of the budget |
| `horsies.fleet.unserved_classes{queue, execution}` | One series per queue and class with `PENDING` rows: 1 when no live worker serves it, else 0 |
| `horsies.workflows{status}` | The `RUNNING` and `PAUSED` workflows |
| `horsies.schedule.last_run.age{schedule}` | The time since the last run of a schedule, in seconds. A schedule that never ran has no series |
| `horsies.schema.version` | The newest successful migration of the database |

A worker is live while its latest snapshot is at most 120 seconds old. `fleet.workers`, `fleet.seats`, `fleet.seats.in_use`, `fleet.workers.telemetry_down` and `workflows` have one series per label value, 0 included.

### The Queue Depth Is Bounded

The count of the `PENDING` rows of one queue and class stops at 1,000: a count reads one index entry per row, so an exact count of a large backlog costs the database a read per row at each collect. Past 1,000, `horsies.queue.pending.more_than` holds the lower bound, and `horsies.queue.tasks` has no `PENDING` series for that queue and class.

Use the age of the oldest pending task to measure a backlog. It has no bound.

### The Oldest Pending Age

- A row is due when its due time has passed. The due time of a `PENDING` row is its send time, the start time of a delayed send, or the time when its retry is due.
- The value is the wait of the oldest due row at the time of the read, from the database clock.
- A queue and class whose rows are all due later has no series: no row waits.
- The read finds the oldest due row of each priority of a queue and class, and the gauge takes the oldest of them: a task that waits behind tasks of a higher priority counts.

### A Query That Fails

A statement that fails, runs past its time limit, or gives an answer that the read refuses, leaves the gauges of its query with no series: never the value of an earlier read, and never 0. The other queries still answer.

| Metric | Value |
|--------|-------|
| `horsies.exporter.collect.failed{query}` | 1 while the last read of the query failed, 0 when it answered |
| `horsies.exporter.collect.errors{query}` | Counts each failed read |
| `horsies.exporter.collect.duration{query}` | The time of each read |

Each failed read is one log line at WARNING. Alert on `horsies_exporter_collect_failed == 1`: an alert on a gauge with no series stays quiet.

## Label Values

- `queue` takes the queues of the app config: `default` in `QueueMode.DEFAULT`, the custom queues in `QueueMode.CUSTOM`. `schedule` takes the schedule names of the schedule config.
- Every other name is `(unknown)`. One `(unknown)` series stands for every such name: counts add up, ages take the largest.
- A config name that cannot be a label value of its own counts in the `(unknown)` series too: the empty name, the text `(unknown)`, and a name after 100,000 names of its set. One WARNING line at the start names each such name with its reason (at most 32 names, then the count of the others). The start goes on.

## Two Collectors

Two collectors of one database give two copies of each fleet series, with different `instance` labels. Run one, or aggregate the fleet series with `max`. The dashboard and the alert rules of the repository take `max`. See [Dashboards and Alerts](/horsies/observability/dashboards-and-alerts/).

## The Endpoint

`horsies metrics` and `GET /metrics` of the web UI answer the Prometheus text format 0.0.4 (`text/plain; version=0.0.4; charset=utf-8`). There is no OpenMetrics negotiation: a scraper that asks for OpenMetrics gets the 0.0.4 text.

The server of `horsies metrics` has these bounds:

| Bound | Value |
|-------|-------|
| Connections at once | 16. A connection over it is closed with no answer |
| Time to read a request head | 5 seconds. Past it, the connection is closed with no answer |
| Bytes of a request | 8 KiB. A request head that does not fit is answered 431 |

The endpoint has no authentication. Bind it to an address of the network of the scraper.

## Things to Avoid

**Don't alert on the queue depth.** It stops at 1,000 per queue and class. Alert on `horsies_queue_oldest_pending_age_seconds`.

**Don't add up the fleet series of two collectors.** Each one counts the whole fleet. Take `max`.
