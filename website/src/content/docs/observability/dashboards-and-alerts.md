---
title: Dashboards and Alerts
section: shared
summary: The Grafana dashboard and the Prometheus alert rules in deploy/ of the repository - where they are, how to install them, and what each alert means.
related: [metrics, fleet-collector, metric-reference, deployment]
tags: [observability, grafana, prometheus, alerts, dashboard]
---

The repository holds a Grafana dashboard and a set of Prometheus alert rules for the metric catalog of the core. Both read the series of a Prometheus that scrapes the fleet collector and the metrics endpoint of each worker (`--metrics-listen`).

| File | Holds |
|------|-------|
| `deploy/grafana/horsies-dashboard.json` | The dashboard, uid `horsies-overview` |
| `deploy/prometheus/horsies-alerts.yaml` | The alert rules, group `horsies` |
| `deploy/prometheus/horsies-alerts.test.yaml` | The unit tests of the rules |

A test of the repository checks that each query, legend and variable of the dashboard, and each expression of the rules, names only metrics, labels and label values of the [Metric Reference](/horsies/observability/metric-reference/).

## The Dashboard

- Grafana 12.2 or later: the file has `schemaVersion` 42.
- Import it (Dashboards > New > Import), or provision it with a dashboard provider of type `file` whose `path` holds the file.
- Select the Prometheus data source in the variable `Data source`. The variables `queue` and `task` take the values of `horsies_task_attempts_total`; "All" also matches the queues that have no attempt yet.

| Row | Source | Panels |
|-----|--------|--------|
| Fleet | The [fleet collector](/horsies/observability/fleet-collector/) | Live workers, unserved classes, oldest pending task, failing fleet queries, workers with telemetry down, schema version; oldest pending task age; live tasks by status; fleet seats; workers by state; workflows; time since the last run of each schedule; fleet query time and failures |
| Tasks | The [worker endpoints](/horsies/observability/metrics/) | Attempts by outcome; not completed ratio by task; not completed attempts by error code; retries by error code; run time; queue wait; slot wait and outcome write; attempts with an unknown end |
| Workers | The worker endpoints | Worker seats; child slots down; children replaced by reason; child boot time; claim passes by result; claims released and starts refused; resident memory; recovery and outcome write retries; a table of the workers |
| Telemetry | The worker endpoints | Events dropped; samples dropped at the series bound; event handler errors and drops |

- The fleet panels take `max` over the fleet collectors, so a second collector does not double a value. The worker panels add the workers up.
- "Live tasks by status" shows "PENDING more than" with the lower bound for a queue and class past 1,000 pending tasks. Read the backlog from the age of the oldest pending task.

## The Alert Rules

Load the rules in Prometheus:

```yaml
rule_files:
  - horsies-alerts.yaml
```

| Alert | Fires when | For | Severity |
|-------|------------|-----|----------|
| `HorsiesOldestPendingTaskTooOld` | The oldest due `PENDING` task of a queue and execution class waits more than 300 s | 5m | warning |
| `HorsiesTaskFailureRatioHigh` | More than 10% of the attempts of a task failed in 10 minutes, with at least 10 attempts | 5m | warning |
| `HorsiesNoLiveWorker` | No worker has a snapshot inside the live window | 5m | critical |
| `HorsiesUnservedClass` | A queue and execution class has `PENDING` tasks, and no live worker serves it | 5m | critical |
| `HorsiesEventsDropped` | A worker dropped lifecycle events in the last 10 minutes | none | warning |
| `HorsiesTelemetryDown` | Live workers have their metrics endpoint or their handler child down | 5m | warning |
| `HorsiesFleetQueryFailed` | The last read of a query of the fleet collector failed or passed its time limit | 5m | warning |

### What Each Alert Means

**`HorsiesOldestPendingTaskTooOld`**: a task of this queue and class is due, and no worker claimed it for more than 5 minutes. The workers that serve the class are full, too slow, or gone. Check their seats in use, and `HorsiesUnservedClass`. The expression is `max by (queue, execution) (horsies_queue_oldest_pending_age_seconds) > 300`.

**`HorsiesTaskFailureRatioHigh`**: more than 10% of at least 10 attempts of a task ended `FAILED` or `WORKER_FAILURE` in 10 minutes. An attempt that an operator cancelled (`CANCELLED`) does not count as a failure. `horsies_task_attempts_total{outcome=~"FAILED|WORKER_FAILURE"}` by `error_code` shows the causes.

**`HorsiesNoLiveWorker`**: no worker wrote a snapshot inside the live window (120 seconds). No task is claimed until a worker starts. The fleet collector still answers, so the database is reachable.

**`HorsiesUnservedClass`**: these tasks stay `PENDING` until a worker whose profile serves the queue and the execution class starts.

**`HorsiesEventsDropped`**: the cause label tells why. `queue_full`: the handler child reads slower than the worker sends. `child_down`: the handler child is not running. Task outcomes do not change; the event handlers miss these events.

**`HorsiesTelemetryDown`**: the part label tells which part. `endpoint`: the metrics endpoint of the worker does not serve. `handler_child`: the event handler child does not run. The workers still run their tasks.

**`HorsiesFleetQueryFailed`**: the gauges of the query have no series while it fails, so the alerts on them stay quiet. The log of the fleet collector has the cause.

### Rules of the File

- The fleet series are aggregated with `max`, so two fleet collectors give one alert. The worker series are added up with `sum`.
- The rules do not name your scrape jobs. A target that does not answer has `up == 0`; add that rule with your job names.
- The thresholds are starting points. To give a queue its own threshold, add a second rule for it, and remove the queue from the first rule:

```yaml
- alert: HorsiesOldestPendingTaskTooOld
  expr: max by (queue, execution) (horsies_queue_oldest_pending_age_seconds{queue!="reports"}) > 300
  # for, labels and annotations as in horsies-alerts.yaml
- alert: HorsiesOldestPendingTaskTooOld
  expr: max by (queue, execution) (horsies_queue_oldest_pending_age_seconds{queue="reports"}) > 3600
  # for, labels and annotations as in horsies-alerts.yaml
```

### Check the Rules

```bash
promtool check rules deploy/prometheus/horsies-alerts.yaml
promtool test rules deploy/prometheus/horsies-alerts.test.yaml
```

The test file has a firing case and a quiet case for each alert.

## Things to Avoid

**Don't alert on `horsies_queue_tasks{status="PENDING"}`.** The count stops at 1,000 per queue and class. Alert on the oldest pending age, as `HorsiesOldestPendingTaskTooOld` does.

**Don't remove `max` from a fleet expression when you run two fleet collectors.** Each collector counts the whole fleet.
