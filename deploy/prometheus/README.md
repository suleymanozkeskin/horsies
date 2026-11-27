# Prometheus alert rules

`horsies-alerts.yaml` holds the alert rules of horsies. `horsies-alerts.test.yaml` holds their unit tests.

## Scrape

Prometheus scrapes two kinds of target. The rules do not name the jobs.

```yaml
rule_files:
  - horsies-alerts.yaml

scrape_configs:
  - job_name: horsies-worker # horsies worker <app> --profile <name> --metrics-listen HOST:PORT
    static_configs:
      - targets: ['worker-1.internal:9464']
  - job_name: horsies-fleet # horsies metrics <app> --metrics-listen HOST:PORT
    static_configs:
      - targets: ['metrics.internal:9465']
```

## Alerts

| Alert | Fires when | For | Severity |
|---|---|---|---|
| `HorsiesOldestPendingTaskTooOld` | the oldest due PENDING task of a queue and execution class waits more than 300 s | 5m | warning |
| `HorsiesTaskFailureRatioHigh` | more than 10% of the attempts of a task did not complete in 10 minutes, with at least 10 attempts | 5m | warning |
| `HorsiesNoLiveWorker` | no worker has a snapshot inside the live window | 5m | critical |
| `HorsiesUnservedClass` | a queue and execution class has PENDING tasks, and no live worker serves it | 5m | critical |
| `HorsiesEventsDropped` | a worker dropped lifecycle events in the last 10 minutes | none | warning |
| `HorsiesTelemetryDown` | live workers have their metrics endpoint or their handler child down | 5m | warning |
| `HorsiesFleetQueryFailed` | the last run of a query of the fleet collector failed or passed its time limit | 5m | warning |

- The backlog alert uses the age of the oldest pending task. The queue depth (`horsies_queue_tasks{status="PENDING"}`) is exact only up to 1,000 per queue and class. Past that, `horsies_queue_pending_more_than` holds the lower bound.
- The fleet series are aggregated with `max`, so two fleet collectors give one alert.
- A failed fleet query leaves its gauges without series. The alerts on these gauges then stay quiet, and `HorsiesFleetQueryFailed` fires.
- A scrape target that does not answer has `up == 0`. That rule names your job names, so add it yourself.

## A threshold per queue

Give a queue its own threshold with a second rule, and remove that queue from the first rule:

```yaml
- alert: HorsiesOldestPendingTaskTooOld
  expr: max by (queue, execution) (horsies_queue_oldest_pending_age_seconds{queue!="reports"}) > 300
  # for, labels and annotations as in horsies-alerts.yaml
- alert: HorsiesOldestPendingTaskTooOld
  expr: max by (queue, execution) (horsies_queue_oldest_pending_age_seconds{queue="reports"}) > 3600
  # for, labels and annotations as in horsies-alerts.yaml
```

## Checks

```sh
promtool check rules deploy/prometheus/horsies-alerts.yaml
promtool test rules deploy/prometheus/horsies-alerts.test.yaml
```

- The `observability-stack` job of `.github/workflows/tests-full.yml` runs both commands with the pinned Prometheus image (v3.16.0). It also loads the rules into a Prometheus that scrapes a worker and the fleet collector, and checks that each rule evaluates without error.
- `clients/python/tests/unit/test_observability_deploy_files.py` checks each metric name, label and label value against `core/src/telemetry/catalog_snapshot.txt`, and checks that each alert has a firing case and a quiet case in `horsies-alerts.test.yaml`.
