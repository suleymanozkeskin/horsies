# Grafana dashboard

`horsies-dashboard.json` (uid `horsies-overview`) shows the fleet, the tasks, the workers and the telemetry of horsies.

- Grafana 12.2 or later: the file has `schemaVersion` 42. CI loads it into Grafana 13.2.6.
- Data source: a Prometheus that scrapes the worker endpoints and the fleet collector (see `deploy/prometheus/README.md`). Select it in the `Data source` variable.
- Variables `queue` and `task`: the values of `horsies_task_attempts_total`. "All" also matches the queues that have no attempt yet.

| Row | Source | Panels |
|---|---|---|
| Fleet | the fleet collector (`horsies metrics`) | live workers, unserved classes, oldest pending task, failing fleet queries, workers with telemetry down, schema version; oldest pending task age; live tasks by status; fleet seats; workers by state; workflows; time since the last run of each schedule; fleet query time and failures |
| Tasks | the worker endpoints | attempts by outcome; not completed ratio; error codes; retries; run time; queue wait; slot wait and outcome write; attempts with an unknown end |
| Workers | the worker endpoints | seats; child slots down; children replaced; child boot time; claim passes; released claims and refused starts; memory; recovery; a table of the workers |
| Telemetry | the worker endpoints | events dropped; samples dropped at the series bound; event handler errors and drops |

- The fleet panels take `max` over the fleet collectors, so a second fleet collector does not double a value. The worker panels add the workers up.
- The queue depth is exact up to 1,000 per queue and class. Past that, "Live tasks by status" shows "PENDING more than" with the lower bound. The backlog is the age of the oldest pending task.

## Install

- Import: Dashboards > New > Import, then upload the file.
- Provisioning: a dashboard provider of type `file` whose `path` holds this file.

## Checks

- `clients/python/tests/unit/test_observability_deploy_files.py` checks that each query, legend and variable names only series, labels and label values of `core/src/telemetry/catalog_snapshot.txt`, and that `schemaVersion` is 42.
- No standalone tool checks a dashboard JSON against its schema version. The `observability-stack` job of `.github/workflows/tests-full.yml` loads the file into Grafana 13.2.6 through provisioning, and runs each query against a Prometheus that scrapes a worker and the fleet collector.
