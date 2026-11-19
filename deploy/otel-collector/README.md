# OpenTelemetry Collector config

`collector.yaml` scrapes the Prometheus endpoints of horsies with the `prometheus` receiver, and exports the series through OTLP over gRPC (`otlp_grpc`).

The file is made for `otel/opentelemetry-collector-contrib` 0.165.0. The container test pins this image (`clients/python/tests/fixtures/observability/compose.yaml`).

## Targets

| Job | Process | Address in the file |
|---|---|---|
| `horsies-worker` | each worker: `horsies worker <app> --profile <name> --metrics-listen HOST:PORT` | `127.0.0.1:9464` |
| `horsies-fleet` | the fleet collector: `horsies metrics <app> --listen HOST:PORT` | `127.0.0.1:9465` |

The worker endpoint has no default address. Replace the addresses with the addresses of your processes.

- Static targets: receiver `prometheus`. Edit its `targets` lists.
- Kubernetes pods: receiver `prometheus/kubernetes`. Put it in place of `prometheus` in `service.pipelines.metrics.receivers`.
  - Give each worker pod the label `app.kubernetes.io/component: horsies-worker`.
  - Give the fleet collector pod the label `app.kubernetes.io/component: horsies-metrics`.
  - Give the container port of the endpoint the name `metrics`.
  - Give the service account of the Collector `get`, `list` and `watch` on pods.

Run one fleet collector for each database. Each additional fleet collector sends each fleet series again.

## Export

Set `OTLP_ENDPOINT` to the `HOST:PORT` of the OTLP gRPC receiver of your backend or of a gateway Collector. The exporter uses TLS. For a receiver without TLS, add `tls: {insecure: true}` to `exporters.otlp_grpc`.

## Metric names

The series keep their Prometheus names, for example `horsies_task_attempts_total` and `horsies_task_run_duration_seconds`. The names, kinds and labels are in `core/src/telemetry/catalog_snapshot.txt`. The receiver option `trim_metric_suffixes: true` removes the unit and `_total` suffixes.

## Run

```sh
docker run --rm --network host \
  -v "$PWD/deploy/otel-collector/collector.yaml:/etc/otelcol/collector.yaml:ro" \
  -e OTLP_ENDPOINT=otlp.example.internal:4317 \
  otel/opentelemetry-collector-contrib:0.165.0 --config=/etc/otelcol/collector.yaml
```

## Test

`clients/python/tests/e2e/test_observability_stack.py` runs this file with a file exporter in place of `otlp_grpc`, and checks the OTLP names and data types of the worker and fleet series. CI runs it in the `observability-stack` job of `.github/workflows/tests-full.yml`.
