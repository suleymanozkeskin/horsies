---
title: OpenTelemetry Collector
section: shared
summary: Send the worker and fleet metrics to an OpenTelemetry backend - the Collector config in deploy/otel-collector/ scrapes the Prometheus endpoints of horsies and exports OTLP.
related: [metrics, fleet-collector, tracing, deployment]
tags: [observability, opentelemetry, collector, otlp, metrics]
---

The core serves its metrics as Prometheus endpoints and pushes nothing. For an OpenTelemetry pipeline, an OpenTelemetry Collector scrapes the endpoints with its `prometheus` receiver and exports the series through OTLP. The core needs no OpenTelemetry dependency for its metrics, and the Python client and the Rust client use one path.

The spans and the producer metrics do not take this path: the client records them with the OpenTelemetry SDK of your process. See [Tracing](/horsies/observability/tracing/).

## How To

`deploy/otel-collector/collector.yaml` in the repository is the config. It is made for `otel/opentelemetry-collector-contrib` 0.165.0, the image that the container test of the repository runs.

| Job | Process | Address in the file |
|-----|---------|---------------------|
| `horsies-worker` | Each worker: `horsies worker <app> --profile <name> --metrics-listen HOST:PORT` | `127.0.0.1:9464` |
| `horsies-fleet` | The fleet collector: `horsies metrics <app> --listen HOST:PORT` | `127.0.0.1:9465` |

1. Replace the addresses with the addresses of your processes:
   - Static targets: the receiver `prometheus`. Edit its `targets` lists.
   - Kubernetes pods: the receiver `prometheus/kubernetes`. Put it in place of `prometheus` in `service.pipelines.metrics.receivers`.
2. Set `OTLP_ENDPOINT` to the `HOST:PORT` of the OTLP gRPC receiver of your backend or of a gateway Collector.
3. Run the Collector:

```bash
docker run --rm --network host \
  -v "$PWD/deploy/otel-collector/collector.yaml:/etc/otelcol/collector.yaml:ro" \
  -e OTLP_ENDPOINT=otlp.example.internal:4317 \
  otel/opentelemetry-collector-contrib:0.165.0 --config=/etc/otelcol/collector.yaml
```

### The Pipeline of the File

| Part | Value |
|------|-------|
| Receiver `prometheus` | Static targets, scrape interval 15 s, timeout 10 s |
| Receiver `prometheus/kubernetes` | Pod discovery: a worker pod with the label `app.kubernetes.io/component: horsies-worker`, a fleet collector pod with `app.kubernetes.io/component: horsies-metrics`; the container port of the endpoint has the name `metrics` |
| Processor `memory_limiter` | Refuses data while the Collector is above its memory limit (`limit_percentage: 80`, `spike_limit_percentage: 20`, checked each second), so a burst does not end the process |
| Exporter `otlp_grpc` | OTLP over gRPC with TLS, to `${env:OTLP_ENDPOINT}`. For a receiver without TLS, add `tls: {insecure: true}` |

For the Kubernetes receiver, the service account of the Collector needs `get`, `list` and `watch` on pods. The receiver sets `k8s.namespace.name`, `k8s.pod.name` and the other Kubernetes resource attributes from the discovery labels.

The file expands environment variables as `${env:NAME}`. Write `$$` for a `$` of the Prometheus config, for example `$$1` in a relabel replacement.

## Metric Names in OTLP

The series keep their Prometheus names, for example `horsies_task_attempts_total` and `horsies_task_run_duration_seconds`: `trim_metric_suffixes` is off in the file. The receiver option `trim_metric_suffixes: true` removes the unit and `_total` suffixes. The OpenTelemetry name of each metric, for example `horsies.task.attempts`, is on the [Metric Reference](/horsies/observability/metric-reference/).

## Things to Avoid

**Don't scrape two fleet collectors of one database.** Each one sends each fleet series again, with a different `service.instance.id`. Run one fleet collector per database.

**Don't point the Collector at a worker that has no `--metrics-listen`.** The worker serves no endpoint without it.
