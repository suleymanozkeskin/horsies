---
title: Observability Deployment
section: shared
summary: Deploy the metrics of horsies on Kubernetes and with docker compose - workers with --metrics-listen and their probes, one fleet collector, Prometheus with the alert rules, and the OpenTelemetry Collector.
related: [overview, metrics, fleet-collector, opentelemetry-collector, dashboards-and-alerts]
tags: [observability, deployment, kubernetes, docker compose, prometheus]
---

Each example runs the same parts:

| Part | Process | Port in the examples |
|------|---------|----------------------|
| Workers | `horsies worker <app> --profile <name> --metrics-listen 0.0.0.0:9464` | 9464 |
| One fleet collector | `horsies metrics <app> --listen 0.0.0.0:9465` | 9465 |
| Prometheus | Scrapes both, and loads `deploy/prometheus/horsies-alerts.yaml` | 9090 |
| OpenTelemetry Collector | Scrapes both, and exports OTLP with `deploy/otel-collector/collector.yaml` | none |

Use Prometheus, the Collector, or both. The image `registry.example.com/myapp:1.0` holds your app and horsies, and the app reads its database URL from `DATABASE_URL`. A Rust app runs its own binary in place of `horsies`: `/app/myapp worker --profile default --metrics-listen 0.0.0.0:9464` and `/app/myapp metrics --listen 0.0.0.0:9465`.

## Kubernetes

### Workers

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: horsies-worker
spec:
  replicas: 3
  selector:
    matchLabels:
      app.kubernetes.io/name: myapp
      app.kubernetes.io/component: horsies-worker
  template:
    metadata:
      labels:
        app.kubernetes.io/name: myapp
        app.kubernetes.io/component: horsies-worker
    spec:
      # Longer than the drain_timeout of the profile (300 s by default).
      terminationGracePeriodSeconds: 330
      containers:
        - name: worker
          image: registry.example.com/myapp:1.0
          command: ["horsies", "worker", "myapp.instance:app", "--profile", "default",
                    "--metrics-listen", "0.0.0.0:9464"]
          env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: myapp-database
                  key: url
          ports:
            - name: metrics
              containerPort: 9464
          livenessProbe:
            httpGet:
              path: /livez
              port: metrics
            periodSeconds: 15
            failureThreshold: 4
          readinessProbe:
            httpGet:
              path: /readyz
              port: metrics
            periodSeconds: 10
```

- The label `app.kubernetes.io/component: horsies-worker` and the port name `metrics` are what the Kubernetes receiver of `deploy/otel-collector/collector.yaml` selects.
- `/livez` answers 503 only when the claim loop of the worker is stuck. During the start and the drain it answers 200.
- `/readyz` answers 503 while the worker starts or stops, while no child serves, and while the worker does not hold its liveness lock. A rollout waits for it.
- The probes answer only once the address is bound. A worker whose port does not bind runs its tasks, but its liveness probe fails, and the kubelet restarts it.

### The Fleet Collector

Run one per database:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: horsies-metrics
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: myapp
      app.kubernetes.io/component: horsies-metrics
  template:
    metadata:
      labels:
        app.kubernetes.io/name: myapp
        app.kubernetes.io/component: horsies-metrics
    spec:
      containers:
        - name: metrics
          image: registry.example.com/myapp:1.0
          command: ["horsies", "metrics", "myapp.instance:app", "--listen", "0.0.0.0:9465"]
          env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: myapp-database
                  key: url
          ports:
            - name: metrics
              containerPort: 9465
```

### Prometheus

A scrape config that finds the pods by their labels and their port name, with the alert rules:

```yaml
rule_files:
  - /etc/prometheus/rules/horsies-alerts.yaml

scrape_configs:
  - job_name: horsies-worker
    kubernetes_sd_configs:
      - role: pod
        selectors:
          - role: pod
            label: app.kubernetes.io/component=horsies-worker
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_container_port_name]
        regex: metrics
        action: keep
  - job_name: horsies-fleet
    kubernetes_sd_configs:
      - role: pod
        selectors:
          - role: pod
            label: app.kubernetes.io/component=horsies-metrics
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_container_port_name]
        regex: metrics
        action: keep
```

The service account of Prometheus needs `get`, `list` and `watch` on pods.

### The OpenTelemetry Collector

Run the Collector with the file of the repository, plus a second file that selects its Kubernetes receiver. The Collector merges the files in order:

```yaml
# kubernetes-receiver.yaml
service:
  pipelines:
    metrics:
      receivers: [prometheus/kubernetes]
```

```bash
kubectl create configmap otel-collector-config \
  --from-file=collector.yaml=deploy/otel-collector/collector.yaml \
  --from-file=kubernetes-receiver.yaml=kubernetes-receiver.yaml
```

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: otel-collector
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: otel-collector-pods
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: otel-collector-pods
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: otel-collector-pods
subjects:
  - kind: ServiceAccount
    name: otel-collector
    namespace: default
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: otel-collector
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: otel-collector
  template:
    metadata:
      labels:
        app.kubernetes.io/name: otel-collector
    spec:
      serviceAccountName: otel-collector
      containers:
        - name: collector
          image: otel/opentelemetry-collector-contrib:0.165.0
          args:
            - --config=/etc/otelcol/collector.yaml
            - --config=/etc/otelcol/kubernetes-receiver.yaml
          env:
            - name: OTLP_ENDPOINT
              value: otlp.example.internal:4317
          volumeMounts:
            - name: config
              mountPath: /etc/otelcol
              readOnly: true
      volumes:
        - name: config
          configMap:
            name: otel-collector-config
```

Run one Collector replica: two replicas scrape each target twice.

## Docker Compose

```yaml
# compose.yaml
services:
  postgres:
    image: postgres:18
    environment:
      POSTGRES_USER: horsies
      POSTGRES_PASSWORD: horsies
      POSTGRES_DB: horsies

  worker:
    image: registry.example.com/myapp:1.0
    command: ["horsies", "worker", "myapp.instance:app", "--profile", "default",
              "--metrics-listen", "0.0.0.0:9464"]
    environment:
      DATABASE_URL: postgresql://horsies:horsies@postgres:5432/horsies
    deploy:
      replicas: 2
    # Longer than the drain_timeout of the profile (300 s by default).
    stop_grace_period: 330s
    depends_on: [postgres]

  metrics:
    image: registry.example.com/myapp:1.0
    command: ["horsies", "metrics", "myapp.instance:app", "--listen", "0.0.0.0:9465"]
    environment:
      DATABASE_URL: postgresql://horsies:horsies@postgres:5432/horsies
    depends_on: [postgres]

  prometheus:
    image: prom/prometheus:v3.16.0
    command: ["--config.file=/etc/prometheus/prometheus.yml"]
    ports: ["9090:9090"]
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - ./deploy/prometheus/horsies-alerts.yaml:/etc/prometheus/rules/horsies-alerts.yaml:ro

  collector:
    image: otel/opentelemetry-collector-contrib:0.165.0
    command: ["--config=/etc/otelcol/collector.yaml", "--config=/etc/otelcol/compose-targets.yaml"]
    environment:
      OTLP_ENDPOINT: otlp.example.internal:4317
    volumes:
      - ./deploy/otel-collector/collector.yaml:/etc/otelcol/collector.yaml:ro
      - ./compose-targets.yaml:/etc/otelcol/compose-targets.yaml:ro
```

The worker replicas share one service name. Each scraper finds every replica through the DNS of that name:

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

rule_files:
  - /etc/prometheus/rules/horsies-alerts.yaml

scrape_configs:
  - job_name: horsies-worker
    dns_sd_configs:
      - names: [worker]
        type: A
        port: 9464
  - job_name: horsies-fleet
    static_configs:
      - targets: ['metrics:9465']
```

```yaml
# compose-targets.yaml: replaces the static targets of the receiver `prometheus`
receivers:
  prometheus:
    config:
      scrape_configs:
        - job_name: horsies-worker
          dns_sd_configs:
            - names: [worker]
              type: A
              port: 9464
        - job_name: horsies-fleet
          static_configs:
            - targets: ['metrics:9465']
```

The Collector merges the second file into the first: the `scrape_configs` list of the second replaces the list of the first, and the other settings of the first stay.

## Things to Avoid

**Don't run the fleet collector in each worker pod.** Each copy counts the whole fleet: run one per database.

**Don't set a liveness probe on `/readyz`.** A worker that waits for its liveness lock, or whose slots are down, is not ready, but a restart does not help it.

**Don't give the pod a grace period shorter than the drain.** The kubelet kills the worker at the end of the grace period, and the attempts that still run go to recovery.
