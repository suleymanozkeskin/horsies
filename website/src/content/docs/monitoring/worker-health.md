---
title: Worker & Database Health
section: shared
summary: What the health reads answer - the database ping, the worker ping, worker state snapshots, and the execution class coverage and report - and what each result means.
related: [health-reads, ../rust/monitoring/health-reads, ../workers/heartbeats-recovery]
tags: [monitoring, health, liveness, worker]
---

The health reads answer three operational questions: is Postgres reachable, which workers are responsive right now, and what is each worker doing.

The API is on the client pages: [Health Reads](/horsies/monitoring/health-reads/) (Python) and [Health Reads](/horsies/rust/monitoring/health-reads/) (Rust).

## Mental model

| Read | Answers |
|------|---------|
| Database ping | Can this app reach Postgres right now? |
| Worker ping | Which workers can respond right now? |
| Worker states | What did the workers last report about themselves? |
| Class coverage | Does a live worker serve every class that has pending tasks? |
| Class report | How long do the pending tasks of each class wait, and how much capacity is free? |

The two pings are active probes at the time of the call. A worker state is the last snapshot that the worker reported, not a live check. A worker that the worker state read lists and the worker ping does not return is known but did not answer.

## Why and When

Use the database ping and the worker ping for active liveness at the time of the call: a Kubernetes readiness gate, a `/health` endpoint, or a check before a dispatch. Use the worker state reads for load and configuration introspection: dashboards and capacity decisions.

## Is Postgres alive?

The database ping runs `SELECT 1` through the live broker pool, so success also confirms that a pooled connection could be checked out. It reports the round-trip latency. A failure has the code `DB_PING_FAILED`.

## Which workers are alive?

The worker ping sends a ping to the workers and collects the replies. A reply proves that the worker's event loop is responsive **and** that the worker can reach Postgres: the reply travels through the worker's own connection. A ping can target one worker.

The ping stops at the first of these conditions:

- **A minimum number of replies:** it returns as soon as that many distinct workers replied. One reply is a fast liveness gate: a healthy fleet answers in milliseconds, and only a degraded fleet waits for the full timeout. This removes the latency floor of a high-frequency `/health` probe.
- **A target worker:** it returns on the first reply of that worker.
- **The timeout:** with no target and no minimum, it waits the full timeout and lists every worker that replied. The number of live workers is not known in advance.

## What is each worker doing?

The worker state read returns the latest snapshot of each worker, **including idle workers**. A snapshot holds the identity of the worker, its queue and concurrency configuration, its current running and claimed task counts, and its memory and CPU (psutil). The history read returns the snapshots of one worker, newest first.

### Which slots are down?

`slot_health` says whether any slot of the worker was down when it wrote the snapshot. A slot is down when its last child start failed for a cause that can clear, such as `EMFILE`. It takes no task, and the worker starts a new child for it after a growing wait. See [Child Start Failures](/horsies/workers/worker-architecture/#child-start-failures).

| `slot_health` | Meaning |
| ------------- | ------- |
| All slots serving | No slot was down |
| Slots down | The worker is degraded. It holds one entry per slot that was down, in slot order |

A down slot has its slot index, its cause (a resource shortage with its errno name, or a refused thread start), the error text of its last failed start (at most 2400 characters), the time it went down, and the number of failed starts since then (at least 1).

### Which classes does a worker serve?

`class_capacity` holds the claim budget of each execution class that the worker runs children for. A budget's capacity counts its children (`ISOLATED`) or seats (a shared class) in service, and the budget lists the classes whose rows it claims. A class that no budget serves is not claimed by the worker. See [Budgets per Execution Class](/horsies/workers/concurrency/#budgets-per-execution-class).

### Which profile does a worker run?

`profile` names the worker profile that the worker runs: the `--profile` value of `horsies worker`, or the profile of `WorkerStart` in the Rust client.

| `profile` | Meaning |
| --------- | ------- |
| Named | The name of the profile in `AppConfig.worker_profiles` |
| Not recorded | The snapshot holds no profile name. A worker of a build before schema migration 1004 wrote it |

## Does a live worker serve every pending class?

A pending stream is the `PENDING` rows of one queue and one execution class. A worker is live while its latest snapshot is at most `WORKER_STATE_LIVE_WINDOW_S` (120) seconds old. `RecoveryConfig.worker_state_snapshot_interval_ms` is at most half of that window (60,000 ms), so a running worker stays live when a snapshot write is late by up to half the window. A live worker serves a stream when the queue is one of its `queues` and one of its budgets serves the class.

The class coverage read gives each pending stream one state:

| State | Meaning |
| ----- | ------- |
| Served | Live workers serve the stream. The read counts the workers, and sums the capacity in service of the budget that serves the class on each of them |
| Without capacity | Live workers serve the stream, and every child or seat of their serving budgets was down at their snapshot. The read counts the workers |
| Unserved | No live worker serves the stream. Its rows wait until a worker that serves the class runs on the queue |

The read also returns its time (database time) and the number of live workers. The streams are ordered by queue name, then by execution class order. A delayed send and a retry that waits for its interval are `PENDING` too, so they count before they are due.

The read fails with `MONITORING_QUERY_FAILED` on a database error. It fails with `retryable=False` when the `class_capacity` of a live snapshot is unreadable or missing (the message names the worker), and when it reaches a bound: 20,000 snapshot rows inside the live window, or 3,000 pending streams.

### The worker logs it

Each reaper pass runs the same check, one check interval after the worker starts. The worker logs an unserved stream at error level and a stream without capacity at warning level, for example:

```
PENDING ISOLATED tasks on queue 'default' wait: no live worker serves this class. Start a worker with isolated children on this queue. A shared task that went back to the queue as ISOLATED also waits here.
```

Each worker logs one stream at most once per 5 minutes. A check that fails logs one warning per 5 minutes and changes nothing.

### What to do

- **Unserved `ISOLATED`**: start a worker with isolated children that serves the queue. A row of a shared class goes back to the queue as `ISOLATED` when its task, as the worker registered it, has another class; such rows need isolated children too.
- **Unserved shared class**: start a worker with children of that class, or with isolated children, that serves the queue.
- **Without capacity**: the slots of the serving workers are down. Read their `slot_health` ([Which slots are down?](#which-slots-are-down)) for the cause. Each slot boots again after a growing wait.

## How long do pending tasks wait, and what capacity is free?

The class report returns the pending streams with the wait of their oldest due row and their coverage state, and the budget load of each live worker.

- The wait of a stream is the oldest waiting row when at least one row is due: its due time and how long it has waited. When every row is due later, the wait is the next due time. A row is due when its `enqueued_at` (its send time, or the time its retry became due) is at or before the read.
- The live workers are ordered by worker id. `GET /api/workers/classes` returns the report.
- Each budget of a worker has its capacity and the classes it serves, from the worker's latest snapshot; its rows in flight (the worker's `RUNNING` rows and `CLAIMED` rows with a live lease of the classes it serves, counted at the read, as the claim counts them); and its free capacity (capacity minus rows in flight, at least 0; a prefetch buffer can hold more rows than the capacity).

The report fails as the coverage read does. It also fails when a live worker holds rows of a class that its recorded budgets do not serve, and when more than 10,000 (queue, class, priority) groups have `PENDING` rows.

Both reads use the claim index. The coverage read takes one index probe per pending (queue, class); the report takes one per (queue, class, priority), so its cost grows with the priorities in use, not with the backlog. With 98,000 pending rows over 5 queues, 3 classes and 100 priorities on PostgreSQL 17, the coverage read used 32 buffers and the stream heads of the report 2,379 buffers (6.5 ms).

## Snapshot Fields

| Field | Meaning |
|-------|---------|
| `worker_id` | Worker instance id |
| `snapshot_at` | When the snapshot was taken |
| `hostname` | Worker host |
| `pid` | Worker process id |
| `processes` | Child process count: the number of children of the worker profile |
| `max_claim_batch` | Claim batch size |
| `max_claim_per_worker` | Per-worker claim cap |
| `cluster_wide_cap` | Cluster-wide concurrency cap: `ClusterCap(tasks)` or `NoClusterCap()` |
| `queues` | Queues this worker serves |
| `queue_priorities` | Per-queue priorities (CUSTOM mode); empty when none is set |
| `queue_max_concurrency` | Per-queue concurrency (CUSTOM mode); empty when none is set |
| `recovery_config` | Recovery configuration snapshot, plus the embedded health payloads below; empty when none is set |
| `tasks_running` | RUNNING tasks at snapshot time |
| `tasks_claimed` | CLAIMED tasks at snapshot time |
| `memory_usage_mb` | Resident memory of the worker process, in MiB |
| `memory_percent` | Resident memory of the worker process, as a percent of the memory of the host |
| `cpu_percent` | CPU use of the worker process, in percent |
| `children_memory_mb` | Summed resident memory of the worker's task children: the last reading of each child, from its ready frame and its result frames. `None` when no child read its size |
| `worker_started_at` | Worker process start time |
| `slot_health` | See [Which slots are down?](#which-slots-are-down) |
| `class_capacity` | See [Which classes does a worker serve?](#which-classes-does-a-worker-serve) |
| `profile` | See [Which profile does a worker run?](#which-profile-does-a-worker-run) |

### Embedded health payloads

`recovery_config` carries four operational payloads beside the
configuration snapshot, refreshed with each snapshot:

- `partition_coverage` — the last coverage-ensure outcome: `state`
  (`ensured` / `failed`); when ensured, `history_covered_through` and
  `heartbeats_covered_through`; on failure the stage, class, and typed
  refusal verbatim. `history_covered_through` is
  `{"kind": "UNTIL", "at": <instant>}`, the first instant at which a
  history retention class has no partition, or `{"kind": "FOREVER"}` when
  only the `forever` class has partitions. `heartbeats_covered_through` is
  the end of the last heartbeat partition. Coverage health fails
  **before** fewer than two future partitions remain, so the alarm
  precedes the lapse.
- `partition_pruning` — the last pruning pass:
  `{"finalized": int, "detached": int, "dropped": int, "refusals": [...],
  "errors": [...]}`. `refusals` and `errors` are reason-carrying lists,
  not counts — a skipped partition arrives with the reason it was
  skipped, and remains a candidate on the next pass.
- `phase2_recovery` — the crashed-worker progression pass summary
  (considered / applied / retained / failed counts, quarantined rows,
  `raised_counted` rows whose progression raised a persistent error, and
  the quarantine function's refusals verbatim).
- `workflow_recovery` — the last workflow recovery pass: `recovered`,
  `errors`, and per case the rows selected, refusals and errors. The
  top-level `errors` adds every case and the failures that no case counted.
  `failures_counted` and `limit_reached` per case show the
  [recovery limit](/horsies/configuration/recovery-config/#workflow-recovery-limit).
  `over_failure_limit` counts the `RUNNING` workflows at or over the limit:
  a live node holds such a workflow, and so does a limit write that fails.

`memory_usage_mb` is the **parent** process only. Task code runs in the worker's
children, so per-child memory growth is invisible there; `children_memory_mb`
captures it. The platform memory quota (e.g. Heroku R14) is measured against the
whole process tree. `children_memory_mb` sums RSS, so with a
[preload parent](/horsies/workers/worker-architecture/#preload-parent) it counts each
shared page once per child, and it reads above real use. Use it to see one child
grow, and read the container or cgroup total for the footprint that the quota
enforces. A child whose reading is unreadable is not in the sum; the
`children_rss` member of the stored `telemetry` column names its slot, its kind
and its pid. To bound it, see
[process recycling](/horsies/workers/concurrency/#process-recycling).
