---
title: Concurrency
summary: Worker, queue, and cluster-level concurrency controls.
related: [worker-architecture, ../../concepts/queue-modes, ../../configuration/app-config]
tags: [workers, concurrency, claiming, limits]
---

## Operational Notes

- **Priority ordering:** Within the same priority, FIFO ordering uses `enqueued_at`. When many tasks share the same timestamp, ordering can appear non-deterministic.
- **Soft-cap bursts:** With `prefetch_buffer > 0`, queue concurrency counts only RUNNING tasks, while each worker's local claim budget still counts its already-claimed rows. Short-lived bursts above a queue's nominal cap can occur during claim/dispatch.
- **Claim leases:** In soft-cap mode, claims can expire and be reclaimed. Workers must verify ownership before running user code.

## Concurrency Levels

<!-- todo:diagram-needed - Concurrency hierarchy -->

```text
┌─────────────────────────────────────────────────────────┐
│  Cluster Wide Cap (optional)                            │
│  Max in-flight tasks across all workers (RUNNING+CLAIMED)│
│                                                         │
│  ┌───────────────────────────────────────────────────┐  │
│  │  Per-Queue Concurrency (CUSTOM mode)              │  │
│  │  Max RUNNING per queue across cluster             │  │
│  │                                                   │  │
│  │  ┌─────────────────────────────────────────────┐  │  │
│  │  │  Per-Worker Concurrency                     │  │  │
│  │  │  Max concurrent tasks per worker            │  │  │
│  │  │  (equals --processes)                       │  │  │
│  │  └─────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

## Worker-Level: --processes

Each worker runs N child processes:

```bash
horsies worker myapp.instance:app --processes=8
```

- Limits concurrent task execution on this worker
- Each process handles one task at a time
- Default: CPU count

## Process Recycling: --max-tasks-per-child

Worker child processes are long-lived: created once, then reused for every task.
Memory that a child does not return to the OS — heap high-water from allocator
fragmentation, C-extension caches, genuine leaks — accumulates for the child's
lifetime. On memory-quota platforms (containers, PaaS dynos) this grows until the
process is killed.

`--max-tasks-per-child` recycles a child after it completes N tasks, returning
its memory to the OS. **Default: 100.**

```bash
# Default is 100; raise it for high-throughput / connection-constrained apps
horsies worker myapp.instance:app --processes=8 --max-tasks-per-child=500

# Disable recycling entirely (uses fork on Linux)
horsies worker myapp.instance:app --processes=8 --max-tasks-per-child=0
```

- **Default `100`.** Tune it for your deployment — there is no universal value
  (see [Choosing N](#choosing-n)).
- `0` disables recycling: children live for the worker's lifetime and the
  worker uses `fork` on Linux.
- `N` must be `>= 2`. Child warmup consumes one executor call, so `N=1` would
  exhaust a child before it runs a task; the CLI rejects values below 2
  (except `0`).

### Per-child and staggered

Recycling is **per child, not fleet-wide**. Each child has its own task counter
and is replaced individually when it reaches N — the other children keep
running. Children rotate independently, never in lockstep, so throughput stays
smooth (no synchronized restart).

### Start method: recycling forces spawn

Any non-zero value forces the `spawn` multiprocessing start method (the stdlib
budget is incompatible with `fork`), at initial startup **and** for every
recycle replacement. On Linux this replaces the default `fork`:

- Under `fork`, children are copy-on-write clones of the parent and share its
  imported modules in memory.
- Under `spawn`, each child starts fresh and **re-imports** the app. This raises
  baseline memory (no shared pages) and slows child startup.

Set `--max-tasks-per-child=0` to keep `fork` when recycling is not needed.

### Choosing N

Recycling cost scales as ~1/N: each recycle pays one child cold start (re-import
app, rebuild the child connection pool). Pick the **highest N whose peak memory
stays under the platform quota**. Use the `children_memory_mb` column in
[worker health](../monitoring/worker-health) snapshots to size it — it reports
the summed RSS of the executor children, which the parent-only `memory_usage_mb`
metric does not capture.

- Low N (e.g. 3): tight memory bound, high cold-start and connection churn.
- High N (e.g. 500): negligible per-task overhead, larger memory headroom needed.
- A high N is a backstop for slow accumulation; a fast leak (large retention per
  task) needs a low N or a fix in the task code.

## Memory Recycling: --max-memory-per-child-mb

`--max-tasks-per-child` bounds task count, not memory. The recycle point that
keeps RSS under quota depends on a child's RSS, not its task count, so a fixed
count under heterogeneous tasks recycles either too early or too late.

`--max-memory-per-child-mb` recycles a child by its **own resident memory**: each
child samples its RSS after every real task and, when at or above the threshold,
finishes the task, sends its result, and exits cleanly. The pool replaces only
that child — siblings and their in-flight tasks are untouched. **Disabled by
default.**

```bash
# Recycle any child once its RSS reaches 200 MB
horsies worker myapp.instance:app --processes=6 --max-memory-per-child-mb=200

# Combine with count recycling: a child recycles when either limit is reached
horsies worker myapp.instance:app --processes=6 \
  --max-tasks-per-child=500 --max-memory-per-child-mb=200
```

- **Disabled by default** (`None`). Any positive value enables it.
- **CPython-only.** It is built on private `ProcessPoolExecutor` internals; the
  worker refuses to start (loudly) on other interpreters or untested CPython
  minor versions.
- Forces the `spawn` start method, exactly like `--max-tasks-per-child`.
- It is a **retention guardrail, not a hard memory sandbox.** A task whose
  working set exceeds the threshold *while it is still running* is not
  interrupted; the recycle happens after it returns. Per-task peak memory is
  bounded by your task code, not by this knob.

### The threshold is checked against the warmed baseline at startup

There is no fixed minimum value — a constant cannot know your deployment's
warmed child baseline, which is set by your app (imports, ORM, connection pool),
not by Horsies. Instead, at startup each child samples its **settled** baseline
RSS (after `gc.collect()`), and the worker:

- **fails to start** if the threshold is at or below the warmed baseline — a
  sub-baseline threshold would recycle after *every* task, and a fresh child
  immediately re-warms to the same baseline, so this means the app does not fit
  the chosen per-child budget. The error names both numbers.
- **warns** if the threshold is within 80% of the baseline (little headroom for
  per-task working set).

### Sizing

Measure your warmed child baseline first, then leave headroom above it. As a
reference, a minimal app warms a child to roughly 75 MB (CPython 3.13, Linux)
before any per-task working set; your baseline is higher with a larger app and
connection pool. Size for the dyno/container quota:

```
(max_memory_per_child_mb + task_peak) × processes + parent RSS + DB/native/page-cache + margin
  < quota
```

The per-child peak is **`threshold + task_peak`**, not the threshold alone: RSS
is sampled *after* the task that crosses the threshold, so a child momentarily
holds the threshold plus that task's working set. Children can peak at the same
time, so multiply by `processes`. Raise headroom by lowering `--processes`, not
by setting the threshold past the quota.

Worked example — 1024 MB quota, 4 processes, ~80 MB task working set, ~90 MB
parent + overhead:

- `--max-memory-per-child-mb=300`: worst case `(300 + 80) × 4 + 90 ≈ 1610 MB`,
  over quota; peak RSS ~1249 MB — exceeds the 1024 MB quota (OOM-killed on a
  container, quota error on a PaaS). Each child stays bounded at ~300 MB; the
  breach is the process-count multiple, not a recycle failure.
- `--max-memory-per-child-mb=200`: worst case `(200 + 30) × 4 + 90 ≈ 1010 MB`;
  peak RSS ~719 MB, mean ~657 MB — within quota.

Solve for the threshold: `(quota − parent − margin) / processes − task_peak`, or
keep a high threshold and lower `--processes`.

Note that summing per-child RSS overcounts shared pages — the cgroup total is
usually lower than `processes × child_rss` because shared libraries and mmap'd
pages are charged once. Use the `children_memory_mb` column in
[worker health](../monitoring/worker-health) snapshots to observe real per-tree
footprint while tuning. For large task results, prefer returning a reference (row
id, object key) over the payload itself, so the result does not inflate the RSS
sampled at recycle time.

### Choosing: count, memory, or both

The two recycle knobs combine with **OR** semantics — a child exits when *either*
limit is reached:

```bash
# recycle at 500 tasks OR once RSS reaches 180 MB, whichever comes first
horsies worker myapp.instance:app --processes=6 \
  --max-tasks-per-child=500 --max-memory-per-child-mb=180
```

- `--max-memory-per-child-mb` is the primary guard for memory-quota deployments
  (containers, PaaS dynos): it tracks the RSS the quota charges.
- `--max-tasks-per-child` is a secondary backstop — an age limit for slow
  accumulation, connection churn, or state that does not surface as RSS in time.
  Set it high (e.g. `500`) rather than `0`.
- **Recommended default:** memory recycling on, count recycling high as a
  backstop, then tune the memory threshold from observed `children_memory_mb`
  under production-like load.

To run **memory-only**, disable count recycling explicitly (otherwise its
default `100` still applies):

```bash
horsies worker myapp.instance:app --processes=6 \
  --max-tasks-per-child=0 --max-memory-per-child-mb=180
```

## Queue-Level: max_concurrency

In CUSTOM mode, limit concurrent tasks per queue:

```python
from horsies.core.models.queues import CustomQueueConfig

config = AppConfig(
    queue_mode=QueueMode.CUSTOM,
    custom_queues=[
        CustomQueueConfig(
            name="api_calls",
            priority=1,
            max_concurrency=5,  # Max 5 tasks running cluster-wide
        ),
        CustomQueueConfig(
            name="low",
            priority=100,
            max_concurrency=2,
        ),
    ],
    broker=PostgresConfig(...),
)
```

`max_concurrency=None` declares the queue uncapped: no per-queue limit is
enforced and the claim pass skips the in-flight count query for that queue
(worker- and cluster-level limits still apply), mirroring
`cluster_wide_cap=None`. `max_concurrency=0` is a valid edge that pauses
claiming from the queue.

Use cases:

- Rate limiting external API calls
- Preventing database overload
- Resource isolation

## Cluster-Level: cluster_wide_cap

Limit total concurrent tasks across all workers:

```python
config = AppConfig(
    cluster_wide_cap=50,  # Max 50 tasks in-flight across entire cluster
    ...
)
```

**Important:** When `cluster_wide_cap` is set, the system operates in **hard cap mode** by default. This counts both RUNNING and CLAIMED tasks to enforce a strict limit. See [Hard Cap vs Soft Cap](#hard-cap-vs-soft-cap) for details.

Use cases:

- Database connection limits
- Shared resource constraints
- Cost control for cloud resources

## Hard Cap vs Soft Cap

Horsies provides two modes for enforcing concurrency limits:

### Hard Cap Mode (Default)

When `prefetch_buffer=0` (default), the system counts **RUNNING + CLAIMED** tasks when enforcing caps:

```python
config = AppConfig(
    cluster_wide_cap=50,
    prefetch_buffer=0,  # Default: hard cap mode
    ...
)
```

**Behavior:**
- Strict enforcement: exactly `cluster_wide_cap` tasks can be in-flight at once
- Fair distribution: fast workers aren't blocked by slow workers' prefetch queues
- No prefetch: workers only claim tasks they can immediately execute

**Why this matters:**

In production, task durations vary wildly. Without hard cap mode, a worker processing slow tasks (10s each) prefetches additional tasks while a worker processing fast tasks (100ms each) sits idle waiting for work. Hard cap mode ensures fair distribution.

### Soft Cap Mode (with Prefetch)

When `prefetch_buffer > 0`, queue concurrency counts **RUNNING** tasks, allowing workers to prefetch beyond their running capacity. A worker's local budget still subtracts already-claimed rows so it cannot hold more than `processes + prefetch_buffer` in total:

```python
config = AppConfig(
    prefetch_buffer=4,      # Allow prefetching up to 4 tasks beyond running
    claim_lease_ms=5000,    # Required: lease expires after 5 seconds
    ...
)
```

**Behavior:**
- Prefetch allowed: workers can claim tasks ahead of execution
- Lease expiry: prefetched claims expire and can be reclaimed by other workers
- Soft limit: actual concurrent tasks may briefly exceed the cap during claiming

**Important:** `cluster_wide_cap` and `prefetch_buffer > 0` cannot be combined. If you need a global cap, use hard cap mode.
**Important:** `claim_lease_ms` must be set when `prefetch_buffer > 0`. In hard cap mode (`prefetch_buffer = 0`), it is optional and overrides the default 60s lease.

**When to use soft cap:**
- Single-worker deployments where prefetch latency matters
- Uniform task durations where work imbalance isn't a concern

## Claiming Controls

### max_claim_batch

Limits claims per queue per claiming pass. The default is `0`, which means
the worker claims up to the available local/global capacity for that queue.
Set a positive value only when you want an explicit fairness cap per queue per
pass.

```bash
horsies worker myapp.instance:app --max-claim-batch=4
```

A positive cap prevents one worker from claiming all available tasks, ensuring
fairness in multi-worker deployments at the cost of more claim passes.

### max_claim_per_worker

Limits total CLAIMED (not yet running) tasks per worker:

```bash
horsies worker myapp.instance:app --max-claim-per-worker=10
```

**Default behavior** (when set to 0):
- Hard cap mode (`prefetch_buffer=0`): defaults to `--processes`
- Soft cap mode (`prefetch_buffer>0`): defaults to `--processes + prefetch_buffer`

**Why this flag exists:**

- **Safety valve**: Explicit hard ceiling per worker, independent of processes or prefetch. Useful for clamping unstable workers without changing app config.
- **Multi-tenant clusters**: Some workers may be "lightweight" (low memory) and need lower in-flight limits even with many processes.
- **Testing and rollout**: Dial down claim pressure without changing global config or per-queue caps.
- **Prevent runaway claiming**: In soft cap mode with high prefetch, limits how much a single worker can hoard.

## How Claiming Works

```sql
-- Simplified claim query
SELECT id FROM tasks
WHERE queue_name = :queue
  AND status = 'PENDING'
ORDER BY priority ASC, enqueued_at ASC
FOR UPDATE SKIP LOCKED
LIMIT :limit
```

Key behaviors:

1. **SKIP LOCKED**: Don't wait for locked rows, skip them
2. **Priority first**: Lower number = higher priority
3. **FIFO within priority**: Earlier `enqueued_at` wins
4. **Limit**: Respects batch size limits

## Concurrency Calculation

During each claim pass:

```python
# Hard cap mode (prefetch_buffer=0): local budget counts RUNNING + CLAIMED
# Soft cap mode (prefetch_buffer>0): queue caps count RUNNING, but local
# worker budget still subtracts already-claimed rows.
hard_cap_mode = prefetch_buffer == 0

if hard_cap_mode:
    in_flight_locally = count_running_and_claimed_for_worker()
    local_budget = processes - in_flight_locally
else:
    running_locally = count_running_for_worker()
    claimed_locally = count_claimed_for_worker()
    local_budget = processes + prefetch_buffer - running_locally - claimed_locally

# Global budget (if cluster_wide_cap set)
if cluster_wide_cap:
    # Hard cap mode always counts RUNNING + CLAIMED globally
    in_flight_globally = count_all_running_and_claimed()
    global_budget = cluster_wide_cap - in_flight_globally
    budget = min(local_budget, global_budget)
else:
    budget = local_budget

# Per-queue limits (CUSTOM mode)
for queue in queues:
    if queue.max_concurrency:
        if hard_cap_mode:
            in_flight_in_queue = count_running_and_claimed_in_queue(queue)
        else:
            in_flight_in_queue = count_running_in_queue(queue)
        queue_budget = queue.max_concurrency - in_flight_in_queue
        if max_claim_batch > 0:
            claim_count = min(budget, queue_budget, max_claim_batch)
        else:
            claim_count = min(budget, queue_budget)
```

## Advisory Locks

Claim passes take transaction-scoped advisory locks only when cap
accounting needs read-then-act serialization, scoped to the invariant:

- `cluster_wide_cap` set: one **global** key — the cap is global, so all
  claim passes serialize.
- per-queue `max_concurrency` (no cluster cap): one key **per capped
  queue** in the pass, acquired in sorted order. Workers claiming
  disjoint capped queues do not contend; a mixed capped/uncapped pass
  holds its capped queues' locks for the whole pass (locks are
  transaction-scoped).
- no caps: no lock — `FOR UPDATE SKIP LOCKED` alone makes concurrent
  claiming safe.

During a rolling deploy across this change, old workers hold the global
key while new workers hold per-queue keys; the two do not contend, so a
per-queue cap can briefly overshoot by up to one pass's batch until the
fleet is on one version.

## Example Configurations

### Single Worker, Simple

```python
config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(...),
)
```

```bash
horsies worker myapp.instance:app --processes=8
```

### Multi-Worker with Global Cap

```python
config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    cluster_wide_cap=20,
    broker=PostgresConfig(...),
)
```

```bash
# Machine 1
horsies worker myapp.instance:app --processes=8

# Machine 2
horsies worker myapp.instance:app --processes=8

# Total: 16 processes, but max 20 RUNNING tasks cluster-wide
```

### Priority Queues with Rate Limiting

```python
config = AppConfig(
    queue_mode=QueueMode.CUSTOM,
    custom_queues=[
        CustomQueueConfig(name="stripe", priority=1, max_concurrency=3),
        CustomQueueConfig(name="email", priority=50, max_concurrency=10),
        CustomQueueConfig(name="reports", priority=100, max_concurrency=2),
    ],
    cluster_wide_cap=15,
    broker=PostgresConfig(...),
)
```

## Monitoring

Worker logs concurrency config at startup:

```text
Concurrency config: processes=4, cluster_wide_cap=20, max_claim_per_worker=4, max_claim_batch=0
```

## Troubleshooting

### Tasks Not Being Claimed

Check:

- `cluster_wide_cap` reached?
- `max_concurrency` for queue reached?
- Tasks expired (`good_until`)?

### Uneven Distribution

Use the default `max_claim_batch=0`, increase `max_claim_per_worker` when
prefetching, or run more workers.

### Database Overload

Lower `cluster_wide_cap` or queue `max_concurrency`.
