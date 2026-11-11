---
title: Concurrency
summary: Worker, queue, and cluster-level concurrency controls.
related: [worker-architecture, ../../concepts/queue-modes, ../../configuration/app-config]
tags: [workers, concurrency, claiming, limits]
---

## Operational Notes

- **Priority ordering:** Priority holds within one execution class. Each (queue, class) is its own claim stream in `(priority, enqueued_at, id)` order. A budget that claims more than one class reads the merge of their streams in that order. Two budgets do not compare priorities: a high-priority `SHARED_ASYNC` row does not take a seat from an `ISOLATED` row. Within the same priority, FIFO ordering uses `enqueued_at`. When many tasks share the same timestamp, ordering can appear non-deterministic.
- **Soft-cap overshoot:** With `prefetch_buffer > 0`, queue concurrency counts only RUNNING tasks; buffered CLAIMED tasks are invisible to the cap and are dispatched without a re-check. A queue's RUNNING count can therefore exceed its nominal cap, the excess lasts for the duration of those tasks (not just the claim pass), and with multiple workers each pass grants against the same RUNNING snapshot — N workers can reach up to N × cap RUNNING, bounded by their aggregate seats. Use hard cap mode (`prefetch_buffer = 0`) where the cap must hold strictly.
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
│  │  │  (equals the seats of the profile)          │  │  │
│  │  └─────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

## Worker-Level: Children

Each worker runs the children of its [worker profile](/horsies/workers/worker-profiles/):

```python
eight_children = WorkerProfile(isolated=IsolatedChildren(count=8))
```

```bash
horsies worker myapp.instance:app --profile default
```

- Limits concurrent task execution on this worker
- Each isolated child handles one task at a time
- `count` is required, from 1 to 1024
- A shared async child handles up to `seats` `SHARED_ASYNC` tasks at once, and a shared thread child up to `seats` `SHARED_THREAD` tasks. The tasks of a worker at once are its seats: one per isolated child, and `seats` per shared child. See [Shared Async Tasks](/horsies/workers/shared-async-tasks/) and [Shared Thread Tasks](/horsies/workers/shared-thread-tasks/)

## Process Recycling

Worker child processes are long-lived: created once, then reused for every task.
Memory that a child does not return to the OS — heap high-water from allocator
fragmentation, C-extension caches, genuine leaks — accumulates for the child's
lifetime. On memory-quota platforms (containers, PaaS dynos) this grows until the
process is killed.

`IsolatedRecycle.after` recycles a child after it completes N tasks, returning
its memory to the OS. **Default: `TaskLimit(tasks=100)`.**

```python
# The default is 100; raise it for high-throughput or connection-constrained apps
recycle_after_500 = WorkerProfile(
    isolated=IsolatedChildren(
        count=8,
        recycle=IsolatedRecycle(after=TaskLimit(tasks=500)),
    ),
)

# Disable recycling entirely
no_recycling = WorkerProfile(
    isolated=IsolatedChildren(
        count=8,
        recycle=IsolatedRecycle(after=NoTaskLimit()),
    ),
)
```

- **Default `TaskLimit(tasks=100)`.** Tune it for your deployment — there is no
  universal value (see [Choosing N](#choosing-n)).
- `NoTaskLimit()` disables recycling: children live for the worker's lifetime.
- `tasks` must be `>= 1`. With `TaskLimit(tasks=1)`, each task runs in a new child.

### Per-child and staggered

Recycling is **per child, not fleet-wide**. Each child has its own task counter
and is replaced individually when it reaches N — the other children keep
running. Children rotate independently, never in lockstep, so throughput stays
smooth (no synchronized restart).

### Child startup

On Linux, a [preload parent](/horsies/workers/worker-architecture/#preload-parent) forks each child,
so a recycle costs milliseconds and children share library pages. With `exec`,
which other platforms always use, each child is a new Python interpreter that
imports everything: a recycle then costs a full import.

### Choosing N

Recycling cost scales as ~1/N: each recycle pays one child start and rebuilds the
child connection pool. On Linux a preload parent makes that start cheap: with 10,000
no-op tasks and 8 children, `N=100` drains 803 tasks/s against 853 with recycling
off. With `exec` the same test drains 437 against 819. Pick the **highest N whose
peak memory stays under the platform quota**. Use the `children_memory_mb` column in
[worker health](/horsies/monitoring/worker-health/) snapshots to watch growth — it reports
the summed RSS of the worker's children, which the parent-only `memory_usage_mb`
metric does not capture. With preload, that sum counts shared pages once per child,
so size against the container or cgroup total.

- Low N (e.g. 3): tight memory bound, high cold-start and connection churn.
- High N (e.g. 500): negligible per-task overhead, larger memory headroom needed.
- A high N is a backstop for slow accumulation; a fast leak (large retention per
  task) needs a low N or a fix in the task code.

## Memory Recycling

`IsolatedRecycle.after` bounds task count, not memory. The recycle point that
keeps RSS under quota depends on a child's RSS, not its task count, so a fixed
count under heterogeneous tasks recycles either too early or too late.

`IsolatedRecycle.above` recycles a child by its **own resident memory**: each
child reports its RSS with every result. When that value is at or above the
limit, the worker stops the child and starts a new one in that slot. Other
children and their in-flight tasks are untouched. **Disabled by default.**

```python
# Recycle any child once its RSS reaches 200 MB
memory_recycling = WorkerProfile(
    isolated=IsolatedChildren(
        count=6,
        recycle=IsolatedRecycle(above=MemoryLimit(mb=200)),
    ),
)

# Combine with count recycling: a child recycles when either limit is reached
count_and_memory = WorkerProfile(
    isolated=IsolatedChildren(
        count=6,
        recycle=IsolatedRecycle(
            after=TaskLimit(tasks=500),
            above=MemoryLimit(mb=200),
        ),
    ),
)
```

- **Disabled by default** (`NoMemoryLimit()`). `MemoryLimit(mb=N)` with `N >= 1` enables it.
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
connection pool. RSS counts pages that a forked child shares with the preload
parent, so read `MemoryLimit.mb` against RSS, and use the cgroup total or
PSS for the real footprint. Size for the dyno/container quota:

```
(MemoryLimit.mb + task_peak) × count + parent RSS + DB/native/page-cache + margin
  < quota
```

The per-child peak is **`threshold + task_peak`**, not the threshold alone: RSS
is sampled *after* the task that crosses the threshold, so a child momentarily
holds the threshold plus that task's working set. Children can peak at the same
time, so multiply by `count`. Raise headroom by lowering `count`, not
by setting the threshold past the quota.

Worked example — 1024 MB quota, 4 processes, ~80 MB task working set, ~90 MB
parent + overhead:

- `MemoryLimit(mb=300)`: worst case `(300 + 80) × 4 + 90 ≈ 1610 MB`,
  over quota; peak RSS ~1249 MB — exceeds the 1024 MB quota (OOM-killed on a
  container, quota error on a PaaS). Each child stays bounded at ~300 MB; the
  breach is the process-count multiple, not a recycle failure.
- `MemoryLimit(mb=200)`: worst case `(200 + 30) × 4 + 90 ≈ 1010 MB`;
  peak RSS ~719 MB, mean ~657 MB — within quota.

Solve for the threshold: `(quota − parent − margin) / count − task_peak`, or
keep a high threshold and lower `count`.

Note that summing per-child RSS overcounts shared pages — the cgroup total is
usually lower than `count × child_rss` because shared libraries and mmap'd
pages are charged once. Use the `children_memory_mb` column in
[worker health](/horsies/monitoring/worker-health/) snapshots to observe real per-tree
footprint while tuning. For large task results, prefer returning a reference (row
id, object key) over the payload itself, so the result does not inflate the RSS
sampled at recycle time.

### Choosing: count, memory, or both

The two recycle limits combine with **OR** semantics — a child exits when *either*
limit is reached:

```python
# recycle at 500 tasks OR once RSS reaches 180 MB, whichever comes first
either_limit = WorkerProfile(
    isolated=IsolatedChildren(
        count=6,
        recycle=IsolatedRecycle(
            after=TaskLimit(tasks=500),
            above=MemoryLimit(mb=180),
        ),
    ),
)
```

- `above` is the primary guard for memory-quota deployments
  (containers, PaaS dynos): it tracks the RSS the quota charges.
- `after` is a secondary backstop — an age limit for slow
  accumulation, connection churn, or state that does not surface as RSS in time.
  Set it high (e.g. `TaskLimit(tasks=500)`) rather than `NoTaskLimit()`.
- **Recommended default:** memory recycling on, count recycling high as a
  backstop, then tune the memory threshold from observed `children_memory_mb`
  under production-like load.

At boot the worker warns when `count × MemoryLimit.mb` is above the memory that
it can use: the container (cgroup) limit when it is below the host RAM, else the
host RAM. It starts in every case. See [Worker Profiles](/horsies/workers/worker-profiles/#memory).

To run **memory-only**, disable count recycling explicitly (otherwise its
default `TaskLimit(tasks=100)` still applies):

```python
memory_only = WorkerProfile(
    isolated=IsolatedChildren(
        count=6,
        recycle=IsolatedRecycle(after=NoTaskLimit(), above=MemoryLimit(mb=180)),
    ),
)
```

## Queue-Level: max_concurrency

In CUSTOM mode, limit concurrent tasks per queue:

<!-- check: skip elided -- ... stands for a config shown on other pages -->
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

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
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

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
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

When `prefetch_buffer > 0`, queue concurrency counts **RUNNING** tasks, allowing workers to prefetch beyond their running capacity. A worker's local budget still subtracts already-claimed rows so it cannot hold more than its seats + `prefetch_buffer` in total:

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
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
- Soft limit: RUNNING tasks can exceed the cap — buffered CLAIMED tasks don't count against it and dispatch without a re-check, so the excess persists while those tasks run and scales with worker count (see [Soft-cap overshoot](#operational-notes))

**Important:** `cluster_wide_cap` and `prefetch_buffer > 0` cannot be combined. If you need a global cap, use hard cap mode.
**Important:** `claim_lease_ms` must be set when `prefetch_buffer > 0`. In hard cap mode (`prefetch_buffer = 0`), it is optional and overrides the default 60s lease.

**When to use soft cap:**
- Single-worker deployments where prefetch latency matters
- Uniform task durations where work imbalance isn't a concern

## Budgets per Execution Class

A worker has one claim budget for each [execution class](/horsies/tasks/defining-tasks/)
that it runs children for. The worker builds its claim plan once at start,
from its profile, and logs it. The plan routes the rows of each class:

| The worker runs | Rows of that class are claimed into |
|-----------------|-------------------------------------|
| Children of the class | The budget of that class |
| No children of a shared class, and isolated children | The `ISOLATED` budget |
| Neither | No budget: the worker claims none of them |

- An isolated child runs a task of any class. A worker gives a task more
  isolation than its row asks, never less.
- The isolated children of a worker that runs children of a shared class never
  take rows of that class.
- A worker with no isolated children claims no `ISOLATED` rows. It starts, and
  logs one line that names each class it does not serve.

A profile with `shared_async=SharedAsyncChildren(...)` has a `SHARED_ASYNC`
budget, and a profile with `shared_thread=SharedThreadChildren(...)` has a
`SHARED_THREAD` budget; a profile with isolated children only claims the rows
of every class into its `ISOLATED` budget. See
[Shared Async Tasks](/horsies/workers/shared-async-tasks/) and
[Shared Thread Tasks](/horsies/workers/shared-thread-tasks/).

The capacity of a budget is its children (`ISOLATED`) or seats (a shared
class) that are in service: a [slot that is down](/horsies/workers/worker-architecture/) does not
count. A shared child that closes counts only the tasks it still runs: its
free seats take no task. The claim counts the use of each budget from the
rows: this worker's RUNNING and live CLAIMED rows of the classes that the
budget claims.

The worker publishes each budget, its capacity, and the classes it claims in
`horsies_worker_states.class_capacity`, with each state snapshot.

Each reaper pass checks that a live worker serves every (queue, class) with
`PENDING` rows, and logs each one that no live worker serves. See
[Does a live worker serve every pending class?](/horsies/monitoring/worker-health/#does-a-live-worker-serve-every-pending-class)

### A row whose class does not fit its task

A row stores the class of its task at send. A deploy can change the class of
the function later, for example from `async def` with `SHARED_ASYNC` to `def`.
Before it dispatches a claimed row of a shared class, the worker compares that
class with the class of the task that it registered. When they differ, the
worker writes `ISOLATED` on the row, returns it to `PENDING` with the claim
fence, wakes the workers of the queue, and logs one warning. An `ISOLATED` row
fits every task. A row of a task that this worker did not register is
dispatched, and its child fails it as before.

## Claiming Controls

`ClaimLimits` in the [worker profile](/horsies/workers/worker-profiles/) holds the claim limits.
The capacity of a worker is the sum of the capacities of its budgets plus
`prefetch_buffer`: it never holds more tasks than that. The sum is
`WorkerProfile.seat_count()`: one per isolated child, and `seats` per shared
child. A limit above the capacity has no
effect, so `AppConfig` validation refuses it (`HRS-218`).

Both limits apply to the worker as a whole, across all budgets.

### per_queue_per_pass

Limits claims per queue per claiming pass. The default is `NoTaskLimit()`:
the worker claims up to the available local/global capacity for that queue.
Set `TaskLimit(tasks=N)` only when you want an explicit fairness cap per queue
per pass. A pass under this cap that claims runs the next pass at once,
because no `NOTIFY` announces work that the pass left.

```python
fair_batches = WorkerProfile(
    isolated=IsolatedChildren(count=8),
    claim=ClaimLimits(per_queue_per_pass=TaskLimit(tasks=4)),
)
```

A cap prevents one worker from claiming all available tasks, ensuring
fairness in multi-worker deployments at the cost of more claim passes.

### claimed_per_worker

Limits total CLAIMED (not yet running) tasks per worker:

```python
few_claimed = WorkerProfile(
    isolated=IsolatedChildren(count=8),
    claim=ClaimLimits(claimed_per_worker=TaskLimit(tasks=4)),
)
```

**Default behavior** (`NoTaskLimit()`): the capacity is the limit.
- Hard cap mode (`prefetch_buffer=0`): the seats of the profile (`seat_count()`)
- Soft cap mode (`prefetch_buffer>0`): the seats of the profile + `prefetch_buffer`

**Why this limit exists:**

- **Safety valve**: Explicit ceiling per worker below its capacity. Useful for clamping a profile without changing the app-wide `prefetch_buffer`.
- **Multi-tenant clusters**: Some workers may be "lightweight" (low memory) and need lower in-flight limits even with many children.
- **Testing and rollout**: Dial down claim pressure without changing global config or per-queue caps.
- **Prevent runaway claiming**: In soft cap mode with high prefetch, limits how much a single worker can hoard.

## How Claiming Works

One claim pass is one call of the `horsies_claim` function. For each queue and
each class that the plan routes to a budget, it reads one stream:

```sql
-- Simplified: one stream
SELECT id FROM tasks
WHERE queue_name = :queue
  AND execution = :class
  AND status = 'PENDING'
ORDER BY priority ASC, enqueued_at ASC, id ASC
FOR UPDATE SKIP LOCKED
LIMIT :take
```

The pass has two steps in one statement:

1. Read the head of each stream without locks: its pending rows and its rows
   with an expired lease, in one order. Walk all heads in rank order (queue
   priority, priority, `enqueued_at`, id), and take a row when its queue (cap
   and batch limit), its budget and the worker budget each have room left. A
   row that one limit drops uses no room of the others. Count the rows to
   take from each stream.
2. Take that many rows from each stream with `FOR UPDATE SKIP LOCKED`.

Key behaviors:

1. **Locks only what it takes**: Another worker can claim every row that this
   pass does not take, while the pass runs
2. **SKIP LOCKED**: A row that another claim holds is skipped, and the stream
   goes on to its next row
3. **Priority first**: Lower number = higher priority, within one class
4. **FIFO within priority**: Earlier `enqueued_at` wins
5. **Limit**: Respects batch size limits

## Concurrency Calculation

During each claim pass:

<!-- check: skip partial -- uses names that the page does not define -->
```python
# Hard cap mode (prefetch_buffer=0): local budgets count RUNNING + CLAIMED
# Soft cap mode (prefetch_buffer>0): queue caps count RUNNING; the local
# budgets subtract already-claimed rows, and each may prefetch.
hard_cap_mode = prefetch_buffer == 0
extra = 0 if hard_cap_mode else prefetch_buffer

# One budget per budget class of the claim plan
for budget in budgets:
    in_flight = count_running_and_claimed_for_worker(budget.classes)
    budget.available = budget.capacity + extra - in_flight

capacity = sum(budget.capacity for budget in budgets)
local_budget = capacity + extra - count_running_and_claimed_for_worker()

# Global budget (if cluster_wide_cap set)
if cluster_wide_cap:
    # Hard cap mode always counts RUNNING + CLAIMED globally
    in_flight_globally = count_all_running_and_claimed()
    global_budget = cluster_wide_cap - in_flight_globally
    worker_budget = min(local_budget, global_budget)
else:
    worker_budget = local_budget

# Per-queue limits (CUSTOM mode), across all classes of the queue
for queue in queues:
    if queue.max_concurrency:
        if hard_cap_mode:
            in_flight_in_queue = count_running_and_claimed_in_queue(queue)
        else:
            in_flight_in_queue = count_running_in_queue(queue)
        queue_budget = queue.max_concurrency - in_flight_in_queue
        if isinstance(per_queue_per_pass, TaskLimit):
            claim_count = min(worker_budget, queue_budget, per_queue_per_pass.tasks)
        else:
            claim_count = min(worker_budget, queue_budget)

# Each budget takes at most budget.available rows of the classes it claims,
# and the pass takes at most worker_budget rows in all. One walk in rank
# order takes a row when its queue, its budget and the pass have room left;
# a row that one limit drops uses no room of the others.
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

<!-- check: skip elided -- ... stands for a config shown on other pages -->
```python
config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(...),
)
```

```bash
horsies worker myapp.instance:app --profile default
```

The `default` profile holds `isolated=IsolatedChildren(count=8)`.

### Multi-Worker with Global Cap

<!-- check: skip elided -- ... stands for a config shown on other pages -->
```python
config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    cluster_wide_cap=20,
    broker=PostgresConfig(...),
)
```

```bash
# Machine 1
horsies worker myapp.instance:app --profile default

# Machine 2
horsies worker myapp.instance:app --profile default

# Total: 16 children (8 each), but max 20 RUNNING tasks cluster-wide
```

### Priority Queues with Rate Limiting

<!-- check: skip elided -- ... stands for a config shown on other pages -->
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

Worker logs its profile and concurrency config at startup:

```text
Worker profile 'default': isolated=4 (recycle after 100 tasks, above no limit), shared_async=none, shared_thread=none, claim per queue per pass: no limit, claimed per worker: no limit, drain_timeout=300s, preload=library
Concurrency config: processes=4, shared_async_children=0, shared_thread_children=0, seats=4, cluster_wide_cap=20, max_claim_per_worker=4, max_claim_batch=0
Claim plan: ISOLATED -> ISOLATED budget, SHARED_ASYNC -> ISOLATED budget, SHARED_THREAD -> ISOLATED budget
```

The second line uses the internal names: `processes` is the number of
isolated children, `seats` is `WorkerProfile.seat_count()`,
`max_claim_per_worker` is the effective `claimed_per_worker` limit, and
`max_claim_batch=0` means `per_queue_per_pass=NoTaskLimit()`. The third line
is the [claim plan](#budgets-per-execution-class).

## Troubleshooting

### Tasks Not Being Claimed

Check:

- `cluster_wide_cap` reached?
- `max_concurrency` for queue reached?
- Tasks expired (`good_until`)?
- No live worker serves the task's execution class on its queue? The worker
  logs it, and `get_class_coverage()` reports it as `StreamUnserved()`.

### Uneven Distribution

Use the default `per_queue_per_pass=NoTaskLimit()`, raise or remove the
`claimed_per_worker` limit when prefetching, or run more workers.

### Database Overload

Lower `cluster_wide_cap` or queue `max_concurrency`.
