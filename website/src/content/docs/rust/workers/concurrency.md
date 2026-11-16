---
title: Concurrency
summary: Worker, queue, and cluster-level concurrency controls.
related: [worker-architecture, ../../concepts/queue-modes, ../../configuration/app-config]
tags: [workers, concurrency, claiming, limits]
---

## Alpha Operational Notes

- **Priority ordering:** Within the same priority, FIFO ordering uses `enqueued_at`. When many tasks share the same timestamp, ordering can appear non-deterministic.
- **Soft-cap bursts:** With `prefetch_buffer > 0`, the system counts only RUNNING tasks. Short-lived bursts above the nominal cap can occur during claim/dispatch.
- **Claim leases:** In soft-cap mode, claims can expire and be reclaimed. Workers must verify ownership before running user code.

## Concurrency Levels

<!-- todo:diagram-needed - Concurrency hierarchy -->

```text
+----------------------------------------------------------+
|  Cluster Wide Cap (optional)                             |
|  Max in-flight tasks across all workers (RUNNING+CLAIMED)|
|                                                          |
|  +----------------------------------------------------+  |
|  |  Per-Queue Concurrency (CUSTOM mode)               |  |
|  |  Max RUNNING per queue across cluster              |  |
|  |                                                    |  |
|  |  +----------------------------------------------+  |  |
|  |  |  Per-Worker Concurrency                      |  |  |
|  |  |  Max concurrent tasks per worker             |  |  |
|  |  |  (equals WorkerConfig.concurrency)           |  |  |
|  |  +----------------------------------------------+  |  |
|  +----------------------------------------------------+  |
+----------------------------------------------------------+
```

## Worker-Level: concurrency

Each worker runs N concurrent tasks:

```rust
use horsies::WorkerConfig;

let worker_config = WorkerConfig {
    concurrency: 8,
    ..Default::default()
};
app.run_worker_with(worker_config).await?;
```

- Limits concurrent task execution on this worker
- Enforced via `tokio::sync::Semaphore`
- Default: number of available CPU cores

## Queue-Level: max_concurrency

In CUSTOM mode, limit concurrent tasks per queue:

```rust
use horsies::{AppConfig, QueueMode, CustomQueueConfig};

let config = AppConfig {
    queue_mode: QueueMode::Custom,
    custom_queues: Some(vec![
        CustomQueueConfig {
            name: "api_calls".into(),
            priority: 1,
            max_concurrency: 5, // Max 5 tasks running cluster-wide
        },
        CustomQueueConfig {
            name: "low".into(),
            priority: 100,
            max_concurrency: 2,
        },
    ]),
    ..AppConfig::for_database_url("postgresql://...")
};
```

Use cases:

- Rate limiting external API calls
- Preventing database overload
- Resource isolation

## Cluster-Level: cluster_wide_cap

Limit total concurrent tasks across all workers:

```rust
let config = AppConfig {
    cluster_wide_cap: Some(50), // Max 50 tasks in-flight across entire cluster
    ..AppConfig::for_database_url("postgresql://...")
};
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

```rust
let config = AppConfig {
    cluster_wide_cap: Some(50),
    prefetch_buffer: 0, // Default: hard cap mode
    ..AppConfig::for_database_url("postgresql://...")
};
```

**Behavior:**
- Strict enforcement: exactly `cluster_wide_cap` tasks can be in-flight at once
- Fair distribution: fast workers are not blocked by slow workers' prefetch queues
- No prefetch: workers only claim tasks they can immediately execute

**Why this matters:**

In production, task durations vary wildly. Without hard cap mode, a worker processing slow tasks (10s each) prefetches additional tasks while a worker processing fast tasks (100ms each) sits idle waiting for work. Hard cap mode ensures fair distribution.

### Soft Cap Mode (with Prefetch)

When `prefetch_buffer > 0`, the system only counts **RUNNING** tasks, allowing workers to prefetch beyond their running capacity:

```rust
let config = AppConfig {
    prefetch_buffer: 4,         // Allow prefetching up to 4 tasks beyond running
    claim_lease_ms: Some(5000), // Required: lease expires after 5 seconds
    ..AppConfig::for_database_url("postgresql://...")
};
```

**Behavior:**
- Prefetch allowed: workers can claim tasks ahead of execution
- Lease expiry: prefetched claims expire and can be reclaimed by other workers
- Soft limit: actual concurrent tasks may briefly exceed the cap during claiming

**Important:** `cluster_wide_cap` and `prefetch_buffer > 0` cannot be combined. If you need a global cap, use hard cap mode.
**Important:** `claim_lease_ms` must be set when `prefetch_buffer > 0`. In hard cap mode (`prefetch_buffer = 0`), it is optional and overrides the default 60s lease.

**When to use soft cap:**
- Single-worker deployments where prefetch latency matters
- Uniform task durations where work imbalance is not a concern

## Claiming Controls

### max_claim_batch

Limits claims per queue per claiming pass:

```rust
let worker_config = WorkerConfig {
    max_claim_batch: 2,
    ..Default::default()
};
```

Prevents one worker from claiming all available tasks, ensuring fairness in multi-worker deployments.

### max_claim_per_worker

Limits total CLAIMED (not yet running) tasks per worker:

```rust
let worker_config = WorkerConfig {
    max_claim_per_worker: 10,
    ..Default::default()
};
```

**Default behavior** (when set to 0):
- Hard cap mode (`prefetch_buffer=0`): defaults to `concurrency`
- Soft cap mode (`prefetch_buffer>0`): defaults to `concurrency + prefetch_buffer`

**Why this field exists:**

- **Safety valve**: Explicit hard ceiling per worker, independent of concurrency or prefetch. Useful for clamping unstable workers without changing app config.
- **Multi-tenant clusters**: Some workers may be "lightweight" (low memory) and need lower in-flight limits even with high concurrency.
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

1. **SKIP LOCKED**: Do not wait for locked rows, skip them
2. **Priority first**: Lower number = higher priority
3. **FIFO within priority**: Earlier `enqueued_at` wins
4. **Limit**: Respects batch size limits

## Concurrency Calculation

During each claim pass:

```text
// Hard cap mode (prefetch_buffer=0): count RUNNING + CLAIMED
// Soft cap mode (prefetch_buffer>0): count only RUNNING

// Local budget (semaphore slots available)
in_flight_locally = count_running_and_claimed_for_worker()  // or just running in soft cap
local_budget = concurrency - in_flight_locally

// Global budget (if cluster_wide_cap set)
if cluster_wide_cap is set:
    in_flight_globally = count_all_running_and_claimed()
    global_budget = cluster_wide_cap - in_flight_globally
    budget = min(local_budget, global_budget)
else:
    budget = local_budget

// Per-queue limits (CUSTOM mode)
for queue in queues:
    if queue.max_concurrency is set:
        in_flight_in_queue = count_in_flight_in_queue(queue)
        queue_budget = queue.max_concurrency - in_flight_in_queue
        claim_count = min(budget, queue_budget, max_claim_batch)
```

## Advisory Locks

Global advisory lock serializes claiming across workers:

```sql
-- Taken during claim pass
SELECT pg_advisory_xact_lock(:key)
```

This prevents race conditions when multiple workers claim simultaneously.

## Example Configurations

### Single Worker, Simple

```rust
let config = AppConfig {
    queue_mode: QueueMode::Default,
    ..AppConfig::for_database_url("postgresql://...")
};

let worker_config = WorkerConfig {
    concurrency: 8,
    ..Default::default()
};
app.run_worker_with(worker_config).await?;
```

### Multi-Worker with Global Cap

```rust
let config = AppConfig {
    queue_mode: QueueMode::Default,
    cluster_wide_cap: Some(20),
    ..AppConfig::for_database_url("postgresql://...")
};

// Machine 1: concurrency=8
// Machine 2: concurrency=8
// Total: 16 slots, but max 20 RUNNING tasks cluster-wide
```

### Priority Queues with Rate Limiting

```rust
let config = AppConfig {
    queue_mode: QueueMode::Custom,
    custom_queues: Some(vec![
        CustomQueueConfig { name: "stripe".into(), priority: 1, max_concurrency: 3 },
        CustomQueueConfig { name: "email".into(), priority: 50, max_concurrency: 10 },
        CustomQueueConfig { name: "reports".into(), priority: 100, max_concurrency: 2 },
    ]),
    cluster_wide_cap: Some(15),
    ..AppConfig::for_database_url("postgresql://...")
};
```

## Monitoring

Worker logs concurrency config at startup:

```text
Concurrency config: concurrency=4, cluster_wide_cap=20, max_claim_per_worker=4, max_claim_batch=2
```

## Troubleshooting

### Tasks Not Being Claimed

Check:

- `cluster_wide_cap` reached?
- `max_concurrency` for queue reached?
- Tasks expired (`good_until`)?

### Uneven Distribution

Increase `max_claim_batch` or run more workers.

### Database Overload

Lower `cluster_wide_cap` or queue `max_concurrency`.
