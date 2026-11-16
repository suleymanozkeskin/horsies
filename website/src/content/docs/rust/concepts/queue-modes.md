---
title: Queue Modes
summary: Default vs Custom queue modes, priorities, and per-queue concurrency.
related: [../../configuration/app-config, ../../workers/concurrency]
tags: [concepts, queues, mode, configuration]
---

# Queue Modes

Horsies supports two queue modes: Default for simple single-queue setups, and Custom for multiple named queues with priorities and concurrency limits.

## Default Mode

The simplest configuration — all tasks go to a single "default" queue.

```rust
use horsies::{Horsies, AppConfig, QueueMode};

let config = AppConfig {
    queue_mode: QueueMode::Default,
    ..AppConfig::for_database_url("postgresql://...")
};
let mut app = Horsies::new(config)?;

#[task("my_task")]  // Goes to "default" queue
async fn my_task() -> Result<String, TaskError> {
    Ok("done".into())
}
```

Characteristics:

- Single queue named "default"
- All tasks processed in order ( priority=100 --> means lowest )
- No per-queue concurrency limits
- Specifying a non-"default" `queue` in the task macro will fail at `register()` time
- FIFO applies

## Custom Mode

Multiple named queues with individual priorities and concurrency limits.
Queue limits are supported within the same deployed instance, no need for deploying a separate instance for each queue.

```rust
use horsies::{Horsies, AppConfig, QueueMode, CustomQueueConfig};

let config = AppConfig {
    queue_mode: QueueMode::Custom,
    custom_queues: Some(vec![
        CustomQueueConfig { name: "critical".into(), priority: 1,   max_concurrency: 10 },
        CustomQueueConfig { name: "normal".into(),   priority: 50,  max_concurrency: 5 },
        CustomQueueConfig { name: "low".into(),      priority: 100, max_concurrency: 2 },
    ]),
    ..AppConfig::for_database_url("postgresql://...")
};
let mut app = Horsies::new(config)?;

#[task("urgent_alert", queue = "critical")]
async fn urgent_alert() -> Result<String, TaskError> {
    Ok("alerted".into())
}

#[task("process_order", queue = "normal")]
async fn process_order() -> Result<String, TaskError> {
    Ok("processed".into())
}

#[task("ship_item", queue = "low")]
async fn ship_item() -> Result<String, TaskError> {
    Ok("shipped".into())
}
```

## CustomQueueConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `String` | required | Unique queue identifier |
| `priority` | `u32` | 1 | Priority level (1=highest, 100=lowest) |
| `max_concurrency` | `u32` | 5 | Max simultaneous Running tasks in this queue |

## How Priority Works

Workers claim tasks in priority order:

1. All priority=1 queues checked first
2. Then priority=2, priority=3, etc.
3. Within same priority, FIFO by `enqueued_at`

This means "critical" tasks (priority=1) are always processed before "low" tasks (priority=100) when both are pending.

## Queue Concurrency

`max_concurrency` limits how many tasks from a queue can be Running simultaneously **across the entire cluster**:

- If `critical` has `max_concurrency=10`, at most 10 critical tasks run at once
- This is checked during claiming, not just per-worker
- Useful for rate-limiting external API calls or database-heavy operations

## When to Use Each Mode

**Default mode** when:

- Simple application with uniform task priority
- No need for per-queue rate limiting
- Getting started / prototyping

**Custom mode** when:

- Different task types need different priorities
- Some operations need rate limiting (e.g., external API calls)
- You want to prevent low-priority operations from blocking urgent tasks
- Resource isolation is important

## Validation

Queue configuration is validated at multiple points:

**At app creation**: Queue names must be unique, `custom_queues` required in Custom mode

**At task registration**: `queue` must match a configured queue (Custom mode) or be "default" (Default mode). The macro itself accepts any `queue` value — validation happens when `register()` is called.

```rust
// The macro compiles fine, but register() will return an error:
#[task("bad_task", queue = "nonexistent")]
async fn bad_task() -> Result<String, TaskError> { Ok("".into()) }

bad_task::register(&mut app)?; // Error: queue 'nonexistent' not in configured custom_queues
```

## Cluster-Wide Cap

In addition to per-queue concurrency, you can limit total Running tasks across all queues:

```rust
let config = AppConfig {
    queue_mode: QueueMode::Custom,
    custom_queues: Some(vec![/* ... */]),
    cluster_wide_cap: Some(50), // Max 50 tasks running across entire cluster
    ..AppConfig::for_database_url("postgresql://...")
};
```

This is useful when:

- Database or external service has connection limits
- You want to cap overall resource usage
- Running multiple worker instances
