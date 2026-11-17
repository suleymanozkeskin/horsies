---
title: Queue Modes
section: shared
summary: DEFAULT vs CUSTOM queue modes, priorities, and per-queue concurrency.
related: [../../configuration/app-config, ../../workers/concurrency]
tags: [concepts, queues, mode, configuration]
---

# Queue Modes

Horsies supports two queue modes: DEFAULT for simple single-queue setups, and CUSTOM for multiple named queues with priorities and concurrency limits.

The API is on the client pages: [Queue Mode Configuration](/horsies/configuration/app-config/#queue-mode-configuration) (Python) and [Queues](/horsies/rust/configuration/app-config/#queues) (Rust).

## DEFAULT Mode

The simplest configuration - all tasks go to a single "default" queue.

Characteristics:

- Single queue named "default"
- All tasks processed in order ( priority=100 --> means lowest )
- No per-queue concurrency limits
- A task cannot name a queue
- FIFO applies

## CUSTOM Mode

Multiple named queues with individual priorities and concurrency limits.
Queues limits are supported within the same deployed device, no need for deploying a separate device for each queue.

Each task names its queue. For example, three queues: `critical` (priority 1, at most 10 running), `normal` (priority 50, at most 5) and `low` (priority 100, at most 2), with one task on each. The Python example is in [CUSTOM Mode](/horsies/configuration/app-config/#custom-mode).

## CustomQueueConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | required | Unique queue identifier |
| `priority` | `int` | 1 | Priority level (1=highest, 100=lowest) |
| `max_concurrency` | `int \| None` | 5 | Max simultaneous RUNNING tasks in this queue; `None` = uncapped, `0` = pause claiming |

## How Priority Works

Workers claim tasks in priority order:

1. All priority=1 queues checked first
2. Then priority=2, priority=3, etc.
3. Within same priority, FIFO by `enqueued_at`

This means "critical" tasks (priority=1) are always processed before "low" tasks (priority=100) when both are pending.

## Queue Concurrency

`max_concurrency` limits how many tasks from a queue can be RUNNING simultaneously **across the entire cluster**:

- If `critical` has `max_concurrency=10`, at most 10 critical tasks run at once
- This is checked during claiming, not just per-worker
- Useful for rate-limiting external API calls or database-heavy operations
- `max_concurrency=None` removes the per-queue limit entirely (worker and
  cluster limits still apply) and skips the per-claim in-flight count for
  that queue; `max_concurrency=0` pauses claiming from the queue

## When to Use Each Mode

**DEFAULT mode** when:

- Simple application with uniform task priority
- No need for per-queue rate limiting
- Getting started / prototyping

**CUSTOM mode** when:

- Different task types need different priorities
- Some operations need rate limiting (e.g., external API calls)
- You want to prevent low-priority operations from blocking urgent tasks
- Resource isolation is important

## Validation

Queue configuration is validated at multiple points:

**At app creation**: Queue names must be unique, custom_queues required in CUSTOM mode

**At task definition**: the queue of a task must match a configured queue (CUSTOM mode) or be omitted (DEFAULT mode)

**At task send time**: Queue name re-validated against current configuration

An unknown queue name is refused with `HRS-103`, and the error lists the configured queues. The Python example and its error text are in [Unknown Queue Names](/horsies/configuration/app-config/#unknown-queue-names).

## Cluster-Wide Cap

In addition to per-queue concurrency, `cluster_wide_cap` limits the total RUNNING tasks across all queues, for example to 50. The Python example is in [Cluster-Wide Concurrency](/horsies/configuration/app-config/#cluster-wide-concurrency).

This is useful when:

- Database or external service has connection limits
- You want to cap overall resource usage
- Running multiple worker instances
