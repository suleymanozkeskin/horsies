---
title: Performance
summary: Measured per-statement latencies of the hot path under a real workload shape, and what governs them.
related: [../../questions-and-answers, database-schema, operational-indexes]
tags: [internals, performance, latency, postgres]
---

# Performance

Per-statement latency over a 24-hour window: workers and managed Postgres on
separate machines in the same region, entry-tier instance, ~2.5M statements
across the top 20 by execution count.

| Statement | Count / 24h | p50 | p99 |
| --- | --- | --- | --- |
| Cap-serialization advisory lock | 902,184 | 0 ms | 0 ms |
| NOTIFY dispatch triggers | 260,248 | 0 ms | 0 ms |
| Claim function (`horsies_claim`) | 150,417 | 1 ms | 2 ms |
| Attempt-history retention delete | 37,478 | 0 ms | 1 ms |
| Heartbeat insert | 36,765 | 0 ms | 1 ms |
| Finalize fence update | 35,923 | 0 ms | 1 ms |
| Task enqueue insert | 31,412 | 1 ms | 2 ms |
| Requeue / unclaim update | 31,350 | 0 ms | 1 ms |

No statement in the top 20 by count exceeds a 2 ms p99.

## Where the runtime goes

The claim function is the most expensive statement on the instance at ~18%
of total DB runtime — by design. It is the entire claim critical section
(advisory lock, candidate selection, per-queue cap enforcement, claim
update) as one server-side statement, so the lock is held across a single
statement rather than across client round trips. The two statements that
reach 2 ms p99 are the two that do the most work per call: the claim
function and the enqueue insert. Everything around them is near-free.

## What the numbers measure

Server-side per-statement latency. End-to-end task latency adds network
round trips between your processes and the database, which scale with
distance — co-locating workers with Postgres removes them entirely.

The table is a reference shape, not a ceiling: the instance is entry-tier,
and per-statement latency stays flat as load grows until the instance
saturates. Headroom scales with the instance tier.

Results scale with the Postgres instance: a PlanetScale Postgres and a
Heroku Postgres will not perform the same, and a transaction-pooled
connection path taxes every round trip. Deployment guidance:
[Throughput expectations](../../questions-and-answers#throughput-expectations-and-comparisons).

## Worker Start Paths

Drain throughput in tasks/s from first progress, median of 3 alternating runs. Conditions: 10,000 no-op tasks, 8 children, a 6 vCPU Linux VM, Postgres 18 on tmpfs. 36 runs, 0 failed tasks. No disk write cost is included, so compare the columns, not the absolute values.

| Claim mode | `max_tasks_per_child` | `--child-preload=none` | `library` | `app` |
| ---------- | --------------------- | ---------------------- | --------- | ----- |
| Hard cap | 0 | 819 | 853 | 855 |
| Hard cap | 100 (default) | 437 | 803 | 785 |
| Prefetch 8 | 0 | 1,121 | 1,189 | 1,150 |
| Prefetch 8 | 100 (default) | 484 | 1,032 | 1,058 |

A recycle replaces one child, so its cost shows in the `max_tasks_per_child=100` rows.
