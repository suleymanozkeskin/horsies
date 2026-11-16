---
title: Performance
summary: Hot-path costs, task-history read shape, and retention cost.
related: [../../questions-and-answers, database-schema, operational-indexes]
tags: [internals, performance, latency, postgres, task-history]
---

# Performance

## Hot-path reference

These figures use one entry-tier managed PostgreSQL instance. Workers and the
database are on separate machines in one region. The sample covers about 2.5
million statements in 24 hours.

| Statement | Count in 24 hours | p50 | p99 |
|---|---:|---:|---:|
| Cap advisory lock | 902,184 | 0 ms | 0 ms |
| Notify triggers | 260,248 | 0 ms | 0 ms |
| Claim function | 150,417 | 1 ms | 2 ms |
| Heartbeat insert | 36,765 | 0 ms | 1 ms |
| Finalize fence update | 35,923 | 0 ms | 1 ms |
| Task enqueue insert | 31,412 | 1 ms | 2 ms |
| Requeue or unclaim update | 31,350 | 0 ms | 1 ms |

The table reports server-side statement time. Network time is separate.

## Terminalization cost

Terminalization remains one database function call. The function locks the
live row. It writes the history row. It archives attempts. It deletes live
attempts. It deletes the live task. It emits the completion notification.

The transaction writes one history leaf. Its cost includes the leaf's task-ID
and enqueue-order indexes.

## Point reads

Task result and info reads check live storage first. Terminal reads use the
staged reader functions.

The staged functions hold a static leaf list. UUIDv7 time orders likely leaf
probes. The reader still probes every skipped leaf before absence. Each probe
uses the leaf task-ID index.

A missing leaf triggers reader publication on the next maintenance pass. The
reader excludes the missing relation. The health report keeps its catalog name.

## History lists

Each leaf has an `enqueued_at` btree. A bounded list can merge leaf scans in
index order and stop at its limit. Migration 0040 adds the schema-v34 index to
existing leaves. New leaves receive the same index.

Filters without a matching index still scan the selected leaves. Add a focused
leaf index for a permanent application query. See [Operational
Indexes](../operational-indexes).

## Retention cost

Task and heartbeat retention drops partitions. Drop cost does not grow with the
number of rows in the leaf. A detach can wait for old transactions. The worker
caps that wait with a statement timeout.

Workflow and worker-state retention deletes rows in batches. Delete cost grows
with the retired row count. Autovacuum must reclaim dead tuples later.

History class duration sets a minimum age. Daily leaf size adds up to one day
of retention. Smaller leaves would reduce that margin but would create more
relations and indexes.

## Cutover estimates

Cutover duration depends on row count and database speed. Fit the preparation
and relocation slopes on the target server. Measure fixed stage time on the
same server. Do not copy coefficients from another deployment.

See the [task-history cutover runbook](../../operations/cutover-runbook).
