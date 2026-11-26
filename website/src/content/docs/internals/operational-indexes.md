---
title: Operational Indexes
section: shared
summary: The indexes that every task history leaf carries, and how to add an index for a query of your own.
related: [database-schema, performance, ../monitoring/worker-health]
tags: [internals, indexes, postgres, performance, history]
---

# Operational Indexes

Horsies creates two indexes on every task history leaf. An index for a query of your application is yours to create on each leaf that the query reads.

## Shipped History Indexes

Every leaf of `horsies_task_history` has these btree indexes:

| Columns | Used by |
|---------|---------|
| `(task_id)` | Point reads of one task: result reads, task info, the rerun source read, the duplicate guard of a resend |
| `(enqueued_at)` | The monitoring task list and its default sort |

Partition coverage creates both indexes when it creates a leaf. See [Database Schema](/horsies/internals/database-schema/#horsies_task_history) for the partition layout.

A point read probes the `(task_id)` index of each leaf that can hold the task. A leaf whose day ends more than 5 s before the UUIDv7 time of the task id cannot hold it, so the read skips that leaf. A task id whose UUIDv7 time is more than 5 s later than its terminal time is recorded in `horsies_task_history_clock_exceptions`, and the read finds it there.

## Add an Index for Your Own Query

Add an index only for a query that your application runs. Each retention class and each UTC day is a separate leaf (`horsies_task_history_<class key>_YYYY_MM_DD`). An index on one leaf does not cover another leaf.

This query filters by task name and sorts by enqueue time:

```sql
SELECT task_id, status, enqueued_at
FROM horsies_task_history
WHERE task_name = $1
ORDER BY enqueued_at DESC
LIMIT 20;
```

A matching leaf index:

```sql
CREATE INDEX CONCURRENTLY horsies_task_history_standard_30d_2026_08_13_name_enqueued_idx
ON horsies_task_history_standard_30d_2026_08_13 (task_name, enqueued_at DESC);
```

- Create the index on each leaf that the query can read.
- Create it on each new leaf when the query is permanent: partition coverage creates new leaves with the two shipped indexes only.
- Keep the index name within PostgreSQL's 63-byte identifier limit.

## Write Cost

Each history row is inserted once, by the move of a finished task. Each extra index adds one index entry to that move, and build and storage cost to every leaf that has it.

Start from the query shape. Confirm the plan with `EXPLAIN (ANALYZE, BUFFERS)`.

## Concurrent Builds

Use `CREATE INDEX CONCURRENTLY` on a leaf that receives writes. A plain build blocks writes to that leaf.

`CONCURRENTLY` cannot run in a transaction block. A failed build can leave an invalid index. Find invalid indexes with:

```sql
SELECT indexrelid::regclass
FROM pg_index
WHERE NOT indisvalid;
```

Drop the invalid index with `DROP INDEX CONCURRENTLY`, then build it again.

## Ownership

Horsies owns the two shipped indexes of each leaf. Your application owns every other index. Retention drops whole leaves, and a leaf drop drops every index on it.
