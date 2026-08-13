---
title: Operational Indexes
summary: Shipped task-history indexes and safe adopter-owned additions.
related: [database-schema, performance, ../../tasks/retrieving-results]
tags: [internals, indexes, observability, postgres, performance]
---

# Operational Indexes

## Shipped history indexes

Every task-history leaf has these btrees:

- `(task_id)` for point reads
- `(enqueued_at)` for bounded lists and default sort

Migration 0040 applies the schema-v34 `enqueued_at` index rule to every attached
leaf. New leaves receive both indexes when they are created.

The list reader can merge leaf indexes in order. It can stop after the requested
limit. It does not need one global sort over all matched history rows.

## Staged point lookup

Task identity reads do not query the partition parent with a dynamic plan. The
worker publishes staged lookup, provenance, and detail functions. Each function
contains the current leaf list.

The reader uses UUIDv7 birth time to order likely probes. It widens the hint by
five seconds. It probes every skipped leaf before it reports absence. A caller
clock error cannot hide a retained row.

The point path uses each leaf's `(task_id)` index. `TaskHandle::get()`,
`TaskHandle::info()`, and rerun source lookup use this path.

## Add an adopter-owned history index

Add an index only for a query that your application runs. Every history class
and day is a separate leaf. An index on one leaf does not cover another leaf.

This query filters by task name and sorts by enqueue time:

```sql
SELECT task_id, status, enqueued_at
FROM horsies_task_history
WHERE task_name = $1
ORDER BY enqueued_at DESC
LIMIT 20;
```

A matching leaf index is:

```sql
CREATE INDEX CONCURRENTLY horsies_task_history_standard_30d_2026_08_13_name_enqueued_idx
ON horsies_task_history_standard_30d_2026_08_13 (task_name, enqueued_at DESC);
```

Repeat the index for each leaf that the query may scan. Automate this for new
leaves when the query is permanent. Use a name that stays within PostgreSQL's
63-byte identifier limit.

## Write cost

Each history row is inserted once. Each extra index adds one index entry to the
terminalization transaction. It also adds build and storage cost to every leaf.

Do not add an all-purpose index set. Start from the query shape. Confirm the
plan with `EXPLAIN (ANALYZE, BUFFERS)`.

## Concurrent builds

Use `CREATE INDEX CONCURRENTLY` on a live leaf. A plain build can block writes
to that leaf.

`CONCURRENTLY` cannot run in a transaction block. A failed build can leave an
invalid index. Find invalid indexes with:

```sql
SELECT indexrelid::regclass
FROM pg_index
WHERE NOT indisvalid;
```

Drop the invalid index concurrently. Then run the build again.

## Ownership

Horsies owns the two shipped indexes on each leaf. Your application owns every
extra index. A leaf drop also drops every index on that leaf. Recreate an
adopter-owned index on each new leaf that needs it.
