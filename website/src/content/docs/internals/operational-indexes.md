---
title: Operational Indexes
summary: Opt-in DDL for adopter-side history queries that horsies deliberately does not index in the shipped schema.
related: [database-schema, ../../tasks/retrieving-results]
tags: [internals, indexes, observability, postgres, performance]
---

# Operational Indexes

Task history in horsies is plain Postgres — `horsies_tasks` and
`horsies_task_attempts` are queryable with ordinary SQL, and building
dashboards or health checks directly on them is a supported pattern. The
shipped schema, however, indexes only what horsies itself queries: the claim
path, retention eligibility, and workflow completion. Dashboard-shaped
queries are not indexed by default, because every additional index taxes the
finalize write path of **every** deployment, including the majority that
never run those queries.

This page lists verified, opt-in DDL for common history-query shapes. Create
them on your own database; they are safe to add and drop independently of
horsies migrations.

## Why and When

Retained history grows with throughput (`terminal_record_retention_hours`
defaults to 30 days). Past roughly 10⁵–10⁶ retained rows, any history query
without a matching index degrades linearly with table size: at 1.2M retained
rows, a latest-terminal-run lookup reads the whole heap at ~2.9 s per
execution — and the same scans slow every other query on the broker
database, including claims.

If you query task history from application code, add the matching index
below. If you only use `handle.get()` / `handle.info()` on known task ids,
you do not need any of this — id lookups use the primary key.

## Latest terminal run by task name

Serves the query shape:

```sql
SELECT ...
FROM horsies_tasks
WHERE task_name = $1
  AND status IN ('CANCELLED', 'COMPLETED', 'EXPIRED', 'FAILED')
ORDER BY COALESCE(completed_at, failed_at, updated_at) DESC
LIMIT 1;
```

Index:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_horsies_tasks_name_terminal_finished
ON horsies_tasks (
  task_name,
  (COALESCE(completed_at, failed_at, updated_at)) DESC
)
WHERE status IN ('CANCELLED', 'COMPLETED', 'EXPIRED', 'FAILED');
```

At 1.2M retained rows this replaces a ~2.9 s full scan with a single index
probe.

Maintenance cost is bounded by the same discipline as the shipped retention
indexes: the partial predicate covers only terminal statuses, so a row
enters the index once, at its finalize transition — claims, lease renewals,
and RUNNING transitions never maintain it. The write cost is one extra index
insert per completed task.

Two exactness requirements:

- The `COALESCE(...)` expression in your query must be **textually
  identical** to the indexed expression, or the planner will not use it.
- The status list must match horsies' terminal statuses
  (`CANCELLED`, `COMPLETED`, `EXPIRED`, `FAILED`). If a future horsies
  version changes the terminal set, recreate the index with the new
  literals.

## Things to Avoid

**Don't index `task_name` over all rows.**

```sql
-- Wrong: maintained by every claim, lease renewal, and status transition
CREATE INDEX idx_horsies_tasks_on_task_name ON horsies_tasks (task_name);
```

An all-rows index on `horsies_tasks` sits on the hottest write path in the
schema. The partial terminal-only form above serves the same dashboard
queries at a fraction of the maintenance cost.

**Don't skip `CONCURRENTLY` on a live database.** A plain `CREATE INDEX`
takes a lock that blocks writes — including claims — for the duration of the
build.

`CONCURRENTLY` has two constraints of its own. It cannot run inside a
transaction block — migration tools that wrap DDL in one must disable that
for this statement. And a failed concurrent build leaves an `INVALID` index
behind, which `IF NOT EXISTS` treats as present — a retry then silently does
nothing. After a failed build, drop the leftover
(`DROP INDEX CONCURRENTLY ...`) and re-run; invalid indexes are listed by:

```sql
SELECT indexrelid::regclass FROM pg_index WHERE NOT indisvalid;
```

## Compatibility

These indexes are adopter-owned. If a future horsies release ships an
equivalent index in a schema migration, the release notes will say so — drop
your copy then. Horsies never drops indexes it did not create.
