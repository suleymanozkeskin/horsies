---
title: Autovacuum Tuning
summary: Per-table autovacuum settings for the high-churn horsies tables, and the queries that verify vacuum keeps up.
related: [../internals/database-schema, ../internals/operational-indexes]
tags: [operations, autovacuum, postgres, maintenance]
---

# Autovacuum Tuning

horsies ships no autovacuum configuration and never alters vacuum behavior.
The settings on this page are PostgreSQL storage parameters, applied and
owned by the operator on their own database. This page exists because the
PostgreSQL defaults are sized for tables that change slowly, and
`horsies_tasks` is not such a table.

## Why and When

PostgreSQL triggers autovacuum on a table when dead tuples exceed
`autovacuum_vacuum_threshold + autovacuum_vacuum_scale_factor × reltuples`.
The defaults are `50` and `0.2`: a table must accumulate dead tuples equal
to roughly 20% of its row count before vacuum starts.

`horsies_tasks` churns faster than that trigger assumes:

- Every task row is rewritten several times between enqueue and terminal
  state — claim, start, finalize each produce a dead tuple under MVCC.
- Retention deletes remove terminal rows in bulk on a fixed cadence, leaving
  dead tuples proportional to throughput, not to table size.

With 30 days of retained history (`terminal_record_retention_hours`
default), the live row count is dominated by finished tasks that never
change. A 1M-row table waits for ~200k dead tuples before vacuum runs; in
the meantime the dead tuples sit in exactly the pages and indexes the claim
path reads, and index-only scans fall back to heap fetches as the visibility
map goes stale.

Tune when `horsies_tasks` holds more than ~10⁵ rows or the dead-tuple ratio
from the monitoring query below stays above a few percent between vacuums.

## How To

Set per-table storage parameters. `ALTER TABLE ... SET` takes a
`SHARE UPDATE EXCLUSIVE` lock, which does not block reads or writes; no
restart is required and the settings persist in the database.

For tables up to a few million rows, shrink the scale factor so the trigger
tracks churn instead of table size:

```sql
ALTER TABLE horsies_tasks SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 1000
);
```

For larger tables, a proportional trigger still stretches as history
accumulates: 1% of 50M rows is 500k dead tuples. Switch to a fixed
threshold so vacuum cadence is set by churn alone:

```sql
ALTER TABLE horsies_tasks SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 10000
);
```

Pick the threshold from write volume: a value near a few minutes of task
throughput keeps dead-tuple ratio low without running vacuum continuously.

The same recipe applies to the other horsies tables when they churn at
comparable rates — `horsies_task_attempts`, `horsies_workflows`, and
`horsies_workflow_tasks` under heavy workflow use, `horsies_heartbeats` and
`horsies_worker_states` under large worker fleets.

To revert to the PostgreSQL defaults:

```sql
ALTER TABLE horsies_tasks RESET (
    autovacuum_vacuum_scale_factor,
    autovacuum_vacuum_threshold,
    autovacuum_analyze_scale_factor,
    autovacuum_analyze_threshold
);
```

## Verifying Vacuum Keeps Up

Dead-tuple ratio and last-vacuum age, from the statistics collector:

```sql
SELECT relname,
       n_live_tup,
       n_dead_tup,
       round(n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup, 0), 3)
           AS dead_ratio,
       last_autovacuum,
       now() - last_autovacuum AS autovacuum_age
FROM pg_stat_user_tables
WHERE relname LIKE 'horsies_%'
ORDER BY n_dead_tup DESC;
```

Healthy: `dead_ratio` oscillates below the configured trigger and
`autovacuum_age` stays within minutes-to-hours of the expected cadence. A
`dead_ratio` that sits above the trigger with an old `last_autovacuum` means
autovacuum is starved (cost limits, too few workers) or dead tuples cannot
be removed because a long-running transaction pins the xmin horizon — check
`pg_stat_activity` for old `xact_start` values.

Visibility-map freshness, which determines whether index-only scans avoid
heap fetches:

```sql
SELECT c.relname,
       c.relpages,
       c.relallvisible,
       round(c.relallvisible::numeric / NULLIF(c.relpages, 0), 3)
           AS all_visible_ratio
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = current_schema()
  AND c.relname LIKE 'horsies_%'
ORDER BY c.relpages DESC;
```

`all_visible_ratio` near 1.0 means the visibility map covers the table and
index-only scans stay index-only. A ratio that decays between vacuums and
recovers after each one is normal; a ratio that stays low signals the same
starvation causes as above. Both `relallvisible` and `relpages` are updated
by vacuum and analyze, so read them as of the last vacuum, not as live
values.

## Things to Avoid

**Don't run `VACUUM FULL` on a live broker database.** It takes an
`ACCESS EXCLUSIVE` lock and rewrites the table, blocking claims, finalizes,
and retention for the duration. Plain `VACUUM` (which autovacuum runs) never
blocks DML.

**Don't disable autovacuum to "reduce load".** Dead tuples then accumulate
until every claim-path scan pays for them, and the eventual wraparound
vacuum is forced and aggressive. Lower the trigger instead, so each vacuum
run is small.
