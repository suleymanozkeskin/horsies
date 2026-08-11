---
title: Migrating to 0.5.0
summary: The task-history live/history split (schema v34) — two upgrade paths, the offline cutover stages, and what each path keeps.
related: [../internals/database-schema, ../monitoring/action-semantics, ../configuration/recovery-config]
tags: [migration, task-history, 0.5.0, breaking-changes, cutover]
---

## What 0.5.0 changes

Terminal task records leave `horsies_tasks` in the same transaction that
terminalizes them, into `horsies_task_history` — partitioned by retention
class and day. The live table holds only `PENDING`, `CLAIMED`, and
`RUNNING` rows. Retention drops whole partitions instead of deleting
rows. Task and workflow identity columns are `uuid` (task ids UUIDv7).
Manual in-place retry is removed; re-execution is a new task minted
through the rerun API. Schema v34.

If you set `RecoveryConfig.queue_terminal_record_retention_hours` in
0.4.x, its successor is
[`AppConfig.retention.queue_retention`](../../configuration/retention-config#retention-per-queue):
the same per-queue idea, taking a `timedelta` (or `None` for forever)
instead of an hour count, and dropping partitions instead of deleting
rows. Setting the old field still fails validation, now naming that
successor.

horsies is pre-1.0: there is no migration contract between pre-1.0
versions and no compatibility shim. A 0.5.0 fleet cannot share a
database with an older fleet.

**Two upgrade paths exist. Most deployments should take Path A.**

| | Path A — new database | Path B — offline cutover |
|---|---|---|
| Keeps terminal history | No (old DB remains readable) | Yes |
| Keeps rerun of old tasks | No | Configurable |
| Keeps idempotency-key window | No (registry starts empty) | Yes |
| Downtime | Minutes (drain + re-point) | Offline window (measured below) |
| Complexity | Config change | Staged operator-driven program |

## Path A (recommended): drain and point at a new database

For every deployment whose terminal history is disposable — which is
most of them, since retention was going to delete it on schedule anyway.
A fresh 0.5.0 database is created at the final schema shape and is the
most heavily exercised install path in the test suite.

**The drain recipe:**

1. Stop your producers (your application stops calling `.send()` /
   schedules). horsies has no server-side intake switch; intake is
   app-side by design.
2. Leave workers running until the pending backlog reaches zero — watch
   queue depth in the dashboard or the monitoring API.
3. Stop workers. Shutdown is graceful: claiming stops and in-flight
   tasks complete.
4. Point the 0.5.0 deployment at a new database and start it. Migrations
   create the full schema on first broker init.

**What you give up, stated plainly:**

- Terminal history and its dashboard past. The old database does not
  vanish — leave it in place, readable, as an archive until you are
  comfortable dropping it.
- Rerun of pre-0.5.0 tasks (their records stay in the old database).
- Idempotency continuity: the key-reservation registry starts empty, so
  replay protection for enqueues made against the old database does not
  carry over.

## Path B: the offline cutover

For deployments that must keep terminal history in place — audit,
rerun-ability of old tasks, idempotency continuity, or an undrainable
backlog. Operator-driven, staged, offline: **stop every unit first**
(workers, producers, monitoring/web). There is no rolling-restart path.

**Worked example** (2 vCPU / 3 GB, PostgreSQL 18, 109,663 terminal
rows): total stage execution ≈ 2 minutes; preparation 52 s, relocation
37 s in flat, non-growing batches; the preflight estimate fitted from
the run's own trajectory landed within 1.2% of the actual, inside its
5/4 planning ceiling. Duration scales with row count; the estimate is
fitted from your own run, never transferred from someone else's
hardware.

### Before the window

1. **Decide retention for legacy rows.** Rows enqueued before 0.5.0
   carry no retention class. Preflight reports them with their size:
   *"N terminal rows (X MB live) carry no retention class; relocation
   will place them in the 'forever' class (no automatic aging);
   backfill a class before cutover to age them."* If you want them to
   age instead of living forever, assign a class first:

   ```sql
   UPDATE horsies_tasks
   SET retention_class_key = 'standard_30d'   -- or 'forever'
   WHERE retention_class_key IS NULL
     AND status NOT IN ('PENDING', 'CLAIMED', 'RUNNING');
   ```

   The backfill is optional for correctness: preparation accepts
   class-less rows and resolves them to `forever`, stamping the class
   back onto the row so the resolution is visible before relocation.
   **It is strongly recommended for monitoring performance.** A large
   `forever` population whose anchors predate the dashboard's default
   24-hour window is scanned in full by every history list call — the
   forever partition carries no time bounds, so the window cannot skip
   it, and on a measured ~110,000-row forever population that scan
   costs more than the daily leaves' entire read path. Rows in a
   finite class land in day partitions the window prunes structurally.
   Backfill a finite class unless the rows genuinely must live forever.

2. **Decide rerun retention.** With the shipped
   `retain_rerun_input_default = False`, pre-0.5.0 tasks are not
   rerunnable after cutover. Set the default to `True` before the
   window if you want legacy reruns; the preparation stage reports the
   split it produced (inline / over-bound / declined by policy /
   declined by failed decode).

3. **Drain the backlog to zero if you can.** Pending rows may legally
   survive a cutover: preparation covers every unprepared row,
   including surviving live ones, so the tighten's preconditions are
   met without manual work. An empty backlog remains the recommended
   posture — it is the simplest state to verify.

4. **Stop every unit**, then take a backup and verify it:

   ```sh
   pg_dump -Fc "$DATABASE_URL" -f pre-050-cutover.dump
   pg_restore --list pre-050-cutover.dump >/dev/null
   ```

   The tighten stage demands the backup's label typed back verbatim
   before it will run.

5. **Privileges:** migrations and the cutover run DDL; from 0.5.0
   onward the worker role also needs `CREATE` on the partition parents
   (workers create partitions ahead of writes). A deployment that
   withholds it must run an external coverage cron.

### The stages, in order

Upgrade the package, apply migrations (broker init applies the chain;
seconds — the v34 step also builds one enqueue-order index per existing
history leaf, so a database that already carries a large history pays
index-build time proportional to its size here), then run the stages.
Each is a typed entrypoint under `horsies.core.history.cutover`; each
refuses rather than proceeding on a violated precondition, and every
refusal names its reason.

| Stage | Entry point | Reversible? |
|---|---|---|
| 1. Preflight (read-only) | `run_preflight` | n/a — reads only |
| 2. Drain verification | `verify_drained` | n/a — reads only |
| 3. Identity normalization | `normalize_attempt_identity` | restore backup |
| 4. Program replacement | `install_programs` | teardown reinstates |
| 5. Envelope preparation | `prepare_legacy_batch` (batched, resumable) | rows are re-preparable |
| 6. Relocation | `relocate_terminal_batch` (ledgered, resumable) | resume-safe; restore backup to abort |
| 7. **Tighten — point of no return** | `tighten_to_frozen` | **backup restore only** |
| 8. Validation (read-only) | `validate_cutover` | n/a |

- The preflight inventories the work and (after a short fitted dry run)
  produces a duration estimate with its coefficients and a 5/4 planning
  ceiling as first-class fields.
- Preparation and relocation run in bounded committing batches and
  resume where they stopped; batch times do not grow with progress.
- Rows whose terminalization operation predates 0.4.6's recording carry
  a legacy marker in history rather than invented provenance.
- The tighten is entered only past typed refusals (backfill state,
  identity parseability, surviving-live-row preparation) and is crossed
  at its first statement. Before it: stop at any stage, run the
  teardown, restore nothing — the old fleet still works. After it: the
  backup is the only way back.
- Validation attests the frozen posture from catalog facts: live-only
  status domain, uuid identity, required enqueue-time columns.

### After the window

Start the upgraded units. Confirm the deploy by **schema version, not
version string** — a pre-release deploy from a branch still reports the
old `__version__`; `SELECT version FROM horsies_schema_version` is the
truth. First worker startup creates partition coverage ahead of writes;
the dashboard reads live and history transparently; new terminal tasks
appear in `horsies_task_history` within minutes.

## Both paths: what to check on day one

- `horsies_tasks` contains only live rows (zero terminal statuses).
- `horsies_task_history` grows as tasks terminalize.
- Heartbeat and history partitions exist ahead of need and the worker
  health surface shows coverage timestamps.
- The web UI's task detail resolves moved tasks from history.
