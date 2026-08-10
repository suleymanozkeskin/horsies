---
title: Database Schema
summary: PostgreSQL tables for live tasks, the task-history archive, heartbeats, worker states, and schedules (schema v34).
related: [postgres-broker, operational-indexes, ../../configuration/broker-config, ../../migrations/migration-to-0-5-0]
tags: [internals, database, schema, PostgreSQL, task-history]
---

## The live/history split

From 0.5.0, terminal task records do not stay in `horsies_tasks`. A task
that reaches a terminal status leaves the live table in the same
transaction that terminalizes it, into the `horsies_task_history`
archive. A `CHECK` constraint on the live table admits only `PENDING`,
`CLAIMED`, and `RUNNING` — a query filtering `horsies_tasks` on a
terminal status is unsatisfiable, not merely out of date.

Task and workflow identity columns are native `uuid`; task ids are
UUIDv7, so id order follows creation time.

## horsies_tasks

Live task storage. Holds only `PENDING`, `CLAIMED`, and `RUNNING` rows
(CHECK-enforced).

| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | UUID PK | UUIDv7 task identifier |
| `task_name` | VARCHAR(255) | Registered task name |
| `queue_name` | VARCHAR(100) | Queue assignment |
| `priority` | INT | 1-100, lower = higher priority |
| `args` / `kwargs` | TEXT | JSON-serialized arguments |
| `status` | VARCHAR | PENDING/CLAIMED/RUNNING (live domain; terminal statuses live in history) |
| `sent_at` | TIMESTAMPTZ | Immutable call-site timestamp |
| `enqueued_at` | TIMESTAMPTZ | Dispatch timestamp (required; updated on retry) |
| `terminal_at` | TIMESTAMPTZ | Canonical terminal instant (set by the terminalizing statement; the row then moves) |
| `claimed_at` / `started_at` / `completed_at` / `failed_at` | TIMESTAMPTZ | Lifecycle timestamps |
| `result` | TEXT | JSON-serialized TaskResult |
| `failed_reason` / `error_code` | TEXT | Final failure summary (attempt-level detail lives in `horsies_task_attempts`) |
| `claimed`, `claimed_by_worker_id`, `claim_expires_at` | — | Claim lease (cleared when the lease ends) |
| `is_workflow_task` | BOOLEAN | Whether the row belongs to a workflow node |
| `finalizing_at`, `finalizing_by_worker_id` | — | Finalization handoff markers |
| `good_until` | TIMESTAMPTZ | Expiry deadline |
| `retry_count`, `max_retries`, `next_retry_at` | — | Automatic retry state |
| `task_options` | TEXT | Serialized TaskOptions |
| `enqueue_sha` | VARCHAR(64) | Required enqueue digest |
| `command_fingerprint`, `command_fingerprint_version` | BYTEA, INT | Canonical enqueue fingerprint |
| `retention_class_key` | TEXT | Retention class assigned at enqueue (required at rest; default is the 30-day class) |
| `input_digest` | BYTEA | Digest of the enqueue input |
| `rerun_of_task_id`, `rerun_root_task_id` | UUID | Rerun lineage (a rerun is a new task referencing its source) |
| `idempotency_key_digest` | BYTEA | Scoped idempotency key digest |
| `retain_rerun_input`, `prepared_rerun_input_*` | — | Rerun-input envelope: disposition, version, codec, content type, digest, inline bytes or reference |
| `worker_pid`, `worker_hostname`, `worker_process_name` | — | Executing process identity |
| `created_at`, `updated_at` | TIMESTAMPTZ | Row bookkeeping |

Enqueue-time facts (`enqueue_sha`, `retention_class_key`, the fingerprint
pair) are required columns with their declared checks from the first row
on a fresh install, and from the cutover's tighten stage on an upgraded
one.

## horsies_task_history

The terminal archive. Partitioned `LIST (retention_class_key)` at the
top; each finite retention class is sub-partitioned `RANGE
(retention_anchor_at)` into daily leaves; the `forever` class is a
single leaf with no time bounds (nothing in it expires). Records carry
the task's terminal projection plus an archived snapshot of its attempt
history; a record is immutable once written and ages with its class —
retention drops whole partitions, it never deletes rows.

Every leaf carries two indexes: `(task_id)` for point lookups and
`(enqueued_at)` for the monitoring list's default sort (the planner
merge-appends leaves in index order and stops at the LIMIT).

Supporting tables: `horsies_retention_classes` (class definitions;
immutable windows), the leaf catalog (which daily leaves exist, their
bounds, and reader publication), and `horsies_key_reservations` (the
scoped idempotency-key registry with its expiry index).

## Workflow progression outbox and quarantine

`horsies_workflow_phase2_pending` records the workflow progression a
terminal task's move owes its node — the crashed-worker recovery path
consumes it. Rows carry `attempt_count`, `last_attempt_at`, and
`last_failure_class`; a row that keeps refusing to resolve moves to
`horsies_workflow_phase2_quarantine` after a bounded attempt count with
its recovery evidence preserved. The pending row's locator is tied to
its workflow node with `ON DELETE CASCADE` — deleting a workflow removes
its unconsumed evidence.

## horsies_task_attempts

Per-attempt execution history **for live tasks**. One row per finished
attempt, written atomically with the task state transition. At
terminalization the attempts are archived into the history record's
snapshot and the live rows are deleted — the snapshot is the attempt
history's only home from then on.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | BIGSERIAL PK | Auto-increment |
| `task_id` | UUID FK | References `horsies_tasks(id)` with `ON DELETE CASCADE` |
| `attempt` | INT | 1-based attempt number |
| `outcome` | VARCHAR(32) | `COMPLETED`, `FAILED`, or `WORKER_FAILURE` (CHECK enforced) |
| `will_retry` | BOOLEAN | Whether a retry was scheduled after this attempt |
| `started_at` / `finished_at` | TIMESTAMPTZ | Attempt window |
| `error_code` / `error_message` / `failed_reason` | TEXT | Per-attempt failure detail |
| `worker_id`, `worker_hostname`, `worker_pid`, `worker_process_name` | — | Executing process identity |
| `created_at` | TIMESTAMPTZ | Row creation time |

Constraints: `UNIQUE (task_id, attempt)`. Pre-execution aborts
(`CLAIM_LOST`, `OWNERSHIP_UNCONFIRMED`, `WORKFLOW_CHECK_FAILED`,
`WORKFLOW_STOPPED`) do not create attempt rows.

## horsies_heartbeats

Task liveness tracking, partitioned by hour. Workers create partitions
ahead of writes; old partitions drop whole (there is no row-delete
retention window).

| Column | Type | Description |
| ------ | ---- | ----------- |
| `task_id` | UUID | Associated task |
| `sender_id` | VARCHAR(255) | Worker/process ID |
| `role` | VARCHAR(20) | 'claimer' or 'runner' |
| `sent_at` | TIMESTAMPTZ | Heartbeat time |
| `hostname` / `pid` | — | Sender identity |

## horsies_worker_states

Worker monitoring snapshots (timeseries). Each worker inserts one row per
`worker_state_snapshot_interval_ms` (default 30s).

| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | INT PK | Auto-increment |
| `worker_id` | VARCHAR(255) | Worker identifier |
| `snapshot_at` | TIMESTAMP | Snapshot time |
| `hostname` | VARCHAR(255) | Machine hostname |
| `pid` | INT | Main process ID |
| `processes` | INT | Worker process count |
| `queues` | VARCHAR[] | Subscribed queues |
| `tasks_running` | INT | Current running count |
| `tasks_claimed` | INT | Current claimed count |
| `memory_usage_mb` | FLOAT | Parent worker process resident memory |
| `children_memory_mb` | FLOAT | Summed resident memory of executor child processes |
| `cpu_percent` | FLOAT | CPU usage |
| `memory_percent` | FLOAT | Memory usage percentage |
| `max_claim_batch` | INT | Max tasks claimed per batch |
| `max_claim_per_worker` | INT | Max tasks claimable per worker |
| `cluster_wide_cap` | INT | Cluster-wide in-flight cap |
| `queue_priorities` | JSONB | Queue priority configuration |
| `queue_max_concurrency` | JSONB | Per-queue concurrency limits |
| `recovery_config` | JSONB | Recovery configuration snapshot |
| `worker_started_at` | TIMESTAMP | Worker start time |

## horsies_schedule_state

Scheduler execution tracking.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `schedule_name` | VARCHAR(255) PK | Schedule identifier |
| `last_run_at` | TIMESTAMP | Last execution time |
| `next_run_at` | TIMESTAMP | Next scheduled time |
| `last_task_id` | UUID | Most recent task ID |
| `run_count` | INT | Total executions |
| `config_hash` | VARCHAR(64) | Configuration hash |
| `updated_at` | TIMESTAMP | Last state update |

## Notifications

`task_new` fires from an insert trigger when a PENDING row is created
(with a per-queue channel beside it). `task_done` has two sources: the
status-change trigger for in-place transitions, and the terminalization
move itself — the move is a DELETE+INSERT the update trigger cannot see,
so the terminalizing statement performs the same `pg_notify('task_done',
task_id)` directly. The payload is the raw task id in both cases;
notifications are internal wake-up signals, not a public schema.

## Trust Boundary

Treat **database write access as task-execution privilege**: anyone who
can INSERT into `horsies_tasks` can run any registered task with any
kwargs that pass its typed signature, and can read stored results and
tracebacks. There is no separate authentication layer between the
tables and the workers (the same model as Celery with the JSON
serializer). Task names resolve only against the in-process registry —
rows cannot trigger arbitrary imports or code loading — but scope DB
credentials accordingly.

## Schema Creation

Tables are created by the broker's first initialization and evolved by
the versioned migration chain (`horsies_schema_version` records the
installed version; 0.5.0 is schema v34). Creation is protected by an
advisory lock. From 0.5.0 the worker role also needs `CREATE` on the
partition parents — workers create heartbeat and history partitions
ahead of writes; a deployment that withholds it must run an external
coverage cron.

## Automatic Retention Cleanup

| Data | Mechanism | Config |
|------|-----------|--------|
| Terminal task records | Partition drop by retention class (assigned at enqueue; default 30-day class, explicit `None` = forever) | none — per-task class |
| Heartbeats | Hourly partition drop | none |
| Terminal workflow records (`horsies_workflows`, `horsies_workflow_tasks`) | Batched row delete by the reaper sweep | `terminal_record_retention_hours` |
| Worker states | Batched row delete, same sweep | `worker_state_retention_hours` |

Workflow-record deletes run in batches of `retention_delete_batch_size`
rows (default 500), one transaction per batch; deleting a workflow also
removes its unconsumed progression evidence (the cascade above). See
[Recovery Config](../../configuration/recovery-config#retention-cleanup).

## File Location

`horsies/core/models/task_pg.py` (live tables);
`horsies/core/history/ddl/tables.py` (history DDL).
