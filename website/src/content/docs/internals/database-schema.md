---
title: Database Schema
section: shared
summary: PostgreSQL tables for live tasks, the task-history archive, workflows, heartbeats, worker states, and schedules (baseline schema, migration 1000).
related: [../../configuration/broker-config]
tags: [internals, database, schema, PostgreSQL, task-history]
---

## Schema Creation

The schema is one migration, version 1000: the SQL files in
`core/baseline/`, run in name order in one transaction.

| File | Holds |
| ---- | ----- |
| `0001_tables.sql` | Tables, constraints, the liveness generation sequence, seed rows, `horsies_uuidv7()`, the notification triggers |
| `0002_indexes.sql` | The indexes that no constraint creates |
| `0003_interim_functions.sql` | The SQL functions the client calls, and their composite types |
| `0004_lifecycle_functions.sql` | The task lifecycle functions: enqueue, claim, start, attempt finish, stale recovery, requeue, release, cancel |
| `0005_workflow_functions.sql` | The workflow engine functions: start, child start, phase 2, child completion, pause, resume, cancel, paused expiry, the claim-time check and the stop before start, recovery |
| `0006_history_functions.sql` | The history and retention functions: the move to history, the absence reason, the rerun source read, the prune candidates, the leaf detach and drop |
| `0007_scheduler_monitoring_functions.sql` | The schedule fire and slot id functions, and the monitoring reads: task detail, list, facets, status counts and breakdown, stale and expired lists, workflow names, runs, run and node, schedules |
| `0008_history_maintenance_functions.sql` | The history maintenance functions: the coverage probe, retention class registration, leaf creation, the reader publication and the miss classifier |

The broker installs the schema at its first initialization (`ensure_schema_initialized`, or the first
enqueue, result wait, task info read, workflow start or scheduler start):

1. It reads the state of the current schema, with no lock.
2. A database with no `horsies_` table: it takes the schema advisory lock on a direct connection to the
   session URL, reads the state again, and runs the migration with its bookkeeping row in
   `horsies_migrations` and the row `PYTHON` in `horsies_client_family`, in one transaction. Then it
   publishes the history readers (`horsies_task_lookup_staged`, `horsies_task_provenance_staged`,
   `horsies_task_detail_staged`, `horsies_task_result_staged`, `horsies_task_history_duplicate_staged`)
   from the leaf catalog, in a second transaction, before it returns.
3. A database that holds version 1000 with this build's checksum, the `PYTHON` family row and every
   history reader: nothing to do, no lock. A missing family row or a missing reader (an install that
   stopped between its two transactions): it takes the lock, publishes the readers and writes the family
   row.
4. Any other database is refused with `SCHEMA_INIT_FAILED`, and nothing is changed. The refusal names what the
   database holds: a horsies 0.x schema, `horsies_` tables with no bookkeeping, a newer version, a migration
   recorded as failed, a migration applied from another text (checksum), migration versions this build does not
   ship, or the other client family.
5. A family row whose value is neither `PYTHON` nor `RUST` is corrupt stored data: the CHECK
   `horsies_client_family_family_known` refuses such a value. The install, every write and a rerun are refused
   with `CoreContractViolation`, whose message names the stored value, and nothing is changed.

There is no upgrade from a 0.x database. Drain it and point the fleet at a new database.

`horsies_migrations` has the sqlx form: `version`, `description`, `installed_on`, `success`, `checksum`
(SHA-384 of the migration text) and `execution_time` (nanoseconds). `horsies_schema_version` holds one row with
version 1000 only so that a 0.x process that runs migrations stops with "database schema is newer than this build
supports".

A broker with `run_schema_migrations=False` runs no schema statement. The monitoring tool never runs one; it
reads the highest successful version in `horsies_migrations`.

The schema has no database defaults: every insert names every value.

## The live/history split

A task that reaches a terminal status leaves `horsies_tasks` in the same transaction that terminalizes it, into
the `horsies_task_history` archive. A `CHECK` on the live table admits only `PENDING`, `CLAIMED` and `RUNNING`.

Task and workflow identity columns are native `uuid`; task ids are UUIDv7, so id order follows creation time.

## horsies_tasks

Live task storage.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | `uuid` PK | UUIDv7 task identifier |
| `task_name` | `varchar(255)` | Registered task name |
| `queue_name` | `varchar(100)` | Queue; at most 52 bytes, so the channel `task_queue_<name>` fits PostgreSQL's 63-byte limit |
| `priority` | `integer` | 1-100, lower runs first |
| `input` | `text` | The encoded task input, one JSON value |
| `task_options` | `text` | Serialized task options |
| `execution` | `text` | `ISOLATED`, `SHARED_ASYNC` or `SHARED_THREAD` |
| `retention_class_key` | `varchar(64)` | `forever` or `standard_<days>d`, days 1-1825 |
| `sent_at` | `timestamptz` | Call time of the send (the slot time for a schedule, the start call time for a workflow task) |
| `created_at` | `timestamptz` | Row creation |
| `command_fingerprint_version`, `command_fingerprint` | `smallint`, `bytea` | The command fingerprint; set on a sent task, empty on a workflow task |
| `idempotency_key_digest` | `bytea` | Scoped idempotency key digest, 32 bytes |
| `status` | `varchar(9)` | `PENDING`, `CLAIMED` or `RUNNING`; `claim_expires_at` is set exactly when `CLAIMED` |
| `enqueued_at` | `timestamptz` | Dispatch time (moves on retry) |
| `claimed_at`, `claimed_by_worker_id`, `claim_expires_at` | | Claim lease |
| `started_at`, `finalizing_at`, `finalizing_by_worker_id` | | Start and finalization markers |
| `worker_pid`, `worker_hostname`, `worker_process_name` | | Executing process identity |
| `retry_count`, `max_retries`, `next_retry_at` | | Automatic retry state; `max_retries` 0-20 |
| `good_until` | `timestamptz` | Expiry deadline |
| `is_workflow_task` | `boolean` | Whether a workflow node owns the row |
| `updated_at` | `timestamptz` | Last write |
| `retain_rerun_input` | `boolean` | Whether history keeps the input for a rerun |
| `rerun_of_task_id`, `rerun_root_task_id` | `uuid` | Rerun lineage, both set or both empty |

The claim reads `horsies_tasks_claim_pending` and `horsies_tasks_claim_expired_order`, keyed `(queue_name,
execution, priority, enqueued_at, id)` over `PENDING` and `CLAIMED` rows, and `horsies_tasks_claim_expired_at`,
keyed `(queue_name, claim_expires_at)` over `CLAIMED` rows.

## horsies_task_history

The terminal archive. Partitioned `LIST (retention_class_key)`; each retention class is partitioned
`RANGE (terminal_at)` into daily leaves. The `forever` class has one static leaf,
`horsies_task_history_forever_open`, from `-infinity`. A record is immutable once written; retention drops
whole partitions and never deletes rows.

A record holds the task's terminal projection (status, `terminalization_kind`, `terminal_at`, the final error
code and failed reason, the last worker), the result envelope (`result_envelope_version`, `result_payload`,
`result_digest`), the attempt archive (`attempt_snapshot` and its digest), and the rerun input envelope
(`rerun_input_disposition`: `INLINE`, `OVER_BOUND`, `DECLINED_BY_POLICY` or `NEVER_ELIGIBLE`). The move builds the
rerun envelope from the live row's `input` and `task_options`. The format version of each envelope implies its
codec (JSON, UTF-8).

Every leaf carries two indexes: `(task_id)` for point lookups and `(enqueued_at)` for the monitoring list's
default sort.

Supporting tables: `horsies_retention_classes` (`class_kind` `FOREVER`, `PERIOD` with `retention_days`, or
`HEARTBEAT` with `heartbeat_horizon_hours`), `horsies_task_history_leaf_catalog` (which leaves exist, their
bounds, when they were detached and dropped), `horsies_task_lookup_manifest` (the leaves the published readers
probe), `horsies_key_reservations` (the scoped idempotency-key registry) and
`horsies_task_history_clock_exceptions` (task ids whose UUIDv7 time is later than their terminal time).

## Workflows

`horsies_workflows` holds one row per run: status (`PENDING`, `RUNNING`, `PAUSED`, `COMPLETED`, `FAILED`,
`CANCELLED`, `EXPIRED`), `on_error` (`fail` or `pause`), the result or error, and the parent link of a
sub-workflow (depth 0-32; a root has depth 0 and no parent).

`horsies_workflow_tasks` holds one row per node: `input`, the dependencies and join (`all`, `any`, or `quorum`
with `min_success`), status, the linked task id, the sub-workflow fields of a sub-workflow node, and the typed
facts of a task node (`max_retries`, `execution`, `good_until`, `retention_class_key`). A sub-workflow node also
stores `sub_workflow_params`: `GIVEN` when the node was given params for the child's `build_with`, `NOT_GIVEN`
when it was given none; a task node stores NULL. Every node stores `downstream_subworkflow`, written at start from
the dependency graph: `REACHABLE` when a sub-workflow node depends on it directly or through other nodes, `NONE`
otherwise.

## Workflow progression outbox and quarantine

`horsies_workflow_phase2_pending` records the workflow progression a terminal task's move owes its node. The
phase 2 of the worker that advances the node deletes the row in the same transaction; a row is left only when
that phase 2 did not commit, and the crashed-worker recovery path consumes it. Its locator is either `HISTORY`
(`history_class`, `history_anchor`) or `QUARANTINE` (`quarantine_task_id`). A row that keeps refusing to resolve
moves to `horsies_workflow_phase2_quarantine` after a bounded attempt count. Both tables refer to their node
with `ON DELETE CASCADE`.

`horsies_workflow_recovery_failures` holds the count of the workflow recovery limit: one row for each workflow
whose recovery raised a persistent error, deleted with its workflow (`ON DELETE CASCADE`). See
[Workflow recovery limit](../../configuration/recovery-config#workflow-recovery-limit).

| Column | Type | Description |
|--------|------|-------------|
| `workflow_id` | `uuid` | Primary key; the workflow that failed recovery |
| `failed_passes` | `integer` | Count of failed passes, at least 1 |
| `last_pass_id` | `uuid` | The last pass that counted |
| `first_failed_at` | `timestamptz` | First failed pass |
| `last_failed_at` | `timestamptz` | Last failed pass |
| `last_case` | `varchar(40)` | Recovery case of the last failure |
| `last_task_index` | `integer` | Task index of the last failure, when the case names a node |
| `last_error_type` | `varchar(255)` | Exception type of the last failure |

## horsies_task_attempts

Per-attempt execution history for live tasks. One row per finished attempt, written with the task state
transition. At terminalization the attempts are archived into the history record's snapshot and the live rows
are deleted.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `task_id` | `uuid` | References `horsies_tasks(id)` with no delete action: the move deletes the attempts first |
| `attempt` | `integer` | 1-based attempt number, 1-21 |
| `outcome` | `varchar(14)` | `COMPLETED`, `FAILED` or `WORKER_FAILURE`; a `COMPLETED` attempt never retries |
| `will_retry` | `boolean` | Whether a retry was scheduled after this attempt |
| `started_at`, `finished_at` | `timestamptz` | Attempt window |
| `error_code`, `error_message`, `failed_reason` | `text` | Per-attempt failure detail |
| `worker_id`, `worker_hostname`, `worker_pid`, `worker_process_name` | | Executing process identity |

Primary key: `(task_id, attempt)`. Pre-execution aborts (`CLAIM_LOST`, `OWNERSHIP_UNCONFIRMED`,
`WORKFLOW_CHECK_FAILED`, `WORKFLOW_STOPPED`) do not create attempt rows.

## horsies_heartbeats

Task liveness, partitioned by hour on `sent_at`. Workers create partitions ahead of writes; old partitions drop
whole.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `task_id` | `uuid` | Associated task |
| `role` | `varchar(7)` | `claimer` or `runner` |
| `sent_at` | `timestamptz` | Heartbeat time |
| `sender_id` | `varchar(255)` | Worker or process id |
| `hostname`, `pid` | | Sender identity |

## horsies_worker_states

Worker monitoring snapshots (timeseries). Each worker inserts one row per `worker_state_snapshot_interval_ms`
(default 30 s). The unique index `(worker_id, snapshot_at DESC)` is the row identity.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `worker_id` | `varchar(255)` | Worker identifier |
| `snapshot_at` | `timestamptz` | Snapshot time |
| `hostname`, `pid` | | Main process identity |
| `processes` | `integer` | Worker process count |
| `max_claim_batch`, `max_claim_per_worker`, `cluster_wide_cap` | `integer` | Claim limits |
| `queues` | `varchar(100)[]` | Subscribed queues |
| `queue_priorities`, `queue_max_concurrency`, `recovery_config` | `jsonb` | Configuration snapshot |
| `tasks_running`, `tasks_claimed` | `integer` | Current counts |
| `memory_usage_mb`, `memory_percent`, `cpu_percent`, `children_memory_mb` | `double precision` | Resource use |
| `worker_started_at` | `timestamptz` | Worker start time |
| `slots_down` | `jsonb` | Each slot that is down for a cause that can clear: `{slot, cause, detail, down_since, failed_boots}`; an empty array when none is |
| `class_capacity` | `jsonb` | Each claim budget class to `{"capacity": N, "serves": [classes]}` |

## horsies_worker_liveness

The generation of the last acquisition of each worker's liveness lock: one row per worker id, written in the
transaction of each acquisition (`horsies_take_worker_liveness`). The values come from the sequence
`horsies_worker_liveness_generation`, so no value is used twice. A reaper judges a worker gone only when its passes
see one generation; see [Worker Liveness Lock](/horsies/workers/heartbeats-recovery/#worker-liveness-lock).

| Column | Type | Description |
| ------ | ---- | ----------- |
| `worker_id` | `text` PK | Worker identifier |
| `generation` | `bigint` | Generation of the last acquisition |

## horsies_schedule_state

Scheduler execution tracking.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `schedule_name` | `varchar(255)` PK | Schedule identifier |
| `next_run_at` | `timestamptz` | Next scheduled time |
| `last_run_at` | `timestamptz` | Last execution time |
| `last_task_id` | `uuid` | Most recent task id; set exactly when `last_run_at` is |
| `run_count` | `integer` | Total executions |
| `config_hash` | `bytea` | Configuration hash, 32 bytes |
| `updated_at` | `timestamptz` | Last state update |

## Notifications

A task insert sends `task_new` (the task id), `task_queue_<queue>` (the payload `new`) and
`horsies_task_status` (the task id). A status change other than the change to `RUNNING` sends
`horsies_task_status`. The terminalization move sends `task_done` and `horsies_task_status` itself, because the
move deletes the live row. A workflow insert and each workflow status change send `horsies_workflow_status`; a
change to a terminal status or `PAUSED` also sends `workflow_done` first. A worker snapshot sends
`horsies_worker_state`. Notifications are internal wake-up signals, not a public schema.

## Trust Boundary

Treat **database write access as task-execution privilege**: anyone who can INSERT into `horsies_tasks` can run
any registered task with any input that passes its typed signature, and can read stored results and tracebacks.
There is no separate authentication layer between the tables and the workers. Task names resolve only against
the in-process registry, so rows cannot trigger arbitrary imports or code loading, but scope DB credentials
accordingly.

The worker role also needs `CREATE` on the partition parents: workers create heartbeat and history partitions
ahead of writes. A deployment that withholds it must run an external coverage job.

## Automatic Retention Cleanup

| Data | Mechanism | Config |
|------|-----------|--------|
| Terminal task records | Partition drop by retention class (assigned at enqueue; default 30-day class, explicit `None` = forever) | none — per-task class |
| Heartbeats | Hourly partition drop | none |
| Terminal workflow records (`horsies_workflows`, `horsies_workflow_tasks`) | Batched row delete by the reaper sweep | `terminal_record_retention_hours` |
| Worker states | Batched row delete, same sweep | `worker_state_retention_hours` |
| Worker liveness generations (`horsies_worker_liveness`) | Batched row delete, same sweep: rows of workers that own no CLAIMED or RUNNING row and whose lock no session holds | none |

Workflow-record deletes run in batches of `retention_delete_batch_size` rows (default 500), one transaction per
batch; deleting a workflow also removes its unconsumed progression evidence and its recovery count (the cascades
above). See [Retention Config](../../configuration/retention-config#row-delete-sweep).

The partition-drop mechanism's cost character, measured with the
mechanism as the only variable (the delete arm reproducing the
pre-0.5.0 retention statement shape): **deleting scales with the
retired population; dropping does not**. Net of round trips, retiring
100,000 rows cost ~8.2 s by batched delete against ~111 ms by drop;
300,000 rows cost ~25.8 s against ~112 ms. A delete also reclaims no
space at the moment it runs — tuples are marked dead, vacuum later
returns most of the space to reuse, and roughly a quarter of the
original file stays allocated unless `VACUUM FULL` takes an exclusive
lock. A drop unlinks the partition's files: the full size returns
immediately, with no vacuum debt. (The comparison's arms are
shape-faithful to what each version actually retires, not
byte-identical.)

## File Location

`core/baseline/` (the schema); `core/src/broker/migrations.rs` (the install).
