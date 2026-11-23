---
title: Database Schema
section: shared
summary: PostgreSQL tables for live tasks, the task-history archive, workflows, heartbeats, worker states, and schedules (migration chain 1000 and 1001).
related: [../../configuration/broker-config]
tags: [internals, database, schema, PostgreSQL, task-history]
---

## Schema Creation

The schema is a chain of migrations. Each migration runs in its own transaction, with its bookkeeping row:

| Version | Files | Holds |
| ------- | ----- | ----- |
| 1000 | `core/baseline/*.sql`, run in name order as one text | The baseline schema (the files below) |
| 1001 | `core/migrations/1001_lifecycle_record.sql` | The phase record of each attempt: two columns on `horsies_tasks`, nine columns on `horsies_task_attempts`, the attempt writers that fill them, and attempt archive version 2. The events of each workflow run: the table `horsies_workflow_events` and the moves and workflow steps that write it |

The files of migration 1000:

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
3. A database that holds the whole chain with this build's checksums, the `PYTHON` family row and every
   history reader: nothing to do, no lock. A database that holds the start of the chain (version 1000 only)
   gets the rest of the chain under the lock, each migration in its own transaction. A missing family row or a missing reader (an install that
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

Upgrade from 0.8.0 (version 1000) to version 1001:

- Stop every 0.8.0 process (workers, schedulers, producers, the monitoring server) before the first start of
  this build. The first start applies migration 1001. A 0.8.0 process that runs on after it calls functions whose
  argument list changed and gets SQLSTATE 42883; a 0.8.0 process that starts after it is refused, because the
  database holds a newer version.
- A database at version 1001 does not go back to a 0.8.0 build.
- The migration adds nullable columns with no default, so it rewrites no table. It takes `ACCESS EXCLUSIVE` on
  `horsies_tasks` and `horsies_task_attempts` until it commits, and each new `CHECK` reads its table once. The
  foreign keys of the new table `horsies_workflow_events` take `SHARE ROW EXCLUSIVE` on `horsies_workflows` and
  `horsies_workflow_tasks` until it commits.
- The migration writes no value into old rows. A live row of an attempt that began before it has
  `attempt_claims` NULL (claims not counted); an attempt row written before it has `phase_record_version` NULL
  (no phase record). It writes no workflow event: a run that began before it has the events of the steps after
  it only.

`horsies_migrations` has the sqlx form: `version`, `description`, `installed_on`, `success`, `checksum`
(SHA-384 of the migration text) and `execution_time` (nanoseconds). The text of a released migration never
changes: a database that holds another checksum for a version is refused. `horsies_schema_version` holds one row with
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
| `trace_carrier`, `trace_id` | `text`, `bytea` | The trace carrier of the send span, as one JSON text, and its 16-byte trace id; both set or both empty. Empty for a send with no trace, a carrier over a bound, and a schedule run. A workflow task copies the pair of its workflow. See [The Trace Carrier](/horsies/observability/tracing/#the-trace-carrier) |
| `attempt_claims` | `integer` | Claims of the current attempt (`retry_count + 1`). 0 before its first claim; each claim adds 1; a release and a stale claim requeue keep it; a retry sets 0. NULL: the attempt began before migration 1001, so its claims are not counted (a claim keeps NULL) |
| `first_claimed_at` | `timestamptz` | Time of the first claim of the current attempt. NULL when `attempt_claims` is 0 or NULL |

The `CHECK` `horsies_tasks_attempt_claims_shape` admits three pairs: both NULL; `attempt_claims` 0 with no
`first_claimed_at` on a `PENDING` row; `attempt_claims` 1 or more with `first_claimed_at` set.

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
`result_digest`), the attempt archive (`attempt_archive_version`, `attempt_snapshot` and its digest), and the rerun input envelope
(`rerun_input_disposition`: `INLINE`, `OVER_BOUND`, `DECLINED_BY_POLICY` or `NEVER_ELIGIBLE`). The move builds the
rerun envelope from the live row's `input` and `task_options`. It copies `trace_id` of the live row; the carrier is not
kept. The format version of each envelope implies its
codec (JSON, UTF-8).

The attempt archive has two versions; the core reads both:

- Version 1 (rows moved before migration 1001): a JSON array of attempts, each an array of 12 values (`attempt`,
  `outcome`, `will_retry`, `started_at`, `finished_at`, `error_code`, `error_message`, `failed_reason`,
  `worker_id`, `worker_hostname`, `worker_pid`, `worker_process_name`).
- Version 2 (`horsies_encode_attempt_archive`, every move from migration 1001): `[attempts, open]`. Each attempt
  is an array of 21 values: the 12 values of version 1, then the phase record (`phase_record_version`,
  `enqueued_at`, `claimed_at`, `attempt_claims`, `first_claimed_at`, `placed_at`, `finalizing_at`, `recorded_by`,
  `last_seen_at`). `open` is `null` when the last attempt ended with its row, else `[attempt_claims,
  first_claimed_at, finalizing_at]` of the attempt that has no row: a cancel, an expiry or a pause abandon ended
  it. Times are epoch microseconds; a missing value is `null`.

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
sub-workflow (depth 0-32; a root has depth 0 and no parent). `trace_carrier` and `trace_id` hold the pair of the start
span: every workflow row of a started tree holds it, and a sub-workflow that starts later copies the pair of its parent.

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

## horsies_workflow_events

The facts of a workflow run that no task row or node row holds, one row per event (migration 1001). The
statement that changes the state writes the event in the same transaction. No trigger writes an event.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `workflow_id` | `uuid` | The workflow of the event |
| `event_id` | `bigint` | The next value of the sequence `horsies_workflow_event_id`. It orders the events of one run: one statement can write several events at one `occurred_at` |
| `event_kind` | `varchar(16)` | `WORKFLOW_PAUSED`, `WORKFLOW_RESUMED`, `NODE_READY`, `NODE_ENQUEUED` or `TASK_MOVED` |
| `occurred_at` | `timestamptz` | The transaction time of the statement. For `TASK_MOVED`: the `terminal_at` of the history row |
| `workflow_node_row_id` | `uuid` | The node of a node event. NULL: a run event (`WORKFLOW_PAUSED`, `WORKFLOW_RESUMED`) |
| `task_id` | `uuid` | `TASK_MOVED` only: the task that moved |
| `history_class` | `varchar(64)` | `TASK_MOVED` only: the retention class of the history row |
| `terminalization_kind` | `varchar(32)` | `TASK_MOVED` only: the kind of the move |
| `run_workflow_id` | `uuid` | Generated: `workflow_id` on a run event, NULL on a node event |

The kinds and the statements that write them:

<!-- members: WorkflowEventKind -->
| Kind | Written by |
| ---- | ---------- |
| `WORKFLOW_PAUSED` | `horsies_workflow_pause` and the failure policy `pause` (`horsies_workflow_record_failure`): one event for the workflow, then one for each descendant that it paused |
| `WORKFLOW_RESUMED` | `horsies_workflow_resume`: one event for the workflow, then one for each descendant that it resumed |
| `NODE_READY` | A node that goes back to `READY` with no work: the abandon of a claimed task of a paused run (`horsies_workflow_pause`, `horsies_workflow_claimed_tasks_settle`, `horsies_workflow_task_stop_before_start`), the pause gate of a promotion (`horsies_workflow_promote`), a child start that finds its workflow not `RUNNING` (`horsies_workflow_child_start`) |
| `NODE_ENQUEUED` | A sub-workflow node that goes `ENQUEUED` and waits for its child start (`horsies_workflow_promote`, `horsies_workflow_promote_level`) |
| `TASK_MOVED` | The move to history of a task that a node links (`horsies_move_task_to_history`, `horsies_move_tasks_to_history`) |

No event is written for a node insert, a task node that goes `ENQUEUED` (its task row holds the time), a node that
goes `RUNNING`, or a node that goes terminal: the rows hold those times. A pause that a cancel or a paused expiry
ends has no end event: the `completed_at` of the run ends it. A plain task and a workflow task that no node links
have no event.

`history_class`, `occurred_at` and `task_id` of `TASK_MOVED` are the locator of the history row: the read of the
row probes one leaf, as the phase-2 read of a pending row does.

The `CHECK` `horsies_workflow_events_kind_shape` admits three shapes: a run event with no node, task, class or
move kind; a node event (`NODE_READY`, `NODE_ENQUEUED`) with a node and no task, class or move kind; `TASK_MOVED`
with a node, a task, a class (`forever` or `standard_<days>d`) and a move kind. The move kinds are the kinds of
`horsies_task_history` less `COMPLETE_FUSED` and `CANCEL_ADMIN`, which the move refuses for a workflow task.

Keys and indexes: the primary key `(workflow_id, event_id)` serves the read of one run and the retention delete.
A node event refers to its node (`(workflow_node_row_id, workflow_id)` to `horsies_workflow_tasks`, `ON DELETE
CASCADE`); a run event refers to its workflow through `run_workflow_id` (`ON DELETE CASCADE`). A node event does
not refer to the workflow row, so the move of a task, which holds the task row, takes no lock on the workflow row:
workflow steps lock workflow rows before task rows. Two partial indexes, on `workflow_node_row_id` and on
`run_workflow_id`, serve the two foreign keys.

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
| `phase_record_version` | `smallint` | 1: the attempt row has the phase record of migration 1001. NULL: written before it; every phase column below is NULL |
| `enqueued_at` | `timestamptz` | Due time of the attempt (the send's due time, or `next_retry_at` after a retry) |
| `claimed_at` | `timestamptz` | The claim generation that ran the attempt |
| `attempt_claims`, `first_claimed_at` | `integer`, `timestamptz` | Claims of the attempt (1 or more) and the time of its first claim. Both NULL: the claims were not counted (the attempt began before migration 1001) |
| `placed_at` | `timestamptz` | When a child of the worker took the task: `claimed_at` plus the slot wait that the worker measured on its monotonic clock. NULL: no placement observed (always for `RECOVERY`) |
| `finalizing_at` | `timestamptz` | The finalizing marker of the claim. NULL: no marker |
| `recorded_by` | `varchar(8)` | `WORKER` (the worker wrote the end) or `RECOVERY` (stale `RUNNING` recovery wrote it) |
| `last_seen_at` | `timestamptz` | `RECOVERY` only: the last time the attempt was known alive, the greatest of `started_at`, `finalizing_at` and the newest runner heartbeat of the attempt |

The statement that writes the attempt row writes the phase record from the locked live row:
`horsies_complete_task_fused`, `horsies_finish_workflow_success` and `horsies_finish_failed_attempt` (each with the
placement pair `p_placement_kind` `PLACED` or `NOT_OBSERVED` and `p_slot_wait_us`), and
`horsies_finish_stale_attempt`. `finished_at` is the time the attempt was recorded. The `CHECK`
`horsies_task_attempts_phase_record_shape` admits a row with no phase record, or a version-1 record with
`enqueued_at` and `claimed_at` set, both claim columns set or both NULL, `recorded_by` `WORKER` with no
`last_seen_at`, or `recorded_by` `RECOVERY` with `last_seen_at` and no `placed_at`. No `CHECK` orders the times:
a step of the database clock can break the order, and a write is never refused for it.

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
| `telemetry` | `jsonb` | The state of the telemetry parts of the worker: `{"endpoint": {"state": ...}, "handler_child": {"state": ...}}`. The endpoint state is `off`, `binding`, `serving` (with the bound address) or `down` (with the cause and `down_since`); the handler child state is `off`, `starting`, `up` (with its pid) or `down` (with the cause, `down_since` and what follows). The fleet collector counts the live workers whose latest snapshot has a part `down` |

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
| Terminal workflow records (`horsies_workflows`, `horsies_workflow_tasks`, `horsies_workflow_events`) | Batched row delete by the reaper sweep | `terminal_record_retention_hours` |
| Worker states | Batched row delete, same sweep | `worker_state_retention_hours` |
| Worker liveness generations (`horsies_worker_liveness`) | Batched row delete, same sweep: rows of workers that own no CLAIMED or RUNNING row and whose lock no session holds | none |

Workflow-record deletes run in batches of `retention_delete_batch_size` rows (default 500), one transaction per
batch. A batch counts the node rows and the event rows of its workflows, and always takes its first workflow, so a
workflow with more rows than the batch size is deleted alone. A batch deletes the events and the nodes of its
workflows with them; deleting a workflow also removes its unconsumed progression evidence and its recovery count
(the cascades above). A history class that is shorter than `terminal_record_retention_hours` drops the history
row of a moved task before the workflow retention deletes its `TASK_MOVED` event, so the locator of that event
then finds no row. See [Retention Config](../../configuration/retention-config#row-delete-sweep).

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

`core/baseline/` (migration 1000); `core/migrations/` (each later migration); `core/src/broker/migrations.rs` (the
install).
