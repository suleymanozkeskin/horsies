---
title: Database Schema
summary: Live tasks, partitioned task history, heartbeats, workflows, and cutover state after migration 0042.
related: [operational-indexes, ../../configuration/retention-config, ../../operations/cutover-runbook]
tags: [internals, database, schema, PostgreSQL, task-history]
---

## Live and history storage

`horsies_tasks` holds live work only. Its status constraint accepts
`PENDING`, `CLAIMED`, and `RUNNING`.

A terminalization statement moves the task to `horsies_task_history`. The
move is atomic. The history row and the terminal outcome commit together.

Task and workflow identity columns use PostgreSQL `uuid`. New task IDs use
UUIDv7. The UUIDv7 time is a lookup hint. It is not proof that a task is
absent.

## `horsies_tasks`

The live task table contains claimable and executing work.

| Column | Type | Meaning |
|---|---|---|
| `id` | UUID PK | UUIDv7 task ID |
| `task_name` | VARCHAR(255) | Registered task name |
| `queue_name` | VARCHAR(100) | Queue |
| `priority` | INT | Priority from 1 to 100 |
| `args`, `kwargs` | TEXT | JSON task input |
| `status` | VARCHAR | `PENDING`, `CLAIMED`, or `RUNNING` |
| `sent_at` | TIMESTAMPTZ | Call time |
| `enqueued_at` | TIMESTAMPTZ | Dispatch time |
| `claimed_at`, `started_at` | TIMESTAMPTZ | Live lifecycle times |
| `good_until` | TIMESTAMPTZ | Start deadline |
| `retry_count`, `max_retries`, `next_retry_at` | mixed | Retry state |
| `task_options` | TEXT | Serialized task options |
| `is_workflow_task` | BOOLEAN | Workflow backing-task marker |
| `finalizing_at`, `finalizing_by_worker_id` | mixed | Finalization handoff |
| `enqueue_sha` | VARCHAR(64) | Retry payload digest |
| `command_fingerprint_version` | SMALLINT | Fingerprint format |
| `command_fingerprint` | BYTEA | Canonical enqueue fingerprint |
| `retention_class_key` | TEXT | Class fixed at enqueue |
| `input_digest` | BYTEA | Canonical input digest |
| `rerun_of_task_id` | UUID | Direct rerun source |
| `rerun_root_task_id` | UUID | First task in the rerun chain |
| `idempotency_key_digest` | BYTEA | Scoped key digest |
| `retain_rerun_input` | BOOLEAN | Input retention policy |
| `prepared_rerun_input_*` | mixed | Prepared input envelope or refusal |
| `worker_pid`, `worker_hostname`, `worker_process_name` | mixed | Worker identity |
| `created_at`, `updated_at` | TIMESTAMPTZ | Row times |

The fingerprint, retention class, rerun-input policy, and prepared disposition
are required at rest.

## `horsies_task_history`

The history table stores immutable terminal tasks. It is partitioned in two
levels:

1. `LIST (retention_class_key)` selects the class.
2. `RANGE (retention_anchor_at)` selects a UTC day.

`standard_30d` keeps records for at least 30 days. Declared and queue-derived
classes use their configured duration. `forever` uses daily range leaves but
never prunes them.

Each history leaf has two indexes:

- a btree on `task_id` for point reads
- a btree on `enqueued_at` for bounded lists and default ordering

The history row carries the terminal task projection. It also carries a
verified attempt snapshot. It stores result and rerun-input envelope metadata
with their digests.

History retention drops whole leaves. It does not delete terminal task rows.

## Staged history readers

Point reads call staged database functions:

- `horsies_task_lookup_staged(uuid)`
- `horsies_task_provenance_staged(uuid, boolean)`
- `horsies_task_detail_staged(uuid)`

The worker publishes all three functions in one transaction. Their static leaf
list lets PostgreSQL plan direct probes.

A valid UUIDv7 time narrows the first probes. A five-second clock bound widens
the likely range. The reader then probes every skipped leaf before it returns
absence. Non-v7 UUIDs probe all leaves.

The leaf catalog keeps the verified minimum UUID birth time. The absence floor
uses the complete attached catalog. A missing relation is excluded from probes
but does not rewrite that floor.

## Retention support tables

`horsies_retention_classes` stores immutable class definitions. Finite classes
store a duration, a daily interval, and a class parent.

`horsies_task_history_leaf_catalog` stores leaf bounds and publication facts.
It also records pruning and missing-relation state.

`horsies_key_reservations` stores scoped idempotency reservations. A key claim
can apply, replay the owning task, or report a fingerprint conflict.

## Workflow progression evidence

`horsies_workflow_phase2_pending` is the workflow progression outbox. A
terminal workflow task writes this evidence in the same transaction that moves
the backing task to history.

The worker consumes pending evidence after the configured grace period. Each
row tracks its attempt count, last attempt time, and last failure class.

`horsies_workflow_phase2_quarantine` stores evidence that crossed the configured
attempt bound. Quarantine stops repeated discovery. The source facts remain
available for inspection.

Deleting a workflow cascades to its unconsumed pending evidence.

## `horsies_task_attempts`

This table holds attempt rows for live tasks.

| Column | Type | Meaning |
|---|---|---|
| `id` | BIGSERIAL PK | Attempt row ID |
| `task_id` | UUID FK | Live task with `ON DELETE CASCADE` |
| `attempt` | INT | One-based attempt number |
| `outcome` | VARCHAR(32) | `COMPLETED`, `FAILED`, or `WORKER_FAILURE` |
| `will_retry` | BOOLEAN | Retry decision |
| `started_at`, `finished_at` | TIMESTAMPTZ | Attempt window |
| `error_code`, `error_message`, `failed_reason` | TEXT | Attempt failure facts |
| `worker_id`, `worker_hostname`, `worker_pid`, `worker_process_name` | mixed | Worker facts |
| `created_at` | TIMESTAMPTZ | Row time |

`UNIQUE (task_id, attempt)` prevents duplicate attempt numbers.

Terminalization encodes all attempts into the history snapshot. It deletes the
live attempt rows in the same transaction. The snapshot is the only attempt
record after the task moves.

## `horsies_heartbeats`

Heartbeats use PostgreSQL `uuid` task IDs. The table is partitioned by
`RANGE (sent_at)` into hourly leaves.

The worker creates leaves ahead of writes. It drops old leaves during the same
maintenance pass. There is no heartbeat row-delete sweep.

Each leaf has `(task_id, role, sent_at DESC)` for stale-task checks.

## Workflow tables

`horsies_workflows.id`, parent IDs, and root IDs use `uuid`.
`horsies_workflow_tasks.id`, workflow IDs, task IDs, and sub-workflow IDs also
use `uuid`.

Workflow status accepts `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `PAUSED`,
`CANCELLED`, and `EXPIRED`. `EXPIRED` is terminal.

The workflow-node status domain is `PENDING`, `READY`, `ENQUEUED`, `RUNNING`,
`COMPLETED`, `FAILED`, and `SKIPPED`.

## Other tables

`horsies_worker_states` stores monitoring snapshots. Retention deletes old
rows in batches.

`horsies_schedule_state.last_task_id` uses `uuid`. The table stores the last
and next schedule times, run count, and config hash.

## Notifications

Task inserts notify `task_new` and the queue channel. Terminal task moves
notify `task_done` directly. Workflow and worker-state triggers notify their
monitoring channels.

Notifications are wake-up signals. Readers always check stored state.

## Migration and cutover state

`horsies_migrations` records the Rust migration chain. The task-history release
ends at migration 0042. Its final table shape matches task-history schema v35.

`horsies_cutover_state` records offline cutover completion. Normal startup
requires this row:

```text
task_history_v1_validated_v1
```

The schema version alone does not authorize startup. Validation writes the row
only after it checks identity types, foreign keys, live status constraints,
history partitions, and relocation ledger totals.

The broker refuses an older migration version. It also refuses a newer version.
See the [task-history cutover runbook](../../operations/cutover-runbook) for an
upgrade from migration 0032.

The worker role needs `CREATE` on the history and heartbeat partition parents.
Use an external coverage job when that privilege is not available.

## Cleanup summary

| Data | Cleanup |
|---|---|
| Terminal tasks | Drop history leaves by retention class |
| Heartbeats | Drop hourly leaves |
| Terminal workflows and workflow nodes | Batched row delete |
| Worker states | Batched row delete |

See [Retention Config](../../configuration/retention-config).
