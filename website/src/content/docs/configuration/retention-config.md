---
title: Retention Config
summary: How long records live, and how their storage is kept ahead.
related: [recovery-config, app-config, ../../tasks/sending-tasks]
tags: [configuration, retention, partitions]
---

## Overview

`AppConfig.retention` answers one question: **how long does a record
survive, and how is its storage prepared and reclaimed?**

That is a different question from recovery. Recovery is about work that
went wrong — stale claims, dead runners, the thresholds for noticing.
Retention is about work that went right and is now history. A knob that
decides when a record is deleted was never a recovery knob, so as of
this release it no longer lives on `RecoveryConfig`.

```python
from datetime import timedelta
from horsies import AppConfig, RetentionConfig, RetentionClassConfig

AppConfig(
    broker=...,
    retention=RetentionConfig(
        queue_retention={
            'emails': timedelta(days=7),
            'audit': None,
        },
        terminal_record_retention_hours=24 * 90,
        retention_classes=(
            RetentionClassConfig(key='audit_1y', duration=timedelta(days=365)),
        ),
    ),
)
```

## Retention per queue

`queue_retention` maps a queue name to how long a task sent on it keeps
its history record, or to `None` to keep it forever. It is the setting
most deployments want: retention usually varies by what the work *is*,
and the queue already says that.

```python
retention=RetentionConfig(
    queue_retention={
        'emails': timedelta(days=7),     # noisy, short-lived
        'reports': timedelta(days=90),
        'audit': None,                   # keep forever
    },
)
```

A queue with no entry is unchanged: its tasks take the immutable 30-day
class. Nothing about existing deployments moves until a queue is named.

### Which class a send gets

Highest precedence first:

1. **What the send asks for** — `with_options(retention_class_key=...)`.
   Naming `standard_30d` explicitly is a choice and beats the mapping.
2. **The queue's mapping**, if it has one.
3. **`standard_30d`**, the immutable 30-day default.

Omitting the argument therefore means "the queue's mapping if there is
one" — not `standard_30d` unconditionally.

The same precedence applies wherever a task is enqueued: `.send()`,
`.send_async()`, `.schedule()`, `.schedule_async()`, a `TaskSchedule`
firing on its cron, and a workflow node's backing task. A cron fire
resolves from the scheduler process's own configuration, and a workflow
node from the queue its node runs on. The class is chosen at enqueue in
every case.

```python
# On the mapped 'emails' queue: kept 7 days
await send_email.send_async(to=...)

# Same queue, this one send opted back to 30 days
await send_email.with_options(retention_class_key='standard_30d').send_async(to=...)
```

### Editing a mapping

The duration is part of the class the mapping derives, so changing a
number **mints a new class** rather than redefining one:

| Time | `queue_retention['emails']` | New sends land in | Older records |
|---|---|---|---|
| Monday | `timedelta(days=7)` | `q_emails_7d` | — |
| Friday | `timedelta(days=14)` | `q_emails_14d` | still in `q_emails_7d`, dropped on their 7-day promise |

Both classes exist, each holding the records it was created for. This is
deliberate: a class has exactly one duration, so rewriting it in place
would silently restate a promise already made to records sitting in its
partitions. Records enqueued under the old mapping age out under the old
mapping.

The same follows for the resolution point — the class is decided **when
the task is sent** and recorded on the row. A retry replays the class its
original send chose, so editing the map governs later sends and never
reaches a task already in flight.

### You will see the derived keys

The mapping is the interface; the derived class is its visible artifact.
Keys appear as `q_<queue>_<duration>` — `q_emails_7d`, `q_reports_90d`,
`q_ingest_36h` — in monitoring surfaces, in the `retention_class_key`
column, and as partition names (`horsies_task_history_q_emails_7d_...`).
A key like `q_emails_7d` means "sent on the `emails` queue while it was
mapped to 7 days". You can also name one explicitly at a send.

`q_` is reserved: a class declared in `retention_classes` may not use the
prefix, since durations there come from the mapping instead.

### What each mapping costs

A mapping keys on the queue *and* its duration, so two queues mapped to
the same duration mint two independent classes — `{'a': 7d, 'b': 7d}`
gives `q_a_7d` and `q_b_7d`, not one shared class. That is deliberate:
a class has one duration, and merging them would tie two queues whose
mappings can later diverge.

Each class carries its own parent relation, `history_leaf_horizon_days
+ 1` daily leaves, and two indexes per leaf, all maintained on every
pass. Ten mapped queues is roughly forty extra leaves and eighty extra
indexes at the default horizon. Map the queues whose retention actually
differs, rather than every queue.

### Length: roughly 13 characters of queue name

A class key is spliced into the relations the class owns, and the longest
of those is a per-leaf index name:

```
horsies_task_history_q_emails_7d_2026_08_11_enqueued_idx
```

PostgreSQL caps an identifier at 63 bytes and **truncates rather than
refuses** past it, so the budget is enforced where you can act on it — at
configuration, which refuses an over-long key and states the arithmetic.
The bound is **18 characters for a class key**. After `q_`, the separator
and a duration like `7d`, that leaves roughly **13 characters for a mapped
queue name**. The same bound applies to keys declared by hand in
`retention_classes`.

### Migrating from `queue_terminal_record_retention_hours`

`queue_retention` is the direct successor. The mapping is the same idea
with a duration instead of an hour count, and it now drops partitions
rather than deleting rows:

| 0.4.x | Here |
|---|---|
| `queue_terminal_record_retention_hours={'emails': 168}` | `queue_retention={'emails': timedelta(days=7)}` |
| `{'audit': None}` (no pruning) | `{'audit': None}` (forever) |
| Row delete, space returned by autovacuum | Partition drop, space returned at once |

## What ages by what

Three categories age by three mechanisms, and only one of them is a row
delete:

| Category | Mechanism | Setting |
|---|---|---|
| Terminal **task** records | **Partition drop** — records live in `horsies_task_history`, partitioned by the retention class assigned at enqueue; a partition past its class's window is dropped whole | `queue_retention` per queue, or `retention_class_key` per send |
| Heartbeats | **Partition drop** — hourly partitions drop whole | `heartbeat_leaf_horizon_hours` (coverage, not the window) |
| Terminal **workflow** records | **Row delete** in batches | `terminal_record_retention_hours` |
| Worker states | **Row delete**, same sweep | `worker_state_retention_hours` |

A partition drop returns its space to the filesystem at once. A row
delete marks tuples dead and returns nothing until autovacuum reaches
them, so the two are not interchangeable — which is why terminal task
records use the first.

## Retention classes

Every task carries a retention class, assigned at enqueue and
snapshotted on the row; its window cannot be changed afterwards. Which
class it gets follows the precedence above: what the send asks for, then
the queue's `queue_retention` mapping, then the immutable 30-day class.
An explicit `None` keeps the record forever. See
[Sending Tasks](../../tasks/sending-tasks#keep-a-terminal-record-forever).

### Declaring your own

A deployment can declare additional finite classes. Declaring a class
does not create it — the maintenance owner registers every declared
class at startup and on each pass, exactly as it registers the classes
the library ships, which keeps partition DDL out of application code.

```python
RetentionConfig(
    retention_classes=(
        RetentionClassConfig(key='audit_1y', duration=timedelta(days=365)),
        RetentionClassConfig(key='transient_2d', duration=timedelta(days=2)),
    )
)
```

Declarations are checked where you write them: a key must be a usable
identifier, must not be one the library owns (`standard_30d`, `forever`,
`heartbeats`), must not repeat, and its duration must be positive. Every
problem in a config is reported together. Re-declaring an existing key
with a *different* duration is refused at startup and named — classes
are immutable, because rows already carry the old window.

**`duration` is a minimum.** History partitions span one day and drop
only once the whole day is past the duration, so a row survives between
`duration` and `duration + 1 day`. Sub-day durations never under-retain
but cannot expire faster than daily granularity allows.

**Validation is per-process.** The acceptable class keys come from this
process's config, so the send-time check costs no database round trip.
A process whose config omits a class refuses it even if another
deployment registered that class in the same database — declare a class
in every process that sends into it.

## Partition coverage

| Setting | Default | Controls |
|---|---|---|
| `history_leaf_horizon_days` | 3 | complete future daily history partitions kept ahead of writes (2–14) |
| `heartbeat_leaf_horizon_hours` | 6 | complete future hourly heartbeat partitions kept ahead (2–48) |
| `partition_maintenance_interval_s` | 900 | seconds between coverage and pruning passes (60–3600) |

These decide how far **ahead** storage is prepared, not how long records
live. Raising `history_leaf_horizon_days` does not keep tasks longer.

The same pass that creates partitions ahead also drops those past their
horizon. A refused drop — recovery evidence still pinning it, or a
reader holding the detach past its timeout — is skipped, reported with
its reason on the worker health surface, and retried each pass until the
blocker clears.

A class that cannot be served is contained to itself. Its failure is
recorded against its own key, the remaining classes still get their
partitions, and the pass reports the refusal naming every class that
failed. Two properties follow, and both are the point:

- **The failure cannot spread.** Classes are served in key order, so an
  unbounded failure would deny partitions to every class sorting after
  it — a problem with one class becoming a coverage gap across the
  deployment.
- **The health surface goes red.** A refusal is a value the pass
  returns and the health surface reports. Coverage health tells you
  whether the owner could run, so a pass that cannot do its job must
  never read the same as one that succeeded.

### A partition dropped outside the library

Dropping a history partition directly — `DROP TABLE` against a leaf, or
a teardown that removes relations before the readers stop naming them —
leaves the leaf catalog saying the partition is attached while the
relation is gone. The next maintenance pass detects the divergence,
regenerates the readers without that partition, and reports it:

```
Partition coverage: cataloged leaves have no relation and were excluded
from the staged readers: horsies_task_history_standard_30d_2026_08_11.
Reads and terminalization are restored; the records those leaves held
are gone. The catalog rows are kept as evidence and resolving them is
an operator decision.
```

The name also appears under `absent_leaves` on the coverage health
surface.

What the pass does and does not restore:

| | State |
|---|---|
| Reads and terminalization | Restored automatically on the next pass |
| Records the partition held | Gone; they were in the dropped relation |
| Every other retention class | Unaffected |

One case needs the operator before coverage resumes. A partition inside
the coverage horizon — today's, or one of the next
`history_leaf_horizon_days` — cannot be recreated while its catalog row
survives, because the row and the database disagree about whether the
partition exists. That class stops gaining new partitions until the
disagreement is resolved. A partition older than the horizon is outside
what coverage creates, so nothing is blocked.

The library never resolves the disagreement on its own: a partition that
vanished unexpectedly is a fact an operator needs to see, and repairing
the catalog silently would destroy the evidence.

To resume coverage for the class, confirm the loss was intended, then
delete the catalog row for the leaf whose relation is gone:

```sql
DELETE FROM horsies_task_history_leaf_catalog
WHERE leaf_name = 'horsies_task_history_standard_30d_2026_08_11';
```

The next pass creates the class's partitions again from its horizon.
Records that lived in the dropped partition are not recoverable.

To remove partitions without entering this state, delete the catalog
rows first, let the readers republish, and drop the relations last.

## Row-delete sweep

| Setting | Default | Controls |
|---|---|---|
| `terminal_record_retention_hours` | 720 | terminal **workflow** rows; `None` disables |
| `worker_state_retention_hours` | 168 | worker-state snapshots; `None` disables |
| `retention_sweep_interval_s` | 300 | seconds between sweeps (30–86400) |
| `retention_delete_batch_size` | 500 | rows per DELETE batch (50–10000), one commit each |

Deleting a workflow also removes any unconsumed crashed-worker
progression evidence still waiting in the outbox, so retention is never
held up by a stalled consumer.

## Paused-workflow expiry

| Setting | Default | Controls |
|---|---|---|
| `paused_workflow_auto_cancel_after` | `None` | age past which a PAUSED workflow is expired by policy (`WorkflowStatus.EXPIRED`) |

`None` disables the sweep, so no deployment changes behaviour without
declaring the rule. This lives here rather than under recovery because
expiring a workflow that has sat paused past a declared age is a policy
about how long a record stays actionable — data lifecycle, not a
response to a crash.

## Moved from RecoveryConfig

These fields previously lived on `RecoveryConfig`. Setting one there now
fails at construction, naming its new home; there is no alias and no
deprecation shim, per the pre-1.0 posture.

```
error[HRS-204]: terminal_record_retention_hours moved to AppConfig.retention
   = note: it governs how long records live, not how stalled work is detected
   = help: set AppConfig.retention.terminal_record_retention_hours
```

Every misplaced field is reported at once, so one edit fixes the config.
