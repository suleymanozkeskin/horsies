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

<!-- check: skip elided -- broker=... stands for the broker config shown on other pages -->
```python
from datetime import timedelta
from horsies import AppConfig, RetentionConfig

AppConfig(
    broker=...,
    retention=RetentionConfig(
        queue_retention={
            'emails': timedelta(days=7),
            'audit': None,
        },
        terminal_record_retention_hours=24 * 90,
        retention_periods=(timedelta(days=365),),
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

A queue with no entry is unchanged: its tasks take the 30-day default
class.

A period is **whole days, from 1 to 1825** (five years). The config
refuses any other value when it loads: a fractional day such as
`timedelta(hours=36)`, zero, or more than 1825 days. History partitions
are daily, so a finer period would only make another set of partitions
with the same behaviour. It is refused, never rounded.

Any configured queue name can be mapped, whatever its length or its
characters.

### A class is its period

A retention class is identified by its period, and its key is
`standard_<days>d`:

| Period | Class key |
|---|---|
| 7 days | `standard_7d` |
| 30 days (the default) | `standard_30d` |
| 365 days | `standard_365d` |

Queues mapped to the same period share one class:
`{'emails': timedelta(days=7), 'alerts': timedelta(days=7)}` stamps
`standard_7d` for both. You see the key in monitoring, in the
`retention_class_key` column, and in partition names
(`horsies_task_history_standard_7d_...`).

The key is a function of the period, so a key always means one duration.
Two processes cannot disagree about it, and a changed period cannot
change a class that already holds rows.

### Which class a send gets

Highest precedence first:

1. **What the send asks for** — `with_options(retention=...)`. Choosing
   30 days explicitly is a choice and beats the mapping.
2. **The queue's mapping**, if it has one.
3. **`standard_30d`**, the 30-day default.

Omitting the argument therefore means "the queue's mapping if there is
one" — not 30 days unconditionally.

The same precedence applies wherever a task is enqueued: `.send()`,
`.send_async()`, `.schedule()`, `.schedule_async()`, a `TaskSchedule`
firing on its cron, and a workflow node's backing task. A cron fire
resolves from the scheduler process's own configuration, and a workflow
node from the queue its node runs on. The class is chosen at enqueue in
every case.

<!-- check: skip partial -- uses names that the page does not define -->
```python
# On the mapped 'emails' queue: kept 7 days
await send_email.send_async(to=...)

# Same queue, this one send opted back to 30 days
await send_email.with_options(retention=timedelta(days=30)).send_async(to=...)
```

An explicit finite period must be one this process registers: the
default, a mapped period, or one in `retention_periods`. Any other period
is refused at the send call, before anything is written. `None` keeps the
record forever.

### Editing a mapping

A changed period is another class:

| Time | `queue_retention['emails']` | New sends land in | Older records |
|---|---|---|---|
| Monday | `timedelta(days=7)` | `standard_7d` | — |
| Friday | `timedelta(days=14)` | `standard_14d` | still in `standard_7d`, dropped on their 7-day promise |

The class is decided **when the task is sent** and recorded on the row. A
retry replays the class its original send chose, so editing the map
governs later sends and never reaches a task already in flight.

### What each class costs

Each class has its own parent table, one daily table for each day it
keeps plus the days kept ahead (`history_leaf_horizon_days`), and two
indexes on each daily table. All of them are created ahead and checked on
every maintenance pass. At the default horizon of 3 days, a 7-day class
keeps about 11 daily tables, a 365-day class about 369, and a 1825-day
class about 1,830. Queues with one period share them, so the cost grows
with the number of distinct periods, not with the number of queues.

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
| Terminal **task** records | **Partition drop** — records live in `horsies_task_history`, partitioned by the retention class assigned at enqueue; a partition past its class's window is dropped whole | `queue_retention` per queue, or `retention` per send |
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

### Periods a send chooses

A send can choose a finite period that no queue maps. List it in
`retention_periods`, so that the maintenance owner registers its class:

```python
RetentionConfig(
    retention_periods=(timedelta(days=365), timedelta(days=2)),
)
```

A listed period follows the same rule as a mapped one: whole days from 1
to 1825. A period listed twice is refused.

**A period is a minimum.** History partitions span one day and drop only
once the whole day is past the period, so a row survives between the
period and the period plus one day.

**Validation is per-process.** The periods a send may choose come from
this process's config, so the send-time check costs no database round
trip. A process whose config omits a period refuses it, even if another
process registered its class in the same database. The worker fleet's
config must contain every period that producers send: the maintenance
owner registers the classes of its own config, and a task whose class is
not registered has no partition to move into when it finishes.

### Classes from earlier versions

Classes made before 0.6.1 stay registered: the `q_<queue>_<duration>`
classes of `queue_retention`, and the classes declared by name in the
removed `retention_classes`. Their rows are deleted on their own
durations, and new sends never stamp them. Nothing needs to be removed.

A class declared by name before 0.6.1 can hold a key of the form
`standard_<n>d` with another duration, for example `standard_14d` with
7 days. A process that maps or lists 14 days then refuses to initialize
its broker, and the error names the key and both durations. Choose
another number of days for that period.

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
