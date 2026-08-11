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
        terminal_record_retention_hours=24 * 90,
        retention_classes=(
            RetentionClassConfig(key='audit_1y', duration=timedelta(days=365)),
        ),
    ),
)
```

## What ages by what

Three categories age by three mechanisms, and only one of them is a row
delete:

| Category | Mechanism | Setting |
|---|---|---|
| Terminal **task** records | **Partition drop** — records live in `horsies_task_history`, partitioned by the retention class assigned at enqueue; a partition past its class's window is dropped whole | retention class per task, not a setting here |
| Heartbeats | **Partition drop** — hourly partitions drop whole | `heartbeat_leaf_horizon_hours` (coverage, not the window) |
| Terminal **workflow** records | **Row delete** in batches | `terminal_record_retention_hours` |
| Worker states | **Row delete**, same sweep | `worker_state_retention_hours` |

A partition drop returns its space to the filesystem at once. A row
delete marks tuples dead and returns nothing until autovacuum reaches
them, so the two are not interchangeable — which is why terminal task
records use the first.

## Retention classes

Every task carries a retention class, assigned at enqueue and
snapshotted on the row; its window cannot be changed afterwards.
Omitting the parameter uses the immutable 30-day class, and an explicit
`None` keeps the record forever. See
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
