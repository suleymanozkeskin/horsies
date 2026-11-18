---
title: Rerun in Your Transaction
section: python
summary: rerun_task - re-execute a terminal task as a new task, in the transaction of the caller - with its command, its enqueue policy, its outcomes and its errors.
related: [../monitoring/action-semantics, ../operations/atomic-enqueue-and-topology, ../configuration/retention-config, ../tasks/sending-tasks]
tags: [operations, rerun, rerun_task, RerunTask, RerunEnqueuePolicy, RerunOutcome, history, transaction]
---

# Rerun in Your Transaction

`rerun_task` re-executes a terminal task as a new task with a new id. It runs its statements on a SQLAlchemy `AsyncConnection` of the caller, so the new task commits or rolls back with the writes of the caller. Which tasks can be rerun, and what each outcome means, is on [Task rerun](/horsies/monitoring/action-semantics/#task-rerun).

## Rerun a Task

```python
from datetime import timedelta
from typing import assert_never
from uuid import UUID

from sqlalchemy.ext.asyncio import create_async_engine

from horsies import (
    RerunEnqueued,
    RerunEnqueuePolicy,
    RerunInputCorrupt,
    RerunInputUnavailable,
    RerunKeyConflict,
    RerunKeyReplay,
    RerunNotEligible,
    RerunOutcome,
    RerunSourceAbsent,
    RerunSourceCorrupt,
    RerunSourceLive,
    RerunTask,
    rerun_task,
)

engine = create_async_engine('postgresql+psycopg://user:pass@localhost:5432/mydb')


async def rerun_failed_task(source_task_id: UUID) -> RerunOutcome:
    async with engine.begin() as connection:
        return await rerun_task(
            connection,
            RerunTask(source_task_id=source_task_id, deadline=None),
            RerunEnqueuePolicy(
                retention=timedelta(days=30),
                retain_rerun_input=True,
                reservation_window=timedelta(hours=24),
            ),
        )


def describe(outcome: RerunOutcome) -> str:
    match outcome:
        case RerunEnqueued(new_task_id=new_task_id):
            return f'enqueued as {new_task_id}'
        case RerunKeyReplay(existing_task_id=existing_task_id):
            return f'this rerun exists: {existing_task_id}'
        case RerunKeyConflict(reserved_by_task_id=reserved_by_task_id):
            return f'the key is reserved by {reserved_by_task_id}'
        case RerunSourceLive(task_id=task_id):
            return f'{task_id} is still live'
        case RerunSourceAbsent(task_id=task_id, reason=reason):
            return f'{task_id} is held nowhere: {reason.value}'
        case RerunSourceCorrupt(task_id=task_id, exception=cause):
            return f'{task_id} does not decode: {cause}'
        case RerunNotEligible(task_id=task_id, reason=reason):
            return f'{task_id} is not eligible: {reason.value}'
        case RerunInputUnavailable(task_id=task_id, disposition=disposition):
            return f'{task_id} has no stored input: {disposition}'
        case RerunInputCorrupt(task_id=task_id, detail=detail):
            return f'{task_id} has a corrupt input: {detail}'
        case _ as unreachable:
            assert_never(unreachable)
```

`engine.begin()` commits when the block ends without an error. Add the writes of your application to the same block to commit them with the rerun.

## API Reference

### `async rerun_task(connection: AsyncConnection, command: RerunTask, policy: RerunEnqueuePolicy) -> RerunOutcome`

| Parameter | Type | Description |
|-----------|------|-------------|
| `connection` | `sqlalchemy.ext.asyncio.AsyncConnection` | The connection of the caller. Two statements run in its transaction: the read of the source, then, for a source that can be rerun, the enqueue of the new task |
| `command` | `RerunTask` | The source task and the choices of the new task |
| `policy` | `RerunEnqueuePolicy` | The enqueue policy of the new task |

**Returns:** `RerunOutcome`. Each outcome other than `RerunEnqueued` writes nothing.

**Raises:** each of these writes nothing.

| Error | When |
|-------|------|
| `ValueError` | The retention class of `policy.retention` is not registered: add the period to `queue_retention` or `retention_periods` of the maintenance owner, and rerun after its next pass. Also: `caller_key` is not a valid idempotency key, or the id of the new task could not be minted |
| `CorePreconditionFailed` (`horsies._core`) | The database was installed by the Rust client, or records no client. `subject` is `rerun_task`; `retryable` is `False` |
| `CoreError` (`horsies._core`) | The core of this process cannot run: see [Calls That Run on the Core](/horsies/tasks/error-types/#calls-that-run-on-the-core) |
| A database error | A statement failed. It raises from the driver of `connection` |

### `RerunTask`

A frozen dataclass.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `source_task_id` | `UUID` | required | The terminal task to rerun |
| `deadline` | `datetime \| None` | required | The `good_until` of the new task, or `None` for no deadline. It has no default, so each rerun states it |
| `caller_key` | `str \| None` | `None` | The idempotency key of the rerun. The key rules are on [Atomic Enqueue and Topology](/horsies/operations/atomic-enqueue-and-topology/#make-the-send-repeatable-with-an-idempotency-key) |

### `RerunEnqueuePolicy`

A frozen dataclass. The new task does not take the policy of its source.

| Field | Type | Description |
|-------|------|-------------|
| `retention` | `timedelta \| None` | The retention period of the new task, or `None` to keep its record forever. A period is whole days from 1 to 1825; see [Retention Config](/horsies/configuration/retention-config/) |
| `retain_rerun_input` | `bool` | `True` keeps the input of the new task, so that the new task can be rerun in turn |
| `reservation_window` | `timedelta` | How long `caller_key` stays reserved: above zero, at most 30 days |

The constructor raises `ValueError` for a period that is not whole days or is outside 1 to 1825 days, and for a window outside its range. The field `retention_class_key` (not an argument) is the class key of the period, for example `standard_30d`.

### `RerunOutcome`

`RerunOutcome` is a union of frozen dataclasses. Match every variant; `assert_never` in the last case makes a missing variant a type error.

| Variant | Fields | The outcome on [Task rerun](/horsies/monitoring/action-semantics/#task-rerun) |
|---------|--------|-----------------|
| `RerunEnqueued` | `new_task_id`, `source_task_id`, `rerun_root_task_id` | Enqueued |
| `RerunKeyReplay` | `existing_task_id` | Key replay |
| `RerunKeyConflict` | `task_id`, `reserved_by_task_id` | Key conflict |
| `RerunSourceLive` | `task_id` | Source live |
| `RerunSourceAbsent` | `task_id`, `reason: TaskAbsenceReason` | Source absent |
| `RerunSourceCorrupt` | `task_id`, `exception: HistoryContractError` (`horsies.core.history.errors`) | Source corrupt |
| `RerunNotEligible` | `task_id`, `reason: NotEligibleReason` (`COMPLETED_SOURCE`, `WORKFLOW_TASK`) | Not eligible |
| `RerunInputUnavailable` | `task_id`, `disposition: str`, the stored reason, for example `DECLINED_BY_POLICY` or `OVER_BOUND` | Input unavailable |
| `RerunInputCorrupt` | `task_id`, `detail: str` | Input corrupt |

`rerun_task`, `RerunTask`, `RerunEnqueuePolicy`, `RerunOutcome`, its variants, `TaskAbsenceReason` and `NotEligibleReason` are exported from `horsies`.
