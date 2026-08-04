"""Executing a terminalization command, from either driver, one way.

Both adapters build the same call from the same command and hand the row to
the same decoder. Neither interprets a column: a driver that decides what a
returned row means is a driver that can decide differently from the other one,
which is the failure this boundary exists to prevent.

The accepted type widens as operations land. That is deliberate — a dispatch
that accepts a command it cannot execute would need a fallthrough case, and a
fallthrough is what lets the sixteenth writer arrive unnoticed. Until an
operation exists in the database, its command is not accepted here, and the
type checker says so at the call site rather than at run time.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any, assert_never

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from .commands import (
    CompleteLockedTask,
    CompleteTaskFused,
    ExpireOwnedClaim,
    ExpirePendingTasks,
    FailLockedTask,
    FailStaleTask,
)
from .outcomes import TerminalizationOutcome, decode_outcome_row

type ExecutableCommand = (
    CompleteLockedTask
    | CompleteTaskFused
    | FailLockedTask
    | FailStaleTask
    | ExpireOwnedClaim
)

# Batch operations return zero or more rows, so they cannot share the
# single-task adapters: exactly-one-row is a contract worth keeping honest
# for the operations it is true of.
type ExecutableBatchCommand = ExpirePendingTasks

_COMPLETE_LOCKED_TASK_SQL = text("""
    SELECT * FROM horsies_complete_locked_task(
        CAST(:task_id AS VARCHAR), :worker_id, :result
    )
""")

_COMPLETE_TASK_FUSED_SQL = text("""
    SELECT * FROM horsies_complete_task_fused(
        CAST(:task_id AS VARCHAR), :worker_id,
        CAST(:claimed_at AS TIMESTAMPTZ), :result,
        :notify_channel, :notify_payload
    )
""")

_FAIL_LOCKED_TASK_SQL = text("""
    SELECT * FROM horsies_fail_locked_task(
        CAST(:task_id AS VARCHAR), :worker_id,
        :result, :error_code, :failed_reason
    )
""")

_FAIL_STALE_TASK_SQL = text("""
    SELECT * FROM horsies_fail_stale_task(
        CAST(:task_id AS VARCHAR),
        CAST(:stale_after_seconds AS INTEGER),
        CAST(:finalizing_stale_after_seconds AS INTEGER),
        :result, :error_code, :failed_reason
    )
""")

_EXPIRE_OWNED_CLAIM_SQL = text("""
    SELECT * FROM horsies_expire_owned_claim(
        CAST(:task_id AS VARCHAR), :worker_id, :result, :error_code
    )
""")

_EXPIRE_PENDING_TASKS_SQL = text("""
    SELECT * FROM horsies_expire_pending_tasks(
        CAST(:batch_size AS INTEGER), :result, :error_code
    )
""")


def call_for(command: ExecutableCommand) -> tuple[Any, dict[str, Any]]:
    """The statement and parameters that execute this command.

    Shared by both drivers so the two cannot diverge in what they send, only
    in how they await it.
    """
    match command:
        case CompleteLockedTask(task_id=task_id, fence=fence, result_json=result):
            return _COMPLETE_LOCKED_TASK_SQL, {
                'task_id': task_id,
                'worker_id': fence.worker_id,
                'result': result,
            }
        case CompleteTaskFused(
            task_id=task_id,
            fence=fence,
            result_json=result,
            notify_channel=channel,
            notify_payload=payload,
        ):
            return _COMPLETE_TASK_FUSED_SQL, {
                'task_id': task_id,
                'worker_id': fence.worker_id,
                'claimed_at': fence.claimed_at,
                'result': result,
                'notify_channel': channel,
                'notify_payload': payload,
            }
        case FailLockedTask(
            task_id=task_id,
            fence=fence,
            result_json=result,
            error_code=error_code,
            failed_reason=failed_reason,
        ):
            return _FAIL_LOCKED_TASK_SQL, {
                'task_id': task_id,
                'worker_id': fence.worker_id,
                'result': result,
                'error_code': error_code,
                'failed_reason': failed_reason,
            }
        case FailStaleTask(
            task_id=task_id,
            stale_after_seconds=stale_after_seconds,
            finalizing_stale_after_seconds=finalizing_stale_after_seconds,
            result_json=result,
            error_code=error_code,
            failed_reason=failed_reason,
        ):
            return _FAIL_STALE_TASK_SQL, {
                'task_id': task_id,
                'stale_after_seconds': stale_after_seconds,
                'finalizing_stale_after_seconds': finalizing_stale_after_seconds,
                'result': result,
                'error_code': error_code,
                'failed_reason': failed_reason,
            }
        case ExpireOwnedClaim(
            task_id=task_id,
            fence=fence,
            result_json=result,
            error_code=error_code,
        ):
            return _EXPIRE_OWNED_CLAIM_SQL, {
                'task_id': task_id,
                'worker_id': fence.worker_id,
                'result': result,
                'error_code': error_code,
            }
        case _ as unreachable:
            assert_never(unreachable)


def batch_call_for(
    command: ExecutableBatchCommand,
) -> tuple[Any, dict[str, Any]]:
    """The statement and parameters that execute this batch command."""
    match command:
        case ExpirePendingTasks(
            batch_size=batch_size,
            result_json=result,
            error_code=error_code,
        ):
            return _EXPIRE_PENDING_TASKS_SQL, {
                'batch_size': batch_size,
                'result': result,
                'error_code': error_code,
            }
        case _ as unreachable:
            assert_never(unreachable)


async def apply_batch_async(
    connection: AsyncConnection,
    command: ExecutableBatchCommand,
) -> list[TerminalizationOutcome]:
    """Execute a batch command and decode every row it reports.

    Zero rows is a valid answer — a discovery batch that found nothing
    eligible has nothing to report, and inventing an outcome for work that
    did not happen is what the row-per-transition contract exists to prevent.
    """
    statement, parameters = batch_call_for(command)
    result = await connection.execute(statement, parameters)
    return [
        decode_outcome_row({str(key): value for key, value in row.items()})
        for row in result.mappings().all()
    ]


async def apply_async(
    connection: AsyncConnection,
    command: ExecutableCommand,
) -> TerminalizationOutcome:
    """Execute a command on the async driver and decode what it reports.

    The transaction stays the caller's: these functions never commit, so a
    coupled write belongs in the same transaction as the transition it proves.
    """
    statement, parameters = call_for(command)
    result = await connection.execute(statement, parameters)
    return decode_outcome_row(_single_row(result.mappings().all(), command))


def apply_sync(
    cursor: Any,
    command: ExecutableCommand,
) -> TerminalizationOutcome:
    """Execute a command on the synchronous child-process driver.

    Takes a psycopg cursor rather than a connection because the child paths
    hold one open across their whole pre-start sequence, and the transaction
    they commit is the one this transition has to land in.
    """
    statement, parameters = call_for(command)
    cursor.execute(_as_psycopg(statement), parameters)
    columns = [description.name for description in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return decode_outcome_row(_single_row(rows, command))


def _single_row(
    rows: Sequence[Any],
    command: ExecutableCommand,
) -> Mapping[str, Any]:
    """A single-task operation reports exactly one row, always.

    Even a task that does not exist produces one — absence is an outcome, not
    an empty result. No rows means the function did not honour its contract,
    which is not something to paper over with a default.
    """
    if len(rows) != 1:
        raise RuntimeError(
            f'{type(command).__name__} returned {len(rows)} rows; every '
            f'single-task operation reports exactly one outcome'
        )
    return rows[0]


# Named-parameter syntax differs between the drivers; the call does not.
# The lookbehind leaves `::` casts alone, and matching every `:name` at once
# avoids the trap a hand-kept name list carries: two parameters where one is
# a prefix of the other would corrupt the longer one if replaced first.
_NAMED_PARAMETER_RE = re.compile(r'(?<!:):(\w+)')


def _as_psycopg(statement: Any) -> str:
    return _NAMED_PARAMETER_RE.sub(r'%(\1)s', str(statement))
