"""Cancel from monitoring, before ``horsies_cancel_task``.

``cancel_task`` (``python/horsies/monitoring/task_actions.py``) sent, in one session
transaction: the task advisory lock (``LOCK_TASK_SQL``), the row lock read
of ``is_workflow_task``, then ``horsies_cancel_locked_task`` with the
permitted source statuses, and committed only an applied cancel. A row
absent from the live table ended the transaction with read-only history
reads, which write nothing and are not repeated here. The baseline no longer
defines ``horsies_cancel_locked_task``: ``interim_terminal_writes.py``
installs its record on the reference database.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, LiteralString

import psycopg

from tests.dualrun.capture import Connection

LOCK_TASK_SQL: Final[LiteralString] = """
    SELECT pg_advisory_xact_lock(
        hashtextextended(CAST(CAST(%(task_id)s AS uuid) AS text), 731)
    )
"""

LOCK_ROW_SQL: Final[LiteralString] = """
    SELECT is_workflow_task
    FROM horsies_tasks
    WHERE id = CAST(%(id)s AS uuid)
    FOR UPDATE
"""

CANCEL_LOCKED_TASK_SQL: Final[LiteralString] = """
    SELECT outcome, observed_status FROM horsies_cancel_locked_task(
        CAST(%(task_id)s AS uuid),
        CAST(%(permitted_source_statuses)s AS TEXT[])
    )
"""


@dataclass(frozen=True, slots=True)
class LiveMiss:
    """The row lock read found no live row; the transaction wrote nothing."""


@dataclass(frozen=True, slots=True)
class CancelCalled:
    """What ``horsies_cancel_locked_task`` answered for the locked row."""

    is_workflow_task: bool
    outcome: str
    observed_status: str | None


def cancel(
    connection: Connection, task_id: str, include_running: bool
) -> LiveMiss | CancelCalled:
    """One cancel in one transaction, committed only when it applied."""
    permitted = ['PENDING', 'CLAIMED', *(['RUNNING'] if include_running else [])]
    answer: LiveMiss | CancelCalled = LiveMiss()
    with connection.transaction():
        _ = connection.execute(LOCK_TASK_SQL, {'task_id': task_id})
        locked = connection.execute(LOCK_ROW_SQL, {'id': task_id}).fetchone()
        match locked:
            case None:
                raise psycopg.Rollback()
            case (bool() as is_workflow_task,):
                pass
            case _:
                raise AssertionError(f'the row lock read returned {locked!r}')
        record = connection.execute(
            CANCEL_LOCKED_TASK_SQL,
            {'task_id': task_id, 'permitted_source_statuses': permitted},
        ).fetchone()
        assert record is not None, 'horsies_cancel_locked_task returns one row'
        outcome, observed_status = record
        answer = CancelCalled(
            is_workflow_task=is_workflow_task,
            outcome=str(outcome),
            observed_status=None if observed_status is None else str(observed_status),
        )
        if outcome != 'APPLIED':
            raise psycopg.Rollback()
    return answer
