"""Doubles for the unit tests of the monitoring reads that call one SQL function.

A read checks out a session, takes its autocommit connection and runs one
statement. ``function_broker`` answers that statement with the given rows,
or raises the given error from it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Final
from unittest.mock import AsyncMock, MagicMock

import psycopg.errors
from sqlalchemy.exc import DBAPIError

from tests.attempt_snapshot_support import snapshot_digest
from tests.broker_resources_support import attach_resources, fake_resources

type FunctionRow = Mapping[str, object]

DETAIL_TIME: Final = datetime(2026, 1, 1, tzinfo=UTC)

_EMPTY_ATTEMPTS: Final = b'[]'


def function_broker(answer: Sequence[FunctionRow] | BaseException) -> tuple[MagicMock, MagicMock]:
    """A broker whose one function statement answers ``answer``; and the connection double."""
    result = MagicMock()
    connection = MagicMock()
    match answer:
        case BaseException():
            connection.execute = AsyncMock(side_effect=answer)
        case _:
            result.mappings.return_value.all.return_value = list(answer)
            connection.execute = AsyncMock(return_value=result)
    session = AsyncMock()
    session.connection = AsyncMock(return_value=connection)
    session_context = AsyncMock()
    session_context.__aenter__ = AsyncMock(return_value=session)
    session_context.__aexit__ = AsyncMock(return_value=False)
    broker = MagicMock()
    attach_resources(
        broker, fake_resources(session_factory=MagicMock(return_value=session_context)),
    )
    return broker, connection


def data_corrupted(message: str) -> DBAPIError:
    """The driver error of a function that raised ``data_corrupted`` (XX001)."""
    return DBAPIError('SELECT', {}, psycopg.errors.DataCorrupted(message))


def detail_row(task_id: str, **columns: object) -> dict[str, object]:
    """A LIVE row of the detail function with no attempt; ``columns`` replace columns."""
    row: dict[str, object] = {
        'location': 'LIVE',
        'task_id': task_id,
        'task_name': 'unit.task',
        'queue_name': 'default',
        'priority': 100,
        'status': 'PENDING',
        'is_workflow_task': False,
        'retry_count': 0,
        'max_retries': 0,
        'enqueued_at': DETAIL_TIME,
        'started_at': None,
        'good_until': None,
        'worker_hostname': None,
        'execution': 'ISOLATED',
        'terminal_at': None,
        'error_code': None,
        'final_failed_reason': None,
        'attempt_archive_version': 1,
        'attempt_snapshot': _EMPTY_ATTEMPTS,
        'attempt_snapshot_digest': None,
        'workflow_id': None,
        'workflow_task_index': None,
        'absence_reason': None,
    }
    row.update(columns)
    return row


def absent_row(task_id: str, reason: str) -> dict[str, object]:
    """An ABSENT row of the detail function: every task column NULL."""
    row: dict[str, object] = {name: None for name in detail_row(task_id)}
    row.update(location='ABSENT', task_id=task_id, absence_reason=reason)
    return row


def history_row(task_id: str, **columns: object) -> dict[str, object]:
    """A HISTORY row of the detail function with no attempt; ``columns`` replace columns."""
    row = detail_row(
        task_id,
        location='HISTORY',
        status='COMPLETED',
        terminal_at=DETAIL_TIME,
        execution=None,
        attempt_snapshot_digest=snapshot_digest(_EMPTY_ATTEMPTS),
    )
    row.update(columns)
    return row


def node_row(task_index: int, detail: Mapping[str, object] | None, **columns: object) -> dict[str, object]:
    """A NODE row of the workflow node function; ``detail`` gives its task columns."""
    row: dict[str, object] = {
        'row_kind': 'NODE',
        'node_task_index': task_index,
        'node_id': None,
        'node_task_name': 'unit.task',
        'node_status': 'RUNNING',
        'node_is_subworkflow': False,
        'node_task_id': None,
        'node_result': None,
    }
    match detail:
        case None:
            row.update({name: None for name in detail_row('unused')})
        case _:
            row.update(detail)
            row['node_task_id'] = detail['task_id']
    row.update(columns)
    return row


def list_totals_row(
    kind: str,
    *,
    live_total: int,
    history_total: int | None,
    plan: str | None = None,
) -> dict[str, object]:
    """The TOTALS row (position 0) of the task list function."""
    row: dict[str, object] = {name: None for name in list_task_row('LIVE', 'unused')}
    row.update(
        row_kind='TOTALS',
        row_position=0,
        total_kind=kind,
        live_total=live_total,
        history_total=history_total,
        history_estimate_plan=plan,
    )
    return row


def list_task_row(location: str, task_id: str, **columns: object) -> dict[str, object]:
    """A TASK row of the task list function; ``columns`` replace columns.

    A LIVE row is PENDING with no end time; a HISTORY row is COMPLETED at
    ``DETAIL_TIME``.
    """
    history = location == 'HISTORY'
    row: dict[str, object] = {
        'row_kind': 'TASK',
        'row_position': 1,
        'total_kind': None,
        'live_total': None,
        'history_total': None,
        'history_estimate_plan': None,
        'location': location,
        'task_id': task_id,
        'task_name': 'unit.task',
        'queue_name': 'default',
        'priority': 100,
        'status': 'COMPLETED' if history else 'PENDING',
        'retry_count': 0,
        'max_retries': 0,
        'is_workflow_task': False,
        'error_code': None,
        'worker_id': None,
        'worker_hostname': None,
        'enqueued_at': DETAIL_TIME,
        'started_at': None,
        'terminal_at': DETAIL_TIME if history else None,
    }
    row.update(columns)
    return row
