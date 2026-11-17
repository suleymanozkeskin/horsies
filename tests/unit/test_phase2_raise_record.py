# pyright: reportPrivateUsage=false
"""The record of a persistent phase-2 raise: one ``horsies_workflow_phase2_record_raise`` call.

``_record_raised_row`` reads the workflow and node of the record, then sends
the function with the limit error on an autocommit connection. Each
outcome of the function maps to one result.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.exc import OperationalError

from horsies.core.workflows.functions import PHASE2_RECORD_RAISE_FUNCTION_SQL
from horsies.core.workflows.phase2_recovery import (
    RAISED_ROW_CONTEXT_SQL,
    _RaisedRowNotRecorded,
    _RaisedRowRecorded,
    _record_raised_row,
)

pytestmark = [pytest.mark.unit]

TASK_ID = '018f0000-0000-7000-8000-000000000001'
WORKFLOW_ID = '018f0000-0000-7000-8000-0000000000aa'
BOUND = 3


def _sessions(
    *,
    context: object,
    raise_row: object,
    error: Exception | None = None,
) -> tuple[MagicMock, AsyncMock]:
    """A session factory whose autocommit connection answers the context
    read with ``context`` and the function with ``raise_row``."""

    def execute(statement: Any, _params: Any) -> MagicMock:
        result = MagicMock()
        if statement is RAISED_ROW_CONTEXT_SQL:
            result.first.return_value = context
            return result
        assert statement is PHASE2_RECORD_RAISE_FUNCTION_SQL
        if error is not None:
            raise error
        result.fetchall.return_value = [raise_row]
        return result

    connection = AsyncMock()
    connection.execute = AsyncMock(side_effect=execute)
    session = AsyncMock()
    session.connection = AsyncMock(return_value=connection)
    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=session)
    cm.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=cm), connection


def _context() -> SimpleNamespace:
    return SimpleNamespace(workflow_id=WORKFLOW_ID, task_index=0)


def _row(
    outcome: str,
    attempt_count: int | None = None,
    limit_outcome: str | None = None,
    quarantine_verdict: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        outcome=outcome,
        attempt_count=attempt_count,
        limit_outcome=limit_outcome,
        quarantine_verdict=quarantine_verdict,
    )


async def _record(factory: MagicMock) -> object:
    return await _record_raised_row(
        factory,
        task_id=TASK_ID,
        exc=RuntimeError('boom'),
        quarantine_after_attempts=BOUND,
    )


@pytest.mark.asyncio
async def test_a_counted_raise_is_recorded_and_sends_the_limit_error() -> None:
    factory, connection = _sessions(context=_context(), raise_row=_row('COUNTED', 1))

    recorded = await _record(factory)

    assert recorded == _RaisedRowRecorded(quarantined=False, quarantine_refusal=None)
    statement, params = connection.execute.await_args_list[1].args
    assert statement is PHASE2_RECORD_RAISE_FUNCTION_SQL
    assert params['task_id'] == TASK_ID
    assert params['attempt_bound'] == BOUND
    error = json.loads(params['limit_error'])
    assert error['error_code'] == {'__builtin_task_code__': 'WORKFLOW_RECOVERY_LIMIT_REACHED'}
    assert error['data']['failed_passes'] == BOUND
    assert error['data']['workflow_id'] == WORKFLOW_ID
    assert json.loads(params['limit_node_result'])['err'] == error


@pytest.mark.asyncio
async def test_a_gone_row_is_not_recorded_and_sends_no_function() -> None:
    factory, connection = _sessions(context=None, raise_row=_row('COUNTED', 1))

    assert await _record(factory) == _RaisedRowNotRecorded()
    assert connection.execute.await_count == 1


@pytest.mark.asyncio
async def test_pending_absent_is_not_recorded() -> None:
    factory, _ = _sessions(context=_context(), raise_row=_row('PENDING_ABSENT'))

    assert await _record(factory) == _RaisedRowNotRecorded()


@pytest.mark.asyncio
async def test_a_refused_limit_is_not_recorded() -> None:
    factory, _ = _sessions(context=_context(), raise_row=_row('NOT_RECORDED', 2))

    assert await _record(factory) == _RaisedRowNotRecorded()


@pytest.mark.asyncio
async def test_the_limit_with_a_repointed_record_is_quarantined() -> None:
    factory, _ = _sessions(
        context=_context(),
        raise_row=_row('LIMIT_REACHED', BOUND, 'LIMIT_REACHED_FAILED', 'REPOINTED'),
    )

    assert await _record(factory) == _RaisedRowRecorded(
        quarantined=True, quarantine_refusal=None,
    )


@pytest.mark.asyncio
async def test_the_limit_with_a_refused_quarantine_reports_the_verdict() -> None:
    factory, _ = _sessions(
        context=_context(),
        raise_row=_row(
            'LIMIT_REACHED', BOUND, 'LIMIT_REACHED_LIVE_NODES', 'SOURCE_ABSENT: no history row',
        ),
    )

    assert await _record(factory) == _RaisedRowRecorded(
        quarantined=False,
        quarantine_refusal=f'{TASK_ID}: quarantine refused with SOURCE_ABSENT: no history row',
    )


@pytest.mark.asyncio
async def test_a_statement_error_is_not_recorded() -> None:
    factory, _ = _sessions(
        context=_context(),
        raise_row=_row('COUNTED', 1),
        error=OperationalError('SELECT 1', {}, Exception('limit write failed')),
    )

    assert await _record(factory) == _RaisedRowNotRecorded()
