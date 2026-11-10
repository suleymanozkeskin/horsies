"""One task run on a seat of a shared async child (``run_seat_task``).

Real registered tasks run here. The database steps around the body, the
RUNNING transition and the finalizing marker, are replaced; their own tests
are in ``test_shared_lifecycle``.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.codec.json_io import loads_json
from horsies.core.codec.typed import decode_task_result
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import (
    Execution,
    OperationalErrorCode,
    TaskError,
    TaskResult,
)
from horsies.core.worker import child_runner, shared_runner
from horsies.core.worker.child_protocol import SharedRunFrame, WireResult

pytestmark = [pytest.mark.unit]

CLAIMED_AT = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)


APP = Horsies(
    AppConfig(
        broker=PostgresConfig(
            database_url=SecretStr('postgresql+psycopg://u:p@localhost/unused'),
        ),
    ),
)
RAN: list[str] = []
STARTED = asyncio.Event()


@APP.task(task_name='seat_double', execution=Execution.SHARED_ASYNC, timeout_ms=10_000)
async def seat_double(*, x: int) -> TaskResult[int, TaskError]:
    await asyncio.sleep(0)
    return TaskResult(ok=x * 2)


@APP.task(task_name='seat_sync')
def seat_sync() -> TaskResult[int, TaskError]:
    RAN.append('seat_sync')
    return TaskResult(ok=1)


@APP.task(task_name='seat_never', execution=Execution.SHARED_ASYNC, timeout_ms=1000)
async def seat_never() -> TaskResult[int, TaskError]:
    RAN.append('seat_never')
    return TaskResult(ok=1)


@APP.task(task_name='seat_raises', execution=Execution.SHARED_ASYNC, timeout_ms=1000)
async def seat_raises() -> TaskResult[int, TaskError]:
    raise ValueError('bad input')


@APP.task(task_name='seat_waits', execution=Execution.SHARED_ASYNC, timeout_ms=1000)
async def seat_waits() -> TaskResult[int, TaskError]:
    STARTED.set()
    await asyncio.Event().wait()
    return TaskResult(ok=1)


@APP.task(task_name='seat_quick', execution=Execution.SHARED_ASYNC, timeout_ms=1000)
async def seat_quick() -> TaskResult[int, TaskError]:
    return TaskResult(ok=1)


def _frame(task_name: str, kwargs: dict[str, Any] | None = None) -> SharedRunFrame:
    return SharedRunFrame(
        dispatch=1,
        task_id='task-1',
        task_name=task_name,
        args_json='[]',
        kwargs_json=json.dumps(kwargs or {}),
        worker_id='worker-1',
        is_workflow_task=False,
        claimed_at=CLAIMED_AT,
        timeout_ms=60_000,
        cancel_grace_ms=1_000,
    )


class _Steps:
    """The replaced database steps, as the seat sees them."""

    def __init__(self) -> None:
        self.confirm = AsyncMock(return_value=None)
        self.mark = AsyncMock(return_value=None)


@pytest.fixture
def steps() -> Iterator[_Steps]:
    replaced = _Steps()
    with (
        patch.object(shared_runner, 'confirm_until_not_resumed', replaced.confirm),
        patch.object(shared_runner, 'mark_task_finalizing', replaced.mark),
    ):
        yield replaced


def _run(frame: SharedRunFrame) -> WireResult:
    with patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)):
        return asyncio.run(shared_runner.run_seat_task(MagicMock(), frame))


def _result(wire: WireResult, ok_type: Any) -> TaskResult[Any, TaskError]:
    ok, payload, _reason = wire
    assert ok is True
    return decode_task_result(loads_json(payload).unwrap(), ok_type)


@pytest.fixture(autouse=True)
def fresh_marks() -> Iterator[None]:
    RAN.clear()
    STARTED.clear()
    yield


def test_an_async_task_runs_on_the_running_loop(steps: _Steps) -> None:
    wire = _run(_frame('seat_double', {'x': 21}))

    assert _result(wire, int).unwrap() == 42
    assert wire[2] is None
    steps.confirm.assert_awaited_once()
    steps.mark.assert_awaited_once()


def test_a_def_task_is_refused_before_it_runs(steps: _Steps) -> None:
    wire = _run(_frame('seat_sync'))

    error = _result(wire, int).unwrap_err()
    assert error.error_code == OperationalErrorCode.WORKER_RESOLUTION_ERROR
    assert wire[2] == (
        'the task is def, and a shared async child runs only async def tasks'
    )
    assert RAN == []


def test_a_stop_before_start_runs_no_body_and_writes_no_marker(steps: _Steps) -> None:
    steps.confirm.return_value = (False, '', 'CLAIM_LOST')

    assert _run(_frame('seat_never')) == (False, '', 'CLAIM_LOST')
    assert RAN == []
    steps.mark.assert_not_awaited()


def test_a_task_that_raises_is_mapped_and_writes_the_marker(steps: _Steps) -> None:
    wire = _run(_frame('seat_raises'))

    error = _result(wire, int).unwrap_err()
    assert error.error_code == OperationalErrorCode.UNHANDLED_EXCEPTION
    steps.mark.assert_awaited_once()


def test_a_cancel_of_the_body_ends_the_run_and_writes_the_marker(
    steps: _Steps,
) -> None:
    """The seat loop turns a cancel of its deadline into the timeout; the run
    itself only has to end, with the marker written."""

    async def cancel_after_start() -> WireResult:
        run = asyncio.ensure_future(
            shared_runner.run_seat_task(MagicMock(), _frame('seat_waits')),
        )
        await STARTED.wait()
        run.cancel()
        return await run

    with patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)):
        wire = asyncio.run(cancel_after_start())

    error = _result(wire, int).unwrap_err()
    assert error.error_code == OperationalErrorCode.TASK_EXCEPTION
    assert 'CancelledError' in (error.message or '')
    steps.mark.assert_awaited_once()


def test_a_cancel_during_the_marker_propagates(steps: _Steps) -> None:
    marking = asyncio.Event()

    async def slow_marker(*_args: object) -> None:
        marking.set()
        await asyncio.Event().wait()

    steps.mark.side_effect = slow_marker

    async def cancel_in_marker() -> None:
        run = asyncio.ensure_future(
            shared_runner.run_seat_task(MagicMock(), _frame('seat_quick')),
        )
        await marking.wait()
        run.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run

    with patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)):
        asyncio.run(cancel_in_marker())
