"""E2E: ``SHARED_ASYNC`` tasks on the shared async children of a real worker.

The worker runs the ``split-isolated-1-shared-2x8`` profile: one isolated
child for the ``def`` ready check, and two shared async children with 8 seats
each. Many shared tasks run at once in those two children; a task that
outruns its timeout is cancelled in its child and fails with
``TASK_TIMEOUT``, and its neighbours finish.

A shared row whose stored timeout or cancel grace the worker cannot use fails
once with ``WORKER_SERIALIZATION_ERROR``, on that profile and on
``shared-only-1x4``, which has no isolated child.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import OperationalErrorCode, OutcomeCode
from horsies.core.task_decorator import TaskHandle

from tests.broker_resources_support import current_resources
from tests.e2e.helpers.assertions import assert_ok, unwrap_get_result, unwrap_send
from tests.e2e.helpers.db import wait_for_status
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import basic as basic_tasks
from tests.e2e.tasks import shared_async_tasks
from tests.integration.history_seeding import AttemptRecord, read_attempts
from tests.e2e.tasks.profiles import (
    SHARED_ONLY_1X4,
    SPLIT_SHARED_1X4,
    SPLIT_SHARED_2X8,
    SPLIT_SHARED_SEATS,
)

DEFAULT_INSTANCE = 'tests.e2e.tasks.instance:app'
RESULT_TIMEOUT_MS = 60_000
SLEEP_S = 1.5
SLEEPERS = 12
NEIGHBOUR_S = 5.0
"""Long enough that the stuck task is reported stuck while it runs."""


def _make_ready_check() -> Callable[[], bool]:
    handle: TaskHandle[str] | None = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            handle = unwrap_send(basic_tasks.healthcheck.send())
        return handle.get(timeout_ms=2000).is_ok()

    return _check


def _make_shared_ready_check() -> Callable[[], bool]:
    """A ready check that a worker with no isolated child can answer."""
    handle: TaskHandle[str] | None = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            handle = unwrap_send(shared_async_tasks.shared_sleep.send(seconds=0))
        return handle.get(timeout_ms=2000).is_ok()

    return _check


READY_CHECKS: dict[str, Callable[[], Callable[[], bool]]] = {
    SPLIT_SHARED_2X8: _make_ready_check,
    SHARED_ONLY_1X4: _make_shared_ready_check,
}


async def _value(handle: TaskHandle[str]) -> str:
    result = await asyncio.to_thread(handle.get, timeout_ms=RESULT_TIMEOUT_MS)
    assert_ok(result)
    return result.unwrap()


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_shared_tasks_run_at_once_and_a_timeout_leaves_neighbours_finished(
    broker: PostgresBroker,
) -> None:
    assert SLEEPERS <= 2 * SPLIT_SHARED_SEATS
    with run_worker(
        DEFAULT_INSTANCE,
        profile=SPLIT_SHARED_2X8,
        ready_check=_make_ready_check(),
    ):
        isolated = unwrap_send(await basic_tasks.pid_task.send_async())
        isolated_result = await asyncio.to_thread(
            isolated.get,
            timeout_ms=RESULT_TIMEOUT_MS,
        )
        assert_ok(isolated_result)
        isolated_pid = str(isolated_result.unwrap())
        hang = unwrap_send(await shared_async_tasks.shared_hang.send_async())
        sleepers = [
            unwrap_send(
                await shared_async_tasks.shared_sleep.send_async(seconds=SLEEP_S)
            )
            for _ in range(SLEEPERS)
        ]
        stamps = [(await _value(handle)).split(':') for handle in sleepers]
        await wait_for_status(
            current_resources(broker).session_factory,
            hang.task_id,
            'FAILED',
            timeout_s=30.0,
        )

        async with current_resources(broker).session_factory() as session:
            hang_row = (
                await session.execute(
                    text('SELECT error_code FROM itest_task_rows WHERE id = :id'),
                    {'id': hang.task_id},
                )
            ).fetchone()
            statuses = (
                (
                    await session.execute(
                        text(
                            'SELECT status FROM itest_task_rows WHERE id = ANY(:ids)',
                        ),
                        {'ids': [handle.task_id for handle in sleepers]},
                    )
                )
                .scalars()
                .all()
            )

    pids = {pid for pid, _started, _ended in stamps}
    starts = [float(started) for _pid, started, _ended in stamps]
    ends = [float(ended) for _pid, _started, ended in stamps]
    assert hang_row is not None
    assert hang_row.error_code == OutcomeCode.TASK_TIMEOUT.value
    assert sorted(statuses) == ['COMPLETED'] * SLEEPERS
    assert 1 <= len(pids) <= 2
    assert isolated_pid not in pids
    # Run one after another, 12 sleeps take 18 s; at once, about one sleep.
    assert max(ends) - min(starts) < SLEEPERS * SLEEP_S / 3
    assert max(starts) < min(ends)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_the_start_hook_runs_on_the_loop_of_the_tasks(
    broker: PostgresBroker,
) -> None:
    _ = broker
    with run_worker(
        DEFAULT_INSTANCE,
        profile=SPLIT_SHARED_2X8,
        ready_check=_make_ready_check(),
    ):
        reports = [
            await _value(
                unwrap_send(await shared_async_tasks.shared_hook_loop.send_async()),
            )
            for _ in range(4)
        ]

    assert all(report.endswith(':True') for report in reports)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_a_shared_task_sends_with_send_async(
    broker: PostgresBroker,
    app: Horsies,
) -> None:
    _ = broker
    with run_worker(
        DEFAULT_INSTANCE,
        profile=SPLIT_SHARED_2X8,
        ready_check=_make_ready_check(),
    ):
        for x in (5, 6):
            sent_id = await _value(
                unwrap_send(await shared_async_tasks.shared_sends.send_async(x=x)),
            )
            result = unwrap_get_result(
                await asyncio.to_thread(
                    app.get_result,
                    sent_id,
                    timeout_ms=RESULT_TIMEOUT_MS,
                ),
            )
            assert_ok(result)
            assert result.unwrap() == x * 2


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
@pytest.mark.parametrize(
    'profile',
    [SPLIT_SHARED_2X8, SHARED_ONLY_1X4],
    ids=['isolated-and-shared-children', 'shared-children-only'],
)
@pytest.mark.parametrize(
    ('dropped_key', 'field'),
    [('cancel_grace_ms', 'cancel_grace_ms'), ('timeout_ms', 'timeout_ms')],
    ids=['no-grace', 'no-timeout'],
)
async def test_a_shared_row_with_unusable_options_fails_once(
    broker: PostgresBroker,
    profile: str,
    dropped_key: str,
    field: str,
) -> None:
    """The row is written while no worker runs, then its stored options lose
    a key that a ``SHARED_ASYNC`` row needs. The worker fails it before the
    task resolves; it never runs it, and never returns it to the queue."""
    session_factory = current_resources(broker).session_factory
    handle = unwrap_send(
        await shared_async_tasks.shared_sleep.send_async(seconds=0),
    )
    async with session_factory() as session:
        stored = (
            await session.execute(
                text('SELECT task_options FROM horsies_tasks WHERE id = :id'),
                {'id': handle.task_id},
            )
        ).scalar_one()
        options = json.loads(stored)
        del options[dropped_key]
        await session.execute(
            text('UPDATE horsies_tasks SET task_options = :opts WHERE id = :id'),
            {'opts': json.dumps(options), 'id': handle.task_id},
        )
        await session.commit()

    with run_worker(
        DEFAULT_INSTANCE,
        profile=profile,
        ready_check=READY_CHECKS[profile](),
    ):
        await wait_for_status(session_factory, handle.task_id, 'FAILED', timeout_s=30.0)

    async with session_factory() as session:
        row = (
            await session.execute(
                text(
                    'SELECT error_code, result, retry_count FROM itest_task_rows '
                    'WHERE id = :id',
                ),
                {'id': handle.task_id},
            )
        ).one()
    match row.result:
        case str() as text_value:
            result = json.loads(text_value)
        case decoded:
            result = decoded
    assert row.error_code == OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value
    assert row.retry_count == 0
    assert f'"field": "{field}"' in json.dumps(result)


async def _isolated_pid() -> str:
    handle = unwrap_send(await basic_tasks.pid_task.send_async())
    result = await asyncio.to_thread(handle.get, timeout_ms=RESULT_TIMEOUT_MS)
    assert_ok(result)
    return str(result.unwrap())


async def _row_and_attempts(
    broker: PostgresBroker,
    task_id: str,
) -> tuple[Any, tuple[AttemptRecord, ...]]:
    """The row of a task and its attempts, live or moved to history."""
    async with current_resources(broker).session_factory() as session:
        row = (
            await session.execute(
                text(
                    'SELECT status, error_code, retry_count FROM itest_task_rows '
                    'WHERE id = :id',
                ),
                {'id': task_id},
            )
        ).one()
        attempts = await read_attempts(session, task_id)
    return row, attempts


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_a_stuck_task_waits_for_its_neighbour_then_retries_isolated(
    broker: PostgresBroker,
    tmp_path: Path,
) -> None:
    """One shared child holds both tasks. The stuck task ignores every
    cancel; its neighbour has more time and finishes. Only then is the child
    killed: the stuck attempt fails with TASK_TIMEOUT, and its retry runs in
    the isolated child."""
    with run_worker(
        DEFAULT_INSTANCE,
        profile=SPLIT_SHARED_1X4,
        ready_check=_make_ready_check(),
    ):
        isolated_pid = await _isolated_pid()
        neighbour = unwrap_send(
            await shared_async_tasks.shared_sleep.send_async(seconds=NEIGHBOUR_S),
        )
        stuck = unwrap_send(
            await shared_async_tasks.shared_stuck_once.send_async(
                marker=str(tmp_path / 'stuck'),
            ),
        )
        neighbour_pid, _started, _ended = (await _value(neighbour)).split(':')
        retry_pid = await _value(stuck)
        row, attempts = await _row_and_attempts(broker, stuck.task_id)

    first_attempt_pid = (tmp_path / 'stuck').read_text()
    assert first_attempt_pid == neighbour_pid
    assert retry_pid == isolated_pid
    assert (row.status, row.retry_count) == ('COMPLETED', 1)
    assert [(a.attempt, a.error_code, a.will_retry) for a in attempts[:1]] == [
        (1, OutcomeCode.TASK_TIMEOUT.value, True),
    ]
    # Alone, the stuck task would be killed 2.4 s after its start. It was
    # killed only after its neighbour, which ran NEIGHBOUR_S, had ended.
    first = attempts[0]
    assert first.finished_at is not None and first.started_at is not None
    assert (first.finished_at - first.started_at).total_seconds() >= NEIGHBOUR_S - 1.0


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_a_blocked_loop_kills_the_child_and_names_the_culprit(
    broker: PostgresBroker,
    tmp_path: Path,
) -> None:
    """The culprit blocks the loop of the shared child for 30 s; the watchdog
    reports it after the 5 s block limit. The culprit fails with
    TASK_BLOCKED_LOOP and its retry runs in the isolated child. The neighbour
    fails with NEIGHBOUR_BLOCKED_LOOP: its policy does not name that code."""
    session_factory = current_resources(broker).session_factory
    with run_worker(
        DEFAULT_INSTANCE,
        profile=SPLIT_SHARED_1X4,
        ready_check=_make_ready_check(),
    ):
        isolated_pid = await _isolated_pid()
        neighbour = unwrap_send(
            await shared_async_tasks.shared_sleep.send_async(seconds=20.0),
        )
        await wait_for_status(session_factory, neighbour.task_id, 'RUNNING')
        culprit = unwrap_send(
            await shared_async_tasks.shared_blocks_once.send_async(
                marker=str(tmp_path / 'blocks'),
            ),
        )
        retry_pid = await _value(culprit)
        await wait_for_status(session_factory, neighbour.task_id, 'FAILED')
        culprit_row, culprit_attempts = await _row_and_attempts(broker, culprit.task_id)
        neighbour_row, _attempts = await _row_and_attempts(broker, neighbour.task_id)

    assert retry_pid == isolated_pid
    assert (culprit_row.status, culprit_row.retry_count) == ('COMPLETED', 1)
    first_attempt = culprit_attempts[0]
    assert first_attempt.error_code == OperationalErrorCode.TASK_BLOCKED_LOOP.value
    assert neighbour_row.error_code == OperationalErrorCode.NEIGHBOUR_BLOCKED_LOOP.value
    assert neighbour_row.retry_count == 0
