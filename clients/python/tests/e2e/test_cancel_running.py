"""E2E: an operator cancels a RUNNING task (``cancel_task`` with
``include_running=True``).

One worker with an isolated, a shared async and a shared thread child runs a
task of each execution mode and body kind. The cancel statement moves the
task to history as ``CANCELLED`` with one ``CANCELLED`` attempt (recorded by
the cancel), and the worker gives the cancel to the task code: an
``async def`` body gets ``CancelledError`` at its ``await``, a ``def`` body
reads ``TaskRuntime.cancel_state()``. The code sees the cancel within 2 s on
a local database. An isolated task with ``cancel_grace_ms`` that ignores its
cancel is killed after the grace; with no grace, nothing is killed.

The Rust client has the same tests in
``clients/rust/tests/worker/tests/cancel_running.rs``.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from pathlib import Path
from typing import Any, Final

import pytest

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import OutcomeCode
from horsies.core.task_decorator import TaskHandle
from horsies.core.types.result import is_err
from horsies.monitoring import TaskDetail, cancel_task, get_task_detail
from horsies.monitoring.models import AttemptPhasesCapturedInfo, AttemptRecordedByCancelInfo

from tests.e2e.helpers.assertions import assert_failed, read_succeeded, unwrap_send
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import basic as basic_tasks
from tests.e2e.tasks import cancel_tasks
from tests.e2e.tasks.profiles import SET_POOLS

DEFAULT_INSTANCE: Final = 'tests.e2e.tasks.instance:app'
START_TIMEOUT_S: Final = 30.0
"""The longest wait for a body to start on its child."""
SEEN_WITHIN_S: Final = 2.0
"""The bound from the commit of the cancel to the body that sees it, on a
local database."""
RESULT_TIMEOUT_MS: Final = 10_000
KILL_WAIT_S: Final = 15.0
"""The longest wait for the child of the hard stop to be gone: the grace,
then the kill and the wait for the exit."""


def _make_ready_check() -> Any:
    handle: TaskHandle[str] | None = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            handle = unwrap_send(basic_tasks.healthcheck.send())
        return read_succeeded(handle.get(timeout_ms=2000))

    return _check


async def _wait_for_file(path: Path, timeout_s: float) -> str:
    """The text of ``path`` once it exists; fails the test after
    ``timeout_s``."""
    ends_at = time.monotonic() + timeout_s
    while not path.exists():
        if time.monotonic() > ends_at:
            pytest.fail(f'{path.name} did not appear within {timeout_s} s')
        await asyncio.sleep(0.01)
    return path.read_text()


async def _cancel_running(broker: PostgresBroker, task_id: uuid.UUID) -> None:
    cancelled = await cancel_task(broker, task_id, include_running=True)
    if is_err(cancelled):
        pytest.fail(f'the cancel of a RUNNING task was refused: {cancelled.err_value}')


async def _detail(broker: PostgresBroker, task_id: uuid.UUID) -> TaskDetail:
    read = await get_task_detail(broker, task_id)
    if is_err(read):
        pytest.fail(f'the task detail read failed: {read.err_value}')
    detail = read.ok_value
    assert isinstance(detail, TaskDetail), detail
    return detail


def _assert_cancelled_attempt(detail: TaskDetail) -> None:
    """History holds the task as ``CANCELLED`` with one ``CANCELLED`` attempt
    that the cancel statement recorded."""
    assert detail.leaf.status == 'CANCELLED'
    [attempt] = detail.attempts
    assert attempt.attempt == 1
    assert attempt.outcome == 'CANCELLED'
    assert attempt.will_retry is False
    assert attempt.error_code == 'TASK_CANCELLED'
    assert attempt.started_at is not None
    phases = attempt.phases
    assert isinstance(phases, AttemptPhasesCapturedInfo), phases
    assert isinstance(phases.recorded_by, AttemptRecordedByCancelInfo), phases.recorded_by


async def _result_is_cancelled(handle: TaskHandle[Any]) -> None:
    result = await asyncio.to_thread(handle.get, timeout_ms=RESULT_TIMEOUT_MS)
    assert_failed(result, expected_code=OutcomeCode.TASK_CANCELLED)


def _process_is_gone(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
@pytest.mark.parametrize(
    'task_name',
    [
        'cancel_isolated_async',
        'cancel_isolated_def',
        'cancel_shared_async',
        'cancel_shared_thread',
    ],
)
async def test_the_code_of_each_mode_sees_the_cancel_and_history_records_it(
    broker: PostgresBroker,
    tmp_path: Path,
    task_name: str,
) -> None:
    task = getattr(cancel_tasks, task_name)
    with run_worker(
        DEFAULT_INSTANCE,
        profile=SET_POOLS,
        ready_check=_make_ready_check(),
    ):
        handle = unwrap_send(await task.send_async(marker_dir=str(tmp_path)))
        _ = await _wait_for_file(tmp_path / 'started', START_TIMEOUT_S)
        cancelled_at = time.monotonic()
        await _cancel_running(broker, handle.task_id)
        cause = await _wait_for_file(tmp_path / 'cancelled', SEEN_WITHIN_S + 1.0)
        seen_after_s = time.monotonic() - cancelled_at
        await _result_is_cancelled(handle)
        detail = await _detail(broker, handle.task_id)

    assert cause == 'OPERATOR'
    assert seen_after_s <= SEEN_WITHIN_S
    _assert_cancelled_attempt(detail)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_an_isolated_task_with_a_grace_is_killed_after_the_grace(
    broker: PostgresBroker,
    tmp_path: Path,
) -> None:
    with run_worker(
        DEFAULT_INSTANCE,
        profile=SET_POOLS,
        ready_check=_make_ready_check(),
    ):
        handle = unwrap_send(
            await cancel_tasks.cancel_isolated_hard_stop.send_async(marker_dir=str(tmp_path)),
        )
        pid = int(await _wait_for_file(tmp_path / 'started', START_TIMEOUT_S))
        cancelled_at = time.monotonic()
        await _cancel_running(broker, handle.task_id)
        ends_at = cancelled_at + KILL_WAIT_S
        while not _process_is_gone(pid):
            assert time.monotonic() < ends_at, 'the child was not killed after its grace'
            await asyncio.sleep(0.05)
        gone_after_s = time.monotonic() - cancelled_at
        await _result_is_cancelled(handle)
        detail = await _detail(broker, handle.task_id)

    assert gone_after_s >= cancel_tasks.HARD_STOP_GRACE_MS / 1000
    assert not (tmp_path / 'returned').exists()
    _assert_cancelled_attempt(detail)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_an_isolated_task_with_no_grace_is_not_killed(
    broker: PostgresBroker,
    tmp_path: Path,
) -> None:
    """The cancel is cooperative only: the body that ignores it returns, and
    what it returned is dropped."""
    with run_worker(
        DEFAULT_INSTANCE,
        profile=SET_POOLS,
        ready_check=_make_ready_check(),
    ):
        handle = unwrap_send(
            await cancel_tasks.cancel_isolated_no_grace.send_async(marker_dir=str(tmp_path)),
        )
        pid = int(await _wait_for_file(tmp_path / 'started', START_TIMEOUT_S))
        await _cancel_running(broker, handle.task_id)
        _ = await _wait_for_file(
            tmp_path / 'returned',
            cancel_tasks.IGNORED_CANCEL_RUN_S + START_TIMEOUT_S,
        )
        await _result_is_cancelled(handle)
        detail = await _detail(broker, handle.task_id)
        alive_after_return = not _process_is_gone(pid)

    assert alive_after_return
    _assert_cancelled_attempt(detail)
