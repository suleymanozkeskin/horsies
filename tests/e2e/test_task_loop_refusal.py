"""E2E: a worker child that cannot create the event loop of an async task.

The instance's child start hook makes loop creation raise OSError(EMFILE)
while a budget file allows it. The task returns to the queue unrun. A child
that ran a task before is replaced and the worker goes on. A new child that
ran no task stops the worker with a non-zero exit.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.types.result import is_err
from horsies.core.types.status import TaskStatus
from tests.broker_resources_support import current_resources
from tests.e2e.helpers.assertions import assert_ok, unwrap_send
from tests.e2e.helpers.db import read_task
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import instance_task_loop_refusal as refusal

INSTANCE = 'tests.e2e.tasks.instance_task_loop_refusal:app'
RESULT_TIMEOUT_MS = 30_000
WORKER_STOP_TIMEOUT_S = 60.0


def _ready_check() -> Callable[[], bool]:
    """Ready once a ``def`` task ran in the child."""
    handle = None

    def check() -> bool:
        nonlocal handle
        if handle is None:
            sent = refusal.healthcheck.send()
            if is_err(sent):
                return False
            handle = sent.ok_value
        return handle.get(timeout_ms=2000).is_ok()

    return check


@pytest.fixture
def refusals(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """The refusal budget of the worker's children, 0 at the start."""
    budget = tmp_path / 'loop_refusals'
    budget.write_text('0', encoding='ascii')
    monkeypatch.setenv(refusal.REFUSALS_ENV, str(budget))
    return budget


async def _attempt_count(broker: PostgresBroker, task_id: str) -> int:
    async with current_resources(broker).session_factory() as session:
        row = (
            await session.execute(
                text(
                    'SELECT count(*) AS n FROM horsies_task_attempts '
                    'WHERE task_id = CAST(:id AS uuid)'
                ),
                {'id': task_id},
            )
        ).one()
        return int(row.n)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_a_child_that_ran_a_task_is_replaced_and_the_task_runs_later(
    broker: PostgresBroker,
    refusals: Path,
) -> None:
    with run_worker(INSTANCE, ready_check=_ready_check()) as worker:
        sync_result = await asyncio.to_thread(
            unwrap_send(refusal.sync_pid.send()).get,
            timeout_ms=RESULT_TIMEOUT_MS,
        )
        assert_ok(sync_result)
        refusals.write_text('1', encoding='ascii')

        handle = unwrap_send(refusal.async_pid.send())
        result = await asyncio.to_thread(handle.get, timeout_ms=RESULT_TIMEOUT_MS)

        assert_ok(result)
        assert result.unwrap() != sync_result.unwrap()
        assert refusals.read_text(encoding='ascii') == '0'
        assert worker.poll() is None
        async with current_resources(broker).session_factory() as session:
            task = await read_task(session, handle.task_id)
        assert task is not None
        assert task.status == TaskStatus.COMPLETED
        assert task.retry_count == 0


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_a_workflow_node_returns_to_the_queue_and_the_workflow_completes(
    refusals: Path,
) -> None:
    with run_worker(INSTANCE, ready_check=_ready_check()) as worker:
        refusals.write_text('1', encoding='ascii')

        started = await refusal.spec_async_node.start_async()
        assert not is_err(started), started
        result = await started.ok_value.get_async(timeout_ms=RESULT_TIMEOUT_MS)

        assert_ok(result)
        assert refusals.read_text(encoding='ascii') == '0'
        assert worker.poll() is None


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_a_new_child_without_a_task_loop_stops_the_worker(
    broker: PostgresBroker,
    refusals: Path,
) -> None:
    """A def task runs with the refusals armed and leaves them unused. The
    first refusal comes from the child that ran it, so that child is
    replaced. Its replacement ran no task, so its refusal stops the
    worker, after the task returned to the queue."""
    with run_worker(INSTANCE, ready_check=_ready_check()) as worker:
        refusals.write_text('2', encoding='ascii')
        sync_result = await asyncio.to_thread(
            unwrap_send(refusal.sync_pid.send()).get,
            timeout_ms=RESULT_TIMEOUT_MS,
        )
        assert_ok(sync_result)
        assert refusals.read_text(encoding='ascii') == '2'

        handle = unwrap_send(refusal.async_pid.send())
        returncode = await asyncio.to_thread(worker.wait, WORKER_STOP_TIMEOUT_S)

        assert returncode != 0
        assert refusals.read_text(encoding='ascii') == '0'
        async with current_resources(broker).session_factory() as session:
            task = await read_task(session, handle.task_id)
        assert task is not None
        assert task.status == TaskStatus.PENDING
        assert task.claimed_by_worker_id is None
        assert task.retry_count == 0
        assert await _attempt_count(broker, handle.task_id) == 0
