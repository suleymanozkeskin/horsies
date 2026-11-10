"""E2E: a worker child creates the task loop at boot.

The child start hook of the instances makes event loop creation raise
OSError(EMFILE) while a budget file allows it. A child whose app has an
``async def`` task creates its loop at boot, so a failure there fails the
boot: after ``CHILD_BOOT_ATTEMPTS`` failed attempts the worker exits non-zero
before it claims any task. A child whose app has no async task creates no
loop.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.task_decorator import TaskFunction, TaskHandle
from horsies.core.types.result import is_err
from horsies.core.types.status import TaskStatus
from horsies.core.worker.slots import CHILD_BOOT_ATTEMPTS
from tests.broker_resources_support import current_resources
from tests.e2e.helpers.assertions import assert_ok, unwrap_send
from tests.e2e.helpers.db import read_task
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import instance_task_loop_boot as boot
from tests.e2e.tasks import instance_task_loop_boot_sync_only as sync_only
from tests.e2e.tasks.loop_creation_budget import LOOP_FAILURES_ENV

INSTANCE = 'tests.e2e.tasks.instance_task_loop_boot:app'
SYNC_ONLY_INSTANCE = 'tests.e2e.tasks.instance_task_loop_boot_sync_only:app'
RESULT_TIMEOUT_MS = 30_000
WORKER_EXIT_TIMEOUT_S = 60.0
LOOP_CAUSE = (
    'the child could not create the event loop for async tasks: '
    'OSError: [Errno 24] Too many open files'
)


def _ready_check(healthcheck: TaskFunction[[], str]) -> Callable[[], bool]:
    """Ready once a ``def`` healthcheck task ran in a child."""
    handle: TaskHandle[str] | None = None

    def check() -> bool:
        nonlocal handle
        if handle is None:
            sent = healthcheck.send()
            if is_err(sent):
                return False
            handle = sent.ok_value
        return handle.get(timeout_ms=2000).is_ok()

    return check


@pytest.fixture
def loop_failures(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """The loop creation failure budget of the worker's children, 0 at first."""
    budget = tmp_path / 'loop_failures'
    budget.write_text('0', encoding='ascii')
    monkeypatch.setenv(LOOP_FAILURES_ENV, str(budget))
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
async def test_a_child_that_cannot_create_its_task_loop_stops_the_worker_before_any_claim(
    broker: PostgresBroker,
    loop_failures: Path,
) -> None:
    """Each boot attempt fails once, so the budget of exactly
    ``CHILD_BOOT_ATTEMPTS`` is used up and the worker stops. The task sent
    before the worker started was never claimed."""
    handle = unwrap_send(await boot.async_pid.send_async())
    loop_failures.write_text(str(CHILD_BOOT_ATTEMPTS), encoding='ascii')

    with pytest.raises(RuntimeError) as stopped:
        with run_worker(
            INSTANCE,
            profile='isolated-1',
            timeout=WORKER_EXIT_TIMEOUT_S,
            ready_check=lambda: False,
        ):
            pass

    message = str(stopped.value)
    assert 'exited before becoming ready' in message
    exit_code = re.search(r'\(code=(-?\d+)\)', message)
    assert exit_code is not None and int(exit_code.group(1)) != 0
    assert (
        f'Child boot attempt {CHILD_BOOT_ATTEMPTS}/{CHILD_BOOT_ATTEMPTS} failed '
        f'on slot 0: {LOOP_CAUSE}'
    ) in message
    assert loop_failures.read_text(encoding='ascii') == '0'
    async with current_resources(broker).session_factory() as session:
        task = await read_task(session, handle.task_id)
    assert task is not None
    assert task.status == TaskStatus.PENDING
    assert task.claimed_by_worker_id is None
    assert task.retry_count == 0
    assert await _attempt_count(broker, handle.task_id) == 0


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_a_task_loop_failure_at_boot_costs_one_attempt(
    broker: PostgresBroker,
    loop_failures: Path,
) -> None:
    """The first boot fails, the second creates the loop, and the async task
    runs in that child with no retry."""
    loop_failures.write_text('1', encoding='ascii')

    with run_worker(
        INSTANCE,
        profile='isolated-1',
        timeout=WORKER_EXIT_TIMEOUT_S,
        ready_check=_ready_check(boot.healthcheck),
    ) as worker:
        handle = unwrap_send(await boot.async_pid.send_async())
        result = await handle.get_async(timeout_ms=RESULT_TIMEOUT_MS)

        assert_ok(result)
        assert loop_failures.read_text(encoding='ascii') == '0'
        assert worker.poll() is None
        async with current_resources(broker).session_factory() as session:
            task = await read_task(session, handle.task_id)
        assert task is not None
        assert task.status == TaskStatus.COMPLETED
        assert task.retry_count == 0


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_an_app_with_no_async_task_creates_no_task_loop(
    loop_failures: Path,
) -> None:
    """Every loop creation would fail, and the children still boot and run
    ``def`` tasks: no child created a loop."""
    loop_failures.write_text('100', encoding='ascii')

    with run_worker(
        SYNC_ONLY_INSTANCE,
        profile='isolated-1',
        timeout=WORKER_EXIT_TIMEOUT_S,
        ready_check=_ready_check(sync_only.healthcheck),
    ) as worker:
        handle = unwrap_send(await sync_only.healthcheck.send_async())
        result = await handle.get_async(timeout_ms=RESULT_TIMEOUT_MS)

        assert_ok(result)
        assert worker.poll() is None
        assert loop_failures.read_text(encoding='ascii') == '100'
