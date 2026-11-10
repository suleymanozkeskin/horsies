"""E2E: a worker child creates the task loop at boot.

The child start hook of the instances makes event loop creation raise
OSError(EMFILE) while a budget file allows it, or always in one slot. A child
whose app has an ``async def`` task creates its loop at boot, so a failure
there fails the boot. That cause can clear: the slot is down and boots again
while another slot serves, and the worker reports itself degraded. A worker
with no other slot that can serve exits non-zero after
``CHILD_BOOT_ATTEMPTS`` failed boots, before it claims any task. A child whose
app has no async task creates no loop.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.boot_failure import TaskLoopCreateFailed
from horsies.core.models.health import DownSlot, SlotsDown, WorkerStateSnapshot
from horsies.core.task_decorator import TaskFunction, TaskHandle
from horsies.core.types.result import is_err, is_ok
from horsies.core.types.status import TaskStatus
from horsies.core.worker.slots import CHILD_BOOT_ATTEMPTS
from tests.broker_resources_support import current_resources
from tests.e2e.helpers.assertions import assert_ok, unwrap_send
from tests.e2e.helpers.db import read_task
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import instance_task_loop_boot as boot
from tests.e2e.tasks import instance_task_loop_boot_sync_only as sync_only
from tests.e2e.tasks.loop_creation_budget import (
    LOOP_FAILS_ON_SLOT_ENV,
    LOOP_FAILURES_ENV,
)

INSTANCE = 'tests.e2e.tasks.instance_task_loop_boot:app'
SYNC_ONLY_INSTANCE = 'tests.e2e.tasks.instance_task_loop_boot_sync_only:app'
RESULT_TIMEOUT_MS = 30_000
WORKER_EXIT_TIMEOUT_S = 60.0
LOOP_CAUSE = (
    'the child could not create the event loop for async tasks: '
    'OSError: [Errno 24] Too many open files'
)
SNAPSHOT_POLLS: Final = 40
SNAPSHOT_POLL_INTERVAL_S: Final = 0.5


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
            timeout=WORKER_EXIT_TIMEOUT_S,
            ready_check=lambda: False,
        ):
            pass

    message = str(stopped.value)
    assert 'exited before becoming ready' in message
    exit_code = re.search(r'\(code=(-?\d+)\)', message)
    assert exit_code is not None and int(exit_code.group(1)) != 0
    assert (
        f'Slot 0 is down: {LOOP_CAUSE}. Failed boots: {CHILD_BOOT_ATTEMPTS}, '
        f'counted attempts: {CHILD_BOOT_ATTEMPTS}/{CHILD_BOOT_ATTEMPTS}.'
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
        timeout=WORKER_EXIT_TIMEOUT_S,
        ready_check=_ready_check(sync_only.healthcheck),
    ) as worker:
        handle = unwrap_send(await sync_only.healthcheck.send_async())
        result = await handle.get_async(timeout_ms=RESULT_TIMEOUT_MS)

        assert_ok(result)
        assert worker.poll() is None
        assert loop_failures.read_text(encoding='ascii') == '100'


async def _degraded_snapshot(
    broker: PostgresBroker,
    started_after: datetime,
) -> WorkerStateSnapshot:
    """The first worker state, of a worker started after ``started_after``,
    that reports a down slot. Polls a bounded number of times."""
    for _ in range(SNAPSHOT_POLLS):
        listed = await broker.list_worker_states_async()
        assert is_ok(listed), listed
        for snapshot in listed.ok_value:
            match snapshot.slot_health:
                case SlotsDown() if snapshot.worker_started_at >= started_after:
                    return snapshot
                case _:
                    continue
        await asyncio.sleep(SNAPSHOT_POLL_INTERVAL_S)
    raise AssertionError('no worker state reported a down slot in time')


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_a_slot_that_cannot_create_its_task_loop_leaves_the_worker_degraded(
    broker: PostgresBroker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Slot 1 can never create its loop. Slot 0 serves, so the worker starts
    degraded, runs the async task, keeps running, and its worker state names
    slot 1 and the cause."""
    monkeypatch.setenv(LOOP_FAILS_ON_SLOT_ENV, '1')
    started_after = datetime.now(timezone.utc)

    with run_worker(
        INSTANCE,
        processes=2,
        timeout=WORKER_EXIT_TIMEOUT_S,
        ready_check=_ready_check(boot.healthcheck),
    ) as worker:
        handle = unwrap_send(await boot.async_pid.send_async())
        assert_ok(await handle.get_async(timeout_ms=RESULT_TIMEOUT_MS))
        snapshot = await _degraded_snapshot(broker, started_after)
        assert worker.poll() is None

    match snapshot.slot_health:
        case SlotsDown(slots=(DownSlot(slot=1, cause=TaskLoopCreateFailed()) as down,)):
            assert down.detail == 'OSError: [Errno 24] Too many open files'
            assert down.failed_boots >= 1
        case other:
            raise AssertionError(other)
