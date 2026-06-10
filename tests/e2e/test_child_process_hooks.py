"""E2E: @app.on_child_process_start through real workers.

Graduates tests/spike/conn_accum: the documented hook body (dispose +
NullPool rebind) must drop the post-burst app-owned connection floor to
zero, while the unfixed control retains it; and a failing hook must stop
the worker at boot instead of restart-looping.
"""

from __future__ import annotations

import asyncio
import time
from typing import Protocol

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.task_send_types import TaskSendResult
from horsies.core.task_decorator import TaskHandle
from horsies.core.types.result import is_err

from tests.e2e.helpers.assertions import unwrap_send
from tests.e2e.helpers.db import wait_for_all_terminal
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import instance_child_hook
from tests.e2e.tasks import instance_child_hook_unfixed


FIXED_INSTANCE = 'tests.e2e.tasks.instance_child_hook:app'
UNFIXED_INSTANCE = 'tests.e2e.tasks.instance_child_hook_unfixed:app'
FAIL_INSTANCE = 'tests.e2e.tasks.instance_child_hook_fail:app'

PROCESSES = 2
BURST = 8
SLEEP_MS = 300
DRAIN_S = 3.0


class _HealthcheckTask(Protocol):
    def send(self) -> TaskSendResult[TaskHandle[str]]: ...


def _make_ready_check(task_func: _HealthcheckTask):
    handle: TaskHandle[str] | None = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            r = task_func.send()
            if is_err(r):
                return False
            handle = r.ok_value
        result = handle.get(timeout_ms=2000)
        return result.is_ok()

    return _check


async def _count_app_engine_connections(
    broker: PostgresBroker,
    application_name: str,
) -> int:
    async with broker.session_factory() as session:
        row = (
            await session.execute(
                text("""
                    SELECT count(*) FROM pg_stat_activity
                    WHERE application_name = :name
                """),
                {'name': application_name},
            )
        ).fetchone()
        assert row is not None
        return int(row[0])


class _DbTask(Protocol):
    def send(self, *, sleep_ms: int) -> TaskSendResult[TaskHandle[str]]: ...


async def _run_burst(
    broker: PostgresBroker,
    db_task: _DbTask,
) -> None:
    handles = [
        unwrap_send(db_task.send(sleep_ms=SLEEP_MS)) for _ in range(BURST)
    ]
    await wait_for_all_terminal(
        broker.session_factory,
        [h.task_id for h in handles],
        timeout_s=60.0,
    )
    await asyncio.sleep(DRAIN_S)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_hook_rebind_drops_post_burst_floor_to_zero(
    broker: PostgresBroker,
) -> None:
    """Documented hook body (dispose + NullPool rebind) → floor 0 after drain."""
    with run_worker(
        FIXED_INSTANCE,
        processes=PROCESSES,
        ready_check=_make_ready_check(instance_child_hook.healthcheck),
    ):
        await _run_burst(broker, instance_child_hook.db_task)
        floor = await _count_app_engine_connections(
            broker, instance_child_hook.APP_ENGINE_APPLICATION_NAME,
        )

    assert floor == 0, (
        f'expected NullPool rebind to drop the app-engine floor to 0, '
        f'got {floor}'
    )


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_unfixed_control_retains_post_burst_floor(
    broker: PostgresBroker,
) -> None:
    """No hook → QueuePool retention keeps the floor >= 1 after drain.

    Also validates application_name attribution: if it broke, the fixed
    test's 0 would be vacuous.
    """
    with run_worker(
        UNFIXED_INSTANCE,
        processes=PROCESSES,
        ready_check=_make_ready_check(instance_child_hook_unfixed.healthcheck),
    ):
        await _run_burst(broker, instance_child_hook_unfixed.db_task)
        floor = await _count_app_engine_connections(
            broker, instance_child_hook_unfixed.APP_ENGINE_APPLICATION_NAME,
        )

    assert floor >= 1, (
        'expected the unfixed QueuePool instance to retain idle app-engine '
        'connections after the burst (the behavior the hook exists to fix), '
        f'got {floor}'
    )


@pytest.mark.e2e
def test_failing_hook_stops_worker_at_boot() -> None:
    """Fail-closed: the worker exits with the hook named, no restart loop."""
    started = time.monotonic()
    with pytest.raises(RuntimeError) as exc_info:
        # A never-true ready check keeps run_worker polling until the worker
        # process exits, which raises with its captured stdout/stderr.
        with run_worker(
            FAIL_INSTANCE,
            processes=1,
            timeout=30.0,
            ready_check=lambda: False,
        ):
            pass

    message = str(exc_info.value)
    assert 'exited before becoming ready' in message
    assert 'on_child_process_start' in message
    # A restart loop would burn the full ready timeout; a fail-closed boot
    # failure surfaces well before it.
    assert time.monotonic() - started < 25.0
