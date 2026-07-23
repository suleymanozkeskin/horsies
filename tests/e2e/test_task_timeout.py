"""E2E: per-task timeout_ms enforcement through a real worker.

A task declared with timeout_ms outruns its deadline; the worker's
parent-side enforcement persists TASK_TIMEOUT, SIGKILLs the child, and
the process pool restarts. The worker must stay functional afterwards.
"""

from __future__ import annotations

import json
from typing import Protocol

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.task_send_types import TaskSendResult
from horsies.core.models.tasks import OutcomeCode
from horsies.core.task_decorator import TaskHandle
from horsies.core.types.result import is_err

from tests.e2e.helpers.assertions import unwrap_send
from tests.e2e.helpers.db import wait_for_status
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import basic as basic_tasks


DEFAULT_INSTANCE = 'tests.e2e.tasks.instance:app'


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


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_timeout_fails_task_and_worker_survives(
    broker: PostgresBroker,
) -> None:
    """Sleeping task with timeout_ms=2000 → FAILED/TASK_TIMEOUT; worker
    survives the pool restart and completes a subsequent task."""
    with run_worker(
        DEFAULT_INSTANCE,
        processes=1,
        ready_check=_make_ready_check(basic_tasks.healthcheck),
    ):
        handle = unwrap_send(await basic_tasks.timeout_sleeper.send_async(duration_ms=60_000))

        # Deadline fires at 2s; allow slack for claim + dispatch + persist.
        await wait_for_status(
            broker.session_factory,
            handle.task_id,
            'FAILED',
            timeout_s=20.0,
        )

        async with broker.session_factory() as session:
            row = (
                await session.execute(
                    text("""
                        SELECT error_code, result
                        FROM horsies_tasks WHERE id = :id
                    """),
                    {'id': handle.task_id},
                )
            ).fetchone()
            assert row is not None
            assert row.error_code == OutcomeCode.TASK_TIMEOUT.value
            assert row.result is not None
            err = json.loads(row.result)['err']
            assert err['error_code'] == {
                '__builtin_task_code__': OutcomeCode.TASK_TIMEOUT.value
            }

            attempt = (
                await session.execute(
                    text("""
                        SELECT outcome, will_retry, error_code
                        FROM horsies_task_attempts WHERE task_id = :id
                    """),
                    {'id': handle.task_id},
                )
            ).fetchone()
            assert attempt is not None
            assert attempt.outcome == 'FAILED'
            assert attempt.will_retry is False
            assert attempt.error_code == OutcomeCode.TASK_TIMEOUT.value

        # The SIGKILL broke the process pool; the worker restarts it and
        # must still execute new work.
        follow_up = unwrap_send(await basic_tasks.simple_task.send_async(x=21))
        result = follow_up.get(timeout_ms=30_000)
        assert result.is_ok()
        assert result.ok_value == 42
