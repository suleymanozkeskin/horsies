"""E2E: per-task timeout_ms enforcement through a real worker.

A task declared with timeout_ms outruns its deadline; the worker's
parent-side enforcement persists TASK_TIMEOUT, SIGKILLs the child, and
the process pool restarts. The worker must stay functional afterwards.
"""

from __future__ import annotations

import asyncio
import json
from typing import Protocol

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.task_send_types import TaskSendResult
from horsies.core.models.tasks import OperationalErrorCode, OutcomeCode
from horsies.core.task_decorator import TaskHandle
from horsies.core.types.result import is_err

from tests.e2e.helpers.assertions import unwrap_send
from tests.e2e.helpers.db import wait_for_status
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import basic as basic_tasks
from tests.integration.history_seeding import read_attempts


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
                        FROM itest_task_rows WHERE id = :id
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

            # The move purges live attempt rows into the record's
            # snapshot; one reader answers for both homes.
            attempts = await read_attempts(session, handle.task_id)
            assert len(attempts) == 1
            attempt = attempts[0]
            assert attempt.outcome == 'FAILED'
            assert attempt.will_retry is False
            assert attempt.error_code == OutcomeCode.TASK_TIMEOUT.value

        # The SIGKILL broke the process pool; the worker restarts it and
        # must still execute new work.
        follow_up = unwrap_send(await basic_tasks.simple_task.send_async(x=21))
        result = follow_up.get(timeout_ms=30_000)
        assert result.is_ok()
        assert result.ok_value == 42


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_timeout_backlog_drains_without_killing_worker(
    broker: PostgresBroker,
) -> None:
    """A backlog of timeout-prone tasks drains with the worker surviving.

    Regression for the warmup-interruption crash: each timeout SIGKILL
    breaks the pool; with the backlog re-dispatching immediately, the next
    kill used to land while the replacement pool was still warming, the
    warmup shortfall was classified process-fatal, and the worker exited
    (`worker child warmup started X/N process(es)`). A worker that
    publishes only fully-warm pools and retries kill-shaped warmup
    interruptions must instead drain the whole backlog and stay up.
    """
    backlog_size = 24
    with run_worker(
        DEFAULT_INSTANCE,
        processes=6,
        ready_check=_make_ready_check(basic_tasks.healthcheck),
    ):
        # Staggered sends desynchronize the deadline kills: instead of one
        # wave of simultaneous timeouts per batch, kills keep arriving while
        # earlier kills' pool replacements are still warming — the exact
        # pressure profile that used to catch a warmup mid-flight.
        handles: list[TaskHandle[str]] = []
        for _ in range(backlog_size):
            handles.append(
                unwrap_send(
                    await basic_tasks.timeout_sleeper.send_async(duration_ms=60_000)
                )
            )
            await asyncio.sleep(0.1)

        # Every task must reach a terminal state; a worker killed mid-drain
        # leaves the tail of the backlog stuck and times this out.
        for handle in handles:
            await wait_for_status(
                broker.session_factory,
                handle.task_id,
                'FAILED',
                timeout_s=180.0,
            )

        async with broker.session_factory() as session:
            rows = (
                await session.execute(
                    text("""
                        SELECT id, error_code
                        FROM itest_task_rows
                        WHERE id = ANY(:ids)
                    """),
                    {'ids': [h.task_id for h in handles]},
                )
            ).fetchall()
            assert len(rows) == backlog_size
            # Deadline enforcement fired (the pool-breaking trigger);
            # collateral tasks caught in a pool break may legitimately
            # carry the crash-recovery code instead.
            codes = {row.error_code for row in rows}
            assert OutcomeCode.TASK_TIMEOUT.value in codes
            assert codes <= {
                OutcomeCode.TASK_TIMEOUT.value,
                OperationalErrorCode.WORKER_CRASHED.value,
            }

        # The load-bearing assertion: the worker survived the drain.
        follow_up = unwrap_send(await basic_tasks.simple_task.send_async(x=21))
        result = follow_up.get(timeout_ms=30_000)
        assert result.is_ok()
        assert result.ok_value == 42


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_pool_breaker_storm_does_not_kill_worker(
    broker: PostgresBroker,
) -> None:
    """A storm of self-killing tasks must not take the worker down.

    Regression for the warmup-interruption crash (the ledger's original
    repro shape): every pool_breaker execution SIGKILLs its child, breaking
    the pool the instant it runs. With a queue of them, one used to be
    dispatched onto the replacement pool while it was still warming and
    break THAT, failing the warmup itself — classified process-fatal, the
    worker exited (`worker child warmup started X/N process(es)`). A worker
    that publishes only fully-warm pools must fail every breaker
    terminally (WORKER_CRASHED via crash recovery) and stay up.
    """
    storm_size = 12
    with run_worker(
        DEFAULT_INSTANCE,
        processes=6,
        ready_check=_make_ready_check(basic_tasks.healthcheck),
    ):
        handles: list[TaskHandle[str]] = []
        for _ in range(storm_size):
            handles.append(
                unwrap_send(await basic_tasks.pool_breaker.send_async())
            )
            await asyncio.sleep(0.05)

        # A worker killed mid-storm leaves the tail stuck and times this out.
        for handle in handles:
            await wait_for_status(
                broker.session_factory,
                handle.task_id,
                'FAILED',
                timeout_s=180.0,
            )

        async with broker.session_factory() as session:
            rows = (
                await session.execute(
                    text("""
                        SELECT error_code FROM itest_task_rows
                        WHERE id = ANY(:ids)
                    """),
                    {'ids': [h.task_id for h in handles]},
                )
            ).fetchall()
            assert len(rows) == storm_size
            assert {row.error_code for row in rows} == {
                OperationalErrorCode.WORKER_CRASHED.value
            }

        follow_up = unwrap_send(await basic_tasks.simple_task.send_async(x=21))
        result = follow_up.get(timeout_ms=30_000)
        assert result.is_ok()
        assert result.ok_value == 42
