"""Focused repros for queue backfill and fan-in join orphaning."""

from __future__ import annotations

import asyncio
import json
from typing import Protocol
from uuid import uuid4

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.task_send_types import TaskSendResult
from horsies.core.task_decorator import TaskHandle
from horsies.core.types.result import is_err

from tests.e2e.helpers.assertions import start_ok_sync
from tests.e2e.helpers.db import wait_for_all_terminal
from tests.e2e.helpers.worker import run_worker
from tests.e2e.helpers.workflow import wait_for_workflow_completion
from tests.e2e.tasks.basic import healthcheck
from tests.e2e.tasks import queues_custom
from tests.e2e.tasks import workflows as wf_tasks


CUSTOM_INSTANCE = 'tests.e2e.tasks.instance_custom:app'
DEFAULT_INSTANCE = 'tests.e2e.tasks.instance:app'


class _HealthcheckTask(Protocol):
    def send(self) -> TaskSendResult[TaskHandle[str]]: ...


def _make_ready_check(task_func: _HealthcheckTask):
    handle: TaskHandle[str] | None = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            result = task_func.send()
            if is_err(result):
                return False
            handle = result.ok_value
        return handle.get(timeout_ms=2000).is_ok()

    return _check


async def _wait_for_high_queue_saturated(
    broker: PostgresBroker,
    high_ids: list[str],
    *,
    running: int,
    timeout_s: float = 10.0,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    last_snapshot: tuple[int, int] = (0, 0)
    while asyncio.get_running_loop().time() < deadline:
        async with broker.session_factory() as session:
            row = (
                await session.execute(
                    text("""
                        SELECT
                            COUNT(*) FILTER (WHERE status = 'RUNNING') AS running,
                            COUNT(*) FILTER (WHERE status = 'PENDING') AS pending
                        FROM horsies_tasks
                        WHERE id = ANY(:ids)
                    """),
                    {'ids': high_ids},
                )
            ).one()
        last_snapshot = (int(row.running or 0), int(row.pending or 0))
        if last_snapshot[0] >= running and last_snapshot[1] > 0:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(
        'high queue did not become saturated with backlog; '
        f'last_snapshot={last_snapshot}'
    )


async def _wait_for_normal_started_before_high_drains(
    broker: PostgresBroker,
    *,
    high_ids: list[str],
    normal_ids: list[str],
    timeout_s: float = 2.0,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    last_snapshot: tuple[int, int] = (0, 0)
    while asyncio.get_running_loop().time() < deadline:
        async with broker.session_factory() as session:
            row = (
                await session.execute(
                    text("""
                        SELECT
                            (
                                SELECT COUNT(*)
                                FROM itest_task_rows
                                WHERE id = ANY(:normal_ids)
                                  AND status IN ('RUNNING', 'COMPLETED', 'FAILED')
                            ) AS normal_started,
                            (
                                SELECT COUNT(*)
                                FROM horsies_tasks
                                WHERE id = ANY(:high_ids)
                                  AND status NOT IN (
                                      'COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED'
                                  )
                            ) AS high_not_terminal
                    """),
                    {'high_ids': high_ids, 'normal_ids': normal_ids},
                )
            ).one()
        last_snapshot = (
            int(row.normal_started or 0),
            int(row.high_not_terminal or 0),
        )
        if last_snapshot[0] > 0 and last_snapshot[1] > 0:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(
        'normal queue did not backfill while high queue was capped; '
        f'last_snapshot={last_snapshot}'
    )


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_lower_priority_queue_backfills_when_high_queue_is_capped(
    custom_broker: PostgresBroker,
) -> None:
    """High queue cap must not block spare worker slots from lower queues."""
    with run_worker(
        CUSTOM_INSTANCE,
        processes=8,
        ready_check=_make_ready_check(queues_custom.high_task),
    ):
        high_ids = [
            custom_broker.enqueue(
                task_name='e2e_custom_slow',
                queue_name='high',
                task_id=str(uuid4()),
                enqueue_sha=f'high-{idx}',
                kwargs_json=json.dumps({'duration_ms': 3000}),
            ).unwrap()
            for idx in range(10)
        ]

        await _wait_for_high_queue_saturated(
            custom_broker, high_ids, running=5,
        )

        normal_ids = [
            custom_broker.enqueue(
                task_name='e2e_custom_slow',
                queue_name='normal',
                task_id=str(uuid4()),
                enqueue_sha=f'normal-{idx}',
                kwargs_json=json.dumps({'duration_ms': 1000}),
            ).unwrap()
            for idx in range(2)
        ]

        await _wait_for_normal_started_before_high_drains(
            custom_broker,
            high_ids=high_ids,
            normal_ids=normal_ids,
        )
        await wait_for_all_terminal(
            custom_broker.session_factory,
            [*high_ids, *normal_ids],
            timeout_s=20.0,
        )


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_wide_fanin_join_does_not_create_orphaned_join_tasks(
    broker: PostgresBroker,
) -> None:
    """Concurrent dependency completion should enqueue one linked join task."""
    with run_worker(
        DEFAULT_INSTANCE,
        processes=8,
        ready_check=_make_ready_check(healthcheck),
    ):
        handle = start_ok_sync(wf_tasks.spec_wide_fanin_join)
        status = await wait_for_workflow_completion(
            broker.session_factory,
            handle.workflow_id,
            timeout_s=30.0,
        )

    assert status == 'COMPLETED'

    async with broker.session_factory() as session:
        row = (
            await session.execute(
                text("""
                    SELECT
                        COUNT(t.id) AS task_rows,
                        COUNT(wt.task_id) AS linked_rows,
                        COUNT(*) FILTER (WHERE wt.task_id IS NULL) AS orphan_rows,
                        COUNT(*) FILTER (
                            WHERE wt.task_id IS NULL AND t.status = 'CLAIMED'
                        ) AS orphan_claimed_rows
                    FROM itest_task_rows t
                    LEFT JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
                    WHERE t.task_name = 'e2e_wf_join_barrier'
                """),
            )
        ).one()

    assert int(row.task_rows or 0) == 1
    assert int(row.linked_rows or 0) == 1
    assert int(row.orphan_rows or 0) == 0
    assert int(row.orphan_claimed_rows or 0) == 0
