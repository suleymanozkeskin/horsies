"""A stored timeout that the worker cannot use fails the task in one claim.

An older version accepted any `timeout_ms` of at least 1000, so a queued row can
hold a value above `MAX_TASK_TIMEOUT_MS`. The claim reads it into a refusal, the
child sets the row RUNNING and returns WORKER_SERIALIZATION_ERROR before it
resolves the task, and the normal persist path fails the task. These tests run
the real claim statement, the child entry in this process, and the parent
finalize.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import (
    MAX_TASK_TIMEOUT_MS,
    OperationalErrorCode,
    RetryPolicy,
    TaskError,
    TaskResult,
)
from horsies.core.models.workflow import TaskNode
from horsies.core.types.result import is_ok
from horsies.core.worker.claiming import ClaimPass
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.current import set_current_app
from horsies.core.worker.stored_timeout import (
    NoTimeout,
    StoredTimeout,
    TimeoutValueInvalid,
)
from horsies.core.worker.worker import Worker, _initialize_worker_pool, _run_task_entry
from tests.broker_resources_support import current_resources, listener_of
from tests.fixtures.slots.handles import task_finished_handle
from tests.integration.conftest import (
    get_task_status as get_workflow_task_status,
    get_workflow_status,
    make_simple_task,
    make_workflow_spec,
    start_ok,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

_ABOVE_LIMIT = MAX_TASK_TIMEOUT_MS + 1


def _make_worker(broker: PostgresBroker, app: Horsies) -> Worker:
    cfg = WorkerConfig(
        dsn=broker.config.database_url.get_secret_value(),
        psycopg_dsn=listener_of(broker).database_url,
        queues=['default'],
        processes=1,
    )
    worker = Worker(
        session_factory=current_resources(broker).session_factory,
        listener=MagicMock(),
        cfg=cfg,
    )
    worker._app = app
    worker.broker = broker
    return worker


def _run_in_process(
    worker: Worker,
    broker: PostgresBroker,
    seen: list[StoredTimeout],
) -> None:
    """Replace the slot pool: run the child entry here, then the parent finalize."""
    database_url = listener_of(broker).database_url
    _initialize_worker_pool(database_url)

    async def _dispatch(
        task_id: str,
        task_name: str,
        args_json: str | None,
        kwargs_json: str | None,
        queue_name: str = 'default',
        is_workflow_task: bool = True,
        timeout: StoredTimeout = NoTimeout(),
        claimed_at: datetime | None = None,
    ) -> None:
        seen.append(timeout)
        wire = _run_task_entry(
            task_name=task_name,
            args_json=args_json,
            kwargs_json=kwargs_json,
            task_id=task_id,
            database_url=database_url,
            master_worker_id=worker.worker_instance_id,
            is_workflow_task=is_workflow_task,
            timeout=timeout,
            claimed_at=claimed_at,
        )
        result = await worker._finalize_after(
            task_finished_handle(wire[0], wire[1], wire[2]),
            task_id,
            queue_name,
            is_workflow_task,
            timeout=timeout,
            task_name=task_name,
            claimed_at=claimed_at,
        )
        assert is_ok(result), result

    worker._dispatch_one = AsyncMock(side_effect=_dispatch)  # type: ignore[method-assign]


async def _store_timeout(session: AsyncSession, task_id: str, timeout_ms: int) -> None:
    """Write the timeout into the row's stored options, as an older version could."""
    stored = (
        await session.execute(
            text('SELECT task_options FROM horsies_tasks WHERE id = :id'),
            {'id': task_id},
        )
    ).scalar_one()
    options: dict[str, Any] = json.loads(stored) if stored else {}
    options['timeout_ms'] = timeout_ms
    await session.execute(
        text('UPDATE horsies_tasks SET task_options = :opts WHERE id = :id'),
        {'opts': json.dumps(options), 'id': task_id},
    )
    await session.commit()


async def _task_row(session: AsyncSession, task_id: str) -> Any:
    row = (
        await session.execute(
            text(
                'SELECT status, result FROM itest_task_rows '
                'WHERE id = CAST(:id AS uuid)'
            ),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None, f'task {task_id} not found'
    return row


async def _attempts(session: AsyncSession, task_id: str) -> list[Any]:
    return list(
        (
            await session.execute(
                text(
                    'SELECT attempt, outcome, will_retry, error_code '
                    'FROM horsies_task_attempts WHERE task_id = :id ORDER BY attempt'
                ),
                {'id': task_id},
            )
        ).fetchall()
    )


async def test_a_refused_timeout_fails_in_one_claim_and_is_not_claimed_again(
    session: AsyncSession,
    broker: PostgresBroker,
    app: Horsies,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    called: list[int] = []

    @app.task(task_name='stored_timeout_plain')
    def stored_timeout_plain(*, value: int) -> TaskResult[int, TaskError]:
        called.append(value)
        return TaskResult(ok=value)

    task_id = (await stored_timeout_plain.send_async(value=1)).unwrap().task_id
    await _store_timeout(session, task_id, _ABOVE_LIMIT)
    set_current_app(app)
    worker = _make_worker(broker, app)
    seen: list[StoredTimeout] = []
    _run_in_process(worker, broker, seen)

    first: ClaimPass = await worker._claim_and_dispatch_all()

    assert first.claimed == 1
    assert len(seen) == 1 and isinstance(seen[0], TimeoutValueInvalid)
    assert called == []
    row = await _task_row(session, task_id)
    assert row.status == 'FAILED'
    assert OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value in json.dumps(row.result)
    attempts = await _attempts(session, task_id)
    assert [(a.outcome, a.will_retry, a.error_code) for a in attempts] == [
        ('FAILED', False, OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value),
    ]

    second: ClaimPass = await worker._claim_and_dispatch_all()

    assert second.claimed == 0
    assert len(seen) == 1


async def test_a_refused_timeout_fails_its_workflow_node(
    session: AsyncSession,
    broker: PostgresBroker,
    app: Horsies,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    task_a = make_simple_task(app, 'stored_timeout_node_a')
    task_b = make_simple_task(app, 'stored_timeout_node_b')
    node_a = TaskNode(fn=task_a, kwargs={'value': 1})
    node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
    spec = make_workflow_spec(
        broker=broker, name='stored_timeout_flow', tasks=[node_a, node_b],
    )
    handle = await start_ok(spec, broker)
    task_id = (
        await session.execute(
            text(
                'SELECT task_id FROM horsies_workflow_tasks '
                'WHERE workflow_id = :wf AND task_index = 0'
            ),
            {'wf': handle.workflow_id},
        )
    ).scalar_one()
    await _store_timeout(session, str(task_id), _ABOVE_LIMIT)
    set_current_app(app)
    worker = _make_worker(broker, app)
    _run_in_process(worker, broker, [])

    claim_pass: ClaimPass = await worker._claim_and_dispatch_all()

    assert claim_pass.claimed == 1
    assert await get_workflow_task_status(session, handle.workflow_id, 0) == 'FAILED'
    assert await get_workflow_task_status(session, handle.workflow_id, 1) == 'SKIPPED'
    assert await get_workflow_status(session, handle.workflow_id) == 'FAILED'


async def test_a_refused_timeout_retries_until_max_retries_when_asked(
    session: AsyncSession,
    broker: PostgresBroker,
    app: Horsies,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    @app.task(
        task_name='stored_timeout_retried',
        retry_policy=RetryPolicy.fixed(
            [1, 1],
            auto_retry_for=[OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value],
        ),
    )
    def stored_timeout_retried(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    task_id = (await stored_timeout_retried.send_async(value=1)).unwrap().task_id
    await _store_timeout(session, task_id, _ABOVE_LIMIT)
    set_current_app(app)
    worker = _make_worker(broker, app)
    seen: list[StoredTimeout] = []
    _run_in_process(worker, broker, seen)

    for _ in range(6):
        await worker._claim_and_dispatch_all()
        if (await _task_row(session, task_id)).status == 'FAILED':
            break
        await asyncio.sleep(1.2)

    assert (await _task_row(session, task_id)).status == 'FAILED'
    attempts = await _attempts(session, task_id)
    assert [(a.will_retry, a.error_code) for a in attempts] == [
        (True, OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value),
        (True, OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value),
        (False, OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value),
    ]
    assert len(seen) == 3
