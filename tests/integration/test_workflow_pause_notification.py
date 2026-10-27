"""Workflow pause: the workflow_done notification comes from the status trigger.

Both pause paths (the handle's pause_workflow and the engine's on_error=pause)
change each paused workflow from RUNNING to PAUSED in one transaction. The
status trigger sends one workflow_done per paused workflow at commit; the
pause paths send no NOTIFY of their own.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncGenerator, Generator
from contextlib import contextmanager
from typing import Any

import psycopg
import pytest
import pytest_asyncio
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.types.result import is_ok
from horsies.core.utils.url import to_psycopg_url
from horsies.core.workflows.engine import (
    _handle_workflow_task_failure,  # pyright: ignore[reportPrivateUsage]
)
from horsies.core.workflows.lifecycle import pause_workflow
from tests.integration.conftest import DB_URL
from tests.broker_resources_support import current_resources

pytestmark = [pytest.mark.integration]

NOTIFICATION_WAIT_SECONDS = 0.5


@pytest_asyncio.fixture
async def workflow_done_listener() -> AsyncGenerator[psycopg.AsyncConnection, None]:
    connection = await psycopg.AsyncConnection.connect(
        to_psycopg_url(DB_URL), autocommit=True,
    )
    await connection.execute('LISTEN workflow_done')
    yield connection
    await connection.close()


async def _received(listener: psycopg.AsyncConnection) -> list[str]:
    payloads: list[str] = []
    try:
        async with asyncio.timeout(NOTIFICATION_WAIT_SECONDS):
            async for notification in listener.notifies():
                payloads.append(notification.payload)
    except TimeoutError:
        pass
    return payloads


@contextmanager
def _captured_statements(
    engine: AsyncEngine,
) -> Generator[list[str], None, None]:
    statements: list[str] = []

    def _record(
        conn: Any, cursor: Any, statement: str, *args: Any,
    ) -> None:
        statements.append(statement)

    event.listen(engine.sync_engine, 'before_cursor_execute', _record)
    try:
        yield statements
    finally:
        event.remove(engine.sync_engine, 'before_cursor_execute', _record)


async def _insert_running_workflow(
    session: AsyncSession,
    *,
    on_error: str,
    parent_workflow_id: str | None = None,
) -> str:
    workflow_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 parent_workflow_id, parent_task_index,
                 sent_at, created_at, started_at, updated_at)
            VALUES
                (:id, 'pause_notification', 'RUNNING', :on_error, :depth,
                 :root, :parent, :parent_index, NOW(), NOW(), NOW(), NOW())
        """),
        {
            'id': workflow_id,
            'on_error': on_error,
            'depth': 0 if parent_workflow_id is None else 1,
            'root': parent_workflow_id or workflow_id,
            'parent': parent_workflow_id,
            'parent_index': None if parent_workflow_id is None else 0,
        },
    )
    return workflow_id


async def _seed_parent_with_child(
    session: AsyncSession,
    *,
    on_error: str,
) -> tuple[str, str]:
    parent_id = await _insert_running_workflow(session, on_error=on_error)
    child_id = await _insert_running_workflow(
        session, on_error='fail', parent_workflow_id=parent_id,
    )
    await session.commit()
    return parent_id, child_id


async def _status_of(session: AsyncSession, workflow_id: str) -> str:
    return (await session.execute(
        text('SELECT status FROM horsies_workflows WHERE id = :id'),
        {'id': workflow_id},
    )).scalar_one()


@pytest.mark.asyncio(loop_scope='function')
async def test_handle_pause_notifies_each_paused_workflow_through_the_trigger(
    clean_workflow_tables: None,
    broker: PostgresBroker,
    session: AsyncSession,
    workflow_done_listener: psycopg.AsyncConnection,
) -> None:
    _ = clean_workflow_tables
    parent_id, child_id = await _seed_parent_with_child(
        session, on_error='fail',
    )

    with _captured_statements(current_resources(broker).engine) as statements:
        paused = await pause_workflow(broker, parent_id)

    assert is_ok(paused) and paused.ok_value is True, paused
    assert sorted(await _received(workflow_done_listener)) == sorted(
        [parent_id, child_id],
    )
    assert not [s for s in statements if 'pg_notify' in s]
    assert await _status_of(session, parent_id) == 'PAUSED'
    assert await _status_of(session, child_id) == 'PAUSED'


@pytest.mark.asyncio(loop_scope='function')
async def test_on_error_pause_notifies_each_paused_workflow_after_commit(
    clean_workflow_tables: None,
    engine: AsyncEngine,
    session: AsyncSession,
    workflow_done_listener: psycopg.AsyncConnection,
) -> None:
    _ = clean_workflow_tables
    parent_id, child_id = await _seed_parent_with_child(
        session, on_error='pause',
    )
    failure: TaskResult[Any, TaskError] = TaskResult(
        err=TaskError(error_code='NODE_FAILED', message='node failed'),
    )

    with _captured_statements(engine) as statements:
        should_continue = await _handle_workflow_task_failure(
            session, parent_id, failure,
        )
        assert await _received(workflow_done_listener) == []
        await session.commit()

    assert should_continue is False
    assert sorted(await _received(workflow_done_listener)) == sorted(
        [parent_id, child_id],
    )
    assert not [s for s in statements if 'pg_notify' in s]
    assert await _status_of(session, parent_id) == 'PAUSED'
    assert await _status_of(session, child_id) == 'PAUSED'
