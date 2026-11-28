"""Workflow pause: the workflow_done notification comes from the status trigger.

Both pause paths (the handle's pause_workflow_async and the engine's on_error=pause)
change each paused workflow from RUNNING to PAUSED in one transaction. The
status trigger sends one workflow_done per paused workflow at commit; the
pause paths send no NOTIFY of their own. The handle's pause runs on a broker
whose connections go through a proxy that refuses every client statement that
names pg_notify, so a NOTIFY of the pause itself fails the pause.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncGenerator, Generator

import psycopg
import pytest
import pytest_asyncio
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import is_ok
from horsies.core.utils.url import to_psycopg_url
from horsies.core.workflows.lifecycle import pause_workflow_async
from tests.integration.autocommit_support import proxied_url, upstream_of
from tests.integration.conftest import DB_URL
from tests.integration.round_trip_proxy import RoundTripProxy

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


@pytest.fixture
def proxy(db_url: str) -> Generator[RoundTripProxy]:
    """A proxy in front of the test database."""
    host, port = upstream_of(db_url)
    started = RoundTripProxy(host, port)
    started.start()
    yield started
    started.stop()


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
    proxy: RoundTripProxy,
    db_url: str,
) -> None:
    _ = clean_workflow_tables, broker
    parent_id, child_id = await _seed_parent_with_child(
        session, on_error='fail',
    )
    proxy.refuse_statements(('pg_notify',))
    proxied = PostgresBroker(
        PostgresConfig(database_url=SecretStr(proxied_url(db_url, proxy))),
    )
    try:
        paused = await pause_workflow_async(proxied, uuid.UUID(parent_id))
    finally:
        _ = await proxied.close_async()

    assert is_ok(paused) and paused.ok_value is True, paused
    assert proxy.refused_statements() == ()
    assert sorted(await _received(workflow_done_listener)) == sorted(
        [parent_id, child_id],
    )
    assert await _status_of(session, parent_id) == 'PAUSED'
    assert await _status_of(session, child_id) == 'PAUSED'


