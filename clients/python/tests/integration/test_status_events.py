"""The monitoring channel ``horsies_task_status`` of the task rows.

The web app listens to it and re-reads the tasks on each event. These tests
pin which writes send it:

- an insert and a status change send it, except the start (RUNNING): a task's
  start commit is one transaction per task, and a NOTIFY would make Postgres
  commit it one at a time with every other notifying commit.

The terminal moves are pinned by the core tests of the status event.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from typing import Any, Final

import psycopg
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tests.integration.autocommit_support import insert_task

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

STATUS_CHANNEL: Final = 'horsies_task_status'
NOTIFY_WAIT_S: Final = 5.0


@pytest_asyncio.fixture(loop_scope='function')
async def status_listener(db_url: str) -> AsyncGenerator[psycopg.AsyncConnection[Any]]:
    connection = await psycopg.AsyncConnection.connect(
        db_url,
        autocommit=True,
    )
    _ = await connection.execute(f'LISTEN {STATUS_CHANNEL}')
    yield connection
    await connection.close()


async def _events_until_sentinel(
    listener: psycopg.AsyncConnection[Any],
    session: AsyncSession,
) -> list[str]:
    """Every payload on the status channel committed before a sentinel that
    this call commits. Postgres delivers notifications in commit order."""
    sentinel = f'sentinel:{uuid.uuid4()}'
    _ = await session.execute(
        text('SELECT pg_notify(:channel, :payload)'),
        {'channel': STATUS_CHANNEL, 'payload': sentinel},
    )
    await session.commit()
    payloads: list[str] = []
    async for notify in listener.notifies(timeout=NOTIFY_WAIT_S):
        if notify.payload == sentinel:
            return payloads
        payloads.append(notify.payload)
    raise AssertionError(f'sentinel NOTIFY not seen; payloads {payloads}')


async def _set_status(session: AsyncSession, task_id: str, status: str) -> None:
    """Set a live status; only a CLAIMED row keeps its lease
    (CHECK horsies_tasks_status_live)."""
    _ = await session.execute(
        text(
            'UPDATE horsies_tasks SET status = CAST(:status AS varchar), '
            "claim_expires_at = CASE WHEN CAST(:status AS varchar) = 'CLAIMED' "
            "THEN COALESCE(claim_expires_at, NOW() + INTERVAL '1 hour') END "
            'WHERE id = CAST(:id AS uuid)'
        ),
        {'status': status, 'id': task_id},
    )
    await session.commit()


async def test_a_start_sends_no_status_event(
    session: AsyncSession,
    status_listener: psycopg.AsyncConnection[Any],
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    task_id, _ = await insert_task(session, status='CLAIMED')
    assert await _events_until_sentinel(status_listener, session) == [task_id]

    await _set_status(session, task_id, 'RUNNING')

    assert await _events_until_sentinel(status_listener, session) == []


async def test_a_status_change_other_than_the_start_sends_the_event(
    session: AsyncSession,
    status_listener: psycopg.AsyncConnection[Any],
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A task that goes back to PENDING, as a retry does."""
    task_id, _ = await insert_task(session, status='RUNNING')
    assert await _events_until_sentinel(status_listener, session) == [task_id]

    await _set_status(session, task_id, 'PENDING')

    assert await _events_until_sentinel(status_listener, session) == [task_id]
