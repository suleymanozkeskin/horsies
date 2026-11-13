"""The monitoring channel ``horsies_task_status`` of the task rows.

The web app listens to it and re-reads the tasks on each event. These tests
pin which writes send it:

- an insert and a status change send it, except the start (RUNNING): a task's
  start commit is one transaction per task, and a NOTIFY would make Postgres
  commit it one at a time with every other notifying commit;
- a terminal move sends it, for each task it moves: the move deletes the live
  row, so no status change of the row is left to send it.
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

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.lifecycle.commands import CancelOrphanedTasks, CompleteTaskFused
from horsies.core.lifecycle.fences import OwnedClaim
from horsies.core.lifecycle.outcomes import Applied
from horsies.core.lifecycle.persistence import apply_batch_async, apply_in_autocommit
from tests.integration.autocommit_support import (
    QUEUE,
    WORKER_ID,
    ProxiedWorker,
    insert_task,
    ok_result_json,
    open_proxied_worker,
    psycopg_url,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

STATUS_CHANNEL: Final = 'horsies_task_status'
NOTIFY_WAIT_S: Final = 5.0


@pytest_asyncio.fixture(loop_scope='function')
async def direct(
    db_url: str,
    broker: PostgresBroker,  # noqa: ARG001 - the schema and history leaves
) -> AsyncGenerator[ProxiedWorker]:
    """A worker on the database URL itself, as the CLI builds it."""
    opened = await open_proxied_worker(db_url)
    yield opened
    await opened.close()


@pytest_asyncio.fixture(loop_scope='function')
async def status_listener(db_url: str) -> AsyncGenerator[psycopg.AsyncConnection[Any]]:
    connection = await psycopg.AsyncConnection.connect(
        psycopg_url(db_url),
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
    _ = await session.execute(
        text('UPDATE horsies_tasks SET status = :status WHERE id = CAST(:id AS uuid)'),
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


async def test_a_completion_sends_the_status_event(
    direct: ProxiedWorker,
    session: AsyncSession,
    status_listener: psycopg.AsyncConnection[Any],
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    task_id, claimed_at = await insert_task(session, status='RUNNING')
    assert await _events_until_sentinel(status_listener, session) == [task_id]

    outcome = await apply_in_autocommit(
        direct.worker.sf,
        CompleteTaskFused(
            task_id=task_id,
            fence=OwnedClaim(worker_id=WORKER_ID, claimed_at=claimed_at),
            result_json=ok_result_json(),
            notify_channel=f'task_queue_{QUEUE}',
            notify_payload=f'capacity:{task_id}',
        ),
    )

    assert isinstance(outcome, Applied)
    assert await _events_until_sentinel(status_listener, session) == [task_id]


async def test_a_batch_move_sends_one_status_event_per_task(
    session: AsyncSession,
    status_listener: psycopg.AsyncConnection[Any],
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The orphan sweep cancels workflow tasks that have no node: one batch
    move for all of them."""
    orphans = [
        (await insert_task(session, status='CLAIMED', is_workflow_task=True))[0]
        for _ in range(3)
    ]
    assert sorted(await _events_until_sentinel(status_listener, session)) == sorted(orphans)

    outcomes = await apply_batch_async(
        await session.connection(),
        CancelOrphanedTasks(batch_size=10),
    )
    await session.commit()

    assert {outcome.task_id for outcome in outcomes} == set(orphans)
    assert sorted(await _events_until_sentinel(status_listener, session)) == sorted(orphans)
