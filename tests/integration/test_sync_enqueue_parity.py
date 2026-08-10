"""The sync enqueue wrapper reaches the database with its full surface.

The wrapper forwards a hand-listed keyword set to ``enqueue_async`` on a
background loop. Mock-broker unit tests accept any keyword and async
integration never calls the wrapper, so a keyword missing from it fails
only at a real sync call site — these tests ARE that call site. On a
wrapper missing the keyword, each call raises ``TypeError`` here rather
than surfacing as a worker that never looks ready.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.ddl.classes import DEFAULT_RETENTION_CLASS_KEY
from horsies.core.history.ddl.tables import FOREVER_CLASS_KEY
from horsies.core.types.result import is_err

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _row_class(broker: PostgresBroker, task_id: str) -> str | None:
    async with broker.async_engine.connect() as connection:
        return (
            await connection.execute(
                text(
                    'SELECT retention_class_key FROM horsies_tasks '
                    'WHERE task_id = CAST(:task_id AS uuid)'
                ),
                {'task_id': task_id},
            )
        ).scalar_one()


async def _sync_enqueue(
    broker: PostgresBroker, **keywords: object
) -> tuple[str, object]:
    """Run the SYNC wrapper off-loop, as a real sync caller would."""
    task_id = str(uuid.uuid4())
    outcome = await asyncio.to_thread(
        broker.enqueue,
        'parity.task',
        task_id=task_id,
        enqueue_sha='c' * 64,
        args_json='[]',
        kwargs_json='{}',
        **keywords,
    )
    assert not is_err(outcome), outcome
    return task_id, outcome


async def test_sync_enqueue_carries_the_retention_class(
    broker: PostgresBroker,
) -> None:
    task_id, _ = await _sync_enqueue(
        broker, retention_class_key=DEFAULT_RETENTION_CLASS_KEY
    )
    assert await _row_class(broker, task_id) == DEFAULT_RETENTION_CLASS_KEY


async def test_sync_enqueue_explicit_none_lands_in_forever(
    broker: PostgresBroker,
) -> None:
    task_id, _ = await _sync_enqueue(broker, retention_class_key=None)
    assert await _row_class(broker, task_id) == FOREVER_CLASS_KEY


async def test_sync_enqueue_carries_retain_rerun_input(
    broker: PostgresBroker,
) -> None:
    task_id, _ = await _sync_enqueue(broker, retain_rerun_input=True)
    async with broker.async_engine.connect() as connection:
        retained = (
            await connection.execute(
                text(
                    'SELECT retain_rerun_input FROM horsies_tasks '
                    'WHERE task_id = CAST(:task_id AS uuid)'
                ),
                {'task_id': task_id},
            )
        ).scalar_one()
    assert retained is True
