"""Soft-cap mode: a capped queue's running count is checked before each start.

In soft-cap mode the claim counts only RUNNING rows against a queue's
``max_concurrency``, so passes that follow each other claim rows of a capped
queue while the rows that they claimed before are still CLAIMED. Each pass
starts a claimed row of a capped queue only while the queue's RUNNING rows,
plus the rows that this worker started that are not RUNNING yet, stay under
the cap. The other rows stay CLAIMED, held by the worker.

The worker here has one isolated slot and ``prefetch_buffer=2``, so a claim
takes up to 3 rows. ``_dispatch_one`` is replaced by a recorder whose
finalizer future the test ends.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from typing import Final
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import Execution
from horsies.core.utils.db import PooledStatements
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.stored_timeout import NoTimeout, StoredTimeout
from horsies.core.worker.worker import Worker
from tests.broker_resources_support import current_resources, listener_of
from tests.integration.conftest import compute_test_command_fingerprint

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

CAPPED: Final = 'capped'
FREE: Final = 'free'
TASK_NAME: Final = 'soft_cap_start_task'
OTHER_WORKER: Final = 'other-worker'

INSERT_PENDING_SQL: Final = text("""
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, status, sent_at, created_at,
        updated_at, retry_count, max_retries, retention_class_key,
        command_fingerprint_version, command_fingerprint, retain_rerun_input,
        execution, enqueued_at, is_workflow_task
    ) VALUES (
        :id, :task_name, :queue_name, 100, '{}', 'PENDING', :sent_at, NOW(), NOW(),
        0, 0, 'standard_30d', 1, :command_fingerprint, FALSE, 'ISOLATED',
        NOW() - :age * INTERVAL '1 second', FALSE
    )
""")


async def _seed_pending(session: AsyncSession, queue_name: str, age_s: int) -> str:
    """One PENDING row of ``queue_name``; an older row is claimed first."""
    task_id = str(uuid.uuid4())
    sent_at, fingerprint = compute_test_command_fingerprint(
        task_name=TASK_NAME, queue_name=queue_name,
    )
    _ = await session.execute(
        INSERT_PENDING_SQL,
        {
            'id': task_id,
            'task_name': TASK_NAME,
            'queue_name': queue_name,
            'sent_at': sent_at,
            'command_fingerprint': fingerprint,
            'age': age_s,
        },
    )
    await session.commit()
    return task_id


async def _set_running(session: AsyncSession, task_id: str, worker_id: str) -> None:
    _ = await session.execute(
        text("""
            UPDATE horsies_tasks
            SET status = 'RUNNING', claimed_at = NOW(), claim_expires_at = NULL,
                claimed_by_worker_id = :wid, started_at = NOW(), updated_at = NOW()
            WHERE id = :id
        """),
        {'id': task_id, 'wid': worker_id},
    )
    await session.commit()


async def _end_task(session: AsyncSession, task_id: str) -> None:
    """The row leaves the live table, as a terminal write moves it."""
    _ = await session.execute(text('DELETE FROM horsies_tasks WHERE id = :id'), {'id': task_id})
    await session.commit()


async def _statuses(session: AsyncSession) -> dict[str, tuple[str, str | None]]:
    rows = (
        await session.execute(
            text('SELECT id::text, status, claimed_by_worker_id FROM horsies_tasks'),
        )
    ).all()
    return {str(row[0]): (str(row[1]), row[2]) for row in rows}


class _RecordedDispatch:
    """Stands in for ``Worker._dispatch_one``: records each started task and
    returns a finalizer future that the test ends."""

    def __init__(self) -> None:
        self.started: list[str] = []
        self.finalizers: dict[str, asyncio.Future[None]] = {}

    async def __call__(
        self,
        task_id: str,
        task_name: str,
        args_json: str | None,
        kwargs_json: str | None,
        queue_name: str = 'default',
        is_workflow_task: bool = True,
        timeout: StoredTimeout = NoTimeout(),
        *,
        claimed_at: datetime,
        execution: Execution,
        task_options: str | None,
    ) -> asyncio.Future[None]:
        _ = (task_name, args_json, kwargs_json, queue_name, is_workflow_task)
        _ = (timeout, claimed_at, execution, task_options)
        self.started.append(task_id)
        finalizer: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.finalizers[task_id] = finalizer
        return finalizer

    async def end(self, task_id: str) -> None:
        self.finalizers[task_id].set_result(None)
        await asyncio.sleep(0)  # let the done callbacks run


def _soft_cap_worker(
    broker: PostgresBroker,
    cap: int,
) -> tuple[Worker, _RecordedDispatch]:
    cfg = WorkerConfig(
        dsn=broker.config.database_url.get_secret_value(),
        psycopg_dsn=listener_of(broker).database_url,
        queues=[CAPPED, FREE],
        processes=1,
        queue_priorities={CAPPED: 1, FREE: 1},
        queue_max_concurrency={CAPPED: cap},
        prefetch_buffer=2,
        claim_lease_ms=60_000,
    )
    worker = Worker(
        session_factory=current_resources(broker).session_factory,
        listener=MagicMock(),
        cfg=cfg,
    )
    worker._claim_runner = PooledStatements(worker.sf)  # pyright: ignore[reportPrivateUsage]
    worker._claim_slot_count = lambda: cfg.processes  # type: ignore[method-assign]
    dispatch = _RecordedDispatch()
    worker._dispatch_one = dispatch  # type: ignore[method-assign,assignment]
    return worker, dispatch


async def _passes(worker: Worker, count: int) -> None:
    for _ in range(count):
        _ = await worker._claim_and_dispatch_all()  # pyright: ignore[reportPrivateUsage]


async def test_rows_of_a_capped_queue_past_the_cap_are_claimed_and_not_started(
    clean_workflow_tables: None,  # noqa: ARG001
    session: AsyncSession,
    broker: PostgresBroker,
) -> None:
    """Cap 2 with one row RUNNING on another worker: one claimed row starts;
    the rows that later passes claim stay CLAIMED and are not started."""
    running_elsewhere = await _seed_pending(session, CAPPED, age_s=100)
    await _set_running(session, running_elsewhere, OTHER_WORKER)
    seeded = [await _seed_pending(session, CAPPED, age_s=50 - n) for n in range(3)]
    worker, dispatch = _soft_cap_worker(broker, cap=2)

    await _passes(worker, 3)

    assert dispatch.started == [seeded[0]]
    statuses = await _statuses(session)
    for task_id in seeded:
        assert statuses[task_id] == ('CLAIMED', worker.worker_instance_id)


async def test_a_started_row_counts_before_its_child_sets_it_running(
    clean_workflow_tables: None,  # noqa: ARG001
    session: AsyncSession,
    broker: PostgresBroker,
) -> None:
    """Cap 1: the first row starts; while it is still CLAIMED, the next
    claimed row is not started."""
    seeded = [await _seed_pending(session, CAPPED, age_s=50 - n) for n in range(2)]
    worker, dispatch = _soft_cap_worker(broker, cap=1)

    await _passes(worker, 2)

    assert dispatch.started == [seeded[0]]
    statuses = await _statuses(session)
    assert statuses[seeded[1]] == ('CLAIMED', worker.worker_instance_id)


async def test_a_held_row_starts_when_the_queue_has_room_again(
    clean_workflow_tables: None,  # noqa: ARG001
    session: AsyncSession,
    broker: PostgresBroker,
) -> None:
    """Cap 1: a held row does not start while the started row runs, and
    starts on the first pass after that row ended."""
    seeded = [await _seed_pending(session, CAPPED, age_s=50 - n) for n in range(2)]
    worker, dispatch = _soft_cap_worker(broker, cap=1)
    await _passes(worker, 2)
    assert dispatch.started == [seeded[0]]

    await _set_running(session, seeded[0], worker.worker_instance_id)
    await _passes(worker, 1)
    assert dispatch.started == [seeded[0]]

    await _end_task(session, seeded[0])
    await dispatch.end(seeded[0])
    await _passes(worker, 1)
    assert dispatch.started == [seeded[0], seeded[1]]


async def test_rows_of_an_uncapped_queue_start_while_a_capped_queue_waits(
    clean_workflow_tables: None,  # noqa: ARG001
    session: AsyncSession,
    broker: PostgresBroker,
) -> None:
    capped = [await _seed_pending(session, CAPPED, age_s=50 - n) for n in range(2)]
    free = await _seed_pending(session, FREE, age_s=10)
    worker, dispatch = _soft_cap_worker(broker, cap=1)

    await _passes(worker, 2)

    assert set(dispatch.started) == {capped[0], free}


async def test_stop_returns_the_held_rows_to_their_queue(
    clean_workflow_tables: None,  # noqa: ARG001
    session: AsyncSession,
    broker: PostgresBroker,
) -> None:
    seeded = [await _seed_pending(session, CAPPED, age_s=50 - n) for n in range(2)]
    worker, dispatch = _soft_cap_worker(broker, cap=1)
    await _passes(worker, 2)
    assert dispatch.started == [seeded[0]]

    await worker._return_held_rows()  # pyright: ignore[reportPrivateUsage]

    statuses = await _statuses(session)
    assert statuses[seeded[1]] == ('PENDING', None)
    assert statuses[seeded[0]] == ('CLAIMED', worker.worker_instance_id)
