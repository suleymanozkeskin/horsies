"""Setup shared by the round-trip and autocommit outcome tests.

A worker built as the CLI builds it (``PostgresConfig.worker_process_config``)
whose engine connects through a ``RoundTripProxy``, and seeded task rows that
the worker owns. ``open_proxied_reference_worker`` builds the ``Worker`` of
the reference copy of the Python worker parent the same way, for the
parent-rule tests on that copy.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import functools
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Final, Literal
from unittest.mock import MagicMock

from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec import JsonValue, encode_task_result
from horsies.core.codec.json_io import dumps_json
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.child_sizing import parent_pool_connections
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.types.result import Err, Ok
from horsies.core.utils.db import PooledStatements
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import Worker
from tests.dualrun.reference.worker_parent.worker.worker import (
    Worker as ReferenceWorker,
)
from tests.integration.conftest import compute_test_command_fingerprint
from tests.integration.round_trip_proxy import RoundTripProxy, Turn

QUEUE: Final = 'round_trip_queue'
TASK_NAME: Final = 'round_trip_task'
WORKER_ID: Final = 'round-trip-worker'
CLAIM_LEASE: Final = timedelta(hours=1)
"""The lease of a seeded CLAIMED row: live for the whole test."""

WARM_RUNS: Final = 7
"""psycopg prepares a statement on its sixth use on a connection, with one
more round trip. From the seventh use a statement is in its steady state."""


@functools.cache
def decode_app() -> Horsies:
    """An app that registers ``TASK_NAME``: finalize resolves the ok type of
    a result through it. It never connects."""
    app = Horsies(
        AppConfig(
            broker=PostgresConfig(
                database_url=SecretStr('postgresql://u:p@localhost/db'),
            ),
        ),
    )

    @app.task(task_name=TASK_NAME)
    def round_trip_task() -> TaskResult[JsonValue, TaskError]:
        return TaskResult(ok=None)

    _ = round_trip_task
    return app


def ok_result_json() -> str:
    """The wire result of a task that returned ``TaskResult(ok=None)``."""
    match dumps_json(encode_task_result(TaskResult(ok=None), Any)):
        case Ok(value):
            return value
        case Err(error):
            raise AssertionError(error)


def proxied_url(database_url: str, proxy: RoundTripProxy) -> str:
    """``database_url`` with its host and port replaced by the proxy's."""
    return (
        make_url(database_url)
        .set(host='127.0.0.1', port=proxy.port)
        .render_as_string(hide_password=False)
    )


def psycopg_url(database_url: str) -> str:
    """The libpq URL of the test database: its ``postgresql://`` URL is one
    already."""
    return database_url


def upstream_of(database_url: str) -> tuple[str, int]:
    url = make_url(database_url)
    return url.host or '127.0.0.1', url.port or 5432


def use_pooled_runners(worker: Worker | ReferenceWorker) -> None:
    """Give ``worker`` one checkout per statement for its claim pass, in
    place of its held connection. For a test that calls the worker's steps
    and never stops the worker: no connection stays held after the test.
    (The batched success write holds none: it writes through the core
    handle.)"""
    worker._claim_runner = PooledStatements(worker.sf)  # pyright: ignore[reportPrivateUsage]
    worker._held_connections = ()  # pyright: ignore[reportPrivateUsage]


@dataclass(frozen=True, slots=True)
class ProxiedWorker:
    """A worker on a broker of its own. The caller closes it (``close``)."""

    worker: Worker
    broker: PostgresBroker

    async def close(self) -> None:
        """Give the worker's held connections back, then close the broker."""
        await self.worker._release_held_connections()  # pyright: ignore[reportPrivateUsage]
        _ = await self.broker.close_async()


@dataclass(frozen=True, slots=True)
class ProxiedReferenceWorker:
    """A ``Worker`` of the reference copy on a broker of its own. The caller
    closes it (``close``)."""

    worker: ReferenceWorker
    broker: PostgresBroker

    async def close(self) -> None:
        """Give the worker's held connections back, then close the broker."""
        await self.worker._release_held_connections()  # pyright: ignore[reportPrivateUsage]
        _ = await self.broker.close_async()


@dataclass(frozen=True, slots=True)
class _WorkerParts:
    """What a worker of ``open_proxied_worker`` is built from."""

    broker: PostgresBroker
    session_factory: Any
    cfg: WorkerConfig


async def _worker_parts(database_url: str, *, pre_ping: bool) -> _WorkerParts:
    """The broker with the engine that the CLI builds for ``database_url``,
    its session factory, and a worker config of one isolated child."""
    config = PostgresConfig(
        database_url=SecretStr(database_url),
        worker_pool_pre_ping=pre_ping,
    ).worker_process_config(parent_pool_connections(1))
    broker = PostgresBroker(config, assume_initialized=True)
    match await broker.select_resources():
        case Ok(resources):
            pass
        case Err(error):
            raise AssertionError(error)
    cfg = WorkerConfig(
        dsn=database_url,
        psycopg_dsn=psycopg_url(database_url),
        queues=[QUEUE],
        processes=1,
        recovery_config=RecoveryConfig(),
    )
    return _WorkerParts(broker=broker, session_factory=resources.session_factory, cfg=cfg)


async def open_proxied_worker(
    database_url: str,
    *,
    pre_ping: bool = False,
) -> ProxiedWorker:
    """A worker whose engine is the one the CLI builds for ``database_url``
    and a profile of one isolated child, with ``worker_pool_pre_ping=pre_ping``.
    No listener, no schema check."""
    parts = await _worker_parts(database_url, pre_ping=pre_ping)
    worker = Worker(
        session_factory=parts.session_factory,
        listener=MagicMock(),
        cfg=parts.cfg,
        broker=parts.broker,
    )
    worker._app = decode_app()
    worker.worker_instance_id = WORKER_ID
    return ProxiedWorker(worker=worker, broker=parts.broker)


async def open_proxied_reference_worker(
    database_url: str,
    *,
    pre_ping: bool = False,
) -> ProxiedReferenceWorker:
    """``open_proxied_worker`` with the ``Worker`` of the reference copy."""
    parts = await _worker_parts(database_url, pre_ping=pre_ping)
    worker = ReferenceWorker(
        session_factory=parts.session_factory,
        listener=MagicMock(),
        cfg=parts.cfg,
        broker=parts.broker,
    )
    worker._app = decode_app()
    worker.worker_instance_id = WORKER_ID
    return ProxiedReferenceWorker(worker=worker, broker=parts.broker)


# The largest threshold that the core's stale CLAIMED requeue takes: no
# claim of a test is that old.
_WARM_UP_REQUEUE_THRESHOLD_MS: Final = 2_147_483_647


async def warm_core_handle(opened: ProxiedWorker | ProxiedReferenceWorker) -> None:
    """Run one core write that changes nothing: a stale CLAIMED requeue
    pass that finds no claim. The worker's core handle has then read its
    client family and holds an open pooled connection, so a drop that the
    test arms next hits the test's own statement, not the family read or
    the connect."""
    match await opened.broker.requeue_stale_claimed(_WARM_UP_REQUEUE_THRESHOLD_MS):
        case Ok(0):
            return
        case other:
            raise AssertionError(f'the warm-up requeue: {other!r}')


async def insert_task(
    session: AsyncSession,
    *,
    status: Literal['PENDING', 'CLAIMED', 'RUNNING'],
    is_workflow_task: bool = False,
) -> tuple[str, datetime]:
    """Insert a task of ``QUEUE`` that ``WORKER_ID`` owns (none when
    PENDING), and commit. Returns its id and claim generation.

    A CLAIMED row holds a live lease of ``CLAIM_LEASE``
    (CHECK ``horsies_tasks_status_live``)."""
    task_id = str(uuid.uuid4())
    claimed_at = datetime.now(timezone.utc)
    sent_at, fingerprint = compute_test_command_fingerprint(
        task_name=TASK_NAME, queue_name=QUEUE,
    )
    owned = status != 'PENDING'
    _ = await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (
                    id, task_name, queue_name, priority, input, status, sent_at,
                    enqueued_at, created_at, updated_at, retry_count, max_retries,
                    started_at, claimed_at, claim_expires_at, claimed_by_worker_id,
                    is_workflow_task, retention_class_key, command_fingerprint_version,
                    command_fingerprint, retain_rerun_input, execution, worker_pid,
                    worker_hostname, worker_process_name
                ) VALUES (
                    :id, :task_name, :queue, 100, '{}', :status, :sent_at, NOW(),
                    NOW(), NOW(), 0, 0, CAST(:started_at AS timestamptz),
                    CAST(:claimed_at AS timestamptz),
                    CAST(:claim_expires_at AS timestamptz), :worker_id,
                    :is_workflow_task, 'standard_30d',
                    CASE WHEN CAST(:is_workflow_task AS boolean) THEN NULL ELSE 1 END,
                    CASE WHEN CAST(:is_workflow_task AS boolean) THEN NULL ELSE :command_fingerprint END,
                    FALSE, 'ISOLATED',
                    CASE WHEN CAST(:status AS varchar) = 'RUNNING' THEN 4242 END,
                    CASE WHEN CAST(:status AS varchar) = 'RUNNING' THEN 'host-1' END,
                    CASE WHEN CAST(:status AS varchar) = 'RUNNING' THEN 'child-1' END
                )
        """),
        {
            'id': task_id,
            'task_name': TASK_NAME,
            'queue': QUEUE,
            'status': status,
            'sent_at': sent_at,
            'started_at': claimed_at if status == 'RUNNING' else None,
            'claimed_at': claimed_at if owned else None,
            'claim_expires_at': claimed_at + CLAIM_LEASE if status == 'CLAIMED' else None,
            'worker_id': WORKER_ID if owned else None,
            'command_fingerprint': fingerprint,
            'is_workflow_task': is_workflow_task,
        },
    )
    await session.commit()
    return task_id, claimed_at


async def recorded_turns[CaseT](
    proxy: RoundTripProxy,
    act: Callable[[CaseT], Awaitable[None]],
    case: CaseT,
) -> tuple[Turn, ...]:
    """The turns that ``act`` sends for ``case``."""
    proxy.start_recording()
    try:
        await act(case)
    finally:
        turns = proxy.stop_recording()
    return turns


async def steady_state_turns[CaseT](
    proxy: RoundTripProxy,
    arrange: Callable[[], Awaitable[CaseT]],
    act: Callable[[CaseT], Awaitable[None]],
) -> tuple[CaseT, tuple[Turn, ...]]:
    """The last case and the turns of ``act`` on its ``WARM_RUNS``-th run,
    each run on a fresh case from ``arrange``. Only ``act`` is recorded."""
    case = await arrange()
    turns = await recorded_turns(proxy, act, case)
    for _ in range(WARM_RUNS - 1):
        case = await arrange()
        turns = await recorded_turns(proxy, act, case)
    return case, turns
