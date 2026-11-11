"""Setup shared by the round-trip and autocommit outcome tests.

A worker built as the CLI builds it (``PostgresConfig.worker_process_config``)
whose engine connects through a ``RoundTripProxy``, and seeded task rows that
the worker owns.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import functools
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
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
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.types.result import Err, Ok
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import Worker
from tests.integration.conftest import compute_test_enqueue_sha
from tests.integration.round_trip_proxy import RoundTripProxy, Turn

QUEUE: Final = 'round_trip_queue'
TASK_NAME: Final = 'round_trip_task'
WORKER_ID: Final = 'round-trip-worker'

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
                database_url=SecretStr('postgresql+psycopg://u:p@localhost/db'),
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
    """The libpq form of a ``postgresql+psycopg://`` URL."""
    return database_url.replace('postgresql+psycopg://', 'postgresql://', 1)


def upstream_of(database_url: str) -> tuple[str, int]:
    url = make_url(database_url)
    return url.host or '127.0.0.1', url.port or 5432


@dataclass(frozen=True, slots=True)
class ProxiedWorker:
    """A worker on a broker of its own. The caller closes the broker."""

    worker: Worker
    broker: PostgresBroker


async def open_proxied_worker(
    database_url: str,
    *,
    pre_ping: bool = False,
) -> ProxiedWorker:
    """A worker whose engine is the one the CLI builds for ``database_url``,
    with ``worker_pool_pre_ping=pre_ping``. No listener, no schema check."""
    config = PostgresConfig(
        database_url=SecretStr(database_url),
        worker_pool_pre_ping=pre_ping,
    ).worker_process_config()
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
    worker = Worker(
        session_factory=resources.session_factory,
        listener=MagicMock(),
        cfg=cfg,
        broker=broker,
    )
    worker._app = decode_app()
    worker.worker_instance_id = WORKER_ID
    return ProxiedWorker(worker=worker, broker=broker)


async def insert_task(
    session: AsyncSession,
    *,
    status: Literal['PENDING', 'CLAIMED', 'RUNNING'],
    is_workflow_task: bool = False,
) -> tuple[str, datetime]:
    """Insert a task of ``QUEUE`` that ``WORKER_ID`` owns (none when
    PENDING), and commit. Returns its id and claim generation."""
    task_id = str(uuid.uuid4())
    claimed_at = datetime.now(timezone.utc)
    sent_at, sha = compute_test_enqueue_sha(task_name=TASK_NAME, queue_name=QUEUE)
    owned = status != 'PENDING'
    _ = await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, enqueued_at, created_at, updated_at,
                 claimed, retry_count, max_retries, started_at,
                 claimed_at, claim_expires_at, claimed_by_worker_id,
                 enqueue_sha, is_workflow_task, retention_class_key,
                 command_fingerprint_version, command_fingerprint,
                 retain_rerun_input, prepared_rerun_input_disposition)
            VALUES
                (:id, :task_name, :queue, 100, '[]', '{}',
                 :status, :sent_at, NOW(), NOW(), NOW(),
                 :claimed, 0, 0, CAST(:started_at AS timestamptz),
                 CAST(:claimed_at AS timestamptz),
                 CAST(:claim_expires_at AS timestamptz), :worker_id,
                 :sha, :is_workflow_task, 'standard_30d',
                 1, sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
                 FALSE, 'DECLINED_BY_POLICY')
        """),
        {
            'id': task_id,
            'task_name': TASK_NAME,
            'queue': QUEUE,
            'status': status,
            'sent_at': sent_at,
            'claimed': status == 'CLAIMED',
            'started_at': claimed_at if status == 'RUNNING' else None,
            'claimed_at': claimed_at if owned else None,
            'claim_expires_at': None,
            'worker_id': WORKER_ID if owned else None,
            'sha': sha,
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
