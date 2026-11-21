"""Setup shared by the round-trip tests: URLs through a ``RoundTripProxy``,
the steady-state turns of a statement, and seeded task rows owned by
``WORKER_ID``.
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import Final, Literal

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession

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


def proxied_url(database_url: str, proxy: RoundTripProxy) -> str:
    """``database_url`` with its host and port replaced by the proxy's."""
    return (
        make_url(database_url)
        .set(host='127.0.0.1', port=proxy.port)
        .render_as_string(hide_password=False)
    )


def upstream_of(database_url: str) -> tuple[str, int]:
    url = make_url(database_url)
    return url.host or '127.0.0.1', url.port or 5432


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
