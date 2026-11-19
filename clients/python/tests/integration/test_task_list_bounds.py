"""The row bound of ``get_stale_tasks`` and ``get_expired_tasks`` on a database.

The bound is the one of ``horsies_list_stale_tasks`` and
``horsies_list_expired_tasks`` (``TASK_LIST_ROW_BOUND``), so each test seeds
one row past it. Every seeded row ties on the first sort key, so the rows
that come back are the first by id.
"""

from __future__ import annotations

from typing import Final, LiteralString

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies._catalog.broker_bounds import TASK_LIST_ROW_BOUND
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.types.result import is_ok

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

_SEEDED_ROWS: Final = TASK_LIST_ROW_BOUND + 1

# :count PENDING rows whose deadline passed 5 minutes ago (one deadline), or
# RUNNING rows started 50 minutes ago with no runner beat.
_SEED_TASKS: Final[LiteralString] = """
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution,
        retention_class_key, sent_at, created_at, command_fingerprint_version,
        command_fingerprint, idempotency_key_digest, status, enqueued_at, claimed_at,
        claimed_by_worker_id, claim_expires_at, started_at, finalizing_at,
        finalizing_by_worker_id, worker_pid, worker_hostname, worker_process_name,
        retry_count, max_retries, next_retry_at, good_until, is_workflow_task, updated_at,
        retain_rerun_input, rerun_of_task_id, rerun_root_task_id
    )
    SELECT horsies_uuidv7(), 'bound_task', 'default', 100, '{}', NULL, 'ISOLATED',
           'standard_30d', now() - interval '1 hour', now() - interval '1 hour', 1,
           sha256(convert_to(n::text, 'UTF8')), NULL, CAST(:status AS text),
           now() - interval '1 hour',
           CASE WHEN :status = 'RUNNING' THEN now() - interval '50 minutes' END,
           CASE WHEN :status = 'RUNNING' THEN 'worker-a' END, NULL,
           CASE WHEN :status = 'RUNNING' THEN now() - interval '50 minutes' END, NULL, NULL,
           CASE WHEN :status = 'RUNNING' THEN 4242 END,
           CASE WHEN :status = 'RUNNING' THEN 'host-a' END,
           CASE WHEN :status = 'RUNNING' THEN 'child-1' END, 0, 0, NULL,
           CASE WHEN :status = 'PENDING' THEN date_trunc('second', now()) - interval '5 minutes' END,
           FALSE, now() - interval '1 hour', FALSE, NULL, NULL
    FROM generate_series(1, CAST(:count AS integer)) AS n
    RETURNING id
"""


async def _seed(session: AsyncSession, status: str) -> list[str]:
    rows = (
        await session.execute(text(_SEED_TASKS), {'status': status, 'count': _SEEDED_ROWS})
    ).scalars().all()
    await session.commit()
    return [str(row) for row in rows]


async def test_expired_list_stops_at_the_bound_in_deadline_then_id_order(
    session: AsyncSession,
    broker: PostgresBroker,
    clean_workflow_tables: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    seeded = await _seed(session, 'PENDING')

    with caplog.at_level('WARNING'):
        result = await broker.get_expired_tasks()

    assert is_ok(result), result
    assert [str(row['id']) for row in result.ok_value] == sorted(seeded)[:TASK_LIST_ROW_BOUND]
    assert (
        f'get_expired_tasks returned the first {TASK_LIST_ROW_BOUND} rows; more rows match'
        in caplog.text
    )


async def test_stale_list_stops_at_the_bound_in_heartbeat_then_id_order(
    session: AsyncSession,
    broker: PostgresBroker,
    clean_workflow_tables: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """RUNNING rows with no runner heartbeat tie on the first key."""
    seeded = await _seed(session, 'RUNNING')

    with caplog.at_level('WARNING'):
        result = await broker.get_stale_tasks(stale_threshold_minutes=0)

    assert is_ok(result), result
    assert [str(row['id']) for row in result.ok_value] == sorted(seeded)[:TASK_LIST_ROW_BOUND]
    assert (
        f'get_stale_tasks returned the first {TASK_LIST_ROW_BOUND} rows; more rows match'
        in caplog.text
    )
