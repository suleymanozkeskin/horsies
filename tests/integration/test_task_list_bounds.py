"""The row bound of ``get_stale_tasks`` and ``get_expired_tasks`` on a database.

The bound is lowered to 2 rows (a read of 3) so that three seeded rows pass
it. Every seeded row ties on the first sort key, so the rows that come back
are the first by id.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers import postgres as postgres_module
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.types.result import is_ok
from tests.integration.autocommit_support import insert_task
from tests.integration.conftest import compute_test_command_fingerprint

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

_SEEDED_ROWS = 3
_LOWERED_BOUND = 2


@pytest.fixture
def lowered_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    """Lower the bound to ``_LOWERED_BOUND`` rows and the read to one more."""
    monkeypatch.setattr(postgres_module, 'TASK_LIST_ROW_BOUND', _LOWERED_BOUND)
    monkeypatch.setattr(postgres_module, '_TASK_LIST_READ_LIMIT', _LOWERED_BOUND + 1)


async def _insert_expired_pending(session: AsyncSession, good_until: datetime) -> str:
    """Insert a PENDING task whose ``good_until`` has passed, and commit."""
    task_id = str(uuid.uuid4())
    sent_at, fingerprint = compute_test_command_fingerprint(
        task_name='expired_task', good_until=good_until,
    )
    _ = await session.execute(
        text("""
            INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, input, status, sent_at,
                enqueued_at, good_until, created_at, updated_at, retry_count,
                max_retries, is_workflow_task, retention_class_key,
                command_fingerprint_version, command_fingerprint,
                retain_rerun_input, execution
            ) VALUES (
                :id, 'expired_task', 'default', 100, '{}', 'PENDING', :sent_at,
                NOW(), :good_until, NOW(), NOW(), 0, 0, FALSE, 'standard_30d',
                1, :fingerprint, FALSE, 'ISOLATED'
            )
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'good_until': good_until,
            'fingerprint': fingerprint,
        },
    )
    await session.commit()
    return task_id


async def test_expired_list_stops_at_the_bound_in_deadline_then_id_order(
    session: AsyncSession,
    broker: PostgresBroker,
    clean_workflow_tables: None,
    lowered_bound: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    good_until = datetime.now(timezone.utc) - timedelta(minutes=5)
    seeded = [await _insert_expired_pending(session, good_until) for _ in range(_SEEDED_ROWS)]

    with caplog.at_level('WARNING'):
        result = await broker.get_expired_tasks()

    assert is_ok(result), result
    assert [str(row['id']) for row in result.ok_value] == sorted(seeded)[:_LOWERED_BOUND]
    assert 'get_expired_tasks returned the first 2 rows; more rows match' in caplog.text


async def test_stale_list_stops_at_the_bound_in_heartbeat_then_id_order(
    session: AsyncSession,
    broker: PostgresBroker,
    clean_workflow_tables: None,
    lowered_bound: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """RUNNING rows with no runner heartbeat tie on the first key."""
    seeded = [
        (await insert_task(session, status='RUNNING'))[0] for _ in range(_SEEDED_ROWS)
    ]

    with caplog.at_level('WARNING'):
        result = await broker.get_stale_tasks(stale_threshold_minutes=0)

    assert is_ok(result), result
    assert [str(row['id']) for row in result.ok_value] == sorted(seeded)[:_LOWERED_BOUND]
    assert 'get_stale_tasks returned the first 2 rows; more rows match' in caplog.text
