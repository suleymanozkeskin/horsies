"""The stale and expired task lists of the broker: their rows, the threshold rule and the row bound.

``get_stale_tasks`` lists RUNNING tasks whose runner stopped beating;
``get_expired_tasks`` lists PENDING tasks whose deadline passed. Each list
holds at most ``MONITORING_LIST_ROW_BOUND`` rows and logs a warning when it
cuts; the stale threshold is whole minutes from 0 to one year.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Final, LiteralString

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import (
    MONITORING_LIST_ROW_BOUND,
    STALE_THRESHOLD_MAX_MINUTES,
    PostgresBroker,
)
from horsies.core.brokers.result_types import BrokerInvalidInput
from horsies.core.history.heartbeats.partitioning import (
    CreateHourlyHeartbeatLeaf,
    create_hourly_heartbeat_leaf,
    hourly_leaf_ref,
)
from horsies.core.history.outcomes import LeafAlreadyConformant, LeafCreated
from horsies.core.history.partitions.catalog import database_now
from horsies.core.types.result import Err, Ok

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

_STALE_KEYS: Final = [
    'id',
    'worker_hostname',
    'worker_pid',
    'worker_process_name',
    'last_heartbeat',
    'started_at',
    'task_name',
]
_EXPIRED_KEYS: Final = [
    'id',
    'task_name',
    'queue_name',
    'priority',
    'sent_at',
    'enqueued_at',
    'good_until',
    'expired_for',
]

# :count RUNNING tasks named :task_name, started 50 minutes ago, each with one
# runner beat :beat_minutes ago (1 ms apart, so the list order is fixed).
_SEED_RUNNING: Final[LiteralString] = """
    WITH seeded AS (
        INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, task_options, execution,
            retention_class_key, sent_at, created_at, command_fingerprint_version,
            command_fingerprint, idempotency_key_digest, status, enqueued_at, claimed_at,
            claimed_by_worker_id, claim_expires_at, started_at, finalizing_at,
            finalizing_by_worker_id, worker_pid, worker_hostname, worker_process_name,
            retry_count, max_retries, next_retry_at, good_until, is_workflow_task, updated_at,
            retain_rerun_input, rerun_of_task_id, rerun_root_task_id
        )
        SELECT horsies_uuidv7(), CAST(:task_name AS text), 'default', 50, '{}', NULL, 'SHARED_ASYNC',
               'standard_30d', now() - interval '1 hour', now() - interval '1 hour', 1,
               sha256(convert_to(n::text || CAST(:task_name AS text), 'UTF8')), NULL, 'RUNNING',
               now() - interval '1 hour', now() - interval '50 minutes', 'worker-a', NULL,
               now() - interval '50 minutes', NULL, NULL, 4242, 'host-a', 'child-1', 0, 0,
               NULL, NULL, FALSE, now() - interval '1 hour', TRUE, NULL, NULL
        FROM generate_series(1, CAST(:count AS integer)) AS n
        RETURNING id
    )
    INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
    SELECT seeded.id, 'worker-a', 'runner',
           now() - make_interval(mins => CAST(:beat_minutes AS integer))
               - make_interval(secs => (row_number() OVER (ORDER BY seeded.id)) / 1000.0),
           'host-a', 4242
    FROM seeded
"""

# :count PENDING tasks whose deadline passed 1 minute ago (1 ms apart).
_SEED_EXPIRED: Final[LiteralString] = """
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution,
        retention_class_key, sent_at, created_at, command_fingerprint_version,
        command_fingerprint, idempotency_key_digest, status, enqueued_at, claimed_at,
        claimed_by_worker_id, claim_expires_at, started_at, finalizing_at,
        finalizing_by_worker_id, worker_pid, worker_hostname, worker_process_name,
        retry_count, max_retries, next_retry_at, good_until, is_workflow_task, updated_at,
        retain_rerun_input, rerun_of_task_id, rerun_root_task_id
    )
    SELECT horsies_uuidv7(), 'lists.expired', 'default', 50, '{}', NULL, 'SHARED_ASYNC',
           'standard_30d', now() - interval '1 hour', now() - interval '1 hour', 1,
           sha256(convert_to(n::text || 'expired', 'UTF8')), NULL, 'PENDING',
           now() - interval '1 hour', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
           0, 0, NULL, now() - interval '1 minute' - make_interval(secs => n / 1000.0),
           FALSE, now() - interval '1 hour', TRUE, NULL, NULL
    FROM generate_series(1, CAST(:count AS integer)) AS n
"""


@pytest_asyncio.fixture
async def clean_tasks(
    session: AsyncSession,
    broker: PostgresBroker,  # noqa: ARG001 - the schema and the leaves exist
) -> AsyncGenerator[None, None]:
    """No task and no beat; a heartbeat leaf for the previous hour, so a beat 30 minutes back has a leaf."""
    _ = await session.execute(text('TRUNCATE horsies_tasks, horsies_heartbeats CASCADE'))
    connection = await session.connection()
    now = await database_now(connection)
    previous_hour = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    match await create_hourly_heartbeat_leaf(
        connection, CreateHourlyHeartbeatLeaf(leaf=hourly_leaf_ref(previous_hour)),
    ):
        case LeafCreated() | LeafAlreadyConformant():
            pass
        case refused:
            raise AssertionError(f'the previous hour heartbeat leaf: {refused!r}')
    await session.commit()
    yield


async def _seed(session: AsyncSession, statement: LiteralString, **parameters: object) -> None:
    _ = await session.execute(text(statement), parameters)
    await session.commit()


@pytest.mark.usefixtures('clean_tasks')
class TestStaleTasks:
    async def test_a_runner_that_stopped_beating_is_listed_and_a_beating_one_is_not(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await _seed(session, _SEED_RUNNING, task_name='lists.stale', count=1, beat_minutes=30)
        await _seed(session, _SEED_RUNNING, task_name='lists.beating', count=1, beat_minutes=1)

        result = await broker.get_stale_tasks(stale_threshold_minutes=10)

        match result:
            case Ok(rows):
                assert [list(row) for row in rows] == [_STALE_KEYS]
                assert [row['task_name'] for row in rows] == ['lists.stale']
                assert rows[0]['worker_hostname'] == 'host-a'
            case Err(error):
                raise AssertionError(f'the stale list failed: {error!r}')

    @pytest.mark.parametrize('threshold', [-1, STALE_THRESHOLD_MAX_MINUTES + 1])
    async def test_a_threshold_outside_zero_to_one_year_is_refused(
        self, broker: PostgresBroker, threshold: int
    ) -> None:
        result = await broker.get_stale_tasks(stale_threshold_minutes=threshold)

        match result:
            case Err(BrokerInvalidInput(message=message)):
                assert 'stale_threshold_minutes' in message
                assert str(threshold) in message
            case _:
                raise AssertionError(f'threshold {threshold} answered {result!r}')

    @pytest.mark.parametrize('threshold', [0, STALE_THRESHOLD_MAX_MINUTES])
    async def test_the_threshold_bounds_are_accepted(
        self, broker: PostgresBroker, threshold: int
    ) -> None:
        result = await broker.get_stale_tasks(stale_threshold_minutes=threshold)

        assert result == Ok([])

    async def test_a_list_past_the_bound_is_cut_and_the_cut_is_logged(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        await _seed(
            session,
            _SEED_RUNNING,
            task_name='lists.stale',
            count=MONITORING_LIST_ROW_BOUND + 1,
            beat_minutes=30,
        )

        with caplog.at_level(logging.WARNING, logger='horsies.broker'):
            result = await broker.get_stale_tasks(stale_threshold_minutes=10)

        match result:
            case Ok(rows):
                assert len(rows) == MONITORING_LIST_ROW_BOUND
            case Err(error):
                raise AssertionError(f'the stale list failed: {error!r}')
        assert [record.getMessage() for record in caplog.records] == [
            f'get_stale_tasks: the list is cut at {MONITORING_LIST_ROW_BOUND} rows; '
            'more exist and are not returned',
        ]

    async def test_a_list_at_the_bound_is_complete_and_nothing_is_logged(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        await _seed(
            session,
            _SEED_RUNNING,
            task_name='lists.stale',
            count=MONITORING_LIST_ROW_BOUND,
            beat_minutes=30,
        )

        with caplog.at_level(logging.WARNING, logger='horsies.broker'):
            result = await broker.get_stale_tasks(stale_threshold_minutes=10)

        match result:
            case Ok(rows):
                assert len(rows) == MONITORING_LIST_ROW_BOUND
            case Err(error):
                raise AssertionError(f'the stale list failed: {error!r}')
        assert caplog.records == []


@pytest.mark.usefixtures('clean_tasks')
class TestExpiredTasks:
    async def test_a_pending_task_past_its_deadline_is_listed(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await _seed(session, _SEED_EXPIRED, count=2)

        result = await broker.get_expired_tasks()

        match result:
            case Ok(rows):
                assert [list(row) for row in rows] == [_EXPIRED_KEYS, _EXPIRED_KEYS]
                assert rows[0]['good_until'] < rows[1]['good_until']
            case Err(error):
                raise AssertionError(f'the expired list failed: {error!r}')

    async def test_a_list_past_the_bound_is_cut_and_the_cut_is_logged(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        await _seed(session, _SEED_EXPIRED, count=MONITORING_LIST_ROW_BOUND + 1)

        with caplog.at_level(logging.WARNING, logger='horsies.broker'):
            result = await broker.get_expired_tasks()

        match result:
            case Ok(rows):
                assert len(rows) == MONITORING_LIST_ROW_BOUND
            case Err(error):
                raise AssertionError(f'the expired list failed: {error!r}')
        assert [record.getMessage() for record in caplog.records] == [
            f'get_expired_tasks: the list is cut at {MONITORING_LIST_ROW_BOUND} rows; '
            'more exist and are not returned',
        ]
