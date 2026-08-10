"""retention_class_key through the public send surface, end to end.

The adopter contract under test: a send that names no class lands in the
immutable 30-day default class; explicit ``None`` lands in forever; an
unknown class is refused at the send call with the class named, never as
a partition-routing failure later; and a terminalized row relocates into
the history partition of the class its enqueue declared.

The app's broker is the real migrated test broker — the public path runs
unmocked from ``with_options`` through the enqueue INSERT. Terminal
transitions use the shipped completion function against a row carrying a
worker, the same shape the terminalization move suite drives.
"""

from __future__ import annotations

from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.ddl.classes import DEFAULT_RETENTION_CLASS_KEY
from horsies.core.history.ddl.tables import FOREVER_CLASS_KEY
from horsies.core.history.maintenance.coverage import (
    StartupCoverageRefused,
    ensure_startup_coverage,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.task_send_types import TaskSendErrorCode
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.types.result import is_err, is_ok

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope='function'),
]

WORKER = 'worker-retention-enqueue-1'

_TEST_APP: Horsies | None = None


def _test_app(broker: PostgresBroker) -> Horsies:
    """A real app whose broker IS the migrated test broker.

    The DSN is never dialed: ``get_broker`` returns the injected broker,
    so the public send path runs against the real schema.
    """
    global _TEST_APP
    if _TEST_APP is None:
        cfg = AppConfig(
            broker=PostgresConfig(
                database_url='postgresql+psycopg://u:p@localhost/db',
            )
        )
        _TEST_APP = Horsies(cfg)

        @_TEST_APP.task(task_name='retention_enqueue_test')
        def _retention_enqueue_test(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

    _TEST_APP._broker = broker  # pyright: ignore[reportPrivateUsage]
    return _TEST_APP


async def _row_class(broker: PostgresBroker, task_id: str) -> str | None:
    async with broker.async_engine.connect() as connection:
        return (
            await connection.execute(
                text(
                    'SELECT retention_class_key FROM horsies_tasks '
                    'WHERE id = CAST(:task_id AS uuid)'
                ),
                {'task_id': task_id},
            )
        ).scalar_one()


async def _terminalize(
    broker: PostgresBroker, task_id: str
) -> tuple[str, str]:
    """Drive the enqueued row to COMPLETED through the shipped function.

    Returns (history retention_class_key, history partition name) for the
    moved record — the partition read from ``tableoid``, which is the
    routing fact itself rather than an inference from the class column.
    """
    async with broker.async_engine.begin() as connection:
        await connection.execute(
            text(
                "UPDATE horsies_tasks SET status = 'RUNNING', "
                'claimed_by_worker_id = :worker, started_at = NOW() '
                'WHERE id = CAST(:task_id AS uuid)'
            ),
            {'task_id': task_id, 'worker': WORKER},
        )
        outcome = (
            await connection.execute(
                text(
                    'SELECT * FROM horsies_complete_locked_task('
                    'CAST(:task_id AS uuid), :worker, :result)'
                ),
                {
                    'task_id': task_id,
                    'worker': WORKER,
                    'result': '{"ok":true}',
                },
            )
        ).one()
        assert outcome.outcome == 'APPLIED', outcome
    async with broker.async_engine.connect() as connection:
        row = (
            await connection.execute(
                text(
                    'SELECT retention_class_key, '
                    'tableoid::regclass::text AS partition_name '
                    'FROM horsies_task_history '
                    'WHERE task_id = CAST(:task_id AS uuid)'
                ),
                {'task_id': task_id},
            )
        ).one()
    return row.retention_class_key, row.partition_name


@pytest_asyncio.fixture(autouse=True)
async def _coverage(broker: PostgresBroker) -> None:
    """Today's leaves exist before any terminalization: a seeded database
    cannot terminalize without provisioned coverage, and the shipped
    startup ensure returns a TYPED refusal that must be checked, not
    assumed away."""
    defaults = RecoveryConfig()
    async with broker.async_engine.begin() as connection:
        outcome = await ensure_startup_coverage(
            connection,
            history_horizon_days=defaults.history_leaf_horizon_days,
            heartbeat_horizon_hours=defaults.heartbeat_leaf_horizon_hours,
        )
    assert not isinstance(outcome, StartupCoverageRefused), outcome


class TestRetentionClassAtEnqueue:
    async def test_plain_send_lands_in_the_default_class(
        self, broker: PostgresBroker
    ) -> None:
        app = _test_app(broker)
        task: Any = app.tasks['retention_enqueue_test']

        result = await task.send_async(x=1)

        assert is_ok(result)
        row_class = await _row_class(broker, result.ok_value.task_id)
        assert row_class == DEFAULT_RETENTION_CLASS_KEY

    async def test_explicit_none_lands_in_forever(
        self, broker: PostgresBroker
    ) -> None:
        app = _test_app(broker)
        task: Any = app.tasks['retention_enqueue_test']

        result = await task.with_options(
            retention_class_key=None
        ).send_async(x=2)

        assert is_ok(result)
        row_class = await _row_class(broker, result.ok_value.task_id)
        assert row_class == FOREVER_CLASS_KEY

    async def test_unknown_class_is_refused_at_send_with_no_row(
        self, broker: PostgresBroker
    ) -> None:
        app = _test_app(broker)
        task: Any = app.tasks['retention_enqueue_test']
        async with broker.async_engine.connect() as connection:
            before = (
                await connection.execute(
                    text('SELECT count(*) FROM horsies_tasks')
                )
            ).scalar_one()

        result = await task.with_options(
            retention_class_key='no_such_class'
        ).send_async(x=3)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.VALIDATION_FAILED
        assert 'no_such_class' in result.err_value.message
        async with broker.async_engine.connect() as connection:
            after = (
                await connection.execute(
                    text('SELECT count(*) FROM horsies_tasks')
                )
            ).scalar_one()
        assert after == before, 'a refused send must write nothing'

    async def test_forever_row_terminalizes_into_the_forever_partition(
        self, broker: PostgresBroker
    ) -> None:
        """The declared class is the routing fact at terminalization."""
        app = _test_app(broker)
        task: Any = app.tasks['retention_enqueue_test']

        result = await task.with_options(
            retention_class_key=None
        ).send_async(x=4)
        assert is_ok(result)
        task_id = result.ok_value.task_id

        history_class, partition_name = await _terminalize(broker, task_id)

        assert history_class == FOREVER_CLASS_KEY
        assert FOREVER_CLASS_KEY in partition_name

    async def test_default_row_terminalizes_into_its_daily_partition(
        self, broker: PostgresBroker
    ) -> None:
        app = _test_app(broker)
        task: Any = app.tasks['retention_enqueue_test']

        result = await task.send_async(x=5)
        assert is_ok(result)
        task_id = result.ok_value.task_id

        history_class, partition_name = await _terminalize(broker, task_id)

        assert history_class == DEFAULT_RETENTION_CLASS_KEY
        assert DEFAULT_RETENTION_CLASS_KEY in partition_name
