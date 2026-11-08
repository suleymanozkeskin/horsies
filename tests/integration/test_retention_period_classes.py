"""A retention class is its period, at each enqueue path and at broker init.

- A direct send, a delayed send and a workflow node on a queue mapped to 7
  days each stamp `standard_7d`. Two queues mapped to 7 days share it.
- A terminalized row lands in a leaf of that class.
- A broker refuses to start when the key of one of its periods is
  registered with another duration, which only a class declared by name
  before 0.6.1 can do. A matching or absent row does not refuse.

The cron path resolves through the same function and is driven in
`tests/unit/test_scheduler_service.py`.
"""

from __future__ import annotations

# pyright: reportPrivateUsage=false

import uuid
from datetime import timedelta
from typing import Any

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import BrokerErrorCode
from horsies.core.history.ddl.classes import (
    ClassRegistered,
    register_finite_retention_class,
)
from horsies.core.history.maintenance.coverage import (
    CoverageEnsured,
    ensure_partition_coverage,
)
from horsies.core.history.names import LEAF_CATALOG, RETENTION_CLASSES
from horsies.core.history.periods import period_class_key
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import CustomQueueConfig, QueueMode
from horsies.core.models.retention import RetentionConfig
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import TaskNode
from horsies.core.types.result import Err, Ok, is_err, is_ok
from horsies.core.workflows.lifecycle import start_workflow_async
from tests.broker_resources_support import current_resources
from tests.integration.conftest import DB_URL

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope='function'),
]

_PERIOD = timedelta(days=7)
_PERIOD_KEY = period_class_key(_PERIOD.days)
WORKER = 'worker-retention-period-1'


def _mapped_app(broker: PostgresBroker) -> tuple[Horsies, Any, Any]:
    """An app with two queues mapped to one period, a task on each."""
    app = Horsies(
        AppConfig(
            queue_mode=QueueMode.CUSTOM,
            broker=broker.config,
            custom_queues=[
                CustomQueueConfig(name='emails'),
                CustomQueueConfig(name='alerts'),
            ],
            retention=RetentionConfig(
                queue_retention={'emails': _PERIOD, 'alerts': _PERIOD},
            ),
        ),
    )
    app._broker = broker
    broker.app = app
    suffix = uuid.uuid4().hex[:8]

    @app.task(task_name=f'period_email_{suffix}', queue_name='emails')
    def email_task(*, x: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=x)  # pragma: no cover - never executed here

    @app.task(task_name=f'period_alert_{suffix}', queue_name='alerts')
    def alert_task(*, x: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=x)  # pragma: no cover - never executed here

    return app, email_task, alert_task


async def _row_class(engine: AsyncEngine, task_id: str) -> str | None:
    async with engine.connect() as connection:
        return (
            await connection.execute(
                text(
                    'SELECT retention_class_key FROM horsies_tasks '
                    'WHERE id = CAST(:task_id AS uuid)'
                ),
                {'task_id': task_id},
            )
        ).scalar_one()


async def _forget(engine: AsyncEngine, key: str) -> None:
    """Remove the class and every leaf a pass built for it.

    The leaf catalog has a foreign key to the class, so the class row
    goes last.
    """
    async with engine.begin() as connection:
        leaves = (
            await connection.execute(
                text(f'SELECT leaf_name FROM {LEAF_CATALOG} WHERE class_key = :key'),
                {'key': key},
            )
        ).scalars().all()
        for leaf_name in leaves:
            await connection.execute(text(f'DROP TABLE IF EXISTS {leaf_name} CASCADE'))
        await connection.execute(
            text(f'DELETE FROM {LEAF_CATALOG} WHERE class_key = :key'),
            {'key': key},
        )
        await connection.execute(
            text(f'DROP TABLE IF EXISTS horsies_task_history_{key} CASCADE')
        )
        await connection.execute(
            text(f'DELETE FROM {RETENTION_CLASSES} WHERE class_key = :key'),
            {'key': key},
        )


class TestEveryPathStampsThePeriodClass:
    async def test_two_queues_with_one_period_share_its_class(
        self, broker: PostgresBroker,
    ) -> None:
        engine = current_resources(broker).engine
        _app, email_task, alert_task = _mapped_app(broker)

        email = await email_task.send_async(x=1)
        alert = await alert_task.send_async(x=2)

        assert is_ok(email) and is_ok(alert), (email, alert)
        assert await _row_class(engine, email.ok_value.task_id) == _PERIOD_KEY
        assert await _row_class(engine, alert.ok_value.task_id) == _PERIOD_KEY

    async def test_a_delayed_send_stamps_the_period_class(
        self, broker: PostgresBroker,
    ) -> None:
        engine = current_resources(broker).engine
        _app, email_task, _alert_task = _mapped_app(broker)

        delayed = await email_task.schedule_async(60, x=3)

        assert is_ok(delayed), delayed
        assert await _row_class(engine, delayed.ok_value.task_id) == _PERIOD_KEY

    async def test_a_workflow_node_stamps_the_period_class(
        self, clean_workflow_tables: None, broker: PostgresBroker,
    ) -> None:
        engine = current_resources(broker).engine
        app, email_task, _alert_task = _mapped_app(broker)
        node = TaskNode(fn=email_task, kwargs={'x': 4}, node_id='email')
        spec = app.workflow(
            f'period_workflow_{uuid.uuid4().hex[:8]}',
            [node],
            output=node,
            definition_key=f'period-workflow-{uuid.uuid4().hex[:8]}',
        )

        started = await start_workflow_async(spec, broker)

        assert not is_err(started), started
        async with engine.connect() as connection:
            stamped = (
                await connection.execute(
                    text(
                        'SELECT t.retention_class_key '
                        'FROM horsies_workflow_tasks wt '
                        'JOIN horsies_tasks t ON t.id = wt.task_id '
                        "WHERE wt.workflow_id = :wf AND wt.node_id = 'email'"
                    ),
                    {'wf': started.ok_value.workflow_id},
                )
            ).scalar_one()
        assert stamped == _PERIOD_KEY

    async def test_a_finished_row_lands_in_a_leaf_of_the_period_class(
        self, broker: PostgresBroker,
    ) -> None:
        engine = current_resources(broker).engine
        app, email_task, _alert_task = _mapped_app(broker)
        async with engine.begin() as connection:
            coverage = await ensure_partition_coverage(
                connection,
                history_horizon_days=2,
                heartbeat_horizon_hours=6,
                declared_classes=app.config.retention.registrable_classes(),
            )
        assert isinstance(coverage, CoverageEnsured), coverage
        try:
            sent = await email_task.send_async(x=5)
            assert is_ok(sent), sent
            task_id = sent.ok_value.task_id
            async with engine.begin() as connection:
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
                        {'task_id': task_id, 'worker': WORKER, 'result': '{"ok":5}'},
                    )
                ).one()
            assert outcome.outcome == 'APPLIED', outcome
            async with engine.connect() as connection:
                partition = (
                    await connection.execute(
                        text(
                            'SELECT tableoid::regclass::text FROM horsies_task_history '
                            'WHERE task_id = CAST(:task_id AS uuid)'
                        ),
                        {'task_id': task_id},
                    )
                ).scalar_one()
            assert partition.startswith(f'horsies_task_history_{_PERIOD_KEY}_'), (
                partition
            )
        finally:
            await _forget(engine, _PERIOD_KEY)


# A period whose key a pre-0.6.1 declaration held with another duration.
_LEGACY_DAYS = 17
_LEGACY_KEY = period_class_key(_LEGACY_DAYS)
_LEGACY_DURATION = timedelta(days=5)


def _app_mapping(days: int) -> Horsies:
    return Horsies(
        AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=PostgresConfig(database_url=SecretStr(DB_URL)),
            retention=RetentionConfig(
                queue_retention={'default': timedelta(days=days)},
            ),
        )
    )


@pytest.mark.parametrize(
    ('registered', 'run_schema_migrations', 'refused'),
    [
        pytest.param(_LEGACY_DURATION, True, True, id='other-duration'),
        pytest.param(_LEGACY_DURATION, False, True, id='read-only-other-duration'),
        pytest.param(timedelta(days=_LEGACY_DAYS), True, False, id='same-duration'),
        pytest.param(None, True, False, id='not-registered'),
    ],
)
async def test_a_broker_refuses_a_period_key_held_with_another_duration(
    broker: PostgresBroker,
    registered: timedelta | None,
    run_schema_migrations: bool,
    refused: bool,
) -> None:
    """A broker that runs no migrations still sends, so it checks too."""
    engine = current_resources(broker).engine
    await _forget(engine, _LEGACY_KEY)
    match registered:
        case None:
            pass
        case duration:
            async with engine.begin() as connection:
                registration = await register_finite_retention_class(
                    connection, class_key=_LEGACY_KEY, duration=duration,
                )
            assert isinstance(registration, ClassRegistered), registration
    starting = PostgresBroker(
        PostgresConfig(database_url=SecretStr(DB_URL)),
        run_schema_migrations=run_schema_migrations,
    )
    starting.app = _app_mapping(_LEGACY_DAYS)
    try:
        initialized = await starting.ensure_schema_initialized()
    finally:
        await starting.close_async()
        await _forget(engine, _LEGACY_KEY)

    match refused, initialized:
        case False, Ok():
            pass
        case True, Err(err_value=failure):
            assert failure.code is BrokerErrorCode.SCHEMA_INIT_FAILED, failure
            assert not failure.retryable, failure
            assert _LEGACY_KEY in failure.message, failure.message
            assert '5 days' in failure.message, failure.message
            assert f'{_LEGACY_DAYS} days' in failure.message, failure.message
        case _:
            pytest.fail(f'refused={refused}, but initialization gave {initialized!r}')
