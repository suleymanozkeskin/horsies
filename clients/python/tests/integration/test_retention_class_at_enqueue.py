"""retention through the public send surface, end to end.

The adopter contract under test: a send that names no period lands in the
immutable 30-day default class; explicit ``None`` lands in forever; a
period this process does not register is refused at the send call with
the period named, never as a partition-routing failure later.

The app's broker is the real migrated test broker — the public path runs
unmocked from ``with_options`` through the enqueue INSERT.
"""

from __future__ import annotations

from datetime import timedelta

from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text

import horsies._core as core
from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.ddl.classes import DEFAULT_RETENTION_CLASS_KEY
from horsies.core.history.ddl.tables import FOREVER_CLASS_KEY
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.history.periods import period_class_key
from horsies.core.models.retention import RetentionConfig
from horsies.core.models.task_send_types import TaskSendValidationFailed
from horsies.core.models.tasks import TaskError
from horsies.core.types.result import Ok, Result, is_err, is_ok
from tests.broker_resources_support import current_resources

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope='function'),
]


_TEST_APP: Horsies | None = None


def _test_app(broker: PostgresBroker) -> Horsies:
    """A real app whose broker IS the migrated test broker.

    The DSN is never dialed: ``get_broker`` returns the injected broker,
    so the public send path runs against the real schema.
    """
    global _TEST_APP
    if _TEST_APP is None:
        cfg = AppConfig(
            database=PostgresConfig(
                database_url='postgresql://u:p@localhost/db',
            )
        )
        _TEST_APP = Horsies(cfg)

        @_TEST_APP.task(name='retention_enqueue_test')
        def _retention_enqueue_test(*, x: int) -> Result[int, TaskError]:
            return Ok(x)

    _TEST_APP._broker = broker  # pyright: ignore[reportPrivateUsage]
    return _TEST_APP


_DECLARING_APP: Horsies | None = None
DECLARED_PERIOD = timedelta(days=90)
DECLARED_KEY = period_class_key(DECLARED_PERIOD.days)


def _declaring_app(broker: PostgresBroker) -> Horsies:
    """An app whose config lists one extra retention period.

    Separate from `_test_app` because the send validator reads the
    listed periods from config: an app that lists nothing must keep
    refusing the period, which is the per-process contract under test.
    """
    global _DECLARING_APP
    if _DECLARING_APP is None:
        cfg = AppConfig(
            database=PostgresConfig(
                database_url='postgresql://u:p@localhost/db',
            ),
            retention=RetentionConfig(retention_periods=(DECLARED_PERIOD,)),
        )
        _DECLARING_APP = Horsies(cfg)

        @_DECLARING_APP.task(name='retention_declared_test')
        def _retention_declared_test(*, x: int) -> Result[int, TaskError]:
            return Ok(x)

    _DECLARING_APP._broker = broker  # pyright: ignore[reportPrivateUsage]
    return _DECLARING_APP


async def _row_class(broker: PostgresBroker, task_id: str) -> str | None:
    async with current_resources(broker).engine.connect() as connection:
        return (
            await connection.execute(
                text(
                    'SELECT retention_class_key FROM horsies_tasks '
                    'WHERE id = CAST(:task_id AS uuid)'
                ),
                {'task_id': task_id},
            )
        ).scalar_one()


@pytest_asyncio.fixture(autouse=True)
async def _coverage(broker: PostgresBroker) -> None:
    """The classes and today's leaves exist before any send: one coverage
    pass of the core (``CoreHandle.ensure_partition_coverage_async``) with
    the default horizons. Its outcome is checked, not assumed."""
    defaults = RetentionConfig()
    outcome = await broker._core_handle.ensure_partition_coverage_async(  # pyright: ignore[reportPrivateUsage]
        core.CoverageRequest(
            history_horizon_days=defaults.history_leaf_horizon_days,
            heartbeat_horizon_hours=defaults.heartbeat_leaf_horizon_hours,
            declared_period_days=[period.days for period in defaults.retention_periods],
            serialization='GATED',
        ),
    )
    assert isinstance(outcome, core.CoverageEnsuredRecord), outcome


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
            retention=None
        ).send_async(x=2)

        assert is_ok(result)
        row_class = await _row_class(broker, result.ok_value.task_id)
        assert row_class == FOREVER_CLASS_KEY

    async def test_an_unregistered_period_is_refused_at_send_with_no_row(
        self, broker: PostgresBroker
    ) -> None:
        app = _test_app(broker)
        task: Any = app.tasks['retention_enqueue_test']
        async with current_resources(broker).engine.connect() as connection:
            before = (
                await connection.execute(
                    text('SELECT count(*) FROM horsies_tasks')
                )
            ).scalar_one()

        result = await task.with_options(
            retention=timedelta(days=9)
        ).send_async(x=3)

        assert is_err(result)
        assert isinstance(result.err_value, TaskSendValidationFailed)
        assert 'Retention of 9 days' in result.err_value.message
        async with current_resources(broker).engine.connect() as connection:
            after = (
                await connection.execute(
                    text('SELECT count(*) FROM horsies_tasks')
                )
            ).scalar_one()
        assert after == before, 'a refused send must write nothing'


    async def test_a_listed_period_is_accepted_at_send(
        self, broker: PostgresBroker
    ) -> None:
        """The validator's set is config-derived, so a listed period lands."""
        app = _declaring_app(broker)
        task: Any = app.tasks['retention_declared_test']

        result = await task.with_options(
            retention=DECLARED_PERIOD
        ).send_async(x=7)

        assert not is_err(result), getattr(result, 'err_value', None)
        assert await _row_class(broker, result.ok_value.task_id) == DECLARED_KEY

    async def test_an_unlisted_period_is_still_refused(
        self, broker: PostgresBroker
    ) -> None:
        """Listing one period does not open the door to every period."""
        app = _declaring_app(broker)
        task: Any = app.tasks['retention_declared_test']

        result = await task.with_options(
            retention=timedelta(days=91)
        ).send_async(x=8)

        assert is_err(result)
        assert isinstance(result.err_value, TaskSendValidationFailed)
        assert 'Retention of 91 days' in result.err_value.message
        assert '90 days' in result.err_value.message, (
            'the refusal should name what this deployment does register'
        )

    async def test_a_process_that_omits_the_period_refuses_it(
        self, broker: PostgresBroker
    ) -> None:
        """The per-process contract, stated as a test.

        The set is process-static by design — no database lookup on the
        send path — so an app whose config omits a period refuses it even
        though another app in this same database lists and uses it.
        """
        declaring = _declaring_app(broker)
        declared_task: Any = declaring.tasks['retention_declared_test']
        accepted = await declared_task.with_options(
            retention=DECLARED_PERIOD
        ).send_async(x=9)
        assert not is_err(accepted)

        plain = _test_app(broker)
        plain_task: Any = plain.tasks['retention_enqueue_test']
        refused = await plain_task.with_options(
            retention=DECLARED_PERIOD
        ).send_async(x=9)

        assert is_err(refused), (
            'a process that does not list the period must refuse it, '
            'even though the same database already holds rows in it'
        )
        assert isinstance(refused.err_value, TaskSendValidationFailed)
