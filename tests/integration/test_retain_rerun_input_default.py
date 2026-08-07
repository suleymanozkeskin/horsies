"""The app-level retain-input default: one policy owner, both readers.

The ratified retention posture is an app-level default plus a per-task
override, snapshotted at enqueue. The per-task parameter's None means
inherit the deployment's standing policy; an explicit value overrides
it; and the resolved value is what the row carries — the same field
the cutover backfill later consults, so policy has exactly one owner.
"""

from __future__ import annotations

import uuid

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import is_err
from tests.integration.test_task_history_schema_emission import (
    MakeDatabase,
    make_database,
)

__all__ = ['make_database']

pytestmark = [pytest.mark.integration]


def test_config_default_is_false_and_never_an_engine_kwarg() -> None:
    config = PostgresConfig(
        database_url=SecretStr('postgresql+psycopg://u:p@localhost/x')
    )
    assert config.retain_rerun_input_default is False
    assert (
        'retain_rerun_input_default'
        not in config.sqlalchemy_engine_kwargs()
    )


async def _enqueue(
    broker: PostgresBroker, *, retain: bool | None
) -> str:
    task_id = str(uuid.uuid4())
    outcome = await broker.enqueue_async(
        'policy.task',
        task_id=task_id,
        enqueue_sha='b' * 64,
        args_json='[]',
        kwargs_json='{}',
        retain_rerun_input=retain,
    )
    assert not is_err(outcome), outcome
    return task_id


@pytest.mark.asyncio
async def test_none_inherits_and_explicit_overrides(
    make_database: MakeDatabase,
) -> None:
    url = await make_database()
    broker = PostgresBroker(
        PostgresConfig(
            database_url=SecretStr(url),
            retain_rerun_input_default=True,
        )
    )
    try:
        await broker.ensure_schema_initialized()
        inherited = await _enqueue(broker, retain=None)
        declined = await _enqueue(broker, retain=False)
        explicit = await _enqueue(broker, retain=True)
    finally:
        await broker.close_async()

    engine = create_async_engine(url)
    try:
        async with engine.connect() as connection:
            rows = {
                str(row.id): row
                for row in (
                    await connection.execute(
                        text(
                            'SELECT id, retain_rerun_input, '
                            'prepared_rerun_input_disposition '
                            'FROM horsies_tasks'
                        )
                    )
                ).all()
            }
    finally:
        await engine.dispose()

    assert rows[inherited].retain_rerun_input is True
    assert rows[inherited].prepared_rerun_input_disposition == 'INLINE'
    assert rows[declined].retain_rerun_input is False
    assert (
        rows[declined].prepared_rerun_input_disposition
        == 'DECLINED_BY_POLICY'
    )
    assert rows[explicit].retain_rerun_input is True
