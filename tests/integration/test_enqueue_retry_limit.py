"""`enqueue_async` refuses task options whose retry_policy is malformed.

The live table has no CHECK on `max_retries`, and the retry path reads the
policy only after a failure. Enqueue validates the policy as `RetryPolicy`,
refuses a malformed one with `INVALID_TASK_OPTIONS`, and writes nothing.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerResult
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import MAX_RETRY_ATTEMPTS
from horsies.core.types.result import Err, is_err
from tests.integration.test_task_history_schema_emission import (
    MakeDatabase,
    make_database,
)

__all__ = ['make_database']

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _enqueue(broker: PostgresBroker, task_options: str) -> BrokerResult[str]:
    return await broker.enqueue_async(
        'retry.limit.task',
        task_id=str(uuid.uuid4()),
        enqueue_sha='c' * 64,
        args_json='[]',
        kwargs_json='{}',
        task_options=task_options,
    )


async def _stored_limits(url: str) -> list[int]:
    engine = create_async_engine(url)
    try:
        async with engine.connect() as connection:
            return list(
                (
                    await connection.execute(
                        text(
                            'SELECT max_retries FROM horsies_tasks '
                            'ORDER BY max_retries'
                        )
                    )
                ).scalars()
            )
    finally:
        await engine.dispose()


def _options(**fields: Any) -> str:
    """Task options with a valid exponential policy, `fields` replaced."""
    policy: dict[str, Any] = {
        'max_retries': 3,
        'intervals': [5],
        'backoff_strategy': 'exponential',
        'jitter': True,
        'auto_retry_for': ['TRANSIENT'],
    }
    policy.update(fields)
    return json.dumps({'retry_policy': policy})


@pytest.mark.parametrize(
    'task_options',
    [
        _options(max_retries=-1),
        _options(max_retries=MAX_RETRY_ATTEMPTS + 1),
        _options(max_retries=True),
        _options(max_retries='3'),
        _options(intervals=[5, 10]),
        _options(auto_retry_for=[]),
        _options(surprise=1),
        '{"retry_policy": {"max_retries": 3}}',
        '{"retry_policy": [3]}',
    ],
    ids=[
        'negative',
        'above_bound',
        'boolean',
        'string',
        'exponential_with_two_intervals',
        'no_retry_codes',
        'unknown_field',
        'partial_policy',
        'not_an_object',
    ],
)
async def test_a_malformed_policy_is_refused_without_a_row(
    make_database: MakeDatabase, task_options: str
) -> None:
    url = await make_database()
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(url)))
    try:
        await broker.ensure_schema_initialized()
        outcome = await _enqueue(broker, task_options)
    finally:
        await broker.close_async()

    assert isinstance(outcome, Err), outcome
    assert outcome.err_value.code == BrokerErrorCode.INVALID_TASK_OPTIONS
    assert outcome.err_value.retryable is False
    assert await _stored_limits(url) == []


async def test_valid_policies_store_their_limits(make_database: MakeDatabase) -> None:
    url = await make_database()
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(url)))
    try:
        await broker.ensure_schema_initialized()
        for task_options in (
            '{"retry_policy": null}',
            _options(max_retries=1),
            _options(max_retries=MAX_RETRY_ATTEMPTS),
        ):
            outcome = await _enqueue(broker, task_options)
            assert not is_err(outcome), outcome
    finally:
        await broker.close_async()

    assert await _stored_limits(url) == [0, 1, MAX_RETRY_ATTEMPTS]
