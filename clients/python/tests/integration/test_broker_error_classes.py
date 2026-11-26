"""The broker error classes that the core builds, one test per outcome.

Each failure of a broker operation that runs in the core comes back as the
generated class of ``horsies._catalog.broker_errors``, built in Rust: the
class, its fields with Python 0.7's message of the operation, and
``exception``, the core's class of the cause with the operation as its
subject (``str(exception)`` is ``{subject}: {cause text}``), or ``NoCause()``
for a check of the core. The brokers point at a port that nothing listens
on, so every read fails at its connect.
"""

from __future__ import annotations

import uuid
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from pydantic import SecretStr

from horsies._catalog.error_detail import NoCause
from horsies._core import CoreConnectionError
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import (
    BrokerErrorCode,
    BrokerInvalidInput,
    BrokerListenerDisabled,
    BrokerOperationFailed,
)
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.schema_mode import SchemaMode
from horsies.core.types.result import is_err

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

UNREACHABLE_URL = 'postgresql://postgres:none@127.0.0.1:1/none'
"""A database that refuses every connect at once."""

TASK_ID = uuid.UUID('0193a1f2-4c5d-7e6f-8a9b-0c1d2e3f4a5b')


async def _broker(*, schema_mode: SchemaMode) -> PostgresBroker:
    return PostgresBroker(
        PostgresConfig(database_url=SecretStr(UNREACHABLE_URL)),
        schema_mode=schema_mode,
    )


@pytest_asyncio.fixture
async def unreachable_broker() -> AsyncGenerator[PostgresBroker, None]:
    """A broker with a listener, pointed at a port that nothing listens on."""
    broken = await _broker(schema_mode=SchemaMode.MIGRATE_AND_VALIDATE)
    yield broken
    await broken.close_async()


@pytest_asyncio.fixture
async def listenerless_broker() -> AsyncGenerator[PostgresBroker, None]:
    """A broker of ``SchemaMode.ASSUME_INITIALIZED``: it has no listener."""
    broken = await _broker(schema_mode=SchemaMode.ASSUME_INITIALIZED)
    yield broken
    await broken.close_async()


def _assert_failed_connect(
    error: object,
    code: BrokerErrorCode,
    prefix: str,
    subject: str,
) -> None:
    """A failed connect of a broker operation, as the core builds it:
    ``BrokerOperationFailed`` with the operation's code, Python 0.7's message
    (``prefix`` and the cause's text), retryable, and the core's class of the
    cause with the operation as its subject and the cause's own text."""
    assert type(error) is BrokerOperationFailed
    assert error.code is code
    assert error.retryable is True
    assert error.message.startswith(prefix)
    cause_text = error.message.removeprefix(prefix)
    assert cause_text != ''
    assert type(error.exception) is CoreConnectionError
    assert error.exception.subject == subject
    assert error.exception.message == cause_text
    assert error.exception.retryable is True
    assert str(error.exception) == f'{subject}: {cause_text}'


async def test_a_failed_database_ping_is_db_ping_failed(
    unreachable_broker: PostgresBroker,
) -> None:
    result = await unreachable_broker.ping_database_async()

    assert is_err(result)
    _assert_failed_connect(
        result.err_value,
        BrokerErrorCode.DB_PING_FAILED,
        'ping_database failed: ',
        'ping_database',
    )


async def test_a_failed_worker_ping_is_worker_ping_failed(
    unreachable_broker: PostgresBroker,
) -> None:
    result = await unreachable_broker.ping_workers_async(timeout_seconds=0.5)

    # The first step of a ping subscribes to the reply channel, as in 0.7, so
    # an unreachable server fails it there.
    assert is_err(result)
    _assert_failed_connect(
        result.err_value,
        BrokerErrorCode.WORKER_PING_FAILED,
        'ping_workers reply subscribe failed: ',
        'ping_workers',
    )


async def test_a_failed_task_info_read_names_the_task(
    unreachable_broker: PostgresBroker,
) -> None:
    result = await unreachable_broker.get_task_info_async(TASK_ID)

    assert is_err(result)
    _assert_failed_connect(
        result.err_value,
        BrokerErrorCode.TASK_INFO_QUERY_FAILED,
        f'get_task_info failed for {TASK_ID}: ',
        'task_info',
    )


async def test_a_failed_result_read_names_the_task(
    unreachable_broker: PostgresBroker,
) -> None:
    result = await unreachable_broker.get_raw_result_record_async(TASK_ID, wait_ms=0)

    assert is_err(result)
    _assert_failed_connect(
        result.err_value,
        BrokerErrorCode.TASK_INFO_QUERY_FAILED,
        f'Broker error while retrieving task {TASK_ID}: ',
        'raw_result_record',
    )


async def test_a_failed_monitoring_read_names_the_read(
    unreachable_broker: PostgresBroker,
) -> None:
    result = await unreachable_broker.list_worker_states_async()

    assert is_err(result)
    _assert_failed_connect(
        result.err_value,
        BrokerErrorCode.MONITORING_QUERY_FAILED,
        'list_worker_states failed: ',
        'list_worker_states',
    )


async def test_a_refused_argument_is_invalid_input_with_no_cause(
    unreachable_broker: PostgresBroker,
) -> None:
    result = await unreachable_broker.ping_workers_async(timeout_seconds=0)

    assert is_err(result)
    error = result.err_value
    assert type(error) is BrokerInvalidInput
    assert error.code is BrokerErrorCode.INVALID_INPUT
    assert error.retryable is False
    assert error.message == 'timeout_seconds must be positive, got 0'
    assert type(error.exception) is NoCause


async def test_a_refused_history_limit_is_invalid_input_with_no_cause(
    unreachable_broker: PostgresBroker,
) -> None:
    result = await unreachable_broker.get_worker_state_history_async('w1', limit=0)

    assert is_err(result)
    error = result.err_value
    assert type(error) is BrokerInvalidInput
    assert error.message == 'limit must be positive when set, got 0'
    assert type(error.exception) is NoCause


async def test_a_ping_on_a_broker_with_no_listener_is_listener_disabled(
    listenerless_broker: PostgresBroker,
) -> None:
    result = await listenerless_broker.ping_workers_async(timeout_seconds=0.5)

    assert is_err(result)
    error = result.err_value
    assert type(error) is BrokerListenerDisabled
    assert error.code is BrokerErrorCode.LISTENER_DISABLED
    assert error.retryable is False
    assert error.message == (
        'ping_workers listener unavailable: this broker has the schema mode '
        'ASSUME_INITIALIZED and has no listener'
    )
