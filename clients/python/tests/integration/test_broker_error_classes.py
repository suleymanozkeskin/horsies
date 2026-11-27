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
    DatabaseErrorCode,
    DatabaseInvalidInput,
    DatabaseListenerDisabled,
    DatabaseOperationFailed,
)
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import is_err

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

UNREACHABLE_URL = 'postgresql://postgres:none@127.0.0.1:1/none'
"""A database that refuses every connect at once."""

TASK_ID = uuid.UUID('0193a1f2-4c5d-7e6f-8a9b-0c1d2e3f4a5b')


async def _broker(*, assume_initialized: bool) -> PostgresBroker:
    return PostgresBroker(
        PostgresConfig(database_url=SecretStr(UNREACHABLE_URL)),
        assume_initialized=assume_initialized,
    )


@pytest_asyncio.fixture
async def unreachable_broker() -> AsyncGenerator[PostgresBroker, None]:
    """A broker with a listener, pointed at a port that nothing listens on."""
    broken = await _broker(assume_initialized=False)
    yield broken
    await broken.close_async()


@pytest_asyncio.fixture
async def listenerless_broker() -> AsyncGenerator[PostgresBroker, None]:
    """A broker built with ``assume_initialized=True``: it has no listener."""
    broken = await _broker(assume_initialized=True)
    yield broken
    await broken.close_async()


def _assert_failed_connect(
    error: object,
    code: DatabaseErrorCode,
    prefix: str,
    subject: str,
) -> None:
    """A failed connect of a broker operation, as the core builds it:
    ``DatabaseOperationFailed`` with the operation's code, Python 0.7's message
    (``prefix`` and the cause's text), retryable, and the core's class of the
    cause with the operation as its subject and the cause's own text."""
    assert type(error) is DatabaseOperationFailed
    assert error.code is code
    assert error.retryable is True
    assert error.message.startswith(prefix)
    cause_text = error.message.removeprefix(prefix)
    assert cause_text != ''
    assert type(error.cause) is CoreConnectionError
    assert error.cause.subject == subject
    assert error.cause.message == cause_text
    assert error.cause.retryable is True
    assert str(error.cause) == f'{subject}: {cause_text}'


async def test_a_failed_database_ping_is_db_ping_failed(
    unreachable_broker: PostgresBroker,
) -> None:
    result = await unreachable_broker.ping_database_async()

    assert is_err(result)
    _assert_failed_connect(
        result.err_value,
        DatabaseErrorCode.DATABASE_PING_FAILED,
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
        DatabaseErrorCode.WORKER_PING_FAILED,
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
        DatabaseErrorCode.TASK_INFO_QUERY_FAILED,
        f'get_task_info failed for {TASK_ID}: ',
        'task_info',
    )


async def test_a_failed_result_read_names_the_task(
    unreachable_broker: PostgresBroker,
) -> None:
    result = await unreachable_broker.get_raw_result_record_async(TASK_ID, timeout_ms=0)

    assert is_err(result)
    _assert_failed_connect(
        result.err_value,
        DatabaseErrorCode.TASK_INFO_QUERY_FAILED,
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
        DatabaseErrorCode.MONITORING_QUERY_FAILED,
        'list_worker_states failed: ',
        'list_worker_states',
    )


async def test_a_refused_argument_is_invalid_input_with_no_cause(
    unreachable_broker: PostgresBroker,
) -> None:
    result = await unreachable_broker.ping_workers_async(timeout_seconds=0)

    assert is_err(result)
    error = result.err_value
    assert type(error) is DatabaseInvalidInput
    assert error.code is DatabaseErrorCode.INVALID_INPUT
    assert error.retryable is False
    assert error.message == 'timeout_seconds must be positive, got 0'
    assert type(error.cause) is NoCause


async def test_a_refused_history_limit_is_invalid_input_with_no_cause(
    unreachable_broker: PostgresBroker,
) -> None:
    result = await unreachable_broker.get_worker_state_history_async('w1', limit=0)

    assert is_err(result)
    error = result.err_value
    assert type(error) is DatabaseInvalidInput
    assert error.message == 'limit must be positive when set, got 0'
    assert type(error.cause) is NoCause


async def test_a_ping_on_a_broker_with_no_listener_is_listener_disabled(
    listenerless_broker: PostgresBroker,
) -> None:
    result = await listenerless_broker.ping_workers_async(timeout_seconds=0.5)

    assert is_err(result)
    error = result.err_value
    assert type(error) is DatabaseListenerDisabled
    assert error.code is DatabaseErrorCode.LISTENER_DISABLED
    assert error.retryable is False
    assert error.message == (
        'ping_workers listener unavailable: this broker was built with '
        'assume_initialized=True and has no listener'
    )
