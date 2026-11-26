"""The enqueue error classes that the core builds, one test per code.

Each failure of ``PostgresBroker.enqueue_async`` comes back as the generated
class of ``horsies._catalog.broker_errors`` that the core built
(``BrokerOperationError::enqueue_error``), and the task send maps it to its
``TaskSend*`` class with the payload that the send made. The reference is
the Python send of 0.7.0:

- ``INVALID_INPUT``: a rule of the version-1 command or of the scoped
  idempotency key has its ``ValueError`` as the cause, with the rule as its
  text; a rule that refused a value that reads has ``NoCause()``.
- ``INVALID_TASK_OPTIONS``: options that do not read as JSON have a
  ``SerializationError`` as the cause; a rule has ``NoCause()``.
- ``ENQUEUE_FAILED``: Python's message ``Failed to enqueue task <name>:
  <cause>``, the retry rule of the cause, and the core's class of the cause
  (0.7: the driver's exception) with ``enqueue`` as its subject and the
  cause's own text.
- ``PAYLOAD_MISMATCH`` and ``IDEMPOTENCY_KEY_CONFLICT``: not retryable, with
  no exception; the conflict names the owner with Python's message.
- The payload of a send error holds ``input_json=None`` for a send with no
  keyword arguments (0.7: ``kwargs_json=None``), and the keyword arguments
  as one JSON object otherwise.

The refusals and the failed connect use a broker that points at a port that
nothing listens on; the mismatch and the conflict use the suite database.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator, Final
from uuid import uuid4

import pytest
import pytest_asyncio
from pydantic import SecretStr

from horsies._catalog.error_detail import NoCause
from horsies._core import CoreConnectionError, CorePreconditionFailed
from horsies.core.app import Horsies
from horsies.core.brokers.enqueue_task import SendKind
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import (
    BrokerEnqueueError,
    BrokerEnqueueFailed,
    BrokerErrorCode,
    BrokerIdempotencyKeyConflict,
    BrokerInvalidInput,
    BrokerInvalidTaskOptions,
    BrokerPayloadMismatch,
)
from horsies.core.codec.json_io import SerializationError
from horsies.core.history.periods import RetentionClassKey
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.task_send_types import (
    TaskSendAsyncContext,
    TaskSendEnqueueFailed,
    TaskSendErrorCode,
    TaskSendIdempotencyKeyConflict,
    TaskSendPayloadMismatch,
    TaskSendValidationFailed,
)
from horsies.core.models.tasks import TaskError
from horsies.core.types.result import Ok, Result, is_err, is_ok
from tests.task_ids import mint_task_id

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

UNREACHABLE_URL: Final = 'postgresql://postgres:none@127.0.0.1:1/none'
"""A database that refuses every connect at once."""

TASK_NAME: Final = 'enqueue.error_classes'


@pytest_asyncio.fixture
async def unreachable_broker() -> AsyncGenerator[PostgresBroker, None]:
    """A broker that runs no schema step, pointed at a port that nothing
    listens on: the send's statement fails at its connect."""
    broken = PostgresBroker(
        PostgresConfig(database_url=SecretStr(UNREACHABLE_URL)),
        assume_initialized=True,
    )
    yield broken
    await broken.close_async()


@pytest.fixture
def unreachable_app(unreachable_broker: PostgresBroker) -> Horsies:
    """An app whose sends go to ``unreachable_broker``."""
    horsies_app = Horsies(
        AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=PostgresConfig(database_url=SecretStr(UNREACHABLE_URL)),
        ),
    )
    horsies_app._broker = unreachable_broker  # pyright: ignore[reportPrivateUsage]
    unreachable_broker.app = horsies_app
    return horsies_app


def _optional_value_task(app: Horsies, name: str) -> Any:
    """A task whose one keyword argument has a default: a send can pass it
    or pass nothing."""

    @app.task(task_name=name)
    def optional_value(*, value: int = 0) -> Result[int, TaskError]:
        return Ok(value)

    return optional_value


async def _send(broker: PostgresBroker, **keywords: Any) -> Result[Any, BrokerEnqueueError]:
    """One send of ``TASK_NAME`` with a new task id, the call time as
    ``sent_at``, and ``keywords`` over those."""
    arguments: dict[str, Any] = {
        'task_id': mint_task_id(),
        'send_kind': SendKind.FIRST,
        'sent_at': datetime.now(timezone.utc),
        **keywords,
    }
    return await broker.enqueue_async(TASK_NAME, **arguments)


def _error_of(result: Result[Any, BrokerEnqueueError]) -> BrokerEnqueueError:
    assert is_err(result), result
    return result.err_value


@pytest.mark.parametrize(
    ('keywords', 'message', 'rule'),
    [
        pytest.param(
            {'priority': 0},
            f'enqueue input refused for {TASK_NAME}: priority must be between 1 and 100',
            'priority must be between 1 and 100',
            id='command-rule',
        ),
        pytest.param(
            {'idempotency_key': 'k' * 256},
            f'idempotency key refused for {TASK_NAME}: '
            'idempotency key must be at most 255 UTF-8 bytes',
            'idempotency key must be at most 255 UTF-8 bytes',
            id='key-rule',
        ),
    ],
)
async def test_a_rule_of_the_command_or_the_key_is_invalid_input_with_its_value_error(
    unreachable_broker: PostgresBroker,
    keywords: dict[str, Any],
    message: str,
    rule: str,
) -> None:
    error = _error_of(await _send(unreachable_broker, **keywords))

    assert type(error) is BrokerInvalidInput
    assert error.code is BrokerErrorCode.INVALID_INPUT
    assert error.retryable is False
    assert error.message == message
    assert type(error.exception) is ValueError
    assert str(error.exception) == rule


async def test_a_rule_that_refused_a_value_that_reads_is_invalid_input_with_no_cause(
    unreachable_broker: PostgresBroker,
) -> None:
    error = _error_of(
        await _send(
            unreachable_broker,
            sent_at=datetime.now(timezone.utc) + timedelta(minutes=10),
        ),
    )

    assert type(error) is BrokerInvalidInput
    assert error.retryable is False
    assert error.message == (
        'enqueue contract violation: sent_at is in the future without enqueued_at or '
        'enqueue_delay_seconds; sent_at is a call-site timestamp'
    )
    assert type(error.exception) is NoCause


async def test_a_send_refused_by_a_rule_keeps_the_value_error(
    unreachable_app: Horsies,
) -> None:
    """The task send maps ``INVALID_INPUT`` to ``VALIDATION_FAILED`` with
    the message and the cause, as in 0.7.0."""
    task = _optional_value_task(unreachable_app, f'enqueue_classes_{uuid4().hex}')

    result = await task.with_options(idempotency_key='k' * 256).send_async(value=1)

    assert is_err(result)
    error = result.err_value
    assert type(error) is TaskSendValidationFailed
    assert error.code is TaskSendErrorCode.VALIDATION_FAILED
    assert type(error.exception) is ValueError
    assert str(error.exception) == 'idempotency key must be at most 255 UTF-8 bytes'


async def test_refused_task_options_are_invalid_task_options_with_python_s_cause(
    unreachable_broker: PostgresBroker,
) -> None:
    unreadable = _error_of(await _send(unreachable_broker, task_options='{bad'))

    assert type(unreadable) is BrokerInvalidTaskOptions
    assert unreadable.code is BrokerErrorCode.INVALID_TASK_OPTIONS
    assert unreadable.retryable is False
    assert type(unreadable.exception) is SerializationError
    reader_error = str(unreadable.exception)
    assert reader_error != ''
    assert unreadable.message.startswith('enqueue contract violation: task_options JSON ')
    assert unreadable.message.endswith(f': {reader_error}')

    by_rule = _error_of(
        await _send(unreachable_broker, task_options='{"retry_policy": "not_a_dict"}'),
    )

    assert type(by_rule) is BrokerInvalidTaskOptions
    assert by_rule.retryable is False
    assert 'retry_policy is not an object: str' in by_rule.message
    assert type(by_rule.exception) is NoCause


async def test_a_failed_connect_is_enqueue_failed_with_the_core_class_of_its_cause(
    unreachable_broker: PostgresBroker,
) -> None:
    error = _error_of(await _send(unreachable_broker, input_json='{"value":1}'))

    assert type(error) is BrokerEnqueueFailed
    assert error.code is BrokerErrorCode.ENQUEUE_FAILED
    assert error.retryable is True
    assert type(error.exception) is CoreConnectionError
    assert error.exception.subject == 'enqueue'
    assert error.exception.retryable is True
    assert error.exception.message != ''
    assert str(error.exception) == f'enqueue: {error.exception.message}'
    assert error.message == f'Failed to enqueue task {TASK_NAME}: {error.exception.message}'


async def test_a_retention_key_that_no_class_derives_is_enqueue_failed_not_retried(
    unreachable_broker: PostgresBroker,
) -> None:
    """Python's insert met the CHECK of the class key and failed the
    enqueue, not retryable; the core refuses the key before any
    statement."""
    error = _error_of(
        await _send(
            unreachable_broker,
            retention_class_key=RetentionClassKey('standard_0d'),
        ),
    )

    assert type(error) is BrokerEnqueueFailed
    assert error.retryable is False
    assert type(error.exception) is CorePreconditionFailed
    assert error.exception.retryable is False
    assert error.exception.message.startswith('retention class key "standard_0d"')
    assert str(error.exception) == f'enqueue: {error.exception.message}'
    assert error.message == f'Failed to enqueue task {TASK_NAME}: {error.exception.message}'


@pytest.mark.parametrize(
    ('keywords', 'input_json'),
    [
        pytest.param({}, None, id='no-input'),
        pytest.param({'value': 1}, '{"value":1}', id='keyword-input'),
    ],
)
async def test_a_failed_send_keeps_the_payload_that_the_send_made(
    unreachable_app: Horsies,
    keywords: dict[str, int],
    input_json: str | None,
) -> None:
    """``ENQUEUE_FAILED`` of a task send: the class, the cause of the broker
    class, and the payload with the input of 0.7.0."""
    task = _optional_value_task(unreachable_app, f'enqueue_classes_{uuid4().hex}')

    result = await task.send_async(**keywords)

    assert is_err(result)
    error = result.err_value
    assert type(error) is TaskSendEnqueueFailed
    assert error.code is TaskSendErrorCode.ENQUEUE_FAILED
    assert error.retryable is True
    assert type(error.exception) is CoreConnectionError
    assert str(error.exception) == f'enqueue: {error.exception.message}'
    assert error.payload.input_json == input_json
    assert error.payload.task_name == task.task_name


async def test_a_task_id_with_another_command_is_payload_mismatch(
    app: Horsies, broker: PostgresBroker,
) -> None:
    task_id = mint_task_id()
    sent_at = datetime.now(timezone.utc)
    first = await _send(broker, task_id=task_id, sent_at=sent_at, input_json='{"value":1}')
    assert first == Ok(task_id)

    error = _error_of(
        await _send(broker, task_id=task_id, sent_at=sent_at, input_json='{"value":2}'),
    )

    assert type(error) is BrokerPayloadMismatch
    assert error.code is BrokerErrorCode.PAYLOAD_MISMATCH
    assert error.retryable is False
    assert not hasattr(error, 'exception')
    assert error.message == (
        f'payload mismatch for task_id {task_id}: the existing task has a different '
        'command or idempotency key'
    )

    # The task send keeps the payload that it sent: a sync send inside the
    # running loop returns its prepared payload, and the async resend sends it.
    task = _optional_value_task(app, f'enqueue_classes_{uuid4().hex}')
    prepared = task.send(value=1)
    assert is_err(prepared)
    assert isinstance(prepared.err_value, TaskSendAsyncContext)
    assert is_ok(await task.resend_async(prepared.err_value))
    changed = dataclasses.replace(
        prepared.err_value,
        payload=dataclasses.replace(prepared.err_value.payload, input_json=None),
    )

    mismatch = await task.resend_async(changed)

    assert is_err(mismatch)
    assert type(mismatch.err_value) is TaskSendPayloadMismatch
    assert mismatch.err_value.task_id == changed.task_id
    assert mismatch.err_value.payload == changed.payload
    assert mismatch.err_value.payload.input_json is None


async def test_a_key_that_another_task_holds_is_idempotency_key_conflict(
    app: Horsies, broker: PostgresBroker,
) -> None:
    name = f'enqueue_classes_{uuid4().hex}'
    task = _optional_value_task(app, name)
    key = f'error-classes-{uuid4()}'
    first = await task.with_options(idempotency_key=key).send_async(value=1)
    assert is_ok(first), first
    owner = first.ok_value.task_id

    error = await broker.enqueue_async(
        name,
        task_id=mint_task_id(),
        send_kind=SendKind.FIRST,
        input_json='{"value":2}',
        sent_at=datetime.now(timezone.utc),
        idempotency_key=key,
    )

    assert is_err(error)
    conflict = error.err_value
    assert type(conflict) is BrokerIdempotencyKeyConflict
    assert conflict.code is BrokerErrorCode.IDEMPOTENCY_KEY_CONFLICT
    assert conflict.retryable is False
    assert not hasattr(conflict, 'exception')
    assert conflict.owner_task_id == owner
    assert conflict.message == (
        f'idempotency key for {name} is reserved by task {owner} with a different command'
    )

    sent = await task.with_options(idempotency_key=key).send_async()

    assert is_err(sent)
    assert type(sent.err_value) is TaskSendIdempotencyKeyConflict
    assert sent.err_value.owner_task_id == owner
    assert sent.err_value.message == conflict.message
    assert sent.err_value.payload.input_json is None
