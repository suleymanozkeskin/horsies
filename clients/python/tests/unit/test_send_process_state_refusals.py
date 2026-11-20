# pyright: reportPrivateUsage=false
"""A process-state refusal of the core raises from every send.

``ForkedAfterCoreStart`` (this process was forked without exec after the
core started) and ``CoreStopped`` (a core background task panicked) are
states of the process, not of one send. The four sends, the four retry
methods and ``PostgresBroker.enqueue`` / ``enqueue_async`` raise them; none
answers them as ``ENQUEUE_FAILED``. Every other core error keeps its
``ENQUEUE_FAILED`` mapping. The core handle is a stand-in; touches no
database.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Final
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from pydantic import SecretStr

from horsies.core.telemetry.send_telemetry import NO_SEND_TELEMETRY
from horsies._core import (
    CoreCallTimedOut,
    CoreError,
    CoreStartFailed,
    CoreStopped,
    ForkedAfterCoreStart,
    TooManyPendingCalls,
)
from horsies.core.brokers.enqueue_task import SendKind
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import (
    BrokerEnqueueError,
    BrokerEnqueueFailed,
    BrokerSyncCallError,
)
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.task_send_types import TaskSendEnqueueFailed, TaskSendPayload
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.task_decorator import create_task_wrapper
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Result, is_err

pytestmark = [pytest.mark.unit]

_URL: Final = 'postgresql://u:p@localhost:5432/db'
_TASK_NAME: Final = 'test.refused_fn'
_TASK_ID: Final = UUID('0198c0de-0000-7000-8000-0000000019a8')
_SCHEDULE_DELAY_S: Final = 5


def _refusals() -> list[Any]:
    """One fresh instance of each process-state refusal, per parametrize."""
    return [
        pytest.param(ForkedAfterCoreStart(100, 101, 'use spawn or forkserver'), id='forked'),
        pytest.param(CoreStopped('log forwarder', 'planned panic'), id='stopped'),
    ]


def _broker_raising(error: CoreError) -> tuple[PostgresBroker, MagicMock]:
    """A broker whose core handle raises ``error`` from both enqueue forms,
    and that handle."""
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(_URL)))
    handle = MagicMock()
    handle.enqueue = MagicMock(side_effect=error)
    handle.enqueue_async = AsyncMock(side_effect=error)
    stand_in: Any = handle
    broker._core_handle = stand_in
    return broker, handle


def _app_with(broker: PostgresBroker) -> MagicMock:
    """An app that resends a retryable ``ENQUEUE_FAILED``: a raise must end
    the send at its first enqueue all the same."""
    app = MagicMock()
    app.config.queue_mode.name = 'DEFAULT'
    app.config.resend_on_transient_err = True
    app.config.payload = PayloadPolicy()
    app.are_sends_suppressed.return_value = False
    app.send_telemetry.return_value = NO_SEND_TELEMETRY
    app.validate_queue_name.return_value = 'default'
    app.get_broker.return_value = broker
    return app


def _wrapper(app: MagicMock) -> Any:
    def refused_fn(*, x: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=x)

    return create_task_wrapper(refused_fn, app, _TASK_NAME)


def _failed_send(delay_seconds: int | None) -> TaskSendEnqueueFailed:
    """The error of an earlier send, as a retry method takes it."""
    return TaskSendEnqueueFailed(
        task_id=_TASK_ID,
        payload=TaskSendPayload(
            task_name=_TASK_NAME,
            queue_name='default',
            priority=100,
            input_json='{"x":1}',
            sent_at=datetime(2026, 11, 18, tzinfo=timezone.utc),
            good_until=None,
            enqueue_delay_seconds=delay_seconds,
            task_options=None,
        ),
        message='Failed to enqueue task test.refused_fn: connection lost',
        retryable=True,
        exception=NoCause(),
    )


def _run_send(wrapper: Any, form: str) -> object:
    """Call one send form of ``wrapper`` with ``x=1``."""
    match form:
        case 'send':
            return wrapper.send(x=1)
        case 'send_async':
            return asyncio.run(wrapper.send_async(x=1))
        case 'schedule':
            return wrapper.schedule(_SCHEDULE_DELAY_S, x=1)
        case 'schedule_async':
            return asyncio.run(wrapper.schedule_async(_SCHEDULE_DELAY_S, x=1))
        case _:
            raise AssertionError(f'unknown send form: {form}')


def _run_retry(wrapper: Any, form: str) -> object:
    """Call one retry method of ``wrapper`` with the error it accepts."""
    match form:
        case 'retry_send':
            return wrapper.retry_send(_failed_send(None))
        case 'retry_send_async':
            return asyncio.run(wrapper.retry_send_async(_failed_send(None)))
        case 'retry_schedule':
            return wrapper.retry_schedule(_failed_send(_SCHEDULE_DELAY_S))
        case 'retry_schedule_async':
            return asyncio.run(wrapper.retry_schedule_async(_failed_send(_SCHEDULE_DELAY_S)))
        case _:
            raise AssertionError(f'unknown retry form: {form}')


def _enqueue_calls(handle: MagicMock, form: str) -> int:
    """The core enqueue calls that a form made: the sync forms call
    ``enqueue``, the async forms ``enqueue_async``."""
    match form.endswith('_async'):
        case True:
            return handle.enqueue_async.await_count
        case False:
            return handle.enqueue.call_count


async def _broker_enqueue_async(broker: PostgresBroker) -> Result[UUID, BrokerEnqueueError]:
    return await broker.enqueue_async(
        _TASK_NAME,
        'default',
        task_id=_TASK_ID,
        send_kind=SendKind.FIRST,
        input_json='{"x":1}',
        sent_at=datetime.now(timezone.utc),
    )


def _broker_enqueue(
    broker: PostgresBroker,
) -> Result[UUID, BrokerEnqueueError | BrokerSyncCallError]:
    return broker.enqueue(
        _TASK_NAME,
        'default',
        task_id=_TASK_ID,
        send_kind=SendKind.FIRST,
        input_json='{"x":1}',
        sent_at=datetime.now(timezone.utc),
    )


class TestBrokerEnqueue:
    @pytest.mark.parametrize('refusal', _refusals())
    def test_enqueue_async_raises_the_refusal(self, refusal: CoreError) -> None:
        broker, handle = _broker_raising(refusal)

        with pytest.raises(type(refusal)) as raised:
            _ = asyncio.run(_broker_enqueue_async(broker))

        assert raised.value is refusal
        handle.enqueue_async.assert_awaited_once()

    @pytest.mark.parametrize('refusal', _refusals())
    def test_enqueue_raises_the_refusal(self, refusal: CoreError) -> None:
        broker, handle = _broker_raising(refusal)

        with pytest.raises(type(refusal)) as raised:
            _ = _broker_enqueue(broker)

        assert raised.value is refusal
        handle.enqueue.assert_called_once()

    @pytest.mark.parametrize('form', ['enqueue', 'enqueue_async'])
    @pytest.mark.parametrize(
        ('error', 'retryable'),
        [
            pytest.param(
                TooManyPendingCalls('CoreHandle.enqueue_async', 65_536), True,
                id='too-many-pending-calls',
            ),
            pytest.param(
                CoreCallTimedOut('CoreHandle.enqueue', 1800, 'outcome_unknown'), True,
                id='call-timed-out',
            ),
            pytest.param(
                CoreStartFailed('the OS refused the runtime threads'), False,
                id='start-failed',
            ),
        ],
    )
    def test_another_core_error_is_still_enqueue_failed(
        self, form: str, error: CoreError, retryable: bool,
    ) -> None:
        broker, _handle = _broker_raising(error)

        result: Result[UUID, BrokerEnqueueError | BrokerSyncCallError]
        match form:
            case 'enqueue':
                result = _broker_enqueue(broker)
            case 'enqueue_async':
                result = asyncio.run(_broker_enqueue_async(broker))
            case _:
                raise AssertionError(f'unknown form: {form}')

        assert is_err(result)
        failed = result.err_value
        assert isinstance(failed, BrokerEnqueueFailed)
        assert failed.exception is error
        assert failed.retryable is retryable


class TestTaskSends:
    @pytest.mark.parametrize('form', ['send', 'send_async', 'schedule', 'schedule_async'])
    @pytest.mark.parametrize('refusal', _refusals())
    def test_a_send_raises_the_refusal_of_the_enqueue(
        self, form: str, refusal: CoreError,
    ) -> None:
        broker, handle = _broker_raising(refusal)
        wrapper = _wrapper(_app_with(broker))

        with pytest.raises(type(refusal)) as raised:
            _ = _run_send(wrapper, form)

        assert raised.value is refusal
        assert _enqueue_calls(handle, form) == 1

    @pytest.mark.parametrize('form', ['send', 'send_async', 'schedule', 'schedule_async'])
    @pytest.mark.parametrize('refusal', _refusals())
    def test_a_send_raises_the_refusal_of_the_id_mint(
        self, form: str, refusal: CoreError,
    ) -> None:
        broker, handle = _broker_raising(TooManyPendingCalls('not reached', 1))
        app = _app_with(broker)
        wrapper = _wrapper(app)

        with patch(
            'horsies.core.task_decorator.core_mint_task_id', side_effect=refusal,
        ):
            with pytest.raises(type(refusal)) as raised:
                _ = _run_send(wrapper, form)

        assert raised.value is refusal
        app.get_broker.assert_not_called()
        handle.enqueue.assert_not_called()
        handle.enqueue_async.assert_not_awaited()


class TestRetryMethods:
    @pytest.mark.parametrize(
        'form', ['retry_send', 'retry_send_async', 'retry_schedule', 'retry_schedule_async'],
    )
    @pytest.mark.parametrize('refusal', _refusals())
    def test_a_retry_raises_the_refusal_of_the_enqueue(
        self, form: str, refusal: CoreError,
    ) -> None:
        broker, handle = _broker_raising(refusal)
        wrapper = _wrapper(_app_with(broker))

        with pytest.raises(type(refusal)) as raised:
            _ = _run_retry(wrapper, form)

        assert raised.value is refusal
        assert _enqueue_calls(handle, form) == 1
