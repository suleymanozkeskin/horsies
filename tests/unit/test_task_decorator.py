"""Tests for task_decorator: effective_priority, TaskHandle, create_task_wrapper."""

from __future__ import annotations

import asyncio
import inspect
import json
from functools import wraps
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.brokers.result_types import BrokerErrorCode, BrokerOperationError
from horsies.core.errors import ConfigurationError, ErrorCode, TaskDefinitionError
from horsies.core.exception_mapper import ExceptionMapper
from horsies.core.models.tasks import (
    ContractCode,
    OperationalErrorCode,
    RetrievalCode,
    TaskError,
    TaskOptions,
    TaskResult,
)
from horsies.core.models.workflow import WorkflowContextMissingIdError
from horsies.core.task_decorator import (
    BoundTaskNode,
    FromNodeMarker,
    TaskHandle,
    bind_task_node,
    create_task_wrapper,
    effective_priority,
    from_node,
    resolve_node_queue_and_priority,
)
from horsies.core.models.task_send_types import (
    TaskSendError,
    TaskSendErrorCode,
    TaskSendPayload,
)
from horsies.core.types.result import Err, Ok, is_ok, is_err


# =============================================================================
# Helpers
# =============================================================================


def _make_app(
    *,
    queue_mode_name: str = 'DEFAULT',
    custom_queues: list[Any] | None = None,
    suppress_sends: bool = False,
    exception_mapper: ExceptionMapper | None = None,
    default_unhandled_error_code: str = 'UNHANDLED_EXCEPTION',
    resend_on_transient_err: bool = False,
) -> MagicMock:
    """Build a minimal mock Horsies app for unit tests."""
    app = MagicMock()
    app.config.queue_mode.name = queue_mode_name
    app.config.custom_queues = custom_queues
    app.config.exception_mapper = exception_mapper or {}
    app.config.default_unhandled_error_code = default_unhandled_error_code
    app.config.resend_on_transient_err = resend_on_transient_err
    app.are_sends_suppressed.return_value = suppress_sends
    app.validate_queue_name.return_value = 'default'
    return app


def _make_queue_config(name: str, priority: int) -> MagicMock:
    """Build a mock CustomQueueConfig."""
    cfg = MagicMock()
    cfg.name = name
    cfg.priority = priority
    return cfg


# =============================================================================
# effective_priority
# =============================================================================


@pytest.mark.unit
class TestEffectivePriority:
    """Tests for effective_priority function."""

    def test_default_mode_returns_100(self) -> None:
        """DEFAULT queue mode always returns priority 100."""
        app = _make_app(queue_mode_name='DEFAULT')

        result = effective_priority(app, 'anything')

        assert result == 100

    def test_custom_mode_matching_queue_returns_configured_priority(self) -> None:
        """CUSTOM mode returns the matching queue's priority."""
        queues = [_make_queue_config('fast', 1), _make_queue_config('slow', 50)]
        app = _make_app(queue_mode_name='CUSTOM', custom_queues=queues)

        result = effective_priority(app, 'fast')

        assert result == 1

    def test_custom_mode_nonexistent_queue_raises_configuration_error(self) -> None:
        """CUSTOM mode with unknown queue raises ConfigurationError(TASK_INVALID_QUEUE)."""
        queues = [_make_queue_config('fast', 1)]
        app = _make_app(queue_mode_name='CUSTOM', custom_queues=queues)

        with pytest.raises(ConfigurationError) as exc_info:
            effective_priority(app, 'missing')

        assert exc_info.value.code == ErrorCode.TASK_INVALID_QUEUE

    def test_custom_mode_none_custom_queues_raises(self) -> None:
        """CUSTOM mode with custom_queues=None raises ConfigurationError."""
        app = _make_app(queue_mode_name='CUSTOM', custom_queues=None)

        with pytest.raises(ConfigurationError) as exc_info:
            effective_priority(app, 'anything')

        assert exc_info.value.code == ErrorCode.TASK_INVALID_QUEUE


# =============================================================================
# resolve_node_queue_and_priority / bind_task_node
# =============================================================================


def _make_node(
    *,
    queue: str | None = None,
    priority: int | None = None,
    fn_queue: str | None = None,
    name: str = 'task',
) -> MagicMock:
    """Build a mock workflow TaskNode for binding tests."""
    node = MagicMock()
    node.queue = queue
    node.priority = priority
    node.name = name
    node.fn = MagicMock()
    node.fn.task_queue_name = fn_queue
    return node


def _custom_app(*queues: tuple[str, int]) -> MagicMock:
    """Build a CUSTOM-mode mock app with the given (name, priority) queues."""
    cfgs = [_make_queue_config(n, p) for n, p in queues]
    app = _make_app(queue_mode_name='CUSTOM', custom_queues=cfgs)
    app.get_valid_queue_names.return_value = [n for n, _ in queues]
    return app


@pytest.mark.unit
class TestResolveNodeQueueAndPriority:
    """Tests for resolve_node_queue_and_priority — the single bind boundary."""

    def test_inherits_queue_priority_when_priority_unset(self) -> None:
        """Unbound priority inherits the queue's configured priority (the bug)."""
        app = _custom_app(('scraping', 30), ('normal', 50))
        node = _make_node(fn_queue='scraping')  # queue=None, priority=None

        queue, priority = resolve_node_queue_and_priority(app, node)

        assert (queue, priority) == ('scraping', 30)

    def test_explicit_priority_is_preserved(self) -> None:
        """An explicit node priority is never overridden by the queue config."""
        app = _custom_app(('scraping', 30))
        node = _make_node(fn_queue='scraping', priority=5)

        assert resolve_node_queue_and_priority(app, node) == ('scraping', 5)

    def test_queue_override_beats_fn_queue(self) -> None:
        """node.queue takes precedence over fn.task_queue_name."""
        app = _custom_app(('scraping', 30), ('normal', 50))
        node = _make_node(queue='normal', fn_queue='scraping')

        assert resolve_node_queue_and_priority(app, node) == ('normal', 50)

    def test_default_queue_when_nothing_set(self) -> None:
        """No queue anywhere resolves to 'default' (DEFAULT mode → priority 100)."""
        app = _make_app(queue_mode_name='DEFAULT')
        node = _make_node()

        assert resolve_node_queue_and_priority(app, node) == ('default', 100)

    def test_custom_mode_unknown_queue_raises(self) -> None:
        """CUSTOM mode + queue not in custom_queues → TASK_INVALID_QUEUE."""
        app = _custom_app(('scraping', 30))
        node = _make_node(queue='ghost')

        with pytest.raises(ConfigurationError) as exc_info:
            resolve_node_queue_and_priority(app, node)

        assert exc_info.value.code == ErrorCode.TASK_INVALID_QUEUE

    def test_default_mode_non_default_queue_raises(self) -> None:
        """DEFAULT mode + non-'default' queue → CONFIG_INVALID_QUEUE_MODE."""
        app = _make_app(queue_mode_name='DEFAULT')
        node = _make_node(queue='scraping')

        with pytest.raises(ConfigurationError) as exc_info:
            resolve_node_queue_and_priority(app, node)

        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_QUEUE_MODE


@pytest.mark.unit
class TestBindTaskNode:
    """Tests for bind_task_node — produces a BoundTaskNode."""

    def test_returns_bound_node_with_concrete_values(self) -> None:
        """bind_task_node binds an unresolved node to concrete queue/priority."""
        app = _custom_app(('scraping', 30))
        node = _make_node(fn_queue='scraping')

        bound = bind_task_node(app, node)

        assert isinstance(bound, BoundTaskNode)
        assert (bound.queue, bound.priority) == ('scraping', 30)
        assert bound.node is node

    def test_propagates_configuration_error(self) -> None:
        """An invalid queue surfaces as ConfigurationError, not a guessed value."""
        app = _custom_app(('scraping', 30))
        node = _make_node(queue='ghost')

        with pytest.raises(ConfigurationError) as exc_info:
            bind_task_node(app, node)

        assert exc_info.value.code == ErrorCode.TASK_INVALID_QUEUE


# =============================================================================
# TaskHandle._error_result
# =============================================================================


@pytest.mark.unit
class TestTaskHandleErrorResult:
    """Tests for TaskHandle._error_result."""

    def test_creates_error_result_and_caches_it(self) -> None:
        """_error_result creates a TaskError result and marks fetched."""
        handle: TaskHandle[int] = TaskHandle('task-1')

        result = handle._error_result(
            error_code=OperationalErrorCode.BROKER_ERROR,
            message='boom',
            data={'key': 'val'},
        )

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == OperationalErrorCode.BROKER_ERROR
        assert result.err.message == 'boom'
        assert handle._result_fetched is True
        assert handle._cached_result is result


# =============================================================================
# TaskHandle.get (sync)
# =============================================================================


@pytest.mark.unit
class TestTaskHandleGet:
    """Tests for TaskHandle.get (blocking)."""

    def test_cached_result_returned_directly(self) -> None:
        """When result is already fetched and cached, returns it."""
        handle: TaskHandle[int] = TaskHandle('t-1')
        cached = TaskResult[int, TaskError](ok=42)
        handle._cached_result = cached
        handle._result_fetched = True

        result = handle.get()

        assert result.is_ok()
        assert result.ok == 42

    def test_cached_none_after_fetch_returns_result_not_available(self) -> None:
        """Fetched flag True but cache is None returns RESULT_NOT_AVAILABLE."""
        handle: TaskHandle[int] = TaskHandle('t-2')
        handle._result_fetched = True
        handle._cached_result = None

        result = handle.get()

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == RetrievalCode.RESULT_NOT_AVAILABLE

    def test_no_broker_mode_no_cache_returns_result_not_available(self) -> None:
        """Without broker mode and no cached result, returns RESULT_NOT_AVAILABLE."""
        handle: TaskHandle[int] = TaskHandle('t-3', broker_mode=False)

        result = handle.get()

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == RetrievalCode.RESULT_NOT_AVAILABLE

    def test_broker_mode_success(self) -> None:
        """Broker mode: terminal envelope decodes via ok_type and caches.

        Strict-serde phase 6: TaskHandle.get pulls a RawResultRecord from
        ``broker.get_raw_result_record`` and runs the typed decode itself.
        """
        from horsies.core.brokers.result_types import RawResultRecord
        from horsies.core.types.status import TaskStatus

        app = _make_app()
        broker = MagicMock()
        broker.get_raw_result_record.return_value = Ok(RawResultRecord(
            task_id='t-4',
            task_name='my_task',
            status=TaskStatus.COMPLETED,
            raw_result={
                '__h_task_result__': True,
                'ok': 99,
                'err': None,
            },
        ))
        app.get_broker.return_value = broker

        handle: TaskHandle[int] = TaskHandle(
            't-4', app=app, broker_mode=True, ok_type=int,
        )

        result = handle.get(timeout_ms=5000)

        assert result.is_ok()
        assert result.ok == 99
        assert handle._result_fetched is True
        broker.get_raw_result_record.assert_called_once_with('t-4', 5000)

    def test_broker_mode_exception_returns_broker_error(self) -> None:
        """Broker mode: exception from broker fetch returns BROKER_ERROR."""
        app = _make_app()
        broker = MagicMock()
        broker.get_raw_result_record.side_effect = ConnectionError('db down')
        app.get_broker.return_value = broker

        handle: TaskHandle[int] = TaskHandle(
            't-5', app=app, broker_mode=True, ok_type=int,
        )

        result = handle.get()

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == OperationalErrorCode.BROKER_ERROR
        assert result.err.exception is not None

    def test_wait_timeout_not_cached(self) -> None:
        """WAIT_TIMEOUT is transient; subsequent get() must re-query.

        Non-terminal status + raw_result=None → WAIT_TIMEOUT at the
        handle layer; the cache stays empty so the next get() call
        revisits the broker.
        """
        from horsies.core.brokers.result_types import RawResultRecord
        from horsies.core.types.status import TaskStatus

        app = _make_app()
        broker = MagicMock()
        broker.get_raw_result_record.side_effect = [
            Ok(RawResultRecord(
                task_id='t-6',
                task_name='my_task',
                status=TaskStatus.RUNNING,
                raw_result=None,
            )),
            Ok(RawResultRecord(
                task_id='t-6',
                task_name='my_task',
                status=TaskStatus.COMPLETED,
                raw_result={
                    '__h_task_result__': True,
                    'ok': 42,
                    'err': None,
                },
            )),
        ]
        app.get_broker.return_value = broker

        handle: TaskHandle[int] = TaskHandle(
            't-6', app=app, broker_mode=True, ok_type=int,
        )

        first = handle.get(timeout_ms=1000)
        assert first.is_err()
        assert first.err is not None
        assert first.err.error_code == RetrievalCode.WAIT_TIMEOUT
        assert handle._result_fetched is False

        second = handle.get(timeout_ms=5000)
        assert second.is_ok()
        assert second.ok == 42
        assert handle._result_fetched is True

        assert broker.get_raw_result_record.call_count == 2


# =============================================================================
# TaskHandle.get_async
# =============================================================================


@pytest.mark.unit
class TestTaskHandleGetAsync:
    """Tests for TaskHandle.get_async."""

    @pytest.mark.asyncio
    async def test_cached_result_returned_directly(self) -> None:
        """Cached result is returned without broker call."""
        handle: TaskHandle[str] = TaskHandle('t-1')
        cached = TaskResult[str, TaskError](ok='hello')
        handle._cached_result = cached
        handle._result_fetched = True

        result = await handle.get_async()

        assert result.is_ok()
        assert result.ok == 'hello'

    @pytest.mark.asyncio
    async def test_cached_none_after_fetch_returns_result_not_available(self) -> None:
        """Fetched but None cache returns RESULT_NOT_AVAILABLE."""
        handle: TaskHandle[str] = TaskHandle('t-2')
        handle._result_fetched = True
        handle._cached_result = None

        result = await handle.get_async()

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == RetrievalCode.RESULT_NOT_AVAILABLE

    @pytest.mark.asyncio
    async def test_no_broker_mode_returns_result_not_available(self) -> None:
        """Without broker mode, returns RESULT_NOT_AVAILABLE."""
        handle: TaskHandle[str] = TaskHandle('t-3', broker_mode=False)

        result = await handle.get_async()

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == RetrievalCode.RESULT_NOT_AVAILABLE

    @pytest.mark.asyncio
    async def test_broker_mode_success(self) -> None:
        """Broker mode: terminal envelope decodes via ok_type and caches."""
        from horsies.core.brokers.result_types import RawResultRecord
        from horsies.core.types.status import TaskStatus

        app = _make_app()
        broker = MagicMock()
        broker.get_raw_result_record_async = AsyncMock(return_value=Ok(
            RawResultRecord(
                task_id='t-4',
                task_name='my_task',
                status=TaskStatus.COMPLETED,
                raw_result={
                    '__h_task_result__': True,
                    'ok': 'async-ok',
                    'err': None,
                },
            ),
        ))
        app.get_broker.return_value = broker

        handle: TaskHandle[str] = TaskHandle(
            't-4', app=app, broker_mode=True, ok_type=str,
        )

        result = await handle.get_async()

        assert result.is_ok()
        assert result.ok == 'async-ok'
        assert handle._result_fetched is True

    @pytest.mark.asyncio
    async def test_broker_mode_exception_returns_broker_error(self) -> None:
        """Broker mode: exception from broker fetch returns BROKER_ERROR."""
        app = _make_app()
        broker = MagicMock()
        broker.get_raw_result_record_async = AsyncMock(
            side_effect=ConnectionError('oops'),
        )
        app.get_broker.return_value = broker

        handle: TaskHandle[str] = TaskHandle(
            't-5', app=app, broker_mode=True, ok_type=str,
        )

        result = await handle.get_async()

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == OperationalErrorCode.BROKER_ERROR

    @pytest.mark.asyncio
    async def test_cancelled_error_propagates(self) -> None:
        """CancelledError is re-raised, not caught as BROKER_ERROR."""
        app = _make_app()
        broker = MagicMock()
        broker.get_raw_result_record_async = AsyncMock(
            side_effect=asyncio.CancelledError,
        )
        app.get_broker.return_value = broker

        handle: TaskHandle[str] = TaskHandle(
            't-6', app=app, broker_mode=True, ok_type=str,
        )

        with pytest.raises(asyncio.CancelledError):
            await handle.get_async()

    @pytest.mark.asyncio
    async def test_wait_timeout_not_cached(self) -> None:
        """WAIT_TIMEOUT is transient; subsequent get_async() must re-query.

        Non-terminal status + raw_result=None → WAIT_TIMEOUT at the
        handle layer; the cache stays empty so the next call revisits
        the broker.
        """
        from horsies.core.brokers.result_types import RawResultRecord
        from horsies.core.types.status import TaskStatus

        app = _make_app()
        broker = MagicMock()
        broker.get_raw_result_record_async = AsyncMock(side_effect=[
            Ok(RawResultRecord(
                task_id='t-7',
                task_name='my_task',
                status=TaskStatus.RUNNING,
                raw_result=None,
            )),
            Ok(RawResultRecord(
                task_id='t-7',
                task_name='my_task',
                status=TaskStatus.COMPLETED,
                raw_result={
                    '__h_task_result__': True,
                    'ok': 'done',
                    'err': None,
                },
            )),
        ])
        app.get_broker.return_value = broker

        handle: TaskHandle[str] = TaskHandle(
            't-7', app=app, broker_mode=True, ok_type=str,
        )

        first = await handle.get_async(timeout_ms=1000)
        assert first.is_err()
        assert first.err is not None
        assert first.err.error_code == RetrievalCode.WAIT_TIMEOUT
        assert handle._result_fetched is False

        second = await handle.get_async(timeout_ms=5000)
        assert second.is_ok()
        assert second.ok == 'done'
        assert handle._result_fetched is True

        assert broker.get_raw_result_record_async.call_count == 2


# =============================================================================
# TaskHandle._record_to_task_result — phase 5/6 envelope guard
# =============================================================================


@pytest.mark.unit
class TestTaskHandleRecordToTaskResult:
    """Direct unit tests for the strict-serde phase 6 envelope-decode
    routine. Constructs ``BrokerResult[RawResultRecord]`` fixtures
    rather than mocking the broker, so the envelope-shape contract is
    exercised independently of the broker layer.

    Locks in the regressions found in the post-PR review:

    1. Err-fast-path must NOT bypass ``validate_task_result_envelope``.
       A payload like ``{"__h_task_result__": True, "err": {...}}`` is
       missing ``ok`` and must fail closed before the err slot is
       touched.

    2. ``decode_task_result``-driven path must reject malformed envelopes
       on the ok slot for typed handles too.

    3. A terminal record with ``raw_result=None`` must map to
       ``RESULT_NOT_AVAILABLE`` (terminal, cacheable) — not
       ``WAIT_TIMEOUT``, which is transient.
    """

    @staticmethod
    def _record(
        *,
        status: Any,
        raw_result: Any,
        task_name: str = 'my_task',
        task_id: str = 'task-x',
    ) -> Any:
        from horsies.core.brokers.result_types import RawResultRecord

        return RawResultRecord(
            task_id=task_id,
            task_name=task_name,
            status=status,
            raw_result=raw_result,
        )

    def test_malformed_envelope_err_fast_rejected(self) -> None:
        """Envelope missing the ``ok`` key fails closed on err-fast-path.

        Regression: an earlier implementation read ``envelope['err']``
        before validating shape, so a malformed envelope (marker present,
        ``ok`` missing, ``err`` populated) decoded via the err-fast-path
        even though the payload was illegal.
        """
        from horsies.core.types.result import Ok
        from horsies.core.types.status import TaskStatus

        # Marker + err slot, but ``ok`` key absent → invalid envelope.
        malformed = {
            '__h_task_result__': True,
            'err': {
                'error_code': {'__builtin_task_code__': 'BROKER_ERROR'},
                'message': 'sneak',
                'data': None,
                'exception': None,
            },
        }
        handle: TaskHandle[int] = TaskHandle('task-x', ok_type=int)
        broker_result = Ok(self._record(
            status=TaskStatus.FAILED, raw_result=malformed,
        ))

        result = handle._record_to_task_result(broker_result, timeout_ms=None)

        assert result.is_err()
        assert result.err is not None
        assert (
            result.err.error_code
            == OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        )

    def test_envelope_with_both_slots_populated_rejected(self) -> None:
        """Both ``ok`` and ``err`` populated → invalid envelope; can't
        smuggle a typed value past the err-fast-path."""
        from horsies.core.types.result import Ok
        from horsies.core.types.status import TaskStatus

        malformed = {
            '__h_task_result__': True,
            'ok': 42,
            'err': {
                'error_code': {'__builtin_task_code__': 'BROKER_ERROR'},
                'message': 'both',
                'data': None,
                'exception': None,
            },
        }
        handle: TaskHandle[int] = TaskHandle('task-x', ok_type=int)
        broker_result = Ok(self._record(
            status=TaskStatus.FAILED, raw_result=malformed,
        ))

        result = handle._record_to_task_result(broker_result, timeout_ms=None)

        assert result.is_err()
        assert result.err is not None
        assert (
            result.err.error_code
            == OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        )

    def test_envelope_missing_marker_rejected(self) -> None:
        """Plain dict without the marker isn't a TaskResult envelope."""
        from horsies.core.types.result import Ok
        from horsies.core.types.status import TaskStatus

        raw = {'ok': 1, 'err': None}
        handle: TaskHandle[int] = TaskHandle('task-x', ok_type=int)
        broker_result = Ok(self._record(
            status=TaskStatus.COMPLETED, raw_result=raw,
        ))

        result = handle._record_to_task_result(broker_result, timeout_ms=None)

        assert result.is_err()
        assert result.err is not None
        assert (
            result.err.error_code
            == OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        )

    def test_terminal_status_with_none_payload_is_result_not_available(
        self,
    ) -> None:
        """Terminal row with empty result column → cacheable
        ``RESULT_NOT_AVAILABLE``, not transient ``WAIT_TIMEOUT``.

        Regression: an earlier implementation always mapped
        ``raw_result=None`` to ``WAIT_TIMEOUT`` even for terminal rows
        (engine never wrote a payload), which produced wrong retry
        semantics.
        """
        from horsies.core.types.result import Ok
        from horsies.core.types.status import TaskStatus

        handle: TaskHandle[int] = TaskHandle('task-x', ok_type=int)
        broker_result = Ok(self._record(
            status=TaskStatus.COMPLETED, raw_result=None,
        ))

        result = handle._record_to_task_result(broker_result, timeout_ms=None)

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == RetrievalCode.RESULT_NOT_AVAILABLE

    def test_non_terminal_status_with_none_payload_is_wait_timeout(self) -> None:
        """Non-terminal row + raw_result=None → ``WAIT_TIMEOUT``
        (timeout fired before terminalization)."""
        from horsies.core.types.result import Ok
        from horsies.core.types.status import TaskStatus

        handle: TaskHandle[int] = TaskHandle('task-x', ok_type=int)
        broker_result = Ok(self._record(
            status=TaskStatus.RUNNING, raw_result=None,
        ))

        result = handle._record_to_task_result(broker_result, timeout_ms=100)

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == RetrievalCode.WAIT_TIMEOUT

    def test_err_fast_path_polymorphic_sub_workflow_error(self) -> None:
        """End-to-end: a parent task's err slot carrying a
        ``SubWorkflowError`` shape (the engine's emit) round-trips
        through ``_record_to_task_result`` as a ``SubWorkflowError``,
        not a plain ``TaskError``."""
        from horsies.core.models.tasks import (
            OperationalErrorCode as _OpCode,
            SubWorkflowError,
        )
        from horsies.core.models.workflow.context import SubWorkflowSummary
        from horsies.core.models.workflow.enums import WorkflowStatus
        from horsies.core.types.result import Ok
        from horsies.core.types.status import TaskStatus

        original = SubWorkflowError(
            error_code=_OpCode.UNHANDLED_EXCEPTION,
            message='child failed',
            sub_workflow_id='wf-1',
            sub_workflow_summary=SubWorkflowSummary(
                status=WorkflowStatus.FAILED,
                output=None,
                total_tasks=1,
                completed_tasks=0,
                failed_tasks=1,
                skipped_tasks=0,
                error_summary='downstream',
            ),
        )
        wire = {
            '__h_task_result__': True,
            'ok': None,
            'err': original.model_dump(mode='json'),
        }
        handle: TaskHandle[int] = TaskHandle('parent-task', ok_type=int)
        broker_result = Ok(self._record(
            status=TaskStatus.FAILED, raw_result=wire,
        ))

        result = handle._record_to_task_result(broker_result, timeout_ms=None)

        assert result.is_err()
        assert isinstance(result.err, SubWorkflowError)
        assert result.err is not None
        sub = result.err
        assert isinstance(sub, SubWorkflowError)
        assert sub.sub_workflow_id == 'wf-1'
        assert sub.sub_workflow_summary.error_summary == 'downstream'


# =============================================================================
# TaskHandle.info / info_async
# =============================================================================


@pytest.mark.unit
class TestTaskHandleInfo:
    """Tests for TaskHandle.info and info_async."""

    def test_info_without_broker_mode_returns_no_broker_error(self) -> None:
        """info() without broker mode returns Err(NO_BROKER)."""
        handle: TaskHandle[int] = TaskHandle('t-1', broker_mode=False)

        result = handle.info()

        assert result.is_err()
        err = result.err_value
        assert err.code == BrokerErrorCode.NO_BROKER
        assert 'requires a broker-backed' in err.message
        assert err.retryable is False

    @pytest.mark.asyncio
    async def test_info_async_without_broker_mode_returns_no_broker_error(self) -> None:
        """info_async() without broker mode returns Err(NO_BROKER)."""
        handle: TaskHandle[int] = TaskHandle('t-2', broker_mode=False)

        result = await handle.info_async()

        assert result.is_err()
        err = result.err_value
        assert err.code == BrokerErrorCode.NO_BROKER
        assert 'requires a broker-backed' in err.message
        assert err.retryable is False

    def test_info_with_broker_delegates(self) -> None:
        """info() with broker mode delegates to ``app.get_task_info``.

        Strict-serde phase 6: TaskHandle.info routes through Horsies so
        the typed ``decoded_result`` field gets populated. The handle no
        longer talks to ``broker.get_task_info`` directly.
        """
        app = _make_app()
        sentinel = object()
        app.get_task_info = MagicMock(return_value=sentinel)

        handle: TaskHandle[int] = TaskHandle('t-3', app=app, broker_mode=True)

        result = handle.info(include_result=True)

        assert result is sentinel
        app.get_task_info.assert_called_once_with(
            't-3',
            include_result=True,
            include_failed_reason=False,
            include_attempts=False,
        )

    @pytest.mark.asyncio
    async def test_info_async_with_broker_delegates(self) -> None:
        """info_async() routes through ``app.get_task_info_async``."""
        app = _make_app()
        sentinel = object()
        app.get_task_info_async = AsyncMock(return_value=sentinel)

        handle: TaskHandle[int] = TaskHandle('t-4', app=app, broker_mode=True)

        result = await handle.info_async(include_failed_reason=True)

        assert result is sentinel
        app.get_task_info_async.assert_called_once_with(
            't-4',
            include_result=False,
            include_failed_reason=True,
            include_attempts=False,
        )


# =============================================================================
# TaskHandle.set_immediate_result
# =============================================================================


@pytest.mark.unit
class TestTaskHandleSetImmediateResult:
    """Tests for TaskHandle.set_immediate_result."""

    def test_sets_cache_and_flag(self) -> None:
        """set_immediate_result sets cached result and fetched flag."""
        handle: TaskHandle[int] = TaskHandle('t-1')
        ok_result = TaskResult[int, TaskError](ok=42)

        handle.set_immediate_result(ok_result)

        assert handle._cached_result is ok_result
        assert handle._result_fetched is True


# =============================================================================
# create_task_wrapper — validation errors
# =============================================================================


@pytest.mark.unit
class TestCreateTaskWrapperValidation:
    """Tests for create_task_wrapper type-annotation validation."""

    def test_no_return_type_raises(self) -> None:
        """Function with no return annotation raises TASK_NO_RETURN_TYPE."""
        def bad_fn(x: int):  # type: ignore[no-untyped-def]
            pass

        app = _make_app()
        with pytest.raises(TaskDefinitionError) as exc_info:
            create_task_wrapper(bad_fn, app, 'test.bad_fn')  # type: ignore[arg-type]

        assert exc_info.value.code == ErrorCode.TASK_NO_RETURN_TYPE

    def test_wrong_return_type_raises(self) -> None:
        """Function returning non-TaskResult raises TASK_INVALID_RETURN_TYPE."""
        def bad_fn(x: int) -> int:
            return x

        app = _make_app()
        with pytest.raises(TaskDefinitionError) as exc_info:
            create_task_wrapper(bad_fn, app, 'test.bad_fn')  # type: ignore[arg-type]

        assert exc_info.value.code == ErrorCode.TASK_INVALID_RETURN_TYPE

    def test_wrong_type_args_count_raises(self) -> None:
        """TaskResult with wrong number of type args raises TASK_INVALID_RETURN_TYPE."""
        # Use raw TaskResult without type params (get_args returns ())
        def bad_fn(*, x: int) -> TaskResult:  # type: ignore[type-arg]
            return TaskResult(ok=x)  # pyright: ignore[reportUnknownVariableType]

        app = _make_app()
        with pytest.raises(TaskDefinitionError) as exc_info:
            create_task_wrapper(bad_fn, app, 'test.bad_fn')  # type: ignore[arg-type]

        assert exc_info.value.code == ErrorCode.TASK_INVALID_RETURN_TYPE

    def test_valid_function_creates_callable_wrapper(self) -> None:
        """Valid function produces a wrapper with correct task_name."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        assert wrapper.task_name == 'test.good_fn'
        assert callable(wrapper)

    def test_factory_task_can_close_over_helper_function(self) -> None:
        """A factory-local helper function is a valid task dependency."""
        def make_task():
            def helper(value: int) -> int:
                return value + 1

            def good_fn(*, x: int) -> TaskResult[int, TaskError]:
                return TaskResult(ok=helper(x))

            return good_fn

        app = _make_app()
        wrapper = create_task_wrapper(make_task(), app, 'test.helper_closure')

        result = wrapper(x=41)
        assert result.is_ok()
        assert result.ok == 42

    def test_factory_task_can_close_over_lambda_helper(self) -> None:
        """A factory-local lambda helper is a valid task dependency."""
        def make_task():
            helper = lambda value: value + 1  # noqa: E731

            def good_fn(*, x: int) -> TaskResult[int, TaskError]:
                return TaskResult(ok=helper(x))

            return good_fn

        app = _make_app()
        wrapper = create_task_wrapper(make_task(), app, 'test.lambda_helper_closure')

        result = wrapper(x=41)
        assert result.is_ok()
        assert result.ok == 42

    def test_factory_task_named_wrapper_can_close_over_helper(self) -> None:
        """Function-named-wrapper does not imply a decorator wrapper.

        Originally exercised `*values: int` to confirm typed variadics
        registered cleanly; strict-serde now rejects variadics at v1
        (see TestVariadics in test_signature_check.py). Test still
        asserts the original concern — a factory-returned `wrapper`
        closing over a helper is accepted — using kwargs-only.
        """
        def make_task():
            def helper(values: tuple[int, ...]) -> int:
                return sum(values)

            def wrapper(*, values: tuple[int, int]) -> TaskResult[int, TaskError]:
                return TaskResult(ok=helper(values))

            return wrapper

        app = _make_app()
        wrapper = create_task_wrapper(make_task(), app, 'test.kwarg_helper_closure')

        result = wrapper(values=(20, 22))
        assert result.is_ok()
        assert result.ok == 42

    def test_predecorated_with_wraps_rejected(self) -> None:
        """Wrapper chain via __wrapped__ is rejected."""

        def passthrough(fn):  # type: ignore[no-untyped-def]
            @wraps(fn)
            def wrapper(*args, **kwargs):  # type: ignore[no-untyped-def]
                return fn(*args, **kwargs)

            return wrapper

        @passthrough
        def bad_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        with pytest.raises(TaskDefinitionError) as exc_info:
            create_task_wrapper(bad_fn, app, 'test.bad_fn')

        assert exc_info.value.code == ErrorCode.TASK_PREDECORATED_NOT_SUPPORTED

    def test_decorator_without_wraps_missing_return_type_rejected(self) -> None:
        """A no-wraps wrapper is rejected if it no longer satisfies the task contract."""

        def passthrough(fn):  # type: ignore[no-untyped-def]
            def wrapper(*args, **kwargs):  # type: ignore[no-untyped-def]
                return fn(*args, **kwargs)

            return wrapper

        @passthrough
        def bad_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        with pytest.raises(TaskDefinitionError) as exc_info:
            create_task_wrapper(bad_fn, app, 'test.bad_fn')  # type: ignore[arg-type]

        assert exc_info.value.code == ErrorCode.TASK_NO_RETURN_TYPE


# =============================================================================
# create_task_wrapper — wrapped_function execution paths
# =============================================================================


@pytest.mark.unit
class TestCreateTaskWrapperExecution:
    """Tests for wrapped_function execution (calling the wrapper)."""

    def test_successful_execution(self) -> None:
        """Calling wrapper with valid function returns TaskResult(ok=...)."""
        def add_one(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x + 1)

        app = _make_app()
        wrapper = create_task_wrapper(add_one, app, 'test.add_one')

        result = wrapper(x=5)

        assert result.is_ok()
        assert result.ok == 6

    def test_returns_none_produces_task_exception(self) -> None:
        """Function returning None produces TASK_EXCEPTION error."""
        def bad_fn(*, x: int) -> TaskResult[int, TaskError]:
            return None  # type: ignore[return-value]

        app = _make_app()
        wrapper = create_task_wrapper(bad_fn, app, 'test.bad_fn')

        result = wrapper(x=1)

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == OperationalErrorCode.TASK_EXCEPTION
        assert 'returned None' in (result.err.message or '')

    def test_return_type_mismatch_produces_error(self) -> None:
        """Returning wrong ok type produces RETURN_TYPE_MISMATCH."""
        def bad_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok='not-an-int')  # type: ignore[arg-type]

        app = _make_app()
        wrapper = create_task_wrapper(bad_fn, app, 'test.bad_fn')

        result = wrapper(x=1)

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == ContractCode.RETURN_TYPE_MISMATCH

    def test_coercible_ok_value_returns_coerced(self) -> None:
        """A lax-coercible ok value is returned coerced, matching the wire.

        Ok('5') for a declared int passes lax validation as 5; the
        in-process caller must observe the same 5 a wire consumer decodes,
        not the original '5'.
        """
        def coercible_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok='5')  # type: ignore[arg-type]

        app = _make_app()
        wrapper = create_task_wrapper(coercible_fn, app, 'test.coercible')

        result = wrapper(x=1)

        assert result.is_ok()
        assert result.ok == 5
        assert isinstance(result.ok, int)

    def test_keyboard_interrupt_propagates(self) -> None:
        """KeyboardInterrupt re-raises for graceful worker shutdown."""
        def interrupting_fn(*, x: int) -> TaskResult[int, TaskError]:
            raise KeyboardInterrupt

        app = _make_app()
        wrapper = create_task_wrapper(interrupting_fn, app, 'test.interrupt')

        with pytest.raises(KeyboardInterrupt):
            wrapper(x=1)

    def test_workflow_context_missing_id_error(self) -> None:
        """WorkflowContextMissingIdError produces WORKFLOW_CTX_MISSING_ID."""
        def ctx_fn(*, x: int) -> TaskResult[int, TaskError]:
            raise WorkflowContextMissingIdError('missing node id')

        app = _make_app()
        wrapper = create_task_wrapper(ctx_fn, app, 'test.ctx_fn')

        result = wrapper(x=1)

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == ContractCode.WORKFLOW_CTX_MISSING_ID

    def test_generic_exception_uses_exception_mapper(self) -> None:
        """Unhandled exception resolved via exception_mapper chain."""

        class CustomError(Exception):
            pass

        def failing_fn(*, x: int) -> TaskResult[int, TaskError]:
            raise CustomError('kaboom')

        mapper: ExceptionMapper = {CustomError: 'CUSTOM_MAPPED'}
        app = _make_app()
        wrapper = create_task_wrapper(
            failing_fn,
            app,
            'test.failing_fn',
            exception_mapper=mapper,
        )

        result = wrapper(x=1)

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == 'CUSTOM_MAPPED'

    def test_generic_exception_falls_back_to_global_default(self) -> None:
        """Without mapper match, uses global default_unhandled_error_code."""
        def failing_fn(*, x: int) -> TaskResult[int, TaskError]:
            raise ValueError('oops')

        app = _make_app(default_unhandled_error_code='UNHANDLED_EXCEPTION')
        wrapper = create_task_wrapper(failing_fn, app, 'test.failing_fn')

        result = wrapper(x=1)

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == 'UNHANDLED_EXCEPTION'
        assert 'ValueError' in (result.err.message or '')


# =============================================================================
# create_task_wrapper — introspection (__wrapped__ / signature)
# =============================================================================


@pytest.mark.unit
class TestCreateTaskWrapperIntrospection:
    """The wrapper must expose the original function's signature."""

    def test_signature_reflects_original_function(self) -> None:
        """inspect.signature resolves to the task's real params, not (*args, **kwargs)."""
        def my_task(*, x: int, label: str = 'hi') -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        wrapper = create_task_wrapper(my_task, _make_app(), 'test.my_task')

        sig = inspect.signature(wrapper)
        assert list(sig.parameters) == ['x', 'label']
        assert sig.parameters['x'].kind is inspect.Parameter.KEYWORD_ONLY
        assert sig.parameters['label'].kind is inspect.Parameter.KEYWORD_ONLY
        assert sig.parameters['label'].default == 'hi'

    def test_wrapped_points_to_original_function(self) -> None:
        """__wrapped__ is set so inspect.unwrap reaches the raw function."""
        def my_task(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        wrapper = create_task_wrapper(my_task, _make_app(), 'test.my_task')

        assert wrapper.__wrapped__ is my_task
        assert inspect.unwrap(wrapper) is my_task


# =============================================================================
# create_task_wrapper — send() paths
# =============================================================================


@pytest.mark.unit
class TestCreateTaskWrapperSend:
    """Tests for wrapper.send() method."""

    def test_send_suppressed_returns_send_suppressed(self) -> None:
        """When sends are suppressed, returns Err(SEND_SUPPRESSED)."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app(suppress_sends=True)
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.send(x=1)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.SEND_SUPPRESSED
        assert result.err_value.retryable is False
        assert result.err_value.task_id is None
        assert result.err_value.payload is None

    def test_send_queue_validation_failure_returns_validation_error(self) -> None:
        """Queue validation error returns Err(VALIDATION_FAILED)."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        app.validate_queue_name.side_effect = ConfigurationError(
            message='bad queue',
            code=ErrorCode.TASK_INVALID_QUEUE,
        )
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.send(x=1)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.VALIDATION_FAILED
        assert result.err_value.retryable is False

    def test_send_success_returns_ok_with_handle(self) -> None:
        """Successful send returns Ok(TaskHandle) with broker_mode=True."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue.return_value = Ok('task-abc')
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.send(x=1)

        assert is_ok(result)
        handle = result.ok_value
        assert handle.task_id == 'task-abc'
        assert handle._broker_mode is True

    def test_with_options_good_until_applies_to_this_send(self) -> None:
        """with_options(good_until=...) sets the expiry on the concrete send."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue.return_value = Ok('task-abc')
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(
            good_fn,
            app,
            'test.good_fn',
            TaskOptions(task_name='test.good_fn'),
        )
        deadline = datetime(2030, 1, 1, tzinfo=timezone.utc)

        result = wrapper.with_options(good_until=deadline).send(x=1)

        assert is_ok(result)
        call_kwargs = broker.enqueue.call_args.kwargs
        assert call_kwargs['good_until'] == deadline
        assert json.loads(call_kwargs['task_options'])['good_until'] == deadline.isoformat()

    def test_with_options_naive_good_until_returns_validation_error(self) -> None:
        """Per-send good_until keeps the same timezone-aware datetime contract."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')
        naive_deadline = datetime(2030, 1, 1)

        result = wrapper.with_options(good_until=naive_deadline).send(x=1)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.VALIDATION_FAILED
        assert 'timezone-aware' in result.err_value.message
        broker.enqueue.assert_not_called()

    def test_with_options_none_clears_existing_task_options_good_until(self) -> None:
        """with_options(good_until=None) explicitly clears stale legacy defaults."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue.return_value = Ok('task-abc')
        app.get_broker.return_value = broker
        stale_deadline = datetime(2030, 1, 1, tzinfo=timezone.utc)
        wrapper = create_task_wrapper(
            good_fn,
            app,
            'test.good_fn',
            TaskOptions(task_name='test.good_fn', good_until=stale_deadline),
        )

        result = wrapper.with_options(good_until=None).send(x=1)

        assert is_ok(result)
        call_kwargs = broker.enqueue.call_args.kwargs
        assert call_kwargs['good_until'] is None
        assert json.loads(call_kwargs['task_options'])['good_until'] is None

    def test_send_broker_failure_returns_enqueue_error_with_payload(self) -> None:
        """Broker Err result during enqueue returns Err(ENQUEUE_FAILED) with payload."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue.return_value = Err(BrokerOperationError(
            code=BrokerErrorCode.ENQUEUE_FAILED,
            message='db gone',
            retryable=True,
            exception=ConnectionError('db gone'),
        ))
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.send(x=1)

        assert is_err(result)
        err = result.err_value
        assert err.code == TaskSendErrorCode.ENQUEUE_FAILED
        assert err.retryable is True
        assert err.task_id is not None
        assert err.payload is not None
        assert err.payload.task_name == 'test.good_fn'


# =============================================================================
# create_task_wrapper — send_async() paths
# =============================================================================


@pytest.mark.unit
class TestCreateTaskWrapperSendAsync:
    """Tests for wrapper.send_async() method."""

    @pytest.mark.asyncio
    async def test_send_async_suppressed(self) -> None:
        """When sends suppressed, send_async returns Err(SEND_SUPPRESSED)."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app(suppress_sends=True)
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = await wrapper.send_async(x=1)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.SEND_SUPPRESSED

    @pytest.mark.asyncio
    async def test_send_async_success(self) -> None:
        """Successful async send returns Ok(TaskHandle) with broker_mode=True."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue_async = AsyncMock(return_value=Ok('task-xyz'))
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = await wrapper.send_async(x=1)

        assert is_ok(result)
        handle = result.ok_value
        assert handle.task_id == 'task-xyz'
        assert handle._broker_mode is True

    @pytest.mark.asyncio
    async def test_with_options_send_async_sets_good_until(self) -> None:
        """with_options(good_until=...) is honored by send_async()."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue_async = AsyncMock(return_value=Ok('task-xyz'))
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(
            good_fn,
            app,
            'test.good_fn',
            TaskOptions(task_name='test.good_fn'),
        )
        deadline = datetime(2030, 1, 1, tzinfo=timezone.utc)

        result = await wrapper.with_options(good_until=deadline).send_async(x=1)

        assert is_ok(result)
        call_kwargs = broker.enqueue_async.call_args.kwargs
        assert call_kwargs['good_until'] == deadline
        assert json.loads(call_kwargs['task_options'])['good_until'] == deadline.isoformat()

    @pytest.mark.asyncio
    async def test_send_async_broker_failure_returns_enqueue_error(self) -> None:
        """Broker Err result during async enqueue returns Err(ENQUEUE_FAILED)."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue_async = AsyncMock(return_value=Err(BrokerOperationError(
            code=BrokerErrorCode.ENQUEUE_FAILED,
            message='fail',
            retryable=False,
            exception=RuntimeError('fail'),
        )))
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = await wrapper.send_async(x=1)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.ENQUEUE_FAILED
        assert result.err_value.retryable is False

    @pytest.mark.asyncio
    async def test_send_async_enqueue_raises_returns_enqueue_error(self) -> None:
        """Async enqueue non-connection exception returns non-retryable Err(ENQUEUE_FAILED)."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue_async = AsyncMock(side_effect=RuntimeError('loop dead'))
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = await wrapper.send_async(x=1)

        assert is_err(result)
        err = result.err_value
        assert err.code == TaskSendErrorCode.ENQUEUE_FAILED
        assert err.retryable is False
        assert isinstance(err.exception, RuntimeError)

    @pytest.mark.asyncio
    async def test_send_async_queue_validation_failure(self) -> None:
        """Queue validation error in send_async returns Err(VALIDATION_FAILED)."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        app.validate_queue_name.side_effect = ConfigurationError(
            message='bad queue',
            code=ErrorCode.TASK_INVALID_QUEUE,
        )
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = await wrapper.send_async(x=1)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.VALIDATION_FAILED


# =============================================================================
# create_task_wrapper — schedule() paths
# =============================================================================


@pytest.mark.unit
class TestCreateTaskWrapperSchedule:
    """Tests for wrapper.schedule() method."""

    def test_schedule_suppressed_returns_send_suppressed(self) -> None:
        """When sends suppressed, schedule() returns SEND_SUPPRESSED error."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app(suppress_sends=True)
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.schedule(60, x=1)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.SEND_SUPPRESSED

    def test_schedule_success(self) -> None:
        """Successful schedule returns broker-mode handle with delay."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue.return_value = Ok('sched-1')
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.schedule(60, x=1)

        assert is_ok(result)
        handle = result.ok_value
        assert handle.task_id == 'sched-1'
        assert handle._broker_mode is True
        # Verify enqueue was called with sent_at and enqueue_delay_seconds
        call_kwargs = broker.enqueue.call_args
        assert call_kwargs.kwargs.get('sent_at') is not None
        assert call_kwargs.kwargs.get('enqueue_delay_seconds') == 60

    def test_with_options_schedule_sets_good_until(self) -> None:
        """with_options(good_until=...) is honored by schedule()."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue.return_value = Ok('sched-1')
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(
            good_fn,
            app,
            'test.good_fn',
            TaskOptions(task_name='test.good_fn'),
        )
        deadline = datetime(2030, 1, 1, tzinfo=timezone.utc)

        result = wrapper.with_options(good_until=deadline).schedule(60, x=1)

        assert is_ok(result)
        call_kwargs = broker.enqueue.call_args.kwargs
        assert call_kwargs['good_until'] == deadline
        assert call_kwargs['enqueue_delay_seconds'] == 60
        assert json.loads(call_kwargs['task_options'])['good_until'] == deadline.isoformat()

    def test_schedule_broker_exception_returns_enqueue_failed(self) -> None:
        """Broker Err result during schedule returns ENQUEUE_FAILED."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue.return_value = Err(BrokerOperationError(
            code=BrokerErrorCode.ENQUEUE_FAILED,
            message='gone',
            retryable=True,
            exception=ConnectionError('gone'),
        ))
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.schedule(60, x=1)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.ENQUEUE_FAILED

    def test_schedule_queue_validation_failure(self) -> None:
        """Queue validation error in schedule returns VALIDATION_FAILED."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        app.validate_queue_name.side_effect = ConfigurationError(
            message='bad',
            code=ErrorCode.TASK_INVALID_QUEUE,
        )
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.schedule(60, x=1)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.VALIDATION_FAILED

    def test_schedule_negative_delay_rejected(self) -> None:
        """A negative delay is rejected before reaching the broker."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue.return_value = Ok('sched-1')
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.schedule(-5, x=1)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.VALIDATION_FAILED
        broker.enqueue.assert_not_called()

    def test_schedule_zero_delay_allowed(self) -> None:
        """delay=0 is valid (enqueue now) so dynamically computed delays need no branch."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue.return_value = Ok('sched-1')
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.schedule(0, x=1)

        assert is_ok(result)
        assert broker.enqueue.call_args.kwargs.get('enqueue_delay_seconds') == 0

    def test_schedule_none_delay_rejected(self) -> None:
        """A None delay returns Err(VALIDATION_FAILED), not a raw TypeError."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue.return_value = Ok('sched-1')
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.schedule(None, x=1)  # type: ignore[arg-type]

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.VALIDATION_FAILED
        broker.enqueue.assert_not_called()

    def test_schedule_string_delay_rejected(self) -> None:
        """A str delay returns Err(VALIDATION_FAILED), not a raw TypeError."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue.return_value = Ok('sched-1')
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.schedule('5', x=1)  # type: ignore[arg-type]

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.VALIDATION_FAILED
        broker.enqueue.assert_not_called()

    def test_schedule_bool_delay_rejected(self) -> None:
        """A bool delay is rejected: schedule(False) silently meaning 0 is a footgun."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue.return_value = Ok('sched-1')
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        for bad in (True, False):
            result = wrapper.schedule(bad, x=1)  # type: ignore[arg-type]
            assert is_err(result), bad
            assert result.err_value.code == TaskSendErrorCode.VALIDATION_FAILED
        broker.enqueue.assert_not_called()

    def test_schedule_float_delay_rejected(self) -> None:
        """A float delay is rejected (signature is int)."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue.return_value = Ok('sched-1')
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.schedule(2.5, x=1)  # type: ignore[arg-type]

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.VALIDATION_FAILED
        broker.enqueue.assert_not_called()

    def test_with_options_schedule_negative_delay_rejected(self) -> None:
        """The with_options(...).schedule() path validates the delay too."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue.return_value = Ok('sched-1')
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.with_options().schedule(-1, x=1)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.VALIDATION_FAILED
        broker.enqueue.assert_not_called()


# =============================================================================
# create_task_wrapper — retry_send / retry_send_async / retry_schedule
# =============================================================================


def _make_enqueue_failed_error(
    *,
    task_name: str = 'test.good_fn',
    task_id: str = 'retry-id-1',
    enqueue_delay_seconds: int | None = None,
) -> TaskSendError:
    """Build an ENQUEUE_FAILED error with full payload for retry tests."""
    from datetime import datetime, timezone
    payload = TaskSendPayload(
        task_name=task_name,
        queue_name='default',
        priority=100,
        args_json='[1]',
        kwargs_json=None,
        sent_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        good_until=None,
        enqueue_delay_seconds=enqueue_delay_seconds,
        task_options=None,
        enqueue_sha='abc123',
    )
    return TaskSendError(
        code=TaskSendErrorCode.ENQUEUE_FAILED,
        message='db gone',
        retryable=True,
        task_id=task_id,
        payload=payload,
        exception=ConnectionError('db gone'),
    )


@pytest.mark.unit
class TestRetrySend:
    """Tests for retry_send / retry_send_async / retry_schedule."""

    def test_retry_send_reuses_task_id_and_payload(self) -> None:
        """retry_send passes the same task_id, enqueue_sha, and args_json to broker."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue.return_value = Ok('retry-id-1')
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        error = _make_enqueue_failed_error()
        result = wrapper.retry_send(error)

        assert is_ok(result)
        call_kwargs = broker.enqueue.call_args.kwargs
        assert call_kwargs['task_id'] == 'retry-id-1'
        assert call_kwargs['enqueue_sha'] == 'abc123'
        assert call_kwargs['args_json'] == '[1]'
        assert call_kwargs['kwargs_json'] is None

    @pytest.mark.asyncio
    async def test_retry_send_async_reuses_task_id_and_payload(self) -> None:
        """retry_send_async passes the same task_id and payload to broker."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue_async = AsyncMock(return_value=Ok('retry-id-1'))
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        error = _make_enqueue_failed_error()
        result = await wrapper.retry_send_async(error)

        assert is_ok(result)
        call_kwargs = broker.enqueue_async.call_args.kwargs
        assert call_kwargs['task_id'] == 'retry-id-1'
        assert call_kwargs['enqueue_sha'] == 'abc123'

    def test_retry_send_only_accepts_enqueue_failed(self) -> None:
        """retry_send rejects non-ENQUEUE_FAILED codes with VALIDATION_FAILED."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        error = TaskSendError(
            code=TaskSendErrorCode.PAYLOAD_MISMATCH,
            message='sha mismatch',
            retryable=False,
        )
        result = wrapper.retry_send(error)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.VALIDATION_FAILED
        assert 'only valid for ENQUEUE_FAILED' in result.err_value.message

    def test_retry_send_rejects_send_suppressed(self) -> None:
        """retry_send rejects SEND_SUPPRESSED errors."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        error = TaskSendError(
            code=TaskSendErrorCode.SEND_SUPPRESSED,
            message='suppressed',
            retryable=False,
        )
        result = wrapper.retry_send(error)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.VALIDATION_FAILED

    def test_retry_send_no_payload_returns_validation_error(self) -> None:
        """retry_send without payload on error returns VALIDATION_FAILED."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        error = TaskSendError(
            code=TaskSendErrorCode.ENQUEUE_FAILED,
            message='db gone',
            retryable=True,
            task_id='retry-id-1',
            payload=None,
        )
        result = wrapper.retry_send(error)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.VALIDATION_FAILED
        assert 'no payload or task_id' in result.err_value.message

    def test_retry_send_no_task_id_returns_validation_error(self) -> None:
        """retry_send without task_id on error returns VALIDATION_FAILED."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        error = TaskSendError(
            code=TaskSendErrorCode.ENQUEUE_FAILED,
            message='db gone',
            retryable=True,
            task_id=None,
            payload=_make_enqueue_failed_error().payload,
        )
        result = wrapper.retry_send(error)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.VALIDATION_FAILED

    def test_retry_send_cross_task_returns_validation_error(self) -> None:
        """retry_send rejects errors from a different task."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        error = _make_enqueue_failed_error(task_name='other.task')
        result = wrapper.retry_send(error)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.VALIDATION_FAILED
        assert 'cross-task retry' in result.err_value.message

    def test_retry_schedule_reuses_task_id_and_delay(self) -> None:
        """retry_schedule passes same task_id and enqueue_delay_seconds."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue.return_value = Ok('retry-id-1')
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        error = _make_enqueue_failed_error(enqueue_delay_seconds=60)
        result = wrapper.retry_schedule(error)

        assert is_ok(result)
        call_kwargs = broker.enqueue.call_args.kwargs
        assert call_kwargs['task_id'] == 'retry-id-1'
        assert call_kwargs['enqueue_delay_seconds'] == 60

    def test_retry_schedule_without_delay_returns_validation_error(self) -> None:
        """retry_schedule rejects errors with no enqueue_delay_seconds."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        error = _make_enqueue_failed_error(enqueue_delay_seconds=None)
        result = wrapper.retry_schedule(error)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.VALIDATION_FAILED
        assert 'missing enqueue_delay_seconds' in result.err_value.message

    def test_retry_send_rejects_scheduled_payload(self) -> None:
        """retry_send rejects errors with enqueue_delay_seconds (use retry_schedule instead)."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        error = _make_enqueue_failed_error(enqueue_delay_seconds=60)
        result = wrapper.retry_send(error)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.VALIDATION_FAILED
        assert 'use retry_schedule instead' in result.err_value.message

    @pytest.mark.asyncio
    async def test_retry_send_async_rejects_scheduled_payload(self) -> None:
        """retry_send_async rejects errors with enqueue_delay_seconds."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        error = _make_enqueue_failed_error(enqueue_delay_seconds=60)
        result = await wrapper.retry_send_async(error)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.VALIDATION_FAILED
        assert 'use retry_schedule instead' in result.err_value.message


# =============================================================================
# create_task_wrapper — auto-retry (resend_on_transient_err)
# =============================================================================


@pytest.mark.unit
class TestAutoRetry:
    """Tests for auto-retry behavior when resend_on_transient_err=True."""

    def test_send_with_resend_on_transient_err_retries_transient_failure(self) -> None:
        """Broker returns retryable Err twice then Ok on 3rd — total 3 calls."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app(resend_on_transient_err=True)
        broker = MagicMock()
        transient_err = Err(BrokerOperationError(
            code=BrokerErrorCode.ENQUEUE_FAILED,
            message='db gone',
            retryable=True,
            exception=ConnectionError('db gone'),
        ))
        broker.enqueue.side_effect = [transient_err, transient_err, Ok('task-ok')]
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.send(x=1)

        assert is_ok(result)
        assert result.ok_value.task_id == 'task-ok'
        assert broker.enqueue.call_count == 3  # 1 initial + 2 retries

    def test_send_with_resend_on_transient_err_gives_up_after_max_retries(self) -> None:
        """Broker always returns retryable Err — gives up after 4 total calls."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app(resend_on_transient_err=True)
        broker = MagicMock()
        transient_err = Err(BrokerOperationError(
            code=BrokerErrorCode.ENQUEUE_FAILED,
            message='db gone',
            retryable=True,
            exception=ConnectionError('db gone'),
        ))
        broker.enqueue.return_value = transient_err
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.send(x=1)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.ENQUEUE_FAILED
        assert broker.enqueue.call_count == 4  # 1 initial + 3 retries

    def test_send_with_resend_on_transient_err_false_no_retry(self) -> None:
        """With flag off, broker retryable Err is returned immediately (1 call)."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app(resend_on_transient_err=False)
        broker = MagicMock()
        broker.enqueue.return_value = Err(BrokerOperationError(
            code=BrokerErrorCode.ENQUEUE_FAILED,
            message='db gone',
            retryable=True,
            exception=ConnectionError('db gone'),
        ))
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.send(x=1)

        assert is_err(result)
        assert broker.enqueue.call_count == 1

    def test_send_with_resend_on_transient_err_non_retryable_no_retry(self) -> None:
        """Non-retryable Err is never retried, even with flag on."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app(resend_on_transient_err=True)
        broker = MagicMock()
        broker.enqueue.return_value = Err(BrokerOperationError(
            code=BrokerErrorCode.PAYLOAD_MISMATCH,
            message='different payload',
            retryable=False,
        ))
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.send(x=1)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.PAYLOAD_MISMATCH
        assert broker.enqueue.call_count == 1

    def test_exception_from_broker_classified_as_retryable_for_connection_error(self) -> None:
        """OperationalError raised by broker is classified retryable and retried."""
        from psycopg import OperationalError

        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app(resend_on_transient_err=True)
        broker = MagicMock()
        broker.enqueue.side_effect = [
            OperationalError('connection reset'),
            Ok('task-ok'),
        ]
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.send(x=1)

        assert is_ok(result)
        assert broker.enqueue.call_count == 2  # 1 failed + 1 retry

    def test_exception_from_broker_not_retried_for_programmer_error(self) -> None:
        """TypeError raised by broker is classified non-retryable — no retry."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app(resend_on_transient_err=True)
        broker = MagicMock()
        broker.enqueue.side_effect = TypeError('missing required argument')
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.send(x=1)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.ENQUEUE_FAILED
        assert not result.err_value.retryable
        assert broker.enqueue.call_count == 1  # no retry


# =============================================================================
# PAYLOAD_MISMATCH classification (regression: must be code-based, not string-based)
# =============================================================================


@pytest.mark.unit
class TestPayloadMismatchClassification:
    """Ensure PAYLOAD_MISMATCH is classified by BrokerErrorCode, not message text."""

    def test_mismatch_classified_by_code_not_message(self) -> None:
        """Broker PAYLOAD_MISMATCH code maps to TaskSendErrorCode.PAYLOAD_MISMATCH
        regardless of message wording."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        # Message intentionally omits 'sha mismatch' — old string-matching would miss it.
        broker.enqueue.return_value = Err(BrokerOperationError(
            code=BrokerErrorCode.PAYLOAD_MISMATCH,
            message='duplicate task_id with conflicting payload',
            retryable=False,
        ))
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.send(x=1)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.PAYLOAD_MISMATCH
        assert not result.err_value.retryable

    def test_enqueue_failed_not_promoted_to_mismatch(self) -> None:
        """Broker ENQUEUE_FAILED with 'sha mismatch' in message stays ENQUEUE_FAILED
        (code governs classification, not message content)."""
        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        # Message contains 'sha mismatch' but code is ENQUEUE_FAILED — must NOT
        # be promoted to PAYLOAD_MISMATCH.
        broker.enqueue.return_value = Err(BrokerOperationError(
            code=BrokerErrorCode.ENQUEUE_FAILED,
            message='unexpected sha mismatch in log',
            retryable=True,
        ))
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = wrapper.send(x=1)

        assert is_err(result)
        assert result.err_value.code == TaskSendErrorCode.ENQUEUE_FAILED
        assert result.err_value.retryable


# =============================================================================
# TaskOptions.good_until timezone validation (regression: naive dt must not reach fingerprinting)
# =============================================================================


@pytest.mark.unit
class TestTaskOptionsGoodUntilTimezone:
    """Ensure TaskOptions rejects naive datetimes at construction time."""

    def test_naive_good_until_raises_validation_error(self) -> None:
        """Naive datetime is rejected by pydantic field_validator."""
        naive = datetime(2025, 6, 1, 12, 0, 0)
        with pytest.raises(Exception, match='timezone-aware'):
            TaskOptions(task_name='test.task', good_until=naive)

    def test_aware_good_until_accepted(self) -> None:
        """Timezone-aware datetime passes validation."""
        aware = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        opts = TaskOptions(task_name='test.task', good_until=aware)
        assert opts.good_until == aware

    def test_none_good_until_accepted(self) -> None:
        """None is the default and passes validation."""
        opts = TaskOptions(task_name='test.task')
        assert opts.good_until is None


@pytest.mark.unit
class TestDefinitionLevelGoodUntil:
    """Decorator-level good_until is rejected because it is definition-time state."""

    def test_app_task_rejects_good_until_at_definition_time(self) -> None:
        from horsies.core.app import Horsies

        app = object.__new__(Horsies)
        deadline = datetime(2030, 1, 1, tzinfo=timezone.utc)

        def bad_deadline() -> TaskResult[str, TaskError]:
            return TaskResult(ok='bad')

        with pytest.raises(TaskDefinitionError) as exc_info:
            decorator = app.task(
                task_name='test.bad_deadline',
                **{'good_until': deadline},
            )
            decorator(bad_deadline)

        assert exc_info.value.code == ErrorCode.TASK_INVALID_OPTIONS
        assert 'good_until must be set when sending a task' in exc_info.value.message


# =============================================================================
# FromNodeMarker / from_node()
# =============================================================================


@pytest.mark.unit
class TestFromNodeMarker:
    """Tests for FromNodeMarker and from_node() helper."""

    def test_from_node_returns_marker_with_correct_node(self) -> None:
        """from_node() wraps the upstream node in a FromNodeMarker."""
        from horsies.core.models.workflow import TaskNode

        upstream_node = TaskNode(fn=_make_task_fn(), kwargs={'value': 1})

        result = from_node(upstream_node)

        assert isinstance(result, FromNodeMarker)
        assert result.node is upstream_node

    def test_marker_repr_is_readable(self) -> None:
        """FromNodeMarker repr includes the wrapped node."""
        mock_node = MagicMock(name='MockNode')

        marker = FromNodeMarker(mock_node)

        assert 'FromNodeMarker' in repr(marker)
        assert 'MockNode' in repr(marker)

    def test_from_node_rejects_non_node_value(self) -> None:
        """from_node() rejects non-node inputs with a structured error."""
        from horsies.core.errors import WorkflowValidationError

        with pytest.raises(WorkflowValidationError) as exc_info:
            from_node('not_a_node')  # type: ignore[arg-type]

        assert exc_info.value.code == ErrorCode.WORKFLOW_INVALID_ARGS_FROM


# =============================================================================
# NodeFactory — positional arg rejection
# =============================================================================


def _make_task_fn(app: MagicMock | None = None) -> Any:
    """Create a minimal wrapper via create_task_wrapper for NodeFactory tests."""
    def sample(*, value: int, label: str = 'default') -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    if app is None:
        app = _make_app()
    return create_task_wrapper(sample, app, 'test.sample')


@pytest.mark.unit
class TestNodeFactoryPositionalRejection:
    """Tests for NodeFactory rejecting positional .node()() calls (D.1)."""

    def test_positional_args_raise_workflow_validation_error(self) -> None:
        """Positional args in .node()() raise WorkflowValidationError(HRS-026)."""
        from horsies.core.errors import WorkflowValidationError

        task_fn = _make_task_fn()
        factory = task_fn.node()

        with pytest.raises(WorkflowValidationError) as exc_info:
            factory(42)  # positional — forbidden

        assert exc_info.value.code == ErrorCode.WORKFLOW_POSITIONAL_ARGS_NOT_SUPPORTED
        assert 'positional' in exc_info.value.message

    def test_kwargs_only_succeeds(self) -> None:
        """Kwargs-only .node()() call succeeds and creates TaskNode."""
        from horsies.core.models.workflow import TaskNode

        task_fn = _make_task_fn()
        factory = task_fn.node()

        node = factory(value=10, label='test')

        assert isinstance(node, TaskNode)
        assert node.kwargs == {'value': 10, 'label': 'test'}
        assert node.args == ()

    def test_empty_call_succeeds(self) -> None:
        """Empty .node()() call (no args) succeeds — all args from injection/defaults."""
        from horsies.core.models.workflow import TaskNode

        task_fn = _make_task_fn()
        factory = task_fn.node()

        node = factory()

        assert isinstance(node, TaskNode)
        assert node.kwargs == {}
        assert node.args == ()


# =============================================================================
# NodeFactory — from_node() marker conversion
# =============================================================================


@pytest.mark.unit
class TestNodeFactoryFromNodeConversion:
    """Tests for NodeFactory converting from_node() markers (C.1)."""

    def test_marker_kwarg_becomes_args_from_entry(self) -> None:
        """from_node() kwarg is extracted into args_from dict."""
        from horsies.core.models.workflow import TaskNode

        app = _make_app()

        # Producer
        def produce(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)
        producer_fn = create_task_wrapper(produce, app, 'test.produce')
        producer_node = TaskNode(fn=producer_fn, kwargs={'value': 42})

        # Consumer
        def consume(*, data: TaskResult[int, TaskError]) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)
        consumer_fn = create_task_wrapper(consume, app, 'test.consume')
        factory = consumer_fn.node()

        node = factory(data=from_node(producer_node))

        assert node.args_from == {'data': producer_node}
        assert node.kwargs == {}  # marker removed from static kwargs

    def test_marker_auto_wires_waits_for(self) -> None:
        """from_node() automatically adds upstream to waits_for."""
        from horsies.core.models.workflow import TaskNode

        app = _make_app()

        def produce(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)
        producer_fn = create_task_wrapper(produce, app, 'test.produce')
        producer_node = TaskNode(fn=producer_fn, kwargs={'value': 42})

        def consume(*, data: TaskResult[int, TaskError]) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)
        consumer_fn = create_task_wrapper(consume, app, 'test.consume')
        factory = consumer_fn.node()

        node = factory(data=from_node(producer_node))

        assert producer_node in node.waits_for

    def test_marker_does_not_duplicate_waits_for(self) -> None:
        """If upstream is already in waits_for, from_node() doesn't add it again."""
        from horsies.core.models.workflow import TaskNode

        app = _make_app()

        def produce(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)
        producer_fn = create_task_wrapper(produce, app, 'test.produce')
        producer_node = TaskNode(fn=producer_fn, kwargs={'value': 42})

        def consume(*, data: TaskResult[int, TaskError]) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)
        consumer_fn = create_task_wrapper(consume, app, 'test.consume')
        # Explicit waits_for already includes producer
        factory = consumer_fn.node(waits_for=[producer_node])

        node = factory(data=from_node(producer_node))

        assert node.waits_for.count(producer_node) == 1
        assert node.args_from == {'data': producer_node}

    def test_multiple_markers_from_different_upstreams(self) -> None:
        """Multiple from_node() markers from different upstreams all wire correctly."""
        from horsies.core.models.workflow import TaskNode

        app = _make_app()

        def produce_a(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)
        fn_a = create_task_wrapper(produce_a, app, 'test.produce_a')
        node_a = TaskNode(fn=fn_a, kwargs={'value': 1})

        def produce_b(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)
        fn_b = create_task_wrapper(produce_b, app, 'test.produce_b')
        node_b = TaskNode(fn=fn_b, kwargs={'value': 2})

        def consume(
            *, first: TaskResult[int, TaskError],
            second: TaskResult[int, TaskError],
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)
        consumer_fn = create_task_wrapper(consume, app, 'test.consume')
        factory = consumer_fn.node()

        node = factory(first=from_node(node_a), second=from_node(node_b))

        assert node.args_from == {'first': node_a, 'second': node_b}
        assert node_a in node.waits_for
        assert node_b in node.waits_for
        assert node.kwargs == {}

    def test_mixed_static_and_marker_kwargs(self) -> None:
        """Static kwargs and from_node() markers coexist correctly."""
        from horsies.core.models.workflow import TaskNode

        app = _make_app()

        def produce(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)
        producer_fn = create_task_wrapper(produce, app, 'test.produce')
        producer_node = TaskNode(fn=producer_fn, kwargs={'value': 42})

        def consume(
            *, data: TaskResult[int, TaskError],
            label: str = 'default',
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)
        consumer_fn = create_task_wrapper(consume, app, 'test.consume')
        factory = consumer_fn.node()

        node = factory(data=from_node(producer_node), label='custom')

        assert node.args_from == {'data': producer_node}
        assert node.kwargs == {'label': 'custom'}
        assert producer_node in node.waits_for


# =============================================================================
# NodeFactory — from_node() conflict detection
# =============================================================================


@pytest.mark.unit
class TestNodeFactoryFromNodeConflicts:
    """Tests for conflict detection between from_node() and explicit args_from."""

    def test_marker_conflicts_with_explicit_args_from_raises(self) -> None:
        """Same key in from_node() and explicit args_from raises HRS-021."""
        from horsies.core.errors import WorkflowValidationError
        from horsies.core.models.workflow import TaskNode

        app = _make_app()

        def produce(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)
        producer_fn = create_task_wrapper(produce, app, 'test.produce')
        producer_node = TaskNode(fn=producer_fn, kwargs={'value': 42})

        def consume(*, data: TaskResult[int, TaskError]) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)
        consumer_fn = create_task_wrapper(consume, app, 'test.consume')
        # Explicit args_from for 'data'
        factory = consumer_fn.node(args_from={'data': producer_node})

        # Also from_node() for 'data' — conflict
        with pytest.raises(WorkflowValidationError) as exc_info:
            factory(data=from_node(producer_node))

        assert exc_info.value.code == ErrorCode.WORKFLOW_KWARGS_ARGS_FROM_OVERLAP
        assert 'data' in str(exc_info.value)

    def test_disjoint_marker_and_explicit_args_from_merges(self) -> None:
        """Disjoint from_node() and explicit args_from keys merge correctly."""
        from horsies.core.models.workflow import TaskNode

        app = _make_app()

        def produce_a(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)
        fn_a = create_task_wrapper(produce_a, app, 'test.produce_a')
        node_a = TaskNode(fn=fn_a, kwargs={'value': 1})

        def produce_b(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)
        fn_b = create_task_wrapper(produce_b, app, 'test.produce_b')
        node_b = TaskNode(fn=fn_b, kwargs={'value': 2})

        def consume(
            *, first: TaskResult[int, TaskError],
            second: TaskResult[int, TaskError],
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)
        consumer_fn = create_task_wrapper(consume, app, 'test.consume')
        # 'first' via explicit args_from, 'second' via from_node()
        factory = consumer_fn.node(
            args_from={'first': node_a},
            waits_for=[node_a],
        )

        node = factory(second=from_node(node_b))  # type: ignore[call-arg]

        assert node.args_from == {'first': node_a, 'second': node_b}  # type: ignore[union-attr]
        assert node_a in node.waits_for  # type: ignore[union-attr]
        assert node_b in node.waits_for  # type: ignore[union-attr]


# =============================================================================
# NodeFactory — manual args_from preserved
# =============================================================================


@pytest.mark.unit
class TestNodeFactoryManualArgsFrom:
    """Tests that manual args_from continues to work without from_node()."""

    def test_manual_args_from_still_works(self) -> None:
        """Explicit args_from without markers is forwarded unchanged."""
        from horsies.core.models.workflow import TaskNode

        app = _make_app()

        def produce(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)
        producer_fn = create_task_wrapper(produce, app, 'test.produce')
        producer_node = TaskNode(fn=producer_fn, kwargs={'value': 42})

        def consume(*, data: TaskResult[int, TaskError]) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)
        consumer_fn = create_task_wrapper(consume, app, 'test.consume')
        factory = consumer_fn.node(
            args_from={'data': producer_node},
            waits_for=[producer_node],
        )

        node = factory()  # type: ignore[call-arg]  # no kwargs needed — all from injection

        assert node.args_from == {'data': producer_node}  # type: ignore[union-attr]
        assert producer_node in node.waits_for  # type: ignore[union-attr]
        assert node.kwargs == {}  # type: ignore[union-attr]


# =============================================================================
# Async-context guard + schedule_async / retry_schedule_async
# =============================================================================


def _make_guard_task(
    broker: MagicMock,
) -> Any:
    """Task wrapper wired to a mock broker whose enqueue paths succeed."""
    app = _make_app()
    app.get_broker.return_value = broker

    def sample(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    return create_task_wrapper(sample, app, 'test.async_guard_sample')


def _ok_broker() -> MagicMock:
    broker = MagicMock()
    broker.enqueue.return_value = Ok('task-id-sync')
    broker.enqueue_async = AsyncMock(return_value=Ok('task-id-async'))
    return broker


def _failed_send_error(
    task: Any,
    broker: MagicMock,
    *,
    schedule_delay: int | None = None,
) -> TaskSendError:
    """Produce a real ENQUEUE_FAILED error (with payload) outside any loop."""
    broker.enqueue.side_effect = ValueError('boom')
    if schedule_delay is None:
        res = task.send(value=1)
    else:
        res = task.schedule(schedule_delay, value=1)
    broker.enqueue.side_effect = None
    assert is_err(res)
    assert res.err_value.code == TaskSendErrorCode.ENQUEUE_FAILED
    return res.err_value


@pytest.mark.unit
class TestAsyncContextGuard:
    """Sync send/schedule fail closed with ASYNC_CONTEXT on a running loop."""

    def test_sync_send_inside_event_loop_returns_async_context(self) -> None:
        broker = _ok_broker()
        task = _make_guard_task(broker)

        async def call_inside_loop() -> Any:
            return task.send(value=1)

        res = asyncio.run(call_inside_loop())
        assert is_err(res)
        assert res.err_value.code == TaskSendErrorCode.ASYNC_CONTEXT
        assert res.err_value.retryable is False
        assert 'send_async' in res.err_value.message
        broker.enqueue.assert_not_called()

    def test_sync_schedule_inside_event_loop_returns_async_context(self) -> None:
        broker = _ok_broker()
        task = _make_guard_task(broker)

        async def call_inside_loop() -> Any:
            return task.schedule(5, value=1)

        res = asyncio.run(call_inside_loop())
        assert is_err(res)
        assert res.err_value.code == TaskSendErrorCode.ASYNC_CONTEXT
        assert 'schedule_async' in res.err_value.message
        broker.enqueue.assert_not_called()

    def test_with_options_send_inside_event_loop_returns_async_context(self) -> None:
        broker = _ok_broker()
        task = _make_guard_task(broker)

        async def call_inside_loop() -> Any:
            return task.with_options(good_until=None).send(value=1)

        res = asyncio.run(call_inside_loop())
        assert is_err(res)
        assert res.err_value.code == TaskSendErrorCode.ASYNC_CONTEXT
        broker.enqueue.assert_not_called()

    def test_retry_send_inside_event_loop_returns_async_context(self) -> None:
        broker = _ok_broker()
        task = _make_guard_task(broker)
        failed = _failed_send_error(task, broker)

        async def call_inside_loop() -> Any:
            return task.retry_send(failed)

        res = asyncio.run(call_inside_loop())
        assert is_err(res)
        assert res.err_value.code == TaskSendErrorCode.ASYNC_CONTEXT
        # The guarded error keeps the payload so the caller can still
        # retry via the async variant.
        assert res.err_value.payload is not None

    def test_retry_schedule_inside_event_loop_returns_async_context(self) -> None:
        broker = _ok_broker()
        task = _make_guard_task(broker)
        failed = _failed_send_error(task, broker, schedule_delay=7)

        async def call_inside_loop() -> Any:
            return task.retry_schedule(failed)

        res = asyncio.run(call_inside_loop())
        assert is_err(res)
        assert res.err_value.code == TaskSendErrorCode.ASYNC_CONTEXT

    def test_sync_send_outside_loop_unaffected(self) -> None:
        broker = _ok_broker()
        task = _make_guard_task(broker)

        res = task.send(value=1)
        assert is_ok(res)
        broker.enqueue.assert_called_once()

    def test_sync_schedule_outside_loop_unaffected(self) -> None:
        broker = _ok_broker()
        task = _make_guard_task(broker)

        res = task.schedule(5, value=1)
        assert is_ok(res)
        broker.enqueue.assert_called_once()
        assert broker.enqueue.call_args.kwargs['enqueue_delay_seconds'] == 5

    def test_send_async_inside_loop_unaffected(self) -> None:
        broker = _ok_broker()
        task = _make_guard_task(broker)

        res = asyncio.run(task.send_async(value=1))
        assert is_ok(res)
        broker.enqueue_async.assert_awaited_once()


@pytest.mark.unit
class TestScheduleAsync:
    """schedule_async / retry_schedule_async mirror the sync semantics."""

    def test_schedule_async_happy_path(self) -> None:
        broker = _ok_broker()
        task = _make_guard_task(broker)

        res = asyncio.run(task.schedule_async(9, value=1))
        assert is_ok(res)
        broker.enqueue_async.assert_awaited_once()
        assert broker.enqueue_async.call_args.kwargs['enqueue_delay_seconds'] == 9

    def test_schedule_async_rejects_negative_delay(self) -> None:
        broker = _ok_broker()
        task = _make_guard_task(broker)

        res = asyncio.run(task.schedule_async(-1, value=1))
        assert is_err(res)
        assert res.err_value.code == TaskSendErrorCode.VALIDATION_FAILED
        broker.enqueue_async.assert_not_awaited()

    def test_schedule_async_rejects_bool_delay(self) -> None:
        broker = _ok_broker()
        task = _make_guard_task(broker)

        res = asyncio.run(task.schedule_async(True, value=1))  # type: ignore[arg-type]
        assert is_err(res)
        assert res.err_value.code == TaskSendErrorCode.VALIDATION_FAILED
        broker.enqueue_async.assert_not_awaited()

    def test_schedule_async_zero_delay_allowed(self) -> None:
        broker = _ok_broker()
        task = _make_guard_task(broker)

        res = asyncio.run(task.schedule_async(0, value=1))
        assert is_ok(res)

    def test_with_options_schedule_async(self) -> None:
        broker = _ok_broker()
        task = _make_guard_task(broker)

        res = asyncio.run(
            task.with_options(good_until=None).schedule_async(3, value=1)
        )
        assert is_ok(res)
        assert broker.enqueue_async.call_args.kwargs['enqueue_delay_seconds'] == 3

    def test_retry_schedule_async_reuses_payload(self) -> None:
        broker = _ok_broker()
        task = _make_guard_task(broker)
        failed = _failed_send_error(task, broker, schedule_delay=7)

        res = asyncio.run(task.retry_schedule_async(failed))
        assert is_ok(res)
        broker.enqueue_async.assert_awaited_once()
        assert broker.enqueue_async.call_args.kwargs['enqueue_delay_seconds'] == 7
        assert broker.enqueue_async.call_args.kwargs['task_id'] == failed.task_id

    def test_retry_schedule_async_rejects_sendless_error(self) -> None:
        broker = _ok_broker()
        task = _make_guard_task(broker)
        failed = _failed_send_error(task, broker)  # plain send, no delay

        res = asyncio.run(task.retry_schedule_async(failed))
        assert is_err(res)
        assert res.err_value.code == TaskSendErrorCode.VALIDATION_FAILED
