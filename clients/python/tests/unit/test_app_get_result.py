"""Unit tests for `Horsies.get_result_async` envelope-validation guards.

Strict-serde phase 5/6 regression coverage. The app-level err-fast-path
sits in front of `decode_task_error`; without `validate_task_result_envelope`
it would happily decode the err slot of a malformed payload, smuggling
the wrong shape past the contract.

Tests bypass the real Postgres broker by patching `Horsies.get_broker`
to return a mock whose `get_raw_result_record_async` yields a crafted
`RawResultRecord` envelope.
"""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.app import Horsies
from horsies.core.brokers.result_types import (
    BrokerErrorCode,
    RawResultRecord,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.outcome import (
    Failed,
    ResultDeserializationError,
    ResultNotAvailable,
    WaitTimeout,
)
from horsies.core.models.tasks import (
    OperationalErrorCode,
    OutcomeCode,
    SubWorkflowError,
)
from horsies.core.task_read import is_final_read
from horsies.core.types.result import Err, Ok, is_err, is_ok
from horsies.core.types.status import TaskStatus


def _make_app() -> Horsies:
    """Build a minimal Horsies (no real broker connection used in tests)."""
    return Horsies(
        config=AppConfig(
            broker=PostgresConfig(
                database_url='postgresql://u:p@localhost/db',
            ),
        ),
    )


def _patch_broker(app: Horsies, raw_record_result: Any) -> MagicMock:
    """Replace `app.get_broker` with a mock returning the given record result."""
    broker = MagicMock()
    broker.get_raw_result_record_async = AsyncMock(
        return_value=raw_record_result,
    )
    app.get_broker = MagicMock(return_value=broker)  # type: ignore[method-assign]
    return broker


@pytest.mark.unit
class TestHorsiesGetResultEnvelopeGuards:
    """`Horsies.get_result_async` must enforce the envelope shape before
    routing into err-fast-path or typed decode."""

    @pytest.mark.asyncio
    async def test_malformed_envelope_err_fast_returns_invalid_json_payload(
        self,
    ) -> None:
        """Marker present, ``ok`` missing, ``err`` populated → invalid
        envelope; must not silently take the err-fast-path.

        Regression for the phase 5/6 review finding: err-fast-path was
        bypassing `validate_task_result_envelope` and reading the err
        slot off any dict with the marker set.
        """
        app = _make_app()
        malformed = {
            '__h_task_result__': True,
            # ``ok`` key missing
            'err': {
                'error_code': {'__builtin_task_code__': 'BROKER_ERROR'},
                'message': 'smuggled',
                'data': None,
                'exception': None,
            },
        }
        record = RawResultRecord(
            task_id='task-x',
            task_name='unknown',
            status=TaskStatus.FAILED,
            raw_result=cast(Any, malformed),
        )
        _patch_broker(app, Ok(record))

        result = await app.get_result_async('task-x')

        match result:
            case Err(ResultDeserializationError() as read_error):
                assert 'envelope invalid' in read_error.message
            case _:
                pytest.fail(f'expected ResultDeserializationError, got {result!r}')
        assert is_final_read(result) is True

    @pytest.mark.asyncio
    async def test_envelope_with_both_slots_populated_rejected(self) -> None:
        """`ok` and `err` both populated → ``ResultDeserializationError``."""
        app = _make_app()
        malformed = {
            '__h_task_result__': True,
            'ok': 1,
            'err': {
                'error_code': {'__builtin_task_code__': 'BROKER_ERROR'},
                'message': 'm',
                'data': None,
                'exception': None,
            },
        }
        record = RawResultRecord(
            task_id='task-x',
            task_name='unknown',
            status=TaskStatus.FAILED,
            raw_result=cast(Any, malformed),
        )
        _patch_broker(app, Ok(record))

        result = await app.get_result_async('task-x')

        assert isinstance(result, Err)
        assert isinstance(result.err_value, ResultDeserializationError)

    @pytest.mark.asyncio
    async def test_envelope_missing_marker_rejected(self) -> None:
        """Payload lacking the marker isn't a TaskResult envelope."""
        app = _make_app()
        record = RawResultRecord(
            task_id='task-x',
            task_name='unknown',
            status=TaskStatus.COMPLETED,
            raw_result=cast(Any, {'ok': 1, 'err': None}),
        )
        _patch_broker(app, Ok(record))

        result = await app.get_result_async('task-x')

        assert isinstance(result, Err)
        assert isinstance(result.err_value, ResultDeserializationError)

    @pytest.mark.asyncio
    async def test_removed_builtin_code_in_err_slot_returns_invalid_json_payload(
        self,
    ) -> None:
        """A stored err slot tagged with the removed code
        ``PYDANTIC_HYDRATION_ERROR`` fails closed with
        ``ResultDeserializationError``."""
        app = _make_app()
        stored = {
            '__h_task_result__': True,
            'ok': None,
            'err': {
                'error_code': {'__builtin_task_code__': 'PYDANTIC_HYDRATION_ERROR'},
                'message': 'stored',
                'data': None,
                'exception': None,
            },
        }
        record = RawResultRecord(
            task_id='task-x',
            task_name='unknown',
            status=TaskStatus.FAILED,
            raw_result=cast(Any, stored),
        )
        _patch_broker(app, Ok(record))

        result = await app.get_result_async('task-x')

        match result:
            case Err(ResultDeserializationError() as read_error):
                assert (
                    "Unknown built-in task code: 'PYDANTIC_HYDRATION_ERROR'"
                    in read_error.message
                )
            case _:
                pytest.fail(f'expected ResultDeserializationError, got {result!r}')

    @pytest.mark.asyncio
    async def test_terminal_status_with_none_payload_is_result_not_available(
        self,
    ) -> None:
        """Terminal status + empty result column → ``ResultNotAvailable``.

        Mirrors the TaskHandle-level guard: terminal failures to write
        the payload are cacheable (engine misbehaviour), not transient
        timeouts.
        """
        app = _make_app()
        record = RawResultRecord(
            task_id='task-x',
            task_name='unknown',
            status=TaskStatus.COMPLETED,
            raw_result=None,
        )
        _patch_broker(app, Ok(record))

        result = await app.get_result_async('task-x')

        # A read failure, not a task outcome: kept by a handle.
        assert isinstance(result, Err)
        assert isinstance(result.err_value, ResultNotAvailable)
        assert is_final_read(result) is True

    @pytest.mark.asyncio
    async def test_non_terminal_status_with_none_payload_is_wait_timeout(
        self,
    ) -> None:
        """Non-terminal + raw_result=None → ``WaitTimeout``."""
        app = _make_app()
        record = RawResultRecord(
            task_id='task-x',
            task_name='unknown',
            status=TaskStatus.RUNNING,
            raw_result=None,
        )
        _patch_broker(app, Ok(record))

        result = await app.get_result_async('task-x', timeout_ms=100)

        assert isinstance(result, Err)
        assert isinstance(result.err_value, WaitTimeout)

    @pytest.mark.asyncio
    async def test_cancelled_status_returns_task_cancelled(self) -> None:
        """Cancelled row → ``Ok(Failed(TASK_CANCELLED))``."""
        app = _make_app()
        record = RawResultRecord(
            task_id='task-x',
            task_name='unknown',
            status=TaskStatus.CANCELLED,
            raw_result=None,
        )
        _patch_broker(app, Ok(record))

        result = await app.get_result_async('task-x')

        match result:
            case Ok(Failed(task_error)):
                assert task_error.error_code == OutcomeCode.TASK_CANCELLED
            case _:
                pytest.fail(f'expected a failed outcome, got {result!r}')

    @pytest.mark.asyncio
    async def test_err_fast_path_polymorphic_sub_workflow_error(self) -> None:
        """End-to-end: a valid envelope carrying a SubWorkflowError dump
        in the err slot round-trips through `Horsies.get_result_async`
        as the concrete subclass — not a downgraded TaskError."""
        app = _make_app()
        from horsies.core.models.workflow.context import SubWorkflowSummary
        from horsies.core.models.workflow.enums import WorkflowStatus

        original = SubWorkflowError(
            error_code=OperationalErrorCode.UNHANDLED_EXCEPTION,
            message='child died',
            sub_workflow_id='wf-abc',
            sub_workflow_summary=SubWorkflowSummary(
                status=WorkflowStatus.FAILED,
                output=None,
                total_nodes=2,
                completed_nodes=0,
                failed_nodes=2,
                skipped_nodes=0,
                error_summary='boom',
            ),
        )
        wire = {
            '__h_task_result__': True,
            'ok': None,
            'err': original.model_dump(mode='json'),
        }
        record = RawResultRecord(
            task_id='parent-task',
            task_name='unknown',
            status=TaskStatus.FAILED,
            raw_result=cast(Any, wire),
        )
        _patch_broker(app, Ok(record))

        result = await app.get_result_async('parent-task')

        match result:
            case Ok(Failed(SubWorkflowError() as sub)):
                assert sub.sub_workflow_id == 'wf-abc'
                assert sub.sub_workflow_summary.failed_nodes == 2
            case _:
                pytest.fail(f'expected Ok(Failed(SubWorkflowError)), got {result!r}')


def _patch_broker_task_info(app: Horsies, task_info_result: Any) -> MagicMock:
    """Replace `app.get_broker` with a mock returning the given task info result."""
    broker = MagicMock()
    broker.get_task_info_async = AsyncMock(return_value=task_info_result)
    app.get_broker = MagicMock(return_value=broker)  # type: ignore[method-assign]
    return broker


def _make_task_info(
    task_id: str,
    task_name: str,
    raw_result: Any,
) -> 'Any':
    """Build a minimal TaskInfo for tests; defaults satisfy required fields."""
    import datetime

    from horsies.core.models.tasks import TaskInfo

    now = datetime.datetime.now(datetime.timezone.utc)
    return TaskInfo(
        task_id=task_id,
        task_name=task_name,
        status=TaskStatus.FAILED,
        queue_name='default',
        priority=0,
        retry_count=0,
        max_retries=0,
        next_retry_at=None,
        sent_at=now,
        enqueued_at=now,
        claimed_at=now,
        started_at=now,
        completed_at=None,
        failed_at=now,
        worker_hostname=None,
        worker_pid=None,
        worker_process_name=None,
        raw_result=raw_result,
    )


@pytest.mark.unit
class TestHorsiesGetTaskInfoErrFastPath:
    """`Horsies.get_task_info_async(include_result=True)` must surface
    the decoded err even when the task isn't registered locally.

    Pre-fix the function gated all decode on `ok_type` lookup. An
    err-only payload from an unregistered task came back with
    `decoded_result=None`, hiding the real failure from any cross-process
    monitoring caller that didn't import the task module.
    """

    @pytest.mark.asyncio
    async def test_err_only_decoded_for_unregistered_task(self) -> None:
        """Unregistered task + err-only envelope → ``decoded_result``
        populated via err-fast-path (no ok_type needed)."""
        app = _make_app()
        wire = {
            '__h_task_result__': True,
            'ok': None,
            'err': {
                'error_code': {'__builtin_task_code__': 'TASK_EXCEPTION'},
                'message': 'boom',
                'data': None,
                'exception': None,
            },
        }
        info = _make_task_info(
            task_id='task-1', task_name='not.registered', raw_result=wire,
        )
        _patch_broker_task_info(app, Ok(info))

        result = await app.get_task_info_async('task-1', include_result=True)

        assert is_ok(result)
        decoded_info = result.unwrap()
        assert decoded_info is not None
        assert decoded_info.result_decoded is True
        match decoded_info.decoded_result:
            case Failed(task_error):
                assert task_error.message == 'boom'
            case other:
                pytest.fail(f'expected a failed outcome, got {other!r}')

    @pytest.mark.asyncio
    async def test_err_only_preserves_sub_workflow_error_subclass(self) -> None:
        """Polymorphic decode path: SubWorkflowError survives via
        ``decode_task_error``, even for an unregistered task."""
        app = _make_app()
        from horsies.core.models.workflow.context import SubWorkflowSummary
        from horsies.core.models.workflow.enums import WorkflowStatus

        original = SubWorkflowError(
            error_code=OperationalErrorCode.UNHANDLED_EXCEPTION,
            message='child died',
            sub_workflow_id='wf-xyz',
            sub_workflow_summary=SubWorkflowSummary(
                status=WorkflowStatus.FAILED,
                output=None,
                total_nodes=1,
                completed_nodes=0,
                failed_nodes=1,
                skipped_nodes=0,
                error_summary='child boom',
            ),
        )
        wire = {
            '__h_task_result__': True,
            'ok': None,
            'err': original.model_dump(mode='json'),
        }
        info = _make_task_info(
            task_id='task-2', task_name='not.registered', raw_result=wire,
        )
        _patch_broker_task_info(app, Ok(info))

        result = await app.get_task_info_async('task-2', include_result=True)

        assert is_ok(result)
        decoded_info = result.unwrap()
        assert decoded_info is not None
        assert decoded_info.result_decoded is True
        match decoded_info.decoded_result:
            case Failed(SubWorkflowError() as sub):
                assert sub.sub_workflow_id == 'wf-xyz'
            case other:
                pytest.fail(f'expected Failed(SubWorkflowError), got {other!r}')

    @pytest.mark.asyncio
    async def test_ok_only_unregistered_task_leaves_decoded_result_none(
        self,
    ) -> None:
        """Ok-only path still requires ok_type. Unregistered → undecoded."""
        app = _make_app()
        wire = {
            '__h_task_result__': True,
            'ok': 42,
            'err': None,
        }
        info = _make_task_info(
            task_id='task-3', task_name='not.registered', raw_result=wire,
        )
        _patch_broker_task_info(app, Ok(info))

        result = await app.get_task_info_async('task-3', include_result=True)

        assert is_ok(result)
        decoded_info = result.unwrap()
        assert decoded_info is not None
        # No ok_type → decoded_result stays None, result_decoded False.
        assert decoded_info.decoded_result is None
        assert decoded_info.result_decoded is False

    @pytest.mark.asyncio
    async def test_malformed_envelope_returns_invalid_json_payload(
        self,
    ) -> None:
        """Envelope shape failure must fail closed — same as get_result."""
        app = _make_app()
        malformed = {
            '__h_task_result__': True,
            # Both slots populated → invariant violation.
            'ok': 1,
            'err': {
                'error_code': {'__builtin_task_code__': 'TASK_EXCEPTION'},
                'message': 'boom',
                'data': None,
                'exception': None,
            },
        }
        info = _make_task_info(
            task_id='task-4', task_name='not.registered', raw_result=malformed,
        )
        _patch_broker_task_info(app, Ok(info))

        result = await app.get_task_info_async('task-4', include_result=True)

        assert is_err(result)
        err = result.unwrap_err()
        assert err.code == BrokerErrorCode.INVALID_JSON_PAYLOAD
        assert err.retryable is False
