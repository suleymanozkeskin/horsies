"""The workflow start error variants: one per code, redacted, and the id-conflict site.

``WorkflowStartError`` is a union with one frozen variant per
``WorkflowStartErrorCode`` member. A variant's repr hides the message and
the cause. Every variant carries ``workflow_name`` and ``workflow_id``.
"""

from __future__ import annotations

from typing import Any, get_args
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.brokers.resources import EventLoopLimit
from horsies.core.types.result import Ok, is_err
from horsies.core.workflows.lifecycle import start_workflow_async
from horsies.core.workflows.start_types import (
    WorkflowStartAsyncContext,
    WorkflowStartBrokerNotConfigured,
    WorkflowStartEnqueueFailed,
    WorkflowStartError,
    WorkflowStartErrorCode,
    WorkflowStartEventLoopLimit,
    WorkflowStartIdConflict,
    WorkflowStartLoopRunnerFailed,
    WorkflowStartRetryable,
    WorkflowStartValidationFailed,
)
from tests.broker_resources_support import attach_resources, fake_resources

pytestmark = [pytest.mark.unit]

MESSAGE_MARKER = 'message-marker-5c1a'
CAUSE_MARKER = 'cause-marker-5c1a'
WORKFLOW_ID = '6a0d2b5e-3f4c-4e71-9b82-1c2d3e4f5a6b'


def _every_variant(cause: BaseException) -> list[WorkflowStartError]:
    """One value of each variant; each cause field holds ``cause``."""
    message = f'failed: {MESSAGE_MARKER}'
    ids = {'workflow_name': 'wf', 'workflow_id': WORKFLOW_ID}
    return [
        WorkflowStartBrokerNotConfigured(message=message, **ids),
        WorkflowStartValidationFailed(message=message, exception=cause, **ids),
        WorkflowStartIdConflict(
            message=message, existing_workflow_name='other-wf', **ids,
        ),
        WorkflowStartEnqueueFailed(
            message=message, retryable=True, exception=cause, **ids,
        ),
        WorkflowStartLoopRunnerFailed(message=message, exception=cause, **ids),
        WorkflowStartEventLoopLimit(
            message=message,
            loop_limit=EventLoopLimit(
                limit=5,
                held_thread_names=('MainThread',),
                current_thread_name='worker-1',
            ),
            **ids,
        ),
        WorkflowStartAsyncContext(message=message, **ids),
    ]


def _variant_id(error: WorkflowStartError) -> str:
    return type(error).__name__


class TestVariantCatalog:
    def test_each_code_names_exactly_one_variant(self) -> None:
        codes = [variant.code for variant in get_args(WorkflowStartError.__value__)]

        assert len(codes) == len(set(codes))
        assert set(codes) == set(WorkflowStartErrorCode)

    def test_the_fixture_builds_every_variant(self) -> None:
        built = {type(error) for error in _every_variant(RuntimeError('x'))}

        assert built == set(get_args(WorkflowStartError.__value__))

    def test_the_retryable_variants_carry_a_start_id(self) -> None:
        assert set(get_args(WorkflowStartRetryable.__value__)) == {
            WorkflowStartEnqueueFailed,
            WorkflowStartAsyncContext,
            WorkflowStartLoopRunnerFailed,
        }

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError(CAUSE_MARKER)), ids=_variant_id,
    )
    def test_repr_hides_message_and_cause(self, error: WorkflowStartError) -> None:
        text = repr(error)

        assert MESSAGE_MARKER not in text
        assert CAUSE_MARKER not in text
        assert type(error).__name__ in text

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError('x')), ids=_variant_id,
    )
    def test_every_variant_has_code_retryable_and_the_start_identity(
        self,
        error: WorkflowStartError,
    ) -> None:
        assert isinstance(error.code, WorkflowStartErrorCode)
        assert isinstance(error.retryable, bool)
        assert error.workflow_name == 'wf'
        assert error.workflow_id == WORKFLOW_ID

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError('x')), ids=_variant_id,
    )
    def test_only_an_enqueue_failure_can_be_retryable(
        self,
        error: WorkflowStartError,
    ) -> None:
        match error:
            case WorkflowStartEnqueueFailed():
                assert error.retryable is True
            case _:
                assert error.retryable is False


def _make_spec(name: str = 'conflict-wf') -> Any:
    spec = MagicMock()
    spec.name = name
    spec.tasks = []
    spec.workflow_def_cls = None
    spec.output = None
    spec.success_policy = None
    spec.on_error.value = 'FAIL_WORKFLOW'
    spec.definition_key = None
    return spec


def _session_with_existing_row(existing_name: str) -> tuple[Any, Any]:
    """A session whose workflow INSERT conflicts with a row named ``existing_name``."""
    insert_result = MagicMock()
    insert_result.scalar_one_or_none.return_value = None
    name_row = MagicMock()
    name_row.name = existing_name
    name_result = MagicMock()
    name_result.fetchone.return_value = name_row
    session = AsyncMock()
    session.info = {}
    session.execute = AsyncMock(side_effect=[insert_result, name_result])
    session.commit = AsyncMock()
    session_ctx = AsyncMock()
    session_ctx.__aenter__ = AsyncMock(return_value=session)
    session_ctx.__aexit__ = AsyncMock(return_value=False)
    return session, session_ctx


class TestIdConflict:
    @pytest.mark.asyncio
    async def test_another_workflow_name_on_the_id_is_an_id_conflict(self) -> None:
        session, session_ctx = _session_with_existing_row('other-wf')
        broker = MagicMock()
        broker.ensure_schema_initialized = AsyncMock(return_value=Ok(None))
        attach_resources(
            broker, fake_resources(session_factory=MagicMock(return_value=session_ctx)),
        )

        result = await start_workflow_async(_make_spec(), broker, WORKFLOW_ID)

        assert is_err(result)
        assert isinstance(result.err_value, WorkflowStartIdConflict)
        assert result.err_value.existing_workflow_name == 'other-wf'
        assert result.err_value.workflow_name == 'conflict-wf'
        assert result.err_value.workflow_id == WORKFLOW_ID
        session.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_same_name_on_the_id_returns_the_existing_handle(self) -> None:
        session, session_ctx = _session_with_existing_row('conflict-wf')
        broker = MagicMock()
        broker.ensure_schema_initialized = AsyncMock(return_value=Ok(None))
        attach_resources(
            broker, fake_resources(session_factory=MagicMock(return_value=session_ctx)),
        )

        result = await start_workflow_async(_make_spec(), broker, WORKFLOW_ID)

        assert not is_err(result)
        assert result.ok_value.workflow_id == WORKFLOW_ID
        session.commit.assert_not_awaited()


class TestValidationCause:
    @pytest.mark.asyncio
    async def test_a_refused_spec_keeps_the_validation_error(self) -> None:
        from horsies.core.errors import WorkflowValidationError

        spec = _make_spec()
        task = MagicMock()
        task.queue = None
        task.priority = 100
        task.name = 'unbound-queue'
        spec.tasks = [task]
        broker = MagicMock()

        result = await start_workflow_async(spec, broker, WORKFLOW_ID)

        assert is_err(result)
        assert isinstance(result.err_value, WorkflowStartValidationFailed)
        assert isinstance(result.err_value.exception, WorkflowValidationError)
