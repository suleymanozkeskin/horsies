"""The workflow start error variants: one per code, redacted, and the id-conflict site.

``WorkflowStartError`` is a union with one frozen variant per
``WorkflowStartErrorCode`` member. A variant's repr hides the message and
the cause. Every variant carries ``workflow_name`` and ``workflow_id``.

A refused spec keeps its ``WorkflowValidationError`` as the cause
(``start_workflow_async``); ``test_workflow_start_through_core.py`` tests
the id conflict that the core answers.
"""

from __future__ import annotations

from typing import Any, get_args
from unittest.mock import MagicMock

import pytest

from horsies.core.types.result import is_err
from horsies.core.workflows.lifecycle import start_workflow_async
from horsies.core.workflows.start_types import (
    WorkflowStartAsyncContext,
    WorkflowStartBrokerNotConfigured,
    WorkflowStartCommandConflict,
    WorkflowStartEnqueueFailed,
    WorkflowStartError,
    WorkflowStartErrorCode,
    WorkflowStartIdConflict,
    WorkflowStartRetryable,
    WorkflowStartValidationFailed,
)
from tests.union_members import union_members

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
        WorkflowStartCommandConflict(message=message, **ids),
        WorkflowStartEnqueueFailed(
            message=message, retryable=True, exception=cause, **ids,
        ),
        WorkflowStartAsyncContext(message=message, **ids),
    ]


def _variant_id(error: WorkflowStartError) -> str:
    return type(error).__name__


class TestVariantCatalog:
    def test_each_code_names_exactly_one_variant(self) -> None:
        codes = [variant.code for variant in union_members(WorkflowStartError)]

        assert len(codes) == len(set(codes))
        assert set(codes) == set(WorkflowStartErrorCode)

    def test_the_fixture_builds_every_variant(self) -> None:
        built = {type(error) for error in _every_variant(RuntimeError('x'))}

        assert built == set(union_members(WorkflowStartError))

    def test_the_retryable_variants_carry_a_start_id(self) -> None:
        assert set(get_args(WorkflowStartRetryable.__value__)) == {
            WorkflowStartEnqueueFailed,
            WorkflowStartAsyncContext,
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
        broker.app = None

        result = await start_workflow_async(spec, broker, WORKFLOW_ID)

        assert is_err(result)
        assert isinstance(result.err_value, WorkflowStartValidationFailed)
        assert isinstance(result.err_value.exception, WorkflowValidationError)
