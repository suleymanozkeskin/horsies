"""TaskError subclass serialization and rehydration tests."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.codec.serde import (
    Json,
    dumps_json,
    loads_json,
    task_error_from_json,
    task_result_from_json,
)
from horsies.core.models.tasks import OutcomeCode, SubWorkflowError, TaskResult
from horsies.core.models.workflow import (
    SubWorkflowSummary,
    WorkflowHandle,
    WorkflowStatus,
)
from horsies.core.types.result import Ok, is_err

pytestmark = [pytest.mark.unit]


def _summary() -> SubWorkflowSummary[Any]:
    return SubWorkflowSummary(
        status=WorkflowStatus.FAILED,
        output=None,
        total_tasks=3,
        completed_tasks=1,
        failed_tasks=1,
        skipped_tasks=1,
        error_summary='child failed',
    )


def _summary_json() -> dict[str, Any]:
    return {
        'status': 'FAILED',
        'output': None,
        'total_tasks': 3,
        'completed_tasks': 1,
        'failed_tasks': 1,
        'skipped_tasks': 1,
        'error_summary': 'child failed',
    }


def test_task_result_roundtrip_preserves_subworkflow_error_subtype() -> None:
    err = SubWorkflowError(
        error_code=OutcomeCode.SUBWORKFLOW_FAILED,
        message='child failed',
        sub_workflow_id='child-1',
        sub_workflow_summary=_summary(),
    )

    raw = dumps_json(TaskResult(err=err)).unwrap()
    payload = loads_json(raw).unwrap()
    assert isinstance(payload, dict)
    assert isinstance(payload['err'], dict)
    assert (
        payload['err']['__task_error_type__']
        == 'horsies.core.models.tasks.SubWorkflowError'
    )

    restored_result = task_result_from_json(payload).unwrap()

    assert restored_result.is_err()
    restored_err = restored_result.err
    assert isinstance(restored_err, SubWorkflowError)
    assert restored_err.error_code is OutcomeCode.SUBWORKFLOW_FAILED
    assert restored_err.sub_workflow_id == 'child-1'
    assert restored_err.sub_workflow_summary.status is WorkflowStatus.FAILED
    assert restored_err.sub_workflow_summary.failed_tasks == 1


def test_task_error_from_json_preserves_subworkflow_error_subtype() -> None:
    err = SubWorkflowError(
        error_code=OutcomeCode.SUBWORKFLOW_FAILED,
        message='child failed',
        sub_workflow_id='child-1',
        sub_workflow_summary=_summary(),
    )
    raw = dumps_json(err).unwrap()
    payload = loads_json(raw).unwrap()

    restored = task_error_from_json(payload).unwrap()

    assert isinstance(restored, SubWorkflowError)
    assert restored.sub_workflow_id == 'child-1'
    assert restored.sub_workflow_summary.error_summary == 'child failed'


def test_legacy_subworkflow_error_payload_rehydrates_by_shape() -> None:
    legacy_payload = cast(Json, {
        '__task_result__': True,
        'ok': None,
        'err': {
            '__task_error__': True,
            'error_code': {'__builtin_task_code__': 'SUBWORKFLOW_FAILED'},
            'message': 'child failed',
            'sub_workflow_id': 'legacy-child',
            'sub_workflow_summary': _summary_json(),
        },
    })

    restored_result = task_result_from_json(legacy_payload).unwrap()

    assert restored_result.is_err()
    restored_err = restored_result.err
    assert isinstance(restored_err, SubWorkflowError)
    assert restored_err.error_code is OutcomeCode.SUBWORKFLOW_FAILED
    assert restored_err.sub_workflow_id == 'legacy-child'
    assert restored_err.sub_workflow_summary.total_tasks == 3


def test_unknown_task_error_type_tag_fails_instead_of_guessing() -> None:
    result = task_error_from_json({
        '__task_error__': True,
        '__task_error_type__': 'example.UnknownTaskError',
        'error_code': 'UNKNOWN',
        'message': 'unknown typed payload',
    })

    assert is_err(result)
    assert 'Unknown TaskError type tag' in str(result.err_value)


@pytest.mark.asyncio
async def test_workflow_handle_error_preserves_subworkflow_error_subtype() -> None:
    err = SubWorkflowError(
        error_code=OutcomeCode.SUBWORKFLOW_FAILED,
        message='child failed',
        sub_workflow_id='child-1',
        sub_workflow_summary=_summary(),
    )
    row = SimpleNamespace(error=dumps_json(err).unwrap(), status='FAILED')
    query_result = MagicMock()
    query_result.fetchone.return_value = row
    session = MagicMock()
    session.execute = AsyncMock(return_value=query_result)
    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=session)
    session_cm.__aexit__ = AsyncMock(return_value=False)
    broker = MagicMock()
    broker.session_factory.return_value = session_cm
    broker.listener.listen = AsyncMock(side_effect=RuntimeError)
    handle: WorkflowHandle[Any] = WorkflowHandle(
        workflow_id='workflow-1',
        broker=cast(Any, broker),
    )
    handle.status_async = cast(Any, AsyncMock(return_value=Ok(WorkflowStatus.FAILED)))

    result = await handle.get_async(timeout_ms=1)

    assert result.is_err()
    restored_err = result.err
    assert isinstance(restored_err, SubWorkflowError)
    assert restored_err.sub_workflow_id == 'child-1'
    assert restored_err.sub_workflow_summary.status is WorkflowStatus.FAILED
