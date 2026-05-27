"""Unit tests for `_decode_workflow_envelope` in WorkflowHandle.

Strict-serde phase 5/6 regression coverage. The decode routine must:

1. Reject envelopes whose ``__h_outputless_terminals__`` flag disagrees
   with the handle's ``out_type`` (silent coercion was the bug).
2. Apply the disagreement check BEFORE the err-fast-path so a smuggled
   outputless flag on a typed payload can't slip through the err route.
3. Use `decode_task_error` for polymorphic err decoding so a
   SubWorkflowError emitted by a child workflow round-trips with its
   subclass fields intact.
"""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from horsies.core.codec.typed import Json
from horsies.core.models.tasks import (
    ContractCode,
    OperationalErrorCode,
    SubWorkflowError,
    TaskError,
    TaskResult,
)
from horsies.core.models.workflow.context import SubWorkflowSummary
from horsies.core.models.workflow.enums import WorkflowStatus
from horsies.core.models.workflow.handle import (
    _OUTPUTLESS_TERMINALS,  # pyright: ignore[reportPrivateUsage]
    _decode_workflow_envelope,  # pyright: ignore[reportPrivateUsage]
)


def _typed_envelope(ok: Any) -> dict[str, Any]:
    """Wire-typed (non-outputless) envelope carrying an ok value."""
    return {
        '__h_task_result__': True,
        'ok': ok,
        'err': None,
    }


def _outputless_envelope(
    results_by_id: dict[str, Any],
    task_name_by_id: dict[str, str],
) -> dict[str, Any]:
    """Wire-outputless envelope with embedded per-node results."""
    return {
        '__h_task_result__': True,
        '__h_outputless_terminals__': True,
        'ok': {
            'results_by_id': results_by_id,
            'task_name_by_id': task_name_by_id,
        },
        'err': None,
    }


def _err_envelope(err_payload: dict[str, Any]) -> dict[str, Any]:
    """Wire envelope carrying an err slot (no outputless flag)."""
    return {
        '__h_task_result__': True,
        'ok': None,
        'err': err_payload,
    }


@pytest.mark.unit
class TestWorkflowEnvelopeOutputlessMismatch:
    """Mismatch between wire flag and handle ``out_type`` is a contract
    violation — silent coercion to the wrong shape is the bug."""

    def test_typed_handle_on_outputless_wire_rejected(self) -> None:
        """Handle has a typed ``out_type`` but the envelope is outputless."""
        wire = _outputless_envelope(
            results_by_id={},
            task_name_by_id={},
        )
        result = _decode_workflow_envelope(
            cast(Json, wire),
            out_type=int,
            app=None,
            workflow_id='wf-1',
        )
        assert result.is_err()
        assert result.err is not None
        assert (
            result.err.error_code
            == OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        )
        assert result.err.data is not None
        assert result.err.data.get('wire_outputless') is True
        assert result.err.data.get('handle_outputless') is False

    def test_outputless_handle_on_typed_wire_rejected(self) -> None:
        """Handle is outputless but the envelope carries a typed payload."""
        wire = _typed_envelope(ok=42)
        result = _decode_workflow_envelope(
            cast(Json, wire),
            out_type=_OUTPUTLESS_TERMINALS,
            app=None,
            workflow_id='wf-2',
        )
        assert result.is_err()
        assert result.err is not None
        assert (
            result.err.error_code
            == OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        )
        assert result.err.data is not None
        assert result.err.data.get('wire_outputless') is False
        assert result.err.data.get('handle_outputless') is True

    def test_smuggled_outputless_flag_on_typed_err_envelope_rejected(
        self,
    ) -> None:
        """Outputless flag MUST be checked before err-fast-path.

        Regression: an attacker / cross-version producer could write
        ``__h_outputless_terminals__: True`` next to an err slot and
        slip past the outputless check if the err-fast-path ran first.
        """
        wire = {
            '__h_task_result__': True,
            '__h_outputless_terminals__': True,
            'ok': None,
            'err': {
                'error_code': {'__builtin_task_code__': 'BROKER_ERROR'},
                'message': 'sneak',
                'data': None,
                'exception': None,
            },
        }
        # Handle is typed; smuggled outputless flag must trip mismatch
        # BEFORE the err-fast-path returns a (spurious) decode.
        result = _decode_workflow_envelope(
            cast(Json, wire),
            out_type=int,
            app=None,
            workflow_id='wf-3',
        )
        assert result.is_err()
        assert result.err is not None
        assert (
            result.err.error_code
            == OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        )
        assert result.err.data is not None
        assert result.err.data.get('wire_outputless') is True
        assert result.err.data.get('handle_outputless') is False


@pytest.mark.unit
class TestWorkflowEnvelopeMarkerGuards:
    """The outer envelope grammar (marker + dict-shape) must hold."""

    def test_non_dict_envelope_rejected(self) -> None:
        result = _decode_workflow_envelope(
            cast(Json, 42),
            out_type=int,
            app=None,
            workflow_id='wf-x',
        )
        assert result.is_err()
        assert result.err is not None
        assert (
            result.err.error_code
            == OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        )

    def test_missing_marker_rejected(self) -> None:
        result = _decode_workflow_envelope(
            cast(Json, {'ok': 1, 'err': None}),
            out_type=int,
            app=None,
            workflow_id='wf-x',
        )
        assert result.is_err()
        assert result.err is not None
        assert (
            result.err.error_code
            == OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        )


@pytest.mark.unit
class TestWorkflowEnvelopeErrSlot:
    """Err slot routes through `decode_task_error` so SubWorkflowError
    survives the round-trip."""

    def test_plain_task_error_decoded(self) -> None:
        err_payload = {
            'error_code': {'__builtin_task_code__': 'BROKER_ERROR'},
            'message': 'plain failure',
            'data': None,
            'exception': None,
        }
        wire = _err_envelope(err_payload)
        result = _decode_workflow_envelope(
            cast(Json, wire),
            out_type=int,
            app=None,
            workflow_id='wf-x',
        )
        assert result.is_err()
        assert result.err is not None
        assert isinstance(result.err, TaskError)
        assert not isinstance(result.err, SubWorkflowError)
        assert result.err.message == 'plain failure'

    def test_sub_workflow_error_preserved(self) -> None:
        """SubWorkflowError dumped by the engine round-trips with the
        ``sub_workflow_id`` / ``sub_workflow_summary`` fields intact."""
        original = SubWorkflowError(
            error_code=OperationalErrorCode.UNHANDLED_EXCEPTION,
            message='nested failure',
            sub_workflow_id='wf-child',
            sub_workflow_summary=SubWorkflowSummary(
                status=WorkflowStatus.FAILED,
                output=None,
                total_tasks=3,
                completed_tasks=1,
                failed_tasks=2,
                skipped_tasks=0,
                error_summary='downstream',
            ),
        )
        wire = _err_envelope(original.model_dump(mode='json'))
        result = _decode_workflow_envelope(
            cast(Json, wire),
            out_type=int,
            app=None,
            workflow_id='wf-parent',
        )
        assert result.is_err()
        assert isinstance(result.err, SubWorkflowError)
        assert result.err is not None
        decoded = result.err
        assert isinstance(decoded, SubWorkflowError)
        assert decoded.sub_workflow_id == 'wf-child'
        assert decoded.sub_workflow_summary.failed_tasks == 2


@pytest.mark.unit
class TestWorkflowEnvelopeOutputlessPath:
    """Outputless path decodes per-node envelopes using the app
    registry. Tests cover the embedded ``task_name_by_id`` mapping."""

    @staticmethod
    def _make_app_with_task(task_name: str, ok_type: Any) -> Any:
        """Build a mock app whose task registry knows one task."""
        app = MagicMock()
        task = MagicMock()
        task.task_ok_type = ok_type
        app.tasks = {task_name: task}
        return app

    def test_per_node_decode_uses_embedded_task_names(self) -> None:
        app = self._make_app_with_task('task_a', int)
        wire = _outputless_envelope(
            results_by_id={
                'node-0': {
                    '__h_task_result__': True,
                    'ok': 7,
                    'err': None,
                },
            },
            task_name_by_id={'node-0': 'task_a'},
        )
        result = _decode_workflow_envelope(
            cast(Json, wire),
            out_type=_OUTPUTLESS_TERMINALS,
            app=app,
            workflow_id='wf-out',
        )
        assert result.is_ok()
        results: dict[str, TaskResult[Any, TaskError]] = result.unwrap()
        assert 'node-0' in results
        node_result = results['node-0']
        assert node_result.is_ok()
        assert node_result.ok == 7

    def test_unknown_task_name_records_no_type_available(self) -> None:
        """Source task missing from the registry → ``NO_TYPE_AVAILABLE``
        sentinel on that node, not a fatal workflow error."""
        app = self._make_app_with_task('task_a', int)
        wire = _outputless_envelope(
            results_by_id={
                'node-0': {
                    '__h_task_result__': True,
                    'ok': 7,
                    'err': None,
                },
            },
            task_name_by_id={'node-0': 'unknown_task'},
        )
        result = _decode_workflow_envelope(
            cast(Json, wire),
            out_type=_OUTPUTLESS_TERMINALS,
            app=app,
            workflow_id='wf-out',
        )
        assert result.is_ok()
        results = result.unwrap()
        node = results['node-0']
        assert node.is_err()
        assert node.err is not None
        assert node.err.error_code == ContractCode.NO_TYPE_AVAILABLE
