"""Unit tests for `_decode_workflow_envelope` in WorkflowHandle.

Strict-serde phase 5/6 regression coverage. The decode routine must:

1. Reject envelopes whose ``__h_outputless_terminals__`` flag disagrees
   with the handle's ``output_type`` (silent coercion was the bug).
2. Apply the disagreement check BEFORE the err-fast-path so a smuggled
   outputless flag on a typed payload can't slip through the err route.
3. Use `decode_task_error` for polymorphic err decoding so a
   SubWorkflowFailure emitted by a child workflow round-trips with its
   subclass fields intact.
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from horsies.core.codec.typed import Json, encode_task_error
from horsies.core.models.outcome import (
    Failed,
    ReadResult,
    ReadResultDeserializationError,
    Succeeded,
)
from horsies.core.models.tasks import (
    OperationalCode,
    SubWorkflowFailure,
    TaskError,
)
from horsies.core.models.workflow.context import SubWorkflowSummary
from horsies.core.models.workflow.enums import WorkflowStatus
from horsies.core.models.workflow.no_output import NoWorkflowOutput
from horsies.core.models.workflow.handle import (
    _OUTPUTLESS_WORKFLOW,  # pyright: ignore[reportPrivateUsage]
    _decode_workflow_envelope,  # pyright: ignore[reportPrivateUsage]
)
from horsies.core.types.result import Err, Ok


def _unreadable_message(result: ReadResult[Any, TaskError]) -> str:
    """The message of a read that is ``Err(ReadResultDeserializationError)``."""
    match result:
        case Err(ReadResultDeserializationError(message=message)):
            return message
        case _:
            pytest.fail(f'expected Err(ReadResultDeserializationError), got {result!r}')


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
    """Mismatch between wire flag and handle ``output_type`` is a contract
    violation — silent coercion to the wrong shape is the bug."""

    def test_typed_handle_on_outputless_wire_rejected(self) -> None:
        """Handle has a typed ``output_type`` but the envelope is outputless."""
        wire = _outputless_envelope(
            results_by_id={},
            task_name_by_id={},
        )
        result = _decode_workflow_envelope(
            cast(Json, wire),
            output_type=int,
            app=None,
            workflow_id='wf-1',
        )
        message = _unreadable_message(result)
        assert 'wire_outputless=True, handle_outputless=False' in message

    def test_outputless_handle_on_typed_wire_rejected(self) -> None:
        """Handle is outputless but the envelope carries a typed payload."""
        wire = _typed_envelope(ok=42)
        result = _decode_workflow_envelope(
            cast(Json, wire),
            output_type=_OUTPUTLESS_WORKFLOW,
            app=None,
            workflow_id='wf-2',
        )
        message = _unreadable_message(result)
        assert 'wire_outputless=False, handle_outputless=True' in message

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
                'error_code': {'__builtin_task_code__': 'DATABASE_ERROR'},
                'message': 'sneak',
                'data': None,
                'exception': None,
            },
        }
        # Handle is typed; smuggled outputless flag must trip mismatch
        # BEFORE the err-fast-path returns a (spurious) decode.
        result = _decode_workflow_envelope(
            cast(Json, wire),
            output_type=int,
            app=None,
            workflow_id='wf-3',
        )
        message = _unreadable_message(result)
        assert 'wire_outputless=True, handle_outputless=False' in message


@pytest.mark.unit
class TestWorkflowEnvelopeMarkerGuards:
    """The outer envelope grammar (marker + dict-shape) must hold."""

    def test_non_dict_envelope_rejected(self) -> None:
        result = _decode_workflow_envelope(
            cast(Json, 42),
            output_type=int,
            app=None,
            workflow_id='wf-x',
        )
        assert isinstance(result, Err)
        assert isinstance(result.err_value, ReadResultDeserializationError)

    def test_missing_marker_rejected(self) -> None:
        result = _decode_workflow_envelope(
            cast(Json, {'ok': 1, 'err': None}),
            output_type=int,
            app=None,
            workflow_id='wf-x',
        )
        assert isinstance(result, Err)
        assert isinstance(result.err_value, ReadResultDeserializationError)


@pytest.mark.unit
class TestWorkflowEnvelopeErrSlot:
    """Err slot routes through `decode_task_error` so SubWorkflowFailure
    survives the round-trip."""

    def test_plain_task_error_decoded(self) -> None:
        err_payload = {
            'error_code': {'__builtin_task_code__': 'DATABASE_ERROR'},
            'message': 'plain failure',
            'data': None,
            'exception': None,
        }
        wire = _err_envelope(err_payload)
        result = _decode_workflow_envelope(
            cast(Json, wire),
            output_type=int,
            app=None,
            workflow_id='wf-x',
        )
        match result:
            case Ok(Failed(task_error)):
                assert isinstance(task_error, TaskError)
                assert not isinstance(task_error, SubWorkflowFailure)
                assert task_error.message == 'plain failure'
            case _:
                pytest.fail(f'expected a failed outcome, got {result!r}')

    def test_sub_workflow_error_preserved(self) -> None:
        """SubWorkflowFailure in its stored form round-trips with the
        ``sub_workflow_id`` / ``sub_workflow_summary`` fields intact."""
        original = SubWorkflowFailure(
            code=OperationalCode.UNHANDLED_EXCEPTION,
            message='nested failure',
            sub_workflow_id='wf-child',
            sub_workflow_summary=SubWorkflowSummary(
                status=WorkflowStatus.FAILED,
                output=None,
                total_nodes=3,
                completed_nodes=1,
                failed_nodes=2,
                skipped_nodes=0,
                error_summary='downstream',
            ),
        )
        wire = _err_envelope(encode_task_error(original))
        result = _decode_workflow_envelope(
            cast(Json, wire),
            output_type=int,
            app=None,
            workflow_id='wf-parent',
        )
        match result:
            case Ok(Failed(SubWorkflowFailure() as decoded)):
                assert decoded.sub_workflow_id == 'wf-child'
                assert decoded.sub_workflow_summary.failed_nodes == 2
            case _:
                pytest.fail(f'expected Ok(Failed(SubWorkflowFailure)), got {result!r}')


@pytest.mark.unit
class TestWorkflowEnvelopeOutputlessPath:
    """An outputless handle reads the outputless envelope as
    ``NoWorkflowOutput``; the embedded per-node map is not read."""

    def test_outputless_wire_reads_as_no_workflow_output(self) -> None:
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
            output_type=_OUTPUTLESS_WORKFLOW,
            app=None,
            workflow_id='wf-out',
        )
        assert result == Ok(Succeeded(NoWorkflowOutput()))

    def test_outputless_wire_with_an_err_slot_reads_as_failed(self) -> None:
        wire = _outputless_envelope(results_by_id={}, task_name_by_id={})
        wire['err'] = encode_task_error(
            TaskError(code=OperationalCode.UNHANDLED_EXCEPTION, message='boom'),
        )
        result = _decode_workflow_envelope(
            cast(Json, wire),
            output_type=_OUTPUTLESS_WORKFLOW,
            app=None,
            workflow_id='wf-out',
        )
        match result:
            case Ok(Failed(task_error)):
                assert task_error.message == 'boom'
            case _:
                pytest.fail(f'expected Ok(Failed(...)), got {result!r}')
