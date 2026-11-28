"""Unit tests for `_decode_workflow_envelope` in WorkflowHandle.

Strict-serde phase 5/6 regression coverage. The decode routine must:

1. Read the plain envelope of a workflow with no output node
   (``ok`` null) as ``NoWorkflowOutput``, and refuse a typed payload for
   that type.
2. Refuse the old outputless envelope (migrations before 1009 stored the
   key ``__h_outputless_terminals__`` and a map of the terminal nodes in
   ``ok``) for every ``output_type``: it is not an envelope of the strict
   form.
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


def _old_outputless_envelope(
    results_by_id: dict[str, Any],
    task_name_by_id: dict[str, str],
) -> dict[str, Any]:
    """The envelope that migrations before 1009 stored for a workflow with
    no output node: the marker and the embedded per-node results."""
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
    """The stored form and the handle's ``output_type`` must agree."""

    def test_typed_handle_on_the_old_outputless_wire_rejected(self) -> None:
        wire = _old_outputless_envelope(results_by_id={}, task_name_by_id={})
        result = _decode_workflow_envelope(
            cast(Json, wire),
            output_type=int,
            app=None,
            workflow_id='wf-1',
        )
        assert '__h_outputless_terminals__' in _unreadable_message(result)

    def test_no_workflow_output_handle_on_a_typed_wire_rejected(self) -> None:
        wire = _typed_envelope(ok=42)
        result = _decode_workflow_envelope(
            cast(Json, wire),
            output_type=NoWorkflowOutput,
            app=None,
            workflow_id='wf-2',
        )
        _unreadable_message(result)


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
    """A handle of ``NoWorkflowOutput`` reads the plain envelope of a
    workflow with no output node."""

    def test_plain_envelope_reads_as_no_workflow_output(self) -> None:
        result = _decode_workflow_envelope(
            cast(Json, _typed_envelope(ok=None)),
            output_type=NoWorkflowOutput,
            app=None,
            workflow_id='wf-out',
        )
        assert result == Ok(Succeeded(NoWorkflowOutput()))

    def test_plain_envelope_with_an_err_slot_reads_as_failed(self) -> None:
        wire = _typed_envelope(ok=None)
        wire['err'] = encode_task_error(
            TaskError(code=OperationalCode.UNHANDLED_EXCEPTION, message='boom'),
        )
        result = _decode_workflow_envelope(
            cast(Json, wire),
            output_type=NoWorkflowOutput,
            app=None,
            workflow_id='wf-out',
        )
        match result:
            case Ok(Failed(task_error)):
                assert task_error.message == 'boom'
            case _:
                pytest.fail(f'expected Ok(Failed(...)), got {result!r}')

    def test_a_row_of_the_old_outputless_form_is_unreadable(self) -> None:
        wire = _old_outputless_envelope(
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
            output_type=NoWorkflowOutput,
            app=None,
            workflow_id='wf-old',
        )
        assert '__h_outputless_terminals__' in _unreadable_message(result)
