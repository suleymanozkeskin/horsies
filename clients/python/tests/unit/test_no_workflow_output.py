"""``NoWorkflowOutput``: the value of a sub-workflow node of a child with no output node.

The core stores the node's result as ``Ok(null)``. A read gives
``Succeeded(NoWorkflowOutput())``, and a task parameter can receive it.
"""

from __future__ import annotations

import pytest
from pydantic import TypeAdapter

from horsies import NoWorkflowOutput
from horsies.core.codec.signature_check import check_task_signature
from horsies.core.codec.typed import Json, decode_task_result
from horsies.core.codec.validation_errors import ValueValidationError
from horsies.core.models.outcome import Failed, Succeeded, TaskOutcome
from horsies.core.models.tasks import TaskError
from horsies.core.types.result import Ok, Result

pytestmark = [pytest.mark.unit]


def _ok_envelope(value: Json) -> dict[str, Json]:
    return {'__h_task_result__': True, 'ok': value, 'err': None}


def test_a_stored_null_reads_as_no_workflow_output() -> None:
    outcome = decode_task_result(_ok_envelope(None), NoWorkflowOutput)

    assert outcome == Succeeded(NoWorkflowOutput())
    assert isinstance(outcome, Succeeded)
    assert outcome.value is not None


def test_a_none_output_still_reads_as_none() -> None:
    outcome = decode_task_result(_ok_envelope(None), type(None))

    assert outcome == Succeeded(None)
    assert outcome != Succeeded(NoWorkflowOutput())


@pytest.mark.parametrize('stored', [5, 'null', {}, [], False])
def test_a_stored_value_other_than_null_is_refused(stored: Json) -> None:
    with pytest.raises(ValueValidationError):
        _ = decode_task_result(_ok_envelope(stored), NoWorkflowOutput)


def test_a_stored_error_reads_as_failed() -> None:
    outcome = decode_task_result(
        {
            '__h_task_result__': True,
            'ok': None,
            'err': {'error_code': 'MY_CODE', 'message': 'broke'},
        },
        NoWorkflowOutput,
    )

    assert isinstance(outcome, Failed)
    assert outcome.error.code == 'MY_CODE'


def test_no_workflow_output_is_stored_as_null() -> None:
    adapter = TypeAdapter(NoWorkflowOutput)

    assert adapter.dump_python(NoWorkflowOutput(), mode='json') is None
    assert adapter.dump_json(NoWorkflowOutput()) == b'null'


def test_every_no_workflow_output_is_equal() -> None:
    assert NoWorkflowOutput() == NoWorkflowOutput()
    assert hash(NoWorkflowOutput()) == hash(NoWorkflowOutput())


def test_a_task_takes_the_outcome_of_an_outputless_child() -> None:
    def after_child(
        *, child: TaskOutcome[NoWorkflowOutput, TaskError],
    ) -> Result[None, TaskError]:
        _ = child
        return Ok(None)

    check_task_signature(after_child, task_name='after_child')
