"""``WorkflowHandle.for_id``: the parse of a workflow id text.

``for_id`` returns ``Ok(handle)`` for each spelling of a UUID that
``uuid.UUID`` accepts; the handle holds the UUID, whose text is the canonical
form. Any other text is ``Err(WorkflowHandleInvalidWorkflowId)`` with the text and
the ``ValueError`` of ``uuid.UUID``. The parse reads nothing.
"""

from __future__ import annotations

import dataclasses
import uuid
from unittest.mock import MagicMock

import pytest

from horsies import WorkflowHandleInvalidWorkflowId, WorkflowHandleError, WorkflowHandle
from horsies.core.types.result import is_err, is_ok
from horsies.core.models.workflow.handle_types import WorkflowHandleErrorCode
from tests.union_members import union_members

pytestmark = [pytest.mark.unit]

CANONICAL = '3c9e1f4a-6b2d-4c58-8e71-0a1b2c3d4e5f'
"""The canonical text of the workflow id of every accepted spelling."""

ACCEPTED_SPELLINGS: dict[str, str] = {
    'canonical': CANONICAL,
    'upper_case': CANONICAL.upper(),
    'braces': '{' + CANONICAL + '}',
    'urn_prefix': 'urn:uuid:' + CANONICAL,
    'uuid_prefix': 'uuid:' + CANONICAL,
    'no_hyphens': CANONICAL.replace('-', ''),
    'some_hyphens': CANONICAL[:8] + '-' + CANONICAL[9:].replace('-', ''),
    'upper_case_braces_no_hyphens': '{' + CANONICAL.replace('-', '').upper() + '}',
}
"""Spellings of one UUID that ``uuid.UUID`` accepts."""

REFUSED_TEXTS: dict[str, str] = {
    'empty': '',
    'words': 'not-a-uuid',
    'one_digit_short': CANONICAL[:-1],
    'one_digit_more': CANONICAL + '0',
    'not_hex': CANONICAL.replace('3', 'g'),
    'space_before': ' ' + CANONICAL,
}
"""Texts that ``uuid.UUID`` refuses."""


@pytest.mark.parametrize(
    'text', list(ACCEPTED_SPELLINGS.values()), ids=list(ACCEPTED_SPELLINGS),
)
def test_each_spelling_that_uuid_accepts_gives_the_canonical_id(text: str) -> None:
    assert uuid.UUID(text) == uuid.UUID(CANONICAL)
    broker = MagicMock()

    result = WorkflowHandle.for_id(text, broker)

    assert is_ok(result)
    handle = result.ok_value
    assert type(handle.workflow_id) is uuid.UUID
    assert handle.workflow_id == uuid.UUID(CANONICAL)
    assert str(handle.workflow_id) == CANONICAL
    assert handle.broker is broker
    assert handle.out_type is None
    assert broker.mock_calls == []


@pytest.mark.parametrize('text', list(REFUSED_TEXTS.values()), ids=list(REFUSED_TEXTS))
def test_text_that_is_not_a_uuid_is_an_invalid_workflow_id(text: str) -> None:
    with pytest.raises(ValueError) as parse_error:
        uuid.UUID(text)
    broker = MagicMock()

    result = WorkflowHandle.for_id(text, broker)

    assert is_err(result)
    error = result.err_value
    assert type(error) is WorkflowHandleInvalidWorkflowId
    assert error.workflow_id_text == text
    assert type(error.cause) is ValueError
    assert str(error.cause) == str(parse_error.value)
    assert error.message == f'workflow id {text!r} is not a UUID: {parse_error.value}'
    assert broker.mock_calls == []


def test_the_same_text_gives_the_same_error() -> None:
    broker = MagicMock()

    first = WorkflowHandle.for_id('not-a-uuid', broker)
    second = WorkflowHandle.for_id('not-a-uuid', broker)

    assert is_err(first)
    assert is_err(second)
    assert first.err_value.message == second.err_value.message
    assert first.err_value.workflow_id_text == second.err_value.workflow_id_text


def test_the_error_is_frozen_and_its_repr_hides_the_message_and_the_cause() -> None:
    marker = 'cause-marker-5b1c'
    error = WorkflowHandleInvalidWorkflowId(
        message=f'message {marker}',
        workflow_id_text='not-a-uuid',
        cause=ValueError(marker),
    )

    assert marker not in repr(error)
    assert 'not-a-uuid' in repr(error)
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(error, 'workflow_id_text', 'other')


def test_the_error_is_not_a_handle_operation_failure() -> None:
    assert WorkflowHandleInvalidWorkflowId not in union_members(WorkflowHandleError)
    assert (
        WorkflowHandleInvalidWorkflowId.code
        is WorkflowHandleErrorCode.INVALID_WORKFLOW_ID
    )
