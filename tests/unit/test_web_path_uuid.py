"""The text of a web path id becomes a uuid.UUID, or a refusal."""

from __future__ import annotations

from uuid import UUID

import pytest

from tests.dualrun.reference.web_routes._common import PathIdNotUuid, parse_path_uuid

pytestmark = [pytest.mark.unit]

TASK_ID = UUID('0f8fad5b-d9cb-469f-a165-70867728950e')


@pytest.mark.parametrize(
    'text',
    [
        '0f8fad5b-d9cb-469f-a165-70867728950e',
        '0F8FAD5B-D9CB-469F-A165-70867728950E',
        '0f8fad5bd9cb469fa16570867728950e',
        '{0f8fad5b-d9cb-469f-a165-70867728950e}',
        'urn:uuid:0f8fad5b-d9cb-469f-a165-70867728950e',
    ],
    ids=['canonical', 'upper_case', 'hex_digits_only', 'braces', 'urn_prefix'],
)
def test_uuid_text_parses_to_the_uuid(text: str) -> None:
    assert parse_path_uuid('task_id', text) == TASK_ID


@pytest.mark.parametrize(
    'text',
    [
        'not-a-uuid',
        '',
        '0f8fad5b-d9cb-469f-a165-70867728950',
        '+f8fad5bd9cb469fa16570867728950e',
    ],
    ids=['word', 'empty', 'short_last_group', 'sign_that_uuid_UUID_accepts'],
)
def test_other_text_is_refused_with_the_parse_error(text: str) -> None:
    match parse_path_uuid('task_id', text):
        case PathIdNotUuid() as refused:
            assert refused.parameter == 'task_id'
            assert refused.reason.startswith('Input should be a valid UUID, ')
            assert refused.detail == f'Path parameter task_id: {refused.reason}.'
        case UUID() as parsed:
            raise AssertionError(f'{text!r} parsed to {parsed}')
