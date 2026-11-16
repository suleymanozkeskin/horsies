"""The read of a workflow's stored success policy.

Each accepted form gives ``NoSuccessPolicy`` or the typed cases; each refused
form gives ``SuccessPolicyUnreadable`` naming the workflow and the rule.
"""

from __future__ import annotations

import pytest

from horsies.core.types.result import Err, Ok
from horsies.core.workflows.stored_success_policy import (
    NoSuccessPolicy,
    StoredSuccessCase,
    StoredSuccessPolicy,
    StoredSuccessPolicyError,
    SuccessPolicyUnreadable,
    evaluate_success_cases,
    read_stored_success_policy,
    required_indices_of,
)

pytestmark = [pytest.mark.unit]

_WORKFLOW = 'wf-1'

_ACCEPTED_WITH_EXTRAS: list[object] = [
    {'cases': [], 'optional_indices': None},
    {'cases': [], 'extra': 1},
    {'cases': [{'required_indices': [], 'extra': 'x'}]},
]

_REFUSED: list[tuple[object, str]] = [
    ('{"cases":', 'the text is not JSON'),
    ([1, 2, 3], 'the policy is a JSON array, not an object'),
    ('"text"', 'the policy is a JSON string, not an object'),
    (7, 'the policy is a JSON number, not an object'),
    ({}, 'the policy has no cases'),
    ({'cases': 'not a list'}, 'cases is a JSON string, not an array'),
    ({'cases': None}, 'cases is a JSON null, not an array'),
    ({'cases': [1]}, 'cases[0]: the case is a JSON number, not an object'),
    ({'cases': [{}]}, 'cases[0]: the case has no required_indices'),
    (
        {'cases': [{'required_indices': None}]},
        'cases[0]: required_indices: a JSON null, not an array',
    ),
    (
        {'cases': [{'required_indices': [0]}, {'required_indices': [True]}]},
        'cases[1]: required_indices: item 0 is a JSON boolean, not an index',
    ),
    (
        {'cases': [{'required_indices': [0, -1]}]},
        'cases[0]: required_indices: item 1 is below 0',
    ),
    (
        {'cases': [{'required_indices': [1.0]}]},
        'cases[0]: required_indices: item 0 is a JSON number, not an index',
    ),
    (
        {'cases': [{'required_indices': ['0']}]},
        'cases[0]: required_indices: item 0 is a JSON string, not an index',
    ),
    (
        {'cases': [], 'optional_indices': 'x'},
        'optional_indices: a JSON string, not an array',
    ),
]


class TestAccepted:
    @pytest.mark.parametrize('stored', [None, 'null'], ids=['sql-null', 'json-null-text'])
    def test_null_is_no_policy(self, stored: object) -> None:
        assert read_stored_success_policy(_WORKFLOW, stored) == Ok(NoSuccessPolicy())

    def test_the_stored_form_of_the_start(self) -> None:
        stored = {
            'cases': [{'required_indices': [0, 2]}, {'required_indices': [1]}],
            'optional_indices': [3],
        }
        assert read_stored_success_policy(_WORKFLOW, stored) == Ok(
            StoredSuccessPolicy(
                cases=(
                    StoredSuccessCase(required_indices=(0, 2)),
                    StoredSuccessCase(required_indices=(1,)),
                ),
            ),
        )

    def test_text_is_read_as_json(self) -> None:
        stored = '{"cases":[{"required_indices":[0]}]}'
        assert read_stored_success_policy(_WORKFLOW, stored) == Ok(
            StoredSuccessPolicy(cases=(StoredSuccessCase(required_indices=(0,)),)),
        )

    @pytest.mark.parametrize(
        'stored',
        _ACCEPTED_WITH_EXTRAS,
        ids=['optional-null', 'unknown-policy-key', 'unknown-case-key'],
    )
    def test_null_optional_indices_and_unknown_keys_are_accepted(
        self, stored: object,
    ) -> None:
        match read_stored_success_policy(_WORKFLOW, stored):
            case Ok(ok_value=StoredSuccessPolicy()):
                pass
            case other:
                raise AssertionError(f'expected a policy, got {other!r}')


class TestRefused:
    @pytest.mark.parametrize(
        ('stored', 'cause'),
        _REFUSED,
        ids=[
            'not-json', 'array', 'string', 'number', 'no-cases', 'cases-string',
            'cases-null', 'case-number', 'case-without-required', 'required-null',
            'required-bool', 'required-negative', 'required-float',
            'required-string', 'optional-string',
        ],
    )
    def test_a_value_that_does_not_parse_is_unreadable(
        self, stored: object, cause: str,
    ) -> None:
        match read_stored_success_policy(_WORKFLOW, stored):
            case Err(err_value=SuccessPolicyUnreadable(workflow_id=workflow_id, cause=text)):
                assert workflow_id == _WORKFLOW
                assert text.startswith(cause)
            case other:
                raise AssertionError(f'expected an unreadable policy, got {other!r}')

    def test_the_error_names_the_workflow_and_the_cause(self) -> None:
        error = StoredSuccessPolicyError(
            SuccessPolicyUnreadable(workflow_id=_WORKFLOW, cause='the policy has no cases'),
        )
        assert str(error) == (
            'stored success_policy of workflow wf-1 does not parse: '
            'the policy has no cases'
        )


class TestEvaluation:
    def test_a_case_whose_required_nodes_completed_is_met(self) -> None:
        policy = StoredSuccessPolicy(
            cases=(
                StoredSuccessCase(required_indices=(0, 1)),
                StoredSuccessCase(required_indices=(2,)),
            ),
        )
        assert evaluate_success_cases(policy, {0: 'COMPLETED', 1: 'FAILED', 2: 'COMPLETED'})
        assert not evaluate_success_cases(policy, {0: 'COMPLETED', 1: 'FAILED', 2: 'SKIPPED'})

    def test_a_case_with_no_required_node_is_skipped(self) -> None:
        policy = StoredSuccessPolicy(cases=(StoredSuccessCase(required_indices=()),))
        assert not evaluate_success_cases(policy, {})

    def test_required_indices_of_every_case(self) -> None:
        policy = StoredSuccessPolicy(
            cases=(
                StoredSuccessCase(required_indices=(0, 1)),
                StoredSuccessCase(required_indices=(1, 3)),
            ),
        )
        assert required_indices_of(policy) == {0, 1, 3}
