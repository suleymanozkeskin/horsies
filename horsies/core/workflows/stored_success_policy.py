"""The success policy that a workflow row stores, read at completion.

The workflow start stores a success policy as
``{"cases": [{"required_indices": [int, ...]}, ...], "optional_indices": [int, ...]}``
(``optional_indices`` only when the spec has optional nodes), or SQL NULL when
the spec has none. The completion step reads it with
``read_stored_success_policy``. A stored value that does not parse fails the
step (``StoredSuccessPolicyError``); the workflow recovery limit then fails
the workflow closed. Keys other than ``cases``, ``required_indices`` and
``optional_indices`` are ignored, as the core's reader ignores them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from horsies.core.codec.json_io import Json, loads_json
from horsies.core.types.result import Err, Ok, Result


@dataclass(frozen=True, slots=True)
class NoSuccessPolicy:
    """The workflow stores no success policy: any FAILED node fails it."""


@dataclass(frozen=True, slots=True)
class StoredSuccessCase:
    """One case: the indices of the nodes that must be COMPLETED."""

    required_indices: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class StoredSuccessPolicy:
    """The cases of a stored policy. The workflow succeeds when every required
    node of one case with required nodes is COMPLETED. ``optional_indices``
    is checked by the read and is not used by the evaluation."""

    cases: tuple[StoredSuccessCase, ...]


type SuccessPolicyRead = NoSuccessPolicy | StoredSuccessPolicy


@dataclass(frozen=True, slots=True)
class SuccessPolicyUnreadable:
    """The stored success policy of ``workflow_id`` does not parse.

    ``cause`` names the field and the rule. It holds no stored value.
    """

    workflow_id: str
    cause: str


class StoredSuccessPolicyError(Exception):
    """A completion step read a stored success policy that does not parse.

    The step fails and its transaction rolls back. The raise is persistent,
    so the workflow recovery limit counts it and fails the workflow closed.
    """

    def __init__(self, unreadable: SuccessPolicyUnreadable) -> None:
        super().__init__(
            f'stored success_policy of workflow {unreadable.workflow_id} '
            f'does not parse: {unreadable.cause}',
        )
        self.unreadable = unreadable


def read_stored_success_policy(
    workflow_id: str,
    stored: object,
) -> Result[SuccessPolicyRead, SuccessPolicyUnreadable]:
    """Read the stored ``success_policy`` column value. Pure.

    ``stored`` is the column as the driver returns it: None (SQL NULL), the
    decoded JSON value, or its text. JSON null is no policy, as SQL NULL is.
    """
    match stored:
        case None:
            return Ok(NoSuccessPolicy())
        case str():
            match loads_json(stored):
                case Err(err_value=error):
                    return _unreadable(workflow_id, f'the text is not JSON: {error}')
                case Ok(ok_value=decoded):
                    return _read_policy(workflow_id, decoded)
        case _:
            return _read_policy(workflow_id, cast(Json, stored))


def _read_policy(
    workflow_id: str,
    value: Json,
) -> Result[SuccessPolicyRead, SuccessPolicyUnreadable]:
    """Read a decoded policy value."""
    match value:
        case None:
            return Ok(NoSuccessPolicy())
        case dict():
            pass
        case _:
            return _unreadable(
                workflow_id, f'the policy is a JSON {_json_kind(value)}, not an object',
            )
    if 'cases' not in value:
        return _unreadable(workflow_id, 'the policy has no cases')
    match value['cases']:
        case list() as raw_cases:
            pass
        case other:
            return _unreadable(
                workflow_id, f'cases is a JSON {_json_kind(other)}, not an array',
            )
    cases: list[StoredSuccessCase] = []
    for position, raw_case in enumerate(raw_cases):
        match _read_case(raw_case):
            case Ok(ok_value=case):
                cases.append(case)
            case Err(err_value=cause):
                return _unreadable(workflow_id, f'cases[{position}]: {cause}')
    match value.get('optional_indices'):
        case None:
            pass
        case optional:
            match _read_indices(optional):
                case Ok():
                    pass
                case Err(err_value=cause):
                    return _unreadable(workflow_id, f'optional_indices: {cause}')
    return Ok(StoredSuccessPolicy(cases=tuple(cases)))


def _read_case(raw: Json) -> Result[StoredSuccessCase, str]:
    """Read one case: an object with a ``required_indices`` array."""
    match raw:
        case dict():
            pass
        case _:
            return Err(f'the case is a JSON {_json_kind(raw)}, not an object')
    if 'required_indices' not in raw:
        return Err('the case has no required_indices')
    match _read_indices(raw['required_indices']):
        case Ok(ok_value=indices):
            return Ok(StoredSuccessCase(required_indices=indices))
        case Err(err_value=cause):
            return Err(f'required_indices: {cause}')


def _read_indices(raw: Json) -> Result[tuple[int, ...], str]:
    """Read an array of node indices (ints from 0, not bools)."""
    match raw:
        case list():
            pass
        case _:
            return Err(f'a JSON {_json_kind(raw)}, not an array')
    indices: list[int] = []
    for position, item in enumerate(raw):
        match item:
            case bool():
                return Err(f'item {position} is a JSON boolean, not an index')
            case int() if item >= 0:
                indices.append(item)
            case int():
                return Err(f'item {position} is below 0')
            case _:
                return Err(f'item {position} is a JSON {_json_kind(item)}, not an index')
    return Ok(tuple(indices))


def _json_kind(value: Json) -> str:
    """The JSON type name of a decoded value."""
    match value:
        case None:
            return 'null'
        case bool():
            return 'boolean'
        case int() | float():
            return 'number'
        case str():
            return 'string'
        case list():
            return 'array'
        case dict():
            return 'object'


def _unreadable(
    workflow_id: str, cause: str,
) -> Err[SuccessPolicyUnreadable]:
    return Err(SuccessPolicyUnreadable(workflow_id=workflow_id, cause=cause))


def evaluate_success_cases(
    policy: StoredSuccessPolicy,
    status_by_index: dict[int, str],
) -> bool:
    """Whether every required node of one case with required nodes is
    COMPLETED. A case with no required node is skipped. Pure."""
    return any(
        len(case.required_indices) > 0
        and all(status_by_index.get(index) == 'COMPLETED' for index in case.required_indices)
        for case in policy.cases
    )


def required_indices_of(policy: StoredSuccessPolicy) -> set[int]:
    """Every index that some case requires. Pure."""
    return {index for case in policy.cases for index in case.required_indices}
