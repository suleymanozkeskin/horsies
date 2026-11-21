"""Typing pin: `TaskError.data` takes `JsonInput` and gives `JsonValue`.

Pyright runs this file through `pyrightconfig.clean.json`. A line without a comment
must pass. A line with an ignore comment must be flagged, because an ignore comment
that hides nothing is an error there. The design rests on checker behaviour, so each
position has a line here.
"""

from __future__ import annotations

from collections import ChainMap, Counter, OrderedDict, UserDict, defaultdict, deque
from collections.abc import Mapping
from decimal import Decimal
from types import MappingProxyType
from typing import Literal, Protocol, TypedDict, assert_never

from horsies import (
    Horsies,
    JsonDict,
    JsonInput,
    JsonList,
    JsonValue,
    SubWorkflowError,
    TaskError,
    TaskResult,
)
from horsies.core.models.workflow import SubWorkflowSummary


_ids: list[str] = ['a']
_counts: dict[str, int] = {'a': 1}
_nested: dict[str, list[str]] = {'a': ['b']}
_deep: list[dict[str, list[int]]] = [{'a': [1]}]
_union: list[int | str | None] = [1]
_ordered: OrderedDict[str, int] = OrderedDict()
_default: defaultdict[str, int] = defaultdict(int)
_tally: Counter[str] = Counter()
_keyed: dict[Literal['a'], int] = {'a': 1}
_exact: JsonValue = [1]
_wide: JsonInput = _ids

_pair: tuple[int, int] = (1, 2)
_raw: bytes = b'x'
_queue: deque[int] = deque()
_items: set[int] = {1}
_price = Decimal('1')
_proxy: MappingProxyType[str, int] = MappingProxyType({'a': 1})
_user_dict: UserDict[str, int] = UserDict()
_chain: ChainMap[str, int] = ChainMap()
_int_keys: dict[int, str] = {1: 'a'}
_bad_inner: list[Decimal] = [_price]
_bad_nested: dict[str, tuple[int, ...]] = {'a': (1,)}


class _Open(TypedDict):
    n: int


_open: _Open = {'n': 1}


def writes_that_pass() -> list[TaskError]:
    return [
        TaskError(error_code='X'),
        TaskError(error_code='X', data=None),
        TaskError(error_code='X', data=_ids),
        TaskError(error_code='X', data=_counts),
        TaskError(error_code='X', data=_nested),
        TaskError(error_code='X', data=_deep),
        TaskError(error_code='X', data=_union),
        TaskError(error_code='X', data=_ordered),
        TaskError(error_code='X', data=_default),
        TaskError(error_code='X', data=_tally),
        TaskError(error_code='X', data=_keyed),
        TaskError(error_code='X', data=_exact),
        TaskError(error_code='X', data=_wide),
        TaskError(error_code='X', data={'ids': _ids, 'm': _counts, 'n': 1}),
        TaskError(error_code='X', data=[_ids, _counts, None]),
    ]


def writes_that_are_flagged(mapping: Mapping[str, int]) -> list[TaskError]:
    return [
        TaskError(error_code='X', data=_pair),  # pyright: ignore[reportArgumentType]
        TaskError(error_code='X', data=_raw),  # pyright: ignore[reportArgumentType]
        TaskError(error_code='X', data=_queue),  # pyright: ignore[reportArgumentType]
        TaskError(error_code='X', data=_items),  # pyright: ignore[reportArgumentType]
        TaskError(error_code='X', data=_price),  # pyright: ignore[reportArgumentType]
        TaskError(error_code='X', data=_proxy),  # pyright: ignore[reportArgumentType]
        TaskError(error_code='X', data=_user_dict),  # pyright: ignore[reportArgumentType]
        TaskError(error_code='X', data=_chain),  # pyright: ignore[reportArgumentType]
        TaskError(error_code='X', data=mapping),  # pyright: ignore[reportArgumentType]
        TaskError(error_code='X', data=_int_keys),  # pyright: ignore[reportArgumentType]
        TaskError(error_code='X', data=_bad_inner),  # pyright: ignore[reportArgumentType]
        TaskError(error_code='X', data=_bad_nested),  # pyright: ignore[reportArgumentType]
        TaskError(error_code='X', data={'a': (1,)}),  # pyright: ignore[reportArgumentType]
        # An open TypedDict can hold keys that it does not declare.
        TaskError(error_code='X', data=_open),  # pyright: ignore[reportArgumentType]
    ]


def a_subclass_has_both_types(summary: SubWorkflowSummary[int]) -> list[SubWorkflowError]:
    return [
        SubWorkflowError(
            error_code='X',
            sub_workflow_id='s',
            sub_workflow_summary=summary,
            data=_ids,
        ),
        SubWorkflowError(
            error_code='X',
            sub_workflow_id='s',
            sub_workflow_summary=summary,
            data=_pair,  # pyright: ignore[reportArgumentType]
        ),
    ]


def an_assignment_has_the_write_type(err: TaskError) -> None:
    err.data = _ids
    err.data = _pair  # pyright: ignore[reportAttributeAccessIssue]


def a_read_is_exact_and_exhaustive(err: TaskError) -> int:
    value = err.data
    match value:
        case None | bool() | int() | float() | str():
            return 0
        case list():
            value.append(1)
            return len(value)
        case dict():
            value['a'] = 1
            return len(value)
        case _ as unreachable:
            assert_never(unreachable)


def a_read_of_a_subclass_is_exact(err: SubWorkflowError) -> JsonValue:
    if isinstance(err.data, dict):
        return err.data['a']
    return err.data


def patterns_read_the_field(err: TaskError) -> JsonValue:
    match err.data:
        case {'ids': [first, *_]}:
            return first
        case [head, *_]:
            return head
        case _:
            return None


def takes_input(value: JsonInput) -> None:
    _ = value


def takes_exact(value: JsonValue) -> None:
    _ = value


def a_position_chooses_by_its_annotation() -> None:
    takes_input(_ids)
    takes_exact(_ids)  # pyright: ignore[reportArgumentType]
    takes_exact(_exact)


def the_protocols_are_the_two_containers() -> None:
    array: JsonList[str] = _ids
    mapping: JsonDict[int] = _counts
    tuple_array: JsonList[int] = _pair  # pyright: ignore[reportAssignmentType]
    proxy_mapping: JsonDict[int] = _proxy  # pyright: ignore[reportAssignmentType]
    _ = array, mapping, tuple_array, proxy_mapping


class _ReadOnlyData(Protocol):
    @property
    def data(self) -> JsonValue: ...


def reads_data(holder: _ReadOnlyData) -> JsonValue:
    return holder.data


def a_pyright_limit(err: TaskError) -> JsonValue:
    """Pyright compares the descriptor with the protocol member. mypy accepts this."""
    return reads_data(err)  # pyright: ignore[reportArgumentType]


def task_parameters(app: Horsies) -> None:
    @app.task('pin.pass_on')
    def pass_on(*, payload: JsonInput) -> TaskResult[int, TaskError]:
        _ = payload
        return TaskResult(ok=1)

    @app.task('pin.reads')
    def reads(*, payload: JsonValue) -> TaskResult[int, TaskError]:
        _ = payload
        return TaskResult(ok=1)

    _ = pass_on.send(payload=_ids)
    _ = reads.send(payload=_exact)
    _ = reads.send(payload=_ids)  # pyright: ignore[reportArgumentType]
