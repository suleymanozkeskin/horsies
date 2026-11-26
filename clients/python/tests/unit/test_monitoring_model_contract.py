"""The committed contract of the monitoring read models is the contract of
the Pydantic models of today.

The core's contract test compares the JSON schema of each Rust model with
``core/src/monitoring/model_contract.json``
(``model_contract_tests.rs``). A change of a Pydantic model that the file
does not hold fails here; write the file again with
``uv run python -m tests.monitoring_model_contract``.
"""

from __future__ import annotations

import enum
from datetime import datetime
from typing import Annotated, Final, Literal
from uuid import UUID

import pytest
from pydantic import BaseModel, Field

from tests.monitoring_model_contract import (
    CONTRACT_PATH,
    INLINE_KIND_NAMES,
    KindNames,
    contract_text,
    model_contract,
    union_names,
)

pytestmark = pytest.mark.unit


def test_the_committed_contract_is_the_contract_of_the_models() -> None:
    committed = CONTRACT_PATH.read_text(encoding='utf-8')

    assert committed == contract_text(), (
        'model_contract.json is stale: run uv run python -m tests.monitoring_model_contract'
    )


class _Colour(str, enum.Enum):
    RED = 'RED'
    BLUE = 'BLUE'


class _Inner(BaseModel):
    at: datetime


class _First(BaseModel):
    kind: Literal['first'] = 'first'
    count: int


class _Second(BaseModel):
    kind: Literal['second'] = 'second'


_Choice = Annotated[_First | _Second, Field(discriminator='kind')]


class _Every(BaseModel):
    text: str
    maybe: float | None
    flag: bool
    id: UUID
    colours: list[_Colour]
    totals: dict[str, int]
    inner: _Inner | None
    choice: _Choice
    mode: Literal['slow', 'fast']


class _Inline(BaseModel):
    choice: Annotated[_First | _Second, Field(discriminator='kind')]


_NAMES: Final = KindNames(
    unions=union_names({'_Choice': _Choice}),
    inline={('_Every', 'mode'): '_Mode'},
)


def test_each_schema_form_reduces_to_its_kind_with_its_name() -> None:
    assert model_contract(_Every, _NAMES) == {
        'text': 'string',
        'maybe': {'nullable': 'number'},
        'flag': 'boolean',
        'id': 'uuid',
        'colours': {'list': {'enum': {'name': '_Colour', 'values': ['BLUE', 'RED']}}},
        'totals': {'map': 'integer'},
        'inner': {
            'nullable': {'model': {'name': '_Inner', 'fields': {'at': 'date-time'}}}
        },
        'choice': {
            'union': {
                'name': '_Choice',
                'tag': 'kind',
                'variants': {
                    'first': {'name': '_First', 'fields': {'count': 'integer'}},
                    'second': {'name': '_Second', 'fields': {}},
                },
            },
        },
        'mode': {'enum': {'name': '_Mode', 'values': ['fast', 'slow']}},
    }


def test_a_union_without_an_alias_takes_the_name_of_its_field() -> None:
    names = KindNames(unions={}, inline={('_Inline', 'choice'): '_InlineChoice'})

    contract = model_contract(_Inline, names)

    assert contract['choice'] == {
        'union': {
            'name': '_InlineChoice',
            'tag': 'kind',
            'variants': {
                'first': {'name': '_First', 'fields': {'count': 'integer'}},
                'second': {'name': '_Second', 'fields': {}},
            },
        },
    }


def test_an_inline_enum_without_a_name_is_refused() -> None:
    names = KindNames(unions=_NAMES.unions, inline={})

    with pytest.raises(
        AssertionError, match=r'_Every\.mode: an inline enum has no name'
    ):
        _ = model_contract(_Every, names)


def test_a_union_without_an_alias_or_a_name_is_refused() -> None:
    names = KindNames(unions={}, inline={})

    with pytest.raises(
        AssertionError, match=r'_Inline\.choice: an inline union has no name'
    ):
        _ = model_contract(_Inline, names)


def test_two_aliases_of_one_union_are_refused() -> None:
    with pytest.raises(
        AssertionError, match='_Again and _Choice are aliases of one union'
    ):
        _ = union_names({'_Choice': _Choice, '_Again': _Choice})


def test_each_inline_kind_name_names_a_kind_of_the_contract() -> None:
    text = contract_text()

    for name in INLINE_KIND_NAMES.values():
        assert f'"name": "{name}"' in text, name
