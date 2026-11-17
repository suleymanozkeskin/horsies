"""The committed contract of the monitoring read models is the contract of
the Pydantic models of today.

The core's contract test compares the JSON schema of each Rust model with
``crates/horsies-core/src/monitoring/model_contract.json``
(``model_contract_tests.rs``). A change of a Pydantic model that the file
does not hold fails here; write the file again with
``uv run python -m tests.monitoring_model_contract``.
"""

from __future__ import annotations

import enum
from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

import pytest
from pydantic import BaseModel, Field

from tests.monitoring_model_contract import CONTRACT_PATH, contract_text, model_contract

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


class _Every(BaseModel):
    text: str
    maybe: float | None
    flag: bool
    id: UUID
    colours: list[_Colour]
    totals: dict[str, int]
    inner: _Inner | None
    choice: Annotated[_First | _Second, Field(discriminator='kind')]


def test_each_schema_form_reduces_to_its_kind() -> None:
    assert model_contract(_Every) == {
        'text': 'string',
        'maybe': {'nullable': 'number'},
        'flag': 'boolean',
        'id': 'uuid',
        'colours': {'list': {'enum': ['BLUE', 'RED']}},
        'totals': {'map': 'integer'},
        'inner': {'nullable': {'model': {'at': 'date-time'}}},
        'choice': {
            'union': {
                'tag': 'kind',
                'variants': {'first': {'count': 'integer'}, 'second': {}},
            },
        },
    }
