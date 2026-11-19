"""PEP 695 JsonValue alias spike — phase 0.

Confirms `type JsonValue = ...` (PEP 695) is accepted by pydantic
`TypeAdapter` on Python 3.13, and that nested JSON shapes round-trip
through `validate_python` / `dump_python`.

Closes the alias-form gating spike from
`ignored-content/design/strict-serde.md` §12 phase 0.
"""

from __future__ import annotations

from typing import cast

import pytest
from pydantic import TypeAdapter

from horsies.core.codec.json_value import JsonValue


_ADAPTER: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)


@pytest.mark.parametrize(
    'value',
    [
        None,
        True,
        False,
        0,
        -1,
        42,
        0.0,
        3.14,
        '',
        'hello',
        [],
        {},
        [1, 'two', None, True, 3.14],
        {'a': 1, 'b': 'two', 'c': None, 'd': False},
        {'nested': {'list': [1, {'deep': [None, 'x']}]}},
        [[1, 2], [3, [4, [5, [6]]]]],
    ],
)
def test_pep_695_alias_round_trips_through_type_adapter(value: object) -> None:
    """PEP 695 alias accepted by TypeAdapter; round-trip is identity."""
    # Parametrize widens to `object`; each entry is hand-curated to satisfy
    # `JsonValue`, so the cast is honest at the test boundary.
    dumped = _ADAPTER.dump_python(cast(JsonValue, value), mode='json')
    validated = _ADAPTER.validate_python(dumped)
    assert validated == value


def test_type_adapter_constructs_without_error() -> None:
    """Constructing the adapter at import time is the gating step.

    Recursive aliases under PEP 695 historically required `model_rebuild` or
    `update_forward_refs`; confirm Pydantic 2.x handles it directly.
    """
    TypeAdapter(JsonValue)
