"""Python's side of the calculator contract with the core.

``crates/horsies-core/src/worker/scheduler/calculator_vectors.json`` holds, per
row, a pattern in the text of ``model_dump(mode='json')``, a timezone, an
instant and the next run that ``calculate_next_run`` gives after it. The core
test ``calculator::tests::each_python_vector_gives_python_s_next_run`` pins
the same table, so a change of either calculator fails one of the two tests.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Final

import pytest
from pydantic import TypeAdapter

from horsies.core.codec.json_io import dumps_json
from horsies.core.models.schedule import SchedulePattern
from horsies.core.scheduler.calculator import calculate_next_run
from horsies.core.types.result import Ok

_VECTORS_PATH: Final = (
    Path(__file__).resolve().parents[2]
    / 'crates'
    / 'horsies-core'
    / 'src'
    / 'worker'
    / 'scheduler'
    / 'calculator_vectors.json'
)

_PATTERNS: Final = TypeAdapter[SchedulePattern](SchedulePattern)


@dataclass(frozen=True, slots=True)
class _Vector:
    name: str
    pattern_json: str
    timezone: str
    after: datetime
    next_run_at: datetime


def _vectors() -> list[_Vector]:
    document = json.loads(_VECTORS_PATH.read_text(encoding='utf-8'))
    assert isinstance(document, dict)
    rows = document['vectors']  # pyright: ignore[reportUnknownVariableType]
    assert isinstance(rows, list) and rows
    vectors: list[_Vector] = []
    for row in rows:  # pyright: ignore[reportUnknownVariableType]
        assert isinstance(row, dict)
        vectors.append(
            _Vector(
                name=str(row['name']),  # pyright: ignore[reportUnknownArgumentType]
                pattern_json=str(row['pattern_json']),  # pyright: ignore[reportUnknownArgumentType]
                timezone=str(row['timezone']),  # pyright: ignore[reportUnknownArgumentType]
                after=datetime.fromisoformat(str(row['after'])),  # pyright: ignore[reportUnknownArgumentType]
                next_run_at=datetime.fromisoformat(str(row['next_run_at'])),  # pyright: ignore[reportUnknownArgumentType]
            ),
        )
    return vectors


@pytest.mark.unit
@pytest.mark.parametrize('vector', _vectors(), ids=lambda vector: vector.name)
def test_each_vector_is_python_s_next_run(vector: _Vector) -> None:
    pattern = _PATTERNS.validate_json(vector.pattern_json)

    assert dumps_json(pattern.model_dump(mode='json')) == Ok(vector.pattern_json)
    assert calculate_next_run(pattern, vector.after, vector.timezone) == vector.next_run_at
