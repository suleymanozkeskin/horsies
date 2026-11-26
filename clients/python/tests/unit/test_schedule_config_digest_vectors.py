"""Python's side of the schedule config digest contract with the core.

``core/src/worker/scheduler/config_digest_vectors.json`` holds, per row, the
pattern text of the scheduler manifest (``pattern_json``, the text of
``model_dump(mode='json')``) with its timezone, the digest text that the core
hashes, and its SHA-256. The core tests in ``config_digest.rs`` pin the digest
of each row.

- The pattern text is the ``pattern_json`` that Python writes: this test pins
  it, byte for byte.
- The digest text is the core's own text. Python does not write it.
- The schedule config refuses a row whose time of day has a fraction of a
  second (``config_check`` ``REFUSED_TIME_WITH_FRACTION``).
"""

from __future__ import annotations

from pathlib import Path
from typing import Final, Literal, TypedDict

import pytest
from pydantic import TypeAdapter

from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.models.schedule import SchedulePattern, TaskSchedule
from horsies.core.scheduler.manifest import pattern_json

pytestmark = [pytest.mark.unit]

_VECTORS_PATH: Final = (
    Path(__file__).resolve().parents[4]
    / 'core'
    / 'src'
    / 'worker'
    / 'scheduler'
    / 'config_digest_vectors.json'
)

type ConfigCheck = Literal['ACCEPTED', 'REFUSED_TIME_WITH_FRACTION']


class _Vector(TypedDict):
    name: str
    pattern_json: str
    timezone: str
    text: str
    digest_hex: str
    config_check: ConfigCheck


class _Table(TypedDict):
    about: str
    vectors: list[_Vector]


_TABLE: Final = TypeAdapter(_Table)
_PATTERNS: Final = TypeAdapter[SchedulePattern](SchedulePattern)


def _vectors(check: ConfigCheck) -> list[_Vector]:
    table = _TABLE.validate_json(_VECTORS_PATH.read_text(encoding='utf-8'))
    return [vector for vector in table['vectors'] if vector['config_check'] == check]


@pytest.mark.parametrize('vector', _vectors('ACCEPTED'), ids=lambda vector: vector['name'])
def test_each_accepted_vector_is_python_s_pattern_text(
    vector: _Vector,
) -> None:
    schedule = TaskSchedule(
        name='config_digest_vector',
        task_name='config_digest_vector_task',
        pattern=_PATTERNS.validate_json(vector['pattern_json']),
        timezone=vector['timezone'],
    )

    assert pattern_json(schedule) == vector['pattern_json']


@pytest.mark.parametrize(
    'vector',
    _vectors('REFUSED_TIME_WITH_FRACTION'),
    ids=lambda vector: vector['name'],
)
def test_each_vector_with_a_fraction_of_a_second_is_refused_by_the_config(
    vector: _Vector,
) -> None:
    with pytest.raises(ConfigurationError) as refused:
        _PATTERNS.validate_json(vector['pattern_json'])

    assert refused.value.code == ErrorCode.CONFIG_INVALID_SCHEDULE
    assert refused.value.message.endswith(' time has microseconds')
