# pyright: reportPrivateUsage=false
"""Python's side of the schedule config digest contract with the core.

``crates/horsies-core/src/worker/scheduler/config_digest_vectors.json`` holds,
per row, the text that the config digest of a schedule hashes (the pattern as
``model_dump(mode='json')`` writes it, then the timezone) and its SHA-256. The
core tests in ``config_digest.rs`` pin the same table, so a change of the text
on either side fails one of the two tests.

- The pattern text is the ``pattern_json`` of the scheduler manifest, which
  the core parses and hashes.
- The digest is the one of ``_compute_config_hash`` of the Python reference
  loop (``tests/dualrun/reference/scheduler_loop_caller.py``).
- The schedule config refuses a row whose time of day has a fraction of a
  second (``config_check`` ``REFUSED_TIME_WITH_FRACTION``).
"""

from __future__ import annotations

from pathlib import Path
from typing import Final, Literal, TypedDict

import pytest
from pydantic import SecretStr, TypeAdapter

from horsies.core.app import Horsies
from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.schedule import ScheduleConfig, SchedulePattern, TaskSchedule
from horsies.core.scheduler.manifest import pattern_json
from tests.dualrun.reference.scheduler_loop_caller import Scheduler as ReferenceScheduler

pytestmark = [pytest.mark.unit]

_VECTORS_PATH: Final = (
    Path(__file__).resolve().parents[2]
    / 'crates'
    / 'horsies-core'
    / 'src'
    / 'worker'
    / 'scheduler'
    / 'config_digest_vectors.json'
)

_PATTERN_PREFIX: Final = '{"pattern":'
_TIMEZONE_KEY: Final = ',"timezone":'

type ConfigCheck = Literal['ACCEPTED', 'REFUSED_TIME_WITH_FRACTION']


class _Vector(TypedDict):
    name: str
    text: str
    digest_hex: str
    config_check: ConfigCheck


class _Table(TypedDict):
    about: str
    vectors: list[_Vector]


class _DigestText(TypedDict):
    """The text that a digest hashes, read by the schedule config."""

    pattern: SchedulePattern
    timezone: str


_TABLE: Final = TypeAdapter(_Table)
_DIGEST_TEXTS: Final = TypeAdapter(_DigestText)


def _vectors(check: ConfigCheck) -> list[_Vector]:
    table = _TABLE.validate_json(_VECTORS_PATH.read_text(encoding='utf-8'))
    return [vector for vector in table['vectors'] if vector['config_check'] == check]


def _pattern_text(vector: _Vector) -> str:
    """The pattern text inside the digest text of ``vector``."""
    text = vector['text']
    assert text.startswith(_PATTERN_PREFIX), text
    return text[len(_PATTERN_PREFIX) : text.rindex(_TIMEZONE_KEY)]


@pytest.fixture(scope='module')
def reference_loop() -> ReferenceScheduler:
    """The Python reference loop of an app with no schedule; only its digest
    function runs."""
    app = Horsies(
        AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=PostgresConfig(database_url=SecretStr('postgresql://user:pass@localhost/db')),
            schedule=ScheduleConfig(schedules=[]),
        ),
    )
    return ReferenceScheduler(app)


@pytest.mark.parametrize('vector', _vectors('ACCEPTED'), ids=lambda vector: vector['name'])
def test_each_accepted_vector_is_python_s_pattern_text_and_digest(
    vector: _Vector,
    reference_loop: ReferenceScheduler,
) -> None:
    digest_text = _DIGEST_TEXTS.validate_json(vector['text'])
    schedule = TaskSchedule(
        name='config_digest_vector',
        task_name='config_digest_vector_task',
        pattern=digest_text['pattern'],
        timezone=digest_text['timezone'],
    )

    assert pattern_json(schedule) == _pattern_text(vector)
    assert reference_loop._compute_config_hash(schedule).hex() == vector['digest_hex']


@pytest.mark.parametrize(
    'vector',
    _vectors('REFUSED_TIME_WITH_FRACTION'),
    ids=lambda vector: vector['name'],
)
def test_each_vector_with_a_fraction_of_a_second_is_refused_by_the_config(
    vector: _Vector,
) -> None:
    with pytest.raises(ConfigurationError) as refused:
        _DIGEST_TEXTS.validate_json(vector['text'])

    assert refused.value.code == ErrorCode.CONFIG_INVALID_SCHEDULE
    assert refused.value.message.endswith(' time has microseconds')
