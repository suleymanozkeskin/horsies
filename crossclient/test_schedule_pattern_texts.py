"""Python's side of the schedule pattern texts
(``core/tests/fixtures/schedule_pattern/pattern_texts.json``).

The scheduler manifest carries the pattern of each schedule as the text
``pattern_json`` (``clients/python/horsies/core/scheduler/manifest.py``). The
fixture holds one text per pattern kind and one cron text per day selector.
Python's text is the reference: this test reads each text into a
``SchedulePattern`` and checks that ``pattern_json`` writes the same text,
byte for byte. The core (``core/src/manifest_schema.rs``) and the TypeScript
client (``clients/typescript/tests/runtime/generated/schedule-pattern.test.ts``)
read the same file.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Final, cast

import pytest
from pydantic import TypeAdapter

from horsies.core.models.schedule import SchedulePattern, TaskSchedule
from horsies.core.scheduler.manifest import pattern_json
from crossclient.stored_form_vectors import rows, text_of

PATTERN_TEXTS_FILE: Final = (
    Path(__file__).resolve().parents[1]
    / 'core'
    / 'tests'
    / 'fixtures'
    / 'schedule_pattern'
    / 'pattern_texts.json'
)

_PATTERNS: Final = TypeAdapter[SchedulePattern](SchedulePattern)


def _fixture() -> dict[str, object]:
    """The parsed fixture file."""
    document: object = json.loads(PATTERN_TEXTS_FILE.read_text(encoding='utf-8'))
    assert isinstance(document, dict), 'pattern_texts.json is not a JSON object'
    return cast('dict[str, object]', document)


@pytest.mark.unit
@pytest.mark.parametrize(
    'row', rows(_fixture(), 'patterns'), ids=lambda row: text_of(row, 'name'),
)
def test_each_pattern_text_is_the_text_that_python_writes(row: dict[str, object]) -> None:
    text = text_of(row, 'text')
    schedule = TaskSchedule(
        name=text_of(row, 'name'),
        task_name='reports.nightly',
        pattern=_PATTERNS.validate_json(text),
    )

    assert pattern_json(schedule) == text
