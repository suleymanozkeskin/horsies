"""The time rule of the monitoring report comparison (``report_difference``).

Monitoring times are UTC in the public answer (decided). Python's reference
read wrote the offset of the database session, so a reference time at
another offset equals the same instant as pydantic writes it in UTC. A
candidate time is compared as written. No database.
"""

from __future__ import annotations

from typing import Final

import pytest

from tests.dualrun.monitoring_reads import report_difference

pytestmark = [pytest.mark.unit]

BERLIN: Final = '2026-11-17T14:59:14.784608+01:00'
"""A time as Python's reference read wrote it in a session at UTC+1."""

UTC_TEXT: Final = '2026-11-17T13:59:14.784608Z'
"""The same instant as pydantic writes it in UTC."""


def test_a_reference_time_at_another_offset_equals_the_instant_in_utc() -> None:
    assert report_difference(BERLIN, UTC_TEXT) == []


def test_a_whole_second_time_equals_the_instant_in_utc() -> None:
    assert report_difference('2026-11-17T14:59:14+01:00', '2026-11-17T13:59:14Z') == []


def test_the_rule_applies_inside_a_report() -> None:
    reference = {'ok': {'rows': [{'enqueued_at': BERLIN, 'task_name': 'a'}]}}
    candidate = {'ok': {'rows': [{'enqueued_at': UTC_TEXT, 'task_name': 'a'}]}}

    assert report_difference(reference, candidate) == []


@pytest.mark.parametrize(
    ('reference', 'candidate'),
    [
        pytest.param(UTC_TEXT, BERLIN, id='the-candidate-at-another-offset'),
        pytest.param(BERLIN, '2026-11-17T13:59:15.784608Z', id='another-instant'),
        pytest.param(BERLIN, '2026-11-17T13:59:14.784608+00:00', id='utc-not-as-pydantic-writes-it'),
        pytest.param(BERLIN, '2026-11-17T14:59:14.784608+01:00 ', id='a-candidate-that-is-not-a-time'),
        pytest.param('2026-11-17 14:59:14.784608+01:00', UTC_TEXT, id='a-reference-not-as-pydantic-writes-it'),
        pytest.param('2026-11-17T14:59:14.784608', '2026-11-17T14:59:14.784608Z', id='a-reference-without-offset'),
    ],
)
def test_other_texts_differ(reference: str, candidate: str) -> None:
    assert report_difference(reference, candidate) == [f'$: {reference!r} != {candidate!r}']


def test_a_text_that_is_not_a_time_is_compared_as_written() -> None:
    assert report_difference({'name': 'a'}, {'name': 'b'}) == ["$.name: 'a' != 'b'"]
