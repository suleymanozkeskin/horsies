"""The task id of a schedule slot: the UUIDv7 layout of ``horsies_schedule_slot_task_id``."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from horsies.core.history.identity.uuid7 import uuid7_birth_at
from horsies.core.utils.fingerprint import schedule_slot_task_id

pytestmark = pytest.mark.unit

SLOT = datetime(2026, 11, 16, 10, 0, tzinfo=timezone.utc)


def test_the_pinned_vector_of_the_sql_function() -> None:
    """The vector of ``baseline_scheduler_monitoring_cases.sql`` for (daily, 2026-11-16 10:00:00+00)."""
    assert schedule_slot_task_id('daily', SLOT) == '01a1e3cc-9d00-7a2c-be43-740ce1fde7de'


def test_the_id_is_a_uuidv7_whose_birth_is_the_slot_millisecond() -> None:
    slot = SLOT + timedelta(microseconds=123_456)

    task_id = uuid.UUID(schedule_slot_task_id('daily', slot))

    assert task_id.version == 7
    assert task_id.variant == uuid.RFC_4122
    assert uuid7_birth_at(task_id) == SLOT + timedelta(milliseconds=123)


def test_one_instant_in_another_time_zone_gives_one_id() -> None:
    berlin = SLOT.astimezone(ZoneInfo('Europe/Berlin'))

    assert schedule_slot_task_id('daily', berlin) == schedule_slot_task_id('daily', SLOT)


def test_another_name_or_another_microsecond_gives_another_id() -> None:
    base = schedule_slot_task_id('daily', SLOT)

    assert schedule_slot_task_id('hourly', SLOT) != base
    assert schedule_slot_task_id('daily', SLOT + timedelta(microseconds=1)) != base


def test_a_naive_slot_time_is_refused() -> None:
    with pytest.raises(ValueError, match='timezone-aware'):
        schedule_slot_task_id('daily', datetime(2026, 11, 16, 10, 0))


def test_a_slot_before_1970_is_a_defect() -> None:
    with pytest.raises(AssertionError, match='outside the UUIDv7 time range'):
        schedule_slot_task_id('daily', datetime(1969, 12, 31, 23, 59, 59, tzinfo=timezone.utc))
