"""Dual-run of the schedule slot id: the client rule against ``horsies_schedule_slot_task_id``.

Reference: the slot id the scheduler sent before this change (a UUIDv5 of
the name and the slot text). Candidate: ``schedule_slot_task_id`` (the
UUIDv7 layout of the SQL function, computed in the client). For every name
and slot of the grid the candidate equals the SQL function, and differs
from the reference: the decided change of the slot id.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Final
from zoneinfo import ZoneInfo

import psycopg
import pytest

from horsies.core.types.result import Err, Ok
from horsies.core.utils.fingerprint import schedule_slot_task_id
from tests.dualrun.databases import DualRunDatabase, install_baseline_schema

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_REFERENCE_NAMESPACE: Final = uuid.UUID('3c01f3f5-afd6-4363-b726-a5dab51a81c7')
_NAMES: Final = ('daily', 'hourly', 'reports.weekly', 'ünïcode-日本', 'x' * 200, '')
_BASE: Final = datetime(2026, 11, 16, 10, 0, tzinfo=timezone.utc)
_SLOTS: Final = (
    datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=1),
    _BASE,
    _BASE + timedelta(microseconds=1),
    _BASE + timedelta(milliseconds=1),
    _BASE + timedelta(microseconds=999_999),
    _BASE.astimezone(ZoneInfo('Asia/Kolkata')),
    datetime(2099, 12, 31, 23, 59, 59, 999_999, tzinfo=ZoneInfo('America/New_York')),
)


def _reference_id(name: str, slot: datetime) -> str:
    """The slot id before this change: UUIDv5 of ``name:<UTC isoformat>``."""
    return str(uuid.uuid5(_REFERENCE_NAMESPACE, f'{name}:{slot.astimezone(timezone.utc).isoformat()}'))


def _sql_id(connection: psycopg.Connection[tuple[object, ...]], name: str, slot: datetime) -> str:
    record = connection.execute(
        'SELECT horsies_schedule_slot_task_id(%s, %s)', (name, slot)
    ).fetchone()
    match record:
        case (uuid.UUID() as task_id,):
            return str(task_id)
        case _:
            raise AssertionError(f'the slot id function answered {record!r}')


def test_the_client_slot_id_is_the_sql_slot_id(empty_database: MakeDatabase) -> None:
    database = empty_database()
    match install_baseline_schema(database):
        case Err(failed):
            pytest.fail(f'{failed.step.value} {failed.database}: {failed.cause}')
        case Ok():
            pass
    grid = [(name, slot) for name in _NAMES for slot in _SLOTS]

    with psycopg.connect(database.url, autocommit=True) as connection:
        sql_ids = [_sql_id(connection, name, slot) for name, slot in grid]

    candidate_ids = [schedule_slot_task_id(name, slot) for name, slot in grid]
    reference_ids = [_reference_id(name, slot) for name, slot in grid]

    assert len(grid) == 42
    assert candidate_ids == sql_ids
    # The Asia/Kolkata slot is the base instant: one id per name for the two.
    assert len(set(candidate_ids)) == len(grid) - len(_NAMES)
    assert all(new != old for new, old in zip(candidate_ids, reference_ids, strict=True))
