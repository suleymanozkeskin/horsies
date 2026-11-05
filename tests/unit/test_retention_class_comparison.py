"""How a declared finite class stands against its registry row.

`compare_registration` is pure, so each outcome is pinned here without a
database: no row, an identical row, and a row that differs in each fact a
finite class carries.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from horsies.core.history.ddl.classes import (
    FINITE_PARTITION_INTERVAL,
    ClassAlreadyRegistered,
    ClassConflict,
    ClassNotRegistered,
    compare_registration,
    describe_class_conflict,
    finite_class_parent_name,
)
from horsies.core.history.partitions.catalog import RetentionClassRow

_KEY = 'xml_7d'
_SEVEN_DAYS = timedelta(days=7)


def _row(
    *,
    duration: timedelta | None = _SEVEN_DAYS,
    partition_interval: timedelta | None = FINITE_PARTITION_INTERVAL,
    finite_parent_name: str | None = None,
) -> RetentionClassRow:
    return RetentionClassRow(
        class_key=_KEY,
        duration=duration,
        partition_interval=partition_interval,
        finite_parent_name=(
            finite_parent_name
            if finite_parent_name is not None
            else finite_class_parent_name(_KEY)
        ),
    )


def test_no_row_is_not_registered() -> None:
    assert compare_registration(
        None, class_key=_KEY, duration=_SEVEN_DAYS,
    ) == ClassNotRegistered(class_key=_KEY)


def test_an_identical_row_is_already_registered() -> None:
    assert compare_registration(
        _row(), class_key=_KEY, duration=_SEVEN_DAYS,
    ) == ClassAlreadyRegistered(class_key=_KEY)


def test_a_different_duration_is_a_conflict_that_names_both() -> None:
    assert compare_registration(
        _row(), class_key=_KEY, duration=timedelta(days=14),
    ) == ClassConflict(
        class_key=_KEY,
        declared_duration=timedelta(days=14),
        existing_duration=_SEVEN_DAYS,
        existing_partition_interval=FINITE_PARTITION_INTERVAL,
        existing_parent_name=finite_class_parent_name(_KEY),
    )


@pytest.mark.parametrize(
    'row',
    [
        pytest.param(
            _row(partition_interval=timedelta(hours=1)), id='interval',
        ),
        pytest.param(
            _row(finite_parent_name='horsies_task_history_other'), id='parent',
        ),
        pytest.param(_row(duration=None), id='forever-row'),
    ],
)
def test_a_row_that_differs_in_any_fact_is_a_conflict(
    row: RetentionClassRow,
) -> None:
    outcome = compare_registration(row, class_key=_KEY, duration=_SEVEN_DAYS)
    assert isinstance(outcome, ClassConflict), outcome


def test_the_description_names_the_key_and_both_durations() -> None:
    description = describe_class_conflict(
        ClassConflict(
            class_key=_KEY,
            declared_duration=timedelta(days=14),
            existing_duration=_SEVEN_DAYS,
            existing_partition_interval=FINITE_PARTITION_INTERVAL,
            existing_parent_name=finite_class_parent_name(_KEY),
        )
    )
    assert description == (
        "retention class 'xml_7d' is registered with duration 7 days, 0:00:00; "
        'this configuration declares duration 14 days, 0:00:00'
    )


def test_the_description_names_a_non_daily_interval_and_a_forever_row() -> None:
    description = describe_class_conflict(
        ClassConflict(
            class_key=_KEY,
            declared_duration=_SEVEN_DAYS,
            existing_duration=None,
            existing_partition_interval=timedelta(hours=1),
            existing_parent_name=finite_class_parent_name(_KEY),
        )
    )
    assert description == (
        "retention class 'xml_7d' is registered with no duration (forever), "
        'partition interval 1:00:00; '
        'this configuration declares duration 7 days, 0:00:00'
    )


def test_the_description_names_a_parent_that_differs() -> None:
    """A conflict on the parent alone has equal durations; the text says why."""
    description = describe_class_conflict(
        ClassConflict(
            class_key=_KEY,
            declared_duration=_SEVEN_DAYS,
            existing_duration=_SEVEN_DAYS,
            existing_partition_interval=FINITE_PARTITION_INTERVAL,
            existing_parent_name='horsies_task_history_other',
        )
    )
    assert description == (
        "retention class 'xml_7d' is registered with duration 7 days, 0:00:00, "
        "parent relation 'horsies_task_history_other'; "
        'this configuration declares duration 7 days, 0:00:00'
    )
