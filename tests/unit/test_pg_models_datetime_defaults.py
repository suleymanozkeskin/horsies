"""Regression for H1: ORM datetime defaults/onupdates must reference utc_now.

A scalar `default=datetime.now(timezone.utc)` evaluates once at import
time and writes the same timestamp to every row. SQLAlchemy treats a
callable as a per-row factory, which is what we want. We pin the
callable to `utc_now` specifically so a future regression that uses a
callable returning a cached/frozen datetime (e.g. `lambda: _FROZEN`) is
also caught.

The test inspects column metadata so a future regression is caught at
static-load time, even without a live database. SQLAlchemy wraps the
user-supplied callable inside `CallableColumnDefault`, so the comparison
is against `inspect.unwrap(clause.arg)`, not `clause.arg` directly.
"""

from __future__ import annotations

import inspect

import pytest
from sqlalchemy.orm import DeclarativeBase

from horsies.core.models.task_pg import (
    ScheduleStateModel,
    TaskAttemptModel,
    TaskHeartbeatModel,
    TaskModel,
    WorkerStateModel,
)
from horsies.core.models.workflow_pg import WorkflowModel, WorkflowTaskModel
from horsies.core.utils.time import utc_now


_CASES: list[tuple[type[DeclarativeBase], str, tuple[str, ...]]] = [
    (TaskModel, 'created_at', ('default',)),
    (TaskModel, 'updated_at', ('default', 'onupdate')),
    (TaskAttemptModel, 'created_at', ('default',)),
    (TaskHeartbeatModel, 'sent_at', ('default',)),
    (WorkerStateModel, 'snapshot_at', ('default',)),
    (ScheduleStateModel, 'updated_at', ('default', 'onupdate')),
    (WorkflowModel, 'created_at', ('default',)),
    (WorkflowModel, 'updated_at', ('default', 'onupdate')),
    (WorkflowTaskModel, 'created_at', ('default',)),
]


@pytest.mark.parametrize('model, col_name, attrs', _CASES)
def test_datetime_default_is_utc_now(
    model: type[DeclarativeBase],
    col_name: str,
    attrs: tuple[str, ...],
) -> None:
    """Each affected column's default/onupdate must reference utc_now.

    A scalar datetime fails this (the original bug); a callable that
    returns a cached/frozen datetime would also fail, which the looser
    `callable(clause.arg)` check could miss.
    """
    column = model.__table__.c[col_name]
    for attr in attrs:
        clause = getattr(column, attr)
        assert clause is not None, f'{model.__name__}.{col_name}: {attr} is None'
        unwrapped = inspect.unwrap(clause.arg)
        assert unwrapped is utc_now, (
            f'{model.__name__}.{col_name}: {attr}.arg must wrap utc_now '
            f'(evaluated per row), got '
            f'{type(clause.arg).__name__}={clause.arg!r}'
        )
