"""Regression for H1: ORM datetime defaults/onupdates must be callables.

A scalar `default=datetime.now(timezone.utc)` evaluates once at import
time and writes the same timestamp to every row. SQLAlchemy treats a
callable as a per-row factory, which is what we want.

This test inspects the column metadata so a future regression is caught
at static-load time, even without a live database.
"""

from __future__ import annotations

import pytest
from sqlalchemy import Column

from horsies.core.models.task_pg import (
    ScheduleStateModel,
    TaskAttemptModel,
    TaskHeartbeatModel,
    TaskModel,
    WorkerStateModel,
)
from horsies.core.models.workflow_pg import WorkflowModel, WorkflowTaskModel


_CASES: list[tuple[type, str, tuple[str, ...]]] = [
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
def test_datetime_default_is_callable(
    model: type, col_name: str, attrs: tuple[str, ...],
) -> None:
    """Each affected column's default/onupdate must wrap a callable, not a
    scalar datetime. Scalar values are evaluated once at import and shared
    across every insert/update."""
    column: Column[object] = model.__table__.c[col_name]
    for attr in attrs:
        clause = getattr(column, attr)
        assert clause is not None, f'{model.__name__}.{col_name}: {attr} is None'
        assert callable(clause.arg), (
            f'{model.__name__}.{col_name}: {attr}.arg must be callable '
            f'(evaluated per row), got '
            f'{type(clause.arg).__name__}={clause.arg!r}'
        )
