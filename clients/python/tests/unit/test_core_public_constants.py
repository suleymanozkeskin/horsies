"""The constants that the core exports are the Python values, from one source."""

from __future__ import annotations

import pytest

from horsies._catalog import queue_defaults, schedule_defaults, task_bounds
from horsies.core.models import tasks
from horsies.core.models.schedule import TaskSchedule

pytestmark = pytest.mark.unit


def test_the_timeout_bounds_are_the_catalog_values() -> None:
    assert task_bounds.MIN_TASK_TIMEOUT_MS == 1000
    assert task_bounds.MAX_TASK_TIMEOUT_MS == 2**32 - 1
    assert tasks.MIN_TASK_TIMEOUT_MS is task_bounds.MIN_TASK_TIMEOUT_MS
    assert tasks.MAX_TASK_TIMEOUT_MS is task_bounds.MAX_TASK_TIMEOUT_MS


def test_the_idempotency_key_bound_is_255_bytes() -> None:
    assert task_bounds.IDEMPOTENCY_KEY_MAX_BYTES == 255


def test_the_default_queue_route_is_default_at_priority_100() -> None:
    assert queue_defaults.DEFAULT_QUEUE_NAME == 'default'
    assert queue_defaults.DEFAULT_QUEUE_PRIORITY == 100


def test_a_schedule_takes_the_catalog_defaults() -> None:
    fields = TaskSchedule.model_fields
    assert fields['enabled'].default is schedule_defaults.DEFAULT_SCHEDULE_ENABLED
    assert fields['timezone'].default == schedule_defaults.DEFAULT_SCHEDULE_TIMEZONE
    assert (
        fields['max_catch_up_runs'].default
        == schedule_defaults.DEFAULT_MAX_CATCH_UP_RUNS
    )
    assert schedule_defaults.DEFAULT_SCHEDULE_ENABLED is True
    assert schedule_defaults.DEFAULT_SCHEDULE_TIMEZONE == 'UTC'
    assert schedule_defaults.DEFAULT_MAX_CATCH_UP_RUNS == 100
