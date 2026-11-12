"""The garbage collection step of the worker process after its start."""

from __future__ import annotations

import gc
import weakref

import pytest

from horsies.core.worker.gc_policy import freeze_startup_objects

pytestmark = [pytest.mark.unit]


class _Node:
    def __init__(self) -> None:
        self.other: _Node | None = None


@pytest.fixture
def unfrozen() -> object:
    """Leave the collector as each test found it: nothing frozen."""
    gc.unfreeze()
    yield None
    gc.unfreeze()


def test_the_objects_alive_at_the_call_are_frozen(unfrozen: object) -> None:
    kept = _Node()

    freeze_startup_objects()

    assert gc.get_freeze_count() > 0
    assert kept.other is None


def test_garbage_of_the_start_is_collected_not_frozen(unfrozen: object) -> None:
    first, second = _Node(), _Node()
    first.other, second.other = second, first
    watcher = weakref.ref(first)
    del first, second

    freeze_startup_objects()

    assert watcher() is None
