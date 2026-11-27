"""`SendKind` and the core error classes are part of the top-level surface.

`PostgresBroker.enqueue` and `enqueue_async` require a `SendKind`. A failed
core call holds or raises a class of the extension module `horsies._core`.
Users import each of them from `horsies`; the definitions stay where they are.
"""

from __future__ import annotations

import pytest

import horsies
import horsies._core as _core
from horsies.core.brokers import enqueue_task

pytestmark = pytest.mark.unit

CORE_ERROR_EXPORTS = (
    'CoreServerError',
    'CoreConnectionError',
    'CoreContractViolation',
    'CorePreconditionFailed',
    'ForkedAfterCoreStart',
    'CoreStopped',
    'TooManyPendingCalls',
    'CoreCallTimedOut',
    'RuntimeStartFailed',
)


def test_every_core_error_is_exported_and_is_the_core_class() -> None:
    for name in CORE_ERROR_EXPORTS:
        assert name in horsies.__all__, name
        assert getattr(horsies, name) is getattr(_core, name), name


def test_send_kind_is_exported_and_is_the_enqueue_task_class() -> None:
    assert 'SendKind' in horsies.__all__
    assert horsies.SendKind is enqueue_task.SendKind
