"""A core handle whose phase 2 of a workflow task fails, for worker tests.

``CoreHandle`` is a frozen extension class: a test cannot set one of its
methods. ``failing_phase2`` puts a stand-in on the broker that answers the
phase 2 with a ``NodeFinishFailed`` record and passes every other operation
to the real handle.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import horsies._core as core
from horsies.core.brokers.postgres import PostgresBroker


class _HandleWithPhaseTwo:
    """The real core handle, with ``finish_workflow_node_async`` replaced."""

    def __init__(self, handle: object, finish_workflow_node_async: AsyncMock) -> None:
        self._handle = handle
        self.finish_workflow_node_async = finish_workflow_node_async

    def __getattr__(self, name: str) -> Any:
        return getattr(self._handle, name)


def failing_phase2(
    broker: PostgresBroker, monkeypatch: pytest.MonkeyPatch, message: str,
) -> AsyncMock:
    """Make the phase 2 of ``broker`` answer ``NodeFinishFailed(message)``,
    not retryable. Returns the stand-in call."""
    record = MagicMock(spec=core.NodeFinishFailed)
    record.message = message
    record.retryable = False
    call = AsyncMock(return_value=record)
    monkeypatch.setattr(
        broker,
        '_core_handle',
        _HandleWithPhaseTwo(broker._core_handle, call),  # pyright: ignore[reportPrivateUsage]
    )
    return call
