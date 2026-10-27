"""Test support for code that selects broker resources.

Public entry points call ``await broker.select_resources()`` once. A test
with a mock broker attaches a fake set with ``attach_resources``. A test with
a real broker reads its set with ``current_resources`` (sync, for setup and
assertions) or ``resources_of`` (through the public selection), and swaps
parts of it with ``replaced_resources``.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from horsies.core.brokers.listener import PostgresListener
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.resources import (
    BrokerResources,
    EventLoopConflict,
    ListenerDisabled,
    ListenerEnabled,
    ListenerState,
    SessionEngineShared,
)
from horsies.core.types.result import Err, Ok, is_ok


def fake_resources(
    *,
    session_factory: Any = None,
    engine: Any = None,
    partition_maintenance: Any = None,
    listener: ListenerState | None = None,
) -> BrokerResources:
    """A resource set of mocks. Each part not given is a fresh ``MagicMock``;
    the listener defaults to ``ListenerEnabled`` around a ``MagicMock``.
    """
    return BrokerResources(
        engine=engine if engine is not None else MagicMock(),
        session_factory=session_factory if session_factory is not None else MagicMock(),
        session_engine=SessionEngineShared(),
        partition_maintenance=(
            partition_maintenance if partition_maintenance is not None else MagicMock()
        ),
        listener=listener if listener is not None else ListenerEnabled(listener=MagicMock()),
    )


def attach_resources(broker: Any, resources: BrokerResources) -> BrokerResources:
    """Make ``broker.select_resources()`` return ``Ok(resources)``."""
    broker.select_resources = AsyncMock(return_value=Ok(resources))
    return resources


def attach_conflict(broker: Any, conflict: EventLoopConflict | None = None) -> EventLoopConflict:
    """Make ``broker.select_resources()`` return ``Err(conflict)``."""
    chosen = conflict or EventLoopConflict(
        bound_thread_name='MainThread', current_thread_name='other-thread',
    )
    broker.select_resources = AsyncMock(return_value=Err(chosen))
    return chosen


async def resources_of(broker: PostgresBroker) -> BrokerResources:
    """The real broker's resources for the running loop; fails on a conflict."""
    selected = await broker.select_resources()
    assert is_ok(selected), selected
    return selected.ok_value


def current_resources(broker: PostgresBroker) -> BrokerResources:
    """The real broker's resource set, read without selection (white-box)."""
    return broker._resources  # pyright: ignore[reportPrivateUsage]


def listener_of(broker: PostgresBroker) -> PostgresListener:
    """The real broker's listener; fails when the broker has none."""
    match current_resources(broker).listener:
        case ListenerEnabled(listener=listener):
            return listener
        case ListenerDisabled():
            raise AssertionError('this broker was built without a listener')


@contextmanager
def replaced_resources(broker: PostgresBroker, **parts: Any) -> Generator[BrokerResources, None, None]:
    """Swap parts of the real broker's resource set for the block, then restore."""
    original = current_resources(broker)
    replaced = dataclasses.replace(original, **parts)
    broker._resources = replaced  # pyright: ignore[reportPrivateUsage]
    try:
        yield replaced
    finally:
        broker._resources = original  # pyright: ignore[reportPrivateUsage]
