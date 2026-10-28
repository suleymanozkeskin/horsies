"""Test support for code that selects broker resources.

Public entry points call ``await broker.select_resources()`` once, and a
real broker keeps one set per open event loop. A test with a mock broker
attaches a fake set with ``attach_resources``. A test with a real broker
reads the running loop's set with ``current_resources`` (sync, for setup and
assertions) or ``resources_of`` (through the public selection), swaps parts
of it with ``replaced_resources``, or gives every loop one mocked set with
``pin_resources``.
"""

from __future__ import annotations

import asyncio
import dataclasses
import threading
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, assert_never
from unittest.mock import AsyncMock, MagicMock

from horsies.core.brokers.listener import PostgresListener
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.resources import (
    BrokerResources,
    EventLoopLimit,
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


def attach_loop_limit(
    broker: Any, loop_limit: EventLoopLimit | None = None,
) -> EventLoopLimit:
    """Make ``broker.select_resources()`` return ``Err(loop_limit)``."""
    chosen = loop_limit or EventLoopLimit(
        limit=5,
        held_thread_names=('MainThread',) * 5,
        current_thread_name='other-thread',
    )
    broker.select_resources = AsyncMock(return_value=Err(chosen))
    return chosen


async def resources_of(broker: PostgresBroker) -> BrokerResources:
    """The real broker's resources for the running loop; fails on the loop limit."""
    selected = await broker.select_resources()
    assert is_ok(selected), selected
    return selected.ok_value


def current_resources(broker: PostgresBroker) -> BrokerResources:
    """The real broker's set for the running event loop, built when absent.

    White-box and sync, for setup and assertions inside a running loop. It
    never releases the sets of closed loops; the next ``select_resources()``
    call does.
    """
    selected = broker._resources_for_loop(  # pyright: ignore[reportPrivateUsage]
        asyncio.get_running_loop(), threading.current_thread().name,
    )
    assert is_ok(selected), selected
    return selected.ok_value


def pin_resources(broker: PostgresBroker, resources: BrokerResources) -> BrokerResources:
    """Make every event loop get ``resources`` from the real broker (white-box).

    For unit tests with mocked parts: sets already built are replaced too.
    """
    broker._build_resources = lambda: resources  # pyright: ignore[reportAttributeAccessIssue]
    broker._loop_sets = tuple(  # pyright: ignore[reportPrivateUsage]
        dataclasses.replace(entry, resources=resources)
        for entry in broker._loop_sets  # pyright: ignore[reportPrivateUsage]
    )
    return resources


def listener_of(broker: PostgresBroker) -> PostgresListener:
    """The real broker's listener; fails when the broker has none."""
    match current_resources(broker).listener:
        case ListenerEnabled(listener=listener):
            return listener
        case ListenerDisabled():
            raise AssertionError('this broker was built without a listener')
        case _ as unexpected:
            assert_never(unexpected)


def _swap_set(broker: PostgresBroker, old: BrokerResources, new: BrokerResources) -> None:
    broker._loop_sets = tuple(  # pyright: ignore[reportPrivateUsage]
        dataclasses.replace(entry, resources=new) if entry.resources is old else entry
        for entry in broker._loop_sets  # pyright: ignore[reportPrivateUsage]
    )


@contextmanager
def replaced_resources(broker: PostgresBroker, **parts: Any) -> Generator[BrokerResources, None, None]:
    """Swap parts of the running loop's set for the block, then restore."""
    original = current_resources(broker)
    replaced = dataclasses.replace(original, **parts)
    _swap_set(broker, original, replaced)
    try:
        yield replaced
    finally:
        _swap_set(broker, replaced, original)
